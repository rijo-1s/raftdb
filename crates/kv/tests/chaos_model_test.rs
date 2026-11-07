//! Chaos testing + model-based invariant checking, combined into one
//! harness deliberately (they share almost all their infrastructure, and
//! a chaos test without an invariant check to violate is just noise).
//!
//! The invariant under test is Raft's **State Machine Safety** property:
//! if two servers have applied the same log index, they must have applied
//! the *same command*. We check this continuously - not just "did the
//! test survive" - by recording the first command ever observed at each
//! index into a shared `ground_truth` map, and asserting every subsequent
//! observation at that index matches. Any mismatch is a safety violation
//! and fails immediately with the exact index and both commands.
//!
//! Faults injected during the chaos period: message loss, message
//! duplication, message reordering, and random network partitions,
//! independently and randomly, layered on top of a live cluster taking
//! random writes.
//!
//! The seed is always printed. Set `RAFTDB_CHAOS_SEED=<n>` to reproduce a
//! specific run if this ever fails.

use std::collections::hash_map::Entry as MapEntry;
use std::collections::{HashMap, HashSet, VecDeque};

use kv::{Command, KvStore};
use raft_core::{Envelope, Message, NodeId, RaftNode};

/// Deterministic xorshift64 PRNG - same style used throughout raft-core,
/// so chaos decisions are reproducible given a seed.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.next() % 100 < pct
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

struct ChaosCluster {
    nodes: HashMap<NodeId, RaftNode>,
    /// Per-node view of applied state, built purely from what that node
    /// itself has observed via `take_committed()` - independent of
    /// `ground_truth`, so comparing the two is a real check, not a tautology.
    stores: HashMap<NodeId, KvStore>,
    inbox: VecDeque<(NodeId, NodeId, Message)>,
    partitioned: HashSet<NodeId>,
    /// index -> the command first observed committed there, across ALL
    /// nodes. Growing this and checking every later observation against
    /// it IS the State Machine Safety check.
    ground_truth: HashMap<u64, Command>,
    rng: Rng,
}

impl ChaosCluster {
    fn new(n: u64, seed: u64) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let mut nodes = HashMap::new();
        let mut stores = HashMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            nodes.insert(id, RaftNode::new(id, peers));
            stores.insert(id, KvStore::new());
        }
        Self {
            nodes,
            stores,
            inbox: VecDeque::new(),
            partitioned: HashSet::new(),
            ground_truth: HashMap::new(),
            rng: Rng::new(seed),
        }
    }

    fn partition(&mut self, id: NodeId) {
        self.partitioned.insert(id);
    }
    fn heal(&mut self, id: NodeId) {
        self.partitioned.remove(&id);
    }
    fn heal_all(&mut self) {
        self.partitioned.clear();
    }

    fn leaders(&self) -> Vec<NodeId> {
        let mut v: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(id, n)| n.role() == raft_core::Role::Leader && !self.partitioned.contains(id))
            .map(|(&id, _)| id)
            .collect();
        v.sort_unstable();
        v
    }

    fn route(&mut self, from: NodeId, out: Vec<Envelope>, chaos: bool) {
        if self.partitioned.contains(&from) {
            return;
        }
        for env in out {
            if chaos && self.rng.chance(5) {
                continue; // dropped on the way out
            }
            let duplicate = chaos && self.rng.chance(3);
            if duplicate {
                self.inbox.push_back((from, env.to, env.msg.clone()));
            }
            self.inbox.push_back((from, env.to, env.msg));
        }
    }

    /// Applies newly committed entries on `id`, checking each against
    /// (and growing) `ground_truth` - the actual safety check.
    fn apply_committed(&mut self, id: NodeId) {
        let node = self.nodes.get_mut(&id).unwrap();
        let committed = node.take_committed();
        for c in committed {
            let cmd = Command::decode(&c.data).expect("this test only ever encodes valid Commands");
            match self.ground_truth.entry(c.index) {
                MapEntry::Occupied(existing) => {
                    assert_eq!(
                        existing.get(),
                        &cmd,
                        "STATE MACHINE SAFETY VIOLATION at index {}: node {id} applied {:?} but ground truth (from an earlier observation) says {:?}",
                        c.index,
                        cmd,
                        existing.get()
                    );
                }
                MapEntry::Vacant(slot) => {
                    slot.insert(cmd.clone());
                }
            }
            self.stores.get_mut(&id).unwrap().apply(&c.data).unwrap();
        }
    }

    fn drain_messages(&mut self, chaos: bool) {
        while !self.inbox.is_empty() {
            // Reordering: usually pop from the front (FIFO), but
            // sometimes yank a random element instead.
            let idx = if chaos && self.rng.chance(20) {
                self.rng.range(0, self.inbox.len() as u64) as usize
            } else {
                0
            };
            let (from, to, msg) = self.inbox.remove(idx).unwrap();
            if self.partitioned.contains(&to) || self.partitioned.contains(&from) {
                continue;
            }
            let out = self.nodes.get_mut(&to).unwrap().step(from, msg);
            self.route(to, out, chaos);
            self.apply_committed(to);
        }
    }

    fn step_time(&mut self, chaos: bool) {
        let mut ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let out = self.nodes.get_mut(&id).unwrap().tick();
            self.route(id, out, chaos);
            self.apply_committed(id);
        }
        self.drain_messages(chaos);
    }

    fn run_ticks(&mut self, n: u64, chaos: bool) {
        for _ in 0..n {
            self.step_time(chaos);
        }
    }

    fn propose_random_write(&mut self) {
        let Some(&leader) = self.leaders().first() else {
            return;
        };
        let key = format!("k{}", self.rng.range(0, 15));
        if self.rng.chance(20) {
            let node = self.nodes.get_mut(&leader).unwrap();
            node.propose(Command::Delete { key }.encode());
        } else {
            let value = format!("v{}", self.rng.next());
            let node = self.nodes.get_mut(&leader).unwrap();
            node.propose(Command::Set { key, value }.encode());
        }
    }
}

fn chaos_seed() -> u64 {
    std::env::var("RAFTDB_CHAOS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xC0FFEE_u64)
}

#[test]
fn chaos_random_faults_never_violate_state_machine_safety() {
    let seed = chaos_seed();
    // Always printed - this is what makes a failure reproducible.
    println!("chaos test seed: {seed} (override with RAFTDB_CHAOS_SEED=<n>)");

    let mut cluster = ChaosCluster::new(5, seed);
    cluster.run_ticks(30, false); // let a leader settle before chaos starts

    const ROUNDS: u64 = 600;
    for _ in 0..ROUNDS {
        if cluster.rng.chance(2) {
            let id = 1 + cluster.rng.range(0, 5);
            if cluster.partitioned.contains(&id) {
                cluster.heal(id);
            } else {
                cluster.partition(id);
            }
        }
        if cluster.rng.chance(30) {
            cluster.propose_random_write();
        }
        // step_time() calls apply_committed() internally for every node
        // on every round - the safety check runs continuously throughout
        // the chaos, not just in a final pass.
        cluster.step_time(true);
    }

    // Heal everything and give the cluster plenty of quiet time to
    // converge before checking final consistency.
    cluster.heal_all();
    cluster.run_ticks(5000, false);

    // Build the reference model purely from what was ever witnessed
    // committed, in index order - this is independent of any single
    // node's store, and is what every caught-up node must match.
    let max_index = cluster.ground_truth.keys().copied().max().unwrap_or(0);
    let mut reference = KvStore::new();
    for idx in 1..=max_index {
        if let Some(cmd) = cluster.ground_truth.get(&idx) {
            reference.apply(&cmd.encode()).unwrap();
        }
    }

    println!(
        "chaos test: {} log entries observed committed across the run; {} nodes to check convergence",
        cluster.ground_truth.len(),
        cluster.stores.len()
    );

    for (&id, store) in cluster.stores.iter() {
        if store != &reference {
            let mut gt_indices: Vec<u64> = cluster.ground_truth.keys().copied().collect();
            gt_indices.sort_unstable();
            eprintln!("=== FAILURE DIAGNOSTICS (seed {seed}) ===");
            eprintln!(
                "ground_truth indices ({}): {:?}",
                gt_indices.len(),
                gt_indices
            );
            for (&nid, node) in cluster.nodes.iter() {
                eprintln!(
                    "node {nid}: term={} commit_index={} log_last_index={} role={:?}",
                    node.term(),
                    node.commit_index(),
                    node.log_last_index(),
                    node.role()
                );
            }
            for (&nid, node) in cluster.nodes.iter() {
                if node.role() == raft_core::Role::Leader {
                    eprintln!(
                        "leader {nid}'s bookkeeping for stuck node {id}: next_index={:?} match_index={:?}",
                        node.debug_next_index(id),
                        node.debug_match_index(id)
                    );
                }
            }
            // Diff the specific keys that differ.
            let mut all_keys: std::collections::BTreeSet<&String> =
                std::collections::BTreeSet::new();
            for k in store.keys() {
                all_keys.insert(k);
            }
            for k in reference.keys() {
                all_keys.insert(k);
            }
            for k in all_keys {
                let a = store.get(k);
                let b = reference.get(k);
                if a != b {
                    eprintln!("  key {k:?}: node {id} has {a:?}, reference has {b:?}");
                }
            }
        }
        assert_eq!(
            store, &reference,
            "node {id}'s final KV state diverged from the reference model built from ground truth (seed {seed})"
        );
    }
}

/// A second run with a different seed and a higher partition rate, since a
/// single seed only exercises one particular sequence of faults. Different
/// parameters here are deliberate, not redundant.
#[test]
fn chaos_with_aggressive_partitioning_still_converges() {
    let seed = chaos_seed().wrapping_add(1);
    println!("chaos (aggressive) test seed: {seed}");

    let mut cluster = ChaosCluster::new(5, seed);
    cluster.run_ticks(30, false);

    const ROUNDS: u64 = 600;
    for _ in 0..ROUNDS {
        if cluster.rng.chance(6) {
            let id = 1 + cluster.rng.range(0, 5);
            if cluster.partitioned.contains(&id) {
                cluster.heal(id);
            } else {
                cluster.partition(id);
            }
        }
        if cluster.rng.chance(40) {
            cluster.propose_random_write();
        }
        cluster.step_time(true);
    }

    cluster.heal_all();
    cluster.run_ticks(5000, false);

    let max_index = cluster.ground_truth.keys().copied().max().unwrap_or(0);
    let mut reference = KvStore::new();
    for idx in 1..=max_index {
        if let Some(cmd) = cluster.ground_truth.get(&idx) {
            reference.apply(&cmd.encode()).unwrap();
        }
    }
    for (&id, store) in cluster.stores.iter() {
        assert_eq!(
            store, &reference,
            "node {id} diverged under aggressive partitioning"
        );
    }
}
