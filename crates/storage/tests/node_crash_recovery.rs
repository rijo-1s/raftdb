//! Randomized, seeded, fully deterministic multi-node simulation with real
//! `WalLog` storage on disk. Nodes are crashed and restarted at arbitrary
//! points, storage faults are injected in the middle of steps, and the
//! network drops, duplicates and reorders messages and partitions nodes.
//! After every step the Raft safety properties are checked:
//!
//! * **Election safety** - at most one leader per term, across all crashes.
//! * **No vote amnesia** - a node never grants votes to two different
//!   candidates in one term, even across restarts.
//! * **State-machine safety** - no two nodes ever apply different data at
//!   the same log index; applied indices are strictly increasing.
//! * **Leader completeness** - every entry ever observed committed is
//!   present, with identical data, in the log of every leader.
//! * **Durability of acknowledgements** - once a follower has replied
//!   success for an index, a clean crash+restart still holds that entry.
//! * **Terms and votes recover** - a restarted node never has a lower term
//!   than before, and never forgets a vote it had cast in its term.
//! * **Nothing is committed by recovery** - a restarted node starts with
//!   commit index 0 (no snapshot) and applies nothing until told.
//!
//! A failure prints the seed; re-run with `RAFTDB_SIM_SEED=<seed>`.
//! `RAFTDB_SIM_SEEDS=<n>` runs more seeds (default 25).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;

use raft_core::{Envelope, LogStore, Message, NodeId, RaftNode, Role, Term};
use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::WalLog;

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
    fn pct(&mut self, p: u64) -> bool {
        self.next() % 100 < p
    }
}

const IDS: [NodeId; 3] = [1, 2, 3];

type Node = RaftNode<WalLog>;

struct Sim {
    root: tempfile::TempDir,
    seed: u64,
    nodes: BTreeMap<NodeId, Option<Node>>,
    inbox: Vec<(NodeId, NodeId, Message)>,
    blocked: HashSet<NodeId>,
    /// Chaos phase: lossy/duplicating network and storage faults. Off while
    /// healing, so convergence is a fair liveness check.
    lossy: bool,
    rng: Rng,
    // --- oracle state
    committed: BTreeMap<u64, Vec<u8>>,
    leader_of_term: HashMap<Term, NodeId>,
    votes: HashMap<(NodeId, Term), NodeId>,
    last_hard: HashMap<NodeId, (Term, Option<NodeId>)>,
    acked_tip: HashMap<NodeId, (u64, Term)>,
    applied_max: HashMap<NodeId, u64>,
    next_payload: u64,
    // --- stats
    fault_crashes: usize,
    clean_crashes: usize,
    restarts: usize,
    max_committed: usize,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let mut s = Self {
            root: tempfile::tempdir().unwrap(),
            seed,
            nodes: BTreeMap::new(),
            inbox: Vec::new(),
            blocked: HashSet::new(),
            lossy: true,
            rng: Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1),
            committed: BTreeMap::new(),
            leader_of_term: HashMap::new(),
            votes: HashMap::new(),
            last_hard: HashMap::new(),
            acked_tip: HashMap::new(),
            applied_max: HashMap::new(),
            next_payload: 0,
            fault_crashes: 0,
            clean_crashes: 0,
            restarts: 0,
            max_committed: 0,
        };
        for id in IDS {
            s.nodes.insert(id, None);
            s.start(id);
        }
        s
    }

    fn fail(&self, msg: &str) -> ! {
        panic!("[seed {}] INVARIANT VIOLATION: {msg}", self.seed);
    }

    fn wal(&self, id: NodeId) -> PathBuf {
        let d = self.root.path().join(format!("n{id}"));
        std::fs::create_dir_all(&d).unwrap();
        d.join("node.wal")
    }

    fn peers_of(id: NodeId) -> Vec<NodeId> {
        IDS.iter().copied().filter(|&p| p != id).collect()
    }

    /// (Re)start node `id` from whatever is on disk.
    fn start(&mut self, id: NodeId) {
        fault::clear();
        let log = WalLog::open(self.wal(id))
            .unwrap_or_else(|e| self.fail(&format!("node {id} failed to recover: {e}")));
        let mut node = RaftNode::with_log_seeded(
            id,
            Self::peers_of(id),
            log,
            self.seed ^ (id << 32) ^ self.restarts as u64,
        );

        // Terms and votes recover.
        if let Some(&(t, v)) = self.last_hard.get(&id) {
            if node.term() < t {
                self.fail(&format!(
                    "node {id} term went backwards {t} -> {}",
                    node.term()
                ));
            }
            if node.term() == t && v.is_some() && node.voted_for() != v {
                self.fail(&format!(
                    "node {id} forgot its vote in term {t}: {v:?} -> {:?}",
                    node.voted_for()
                ));
            }
        }
        // Recovery commits nothing by itself.
        if node.commit_index() != 0 || !node.take_committed().is_empty() {
            self.fail(&format!(
                "node {id} treated entries as committed after restart"
            ));
        }
        // Acknowledged entries survived a clean crash.
        if let Some(&(m, term)) = self.acked_tip.get(&id) {
            if node.log().term_at(m) != term {
                self.fail(&format!(
                    "node {id} lost acknowledged entry {m} (term {term}) across a crash"
                ));
            }
        }
        self.applied_max.insert(id, 0);
        self.last_hard.insert(id, (node.term(), node.voted_for()));
        self.nodes.insert(id, Some(node));
        self.restarts += 1;
    }

    fn live(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.is_some())
            .map(|(&i, _)| i)
            .collect()
    }

    fn route(&mut self, from: NodeId, out: Vec<Envelope>) {
        if self.blocked.contains(&from) {
            return;
        }
        for env in out {
            if self.inbox.len() < 3000 {
                self.inbox.push((from, env.to, env.msg));
            }
        }
    }

    /// Run `f` on node `id`, optionally under an injected storage fault. A
    /// panic (the fault) crashes the node: its memory is gone, the step's
    /// output is never sent, and the disk is whatever the fault left.
    fn run<R>(
        &mut self,
        id: NodeId,
        plan: Option<FaultPlan>,
        f: impl FnOnce(&mut Node) -> R,
    ) -> Option<R> {
        let mut node = self.nodes.get_mut(&id)?.take()?;
        fault::clear();
        if let Some(p) = plan {
            fault::arm(p);
        }
        let res = catch_unwind(AssertUnwindSafe(|| f(&mut node)));
        let fired = fault::fired().is_some();
        fault::clear();
        match res {
            Ok(r) => {
                self.nodes.insert(id, Some(node));
                self.after_step(id);
                Some(r)
            }
            Err(_) => {
                assert!(
                    fired,
                    "[seed {}] node panicked without an injected fault",
                    self.seed
                );
                self.fault_crashes += 1;
                drop(node);
                // A mid-step crash may legitimately have applied a log
                // truncation on disk before dying; don't hold it to the
                // pre-step acknowledgement.
                self.acked_tip.remove(&id);
                None
            }
        }
    }

    fn random_plan(&mut self) -> Option<FaultPlan> {
        if !self.lossy || !self.rng.pct(45) {
            return None;
        }
        let points: Vec<FaultPoint> = FaultPoint::ALL
            .iter()
            .copied()
            .filter(|p| {
                !matches!(
                    p,
                    FaultPoint::WalRecoverTruncate | FaultPoint::WalRecoverSync
                )
            })
            .collect();
        let point = points[self.rng.below(points.len())];
        let action = match self.rng.below(4) {
            0 => FaultAction::FailBefore,
            1 => FaultAction::FailAfter,
            2 => FaultAction::PartialWrite(self.rng.below(60)),
            _ => FaultAction::FailBefore,
        };
        Some(FaultPlan {
            point,
            skip: self.rng.below(2),
            action,
        })
    }

    /// Check every invariant that can be evaluated from `id`'s state.
    fn after_step(&mut self, id: NodeId) {
        let mut node = self.nodes.get_mut(&id).unwrap().take().unwrap();

        // Election safety.
        if node.role() == Role::Leader {
            let t = node.term();
            match self.leader_of_term.get(&t) {
                Some(&other) if other != id => {
                    self.fail(&format!("two leaders in term {t}: {other} and {id}"))
                }
                _ => {
                    self.leader_of_term.insert(t, id);
                }
            }
        }

        // State-machine safety.
        let mut last = *self.applied_max.get(&id).unwrap_or(&0);
        for c in node.take_committed() {
            if c.index <= last {
                self.fail(&format!("node {id} applied index {} after {last}", c.index));
            }
            last = c.index;
            match self.committed.get(&c.index) {
                Some(existing) if *existing != c.data => self.fail(&format!(
                    "conflicting committed entries at index {}: node {id} applied different data",
                    c.index
                )),
                Some(_) => {}
                None => {
                    self.committed.insert(c.index, c.data);
                }
            }
        }
        self.applied_max.insert(id, last);
        self.max_committed = self.max_committed.max(self.committed.len());

        // Leader completeness.
        if node.role() == Role::Leader {
            for (&idx, data) in &self.committed {
                match node.log().entry_at(idx) {
                    Some(e) if e.data == *data => {}
                    other => self.fail(&format!(
                        "leader {id} (term {}) is missing committed entry {idx}: has {:?}",
                        node.term(),
                        other.map(|e| (e.term, e.data.clone()))
                    )),
                }
            }
        }

        // Account for legitimate truncation in the acknowledgement oracle.
        if let Some(&(m, term)) = self.acked_tip.get(&id) {
            if node.log().term_at(m) != term {
                self.acked_tip.remove(&id);
            }
        }
        self.last_hard.insert(id, (node.term(), node.voted_for()));
        self.nodes.insert(id, Some(node));
    }

    /// Record durability obligations created by outbound replies.
    fn note_outputs(&mut self, id: NodeId, request_from: Option<NodeId>, out: &[Envelope]) {
        for env in out {
            match &env.msg {
                Message::RequestVoteReply {
                    term,
                    vote_granted: true,
                } => {
                    let cand = request_from.unwrap_or(env.to);
                    match self.votes.insert((id, *term), cand) {
                        Some(prev) if prev != cand => self.fail(&format!(
                            "node {id} granted votes to {prev} and {cand} in term {term}"
                        )),
                        _ => {}
                    }
                }
                Message::AppendEntriesReply {
                    success: true,
                    match_index,
                    ..
                } if *match_index > 0 => {
                    if let Some(Some(n)) = self.nodes.get(&id) {
                        let t = n.log().term_at(*match_index);
                        if t != 0 {
                            self.acked_tip.insert(id, (*match_index, t));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn deliver_one(&mut self) {
        if self.inbox.is_empty() {
            return;
        }
        let i = self.rng.below(self.inbox.len()); // reordering
        let (from, to, msg) = self.inbox.swap_remove(i);
        if self.lossy {
            if self.rng.pct(8) {
                return; // lost in the network
            }
            if self.rng.pct(10) && self.inbox.len() < 3000 {
                self.inbox.push((from, to, msg.clone())); // duplicated
            }
        }
        if self.blocked.contains(&from) || self.blocked.contains(&to) {
            return;
        }
        let plan = if self.lossy { self.random_plan() } else { None };
        let is_vote = matches!(msg, Message::RequestVote { .. });
        if let Some(out) = self.run(to, plan, |n| n.step(from, msg)) {
            self.note_outputs(to, if is_vote { Some(from) } else { None }, &out);
            self.route(to, out);
        }
    }

    fn tick_one(&mut self) {
        let live = self.live();
        if live.is_empty() {
            return;
        }
        let id = live[self.rng.below(live.len())];
        let plan = self.random_plan();
        if let Some(out) = self.run(id, plan, |n| n.tick()) {
            self.note_outputs(id, None, &out);
            self.route(id, out);
        }
    }

    fn propose(&mut self) {
        let leaders: Vec<NodeId> = self
            .live()
            .into_iter()
            .filter(|id| self.nodes[id].as_ref().unwrap().role() == Role::Leader)
            .collect();
        if leaders.is_empty() {
            return;
        }
        let id = leaders[self.rng.below(leaders.len())];
        let n = 1 + self.rng.below(4);
        let batch: Vec<Vec<u8>> = (0..n)
            .map(|_| {
                self.next_payload += 1;
                format!("v{}", self.next_payload).into_bytes()
            })
            .collect();
        let plan = self.random_plan();
        let single = self.rng.pct(30);
        let res = self.run(id, plan, |node| {
            if single {
                for b in batch {
                    node.propose(b);
                }
            } else {
                node.propose_batch(batch);
            }
            node.force_heartbeat()
        });
        if let Some(out) = res {
            self.note_outputs(id, None, &out);
            self.route(id, out);
        }
    }

    fn crash_clean(&mut self) {
        let live = self.live();
        if live.len() <= 1 {
            return;
        }
        let id = live[self.rng.below(live.len())];
        self.nodes.insert(id, None);
        self.clean_crashes += 1;
    }

    fn restart_one(&mut self) {
        let dead: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.is_none())
            .map(|(&i, _)| i)
            .collect();
        if dead.is_empty() {
            return;
        }
        let id = dead[self.rng.below(dead.len())];
        self.start(id);
    }

    fn chaos_step(&mut self) {
        match self.rng.below(100) {
            0..=39 => self.deliver_one(),
            40..=69 => self.tick_one(),
            70..=82 => self.propose(),
            83..=86 => self.crash_clean(),
            87..=92 => self.restart_one(),
            93..=95 => {
                let id = IDS[self.rng.below(3)];
                if !self.blocked.remove(&id) {
                    self.blocked.insert(id);
                }
            }
            _ => self.tick_one(),
        }
    }

    /// Heal everything and check the cluster converges and keeps every
    /// committed entry.
    fn heal_and_converge(&mut self) {
        self.blocked.clear();
        self.lossy = false;
        for id in IDS {
            if self.nodes[&id].is_none() {
                self.start(id);
            }
        }
        let mut leader = None;
        for _ in 0..6000 {
            self.tick_one();
            for _ in 0..4 {
                self.deliver_one();
            }
            let ls: Vec<_> = self
                .live()
                .into_iter()
                .filter(|i| self.nodes[i].as_ref().unwrap().role() == Role::Leader)
                .collect();
            if ls.len() == 1 {
                leader = Some(ls[0]);
                // need it to have committed something in its own term
                let n = self.nodes[&ls[0]].as_ref().unwrap();
                if n.commit_index() > 0 && n.commit_index() >= n.log_last_index() {
                    break;
                }
            }
        }
        let Some(leader) = leader else {
            self.fail("no leader emerged after healing (liveness)")
        };
        // A final write must commit everywhere.
        self.next_payload += 1;
        let marker = format!("final{}", self.next_payload).into_bytes();
        let m = marker.clone();
        let out = self
            .run(leader, None, move |n| {
                n.propose(m);
                n.force_heartbeat()
            })
            .unwrap_or_else(|| self.fail("leader crashed with no fault armed"));
        self.route(leader, out);
        for _ in 0..4000 {
            self.tick_one();
            for _ in 0..4 {
                self.deliver_one();
            }
            if self.committed.values().any(|d| *d == marker) {
                // wait for followers to apply as well
                let all = IDS.iter().all(|id| {
                    self.applied_max.get(id).copied().unwrap_or(0)
                        >= *self
                            .committed
                            .iter()
                            .find(|(_, d)| **d == marker)
                            .unwrap()
                            .0
                });
                if all {
                    return;
                }
            }
        }
        self.fail("final write never committed on all nodes (liveness)");
    }
}

fn run_seed(seed: u64, steps: usize) -> (usize, usize, usize, usize) {
    let mut sim = Sim::new(seed);
    for _ in 0..steps {
        sim.chaos_step();
    }
    sim.heal_and_converge();
    (
        sim.committed.len(),
        sim.fault_crashes,
        sim.clean_crashes,
        sim.restarts,
    )
}

#[test]
fn randomized_crash_fault_and_partition_simulation_preserves_raft_safety() {
    let seeds: Vec<u64> = match std::env::var("RAFTDB_SIM_SEED") {
        Ok(s) => vec![s.parse().unwrap()],
        Err(_) => {
            let n: u64 = std::env::var("RAFTDB_SIM_SEEDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(40);
            (1..=n).collect()
        }
    };
    let (mut commits, mut faults, mut crashes, mut restarts) = (0, 0, 0, 0);
    for seed in &seeds {
        let (c, f, cr, r) = run_seed(*seed, 1200);
        commits += c;
        faults += f;
        crashes += cr;
        restarts += r;
    }
    eprintln!(
        "node_crash_sim: {} seeds, {commits} entries committed, {faults} fault-injected crashes, \
         {crashes} clean crashes, {restarts} restarts; all invariants held",
        seeds.len()
    );
    assert!(commits > 0 && faults > 0 && restarts > 0);
}
