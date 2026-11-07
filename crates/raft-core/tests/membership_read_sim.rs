//! Randomized, deterministic simulation of membership changes and ReadIndex
//! reads under message reordering, loss, duplication and partitions.
//!
//! The existing randomized crash simulation (`storage/tests/node_crash_recovery.rs`)
//! uses a fixed three-node membership and issues no reads, so membership
//! changes and linearizable reads were only covered by directed tests. This
//! simulation drives real `RaftNode`s through random schedules and checks,
//! after every step:
//!
//!  * **Election safety** - at most one leader per term.
//!  * **State-machine safety** - every node applies the *same* entry
//!    (term, kind, data) at every index, config entries included.
//!  * **Leader completeness** - a node that becomes leader holds every entry
//!    that any node has already applied (applied implies committed).
//!  * **Read linearizability** - a read confirmed by `take_ready_reads`,
//!    served while the node is still leader of the term it was requested in,
//!    is served from a state that has applied at least everything any node
//!    had applied when the read was *requested*.
//!
//! Liveness is not asserted. Failures print the seed (`RAFTDB_MEMB_SEED=<n>`
//! replays one seed; `RAFTDB_MEMB_SEEDS=<n>` runs more) and a trace tail.

use std::collections::{BTreeMap, HashMap, HashSet};

use raft_core::{Envelope, LogStore, MemLog, Message, NodeId, RaftNode, Role};

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
        (self.next() % n.max(1) as u64) as usize
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

struct ReadReq {
    term: u64,
    floor: u64,
}

#[derive(Default, Debug)]
struct Stats {
    leaders: u64,
    config_entries: u64,
    adds: u64,
    removes: u64,
    reads_requested: u64,
    reads_served: u64,
    reads_aborted: u64,
    data_committed: u64,
    refused_proposals: u64,
    hot_windows: u64,
}

struct Sim {
    seed: u64,
    rng: Rng,
    nodes: BTreeMap<NodeId, RaftNode>,
    inflight: Vec<(NodeId, Envelope)>,
    blocked: HashSet<(NodeId, NodeId)>,
    leaders_by_term: HashMap<u64, NodeId>,
    seen_leader: HashSet<(NodeId, u64)>,
    applied: BTreeMap<u64, (u64, String, Vec<u8>)>,
    prev_applied: HashMap<NodeId, u64>,
    reads: HashMap<(NodeId, u64), ReadReq>,
    next_data: u64,
    next_node: NodeId,
    /// A node that has just become leader while still holding an unapplied
    /// config entry: the window in which stacking a second change is unsafe.
    hot_leader: Option<NodeId>,
    trace: Vec<String>,
    stats: Stats,
}

const MAX_NODES: NodeId = 9;

impl Sim {
    fn new(seed: u64) -> Self {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let founders = [1u64, 2, 3];
        let mut nodes = BTreeMap::new();
        for &id in &founders {
            let peers = founders.iter().copied().filter(|&p| p != id).collect();
            nodes.insert(
                id,
                RaftNode::with_log_seeded(id, peers, MemLog::new(), rng.next()),
            );
        }
        Sim {
            seed,
            rng,
            nodes,
            inflight: Vec::new(),
            blocked: HashSet::new(),
            leaders_by_term: HashMap::new(),
            seen_leader: HashSet::new(),
            applied: BTreeMap::new(),
            prev_applied: HashMap::new(),
            reads: HashMap::new(),
            next_data: 0,
            next_node: 4,
            hot_leader: None,
            trace: Vec::new(),
            stats: Stats::default(),
        }
    }

    fn note(&mut self, s: String) {
        self.trace.push(s);
        if self.trace.len() > 40 {
            self.trace.remove(0);
        }
    }

    fn fail(&self, what: &str) -> ! {
        panic!(
            "INVARIANT VIOLATED (seed {}; replay with RAFTDB_MEMB_SEED={}): {what}\n--- last events ---\n{}",
            self.seed,
            self.seed,
            self.trace.join("\n")
        );
    }

    fn global_applied_max(&self) -> u64 {
        self.nodes
            .values()
            .map(|n| n.last_applied_index())
            .max()
            .unwrap_or(0)
    }

    /// What the real server does after every tick/step: apply committed
    /// entries, then look at leadership and ready reads. Also checks the
    /// invariants.
    fn after(&mut self, id: NodeId) {
        // 1. apply, then record/compare every newly applied entry.
        let node = self.nodes.get_mut(&id).unwrap();
        let committed = node.take_committed();
        self.stats.data_committed += committed.len() as u64;
        let prev = *self.prev_applied.get(&id).unwrap_or(&0);
        let cur = node.last_applied_index();
        let mut found = Vec::new();
        for idx in prev + 1..=cur {
            let e = node
                .log()
                .entry_at(idx)
                .unwrap_or_else(|| panic!("node {id} applied index {idx} but has no such entry"));
            found.push((idx, (e.term, format!("{:?}", e.kind), e.data.clone())));
        }
        self.prev_applied.insert(id, cur);
        for (idx, val) in found {
            match self.applied.get(&idx) {
                Some(existing) if *existing != val => self.fail(&format!(
                    "STATE-MACHINE SAFETY: index {idx} applied as {existing:?} elsewhere but node {id} applied {val:?}"
                )),
                Some(_) => {}
                None => {
                    if val.1.starts_with("AddNode") {
                        self.stats.adds += 1;
                        self.stats.config_entries += 1;
                    } else if val.1.starts_with("RemoveNode") {
                        self.stats.removes += 1;
                        self.stats.config_entries += 1;
                    }
                    self.applied.insert(idx, val);
                }
            }
        }

        // 2. leadership: election safety + leader completeness.
        let node = &self.nodes[&id];
        if node.role() == Role::Leader {
            let term = node.term();
            if self.seen_leader.insert((id, term)) {
                self.stats.leaders += 1;
                let unapplied_config = (node.last_applied_index() + 1..=node.log().last_index())
                    .filter_map(|i| node.log().entry_at(i))
                    .any(|e| {
                        let k = format!("{:?}", e.kind);
                        k.starts_with("AddNode") || k.starts_with("RemoveNode")
                    });
                if unapplied_config {
                    self.hot_leader = Some(id);
                    self.stats.hot_windows += 1;
                }
                if let Some(other) = self.leaders_by_term.insert(term, id) {
                    if other != id {
                        self.fail(&format!(
                            "ELECTION SAFETY: nodes {other} and {id} both leaders of term {term}"
                        ));
                    }
                }
                for (idx, (t, k, d)) in &self.applied {
                    let ok = node.log().entry_at(*idx).is_some_and(|e| {
                        e.term == *t && format!("{:?}", e.kind) == *k && e.data == *d
                    });
                    if !ok {
                        self.fail(&format!(
                            "LEADER COMPLETENESS: node {id} became leader of term {term} without \
                             applied entry {idx} ({t}, {k}, {d:?}); it has {:?}",
                            node.log().entry_at(*idx)
                        ));
                    }
                }
            }
        }

        // 3. ready reads.
        let ready = self.nodes.get_mut(&id).unwrap().take_ready_reads();
        for rid in ready {
            let Some(req) = self.reads.remove(&(id, rid)) else {
                continue;
            };
            let node = &self.nodes[&id];
            if node.role() != Role::Leader || node.term() != req.term {
                self.stats.reads_aborted += 1; // not served: no longer the leader it asked as
                continue;
            }
            if node.last_applied_index() < req.floor {
                self.fail(&format!(
                    "STALE READ: node {id} (leader of term {}) served read {rid} having applied \
                     {} but entries up to {} had already been applied when the read was requested",
                    req.term,
                    node.last_applied_index(),
                    req.floor
                ));
            }
            self.stats.reads_served += 1;
        }
    }

    fn send(&mut self, from: NodeId, out: Vec<Envelope>) {
        self.inflight.extend(out.into_iter().map(|e| (from, e)));
    }

    fn pick_node(&mut self) -> NodeId {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        ids[self.rng.below(ids.len())]
    }

    fn pick_leader(&mut self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.role() == Role::Leader)
            .map(|(&i, _)| i)
            .collect();
        (!leaders.is_empty()).then(|| leaders[self.rng.below(leaders.len())])
    }

    fn event(&mut self) {
        // Exploit the dangerous window straight away: try to stack another
        // membership change, then write data that a stale quorum could commit.
        if let Some(l) = self.hot_leader.take() {
            if self.rng.chance(80) && self.nodes[&l].role() == Role::Leader {
                self.membership_change_on(l);
                self.next_data += 1;
                let data = format!("w{}", self.next_data).into_bytes();
                let _ = self.nodes.get_mut(&l).unwrap().propose(data);
                return;
            }
        }
        let roll = self.rng.below(100);
        match roll {
            // time passes on one node
            0..=29 => {
                let id = self.pick_node();
                let out = self.nodes.get_mut(&id).unwrap().tick();
                self.after(id);
                self.send(id, out);
            }
            // deliver, drop or duplicate one in-flight message
            30..=74 => {
                if self.inflight.is_empty() {
                    return;
                }
                let i = self.rng.below(self.inflight.len());
                let (from, env) = self.inflight.swap_remove(i);
                let to = env.to;
                if self.blocked.contains(&(from, to)) || self.rng.chance(4) {
                    return; // lost
                }
                if self.rng.chance(3) {
                    self.inflight
                        .push((from, Envelope::new(to, env.msg.clone())));
                }
                let Some(target) = self.nodes.get_mut(&to) else {
                    return;
                };
                let out = target.step(from, env.msg);
                self.after(to);
                self.send(to, out);
            }
            // client write
            75..=82 => {
                if let Some(l) = self.pick_leader() {
                    self.next_data += 1;
                    let data = format!("w{}", self.next_data).into_bytes();
                    let _ = self.nodes.get_mut(&l).unwrap().propose(data);
                }
            }
            // membership change
            83..=88 => self.membership_change(),
            // linearizable read
            89..=95 => {
                if let Some(l) = self.pick_leader() {
                    let floor = self.global_applied_max();
                    let node = self.nodes.get_mut(&l).unwrap();
                    let term = node.term();
                    if let Some(rid) = node.request_read_index() {
                        self.stats.reads_requested += 1;
                        self.reads.insert((l, rid), ReadReq { term, floor });
                        self.after(l);
                    }
                }
            }
            // network faults
            _ => match self.rng.below(3) {
                0 => self.blocked.clear(),
                1 => {
                    let victim = self.pick_node();
                    let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                    for o in ids.into_iter().filter(|&o| o != victim) {
                        self.blocked.insert((victim, o));
                        self.blocked.insert((o, victim));
                    }
                }
                _ => {
                    let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
                    let cut = self.rng.below(ids.len().max(1));
                    for (i, a) in ids.iter().enumerate() {
                        for (j, b) in ids.iter().enumerate() {
                            if (i < cut) != (j < cut) {
                                self.blocked.insert((*a, *b));
                            }
                        }
                    }
                }
            },
        }
    }

    fn membership_change(&mut self) {
        let Some(l) = self.pick_leader() else { return };
        self.membership_change_on(l);
    }

    fn membership_change_on(&mut self, l: NodeId) {
        let members: Vec<NodeId> = {
            let n = &self.nodes[&l];
            let mut m: Vec<NodeId> = n.peers().collect();
            m.push(l);
            m.sort_unstable();
            m
        };
        let add = self.rng.chance(50);
        if add && self.next_node <= MAX_NODES && members.len() < 7 {
            // An operator starts the new node first (knowing the current
            // members), then asks the leader to add it.
            let id = self.next_node;
            let peers: Vec<NodeId> = members.clone();
            let seed = self.rng.next();
            let accepted = self
                .nodes
                .get_mut(&l)
                .unwrap()
                .propose_add_node(id)
                .is_some();
            if accepted {
                self.next_node += 1;
                self.nodes
                    .insert(id, RaftNode::joining_seeded(id, peers, MemLog::new(), seed));
                self.note(format!("leader {l} proposed AddNode({id})"));
                self.after(l);
            } else {
                self.stats.refused_proposals += 1;
            }
        } else if !add && members.len() > 2 {
            let candidates: Vec<NodeId> = members.iter().copied().filter(|&m| m != l).collect();
            let victim = candidates[self.rng.below(candidates.len())];
            if self
                .nodes
                .get_mut(&l)
                .unwrap()
                .propose_remove_node(victim)
                .is_some()
            {
                self.note(format!("leader {l} proposed RemoveNode({victim})"));
                self.after(l);
            } else {
                self.stats.refused_proposals += 1;
            }
        }
    }
}

fn run_seed(seed: u64, steps: usize) -> Stats {
    let mut sim = Sim::new(seed);
    for step in 0..steps {
        sim.event();
        if step % 50 == 0 {
            sim.note(format!(
                "step {step}: nodes={:?} roles={:?}",
                sim.nodes.keys().collect::<Vec<_>>(),
                sim.nodes
                    .iter()
                    .map(|(i, n)| (*i, n.role(), n.term()))
                    .collect::<Vec<_>>()
            ));
        }
    }
    // Heal and let the cluster settle; the invariants still apply.
    sim.blocked.clear();
    for _ in 0..steps / 4 {
        sim.event();
    }
    sim.stats
}

#[test]
fn randomized_membership_and_reads_preserve_safety() {
    let seeds: Vec<u64> = match std::env::var("RAFTDB_MEMB_SEED") {
        Ok(s) => vec![s.parse().expect("RAFTDB_MEMB_SEED must be a number")],
        Err(_) => {
            let n: u64 = std::env::var("RAFTDB_MEMB_SEEDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(300);
            (1..=n).collect()
        }
    };
    let mut total = Stats::default();
    for &seed in &seeds {
        let s = run_seed(seed, 4000);
        total.leaders += s.leaders;
        total.config_entries += s.config_entries;
        total.adds += s.adds;
        total.removes += s.removes;
        total.reads_requested += s.reads_requested;
        total.reads_served += s.reads_served;
        total.reads_aborted += s.reads_aborted;
        total.data_committed += s.data_committed;
        total.refused_proposals += s.refused_proposals;
        total.hot_windows += s.hot_windows;
    }
    eprintln!("membership_read_sim over {} seeds: {total:?}", seeds.len());
    if seeds.len() > 1 {
        // The run must actually have exercised what it claims to check.
        assert!(
            total.leaders > seeds.len() as u64,
            "too few elections: {total:?}"
        );
        assert!(
            total.adds > 20 && total.removes > 20,
            "too few membership changes: {total:?}"
        );
        assert!(total.reads_served > 100, "too few served reads: {total:?}");
        assert!(
            total.data_committed > 500,
            "too little committed data: {total:?}"
        );
    }
}

#[allow(dead_code)]
fn _unused(_: Message) {}
