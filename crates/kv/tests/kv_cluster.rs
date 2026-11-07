use std::collections::{HashMap, HashSet, VecDeque};

use kv::KvNode;
use raft_core::{Envelope, Message, NodeId, Role};

/// Same style harness as raft-core's cluster_sim.rs, one layer up: drives
/// a cluster of `KvNode`s instead of raw `RaftNode`s, so committed entries
/// actually get applied to a real key-value store as the simulation runs.
struct Cluster {
    nodes: HashMap<NodeId, KvNode>,
    inbox: VecDeque<(NodeId, NodeId, Message)>,
    partitioned: HashSet<NodeId>,
}

#[allow(dead_code)]
impl Cluster {
    fn new(n: u64, compaction_threshold: usize) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let mut nodes = HashMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            nodes.insert(id, KvNode::new(id, peers, compaction_threshold));
        }
        Self {
            nodes,
            inbox: VecDeque::new(),
            partitioned: HashSet::new(),
        }
    }

    fn partition(&mut self, id: NodeId) {
        self.partitioned.insert(id);
    }

    fn heal(&mut self, id: NodeId) {
        self.partitioned.remove(&id);
    }

    fn route(&mut self, from: NodeId, out: Vec<Envelope>) {
        if self.partitioned.contains(&from) {
            return;
        }
        for env in out {
            self.inbox.push_back((from, env.to, env.msg));
        }
    }

    fn step_time(&mut self) {
        let ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in ids {
            let out = self.nodes.get_mut(&id).unwrap().tick();
            self.route(id, out);
        }
        self.drain_messages();
    }

    fn drain_messages(&mut self) {
        while let Some((from, to, msg)) = self.inbox.pop_front() {
            if self.partitioned.contains(&to) || self.partitioned.contains(&from) {
                continue;
            }
            let out = self.nodes.get_mut(&to).unwrap().step(from, msg);
            self.route(to, out);
        }
    }

    fn run_ticks(&mut self, n: u64) {
        for _ in 0..n {
            self.step_time();
        }
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(id, n)| n.role() == Role::Leader && !self.partitioned.contains(id))
            .map(|(&id, _)| id)
            .collect()
    }

    fn set_via_leader(&mut self, key: &str, value: &str) -> Option<u64> {
        let leader = self.leaders().first().copied()?;
        let idx = self.nodes.get_mut(&leader).unwrap().propose_set(key, value);
        self.drain_messages();
        idx
    }
}

#[test]
fn write_replicates_and_is_readable_on_every_node() {
    let mut cluster = Cluster::new(3, 1000);
    cluster.run_ticks(30);

    cluster
        .set_via_leader("user:1", "alice")
        .expect("leader accepts the write");
    cluster.run_ticks(10);

    for node in cluster.nodes.values() {
        assert_eq!(node.get("user:1"), Some(&"alice".to_string()));
    }
}

#[test]
fn linearizable_read_on_healthy_leader_returns_latest_value() {
    let mut cluster = Cluster::new(3, 1000);
    cluster.run_ticks(30);
    let leader = cluster.leaders()[0];

    cluster.set_via_leader("x", "1").unwrap();
    cluster.run_ticks(10);

    let read_id = cluster
        .nodes
        .get_mut(&leader)
        .unwrap()
        .begin_linearizable_read()
        .expect("leader accepts the read request");
    cluster.run_ticks(10);

    let ready = cluster.nodes.get_mut(&leader).unwrap().poll_ready_reads();
    assert!(
        ready.contains(&read_id),
        "read must be confirmed by a healthy leader"
    );
    assert_eq!(cluster.nodes[&leader].get("x"), Some(&"1".to_string()));
}

/// The scenario that actually matters: an old leader gets partitioned away
/// (it doesn't know it's stale), a new leader takes over and accepts a
/// write the old leader never saw. The old leader's linearizable read must
/// never be confirmed - if it were, and it answered from local state, a
/// client could get a stale value or even miss a value entirely.
#[test]
fn partitioned_stale_leader_cannot_confirm_a_read_after_new_writes() {
    let mut cluster = Cluster::new(3, 1000);
    cluster.run_ticks(30);
    let old_leader = cluster.leaders()[0];

    cluster.set_via_leader("x", "1").unwrap();
    cluster.run_ticks(10);
    assert_eq!(cluster.nodes[&old_leader].get("x"), Some(&"1".to_string()));

    cluster.partition(old_leader);

    // A read requested right as the partition happens.
    let stale_read_id = cluster
        .nodes
        .get_mut(&old_leader)
        .unwrap()
        .begin_linearizable_read()
        .expect("old leader still believes it's leader");

    // New leader emerges among the remaining 2 nodes and accepts a write
    // the old (partitioned) leader can never see.
    cluster.run_ticks(40);
    cluster
        .set_via_leader("x", "2")
        .expect("2-of-3 quorum elects a new leader and commits");
    cluster.run_ticks(10);

    // The old leader's read must never have been confirmed.
    for _ in 0..20 {
        cluster.nodes.get_mut(&old_leader).unwrap().tick();
    }
    let ready = cluster
        .nodes
        .get_mut(&old_leader)
        .unwrap()
        .poll_ready_reads();
    assert!(
        !ready.contains(&stale_read_id),
        "a partitioned former leader must never confirm a linearizable read"
    );

    // Meanwhile the actual current cluster has the new value.
    let current_leader = cluster.leaders()[0];
    assert_eq!(
        cluster.nodes[&current_leader].get("x"),
        Some(&"2".to_string())
    );
}

/// A follower falls behind, the leader auto-compacts (via the KvNode
/// compaction_threshold policy), and the follower catches up via
/// InstallSnapshot with its *KvStore* correctly rebuilt from the snapshot
/// bytes, not just its raft log length matching.
#[test]
fn lagging_follower_rebuilds_correct_kv_state_via_snapshot() {
    // Low threshold so a handful of writes triggers compaction.
    let mut cluster = Cluster::new(3, 3);
    cluster.run_ticks(30);
    let leader = cluster.leaders()[0];
    let lagging = *cluster.nodes.keys().find(|&&id| id != leader).unwrap();

    cluster.partition(lagging);

    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
        cluster
            .set_via_leader(k, v)
            .expect("2-of-3 quorum still commits");
        cluster.run_ticks(10);
    }

    cluster.heal(lagging);
    cluster.run_ticks(40);

    let lagging_node = &cluster.nodes[&lagging];
    for (k, v) in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4"), ("e", "5")] {
        assert_eq!(
            lagging_node.get(k),
            Some(&v.to_string()),
            "key {k} must have the correct value after catching up via snapshot"
        );
    }
}

/// Compaction needs room for its own snapshot. When the host says there is
/// none, the log must be left alone (not crash) and compaction must resume as
/// soon as there is room.
#[test]
fn compaction_is_skipped_while_there_is_no_room_for_the_snapshot_and_resumes_after() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let mut n = KvNode::new(1, vec![], 3);
    for _ in 0..500 {
        n.tick();
        if n.role() == raft_core::Role::Leader {
            break;
        }
    }
    assert_eq!(n.role(), raft_core::Role::Leader);
    let room = Arc::new(AtomicBool::new(false));
    let flag = room.clone();
    n.snapshot_space_check = Some(Box::new(move |_bytes| flag.load(Ordering::Relaxed)));

    let write = |n: &mut KvNode, from: usize, to: usize| {
        for i in from..to {
            n.propose_set(format!("k{i}"), format!("v{i}")).unwrap();
            n.tick();
        }
    };
    write(&mut n, 0, 12);
    assert_eq!(
        raft_core::LogStore::last_included_index(n.raft.log()),
        0,
        "no room: nothing may be compacted"
    );
    assert_eq!(
        n.get("k11").map(String::as_str),
        Some("v11"),
        "and nothing is lost"
    );

    room.store(true, Ordering::Relaxed);
    write(&mut n, 12, 30);
    assert!(
        raft_core::LogStore::last_included_index(n.raft.log()) > 0,
        "room again: compaction resumes by itself"
    );
    assert_eq!(n.get("k29").map(String::as_str), Some("v29"));
}
