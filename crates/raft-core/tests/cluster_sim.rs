use std::collections::{HashMap, VecDeque};

use raft_core::{Envelope, MemLog, Message, NodeId, RaftNode, Role};

/// A tiny in-memory network: an inbox queue per node. No sockets, no
/// threads, no timers - we advance the whole cluster by calling `tick()`
/// on every node and hand-delivering whatever messages come out. This is
/// the same style production Raft test suites (etcd, TiKV) use for
/// deterministic, reproducible cluster tests.
struct Cluster {
    nodes: HashMap<NodeId, RaftNode>,
    inbox: VecDeque<(NodeId, NodeId, raft_core::Message)>, // (from, to, msg)
    /// Nodes currently "down" - ticks/messages to/from them are dropped.
    partitioned: std::collections::HashSet<NodeId>,
}

#[allow(dead_code)]
impl Cluster {
    fn new(n: u64) -> Self {
        let ids: Vec<NodeId> = (1..=n).collect();
        let mut nodes = HashMap::new();
        for &id in &ids {
            let peers: Vec<NodeId> = ids.iter().copied().filter(|&p| p != id).collect();
            nodes.insert(id, RaftNode::new(id, peers));
        }
        Self {
            nodes,
            inbox: VecDeque::new(),
            partitioned: std::collections::HashSet::new(),
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

    /// Advance the whole cluster by one tick and fully drain the resulting
    /// message traffic (as if delivery were instantaneous).
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

    fn propose_via_leader(&mut self, data: &[u8]) -> Option<u64> {
        let leader = self.leaders().first().copied()?;
        let idx = self.nodes.get_mut(&leader).unwrap().propose(data.to_vec());
        self.drain_messages();
        idx
    }
}

#[test]
fn elects_exactly_one_leader() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    let leaders = cluster.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "expected exactly one leader, got {leaders:?}"
    );
}

#[test]
fn replicates_a_proposed_entry_to_all_followers() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    assert_eq!(cluster.leaders().len(), 1);

    let index = cluster
        .propose_via_leader(b"SET x=1")
        .expect("leader should accept the proposal");

    // Let heartbeats carry the entry to followers and replies flow back.
    cluster.run_ticks(10);

    for node in cluster.nodes.values_mut() {
        assert!(
            node.commit_index() >= index,
            "node {} did not commit index {index} (commit_index={})",
            node.id,
            node.commit_index()
        );
        let committed = node.take_committed();
        assert!(committed.iter().any(|c| c.data == b"SET x=1"));
    }
}

#[test]
fn survives_leader_crash_and_elects_a_new_one() {
    let mut cluster = Cluster::new(5);
    cluster.run_ticks(30);
    let first_leader = cluster.leaders()[0];

    cluster
        .propose_via_leader(b"SET a=1")
        .expect("first leader accepts proposal");
    cluster.run_ticks(10);

    // Kill the leader: simulate a permanent crash by permanently
    // partitioning it away from the rest of the cluster.
    cluster.partition(first_leader);
    cluster.run_ticks(40); // long enough for a new election to happen

    let leaders = cluster.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "expected a new single leader, got {leaders:?}"
    );
    assert_ne!(leaders[0], first_leader, "a different node must take over");

    // The new leader must still be able to commit new writes.
    let index = cluster
        .propose_via_leader(b"SET b=2")
        .expect("new leader accepts proposals");
    cluster.run_ticks(10);

    for (id, node) in cluster.nodes.iter_mut() {
        if *id == first_leader {
            continue; // still partitioned away
        }
        assert!(
            node.commit_index() >= index,
            "node {id} should have caught up"
        );
    }
}

#[test]
fn minority_partition_cannot_elect_a_leader() {
    let mut cluster = Cluster::new(5);
    cluster.run_ticks(30);

    // Partition away 2 of 5 nodes - the remaining 3 are still a majority
    // and must keep making progress; the isolated pair must not elect a
    // leader of their own (they can't reach quorum).
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    let minority = &ids[0..2];
    for &id in minority {
        cluster.partition(id);
    }

    cluster.run_ticks(40);

    let leaders = cluster.leaders();
    assert_eq!(
        leaders.len(),
        1,
        "majority side should elect exactly one leader"
    );
    assert!(
        !minority.contains(&leaders[0]),
        "leader must come from the majority side"
    );
}

/// The actual point of log compaction: a follower that's been cut off long
/// enough that the leader has thrown away the log entries it would need
/// can't catch up incrementally anymore - it needs the whole snapshot.
#[test]
fn lagging_follower_catches_up_via_install_snapshot_after_compaction() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    let leader = cluster.leaders()[0];
    let lagging = *cluster.nodes.keys().find(|&&id| id != leader).unwrap();

    cluster.partition(lagging);

    // Commit and apply several entries while `lagging` is cut off - the
    // other two nodes (leader + one follower) are still a quorum.
    for i in 0..5 {
        cluster
            .propose_via_leader(format!("SET k{i}={i}").as_bytes())
            .expect("2-of-3 quorum should still commit");
        cluster.run_ticks(10);
    }

    // Apply on every reachable node so last_applied advances far enough
    // to make compaction meaningful.
    for (&id, node) in cluster.nodes.iter_mut() {
        if id != lagging {
            node.take_committed();
        }
    }

    // The leader compacts everything it's applied. `lagging`, once healed,
    // will need entries older than what the leader now retains.
    let leader_node = cluster.nodes.get_mut(&leader).unwrap();
    let snapshot_index = leader_node
        .compact_log(b"snapshot-covers-5-entries".to_vec())
        .expect("leader must have applied entries to compact");
    assert!(snapshot_index > 0);

    cluster.heal(lagging);
    cluster.run_ticks(40); // enough for a heartbeat -> InstallSnapshot round trip

    let leader_last_index = cluster.nodes[&leader].log_last_index();
    let lagging_node = cluster.nodes.get_mut(&lagging).unwrap();

    assert_eq!(
        lagging_node.log_last_index(),
        leader_last_index,
        "lagging node must catch up to the leader's log length via InstallSnapshot"
    );
    assert_eq!(
        lagging_node.take_pending_snapshot(),
        Some(b"snapshot-covers-5-entries".to_vec()),
        "lagging node must have actually received and installed the leader's snapshot"
    );
}

#[test]
fn healthy_leader_confirms_a_read_index() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    let leader = cluster.leaders()[0];

    let read_id = cluster
        .nodes
        .get_mut(&leader)
        .unwrap()
        .request_read_index()
        .expect("the actual leader must accept a read request");

    // A heartbeat round is enough for peers to ack. take_committed() is
    // what lets last_applied catch up to commit_index - real callers
    // (KvNode) always do this immediately after every tick/step; a raw
    // RaftNode used directly (as here) must do it explicitly too, since
    // ReadIndex genuinely requires the state machine to have caught up,
    // not just a quorum ack.
    cluster.run_ticks(10);
    cluster.nodes.get_mut(&leader).unwrap().take_committed();

    let ready = cluster.nodes.get_mut(&leader).unwrap().take_ready_reads();
    assert!(
        ready.contains(&read_id),
        "a healthy, connected leader must be able to confirm a read"
    );
}

/// The whole point of ReadIndex: a leader that's been silently partitioned
/// away (it doesn't know it's no longer the "real" leader) must never be
/// able to confirm a linearizable read, because it can't get a quorum of
/// acks anymore. If this ever became ready, a client could read stale data
/// from a leader that's actually been superseded.
#[test]
fn partitioned_former_leader_cannot_confirm_a_read() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    let old_leader = cluster.leaders()[0];

    // Cut the old leader off from everyone - it still *thinks* it's
    // leader (nothing tells it otherwise), but it can no longer reach a
    // quorum of the cluster.
    cluster.partition(old_leader);

    let read_id = cluster
        .nodes
        .get_mut(&old_leader)
        .unwrap()
        .request_read_index()
        .expect("node still believes it's leader and accepts the read request");

    // Let plenty of time pass. A real leader would either get acks (it
    // can't - it's cut off) or eventually step down once it hears from a
    // higher term, but even before that happens, the read must not go ready.
    for _ in 0..40 {
        let out = cluster.nodes.get_mut(&old_leader).unwrap().tick();
        // Partitioned: nothing actually gets delivered anywhere.
        drop(out);
    }

    let ready = cluster
        .nodes
        .get_mut(&old_leader)
        .unwrap()
        .take_ready_reads();
    assert!(
        !ready.contains(&read_id),
        "a partitioned former leader must never confirm a linearizable read"
    );
}

/// A node that knows the current peers (so it has the right log-matching
/// context) but has not yet been confirmed as a member must never elect
/// itself, even after a very long silence from the cluster. Otherwise a
/// node in the middle of being added could declare itself a single-node
/// leader, which is a split-brain hazard.
#[test]
fn joining_node_does_not_self_elect_before_being_confirmed() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    assert_eq!(cluster.leaders().len(), 1);

    let joining = RaftNode::joining(4, vec![1, 2, 3], MemLog::new());
    assert!(!joining.is_active());
    cluster.nodes.insert(4, joining);

    // Far longer than any election timeout (max ~19 ticks) - if the guard
    // weren't in place, node 4 would have declared itself leader by now.
    cluster.run_ticks(80);

    let node4 = &cluster.nodes[&4];
    assert_eq!(
        node4.role(),
        Role::Follower,
        "an unconfirmed joining node must never elect itself"
    );
    assert!(!node4.is_active());
}

/// The full, correct lifecycle: a joining node is confirmed via a normal
/// committed AddNode entry (needing no help from the joining node itself:
/// the existing majority commits it on their own), after which it becomes
/// a real participant: it receives replication and could win an election.
#[test]
fn add_node_confirms_and_activates_a_joining_node() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    let leader = cluster.leaders()[0];

    let joining = RaftNode::joining(4, vec![1, 2, 3], MemLog::new());
    cluster.nodes.insert(4, joining);
    cluster.run_ticks(10);
    assert!(!cluster.nodes[&4].is_active());

    // A leader may start a membership change only after applying its own-term
    // no-op (`RaftNode::config_changes_allowed`). The real server applies
    // committed entries after every step; this test must do the same.
    cluster.nodes.get_mut(&leader).unwrap().take_committed();
    let index = cluster
        .nodes
        .get_mut(&leader)
        .unwrap()
        .propose_add_node(4)
        .expect("leader accepts add-node for a node that isn't yet a member");
    cluster.run_ticks(20);

    // The original 3-node majority commits this without any help from
    // node 4 (it has no vote yet - it's not active). Draining
    // take_committed() is what actually *applies* it - on the leader,
    // that's what starts replicating to node 4 in the first place.
    for (&id, node) in cluster.nodes.iter_mut() {
        if id == 4 {
            continue;
        }
        assert!(
            node.commit_index() >= index,
            "node {id} should have committed the add-node entry"
        );
        node.take_committed();
    }

    // Now that the leader knows about node 4, more ticks let the AddNode
    // entry (and the leader_commit covering it) actually reach node 4.
    cluster.run_ticks(15);
    let node4 = cluster.nodes.get_mut(&4).unwrap();
    assert!(
        node4.commit_index() >= index,
        "node 4 should have received and committed its own confirmation"
    );
    node4.take_committed(); // applying it is what flips `active`
    assert!(
        node4.is_active(),
        "node 4 must activate once its own AddNode entry commits and is applied"
    );

    // And it's now a real participant: proposing a write and ticking
    // further should replicate it to node 4 like any other follower.
    let write_index = cluster
        .propose_via_leader(b"SET x=1")
        .expect("leader still accepts writes");
    cluster.run_ticks(15);
    let node4 = cluster.nodes.get_mut(&4).unwrap();
    assert!(
        node4.commit_index() >= write_index,
        "node 4 should now receive normal replication like any other member"
    );
}

/// Removing a node stops it from receiving further replication and drops
/// it from future quorum calculations, while the rest of the cluster keeps
/// operating normally.
#[test]
fn remove_node_excludes_it_from_further_replication() {
    let mut cluster = Cluster::new(3);
    cluster.run_ticks(30);
    let leader = cluster.leaders()[0];
    let victim = *cluster.nodes.keys().find(|&&id| id != leader).unwrap();

    // A leader may start a membership change only after applying its own-term
    // no-op (`RaftNode::config_changes_allowed`). The real server applies
    // committed entries after every step; this test must do the same.
    cluster.nodes.get_mut(&leader).unwrap().take_committed();
    let index = cluster
        .nodes
        .get_mut(&leader)
        .unwrap()
        .propose_remove_node(victim)
        .expect("leader accepts remove-node for a current member");
    cluster.run_ticks(20);

    for (&id, node) in cluster.nodes.iter_mut() {
        if id == victim {
            continue;
        }
        assert!(
            node.commit_index() >= index,
            "node {id} should have committed the remove-node entry"
        );
        node.take_committed(); // applying is what actually removes the peer from bookkeeping
    }
    assert!(
        !cluster.nodes[&leader].peers().any(|p| p == victim),
        "leader must no longer count the removed node as a peer"
    );

    // Partition the removed node for realism (in a real deployment its
    // process would typically be stopped) and confirm the remaining
    // cluster still makes progress on its own - proving quorum correctly
    // recalculated to exclude it (2-of-2 remaining, not stuck needing 3).
    cluster.partition(victim);
    let write_index = cluster
        .propose_via_leader(b"SET y=2")
        .expect("cluster still commits without the removed node");
    cluster.run_ticks(15);

    for (&id, node) in cluster.nodes.iter_mut() {
        if id == victim {
            continue;
        }
        assert!(node.commit_index() >= write_index);
    }
}

/// `with_log` seeds the election-timeout RNG from `id` alone, which is
/// deliberate for reproducible tests but would leave a deployment
/// to break ties between competing candidates on incidental scheduling
/// jitter. This checks that `with_log_seeded` changes the timeout
/// sequence: same id, different seeds, different number of ticks until an
/// election starts.
#[test]
fn seeded_constructor_produces_a_different_timeout_sequence_than_the_default() {
    let mut default_node = RaftNode::new(1, vec![2]);
    let mut seeded_node =
        RaftNode::with_log_seeded(1, vec![2], MemLog::new(), 0xDEAD_BEEF_CAFE_u64);

    let default_ticks_to_candidate = ticks_until_candidate(&mut default_node);
    let seeded_ticks_to_candidate = ticks_until_candidate(&mut seeded_node);

    assert_ne!(
        default_ticks_to_candidate, seeded_ticks_to_candidate,
        "an explicit seed should change the election-timeout sequence for the same node ID \
         (got the same value for both, which would suggest the seed isn't actually being used)"
    );
}

/// Ticks until the node first asks for pre-votes, i.e. until its election
/// timeout fires.
fn ticks_until_candidate(node: &mut RaftNode) -> u64 {
    for i in 0..100 {
        if node
            .tick()
            .iter()
            .any(|e| matches!(e.msg, Message::PreVote { .. }))
        {
            return i;
        }
    }
    panic!("node never started an election within 100 ticks");
}
