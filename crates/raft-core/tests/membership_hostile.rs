//! Hostile membership-change scenarios for `raft-core`.
//!
//! Membership changes take effect when their entry is *applied* (committed
//! and consumed via `take_committed`), and the quorum used to commit and to
//! win elections is computed from that applied view. That is only safe if
//! changes are strictly one at a time: a node must never start a new change
//! while an earlier config entry sits in its log unapplied. Two stacked
//! single-server changes can otherwise yield quorums that do not intersect
//! (e.g. 2-of-{1,2,3} versus 3-of-{1,2,3,4,5}), and a committed entry can be
//! lost.
//!
//! Every message is produced by a real `RaftNode` and forwarded unmodified;
//! the tests only choose which messages arrive, and when. No clocks, no
//! threads.

use std::collections::{HashMap, VecDeque};

use raft_core::{Envelope, LogStore, MemLog, NodeId, RaftNode, Role};

type Nodes = HashMap<NodeId, RaftNode>;

fn cluster(ids: &[NodeId]) -> Nodes {
    ids.iter()
        .map(|&id| {
            let peers = ids.iter().copied().filter(|&p| p != id).collect();
            (id, RaftNode::new(id, peers))
        })
        .collect()
}

/// Deliver FIFO until quiet. `hold` returning true parks (drops) a message.
fn pump(
    nodes: &mut Nodes,
    start: Vec<(NodeId, Envelope)>,
    mut hold: impl FnMut(NodeId, &Envelope) -> bool,
) {
    let mut q: VecDeque<(NodeId, Envelope)> = start.into();
    let mut guard = 0;
    while let Some((from, env)) = q.pop_front() {
        guard += 1;
        assert!(guard < 10_000, "message storm");
        if hold(from, &env) {
            continue;
        }
        let out = nodes.get_mut(&env.to).unwrap().step(from, env.msg);
        let to = env.to;
        q.extend(out.into_iter().map(|e| (to, e)));
    }
}

fn tag(from: NodeId, out: Vec<Envelope>) -> Vec<(NodeId, Envelope)> {
    out.into_iter().map(|e| (from, e)).collect()
}

fn tick_until_output(nodes: &mut Nodes, id: NodeId) -> Vec<Envelope> {
    for _ in 0..500 {
        let out = nodes.get_mut(&id).unwrap().tick();
        if !out.is_empty() {
            return out;
        }
    }
    panic!("node {id} never produced output");
}

/// Deliver `out` (sent by `from`) only to `only`, and feed the replies back
/// to `from` unless `deliver_replies` is false. Returns nothing: callers
/// inspect the nodes afterwards.
fn deliver_only(
    nodes: &mut Nodes,
    from: NodeId,
    out: Vec<Envelope>,
    only: &[NodeId],
    deliver_replies: bool,
) {
    for env in out.into_iter().filter(|e| only.contains(&e.to)) {
        let to = env.to;
        let replies = nodes.get_mut(&to).unwrap().step(from, env.msg);
        if deliver_replies {
            for r in replies.into_iter().filter(|r| r.to == from) {
                nodes.get_mut(&from).unwrap().step(to, r.msg);
            }
        }
    }
}

fn apply(nodes: &mut Nodes, id: NodeId) -> Vec<Vec<u8>> {
    nodes
        .get_mut(&id)
        .unwrap()
        .take_committed()
        .into_iter()
        .map(|c| c.data)
        .collect()
}

fn members(nodes: &Nodes, id: NodeId) -> Vec<NodeId> {
    let mut m: Vec<NodeId> = nodes[&id].peers().collect();
    m.push(id);
    m.sort_unstable();
    m
}

/// Founders 1,2,3 plus two brand-new nodes (empty logs) that were started
/// to join the cluster and know the founders.
fn world() -> Nodes {
    let mut n = cluster(&[1, 2, 3]);
    for id in [4, 5] {
        n.insert(id, RaftNode::joining(id, vec![1, 2, 3], MemLog::new()));
    }
    n
}

/// Let time pass for every follower except `except`: a node that heard from
/// a leader recently refuses to pre-vote for anyone else.
fn age_followers(nodes: &mut Nodes, except: NodeId) {
    for (&id, n) in nodes.iter_mut() {
        if id != except && n.role() != Role::Leader {
            for _ in 0..25 {
                n.tick();
            }
        }
    }
}

fn elect(nodes: &mut Nodes, id: NodeId) {
    let out = tick_until_output(nodes, id);
    age_followers(nodes, id);
    pump(nodes, tag(id, out), |_, _| false);
    assert_eq!(nodes[&id].role(), Role::Leader);
    let out = nodes.get_mut(&id).unwrap().force_heartbeat();
    pump(nodes, tag(id, out), |_, _| false);
    for n in nodes.values_mut() {
        n.take_committed();
    }
}

/// Leader 1 appends `AddNode(4)`; only node 2 receives it. Leader 1 commits
/// it (2 of 3) and applies it, then is cut off. Node 2 holds the entry in
/// its log but has not learned it is committed, so its membership view is
/// still {1,2,3}. Returns the index of the config entry.
fn leader_one_adds_node_four_but_only_node_two_has_it(nodes: &mut Nodes) -> u64 {
    elect(nodes, 1);
    let a = nodes
        .get_mut(&1)
        .unwrap()
        .propose_add_node(4)
        .expect("add 4");
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    deliver_only(nodes, 1, out, &[2], true);
    apply(nodes, 1); // leader 1 applies it: view {1,2,3,4}
    assert_eq!(members(nodes, 1), vec![1, 2, 3, 4]);
    // Node 2 has the entry but not the commit index: still the old view.
    assert!(nodes[&2].log().last_index() >= a);
    assert!(
        nodes[&2].commit_index() < a,
        "node 2 must not know it is committed"
    );
    assert_eq!(members(nodes, 2), vec![1, 2, 3]);
    a
}

/// Node 2 wins term 2 with node 3's vote while node 1 is unreachable. Its
/// replication messages are withheld (delayed), so it has not yet committed
/// anything in its own term.
fn node_two_becomes_leader_still_holding_the_config_entry_unapplied(nodes: &mut Nodes) {
    // Pre-vote round (node 3 has had time to notice the old leader is gone),
    // then the real election.
    let pre = tick_until_output(nodes, 2);
    age_followers(nodes, 2);
    let mut votes = Vec::new();
    for env in pre.into_iter().filter(|e| e.to == 3) {
        for r in nodes.get_mut(&3).unwrap().step(2, env.msg) {
            votes.extend(nodes.get_mut(&2).unwrap().step(3, r.msg));
        }
    }
    for env in votes.into_iter().filter(|e| e.to == 3) {
        for r in nodes.get_mut(&3).unwrap().step(2, env.msg) {
            // the new leader's own first replication round is delayed: drop it
            nodes.get_mut(&2).unwrap().step(3, r.msg);
        }
    }
    assert_eq!(nodes[&2].role(), Role::Leader);
    assert_eq!(
        members(nodes, 2),
        vec![1, 2, 3],
        "Add(4) is still unapplied at node 2"
    );
}

#[test]
fn a_new_leader_cannot_stack_a_second_membership_change_on_an_unapplied_one() {
    let mut nodes = world();
    let a = leader_one_adds_node_four_but_only_node_two_has_it(&mut nodes);
    node_two_becomes_leader_still_holding_the_config_entry_unapplied(&mut nodes);

    // Node 2's log holds Add(4) at `a`, unapplied. A second change must wait.
    let second = nodes.get_mut(&2).unwrap().propose_add_node(5);
    if second.is_none() {
        // Guard in place. Liveness: once node 2 has committed an entry in
        // its own term and applied everything before it, changes work again.
        // (Node 3 first rejects, the leader retries on the next heartbeat.)
        for _ in 0..4 {
            let out = nodes.get_mut(&2).unwrap().force_heartbeat();
            pump(&mut nodes, tag(2, out), |_, e| e.to != 2 && e.to != 3);
            apply(&mut nodes, 2);
            if members(&nodes, 2) == vec![1, 2, 3, 4] {
                break;
            }
        }
        assert_eq!(
            members(&nodes, 2),
            vec![1, 2, 3, 4],
            "Add(4) must be applied now"
        );
        assert!(
            nodes.get_mut(&2).unwrap().propose_add_node(5).is_some(),
            "after applying the earlier change a new one must be accepted"
        );
        let _ = a;
        return;
    }

    // ---- The guard is missing: play the dangerous schedule to its end. ----
    let d1 = nodes
        .get_mut(&2)
        .unwrap()
        .propose(b"D1".to_vec())
        .expect("leader accepts data");
    // One replication round reaches only node 3. Node 2 still counts the
    // old 2-of-3 quorum, so Add(4), Add(5) and D1 all commit on {2,3}.
    // Everything between 2 and 3 is delivered; nothing reaches 1, 4 or 5.
    // Node 3 first rejects (it lacks the earlier entries) and the leader
    // retries on its next heartbeat, so run heartbeat rounds until caught up.
    for _ in 0..4 {
        let out = nodes.get_mut(&2).unwrap().force_heartbeat();
        pump(&mut nodes, tag(2, out), |_, e| e.to != 2 && e.to != 3);
        if nodes[&3].log().last_index() >= d1 {
            break;
        }
    }
    assert!(
        nodes[&3].log().last_index() >= d1,
        "node 3 must be fully caught up"
    );
    let acked = apply(&mut nodes, 2);
    assert!(
        acked.iter().any(|d| d == b"D1"),
        "leader 2 acknowledged D1 as committed (replicated on 2 of 5 nodes)"
    );

    // Slow node 1 now catches up in small batches: it receives the config
    // entries but not D1, and applies Add(5): its view becomes 5 nodes.
    nodes
        .get_mut(&2)
        .unwrap()
        .set_replication_limits(2, raft_core::DEFAULT_MAX_APPEND_BYTES);
    let out = nodes.get_mut(&2).unwrap().force_heartbeat();
    deliver_only(&mut nodes, 2, out, &[1], false);
    apply(&mut nodes, 1);
    assert_eq!(members(&nodes, 1), vec![1, 2, 3, 4, 5]);
    assert!(
        nodes[&1].log().last_index() < d1,
        "node 1 does not have D1 yet"
    );

    // Node 1 is cut off from 2 and 3, but 4 and 5 (fresh, empty logs) are
    // reachable. 3 of 5 votes are enough in node 1's view.
    // First the pre-vote round, then (once a majority would vote yes) the
    // real election.
    let pre = tick_until_output(&mut nodes, 1);
    let mut votes = Vec::new();
    for env in pre.into_iter().filter(|e| e.to == 4 || e.to == 5) {
        let to = env.to;
        for r in nodes.get_mut(&to).unwrap().step(1, env.msg) {
            votes.extend(nodes.get_mut(&1).unwrap().step(to, r.msg));
        }
    }
    for env in votes.into_iter().filter(|e| e.to == 4 || e.to == 5) {
        let to = env.to;
        for r in nodes.get_mut(&to).unwrap().step(1, env.msg) {
            nodes.get_mut(&1).unwrap().step(to, r.msg);
        }
    }
    if nodes[&1].role() == Role::Leader {
        let entry = nodes[&1].log().entry_at(d1).map(|e| e.data.clone());
        assert_eq!(
            entry.as_deref(),
            Some(&b"D1"[..]),
            "LEADER COMPLETENESS VIOLATED: node 1 won term {} without entry {d1} (D1), which \
             leader 2 had already acknowledged as committed; two stacked membership changes \
             produced non-intersecting quorums (2-of-{{1,2,3}} vs 3-of-{{1..5}})",
            nodes[&1].term()
        );
    }
}

// a config entry that was never committed by the old leader

/// Heartbeat rounds from `leader`, delivering only among `alive`, applying
/// committed entries on every live node after each round.
fn rounds(nodes: &mut Nodes, leader: NodeId, alive: &[NodeId], n: usize) {
    for _ in 0..n {
        let out = nodes.get_mut(&leader).unwrap().force_heartbeat();
        pump(nodes, tag(leader, out), |_, e| !alive.contains(&e.to));
        for &id in alive {
            apply(nodes, id);
        }
    }
}

/// Leader 1 appends `change`; only node 2 gets it and node 1 never sees the
/// ack, so it is **uncommitted** everywhere. Node 1 then dies and node 2 wins
/// term 2 with node 3's vote, still holding the entry unapplied.
fn old_leader_dies_with_an_uncommitted_config_entry(nodes: &mut Nodes, remove_three: bool) -> u64 {
    elect(nodes, 1);
    let idx = if remove_three {
        nodes.get_mut(&1).unwrap().propose_remove_node(3)
    } else {
        nodes.get_mut(&1).unwrap().propose_add_node(4)
    }
    .expect("leader 1 accepts the change");
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    deliver_only(nodes, 1, out, &[2], false); // node 1 never hears back
    assert!(nodes[&1].commit_index() < idx, "must be uncommitted");
    assert!(nodes[&2].log().last_index() >= idx);
    node_two_becomes_leader_still_holding_the_config_entry_unapplied(nodes);
    idx
}

#[test]
fn an_uncommitted_add_from_the_old_leader_blocks_new_changes_and_is_then_committed() {
    let mut nodes = world();
    let idx = old_leader_dies_with_an_uncommitted_config_entry(&mut nodes, false);

    // Until node 2 has applied through its own-term no-op, no change at all.
    assert!(nodes.get_mut(&2).unwrap().propose_add_node(5).is_none());
    assert!(nodes.get_mut(&2).unwrap().propose_remove_node(3).is_none());

    // Nodes 2 and 3 (node 1 is gone) commit the old leader's entry in the new
    // term: it is not lost, and both converge on the 4-member view.
    rounds(&mut nodes, 2, &[2, 3], 6);
    assert_eq!(members(&nodes, 2), vec![1, 2, 3, 4]);
    assert_eq!(members(&nodes, 3), vec![1, 2, 3, 4]);
    assert_eq!(
        nodes[&2].log().entry_at(idx).map(|e| e.term),
        Some(1),
        "entry kept"
    );
    // Now (and only now) the next change is accepted.
    assert!(nodes.get_mut(&2).unwrap().propose_add_node(5).is_some());
}

#[test]
fn an_uncommitted_remove_from_the_old_leader_blocks_new_changes_and_is_then_committed() {
    let mut nodes = world();
    old_leader_dies_with_an_uncommitted_config_entry(&mut nodes, true);
    assert!(nodes.get_mut(&2).unwrap().propose_add_node(4).is_none());
    assert!(nodes.get_mut(&2).unwrap().propose_remove_node(1).is_none());
    rounds(&mut nodes, 2, &[2, 3], 6);
    assert_eq!(members(&nodes, 2), vec![1, 2], "node 3 was removed");
    assert!(nodes.get_mut(&2).unwrap().propose_add_node(4).is_some());
}

#[test]
fn changes_are_accepted_again_once_the_leader_has_applied_its_first_entry() {
    // The guard must not make membership changes unavailable in steady state.
    let mut nodes = world();
    elect(&mut nodes, 1);
    let first = nodes.get_mut(&1).unwrap().propose_add_node(4);
    assert!(first.is_some(), "steady-state leader accepts a change");
    rounds(&mut nodes, 1, &[1, 2, 3], 4);
    assert_eq!(members(&nodes, 1), vec![1, 2, 3, 4]);
    assert!(nodes.get_mut(&1).unwrap().propose_add_node(5).is_some());
}
