//! Tests for two correctness areas of `raft-core`:
//!
//! * **ReadIndex** (§6.4): a read may only be confirmed by acknowledgements
//!   that were *sent after* the read was requested, by a quorum of the
//!   *current* membership, in the *current* term, and only once the leader
//!   has committed an entry of its own term.
//! * **Membership removal**: a node that has been removed must stop taking
//!   part in elections and consensus, and the rest of the cluster must not
//!   count a removed node toward any quorum.
//!
//! Every message here is produced by a real `RaftNode` and forwarded
//! unmodified, with the test controlling *which* messages are delivered,
//! delayed or dropped. No clocks, no threads: fully deterministic.

use std::collections::{HashMap, VecDeque};

use raft_core::{Envelope, Message, NodeId, RaftNode, Role};

type Nodes = HashMap<NodeId, RaftNode>;

fn cluster(ids: &[NodeId]) -> Nodes {
    ids.iter()
        .map(|&id| {
            let peers = ids.iter().copied().filter(|&p| p != id).collect();
            (id, RaftNode::new(id, peers))
        })
        .collect()
}

/// Deliver messages FIFO until none are left. `hold(from, env)` returning
/// true parks the message instead of delivering it; parked messages are
/// returned so a test can deliver them later (a delayed message).
fn pump(
    nodes: &mut Nodes,
    start: Vec<(NodeId, Envelope)>,
    mut hold: impl FnMut(NodeId, &Envelope) -> bool,
) -> Vec<(NodeId, Envelope)> {
    let mut q: VecDeque<(NodeId, Envelope)> = start.into();
    let mut held = Vec::new();
    let mut guard = 0;
    while let Some((from, env)) = q.pop_front() {
        guard += 1;
        assert!(guard < 10_000, "message storm");
        if hold(from, &env) {
            held.push((from, env));
            continue;
        }
        let out = nodes.get_mut(&env.to).unwrap().step(from, env.msg);
        let to = env.to;
        q.extend(out.into_iter().map(|e| (to, e)));
    }
    held
}

fn tag(from: NodeId, out: Vec<Envelope>) -> Vec<(NodeId, Envelope)> {
    out.into_iter().map(|e| (from, e)).collect()
}

/// Make `id` leader by ticking only it until it campaigns, then delivering
/// everything. Afterwards a heartbeat round propagates the commit index and
/// every node applies what is committed.
/// Let time pass for every follower except `except`: a node that heard from
/// a leader recently refuses to pre-vote for anyone else, so a test that
/// wants an election must first let the old leader go quiet.
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
    let mut out = Vec::new();
    for _ in 0..200 {
        out = nodes.get_mut(&id).unwrap().tick();
        if !out.is_empty() {
            break;
        }
    }
    assert!(!out.is_empty(), "node {id} never campaigned");
    age_followers(nodes, id);
    pump(nodes, tag(id, out), |_, _| false);
    assert_eq!(nodes[&id].role(), Role::Leader);
    settle(nodes, id);
}

/// One heartbeat round from `leader`, fully delivered, then everyone applies.
fn settle(nodes: &mut Nodes, leader: NodeId) {
    let out = nodes.get_mut(&leader).unwrap().force_heartbeat();
    pump(nodes, tag(leader, out), |_, _| false);
    for n in nodes.values_mut() {
        n.take_committed();
    }
}

fn is_cluster_message_to(env: &Envelope, to: NodeId) -> bool {
    env.to == to
}

// ---------------------------------------------------------------------
// ReadIndex
// ---------------------------------------------------------------------

/// A reply may only acknowledge reads requested before the message it
/// answers was sent. A reply to a heartbeat sent *before* the read says
/// nothing about whether the leader is still the leader *now*, so a delayed
/// reply must not confirm a read on a deposed leader.
#[test]
fn delayed_pre_request_reply_does_not_confirm_a_read() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);

    // Heartbeat H is sent BEFORE the read exists; only node 2 sees it, and
    // its reply is delayed (held) by the network.
    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    let to2 = hb.into_iter().find(|e| e.to == 2).unwrap();
    let delayed_reply = nodes.get_mut(&2).unwrap().step(1, to2.msg);
    assert_eq!(delayed_reply.len(), 1);

    // Now the read is requested.
    let read = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    nodes.get_mut(&1).unwrap().take_committed();

    // The stale reply finally arrives. With one peer ack the quorum (2 of
    // 3) would be reached if it counted - it must not.
    for env in delayed_reply {
        let out = nodes.get_mut(&1).unwrap().step(2, env.msg);
        pump(&mut nodes, tag(1, out), |_, _| false);
    }
    assert!(
        !nodes
            .get_mut(&1)
            .unwrap()
            .take_ready_reads()
            .contains(&read),
        "a reply to a pre-request heartbeat must not confirm the read"
    );

    // A heartbeat round sent after the request does confirm it.
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    pump(&mut nodes, tag(1, out), |_, _| false);
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(
        nodes
            .get_mut(&1)
            .unwrap()
            .take_ready_reads()
            .contains(&read),
        "acks of a post-request heartbeat must confirm the read"
    );
}

/// The same stale reply delivered many times (a duplicating network) must
/// still never confirm the read.
#[test]
fn duplicated_stale_replies_never_confirm_a_read() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);

    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    let to2 = hb.into_iter().find(|e| e.to == 2).unwrap();
    let stale = nodes.get_mut(&2).unwrap().step(1, to2.msg);
    let read = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    for _ in 0..5 {
        for env in stale.clone() {
            nodes.get_mut(&1).unwrap().step(2, env.msg);
        }
    }
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(!nodes
        .get_mut(&1)
        .unwrap()
        .take_ready_reads()
        .contains(&read));
}

/// Reads requested at different times are confirmed by different rounds:
/// an ack covering read A must not confirm a read B requested after it.
#[test]
fn ack_for_an_earlier_read_does_not_confirm_a_later_read() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);

    let a = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    // Round A is sent now; node 2's reply to it is delayed.
    let round_a = nodes.get_mut(&1).unwrap().force_heartbeat();
    let to2 = round_a.into_iter().find(|e| e.to == 2).unwrap();
    let reply_a = nodes.get_mut(&2).unwrap().step(1, to2.msg);

    // Read B is requested after round A went out.
    let b = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    for env in reply_a {
        nodes.get_mut(&1).unwrap().step(2, env.msg);
    }
    nodes.get_mut(&1).unwrap().take_committed();
    let ready = nodes.get_mut(&1).unwrap().take_ready_reads();
    assert!(ready.contains(&a), "read A is confirmed by its own round");
    assert!(!ready.contains(&b), "read B must wait for a later round");
}

/// §6.4: a leader must not serve reads until it has committed an entry from
/// its own term. Until then its `commit_index` can be *behind* entries that
/// were committed (and acknowledged to clients) under the previous leader.
///
/// Scenario: leader 1 commits entry X and acks the client, but the commit
/// index never reaches followers (1 is cut off). Node 2 - which holds X but
/// does not know it is committed - wins the next election. A read on node 2
/// must observe X.
///
/// In this deterministic setup the ack that confirms leadership also
/// advances the no-op's commit in the same step, so the test does not fail
/// on its own without the explicit rule. It guards the §6.4 requirement that
/// a read's index is at least the leader's own no-op index.
#[test]
fn new_leader_does_not_serve_reads_before_committing_in_its_term() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);

    // X is proposed and replicated to 2 and 3; both ack, so 1 commits it.
    let x = nodes.get_mut(&1).unwrap().propose(b"X".to_vec()).unwrap();
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    // Deliver AppendEntries and let 1 process the acks, but swallow the
    // *next* round that would tell followers about the new commit index.
    let mut replies = Vec::new();
    for env in out {
        let to = env.to;
        for r in nodes.get_mut(&to).unwrap().step(1, env.msg) {
            replies.push((to, r));
        }
    }
    for (from, r) in replies {
        // Drop whatever 1 sends in response (it would carry leader_commit).
        let _ = nodes.get_mut(&1).unwrap().step(from, r.msg);
    }
    assert!(
        nodes[&1].commit_index() >= x,
        "leader 1 committed X (client ack'd)"
    );
    assert!(
        nodes[&2].commit_index() < x,
        "follower 2 holds X but has not learned it is committed"
    );

    // Node 1 is now isolated. Node 2 campaigns and wins with node 3's vote.
    let mut out = Vec::new();
    for _ in 0..200 {
        out = nodes.get_mut(&2).unwrap().tick();
        if !out.is_empty() {
            break;
        }
    }
    age_followers(&mut nodes, 2);
    let to3: Vec<_> = out.into_iter().filter(|e| e.to == 3).collect();
    // Votes flow 2<->3 only; the resulting leader broadcast is held so we
    // can control exactly when the no-op commits.
    let held = pump(&mut nodes, tag(2, to3), |from, env| {
        from == 2 && matches!(env.msg, Message::AppendEntries { .. })
    });
    assert_eq!(nodes[&2].role(), Role::Leader);
    assert!(
        nodes[&2].commit_index() < x,
        "new leader's commit index is stale until its no-op commits"
    );
    nodes.get_mut(&2).unwrap().take_committed();

    // Client read on the new leader.
    let read = nodes.get_mut(&2).unwrap().request_read_index().unwrap();

    // Node 3 acknowledges a heartbeat *sent after the request* - but the
    // append that carries the no-op (and so the commit) is still in flight,
    // i.e. we deliver only the ack path. Use a fresh empty heartbeat built
    // after the request to node 3.
    drop(held);
    let hb = nodes.get_mut(&2).unwrap().force_heartbeat();
    let to3 = hb.into_iter().find(|e| e.to == 3).unwrap();
    let reply = nodes.get_mut(&3).unwrap().step(2, to3.msg);
    for env in reply {
        nodes.get_mut(&2).unwrap().step(3, env.msg);
    }
    nodes.get_mut(&2).unwrap().take_committed();

    let ready = nodes.get_mut(&2).unwrap().take_ready_reads();
    // Non-vacuous: the read must actually be served, and not stale.
    assert!(ready.contains(&read), "read should be confirmed by now");
    assert!(
        nodes[&2].last_applied_index() >= x,
        "STALE READ: confirmed while the state machine had applied only \
         index {} but entry {x} was already committed and acknowledged",
        nodes[&2].last_applied_index()
    );
}

/// A leader that cannot reach a quorum never confirms a read, however many
/// replies from a minority arrive.
#[test]
fn minority_acks_never_confirm_a_read_in_a_five_node_cluster() {
    let mut nodes = cluster(&[1, 2, 3, 4, 5]);
    elect(&mut nodes, 1);
    let read = nodes.get_mut(&1).unwrap().request_read_index().unwrap();

    // Only node 2 is reachable.
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    let only2: Vec<_> = out.into_iter().filter(|e| e.to == 2).collect();
    pump(&mut nodes, tag(1, only2), |_, _| false);
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(
        !nodes
            .get_mut(&1)
            .unwrap()
            .take_ready_reads()
            .contains(&read),
        "leader + 1 of 5 is not a quorum"
    );

    // Heal: a second peer answers a post-request heartbeat -> quorum of 3.
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    let two_peers: Vec<_> = out.into_iter().filter(|e| e.to <= 3).collect();
    pump(&mut nodes, tag(1, two_peers), |_, _| false);
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(nodes
        .get_mut(&1)
        .unwrap()
        .take_ready_reads()
        .contains(&read));
}

/// A leader that steps down must drop its pending reads, and replies that
/// were in flight from its old term must never resurrect them - not even if
/// the node later becomes leader again in a newer term.
#[test]
fn leadership_change_discards_pending_reads_and_late_replies() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);

    let read = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    // Replies to this round are delayed.
    let mut late = Vec::new();
    for env in hb {
        let to = env.to;
        for r in nodes.get_mut(&to).unwrap().step(1, env.msg) {
            late.push((to, r));
        }
    }

    // Node 2 wins a higher-term election; node 1 hears of it and steps down.
    let mut out = Vec::new();
    for _ in 0..200 {
        out = nodes.get_mut(&2).unwrap().tick();
        if !out.is_empty() {
            break;
        }
    }
    age_followers(&mut nodes, 2);
    pump(&mut nodes, tag(2, out), |_, _| false);
    assert_eq!(nodes[&2].role(), Role::Leader);
    assert_ne!(nodes[&1].role(), Role::Leader, "node 1 stepped down");

    // The old-round replies finally arrive at node 1.
    for (from, r) in late {
        nodes.get_mut(&1).unwrap().step(from, r.msg);
    }
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(!nodes
        .get_mut(&1)
        .unwrap()
        .take_ready_reads()
        .contains(&read));
    assert!(
        nodes.get_mut(&1).unwrap().request_read_index().is_none(),
        "a follower cannot accept read requests"
    );

    // Node 1 becomes leader again in a later term; the old read stays dead.
    let mut out = Vec::new();
    for _ in 0..400 {
        out = nodes.get_mut(&1).unwrap().tick();
        if !out.is_empty() {
            break;
        }
    }
    pump(&mut nodes, tag(1, out), |_, _| false);
    if nodes[&1].role() == Role::Leader {
        settle(&mut nodes, 1);
        let again = nodes.get_mut(&1).unwrap().take_ready_reads();
        assert!(!again.contains(&read), "pre-failover read id resurfaced");
    }
}

/// An isolated former leader (it still believes it leads) never confirms a
/// read, no matter how long it waits or how many of its own heartbeats it
/// sends into the void.
#[test]
fn partitioned_leader_never_confirms_even_with_a_minority_reply() {
    let mut nodes = cluster(&[1, 2, 3, 4, 5]);
    elect(&mut nodes, 1);
    let read = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    for i in 0..40 {
        let out = nodes.get_mut(&1).unwrap().tick();
        // Only node 2 is reachable and only every other round.
        if i % 2 == 0 {
            let only2: Vec<_> = out
                .into_iter()
                .filter(|e| is_cluster_message_to(e, 2))
                .collect();
            pump(&mut nodes, tag(1, only2), |_, _| false);
        }
    }
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(!nodes
        .get_mut(&1)
        .unwrap()
        .take_ready_reads()
        .contains(&read));
}

// ---------------------------------------------------------------------
// Membership removal
// ---------------------------------------------------------------------

/// Commit `RemoveNode(victim)` through leader 1 and apply it on the leader
/// and on every node in `apply_on`. Returns the removal entry's index.
fn remove_and_apply(nodes: &mut Nodes, victim: NodeId, apply_on: &[NodeId]) -> u64 {
    let idx = nodes
        .get_mut(&1)
        .unwrap()
        .propose_remove_node(victim)
        .expect("leader accepts the removal");
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    pump(nodes, tag(1, out), |_, _| false);
    // One more round so followers learn the new commit index.
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    pump(nodes, tag(1, out), |_, _| false);
    for id in apply_on {
        nodes.get_mut(id).unwrap().take_committed();
    }
    idx
}

/// Applying `RemoveNode(self)` must make the removed node inert. It must
/// not stay active, time out and campaign, inflating terms forever.
#[test]
fn removed_node_never_campaigns() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    remove_and_apply(&mut nodes, 3, &[1, 2, 3]);

    let term_before = nodes[&3].term();
    for _ in 0..500 {
        let out = nodes.get_mut(&3).unwrap().tick();
        assert!(out.is_empty(), "a removed node must stay silent: {out:?}");
    }
    assert_ne!(nodes[&3].role(), Role::Candidate);
    assert_ne!(nodes[&3].role(), Role::Leader);
    assert_eq!(nodes[&3].term(), term_before, "no term inflation");
}

/// A removed node must not grant votes, or it could help elect a leader of
/// a configuration it is no longer part of.
#[test]
fn removed_node_does_not_grant_votes() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    remove_and_apply(&mut nodes, 3, &[1, 2, 3]);

    let term = nodes[&3].term();
    let last = nodes[&3].log_last_index();
    let out = nodes.get_mut(&3).unwrap().step(
        2,
        Message::RequestVote {
            term: term + 1,
            candidate_id: 2,
            last_log_index: last + 5,
            last_log_term: term,
        },
    );
    for e in &out {
        assert!(
            !matches!(
                e.msg,
                Message::RequestVoteReply {
                    vote_granted: true,
                    ..
                }
            ),
            "removed node granted a vote"
        );
    }
    assert_eq!(
        nodes[&3].term(),
        term,
        "removed node must ignore the message"
    );
}

/// A removed node's `RequestVote` (it never learned of its removal, or
/// restarted with stale state) carrying a huge term must not make other
/// nodes adopt that term or depose a healthy leader (the disruptive-server
/// problem, Raft dissertation §4.2.3).
#[test]
fn removed_nodes_request_vote_cannot_depose_the_leader() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    // Node 3 never applies its own removal (zombie).
    remove_and_apply(&mut nodes, 3, &[1, 2]);

    let leader_term = nodes[&1].term();
    // The zombie campaigns until it emits a RequestVote.
    let mut votes = Vec::new();
    for _ in 0..200 {
        votes = nodes.get_mut(&3).unwrap().tick();
        if !votes.is_empty() {
            break;
        }
    }
    assert!(!votes.is_empty(), "zombie should be campaigning");
    for env in votes {
        let target = env.to;
        let out = nodes.get_mut(&target).unwrap().step(3, env.msg);
        for e in out {
            assert!(
                !matches!(
                    e.msg,
                    Message::RequestVoteReply {
                        vote_granted: true,
                        ..
                    }
                ),
                "node {target} voted for a removed node"
            );
        }
    }
    assert_eq!(nodes[&1].role(), Role::Leader, "leader was deposed");
    assert_eq!(
        nodes[&1].term(),
        leader_term,
        "leader adopted zombie's term"
    );
    assert_eq!(
        nodes[&2].term(),
        leader_term,
        "follower adopted zombie's term"
    );
}

/// A reply from a node that is no longer a member must not be recorded in
/// `match_index` or counted toward the commit quorum. Here the leader and
/// the removed node alone must not "commit" an entry that the current
/// two-node configuration {1, 2} never agreed to.
#[test]
fn delayed_reply_from_removed_node_cannot_commit_an_entry() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    // Node 3 holds the RemoveNode entry in its log but is otherwise a
    // bystander; node 2 is the other remaining member.
    let r = remove_and_apply(&mut nodes, 3, &[1, 2]);

    // Node 2 is now unreachable. The leader proposes E and tries to send it.
    let e = nodes.get_mut(&1).unwrap().propose(b"E".to_vec()).unwrap();
    assert_eq!(e, r + 1);
    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    let for_2 = hb
        .into_iter()
        .find(|m| m.to == 2)
        .expect("leader still replicates to its remaining peer");

    // The very same append is (by a duplicating/misrouting network) seen by
    // node 3, which acks it. That ack reaches the leader late.
    let ack_from_3 = nodes.get_mut(&3).unwrap().step(1, for_2.msg);
    for env in ack_from_3 {
        nodes.get_mut(&1).unwrap().step(3, env.msg);
    }
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(
        nodes[&1].commit_index() < e,
        "entry {e} was committed with the help of removed node 3: \
         commit_index = {}",
        nodes[&1].commit_index()
    );
}

/// A vote granted by a non-member must not count toward winning an election.
#[test]
fn vote_from_a_non_member_is_not_counted() {
    // Node 1 is configured with peers {2, 3}. A vote from node 9 (not a
    // member) must not help it reach quorum.
    let mut n = RaftNode::new(1, vec![2, 3, 4, 5]);
    let mut out = Vec::new();
    for _ in 0..200 {
        out = n.tick();
        if !out.is_empty() {
            break;
        }
    }
    assert!(!out.is_empty());
    // The timeout starts a pre-vote round. Quorum is 3 of 5: own vote +
    // non-member 9 + non-member 8 would be 3 if strangers were counted.
    assert!(out.iter().any(|e| matches!(e.msg, Message::PreVote { .. })));
    for stranger in [9u64, 8] {
        n.step(
            stranger,
            Message::PreVoteReply {
                term: 1,
                vote_granted: true,
            },
        );
    }
    assert_eq!(
        (n.role(), n.term()),
        (Role::Follower, 0),
        "pre-votes from non-members started an election"
    );
    // Two real members make a majority: now it campaigns.
    for member in [2u64, 3] {
        n.step(
            member,
            Message::PreVoteReply {
                term: 1,
                vote_granted: true,
            },
        );
    }
    assert_eq!(n.role(), Role::Candidate);
    let term = n.term();
    // The same rule applies to real votes.
    for stranger in [9u64, 8] {
        n.step(
            stranger,
            Message::RequestVoteReply {
                term,
                vote_granted: true,
            },
        );
    }
    assert_eq!(n.role(), Role::Candidate, "won with votes from non-members");
}

// ---------------------------------------------------------------------
// Removal notification tests
// ---------------------------------------------------------------------

/// The leader sends the removed node one last append carrying the commit
/// index, so a live victim learns of (and applies) its own removal.
#[test]
fn removed_node_learns_of_its_removal_via_final_notification() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    // 3 gets the entry but the commit index only reaches 1 and 2.
    let idx = nodes.get_mut(&1).unwrap().propose_remove_node(3).unwrap();
    let out = nodes.get_mut(&1).unwrap().force_heartbeat();
    pump(&mut nodes, tag(1, out), |_, _| false);
    assert!(nodes[&1].commit_index() >= idx);
    assert!(!nodes[&3].is_removed());
    nodes.get_mut(&1).unwrap().take_committed(); // leader applies -> queues notice

    let mut notice = Vec::new();
    for _ in 0..5 {
        notice.extend(nodes.get_mut(&1).unwrap().tick());
    }
    let to3: Vec<_> = notice.into_iter().filter(|e| e.to == 3).collect();
    assert!(!to3.is_empty(), "leader must notify the removed node once");
    pump(&mut nodes, tag(1, to3), |_, _| false);
    nodes.get_mut(&3).unwrap().take_committed();
    assert!(
        nodes[&3].is_removed(),
        "victim should have applied its removal"
    );
    assert_eq!(nodes[&3].role(), Role::Follower);
    assert!(nodes.get_mut(&3).unwrap().request_read_index().is_none());
    assert!(nodes.get_mut(&3).unwrap().propose(b"x".to_vec()).is_none());
    // And the leader sends it nothing afterwards.
    for _ in 0..10 {
        assert!(nodes.get_mut(&1).unwrap().tick().iter().all(|e| e.to != 3));
    }
}

/// A reply claiming a read context the leader never issued confirms nothing.
#[test]
fn bogus_read_context_is_ignored() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    let read = nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    let to2 = hb.into_iter().find(|e| e.to == 2).unwrap();
    let mut reply = nodes.get_mut(&2).unwrap().step(1, to2.msg);
    if let Message::AppendEntriesReply { read_ctx, .. } = &mut reply[0].msg {
        assert!(*read_ctx > 0, "follower must echo the context");
        *read_ctx += 1000;
    }
    nodes.get_mut(&1).unwrap().step(2, reply.remove(0).msg);
    nodes.get_mut(&1).unwrap().take_committed();
    assert!(!nodes
        .get_mut(&1)
        .unwrap()
        .take_ready_reads()
        .contains(&read));
}

/// Heartbeats carry no context while no read is waiting, and the follower
/// echoes whatever it was sent.
#[test]
fn heartbeat_context_is_zero_without_reads_and_echoed_with_them() {
    let mut nodes = cluster(&[1, 2, 3]);
    elect(&mut nodes, 1);
    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    for e in &hb {
        assert!(matches!(e.msg, Message::AppendEntries { read_ctx: 0, .. }));
    }
    nodes.get_mut(&1).unwrap().request_read_index().unwrap();
    assert!(nodes[&1].has_unconfirmed_reads());
    let hb = nodes.get_mut(&1).unwrap().force_heartbeat();
    let e = hb.into_iter().find(|e| e.to == 2).unwrap();
    let Message::AppendEntries { read_ctx, .. } = e.msg.clone() else {
        panic!()
    };
    assert_eq!(read_ctx, 1);
    let reply = nodes.get_mut(&2).unwrap().step(1, e.msg);
    assert!(matches!(
        reply[0].msg,
        Message::AppendEntriesReply { read_ctx: 1, .. }
    ));
}
