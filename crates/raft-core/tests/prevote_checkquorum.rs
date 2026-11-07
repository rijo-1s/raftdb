#![allow(dead_code)] // the simulator harness below is shared with cluster_sim.rs
use std::collections::{HashMap, VecDeque};

use raft_core::{Envelope, Message, NodeId, RaftNode, Role};

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

// ---------------------------------------------------------------------------
// PreVote and CheckQuorum
// ---------------------------------------------------------------------------

fn leader_of(c: &Cluster) -> NodeId {
    let l = c.leaders();
    assert_eq!(l.len(), 1, "expected exactly one leader, got {l:?}");
    l[0]
}

fn a_follower(c: &Cluster) -> NodeId {
    let leader = leader_of(c);
    *c.nodes.keys().find(|&&id| id != leader).unwrap()
}

/// Why PreVote exists: a follower that stops hearing the leader (stalled,
/// or cut off) must not bump its term at every timeout, or it deposes a
/// healthy leader when it returns.
#[test]
fn a_cut_off_follower_never_raises_its_term_and_does_not_disturb_the_leader_on_return() {
    for _ in 0..20 {
        let mut c = Cluster::new(3);
        c.run_ticks(30);
        let leader = leader_of(&c);
        let term = c.nodes[&leader].term();
        let f = a_follower(&c);

        c.partition(f);
        c.run_ticks(200); // ten election timeouts of silence
        assert_eq!(
            c.nodes[&f].term(),
            term,
            "a cut-off node must not inflate its term"
        );
        assert_eq!(c.nodes[&f].role(), Role::Follower);
        assert_eq!(c.leaders(), vec![leader]);

        c.heal(f);
        c.run_ticks(40);
        assert_eq!(c.leaders(), vec![leader], "the healthy leader was deposed");
        assert_eq!(
            c.nodes[&leader].term(),
            term,
            "the term must not have moved"
        );
        assert_eq!(c.nodes[&f].term(), term);
    }
}

#[test]
fn a_follower_with_a_live_leader_refuses_pre_votes_and_changes_nothing() {
    let mut c = Cluster::new(3);
    c.run_ticks(30);
    let f = a_follower(&c);
    let term = c.nodes[&f].term();
    let rival = *c
        .nodes
        .keys()
        .find(|&&id| id != f && id != leader_of(&c))
        .unwrap();
    let node = c.nodes.get_mut(&f).unwrap();
    let out = node.step(
        rival,
        Message::PreVote {
            term: term + 1,
            candidate_id: rival,
            last_log_index: u64::MAX,
            last_log_term: u64::MAX,
        },
    );
    assert!(
        matches!(
            out[0].msg,
            Message::PreVoteReply { vote_granted: false, term: t } if t == term
        ),
        "{out:?}"
    );
    assert_eq!(
        node.term(),
        term,
        "answering a pre-vote must never change the term"
    );
}

#[test]
fn granting_a_pre_vote_changes_neither_term_nor_vote() {
    let mut node = RaftNode::new(1, vec![2, 3]);
    let out = node.step(
        2,
        Message::PreVote {
            term: 5,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
    );
    assert!(matches!(
        out[0].msg,
        Message::PreVoteReply {
            vote_granted: true,
            term: 5
        }
    ));
    assert_eq!(node.term(), 0, "the responder's term must not move");
    // It can still vote for someone else in term 1: it never voted.
    let out = node.step(
        3,
        Message::RequestVote {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        },
    );
    assert!(matches!(
        out[0].msg,
        Message::RequestVoteReply {
            vote_granted: true,
            ..
        }
    ));
}

#[test]
fn a_pre_vote_is_refused_for_a_stale_term_or_a_stale_log() {
    let mut c = Cluster::new(3);
    c.run_ticks(30);
    let f = a_follower(&c);
    let rival = *c
        .nodes
        .keys()
        .find(|&&id| id != f && id != leader_of(&c))
        .unwrap();
    // Let F lose track of the leader so only the term and log rules decide.
    c.partition(f);
    c.run_ticks(30);
    let node = c.nodes.get_mut(&f).unwrap();
    let term = node.term();
    let ask = |node: &mut RaftNode, term, idx, lt| {
        node.step(
            rival,
            Message::PreVote {
                term,
                candidate_id: rival,
                last_log_index: idx,
                last_log_term: lt,
            },
        )
    };
    let granted = |out: &[Envelope]| {
        matches!(
            out[0].msg,
            Message::PreVoteReply {
                vote_granted: true,
                ..
            }
        )
    };
    assert!(
        !granted(&ask(node, term + 1, 0, 0)),
        "an empty log is behind F's log"
    );
    assert!(
        !granted(&ask(node, term, u64::MAX, u64::MAX)),
        "a term that is not above F's"
    );
    assert!(
        granted(&ask(node, term + 1, u64::MAX, u64::MAX)),
        "up-to-date, newer, no live leader"
    );
    assert_eq!(node.term(), term);
}

#[test]
fn a_leader_cut_off_from_the_majority_steps_down_by_itself_and_keeps_its_vote() {
    let mut c = Cluster::new(3);
    c.run_ticks(30);
    let leader = leader_of(&c);
    let term = c.nodes[&leader].term();
    c.partition(leader);
    c.run_ticks(250);
    let node = &c.nodes[&leader];
    assert_ne!(
        node.role(),
        Role::Leader,
        "CheckQuorum: it must stop claiming leadership"
    );
    assert_eq!(node.term(), term, "stepping down must not change the term");

    // Safety: it voted for itself in this term. If stepping down forgot that
    // it could vote for a rival in the SAME term and two leaders could exist.
    let rival = *c.nodes.keys().find(|&&id| id != leader).unwrap();
    let node = c.nodes.get_mut(&leader).unwrap();
    let out = node.step(
        rival,
        Message::RequestVote {
            term,
            candidate_id: rival,
            last_log_index: u64::MAX,
            last_log_term: u64::MAX,
        },
    );
    assert!(
        matches!(
            out[0].msg,
            Message::RequestVoteReply {
                vote_granted: false,
                ..
            }
        ),
        "a former leader granted a second vote in its own term: {out:?}"
    );
}

#[test]
fn a_leader_that_keeps_a_majority_never_steps_down() {
    let mut c = Cluster::new(5);
    c.run_ticks(30);
    let leader = leader_of(&c);
    let term = c.nodes[&leader].term();
    let mut others = c.nodes.keys().copied().filter(|&i| i != leader);
    let (a, b) = (others.next().unwrap(), others.next().unwrap());
    c.partition(a);
    c.partition(b);
    c.run_ticks(300);
    assert_eq!(c.leaders(), vec![leader]);
    assert_eq!(c.nodes[&leader].term(), term);
}

#[test]
fn a_one_node_cluster_still_elects_itself_without_asking_anyone() {
    let mut n = RaftNode::new(1, vec![]);
    for _ in 0..100 {
        n.tick();
    }
    assert_eq!(n.role(), Role::Leader);
    for _ in 0..200 {
        n.tick();
    }
    assert_eq!(
        n.role(),
        Role::Leader,
        "CheckQuorum must not depose a single node"
    );
}

/// After the leader dies, the survivors still elect a new one promptly, even
/// when every follower times out at about the same moment.
#[test]
fn survivors_elect_a_new_leader_after_the_leader_dies_even_with_simultaneous_timeouts() {
    for _ in 0..200 {
        let mut c = Cluster::new(5);
        c.run_ticks(30);
        let leader = leader_of(&c);
        c.partition(leader);
        c.run_ticks(60);
        assert_eq!(
            c.leaders().len(),
            1,
            "no leader emerged among the survivors"
        );
    }
}
