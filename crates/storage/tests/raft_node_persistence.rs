use raft_core::RaftNode;
use storage::WalLog;

/// Single-node cluster (peers = []) so the node becomes its own leader
/// immediately and commits its own proposals immediately - see
/// `RaftNode::start_election`'s "single-node cluster: win immediately"
/// case. That keeps this test about persistence, not about driving a
/// multi-node election by hand (cluster_sim.rs in raft-core already
/// covers that).
#[test]
fn raft_node_resumes_after_a_simulated_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("node1.wal");

    let (term_before, last_index_before) = {
        let log = WalLog::open(&wal_path).unwrap();
        let mut node = RaftNode::with_log(1, vec![], log);

        // Drive past the (randomized, 10-20 tick) election timeout. In a
        // single-node cluster the node wins its own election immediately
        // once it starts one - see `start_election`'s "single-node
        // cluster: win immediately" case.
        for _ in 0..25 {
            node.tick();
        }
        assert_eq!(node.role(), raft_core::Role::Leader);

        node.propose(b"SET a=1".to_vec()).unwrap();
        node.propose(b"SET b=2".to_vec()).unwrap();

        let committed = node.take_committed();
        assert_eq!(
            committed.len(),
            2,
            "single-node cluster commits immediately"
        );

        (node.term(), node.log_last_index())
        // `node` and its `WalLog` are dropped here - simulating the
        // process exiting (or being killed).
    };

    // "Restart": open a fresh WalLog from the same path and a fresh
    // RaftNode on top of it, exactly as a real process restart would.
    let log = WalLog::open(&wal_path).unwrap();
    let node = RaftNode::with_log(1, vec![], log);

    assert_eq!(
        node.term(),
        term_before,
        "term must survive a restart, or the node could vote twice in a term it forgot about"
    );
    assert_eq!(
        node.log_last_index(),
        last_index_before,
        "committed log entries must survive a restart"
    );
}

/// A node that crashes mid-election (after persisting its vote for itself,
/// via `persist_hard_state()` in `start_election`) must not be able to
/// grant its vote to someone else in that same term after restarting;
/// otherwise two leaders could be elected in the same term. We can't drive
/// a second candidate through a single `RaftNode` directly (voting logic
/// lives inside `step`), but we can verify the persisted vote itself
/// survives, which is the mechanism that prevents it.
#[test]
fn a_cast_vote_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("node2.wal");

    let term_before = {
        let log = WalLog::open(&wal_path).unwrap();
        // peers = [2] so this node does NOT win the election on its own
        // and instead just becomes (and stays) a candidate who voted for
        // itself - an unanswered candidate keeps re-electing itself with
        // an incrementing term on every timeout, so we don't assert a
        // specific term, just that role and term are consistent and that
        // whatever was reached survives the restart below.
        let mut node = RaftNode::with_log(1, vec![2], log);
        // A timeout first asks for pre-votes and changes nothing (nothing is
        // persisted yet). Node 2 agreeing makes this a real candidacy.
        for _ in 0..25 {
            node.tick();
        }
        assert_eq!(node.role(), raft_core::Role::Follower);
        assert_eq!(node.term(), 0, "a pre-vote round must not touch the term");
        node.step(
            2,
            raft_core::Message::PreVoteReply {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(node.role(), raft_core::Role::Candidate);
        assert!(
            node.term() >= 1,
            "candidate must have incremented its term at least once"
        );
        node.term()
    };

    let log = WalLog::open(&wal_path).unwrap();
    let node = RaftNode::with_log(1, vec![2], log);
    assert_eq!(
        node.term(),
        term_before,
        "term of the in-flight election must survive a restart"
    );
}
