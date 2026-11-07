use raft_core::{LogStore, RaftNode};
use storage::WalLog;

/// Compact a node's log, restart it, and confirm the snapshot boundary
/// and remaining log both survived - the disk-level equivalent of
/// `raft-core`'s `lagging_follower_catches_up_via_install_snapshot_after_compaction`.
#[test]
fn compaction_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("node.wal");

    let (index_before, term_before, log_last_index_before) = {
        let log = WalLog::open(&wal_path).unwrap();
        let mut node = RaftNode::with_log(1, vec![], log);

        for _ in 0..25 {
            node.tick(); // cross the election timeout; single-node cluster wins immediately
        }
        assert_eq!(node.role(), raft_core::Role::Leader);

        for i in 0..5 {
            node.propose(format!("SET k{i}={i}").into_bytes()).unwrap();
        }
        let committed = node.take_committed();
        assert_eq!(committed.len(), 5);

        let index = node
            .compact_log(b"kv-state-after-5-writes".to_vec())
            .expect("node should have applied entries to compact");

        (index, node.term(), node.log_last_index())
        // node + its WalLog drop here, simulating a crash right after compaction
    };

    // Reopen the raw WalLog directly (bypassing RaftNode) to check the
    // storage layer persisted the snapshot boundary correctly.
    let log = WalLog::open(&wal_path).unwrap();
    assert_eq!(log.last_included_index(), index_before);
    assert_eq!(log.snapshot_data(), Some(&b"kv-state-after-5-writes"[..]));

    // And through RaftNode, term/log length must also be exactly as before.
    let node = RaftNode::with_log(1, vec![], log);
    assert_eq!(node.term(), term_before);
    assert_eq!(node.log_last_index(), log_last_index_before);
}
