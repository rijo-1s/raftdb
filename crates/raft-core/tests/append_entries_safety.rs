//! Safety properties around AppendEntries, log truncation, snapshots and
//! batching. Each test pins one failure mode (described in its doc comment)
//! or one batching guarantee.

use raft_core::{
    EntryKind, Envelope, LogEntry, LogStore, MemLog, Message, NodeId, RaftNode, Role, Term,
};

fn entry(index: u64, term: Term, data: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        kind: EntryKind::Data,
        data: data.as_bytes().to_vec(),
    }
}

fn ae(term: Term, prev: (u64, Term), entries: Vec<LogEntry>, leader_commit: u64) -> Message {
    Message::AppendEntries {
        term,
        leader_id: 9,
        prev_log_index: prev.0,
        prev_log_term: prev.1,
        entries,
        leader_commit,
        read_ctx: 0,
    }
}

fn reply_of(out: &[Envelope]) -> (bool, u64) {
    match &out[0].msg {
        Message::AppendEntriesReply {
            success,
            match_index,
            ..
        } => (*success, *match_index),
        other => panic!("expected AppendEntriesReply, got {other:?}"),
    }
}

fn follower() -> RaftNode {
    RaftNode::new(1, vec![9])
}

/// Elect node 1 leader of {1,2,3} in term 1 and return it with its no-op
/// (index 1) still uncommitted.
fn elect_leader() -> RaftNode {
    let mut n = RaftNode::new(1, vec![2, 3]);
    // A timed-out node first asks for pre-votes and changes nothing...
    let mut asked = false;
    for _ in 0..1000 {
        if n.tick()
            .iter()
            .any(|e| matches!(e.msg, Message::PreVote { .. }))
        {
            asked = true;
            break;
        }
    }
    assert!(asked, "an election timeout must start a pre-vote round");
    assert_eq!((n.role(), n.term()), (Role::Follower, 0));
    // ...and campaigns for real once a majority says it would vote yes.
    n.step(
        2,
        Message::PreVoteReply {
            term: 1,
            vote_granted: true,
        },
    );
    assert_eq!(n.role(), Role::Candidate);
    let term = n.term();
    n.step(
        2,
        Message::RequestVoteReply {
            term,
            vote_granted: true,
        },
    );
    assert_eq!(n.role(), Role::Leader);
    n
}

fn success(term: Term, match_index: u64) -> Message {
    Message::AppendEntriesReply {
        term,
        success: true,
        match_index,
        read_ctx: 0,
    }
}

/// `append_entries` must truncate only at a real conflict, not at
/// `prev_index` unconditionally: a delayed or duplicated AppendEntries
/// carrying fewer entries must not erase a suffix the follower has already
/// acknowledged.
#[test]
fn stale_append_entries_does_not_erase_matching_suffix() {
    let mut f = follower();
    let all = vec![entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")];
    f.step(9, ae(1, (0, 0), all.clone(), 0));
    assert_eq!(f.log_last_index(), 3);

    // Old duplicate carrying only the first entry arrives late.
    let out = f.step(9, ae(1, (0, 0), vec![entry(1, 1, "a")], 0));
    assert_eq!(f.log_last_index(), 3, "matching suffix must be retained");
    // It only vouches for what *it* verified.
    assert_eq!(reply_of(&out), (true, 1));
}

#[test]
fn stale_append_entries_with_nonzero_prev_keeps_suffix_too() {
    let mut f = follower();
    let all: Vec<_> = (1..=5).map(|i| entry(i, 1, "x")).collect();
    f.step(9, ae(1, (0, 0), all, 0));
    let out = f.step(9, ae(1, (2, 1), vec![entry(3, 1, "x")], 0));
    assert_eq!(f.log_last_index(), 5);
    assert_eq!(reply_of(&out), (true, 3));
}

#[test]
fn conflicting_entry_truncates_from_the_conflict_only() {
    let mut f = follower();
    let old = vec![entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")];
    f.step(9, ae(1, (0, 0), old, 0));
    // New leader (term 2) agrees on index 1 but replaces 2..
    let new = vec![entry(2, 2, "B"), entry(3, 2, "C"), entry(4, 2, "D")];
    let out = f.step(9, ae(2, (1, 1), new, 0));
    assert_eq!(reply_of(&out), (true, 4));
    assert_eq!(f.log_last_index(), 4);
}

/// Commit index and the reported match index must be bounded by the last
/// entry this RPC *verified*, not by the follower's own log length, or an
/// unverified divergent tail could be committed.
#[test]
fn commit_and_match_are_bounded_by_last_verified_entry() {
    let mut f = follower();
    // Follower holds an uncommitted divergent tail from an old term-1 leader.
    let tail = vec![
        entry(1, 1, "a"),
        entry(2, 1, "stale2"),
        entry(3, 1, "stale3"),
    ];
    f.step(9, ae(1, (0, 0), tail, 0));
    // Term-2 leader heartbeat: only index 1 is verified, leader_commit = 3.
    let out = f.step(9, ae(2, (1, 1), vec![], 3));
    assert_eq!(reply_of(&out), (true, 1));
    assert_eq!(
        f.commit_index(),
        1,
        "must not commit unverified entries 2..3"
    );
}

#[test]
fn stale_append_entries_never_lowers_commit_index() {
    let mut f = follower();
    let all: Vec<_> = (1..=4).map(|i| entry(i, 1, "x")).collect();
    f.step(9, ae(1, (0, 0), all, 4));
    assert_eq!(f.commit_index(), 4);
    f.step(9, ae(1, (0, 0), vec![entry(1, 1, "x")], 1));
    assert_eq!(f.commit_index(), 4);
}

#[test]
fn non_contiguous_entries_are_rejected_not_stored() {
    let mut f = follower();
    let out = f.step(
        9,
        ae(1, (0, 0), vec![entry(1, 1, "a"), entry(3, 1, "c")], 0),
    );
    assert!(!reply_of(&out).0);
    assert_eq!(f.log_last_index(), 0);
}

#[test]
#[should_panic(expected = "overwrite committed entry")]
fn a_leader_overwriting_a_committed_entry_is_fail_stop() {
    let mut f = follower();
    let all = vec![entry(1, 1, "a"), entry(2, 1, "b")];
    f.step(9, ae(1, (0, 0), all, 2)); // both committed
    f.step(9, ae(2, (1, 1), vec![entry(2, 2, "EVIL")], 2));
}

/// A delayed success reply must not move `match_index` backwards.
#[test]
fn match_index_is_monotonic_under_reordered_replies() {
    let mut l = elect_leader();
    let t = l.term();
    l.propose_batch(vec![b"a".to_vec(), b"b".to_vec()]).unwrap();
    l.step(2, success(t, 3));
    assert_eq!(l.debug_match_index(2), Some(3));
    l.step(2, success(t, 1)); // late reply for an older message
    assert_eq!(l.debug_match_index(2), Some(3));
    assert_eq!(l.debug_next_index(2), Some(4));
}

#[test]
fn memlog_install_snapshot_retains_matching_suffix() {
    let mut log = MemLog::new();
    for i in 1..=5 {
        log.append(1, vec![i as u8]);
    }
    log.install_snapshot(3, 1, b"snap".to_vec(), vec![1, 2]);
    assert_eq!(log.last_included_index(), 3);
    assert_eq!(log.last_index(), 5, "entries 4..5 extend the snapshot");
    assert_eq!(log.term_at(4), 1);
}

#[test]
fn memlog_install_snapshot_discards_conflicting_log() {
    let mut log = MemLog::new();
    for i in 1..=5 {
        log.append(1, vec![i as u8]);
    }
    // Snapshot says index 3 has term 2: our term-1 log conflicts with it.
    log.install_snapshot(3, 2, b"snap".to_vec(), vec![1, 2]);
    assert_eq!(log.last_index(), 3);
}

// ---------------------------------------------------------------- batching

#[test]
fn propose_batch_returns_contiguous_range_and_requires_leadership() {
    let mut f = follower();
    assert!(f.propose_batch(vec![b"x".to_vec()]).is_none());

    let mut l = elect_leader();
    let r = l
        .propose_batch(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
        .unwrap();
    assert_eq!(r, 2..5); // index 1 is the leader no-op
    assert_eq!(l.log_last_index(), 4);
    assert_eq!(l.propose_batch(vec![]).unwrap(), 5..5);
}

#[test]
fn batch_is_not_committed_until_a_quorum_acks_it() {
    let mut l = elect_leader();
    let t = l.term();
    l.propose_batch(vec![b"a".to_vec(), b"b".to_vec()]).unwrap();
    assert_eq!(l.commit_index(), 0, "durable locally != committed");
    l.step(2, success(t, 3));
    assert_eq!(l.commit_index(), 3, "leader + one follower = quorum of 3");
    let committed: Vec<_> = l
        .take_committed()
        .into_iter()
        .filter(|c| !c.data.is_empty())
        .map(|c| (c.index, c.data))
        .collect();
    assert_eq!(committed, vec![(2, b"a".to_vec()), (3, b"b".to_vec())]);
}

#[test]
fn single_node_batch_commits_in_order() {
    let mut n = RaftNode::new(1, vec![]);
    while n.role() != Role::Leader {
        n.tick();
    }
    n.propose_batch(vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    let datas: Vec<_> = n
        .take_committed()
        .into_iter()
        .filter(|c| !c.data.is_empty())
        .map(|c| c.data)
        .collect();
    assert_eq!(datas, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
}

fn ae_entries(env: &Envelope) -> &Vec<LogEntry> {
    match &env.msg {
        Message::AppendEntries { entries, .. } => entries,
        other => panic!("expected AppendEntries, got {other:?}"),
    }
}

#[test]
fn replication_is_bounded_by_entry_count_and_streams_the_rest() {
    let mut l = elect_leader();
    let t = l.term();
    l.set_replication_limits(3, usize::MAX);
    l.propose_batch((0..10).map(|i| vec![i]).collect()).unwrap(); // idx 2..=11
    let out = l.force_heartbeat();
    let to2: &Envelope = out.iter().find(|e| e.to == 2).unwrap();
    assert!(ae_entries(to2).len() <= 3);

    // Acking each chunk immediately triggers the next one until caught up.
    let mut sent = ae_entries(to2).len() as u64;
    let mut matched = ae_entries(to2).last().unwrap().index;
    for _ in 0..20 {
        let out = l.step(2, success(t, matched));
        if out.is_empty() {
            break;
        }
        let es = ae_entries(&out[0]);
        assert!(!es.is_empty() && es.len() <= 3);
        assert_eq!(es[0].index, matched + 1, "chunks must be contiguous");
        matched = es.last().unwrap().index;
        sent += es.len() as u64;
    }
    assert_eq!(matched, 11);
    assert_eq!(sent, 11, "every entry sent exactly once, no gaps");
}

#[test]
fn replication_respects_byte_limit_but_always_sends_one_entry() {
    let mut l = elect_leader();
    l.set_replication_limits(1000, 100);
    l.propose_batch(vec![vec![7u8; 400], vec![8u8; 10], vec![9u8; 10]])
        .unwrap();
    let out = l.force_heartbeat();
    let to2 = out.iter().find(|e| e.to == 2).unwrap();
    // The first entry (the 32-byte no-op) fits; the 400-byte entry doesn't
    // fit alongside it, so the chunk ends before it.
    let es = ae_entries(to2);
    assert!(!es.is_empty());
    assert!(es.iter().map(|e| e.data.len() + 32).sum::<usize>() <= 100 || es.len() == 1);

    // An entry bigger than the limit still replicates, alone.
    let t = l.term();
    let out = l.step(2, success(t, es.last().unwrap().index));
    let es2 = ae_entries(&out[0]);
    assert_eq!(es2.len(), 1);
    assert_eq!(es2[0].data.len(), 400);
}

#[test]
fn uncommitted_entries_reports_the_replication_backlog() {
    let mut l = elect_leader();
    assert_eq!(l.uncommitted_entries(), 1);
    l.propose_batch(vec![vec![1], vec![2]]).unwrap();
    assert_eq!(l.uncommitted_entries(), 3);
}

#[allow(dead_code)]
fn _unused(_: NodeId) {}

/// After a restart, a single-node cluster re-elects itself and must advance
/// its commit index, so recovered entries are applied (reads see them)
/// without waiting for a new write to arrive.
#[test]
fn single_node_restart_recommits_recovered_entries_without_a_new_write() {
    let mut log = MemLog::new();
    log.save_hard_state(1, Some(1));
    log.append(1, b"recovered-1".to_vec());
    log.append(1, b"recovered-2".to_vec());

    let mut n = RaftNode::with_log(1, vec![], log);
    assert_eq!(n.commit_index(), 0, "recovery itself commits nothing");
    for _ in 0..1000 {
        if n.role() == Role::Leader {
            break;
        }
        n.tick();
    }
    assert_eq!(n.role(), Role::Leader);
    let applied: Vec<_> = n
        .take_committed()
        .into_iter()
        .filter(|c| !c.data.is_empty())
        .map(|c| c.data)
        .collect();
    assert_eq!(
        applied,
        vec![b"recovered-1".to_vec(), b"recovered-2".to_vec()]
    );
}

/// The no-op must not let a *multi-node* leader commit anything alone.
#[test]
fn multi_node_leader_does_not_self_commit_on_election() {
    let l = elect_leader();
    assert_eq!(l.commit_index(), 0);
}

/// A one-node leader is its own quorum, so a membership entry it appends
/// commits at once. `add-node` on a one-node cluster must not wait for the
/// next client write.
#[test]
fn single_node_add_node_commits_without_a_further_write() {
    let mut n = RaftNode::new(1, vec![]);
    while n.role() != Role::Leader {
        n.tick();
    }
    n.take_committed(); // apply the election no-op: config changes wait for it
    let index = n
        .propose_add_node(2)
        .expect("a one-node leader can add a node");
    assert_eq!(
        n.commit_index(),
        index,
        "the AddNode entry must commit without any further proposal"
    );
    n.take_committed();
    assert!(
        n.peers().any(|p| p == 2),
        "node 2 is a member once the entry is applied"
    );
    // The change is complete, so another one may start straight away.
    assert!(n.propose_add_node(3).is_some());
}

/// A multi-node leader must not commit a membership change on its own: with
/// peers, the entry still needs a majority to acknowledge it.
#[test]
fn multi_node_add_node_still_needs_a_majority() {
    let mut n = elect_leader(); // leader of {1,2,3}, no-op at index 1 uncommitted
    let term = n.term();
    n.step(2, success(term, 1)); // node 2 acknowledges the no-op: {1,2} is a majority
    n.take_committed();
    let before = n.commit_index();
    let index = n
        .propose_add_node(4)
        .expect("the no-op is applied, so a change may start");
    assert!(index > before);
    assert_eq!(
        n.commit_index(),
        before,
        "a multi-node leader must not commit a membership entry alone"
    );
    n.step(3, success(term, index)); // a majority now has it
    assert_eq!(n.commit_index(), index);
}
