//! Deterministic crash/fault-injection tests for the durability layer.
//!
//! A "crash" is simulated by failing a specific filesystem operation
//! (write, fsync, rename, directory fsync) at a specific point - before it
//! takes effect, after it takes effect, or (for writes) halfway through.
//! `WalLog` treats storage errors as fatal and panics, so the test catches
//! the panic, drops the handle, and reopens the directory exactly as a
//! restarted process would.
//!
//! Correctness oracle: `MemLog`, which implements the same `LogStore`
//! contract in memory. After a crash at *any* I/O step the recovered state
//! must equal the reference state either just before or just after the
//! interrupted operation (or, for plain appends, before plus a prefix of
//! the batch) - never anything else, and never an earlier state than the
//! last acknowledged operation.

use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use raft_core::{EntryKind, LogEntry, LogStore, MemLog, NodeId, Term};
use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::{Wal, WalError, WalLog};

// ------------------------------------------------------------------ model

#[derive(Debug, Clone, PartialEq)]
struct Obs {
    lii: u64,
    lit: Term,
    snap: Option<Vec<u8>>,
    peers: Vec<NodeId>,
    entries: Vec<(u64, Term, String, Vec<u8>)>,
    hard: (Term, Option<NodeId>),
}

fn observe<L: LogStore>(l: &L) -> Obs {
    let lii = l.last_included_index();
    Obs {
        lii,
        lit: l.last_included_term(),
        snap: l.snapshot_data().map(|s| s.to_vec()),
        peers: l.snapshot_peers().to_vec(),
        entries: l
            .entries_from(lii + 1)
            .iter()
            .map(|e| (e.index, e.term, format!("{:?}", e.kind), e.data.clone()))
            .collect(),
        hard: l.load_hard_state(),
    }
}

#[derive(Debug, Clone)]
enum Op {
    SaveHard(Term, Option<NodeId>),
    Append(Term, Vec<u8>),
    AppendBatch(Term, Vec<Vec<u8>>),
    AppendEntries(u64, Vec<LogEntry>),
    Compact(u64, Term, Vec<u8>, Vec<NodeId>),
    InstallSnapshot(u64, Term, Vec<u8>, Vec<NodeId>),
}

fn apply<L: LogStore>(l: &mut L, op: &Op) {
    match op {
        Op::SaveHard(t, v) => l.save_hard_state(*t, *v),
        Op::Append(t, d) => {
            l.append(*t, d.clone());
        }
        Op::AppendBatch(t, ds) => {
            l.append_batch(*t, ds.clone());
        }
        Op::AppendEntries(prev, es) => l.append_entries(*prev, es),
        Op::Compact(i, t, d, p) => l.compact(*i, *t, d.clone(), p.clone()),
        Op::InstallSnapshot(i, t, d, p) => l.install_snapshot(*i, *t, d.clone(), p.clone()),
    }
}

fn e(index: u64, term: Term, data: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        kind: EntryKind::Data,
        data: data.as_bytes().to_vec(),
    }
}

/// Exercises every durable code path: hard state, single append, batch
/// append, a conflicting truncation (atomic WAL rewrite), compaction,
/// snapshot install that retains a suffix, and one that discards the log.
fn script() -> Vec<Op> {
    vec![
        Op::SaveHard(1, Some(1)),
        Op::AppendBatch(1, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]), // 1..3
        Op::Append(1, b"d".to_vec()),                                          // 4
        Op::SaveHard(2, Some(2)),
        // Conflicts at index 3 (term 1 vs 2): truncate 3.. and append.
        Op::AppendEntries(2, vec![e(3, 2, "C"), e(4, 2, "D"), e(5, 2, "E")]),
        Op::Compact(3, 2, b"snap-at-3".to_vec(), vec![1, 2, 3]),
        Op::AppendBatch(2, vec![b"f".to_vec(), b"g".to_vec()]), // 6..7
        Op::SaveHard(3, None),
        // Our log holds index 5 with term 2: suffix 6..7 is retained.
        Op::InstallSnapshot(5, 2, b"snap-at-5".to_vec(), vec![1, 2, 3]),
        // Beyond our log: everything is discarded.
        Op::InstallSnapshot(9, 3, b"snap-at-9".to_vec(), vec![1, 2, 3, 4]),
    ]
}

fn mem_after(ops: &[Op]) -> MemLog {
    let mut m = MemLog::new();
    for op in ops {
        apply(&mut m, op);
    }
    m
}

/// Every state recovery is allowed to land in if `ops[i]` was in flight.
fn allowed(ops: &[Op], i: usize) -> Vec<Obs> {
    let before = observe(&mem_after(&ops[..i]));
    let after = observe(&mem_after(&ops[..=i]));
    let mut v = vec![before, after];
    // A batch append is not atomic on disk: any prefix of frames may land.
    if let Op::AppendBatch(t, ds) = &ops[i] {
        for k in 1..ds.len() {
            let mut m = mem_after(&ops[..i]);
            m.append_batch(*t, ds[..k].to_vec());
            v.push(observe(&m));
        }
    }
    v
}

fn wal_path(dir: &Path) -> PathBuf {
    dir.join("node.wal")
}

/// Run `ops` against a real `WalLog` with `plan` armed. Returns the index
/// of the op that crashed (`ops.len()` if none) and whether the plan fired.
fn run(dir: &Path, ops: &[Op], plan: Option<FaultPlan>) -> (usize, bool, bool) {
    fault::clear();
    if let Some(p) = plan {
        fault::arm(p);
    }
    let opened = catch_unwind(AssertUnwindSafe(|| WalLog::open(wal_path(dir))));
    let mut log = match opened {
        Ok(Ok(l)) => l,
        _ => {
            let fired = fault::fired().is_some();
            fault::clear();
            return (0, fired, true); // crashed while creating the node
        }
    };
    for (i, op) in ops.iter().enumerate() {
        if catch_unwind(AssertUnwindSafe(|| apply(&mut log, op))).is_err() {
            let fired = fault::fired().is_some();
            fault::clear();
            return (i, fired, false);
        }
    }
    let fired = fault::fired().is_some();
    fault::clear();
    (ops.len(), fired, false)
}

fn all_actions() -> Vec<FaultAction> {
    vec![
        FaultAction::FailBefore,
        FaultAction::FailAfter,
        FaultAction::PartialWrite(0),
        FaultAction::PartialWrite(1),
        FaultAction::PartialWrite(9),
        FaultAction::PartialWrite(37),
    ]
}

// ------------------------------------------------------------ the big one

/// Crash at *every* durable I/O step of a workload that covers all
/// persistence paths, with every fault action, and verify recovery.
#[test]
fn crash_at_every_io_step_recovers_to_a_valid_state() {
    let ops = script();
    let mut crashes_checked = 0usize;
    let mut points_exercised = std::collections::HashSet::new();

    for &point in FaultPoint::ALL.iter() {
        for action in all_actions() {
            for skip in 0.. {
                let dir = tempfile::tempdir().unwrap();
                let plan = FaultPlan {
                    point,
                    skip,
                    action,
                };
                let (done, fired, crashed_in_open) = run(dir.path(), &ops, Some(plan));
                if !fired {
                    break; // fewer than `skip+1` hits of this point: sweep done
                }
                points_exercised.insert(point);
                crashes_checked += 1;

                // Restart: must always succeed, never refuse or panic.
                let ctx = format!("point={point:?} action={action:?} skip={skip} op#{done}");
                let recovered = WalLog::open(wal_path(dir.path()))
                    .unwrap_or_else(|err| panic!("reopen failed after crash [{ctx}]: {err}"));
                let got = observe(&recovered);

                let ok_states = if crashed_in_open {
                    vec![observe(&MemLog::new())]
                } else {
                    allowed(&ops, done)
                };
                assert!(
                    ok_states.contains(&got),
                    "recovered state is neither pre- nor post-op [{ctx}]\n got: {got:#?}\n allowed: {ok_states:#?}"
                );

                // Recovery is idempotent and the node stays usable.
                drop(recovered);
                let again = WalLog::open(wal_path(dir.path()))
                    .unwrap_or_else(|err| panic!("second reopen failed [{ctx}]: {err}"));
                assert_eq!(observe(&again), got, "second recovery differs [{ctx}]");
                let mut cont = again;
                let term = cont.load_hard_state().0.max(1);
                cont.save_hard_state(term, cont.load_hard_state().1);
                let idx = cont.append(term, b"post-crash".to_vec());
                drop(cont);
                let fin = WalLog::open(wal_path(dir.path())).unwrap_or_else(|err| {
                    panic!("reopen after post-crash append failed [{ctx}]: {err}")
                });
                assert_eq!(fin.entry_at(idx).unwrap().data, b"post-crash", "[{ctx}]");
            }
        }
    }

    // Every injection point that the workload can reach was actually hit
    // (the recovery-truncation points need a torn tail; covered below).
    for p in FaultPoint::ALL {
        if matches!(
            p,
            FaultPoint::WalRecoverTruncate | FaultPoint::WalRecoverSync
        ) {
            continue;
        }
        assert!(
            points_exercised.contains(&p),
            "fault point {p:?} never fired"
        );
    }
    eprintln!("crash_sweep: {crashes_checked} injected crashes, each followed by recovery + invariant checks");
    assert!(
        crashes_checked > 200,
        "only {crashes_checked} crashes checked"
    );
}

/// Same sweep, but a failed append fsync also *loses* the unsynced bytes
/// (power loss rather than process crash).
#[test]
fn power_loss_during_append_fsync_loses_only_the_unacked_batch() {
    let dir = tempfile::tempdir().unwrap();
    let path = wal_path(dir.path());
    fault::clear();
    let mut log = WalLog::open(&path).unwrap();
    log.save_hard_state(1, Some(1));
    log.append_batch(1, vec![b"a".to_vec(), b"b".to_vec()]);
    let acked = observe(&log);
    let durable_len = fs::metadata(&path).unwrap().len();

    fault::arm(FaultPlan {
        point: FaultPoint::WalAppendSync,
        skip: 0,
        action: FaultAction::FailBefore,
    });
    let r = catch_unwind(AssertUnwindSafe(|| {
        log.append_batch(1, vec![b"c".to_vec(), b"d".to_vec()])
    }));
    assert!(
        r.is_err(),
        "append must not be acknowledged after a failed fsync"
    );
    fault::clear();
    drop(log);

    // Un-fsynced pages vanish.
    let f = fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(durable_len).unwrap();
    drop(f);

    let recovered = WalLog::open(&path).unwrap();
    assert_eq!(observe(&recovered), acked);
}

// ------------------------------------------------- targeted crash outcomes

fn seeded(dir: &Path, n: u64) -> WalLog {
    fault::clear();
    let mut log = WalLog::open(wal_path(dir)).unwrap();
    log.save_hard_state(1, Some(1));
    for i in 1..=n {
        log.append(1, vec![i as u8]);
    }
    log
}

fn crash_with<R>(plan: FaultPlan, f: impl FnOnce() -> R) -> bool {
    fault::arm(plan);
    let r = catch_unwind(AssertUnwindSafe(f));
    let fired = fault::fired().is_some();
    fault::clear();
    assert!(fired, "fault never fired: {plan:?}");
    r.is_err()
}

#[test]
fn wal_rewrite_crash_before_rename_keeps_the_complete_old_log() {
    for point in [
        FaultPoint::WalRewriteTmpWrite,
        FaultPoint::WalRewriteTmpSync,
        FaultPoint::WalRewriteRename,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut log = seeded(dir.path(), 5);
        log.save_hard_state(2, None); // a real node adopts the term first
        let before = observe(&log);
        let conflict = vec![e(3, 2, "X"), e(4, 2, "Y")];
        assert!(crash_with(
            FaultPlan {
                point,
                skip: 0,
                action: FaultAction::FailBefore
            },
            || log.append_entries(2, &conflict)
        ));
        drop(log);
        let rec = WalLog::open(wal_path(dir.path())).unwrap();
        assert_eq!(
            observe(&rec),
            before,
            "{point:?}: old log must survive intact"
        );
        // The stale temp file is cleaned up by recovery.
        assert!(!dir.path().join("node.wal.tmp").exists(), "{point:?}");
    }
}

#[test]
fn wal_rewrite_crash_after_rename_yields_the_complete_new_log() {
    for point in [FaultPoint::WalRewriteRename, FaultPoint::WalRewriteDirSync] {
        let dir = tempfile::tempdir().unwrap();
        let mut log = seeded(dir.path(), 5);
        log.save_hard_state(2, None);
        let conflict = vec![e(3, 2, "X"), e(4, 2, "Y")];
        assert!(crash_with(
            FaultPlan {
                point,
                skip: 0,
                action: FaultAction::FailAfter
            },
            || log.append_entries(2, &conflict)
        ));
        drop(log);
        let rec = WalLog::open(wal_path(dir.path())).unwrap();
        let idx: Vec<_> = rec
            .entries_from(1)
            .iter()
            .map(|x| (x.index, x.term))
            .collect();
        assert_eq!(idx, vec![(1, 1), (2, 1), (3, 2), (4, 2)], "{point:?}");
    }
}

/// `Wal::rewrite` must not truncate the live file and then rewrite it in
/// place: a crash mid-rewrite would destroy the whole log.
#[test]
fn torn_wal_rewrite_can_no_longer_destroy_the_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 8);
    log.save_hard_state(2, None);
    let before = observe(&log);
    let conflict = vec![e(2, 2, "X")];
    assert!(crash_with(
        FaultPlan {
            point: FaultPoint::WalRewriteTmpWrite,
            skip: 0,
            action: FaultAction::PartialWrite(10)
        },
        || log.append_entries(1, &conflict)
    ));
    drop(log);
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(observe(&rec), before);
    assert_eq!(rec.last_index(), 8);
}

/// A torn in-place snapshot overwrite must not destroy the previous
/// snapshot, and a bad snapshot read must not be treated as "no snapshot".
#[test]
fn torn_snapshot_publication_keeps_the_previous_snapshot() {
    for action in [
        FaultAction::PartialWrite(5),
        FaultAction::FailBefore,
        FaultAction::FailAfter,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut log = seeded(dir.path(), 6);
        log.compact(2, 1, b"old-snapshot".to_vec(), vec![1, 2]);
        let before = observe(&log);
        let crashed = crash_with(
            FaultPlan {
                point: FaultPoint::SnapshotTmpWrite,
                skip: 0,
                action,
            },
            || log.compact(4, 1, b"new-snapshot".to_vec(), vec![1, 2]),
        );
        assert!(crashed);
        drop(log);
        let rec = WalLog::open(wal_path(dir.path())).unwrap();
        assert_eq!(observe(&rec), before, "{action:?}");
        assert_eq!(rec.snapshot_data(), Some(&b"old-snapshot"[..]));
    }
}

#[test]
fn crash_between_snapshot_publish_and_wal_rewrite_is_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 6);
    assert!(crash_with(
        FaultPlan {
            point: FaultPoint::WalRewriteTmpWrite,
            skip: 0,
            action: FaultAction::FailBefore
        },
        || log.compact(4, 1, b"snap".to_vec(), vec![1, 2])
    ));
    drop(log);
    // Snapshot is durable, WAL still has entries 1..6: open drops 1..4.
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(rec.last_included_index(), 4);
    assert_eq!(rec.last_index(), 6);
    assert_eq!(rec.snapshot_data(), Some(&b"snap"[..]));
    assert_eq!(rec.entry_at(5).unwrap().data, vec![5]);
}

/// A crash between publishing a snapshot that *conflicts* with the local
/// log and rewriting the WAL must not leave the stale conflicting entries
/// attached to the snapshot.
#[test]
fn interrupted_conflicting_install_snapshot_discards_the_stale_log() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 5); // entries 1..5, all term 1
    log.save_hard_state(2, None);
    assert!(crash_with(
        FaultPlan {
            point: FaultPoint::WalRewriteTmpWrite,
            skip: 0,
            action: FaultAction::FailBefore
        },
        // The leader's index 3 has term 2: our term-1 log conflicts.
        || log.install_snapshot(3, 2, b"s".to_vec(), vec![1, 2])
    ));
    drop(log);
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(rec.last_included_index(), 3);
    assert_eq!(
        rec.last_index(),
        3,
        "stale term-1 entries 4..5 must not survive"
    );
}

/// `install_snapshot` must not always clear the log: entries beyond the
/// snapshot that the follower has already acknowledged must be kept.
#[test]
fn install_snapshot_retains_acknowledged_suffix_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 6);
    log.install_snapshot(3, 1, b"s".to_vec(), vec![1, 2]);
    drop(log);
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(rec.last_included_index(), 3);
    assert_eq!(rec.last_index(), 6);
    assert_eq!(rec.entry_at(6).unwrap().data, vec![6]);
}

#[test]
fn hard_state_publication_is_all_or_nothing() {
    for (point, action, expect_new) in [
        (
            FaultPoint::HardStateTmpWrite,
            FaultAction::PartialWrite(3),
            false,
        ),
        (FaultPoint::HardStateTmpSync, FaultAction::FailBefore, false),
        (FaultPoint::HardStateRename, FaultAction::FailBefore, false),
        (FaultPoint::HardStateRename, FaultAction::FailAfter, true),
        (FaultPoint::HardStateDirSync, FaultAction::FailAfter, true),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut log = seeded(dir.path(), 0);
        log.save_hard_state(5, Some(2));
        assert!(crash_with(
            FaultPlan {
                point,
                skip: 0,
                action
            },
            || log.save_hard_state(6, Some(3))
        ));
        drop(log);
        let rec = WalLog::open(wal_path(dir.path())).unwrap();
        let want = if expect_new {
            (6, Some(3))
        } else {
            (5, Some(2))
        };
        assert_eq!(rec.load_hard_state(), want, "{point:?}/{action:?}");
    }
}

#[test]
fn failed_hard_state_write_does_not_update_the_in_memory_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 0);
    log.save_hard_state(5, Some(2));
    assert!(crash_with(
        FaultPlan {
            point: FaultPoint::HardStateTmpSync,
            skip: 0,
            action: FaultAction::FailBefore
        },
        || log.save_hard_state(6, Some(3))
    ));
    // Disk before memory: the handle must not claim a vote it didn't persist.
    assert_eq!(log.load_hard_state(), (5, Some(2)));
}

#[test]
fn wal_handle_is_poisoned_after_a_failed_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(wal_path(dir.path())).unwrap();
    fault::clear();
    fault::arm(FaultPlan {
        point: FaultPoint::WalAppendSync,
        skip: 0,
        action: FaultAction::FailBefore,
    });
    assert!(wal.append(&e(1, 1, "a")).is_err());
    fault::clear();
    // No retry-and-hope after a failed fsync: later writes are refused.
    assert!(matches!(wal.append(&e(2, 1, "b")), Err(WalError::Poisoned)));
    assert!(matches!(wal.rewrite(&[]), Err(WalError::Poisoned)));
}

// ----------------------------------------------------------- group commit

#[test]
fn a_batch_costs_exactly_one_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 0);
    fault::clear();
    log.append_batch(1, (0..50).map(|i| vec![i]).collect());
    assert_eq!(fault::hits(FaultPoint::WalAppendSync), 1);
    assert_eq!(fault::hits(FaultPoint::WalAppendWrite), 1);

    fault::clear();
    for i in 0..50 {
        log.append(1, vec![i]);
    }
    assert_eq!(fault::hits(FaultPoint::WalAppendSync), 50);
    fault::clear();

    drop(log);
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(rec.last_index(), 100);
}

#[test]
fn follower_append_entries_batch_is_one_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 0);
    let es: Vec<LogEntry> = (1..=20).map(|i| e(i, 1, "x")).collect();
    fault::clear();
    log.append_entries(0, &es);
    assert_eq!(fault::hits(FaultPoint::WalAppendSync), 1);
    fault::clear();
}

#[test]
fn torn_batch_recovers_a_whole_prefix_of_frames() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 2);
    assert!(crash_with(
        FaultPlan {
            point: FaultPoint::WalAppendWrite,
            skip: 0,
            action: FaultAction::PartialWrite(100)
        },
        || log.append_batch(1, (0..10).map(|i| vec![i; 20]).collect())
    ));
    drop(log);
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    // Entries 1..2 were acked; whatever whole frames of the batch landed
    // form a contiguous extension, with the torn frame discarded.
    assert!(rec.last_index() >= 2 && rec.last_index() < 12);
    for i in 1..=rec.last_index() {
        assert_eq!(rec.entry_at(i).unwrap().index, i);
    }
}

// ----------------------------------------------- recovery-time faults

#[test]
fn failure_while_truncating_a_torn_tail_is_an_error_not_data_loss() {
    for point in [FaultPoint::WalRecoverTruncate, FaultPoint::WalRecoverSync] {
        let dir = tempfile::tempdir().unwrap();
        let log = seeded(dir.path(), 3);
        drop(log);
        // Append half a frame of garbage: a torn tail.
        let p = wal_path(dir.path());
        let mut bytes = fs::read(&p).unwrap();
        let good_len = bytes.len();
        bytes.extend_from_slice(&[40, 0, 0, 0, 1, 2, 3, 4, 9, 9, 9]);
        fs::write(&p, &bytes).unwrap();

        fault::arm(FaultPlan {
            point,
            skip: 0,
            action: FaultAction::FailBefore,
        });
        let r = WalLog::open(&p);
        fault::clear();
        assert!(r.is_err(), "{point:?}: open must report the failure");
        // Retrying recovers fully.
        let rec = WalLog::open(&p).unwrap();
        assert_eq!(rec.last_index(), 3);
        assert_eq!(fs::metadata(&p).unwrap().len() as usize, good_len);
    }
}

// ---------------------------------------- corruption: explicit, no data loss

fn corrupt_byte(path: &Path, at: usize) {
    let mut b = fs::read(path).unwrap();
    b[at] ^= 0xFF;
    fs::write(path, b).unwrap();
}

#[test]
fn mid_log_corruption_is_an_error_and_the_file_is_left_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(dir.path(), 5);
    drop(log);
    let p = wal_path(dir.path());
    let frame = fs::metadata(&p).unwrap().len() as usize / 5;
    corrupt_byte(&p, frame + 20); // inside frame 2 of 5
    let before = fs::read(&p).unwrap();

    match WalLog::open(&p) {
        Err(WalError::LogCorrupt { .. }) => {}
        other => panic!("expected LogCorrupt, got {:?}", other.map(|_| ())),
    }
    // Recovery must not silently truncate frames 2..5.
    assert_eq!(fs::read(&p).unwrap(), before, "no byte may be modified");
}

#[test]
fn corrupt_last_frame_is_a_torn_tail_and_is_preserved_aside() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(dir.path(), 3);
    drop(log);
    let p = wal_path(dir.path());
    let len = fs::metadata(&p).unwrap().len() as usize;
    corrupt_byte(&p, len - 2);
    let rec = WalLog::open(&p).unwrap();
    assert_eq!(rec.last_index(), 2);
    assert!(dir.path().join("node.wal.discarded-tail").exists());
}

#[test]
fn zero_filled_tail_is_treated_as_a_torn_tail() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(dir.path(), 3);
    drop(log);
    let p = wal_path(dir.path());
    let mut b = fs::read(&p).unwrap();
    b.extend_from_slice(&[0u8; 4096]); // size extended, data never persisted
    fs::write(&p, b).unwrap();
    assert_eq!(WalLog::open(&p).unwrap().last_index(), 3);
}

#[test]
fn crc_valid_but_non_contiguous_frames_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let p = wal_path(dir.path());
    {
        let mut wal = Wal::open(&p).unwrap();
        wal.append(&e(1, 1, "a")).unwrap();
        wal.append(&e(3, 1, "c")).unwrap(); // gap
    }
    let mut wal = Wal::open(&p).unwrap();
    assert!(matches!(wal.recover(), Err(WalError::LogCorrupt { .. })));
}

#[test]
fn decreasing_terms_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let p = wal_path(dir.path());
    {
        let mut wal = Wal::open(&p).unwrap();
        wal.append(&e(1, 5, "a")).unwrap();
        wal.append(&e(2, 4, "b")).unwrap();
    }
    let mut wal = Wal::open(&p).unwrap();
    assert!(matches!(wal.recover(), Err(WalError::LogCorrupt { .. })));
}

type Mutation = Box<dyn Fn(&mut Vec<u8>)>;

fn snapshot_dir() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 6);
    log.compact(4, 1, b"snapshot-bytes".to_vec(), vec![1, 2, 3]);
    drop(log);
    let snap = dir.path().join("node.wal.snapshot");
    let wal = wal_path(dir.path());
    (dir, wal, snap)
}

/// An unreadable or corrupt snapshot must never be silently treated as "no
/// snapshot": that would reset the applied state while the WAL prefix it
/// covered is already gone.
#[test]
fn corrupt_snapshots_are_explicit_errors_and_never_silently_empty() {
    let (_d, wal, snap) = snapshot_dir();
    let pristine = fs::read(&snap).unwrap();

    let mutations: Vec<(&str, Mutation)> = vec![
        ("flip data byte", Box::new(|b| b[45] ^= 1)),
        ("flip index field (header)", Box::new(|b| b[10] ^= 1)),
        ("flip term field (header)", Box::new(|b| b[18] ^= 1)),
        (
            "flip crc",
            Box::new(|b| {
                let n = b.len();
                b[n - 1] ^= 1
            }),
        ),
        ("bad magic", Box::new(|b| b[0] = b'X')),
        ("truncated", Box::new(|b| b.truncate(30))),
        (
            "truncated mid-data",
            Box::new(|b| {
                let n = b.len();
                b.truncate(n - 6)
            }),
        ),
        (
            "trailing garbage",
            Box::new(|b| b.extend_from_slice(b"junk")),
        ),
        ("empty file", Box::new(|b| b.clear())),
        (
            "absurd data_len",
            Box::new(|b| b[26..34].copy_from_slice(&u64::MAX.to_le_bytes())),
        ),
        (
            "absurd peer_count",
            Box::new(|b| b[34..38].copy_from_slice(&u32::MAX.to_le_bytes())),
        ),
    ];
    for (name, mutate) in mutations {
        let mut bytes = pristine.clone();
        mutate(&mut bytes);
        fs::write(&snap, &bytes).unwrap();
        match WalLog::open(&wal) {
            Err(WalError::SnapshotCorrupt(_)) => {}
            other => panic!(
                "{name}: expected SnapshotCorrupt, got {:?}",
                other.map(|_| ())
            ),
        }
        // The bad file is preserved for repair, not deleted or replaced.
        assert_eq!(fs::read(&snap).unwrap(), bytes, "{name}");
    }
    // Restoring the pristine file makes the node recoverable again.
    fs::write(&snap, &pristine).unwrap();
    let rec = WalLog::open(&wal).unwrap();
    assert_eq!(rec.last_included_index(), 4);
    assert_eq!(rec.snapshot_data(), Some(&b"snapshot-bytes"[..]));
    assert_eq!(rec.snapshot_peers(), &[1, 2, 3]);
}

#[test]
fn unreadable_snapshot_is_an_io_error_not_an_empty_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(dir.path(), 2);
    drop(log);
    // A directory where the snapshot file should be: reading it fails with
    // something other than NotFound.
    fs::create_dir(dir.path().join("node.wal.snapshot")).unwrap();
    assert!(matches!(
        WalLog::open(wal_path(dir.path())),
        Err(WalError::Io(_))
    ));
}

#[test]
fn missing_snapshot_with_compacted_wal_is_detected() {
    let (_d, wal, snap) = snapshot_dir();
    fs::remove_file(snap).unwrap();
    // WAL now starts at index 5 with no snapshot to anchor it.
    assert!(matches!(
        WalLog::open(wal),
        Err(WalError::LogInconsistent(_))
    ));
}

#[test]
fn hard_state_older_than_the_log_is_detected() {
    let dir = tempfile::tempdir().unwrap();
    let hs = dir.path().join("node.wal.hardstate");
    let mut log = seeded(dir.path(), 0);
    log.save_hard_state(3, Some(1));
    let old_hs = fs::read(&hs).unwrap();
    log.save_hard_state(5, Some(2));
    log.append(5, b"term5".to_vec());
    drop(log);
    // Simulate a restored-from-backup / rolled-back hard state file.
    fs::write(&hs, old_hs).unwrap();
    assert!(matches!(
        WalLog::open(wal_path(dir.path())),
        Err(WalError::StateInconsistent(_))
    ));
}

#[test]
fn stale_temp_files_from_a_crash_are_ignored_and_removed() {
    let dir = tempfile::tempdir().unwrap();
    let log = seeded(dir.path(), 2);
    drop(log);
    for n in [
        "node.wal.tmp",
        "node.wal.hardstate.tmp",
        "node.wal.snapshot.tmp",
    ] {
        fs::write(dir.path().join(n), b"half-written garbage").unwrap();
    }
    let rec = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(rec.last_index(), 2);
    assert_eq!(rec.load_hard_state(), (1, Some(1)));
    for n in [
        "node.wal.tmp",
        "node.wal.hardstate.tmp",
        "node.wal.snapshot.tmp",
    ] {
        assert!(!dir.path().join(n).exists(), "{n}");
    }
}

#[test]
fn hard_state_never_goes_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 0);
    log.save_hard_state(4, None);
    let r = catch_unwind(AssertUnwindSafe(|| log.save_hard_state(3, None)));
    assert!(r.is_err());
}

#[test]
fn redundant_hard_state_saves_do_not_fsync() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 0);
    log.save_hard_state(2, Some(1));
    fault::clear();
    log.save_hard_state(2, Some(1));
    assert_eq!(fault::hits(FaultPoint::HardStateTmpSync), 0);
    fault::clear();
}

/// After a crash between snapshot publication and the WAL rewrite, `open`
/// must not just ignore the stale covered entries in memory and leave them
/// in the file. The next append would land after them, and the *following*
/// restart would see a non-contiguous WAL and refuse to start.
#[test]
fn interrupted_compaction_is_completed_on_open_so_later_appends_stay_valid() {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 6);
    assert!(crash_with(
        FaultPlan {
            point: FaultPoint::WalRewriteTmpWrite,
            skip: 0,
            action: FaultAction::FailBefore
        },
        || log.compact(4, 1, b"snap".to_vec(), vec![1, 2])
    ));
    drop(log);

    // First restart: finishes the compaction; keep operating.
    let mut log = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!((log.last_included_index(), log.last_index()), (4, 6));
    let idx = log.append(1, b"after".to_vec());
    assert_eq!(idx, 7);
    drop(log);

    // Second restart must still succeed and see everything.
    let log = WalLog::open(wal_path(dir.path())).unwrap();
    assert_eq!(log.last_index(), 7);
    assert_eq!(log.entry_at(5).unwrap().data, vec![5]);
    assert_eq!(log.entry_at(7).unwrap().data, b"after");
}
