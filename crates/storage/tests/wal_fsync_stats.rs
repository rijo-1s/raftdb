//! The WAL's process-wide fsync statistics.
//! The counters are global, so every test takes `LOCK` and asserts on exact
//! deltas.

use std::sync::Mutex;

use raft_core::{EntryKind, LogEntry};
use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::Wal;

static LOCK: Mutex<()> = Mutex::new(());

fn entry(index: u64, data: &[u8]) -> LogEntry {
    LogEntry {
        term: 1,
        index,
        kind: EntryKind::Data,
        data: data.to_vec(),
    }
}

/// On-disk size of one frame: 8-byte header + 29 fixed + data.
fn frame(e: &LogEntry) -> u64 {
    8 + 29 + e.data.len() as u64
}

fn wal() -> (tempfile::TempDir, Wal) {
    let d = tempfile::tempdir().unwrap();
    let w = Wal::open(d.path().join("wal.log")).unwrap();
    (d, w)
}

#[test]
fn a_batch_is_one_fsync_with_exact_entry_and_byte_counts() {
    let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_d, mut w) = wal();
    let b = Wal::fsync_stats();
    let batch = [entry(1, b"aaaa"), entry(2, b""), entry(3, &[7u8; 100])];
    w.append_batch(&batch).unwrap();
    let a = Wal::fsync_stats();
    assert_eq!(a.count - b.count, 1, "one group commit = one fsync");
    assert_eq!(a.entries - b.entries, 3);
    assert_eq!(a.bytes - b.bytes, batch.iter().map(frame).sum::<u64>());
    assert_eq!(a.errors, b.errors);
    let new_obs: u64 = a.bucket_counts.iter().sum::<u64>() - b.bucket_counts.iter().sum::<u64>();
    assert_eq!(new_obs, 1, "exactly one histogram observation");
    assert!(a.max_micros >= b.max_micros);
}

#[test]
fn group_commit_shows_up_as_fewer_fsyncs_per_entry() {
    let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_d, mut w) = wal();
    let b = Wal::fsync_stats();
    for i in 1..=5 {
        w.append(&entry(i, b"x")).unwrap();
    }
    let mid = Wal::fsync_stats();
    let five: Vec<_> = (6..=10).map(|i| entry(i, b"x")).collect();
    w.append_batch(&five).unwrap();
    let a = Wal::fsync_stats();
    assert_eq!(mid.count - b.count, 5, "five single appends = five fsyncs");
    assert_eq!(a.count - mid.count, 1, "one batch of five = one fsync");
    assert_eq!(a.entries - b.entries, 10);
}

#[test]
fn histogram_invariants_hold() {
    let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_d, mut w) = wal();
    for i in 1..=20 {
        w.append(&entry(i, b"y")).unwrap();
    }
    let s = Wal::fsync_stats();
    assert_eq!(
        s.bucket_counts.len(),
        s.bounds_micros.len() + 1,
        "one +Inf bucket"
    );
    assert!(
        s.bounds_micros.windows(2).all(|p| p[0] < p[1]),
        "bounds strictly increase"
    );
    assert_eq!(
        s.bucket_counts.iter().sum::<u64>(),
        s.count,
        "buckets add up to the count"
    );
    assert!(s.sum_micros >= s.max_micros);
}

#[test]
fn empty_batches_do_not_fsync_or_count() {
    let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_d, mut w) = wal();
    let b = Wal::fsync_stats();
    w.append_batch(&[]).unwrap();
    assert_eq!(Wal::fsync_stats(), b);
}

#[test]
fn rewrites_are_counted_separately_from_appends() {
    let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_d, mut w) = wal();
    w.append(&entry(1, b"a")).unwrap();
    let b = Wal::fsync_stats();
    w.rewrite(&[entry(1, b"a")]).unwrap();
    let a = Wal::fsync_stats();
    assert_eq!(a.rewrites - b.rewrites, 1);
    assert_eq!(a.count, b.count, "a rewrite is not an append fsync");
    assert_eq!(a.entries, b.entries);
}

#[test]
fn a_failed_fsync_is_an_error_not_a_success_and_poisons_the_handle() {
    let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (_d, mut w) = wal();
    let b = Wal::fsync_stats();
    fault::clear();
    fault::arm(FaultPlan {
        point: FaultPoint::WalAppendSync,
        skip: 0,
        action: FaultAction::FailBefore,
    });
    assert!(w.append(&entry(1, b"doomed")).is_err());
    fault::clear();
    let a = Wal::fsync_stats();
    assert_eq!(a.errors - b.errors, 1);
    assert_eq!(
        a.count, b.count,
        "a failed fsync must not be counted as a durable append"
    );
    assert_eq!(a.entries, b.entries, "its entries were never acknowledged");
    assert_eq!(a.bytes, b.bytes);
    // poisoned: later appends fail fast without touching the counters
    assert!(w.append(&entry(2, b"later")).is_err());
    assert_eq!(Wal::fsync_stats().errors, a.errors);
}
