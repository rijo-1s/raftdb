//! Recovery ordering and missing-file detection for the Raft persistence
//! layer (`WalLog` = WAL + hard state + snapshot).
//!
//! Rules under test:
//!  * `WalLog::open` validates the WAL, hard state and snapshot *together*
//!    before it modifies anything. If it refuses, every file (including
//!    stale temp files and a torn WAL tail) is byte-for-byte as found.
//!  * A missing WAL is never silently replaced by an empty one when a hard
//!    state or snapshot proves the node has persisted state: the log suffix
//!    (possibly committed, acknowledged entries) may be gone.
//!  * A genuinely fresh node, and a torn tail that is legitimately
//!    recoverable, still open.
//!  * Every phase-2 step (temp removal, torn-tail truncation, interrupted
//!    compaction rewrite) can crash at any point and recovery still
//!    converges to the same state.

use std::collections::BTreeMap;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use raft_core::LogStore;
use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::{WalError, WalLog};

type Image = BTreeMap<String, Vec<u8>>;

fn wal(d: &Path) -> PathBuf {
    d.join("node.wal")
}
fn hs(d: &Path) -> PathBuf {
    d.join("node.wal.hardstate")
}
fn snap(d: &Path) -> PathBuf {
    d.join("node.wal.snapshot")
}

fn image(dir: &Path) -> Image {
    fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| {
            (
                e.file_name().to_string_lossy().to_string(),
                fs::read(e.path()).unwrap(),
            )
        })
        .collect()
}

fn restore(dir: &Path, img: &Image) {
    for e in fs::read_dir(dir).unwrap().flatten() {
        fs::remove_file(e.path()).unwrap();
    }
    for (n, b) in img {
        fs::write(dir.join(n), b).unwrap();
    }
}

/// A node with `n` term-1 entries and a persisted hard state.
fn seeded(dir: &Path, n: u64) -> WalLog {
    fault::clear();
    let mut log = WalLog::open(wal(dir)).unwrap();
    log.save_hard_state(1, Some(1));
    for i in 1..=n {
        log.append(1, vec![i as u8]);
    }
    log
}

/// Snapshot at index 4; the WAL holds entries 5..=`n`.
fn compacted(dir: &Path, n: u64) {
    let mut log = seeded(dir, n);
    log.compact(4, 1, b"snapshot-bytes".to_vec(), vec![1, 2, 3]);
}

#[derive(Debug, PartialEq, Eq)]
struct Digest {
    boundary: (u64, u64),
    snapshot: Option<Vec<u8>>,
    peers: Vec<u64>,
    hard: (u64, Option<u64>),
    entries: Vec<(u64, u64, Vec<u8>)>,
}

fn digest(log: &WalLog) -> Digest {
    let from = log.last_included_index() + 1;
    Digest {
        boundary: (log.last_included_index(), log.last_included_term()),
        snapshot: log.snapshot_data().map(|d| d.to_vec()),
        peers: log.snapshot_peers().to_vec(),
        hard: log.load_hard_state(),
        entries: log
            .entries_from(from)
            .into_iter()
            .map(|e| (e.term, e.index, e.data))
            .collect(),
    }
}

fn open_digest(dir: &Path) -> Digest {
    let log = WalLog::open(wal(dir)).unwrap_or_else(|e| panic!("open failed: {e}"));
    digest(&log)
}

/// Open must fail, and must not change a single file or create any.
fn assert_refused(dir: &Path, ctx: &str) -> WalError {
    fault::clear();
    let before = image(dir);
    let err = match WalLog::open(wal(dir)) {
        Err(e) => e,
        Ok(_) => panic!("{ctx}: open succeeded; the damage went undetected"),
    };
    assert_eq!(
        image(dir),
        before,
        "{ctx}: a refused open must leave every file byte-for-byte unchanged \
         (error was: {err})"
    );
    err
}

#[derive(Clone, Copy, Debug)]
enum Tear {
    Truncate,
    ZeroPad,
    DamagedFinalFrame,
}
const TEARS: [Tear; 3] = [Tear::Truncate, Tear::ZeroPad, Tear::DamagedFinalFrame];

fn tear(path: &Path, how: Tear) {
    let mut b = fs::read(path).unwrap();
    match how {
        Tear::Truncate => b.truncate(b.len() - 3),
        Tear::ZeroPad => b.extend_from_slice(&[0; 11]),
        Tear::DamagedFinalFrame => *b.last_mut().unwrap() ^= 0xff,
    }
    fs::write(path, b).unwrap();
}

fn loses_last(how: Tear) -> bool {
    !matches!(how, Tear::ZeroPad)
}

/// Stale temp files a crash mid-update can leave behind.
fn plant_tmp_files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let v = vec![
        (dir.join("node.wal.tmp"), b"stale wal rewrite".to_vec()),
        (dir.join("node.wal.hardstate.tmp"), b"stale hs".to_vec()),
        (dir.join("node.wal.snapshot.tmp"), b"stale snap".to_vec()),
    ];
    for (p, b) in &v {
        fs::write(p, b).unwrap();
    }
    v
}

// torn tail + a second problem: refuse first, modify nothing

#[test]
fn torn_wal_tail_with_a_corrupt_hard_state_is_refused_untouched() {
    for how in TEARS {
        let dir = tempfile::tempdir().unwrap();
        drop(seeded(dir.path(), 5));
        let good_hs = fs::read(hs(dir.path())).unwrap();
        tear(&wal(dir.path()), how);
        type Mutate = Box<dyn Fn(&mut Vec<u8>)>;
        let mutations: Vec<(&str, Mutate)> = vec![
            ("flipped byte", Box::new(|b| b[16] ^= 1)),
            ("bad magic", Box::new(|b| b[0] ^= 1)),
            ("truncated", Box::new(|b| b.truncate(10))),
            ("oversized", Box::new(|b| b.extend_from_slice(&[0; 40]))),
        ];
        for (name, m) in mutations {
            let mut b = good_hs.clone();
            m(&mut b);
            fs::write(hs(dir.path()), b).unwrap();
            assert_refused(dir.path(), &format!("{how:?} + hard state {name}"));
        }
    }
}

#[test]
fn torn_wal_tail_with_a_corrupt_snapshot_is_refused_untouched() {
    for how in TEARS {
        let dir = tempfile::tempdir().unwrap();
        compacted(dir.path(), 8);
        tear(&wal(dir.path()), how);
        let good = fs::read(snap(dir.path())).unwrap();
        for (name, off) in [
            ("data byte", 45usize),
            ("index field", 10),
            ("term field", 18),
        ] {
            let mut b = good.clone();
            b[off] ^= 1;
            fs::write(snap(dir.path()), b).unwrap();
            assert_refused(dir.path(), &format!("{how:?} + snapshot {name}"));
        }
        let mut b = good.clone();
        b.truncate(b.len() - 5);
        fs::write(snap(dir.path()), b).unwrap();
        assert_refused(dir.path(), &format!("{how:?} + truncated snapshot"));
    }
}

#[test]
fn torn_wal_tail_with_a_missing_hard_state_is_refused_untouched() {
    for how in TEARS {
        let dir = tempfile::tempdir().unwrap();
        drop(seeded(dir.path(), 5));
        tear(&wal(dir.path()), how);
        fs::remove_file(hs(dir.path())).unwrap();
        let e = assert_refused(dir.path(), &format!("{how:?}"));
        assert!(matches!(e, WalError::HardStateMissing), "{e}");
    }
}

#[test]
fn torn_wal_tail_with_a_missing_snapshot_is_refused_untouched() {
    // The WAL starts at index 5, anchored by a snapshot that is now gone.
    for how in TEARS {
        let dir = tempfile::tempdir().unwrap();
        compacted(dir.path(), 8);
        tear(&wal(dir.path()), how);
        fs::remove_file(snap(dir.path())).unwrap();
        let e = assert_refused(dir.path(), &format!("{how:?}"));
        assert!(matches!(e, WalError::LogInconsistent(_)), "{e}");
    }
}

#[test]
fn torn_wal_tail_with_a_rolled_back_hard_state_is_refused_untouched() {
    for how in TEARS {
        let dir = tempfile::tempdir().unwrap();
        let mut log = seeded(dir.path(), 0);
        log.save_hard_state(3, Some(1));
        let old_hs = fs::read(hs(dir.path())).unwrap();
        log.save_hard_state(5, Some(2));
        log.append(5, b"term5-a".to_vec());
        log.append(5, b"term5-b".to_vec());
        drop(log);
        fs::write(hs(dir.path()), old_hs).unwrap();
        tear(&wal(dir.path()), how);
        let e = assert_refused(dir.path(), &format!("{how:?}"));
        assert!(matches!(e, WalError::StateInconsistent(_)), "{e}");
    }
}

#[test]
fn interrupted_compaction_plus_a_rolled_back_hard_state_does_not_rewrite_the_wal() {
    // The finishing rewrite of an interrupted compaction must not run before
    // the hard state has been cross-checked against the log.
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 0);
    log.save_hard_state(1, Some(1));
    let old_hs = fs::read(hs(dir.path())).unwrap();
    log.save_hard_state(2, Some(1));
    for i in 1..=6u8 {
        log.append(2, vec![i]);
    }
    let pre_compaction_wal = fs::read(wal(dir.path())).unwrap();
    log.compact(4, 2, b"snap".to_vec(), vec![1]);
    drop(log);
    // Crash state: snapshot published, WAL not yet rewritten...
    fs::write(wal(dir.path()), &pre_compaction_wal).unwrap();
    // ...and a hard state that predates the log's terms.
    fs::write(hs(dir.path()), old_hs).unwrap();
    let e = assert_refused(dir.path(), "interrupted compaction + old hard state");
    assert!(matches!(e, WalError::StateInconsistent(_)), "{e}");
    assert_eq!(fs::read(wal(dir.path())).unwrap(), pre_compaction_wal);
}

#[test]
fn stale_temp_files_survive_a_refusal_and_are_removed_after_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    compacted(dir.path(), 8);
    let good_snap = fs::read(snap(dir.path())).unwrap();
    let tmps = plant_tmp_files(dir.path());
    let mut bad = good_snap.clone();
    bad[45] ^= 1;
    fs::write(snap(dir.path()), bad).unwrap();
    assert_refused(dir.path(), "corrupt snapshot with stale temp files");
    for (p, b) in &tmps {
        assert_eq!(&fs::read(p).unwrap(), b, "{} was touched", p.display());
    }
    fs::write(snap(dir.path()), good_snap).unwrap();
    let _ = open_digest(dir.path());
    for (p, _) in &tmps {
        assert!(
            !p.exists(),
            "{} should be cleaned after acceptance",
            p.display()
        );
    }
}

// missing WAL

#[test]
fn missing_wal_with_a_persisted_hard_state_is_refused_not_recreated_empty() {
    let dir = tempfile::tempdir().unwrap();
    drop(seeded(dir.path(), 5));
    let before = open_digest(dir.path());
    let saved = fs::read(wal(dir.path())).unwrap();
    fs::remove_file(wal(dir.path())).unwrap();
    let e = assert_refused(dir.path(), "WAL deleted, hard state present");
    assert!(matches!(e, WalError::LogMissing(_)), "{e}");
    assert!(
        !wal(dir.path()).exists(),
        "an empty WAL must not be invented"
    );

    // Nothing was clobbered: restoring the file restores everything.
    fs::write(wal(dir.path()), saved).unwrap();
    assert_eq!(open_digest(dir.path()), before);
}

#[test]
fn missing_wal_with_a_snapshot_that_may_have_a_log_suffix_is_refused() {
    // Snapshot at 4 + WAL entries 5..=8: the suffix is unrecoverable if the
    // WAL is gone, and nothing on disk says how long it was.
    for appended_after_snapshot in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        compacted(dir.path(), if appended_after_snapshot { 8 } else { 4 });
        let before = open_digest(dir.path());
        let saved = fs::read(wal(dir.path())).unwrap();
        fs::remove_file(wal(dir.path())).unwrap();
        let e = assert_refused(dir.path(), "WAL deleted, snapshot present");
        assert!(matches!(e, WalError::LogMissing(_)), "{e}");
        fs::write(wal(dir.path()), saved).unwrap();
        assert_eq!(open_digest(dir.path()), before);
    }
}

#[test]
fn missing_wal_is_refused_whichever_other_files_survive() {
    for (keep_hs, keep_snap) in [(true, true), (true, false), (false, true)] {
        let dir = tempfile::tempdir().unwrap();
        compacted(dir.path(), 8);
        fs::remove_file(wal(dir.path())).unwrap();
        if !keep_hs {
            fs::remove_file(hs(dir.path())).unwrap();
        }
        if !keep_snap {
            fs::remove_file(snap(dir.path())).unwrap();
        }
        let e = assert_refused(dir.path(), &format!("hs={keep_hs} snap={keep_snap}"));
        assert!(matches!(e, WalError::LogMissing(_)), "{e}");
    }
}

#[test]
fn a_stray_rewrite_temp_file_without_a_wal_is_not_a_fresh_node() {
    // A rewrite needs the live file to exist, so "tmp but no WAL" cannot be
    // produced by a crash; it means the WAL was removed.
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("node.wal.tmp"), b"entries?").unwrap();
    let e = assert_refused(dir.path(), "tmp without WAL");
    assert!(matches!(e, WalError::LogMissing(_)), "{e}");
}

// legitimate recovery still works

#[test]
fn a_fresh_node_initialises_and_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let d0 = open_digest(dir.path());
    assert_eq!(d0.hard, (0, None));
    assert!(d0.entries.is_empty() && d0.snapshot.is_none());
    assert!(wal(dir.path()).exists());
    assert_eq!(open_digest(dir.path()), d0);
    // Then it can persist state and come back.
    drop(seeded(dir.path(), 3));
    assert_eq!(open_digest(dir.path()).entries.len(), 3);
}

#[test]
fn a_wal_that_exists_but_is_empty_with_no_other_state_is_a_fresh_node() {
    // The residue of a crash right after the first open created the file.
    let dir = tempfile::tempdir().unwrap();
    fs::write(wal(dir.path()), b"").unwrap();
    let d = open_digest(dir.path());
    assert_eq!(d.hard, (0, None));
}

#[test]
fn a_crash_during_first_open_leaves_a_node_that_opens_fresh() {
    for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
        fault::clear();
        let dir = tempfile::tempdir().unwrap();
        fault::arm(FaultPlan {
            point: FaultPoint::WalCreateDirSync,
            skip: 0,
            action,
        });
        let first = catch_unwind(AssertUnwindSafe(|| WalLog::open(wal(dir.path())).is_ok()));
        fault::clear();
        assert!(!matches!(first, Ok(true)), "{action:?}: fault must surface");
        let d = open_digest(dir.path());
        assert!(d.entries.is_empty() && d.snapshot.is_none(), "{action:?}");
    }
}

#[test]
fn torn_tail_with_valid_hard_state_and_snapshot_is_recovered_and_truncated() {
    for how in TEARS {
        let ctx = format!("{how:?}");
        let dir = tempfile::tempdir().unwrap();
        compacted(dir.path(), 8); // snapshot @4, WAL 5..=8
        let wal_len = fs::metadata(wal(dir.path())).unwrap().len();
        tear(&wal(dir.path()), how);
        let d = open_digest(dir.path());
        let last = if loses_last(how) { 7 } else { 8 };
        assert_eq!(d.boundary, (4, 1), "{ctx}");
        assert_eq!(d.snapshot.as_deref(), Some(&b"snapshot-bytes"[..]), "{ctx}");
        assert_eq!(
            d.entries.iter().map(|e| e.1).collect::<Vec<_>>(),
            (5..=last).collect::<Vec<_>>(),
            "{ctx}"
        );
        // The torn bytes are gone, so the file is a clean prefix again.
        assert!(
            fs::metadata(wal(dir.path())).unwrap().len() <= wal_len,
            "{ctx}"
        );
        assert_eq!(open_digest(dir.path()), d, "{ctx}: recovery not idempotent");
        if loses_last(how) {
            assert!(
                dir.path().join("node.wal.discarded-tail").exists(),
                "{ctx}: discarded bytes must be preserved"
            );
        }
    }
}

// phase-2 crash sweep: every step, every action

/// Interrupted compaction (snapshot published, WAL not yet rewritten) with a
/// torn tail on top and every kind of stale temp file: recovery has to do
/// all of {remove temp files, truncate the tail, rewrite the WAL}.
fn messy_recovery_image() -> (tempfile::TempDir, Image) {
    let dir = tempfile::tempdir().unwrap();
    let mut log = seeded(dir.path(), 8);
    let pre = fs::read(wal(dir.path())).unwrap();
    log.compact(4, 1, b"snapshot-bytes".to_vec(), vec![1, 2, 3]);
    drop(log);
    fs::write(wal(dir.path()), pre).unwrap();
    tear(&wal(dir.path()), Tear::Truncate);
    plant_tmp_files(dir.path());
    let img = image(dir.path());
    (dir, img)
}

#[test]
fn crash_at_every_phase_two_step_then_repeated_recovery_converges() {
    let (clean_dir, pristine) = messy_recovery_image();
    fault::clear();
    let expected = open_digest(clean_dir.path());
    assert_eq!(expected.boundary, (4, 1));
    assert_eq!(
        expected.entries.iter().map(|e| e.1).collect::<Vec<_>>(),
        vec![5, 6, 7],
        "the torn entry 8 is dropped, the covered entries 1..=4 are pruned"
    );
    let settled = image(clean_dir.path());
    assert!(
        !settled.keys().any(|n| n.ends_with(".tmp")),
        "{:?}",
        settled.keys().collect::<Vec<_>>()
    );

    let points = [
        FaultPoint::RecoverRemoveTmp,
        FaultPoint::WalRecoverTruncate,
        FaultPoint::WalRecoverSync,
        FaultPoint::WalRewriteTmpWrite,
        FaultPoint::WalRewriteTmpSync,
        FaultPoint::WalRewriteRename,
        FaultPoint::WalRewriteDirSync,
    ];
    // Hits of each point during one clean recovery.
    let probe = tempfile::tempdir().unwrap();
    restore(probe.path(), &pristine);
    fault::clear();
    open_digest(probe.path());
    let counts: Vec<_> = points.iter().map(|p| (*p, fault::hits(*p))).collect();
    fault::clear();
    assert!(
        counts.iter().all(|(_, n)| *n >= 1),
        "recovery must reach every phase-2 step: {counts:?}"
    );

    let mut runs = 0;
    for (point, hits) in counts {
        for skip in 0..hits {
            for action in [
                FaultAction::FailBefore,
                FaultAction::FailAfter,
                FaultAction::PartialWrite(5),
            ] {
                if matches!(action, FaultAction::PartialWrite(_))
                    && point != FaultPoint::WalRewriteTmpWrite
                {
                    continue;
                }
                let ctx = format!("{point:?}#{skip}/{action:?}");
                let dir = tempfile::tempdir().unwrap();
                restore(dir.path(), &pristine);
                fault::clear();
                fault::arm(FaultPlan {
                    point,
                    skip,
                    action,
                });
                let first =
                    catch_unwind(AssertUnwindSafe(|| WalLog::open(wal(dir.path())).is_ok()));
                let fired = fault::fired();
                fault::clear();
                assert_eq!(fired, Some(point), "{ctx}: fault did not fire");
                assert!(
                    !matches!(first, Ok(true)),
                    "{ctx}: crash must abort the open"
                );

                // Repeated recovery: converges to the same state every time.
                let d1 = open_digest(dir.path());
                assert_eq!(d1, expected, "{ctx}: recovered state differs");
                let img1 = image(dir.path());
                let d2 = open_digest(dir.path());
                assert_eq!(d2, expected, "{ctx}: second recovery differs");
                assert_eq!(
                    image(dir.path()),
                    img1,
                    "{ctx}: recovery not idempotent on disk"
                );
                assert!(
                    !img1.keys().any(|n| n.ends_with(".tmp")),
                    "{ctx}: temp files left: {:?}",
                    img1.keys().collect::<Vec<_>>()
                );
                runs += 1;
            }
        }
    }
    assert!(runs >= 12, "sweep suspiciously small: {runs}");
}

// corrupted frame headers in the middle of the log
//
// A damaged length field cannot be told apart from a torn tail by looking at
// the frame alone ("claims more bytes than the file has"). If valid frames
// exist *after* it, treating it as a torn tail would silently truncate
// entries that may have been committed. That must be corruption, not
// recovery.

/// (start offset, total length) of every frame in a WAL file.
fn frames(bytes: &[u8]) -> Vec<(usize, usize)> {
    let (mut v, mut p) = (Vec::new(), 0);
    while p + 8 <= bytes.len() {
        let len = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
        v.push((p, 8 + len));
        p += 8 + len;
    }
    v
}

fn five_entry_wal(dir: &Path) -> Vec<u8> {
    drop(seeded(dir, 5));
    fs::read(wal(dir)).unwrap()
}

fn entry_count(dir: &Path) -> usize {
    let log = WalLog::open(wal(dir)).unwrap_or_else(|e| panic!("open failed: {e}"));
    log.entries_from(1).len()
}

#[test]
fn a_corrupted_length_field_before_valid_frames_is_refused_untouched() {
    for corrupt_frame in 0..4usize {
        // Every frame except the last: valid frames follow it.
        for (name, newlen) in [("huge", 0x00ff_ffffu32), ("just past EOF", u32::MAX / 2)] {
            let dir = tempfile::tempdir().unwrap();
            let mut b = five_entry_wal(dir.path());
            let (off, _) = frames(&b)[corrupt_frame];
            b[off..off + 4].copy_from_slice(&newlen.to_le_bytes());
            fs::write(wal(dir.path()), &b).unwrap();
            let e = assert_refused(dir.path(), &format!("frame {corrupt_frame} length {name}"));
            assert!(
                matches!(e, WalError::LogCorrupt { .. }),
                "frame {corrupt_frame}: {e}"
            );
        }
    }
}

#[test]
fn a_length_field_inflated_to_exactly_end_of_file_is_refused_untouched() {
    // The damaged frame now claims to end exactly at EOF, so it *looks* like
    // a damaged final frame, while the real frames after it are swallowed.
    for corrupt_frame in 0..4usize {
        let dir = tempfile::tempdir().unwrap();
        let mut b = five_entry_wal(dir.path());
        let (off, _) = frames(&b)[corrupt_frame];
        let swallow_to_eof = (b.len() - off - 8) as u32;
        b[off..off + 4].copy_from_slice(&swallow_to_eof.to_le_bytes());
        fs::write(wal(dir.path()), &b).unwrap();
        let e = assert_refused(
            dir.path(),
            &format!("frame {corrupt_frame} swallows to EOF"),
        );
        assert!(
            matches!(e, WalError::LogCorrupt { .. }),
            "frame {corrupt_frame}: {e}"
        );
    }
}

#[test]
fn a_corrupted_length_field_on_the_final_frame_is_still_a_torn_tail() {
    // Nothing valid follows, so this is indistinguishable from a torn write
    // and stays recoverable: the first four entries survive.
    let dir = tempfile::tempdir().unwrap();
    let mut b = five_entry_wal(dir.path());
    let (off, _) = frames(&b)[4];
    b[off..off + 4].copy_from_slice(&0x00ff_ffffu32.to_le_bytes());
    fs::write(wal(dir.path()), &b).unwrap();
    assert_eq!(entry_count(dir.path()), 4);
}

#[test]
fn no_single_byte_corruption_silently_drops_a_non_final_entry() {
    // Flip every bit-pattern of every byte of a four-entry WAL. Outcomes:
    //  * the open is refused (always acceptable), or
    //  * it succeeds having lost *at most* the final entry, and only when
    //    the damage was inside the final frame.
    // Losing any earlier entry without an error is the bug.
    let base = tempfile::tempdir().unwrap();
    drop(seeded(base.path(), 4));
    let clean = fs::read(wal(base.path())).unwrap();
    let hs_bytes = fs::read(hs(base.path())).unwrap();
    let fr = frames(&clean);
    let last_start = fr[3].0;
    let mut refused = 0;
    let mut torn_tail = 0;
    for i in 0..clean.len() {
        for mask in [0x01u8, 0x40, 0xff] {
            let dir = tempfile::tempdir().unwrap();
            let mut b = clean.clone();
            b[i] ^= mask;
            fs::write(wal(dir.path()), &b).unwrap();
            fs::write(hs(dir.path()), &hs_bytes).unwrap();
            fault::clear();
            match WalLog::open(wal(dir.path())) {
                Err(_) => refused += 1,
                Ok(log) => {
                    let n = log.entries_from(1).len();
                    assert!(
                        i >= last_start && n == 3,
                        "byte {i} (mask {mask:#x}) in frame {}: open succeeded with {n} of 4 \
                         entries - an earlier entry was silently dropped",
                        fr.iter().position(|(s, l)| i >= *s && i < s + l).unwrap()
                    );
                    torn_tail += 1;
                }
            }
        }
    }
    assert!(
        refused > 100,
        "sweep should mostly refuse mid-log damage: {refused}"
    );
    assert!(
        torn_tail > 0,
        "damage inside the final frame must stay recoverable"
    );
}
