//! Recovery-safety tests for `Lsm::open`.
//!
//! The rule under test: startup must never *silently* manufacture an empty
//! WAL (and so an empty-looking database) when the persistence files say
//! data should be there. A genuinely fresh directory, a clean shutdown, a
//! crash mid-compaction and a snapshot ingest must all still recover
//! exactly; missing or damaged persistence files must surface as
//! `LsmError::Corruption` with the directory left untouched.
//!
//! Every recovery is checked against a reference `BTreeMap` model.
//! Fault plans are thread-local, so the engine runs with
//! `background: false`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::lsm::{Lsm, LsmError, LsmOptions, Op};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type DirImage = BTreeMap<String, Vec<u8>>;

fn opts() -> LsmOptions {
    LsmOptions {
        memtable_bytes: 700,
        block_bytes: 128,
        l1_file_bytes: 1024,
        l0_compaction_trigger: 2,
        cache_bytes: 16 * 1024,
        sync_wal: true,
        background: false,
        verify_on_open: true,
        ..LsmOptions::default()
    }
}

/// Memtable too large to rotate on its own and compaction never triggers:
/// lets a test decide exactly when flushes happen.
fn manual() -> LsmOptions {
    LsmOptions {
        memtable_bytes: 1 << 20,
        l0_compaction_trigger: 1000,
        ..opts()
    }
}

fn k(i: u64) -> Vec<u8> {
    format!("key{i:04}").into_bytes()
}

fn v(i: u64) -> Vec<u8> {
    format!("value-{i}-{}", "x".repeat(24)).into_bytes()
}

/// Apply op `i` (Put or, every 5th, Delete of an earlier key) to both the
/// engine and the model, with Raft index `i`.
fn step(db: &Lsm, model: &mut Model, i: u64) -> storage::lsm::Result<()> {
    let op = if i.is_multiple_of(5) {
        Op::Delete(k(i - 3))
    } else {
        Op::Put(k(i), v(i))
    };
    match &op {
        Op::Put(key, val) => {
            model.insert(key.clone(), val.clone());
        }
        Op::Delete(key) => {
            model.remove(key);
        }
    }
    db.apply(&[op], i)
}

fn fill(db: &Lsm, model: &mut Model, range: std::ops::RangeInclusive<u64>) {
    for i in range {
        step(db, model, i).unwrap();
    }
}

fn assert_model(db: &Lsm, model: &Model, ctx: &str) {
    let dump: Model = db.dump().unwrap().into_iter().collect();
    assert_eq!(&dump, model, "{ctx}: recovered contents differ from model");
    for (key, val) in model {
        assert_eq!(
            db.get(key).unwrap().as_ref(),
            Some(val),
            "{ctx}: point read {key:?}"
        );
    }
}

fn image(dir: &Path) -> DirImage {
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

fn restore(dir: &Path, img: &DirImage) {
    for e in fs::read_dir(dir).unwrap().flatten() {
        fs::remove_file(e.path()).unwrap();
    }
    for (name, bytes) in img {
        fs::write(dir.join(name), bytes).unwrap();
    }
}

fn names(dir: &Path, prefix: &str) -> Vec<PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            let n = p.file_name().unwrap().to_string_lossy();
            n.starts_with(prefix) && !n.ends_with(".tmp")
        })
        .collect();
    v.sort();
    v
}

/// Open must fail with `Corruption` and must not modify the directory.
fn assert_refused(dir: &Path, ctx: &str) -> String {
    let before = image(dir);
    let msg = match Lsm::open(dir, opts()) {
        Err(LsmError::Corruption(m)) => m,
        Err(e) => panic!("{ctx}: expected Corruption, got {e}"),
        Ok(_) => panic!("{ctx}: open succeeded; missing/corrupt state went undetected"),
    };
    assert_eq!(
        image(dir),
        before,
        "{ctx}: a refused open must leave the directory untouched \
         (in particular it must not create a fresh WAL or MANIFEST)"
    );
    msg
}

// fresh database

#[test]
fn fresh_directory_initialises_and_is_usable_across_reopens() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), opts()).unwrap();
        assert_eq!(db.applied_index(), 0);
        assert!(db.dump().unwrap().is_empty());
        fill(&db, &mut model, 1..=30);
    }
    assert!(dir.path().join("MANIFEST").exists());
    assert_eq!(names(dir.path(), "wal-").len(), 1, "exactly one WAL");
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), 30);
    assert_model(&db, &model, "reopen after fresh init");
}

#[test]
fn nonexistent_directory_is_created_as_fresh() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("a").join("b");
    let db = Lsm::open(&dir, opts()).unwrap();
    assert!(db.dump().unwrap().is_empty());
    assert!(dir.join("MANIFEST").exists());
}

#[test]
fn empty_but_initialised_database_reopens_as_empty() {
    // Opened and closed with no writes at all: MANIFEST + empty WAL.
    let dir = tempfile::tempdir().unwrap();
    drop(Lsm::open(dir.path(), opts()).unwrap());
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert!(db.dump().unwrap().is_empty());
    assert_eq!(db.applied_index(), 0);
}

#[test]
fn interrupted_first_open_is_recoverable_as_fresh() {
    // Crash at every I/O point of the *first* open. Whatever was left on
    // disk must reopen as an empty database (nothing was ever written).
    for point in FaultPoint::LSM_ALL {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            fault::clear();
            let dir = tempfile::tempdir().unwrap();
            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            let first = Lsm::open(dir.path(), opts());
            let fired = fault::fired();
            fault::clear();
            if fired.is_some() {
                assert!(first.is_err(), "{point:?}/{action:?}: fault must surface");
            }
            drop(first);
            let db = Lsm::open(dir.path(), opts())
                .unwrap_or_else(|e| panic!("{point:?}/{action:?}: reopen failed: {e}"));
            assert!(db.dump().unwrap().is_empty(), "{point:?}/{action:?}");
            assert_eq!(db.applied_index(), 0);
            let mut model = Model::new();
            fill(&db, &mut model, 1..=5);
            drop(db);
            let db = Lsm::open(dir.path(), opts()).unwrap();
            assert_model(&db, &model, &format!("{point:?}/{action:?} then writes"));
        }
    }
}

#[test]
fn empty_first_wal_without_manifest_is_a_fresh_database() {
    // The residue of a crash between "WAL 1 created" and "MANIFEST written".
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("wal-0000000001.log"), b"").unwrap();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert!(db.dump().unwrap().is_empty());
    assert!(dir.path().join("MANIFEST").exists());
}

#[test]
fn missing_manifest_with_any_real_data_is_refused() {
    // WAL with data, no MANIFEST.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), opts()).unwrap();
        fill(&db, &mut model, 1..=4);
    }
    fs::remove_file(dir.path().join("MANIFEST")).unwrap();
    assert_refused(dir.path(), "WAL data but no MANIFEST");

    // An empty WAL that is *not* WAL 1 cannot be first-open residue.
    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("wal-0000000007.log"), b"").unwrap();
    assert_refused(dir.path(), "empty WAL 7, no MANIFEST");

    // SSTables but no MANIFEST (after a flush).
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Lsm::open(dir.path(), opts()).unwrap();
        let mut m = Model::new();
        fill(&db, &mut m, 1..=10);
        db.flush().unwrap();
    }
    fs::remove_file(dir.path().join("MANIFEST")).unwrap();
    assert_refused(dir.path(), "SSTables but no MANIFEST");
}

// missing WAL files

#[test]
fn missing_only_wal_with_unflushed_data_is_refused_not_replaced() {
    // This must be refused: opening it as an empty database would silently
    // drop every acknowledged write.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=12);
    }
    let wal = names(dir.path(), "wal-").pop().unwrap();
    let saved = fs::read(&wal).unwrap();
    fs::remove_file(&wal).unwrap();
    let msg = assert_refused(dir.path(), "only WAL deleted");
    assert!(msg.contains("WAL"), "error should name the problem: {msg}");

    // Restoring the file restores everything (nothing was clobbered).
    fs::write(&wal, saved).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), 12);
    assert_model(&db, &model, "WAL restored");
}

#[test]
fn missing_active_wal_after_flush_is_refused() {
    // After a flush the MANIFEST points min_wal_id at the new active WAL,
    // which then holds the unflushed tail.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=20);
        db.flush().unwrap();
        fill(&db, &mut model, 21..=26); // unflushed, WAL only
    }
    let wals = names(dir.path(), "wal-");
    assert_eq!(wals.len(), 1, "flush must have retired the old WAL");
    let saved = fs::read(&wals[0]).unwrap();
    fs::remove_file(&wals[0]).unwrap();
    assert_refused(dir.path(), "active WAL deleted after flush");

    fs::write(&wals[0], saved).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), 26);
    assert_model(&db, &model, "active WAL restored after flush");
}

#[test]
fn missing_empty_active_wal_is_still_refused() {
    // Conservative by design: from the MANIFEST alone a deleted *empty*
    // WAL is indistinguishable from a deleted full one.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=10);
        db.flush().unwrap(); // active WAL is now empty
    }
    let wal = names(dir.path(), "wal-").pop().unwrap();
    assert_eq!(fs::metadata(&wal).unwrap().len(), 0);
    fs::remove_file(&wal).unwrap();
    assert_refused(dir.path(), "empty active WAL deleted");
}

/// Build a directory holding two live WALs, both with data, and a MANIFEST
/// that predates both. Returns (dir, model, last index, [older, newer]).
fn two_live_wals() -> (tempfile::TempDir, Model, u64, Vec<PathBuf>) {
    fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let mut last = 0;
    {
        // Step 1: fill until the memtable rotates, with the resulting
        // flush failing at its SST rename. That leaves the old WAL live
        // (its data is not in any table) plus a fresh active WAL.
        let db = Lsm::open(dir.path(), opts()).unwrap();
        fault::arm(FaultPlan {
            point: FaultPoint::LsmSstRename,
            skip: 0,
            action: FaultAction::FailBefore,
        });
        loop {
            last += 1;
            if step(&db, &mut model, last).is_err() {
                break; // the record itself was durable before the flush failed
            }
            assert!(last < 200, "memtable never rotated");
        }
        assert_eq!(fault::fired(), Some(FaultPoint::LsmSstRename));
        fault::clear();
        assert!(db.is_poisoned());
    }
    {
        // Step 2: recover (replays WAL A, appends to WAL B) and write a
        // few more records that land in the newer WAL.
        let db = Lsm::open(dir.path(), manual()).unwrap();
        assert_eq!(db.applied_index(), last);
        for _ in 0..4 {
            last += 1;
            step(&db, &mut model, last).unwrap();
        }
    }
    let wals = names(dir.path(), "wal-");
    assert_eq!(wals.len(), 2, "expected two live WALs, got {wals:?}");
    assert!(wals.iter().all(|w| fs::metadata(w).unwrap().len() > 0));
    (dir, model, last, wals)
}

#[test]
fn multiple_wals_all_present_recover_exactly() {
    let (dir, model, last, _) = two_live_wals();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "two live WALs");
}

#[test]
fn missing_oldest_of_multiple_wals_is_refused() {
    let (dir, model, last, wals) = two_live_wals();
    let saved = fs::read(&wals[0]).unwrap();
    fs::remove_file(&wals[0]).unwrap();
    assert_refused(dir.path(), "oldest of two WALs deleted");

    fs::write(&wals[0], saved).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "oldest WAL restored");
}

#[test]
fn midlog_damage_in_one_of_multiple_wals_is_refused() {
    let (dir, _, _, wals) = two_live_wals();
    let mut bytes = fs::read(&wals[0]).unwrap();
    bytes[10] ^= 0xff;
    fs::write(&wals[0], bytes).unwrap();
    assert_refused(dir.path(), "damaged older WAL");
}

#[test]
fn missing_newest_of_multiple_wals_is_refused() {
    let (dir, _, _, wals) = two_live_wals();
    fs::remove_file(&wals[1]).unwrap();
    assert_refused(dir.path(), "newest of two WALs deleted");
}

#[test]
fn rotation_crash_boundaries_preserve_acknowledged_writes_and_recover_orphans() {
    for point in [
        FaultPoint::LsmWalCreateDirSync,
        FaultPoint::LsmManifestTmpWrite,
        FaultPoint::LsmManifestTmpSync,
        FaultPoint::LsmManifestRename,
        FaultPoint::LsmManifestDirSync,
    ] {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            fault::clear();
            let dir = tempfile::tempdir().unwrap();
            let db = Lsm::open(dir.path(), opts()).unwrap();
            let mut acknowledged = Vec::new();
            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            for i in 1..=100u64 {
                let key = k(i);
                let value = v(i);
                if db.apply(&[Op::Put(key.clone(), value.clone())], i).is_ok() {
                    acknowledged.push((key, value));
                }
                if fault::fired().is_some() {
                    break;
                }
            }
            let fired = fault::fired();
            fault::clear();
            assert_eq!(
                fired,
                Some(point),
                "expected rotation to hit {point:?}/{action:?}"
            );
            drop(db);

            let recovered = Lsm::open(dir.path(), opts())
                .unwrap_or_else(|e| panic!("reopen after {point:?}/{action:?}: {e}"));
            for (key, value) in acknowledged {
                assert_eq!(
                    recovered.get(&key).unwrap(),
                    Some(value),
                    "acknowledged write lost at {point:?}/{action:?}"
                );
            }
            // Reaching this point also proves startup tolerated any orphan
            // left by the interrupted create/publication transition.
        }
    }
}

#[test]
fn unreferenced_wal_from_create_before_manifest_publish_is_cleaned() {
    let (dir, model, last, _) = two_live_wals();
    // IDs are monotonically allocated. A file well beyond every current file
    // ID models a WAL created before its MANIFEST publication, so it cannot
    // have been installed as the active writer or acknowledged any writes.
    let max_id = names(dir.path(), "wal-")
        .iter()
        .filter_map(|p| {
            p.file_name()?
                .to_str()?
                .strip_prefix("wal-")?
                .strip_suffix(".log")?
                .parse::<u64>()
                .ok()
        })
        .max()
        .unwrap();
    let orphan = dir.path().join(format!("wal-{:010}.log", max_id + 10_000));
    fs::write(&orphan, b"").unwrap(); // create() writes nothing: a real orphan is empty

    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert!(
        !orphan.exists(),
        "provably unallocated WAL orphan must be removed"
    );
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "orphan WAL cleanup");
}

#[test]
fn missing_required_wal_is_still_refused_when_an_orphan_exists() {
    let (dir, _, _, wals) = two_live_wals();
    let max_id = names(dir.path(), "wal-")
        .iter()
        .filter_map(|p| {
            p.file_name()?
                .to_str()?
                .strip_prefix("wal-")?
                .strip_suffix(".log")?
                .parse::<u64>()
                .ok()
        })
        .max()
        .unwrap();
    let orphan = dir.path().join(format!("wal-{:010}.log", max_id + 10_000));
    fs::write(&orphan, b"orphan").unwrap();
    fs::remove_file(&wals[1]).unwrap();
    assert_refused(dir.path(), "missing required WAL despite extra orphan");
    assert!(orphan.exists(), "refusal must not mutate the directory");
}

#[test]
fn stale_wal_older_than_min_wal_id_is_garbage_not_an_error() {
    // Crash after the flush's MANIFEST publish but before the old WAL was
    // unlinked: the leftover must be cleaned up, not trusted or fatal.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let old_wal;
    let old_bytes;
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=15);
        old_wal = names(dir.path(), "wal-").pop().unwrap();
        old_bytes = fs::read(&old_wal).unwrap();
        db.flush().unwrap();
        fill(&db, &mut model, 16..=18);
    }
    assert!(!old_wal.exists());
    fs::write(&old_wal, old_bytes).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert!(!old_wal.exists(), "stale WAL must be removed");
    assert_eq!(db.applied_index(), 18);
    assert_model(&db, &model, "stale WAL present");
}

// corrupted manifest

fn populated_with_unflushed_tail() -> (tempfile::TempDir, Model) {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    fill(&db, &mut model, 1..=60); // several flushes (small memtable)
    db.compact_all().unwrap();
    fill(&db, &mut model, 61..=64);
    drop(db);
    (dir, model)
}

#[test]
fn corrupted_manifest_is_refused_and_never_replaced_by_a_fresh_one() {
    let (dir, model) = populated_with_unflushed_tail();
    let mpath = dir.path().join("MANIFEST");
    let good = fs::read(&mpath).unwrap();

    let mut cases: Vec<(String, Vec<u8>)> = vec![
        ("empty".into(), vec![]),
        ("one byte".into(), good[..1].to_vec()),
        ("header only".into(), good[..8].to_vec()),
        ("truncated".into(), good[..good.len() - 1].to_vec()),
        ("garbage".into(), vec![0xAB; good.len()]),
        ("zeros".into(), vec![0; good.len()]),
        ("appended junk".into(), {
            let mut b = good.clone();
            b.extend_from_slice(b"junk");
            b
        }),
    ];
    for off in (0..good.len()).step_by(3) {
        let mut b = good.clone();
        b[off] ^= 0x10;
        cases.push((format!("bit flip at {off}"), b));
    }
    for (name, bytes) in cases {
        fs::write(&mpath, &bytes).unwrap();
        assert_refused(dir.path(), &format!("manifest: {name}"));
    }

    fs::write(&mpath, &good).unwrap();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), 64);
    assert_model(&db, &model, "manifest restored");
}

#[test]
fn manifest_that_disagrees_with_the_files_is_refused() {
    // A well-formed, checksum-valid MANIFEST from an *older* point in time
    // (version rollback). It still names min_wal_id of a WAL that has since
    // been retired, so recovery must refuse rather than guess.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    fill(&db, &mut model, 1..=10);
    db.flush().unwrap();
    let old_manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
    fill(&db, &mut model, 11..=20);
    db.flush().unwrap();
    drop(db);

    fs::write(dir.path().join("MANIFEST"), old_manifest).unwrap();
    assert_refused(dir.path(), "rolled-back MANIFEST");
}

// compaction

/// Four flushed L0 tables, nothing else; compaction left to the caller.
fn four_l0_tables() -> (tempfile::TempDir, Model) {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    let mut i = 0;
    for _ in 0..4 {
        for _ in 0..12 {
            i += 1;
            step(&db, &mut model, i).unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.stats().l0_files, 4);
    drop(db);
    (dir, model)
}

#[test]
fn compaction_then_clean_reopen_recovers_exactly() {
    let (dir, model) = four_l0_tables();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    db.compact_all().unwrap();
    assert!(db.stats().compactions > 0);
    assert_model(&db, &model, "after compaction");
    drop(db);
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), 48);
    assert_model(&db, &model, "reopen after compaction");
}

#[test]
fn crash_at_every_compaction_io_step_recovers_the_full_model() {
    let points = [
        FaultPoint::LsmSstTmpWrite,
        FaultPoint::LsmSstTmpSync,
        FaultPoint::LsmSstRename,
        FaultPoint::LsmSstDirSync,
        FaultPoint::LsmManifestTmpWrite,
        FaultPoint::LsmManifestTmpSync,
        FaultPoint::LsmManifestRename,
        FaultPoint::LsmManifestDirSync,
    ];
    let (pristine, model) = four_l0_tables();
    let base = image(pristine.path());

    // Count hits in a fault-free compaction.
    let probe = tempfile::tempdir().unwrap();
    restore(probe.path(), &base);
    let db = Lsm::open(probe.path(), opts()).unwrap();
    fault::clear();
    db.compact_all().unwrap();
    let counts: Vec<_> = points.iter().map(|p| (*p, fault::hits(*p))).collect();
    drop(db);
    assert!(
        counts.iter().all(|(_, n)| *n > 0),
        "compaction must reach every point: {counts:?}"
    );

    let mut runs = 0;
    for (point, n) in counts {
        for skip in 0..n {
            for action in [
                FaultAction::FailBefore,
                FaultAction::FailAfter,
                FaultAction::PartialWrite(7),
            ] {
                if matches!(action, FaultAction::PartialWrite(_))
                    && !matches!(
                        point,
                        FaultPoint::LsmSstTmpWrite | FaultPoint::LsmManifestTmpWrite
                    )
                {
                    continue;
                }
                let dir = tempfile::tempdir().unwrap();
                restore(dir.path(), &base);
                let db = Lsm::open(dir.path(), opts()).unwrap();
                fault::clear();
                fault::arm(FaultPlan {
                    point,
                    skip,
                    action,
                });
                let r = db.compact_all();
                let fired = fault::fired();
                fault::clear();
                assert_eq!(
                    fired,
                    Some(point),
                    "{point:?}#{skip}/{action:?} did not fire"
                );
                assert!(
                    r.is_err(),
                    "{point:?}#{skip}/{action:?}: fault must surface"
                );
                drop(db);

                let ctx = format!("{point:?}#{skip}/{action:?}");
                let db = Lsm::open(dir.path(), opts())
                    .unwrap_or_else(|e| panic!("{ctx}: recovery failed: {e}"));
                assert_eq!(db.applied_index(), 48, "{ctx}");
                assert_model(&db, &model, &ctx);
                // Finishing the interrupted compaction must also be fine.
                db.compact_all().unwrap();
                assert_model(&db, &model, &format!("{ctx} + recompaction"));
                drop(db);
                let db = Lsm::open(dir.path(), opts()).unwrap();
                assert_model(&db, &model, &format!("{ctx} + reopen"));
                runs += 1;
            }
        }
    }
    assert!(runs >= 20, "sweep suspiciously small: {runs}");
}

#[test]
fn compaction_leftovers_are_garbage_in_both_crash_directions() {
    let (dir, model) = four_l0_tables();
    let before = image(dir.path()); // 4 inputs, pre-compaction MANIFEST
    {
        let db = Lsm::open(dir.path(), opts()).unwrap();
        db.compact_all().unwrap();
    }
    let after = image(dir.path()); // outputs, post-compaction MANIFEST

    // (a) Crash after the MANIFEST publish, before the inputs were
    //     unlinked: put the dead inputs back next to the new state.
    restore(dir.path(), &after);
    for (name, bytes) in &before {
        if name.starts_with("sst-") && !after.contains_key(name) {
            fs::write(dir.path().join(name), bytes).unwrap();
        }
    }
    let revived = names(dir.path(), "sst-").len();
    assert!(revived > after.keys().filter(|n| n.starts_with("sst-")).count());
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_model(&db, &model, "dead inputs left behind");
    assert_eq!(
        names(dir.path(), "sst-").len(),
        after.keys().filter(|n| n.starts_with("sst-")).count(),
        "dead inputs must be unlinked"
    );
    drop(db);

    // (b) Crash before the MANIFEST publish: old MANIFEST + inputs, plus
    //     fully-written but unreferenced outputs and a MANIFEST.tmp.
    restore(dir.path(), &before);
    for (name, bytes) in &after {
        if name.starts_with("sst-") && !before.contains_key(name) {
            fs::write(dir.path().join(name), bytes).unwrap();
        }
    }
    fs::write(dir.path().join("MANIFEST.tmp"), b"half").unwrap();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.stats().l0_files, 4, "old MANIFEST state must win");
    assert_model(&db, &model, "orphan outputs left behind");
    assert!(!dir.path().join("MANIFEST.tmp").exists());
    assert_eq!(
        names(dir.path(), "sst-").len(),
        4,
        "orphan outputs must be unlinked"
    );
}

#[test]
fn a_table_named_by_the_manifest_but_missing_is_refused() {
    let (dir, _) = four_l0_tables();
    let sst = names(dir.path(), "sst-").remove(0);
    fs::remove_file(sst).unwrap();
    assert_refused(dir.path(), "live SSTable deleted");
}

// legitimate recovery must keep working

#[test]
fn snapshot_ingest_then_more_writes_recovers_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), opts()).unwrap();
        fill(&db, &mut model, 1..=40);
        let snap: Vec<_> = (500..540).map(|i| (k(i), v(i))).collect();
        db.replace_all(snap.clone(), 100).unwrap();
        model = snap.into_iter().collect();
        for i in 101..=106 {
            let op = Op::Put(k(i), v(i));
            model.insert(k(i), v(i));
            db.apply(&[op], i).unwrap();
        }
    }
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), 106);
    assert_model(&db, &model, "after snapshot ingest + tail");
    drop(db);

    // The active WAL created by the ingest is as required as any other.
    let wal = names(dir.path(), "wal-").pop().unwrap();
    fs::remove_file(wal).unwrap();
    assert_refused(dir.path(), "post-ingest WAL deleted");
}

#[test]
fn repeated_clean_restarts_with_interleaved_work_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let mut i = 0;
    for round in 0..12 {
        let db = Lsm::open(dir.path(), opts()).unwrap();
        assert_eq!(db.applied_index(), i, "round {round}");
        assert_model(&db, &model, &format!("round {round} open"));
        for _ in 0..9 {
            i += 1;
            step(&db, &mut model, i).unwrap();
        }
        match round % 4 {
            0 => db.flush().unwrap(),
            1 => db.compact_all().unwrap(),
            2 => db.sync().unwrap(),
            _ => {}
        }
    }
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_model(&db, &model, "final");
}

#[test]
fn torn_wal_tail_is_still_dropped_not_fatal() {
    // Tightening the missing-WAL check must not turn ordinary torn tails
    // into errors.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=9);
    }
    let wal = names(dir.path(), "wal-").pop().unwrap();
    let bytes = fs::read(&wal).unwrap();
    fs::write(&wal, &bytes[..bytes.len() - 3]).unwrap();
    // Model after dropping the torn final record (op 9).
    let mut expect = Model::new();
    let scratch = tempfile::tempdir().unwrap();
    let sdb = Lsm::open(scratch.path(), manual()).unwrap();
    fill(&sdb, &mut expect, 1..=8);
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), 8);
    assert_model(&db, &expect, "torn tail");
}

// orphan WALs, MANIFEST publication and legacy v1
//
// Protocol under test (see `Lsm::open`): a WAL is created *before* the
// MANIFEST that lists it is published, and no writer is switched to it until
// that publish succeeds. So a crash in between can leave an unreferenced WAL,
// but one that is provably empty and whose id is >= the MANIFEST's
// `next_file_id`. Everything else unreferenced is *not* explainable by a
// crash and must fail closed.

/// Raw view of a v2 MANIFEST (the test crate cannot see the private type).
#[derive(Debug, Clone)]
struct Mf {
    next: u64,
    min: u64,
    live: Vec<u64>,
}

fn read_manifest(dir: &Path) -> Mf {
    let b = fs::read(dir.join("MANIFEST")).unwrap();
    assert_eq!(&b[..8], b"LSMMANI2", "expected a v2 MANIFEST");
    let u64_at = |o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());
    let n = u32::from_le_bytes(b[40..44].try_into().unwrap()) as usize;
    Mf {
        next: u64_at(16),
        min: u64_at(24),
        live: (0..n).map(|i| u64_at(44 + 8 * i)).collect(),
    }
}

/// (id, length) of every WAL file, ascending.
fn disk_wals(dir: &Path) -> Vec<(u64, u64)> {
    let mut v: Vec<_> = names(dir, "wal-")
        .iter()
        .map(|p| {
            let n = p.file_name().unwrap().to_string_lossy().to_string();
            let id = n["wal-".len()..n.len() - ".log".len()].parse().unwrap();
            (id, fs::metadata(p).unwrap().len())
        })
        .collect();
    v.sort();
    v
}

fn wal_file(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("wal-{id:010}.log"))
}

/// WAL files the MANIFEST does not list: (orphans: id >= min, stale: id < min).
/// `(id, length)` pairs.
type WalList = Vec<(u64, u64)>;

fn unreferenced(dir: &Path) -> (WalList, WalList) {
    let mf = read_manifest(dir);
    let extra: Vec<_> = disk_wals(dir)
        .into_iter()
        .filter(|(id, _)| !mf.live.contains(id))
        .collect();
    (
        extra
            .iter()
            .copied()
            .filter(|(id, _)| *id >= mf.min)
            .collect(),
        extra
            .iter()
            .copied()
            .filter(|(id, _)| *id < mf.min)
            .collect(),
    )
}

/// After a successful open every WAL on disk must be exactly the live set.
fn assert_wals_are_exactly_live(dir: &Path, ctx: &str) {
    let mf = read_manifest(dir);
    let ids: Vec<u64> = disk_wals(dir).into_iter().map(|(id, _)| id).collect();
    assert_eq!(
        ids, mf.live,
        "{ctx}: WALs on disk must equal MANIFEST live set"
    );
}

struct Crashed {
    dir: tempfile::TempDir,
    /// State of every acknowledged write.
    acked: Model,
    /// `acked` plus the one write whose `apply` returned an error. That
    /// record reached the WAL before the failing rotate/flush, so it may
    /// legitimately survive; it was never acknowledged.
    with_inflight: Model,
    last_acked: u64,
}

/// Fresh DB, then write until the armed fault fires (a rotation / flush).
fn crash_while_writing(point: FaultPoint, skip: usize, action: FaultAction) -> Crashed {
    fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    let mut acked = Model::new();
    let mut with_inflight = Model::new();
    let mut last_acked = 0;
    fault::arm(FaultPlan {
        point,
        skip,
        action,
    });
    let mut failed = false;
    for i in 1..=300u64 {
        let before = acked.clone();
        match step(&db, &mut acked, i) {
            Ok(()) => last_acked = i,
            Err(_) => {
                with_inflight = acked.clone();
                acked = before;
                failed = true;
                break;
            }
        }
    }
    let fired = fault::fired();
    fault::clear();
    assert!(failed, "{point:?}#{skip}/{action:?}: fault never surfaced");
    assert_eq!(fired, Some(point), "{point:?}#{skip}/{action:?}");
    drop(db);
    Crashed {
        dir,
        acked,
        with_inflight,
        last_acked,
    }
}

/// Recovered contents must be exactly the acknowledged state, optionally
/// plus the single in-flight write; never anything else.
fn assert_recovered(c: &Crashed, db: &Lsm, ctx: &str) {
    let dump: Model = db.dump().unwrap().into_iter().collect();
    if dump == c.acked {
        assert!(db.applied_index() >= c.last_acked, "{ctx}");
    } else {
        assert_eq!(
            dump, c.with_inflight,
            "{ctx}: recovered state is neither the acknowledged state nor acknowledged+in-flight"
        );
        assert_eq!(db.applied_index(), c.last_acked + 1, "{ctx}");
    }
    for (key, val) in &c.acked {
        assert_eq!(
            db.get(key).unwrap().as_ref(),
            Some(val),
            "{ctx}: acked {key:?}"
        );
    }
}

const ROTATION_POINTS: [FaultPoint; 5] = [
    FaultPoint::LsmWalCreateDirSync,
    FaultPoint::LsmManifestTmpWrite,
    FaultPoint::LsmManifestTmpSync,
    FaultPoint::LsmManifestRename,
    FaultPoint::LsmManifestDirSync,
];

#[test]
fn crash_during_wal_rotation_leaves_only_empty_orphans_which_are_cleaned() {
    let (mut with_orphan, mut published_new_wal) = (0, 0);
    for point in ROTATION_POINTS {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            let ctx = format!("{point:?}/{action:?}");
            let c = crash_while_writing(point, 0, action);
            let (orphans, stale) = unreferenced(c.dir.path());
            // The protocol claim itself: nothing unreferenced and >= min
            // has ever received a byte.
            assert!(
                orphans.iter().all(|(_, len)| *len == 0),
                "{ctx}: orphan WAL was non-empty: {orphans:?}"
            );
            assert!(
                stale.is_empty(),
                "{ctx}: nothing can be stale yet: {stale:?}"
            );
            with_orphan += usize::from(!orphans.is_empty());
            published_new_wal += usize::from(read_manifest(c.dir.path()).live.len() == 2);

            let db = Lsm::open(c.dir.path(), opts())
                .unwrap_or_else(|e| panic!("{ctx}: recovery refused: {e}"));
            assert_recovered(&c, &db, &ctx);
            assert_wals_are_exactly_live(c.dir.path(), &ctx);

            // The recovered database keeps working and stays recoverable.
            let mut model: Model = db.dump().unwrap().into_iter().collect();
            let from = db.applied_index() + 1;
            fill(&db, &mut model, from..=from + 40);
            drop(db);
            let db = Lsm::open(c.dir.path(), opts()).unwrap();
            assert_model(&db, &model, &format!("{ctx}: after further writes"));
        }
    }
    assert!(
        with_orphan >= 6,
        "sweep must actually produce orphans (got {with_orphan})"
    );
    assert!(
        published_new_wal >= 2,
        "sweep must also cover crashes after the publish took effect (got {published_new_wal})"
    );
}

#[test]
fn crash_after_manifest_publish_before_old_wal_deletion_cleans_the_stale_wal() {
    // Flush publish is the 2nd MANIFEST publication after the fault is
    // armed (rotation is the 1st). Failing at/after the rename leaves the
    // new MANIFEST in place and the retired WAL still on disk.
    let cases = [
        (FaultPoint::LsmManifestRename, FaultAction::FailAfter),
        (FaultPoint::LsmManifestDirSync, FaultAction::FailBefore),
        (FaultPoint::LsmManifestDirSync, FaultAction::FailAfter),
    ];
    for (point, action) in cases {
        let ctx = format!("flush publish {point:?}/{action:?}");
        let c = crash_while_writing(point, 1, action);
        let mf = read_manifest(c.dir.path());
        let (orphans, stale) = unreferenced(c.dir.path());
        assert!(orphans.is_empty(), "{ctx}: {orphans:?}");
        assert!(
            stale.iter().any(|(_, len)| *len > 0),
            "{ctx}: expected the retired, non-empty WAL to be left behind (min={})",
            mf.min
        );
        let db = Lsm::open(c.dir.path(), opts())
            .unwrap_or_else(|e| panic!("{ctx}: recovery refused: {e}"));
        assert_recovered(&c, &db, &ctx);
        assert_wals_are_exactly_live(c.dir.path(), &ctx);
        assert!(
            unreferenced(c.dir.path()).1.is_empty(),
            "{ctx}: stale WAL kept"
        );
    }
}

#[test]
fn missing_required_wal_is_rejected_in_every_crash_state_even_beside_an_orphan() {
    // (a) rotation crashed before publish: live=[1], empty orphan wal-2.
    let c = crash_while_writing(FaultPoint::LsmManifestTmpWrite, 0, FaultAction::FailBefore);
    let mf = read_manifest(c.dir.path());
    let (orphans, _) = unreferenced(c.dir.path());
    assert_eq!((mf.live.clone(), orphans.len()), (vec![1], 1));
    let req = wal_file(c.dir.path(), 1);
    let saved = fs::read(&req).unwrap();
    fs::remove_file(&req).unwrap();
    assert_refused(c.dir.path(), "required WAL gone, orphan present");
    assert!(
        wal_file(c.dir.path(), orphans[0].0).exists(),
        "refusal must not touch the orphan"
    );
    fs::write(&req, saved).unwrap();
    let db = Lsm::open(c.dir.path(), opts()).unwrap();
    assert_recovered(&c, &db, "required WAL restored");
    drop(db);

    // (b) rotation published (live=[1,2]); losing either WAL is fatal.
    let c = crash_while_writing(FaultPoint::LsmManifestRename, 0, FaultAction::FailAfter);
    assert_eq!(read_manifest(c.dir.path()).live, vec![1, 2]);
    for gone in [1u64, 2] {
        let p = wal_file(c.dir.path(), gone);
        let saved = fs::read(&p).unwrap();
        fs::remove_file(&p).unwrap();
        assert_refused(c.dir.path(), &format!("published live WAL {gone} deleted"));
        fs::write(&p, saved).unwrap();
    }
    let db = Lsm::open(c.dir.path(), opts()).unwrap();
    assert_recovered(&c, &db, "published WALs restored");
    drop(db);

    // (c) flush published, stale WAL still present: the live WAL is required.
    let c = crash_while_writing(FaultPoint::LsmManifestRename, 1, FaultAction::FailAfter);
    let mf = read_manifest(c.dir.path());
    assert!(!unreferenced(c.dir.path()).1.is_empty());
    let p = wal_file(c.dir.path(), mf.live[0]);
    fs::remove_file(p).unwrap();
    assert_refused(c.dir.path(), "live WAL deleted, stale WAL present");
}

#[test]
fn non_empty_unreferenced_wal_is_never_deleted_as_an_orphan() {
    // A MANIFEST older than the WALs (restored/rolled back) makes WAL 2
    // look unreferenced and "beyond next_file_id", but it holds acknowledged
    // writes. Deleting it would be silent data loss.
    let fresh = tempfile::tempdir().unwrap();
    drop(Lsm::open(fresh.path(), opts()).unwrap());
    let old_manifest = fs::read(fresh.path().join("MANIFEST")).unwrap();

    let (dir, model, last, wals) = two_live_wals();
    let real_manifest = fs::read(dir.path().join("MANIFEST")).unwrap();
    assert!(fs::metadata(&wals[1]).unwrap().len() > 0);
    fs::write(dir.path().join("MANIFEST"), old_manifest).unwrap();
    assert_refused(dir.path(), "older MANIFEST + non-empty unreferenced WAL");
    assert!(wals[1].exists(), "acknowledged data must not be deleted");

    fs::write(dir.path().join("MANIFEST"), real_manifest).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "genuine MANIFEST restored");
}

#[test]
fn unreferenced_wal_inside_the_allocated_range_is_refused_even_if_empty() {
    // Ids below next_file_id were handed out before the MANIFEST was
    // published, so an unreferenced WAL there is not an interrupted
    // rotation. Fail closed.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=10);
        db.flush().unwrap();
        fill(&db, &mut model, 11..=20);
        db.flush().unwrap();
        fill(&db, &mut model, 21..=23);
    }
    let mf = read_manifest(dir.path());
    let id = (mf.min + 1..mf.next)
        .find(|id| !mf.live.contains(id))
        .unwrap_or_else(|| panic!("no free id in ({}, {}): {mf:?}", mf.min, mf.next));
    fs::write(wal_file(dir.path(), id), b"").unwrap();
    assert_refused(dir.path(), "empty unreferenced WAL below next_file_id");
    fs::remove_file(wal_file(dir.path(), id)).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_model(&db, &model, "after removing the stray file");
}

#[test]
fn snapshot_ingest_crash_is_all_or_nothing_and_orphans_are_cleaned() {
    let snap: Vec<_> = (500..520).map(|i| (k(i), v(i))).collect();
    let snap_model: Model = snap.iter().cloned().collect();
    let (mut old_wins, mut new_wins) = (0, 0);
    for point in ROTATION_POINTS {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            let ctx = format!("replace_all {point:?}/{action:?}");
            fault::clear();
            let dir = tempfile::tempdir().unwrap();
            let mut old = Model::new();
            let db = Lsm::open(dir.path(), opts()).unwrap();
            fill(&db, &mut old, 1..=25);
            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            let r = db.replace_all(snap.clone(), 100);
            let fired = fault::fired();
            fault::clear();
            assert!(
                r.is_err() && fired == Some(point),
                "{ctx}: fault must surface"
            );
            drop(db);

            let (orphans, _) = unreferenced(dir.path());
            assert!(
                orphans.iter().all(|(_, len)| *len == 0),
                "{ctx}: non-empty orphan {orphans:?}"
            );
            let db = Lsm::open(dir.path(), opts())
                .unwrap_or_else(|e| panic!("{ctx}: recovery refused: {e}"));
            let dump: Model = db.dump().unwrap().into_iter().collect();
            if dump == old {
                assert_eq!(db.applied_index(), 25, "{ctx}");
                old_wins += 1;
            } else {
                assert_eq!(dump, snap_model, "{ctx}: neither old state nor snapshot");
                assert_eq!(db.applied_index(), 100, "{ctx}");
                new_wins += 1;
            }
            assert_wals_are_exactly_live(dir.path(), &ctx);
        }
    }
    assert!(
        old_wins > 0 && new_wins > 0,
        "both outcomes expected: {old_wins}/{new_wins}"
    );
}

// legacy v1 MANIFEST

fn crc32(b: &[u8]) -> u32 {
    let mut c = 0xFFFF_FFFFu32;
    for &x in b {
        c ^= x as u32;
        for _ in 0..8 {
            c = if c & 1 == 1 {
                (c >> 1) ^ 0xEDB8_8320
            } else {
                c >> 1
            };
        }
    }
    !c
}

/// Rewrite a v2 MANIFEST in the v1 layout that predates `live_wals`.
fn downgrade_to_v1(dir: &Path) {
    let p = dir.join("MANIFEST");
    let b = fs::read(&p).unwrap();
    let body = &b[..b.len() - 4];
    assert_eq!(&body[..8], b"LSMMANI2");
    let n = u32::from_le_bytes(body[40..44].try_into().unwrap()) as usize;
    let mut v1 = b"LSMMANI1".to_vec();
    v1.extend_from_slice(&body[8..40]);
    v1.extend_from_slice(&body[44 + 8 * n..]);
    let c = crc32(&v1);
    v1.extend_from_slice(&c.to_le_bytes());
    fs::write(p, v1).unwrap();
}

#[test]
fn legacy_v1_with_several_intact_wals_opens_and_is_migrated_to_the_exact_set() {
    let (dir, model, last, wals) = two_live_wals();
    downgrade_to_v1(dir.path());
    let db = Lsm::open(dir.path(), manual()).expect("v1 + intact WALs must open");
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "v1 two WALs");
    drop(db);

    // The migrated MANIFEST records exactly what was found, with
    // next_file_id beyond every live WAL...
    let mf = read_manifest(dir.path());
    assert_eq!(mf.live, vec![1, 2]);
    assert!(mf.next > 2, "{mf:?}");
    // ...so from here on a lost WAL is detected.
    fs::remove_file(&wals[1]).unwrap();
    assert_refused(dir.path(), "newest WAL lost after migration");
}

#[test]
fn legacy_v1_never_deletes_wals_it_cannot_prove_are_garbage() {
    // v1 rotation did not publish, so *any* WAL >= min_wal_id may hold
    // acknowledged writes, whatever its id or size.
    let (dir, model, last, _) = two_live_wals();
    downgrade_to_v1(dir.path());
    let extra = wal_file(dir.path(), 900);
    fs::write(&extra, b"").unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "v1 with extra empty WAL");
    assert!(
        extra.exists(),
        "v1 must not delete a WAL it cannot classify"
    );
    assert!(read_manifest(dir.path()).live.contains(&900));
}

#[test]
fn legacy_v1_missing_oldest_required_wal_is_refused_untouched() {
    let (dir, _, _, wals) = two_live_wals();
    downgrade_to_v1(dir.path());
    fs::remove_file(&wals[0]).unwrap();
    assert_refused(dir.path(), "v1 min_wal_id WAL deleted");
}

#[test]
fn legacy_v1_stale_wal_below_min_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let (old_wal, old_bytes);
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=15);
        old_wal = names(dir.path(), "wal-").pop().unwrap();
        old_bytes = fs::read(&old_wal).unwrap();
        db.flush().unwrap();
        fill(&db, &mut model, 16..=18);
    }
    downgrade_to_v1(dir.path());
    fs::write(&old_wal, old_bytes).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_model(&db, &model, "v1 + stale WAL");
    assert!(!old_wal.exists());
}

#[test]
fn crash_during_legacy_migration_is_recoverable() {
    let (pristine, model, last, _) = two_live_wals();
    downgrade_to_v1(pristine.path());
    let base = image(pristine.path());
    let mut fired_any = 0;
    for point in [
        FaultPoint::LsmManifestTmpWrite,
        FaultPoint::LsmManifestTmpSync,
        FaultPoint::LsmManifestRename,
        FaultPoint::LsmManifestDirSync,
    ] {
        for action in [
            FaultAction::FailBefore,
            FaultAction::FailAfter,
            FaultAction::PartialWrite(7),
        ] {
            if matches!(action, FaultAction::PartialWrite(_))
                && point != FaultPoint::LsmManifestTmpWrite
            {
                continue;
            }
            let ctx = format!("migration {point:?}/{action:?}");
            let dir = tempfile::tempdir().unwrap();
            restore(dir.path(), &base);
            fault::clear();
            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            let first = Lsm::open(dir.path(), manual());
            let fired = fault::fired();
            fault::clear();
            assert_eq!(fired, Some(point), "{ctx}: migration must hit the fault");
            fired_any += 1;
            drop(first);
            let db = Lsm::open(dir.path(), manual())
                .unwrap_or_else(|e| panic!("{ctx}: reopen failed: {e}"));
            assert_eq!(db.applied_index(), last, "{ctx}");
            assert_model(&db, &model, &ctx);
        }
    }
    assert!(fired_any >= 9);
}

#[test]
fn a_freshly_created_wal_is_empty_which_the_orphan_proof_relies_on() {
    // `Lsm::open` only deletes an unreferenced WAL as an interrupted-rotation
    // orphan if it is zero-length, on the premise that creating a WAL writes
    // nothing. If a WAL header is ever added, that premise (and the orphan
    // rule) must be revisited: this test is meant to fail loudly then.
    let dir = tempfile::tempdir().unwrap();
    drop(Lsm::open(dir.path(), opts()).unwrap());
    assert_eq!(disk_wals(dir.path()), vec![(1, 0)], "new WAL must be empty");

    // A rotation creates a second WAL; once published and before any write
    // reaches it, it must be empty too.
    let c = crash_while_writing(FaultPoint::LsmManifestRename, 0, FaultAction::FailAfter);
    let mf = read_manifest(c.dir.path());
    assert_eq!(mf.live, vec![1, 2]);
    assert_eq!(
        disk_wals(c.dir.path())[1],
        (2, 0),
        "rotated WAL must be empty"
    );
}

// MANIFEST consistency (checksum-valid but wrong)
//
// A CRC only proves the bytes are what was written. These tests forge
// MANIFESTs with a *correct* checksum whose contents break an invariant the
// read path silently depends on. Each must be refused, untouched.

/// Byte offsets of each file record in a v2 MANIFEST body (without CRC).
fn file_records(body: &[u8]) -> Vec<usize> {
    let nwal = u32::from_le_bytes(body[40..44].try_into().unwrap()) as usize;
    let mut p = 44 + 8 * nwal;
    let n = u32::from_le_bytes(body[p..p + 4].try_into().unwrap()) as usize;
    p += 4;
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(p);
        p += 8 + 1 + 8 + 8; // id, level, size, entries
        for _ in 0..2 {
            let l = u32::from_le_bytes(body[p..p + 4].try_into().unwrap()) as usize;
            p += 4 + l;
        }
    }
    out
}

fn rec_id(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}
fn rec_level(b: &[u8], off: usize) -> u8 {
    b[off + 8]
}
/// (start, len) of the min_key / max_key bytes of a record.
fn rec_keys(b: &[u8], off: usize) -> ((usize, usize), (usize, usize)) {
    let p = off + 25;
    let l1 = u32::from_le_bytes(b[p..p + 4].try_into().unwrap()) as usize;
    let q = p + 4 + l1;
    let l2 = u32::from_le_bytes(b[q..q + 4].try_into().unwrap()) as usize;
    ((p + 4, l1), (q + 4, l2))
}

/// Mutate the MANIFEST body and re-seal it with a valid checksum.
fn forge_manifest(dir: &Path, f: impl FnOnce(&mut Vec<u8>)) {
    let p = dir.join("MANIFEST");
    let b = fs::read(&p).unwrap();
    let mut body = b[..b.len() - 4].to_vec();
    f(&mut body);
    let c = crc32(&body);
    body.extend_from_slice(&c.to_le_bytes());
    fs::write(p, body).unwrap();
}

/// Compacted database with several non-overlapping L1 tables.
fn compacted_db() -> (tempfile::TempDir, Model) {
    let (dir, model) = four_l0_tables();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    db.compact_all().unwrap();
    drop(db);
    let b = fs::read(dir.path().join("MANIFEST")).unwrap();
    let recs = file_records(&b[..b.len() - 4]);
    let l1 = recs.iter().filter(|o| rec_level(&b, **o) == 1).count();
    assert!(l1 >= 2, "need >=2 L1 tables to forge an overlap, got {l1}");
    (dir, model)
}

#[test]
fn checksum_valid_but_inconsistent_manifests_are_refused_untouched() {
    let (dir, model) = compacted_db();
    let good = fs::read(dir.path().join("MANIFEST")).unwrap();
    let body = &good[..good.len() - 4];
    let recs = file_records(body);
    let mut l1: Vec<usize> = recs
        .iter()
        .copied()
        .filter(|o| rec_level(body, *o) == 1)
        .collect();
    l1.sort_by_key(|o| {
        let ((s, l), _) = rec_keys(body, *o);
        body[s..s + l].to_vec()
    });
    let (first_l1, second_l1) = (l1[0], l1[1]);

    type Forge = Box<dyn Fn(&mut Vec<u8>)>;
    let cases: Vec<(&str, &str, Forge)> = vec![
        (
            "a table at level 2 (invisible to reads)",
            "level",
            Box::new(move |b| b[first_l1 + 8] = 2),
        ),
        (
            "duplicate table id",
            "duplicate",
            Box::new(move |b| {
                let id = rec_id(b, first_l1);
                b[second_l1..second_l1 + 8].copy_from_slice(&id.to_le_bytes());
            }),
        ),
        (
            "next_file_id below live ids",
            "MANIFEST",
            Box::new(|b| b[16..24].copy_from_slice(&2u64.to_le_bytes())),
        ),
        (
            "overlapping L1 tables",
            "overlap",
            Box::new(move |b| {
                let (_, (s, l)) = rec_keys(b, first_l1);
                b[s..s + l].copy_from_slice(&b"key9999"[..l]);
            }),
        ),
        (
            "min_key > max_key",
            "min_key > max_key",
            Box::new(move |b| {
                let ((s, l), _) = rec_keys(b, first_l1);
                b[s..s + l].copy_from_slice(&b"key9999"[..l]);
            }),
        ),
        (
            // Structurally valid (still disjoint), but not what the table holds.
            "key range that disagrees with the real table",
            "key range",
            Box::new(move |b| {
                let ((s, l), _) = rec_keys(b, first_l1);
                b[s..s + l].copy_from_slice(&b"key0000"[..l]);
            }),
        ),
    ];
    for (name, expect, forge) in cases {
        restore_manifest(dir.path(), &good);
        forge_manifest(dir.path(), forge);
        let msg = assert_refused(dir.path(), name);
        assert!(
            msg.to_lowercase().contains(&expect.to_lowercase()),
            "{name}: error should mention {expect:?}, got: {msg}"
        );
    }
    // Nothing above damaged the data: the genuine MANIFEST opens exactly.
    restore_manifest(dir.path(), &good);
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), 48);
    assert_model(&db, &model, "genuine MANIFEST");
}

fn restore_manifest(dir: &Path, bytes: &[u8]) {
    fs::write(dir.join("MANIFEST"), bytes).unwrap();
}

/// Every id in a v2 MANIFEST and on disk must be unique and below next_file_id.
fn assert_id_invariants(dir: &Path, ctx: &str) {
    let b = fs::read(dir.join("MANIFEST")).unwrap();
    let body = &b[..b.len() - 4];
    let mf = read_manifest(dir);
    let mut ids: Vec<u64> = mf.live.clone();
    ids.extend(file_records(body).iter().map(|o| rec_id(body, *o)));
    let n = ids.len();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), n, "{ctx}: duplicate id in MANIFEST");
    assert!(
        ids.iter().all(|i| *i < mf.next),
        "{ctx}: id >= next_file_id {mf:?} {ids:?}"
    );
    // Everything on disk is accounted for and below the allocator.
    for e in fs::read_dir(dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let id = name
            .strip_prefix("sst-")
            .and_then(|s| s.strip_suffix(".sst"))
            .or_else(|| {
                name.strip_prefix("wal-")
                    .and_then(|s| s.strip_suffix(".log"))
            })
            .map(|s| s.parse::<u64>().unwrap());
        if let Some(id) = id {
            assert!(
                ids.contains(&id),
                "{ctx}: {name} on disk but not in MANIFEST"
            );
            assert!(id < mf.next, "{ctx}: {name} >= next_file_id {}", mf.next);
        }
    }
}

#[test]
fn file_ids_are_never_reused_and_stay_below_the_allocator_across_crashes() {
    // Crash at every rotation/publish step, recover, keep working (flushes
    // and compactions allocate more ids), then check the id invariants on a
    // quiescent directory. Any publish that broke them would also have been
    // refused by `validate`, so this exercises both.
    for point in ROTATION_POINTS {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            let ctx = format!("{point:?}/{action:?}");
            let c = crash_while_writing(point, 0, action);
            let db = Lsm::open(c.dir.path(), opts()).unwrap();
            let mut model: Model = db.dump().unwrap().into_iter().collect();
            let from = db.applied_index() + 1;
            fill(&db, &mut model, from..=from + 120);
            db.flush().unwrap();
            db.compact_all().unwrap();
            drop(db);
            let db = Lsm::open(c.dir.path(), opts()).unwrap();
            assert_model(&db, &model, &ctx);
            drop(db);
            assert_id_invariants(c.dir.path(), &ctx);
        }
    }
    // And across interrupted compactions.
    let (pristine, _) = four_l0_tables();
    let base = image(pristine.path());
    for point in [
        FaultPoint::LsmSstTmpWrite,
        FaultPoint::LsmSstRename,
        FaultPoint::LsmManifestTmpWrite,
        FaultPoint::LsmManifestRename,
        FaultPoint::LsmManifestDirSync,
    ] {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            let ctx = format!("compaction {point:?}/{action:?}");
            let dir = tempfile::tempdir().unwrap();
            restore(dir.path(), &base);
            let db = Lsm::open(dir.path(), opts()).unwrap();
            fault::clear();
            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            let _ = db.compact_all();
            fault::clear();
            drop(db);
            let db = Lsm::open(dir.path(), opts()).unwrap();
            db.compact_all().unwrap();
            let mut model: Model = db.dump().unwrap().into_iter().collect();
            fill(&db, &mut model, 49..=80);
            db.flush().unwrap();
            drop(db);
            let _ = Lsm::open(dir.path(), opts()).unwrap();
            assert_id_invariants(dir.path(), &ctx);
        }
    }
}

// two-phase recovery: validate everything, then modify
//
// A refused open must leave *every* file byte-for-byte as found. Before the
// two-phase restructure, an earlier WAL's torn tail was truncated before the
// later WALs had been read, so a refusal discovered afterwards had already
// modified the database (and orphan/temp cleanup and the legacy MANIFEST
// upgrade likewise ran before replay could still refuse).

/// Reference model after applying ops `1..=n` (no engine involved).
fn model_upto(n: u64) -> Model {
    let mut m = Model::new();
    for i in 1..=n {
        if i % 5 == 0 {
            m.remove(&k(i - 3));
        } else {
            m.insert(k(i), v(i));
        }
    }
    m
}

#[derive(Clone, Copy, Debug)]
enum Tear {
    /// Cut the file short in the middle of its last record.
    Truncate,
    /// Zero padding after the data (a size-extending crash).
    ZeroPad,
    /// The final record is damaged but nothing follows it.
    DamagedFinalRecord,
}
const TEARS: [Tear; 3] = [Tear::Truncate, Tear::ZeroPad, Tear::DamagedFinalRecord];

fn tear(path: &Path, how: Tear) {
    let mut b = fs::read(path).unwrap();
    match how {
        Tear::Truncate => b.truncate(b.len() - 3),
        Tear::ZeroPad => b.extend_from_slice(&[0; 11]),
        Tear::DamagedFinalRecord => *b.last_mut().unwrap() ^= 0xff,
    }
    fs::write(path, b).unwrap();
}

/// Is a torn tail on `how` expected to cost the last record?
fn loses_last_record(how: Tear) -> bool {
    !matches!(how, Tear::ZeroPad)
}

#[test]
fn torn_earlier_wal_with_records_in_a_later_wal_is_refused_with_every_file_unchanged() {
    for how in TEARS {
        let ctx = format!("{how:?}");
        let (dir, model, last, wals) = two_live_wals();
        let genuine = image(dir.path());
        tear(&wals[0], how);
        let torn = image(dir.path());
        assert_ne!(torn, genuine, "{ctx}: the tear must change the file");

        let msg = assert_refused(dir.path(), &ctx); // also compares every file
        assert!(msg.contains("torn tail"), "{ctx}: {msg}");
        // A second attempt is equally harmless and gives the same answer.
        assert_eq!(assert_refused(dir.path(), &ctx), msg);
        assert_eq!(
            image(dir.path()),
            torn,
            "{ctx}: refusal modified the directory"
        );

        // Nothing was lost by refusing: with the file intact it recovers fully.
        restore(dir.path(), &genuine);
        let db = Lsm::open(dir.path(), manual()).unwrap();
        assert_eq!(db.applied_index(), last, "{ctx}");
        assert_model(&db, &model, &ctx);
    }
}

#[test]
fn refusal_leaves_orphan_temp_and_garbage_files_in_place() {
    // Cleanup of these must not run before replay can still refuse.
    for how in TEARS {
        let ctx = format!("{how:?}");
        let (dir, model, last, wals) = two_live_wals();
        let mf = read_manifest(dir.path());
        let orphan = wal_file(dir.path(), mf.next + 50); // empty, >= next_file_id
        let extras = [
            (orphan.clone(), Vec::new()),
            (dir.path().join("MANIFEST.tmp"), b"half-written".to_vec()),
            (dir.path().join("sst-0000009999.sst.tmp"), b"junk".to_vec()),
            (
                dir.path().join("sst-0000009998.sst"),
                b"unreferenced".to_vec(),
            ),
        ];
        for (p, bytes) in &extras {
            fs::write(p, bytes).unwrap();
        }
        let before_tear = image(dir.path());
        tear(&wals[0], how);
        let torn = image(dir.path());

        assert_refused(dir.path(), &ctx);
        for (p, bytes) in &extras {
            assert_eq!(
                &fs::read(p).unwrap(),
                bytes,
                "{ctx}: {} changed",
                p.display()
            );
        }
        assert_eq!(image(dir.path()), torn);

        // Once the sequence is acceptable, the same open cleans them up.
        restore(dir.path(), &before_tear);
        let db = Lsm::open(dir.path(), manual()).unwrap();
        assert_model(&db, &model, &ctx);
        assert_eq!(db.applied_index(), last);
        for (p, _) in &extras {
            assert!(
                !p.exists(),
                "{ctx}: {} should be cleaned after acceptance",
                p.display()
            );
        }
    }
}

#[test]
fn damage_in_a_later_wal_refuses_before_an_earlier_torn_tail_is_truncated() {
    // The later WAL's mid-log corruption is only discovered after the earlier
    // WAL has been parsed; the earlier file must still be untouched.
    for how in TEARS {
        let ctx = format!("{how:?}");
        let (dir, _, _, wals) = two_live_wals();
        tear(&wals[0], how);
        let mut later = fs::read(&wals[1]).unwrap();
        later[10] ^= 0xff; // not the last record: genuine corruption
        fs::write(&wals[1], later).unwrap();
        assert_refused(dir.path(), &ctx);
    }
}

#[test]
fn legacy_v1_refusal_does_not_upgrade_the_manifest() {
    // The v1 -> v2 upgrade must not be written before replay can still refuse.
    for how in TEARS {
        let ctx = format!("{how:?}");
        let (dir, _, _, wals) = two_live_wals();
        downgrade_to_v1(dir.path());
        tear(&wals[0], how);
        let v1 = fs::read(dir.path().join("MANIFEST")).unwrap();
        assert_eq!(&v1[..8], b"LSMMANI1");
        assert_refused(dir.path(), &ctx);
        assert_eq!(fs::read(dir.path().join("MANIFEST")).unwrap(), v1, "{ctx}");
    }
}

#[test]
fn torn_tail_in_the_final_wal_is_still_recovered_and_truncated() {
    for how in TEARS {
        let ctx = format!("{how:?}");
        let (dir, _, last, wals) = two_live_wals();
        let before = fs::metadata(&wals[1]).unwrap().len();
        tear(&wals[1], how);
        let expect = if loses_last_record(how) {
            last - 1
        } else {
            last
        };

        let db = Lsm::open(dir.path(), manual())
            .unwrap_or_else(|e| panic!("{ctx}: final-WAL torn tail must recover: {e}"));
        assert_eq!(db.applied_index(), expect, "{ctx}");
        assert_model(&db, &model_upto(expect), &ctx);
        drop(db);
        // The torn bytes were cut off, so the file is a clean prefix again.
        let after = fs::metadata(&wals[1]).unwrap().len();
        assert!(after <= before, "{ctx}: {after} > {before}");
        assert!(fs::read(&wals[0]).is_ok());

        // And the database keeps working across a further restart.
        let db = Lsm::open(dir.path(), manual()).unwrap();
        let mut m = model_upto(expect);
        step(&db, &mut m, expect + 1).unwrap();
        drop(db);
        let db = Lsm::open(dir.path(), manual()).unwrap();
        assert_model(&db, &m, &format!("{ctx}: after further write"));
    }
}

#[test]
fn torn_earlier_wal_followed_only_by_empty_wals_remains_recoverable() {
    // Pinned, existing behaviour (not weakened): a torn tail is only fatal
    // when a *later* WAL holds records. Here wal-1 has the data and wal-2 is
    // the empty WAL created by an interrupted rotation.
    for how in TEARS {
        let ctx = format!("{how:?}");
        let c = crash_while_writing(FaultPoint::LsmManifestRename, 0, FaultAction::FailAfter);
        let wals = names(c.dir.path(), "wal-");
        assert_eq!(wals.len(), 2);
        assert_eq!(
            fs::metadata(&wals[1]).unwrap().len(),
            0,
            "later WAL is empty"
        );
        tear(&wals[0], how);
        let db = Lsm::open(c.dir.path(), opts())
            .unwrap_or_else(|e| panic!("{ctx}: torn earlier WAL + empty later WAL: {e}"));
        let applied = db.applied_index();
        assert_model(&db, &model_upto(applied), &ctx);
        let want = c.last_acked + 1; // the in-flight record was the last one in wal-1
        let expect = if loses_last_record(how) {
            want - 1
        } else {
            want
        };
        assert_eq!(applied, expect, "{ctx}");
        assert_wals_are_exactly_live(c.dir.path(), &ctx);
    }
}

// `applied` ordering contract (recovery + write guard)
//
// Contract: a WAL record's `applied` is 0 ("no Raft index", direct engine
// use) or a positive Raft index. Positive indexes are strictly increasing:
// each must exceed every earlier positive one (earlier records in the same
// WAL, then records of earlier WALs, in ascending WAL id), starting above
// the MANIFEST's own `applied`. Gaps are fine; zeros carry no ordering
// information and are accepted anywhere, any number of times.
// Recovery refuses a WAL that breaks this, and `apply` refuses to write one.

/// A CRC-valid WAL frame: one Put with the given `applied` index.
fn frame(applied: u64, n: u64) -> Vec<u8> {
    let (key, val) = (
        format!("k{n:03}").into_bytes(),
        format!("v{n:03}").into_bytes(),
    );
    let mut p = applied.to_le_bytes().to_vec();
    p.extend_from_slice(&1u32.to_le_bytes()); // one op
    p.push(1); // TAG_PUT
    p.extend_from_slice(&(key.len() as u32).to_le_bytes());
    p.extend_from_slice(&key);
    p.extend_from_slice(&(val.len() as u32).to_le_bytes());
    p.extend_from_slice(&val);
    let mut f = (p.len() as u32).to_le_bytes().to_vec();
    f.extend_from_slice(&crc32(&p).to_le_bytes());
    f.extend_from_slice(&p);
    f
}

/// Directory whose live WALs hold exactly these `applied` sequences
/// (MANIFEST `applied` = 0). Returns the dir and the model the frames imply.
fn forged(wals: &[&[u64]]) -> (tempfile::TempDir, Model) {
    let dir = match wals.len() {
        1 => {
            let d = tempfile::tempdir().unwrap();
            drop(Lsm::open(d.path(), manual()).unwrap());
            d
        }
        2 => {
            let c = crash_while_writing(FaultPoint::LsmManifestRename, 0, FaultAction::FailAfter);
            assert_eq!(read_manifest(c.dir.path()).live, vec![1, 2]);
            c.dir
        }
        n => panic!("forged supports 1 or 2 WALs, got {n}"),
    };
    let mut model = Model::new();
    let mut n = 0;
    for (i, seq) in wals.iter().enumerate() {
        let mut bytes = Vec::new();
        for &a in *seq {
            n += 1;
            bytes.extend_from_slice(&frame(a, n));
            model.insert(
                format!("k{n:03}").into_bytes(),
                format!("v{n:03}").into_bytes(),
            );
        }
        fs::write(wal_file(dir.path(), i as u64 + 1), bytes).unwrap();
    }
    (dir, model)
}

#[test]
fn strictly_increasing_positive_indexes_and_zero_records_are_accepted() {
    let cases: Vec<(&str, Vec<&[u64]>, u64)> = vec![
        ("increasing", vec![&[1, 2, 3]], 3),
        ("gaps are fine", vec![&[2, 5, 9]], 9),
        ("zeros anywhere, repeated", vec![&[0, 3, 0, 0, 4, 0]], 4),
        ("only zeros", vec![&[0, 0, 0]], 0),
        ("zero-only later WAL", vec![&[7, 8], &[0, 0]], 8),
        ("zeros then positives across WALs", vec![&[0], &[0, 5]], 5),
        ("increasing across WALs", vec![&[3, 4], &[5, 9]], 9),
        ("zero between WALs", vec![&[3, 0], &[0, 4]], 4),
    ];
    for (name, wals, want) in cases {
        let (dir, model) = forged(&wals);
        let db = Lsm::open(dir.path(), manual())
            .unwrap_or_else(|e| panic!("{name}: valid sequence refused: {e}"));
        assert_eq!(db.applied_index(), want, "{name}");
        assert_model(&db, &model, name);
        drop(db);
        // And it is stable across another restart.
        let db = Lsm::open(dir.path(), manual()).unwrap();
        assert_model(&db, &model, &format!("{name}: second open"));
    }
}

#[test]
fn contradictory_applied_sequences_are_refused_untouched() {
    let cases: Vec<(&str, Vec<&[u64]>)> = vec![
        ("duplicate positive index", vec![&[3, 3]]),
        ("out of order", vec![&[5, 4]]),
        ("regression after a gap", vec![&[2, 5, 3]]),
        ("later WAL has earlier indexes", vec![&[5, 6], &[2, 3]]),
        ("same index ends one WAL, starts the next", vec![&[4], &[4]]),
        (
            "repeat after a zero in the next WAL",
            vec![&[1, 2], &[0, 1]],
        ),
    ];
    for (name, wals) in cases {
        let (dir, _) = forged(&wals);
        let msg = assert_refused(dir.path(), name); // every file unchanged
        assert!(msg.contains("applied"), "{name}: {msg}");
        assert_eq!(
            assert_refused(dir.path(), name),
            msg,
            "{name}: not repeatable"
        );
    }
}

#[test]
fn swapped_live_wals_are_refused_untouched() {
    // Two real WALs whose contents were exchanged: replay order would no
    // longer match apply order, silently reordering the operations.
    let (dir, model, last, wals) = two_live_wals();
    let (a, b) = (fs::read(&wals[0]).unwrap(), fs::read(&wals[1]).unwrap());
    fs::write(&wals[0], &b).unwrap();
    fs::write(&wals[1], &a).unwrap();
    let msg = assert_refused(dir.path(), "swapped WAL contents");
    assert!(msg.contains("applied"), "{msg}");
    fs::write(&wals[0], a).unwrap();
    fs::write(&wals[1], b).unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "swap undone");
}

#[test]
fn an_ordering_violation_is_refused_before_a_torn_tail_is_truncated() {
    let (dir, _) = forged(&[&[5, 4, 6]]);
    let w = wal_file(dir.path(), 1);
    let mut b = fs::read(&w).unwrap();
    b.truncate(b.len() - 3); // tear the third frame
    fs::write(&w, b).unwrap();
    let msg = assert_refused(dir.path(), "violation + torn tail"); // file unchanged
    assert!(msg.contains("applied"), "{msg}");
}

#[test]
fn indexes_at_or_below_the_manifests_applied_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=20);
        db.flush().unwrap(); // MANIFEST applied = 20; the active WAL is empty
    }
    let active = names(dir.path(), "wal-").pop().unwrap();
    let genuine = image(dir.path());
    for (name, seq, ok) in [
        ("equal to MANIFEST applied", vec![20u64], false),
        ("below MANIFEST applied", vec![3], false),
        ("zero then stale", vec![0, 7], false),
        ("above MANIFEST applied", vec![21, 22], true),
        ("zeros only", vec![0], true),
    ] {
        restore(dir.path(), &genuine);
        let bytes: Vec<u8> = seq.iter().flat_map(|a| frame(*a, 900 + a)).collect();
        fs::write(&active, bytes).unwrap();
        if ok {
            let db = Lsm::open(dir.path(), manual())
                .unwrap_or_else(|e| panic!("{name}: valid sequence refused: {e}"));
            assert!(db.applied_index() >= 20, "{name}");
        } else {
            let msg = assert_refused(dir.path(), name);
            assert!(msg.contains("applied"), "{name}: {msg}");
        }
    }
}

#[test]
fn apply_rejects_a_non_increasing_index_before_writing_anything() {
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), manual()).unwrap();
    let put = |k: &str| [Op::Put(k.as_bytes().to_vec(), b"v".to_vec())];
    db.apply(&put("a"), 5).unwrap();
    let wal = names(dir.path(), "wal-").pop().unwrap();
    let len = fs::metadata(&wal).unwrap().len();
    for bad in [5u64, 4, 1] {
        match db.apply(&put("rejected"), bad) {
            Err(LsmError::InvalidInput(m)) => assert!(m.contains("strictly increasing"), "{m}"),
            other => panic!(
                "index {bad}: expected InvalidInput, got {:?}",
                other.is_ok()
            ),
        }
    }
    assert_eq!(
        fs::metadata(&wal).unwrap().len(),
        len,
        "nothing may be appended"
    );
    assert!(
        !db.is_poisoned(),
        "a rejected argument must not poison the engine"
    );
    assert_eq!(db.get(b"rejected").unwrap(), None);
    // Zero is always allowed, and a larger index still works afterwards.
    db.apply(&put("z1"), 0).unwrap();
    db.apply(&put("six"), 6).unwrap();
    db.apply(&put("z2"), 0).unwrap();
    drop(db);
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), 6);
    for k in ["a", "z1", "six", "z2"] {
        assert!(db.get(k.as_bytes()).unwrap().is_some(), "{k}");
    }
    assert_eq!(db.get(b"rejected").unwrap(), None);
}

#[test]
fn snapshot_ingest_may_lower_the_index_and_later_writes_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    fill(&db, &mut model, 1..=20);
    let snap: Vec<_> = (500..510).map(|i| (k(i), v(i))).collect();
    db.replace_all(snap.clone(), 10).unwrap(); // lower than the current 20
    assert_eq!(db.applied_index(), 10);
    assert!(db.apply(&[Op::Put(k(1), v(1))], 10).is_err());
    let mut after: Model = snap.into_iter().collect();
    db.apply(&[Op::Put(k(77), v(77))], 11).unwrap();
    after.insert(k(77), v(77));
    drop(db);
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), 11);
    assert_model(&db, &after, "after lowering ingest");
}

#[test]
fn whatever_the_engine_writes_the_ordering_check_accepts() {
    // A deterministic mix of increasing/zero applies, flushes, compactions
    // and restarts. The write guard and the recovery check must agree.
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let mut db = Lsm::open(dir.path(), opts()).unwrap();
    let (mut idx, mut seed) = (0u64, 0x2545_F491_4F6C_DD1Du64);
    for n in 0..400u64 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        match (seed >> 33) % 10 {
            0 => db.flush().unwrap(),
            1 => db.compact_all().unwrap(),
            2 => {
                drop(db);
                db = Lsm::open(dir.path(), opts()).unwrap();
                assert_eq!(db.applied_index(), idx, "restart at step {n}");
                assert_model(&db, &model, &format!("restart at step {n}"));
            }
            3 => {
                let key = k(1000 + n);
                db.apply(&[Op::Put(key.clone(), v(n))], 0).unwrap();
                model.insert(key, v(n));
            }
            _ => {
                idx += 1 + (seed >> 40) % 3; // gaps allowed
                let key = k(n % 37);
                db.apply(&[Op::Put(key.clone(), v(n))], idx).unwrap();
                model.insert(key, v(n));
            }
        }
    }
    drop(db);
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.applied_index(), idx);
    assert_model(&db, &model, "final");
}

// phase-2 crash sweep: every recovery operation, every action

fn write_at(dir: &Path, name: &str, bytes: &[u8]) {
    fs::write(dir.join(name), bytes).unwrap();
}

/// A directory needing every kind of phase-2 work: a torn tail in the
/// final WAL, a retired WAL, stale temp files and an unreferenced table;
/// `v2` adds an orphan WAL from an interrupted rotation, otherwise the
/// MANIFEST is legacy v1 and must be upgraded.
fn messy_lsm_image(v2: bool) -> (tempfile::TempDir, DirImage) {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    let (retired, retired_bytes);
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=15);
        retired = names(dir.path(), "wal-").pop().unwrap();
        retired_bytes = fs::read(&retired).unwrap();
        db.flush().unwrap();
        fill(&db, &mut model, 16..=20); // unflushed tail
    }
    fs::write(&retired, retired_bytes).unwrap(); // its unlink "never happened"
    let active = names(dir.path(), "wal-").pop().unwrap();
    tear(&active, Tear::Truncate); // loses op 20
    if v2 {
        let mf = read_manifest(dir.path());
        write_at(dir.path(), &format!("wal-{:010}.log", mf.next + 50), b""); // orphan
    } else {
        downgrade_to_v1(dir.path());
    }
    write_at(dir.path(), "MANIFEST.tmp", b"half-written manifest");
    write_at(dir.path(), "sst-0000007777.sst.tmp", b"half-written table");
    write_at(dir.path(), "sst-0000007778.sst", b"unreferenced table");
    let img = image(dir.path());
    (dir, img)
}

fn sweep_phase_two(v2: bool, extra_points: &[FaultPoint]) -> usize {
    let (clean, pristine) = messy_lsm_image(v2);
    fault::clear();
    let db = Lsm::open(clean.path(), manual()).unwrap();
    let expected_applied = db.applied_index();
    assert_eq!(expected_applied, 19, "torn op 20 is dropped");
    let expected: Model = db.dump().unwrap().into_iter().collect();
    assert_eq!(expected, model_upto(19));
    drop(db);
    // Only the live WALs remain: the retired WAL and (v2) the orphan are gone.
    assert_wals_are_exactly_live(clean.path(), &format!("clean recovery v2={v2}"));
    let settled = image(clean.path());
    assert!(
        !settled
            .keys()
            .any(|n| n.ends_with(".tmp") || n == "sst-0000007778.sst"),
        "clean recovery must remove the garbage: {:?}",
        settled.keys().collect::<Vec<_>>()
    );

    let mut points = vec![
        FaultPoint::LsmWalRecoverTruncate,
        FaultPoint::LsmWalRecoverSync,
        FaultPoint::LsmRecoverRemove,
    ];
    points.extend_from_slice(extra_points);

    let probe = tempfile::tempdir().unwrap();
    restore(probe.path(), &pristine);
    fault::clear();
    drop(Lsm::open(probe.path(), manual()).unwrap());
    let counts: Vec<_> = points.iter().map(|p| (*p, fault::hits(*p))).collect();
    fault::clear();
    assert!(
        counts.iter().all(|(_, n)| *n >= 1),
        "recovery must reach every phase-2 operation: {counts:?}"
    );
    let removals = counts
        .iter()
        .find(|(p, _)| *p == FaultPoint::LsmRecoverRemove)
        .unwrap()
        .1;
    assert!(
        removals >= if v2 { 5 } else { 4 },
        "removals reached: {removals}"
    );

    let mut runs = 0;
    for (point, hits) in counts {
        for skip in 0..hits {
            for action in [
                FaultAction::FailBefore,
                FaultAction::FailAfter,
                FaultAction::PartialWrite(7),
            ] {
                if matches!(action, FaultAction::PartialWrite(_))
                    && point != FaultPoint::LsmManifestTmpWrite
                {
                    continue;
                }
                let ctx = format!("v2={v2} {point:?}#{skip}/{action:?}");
                let dir = tempfile::tempdir().unwrap();
                restore(dir.path(), &pristine);
                fault::clear();
                fault::arm(FaultPlan {
                    point,
                    skip,
                    action,
                });
                let first = Lsm::open(dir.path(), manual());
                let fired = fault::fired();
                fault::clear();
                assert_eq!(fired, Some(point), "{ctx}: fault did not fire");
                assert!(first.is_err(), "{ctx}: the crash must abort the open");
                drop(first);

                // Recovery repeated after the interruption converges...
                let db = Lsm::open(dir.path(), manual())
                    .unwrap_or_else(|e| panic!("{ctx}: re-recovery failed: {e}"));
                assert_eq!(db.applied_index(), expected_applied, "{ctx}");
                assert_model(&db, &expected, &ctx);
                drop(db);
                assert_eq!(
                    image(dir.path()),
                    settled,
                    "{ctx}: did not converge to the clean result"
                );
                // ...and a further recovery changes nothing.
                let db = Lsm::open(dir.path(), manual()).unwrap();
                assert_model(&db, &expected, &format!("{ctx}: third open"));
                drop(db);
                assert_eq!(image(dir.path()), settled, "{ctx}: recovery not idempotent");
                runs += 1;
            }
        }
    }
    runs
}

#[test]
fn crash_at_every_phase_two_step_of_a_v2_recovery_converges() {
    let runs = sweep_phase_two(true, &[]);
    assert!(runs >= 14, "sweep suspiciously small: {runs}");
}

#[test]
fn crash_at_every_phase_two_step_of_a_legacy_upgrade_converges() {
    // Adds the legacy MANIFEST publication (4 steps, plus a torn write).
    let runs = sweep_phase_two(
        false,
        &[
            FaultPoint::LsmManifestTmpWrite,
            FaultPoint::LsmManifestTmpSync,
            FaultPoint::LsmManifestRename,
            FaultPoint::LsmManifestDirSync,
        ],
    );
    assert!(runs >= 19, "sweep suspiciously small: {runs}");
}
