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

/// Rewrite a v2 MANIFEST as the v1 format that shipped before `live_wals`.
fn downgrade_manifest_to_v1(dir: &Path) {
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
fn legacy_v1_single_wal_migrates() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let db = Lsm::open(dir.path(), manual()).unwrap();
        fill(&db, &mut model, 1..=20);
        db.flush().unwrap();
        fill(&db, &mut model, 21..=25);
    }
    downgrade_manifest_to_v1(dir.path());
    let db = Lsm::open(dir.path(), manual()).unwrap();
    assert_eq!(db.applied_index(), 25);
    assert_model(&db, &model, "v1 single WAL");
}

#[test]
fn legacy_v1_crashed_with_two_live_wals() {
    // A v1 database that crashed between rotation and flush: both WALs
    // hold acknowledged data and nothing is missing.
    let (dir, model, last, _) = two_live_wals();
    downgrade_manifest_to_v1(dir.path());
    let db = Lsm::open(dir.path(), manual()).expect("v1 dir with intact WALs must open");
    assert_eq!(db.applied_index(), last);
    assert_model(&db, &model, "v1 two WALs");
}
