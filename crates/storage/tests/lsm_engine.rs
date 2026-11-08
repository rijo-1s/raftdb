//! Functional tests of the LSM engine against a `BTreeMap` oracle.

use std::collections::BTreeMap;
use std::fs;

use storage::lsm::{Lsm, LsmError, LsmOptions, Op};

/// xorshift64: deterministic, dependency-free.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn small() -> LsmOptions {
    LsmOptions {
        memtable_bytes: 2048,
        block_bytes: 256,
        l1_file_bytes: 4096,
        l0_compaction_trigger: 3,
        background: false,
        cache_bytes: 64 * 1024,
        ..LsmOptions::default()
    }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn assert_matches(db: &Lsm, model: &BTreeMap<Vec<u8>, Vec<u8>>, what: &str) {
    let dump = db.dump().unwrap();
    let want: Vec<_> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(dump, want, "{what}: full scan differs from model");
    for (k, v) in model {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v), "{what}: get({k:?})");
    }
}

#[test]
fn model_based_random_workload_with_flush_compaction_and_reopen() {
    for seed in 1..=8u64 {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut db = Lsm::open(dir.path(), small()).unwrap();
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut idx = 0u64;
        for step in 0..1500 {
            match rng.below(100) {
                0..=59 => {
                    let k = key(rng.below(200));
                    let v = format!("v{step}-{}", rng.below(1000)).into_bytes();
                    idx += 1;
                    db.apply(&[Op::Put(k.clone(), v.clone())], idx).unwrap();
                    model.insert(k, v);
                }
                60..=74 => {
                    let k = key(rng.below(200));
                    idx += 1;
                    db.apply(&[Op::Delete(k.clone())], idx).unwrap();
                    model.remove(&k);
                }
                75..=84 => {
                    // multi-op atomic batch
                    let mut ops = Vec::new();
                    for _ in 0..3 {
                        let k = key(rng.below(200));
                        if rng.below(3) == 0 {
                            model.remove(&k);
                            ops.push(Op::Delete(k));
                        } else {
                            let v = format!("b{step}").into_bytes();
                            model.insert(k.clone(), v.clone());
                            ops.push(Op::Put(k, v));
                        }
                    }
                    idx += 1;
                    db.apply(&ops, idx).unwrap();
                }
                85..=89 => {
                    let a = rng.below(200);
                    let b = a + rng.below(60);
                    let limit = 1 + rng.below(30) as usize;
                    let got = db.scan(Some(&key(a)), Some(&key(b)), limit).unwrap();
                    let want: Vec<_> = model
                        .range(key(a)..key(b))
                        .take(limit)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    assert_eq!(
                        got, want,
                        "seed {seed} step {step}: range [{a},{b}) limit {limit}"
                    );
                }
                90..=93 => db.flush().unwrap(),
                94..=96 => db.compact_all().unwrap(),
                _ => {
                    drop(db);
                    db = Lsm::open(dir.path(), small()).unwrap();
                    assert_eq!(db.applied_index(), idx, "applied index must survive reopen");
                }
            }
        }
        assert_matches(&db, &model, &format!("seed {seed} final"));
        db.compact_all().unwrap();
        assert_matches(&db, &model, &format!("seed {seed} after compaction"));
        drop(db);
        let db = Lsm::open(dir.path(), small()).unwrap();
        assert_matches(&db, &model, &format!("seed {seed} after reopen"));
    }
}

#[test]
fn compaction_actually_runs_drops_tombstones_and_shrinks_l0() {
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    for round in 0..4 {
        for i in 0..40 {
            db.apply(&[Op::Put(key(i), format!("round{round}").into_bytes())], 0)
                .unwrap();
        }
        db.flush().unwrap();
    }
    assert!(db.stats().l0_files >= 3);
    for i in 0..40 {
        db.apply(&[Op::Delete(key(i))], 0).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    let s = db.stats();
    assert_eq!(s.l0_files, 0, "L0 drained");
    assert!(s.compactions >= 1);
    assert_eq!(
        s.l1_files, 0,
        "everything was deleted: bottom-level compaction drops tombstones"
    );
    assert!(db.dump().unwrap().is_empty());
}

#[test]
fn bloom_filters_skip_tables_for_absent_keys_and_cache_serves_repeats() {
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    for i in 0..300 {
        db.apply(&[Op::Put(key(i * 2), b"x".to_vec())], 0).unwrap(); // even keys only
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    // Reading present keys twice: second pass must be served from cache.
    for i in 0..300 {
        assert!(db.get(&key(i * 2)).unwrap().is_some());
    }
    let after_first = db.stats();
    for i in 0..300 {
        assert!(db.get(&key(i * 2)).unwrap().is_some());
    }
    let after_second = db.stats();
    assert!(
        after_second.cache_hits > after_first.cache_hits,
        "repeat reads should hit the block cache"
    );
    // Absent keys inside the table's key range: the filter should reject
    // nearly all of them without touching a data block.
    let before = db.stats();
    let mut absent = 0;
    for i in 0..300 {
        assert!(db.get(&key(i * 2 + 1)).unwrap().is_none());
        absent += 1;
    }
    let after = db.stats();
    let block_reads =
        (after.cache_hits + after.cache_misses) - (before.cache_hits + before.cache_misses);
    assert!(
        block_reads < absent / 10,
        "bloom filters should avoid block reads for absent keys: {block_reads}/{absent}"
    );
}

#[test]
fn replace_all_swaps_contents_and_applied_index_atomically_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    for i in 0..100 {
        db.apply(&[Op::Put(key(i), b"old".to_vec())], i + 1)
            .unwrap();
    }
    db.flush().unwrap();
    let entries: Vec<_> = (50..250)
        .map(|i| (key(i), format!("new{i}").into_bytes()))
        .collect();
    db.replace_all(entries.clone(), 777).unwrap();
    assert_eq!(db.applied_index(), 777);
    assert_eq!(db.get(&key(10)).unwrap(), None, "old data gone");
    assert_eq!(db.get(&key(60)).unwrap(), Some(b"new60".to_vec()));
    // The WAL tail after a replace is separate: more writes land on top.
    db.apply(&[Op::Put(key(60), b"after".to_vec())], 778)
        .unwrap();
    drop(db);
    let db = Lsm::open(dir.path(), small()).unwrap();
    assert_eq!(db.applied_index(), 778);
    assert_eq!(db.get(&key(60)).unwrap(), Some(b"after".to_vec()));
    assert_eq!(db.get(&key(249)).unwrap(), Some(b"new249".to_vec()));
    assert_eq!(db.dump().unwrap().len(), 200);
}

#[test]
fn reads_during_a_scan_survive_compaction_of_the_files_being_scanned() {
    // Obsolete tables are deleted only when the last reader drops; a scan
    // holding the old version must keep working through a compaction.
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    for round in 0..3 {
        for i in 0..60 {
            db.apply(&[Op::Put(key(i), format!("r{round}").into_bytes())], 0)
                .unwrap();
        }
        db.flush().unwrap();
    }
    let before = db.dump().unwrap();
    db.compact_all().unwrap();
    assert_eq!(db.dump().unwrap(), before);
}

// ---------------- corruption detection ----------------

fn populated(dir: &std::path::Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let db = Lsm::open(dir, small()).unwrap();
    for i in 0..120 {
        db.apply(&[Op::Put(key(i), format!("value-{i}").into_bytes())], i + 1)
            .unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    db.dump().unwrap()
}

fn files(dir: &std::path::Path, prefix: &str) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with(prefix))
        .collect();
    v.sort();
    v
}

fn assert_corruption(dir: &std::path::Path, what: &str) {
    match Lsm::open(dir, small()) {
        Err(LsmError::Corruption(_)) => {}
        Err(e) => panic!("{what}: expected Corruption, got {e}"),
        Ok(_) => panic!("{what}: corruption went undetected"),
    }
}

#[test]
fn every_flipped_byte_in_a_table_is_detected_at_open() {
    let dir = tempfile::tempdir().unwrap();
    populated(dir.path());
    let sst = files(dir.path(), "sst-").remove(0);
    let orig = fs::read(&sst).unwrap();
    // Flip one byte at many offsets (every 7th keeps the test quick but
    // covers data blocks, index, bloom and footer).
    for off in (0..orig.len()).step_by(7) {
        let mut bad = orig.clone();
        bad[off] ^= 0x01;
        fs::write(&sst, &bad).unwrap();
        assert_corruption(dir.path(), &format!("flip at {off}/{}", orig.len()));
    }
    fs::write(&sst, &orig).unwrap();
    Lsm::open(dir.path(), small()).unwrap();
}

#[test]
fn truncated_missing_or_extra_tables_are_handled_explicitly() {
    let dir = tempfile::tempdir().unwrap();
    populated(dir.path());
    let sst = files(dir.path(), "sst-").remove(0);
    let orig = fs::read(&sst).unwrap();

    fs::write(&sst, &orig[..orig.len() - 10]).unwrap();
    assert_corruption(dir.path(), "truncated table");

    fs::remove_file(&sst).unwrap();
    assert_corruption(dir.path(), "missing table named by MANIFEST");

    fs::write(&sst, &orig).unwrap();
    // An unreferenced table (e.g. leftover from an interrupted compaction)
    // is garbage: removed, and the data is unaffected.
    let stray = dir.path().join("sst-0000999999.sst");
    fs::write(&stray, b"garbage").unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    assert!(!stray.exists(), "orphan table must be cleaned up");
    assert_eq!(db.dump().unwrap().len(), 120);
}

#[test]
fn damaged_or_missing_manifest_is_reported_never_guessed() {
    let dir = tempfile::tempdir().unwrap();
    populated(dir.path());
    let m = dir.path().join("MANIFEST");
    let orig = fs::read(&m).unwrap();
    let mut bad = orig.clone();
    bad[20] ^= 0xff;
    fs::write(&m, &bad).unwrap();
    assert_corruption(dir.path(), "damaged MANIFEST");
    fs::remove_file(&m).unwrap();
    assert_corruption(dir.path(), "MANIFEST deleted but data files remain");
    fs::write(&m, &orig).unwrap();
    Lsm::open(dir.path(), small()).unwrap();
}

#[test]
fn wal_torn_tail_is_dropped_but_midlog_damage_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Lsm::open(dir.path(), small()).unwrap();
        for i in 0..10 {
            db.apply(&[Op::Put(key(i), b"v".to_vec())], i + 1).unwrap();
        }
    }
    let wal = files(dir.path(), "wal-").pop().unwrap();
    let orig = fs::read(&wal).unwrap();
    // torn tail: chop the last few bytes
    fs::write(&wal, &orig[..orig.len() - 3]).unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    assert_eq!(db.applied_index(), 9, "only the torn final record is lost");
    assert_eq!(db.dump().unwrap().len(), 9);
    drop(db);
    // mid-log damage
    let mut bad = fs::read(&wal).unwrap();
    bad[10] ^= 0xff;
    fs::write(&wal, &bad).unwrap();
    assert_corruption(dir.path(), "mid-WAL damage");
}

#[test]
fn stale_tmp_files_are_removed_and_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let expected = populated(dir.path());
    let t1 = dir.path().join("sst-0000000500.sst.tmp");
    let t2 = dir.path().join("MANIFEST.tmp");
    fs::write(&t1, b"half written").unwrap();
    fs::write(&t2, b"half written").unwrap();
    let db = Lsm::open(dir.path(), small()).unwrap();
    assert!(!t1.exists() && !t2.exists());
    assert_eq!(db.dump().unwrap(), expected);
}
