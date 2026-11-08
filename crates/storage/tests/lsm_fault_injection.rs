//! Deterministic crash/fault tests for every LSM persistence boundary.
//!
//! Fault plans are thread-local, so every test runs the engine with
//! `background: false`: flush and compaction then happen on the calling
//! thread, where the armed fault can hit them.
//!
//! The oracle: after a crash at any instant the recovered engine must hold
//! *exactly* the state of some prefix of the applied history - the state
//! at its recovered `applied_index` - never a mixture; with `sync_wal` it
//! must contain every acknowledged write; and it may contain at most the one
//! operation that was in flight.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::lsm::{Lsm, LsmError, LsmOptions, Op};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

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

fn k(i: u64) -> Vec<u8> {
    format!("key{i:03}").into_bytes()
}

#[derive(Clone, Debug)]
enum Step {
    Apply(u64, Vec<Op>),
    Flush,
    Compact,
    Sync,
    Replace(u64, Vec<(Vec<u8>, Vec<u8>)>),
}

fn apply_to_model(m: &mut Model, ops: &[Op]) {
    for op in ops {
        match op {
            Op::Put(k, v) => {
                m.insert(k.clone(), v.clone());
            }
            Op::Delete(k) => {
                m.remove(k);
            }
        }
    }
}

/// The fixed workload: 45 applies with flushes and compactions interleaved,
/// then a snapshot ingest that jumps the index to 100, then 20 more applies.
fn workload() -> Vec<Step> {
    let mut v = Vec::new();
    for i in 1..=45u64 {
        let ops = match i % 7 {
            0 => vec![Op::Delete(k(i % 11))],
            3 => vec![
                Op::Put(k(i % 13), format!("batch-a-{i}").into_bytes()),
                Op::Put(k(20 + i % 5), format!("batch-b-{i}").into_bytes()),
            ],
            _ => vec![Op::Put(
                k(i % 17),
                format!("value-{i}-{}", "x".repeat(20)).into_bytes(),
            )],
        };
        v.push(Step::Apply(i, ops));
        if i % 12 == 0 {
            v.push(Step::Flush);
        }
        if i % 15 == 0 {
            v.push(Step::Compact);
        }
        if i % 20 == 0 {
            v.push(Step::Sync);
        }
    }
    let snap: Vec<_> = (30..70)
        .map(|i| (k(i), format!("snap-{i}").into_bytes()))
        .collect();
    v.push(Step::Replace(100, snap));
    for i in 101..=120u64 {
        v.push(Step::Apply(
            i,
            vec![Op::Put(k(i % 40 + 30), format!("post-{i}").into_bytes())],
        ));
        if i % 9 == 0 {
            v.push(Step::Flush);
        }
    }
    v.push(Step::Compact);
    v
}

/// Oracle states keyed by applied index, plus the index each step produces.
fn oracle(steps: &[Step]) -> BTreeMap<u64, Model> {
    let mut states = BTreeMap::new();
    let mut cur = Model::new();
    states.insert(0, cur.clone());
    for s in steps {
        match s {
            Step::Apply(i, ops) => {
                apply_to_model(&mut cur, ops);
                states.insert(*i, cur.clone());
            }
            Step::Replace(i, entries) => {
                cur = entries.iter().cloned().collect();
                states.insert(*i, cur.clone());
            }
            _ => {}
        }
    }
    states
}

/// Run `steps` until one fails. Returns (last acknowledged index, in-flight
/// index if a state-changing step failed, the error).
fn run(db: &Lsm, steps: &[Step]) -> (u64, Option<u64>, Option<LsmError>) {
    let mut acked = 0;
    for s in steps {
        let (idx, r) = match s {
            Step::Apply(i, ops) => (Some(*i), db.apply(ops, *i)),
            Step::Replace(i, e) => (Some(*i), db.replace_all(e.clone(), *i)),
            Step::Flush => (None, db.flush()),
            Step::Compact => (None, db.compact_all()),
            Step::Sync => (None, db.sync()),
        };
        match r {
            Ok(()) => {
                if let Some(i) = idx {
                    acked = i;
                }
            }
            Err(e) => return (acked, idx, Some(e)),
        }
    }
    (acked, None, None)
}

fn check_recovered(
    dir: &Path,
    states: &BTreeMap<u64, Model>,
    acked: u64,
    inflight: Option<u64>,
    ctx: &str,
) {
    fault::clear();
    let db = Lsm::open(dir, opts()).unwrap_or_else(|e| panic!("{ctx}: recovery failed: {e}"));
    let applied = db.applied_index();
    let state = states.get(&applied).unwrap_or_else(|| {
        panic!("{ctx}: recovered applied index {applied} was never a real state")
    });
    let dump: Model = db.dump().unwrap().into_iter().collect();
    assert_eq!(
        &dump, state,
        "{ctx}: recovered contents are not the state at index {applied}"
    );
    for (key, v) in state {
        assert_eq!(
            db.get(key).unwrap().as_ref(),
            Some(v),
            "{ctx}: point read of {key:?}"
        );
    }
    assert!(
        applied >= acked,
        "{ctx}: LOST ACKNOWLEDGED WRITE (recovered {applied} < acked {acked})"
    );
    let ok_hi = inflight.unwrap_or(acked).max(acked);
    assert!(
        applied <= ok_hi,
        "{ctx}: recovered {applied} beyond in-flight {ok_hi}"
    );
    // Hygiene: no temp files; every table on disk is referenced (checked
    // indirectly: a second open must be identical and clean).
    drop(db);
    let leftovers: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "{ctx}: temp files survived recovery: {leftovers:?}"
    );
    let db2 = Lsm::open(dir, opts()).unwrap();
    assert_eq!(
        db2.applied_index(),
        applied,
        "{ctx}: recovery not idempotent"
    );
    let again: Model = db2.dump().unwrap().into_iter().collect();
    assert_eq!(&again, state, "{ctx}: second recovery differs");
    // The recovered engine must be fully usable: write on top and re-read.
    let next = applied + 1;
    db2.apply(
        &[Op::Put(b"zz-after-recovery".to_vec(), b"ok".to_vec())],
        next,
    )
    .unwrap();
    db2.flush().unwrap();
    db2.compact_all().unwrap();
    assert_eq!(
        db2.get(b"zz-after-recovery").unwrap(),
        Some(b"ok".to_vec()),
        "{ctx}"
    );
}

#[test]
fn clean_run_matches_oracle_and_exercises_every_boundary() {
    fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let steps = workload();
    let states = oracle(&steps);
    let db = Lsm::open(dir.path(), opts()).unwrap();
    let (acked, _, err) = run(&db, &steps);
    assert!(err.is_none(), "{err:?}");
    assert_eq!(acked, 120);
    // Every point except recovery-time truncation must be reachable by the
    // workload, otherwise the sweep below would silently skip a boundary.
    for p in FaultPoint::LSM_ALL {
        if matches!(
            p,
            FaultPoint::LsmWalRecoverTruncate | FaultPoint::LsmWalRecoverSync
        ) {
            continue;
        }
        assert!(fault::hits(p) > 0, "workload never reaches {p:?}");
    }
    drop(db);
    check_recovered(dir.path(), &states, 120, None, "clean");
    let s = Lsm::open(dir.path(), opts()).unwrap().stats();
    assert!(
        s.flushes > 0 || s.l1_files > 0,
        "workload should have produced tables"
    );
}

#[test]
fn crash_at_every_io_step_with_every_fault_action_recovers_a_valid_prefix() {
    let steps = workload();
    let states = oracle(&steps);

    // Count hits per point in a fault-free run.
    fault::clear();
    let d = tempfile::tempdir().unwrap();
    let db = Lsm::open(d.path(), opts()).unwrap();
    fault::clear(); // count only the run phase: the sweep arms after open
    assert!(run(&db, &steps).2.is_none());
    drop(db);
    let mut hits = Vec::new();
    for p in FaultPoint::LSM_ALL {
        if matches!(
            p,
            FaultPoint::LsmWalRecoverTruncate | FaultPoint::LsmWalRecoverSync
        ) {
            continue;
        }
        hits.push((p, fault::hits(p)));
    }

    let write_points = [
        FaultPoint::LsmWalAppendWrite,
        FaultPoint::LsmSstTmpWrite,
        FaultPoint::LsmManifestTmpWrite,
    ];
    let mut crashes = 0usize;
    let mut per_point: BTreeMap<String, usize> = BTreeMap::new();
    for (point, n) in hits {
        for skip in 0..n {
            let mut actions = vec![FaultAction::FailBefore, FaultAction::FailAfter];
            if write_points.contains(&point) {
                actions.extend([
                    FaultAction::PartialWrite(1),
                    FaultAction::PartialWrite(9),
                    FaultAction::PartialWrite(40),
                ]);
            }
            for action in actions {
                fault::clear();
                let dir = tempfile::tempdir().unwrap();
                let db = Lsm::open(dir.path(), opts()).unwrap();
                fault::arm(FaultPlan {
                    point,
                    skip,
                    action,
                });
                let (acked, inflight, err) = run(&db, &steps);
                let fired = fault::fired();
                let ctx = format!("{point:?} hit#{skip} {action:?}");
                assert_eq!(fired, Some(point), "{ctx}: planned fault never fired");
                let err = err.unwrap_or_else(|| panic!("{ctx}: fault fired but no error surfaced"));
                // Fail-stop: after an I/O error the handle must be poisoned
                // and refuse further work.
                if matches!(err, LsmError::Io(_)) {
                    assert!(
                        db.is_poisoned(),
                        "{ctx}: handle not poisoned after I/O error"
                    );
                    assert!(
                        matches!(
                            db.apply(&[Op::Put(b"x".to_vec(), b"y".to_vec())], 999),
                            Err(LsmError::Poisoned(_))
                        ),
                        "{ctx}: poisoned handle accepted a write"
                    );
                    assert!(
                        matches!(db.get(b"x"), Err(LsmError::Poisoned(_))),
                        "{ctx}: poisoned handle served a read"
                    );
                }
                drop(db); // the "crash": no flush, no fsync
                check_recovered(dir.path(), &states, acked, inflight, &ctx);
                crashes += 1;
                *per_point.entry(format!("{point:?}")).or_default() += 1;
            }
        }
    }
    fault::clear();
    eprintln!("LSM fault sweep: {crashes} crash/recover cycles");
    for (p, c) in &per_point {
        eprintln!("  {p:<24} {c}");
    }
    assert!(crashes > 150, "sweep unexpectedly small: {crashes}");
}

#[test]
fn crash_during_recovery_itself_is_safe_and_idempotent() {
    for point in [
        FaultPoint::LsmWalRecoverTruncate,
        FaultPoint::LsmWalRecoverSync,
    ] {
        for action in [FaultAction::FailBefore, FaultAction::FailAfter] {
            fault::clear();
            let dir = tempfile::tempdir().unwrap();
            {
                let db = Lsm::open(dir.path(), opts()).unwrap();
                for i in 1..=8u64 {
                    db.apply(&[Op::Put(k(i), b"v".to_vec())], i).unwrap();
                }
            }
            // Tear the WAL tail.
            let wal = fs::read_dir(dir.path())
                .unwrap()
                .flatten()
                .map(|e| e.path())
                .find(|p| p.file_name().unwrap().to_string_lossy().starts_with("wal-"))
                .unwrap();
            let bytes = fs::read(&wal).unwrap();
            fs::write(&wal, &bytes[..bytes.len() - 5]).unwrap();

            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            assert!(
                Lsm::open(dir.path(), opts()).is_err(),
                "{point:?}/{action:?}: open should fail"
            );
            assert_eq!(fault::fired(), Some(point));
            fault::clear();
            let db = Lsm::open(dir.path(), opts()).unwrap();
            assert_eq!(db.applied_index(), 7, "{point:?}/{action:?}");
            assert_eq!(db.dump().unwrap().len(), 7);
        }
    }
}

#[test]
fn crash_after_manifest_publish_but_before_old_files_are_deleted_leaves_only_garbage() {
    // Compaction publishes the MANIFEST first and deletes inputs after; a
    // crash in between leaves the inputs on disk, unreferenced. Simulate it
    // by resurrecting a deleted input from a copy.
    fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let db = Lsm::open(dir.path(), opts()).unwrap();
    for round in 0..3u64 {
        for i in 0..20u64 {
            db.apply(
                &[Op::Put(k(i), format!("r{round}").into_bytes())],
                round * 20 + i + 1,
            )
            .unwrap();
        }
        db.flush().unwrap();
    }
    let l0_before: Vec<_> = fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("sst-"))
        .map(|e| (e.path(), fs::read(e.path()).unwrap()))
        .collect();
    db.compact_all().unwrap();
    let want = db.dump().unwrap();
    drop(db);
    for (p, bytes) in &l0_before {
        if !p.exists() {
            fs::write(p, bytes).unwrap(); // the inputs a crash would have left behind
        }
    }
    let db = Lsm::open(dir.path(), opts()).unwrap();
    assert_eq!(db.dump().unwrap(), want, "stale inputs must not resurface");
    for (p, _) in &l0_before {
        // either never deleted-and-recreated, or removed as orphan now
        let id_live = db.stats().l0_files + db.stats().l1_files;
        let _ = id_live;
        let _ = p;
    }
    let live = db.stats().l0_files + db.stats().l1_files;
    let on_disk = fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("sst-"))
        .count();
    assert_eq!(
        live, on_disk,
        "orphaned inputs must be garbage-collected on open"
    );
}

#[test]
fn unsynced_wal_tail_lost_in_a_crash_is_a_valid_prefix_and_sync_is_a_real_barrier() {
    fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let mut o = opts();
    o.sync_wal = false;
    o.memtable_bytes = 1 << 20; // keep everything in one WAL
    let db = Lsm::open(dir.path(), o.clone()).unwrap();
    let mut model = Model::new();
    let mut states = BTreeMap::new();
    states.insert(0, model.clone());
    for i in 1..=10u64 {
        let op = Op::Put(k(i), format!("v{i}").into_bytes());
        apply_to_model(&mut model, std::slice::from_ref(&op));
        states.insert(i, model.clone());
        db.apply(&[op], i).unwrap();
    }
    db.sync().unwrap(); // durability barrier at index 10
    let wal = fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.file_name().unwrap().to_string_lossy().starts_with("wal-"))
        .unwrap();
    let synced_len = fs::metadata(&wal).unwrap().len();
    for i in 11..=15u64 {
        let op = Op::Put(k(i), format!("v{i}").into_bytes());
        apply_to_model(&mut model, std::slice::from_ref(&op));
        states.insert(i, model.clone());
        db.apply(&[op], i).unwrap();
    }
    drop(db);
    // Power loss: everything after the barrier may vanish, possibly leaving
    // a torn fragment of the next record.
    for extra in [0usize, 3, 11] {
        let full = fs::read(&wal).unwrap();
        let keep = (synced_len as usize + extra).min(full.len());
        let tmp = tempfile::tempdir().unwrap();
        for e in fs::read_dir(dir.path()).unwrap().flatten() {
            fs::copy(e.path(), tmp.path().join(e.file_name())).unwrap();
        }
        fs::write(tmp.path().join(wal.file_name().unwrap()), &full[..keep]).unwrap();
        let db = Lsm::open(tmp.path(), o.clone()).unwrap();
        assert_eq!(
            db.applied_index(),
            10,
            "extra={extra}: barrier must preserve index 10 exactly"
        );
        let dump: Model = db.dump().unwrap().into_iter().collect();
        assert_eq!(dump, states[&10], "extra={extra}");
    }
}

/// Randomized chain: many crashes in a row on the *same* directory, each
/// recovery checked against the oracle, each followed by more work.
#[test]
fn randomized_multi_crash_chain() {
    let seeds: u64 = std::env::var("RAFTDB_LSM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(25);
    let mut total_crashes = 0;
    for seed in 1..=seeds {
        let mut rng = seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let mut states: BTreeMap<u64, Model> = BTreeMap::new();
        states.insert(0, Model::new());
        let mut acked = 0u64;
        for round in 0..12 {
            let db = Lsm::open(dir.path(), opts()).unwrap();
            let base = db.applied_index();
            let mut cur = states[&base].clone();
            // Random plan for this iteration (or none).
            let points: Vec<FaultPoint> = FaultPoint::LSM_ALL
                .iter()
                .copied()
                .filter(|p| {
                    !matches!(
                        p,
                        FaultPoint::LsmWalRecoverTruncate | FaultPoint::LsmWalRecoverSync
                    )
                })
                .collect();
            let plan = (next() % 5 != 0).then(|| {
                let point = points[(next() % points.len() as u64) as usize];
                let action = match next() % 3 {
                    0 => FaultAction::FailBefore,
                    1 => FaultAction::FailAfter,
                    _ => FaultAction::PartialWrite((next() % 50) as usize),
                };
                FaultPlan {
                    point,
                    skip: (next() % 6) as usize,
                    action,
                }
            });
            if let Some(p) = plan {
                fault::arm(p);
            }
            let mut idx = base;
            let mut inflight = None;
            let mut failed = false;
            for _ in 0..40 {
                let r = match next() % 10 {
                    0 => db.flush(),
                    1 => db.compact_all(),
                    2 => db.sync(),
                    3 if next() % 4 == 0 => {
                        idx += 1;
                        let entries: Vec<_> = (0..(next() % 30))
                            .map(|i| (k(i * 2), format!("s{idx}-{i}").into_bytes()))
                            .collect();
                        let cand: Model = entries.iter().cloned().collect();
                        states.insert(idx, cand.clone());
                        inflight = Some(idx);
                        let r = db.replace_all(entries, idx);
                        if r.is_ok() {
                            cur = cand;
                            acked = idx;
                            inflight = None;
                        }
                        r
                    }
                    _ => {
                        idx += 1;
                        let op = if next() % 4 == 0 {
                            Op::Delete(k(next() % 25))
                        } else {
                            Op::Put(
                                k(next() % 25),
                                format!("v{seed}-{round}-{idx}").into_bytes(),
                            )
                        };
                        let mut cand = cur.clone();
                        apply_to_model(&mut cand, std::slice::from_ref(&op));
                        states.insert(idx, cand.clone());
                        inflight = Some(idx);
                        let r = db.apply(&[op], idx);
                        if r.is_ok() {
                            cur = cand;
                            acked = idx;
                            inflight = None;
                        }
                        r
                    }
                };
                if r.is_err() {
                    failed = true;
                    break;
                }
            }
            if failed {
                total_crashes += 1;
            }
            fault::clear();
            drop(db);
            check_recovered(
                dir.path(),
                &states,
                acked,
                inflight,
                &format!("seed {seed} round {round}"),
            );
            // check_recovered wrote one extra op at applied+1; account for it
            // so the oracle stays aligned with disk.
            let db = Lsm::open(dir.path(), opts()).unwrap();
            let a = db.applied_index();
            let mut m = states[&(a - 1)].clone();
            m.insert(b"zz-after-recovery".to_vec(), b"ok".to_vec());
            states.insert(a, m);
            acked = a;
        }
    }
    eprintln!("randomized chain: {total_crashes} injected crashes across {seeds} seeds");
    assert!(
        total_crashes > seeds,
        "plans rarely fired; chain test is too weak"
    );
}

/// Faults while the database is first being created (initial MANIFEST and
/// first WAL). A crash there must leave a directory that opens as empty.
#[test]
fn crash_during_initial_creation_leaves_an_openable_empty_database() {
    let creation_points = [
        FaultPoint::LsmManifestTmpWrite,
        FaultPoint::LsmManifestTmpSync,
        FaultPoint::LsmManifestRename,
        FaultPoint::LsmManifestDirSync,
        FaultPoint::LsmWalCreateDirSync,
    ];
    let mut n = 0;
    for point in creation_points {
        for action in [
            FaultAction::FailBefore,
            FaultAction::FailAfter,
            FaultAction::PartialWrite(3),
        ] {
            fault::clear();
            let dir = tempfile::tempdir().unwrap();
            fault::arm(FaultPlan {
                point,
                skip: 0,
                action,
            });
            let r = Lsm::open(dir.path(), opts());
            let fired = fault::fired();
            fault::clear();
            if fired.is_some() {
                assert!(
                    r.is_err(),
                    "{point:?}/{action:?}: open must fail when its I/O fails"
                );
                n += 1;
            }
            drop(r);
            let db = Lsm::open(dir.path(), opts()).unwrap_or_else(|e| {
                panic!("{point:?}/{action:?}: reopen after failed creation: {e}")
            });
            assert_eq!(db.applied_index(), 0);
            assert!(db.dump().unwrap().is_empty());
            db.apply(&[Op::Put(b"a".to_vec(), b"b".to_vec())], 1)
                .unwrap();
            assert_eq!(db.get(b"a").unwrap(), Some(b"b".to_vec()));
        }
    }
    assert!(n >= 10, "creation faults rarely fired: {n}");
}
