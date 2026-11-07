//! Property tests for everything the LSM engine persists: the memtable WAL,
//! the MANIFEST and the SSTables, driven through the public `Lsm::open`.
//!
//! A directory is built through the real write path, then damaged, then
//! reopened. For every damage the outcome must be one of:
//!   * `Lsm::open` refuses (and the directory is byte-for-byte untouched),
//!   * a read returns an error (checksum caught the damage lazily), or
//!   * the data is identical to what was acknowledged, or - ONLY when the
//!     damage lies inside the final record of the live WAL - identical minus
//!     that one final record.
//!
//! Anything else (different data, a dropped acknowledged write, a panic) fails.

use std::collections::BTreeMap;
use std::path::Path;

use proptest::prelude::*;
use raftdb_fuzz::*;
use storage::lsm::{Lsm, LsmOptions, Op};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn opts() -> LsmOptions {
    LsmOptions {
        background: false,
        block_bytes: 64, // many tiny blocks: more structure to damage
        memtable_bytes: 1 << 30,
        l0_compaction_trigger: 100,
        ..Default::default()
    }
}

#[derive(Clone, Debug)]
pub enum RawOp {
    Put(u8, Vec<u8>),
    Del(u8),
}

fn key(k: u8) -> Vec<u8> {
    format!("key{:03}", k % 24).into_bytes()
}

fn to_ops(batch: &[RawOp]) -> Vec<Op> {
    batch
        .iter()
        .map(|o| match o {
            RawOp::Put(k, v) => Op::Put(key(*k), v.clone()),
            RawOp::Del(k) => Op::Delete(key(*k)),
        })
        .collect()
}

fn model_apply(m: &mut Model, batch: &[RawOp]) {
    for o in batch {
        match o {
            RawOp::Put(k, v) => {
                m.insert(key(*k), v.clone());
            }
            RawOp::Del(k) => {
                m.remove(&key(*k));
            }
        }
    }
}

fn arb_batch() -> impl Strategy<Value = Vec<RawOp>> {
    prop::collection::vec(
        prop_oneof![
            (any::<u8>(), prop::collection::vec(any::<u8>(), 0..40))
                .prop_map(|(k, v)| RawOp::Put(k, v)),
            any::<u8>().prop_map(RawOp::Del),
        ],
        1..5,
    )
}

/// The acknowledged states. `states[k]` is the database after the flush plus
/// the first `k` live-WAL records (`states[0]` = just the flushed data), with
/// the Raft `applied` index that goes with it. The last entry is the full,
/// fully-acknowledged state.
struct Fixture {
    states: Vec<(Model, u64)>,
}

impl Fixture {
    fn n(&self) -> usize {
        self.states.len() - 1
    }
}

/// `pre` batches are flushed to an SSTable (and optionally compacted to L1);
/// `post` batches (>= 1) stay in the live WAL, the last one being the record
/// a torn write is allowed to cost.
fn build(dir: &Path, pre: &[Vec<RawOp>], post: &[Vec<RawOp>], compact: bool) -> Fixture {
    // A low L0 trigger only while building, so `compact` really merges.
    let build_opts = LsmOptions {
        l0_compaction_trigger: if compact { 2 } else { 100 },
        ..opts()
    };
    let db = Lsm::open(dir, build_opts).unwrap();
    let mut m = Model::new();
    let mut n = 0u64;
    // Two flushes, so `compact` merges two L0 tables into L1 (a real merge).
    let half = pre.len() / 2;
    for (i, b) in pre.iter().enumerate() {
        if i == half && half > 0 {
            db.flush().unwrap();
        }
        n += 1;
        db.apply(&to_ops(b), n).unwrap();
        model_apply(&mut m, b);
    }
    db.flush().unwrap();
    if compact {
        db.compact_all().unwrap();
        let st = db.stats();
        assert!(
            st.compactions > 0,
            "fixture must have run a real compaction: {st:?}"
        );
    } else {
        assert_eq!(db.stats().l1_files, 0);
    }
    let mut states = vec![(m.clone(), n)];
    for b in post {
        n += 1;
        db.apply(&to_ops(b), n).unwrap();
        model_apply(&mut m, b);
        states.push((m.clone(), n));
    }
    drop(db); // a crash: nothing flushed on drop
    Fixture { states }
}

#[derive(Debug, PartialEq)]
enum Outcome {
    /// `Lsm::open` refused to start.
    Refused,
    /// Opened, but a read reported corruption.
    ReadError,
    /// Opened and exactly `fx.states[k]` is visible.
    State(usize),
    /// Opened and returned data that was never acknowledged. Always a bug.
    Wrong(Vec<(Vec<u8>, Vec<u8>)>),
}

fn reopen(dir: &Path, fx: &Fixture) -> Outcome {
    let Ok(db) = Lsm::open(dir, opts()) else {
        return Outcome::Refused;
    };
    let Ok(d) = db.dump() else {
        return Outcome::ReadError;
    };
    let got: Model = d.iter().cloned().collect();
    // point reads must agree with the scan (bloom / index / block paths)
    for k in fx
        .states
        .iter()
        .flat_map(|(m, _)| m.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        match db.get(k) {
            Err(_) => return Outcome::ReadError,
            Ok(g) => {
                if got.get(k) != g.as_ref() {
                    return Outcome::Wrong(d);
                }
            }
        }
    }
    match fx
        .states
        .iter()
        .position(|(m, a)| *m == got && *a == db.applied_index())
    {
        Some(k) => Outcome::State(k),
        None => Outcome::Wrong(d),
    }
}

#[derive(Clone, Copy, Debug)]
enum Damage {
    Flip(u8),
    Truncate,
}

/// Record boundaries of a `len|crc|payload` WAL: `bounds[k]` = end of record
/// k-1 (`bounds[0]` = 0); `bounds.len() == records + 1`.
fn record_bounds(wal: &[u8]) -> Vec<usize> {
    let mut bounds = vec![0usize];
    let mut pos = 0usize;
    while pos + 8 <= wal.len() {
        let l = u32::from_le_bytes(wal[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 8 + l;
        bounds.push(pos);
    }
    bounds
}

/// Apply `damage` at `at` to `file` inside `dir`.
fn inflict(dir: &Path, file: &str, orig: &[u8], at: usize, d: Damage) {
    let mut b = orig.to_vec();
    match d {
        Damage::Flip(m) => b[at] ^= m,
        Damage::Truncate => b.truncate(at),
    }
    std::fs::write(dir.join(file), &b).unwrap();
}

/// The verdict for one damaged directory. Returns Err(message) on a violation.
///
/// Rules (all must hold):
///  * Wrong data is never acceptable.
///  * A refused open must leave the directory untouched (checked here).
///  * Damage to MANIFEST or an SSTable must never be silently accepted.
///  * Flip in the live WAL: refuse, or the full state, or - only if the flip
///    lies inside the final record - the state without that record.
///  * Truncation of the live WAL is a torn tail: exactly the whole records
///    before the cut survive (a cut on a record boundary is indistinguishable
///    from those records never having been written, so it cannot be refused).
fn verdict(
    dir: &Path,
    fx: &Fixture,
    file: &str,
    orig: &[u8],
    at: usize,
    d: Damage,
) -> Result<(), String> {
    let damaged = snapshot_dir(dir);
    let out = reopen(dir, fx);
    let is_wal = file.starts_with("wal-");
    let n = fx.n();
    match out {
        Outcome::Wrong(w) => Err(format!("{file}@{at} {d:?}: SILENTLY WRONG DATA {w:?}")),
        Outcome::Refused => {
            if snapshot_dir(dir) != damaged {
                return Err(format!(
                    "{file}@{at} {d:?}: refused open modified the directory"
                ));
            }
            if is_wal && matches!(d, Damage::Truncate) {
                return Err(format!(
                    "{file}@{at}: a torn tail must be recoverable, not refused"
                ));
            }
            Ok(())
        }
        Outcome::ReadError => {
            if is_wal {
                return Err(format!(
                    "{file}@{at} {d:?}: WAL damage surfaced as a read error"
                ));
            }
            Ok(())
        }
        Outcome::State(k) => {
            if !is_wal {
                // Identical data from a damaged checksummed file is an
                // undetected corruption of a protected byte.
                return Err(format!(
                    "{file}@{at} {d:?}: damage to a checksummed file was accepted (state {k})"
                ));
            }
            let bounds = record_bounds(orig);
            match d {
                Damage::Truncate => {
                    let whole = bounds.iter().rposition(|&b| b <= at).unwrap();
                    if k != whole {
                        return Err(format!(
                            "{file}@{at}: truncate kept {k} records, expected exactly {whole}"
                        ));
                    }
                }
                Damage::Flip(_) => {
                    let last_start = bounds[bounds.len() - 2];
                    let ok = k == n || (k + 1 == n && at >= last_start);
                    if !ok {
                        return Err(format!("{file}@{at} {d:?}: kept {k}/{n} records but the flip is not inside the final record (starts {last_start})"));
                    }
                }
            }
            Ok(())
        }
    }
}

fn fixture(compact: bool) -> (tempfile::TempDir, Fixture) {
    let pre: Vec<Vec<RawOp>> = (0..14u8)
        .map(|i| vec![RawOp::Put(i, vec![i; 18]), RawOp::Put(i + 5, vec![0xAB; 9])])
        .chain([vec![RawOp::Del(3)]])
        .collect();
    let post: Vec<Vec<RawOp>> = vec![
        vec![RawOp::Put(1, b"first".to_vec())],
        vec![RawOp::Put(30, b"last-record".to_vec()), RawOp::Del(2)],
    ];
    let d = tempfile::tempdir().unwrap();
    let fx = build(d.path(), &pre, &post, compact);
    (d, fx)
}

// Exhaustive and deterministic: flip EVERY byte of EVERY file, truncate
// EVERY file at EVERY length.

fn exhaustive(compact: bool) {
    let (d, fx) = fixture(compact);
    assert_eq!(
        reopen(d.path(), &fx),
        Outcome::State(fx.n()),
        "fixture must round-trip"
    );
    let pristine = snapshot_dir(d.path());
    let work = tempfile::tempdir().unwrap();
    let mut tally = std::collections::BTreeMap::<&str, u32>::new();
    let mut bad = Vec::new();
    for (name, orig) in &pristine {
        let cases = (0..orig.len())
            .map(|i| (i, Damage::Flip(0x40)))
            .chain((0..orig.len()).map(|l| (l, Damage::Truncate)));
        for (at, dmg) in cases {
            std::fs::remove_dir_all(work.path()).ok();
            copy_dir(d.path(), work.path());
            inflict(work.path(), name, orig, at, dmg);
            match verdict(work.path(), &fx, name, orig, at, dmg) {
                Ok(()) => *tally.entry("ok").or_default() += 1,
                Err(e) => bad.push(e),
            }
        }
    }
    eprintln!(
        "compact={compact}: files={:?} checked={:?}",
        pristine.keys().collect::<Vec<_>>(),
        tally
    );
    assert!(
        bad.is_empty(),
        "{} violations, first: {:#?}",
        bad.len(),
        &bad[..bad.len().min(5)]
    );
}

#[test]
fn every_byte_flip_and_truncation_flushed_only() {
    exhaustive(false);
}

#[test]
fn every_byte_flip_and_truncation_after_compaction() {
    exhaustive(true);
}

#[test]
fn clean_fixture_round_trips_both_shapes() {
    for compact in [false, true] {
        let (d, fx) = fixture(compact);
        assert_eq!(reopen(d.path(), &fx), Outcome::State(fx.n()));
        // and a second, clean reopen is stable
        assert_eq!(reopen(d.path(), &fx), Outcome::State(fx.n()));
    }
}

// ---------------------------------------------------------------------
// Randomised (proptest)
// ---------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig { cases: 120, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn round_trip_of_arbitrary_histories(
        pre in prop::collection::vec(arb_batch(), 2..8),
        post in prop::collection::vec(arb_batch(), 1..4),
        compact in any::<bool>(),
    ) {
        let d = tempfile::tempdir().unwrap();
        let fx = build(d.path(), &pre, &post, compact);
        prop_assert_eq!(reopen(d.path(), &fx), Outcome::State(fx.n()));
    }

    #[test]
    fn random_damage_to_random_histories(
        pre in prop::collection::vec(arb_batch(), 2..8),
        post in prop::collection::vec(arb_batch(), 1..4),
        compact in any::<bool>(),
        which in any::<prop::sample::Index>(),
        at in any::<prop::sample::Index>(),
        mask in 1u8..=255,
        truncate in any::<bool>(),
    ) {
        let d = tempfile::tempdir().unwrap();
        let fx = build(d.path(), &pre, &post, compact);
        let before = snapshot_dir(d.path());
        let names: Vec<_> = before.keys().cloned().collect();
        let name = &names[which.index(names.len())];
        let orig = &before[name];
        prop_assume!(!orig.is_empty());
        let i = at.index(orig.len());
        let dmg = if truncate { Damage::Truncate } else { Damage::Flip(mask) };
        inflict(d.path(), name, orig, i, dmg);
        verdict(d.path(), &fx, name, orig, i, dmg).map_err(TestCaseError::fail)?;
    }

    #[test]
    fn arbitrary_bytes_in_any_file_never_panic(
        pre in prop::collection::vec(arb_batch(), 2..5),
        post in prop::collection::vec(arb_batch(), 1..3),
        which in any::<prop::sample::Index>(),
        junk in prop::collection::vec(any::<u8>(), 0..300),
    ) {
        let d = tempfile::tempdir().unwrap();
        let fx = build(d.path(), &pre, &post, true);
        let names: Vec<_> = snapshot_dir(d.path()).keys().cloned().collect();
        let name = &names[which.index(names.len())];
        std::fs::write(d.path().join(name), junk).unwrap();
        let snap = snapshot_dir(d.path());
        if reopen(d.path(), &fx) == Outcome::Refused {
            prop_assert_eq!(snapshot_dir(d.path()), snap);
        }
    }

    #[test]
    fn a_directory_of_pure_garbage_never_panics(
        manifest in prop::option::of(prop::collection::vec(any::<u8>(), 0..200)),
        wal in prop::option::of(prop::collection::vec(any::<u8>(), 0..200)),
        sst in prop::option::of(prop::collection::vec(any::<u8>(), 0..400)),
    ) {
        let d = tempfile::tempdir().unwrap();
        if let Some(m) = &manifest { std::fs::write(d.path().join("MANIFEST"), m).unwrap(); }
        if let Some(w) = &wal { std::fs::write(d.path().join("wal-0000000001.log"), w).unwrap(); }
        if let Some(s) = &sst { std::fs::write(d.path().join("sst-0000000002.sst"), s).unwrap(); }
        let snap = snapshot_dir(d.path());
        if Lsm::open(d.path(), opts()).is_err() {
            prop_assert_eq!(snapshot_dir(d.path()), snap, "refused open modified the directory");
        }
    }
}
