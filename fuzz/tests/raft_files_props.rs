//! Corruption properties for a Raft node's persistent state: the WAL, the
//! checksummed HardState file and the snapshot file, opened through
//! `storage::WalLog::open` (which must be two-phase: read-only checks first,
//! modification only after everything has been validated).

use std::path::Path;

use proptest::prelude::*;
use raft_core::{LogEntry, LogStore, NodeId, Term};
use raftdb_fuzz::*;
use storage::WalLog;

#[derive(Debug, Clone, PartialEq)]
struct Observed {
    hard: (Term, Option<NodeId>),
    last_included: (u64, Term),
    snapshot: Option<Vec<u8>>,
    peers: Vec<NodeId>,
    entries: Vec<LogEntry>,
}

fn observe(l: &WalLog) -> Observed {
    Observed {
        hard: l.load_hard_state(),
        last_included: (l.last_included_index(), l.last_included_term()),
        snapshot: l.snapshot_data().map(|s| s.to_vec()),
        peers: l.snapshot_peers().to_vec(),
        entries: l.entries_from(l.last_included_index() + 1),
    }
}

/// A node with a snapshot AND a live log tail: term/vote persisted, 6 entries
/// appended, compacted at index 3, then `tail` more entries. Returns the
/// directory and what a clean reopen must observe.
fn build(tail: usize, with_snapshot: bool) -> (tempfile::TempDir, Observed) {
    let d = tempfile::tempdir().unwrap();
    {
        let mut l = WalLog::open(d.path().join("node.wal")).unwrap();
        l.save_hard_state(4, Some(2));
        for i in 0..6u8 {
            l.append(if i < 3 { 2 } else { 3 }, vec![i; 5 + i as usize]);
        }
        if with_snapshot {
            l.compact(3, 2, b"snapshot-bytes".to_vec(), vec![1, 2, 3]);
        }
        for i in 0..tail {
            l.append(4, vec![0xA0 + i as u8; 7]);
        }
    }
    let clean = observe(&WalLog::open(d.path().join("node.wal")).unwrap());
    (d, clean)
}

fn open(dir: &Path) -> Result<Observed, storage::WalError> {
    WalLog::open(dir.join("node.wal")).map(|l| observe(&l))
}

fn record_bounds(wal: &[u8]) -> Vec<usize> {
    let mut b = vec![0usize];
    let mut pos = 0usize;
    while pos + 8 <= wal.len() {
        pos += 8 + u32::from_le_bytes(wal[pos..pos + 4].try_into().unwrap()) as usize;
        b.push(pos);
    }
    b
}

#[derive(Clone, Copy, Debug)]
enum Damage {
    Flip(u8),
    Truncate,
}

fn inflict(dir: &Path, name: &str, orig: &[u8], at: usize, d: Damage) {
    let mut b = orig.to_vec();
    match d {
        Damage::Flip(m) => b[at] ^= m,
        Damage::Truncate => b.truncate(at),
    }
    std::fs::write(dir.join(name), &b).unwrap();
}

fn verdict(
    dir: &Path,
    clean: &Observed,
    name: &str,
    orig: &[u8],
    at: usize,
    d: Damage,
) -> Result<(), String> {
    let damaged = snapshot_dir(dir);
    let is_wal = name == "node.wal";
    match open(dir) {
        Err(e) => {
            if snapshot_dir(dir) != damaged {
                return Err(format!(
                    "{name}@{at} {d:?}: refused open ({e}) modified the directory"
                ));
            }
            if is_wal && matches!(d, Damage::Truncate) {
                return Err(format!(
                    "{name}@{at}: a torn WAL tail must be recoverable, got refusal: {e}"
                ));
            }
            Ok(())
        }
        Ok(got) => {
            if !is_wal {
                return Err(format!(
                    "{name}@{at} {d:?}: damage to a checksummed file was accepted"
                ));
            }
            let b = record_bounds(orig);
            let n = clean.entries.len();
            let same_but_entries = Observed {
                entries: vec![],
                ..got.clone()
            } == Observed {
                entries: vec![],
                ..clean.clone()
            };
            if !same_but_entries
                || got.entries[..] != clean.entries[..got.entries.len().min(n)]
                || got.entries.len() > n
            {
                return Err(format!("{name}@{at} {d:?}: accepted state differs from any acknowledged prefix: {got:?}"));
            }
            match d {
                Damage::Truncate => {
                    let whole = b.iter().rposition(|&x| x <= at).unwrap();
                    if got.entries.len() != whole {
                        return Err(format!(
                            "{name}@{at}: truncate kept {} frames, expected {whole}",
                            got.entries.len()
                        ));
                    }
                }
                Damage::Flip(_) => {
                    let last_start = b[b.len() - 2];
                    if !(got.entries.len() == n || (got.entries.len() + 1 == n && at >= last_start))
                    {
                        return Err(format!("{name}@{at} {d:?}: lost records ({}/{n}) with the flip outside the final frame", got.entries.len()));
                    }
                }
            }
            Ok(())
        }
    }
}

#[test]
fn clean_reopen_is_stable_and_complete() {
    for (tail, snap) in [(0, true), (3, true), (4, false)] {
        let (d, clean) = build(tail, snap);
        assert_eq!(open(d.path()).unwrap(), clean);
        assert_eq!(open(d.path()).unwrap(), clean, "second open differs");
        assert_eq!(clean.snapshot.is_some(), snap);
    }
}

/// Flip EVERY byte, truncate EVERYWHERE, of EVERY file.
#[test]
fn every_byte_flip_and_truncation_of_every_file() {
    for (tail, snap) in [(3, true), (4, false)] {
        let (d, clean) = build(tail, snap);
        let pristine = snapshot_dir(d.path());
        assert!(pristine.contains_key("node.wal") && pristine.contains_key("node.wal.hardstate"));
        if snap {
            assert!(pristine.contains_key("node.wal.snapshot"));
        }
        let work = tempfile::tempdir().unwrap();
        let mut bad = Vec::new();
        let mut checked = 0;
        for (name, orig) in &pristine {
            let cases = (0..orig.len())
                .map(|i| (i, Damage::Flip(0x40)))
                .chain((0..orig.len()).map(|l| (l, Damage::Truncate)));
            for (at, dmg) in cases {
                std::fs::remove_dir_all(work.path()).ok();
                copy_dir(d.path(), work.path());
                inflict(work.path(), name, orig, at, dmg);
                checked += 1;
                if let Err(e) = verdict(work.path(), &clean, name, orig, at, dmg) {
                    bad.push(e);
                }
            }
        }
        eprintln!(
            "snap={snap} tail={tail}: files={:?} checked={checked} violations={}",
            pristine.keys().collect::<Vec<_>>(),
            bad.len()
        );
        assert!(
            bad.is_empty(),
            "{} violations, first: {:#?}",
            bad.len(),
            &bad[..bad.len().min(5)]
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 150, failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn random_damage_to_random_node_state(
        tail in 0usize..5, snap in any::<bool>(),
        which in any::<prop::sample::Index>(), at in any::<prop::sample::Index>(),
        mask in 1u8..=255, truncate in any::<bool>(),
    ) {
        let (d, clean) = build(tail, snap);
        let before = snapshot_dir(d.path());
        let names: Vec<_> = before.keys().cloned().collect();
        let name = &names[which.index(names.len())];
        let orig = &before[name];
        prop_assume!(!orig.is_empty());
        let i = at.index(orig.len());
        let dmg = if truncate { Damage::Truncate } else { Damage::Flip(mask) };
        inflict(d.path(), name, orig, i, dmg);
        verdict(d.path(), &clean, name, orig, i, dmg).map_err(TestCaseError::fail)?;
    }

    #[test]
    fn arbitrary_bytes_in_hardstate_snapshot_or_wal_never_panic_and_refusal_is_inert(
        which in 0usize..3, junk in prop::collection::vec(any::<u8>(), 0..200),
    ) {
        let (d, _) = build(3, true);
        let name = ["node.wal", "node.wal.hardstate", "node.wal.snapshot"][which];
        std::fs::write(d.path().join(name), junk).unwrap();
        let snap = snapshot_dir(d.path());
        if open(d.path()).is_err() {
            prop_assert_eq!(snapshot_dir(d.path()), snap, "refused open modified the directory");
        }
    }
}
