//! Shared helpers for the RaftDB property / fuzz tests.
//!
//! Everything here drives the *public* APIs of the crates under test, so the
//! tests double as a check that a refused open really does leave the
//! directory untouched, from the outside, byte for byte.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use raft_core::{EntryKind, LogEntry};

/// CRC-32 (IEEE), identical to the engine's `durable::crc32`. Needed to forge
/// frames whose checksum is *valid* but whose body is garbage - the only way
/// random input ever reaches the structural decoders behind the checksum.
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Every regular file under `dir` (recursively), keyed by relative path.
pub fn snapshot_dir(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, cur: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(rd) = fs::read_dir(cur) else { return };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                let rel = p.strip_prefix(base).unwrap().to_string_lossy().into_owned();
                out.insert(rel, fs::read(&p).unwrap());
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// Copy a flat directory (the engine never nests).
pub fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for e in fs::read_dir(from).unwrap().flatten() {
        if e.path().is_file() {
            fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

/// Size in bytes of one main-WAL frame for `e` (8 header + 29 fixed + data).
pub fn frame_len(e: &LogEntry) -> usize {
    8 + 29 + e.data.len()
}

/// Build a main-WAL log: contiguous indexes from `start`, non-decreasing
/// terms, arbitrary kinds. `spec` = (term_increment, kind selector, data).
pub fn make_entries(start: u64, first_term: u64, spec: &[(u8, u8, Vec<u8>)]) -> Vec<LogEntry> {
    let mut term = first_term;
    spec.iter()
        .enumerate()
        .map(|(i, (dt, k, data))| {
            term += (*dt % 3) as u64;
            let kind = match k % 4 {
                0 => EntryKind::Data,
                1 => EntryKind::AddNode(*k as u64 + 7),
                2 => EntryKind::RemoveNode(*k as u64 + 9),
                _ => EntryKind::NoOp,
            };
            LogEntry {
                term,
                index: start + i as u64,
                kind,
                data: data.clone(),
            }
        })
        .collect()
}

/// Wrap `payload` in a main-WAL / LSM-WAL style frame: len | crc | payload.
pub fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(payload).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------
// Fixtures shared by the fuzz-loop binary (the test files build their own).
// ---------------------------------------------------------------------

use storage::lsm::{Lsm, LsmOptions, Op};

pub fn lsm_opts() -> LsmOptions {
    LsmOptions {
        background: false,
        block_bytes: 64,
        memtable_bytes: 1 << 30,
        ..Default::default()
    }
}

/// A small, valid LSM directory: two flushed tables, one compaction, and a
/// live WAL with three records. Returns nothing; inspect the directory.
pub fn lsm_fixture(dir: &Path) {
    let db = Lsm::open(
        dir,
        LsmOptions {
            l0_compaction_trigger: 2,
            ..lsm_opts()
        },
    )
    .unwrap();
    let mut n = 0;
    for round in 0..2u8 {
        for i in 0..8u8 {
            n += 1;
            db.apply(
                &[Op::Put(
                    format!("key{:03}", i + round * 4).into_bytes(),
                    vec![i; 12],
                )],
                n,
            )
            .unwrap();
        }
        db.flush().unwrap();
    }
    db.compact_all().unwrap();
    for i in 0..3u8 {
        n += 1;
        let op = if i == 2 {
            Op::Delete(b"key001".to_vec())
        } else {
            Op::Put(vec![b'w', i], vec![0xEE; 9])
        };
        db.apply(&[op], n).unwrap();
    }
}

/// A valid Raft node directory (`node.wal`, `.hardstate`, `.snapshot`).
pub fn raft_fixture(dir: &Path) {
    use raft_core::LogStore;
    let mut l = storage::WalLog::open(dir.join("node.wal")).unwrap();
    l.save_hard_state(4, Some(2));
    for i in 0..6u8 {
        l.append(if i < 3 { 2 } else { 3 }, vec![i; 5 + i as usize]);
    }
    l.compact(3, 2, b"snapshot-bytes".to_vec(), vec![1, 2, 3]);
    for i in 0..3u8 {
        l.append(4, vec![0xA0 + i; 7]);
    }
}

/// A valid main-WAL file's bytes.
pub fn wal_fixture_bytes() -> Vec<u8> {
    let spec: Vec<(u8, u8, Vec<u8>)> = (0..6u8)
        .map(|i| (i % 2, i, vec![i; 3 + i as usize]))
        .collect();
    let entries = make_entries(1, 1, &spec);
    let dir = std::env::temp_dir().join(format!("raftdb-fuzz-walfx-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("wal.log");
    let _ = std::fs::remove_file(&p);
    storage::Wal::open(&p)
        .unwrap()
        .append_batch(&entries)
        .unwrap();
    let b = std::fs::read(&p).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }
}
