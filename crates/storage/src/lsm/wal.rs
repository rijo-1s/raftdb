//! The memtable write-ahead log.
//!
//! Record: `len:u32 | crc32(payload):u32 | payload`, payload =
//! `applied:u64 | nops:u32 | op*`, op = `tag:u8 | klen:u32 | key | (vlen:u32 | value)?`.
//! The Raft `applied` index travels in the same record as the operations it
//! describes, so data and its position can never be separated by a crash.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use super::error::{corrupt, Result};
use crate::durable::{self, crc32};
use crate::fault::FaultPoint;

pub(crate) const TAG_PUT: u8 = 1;
pub(crate) const TAG_DEL: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl Op {
    pub fn key(&self) -> &[u8] {
        match self {
            Op::Put(k, _) | Op::Delete(k) => k,
        }
    }
    pub(crate) fn approx_bytes(&self) -> usize {
        match self {
            Op::Put(k, v) => k.len() + v.len() + 32,
            Op::Delete(k) => k.len() + 32,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Record {
    pub applied: u64,
    pub ops: Vec<Op>,
}

pub(crate) fn encode_record(applied: u64, ops: &[Op]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&applied.to_le_bytes());
    p.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        match op {
            Op::Put(k, v) => {
                p.push(TAG_PUT);
                p.extend_from_slice(&(k.len() as u32).to_le_bytes());
                p.extend_from_slice(k);
                p.extend_from_slice(&(v.len() as u32).to_le_bytes());
                p.extend_from_slice(v);
            }
            Op::Delete(k) => {
                p.push(TAG_DEL);
                p.extend_from_slice(&(k.len() as u32).to_le_bytes());
                p.extend_from_slice(k);
            }
        }
    }
    let mut out = Vec::with_capacity(8 + p.len());
    out.extend_from_slice(&(p.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(&p).to_le_bytes());
    out.extend_from_slice(&p);
    out
}

fn decode_payload(p: &[u8]) -> Option<Record> {
    let applied = u64::from_le_bytes(p.get(0..8)?.try_into().ok()?);
    let n = u32::from_le_bytes(p.get(8..12)?.try_into().ok()?) as usize;
    let mut pos = 12;
    let mut ops = Vec::new();
    let rd32 = |pos: &mut usize| -> Option<usize> {
        let v = u32::from_le_bytes(p.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
        *pos += 4;
        Some(v)
    };
    for _ in 0..n {
        let tag = *p.get(pos)?;
        pos += 1;
        let kl = rd32(&mut pos)?;
        let k = p.get(pos..pos + kl)?.to_vec();
        pos += kl;
        match tag {
            TAG_PUT => {
                let vl = rd32(&mut pos)?;
                let v = p.get(pos..pos + vl)?.to_vec();
                pos += vl;
                ops.push(Op::Put(k, v));
            }
            TAG_DEL => ops.push(Op::Delete(k)),
            _ => return None,
        }
    }
    (pos == p.len()).then_some(Record { applied, ops })
}

pub(crate) struct WalWriter {
    file: File,
}

pub(crate) fn wal_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("wal-{id:010}.log"))
}

impl WalWriter {
    /// Create a brand-new, empty WAL and make its directory entry durable.
    pub fn create(dir: &Path, id: u64) -> Result<WalWriter> {
        let path = wal_path(dir, id);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        durable::sync_parent_dir(FaultPoint::LsmWalCreateDirSync, &path)?;
        Ok(WalWriter { file })
    }

    /// Reopen an existing WAL for appending (after recovery).
    pub fn reopen(dir: &Path, id: u64) -> Result<WalWriter> {
        let path = wal_path(dir, id);
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(WalWriter { file })
    }

    pub fn append(&mut self, encoded: &[u8], sync: bool) -> Result<()> {
        durable::write_all(FaultPoint::LsmWalAppendWrite, &mut self.file, encoded)?;
        if sync {
            self.sync(FaultPoint::LsmWalAppendSync)?;
        }
        Ok(())
    }

    pub fn sync(&mut self, point: FaultPoint) -> Result<()> {
        self.file.flush()?;
        durable::sync_file(point, &self.file)?;
        Ok(())
    }
}

pub(crate) struct Replayed {
    pub records: Vec<Record>,
    /// Bytes of the file that are valid records. If shorter than the file,
    /// the remainder is a torn tail.
    pub valid_len: u64,
    pub file_len: u64,
}

/// True if a complete, checksum-valid, well-formed record starts at any
/// offset >= `from`. Only used on the (rare) damaged-record path.
fn valid_record_follows(data: &[u8], from: usize) -> bool {
    (from..data.len().saturating_sub(7)).any(|o| {
        let len = u32::from_le_bytes(data[o..o + 4].try_into().unwrap()) as usize;
        let Some(end) = o.checked_add(8).and_then(|x| x.checked_add(len)) else {
            return false;
        };
        if len == 0 || end > data.len() {
            return false;
        }
        let crc = u32::from_le_bytes(data[o + 4..o + 8].try_into().unwrap());
        let payload = &data[o + 8..end];
        crc32(payload) == crc && decode_payload(payload).is_some()
    })
}

/// Parse a WAL. A damaged record that is the *last thing in the file* (or
/// followed only by zero padding) is a torn tail; a damaged record with
/// valid-looking data after it is corruption and is reported as such.
pub(crate) fn replay(path: &Path) -> Result<Replayed> {
    let data = std::fs::read(path)?;
    let mut pos = 0usize;
    let mut records = Vec::new();
    while pos < data.len() {
        let rest = &data[pos..];
        let torn = |records, pos: usize| {
            Ok(Replayed {
                records,
                valid_len: pos as u64,
                file_len: data.len() as u64,
            })
        };
        if rest.len() < 8 {
            return torn(records, pos); // header itself incomplete
        }
        let len = u32::from_le_bytes(rest[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(rest[4..8].try_into().unwrap());
        if rest.iter().all(|&b| b == 0) {
            return torn(records, pos); // zero padding after a size-extending crash
        }
        if 8 + len > rest.len() {
            // Either a genuinely incomplete final record, or a corrupted
            // length field in the middle of the log. They look the same, so
            // look for any intact record after this point: if one exists,
            // dropping everything from here would destroy acknowledged writes.
            if valid_record_follows(&data, pos + 1) {
                return corrupt(format!(
                    "{}: record at byte {pos} claims {len} bytes but a valid record follows it",
                    path.display()
                ));
            }
            return torn(records, pos); // incomplete final record
        }
        let payload = &rest[8..8 + len];
        let good = crc32(payload) == crc;
        let rec = if good { decode_payload(payload) } else { None };
        match rec {
            Some(r) => {
                records.push(r);
                pos += 8 + len;
            }
            None => {
                let end = pos + 8 + len;
                if data[end..].iter().all(|&b| b == 0) && !valid_record_follows(&data, pos + 1) {
                    return torn(records, pos); // damaged final record
                }
                return corrupt(format!(
                    "{}: damaged record at byte {pos} is followed by more data",
                    path.display()
                ));
            }
        }
    }
    Ok(Replayed {
        records,
        valid_len: pos as u64,
        file_len: data.len() as u64,
    })
}

/// Truncate a torn tail (fsynced), with fault-injection hooks.
pub(crate) fn truncate_tail(path: &Path, valid_len: u64) -> Result<()> {
    use crate::fault::{self, FaultAction};
    let f = OpenOptions::new().write(true).open(path)?;
    match fault::poll(FaultPoint::LsmWalRecoverTruncate) {
        None => f.set_len(valid_len)?,
        Some(FaultAction::FailAfter) => {
            f.set_len(valid_len)?;
            return Err(super::error::LsmError::Io(std::io::Error::other(
                "injected fault after truncate",
            )));
        }
        Some(_) => {
            return Err(super::error::LsmError::Io(std::io::Error::other(
                "injected fault before truncate",
            )))
        }
    }
    durable::sync_file(FaultPoint::LsmWalRecoverSync, &f)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_roundtrip() {
        let ops = vec![
            Op::Put(b"k".to_vec(), b"v".to_vec()),
            Op::Delete(b"d".to_vec()),
        ];
        let e = encode_record(9, &ops);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("w");
        std::fs::write(&p, &e).unwrap();
        let r = replay(&p).unwrap();
        assert_eq!(r.records, vec![Record { applied: 9, ops }]);
        assert_eq!(r.valid_len, e.len() as u64);
    }

    #[test]
    fn torn_tail_vs_midlog_corruption() {
        let a = encode_record(1, &[Op::Put(b"a".to_vec(), b"1".to_vec())]);
        let b = encode_record(2, &[Op::Put(b"b".to_vec(), b"2".to_vec())]);
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("w");
        // Every proper prefix of the second record is a torn tail.
        for cut in 0..b.len() {
            let mut d = a.clone();
            d.extend_from_slice(&b[..cut]);
            std::fs::write(&p, &d).unwrap();
            let r = replay(&p).unwrap();
            assert_eq!(r.records.len(), 1, "cut {cut}");
            assert_eq!(r.valid_len, a.len() as u64);
        }
        // A flipped byte inside the *first* record, with a valid second
        // record after it, is corruption, not a torn tail.
        let mut d = a.clone();
        d.extend_from_slice(&b);
        d[10] ^= 0xff;
        std::fs::write(&p, &d).unwrap();
        assert!(replay(&p).is_err());
        // The same flip in the last record is indistinguishable from a torn
        // write and is dropped.
        let mut d = a.clone();
        d.extend_from_slice(&b);
        let last = d.len() - 1;
        d[last] ^= 0xff;
        std::fs::write(&p, &d).unwrap();
        assert_eq!(replay(&p).unwrap().records.len(), 1);
    }

    #[test]
    fn corrupt_length_field_midlog_is_an_error_not_a_torn_tail() {
        let recs: Vec<Vec<u8>> = (1..=3u64)
            .map(|i| encode_record(i, &[Op::Put(format!("k{i}").into_bytes(), b"v".to_vec())]))
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("w");
        let mut d: Vec<u8> = recs.concat();
        let off = recs[0].len();
        d[off..off + 4].copy_from_slice(&0x00ff_ffffu32.to_le_bytes());
        std::fs::write(&p, &d).unwrap();
        assert!(replay(&p).is_err(), "huge length mid-log must not truncate");
    }

    #[test]
    fn every_single_byte_flip_is_detected_or_only_drops_the_last_record() {
        let recs: Vec<Vec<u8>> = (1..=3u64)
            .map(|i| encode_record(i, &[Op::Put(format!("k{i}").into_bytes(), b"v".to_vec())]))
            .collect();
        let clean: Vec<u8> = recs.concat();
        let last_start = recs[0].len() + recs[1].len();
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("w");
        for i in 0..clean.len() {
            let mut d = clean.clone();
            d[i] ^= 0x40;
            std::fs::write(&p, &d).unwrap();
            match replay(&p) {
                Err(_) => {}
                Ok(r) => {
                    assert!(
                        i >= last_start,
                        "flip at {i} in an earlier record went undetected"
                    );
                    assert_eq!(r.records.len(), 2, "flip at {i}");
                }
            }
        }
    }

    // -----------------------------------------------------------------
    // Property tests. `storage` has no proptest
    // dev-dependency, so these use a tiny seeded xorshift generator; every
    // failure message carries the seed so a case is replayable.
    // -----------------------------------------------------------------

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: usize) -> usize {
            (self.next() % n.max(1) as u64) as usize
        }
        fn bytes(&mut self, max: usize) -> Vec<u8> {
            let n = self.below(max + 1);
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    fn random_ops(r: &mut Rng) -> Vec<Op> {
        (0..1 + r.below(4))
            .map(|_| {
                if r.below(3) == 0 {
                    Op::Delete(r.bytes(12))
                } else {
                    Op::Put(r.bytes(12), r.bytes(40))
                }
            })
            .collect()
    }

    /// 1..6 random records; returns (their encodings, the decoded form).
    fn random_log(seed: u64) -> (Vec<Vec<u8>>, Vec<Record>) {
        let mut r = Rng(seed | 1);
        let n = 1 + r.below(5);
        let mut encs = Vec::new();
        let mut recs = Vec::new();
        for i in 0..n {
            let ops = random_ops(&mut r);
            let applied = i as u64 + 1 + r.below(3) as u64;
            encs.push(encode_record(applied, &ops));
            recs.push(Record { applied, ops });
        }
        (encs, recs)
    }

    fn write_and_replay(bytes: &[u8]) -> (Result<Replayed>, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("w");
        std::fs::write(&p, bytes).unwrap();
        let r = replay(&p);
        let after = std::fs::read(&p).unwrap();
        (r, after)
    }

    #[test]
    fn prop_valid_logs_round_trip() {
        for seed in 1..=200u64 {
            let (encs, recs) = random_log(seed);
            let bytes: Vec<u8> = encs.concat();
            let (r, after) = write_and_replay(&bytes);
            let r = r.unwrap_or_else(|e| panic!("seed {seed}: {e}"));
            assert_eq!(r.records, recs, "seed {seed}");
            assert_eq!(r.valid_len, bytes.len() as u64, "seed {seed}");
            assert_eq!(after, bytes, "replay must be read-only (seed {seed})");
        }
    }

    #[test]
    fn prop_every_single_bit_flip_is_detected_or_loses_only_the_final_record() {
        for seed in 1..=12u64 {
            let (encs, recs) = random_log(seed);
            let clean: Vec<u8> = encs.concat();
            let last_start = clean.len() - encs.last().unwrap().len();
            for i in 0..clean.len() {
                for bit in 0..8 {
                    let mut d = clean.clone();
                    d[i] ^= 1 << bit;
                    let (r, after) = write_and_replay(&d);
                    match r {
                        Err(_) => assert_eq!(after, d, "refused replay touched the file"),
                        Ok(got) => {
                            assert!(
                                i >= last_start,
                                "seed {seed}: flip {i}.{bit} in an earlier record accepted"
                            );
                            assert_eq!(
                                got.records,
                                recs[..recs.len() - 1].to_vec(),
                                "seed {seed} flip {i}.{bit}"
                            );
                            assert_eq!(got.valid_len as usize, last_start);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn prop_truncation_anywhere_keeps_exactly_the_whole_records() {
        for seed in 1..=25u64 {
            let (encs, recs) = random_log(seed);
            let clean: Vec<u8> = encs.concat();
            for cut in 0..=clean.len() {
                let (r, _) = write_and_replay(&clean[..cut]);
                let got = r.unwrap_or_else(|e| {
                    panic!("seed {seed} cut {cut}: a truncation is a torn tail, got {e}")
                });
                let (mut acc, mut keep) = (0, 0);
                for e in &encs {
                    if acc + e.len() <= cut {
                        acc += e.len();
                        keep += 1;
                    } else {
                        break;
                    }
                }
                assert_eq!(got.records, recs[..keep].to_vec(), "seed {seed} cut {cut}");
                assert_eq!(got.valid_len as usize, acc);
            }
        }
    }

    #[test]
    fn prop_arbitrary_bytes_never_panic_and_never_modify_the_file() {
        let mut r = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..800 {
            let junk = r.bytes(300);
            let (res, after) = write_and_replay(&junk);
            assert_eq!(after, junk, "replay modified the file");
            let _ = res; // Ok or Err are both fine; a panic is not
        }
    }

    #[test]
    fn prop_checksum_valid_garbage_payloads_never_panic() {
        // Reach `decode_payload` behind the CRC: forge frames with a correct
        // checksum around random bodies, including hostile length claims.
        let mut r = Rng(0xD1B5_4A32_D192_ED03);
        for _ in 0..1000 {
            let mut payload = r.bytes(60);
            if r.below(3) == 0 && payload.len() >= 12 {
                payload[8..12].copy_from_slice(&u32::MAX.to_le_bytes()); // nops = 4 billion
            }
            if r.below(3) == 0 && payload.len() >= 17 {
                payload[13..17].copy_from_slice(&u32::MAX.to_le_bytes()); // klen = 4 GiB
            }
            let mut f = Vec::new();
            f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            f.extend_from_slice(&crc32(&payload).to_le_bytes());
            f.extend_from_slice(&payload);
            if r.below(2) == 0 {
                f.extend(r.bytes(10));
            }
            let (res, after) = write_and_replay(&f);
            assert_eq!(after, f);
            if let Ok(rep) = res {
                assert!(rep.valid_len <= rep.file_len);
            }
        }
    }

    #[test]
    fn prop_two_valid_records_with_garbage_between_are_corruption() {
        // An intact record AFTER a damaged one must never be dropped as a
        // "torn tail".
        for seed in 1..=100u64 {
            let (encs, _) = random_log(seed);
            let mut r = Rng(seed ^ 0xABCD);
            let mut d = encs[0].clone();
            let garbage = {
                let mut g = r.bytes(30);
                g.push(1); // never all-zero padding
                g
            };
            d.extend(&garbage);
            d.extend(encode_record(
                99,
                &[Op::Put(b"survivor".to_vec(), b"x".to_vec())],
            ));
            let (res, after) = write_and_replay(&d);
            assert!(
                res.is_err(),
                "seed {seed}: garbage followed by an intact record must be refused"
            );
            assert_eq!(after, d);
        }
    }

    #[test]
    fn prop_length_inflated_to_exactly_eof_is_corruption_not_a_torn_tail() {
        // See the main-WAL twin in fuzz/tests/wal_props.rs: an inflated length
        // that ends exactly at EOF must not let the damaged record pass as a
        // torn tail while intact records follow it.
        for seed in 1..=60u64 {
            let (encs, _) = random_log(seed);
            if encs.len() < 2 {
                continue;
            }
            let clean: Vec<u8> = encs.concat();
            let mut start = 0;
            for (k, e) in encs.iter().enumerate().take(encs.len() - 1) {
                let mut d = clean.clone();
                let inflated = (d.len() - start - 8) as u32;
                d[start..start + 4].copy_from_slice(&inflated.to_le_bytes());
                let (r, after) = write_and_replay(&d);
                assert!(
                    r.is_err(),
                    "seed {seed}: record {k} swallowed the records after it and was accepted"
                );
                assert_eq!(after, d, "refusal modified the file");
                start += e.len();
            }
        }
    }
}
