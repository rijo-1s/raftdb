//! The MANIFEST: the single commit point of the engine.
//!
//! A flush, compaction or snapshot ingest becomes real only when a new
//! MANIFEST replaces the old one atomically (`durable::atomic_write`). It is
//! rewritten whole each time, so there is no manifest log to replay and no
//! partially-applied edit to reason about: a crash leaves the previous
//! complete manifest or the new complete one.

use super::error::{corrupt, Result};
use crate::durable::crc32;

const MAGIC: &[u8; 8] = b"LSMMANI2";
const LEGACY_MAGIC: &[u8; 8] = b"LSMMANI1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FileMeta {
    pub id: u64,
    pub level: u8,
    pub size: u64,
    pub entries: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Manifest {
    /// Monotonic publish counter (diagnostics, and a cheap sanity check).
    pub version: u64,
    /// Next unused file id (SSTs and WALs share one id space).
    pub next_file_id: u64,
    /// WAL files with id >= this hold data not yet in any SSTable; older
    /// WALs are garbage.
    pub min_wal_id: u64,
    /// Exact set of WALs required for recovery. Empty only when decoding a legacy v1 manifest.
    pub live_wals: Vec<u64>,
    /// Highest Raft index whose effects are fully contained in `files`.
    pub applied: u64,
    pub files: Vec<FileMeta>,
}

impl Manifest {
    /// Structural invariants every MANIFEST must satisfy, checked before one
    /// is trusted (`open`) and before one becomes durable (every publish).
    ///
    /// These are exactly the properties the read path silently relies on:
    /// a file at a level other than 0/1 is dropped by `Version::build`
    /// (invisible, i.e. silent data loss); L1 lookups binary-search on
    /// sorted, pairwise-disjoint key ranges; and file ids come from one
    /// monotonic allocator shared by SSTables and WALs, so any live id at or
    /// above `next_file_id` could be handed out again and overwrite a live
    /// file. An empty `live_wals` means a legacy v1 manifest and skips the
    /// WAL-set checks only.
    pub fn validate(&self) -> Result<()> {
        if self.min_wal_id == 0 || self.min_wal_id >= self.next_file_id {
            return corrupt(format!(
                "MANIFEST: min_wal_id {} must be in 1..next_file_id ({})",
                self.min_wal_id, self.next_file_id
            ));
        }
        if !self.live_wals.is_empty() {
            if self.live_wals.windows(2).any(|w| w[0] >= w[1]) {
                return corrupt(format!(
                    "MANIFEST: live WAL set {:?} is not strictly increasing",
                    self.live_wals
                ));
            }
            if self.live_wals[0] != self.min_wal_id {
                return corrupt(format!(
                    "MANIFEST: live WAL set {:?} does not start at min_wal_id {}",
                    self.live_wals, self.min_wal_id
                ));
            }
            let last = *self.live_wals.last().unwrap();
            if last >= self.next_file_id {
                return corrupt(format!(
                    "MANIFEST: live WAL {last} is not below next_file_id {}",
                    self.next_file_id
                ));
            }
        }
        let mut seen = std::collections::HashSet::new();
        for f in &self.files {
            if f.id == 0 || f.id >= self.next_file_id {
                return corrupt(format!(
                    "MANIFEST: table id {} is not in 1..next_file_id ({})",
                    f.id, self.next_file_id
                ));
            }
            if !seen.insert(f.id) {
                return corrupt(format!("MANIFEST: duplicate table id {}", f.id));
            }
            if self.live_wals.contains(&f.id) {
                return corrupt(format!(
                    "MANIFEST: id {} is both a table and a live WAL",
                    f.id
                ));
            }
            if f.level > 1 {
                return corrupt(format!(
                    "MANIFEST: table {} is at level {}; only levels 0 and 1 exist",
                    f.id, f.level
                ));
            }
            if f.min_key > f.max_key {
                return corrupt(format!("MANIFEST: table {} has min_key > max_key", f.id));
            }
        }
        let mut l1: Vec<&FileMeta> = self.files.iter().filter(|f| f.level == 1).collect();
        l1.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        for w in l1.windows(2) {
            if w[1].min_key <= w[0].max_key {
                return corrupt(format!(
                    "MANIFEST: level-1 tables {} and {} overlap",
                    w[0].id, w[1].id
                ));
            }
        }
        Ok(())
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(MAGIC);
        for v in [
            self.version,
            self.next_file_id,
            self.min_wal_id,
            self.applied,
        ] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b.extend_from_slice(&(self.live_wals.len() as u32).to_le_bytes());
        for id in &self.live_wals {
            b.extend_from_slice(&id.to_le_bytes());
        }
        b.extend_from_slice(&(self.files.len() as u32).to_le_bytes());
        for f in &self.files {
            b.extend_from_slice(&f.id.to_le_bytes());
            b.push(f.level);
            b.extend_from_slice(&f.size.to_le_bytes());
            b.extend_from_slice(&f.entries.to_le_bytes());
            for k in [&f.min_key, &f.max_key] {
                b.extend_from_slice(&(k.len() as u32).to_le_bytes());
                b.extend_from_slice(k);
            }
        }
        let c = crc32(&b);
        b.extend_from_slice(&c.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Result<Manifest> {
        if b.len() < 8 + 32 + 4 + 4 || (&b[..8] != MAGIC && &b[..8] != LEGACY_MAGIC) {
            return corrupt("MANIFEST: bad magic or too short");
        }
        let (body, c) = b.split_at(b.len() - 4);
        if crc32(body) != u32::from_le_bytes(c.try_into().unwrap()) {
            return corrupt("MANIFEST: checksum mismatch");
        }
        let legacy = &body[..8] == LEGACY_MAGIC;
        let mut p = 8;
        let u64_at = |p: &mut usize| -> Option<u64> {
            let v = u64::from_le_bytes(body.get(*p..*p + 8)?.try_into().ok()?);
            *p += 8;
            Some(v)
        };
        let u32_at = |p: &mut usize| -> Option<u32> {
            let v = u32::from_le_bytes(body.get(*p..*p + 4)?.try_into().ok()?);
            *p += 4;
            Some(v)
        };
        let parsed = (|| {
            let version = u64_at(&mut p)?;
            let next_file_id = u64_at(&mut p)?;
            let min_wal_id = u64_at(&mut p)?;
            let applied = u64_at(&mut p)?;
            let mut live_wals = Vec::new();
            if !legacy {
                let nw = u32_at(&mut p)? as usize;
                // A v2 MANIFEST always lists at least the active WAL. An empty
                // list is reserved to mean "legacy v1" for the caller, so it
                // must never be reachable from a v2 file.
                if nw == 0 {
                    return None;
                }
                for _ in 0..nw {
                    live_wals.push(u64_at(&mut p)?);
                }
            }
            let n = u32_at(&mut p)? as usize;
            let mut files = Vec::new();
            for _ in 0..n {
                let id = u64_at(&mut p)?;
                let level = *body.get(p)?;
                p += 1;
                let size = u64_at(&mut p)?;
                let entries = u64_at(&mut p)?;
                let mut keys = Vec::new();
                for _ in 0..2 {
                    let l = u32_at(&mut p)? as usize;
                    keys.push(body.get(p..p + l)?.to_vec());
                    p += l;
                }
                let max_key = keys.pop()?;
                let min_key = keys.pop()?;
                files.push(FileMeta {
                    id,
                    level,
                    size,
                    entries,
                    min_key,
                    max_key,
                });
            }
            (p == body.len()).then_some(Manifest {
                version,
                next_file_id,
                min_wal_id,
                live_wals,
                applied,
                files,
            })
        })();
        match parsed {
            Some(m) => Ok(m),
            None => corrupt("MANIFEST: malformed body"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_detects_any_flipped_byte() {
        let m = Manifest {
            version: 7,
            next_file_id: 42,
            min_wal_id: 40,
            live_wals: vec![40, 41],
            applied: 1234,
            files: vec![FileMeta {
                id: 3,
                level: 1,
                size: 999,
                entries: 10,
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
            }],
        };
        let e = m.encode();
        assert_eq!(Manifest::decode(&e).unwrap(), m);
        for i in 0..e.len() {
            let mut bad = e.clone();
            bad[i] ^= 0x40;
            assert!(
                Manifest::decode(&bad).is_err(),
                "flip at byte {i} undetected"
            );
        }
        assert!(Manifest::decode(&e[..e.len() - 1]).is_err());
    }

    #[test]
    fn v2_with_empty_wal_set_is_rejected_but_legacy_v1_decodes() {
        let m = Manifest {
            version: 1,
            next_file_id: 3,
            min_wal_id: 2,
            live_wals: vec![],
            applied: 0,
            files: vec![],
        };
        // A v2 encoding of an empty set must not decode (it would be
        // indistinguishable from a legacy manifest).
        assert!(Manifest::decode(&m.encode()).is_err());

        // The same fields in the v1 layout (no WAL list) decode, with an
        // empty `live_wals`.
        let mut v1 = b"LSMMANI1".to_vec();
        for v in [1u64, 3, 2, 0] {
            v1.extend_from_slice(&v.to_le_bytes());
        }
        v1.extend_from_slice(&0u32.to_le_bytes()); // zero files
        let c = crc32(&v1);
        v1.extend_from_slice(&c.to_le_bytes());
        assert_eq!(Manifest::decode(&v1).unwrap(), m);
    }

    fn file(id: u64, level: u8, lo: &str, hi: &str) -> FileMeta {
        FileMeta {
            id,
            level,
            size: 10,
            entries: 2,
            min_key: lo.into(),
            max_key: hi.into(),
        }
    }

    fn good() -> Manifest {
        Manifest {
            version: 5,
            next_file_id: 20,
            min_wal_id: 10,
            live_wals: vec![10, 12],
            applied: 7,
            files: vec![
                file(3, 0, "a", "m"),
                file(4, 0, "c", "z"),
                file(5, 1, "a", "f"),
                file(6, 1, "g", "k"),
                file(7, 1, "l", "l"),
            ],
        }
    }

    #[test]
    fn validate_accepts_a_consistent_manifest_and_a_legacy_one() {
        good().validate().unwrap();
        let mut legacy = good();
        legacy.live_wals.clear(); // legacy v1: no WAL list
        legacy.validate().unwrap();
        let fresh = Manifest {
            version: 1,
            next_file_id: 2,
            min_wal_id: 1,
            live_wals: vec![1],
            applied: 0,
            files: vec![],
        };
        fresh.validate().unwrap();
    }

    #[test]
    fn validate_rejects_every_inconsistency() {
        type Mutate = Box<dyn Fn(&mut Manifest)>;
        let cases: Vec<(&str, Mutate)> = vec![
            ("min_wal_id zero", Box::new(|m| m.min_wal_id = 0)),
            ("min_wal_id >= next", Box::new(|m| m.min_wal_id = 20)),
            (
                "wals not increasing",
                Box::new(|m| m.live_wals = vec![10, 10]),
            ),
            ("wals decreasing", Box::new(|m| m.live_wals = vec![12, 10])),
            ("wals miss min", Box::new(|m| m.live_wals = vec![11, 12])),
            ("wal >= next", Box::new(|m| m.live_wals = vec![10, 20])),
            ("table id zero", Box::new(|m| m.files[0].id = 0)),
            ("table id >= next", Box::new(|m| m.files[0].id = 20)),
            ("duplicate table id", Box::new(|m| m.files[1].id = 3)),
            ("table id == live wal", Box::new(|m| m.files[0].id = 12)),
            ("level 2", Box::new(|m| m.files[0].level = 2)),
            (
                "min > max",
                Box::new(|m| m.files[0].min_key = b"zz".to_vec()),
            ),
            (
                "l1 overlap",
                Box::new(|m| m.files[3].min_key = b"f".to_vec()),
            ),
            (
                "l1 contained",
                Box::new(|m| m.files[3].min_key = b"b".to_vec()),
            ),
        ];
        for (name, mutate) in cases {
            let mut m = good();
            mutate(&mut m);
            assert!(m.validate().is_err(), "{name} must be rejected");
        }
        // L0 files may overlap each other (and L1); only L1 must be disjoint.
        let mut m = good();
        m.files[0].min_key = b"a".to_vec();
        m.validate().unwrap();
    }

    #[test]
    fn a_checksum_valid_but_inconsistent_manifest_decodes_and_is_caught_by_validate() {
        let mut m = good();
        m.files[0].level = 2;
        let d = Manifest::decode(&m.encode()).unwrap(); // structurally fine
        assert!(d.validate().is_err());
    }

    // -----------------------------------------------------------------
    // Property tests: seeded xorshift, no extra deps.
    // -----------------------------------------------------------------

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n.max(1)
        }
        fn bytes(&mut self, max: usize) -> Vec<u8> {
            let n = self.below(max as u64 + 1) as usize;
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    /// A random manifest that satisfies `validate()` (non-empty live WALs).
    fn random_valid(seed: u64) -> Manifest {
        let mut r = Rng(seed | 1);
        let min_wal = 1 + r.below(50);
        let nwals = 1 + r.below(4);
        let mut live = vec![min_wal];
        for _ in 1..nwals {
            let last = *live.last().unwrap();
            live.push(last + 1 + r.below(3));
        }
        let mut next = live.last().unwrap() + 1 + r.below(5);
        // Table ids: distinct, nonzero, below `next`, not a live WAL.
        let mut files = Vec::new();
        let mut l1_hi = 0u8; // level-1 tables get disjoint, ascending ranges
        for id in 1..next {
            if live.contains(&id) || r.below(3) != 0 {
                continue;
            }
            let level = (r.below(2)) as u8;
            let (lo, hi) = if level == 1 {
                l1_hi += 2;
                (vec![l1_hi], vec![l1_hi + 1])
            } else {
                let a = r.bytes(5);
                let mut b = a.clone();
                b.extend(r.bytes(3));
                (a, b) // lo <= hi by construction (hi extends lo)
            };
            files.push(FileMeta {
                id,
                level,
                size: r.next(),
                entries: r.next(),
                min_key: lo,
                max_key: hi,
            });
        }
        next += 0;
        Manifest {
            version: r.next(),
            next_file_id: next,
            min_wal_id: min_wal,
            live_wals: live,
            applied: r.next(),
            files,
        }
    }

    #[test]
    fn prop_valid_manifests_round_trip_and_validate() {
        for seed in 1..=500u64 {
            let m = random_valid(seed);
            m.validate().unwrap_or_else(|e| {
                panic!("seed {seed}: generator produced an invalid manifest: {e}")
            });
            assert_eq!(Manifest::decode(&m.encode()).unwrap(), m, "seed {seed}");
        }
    }

    #[test]
    fn prop_every_single_bit_flip_and_every_truncation_is_detected() {
        for seed in 1..=30u64 {
            let e = random_valid(seed).encode();
            for i in 0..e.len() {
                for bit in 0..8 {
                    let mut d = e.clone();
                    d[i] ^= 1 << bit;
                    assert!(
                        Manifest::decode(&d).is_err(),
                        "seed {seed}: flip {i}.{bit} undetected"
                    );
                }
            }
            for cut in 0..e.len() {
                assert!(
                    Manifest::decode(&e[..cut]).is_err(),
                    "seed {seed}: truncation to {cut} undetected"
                );
            }
            // appended garbage changes the CRC position: also detected
            let mut longer = e.clone();
            longer.push(0);
            assert!(
                Manifest::decode(&longer).is_err(),
                "seed {seed}: trailing byte undetected"
            );
        }
    }

    #[test]
    fn prop_arbitrary_bytes_never_panic() {
        let mut r = Rng(0xC2B2_AE3D_27D4_EB4F);
        for _ in 0..20_000 {
            let _ = Manifest::decode(&r.bytes(200));
        }
        // with a plausible magic
        for _ in 0..20_000 {
            let mut b = if r.below(2) == 0 {
                MAGIC.to_vec()
            } else {
                LEGACY_MAGIC.to_vec()
            };
            b.extend(r.bytes(150));
            let _ = Manifest::decode(&b);
        }
    }

    #[test]
    fn prop_checksum_valid_garbage_bodies_never_panic_and_accepted_ones_are_stable() {
        // Forge a correct CRC around random bodies so the structural parser
        // (and `validate`) see hostile input, including huge count fields.
        let mut r = Rng(0x1656_67B1_9E37_79F9);
        let mut accepted = 0;
        for _ in 0..30_000 {
            let mut body = if r.below(4) == 0 {
                LEGACY_MAGIC.to_vec()
            } else {
                MAGIC.to_vec()
            };
            for _ in 0..4 {
                body.extend_from_slice(&r.below(100).to_le_bytes());
            }
            // counts: sometimes absurd
            let count = |r: &mut Rng| -> u32 {
                match r.below(4) {
                    0 => u32::MAX,
                    1 => 0,
                    2 => 1,
                    _ => r.below(3) as u32,
                }
            };
            body.extend_from_slice(&count(&mut r).to_le_bytes());
            body.extend(r.bytes(80));
            let c = crc32(&body);
            body.extend_from_slice(&c.to_le_bytes());
            if let Ok(m) = Manifest::decode(&body) {
                accepted += 1;
                let _ = m.validate(); // must not panic either
                if !m.live_wals.is_empty() {
                    assert_eq!(
                        Manifest::decode(&m.encode()).unwrap(),
                        m,
                        "an accepted manifest must re-encode stably"
                    );
                }
            }
        }
        // The generator must actually get through the parser sometimes,
        // otherwise this test would silently check nothing.
        assert!(
            accepted > 0,
            "forged bodies never decoded; the generator is useless"
        );
    }

    #[test]
    fn prop_validate_catches_every_single_field_corruption_of_a_valid_manifest() {
        // Mutate one structural field of a valid manifest at a time; each
        // mutation below breaks an invariant `validate` documents.
        for seed in 1..=200u64 {
            let m = random_valid(seed);
            let mut cases: Vec<(&str, Manifest)> = Vec::new();
            let mut x = m.clone();
            x.min_wal_id = 0;
            cases.push(("min_wal_id=0", x));
            let mut x = m.clone();
            x.next_file_id = x.live_wals[0]; // next <= min_wal / live wal
            cases.push(("next_file_id too small", x));
            let mut x = m.clone();
            x.live_wals.push(*x.live_wals.last().unwrap()); // duplicate, not strictly increasing
            cases.push(("duplicate live wal", x));
            if let Some(f) = m.files.first() {
                let mut x = m.clone();
                x.files[0].id = x.next_file_id + f.id; // id >= next_file_id
                cases.push(("table id >= next", x));
                let mut x = m.clone();
                x.files[0].level = 2;
                cases.push(("level 2", x));
                let mut x = m.clone();
                x.files.push(f.clone()); // duplicate table id
                cases.push(("duplicate table", x));
            }
            for (name, bad) in cases {
                assert!(
                    bad.validate().is_err(),
                    "seed {seed}: `{name}` was not rejected"
                );
            }
        }
    }
}
