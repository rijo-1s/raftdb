//! The backup file: a point-in-time export of one node's state machine.
//!
//! ```text
//! offset  size   field
//! 0       8      magic  "RDBBAK\0" + format version byte (1)
//! 8       8      created_unix_ms        (informational)
//! 16      8      source_node            (informational; 0 = unknown)
//! 24      8      last_applied           the applied log index the export is as-of
//! 32      8      last_applied_term      the term of the entry at that index
//! 40      4      n_peers
//! 44      8*n    peers                  full cluster membership at that point
//! ..      8      data_len
//! ..      data_len  state machine snapshot (the SAME bytes a Raft snapshot holds:
//!                snapshot v2, keys + the exactly-once client session table)
//! ..      4      CRC-32 of every preceding byte
//! ```
//!
//! All integers little-endian. Decoding verifies the checksum before it parses
//! anything, never allocates from a length it has not checked against the
//! bytes actually present, and rejects trailing bytes: every truncation and
//! every single-byte flip is refused, never partly accepted.

use crate::session::crc32;
use crate::KvStore;

/// Largest state image a backup RPC will carry (one gRPC message). Larger
/// databases are refused with a clear error instead of silently truncating.
pub const MAX_STATE_BYTES: usize = 512 << 20;
/// gRPC message limit for the Backup RPC: the state plus generous headroom.
pub const MAX_MESSAGE_BYTES: usize = MAX_STATE_BYTES + (1 << 20);

pub const MAGIC: [u8; 8] = *b"RDBBAK\x00\x01";
/// Smallest possible file: magic + 4 u64 + n_peers + data_len + CRC.
const MIN_LEN: usize = 8 + 32 + 4 + 8 + 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Backup {
    pub created_unix_ms: u64,
    pub source_node: u64,
    pub last_applied: u64,
    pub last_applied_term: u64,
    pub peers: Vec<u64>,
    pub state: Vec<u8>,
}

/// What the state machine image contains.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Summary {
    pub keys: usize,
    pub sessions: usize,
}

impl Backup {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(MIN_LEN + self.peers.len() * 8 + self.state.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&self.created_unix_ms.to_le_bytes());
        out.extend_from_slice(&self.source_node.to_le_bytes());
        out.extend_from_slice(&self.last_applied.to_le_bytes());
        out.extend_from_slice(&self.last_applied_term.to_le_bytes());
        out.extend_from_slice(&(self.peers.len() as u32).to_le_bytes());
        for p in &self.peers {
            out.extend_from_slice(&p.to_le_bytes());
        }
        out.extend_from_slice(&(self.state.len() as u64).to_le_bytes());
        out.extend_from_slice(&self.state);
        let c = crc32(&out);
        out.extend_from_slice(&c.to_le_bytes());
        out
    }

    /// Structural decode: magic, checksum, lengths. Does not interpret the
    /// state machine bytes (see [`Backup::check`]).
    pub fn decode(bytes: &[u8]) -> Result<Backup, String> {
        if bytes.len() < MIN_LEN {
            return Err("file is too short to be a backup".into());
        }
        if bytes[..8] != MAGIC {
            return Err("bad magic: not a RaftDB backup, or an unsupported format version".into());
        }
        let (body, tail) = bytes.split_at(bytes.len() - 4);
        if crc32(body) != u32::from_le_bytes(tail.try_into().unwrap()) {
            return Err("checksum mismatch: the backup is damaged or truncated".into());
        }
        let mut p = 8usize;
        let u64_at = |p: &mut usize| -> u64 {
            let v = u64::from_le_bytes(body[*p..*p + 8].try_into().unwrap());
            *p += 8;
            v
        };
        let created_unix_ms = u64_at(&mut p);
        let source_node = u64_at(&mut p);
        let last_applied = u64_at(&mut p);
        let last_applied_term = u64_at(&mut p);
        let n = u32::from_le_bytes(body[p..p + 4].try_into().unwrap()) as usize;
        p += 4;
        // The count must be backed by real bytes before anything is allocated.
        let peer_bytes = n.checked_mul(8).ok_or("peer count overflows")?;
        if p.checked_add(peer_bytes)
            .and_then(|e| e.checked_add(8))
            .is_none_or(|e| e > body.len())
        {
            return Err("peer list runs past the end of the file".into());
        }
        let mut peers = Vec::with_capacity(n);
        for _ in 0..n {
            peers.push(u64_at(&mut p));
        }
        let data_len = u64_at(&mut p);
        if data_len != (body.len() - p) as u64 {
            return Err("state length disagrees with the file size".into());
        }
        Ok(Backup {
            created_unix_ms,
            source_node,
            last_applied,
            last_applied_term,
            peers,
            state: body[p..].to_vec(),
        })
    }

    /// Semantic validation: is this something a node could boot from?
    pub fn check(&self) -> Result<Summary, String> {
        if self.last_applied == 0 {
            return Err("last_applied is 0: not a snapshot".into());
        }
        if self.last_applied_term == 0 {
            return Err("last_applied_term is 0".into());
        }
        if self.peers.is_empty() {
            return Err("empty membership".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for &p in &self.peers {
            if p == 0 || !seen.insert(p) {
                return Err(format!(
                    "invalid membership (node id {p} is zero or repeated)"
                ));
            }
        }
        let store = KvStore::restore(&self.state)
            .map_err(|e| format!("state machine image is invalid: {e}"))?;
        Ok(Summary {
            keys: store.len(),
            sessions: store.sessions().len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Command;

    fn sample() -> Backup {
        let mut s = KvStore::new();
        for i in 1..=6u64 {
            s.apply_at(
                i,
                &Command::Set {
                    key: format!("k{i}"),
                    value: format!("v{i}"),
                }
                .encode(),
            )
            .unwrap();
        }
        let sess = Command::session(
            77,
            5,
            Command::Set {
                key: "sk".into(),
                value: "sv".into(),
            },
        )
        .unwrap();
        s.apply_at(7, &sess.encode()).unwrap();
        Backup {
            created_unix_ms: 1_700_000_000_000,
            source_node: 2,
            last_applied: 7,
            last_applied_term: 3,
            peers: vec![1, 2, 3],
            state: s.snapshot(),
        }
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn bytes(&mut self, max: usize) -> Vec<u8> {
            let n = (self.next() % (max as u64 + 1)) as usize;
            (0..n).map(|_| self.next() as u8).collect()
        }
    }

    #[test]
    fn round_trips_and_summarises_keys_and_sessions() {
        let b = sample();
        assert_eq!(Backup::decode(&b.encode()).unwrap(), b);
        let s = b.check().unwrap();
        assert_eq!((s.keys, s.sessions), (7, 1));
    }

    #[test]
    fn every_single_byte_flip_is_refused() {
        let enc = sample().encode();
        for i in 0..enc.len() {
            for mask in [0x01u8, 0x40, 0xFF] {
                let mut d = enc.clone();
                d[i] ^= mask;
                assert!(
                    Backup::decode(&d).is_err(),
                    "flip {mask:#x} at byte {i}/{} was accepted",
                    enc.len()
                );
            }
        }
    }

    #[test]
    fn every_truncation_and_every_extension_is_refused() {
        let enc = sample().encode();
        for cut in 0..enc.len() {
            assert!(
                Backup::decode(&enc[..cut]).is_err(),
                "truncation to {cut} bytes was accepted"
            );
        }
        for extra in [vec![0u8], vec![0; 4], vec![0xFF; 17]] {
            let mut d = enc.clone();
            d.extend_from_slice(&extra);
            assert!(
                Backup::decode(&d).is_err(),
                "{} trailing bytes were accepted",
                extra.len()
            );
        }
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        let mut r = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..20_000 {
            let _ = Backup::decode(&r.bytes(200));
        }
        for _ in 0..20_000 {
            let mut b = MAGIC.to_vec();
            b.extend(r.bytes(150));
            let _ = Backup::decode(&b);
        }
    }

    #[test]
    fn checksum_valid_garbage_never_panics_and_hostile_counts_are_refused() {
        let mut r = Rng(0xD1B5_4A32_D192_ED03);
        let mut decoded = 0;
        for i in 0..20_000u64 {
            let mut body = MAGIC.to_vec();
            for _ in 0..4 {
                body.extend_from_slice(&(r.next() % 50).to_le_bytes());
            }
            let hostile = i % 4 == 0;
            let n: u32 = if hostile {
                u32::MAX
            } else {
                (r.next() % 3) as u32
            };
            body.extend_from_slice(&n.to_le_bytes());
            if hostile {
                // a count no file could back: must be refused WITHOUT allocating 32 GiB
                body.extend(r.bytes(70));
            } else {
                for _ in 0..n {
                    body.extend_from_slice(&(r.next() % 5).to_le_bytes());
                }
                let data = r.bytes(70);
                body.extend_from_slice(&(data.len() as u64).to_le_bytes());
                body.extend_from_slice(&data);
            }
            let c = crc32(&body);
            body.extend_from_slice(&c.to_le_bytes());
            if let Ok(b) = Backup::decode(&body) {
                assert!(!hostile, "a hostile peer count was accepted");
                decoded += 1;
                let _ = b.check(); // random state bytes: must refuse, never panic
            }
        }
        assert!(
            decoded > 0,
            "the generator never got past the structural decoder"
        );
    }

    #[test]
    fn check_refuses_well_formed_files_that_are_not_bootable() {
        let good = sample();
        let mut cases: Vec<(&str, Backup)> = vec![];
        for (name, f) in [
            (
                "index 0",
                (|b: &mut Backup| b.last_applied = 0) as fn(&mut Backup),
            ),
            ("term 0", |b| b.last_applied_term = 0),
            ("no peers", |b| b.peers.clear()),
            ("duplicate peer", |b| b.peers = vec![1, 1]),
            ("peer 0", |b| b.peers = vec![0, 1]),
            ("garbage state", |b| {
                b.state = b"not a state machine".to_vec()
            }),
            ("flipped state byte", |b| {
                let n = b.state.len();
                b.state[n / 2] ^= 0x10;
            }),
        ] {
            let mut b = good.clone();
            f(&mut b);
            cases.push((name, b));
        }
        for (name, b) in cases {
            let again = Backup::decode(&b.encode()).unwrap(); // structurally fine (CRC recomputed)
            assert!(again.check().is_err(), "{name} was accepted by check()");
        }
    }
}
