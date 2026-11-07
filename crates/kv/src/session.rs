//! Durable client sessions: exactly-once application of retried writes.
//!
//! A client that does not know whether its write was applied (timeout,
//! leadership change, lost reply) retries it with the SAME `(client_id, seq)`.
//! Every replica remembers, per client, the highest `seq` it applied; a
//! command whose `seq` is not greater than that is NOT applied again.
//!
//! * `seq == last`  -> the retry of the latest write: acknowledged with the
//!   original result (success - sets and deletes have no other result).
//! * `seq <  last`  -> the client has already moved on; the write is NOT
//!   re-applied and the client gets an explicit "superseded" error. It is
//!   never silently dropped or acknowledged.
//! * `seq >  last`  -> new: applied, and the session advances.
//!
//! One client id must be one sequential stream (one outstanding write at a
//! time, increasing `seq`). Pipelining on a single id would get its
//! out-of-order writes rejected as superseded.
//!
//! The table is part of the state machine: it is replicated by the log,
//! written atomically with the data in the LSM engine (see `LsmStore`), and
//! carried in snapshots. It is bounded: when full, the session with the
//! oldest last-applied log index is evicted (ties by client id), which is
//! deterministic on every replica. A client idle long enough to be evicted
//! loses its protection: a very late retry would be applied again.
//!
//! # On-disk / wire formats (all new formats are CRC-protected)
//!
//! Session record (LSM value, key = `KEY_PREFIX` + client id big-endian):
//! `[ver:u8=1][client:u64][seq:u64][index:u64][crc32:u32 of the preceding bytes]`
//!
//! Snapshot v2 (used only when the table is non-empty; an empty table emits
//! the original v1 bytes unchanged):
//! `[MAGIC:u32][body_len:u32][body = v1 snapshot][n:u32][n x (client,seq,index: u64 x3)][crc32 of all preceding bytes]`
//! MAGIC read as a v1 entry count would be ~4.29 billion, which no valid v1
//! snapshot can contain.

use std::collections::HashMap;

use raft_core::LogIndex;

pub type ClientId = u64;

/// Maximum number of remembered clients.
pub const MAX_SESSIONS: usize = 10_000;

/// First four bytes (LE) of a v2 snapshot.
pub const SNAPSHOT_V2_MAGIC: u32 = 0xFFFF_DD02;

/// LSM key namespace for session records. Engine keys are raw bytes while
/// user keys are valid UTF-8, and `0xFF` never occurs in UTF-8, so a user key
/// can never collide with (or be mistaken for) a session record.
pub const KEY_PREFIX: &[u8] = b"\xFFsess/";
/// Exclusive upper bound of the session key range (`/` + 1).
pub const KEY_PREFIX_END: &[u8] = b"\xFFsess0";

const REC_VERSION: u8 = 1;
const REC_LEN: usize = 1 + 8 + 8 + 8 + 4;

/// CRC-32 (IEEE 802.3), the same polynomial the WALs use.
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &b in bytes {
        crc ^= b as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Session {
    /// Highest sequence number applied for this client.
    pub seq: u64,
    /// Log index at which it was applied (drives deterministic eviction).
    pub index: LogIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Not seen before: apply it.
    Fresh,
    /// Retry of the latest applied write: do not apply, acknowledge.
    Duplicate,
    /// Older than the latest applied write: do not apply, report superseded.
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTable {
    map: HashMap<ClientId, Session>,
    cap: usize,
}

impl Default for SessionTable {
    fn default() -> Self {
        Self::with_cap(MAX_SESSIONS)
    }
}

impl SessionTable {
    pub fn with_cap(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            cap: cap.max(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn len(&self) -> usize {
        self.map.len()
    }
    pub fn get(&self, client: ClientId) -> Option<Session> {
        self.map.get(&client).copied()
    }
    pub fn last_seq(&self, client: ClientId) -> Option<u64> {
        self.map.get(&client).map(|s| s.seq)
    }

    pub fn verdict(&self, client: ClientId, seq: u64) -> Verdict {
        match self.map.get(&client) {
            None => Verdict::Fresh,
            Some(s) if seq > s.seq => Verdict::Fresh,
            Some(s) if seq == s.seq => Verdict::Duplicate,
            Some(_) => Verdict::Stale,
        }
    }

    /// Record an applied write. Returns the client evicted to make room, if any.
    pub fn record(&mut self, client: ClientId, seq: u64, index: LogIndex) -> Option<ClientId> {
        let mut evicted = None;
        if !self.map.contains_key(&client) && self.map.len() >= self.cap {
            let victim = self
                .map
                .iter()
                .min_by_key(|(c, s)| (s.index, **c))
                .map(|(c, _)| *c)
                .expect("table is non-empty when full");
            self.map.remove(&victim);
            evicted = Some(victim);
        }
        self.map.insert(client, Session { seq, index });
        evicted
    }

    /// Sorted by client id: deterministic.
    pub fn sorted(&self) -> Vec<(ClientId, Session)> {
        let mut v: Vec<_> = self.map.iter().map(|(c, s)| (*c, *s)).collect();
        v.sort_by_key(|(c, _)| *c);
        v
    }

    pub fn from_entries(entries: Vec<(ClientId, Session)>) -> Result<Self, String> {
        let mut t = Self::default();
        if entries.len() > t.cap {
            return Err(format!(
                "{} sessions exceeds the limit of {}",
                entries.len(),
                t.cap
            ));
        }
        for (c, s) in entries {
            if c == 0 || s.seq == 0 {
                return Err("session with client id 0 or sequence 0".to_string());
            }
            if t.map.insert(c, s).is_some() {
                return Err(format!("duplicate session for client {c}"));
            }
        }
        Ok(t)
    }
}

// ----------------------------------------------------------- LSM records

pub fn key_for(client: ClientId) -> Vec<u8> {
    let mut k = KEY_PREFIX.to_vec();
    k.extend_from_slice(&client.to_be_bytes());
    k
}

pub fn is_session_key(key: &[u8]) -> bool {
    key.first() == Some(&0xFF)
}

pub fn client_from_key(key: &[u8]) -> Result<ClientId, String> {
    if key.len() != KEY_PREFIX.len() + 8 || !key.starts_with(KEY_PREFIX) {
        return Err("malformed session key".to_string());
    }
    Ok(u64::from_be_bytes(
        key[KEY_PREFIX.len()..].try_into().unwrap(),
    ))
}

pub fn encode_record(client: ClientId, s: &Session) -> Vec<u8> {
    let mut out = Vec::with_capacity(REC_LEN);
    out.push(REC_VERSION);
    out.extend_from_slice(&client.to_le_bytes());
    out.extend_from_slice(&s.seq.to_le_bytes());
    out.extend_from_slice(&s.index.to_le_bytes());
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// `client` is the id taken from the record's key; the id stored inside the
/// record must agree, binding value to key.
pub fn decode_record(client: ClientId, bytes: &[u8]) -> Result<Session, String> {
    if bytes.len() != REC_LEN {
        return Err(format!(
            "session record is {} bytes, expected {REC_LEN}",
            bytes.len()
        ));
    }
    let (body, crc) = bytes.split_at(REC_LEN - 4);
    if crc32(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err("session record checksum mismatch".to_string());
    }
    if body[0] != REC_VERSION {
        return Err(format!("unknown session record version {}", body[0]));
    }
    let stored = u64::from_le_bytes(body[1..9].try_into().unwrap());
    if stored != client {
        return Err("session record belongs to a different client than its key".to_string());
    }
    let seq = u64::from_le_bytes(body[9..17].try_into().unwrap());
    let index = u64::from_le_bytes(body[17..25].try_into().unwrap());
    if client == 0 || seq == 0 {
        return Err("session record with client id 0 or sequence 0".to_string());
    }
    Ok(Session { seq, index })
}

// -------------------------------------------------------------- snapshots

/// Wrap a v1 snapshot body with the session table. Empty table => the v1
/// bytes unchanged (so engines without sessions behave exactly as before).
pub fn wrap_snapshot(body: Vec<u8>, table: &SessionTable) -> Vec<u8> {
    if table.is_empty() {
        return body;
    }
    let sessions = table.sorted();
    let mut out = Vec::with_capacity(body.len() + 16 + sessions.len() * 24 + 4);
    out.extend_from_slice(&SNAPSHOT_V2_MAGIC.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out.extend_from_slice(&(sessions.len() as u32).to_le_bytes());
    for (c, s) in sessions {
        out.extend_from_slice(&c.to_le_bytes());
        out.extend_from_slice(&s.seq.to_le_bytes());
        out.extend_from_slice(&s.index.to_le_bytes());
    }
    let crc = crc32(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Split a snapshot into its v1 body and its sessions. A v1 snapshot yields
/// no sessions. Any damage to a v2 snapshot (truncation, bit flip) is an
/// error: dedup state is never silently dropped.
pub fn split_snapshot(data: &[u8]) -> Result<(&[u8], SessionTable), String> {
    let is_v2 =
        data.len() >= 4 && u32::from_le_bytes(data[..4].try_into().unwrap()) == SNAPSHOT_V2_MAGIC;
    if !is_v2 {
        return Ok((data, SessionTable::default()));
    }
    if data.len() < 4 + 4 + 4 + 4 {
        return Err("truncated v2 snapshot".to_string());
    }
    let (covered, crc) = data.split_at(data.len() - 4);
    if crc32(covered) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err("v2 snapshot checksum mismatch".to_string());
    }
    let body_len = u32::from_le_bytes(covered[4..8].try_into().unwrap()) as usize;
    let body_end = 8usize
        .checked_add(body_len)
        .filter(|e| e + 4 <= covered.len())
        .ok_or("v2 snapshot body length is out of range")?;
    let body = &covered[8..body_end];
    let rest = &covered[body_end..];
    let n = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
    if rest.len() != 4 + n.checked_mul(24).ok_or("session count overflow")? {
        return Err("v2 snapshot session section has the wrong length".to_string());
    }
    let mut entries = Vec::with_capacity(n);
    for chunk in rest[4..].as_chunks::<24>().0 {
        let c = u64::from_le_bytes(chunk[0..8].try_into().unwrap());
        let seq = u64::from_le_bytes(chunk[8..16].try_into().unwrap());
        let index = u64::from_le_bytes(chunk[16..24].try_into().unwrap());
        entries.push((c, Session { seq, index }));
    }
    Ok((body, SessionTable::from_entries(entries)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc32_matches_the_standard_check_value() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn verdicts() {
        let mut t = SessionTable::default();
        assert_eq!(t.verdict(7, 1), Verdict::Fresh);
        t.record(7, 5, 10);
        assert_eq!(t.verdict(7, 5), Verdict::Duplicate);
        assert_eq!(t.verdict(7, 4), Verdict::Stale);
        assert_eq!(t.verdict(7, 6), Verdict::Fresh);
        assert_eq!(t.verdict(8, 1), Verdict::Fresh);
    }

    #[test]
    fn eviction_is_deterministic_oldest_index_first() {
        let mut t = SessionTable::with_cap(2);
        assert_eq!(t.record(5, 1, 10), None);
        assert_eq!(t.record(3, 1, 10), None);
        // tie on index 10 -> smaller client id (3) goes
        assert_eq!(t.record(9, 1, 11), Some(3));
        // updating an existing client never evicts
        assert_eq!(t.record(5, 2, 12), None);
        assert_eq!(t.record(1, 1, 13), Some(9));
    }

    #[test]
    fn record_roundtrip_and_every_flip_and_truncation_is_rejected() {
        let s = Session {
            seq: 41,
            index: 977,
        };
        let enc = encode_record(0xABCD, &s);
        assert_eq!(decode_record(0xABCD, &enc).unwrap(), s);
        // wrong client for the key
        assert!(decode_record(0xABCE, &enc).is_err());
        for i in 0..enc.len() {
            for bit in 0..8 {
                let mut m = enc.clone();
                m[i] ^= 1 << bit;
                assert!(
                    decode_record(0xABCD, &m).is_err(),
                    "flip byte {i} bit {bit} accepted"
                );
            }
        }
        for len in 0..enc.len() {
            assert!(
                decode_record(0xABCD, &enc[..len]).is_err(),
                "truncation to {len} accepted"
            );
        }
        let mut longer = enc.clone();
        longer.push(0);
        assert!(decode_record(0xABCD, &longer).is_err());
    }

    fn table() -> SessionTable {
        let mut t = SessionTable::default();
        t.record(3, 9, 100);
        t.record(1 << 40, 2, 101);
        t
    }

    #[test]
    fn snapshot_roundtrip_and_empty_table_is_the_v1_bytes() {
        let body = vec![0, 0, 0, 0];
        assert_eq!(wrap_snapshot(body.clone(), &SessionTable::default()), body);
        let w = wrap_snapshot(body.clone(), &table());
        let (b, t) = split_snapshot(&w).unwrap();
        assert_eq!(b, &body[..]);
        assert_eq!(t, table());
        // a v1 snapshot passes through untouched
        let (b, t) = split_snapshot(&body).unwrap();
        assert_eq!(b, &body[..]);
        assert!(t.is_empty());
    }

    #[test]
    fn v2_snapshot_every_byte_flip_and_every_truncation_is_rejected() {
        let body = vec![1, 0, 0, 0, 1, 0, 0, 0, b'k', 1, 0, 0, 0, b'v'];
        let w = wrap_snapshot(body, &table());
        for i in 0..w.len() {
            for bit in 0..8 {
                let mut m = w.clone();
                m[i] ^= 1 << bit;
                // A damaged magic reads as a v1 count of ~4 billion; callers
                // reject that when they parse the body. Everything else must
                // fail here.
                match split_snapshot(&m) {
                    Err(_) => {}
                    Ok((b, t)) => {
                        assert!(i < 4, "flip byte {i} bit {bit} silently accepted");
                        assert!(
                            t.is_empty() && b.len() == m.len(),
                            "damaged magic must fall to the v1 path"
                        );
                        let count = u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
                        assert!(count > b.len(), "v1 path would accept a damaged magic");
                    }
                }
            }
        }
        for len in 0..w.len() {
            match split_snapshot(&w[..len]) {
                Err(_) => {}
                Ok((_, t)) => {
                    // only possible for < 4 bytes (too short to be v2); the v1 parser rejects those
                    assert!(
                        len < 4 && t.is_empty(),
                        "truncation to {len} silently accepted"
                    );
                }
            }
        }
    }
}
