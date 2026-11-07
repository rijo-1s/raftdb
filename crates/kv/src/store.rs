//! The state machine that sits on top of `raft-core`: a simple string
//! key-value store, plus the on-the-wire encoding for the commands
//! `RaftNode::propose()` replicates as opaque `Vec<u8>` log entries.
//!
//! `raft-core` never looks inside these bytes - encoding/decoding and
//! applying commands is entirely this crate's job, which is exactly the
//! separation of concerns the whole project has been building toward.

use std::collections::HashMap;

use raft_core::LogIndex;

use crate::session::{self, SessionTable, Verdict};

const TAG_SET: u8 = 1;
const TAG_DELETE: u8 = 2;
const TAG_SESSION: u8 = 3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// A Set or Delete made idempotent by `(client, seq)`; see `session.rs`.
    /// `op` is never itself a `Session`.
    Session {
        client: u64,
        seq: u64,
        op: Box<Command>,
    },
}

impl Command {
    /// `None` if `client` or `seq` is 0 (reserved for "no session") or `op`
    /// is not a plain Set/Delete.
    pub fn session(client: u64, seq: u64, op: Command) -> Option<Command> {
        if client == 0 || seq == 0 || matches!(op, Command::Session { .. }) {
            return None;
        }
        Some(Command::Session {
            client,
            seq,
            op: Box::new(op),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            Command::Session { client, seq, op } => {
                let inner = op.encode();
                let mut out = Vec::with_capacity(1 + 8 + 8 + inner.len());
                out.push(TAG_SESSION);
                out.extend_from_slice(&client.to_le_bytes());
                out.extend_from_slice(&seq.to_le_bytes());
                out.extend_from_slice(&inner);
                out
            }
            Command::Set { key, value } => {
                let kb = key.as_bytes();
                let vb = value.as_bytes();
                let mut out = Vec::with_capacity(1 + 4 + kb.len() + 4 + vb.len());
                out.push(TAG_SET);
                out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                out.extend_from_slice(kb);
                out.extend_from_slice(&(vb.len() as u32).to_le_bytes());
                out.extend_from_slice(vb);
                out
            }
            Command::Delete { key } => {
                let kb = key.as_bytes();
                let mut out = Vec::with_capacity(1 + 4 + kb.len());
                out.push(TAG_DELETE);
                out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
                out.extend_from_slice(kb);
                out
            }
        }
    }

    pub fn decode(data: &[u8]) -> Option<Command> {
        let (&tag, rest) = data.split_first()?;
        match tag {
            TAG_SESSION => {
                if rest.len() < 16 {
                    return None;
                }
                let client = u64::from_le_bytes(rest[..8].try_into().ok()?);
                let seq = u64::from_le_bytes(rest[8..16].try_into().ok()?);
                let op = Command::decode(&rest[16..])?;
                Command::session(client, seq, op)
            }
            TAG_SET => {
                let (key, rest) = read_string(rest)?;
                let (value, rest) = read_string(rest)?;
                if !rest.is_empty() {
                    return None; // trailing garbage
                }
                Some(Command::Set { key, value })
            }
            TAG_DELETE => {
                let (key, rest) = read_string(rest)?;
                if !rest.is_empty() {
                    return None;
                }
                Some(Command::Delete { key })
            }
            _ => None,
        }
    }
}

fn read_string(data: &[u8]) -> Option<(String, &[u8])> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes(data[0..4].try_into().ok()?) as usize;
    let rest = &data[4..];
    if rest.len() < len {
        return None;
    }
    let s = String::from_utf8(rest[..len].to_vec()).ok()?;
    Some((s, &rest[len..]))
}

/// The state machine itself: just a string->string map. `apply()` is what
/// `raft-core`'s committed entries get fed into; `snapshot()`/`restore()`
/// are what `RaftNode::compact_log()`/`InstallSnapshot` use to move the
/// entire state around as one opaque blob.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct KvStore {
    map: HashMap<String, String>,
    sessions: SessionTable,
}

impl KvStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.map.keys()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Apply one committed log entry's bytes, with no log index (so any
    /// session it records is treated as index 0 for eviction ordering). The
    /// state-machine path uses [`KvStore::apply_at`].
    pub fn apply(&mut self, data: &[u8]) -> Result<(), String> {
        self.apply_at(0, data)
    }

    /// Apply the committed entry at log `index`. Malformed input (which
    /// should never happen for entries this crate itself produced) is
    /// reported rather than silently ignored, since a state machine that
    /// quietly skips a write has diverged from every other replica.
    pub fn apply_at(&mut self, index: LogIndex, data: &[u8]) -> Result<(), String> {
        match Command::decode(data) {
            Some(Command::Session { client, seq, op }) => {
                if self.sessions.verdict(client, seq) == Verdict::Fresh {
                    self.sessions.record(client, seq, index);
                    self.apply_plain(*op);
                }
                // Duplicate / Stale: deliberately not applied again.
                Ok(())
            }
            Some(plain) => {
                self.apply_plain(plain);
                Ok(())
            }
            None => Err("malformed command bytes in committed log entry".to_string()),
        }
    }

    fn apply_plain(&mut self, cmd: Command) {
        match cmd {
            Command::Set { key, value } => {
                self.map.insert(key, value);
            }
            Command::Delete { key } => {
                self.map.remove(&key);
            }
            Command::Session { .. } => unreachable!("sessions do not nest"),
        }
    }

    pub fn session_last_seq(&self, client: u64) -> Option<u64> {
        self.sessions.last_seq(client)
    }

    pub fn sessions(&self) -> &SessionTable {
        &self.sessions
    }

    /// Serialize the entire store to bytes, for `RaftNode::compact_log()`.
    /// Format: `[num_entries:u32]` then, per entry, `[key_len:u32][key]
    /// [value_len:u32][value]`.
    pub fn snapshot(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.map.len() as u32).to_le_bytes());
        // Sort for deterministic output (helps tests compare snapshots;
        // HashMap iteration order isn't stable).
        let mut entries: Vec<(&String, &String)> = self.map.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        for (k, v) in entries {
            let kb = k.as_bytes();
            let vb = v.as_bytes();
            out.extend_from_slice(&(kb.len() as u32).to_le_bytes());
            out.extend_from_slice(kb);
            out.extend_from_slice(&(vb.len() as u32).to_le_bytes());
            out.extend_from_slice(vb);
        }
        session::wrap_snapshot(out, &self.sessions)
    }

    /// Rebuild a store from bytes produced by `snapshot()` - this is what
    /// happens when `RaftNode::take_pending_snapshot()` hands back a
    /// leader's `InstallSnapshot` payload.
    pub fn restore(data: &[u8]) -> Result<Self, String> {
        let (data, sessions) = session::split_snapshot(data)?;
        if data.len() < 4 {
            return Err("snapshot too short to contain a length header".to_string());
        }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut rest = &data[4..];
        // `count` comes from the file: never trust it for an allocation. Each
        // entry needs at least 8 bytes, so a larger count is already invalid.
        if count > data.len() / 8 {
            return Err("snapshot entry count exceeds what its bytes can hold".to_string());
        }
        let mut map = HashMap::with_capacity(count);
        for _ in 0..count {
            let (key, r) = read_string(rest).ok_or("truncated snapshot: missing key")?;
            let (value, r) = read_string(r).ok_or("truncated snapshot: missing value")?;
            map.insert(key, value);
            rest = r;
        }
        if !rest.is_empty() {
            return Err("trailing garbage after snapshot entries".to_string());
        }
        Ok(Self { map, sessions })
    }
}
