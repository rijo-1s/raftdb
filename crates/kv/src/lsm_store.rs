//! `LsmStore`: the KV state machine on the durable LSM engine.
//!
//! ## Why the engine may skip per-write fsync (and why that is safe)
//!
//! Raft's own log is the source of truth: an entry is fsynced there on a
//! majority *before* it can be committed, let alone applied here. The engine
//! therefore only has to be *recoverable*, not independently durable:
//!
//! * the Raft `applied` index is persisted atomically with the data it
//!   describes (same WAL record);
//! * after a crash the engine reports how far it got; Raft replays committed
//!   entries after the log boundary and entries `<= applied_index` are
//!   skipped, the rest re-applied, so a lost engine tail is simply redone;
//! * [`StateMachine::durability_barrier`] (an fsync) runs before the Raft
//!   log is compacted, so the log is never truncated past what the engine
//!   cannot reproduce; and if the engine is ever *behind* the Raft snapshot
//!   (or corrupt) it is rebuilt from that snapshot.
//!
//! `sync_wal = true` additionally fsyncs every apply; it is available (and
//! tested) but not needed for correctness.

use std::path::{Path, PathBuf};

use raft_core::LogIndex;
use storage::lsm::{Lsm, LsmError, LsmOptions, Op, Stats};

use crate::session::{self, SessionTable, Verdict};
use crate::state_machine::StateMachine;
use crate::store::Command;

#[derive(Debug, Clone)]
pub struct LsmStoreOptions {
    pub engine: LsmOptions,
}

impl Default for LsmStoreOptions {
    fn default() -> Self {
        Self {
            engine: LsmOptions {
                // Recoverable from the Raft log; see the module docs.
                sync_wal: false,
                ..LsmOptions::default()
            },
        }
    }
}

/// What `open_or_rebuild` did, for logging and tests.
#[derive(Debug)]
pub enum OpenOutcome {
    Clean,
    /// The engine directory failed validation. It was moved aside (never
    /// deleted) to this path and a fresh engine created; the Raft snapshot
    /// and log rebuild the contents.
    Quarantined {
        moved_to: PathBuf,
        reason: String,
    },
}

pub struct LsmStore {
    db: Lsm,
    /// Mirror of the session records stored in the engine under
    /// `session::KEY_PREFIX`. Those records are written in the same atomic
    /// engine write as the data and the applied index, so after a crash the
    /// table reloaded from disk is exactly consistent with the data.
    sessions: SessionTable,
}

fn map_err(e: LsmError) -> String {
    e.to_string()
}

impl LsmStore {
    /// Wrap an opened engine, reloading the session table from it. A damaged
    /// session record is corruption: fail closed rather than run with a
    /// table that silently forgot a client.
    fn from_db(db: Lsm) -> Result<LsmStore, LsmError> {
        let sessions = Self::load_sessions(&db)?;
        Ok(LsmStore { db, sessions })
    }

    fn load_sessions(db: &Lsm) -> Result<SessionTable, LsmError> {
        let rows = db.scan(
            Some(session::KEY_PREFIX),
            Some(session::KEY_PREFIX_END),
            usize::MAX,
        )?;
        let mut entries = Vec::with_capacity(rows.len());
        for (k, v) in rows {
            let bad = |m: String| LsmError::Corruption(format!("session record: {m}"));
            let client = session::client_from_key(&k).map_err(bad)?;
            let s = session::decode_record(client, &v).map_err(bad)?;
            entries.push((client, s));
        }
        SessionTable::from_entries(entries)
            .map_err(|m| LsmError::Corruption(format!("session table: {m}")))
    }

    /// Open strictly: corruption is an error and nothing is modified.
    pub fn open(dir: impl AsRef<Path>, opts: LsmStoreOptions) -> Result<LsmStore, LsmError> {
        Self::from_db(Lsm::open(dir, opts.engine)?)
    }

    /// Open, and if the engine's files are corrupt, quarantine them and start
    /// empty so the caller can rebuild from Raft state. Plain I/O errors are
    /// *not* treated as corruption and are returned.
    pub fn open_or_rebuild(
        dir: impl AsRef<Path>,
        opts: LsmStoreOptions,
    ) -> Result<(LsmStore, OpenOutcome), LsmError> {
        let dir = dir.as_ref();
        match Lsm::open(dir, opts.engine.clone()).and_then(Self::from_db) {
            Ok(store) => Ok((store, OpenOutcome::Clean)),
            Err(LsmError::Corruption(reason)) => {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or(0);
                let mut name = dir
                    .file_name()
                    .map(|n| n.to_os_string())
                    .unwrap_or_default();
                name.push(format!(".corrupt-{ts}"));
                let moved_to = dir.with_file_name(name);
                std::fs::rename(dir, &moved_to)?;
                let store = Self::from_db(Lsm::open(dir, opts.engine)?)?;
                Ok((store, OpenOutcome::Quarantined { moved_to, reason }))
            }
            Err(e) => Err(e),
        }
    }

    pub fn stats(&self) -> Stats {
        self.db.stats()
    }
    pub fn engine(&self) -> &Lsm {
        &self.db
    }

    /// Range scan over string keys: `start <= key < end`, ascending.
    pub fn scan(
        &self,
        start: Option<&str>,
        end: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, String)>, String> {
        // User keys are valid UTF-8 and so always sort below 0xFF; clamping an
        // open end here keeps the engine's session records out of user scans.
        let end = end.map(str::as_bytes).unwrap_or(&[0xFF]);
        let rows = self
            .db
            .scan(start.map(str::as_bytes), Some(end), limit)
            .map_err(map_err)?;
        rows.into_iter()
            .map(|(k, v)| Ok((utf8(k)?, utf8(v)?)))
            .collect()
    }

    fn plain_op(cmd: Command) -> Op {
        match cmd {
            Command::Set { key, value } => Op::Put(key.into_bytes(), value.into_bytes()),
            Command::Delete { key } => Op::Delete(key.into_bytes()),
            Command::Session { .. } => unreachable!("sessions do not nest"),
        }
    }

    /// Turn one committed entry into engine ops, consulting and advancing the
    /// in-memory session table. A retried (duplicate/stale) write contributes
    /// no data op; it re-asserts the unchanged session record instead, so the
    /// engine write is never empty and the applied index still advances.
    fn plan(&mut self, index: LogIndex, data: &[u8], ops: &mut Vec<Op>) -> Result<(), String> {
        match Command::decode(data).ok_or("malformed command bytes in committed log entry")? {
            Command::Session { client, seq, op } => match self.sessions.verdict(client, seq) {
                Verdict::Fresh => {
                    ops.push(Self::plain_op(*op));
                    if let Some(evicted) = self.sessions.record(client, seq, index) {
                        ops.push(Op::Delete(session::key_for(evicted)));
                    }
                    let s = self.sessions.get(client).expect("just recorded");
                    ops.push(Op::Put(
                        session::key_for(client),
                        session::encode_record(client, &s),
                    ));
                }
                Verdict::Duplicate | Verdict::Stale => {
                    let s = self
                        .sessions
                        .get(client)
                        .expect("verdict implies a session");
                    ops.push(Op::Put(
                        session::key_for(client),
                        session::encode_record(client, &s),
                    ));
                }
            },
            plain => ops.push(Self::plain_op(plain)),
        }
        Ok(())
    }

    fn reload_sessions(&mut self) {
        if let Ok(t) = Self::load_sessions(&self.db) {
            self.sessions = t;
        }
    }

    pub fn session_last_seq(&self, client: u64) -> Option<u64> {
        self.sessions.last_seq(client)
    }

    pub fn sessions(&self) -> &SessionTable {
        &self.sessions
    }
}

fn utf8(b: Vec<u8>) -> Result<String, String> {
    String::from_utf8(b).map_err(|_| "stored value is not valid UTF-8".to_string())
}

/// Key/value byte pairs, as carried by a baseline snapshot.
type KvPairs = Vec<(Vec<u8>, Vec<u8>)>;

/// Parse a snapshot (v1 body, optionally wrapped with sessions as v2) into
/// sorted byte pairs and the session table.
fn parse_snapshot(data: &[u8]) -> Result<(KvPairs, SessionTable), String> {
    let (body, sessions) = session::split_snapshot(data)?;
    Ok((parse_body(body)?, sessions))
}

/// Parse the baseline (v1) snapshot format into sorted byte pairs.
fn parse_body(data: &[u8]) -> Result<KvPairs, String> {
    fn take<'a>(d: &mut &'a [u8]) -> Option<&'a [u8]> {
        if d.len() < 4 {
            return None;
        }
        let n = u32::from_le_bytes(d[..4].try_into().ok()?) as usize;
        if d.len() < 4 + n {
            return None;
        }
        let out = &d[4..4 + n];
        *d = &d[4 + n..];
        Some(out)
    }
    if data.len() < 4 {
        return Err("snapshot too short to contain a length header".to_string());
    }
    let count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    if count > data.len() / 8 {
        return Err("snapshot entry count exceeds what its bytes can hold".to_string());
    }
    let mut rest = &data[4..];
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        let k = take(&mut rest).ok_or("truncated snapshot: missing key")?;
        let v = take(&mut rest).ok_or("truncated snapshot: missing value")?;
        std::str::from_utf8(k).map_err(|_| "snapshot key is not valid UTF-8")?;
        std::str::from_utf8(v).map_err(|_| "snapshot value is not valid UTF-8")?;
        out.push((k.to_vec(), v.to_vec()));
    }
    if !rest.is_empty() {
        return Err("trailing garbage after snapshot entries".to_string());
    }
    // The engine needs strictly ascending keys; the baseline format sorts,
    // and duplicates would make the snapshot ambiguous.
    out.sort();
    if out.windows(2).any(|w| w[0].0 == w[1].0) {
        return Err("snapshot contains duplicate keys".to_string());
    }
    Ok(out)
}

impl StateMachine for LsmStore {
    fn apply(&mut self, index: LogIndex, data: &[u8]) -> Result<(), String> {
        self.apply_batch(&[(index, data)])
    }

    fn apply_batch(&mut self, entries: &[(LogIndex, &[u8])]) -> Result<(), String> {
        let have = self.db.applied_index();
        let mut ops = Vec::with_capacity(entries.len());
        let mut last = 0;
        for (index, data) in entries {
            if *index <= have {
                continue; // already reflected (replay after restart)
            }
            if let Err(e) = self.plan(*index, data, &mut ops) {
                self.reload_sessions();
                return Err(e);
            }
            last = *index;
        }
        if ops.is_empty() {
            return Ok(());
        }
        // One WAL record: all the ops (data AND session records) and the new
        // applied index, atomically.
        if let Err(e) = self.db.apply(&ops, last) {
            // The in-memory table ran ahead of the engine; put it back.
            self.reload_sessions();
            return Err(map_err(e));
        }
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<String>, String> {
        match self.db.get(key.as_bytes()).map_err(map_err)? {
            Some(v) => utf8(v).map(Some),
            None => Ok(None),
        }
    }

    fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        let rows = self.db.dump().map_err(map_err)?;
        let user: Vec<_> = rows
            .iter()
            .filter(|(k, _)| !session::is_session_key(k))
            .collect();
        let mut out = Vec::new();
        out.extend_from_slice(&(user.len() as u32).to_le_bytes());
        for (k, v) in user {
            out.extend_from_slice(&(k.len() as u32).to_le_bytes());
            out.extend_from_slice(k);
            out.extend_from_slice(&(v.len() as u32).to_le_bytes());
            out.extend_from_slice(v);
        }
        Ok(session::wrap_snapshot(out, &self.sessions))
    }

    fn restore_from_snapshot(&mut self, bytes: &[u8], index: LogIndex) -> Result<(), String> {
        let (mut rows, sessions) = parse_snapshot(bytes)?;
        for (c, s) in sessions.sorted() {
            rows.push((session::key_for(c), session::encode_record(c, &s)));
        }
        // Session keys start with 0xFF, above every UTF-8 user key.
        rows.sort();
        self.db.replace_all(rows, index).map_err(map_err)?;
        self.sessions = sessions;
        Ok(())
    }

    fn session_last_seq(&self, client: u64) -> Option<u64> {
        self.sessions.last_seq(client)
    }

    fn applied_index(&self) -> LogIndex {
        self.db.applied_index()
    }

    fn durability_barrier(&mut self) -> Result<(), String> {
        self.db.sync().map_err(map_err)
    }
}
