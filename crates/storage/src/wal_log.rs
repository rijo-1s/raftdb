//! `WalLog`: the disk-backed `raft_core::LogStore` implementation. This is
//! what turns a `RaftNode` from "resets to nothing on restart" into
//! "survives a crash and resumes exactly where it left off" - including
//! after log compaction.
//!
//! Three files on disk, all under `<path>`:
//! - `<path>` - the log entries, via `Wal` (see `wal.rs`).
//! - `<path>.hardstate` - self-validating persistent `currentTerm` + `votedFor`.
//! - `<path>.snapshot` - the compaction boundary plus the state-machine
//!   snapshot bytes and cluster membership, in a versioned record whose
//!   CRC covers every byte.
//!
//! # Crash-safety rules
//!
//! * **Every multi-step or whole-file update is atomic-replace** (temp file,
//!   fsync, rename, directory fsync - see `durable.rs`): the HardState, the
//!   snapshot, and any WAL rewrite. A crash can never leave a half-written
//!   version under the real name.
//! * **Disk before memory.** Each mutation reaches stable storage before the
//!   in-memory view changes, and no caller is told "done" earlier.
//! * **Compaction order**: the new snapshot is published *before* the WAL is
//!   rewritten. A crash in between leaves a WAL that still contains entries
//!   the snapshot covers; `open()` drops those. It also checks that the
//!   WAL's entry at the snapshot boundary carries the snapshot's term, and
//!   discards the old log if it does not (a leftover from an interrupted
//!   `install_snapshot` of a conflicting log).
//! * **Nothing is silently treated as empty.** A corrupt or unreadable
//!   snapshot, a corrupt hard state, mid-log WAL corruption, a gap between
//!   the snapshot and the log, or files that contradict each other make
//!   `open()` fail with an explicit [`WalError`] and leave the files
//!   untouched for inspection or repair.
//! * **Any storage error is fatal** to the running node (the `LogStore`
//!   trait has no error channel): methods panic rather than continue with
//!   state that might not be durable. Restarting runs recovery.

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use raft_core::{EntryKind, LogEntry, LogIndex, LogStore, NodeId, Term};

use crate::durable::{self, crc32, HARD_STATE_POINTS, SNAPSHOT_POINTS};
use crate::wal::{Wal, WalError, WalScan};

pub struct WalLog {
    wal: Wal,
    entries: Vec<LogEntry>,
    hard_state_path: PathBuf,
    hard_term: Term,
    hard_voted_for: Option<NodeId>,
    /// True once a hard state record is known to be on disk.
    hard_state_on_disk: bool,
    snapshot_path: PathBuf,
    last_included_index: LogIndex,
    last_included_term: Term,
    snapshot: Option<Vec<u8>>,
    snapshot_peers: Vec<NodeId>,
}

/// A decoded snapshot file.
struct SnapshotRecord {
    last_included_index: LogIndex,
    last_included_term: Term,
    data: Vec<u8>,
    peers: Vec<NodeId>,
}

impl WalLog {
    /// Open (creating if needed) the WAL at `path`, replay it, and load
    /// whatever hard state and snapshot were last persisted alongside it.
    /// For a brand-new node, all three come back empty/zeroed.
    ///
    /// Fails (without modifying anything but a torn WAL tail) if the
    /// persisted state is corrupt or self-contradictory; see the module docs.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let path = path.as_ref();
        let hard_state_path = sibling_path(path, "hardstate");
        let snapshot_path = sibling_path(path, "snapshot");

        // ---- Phase 1: read-only. Nothing is created, truncated, rewritten
        // or deleted until the WAL, hard state and snapshot have all been
        // read and cross-checked, so a refusal leaves every file as found.
        let wal_present = match std::fs::metadata(path) {
            Ok(_) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => return Err(WalError::Io(e)),
        };
        let scan = if wal_present {
            Wal::scan(path)?
        } else {
            WalScan {
                entries: Vec::new(),
                torn: None,
            }
        };
        let hard_state = read_hard_state(&hard_state_path)?;
        let snap = read_snapshot(&snapshot_path)?;

        // The first open of a node creates the WAL (and fsyncs the
        // directory) before any hard state, snapshot or rewrite can exist.
        // So a missing WAL next to any of them means the file was removed,
        // not that the node is new; an empty replacement would silently
        // discard log entries that may have been committed.
        if !wal_present {
            let why = if hard_state.is_some() {
                "a hard state file exists"
            } else if snap.is_some() {
                "a snapshot file exists"
            } else if durable::tmp_path(path).exists() {
                "a WAL rewrite temp file exists"
            } else {
                ""
            };
            if !why.is_empty() {
                return Err(WalError::LogMissing(why));
            }
        }

        let raw_entries = scan.entries;
        let (last_included_index, last_included_term, snapshot, snapshot_peers) = match snap {
            Some(s) => (
                s.last_included_index,
                s.last_included_term,
                Some(s.data),
                s.peers,
            ),
            None => (0, 0, None, Vec::new()),
        };

        let hard_state_on_disk = hard_state.is_some();
        let (hard_term, hard_voted_for) = match hard_state {
            Some(state) => state,
            None if raw_entries.is_empty() && last_included_index == 0 => (0, None),
            None => return Err(WalError::HardStateMissing),
        };

        // A previous process may have died after publishing a snapshot but
        // before rewriting the WAL. If the old log's entry at the snapshot
        // boundary disagrees with the snapshot's term, that log conflicts
        // with the snapshot and none of it may be kept (§7).
        let raw_len = raw_entries.len();
        let conflicts_with_snapshot = last_included_index > 0
            && raw_entries
                .iter()
                .find(|e| e.index == last_included_index)
                .is_some_and(|e| e.term != last_included_term);
        let entries: Vec<LogEntry> = if conflicts_with_snapshot {
            Vec::new()
        } else {
            raw_entries
                .into_iter()
                .filter(|e| e.index > last_included_index)
                .collect()
        };

        if let Some(first) = entries.first() {
            if first.index != last_included_index + 1 {
                return Err(WalError::LogInconsistent(
                    "first WAL entry does not follow the snapshot boundary \
                     (missing or stale snapshot file?)",
                ));
            }
        }
        let newest_log_term = entries
            .last()
            .map(|e| e.term)
            .unwrap_or(0)
            .max(last_included_term);
        if newest_log_term > hard_term {
            return Err(WalError::StateInconsistent(
                "log or snapshot contains a term newer than the persisted current term \
                 (hard state file rolled back or replaced?)",
            ));
        }

        // ---- Phase 2: the persisted state is accepted, so recovery may now
        // modify files. Every step is idempotent: a crash (or an I/O error)
        // part-way just repeats the remainder on the next open.
        //
        // `Wal::open` removes the WAL's stale temp file and creates the file
        // only for a genuinely fresh node (a missing WAL beside any other
        // persisted state was refused above).
        let mut wal = Wal::open(path)?;
        durable::remove_stale_tmp(&hard_state_path)?;
        durable::remove_stale_tmp(&snapshot_path)?;
        if let Some(torn) = &scan.torn {
            wal.truncate_torn(torn)?;
        }
        // Finish an interrupted compaction / snapshot install: the file
        // still holds entries the snapshot already covers (or a conflicting
        // old log). They are ignored in memory, but if left on disk the next
        // append would land *after* them and produce a non-contiguous WAL
        // that the following restart would reject. Make the file match.
        if entries.len() != raw_len {
            wal.rewrite(&entries)?;
        }

        Ok(Self {
            wal,
            entries,
            hard_state_path,
            hard_term,
            hard_voted_for,
            hard_state_on_disk,
            snapshot_path,
            last_included_index,
            last_included_term,
            snapshot,
            snapshot_peers,
        })
    }

    fn local_index(&self, index: LogIndex) -> Option<usize> {
        if index <= self.last_included_index {
            return None;
        }
        Some((index - self.last_included_index - 1) as usize)
    }
}

impl LogStore for WalLog {
    fn last_index(&self) -> LogIndex {
        self.last_included_index + self.entries.len() as LogIndex
    }

    fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.last_included_term)
    }

    fn term_at(&self, index: LogIndex) -> Term {
        if index == 0 {
            return 0;
        }
        if index == self.last_included_index {
            return self.last_included_term;
        }
        match self.local_index(index) {
            Some(local) => self.entries.get(local).map(|e| e.term).unwrap_or(0),
            None => 0,
        }
    }

    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.local_index(index)
            .and_then(|local| self.entries.get(local))
    }

    fn append(&mut self, term: Term, data: Vec<u8>) -> LogIndex {
        let index = self.last_index() + 1;
        let entry = LogEntry {
            term,
            index,
            kind: EntryKind::Data,
            data,
        };
        // Durable first, visible second.
        self.wal
            .append(&entry)
            .expect("WAL append failed: treating as fatal");
        self.entries.push(entry);
        index
    }

    fn append_batch(&mut self, term: Term, data: Vec<Vec<u8>>) -> LogIndex {
        let first = self.last_index() + 1;
        let batch: Vec<LogEntry> = data
            .into_iter()
            .enumerate()
            .map(|(i, data)| LogEntry {
                term,
                index: first + i as LogIndex,
                kind: EntryKind::Data,
                data,
            })
            .collect();
        // One write + one fsync for the whole batch (group commit).
        self.wal
            .append_batch(&batch)
            .expect("WAL batch append failed: treating as fatal");
        self.entries.extend(batch);
        first
    }

    fn append_config_entry(&mut self, term: Term, kind: EntryKind) -> LogIndex {
        let index = self.last_index() + 1;
        let entry = LogEntry {
            term,
            index,
            kind,
            data: Vec::new(),
        };
        self.wal
            .append(&entry)
            .expect("WAL append failed: treating as fatal");
        self.entries.push(entry);
        index
    }

    fn entries_from(&self, from_index: LogIndex) -> Vec<LogEntry> {
        if from_index > self.last_index() {
            return Vec::new();
        }
        match self.local_index(from_index) {
            Some(local) => self.entries[local..].to_vec(),
            None => Vec::new(), // at/below the snapshot boundary: caller's bug
        }
    }

    fn append_entries(&mut self, _prev_index: LogIndex, entries: &[LogEntry]) {
        // Raft §5.3 step 3: only a *conflicting* entry (same index,
        // different term) triggers truncation. Entries we already have are
        // left alone - a delayed or duplicated AppendEntries must never
        // erase a suffix we may already have acknowledged.
        let mut first_new = entries.len();
        let mut conflict = false;
        for (i, e) in entries.iter().enumerate() {
            if e.index <= self.last_included_index {
                continue; // covered by our snapshot
            }
            if e.index <= self.last_index() {
                if self.term_at(e.index) == e.term {
                    continue; // already present
                }
                conflict = true;
            }
            first_new = i;
            break;
        }
        let new_entries = &entries[first_new..];
        if new_entries.is_empty() {
            return;
        }
        if conflict {
            // Truncate + append is one atomic WAL replacement: a crash
            // leaves either the old log or the complete new one.
            let keep = (new_entries[0].index - self.last_included_index - 1) as usize;
            let mut next: Vec<LogEntry> = self.entries[..keep].to_vec();
            next.extend_from_slice(new_entries);
            self.wal
                .rewrite(&next)
                .expect("WAL rewrite failed: treating as fatal");
            self.entries = next;
        } else {
            debug_assert_eq!(new_entries[0].index, self.last_index() + 1);
            self.wal
                .append_batch(new_entries)
                .expect("WAL append failed: treating as fatal");
            self.entries.extend_from_slice(new_entries);
        }
    }

    fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        assert!(
            term >= self.hard_term,
            "current term must never decrease ({} -> {term})",
            self.hard_term
        );
        if self.hard_state_on_disk && (term, voted_for) == (self.hard_term, self.hard_voted_for) {
            return; // already durable: skip a pointless fsync
        }
        write_hard_state(&self.hard_state_path, term, voted_for)
            .expect("hard state write failed: treating as fatal, since a lost vote/term is a safety hazard, not just a performance one");
        self.hard_term = term;
        self.hard_voted_for = voted_for;
        self.hard_state_on_disk = true;
    }

    fn load_hard_state(&self) -> (Term, Option<NodeId>) {
        (self.hard_term, self.hard_voted_for)
    }

    fn last_included_index(&self) -> LogIndex {
        self.last_included_index
    }

    fn last_included_term(&self) -> Term {
        self.last_included_term
    }

    fn snapshot_data(&self) -> Option<&[u8]> {
        self.snapshot.as_deref()
    }

    fn snapshot_peers(&self) -> &[NodeId] {
        &self.snapshot_peers
    }

    fn compact(
        &mut self,
        up_to_index: LogIndex,
        up_to_term: Term,
        snapshot_data: Vec<u8>,
        snapshot_peers: Vec<NodeId>,
    ) {
        if up_to_index <= self.last_included_index {
            return;
        }
        // 1) Publish the snapshot atomically - see module doc on ordering.
        write_snapshot(
            &self.snapshot_path,
            up_to_index,
            up_to_term,
            &snapshot_data,
            &snapshot_peers,
        )
        .expect("snapshot write failed: treating as fatal");
        // 2) Now shrink the WAL (atomically) to just what is left.
        let remaining: Vec<LogEntry> = self
            .entries
            .iter()
            .filter(|e| e.index > up_to_index)
            .cloned()
            .collect();
        self.wal
            .rewrite(&remaining)
            .expect("WAL rewrite failed: treating as fatal");
        self.last_included_index = up_to_index;
        self.last_included_term = up_to_term;
        self.snapshot = Some(snapshot_data);
        self.snapshot_peers = snapshot_peers;
        self.entries = remaining;
    }

    fn install_snapshot(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        peers: Vec<NodeId>,
    ) {
        if last_included_index <= self.last_included_index {
            return;
        }
        // §7: if we already hold the snapshot's last entry (same index and
        // term), everything after it is still valid and may already have
        // been acknowledged to the leader - keep it. Otherwise our log
        // conflicts with the snapshot and is discarded.
        let retain = last_included_index <= self.last_index()
            && self.term_at(last_included_index) == last_included_term;
        let remaining: Vec<LogEntry> = if retain {
            self.entries
                .iter()
                .filter(|e| e.index > last_included_index)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        write_snapshot(
            &self.snapshot_path,
            last_included_index,
            last_included_term,
            &data,
            &peers,
        )
        .expect("snapshot write failed: treating as fatal");
        self.wal
            .rewrite(&remaining)
            .expect("WAL rewrite failed: treating as fatal");
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
        self.snapshot = Some(data);
        self.snapshot_peers = peers;
        self.entries = remaining;
    }
}

fn sibling_path(base: &Path, suffix: &str) -> PathBuf {
    let mut p = base.as_os_str().to_owned();
    p.push(".");
    p.push(suffix);
    PathBuf::from(p)
}

const HARD_STATE_MAGIC: [u8; 8] = *b"RDBHST01";
const HARD_STATE_VERSION: u16 = 1;
const HARD_STATE_PAYLOAD_LEN: usize = 16;
const HARD_STATE_HEADER_LEN: usize = 8 + 2 + 4;
const HARD_STATE_CHECKSUM_LEN: usize = 4;
const HARD_STATE_RECORD_LEN: usize =
    HARD_STATE_HEADER_LEN + HARD_STATE_PAYLOAD_LEN + HARD_STATE_CHECKSUM_LEN;
const HARD_STATE_MAX_SIZE: usize = HARD_STATE_RECORD_LEN;

#[cfg(test)]
fn hard_state_tmp_path(path: &Path) -> PathBuf {
    durable::tmp_path(path)
}

fn read_hard_state(path: &Path) -> Result<Option<(Term, Option<NodeId>)>, WalError> {
    let mut f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(WalError::Io(e)),
    };

    let file_len = f.metadata()?.len();
    if file_len == 16 {
        return Err(WalError::HardStateUnsupportedLegacyFormat);
    }
    if file_len > HARD_STATE_MAX_SIZE as u64 {
        return Err(WalError::HardStateCorrupt("hard state file is too large"));
    }
    if file_len as usize != HARD_STATE_RECORD_LEN {
        return Err(WalError::HardStateCorrupt("hard state record is truncated"));
    }

    let mut record = [0u8; HARD_STATE_RECORD_LEN];
    f.read_exact(&mut record)?;

    if record[0..8] != HARD_STATE_MAGIC {
        return Err(WalError::HardStateCorrupt("invalid hard state magic"));
    }

    let version = u16::from_le_bytes(record[8..10].try_into().unwrap());
    if version != HARD_STATE_VERSION {
        return Err(WalError::HardStateUnsupportedVersion(version));
    }

    let payload_len = u32::from_le_bytes(record[10..14].try_into().unwrap()) as usize;
    if payload_len == 0 || payload_len > HARD_STATE_PAYLOAD_LEN {
        return Err(WalError::HardStateCorrupt(
            "invalid hard state payload length",
        ));
    }
    if payload_len != HARD_STATE_PAYLOAD_LEN {
        return Err(WalError::HardStateCorrupt(
            "unexpected hard state payload length",
        ));
    }

    let checksum_start = HARD_STATE_HEADER_LEN + HARD_STATE_PAYLOAD_LEN;
    let expected_crc = u32::from_le_bytes(
        record[checksum_start..checksum_start + HARD_STATE_CHECKSUM_LEN]
            .try_into()
            .unwrap(),
    );
    if crc32(&record[..HARD_STATE_HEADER_LEN + HARD_STATE_PAYLOAD_LEN]) != expected_crc {
        return Err(WalError::HardStateCorrupt("hard state checksum mismatch"));
    }

    let payload_start = HARD_STATE_HEADER_LEN;
    let term = u64::from_le_bytes(record[payload_start..payload_start + 8].try_into().unwrap());
    let voted_raw = u64::from_le_bytes(
        record[payload_start + 8..payload_start + HARD_STATE_PAYLOAD_LEN]
            .try_into()
            .unwrap(),
    );
    let voted_for = if voted_raw == 0 {
        None
    } else if voted_raw == u64::MAX {
        return Err(WalError::HardStateCorrupt("invalid voted_for encoding"));
    } else {
        Some(voted_raw - 1)
    };

    Ok(Some((term, voted_for)))
}

fn write_hard_state(path: &Path, term: Term, voted_for: Option<NodeId>) -> Result<(), WalError> {
    let voted_raw = match voted_for {
        None => 0,
        Some(id) => id.checked_add(1).ok_or(WalError::HardStateSerialization(
            "voted_for cannot be represented in the hard state format",
        ))?,
    };
    let mut record = [0u8; HARD_STATE_RECORD_LEN];
    record[0..8].copy_from_slice(&HARD_STATE_MAGIC);
    record[8..10].copy_from_slice(&HARD_STATE_VERSION.to_le_bytes());
    record[10..14].copy_from_slice(&(HARD_STATE_PAYLOAD_LEN as u32).to_le_bytes());
    record[14..22].copy_from_slice(&term.to_le_bytes());
    record[22..30].copy_from_slice(&voted_raw.to_le_bytes());
    let checksum = crc32(&record[..HARD_STATE_HEADER_LEN + HARD_STATE_PAYLOAD_LEN]);
    record[30..34].copy_from_slice(&checksum.to_le_bytes());

    // The live file is untouched until the atomic rename inside
    // `atomic_write`; if anything before it fails, the previous valid
    // HardState remains recoverable from `path`.
    durable::atomic_write(path, &record, HARD_STATE_POINTS)?;
    Ok(())
}

// ---------------------------------------------------------------------
// Snapshot file, format v2
//
//   magic "RDBSNP02" (8) | version u16 | last_included_index u64
//   | last_included_term u64 | data_len u64 | peer_count u32
//   | data (data_len bytes) | peers (peer_count * u64) | crc32 u32
//
// The CRC covers *every* preceding byte, including the header fields - the
// previous format did not protect the index/term/length fields at all. The
// total file length is fully determined by the header, so truncation and
// trailing garbage are both detected.
// ---------------------------------------------------------------------

const SNAPSHOT_MAGIC: [u8; 8] = *b"RDBSNP02";
const SNAPSHOT_VERSION: u16 = 2;
const SNAPSHOT_HEADER_LEN: usize = 8 + 2 + 8 + 8 + 8 + 4;
const SNAPSHOT_CRC_LEN: usize = 4;

fn read_snapshot(path: &Path) -> Result<Option<SnapshotRecord>, WalError> {
    let bytes = match std::fs::read(path) {
        Ok(b) => b,
        // Only "no such file" means "no snapshot yet". Anything else
        // (permissions, I/O error) must not masquerade as an empty state.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(WalError::Io(e)),
    };
    if bytes.len() < SNAPSHOT_HEADER_LEN + SNAPSHOT_CRC_LEN {
        return Err(WalError::SnapshotCorrupt("snapshot file is truncated"));
    }
    if bytes[0..8] != SNAPSHOT_MAGIC {
        return Err(WalError::SnapshotCorrupt(
            "invalid snapshot magic (corrupt, or a pre-v2 snapshot file)",
        ));
    }
    let version = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    if version != SNAPSHOT_VERSION {
        return Err(WalError::SnapshotUnsupportedVersion(version));
    }
    let last_included_index = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    let last_included_term = u64::from_le_bytes(bytes[18..26].try_into().unwrap());
    let data_len = u64::from_le_bytes(bytes[26..34].try_into().unwrap());
    let peer_count = u32::from_le_bytes(bytes[34..38].try_into().unwrap()) as u64;

    // Checked arithmetic: the lengths come from an untrusted file.
    let expected_len = (SNAPSHOT_HEADER_LEN as u64)
        .checked_add(data_len)
        .and_then(|n| n.checked_add(peer_count.checked_mul(8)?))
        .and_then(|n| n.checked_add(SNAPSHOT_CRC_LEN as u64));
    if expected_len != Some(bytes.len() as u64) {
        return Err(WalError::SnapshotCorrupt(
            "snapshot length does not match its header",
        ));
    }
    let crc_at = bytes.len() - SNAPSHOT_CRC_LEN;
    let expected_crc = u32::from_le_bytes(bytes[crc_at..].try_into().unwrap());
    if crc32(&bytes[..crc_at]) != expected_crc {
        return Err(WalError::SnapshotCorrupt("snapshot checksum mismatch"));
    }
    if last_included_index == 0 {
        return Err(WalError::SnapshotCorrupt("snapshot has index 0"));
    }

    let data_end = SNAPSHOT_HEADER_LEN + data_len as usize;
    let data = bytes[SNAPSHOT_HEADER_LEN..data_end].to_vec();
    let peers: Vec<NodeId> = bytes[data_end..crc_at]
        .as_chunks::<8>()
        .0
        .iter()
        .map(|c| u64::from_le_bytes(*c))
        .collect();

    Ok(Some(SnapshotRecord {
        last_included_index,
        last_included_term,
        data,
        peers,
    }))
}

fn write_snapshot(
    path: &Path,
    index: LogIndex,
    term: Term,
    data: &[u8],
    peers: &[NodeId],
) -> Result<(), WalError> {
    let mut buf = Vec::with_capacity(SNAPSHOT_HEADER_LEN + data.len() + peers.len() * 8 + 4);
    buf.extend_from_slice(&SNAPSHOT_MAGIC);
    buf.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(&term.to_le_bytes());
    buf.extend_from_slice(&(data.len() as u64).to_le_bytes());
    buf.extend_from_slice(&(peers.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
    for &p in peers {
        buf.extend_from_slice(&p.to_le_bytes());
    }
    let crc = crc32(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    // Atomic replace: the previous snapshot stays valid until the rename.
    durable::atomic_write(path, &buf, SNAPSHOT_POINTS)?;
    Ok(())
}

#[cfg(test)]
mod hard_state_tests {
    use super::*;
    use raft_core::LogStore;
    use std::fs;
    use tempfile::tempdir;

    fn wal_path(dir: &Path) -> PathBuf {
        dir.join("node.wal")
    }

    fn hardstate_path(dir: &Path) -> PathBuf {
        sibling_path(&wal_path(dir), "hardstate")
    }

    fn write_state(dir: &Path, term: Term, voted_for: Option<NodeId>) {
        let mut log = WalLog::open(wal_path(dir)).unwrap();
        log.save_hard_state(term, voted_for);
    }

    fn read_state(dir: &Path) -> Result<(Term, Option<NodeId>), WalError> {
        let log = WalLog::open(wal_path(dir))?;
        Ok(log.load_hard_state())
    }

    #[test]
    fn hard_state_round_trips_exactly() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 42, Some(7));
        assert_eq!(read_state(dir.path()).unwrap(), (42, Some(7)));
    }

    #[test]
    fn hard_state_multiple_updates_survive_each_restart() {
        let dir = tempdir().unwrap();
        for state in [(1, Some(1)), (2, Some(2)), (3, None)] {
            write_state(dir.path(), state.0, state.1);
            assert_eq!(read_state(dir.path()).unwrap(), state);
        }
    }

    #[test]
    fn missing_hard_state_is_only_the_brand_new_state() {
        let dir = tempdir().unwrap();
        assert_eq!(read_state(dir.path()).unwrap(), (0, None));
    }

    #[test]
    fn missing_hard_state_is_rejected_when_persisted_raft_data_exists() {
        let dir = tempdir().unwrap();
        let wal = wal_path(dir.path());
        {
            let mut log = WalLog::open(wal).unwrap();
            // A real node always persists its term before it accepts log
            // entries; `append` alone never creates the hard state file.
            log.save_hard_state(1, None);
            log.append(1, b"data".to_vec());
        }
        fs::remove_file(hardstate_path(dir.path())).unwrap();

        assert!(matches!(
            read_state(dir.path()),
            Err(WalError::HardStateMissing)
        ));
    }

    #[test]
    fn legacy_16_byte_hard_state_is_rejected() {
        let dir = tempdir().unwrap();
        let path = hardstate_path(dir.path());
        let mut legacy = [0u8; 16];
        legacy[0..8].copy_from_slice(&7u64.to_le_bytes());
        legacy[8..16].copy_from_slice(&3u64.to_le_bytes());
        fs::write(path, legacy).unwrap();

        assert!(matches!(
            read_state(dir.path()),
            Err(WalError::HardStateUnsupportedLegacyFormat)
        ));
    }

    #[test]
    fn truncated_hard_state_is_rejected_at_every_required_boundary() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 42, Some(7));
        let path = hardstate_path(dir.path());
        let valid = fs::read(&path).unwrap();
        assert_eq!(valid.len(), HARD_STATE_RECORD_LEN);

        for len in [0, 1, 17, 29, 30, 31, 33] {
            fs::write(&path, &valid[..len]).unwrap();
            assert!(
                matches!(read_state(dir.path()), Err(WalError::HardStateCorrupt(_))),
                "truncation at {len} bytes was accepted"
            );
        }
    }

    #[test]
    fn corruption_in_magic_version_length_payload_and_checksum_is_rejected() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 42, Some(7));
        let path = hardstate_path(dir.path());
        let valid = fs::read(&path).unwrap();

        for offset in [0, 8, 10, 14, 22, 30] {
            let mut corrupted = valid.clone();
            corrupted[offset] ^= 0x01;
            fs::write(&path, corrupted).unwrap();
            assert!(
                matches!(
                    read_state(dir.path()),
                    Err(WalError::HardStateCorrupt(_))
                        | Err(WalError::HardStateUnsupportedVersion(_))
                ),
                "corruption at byte {offset} was accepted"
            );
        }
    }

    #[test]
    fn invalid_lengths_are_rejected_without_allocating_from_the_length() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 42, Some(7));
        let path = hardstate_path(dir.path());
        let valid = fs::read(&path).unwrap();

        for length in [0u32, 17, u32::MAX] {
            let mut record = valid.clone();
            record[10..14].copy_from_slice(&length.to_le_bytes());
            fs::write(&path, record).unwrap();
            assert!(
                matches!(read_state(dir.path()), Err(WalError::HardStateCorrupt(_))),
                "invalid payload length {length} was accepted"
            );
        }
    }

    #[test]
    fn invalid_voted_for_encoding_is_rejected_after_checksum_validation() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 42, Some(7));
        let path = hardstate_path(dir.path());
        let mut record = fs::read(&path).unwrap();
        record[22..30].copy_from_slice(&u64::MAX.to_le_bytes());
        let checksum = crc32(&record[..HARD_STATE_HEADER_LEN + HARD_STATE_PAYLOAD_LEN]);
        record[30..34].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&path, record).unwrap();

        assert!(matches!(
            read_state(dir.path()),
            Err(WalError::HardStateCorrupt("invalid voted_for encoding"))
        ));
    }

    #[test]
    fn unexpected_extra_bytes_are_rejected() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 42, Some(7));
        let path = hardstate_path(dir.path());
        let mut record = fs::read(&path).unwrap();
        record.push(0xAA);
        fs::write(&path, record).unwrap();

        assert!(matches!(
            read_state(dir.path()),
            Err(WalError::HardStateCorrupt(_))
        ));
    }

    #[test]
    fn failed_temporary_write_leaves_published_state_recoverable() {
        let dir = tempdir().unwrap();
        write_state(dir.path(), 10, Some(1));

        // Use a real filesystem failure rather than a mocked persistence
        // function: a directory at the temporary-file path prevents the
        // replacement record from being written. The live HardState is never
        // touched before atomic rename, so the old state remains recoverable.
        let tmp_path = hard_state_tmp_path(&hardstate_path(dir.path()));
        fs::create_dir(tmp_path).unwrap();

        let result = write_hard_state(&hardstate_path(dir.path()), 11, Some(2));
        assert!(matches!(result, Err(WalError::Io(_))));
        assert_eq!(read_state(dir.path()).unwrap(), (10, Some(1)));
    }
}
