//! A minimal write-ahead log.
//!
//! On-disk frame format (little-endian), one per log entry:
//!
//! ```text
//! [ len: u32 ][ crc32: u32 ][ term: u64 ][ index: u64 ][ kind_tag: u8 ][ kind_node_id: u64 ][ data_len: u32 ][ data: data_len bytes ]
//!             \_________________________________________ len bytes _________________________________________/
//! ```
//!
//! `kind_tag` is 0 (Data), 1 (AddNode), 2 (RemoveNode) or 3 (NoOp) - see
//! `raft_core::EntryKind`. `kind_node_id` only means anything for tags 1/2.
//! `len` covers everything after the crc32 field; `crc32` covers that same
//! span, so a torn or corrupted frame is detected on replay.
//!
//! # Durability guarantees
//!
//! * `append` / `append_batch` return `Ok` only after `fsync`. A batch of
//!   N entries costs **one** `write` and **one** `fsync` (group commit).
//! * `rewrite` (used for log truncation and compaction) is **crash-atomic**:
//!   the replacement is written to `<path>.tmp`, fsynced, renamed over the
//!   live file and the directory fsynced. A crash at any point leaves
//!   either the complete old log or the complete new log - never a
//!   partial or empty one.
//! * After any failed I/O the `Wal` is *poisoned*: every later call fails.
//!   After a failed `fsync` the kernel may have dropped the dirty pages, so
//!   retrying would silently lose data; the only safe response is to
//!   restart and re-run recovery.
//!
//! # Recovery
//!
//! `recover` distinguishes a **torn tail** from **mid-log corruption**:
//!
//! * A bad frame that is incomplete, or is the *last* frame in the file, or
//!   is followed only by zero bytes, is a torn tail from a crash during an
//!   append. It is truncated (and the truncation fsynced); the discarded
//!   bytes are first saved to `<path>.discarded-tail`.
//! * A bad frame that is followed by more data cannot be explained by a
//!   crash during an append. Recovery returns [`WalError::LogCorrupt`] and
//!   **does not modify the file**, instead of silently dropping everything
//!   after it (which may include committed entries).

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use raft_core::{EntryKind, LogEntry};

use crate::durable::{self, crc32, WAL_REWRITE_POINTS};
use crate::fault::FaultPoint;

const FRAME_HEADER_LEN: usize = 8;
const MIN_PAYLOAD_LEN: usize = 29;

fn encode_payload(entry: &LogEntry) -> Vec<u8> {
    let (kind_tag, kind_node_id): (u8, u64) = match entry.kind {
        EntryKind::Data => (0, 0),
        EntryKind::AddNode(id) => (1, id),
        EntryKind::RemoveNode(id) => (2, id),
        EntryKind::NoOp => (3, 0),
    };
    let mut payload = Vec::with_capacity(MIN_PAYLOAD_LEN + entry.data.len());
    payload.extend_from_slice(&entry.term.to_le_bytes());
    payload.extend_from_slice(&entry.index.to_le_bytes());
    payload.push(kind_tag);
    payload.extend_from_slice(&kind_node_id.to_le_bytes());
    payload.extend_from_slice(&(entry.data.len() as u32).to_le_bytes());
    payload.extend_from_slice(&entry.data);
    payload
}

fn encode_frame_into(out: &mut Vec<u8>, entry: &LogEntry) {
    let payload = encode_payload(entry);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(&payload).to_le_bytes());
    out.extend_from_slice(&payload);
}

fn decode_payload(payload: &[u8]) -> Option<LogEntry> {
    if payload.len() < MIN_PAYLOAD_LEN {
        return None;
    }
    let term = u64::from_le_bytes(payload[0..8].try_into().ok()?);
    let index = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let kind_tag = payload[16];
    let kind_node_id = u64::from_le_bytes(payload[17..25].try_into().ok()?);
    let kind = match kind_tag {
        0 => EntryKind::Data,
        1 => EntryKind::AddNode(kind_node_id),
        2 => EntryKind::RemoveNode(kind_node_id),
        3 => EntryKind::NoOp,
        _ => return None,
    };
    let data_len = u32::from_le_bytes(payload[25..29].try_into().ok()?) as usize;
    if MIN_PAYLOAD_LEN.checked_add(data_len)? != payload.len() {
        return None; // wrong length or trailing garbage: treat as corrupt
    }
    Some(LogEntry {
        term,
        index,
        kind,
        data: payload[MIN_PAYLOAD_LEN..].to_vec(),
    })
}

/// A torn tail found by [`Wal::scan`]: where the good data ends, and the
/// bytes that would be discarded.
pub(crate) struct TornTail {
    pub offset: u64,
    pub discarded: Vec<u8>,
}

/// The result of reading a WAL without modifying it.
pub(crate) struct WalScan {
    pub entries: Vec<LogEntry>,
    pub torn: Option<TornTail>,
}

// Process-wide fsync statistics.
//
// Pure measurement: nothing here changes what is written or when it is
// fsynced. Plain atomics, so the cost on the append path is a handful of
// relaxed adds. The counters are process-wide (every `Wal` in the process
// feeds them); a node has one Raft WAL, so in production this is that WAL.

/// Upper bounds, in microseconds, of the fsync latency histogram buckets
/// (a final implicit +Inf bucket catches everything slower).
pub const FSYNC_BUCKET_BOUNDS_MICROS: [u64; 14] = [
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 5_000_000,
];

struct FsyncCounters {
    buckets: [std::sync::atomic::AtomicU64; FSYNC_BUCKET_BOUNDS_MICROS.len() + 1],
    count: std::sync::atomic::AtomicU64,
    sum_micros: std::sync::atomic::AtomicU64,
    max_micros: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
    entries: std::sync::atomic::AtomicU64,
    bytes: std::sync::atomic::AtomicU64,
    rewrites: std::sync::atomic::AtomicU64,
    rewrite_micros: std::sync::atomic::AtomicU64,
}

#[allow(clippy::declare_interior_mutable_const)]
const ZERO: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static FSYNC: FsyncCounters = FsyncCounters {
    buckets: [ZERO; FSYNC_BUCKET_BOUNDS_MICROS.len() + 1],
    count: ZERO,
    sum_micros: ZERO,
    max_micros: ZERO,
    errors: ZERO,
    entries: ZERO,
    bytes: ZERO,
    rewrites: ZERO,
    rewrite_micros: ZERO,
};

/// A point-in-time copy of the WAL's fsync statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsyncStats {
    /// Bucket upper bounds in microseconds; `bucket_counts` has one more
    /// element (the +Inf bucket). Counts are per bucket, NOT cumulative.
    pub bounds_micros: &'static [u64],
    pub bucket_counts: Vec<u64>,
    /// Successful append fsyncs (== number of group-commit batches made durable).
    pub count: u64,
    pub sum_micros: u64,
    pub max_micros: u64,
    /// Appends whose write or fsync failed (each one poisons its `Wal`).
    pub errors: u64,
    /// Entries and bytes made durable by successful appends.
    pub entries: u64,
    pub bytes: u64,
    /// Atomic rewrites (log truncation / compaction) and their total time.
    pub rewrites: u64,
    pub rewrite_micros: u64,
}

fn record_fsync(elapsed: std::time::Duration) {
    use std::sync::atomic::Ordering::Relaxed;
    let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
    let idx = FSYNC_BUCKET_BOUNDS_MICROS
        .iter()
        .position(|&b| us <= b)
        .unwrap_or(FSYNC_BUCKET_BOUNDS_MICROS.len());
    FSYNC.buckets[idx].fetch_add(1, Relaxed);
    FSYNC.count.fetch_add(1, Relaxed);
    FSYNC.sum_micros.fetch_add(us, Relaxed);
    FSYNC.max_micros.fetch_max(us, Relaxed);
}

pub struct Wal {
    file: File,
    path: PathBuf,
    /// Set after any failed I/O; see module docs.
    failed: bool,
}

#[derive(Debug)]
pub enum WalError {
    Io(io::Error),
    HardStateCorrupt(&'static str),
    HardStateUnsupportedVersion(u16),
    HardStateUnsupportedLegacyFormat,
    HardStateSerialization(&'static str),
    HardStateMissing,
    /// A WAL frame is damaged but valid-looking data follows it, or the
    /// frames are internally inconsistent. Nothing was modified.
    LogCorrupt {
        offset: u64,
        reason: &'static str,
    },
    /// The surviving log does not line up with the snapshot or itself.
    LogInconsistent(&'static str),
    /// The snapshot file exists but cannot be trusted. It is left in place.
    SnapshotCorrupt(&'static str),
    SnapshotUnsupportedVersion(u16),
    /// The persisted pieces (hard state / log / snapshot) contradict each
    /// other, e.g. the log holds an entry from a term newer than the
    /// persisted current term (a rolled-back or swapped file).
    StateInconsistent(&'static str),
    /// The WAL file is missing although the node has persisted state that
    /// proves it once existed (a hard state, a snapshot, or a rewrite temp
    /// file). Creating an empty replacement would silently discard the log
    /// entries - possibly committed, acknowledged ones - it held, so the
    /// open is refused and nothing is created or modified.
    LogMissing(&'static str),
    /// An earlier I/O error poisoned this handle.
    Poisoned,
}

impl std::fmt::Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalError::Io(e) => write!(f, "I/O error: {e}"),
            WalError::HardStateCorrupt(w) => write!(f, "hard state corrupt: {w}"),
            WalError::HardStateUnsupportedVersion(v) => {
                write!(f, "hard state has unsupported version {v}")
            }
            WalError::HardStateUnsupportedLegacyFormat => {
                write!(f, "hard state is in the unsupported legacy 16-byte format")
            }
            WalError::HardStateSerialization(w) => write!(f, "hard state not serializable: {w}"),
            WalError::HardStateMissing => write!(
                f,
                "hard state file is missing but the node has persisted log/snapshot data; \
                 refusing to start with term 0 (could vote twice in a term)"
            ),
            WalError::LogCorrupt { offset, reason } => {
                write!(f, "WAL corrupt at byte offset {offset}: {reason}")
            }
            WalError::LogInconsistent(w) => write!(f, "WAL inconsistent: {w}"),
            WalError::SnapshotCorrupt(w) => write!(f, "snapshot corrupt: {w}"),
            WalError::SnapshotUnsupportedVersion(v) => {
                write!(f, "snapshot has unsupported version {v}")
            }
            WalError::StateInconsistent(w) => write!(f, "persisted state inconsistent: {w}"),
            WalError::LogMissing(w) => write!(
                f,
                "WAL file is missing ({w}); refusing to recreate it empty, because \
                 log entries that may have been committed could be lost"
            ),
            WalError::Poisoned => write!(f, "WAL handle poisoned by an earlier I/O failure"),
        }
    }
}

impl std::error::Error for WalError {}

impl From<io::Error> for WalError {
    fn from(e: io::Error) -> Self {
        WalError::Io(e)
    }
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
}

impl Wal {
    /// Open (or create) the WAL file at `path`. Does NOT replay it - call
    /// `recover()` for that.
    ///
    /// A newly created file's directory entry is fsynced, and a stale
    /// `<path>.tmp` left by a crash mid-rewrite is removed (the live file is
    /// always valid on its own; see `rewrite`).
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, WalError> {
        let path = path.as_ref();
        durable::remove_stale_tmp(path)?;
        let existed = path.exists();
        let file = open_append(path)?;
        if !existed {
            durable::sync_parent_dir(FaultPoint::WalCreateDirSync, path)?;
        }
        Ok(Self {
            file,
            path: path.to_path_buf(),
            failed: false,
        })
    }

    /// Process-wide fsync statistics; see [`FsyncStats`]. Cheap (a few
    /// relaxed loads) and safe to call from any thread, e.g. a metrics scrape.
    pub fn fsync_stats() -> FsyncStats {
        use std::sync::atomic::Ordering::Relaxed;
        FsyncStats {
            bounds_micros: &FSYNC_BUCKET_BOUNDS_MICROS,
            bucket_counts: FSYNC.buckets.iter().map(|b| b.load(Relaxed)).collect(),
            count: FSYNC.count.load(Relaxed),
            sum_micros: FSYNC.sum_micros.load(Relaxed),
            max_micros: FSYNC.max_micros.load(Relaxed),
            errors: FSYNC.errors.load(Relaxed),
            entries: FSYNC.entries.load(Relaxed),
            bytes: FSYNC.bytes.load(Relaxed),
            rewrites: FSYNC.rewrites.load(Relaxed),
            rewrite_micros: FSYNC.rewrite_micros.load(Relaxed),
        }
    }

    fn guard(&self) -> Result<(), WalError> {
        if self.failed {
            Err(WalError::Poisoned)
        } else {
            Ok(())
        }
    }

    /// Append one entry and fsync before returning.
    pub fn append(&mut self, entry: &LogEntry) -> Result<(), WalError> {
        self.append_batch(std::slice::from_ref(entry))
    }

    /// Group commit: append all `entries` with one `write` and one
    /// `fsync`. Returns `Ok` only once every entry is durable. On error a
    /// prefix of the batch may be on disk (a torn tail, discarded by the
    /// next `recover`); the caller must not treat any of it as
    /// acknowledged.
    pub fn append_batch(&mut self, entries: &[LogEntry]) -> Result<(), WalError> {
        self.guard()?;
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        for e in entries {
            encode_frame_into(&mut buf, e);
        }
        let result = (|| -> io::Result<std::time::Duration> {
            durable::write_all(FaultPoint::WalAppendWrite, &mut self.file, &buf)?;
            let t = std::time::Instant::now();
            durable::sync_file(FaultPoint::WalAppendSync, &self.file)?;
            Ok(t.elapsed())
        })();
        match result {
            Ok(fsync_time) => {
                use std::sync::atomic::Ordering::Relaxed;
                record_fsync(fsync_time);
                FSYNC.entries.fetch_add(entries.len() as u64, Relaxed);
                FSYNC.bytes.fetch_add(buf.len() as u64, Relaxed);
            }
            Err(e) => {
                FSYNC
                    .errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.failed = true;
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Replace the log's contents with exactly `entries`, **atomically**.
    ///
    /// This is how a Raft log truncation (§5.3) and a compaction are
    /// realized on disk. The old file stays intact and authoritative until
    /// the rename; see the module docs. O(entries.len()).
    pub fn rewrite(&mut self, entries: &[LogEntry]) -> Result<(), WalError> {
        self.guard()?;
        let mut buf = Vec::new();
        for e in entries {
            encode_frame_into(&mut buf, e);
        }
        let started = std::time::Instant::now();
        let result = (|| -> io::Result<File> {
            durable::atomic_write(&self.path, &buf, WAL_REWRITE_POINTS)?;
            // The old handle points at the replaced (now unlinked) inode.
            open_append(&self.path)
        })();
        match result {
            Ok(f) => {
                use std::sync::atomic::Ordering::Relaxed;
                FSYNC.rewrites.fetch_add(1, Relaxed);
                FSYNC.rewrite_micros.fetch_add(
                    started.elapsed().as_micros().min(u64::MAX as u128) as u64,
                    Relaxed,
                );
                self.file = f;
                Ok(())
            }
            Err(e) => {
                self.failed = true;
                Err(e.into())
            }
        }
    }

    /// True if a complete, checksum-valid, decodable frame starts at any
    /// offset `>= from`. Used only on the (rare) damaged-frame path.
    ///
    /// A damaged length field cannot be told apart from a torn write by
    /// looking at that frame alone: both read as "claims more bytes than the
    /// file has" (or as a frame that ends at the end of the file). If an
    /// intact frame exists *after* the damaged one, the damage is not at the
    /// tail, and discarding "the tail" would silently drop entries that may
    /// have been committed.
    fn valid_frame_follows(bytes: &[u8], from: usize) -> bool {
        (from..bytes.len().saturating_sub(FRAME_HEADER_LEN - 1)).any(|o| {
            let len = u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap()) as usize;
            let Some(end) = o
                .checked_add(FRAME_HEADER_LEN)
                .and_then(|x| x.checked_add(len))
            else {
                return false;
            };
            if len == 0 || end > bytes.len() {
                return false;
            }
            let crc = u32::from_le_bytes(bytes[o + 4..o + 8].try_into().unwrap());
            let payload = &bytes[o + FRAME_HEADER_LEN..end];
            crc32(payload) == crc && decode_payload(payload).is_some()
        })
    }

    /// Read and validate the whole file at `path` **without modifying it**:
    /// the entries that are good, and - separately - any torn tail that
    /// recovery would cut off. See the module docs for what counts as a torn
    /// tail and what is reported as corruption. Use this when other files
    /// must be validated before anything is truncated (`WalLog::open`).
    pub(crate) fn scan(path: &Path) -> Result<WalScan, WalError> {
        let bytes = fs::read(path)?;
        let total = bytes.len();
        let mut entries: Vec<LogEntry> = Vec::new();
        let mut offset: usize = 0;

        // `Some(reason)` once we hit a bad frame at `offset`.
        let mut bad: Option<(&'static str, bool)> = None; // (reason, frame_is_complete)

        while offset < total {
            if total - offset < FRAME_HEADER_LEN {
                bad = Some(("incomplete frame header", false));
                break;
            }
            let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap());
            let end = offset + FRAME_HEADER_LEN + len;
            if end > total {
                bad = Some(("incomplete frame payload", false));
                break;
            }
            let payload = &bytes[offset + FRAME_HEADER_LEN..end];
            if crc32(payload) != crc {
                bad = Some(("checksum mismatch", true));
                break;
            }
            let Some(entry) = decode_payload(payload) else {
                bad = Some(("undecodable frame", true));
                break;
            };
            // A CRC-valid frame that breaks log invariants is not a torn
            // write; it must never be silently accepted *or* dropped.
            if let Some(prev) = entries.last() {
                if entry.index != prev.index + 1 {
                    return Err(WalError::LogCorrupt {
                        offset: offset as u64,
                        reason: "non-contiguous entry index",
                    });
                }
                if entry.term < prev.term {
                    return Err(WalError::LogCorrupt {
                        offset: offset as u64,
                        reason: "entry term decreases",
                    });
                }
            }
            entries.push(entry);
            offset = end;
        }

        let mut torn = None;
        if let Some((reason, complete)) = bad {
            let rest = &bytes[offset..];
            let torn_tail = if !complete {
                // Ran off the end of the file mid-frame: a genuine torn
                // write, unless the frame's own (possibly damaged) header
                // hides intact frames that follow it.
                !Self::valid_frame_follows(&bytes, offset + 1)
            } else {
                // A complete-but-bad frame is a torn tail only if it is the
                // last thing in the file, or everything from here on is
                // zero padding (file size extended, data never persisted)
                // - and, again, no intact frame follows it (a length field
                // inflated to exactly the end of the file must not make the
                // frames it swallows look like one damaged final frame).
                let frame_len = FRAME_HEADER_LEN
                    + u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
                (offset + frame_len == total || rest.iter().all(|&b| b == 0))
                    && !Self::valid_frame_follows(&bytes, offset + 1)
            };
            if !torn_tail {
                return Err(WalError::LogCorrupt {
                    offset: offset as u64,
                    reason,
                });
            }
            torn = Some(TornTail {
                offset: offset as u64,
                discarded: rest.to_vec(),
            });
        }
        Ok(WalScan { entries, torn })
    }

    /// Replay the file from the start; see the module docs for exactly what
    /// is truncated and what is reported as corruption. A torn tail is cut
    /// off only after the whole file has been validated.
    pub fn recover(&mut self) -> Result<Vec<LogEntry>, WalError> {
        self.guard()?;
        let scan = Self::scan(&self.path)?;
        if let Some(t) = &scan.torn {
            self.truncate_tail(t.offset, &t.discarded)?;
        }
        Ok(scan.entries)
    }

    /// Cut off a torn tail found by [`Wal::scan`].
    pub(crate) fn truncate_torn(&mut self, torn: &TornTail) -> Result<(), WalError> {
        self.guard()?;
        self.truncate_tail(torn.offset, &torn.discarded)
    }

    /// Drop a torn tail: save the discarded bytes aside, truncate, fsync.
    fn truncate_tail(&mut self, good_offset: u64, discarded: &[u8]) -> Result<(), WalError> {
        if discarded.iter().any(|&b| b != 0) {
            // Preserve what we are about to drop (best effort; a failure
            // to write the backup must not block recovery of a torn tail).
            let mut p = self.path.as_os_str().to_owned();
            p.push(".discarded-tail");
            let _ = fs::write(PathBuf::from(p), discarded);
        }
        let result = (|| -> io::Result<()> {
            match crate::fault::poll(FaultPoint::WalRecoverTruncate) {
                None => self.file.set_len(good_offset)?,
                Some(_) => return Err(io::Error::other("injected fault at WalRecoverTruncate")),
            }
            durable::sync_file(FaultPoint::WalRecoverSync, &self.file)
        })();
        if let Err(e) = result {
            self.failed = true;
            return Err(e.into());
        }
        Ok(())
    }
}
