use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use raft_core::{EntryKind, LogEntry};
use storage::Wal;

fn entry(term: u64, index: u64, data: &str) -> LogEntry {
    LogEntry {
        term,
        index,
        kind: EntryKind::Data,
        data: data.as_bytes().to_vec(),
    }
}

#[test]
fn recovers_all_entries_after_clean_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&entry(1, 1, "SET x=1")).unwrap();
        wal.append(&entry(1, 2, "SET y=2")).unwrap();
        wal.append(&entry(2, 3, "SET x=3")).unwrap();
    } // drop: simulates closing the file normally

    let mut wal = Wal::open(&path).unwrap();
    let recovered = wal.recover().unwrap();

    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[0], entry(1, 1, "SET x=1"));
    assert_eq!(recovered[1], entry(1, 2, "SET y=2"));
    assert_eq!(recovered[2], entry(2, 3, "SET x=3"));
}

#[test]
fn empty_wal_recovers_to_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.log");

    let mut wal = Wal::open(path).unwrap();
    let recovered = wal.recover().unwrap();
    assert!(recovered.is_empty());
}

/// A crash *during* a write, not only after successful ones: two good
/// entries, then the file truncated partway into a third frame (as if the
/// process died mid-`write` before `fsync` ever ran).
#[test]
fn recovers_cleanly_from_a_crash_mid_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&entry(1, 1, "SET a=1")).unwrap();
        wal.append(&entry(1, 2, "SET b=2")).unwrap();
    }

    // Simulate a partial third write: append a frame header claiming a
    // large payload, then only write half of it before "crashing".
    {
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        let fake_len: u32 = 100;
        let fake_checksum: u32 = 0xDEAD_BEEF;
        f.write_all(&fake_len.to_le_bytes()).unwrap();
        f.write_all(&fake_checksum.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 40]).unwrap(); // only half of the promised 100 bytes
        f.sync_all().unwrap();
    }

    let mut wal = Wal::open(&path).unwrap();
    let recovered = wal.recover().unwrap();

    // Only the two clean entries survive; the torn frame is discarded.
    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[0], entry(1, 1, "SET a=1"));
    assert_eq!(recovered[1], entry(1, 2, "SET b=2"));

    // Recovery must also self-heal the file: further appends should work
    // and produce a consistent log, not append after garbage bytes.
    wal.append(&entry(2, 3, "SET c=3")).unwrap();
    let recovered_again = wal.recover().unwrap();
    assert_eq!(recovered_again.len(), 3);
    assert_eq!(recovered_again[2], entry(2, 3, "SET c=3"));
}

#[test]
fn detects_corrupted_frame_and_stops_before_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&entry(1, 1, "SET a=1")).unwrap();
        wal.append(&entry(1, 2, "SET b=2")).unwrap();
    }

    // Flip a byte inside the second frame's payload without touching its
    // length/checksum header - this simulates disk-level bit rot / a torn
    // write that still happened to land on a frame-sized boundary.
    {
        let mut f = OpenOptions::new().write(true).open(&path).unwrap();
        // First frame: 8-byte header + payload(term8+index8+kind_tag1+kind_node_id8+data_len4+"SET a=1".len()=7) = 8+28+7 = 43 bytes total.
        let first_frame_len = 8 + (8 + 8 + 1 + 8 + 4 + "SET a=1".len());
        f.seek(SeekFrom::Start(first_frame_len as u64 + 8 + 29))
            .unwrap(); // into 2nd frame's data bytes
        f.write_all(&[0xFF, 0xFF]).unwrap();
        f.sync_all().unwrap();
    }

    let mut wal = Wal::open(&path).unwrap();
    let recovered = wal.recover().unwrap();

    // The first entry is untouched and must still recover; the corrupted
    // second entry must not be returned (and nothing after it, though
    // there's nothing after it here).
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0], entry(1, 1, "SET a=1"));
}

#[test]
fn reopening_after_recovery_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal.log");

    {
        let mut wal = Wal::open(&path).unwrap();
        wal.append(&entry(1, 1, "SET a=1")).unwrap();
    }

    // Recover twice in a row (e.g. two restarts with no writes in between)
    // and confirm we get the same, stable result both times.
    let mut wal = Wal::open(&path).unwrap();
    let first = wal.recover().unwrap();
    drop(wal);

    let mut wal = Wal::open(&path).unwrap();
    let second = wal.recover().unwrap();

    assert_eq!(first, second);
}
