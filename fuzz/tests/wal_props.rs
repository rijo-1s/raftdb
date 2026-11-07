//! Property tests for the main Raft WAL (`storage::Wal`).
//!
//! Properties (see the task brief):
//!  P1 valid data round-trips (append, batch, rewrite)
//!  P2 any single-byte corruption is detected, or loses ONLY the final record
//!  P3 truncation anywhere never errors and loses only the cut frame
//!  P4 a refused start leaves the file byte-for-byte untouched
//!  P5 arbitrary bytes never panic, and anything accepted is a valid log

use proptest::prelude::*;
use raft_core::LogEntry;
use raftdb_fuzz::*;
use storage::Wal;

fn spec() -> impl Strategy<Value = Vec<(u8, u8, Vec<u8>)>> {
    spec_of_at_least(1)
}

/// Like `spec()` but never shorter than `min` entries. Generating long enough lists
/// directly (instead of discarding short ones with `prop_assume!`) keeps the number of
/// rejected cases at zero, so a large `PROPTEST_CASES` cannot trip proptest's cap on
/// global rejects.
fn spec_of_at_least(min: usize) -> impl Strategy<Value = Vec<(u8, u8, Vec<u8>)>> {
    prop::collection::vec(
        (
            any::<u8>(),
            any::<u8>(),
            prop::collection::vec(any::<u8>(), 0..48),
        ),
        min..12,
    )
}

fn write_wal(path: &std::path::Path, entries: &[LogEntry]) {
    let mut w = Wal::open(path).unwrap();
    w.append_batch(entries).unwrap();
}

fn recover(path: &std::path::Path) -> Result<Vec<LogEntry>, storage::WalError> {
    Wal::open(path)?.recover()
}

/// A recovered log must itself satisfy the log invariants the scanner enforces.
fn assert_valid_log(entries: &[LogEntry]) {
    for w in entries.windows(2) {
        assert_eq!(w[1].index, w[0].index + 1, "non-contiguous index accepted");
        assert!(w[1].term >= w[0].term, "decreasing term accepted");
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 300, failure_persistence: None, ..ProptestConfig::default() })]

    // P1
    #[test]
    fn valid_data_round_trips(spec in spec(), start in 1u64..1_000, term in 0u64..50, chunk in 1usize..5) {
        let entries = make_entries(start, term, &spec);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let mut w = Wal::open(&path).unwrap();
            for c in entries.chunks(chunk) {
                w.append_batch(c).unwrap();
            }
        }
        prop_assert_eq!(recover(&path).unwrap(), entries.clone());
        // rewrite replaces the contents exactly
        let half = &entries[..entries.len() / 2];
        let mut w = Wal::open(&path).unwrap();
        w.rewrite(half).unwrap();
        drop(w);
        prop_assert_eq!(recover(&path).unwrap(), half.to_vec());
    }

    // P2 + P4
    #[test]
    fn any_single_byte_flip_is_detected_or_loses_only_the_last_record(
        spec in spec(), pos in any::<prop::sample::Index>(), mask in 1u8..=255,
    ) {
        let entries = make_entries(1, 1, &spec);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_wal(&path, &entries);
        let mut bytes = std::fs::read(&path).unwrap();
        let i = pos.index(bytes.len());
        bytes[i] ^= mask;
        std::fs::write(&path, &bytes).unwrap();
        let last_start: usize = entries[..entries.len() - 1].iter().map(frame_len).sum();
        match recover(&path) {
            Err(_) => prop_assert_eq!(std::fs::read(&path).unwrap(), bytes, "refusal modified the file"),
            Ok(got) => {
                prop_assert!(i >= last_start, "flip at {i} in an earlier frame (last starts {last_start}) went undetected");
                prop_assert_eq!(got, entries[..entries.len() - 1].to_vec());
            }
        }
    }

    // P3
    #[test]
    fn truncation_anywhere_loses_only_the_cut_frame(spec in spec(), cut in any::<prop::sample::Index>()) {
        let entries = make_entries(1, 1, &spec);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_wal(&path, &entries);
        let full = std::fs::read(&path).unwrap();
        let l = cut.index(full.len() + 1); // 0..=len
        std::fs::write(&path, &full[..l]).unwrap();
        let got = recover(&path).expect("a truncated log is a torn tail, never corruption");
        let mut acc = 0; let mut keep = 0;
        for e in &entries { if acc + frame_len(e) <= l { acc += frame_len(e); keep += 1 } else { break } }
        prop_assert_eq!(&got, &entries[..keep].to_vec());
        prop_assert_eq!(std::fs::read(&path).unwrap(), full[..acc].to_vec(), "tail not truncated to the last whole frame");
        // idempotent
        prop_assert_eq!(recover(&path).unwrap(), got);
    }

    // P2 (multi-byte) + P4: random damage over a range
    #[test]
    fn ranged_damage_never_yields_wrong_or_skipped_data(
        spec in spec(), a in any::<prop::sample::Index>(), n in 1usize..40,
        junk in prop::collection::vec(any::<u8>(), 40), zero in any::<bool>(),
    ) {
        let entries = make_entries(1, 1, &spec);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_wal(&path, &entries);
        let orig = std::fs::read(&path).unwrap();
        let start = a.index(orig.len());
        let end = (start + n).min(orig.len());
        let mut bytes = orig.clone();
        for (k, b) in bytes[start..end].iter_mut().enumerate() {
            *b = if zero { 0 } else { junk[k] };
        }
        prop_assume!(bytes != orig);
        let first_changed = (start..end).find(|&k| bytes[k] != orig[k]).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        match recover(&path) {
            Err(_) => prop_assert_eq!(std::fs::read(&path).unwrap(), bytes),
            Ok(got) => {
                // accepted data is a true prefix of what was written ...
                prop_assert_eq!(&got[..], &entries[..got.len()]);
                // ... and the damage lies entirely in the discarded tail.
                let valid: usize = got.iter().map(frame_len).sum();
                prop_assert!(first_changed >= valid, "damage at {first_changed} inside accepted prefix {valid}");
            }
        }
    }

    // P5: arbitrary bytes
    #[test]
    fn arbitrary_bytes_never_panic_and_refusal_is_inert(bytes in prop::collection::vec(any::<u8>(), 0..400)) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        std::fs::write(&path, &bytes).unwrap();
        match recover(&path) {
            Err(_) => prop_assert_eq!(std::fs::read(&path).unwrap(), bytes),
            Ok(got) => assert_valid_log(&got),
        }
    }

    // P5: CRC-valid frames with garbage bodies reach the structural decoder
    #[test]
    fn crc_valid_garbage_frames_never_panic(
        payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..80), 1..5),
        tail in prop::collection::vec(any::<u8>(), 0..20),
    ) {
        let mut bytes = Vec::new();
        for p in &payloads { bytes.extend(frame(p)); }
        bytes.extend(tail);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        std::fs::write(&path, &bytes).unwrap();
        match recover(&path) {
            Err(_) => prop_assert_eq!(std::fs::read(&path).unwrap(), bytes),
            Ok(got) => assert_valid_log(&got),
        }
    }

    // P5: forged but well-formed frames that violate log ordering
    #[test]
    fn crc_valid_but_misordered_logs_are_refused_not_accepted(
        spec in spec_of_at_least(3), swap in any::<prop::sample::Index>(),
    ) {
        let mut entries = make_entries(1, 1, &spec);
        let i = swap.index(entries.len() - 1);
        entries.swap(i, i + 1); // indexes now out of order, each frame still valid
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_wal(&path, &entries);
        let before = std::fs::read(&path).unwrap();
        prop_assert!(recover(&path).is_err());
        prop_assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    // The shape random bit flips almost never produce: a frame's length field
    // inflated to *exactly* the end of the file, so the frame "ends" at EOF
    // and swallows every intact frame behind it. That looks like a damaged
    // final frame (a torn tail) unless the scanner also looks for intact
    // frames after it. Accepting it as a torn tail would silently truncate
    // acknowledged entries.
    #[test]
    fn a_length_inflated_to_exactly_eof_is_corruption_not_a_torn_tail(spec in spec(), which in any::<prop::sample::Index>()) {
        prop_assume!(spec.len() >= 2);
        let entries = make_entries(1, 1, &spec);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        write_wal(&path, &entries);
        let mut bytes = std::fs::read(&path).unwrap();
        let k = which.index(entries.len() - 1); // any frame except the last
        let start: usize = entries[..k].iter().map(frame_len).sum();
        let inflated = (bytes.len() - start - 8) as u32;
        bytes[start..start + 4].copy_from_slice(&inflated.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        prop_assert!(recover(&path).is_err(), "frame {k} swallowed the frames after it and was accepted as a torn tail");
        prop_assert_eq!(std::fs::read(&path).unwrap(), bytes, "refusal modified the file");
    }
}
