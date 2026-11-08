//! Checksum-valid but hostile SSTables. Random bytes never get past the CRCs,
//! so these forge a correct CRC around a malicious body to reach the
//! structural checks behind it.

use storage::lsm::{Lsm, LsmOptions, Op};

fn opts() -> LsmOptions {
    LsmOptions {
        background: false,
        block_bytes: 64,
        memtable_bytes: 1 << 30,
        ..Default::default()
    }
}

fn crc(b: &[u8]) -> u32 {
    raftdb_fuzz::crc32(b)
}

/// Build a directory with one SSTable, then let `patch` rewrite the index
/// block body (entries) of that table; CRCs are recomputed so the file stays
/// checksum-valid.
fn forged_index(patch: impl FnOnce(&mut Vec<u8>)) -> (tempfile::TempDir, Result<(), String>) {
    let d = tempfile::tempdir().unwrap();
    {
        let db = Lsm::open(d.path(), opts()).unwrap();
        for i in 0..20u64 {
            db.apply(
                &[Op::Put(format!("k{i:03}").into_bytes(), vec![b'v'; 30])],
                i + 1,
            )
            .unwrap();
        }
        db.flush().unwrap();
    }
    let sst = std::fs::read_dir(d.path())
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|e| e == "sst"))
        .unwrap();
    let mut b = std::fs::read(&sst).unwrap();
    let n = b.len();
    let index_off = u64::from_le_bytes(b[n - 52..n - 44].try_into().unwrap()) as usize;
    let index_len = u64::from_le_bytes(b[n - 44..n - 36].try_into().unwrap()) as usize;
    let mut body = b[index_off..index_off + index_len - 4].to_vec();
    patch(&mut body);
    assert_eq!(
        body.len() + 4,
        index_len,
        "patch must keep the index length (footer offsets stay valid)"
    );
    let c = crc(&body);
    b[index_off..index_off + body.len()].copy_from_slice(&body);
    b[index_off + body.len()..index_off + index_len].copy_from_slice(&c.to_le_bytes());
    std::fs::write(&sst, &b).unwrap();
    let r = std::panic::catch_unwind(|| {
        Lsm::open(d.path(), opts())
            .map(|_| ())
            .map_err(|e| e.to_string())
    });
    let out = match r {
        Ok(r) => r,
        Err(p) => Err(format!(
            "PANIC: {}",
            p.downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default()
        )),
    };
    (d, out)
}

/// First index entry: `klen:u32 | key | off:u64 | len:u32`; returns the offset of `off`.
fn first_entry_off_pos(body: &[u8]) -> usize {
    let kl = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
    4 + kl
}

#[test]
fn control_an_unpatched_rewrite_of_the_index_still_opens() {
    let (_d, r) = forged_index(|_| {});
    assert!(r.is_ok(), "{r:?}");
}

#[test]
fn index_block_offset_in_the_valid_range_but_wrong_is_refused() {
    let (_d, r) = forged_index(|body| {
        let p = first_entry_off_pos(body);
        body[p..p + 8].copy_from_slice(&3u64.to_le_bytes()); // points into the middle of block 0
    });
    assert!(
        r.is_err(),
        "a misplaced block must be refused at open, got {r:?}"
    );
}

/// `SstReader::open` must not check `off + len > index_off` with plain `+`.
/// With a checksum-valid index whose `off` is near u64::MAX that would
/// overflow (a panic in debug builds, a silent wrap in release builds). It
/// must be a clean `Corruption` error in both.
#[test]
fn index_block_offset_near_u64_max_must_be_refused_not_panic() {
    let (_d, r) = forged_index(|body| {
        let p = first_entry_off_pos(body);
        body[p..p + 8].copy_from_slice(&(u64::MAX - 1).to_le_bytes());
    });
    match r {
        Err(m) if m.starts_with("PANIC") => panic!("open() panicked on a hostile index: {m}"),
        Err(_) => {}
        Ok(()) => panic!("open() accepted an index entry pointing outside the file"),
    }
}
