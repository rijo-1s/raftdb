//! Concurrent readers/scanners against a writer with the real background
//! flush+compaction thread running. Invariants checked while it runs:
//! per-key values never go backwards, and scans are internally consistent
//! (sorted, no duplicates, agree with point reads' monotonic bound).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;

use storage::lsm::{Lsm, LsmOptions, Op};

fn key(i: u64) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}
fn num(v: &[u8]) -> u64 {
    std::str::from_utf8(v).unwrap().parse().unwrap()
}

#[test]
fn readers_see_monotonic_values_while_flush_and_compaction_run() {
    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(
        Lsm::open(
            dir.path(),
            LsmOptions {
                memtable_bytes: 4096,
                block_bytes: 256,
                l1_file_bytes: 8192,
                l0_compaction_trigger: 2,
                sync_wal: false,
                background: true,
                ..LsmOptions::default()
            },
        )
        .unwrap(),
    );
    let stop = Arc::new(AtomicBool::new(false));
    let committed = Arc::new(AtomicU64::new(0)); // highest counter fully written
    const KEYS: u64 = 50;
    const ROUNDS: u64 = 400;

    let mut readers = Vec::new();
    for t in 0..3 {
        let (db, stop, committed) = (db.clone(), stop.clone(), committed.clone());
        readers.push(thread::spawn(move || {
            let mut last = vec![0u64; KEYS as usize];
            let mut reads = 0u64;
            while !stop.load(Ordering::Acquire) {
                let floor = committed.load(Ordering::Acquire);
                if t == 0 {
                    // scan: sorted, unique, each value >= what we saw before
                    let rows = db.scan(None, None, usize::MAX).unwrap();
                    assert!(
                        rows.windows(2).all(|w| w[0].0 < w[1].0),
                        "scan not strictly sorted"
                    );
                    for (k, v) in rows {
                        let i = std::str::from_utf8(&k[1..])
                            .unwrap()
                            .parse::<usize>()
                            .unwrap();
                        let n = num(&v);
                        assert!(
                            n >= last[i],
                            "scan saw key {i} go backwards: {} -> {n}",
                            last[i]
                        );
                        last[i] = n;
                    }
                } else {
                    for i in 0..KEYS {
                        let v = db.get(&key(i)).unwrap().map(|v| num(&v)).unwrap_or(0);
                        assert!(
                            v >= last[i as usize],
                            "key {i} went backwards: {} -> {v}",
                            last[i as usize]
                        );
                        // Every key was written in rounds <= floor, so it must
                        // be at least floor (writes complete in key order per round).
                        assert!(
                            v + 1 >= floor || floor == 0,
                            "key {i}: read {v} but round {floor} fully committed"
                        );
                        last[i as usize] = v;
                    }
                }
                reads += 1;
            }
            reads
        }));
    }

    for round in 1..=ROUNDS {
        for i in 0..KEYS {
            db.apply(
                &[Op::Put(key(i), round.to_string().into_bytes())],
                round * KEYS + i,
            )
            .unwrap();
        }
        committed.store(round, Ordering::Release);
    }
    stop.store(true, Ordering::Release);
    let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 10, "readers barely ran");

    // Let the background thread drain, then verify final state + that the
    // background machinery really did work.
    for _ in 0..200 {
        if db.stats().l0_files < 2 {
            break;
        }
        thread::sleep(std::time::Duration::from_millis(25));
    }
    let s = db.stats();
    assert!(
        s.flushes > 3 && s.compactions > 0,
        "background work did not run: {s:?}"
    );
    for i in 0..KEYS {
        assert_eq!(
            db.get(&key(i)).unwrap(),
            Some(ROUNDS.to_string().into_bytes())
        );
    }
    assert_eq!(db.applied_index(), ROUNDS * KEYS + KEYS - 1);
    drop(db);
    let db = Lsm::open(
        dir.path(),
        LsmOptions {
            background: false,
            ..LsmOptions::default()
        },
    )
    .unwrap();
    assert_eq!(
        db.get(&key(7)).unwrap(),
        Some(ROUNDS.to_string().into_bytes())
    );
}
