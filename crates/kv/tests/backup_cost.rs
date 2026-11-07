//! How long does the node loop stand still while a backup is exported?
//!
//! The in-loop work is `durability_barrier` + `snapshot_bytes` (the same two
//! calls every compaction makes). This prints those timings for both engines.
//! Run: cargo test -p kv --release --test backup_cost -- --ignored --nocapture

use std::time::Instant;

use kv::{AnyStore, Command, LsmStore, LsmStoreOptions, StateMachine};

fn fill(store: &mut AnyStore, n: u64) {
    let mut idx = 0;
    while idx < n {
        let batch: Vec<(u64, Vec<u8>)> = (idx + 1..=(idx + 1000).min(n))
            .map(|i| {
                (
                    i,
                    Command::Set {
                        key: format!("key{i:08}"),
                        value: format!("{:0>100}", i),
                    }
                    .encode(),
                )
            })
            .collect();
        let refs: Vec<(u64, &[u8])> = batch.iter().map(|(i, d)| (*i, d.as_slice())).collect();
        store.apply_batch(&refs).unwrap();
        idx += refs.len() as u64;
    }
}

#[test]
#[ignore = "timing measurement, run manually (see the module docs)"]
fn how_long_does_the_node_loop_pause_for_a_backup() {
    println!(
        "\n{:>9} {:>7} {:>10} {:>12} {:>12}",
        "keys", "engine", "state MB", "barrier ms", "snapshot ms"
    );
    for n in [10_000u64, 100_000, 500_000] {
        for engine in ["mem", "lsm"] {
            let dir = tempfile::tempdir().unwrap();
            let mut store = if engine == "mem" {
                AnyStore::Mem(kv::KvStore::new())
            } else {
                AnyStore::Lsm(Box::new(
                    LsmStore::open(dir.path(), LsmStoreOptions::default()).unwrap(),
                ))
            };
            fill(&mut store, n);
            let t = Instant::now();
            store.durability_barrier().unwrap();
            let barrier = t.elapsed();
            let t = Instant::now();
            let bytes = store.snapshot_bytes().unwrap();
            let snap = t.elapsed();
            println!(
                "{:>9} {:>7} {:>10.1} {:>12.1} {:>12.1}",
                n,
                engine,
                bytes.len() as f64 / 1e6,
                barrier.as_secs_f64() * 1e3,
                snap.as_secs_f64() * 1e3
            );
        }
    }
}
