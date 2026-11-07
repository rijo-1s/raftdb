//! Baseline (`KvStore`) vs LSM (`LsmStore`) benchmark.
//! Run: cargo bench -p kv --bench engine_bench
//! Env: BENCH_N (keys, default 200000), BENCH_RAFT_N (e2e writes, default 1500).

use std::time::{Duration, Instant};

use kv::{Command, KvNode, KvStore, LsmStore, LsmStoreOptions, StateMachine};
use raft_core::{RaftNode, Role};
use storage::WalLog;

fn pct(v: &mut [Duration], p: f64) -> f64 {
    v.sort();
    v[((v.len() as f64 * p) as usize).min(v.len() - 1)].as_secs_f64() * 1e6
}

fn cmd(i: u64) -> Vec<u8> {
    Command::Set {
        key: format!("user{i:08}"),
        value: format!("{i:0>100}"),
    }
    .encode()
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn dir_bytes(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .map(|rd| {
            rd.flatten()
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .sum()
        })
        .unwrap_or(0)
}

fn lsm(dir: &std::path::Path, sync: bool) -> LsmStore {
    let mut o = LsmStoreOptions::default();
    o.engine.sync_wal = sync;
    LsmStore::open(dir, o).unwrap()
}

fn row(name: &str, ops: f64, p50: f64, p99: f64) {
    println!("{name:<34} {ops:>12.0} ops/s   p50 {p50:>9.1} us   p99 {p99:>9.1} us");
}

fn bench_apply(name: &str, sm: &mut dyn StateMachine, n: u64, batch: u64) {
    let mut lat = Vec::new();
    let t = Instant::now();
    let mut i = 1;
    while i <= n {
        let end = (i + batch - 1).min(n);
        let cmds: Vec<Vec<u8>> = (i..=end).map(cmd).collect();
        let entries: Vec<(u64, &[u8])> = cmds
            .iter()
            .enumerate()
            .map(|(j, c)| (i + j as u64, c.as_slice()))
            .collect();
        let s = Instant::now();
        sm.apply_batch(&entries).unwrap();
        lat.push(s.elapsed());
        i = end + 1;
    }
    let el = t.elapsed().as_secs_f64();
    let (p50, p99) = (pct(&mut lat, 0.5), pct(&mut lat, 0.99));
    row(name, n as f64 / el, p50, p99);
}

fn bench_reads(name: &str, sm: &dyn StateMachine, n: u64, reads: u64) {
    let mut rng = Rng(0x1234_5678_9abc_def1);
    let mut lat = Vec::with_capacity(reads as usize);
    let t = Instant::now();
    for _ in 0..reads {
        let k = format!("user{:08}", 1 + rng.next() % n);
        let s = Instant::now();
        let v = sm.read(&k).unwrap();
        lat.push(s.elapsed());
        assert!(v.is_some());
    }
    let el = t.elapsed().as_secs_f64();
    let (p50, p99) = (pct(&mut lat, 0.5), pct(&mut lat, 0.99));
    row(name, reads as f64 / el, p50, p99);
}

fn raft_e2e(name: &str, mk: &dyn Fn(&std::path::Path) -> Box<dyn StateMachine>, n: u64) {
    let dir = tempfile::tempdir().unwrap();
    let wal = WalLog::open(dir.path().join("node.wal")).unwrap();
    let raft = RaftNode::with_log_seeded(1, vec![], wal, 7);
    let mut node = KvNode::with_state_machine(raft, mk(dir.path()), 1_000_000).unwrap();
    while node.role() != Role::Leader {
        node.tick();
    }
    for _ in 0..4 {
        node.tick();
    }
    let mut lat = Vec::new();
    let t = Instant::now();
    for i in 0..n {
        let s = Instant::now();
        node.propose_set(format!("user{i:08}"), format!("{i:0>100}"))
            .unwrap();
        while node.read(&format!("user{i:08}")).unwrap().is_none() {
            node.tick();
        }
        lat.push(s.elapsed());
    }
    let el = t.elapsed().as_secs_f64();
    let (p50, p99) = (pct(&mut lat, 0.5), pct(&mut lat, 0.99));
    row(name, n as f64 / el, p50, p99);
}

fn main() {
    let n: u64 = std::env::var("BENCH_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000);
    let raft_n: u64 = std::env::var("BENCH_RAFT_N")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1500);
    println!(
        "keys={n}  value=100B  key=12B  cpus={}\n",
        std::thread::available_parallelism()
            .map(|c| c.get())
            .unwrap_or(1)
    );

    println!("== apply (state machine only) ==");
    let mut base = KvStore::new();
    bench_apply("baseline mem, batch=1", &mut base, n, 1);
    let d1 = tempfile::tempdir().unwrap();
    let mut l1 = lsm(d1.path(), false);
    bench_apply("lsm sync_wal=off, batch=1", &mut l1, n, 1);
    let d2 = tempfile::tempdir().unwrap();
    let mut l2 = lsm(d2.path(), true);
    bench_apply("lsm sync_wal=ON,  batch=1", &mut l2, n.min(20_000), 1);
    let mut base64 = KvStore::new();
    bench_apply("baseline mem, batch=64", &mut base64, n, 64);
    let d3 = tempfile::tempdir().unwrap();
    let mut l3 = lsm(d3.path(), false);
    bench_apply("lsm sync_wal=off, batch=64", &mut l3, n, 64);
    let d4 = tempfile::tempdir().unwrap();
    let mut l4 = lsm(d4.path(), true);
    bench_apply("lsm sync_wal=ON,  batch=64", &mut l4, n, 64);

    println!("\n== point reads (uniform random, {n} keys; block cache 8 MiB) ==");
    bench_reads("baseline mem", &base64, n, 200_000);
    // let background work settle so reads hit tables, not just the memtable
    l3.engine().flush().unwrap();
    std::thread::sleep(Duration::from_millis(1500));
    bench_reads("lsm", &l3, n, 200_000);
    let st = l3.stats();
    println!(
        "  (lsm: L0={} L1={} files, cache hits={} misses={})",
        st.l0_files, st.l1_files, st.cache_hits, st.cache_misses
    );

    println!("\n== range scan, 100 keys per scan ==");
    let t = Instant::now();
    let mut lat = Vec::new();
    let mut rng = Rng(99);
    for _ in 0..2000 {
        let a = 1 + rng.next() % (n - 200);
        let s = Instant::now();
        let r = l3.scan(Some(&format!("user{a:08}")), None, 100).unwrap();
        lat.push(s.elapsed());
        assert_eq!(r.len(), 100);
    }
    let el = t.elapsed().as_secs_f64();
    row(
        "lsm scan(100)",
        2000.0 / el,
        pct(&mut lat, 0.5),
        pct(&mut lat, 0.99),
    );
    println!("  (baseline KvStore has no ordered scan; it would need a full sort per call)");

    println!("\n== disk usage after loading {n} keys ==");
    l3.engine().flush().unwrap();
    l3.engine().compact_all().unwrap();
    let blob = base64.snapshot_bytes().unwrap();
    println!("baseline: persists only the Raft snapshot blob: {:>10} bytes (state machine itself is RAM-only)", blob.len());
    println!(
        "lsm (uncompressed):                         {:>10} bytes on disk",
        dir_bytes(d3.path())
    );
    println!(
        "raw payload (keys+values):                  {:>10} bytes",
        n * (12 + 100)
    );

    println!("\n== recovery time ==");
    let t = Instant::now();
    let r = KvStore::restore(&blob).unwrap();
    println!(
        "baseline: parse {} B snapshot into memory:   {:>8.1} ms ({} keys)",
        blob.len(),
        t.elapsed().as_secs_f64() * 1e3,
        r.len()
    );
    drop(l3);
    let mut o = LsmStoreOptions::default();
    o.engine.verify_on_open = true;
    let t = Instant::now();
    let a = LsmStore::open(d3.path(), o.clone()).unwrap();
    println!(
        "lsm open, verify_on_open=true (full checksum):{:>7.1} ms",
        t.elapsed().as_secs_f64() * 1e3
    );
    drop(a);
    o.engine.verify_on_open = false;
    let t = Instant::now();
    let a = LsmStore::open(d3.path(), o.clone()).unwrap();
    println!(
        "lsm open, verify_on_open=false:               {:>7.1} ms",
        t.elapsed().as_secs_f64() * 1e3
    );
    drop(a);
    // WAL-heavy: 50k unflushed entries to replay
    let d5 = tempfile::tempdir().unwrap();
    {
        let mut big = LsmStoreOptions::default();
        big.engine.memtable_bytes = 256 << 20;
        big.engine.sync_wal = false;
        let mut s = LsmStore::open(d5.path(), big).unwrap();
        for i in 1..=50_000u64 {
            s.apply(i, &cmd(i)).unwrap();
        }
    }
    let mut big = LsmStoreOptions::default();
    big.engine.memtable_bytes = 256 << 20;
    let t = Instant::now();
    let a = LsmStore::open(d5.path(), big).unwrap();
    println!(
        "lsm open replaying 50k-entry WAL:             {:>7.1} ms (applied={})",
        t.elapsed().as_secs_f64() * 1e3,
        a.applied_index()
    );

    println!("\n== end-to-end single-node Raft (fsync'd WalLog, one write at a time, {raft_n} writes) ==");
    raft_e2e("baseline mem", &|_| Box::new(KvStore::new()), raft_n);
    raft_e2e(
        "lsm sync_wal=off",
        &|d| Box::new(lsm(&d.join("lsm"), false)),
        raft_n,
    );
    raft_e2e(
        "lsm sync_wal=ON",
        &|d| Box::new(lsm(&d.join("lsm"), true)),
        raft_n,
    );
}
