//! In-process engine benchmarks. Two families, never mixed:
//!   * cpu_bound: nothing is fsynced in the timed path (KvStore, MemLog,
//!     LsmStore with sync_wal=false).
//!   * durable:   every acknowledged unit is fsynced (WalLog, LsmStore with
//!     sync_wal=true, raw fsync probe).
//!
//! Every benchmark reads its data back and compares it with the expected
//! values; a mismatch fails the run (so dropped work cannot "win").

use std::io::Write;
use std::path::Path;
use std::time::Instant;

use kv::{Command, KvNode, KvStore, LsmStore, LsmStoreOptions, StateMachine};
use raft_core::{LogStore, MemLog, RaftNode, Role};
use storage::WalLog;

use crate::common::*;
use crate::json::{nu, obj, s, J};

fn key(i: u64) -> String {
    format!("user{i:010}")
}
fn val(i: u64) -> String {
    format!("{i:0>100}")
}
fn set_cmd(i: u64) -> Vec<u8> {
    Command::Set {
        key: key(i),
        value: val(i),
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

/// Same as `set_cmd` but as an exactly-once session write: 1000 clients,
/// each with an increasing sequence number.
fn session_cmd(i: u64) -> Vec<u8> {
    Command::session(
        1 + i % 1000,
        i,
        Command::Set {
            key: key(i),
            value: val(i),
        },
    )
    .unwrap()
    .encode()
}

/// Apply keys 1..=n in batches of `batch`; returns per-call latencies (ms).
fn apply_all(sm: &mut dyn StateMachine, n: u64, batch: u64) -> Result<Vec<f64>, String> {
    apply_with(sm, n, batch, set_cmd)
}

fn apply_with(
    sm: &mut dyn StateMachine,
    n: u64,
    batch: u64,
    make: fn(u64) -> Vec<u8>,
) -> Result<Vec<f64>, String> {
    let mut lat = Vec::new();
    let mut i = 1;
    while i <= n {
        let end = (i + batch - 1).min(n);
        let cmds: Vec<Vec<u8>> = (i..=end).map(make).collect();
        let entries: Vec<(u64, &[u8])> = cmds
            .iter()
            .enumerate()
            .map(|(j, c)| (i + j as u64, c.as_slice()))
            .collect();
        let t = Instant::now();
        sm.apply_batch(&entries)?;
        lat.push(t.elapsed().as_secs_f64() * 1e3);
        i = end + 1;
    }
    Ok(lat)
}

/// Read every key back and compare. Also checks a never-written key is absent.
fn verify_sm(sm: &dyn StateMachine, n: u64) -> Result<u64, String> {
    for i in 1..=n {
        match sm.read(&key(i))? {
            Some(v) if v == val(i) => {}
            other => return Err(format!("key {i}: expected value, got {other:?}")),
        }
    }
    if sm.read(&key(n + 1))?.is_some() {
        return Err("phantom key present".into());
    }
    Ok(n)
}

fn lsm_opts(sync: bool) -> LsmStoreOptions {
    let mut o = LsmStoreOptions::default();
    o.engine.sync_wal = sync;
    o
}

fn latency(lat: &mut [f64], r: &mut RunResult) {
    r.p50_ms = Some(pct(lat, 0.50));
    r.p99_ms = Some(pct(lat, 0.99));
}

fn sc(cfg: &Cfg, full: u64, quick: u64) -> u64 {
    if cfg.quick {
        quick
    } else {
        full
    }
}

// ---------------------------------------------------------------- cpu bound

pub fn cpu_suite(cfg: &Cfg) -> Vec<J> {
    let mut out = Vec::new();
    let n = sc(cfg, 100_000, 20_000);

    for batch in [1u64, 64] {
        out.push(run_bench(
            cfg,
            &format!("cpu/kvstore_apply_b{batch}"),
            "cpu_bound",
            "ops_per_sec",
            true,
            obj(vec![
                ("keys", nu(n)),
                ("batch", nu(batch)),
                ("engine", s("KvStore (memory)")),
            ]),
            |_| {
                let mut sm = KvStore::new();
                let t = Instant::now();
                let mut lat = apply_all(&mut sm, n, batch)?;
                let el = t.elapsed().as_secs_f64();
                let mut r = RunResult::new(n as f64 / el);
                latency(&mut lat, &mut r);
                r.verified = verify_sm(&sm, n)?;
                if sm.len() as u64 != n {
                    return Err(format!("len {} != {n}", sm.len()));
                }
                Ok(r)
            },
        ));
    }

    out.push(run_bench(
        cfg,
        "cpu/lsm_nosync_apply_b64",
        "cpu_bound",
        "ops_per_sec",
        true,
        obj(vec![
            ("keys", nu(n)),
            ("batch", nu(64)),
            (
                "engine",
                s("LsmStore sync_wal=false (page cache, not durable)"),
            ),
        ]),
        |_| {
            let d = tempfile::tempdir_in(&cfg.cpu_dir).map_err(|e| e.to_string())?;
            let mut sm = LsmStore::open(d.path(), lsm_opts(false)).map_err(|e| e.to_string())?;
            let t = Instant::now();
            let mut lat = apply_all(&mut sm, n, 64)?;
            let el = t.elapsed().as_secs_f64();
            let mut r = RunResult::new(n as f64 / el);
            latency(&mut lat, &mut r);
            r.verified = verify_sm(&sm, n)?;
            if sm.applied_index() != n {
                return Err(format!("applied {} != {n}", sm.applied_index()));
            }
            r.disk_bytes = dir_bytes(d.path());
            Ok(r)
        },
    ));

    // Cost of exactly-once sessions on the durable engine: compare with
    // cpu/lsm_nosync_apply_b64 in the same session.
    out.push(run_bench(
        cfg,
        "cpu/lsm_nosync_apply_b64_sessions",
        "cpu_bound",
        "ops_per_sec",
        true,
        obj(vec![
            ("keys", nu(n)),
            ("batch", nu(64)),
            ("clients", nu(1000)),
            (
                "engine",
                s("LsmStore sync_wal=false, every write wrapped in a session"),
            ),
        ]),
        |_| {
            let d = tempfile::tempdir_in(&cfg.cpu_dir).map_err(|e| e.to_string())?;
            let mut sm = LsmStore::open(d.path(), lsm_opts(false)).map_err(|e| e.to_string())?;
            let t = Instant::now();
            let mut lat = apply_with(&mut sm, n, 64, session_cmd)?;
            let el = t.elapsed().as_secs_f64();
            let mut r = RunResult::new(n as f64 / el);
            latency(&mut lat, &mut r);
            r.verified = verify_sm(&sm, n)?;
            if sm.sessions().len() != 1000 {
                return Err(format!("{} sessions, expected 1000", sm.sessions().len()));
            }
            // every client's last sequence must be its highest key index
            for c in 1..=1000u64 {
                let want = (1..=n).rev().find(|i| 1 + i % 1000 == c);
                if sm.session_last_seq(c) != want {
                    return Err(format!(
                        "client {c}: last seq {:?}, want {want:?}",
                        sm.session_last_seq(c)
                    ));
                }
            }
            r.disk_bytes = dir_bytes(d.path());
            Ok(r)
        },
    ));

    let reads = sc(cfg, 100_000, 20_000);
    out.push(run_bench(
        cfg,
        "cpu/lsm_point_read",
        "cpu_bound",
        "ops_per_sec",
        true,
        obj(vec![
            ("keys", nu(n)),
            ("reads", nu(reads)),
            ("cache_mib", nu(8)),
        ]),
        |_| {
            let d = tempfile::tempdir_in(&cfg.cpu_dir).map_err(|e| e.to_string())?;
            let mut sm = LsmStore::open(d.path(), lsm_opts(false)).map_err(|e| e.to_string())?;
            apply_all(&mut sm, n, 64)?;
            sm.engine().flush().map_err(|e| e.to_string())?;
            sm.engine().compact_all().map_err(|e| e.to_string())?;
            let mut rng = Rng(0x1234_5678_9abc_def1);
            let mut lat = Vec::with_capacity(reads as usize);
            let t = Instant::now();
            let mut checked = 0;
            for _ in 0..reads {
                let i = 1 + rng.next() % n;
                let t0 = Instant::now();
                let v = sm.read(&key(i))?;
                lat.push(t0.elapsed().as_secs_f64() * 1e3);
                if v.as_deref() != Some(val(i).as_str()) {
                    return Err(format!("read of key {i} returned {v:?}"));
                }
                checked += 1;
            }
            let el = t.elapsed().as_secs_f64();
            let mut r = RunResult::new(reads as f64 / el);
            latency(&mut lat, &mut r);
            r.verified = checked;
            r.disk_bytes = dir_bytes(d.path());
            Ok(r)
        },
    ));

    let rn = sc(cfg, 3000, 500);
    out.push(run_bench(
        cfg,
        "cpu/raft_single_node_memlog",
        "cpu_bound",
        "ops_per_sec",
        true,
        obj(vec![
            ("writes", nu(rn)),
            ("log", s("MemLog")),
            ("engine", s("KvStore")),
        ]),
        |_| {
            let raft = RaftNode::with_log_seeded(1, vec![], MemLog::new(), 7);
            let sm: Box<dyn StateMachine> = Box::new(KvStore::new());
            let mut node =
                KvNode::with_state_machine(raft, sm, 1_000_000).map_err(|e| e.to_string())?;
            while node.role() != Role::Leader {
                node.tick();
            }
            for _ in 0..4 {
                node.tick();
            }
            let mut lat = Vec::new();
            let t = Instant::now();
            for i in 1..=rn {
                let t0 = Instant::now();
                node.propose_set(key(i), val(i)).ok_or("not leader")?;
                while node.read(&key(i))?.is_none() {
                    node.tick();
                }
                lat.push(t0.elapsed().as_secs_f64() * 1e3);
            }
            let el = t.elapsed().as_secs_f64();
            let mut r = RunResult::new(rn as f64 / el);
            latency(&mut lat, &mut r);
            for i in 1..=rn {
                if node.read(&key(i))?.as_deref() != Some(val(i).as_str()) {
                    return Err(format!("key {i} wrong after run"));
                }
            }
            r.verified = rn;
            Ok(r)
        },
    ));
    out
}

// ------------------------------------------------------------------ durable

pub fn durable_suite(cfg: &Cfg) -> Vec<J> {
    let mut out = Vec::new();

    // Raw fsync probe: calibrates what the disk can do at all.
    let probes = sc(cfg, 500, 100);
    out.push(run_bench(
        cfg,
        "durable/raw_fsync_4k",
        "durable",
        "ops_per_sec",
        true,
        obj(vec![("writes", nu(probes)), ("bytes", nu(4096))]),
        |_| {
            let d = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
            let mut f = std::fs::File::create(d.path().join("probe")).map_err(|e| e.to_string())?;
            let buf = [0x5Au8; 4096];
            let mut lat = Vec::new();
            let t = Instant::now();
            for _ in 0..probes {
                let t0 = Instant::now();
                f.write_all(&buf).map_err(|e| e.to_string())?;
                f.sync_data().map_err(|e| e.to_string())?;
                lat.push(t0.elapsed().as_secs_f64() * 1e3);
            }
            let mut r = RunResult::new(probes as f64 / t.elapsed().as_secs_f64());
            latency(&mut lat, &mut r);
            let len = std::fs::metadata(d.path().join("probe"))
                .map_err(|e| e.to_string())?
                .len();
            if len != probes * 4096 {
                return Err(format!("probe file is {len} bytes"));
            }
            r.verified = probes;
            r.disk_bytes = len;
            Ok(r)
        },
    ));

    // WalLog group commit: durable appends at several batch sizes. After the
    // run the log is reopened from disk and every entry compared.
    for (batch, full, quick) in [(1u64, 1000u64, 200u64), (16, 4000, 800), (256, 4000, 800)] {
        let total = sc(cfg, full, quick);
        out.push(run_bench(
            cfg,
            &format!("durable/wal_append_b{batch}"),
            "durable",
            "entries_per_sec",
            true,
            obj(vec![
                ("entries", nu(total)),
                ("batch", nu(batch)),
                ("payload_bytes", nu(100)),
            ]),
            |_| {
                let d = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
                let path = d.path().join("node.wal");
                let mut log = WalLog::open(&path).map_err(|e| e.to_string())?;
                log.save_hard_state(1, None);
                let mut lat = Vec::new();
                let t = Instant::now();
                let mut next = 1u64;
                while next <= total {
                    let nn = batch.min(total - next + 1);
                    let data: Vec<Vec<u8>> =
                        (next..next + nn).map(|i| val(i).into_bytes()).collect();
                    let t0 = Instant::now();
                    if nn == 1 {
                        log.append(1, data.into_iter().next().unwrap());
                    } else {
                        log.append_batch(1, data);
                    }
                    lat.push(t0.elapsed().as_secs_f64() * 1e3);
                    next += nn;
                }
                let el = t.elapsed().as_secs_f64();
                let mut r = RunResult::new(total as f64 / el);
                latency(&mut lat, &mut r);
                drop(log);
                r.disk_bytes = dir_bytes(d.path());
                r.verified = verify_wal(&path, total)?;
                Ok(r)
            },
        ));
    }

    // LsmStore with sync_wal=true: fsync per apply call. Verified in place
    // and again after close + reopen (the durability claim).
    for (batch, full, quick) in [(1u64, 1000u64, 200u64), (64, 20_000, 4000)] {
        let total = sc(cfg, full, quick);
        out.push(run_bench(
            cfg,
            &format!("durable/lsm_sync_apply_b{batch}"),
            "durable",
            "ops_per_sec",
            true,
            obj(vec![
                ("keys", nu(total)),
                ("batch", nu(batch)),
                ("engine", s("LsmStore sync_wal=true")),
            ]),
            |_| {
                let d = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
                let mut sm = LsmStore::open(d.path(), lsm_opts(true)).map_err(|e| e.to_string())?;
                let t = Instant::now();
                let mut lat = apply_all(&mut sm, total, batch)?;
                let el = t.elapsed().as_secs_f64();
                let mut r = RunResult::new(total as f64 / el);
                latency(&mut lat, &mut r);
                verify_sm(&sm, total)?;
                drop(sm);
                let sm = LsmStore::open(d.path(), lsm_opts(true)).map_err(|e| e.to_string())?;
                if sm.applied_index() != total {
                    return Err(format!(
                        "reopened applied {} != {total}",
                        sm.applied_index()
                    ));
                }
                r.verified = verify_sm(&sm, total)?;
                r.disk_bytes = dir_bytes(d.path());
                Ok(r)
            },
        ));
    }

    // End-to-end single node: fsynced WalLog + LsmStore(sync), one write at a time.
    let rn = sc(cfg, 500, 100);
    out.push(run_bench(
        cfg,
        "durable/raft_single_node_wal_lsmsync",
        "durable",
        "ops_per_sec",
        true,
        obj(vec![
            ("writes", nu(rn)),
            ("log", s("WalLog fsync")),
            ("engine", s("LsmStore sync_wal=true")),
        ]),
        |_| {
            let d = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
            let wal = WalLog::open(d.path().join("node.wal")).map_err(|e| e.to_string())?;
            let raft = RaftNode::with_log_seeded(1, vec![], wal, 7);
            let store =
                LsmStore::open(d.path().join("lsm"), lsm_opts(true)).map_err(|e| e.to_string())?;
            let sm: Box<dyn StateMachine> = Box::new(store);
            let mut node =
                KvNode::with_state_machine(raft, sm, 1_000_000).map_err(|e| e.to_string())?;
            while node.role() != Role::Leader {
                node.tick();
            }
            for _ in 0..4 {
                node.tick();
            }
            let mut lat = Vec::new();
            let t = Instant::now();
            for i in 1..=rn {
                let t0 = Instant::now();
                node.propose_set(key(i), val(i)).ok_or("not leader")?;
                while node.read(&key(i))?.is_none() {
                    node.tick();
                }
                lat.push(t0.elapsed().as_secs_f64() * 1e3);
            }
            let el = t.elapsed().as_secs_f64();
            let mut r = RunResult::new(rn as f64 / el);
            latency(&mut lat, &mut r);
            drop(node);
            // Reopen the state machine alone: every acknowledged write must be there.
            let store =
                LsmStore::open(d.path().join("lsm"), lsm_opts(true)).map_err(|e| e.to_string())?;
            let mut present = 0;
            for i in 1..=rn {
                if store.read(&key(i))?.as_deref() == Some(val(i).as_str()) {
                    present += 1;
                }
            }
            // The LSM may legitimately lag the (fsynced) Raft log only by
            // entries not yet applied; with sync_wal=true applied == acked.
            if present != rn {
                return Err(format!(
                    "only {present}/{rn} acked writes present after reopen"
                ));
            }
            r.verified = present;
            r.disk_bytes = dir_bytes(d.path());
            Ok(r)
        },
    ));
    out
}

fn verify_wal(path: &Path, total: u64) -> Result<u64, String> {
    let log = WalLog::open(path).map_err(|e| format!("reopen: {e}"))?;
    if log.last_index() != total {
        return Err(format!(
            "reopened last_index {} != {total}",
            log.last_index()
        ));
    }
    for i in 1..=total {
        match log.entry_at(i) {
            Some(e) if e.data == val(i).into_bytes() => {}
            _ => return Err(format!("wal entry {i} missing or wrong after reopen")),
        }
    }
    Ok(total)
}

// ----------------------------------------------------------------- recovery

fn maybe_drop(cfg: &Cfg) {
    if cfg.drop_caches {
        drop_caches();
    }
}

pub fn recovery_suite(cfg: &Cfg) -> Vec<J> {
    let mut out = Vec::new();
    let sizes: Vec<u64> = if cfg.quick {
        vec![5_000, 20_000]
    } else {
        vec![10_000, 50_000, 200_000]
    };

    // Flushed + compacted LSM: open time vs data size, with and without
    // the full-checksum pass.
    for &nk in &sizes {
        let src = tempfile::tempdir_in(&cfg.bench_dir).expect("tempdir");
        {
            let mut sm = LsmStore::open(src.path(), lsm_opts(false)).expect("open");
            apply_all(&mut sm, nk, 64).expect("load");
            sm.engine().flush().expect("flush");
            sm.engine().compact_all().expect("compact");
        }
        let disk = dir_bytes(src.path());
        for verify in [true, false] {
            out.push(run_bench(
                cfg,
                &format!(
                    "recovery/lsm_open_n{nk}_verify{}",
                    if verify { "On" } else { "Off" }
                ),
                "durable",
                "recovery_ms",
                false,
                obj(vec![
                    ("keys", nu(nk)),
                    ("raw_payload_bytes", nu(nk * 110)),
                    ("verify_on_open", J::Bool(verify)),
                    ("state", s("flushed+compacted SSTables")),
                ]),
                |_| {
                    let work = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
                    let dst = work.path().join("db");
                    copy_dir(src.path(), &dst).map_err(|e| e.to_string())?;
                    maybe_drop(cfg);
                    let mut o = lsm_opts(false);
                    o.engine.verify_on_open = verify;
                    let t = Instant::now();
                    let sm = LsmStore::open(&dst, o).map_err(|e| e.to_string())?;
                    let ms = t.elapsed().as_secs_f64() * 1e3;
                    let mut r = RunResult::new(ms);
                    r.verified = verify_sm(&sm, nk)?;
                    r.disk_bytes = disk;
                    Ok(r)
                },
            ));
        }
    }

    // Unflushed: recovery must replay the WAL.
    let wal_sizes: Vec<u64> = if cfg.quick {
        vec![5_000, 10_000]
    } else {
        vec![10_000, 50_000]
    };
    for &nk in &wal_sizes {
        let src = tempfile::tempdir_in(&cfg.bench_dir).expect("tempdir");
        {
            let mut o = lsm_opts(false);
            o.engine.memtable_bytes = 256 << 20;
            let mut sm = LsmStore::open(src.path(), o).expect("open");
            apply_all(&mut sm, nk, 64).expect("load");
        }
        let disk = dir_bytes(src.path());
        out.push(run_bench(
            cfg,
            &format!("recovery/lsm_wal_replay_n{nk}"),
            "durable",
            "recovery_ms",
            false,
            obj(vec![
                ("keys", nu(nk)),
                ("state", s("unflushed memtable, WAL replay")),
            ]),
            |_| {
                let work = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
                let dst = work.path().join("db");
                copy_dir(src.path(), &dst).map_err(|e| e.to_string())?;
                maybe_drop(cfg);
                let mut o = lsm_opts(false);
                o.engine.memtable_bytes = 256 << 20;
                let t = Instant::now();
                let sm = LsmStore::open(&dst, o).map_err(|e| e.to_string())?;
                let ms = t.elapsed().as_secs_f64() * 1e3;
                if sm.applied_index() != nk {
                    return Err(format!("applied {} != {nk}", sm.applied_index()));
                }
                let mut r = RunResult::new(ms);
                r.verified = verify_sm(&sm, nk)?;
                r.disk_bytes = disk;
                Ok(r)
            },
        ));
    }

    // Raft WAL reopen time vs number of entries.
    let wl: Vec<u64> = if cfg.quick {
        vec![1_000, 10_000]
    } else {
        vec![1_000, 10_000, 50_000]
    };
    for &ne in &wl {
        let src = tempfile::tempdir_in(&cfg.bench_dir).expect("tempdir");
        {
            let mut log = WalLog::open(src.path().join("node.wal")).expect("open");
            log.save_hard_state(1, None);
            let mut i = 1;
            while i <= ne {
                let end = (i + 255).min(ne);
                log.append_batch(1, (i..=end).map(|j| val(j).into_bytes()).collect());
                i = end + 1;
            }
        }
        let disk = dir_bytes(src.path());
        out.push(run_bench(
            cfg,
            &format!("recovery/wal_reopen_n{ne}"),
            "durable",
            "recovery_ms",
            false,
            obj(vec![("entries", nu(ne)), ("payload_bytes", nu(100))]),
            |_| {
                let work = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
                let dst = work.path().join("db");
                copy_dir(src.path(), &dst).map_err(|e| e.to_string())?;
                maybe_drop(cfg);
                let t = Instant::now();
                let log = WalLog::open(dst.join("node.wal")).map_err(|e| e.to_string())?;
                let ms = t.elapsed().as_secs_f64() * 1e3;
                if log.last_index() != ne {
                    return Err(format!("last_index {} != {ne}", log.last_index()));
                }
                for i in 1..=ne {
                    if log.entry_at(i).map(|e| e.data.as_slice()) != Some(val(i).as_bytes()) {
                        return Err(format!("entry {i} wrong"));
                    }
                }
                let mut r = RunResult::new(ms);
                r.verified = ne;
                r.disk_bytes = disk;
                Ok(r)
            },
        ));
    }
    out
}
