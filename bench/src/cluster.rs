//! Process-level benchmarks: a real 3-node cluster (separate `raftdb`
//! processes, real gRPC, real fsyncs on the chosen filesystem).
//!
//! The harness's own gRPC client lives here (not the CLI's) because the
//! overload run must NOT retry rejected writes, and every run must read all
//! acknowledged data back through linearizable Gets.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use raftdb_proto as proto;

use crate::common::*;
use crate::json::{n, nu, obj, s, J};

type Client = proto::client_api_client::ClientApiClient<tonic::transport::Channel>;
const OVERLOADED: &str = "server overloaded";
static PORT_SLOT: AtomicU16 = AtomicU16::new(0);

/// When set (`--sessions`), every benchmark write carries an exactly-once
/// session so its end-to-end cost can be measured against plain writes.
pub static USE_SESSIONS: AtomicBool = AtomicBool::new(false);

/// (client_id, seq) for write `i` of worker `w` in phase `tag`. Distinct per
/// (tag, worker) so reused worker numbers never look like retries.
fn session_for(tag: &str, w: u32, i: u64) -> (u64, u64) {
    if !USE_SESSIONS.load(Ordering::Relaxed) {
        return (0, 0);
    }
    let h = tag
        .bytes()
        .fold(7u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64))
        & 0xFFFF_FFFF;
    ((h << 24) + w as u64 + 1, i + 1)
}

fn next_port() -> u16 {
    7200 + (PORT_SLOT.fetch_add(1, Ordering::Relaxed) % 60) * 8
}

async fn connect(addr: &str, rpc_timeout_s: u64) -> Result<Client, String> {
    let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(rpc_timeout_s));
    let ch = ep
        .connect()
        .await
        .map_err(|e| format!("connect {addr}: {e}"))?;
    Ok(proto::client_api_client::ClientApiClient::new(ch))
}

fn value_for(tag: &str, w: u32, i: u64, size: usize) -> String {
    let mut v = format!("{tag}:{w}:{i}:");
    while v.len() < size {
        v.push('x');
    }
    v
}
fn key_for(tag: &str, w: u32, i: u64) -> String {
    format!("h:{tag}:{w}:{i}")
}

// ------------------------------------------------------------ process mgmt

struct Cluster {
    bin: PathBuf,
    dir: PathBuf,
    base: u16,
    extra: Vec<String>,
    kids: Vec<Option<Child>>,
}

impl Cluster {
    fn new(bin: &Path, dir: &Path, base: u16, extra: &[String]) -> Self {
        Self {
            bin: bin.to_path_buf(),
            dir: dir.to_path_buf(),
            base,
            extra: extra.to_vec(),
            kids: vec![None, None, None],
        }
    }
    fn addr(&self, id: u64) -> String {
        format!("127.0.0.1:{}", self.base as u64 + id)
    }
    fn start_node(&mut self, id: u64) -> Result<(), String> {
        let peers: Vec<String> = (1..=3u64)
            .filter(|p| *p != id)
            .map(|p| format!("{p}={}", self.addr(p)))
            .collect();
        let log = std::fs::File::create(self.dir.join(format!("n{id}.log")))
            .map_err(|e| e.to_string())?;
        let child = Command::new(&self.bin)
            .arg("start")
            .args(["--id", &id.to_string()])
            .args(["--addr", &self.addr(id)])
            .args(["--peers", &peers.join(",")])
            .args([
                "--data-dir",
                self.dir.join(format!("n{id}")).to_str().unwrap(),
            ])
            .args(["--compaction-threshold", "100000"])
            .args(&self.extra)
            .stdout(Stdio::null())
            .stderr(log)
            .spawn()
            .map_err(|e| format!("spawn {}: {e}", self.bin.display()))?;
        self.kids[(id - 1) as usize] = Some(child);
        Ok(())
    }
    fn start_all(&mut self) -> Result<(), String> {
        for id in 1..=3 {
            self.start_node(id)?;
        }
        Ok(())
    }
    /// SIGKILL: a crash, not a graceful shutdown.
    fn kill_all(&mut self) {
        for k in self.kids.iter_mut() {
            if let Some(mut c) = k.take() {
                let _ = c.kill();
                let _ = c.wait();
            }
        }
    }
    fn pid(&self, id: u64) -> Option<u32> {
        self.kids[(id - 1) as usize].as_ref().map(|c| c.id())
    }
    fn data_bytes(&self) -> u64 {
        (1..=3)
            .map(|i| dir_bytes(&self.dir.join(format!("n{i}"))))
            .sum()
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        self.kill_all();
    }
}

fn rss_kb(pid: u32) -> u64 {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|x| x.parse().ok())
        .unwrap_or(0)
}

/// Wait until some node accepts a write; returns the leader's id.
async fn await_leader(cl: &Cluster) -> Result<u64, String> {
    let deadline = Instant::now() + Duration::from_secs(40);
    while Instant::now() < deadline {
        for id in 1..=3u64 {
            if let Ok(mut c) = connect(&cl.addr(id), 5).await {
                if let Ok(r) = c
                    .put(proto::PutRequest {
                        key: "h:probe".into(),
                        value: "1".into(),
                        client_id: 0,
                        seq: 0,
                    })
                    .await
                {
                    let r = r.into_inner();
                    if r.ok {
                        return Ok(id);
                    }
                    if let Some(h) = r.leader_hint {
                        if let Ok(mut c2) = connect(&cl.addr(h), 5).await {
                            if let Ok(r2) = c2
                                .put(proto::PutRequest {
                                    key: "h:probe".into(),
                                    value: "1".into(),
                                    client_id: 0,
                                    seq: 0,
                                })
                                .await
                            {
                                if r2.into_inner().ok {
                                    return Ok(h);
                                }
                            }
                        }
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err("cluster never became writable".into())
}

// ------------------------------------------------------------------ loads

#[derive(Default)]
struct Load {
    ok: u64,
    overloaded: u64,
    other: u64,
    other_msgs: Vec<String>,
    lat_ok: Vec<f64>,
    lat_rej: Vec<f64>,
    acked: Vec<(u32, u64)>,
}
impl Load {
    fn merge(&mut self, o: Load) {
        self.ok += o.ok;
        self.overloaded += o.overloaded;
        self.other += o.other;
        for m in o.other_msgs {
            if self.other_msgs.len() < 5 {
                self.other_msgs.push(m);
            }
        }
        self.lat_ok.extend(o.lat_ok);
        self.lat_rej.extend(o.lat_rej);
        self.acked.extend(o.acked);
    }
}

fn classify(
    l: &mut Load,
    w: u32,
    i: u64,
    t0: Instant,
    r: Result<tonic::Response<proto::PutReply>, tonic::Status>,
) -> bool {
    let ms = t0.elapsed().as_secs_f64() * 1e3;
    match r {
        Ok(r) => {
            let r = r.into_inner();
            if r.ok {
                l.ok += 1;
                l.lat_ok.push(ms);
                l.acked.push((w, i));
                true
            } else if r.error.starts_with(OVERLOADED) {
                l.overloaded += 1;
                l.lat_rej.push(ms);
                false
            } else {
                l.other += 1;
                l.other_msgs.push(r.error);
                false
            }
        }
        Err(e) => {
            l.other += 1;
            l.other_msgs.push(e.to_string());
            false
        }
    }
}

/// `workers` connections, each doing `share` sequential puts. With `retry`,
/// an overloaded write is retried (latency includes the retries).
async fn closed_loop(
    addr: String,
    tag: String,
    workers: u32,
    count: u64,
    vsize: usize,
) -> Result<(Load, f64), String> {
    let t = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for w in 0..workers {
        let share = count / workers as u64 + u64::from((w as u64) < count % workers as u64);
        let (addr, tag) = (addr.clone(), tag.clone());
        set.spawn(async move {
            let mut c = connect(&addr, 20).await?;
            let mut l = Load::default();
            for i in 0..share {
                let t0 = Instant::now();
                loop {
                    let (client_id, seq) = session_for(&tag, w, i);
                    let r = c
                        .put(proto::PutRequest {
                            key: key_for(&tag, w, i),
                            value: value_for(&tag, w, i, vsize),
                            client_id,
                            seq,
                        })
                        .await;
                    let before = l.overloaded;
                    let done = classify(&mut l, w, i, t0, r);
                    if done {
                        break;
                    }
                    if l.overloaded > before {
                        // classify pushed a rejection latency; retry after a pause
                        l.lat_rej.pop();
                        tokio::time::sleep(Duration::from_millis(2)).await;
                        continue;
                    }
                    break; // hard error: recorded
                }
            }
            Ok::<_, String>(l)
        });
    }
    let mut all = Load::default();
    while let Some(j) = set.join_next().await {
        all.merge(j.map_err(|e| e.to_string())??);
    }
    Ok((all, t.elapsed().as_secs_f64()))
}

/// Open loop for `dur`: `tasks` concurrent requesters sharing a few HTTP/2
/// channels, NEVER retrying. Offered load is bounded only by `tasks`.
async fn open_loop(
    addr: String,
    tag: String,
    tasks: u32,
    channels: u32,
    dur: Duration,
    vsize: usize,
) -> Result<(Load, f64), String> {
    let mut clients = Vec::new();
    for _ in 0..channels {
        clients.push(connect(&addr, 15).await?);
    }
    let deadline = Instant::now() + dur;
    let t = Instant::now();
    let mut set = tokio::task::JoinSet::new();
    for w in 0..tasks {
        let base = clients[(w % channels) as usize].clone();
        let tag = tag.clone();
        set.spawn(async move {
            let mut l = Load::default();
            let mut i = 0u64;
            while Instant::now() < deadline {
                let mut c = base.clone();
                let t0 = Instant::now();
                let (client_id, seq) = session_for(&tag, w, i);
                let r = c
                    .put(proto::PutRequest {
                        key: key_for(&tag, w, i),
                        value: value_for(&tag, w, i, vsize),
                        client_id,
                        seq,
                    })
                    .await;
                classify(&mut l, w, i, t0, r);
                i += 1;
            }
            l
        });
    }
    let mut all = Load::default();
    while let Some(j) = set.join_next().await {
        all.merge(j.map_err(|e| e.to_string())?);
    }
    Ok((all, t.elapsed().as_secs_f64()))
}

/// Linearizable Get of every (w,i); returns (checked, bad).
async fn verify_keys(
    addr: String,
    tag: String,
    vsize: usize,
    keys: Vec<(u32, u64)>,
) -> Result<(u64, u64), String> {
    let chunks = 16usize;
    let per = (keys.len() + chunks - 1) / chunks.max(1);
    let mut set = tokio::task::JoinSet::new();
    for ch in keys.chunks(per.max(1)) {
        let ch = ch.to_vec();
        let (addr, tag) = (addr.clone(), tag.clone());
        set.spawn(async move {
            let mut c = connect(&addr, 20).await?;
            let (mut checked, mut bad) = (0u64, 0u64);
            for (w, i) in ch {
                let mut tries = 0;
                let r = loop {
                    let r = c
                        .get(proto::GetRequest {
                            key: key_for(&tag, w, i),
                        })
                        .await
                        .map_err(|e| e.to_string())?
                        .into_inner();
                    if r.error.is_empty() {
                        break r;
                    }
                    tries += 1;
                    if tries > 100 {
                        return Err(format!("read never answered: {}", r.error));
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                };
                checked += 1;
                if !r.found || r.value != value_for(&tag, w, i, vsize) {
                    bad += 1;
                }
            }
            Ok::<_, String>((checked, bad))
        });
    }
    let (mut a, mut b) = (0, 0);
    while let Some(j) = set.join_next().await {
        let (x, y) = j.map_err(|e| e.to_string())??;
        a += x;
        b += y;
    }
    Ok((a, b))
}

fn all_keys(workers: u32, count: u64) -> Vec<(u32, u64)> {
    let mut v = Vec::new();
    for w in 0..workers {
        let share = count / workers as u64 + u64::from((w as u64) < count % workers as u64);
        for i in 0..share {
            v.push((w, i));
        }
    }
    v
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// ------------------------------------------------ throughput + recovery

fn load_run(
    cfg: &Cfg,
    base_dir: &Path,
    extra: &[String],
    conc: u32,
    count: u64,
    vsize: usize,
) -> Result<RunResult, String> {
    let dir = tempfile::tempdir_in(base_dir).map_err(|e| e.to_string())?;
    let mut cl = Cluster::new(&cfg.bin, dir.path(), next_port(), extra);
    cl.start_all()?;
    let rt = rt();
    rt.block_on(async {
        let leader = await_leader(&cl).await?;
        let (mut load, el) = closed_loop(cl.addr(leader), "t".into(), conc, count, vsize).await?;
        if load.other > 0 || load.ok != count {
            return Err(format!(
                "load failed: ok={} other={} {:?}",
                load.ok, load.other, load.other_msgs
            ));
        }
        let (checked, bad) =
            verify_keys(cl.addr(leader), "t".into(), vsize, all_keys(conc, count)).await?;
        if bad > 0 || checked != count {
            return Err(format!(
                "read-back: {bad} bad of {checked} (expected {count})"
            ));
        }
        let mut r = RunResult::new(count as f64 / el);
        r.p50_ms = Some(pct(&mut load.lat_ok, 0.50));
        r.p99_ms = Some(pct(&mut load.lat_ok, 0.99));
        r.verified = checked;
        r.disk_bytes = cl.data_bytes();
        r.extra
            .push(("overloaded_retries".into(), nu(load.overloaded)));
        Ok(r)
    })
}

fn recovery_run(cfg: &Cfg, base_dir: &Path, count: u64, vsize: usize) -> Result<RunResult, String> {
    let dir = tempfile::tempdir_in(base_dir).map_err(|e| e.to_string())?;
    let mut cl = Cluster::new(&cfg.bin, dir.path(), next_port(), &[]);
    cl.start_all()?;
    let rt = rt();
    let conc = 64u32.min(count as u32);
    let leader = rt.block_on(async {
        let leader = await_leader(&cl).await?;
        let (load, _) = closed_loop(cl.addr(leader), "r".into(), conc, count, vsize).await?;
        if load.other > 0 || load.ok != count {
            return Err(format!("load failed: ok={} other={}", load.ok, load.other));
        }
        Ok::<_, String>(leader)
    })?;
    let _ = leader;
    let before_disk = cl.data_bytes();
    cl.kill_all(); // SIGKILL all three: a whole-cluster crash
    if cfg.drop_caches {
        drop_caches();
    }
    let t0 = Instant::now();
    cl.start_all()?;
    rt.block_on(async {
        // Ready = some node serves a linearizable read of a known key.
        let probe = (0u32, 0u64);
        let deadline = Instant::now() + Duration::from_secs(90);
        let mut leader = 0;
        'outer: while Instant::now() < deadline {
            for id in 1..=3u64 {
                if let Ok(mut c) = connect(&cl.addr(id), 5).await {
                    if let Ok(r) = c
                        .get(proto::GetRequest {
                            key: key_for("r", probe.0, probe.1),
                        })
                        .await
                    {
                        let r = r.into_inner();
                        if r.found && r.value == value_for("r", probe.0, probe.1, vsize) {
                            leader = id;
                            break 'outer;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if leader == 0 {
            return Err("cluster did not serve reads after restart".to_string());
        }
        let ms = t0.elapsed().as_secs_f64() * 1e3;
        let (checked, bad) =
            verify_keys(cl.addr(leader), "r".into(), vsize, all_keys(conc, count)).await?;
        if bad > 0 || checked != count {
            return Err(format!("after crash: {bad} bad of {checked}"));
        }
        let mut r = RunResult::new(ms);
        r.verified = checked;
        r.disk_bytes = before_disk;
        Ok(r)
    })
}

pub fn throughput_and_recovery(cfg: &Cfg) -> Vec<J> {
    let mut out = Vec::new();
    if !cfg.bin.exists() {
        return vec![obj(vec![
            ("name", s("cluster/setup")),
            ("ok", J::Bool(false)),
            (
                "errors",
                J::Arr(vec![s(format!(
                    "raftdb binary not found at {} (cargo build --release first)",
                    cfg.bin.display()
                ))]),
            ),
        ])];
    }
    let vsize = 64usize;
    let flags = |a: &[&str]| -> Vec<String> { a.iter().map(|x| x.to_string()).collect() };
    // (label, dir-kind, mode, extra flags, concurrency, full count, quick count)
    let sp = |c: u32, f: u64, q: u64| (c, if cfg.quick { q } else { f });
    type BenchCfg<'a> = (String, bool, &'a str, Vec<String>, (u32, u64));
    let mut cfgs: Vec<BenchCfg> = Vec::new();
    for (name, extra) in [
        ("unbatched", flags(&["--no-batching"])),
        ("batched", flags(&[])),
    ] {
        for (c, f, q) in [(1u32, 1500u64, 200u64), (16, 4000, 800), (64, 4000, 1000)] {
            cfgs.push((
                format!("cluster/{name}_c{c}"),
                false,
                "durable",
                extra.clone(),
                sp(c, f, q),
            ));
        }
    }
    cfgs.push((
        "cluster/batched_lsm_syncwal_c16".into(),
        false,
        "durable",
        flags(&["--engine", "lsm", "--lsm-sync-wal"]),
        sp(16, 4000, 800),
    ));
    // tmpfs: fsync is a no-op, so this isolates CPU/RPC cost from disk latency.
    cfgs.push((
        "cluster/tmpfs_unbatched_c16".into(),
        true,
        "cpu_bound",
        flags(&["--no-batching"]),
        sp(16, 4000, 800),
    ));
    cfgs.push((
        "cluster/tmpfs_batched_c16".into(),
        true,
        "cpu_bound",
        flags(&[]),
        sp(16, 4000, 800),
    ));

    for (name, cpu, mode, extra, (conc, count)) in cfgs {
        let base = if cpu {
            cfg.cpu_dir.clone()
        } else {
            cfg.bench_dir.clone()
        };
        out.push(run_bench(
            cfg,
            &name,
            mode,
            "writes_per_sec",
            true,
            obj(vec![
                ("nodes", nu(3)),
                ("concurrency", nu(conc as u64)),
                ("writes", nu(count)),
                ("value_bytes", nu(vsize as u64)),
                ("node_flags", s(extra.join(" "))),
                ("sessions", J::Bool(USE_SESSIONS.load(Ordering::Relaxed))),
                ("fsync_effective", J::Bool(!cpu)),
                ("data_dir_fs", s(base.display().to_string())),
            ]),
            |_| load_run(cfg, &base, &extra, conc, count, vsize),
        ));
    }

    let sizes: Vec<u64> = if cfg.quick {
        vec![500, 2000]
    } else {
        vec![1_000, 10_000, 40_000]
    };
    for sz in sizes {
        out.push(run_bench(
            cfg,
            &format!("cluster/recovery_after_kill9_n{sz}"),
            "durable",
            "recovery_ms",
            false,
            obj(vec![
                ("nodes", nu(3)),
                ("writes", nu(sz)),
                ("value_bytes", nu(vsize as u64)),
                ("compaction_threshold", nu(100000)),
                ("measures", s("SIGKILL all 3 nodes, restart, time until a linearizable read succeeds; then every key is read back")),
            ]),
            |_| recovery_run(cfg, &cfg.bench_dir, sz, vsize),
        ));
    }
    out
}

// ---------------------------------------------------------------- overload

struct OverloadParams {
    label: &'static str,
    max_pending: usize,
    max_batch: usize,
    /// Pass/fail criteria apply (the contrast run is informational).
    gated: bool,
}

fn overload_run(cfg: &Cfg, p: &OverloadParams) -> Result<RunResult, String> {
    let vsize = 1024usize;
    let tasks: u32 = if cfg.quick { 256 } else { 1024 };
    let dur = Duration::from_secs(if cfg.quick { 4 } else { 10 });
    let cap_n: u64 = if cfg.quick { 500 } else { 2000 };
    let dir = tempfile::tempdir_in(&cfg.bench_dir).map_err(|e| e.to_string())?;
    let extra: Vec<String> = [
        "--max-pending-writes".to_string(),
        p.max_pending.to_string(),
        "--max-batch-entries".to_string(),
        p.max_batch.to_string(),
    ]
    .to_vec();
    let mut cl = Cluster::new(&cfg.bin, dir.path(), next_port(), &extra);
    cl.start_all()?;
    let rt = rt();
    rt.block_on(async {
        let leader = await_leader(&cl).await?;
        let pid = cl.pid(leader).ok_or("no leader pid")?;

        // Capacity probe: closed loop, retrying, well inside the limit.
        let (cap, cap_el) = closed_loop(
            cl.addr(leader),
            "cap".into(),
            32.min(p.max_pending as u32),
            cap_n,
            vsize,
        )
        .await?;
        if cap.other > 0 {
            return Err(format!("capacity probe errors: {:?}", cap.other_msgs));
        }
        let capacity = cap.ok as f64 / cap_el;
        let rss_idle = rss_kb(pid);

        // RSS sampler.
        let stop = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(rss_idle));
        let sampler = {
            let (stop, peak) = (stop.clone(), peak.clone());
            tokio::spawn(async move {
                while !stop.load(Ordering::Relaxed) {
                    peak.fetch_max(rss_kb(pid), Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
        };

        let (mut load, el) = open_loop(cl.addr(leader), "ov".into(), tasks, 16, dur, vsize).await?;
        stop.store(true, Ordering::Relaxed);
        let _ = sampler.await;
        let rss_peak = peak.load(Ordering::Relaxed);

        // Let the queue drain, then verify: every ACKED write is readable.
        tokio::time::sleep(Duration::from_secs(1)).await;
        let acked = std::mem::take(&mut load.acked);
        let acked_n = acked.len() as u64;
        let mut verdict: Result<(u64, u64), String> = Err(String::new());
        for _ in 0..6 {
            let leader = await_leader(&cl).await?;
            verdict = verify_keys(cl.addr(leader), "ov".into(), vsize, acked.clone()).await;
            if verdict.is_ok() {
                break;
            }
        }
        let (checked, bad) = verdict?;
        // Still writable afterwards?
        let health = await_leader(&cl).await.is_ok();

        let total = load.ok + load.overloaded + load.other;
        let rss_growth_mb = (rss_peak.saturating_sub(rss_idle)) as f64 / 1024.0;
        let ok_p50 = pct(&mut load.lat_ok, 0.50);
        let ok_p99 = pct(&mut load.lat_ok, 0.99);
        let rej_p50 = pct(&mut load.lat_rej, 0.50);
        let rej_p99 = pct(&mut load.lat_rej, 0.99);

        // Criteria (gated run only). Write timeout is 10s by default; accepted
        // latency must stay far below it, nothing may time out / go unknown,
        // memory growth must stay small, and rejection must actually happen.
        let mut failures: Vec<String> = Vec::new();
        if bad > 0 || checked != acked_n {
            failures.push(format!("{bad} acked writes missing/wrong of {checked}"));
        }
        if !health {
            failures.push("server unhealthy after overload".into());
        }
        if p.gated {
            if load.overloaded == 0 {
                failures.push("no request was rejected: load did not exceed capacity".into());
            }
            if load.other > 0 {
                failures.push(format!(
                    "{} requests failed other than overload: {:?}",
                    load.other, load.other_msgs
                ));
            }
            if ok_p99 > 5000.0 {
                failures.push(format!(
                    "accepted p99 {ok_p99:.0}ms is not bounded (< 5000ms)"
                ));
            }
            if rss_growth_mb > 128.0 {
                failures.push(format!("leader RSS grew {rss_growth_mb:.0} MiB (> 128)"));
            }
        }
        let mut r = RunResult::new(load.ok as f64 / el);
        r.p50_ms = Some(ok_p50);
        r.p99_ms = Some(ok_p99);
        r.verified = checked;
        r.pass = failures.is_empty();
        r.extra = vec![
            ("label".into(), s(p.label)),
            (
                "failures".into(),
                J::Arr(failures.iter().map(|f| s(f.clone())).collect()),
            ),
            ("gated".into(), J::Bool(p.gated)),
            ("max_pending_writes".into(), nu(p.max_pending as u64)),
            ("concurrent_requesters".into(), nu(tasks as u64)),
            ("capacity_probe_writes_per_sec".into(), n(capacity)),
            ("offered_requests_per_sec".into(), n(total as f64 / el)),
            ("accepted_per_sec".into(), n(load.ok as f64 / el)),
            ("rejected_overloaded".into(), nu(load.overloaded)),
            ("other_errors".into(), nu(load.other)),
            ("accepted_p50_ms".into(), n(ok_p50)),
            ("accepted_p99_ms".into(), n(ok_p99)),
            ("rejected_p50_ms".into(), n(rej_p50)),
            ("rejected_p99_ms".into(), n(rej_p99)),
            ("leader_rss_idle_mib".into(), n(rss_idle as f64 / 1024.0)),
            ("leader_rss_peak_mib".into(), n(rss_peak as f64 / 1024.0)),
            ("leader_rss_growth_mib".into(), n(rss_growth_mb)),
            (
                "queue_payload_bound_bytes".into(),
                nu((p.max_pending * vsize) as u64),
            ),
            ("acked_writes_verified".into(), nu(checked)),
            ("healthy_after".into(), J::Bool(health)),
        ];
        Ok(r)
    })
}

pub fn overload_suite(cfg: &Cfg) -> Vec<J> {
    if !cfg.bin.exists() {
        return vec![obj(vec![
            ("name", s("overload/setup")),
            ("ok", J::Bool(false)),
            (
                "errors",
                J::Arr(vec![s(format!(
                    "raftdb binary not found at {}",
                    cfg.bin.display()
                ))]),
            ),
        ])];
    }
    let mut out = Vec::new();
    for p in [
        OverloadParams {
            label: "bounded: max-pending-writes=64",
            max_pending: 64,
            max_batch: 32,
            gated: true,
        },
        OverloadParams {
            label: "contrast: max-pending-writes=100000 (effectively unbounded)",
            max_pending: 100_000,
            max_batch: 256,
            gated: false,
        },
    ] {
        out.push(run_bench(
            cfg,
            &format!("overload/pending{}", p.max_pending),
            "durable",
            "accepted_writes_per_sec",
            true,
            obj(vec![
                ("label", s(p.label)),
                ("method", s("open-loop, no client retries; more concurrent requesters than the pending limit; 1 KiB values; leader RSS sampled every 100ms")),
                ("gated", J::Bool(p.gated)),
            ]),
            |_| overload_run(cfg, &p),
        ));
    }
    out
}

/// Tuning experiments: one benchmark per (flag set, concurrency). Same
/// warm-up / runs / read-back verification as every other benchmark.
pub fn sweep(
    cfg: &Cfg,
    sets: &[(String, String)],
    concs: &[u32],
    writes: u64,
    engine_flags: &str,
) -> Vec<J> {
    if !cfg.bin.exists() {
        return vec![obj(vec![
            ("name", s("sweep/setup")),
            ("ok", J::Bool(false)),
            (
                "errors",
                J::Arr(vec![s(format!(
                    "raftdb binary not found at {}",
                    cfg.bin.display()
                ))]),
            ),
        ])];
    }
    let mut out = Vec::new();
    for (label, flags) in sets {
        for &conc in concs {
            let mut extra: Vec<String> =
                engine_flags.split_whitespace().map(String::from).collect();
            extra.extend(flags.split_whitespace().map(String::from));
            out.push(run_bench(
                cfg,
                &format!("sweep/{label}_c{conc}"),
                "durable",
                "writes_per_sec",
                true,
                obj(vec![
                    ("concurrency", nu(conc as u64)),
                    ("writes", nu(writes)),
                    ("node_flags", s(extra.join(" "))),
                    ("sessions", J::Bool(USE_SESSIONS.load(Ordering::Relaxed))),
                    ("data_dir_fs", s(cfg.bench_dir.display().to_string())),
                ]),
                |_| load_run(cfg, &cfg.bench_dir, &extra, conc, writes, 64),
            ));
        }
    }
    out
}
