//! Shared harness plumbing: config, statistics, run orchestration,
//! environment capture.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::json::{n, nu, obj, s, J};

#[derive(Clone)]
pub struct Cfg {
    pub warmup: usize,
    pub runs: usize,
    pub quick: bool,
    pub out_dir: PathBuf,
    /// Filesystem for durable (fsync) modes. Default: system temp dir.
    pub bench_dir: PathBuf,
    /// Filesystem for CPU-bound modes (tmpfs: fsync is a no-op).
    pub cpu_dir: PathBuf,
    pub bin: PathBuf,
    pub drop_caches: bool,
    pub command: String,
}

/// One measured run.
pub struct RunResult {
    /// Throughput (ops/s) or, for recovery benches, milliseconds.
    pub primary: f64,
    pub p50_ms: Option<f64>,
    pub p99_ms: Option<f64>,
    pub disk_bytes: u64,
    /// Keys/entries read back and compared with the expected value.
    pub verified: u64,
    pub pass: bool,
    pub extra: Vec<(String, J)>,
}

impl RunResult {
    pub fn new(primary: f64) -> Self {
        Self {
            primary,
            p50_ms: None,
            p99_ms: None,
            disk_bytes: 0,
            verified: 0,
            pass: true,
            extra: vec![],
        }
    }
}

pub fn median(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mut t = v.to_vec();
    t.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let m = t.len() / 2;
    if t.len() % 2 == 1 {
        t[m]
    } else {
        (t[m - 1] + t[m]) / 2.0
    }
}

/// Spread = (max - min) / median, in percent.
pub fn spread_pct(v: &[f64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    let mx = v.iter().cloned().fold(f64::MIN, f64::max);
    let mn = v.iter().cloned().fold(f64::MAX, f64::min);
    let md = median(v);
    if md == 0.0 {
        0.0
    } else {
        (mx - mn) / md * 100.0
    }
}

pub fn stat(v: &[f64]) -> J {
    obj(vec![
        ("runs", J::Arr(v.iter().map(|x| n(*x)).collect())),
        ("median", n(median(v))),
        ("min", n(v.iter().cloned().fold(f64::MAX, f64::min))),
        ("max", n(v.iter().cloned().fold(f64::MIN, f64::max))),
        ("spread_pct", n(spread_pct(v))),
    ])
}

/// Percentile of an unsorted sample, in the sample's own unit.
pub fn pct(v: &mut [f64], p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[(((v.len() - 1) as f64) * p).round() as usize]
}

/// Runs `f` `warmup` times (discarded) then `runs` times and summarises.
/// Never aborts: a failing run is recorded and fails the whole harness.
pub fn run_bench(
    cfg: &Cfg,
    name: &str,
    mode: &str,
    unit: &str,
    higher_is_better: bool,
    params: J,
    mut f: impl FnMut(usize) -> Result<RunResult, String>,
) -> J {
    eprintln!("[bench] {name} ({mode}) ...");
    let mut errors: Vec<String> = Vec::new();
    for w in 0..cfg.warmup {
        if let Err(e) = f(w) {
            errors.push(format!("warmup {w}: {e}"));
        }
    }
    let mut res: Vec<RunResult> = Vec::new();
    for r in 0..cfg.runs {
        match f(cfg.warmup + r) {
            Ok(x) => {
                if !x.pass {
                    errors.push(format!("run {r}: self-check failed"));
                }
                res.push(x)
            }
            Err(e) => errors.push(format!("run {r}: {e}")),
        }
    }
    let ok = errors.is_empty() && res.len() == cfg.runs;
    let prim: Vec<f64> = res.iter().map(|r| r.primary).collect();
    let mut items = vec![
        ("name", s(name)),
        ("mode", s(mode)),
        ("unit", s(unit)),
        ("higher_is_better", J::Bool(higher_is_better)),
        ("params", params),
        ("warmup_runs", nu(cfg.warmup as u64)),
        ("runs", nu(cfg.runs as u64)),
        ("ok", J::Bool(ok)),
        (
            "errors",
            J::Arr(errors.iter().map(|e| s(e.clone())).collect()),
        ),
        ("primary", stat(&prim)),
    ];
    let p50: Vec<f64> = res.iter().filter_map(|r| r.p50_ms).collect();
    let p99: Vec<f64> = res.iter().filter_map(|r| r.p99_ms).collect();
    if !p50.is_empty() {
        items.push(("latency_p50_ms", stat(&p50)));
        items.push(("latency_p99_ms", stat(&p99)));
    }
    let disk: Vec<f64> = res.iter().map(|r| r.disk_bytes as f64).collect();
    if disk.iter().any(|d| *d > 0.0) {
        items.push(("disk_bytes", stat(&disk)));
    }
    items.push((
        "verified_per_run",
        J::Arr(res.iter().map(|r| nu(r.verified)).collect()),
    ));
    if res.iter().any(|r| !r.extra.is_empty()) {
        items.push((
            "details",
            J::Arr(res.iter().map(|r| J::Obj(r.extra.clone())).collect()),
        ));
    }
    if !ok {
        eprintln!("[bench] {name} FAILED: {errors:?}");
    }
    obj(items)
}

pub fn dir_bytes(p: &Path) -> u64 {
    let mut total = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            if let Ok(m) = e.metadata() {
                if m.is_dir() {
                    total += dir_bytes(&e.path());
                } else {
                    total += m.len();
                }
            }
        }
    }
    total
}

pub fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for e in std::fs::read_dir(from)? {
        let e = e?;
        let t = to.join(e.file_name());
        if e.file_type()?.is_dir() {
            copy_dir(&e.path(), &t)?;
        } else {
            std::fs::copy(e.path(), t)?;
        }
    }
    Ok(())
}

fn sh(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_default()
}

fn file_line(path: &str, key: &str) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .find(|l| l.starts_with(key))
        .map(|l| {
            l.split_once(':')
                .map(|x| x.1)
                .unwrap_or("")
                .trim()
                .to_string()
        })
        .unwrap_or_default()
}

/// What device/filesystem backs `dir`.
fn disk_info(dir: &Path) -> J {
    let d = dir.to_string_lossy().to_string();
    let df = sh("df", &["-PT", &d]);
    let line = df.lines().nth(1).unwrap_or("").to_string();
    let cols: Vec<&str> = line.split_whitespace().collect();
    let dev = cols.first().copied().unwrap_or("").to_string();
    let fstype = cols.get(1).copied().unwrap_or("").to_string();
    let base = dev.trim_start_matches("/dev/").to_string();
    // strip partition suffix to find /sys/block/<disk>
    let disk: String = {
        let b = base.trim_end_matches(|c: char| c.is_ascii_digit());
        if b.is_empty() {
            base.clone()
        } else {
            b.trim_end_matches('p').to_string()
        }
    };
    let rot = std::fs::read_to_string(format!("/sys/block/{disk}/queue/rotational"))
        .unwrap_or_default()
        .trim()
        .to_string();
    let model = std::fs::read_to_string(format!("/sys/block/{disk}/device/model"))
        .unwrap_or_default()
        .trim()
        .to_string();
    obj(vec![
        ("path", s(d)),
        ("device", s(dev)),
        ("filesystem", s(fstype)),
        (
            "rotational",
            s(if rot.is_empty() {
                "unknown".into()
            } else {
                rot
            }),
        ),
        ("model", s(model)),
    ])
}

pub fn meta(cfg: &Cfg, suite: &str) -> J {
    let git_commit = sh("git", &["rev-parse", "HEAD"]);
    let dirty = !sh("git", &["status", "--porcelain"]).is_empty();
    let envs: Vec<(String, J)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("BENCH_") || k.starts_with("RAFTDB_") || k == "RUSTFLAGS")
        .map(|(k, v)| (k, s(v)))
        .collect();
    obj(vec![
        ("suite", s(suite)),
        (
            "timestamp_unix",
            nu(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0)),
        ),
        ("command", s(cfg.command.clone())),
        ("env", J::Obj(envs)),
        (
            "git",
            obj(vec![("commit", s(git_commit)), ("dirty", J::Bool(dirty))]),
        ),
        (
            "machine",
            obj(vec![
                ("cpu_model", s(file_line("/proc/cpuinfo", "model name"))),
                (
                    "cpus",
                    nu(std::thread::available_parallelism()
                        .map(|c| c.get() as u64)
                        .unwrap_or(1)),
                ),
                ("mem_total", s(file_line("/proc/meminfo", "MemTotal"))),
                ("kernel", s(sh("uname", &["-r"]))),
                ("os", s(sh("uname", &["-sm"]))),
            ]),
        ),
        (
            "disk",
            obj(vec![
                ("durable_dir", disk_info(&cfg.bench_dir)),
                ("cpu_dir", disk_info(&cfg.cpu_dir)),
            ]),
        ),
        (
            "toolchain",
            obj(vec![
                ("rustc", s(sh("rustc", &["--version"]))),
                (
                    "profile",
                    s(if cfg!(debug_assertions) {
                        "debug"
                    } else {
                        "release"
                    }),
                ),
            ]),
        ),
        (
            "protocol",
            obj(vec![
                ("warmup_runs", nu(cfg.warmup as u64)),
                ("measured_runs", nu(cfg.runs as u64)),
                ("quick_mode", J::Bool(cfg.quick)),
                (
                    "spread_definition",
                    s("(max-min)/median in percent over measured runs"),
                ),
                (
                    "page_cache",
                    s(if cfg.drop_caches {
                        "dropped before each recovery run (if permitted)"
                    } else {
                        "warm"
                    }),
                ),
            ]),
        ),
    ])
}

/// Best-effort page-cache drop (needs root). Returns whether it worked.
pub fn drop_caches() -> bool {
    let _ = Command::new("sync").status();
    std::fs::write("/proc/sys/vm/drop_caches", "3").is_ok()
}

pub fn write_results(cfg: &Cfg, suite: &str, benches: Vec<J>) -> bool {
    let all_ok = benches
        .iter()
        .all(|b| matches!(b.get("ok"), Some(J::Bool(true))));
    let doc = obj(vec![
        ("meta", meta(cfg, suite)),
        ("all_ok", J::Bool(all_ok)),
        ("benchmarks", J::Arr(benches)),
    ]);
    std::fs::create_dir_all(&cfg.out_dir).expect("create results dir");
    let path = cfg.out_dir.join(format!("{suite}.json"));
    std::fs::write(&path, doc.pretty()).expect("write results");
    eprintln!("[bench] wrote {} (all_ok={all_ok})", path.display());
    all_ok
}
