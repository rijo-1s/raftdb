mod cluster;
mod common;
mod compare;
mod engine;
mod json;

use std::path::PathBuf;

use common::{write_results, Cfg};

fn usage() -> ! {
    eprintln!(
        "usage: harness <all|engine|recovery|cluster|overload|compare> [options]\n\
         \n\
         options (run commands):\n\
           --quick            small sizes, 3 runs (smoke test; not for recording)\n\
           --runs N           measured runs per benchmark (default 5, minimum 5 unless --quick)\n\
           --warmup N         discarded warm-up runs (default 1)\n\
           --out DIR          results directory (default bench/results)\n\
           --bench-dir DIR    filesystem for durable modes (default $BENCH_DIR or system temp)\n\
           --cpu-dir DIR      filesystem for CPU-bound modes (default /dev/shm if present)\n\
           --bin PATH         raftdb binary for cluster suites (default target/release/raftdb)\n\
           --sessions         cluster suites: send every write with an exactly-once session\n\
           --drop-caches      drop the page cache before each recovery run (needs root)\n\
         \n\
         sweep --set label=\"node flags\" [--set ...] [--conc 16,64] [--writes N] [--engine-flags \"...\"]\n\
           runs the 3-node cluster load once per flag set (tuning experiments)\n\
         compare --baseline DIR --current DIR [--floor-pct 30]"
    );
    std::process::exit(2)
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let command = args.join(" ");
    if args.len() < 2 {
        usage();
    }
    let sub = args[1].clone();
    let mut quick = false;
    let mut runs: Option<usize> = None;
    let mut warmup = 1usize;
    let mut out = PathBuf::from("bench/results");
    let mut bench_dir: Option<PathBuf> = std::env::var("BENCH_DIR").ok().map(PathBuf::from);
    let mut cpu_dir: Option<PathBuf> = std::env::var("BENCH_CPU_DIR").ok().map(PathBuf::from);
    let mut bin = PathBuf::from(
        std::env::var("RAFTDB_BIN").unwrap_or_else(|_| "target/release/raftdb".into()),
    );
    let mut drop_caches = false;
    let mut baseline: Option<PathBuf> = None;
    let mut current: Option<PathBuf> = None;
    let mut floor = compare::DEFAULT_FLOOR_PCT;
    let mut sets: Vec<(String, String)> = Vec::new();
    let mut conc_list: Vec<u32> = vec![16, 64];
    let mut sweep_writes: Option<u64> = None;
    let mut sweep_engine_flags = String::new();
    let mut i = 2;
    while i < args.len() {
        let flag = args[i].clone();
        let mut val = || {
            i += 1;
            args.get(i).cloned().unwrap_or_else(|| usage())
        };
        match flag.as_str() {
            "--quick" => quick = true,
            "--runs" => runs = Some(val().parse().unwrap_or_else(|_| usage())),
            "--warmup" => warmup = val().parse().unwrap_or_else(|_| usage()),
            "--out" => out = PathBuf::from(val()),
            "--bench-dir" => bench_dir = Some(PathBuf::from(val())),
            "--cpu-dir" => cpu_dir = Some(PathBuf::from(val())),
            "--bin" => bin = PathBuf::from(val()),
            "--drop-caches" => drop_caches = true,
            "--sessions" => cluster::USE_SESSIONS.store(true, std::sync::atomic::Ordering::Relaxed),
            "--baseline" => baseline = Some(PathBuf::from(val())),
            "--current" => current = Some(PathBuf::from(val())),
            "--set" => {
                let v = val();
                let (l, f) = v.split_once('=').unwrap_or((v.as_str(), ""));
                sets.push((l.to_string(), f.to_string()));
            }
            "--conc" => {
                conc_list = val()
                    .split(',')
                    .map(|x| x.parse().unwrap_or_else(|_| usage()))
                    .collect()
            }
            "--writes" => sweep_writes = Some(val().parse().unwrap_or_else(|_| usage())),
            "--engine-flags" => sweep_engine_flags = val(),
            "--floor-pct" => floor = val().parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
        i += 1;
    }

    if sub == "compare" {
        let (Some(b), Some(c)) = (baseline, current) else {
            usage()
        };
        match compare::compare_dirs(&b, &c, floor) {
            Ok(true) => println!("\nNO REGRESSION"),
            Ok(false) => {
                println!("\nREGRESSION OR FAILURE");
                std::process::exit(1)
            }
            Err(e) => {
                eprintln!("compare: {e}");
                std::process::exit(2)
            }
        }
        return;
    }

    let runs = runs.unwrap_or(if quick { 3 } else { 5 });
    if !quick && runs < 5 {
        eprintln!("--runs must be at least 5 (use --quick for smoke tests)");
        std::process::exit(2);
    }
    let cpu_default = PathBuf::from("/dev/shm");
    let cfg = Cfg {
        warmup,
        runs,
        quick,
        out_dir: out,
        bench_dir: bench_dir.unwrap_or_else(std::env::temp_dir),
        cpu_dir: cpu_dir.unwrap_or_else(|| {
            if cpu_default.is_dir() {
                cpu_default
            } else {
                std::env::temp_dir()
            }
        }),
        bin,
        drop_caches,
        command,
    };

    let mut ok = true;
    let want = |s: &str| sub == "all" || sub == s;
    match sub.as_str() {
        "all" | "engine" | "recovery" | "cluster" | "overload" | "sweep" => {}
        _ => usage(),
    }
    if sub == "sweep" {
        if sets.is_empty() {
            eprintln!("sweep needs at least one --set label=\"node flags\"");
            std::process::exit(2);
        }
        let w = sweep_writes.unwrap_or(if cfg.quick { 1000 } else { 6000 });
        ok &= write_results(
            &cfg,
            "sweep",
            cluster::sweep(&cfg, &sets, &conc_list, w, &sweep_engine_flags),
        );
        if !ok {
            std::process::exit(1);
        }
        return;
    }
    if want("engine") {
        ok &= write_results(&cfg, "engine_cpu_bound", engine::cpu_suite(&cfg));
        ok &= write_results(&cfg, "engine_durable", engine::durable_suite(&cfg));
    }
    if want("recovery") {
        ok &= write_results(&cfg, "recovery", engine::recovery_suite(&cfg));
    }
    if want("cluster") {
        ok &= write_results(&cfg, "cluster", cluster::throughput_and_recovery(&cfg));
    }
    if want("overload") {
        ok &= write_results(&cfg, "overload", cluster::overload_suite(&cfg));
    }
    if !ok {
        eprintln!("HARNESS: at least one benchmark failed verification or errored");
        std::process::exit(1);
    }
}
