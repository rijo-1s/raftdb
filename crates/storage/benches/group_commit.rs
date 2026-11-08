//! Group-commit benchmark for the WAL: durably appends the same number of
//! entries (each fsynced) with different batch sizes and reports
//! throughput plus per-commit latency percentiles. Batch size 1 is the
//! unbatched baseline: one fsync per entry.
//!
//! Run: `cargo bench -p storage --bench group_commit`
//! Set `BENCH_DIR` to choose the filesystem (default: the system temp dir).
//! Results depend entirely on the disk's fsync latency - see the README for
//! how to read them.

use std::time::Instant;

use raft_core::LogStore;
use storage::WalLog;

const TOTAL_ENTRIES: usize = 4000;
const PAYLOAD: usize = 100;

fn pct(sorted: &[f64], p: f64) -> f64 {
    sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
}

fn main() {
    let base = std::env::var("BENCH_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir());
    println!(
        "WAL group-commit benchmark: {TOTAL_ENTRIES} entries of {PAYLOAD} bytes, dir = {}",
        base.display()
    );
    println!(
        "{:>6} {:>8} {:>12} {:>14} {:>10} {:>10} {:>10}",
        "batch", "fsyncs", "entries/s", "speedup", "p50 ms", "p95 ms", "p99 ms"
    );

    let mut baseline = 0.0f64;
    for batch in [1usize, 4, 16, 64, 256] {
        let dir = tempfile::tempdir_in(&base).unwrap();
        let mut log = WalLog::open(dir.path().join("node.wal")).unwrap();
        log.save_hard_state(1, None);

        let mut commit_lat_ms: Vec<f64> = Vec::new();
        let start = Instant::now();
        let mut written = 0;
        while written < TOTAL_ENTRIES {
            let n = batch.min(TOTAL_ENTRIES - written);
            let data: Vec<Vec<u8>> = (0..n).map(|_| vec![0xAB; PAYLOAD]).collect();
            let t0 = Instant::now();
            if n == 1 {
                log.append(1, data.into_iter().next().unwrap());
            } else {
                log.append_batch(1, data);
            }
            commit_lat_ms.push(t0.elapsed().as_secs_f64() * 1000.0);
            written += n;
        }
        let total = start.elapsed().as_secs_f64();
        commit_lat_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let eps = TOTAL_ENTRIES as f64 / total;
        if batch == 1 {
            baseline = eps;
        }
        println!(
            "{:>6} {:>8} {:>12.0} {:>13.1}x {:>10.3} {:>10.3} {:>10.3}",
            batch,
            commit_lat_ms.len(),
            eps,
            eps / baseline,
            pct(&commit_lat_ms, 0.50),
            pct(&commit_lat_ms, 0.95),
            pct(&commit_lat_ms, 0.99),
        );
    }
}
