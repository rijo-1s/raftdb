//! Metrics, health and readiness for one node.
//!
//! Hand-rolled rather than pulling in a metrics crate and exporter: the
//! Prometheus text format is plain text, and this keeps the dependency set
//! small. Everything is std plus the tokio the node already uses.
//!
//! All state is atomics, so `Metrics` is shared via `Arc` between the node
//! loop (which updates it) and the HTTP server (which reads it) without any
//! locking on the hot path.
//!
//! Endpoints (all on the `--metrics-addr` listener):
//!   `GET /metrics`  Prometheus exposition
//!   `GET /healthz`  liveness  - 200 while the node loop is making progress
//!   `GET /readyz`   readiness - 200 only while the node can serve or redirect
//! See `docs/observability.md` for the catalogue and the probe semantics.

// Hooks (`observe_*`, `set_leader_known`, `record_lsm`, ...) are called from
// `node_loop.rs`, `server.rs` and `main.rs`; the few not wired yet are marked "reserved".
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------

/// Latency bucket upper bounds, microseconds: 100us .. 10s.
pub const LATENCY_BOUNDS_MICROS: &[u64] = &[
    100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000,
    1_000_000, 2_500_000, 5_000_000, 10_000_000,
];
/// Batch-size bucket upper bounds (writes per group-commit batch).
pub const SIZE_BOUNDS: &[u64] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];

/// A lock-free fixed-bucket histogram. `observe` is a few relaxed atomic
/// adds. Values are `u64` in the histogram's own unit (microseconds for
/// latencies, plain counts for sizes); `scale` divides them at render time
/// (1e6 turns microseconds into the seconds Prometheus expects).
pub struct Histogram {
    bounds: &'static [u64],
    /// Per-bucket (NOT cumulative) counts; one extra slot for +Inf.
    buckets: Vec<AtomicU64>,
    sum: AtomicU64,
    count: AtomicU64,
}

/// A consistent-enough copy of a histogram (each field read once).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistogramSnapshot {
    pub bounds: &'static [u64],
    pub buckets: Vec<u64>,
    pub sum: u64,
    pub count: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self::with_bounds(LATENCY_BOUNDS_MICROS)
    }
}

impl Histogram {
    pub fn with_bounds(bounds: &'static [u64]) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Record one value. `le` buckets are inclusive: a value equal to a
    /// bound lands in that bound's bucket.
    pub fn observe(&self, value: u64) {
        let idx = self
            .bounds
            .iter()
            .position(|&b| value <= b)
            .unwrap_or(self.bounds.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_duration(&self, d: Duration) {
        self.observe(d.as_micros().min(u64::MAX as u128) as u64);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            bounds: self.bounds,
            buckets: self
                .buckets
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            sum: self.sum.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

impl HistogramSnapshot {
    /// Smallest bucket upper bound that covers at least fraction `q` of the
    /// observations (`None` when empty, or when the quantile falls in the
    /// +Inf bucket). An upper estimate, like `histogram_quantile`'s bucket.
    pub fn quantile_upper_bound(&self, q: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let need = ((self.count as f64) * q.clamp(0.0, 1.0)).ceil().max(1.0) as u64;
        let mut cum = 0;
        for (i, c) in self.buckets.iter().enumerate() {
            cum += c;
            if cum >= need {
                return self.bounds.get(i).copied();
            }
        }
        None
    }
}

/// Render one histogram in exposition format (`_bucket` series are
/// cumulative; `+Inf` equals `_count`).
fn render_histogram(
    out: &mut String,
    name: &str,
    help: &str,
    node: u64,
    h: &HistogramSnapshot,
    scale: f64,
) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
    let mut cum = 0u64;
    for (i, b) in h.bounds.iter().enumerate() {
        cum += h.buckets[i];
        out.push_str(&format!(
            "{name}_bucket{{node=\"{node}\",le=\"{}\"}} {cum}\n",
            *b as f64 / scale
        ));
    }
    cum += h.buckets[h.bounds.len()];
    out.push_str(&format!(
        "{name}_bucket{{node=\"{node}\",le=\"+Inf\"}} {cum}\n"
    ));
    out.push_str(&format!(
        "{name}_sum{{node=\"{node}\"}} {}\n",
        h.sum as f64 / scale
    ));
    out.push_str(&format!("{name}_count{{node=\"{node}\"}} {}\n", h.count));
}

// ---------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------

/// Does the node know who the leader is?
const LEADER_UNWIRED: u8 = 0;
const LEADER_KNOWN: u8 = 1;
const LEADER_UNKNOWN: u8 = 2;

/// Tunables for the probes.
pub struct HealthConfig {
    /// How long the node loop may go without a heartbeat before `/healthz`
    /// fails. The loop beats every ~50ms, so this only trips on a real stall
    /// (a hung fsync, a deadlock).
    pub heartbeat_timeout: Duration,
    /// A node that has never beaten is "starting" (alive, not ready) for this
    /// long after process start, then declared dead.
    pub startup_grace: Duration,
    /// `/readyz` fails when `commit_index - applied_index` exceeds this.
    pub max_apply_lag: u64,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout: Duration::from_secs(10),
            startup_grace: Duration::from_secs(60),
            max_apply_lag: 10_000,
        }
    }
}

/// The result of one probe: overall verdict plus every individual check, so
/// an operator can see *why* a node is unready without reading logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub ok: bool,
    /// "ok", "starting", "draining" or "failing".
    pub status: &'static str,
    pub checks: Vec<Check>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    pub name: &'static str,
    pub ok: bool,
    pub detail: String,
}

fn json_escape(s: &str) -> String {
    let mut o = String::with_capacity(s.len() + 2);
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o
}

impl Report {
    pub fn to_json(&self, node: u64) -> String {
        let checks: Vec<String> = self
            .checks
            .iter()
            .map(|c| {
                format!(
                    "\"{}\":{{\"ok\":{},\"detail\":\"{}\"}}",
                    json_escape(c.name),
                    c.ok,
                    json_escape(&c.detail)
                )
            })
            .collect();
        format!(
            "{{\"node\":{node},\"status\":\"{}\",\"ok\":{},\"checks\":{{{}}}}}",
            self.status,
            self.ok,
            checks.join(",")
        )
    }
}

pub struct Metrics {
    pub leader_changes_total: AtomicU64,
    pub elections_started_total: AtomicU64,
    pub puts_total: AtomicU64,
    pub gets_total: AtomicU64,
    pub deletes_total: AtomicU64,
    pub writes_rejected_not_leader_total: AtomicU64,
    pub reads_confirmed_total: AtomicU64,
    pub snapshots_taken_total: AtomicU64,
    pub install_snapshots_received_total: AtomicU64,
    /// Group-commit batches proposed, and the writes/bytes they carried
    /// (writes / batches = average batch size).
    pub batches_flushed_total: AtomicU64,
    pub batched_writes_total: AtomicU64,
    pub batched_bytes_total: AtomicU64,
    pub writes_overloaded_total: AtomicU64,
    pub writes_timed_out_total: AtomicU64,
    pub writes_dropped_disconnected_total: AtomicU64,

    // Gauges (current value, not cumulative).
    pub current_term: AtomicU64,
    pub commit_index: AtomicU64,
    pub log_last_index: AtomicU64,
    /// 1 if this node currently believes it's leader, else 0.
    pub is_leader: AtomicU64,
    /// Writes queued or proposed-but-not-yet-applied.
    pub pending_writes: AtomicU64,

    // ---- Latencies and sizes (histograms) --------------------------------
    /// Client write: accepted by the node loop -> applied and acknowledged.
    pub write_latency: Histogram,
    /// Linearizable read: accepted -> ReadIndex confirmed and answered.
    pub read_latency: Histogram,
    /// Time to apply one batch of committed entries to the state machine.
    pub apply_latency: Histogram,
    /// Writes per group-commit batch.
    pub batch_size: Histogram,
    /// Wall time of one LSM compaction round.
    pub compaction_duration: Histogram,

    // ---- Queue depths and progress (gauges) ------------------------------
    /// Highest log index applied to the state machine.
    pub applied_index: AtomicU64,
    applied_wired: AtomicBool,
    /// Linearizable reads waiting for their ReadIndex confirmation.
    pub pending_reads: AtomicU64,
    /// Events queued for the node loop and not yet picked up.
    pub inbox_depth: AtomicU64,

    // ---- node certificate expiry, set once at startup ---------------------
    cert_wired: AtomicBool,
    cert_not_after: std::sync::atomic::AtomicI64,

    // ---- data-volume free space, refreshed by `record_disk` ----------------
    disk_wired: AtomicBool,
    pub disk_free_bytes: AtomicU64,
    pub disk_min_free_bytes: AtomicU64,
    pub writes_protected: AtomicU64,
    pub writes_disk_full_total: AtomicU64,

    // ---- LSM engine, refreshed by `record_lsm` ---------------------------
    lsm_wired: AtomicBool,
    pub lsm_l0_files: AtomicU64,
    pub lsm_l1_files: AtomicU64,
    pub lsm_sst_bytes: AtomicU64,
    pub lsm_disk_bytes: AtomicU64,
    pub lsm_memtable_bytes: AtomicU64,
    pub lsm_flushes_total: AtomicU64,
    pub lsm_compactions_total: AtomicU64,
    pub lsm_cache_hits_total: AtomicU64,
    pub lsm_cache_misses_total: AtomicU64,
    /// 1 once the engine refused further work after an I/O error.
    pub lsm_poisoned: AtomicU64,

    // ---- Health ----------------------------------------------------------
    epoch: Instant,
    /// Milliseconds since `epoch` of the last node-loop heartbeat, plus one
    /// (0 = never beaten).
    last_beat_ms: AtomicU64,
    leader_state: AtomicU8,
    draining: AtomicBool,
    storage_failed: AtomicBool,
    health_cfg: HealthConfig,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::with_health_config(HealthConfig::default())
    }
}

impl Metrics {
    pub fn with_health_config(cfg: HealthConfig) -> Self {
        Self {
            leader_changes_total: AtomicU64::new(0),
            elections_started_total: AtomicU64::new(0),
            puts_total: AtomicU64::new(0),
            gets_total: AtomicU64::new(0),
            deletes_total: AtomicU64::new(0),
            writes_rejected_not_leader_total: AtomicU64::new(0),
            reads_confirmed_total: AtomicU64::new(0),
            snapshots_taken_total: AtomicU64::new(0),
            install_snapshots_received_total: AtomicU64::new(0),
            batches_flushed_total: AtomicU64::new(0),
            batched_writes_total: AtomicU64::new(0),
            batched_bytes_total: AtomicU64::new(0),
            writes_overloaded_total: AtomicU64::new(0),
            writes_timed_out_total: AtomicU64::new(0),
            writes_dropped_disconnected_total: AtomicU64::new(0),
            current_term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            log_last_index: AtomicU64::new(0),
            is_leader: AtomicU64::new(0),
            pending_writes: AtomicU64::new(0),
            write_latency: Histogram::default(),
            read_latency: Histogram::default(),
            apply_latency: Histogram::default(),
            batch_size: Histogram::with_bounds(SIZE_BOUNDS),
            compaction_duration: Histogram::default(),
            applied_index: AtomicU64::new(0),
            applied_wired: AtomicBool::new(false),
            pending_reads: AtomicU64::new(0),
            inbox_depth: AtomicU64::new(0),
            cert_wired: AtomicBool::new(false),
            cert_not_after: std::sync::atomic::AtomicI64::new(0),
            disk_wired: AtomicBool::new(false),
            disk_free_bytes: AtomicU64::new(0),
            disk_min_free_bytes: AtomicU64::new(0),
            writes_protected: AtomicU64::new(0),
            writes_disk_full_total: AtomicU64::new(0),
            lsm_wired: AtomicBool::new(false),
            lsm_l0_files: AtomicU64::new(0),
            lsm_l1_files: AtomicU64::new(0),
            lsm_sst_bytes: AtomicU64::new(0),
            lsm_disk_bytes: AtomicU64::new(0),
            lsm_memtable_bytes: AtomicU64::new(0),
            lsm_flushes_total: AtomicU64::new(0),
            lsm_compactions_total: AtomicU64::new(0),
            lsm_cache_hits_total: AtomicU64::new(0),
            lsm_cache_misses_total: AtomicU64::new(0),
            lsm_poisoned: AtomicU64::new(0),
            epoch: Instant::now(),
            last_beat_ms: AtomicU64::new(0),
            leader_state: AtomicU8::new(LEADER_UNWIRED),
            draining: AtomicBool::new(false),
            storage_failed: AtomicBool::new(false),
            health_cfg: cfg,
        }
    }

    // ---- Hooks for the node loop (see docs/observability.md) -------------

    pub fn observe_write_latency(&self, d: Duration) {
        self.write_latency.observe_duration(d);
    }
    pub fn observe_read_latency(&self, d: Duration) {
        self.read_latency.observe_duration(d);
    }
    pub fn observe_apply_latency(&self, d: Duration) {
        self.apply_latency.observe_duration(d);
    }
    pub fn observe_compaction(&self, d: Duration) {
        self.compaction_duration.observe_duration(d);
    }
    pub fn set_applied_index(&self, v: u64) {
        self.applied_wired.store(true, Ordering::Relaxed);
        Self::set(&self.applied_index, v);
    }
    pub fn set_pending_reads(&self, v: u64) {
        Self::set(&self.pending_reads, v);
    }
    pub fn set_inbox_depth(&self, v: u64) {
        Self::set(&self.inbox_depth, v);
    }
    /// Record whether this node currently knows a leader (itself or another).
    pub fn set_leader_known(&self, known: bool) {
        self.leader_state.store(
            if known { LEADER_KNOWN } else { LEADER_UNKNOWN },
            Ordering::Relaxed,
        );
    }
    /// Call when shutdown begins: `/readyz` goes 503 immediately so traffic
    /// drains, while `/healthz` stays 200 until the process actually exits.
    pub fn begin_shutdown(&self) {
        self.draining.store(true, Ordering::Relaxed);
    }
    /// An unrecoverable storage failure (the handle is poisoned): the node
    /// refuses all work until restarted, so both probes fail.
    pub fn set_storage_failed(&self, failed: bool) {
        self.storage_failed.store(failed, Ordering::Relaxed);
    }

    /// Mirror the LSM engine's counters. Call periodically with
    /// `lsm.stats()` and `lsm.is_poisoned()`. Counters never go backwards.
    pub fn record_lsm(&self, st: &storage::lsm::Stats, poisoned: bool) {
        self.lsm_wired.store(true, Ordering::Relaxed);
        Self::set(&self.lsm_l0_files, st.l0_files as u64);
        Self::set(&self.lsm_l1_files, st.l1_files as u64);
        Self::set(&self.lsm_sst_bytes, st.sst_bytes);
        Self::set(&self.lsm_disk_bytes, st.disk_bytes);
        Self::set(&self.lsm_memtable_bytes, st.memtable_bytes as u64);
        self.lsm_flushes_total
            .fetch_max(st.flushes, Ordering::Relaxed);
        self.lsm_compactions_total
            .fetch_max(st.compactions, Ordering::Relaxed);
        self.lsm_cache_hits_total
            .fetch_max(st.cache_hits, Ordering::Relaxed);
        self.lsm_cache_misses_total
            .fetch_max(st.cache_misses, Ordering::Relaxed);
        Self::set(&self.lsm_poisoned, poisoned as u64);
        if poisoned {
            self.set_storage_failed(true);
        }
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis().min(u64::MAX as u128 - 1) as u64
    }

    /// Stamp "the node loop is alive". Called from the existing gauge
    /// setters, which the loop runs every tick, so no extra call is needed.
    pub fn beat(&self) {
        self.last_beat_ms
            .store(self.now_ms() + 1, Ordering::Relaxed);
    }

    // ---- Probes ----------------------------------------------------------

    /// Liveness at an explicit clock reading (milliseconds since start); the
    /// pure form is what the tests drive.
    pub fn liveness_at(&self, now_ms: u64) -> Report {
        let beat = self.last_beat_ms.load(Ordering::Relaxed);
        let timeout = self.health_cfg.heartbeat_timeout.as_millis() as u64;
        let grace = self.health_cfg.startup_grace.as_millis() as u64;
        let (hb_ok, hb_detail, starting) = if beat == 0 {
            if now_ms < grace {
                (
                    true,
                    format!("node loop not started yet ({now_ms}ms since process start)"),
                    true,
                )
            } else {
                (
                    false,
                    format!("node loop never started ({now_ms}ms since process start)"),
                    false,
                )
            }
        } else {
            let age = now_ms.saturating_sub(beat - 1);
            (
                age <= timeout,
                format!("last heartbeat {age}ms ago (limit {timeout}ms)"),
                false,
            )
        };
        let storage_ok = !self.storage_failed.load(Ordering::Relaxed);
        let ok = hb_ok && storage_ok;
        Report {
            ok,
            status: if !ok {
                "failing"
            } else if starting {
                "starting"
            } else {
                "ok"
            },
            checks: vec![
                Check {
                    name: "node_loop_heartbeat",
                    ok: hb_ok,
                    detail: hb_detail,
                },
                Check {
                    name: "storage",
                    ok: storage_ok,
                    detail: if storage_ok {
                        "healthy".into()
                    } else {
                        "storage failed; restart required".into()
                    },
                },
            ],
        }
    }

    /// Readiness: can this node serve a client, or redirect it to the leader?
    pub fn readiness_at(&self, now_ms: u64) -> Report {
        let live = self.liveness_at(now_ms);
        let started = self.last_beat_ms.load(Ordering::Relaxed) != 0;
        let draining = self.draining.load(Ordering::Relaxed);
        let mut checks = vec![
            Check {
                name: "started",
                ok: started,
                detail: if started {
                    "node loop running".into()
                } else {
                    "node loop not started".into()
                },
            },
            Check {
                name: "alive",
                ok: live.ok,
                detail: if live.ok {
                    "liveness ok".into()
                } else {
                    "liveness failing".into()
                },
            },
            Check {
                name: "not_draining",
                ok: !draining,
                detail: if draining {
                    "shutting down".into()
                } else {
                    "accepting traffic".into()
                },
            },
        ];
        // Only enforced once the node loop reports it (otherwise an
        // un-instrumented build could never become ready).
        match self.leader_state.load(Ordering::Relaxed) {
            LEADER_KNOWN => checks.push(Check {
                name: "leader_known",
                ok: true,
                detail: "a leader is known".into(),
            }),
            LEADER_UNKNOWN => checks.push(Check {
                name: "leader_known",
                ok: false,
                detail: "no leader known (election in progress or partitioned)".into(),
            }),
            _ => {}
        }
        if self.applied_wired.load(Ordering::Relaxed) {
            let commit = self.commit_index.load(Ordering::Relaxed);
            let applied = self.applied_index.load(Ordering::Relaxed);
            let lag = commit.saturating_sub(applied);
            let max = self.health_cfg.max_apply_lag;
            checks.push(Check {
                name: "apply_lag",
                ok: lag <= max,
                detail: format!("commit {commit} - applied {applied} = {lag} (limit {max})"),
            });
        }
        let ok = checks.iter().all(|c| c.ok);
        let status = if ok {
            "ok"
        } else if draining {
            "draining"
        } else if !started {
            "starting"
        } else {
            "failing"
        };
        Report { ok, status, checks }
    }

    pub fn liveness(&self) -> Report {
        self.liveness_at(self.now_ms())
    }
    pub fn readiness(&self) -> Report {
        self.readiness_at(self.now_ms())
    }

    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    fn set(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    pub fn inc_leader_changes(&self) {
        Self::inc(&self.leader_changes_total);
    }
    #[allow(dead_code)] // reserved: no cheap hook point yet for election starts specifically
    pub fn inc_elections_started(&self) {
        Self::inc(&self.elections_started_total);
    }
    pub fn inc_puts(&self) {
        Self::inc(&self.puts_total);
    }
    pub fn inc_gets(&self) {
        Self::inc(&self.gets_total);
    }
    pub fn inc_deletes(&self) {
        Self::inc(&self.deletes_total);
    }
    pub fn inc_writes_rejected(&self) {
        Self::inc(&self.writes_rejected_not_leader_total);
    }
    pub fn inc_reads_confirmed(&self) {
        Self::inc(&self.reads_confirmed_total);
    }
    #[allow(dead_code)] // reserved: KvNode's compaction is internal to apply_ready(); would need a small kv-crate API addition to hook cheaply
    pub fn inc_snapshots_taken(&self) {
        Self::inc(&self.snapshots_taken_total);
    }
    #[allow(dead_code)] // reserved, same reason as inc_snapshots_taken
    pub fn inc_install_snapshots_received(&self) {
        Self::inc(&self.install_snapshots_received_total);
    }

    pub fn add_batch(&self, writes: u64, bytes: u64) {
        self.batch_size.observe(writes);
        self.batches_flushed_total.fetch_add(1, Ordering::Relaxed);
        self.batched_writes_total
            .fetch_add(writes, Ordering::Relaxed);
        self.batched_bytes_total.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record when the node certificate expires (Unix seconds).
    pub fn record_cert_expiry(&self, not_after_unix: i64) {
        self.cert_not_after.store(not_after_unix, Ordering::Relaxed);
        self.cert_wired.store(true, Ordering::Relaxed);
    }

    /// Mirror the data-volume reading. Call whenever the volume is sampled.
    pub fn record_disk(&self, free: u64, min_free: u64, protected: bool) {
        self.disk_wired.store(true, Ordering::Relaxed);
        Self::set(&self.disk_free_bytes, free);
        Self::set(&self.disk_min_free_bytes, min_free);
        Self::set(&self.writes_protected, protected as u64);
    }

    pub fn inc_writes_disk_full(&self) {
        Self::inc(&self.writes_disk_full_total);
    }

    pub fn inc_writes_overloaded(&self) {
        Self::inc(&self.writes_overloaded_total);
    }

    pub fn inc_writes_timed_out(&self) {
        Self::inc(&self.writes_timed_out_total);
    }

    pub fn add_writes_dropped_disconnected(&self, n: u64) {
        self.writes_dropped_disconnected_total
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_pending_writes(&self, v: u64) {
        self.beat();
        Self::set(&self.pending_writes, v);
    }

    pub fn set_term(&self, v: u64) {
        self.beat();
        Self::set(&self.current_term, v);
    }
    pub fn set_commit_index(&self, v: u64) {
        self.beat();
        Self::set(&self.commit_index, v);
    }
    pub fn set_log_last_index(&self, v: u64) {
        self.beat();
        Self::set(&self.log_last_index, v);
    }
    pub fn set_is_leader(&self, leader: bool) {
        self.beat();
        Self::set(&self.is_leader, if leader { 1 } else { 0 });
    }

    fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    /// Render in Prometheus's plain-text exposition format.
    pub fn render(&self, node_id: u64) -> String {
        let labels = format!("{{node=\"{node_id}\"}}");
        let mut out = String::new();

        let counter = |out: &mut String, name: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name}{labels} {value}\n"
            ));
        };
        let gauge = |out: &mut String, name: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name}{labels} {value}\n"
            ));
        };

        counter(
            &mut out,
            "raft_leader_changes_total",
            "Times this node has become leader",
            Self::get(&self.leader_changes_total),
        );
        counter(
            &mut out,
            "raft_elections_started_total",
            "Elections this node has started",
            Self::get(&self.elections_started_total),
        );
        counter(
            &mut out,
            "kv_puts_total",
            "PUT requests handled",
            Self::get(&self.puts_total),
        );
        counter(
            &mut out,
            "kv_gets_total",
            "GET requests handled",
            Self::get(&self.gets_total),
        );
        counter(
            &mut out,
            "kv_deletes_total",
            "DELETE requests handled",
            Self::get(&self.deletes_total),
        );
        counter(
            &mut out,
            "kv_writes_rejected_not_leader_total",
            "Writes rejected because this node wasn't leader",
            Self::get(&self.writes_rejected_not_leader_total),
        );
        counter(
            &mut out,
            "raft_reads_confirmed_total",
            "Linearizable reads confirmed via ReadIndex",
            Self::get(&self.reads_confirmed_total),
        );
        counter(
            &mut out,
            "raft_snapshots_taken_total",
            "Log compactions performed by this node",
            Self::get(&self.snapshots_taken_total),
        );
        counter(
            &mut out,
            "raft_install_snapshots_received_total",
            "InstallSnapshot RPCs received and applied",
            Self::get(&self.install_snapshots_received_total),
        );

        counter(
            &mut out,
            "kv_write_batches_total",
            "Group-commit batches proposed (one WAL fsync each)",
            Self::get(&self.batches_flushed_total),
        );
        counter(
            &mut out,
            "kv_batched_writes_total",
            "Client writes proposed through group-commit batches",
            Self::get(&self.batched_writes_total),
        );
        counter(
            &mut out,
            "kv_batched_bytes_total",
            "Command bytes proposed through group-commit batches",
            Self::get(&self.batched_bytes_total),
        );
        counter(
            &mut out,
            "kv_writes_overloaded_total",
            "Writes shed because too many were already in flight",
            Self::get(&self.writes_overloaded_total),
        );
        counter(
            &mut out,
            "kv_writes_timed_out_total",
            "Proposed writes that timed out before being applied (outcome unknown)",
            Self::get(&self.writes_timed_out_total),
        );
        counter(
            &mut out,
            "kv_writes_dropped_disconnected_total",
            "Queued writes dropped because the client disconnected before proposal",
            Self::get(&self.writes_dropped_disconnected_total),
        );
        gauge(
            &mut out,
            "kv_pending_writes",
            "Writes queued or proposed but not yet applied",
            Self::get(&self.pending_writes),
        );

        gauge(
            &mut out,
            "raft_current_term",
            "This node's current term",
            Self::get(&self.current_term),
        );
        gauge(
            &mut out,
            "raft_commit_index",
            "This node's current commit index",
            Self::get(&self.commit_index),
        );
        gauge(
            &mut out,
            "raft_log_last_index",
            "This node's current log length",
            Self::get(&self.log_last_index),
        );
        gauge(
            &mut out,
            "raft_is_leader",
            "1 if this node currently believes it's leader",
            Self::get(&self.is_leader),
        );

        self.render_extra(&mut out, node_id);
        out
    }

    /// Histograms, queue depths, LSM/compaction, WAL fsync and health. Kept
    /// apart from the basic counters and gauges.
    fn render_extra(&self, out: &mut String, node: u64) {
        let labels = format!("{{node=\"{node}\"}}");
        let counter = |out: &mut String, name: &str, help: &str, value: String| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name}{labels} {value}\n"
            ));
        };
        let gauge = |out: &mut String, name: &str, help: &str, value: String| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} gauge\n{name}{labels} {value}\n"
            ));
        };
        let g = |a: &AtomicU64| Self::get(a).to_string();

        render_histogram(
            out,
            "kv_write_latency_seconds",
            "Client write: accepted -> applied and acknowledged",
            node,
            &self.write_latency.snapshot(),
            1e6,
        );
        render_histogram(
            out,
            "kv_read_latency_seconds",
            "Linearizable read: accepted -> ReadIndex confirmed and answered",
            node,
            &self.read_latency.snapshot(),
            1e6,
        );
        render_histogram(
            out,
            "raft_apply_latency_seconds",
            "Time to apply a batch of committed entries to the state machine",
            node,
            &self.apply_latency.snapshot(),
            1e6,
        );
        render_histogram(
            out,
            "kv_write_batch_size",
            "Client writes per group-commit batch",
            node,
            &self.batch_size.snapshot(),
            1.0,
        );

        // Only once the node loop reports an applied index: until then
        // `applied_index` is a meaningless 0 and the "lag" would equal the
        // whole commit index (a false alarm on a healthy node).
        if self.applied_wired.load(Ordering::Relaxed) {
            gauge(
                out,
                "raft_applied_index",
                "Highest log index applied to the state machine",
                g(&self.applied_index),
            );
            let lag = Self::get(&self.commit_index).saturating_sub(Self::get(&self.applied_index));
            gauge(
                out,
                "raft_apply_lag",
                "Committed but not yet applied entries (commit_index - applied_index)",
                lag.to_string(),
            );
        }
        gauge(
            out,
            "kv_pending_reads",
            "Linearizable reads waiting for ReadIndex confirmation",
            g(&self.pending_reads),
        );
        gauge(
            out,
            "kv_inbox_depth",
            "Events queued for the node loop and not yet picked up",
            g(&self.inbox_depth),
        );

        // WAL: pulled from storage's process-wide counters at scrape time.
        let w = storage::Wal::fsync_stats();
        let wal_hist = HistogramSnapshot {
            bounds: w.bounds_micros,
            buckets: w.bucket_counts.clone(),
            sum: w.sum_micros,
            count: w.count,
        };
        render_histogram(
            out,
            "raft_wal_fsync_seconds",
            "Latency of the fsync that makes a WAL append durable (one per group-commit batch)",
            node,
            &wal_hist,
            1e6,
        );
        gauge(
            out,
            "raft_wal_fsync_max_seconds",
            "Slowest WAL fsync since process start",
            format!("{}", w.max_micros as f64 / 1e6),
        );
        counter(
            out,
            "raft_wal_appended_entries_total",
            "Raft log entries made durable",
            w.entries.to_string(),
        );
        counter(
            out,
            "raft_wal_appended_bytes_total",
            "Bytes made durable in the Raft WAL",
            w.bytes.to_string(),
        );
        counter(
            out,
            "raft_wal_fsync_errors_total",
            "WAL appends whose write or fsync failed (each poisons the WAL)",
            w.errors.to_string(),
        );
        counter(
            out,
            "raft_wal_rewrites_total",
            "Atomic WAL rewrites (log truncation / compaction)",
            w.rewrites.to_string(),
        );
        counter(
            out,
            "raft_wal_rewrite_seconds_total",
            "Total time spent in WAL rewrites",
            format!("{}", w.rewrite_micros as f64 / 1e6),
        );

        // LSM engine (only when the node loop has reported it).
        if self.lsm_wired.load(Ordering::Relaxed) {
            gauge(
                out,
                "lsm_l0_files",
                "SSTables at level 0 (compaction backlog)",
                g(&self.lsm_l0_files),
            );
            gauge(
                out,
                "lsm_l1_files",
                "SSTables at level 1",
                g(&self.lsm_l1_files),
            );
            gauge(
                out,
                "lsm_sst_bytes",
                "Bytes in live SSTables",
                g(&self.lsm_sst_bytes),
            );
            gauge(
                out,
                "lsm_disk_bytes",
                "Bytes the engine occupies on disk",
                g(&self.lsm_disk_bytes),
            );
            gauge(
                out,
                "lsm_memtable_bytes",
                "Bytes in the active memtable",
                g(&self.lsm_memtable_bytes),
            );
            counter(
                out,
                "lsm_flushes_total",
                "Memtable flushes to SSTables",
                g(&self.lsm_flushes_total),
            );
            counter(
                out,
                "lsm_compactions_total",
                "Compaction rounds completed",
                g(&self.lsm_compactions_total),
            );
            counter(
                out,
                "lsm_cache_hits_total",
                "Block cache hits",
                g(&self.lsm_cache_hits_total),
            );
            counter(
                out,
                "lsm_cache_misses_total",
                "Block cache misses",
                g(&self.lsm_cache_misses_total),
            );
            gauge(
                out,
                "lsm_poisoned",
                "1 if the engine refused further work after an I/O error",
                g(&self.lsm_poisoned),
            );
        }
        render_histogram(
            out,
            "lsm_compaction_seconds",
            "Wall time of one compaction round",
            node,
            &self.compaction_duration.snapshot(),
            1e6,
        );

        // Certificate expiry (only on a node started with its own certificate).
        if self.cert_wired.load(Ordering::Relaxed) {
            let not_after = self.cert_not_after.load(Ordering::Relaxed);
            gauge(
                out,
                "tls_cert_not_after_seconds",
                "Unix time at which the node certificate expires",
                not_after.to_string(),
            );
            gauge(
                out,
                "tls_cert_expires_in_seconds",
                "Seconds until the node certificate expires (negative = already expired). Alert well before 0.",
                (not_after - crate::certinfo::now_unix()).to_string(),
            );
        }

        // Data volume (only once the node loop has sampled it).
        if self.disk_wired.load(Ordering::Relaxed) {
            gauge(
                out,
                "node_disk_free_bytes",
                "Bytes available to unprivileged writers on the data volume",
                g(&self.disk_free_bytes),
            );
            gauge(
                out,
                "node_disk_min_free_bytes",
                "Configured free-space floor (--min-free-mb); 0 = protection off",
                g(&self.disk_min_free_bytes),
            );
            gauge(
                out,
                "node_writes_protected",
                "1 while client writes are rejected because free space is below the floor",
                g(&self.writes_protected),
            );
            counter(
                out,
                "kv_writes_disk_full_total",
                "Client writes rejected because the data volume is nearly full",
                g(&self.writes_disk_full_total),
            );
        }

        // Health, as numbers an alert can use.
        let now = self.now_ms();
        gauge(
            out,
            "node_healthy",
            "1 if /healthz would return 200",
            (self.liveness_at(now).ok as u8).to_string(),
        );
        gauge(
            out,
            "node_ready",
            "1 if /readyz would return 200",
            (self.readiness_at(now).ok as u8).to_string(),
        );
        let beat = self.last_beat_ms.load(Ordering::Relaxed);
        gauge(
            out,
            "node_loop_heartbeat_age_seconds",
            "Seconds since the node loop last reported in (-1 = never)",
            if beat == 0 {
                "-1".to_string()
            } else {
                format!("{}", now.saturating_sub(beat - 1) as f64 / 1000.0)
            },
        );
        gauge(
            out,
            "node_uptime_seconds",
            "Seconds since this process started",
            format!("{}", now as f64 / 1000.0),
        );
    }
}

/// Binds `addr` and serves the observability endpoints until the process
/// exits; see [`serve_on`].
pub async fn serve(addr: std::net::SocketAddr, node_id: u64, metrics: std::sync::Arc<Metrics>) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("metrics server failed to bind {addr}: {e}");
            return;
        }
    };
    serve_on(listener, node_id, metrics).await
}

/// The same endpoints over HTTPS (TLS from `--tls-cert` / `--tls-key`). A TLS
/// handshake that does not finish within a few seconds is dropped.
pub async fn serve_tls(
    addr: std::net::SocketAddr,
    node_id: u64,
    metrics: std::sync::Arc<Metrics>,
    config: std::sync::Arc<rustls::ServerConfig>,
) {
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("metrics HTTPS server failed to bind {addr}: {e}");
            return;
        }
    };
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let (acceptor, metrics) = (acceptor.clone(), metrics.clone());
        tokio::spawn(async move {
            if let Ok(Ok(stream)) =
                tokio::time::timeout(Duration::from_secs(5), acceptor.accept(socket)).await
            {
                let _ = handle(stream, node_id, &metrics).await;
            }
        });
    }
}

/// How long a client may take to send its request line. A connection that
/// sends nothing is dropped, so idle sockets cannot pile up.
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Minimal HTTP/1.1: one request per connection, no keep-alive, no bodies.
///
///   GET|HEAD /metrics (also `/` and `/metrics/`)  200 text/plain Prometheus
///   GET|HEAD /healthz (also `/livez`)             200 / 503 application/json
///   GET|HEAD /readyz                              200 / 503 application/json
///   anything else                                  404; other methods 405
pub async fn serve_on(
    listener: tokio::net::TcpListener,
    node_id: u64,
    metrics: std::sync::Arc<Metrics>,
) {
    loop {
        let (socket, _) = match listener.accept().await {
            Ok(pair) => pair,
            Err(_) => continue,
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let _ = handle(socket, node_id, &metrics).await;
        });
    }
}

async fn handle<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut socket: S,
    node_id: u64,
    metrics: &Metrics,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Read until the end of the request head (or 4 KiB, or the timeout).
    let mut buf = Vec::with_capacity(512);
    let mut chunk = [0u8; 512];
    let read = tokio::time::timeout(REQUEST_READ_TIMEOUT, async {
        loop {
            let n = socket.read(&mut chunk).await?;
            if n == 0 {
                return Ok::<_, std::io::Error>(());
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") || buf.len() >= 4096 || buf.contains(&b'\n')
            {
                return Ok(());
            }
        }
    })
    .await;
    if read.is_err() || buf.is_empty() {
        return Ok(()); // timed out or sent nothing: just close
    }
    let (status, content_type, body, head_only) = route(&buf, node_id, metrics);
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        503 => "Service Unavailable",
        _ => "Bad Request",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket.write_all(head.as_bytes()).await?;
    if !head_only {
        socket.write_all(body.as_bytes()).await?;
    }
    socket.shutdown().await
}

/// Pure routing, separated from the socket code so it is trivially testable.
/// Returns (status, content type, body, is_head).
fn route(request: &[u8], node_id: u64, metrics: &Metrics) -> (u16, &'static str, String, bool) {
    const TEXT: &str = "text/plain; charset=utf-8";
    const PROM: &str = "text/plain; version=0.0.4; charset=utf-8";
    const JSON: &str = "application/json";
    let line = request.split(|&b| b == b'\n').next().unwrap_or(&[]);
    let line = String::from_utf8_lossy(line);
    let mut parts = line.split_whitespace();
    let (method, target) = match (parts.next(), parts.next()) {
        (Some(m), Some(t)) => (m, t),
        _ => return (400, TEXT, "malformed request\n".into(), false),
    };
    let head_only = method == "HEAD";
    if method != "GET" && !head_only {
        return (405, TEXT, "only GET and HEAD are supported\n".into(), false);
    }
    let path = target.split('?').next().unwrap_or("");
    match path {
        "/" | "/metrics" | "/metrics/" => (200, PROM, metrics.render(node_id), head_only),
        "/healthz" | "/livez" => {
            let r = metrics.liveness();
            (
                if r.ok { 200 } else { 503 },
                JSON,
                r.to_json(node_id),
                head_only,
            )
        }
        "/readyz" => {
            let r = metrics.readiness();
            (
                if r.ok { 200 } else { 503 },
                JSON,
                r.to_json(node_id),
                head_only,
            )
        }
        _ => (
            404,
            TEXT,
            "not found; try /metrics, /healthz or /readyz\n".into(),
            head_only,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::sync::Arc;

    // ---- a strict reader for the exposition format ----------------------

    struct Parsed {
        types: HashMap<String, String>,
        samples: Vec<(String, String, f64)>, // (name, labels, value)
    }

    /// Parse and validate. Panics (failing the test) on anything a real
    /// Prometheus server would reject or find ambiguous.
    fn parse(text: &str) -> Parsed {
        let mut types = HashMap::new();
        let mut helps = HashSet::new();
        let mut samples = Vec::new();
        let mut seen = HashSet::new();
        for line in text.lines() {
            if let Some(r) = line.strip_prefix("# HELP ") {
                let name = r.split_whitespace().next().unwrap();
                assert!(helps.insert(name.to_string()), "duplicate HELP for {name}");
            } else if let Some(r) = line.strip_prefix("# TYPE ") {
                let mut it = r.split_whitespace();
                let (name, ty) = (it.next().unwrap(), it.next().unwrap());
                assert!(
                    ["counter", "gauge", "histogram"].contains(&ty),
                    "bad type {ty}"
                );
                assert!(
                    types.insert(name.to_string(), ty.to_string()).is_none(),
                    "duplicate TYPE for {name}"
                );
            } else if line.starts_with('#') || line.is_empty() {
                panic!("unexpected comment/blank line: {line:?}");
            } else {
                let (series, value) = line
                    .rsplit_once(' ')
                    .unwrap_or_else(|| panic!("no value: {line:?}"));
                let value: f64 = value
                    .parse()
                    .unwrap_or_else(|_| panic!("bad value in {line:?}"));
                let (name, labels) = match series.split_once('{') {
                    Some((n, l)) => (
                        n,
                        l.strip_suffix('}')
                            .unwrap_or_else(|| panic!("unterminated labels: {line:?}")),
                    ),
                    None => (series, ""),
                };
                assert!(seen.insert(series.to_string()), "duplicate series {series}");
                assert!(
                    name.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':'),
                    "bad metric name {name:?}"
                );
                samples.push((name.to_string(), labels.to_string(), value));
            }
        }
        // every sample belongs to a declared family
        for (name, _, _) in &samples {
            let base = ["_bucket", "_sum", "_count"]
                .iter()
                .find_map(|s| name.strip_suffix(s))
                .filter(|b| types.get(*b).map(String::as_str) == Some("histogram"))
                .unwrap_or(name);
            assert!(types.contains_key(base), "sample {name} has no TYPE");
            assert!(helps.contains(base), "sample {name} has no HELP");
        }
        for t in types.keys() {
            assert!(helps.contains(t), "TYPE without HELP: {t}");
        }
        // histogram sanity
        for (fam, ty) in &types {
            if ty != "histogram" {
                continue;
            }
            let buckets: Vec<_> = samples
                .iter()
                .filter(|(n, _, _)| *n == format!("{fam}_bucket"))
                .collect();
            assert!(buckets.len() >= 2, "{fam}: no buckets");
            let mut last = 0.0;
            for (_, l, v) in &buckets {
                assert!(*v >= last, "{fam}: buckets not cumulative at {l}");
                last = *v;
            }
            assert!(
                buckets.last().unwrap().1.contains("le=\"+Inf\""),
                "{fam}: last bucket must be +Inf"
            );
            let count = samples
                .iter()
                .find(|(n, _, _)| *n == format!("{fam}_count"))
                .unwrap()
                .2;
            assert_eq!(
                buckets.last().unwrap().2,
                count,
                "{fam}: +Inf bucket != _count"
            );
            assert!(
                samples.iter().any(|(n, _, _)| *n == format!("{fam}_sum")),
                "{fam}: no _sum"
            );
        }
        Parsed { types, samples }
    }

    fn value(p: &Parsed, name: &str) -> f64 {
        p.samples
            .iter()
            .find(|(n, _, _)| n == name)
            .unwrap_or_else(|| panic!("no sample {name}"))
            .2
    }

    // ---- histogram --------------------------------------------------------

    #[test]
    fn bucket_bounds_are_inclusive_and_overflow_goes_to_inf() {
        let h = Histogram::with_bounds(&[10, 20, 30]);
        for v in [0, 10, 11, 20, 21, 30, 31, 1_000_000] {
            h.observe(v);
        }
        let s = h.snapshot();
        assert_eq!(
            s.buckets,
            vec![2, 2, 2, 2],
            "le is inclusive: 10 -> first bucket, 31 -> +Inf"
        );
        assert_eq!(s.count, 8);
        assert_eq!(s.sum, 10 + 11 + 20 + 21 + 30 + 31 + 1_000_000);
        assert_eq!(s.buckets.iter().sum::<u64>(), s.count);
    }

    #[test]
    fn quantiles_are_upper_bucket_bounds() {
        let h = Histogram::with_bounds(&[1, 10, 100]);
        for _ in 0..90 {
            h.observe(1);
        }
        for _ in 0..9 {
            h.observe(10);
        }
        h.observe(5000); // one slow outlier in +Inf
        let s = h.snapshot();
        assert_eq!(s.quantile_upper_bound(0.5), Some(1));
        assert_eq!(s.quantile_upper_bound(0.95), Some(10));
        assert_eq!(
            s.quantile_upper_bound(1.0),
            None,
            "max is in the +Inf bucket"
        );
        assert_eq!(
            Histogram::with_bounds(&[1])
                .snapshot()
                .quantile_upper_bound(0.5),
            None
        );
    }

    #[test]
    fn concurrent_observers_lose_nothing() {
        let h = Arc::new(Histogram::default());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let h = h.clone();
                std::thread::spawn(move || {
                    for i in 0..10_000u64 {
                        h.observe((i * 7 + t) % 3_000_000);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let s = h.snapshot();
        assert_eq!(s.count, 80_000);
        assert_eq!(s.buckets.iter().sum::<u64>(), 80_000);
    }

    #[test]
    fn durations_are_recorded_in_microseconds() {
        let h = Histogram::default();
        h.observe_duration(Duration::from_millis(3)); // 3000us -> le 5000
        let s = h.snapshot();
        assert_eq!(s.sum, 3000);
        let idx = s.bounds.iter().position(|&b| b == 5_000).unwrap();
        assert_eq!(s.buckets[idx], 1);
    }

    // ---- exposition -------------------------------------------------------

    #[test]
    fn the_full_exposition_is_valid_and_has_every_documented_family() {
        let m = Metrics::default();
        m.observe_write_latency(Duration::from_millis(2));
        m.add_batch(17, 4096);
        m.set_applied_index(3);
        let p = parse(&m.render(7));
        for name in [
            // pre-existing series must be untouched
            "raft_leader_changes_total",
            "kv_puts_total",
            "kv_pending_writes",
            "raft_current_term",
            "raft_is_leader",
            "kv_write_batches_total",
            "kv_writes_overloaded_total",
            // 4b
            "kv_write_latency_seconds",
            "kv_read_latency_seconds",
            "raft_apply_latency_seconds",
            "kv_write_batch_size",
            "raft_applied_index",
            "raft_apply_lag",
            "kv_pending_reads",
            "kv_inbox_depth",
            "raft_wal_fsync_seconds",
            "raft_wal_appended_entries_total",
            "raft_wal_fsync_errors_total",
            "raft_wal_rewrites_total",
            "lsm_compaction_seconds",
            "node_healthy",
            "node_ready",
            "node_loop_heartbeat_age_seconds",
            "node_uptime_seconds",
        ] {
            assert!(p.types.contains_key(name), "missing family {name}");
        }
        assert!(
            p.samples.iter().all(|(_, l, _)| l.contains("node=\"7\"")),
            "every series carries the node label"
        );
        assert_eq!(value(&p, "kv_write_latency_seconds_count"), 1.0);
        assert_eq!(value(&p, "kv_write_batch_size_sum"), 17.0);
        assert!(
            (value(&p, "kv_write_latency_seconds_sum") - 0.002).abs() < 1e-9,
            "sum is rendered in seconds"
        );
    }

    #[test]
    fn group_commit_batches_feed_the_batch_size_histogram_automatically() {
        // `add_batch` is already called by the node loop on every flush, so
        // this needs no new hook.
        let m = Metrics::default();
        for w in [1u64, 1, 5, 200] {
            m.add_batch(w, w * 10);
        }
        let s = m.batch_size.snapshot();
        assert_eq!((s.count, s.sum), (4, 207));
        assert_eq!(s.quantile_upper_bound(0.5), Some(1));
        assert_eq!(s.quantile_upper_bound(1.0), Some(256));
        assert_eq!(
            m.batches_flushed_total.load(Ordering::Relaxed),
            4,
            "existing counters unchanged"
        );
    }

    #[test]
    fn lsm_series_appear_only_once_the_engine_is_reported() {
        let m = Metrics::default();
        assert!(
            !m.render(1).contains("lsm_l0_files"),
            "no LSM numbers for an engine that never reported"
        );
        let st = storage::lsm::Stats {
            l0_files: 3,
            l1_files: 5,
            compactions: 9,
            flushes: 12,
            disk_bytes: 4096,
            ..Default::default()
        };
        m.record_lsm(&st, false);
        let p = parse(&m.render(1));
        assert_eq!(value(&p, "lsm_l0_files"), 3.0);
        assert_eq!(value(&p, "lsm_compactions_total"), 9.0);
        assert_eq!(value(&p, "lsm_flushes_total"), 12.0);
        assert_eq!(value(&p, "lsm_poisoned"), 0.0);
    }

    #[test]
    fn apply_lag_is_not_reported_until_the_node_loop_reports_an_applied_index() {
        // Guards against reporting raft_apply_lag == commit_index on a healthy
        // node just because the applied index was still its default 0.
        let m = Metrics::default();
        m.set_commit_index(41);
        let before = m.render(1);
        assert!(!before.contains("raft_apply_lag") && !before.contains("raft_applied_index"));
        m.set_applied_index(40);
        let p = parse(&m.render(1));
        assert_eq!(value(&p, "raft_applied_index"), 40.0);
        assert_eq!(value(&p, "raft_apply_lag"), 1.0);
    }

    #[test]
    fn lsm_counters_never_go_backwards() {
        let m = Metrics::default();
        m.record_lsm(
            &storage::lsm::Stats {
                compactions: 10,
                ..Default::default()
            },
            false,
        );
        m.record_lsm(
            &storage::lsm::Stats {
                compactions: 4,
                ..Default::default()
            },
            false,
        ); // stale reading
        assert_eq!(m.lsm_compactions_total.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn a_poisoned_engine_fails_both_probes() {
        let m = Metrics::default();
        m.set_term(1);
        assert!(m.liveness_at(100).ok);
        m.record_lsm(&Default::default(), true);
        assert!(
            !m.liveness_at(100).ok,
            "poisoned storage needs a restart: liveness must fail"
        );
        assert!(!m.readiness_at(100).ok);
        assert_eq!(
            parse(&m.render(1))
                .samples
                .iter()
                .find(|s| s.0 == "lsm_poisoned")
                .unwrap()
                .2,
            1.0
        );
    }

    // ---- probes -----------------------------------------------------------

    fn cfg() -> HealthConfig {
        HealthConfig {
            heartbeat_timeout: Duration::from_millis(1000),
            startup_grace: Duration::from_millis(5000),
            max_apply_lag: 10,
        }
    }

    #[test]
    fn liveness_has_a_startup_grace_then_demands_a_heartbeat() {
        let m = Metrics::with_health_config(cfg());
        let r = m.liveness_at(1000);
        assert!(
            r.ok && r.status == "starting",
            "within grace: alive but starting, got {r:?}"
        );
        let r = m.liveness_at(6000);
        assert!(
            !r.ok && r.status == "failing",
            "never started past grace: dead, got {r:?}"
        );
    }

    #[test]
    fn the_existing_gauge_setters_double_as_the_heartbeat() {
        for stamp in [
            (|m: &Metrics| m.set_term(1)) as fn(&Metrics),
            |m| m.set_commit_index(1),
            |m| m.set_log_last_index(1),
            |m| m.set_is_leader(true),
            |m| m.set_pending_writes(1),
        ] {
            let m = Metrics::with_health_config(cfg());
            stamp(&m);
            assert_eq!(
                m.liveness_at(500).status,
                "ok",
                "a setter must stamp the heartbeat"
            );
            let stale = m.liveness_at(5000);
            assert!(
                !stale.ok,
                "a stalled loop must fail liveness, got {stale:?}"
            );
        }
    }

    #[test]
    fn readiness_requires_a_started_alive_undrained_node() {
        let m = Metrics::with_health_config(cfg());
        let r = m.readiness_at(100);
        assert!(!r.ok && r.status == "starting", "{r:?}");
        m.set_term(1);
        assert!(
            m.readiness_at(100).ok,
            "started + alive + nothing else wired -> ready"
        );
        assert!(!m.readiness_at(9000).ok, "a stalled loop is not ready");
        m.begin_shutdown();
        let r = m.readiness_at(100);
        assert!(!r.ok && r.status == "draining", "{r:?}");
        assert!(
            m.liveness_at(100).ok,
            "draining is still alive: do not restart a node that is shutting down"
        );
    }

    #[test]
    fn readiness_follows_leader_knowledge_once_it_is_reported() {
        let m = Metrics::with_health_config(cfg());
        m.set_term(1);
        assert!(
            m.readiness_at(100)
                .checks
                .iter()
                .all(|c| c.name != "leader_known"),
            "unwired: not enforced"
        );
        m.set_leader_known(false);
        let r = m.readiness_at(100);
        assert!(!r.ok);
        assert!(r.checks.iter().any(|c| c.name == "leader_known" && !c.ok));
        m.set_leader_known(true);
        assert!(m.readiness_at(100).ok);
    }

    #[test]
    fn readiness_fails_when_apply_lag_exceeds_the_limit() {
        let m = Metrics::with_health_config(cfg());
        m.set_term(1);
        m.set_commit_index(100);
        m.set_applied_index(95);
        assert!(m.readiness_at(100).ok, "lag 5 <= 10");
        m.set_applied_index(50);
        let r = m.readiness_at(100);
        assert!(!r.ok);
        let c = r.checks.iter().find(|c| c.name == "apply_lag").unwrap();
        assert!(!c.ok && c.detail.contains("= 50"), "{c:?}");
        m.set_applied_index(100);
        assert!(m.readiness_at(100).ok);
    }

    #[test]
    fn probe_json_is_escaped_and_well_formed_enough_to_grep() {
        let r = Report {
            ok: false,
            status: "failing",
            checks: vec![Check {
                name: "x",
                ok: false,
                detail: "quote \" back\\slash\nnewline\ttab \u{1}".into(),
            }],
        };
        let j = r.to_json(4);
        assert!(j.starts_with("{\"node\":4,\"status\":\"failing\",\"ok\":false"));
        assert!(
            j.contains("quote \\\" back\\\\slash\\nnewline\\ttab \\u0001"),
            "{j}"
        );
        assert!(!j.contains('\n'));
    }

    // ---- routing and the real socket -------------------------------------

    #[test]
    fn routing_table() {
        let m = Metrics::with_health_config(cfg());
        let get = |t: &str| route(format!("GET {t} HTTP/1.1\r\n\r\n").as_bytes(), 1, &m);
        assert_eq!(get("/metrics").0, 200);
        assert_eq!(get("/metrics?x=1").0, 200, "query strings are ignored");
        assert_eq!(
            get("/").0,
            200,
            "legacy: the old server answered every path"
        );
        assert_eq!(get("/healthz").0, 200, "within startup grace");
        assert_eq!(get("/livez").0, 200);
        assert_eq!(get("/readyz").0, 503, "not started");
        assert_eq!(get("/nope").0, 404);
        assert_eq!(route(b"POST /metrics HTTP/1.1\r\n\r\n", 1, &m).0, 405);
        assert_eq!(route(b"garbage", 1, &m).0, 400);
        assert_eq!(route(b"\r\n", 1, &m).0, 400);
        let head = route(b"HEAD /metrics HTTP/1.1\r\n\r\n", 1, &m);
        assert!(
            head.3 && head.0 == 200,
            "HEAD is routed like GET but sends no body"
        );
    }

    async fn http(addr: std::net::SocketAddr, request: &str) -> (u16, String, String) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(request.as_bytes()).await.unwrap();
        let mut out = Vec::new();
        s.read_to_end(&mut out).await.unwrap();
        let text = String::from_utf8(out).unwrap();
        let (head, body) = text.split_once("\r\n\r\n").expect("response has a head");
        let status: u16 = head.split_whitespace().nth(1).unwrap().parse().unwrap();
        (status, head.to_string(), body.to_string())
    }

    async fn spawn_server(m: Arc<Metrics>) -> std::net::SocketAddr {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(serve_on(l, 9, m));
        addr
    }

    #[tokio::test]
    async fn end_to_end_probes_over_a_real_socket() {
        let m = Arc::new(Metrics::with_health_config(cfg()));
        let addr = spawn_server(m.clone()).await;

        let (st, head, body) = http(addr, "GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_eq!(st, 200);
        assert!(head.contains("Content-Type: text/plain; version=0.0.4"));
        assert!(
            head.contains(&format!("Content-Length: {}", body.len())),
            "Content-Length must match the body"
        );
        parse(&body);

        let (st, _, body) = http(addr, "GET /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!(st, 503);
        assert!(body.contains("\"status\":\"starting\""), "{body}");

        m.set_term(2); // the node loop's first tick
        let (st, head, body) = http(addr, "GET /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!(st, 200, "{body}");
        assert!(head.contains("application/json") && head.contains("Cache-Control: no-store"));
        assert!(
            body.contains("\"ok\":true") && body.contains("\"node\":9"),
            "{body}"
        );

        m.begin_shutdown();
        let (st, _, body) = http(addr, "GET /readyz HTTP/1.1\r\n\r\n").await;
        assert_eq!(st, 503);
        assert!(body.contains("\"status\":\"draining\""), "{body}");
        assert_eq!(
            http(addr, "GET /healthz HTTP/1.1\r\n\r\n").await.0,
            200,
            "draining is still alive"
        );

        m.set_storage_failed(true);
        assert_eq!(http(addr, "GET /healthz HTTP/1.1\r\n\r\n").await.0, 503);

        assert_eq!(http(addr, "GET /nope HTTP/1.1\r\n\r\n").await.0, 404);
        assert_eq!(http(addr, "DELETE /metrics HTTP/1.1\r\n\r\n").await.0, 405);
        let (st, _, body) = http(addr, "HEAD /metrics HTTP/1.1\r\n\r\n").await;
        assert!(st == 200 && body.is_empty(), "HEAD must not send a body");
    }

    #[tokio::test]
    async fn a_silent_client_cannot_block_other_scrapes() {
        let m = Arc::new(Metrics::default());
        let addr = spawn_server(m).await;
        // open a connection and send nothing
        let _idle = tokio::net::TcpStream::connect(addr).await.unwrap();
        // a real scrape still gets answered promptly
        let r = tokio::time::timeout(
            Duration::from_secs(1),
            http(addr, "GET /metrics HTTP/1.1\r\n\r\n"),
        )
        .await;
        assert_eq!(r.expect("scrape was blocked by an idle client").0, 200);
    }

    #[tokio::test]
    async fn a_request_split_across_packets_is_still_served() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let addr = spawn_server(Arc::new(Metrics::default())).await;
        let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /heal").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        s.write_all(b"thz HTTP/1.1\r\n\r\n").await.unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).await.unwrap();
        assert!(out.starts_with("HTTP/1.1 200"), "{out}");
    }

    #[test]
    fn rendering_is_deterministic_in_shape() {
        // Same families in the same order every time: dashboards and diffs
        // depend on a stable layout.
        let m = Metrics::default();
        let names = |t: &str| -> Vec<String> {
            t.lines()
                .filter(|l| l.starts_with("# TYPE "))
                .map(|l| l.to_string())
                .collect()
        };
        assert_eq!(names(&m.render(1)), names(&m.render(1)));
        let by_type: BTreeMap<_, _> = parse(&m.render(1)).types.into_iter().collect();
        assert_eq!(
            by_type.len(),
            38,
            "metric family count changed: update docs/observability.md"
        );
    }

    #[test]
    fn disk_series_appear_only_once_the_volume_is_sampled() {
        let m = Metrics::default();
        assert!(!m.render(1).contains("node_disk_free_bytes"));
        m.record_disk(5_000, 10_000, true);
        m.inc_writes_disk_full();
        let p = parse(&m.render(1));
        assert_eq!(value(&p, "node_disk_free_bytes"), 5000.0);
        assert_eq!(value(&p, "node_disk_min_free_bytes"), 10000.0);
        assert_eq!(value(&p, "node_writes_protected"), 1.0);
        assert_eq!(value(&p, "kv_writes_disk_full_total"), 1.0);
        m.record_disk(50_000, 10_000, false);
        assert_eq!(value(&parse(&m.render(1)), "node_writes_protected"), 0.0);
    }

    #[test]
    fn certificate_expiry_series_appear_only_for_a_node_with_a_certificate() {
        let m = Metrics::default();
        assert!(!m.render(1).contains("tls_cert_"));
        let in_ten_days = crate::certinfo::now_unix() + 10 * 86_400;
        m.record_cert_expiry(in_ten_days);
        let p = parse(&m.render(1));
        assert_eq!(value(&p, "tls_cert_not_after_seconds"), in_ten_days as f64);
        let left = value(&p, "tls_cert_expires_in_seconds");
        assert!((left - 864_000.0).abs() < 5.0, "{left}");
        m.record_cert_expiry(crate::certinfo::now_unix() - 60);
        assert!(
            value(&parse(&m.render(1)), "tls_cert_expires_in_seconds") < 0.0,
            "expired = negative"
        );
    }
}
