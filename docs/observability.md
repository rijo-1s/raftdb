# Observability

Four things, all on the existing `--metrics-addr` listener plus stderr:

| what | where |
|---|---|
| Prometheus metrics | `GET /metrics` |
| Liveness | `GET /healthz` (alias `/livez`) |
| Readiness | `GET /readyz` |
| Structured logs | stderr, one event per line (`RAFTDB_LOG_FORMAT=json`) |

`/`, `/metrics` and `/metrics/` all serve metrics. `HEAD` works, other methods get 405 and unknown
paths 404. There is one request per connection, and a client that sends nothing is dropped after 2 s,
so idle sockets cannot block scrapes.

## Where each signal comes from

`metrics.rs` and `crates/raftdb-log` are self-contained. Signals that only the node code can supply
(`node_loop.rs`, `server.rs`, `main.rs`) are one-line calls on `Metrics`.

| signal | source |
|---|---|
| liveness heartbeat | the `set_term/commit_index/log_last_index/is_leader/pending_writes` calls in `after_step` (every 50 ms tick) stamp it |
| WAL fsync latency, entries and bytes durable, fsync errors, rewrites | measured inside `storage::Wal` and read at scrape time |
| group-commit batch size | fed from the `add_batch` call |
| leader known (`set_leader_known`) | `now_leader \|\| raft.leader_id().is_some()` |
| applied index and lag (`set_applied_index`) | `raft.last_applied_index()` |
| pending reads (`set_pending_reads`) | the node loop |
| LSM files, bytes, flushes, compactions, cache, poisoned (`record_lsm`) | polls `LsmStore::stats()` about once a second |
| client write and read latency | timed in the gRPC handler (`observe_write_latency`, `observe_read_latency`) |
| shutdown draining (`begin_shutdown`) | called when the shutdown signal arrives |
| structured logs, panic hook | `raftdb_log::init`, `install_panic_hook`, `events::*` |

Three series are defined but not fed yet, because the timing would have to live inside `kv` and
`lsm/mod.rs`: `raft_apply_latency_seconds`, `lsm_compaction_seconds` and `kv_inbox_depth`. They stay at
zero.

Series whose source has not reported are omitted or zero-count, never misleading: `raft_apply_lag`,
`raft_applied_index` and every `lsm_*` gauge and counter are only emitted once reported. An unreported
applied index of 0 would otherwise make the lag equal the whole commit index.

## Metric catalogue

Every series carries `{node="<id>"}`. Latency histograms are in seconds.

| family | type | meaning |
|---|---|---|
| `kv_write_latency_seconds` | histogram | client write: received -> applied and acknowledged |
| `kv_read_latency_seconds` | histogram | linearizable read: received -> ReadIndex confirmed and answered |
| `raft_apply_latency_seconds` | histogram | applying a batch of committed entries |
| `kv_write_batch_size` | histogram | writes per group-commit batch |
| `raft_wal_fsync_seconds` | histogram | the fsync that makes an append durable (one per batch) |
| `raft_wal_fsync_max_seconds` | gauge | slowest fsync since start |
| `raft_wal_appended_entries_total`, `raft_wal_appended_bytes_total` | counter | made durable |
| `raft_wal_fsync_errors_total` | counter | failed appends (each poisons the WAL) |
| `raft_wal_rewrites_total`, `raft_wal_rewrite_seconds_total` | counter | log truncation / compaction rewrites |
| `raft_applied_index`, `raft_apply_lag` | gauge | applied index; `commit_index - applied_index` |
| `kv_pending_reads`, `kv_inbox_depth` | gauge | queue depths (`kv_pending_writes` already existed) |
| `lsm_l0_files`, `lsm_l1_files` | gauge | tables per level; L0 is the compaction backlog |
| `lsm_sst_bytes`, `lsm_disk_bytes`, `lsm_memtable_bytes` | gauge | sizes |
| `lsm_flushes_total`, `lsm_compactions_total` | counter | background work done |
| `lsm_cache_hits_total`, `lsm_cache_misses_total` | counter | block cache |
| `lsm_compaction_seconds` | histogram | one compaction round |
| `lsm_poisoned` | gauge | 1 = engine refused further work after an I/O error |
| `node_healthy`, `node_ready` | gauge | what `/healthz` and `/readyz` would return |
| `node_loop_heartbeat_age_seconds` | gauge | -1 = never |
| `node_uptime_seconds` | gauge | |
| `node_disk_free_bytes` | gauge | Bytes available on the data volume (appears once the node loop has sampled it) |
| `node_disk_min_free_bytes` | gauge | The configured floor (`--min-free-mb`); 0 = protection off |
| `node_writes_protected` | gauge | 1 while client writes are rejected because free space is below the floor. **Alert on this.** |
| `kv_writes_disk_full_total` | counter | Client writes rejected for lack of disk space |
| `tls_cert_not_after_seconds` | gauge | Unix time the node certificate expires (only on a node started with `--tls-cert`) |
| `tls_cert_expires_in_seconds` | gauge | Seconds until it expires; negative = expired. **Alert below ~14 days.** |

`raft_wal_*` counters are process-wide (every `Wal` in the process feeds them); a node has one Raft WAL.

## Probes

Both return JSON naming every check, so an operator sees *why*:

```json
{"node":1,"status":"failing","ok":false,"checks":{"node_loop_heartbeat":{"ok":false,"detail":"last heartbeat 12031ms ago (limit 10000ms)"},"storage":{"ok":true,"detail":"healthy"}}}
```

**`/healthz` (liveness)** - restart me if this fails. 200 while the node loop is making progress.
- `node_loop_heartbeat`: a beat within 10 s. The loop beats every ~50 ms, so this only trips on a real
  stall (hung fsync, deadlock). Before the first beat the node is `starting` (alive) for 60 s, then dead.
- `storage`: fails when the engine is poisoned. A poisoned handle refuses all work by design and only a
  restart (which re-runs recovery) clears it.

**`/readyz` (readiness)** - route traffic to me only if this is 200.
- `started`, `alive`, `not_draining` (503 as soon as shutdown begins; `/healthz` stays 200 so the node
  is not restarted while draining).
- `leader_known` - enforced once reported: 503 during an election or when partitioned from a quorum.
- `apply_lag` - enforced once reported: 503 if `commit - applied` exceeds 10 000.

Kubernetes:

```yaml
livenessProbe:  { httpGet: { path: /healthz, port: 9001 }, periodSeconds: 5, failureThreshold: 3 }
readinessProbe: { httpGet: { path: /readyz,  port: 9001 }, periodSeconds: 2 }
```

## Logs

```sh
RAFTDB_LOG=info                # EnvFilter syntax: "warn,raftdb=debug"
RAFTDB_LOG_FORMAT=json         # or text (default)
```

One event per line. In JSON mode every line is a single valid object whatever the content (newlines
and quotes are escaped); the `events::*` helpers also escape control characters so the *text* format
cannot be line-forged by a hostile key or error string. Match on `event`, never on message text.

```json
{"timestamp":"2026-10-18T17:07:01.280632Z","level":"INFO","message":"became leader","event":"became_leader","node":1,"term":2,"target":"raftdb_log::events"}
```

| event | level | fields |
|---|---|---|
| `node_started` | INFO | node bind peers data_dir engine join |
| `became_leader`, `stepped_down` | INFO | node term |
| `removed_from_cluster` | WARN | node |
| `readiness_changed` | INFO/WARN | node ready reason |
| `slow_fsync` | WARN | node micros threshold_micros |
| `compaction_finished` | INFO | node duration_ms l0_files l1_files |
| `storage_failure` | ERROR | node operation error |
| `node_loop_died` | ERROR | node reason |
| `peer_unreachable` | WARN | node peer error suppressed |
| `disk_space_changed` | WARN / INFO | node protected free_bytes min_free_bytes |
| `compaction_skipped` | WARN | node needed_bytes free_bytes |
| `tls_cert_expiry` | INFO / WARN | node not_after_unix seconds_left (at startup; WARN within 30 days) |
| `tls_cert_expiry_unknown` | WARN | node reason |
| `recovery_refused` | ERROR | node path error |
| `snapshot_send_failed` | WARN | node peer bytes elapsed_ms error (a snapshot transfer to a peer did not complete; repeated for one peer means it cannot catch up) |
| `authz_enabled` | INFO | node admin read_write read_only nodes (entry counts) |
| `authz_denied` | WARN | node who (certificate fingerprint prefix, or `none`) method required suppressed (rate-limited to 1 per second) |
| `lsm_quarantined` | WARN | node reason moved_to |
| `metrics_listening` | INFO | node addr |
| `shutdown_started`, `shutdown_complete` | INFO | node |
| `panic` | ERROR | thread location panic_message |

`lsm_quarantined` means the LSM directory was found corrupt, moved aside and rebuilt from the Raft log:
the node recovered, but the disk deserves attention.

```sh
jq -c 'select(.level=="ERROR" or .event=="lsm_quarantined")' node.log
```

## Alerts (starting points)

```yaml
- alert: RaftDBNotReady
  expr: node_ready == 0
  for: 1m
- alert: RaftDBNoLeader                # quorum lost or split brain suspected
  expr: sum(raft_is_leader) == 0
  for: 30s
- alert: RaftDBStorageFailed
  expr: lsm_poisoned == 1 or increase(raft_wal_fsync_errors_total[5m]) > 0
- alert: RaftDBSlowFsync               # the usual cause of high write latency
  expr: histogram_quantile(0.99, sum by (le, node) (rate(raft_wal_fsync_seconds_bucket[5m]))) > 0.05
  for: 5m
- alert: RaftDBCompactionBacklog
  expr: lsm_l0_files > 12
  for: 10m
- alert: RaftDBApplyLag
  expr: raft_apply_lag > 1000
  for: 2m
- alert: RaftDBWriteLatencyP99
  expr: histogram_quantile(0.99, sum by (le, node) (rate(kv_write_latency_seconds_bucket[5m]))) > 0.25
  for: 5m
```

## Known limits

- `kv_puts_total` (pre-existing) counts requests received, including ones a follower redirects.
- If the metrics listener cannot bind, `metrics.rs` prints one plain `eprintln!` line (the node keeps running).
- Histogram bounds are fixed (100 us .. 10 s); quantiles come from Prometheus' `histogram_quantile`.
