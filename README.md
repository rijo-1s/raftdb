# raftdb

A distributed key-value store built on Raft, written in Rust.

I wrote the Raft implementation, the write-ahead log, the LSM storage engine, the exactly-once client sessions and the tooling around them in this repository. gRPC (tonic), TLS (rustls) and the async runtime (tokio) are libraries.

It has not been run under long soak tests or on real power-loss hardware. Read [Known limitations](#known-limitations) before putting data in it that you cannot lose.

## Layout

```
crates/
  raft-core/     the Raft algorithm: no networking, no disk I/O, no dependencies
  storage/       write-ahead log, snapshots, hard state, and the LSM engine
  kv/            the key-value state machine, client sessions, backup format
  raftdb-proto/  protobuf and gRPC definitions
  raftdb/        the binary: gRPC server, CLI client, metrics, backup and restore
  raftdb-log/    structured logging, stable event names, panic hook
bench/           benchmark harness (bench/run_all.sh, see bench/README.md)
fuzz/            property tests and random-byte loops for every decoder
scripts/         checks against real processes (see Testing)
docs/            runbook, TLS, authorization, observability, backup, checklists
dashboards/      Grafana dashboard for the metrics endpoint
.github/         CI: format, clippy, test and release build on pushes and PRs to main
```

`raft-core` is a state machine driven by two calls: `tick()` for time passing and `step(from, message)` for an incoming message. Both return the messages to send. Nothing in it touches a socket or a disk, so `tests/cluster_sim.rs` can run a whole cluster inside one test function with in-memory message delivery. This is the same split etcd's `raft` and TiKV's `raft-rs` use.

Each node in `raftdb` is a single async task that owns its `RaftNode` and state machine. Timer ticks, inbound RPCs and client requests all go through one channel and are handled one at a time. Outbound RPCs are sent from separate tasks so a slow peer cannot block the node.

## Build and run

You need Rust and `protoc` (`apt install protobuf-compiler` on Debian and Ubuntu).

```
cargo build --release
```

Start three nodes, one per terminal:

```
raftdb start --id 1 --addr 127.0.0.1:6001 --peers 2=127.0.0.1:6002,3=127.0.0.1:6003 --data-dir ./data1
raftdb start --id 2 --addr 127.0.0.1:6002 --peers 1=127.0.0.1:6001,3=127.0.0.1:6003 --data-dir ./data2
raftdb start --id 3 --addr 127.0.0.1:6003 --peers 1=127.0.0.1:6001,2=127.0.0.1:6002 --data-dir ./data3
```

`--peers` lists the other nodes, not the node itself. Then from any terminal:

```
raftdb put --cluster 1=127.0.0.1:6001,2=127.0.0.1:6002,3=127.0.0.1:6003 mykey myvalue
raftdb get --cluster 1=127.0.0.1:6001,2=127.0.0.1:6002,3=127.0.0.1:6003 mykey
```

The client tries one node first (`--node <id>` chooses which). If that node is not the leader it says who is, and the client retries there. Each client process uses a session id and increasing sequence numbers, so a write whose outcome was unknown is retried with the same sequence and applied at most once.

| Command | What it does |
|---|---|
| `start` | Run one node |
| `put`, `get`, `delete` | Client operations; `get` is linearizable |
| `add-node`, `remove-node` | Change cluster membership, one at a time |
| `backup` | Export a point-in-time backup from a running node |
| `restore` | Build a new single-node data directory from a backup |
| `bench` | Write load generator (`--count`, `--value-size`, `--concurrency`) |

To add a node to a running cluster, start it with `--join` and the current members in `--peers`, then run `add-node` against the cluster. The existing nodes must already know its address (start them with `--known-peers 4=host:port`), because addresses are fixed at startup.

```
raftdb start --id 4 --addr 127.0.0.1:6004 --peers 1=127.0.0.1:6001,2=127.0.0.1:6002,3=127.0.0.1:6003 --join --data-dir ./data4
raftdb add-node --cluster 1=127.0.0.1:6001,2=127.0.0.1:6002,3=127.0.0.1:6003 4
```

### `start` flags

| Flag | Default | Meaning |
|---|---|---|
| `--id`, `--addr`, `--peers`, `--data-dir` | required | Node id, bind address, the other nodes (`id=addr,...`), storage directory |
| `--known-peers` | none | Addresses to dial that are not Raft members yet |
| `--join` | off | Join a running cluster; no elections until `add-node` confirms it |
| `--engine` | `mem` | `mem` keeps the state in RAM; `lsm` keeps it on disk |
| `--lsm-sync-wal`, `--lsm-memtable-mb`, `--lsm-cache-mb` | off, 4, 8 | LSM engine tuning |
| `--compaction-threshold` | 50 | Compact the Raft log every N applied entries |
| `--metrics-addr`, `--metrics-tls-addr` | off | Metrics and health over HTTP and HTTPS |
| `--min-free-mb` | 256 | Reject writes below this much free disk; 0 turns it off |
| `--max-batch-entries` | 256 | Flush a write batch at this many writes (1 or `--no-batching` disables batching) |
| `--max-batch-bytes` | 1 MiB | Flush a write batch at this many bytes |
| `--max-batch-delay-us` | 0 | Longest the oldest queued write waits for company |
| `--max-pending-writes` | 8192 | Queued plus in-flight writes allowed; more are rejected |
| `--max-command-bytes` | 1 MiB | Largest single write |
| `--write-timeout-ms` | 10000 | After this a proposed write is answered "outcome unknown" |
| `--max-append-entries`, `--max-append-bytes` | 1024, 1 MiB | Bounds on one AppendEntries |

Global flags, also accepted by client commands: `--tls-cert`, `--tls-key`, `--tls-ca`, `--tls-require-client-cert`, `--tls-domain`, `--tls-strict-key-permissions`, `--authz-file`, `--audit-all`. See `docs/tls.md` and `docs/authorization.md`.

## What is implemented

Raft: leader election with randomized timeouts, PreVote and CheckQuorum, log replication, persistent term, vote and log, snapshots with `InstallSnapshot`, ReadIndex reads, and single-server membership changes through the log.

Storage: a crash-safe WAL with group commit, hard state and snapshot files, and an optional LSM engine (memtable and WAL, checksummed SSTables, Bloom filters, block cache, two-level background compaction, atomic manifest). Both engines produce byte-identical snapshots, so a cluster can mix them.

Client sessions: a write may carry a `(client_id, seq)`. A retry with the same pair is acknowledged with the original result and not applied again. The session table is part of the state machine, so it survives restarts, snapshots and backups.

Backup and restore: one checksummed file taken inside the node loop at an exact applied index. See `docs/backup-restore.md`.

Security: TLS and mutual TLS, optional role-based authorization by certificate fingerprint (read-only, read-write, admin, node), node identity binding, SIGHUP reload of the authorization file, an audit log, key permission checks, HTTPS metrics. The TLS, mutual TLS and authorization code has been reviewed (`docs/security-review-brief.md`). See `docs/tls.md` and `docs/authorization.md`.

Operations: Prometheus metrics, `/healthz` and `/readyz`, structured logs with stable event names (`RAFTDB_LOG_FORMAT=json`), a disk-full guard, and a Grafana dashboard. See `docs/observability.md` and `docs/operations.md`.

## Durability

These hold assuming the storage stack honors `fsync` and a crash means process death or power loss on a journaling filesystem.

- A log append, a vote or a term change reaches stable storage before the node acts on it or replies. An `AppendEntries` success or a vote grant is never sent for state a crash could lose.
- The hard state, the snapshot and any WAL rewrite are published atomically (temp file, fsync, rename, directory fsync). A crash leaves the complete old file or the complete new one. Stale `*.tmp` files are ignored and removed on startup.
- Compaction writes the snapshot first and rewrites the WAL second. A crash in between is detected on startup and finished.
- Recovery refuses to start, with an explicit error and every file untouched, on: a corrupt, truncated or unreadable snapshot; a corrupt or missing hard state when log or snapshot data exists; a damaged WAL frame followed by more data; non-contiguous or term-decreasing entries; a WAL that does not line up with its snapshot; a term in the log or snapshot newer than the persisted term.
- A torn tail (an incomplete or last-frame-corrupt append, or zero padding after a size-extending crash) is the one thing recovery repairs by itself. It is truncated, the truncation is fsynced, and the cut bytes go to `node.wal.discarded-tail`.
- After any I/O error the storage handle is poisoned and the node panics. There is no retry after a failed fsync. Restarting runs recovery.
- A restarted node starts with its commit index at the snapshot boundary and applies nothing until the cluster says entries are committed. A delayed or duplicate `AppendEntries` cannot truncate entries the follower already acknowledged. A leader that tries to overwrite a committed entry stops the process.

Not covered: disks or filesystems that lie about `fsync`, non-journaling filesystems, and silent corruption of a record whose CRC is still valid. I have not tested real power loss; most crash tests inject failures in process, and `crash_after_ack.rs` and `scripts/crash_restart_check.sh` `kill -9` real processes. None of them reorders or drops unsynced writes the way a power cut can.

### Startup checks

`WalLog::open` reads the WAL, hard state and snapshot together and cross-checks them before changing anything. Only then does it remove stale temp files, truncate a torn tail and finish an interrupted compaction. A refused open leaves every file as it was. A missing WAL is refused when a hard state, snapshot or rewrite temp file exists, because a first start creates the WAL before any of those can exist. The way out of a refusal is a backup restore or wiping the node and re-adding it. There is no automatic rebuild: a node that lost acknowledged entries must not rejoin with its old identity and term.

A corrupted length field cannot be told apart from a torn write by looking at that frame alone. If an intact checksummed frame exists after the damaged one, the damage is not at the tail, so recovery refuses. Only damage with nothing valid after it counts as a torn tail. A real torn tail is also refused if user data inside the partial record happens to contain a complete valid frame; that fails closed and loses nothing.

`Lsm::open` checks the MANIFEST and the WAL set the same way, in two phases. Phase 1 only reads and validates; phase 2 repairs. If anything is refused the directory is left byte for byte as found. The checks:

- Tables only at levels 0 and 1, level-1 tables sorted and disjoint, unique table ids, every table and WAL id below `next_file_id`, `live_wals` strictly increasing from `min_wal_id`, and each table's real key range matching the manifest.
- Every WAL in `live_wals` exists. An unreferenced WAL is deleted only if it is empty and has an id at or above `next_file_id` (a crash between creating a WAL and publishing the manifest). Any other unreferenced WAL at or above `min_wal_id` may hold acknowledged writes and is refused.
- Positive `applied` indexes in WAL records strictly increase across records and WALs, starting above the manifest's `applied`. A violation means WALs were duplicated, swapped or spliced. Snapshot ingest may lower the index because it starts a fresh WAL.
- A v1 manifest has no WAL list. All WALs at or above `min_wal_id` are kept and recorded in a v2 manifest on first open. A WAL lost before that first open cannot be detected.

In `raftdb start` an LSM validation error moves the engine directory aside as `lsm.corrupt-<timestamp>` (never deleted) and rebuilds from the Raft snapshot and log. If the engine is ahead of the Raft log the node refuses to start.

## Writes

Concurrent client writes are collected into a batch and proposed together: one WAL write and one fsync on the leader, one AppendEntries round, and one fsync per multi-entry append on followers. A batch is a throughput optimization, not a transaction. Its entries commit and apply individually, and a leadership change can commit a prefix of a batch.

- A write is acknowledged only after it is committed on a majority and applied.
- A write that was never proposed (still queued, or the leader stepped down first) fails definitively and is safe to retry.
- A write that was proposed but not yet applied when leadership was lost, or that timed out, fails with "outcome unknown". It may still commit. With a session, retrying it is safe. Without a session it can apply twice.
- A client that disconnects before its write is proposed has the write dropped. After proposal the write goes ahead regardless.
- Memory is bounded by `--max-pending-writes` times `--max-command-bytes`, plus the 1024-slot event channel.

## Design notes

Reads: a `Get` is not answered from local state, because the node could be a partitioned leader that a newer leader has superseded. The leader confirms it still holds a majority first (ReadIndex). `AppendEntries` carries a `read_ctx` that the follower echoes, so a delayed reply cannot confirm a newer read. A new leader serves no reads until it has committed an entry in its own term. Replies from non-members never count, and old peers without `read_ctx` cannot confirm reads.

Membership: it takes effect when an entry is applied, so changes must be strictly one at a time. A leader accepts `AddNode` or `RemoveNode` only after it has applied everything up to its own-term no-op. Until then the request fails with a retryable error. Without this rule a new leader could stack a second change on an unapplied one and produce non-intersecting quorums; `crates/raft-core/tests/membership_hostile.rs` reproduces that schedule. A node that is joining does not start elections until a committed entry confirms it.

Removal: a node that applies its own removal goes inert and writes `<data-dir>/node.removed`, after which the process refuses to start. To reuse the id, start a fresh process with an empty data directory and `--join`. Other nodes drop messages from removed ids before term handling, so a removed node cannot depose a leader or count toward a quorum. A leader cannot remove itself.

Elections: a node whose timer fires sends a pre-vote (the existing `RequestVote` RPC with a flag) and raises its term only if a majority would vote for it. A leader that hears from no majority for about 5 seconds steps down. During a rolling upgrade an old node treats a pre-vote as a real vote, so do not leave a cluster mixed (`docs/operations.md`).

Compaction: applied entries are replaced by a snapshot. A follower too far behind to catch up from the log receives the snapshot instead.

Disk full: each node checks its data volume once a second. Below `--min-free-mb` it rejects client writes, keeps serving reads and membership changes, and pauses compaction. It resumes by itself once free space is 25% above the floor.

## Benchmarks

`bench/run_all.sh` builds in release mode and runs four suites: engine, recovery, cluster and overload. Each benchmark does one discarded warm-up run and five measured runs. The tables show the median, and spread is (max - min) / median across the five. Every run reads its data back and compares it with what was written (WAL and LSM runs are reopened from disk first, cluster runs read every key with linearizable Gets, the overload run reads back every acknowledged write). A mismatch fails the run. All suites passed. The protocol is in `bench/README.md`.

These numbers come from one machine: an Intel Core i5-1240P (16 logical CPUs) with about 3.7 GB of RAM visible to the OS, on an ext4 disk where a 4 KiB fsync takes about 4 ms. Spread is often 15 to 50% and sometimes higher, so differences under about 30% are noise. Treat the numbers as relative, not as a performance claim.

### Durable storage (fsync in the timed path)

| Benchmark | Median | p50 | p99 |
|---|---:|---:|---:|
| raw 4 KiB fsync | 250 /s | 4.08 ms | 6.16 ms |
| WAL append, batch 1 | 245 entries/s | 4.29 ms | 6.05 ms |
| WAL append, batch 16 | 3,605 entries/s | 4.65 ms | 6.40 ms |
| WAL append, batch 256 | 47,864 entries/s | 5.10 ms | 5.77 ms |
| LSM apply, `sync_wal` on, batch 1 | 207 ops/s | 4.89 ms | 6.62 ms |
| LSM apply, `sync_wal` on, batch 64 | 11,387 ops/s | 5.39 ms | 8.60 ms |
| single-node Raft on WAL + LSM `sync_wal` | 97 ops/s | 10.24 ms | 16.56 ms |

One fsync costs about the same whether it covers one entry or 256, so throughput follows the batch size.

### CPU-bound (no fsync in the timed path)

| Benchmark | Median | Spread |
|---|---:|---:|
| in-memory state machine apply, batch 1 | 968,941 ops/s | 33% |
| in-memory state machine apply, batch 64 | 1,302,386 ops/s | 6% |
| LSM apply, `sync_wal` off, batch 64 | 659,609 ops/s | 2% |
| the same with exactly-once sessions | 423,799 ops/s | 5% |
| LSM point read | 173,553 ops/s | 5% |
| single-node Raft on an in-memory log | 909,100 ops/s | 16% |

Sessions cost about 36% of LSM apply throughput in this benchmark.

### Recovery

| Benchmark | 10,000 | 50,000 | 200,000 |
|---|---:|---:|---:|
| LSM open, full checksum (`verify_on_open` on) | 4.9 ms | 22.8 ms | 88.3 ms |
| LSM open, `verify_on_open` off | 0.3 ms | 0.7 ms | 21.9 ms |

| Benchmark | 1,000 | 10,000 | 50,000 |
|---|---:|---:|---:|
| Raft WAL reopen (entries) | 1.7 ms | 4.3 ms | 19.4 ms |
| LSM WAL replay (entries) | | 6.9 ms | 35.7 ms |

Killing all three nodes with `kill -9` and restarting them took 646 ms, 696 ms and 642 ms until a read of a written key succeeded, with 1,000, 10,000 and 40,000 writes behind it. That time is dominated by restart and election, not by data size.

### Real 3-node cluster

Three processes on one machine over gRPC, 64-byte values, 1,500 writes per run at 1 client and 4,000 at 16 and 64 clients. Latency is per request.

| Clients | Mode | Writes/s | p50 | p99 |
|---:|---|---:|---:|---:|
| 1 | unbatched | 91 | 11.4 ms | 18.1 ms |
| 1 | batched | 87 | 12.0 ms | 14.7 ms |
| 16 | unbatched | 115 | 143.5 ms | 198.0 ms |
| 16 | batched | 507 | 31.6 ms | 42.1 ms |
| 64 | unbatched | 113 | 573.0 ms | 794.9 ms |
| 64 | batched | 1,917 | 33.3 ms | 42.6 ms |

Batching changes nothing for one sequential client, gives about 4.4x at 16 clients and about 17x at 64. The LSM engine with `sync_wal` on, batched, at 16 clients did 463 writes/s (p50 34.9 ms, p99 48.0 ms). With the data directory on tmpfs, where fsync does nothing, 16 clients did 6,834 writes/s unbatched and 12,764 batched, so the disk is what limits the table above.

### Overload

Open-loop load well past capacity, no client retries.

| `--max-pending-writes` | Accepted writes/s | p50 | p99 |
|---|---:|---:|---:|
| 64 | 1,913 | 57.9 ms | 95.9 ms |
| 100,000 (limit effectively off) | 9,275 | 82.2 ms | 205.2 ms |

With the limit at 64 the node rejected the excess with "server overloaded", nothing timed out, and leader memory grew by about 70 MiB. With the limit off it accepted more writes per second, but leader memory grew by 500 to 700 MiB, and between a few hundred and about twenty thousand writes per run ended in errors. After the load stopped, the harness sometimes had to look for the new leader before it could read anything back. In every run of both configurations, every acknowledged write read back correctly.

## Testing

```
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

478 tests pass; one more, a timing measurement of backup cost, is ignored and run by hand. The count changes as tests are added.

- `raft-core`: elections, replication, leader failure, compaction, ReadIndex and membership, all driven by hand in an in-memory harness. `append_entries_safety.rs` pins each safety bug fixed so far. `membership_hostile.rs`, `prevote_checkquorum.rs` and `read_index_membership.rs` cover their respective areas.
- `storage`: WAL, hard state, manifest and LSM tests. `fault_injection.rs` fails every write, fsync, rename and directory fsync (before, after, or torn) and sweeps a scripted workload through every I/O step, comparing the recovered state with a reference model. `node_crash_recovery.rs` is a seeded multi-node simulation on real WAL files that crashes nodes, injects storage faults, and drops, duplicates and reorders messages while checking election safety, vote durability, state-machine safety, leader completeness and acknowledged-entry durability. It was mutation-checked: breaking persistence on purpose makes it fail. `RAFTDB_SIM_SEED=<n>` replays one seed.
- `kv`: encoding and the state machine, a chaos test, real `WalLog` clusters with batched proposals (`durable_cluster.rs`), exactly-once sessions on both engines (`sessions.rs`), and `linearizability.rs`, a history checker run over a simulation with crashes, partitions and compaction. The checker must also catch injected bugs (stale reads, acknowledging before commit).
- `raftdb`: write queue and node loop tests, plus integration tests against the real binary for authorization, TLS, backup and restore, crash after acknowledgment, session dedup, and large snapshots.
- `fuzz`: property tests and seeded random-byte loops for every on-disk and wire decoder. `cargo test -p raftdb-fuzz`.
- `raftdb-log`: event names, JSON output, line integrity, the panic hook.

Longer runs, all clean on the current tree:

```
RAFTDB_SIM_SEEDS=400 cargo test -p kv --test linearizability
RAFTDB_SIM_SEEDS=400 cargo test -p storage --test node_crash_recovery
PROPTEST_CASES=5000 cargo test -p raftdb-fuzz --release
```

Scripts that run real processes (Linux, `cargo build --release` first):

| Script | Checks |
|---|---|
| `scripts/crash_restart_check.sh` | Acknowledges batched writes from 8 clients, `kill -9`s all three nodes, restarts, verifies the data with compaction active |
| `scripts/fsync_failure_check.sh` | Makes one node's fsync fail (`VICTIM_ROLE=leader` or `VICTIM_NODE=<id>` to choose); it must exit with code 70 while the others keep serving. Needs a C compiler |
| `scripts/disk_full_check.sh` | Fills tiny volumes; needs root and `mount` |
| `scripts/tls_review.sh` | 71 checks against a real mutual-TLS cluster: startup validation, roles, refused connections, traffic capture, rotation and revocation with SIGHUP, identity binding, metrics, logs |
| `scripts/bench_cluster.sh` | A quick 3-node throughput comparison of batched and unbatched writes (`bench/run_all.sh` is the full harness) |

`docs/pre-release-checklist.md` lists what still has to be run on real hardware.

## Known limitations

- Node addresses are fixed at startup through `--peers` and `--known-peers`. A node restarted without a snapshot starts from its `--peers` list and re-derives later membership changes by replaying the log. Once a snapshot exists it is authoritative.
- Exactly-once needs a client session: one client id, strictly increasing sequence numbers, sent sequentially. Only the latest sequence per client is kept, and at most 10,000 sessions are kept (the oldest is evicted), so a very late retry from an evicted client can apply twice. Writes without a session are not deduplicated. Upgrade every node before any client uses sessions.
- TLS and authorization are off by default. Authorization is by whole method only, with no per-key permissions. Without it, any certificate the CA signed can do everything, including sending Raft messages. Switching a cluster to TLS needs a full restart. A node's own certificate and the ones nodes use toward each other are read at startup, so rotating them is a rolling restart; the authorization file reloads on SIGHUP. There is no CRL or OCSP: revoke through the authorization file. `--metrics-addr` is plain HTTP, so keep it on localhost. Without TLS, keep every port on localhost or a private network.
- Data at rest and backups are not encrypted. Use volume encryption.
- No MVCC, transactions or multi-key atomicity.
- A backup pauses the node for about 13 ms per MB of state (measured on one machine). States over 512 MiB are refused. Restore always creates a new single-node cluster, and you must not restore into a node id the old cluster can still reach.
- Snapshots are not streamed. A snapshot travels as one message and is held in memory, with several copies on the leader while a peer catches up. The per-transfer limit is about 512 MiB, and catch-up is tested up to a 40 MB state on both engines. While a peer lags, the leader copies the whole state once per replication round (every 150 ms), which cost about 27 ms per 40 MiB and 320 ms per 400 MiB on a slow machine. Keep the state below about 100 MiB; that figure is an estimate, not a measurement on real hardware.
- The WAL is rewritten in full on truncation and compaction. It is atomic but costs O(log size), and the leader keeps the whole uncompacted log in memory.
- Client writes and Raft messages share one 1024-slot channel into the node loop, and `--max-pending-writes` is applied only after a write leaves it. A sustained flood can delay heartbeats (`docs/dev-notes/engine-and-sessions.md`). With `--max-pending-writes 64` the overload benchmark stayed healthy. With the limit effectively off, leader memory grew by 500 to 700 MiB and leadership sometimes moved.
- Storage errors panic the node, because `LogStore` has no error channel. It restarts into recovery instead of degrading.
- `InstallSnapshotReply` carries only the term. A delayed reply after the leader has compacted again can make the leader briefly think a follower is further along than it is. It corrects itself on the next rejected `AppendEntries`.
- A follower whose own disk fills before the leader's can still stop with an out-of-space error. Alert on free space for every node.
- Not done: soak testing, real power-loss testing, and multi-machine and network-partition benchmarks. The Windows persistence paths (`MOVEFILE_WRITE_THROUGH`) are compiled but never run.
- Old-format `.hardstate` (16 bytes) and `.snapshot` files are rejected at startup, not migrated. There is no on-disk or wire compatibility guarantee between versions.

## Documentation

| File | Contents |
|---|---|
| `docs/operations.md` | Deployment, membership, upgrades, monitoring, failure handling |
| `docs/tls.md` | TLS and mutual TLS setup, rotation |
| `docs/authorization.md` | Roles, the authorization file, reload |
| `docs/observability.md` | Metrics, health endpoints, log events |
| `docs/backup-restore.md` | Backup contents, consistency, restore |
| `docs/security-review-brief.md` | What the TLS and authorization review covered |
| `docs/pre-release-checklist.md` | Checks to run on real hardware before relying on a build |
| `docs/dev-notes/` | Design notes: sessions, batching, the linearizability checker, backup and restore, large snapshots |
