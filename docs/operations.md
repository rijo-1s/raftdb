# Operations runbook

How to deploy, run, monitor, upgrade and recover RaftDB. It matches the flags and behavior of this
version, and anything I have not tested says so.

Related: `docs/tls.md` (encryption), `docs/authorization.md`, `docs/backup-restore.md`,
`docs/observability.md` (metrics, health, log events), `docs/pre-release-checklist.md`.

## 1. Decide before you deploy

| Decision | Guidance |
|---|---|
| Cluster size | 3 nodes tolerate 1 failure, 5 tolerate 2. Use an odd number. A cluster with no quorum cannot accept writes. |
| Engine (`--engine`) | `mem` (default): the state lives in RAM and is rebuilt from the Raft snapshot and log on start, so size RAM for all your data. `lsm`: the state lives on disk (memtable, SSTables, cache), for data larger than RAM. |
| Disk | Local SSD or NVMe, one per node, never shared between nodes. fsync latency is the main limit on write speed (see the benchmarks in the README). Do not disable fsync. |
| Network | Nodes must reach each other on their `--addr` port, and clients need the same port. Keep `--metrics-addr` on localhost or a private network (plain HTTP, no authentication). |
| TLS | Off by default. Turn it on before any network you do not fully control (`docs/tls.md`). |
| State size | Keep the state below about 100 MiB: a joining or lagging node receives the whole state as one in-memory message (`docs/dev-notes/large-snapshots.md`). |
| Capacity | Run `bench/run_all.sh` on your own hardware first (`docs/pre-release-checklist.md`). |

## 2. Files and permissions

- `--data-dir` holds the Raft log, the vote and term state, and snapshots. With `--engine lsm` it also
  holds an `lsm/` directory. Run the process as a dedicated user that owns the directory (mode 700).
  Do not edit or copy these files by hand while the node runs.
- If the LSM engine fails its startup validation, the node moves it aside as
  `lsm.corrupt-<timestamp>` and rebuilds from the Raft state. The program never deletes these. Once the
  node is healthy and you hold a fresh backup, you may delete them to free space.
- TLS keys: mode 600, owned by the service user.

## 3. Starting a cluster

Three nodes, one per machine (the addresses are examples):

```sh
raftdb start --id 1 --addr 10.0.0.11:6001 --peers 2=10.0.0.12:6001,3=10.0.0.13:6001 \
  --data-dir /var/lib/raftdb --engine lsm --metrics-addr 127.0.0.1:9001
raftdb start --id 2 --addr 10.0.0.12:6001 --peers 1=10.0.0.11:6001,3=10.0.0.13:6001 ...
raftdb start --id 3 --addr 10.0.0.13:6001 --peers 1=10.0.0.11:6001,2=10.0.0.12:6001 ...
```

`--peers` lists the other nodes. When TLS is on, add the `--tls-*` flags from `docs/tls.md` to every
node and every client command.

### systemd unit (example)

```ini
[Unit]
Description=RaftDB node
After=network-online.target
Wants=network-online.target

[Service]
User=raftdb
ExecStart=/usr/local/bin/raftdb start --id 1 --addr 10.0.0.11:6001 \
  --peers 2=10.0.0.12:6001,3=10.0.0.13:6001 --data-dir /var/lib/raftdb \
  --engine lsm --metrics-addr 127.0.0.1:9001
Restart=on-failure
RestartSec=2
TimeoutStopSec=30
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

- Shutdown: SIGTERM (and Ctrl-C) start an orderly shutdown. `/readyz` turns 503 first so load
  balancers stop sending traffic, while `/healthz` stays 200.
- Restart policy: if the node loop dies the process exits with code 70 on purpose, because it must not
  linger with its port open and nothing serving. `Restart=on-failure` restarts it and recovery runs on
  start. A node that keeps exiting needs its logs read, not another restart.

## 4. Changing membership

- Add a node: start it with `--join --peers <current members>`. Start the existing nodes with
  `--known-peers <new id>=<addr>` so they can already reach it. Then run
  `raftdb add-node --cluster ... <new id>` against the cluster. The new node catches up from the
  leader. Make one change at a time and wait until it completes.
- "retry shortly": right after an election, membership changes are refused until the new leader has
  applied its first entry. Retry after a moment. This is a safety rule.
- Remove a node: `raftdb remove-node --cluster ... <id>`. A removed node stays removed. To reuse its id,
  start a fresh process with an empty data directory.
- Replace a dead node: remove it, then add the replacement as a new node with an empty data directory.
  Do not shrink below a working quorum while doing so.
- With TLS, every node and client uses the same flags. A plaintext node cannot join a TLS cluster.

## 5. Backups

See `docs/backup-restore.md`. The routine I use:

1. Take a backup from a follower on a schedule (nightly is a reasonable start). It pauses that node's
   loop while the state is serialised, about 13 ms per MB in my measurements on one machine. Measure
   on yours.
2. Copy backups off the machine. They are not encrypted.
3. Restore into a scratch directory on new ports at least monthly and after every upgrade. A backup
   that was never restored is not a backup.

## 6. Upgrades

There is no on-disk or wire compatibility guarantee between versions yet.

1. Take a backup and read the release notes for format changes.
2. Try the new version against a copy of the data first.
3. Client sessions (exactly-once retries): upgrade every node before any client uses sessions. An
   older node cannot apply the session command or read the v2 snapshot.
4. Switching on TLS needs all nodes restarted together. There is no rolling switch.
5. PreVote and CheckQuorum can be rolled out one node at a time, but an old node treats a pre-vote as a
   real vote request. In a mixed cluster the old nodes decide elections and the protection is
   incomplete, so do not leave a cluster mixed.
6. Otherwise upgrade one node at a time. Wait for `/readyz` to return 200 and for the node to catch up
   before touching the next. If a node refuses to start, stop and roll that node back and leave the
   others alone.

## 7. What to monitor

Metric and event names are in `docs/observability.md`. A first set of alerts:

| Alert | Source |
|---|---|
| Node not ready for more than a few minutes | `node_ready` = 0 or `/readyz` != 200 |
| Node loop stalled | `node_healthy` = 0, or `node_loop_heartbeat_age_seconds` high |
| Storage failure | `lsm_poisoned` = 1, any increase of `raft_wal_fsync_errors_total`, `storage_failure` or `node_loop_died` log events |
| Slow disk | `raft_wal_fsync_max_seconds` high, `slow_fsync` log events |
| Write latency | p99 of `kv_write_latency_seconds` above your target |
| A node falling behind | `raft_applied_index` stuck while others advance |
| Frequent leader changes | repeated `became_leader` log events |
| A node cannot catch up | repeated `snapshot_send_failed` events for the same peer |
| Disk nearly full | `node_writes_protected` = 1 (page: client writes are being refused), `node_disk_free_bytes` trending down, `disk_space_changed` and `compaction_skipped` log events |
| Certificate expiry | `tls_cert_expires_in_seconds` below about 14 days (page: rotate by restarting nodes one at a time, see `docs/tls.md`) |
| A node cannot reach a peer (wrong CA, expired certificate, plaintext peer, bad address) | `peer_unreachable` log events; the `error` field names the cause |
| Node refused to start | `recovery_refused` and `lsm_quarantined` log events |
| Refused requests (probing, or a wrong or expired client certificate) | `authz_denied` log events |

## 8. When things go wrong

| Symptom | What to do |
|---|---|
| One node down, others up | Writes continue (quorum). Restart it and it catches up. If its disk is gone, replace it (section 4). |
| Writes fail, no leader | Quorum lost (partition or too many nodes down). Restore connectivity or restart nodes. Do not run `remove-node` to "fix" this. |
| Majority lost permanently | Restore the newest backup as a new single-node cluster on fresh directories and new ports, then add nodes (`docs/backup-restore.md`). Writes after that backup are lost. |
| Node exits with code 70 | The node loop died (a panic or a storage failure). Check the logs (`node_loop_died`, `storage_failure`, `panic`). A failing disk shows up in `raft_wal_fsync_errors_total`. |
| Node refuses to start ("recovery refused") | The data directory is inconsistent. The program changes nothing and keeps the files. Do not delete anything: keep the directory, restore a backup, or rebuild that node as a new member. |
| Clients get "server overloaded" | Backpressure: more writes are queued than `--max-pending-writes` allows. Slow the clients down or add capacity. Do not raise the limit blindly. |
| A write times out | The outcome is unknown, since it may still commit. Retry only with a client session (exactly-once). Plain writes are not deduplicated. |

## 9. Full disk

Every node fills at about the same rate, so a full volume tends to hit the whole cluster at once. Each
node checks the volume holding `--data-dir` once a second. Below `--min-free-mb` (default 256) it
rejects new client writes with "data volume nearly full", keeps serving reads and membership changes,
and pauses log compaction (a snapshot needs about its own size in free space while it is written, and an
out-of-space error there is fatal). It resumes by itself once free space is 25% above the floor. A
compaction is also skipped, with a `compaction_skipped` event, when there is no room for its snapshot.
To recover, free space or grow the volume on the nodes. No restart is needed. `--min-free-mb 0` turns
the protection off.

A follower cannot refuse entries the leader replicates. If a follower's volume is much fuller than the
leader's it can still hit a real out-of-space error and stop (fail-stop; restart it after freeing
space). Alert on free space of every node, not just the leader. `scripts/disk_full_check.sh` (Linux,
root) reproduces the scenario on tiny volumes.

## 10. Elections

A node whose election timer fires first asks its peers for a pre-vote and raises its term only if a
majority would vote for it, so a slow or briefly cut-off follower cannot depose a healthy leader. A
leader that has heard from no majority for about 5 seconds steps down on its own (CheckQuorum). A peer
that is receiving a snapshot from it counts as reachable. Both are always on.

## 11. Security operations

- Metrics over HTTPS: `--metrics-tls-addr` (see `docs/tls.md`). Keep the plain `--metrics-addr` on
  localhost.
- Add or revoke a client certificate with no restart: edit the authorization file on every node, then
  `kill -HUP $(pidof raftdb)` on each. Look for `authz_reloaded` (or `authz_reload_failed`) in the logs.
- Rotate a node's own certificate: replace the files and restart nodes one at a time (the cluster keeps
  its quorum), and alert on `tls_cert_expires_in_seconds`.
- Audit trail: `grep '"event":"audit"'` (or `event="audit"` in text logs). Admin operations are always
  logged. Start with `--audit-all` to log every allowed client request.
- Key files: keep them at mode 600, and start with `--tls-strict-key-permissions` to enforce it.
- Data at rest (stored values and backup files) is not encrypted by RaftDB. Use volume encryption.

## 12. Known limits

See "Known limitations" in the README. In short: authorization is optional and applies to whole
methods only (`docs/authorization.md`), and without it any trusted certificate can do everything.
There is no encryption at rest, no MVCC or transactions, and snapshots are not streamed: they are held in
memory and copied once per replication round while a peer lags, so keep the state below about 100 MiB
(a rough estimate). Backups over 512 MiB are refused. I have not done soak testing or real power-loss
testing. A follower whose own disk fills before the leader's can still stop (section 9).
