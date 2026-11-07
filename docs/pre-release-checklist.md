# Pre-release checklist

What I run on my own hardware, on the same kind of machine and disk I will run on, before calling a
build ready. I tick each box and keep the output with the release.

## A. Code gates

- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace --all-targets`
- [ ] `RAFTDB_SIM_SEEDS=400 cargo test -p kv --test linearizability` and
      `RAFTDB_SIM_SEEDS=400 cargo test -p storage --test node_crash_recovery`
- [ ] `PROPTEST_CASES=5000 cargo test -p raftdb-fuzz --release`
- [ ] `cargo build --workspace --release`
- [ ] CI is green on the release commit.

## B. Real-machine checks

I am not planning soak tests, so the README says the system has not been validated under multi-hour or
multi-day load. The checks below are what I do on real hardware before relying on the system for data
I cannot lose.

1. Benchmarks. `bench/run_all.sh` (not `--quick`) on the real disk
   (`BENCH_DIR=/path/on/real/disk`). Commit the JSON in `bench/results/`.
2. Crash and restart. `scripts/crash_restart_check.sh` kills all nodes with `kill -9` and checks that
   acknowledged data survives.
3. Storage failure. `scripts/fsync_failure_check.sh` (needs a C compiler) makes one node's fsync fail
   and checks that it exits with code 70 while the others keep serving. Run it with
   `VICTIM_ROLE=leader` and `VICTIM_NODE=1` as well.
4. Fuzz loop. Run the `fuzz_loop` binary from `fuzz/` for as long as I can spare, for example
   `cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --target all --iters 20000000 --seed 1`
   (options in `fuzz/README.md`). Any crash is saved so it can be replayed with `--replay`.
5. Soak (not planned). If I ever do it: a multi-hour run of the 3-node cluster under steady write load
   while I `kill -9` a random node every few minutes. Latency, memory (RSS), open files and disk usage
   must stay flat in `/metrics`. This is the largest gap against "production".
6. Disk and network faults. With `tc netem` (delay, loss, partition) and a fault-injecting block device
   (`dm-flakey`, or LazyFS for lost un-fsynced writes), run the cluster under load for a few hours.
   Expect no acknowledged write lost, no node stuck, and a clear log event for every refusal to start.
7. Large-state catch-up. Grow the state to the largest size I expect, add a node, and watch
   `snapshot_send_failed` and `diag_stall` events on the leader. A stall longer than the 150 ms
   heartbeat while the node catches up means the state is too big for non-streaming snapshots.
8. Full disk. `scripts/disk_full_check.sh` (Linux, root) refuses writes cleanly, keeps all nodes up and
   reads working, and resumes writes when space is freed. On real hardware I repeat it on the real
   disk and check that `--min-free-mb` is larger than the largest snapshot I expect.
9. Backup and restore drill. Follow `docs/backup-restore.md` end to end on a copy of real data and
   compare contents.
10. Upgrade drill. From the previous release to this one on a copy of a real data directory
    (`docs/operations.md`, section 6).

## C. Security

- [x] TLS, mutual TLS and authorization code reviewed (`docs/security-review-brief.md`).
- [ ] TLS and mutual TLS enabled in staging, plaintext clients refused (`scripts/tls_review.sh`).
- [ ] Authorization turned on in staging (`--authz-file`, `docs/authorization.md`): every client has the
      least role it needs and the file lists every node certificate.
- [ ] Private keys are mode 600, and certificate expiry dates are in a calendar or monitored.
- [ ] `--metrics-addr` is not reachable from outside the private network.

## D. Documentation and release

- [ ] README "Known limitations" matches reality (update it after each of the above).
- [ ] Release notes list on-disk or wire format changes and the upgrade order.
- [ ] The release commit is tagged and `Cargo.lock` is committed.
