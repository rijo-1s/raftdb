# RaftDB benchmark harness

One command, from the repo root:

    bench/run_all.sh                    # full run: builds release, runs everything
    bench/run_all.sh --quick            # smoke test (small sizes), never record these
    BENCH_DIR=/path/on/real/disk bench/run_all.sh

Writes one JSON file per suite into `bench/results/`:

| file | what |
|---|---|
| `engine_cpu_bound.json` | KvStore, MemLog single-node Raft, LsmStore with sync_wal=off, LSM point reads. No fsync in the timed path. |
| `engine_durable.json`   | raw fsync probe, WalLog group commit (batch 1/16/256), LsmStore sync_wal=on, single-node Raft on fsynced WalLog + LSM |
| `recovery.json`         | LSM open time vs data size (verify_on_open on/off), LSM WAL replay, Raft WAL reopen |
| `cluster.json`          | real 3-process cluster, gRPC: batched vs unbatched at c=1/16/64; tmpfs (CPU-bound) variants; whole-cluster `kill -9` recovery vs data size |
| `overload.json`         | open-loop past capacity, no retries, bounded vs effectively-unbounded pending limit |

## Protocol (every benchmark)

* 1 discarded warm-up run, then >= 5 measured runs (`--quick`: 3). Median, min, max and
  `spread_pct = (max-min)/median` are recorded per metric, plus every raw run.
* Each file's `meta` records: exact command line, env overrides, git commit + dirty flag, CPU model,
  core count, RAM, kernel, rustc, build profile, and the filesystem/device behind the durable and
  CPU-bound directories. (`rotational` comes from sysfs and is unreliable on virtio disks.)
* Nothing is trusted unread. After each run the data is read back and compared with the expected
  value (WAL/LSM runs are closed and *reopened from disk* first; cluster runs read every key with
  linearizable Gets; the overload run reads back every *acknowledged* write). A mismatch fails the
  run and the whole harness exits non-zero. `verified_per_run` shows how many items were checked.
* CPU-bound vs durable is never mixed: the `mode` field says which, and cluster runs on tmpfs
  (`--cpu-dir`, default `/dev/shm`) have `fsync_effective: false`.

## Overload criteria (gated run, `max-pending-writes=64`)

Fails unless: some requests were rejected with `server overloaded`; zero other errors (no timeouts /
unknown outcomes); accepted p99 < 5 s; leader RSS growth < 128 MiB; every acked write reads back;
server still accepts writes afterwards. The second run (limit 100000) is informational contrast.

## Regression gate

    target/release/harness compare --baseline bench/baseline --current bench/results

`threshold = max(30%, 1.5 x max(spread_baseline, spread_current))` per metric (throughput/time and p99
separately). The 30% floor sits above the 5-25% run-to-run noise I saw on the test VM, and a noisier metric
widens its own gate. Any `ok:false` fails regardless of threshold. Record a baseline on the real
machine with the same commit, then re-tune `--floor-pct` from the measured `spread_pct` values.

## Tuning sweeps

    target/release/harness sweep --out /tmp/sw --writes 4000 --conc 16,64 \
        --set b16="--max-batch-entries 16" --set b256_default="" --set d500us="--max-batch-delay-us 500"

Runs the 3-node cluster once per flag set with the standard protocol (warm-up, >=5 runs, read-back
verification) and writes `sweep.json`. Always include the default flag set (`--set name=""`) in the same
sweep so every comparison is made in the same session, and judge differences against `spread_pct`.

## Exactly-once sessions

`--sessions` (cluster suites and `sweep`) sends every write with a `(client_id, seq)`; the result files
record `"sessions": true`. `cpu/lsm_nosync_apply_b64_sessions` is the engine-level counterpart of
`cpu/lsm_nosync_apply_b64`. Compare the pair within one session.
