#!/usr/bin/env bash
# End-to-end benchmark: spin up a real 3-node cluster on localhost (separate
# processes, real gRPC, real fsyncs on $BENCH_DIR), then compare unbatched
# (--no-batching: one fsync + one replication round per write) with group
# commit at several client concurrencies.
#
#   cargo build --release && scripts/bench_cluster.sh
#
# Env: BIN, COUNT (writes per run), VALUE_SIZE, BENCH_DIR, BASE_PORT,
#      CONCS (space-separated client concurrencies, default "1 16 64").
#
# If a run fails, its nodes are stopped, its logs are KEPT, and the leadership
# and loop-stall events from every node are printed.
set -euo pipefail

BIN=${BIN:-target/release/raftdb}
COUNT=${COUNT:-4000}
VALUE_SIZE=${VALUE_SIZE:-64}
BASE_PORT=${BASE_PORT:-7100}
BENCH_DIR=${BENCH_DIR:-${TMPDIR:-/tmp}}
CONCS=${CONCS:-"1 16 64"}
KEEP_LOGS=0

addr() { echo "127.0.0.1:$((BASE_PORT + $1))"; }
cluster_arg() { echo "1=$(addr 1),2=$(addr 2),3=$(addr 3)"; }

run_one() { # label  concurrency  extra-start-flags...
  local label=$1 conc=$2; shift 2
  local dir; dir=$(mktemp -d -p "$BENCH_DIR" raftdb-bench.XXXXXX)
  local pids=()
  for id in 1 2 3; do
    local peers=""
    for p in 1 2 3; do
      [ "$p" = "$id" ] && continue
      peers+="${peers:+,}$p=$(addr $p)"
    done
    "$BIN" start --id "$id" --addr "$(addr $id)" --peers "$peers" \
      --data-dir "$dir/n$id" --compaction-threshold 100000 "$@" \
      >"$dir/n$id.log" 2>&1 &
    pids+=($!)
  done
  cleanup() {
    kill "${pids[@]}" 2>/dev/null || true
    wait "${pids[@]}" 2>/dev/null || true
    if [ "$KEEP_LOGS" = 0 ]; then rm -rf "$dir"; else echo "  node logs kept in $dir"; fi
  }
  trap cleanup RETURN

  # Wait for a leader to accept a write.
  local ok=0
  for _ in $(seq 1 100); do
    if "$BIN" put --cluster "$(cluster_arg)" warmup 1 >/dev/null 2>&1; then ok=1; break; fi
    sleep 0.2
  done
  [ "$ok" = 1 ] || { echo "cluster never became writable ($label)"; cat "$dir"/n1.log; KEEP_LOGS=1; return 1; }

  echo "=== $label | concurrency=$conc | $COUNT writes x ${VALUE_SIZE}B ==="
  local rc=0
  "$BIN" bench --cluster "$(cluster_arg)" --count "$COUNT" \
    --value-size "$VALUE_SIZE" --concurrency "$conc" 2>&1 | sed 's/^/  /' || rc=$?
  if [ "$rc" != 0 ]; then
    KEEP_LOGS=1
    echo "  bench FAILED (exit $rc). Node events, oldest first:"
    grep -hE 'diag_|became_leader|stepped_down|storage_failure' "$dir"/n*.log \
      | cut -c1-200 | sort | head -80 | sed 's/^/    /'
    return "$rc"
  fi
}

for conc in $CONCS; do
  run_one "unbatched (--no-batching)" "$conc" --no-batching
  run_one "batched (defaults)       " "$conc"
done
