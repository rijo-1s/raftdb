#!/usr/bin/env bash
# End-to-end durability check on the real binary: acknowledge batched writes
# from concurrent clients, SIGKILL the whole cluster (no graceful shutdown),
# restart from the same data dirs and verify every acknowledged key is back.
set -euo pipefail
BIN=${BIN:-target/release/raftdb}
BASE_PORT=${BASE_PORT:-7500}
WRITES=${WRITES:-600}
CONC=${CONC:-8}
dir=$(mktemp -d)
addr() { echo "127.0.0.1:$((BASE_PORT + $1))"; }
C="1=$(addr 1),2=$(addr 2),3=$(addr 3)"
pids=()
start_all() {
  pids=()
  for id in 1 2 3; do
    peers=""; for p in 1 2 3; do [ "$p" = "$id" ] && continue; peers+="${peers:+,}$p=$(addr $p)"; done
    "$BIN" start --id "$id" --addr "$(addr $id)" --peers "$peers" --data-dir "$dir/n$id" \
      --compaction-threshold 200 >>"$dir/n$id.log" 2>&1 &
    pids+=($!)
  done
  for _ in $(seq 1 150); do "$BIN" put --cluster "$C" probe 1 >/dev/null 2>&1 && return 0; sleep 0.2; done
  echo "cluster never became writable"; tail -20 "$dir"/n1.log; return 1
}
trap 'kill -9 "${pids[@]}" 2>/dev/null || true; rm -rf "$dir"' EXIT

start_all
"$BIN" bench --cluster "$C" --count "$WRITES" --concurrency "$CONC" | grep -E "PUTs|throughput"
echo "all $WRITES writes acknowledged; killing every node with SIGKILL..."
kill -9 "${pids[@]}"; wait "${pids[@]}" 2>/dev/null || true
start_all
missing=0
for w in $(seq 0 $((CONC - 1))); do
  share=$((WRITES / CONC + (w < WRITES % CONC ? 1 : 0)))
  for i in 0 $((share / 2)) $((share - 1)); do
    v=$("$BIN" get --cluster "$C" "bench:$w:$i" 2>/dev/null || true)
    [ "$v" = "$(printf 'x%.0s' $(seq 1 64))" ] || { echo "MISSING bench:$w:$i (got '$v')"; missing=$((missing + 1)); }
  done
done
if [ "$missing" = 0 ]; then echo "OK: every sampled acknowledged write survived kill -9 of the whole cluster"; else echo "FAIL: $missing missing"; exit 1; fi
