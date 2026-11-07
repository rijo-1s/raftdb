#!/usr/bin/env bash
# One command: build (release), run every benchmark suite, write
# bench/results/*.json. Exit status is non-zero if any benchmark errored or
# any read-back verification / overload criterion failed.
#
#   bench/run_all.sh                 # full run (>= 5 measured runs + 1 warm-up each)
#   bench/run_all.sh --quick         # smoke test only; do NOT record these numbers
#   BENCH_DIR=/mnt/nvme/tmp bench/run_all.sh   # choose the durable filesystem
#
# Extra flags are passed to the harness (see `harness` usage): --runs N,
# --warmup N, --out DIR, --cpu-dir DIR, --drop-caches ...
#
# Regression check against a saved baseline:
#   target/release/harness compare --baseline bench/baseline --current bench/results
set -euo pipefail
cd "$(dirname "$0")/.."

cargo build --release -p raftdb -p raftdb-bench
exec target/release/harness all "$@"
