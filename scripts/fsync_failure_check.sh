#!/usr/bin/env bash
# Storage-failure check (Linux only; needs a C compiler).
#
# Starts a 3-node cluster, makes ONE node's fsync fail with EIO, and checks:
#   1. that node exits with code 70 (it must not linger as a zombie with its
#      port open and a dead event loop), and
#   2. the other two nodes keep serving: a write and a read still succeed.
#
# Env: BIN (default target/release/raftdb), BASE_PORT (default 7400),
#      VICTIM_ROLE=follower|leader (default follower), CC (default cc),
#      VICTIM_NODE=<1|2|3> to pick the node explicitly (1 also exercises the
#      client failing over past a dead first node).
set -euo pipefail
BIN=${BIN:-target/release/raftdb}
BASE_PORT=${BASE_PORT:-7400}
VICTIM_ROLE=${VICTIM_ROLE:-follower}
CC=${CC:-cc}
here=$(cd "$(dirname "$0")" && pwd)
dir=$(mktemp -d -p "${TMPDIR:-/tmp}" raftdb-fsyncfail.XXXXXX)
pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$dir"; }
trap cleanup EXIT
fail() { echo "FAIL: $*"; for f in "$dir"/n*.log; do echo "--- $f"; tail -5 "$f" | cut -c1-200; done; exit 1; }

"$CC" -shared -fPIC -o "$dir/failsync.so" "$here/failsync.c" -ldl
addr() { echo "127.0.0.1:$((BASE_PORT + $1))"; }
cluster="1=$(addr 1),2=$(addr 2),3=$(addr 3)"
for id in 1 2 3; do
  peers=""
  for p in 1 2 3; do [ "$p" = "$id" ] && continue; peers+="${peers:+,}$p=$(addr "$p")"; done
  LD_PRELOAD="$dir/failsync.so" FSYNC_FAIL_FILE="$dir/fail.n$id" \
    "$BIN" start --id "$id" --addr "$(addr "$id")" --peers "$peers" \
    --data-dir "$dir/n$id" >"$dir/n$id.log" 2>&1 &
  pids+=($!)
done

leader=""
for _ in $(seq 1 100); do
  leader=$(grep -l became_leader "$dir"/n*.log 2>/dev/null | head -1 | sed 's/.*n\([0-9]\)\.log/\1/' || true)
  [ -n "$leader" ] && "$BIN" put --cluster "$cluster" warmup 1 >/dev/null 2>&1 && break
  sleep 0.2
done
[ -n "$leader" ] || fail "no leader elected"

if [ -n "${VICTIM_NODE:-}" ]; then victim=$VICTIM_NODE
elif [ "$VICTIM_ROLE" = leader ]; then victim=$leader
else victim=$(( leader % 3 + 1 )); fi
role=follower; [ "$victim" = "$leader" ] && role=leader
echo "leader = node $leader; making node $victim's fsync fail ($role)"
touch "$dir/fail.n$victim"
# Drive writes so the victim has to fsync; errors here are expected when the
# victim is the leader (leadership is lost), so ignore the client's result.
"$BIN" bench --cluster "$cluster" --count 300 --value-size 64 --concurrency 4 >/dev/null 2>&1 || true

vpid=${pids[$((victim - 1))]}
for _ in $(seq 1 100); do kill -0 "$vpid" 2>/dev/null || break; sleep 0.1; done
if kill -0 "$vpid" 2>/dev/null; then fail "node $victim is still running after its storage failed (zombie)"; fi
rc=0; wait "$vpid" || rc=$?
[ "$rc" = 70 ] || fail "node $victim exited with code $rc, expected 70"
grep -q 'event="node_loop_died"' "$dir/n$victim.log" || fail "no node_loop_died event in node $victim's log"
echo "ok: node $victim exited with code 70 and logged node_loop_died"

ok=0
for _ in $(seq 1 60); do
  if "$BIN" put --cluster "$cluster" after-failure yes >/dev/null 2>&1 \
     && [ "$("$BIN" get --cluster "$cluster" after-failure 2>/dev/null | tr -d '\r\n' )" != "" ]; then ok=1; break; fi
  sleep 0.5
done
[ "$ok" = 1 ] || fail "the surviving nodes stopped serving"
echo "ok: the remaining two nodes still accept writes and serve reads"
echo "PASS"
