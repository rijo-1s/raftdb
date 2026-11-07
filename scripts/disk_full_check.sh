#!/usr/bin/env bash
# Disk-full check (Linux; needs root to mount small tmpfs volumes).
#
# Runs a 3-node cluster with every data directory on its own tiny volume and
# writes until the volumes run low, then checks that
#   1. all three nodes stay up (a full disk can kill nodes, nearly at once),
#   2. a write is refused with a clear "data volume nearly full" error,
#   3. reads keep working, and
#   4. once space is freed, writes are accepted again with no restart.
#
# Env: BIN (default target/release/raftdb), BASE_PORT (default 7600),
#      DISK_MB (12), BALLAST_MB (4, the file deleted to "free space"),
#      FLOOR_MB (3, passed as --min-free-mb).
set -uo pipefail
BIN=${BIN:-target/release/raftdb}
BASE_PORT=${BASE_PORT:-7600}
DISK_MB=${DISK_MB:-12}; BALLAST_MB=${BALLAST_MB:-4}; FLOOR_MB=${FLOOR_MB:-3}

if [ "$(id -u)" != 0 ] || ! command -v mount >/dev/null; then
  echo "SKIP: needs root and mount(8) to create small volumes"; exit 0
fi
work=$(mktemp -d -p "${TMPDIR:-/tmp}" raftdb-diskfull.XXXXXX)
pids=()
cleanup() {
  kill -9 "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true
  for i in 1 2 3; do umount "$work/disk$i" 2>/dev/null || true; done
  rm -rf "$work"
}
trap cleanup EXIT
fail() { echo "FAIL: $*"; for f in "$work"/n*.log; do echo "--- $f"; grep -E "disk_space|node_loop_died|panicked|No space|became_leader|stepped_down|diag_election|diag_stall|peer_unreachable" "$f" | tail -8 | cut -c1-250; done; exit 1; }

addr() { echo "127.0.0.1:$((BASE_PORT + $1))"; }
cluster="1=$(addr 1),2=$(addr 2),3=$(addr 3)"
for i in 1 2 3; do
  mkdir -p "$work/disk$i"
  mount -t tmpfs -o "size=${DISK_MB}m" tmpfs "$work/disk$i" || { echo "SKIP: cannot mount tmpfs"; exit 0; }
  dd if=/dev/zero of="$work/disk$i/ballast" bs=1M count="$BALLAST_MB" 2>/dev/null
done
for id in 1 2 3; do
  peers=""; for p in 1 2 3; do [ "$p" = "$id" ] && continue; peers+="${peers:+,}$p=$(addr "$p")"; done
  "$BIN" start --id "$id" --addr "$(addr "$id")" --peers "$peers" \
    --data-dir "$work/disk$id/d" --min-free-mb "$FLOOR_MB" --compaction-threshold 20 \
    >"$work/n$id.log" 2>&1 &
  pids+=($!)
done
ok=0
for _ in $(seq 1 100); do "$BIN" put --cluster "$cluster" warmup 1 >/dev/null 2>&1 && { ok=1; break; }; sleep 0.2; done
[ "$ok" = 1 ] || fail "cluster never became writable"
"$BIN" put --cluster "$cluster" first "the-first-value" >/dev/null 2>&1 || fail "initial write failed"

value=$(head -c 32768 /dev/zero | tr '\0' x)
refused=0; written=0
for i in $(seq 1 600); do
  out=$("$BIN" put --cluster "$cluster" "k$i" "$value" 2>&1); rc=$?
  if [ $rc -eq 0 ]; then written=$((written + 1)); continue; fi
  case "$out" in
    *"data volume nearly full"*) refused=1; break ;;
    *) fail "write $i failed with something other than the disk-full refusal: $(echo "$out" | tail -1 | cut -c1-200)" ;;
  esac
done
[ "$refused" = 1 ] || fail "wrote $written values and was never refused: the guard did not engage"
echo "ok: refused with a clear disk-full error after $written writes of 32 KiB"

for i in 1 2 3; do kill -0 "${pids[$((i - 1))]}" 2>/dev/null || fail "node $i died when its disk filled"; done
echo "ok: all three nodes are still running"
got=$("$BIN" get --cluster "$cluster" first 2>/dev/null | tr -d '\r\n')
[ "$got" = "the-first-value" ] || fail "a read during the shortage returned: $got"
echo "ok: reads still work"
out=$("$BIN" put --cluster "$cluster" again 1 2>&1); case "$out" in *"data volume nearly full"*) ;; *) fail "a second write was not refused: $out" ;; esac

for i in 1 2 3; do rm -f "$work/disk$i/ballast"; done
resumed=0
for _ in $(seq 1 40); do
  "$BIN" put --cluster "$cluster" resumed yes >/dev/null 2>&1 && { resumed=1; break; }; sleep 0.5
done
[ "$resumed" = 1 ] || fail "writes did not resume after space was freed"
[ "$("$BIN" get --cluster "$cluster" resumed 2>/dev/null | tr -d '\r\n')" = "yes" ] || fail "the resumed write was not readable"
echo "ok: writes resumed by themselves after space was freed (no restart)"
for i in 1 2 3; do kill -0 "${pids[$((i - 1))]}" 2>/dev/null || fail "node $i died after recovery"; done
grep -lE "node_loop_died|panicked" "$work"/n*.log >/dev/null 2>&1 && fail "a node logged a fatal error"
echo PASS
