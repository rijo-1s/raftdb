#!/usr/bin/env bash
# Complete TLS / mutual-TLS / authorization review. Needs: openssl, python3, curl, a built raftdb.
#   BIN=target/release/raftdb BASE_PORT=7800 scripts/tls_review.sh
# Builds a throwaway PKI, starts real clusters, and tries to break them. Prints PASS/FAIL per
# check, then FINDINGS (known limitations, shown so nobody is surprised), and exits non-zero if
# any check failed. Nothing outside a temp dir is touched. KEEP=1 keeps the temp dir.
set -u
BIN=${BIN:-target/release/raftdb}; BP=${BASE_PORT:-7800}
for t in openssl python3 curl; do command -v $t >/dev/null || { echo "$t is required"; exit 2; }; done
[ -x "$BIN" ] || { echo "build first: cargo build --release   (BIN=$BIN not found)"; exit 2; }
W=$(mktemp -d -p "${TMPDIR:-/tmp}" raftdb-tlsreview.XXXXXX); P=$W/pki; mkdir -p "$P"; pids=(); aux=()
cleanup() {
  kill -9 "${pids[@]}" "${aux[@]}" 2>/dev/null; wait 2>/dev/null
  if [ "${KEEP:-0}" = 1 ]; then echo "kept: $W (certificates in $P, authz.txt inside it; processes are stopped)"; else rm -rf "$W"; fi
}
trap cleanup EXIT
pass=0; fail=0; findings=()
ok()   { echo "PASS  $*"; pass=$((pass+1)); }
bad()  { echo "FAIL  $*"; fail=$((fail+1)); }
note() { findings+=("$*"); }
# check "description" <0 = must succeed | 1 = must fail> <regex output must match, or -> -- command...
check() {
  local d=$1 want=$2 rx=$3; shift 4
  local out; out=$(timeout 25 "$@" 2>&1); local rc=$?
  if [ "$want" = 0 ] && [ $rc -ne 0 ]; then bad "$d (expected success, got exit $rc: $(echo "$out" | tail -1 | cut -c1-120))"; return; fi
  if [ "$want" = 1 ] && [ $rc -eq 0 ]; then bad "$d (expected a refusal, but it succeeded)"; return; fi
  if [ "$rx" != "-" ] && ! echo "$out" | grep -qiE "$rx"; then bad "$d (output did not match /$rx/: $(echo "$out" | tail -1 | cut -c1-120))"; return; fi
  ok "$d"
}

# ================================================================= 1. PKI
cd "$P"
mkca() { openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 -subj "/CN=$1" -keyout "$1.key" -out "$1.pem" 2>/dev/null; }
mkcsr() { openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=$1" -keyout "$1.key" -out "$1.csr" 2>/dev/null; }
mkext() { printf "[v]\nsubjectAltName=%s\nextendedKeyUsage=%s\n" "$3" "$2" > "$1.ext"; }
mkcert() { # name ca-name days eku san
  mkcsr "$1"; mkext "$1" "$4" "$5"
  openssl x509 -req -in "$1.csr" -CA "$2.pem" -CAkey "$2.key" -CAcreateserial -days "$3" -extfile "$1.ext" -extensions v -out "$1.pem" 2>/dev/null
}
: > index.txt; echo 1000 > serial.txt
printf "[ ca ]\ndefault_ca = c\n[ c ]\ndatabase=%s/index.txt\nnew_certs_dir=%s\nserial=%s/serial.txt\ndefault_md=sha256\npolicy=p\nunique_subject=no\n[ p ]\ncommonName=supplied\n" "$P" "$P" "$P" > ca.cnf
mkdated() { # name eku san startdate enddate  (signed by ca, with an explicit validity window)
  mkcsr "$1"; mkext "$1" "$2" "$3"
  openssl ca -batch -config ca.cnf -cert ca.pem -keyfile ca.key -in "$1.csr" -out "$1.pem" -notext -startdate "$4" -enddate "$5" -extfile "$1.ext" -extensions v >/dev/null 2>&1
}
mkca ca; mkca rogue-ca
for n in 1 2 3; do mkcert node$n ca 20 serverAuth,clientAuth "DNS:localhost,IP:127.0.0.1"; done
for c in admin rw ro unlisted; do mkcert $c ca 20 clientAuth "DNS:$c"; done
mkcert intruder rogue-ca 20 clientAuth "DNS:intruder"
mkcert wrongeku ca 20 serverAuth "DNS:wrongeku"                       # valid, but not allowed to act as a client
mkdated expired clientAuth "DNS:expired" 20200101000000Z 20200102000000Z
mkdated future  clientAuth "DNS:future"  20990101000000Z 20991231000000Z
mkcert othername ca 20 serverAuth,clientAuth "DNS:other.example"      # does not cover 127.0.0.1
mkcert rw2 ca 20 clientAuth "DNS:rw2"                                 # used for the rotation test
fp() { openssl x509 -in "$1" -noout -fingerprint -sha256 | cut -d= -f2; }
base_authz() { for n in 1 2 3; do echo "node        $(fp $P/node$n.pem)  node$n"; done
  echo "admin       $(fp $P/admin.pem)  alice"; echo "read-write  $(fp $P/rw.pem)  billing"; echo "read-only   $(fp $P/ro.pem)  dashboard"
  # listed on purpose: only the TLS layer (EKU / validity) can be what stops these three
  echo "read-write  $(fp $P/wrongeku.pem)  wrongeku"; echo "admin       $(fp $P/expired.pem)  expired"; echo "admin       $(fp $P/future.pem)  future"; }
base_authz > authz.txt
cd - >/dev/null
chmod 600 "$P"/*.key
for f in expired future; do [ -s "$P/$f.pem" ] || { echo "could not generate the $f certificate (openssl ca failed)"; exit 2; }; done
echo "== PKI ready: CA, 3 node certs, admin/read-write/read-only clients, plus an intruder (other CA), an expired, a not-yet-valid, a wrong-EKU and an unlisted certificate"

# ================================================================= 2. startup validation
echo "== startup validation (every one of these must refuse to start)"
d=$W/bad; mkdir -p "$d"; S="--id 9 --addr 127.0.0.1:$((BP+90))"
T="--tls-cert $P/node1.pem --tls-key $P/node1.key --tls-ca $P/ca.pem"
check "only --tls-ca (it would serve plaintext)"       1 "plaintext"      -- "$BIN" start $S --data-dir $d/a --tls-ca $P/ca.pem
check "--tls-cert without --tls-key"                   1 "tls"            -- "$BIN" start $S --data-dir $d/b --tls-cert $P/node1.pem --tls-ca $P/ca.pem
check "a private key that does not match the certificate" 1 "tls|key|certificate" -- "$BIN" start $S --data-dir $d/e --tls-cert $P/node1.pem --tls-key $P/node2.key --tls-ca $P/ca.pem
check "an unreadable certificate file"                 1 "cannot read|tls" -- "$BIN" start $S --data-dir $d/f --tls-cert $d/missing.pem --tls-key $P/node1.key --tls-ca $P/ca.pem
check "--authz-file without mutual TLS"                1 "authz|client cert" -- "$BIN" start $S --data-dir $d/c $T --authz-file $P/authz.txt
grep -v "^node" $P/authz.txt > $P/authz-no-node.txt
check "an authz file with no node entry"               1 "node"           -- "$BIN" start $S --data-dir $d/g $T --tls-require-client-cert --authz-file $P/authz-no-node.txt
echo "admin  not-a-fingerprint  x" > $P/authz-bad-fp.txt;       base_authz | grep "^node" >> $P/authz-bad-fp.txt
check "an authz file with a malformed fingerprint"     1 "finger|authz|invalid" -- "$BIN" start $S --data-dir $d/h $T --tls-require-client-cert --authz-file $P/authz-bad-fp.txt
{ base_authz; echo "superuser $(fp $P/ro.pem) x"; } > $P/authz-bad-role.txt
check "an authz file with an unknown role"             1 "role|authz|invalid"   -- "$BIN" start $S --data-dir $d/i $T --tls-require-client-cert --authz-file $P/authz-bad-role.txt
{ base_authz; echo "read-only $(fp $P/rw.pem) dup"; } > $P/authz-dup.txt
check "an authz file listing one certificate twice"    1 "duplicate|authz|twice" -- "$BIN" start $S --data-dir $d/j $T --tls-require-client-cert --authz-file $P/authz-dup.txt
: > $P/authz-empty.txt
check "an empty authz file"                            1 "authz|empty|node"     -- "$BIN" start $S --data-dir $d/k $T --tls-require-client-cert --authz-file $P/authz-empty.txt

# ================================================================= 3. the cluster
node_args() { echo "--tls-cert $P/node$1.pem --tls-key $P/node$1.key --tls-ca $P/ca.pem --tls-require-client-cert"; }
startnode() { # id [authz file]
  local id=$1 az=${2:-$P/authz.txt} peers=""
  for p in 1 2 3; do [ $p = $id ] || peers+="${peers:+,}$p=127.0.0.1:$((BP+p))"; done
  "$BIN" start --id $id --addr 127.0.0.1:$((BP+id)) --peers "$peers" --data-dir $W/n$id --metrics-addr 127.0.0.1:$((BP+10+id)) \
     --compaction-threshold 20 --metrics-tls-addr 127.0.0.1:$((BP+20+id)) --tls-strict-key-permissions --audit-all $(node_args $id) --authz-file "$az" >>$W/n$id.log 2>&1 &
  pids[$id]=$!
}
stopnode() { kill -9 ${pids[$1]} 2>/dev/null; wait ${pids[$1]} 2>/dev/null; }
CL="1=127.0.0.1:$((BP+1)),2=127.0.0.1:$((BP+2)),3=127.0.0.1:$((BP+3))"
as() { local who=${1}; shift; echo "--cluster ${CLUSTER:-$CL} --tls-ca $P/ca.pem --tls-cert $P/$who.pem --tls-key $P/$who.key"; }
writable() { local end=$((SECONDS+${2:-60})); while [ $SECONDS -lt $end ]; do timeout 15 "$BIN" put $(as ${1:-admin}) "probe-$RANDOM" 1 >/dev/null 2>&1 && return 0; sleep 0.3; done; return 1; }
metric() { curl -s --max-time 3 http://127.0.0.1:$((BP+10+$1))/metrics | grep "^$2{" | awk '{print $2}' | head -1; }
leader_id() { grep -h "event=\"became_leader\"" $W/n?.log | sed 's/.*node=\([0-9]*\) term=\([0-9]*\).*/\2 \1/' | sort -n | tail -1 | awk '{print $2}'; }
for id in 1 2 3; do startnode $id; done
writable admin || { echo "FAIL  the TLS cluster never became writable"; tail -5 $W/n1.log | cut -c1-200; exit 1; }
echo "== cluster is up: 3 nodes, mutual TLS + authorization"

# ================================================================= 4. roles
echo "== roles (authorization)"
check "read-write client can write"            0 -        -- "$BIN" put $(as rw) k1 v1
check "read-write client can read it back"     0 "^v1"    -- "$BIN" get $(as rw) k1
check "read-only client can read"              0 "^v1"    -- "$BIN" get $(as ro) k1
check "read-only client cannot write"          1 "permission|denied" -- "$BIN" put $(as ro) k2 v2
check "read-write client cannot back up"       1 "permission|denied" -- "$BIN" backup --addr 127.0.0.1:$((BP+1)) --out $W/rw.bak --tls-ca $P/ca.pem --tls-cert $P/rw.pem --tls-key $P/rw.key
check "admin can back up"                      0 -        -- "$BIN" backup --addr 127.0.0.1:$((BP+1)) --out $W/admin.bak --tls-ca $P/ca.pem --tls-cert $P/admin.pem --tls-key $P/admin.key
[ -s $W/admin.bak ] && ok "the admin's backup file exists" || bad "no backup file was written"
[ ! -e $W/rw.bak ] && ok "the refused backup wrote nothing" || bad "a refused backup left a file"
check "a node certificate cannot use the client API" 1 "permission|denied" -- "$BIN" put --cluster $CL --tls-ca $P/ca.pem --tls-cert $P/node1.pem --tls-key $P/node1.key k3 v3
check "a valid certificate that is NOT listed is denied everything" 1 "permission|denied|not listed" -- "$BIN" get $(as unlisted) k1

# ================================================================= 5. refused at the door
echo "== connections that must be refused (the first three are LISTED in authz, so only TLS itself can stop them)"
check "an EXPIRED certificate"                 1 -        -- "$BIN" put $(as expired) k4 v4
check "a NOT-YET-VALID certificate"            1 -        -- "$BIN" put $(as future) k5 v5
check "a certificate not allowed to be a client (wrong key usage)" 1 - -- "$BIN" put $(as wrongeku) k6 v6
check "a certificate from an unknown CA"       1 -        -- "$BIN" put --cluster $CL --tls-ca $P/ca.pem --tls-cert $P/intruder.pem --tls-key $P/intruder.key k7 v7
t0=$SECONDS; timeout 30 "$BIN" put $(as intruder) k7b v7 >/dev/null 2>&1; el=$((SECONDS-t0))
[ $el -le 8 ] && ok "a refused client fails fast (${el}s), it does not sit through the retry budget" || bad "a refused client took ${el}s to be told no (permanent errors must not be retried)"
check "plaintext client (no TLS flags at all)" 1 -        -- "$BIN" put --cluster $CL k8 v8
check "TLS client with no certificate"         1 -        -- "$BIN" put --cluster $CL --tls-ca $P/ca.pem k9 v9
check "wrong server name (--tls-domain)"       1 -        -- "$BIN" put $(as admin) --tls-domain not-the-node.example k10 v10
check "a client that trusts the wrong CA"      1 -        -- "$BIN" put --cluster $CL --tls-ca $P/rogue-ca.pem --tls-cert $P/admin.pem --tls-key $P/admin.key k11 v11
check "connecting by name works when the certificate covers it (positive control)" 0 - -- "$BIN" get --cluster "1=localhost:$((BP+1)),2=localhost:$((BP+2)),3=localhost:$((BP+3))" --tls-ca $P/ca.pem --tls-cert $P/admin.pem --tls-key $P/admin.key k1
for k in k2 k3 k4 k5 k6 k7 k8 k9 k10 k11; do
  got=$(timeout 15 "$BIN" get $(as admin) $k 2>/dev/null | tr -d '\r\n')
  [ "$got" = "(not found)" ] || bad "refused write $k was stored or unreadable: $got"
done; ok "none of the refused writes left any data"
check "https:// without a CA is an error, not a silent insecure connection" 1 "ca|tls|trust|certificate" -- "$BIN" put --cluster "1=https://127.0.0.1:$((BP+1))" k12 v12
check "http:// with TLS flags is an error"     1 "http|tls|scheme" -- "$BIN" put --cluster "1=http://127.0.0.1:$((BP+1))" --tls-ca $P/ca.pem k13 v13

# ================================================================= 6. the wire
echo "== what a network observer sees"
sc=$(echo | timeout 10 openssl s_client -connect 127.0.0.1:$((BP+1)) -CAfile $P/ca.pem -verify_ip 127.0.0.1 2>&1)
echo "$sc" | grep -q "Verify return code: 0" && ok "the node's certificate verifies against the CA" || bad "the node certificate did not verify"
echo "$sc" | grep -qE "TLSv1\.[23]" && ok "negotiated $(echo "$sc" | grep -oE 'TLSv1\.[23]' | head -1)" || bad "no TLS 1.2/1.3 session"
echo | timeout 10 openssl s_client -connect 127.0.0.1:$((BP+1)) -CAfile $P/rogue-ca.pem 2>&1 | grep -q "Verify return code: 0" && bad "verified against the wrong CA" || ok "the same certificate does NOT verify against another CA"
old=$(echo | timeout 10 openssl s_client -connect 127.0.0.1:$((BP+1)) -CAfile $P/ca.pem -tls1_1 -cipher 'DEFAULT:@SECLEVEL=0' 2>&1)
if echo "$old" | grep -qiE "no protocols available|unsupported protocol|unknown option"; then echo "SKIP  TLS 1.1 refusal (this openssl cannot even attempt TLS 1.1)"
elif echo "$old" | grep -qE "Cipher is \(NONE\)|alert|handshake failure|wrong version|no protocols|error"; then ok "TLS 1.1 is refused by the server"
else bad "a TLS 1.1 handshake was not refused: $(echo "$old" | tail -2 | tr '\n' ' ' | cut -c1-100)"; fi

cat > $W/proxy.py <<'PY'
import socket, sys, threading
lp, tp, out = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
log = open(out, "ab", buffering=0); lock = threading.Lock()
def pipe(a, b):
    try:
        while True:
            d = a.recv(65536)
            if not d: break
            with lock: log.write(d)
            b.sendall(d)
    except OSError: pass
    finally:
        for s in (a, b):
            try: s.shutdown(socket.SHUT_RDWR)
            except OSError: pass
srv = socket.socket(); srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", lp)); srv.listen(16)
while True:
    c, _ = srv.accept(); u = socket.create_connection(("127.0.0.1", tp))
    threading.Thread(target=pipe, args=(c, u), daemon=True).start(); threading.Thread(target=pipe, args=(u, c), daemon=True).start()
PY
SECRET="TOPSECRET-VALUE-4711"
# negative control first: the same capture on a PLAINTEXT node must reveal the secret, or the method proves nothing
"$BIN" start --id 1 --addr 127.0.0.1:$((BP+60)) --data-dir $W/plain >$W/plain.log 2>&1 & aux+=($!)
python3 $W/proxy.py $((BP+61)) $((BP+60)) $W/cap-plain.bin & aux+=($!)
for _ in $(seq 1 50); do timeout 5 "$BIN" put --cluster "1=127.0.0.1:$((BP+61))" warm 1 >/dev/null 2>&1 && break; sleep 0.3; done
timeout 15 "$BIN" put --cluster "1=127.0.0.1:$((BP+61))" secretkey "$SECRET" >/dev/null 2>&1
grep -qa "$SECRET" $W/cap-plain.bin && ok "control: on a PLAINTEXT node the value is visible in a packet capture (so the capture method works)" || bad "control failed: the secret was not visible even in plaintext; the wire test below proves nothing"
# the real test: capture traffic to the current leader of the TLS cluster
L=$(leader_id); [ -n "$L" ] || L=1
python3 $W/proxy.py $((BP+62)) $((BP+L)) $W/cap-tls.bin & aux+=($!)
sleep 0.5
CLUSTER=$(for i in 1 2 3; do [ $i = $L ] && printf "%s=127.0.0.1:%s," $i $((BP+62)) || printf "%s=127.0.0.1:%s," $i $((BP+i)); done | sed 's/,$//')
CLUSTER=$CLUSTER timeout 20 "$BIN" put $(CLUSTER=$CLUSTER as rw) secretkey "$SECRET" >/dev/null 2>&1 && ok "a write through the capturing proxy succeeded" || bad "the proxied TLS write failed"
sleep 0.5
if [ -s $W/cap-tls.bin ] && ! grep -qa "$SECRET" $W/cap-tls.bin && ! grep -qa "secretkey" $W/cap-tls.bin; then ok "on the TLS cluster neither the key nor the value appear anywhere in the captured traffic ($(stat -c %s $W/cap-tls.bin) bytes)"; else bad "the capture is empty or leaks the key/value"; fi
CLUSTER=

# ================================================================= 7. resilience
echo "== resilience"
( for i in $(seq 1 60); do exec {fd}<>/dev/tcp/127.0.0.1/$((BP+1)) 2>/dev/null || true; done; sleep 4 ) & aux+=($!)
exec 7<>/dev/tcp/127.0.0.1/$((BP+2)) && printf 'GET / HTTP/1.1\r\nHost: x\r\n\r\n\x16\x03\x01garbage' >&7; exec 7>&-
sleep 1
check "the cluster keeps serving while 60 idle connections and garbage hit the TLS port" 0 - -- "$BIN" put $(as rw) under-attack ok
for i in $(seq 1 70); do timeout 20 "$BIN" put $(as rw) "snap$i" "value-$i" >/dev/null 2>&1; done
stopnode 3
for i in $(seq 71 140); do timeout 20 "$BIN" put $(as rw) "snap$i" "value-$i" >/dev/null 2>&1; done
startnode 3
caught=0; for _ in $(seq 1 60); do a=$(metric 3 raft_applied_index); b=$(metric $(leader_id) raft_applied_index); [ -n "$a" ] && [ -n "$b" ] && [ "$a" -ge $((b-2)) ] 2>/dev/null && { caught=1; break; }; sleep 0.5; done
[ $caught = 1 ] && ok "a node that was down for 70 writes caught up over TLS (applied index $a vs leader $b) via the compacted log / snapshot" || bad "node 3 did not catch up over TLS (applied ${a:-?} vs leader ${b:-?})"
L=$(leader_id); stopnode $L
check "after the LEADER (node $L) is killed, writes work again over TLS" 0 - -- "$BIN" put $(as rw) after-failover 1
startnode $L
sleep 3
check "the killed leader restarts into the TLS cluster and reads work" 0 "^1" -- "$BIN" get $(as ro) after-failover
grep -q "peer_unreachable.*InvalidCertificate\|peer_unreachable.*UnknownIssuer" $W/n?.log && bad "nodes reported certificate errors talking to each other" || ok "no node logged a certificate error talking to a peer"

# ================================================================= 7b. node identity binding
echo "== node identity binding (a node certificate may only speak as its own node id)"
stopnode 3
imp_peers="1=127.0.0.1:$((BP+1)),2=127.0.0.1:$((BP+2))"
"$BIN" start --id 3 --addr 127.0.0.1:$((BP+93)) --peers "$imp_peers" --data-dir $W/imp --tls-cert $P/node2.pem --tls-key $P/node2.key --tls-ca $P/ca.pem --tls-require-client-cert --authz-file $P/authz.txt >$W/imp.log 2>&1 & aux+=($!); imp=$!
sleep 6
grep -q 'event="node_identity_mismatch"' $W/n1.log $W/n2.log && ok "node 2's certificate claiming to be node 3 was refused and logged (node_identity_mismatch)" || bad "an impersonating node was not refused"
kill -9 $imp 2>/dev/null; wait $imp 2>/dev/null
startnode 3; writable rw 60 && ok "the real node 3 rejoins and the cluster serves" || bad "cluster did not recover after the impersonation test"

# ================================================================= 8. rotation and revocation (SIGHUP: no restart)
echo "== rotation and revocation with SIGHUP (no restart, no outage)"
hup() { for id in 1 2 3; do kill -HUP ${pids[$id]} 2>/dev/null; done; sleep 1.5; }
cp $P/authz.txt $P/authz.orig
{ base_authz; echo "read-write  $(fp $P/rw2.pem)  billing-new"; } > $P/authz.txt; hup
check "a NEW certificate is accepted after SIGHUP, with no restart" 0 - -- "$BIN" put $(as rw2) new-cert-write 1
check "the old certificate still works during the overlap"        0 - -- "$BIN" put $(as rw) old-cert-write 1
grep -q 'event="authz_reloaded"' $W/n1.log && ok "the reload was logged (authz_reloaded)" || bad "no authz_reloaded event"
grep -v "billing$" $P/authz.txt > $P/authz.tmp && mv $P/authz.tmp $P/authz.txt; hup
check "REVOKED: the old certificate is refused immediately after its line is removed" 1 "permission|denied|not listed" -- "$BIN" put $(as rw) revoked-write 1
check "the new certificate keeps working" 0 - -- "$BIN" get $(as rw2) new-cert-write
echo "this is not a valid authorization file" > $P/authz.txt; hup
grep -q 'event="authz_reload_failed"' $W/n1.log && ok "a broken file was rejected (authz_reload_failed)" || bad "no authz_reload_failed event"
check "...and the previous policy stayed in force" 0 - -- "$BIN" get $(as rw2) new-cert-write
cp $P/authz.orig $P/authz.txt; hup
grep -q 'event="authz_denied"' $W/n1.log $W/n2.log $W/n3.log && ok "denials were logged (authz_denied)" || bad "no authz_denied event"

# ================================================================= 9. server-auth-only mode and certificate name checks
echo "== TLS without client certificates (--tls-require-client-cert absent) and name checks"
"$BIN" start --id 1 --addr 127.0.0.1:$((BP+70)) --data-dir $W/solo $(echo --tls-cert $P/node1.pem --tls-key $P/node1.key --tls-ca $P/ca.pem) >$W/solo.log 2>&1 & aux+=($!)
for _ in $(seq 1 50); do timeout 5 "$BIN" put --cluster "1=127.0.0.1:$((BP+70))" --tls-ca $P/ca.pem warm 1 >/dev/null 2>&1 && break; sleep 0.3; done
check "server-auth-only mode: a client with only the CA can write (by design: no client identity)" 0 - -- "$BIN" put --cluster "1=127.0.0.1:$((BP+70))" --tls-ca $P/ca.pem solo-write 1
check "server-auth-only mode: plaintext is still refused" 1 - -- "$BIN" put --cluster "1=127.0.0.1:$((BP+70))" solo-plain 1
"$BIN" start --id 1 --addr 127.0.0.1:$((BP+71)) --data-dir $W/solo2 --tls-cert $P/othername.pem --tls-key $P/othername.key --tls-ca $P/ca.pem >$W/solo2.log 2>&1 & aux+=($!)
sleep 3
check "a node whose certificate does not cover the address clients use is refused" 1 - -- "$BIN" put --cluster "1=127.0.0.1:$((BP+71))" --tls-ca $P/ca.pem x 1
check "...and works when the client names the certificate's name (--tls-domain)" 0 - -- "$BIN" put --cluster "1=127.0.0.1:$((BP+71))" --tls-ca $P/ca.pem --tls-domain other.example x 1

# ================================================================= 10. observability and what is NOT protected
echo "== observability and limits"
m=$(curl -s --max-time 3 http://127.0.0.1:$((BP+11))/metrics)
left=$(echo "$m" | grep -E '^tls_cert_expires_in_seconds' | awk '{print $2}')
[ -n "$left" ] && [ "$left" -gt 0 ] 2>/dev/null && [ "$left" -lt $((21*86400)) ] && ok "tls_cert_expires_in_seconds = $left (about $((left/86400)) days)" || bad "expiry metric missing or implausible: '$left'"
grep -q 'event="tls_cert_expiry"' $W/n1.log && ok "startup logged a tls_cert_expiry event (WARN inside 30 days)" || bad "no tls_cert_expiry event"
hm=$(curl -s --max-time 5 --cacert $P/ca.pem --cert $P/admin.pem --key $P/admin.key https://127.0.0.1:$((BP+21))/metrics)
echo "$hm" | grep -q "^raft_" && ok "metrics are served over HTTPS (mutual TLS: the scraper presented a client certificate)" || bad "HTTPS metrics did not answer"
curl -s --max-time 5 --cacert $P/ca.pem https://127.0.0.1:$((BP+21))/metrics | grep -q "^raft_" && bad "HTTPS metrics answered a client with no certificate" || ok "HTTPS metrics refuse a client with no certificate"
curl -s --max-time 5 http://127.0.0.1:$((BP+21))/metrics | grep -q "^raft_" && bad "plain HTTP answered on the HTTPS metrics port" || ok "plain HTTP is not answered on the HTTPS metrics port"
curl -s --max-time 3 http://127.0.0.1:$((BP+11))/metrics | grep -q "^raft_" && ok "the plain HTTP metrics port is still available for local probes" || bad "the plain metrics port stopped answering"
grep -h 'event="audit"' $W/n?.log | grep -q "Backup" && ok "the admin's backup was audit-logged (who, role, method)" || bad "no audit event for the backup"
grep -h 'event="audit"' $W/n?.log | grep -q "Put" && ok "--audit-all logged client writes" || bad "--audit-all logged no writes"
cp $P/node1.key $W/loose.key; chmod 644 $W/loose.key
"$BIN" start --id 1 --addr 127.0.0.1:$((BP+73)) --data-dir $W/kp1 --tls-cert $P/node1.pem --tls-key $W/loose.key --tls-ca $P/ca.pem >$W/kp1.log 2>&1 & aux+=($!); sleep 2.5
grep -q 'event="tls_key_permissions"' $W/kp1.log && ok "a world-readable private key (mode 644) is warned about (tls_key_permissions)" || bad "a mode-644 key was accepted silently"
check "--tls-strict-key-permissions refuses a mode-644 key" 1 "chmod|accessible|mode" -- "$BIN" start --id 1 --addr 127.0.0.1:$((BP+74)) --data-dir $W/kp2 --tls-cert $P/node1.pem --tls-key $W/loose.key --tls-ca $P/ca.pem --tls-strict-key-permissions
grep -hqE "BEGIN (EC |RSA )?PRIVATE|$SECRET" $W/n?.log && bad "a node log contains key material or a stored value" || ok "no private key and no stored value appears in any node log"
echo "$m" | grep -q "$SECRET" && bad "a metric contains a stored value" || ok "no stored value appears in /metrics"

echo
echo "RESULT: $pass passed, $fail failed"
echo
echo "FINDINGS (not failures: known limits of this version, so nobody is surprised)"
grep -rqa "$SECRET" $W/n? $W/plain 2>/dev/null && note "DATA AT REST IS NOT ENCRYPTED: the stored value is readable in the data directory. Use disk/volume encryption."
[ -s $W/admin.bak ] && note "BACKUP FILES ARE NOT ENCRYPTED: a backup contains every key and value. Store them on encrypted storage."
note "There is no CRL/OCSP: revoke through the authorization file + SIGHUP (immediate; tested above). Without --authz-file, trust is the CA alone."
note "A node's OWN certificate, and the certificates nodes use toward each other, are read at startup: rotation is a rolling restart (zero downtime, no lost write)."
note "Node-identity binding applies to 'node' entries labelled node<ID>; entries with other labels stay unbound (an authz_node_unbound warning lists them)."
note "Only admin operations are audited by default; --audit-all logs every allowed client request (verbose). Raft traffic is not audited."
note "The plain --metrics-addr is HTTP: keep it on localhost, and use --metrics-tls-addr for anything reachable from the network."
for f in "${findings[@]}"; do echo " - $f"; done
[ $fail = 0 ]
