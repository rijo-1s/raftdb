# TLS and mutual TLS

RaftDB can encrypt and authenticate every gRPC connection: node-to-node Raft traffic, the client API,
the CLI commands (`put`, `get`, `delete`, `add-node`, ...) and `backup`. It is off by default. With none
of the `--tls-*` flags the process uses plain gRPC.

## What you get, and what you don't

| You get | You do not get |
|---|---|
| Encryption on the wire (rustls, TLS 1.2 and 1.3) | Authorization on its own: by default a client with a trusted certificate can do everything. Turn on roles with `--authz-file` (`docs/authorization.md`) |
| Server authentication: clients verify the node's certificate | Encryption at rest (data directories and backup files are not encrypted; use volume encryption) |
| With `--tls-require-client-cert`: only holders of a certificate signed by your CA can connect, clients and peers alike | Certificate revocation lists or OCSP (revoke through the authorization file and `SIGHUP`, see `docs/authorization.md`) |
| A bad or partial TLS configuration stops the process and never falls back to plaintext | Hot reload of a node's own certificate (rotation is a rolling restart) |

## Flags

They work on `start` and on every client command, before or after the subcommand.

| Flag | Meaning |
|---|---|
| `--tls-cert <pem>` | This process's certificate chain. Used as the server certificate and, when connecting out, as the client identity. Needs `--tls-key` and `--tls-ca`. |
| `--tls-key <pem>` | Private key for `--tls-cert`. |
| `--tls-ca <pem>` | CA certificate(s) used to verify the other side of every connection. Required whenever TLS is on. On its own (client commands) it means "use TLS, verify the server". |
| `--tls-require-client-cert` | `start` only. Reject any connection that does not present a certificate signed by `--tls-ca`. Peers use their own `--tls-cert` to satisfy this. |
| `--tls-domain <name>` | Verify the server certificate against this name instead of the host in the address. |
| `--tls-strict-key-permissions` | Refuse to start if a key file is readable by group or others (Unix only). |

`--tls-ca` on its own is valid for client commands, but `raftdb start` refuses it: a node must also have
`--tls-cert` and `--tls-key`, otherwise it would accept plaintext while using TLS towards its peers.

Addresses without a scheme become `https://` when TLS is on. An explicit `http://` address with TLS on,
or `https://` without `--tls-ca`, is an error.

## Certificates

The certificate must be valid for the name or IP that clients and peers use in the address (a
`subjectAltName`), and for mutual TLS it must allow both server and client use. A recipe with `openssl`
that I have tested:

```sh
# 1. A private CA (keep ca.key offline; only ca.pem is distributed)
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -days 3650 -subj "/CN=raftdb CA" -keyout ca.key -out ca.pem

# 2. One certificate per node (repeat with its own names/IPs)
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -subj "/CN=raftdb-node1" -keyout node1.key -out node1.csr
printf "subjectAltName=DNS:node1.internal,IP:10.0.0.11\nextendedKeyUsage=serverAuth,clientAuth\n" > ext.cnf
openssl x509 -req -in node1.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -days 825 -extfile ext.cnf -out node1.pem

chmod 600 *.key
```

If you address nodes by IP but your certificates carry only DNS names, pass `--tls-domain <that name>`
everywhere.

## Running a TLS cluster

```sh
raftdb start --id 1 --addr 10.0.0.11:6001 --peers 2=10.0.0.12:6001,3=10.0.0.13:6001 \
  --data-dir /var/lib/raftdb \
  --tls-cert node1.pem --tls-key node1.key --tls-ca ca.pem --tls-require-client-cert

raftdb put --cluster 1=10.0.0.11:6001,2=10.0.0.12:6001,3=10.0.0.13:6001 \
  --tls-ca ca.pem --tls-cert client.pem --tls-key client.key  greeting hello

raftdb backup --addr 10.0.0.11:6001 --out b.bak --tls-ca ca.pem --tls-cert client.pem --tls-key client.key
```

Without `--tls-require-client-cert`, clients only need `--tls-ca`.

## Switching an existing plaintext cluster to TLS

TLS and plaintext nodes cannot talk to each other, so there is no rolling switch: stop every node, then
restart every node with the TLS flags. Plan a short outage. Data directories are unchanged by TLS. A node
you add later needs the same flags, since its peers dial it over TLS.

## Rotating certificates

Certificates are read once at startup. To rotate while keeping the same CA, replace a node's files and
restart it, one node at a time (the cluster keeps quorum). To change the CA itself, every node and
client must trust both CAs during the changeover (put both in the `--tls-ca` file), so do it in two
rounds.

A node started with `--tls-cert` reports when its certificate expires: a `tls_cert_expiry` log event at
startup (WARN within 30 days) and the metrics `tls_cert_not_after_seconds` and
`tls_cert_expires_in_seconds`. Alert on the latter (below 14 days, for example) and rotate in time. A
rotated certificate shows up after the restart that loads it. The CA certificate and the clients'
certificates are not monitored.

## Metrics and health over HTTPS

`--metrics-addr` is plain HTTP, which suits probes on localhost. To serve the same endpoints over TLS
add `--metrics-tls-addr 0.0.0.0:9100`. It uses the node's `--tls-cert` and `--tls-key`, and requires a
client certificate signed by `--tls-ca` exactly when `--tls-require-client-cert` is set. The two can run
together (HTTP on loopback for probes, HTTPS on the network for scraping). A Prometheus example:

```yaml
scheme: https
tls_config: { ca_file: ca.pem, cert_file: scraper.pem, key_file: scraper.key, server_name: localhost }
```

## Private key permissions

At startup a key file readable by group or others (anything beyond mode 600 or 400) logs a
`tls_key_permissions` warning. With `--tls-strict-key-permissions` the node refuses to start instead.
Unix only.

## Troubleshooting

- `TLS configuration error: ...` at startup: a flag is missing, a file cannot be read, or a file is not
  PEM. The node did not start and is not listening.
- Clients report `transport error`: the server refused the handshake. Typical causes are a client not
  using TLS, a CA that does not match, a certificate that does not cover the address (see
  `--tls-domain`), mutual TLS required while the client sent no certificate, or an expired certificate.
- A node cannot reach its peers after enabling TLS: every node must be restarted with TLS, and a single
  plaintext node is unreachable for the others.
- The cluster will not elect a leader and the logs are otherwise quiet: look for `peer_unreachable`
  events (one line per peer per 30 s, with a `suppressed` count). The `error` field carries the
  underlying cause, for example an untrusted or expired certificate, a hostname the certificate does not
  cover, or a plaintext peer.
