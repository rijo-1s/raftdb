# Security review: TLS, mutual TLS and authorization

The TLS, mutual TLS and authorization code has been reviewed. This page records what it claims, what it
deliberately does not do, where the code is, and how the main design questions were answered.

## What is claimed

1. With `--tls-cert/--tls-key/--tls-ca`, every gRPC connection a node accepts or makes (client API and
   Raft peer traffic) and every CLI command (including `backup`) uses TLS (rustls via tonic; TLS 1.2
   and 1.3 defaults).
2. With `--tls-require-client-cert`, a connection is accepted only if the peer presents a certificate
   chaining to `--tls-ca`. Peers authenticate to each other the same way.
3. A misconfiguration never degrades to plaintext. A half-configured or unreadable setup stops the
   process before it binds a port (`crates/raftdb/src/tls.rs`, tested).
4. TLS is off by default. With no `--tls-*` flag the behavior is plain gRPC.

## Not provided

- Fine-grained authorization. Roles exist (`docs/authorization.md`) but apply to whole methods, and there
  are no per-key permissions. Without `--authz-file` any holder of a trusted certificate can do
  everything, including sending Raft messages.
- Revocation lists or OCSP. Revoke through the authorization file and `SIGHUP`, which is immediate. The
  node's own certificate still needs a restart to rotate, and expiry is monitored
  (`tls_cert_expires_in_seconds`).
- `--metrics-addr` is plain, unauthenticated HTTP. `--metrics-tls-addr` serves the same endpoints over
  HTTPS.
- Encryption at rest (data directories and backup files).
- Node identity binding applies to `node` entries labelled `node<ID>`. Unlabelled entries stay unbound
  (`check_sender` in `authz.rs` and the three Raft handlers in `server.rs`).
- Rate limiting or abuse protection on the client API.

## Where the code is

| Area | Files |
|---|---|
| Flag validation, config building, PEM checks | `crates/raftdb/src/tls.rs` |
| Server setup, address scheme rules, startup refusal | `crates/raftdb/src/main.rs` (`parse_id_addr_list`, `main`, `run_node`) |
| Outbound peer channels | `crates/raftdb/src/node_loop.rs` (`dispatch_one`) |
| Client and backup connections | `crates/raftdb/src/client.rs` (`connect`), `crates/raftdb/src/backup.rs` |
| Authorization layer and node identity binding | `crates/raftdb/src/authz.rs`, `crates/raftdb/src/server.rs` |
| Behavior tests (real processes, real certificates) | `crates/raftdb/tests/tls.rs`, `crates/raftdb/tests/authz.rs`, `scripts/tls_review.sh` |
| Operator documentation | `docs/tls.md`, `docs/authorization.md` |

## Design points

1. Trust model. One CA verifies every connection, for clients and for peers. Within that, roles are
   attached to the SHA-256 fingerprint of the leaf certificate, and node and client roles are strictly
   separate: a client certificate cannot speak the Raft protocol and a node certificate cannot use the
   client API.
2. Name verification. Nodes are dialled by the host in the address, or by `--tls-domain`, and the
   certificate must cover that name or IP. `scripts/tls_review.sh` checks a wrong server name and a
   certificate that does not cover the address.
3. Peer impersonation. A `node` entry labelled `node<ID>` binds that certificate to the node id. A Raft
   message from it must claim the same id, otherwise it is refused and logged as
   `node_identity_mismatch`. The check is at the Raft layer, in the three Raft handlers.
4. Failure modes. `scripts/tls_review.sh` covers a handshake with an unknown CA, expired, not-yet-valid,
   wrong-EKU and unlisted certificates, a plaintext client, a plaintext peer, and every half-configured
   startup. Each is refused, and a bad startup configuration stops the node.
5. Dependencies. The TLS feature pulls in rustls 0.22, ring and tokio-rustls. rcgen is a test-only
   dependency.
6. Authorization bypass. Roles are checked in a tower layer in front of both gRPC services, with deny by
   default for any method that is not classified. A test fails if an RPC is added to the proto file
   without being classified. Certificates are identified by fingerprint, with no name parsing.
