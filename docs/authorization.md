# Authorization (roles)

Mutual TLS (`docs/tls.md`) proves who is connecting. Authorization decides what they may do. It is off
by default: without `--authz-file` the server lets any trusted certificate do everything.

## Roles

| Role | May call |
|---|---|
| `read-only` | `Get` |
| `read-write` | `Get`, `Put`, `Delete` |
| `admin` | everything a client can do, including `AddNode`, `RemoveNode` and `Backup` |
| `node` | the Raft transport only: `RequestVote`, `AppendEntries`, `InstallSnapshot` |

Two properties worth knowing:

- Client certificates can never speak the Raft protocol, and node certificates can never use the client
  API. Without authorization, any certificate signed by your CA can send Raft messages (for example a
  vote request with a huge term, which forces the leader to step down). Every client role is refused
  on the transport, including `admin`.
- Deny by default. A method that is not in the table above needs `admin`. A test fails if an RPC is added
  to the proto file without being classified in `crates/raftdb/src/authz.rs`.

## Turning it on

1. Run with mutual TLS (`--tls-cert`, `--tls-key`, `--tls-ca`, `--tls-require-client-cert`).
   `--authz-file` is refused without it, because roles are identified by the verified client
   certificate.
2. Write the authorization file, one entry per line (`#` starts a comment):

   ```text
   # role         sha256 fingerprint of the certificate                       label
   node           3B:AA:...:91                                                node1
   admin          0123456789abcdef...                                         alice
   read-write     ...                                                         billing-service
   read-only      ...                                                         dashboard
   ```

   Colons in the fingerprint are optional. Get the fingerprint from a certificate with:

   ```sh
   openssl x509 -in client.pem -noout -fingerprint -sha256
   ```

3. Start every node with `--authz-file /etc/raftdb/authz.txt` (plus the TLS flags).

The file must contain at least one `node` entry (list every distinct node certificate; one certificate
shared by all nodes needs one line), and a certificate may be listed only once. A missing, unreadable or
malformed file stops the node before it listens.

The fingerprint is of the leaf certificate exactly as presented, so a renewed certificate is a new
identity and needs a new line.

## Node identity

Label a `node` entry `node1`, `node2`, ... (`node-3`, `node_3` and `3` also work). A Raft message from
that certificate must then claim the same node id. Anything else is refused with `PermissionDenied` and
logged as `node_identity_mismatch`, so a stolen node certificate cannot speak as another node. Entries
with other labels stay unbound and are listed in an `authz_node_unbound` warning.

## Changing the file without a restart

The file is read at startup and again when the node receives `SIGHUP`. To add,
change or remove an entry, edit the file on every node and send `SIGHUP` to each. The new policy applies
to the next request. If the new file is invalid, the node keeps the previous policy and logs
`authz_reload_failed`. A successful reload logs `authz_reloaded`.

This makes revocation immediate: delete the line and send `SIGHUP`. To renew a certificate without a gap,
add the new fingerprint and reload, switch the client to the new certificate, then remove the old line and
reload again.

## What happens to a refused request

The client gets gRPC status `PermissionDenied` with a short reason (the certificate is not listed, or the
role needed for the method). The server logs an `authz_denied` event with the certificate fingerprint
prefix, the method and the role needed. It logs at most one line per second, with a count of the refusals
left out.

## Audit

Every allowed admin operation (`backup`, `add-node`, `remove-node`) is logged as an `audit` event with the
certificate's label, role and method. `--audit-all` also logs every allowed client request, which is
verbose at high request rates. Raft traffic between nodes is never audited.

## Not provided

- Per-key or per-prefix permissions. Roles apply to whole methods.
- Revocation lists or OCSP. Revoke by removing the line and sending `SIGHUP`.
- Binding a node certificate to a node id unless the entry is labelled `node<ID>`.
- Authentication of the plain `--metrics-addr` endpoint. Use `--metrics-tls-addr` (HTTPS) and bind the
  HTTP one to localhost.

## Troubleshooting

- `PermissionDenied: this certificate is not listed in the authorization file`: the fingerprint in the
  file does not match the certificate the client is using. Recompute it and check for a renewed
  certificate.
- `PermissionDenied: role X may not call ...`: the entry exists but its role is too low.
- The cluster does not form after turning authorization on: the node certificates are not listed with
  role `node` on every node.
