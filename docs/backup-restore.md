# Backup and restore

Two commands:

```sh
# ONLINE, against a running node (leader or follower). Never overwrites a file.
raftdb backup  --addr 127.0.0.1:6001 --out /backups/2026-10-21.rdbbak

# OFFLINE, builds a NEW single-node data directory. The target must not exist
# or must be empty.
raftdb restore --backup /backups/2026-10-21.rdbbak --data-dir /data/restored --node-id 9
```

## What a backup contains

One checksummed file (`kv::backup`, format version 1):

| field | |
|---|---|
| `last_applied` | the applied log index the export is as-of |
| `last_applied_term` | the term of that entry |
| `peers` | the cluster membership at that point (informational: restore replaces it) |
| state | the state-machine snapshot: all keys and the exactly-once client session table |
| CRC-32 | over every preceding byte |

The state bytes come from the same two calls the node makes when it compacts its
log (`durability_barrier`, then `snapshot_bytes`), so they are byte-for-byte what a
Raft snapshot holds, with either storage engine. A backup taken from a `mem` node can
be restored into an `lsm` node and vice versa.

The file does not contain the Raft log tail, the hard state (term/vote), or the
engine directory. Raw-copying a live LSM directory is not consistent and is not supported.

## Consistency point

The export runs inside the node's event loop. The loop handles one event at a time
and applies a batch of committed entries to the state machine inside a single step, so
between events `last_applied` and the state machine agree exactly. The backup is the
state at `last_applied` at the instant the request is handled: not at the last
compaction, and not a mixture of two points in time. Everything in it is committed.

Writes acknowledged before `raftdb backup` was issued are in the backup. Writes
acknowledged afterwards may or may not be.

`backup` also works against a follower, which then serves a backup of its own applied
state (possibly slightly behind the leader) and spares the leader the pause.

## Cost: the node-loop pause

While the export runs the node does nothing else (no heartbeats, no client requests,
no replication). Only the barrier and the state serialization run on the loop; the
validation and checksum run afterwards on another thread. That is exactly the work each
compaction already does every `--compaction-threshold` applied entries.

Measured (release build, 100-byte values, one core; reproduce with
`cargo test -p kv --release --test backup_cost -- --ignored --nocapture`):

| keys | state size | `mem` engine | `lsm` engine |
|---|---|---|---|
| 10,000 | 1.2 MB | 3 ms | 6 ms |
| 100,000 | 11.9 MB | 87 ms | 76 ms |
| 500,000 | 59.5 MB | 806 ms | 680 ms |

Roughly 13 ms per MB of state. A long pause can make followers time out and
start an election, so back up a follower if your state is large. Debug builds are
several times slower. The whole state is also held in memory (again while it is
validated and encoded), and one backup is limited to 512 MiB of state (a single gRPC message):
larger databases are refused with a clear error rather than truncated.

## Restoring

1. Pick a fresh data directory, a fresh node id and new ports.
2. `raftdb restore --backup FILE --data-dir NEW_DIR --node-id N`
   - The backup is fully verified (checksum, structure, and that the state image
     parses) *before* anything is created.
   - The directory is built under a hidden sibling name, read back with the real
     recovery code, and published with one rename. A rejected or failed restore
     leaves nothing behind, and a non-empty target is refused untouched.
   - The result is a snapshot-only single-node cluster `{N}`: the snapshot at
     `last_applied`, a hard state with the snapshot's term and no vote, an empty log.
     The engine directory is deliberately not created.
3. `raftdb start --id N --addr NEW_ADDR --data-dir NEW_DIR [--engine lsm]`.
   On start the node finds the Raft snapshot ahead of its (empty) engine and rebuilds
   the engine from it, so either engine works. It elects itself leader alone and
   serves immediately, at `last_applied` plus one new-term no-op.
4. Check: `raftdb get --cluster N=NEW_ADDR some-key`.

### Adding nodes afterwards

Use the ordinary procedure (see the README): start the new node with `--join --peers
N=NEW_ADDR`, make sure the restored node was started with `--known-peers
<new id>=<its addr>`, then `raftdb add-node`.

A restored (or any one-node) cluster can grow right away: `add-node` completes without any
write first, because a one-node leader is its own quorum and commits the membership entry
at once. This is tested in `tests/backup_restore.rs` and `raft-core/tests/append_entries_safety.rs`.

### Do not restore under an existing node id

Never start a restored directory as a node of a cluster that is still running, and
never reuse an id or address the old cluster can still reach. A restored node has
forgotten everything it promised after the backup: votes it cast and entries it
acknowledged. If the old cluster counts that node as a member, it can vote twice in a
term or "un-acknowledge" entries that were already committed with its help, which can
lose committed data. A restored node is a new identity: a new id, a fresh directory,
new ports, a new cluster that grows from one node. The old cluster should be shut down
(or the member removed with `remove-node`) before its replacement is brought up.

## Client sessions (exactly-once)

The session table is part of the state, so it is part of the backup. After a restore, a
client retrying a write that was acknowledged before the backup (same client id and
sequence number) is acknowledged again but not applied twice. This is covered by the
test `restore_brings_back_every_key_the_state_and_the_exactly_once_sessions`.

Backups that carry sessions use snapshot format v2. Nodes from before the session
layer cannot read it; restore into a binary at least as new as the one that took the
backup.

## Limits

- No encryption. The file holds every key and value in clear text. Protect it like
  the database: file permissions, an encrypted volume, encrypted transport to wherever
  it is stored.
- No point-in-time recovery between backups. You can restore to the instant of a
  backup and no other. Anything written after the newest backup is lost. Take backups
  often enough for your recovery-point requirement; there is no log shipping.
- Backups are never overwritten, and are never deleted by these commands.
- Not a substitute for replication: it protects against operator error and total loss,
  and gives you a rollback point.
