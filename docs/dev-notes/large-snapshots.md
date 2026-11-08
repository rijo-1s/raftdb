# Design notes: large snapshots

## What was fixed
A node that joins, or falls behind the compacted part of the log, is brought up to date with a
snapshot of the whole state, sent as one `InstallSnapshot` message. Three things went wrong once the
state was large:

1. 4 MiB message limit. The Raft transport used gRPC's default 4 MiB limit for incoming messages,
   so a follower rejected any larger snapshot. The leader dropped the error silently and retried
   forever; in a 2-node cluster the next write then blocked, because the new node was needed for the
   quorum. `tests/large_snapshot.rs` reproduces it (without the fix the write after `add-node`
   times out). The fix raises the limit on the transport service
   (`kv::backup::MAX_MESSAGE_BYTES`, about 512 MiB, the same bound as backups).
2. Flat 2-second peer timeout for every peer call, including snapshots. A snapshot now gets extra
   time in proportion to its size (assuming at least 8 MiB/s): `peer_rpc_timeout` in `node_loop.rs`.
3. Re-sent on every replication round. `raft-core` builds (and copies) the snapshot message
   every time it replicates to a lagging peer, with no throttle. The node loop now allows one
   snapshot transfer in flight per peer (`SnapshotInflight`, released on completion, failure,
   timeout or panic), so transfers cannot pile up.

A failed or timed-out snapshot transfer logs `snapshot_send_failed`.

Single values are limited to 1 MiB per command, so a large state means many entries, not big ones.

## Not done: streaming snapshots
Snapshots are still one in-memory message, and `raft-core` still copies the whole snapshot once per
replication round while a peer lags (every 150 ms). A rough test that copied a buffer of that size on a
slow shared machine took about 27 ms for 40 MiB and about 320 ms for 400 MiB. That is longer than a heartbeat at
a few hundred MiB, so keep the state well below that (about 100 MiB is a cautious estimate, not a
measurement on real hardware). I chose not to throttle inside `raft-core`: the builder is called from
four places (two are reply-driven resends) and would need new state in safety-critical code.

What true streaming needs, in order, each with corruption tests for any new on-disk format:
1. A chunked `InstallSnapshot` (offset, done flag, per-chunk checks) with the follower assembling the
   snapshot in a temporary file and installing it atomically only when complete (Raft paper, figure 13).
   Must handle: duplicate or out-of-order chunks, a new term or leader mid-transfer, restarts.
2. A streaming snapshot writer and reader in `storage` (temporary file, incremental CRC, fsync,
   rename) so neither side holds the whole state in memory.
3. Exporting engine state incrementally. The LSM engine can give a stable view cheaply (immutable
   tables plus a frozen memtable); the in-memory engine would need the node loop paused for the whole
   export, or a copy, so its pause stays proportional to the state size.
4. A server-streaming backup RPC and a backup file format that can be written and verified in chunks.
