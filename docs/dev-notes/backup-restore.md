# Design notes: backup and restore

User-facing behavior is in `docs/backup-restore.md`. This page records how it is built and why.

## Pieces

1. `kv/src/backup.rs`: the backup file format (magic and version, `last_applied`, term, peers, state, CRC-32).
   Its unit tests cover a round trip, flipping every byte (three masks), truncating at every offset,
   trailing bytes, 40k arbitrary inputs, checksum-valid garbage including hostile peer counts, and the
   semantic `check()` refusals.
2. The admin RPC `ClientApi.Backup` (additive proto: `BackupRequest`, `BackupReply`), `LoopEvent::Backup`,
   `NodeLoop::export_backup`, the gRPC handler, and the CLI `raftdb backup --addr --out`, which writes a
   temp file, fsyncs it and publishes it atomically without clobbering.
3. The CLI `raftdb restore --backup --data-dir --node-id` (`raftdb/src/backup.rs`).
4. `crates/raftdb/tests/backup_restore.rs`: five end-to-end tests through the real binary, on both engines.
5. `crates/kv/tests/backup_cost.rs`: an ignored timing measurement.

## Decisions

- The export validates and encodes off the node loop (in the server handler, via `spawn_blocking`). My first
  draft ran `check()` and the bitwise CRC inside the loop. Measuring showed that multiplied the pause, so
  only the barrier and `snapshot_bytes` run on the loop.
- A backup is published with `hard_link`, which fails if the name exists, instead of `rename`, so a
  concurrent file is never clobbered.
- Restore writes a hard state of (snapshot term, no vote). It refuses a non-empty target and accepts an
  empty directory.
- Restore verifies by reopening the staged directory with the real `WalLog::open` before the rename.
- Restore always creates a single-node cluster. There is no `--members` or multi-node restore.
- Backups of state over 512 MiB are refused, since a backup is a single gRPC message. There is no
  streaming.

## How it works

- The term at `last_applied` comes from `raft.log().term_at(last_applied)`. Peers come from `raft.peers()`
  (which excludes self) plus `raft.id`, sorted and deduplicated, the same convention as `compact_log`.
- The snapshot bytes come from `store.durability_barrier()` followed by `store.snapshot_bytes()`, the two
  calls compaction makes in `KvNode::step`. They include the session table (v2), and both engines yield
  identical bytes.
- The node loop does pause for exactly the barrier plus snapshot time. Measured in a release build with
  100-byte values: 100k keys (11.9 MB) took about 76 to 87 ms, and 500k keys (59.5 MB) about 0.7 to 0.8 s,
  roughly 13 ms per MB. That is the same cost as each compaction.
- `WalLog::open` refuses `StateInconsistent` if the snapshot term is above the persisted current term,
  `HardStateMissing` if a snapshot exists without a hard-state file, and `LogMissing` if the WAL file is
  absent while other files exist. So restore writes the WAL (through `open`), the snapshot and the hard state
  (term equal to the snapshot term).
- The engine rebuild path: `KvNode::with_state_machine` restores the store from the Raft snapshot when the
  engine's applied index is behind the boundary. It runs at every start and in the end-to-end tests (a
  restored directory has no `lsm/`).

## Not covered

- `run_restore`'s fault path (a `WalLog` write failing mid-restore) has no unit test. The code catches the
  panic and removes the staging directory, but I did not inject a fault there. The corrupt-backup and
  non-empty-target paths are tested end to end.

## A fix that came out of this

In a one-node cluster `add-node` used to hang until the next write, because `propose_add_node` appended the
entry without advancing the commit index (a one-node leader receives no replies). It now calls
`advance_commit_index()` like `become_leader` does. A multi-node leader is unaffected, and there is a test
for it.
