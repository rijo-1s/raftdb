# Design notes: client sessions, batching and the linearizability checker

## Client sessions (exactly-once retries)

How a retried write is applied at most once.

- `Command::Session{client, seq, op}` (tag 3) wraps a Set or Delete. Every replica keeps, per client, the
  highest applied `seq` and the log index it was applied at.
  - `seq > last`: apply it.
  - `seq == last`: a retry of the latest write. It is acknowledged with the original result (success) and
    not applied again.
  - `seq < last`: not applied. The client gets an explicit "write superseded" error. It is never silently
    dropped and never acknowledged.
  - `client_id` and `seq` of 0 mean no session, which is the plain behavior.
- Half-specified sessions are refused before proposal, because an undecodable committed entry panics every
  replica.
- One client id is one sequential stream. Pipelining on one id gets its out-of-order writes rejected as
  superseded.
- `LsmStore` keeps session records in the engine under the reserved key prefix `0xFF"sess/"`. They are
  written in the same atomic `Lsm::apply` operation list as the data and the applied index, so recovery
  restores them with no change to the LSM, its WAL, its manifest or any recovery rule. Engine keys are
  bytes and user keys are valid UTF-8, so a user key can never collide with the prefix. User scans clamp
  their end bound below `0xFF`.
- A retry that is a duplicate still rewrites the unchanged session record, so the engine write is never
  empty and `applied_index` advances.
- Snapshots use a v2 format only when the session table is non-empty. An empty table gives a snapshot
  byte-identical to v1, so every existing snapshot and differential test still holds. v2 is a magic
  number, the explicit v1 body length, the sessions, and a CRC32 of everything. A damaged v2 magic reads as
  a roughly 4.29-billion-entry v1 count and is rejected by the v1 parser. Both engines emit identical
  bytes.
- The table is bounded to 10,000 clients (`kv::session::MAX_SESSIONS`). When it is full, the session with
  the oldest last-applied log index (ties broken by client id) is evicted, identically on every replica and
  persisted in the same write. A client idle long enough to be evicted loses its protection, so a very
  late retry would be applied again.
- Client library: `client::Session` has a random non-zero id and an increasing seq. `put` and `delete`
  retry up to 8 times with the same `(id, seq)` on transport errors and timeouts (rotating nodes), on
  "server overloaded", "outcome unknown" and "safe to retry", and they follow leader hints.
- `KvStore::restore` used to allocate `HashMap::with_capacity(count)` with `count` read from the file, so a
  flipped byte meant a multi-GB allocation. Both parsers now reject a count the bytes cannot hold.

Upgrades: a node that does not know command tag 3 cannot decode a session entry and panics applying it (the
existing "malformed committed entry is fatal" behavior). Upgrade every node before any client sends a
session. An old node also cannot read a v2 snapshot. Plain traffic is byte-compatible both ways. The
protocol change is two optional fields, `client_id` and `seq`, on `PutRequest` and `DeleteRequest` in
`raftdb.proto`. New fields default to 0, which means no session.

The eviction cap (10,000) and the client retry budget (8 attempts, 150 ms) are my choices, to be tuned on
real load.

### Tests

- `kv/session.rs` unit tests.
- `kv/tests/sessions.rs`: semantics on both engines, duplicates advance `applied_index`, durability across
  reopen, flush and compaction, snapshot round trips between engines in both directions, restore replacing
  old sessions, flipping every bit and truncating everywhere in a v2 snapshot through both engines (a
  rejected restore leaves the engine untouched), and corrupting a stored session record so that open fails
  closed and `open_or_rebuild` quarantines the directory. It also covers eviction parity, `Command` decode
  shape checks, and a randomized differential run with restarts and snapshot restores.
- `raftdb/tests/session_dedup.rs` (real binary): a retry after the ack, a stale seq, delete dedup, refusal
  of half-specified sessions, and `kill -9` plus restart plus a late retry on both engines, with and
  without snapshot compaction (`--compaction-threshold 3`).

Mutation check: making `LsmStore` stop persisting the session record on a fresh write fails four `kv` tests
and the end-to-end test.

On cost: in the engine apply microbenchmark sessions take about a third off LSM apply throughput (see the
README). In an end-to-end batched cluster run the difference was within noise. To compare on your own
hardware, run `harness sweep --set batched="" [--sessions] --conc 16,64`.

## Batching and tuning

The defaults (`--max-batch-entries 256`, `--max-batch-delay-us 0`) are unchanged because the sweeps did not
justify a change. Batch sizes from 16 to 1024 were indistinguishable within noise, and a delay of a few
hundred microseconds gave no gain while 2 ms cost throughput at 16 clients. `max-append-entries` and
`max-append-bytes` (replication batching) have not been swept. Use `harness sweep` for that on the target
hardware.

`crates/raftdb/tests/crash_after_ack.rs` runs the real binary, SIGKILLs it right after the ack (a lone write,
and a 32-way burst killed after 6 acks with the rest in flight), restarts on the same directory, and requires
every acknowledged write to read back, on the `mem` and `lsm` + `sync_wal` engines. Making
`WalLog::append` skip the durable write fails both tests. SIGKILL keeps the page cache, so this cannot
catch a missing fsync. The storage fault-injection tests cover that.

### Open issue: client writes share a channel with Raft traffic

`server.rs` sends every put into the node loop's 1024-slot channel before `max_pending_writes` is checked,
which happens later inside `WriteQueue`. That same channel carries peer traffic (AppendEntries, votes,
replies). Two consequences:

1. A client flood can delay heartbeats and acks behind it.
2. Up to 1024 full request payloads can sit in the channel outside the `max_pending_writes` accounting
   (worst case about 1024 times the gRPC maximum message size).

I wrote a version that sheds load in the gRPC handler using the `kv_pending_writes` gauge (reject-only, the
exact check stays in the queue), but I measured no benefit on a single-core machine and did not include it.
The unbounded overload benchmark (`--max-pending-writes 100000`) shows leadership churn and large memory
growth after a flood, which is consistent with this. To evaluate a change, run
`harness overload --bin <before>` against `--bin <after>` on a machine with a slow fsync.

## Linearizability checker

Test-only code in `crates/kv/tests/lincheck/mod.rs` and `crates/kv/tests/linearizability.rs`.

Approach: the Wing and Gong search with Lowe's memoisation of (linearised set, state) configurations, which
is the Knossos/Porcupine algorithm. It is applied per key, since linearizability is compositional across
independent objects (Herlihy and Wing; P-compositionality, Horn and Kroening). Each key is a register
with Set, Delete and Get. An operation with no response may take effect at any time after its call, or never.
A Get with no response has no effect and is dropped. A 20M-step budget turns a pathological history into
"inconclusive", which fails the test and is never a silent pass.

The simulation runs three `WalLog`-backed `KvNode`s with four concurrent clients and 700 steps per seed. It
injects crash and restart (with WAL recovery) and single-node partitions (clients can still reach an
isolated leader), and uses a compaction threshold of 25 so snapshots and installs happen. A write counts
as acknowledged when it is on the same node, same term, that node is still leader, and the entry is applied.
A not-leader refusal means not executed and is not recorded. A leader crash, step-down or timeout after
proposing means unknown outcome. Reads use ReadIndex.

Evidence:

- `RAFTDB_SIM_SEEDS=400` finds no violation.
- Two deliberately broken variants (stale local reads, acknowledging before commit) are flagged by the checker
  on real simulation output.
- Checker unit tests cover sequential histories, concurrent either-order, an impossible b-a-b read, a stale
  read after two writes, unknown-outcome writes that apply late or never, an operation that cannot apply
  before it was invoked, and per-key independence.
- Mutation check: a checker that accepts any Get value fails 7 of 8 tests. The eighth is the real-system
  test, which would pass vacuously with a broken checker, which is why the two broken-system tests exist.

Knobs: `RAFTDB_SIM_SEEDS=<n>` (default 30) and `RAFTDB_LIN_SEED=<s>`, which replays one seed and prints its
history.

Limits: a single-key register model (there are no multi-key transactions), partitions that isolate one node
at a time, no message loss, duplication or reordering beyond what partitions and crashes cause, and sessions
are not exercised by this simulation (they have their own tests).
