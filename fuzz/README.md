# raftdb-fuzz: property tests and a fuzz loop for every decoder

`cargo-fuzz` needs nightly, so this crate uses proptest plus a seeded random-bytes loop. Both run on
stable Rust.

## What is checked

| property | where |
|---|---|
| valid data round-trips | all suites |
| any corruption is detected, or loses ONLY the final record | `wal_props`, `lsm_props`, `raft_files_props`, in-file tests in `lsm/wal.rs`, `lsm/manifest.rs` |
| a refused start leaves every file byte-for-byte untouched | same suites (`snapshot_dir` before/after) |
| never panics on arbitrary bytes, including checksum-valid garbage | `*_props`, `forged_sst`, `fuzz_loop` |
| flip-every-byte and truncate-everywhere | `lsm_props::every_byte_*`, `raft_files_props::every_byte_*` |

Truncation exactly on a record boundary is *not* detectable at the file level (it is
indistinguishable from the records never having been written); the tests pin that precisely:
the survivors must be exactly the whole records before the cut. Raft's own log is what
re-supplies those entries at system level.

## Commands

```sh
# everything (property tests run as part of the normal suite)
cargo test --workspace --all-targets

# just this crate
cargo test -p raftdb-fuzz

# more cases for the proptest suites (default 120-400 per property)
PROPTEST_CASES=5000 cargo test -p raftdb-fuzz --release

# the seeded random-bytes loop: 9 targets, mutates the seed corpus in fuzz/corpus/
cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --target all --iters 20000 --seed 1
cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --target lsm-sst --iters 100000 --seed 7

# replay a crash it saved (it prints this exact command)
cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --target wal --replay fuzz/corpus/wal/crash-1-77

# regenerate the valid seed inputs
cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --emit-corpus

# the forged-index regression tests (hostile SSTable index offsets)
cargo test -p raftdb-fuzz --test forged_sst
```

Targets: `wal lsm-wal lsm-manifest lsm-sst raft-wal raft-hardstate raft-snapshot proto kv`.
Note: `--release` wraps integer overflow instead of panicking, so run the loop (or the
`forged_sst` test) in a debug build too if you are hunting overflow bugs.
