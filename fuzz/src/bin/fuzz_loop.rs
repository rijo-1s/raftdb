//! Seeded random-bytes fuzz loop (a stand-in for cargo-fuzz, which needs
//! nightly). For every target it mutates valid seed inputs - and sometimes
//! generates pure noise - and feeds them to the real decoders, requiring:
//!
//!   * no panic, ever;
//!   * a refused open leaves the directory byte-for-byte untouched.
//!
//! Failures are reproducible: the failing input is written to
//! `fuzz/corpus/<target>/crash-<seed>-<iter>` and the exact command is printed.
//!
//!   cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --target all --iters 20000 --seed 1
//!   cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --emit-corpus     # regenerate seeds
//!   cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --replay fuzz/corpus/wal/crash-1-77 --target wal

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use prost::Message as _;
use raftdb_fuzz::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const TARGETS: &[&str] = &[
    "wal",
    "lsm-wal",
    "lsm-manifest",
    "lsm-sst",
    "raft-wal",
    "raft-hardstate",
    "raft-snapshot",
    "proto",
    "kv",
];

fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

/// The valid seed bytes for a target (what mutations start from).
fn seed_bytes(target: &str) -> Vec<u8> {
    let tmp = tempfile::tempdir().unwrap();
    let pick = |dir: &Path, prefix: &str, ext: &str| -> Vec<u8> {
        let f = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                let n = p.file_name().unwrap().to_string_lossy().to_string();
                n.starts_with(prefix) && n.ends_with(ext)
            });
        std::fs::read(f.unwrap()).unwrap()
    };
    match target {
        "wal" => wal_fixture_bytes(),
        "lsm-wal" | "lsm-manifest" | "lsm-sst" => {
            lsm_fixture(tmp.path());
            match target {
                "lsm-wal" => pick(tmp.path(), "wal-", ".log"),
                "lsm-manifest" => pick(tmp.path(), "MANIFEST", ""),
                _ => pick(tmp.path(), "sst-", ".sst"),
            }
        }
        "raft-wal" | "raft-hardstate" | "raft-snapshot" => {
            raft_fixture(tmp.path());
            let suffix = match target {
                "raft-wal" => "node.wal",
                "raft-hardstate" => "node.wal.hardstate",
                _ => "node.wal.snapshot",
            };
            std::fs::read(tmp.path().join(suffix)).unwrap()
        }
        "proto" => proto_seed(),
        "kv" => kv::Command::Set {
            key: "key".into(),
            value: "value".into(),
        }
        .encode(),
        _ => unreachable!(),
    }
}

fn proto_seed() -> Vec<u8> {
    raftdb_proto::AppendEntriesRequest {
        term: 3,
        leader_id: 1,
        prev_log_index: 4,
        prev_log_term: 2,
        entries: vec![raftdb_proto::LogEntry {
            term: 3,
            index: 5,
            data: b"payload".to_vec(),
            kind_tag: 0,
            kind_node_id: 0,
        }],
        leader_commit: 4,
        read_ctx: 0,
    }
    .encode_to_vec()
}

/// One random mutation (or pure noise) of `seed`.
fn mutate(rng: &mut StdRng, seed: &[u8]) -> Vec<u8> {
    let mut b = seed.to_vec();
    match rng.gen_range(0..9) {
        0 => b = (0..rng.gen_range(0..300)).map(|_| rng.gen()).collect(), // noise
        1 => {
            // flip a few bits
            for _ in 0..rng.gen_range(1..4) {
                if !b.is_empty() {
                    let i = rng.gen_range(0..b.len());
                    b[i] ^= 1 << rng.gen_range(0..8);
                }
            }
        }
        2 => {
            let l = rng.gen_range(0..=b.len());
            b.truncate(l);
        }
        3 => {
            // overwrite a range with noise
            if !b.is_empty() {
                let s = rng.gen_range(0..b.len());
                let e = (s + rng.gen_range(1..32)).min(b.len());
                for x in &mut b[s..e] {
                    *x = rng.gen();
                }
            }
        }
        4 => {
            // zero a range (size-extended-file crash shape)
            if !b.is_empty() {
                let s = rng.gen_range(0..b.len());
                let e = (s + rng.gen_range(1..64)).min(b.len());
                for x in &mut b[s..e] {
                    *x = 0;
                }
            }
        }
        5 => {
            // duplicate a chunk
            if !b.is_empty() {
                let s = rng.gen_range(0..b.len());
                let e = (s + rng.gen_range(1..40)).min(b.len());
                let c = b[s..e].to_vec();
                let at = rng.gen_range(0..=b.len());
                b.splice(at..at, c);
            }
        }
        6 => {
            // append noise
            for _ in 0..rng.gen_range(1..40) {
                b.push(rng.gen());
            }
        }
        7 => {
            // hostile length field: 0xFFFFFFFF at a random 4-byte slot
            if b.len() >= 4 {
                let i = rng.gen_range(0..=b.len() - 4);
                b[i..i + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            }
        }
        _ => {
            // delete a chunk
            if !b.is_empty() {
                let s = rng.gen_range(0..b.len());
                let e = (s + rng.gen_range(1..40)).min(b.len());
                b.drain(s..e);
            }
        }
    }
    b
}

/// Run one input through one target. `Err` = a property violation.
fn run_one(target: &str, input: &[u8]) -> Result<(), String> {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let check = |opened_ok: bool| -> Result<(), String> {
        let _ = opened_ok;
        Ok(())
    };
    let _ = check;
    match target {
        "wal" => {
            let p = dir.join("wal.log");
            std::fs::write(&p, input).unwrap();
            let r = storage::Wal::open(&p).and_then(|mut w| w.recover());
            if r.is_err() && std::fs::read(&p).unwrap() != input {
                return Err("refused recover modified the file".into());
            }
        }
        "lsm-wal" | "lsm-manifest" | "lsm-sst" => {
            lsm_fixture(dir);
            let (prefix, ext) = match target {
                "lsm-wal" => ("wal-", ".log"),
                "lsm-manifest" => ("MANIFEST", ""),
                _ => ("sst-", ".sst"),
            };
            let victim = std::fs::read_dir(dir)
                .unwrap()
                .flatten()
                .map(|e| e.path())
                .find(|p| {
                    let n = p.file_name().unwrap().to_string_lossy().to_string();
                    n.starts_with(prefix) && n.ends_with(ext)
                })
                .unwrap();
            std::fs::write(victim, input).unwrap();
            let before = snapshot_dir(dir);
            match storage::lsm::Lsm::open(dir, lsm_opts()) {
                Err(_) => {
                    if snapshot_dir(dir) != before {
                        return Err("refused open modified the directory".into());
                    }
                }
                Ok(db) => {
                    let _ = db.dump();
                } // a read error is fine; a panic is not
            }
        }
        "raft-wal" | "raft-hardstate" | "raft-snapshot" => {
            raft_fixture(dir);
            let name = match target {
                "raft-wal" => "node.wal",
                "raft-hardstate" => "node.wal.hardstate",
                _ => "node.wal.snapshot",
            };
            std::fs::write(dir.join(name), input).unwrap();
            let before = snapshot_dir(dir);
            if storage::WalLog::open(dir.join("node.wal")).is_err() && snapshot_dir(dir) != before {
                return Err("refused open modified the directory".into());
            }
        }
        "proto" => {
            let _ = raftdb_proto::RequestVoteRequest::decode(input);
            let _ = raftdb_proto::RequestVoteReply::decode(input);
            let _ = raftdb_proto::AppendEntriesRequest::decode(input);
            let _ = raftdb_proto::AppendEntriesReply::decode(input);
            let _ = raftdb_proto::InstallSnapshotRequest::decode(input);
            let _ = raftdb_proto::InstallSnapshotReply::decode(input);
        }
        "kv" => {
            if let Some(c) = kv::Command::decode(input) {
                if c.encode() != input {
                    return Err("kv accepted a non-canonical encoding".into());
                }
            }
        }
        _ => return Err(format!("unknown target {target}")),
    }
    Ok(())
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::panic::set_hook(Box::new(|_| {})); // failures are reported below, once, with the seed

    if args.iter().any(|a| a == "--emit-corpus") {
        for t in TARGETS {
            let d = corpus_dir().join(t);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("seed-valid"), seed_bytes(t)).unwrap();
        }
        println!("seed corpus written to {}", corpus_dir().display());
        return;
    }

    let target = arg(&args, "--target").unwrap_or_else(|| "all".into());
    let targets: Vec<&str> = if target == "all" {
        TARGETS.to_vec()
    } else {
        vec![TARGETS
            .iter()
            .copied()
            .find(|t| *t == target)
            .unwrap_or_else(|| {
                eprintln!("unknown target {target}; one of {TARGETS:?} or all");
                std::process::exit(2)
            })]
    };

    if let Some(f) = arg(&args, "--replay") {
        let input = std::fs::read(&f).unwrap();
        let r = catch_unwind(AssertUnwindSafe(|| run_one(targets[0], &input)));
        println!(
            "{f}: {:?}",
            match r {
                Ok(Ok(())) => "ok".to_string(),
                Ok(Err(e)) => format!("VIOLATION: {e}"),
                Err(_) => "PANIC".into(),
            }
        );
        return;
    }

    let iters: u64 = arg(&args, "--iters")
        .map(|s| s.parse().unwrap())
        .unwrap_or(5000);
    let seed: u64 = arg(&args, "--seed")
        .map(|s| s.parse().unwrap())
        .unwrap_or(1);
    let mut failures = 0;
    for t in targets {
        let seeds: Vec<Vec<u8>> = {
            let mut v = vec![seed_bytes(t)];
            // plus any extra corpus files (including earlier crashes)
            if let Ok(rd) = std::fs::read_dir(corpus_dir().join(t)) {
                for e in rd.flatten() {
                    if let Ok(b) = std::fs::read(e.path()) {
                        if !v.contains(&b) {
                            v.push(b);
                        }
                    }
                }
            }
            v
        };
        let mut rng = StdRng::seed_from_u64(seed ^ (t.len() as u64) << 32);
        let mut bad = 0;
        for i in 0..iters {
            let base = &seeds[rng.gen_range(0..seeds.len())];
            let input = mutate(&mut rng, base);
            let r = catch_unwind(AssertUnwindSafe(|| run_one(t, &input)));
            let why = match r {
                Ok(Ok(())) => continue,
                Ok(Err(e)) => format!("VIOLATION: {e}"),
                Err(p) => format!(
                    "PANIC: {}",
                    p.downcast_ref::<String>()
                        .cloned()
                        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default()
                ),
            };
            bad += 1;
            let d = corpus_dir().join(t);
            std::fs::create_dir_all(&d).unwrap();
            let f = d.join(format!("crash-{seed}-{i}"));
            std::fs::write(&f, &input).unwrap();
            println!("[{t}] iter {i}: {why}\n    saved {}\n    replay: cargo run --release -p raftdb-fuzz --bin fuzz_loop -- --target {t} --replay {}", f.display(), f.display());
            if bad >= 3 {
                break;
            }
        }
        println!(
            "[{t}] {iters} iterations, seed {seed}: {}",
            if bad == 0 {
                "clean".to_string()
            } else {
                format!("{bad} failure(s)")
            }
        );
        failures += bad;
    }
    if failures > 0 {
        std::process::exit(1);
    }
}
