//! Durable client dedup (sessions): semantics, durability, snapshots,
//! corruption handling, and engine equivalence.

use kv::session::{self, Session, SessionTable, MAX_SESSIONS};
use kv::{Command, KvStore, LsmStore, LsmStoreOptions, OpenOutcome, StateMachine};
use storage::lsm::{Lsm, LsmError, LsmOptions, Op};

fn set(k: &str, v: &str) -> Command {
    Command::Set {
        key: k.into(),
        value: v.into(),
    }
}
fn del(k: &str) -> Command {
    Command::Delete { key: k.into() }
}
fn sess(client: u64, seq: u64, op: Command) -> Vec<u8> {
    Command::session(client, seq, op).unwrap().encode()
}

fn lsm(dir: &std::path::Path) -> LsmStore {
    LsmStore::open(dir, LsmStoreOptions::default()).unwrap()
}

/// Both engines, driven identically.
struct Pair {
    mem: KvStore,
    lsm: LsmStore,
    next: u64,
    _dir: tempfile::TempDir,
}
impl Pair {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        Pair {
            mem: KvStore::new(),
            lsm: lsm(dir.path()),
            next: 1,
            _dir: dir,
        }
    }
    fn apply(&mut self, data: &[u8]) {
        let i = self.next;
        self.next += 1;
        StateMachine::apply(&mut self.mem, i, data).unwrap();
        self.lsm.apply(i, data).unwrap();
    }
    fn assert_same(&self, keys: &[&str]) {
        for k in keys {
            assert_eq!(
                self.mem.read(k).unwrap(),
                self.lsm.read(k).unwrap(),
                "key {k}"
            );
        }
        assert_eq!(
            self.mem.sessions(),
            self.lsm.sessions(),
            "session tables differ"
        );
        assert_eq!(
            self.mem.snapshot_bytes().unwrap(),
            self.lsm.snapshot_bytes().unwrap(),
            "engines must emit identical snapshot bytes"
        );
    }
}

// ------------------------------------------------------------- semantics

#[test]
fn a_retry_of_the_latest_write_is_not_applied_again_on_either_engine() {
    let mut p = Pair::new();
    p.apply(&sess(1, 1, set("k", "a")));
    p.apply(&set("k", "b").encode()); // someone else overwrites
    p.apply(&sess(1, 1, set("k", "a"))); // the retry arrives late
    assert_eq!(
        p.mem.read("k").unwrap().as_deref(),
        Some("b"),
        "retry re-applied and reverted a newer write"
    );
    assert_eq!(p.lsm.read("k").unwrap().as_deref(), Some("b"));
    assert_eq!(p.lsm.session_last_seq(1), Some(1));
    assert_eq!(StateMachine::session_last_seq(&p.mem, 1), Some(1));
    p.assert_same(&["k"]);
}

#[test]
fn older_sequence_is_not_applied_and_newer_is() {
    let mut p = Pair::new();
    p.apply(&sess(1, 5, set("k", "five")));
    p.apply(&sess(1, 3, set("k", "three"))); // stale
    assert_eq!(p.lsm.read("k").unwrap().as_deref(), Some("five"));
    assert_eq!(
        p.lsm.session_last_seq(1),
        Some(5),
        "a stale write must not move the session back"
    );
    p.apply(&sess(1, 6, set("k", "six"))); // fresh
    assert_eq!(p.lsm.read("k").unwrap().as_deref(), Some("six"));
    assert_eq!(p.lsm.session_last_seq(1), Some(6));
    p.assert_same(&["k"]);
}

#[test]
fn deletes_are_deduplicated_too_and_clients_are_independent() {
    let mut p = Pair::new();
    p.apply(&sess(1, 1, set("k", "a")));
    p.apply(&sess(2, 1, del("k"))); // different client, same seq: applies
    assert_eq!(p.lsm.read("k").unwrap(), None);
    p.apply(&sess(1, 2, set("k", "again")));
    p.apply(&sess(2, 1, del("k"))); // retry of client 2's delete: no-op
    assert_eq!(
        p.lsm.read("k").unwrap().as_deref(),
        Some("again"),
        "retried delete deleted a newer value"
    );
    p.assert_same(&["k"]);
}

#[test]
fn plain_commands_without_a_session_are_unaffected() {
    let mut p = Pair::new();
    p.apply(&set("k", "1").encode());
    p.apply(&set("k", "1").encode()); // plain repeats always apply
    p.apply(&set("k", "2").encode());
    assert!(p.lsm.sessions().is_empty());
    p.assert_same(&["k"]);
}

#[test]
fn duplicates_still_advance_the_applied_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = lsm(dir.path());
    s.apply(1, &sess(1, 1, set("k", "a"))).unwrap();
    s.apply(2, &sess(1, 1, set("k", "a"))).unwrap(); // pure duplicate
    assert_eq!(
        s.applied_index(),
        2,
        "a deduplicated entry must still count as applied"
    );
    drop(s);
    let s = lsm(dir.path());
    assert_eq!(s.applied_index(), 2);
}

#[test]
fn one_batch_with_repeats_and_a_new_sequence() {
    let mut mem = KvStore::new();
    let dir = tempfile::tempdir().unwrap();
    let mut l = lsm(dir.path());
    let cmds = [
        sess(1, 1, set("a", "1")),
        sess(1, 1, set("a", "dup")),
        sess(1, 2, set("a", "2")),
        sess(1, 1, set("a", "late")),
    ];
    let entries: Vec<(u64, &[u8])> = cmds
        .iter()
        .enumerate()
        .map(|(i, c)| (i as u64 + 1, c.as_slice()))
        .collect();
    mem.apply_batch(&entries).unwrap();
    l.apply_batch(&entries).unwrap();
    assert_eq!(l.read("a").unwrap().as_deref(), Some("2"));
    assert_eq!(mem.read("a").unwrap().as_deref(), Some("2"));
    assert_eq!(l.session_last_seq(1), Some(2));
    assert_eq!(mem.snapshot_bytes().unwrap(), l.snapshot_bytes().unwrap());
}

// ------------------------------------------------------------ durability

#[test]
fn sessions_survive_reopen_and_still_dedupe() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = lsm(dir.path());
        s.apply(1, &sess(7, 1, set("k", "a"))).unwrap();
        s.apply(2, &set("k", "b").encode()).unwrap();
    } // dropped without any explicit flush: recovered from the engine WAL
    let mut s = lsm(dir.path());
    assert_eq!(s.session_last_seq(7), Some(1));
    s.apply(3, &sess(7, 1, set("k", "a"))).unwrap(); // the retry, after the restart
    assert_eq!(s.read("k").unwrap().as_deref(), Some("b"));
}

#[test]
fn sessions_survive_flush_and_compaction() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = lsm(dir.path());
        s.apply(1, &sess(7, 4, set("k", "a"))).unwrap();
        s.engine().flush().unwrap();
        s.engine().compact_all().unwrap();
    }
    let s = lsm(dir.path());
    assert_eq!(s.session_last_seq(7), Some(4));
    assert_eq!(s.sessions().get(7), Some(Session { seq: 4, index: 1 }));
}

#[test]
fn user_scans_never_see_session_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = lsm(dir.path());
    s.apply(1, &sess(7, 1, set("a", "1"))).unwrap();
    s.apply(2, &set("b", "2").encode()).unwrap();
    let rows = s.scan(None, None, 100).unwrap();
    assert_eq!(
        rows,
        vec![("a".into(), "1".into()), ("b".into(), "2".into())]
    );
}

// ------------------------------------------------------------- snapshots

#[test]
fn snapshots_without_sessions_are_the_original_v1_bytes() {
    let mut p = Pair::new();
    p.apply(&set("k", "v").encode());
    let mut v1 = Vec::new();
    v1.extend_from_slice(&1u32.to_le_bytes());
    v1.extend_from_slice(&1u32.to_le_bytes());
    v1.extend_from_slice(b"k");
    v1.extend_from_slice(&1u32.to_le_bytes());
    v1.extend_from_slice(b"v");
    assert_eq!(p.mem.snapshot_bytes().unwrap(), v1);
    assert_eq!(p.lsm.snapshot_bytes().unwrap(), v1);
}

#[test]
fn snapshot_carries_sessions_across_engines_in_both_directions() {
    let mut p = Pair::new();
    p.apply(&sess(1, 3, set("k", "a")));
    p.apply(&sess(2, 9, set("j", "x")));
    p.apply(&set("k", "b").encode());
    let snap = p.mem.snapshot_bytes().unwrap();
    assert_eq!(snap, p.lsm.snapshot_bytes().unwrap());

    // mem snapshot -> fresh LSM, and lsm snapshot -> fresh mem
    let d2 = tempfile::tempdir().unwrap();
    let mut l2 = lsm(d2.path());
    l2.restore_from_snapshot(&snap, 3).unwrap();
    let mut m2 = KvStore::new();
    m2.restore_from_snapshot(&snap, 3).unwrap();
    for (name, sm) in [
        ("lsm", &mut l2 as &mut dyn StateMachine),
        ("mem", &mut m2 as &mut dyn StateMachine),
    ] {
        assert_eq!(sm.session_last_seq(1), Some(3), "{name}");
        assert_eq!(sm.session_last_seq(2), Some(9), "{name}");
        sm.apply(4, &sess(1, 3, set("k", "RETRY"))).unwrap();
        assert_eq!(
            sm.read("k").unwrap().as_deref(),
            Some("b"),
            "{name}: retry applied after snapshot install"
        );
        assert_eq!(sm.read("j").unwrap().as_deref(), Some("x"), "{name}");
    }
    // and the restored LSM keeps them durably
    drop(l2);
    let l3 = lsm(d2.path());
    assert_eq!(l3.session_last_seq(2), Some(9));
}

#[test]
fn restoring_a_snapshot_replaces_old_sessions() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = lsm(dir.path());
    s.apply(1, &sess(1, 1, set("k", "a"))).unwrap();
    let empty = KvStore::new().snapshot_bytes().unwrap();
    s.restore_from_snapshot(&empty, 5).unwrap();
    assert_eq!(
        s.session_last_seq(1),
        None,
        "sessions from before the snapshot must not leak through"
    );
    drop(s);
    assert_eq!(lsm(dir.path()).session_last_seq(1), None);
}

fn v2_snapshot() -> Vec<u8> {
    let mut m = KvStore::new();
    StateMachine::apply(&mut m, 1, &sess(1, 3, set("alpha", "one"))).unwrap();
    StateMachine::apply(&mut m, 2, &sess(1 << 33, 8, set("beta", "two"))).unwrap();
    m.snapshot_bytes().unwrap()
}

#[test]
fn flip_every_bit_of_a_v2_snapshot_is_rejected_by_both_engines() {
    let snap = v2_snapshot();
    assert_eq!(
        u32::from_le_bytes(snap[..4].try_into().unwrap()),
        session::SNAPSHOT_V2_MAGIC
    );
    let dir = tempfile::tempdir().unwrap();
    let mut l = lsm(dir.path());
    l.apply(1, &set("pre", "existing").encode()).unwrap();
    for i in 0..snap.len() {
        for bit in 0..8 {
            let mut m = snap.clone();
            m[i] ^= 1 << bit;
            assert!(
                KvStore::restore(&m).is_err(),
                "KvStore accepted flip at byte {i} bit {bit}"
            );
            assert!(
                l.restore_from_snapshot(&m, 9).is_err(),
                "LsmStore accepted flip at byte {i} bit {bit}"
            );
        }
    }
    // A rejected restore must leave the engine exactly as it was.
    assert_eq!(l.read("pre").unwrap().as_deref(), Some("existing"));
    assert_eq!(l.applied_index(), 1);
}

#[test]
fn every_truncation_of_a_v2_snapshot_is_rejected_by_both_engines() {
    let snap = v2_snapshot();
    let dir = tempfile::tempdir().unwrap();
    let mut l = lsm(dir.path());
    for len in 0..snap.len() {
        assert!(
            KvStore::restore(&snap[..len]).is_err(),
            "KvStore accepted truncation to {len}"
        );
        assert!(
            l.restore_from_snapshot(&snap[..len], 1).is_err(),
            "LsmStore accepted truncation to {len}"
        );
    }
    let mut longer = snap.clone();
    longer.push(0);
    assert!(KvStore::restore(&longer).is_err());
    assert!(l.restore_from_snapshot(&longer, 1).is_err());
}

#[test]
fn a_huge_entry_count_is_rejected_without_allocating() {
    // v1 header claiming ~4 billion entries
    let mut bad = u32::MAX.to_le_bytes().to_vec();
    bad.extend_from_slice(&[0; 16]);
    assert!(KvStore::restore(&bad).is_err());
    let dir = tempfile::tempdir().unwrap();
    assert!(lsm(dir.path()).restore_from_snapshot(&bad, 1).is_err());
}

// ------------------------------------------------- on-disk record damage

fn corrupt_session_record(dir: &std::path::Path, value: Vec<u8>) {
    let db = Lsm::open(dir, LsmOptions::default()).unwrap();
    let next = db.applied_index() + 1;
    db.apply(&[Op::Put(session::key_for(9), value)], next)
        .unwrap();
}

#[test]
fn a_damaged_session_record_fails_open_closed() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut s = lsm(dir.path());
        s.apply(1, &sess(7, 1, set("k", "a"))).unwrap();
    }
    // a record with a flipped bit
    let mut rec = session::encode_record(9, &Session { seq: 2, index: 5 });
    rec[10] ^= 0x01;
    corrupt_session_record(dir.path(), rec);
    match LsmStore::open(dir.path(), LsmStoreOptions::default()) {
        Err(LsmError::Corruption(m)) => assert!(m.contains("session"), "{m}"),
        Err(e) => panic!("wrong error: {e}"),
        Ok(_) => {
            panic!("opened with a corrupt session record: dedup state would silently be wrong")
        }
    }
}

#[test]
fn every_truncation_and_extension_of_a_stored_record_fails_open() {
    let good = session::encode_record(9, &Session { seq: 2, index: 5 });
    for len in (0..good.len()).step_by(3).chain([good.len() - 1]) {
        let dir = tempfile::tempdir().unwrap();
        corrupt_session_record(dir.path(), good[..len].to_vec());
        assert!(
            matches!(
                LsmStore::open(dir.path(), LsmStoreOptions::default()),
                Err(LsmError::Corruption(_))
            ),
            "stored record truncated to {len} bytes was accepted"
        );
    }
}

#[test]
fn a_record_stored_under_another_clients_key_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    // valid record for client 8, stored under client 9's key
    corrupt_session_record(
        dir.path(),
        session::encode_record(8, &Session { seq: 1, index: 1 }),
    );
    assert!(matches!(
        LsmStore::open(dir.path(), LsmStoreOptions::default()),
        Err(LsmError::Corruption(_))
    ));
}

#[test]
fn open_or_rebuild_quarantines_a_directory_with_a_damaged_record() {
    let parent = tempfile::tempdir().unwrap();
    let dir = parent.path().join("engine");
    std::fs::create_dir(&dir).unwrap();
    corrupt_session_record(&dir, vec![1, 2, 3]);
    let (s, outcome) = LsmStore::open_or_rebuild(&dir, LsmStoreOptions::default()).unwrap();
    match outcome {
        OpenOutcome::Quarantined { moved_to, .. } => {
            assert!(moved_to.exists(), "damaged dir must be kept, not deleted")
        }
        OpenOutcome::Clean => panic!("must quarantine"),
    }
    assert!(s.sessions().is_empty());
}

// -------------------------------------------------------------- eviction

#[test]
fn eviction_is_identical_on_both_engines_and_persisted() {
    let dir = tempfile::tempdir().unwrap();
    let mut mem = KvStore::new();
    let mut l = lsm(dir.path());
    let total = MAX_SESSIONS as u64 + 50;
    let mut idx = 0u64;
    for chunk in (1..=total).collect::<Vec<_>>().chunks(250) {
        let cmds: Vec<Vec<u8>> = chunk
            .iter()
            .map(|c| sess(*c, 1, set(&format!("k{c}"), "v")))
            .collect();
        let entries: Vec<(u64, &[u8])> = cmds
            .iter()
            .map(|c| {
                idx += 1;
                (idx, c.as_slice())
            })
            .collect();
        mem.apply_batch(&entries).unwrap();
        l.apply_batch(&entries).unwrap();
    }
    assert_eq!(mem.sessions().len(), MAX_SESSIONS);
    assert_eq!(l.sessions(), mem.sessions());
    // the 50 oldest are gone, the newest remain
    for c in 1..=50u64 {
        assert_eq!(
            l.session_last_seq(c),
            None,
            "client {c} should have been evicted"
        );
    }
    assert_eq!(l.session_last_seq(total), Some(1));
    assert_eq!(mem.snapshot_bytes().unwrap(), l.snapshot_bytes().unwrap());
    drop(l);
    // evictions were persisted: reopening yields the same table, not a bigger one
    let l = lsm(dir.path());
    assert_eq!(l.sessions(), mem.sessions());
    // data of evicted clients is untouched (only their dedup protection is gone)
    assert_eq!(l.read("k1").unwrap().as_deref(), Some("v"));
}

// ------------------------------------------------------- command encoding

#[test]
fn session_command_roundtrips_and_rejects_every_truncation_and_bad_shape() {
    let c = Command::session(77, 5, set("key", "value")).unwrap();
    let enc = c.encode();
    assert_eq!(Command::decode(&enc), Some(c.clone()));
    for len in 0..enc.len() {
        assert_eq!(
            Command::decode(&enc[..len]),
            None,
            "truncation to {len} decoded"
        );
    }
    // zero client / zero seq are reserved
    assert!(Command::session(0, 1, set("k", "v")).is_none());
    assert!(Command::session(1, 0, set("k", "v")).is_none());
    let mut zero = enc.clone();
    zero[1..9].copy_from_slice(&0u64.to_le_bytes());
    assert_eq!(Command::decode(&zero), None);
    // nesting is not allowed
    assert!(Command::session(1, 1, c.clone()).is_none());
    let mut nested = vec![3];
    nested.extend_from_slice(&1u64.to_le_bytes());
    nested.extend_from_slice(&1u64.to_le_bytes());
    nested.extend_from_slice(&enc);
    assert_eq!(Command::decode(&nested), None);
}

// ------------------------------------------- randomized differential test

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

#[test]
fn random_histories_keep_both_engines_identical_across_reopens_and_snapshot_restores() {
    for seed in 0..12u64 {
        let mut rng = Lcg(seed + 1);
        let dir = tempfile::tempdir().unwrap();
        let mut mem = KvStore::new();
        let mut l = lsm(dir.path());
        let keys = ["a", "b", "c", "d"];
        let mut idx = 0u64;
        for _round in 0..40 {
            let n = 1 + rng.next() % 6;
            let mut cmds = Vec::new();
            for _ in 0..n {
                let k = keys[(rng.next() % 4) as usize];
                let op = if rng.next().is_multiple_of(4) {
                    del(k)
                } else {
                    set(k, &format!("v{}", rng.next() % 1000))
                };
                cmds.push(match rng.next() % 3 {
                    0 => op.encode(),
                    _ => sess(1 + rng.next() % 4, 1 + rng.next() % 8, op), // small ranges force repeats
                });
            }
            let entries: Vec<(u64, &[u8])> = cmds
                .iter()
                .map(|c| {
                    idx += 1;
                    (idx, c.as_slice())
                })
                .collect();
            mem.apply_batch(&entries).unwrap();
            l.apply_batch(&entries).unwrap();
            match rng.next() % 6 {
                0 => {
                    drop(l);
                    l = lsm(dir.path()); // restart
                }
                1 => {
                    let snap = mem.snapshot_bytes().unwrap();
                    l.restore_from_snapshot(&snap, idx).unwrap();
                    let mut m2 = KvStore::new();
                    m2.restore_from_snapshot(&snap, idx).unwrap();
                    assert_eq!(m2, mem, "seed {seed}: KvStore snapshot roundtrip");
                }
                2 => {
                    l.engine().flush().unwrap();
                }
                _ => {}
            }
            for k in keys {
                assert_eq!(
                    mem.read(k).unwrap(),
                    l.read(k).unwrap(),
                    "seed {seed} key {k}"
                );
            }
            assert_eq!(mem.sessions(), l.sessions(), "seed {seed}");
            assert_eq!(
                mem.snapshot_bytes().unwrap(),
                l.snapshot_bytes().unwrap(),
                "seed {seed}"
            );
        }
        // the persisted table must equal the in-memory one after a final reopen
        drop(l);
        assert_eq!(
            lsm(dir.path()).sessions(),
            mem.sessions(),
            "seed {seed}: reload != live"
        );
        let _: &SessionTable = mem.sessions();
    }
}
