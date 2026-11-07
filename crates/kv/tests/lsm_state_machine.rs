//! The LSM-backed state machine against the in-memory baseline, under
//! Raft: differential equality, mixed-engine clusters with snapshot
//! catch-up, and crash recovery (lost engine tail, wiped engine, corrupt
//! engine, engine/log mismatch).

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::Path;

use kv::{Command, KvNode, KvStore, LsmStore, LsmStoreOptions, OpenOutcome, StateMachine};
use raft_core::{Envelope, MemLog, NodeId, RaftNode, Role};
use storage::WalLog;

fn small_opts(sync_wal: bool) -> LsmStoreOptions {
    let mut o = LsmStoreOptions::default();
    o.engine.memtable_bytes = 1024;
    o.engine.block_bytes = 128;
    o.engine.l1_file_bytes = 2048;
    o.engine.l0_compaction_trigger = 2;
    o.engine.background = false;
    o.engine.sync_wal = sync_wal;
    o
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn cmd(rng: &mut Rng, i: u64) -> Vec<u8> {
    let k = format!("key{:03}", rng.next() % 60);
    if rng.next().is_multiple_of(4) {
        Command::Delete { key: k }.encode()
    } else {
        Command::Set {
            key: k,
            value: format!("v{i}-{}", rng.next() % 1000),
        }
        .encode()
    }
}

#[test]
fn differential_lsm_vs_baseline_reads_and_byte_identical_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    let mut base = KvStore::new();
    let mut lsm = LsmStore::open(dir.path().join("lsm"), small_opts(false)).unwrap();
    let mut rng = Rng(0xDEAD_BEEF_1234_5679);
    for i in 1..=3000u64 {
        let c = cmd(&mut rng, i);
        StateMachine::apply(&mut base, i, &c).unwrap();
        lsm.apply(i, &c).unwrap();
        if i % 500 == 0 {
            for k in 0..60 {
                let key = format!("key{k:03}");
                assert_eq!(
                    base.read(&key).unwrap(),
                    lsm.read(&key).unwrap(),
                    "i={i} {key}"
                );
            }
            assert_eq!(
                base.snapshot_bytes().unwrap(),
                lsm.snapshot_bytes().unwrap(),
                "snapshot bytes differ at {i}"
            );
        }
    }
    // Range scan agrees with the baseline's sorted contents.
    let want: Vec<(String, String)> = {
        let mut v: Vec<_> = (0..60)
            .filter_map(|k| {
                let key = format!("key{k:03}");
                base.read(&key).unwrap().map(|v| (key, v))
            })
            .filter(|(k, _)| k.as_str() >= "key010" && k.as_str() < "key030")
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        lsm.scan(Some("key010"), Some("key030"), 1000).unwrap(),
        want
    );
    // Snapshots are interchangeable between engines in both directions.
    let blob = base.snapshot_bytes().unwrap();
    let mut other = LsmStore::open(dir.path().join("lsm2"), small_opts(false)).unwrap();
    other.restore_from_snapshot(&blob, 3000).unwrap();
    assert_eq!(other.snapshot_bytes().unwrap(), blob);
    assert_eq!(other.applied_index(), 3000);
    let mut back = KvStore::new();
    back.restore_from_snapshot(&lsm.snapshot_bytes().unwrap(), 3000)
        .unwrap();
    assert_eq!(back.snapshot_bytes().unwrap(), blob);
}

#[test]
fn replayed_entries_at_or_below_the_applied_index_are_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let mut lsm = LsmStore::open(dir.path(), small_opts(false)).unwrap();
    let set = |k: &str, v: &str| {
        Command::Set {
            key: k.into(),
            value: v.into(),
        }
        .encode()
    };
    lsm.apply(1, &set("x", "1")).unwrap();
    lsm.apply(2, &set("x", "2")).unwrap();
    // Re-delivery of an old entry must not regress the value.
    lsm.apply(1, &set("x", "1")).unwrap();
    assert_eq!(lsm.read("x").unwrap(), Some("2".into()));
    lsm.apply_batch(&[(2, &set("x", "stale")), (3, &set("y", "3"))])
        .unwrap();
    assert_eq!(lsm.read("x").unwrap(), Some("2".into()));
    assert_eq!(lsm.read("y").unwrap(), Some("3".into()));
}

// ---------------- multi-node consistency (mixed engines) ----------------

type Node = KvNode<MemLog, Box<dyn StateMachine>>;

struct Cluster {
    nodes: HashMap<NodeId, Node>,
    q: VecDeque<(NodeId, Envelope)>,
    isolated: Vec<NodeId>,
}

impl Cluster {
    /// Nodes 1 and 2 use the LSM engine, node 3 the in-memory baseline.
    fn new(dir: &Path, threshold: usize) -> Cluster {
        let mut nodes = HashMap::new();
        for id in 1..=3u64 {
            let peers: Vec<_> = (1..=3).filter(|p| *p != id).collect();
            let sm: Box<dyn StateMachine> = if id == 3 {
                Box::new(KvStore::new())
            } else {
                Box::new(LsmStore::open(dir.join(format!("n{id}")), small_opts(false)).unwrap())
            };
            let raft = RaftNode::with_log(id, peers, MemLog::new());
            nodes.insert(id, KvNode::with_state_machine(raft, sm, threshold).unwrap());
        }
        Cluster {
            nodes,
            q: VecDeque::new(),
            isolated: vec![],
        }
    }
    fn send(&mut self, from: NodeId, out: Vec<Envelope>) {
        for e in out {
            if !self.isolated.contains(&from) && !self.isolated.contains(&e.to) {
                self.q.push_back((from, e));
            }
        }
    }
    fn drain(&mut self) {
        let mut guard = 0;
        while let Some((from, e)) = self.q.pop_front() {
            guard += 1;
            assert!(guard < 200_000);
            let to = e.to;
            let out = self.nodes.get_mut(&to).unwrap().step(from, e.msg);
            self.send(to, out);
        }
    }
    fn tick_all(&mut self, n: usize) {
        for _ in 0..n {
            let ids: Vec<_> = self.nodes.keys().copied().collect();
            for id in ids {
                let out = self.nodes.get_mut(&id).unwrap().tick();
                self.send(id, out);
            }
            self.drain();
        }
    }
    fn leader(&self) -> NodeId {
        *self
            .nodes
            .iter()
            .filter(|(id, n)| n.role() == Role::Leader && !self.isolated.contains(id))
            .map(|(id, _)| id)
            .next()
            .expect("no leader")
    }
    fn put(&mut self, k: &str, v: &str) {
        let l = self.leader();
        self.nodes.get_mut(&l).unwrap().propose_set(k, v).unwrap();
        let out = self.nodes.get_mut(&l).unwrap().raft.force_heartbeat();
        self.send(l, out);
        self.drain();
    }
    fn linearizable_read(&mut self, key: &str) -> Option<String> {
        let l = self.leader();
        let id = self
            .nodes
            .get_mut(&l)
            .unwrap()
            .begin_linearizable_read()
            .unwrap();
        for _ in 0..4 {
            let out = self.nodes.get_mut(&l).unwrap().raft.force_heartbeat();
            self.send(l, out);
            self.drain();
            if self
                .nodes
                .get_mut(&l)
                .unwrap()
                .poll_ready_reads()
                .contains(&id)
            {
                return self.nodes[&l].read(key).unwrap();
            }
        }
        panic!("read never confirmed");
    }
}

#[test]
fn mixed_engine_cluster_converges_through_snapshots_and_serves_linearizable_reads() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Cluster::new(dir.path(), 8);
    c.tick_all(60);
    let mut model: HashMap<String, String> = HashMap::new();
    let mut rng = Rng(42);
    for i in 0..40 {
        let (k, v) = (format!("k{}", rng.next() % 15), format!("v{i}"));
        c.put(&k, &v);
        model.insert(k.clone(), v.clone());
        assert_eq!(c.linearizable_read(&k), Some(v), "read-your-write at {i}");
    }
    // Cut a follower off, write enough for the leader to compact past it,
    // then heal: it must catch up via InstallSnapshot.
    let leader = c.leader();
    let victim = *c.nodes.keys().find(|id| **id != leader).unwrap();
    c.isolated.push(victim);
    for i in 40..120 {
        let (k, v) = (format!("k{}", rng.next() % 15), format!("v{i}"));
        c.put(&k, &v);
        model.insert(k, v);
    }
    c.isolated.clear();
    c.tick_all(30);
    c.put("final", "x");
    model.insert("final".into(), "x".into());
    c.tick_all(10);
    let want = c.nodes[&leader].store.snapshot_bytes().unwrap();
    for (id, n) in &c.nodes {
        assert_eq!(
            n.store.snapshot_bytes().unwrap(),
            want,
            "node {id} diverged"
        );
        for (k, v) in &model {
            assert_eq!(n.read(k).unwrap().as_ref(), Some(v), "node {id} key {k}");
        }
    }
}

// ---------------- crash recovery with a real Raft WAL ----------------

fn open_node(dir: &Path, threshold: usize, sync_wal: bool) -> KvNode<WalLog, LsmStore> {
    let wal = WalLog::open(dir.join("node.wal")).unwrap();
    let (store, _) = LsmStore::open_or_rebuild(dir.join("lsm"), small_opts(sync_wal)).unwrap();
    let raft = RaftNode::with_log_seeded(1, vec![], wal, 7);
    KvNode::with_state_machine(raft, store, threshold).unwrap()
}

fn elect(n: &mut KvNode<WalLog, LsmStore>) {
    for _ in 0..400 {
        n.tick();
        if n.role() == Role::Leader {
            break;
        }
    }
    assert_eq!(n.role(), Role::Leader);
    for _ in 0..4 {
        n.tick(); // apply the no-op
    }
}

fn write_all(n: &mut KvNode<WalLog, LsmStore>, range: std::ops::Range<u32>) {
    for i in range {
        n.propose_set(format!("k{i:03}"), format!("val{i}"))
            .unwrap();
        n.tick();
        n.tick();
    }
}

fn assert_has_all(n: &KvNode<WalLog, LsmStore>, range: std::ops::Range<u32>) {
    for i in range {
        assert_eq!(
            n.read(&format!("k{i:03}")).unwrap(),
            Some(format!("val{i}")),
            "key {i}"
        );
    }
}

#[test]
fn restart_replays_raft_log_over_lsm_with_and_without_engine_tail_loss() {
    for lose_engine_tail in [false, true] {
        for threshold in [1_000_000usize, 25] {
            let dir = tempfile::tempdir().unwrap();
            {
                let mut n = open_node(dir.path(), threshold, false);
                elect(&mut n);
                write_all(&mut n, 0..80);
                assert_has_all(&n, 0..80);
            } // crash: drop without any shutdown path
            if lose_engine_tail {
                // Power loss: the engine's unsynced WAL tail vanishes.
                for e in fs::read_dir(dir.path().join("lsm")).unwrap().flatten() {
                    if e.file_name().to_string_lossy().starts_with("wal-") {
                        let b = fs::read(e.path()).unwrap();
                        fs::write(e.path(), &b[..b.len() / 2]).unwrap();
                    }
                }
            }
            let mut n = open_node(dir.path(), threshold, false);
            elect(&mut n);
            assert_has_all(&n, 0..80);
            // And it keeps working afterwards.
            write_all(&mut n, 80..90);
            assert_has_all(&n, 0..90);
        }
    }
}

#[test]
fn wiped_engine_is_rebuilt_from_the_raft_snapshot_and_log() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut n = open_node(dir.path(), 20, false);
        elect(&mut n);
        write_all(&mut n, 0..70);
    }
    fs::remove_dir_all(dir.path().join("lsm")).unwrap();
    let mut n = open_node(dir.path(), 20, false);
    elect(&mut n);
    assert_has_all(&n, 0..70);
}

#[test]
fn corrupt_engine_is_quarantined_not_deleted_and_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut n = open_node(dir.path(), 20, false);
        elect(&mut n);
        write_all(&mut n, 0..70);
        n.store.engine().flush().unwrap();
    }
    let sst = fs::read_dir(dir.path().join("lsm"))
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "sst"))
        .expect("an sstable exists");
    let mut b = fs::read(&sst).unwrap();
    b[5] ^= 0xff;
    fs::write(&sst, &b).unwrap();

    let (_s, outcome) =
        LsmStore::open_or_rebuild(dir.path().join("lsm"), small_opts(false)).unwrap();
    let OpenOutcome::Quarantined { moved_to, .. } = outcome else {
        panic!("corruption not detected")
    };
    assert!(moved_to.exists(), "evidence must be preserved");
    drop(_s);
    fs::remove_dir_all(dir.path().join("lsm")).unwrap();
    fs::rename(&moved_to, dir.path().join("lsm")).unwrap(); // put the corrupt one back
    let mut n = open_node(dir.path(), 20, false);
    elect(&mut n);
    assert_has_all(&n, 0..70);
}

#[test]
fn engine_from_a_different_history_than_the_raft_log_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut n = open_node(dir.path(), 1_000_000, false);
        elect(&mut n);
        write_all(&mut n, 0..30);
    }
    // Lose the Raft log but keep the (ahead) engine.
    fs::remove_file(dir.path().join("node.wal")).ok();
    for e in fs::read_dir(dir.path()).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name != "lsm" {
            fs::remove_file(e.path()).ok();
        }
    }
    let wal = WalLog::open(dir.path().join("node.wal")).unwrap();
    let (store, _) = LsmStore::open_or_rebuild(dir.path().join("lsm"), small_opts(false)).unwrap();
    let raft = RaftNode::with_log_seeded(1, vec![], wal, 7);
    let err = KvNode::with_state_machine(raft, store, 100)
        .err()
        .expect("must refuse");
    assert!(err.contains("different histories"), "{err}");
}

// ---- a lost engine WAL is now `Corruption`; the node must rebuild, not lose data ----

/// Delete every WAL of the engine, leaving its SSTables and MANIFEST. The
/// unflushed tail of the data lived only in those WALs.
fn delete_engine_wals(lsm_dir: &Path) -> usize {
    let mut n = 0;
    for e in fs::read_dir(lsm_dir).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.starts_with("wal-") && name.ends_with(".log") {
            fs::remove_file(e.path()).unwrap();
            n += 1;
        }
    }
    n
}

fn lost_engine_wal_is_quarantined_and_rebuilt_from_raft(raft_compaction_threshold: usize) {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut n = open_node(dir.path(), raft_compaction_threshold, false);
        elect(&mut n);
        write_all(&mut n, 0..50);
        n.store.engine().flush().unwrap(); // 0..50 now in SSTables
        write_all(&mut n, 50..70); // unflushed: only in the engine WAL
    }
    let lsm = dir.path().join("lsm");
    assert!(
        delete_engine_wals(&lsm) >= 1,
        "expected an engine WAL to delete"
    );

    // `Lsm::open` refuses (it must not invent an empty WAL), and the store
    // quarantines the directory instead of silently serving a stale engine.
    let (s, outcome) = LsmStore::open_or_rebuild(&lsm, small_opts(false)).unwrap();
    let OpenOutcome::Quarantined { moved_to, reason } = outcome else {
        panic!("lost WAL was not detected: the engine opened as if healthy")
    };
    assert!(reason.contains("WAL"), "{reason}");
    assert!(moved_to.exists(), "evidence must be preserved");
    drop(s);

    // Raft state (snapshot and/or log) restores every acknowledged write.
    let mut n = open_node(dir.path(), raft_compaction_threshold, false);
    elect(&mut n);
    assert_has_all(&n, 0..70);
}

#[test]
fn lost_engine_wal_is_quarantined_and_rebuilt_with_raft_log_compaction() {
    lost_engine_wal_is_quarantined_and_rebuilt_from_raft(20);
}

#[test]
fn lost_engine_wal_is_quarantined_and_rebuilt_from_the_full_raft_log() {
    lost_engine_wal_is_quarantined_and_rebuilt_from_raft(1_000_000);
}
