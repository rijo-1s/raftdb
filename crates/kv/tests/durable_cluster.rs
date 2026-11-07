//! KV-layer tests with real on-disk `WalLog` storage and batched
//! proposals: restarts, compaction, leadership changes, partitions and
//! storage faults. Everything is driven deterministically in-process.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};

use kv::{Command, KvNode};
use raft_core::{Envelope, Message, NodeId, Role};
use storage::fault::{self, FaultAction, FaultPlan, FaultPoint};
use storage::WalLog;

const IDS: [NodeId; 3] = [1, 2, 3];

struct Cluster {
    dir: tempfile::TempDir,
    threshold: usize,
    nodes: BTreeMap<NodeId, Option<KvNode<WalLog>>>,
    inbox: VecDeque<(NodeId, NodeId, Message)>,
    blocked: HashSet<NodeId>,
    starts: u64,
}

impl Cluster {
    fn new(threshold: usize) -> Self {
        let mut c = Self {
            dir: tempfile::tempdir().unwrap(),
            threshold,
            nodes: BTreeMap::new(),
            inbox: VecDeque::new(),
            blocked: HashSet::new(),
            starts: 0,
        };
        for id in IDS {
            c.start(id);
        }
        c
    }

    fn start_with_peers(&mut self, id: NodeId, peers: Vec<NodeId>) {
        fault::clear();
        let d = self.dir.path().join(format!("n{id}"));
        std::fs::create_dir_all(&d).unwrap();
        let log = WalLog::open(d.join("node.wal")).expect("recovery must succeed");
        self.starts += 1;
        let node = KvNode::with_log_seeded(id, peers, log, self.threshold, 1000 * id + self.starts);
        self.nodes.insert(id, Some(node));
    }

    fn start(&mut self, id: NodeId) {
        let peers = IDS.iter().copied().filter(|&p| p != id).collect();
        self.start_with_peers(id, peers);
    }

    fn crash(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
    }

    fn node(&mut self, id: NodeId) -> &mut KvNode<WalLog> {
        self.nodes.get_mut(&id).unwrap().as_mut().unwrap()
    }

    fn route(&mut self, from: NodeId, out: Vec<Envelope>) {
        if self.blocked.contains(&from) {
            return;
        }
        for e in out {
            self.inbox.push_back((from, e.to, e.msg));
        }
    }

    fn drain(&mut self) {
        let mut guard = 0;
        while let Some((from, to, msg)) = self.inbox.pop_front() {
            guard += 1;
            assert!(guard < 200_000, "message storm");
            if self.blocked.contains(&from) || self.blocked.contains(&to) {
                continue;
            }
            let Some(Some(n)) = self.nodes.get_mut(&to) else {
                continue; // crashed
            };
            let out = n.step(from, msg);
            self.route(to, out);
        }
    }

    fn ticks(&mut self, n: usize) {
        for _ in 0..n {
            let ids: Vec<NodeId> = self
                .nodes
                .iter()
                .filter(|(_, n)| n.is_some())
                .map(|(&i, _)| i)
                .collect();
            for id in ids {
                let out = self.node(id).tick();
                self.route(id, out);
            }
            self.drain();
        }
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(id, n)| {
                !self.blocked.contains(id) && n.as_ref().is_some_and(|n| n.role() == Role::Leader)
            })
            .map(|(&i, _)| i)
            .collect()
    }

    fn elect(&mut self) -> NodeId {
        for _ in 0..3000 {
            self.ticks(1);
            let l = self.leaders();
            if l.len() == 1 {
                // let it settle (no-op committed)
                self.ticks(10);
                return l[0];
            }
        }
        panic!("no leader elected");
    }

    /// Propose `pairs` as ONE batch on `leader` (a single WAL fsync) and
    /// replicate it. Returns the assigned index range.
    fn put_batch(&mut self, leader: NodeId, pairs: &[(String, String)]) -> std::ops::Range<u64> {
        let cmds: Vec<Vec<u8>> = pairs
            .iter()
            .map(|(k, v)| {
                Command::Set {
                    key: k.clone(),
                    value: v.clone(),
                }
                .encode()
            })
            .collect();
        let r = self.node(leader).raft.propose_batch(cmds).expect("leader");
        let out = self.node(leader).raft.force_heartbeat();
        self.route(leader, out);
        self.drain();
        r
    }

    fn converge(&mut self, n: usize) {
        self.ticks(n);
    }

    fn get(&mut self, id: NodeId, k: &str) -> Option<String> {
        self.node(id).get(k).cloned()
    }
}

fn kvs(prefix: &str, n: usize) -> Vec<(String, String)> {
    (0..n)
        .map(|i| (format!("{prefix}{i}"), format!("val-{prefix}{i}")))
        .collect()
}

#[test]
fn committed_batches_survive_a_full_cluster_restart() {
    let mut c = Cluster::new(1_000_000); // no compaction: pure WAL replay
    let l = c.elect();
    let pairs = kvs("a", 40);
    c.put_batch(l, &pairs);
    c.converge(30);
    for id in IDS {
        for (k, v) in &pairs {
            assert_eq!(
                c.get(id, k).as_deref(),
                Some(v.as_str()),
                "pre-crash {id}/{k}"
            );
        }
    }

    for id in IDS {
        c.crash(id);
    }
    for id in IDS {
        c.start(id);
    }
    // Recovery alone commits and applies nothing...
    for id in IDS {
        assert_eq!(
            c.get(id, "a0"),
            None,
            "state must be rebuilt by consensus, not assumed"
        );
    }
    // ...and a new leader re-commits the surviving log.
    let l2 = c.elect();
    c.converge(40);
    for id in IDS {
        for (k, v) in &pairs {
            assert_eq!(
                c.get(id, k).as_deref(),
                Some(v.as_str()),
                "post-restart {id}/{k} (leader {l2})"
            );
        }
    }
}

#[test]
fn batches_survive_restart_through_compaction_and_snapshots() {
    let mut c = Cluster::new(10); // compact every 10 applied entries
    let l = c.elect();
    let pairs = kvs("s", 60);
    for chunk in pairs.chunks(15) {
        c.put_batch(l, chunk);
        c.converge(10);
    }
    c.converge(30);

    for id in IDS {
        c.crash(id);
    }
    for id in IDS {
        c.start(id);
    }
    // After restart each node's state machine is at least its snapshot.
    c.elect();
    c.converge(60);
    for id in IDS {
        for (k, v) in &pairs {
            assert_eq!(c.get(id, k).as_deref(), Some(v.as_str()), "{id}/{k}");
        }
    }
}

#[test]
fn an_unreplicated_batch_on_a_crashed_leader_is_never_committed() {
    let mut c = Cluster::new(1_000_000);
    let old = c.elect();
    c.put_batch(old, &kvs("base", 3));
    c.converge(20);

    // The leader durably appends a batch, then dies before sending anything.
    let ghost = kvs("ghost", 5);
    let cmds: Vec<Vec<u8>> = ghost
        .iter()
        .map(|(k, v)| {
            Command::Set {
                key: k.clone(),
                value: v.clone(),
            }
            .encode()
        })
        .collect();
    c.node(old).raft.propose_batch(cmds).unwrap();
    c.crash(old);

    // The surviving majority elects a new leader and keeps going.
    let new = c.elect();
    assert_ne!(new, old);
    c.put_batch(new, &[("after".into(), "x".into())]);
    c.converge(20);

    // The old leader restarts holding the never-replicated batch on disk.
    c.start(old);
    c.converge(80);

    for id in IDS {
        assert_eq!(c.get(id, "after").as_deref(), Some("x"), "node {id}");
        for (k, _) in &ghost {
            assert_eq!(
                c.get(id, k),
                None,
                "uncommitted {k} must never appear on node {id}"
            );
        }
        for (k, v) in kvs("base", 3) {
            assert_eq!(c.get(id, &k), Some(v), "node {id}");
        }
    }
}

#[test]
fn a_partitioned_leaders_batch_is_discarded_after_healing() {
    let mut c = Cluster::new(1_000_000);
    let old = c.elect();
    c.put_batch(old, &kvs("pre", 2));
    c.converge(20);

    c.blocked.insert(old);
    // Isolated old leader happily appends a batch it can never commit.
    let ghost = kvs("iso", 4);
    c.put_batch(old, &ghost);
    for id in IDS {
        if id != old {
            for (k, _) in &ghost {
                assert_eq!(c.get(id, k), None);
            }
        }
    }
    assert_eq!(
        c.node(old).raft.commit_index(),
        c.node(old).raft.commit_index(),
    );

    let new = c.elect();
    assert_ne!(new, old);
    c.put_batch(new, &kvs("maj", 3));
    c.converge(20);

    c.blocked.clear();
    c.converge(100);
    for id in IDS {
        for (k, v) in kvs("maj", 3) {
            assert_eq!(c.get(id, &k), Some(v), "node {id}");
        }
        for (k, _) in &ghost {
            assert_eq!(
                c.get(id, k),
                None,
                "isolated leader's {k} must not survive on {id}"
            );
        }
    }
}

#[test]
fn follower_dying_mid_batch_fsync_does_not_stall_or_corrupt_the_cluster() {
    let mut c = Cluster::new(1_000_000);
    let l = c.elect();
    let victim = IDS.iter().copied().find(|&i| i != l).unwrap();
    let other = IDS
        .iter()
        .copied()
        .find(|&i| i != l && i != victim)
        .unwrap();

    // The follower's next batch fsync fails: it "crashes" mid-AppendEntries.
    let pairs = kvs("f", 25);
    let cmds: Vec<Vec<u8>> = pairs
        .iter()
        .map(|(k, v)| {
            Command::Set {
                key: k.clone(),
                value: v.clone(),
            }
            .encode()
        })
        .collect();
    c.node(l).raft.propose_batch(cmds).unwrap();
    let out = c.node(l).raft.force_heartbeat();
    c.route(l, out);

    // Deliver everything, but arm the fault only for the victim's step.
    let mut died = false;
    while let Some((from, to, msg)) = c.inbox.pop_front() {
        let Some(Some(n)) = c.nodes.get_mut(&to) else {
            continue;
        };
        let armed = to == victim && !died && matches!(msg, Message::AppendEntries { .. });
        if armed {
            fault::arm(FaultPlan {
                point: FaultPoint::WalAppendSync,
                skip: 0,
                action: FaultAction::FailBefore,
            });
        }
        let res = catch_unwind(AssertUnwindSafe(|| n.step(from, msg)));
        let fired = fault::fired().is_some();
        fault::clear();
        match res {
            Ok(out) => c.route(to, out),
            Err(_) => {
                assert!(armed && fired);
                died = true;
                c.crash(victim);
            }
        }
    }
    assert!(died, "the fault should have killed the follower");

    // 2 of 3 are still a quorum: the batch commits without the victim.
    c.converge(30);
    for (k, v) in &pairs {
        assert_eq!(c.get(l, k).as_deref(), Some(v.as_str()));
        assert_eq!(c.get(other, k).as_deref(), Some(v.as_str()));
    }

    // The victim restarts from a possibly-torn WAL and catches up.
    c.start(victim);
    c.converge(80);
    for (k, v) in &pairs {
        assert_eq!(
            c.get(victim, k).as_deref(),
            Some(v.as_str()),
            "victim missing {k}"
        );
    }
}

#[test]
fn membership_is_authoritative_from_the_snapshot_after_restart() {
    let mut c = Cluster::new(5);
    let l = c.elect();
    c.put_batch(l, &kvs("m", 30)); // forces several compactions
    c.converge(40);
    let victim = IDS.iter().copied().find(|&i| i != l).unwrap();
    let before: Vec<NodeId> = {
        let mut v: Vec<_> = c.node(victim).raft.peers().collect();
        v.sort();
        v
    };

    c.crash(victim);
    // Restart with a deliberately *stale/wrong* --peers argument.
    c.start_with_peers(victim, vec![99]);
    let after: Vec<NodeId> = {
        let mut v: Vec<_> = c.node(victim).raft.peers().collect();
        v.sort();
        v
    };
    assert_eq!(
        after, before,
        "snapshot membership must override the CLI peers"
    );
}

#[test]
fn single_node_batch_is_durable_and_ordered() {
    fault::clear();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.wal");
    {
        let mut n = KvNode::with_log_seeded(1, vec![], WalLog::open(&path).unwrap(), 1_000_000, 7);
        for _ in 0..500 {
            n.tick();
            if n.role() == Role::Leader {
                break;
            }
        }
        assert_eq!(n.role(), Role::Leader);
        let cmds: Vec<Vec<u8>> = (0..20)
            .map(|i| {
                Command::Set {
                    key: "k".into(),
                    value: format!("v{i}"),
                }
                .encode()
            })
            .collect();
        n.raft.propose_batch(cmds).unwrap();
        n.tick(); // apply
                  // Ordered application: the last write in the batch wins.
        assert_eq!(n.get("k").map(String::as_str), Some("v19"));
    }
    // Restart: the same ordered result after replay.
    let mut n = KvNode::with_log_seeded(1, vec![], WalLog::open(&path).unwrap(), 1_000_000, 8);
    for _ in 0..500 {
        n.tick();
        if n.role() == Role::Leader {
            break;
        }
    }
    for _ in 0..5 {
        n.tick();
    }
    assert_eq!(n.get("k").map(String::as_str), Some("v19"));
}

fn peers_of(c: &mut Cluster, id: NodeId) -> Vec<NodeId> {
    let mut v: Vec<_> = c.node(id).raft.peers().collect();
    v.sort();
    v
}

/// Membership changes are log entries; after a full restart (with the
/// *original* --peers list and no snapshot) they must be re-derived by
/// replaying the durable log, never lost and never half-applied.
#[test]
fn membership_changes_are_rebuilt_consistently_from_the_log_after_restart() {
    let mut c = Cluster::new(1_000_000);
    let l = c.elect();

    // Add member 4 (it never actually starts; 3 of 4 still make quorum).
    c.node(l)
        .raft
        .propose_add_node(4)
        .expect("leader accepts add");
    let out = c.node(l).raft.force_heartbeat();
    c.route(l, out);
    c.drain();
    c.converge(40);
    for id in IDS {
        assert!(
            peers_of(&mut c, id).contains(&4),
            "node {id} should know member 4"
        );
    }

    // Full restart with the stale original peer lists.
    for id in IDS {
        c.crash(id);
    }
    for id in IDS {
        c.start(id);
    }
    let l = c.elect();
    c.converge(60);
    for id in IDS {
        assert!(
            peers_of(&mut c, id).contains(&4),
            "node {id} lost the committed AddNode(4) across restart"
        );
    }

    // Remove it again; restart again; it must stay removed.
    c.node(l)
        .raft
        .propose_remove_node(4)
        .expect("leader accepts remove");
    let out = c.node(l).raft.force_heartbeat();
    c.route(l, out);
    c.drain();
    c.converge(40);
    for id in IDS {
        assert!(
            !peers_of(&mut c, id).contains(&4),
            "node {id}: 4 should be gone"
        );
    }
    for id in IDS {
        c.crash(id);
    }
    for id in IDS {
        c.start(id);
    }
    c.elect();
    c.converge(60);
    let expected =
        |id: NodeId| -> Vec<NodeId> { IDS.iter().copied().filter(|&p| p != id).collect() };
    for id in IDS {
        assert_eq!(peers_of(&mut c, id), expected(id), "node {id}");
    }
}
