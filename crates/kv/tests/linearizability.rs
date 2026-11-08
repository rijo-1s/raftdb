//! Linearizability of the replicated KV store under crashes, restarts,
//! partitions and log compaction.
//!
//! A deterministic in-process cluster of three `WalLog`-backed `KvNode`s is
//! driven by several concurrent clients. Every operation's invocation and
//! response is recorded on a logical clock, and the history is checked with
//! the Wing-Gong/Lowe search in `lincheck`.
//!
//! What a client is allowed to conclude (this keeps the recorded history
//! faithful):
//!   * write acknowledged  <=> the node it proposed to is STILL the leader of
//!     the same term and has applied the entry (committed + applied);
//!   * proposal refused (not leader)  => definitely not executed, not recorded;
//!   * leader crashed / lost leadership / timed out after proposing => outcome
//!     UNKNOWN: recorded with no response, so it may take effect any time
//!     after its call or never;
//!   * reads are linearizable ReadIndex reads on the leader; one that never
//!     completes has no effect and is dropped.
//!
//! `RAFTDB_SIM_SEEDS=<n>` runs more seeds (default 30);
//! `RAFTDB_LIN_SEED=<s>` replays one seed and prints its history.

mod lincheck;

use std::collections::{BTreeMap, HashSet, VecDeque};

use kv::KvNode;
use lincheck::{check, Op, Rec, Verdict};
use raft_core::{Envelope, Message, NodeId, Role};
use storage::WalLog;

const IDS: [NodeId; 3] = [1, 2, 3];
const KEYS: [&str; 3] = ["x", "y", "z"];
const CLIENTS: usize = 4;
const STEPS: u64 = 700;
const TIMEOUT: u64 = 120;

struct Lcg(u64);
impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn pct(&mut self, p: u64) -> bool {
        self.next() % 100 < p
    }
    fn pick(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Deliberately broken behaviours, used to prove the checker catches real bugs.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    Correct,
    /// Reads served from any node's local state, with no leadership check.
    StaleReads,
    /// A write is acknowledged as soon as it is proposed, before commit.
    AckOnPropose,
}

enum Pending {
    Write { node: NodeId, term: u64, index: u64 },
    Read { node: NodeId, id: u64 },
}

struct Inflight {
    rec: usize,
    what: Pending,
    started: u64,
}

struct Sim {
    mode: Mode,
    dir: tempfile::TempDir,
    nodes: BTreeMap<NodeId, Option<KvNode<WalLog>>>,
    inbox: VecDeque<(NodeId, NodeId, Message)>,
    blocked: HashSet<NodeId>,
    starts: u64,
    seed: u64,
    rng: Lcg,
    clock: u64,
    hist: Vec<Rec>,
    /// Records of reads that turned out to have no effect (dropped).
    dead: HashSet<usize>,
    inflight: Vec<Option<Inflight>>,
    next_client: usize,
    next_val: u64,
}

impl Sim {
    fn new(seed: u64, mode: Mode) -> Self {
        let mut s = Sim {
            mode,
            dir: tempfile::tempdir().unwrap(),
            nodes: BTreeMap::new(),
            inbox: VecDeque::new(),
            blocked: HashSet::new(),
            starts: 0,
            seed,
            rng: Lcg(seed.wrapping_mul(0x9E37_79B9).wrapping_add(12345)),
            clock: 0,
            hist: Vec::new(),
            dead: HashSet::new(),
            inflight: (0..CLIENTS).map(|_| None).collect(),
            next_client: CLIENTS,
            next_val: 0,
        };
        for id in IDS {
            s.start(id);
        }
        s
    }

    fn start(&mut self, id: NodeId) {
        let d = self.dir.path().join(format!("n{id}"));
        std::fs::create_dir_all(&d).unwrap();
        let log = WalLog::open(d.join("node.wal")).expect("recovery must succeed");
        self.starts += 1;
        let peers = IDS.iter().copied().filter(|&p| p != id).collect();
        // threshold 25: compaction and snapshot install happen during the run
        let node = KvNode::with_log_seeded(
            id,
            peers,
            log,
            25,
            7919 * self.seed + 1000 * id + self.starts,
        );
        self.nodes.insert(id, Some(node));
    }

    fn live(&self) -> Vec<NodeId> {
        self.nodes
            .iter()
            .filter(|(_, n)| n.is_some())
            .map(|(&i, _)| i)
            .collect()
    }

    fn node(&mut self, id: NodeId) -> &mut KvNode<WalLog> {
        self.nodes.get_mut(&id).unwrap().as_mut().unwrap()
    }

    fn tick_clock(&mut self) -> u64 {
        self.clock += 1;
        self.clock
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
                continue;
            };
            let out = n.step(from, msg);
            self.route(to, out);
        }
    }

    fn tick_all(&mut self) {
        for id in self.live() {
            let out = self.node(id).tick();
            self.route(id, out);
        }
        self.drain();
    }

    // ---------------------------------------------------------- faults

    fn faults(&mut self) {
        if self.rng.pct(2) && self.live().len() > 1 {
            let l = self.live();
            let id = l[self.rng.pick(l.len())];
            self.crash(id);
        }
        if self.rng.pct(6) {
            let down: Vec<NodeId> = IDS
                .iter()
                .copied()
                .filter(|i| self.nodes[i].is_none())
                .collect();
            if !down.is_empty() {
                let id = down[self.rng.pick(down.len())];
                self.start(id);
            }
        }
        if self.rng.pct(3) && self.blocked.is_empty() {
            let id = IDS[self.rng.pick(3)];
            self.blocked.insert(id);
        }
        if self.rng.pct(5) {
            self.blocked.clear();
        }
    }

    fn crash(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
        // Everything in flight on that node loses its connection.
        for c in 0..self.inflight.len() {
            let on_it = matches!(&self.inflight[c], Some(Inflight { what: Pending::Write { node, .. } | Pending::Read { node, .. }, .. }) if *node == id);
            if on_it {
                self.abandon(c);
            }
        }
    }

    /// The client stops waiting: a write is now of unknown outcome (stays in
    /// the history with no response), a read is dropped.
    fn abandon(&mut self, c: usize) {
        if let Some(f) = self.inflight[c].take() {
            if let Pending::Read { .. } = f.what {
                self.dead.insert(f.rec);
            }
            // a fresh client identity takes over this slot; the old one stays
            // "pending forever" in the history
            self.next_client += 1;
        }
    }

    // --------------------------------------------------------- clients

    fn start_op(&mut self, c: usize) {
        let live = self.live();
        if live.is_empty() {
            return;
        }
        let leaders: Vec<NodeId> = live
            .iter()
            .copied()
            .filter(|i| self.nodes[i].as_ref().unwrap().role() == Role::Leader)
            .collect();
        // Clients can reach any live node, including a partitioned one.
        let target = if !leaders.is_empty() && !self.rng.pct(15) {
            leaders[self.rng.pick(leaders.len())]
        } else {
            live[self.rng.pick(live.len())]
        };
        let key = KEYS[self.rng.pick(KEYS.len())].to_string();
        let kind = self.rng.next() % 100;
        let client = self.next_client + c * 1_000_000; // unique per slot+generation
        let now = self.tick_clock();

        if kind < 45 {
            self.next_val += 1;
            let v = format!("v{}", self.next_val);
            let term = self.node(target).raft.term();
            let Some(index) = self.node(target).propose_set(key.clone(), v.clone()) else {
                return;
            };
            let out = self.node(target).raft.force_heartbeat();
            self.route(target, out);
            self.begin_write(c, client, key, Op::Set(v), now, target, term, index);
        } else if kind < 55 {
            let term = self.node(target).raft.term();
            let Some(index) = self.node(target).propose_delete(key.clone()) else {
                return;
            };
            let out = self.node(target).raft.force_heartbeat();
            self.route(target, out);
            self.begin_write(c, client, key, Op::Delete, now, target, term, index);
        } else if self.mode == Mode::StaleReads {
            // Deliberately broken: answer from local state, no leadership check.
            let got = self.node(target).read(&key).unwrap();
            let ret = self.tick_clock();
            self.hist.push(Rec {
                client,
                key,
                op: Op::Get,
                call: now,
                ret: Some(ret),
                got: Some(got),
            });
        } else {
            let Some(id) = self.node(target).begin_linearizable_read() else {
                return;
            };
            self.hist.push(Rec {
                client,
                key,
                op: Op::Get,
                call: now,
                ret: None,
                got: None,
            });
            self.inflight[c] = Some(Inflight {
                rec: self.hist.len() - 1,
                what: Pending::Read { node: target, id },
                started: self.clock,
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn begin_write(
        &mut self,
        c: usize,
        client: usize,
        key: String,
        op: Op,
        call: u64,
        node: NodeId,
        term: u64,
        index: u64,
    ) {
        let mut rec = Rec {
            client,
            key,
            op,
            call,
            ret: None,
            got: None,
        };
        if self.mode == Mode::AckOnPropose {
            // Deliberately broken: acknowledged before it is committed.
            rec.ret = Some(self.tick_clock());
            self.hist.push(rec);
            return;
        }
        self.hist.push(rec);
        self.inflight[c] = Some(Inflight {
            rec: self.hist.len() - 1,
            what: Pending::Write { node, term, index },
            started: self.clock,
        });
    }

    fn complete(&mut self) {
        // ReadIndex results are consumed once per node per step.
        let mut ready: BTreeMap<NodeId, Vec<u64>> = BTreeMap::new();
        for id in self.live() {
            let r = self.node(id).poll_ready_reads();
            if !r.is_empty() {
                ready.insert(id, r);
            }
        }
        for c in 0..self.inflight.len() {
            let Some(f) = &self.inflight[c] else { continue };
            let (rec, started) = (f.rec, f.started);
            let outcome = match &f.what {
                Pending::Write { node, term, index } => match self.nodes[node].as_ref() {
                    None => Some(false),
                    Some(n) => {
                        if n.raft.term() == *term && n.role() == Role::Leader {
                            if n.raft.last_applied_index() >= *index {
                                Some(true)
                            } else {
                                None
                            }
                        } else {
                            Some(false) // lost leadership: outcome unknown
                        }
                    }
                },
                Pending::Read { node, id } => match self.nodes[node].as_ref() {
                    None => Some(false),
                    Some(_) if ready.get(node).is_some_and(|v| v.contains(id)) => Some(true),
                    Some(_) => None,
                },
            };
            let timed_out = self.clock.saturating_sub(started) > TIMEOUT;
            match outcome {
                Some(true) => {
                    let f = self.inflight[c].take().unwrap();
                    let ret = self.tick_clock();
                    match f.what {
                        Pending::Write { .. } => self.hist[rec].ret = Some(ret),
                        Pending::Read { node, .. } => {
                            let key = self.hist[rec].key.clone();
                            let got = self.node(node).read(&key).unwrap();
                            self.hist[rec].ret = Some(ret);
                            self.hist[rec].got = Some(got);
                        }
                    }
                }
                Some(false) => self.abandon(c),
                None if timed_out => self.abandon(c),
                None => {}
            }
        }
    }

    fn run(mut self) -> Vec<Rec> {
        for _ in 0..STEPS {
            self.faults();
            for c in 0..CLIENTS {
                if self.inflight[c].is_none() && self.rng.pct(60) {
                    self.start_op(c);
                }
            }
            self.tick_all();
            self.complete();
        }
        // Heal, restart everything, let it settle, and finish what can finish.
        self.blocked.clear();
        for id in IDS {
            if self.nodes[&id].is_none() {
                self.start(id);
            }
        }
        for _ in 0..400 {
            self.tick_all();
            self.complete();
        }
        // Anything still waiting stays "outcome unknown" (writes) / dropped (reads).
        for c in 0..self.inflight.len() {
            self.abandon(c);
        }
        let dead = std::mem::take(&mut self.dead);
        self.hist
            .into_iter()
            .enumerate()
            .filter(|(i, r)| !(dead.contains(i) || (r.op == Op::Get && r.ret.is_none())))
            .map(|(_, r)| r)
            .collect()
    }
}

fn seeds() -> u64 {
    std::env::var("RAFTDB_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}

fn describe(h: &[Rec]) -> String {
    let mut v: Vec<&Rec> = h.iter().collect();
    v.sort_by_key(|r| r.call);
    v.iter()
        .map(|r| {
            format!(
                "  c{:<8} [{:>5}, {:>6}] {:?} {} -> {:?}",
                r.client,
                r.call,
                r.ret.map_or("?".into(), |x| x.to_string()),
                r.op,
                r.key,
                r.got
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn run_seed(seed: u64, mode: Mode) -> (Vec<Rec>, Verdict) {
    let h = Sim::new(seed, mode).run();
    let v = check(&h);
    (h, v)
}

// ------------------------------------------------------------- the real thing

#[test]
fn replicated_kv_is_linearizable_under_crashes_partitions_and_compaction() {
    if let Ok(s) = std::env::var("RAFTDB_LIN_SEED") {
        let seed: u64 = s.parse().unwrap();
        let (h, v) = run_seed(seed, Mode::Correct);
        println!("{}\n{v:?}", describe(&h));
        assert!(matches!(v, Verdict::Linearizable));
        return;
    }
    let (mut ops, mut unknown, mut gets) = (0, 0, 0);
    for seed in 0..seeds() {
        let (h, v) = run_seed(seed, Mode::Correct);
        ops += h.len();
        unknown += h.iter().filter(|r| r.ret.is_none()).count();
        gets += h.iter().filter(|r| r.op == Op::Get).count();
        match v {
            Verdict::Linearizable => {}
            Verdict::Violation { key, ops } => {
                let mut ops = ops;
                ops.sort_by_key(|r| r.call);
                panic!("seed {seed}: NOT LINEARIZABLE on key {key}\n{}\nreplay: RAFTDB_LIN_SEED={seed}", describe(&ops));
            }
            Verdict::Inconclusive { key } => {
                panic!("seed {seed}: checker budget exhausted on key {key}")
            }
        }
    }
    // The histories must actually exercise the hard cases.
    println!(
        "checked {} seeds: {ops} ops ({gets} reads, {unknown} of unknown outcome)",
        seeds()
    );
    assert!(
        gets > 0 && unknown > 0,
        "workload never produced reads / indeterminate writes"
    );
}

// ------------------------------------------- the checker must catch real bugs

fn detects(mode: Mode) -> usize {
    (0..seeds().clamp(60, 120))
        .filter(|&seed| matches!(run_seed(seed, mode).1, Verdict::Violation { .. }))
        .count()
}

#[test]
fn stale_local_reads_are_detected_as_non_linearizable() {
    let n = detects(Mode::StaleReads);
    println!(
        "stale reads: flagged {n} of {} seeds",
        seeds().clamp(60, 120)
    );
    assert!(
        n > 0,
        "checker failed to flag any history with stale reads served from partitioned/lagging nodes"
    );
}

#[test]
fn acking_before_commit_is_detected_as_non_linearizable() {
    let n = detects(Mode::AckOnPropose);
    println!(
        "ack-on-propose: flagged {n} of {} seeds",
        seeds().clamp(60, 120)
    );
    assert!(
        n > 0,
        "checker failed to flag acknowledged-but-uncommitted writes"
    );
}

// ------------------------------------------------- checker unit tests (known cases)

fn w(client: usize, key: &str, v: &str, call: u64, ret: Option<u64>) -> Rec {
    Rec {
        client,
        key: key.into(),
        op: Op::Set(v.into()),
        call,
        ret,
        got: None,
    }
}
fn d(client: usize, key: &str, call: u64, ret: Option<u64>) -> Rec {
    Rec {
        client,
        key: key.into(),
        op: Op::Delete,
        call,
        ret,
        got: None,
    }
}
fn r(client: usize, key: &str, got: Option<&str>, call: u64, ret: u64) -> Rec {
    Rec {
        client,
        key: key.into(),
        op: Op::Get,
        call,
        ret: Some(ret),
        got: Some(got.map(String::from)),
    }
}
fn ok(h: &[Rec]) -> bool {
    matches!(check(h), Verdict::Linearizable)
}
fn bad(h: &[Rec]) -> bool {
    matches!(check(h), Verdict::Violation { .. })
}

#[test]
fn checker_sequential_histories() {
    assert!(ok(&[
        w(1, "k", "a", 1, Some(2)),
        r(1, "k", Some("a"), 3, 4)
    ]));
    assert!(
        bad(&[w(1, "k", "a", 1, Some(2)), r(1, "k", None, 3, 4)]),
        "read after completed write must see it"
    );
    assert!(ok(&[r(1, "k", None, 1, 2)]), "empty register reads None");
    assert!(bad(&[r(1, "k", Some("ghost"), 1, 2)]), "value nobody wrote");
    assert!(ok(&[
        w(1, "k", "a", 1, Some(2)),
        d(1, "k", 3, Some(4)),
        r(1, "k", None, 5, 6)
    ]));
    assert!(bad(&[
        w(1, "k", "a", 1, Some(2)),
        d(1, "k", 3, Some(4)),
        r(1, "k", Some("a"), 5, 6)
    ]));
}

#[test]
fn checker_stale_read_after_two_writes() {
    assert!(bad(&[
        w(1, "k", "a", 1, Some(2)),
        w(1, "k", "b", 3, Some(4)),
        r(2, "k", Some("a"), 5, 6),
    ]));
}

#[test]
fn checker_concurrency_allows_either_order() {
    // write a [1,6] overlaps read [2,3]: the read may see a or nothing
    assert!(ok(&[
        w(1, "k", "a", 1, Some(6)),
        r(2, "k", Some("a"), 2, 3)
    ]));
    assert!(ok(&[w(1, "k", "a", 1, Some(6)), r(2, "k", None, 2, 3)]));
    // two concurrent writes, reads fix the order b then a
    assert!(ok(&[
        w(1, "k", "a", 1, Some(6)),
        w(2, "k", "b", 2, Some(5)),
        r(3, "k", Some("b"), 3, 4),
        r(3, "k", Some("a"), 7, 8),
    ]));
    // b, a, b is impossible with only one write of each
    assert!(bad(&[
        w(1, "k", "a", 1, Some(6)),
        w(2, "k", "b", 2, Some(5)),
        r(3, "k", Some("b"), 3, 4),
        r(3, "k", Some("a"), 7, 8),
        r(3, "k", Some("b"), 9, 10),
    ]));
}

#[test]
fn checker_unknown_outcome_writes_may_apply_late_or_never() {
    // never completed, but a later read saw it: it applied
    assert!(ok(&[w(1, "k", "a", 1, None), r(2, "k", Some("a"), 5, 6)]));
    // never completed and never seen: it did not apply (or has not yet)
    assert!(ok(&[
        w(1, "k", "a", 1, None),
        r(2, "k", None, 5, 6),
        r(2, "k", None, 7, 8)
    ]));
    // it may apply between two reads
    assert!(ok(&[
        w(1, "k", "a", 1, None),
        r(2, "k", None, 5, 6),
        r(2, "k", Some("a"), 7, 8)
    ]));
    // but once seen it cannot vanish without a delete
    assert!(bad(&[
        w(1, "k", "a", 1, None),
        r(2, "k", Some("a"), 5, 6),
        r(2, "k", None, 7, 8)
    ]));
    // it cannot apply BEFORE it was invoked
    assert!(bad(&[r(2, "k", Some("a"), 1, 2), w(1, "k", "a", 5, None)]));
}

#[test]
fn checker_keys_are_independent() {
    assert!(ok(&[
        w(1, "x", "1", 1, Some(2)),
        w(1, "y", "2", 3, Some(4)),
        r(2, "x", Some("1"), 5, 6),
        r(2, "y", Some("2"), 7, 8),
    ]));
    match check(&[
        w(1, "x", "1", 1, Some(2)),
        r(2, "x", None, 3, 4),
        w(1, "y", "2", 5, Some(6)),
    ]) {
        Verdict::Violation { key, .. } => assert_eq!(key, "x"),
        v => panic!("{v:?}"),
    }
}
