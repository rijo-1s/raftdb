//! The heart of a running node: a single Tokio task that owns `KvNode`
//! exclusively. Every mutation - a timer tick, an inbound RPC from a peer,
//! an inbound client request, or a reply from an RPC we sent - flows
//! through one `tokio::select!` loop and is handled one at a time. This is
//! the "Raft event loop" design, as opposed to a `Mutex<KvNode>` touched
//! from many tasks: state transitions stay sequential and easy to reason
//! about, and there is no risk of two tasks racing to mutate Raft state.
//!
//! Outbound RPCs to peers are fire-and-forget: `dispatch_one` spawns a
//! separate task per call so a slow/unreachable peer can never block this
//! loop, and whatever reply (if any) comes back gets fed back in as a
//! `LoopEvent::PeerReply` - just another event in the same serial stream.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use raft_core::{Envelope, Message, NodeId, Role};
use tokio::sync::{mpsc, oneshot};
use tonic::transport::{Channel, Endpoint};

use kv::{AnyStore, Command, KvNode};
use raftdb_proto as proto;
use storage::WalLog;

use crate::batching::{BatchConfig, Reject, Ticket, WriteQueue, OVERLOADED_PREFIX};
use crate::convert;
use crate::diskguard::{self, DiskGuard};
use crate::metrics::Metrics;

const TICK_INTERVAL: Duration = Duration::from_millis(50);
const PEER_RPC_TIMEOUT: Duration = Duration::from_secs(2);
/// A snapshot transfer is given extra time in proportion to its size, assuming at least this much
/// throughput (8 MiB/s), so a large snapshot is not abandoned by the flat 2-second peer timeout.
const SNAPSHOT_MIN_BYTES_PER_SEC: u64 = 8 << 20;

/// How long one outbound peer RPC may take. `snapshot_bytes` is 0 for everything but snapshots.
fn peer_rpc_timeout(snapshot_bytes: usize) -> Duration {
    PEER_RPC_TIMEOUT
        + Duration::from_millis(
            (snapshot_bytes as u64).saturating_mul(1000) / SNAPSHOT_MIN_BYTES_PER_SEC,
        )
}

/// Marks a peer as having a snapshot transfer in flight; released when dropped (also on panic or
/// task cancellation), so a peer can never be stuck as "busy".
struct SnapshotInflight {
    set: std::sync::Arc<std::sync::Mutex<HashSet<NodeId>>>,
    peer: NodeId,
}
impl SnapshotInflight {
    /// `None` if a snapshot to `peer` is already in flight.
    fn acquire(
        set: &std::sync::Arc<std::sync::Mutex<HashSet<NodeId>>>,
        peer: NodeId,
    ) -> Option<Self> {
        let mut s = set.lock().unwrap_or_else(|e| e.into_inner());
        s.insert(peer).then(|| SnapshotInflight {
            set: set.clone(),
            peer,
        })
    }
}
impl Drop for SnapshotInflight {
    fn drop(&mut self) {
        self.set
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.peer);
    }
}

/// Prefix of the error a client gets while the data volume is nearly full.
/// Deliberately not retryable by the CLI: retrying cannot help until space is
/// freed, and the message says so.
pub const DISK_FULL_PREFIX: &str = "data volume nearly full";
/// At most one "peer unreachable" log line per peer per this interval.
const PEER_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(30);

/// Remembers when each peer's failures were last logged, so a down peer (a
/// heartbeat every 150 ms) produces one line per interval, not thousands.
#[derive(Default)]
struct PeerFailures {
    state: Mutex<HashMap<NodeId, (Instant, u64)>>,
}

impl PeerFailures {
    /// Records one failure. Returns `Some(suppressed)` when it should be
    /// logged now (`suppressed` = failures swallowed since the last line),
    /// `None` when it falls inside the quiet interval.
    fn note(&self, peer: NodeId, now: Instant, interval: Duration) -> Option<u64> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.get_mut(&peer) {
            Some((last, swallowed)) if now.saturating_duration_since(*last) < interval => {
                *swallowed += 1;
                None
            }
            Some((last, swallowed)) => {
                let n = std::mem::take(swallowed);
                *last = now;
                Some(n)
            }
            None => {
                state.insert(peer, (now, 0));
                Some(0)
            }
        }
    }
}

/// A gRPC failure as one line: the status plus the root cause. For a TLS
/// problem the useful part (an unknown issuer, an expired certificate, a
/// plaintext peer) is the innermost cause; the status text alone is just
/// "transport error", and the intermediate errors only repeat each other.
fn describe(status: tonic::Status) -> String {
    use std::error::Error;
    let mut root: Option<String> = None;
    let mut cause = status.source();
    while let Some(e) = cause {
        root = Some(e.to_string());
        cause = e.source();
    }
    match root {
        // tonic often already puts the cause in the message; do not repeat it.
        Some(root) if !status.message().contains(&root) => {
            format!("{}: {}; cause: {root}", status.code(), status.message())
        }
        _ => format!("{}: {}", status.code(), status.message()),
    }
}
/// Diagnostics: report a tick or handler that is this much late/slow.
const DIAG_STALL: Duration = Duration::from_millis(100);

#[derive(Debug, Clone)]
pub struct ClientError {
    pub message: String,
    /// Who this node currently believes is leader, if anyone - lets the
    /// CLI redirect instead of guessing.
    pub leader_hint: Option<NodeId>,
}

pub enum LoopEvent {
    Inbound {
        from: NodeId,
        msg: Message,
        respond: oneshot::Sender<Message>,
    },
    PeerReply {
        from: NodeId,
        msg: Message,
    },
    ClientPut {
        key: String,
        value: String,
        /// Exactly-once session; both zero = none.
        client_id: u64,
        seq: u64,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    ClientGet {
        key: String,
        respond: oneshot::Sender<Result<Option<String>, ClientError>>,
    },
    ClientDelete {
        key: String,
        client_id: u64,
        seq: u64,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    ClientAddNode {
        node_id: NodeId,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    ClientRemoveNode {
        node_id: NodeId,
        respond: oneshot::Sender<Result<(), ClientError>>,
    },
    /// Point-in-time export of the state machine, taken between applies.
    Backup {
        respond: oneshot::Sender<Result<kv::backup::Backup, String>>,
    },
}

/// Per-node behavior knobs (as opposed to identity and wiring).
pub struct NodeOptions {
    /// Joining an existing cluster rather than bootstrapping a new one.
    pub join: bool,
    /// Compact the log every N applied entries.
    pub compaction_threshold: usize,
    /// Group-commit, backpressure and replication limits.
    pub batch: BatchConfig,
    /// Where to durably record that this node was removed from the cluster
    /// (so a restart refuses to revive it). `None` disables the marker
    /// (in-process tests).
    pub removed_marker: Option<std::path::PathBuf>,
    /// Reject client writes (and pause compaction) while the data volume has
    /// less than this many bytes free. 0 turns the protection off.
    pub min_free_bytes: u64,
    /// The directory whose volume is watched; `None` disables the check
    /// (in-process tests).
    pub data_dir: Option<std::path::PathBuf>,
    /// The state-machine engine (in-memory baseline or durable LSM).
    pub store: AnyStore,
}

/// Name of the marker file written in the data directory when a node learns
/// it was removed from the cluster.
pub const REMOVED_MARKER: &str = "node.removed";

fn write_removed_marker(path: &std::path::Path) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::File::create(path)?;
    f.write_all(b"this node was removed from the cluster\n")?;
    f.sync_all()?;
    if let Some(dir) = path.parent() {
        std::fs::File::open(dir)?.sync_all()?;
    }
    Ok(())
}

pub fn spawn(
    id: NodeId,
    peers: Vec<NodeId>,
    peer_addrs: HashMap<NodeId, String>,
    wal: WalLog,
    metrics: std::sync::Arc<Metrics>,
    options: NodeOptions,
) -> mpsc::Sender<LoopEvent> {
    let NodeOptions {
        join,
        compaction_threshold,
        batch: batch_cfg,
        removed_marker,
        min_free_bytes,
        data_dir,
        store,
    } = options;
    // Bounded, not unbounded: an unbounded channel here is an unbounded
    // memory-growth risk under sustained client load, since this is the
    // only consumer (the node loop's select!). 1024 is generous headroom
    // for a burst without meaningfully affecting normal-load latency. A
    // full channel makes `send().await` block, which is the intended
    // backpressure signal (blocking rather than failing fast).
    let (tx, rx) = mpsc::channel(1024);
    let seed = real_entropy_seed();
    let raft = if join {
        raft_core::RaftNode::joining_seeded(id, peers, wal, seed)
    } else {
        raft_core::RaftNode::with_log_seeded(id, peers, wal, seed)
    };
    let mut kv = KvNode::with_state_machine(raft, store, compaction_threshold)
        .unwrap_or_else(|e| panic!("refusing to start: {e}"));
    kv.raft
        .set_replication_limits(batch_cfg.max_append_entries, batch_cfg.max_append_bytes);
    // Sample the volume before serving anything, so a node that starts on an
    // already-full disk says so and refuses writes from its first request.
    let mut disk = DiskGuard::new(min_free_bytes);
    let mut last_free_bytes = 0;
    if let Some(free) = data_dir.as_deref().and_then(diskguard::free_bytes) {
        last_free_bytes = free;
        if disk.observe(free) == Some(true) {
            raftdb_log::events::disk_space_changed(id, true, free, disk.min_free());
        }
        metrics.record_disk(free, disk.min_free(), disk.protected());
    }
    kv.compaction_paused = disk.protected();
    if let Some(dir) = data_dir.clone() {
        // A snapshot needs about its own size in free space while it is
        // written (it goes to a temporary file first); twice that is a safe
        // margin. Refusing is cheap, running out of space is fatal.
        kv.snapshot_space_check = Some(Box::new(move |needed| {
            let free = diskguard::free_bytes(&dir).unwrap_or(u64::MAX);
            let ok = free >= needed.saturating_mul(2);
            if !ok {
                raftdb_log::events::compaction_skipped(id, needed, free);
            }
            ok
        }));
    }
    let node_loop = NodeLoop {
        kv,
        peer_addrs,
        peer_clients: HashMap::new(),
        peer_failures: Arc::new(PeerFailures::default()),
        snapshot_inflight: Default::default(),
        tx: tx.clone(),
        was_leader: false,
        writes: WriteQueue::new(batch_cfg),
        pending_reads: HashMap::new(),
        reads_dirty: false,
        removed_marker,
        metrics,
        last_lsm_poll: Instant::now(),
        last_leader_msg: Instant::now(),
        disk,
        data_dir,
        last_disk_poll: Instant::now(),
        last_free_bytes,
    };
    let handle = tokio::spawn(node_loop.run(rx));
    // Fail-stop must actually stop. Storage errors panic inside the node loop
    // (the `LogStore` trait has no error channel), and a panic in a spawned
    // task only kills that task: without this watcher the process stays up
    // with its port open and a dead loop, and a supervisor that restarts on
    // process exit never restarts it.
    tokio::spawn(watch_node_loop(id, handle, |code| std::process::exit(code)));
    tx
}

/// Process exit code when the node loop dies (sysexits `EX_SOFTWARE`).
pub const EXIT_NODE_LOOP_DIED: i32 = 70;

/// Waits for the node loop to end. A panic is fatal: log it and call
/// `on_fatal` (the real binary exits the process; tests pass a recorder). A
/// normal return (the channel closed during an orderly shutdown) or a
/// cancellation (the runtime is going away) is not a failure.
async fn watch_node_loop<F: FnOnce(i32)>(
    id: NodeId,
    handle: tokio::task::JoinHandle<()>,
    on_fatal: F,
) {
    match handle.await {
        Ok(()) => {}
        Err(e) if e.is_panic() => {
            raftdb_log::events::node_loop_died(id, "panicked");
            on_fatal(EXIT_NODE_LOOP_DIED);
        }
        Err(_) => {}
    }
}

/// Real OS entropy for the election-timeout RNG - deliberately NOT a
/// function of `id` alone (see `RaftNode::with_log_seeded`'s doc for why
/// that's unsafe outside tests). `RandomState` pulls from the OS's CSPRNG
/// internally, which gets us real entropy from `std` with no extra
/// dependency; mixed with wall-clock time and this process's PID as extra
/// insurance against ever colliding.
fn real_entropy_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let os_entropy = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    let time_component = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    os_entropy ^ time_component ^ (std::process::id() as u64)
}

type WriteReply = oneshot::Sender<Result<(), ClientError>>;

/// A queued write's reply channel plus, for session writes, the
/// `(client_id, seq)` needed to decide what to tell the client once the
/// entry is applied.
struct WriteTicket {
    respond: WriteReply,
    session: Option<(u64, u64)>,
}

impl WriteTicket {
    fn plain(respond: WriteReply) -> Self {
        Self {
            respond,
            session: None,
        }
    }
}

impl Ticket for WriteTicket {
    fn is_closed(&self) -> bool {
        self.respond.is_closed()
    }
}

/// Encode a client write, wrapping it in a session when `client_id`/`seq`
/// are given. Rejects half-specified sessions BEFORE anything is proposed:
/// an undecodable committed entry is fatal to every replica.
fn write_command(op: Command, client_id: u64, seq: u64) -> Result<Vec<u8>, ClientError> {
    match (client_id, seq) {
        (0, 0) => Ok(op.encode()),
        (0, _) | (_, 0) => Err(ClientError {
            message: "client_id and seq must both be set (non-zero) or both be zero".to_string(),
            leader_hint: None,
        }),
        (c, s) => Ok(Command::session(c, s, op)
            .expect("plain Set/Delete wraps")
            .encode()),
    }
}

/// A pending linearizable read: the key it's for, and the channel to
/// answer once ReadIndex confirms it's safe.
type PendingRead = (String, oneshot::Sender<Result<Option<String>, ClientError>>);

struct NodeLoop {
    kv: KvNode<WalLog, AnyStore>,
    peer_addrs: HashMap<NodeId, String>,
    peer_clients: HashMap<NodeId, proto::raft_transport_client::RaftTransportClient<Channel>>,
    peer_failures: Arc<PeerFailures>,
    /// Peers that currently have a snapshot transfer in flight (at most one each).
    snapshot_inflight: std::sync::Arc<std::sync::Mutex<HashSet<NodeId>>>,
    tx: mpsc::Sender<LoopEvent>,
    was_leader: bool,
    /// Client writes: queued for the next batch, or proposed and waiting to
    /// be applied. See `batching.rs` for the rules.
    writes: WriteQueue<WriteTicket>,
    pending_reads: HashMap<u64, PendingRead>,
    /// A read was registered since the last heartbeat round: send one now
    /// so it is confirmed without waiting for the next heartbeat tick.
    reads_dirty: bool,
    removed_marker: Option<std::path::PathBuf>,
    metrics: std::sync::Arc<Metrics>,
    /// Rate limit for polling LSM stats (they take engine locks).
    last_lsm_poll: Instant,
    disk: DiskGuard,
    data_dir: Option<std::path::PathBuf>,
    last_disk_poll: Instant,
    last_free_bytes: u64,
    /// Diagnostics: when this node last received an AppendEntries.
    last_leader_msg: Instant,
}

impl NodeLoop {
    async fn run(mut self, mut rx: mpsc::Receiver<LoopEvent>) {
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        // After a stall, do NOT replay every missed tick back to back (the
        // default, `Burst`). The election timer counts ticks, so a burst lets
        // a node burn its whole election timeout before it reads the
        // heartbeats that arrived while it was stalled, and it then deposes a
        // healthy leader. `Delay` fires one late tick and resumes the normal
        // cadence, so queued messages are handled before more time is counted.
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Armed only while a partial batch is waiting out its delay.
            let batch_deadline = self
                .writes
                .flush_deadline()
                .map(tokio::time::Instant::from_std);
            tokio::select! {
                scheduled = interval.tick() => {
                    let id = self.kv.raft.id;
                    let late = tokio::time::Instant::now().saturating_duration_since(scheduled);
                    if late >= DIAG_STALL {
                        raftdb_log::events::diag_stall(id, "tick_late", late.as_millis() as u64);
                    }
                    let started = Instant::now();
                    // A peer we are streaming a snapshot to is busy, not gone:
                    // it cannot answer heartbeats until the install finishes.
                    for peer in self
                        .snapshot_inflight
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .iter()
                    {
                        self.kv.raft.mark_peer_active(*peer);
                    }
                    let was_candidate = self.kv.role() == Role::Candidate;
                    let out = self.kv.tick();
                    if !was_candidate && self.kv.role() == Role::Candidate {
                        raftdb_log::events::diag_election_started(
                            id,
                            self.kv.raft.term(),
                            self.last_leader_msg.elapsed().as_millis() as u64,
                        );
                    }
                    self.dispatch_all(out);
                    self.expire_timed_out_writes();
                    self.after_step();
                    self.pump_writes();
                    if started.elapsed() >= DIAG_STALL {
                        raftdb_log::events::diag_stall(id, "tick_handler", started.elapsed().as_millis() as u64);
                    }
                }
                _ = async {
                    match batch_deadline {
                        Some(d) => tokio::time::sleep_until(d).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    self.pump_writes();
                }
                event = rx.recv() => {
                    match event {
                        Some(event) => {
                            let started = Instant::now();
                            self.on_event(event);
                            self.coalesce_writes(&mut rx).await;
                            self.pump_writes();
                            self.pump_reads();
                            if started.elapsed() >= DIAG_STALL {
                                raftdb_log::events::diag_stall(
                                    self.kv.raft.id,
                                    "event_handler",
                                    started.elapsed().as_millis() as u64,
                                );
                            }
                        }
                        None => break, // sender dropped: shutting down
                    }
                }
            }
        }
    }

    /// Opportunistic group commit. Pull in whatever other events are already
    /// queued, then yield to the scheduler so other *ready* client handlers
    /// get to enqueue their writes too, repeating while new writes keep
    /// arriving. Without the yield, tokio runs this task the instant the
    /// first request wakes it and the batch is always size 1.
    ///
    /// Bounded three ways so it cannot starve ticks or peer traffic: a
    /// batch-size cap, a limit on yield rounds, and stopping at the first
    /// round that brings nothing new. A lone client pays one yield (~µs).
    async fn coalesce_writes(&mut self, rx: &mut mpsc::Receiver<LoopEvent>) {
        const MAX_YIELD_ROUNDS: usize = 8;
        let cap = self.writes.config().max_batch_entries.min(1024);
        for round in 0..=MAX_YIELD_ROUNDS {
            let before = self.writes.queued();
            let mut handled = 0;
            while handled < cap {
                match rx.try_recv() {
                    Ok(e) => {
                        self.on_event(e);
                        handled += 1;
                    }
                    Err(_) => break,
                }
            }
            let grew = self.writes.queued() > before;
            // Nothing queued, a size limit already forces a flush, or the
            // last round brought nothing new: flush now.
            if self.writes.queued() == 0
                || self.writes.size_limit_reached()
                || round == MAX_YIELD_ROUNDS
                || (round > 0 && !grew)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    }

    fn on_event(&mut self, event: LoopEvent) {
        match event {
            LoopEvent::Inbound { from, msg, respond } => {
                if matches!(msg, Message::AppendEntries { .. }) {
                    self.last_leader_msg = Instant::now();
                }
                let out = self.kv.step(from, msg);
                let mut respond = Some(respond);
                for env in out {
                    if env.to == from {
                        if let Some(r) = respond.take() {
                            let _ = r.send(env.msg);
                        }
                    } else {
                        self.dispatch_one(env);
                    }
                }
                self.after_step();
            }
            LoopEvent::PeerReply { from, msg } => {
                let out = self.kv.step(from, msg);
                self.dispatch_all(out);
                self.after_step();
            }
            LoopEvent::ClientPut {
                key,
                value,
                client_id,
                seq,
                respond,
            } => {
                self.metrics.inc_puts();
                match write_command(Command::Set { key, value }, client_id, seq) {
                    Ok(cmd) => self.submit_write(cmd, respond, client_id, seq),
                    Err(e) => {
                        let _ = respond.send(Err(e));
                    }
                }
            }
            LoopEvent::ClientDelete {
                key,
                client_id,
                seq,
                respond,
            } => {
                self.metrics.inc_deletes();
                match write_command(Command::Delete { key }, client_id, seq) {
                    Ok(cmd) => self.submit_write(cmd, respond, client_id, seq),
                    Err(e) => {
                        let _ = respond.send(Err(e));
                    }
                }
            }
            LoopEvent::ClientAddNode { node_id, respond } => {
                if self.kv.role() != Role::Leader {
                    let _ = respond.send(Err(self.not_leader_error()));
                    return;
                }
                match self.kv.raft.propose_add_node(node_id) {
                    Some(index) => {
                        self.writes.register_one(
                            index,
                            WriteTicket::plain(respond),
                            Instant::now(),
                        );
                        let out = self.kv.raft.force_heartbeat();
                        self.dispatch_all(out);
                    }
                    None => {
                        let _ = respond.send(Err(ClientError {
                            message: "cannot add node: not leader, already a member, another config change is in flight, or this leader has not yet applied its first entry after the election (retry shortly)".to_string(),
                            leader_hint: self.kv.raft.leader_id(),
                        }));
                    }
                }
                self.after_step();
            }
            LoopEvent::ClientRemoveNode { node_id, respond } => {
                if self.kv.role() != Role::Leader {
                    let _ = respond.send(Err(self.not_leader_error()));
                    return;
                }
                match self.kv.raft.propose_remove_node(node_id) {
                    Some(index) => {
                        self.writes.register_one(
                            index,
                            WriteTicket::plain(respond),
                            Instant::now(),
                        );
                        let out = self.kv.raft.force_heartbeat();
                        self.dispatch_all(out);
                    }
                    None => {
                        let _ = respond.send(Err(ClientError {
                            message: "cannot remove node: not leader, not a member, another config change is in flight, or this leader has not yet applied its first entry after the election (retry shortly)".to_string(),
                            leader_hint: self.kv.raft.leader_id(),
                        }));
                    }
                }
                self.after_step();
            }
            LoopEvent::Backup { respond } => {
                let _ = respond.send(self.export_backup());
            }
            LoopEvent::ClientGet { key, respond } => {
                self.metrics.inc_gets();
                if self.kv.role() != Role::Leader {
                    let _ = respond.send(Err(self.not_leader_error()));
                    return;
                }
                match self.kv.begin_linearizable_read() {
                    Some(read_id) => {
                        self.pending_reads.insert(read_id, (key, respond));
                        self.reads_dirty = true;
                    }
                    None => {
                        let _ = respond.send(Err(self.not_leader_error()));
                    }
                }
                self.after_step();
            }
        }
    }

    /// Runs after every state change: resolve any reads that just became
    /// safe to answer, resolve any writes that just got applied, and - if
    /// this node just stepped down from leader - fail everything still
    /// outstanding rather than let callers hang forever.
    /// A consistent backup: events are handled one at a time, and a batch of
    /// committed entries is applied to the state machine inside `kv.step`, so
    /// between events `last_applied` and the state machine agree exactly.
    /// The snapshot bytes come from the same two calls the node uses when it
    /// compacts its log (barrier, then `snapshot_bytes`), so they include the
    /// exactly-once session table.
    ///
    /// COST: the node loop does nothing else while this runs, so only the two
    /// things that MUST happen between applies happen here: the durability
    /// barrier and the state serialization (exactly what every compaction
    /// already does). Validation and the checksum run later, off the loop, in
    /// the gRPC handler. See docs/backup-restore.md for measured numbers.
    fn export_backup(&mut self) -> Result<kv::backup::Backup, String> {
        use kv::StateMachine;
        use raft_core::LogStore;
        if self.kv.raft.is_removed() {
            return Err(
                "this node was removed from the cluster; there is nothing to back up".into(),
            );
        }
        let last_applied = self.kv.raft.last_applied_index();
        if last_applied == 0 {
            return Err("nothing has been applied yet; retry once the node has a leader".into());
        }
        self.kv.store.durability_barrier()?;
        let state = self.kv.store.snapshot_bytes()?;
        if state.len() > kv::backup::MAX_STATE_BYTES {
            return Err(format!(
                "the state is {} bytes, over the {}-byte limit of a single backup message",
                state.len(),
                kv::backup::MAX_STATE_BYTES
            ));
        }
        let id = self.kv.raft.id;
        let mut peers: Vec<NodeId> = self.kv.raft.peers().collect();
        peers.push(id);
        peers.sort_unstable();
        peers.dedup();
        Ok(kv::backup::Backup {
            created_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            source_node: id,
            last_applied,
            last_applied_term: self.kv.raft.log().term_at(last_applied),
            peers,
            state,
        })
    }

    fn after_step(&mut self) {
        self.poll_disk();
        if self.kv.raft.is_removed() && self.removed_marker.is_some() {
            if let Some(path) = self.removed_marker.take() {
                // Durable before we stop serving: a restart must not revive
                // a node the cluster has already removed.
                if let Err(e) = write_removed_marker(&path) {
                    raftdb_log::events::storage_failure(
                        self.kv.raft.id,
                        "write_removed_marker",
                        &e,
                    );
                }
                raftdb_log::events::removed_from_cluster(self.kv.raft.id);
            }
        }
        for read_id in self.kv.poll_ready_reads() {
            if let Some((key, respond)) = self.pending_reads.remove(&read_id) {
                let result = self.kv.read(&key).map_err(|e| ClientError {
                    message: format!("state machine read failed: {e}"),
                    leader_hint: None,
                });
                self.metrics.inc_reads_confirmed();
                let _ = respond.send(result);
            }
        }

        // Acknowledge writes only once they are committed *and* applied, in
        // log order.
        let applied = self.kv.raft.last_applied_index();
        for ticket in self.writes.resolve_applied(applied) {
            let result = self.session_outcome(ticket.session);
            let _ = ticket.respond.send(result);
        }

        let now_leader = self.kv.role() == Role::Leader;
        if now_leader && !self.was_leader {
            raftdb_log::events::became_leader(self.kv.raft.id, self.kv.raft.term());
            self.metrics.inc_leader_changes();
        } else if self.was_leader && !now_leader {
            raftdb_log::events::stepped_down(self.kv.raft.id, self.kv.raft.term());
        }
        if self.was_leader && !now_leader {
            let hint = self.kv.raft.leader_id();
            let (never_proposed, outcome_unknown) = self.writes.drain_all();
            for WriteTicket { respond, .. } in never_proposed {
                // Definitive: this write was never put in the log.
                let _ = respond.send(Err(ClientError {
                    message: "leadership lost before this write was proposed; safe to retry"
                        .to_string(),
                    leader_hint: hint,
                }));
            }
            for WriteTicket { respond, .. } in outcome_unknown {
                let _ = respond.send(Err(ClientError {
                    message: "leadership lost before this write was confirmed; \
                              it may still commit (outcome unknown)"
                        .to_string(),
                    leader_hint: hint,
                }));
            }
            for (_, (_, respond)) in self.pending_reads.drain() {
                let _ = respond.send(Err(ClientError {
                    message: "leadership lost before this read was confirmed".to_string(),
                    leader_hint: hint,
                }));
            }
        }
        self.was_leader = now_leader;

        self.metrics.set_term(self.kv.raft.term());
        self.metrics.set_commit_index(self.kv.raft.commit_index());
        self.metrics
            .set_log_last_index(self.kv.raft.log_last_index());
        self.metrics.set_is_leader(now_leader);
        self.metrics
            .set_pending_writes(self.writes.pending() as u64);
        // --- observability hooks ---
        self.metrics
            .set_leader_known(now_leader || self.kv.raft.leader_id().is_some());
        self.metrics.set_applied_index(applied);
        self.metrics
            .set_pending_reads(self.pending_reads.len() as u64);
        if let kv::AnyStore::Lsm(s) = &self.kv.store {
            if self.last_lsm_poll.elapsed() >= Duration::from_secs(1) {
                self.last_lsm_poll = Instant::now();
                let poisoned = s.engine().is_poisoned();
                self.metrics.record_lsm(&s.stats(), poisoned);
                if poisoned {
                    // The engine refuses all further work after an
                    // unrecoverable storage error. Staying up would only
                    // serve errors; stop so the node restarts into recovery.
                    raftdb_log::events::storage_failure(
                        self.kv.raft.id,
                        "lsm_engine_poisoned",
                        &"the LSM engine refused further work after a storage error",
                    );
                    panic!("LSM engine poisoned: treating as fatal");
                }
            }
        }
    }

    /// Sample free space on the data volume (at most once a second) and
    /// switch write protection and compaction accordingly.
    fn poll_disk(&mut self) {
        if !self.disk.enabled() || self.last_disk_poll.elapsed() < Duration::from_secs(1) {
            return;
        }
        self.last_disk_poll = Instant::now();
        let Some(free) = self.data_dir.as_deref().and_then(diskguard::free_bytes) else {
            return;
        };
        self.last_free_bytes = free;
        if let Some(protected) = self.disk.observe(free) {
            raftdb_log::events::disk_space_changed(
                self.kv.raft.id,
                protected,
                free,
                self.disk.min_free(),
            );
            self.kv.compaction_paused = protected;
        }
        self.metrics
            .record_disk(free, self.disk.min_free(), self.disk.protected());
    }

    /// Queue a client write for the next batch, applying every admission
    /// rule (leadership, size, backpressure). Never blocks.
    fn submit_write(&mut self, cmd: Vec<u8>, respond: WriteReply, client_id: u64, seq: u64) {
        if self.kv.role() != Role::Leader {
            self.metrics.inc_writes_rejected();
            let _ = respond.send(Err(self.not_leader_error()));
            return;
        }
        if self.disk.protected() {
            self.metrics.inc_writes_disk_full();
            let _ = respond.send(Err(ClientError {
                message: format!(
                    "{DISK_FULL_PREFIX}: {} MiB free, below the {} MiB floor; writes are \
                     rejected until space is freed (reads and membership changes still work)",
                    self.last_free_bytes >> 20,
                    self.disk.min_free() >> 20
                ),
                leader_hint: None,
            }));
            return;
        }
        let session = (client_id != 0).then_some((client_id, seq));
        match self
            .writes
            .submit(cmd, WriteTicket { respond, session }, Instant::now())
        {
            Ok(flush_now) => {
                if flush_now {
                    self.pump_writes();
                }
            }
            Err((reason, WriteTicket { respond, .. })) => {
                let message = match reason {
                    Reject::Overloaded => {
                        self.metrics.inc_writes_overloaded();
                        format!(
                            "{OVERLOADED_PREFIX}: {} writes already in flight; back off and retry",
                            self.writes.pending()
                        )
                    }
                    Reject::TooLarge { len, max } => {
                        format!("command is {len} bytes, over the {max}-byte limit")
                    }
                };
                let _ = respond.send(Err(ClientError {
                    message,
                    leader_hint: None,
                }));
            }
        }
    }

    /// One heartbeat round for all reads registered this iteration. The
    /// ReadIndex context only counts acks of messages sent *after* the
    /// read, so without this a read would wait for the next tick.
    fn pump_reads(&mut self) {
        if self.reads_dirty {
            self.reads_dirty = false;
            if self.kv.raft.has_unconfirmed_reads() {
                let out = self.kv.raft.force_heartbeat();
                self.dispatch_all(out);
            }
        }
    }

    /// Flush every batch that is due (size limit reached or delay elapsed).
    fn pump_writes(&mut self) {
        while self.writes.should_flush(Instant::now()) {
            if !self.flush_one_batch() {
                break;
            }
        }
    }

    /// Propose one batch: a single WAL fsync, then one replication round.
    /// Returns false if nothing was proposed.
    fn flush_one_batch(&mut self) -> bool {
        let now = Instant::now();
        let dropped_before = self.writes.stats.dropped_disconnected;
        let batch = self.writes.take_batch(now);
        self.metrics.add_writes_dropped_disconnected(
            self.writes.stats.dropped_disconnected - dropped_before,
        );
        if batch.tickets.is_empty() {
            return false; // everything queued had already hung up
        }
        let bytes: usize = batch.commands.iter().map(Vec::len).sum();
        let writes = batch.tickets.len();
        match self.kv.raft.propose_batch(batch.commands) {
            Some(range) => {
                self.metrics.add_batch(writes as u64, bytes as u64);
                self.writes
                    .register_in_flight(range.start, batch.tickets, now);
                let out = self.kv.raft.force_heartbeat();
                self.dispatch_all(out);
            }
            None => {
                // Lost leadership between queueing and flushing: nothing
                // was proposed, so this is a definitive, retryable failure.
                for WriteTicket { respond, .. } in batch.tickets {
                    self.metrics.inc_writes_rejected();
                    let _ = respond.send(Err(self.not_leader_error()));
                }
            }
        }
        self.after_step();
        true
    }

    /// Tell clients of proposed-but-unapplied writes that waited too long.
    /// The outcome is *unknown*, never reported as success.
    fn expire_timed_out_writes(&mut self) {
        for WriteTicket { respond, .. } in self.writes.expire(Instant::now()) {
            self.metrics.inc_writes_timed_out();
            let _ = respond.send(Err(ClientError {
                message: "timed out waiting for the write to commit; \
                          it may still commit (outcome unknown)"
                    .to_string(),
                leader_hint: self.kv.raft.leader_id(),
            }));
        }
    }

    /// What to tell a client whose write's log entry has just been applied.
    /// Plain writes: success. Session writes: the state machine may have
    /// skipped the entry because the session had already seen that sequence.
    ///   * last applied seq == this seq: either we applied it now or an
    ///     earlier copy was applied - the original result (success).
    ///   * last applied seq  > this seq: the client moved on; this write was
    ///     NOT applied and we say so rather than acknowledge it.
    ///   * session no longer tracked (evicted): we cannot tell, so we say so.
    fn session_outcome(&self, session: Option<(u64, u64)>) -> Result<(), ClientError> {
        let Some((client, seq)) = session else {
            return Ok(());
        };
        match self.kv.session_last_seq(client) {
            Some(last) if last == seq => Ok(()),
            Some(last) if last > seq => Err(ClientError {
                message: format!(
                    "write superseded: sequence {last} from this client was already applied; \
                     this write (sequence {seq}) was NOT applied"
                ),
                leader_hint: None,
            }),
            _ => Err(ClientError {
                message: "outcome unknown: this client's session is no longer tracked".to_string(),
                leader_hint: self.kv.raft.leader_id(),
            }),
        }
    }

    fn not_leader_error(&self) -> ClientError {
        let message = if self.kv.raft.is_removed() {
            "this node has been removed from the cluster"
        } else {
            "this node is not the leader"
        };
        ClientError {
            message: message.to_string(),
            leader_hint: self.kv.raft.leader_id(),
        }
    }

    fn dispatch_all(&mut self, envelopes: Vec<Envelope>) {
        for env in envelopes {
            self.dispatch_one(env);
        }
    }

    /// Fire off one outbound RPC as a detached task. Never blocks the
    /// event loop - an unreachable peer just means no `PeerReply` ever
    /// comes back, which is exactly how Raft is supposed to tolerate it.
    fn dispatch_one(&mut self, env: Envelope) {
        let Some(addr) = self.peer_addrs.get(&env.to).cloned() else {
            return; // unknown peer id: nothing we can do
        };
        // A lagging peer is offered the snapshot on every replication round. Send it once and
        // wait for that attempt to finish: without this, several multi-megabyte transfers would
        // pile up concurrently.
        let snapshot_bytes = match &env.msg {
            Message::InstallSnapshot { data, .. } => data.len(),
            _ => 0,
        };
        let snapshot_guard = if matches!(env.msg, Message::InstallSnapshot { .. }) {
            match SnapshotInflight::acquire(&self.snapshot_inflight, env.to) {
                Some(g) => Some(g),
                None => return,
            }
        } else {
            None
        };
        let node = self.kv.raft.id;
        let client = self.peer_clients.entry(env.to).or_insert_with(|| {
            let endpoint = Endpoint::from_shared(addr).expect("invalid peer address");
            // Plaintext unless TLS was configured (and validated) at startup.
            let endpoint =
                crate::tls::apply(endpoint).expect("TLS config was validated at startup");
            let channel = endpoint.connect_lazy();
            proto::raft_transport_client::RaftTransportClient::new(channel)
        });
        let mut client = client.clone();
        let tx = self.tx.clone();
        let failures = self.peer_failures.clone();
        let to = env.to;
        let msg = env.msg;

        tokio::spawn(async move {
            let started = Instant::now();
            let outcome = match tokio::time::timeout(
                peer_rpc_timeout(snapshot_bytes),
                call_peer(&mut client, &msg),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => Err(format!(
                    "no reply within {:?}",
                    peer_rpc_timeout(snapshot_bytes)
                )),
            };
            drop(snapshot_guard); // the next snapshot attempt for this peer may start
            match outcome {
                Ok(reply_msg) => {
                    let _ = tx
                        .send(LoopEvent::PeerReply {
                            from: to,
                            msg: reply_msg,
                        })
                        .await;
                }
                // A failed snapshot transfer is always logged: if it repeats, the peer can
                // never catch up.
                Err(error) if snapshot_bytes > 0 => raftdb_log::events::snapshot_send_failed(
                    node,
                    to,
                    snapshot_bytes,
                    started.elapsed().as_millis() as u64,
                    &error,
                ),
                // Any other failure: no reply is sent and the next heartbeat tries again, which
                // is correct Raft behavior for an unreachable peer. It is logged
                // (rate-limited) so a TLS or addressing mistake is not silent.
                Err(error) => {
                    if let Some(suppressed) =
                        failures.note(to, Instant::now(), PEER_FAILURE_LOG_INTERVAL)
                    {
                        raftdb_log::events::peer_unreachable(node, to, &error, suppressed);
                    }
                }
            }
        });
    }
}

async fn call_peer(
    client: &mut proto::raft_transport_client::RaftTransportClient<Channel>,
    msg: &Message,
) -> Result<Message, String> {
    match msg {
        Message::RequestVote { .. } | Message::PreVote { .. } => {
            let req = convert::request_vote_to_proto(msg);
            let resp = client.request_vote(req).await.map_err(describe)?;
            Ok(convert::request_vote_reply_from_proto(resp.into_inner()))
        }
        Message::AppendEntries { .. } => {
            let req = convert::append_entries_to_proto(msg);
            let resp = client.append_entries(req).await.map_err(describe)?;
            Ok(convert::append_entries_reply_from_proto(resp.into_inner()))
        }
        Message::InstallSnapshot { .. } => {
            let req = convert::install_snapshot_to_proto(msg);
            let resp = client.install_snapshot(req).await.map_err(describe)?;
            Ok(convert::install_snapshot_reply_from_proto(
                resp.into_inner(),
            ))
        }
        // Replies are never dispatched outbound; only requests are.
        _ => Err("internal error: asked to send a reply as a request".to_string()),
    }
}

#[cfg(test)]
mod tests {
    //! Drives the real node loop (real `WalLog`, real event channel, real
    //! batching) in-process, without gRPC. A single-node cluster elects
    //! itself quickly and commits as soon as an entry is durable, which
    //! isolates the batching plumbing from replication.

    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Instant;

    use super::*;

    type PutResult = Result<(), ClientError>;

    fn start(dir: &std::path::Path, cfg: BatchConfig) -> (mpsc::Sender<LoopEvent>, Arc<Metrics>) {
        let wal = WalLog::open(dir.join("node.wal")).unwrap();
        let metrics = Arc::new(Metrics::default());
        let tx = spawn(
            1,
            vec![],
            HashMap::new(),
            wal,
            metrics.clone(),
            NodeOptions {
                join: false,
                compaction_threshold: 1_000_000,
                batch: cfg,
                removed_marker: None,
                min_free_bytes: 0,
                data_dir: None,
                store: AnyStore::Mem(kv::KvStore::new()),
            },
        );
        (tx, metrics)
    }

    async fn put(tx: &mpsc::Sender<LoopEvent>, k: &str, v: &str) -> PutResult {
        let (respond, rx) = oneshot::channel();
        tx.send(LoopEvent::ClientPut {
            key: k.to_string(),
            value: v.to_string(),
            client_id: 0,
            seq: 0,
            respond,
        })
        .await
        .unwrap();
        rx.await.unwrap()
    }

    async fn get(tx: &mpsc::Sender<LoopEvent>, k: &str) -> Option<String> {
        let (respond, rx) = oneshot::channel();
        tx.send(LoopEvent::ClientGet {
            key: k.to_string(),
            respond,
        })
        .await
        .unwrap();
        rx.await.unwrap().expect("read must succeed on the leader")
    }

    /// Wait until the single node has elected itself. Uses reads, which
    /// never touch the write queue, so it works under any batching config.
    async fn wait_leader(tx: &mpsc::Sender<LoopEvent>) {
        for _ in 0..400 {
            let (respond, rx) = oneshot::channel();
            tx.send(LoopEvent::ClientGet {
                key: "__ready__".to_string(),
                respond,
            })
            .await
            .unwrap();
            if rx.await.unwrap().is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("node never became leader");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writes_are_group_committed_acked_after_apply_and_durable() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, metrics) = start(dir.path(), BatchConfig::default());
        wait_leader(&tx).await;
        let before = metrics.batches_flushed_total.load(Ordering::Relaxed);

        let mut tasks = Vec::new();
        for i in 0..300 {
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                put(&tx, &format!("k{i}"), &format!("v{i}")).await
            }));
        }
        for t in tasks {
            t.await.unwrap().expect("every write must be acknowledged");
        }

        // Acknowledged => applied: visible to a linearizable read at once.
        for i in 0..300 {
            assert_eq!(get(&tx, &format!("k{i}")).await, Some(format!("v{i}")));
        }
        let batches = metrics.batches_flushed_total.load(Ordering::Relaxed) - before;
        assert!(
            batches < 300,
            "300 concurrent writes should share fsyncs, but used {batches} batches"
        );
        assert_eq!(metrics.pending_writes.load(Ordering::Relaxed), 0);

        // Durable: a fresh process-equivalent sees them after restart.
        drop(tx);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (tx2, _m) = start(dir.path(), BatchConfig::default());
        wait_leader(&tx2).await;
        for i in (0..300).step_by(37) {
            assert_eq!(get(&tx2, &format!("k{i}")).await, Some(format!("v{i}")));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn unbatched_mode_proposes_one_write_per_batch() {
        let dir = tempfile::tempdir().unwrap();
        let (tx, metrics) = start(dir.path(), BatchConfig::unbatched());
        wait_leader(&tx).await;
        let (b0, w0) = (
            metrics.batches_flushed_total.load(Ordering::Relaxed),
            metrics.batched_writes_total.load(Ordering::Relaxed),
        );
        let mut tasks = Vec::new();
        for i in 0..50 {
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                put(&tx, &format!("u{i}"), "x").await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        let b = metrics.batches_flushed_total.load(Ordering::Relaxed) - b0;
        let w = metrics.batched_writes_total.load(Ordering::Relaxed) - w0;
        assert_eq!((b, w), (50, 50));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_client_that_disconnects_before_the_flush_is_never_proposed() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BatchConfig {
            max_batch_delay: Duration::from_millis(300),
            ..BatchConfig::default()
        };
        let (tx, metrics) = start(dir.path(), cfg);
        wait_leader(&tx).await;

        // Send a write and hang up immediately.
        let (respond, rx) = oneshot::channel();
        tx.send(LoopEvent::ClientPut {
            key: "ghost".into(),
            value: "boo".into(),
            client_id: 0,
            seq: 0,
            respond,
        })
        .await
        .unwrap();
        drop(rx);
        tokio::time::sleep(Duration::from_millis(700)).await;

        assert_eq!(
            metrics
                .writes_dropped_disconnected_total
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(get(&tx, "ghost").await, None, "must not have been applied");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn batch_delay_is_honored_for_a_lone_write() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BatchConfig {
            max_batch_delay: Duration::from_millis(250),
            ..BatchConfig::default()
        };
        let (tx, _m) = start(dir.path(), cfg);
        wait_leader(&tx).await;
        let t0 = Instant::now();
        put(&tx, "slow", "1").await.unwrap();
        let waited = t0.elapsed();
        assert!(
            waited >= Duration::from_millis(200),
            "flushed after only {waited:?}; the delay trigger did not wait"
        );
        assert!(
            waited < Duration::from_secs(3),
            "delay trigger never fired: {waited:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_batch_limit_flushes_immediately_without_waiting_for_the_delay() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BatchConfig {
            max_batch_entries: 4,
            max_batch_delay: Duration::from_secs(30), // would hang if it mattered
            ..BatchConfig::default()
        };
        let (tx, _m) = start(dir.path(), cfg);
        wait_leader(&tx).await;
        let t0 = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..4 {
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                put(&tx, &format!("b{i}"), "x").await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "a full batch must flush at once, took {:?}",
            t0.elapsed()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oversized_commands_are_rejected_not_queued() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = BatchConfig {
            max_command_bytes: 1000,
            ..BatchConfig::default()
        };
        let (tx, metrics) = start(dir.path(), cfg);
        wait_leader(&tx).await;
        let err = put(&tx, "big", &"x".repeat(5000)).await.unwrap_err();
        assert!(
            err.message.contains("over the 1000-byte limit"),
            "{}",
            err.message
        );
        assert_eq!(get(&tx, "big").await, None);
        assert_eq!(metrics.pending_writes.load(Ordering::Relaxed), 0);
        // Normal writes still work afterwards.
        put(&tx, "small", "ok").await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_to_a_non_leader_fail_fast_with_a_retryable_error() {
        let dir = tempfile::tempdir().unwrap();
        // Two peers that never answer: this node can never win an election.
        let wal = WalLog::open(dir.path().join("node.wal")).unwrap();
        let metrics = Arc::new(Metrics::default());
        let tx = spawn(
            1,
            vec![2, 3],
            HashMap::new(),
            wal,
            metrics,
            NodeOptions {
                join: false,
                compaction_threshold: 1_000_000,
                batch: BatchConfig::default(),
                removed_marker: None,
                min_free_bytes: 0,
                data_dir: None,
                store: AnyStore::Mem(kv::KvStore::new()),
            },
        );
        let err = put(&tx, "k", "v").await.unwrap_err();
        assert!(err.message.contains("not the leader"));
    }
    #[tokio::test]
    async fn watcher_reports_a_panicking_loop_as_fatal_with_the_exit_code() {
        let code = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let seen = code.clone();
        let handle = tokio::spawn(async { panic!("simulated storage failure") });
        watch_node_loop(1, handle, move |c| seen.store(c, Ordering::SeqCst)).await;
        assert_eq!(code.load(Ordering::SeqCst), EXIT_NODE_LOOP_DIED);
    }

    #[tokio::test]
    async fn watcher_is_quiet_when_the_loop_returns_normally() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = called.clone();
        let handle = tokio::spawn(async {});
        watch_node_loop(1, handle, move |_| seen.store(true, Ordering::SeqCst)).await;
        assert!(!called.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn watcher_is_quiet_when_the_loop_is_cancelled() {
        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen = called.clone();
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        watch_node_loop(1, handle, move |_| seen.store(true, Ordering::SeqCst)).await;
        assert!(!called.load(Ordering::SeqCst));
    }
    #[test]
    fn peer_failures_are_logged_once_per_interval_with_a_count_of_the_rest() {
        let f = PeerFailures::default();
        let t0 = Instant::now();
        let every = Duration::from_secs(30);
        assert_eq!(
            f.note(2, t0, every),
            Some(0),
            "the first failure is logged at once"
        );
        for i in 1..=5 {
            assert_eq!(f.note(2, t0 + Duration::from_secs(i), every), None);
        }
        assert_eq!(
            f.note(3, t0 + Duration::from_secs(2), every),
            Some(0),
            "peers are independent"
        );
        assert_eq!(
            f.note(2, t0 + Duration::from_secs(31), every),
            Some(5),
            "after the interval: logged again, standing for the 5 swallowed"
        );
        assert_eq!(
            f.note(2, t0 + Duration::from_secs(32), every),
            None,
            "and quiet again"
        );
    }

    #[tokio::test]
    async fn a_nearly_full_volume_rejects_writes_but_still_serves_reads() {
        let dir = tempfile::tempdir().unwrap();
        let wal = WalLog::open(dir.path().join("node.wal")).unwrap();
        let metrics = Arc::new(Metrics::default());
        let tx = spawn(
            1,
            vec![],
            HashMap::new(),
            wal,
            metrics.clone(),
            NodeOptions {
                join: false,
                compaction_threshold: 1_000_000,
                batch: BatchConfig::default(),
                removed_marker: None,
                // A floor no real volume can meet: protected from the start.
                min_free_bytes: u64::MAX,
                data_dir: Some(dir.path().to_path_buf()),
                store: AnyStore::Mem(kv::KvStore::new()),
            },
        );
        wait_leader(&tx).await;
        let err = put(&tx, "k", "v").await.unwrap_err();
        assert!(err.message.starts_with(DISK_FULL_PREFIX), "{}", err.message);
        assert_eq!(get(&tx, "k").await, None, "reads are still answered");
        assert_eq!(metrics.writes_disk_full_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.writes_protected.load(Ordering::Relaxed), 1);
    }
}

#[cfg(test)]
mod snapshot_transfer_tests {
    use super::*;

    #[test]
    fn small_messages_keep_the_flat_timeout_and_snapshots_get_time_in_proportion_to_size() {
        assert_eq!(peer_rpc_timeout(0), PEER_RPC_TIMEOUT);
        // 8 MiB at a guaranteed 8 MiB/s is one extra second.
        assert_eq!(
            peer_rpc_timeout(8 << 20),
            PEER_RPC_TIMEOUT + Duration::from_secs(1)
        );
        // The largest supported state gets about a minute, not two seconds.
        let big = peer_rpc_timeout(kv::backup::MAX_MESSAGE_BYTES);
        assert!(
            big >= Duration::from_secs(60) && big <= Duration::from_secs(70),
            "{big:?}"
        );
        // Never overflows, even for absurd sizes.
        let _ = peer_rpc_timeout(usize::MAX);
    }

    #[test]
    fn only_one_snapshot_transfer_per_peer_is_in_flight_and_it_is_always_released() {
        let set: std::sync::Arc<std::sync::Mutex<HashSet<NodeId>>> = Default::default();
        let first = SnapshotInflight::acquire(&set, 2).expect("first transfer starts");
        assert!(
            SnapshotInflight::acquire(&set, 2).is_none(),
            "a second one to the same peer must wait"
        );
        assert!(
            SnapshotInflight::acquire(&set, 3).is_some(),
            "other peers are independent"
        );
        drop(first);
        assert!(
            SnapshotInflight::acquire(&set, 2).is_some(),
            "released when the transfer ends"
        );
        // Released even if the task holding it panics.
        let s2 = set.clone();
        let _ = std::thread::spawn(move || {
            let _g = SnapshotInflight::acquire(&s2, 7).unwrap();
            panic!("task died");
        })
        .join();
        assert!(
            SnapshotInflight::acquire(&set, 7).is_some(),
            "a panicking task must not leave the peer stuck"
        );
    }
}
