use std::collections::{HashMap, HashSet};

use crate::log::{LogStore, MemLog};
use crate::types::{EntryKind, Envelope, LogEntry, LogIndex, Message, NodeId, Role, Term};

/// Election timeout is randomized in [ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
/// ticks, per the Raft paper's recommendation to avoid split votes.
const ELECTION_TIMEOUT_MIN: u64 = 10;
const ELECTION_TIMEOUT_MAX: u64 = 20;
/// A leader that has heard from no majority for this many ticks steps down.
const CHECK_QUORUM_TICKS: u64 = 100;
const HEARTBEAT_INTERVAL: u64 = 3;

/// Tiny deterministic PRNG (xorshift64) so tests are reproducible without
/// pulling in an external crate. Not for cryptographic use.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo)
    }
}

/// A committed entry, handed back to the caller so it can be applied to
/// the state machine on top of the log (for example the KV store).
#[derive(Debug, Clone, PartialEq)]
pub struct Committed {
    pub index: LogIndex,
    pub data: Vec<u8>,
}

/// `L` defaults to `MemLog` so existing code (`RaftNode::new(...)`,
/// `HashMap<NodeId, RaftNode>`, etc.) keeps compiling unchanged. Pass a
/// disk-backed `L` (e.g. `storage::WalLog`) to get a durable node instead.
pub struct RaftNode<L: LogStore = MemLog> {
    pub id: NodeId,
    /// Cluster membership, excluding this node itself. Mutable - see
    /// `propose_add_node`/`propose_remove_node` - which is why this is a
    /// `HashSet` rather than the `Vec` earlier phases used.
    peers: HashSet<NodeId>,

    // Persistent state (Raft paper Figure 2). `current_term`/`voted_for`
    // are kept here as an in-memory cache for convenience, but every
    // mutation is mirrored into `log` via `persist_hard_state()` before
    // this node acts on it - `log` (not these fields) is the source of
    // truth on restart, via `load_hard_state()` in `with_log()`.
    current_term: Term,
    voted_for: Option<NodeId>,
    log: L,

    // Volatile state, all nodes.
    commit_index: LogIndex,
    last_applied: LogIndex,
    role: Role,
    leader_id: Option<NodeId>,

    // Volatile state, candidates only.
    votes_received: HashSet<NodeId>,

    // Volatile state of a node that is *about to* campaign: the peers that
    // said they would vote for it. Pre-votes never touch term or vote.
    pre_voting: bool,
    pre_votes: HashSet<NodeId>,

    // Volatile state, leaders only: peers heard from since the last quorum
    // check (CheckQuorum), and ticks since that check.
    heard_from: HashSet<NodeId>,
    leader_check_elapsed: u64,

    // Volatile state, leaders only (reinitialized after each election).
    next_index: HashMap<NodeId, LogIndex>,
    match_index: HashMap<NodeId, LogIndex>,

    // Timers, driven by `tick()`.
    election_elapsed: u64,
    election_timeout: u64,
    heartbeat_elapsed: u64,

    /// Set when this node just installed a leader's snapshot and hasn't
    /// handed it to the caller yet; see `take_pending_snapshot()`. The state
    /// machine loads it instead of replaying every entry from index 1.
    pending_snapshot: Option<Vec<u8>>,

    // ReadIndex state (linearizable reads, Raft paper §6.4). A read is
    // "ready" once a quorum of peers have acknowledged this leader in the
    // current term *after* the read was requested, and this node's own
    // state machine has caught up to the commit index the read was
    // requested against.
    next_read_id: u64,
    pending_reads: HashMap<u64, PendingRead>,
    ready_reads: Vec<u64>,
    /// Index of the no-op this node appended when it last became leader.
    /// A read's index is never below it (§6.4: a leader must have
    /// committed an entry of its own term before it can trust its commit
    /// index to cover everything committed by earlier leaders).
    term_start_index: LogIndex,

    /// True once this node has applied a committed `RemoveNode` naming
    /// itself. A removed node is inert: it never campaigns, votes,
    /// replicates, leads or confirms reads, and drops every inbound
    /// message. It cannot rejoin without being re-added (and, in a real
    /// deployment, restarted with a clean data directory).
    removed: bool,
    /// Members this node has seen removed. Messages *from* them are dropped
    /// before any term handling, so a removed (or never-notified) node can
    /// neither depose a healthy leader with an inflated term nor take part
    /// in any quorum (Raft dissertation §4.2.3). Volatile: rebuilt as
    /// committed removals are re-applied after a restart.
    tombstones: HashSet<NodeId>,
    /// Messages produced outside `tick`/`step` (a leader's final
    /// notification to a node it just removed); drained by the next
    /// `tick`/`step`/`force_heartbeat`.
    outbox: Vec<Envelope>,

    /// True while a membership-change entry has been proposed but not yet
    /// committed. Enforces the "single-server change" safety rule from the
    /// Raft dissertation: only one configuration change in flight at a
    /// time, each fully committed before the next is proposed.
    pending_config_change: bool,

    /// True for a normally-bootstrapped node (the common case). False for
    /// a node constructed via `joining()` - one that's been told about
    /// existing peers (so it has correct log-matching context) but hasn't
    /// yet been confirmed as an actual member. An inactive node never
    /// starts an election on its own; it flips to active the moment a
    /// committed `AddNode` entry naming its own ID comes through. Without
    /// this, a node started with knowledge of peers but not yet officially
    /// added could time out waiting to hear from anyone and declare itself
    /// a rogue single-node leader - exactly the split-brain scenario Raft
    /// exists to prevent.
    active: bool,

    /// Upper bounds on one AppendEntries message (leader side). At least one
    /// entry is always sent so an oversized entry can still replicate.
    max_append_entries: usize,
    max_append_bytes: usize,

    rng: Rng,
}

/// Default replication batch limits; see `RaftNode::set_replication_limits`.
pub const DEFAULT_MAX_APPEND_ENTRIES: usize = 1024;
pub const DEFAULT_MAX_APPEND_BYTES: usize = 1024 * 1024;

struct PendingRead {
    /// commit_index at the moment the read was requested - the leader must
    /// have applied at least this far before it's safe to answer.
    commit_index: LogIndex,
    /// Peers (not counting the leader itself) that acknowledged a message
    /// *sent after* this read was requested (see `AppendEntries::read_ctx`),
    /// in the current term.
    acked: HashSet<NodeId>,
}

impl RaftNode<MemLog> {
    /// Convenience constructor for the common case: an in-memory,
    /// non-durable node (tests, simulations).
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self::with_log(id, peers, MemLog::new())
    }
}

impl<L: LogStore> RaftNode<L> {
    /// Construct a node backed by any `LogStore`, e.g. a durable
    /// `storage::WalLog`. If `log` already has persisted hard state (a
    /// restart, not a fresh node), this node resumes from it instead of
    /// starting at term 0.
    ///
    /// The election-timeout RNG is seeded from `id` alone, so tests and the
    /// simulator get reproducible timeout sequences. Do not use this
    /// constructor for a deployed node; see `with_log_seeded`.
    pub fn with_log(id: NodeId, peers: Vec<NodeId>, log: L) -> Self {
        Self::construct(id, peers, log, true, Self::deterministic_seed(id))
    }

    /// Construct a node that's been told about existing peers (so it has
    /// correct log-matching context once it starts receiving replication)
    /// but is NOT yet an active participant - it won't start an election
    /// on its own until a committed `AddNode` entry confirms its own ID as
    /// a real member. Use this for a node joining an already-running
    /// cluster; use `with_log`/`new` for bootstrapping a fresh cluster
    /// where every founding node starts symmetrically.
    ///
    /// Same seeding caveat as `with_log` applies - see `joining_seeded`.
    pub fn joining(id: NodeId, peers: Vec<NodeId>, log: L) -> Self {
        Self::construct(id, peers, log, false, Self::deterministic_seed(id))
    }

    /// Like `with_log`, but the election-timeout RNG is seeded from an
    /// explicit `seed` instead of from `id` alone.
    ///
    /// Deployed nodes should use this constructor. Seeding from `id` alone
    /// replays the same election-timeout sequence on every restart, so only
    /// incidental scheduling and network jitter would break ties between
    /// competing candidates. With less jitter (containers started at the same
    /// moment, for example) a fixed per-id pattern can reproduce repeated
    /// split votes. Pass a seed drawn from real OS entropy, as
    /// `raftdb`'s `node_loop::spawn` does.
    pub fn with_log_seeded(id: NodeId, peers: Vec<NodeId>, log: L, seed: u64) -> Self {
        Self::construct(id, peers, log, true, seed)
    }

    /// `joining`, but with an explicit seed - see `with_log_seeded`.
    pub fn joining_seeded(id: NodeId, peers: Vec<NodeId>, log: L, seed: u64) -> Self {
        Self::construct(id, peers, log, false, seed)
    }

    fn deterministic_seed(id: NodeId) -> u64 {
        id.wrapping_mul(0x9E3779B97F4A7C15)
    }

    fn construct(id: NodeId, peers: Vec<NodeId>, log: L, active: bool, seed: u64) -> Self {
        let mut rng = Rng(seed | 1);
        let election_timeout = rng.range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
        let (current_term, voted_for) = log.load_hard_state();
        // If a snapshot already covers a prefix of the log (e.g. resuming
        // after a restart that happened post-compaction), everything up to
        // that boundary is by definition already committed and applied.
        let last_included_index = log.last_included_index();
        // Membership as of a restart: if a snapshot already exists, it's
        // the authoritative record of membership (actual AddNode/
        // RemoveNode history may have been compacted away, so the `peers`
        // argument the caller passed in - typically just whatever --peers
        // the CLI was started with - could be stale). Only fall back to
        // the constructor argument for a genuinely fresh node with no
        // snapshot yet.
        let peers: HashSet<NodeId> = if last_included_index > 0 {
            log.snapshot_peers()
                .iter()
                .copied()
                .filter(|&p| p != id)
                .collect()
        } else {
            peers.into_iter().collect()
        };
        // A founding (active) node whose own snapshot's membership does not
        // list it was removed before that snapshot was taken: stay inert.
        // (A joining node is legitimately absent from older snapshots.)
        let removed_per_snapshot = active
            && last_included_index > 0
            && !log.snapshot_peers().is_empty()
            && !log.snapshot_peers().contains(&id);
        Self {
            id,
            peers,
            current_term,
            voted_for,
            log,
            commit_index: last_included_index,
            last_applied: last_included_index,
            role: Role::Follower,
            leader_id: None,
            votes_received: HashSet::new(),
            pre_voting: false,
            pre_votes: HashSet::new(),
            heard_from: HashSet::new(),
            leader_check_elapsed: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_elapsed: 0,
            election_timeout,
            heartbeat_elapsed: 0,
            pending_snapshot: None,
            next_read_id: 0,
            pending_reads: HashMap::new(),
            ready_reads: Vec::new(),
            term_start_index: 0,
            removed: removed_per_snapshot,
            tombstones: HashSet::new(),
            outbox: Vec::new(),
            pending_config_change: false,
            active: active && !removed_per_snapshot,
            max_append_entries: DEFAULT_MAX_APPEND_ENTRIES,
            max_append_bytes: DEFAULT_MAX_APPEND_BYTES,
            rng,
        }
    }

    /// Bound how many entries / payload bytes one AppendEntries carries.
    /// Larger backlogs are streamed as consecutive messages (the leader
    /// sends the next one as soon as the previous is acknowledged). Both
    /// limits are clamped to at least 1.
    pub fn set_replication_limits(&mut self, max_entries: usize, max_bytes: usize) {
        self.max_append_entries = max_entries.max(1);
        self.max_append_bytes = max_bytes.max(1);
    }

    /// Entries appended to the log but not yet committed. A server can use
    /// this for backpressure.
    pub fn uncommitted_entries(&self) -> u64 {
        self.log.last_index().saturating_sub(self.commit_index)
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn term(&self) -> Term {
        self.current_term
    }

    /// The candidate this node voted for in the current term, if any.
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Read-only access to the underlying log store (inspection, tests).
    pub fn log(&self) -> &L {
        &self.log
    }

    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }

    pub fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// How far this node's own state machine has caught up - distinct
    /// from `commit_index()`, which can be ahead of it momentarily between
    /// a commit-index advance and the next `take_committed()` call.
    pub fn last_applied_index(&self) -> LogIndex {
        self.last_applied
    }

    /// Exposed for inspection/tests (e.g. verifying the log survived a
    /// restart) - not needed by the core algorithm itself.
    pub fn log_last_index(&self) -> LogIndex {
        self.log.last_index()
    }

    /// The current compaction snapshot's bytes, if the log has ever been
    /// compacted (locally, or via a leader's InstallSnapshot). Distinct
    /// from `take_pending_snapshot()`: this is available any time, not
    /// just right after a fresh InstallSnapshot - used by a caller
    /// resuming after a restart to rebuild its state machine correctly
    /// even when there's nothing newly "pending".
    pub fn snapshot_data(&self) -> Option<&[u8]> {
        self.log.snapshot_data()
    }

    /// Drain the snapshot this node just received via `InstallSnapshot`
    /// (if any). The state machine loads it instead of replaying log entries
    /// from the start.
    pub fn take_pending_snapshot(&mut self) -> Option<Vec<u8>> {
        self.pending_snapshot.take()
    }

    /// Ask the log to discard everything already applied to the state
    /// machine, replacing it with `snapshot_data` (opaque to `raft-core`).
    /// Returns the index the snapshot now covers, or `None` if nothing has
    /// been applied yet (nothing to compact). Safe to call on any node,
    /// not just the leader - every node needs its own log kept small.
    pub fn compact_log(&mut self, snapshot_data: Vec<u8>) -> Option<LogIndex> {
        if self.last_applied == 0 {
            return None;
        }
        let up_to_index = self.last_applied;
        let up_to_term = self.log.term_at(up_to_index);
        // Full membership, including this node's own ID - the same
        // convention used on the wire for InstallSnapshot (see
        // `broadcast_append_entries`), so every consumer of
        // `snapshot_peers()` (a restart, or a follower receiving a
        // snapshot) can apply the same "filter out my own ID" rule
        // regardless of how the snapshot got there.
        let full_membership: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .chain((!self.removed).then_some(self.id))
            .collect();
        self.log
            .compact(up_to_index, up_to_term, snapshot_data, full_membership);
        Some(up_to_index)
    }

    /// Current cluster membership as of the log's snapshot boundary
    /// (including whoever took the snapshot's own ID) - exposed mainly
    /// for tests verifying membership actually survives compaction.
    pub fn snapshot_peers(&self) -> Vec<NodeId> {
        self.log.snapshot_peers().to_vec()
    }

    /// §6.4: request confirmation that this node is still leader, as a
    /// prerequisite for answering a linearizable read. Returns `None` if
    /// this node isn't the leader (the caller must retry elsewhere - Raft
    /// gives no way to know who the real leader is except by trying).
    /// Returns `Some(read_id)` otherwise; poll `take_ready_reads()` after
    /// subsequent `tick()`/`step()` calls until it contains `read_id`;
    /// only then is it safe to read the local state machine.
    pub fn request_read_index(&mut self) -> Option<u64> {
        if self.role != Role::Leader || self.removed {
            return None;
        }
        let id = self.next_read_id;
        self.next_read_id += 1;
        self.pending_reads.insert(
            id,
            PendingRead {
                // §6.4: never below our own no-op, so a freshly elected
                // leader cannot serve a read from a commit index that is
                // still behind what earlier leaders committed.
                commit_index: self.commit_index.max(self.term_start_index),
                acked: HashSet::new(),
            },
        );
        self.promote_ready_reads();
        Some(id)
    }

    /// True if a read was requested that no outgoing message has been able
    /// to confirm yet - a caller wanting low read latency should then call
    /// `force_heartbeat()` instead of waiting for the next heartbeat tick.
    pub fn has_unconfirmed_reads(&self) -> bool {
        !self.pending_reads.is_empty()
    }

    /// The `read_ctx` to stamp on a message built right now: every read
    /// issued so far (ids `0..next_read_id`) was requested before this
    /// message exists, so an ack of it may confirm all of them. `0` when no
    /// read is waiting.
    fn current_read_ctx(&self) -> u64 {
        if self.pending_reads.is_empty() {
            0
        } else {
            self.next_read_id
        }
    }

    /// Drain the set of read IDs that have become safe to answer since the
    /// last call.
    pub fn take_ready_reads(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.ready_reads)
    }

    /// Record that `from` answered, in the current term, a message that
    /// carried `read_ctx`. Only reads that were already pending when that
    /// message was built (`id < read_ctx`) are credited; a reply to an older
    /// message (`read_ctx` smaller or 0) cannot confirm a newer read. A
    /// context larger than anything we ever issued is bogus and ignored.
    fn record_leadership_ack(&mut self, from: NodeId, read_ctx: u64) {
        if read_ctx == 0 || read_ctx > self.next_read_id {
            return;
        }
        for (&id, pr) in self.pending_reads.iter_mut() {
            if id < read_ctx {
                pr.acked.insert(from);
            }
        }
        self.promote_ready_reads();
    }

    fn promote_ready_reads(&mut self) {
        let quorum = self.quorum();
        let last_applied = self.last_applied;
        let peers = &self.peers;
        let ready_ids: Vec<u64> = self
            .pending_reads
            .iter()
            .filter(|(_, pr)| {
                // Only current members count toward the quorum.
                let acks = pr.acked.iter().filter(|p| peers.contains(p)).count();
                acks + 1 >= quorum && last_applied >= pr.commit_index
            })
            .map(|(&id, _)| id)
            .collect();
        for id in ready_ids {
            self.pending_reads.remove(&id);
            self.ready_reads.push(id);
        }
    }

    /// Current cluster membership, excluding this node itself.
    pub fn peers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.peers.iter().copied()
    }

    /// True once this node has applied its own removal from the cluster
    /// (or its own snapshot shows it was removed). A removed node is inert;
    /// the embedding application should stop serving and record the fact
    /// durably so a restart cannot revive it.
    pub fn is_removed(&self) -> bool {
        self.removed
    }

    /// Whether this node is an active participant (can start elections).
    /// Always true except for a node constructed via `joining()` that
    /// hasn't yet been confirmed as a member - see the `active` field doc.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Leader-only. Proposes adding `node_id` to the cluster. Returns the
    /// log index assigned, or `None` if this node isn't the leader, the
    /// node is already a member, or another config change is still
    /// in-flight (Raft's dissertation requires serializing config changes
    /// one at a time for the single-server-change approach to stay safe), or
    /// this leader has not yet applied through its own-term no-op (see
    /// `config_changes_allowed`).
    pub fn propose_add_node(&mut self, node_id: NodeId) -> Option<LogIndex> {
        if self.role != Role::Leader
            || self.pending_config_change
            || !self.config_changes_allowed()
            || self.peers.contains(&node_id)
            || node_id == self.id
        {
            return None;
        }
        self.pending_config_change = true;
        let index = self
            .log
            .append_config_entry(self.current_term, EntryKind::AddNode(node_id));
        // A leader with no peers is its own quorum and will never receive a
        // reply that advances the commit index (same reasoning as in
        // `become_leader`). Without this, the entry waited until some later
        // proposal happened to commit it, so `add-node` on a one-node cluster
        // hung until the next client write. With peers this is a no-op: the
        // new entry is on no follower yet, so it cannot be committed here.
        // (`propose_remove_node` needs an existing peer, so it never runs on
        // a one-node cluster and does not need this.)
        self.advance_commit_index();
        Some(index)
    }

    /// A leader may start a membership change only once it has *applied*
    /// everything up to and including the no-op it appended when elected.
    ///
    /// Membership takes effect when an entry is applied, and the quorum
    /// used to commit and to win elections comes from that applied view. A
    /// new leader can hold a config entry from a previous leader that it has
    /// not applied yet (committed or not), so its view may be a change
    /// behind its log. Starting another change then stacks two single-server
    /// changes whose quorums need not intersect (2-of-{1,2,3} vs
    /// 3-of-{1,2,3,4,5}), and a committed entry can be lost. Waiting for the
    /// own-term no-op to be applied guarantees both that every earlier
    /// config entry is applied and that this leader has committed an entry
    /// in its own term (the same rule etcd/raft enforces with its
    /// `pendingConfIndex`).
    fn config_changes_allowed(&self) -> bool {
        self.last_applied >= self.term_start_index
    }

    /// Leader-only. Proposes removing `node_id` from the cluster. Same
    /// constraints as `propose_add_node`.
    pub fn propose_remove_node(&mut self, node_id: NodeId) -> Option<LogIndex> {
        if self.role != Role::Leader
            || self.pending_config_change
            || !self.config_changes_allowed()
            || !self.peers.contains(&node_id)
        {
            return None;
        }
        self.pending_config_change = true;
        Some(
            self.log
                .append_config_entry(self.current_term, EntryKind::RemoveNode(node_id)),
        )
    }

    /// Apply one committed membership-change entry to this node's own view
    /// of the cluster. Runs on every node (leader and followers alike) as
    /// the entry commits - everyone needs an accurate membership view in
    /// case they become leader later. Never surfaced to the application
    /// (see `take_committed`).
    fn apply_config_change(&mut self, kind: EntryKind) {
        match kind {
            EntryKind::Data | EntryKind::NoOp => {}
            EntryKind::AddNode(id) => {
                self.tombstones.remove(&id);
                if id == self.id {
                    // This is confirmation that *we* are now an official
                    // member - safe to start participating in elections.
                    // (A removed node stays removed: re-adding an ID
                    // requires a fresh process with a clean data dir.)
                    if !self.removed {
                        self.active = true;
                    }
                } else {
                    self.peers.insert(id);
                    if self.role == Role::Leader {
                        // Start replicating to the new peer immediately
                        // rather than waiting for the next election.
                        self.next_index.insert(id, self.log.last_index() + 1);
                        self.match_index.insert(id, 0);
                    }
                }
            }
            EntryKind::RemoveNode(id) if id == self.id => {
                // We are the node being removed: go inert. Never again
                // campaign, vote, lead or confirm reads.
                self.removed = true;
                self.active = false;
                self.role = Role::Follower;
                self.leader_id = None;
                self.votes_received.clear();
                self.next_index.clear();
                self.match_index.clear();
                self.pending_reads.clear();
                self.ready_reads.clear();
            }
            EntryKind::RemoveNode(id) => {
                // Leader: the removed node would otherwise never learn the
                // entry committed (we stop replicating to it from now on).
                // Send one last best-effort append carrying the commit index
                // so it can apply its own removal. Safety never depends on
                // this arriving: everyone else drops its messages anyway.
                if self.role == Role::Leader && self.peers.contains(&id) {
                    let env = self.build_replication_message(id);
                    if matches!(env.msg, Message::AppendEntries { .. }) {
                        self.outbox.push(env);
                    }
                }
                self.peers.remove(&id);
                self.next_index.remove(&id);
                self.match_index.remove(&id);
                self.votes_received.remove(&id);
                self.tombstones.insert(id);
            }
        }
        self.pending_config_change = false;
    }

    /// Debug/diagnostic only: this leader's current next_index for `peer`.
    pub fn debug_next_index(&self, peer: NodeId) -> Option<LogIndex> {
        self.next_index.get(&peer).copied()
    }

    /// Debug/diagnostic only: this leader's current match_index for `peer`.
    pub fn debug_match_index(&self, peer: NodeId) -> Option<LogIndex> {
        self.match_index.get(&peer).copied()
    }

    fn quorum(&self) -> usize {
        self.peers.len().div_ceil(2) + 1
    }

    fn reset_election_timer(&mut self) {
        // Anything that restarts the timer (hearing from a leader, granting a
        // vote, starting a real election) also ends a pre-vote round.
        self.pre_voting = false;
        self.pre_votes.clear();
        self.election_elapsed = 0;
        self.election_timeout = self.rng.range(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX);
    }

    /// Write `current_term`/`voted_for` through to the log store. Must be
    /// called (and, for a durable `L`, must have actually hit disk) before
    /// this node relies on the term/vote it just adopted - e.g. before
    /// replying to a RequestVote, or before a candidate counts its own vote.
    fn persist_hard_state(&mut self) {
        self.log.save_hard_state(self.current_term, self.voted_for);
    }

    fn become_follower(&mut self, term: Term, leader_id: Option<NodeId>) {
        self.role = Role::Follower;
        self.current_term = term;
        self.voted_for = None;
        self.leader_id = leader_id;
        self.votes_received.clear();
        // Any reads this node was confirming as leader are no longer
        // trustworthy - a new leader may already have diverged. Silently
        // dropping them (never marking ready) is correct: the caller must
        // retry, which will land on the actual current leader.
        self.pending_reads.clear();
        self.persist_hard_state();
        self.reset_election_timer();
    }

    /// Advance time by one tick. Returns messages to send as a side effect
    /// (election start, or leader heartbeats).
    pub fn tick(&mut self) -> Vec<Envelope> {
        if self.removed {
            return Vec::new();
        }
        let out = match self.role {
            Role::Leader => {
                // CheckQuorum: a leader that has not heard from a majority
                // for a whole election timeout cannot commit anything and
                // may already have been replaced; stop pretending.
                // The window is deliberately long (5 s of ticks). Every node
                // compacts at the same applied index, so all followers can
                // stall together for as long as a snapshot takes; a leader
                // must ride that out. An isolated leader still steps down,
                // just not instantly (it is harmless meanwhile: it cannot
                // commit or confirm a read without a majority).
                self.leader_check_elapsed += 1;
                if self.leader_check_elapsed >= CHECK_QUORUM_TICKS {
                    self.leader_check_elapsed = 0;
                    let reachable = 1 + self
                        .heard_from
                        .iter()
                        .filter(|p| self.peers.contains(p))
                        .count();
                    self.heard_from.clear();
                    if reachable < self.quorum() {
                        self.step_down_keeping_vote();
                        return self.with_outbox(Vec::new());
                    }
                }
                self.heartbeat_elapsed += 1;
                if self.heartbeat_elapsed >= HEARTBEAT_INTERVAL {
                    self.heartbeat_elapsed = 0;
                    self.broadcast_append_entries()
                } else {
                    Vec::new()
                }
            }
            Role::Follower | Role::Candidate => {
                self.election_elapsed += 1;
                if self.active && self.election_elapsed >= self.election_timeout {
                    // Ask first, campaign only if a majority would vote yes.
                    // A single node has nobody to ask.
                    if self.peers.is_empty() {
                        self.start_election()
                    } else {
                        self.start_pre_vote()
                    }
                } else {
                    Vec::new()
                }
            }
        };
        self.with_outbox(out)
    }

    /// Append any queued out-of-band messages to `out`.
    fn with_outbox(&mut self, mut out: Vec<Envelope>) -> Vec<Envelope> {
        out.append(&mut self.outbox);
        out
    }

    /// Begin a pre-vote round: ask every peer whether it would vote for us at
    /// `current_term + 1`, without changing our term, our vote or our role.
    fn start_pre_vote(&mut self) -> Vec<Envelope> {
        self.reset_election_timer(); // also clears any previous round
                                     // We no longer believe in a live leader. Without this, a node that has
                                     // just timed out would still count as "recently heard from a leader"
                                     // and refuse everybody else's pre-votes; followers that time out
                                     // together would then veto each other and nobody could campaign.
        self.leader_id = None;
        self.pre_voting = true;
        self.pre_votes.insert(self.id);
        self.peers
            .iter()
            .map(|&peer| {
                Envelope::new(
                    peer,
                    Message::PreVote {
                        term: self.current_term + 1,
                        candidate_id: self.id,
                        last_log_index: self.log.last_index(),
                        last_log_term: self.log.last_term(),
                    },
                )
            })
            .collect()
    }

    /// Tell a leader that `peer` is alive even though it has not answered
    /// lately, e.g. because it is busy installing a snapshot we are sending
    /// it. Counts for CheckQuorum only; a no-op on followers and for
    /// non-members.
    pub fn mark_peer_active(&mut self, peer: NodeId) {
        if self.role == Role::Leader && self.peers.contains(&peer) {
            self.heard_from.insert(peer);
        }
    }

    /// A leader that lost contact with a majority becomes a follower *in the
    /// same term*. Unlike `become_follower` it keeps `voted_for`: this node
    /// voted for itself in this term, and forgetting that would let it vote
    /// for a rival in the same term, which can produce two leaders.
    fn step_down_keeping_vote(&mut self) {
        self.role = Role::Follower;
        self.leader_id = None;
        self.votes_received.clear();
        self.pending_reads.clear();
        self.reset_election_timer();
    }

    fn start_election(&mut self) -> Vec<Envelope> {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.persist_hard_state();
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.leader_id = None;
        self.reset_election_timer();

        // Single-node cluster: win immediately.
        if self.peers.is_empty() {
            self.become_leader();
            return Vec::new();
        }

        self.peers
            .iter()
            .map(|&peer| {
                Envelope::new(
                    peer,
                    Message::RequestVote {
                        term: self.current_term,
                        candidate_id: self.id,
                        last_log_index: self.log.last_index(),
                        last_log_term: self.log.last_term(),
                    },
                )
            })
            .collect()
    }

    /// Leader-only. Forces an immediate `AppendEntries` broadcast to all
    /// peers, bypassing the heartbeat timer. Call it right after a successful
    /// `propose()` so a new write starts replicating at once. Otherwise the
    /// write waits for the next heartbeat, and its latency is bounded by the
    /// heartbeat period instead of by the replication work.
    pub fn force_heartbeat(&mut self) -> Vec<Envelope> {
        if self.role != Role::Leader || self.removed {
            return Vec::new();
        }
        self.heartbeat_elapsed = 0;
        let out = self.broadcast_append_entries();
        self.with_outbox(out)
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.heartbeat_elapsed = 0;
        // Give every peer one full interval to prove it is reachable.
        self.heard_from = self.peers.clone();
        self.leader_check_elapsed = 0;
        let next = self.log.last_index() + 1;
        self.next_index = self.peers.iter().map(|&p| (p, next)).collect();
        self.match_index = self.peers.iter().map(|&p| (p, 0)).collect();
        // §8: append a no-op in our own term right away. Without this, a
        // fresh leader can be stuck unable to advance commit_index over
        // entries replicated under a previous leader/term (§5.4.2's
        // "Figure 8" rule) until some client happens to propose something
        // new - which might never happen, silently stalling reads and
        // application of already-safely-replicated entries.
        self.term_start_index = self
            .log
            .append_config_entry(self.current_term, EntryKind::NoOp);
        // A single-node cluster never receives a reply that would advance
        // the commit index, so commit what this leader alone constitutes a
        // quorum for - including entries recovered from disk after a
        // restart, which would otherwise stay unapplied until the next
        // client write. (A no-op for multi-node clusters.)
        self.advance_commit_index();
    }

    fn broadcast_append_entries(&mut self) -> Vec<Envelope> {
        let peers: Vec<NodeId> = self.peers.iter().copied().collect();
        peers
            .into_iter()
            .map(|peer| self.build_replication_message(peer))
            .collect()
    }

    /// The next message for `peer`: a snapshot if it has fallen behind the
    /// compaction boundary, otherwise a size-bounded AppendEntries.
    fn build_replication_message(&self, peer: NodeId) -> Envelope {
        let leader_commit = self.commit_index;
        let last_included_index = self.log.last_included_index();
        let next = *self.next_index.get(&peer).unwrap_or(&1);
        if next <= last_included_index {
            // This follower needs entries we've already compacted
            // away (Raft paper §7) - send the snapshot instead.
            return Envelope::new(
                peer,
                Message::InstallSnapshot {
                    term: self.current_term,
                    leader_id: self.id,
                    last_included_index,
                    last_included_term: self.log.last_included_term(),
                    data: self.log.snapshot_data().unwrap_or(&[]).to_vec(),
                    peers: self.log.snapshot_peers().to_vec(),
                },
            );
        }
        let prev_index = next.saturating_sub(1);
        let prev_term = self.log.term_at(prev_index);
        let entries = self.bounded_entries_from(next);
        Envelope::new(
            peer,
            Message::AppendEntries {
                term: self.current_term,
                leader_id: self.id,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit,
                read_ctx: self.current_read_ctx(),
            },
        )
    }

    /// Entries starting at `from`, capped by the replication limits (but
    /// always at least one if any exist).
    fn bounded_entries_from(&self, from: LogIndex) -> Vec<LogEntry> {
        let mut out: Vec<LogEntry> = Vec::new();
        let mut bytes = 0usize;
        let mut idx = from;
        while let Some(e) = self.log.entry_at(idx) {
            let cost = e.data.len() + 32; // payload + rough per-entry overhead
            if !out.is_empty()
                && (out.len() >= self.max_append_entries || bytes + cost > self.max_append_bytes)
            {
                break;
            }
            bytes += cost;
            out.push(e.clone());
            idx += 1;
        }
        out
    }

    /// Leader-only: append `data` to the log. Replication happens on the
    /// next heartbeat/tick. Returns the assigned log index, or None if
    /// this node isn't the leader.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<LogIndex> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.log.append(self.current_term, data);
        // Single-node cluster: commit immediately.
        if self.peers.is_empty() {
            self.commit_index = index;
        }
        Some(index)
    }

    /// Leader-only **group commit**: append all of `batch` as consecutive
    /// entries with a single durable write (one fsync for a `WalLog`) and
    /// return the half-open index range they occupy, or `None` if this node
    /// is not the leader.
    ///
    /// The entries are in the log (and durable on this node) when this
    /// returns, but they are *not* committed: that still requires a quorum,
    /// exactly as for `propose`. The batch is not atomic - entries commit
    /// and apply independently, in order, and a leadership change may
    /// commit only a prefix of it.
    pub fn propose_batch(&mut self, batch: Vec<Vec<u8>>) -> Option<std::ops::Range<LogIndex>> {
        if self.role != Role::Leader {
            return None;
        }
        let n = batch.len() as LogIndex;
        if n == 0 {
            let next = self.log.last_index() + 1;
            return Some(next..next);
        }
        let first = self.log.append_batch(self.current_term, batch);
        debug_assert_eq!(first + n - 1, self.log.last_index());
        // Single-node cluster: durable on this node *is* a majority.
        if self.peers.is_empty() {
            self.commit_index = first + n - 1;
        }
        Some(first..first + n)
    }

    /// Feed an inbound message from `from`. Returns any resulting outbound
    /// messages (RPC replies, or new AppendEntries after a commit-index bump).
    pub fn step(&mut self, from: NodeId, msg: Message) -> Vec<Envelope> {
        // A removed node is inert, and nothing from a removed node is
        // processed. Both checks come *before* the "higher term => become
        // follower" rule, so neither can disturb this node's state.
        if self.removed {
            return Vec::new();
        }
        let sender = match &msg {
            Message::RequestVote { candidate_id, .. } | Message::PreVote { candidate_id, .. } => {
                *candidate_id
            }
            Message::AppendEntries { leader_id, .. }
            | Message::InstallSnapshot { leader_id, .. } => *leader_id,
            _ => from,
        };
        if self.tombstones.contains(&from) || self.tombstones.contains(&sender) {
            return Vec::new();
        }
        let out = self.step_inner(from, msg);
        self.with_outbox(out)
    }

    fn step_inner(&mut self, from: NodeId, msg: Message) -> Vec<Envelope> {
        let msg_term = match &msg {
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::PreVote { term, .. }
            | Message::PreVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. }
            | Message::InstallSnapshot { term, .. }
            | Message::InstallSnapshotReply { term, .. } => *term,
        };

        // Rule for all servers (Figure 2): if RPC term > currentTerm,
        // convert to follower.
        // Pre-votes carry a term in the *future*; neither asking nor being
        // granted one may change our term (that is their whole point). A
        // refusal carries the responder's real term, which does count.
        let hypothetical_term = matches!(
            msg,
            Message::PreVote { .. }
                | Message::PreVoteReply {
                    vote_granted: true,
                    ..
                }
        );
        if msg_term > self.current_term && !hypothetical_term {
            let leader_hint = match &msg {
                Message::AppendEntries { leader_id, .. } => Some(*leader_id),
                _ => None,
            };
            self.become_follower(msg_term, leader_hint);
        }

        match msg {
            Message::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(from, term, candidate_id, last_log_index, last_log_term),
            Message::RequestVoteReply { term, vote_granted } => {
                self.handle_request_vote_reply(from, term, vote_granted)
            }
            Message::PreVote {
                term,
                last_log_index,
                last_log_term,
                ..
            } => self.handle_pre_vote(from, term, last_log_index, last_log_term),
            Message::PreVoteReply { term, vote_granted } => {
                self.handle_pre_vote_reply(from, term, vote_granted)
            }
            Message::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_ctx,
            } => self.handle_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_ctx,
            ),
            Message::AppendEntriesReply {
                term,
                success,
                match_index,
                read_ctx,
            } => self.handle_append_entries_reply(from, term, success, match_index, read_ctx),
            Message::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
                peers,
            } => self.handle_install_snapshot(
                from,
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
                peers,
            ),
            Message::InstallSnapshotReply { term } => {
                self.handle_install_snapshot_reply(from, term)
            }
        }
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Vec<Envelope> {
        // Stale term: reject.
        if term < self.current_term {
            return vec![Envelope::new(
                from,
                Message::RequestVoteReply {
                    term: self.current_term,
                    vote_granted: false,
                },
            )];
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
        // §5.4.1: candidate's log must be at least as up-to-date as ours.
        let candidate_up_to_date =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());

        let grant = can_vote && candidate_up_to_date;
        if grant {
            self.voted_for = Some(candidate_id);
            // Must hit disk before we tell the candidate "yes" - otherwise
            // a crash+restart right after this could let us vote again for
            // someone else in the same term, the exact thing votedFor
            // persistence exists to prevent.
            self.persist_hard_state();
            self.reset_election_timer();
        }

        vec![Envelope::new(
            from,
            Message::RequestVoteReply {
                term: self.current_term,
                vote_granted: grant,
            },
        )]
    }

    /// Answer a pre-vote. Changes nothing: no term bump, no persisted vote, no
    /// timer reset.
    fn handle_pre_vote(
        &mut self,
        from: NodeId,
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Vec<Envelope> {
        // We would only help a campaign for a term above ours...
        let newer_term = term > self.current_term;
        // ...by a candidate whose log is at least as up to date as ours...
        let up_to_date =
            (last_log_term, last_log_index) >= (self.log.last_term(), self.log.last_index());
        // ...while we have no live leader. A leader never grants (it is the
        // leader), and a follower that heard from one within the minimum
        // election timeout refuses, so a node that only *thinks* the leader
        // is gone cannot disturb a cluster that is working.
        let leader_is_live = self.role == Role::Leader
            || (self.leader_id.is_some() && self.election_elapsed < ELECTION_TIMEOUT_MIN);
        let grant = newer_term && up_to_date && !leader_is_live;
        vec![Envelope::new(
            from,
            Message::PreVoteReply {
                term: if grant { term } else { self.current_term },
                vote_granted: grant,
            },
        )]
    }

    fn handle_pre_vote_reply(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
    ) -> Vec<Envelope> {
        // A refusal from a higher term was already adopted by the generic
        // term rule in `step_inner`.
        if !vote_granted
            || !self.pre_voting
            || self.role == Role::Leader
            || term != self.current_term + 1
            || !self.peers.contains(&from)
        {
            return Vec::new();
        }
        self.pre_votes.insert(from);
        if self.pre_votes.len() >= self.quorum() {
            // A majority would vote for us: campaign for real.
            return self.start_election();
        }
        Vec::new()
    }

    fn handle_request_vote_reply(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
    ) -> Vec<Envelope> {
        if self.role != Role::Candidate || term != self.current_term || !vote_granted {
            return Vec::new();
        }
        // Only a current member's vote counts toward a quorum.
        if !self.peers.contains(&from) {
            return Vec::new();
        }
        self.votes_received.insert(from);
        if self.votes_received.len() >= self.quorum() {
            self.become_leader();
            return self.broadcast_append_entries();
        }
        Vec::new()
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        read_ctx: u64,
    ) -> Vec<Envelope> {
        if term < self.current_term {
            return vec![Envelope::new(
                from,
                Message::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: 0,
                    read_ctx,
                },
            )];
        }

        // A valid leader for our term: stay/convert to follower and reset
        // the election timer (this is the heartbeat).
        self.role = Role::Follower;
        self.leader_id = Some(leader_id);
        self.reset_election_timer();

        if !self.log.contains(prev_log_index, prev_log_term) {
            return vec![Envelope::new(
                from,
                Message::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: self.log.last_index(),
                    read_ctx,
                },
            )];
        }

        // Defend against a malformed message: the entries must be a
        // contiguous run starting right after `prev_log_index`.
        let well_formed = entries
            .iter()
            .enumerate()
            .all(|(i, e)| e.index == prev_log_index + 1 + i as LogIndex);
        if !well_formed {
            return vec![Envelope::new(
                from,
                Message::AppendEntriesReply {
                    term: self.current_term,
                    success: false,
                    match_index: self.log.last_index(),
                    read_ctx,
                },
            )];
        }

        // A correct leader never rewrites an entry we consider committed
        // (Leader Completeness). If one tries, continuing would silently
        // corrupt the state machine; stopping is the only safe response.
        if let Some(conflict) = self.log.first_conflict_index(&entries) {
            assert!(
                conflict > self.commit_index,
                "leader {leader_id} tried to overwrite committed entry {conflict}                  (commit_index {})",
                self.commit_index
            );
        }

        self.log.append_entries(prev_log_index, &entries);

        // Only the prefix this leader just verified matches its log (log
        // matching holds up to `last_new`); anything we hold beyond that
        // is unverified and must not be committed or reported as matching.
        let last_new = entries.last().map(|e| e.index).unwrap_or(prev_log_index);
        let new_commit = leader_commit.min(last_new);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
        }

        vec![Envelope::new(
            from,
            Message::AppendEntriesReply {
                term: self.current_term,
                success: true,
                match_index: last_new,
                read_ctx,
            },
        )]
    }

    fn handle_append_entries_reply(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        read_ctx: u64,
    ) -> Vec<Envelope> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        // A reply from a node that is not a current member (removed, or
        // never added) must not touch replication progress, the commit
        // quorum, or read confirmation.
        if !self.peers.contains(&from) {
            return Vec::new();
        }
        self.heard_from.insert(from);

        if success {
            // Monotonic: a delayed or duplicated reply must never move a
            // follower's match point backwards.
            let prev_match = *self.match_index.get(&from).unwrap_or(&0);
            let new_match = prev_match.max(match_index);
            self.match_index.insert(from, new_match);
            let next = self.next_index.entry(from).or_insert(1);
            *next = (*next).max(new_match + 1);
            self.advance_commit_index();
            self.record_leadership_ack(from, read_ctx);
            // Stream the rest of a backlog that didn't fit in one message.
            if new_match > prev_match && self.next_index[&from] <= self.log.last_index() {
                return vec![self.build_replication_message(from)];
            }
        } else {
            // Back up exactly one index per rejection (§5.3). That guarantees strict
            // progress toward the divergence point. Deriving next_index from the
            // follower's reported log length can livelock: on a term mismatch at an
            // index the follower already has, its last_index does not change between
            // retries, so next_index would recompute to the same value forever.
            let current_next = *self.next_index.get(&from).unwrap_or(&1);
            let new_next = current_next.saturating_sub(1).max(1);
            self.next_index.insert(from, new_next);
        }
        Vec::new()
    }

    /// §7: a leader sent us a snapshot because our `next_index` had fallen
    /// at or below what it had already compacted away - we have no way to
    /// catch up incrementally, so we adopt its snapshot wholesale.
    #[allow(clippy::too_many_arguments)]
    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        peers: Vec<NodeId>,
    ) -> Vec<Envelope> {
        if term < self.current_term {
            return vec![Envelope::new(
                from,
                Message::InstallSnapshotReply {
                    term: self.current_term,
                },
            )];
        }

        self.role = Role::Follower;
        self.leader_id = Some(leader_id);
        self.reset_election_timer();

        if last_included_index > self.log.last_included_index() {
            let data_for_caller = data.clone();
            // `peers` is full membership including this node's own ID
            // (see `compact_log`'s doc on the convention) - confirms
            // this node as an active member the same way a committed
            // AddNode entry naming its own ID would (see
            // `apply_config_change`), and gives it a correct peer set
            // even if the AddNode entries for some members were
            // compacted away before this node ever replayed the log.
            let im_a_member = peers.contains(&self.id);
            self.peers = peers.iter().copied().filter(|&p| p != self.id).collect();
            if im_a_member {
                self.active = true;
            }
            self.log
                .install_snapshot(last_included_index, last_included_term, data, peers);
            self.commit_index = self.commit_index.max(last_included_index);
            self.last_applied = self.last_applied.max(last_included_index);
            self.pending_snapshot = Some(data_for_caller);
        }
        // else: stale or duplicate snapshot - we already have at least
        // this much, silently ack without redoing the install.

        vec![Envelope::new(
            from,
            Message::InstallSnapshotReply {
                term: self.current_term,
            },
        )]
    }

    fn handle_install_snapshot_reply(&mut self, from: NodeId, term: Term) -> Vec<Envelope> {
        if self.role != Role::Leader || term != self.current_term {
            return Vec::new();
        }
        if !self.peers.contains(&from) {
            return Vec::new();
        }
        self.heard_from.insert(from);
        // A same-term reply means the follower now has everything up to
        // our current snapshot boundary - resume normal replication from
        // just past it.
        let last_included_index = self.log.last_included_index();
        let prev_match = *self.match_index.get(&from).unwrap_or(&0);
        let new_match = prev_match.max(last_included_index);
        self.match_index.insert(from, new_match);
        self.next_index.insert(from, new_match + 1);
        self.advance_commit_index();
        // Deliberately *not* a ReadIndex ack: the snapshot message carries
        // no read context, so it cannot prove leadership at any particular
        // moment. The AppendEntries sent next (below) does.
        // Continue with whatever follows the snapshot.
        if new_match < self.log.last_index() {
            return vec![self.build_replication_message(from)];
        }
        Vec::new()
    }

    /// §5.3/5.4: advance commit_index to the highest N replicated on a
    /// majority of servers, where log[N].term == currentTerm.
    fn advance_commit_index(&mut self) {
        let mut indices: Vec<LogIndex> = self.match_index.values().copied().collect();
        indices.push(self.log.last_index()); // the leader itself
        indices.sort_unstable();
        // With len == quorum-eligible count, the majority-replicated index
        // is the one at position (len - quorum).
        let n = indices.len();
        let q = self.quorum();
        if n < q {
            return;
        }
        let candidate = indices[n - q];
        if candidate > self.commit_index && self.log.term_at(candidate) == self.current_term {
            self.commit_index = candidate;
        }
    }

    /// Drain newly-committed entries (commit_index advanced past last_applied)
    /// for the caller to apply to its state machine.
    pub fn take_committed(&mut self) -> Vec<Committed> {
        let mut out = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(e) = self.log.entry_at(self.last_applied) {
                match e.kind {
                    EntryKind::Data => {
                        out.push(Committed {
                            index: e.index,
                            data: e.data.clone(),
                        });
                    }
                    EntryKind::NoOp => {
                        // Nothing to do - its only purpose was letting
                        // commit_index advance (see `become_leader`).
                    }
                    kind => {
                        // Config-change entries (AddNode/RemoveNode) are
                        // consumed here, not handed to the application.
                        self.apply_config_change(kind);
                    }
                }
            }
        }
        self.promote_ready_reads();
        out
    }
}
