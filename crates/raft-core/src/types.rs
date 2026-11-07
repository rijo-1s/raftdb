pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

/// What kind of thing a log entry represents. `Data` entries are opaque
/// application commands (raft-core never looks inside them - see
/// `LogEntry::data`); `AddNode`/`RemoveNode` are membership changes,
/// which raft-core *does* understand and applies internally as they
/// commit (see `RaftNode::take_committed`) - they never reach the
/// application's state machine, matching how e.g. etcd keeps
/// `EntryConfChange` distinct from `EntryNormal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Data,
    AddNode(NodeId),
    RemoveNode(NodeId),
    /// A blank entry a leader appends in its own term immediately upon
    /// election (Raft paper §8). Never surfaced to the application - its
    /// only purpose is giving a fresh leader something of its own current
    /// term to commit, which is required before it can safely advance its
    /// commit index over entries replicated under a *previous* leader
    /// (the "Figure 8" problem, §5.4.2: a leader must not conclude an
    /// older-term entry is committed by counting replicas alone).
    NoOp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub kind: EntryKind,
    /// Only meaningful when `kind == EntryKind::Data`; empty otherwise.
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

/// All RPCs a node can send/receive. This mirrors the Raft paper's
/// RequestVote and AppendEntries RPCs (Figure 2).
#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteReply {
        term: Term,
        vote_granted: bool,
    },
    /// "Would you vote for me if I started an election?" (Raft thesis §9.6).
    /// Sent when an election timer fires, *before* the candidate raises its
    /// term. `term` is the term the candidate would use (its current term
    /// plus one). Receiving or answering one never changes anybody's term or
    /// vote, so a node that is merely slow or cut off cannot push the term up
    /// and depose a healthy leader.
    PreVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    /// Answer to [`Message::PreVote`]. When granted, `term` echoes the
    /// proposed term; when refused it is the responder's own term, which a
    /// stale candidate adopts.
    PreVoteReply {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
        /// ReadIndex context (§6.4). `0` means "no read is waiting on this
        /// message". Otherwise it is the number of linearizable reads the
        /// leader had issued when it built this message: a follower
        /// echoes it back unchanged in its reply, and the leader credits
        /// that reply only to reads whose id is *below* this value, i.e.
        /// reads that were already pending when this message was sent. A
        /// reply to an older message therefore can never confirm a newer
        /// read, however late it arrives.
        read_ctx: u64,
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        /// Index of the last entry this follower now has, used by the
        /// leader to update `match_index`/`next_index` on success, or to
        /// backtrack quickly on failure (we send the follower's log length).
        match_index: LogIndex,
        /// Echo of the request's `read_ctx`; see `AppendEntries::read_ctx`.
        read_ctx: u64,
    },
    /// Sent instead of AppendEntries when the leader's `next_index` for a
    /// follower has fallen at or below what the leader has compacted away
    /// (Raft paper §7). `data` is the state machine's opaque snapshot
    /// bytes - `raft-core` never looks inside it.
    InstallSnapshot {
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        /// Full cluster membership (every node ID, including the sender's
        /// own) as of the snapshot boundary. This is what lets a node that
        /// only ever catches up via snapshot - never replaying the log
        /// entries before the boundary - still learn about members whose
        /// `AddNode` entry was compacted away before it ever saw it. The
        /// receiver filters out its own ID when adopting this as its peer
        /// set (see `RaftNode::handle_install_snapshot`).
        peers: Vec<NodeId>,
    },
    InstallSnapshotReply {
        term: Term,
    },
}

/// An outbound message paired with its destination.
#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub to: NodeId,
    pub msg: Message,
}

impl Envelope {
    pub fn new(to: NodeId, msg: Message) -> Self {
        Self { to, msg }
    }
}
