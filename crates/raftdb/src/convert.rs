//! Conversions between `raft_core::Message`/`LogEntry` and the generated
//! protobuf types in `raftdb_proto`. Kept mechanical and boring on
//! purpose - this file should never contain any actual Raft logic.

use raft_core::{EntryKind, LogEntry, Message, NodeId};
use raftdb_proto as proto;

pub fn log_entry_to_proto(e: &LogEntry) -> proto::LogEntry {
    let (kind_tag, kind_node_id): (u32, u64) = match e.kind {
        EntryKind::Data => (0, 0),
        EntryKind::AddNode(id) => (1, id),
        EntryKind::RemoveNode(id) => (2, id),
        EntryKind::NoOp => (3, 0),
    };
    proto::LogEntry {
        term: e.term,
        index: e.index,
        data: e.data.clone(),
        kind_tag,
        kind_node_id,
    }
}

pub fn log_entry_from_proto(e: proto::LogEntry) -> LogEntry {
    let kind = match e.kind_tag {
        1 => EntryKind::AddNode(e.kind_node_id),
        2 => EntryKind::RemoveNode(e.kind_node_id),
        3 => EntryKind::NoOp,
        _ => EntryKind::Data,
    };
    LogEntry {
        term: e.term,
        index: e.index,
        kind,
        data: e.data,
    }
}

// ---- RequestVote ----

pub fn request_vote_to_proto(msg: &Message) -> proto::RequestVoteRequest {
    match msg {
        Message::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => proto::RequestVoteRequest {
            term: *term,
            candidate_id: *candidate_id,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term,
            pre_vote: false,
        },
        Message::PreVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => proto::RequestVoteRequest {
            term: *term,
            candidate_id: *candidate_id,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term,
            pre_vote: true,
        },
        _ => unreachable!("request_vote_to_proto called with a non-vote message"),
    }
}

/// Returns `(from, message)` - the candidate's own ID doubles as `from`,
/// per Raft semantics (no separate field needed on the wire).
pub fn request_vote_from_proto(req: proto::RequestVoteRequest) -> (NodeId, Message) {
    let from = req.candidate_id;
    let (term, candidate_id, last_log_index, last_log_term) = (
        req.term,
        req.candidate_id,
        req.last_log_index,
        req.last_log_term,
    );
    let msg = if req.pre_vote {
        Message::PreVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    } else {
        Message::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    };
    (from, msg)
}

pub fn request_vote_reply_to_proto(msg: &Message) -> proto::RequestVoteReply {
    match msg {
        Message::RequestVoteReply { term, vote_granted } => proto::RequestVoteReply {
            term: *term,
            vote_granted: *vote_granted,
            pre_vote: false,
        },
        Message::PreVoteReply { term, vote_granted } => proto::RequestVoteReply {
            term: *term,
            vote_granted: *vote_granted,
            pre_vote: true,
        },
        _ => unreachable!("request_vote_reply_to_proto called with a non-reply message"),
    }
}

pub fn request_vote_reply_from_proto(reply: proto::RequestVoteReply) -> Message {
    if reply.pre_vote {
        Message::PreVoteReply {
            term: reply.term,
            vote_granted: reply.vote_granted,
        }
    } else {
        Message::RequestVoteReply {
            term: reply.term,
            vote_granted: reply.vote_granted,
        }
    }
}

// ---- AppendEntries ----

pub fn append_entries_to_proto(msg: &Message) -> proto::AppendEntriesRequest {
    match msg {
        Message::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            read_ctx,
        } => proto::AppendEntriesRequest {
            term: *term,
            leader_id: *leader_id,
            prev_log_index: *prev_log_index,
            prev_log_term: *prev_log_term,
            entries: entries.iter().map(log_entry_to_proto).collect(),
            leader_commit: *leader_commit,
            read_ctx: *read_ctx,
        },
        _ => unreachable!("append_entries_to_proto called with a non-AppendEntries message"),
    }
}

pub fn append_entries_from_proto(req: proto::AppendEntriesRequest) -> (NodeId, Message) {
    let from = req.leader_id;
    (
        from,
        Message::AppendEntries {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: req.prev_log_index,
            prev_log_term: req.prev_log_term,
            entries: req.entries.into_iter().map(log_entry_from_proto).collect(),
            leader_commit: req.leader_commit,
            read_ctx: req.read_ctx,
        },
    )
}

pub fn append_entries_reply_to_proto(msg: &Message) -> proto::AppendEntriesReply {
    match msg {
        Message::AppendEntriesReply {
            term,
            success,
            match_index,
            read_ctx,
        } => proto::AppendEntriesReply {
            term: *term,
            success: *success,
            match_index: *match_index,
            read_ctx: *read_ctx,
        },
        _ => unreachable!("append_entries_reply_to_proto called with a non-reply message"),
    }
}

pub fn append_entries_reply_from_proto(reply: proto::AppendEntriesReply) -> Message {
    Message::AppendEntriesReply {
        term: reply.term,
        success: reply.success,
        match_index: reply.match_index,
        read_ctx: reply.read_ctx,
    }
}

// ---- InstallSnapshot ----

pub fn install_snapshot_to_proto(msg: &Message) -> proto::InstallSnapshotRequest {
    match msg {
        Message::InstallSnapshot {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            data,
            peers,
        } => proto::InstallSnapshotRequest {
            term: *term,
            leader_id: *leader_id,
            last_included_index: *last_included_index,
            last_included_term: *last_included_term,
            data: data.clone(),
            peers: peers.clone(),
        },
        _ => unreachable!("install_snapshot_to_proto called with a non-InstallSnapshot message"),
    }
}

pub fn install_snapshot_from_proto(req: proto::InstallSnapshotRequest) -> (NodeId, Message) {
    let from = req.leader_id;
    (
        from,
        Message::InstallSnapshot {
            term: req.term,
            leader_id: req.leader_id,
            last_included_index: req.last_included_index,
            last_included_term: req.last_included_term,
            data: req.data,
            peers: req.peers,
        },
    )
}

pub fn install_snapshot_reply_to_proto(msg: &Message) -> proto::InstallSnapshotReply {
    match msg {
        Message::InstallSnapshotReply { term } => proto::InstallSnapshotReply { term: *term },
        _ => unreachable!("install_snapshot_reply_to_proto called with a non-reply message"),
    }
}

pub fn install_snapshot_reply_from_proto(reply: proto::InstallSnapshotReply) -> Message {
    Message::InstallSnapshotReply { term: reply.term }
}
