use crate::types::{EntryKind, LogEntry, LogIndex, NodeId, Term};

/// Everything `RaftNode` needs from the Raft paper's "persistent state"
/// (Figure 2) plus snapshotting (§7): the replicated log, `currentTerm`/
/// `votedFor`, and - once the log has been compacted - a boundary
/// (`last_included_index`/`last_included_term`) plus opaque state-machine
/// snapshot bytes covering everything up to that boundary.
///
/// This is a trait, not a concrete type, so `RaftNode<L: LogStore>` doesn't
/// care whether `L` is in-memory (`MemLog`) or disk-backed
/// (`storage::WalLog`). Indices are 1-based; index 0 means "before the log
/// begins". Once compaction has happened, indices at or below
/// `last_included_index` are no longer stored as entries - the snapshot
/// stands in for them.
pub trait LogStore {
    fn last_index(&self) -> LogIndex;
    fn last_term(&self) -> Term;
    fn term_at(&self, index: LogIndex) -> Term;
    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry>;

    /// Append a brand-new entry (used by the leader on `propose`).
    fn append(&mut self, term: Term, data: Vec<u8>) -> LogIndex;

    /// Append several brand-new entries at once and return the index of the
    /// first (they occupy `first..first + data.len()`). **Group commit
    /// hook**: a durable store should persist the whole batch with a single
    /// fsync, and must not return until every entry is durable. The default
    /// just loops over `append`, which is correct but pays one fsync each.
    fn append_batch(&mut self, term: Term, data: Vec<Vec<u8>>) -> LogIndex {
        let first = self.last_index() + 1;
        for d in data {
            self.append(term, d);
        }
        first
    }

    /// Append a membership-change entry (used by `propose_add_node`/
    /// `propose_remove_node`). Kept as a separate method from `append`
    /// rather than an overload, so every `LogStore` implementor has to
    /// consciously handle it rather than silently mishandling `kind`.
    fn append_config_entry(&mut self, term: Term, kind: EntryKind) -> LogIndex;

    /// Slice of entries starting at `from_index` (1-based, inclusive).
    /// Callers must not pass `from_index <= last_included_index()` - that
    /// range has been compacted away; use a snapshot (`InstallSnapshot`)
    /// instead. `RaftNode` enforces this before ever calling in here.
    fn entries_from(&self, from_index: LogIndex) -> Vec<LogEntry>;

    /// Merge `entries` (which follow `prev_index`) into the log per Raft
    /// §5.3: an entry we already hold with the same index *and* term is
    /// left alone; only an entry that **conflicts** (same index, different
    /// term) causes it and everything after it to be discarded. A stale or
    /// duplicated AppendEntries therefore never erases entries we hold.
    fn append_entries(&mut self, prev_index: LogIndex, entries: &[LogEntry]);

    /// Index of the first of `entries` that conflicts with this log (we
    /// hold an entry at that index with a different term), if any.
    /// `RaftNode` uses this to refuse to overwrite committed entries.
    fn first_conflict_index(&self, entries: &[LogEntry]) -> Option<LogIndex> {
        entries
            .iter()
            .find(|e| {
                e.index > self.last_included_index()
                    && e.index <= self.last_index()
                    && self.term_at(e.index) != e.term
            })
            .map(|e| e.index)
    }

    /// Persist `currentTerm`/`votedFor`. Must be durable before `RaftNode`
    /// acts on the vote it just cast or the term it just adopted - see the
    /// Raft paper's rule that a server must not vote twice in the same
    /// term, even across a restart.
    fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>);

    /// Load whatever `currentTerm`/`votedFor` were last persisted, or
    /// `(0, None)` for a brand-new node.
    fn load_hard_state(&self) -> (Term, Option<NodeId>);

    /// Index of the last entry covered by the current snapshot, or 0 if
    /// the log has never been compacted.
    fn last_included_index(&self) -> LogIndex;

    /// Term of the entry at `last_included_index`.
    fn last_included_term(&self) -> Term;

    /// The current snapshot's opaque bytes, if any.
    fn snapshot_data(&self) -> Option<&[u8]>;

    /// Cluster membership as of the current snapshot boundary (every
    /// member's ID, including whichever node's own set this was taken
    /// from). Empty if the log has never been compacted. This exists so a
    /// node that only ever catches up via snapshot - never replaying the
    /// log entries before the boundary - can still learn correct
    /// membership, including members whose `AddNode` entry was compacted
    /// away before this node ever saw it.
    fn snapshot_peers(&self) -> &[NodeId];

    /// Discard every entry at or below `up_to_index` (which must already
    /// be applied to the state machine - the caller, `RaftNode::compact_log`,
    /// enforces this), replacing them with `snapshot_data` and recording
    /// `snapshot_peers` as the membership as of this boundary. A no-op if
    /// `up_to_index` doesn't move the boundary forward.
    fn compact(
        &mut self,
        up_to_index: LogIndex,
        up_to_term: Term,
        snapshot_data: Vec<u8>,
        snapshot_peers: Vec<NodeId>,
    );

    /// Install a snapshot received from a leader via `InstallSnapshot`:
    /// adopt the leader's snapshot boundary, bytes, and membership. If this
    /// log already holds the snapshot's last entry (same index and term),
    /// the entries after it are retained (§7) - they may already have been
    /// acknowledged to the leader. Otherwise the whole local log is
    /// discarded. A no-op
    /// if `last_included_index` doesn't move the boundary forward (a stale
    /// or duplicate InstallSnapshot).
    fn install_snapshot(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        peers: Vec<NodeId>,
    );

    /// §5.4.1/§7: does this log contain an entry at `index` with term
    /// `term`? Has a default impl in terms of the methods above.
    fn contains(&self, index: LogIndex, term: Term) -> bool {
        if index == 0 {
            return term == 0;
        }
        self.term_at(index) == term && index <= self.last_index()
    }
}

/// In-memory `LogStore`. Nothing here survives a restart - this is what
/// `RaftNode::new()` uses by default, and what the deterministic cluster
/// simulation in `tests/cluster_sim.rs` uses, since "restart" there just
/// means "construct a fresh RaftNode", not "reload from disk".
///
/// `entries[0]` holds absolute index `last_included_index + 1`; everything
/// at or below `last_included_index` has been compacted into `snapshot`.
#[derive(Debug, Default)]
pub struct MemLog {
    entries: Vec<LogEntry>,
    hard_term: Term,
    hard_voted_for: Option<NodeId>,
    last_included_index: LogIndex,
    last_included_term: Term,
    snapshot: Option<Vec<u8>>,
    snapshot_peers: Vec<NodeId>,
}

impl MemLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert an absolute log index into an index into `self.entries`,
    /// if it's currently stored there (i.e. not compacted away).
    fn local_index(&self, index: LogIndex) -> Option<usize> {
        if index <= self.last_included_index {
            return None;
        }
        Some((index - self.last_included_index - 1) as usize)
    }
}

impl LogStore for MemLog {
    fn last_index(&self) -> LogIndex {
        self.last_included_index + self.entries.len() as LogIndex
    }

    fn last_term(&self) -> Term {
        self.entries
            .last()
            .map(|e| e.term)
            .unwrap_or(self.last_included_term)
    }

    fn term_at(&self, index: LogIndex) -> Term {
        if index == 0 {
            return 0;
        }
        if index == self.last_included_index {
            return self.last_included_term;
        }
        match self.local_index(index) {
            Some(local) => self.entries.get(local).map(|e| e.term).unwrap_or(0),
            None => 0, // either 0 or compacted-away and not the boundary: unknown
        }
    }

    fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        self.local_index(index)
            .and_then(|local| self.entries.get(local))
    }

    fn append(&mut self, term: Term, data: Vec<u8>) -> LogIndex {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry {
            term,
            index,
            kind: EntryKind::Data,
            data,
        });
        index
    }

    fn append_config_entry(&mut self, term: Term, kind: EntryKind) -> LogIndex {
        let index = self.last_index() + 1;
        self.entries.push(LogEntry {
            term,
            index,
            kind,
            data: Vec::new(),
        });
        index
    }

    fn entries_from(&self, from_index: LogIndex) -> Vec<LogEntry> {
        if from_index > self.last_index() {
            return Vec::new();
        }
        match self.local_index(from_index) {
            Some(local) => self.entries[local..].to_vec(),
            None => Vec::new(), // at/below the snapshot boundary: caller's bug
        }
    }

    fn append_entries(&mut self, _prev_index: LogIndex, entries: &[LogEntry]) {
        // §5.3: truncate only on a genuine conflict; skip entries we
        // already hold (see the trait docs).
        for e in entries {
            if e.index <= self.last_included_index {
                continue;
            }
            if e.index <= self.last_index() {
                if self.term_at(e.index) == e.term {
                    continue;
                }
                let keep = (e.index - self.last_included_index - 1) as usize;
                self.entries.truncate(keep);
            }
            debug_assert_eq!(e.index, self.last_index() + 1);
            self.entries.push(e.clone());
        }
    }

    fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        self.hard_term = term;
        self.hard_voted_for = voted_for;
    }

    fn load_hard_state(&self) -> (Term, Option<NodeId>) {
        (self.hard_term, self.hard_voted_for)
    }

    fn last_included_index(&self) -> LogIndex {
        self.last_included_index
    }

    fn last_included_term(&self) -> Term {
        self.last_included_term
    }

    fn snapshot_data(&self) -> Option<&[u8]> {
        self.snapshot.as_deref()
    }

    fn snapshot_peers(&self) -> &[NodeId] {
        &self.snapshot_peers
    }

    fn compact(
        &mut self,
        up_to_index: LogIndex,
        up_to_term: Term,
        snapshot_data: Vec<u8>,
        snapshot_peers: Vec<NodeId>,
    ) {
        if up_to_index <= self.last_included_index {
            return; // already compacted at least this far
        }
        let drop_count = (up_to_index - self.last_included_index) as usize;
        self.entries.drain(0..drop_count.min(self.entries.len()));
        self.last_included_index = up_to_index;
        self.last_included_term = up_to_term;
        self.snapshot = Some(snapshot_data);
        self.snapshot_peers = snapshot_peers;
    }

    fn install_snapshot(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
        data: Vec<u8>,
        peers: Vec<NodeId>,
    ) {
        if last_included_index <= self.last_included_index {
            return; // stale/duplicate: we already have at least this much
        }
        // §7: keep a suffix we already hold if it extends the snapshot's
        // last entry (same index and term); otherwise discard the log.
        let retain = last_included_index <= self.last_index()
            && self.term_at(last_included_index) == last_included_term;
        if retain {
            let drop_count = (last_included_index - self.last_included_index) as usize;
            self.entries.drain(0..drop_count.min(self.entries.len()));
        } else {
            self.entries.clear();
        }
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
        self.snapshot = Some(data);
        self.snapshot_peers = peers;
    }
}
