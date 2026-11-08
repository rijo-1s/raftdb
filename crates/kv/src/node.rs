//! `KvNode`: glues a `RaftNode` to a state machine. This is the "apply loop"
//! every real Raft-backed system needs - something that takes whatever
//! `RaftNode` just committed (or whatever snapshot it just installed) and
//! actually does something with it.
//!
//! `KvNode<L>` (the default `S = KvStore`) is the original in-memory
//! baseline and keeps its original API. `KvNode<L, S>` works with any
//! [`StateMachine`], e.g. the durable `LsmStore`.

use raft_core::{Envelope, LogIndex, LogStore, MemLog, Message, NodeId, RaftNode, Role};

use crate::state_machine::StateMachine;
use crate::store::{Command, KvStore};

pub struct KvNode<L: LogStore = MemLog, S: StateMachine = KvStore> {
    pub raft: RaftNode<L>,
    pub store: S,
    /// Compact once this many entries have been applied since the last
    /// snapshot. This is a policy; `compact_log()` is only the mechanism. Kept
    /// small in tests so compaction actually triggers.
    compaction_threshold: usize,
    applied_since_snapshot: usize,
    /// While true, compaction is deferred (the log keeps growing). Set by the
    /// host when the data volume is nearly full: a snapshot or log rewrite
    /// needs extra room while it runs and an out-of-space error there is
    /// fatal. It resumes by itself once this is cleared.
    pub compaction_paused: bool,
    /// Asked, with the size in bytes of the snapshot about to be written,
    /// whether there is room to write it. When it says no, compaction is
    /// skipped and retried after another `compaction_threshold` entries. A
    /// snapshot needs roughly its own size in free space while it is written,
    /// and running out of space there is fatal, so a refusal is far cheaper.
    pub snapshot_space_check: Option<SpaceCheck>,
}

/// See [`KvNode::snapshot_space_check`].
pub type SpaceCheck = Box<dyn Fn(u64) -> bool + Send>;

impl KvNode<MemLog> {
    pub fn new(id: NodeId, peers: Vec<NodeId>, compaction_threshold: usize) -> Self {
        Self::with_log(id, peers, MemLog::new(), compaction_threshold)
    }
}

impl<L: LogStore> KvNode<L> {
    pub fn with_log(id: NodeId, peers: Vec<NodeId>, log: L, compaction_threshold: usize) -> Self {
        Self::from_raft(RaftNode::with_log(id, peers, log), compaction_threshold)
    }

    /// See `RaftNode::joining` - for a node being added to an
    /// already-running cluster rather than bootstrapping a fresh one.
    pub fn joining(id: NodeId, peers: Vec<NodeId>, log: L, compaction_threshold: usize) -> Self {
        Self::from_raft(RaftNode::joining(id, peers, log), compaction_threshold)
    }

    /// See `RaftNode::with_log_seeded` - real deployments should use this
    /// (or `joining_seeded`), not the plain `with_log`/`joining`, which
    /// seed their election-timeout RNG purely from `id` and are meant for
    /// tests/the simulator only.
    pub fn with_log_seeded(
        id: NodeId,
        peers: Vec<NodeId>,
        log: L,
        compaction_threshold: usize,
        seed: u64,
    ) -> Self {
        Self::from_raft(
            RaftNode::with_log_seeded(id, peers, log, seed),
            compaction_threshold,
        )
    }

    pub fn joining_seeded(
        id: NodeId,
        peers: Vec<NodeId>,
        log: L,
        compaction_threshold: usize,
        seed: u64,
    ) -> Self {
        Self::from_raft(
            RaftNode::joining_seeded(id, peers, log, seed),
            compaction_threshold,
        )
    }

    fn from_raft(raft: RaftNode<L>, compaction_threshold: usize) -> Self {
        KvNode::with_state_machine(raft, KvStore::new(), compaction_threshold)
            .expect("persisted snapshot must be well-formed")
    }

    /// Reads the *local* state machine. Only linearizable if called after
    /// the read ID from `begin_linearizable_read()` shows up in
    /// `poll_ready_reads()` - otherwise this is a merely "probably fine"
    /// local read, not a guaranteed-fresh one.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.store.get(key)
    }
}

impl<L: LogStore, S: StateMachine> KvNode<L, S> {
    /// Attach a state machine to a (possibly restarted) Raft node and
    /// reconcile the two.
    ///
    /// * state machine **behind** the Raft snapshot boundary (fresh, wiped,
    ///   or volatile): rebuild it from the snapshot blob;
    /// * **at or ahead of** the boundary: keep it. Entries after the
    ///   boundary are re-delivered by Raft as commits are re-learned and
    ///   those the engine already holds are skipped by index;
    /// * ahead of the Raft **log** itself: impossible unless one of the two
    ///   was rolled back or swapped out - refuse rather than diverge.
    pub fn with_state_machine(
        raft: RaftNode<L>,
        mut store: S,
        compaction_threshold: usize,
    ) -> Result<Self, String> {
        let boundary = raft.log().last_included_index();
        let applied = store.applied_index();
        if applied > raft.log_last_index().max(boundary) {
            return Err(format!(
                "state machine has applied index {applied} but the Raft log ends at {}: \
                 the engine and the Raft log are from different histories; refusing to start",
                raft.log_last_index()
            ));
        }
        if applied < boundary {
            let snapshot = raft
                .snapshot_data()
                .ok_or("log is compacted but no snapshot data is available")?;
            store.restore_from_snapshot(snapshot, boundary)?;
        }
        Ok(Self {
            raft,
            store,
            compaction_threshold,
            applied_since_snapshot: 0,
            compaction_paused: false,
            snapshot_space_check: None,
        })
    }

    pub fn role(&self) -> Role {
        self.raft.role()
    }

    /// Leader-only. Returns the log index the write was assigned, or
    /// `None` if this node isn't the leader (the caller must find the
    /// actual leader and retry there).
    pub fn propose_set(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Option<LogIndex> {
        let cmd = Command::Set {
            key: key.into(),
            value: value.into(),
        };
        self.raft.propose(cmd.encode())
    }

    pub fn propose_delete(&mut self, key: impl Into<String>) -> Option<LogIndex> {
        let cmd = Command::Delete { key: key.into() };
        self.raft.propose(cmd.encode())
    }

    /// Highest sequence applied for `client`, if its session is tracked.
    pub fn session_last_seq(&self, client: u64) -> Option<u64> {
        self.store.session_last_seq(client)
    }

    /// §6.4 linearizable read, step 1: ask to confirm this node is still
    /// leader. `None` if it isn't - try elsewhere. Poll `poll_ready_reads()`
    /// afterward; only once it returns this ID is a read safe to trust.
    pub fn begin_linearizable_read(&mut self) -> Option<u64> {
        self.raft.request_read_index()
    }

    pub fn poll_ready_reads(&mut self) -> Vec<u64> {
        self.raft.take_ready_reads()
    }

    /// Read from the local state machine (see `get` for the caveat).
    pub fn read(&self, key: &str) -> Result<Option<String>, String> {
        self.store.read(key)
    }

    pub fn tick(&mut self) -> Vec<Envelope> {
        let out = self.raft.tick();
        self.apply_ready();
        out
    }

    pub fn step(&mut self, from: NodeId, msg: Message) -> Vec<Envelope> {
        let out = self.raft.step(from, msg);
        self.apply_ready();
        out
    }

    /// Drain whatever `raft-core` has ready - a snapshot to load, and/or
    /// newly committed entries to apply - and feed it into the state
    /// machine. Compacts the log afterward if enough entries have piled up.
    ///
    /// A failure of the state machine is fatal (a replica that skips a write
    /// has diverged from every other), so it panics rather than limps on.
    fn apply_ready(&mut self) {
        if let Some(snapshot) = self.raft.take_pending_snapshot() {
            let boundary = self.raft.log().last_included_index();
            self.store
                .restore_from_snapshot(&snapshot, boundary)
                .expect("leader-sent snapshot must be well-formed");
            self.applied_since_snapshot = 0;
        }

        let committed = self.raft.take_committed();
        if !committed.is_empty() {
            let batch: Vec<(LogIndex, &[u8])> = committed
                .iter()
                .map(|c| (c.index, c.data.as_slice()))
                .collect();
            if let Err(e) = self.store.apply_batch(&batch) {
                panic!(
                    "state machine apply failed at index {}: {e}",
                    committed.last().map_or(0, |c| c.index)
                );
            }
            self.applied_since_snapshot += committed.len();
        }

        if self.applied_since_snapshot >= self.compaction_threshold && !self.compaction_paused {
            // The log is about to forget these entries: the state machine
            // must be able to reproduce them by itself first.
            self.store
                .durability_barrier()
                .expect("state machine durability barrier failed");
            let snapshot = self
                .store
                .snapshot_bytes()
                .expect("state machine snapshot failed");
            let room = self
                .snapshot_space_check
                .as_ref()
                .is_none_or(|has_room| has_room(snapshot.len() as u64));
            if !room {
                // Try again after another `compaction_threshold` entries.
                self.applied_since_snapshot = 0;
            } else if self.raft.compact_log(snapshot).is_some() {
                self.applied_since_snapshot = 0;
            }
        }
    }
}
