//! The seam between the replicated log and whatever stores the key/value
//! data. `KvStore` (volatile, in memory) is the correctness baseline;
//! `LsmStore` (durable, on disk) is the production engine. Both must be
//! observationally identical, which the differential tests enforce.

use raft_core::LogIndex;

use crate::store::KvStore;

pub trait StateMachine {
    /// Apply one committed command. `index` is its Raft log index. An engine
    /// that persists its position must ignore an `index` it already holds.
    fn apply(&mut self, index: LogIndex, data: &[u8]) -> Result<(), String>;

    /// Apply a run of committed commands (consecutive indexes) as one unit
    /// of work. Engines override this to share a single durable write.
    fn apply_batch(&mut self, entries: &[(LogIndex, &[u8])]) -> Result<(), String> {
        for (i, d) in entries {
            self.apply(*i, d)?;
        }
        Ok(())
    }

    fn read(&self, key: &str) -> Result<Option<String>, String>;

    /// The whole state as the opaque Raft snapshot blob. Both engines emit
    /// the same bytes for the same contents, so snapshots interoperate.
    fn snapshot_bytes(&self) -> Result<Vec<u8>, String>;

    /// Replace the whole state with a snapshot blob taken at `index`.
    fn restore_from_snapshot(&mut self, bytes: &[u8], index: LogIndex) -> Result<(), String>;

    /// Highest log index whose effects this engine already holds (0 for a
    /// volatile engine, which always restarts empty).
    fn applied_index(&self) -> LogIndex {
        0
    }

    /// Highest sequence number applied for `client` (see `session.rs`), if
    /// this engine remembers the client. Used to answer a retried write.
    fn session_last_seq(&self, _client: u64) -> Option<u64> {
        None
    }

    /// Make everything applied so far survive a crash. Called before the
    /// Raft log is compacted past those entries.
    fn durability_barrier(&mut self) -> Result<(), String> {
        Ok(())
    }
}

impl StateMachine for KvStore {
    fn apply(&mut self, index: LogIndex, data: &[u8]) -> Result<(), String> {
        KvStore::apply_at(self, index, data)
    }
    fn session_last_seq(&self, client: u64) -> Option<u64> {
        KvStore::session_last_seq(self, client)
    }
    fn read(&self, key: &str) -> Result<Option<String>, String> {
        Ok(self.get(key).cloned())
    }
    fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        Ok(self.snapshot())
    }
    fn restore_from_snapshot(&mut self, bytes: &[u8], _index: LogIndex) -> Result<(), String> {
        *self = KvStore::restore(bytes)?;
        Ok(())
    }
}

/// Lets callers choose an engine at runtime without a bespoke enum.
impl StateMachine for Box<dyn StateMachine> {
    fn apply(&mut self, i: u64, d: &[u8]) -> Result<(), String> {
        (**self).apply(i, d)
    }
    fn apply_batch(&mut self, e: &[(u64, &[u8])]) -> Result<(), String> {
        (**self).apply_batch(e)
    }
    fn read(&self, k: &str) -> Result<Option<String>, String> {
        (**self).read(k)
    }
    fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        (**self).snapshot_bytes()
    }
    fn restore_from_snapshot(&mut self, b: &[u8], i: u64) -> Result<(), String> {
        (**self).restore_from_snapshot(b, i)
    }
    fn applied_index(&self) -> u64 {
        (**self).applied_index()
    }
    fn durability_barrier(&mut self) -> Result<(), String> {
        (**self).durability_barrier()
    }
    fn session_last_seq(&self, client: u64) -> Option<u64> {
        (**self).session_last_seq(client)
    }
}
