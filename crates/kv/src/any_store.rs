//! A runtime choice between the baseline and the LSM engine, so the server
//! loop does not need to be generic over the state machine.

use raft_core::LogIndex;

use crate::lsm_store::LsmStore;
use crate::state_machine::StateMachine;
use crate::store::KvStore;

pub enum AnyStore {
    Mem(KvStore),
    Lsm(Box<LsmStore>),
}

impl StateMachine for AnyStore {
    fn apply(&mut self, index: LogIndex, data: &[u8]) -> Result<(), String> {
        match self {
            AnyStore::Mem(s) => StateMachine::apply(s, index, data),
            AnyStore::Lsm(s) => s.apply(index, data),
        }
    }
    fn apply_batch(&mut self, entries: &[(LogIndex, &[u8])]) -> Result<(), String> {
        match self {
            AnyStore::Mem(s) => s.apply_batch(entries),
            AnyStore::Lsm(s) => s.apply_batch(entries),
        }
    }
    fn read(&self, key: &str) -> Result<Option<String>, String> {
        match self {
            AnyStore::Mem(s) => s.read(key),
            AnyStore::Lsm(s) => s.read(key),
        }
    }
    fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        match self {
            AnyStore::Mem(s) => s.snapshot_bytes(),
            AnyStore::Lsm(s) => s.snapshot_bytes(),
        }
    }
    fn restore_from_snapshot(&mut self, bytes: &[u8], index: LogIndex) -> Result<(), String> {
        match self {
            AnyStore::Mem(s) => s.restore_from_snapshot(bytes, index),
            AnyStore::Lsm(s) => s.restore_from_snapshot(bytes, index),
        }
    }
    fn applied_index(&self) -> LogIndex {
        match self {
            AnyStore::Mem(s) => s.applied_index(),
            AnyStore::Lsm(s) => s.applied_index(),
        }
    }
    fn durability_barrier(&mut self) -> Result<(), String> {
        match self {
            AnyStore::Mem(s) => s.durability_barrier(),
            AnyStore::Lsm(s) => s.durability_barrier(),
        }
    }
    // Must be forwarded explicitly: the trait default is `None`, which would
    // make every retried write look like an unknown outcome.
    fn session_last_seq(&self, client: u64) -> Option<u64> {
        match self {
            AnyStore::Mem(s) => StateMachine::session_last_seq(s, client),
            AnyStore::Lsm(s) => s.session_last_seq(client),
        }
    }
}
