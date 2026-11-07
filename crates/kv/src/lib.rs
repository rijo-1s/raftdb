//! The state machine layer: a simple KV store, plus `KvNode`, which glues
//! it to a `raft_core::RaftNode` (the "apply loop" - take what Raft just
//! committed or snapshotted, and do something with it).
//!
//! Two interchangeable engines implement [`StateMachine`]: the in-memory
//! `KvStore` (the correctness baseline) and the durable LSM-backed
//! `LsmStore`.

mod any_store;
pub mod backup;
mod lsm_store;
mod node;
pub mod session;
mod state_machine;
mod store;

pub use any_store::AnyStore;
pub use lsm_store::{LsmStore, LsmStoreOptions, OpenOutcome};
pub use node::KvNode;
pub use state_machine::StateMachine;
pub use store::{Command, KvStore};
