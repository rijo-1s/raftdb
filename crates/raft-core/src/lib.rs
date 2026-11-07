//! Pure, I/O-free Raft consensus core.
//!
//! This crate implements only the algorithm from the Raft paper
//! ("In Search of an Understandable Consensus Algorithm", Ongaro & Ousterhout):
//! leader election and log replication. It deliberately knows nothing about
//! networking or disk persistence - `RaftNode` is a state machine driven by
//! `tick()` (time passing) and `step()` (an inbound message arriving), and it
//! produces `Envelope`s (outbound messages) as output.
//!
//! This separation mirrors etcd's raft package and tikv/raft-rs, and it's
//! what makes the core testable deterministically in-process, with no
//! sockets, threads, or timing flakiness - see `tests/` for a simulated
//! multi-node cluster driven entirely by hand.
//!
//! Networking (Phase transport), a WAL (Phase storage), and a KV state
//! machine (Phase kv) get layered on top of this in later crates without
//! changing anything in here.

mod log;
mod node;
mod types;

pub use log::{LogStore, MemLog};
pub use node::{Committed, RaftNode, DEFAULT_MAX_APPEND_BYTES, DEFAULT_MAX_APPEND_ENTRIES};
pub use types::{EntryKind, Envelope, LogEntry, LogIndex, Message, NodeId, Role, Term};
