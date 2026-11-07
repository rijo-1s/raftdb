//! Durable storage for `raft-core`: a crash-safe write-ahead log with
//! atomic snapshot publication, a checksummed HardState file, group-commit
//! batch appends, and a deterministic fault-injection layer for tests.

mod durable;
pub mod fault;
pub mod lsm;
mod wal;
mod wal_log;

pub use wal::{Wal, WalError};
pub use wal_log::WalLog;
