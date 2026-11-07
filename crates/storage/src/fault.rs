//! Deterministic fault injection for the durability layer.
//!
//! Every filesystem operation that matters for crash safety (write, fsync,
//! rename, directory fsync) goes through the wrappers in `durable.rs`, and
//! each of those consults a [`FaultPoint`] here first. A test arms a
//! one-shot [`FaultPlan`]; when the matching point is hit, the wrapper
//! fails in the requested way, leaving on disk exactly what a process kill
//! at that instant would leave behind.
//!
//! Plans are **thread-local**, so tests running in parallel never see each
//! other's faults. That also means they only fire on the thread that armed
//! them - which is what the synchronous storage/KV tests want.
//!
//! The whole mechanism is compiled only for `cfg(test)` or the
//! `fault-injection` feature. In a normal build [`check`] is an
//! always-`Ok` inline no-op.

/// A place in the durability code where a crash or I/O error can be
/// injected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultPoint {
    /// fsync of the parent directory after creating a brand-new WAL file.
    WalCreateDirSync,
    /// Appending frames to the live WAL (single entry or a batch).
    WalAppendWrite,
    /// The fsync that makes an append durable.
    WalAppendSync,
    /// Writing the replacement WAL image to its temporary file.
    WalRewriteTmpWrite,
    WalRewriteTmpSync,
    /// Atomically replacing the live WAL with the temporary file.
    WalRewriteRename,
    WalRewriteDirSync,
    /// Writing the replacement snapshot to its temporary file.
    SnapshotTmpWrite,
    SnapshotTmpSync,
    SnapshotRename,
    SnapshotDirSync,
    /// Same four steps for the HardState file.
    HardStateTmpWrite,
    HardStateTmpSync,
    HardStateRename,
    HardStateDirSync,
    /// Truncating a torn tail during recovery, and fsyncing that.
    WalRecoverTruncate,
    WalRecoverSync,
    // ---- LSM engine persistence boundaries ----
    /// fsync of the directory after creating a new memtable WAL file.
    LsmWalCreateDirSync,
    /// Appending a record to the memtable WAL.
    LsmWalAppendWrite,
    /// The fsync that makes a WAL append durable (per write, or on demand).
    LsmWalAppendSync,
    /// fsync of the old WAL when it is rotated out.
    LsmWalRotateSync,
    /// Writing / syncing / renaming / dir-syncing a new SSTable (flush,
    /// compaction output, or snapshot ingest).
    LsmSstTmpWrite,
    LsmSstTmpSync,
    LsmSstRename,
    LsmSstDirSync,
    /// Publishing a new MANIFEST (the commit point of flush, compaction and
    /// snapshot ingest).
    LsmManifestTmpWrite,
    LsmManifestTmpSync,
    LsmManifestRename,
    LsmManifestDirSync,
    /// Truncating a torn WAL tail during recovery.
    LsmWalRecoverTruncate,
    LsmWalRecoverSync,
    /// Removing a file during recovery clean-up (temp file, unreferenced
    /// table, retired WAL, orphan WAL). Deliberately *not* in `LSM_ALL`:
    /// it is only reachable from recovery, never from a normal op sequence.
    LsmRecoverRemove,
    /// Removing a stale temp file while opening the Raft WAL, hard state or
    /// snapshot. Deliberately *not* in `ALL`, for the same reason.
    RecoverRemoveTmp,
}

impl FaultPoint {
    /// Every LSM point, for exhaustive matrix tests.
    pub const LSM_ALL: [FaultPoint; 14] = [
        FaultPoint::LsmWalCreateDirSync,
        FaultPoint::LsmWalAppendWrite,
        FaultPoint::LsmWalAppendSync,
        FaultPoint::LsmWalRotateSync,
        FaultPoint::LsmSstTmpWrite,
        FaultPoint::LsmSstTmpSync,
        FaultPoint::LsmSstRename,
        FaultPoint::LsmSstDirSync,
        FaultPoint::LsmManifestTmpWrite,
        FaultPoint::LsmManifestTmpSync,
        FaultPoint::LsmManifestRename,
        FaultPoint::LsmManifestDirSync,
        FaultPoint::LsmWalRecoverTruncate,
        FaultPoint::LsmWalRecoverSync,
    ];

    /// Every point, for exhaustive matrix tests.
    pub const ALL: [FaultPoint; 17] = [
        FaultPoint::WalCreateDirSync,
        FaultPoint::WalAppendWrite,
        FaultPoint::WalAppendSync,
        FaultPoint::WalRewriteTmpWrite,
        FaultPoint::WalRewriteTmpSync,
        FaultPoint::WalRewriteRename,
        FaultPoint::WalRewriteDirSync,
        FaultPoint::SnapshotTmpWrite,
        FaultPoint::SnapshotTmpSync,
        FaultPoint::SnapshotRename,
        FaultPoint::SnapshotDirSync,
        FaultPoint::HardStateTmpWrite,
        FaultPoint::HardStateTmpSync,
        FaultPoint::HardStateRename,
        FaultPoint::HardStateDirSync,
        FaultPoint::WalRecoverTruncate,
        FaultPoint::WalRecoverSync,
    ];
}

/// What happens when an armed point is reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultAction {
    /// Fail *before* the operation takes effect (crash just before the
    /// syscall).
    FailBefore,
    /// Let the operation take effect, then fail (crash just after the
    /// syscall returned, before the caller learned of it).
    FailAfter,
    /// Writes only: persist the first `n` bytes, then fail (a torn write).
    PartialWrite(usize),
}

#[derive(Debug, Clone, Copy)]
pub struct FaultPlan {
    pub point: FaultPoint,
    /// Number of hits of `point` to let through before firing (0 = first).
    pub skip: usize,
    pub action: FaultAction,
}

#[cfg(any(test, feature = "fault-injection"))]
mod imp {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::io;

    #[derive(Default)]
    struct State {
        plan: Option<FaultPlan>,
        seen: usize,
        hits: HashMap<FaultPoint, usize>,
        fired: Option<FaultPoint>,
    }

    thread_local! {
        static STATE: RefCell<State> = RefCell::new(State::default());
    }

    pub fn arm(plan: FaultPlan) {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            s.plan = Some(plan);
            s.seen = 0;
            s.fired = None;
        });
    }

    /// Disarm and reset all counters.
    pub fn clear() {
        STATE.with(|s| *s.borrow_mut() = State::default());
    }

    /// How many times `point` has been reached since the last `clear()`.
    pub fn hits(point: FaultPoint) -> usize {
        STATE.with(|s| s.borrow().hits.get(&point).copied().unwrap_or(0))
    }

    /// The point that fired, if the armed plan has triggered.
    pub fn fired() -> Option<FaultPoint> {
        STATE.with(|s| s.borrow().fired)
    }

    pub fn injected_error(point: FaultPoint) -> io::Error {
        io::Error::other(format!("injected fault at {point:?}"))
    }

    /// Consult the plan for `point`. Returns the action to take, if this is
    /// the hit the plan was waiting for.
    pub fn poll(point: FaultPoint) -> Option<FaultAction> {
        STATE.with(|s| {
            let mut s = s.borrow_mut();
            *s.hits.entry(point).or_insert(0) += 1;
            let plan = s.plan?;
            if plan.point != point {
                return None;
            }
            if s.seen < plan.skip {
                s.seen += 1;
                return None;
            }
            s.plan = None;
            s.fired = Some(point);
            Some(plan.action)
        })
    }
}

#[cfg(any(test, feature = "fault-injection"))]
pub use imp::{arm, clear, fired, hits, injected_error};

#[cfg(any(test, feature = "fault-injection"))]
pub(crate) use imp::poll;

/// No-op stand-in when injection is compiled out.
#[cfg(not(any(test, feature = "fault-injection")))]
#[inline(always)]
pub(crate) fn poll(_point: FaultPoint) -> Option<FaultAction> {
    None
}
