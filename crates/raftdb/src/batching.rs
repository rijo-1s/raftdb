//! Bounded client-write batching ("group commit") for the leader.
//!
//! Client writes are queued here instead of being proposed one at a time.
//! A queued batch is handed to `RaftNode::propose_batch`, which persists all
//! of its entries with **one** WAL fsync and replicates them in the next
//! AppendEntries - so N concurrent writes cost one fsync and one round of
//! RPCs instead of N.
//!
//! This module is deliberately free of tokio, gRPC and Raft: it is a small
//! state machine over "tickets" (anything that can say whether its client
//! is still there), which makes every rule below directly unit-testable.
//!
//! # Rules
//!
//! * **Flush triggers.** A batch flushes immediately when it reaches
//!   `max_batch_entries` or `max_batch_bytes`; otherwise once the oldest
//!   queued write has waited `max_batch_delay`. With a zero delay the event
//!   loop flushes as soon as it has drained whatever is already queued, so
//!   an idle server adds no latency and a busy one batches naturally.
//! * **Bounded memory.** At most `max_pending_writes` writes may be queued
//!   or in flight (proposed but not yet applied); further writes are
//!   rejected immediately with an explicit overload error - never queued
//!   without bound. A single command larger than `max_command_bytes` is
//!   rejected outright.
//! * **Acknowledgement.** A ticket is only ever completed with success by
//!   [`WriteQueue::resolve_applied`], i.e. after its entry is committed
//!   (durable on a quorum) *and* applied, and in log-index order.
//! * **Failure outcomes are exact.** A write that was never proposed (still
//!   queued, or its client vanished) fails *definitively*. A write that was
//!   proposed but not yet applied when leadership was lost, or whose
//!   `write_timeout` elapsed, fails with an *outcome-unknown* error: the
//!   entry may still commit. Retrying such a write without a client session
//!   can apply it twice (see the README).
//! * **Not transactional.** A batch is a throughput optimization only.
//!   Its entries commit and apply individually and in order; a leadership
//!   change can commit a prefix of a batch.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use raft_core::LogIndex;
use tokio::sync::oneshot;

/// Prefix of the error message returned when a write is shed because the
/// server is at its in-flight limit. Clients should back off and retry.
pub const OVERLOADED_PREFIX: &str = "server overloaded";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchConfig {
    /// Flush when this many writes are queued. `1` disables batching.
    pub max_batch_entries: usize,
    /// Flush when queued command bytes reach this. A single larger command
    /// still goes out, alone.
    pub max_batch_bytes: usize,
    /// Longest the oldest queued write waits for company. Zero = flush as
    /// soon as the queue has been drained.
    pub max_batch_delay: Duration,
    /// Queued + in-flight write cap (backpressure).
    pub max_pending_writes: usize,
    /// Largest single encoded command accepted.
    pub max_command_bytes: usize,
    /// How long a proposed write may wait to be applied before the client
    /// is told the outcome is unknown.
    pub write_timeout: Duration,
    /// Per-AppendEntries bounds (see `RaftNode::set_replication_limits`).
    pub max_append_entries: usize,
    pub max_append_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_entries: 256,
            max_batch_bytes: 1024 * 1024,
            max_batch_delay: Duration::ZERO,
            max_pending_writes: 8192,
            max_command_bytes: 1024 * 1024,
            write_timeout: Duration::from_secs(10),
            max_append_entries: raft_core::DEFAULT_MAX_APPEND_ENTRIES,
            max_append_bytes: raft_core::DEFAULT_MAX_APPEND_BYTES,
        }
    }
}

impl BatchConfig {
    /// One write per fsync and per AppendEntries: the pre-batching behavior.
    #[cfg(test)]
    pub fn unbatched() -> Self {
        Self {
            max_batch_entries: 1,
            ..Self::default()
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let positive = [
            ("max-batch-entries", self.max_batch_entries),
            ("max-batch-bytes", self.max_batch_bytes),
            ("max-pending-writes", self.max_pending_writes),
            ("max-command-bytes", self.max_command_bytes),
            ("max-append-entries", self.max_append_entries),
            ("max-append-bytes", self.max_append_bytes),
        ];
        for (name, v) in positive {
            if v == 0 {
                return Err(format!("--{name} must be at least 1"));
            }
        }
        if self.write_timeout.is_zero() {
            return Err("--write-timeout-ms must be at least 1".to_string());
        }
        if self.max_batch_entries > self.max_pending_writes {
            return Err(
                "--max-batch-entries cannot exceed --max-pending-writes (a batch could never fill)"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Something a write can be acknowledged through. `is_closed` reports that
/// the client gave up (disconnected or timed out) so the write can be
/// dropped *before* it is proposed.
pub trait Ticket {
    fn is_closed(&self) -> bool;
}

impl<T> Ticket for oneshot::Sender<T> {
    fn is_closed(&self) -> bool {
        oneshot::Sender::is_closed(self)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Reject {
    /// At `max_pending_writes`.
    Overloaded,
    /// Command exceeds `max_command_bytes`.
    TooLarge { len: usize, max: usize },
}

struct Queued<T> {
    cmd: Vec<u8>,
    ticket: T,
}

struct InFlight<T> {
    ticket: T,
    deadline: Instant,
}

/// A batch ready to propose.
pub struct Batch<T> {
    pub commands: Vec<Vec<u8>>,
    pub tickets: Vec<T>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct QueueStats {
    pub dropped_disconnected: u64,
}

pub struct WriteQueue<T: Ticket> {
    cfg: BatchConfig,
    queue: Vec<Queued<T>>,
    queued_bytes: usize,
    /// When the oldest queued write arrived.
    oldest: Option<Instant>,
    inflight: BTreeMap<LogIndex, InFlight<T>>,
    pub stats: QueueStats,
}

impl<T: Ticket> WriteQueue<T> {
    pub fn new(cfg: BatchConfig) -> Self {
        Self {
            cfg,
            queue: Vec::new(),
            queued_bytes: 0,
            oldest: None,
            inflight: BTreeMap::new(),
            stats: QueueStats::default(),
        }
    }

    pub fn config(&self) -> &BatchConfig {
        &self.cfg
    }

    /// Queued plus in-flight writes.
    pub fn pending(&self) -> usize {
        self.queue.len() + self.inflight.len()
    }

    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    #[cfg(test)]
    pub fn in_flight(&self) -> usize {
        self.inflight.len()
    }

    /// Queue a write. On success returns `true` if a size limit says the
    /// batch must flush *now*. On rejection the ticket is handed back so
    /// the caller can answer the client.
    pub fn submit(&mut self, cmd: Vec<u8>, ticket: T, now: Instant) -> Result<bool, (Reject, T)> {
        if cmd.len() > self.cfg.max_command_bytes {
            let len = cmd.len();
            return Err((
                Reject::TooLarge {
                    len,
                    max: self.cfg.max_command_bytes,
                },
                ticket,
            ));
        }
        if self.pending() >= self.cfg.max_pending_writes {
            return Err((Reject::Overloaded, ticket));
        }
        self.queued_bytes += cmd.len();
        self.queue.push(Queued { cmd, ticket });
        self.oldest.get_or_insert(now);
        Ok(self.size_limit_reached())
    }

    /// True once the queue holds a full batch by count or bytes.
    pub fn size_limit_reached(&self) -> bool {
        self.queue.len() >= self.cfg.max_batch_entries
            || self.queued_bytes >= self.cfg.max_batch_bytes
    }

    /// Instant at which the delay trigger fires for the current queue.
    pub fn flush_deadline(&self) -> Option<Instant> {
        self.oldest.map(|t| t + self.cfg.max_batch_delay)
    }

    /// True if a batch should be flushed at `now` (size limit reached, or
    /// the oldest write has waited out the delay).
    pub fn should_flush(&self, now: Instant) -> bool {
        if self.queue.is_empty() {
            return false;
        }
        self.size_limit_reached() || self.flush_deadline().is_some_and(|d| now >= d)
    }

    /// Remove up to one batch's worth of writes (at most `max_batch_entries`
    /// and, beyond the first, at most `max_batch_bytes`), oldest first.
    /// Writes whose client has already gone are dropped here, never
    /// proposed. Anything over the limits stays queued, in order.
    pub fn take_batch(&mut self, now: Instant) -> Batch<T> {
        let mut commands = Vec::new();
        let mut tickets = Vec::new();
        let mut bytes = 0usize;
        let mut taken = 0usize;
        let all = std::mem::take(&mut self.queue);
        let mut rest = Vec::new();
        for q in all {
            if q.ticket.is_closed() {
                self.stats.dropped_disconnected += 1;
                self.queued_bytes -= q.cmd.len();
                continue;
            }
            let fits = taken < self.cfg.max_batch_entries
                && (taken == 0 || bytes + q.cmd.len() <= self.cfg.max_batch_bytes);
            if fits {
                bytes += q.cmd.len();
                taken += 1;
                self.queued_bytes -= q.cmd.len();
                commands.push(q.cmd);
                tickets.push(q.ticket);
            } else {
                rest.push(q);
            }
        }
        self.queue = rest;
        // Whatever is left has been waiting at least as long as `now`.
        self.oldest = if self.queue.is_empty() {
            None
        } else {
            Some(now)
        };
        Batch { commands, tickets }
    }

    /// Record that `tickets` were proposed as consecutive entries starting
    /// at `first_index`.
    pub fn register_in_flight(&mut self, first_index: LogIndex, tickets: Vec<T>, now: Instant) {
        let deadline = now + self.cfg.write_timeout;
        for (i, ticket) in tickets.into_iter().enumerate() {
            self.inflight
                .insert(first_index + i as LogIndex, InFlight { ticket, deadline });
        }
    }

    /// Register a single proposed entry (membership changes use this).
    pub fn register_one(&mut self, index: LogIndex, ticket: T, now: Instant) {
        self.register_in_flight(index, vec![ticket], now);
    }

    /// Everything at or below `applied` has been committed and applied:
    /// hand back those tickets, in log order, for a success reply.
    pub fn resolve_applied(&mut self, applied: LogIndex) -> Vec<T> {
        let still = self.inflight.split_off(&(applied + 1));
        let done = std::mem::replace(&mut self.inflight, still);
        done.into_values().map(|f| f.ticket).collect()
    }

    /// In-flight writes whose timeout has passed. They are removed and must
    /// be answered with an outcome-unknown error.
    pub fn expire(&mut self, now: Instant) -> Vec<T> {
        let expired: Vec<LogIndex> = self
            .inflight
            .iter()
            .filter(|(_, f)| now >= f.deadline)
            .map(|(&i, _)| i)
            .collect();
        expired
            .into_iter()
            .filter_map(|i| self.inflight.remove(&i))
            .map(|f| f.ticket)
            .collect()
    }

    /// Leadership lost (or shutdown): empty everything. Returns
    /// `(never_proposed, outcome_unknown)`.
    pub fn drain_all(&mut self) -> (Vec<T>, Vec<T>) {
        let queued: Vec<T> = std::mem::take(&mut self.queue)
            .into_iter()
            .map(|q| q.ticket)
            .collect();
        self.queued_bytes = 0;
        self.oldest = None;
        let inflight: Vec<T> = std::mem::take(&mut self.inflight)
            .into_values()
            .map(|f| f.ticket)
            .collect();
        (queued, inflight)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::rc::Rc;

    /// Test ticket: an id plus a flag the "client" can set when it leaves.
    #[derive(Clone)]
    struct T {
        id: u32,
        gone: Rc<Cell<bool>>,
    }
    impl Ticket for T {
        fn is_closed(&self) -> bool {
            self.gone.get()
        }
    }
    fn t(id: u32) -> T {
        T {
            id,
            gone: Rc::new(Cell::new(false)),
        }
    }
    fn ids(v: &[T]) -> Vec<u32> {
        v.iter().map(|t| t.id).collect()
    }

    fn cfg(entries: usize, bytes: usize, delay_ms: u64) -> BatchConfig {
        BatchConfig {
            max_batch_entries: entries,
            max_batch_bytes: bytes,
            max_batch_delay: Duration::from_millis(delay_ms),
            max_pending_writes: 100,
            max_command_bytes: 1000,
            write_timeout: Duration::from_secs(5),
            ..BatchConfig::default()
        }
    }

    #[test]
    fn flushes_immediately_when_entry_limit_reached() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(3, 10_000, 1000));
        assert_eq!(q.submit(vec![0; 10], t(1), now).map_err(|e| e.0), Ok(false));
        assert_eq!(q.submit(vec![0; 10], t(2), now).map_err(|e| e.0), Ok(false));
        assert!(!q.should_flush(now), "delay not elapsed, limit not reached");
        assert_eq!(q.submit(vec![0; 10], t(3), now).map_err(|e| e.0), Ok(true));
        assert!(q.should_flush(now));
        let b = q.take_batch(now);
        assert_eq!(ids(&b.tickets), vec![1, 2, 3]);
        assert_eq!(q.queued(), 0);
    }

    #[test]
    fn flushes_immediately_when_byte_limit_reached() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(100, 250, 1000));
        assert_eq!(
            q.submit(vec![0; 100], t(1), now).map_err(|e| e.0),
            Ok(false)
        );
        assert_eq!(
            q.submit(vec![0; 100], t(2), now).map_err(|e| e.0),
            Ok(false)
        );
        assert_eq!(q.submit(vec![0; 100], t(3), now).map_err(|e| e.0), Ok(true));
        // byte cap: first two (200B) fit, third would make 300 > 250
        let b = q.take_batch(now);
        assert_eq!(ids(&b.tickets), vec![1, 2]);
        assert_eq!(q.queued(), 1, "overflow stays queued, in order");
        let b2 = q.take_batch(now);
        assert_eq!(ids(&b2.tickets), vec![3]);
    }

    #[test]
    fn delay_trigger_fires_for_the_oldest_write() {
        let t0 = Instant::now();
        let mut q = WriteQueue::new(cfg(100, 10_000, 5));
        q.submit(vec![1], t(1), t0).ok().unwrap();
        assert_eq!(q.flush_deadline(), Some(t0 + Duration::from_millis(5)));
        // A later arrival does not push the deadline out.
        q.submit(vec![2], t(2), t0 + Duration::from_millis(3))
            .ok()
            .unwrap();
        assert!(!q.should_flush(t0 + Duration::from_millis(4)));
        assert!(q.should_flush(t0 + Duration::from_millis(5)));
    }

    #[test]
    fn zero_delay_flushes_as_soon_as_anything_is_queued() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(100, 10_000, 0));
        assert!(!q.should_flush(now));
        q.submit(vec![1], t(1), now).ok().unwrap();
        assert!(q.should_flush(now));
    }

    #[test]
    fn unbatched_config_flushes_every_write_alone() {
        let now = Instant::now();
        let mut q = WriteQueue::new(BatchConfig::unbatched());
        for i in 0..4 {
            assert_eq!(q.submit(vec![0], t(i), now).map_err(|e| e.0), Ok(true));
            assert_eq!(q.take_batch(now).tickets.len(), 1);
        }
    }

    #[test]
    fn a_single_oversized_batch_command_still_goes_out_alone() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(10, 50, 0));
        q.submit(vec![0; 400], t(1), now).ok().unwrap();
        q.submit(vec![0; 10], t(2), now).ok().unwrap();
        assert_eq!(ids(&q.take_batch(now).tickets), vec![1]);
        assert_eq!(ids(&q.take_batch(now).tickets), vec![2]);
    }

    #[test]
    fn backpressure_rejects_instead_of_growing_without_bound() {
        let now = Instant::now();
        let mut c = cfg(2, 10_000, 1000);
        c.max_pending_writes = 4;
        let mut q = WriteQueue::new(c);
        for i in 0..4 {
            assert!(q.submit(vec![0], t(i), now).is_ok());
        }
        let (why, ticket) = q.submit(vec![0], t(99), now).err().unwrap();
        assert_eq!(why, Reject::Overloaded);
        assert_eq!(ticket.id, 99, "rejected ticket is returned to be answered");
        assert_eq!(q.pending(), 4);

        // In-flight writes count against the cap too.
        let b = q.take_batch(now);
        q.register_in_flight(10, b.tickets, now);
        assert_eq!(q.pending(), 4);
        assert!(q.submit(vec![0], t(100), now).is_err());
        // Applying frees capacity.
        q.resolve_applied(11);
        assert!(q.submit(vec![0], t(101), now).is_ok());
    }

    #[test]
    fn oversized_command_is_rejected() {
        let mut q = WriteQueue::new(cfg(10, 10_000, 0));
        let (why, _) = q.submit(vec![0; 1001], t(1), Instant::now()).err().unwrap();
        assert_eq!(
            why,
            Reject::TooLarge {
                len: 1001,
                max: 1000
            }
        );
        assert_eq!(q.pending(), 0);
    }

    #[test]
    fn writes_are_acknowledged_only_when_applied_and_in_index_order() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(10, 10_000, 0));
        for i in 1..=5 {
            q.submit(vec![0], t(i), now).ok().unwrap();
        }
        let b = q.take_batch(now);
        q.register_in_flight(7, b.tickets, now); // indices 7..=11
        assert!(q.resolve_applied(6).is_empty(), "nothing applied yet");
        assert_eq!(ids(&q.resolve_applied(8)), vec![1, 2]);
        assert_eq!(
            ids(&q.resolve_applied(8)),
            Vec::<u32>::new(),
            "no double ack"
        );
        assert_eq!(ids(&q.resolve_applied(100)), vec![3, 4, 5]);
        assert_eq!(q.pending(), 0);
    }

    #[test]
    fn disconnected_clients_are_dropped_before_proposal() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(10, 10_000, 0));
        let gone = t(2);
        q.submit(vec![1], t(1), now).ok().unwrap();
        q.submit(vec![2], gone.clone(), now).ok().unwrap();
        q.submit(vec![3], t(3), now).ok().unwrap();
        gone.gone.set(true); // client hangs up while queued
        let b = q.take_batch(now);
        assert_eq!(ids(&b.tickets), vec![1, 3]);
        assert_eq!(
            b.commands,
            vec![vec![1], vec![3]],
            "its command is not proposed"
        );
        assert_eq!(q.stats.dropped_disconnected, 1);
    }

    #[test]
    fn a_client_that_leaves_after_proposal_does_not_unpropose_the_write() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(10, 10_000, 0));
        let c = t(1);
        q.submit(vec![1], c.clone(), now).ok().unwrap();
        let b = q.take_batch(now);
        q.register_in_flight(5, b.tickets, now);
        c.gone.set(true);
        // Still tracked; will be resolved (the send just fails harmlessly).
        assert_eq!(q.in_flight(), 1);
        assert_eq!(ids(&q.resolve_applied(5)), vec![1]);
    }

    #[test]
    fn timeouts_only_expire_in_flight_writes_past_their_deadline() {
        let t0 = Instant::now();
        let mut q = WriteQueue::new(cfg(10, 10_000, 0));
        q.submit(vec![1], t(1), t0).ok().unwrap();
        let b = q.take_batch(t0);
        q.register_in_flight(1, b.tickets, t0);
        assert!(q.expire(t0 + Duration::from_secs(4)).is_empty());
        let late = q.expire(t0 + Duration::from_secs(5));
        assert_eq!(ids(&late), vec![1]);
        assert_eq!(q.in_flight(), 0);
        // A late apply of the expired write must not ack anyone twice.
        assert!(q.resolve_applied(1).is_empty());
    }

    #[test]
    fn leadership_loss_separates_never_proposed_from_outcome_unknown() {
        let now = Instant::now();
        let mut q = WriteQueue::new(cfg(2, 10_000, 1000));
        for i in 1..=3 {
            q.submit(vec![0], t(i), now).ok().unwrap();
        }
        let b = q.take_batch(now); // takes 1,2; 3 remains queued
        q.register_in_flight(1, b.tickets, now);
        let (never, unknown) = q.drain_all();
        assert_eq!(ids(&never), vec![3], "definitely not proposed");
        assert_eq!(ids(&unknown), vec![1, 2], "may still commit");
        assert_eq!(q.pending(), 0);
        assert!(q.flush_deadline().is_none());
    }

    #[test]
    fn config_validation_rejects_nonsense() {
        assert!(BatchConfig::default().validate().is_ok());
        let zero_entries = BatchConfig {
            max_batch_entries: 0,
            ..BatchConfig::default()
        };
        assert!(zero_entries.validate().is_err());
        let batch_larger_than_cap = BatchConfig {
            max_batch_entries: 10,
            max_pending_writes: 5,
            ..BatchConfig::default()
        };
        assert!(batch_larger_than_cap.validate().is_err());
        let zero_timeout = BatchConfig {
            write_timeout: Duration::ZERO,
            ..BatchConfig::default()
        };
        assert!(zero_timeout.validate().is_err());
    }
}
