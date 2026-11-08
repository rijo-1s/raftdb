//! The operational events RaftDB emits, with stable names.
//!
//! Every function logs exactly one event whose `event` field is the function
//! name. Dashboards and alerts should match on `event`, never on the human
//! message. Field names are part of the contract; add fields freely, but do
//! not rename or remove them without updating `docs/observability.md`.
//!
//! | event               | level | fields                                           |
//! |---------------------|-------|--------------------------------------------------|
//! | node_started        | INFO  | node bind peers data_dir engine join             |
//! | became_leader       | INFO  | node term                                        |
//! | stepped_down        | INFO  | node term                                        |
//! | removed_from_cluster| WARN  | node                                             |
//! | readiness_changed   | INFO/WARN | node ready reason                            |
//! | slow_fsync          | WARN  | node micros threshold_micros                     |
//! | compaction_finished | INFO  | node duration_ms l0_files l1_files               |
//! | storage_failure     | ERROR | node operation error                             |
//! | node_loop_died      | ERROR | node reason                                      |
//! | peer_unreachable    | WARN  | node peer error suppressed                       |
//! | disk_space_changed  | WARN  | node protected free_bytes min_free_bytes         |
//! | compaction_skipped  | WARN  | node needed_bytes free_bytes                     |
//! | audit               | INFO  | node who role method                             |
//! | authz_reloaded      | INFO  | node admin read_write read_only nodes            |
//! | authz_reload_failed | WARN  | node error                                       |
//! | authz_node_unbound  | WARN  | node labels                                      |
//! | node_identity_mismatch | WARN | node authenticated claimed                    |
//! | tls_key_permissions | WARN  | path mode                                        |
//! | tls_cert_expiry     | INFO/WARN | node not_after_unix seconds_left             |
//! | recovery_refused    | ERROR | node path error                                  |
//! | lsm_quarantined     | WARN  | node reason moved_to                             |
//! | metrics_listening   | INFO  | node addr                                        |
//! | shutdown_started    | INFO  | node                                             |
//! | shutdown_complete   | INFO  | node                                             |
//! | panic               | ERROR | thread location panic_message (see the hook)    |
//! | diag_stall          | WARN  | node what millis (diagnostics)                     |
//! | diag_election_started | WARN | node term millis_since_leader_msg (diagnostic)  |

use std::fmt::Display;

/// Make a string safe to put in one log line in ANY format: control
/// characters (newline, carriage return, escape, ...) become visible escapes.
/// JSON output escapes them anyway; the text format does not, so without this
/// a multi-line error or a key containing `\n` could forge extra log lines.
pub fn clean(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\n' => o.push_str("\\n"),
            '\r' => o.push_str("\\r"),
            '\t' => o.push_str("\\t"),
            c if c.is_control() => o.push_str(&format!("\\u{{{:04x}}}", c as u32)),
            c => o.push(c),
        }
    }
    o
}

pub fn node_started(
    node: u64,
    bind: &str,
    peers: &[u64],
    data_dir: &str,
    engine: &str,
    join: bool,
) {
    let peers = peers
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let (bind, data_dir, engine) = (clean(bind), clean(data_dir), clean(engine));
    tracing::info!(event = "node_started", node, bind = %bind, peers = %peers, data_dir = %data_dir, engine = %engine, join, "node starting");
}

pub fn became_leader(node: u64, term: u64) {
    tracing::info!(event = "became_leader", node, term, "became leader");
}

pub fn stepped_down(node: u64, term: u64) {
    tracing::info!(
        event = "stepped_down",
        node,
        term,
        "stepped down from leader"
    );
}

pub fn removed_from_cluster(node: u64) {
    tracing::warn!(
        event = "removed_from_cluster",
        node,
        "removed from the cluster; node is inert"
    );
}

/// `/readyz` changed verdict. `reason` is the first failing check (or "ok").
pub fn readiness_changed(node: u64, ready: bool, reason: &str) {
    let reason = clean(reason);
    let reason = reason.as_str();
    if ready {
        tracing::info!(
            event = "readiness_changed",
            node,
            ready,
            reason,
            "node is ready"
        );
    } else {
        tracing::warn!(
            event = "readiness_changed",
            node,
            ready,
            reason,
            "node is not ready"
        );
    }
}

pub fn slow_fsync(node: u64, micros: u64, threshold_micros: u64) {
    tracing::warn!(
        event = "slow_fsync",
        node,
        micros,
        threshold_micros,
        "WAL fsync exceeded the slow threshold"
    );
}

pub fn compaction_finished(node: u64, duration_ms: u64, l0_files: u64, l1_files: u64) {
    tracing::info!(
        event = "compaction_finished",
        node,
        duration_ms,
        l0_files,
        l1_files,
        "compaction finished"
    );
}

pub fn storage_failure(node: u64, operation: &str, error: &dyn Display) {
    let (operation, error) = (clean(operation), clean(&error.to_string()));
    tracing::error!(event = "storage_failure", node, operation = %operation, error = %error, "storage operation failed; the node needs a restart");
}

/// An outbound RPC to `peer` failed or timed out. Rate-limited by the caller
/// (one line per peer per interval); `suppressed` is how many further failures
/// that line stands for. This is the line to look for when a cluster will not
/// form: a TLS mismatch (wrong CA, expired or missing certificate) shows up
/// here as a transport error.
pub fn peer_unreachable(node: u64, peer: u64, error: &str, suppressed: u64) {
    let error = clean(error);
    tracing::warn!(event = "peer_unreachable", node, peer, error = %error, suppressed, "outbound RPC to a peer failed");
}

/// The data volume crossed the free-space floor (`--min-free-mb`). WARN when
/// writes start being refused; INFO when they are accepted again.
pub fn disk_space_changed(node: u64, protected: bool, free_bytes: u64, min_free_bytes: u64) {
    if protected {
        tracing::warn!(
            event = "disk_space_changed",
            node,
            protected,
            free_bytes,
            min_free_bytes,
            "data volume is nearly full: client writes are rejected until space is freed"
        );
    } else {
        tracing::info!(
            event = "disk_space_changed",
            node,
            protected,
            free_bytes,
            min_free_bytes,
            "data volume has room again: client writes are accepted"
        );
    }
}

/// A log compaction was deferred because the data volume has no room for its
/// snapshot. It is retried automatically; free some space.
pub fn compaction_skipped(node: u64, needed_bytes: u64, free_bytes: u64) {
    tracing::warn!(
        event = "compaction_skipped",
        node,
        needed_bytes,
        free_bytes,
        "log compaction deferred: not enough free space for the snapshot"
    );
}

/// The node certificate\'s expiry, logged at startup. WARN when it expires
/// within 30 days or already has.
pub fn tls_cert_expiry(node: u64, not_after_unix: i64, seconds_left: i64) {
    if seconds_left < 30 * 86_400 {
        tracing::warn!(
            event = "tls_cert_expiry",
            node,
            not_after_unix,
            seconds_left,
            "the node certificate expires soon (or has expired): rotate it"
        );
    } else {
        tracing::info!(
            event = "tls_cert_expiry",
            node,
            not_after_unix,
            seconds_left,
            "node certificate expiry"
        );
    }
}

/// The certificate\'s expiry could not be determined (the node still starts).
pub fn tls_cert_expiry_unknown(node: u64, reason: &str) {
    let reason = clean(reason);
    tracing::warn!(event = "tls_cert_expiry_unknown", node, reason = %reason, "could not read the certificate expiry; it is not being monitored");
}

/// An allowed request that is worth an audit trail: every admin operation, and
/// (with `--audit-all`) every client request.
pub fn audit(node: u64, who: &str, role: &str, method: &str) {
    let (who, method) = (clean(who), clean(method));
    tracing::info!(event = "audit", node, who = %who, role, method = %method, "request allowed");
}

pub fn authz_reloaded(node: u64, admin: usize, read_write: usize, read_only: usize, nodes: usize) {
    tracing::info!(
        event = "authz_reloaded",
        node,
        admin,
        read_write,
        read_only,
        nodes,
        "authorization file reloaded"
    );
}

/// The reload failed; the previous policy stays in force.
pub fn authz_reload_failed(node: u64, error: &str) {
    let error = clean(error);
    tracing::warn!(event = "authz_reload_failed", node, error = %error, "authorization file NOT reloaded; the previous policy stays in force");
}

/// `node` entries whose label does not name a node id, so their certificates
/// are not bound to one (they may speak as any node).
pub fn authz_node_unbound(node: u64, labels: &str) {
    let labels = clean(labels);
    tracing::warn!(event = "authz_node_unbound", node, labels = %labels, "node certificates not bound to a node id: label them node1, node2, ...");
}

/// A node certificate claimed to be a different node.
pub fn node_identity_mismatch(node: u64, authenticated: u64, claimed: u64) {
    tracing::warn!(
        event = "node_identity_mismatch",
        node,
        authenticated,
        claimed,
        "a node certificate tried to act as another node"
    );
}

pub fn tls_key_permissions(path: &str, mode: u32) {
    let path = clean(path);
    tracing::warn!(event = "tls_key_permissions", path = %path, mode = format!("{mode:o}"), "the TLS private key is readable by group or others; chmod 600 it");
}

pub fn node_loop_died(node: u64, reason: &str) {
    let reason = clean(reason);
    tracing::error!(event = "node_loop_died", node, reason = %reason, "the node event loop terminated; exiting so a supervisor can restart the node into recovery");
}

pub fn recovery_refused(node: u64, path: &str, error: &dyn Display) {
    let (path, error) = (clean(path), clean(&error.to_string()));
    tracing::error!(event = "recovery_refused", node, path = %path, error = %error, "refusing to start: persisted state is corrupt or inconsistent (nothing was modified)");
}

/// Sending a snapshot to a peer did not complete (timeout, message-size limit, connection error).
/// The leader retries; if this repeats for the same peer the peer cannot catch up.
pub fn snapshot_send_failed(node: u64, peer: u64, bytes: usize, elapsed_ms: u64, error: &str) {
    let error = clean(error);
    tracing::warn!(event = "snapshot_send_failed", node, peer, bytes, elapsed_ms, error = %error, "sending a snapshot to a peer did not complete; it will be retried");
}

/// Authorization is on: only certificates listed in the authorization file may call the server.
pub fn authz_enabled(node: u64, admin: usize, read_write: usize, read_only: usize, nodes: usize) {
    tracing::info!(
        event = "authz_enabled",
        node,
        admin,
        read_write,
        read_only,
        nodes,
        "authorization is on: only listed certificates may call this node"
    );
}

/// A request was refused by authorization. Rate-limited to one line per second; `suppressed` is how
/// many further refusals were left out since the previous line.
pub fn authz_denied(node: u64, who: &str, method: &str, required: &str, suppressed: u64) {
    let (who, method) = (clean(who), clean(method));
    tracing::warn!(event = "authz_denied", node, who = %who, method = %method, required = %required, suppressed, "request refused: this certificate is not allowed to call this method");
}

/// The LSM engine was found corrupt, moved aside, and is being rebuilt from
/// the Raft log/snapshot. Worth an alert: it means real on-disk damage.
pub fn lsm_quarantined(node: u64, reason: &dyn Display, moved_to: &str) {
    let (reason, moved_to) = (clean(&reason.to_string()), clean(moved_to));
    tracing::warn!(event = "lsm_quarantined", node, reason = %reason, moved_to = %moved_to, "LSM engine was corrupt; moved aside and rebuilding from the Raft log");
}

pub fn metrics_listening(node: u64, addr: &str) {
    let addr = clean(addr);
    tracing::info!(event = "metrics_listening", node, addr = %addr, "serving /metrics, /healthz and /readyz");
}

pub fn shutdown_started(node: u64) {
    tracing::info!(event = "shutdown_started", node, "shutting down");
}

pub fn shutdown_complete(node: u64) {
    tracing::info!(event = "shutdown_complete", node, "shut down cleanly");
}

/// Diagnostic: a node-loop handler ran, or a timer tick arrived,
/// `millis` later than it should have.
pub fn diag_stall(node: u64, what: &str, millis: u64) {
    tracing::warn!(event = "diag_stall", node, what = %what, millis, "node loop stall");
}

/// Diagnostic: this node's election timer fired and it became a
/// candidate; `millis_since_leader_msg` is how long since it last received an
/// AppendEntries from any leader.
pub fn diag_election_started(node: u64, term: u64, millis_since_leader_msg: u64) {
    tracing::warn!(
        event = "diag_election_started",
        node,
        term,
        millis_since_leader_msg,
        "election timer fired"
    );
}
