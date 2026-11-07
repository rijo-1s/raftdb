//! Optional role-based authorization, on top of mutual TLS.
//!
//! **Off by default.** Without `--authz-file` nothing here is used and requests are served exactly
//! as before. With it, every gRPC request must come from a client certificate that is listed in
//! the file, and the role listed for it must allow the method being called:
//!
//! | role         | may call                                                         |
//! |--------------|------------------------------------------------------------------|
//! | `read-only`  | `Get`                                                            |
//! | `read-write` | `Get`, `Put`, `Delete`                                           |
//! | `admin`      | all client methods, including `AddNode`, `RemoveNode`, `Backup`  |
//! | `node`       | the Raft transport only (`RequestVote`, `AppendEntries`, `InstallSnapshot`) |
//!
//! The roles are strictly separate in one respect: a client certificate (even an admin) can never
//! speak the Raft protocol, and a node certificate can never call the client API. Without that,
//! any holder of a certificate signed by the CA could send Raft messages (for example a vote
//! request with a huge term) and disrupt the cluster.
//!
//! Identity is the SHA-256 fingerprint of the certificate the client presented, taken after the
//! TLS handshake has verified it against `--tls-ca`. Fingerprints, not names, are used so that no
//! X.509 name parsing is involved. Methods that are not in [`CLASSIFIED`] need `admin` (deny by
//! default), and a test fails if an RPC in the proto file is missing from the table.
//!
//! File format, one entry per line (`#` starts a comment):
//!
//! ```text
//! # role        sha256-fingerprint (hex, colons optional)                         label
//! node          AB:CD:...                                                          node1
//! admin         0123...                                                            alice
//! ```
//!
//! Get a fingerprint with `openssl x509 -in cert.pem -noout -fingerprint -sha256`.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{SystemTime, UNIX_EPOCH};

use tonic::codegen::http;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tonic::Status;
use tower_layer::Layer;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    ReadOnly,
    ReadWrite,
    Admin,
    Node,
}

impl Role {
    pub fn name(self) -> &'static str {
        match self {
            Role::ReadOnly => "read-only",
            Role::ReadWrite => "read-write",
            Role::Admin => "admin",
            Role::Node => "node",
        }
    }
    fn parse(s: &str) -> Option<Role> {
        Some(match s {
            "read-only" => Role::ReadOnly,
            "read-write" => Role::ReadWrite,
            "admin" => Role::Admin,
            "node" => Role::Node,
            _ => return None,
        })
    }
}

/// Every RPC and the role it needs. Anything not listed needs `Admin`.
pub const CLASSIFIED: &[(&str, Role)] = &[
    ("/raftdb.RaftTransport/RequestVote", Role::Node),
    ("/raftdb.RaftTransport/AppendEntries", Role::Node),
    ("/raftdb.RaftTransport/InstallSnapshot", Role::Node),
    ("/raftdb.ClientApi/Get", Role::ReadOnly),
    ("/raftdb.ClientApi/Put", Role::ReadWrite),
    ("/raftdb.ClientApi/Delete", Role::ReadWrite),
    ("/raftdb.ClientApi/AddNode", Role::Admin),
    ("/raftdb.ClientApi/RemoveNode", Role::Admin),
    ("/raftdb.ClientApi/Backup", Role::Admin),
];

/// The role a method needs. Unknown methods (and unknown services) need `Admin`.
pub fn required_role(path: &str) -> Role {
    CLASSIFIED
        .iter()
        .find(|(p, _)| *p == path)
        .map(|(_, r)| *r)
        .unwrap_or(Role::Admin)
}

/// Whether a certificate with role `have` may call a method that needs `need`.
pub fn allows(have: Role, need: Role) -> bool {
    match need {
        // Raft traffic: node certificates only.
        Role::Node => have == Role::Node,
        // Client methods: any client role at or above the needed one, never a node certificate.
        _ => have != Role::Node && have >= need,
    }
}

pub fn fingerprint(der: &[u8]) -> [u8; 32] {
    let d = ring::digest::digest(&ring::digest::SHA256, der);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

#[derive(Debug)]
pub struct Policy {
    by_fingerprint: HashMap<[u8; 32], (Role, String)>,
}

impl Policy {
    pub fn parse(text: &str) -> Result<Policy, String> {
        let mut by_fingerprint = HashMap::new();
        for (n, raw) in text.lines().enumerate() {
            let n = n + 1;
            let line = raw.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let role_text = parts.next().unwrap_or("");
            let role = Role::parse(role_text).ok_or_else(|| {
                format!(
                    "line {n}: unknown role '{role_text}' (use read-only, read-write, admin or node)"
                )
            })?;
            let fp_text = parts
                .next()
                .ok_or_else(|| format!("line {n}: missing the certificate fingerprint"))?;
            let fp = parse_fingerprint(fp_text).map_err(|e| format!("line {n}: {e}"))?;
            let label = parts.collect::<Vec<_>>().join(" ");
            if let Some((other, _)) = by_fingerprint.insert(fp, (role, label)) {
                return Err(format!(
                    "line {n}: this certificate is listed twice (already as {})",
                    other.name()
                ));
            }
        }
        let p = Policy { by_fingerprint };
        if p.count(Role::Node) == 0 {
            return Err(
                "no `node` entry: with authorization on, the nodes must be listed too, or they \
                 could not talk to each other"
                    .into(),
            );
        }
        Ok(p)
    }

    pub fn load(path: &Path) -> Result<Policy, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read --authz-file {}: {e}", path.display()))?;
        Policy::parse(&text).map_err(|e| format!("--authz-file {}: {e}", path.display()))
    }

    #[cfg(test)]
    pub fn role_of(&self, fp: &[u8; 32]) -> Option<Role> {
        self.by_fingerprint.get(fp).map(|(r, _)| *r)
    }

    pub fn count(&self, role: Role) -> usize {
        self.by_fingerprint
            .values()
            .filter(|(r, _)| *r == role)
            .count()
    }
}

fn parse_fingerprint(s: &str) -> Result<[u8; 32], String> {
    let cleaned: String = s.chars().filter(|c| *c != ':').collect();
    if cleaned.len() != 64 || !cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "'{s}' is not a SHA-256 fingerprint (64 hex digits, colons optional)"
        ));
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&cleaned[2 * i..2 * i + 2], 16).map_err(|e| e.to_string())?;
    }
    Ok(out)
}

/// Rate-limits the denial log line so a hostile client cannot flood the log.
struct DenyLog {
    last_ms: AtomicU64,
    suppressed: AtomicU64,
}

impl DenyLog {
    fn record(&self, node: u64, who: &str, method: &str, required: Role) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = self.last_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= 1000
            && self
                .last_ms
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            let suppressed = self.suppressed.swap(0, Ordering::Relaxed);
            raftdb_log::events::authz_denied(node, who, method, required.name(), suppressed);
        } else {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Who a verified certificate belongs to. Attached to every allowed request so
/// handlers can tie behaviour to it (a node certificate may only speak as its
/// own node id).
#[derive(Clone, Debug)]
pub struct Principal {
    pub role: Role,
    pub label: String,
    pub node_id: Option<u64>,
}

/// `node3`, `node-3`, `node_3` or `3` (first word of the label) -> 3.
pub fn node_id_from_label(label: &str) -> Option<u64> {
    let word = label.split_whitespace().next()?.to_ascii_lowercase();
    let digits = word
        .strip_prefix("node")
        .map(|r| r.trim_start_matches(['-', '_']))
        .unwrap_or(&word);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok().filter(|&n| n != 0)
}

impl Policy {
    fn principal_of(&self, fp: &[u8; 32]) -> Option<Principal> {
        let (role, label) = self.by_fingerprint.get(fp)?;
        Some(Principal {
            role: *role,
            label: label.clone(),
            node_id: (*role == Role::Node)
                .then(|| node_id_from_label(label))
                .flatten(),
        })
    }

    /// Labels of `node` entries that name no node id (their certificates are
    /// not bound to one).
    fn unbound_node_labels(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .by_fingerprint
            .values()
            .filter(|(r, l)| *r == Role::Node && node_id_from_label(l).is_none())
            .map(|(_, l)| l.clone())
            .collect();
        v.sort();
        v
    }
}

/// The policy in force, replaceable while the node runs (SIGHUP re-reads the
/// file). A failed reload keeps the previous policy.
pub struct PolicyHandle {
    path: std::path::PathBuf,
    current: std::sync::RwLock<Arc<Policy>>,
}

impl PolicyHandle {
    pub fn load(path: &std::path::Path) -> Result<Arc<PolicyHandle>, String> {
        let policy = Policy::load(path)?;
        Ok(Arc::new(PolicyHandle {
            path: path.to_path_buf(),
            current: std::sync::RwLock::new(Arc::new(policy)),
        }))
    }

    pub fn get(&self) -> Arc<Policy> {
        self.current
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    pub fn reload(&self) -> Result<Arc<Policy>, String> {
        let fresh = Arc::new(Policy::load(&self.path)?);
        *self.current.write().unwrap_or_else(|p| p.into_inner()) = fresh.clone();
        Ok(fresh)
    }
}

/// Re-read the authorization file on SIGHUP: a certificate can be added or
/// revoked immediately, with no restart.
#[cfg(unix)]
pub fn spawn_reload_on_sighup(handle: Arc<PolicyHandle>, node: u64) {
    tokio::spawn(async move {
        let Ok(mut hup) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::hangup())
        else {
            return;
        };
        while hup.recv().await.is_some() {
            match handle.reload() {
                Ok(p) => {
                    raftdb_log::events::authz_reloaded(
                        node,
                        p.count(Role::Admin),
                        p.count(Role::ReadWrite),
                        p.count(Role::ReadOnly),
                        p.count(Role::Node),
                    );
                    let unbound = p.unbound_node_labels();
                    if !unbound.is_empty() {
                        raftdb_log::events::authz_node_unbound(node, &unbound.join(","));
                    }
                }
                Err(e) => raftdb_log::events::authz_reload_failed(node, &e),
            }
        }
    });
}

#[cfg(not(unix))]
pub fn spawn_reload_on_sighup(_handle: Arc<PolicyHandle>, _node: u64) {}

/// For a Raft RPC: the node id the sender claims must be the one its
/// certificate was issued for (when the policy names one).
#[allow(clippy::result_large_err)]
pub fn check_sender(
    principal: Option<&Principal>,
    claimed: u64,
    this_node: u64,
) -> Result<(), Status> {
    match principal.and_then(|p| p.node_id) {
        Some(id) if id != claimed => {
            static LAST_MS: AtomicU64 = AtomicU64::new(0);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let last = LAST_MS.load(Ordering::Relaxed);
            if now.saturating_sub(last) >= 1000
                && LAST_MS
                    .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
                raftdb_log::events::node_identity_mismatch(this_node, id, claimed);
            }
            Err(Status::permission_denied(format!(
                "this certificate belongs to node {id}, not node {claimed}"
            )))
        }
        _ => Ok(()),
    }
}

#[derive(Clone)]
pub struct AuthzLayer {
    policy: Arc<PolicyHandle>,
    node: u64,
    audit_all: bool,
    log: Arc<DenyLog>,
}

impl AuthzLayer {
    pub fn new(handle: Arc<PolicyHandle>, node: u64, audit_all: bool) -> AuthzLayer {
        let policy = handle.get();
        let unbound = policy.unbound_node_labels();
        if !unbound.is_empty() {
            raftdb_log::events::authz_node_unbound(node, &unbound.join(","));
        }
        raftdb_log::events::authz_enabled(
            node,
            policy.count(Role::Admin),
            policy.count(Role::ReadWrite),
            policy.count(Role::ReadOnly),
            policy.count(Role::Node),
        );
        AuthzLayer {
            policy: handle,
            node,
            audit_all,
            log: Arc::new(DenyLog {
                last_ms: AtomicU64::new(0),
                suppressed: AtomicU64::new(0),
            }),
        }
    }
}

impl<S> Layer<S> for AuthzLayer {
    type Service = AuthzService<S>;
    fn layer(&self, inner: S) -> AuthzService<S> {
        AuthzService {
            inner,
            policy: self.policy.clone(),
            node: self.node,
            audit_all: self.audit_all,
            log: self.log.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthzService<S> {
    inner: S,
    policy: Arc<PolicyHandle>,
    node: u64,
    audit_all: bool,
    log: Arc<DenyLog>,
}

impl<S> AuthzService<S> {
    // `Status` is tonic's own error type, returned to the client as-is.
    #[allow(clippy::result_large_err)]
    fn decide<B>(&self, req: &http::Request<B>) -> Result<Principal, Status> {
        let path = req.uri().path();
        let need = required_role(path);
        let fp = req
            .extensions()
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .and_then(|info| info.peer_certs())
            .and_then(|certs| certs.first().map(|leaf| fingerprint(leaf.as_ref())));
        let Some(fp) = fp else {
            self.log.record(self.node, "none", path, need);
            return Err(Status::permission_denied(
                "no client certificate was presented",
            ));
        };
        match self.policy.get().principal_of(&fp) {
            None => {
                self.log.record(self.node, &hex(&fp[..6]), path, need);
                Err(Status::permission_denied(
                    "this certificate is not listed in the authorization file",
                ))
            }
            Some(p) if !allows(p.role, need) => {
                self.log.record(self.node, &hex(&fp[..6]), path, need);
                Err(Status::permission_denied(format!(
                    "role {} may not call {path}; it needs {}",
                    p.role.name(),
                    need.name()
                )))
            }
            Some(p) => {
                // Raft traffic is not audited (it is constant); client requests
                // are, when they are admin operations or --audit-all is set.
                if need != Role::Node && (need == Role::Admin || self.audit_all) {
                    raftdb_log::events::audit(self.node, &p.label, p.role.name(), path);
                }
                Ok(p)
            }
        }
    }
}

impl<S, B> tonic::codegen::Service<http::Request<B>> for AuthzService<S>
where
    S: tonic::codegen::Service<http::Request<B>, Response = http::Response<tonic::body::BoxBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        match self.decide(&req) {
            Ok(principal) => {
                req.extensions_mut().insert(principal);
                // Use the instance that was polled ready, leave a clone behind.
                let clone = self.inner.clone();
                let mut inner = std::mem::replace(&mut self.inner, clone);
                Box::pin(async move { inner.call(req).await })
            }
            Err(status) => Box::pin(async move { Ok(status.to_http()) }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FP_A: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
    const FP_B: &str = "ffeeddccbbaa99887766554433221100ffeeddccbbaa99887766554433221100";

    #[test]
    fn sha256_of_abc_matches_the_known_vector() {
        assert_eq!(
            hex(&fingerprint(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn a_policy_parses_with_comments_colons_and_labels() {
        let colon: String = FP_B
            .as_bytes()
            .chunks(2)
            .map(|c| std::str::from_utf8(c).unwrap().to_uppercase())
            .collect::<Vec<_>>()
            .join(":");
        let text = format!("# nodes\nnode {FP_A} node1  # first\n\nadmin {colon} alice smith\n");
        let p = Policy::parse(&text).unwrap();
        assert_eq!(p.count(Role::Node), 1);
        assert_eq!(p.count(Role::Admin), 1);
        assert_eq!(
            p.role_of(&parse_fingerprint(FP_B).unwrap()),
            Some(Role::Admin)
        );
        assert_eq!(p.role_of(&[0u8; 32]), None);
    }

    #[test]
    fn bad_policies_are_refused() {
        let bad = [
            ("empty file", String::new()),
            ("no node entry", format!("admin {FP_A}")),
            ("unknown role", format!("node {FP_A}\nroot {FP_B}")),
            ("short fingerprint", "node abcd".to_string()),
            ("non-hex fingerprint", format!("node {}", "zz".repeat(32))),
            ("missing fingerprint", "node".to_string()),
            (
                "same certificate twice",
                format!("node {FP_A}\nadmin {FP_A}"),
            ),
        ];
        for (name, text) in bad {
            assert!(Policy::parse(&text).is_err(), "{name} must be refused");
        }
    }

    #[test]
    fn the_role_matrix() {
        use Role::*;
        // (role, needs, allowed)
        let cases = [
            (ReadOnly, ReadOnly, true),
            (ReadOnly, ReadWrite, false),
            (ReadOnly, Admin, false),
            (ReadWrite, ReadOnly, true),
            (ReadWrite, ReadWrite, true),
            (ReadWrite, Admin, false),
            (Admin, ReadOnly, true),
            (Admin, ReadWrite, true),
            (Admin, Admin, true),
            // Raft transport: node certificates only; nodes never use the client API.
            (Node, Node, true),
            (Admin, Node, false),
            (ReadWrite, Node, false),
            (ReadOnly, Node, false),
            (Node, ReadOnly, false),
            (Node, ReadWrite, false),
            (Node, Admin, false),
        ];
        for (have, need, ok) in cases {
            assert_eq!(
                allows(have, need),
                ok,
                "{have:?} calling something that needs {need:?}"
            );
        }
    }

    #[test]
    fn unknown_methods_need_admin() {
        assert_eq!(required_role("/raftdb.ClientApi/SomethingNew"), Role::Admin);
        assert_eq!(required_role("/other.Service/Whatever"), Role::Admin);
        assert_eq!(required_role(""), Role::Admin);
        // ...and so a node certificate can call none of them.
        assert!(!allows(
            Role::Node,
            required_role("/raftdb.ClientApi/SomethingNew")
        ));
    }

    /// Adding an RPC to the proto without deciding who may call it must fail a test, not ship.
    #[test]
    fn every_rpc_in_the_proto_file_is_classified() {
        let proto = include_str!("../../raftdb-proto/proto/raftdb.proto");
        let mut service = String::new();
        let mut found = 0;
        for line in proto.lines() {
            let l = line.trim();
            if let Some(rest) = l.strip_prefix("service ") {
                service = rest.trim_end_matches('{').trim().to_string();
            } else if let Some(rest) = l.strip_prefix("rpc ") {
                let name = rest.split('(').next().unwrap().trim();
                let path = format!("/raftdb.{service}/{name}");
                assert!(
                    CLASSIFIED.iter().any(|(p, _)| *p == path),
                    "{path} is in the proto file but not in authz::CLASSIFIED"
                );
                found += 1;
            }
        }
        assert_eq!(
            found,
            CLASSIFIED.len(),
            "CLASSIFIED has entries that are not in the proto"
        );
    }

    #[test]
    fn node_labels_name_their_node_id() {
        for (l, want) in [
            ("node1", Some(1)),
            ("node-2", Some(2)),
            ("NODE_3", Some(3)),
            ("4", Some(4)),
            ("node5 primary", Some(5)),
            ("node", None),
            ("node0", None),
            ("all nodes", None),
            ("nodeX", None),
            ("", None),
        ] {
            assert_eq!(node_id_from_label(l), want, "{l:?}");
        }
    }

    #[test]
    fn a_node_certificate_is_bound_to_its_node_id_but_other_roles_are_not() {
        let policy =
            Policy::parse(&format!("node {FP_A} node2\nread-write {FP_B} node3\n")).unwrap();
        let a = policy
            .principal_of(&parse_fingerprint(FP_A).unwrap())
            .unwrap();
        assert_eq!((a.role, a.node_id), (Role::Node, Some(2)));
        let b = policy
            .principal_of(&parse_fingerprint(FP_B).unwrap())
            .unwrap();
        assert_eq!(b.node_id, None, "only node certificates carry a node id");
        assert!(check_sender(Some(&a), 2, 1).is_ok());
        let denied = check_sender(Some(&a), 3, 1).unwrap_err();
        assert_eq!(denied.code(), tonic::Code::PermissionDenied);
        assert!(
            check_sender(Some(&b), 3, 1).is_ok(),
            "no binding for a non-node role"
        );
        assert!(
            check_sender(None, 3, 1).is_ok(),
            "no authorization configured: unchanged"
        );
    }

    #[test]
    fn unlabelled_node_entries_are_reported_as_unbound() {
        let p = Policy::parse(&format!("node {FP_A} all nodes\nnode {FP_B} node2\n")).unwrap();
        assert_eq!(p.unbound_node_labels(), vec!["all nodes".to_string()]);
    }

    #[test]
    fn a_failed_reload_keeps_the_previous_policy_and_a_good_one_replaces_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authz.txt");
        std::fs::write(&path, format!("node {FP_A} node1\nadmin {FP_B} alice\n")).unwrap();
        let h = PolicyHandle::load(&path).unwrap();
        assert_eq!(h.get().count(Role::Admin), 1);
        std::fs::write(&path, "this is not a policy").unwrap();
        assert!(h.reload().is_err());
        assert_eq!(
            h.get().count(Role::Admin),
            1,
            "the old policy must stay in force"
        );
        std::fs::write(&path, format!("node {FP_A} node1\n")).unwrap();
        assert!(h.reload().is_ok());
        assert_eq!(h.get().count(Role::Admin), 0, "the new policy replaced it");
    }
}
