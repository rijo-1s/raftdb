//! The `raftdb put/get/delete` side: a thin gRPC client against
//! `ClientApi`, with simple leader-following - if the node we ask isn't
//! the leader, it tells us who (probably) is via `leader_hint`, and we
//! retry there instead of making the person guess.

use std::collections::HashMap;
use std::time::Duration;

use raft_core::NodeId;
use raftdb_proto as proto;

type ClientApi = proto::client_api_client::ClientApiClient<tonic::transport::Channel>;

const MAX_REDIRECTS: usize = 5;
/// How long to wait for the initial TCP/TLS handshake to a node.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
/// How long to wait for any single RPC's response, once connected. Applied
/// automatically to every call made through a channel built by `connect()`, so
/// nothing here needs its own manual timeout wrapper.
const RPC_TIMEOUT: Duration = Duration::from_secs(5);

/// One logical client: a random non-zero id plus an increasing sequence
/// number. A write that may or may not have been applied (timeout, leader
/// change, lost reply) is retried with the SAME sequence number, and the
/// cluster guarantees it is applied at most once (see `kv::session`).
///
/// One `Session` is one sequential stream: do not run two writes on it at
/// the same time (the second would be rejected as superseded).
pub struct Session {
    id: u64,
    next_seq: u64,
}

impl Session {
    pub fn new() -> Self {
        use std::hash::{BuildHasher, Hasher};
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
        );
        h.write_u32(std::process::id());
        Session {
            id: h.finish() | 1, // never 0 (0 means "no session")
            next_seq: 1,
        }
    }

    fn take_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

/// Total tries for one write (initial + retries).
/// The retry budget is about 10 seconds in total (40 attempts, 250 ms apart).
/// It has to outlast a leader failover: the election timeout alone is 0.5-1 s,
/// then a pre-vote and a vote, so a budget of a second or so fails writes that
/// a moment later would have succeeded.
const MAX_ATTEMPTS: usize = 40;
const RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// Errors after which the SAME sequence number is safe to resend: the write
/// was never proposed (overloaded, leadership lost before proposal) or its
/// outcome is unknown and the session makes a resend idempotent.
/// An error followed by its root cause. tonic prints a transport failure as
/// just "transport error"; the useful part (an untrusted or expired
/// certificate, a refused connection) is further down the chain.
fn with_cause(e: &(dyn std::error::Error + 'static)) -> String {
    let mut root = None;
    let mut cause = e.source();
    while let Some(c) = cause {
        root = Some(c.to_string());
        cause = c.source();
    }
    match root {
        Some(r) if !e.to_string().contains(&r) => format!("{e}: {r}"),
        _ => e.to_string(),
    }
}

/// Errors that retrying cannot fix: the server refused this client (an
/// authorization denial) or the TLS handshake failed (an untrusted, expired or
/// missing certificate). Failing at once beats making the caller wait out the
/// whole retry budget just to learn the same thing.
fn permanent(error: &str) -> bool {
    [
        "PermissionDenied",
        "Unauthenticated",
        "ertificate",
        "alert",
        "handshake",
        // The other end is not speaking what we speak (e.g. plaintext to a TLS port).
        "protocol error",
    ]
    .iter()
    .any(|m| error.contains(m))
}

fn retryable(error: &str) -> bool {
    error.starts_with("server overloaded")
        || error.contains("outcome unknown")
        || error.contains("safe to retry")
        // Refused before it was proposed, and no leader to point at: an
        // election is in progress. Safe to retry, and it will resolve.
        || error.contains("not the leader")
}

pub async fn put(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    key: String,
    value: String,
) -> Result<(), String> {
    put_with(&mut Session::new(), cluster, start, key, value).await
}

pub async fn put_with(
    session: &mut Session,
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    key: String,
    value: String,
) -> Result<(), String> {
    let (client_id, seq) = (session.id, session.take_seq());
    write_with_retries(cluster, start, "write", |client| {
        let req = proto::PutRequest {
            key: key.clone(),
            value: value.clone(),
            client_id,
            seq,
        };
        Box::pin(async move {
            client.put(req).await.map(|r| {
                let r = r.into_inner();
                (r.ok, r.error, r.leader_hint)
            })
        })
    })
    .await
}

/// Shared retry loop for put/delete. Follows leader hints, rotates to the
/// next node after a transport error, and resends the identical request
/// (same client id and sequence) when the outcome was unknown.
async fn write_with_retries<F>(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    what: &str,
    mut call: F,
) -> Result<(), String>
where
    F: for<'a> FnMut(
        &'a mut ClientApi,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(bool, String, Option<u64>), tonic::Status>>
                + Send
                + 'a,
        >,
    >,
{
    let mut ids: Vec<NodeId> = cluster.keys().copied().collect();
    ids.sort_unstable();
    let mut target = start;
    let mut last_err = String::new();
    for attempt in 0..MAX_ATTEMPTS {
        let addr = cluster
            .get(&target)
            .ok_or_else(|| format!("no address known for node {target}"))?;
        let outcome = match connect(addr).await {
            Err(e) => Err(e), // never reached the node: nothing was sent
            Ok(mut client) => call(&mut client)
                .await
                .map_err(|e| format!("RPC to node {target} failed: {}", with_cause(&e))),
        };
        match outcome {
            Ok((true, _, _)) => return Ok(()),
            Ok((false, error, hint)) => {
                if let Some(h) = hint.filter(|h| *h != target) {
                    eprintln!("node {target} isn't leader ({error}); retrying against node {h}");
                    target = h;
                    last_err = error;
                    continue;
                }
                if retryable(&error) {
                    last_err = error;
                    tokio::time::sleep(RETRY_BACKOFF).await;
                    continue;
                }
                return Err(format!("node {target} rejected the {what}: {error}"));
            }
            Err(e) => {
                // Transport failure or timeout: the write may have been
                // applied. Resend the same sequence to the next node.
                last_err = e;
                if permanent(&last_err) {
                    return Err(last_err);
                }
                if let Some(pos) = ids.iter().position(|i| *i == target) {
                    target = ids[(pos + 1) % ids.len()];
                }
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
    }
    Err(format!("gave up after {MAX_ATTEMPTS} attempts: {last_err}"))
}

pub async fn get(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    key: String,
) -> Result<Option<String>, String> {
    let mut ids: Vec<NodeId> = cluster.keys().copied().collect();
    ids.sort_unstable();
    let mut target = start;
    let mut last_err = String::new();
    for attempt in 0..MAX_ATTEMPTS {
        let addr = cluster
            .get(&target)
            .ok_or_else(|| format!("no address known for node {target}"))?;
        let outcome = match connect(addr).await {
            Err(e) => Err(e),
            Ok(mut client) => client
                .get(proto::GetRequest { key: key.clone() })
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("RPC to node {target} failed: {}", with_cause(&e))),
        };
        match outcome {
            Ok(resp) if resp.error.is_empty() => {
                return Ok(if resp.found { Some(resp.value) } else { None });
            }
            Ok(resp) => match resp.leader_hint {
                Some(hint) if hint != target => {
                    eprintln!(
                        "node {target} isn't leader ({}); retrying against node {hint}",
                        resp.error
                    );
                    target = hint;
                    last_err = resp.error;
                }
                _ if retryable(&resp.error) => {
                    last_err = resp.error;
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
                _ => return Err(format!("node {target} rejected the read: {}", resp.error)),
            },
            Err(e) => {
                // The node is unreachable. A read changes nothing, so unlike
                // a write it is always safe to ask another node: a dead first
                // node in the list must not make the whole cluster unreadable.
                last_err = e;
                if permanent(&last_err) {
                    return Err(last_err);
                }
                if let Some(pos) = ids.iter().position(|i| *i == target) {
                    target = ids[(pos + 1) % ids.len()];
                }
                if attempt + 1 < MAX_ATTEMPTS {
                    tokio::time::sleep(RETRY_BACKOFF).await;
                }
            }
        }
    }
    Err(format!("gave up after {MAX_ATTEMPTS} attempts: {last_err}"))
}

pub async fn delete(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    key: String,
) -> Result<(), String> {
    delete_with(&mut Session::new(), cluster, start, key).await
}

pub async fn delete_with(
    session: &mut Session,
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    key: String,
) -> Result<(), String> {
    let (client_id, seq) = (session.id, session.take_seq());
    write_with_retries(cluster, start, "delete", |client| {
        let req = proto::DeleteRequest {
            key: key.clone(),
            client_id,
            seq,
        };
        Box::pin(async move {
            client.delete(req).await.map(|r| {
                let r = r.into_inner();
                (r.ok, r.error, r.leader_hint)
            })
        })
    })
    .await
}

pub async fn add_node(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    node_id: NodeId,
) -> Result<(), String> {
    let mut target = start;
    for _ in 0..MAX_REDIRECTS {
        let addr = cluster
            .get(&target)
            .ok_or_else(|| format!("no address known for node {target}"))?;
        let mut client = connect(addr).await?;
        let resp = client
            .add_node(proto::AddNodeRequest { node_id })
            .await
            .map_err(|e| format!("RPC to node {target} failed: {}", with_cause(&e)))?
            .into_inner();
        if resp.ok {
            return Ok(());
        }
        match resp.leader_hint {
            Some(hint) if hint != target => {
                eprintln!(
                    "node {target} isn't leader ({}); retrying against node {hint}",
                    resp.error
                );
                target = hint;
                continue;
            }
            _ => return Err(format!("node {target} rejected add-node: {}", resp.error)),
        }
    }
    Err(format!("gave up after {MAX_REDIRECTS} redirects"))
}

pub async fn remove_node(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    node_id: NodeId,
) -> Result<(), String> {
    let mut target = start;
    for _ in 0..MAX_REDIRECTS {
        let addr = cluster
            .get(&target)
            .ok_or_else(|| format!("no address known for node {target}"))?;
        let mut client = connect(addr).await?;
        let resp = client
            .remove_node(proto::RemoveNodeRequest { node_id })
            .await
            .map_err(|e| format!("RPC to node {target} failed: {}", with_cause(&e)))?
            .into_inner();
        if resp.ok {
            return Ok(());
        }
        match resp.leader_hint {
            Some(hint) if hint != target => {
                eprintln!(
                    "node {target} isn't leader ({}); retrying against node {hint}",
                    resp.error
                );
                target = hint;
                continue;
            }
            _ => {
                return Err(format!(
                    "node {target} rejected remove-node: {}",
                    resp.error
                ))
            }
        }
    }
    Err(format!("gave up after {MAX_REDIRECTS} redirects"))
}

/// Runs `count` PUTs split across `concurrency` clients (each with one
/// persistent connection to the leader, which is discovered first) and
/// reports throughput and latency percentiles. Real wall-clock numbers
/// against the actual running binary - not a simulation. Writes shed with a
/// "server overloaded" error are retried after a short back-off and counted.
pub async fn bench(
    cluster: &HashMap<NodeId, String>,
    start: NodeId,
    count: usize,
    value_size: usize,
    concurrency: usize,
) -> Result<(), String> {
    let concurrency = concurrency.max(1).min(count.max(1));
    let value = "x".repeat(value_size);

    // Find the leader with one real write.
    let mut target = start;
    let mut redirects = 0usize;
    loop {
        let addr = cluster
            .get(&target)
            .ok_or_else(|| format!("no address known for node {target}"))?;
        let mut client = connect(addr).await?;
        let resp = client
            .put(proto::PutRequest {
                key: "bench:probe".to_string(),
                value: value.clone(),
                client_id: 0,
                seq: 0,
            })
            .await
            .map_err(|e| format!("RPC to node {target} failed: {}", with_cause(&e)))?
            .into_inner();
        if resp.ok {
            break;
        }
        redirects += 1;
        if redirects > 20 {
            return Err(
                "too many redirects while benchmarking — is there a stable leader?".to_string(),
            );
        }
        match resp.leader_hint {
            Some(hint) if hint != target => target = hint,
            _ => {
                return Err(format!(
                    "node {target} rejected write during bench: {}",
                    resp.error
                ))
            }
        }
    }
    let leader_addr = cluster.get(&target).cloned().unwrap();

    let overall_start = std::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..concurrency {
        let share = count / concurrency + usize::from(worker < count % concurrency);
        let addr = leader_addr.clone();
        let value = value.clone();
        tasks.spawn(async move {
            let mut client = connect(&addr).await?;
            let mut lat: Vec<std::time::Duration> = Vec::with_capacity(share);
            let mut overloaded = 0u64;
            for i in 0..share {
                let key = format!("bench:{worker}:{i}");
                let t0 = std::time::Instant::now();
                loop {
                    let resp = client
                        .put(proto::PutRequest {
                            key: key.clone(),
                            value: value.clone(),
                            client_id: 0,
                            seq: 0,
                        })
                        .await
                        .map_err(|e| format!("RPC failed: {}", with_cause(&e)))?
                        .into_inner();
                    if resp.ok {
                        lat.push(t0.elapsed());
                        break;
                    }
                    if resp.error.starts_with(crate::batching::OVERLOADED_PREFIX) {
                        overloaded += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        continue;
                    }
                    return Err(format!("write rejected during bench: {}", resp.error));
                }
            }
            Ok::<_, String>((lat, overloaded))
        });
    }
    let mut latencies: Vec<std::time::Duration> = Vec::with_capacity(count);
    let mut overloaded_total = 0u64;
    while let Some(joined) = tasks.join_next().await {
        let (lat, overloaded) = joined.map_err(|e| format!("bench worker panicked: {e}"))??;
        latencies.extend(lat);
        overloaded_total += overloaded;
    }
    let total = overall_start.elapsed();

    latencies.sort();
    let pct = |p: f64| -> std::time::Duration {
        let idx = (((latencies.len() as f64) - 1.0) * p).round() as usize;
        latencies[idx]
    };
    let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
    let throughput = latencies.len() as f64 / total.as_secs_f64();

    println!(
        "raftdb bench: {} PUTs, {value_size}-byte values, {concurrency} concurrent client(s), leader = node {target}",
        latencies.len()
    );
    println!("  total time:  {:.3}s", total.as_secs_f64());
    println!("  throughput:  {throughput:.1} ops/sec");
    println!("  latency p50: {:.2}ms", ms(pct(0.50)));
    println!("  latency p95: {:.2}ms", ms(pct(0.95)));
    println!("  latency p99: {:.2}ms", ms(pct(0.99)));
    println!("  latency max: {:.2}ms", ms(*latencies.last().unwrap()));
    if overloaded_total > 0 {
        println!("  overload retries: {overloaded_total}");
    }

    Ok(())
}

async fn connect(
    addr: &str,
) -> Result<proto::client_api_client::ClientApiClient<tonic::transport::Channel>, String> {
    let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
        .map_err(|e| format!("invalid address {addr}: {e}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT);
    let endpoint =
        crate::tls::apply(endpoint).map_err(|e| format!("TLS configuration for {addr}: {e}"))?;
    let channel = endpoint.connect().await.map_err(|e| {
        format!(
            "could not connect to {addr} within {CONNECT_TIMEOUT:?}: {}",
            with_cause(&e)
        )
    })?;
    Ok(proto::client_api_client::ClientApiClient::new(channel))
}
