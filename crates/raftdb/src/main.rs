mod backup;
mod batching;
mod certinfo;
mod client;
mod convert;
mod diskguard;
mod metrics;
mod node_loop;
mod server;

use std::collections::HashMap;
use std::net::SocketAddr;

use clap::{Parser, Subcommand};
use raft_core::NodeId;
use raftdb_proto as proto;
use storage::WalLog;

mod authz;
mod tls;

#[derive(Parser)]
#[command(
    name = "raftdb",
    about = "A small Raft-backed distributed key-value store"
)]
struct Cli {
    #[command(flatten)]
    tls: tls::TlsArgs,
    /// (start) Turn on role-based authorization from this file (see docs/authorization.md).
    /// Requires mutual TLS: roles are identified by the client certificate's fingerprint.
    #[arg(long, global = true, value_name = "FILE")]
    authz_file: Option<std::path::PathBuf>,
    /// (start) Also log every allowed client request (who, role, method) as an `audit`
    /// event. Admin operations (backup, add/remove node) are always logged.
    #[arg(long, global = true, default_value_t = false)]
    audit_all: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run this process as one node of the cluster.
    Start {
        /// This node's own ID.
        #[arg(long)]
        id: NodeId,
        /// Address to bind this node's gRPC server to, e.g. 127.0.0.1:6001
        #[arg(long)]
        addr: String,
        /// Other nodes in the cluster: id=addr,id=addr,... (do not include self)
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,
        /// Additional node addresses this process should be able to dial
        /// (for gRPC transport) but that are not yet Raft members. Use this on
        /// existing nodes when you plan to `add-node` a new one later, so they can
        /// already reach it once it is added. Kept separate from --peers because
        /// peers are Raft members and these are not.
        #[arg(long, value_delimiter = ',', default_value = "")]
        known_peers: Vec<String>,
        /// Directory to store this node's WAL/hardstate/snapshot files in.
        #[arg(long)]
        data_dir: String,
        /// This node is joining an already-running cluster rather than
        /// bootstrapping a fresh one - it won't start elections on its own
        /// until an operator runs `add-node` for it against the current
        /// leader. --peers should list the cluster's current membership.
        #[arg(long, default_value_t = false)]
        join: bool,
        /// Compact the log every N applied entries.
        #[arg(long, default_value_t = 50)]
        compaction_threshold: usize,
        /// Optional address to serve Prometheus metrics on (e.g.
        /// 127.0.0.1:9001). If omitted, no metrics server is started.
        #[arg(long)]
        metrics_addr: Option<String>,
        /// Also serve metrics and health over HTTPS on this address, using
        /// --tls-cert/--tls-key (and requiring client certificates when
        /// --tls-require-client-cert is set). May be used together with --metrics-addr.
        #[arg(long)]
        metrics_tls_addr: Option<String>,
        /// Group commit: flush a batch of client writes once this many are
        /// queued. All of a batch's entries share one WAL fsync and one
        /// AppendEntries round. 1 disables batching.
        #[arg(long, default_value_t = 256)]
        max_batch_entries: usize,
        /// Group commit: flush once queued command bytes reach this.
        #[arg(long, default_value_t = 1_048_576)]
        max_batch_bytes: usize,
        /// Group commit: longest the oldest queued write waits for others,
        /// in microseconds. 0 (default) = flush as soon as the already-queued
        /// requests have been collected: no added latency when idle.
        #[arg(long, default_value_t = 0)]
        max_batch_delay_us: u64,
        #[arg(long, default_value = "mem")]
        engine: String,
        #[arg(long, default_value_t = false)]
        lsm_sync_wal: bool,
        #[arg(long, default_value_t = 4)]
        lsm_memtable_mb: usize,
        #[arg(long, default_value_t = 8)]
        lsm_cache_mb: usize,
        /// Backpressure: queued + in-flight writes allowed; more are
        /// rejected with a "server overloaded" error.
        #[arg(long, default_value_t = 8192)]
        max_pending_writes: usize,
        /// Free-space floor for the data volume, in MiB. Below it the node
        /// rejects new client writes (reads and membership changes keep
        /// working) and pauses log compaction, and resumes by itself once
        /// space is freed. 0 turns the protection off.
        #[arg(long, default_value_t = 256)]
        min_free_mb: u64,
        /// Largest single write command accepted, in bytes.
        #[arg(long, default_value_t = 1_048_576)]
        max_command_bytes: usize,
        /// How long a proposed write may wait to be applied before the
        /// client is told the outcome is unknown (it may still commit).
        #[arg(long, default_value_t = 10_000)]
        write_timeout_ms: u64,
        /// Most entries one AppendEntries RPC carries.
        #[arg(long, default_value_t = 1024)]
        max_append_entries: usize,
        /// Most payload bytes one AppendEntries RPC carries.
        #[arg(long, default_value_t = 1_048_576)]
        max_append_bytes: usize,
        /// Shorthand for --max-batch-entries 1: one fsync and one
        /// replication round per write (the pre-batching behavior).
        #[arg(long, default_value_t = false)]
        no_batching: bool,
    },
    /// Write a key's value. Redirects automatically if the node you asked isn't leader.
    Put {
        #[arg(long, value_delimiter = ',')]
        cluster: Vec<String>,
        #[arg(long)]
        node: Option<NodeId>,
        key: String,
        value: String,
    },
    /// Read a key's value (linearizable). Redirects automatically if needed.
    Get {
        #[arg(long, value_delimiter = ',')]
        cluster: Vec<String>,
        #[arg(long)]
        node: Option<NodeId>,
        key: String,
    },
    /// Delete a key. Redirects automatically if needed.
    Delete {
        #[arg(long, value_delimiter = ',')]
        cluster: Vec<String>,
        #[arg(long)]
        node: Option<NodeId>,
        key: String,
    },
    /// Add a node to the cluster's Raft membership (the node's process and
    /// address must already be reachable, and every existing node must
    /// already know its address via its own --peers list).
    AddNode {
        #[arg(long, value_delimiter = ',')]
        cluster: Vec<String>,
        #[arg(long)]
        node: Option<NodeId>,
        node_id: NodeId,
    },
    /// Export a consistent point-in-time backup from a RUNNING node (any
    /// node: leader or follower). Never overwrites an existing file.
    Backup {
        /// The node to back up, e.g. 127.0.0.1:6001
        #[arg(long)]
        addr: String,
        #[arg(long)]
        out: String,
    },
    /// OFFLINE: create a NEW data directory holding a single-node cluster
    /// from a backup. The directory must not exist or must be empty.
    Restore {
        #[arg(long)]
        backup: String,
        #[arg(long)]
        data_dir: String,
        /// The id the restored node will have. Use a FRESH id/data dir and
        /// new ports; never reuse a node of a cluster that is still running.
        #[arg(long)]
        node_id: NodeId,
    },
    /// Remove a node from the cluster's Raft membership.
    RemoveNode {
        #[arg(long, value_delimiter = ',')]
        cluster: Vec<String>,
        #[arg(long)]
        node: Option<NodeId>,
        node_id: NodeId,
    },
    /// Run N sequential PUTs against the cluster and report throughput and
    /// p50/p95/p99 latency.
    Bench {
        #[arg(long, value_delimiter = ',')]
        cluster: Vec<String>,
        #[arg(long)]
        node: Option<NodeId>,
        #[arg(long, default_value_t = 500)]
        count: usize,
        #[arg(long, default_value_t = 64)]
        value_size: usize,
        /// Number of concurrent clients (each with its own connection).
        #[arg(long, default_value_t = 1)]
        concurrency: usize,
    },
}

/// Parses "1=127.0.0.1:6001,2=127.0.0.1:6002" into a NodeId -> gRPC target
/// URL map, adding the "http://" scheme tonic requires if it's missing.
fn parse_id_addr_list(items: &[String]) -> Result<HashMap<NodeId, String>, String> {
    let mut map = HashMap::new();
    for item in items {
        let (id_str, addr) = item
            .split_once('=')
            .ok_or_else(|| format!("expected id=addr, got '{item}'"))?;
        let id: NodeId = id_str
            .parse()
            .map_err(|_| format!("bad node id in '{item}'"))?;
        let tls_on = tls::outbound().is_some();
        if tls_on && addr.starts_with("http://") {
            return Err(format!(
                "'{item}' uses http:// but TLS is enabled; use https:// or just host:port"
            ));
        }
        if !tls_on && addr.starts_with("https://") {
            return Err(format!("'{item}' uses https:// but no --tls-ca was given"));
        }
        let url = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("{}://{addr}", tls::scheme())
        };
        map.insert(id, url);
    }
    Ok(map)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match raftdb_log::LogConfig::from_env() {
        Ok(cfg) => {
            let _ = raftdb_log::init(&cfg);
        }
        Err(e) => eprintln!("{e}"),
    }
    raftdb_log::install_panic_hook();
    let cli = Cli::parse();

    // Validate TLS before anything else is opened or bound: a bad or partial
    // configuration must stop the process, never fall back to plaintext.
    let tls_result = if matches!(cli.command, Command::Start { .. }) {
        cli.tls.build_for_node()
    } else {
        cli.tls.build()
    };
    let tls_setup = tls_result.map_err(|e| format!("TLS configuration error: {e}"))?;
    if let Some(cfg) = tls_setup.outbound.clone() {
        tls::set_outbound(cfg);
    }
    let server_tls = tls_setup.server;
    let mutual_tls = cli.tls.tls_require_client_cert;
    let cert_path = cli.tls.tls_cert.clone();
    let authz: Option<std::sync::Arc<authz::PolicyHandle>> = match &cli.authz_file {
        None => None,
        Some(_) if !matches!(cli.command, Command::Start { .. }) => {
            return Err("--authz-file only applies to `start`".into());
        }
        Some(path) => {
            if server_tls.is_none() || !mutual_tls {
                return Err(
                    "--authz-file needs mutual TLS (--tls-cert, --tls-key, --tls-ca and \
                            --tls-require-client-cert): roles are identified by the client \
                            certificate, so the certificate must be required and verified"
                        .into(),
                );
            }
            Some(
                authz::PolicyHandle::load(path)
                    .map_err(|e| format!("authorization configuration error: {e}"))?,
            )
        }
    };

    let is_start = matches!(cli.command, Command::Start { .. });
    let mut key_permissions = None;
    if is_start {
        if let Some((path, mode)) = cli.tls.key_permission_problem() {
            if cli.tls.tls_strict_key_permissions {
                return Err(format!(
                    "TLS configuration error: the private key {} is accessible by group or \
                     others (mode {mode:o}); run `chmod 600 {}`, or drop \
                     --tls-strict-key-permissions to only warn",
                    path.display(),
                    path.display()
                )
                .into());
            }
            key_permissions = Some((path.display().to_string(), mode));
        }
    }
    if cli.audit_all && authz.is_none() {
        return Err("--audit-all needs --authz-file".into());
    }
    let audit_all = cli.audit_all;
    let metrics_tls_cfg = if is_start {
        cli.tls
            .metrics_server_config()
            .map_err(|e| format!("TLS configuration error: {e}"))?
    } else {
        None
    };

    match cli.command {
        Command::Start {
            id,
            addr,
            peers,
            known_peers,
            data_dir,
            join,
            compaction_threshold,
            metrics_addr,
            metrics_tls_addr,
            max_batch_entries,
            max_batch_bytes,
            max_batch_delay_us,
            max_pending_writes,
            min_free_mb,
            max_command_bytes,
            write_timeout_ms,
            max_append_entries,
            max_append_bytes,
            no_batching,
            engine,
            lsm_sync_wal,
            lsm_memtable_mb,
            lsm_cache_mb,
        } => {
            let batch_cfg = batching::BatchConfig {
                max_batch_entries: if no_batching { 1 } else { max_batch_entries },
                max_batch_bytes,
                max_batch_delay: std::time::Duration::from_micros(max_batch_delay_us),
                max_pending_writes,
                max_command_bytes,
                write_timeout: std::time::Duration::from_millis(write_timeout_ms),
                max_append_entries,
                max_append_bytes,
            };
            batch_cfg.validate()?;
            run_node(StartArgs {
                id,
                addr,
                peers,
                known_peers,
                data_dir,
                join,
                compaction_threshold,
                min_free_mb,
                metrics_addr,
                metrics_tls_addr,
                metrics_tls: metrics_tls_cfg,
                key_permissions,
                audit_all,
                batch_cfg,
                engine,
                lsm_sync_wal,
                lsm_memtable_mb,
                lsm_cache_mb,
                server_tls,
                mutual_tls,
                cert_path,
                authz,
            })
            .await
        }
        Command::Put {
            cluster,
            node,
            key,
            value,
        } => {
            let cluster = parse_id_addr_list(&cluster)?;
            let start = node
                .or_else(|| cluster.keys().min().copied())
                .ok_or("empty --cluster list")?;
            client::put(&cluster, start, key, value)
                .await
                .map_err(Into::into)
        }
        Command::Get { cluster, node, key } => {
            let cluster = parse_id_addr_list(&cluster)?;
            let start = node
                .or_else(|| cluster.keys().min().copied())
                .ok_or("empty --cluster list")?;
            match client::get(&cluster, start, key).await? {
                Some(value) => println!("{value}"),
                None => println!("(not found)"),
            }
            Ok(())
        }
        Command::Delete { cluster, node, key } => {
            let cluster = parse_id_addr_list(&cluster)?;
            let start = node
                .or_else(|| cluster.keys().min().copied())
                .ok_or("empty --cluster list")?;
            client::delete(&cluster, start, key)
                .await
                .map_err(Into::into)
        }
        Command::Backup { addr, out } => {
            backup::run_backup(&addr, std::path::Path::new(&out)).await
        }
        Command::Restore {
            backup: file,
            data_dir,
            node_id,
        } => {
            // A rejected restore is a message, not a backtrace.
            std::panic::set_hook(Box::new(|_| {}));
            let dir = std::path::Path::new(&data_dir);
            match backup::run_restore(std::path::Path::new(&file), dir, node_id) {
                Ok(r) => {
                    backup::print_restore(&r, dir, node_id);
                    Ok(())
                }
                Err(e) => Err(format!("restore failed: {e}").into()),
            }
        }
        Command::AddNode {
            cluster,
            node,
            node_id,
        } => {
            let cluster = parse_id_addr_list(&cluster)?;
            let start = node
                .or_else(|| cluster.keys().min().copied())
                .ok_or("empty --cluster list")?;
            client::add_node(&cluster, start, node_id)
                .await
                .map_err(Into::into)
        }
        Command::RemoveNode {
            cluster,
            node,
            node_id,
        } => {
            let cluster = parse_id_addr_list(&cluster)?;
            let start = node
                .or_else(|| cluster.keys().min().copied())
                .ok_or("empty --cluster list")?;
            client::remove_node(&cluster, start, node_id)
                .await
                .map_err(Into::into)
        }
        Command::Bench {
            cluster,
            node,
            count,
            value_size,
            concurrency,
        } => {
            let cluster = parse_id_addr_list(&cluster)?;
            let start = node
                .or_else(|| cluster.keys().min().copied())
                .ok_or("empty --cluster list")?;
            client::bench(&cluster, start, count, value_size, concurrency)
                .await
                .map_err(Into::into)
        }
    }
}

struct StartArgs {
    id: NodeId,
    addr: String,
    peers: Vec<String>,
    known_peers: Vec<String>,
    data_dir: String,
    join: bool,
    compaction_threshold: usize,
    min_free_mb: u64,
    metrics_addr: Option<String>,
    metrics_tls_addr: Option<String>,
    metrics_tls: Option<std::sync::Arc<rustls::ServerConfig>>,
    key_permissions: Option<(String, u32)>,
    audit_all: bool,
    batch_cfg: batching::BatchConfig,
    engine: String,
    lsm_sync_wal: bool,
    lsm_memtable_mb: usize,
    lsm_cache_mb: usize,
    server_tls: Option<tonic::transport::ServerTlsConfig>,
    cert_path: Option<std::path::PathBuf>,
    mutual_tls: bool,
    authz: Option<std::sync::Arc<authz::PolicyHandle>>,
}

async fn run_node(args: StartArgs) -> Result<(), Box<dyn std::error::Error>> {
    let StartArgs {
        id,
        addr,
        peers: peers_raw,
        known_peers: known_peers_raw,
        data_dir,
        join,
        compaction_threshold,
        min_free_mb,
        metrics_addr,
        metrics_tls_addr,
        metrics_tls,
        key_permissions,
        audit_all,
        batch_cfg,
        engine,
        lsm_sync_wal,
        lsm_memtable_mb,
        lsm_cache_mb,
        server_tls,
        mutual_tls,
        cert_path,
        authz,
    } = args;
    let mut peer_addrs = parse_id_addr_list(&peers_raw)?;
    // Initial Raft membership comes only from --peers, captured *before*
    // merging in --known-peers (which are transport-reachable but not
    // yet Raft members - see the flag's help text).
    let peer_ids: Vec<NodeId> = peer_addrs.keys().copied().filter(|&p| p != id).collect();
    let known_peer_addrs = parse_id_addr_list(
        &known_peers_raw
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>(),
    )?;
    peer_addrs.extend(known_peer_addrs);

    std::fs::create_dir_all(&data_dir)?;
    let removed_marker = std::path::Path::new(&data_dir).join(node_loop::REMOVED_MARKER);
    if removed_marker.exists() {
        return Err(format!(
            "refusing to start: node {id} was removed from the cluster ({} exists). \
             To re-add it, wipe {data_dir} and start it with --join.",
            removed_marker.display()
        )
        .into());
    }
    let wal_path = std::path::Path::new(&data_dir).join("node.wal");
    // Recovery failures are explicit and the files are left untouched for
    // inspection: refuse to start rather than run on state we can't trust.
    let wal = WalLog::open(&wal_path).map_err(|e| {
        format!(
            "refusing to start: could not recover persisted state in {data_dir}: {e}\n\
             (the files have not been modified; restore from a backup or a healthy replica)"
        )
    })?;

    let store = match engine.as_str() {
        "mem" => kv::AnyStore::Mem(kv::KvStore::new()),
        "lsm" => {
            let mut o = kv::LsmStoreOptions::default();
            o.engine.sync_wal = lsm_sync_wal;
            o.engine.memtable_bytes = lsm_memtable_mb << 20;
            o.engine.cache_bytes = lsm_cache_mb << 20;
            let lsm_dir = std::path::Path::new(&data_dir).join("lsm");
            let (st, outcome) = kv::LsmStore::open_or_rebuild(&lsm_dir, o).map_err(|e| {
                raftdb_log::events::recovery_refused(id, &lsm_dir.display().to_string(), &e);
                format!(
                    "refusing to start: cannot open LSM engine in {}: {e}",
                    lsm_dir.display()
                )
            })?;
            if let kv::OpenOutcome::Quarantined { moved_to, reason } = outcome {
                raftdb_log::events::lsm_quarantined(id, &reason, &moved_to.display().to_string());
            }
            kv::AnyStore::Lsm(Box::new(st))
        }
        other => return Err(format!("unknown --engine {other:?} (expected mem or lsm)").into()),
    };

    raftdb_log::events::node_started(id, &addr.to_string(), &peer_ids, &data_dir, &engine, join);

    let node_metrics = std::sync::Arc::new(metrics::Metrics::default());
    if let Some(path) = &cert_path {
        match std::fs::read(path)
            .map_err(|e| e.to_string())
            .and_then(|pem| certinfo::leaf_not_after_from_pem(&pem))
        {
            Ok(not_after) => {
                node_metrics.record_cert_expiry(not_after);
                raftdb_log::events::tls_cert_expiry(
                    id,
                    not_after,
                    not_after - certinfo::now_unix(),
                );
            }
            Err(e) => raftdb_log::events::tls_cert_expiry_unknown(id, &e),
        }
    }
    if let Some((path, mode)) = &key_permissions {
        raftdb_log::events::tls_key_permissions(path, *mode);
    }
    if let Some(taddr) = metrics_tls_addr {
        let cfg = metrics_tls.ok_or("--metrics-tls-addr needs --tls-cert and --tls-key")?;
        let taddr: SocketAddr = taddr.parse()?;
        let m = node_metrics.clone();
        raftdb_log::events::metrics_listening(id, &format!("https://{taddr}"));
        tokio::spawn(async move { metrics::serve_tls(taddr, id, m, cfg).await });
    }
    if let Some(maddr) = metrics_addr {
        let maddr: SocketAddr = maddr.parse()?;
        let m = node_metrics.clone();
        raftdb_log::events::metrics_listening(id, &maddr.to_string());
        tokio::spawn(async move { metrics::serve(maddr, id, m).await });
    }

    let client_metrics = node_metrics.clone();
    let shutdown_metrics = node_metrics.clone();
    let tx = node_loop::spawn(
        id,
        peer_ids,
        peer_addrs,
        wal,
        node_metrics,
        node_loop::NodeOptions {
            join,
            compaction_threshold,
            batch: batch_cfg,
            removed_marker: Some(removed_marker),
            min_free_bytes: min_free_mb.saturating_mul(1024 * 1024),
            data_dir: Some(std::path::PathBuf::from(&data_dir)),
            store,
        },
    );

    let bind_addr: SocketAddr = addr.parse()?;
    let raft_service = server::RaftTransportService {
        tx: tx.clone(),
        node_id: id,
    };
    let client_service = server::ClientApiService {
        tx,
        metrics: client_metrics,
    };

    let shutdown_id = id;
    let shutdown_signal = async move {
        let ctrl_c = async {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to install ctrl-c signal handler");
        };
        #[cfg(unix)]
        let terminate = async {
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler")
                .recv()
                .await;
        };
        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
        shutdown_metrics.begin_shutdown();
        raftdb_log::events::shutdown_started(shutdown_id);
    };

    let mut server_builder = tonic::transport::Server::builder();
    if let Some(cfg) = server_tls {
        server_builder = server_builder.tls_config(cfg)?;
        eprintln!(
            "node {id}: TLS enabled on {bind_addr} ({})",
            if mutual_tls {
                "mutual: client certificates required"
            } else {
                "server authentication only"
            }
        );
    }
    // A snapshot sent to a lagging or joining node travels as one message; the default 4 MiB limit
    // would reject any state larger than that and the node would never catch up.
    let raft_svc = proto::raft_transport_server::RaftTransportServer::new(raft_service)
        .max_decoding_message_size(kv::backup::MAX_MESSAGE_BYTES);
    let client_svc = proto::client_api_server::ClientApiServer::new(client_service)
        .max_encoding_message_size(kv::backup::MAX_MESSAGE_BYTES);
    type Serving =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), tonic::transport::Error>>>>;
    // Without --authz-file the server is built exactly as it always was (no extra layer).
    if let Some(handle) = &authz {
        authz::spawn_reload_on_sighup(handle.clone(), id);
    }
    let serving: Serving = match authz {
        Some(policy) => Box::pin(
            server_builder
                .layer(authz::AuthzLayer::new(policy, id, audit_all))
                .add_service(raft_svc)
                .add_service(client_svc)
                .serve_with_shutdown(bind_addr, shutdown_signal),
        ),
        None => Box::pin(
            server_builder
                .add_service(raft_svc)
                .add_service(client_svc)
                .serve_with_shutdown(bind_addr, shutdown_signal),
        ),
    };
    serving.await?;

    raftdb_log::events::shutdown_complete(id);
    Ok(())
}
