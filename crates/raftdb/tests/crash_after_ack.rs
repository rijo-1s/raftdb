//! Crash-after-ack durability, against the real `raftdb` binary.
//!
//! Contract under test: once a client has been told a write succeeded, that
//! write survives the process being killed immediately afterwards. The node
//! is SIGKILLed (no graceful shutdown, no flush hooks) the instant acks
//! arrive, restarted on the same data directory, and every write acknowledged
//! so far must read back with the right value.
//!
//! Rounds alternate between
//!   * a lone write (a batch of one: one fsync), killed right after its ack, and
//!   * a concurrent burst (real group commit: many writes share fsyncs),
//!     killed after the first few acks while the rest are still in flight.
//!
//! Writes that were NOT acknowledged may or may not survive; they are never
//! checked. Only acknowledged writes are.
//!
//! Limits: SIGKILL does not drop the OS page cache, so this proves
//! "acked => written out of the process", i.e. no ack before the WAL write and
//! nothing lost to in-process buffering or batching. It cannot detect a
//! missing fsync (that needs power-loss simulation, which the storage crate's
//! fault-injection tests cover).

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use raftdb_proto as proto;

const BIN: &str = env!("CARGO_BIN_EXE_raftdb");
type Client = proto::client_api_client::ClientApiClient<tonic::transport::Channel>;

fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("127.0.0.1:{}", l.local_addr().unwrap().port())
}

struct Node {
    child: Option<Child>,
}

impl Node {
    fn start(dir: &Path, addr: &str, extra: &[&str]) -> Self {
        let child = Command::new(BIN)
            .args(["start", "--id", "1", "--addr", addr, "--data-dir"])
            .arg(dir)
            .args(extra)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn raftdb");
        Node { child: Some(child) }
    }
    /// SIGKILL and reap.
    fn crash(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.crash();
    }
}

async fn connect(addr: &str) -> Option<Client> {
    let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .ok()?
        .connect_timeout(Duration::from_secs(2))
        .timeout(Duration::from_secs(10));
    let ch = ep.connect().await.ok()?;
    Some(proto::client_api_client::ClientApiClient::new(ch))
}

/// Wait until the (single-node) cluster elects itself and serves writes.
async fn wait_ready(addr: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(mut c) = connect(addr).await {
            let r = c
                .put(proto::PutRequest {
                    key: "ready".into(),
                    value: "1".into(),
                    client_id: 0,
                    seq: 0,
                })
                .await;
            if r.map(|r| r.into_inner().ok).unwrap_or(false) {
                return c;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node never became ready");
}

async fn put(c: &mut Client, k: &str, v: &str) -> bool {
    c.put(proto::PutRequest {
        key: k.into(),
        value: v.into(),
        client_id: 0,
        seq: 0,
    })
    .await
    .map(|r| r.into_inner().ok)
    .unwrap_or(false)
}

/// Every acknowledged write must be present with its value.
async fn check_all(c: &mut Client, acked: &BTreeMap<String, String>, ctx: &str) {
    for (k, v) in acked {
        let r = c
            .get(proto::GetRequest { key: k.clone() })
            .await
            .unwrap_or_else(|e| panic!("{ctx}: get {k}: {e}"))
            .into_inner();
        assert!(
            r.found && &r.value == v,
            "{ctx}: ACKNOWLEDGED write {k}={v} was lost or wrong after crash (found={}, value={:?})",
            r.found,
            r.value
        );
    }
}

async fn run(engine_flags: &[&str], label: &str) {
    const ROUNDS: usize = 6;
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let mut acked: BTreeMap<String, String> = BTreeMap::new();

    for round in 0..ROUNDS {
        let mut node = Node::start(dir.path(), &addr, engine_flags);
        let mut c = wait_ready(&addr).await;
        // Everything acknowledged before the previous crash must be here.
        check_all(
            &mut c,
            &acked,
            &format!("{label} round {round} after restart"),
        )
        .await;

        if round % 2 == 0 {
            // Lone write: acked, then killed immediately.
            let (k, v) = (format!("solo-{round}"), format!("val-{round}"));
            assert!(put(&mut c, &k, &v).await, "{label}: solo write not acked");
            node.crash();
            acked.insert(k, v);
        } else {
            // Concurrent burst => real group commit. Kill after the first
            // few acks, with the rest still in flight.
            let mut set = tokio::task::JoinSet::new();
            for i in 0..32 {
                let addr = addr.clone();
                let (k, v) = (format!("burst-{round}-{i}"), format!("bval-{round}-{i}"));
                set.spawn(async move {
                    let ok = match connect(&addr).await {
                        Some(mut c) => put(&mut c, &k, &v).await,
                        None => false,
                    };
                    (k, v, ok)
                });
            }
            let mut got = 0;
            while let Some(j) = set.join_next().await {
                if let Ok((k, v, true)) = j {
                    acked.insert(k, v);
                    got += 1;
                    if got == 6 {
                        node.crash(); // writes still in flight are simply not counted
                        break;
                    }
                }
            }
            node.crash();
            assert!(got >= 1, "{label}: burst produced no acks");
        }
    }

    // Final restart: verify the whole history once more.
    let mut node = Node::start(dir.path(), &addr, engine_flags);
    let mut c = wait_ready(&addr).await;
    check_all(&mut c, &acked, &format!("{label} final")).await;
    assert!(acked.len() >= ROUNDS, "{label}: too few acks recorded");
    node.crash();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acked_writes_survive_a_crash_right_after_the_ack_mem_engine() {
    run(&[], "mem").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acked_writes_survive_a_crash_right_after_the_ack_lsm_sync_wal() {
    run(&["--engine", "lsm", "--lsm-sync-wal"], "lsm+sync").await;
}
