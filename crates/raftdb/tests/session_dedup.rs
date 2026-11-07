//! Exactly-once writes through the real binary: a retried (client_id, seq)
//! is acknowledged with the original result and never applied twice -- also
//! across kill -9, snapshot installation, and both storage engines.

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

struct Node(Option<Child>);
impl Node {
    fn start(dir: &Path, addr: &str, extra: &[&str]) -> Self {
        Node(Some(
            Command::new(BIN)
                .args(["start", "--id", "1", "--addr", addr, "--data-dir"])
                .arg(dir)
                .args(extra)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn raftdb"),
        ))
    }
    fn crash(&mut self) {
        if let Some(mut c) = self.0.take() {
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

async fn ready(addr: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10));
        if let Ok(ch) = ep.connect().await {
            let mut c = proto::client_api_client::ClientApiClient::new(ch);
            let ok = c
                .put(proto::PutRequest {
                    key: "ready".into(),
                    value: "1".into(),
                    client_id: 0,
                    seq: 0,
                })
                .await
                .map(|r| r.into_inner().ok)
                .unwrap_or(false);
            if ok {
                return c;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("node never became ready");
}

/// (ok, error)
async fn put(c: &mut Client, k: &str, v: &str, client_id: u64, seq: u64) -> (bool, String) {
    let r = c
        .put(proto::PutRequest {
            key: k.into(),
            value: v.into(),
            client_id,
            seq,
        })
        .await
        .unwrap()
        .into_inner();
    (r.ok, r.error)
}

async fn del(c: &mut Client, k: &str, client_id: u64, seq: u64) -> (bool, String) {
    let r = c
        .delete(proto::DeleteRequest {
            key: k.into(),
            client_id,
            seq,
        })
        .await
        .unwrap()
        .into_inner();
    (r.ok, r.error)
}

async fn get(c: &mut Client, k: &str) -> Option<String> {
    let r = c
        .get(proto::GetRequest { key: k.into() })
        .await
        .unwrap()
        .into_inner();
    r.found.then_some(r.value)
}

const ENGINES: [(&str, &[&str]); 2] = [("mem", &[]), ("lsm", &["--engine", "lsm"])];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retry_after_the_ack_is_acknowledged_but_not_applied_again() {
    for (name, flags) in ENGINES {
        let dir = tempfile::tempdir().unwrap();
        let addr = free_addr();
        let _node = Node::start(dir.path(), &addr, flags);
        let mut c = ready(&addr).await;

        assert_eq!(
            put(&mut c, "k", "a", 7, 1).await,
            (true, String::new()),
            "{name}"
        );
        assert!(
            put(&mut c, "k", "b", 0, 0).await.0,
            "{name}: plain overwrite"
        );
        // The client never saw the first ack, so it retries the same (7, 1).
        let (ok, err) = put(&mut c, "k", "a", 7, 1).await;
        assert!(
            ok,
            "{name}: retry must return the original success, got {err:?}"
        );
        assert_eq!(
            get(&mut c, "k").await.as_deref(),
            Some("b"),
            "{name}: the retry was applied a second time"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stale_sequence_is_rejected_loudly_and_not_applied() {
    for (name, flags) in ENGINES {
        let dir = tempfile::tempdir().unwrap();
        let addr = free_addr();
        let _node = Node::start(dir.path(), &addr, flags);
        let mut c = ready(&addr).await;

        assert!(put(&mut c, "k", "two", 7, 2).await.0, "{name}");
        let (ok, err) = put(&mut c, "k", "one", 7, 1).await;
        assert!(!ok, "{name}: a stale write must not be acknowledged");
        assert!(err.contains("superseded"), "{name}: {err}");
        assert_eq!(get(&mut c, "k").await.as_deref(), Some("two"), "{name}");
        // the session still works going forward
        assert!(put(&mut c, "k", "three", 7, 3).await.0, "{name}");
        assert_eq!(get(&mut c, "k").await.as_deref(), Some("three"), "{name}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deletes_dedupe_and_half_specified_sessions_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let addr = free_addr();
    let _node = Node::start(dir.path(), &addr, &[]);
    let mut c = ready(&addr).await;

    assert!(put(&mut c, "k", "v", 0, 0).await.0);
    assert!(del(&mut c, "k", 5, 1).await.0);
    assert!(put(&mut c, "k", "back", 0, 0).await.0);
    assert!(
        del(&mut c, "k", 5, 1).await.0,
        "retried delete acknowledged"
    );
    assert_eq!(
        get(&mut c, "k").await.as_deref(),
        Some("back"),
        "retried delete deleted a newer value"
    );

    for (cid, seq) in [(7, 0), (0, 3)] {
        let (ok, err) = put(&mut c, "x", "y", cid, seq).await;
        assert!(
            !ok && err.contains("both"),
            "({cid},{seq}) accepted or wrong error: {err}"
        );
    }
    assert_eq!(
        get(&mut c, "x").await,
        None,
        "a refused write must not be applied"
    );
}

/// The point of "durable": the retry arrives after the node was killed and
/// restarted, possibly after the log was compacted into a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dedup_survives_kill_9_restart_and_snapshots() {
    for (name, flags) in ENGINES {
        for threshold in ["1000000", "3"] {
            let dir = tempfile::tempdir().unwrap();
            let addr = free_addr();
            let mut args: Vec<&str> = flags.to_vec();
            args.extend(["--compaction-threshold", threshold]);

            let mut node = Node::start(dir.path(), &addr, &args);
            let mut c = ready(&addr).await;
            assert!(put(&mut c, "k", "a", 9, 1).await.0, "{name}/{threshold}");
            assert!(put(&mut c, "k", "b", 0, 0).await.0);
            // enough extra traffic from other clients to trigger compaction
            // when the threshold is small
            for i in 0..8u64 {
                assert!(put(&mut c, &format!("f{i}"), "x", 100 + i, 1).await.0);
            }
            node.crash();

            let _node = Node::start(dir.path(), &addr, &args);
            let mut c = ready(&addr).await;
            let (ok, err) = put(&mut c, "k", "a", 9, 1).await; // the late retry
            assert!(ok, "{name}/{threshold}: retry after restart: {err}");
            assert_eq!(
                get(&mut c, "k").await.as_deref(),
                Some("b"),
                "{name}/{threshold}: dedup state lost across kill -9; the retry was applied again"
            );
            // another client's retry (same id+seq, applied before the crash) is also a no-op
            let (ok, err) = put(&mut c, "k", "z", 100, 1).await;
            assert!(ok, "{name}/{threshold}: client 100's retry: {err}");
            assert_eq!(get(&mut c, "k").await.as_deref(), Some("b"));
        }
    }
}
