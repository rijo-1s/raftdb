//! A node that joins (or falls far behind) is brought up to date with a snapshot of the whole
//! state. This must work when the state is larger than gRPC's default 4 MiB message limit: a node
//! that cannot receive the snapshot never catches up, and in a 2-node cluster the next write then
//! blocks forever because the new node is needed for the quorum.

use std::path::{Path, PathBuf};
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
    fn start(id: u64, dir: &Path, addr: &str, extra: &[&str]) -> Self {
        Node(Some(
            Command::new(BIN)
                .args([
                    "start",
                    "--id",
                    &id.to_string(),
                    "--addr",
                    addr,
                    "--data-dir",
                ])
                .arg(dir)
                .args(extra)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn raftdb"),
        ))
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

async fn connect(addr: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(60));
        if let Ok(ch) = ep.connect().await {
            return proto::client_api_client::ClientApiClient::new(ch);
        }
        assert!(Instant::now() < deadline, "cannot connect to {addr}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn put(c: &mut Client, k: &str, v: String) -> bool {
    let r = c
        .put(proto::PutRequest {
            key: k.into(),
            value: v,
            client_id: 0,
            seq: 0,
        })
        .await
        .unwrap()
        .into_inner();
    if !r.ok && k != "warmup" {
        eprintln!("put {k} refused: {}", r.error);
    }
    r.ok
}

/// Grows the state to `big_mib` MiB (in ~1 MB values (a command may be at most 1 MiB)), then writes enough small entries that the
/// log is compacted (every 50 applied entries) so a new node can only be served a snapshot.
async fn wait_for_leader(c: &mut Client) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !put(c, "warmup", "1".into()).await {
        assert!(Instant::now() < deadline, "no leader elected");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn load(c: &mut Client, big_mib: usize) {
    wait_for_leader(c).await;
    for i in 0..big_mib {
        assert!(put(c, &format!("big-{i}"), "x".repeat(1_000_000)).await);
    }
    for i in 0..150 {
        assert!(put(c, &format!("small-{i}"), format!("v{i}")).await);
    }
}

async fn scenario(extra: &[&str], big_mib: usize) {
    let tmp: PathBuf = tempfile::tempdir().unwrap().into_path();
    let (a1, a2) = (free_addr(), free_addr());
    let mut n1_args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    n1_args.extend(["--known-peers".into(), format!("2={a2}")]);
    let n1_refs: Vec<&str> = n1_args.iter().map(|s| s.as_str()).collect();
    let _n1 = Node::start(1, &tmp.join("n1"), &a1, &n1_refs);
    let mut c1 = connect(&a1).await;
    load(&mut c1, big_mib).await;

    let mut n2_args: Vec<String> = extra.iter().map(|s| s.to_string()).collect();
    n2_args.extend(["--join".into(), "--peers".into(), format!("1={a1}")]);
    let n2_refs: Vec<&str> = n2_args.iter().map(|s| s.as_str()).collect();
    let _n2 = Node::start(2, &tmp.join("n2"), &a2, &n2_refs);
    let _ = connect(&a2).await;

    let r = tokio::time::timeout(
        Duration::from_secs(20),
        c1.add_node(proto::AddNodeRequest { node_id: 2 }),
    )
    .await
    .expect("add-node never completed")
    .unwrap()
    .into_inner();
    assert!(r.ok, "add-node failed: {}", r.error);

    // With two members the quorum is two: this write commits only once node 2 has the snapshot.
    let wrote = tokio::time::timeout(
        Duration::from_secs(40),
        put(&mut c1, "after-join", "1".into()),
    )
    .await
    .unwrap_or_else(|_| {
        panic!("node 2 never caught up: a {big_mib} MiB snapshot could not be installed")
    });
    assert!(
        wrote,
        "node 2 never caught up: the write after joining timed out, so a {big_mib} MB snapshot could not be installed"
    );
    let _ = std::fs::remove_dir_all(&tmp);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_node_catches_up_from_a_snapshot_larger_than_the_default_grpc_limit() {
    scenario(&[], 6).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_same_holds_for_the_disk_backed_lsm_engine() {
    scenario(&["--engine", "lsm"], 6).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn and_for_a_much_larger_state() {
    scenario(&[], 40).await;
}
