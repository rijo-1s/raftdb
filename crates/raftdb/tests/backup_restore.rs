//! Backup and restore through the real binary (same harness pattern as
//! `session_dedup.rs`): a consistent point-in-time export taken inside the
//! node loop, and an offline restore into a new single-node cluster.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kv::backup::Backup;
use kv::KvStore;
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
    fn stop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn connect(addr: &str) -> Client {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let ep = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(20));
        if let Ok(ch) = ep.connect().await {
            return proto::client_api_client::ClientApiClient::new(ch)
                .max_decoding_message_size(kv::backup::MAX_MESSAGE_BYTES);
        }
        assert!(Instant::now() < deadline, "cannot connect to {addr}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

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

async fn del(c: &mut Client, k: &str) -> bool {
    c.delete(proto::DeleteRequest {
        key: k.into(),
        client_id: 0,
        seq: 0,
    })
    .await
    .unwrap()
    .into_inner()
    .ok
}

async fn get(c: &mut Client, k: &str) -> Option<String> {
    let r = c
        .get(proto::GetRequest { key: k.into() })
        .await
        .unwrap()
        .into_inner();
    r.found.then_some(r.value)
}

/// Wait until the node serves a linearizable read (i.e. it is leader) without
/// writing anything, so the log is untouched.
async fn wait_leader(addr: &str, probe_key: &str) -> Client {
    let mut c = connect(addr).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(r) = c
            .get(proto::GetRequest {
                key: probe_key.into(),
            })
            .await
        {
            if r.into_inner().error.is_empty() {
                return c;
            }
        }
        assert!(Instant::now() < deadline, "node {addr} never became leader");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The node writes its own "ready" key; used where an initial write is fine.
async fn ready(addr: &str) -> Client {
    let mut c = connect(addr).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while !put(&mut c, "ready", "1", 0, 0).await.0 {
        assert!(Instant::now() < deadline, "node never became ready");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    c
}

/// A backup over the RPC: (decoded backup, its parsed state).
async fn rpc_backup(c: &mut Client) -> (Backup, KvStore) {
    let r = c
        .backup(proto::BackupRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(r.ok, "backup refused: {}", r.error);
    let b = Backup::decode(&r.data).expect("the node produced a valid backup");
    assert_eq!(b.last_applied, r.last_applied);
    let s = b.check().expect("and a bootable one");
    let _ = s;
    let st = KvStore::restore(&b.state).unwrap();
    (b, st)
}

fn cli(args: &[&str]) -> (bool, String) {
    let o = Command::new(BIN).args(args).output().unwrap();
    (
        o.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        ),
    )
}

fn all_keys(s: &KvStore) -> BTreeMap<String, String> {
    s.keys()
        .map(|k| (k.clone(), s.get(k).unwrap().clone()))
        .collect()
}

fn listing(dir: &Path) -> Vec<String> {
    let mut v: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    v.sort();
    v
}

const ENGINES: [(&str, &[&str]); 2] = [("mem", &[]), ("lsm", &["--engine", "lsm"])];

// ---------------------------------------------------------------------
// (a) a backup taken while writes are in flight IS the state at its index
// ---------------------------------------------------------------------

/// Two sequential writers hammer the node:
///  * `k00..k39`: write i sets key `k{i%40}` = i (overwrites)
///  * `w/{i}`: write i sets `w/{i:06}` = i (never overwritten)
///
/// A snapshot of the state at ANY log index must therefore look like "the
/// first N k-writes and the first n w-writes" for some N, n, with no holes,
/// and since every write is exactly one log entry, `last_applied` must equal
/// `baseline + N + n`. A backup that mixed two points in time, or that was
/// not taken between applies, cannot satisfy both.
fn check_consistent(b: &Backup, st: &KvStore, baseline: u64) -> (u64, u64) {
    let all = all_keys(st);
    let k: BTreeMap<_, _> = all
        .iter()
        .filter(|(k, _)| k.len() == 3 && k.starts_with('k'))
        .collect();
    let n_k = k
        .values()
        .map(|v| v.parse::<u64>().unwrap())
        .max()
        .unwrap_or(0);
    let mut expect = BTreeMap::new();
    for i in 1..=n_k {
        expect.insert(format!("k{:02}", i % 40), i.to_string());
    }
    let got: BTreeMap<_, _> = k
        .iter()
        .map(|(a, b)| ((*a).clone(), (*b).clone()))
        .collect();
    assert_eq!(
        got, expect,
        "the k-keys are not the result of the first {n_k} writes in order"
    );

    let w: Vec<u64> = all
        .keys()
        .filter_map(|k| k.strip_prefix("w/"))
        .map(|n| n.parse().unwrap())
        .collect();
    let n_w = w.len() as u64;
    assert_eq!(
        w.iter().copied().max().unwrap_or(0),
        n_w,
        "hole in the w-writes: {} keys but max {:?}",
        n_w,
        w.iter().max()
    );
    for (kk, vv) in all.iter().filter(|(k, _)| k.starts_with("w/")) {
        assert_eq!(kk, &format!("w/{vv:0>6}"), "a w-key holds the wrong value");
    }
    assert_eq!(
        b.last_applied,
        baseline + n_k + n_w,
        "last_applied {} != baseline {baseline} + {n_k} k-writes + {n_w} w-writes",
        b.last_applied
    );
    (n_k, n_w)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_taken_during_writes_is_exactly_the_state_at_its_recorded_index() {
    for (name, flags) in ENGINES {
        let dir = tempfile::tempdir().unwrap();
        let addr = free_addr();
        // A tiny compaction threshold: Raft snapshots happen constantly during the test.
        let mut args = vec!["--compaction-threshold", "10"];
        args.extend_from_slice(flags);
        let _node = Node::start(1, dir.path(), &addr, &args);
        let mut c = ready(&addr).await;
        let (b0, s0) = rpc_backup(&mut c).await;
        let baseline = b0.last_applied; // everything that is not a counted write
        assert_eq!(check_consistent(&b0, &s0, baseline), (0, 0));

        let stop = Arc::new(AtomicBool::new(false));
        let spawn_writer = |kind: char| {
            let (mut wc, stop) = (c.clone(), stop.clone());
            tokio::spawn(async move {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let (key, val) = match kind {
                        'k' => (format!("k{:02}", i % 40), i.to_string()),
                        _ => (format!("w/{i:06}"), i.to_string()),
                    };
                    assert!(put(&mut wc, &key, &val, 0, 0).await.0, "write {i} failed");
                }
                i
            })
        };
        let (wk, ww) = (spawn_writer('k'), spawn_writer('w'));

        let mut last = baseline;
        let mut seen_progress = false;
        for round in 0..6 {
            tokio::time::sleep(Duration::from_millis(150)).await;
            let (b, st) = rpc_backup(&mut c).await;
            let (n_k, n_w) = check_consistent(&b, &st, baseline);
            assert!(
                b.last_applied >= last,
                "{name}: backup went backwards in time"
            );
            seen_progress |= n_k + n_w > 0 && b.last_applied > last;
            last = b.last_applied;
            let _ = round;
        }
        stop.store(true, Ordering::Relaxed);
        let (acked_k, acked_w) = (wk.await.unwrap(), ww.await.unwrap());
        assert!(
            seen_progress,
            "{name}: no writes landed between backups; the test proved nothing"
        );

        // Quiesced: every acknowledged write is in the next backup.
        let (b, st) = rpc_backup(&mut c).await;
        let (n_k, n_w) = check_consistent(&b, &st, baseline);
        assert_eq!(
            (n_k, n_w),
            (acked_k, acked_w),
            "{name}: an acknowledged write is missing from the backup"
        );
    }
}

// ---------------------------------------------------------------------
// shared scenario for (b) (c) (d): a node with data, a session and a delete
// ---------------------------------------------------------------------

struct Source {
    _tmp: tempfile::TempDir,
    backup: PathBuf,
    /// The full expected key space at backup time.
    expected: BTreeMap<String, String>,
    last_applied: u64,
}

async fn make_source(engine_flags: &[&str]) -> Source {
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path().join("src");
    let addr = free_addr();
    let mut node = Node::start(1, &data, &addr, engine_flags);
    let mut c = ready(&addr).await;
    let mut expected = BTreeMap::new();
    expected.insert("ready".to_string(), "1".to_string());
    for i in 1..=30 {
        assert!(
            put(&mut c, &format!("k{i:02}"), &format!("v{i}"), 0, 0)
                .await
                .0
        );
        expected.insert(format!("k{i:02}"), format!("v{i}"));
    }
    assert!(del(&mut c, "k05").await);
    expected.remove("k05");
    // A write acknowledged to session (7, 1), then overwritten by someone else.
    assert!(put(&mut c, "sk", "a", 7, 1).await.0);
    assert!(put(&mut c, "sk", "b", 0, 0).await.0);
    expected.insert("sk".into(), "b".into());

    let file = tmp.path().join("b.rdbbak");
    let (ok, out) = cli(&["backup", "--addr", &addr, "--out", file.to_str().unwrap()]);
    assert!(ok, "backup CLI failed: {out}");
    let (b, _) = (Backup::decode(&std::fs::read(&file).unwrap()).unwrap(), ());
    node.stop();
    Source {
        _tmp: tmp,
        backup: file,
        expected,
        last_applied: b.last_applied,
    }
}

// ---------------------------------------------------------------------
// (b) (c) (d) restore, across every source/target engine combination
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_brings_back_every_key_the_state_and_the_exactly_once_sessions() {
    for (src_name, src_flags) in ENGINES {
        let src = make_source(src_flags).await;
        for (dst_name, dst_flags) in ENGINES {
            let label = format!("{src_name} backup -> {dst_name} restore");
            let tmp = tempfile::tempdir().unwrap();
            let dir = tmp.path().join("restored");
            let (ok, out) = cli(&[
                "restore",
                "--backup",
                src.backup.to_str().unwrap(),
                "--data-dir",
                dir.to_str().unwrap(),
                "--node-id",
                "9",
            ]);
            assert!(ok, "{label}: restore failed: {out}");
            // The engine directory is NOT restored: the node rebuilds it from the Raft snapshot.
            assert!(
                !dir.join("lsm").exists(),
                "{label}: restore must leave the engine directory empty"
            );

            let addr = free_addr();
            let mut args = vec![];
            args.extend_from_slice(dst_flags);
            let _node = Node::start(9, &dir, &addr, &args);
            // Wait for leadership WITHOUT writing anything: the log stays exactly as restored.
            let mut c = wait_leader(&addr, "k01").await;

            // (d) applied state matches the backup: a fresh backup of the restored node has the same
            //     contents, the same sessions, and is exactly one entry (the new term's no-op) ahead.
            let (b2, st2) = rpc_backup(&mut c).await;
            assert_eq!(
                all_keys(&st2),
                src.expected,
                "{label}: the restored state differs from the backup"
            );
            assert_eq!(
                b2.last_applied,
                src.last_applied + 1,
                "{label}: applied index is not the backup's + the election no-op"
            );
            assert_eq!(
                b2.peers,
                vec![9],
                "{label}: a restored node is a single-node cluster"
            );
            assert_eq!(
                st2.session_last_seq(7),
                Some(1),
                "{label}: the session table was not restored"
            );

            // (b) every key readable, deletes stayed deleted
            for (k, v) in &src.expected {
                assert_eq!(
                    get(&mut c, k).await.as_deref(),
                    Some(v.as_str()),
                    "{label}: key {k}"
                );
            }
            assert_eq!(
                get(&mut c, "k05").await,
                None,
                "{label}: a deleted key came back"
            );

            // (c) the retry of the pre-backup session write is acknowledged but NOT applied again
            let (ok, err) = put(&mut c, "sk", "a", 7, 1).await;
            assert!(
                ok,
                "{label}: the retry must return the original success, got {err:?}"
            );
            assert_eq!(
                get(&mut c, "sk").await.as_deref(),
                Some("b"),
                "{label}: the retry was APPLIED AGAIN after restore"
            );
            // ...while the session continues normally
            assert!(put(&mut c, "sk", "c", 7, 2).await.0, "{label}");
            assert_eq!(get(&mut c, "sk").await.as_deref(), Some("c"), "{label}");
        }
    }
}

// ---------------------------------------------------------------------
// (d) a corrupt backup is refused and nothing is left behind
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_corrupt_backup_is_refused_and_leaves_the_target_untouched() {
    let src = make_source(&[]).await;
    let good = std::fs::read(&src.backup).unwrap();
    let work = tempfile::tempdir().unwrap();
    let bad = work.path().join("bad.rdbbak");
    let (outputs_before, mut refused) = (listing(work.path()), 0);
    let _ = outputs_before;

    // Every 3rd byte flipped, and truncations at many lengths, each through the real CLI.
    let mut cases: Vec<Vec<u8>> = Vec::new();
    for i in (0..good.len()).step_by(3) {
        let mut b = good.clone();
        b[i] ^= 0x01;
        cases.push(b);
    }
    for cut in (0..good.len()).step_by(7) {
        cases.push(good[..cut].to_vec());
    }
    let mut extended = good.clone();
    extended.push(0);
    cases.push(extended);

    for (n, bytes) in cases.iter().enumerate() {
        std::fs::write(&bad, bytes).unwrap();
        // (1) target does not exist: it must still not exist, and no staging directory remains
        let missing = work.path().join("target");
        let before = listing(work.path());
        let (ok, out) = cli(&[
            "restore",
            "--backup",
            bad.to_str().unwrap(),
            "--data-dir",
            missing.to_str().unwrap(),
            "--node-id",
            "3",
        ]);
        assert!(!ok, "case {n}: a damaged backup was restored: {out}");
        assert_eq!(
            listing(work.path()),
            before,
            "case {n}: a rejected restore left something behind"
        );
        // (2) target is an existing EMPTY directory: still empty
        let empty = work.path().join("empty");
        std::fs::create_dir_all(&empty).unwrap();
        let (ok, _) = cli(&[
            "restore",
            "--backup",
            bad.to_str().unwrap(),
            "--data-dir",
            empty.to_str().unwrap(),
            "--node-id",
            "3",
        ]);
        assert!(!ok);
        assert!(
            listing(&empty).is_empty(),
            "case {n}: the target directory was modified"
        );
        std::fs::remove_dir(&empty).unwrap();
        refused += 1;
    }
    assert!(refused > 30, "too few corruptions exercised: {refused}");

    // A non-empty target is refused even for a perfectly good backup, and is not touched.
    let full = work.path().join("full");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::write(full.join("precious"), b"do not touch").unwrap();
    let (ok, out) = cli(&[
        "restore",
        "--backup",
        src.backup.to_str().unwrap(),
        "--data-dir",
        full.to_str().unwrap(),
        "--node-id",
        "3",
    ]);
    assert!(!ok && out.contains("not empty"), "{out}");
    assert_eq!(listing(&full), vec!["precious"]);
    assert_eq!(
        std::fs::read(full.join("precious")).unwrap(),
        b"do not touch"
    );

    // And the good backup still restores (the refusals did not poison anything).
    let ok_dir = work.path().join("ok");
    let (ok, out) = cli(&[
        "restore",
        "--backup",
        src.backup.to_str().unwrap(),
        "--data-dir",
        ok_dir.to_str().unwrap(),
        "--node-id",
        "3",
    ]);
    assert!(ok, "{out}");
}

#[test]
fn backup_never_overwrites_an_existing_file_and_a_dead_node_leaves_no_file() {
    let work = tempfile::tempdir().unwrap();
    let out = work.path().join("b.rdbbak");
    std::fs::write(&out, b"precious").unwrap();
    let (ok, msg) = cli(&[
        "backup",
        "--addr",
        &free_addr(),
        "--out",
        out.to_str().unwrap(),
    ]);
    assert!(!ok && msg.contains("already exists"), "{msg}");
    assert_eq!(std::fs::read(&out).unwrap(), b"precious");
    let fresh = work.path().join("fresh.rdbbak");
    let (ok, _) = cli(&[
        "backup",
        "--addr",
        &free_addr(),
        "--out",
        fresh.to_str().unwrap(),
    ]);
    assert!(!ok, "nothing is listening on that address");
    assert_eq!(
        listing(work.path()),
        vec!["b.rdbbak"],
        "a failed backup left a file or a temp file behind"
    );
}

// ---------------------------------------------------------------------
// (e) the restored node takes writes and can grow into a cluster again
// ---------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_restored_node_accepts_writes_and_a_second_node_can_be_added() {
    let src = make_source(&["--engine", "lsm"]).await;
    let tmp = tempfile::tempdir().unwrap();
    let dir1 = tmp.path().join("r1");
    let (ok, out) = cli(&[
        "restore",
        "--backup",
        src.backup.to_str().unwrap(),
        "--data-dir",
        dir1.to_str().unwrap(),
        "--node-id",
        "1",
    ]);
    assert!(ok, "{out}");

    // Fresh ports, never the old cluster's addresses.
    let (a1, a2) = (free_addr(), free_addr());
    let _n1 = Node::start(
        1,
        &dir1,
        &a1,
        &["--engine", "lsm", "--known-peers", &format!("2={a2}")],
    );
    let mut c1 = wait_leader(&a1, "k01").await;

    // New writes work on the restored single node.
    assert!(put(&mut c1, "after-restore", "yes", 0, 0).await.0);
    assert_eq!(get(&mut c1, "after-restore").await.as_deref(), Some("yes"));

    // Add a second node with the ordinary procedure. In a one-node cluster the leader is its own
    // quorum, so the AddNode entry commits at once: no write is needed first.
    let dir2 = tmp.path().join("n2");
    let _n2 = Node::start(2, &dir2, &a2, &["--join", "--peers", &format!("1={a1}")]);
    let _ = connect(&a2).await;
    let mut adder = c1.clone();
    let add = tokio::spawn(async move {
        adder
            .add_node(proto::AddNodeRequest { node_id: 2 })
            .await
            .unwrap()
            .into_inner()
    });
    let r = tokio::time::timeout(Duration::from_secs(20), add)
        .await
        .expect("add-node never completed")
        .unwrap();
    assert!(r.ok, "add-node failed: {}", r.error);
    assert!(
        put(&mut c1, "after-add", "two-nodes", 0, 0).await.0,
        "a 2-node cluster must commit with both members"
    );

    // The new node holds the restored data AND everything written since
    // (read through a BACKUP of the follower: also shows backups work on followers).
    let mut c2 = connect(&a2).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(r) = c2.backup(proto::BackupRequest {}).await {
            let r = r.into_inner();
            if r.ok {
                let st = KvStore::restore(&Backup::decode(&r.data).unwrap().state).unwrap();
                let mut want = src.expected.clone();
                want.insert("after-restore".into(), "yes".into());
                want.insert("after-add".into(), "two-nodes".into());
                if all_keys(&st) == want {
                    assert_eq!(st.session_last_seq(7), Some(1));
                    break;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "node 2 never converged to the restored data plus new writes"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
