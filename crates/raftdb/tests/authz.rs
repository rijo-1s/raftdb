//! Role-based authorization through the real binary: a 3-node mutual-TLS cluster whose
//! `--authz-file` lists a node certificate and one certificate per client role. Checks the whole
//! matrix of role x method, that unlisted certificates are refused, that client certificates can
//! never speak the Raft protocol, and that bad configurations stop the node.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use rcgen::{BasicConstraints, Certificate, CertificateParams, DnType, IsCa, SanType};

const BIN: &str = env!("CARGO_BIN_EXE_raftdb");

fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    format!("127.0.0.1:{}", l.local_addr().unwrap().port())
}

fn make_ca(name: &str) -> Certificate {
    let mut p = CertificateParams::new(Vec::<String>::new());
    p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    p.distinguished_name.push(DnType::CommonName, name);
    Certificate::from_params(p).unwrap()
}

fn s(p: &Path) -> String {
    p.to_str().unwrap().to_string()
}

struct Identity {
    cert: PathBuf,
    key: PathBuf,
    fingerprint: String,
}

struct Pki {
    dir: tempfile::TempDir,
    ca: PathBuf,
    node: Identity,
    admin: Identity,
    rw: Identity,
    ro: Identity,
    unlisted: Identity,
}

/// SHA-256 of the certificate as written to the PEM file (what the server will see).
fn fingerprint_of_pem(pem: &str) -> String {
    let body: String = pem.lines().filter(|l| !l.starts_with("-----")).collect();
    let der = base64::engine::general_purpose::STANDARD
        .decode(body)
        .unwrap();
    use std::fmt::Write;
    ring::digest::digest(&ring::digest::SHA256, &der)
        .as_ref()
        .iter()
        .fold(String::new(), |mut out, b| {
            let _ = write!(out, "{b:02x}");
            out
        })
}

fn pki() -> Pki {
    let dir = tempfile::tempdir().unwrap();
    let ca = make_ca("raftdb authz test CA");
    let write = |name: &str, body: &str| {
        let p = dir.path().join(name);
        std::fs::write(&p, body).unwrap();
        p
    };
    let leaf = |cn: &str| -> Identity {
        let mut p = CertificateParams::new(vec!["localhost".to_string()]);
        p.subject_alt_names
            .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
        p.distinguished_name.push(DnType::CommonName, cn);
        let c = Certificate::from_params(p).unwrap();
        let pem = c.serialize_pem_with_signer(&ca).unwrap();
        Identity {
            fingerprint: fingerprint_of_pem(&pem),
            cert: write(&format!("{cn}.pem"), &pem),
            key: write(&format!("{cn}.key"), &c.serialize_private_key_pem()),
        }
    };
    let ca_path = write("ca.pem", &ca.serialize_pem().unwrap());
    Pki {
        node: leaf("node"),
        admin: leaf("admin"),
        rw: leaf("rw"),
        ro: leaf("ro"),
        unlisted: leaf("unlisted"),
        ca: ca_path,
        dir,
    }
}

fn authz_file(p: &Pki) -> PathBuf {
    let body = format!(
        "# test policy\nnode {}  all nodes\nadmin {}  alice\nread-write {}  app\nread-only {}  dashboard\n",
        p.node.fingerprint, p.admin.fingerprint, p.rw.fingerprint, p.ro.fingerprint
    );
    let path = p.dir.path().join("authz.txt");
    std::fs::write(&path, body).unwrap();
    path
}

struct Cluster {
    nodes: Vec<Child>,
    addrs: Vec<String>,
    _data: tempfile::TempDir,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for c in &mut self.nodes {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
impl Cluster {
    fn cluster_arg(&self) -> String {
        self.addrs
            .iter()
            .enumerate()
            .map(|(i, a)| format!("{}={a}", i + 1))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn node_flags(p: &Pki) -> Vec<String> {
    vec![
        "--tls-cert".into(),
        s(&p.node.cert),
        "--tls-key".into(),
        s(&p.node.key),
        "--tls-ca".into(),
        s(&p.ca),
        "--tls-require-client-cert".into(),
        "--authz-file".into(),
        s(&authz_file(p)),
    ]
}

fn start_cluster(flags: &[String]) -> Cluster {
    let data = tempfile::tempdir().unwrap();
    let addrs: Vec<String> = (0..3).map(|_| free_addr()).collect();
    let mut nodes = Vec::new();
    for i in 0..3usize {
        let id = i + 1;
        let peers = (0..3usize)
            .filter(|j| *j != i)
            .map(|j| format!("{}={}", j + 1, addrs[j]))
            .collect::<Vec<_>>()
            .join(",");
        let dir = data.path().join(format!("n{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        nodes.push(
            Command::new(BIN)
                .args([
                    "start",
                    "--id",
                    &id.to_string(),
                    "--addr",
                    &addrs[i],
                    "--peers",
                    &peers,
                    "--data-dir",
                ])
                .arg(&dir)
                .args(flags)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn raftdb"),
        );
    }
    Cluster {
        nodes,
        addrs,
        _data: data,
    }
}

fn who(p: &Pki, id: &Identity) -> Vec<String> {
    vec![
        "--tls-ca".into(),
        s(&p.ca),
        "--tls-cert".into(),
        s(&id.cert),
        "--tls-key".into(),
        s(&id.key),
    ]
}

fn cli(c: &Cluster, sub: &str, flags: &[String], tail: &[&str]) -> Output {
    Command::new(BIN)
        .arg(sub)
        .args(["--cluster", &c.cluster_arg()])
        .args(flags)
        .args(tail)
        .output()
        .expect("run raftdb cli")
}

fn denied(o: &Output) -> bool {
    !o.status.success() && String::from_utf8_lossy(&o.stderr).contains("PermissionDenied")
}

fn text(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).trim().to_string()
}

fn wait_until_serving(c: &Cluster, flags: &[String]) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let out = cli(c, "put", flags, &["warmup", "1"]);
        if out.status.success() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster never became ready; last error: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_role_can_do_exactly_what_it_is_allowed_to_and_nothing_else() {
    let p = pki();
    let c = start_cluster(&node_flags(&p));
    let admin = who(&p, &p.admin);
    let rw = who(&p, &p.rw);
    let ro = who(&p, &p.ro);
    // The cluster only forms if the node certificate is accepted on the Raft transport.
    wait_until_serving(&c, &admin);

    // --- admin: everything ------------------------------------------------------------------
    assert!(cli(&c, "put", &admin, &["k", "v1"]).status.success());
    assert_eq!(text(&cli(&c, "get", &admin, &["k"])), "v1");
    let rm = cli(&c, "remove-node", &admin, &["99"]);
    assert!(
        !denied(&rm),
        "admin must be allowed to call remove-node (it may still fail for other reasons)"
    );
    let dir = tempfile::tempdir().unwrap();
    let b = Command::new(BIN)
        .args(["backup", "--addr", &c.addrs[0], "--out"])
        .arg(dir.path().join("a.bak"))
        .args(&admin)
        .output()
        .unwrap();
    assert!(
        b.status.success(),
        "admin backup failed: {}",
        String::from_utf8_lossy(&b.stderr)
    );

    // --- read-write: data only ---------------------------------------------------------------
    assert!(cli(&c, "put", &rw, &["k", "v2"]).status.success());
    assert_eq!(text(&cli(&c, "get", &rw, &["k"])), "v2");
    assert!(cli(&c, "delete", &rw, &["k"]).status.success());
    assert!(
        denied(&cli(&c, "remove-node", &rw, &["99"])),
        "read-write must not remove nodes"
    );
    assert!(
        denied(&cli(&c, "add-node", &rw, &["99"])),
        "read-write must not add nodes"
    );
    let b = Command::new(BIN)
        .args(["backup", "--addr", &c.addrs[0], "--out"])
        .arg(dir.path().join("rw.bak"))
        .args(&rw)
        .output()
        .unwrap();
    assert!(!b.status.success() && String::from_utf8_lossy(&b.stderr).contains("PermissionDenied"));
    assert!(
        !dir.path().join("rw.bak").exists(),
        "a refused backup must leave no file"
    );

    // --- read-only: reads only ---------------------------------------------------------------
    assert!(cli(&c, "put", &admin, &["r", "x"]).status.success());
    assert_eq!(text(&cli(&c, "get", &ro, &["r"])), "x");
    assert!(
        denied(&cli(&c, "put", &ro, &["r", "hacked"])),
        "read-only must not write"
    );
    assert!(
        denied(&cli(&c, "delete", &ro, &["r"])),
        "read-only must not delete"
    );
    assert!(denied(&cli(&c, "remove-node", &ro, &["99"])));
    assert_eq!(
        text(&cli(&c, "get", &admin, &["r"])),
        "x",
        "the refused write must not have happened"
    );

    // --- a certificate the CA signed but the file does not list: nothing -------------------------
    let unlisted = who(&p, &p.unlisted);
    let g = cli(&c, "get", &unlisted, &["r"]);
    assert!(
        denied(&g),
        "an unlisted certificate must be refused: {}",
        String::from_utf8_lossy(&g.stderr)
    );
    assert!(denied(&cli(&c, "put", &unlisted, &["u", "x"])));

    // --- a node certificate never gets the client API -----------------------------------------
    let as_node = who(&p, &p.node);
    assert!(
        denied(&cli(&c, "get", &as_node, &["r"])),
        "a node certificate must not use the client API"
    );
    assert!(denied(&cli(&c, "put", &as_node, &["n", "x"])));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_certificate_cannot_speak_the_raft_protocol() {
    use raftdb_proto::raft_transport_client::RaftTransportClient;
    use raftdb_proto::RequestVoteRequest;
    use tonic::transport::{Certificate as TCert, ClientTlsConfig, Endpoint, Identity as TId};

    let p = pki();
    let c = start_cluster(&node_flags(&p));
    let admin = who(&p, &p.admin);
    wait_until_serving(&c, &admin);
    assert!(cli(&c, "put", &admin, &["before", "1"]).status.success());

    // Even the strongest client role (admin) is refused on the Raft transport. Without this, a
    // vote request with a huge term would force the leader to step down.
    let tls = ClientTlsConfig::new()
        .ca_certificate(TCert::from_pem(std::fs::read(&p.ca).unwrap()))
        .identity(TId::from_pem(
            std::fs::read(&p.admin.cert).unwrap(),
            std::fs::read(&p.admin.key).unwrap(),
        ));
    let channel = Endpoint::from_shared(format!("https://{}", c.addrs[0]))
        .unwrap()
        .tls_config(tls)
        .unwrap()
        .connect()
        .await
        .expect("TLS connect with a valid client certificate");
    let mut raft = RaftTransportClient::new(channel);
    let err = raft
        .request_vote(RequestVoteRequest {
            term: u64::MAX - 1,
            candidate_id: 9,
            last_log_index: u64::MAX,
            last_log_term: u64::MAX,
            pre_vote: false,
        })
        .await
        .expect_err("a client certificate must not be able to send Raft messages");
    assert_eq!(err.code(), tonic::Code::PermissionDenied, "{err}");

    // The cluster is undisturbed: it still serves and kept the earlier write.
    assert!(cli(&c, "put", &admin, &["after", "2"]).status.success());
    assert_eq!(text(&cli(&c, "get", &admin, &["before"])), "1");
}

#[test]
fn bad_authorization_setups_stop_the_node() {
    let p = pki();
    let good = authz_file(&p);
    let junk = p.dir.path().join("junk.txt");
    std::fs::write(&junk, "root nonsense\n").unwrap();
    let no_node = p.dir.path().join("no-node.txt");
    std::fs::write(&no_node, format!("admin {}\n", p.admin.fingerprint)).unwrap();
    let mtls: Vec<String> = vec![
        "--tls-cert".into(),
        s(&p.node.cert),
        "--tls-key".into(),
        s(&p.node.key),
        "--tls-ca".into(),
        s(&p.ca),
    ];

    let mut cases: Vec<(&str, Vec<String>, &str)> = vec![
        (
            "authz without any TLS",
            vec!["--authz-file".into(), s(&good)],
            "needs mutual TLS",
        ),
        (
            "authz with TLS but client certificates not required",
            [mtls.clone(), vec!["--authz-file".into(), s(&good)]].concat(),
            "needs mutual TLS",
        ),
    ];
    for (name, file, expect) in [
        ("missing file", p.dir.path().join("nope.txt"), "cannot read"),
        ("malformed file", junk, "unknown role"),
        ("no node entry", no_node, "no `node` entry"),
    ] {
        cases.push((
            name,
            [
                mtls.clone(),
                vec![
                    "--tls-require-client-cert".into(),
                    "--authz-file".into(),
                    s(&file),
                ],
            ]
            .concat(),
            expect,
        ));
    }

    for (name, flags, expect) in cases {
        let data = tempfile::tempdir().unwrap();
        let addr = free_addr();
        let mut child = Command::new(BIN)
            .args(["start", "--id", "1", "--addr", &addr, "--data-dir"])
            .arg(data.path())
            .args(&flags)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let exited = loop {
            if child.try_wait().unwrap().is_some() {
                break true;
            }
            if Instant::now() > deadline {
                break false;
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        if !exited {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{name}: the node kept running instead of refusing to start");
        }
        let out = child.wait_with_output().unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(!out.status.success(), "{name}: must refuse to start");
        assert!(
            err.contains(expect),
            "{name}: expected '{expect}' in: {err}"
        );
        assert!(
            std::net::TcpListener::bind(&addr).is_ok(),
            "{name}: must not be listening"
        );
    }

    // The flag belongs to `start` only.
    let out = Command::new(BIN)
        .args(["get", "--cluster", "1=127.0.0.1:1", "--authz-file"])
        .arg(&good)
        .arg("k")
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("only applies to `start`"));
}
