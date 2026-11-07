//! TLS and mutual TLS through the real binary: a 3-node cluster whose Raft
//! traffic, client API and backup all run over TLS, plus the refusals that
//! make "TLS on" actually mean something (plaintext, untrusted CAs, missing
//! client certificates, half-configured flags).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

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

/// (certificate PEM, private key PEM), valid for localhost and 127.0.0.1.
fn make_leaf(ca: &Certificate, cn: &str) -> (String, String) {
    let mut p = CertificateParams::new(vec!["localhost".to_string()]);
    p.subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    p.distinguished_name.push(DnType::CommonName, cn);
    let c = Certificate::from_params(p).unwrap();
    (
        c.serialize_pem_with_signer(ca).unwrap(),
        c.serialize_private_key_pem(),
    )
}

struct Pki {
    _dir: tempfile::TempDir,
    ca: PathBuf,
    cert: PathBuf,
    key: PathBuf,
    stranger_ca: PathBuf,
    stranger_cert: PathBuf,
    stranger_key: PathBuf,
}

fn pki() -> Pki {
    let dir = tempfile::tempdir().unwrap();
    let w = |name: &str, body: String| {
        let p = dir.path().join(name);
        std::fs::write(&p, body).unwrap();
        p
    };
    let ca = make_ca("raftdb test CA");
    let (c, k) = make_leaf(&ca, "node");
    let other = make_ca("some other CA");
    let (oc, ok) = make_leaf(&other, "stranger");
    Pki {
        ca: w("ca.pem", ca.serialize_pem().unwrap()),
        cert: w("node.pem", c),
        key: w("node.key", k),
        stranger_ca: w("stranger-ca.pem", other.serialize_pem().unwrap()),
        stranger_cert: w("stranger.pem", oc),
        stranger_key: w("stranger.key", ok),
        _dir: dir,
    }
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

fn s(p: &Path) -> String {
    p.to_str().unwrap().to_string()
}

/// Starts nodes 1..=3, each with `tls_flags` (empty = plaintext).
fn start_cluster(tls_flags: &[String]) -> Cluster {
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
                .args(tls_flags)
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

fn cli(cluster: &Cluster, sub: &str, extra: &[String], tail: &[&str]) -> Output {
    Command::new(BIN)
        .arg(sub)
        .args(["--cluster", &cluster.cluster_arg()])
        .args(extra)
        .args(tail)
        .output()
        .expect("run raftdb cli")
}

fn client_flags(ca: &Path, identity: Option<(&Path, &Path)>) -> Vec<String> {
    let mut v = vec!["--tls-ca".to_string(), s(ca)];
    if let Some((c, k)) = identity {
        v.extend([
            "--tls-cert".to_string(),
            s(c),
            "--tls-key".to_string(),
            s(k),
        ]);
    }
    v
}

fn node_flags(p: &Pki, mutual: bool) -> Vec<String> {
    let mut v = vec![
        "--tls-cert".into(),
        s(&p.cert),
        "--tls-key".into(),
        s(&p.key),
        "--tls-ca".into(),
        s(&p.ca),
    ];
    if mutual {
        v.push("--tls-require-client-cert".into());
    }
    v
}

/// Retries a put until the cluster has elected a leader (which, with TLS on,
/// itself proves node-to-node TLS works).
fn wait_until_serving(c: &Cluster, flags: &[String]) {
    let deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let out = cli(c, "put", flags, &["warmup", "1"]);
        if out.status.success() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "cluster never became ready over TLS; last error: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[test]
fn tls_cluster_serves_clients_and_refuses_everything_untrusted() {
    let p = pki();
    let c = start_cluster(&node_flags(&p, false));
    let good = client_flags(&p.ca, None);
    wait_until_serving(&c, &good);

    // Round trip through the TLS client API (and, behind it, TLS Raft traffic).
    assert!(cli(&c, "put", &good, &["k", "v-over-tls"]).status.success());
    let got = cli(&c, "get", &good, &["k"]);
    assert!(got.status.success());
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), "v-over-tls");

    // A plaintext client must be refused, not silently served.
    let plain = cli(&c, "put", &[], &["leak", "x"]);
    assert!(
        !plain.status.success(),
        "a plaintext client must not be able to write to a TLS cluster"
    );

    // A client that trusts a different CA must refuse the server's certificate.
    let wrong = cli(&c, "get", &client_flags(&p.stranger_ca, None), &["k"]);
    assert!(
        !wrong.status.success(),
        "a client with the wrong CA must not connect"
    );

    // And the refused write really did not happen.
    let leak = cli(&c, "get", &good, &["leak"]);
    assert_eq!(String::from_utf8_lossy(&leak.stdout).trim(), "(not found)");

    // Backup (a different code path to the client commands) works over TLS.
    let out_dir = tempfile::tempdir().unwrap();
    let file = out_dir.path().join("b.bak");
    let mut args = vec![
        "--addr".to_string(),
        c.addrs[0].clone(),
        "--out".to_string(),
        s(&file),
    ];
    args.extend(good.clone());
    let b = Command::new(BIN)
        .arg("backup")
        .args(&args)
        .output()
        .unwrap();
    assert!(
        b.status.success(),
        "backup over TLS failed: {}",
        String::from_utf8_lossy(&b.stderr)
    );
    assert!(file.exists() && std::fs::metadata(&file).unwrap().len() > 0);

    // The same backup request without TLS is refused.
    let file2 = out_dir.path().join("b2.bak");
    let nb = Command::new(BIN)
        .args(["backup", "--addr", &c.addrs[0], "--out"])
        .arg(&file2)
        .output()
        .unwrap();
    assert!(
        !nb.status.success() && !file2.exists(),
        "plaintext backup must be refused"
    );
}

#[test]
fn mutual_tls_rejects_clients_without_a_certificate_the_cluster_trusts() {
    let p = pki();
    let c = start_cluster(&node_flags(&p, true));
    let good = client_flags(&p.ca, Some((&p.cert, &p.key)));
    wait_until_serving(&c, &good); // peers authenticate each other with the same CA

    assert!(cli(&c, "put", &good, &["k", "mtls"]).status.success());

    // Trusts the server but presents no certificate: refused.
    let no_cert = cli(&c, "put", &client_flags(&p.ca, None), &["nocert", "x"]);
    assert!(
        !no_cert.status.success(),
        "mutual TLS must refuse a client with no certificate"
    );

    // Presents a certificate from a CA the cluster does not trust: refused.
    let stranger = cli(
        &c,
        "put",
        &client_flags(&p.ca, Some((&p.stranger_cert, &p.stranger_key))),
        &["stranger", "x"],
    );
    assert!(
        !stranger.status.success(),
        "mutual TLS must refuse a certificate from an untrusted CA"
    );

    // Neither refused write happened, and the trusted client still works.
    let got = cli(&c, "get", &good, &["nocert"]);
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), "(not found)");
    let got = cli(&c, "get", &good, &["stranger"]);
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), "(not found)");
    let got = cli(&c, "get", &good, &["k"]);
    assert_eq!(String::from_utf8_lossy(&got.stdout).trim(), "mtls");
}

#[test]
fn a_bad_tls_configuration_stops_the_node_instead_of_falling_back_to_plaintext() {
    let p = pki();
    let dir = tempfile::tempdir().unwrap();
    let junk = dir.path().join("junk.pem");
    std::fs::write(&junk, "not a certificate").unwrap();

    let bad: Vec<(&str, Vec<String>)> = vec![
        (
            "cert without key",
            vec!["--tls-cert".into(), s(&p.cert), "--tls-ca".into(), s(&p.ca)],
        ),
        (
            "CA only (would serve plaintext but dial peers over TLS)",
            vec!["--tls-ca".into(), s(&p.ca)],
        ),
        (
            "cert and key without a CA",
            vec![
                "--tls-cert".into(),
                s(&p.cert),
                "--tls-key".into(),
                s(&p.key),
            ],
        ),
        (
            "mutual TLS without a CA",
            vec![
                "--tls-cert".into(),
                s(&p.cert),
                "--tls-key".into(),
                s(&p.key),
                "--tls-require-client-cert".into(),
            ],
        ),
        (
            "garbage CA file",
            vec![
                "--tls-cert".into(),
                s(&p.cert),
                "--tls-key".into(),
                s(&p.key),
                "--tls-ca".into(),
                s(&junk),
            ],
        ),
        (
            "missing key file",
            vec![
                "--tls-cert".into(),
                s(&p.cert),
                "--tls-key".into(),
                s(&dir.path().join("nope.key")),
                "--tls-ca".into(),
                s(&p.ca),
            ],
        ),
    ];
    for (name, flags) in bad {
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
        // A node that refuses a bad configuration exits within moments; one
        // that is still running here has started anyway. Fail fast, not hang.
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
        assert!(
            !out.status.success(),
            "{name}: the node must refuse to start"
        );
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            err.contains("TLS configuration error"),
            "{name}: expected a clear TLS error, got: {err}"
        );
        // Nothing was bound: the port is still free.
        assert!(
            std::net::TcpListener::bind(&addr).is_ok(),
            "{name}: the node must not be listening"
        );
    }
}
