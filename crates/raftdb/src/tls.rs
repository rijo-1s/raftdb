//! Optional TLS and mutual TLS for every gRPC connection this process makes
//! or accepts: node-to-node Raft traffic, the client API, the CLI commands and
//! `backup`.
//!
//! **Off by default.** With none of the `--tls-*` flags the process behaves
//! exactly as before (plaintext gRPC). Turning TLS on is all-or-nothing per
//! process: a node with a certificate also talks TLS to its peers, so a
//! cluster must be switched over node by node with every node restarted
//! (see `docs/tls.md`). A half-configured setup is a startup error, never a
//! silent fallback to plaintext.
//!
//! What this gives you: encryption on the wire, server authentication, and
//! (with `--tls-require-client-cert`) authentication of clients and peers by
//! certificate. It does **not** give you authorization (who may do what).
//! The `--metrics-addr` HTTP listener stays plaintext.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, Server, ServerTlsConfig};

#[derive(clap::Args, Clone, Debug, Default)]
pub struct TlsArgs {
    /// PEM certificate chain for this process. Used as the server certificate
    /// and, when connecting out, as the client identity (for mutual TLS).
    /// Requires --tls-key and --tls-ca.
    #[arg(long, global = true, value_name = "PEM")]
    pub tls_cert: Option<PathBuf>,
    /// PEM private key matching --tls-cert.
    #[arg(long, global = true, value_name = "PEM")]
    pub tls_key: Option<PathBuf>,
    /// PEM CA certificate(s) used to verify the other side of every
    /// connection (peers, servers, and clients when mutual TLS is required).
    /// Giving only this flag makes a client command use TLS (server
    /// authentication only).
    #[arg(long, global = true, value_name = "PEM")]
    pub tls_ca: Option<PathBuf>,
    /// (start) Require every inbound connection to present a certificate
    /// signed by --tls-ca (mutual TLS). Peers present --tls-cert.
    #[arg(long, global = true, default_value_t = false)]
    pub tls_require_client_cert: bool,
    /// Verify the server certificate against this name instead of the host in
    /// the address. Use it when nodes are addressed by IP but certificates
    /// carry a DNS name.
    #[arg(long, global = true, value_name = "NAME")]
    pub tls_domain: Option<String>,
    /// (start) Refuse to start when the private key file is readable by group or
    /// others (not mode 600/400). Without this flag it only logs a warning.
    #[arg(long, global = true, default_value_t = false)]
    pub tls_strict_key_permissions: bool,
}

/// The validated result of [`TlsArgs::build`].
#[derive(Debug)]
pub struct TlsSetup {
    /// Present when this process should accept TLS connections.
    pub server: Option<ServerTlsConfig>,
    /// Present when this process should use TLS for outgoing connections.
    pub outbound: Option<ClientTlsConfig>,
}

static OUTBOUND: OnceLock<ClientTlsConfig> = OnceLock::new();

/// Installed once at startup. Never called in tests that dial plaintext, so
/// the default stays plaintext.
pub fn set_outbound(cfg: ClientTlsConfig) {
    let _ = OUTBOUND.set(cfg);
}

pub fn outbound() -> Option<&'static ClientTlsConfig> {
    OUTBOUND.get()
}

/// URL scheme for addresses given without one.
pub fn scheme() -> &'static str {
    if OUTBOUND.get().is_some() {
        "https"
    } else {
        "http"
    }
}

/// Applies the outbound TLS configuration (if any) to an endpoint.
pub fn apply(endpoint: Endpoint) -> Result<Endpoint, tonic::transport::Error> {
    match outbound() {
        Some(cfg) => endpoint.tls_config(cfg.clone()),
        None => Ok(endpoint),
    }
}

fn read(path: &Path, what: &str) -> Result<Vec<u8>, String> {
    std::fs::read(path).map_err(|e| format!("cannot read {what} {}: {e}", path.display()))
}

/// Like `read`, but also requires the file to contain a PEM block of the
/// expected kind. (A CA file with no certificates would otherwise be accepted
/// and silently trust nothing.)
fn read_pem(path: &Path, what: &str, marker: &str) -> Result<Vec<u8>, String> {
    let bytes = read(path, what)?;
    let text = String::from_utf8_lossy(&bytes);
    let ok = if marker.is_empty() {
        text.contains("PRIVATE KEY-----")
    } else {
        text.contains(&format!("-----BEGIN {marker}-----"))
    };
    if !ok {
        let kind = if marker.is_empty() {
            "private key"
        } else {
            marker
        };
        return Err(format!(
            "{what} {} does not contain a PEM {kind}",
            path.display()
        ));
    }
    Ok(bytes)
}

impl TlsArgs {
    pub fn any(&self) -> bool {
        self.tls_cert.is_some()
            || self.tls_key.is_some()
            || self.tls_ca.is_some()
            || self.tls_require_client_cert
            || self.tls_domain.is_some()
    }

    /// Checks the combination of flags, reads and parses every file, and
    /// returns the configs. Any problem is an error: TLS never degrades to
    /// plaintext.
    pub fn build(&self) -> Result<TlsSetup, String> {
        if !self.any() {
            return Ok(TlsSetup {
                server: None,
                outbound: None,
            });
        }
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err("--tls-cert and --tls-key must be given together".into());
        }
        let Some(ca_path) = &self.tls_ca else {
            return Err(
                "--tls-ca is required whenever TLS is configured: it is what this process uses \
                 to verify the nodes it connects to"
                    .into(),
            );
        };
        if self.tls_require_client_cert && self.tls_cert.is_none() {
            return Err("--tls-require-client-cert needs --tls-cert and --tls-key".into());
        }

        let ca = Certificate::from_pem(read_pem(ca_path, "--tls-ca", "CERTIFICATE")?);
        let identity = match (&self.tls_cert, &self.tls_key) {
            (Some(c), Some(k)) => Some(Identity::from_pem(
                read_pem(c, "--tls-cert", "CERTIFICATE")?,
                read_pem(k, "--tls-key", "")?,
            )),
            _ => None,
        };

        let mut outbound = ClientTlsConfig::new().ca_certificate(ca.clone());
        if let Some(d) = &self.tls_domain {
            outbound = outbound.domain_name(d.clone());
        }
        if let Some(id) = &identity {
            outbound = outbound.identity(id.clone());
        }
        // Parse eagerly so a bad PEM fails now, not on the first connection.
        Endpoint::from_static("https://localhost")
            .tls_config(outbound.clone())
            .map_err(|e| format!("invalid TLS client configuration: {e}"))?;

        let server = match identity {
            Some(id) => {
                let mut s = ServerTlsConfig::new().identity(id);
                if self.tls_require_client_cert {
                    s = s.client_ca_root(ca);
                }
                Server::builder()
                    .tls_config(s.clone())
                    .map_err(|e| format!("invalid TLS server configuration: {e}"))?;
                Some(s)
            }
            None => None,
        };
        Ok(TlsSetup {
            server,
            outbound: Some(outbound),
        })
    }
}

impl TlsArgs {
    /// Like [`build`](Self::build), for `raftdb start`. A node that was given
    /// any TLS flag must also have a certificate and key: with only `--tls-ca`
    /// it would serve plaintext while dialing its peers over TLS, which looks
    /// like TLS being on but is not. (For client commands `--tls-ca` alone is
    /// valid: it means "use TLS and verify the server".)
    pub fn build_for_node(&self) -> Result<TlsSetup, String> {
        let setup = self.build()?;
        if self.any() && setup.server.is_none() {
            return Err(
                "`start` was given TLS flags but no --tls-cert/--tls-key, so it would accept \
                 plaintext connections while using TLS to reach its peers; give both, or \
                 remove every --tls-* flag"
                    .into(),
            );
        }
        Ok(setup)
    }
}

impl TlsArgs {
    /// The private key path and its mode when the key is accessible by group or
    /// others (Unix only; `None` elsewhere or when the file cannot be read).
    pub fn key_permission_problem(&self) -> Option<(PathBuf, u32)> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let path = self.tls_key.as_ref()?;
            let mode = std::fs::metadata(path).ok()?.permissions().mode() & 0o777;
            (mode & 0o077 != 0).then(|| (path.clone(), mode))
        }
        #[cfg(not(unix))]
        {
            None
        }
    }

    /// A rustls server config for the HTTPS metrics endpoint, from the same
    /// certificate and key as the gRPC server. Client certificates are required
    /// (signed by --tls-ca) exactly when --tls-require-client-cert is set.
    /// `None` when no --tls-cert is configured.
    pub fn metrics_server_config(
        &self,
    ) -> Result<Option<std::sync::Arc<rustls::ServerConfig>>, String> {
        use std::sync::Arc;
        let (Some(cert), Some(key)) = (&self.tls_cert, &self.tls_key) else {
            return Ok(None);
        };
        let read = |p: &PathBuf, what: &str| {
            std::fs::read(p).map_err(|e| format!("cannot read {what} {}: {e}", p.display()))
        };
        let chain = rustls_pemfile::certs(&mut &read(cert, "--tls-cert")?[..])
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("unreadable --tls-cert: {e}"))?;
        let key = rustls_pemfile::private_key(&mut &read(key, "--tls-key")?[..])
            .map_err(|e| format!("unreadable --tls-key: {e}"))?
            .ok_or("no private key found in --tls-key")?;
        let builder = rustls::ServerConfig::builder();
        let mut config = if self.tls_require_client_cert {
            let ca = self
                .tls_ca
                .as_ref()
                .ok_or("--tls-require-client-cert needs --tls-ca")?;
            let mut roots = rustls::RootCertStore::empty();
            for c in rustls_pemfile::certs(&mut &read(ca, "--tls-ca")?[..]) {
                roots
                    .add(c.map_err(|e| format!("unreadable --tls-ca: {e}"))?)
                    .map_err(|e| format!("bad CA certificate: {e}"))?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("cannot build the client verifier: {e}"))?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(chain, key)
        } else {
            builder.with_no_client_auth().with_single_cert(chain, key)
        }
        .map_err(|e| format!("the certificate and key do not form a valid identity: {e}"))?;
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok(Some(Arc::new(config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> TlsArgs {
        TlsArgs::default()
    }

    #[test]
    fn no_flags_means_plaintext() {
        let s = args().build().unwrap();
        assert!(s.server.is_none() && s.outbound.is_none());
    }

    #[test]
    fn every_half_configured_combination_is_refused() {
        let p = Some(PathBuf::from("/nonexistent"));
        let cases = [
            TlsArgs {
                tls_cert: p.clone(),
                ..args()
            },
            TlsArgs {
                tls_key: p.clone(),
                ..args()
            },
            TlsArgs {
                tls_cert: p.clone(),
                tls_key: p.clone(),
                ..args()
            }, // no CA
            TlsArgs {
                tls_require_client_cert: true,
                ..args()
            },
            TlsArgs {
                tls_domain: Some("x".into()),
                ..args()
            },
            TlsArgs {
                tls_ca: p.clone(),
                tls_require_client_cert: true,
                ..args()
            }, // no cert
        ];
        for (i, c) in cases.iter().enumerate() {
            assert!(c.build().is_err(), "case {i} must be refused");
        }
    }

    #[test]
    fn a_node_with_only_a_ca_is_refused_but_a_client_with_only_a_ca_is_fine() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.pem");
        let c = rcgen::Certificate::from_params({
            let mut p = rcgen::CertificateParams::new(Vec::<String>::new());
            p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            p
        })
        .unwrap();
        std::fs::write(&ca, c.serialize_pem().unwrap()).unwrap();
        let only_ca = TlsArgs {
            tls_ca: Some(ca),
            ..args()
        };
        assert!(
            only_ca.build().is_ok(),
            "a client command with only --tls-ca is valid"
        );
        assert!(
            only_ca.build_for_node().is_err(),
            "a node with only --tls-ca must be refused"
        );
        assert!(
            args().build_for_node().is_ok(),
            "no flags at all is still plaintext"
        );
    }

    #[test]
    fn unreadable_or_garbage_files_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let missing = TlsArgs {
            tls_ca: Some(dir.path().join("nope.pem")),
            ..args()
        };
        assert!(missing.build().unwrap_err().contains("cannot read"));
        let junk = dir.path().join("junk.pem");
        std::fs::write(&junk, b"this is not pem").unwrap();
        let c = TlsArgs {
            tls_ca: Some(junk.clone()),
            tls_cert: Some(junk.clone()),
            tls_key: Some(junk),
            ..args()
        };
        assert!(c.build().is_err(), "garbage PEM must not be accepted");
    }

    #[cfg(unix)]
    #[test]
    fn a_key_readable_by_others_is_reported_and_a_private_one_is_not() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("k.pem");
        std::fs::write(&key, "x").unwrap();
        let args = TlsArgs {
            tls_key: Some(key.clone()),
            ..Default::default()
        };
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert_eq!(args.key_permission_problem(), Some((key.clone(), 0o644)));
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert_eq!(args.key_permission_problem(), None);
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(
            args.key_permission_problem().is_some(),
            "group-readable counts"
        );
    }
}
