//! Process-wide transport mode for the cluster control plane (barrier and
//! shuffle), resolved once at startup with at most one TLS identity.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::OnceLock;

use sha2::{Digest as _, Sha256};
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

const TRANSPORT_UNUSED: u8 = 0;
const TRANSPORT_PLAINTEXT: u8 = 1;
const TRANSPORT_TLS_INSTALLING: u8 = 2;
const TRANSPORT_TLS: u8 = 3;

/// TLS material shared by every control-plane server and client in this process.
pub struct ClusterTls {
    server: ServerTlsConfig,
    client: ClientTlsConfig,
    fingerprint: [u8; 32],
}

impl ClusterTls {
    /// Build mTLS configs from PEM: this node's `cert`+`key`, the `ca` that signed
    /// every peer cert, and the `server_name` SAN peers are verified against.
    #[must_use]
    pub fn from_pem(cert: &[u8], key: &[u8], ca: &[u8], server_name: &str) -> Self {
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
        let fingerprint = tls_material_fingerprint(cert, key, ca, server_name);
        let identity = Identity::from_pem(cert, key);
        let ca = Certificate::from_pem(ca);
        let server = ServerTlsConfig::new()
            .identity(identity.clone())
            .client_ca_root(ca.clone());
        let client = ClientTlsConfig::new()
            .ca_certificate(ca)
            .identity(identity)
            .domain_name(server_name.to_string());
        Self {
            server,
            client,
            fingerprint,
        }
    }
}

fn tls_material_fingerprint(cert: &[u8], key: &[u8], ca: &[u8], server_name: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"laminardb-cluster-tls-v1");
    for field in [cert, key, ca, server_name.as_bytes()] {
        let length = u64::try_from(field.len()).expect("TLS material length fits u64");
        digest.update(length.to_be_bytes());
        digest.update(field);
    }
    digest.finalize().into()
}

struct ClusterTlsState {
    mode: AtomicU8,
    tls: OnceLock<ClusterTls>,
}

impl ClusterTlsState {
    const fn new() -> Self {
        Self {
            mode: AtomicU8::new(TRANSPORT_UNUSED),
            tls: OnceLock::new(),
        }
    }

    fn install(&self, tls: ClusterTls) -> Result<(), String> {
        loop {
            match self.mode.load(Ordering::Acquire) {
                TRANSPORT_UNUSED => {
                    if self
                        .mode
                        .compare_exchange(
                            TRANSPORT_UNUSED,
                            TRANSPORT_TLS_INSTALLING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let fingerprint = tls.fingerprint;
                    let result = match self.tls.set(tls) {
                        Ok(()) => Ok(()),
                        Err(_)
                            if self
                                .tls
                                .get()
                                .is_some_and(|tls| tls.fingerprint == fingerprint) =>
                        {
                            Ok(())
                        }
                        Err(_) => {
                            Err("cluster TLS state already contains different material".to_string())
                        }
                    };
                    self.mode.store(TRANSPORT_TLS, Ordering::Release);
                    return result;
                }
                TRANSPORT_PLAINTEXT => {
                    return Err(
                        "cluster TLS cannot be installed after plaintext was selected".into(),
                    );
                }
                TRANSPORT_TLS_INSTALLING => std::hint::spin_loop(),
                TRANSPORT_TLS => {
                    return if self
                        .tls
                        .get()
                        .is_some_and(|installed| installed.fingerprint == tls.fingerprint)
                    {
                        Ok(())
                    } else {
                        Err("different cluster TLS material is already installed".into())
                    };
                }
                _ => unreachable!("cluster transport mode is internal"),
            }
        }
    }

    fn claim_plaintext(&self) -> Result<(), String> {
        loop {
            match self.mode.load(Ordering::Acquire) {
                TRANSPORT_UNUSED => {
                    if self
                        .mode
                        .compare_exchange(
                            TRANSPORT_UNUSED,
                            TRANSPORT_PLAINTEXT,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    return Ok(());
                }
                TRANSPORT_PLAINTEXT => return Ok(()),
                TRANSPORT_TLS_INSTALLING | TRANSPORT_TLS => {
                    return Err(
                        "cluster plaintext cannot be claimed after TLS installation has begun"
                            .into(),
                    );
                }
                _ => unreachable!("cluster transport mode is internal"),
            }
        }
    }

    fn transport_tls(&self) -> Option<&ClusterTls> {
        loop {
            match self.mode.load(Ordering::Acquire) {
                TRANSPORT_UNUSED => {
                    if self
                        .mode
                        .compare_exchange(
                            TRANSPORT_UNUSED,
                            TRANSPORT_PLAINTEXT,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    return None;
                }
                TRANSPORT_PLAINTEXT => return None,
                TRANSPORT_TLS_INSTALLING => std::hint::spin_loop(),
                TRANSPORT_TLS => {
                    return Some(
                        self.tls
                            .get()
                            .expect("TLS mode is published only after its material"),
                    );
                }
                _ => unreachable!("cluster transport mode is internal"),
            }
        }
    }
}

static CLUSTER_TLS: ClusterTlsState = ClusterTlsState::new();

/// Install process-wide control-plane TLS before any cluster transport is constructed.
///
/// Reinstalling byte-identical material is idempotent. Different material, or installation after
/// plaintext was explicitly selected or used by a transport, fails closed.
///
/// # Errors
/// Returns an error when transport mode is already plaintext or different TLS material is active.
pub fn set_cluster_tls(tls: ClusterTls) -> Result<(), String> {
    CLUSTER_TLS.install(tls)
}

/// Select process-wide plaintext before any cluster transport is constructed.
///
/// Repeated plaintext claims are idempotent. A claim fails once TLS installation begins.
///
/// # Errors
/// Returns an error when TLS installation has already begun or completed.
pub fn claim_cluster_plaintext() -> Result<(), String> {
    CLUSTER_TLS.claim_plaintext()
}

/// Server config for the shared control-plane / shuffle listeners.
pub(crate) fn server_tls() -> Option<&'static ServerTlsConfig> {
    CLUSTER_TLS.transport_tls().map(|tls| &tls.server)
}

/// Client endpoint for `host_port`, using control-plane TLS + `https` when
/// installed, plaintext `http` otherwise.
pub(crate) fn client_endpoint(host_port: &str) -> Result<Endpoint, String> {
    let tls = CLUSTER_TLS.transport_tls();
    let scheme = if tls.is_some() { "https" } else { "http" };
    // HTTP/2 keepalive + connect timeout so a half-open conn (peer machine gone with no
    // TCP RST — kill-9 on loopback RST-closes promptly, but a true network/host failure
    // does not) flips dead within ~6s instead of blocking sends until the OS TCP timeout
    // (minutes). Sub-`align_shuffle_barriers` ALIGN_TIMEOUT (8s) so the driver errors and
    // the next send reconnects before alignment gives up.
    let endpoint = Endpoint::from_shared(format!("{scheme}://{host_port}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(std::time::Duration::from_secs(3))
        .http2_keep_alive_interval(std::time::Duration::from_secs(3))
        .keep_alive_timeout(std::time::Duration::from_secs(3))
        .keep_alive_while_idle(true);
    match tls {
        Some(t) => endpoint
            .tls_config(t.client.clone())
            .map_err(|e| e.to_string()),
        None => Ok(endpoint),
    }
}

#[cfg(test)]
mod tests;
