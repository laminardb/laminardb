//! Process-wide mutual TLS for the cluster control plane (barrier, query,
//! shuffle). One identity per node/process, installed at startup and
//! replaceable at runtime so rotated certificates take effect without a
//! process restart.

use parking_lot::RwLock;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

/// TLS material shared by every control-plane server and client in this process.
pub struct ClusterTls {
    server: ServerTlsConfig,
    client: ClientTlsConfig,
}

impl ClusterTls {
    /// Build mTLS configs from PEM: this node's `cert`+`key`, the `ca` that
    /// signed every peer cert, and the `server_name` SAN to verify peers
    /// against (peers connect by IP, so all certs share one DNS SAN).
    #[must_use]
    pub fn from_pem(cert: &[u8], key: &[u8], ca: &[u8], server_name: &str) -> Self {
        let identity = Identity::from_pem(cert, key);
        let ca = Certificate::from_pem(ca);
        let server = ServerTlsConfig::new()
            .identity(identity.clone())
            .client_ca_root(ca.clone());
        let client = ClientTlsConfig::new()
            .ca_certificate(ca)
            .identity(identity)
            .domain_name(server_name.to_string());
        Self { server, client }
    }
}

static CLUSTER_TLS: RwLock<Option<ClusterTls>> = RwLock::new(None);

/// Install (or replace) the process-wide control-plane TLS. Call at startup
/// before any control-plane server or client is created; safe to call again at
/// runtime to apply rotated certificates, overwriting the previous material.
/// New servers and client endpoints pick up the change on their next creation.
pub fn set_cluster_tls(tls: ClusterTls) {
    *CLUSTER_TLS.write() = Some(tls);
}

/// Server config to apply on the shared control-plane / shuffle listeners.
///
/// Returns an owned clone; in Tonic `ServerTlsConfig` wraps atomically
/// reference-counted internals, so cloning is cheap.
pub(crate) fn server_tls() -> Option<ServerTlsConfig> {
    CLUSTER_TLS.read().as_ref().map(|t| t.server.clone())
}

/// Build a client endpoint for `host_port`, applying control-plane TLS (and the
/// `https` scheme) when installed; plaintext `http` otherwise.
pub(crate) fn client_endpoint(host_port: &str) -> Result<Endpoint, String> {
    let client = CLUSTER_TLS.read().as_ref().map(|t| t.client.clone());
    let scheme = if client.is_some() { "https" } else { "http" };
    let endpoint =
        Endpoint::from_shared(format!("{scheme}://{host_port}")).map_err(|e| e.to_string())?;
    match client {
        Some(c) => endpoint.tls_config(c).map_err(|e| e.to_string()),
        None => Ok(endpoint),
    }
}
