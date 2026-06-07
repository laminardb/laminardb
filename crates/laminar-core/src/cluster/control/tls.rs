//! Process-wide mutual TLS for the cluster control plane (barrier, query,
//! shuffle). One identity per node/process, installed once at startup.

use std::sync::OnceLock;

use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

/// TLS material shared by every control-plane server and client in this process.
pub struct ClusterTls {
    server: ServerTlsConfig,
    client: ClientTlsConfig,
}

impl ClusterTls {
    /// Build mTLS configs from PEM: this node's `cert`+`key`, the `ca` that signed
    /// every peer cert, and the `server_name` SAN peers are verified against.
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

static CLUSTER_TLS: OnceLock<ClusterTls> = OnceLock::new();

/// Install the process-wide control-plane TLS. Call once at startup before any
/// control-plane server or client is created; later calls are ignored.
pub fn set_cluster_tls(tls: ClusterTls) {
    let _ = CLUSTER_TLS.set(tls);
}

/// Server config for the shared control-plane / shuffle listeners.
pub(crate) fn server_tls() -> Option<&'static ServerTlsConfig> {
    CLUSTER_TLS.get().map(|t| &t.server)
}

/// Client endpoint for `host_port`, using control-plane TLS + `https` when
/// installed, plaintext `http` otherwise.
pub(crate) fn client_endpoint(host_port: &str) -> Result<Endpoint, String> {
    let tls = CLUSTER_TLS.get();
    let scheme = if tls.is_some() { "https" } else { "http" };
    let endpoint =
        Endpoint::from_shared(format!("{scheme}://{host_port}")).map_err(|e| e.to_string())?;
    match tls {
        Some(t) => endpoint
            .tls_config(t.client.clone())
            .map_err(|e| e.to_string()),
        None => Ok(endpoint),
    }
}
