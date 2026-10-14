//! Verified TLS for the engine's plaintext socket.
//!
//! QuickFIX's own SSL support only sets SNI and skips verification when no CA is configured, so
//! the engine connects in plaintext to a loopback listener owned by this source and the bridge
//! forwards bytes over rustls with certificate-chain and hostname verification and optional
//! mutual TLS. The listener accepts one connection at a time; when the TLS side fails it closes
//! the engine connection and the engine's own reconnect interval applies.
//!
//! Residual exposure: any local process able to connect to the ephemeral loopback port while
//! the engine is reconnecting could use the tunnel. Run the server where untrusted local users
//! cannot open loopback connections.

use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

use super::config::TlsSettings;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

fn config_error(
    what: &str,
    path: &std::path::Path,
    error: impl std::fmt::Display,
) -> ConnectorError {
    ConnectorError::ConfigurationError(format!("FIX TLS {what} '{}': {error}", path.display()))
}

/// Build the verifying client configuration.
pub(crate) fn client_config(settings: &TlsSettings) -> Result<ClientConfig, ConnectorError> {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = RootCertStore::empty();
    match &settings.ca_file {
        Some(path) => {
            let certificates = CertificateDer::pem_file_iter(path)
                .map_err(|error| config_error("CA file", path, error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| config_error("CA file", path, error))?;
            if certificates.is_empty() {
                return Err(config_error("CA file", path, "contains no certificates"));
            }
            for certificate in certificates {
                roots
                    .add(certificate)
                    .map_err(|error| config_error("CA certificate", path, error))?;
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }
    let builder = ClientConfig::builder().with_root_certificates(roots);
    match &settings.client_identity {
        None => Ok(builder.with_no_client_auth()),
        Some((cert, key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .map_err(|error| config_error("client certificate", cert, error))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| config_error("client certificate", cert, error))?;
            let key = PrivateKeyDer::from_pem_file(key)
                .map_err(|error| config_error("client key", key, error))?;
            builder.with_client_auth_cert(chain, key).map_err(|error| {
                ConnectorError::ConfigurationError(format!("FIX TLS client identity: {error}"))
            })
        }
    }
}

/// A running loopback-to-TLS bridge.
pub(crate) struct TlsBridge {
    pub(crate) local_port: u16,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl TlsBridge {
    /// Bind the loopback listener and start forwarding to `host:port`.
    pub(crate) async fn start(
        settings: &TlsSettings,
        host: &str,
        port: u16,
        guard: ConnectorTaskGuard,
    ) -> Result<Self, ConnectorError> {
        let connector = TlsConnector::from(Arc::new(client_config(settings)?));
        let server_name = ServerName::try_from(settings.server_name.clone()).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "FIX TLS server name '{}' is not a valid DNS name or IP address",
                settings.server_name
            ))
        })?;
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let local_port = listener.local_addr()?.port();
        let (shutdown, stop) = watch::channel(false);
        let remote = (host.to_string(), port);
        let task = tokio::spawn(async move {
            let _guard = guard;
            serve(listener, connector, server_name, remote, stop).await;
        });
        Ok(Self {
            local_port,
            shutdown,
            task: Some(task),
        })
    }

    /// Stop forwarding and wait for the bridge task.
    pub(crate) async fn stop(mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for TlsBridge {
    fn drop(&mut self) {
        let _ = self.shutdown.send(true);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn serve(
    listener: TcpListener,
    connector: TlsConnector,
    server_name: ServerName<'static>,
    remote: (String, u16),
    mut stop: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            _ = stop.changed() => return,
            accepted = listener.accept() => accepted,
        };
        let local = match accepted {
            Ok((local, peer)) if peer.ip().is_loopback() => local,
            Ok(_) => continue,
            Err(error) => {
                tracing::warn!(error = %error, "FIX TLS bridge accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        tokio::select! {
            _ = stop.changed() => return,
            result = forward(local, &connector, &server_name, &remote) => {
                if let Err(error) = result {
                    tracing::warn!(
                        host = %remote.0,
                        port = remote.1,
                        error = %error,
                        "FIX TLS connection ended with an error"
                    );
                }
            }
        }
    }
}

async fn forward(
    mut local: TcpStream,
    connector: &TlsConnector,
    server_name: &ServerName<'static>,
    remote: &(String, u16),
) -> std::io::Result<()> {
    let tcp = tokio::time::timeout(
        CONNECT_TIMEOUT,
        TcpStream::connect((remote.0.as_str(), remote.1)),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "TCP connect timed out"))??;
    tcp.set_nodelay(true)?;
    local.set_nodelay(true)?;
    let mut tls = tokio::time::timeout(
        HANDSHAKE_TIMEOUT,
        connector.connect(server_name.clone(), tcp),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "TLS handshake timed out"))??;
    tokio::io::copy_bidirectional(&mut local, &mut tls).await?;
    Ok(())
}
