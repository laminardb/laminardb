//! PgWire TLS: acceptor construction and hot-reload.
//!
//! A failed reload (truncated file, expired cert) keeps the previous acceptor in
//! place so a bad rotation cannot take TLS down. The accept path snapshots the
//! live acceptor per connection so in-flight handshakes complete against the
//! cert that was current at accept time.

use std::sync::Arc;

use tracing::{info, warn};

use crate::server::ServerError;

pub struct TlsPaths<'a> {
    pub cert: &'a std::path::Path,
    pub key: &'a std::path::Path,
    pub min_version: TlsMinVersion,
    /// PEM bundle of CA roots; presence enables mTLS — every client must
    /// present a cert that chains to one of these roots.
    pub client_ca: Option<&'a std::path::Path>,
}

/// Owned counterpart to `TlsPaths` that the listener keeps for the
/// lifetime of `serve()` so the file watcher can rebuild the acceptor
/// without the original config still being in scope.
#[derive(Debug, Clone)]
pub(super) struct TlsConfigPaths {
    cert: std::path::PathBuf,
    key: std::path::PathBuf,
    min_version: TlsMinVersion,
    client_ca: Option<std::path::PathBuf>,
}

impl TlsConfigPaths {
    pub(super) fn from_paths(paths: &TlsPaths<'_>) -> Self {
        Self {
            cert: paths.cert.to_path_buf(),
            key: paths.key.to_path_buf(),
            min_version: paths.min_version,
            client_ca: paths.client_ca.map(|p| p.to_path_buf()),
        }
    }

    fn borrow(&self) -> TlsPaths<'_> {
        TlsPaths {
            cert: &self.cert,
            key: &self.key,
            min_version: self.min_version,
            client_ca: self.client_ca.as_deref(),
        }
    }
}

/// Live TLS acceptor + paths needed to rebuild it on cert rotation.
/// Reads on the accept path are a single mutex acquire and a cheap
/// `TlsAcceptor` clone; reloads are triggered by the file watcher.
pub struct TlsReloadState {
    pub(super) paths: TlsConfigPaths,
    pub(super) acceptor: parking_lot::Mutex<Arc<tokio_rustls::TlsAcceptor>>,
}

impl TlsReloadState {
    pub(super) fn snapshot(&self) -> Arc<tokio_rustls::TlsAcceptor> {
        Arc::clone(&self.acceptor.lock())
    }
}

/// Rebuild the TLS acceptor from `state.paths` and atomically swap it in.
/// On any error the previous acceptor is left in place, so a bad rotation
/// (truncated file, expired cert) doesn't take TLS down.
#[allow(clippy::result_large_err)]
pub(crate) fn try_reload_tls(state: &TlsReloadState) -> Result<(), ServerError> {
    let new_acceptor = load_tls_acceptor(state.paths.borrow())?;
    *state.acceptor.lock() = Arc::new(new_acceptor);
    Ok(())
}

/// Load the initial acceptor and wrap it with its paths for hot reload.
#[allow(clippy::result_large_err)]
pub(super) fn build_tls_state(paths: &TlsPaths<'_>) -> Result<Arc<TlsReloadState>, ServerError> {
    let acceptor = load_tls_acceptor(TlsPaths {
        cert: paths.cert,
        key: paths.key,
        min_version: paths.min_version,
        client_ca: paths.client_ca,
    })?;
    Ok(Arc::new(TlsReloadState {
        paths: TlsConfigPaths::from_paths(paths),
        acceptor: parking_lot::Mutex::new(Arc::new(acceptor)),
    }))
}

/// Watch the cert / key / client-CA files and call `try_reload_tls` after
/// debounced changes. Mirrors the pattern in `watcher.rs` (parent-dir
/// watch, debounce, then act). Runs until the channel closes; the caller
/// drives shutdown by aborting the task that owns this future.
pub(super) async fn watch_tls_files(state: Arc<TlsReloadState>, debounce: std::time::Duration) {
    use crossfire::{mpsc, MTx};
    use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};

    // Track raw + canonical paths so symlink-swap rotations and edits to
    // the symlink target both produce visible events.
    let mut raw_targets: Vec<std::path::PathBuf> = Vec::new();
    let mut canon_targets: Vec<std::path::PathBuf> = Vec::new();
    for path in [
        Some(state.paths.cert.clone()),
        Some(state.paths.key.clone()),
        state.paths.client_ca.clone(),
    ]
    .into_iter()
    .flatten()
    {
        match path.canonicalize() {
            Ok(canonical) => {
                canon_targets.push(canonical);
                raw_targets.push(path);
            }
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "pgwire TLS watcher: cannot canonicalize path; reload disabled",
                );
                return;
            }
        }
    }
    let mut dirs: Vec<std::path::PathBuf> = raw_targets
        .iter()
        .chain(canon_targets.iter())
        .filter_map(|p| p.parent().map(|d| d.to_path_buf()))
        .collect();
    dirs.sort();
    dirs.dedup();

    let (tx, rx) = mpsc::bounded_async::<()>(16);
    let blocking_tx: MTx<_> = tx.clone().into_blocking();
    let watch_raw = raw_targets.clone();
    let watch_canon = canon_targets.clone();

    let mut watcher: RecommendedWatcher = match notify::recommended_watcher(
        move |result: Result<Event, notify::Error>| match result {
            Ok(event) => {
                let touched = event.paths.iter().any(|p| {
                    watch_raw.iter().any(|t| t == p)
                        || p.canonicalize()
                            .ok()
                            .as_ref()
                            .is_some_and(|c| watch_canon.contains(c))
                });
                if touched {
                    let _ = blocking_tx.send(());
                }
            }
            Err(e) => warn!(error = %e, "pgwire TLS watcher: notify error"),
        },
    ) {
        Ok(w) => w,
        Err(e) => {
            warn!(error = %e, "pgwire TLS watcher: failed to create watcher; reload disabled");
            return;
        }
    };

    for dir in &dirs {
        if let Err(e) = watcher.watch(dir, RecursiveMode::NonRecursive) {
            warn!(
                dir = %dir.display(),
                error = %e,
                "pgwire TLS watcher: failed to watch directory; reload disabled",
            );
            return;
        }
    }
    info!(
        files = ?raw_targets.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
        "pgwire TLS watcher started",
    );

    loop {
        if rx.recv().await.is_err() {
            return;
        }
        // Debounce: sleep then drain so a burst of inotify events
        // (cert + key written separately) coalesces into one reload.
        tokio::time::sleep(debounce).await;
        while rx.try_recv().is_ok() {}

        match try_reload_tls(&state) {
            Ok(()) => tracing::info!(
                target: "audit",
                event = "pgwire.tls_reload",
                outcome = "ok",
            ),
            Err(e) => tracing::warn!(
                target: "audit",
                event = "pgwire.tls_reload",
                outcome = "failed",
                error = %e,
                "pgwire TLS reload failed; previous certificate kept",
            ),
        }
    }
}

/// Minimum TLS protocol version accepted on the pgwire listener. rustls
/// already disables TLS 1.0/1.1; this narrows further when an operator
/// needs TLS 1.3 only.
#[derive(Clone, Copy, Debug)]
pub enum TlsMinVersion {
    V1_2,
    V1_3,
}

impl TlsMinVersion {
    pub(crate) fn from_config_str(s: &str) -> Option<Self> {
        match s {
            "1.2" => Some(Self::V1_2),
            "1.3" => Some(Self::V1_3),
            _ => None,
        }
    }

    fn versions(self) -> &'static [&'static tokio_rustls::rustls::SupportedProtocolVersion] {
        use tokio_rustls::rustls::version::{TLS12, TLS13};
        static BOTH: &[&tokio_rustls::rustls::SupportedProtocolVersion] = &[&TLS12, &TLS13];
        static ONLY_13: &[&tokio_rustls::rustls::SupportedProtocolVersion] = &[&TLS13];
        match self {
            Self::V1_2 => BOTH,
            Self::V1_3 => ONLY_13,
        }
    }

    pub(super) fn label(self) -> &'static str {
        match self {
            Self::V1_2 => "1.2",
            Self::V1_3 => "1.3",
        }
    }
}

/// Warn if the key file is group/other-readable.
#[cfg(unix)]
fn warn_if_key_world_readable(file: &std::fs::File, path: &std::path::Path) {
    use std::os::unix::fs::MetadataExt;
    if let Ok(meta) = file.metadata() {
        let mode = meta.mode();
        if mode & 0o077 != 0 {
            warn!(
                path = %path.display(),
                mode = format!("{:o}", mode & 0o777),
                "pgwire_tls_key permissions are too broad; tighten to 0600",
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_key_world_readable(_file: &std::fs::File, _path: &std::path::Path) {}

/// Reject certs past `notAfter`; warn within 30 days.
#[allow(clippy::result_large_err)]
fn check_cert_expiry(
    der: &tokio_rustls::rustls::pki_types::CertificateDer<'_>,
    path: &std::path::Path,
) -> Result<(), ServerError> {
    use x509_parser::prelude::FromDer;
    let (_, cert) = x509_parser::certificate::X509Certificate::from_der(der.as_ref())
        .map_err(|e| ServerError::Http(format!("parse pgwire_tls_cert {}: {e}", path.display())))?;
    let now = x509_parser::time::ASN1Time::now();
    let not_after = cert.validity().not_after;
    if not_after < now {
        return Err(ServerError::Http(format!(
            "pgwire_tls_cert {} expired at {not_after}",
            path.display()
        )));
    }
    let remaining = not_after.to_datetime() - now.to_datetime();
    if remaining <= time::Duration::days(30) {
        warn!(
            path = %path.display(),
            expires_at = %not_after,
            "pgwire_tls_cert expires within 30 days; rotate before it lapses",
        );
    }
    Ok(())
}

/// Idempotent install of aws-lc-rs as rustls' default provider.
pub(super) fn ensure_tls_provider() {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
}

#[allow(clippy::result_large_err)]
pub(super) fn load_tls_acceptor(
    paths: TlsPaths<'_>,
) -> Result<tokio_rustls::TlsAcceptor, ServerError> {
    use std::fs::File;
    use std::io::BufReader;

    ensure_tls_provider();

    let cert_file = File::open(paths.cert)
        .map_err(|e| ServerError::Http(format!("open pgwire_tls_cert: {e}")))?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ServerError::Http(format!("parse pgwire_tls_cert: {e}")))?;
    if certs.is_empty() {
        return Err(ServerError::Http(format!(
            "pgwire_tls_cert {} contains no certificates",
            paths.cert.display()
        )));
    }
    for cert in &certs {
        check_cert_expiry(cert, paths.cert)?;
    }

    let key_file = File::open(paths.key)
        .map_err(|e| ServerError::Http(format!("open pgwire_tls_key: {e}")))?;
    warn_if_key_world_readable(&key_file, paths.key);
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .map_err(|e| ServerError::Http(format!("parse pgwire_tls_key: {e}")))?
        .ok_or_else(|| {
            ServerError::Http(format!(
                "pgwire_tls_key {} contains no private key",
                paths.key.display()
            ))
        })?;

    let builder = tokio_rustls::rustls::ServerConfig::builder_with_protocol_versions(
        paths.min_version.versions(),
    );
    let builder = match paths.client_ca {
        Some(ca_path) => {
            let verifier = build_client_cert_verifier(ca_path)?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let server_config = builder
        .with_single_cert(certs, key)
        .map_err(|e| ServerError::Http(format!("rustls server config: {e}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
}

#[allow(clippy::result_large_err)]
fn build_client_cert_verifier(
    ca_path: &std::path::Path,
) -> Result<Arc<dyn tokio_rustls::rustls::server::danger::ClientCertVerifier>, ServerError> {
    use std::fs::File;
    use std::io::BufReader;
    use tokio_rustls::rustls::server::WebPkiClientVerifier;
    use tokio_rustls::rustls::RootCertStore;

    let file = File::open(ca_path)
        .map_err(|e| ServerError::Http(format!("open pgwire_tls_client_ca: {e}")))?;
    let mut roots = RootCertStore::empty();
    let mut added = 0usize;
    for cert in rustls_pemfile::certs(&mut BufReader::new(file)) {
        let cert =
            cert.map_err(|e| ServerError::Http(format!("parse pgwire_tls_client_ca: {e}")))?;
        roots
            .add(cert)
            .map_err(|e| ServerError::Http(format!("invalid CA in pgwire_tls_client_ca: {e}")))?;
        added += 1;
    }
    if added == 0 {
        return Err(ServerError::Http(format!(
            "pgwire_tls_client_ca {} contains no certificates",
            ca_path.display()
        )));
    }
    WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|e| ServerError::Http(format!("build client-cert verifier: {e}")))
}
