//! Postgres wire endpoint. Trust by default; MD5 with `pgwire_users`;
//! TLS with `pgwire_tls_cert` + `pgwire_tls_key`. Non-loopback binds
//! require authenticated users, TLS, and `pgwire_allow_remote = true`.
//!
//! Child modules: `session` (handler factory, startup auth, admission,
//! failure throttling), `dispatch` (simple/extended SQL dispatch and
//! SQLSTATE mapping), `cursor` (DECLARE/FETCH/CLOSE over subscriptions),
//! `subscription` (portal open + envelope row encoding), `encoding`
//! (Arrow→Postgres row encoding), `tls` (acceptor + hot reload). The
//! facade owns the listener lifecycle (`serve`).

mod cursor;
mod dispatch;
mod encoding;
mod session;
mod subscription;
mod tls;

pub use tls::{TlsMinVersion, TlsPaths};

// Names the cfg(test) suites inside this module tree import through `super::`.
#[cfg(test)]
use cursor::SUBSCRIPTION_FETCH_WAIT;
#[cfg(test)]
use session::{parse_pre_hashed_md5, FailureTracker, MAX_TRACKED_IPS};
#[cfg(test)]
use subscription::{
    SUBSCRIPTION_CHECKPOINT_COLUMN, SUBSCRIPTION_EPOCH_COLUMN, SUBSCRIPTION_KIND_COLUMN,
    SUBSCRIPTION_LOG_SEQUENCE_COLUMN, SUBSCRIPTION_ROW_INDEX_COLUMN,
    SUBSCRIPTION_THROUGH_SEQUENCE_COLUMN,
};
#[cfg(test)]
use tls::{ensure_tls_provider, load_tls_acceptor, try_reload_tls, TlsConfigPaths};

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use pgwire::tokio::process_socket;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use laminar_db::LaminarDB;

use crate::config::Secret;
use crate::server::ServerError;

use session::{classify_outcome, LaminarHandlerFactory, MAX_PENDING_PGWIRE_HANDSHAKES};
use tls::{build_tls_state, watch_tls_files, TlsReloadState};

/// Fail-closed listener admission: trust auth never leaves loopback, and MD5
/// leaves loopback only with the explicit remote opt-in.
#[allow(clippy::result_large_err)]
fn validate_bind_admission(
    addr: SocketAddr,
    users_empty: bool,
    allow_remote: bool,
) -> Result<(), ServerError> {
    let auth_mode = if users_empty { "trust" } else { "md5" };
    let is_remote_bind = !addr.ip().is_loopback();
    match (auth_mode, is_remote_bind, allow_remote) {
        ("trust", true, _) => Err(ServerError::Http(format!(
            "pgwire_bind '{addr}' is not loopback and pgwire_users is empty (trust auth); \
             configure pgwire_users + pgwire_allow_remote=true, or bind to 127.0.0.1"
        ))),
        ("md5", true, false) => Err(ServerError::Http(format!(
            "pgwire_bind '{addr}' is not loopback; set pgwire_allow_remote=true to opt in"
        ))),
        _ => Ok(()),
    }
}

/// Immutable listener mode labels shared by the accept-time audit events.
#[derive(Clone, Copy)]
struct ListenerMode {
    auth: &'static str,
    tls: &'static str,
}

/// Gate one accepted socket: bounded pending handshakes, then the rolling
/// auth-failure throttle. `None` rejects the socket (audit-logged).
fn admit_connection(
    pending_handshakes: &Arc<Semaphore>,
    failures: &session::FailureTracker,
    peer: SocketAddr,
    max_auth_failures_per_min: u32,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    let Ok(pending) = Arc::clone(pending_handshakes).try_acquire_owned() else {
        tracing::info!(
            target: "audit",
            event = "pgwire.connection_rejected",
            peer = %peer,
            reason = "pending_handshake_limit",
            in_flight = MAX_PENDING_PGWIRE_HANDSHAKES,
        );
        return None;
    };
    if failures.is_blocked(
        peer.ip(),
        max_auth_failures_per_min,
        std::time::Duration::from_secs(60),
    ) {
        tracing::warn!(
            target: "audit",
            event = "pgwire.connection_rejected",
            peer = %peer,
            reason = "auth_failure_throttle",
        );
        return None;
    }
    Some(pending)
}

/// Spawn the session for one admitted socket with connection audit events.
fn spawn_connection_session(
    sessions: &mut tokio::task::JoinSet<()>,
    sock: tokio::net::TcpStream,
    peer: SocketAddr,
    tls: Option<tokio_rustls::TlsAcceptor>,
    handlers: session::LaminarConnectionHandlers,
    failures: Arc<session::FailureTracker>,
    mode: ListenerMode,
) {
    tracing::info!(
        target: "audit",
        event = "pgwire.connection_accepted",
        peer = %peer,
        auth = mode.auth,
        tls = mode.tls,
    );
    let peer_ip = peer.ip();
    let peer_str = peer.to_string();
    sessions.spawn(async move {
        let result = process_socket(sock, tls, handlers).await;
        let outcome = classify_outcome(&result);
        if outcome == "auth_failed" {
            failures.record_failure(peer_ip);
        }
        tracing::info!(
            target: "audit",
            event = "pgwire.connection_closed",
            peer = %peer_str,
            outcome,
        );
        if let Err(e) = result {
            warn!(peer = %peer_str, error = %e, "pgwire connection error");
        }
    });
}

/// Warn on TRUST auth (any reachable client is admin); otherwise log the
/// listener mode once at startup.
fn log_listener_mode(local_addr: SocketAddr, mode: ListenerMode, tls_min: &str, mtls: &str) {
    if mode.auth == "trust" {
        warn!(
            addr = %local_addr,
            tls = mode.tls,
            tls_min,
            mtls,
            "pgwire listening with TRUST auth — any client reaching this address is admin",
        );
    } else {
        info!(
            addr = %local_addr,
            auth = mode.auth,
            tls = mode.tls,
            tls_min,
            mtls,
            "pgwire listening",
        );
    }
}

pub async fn serve(
    db: Arc<LaminarDB>,
    bind: &str,
    users: HashMap<String, Secret>,
    allow_remote: bool,
    tls: Option<TlsPaths<'_>>,
    max_connections: usize,
    max_auth_failures_per_min: u32,
) -> Result<(SocketAddr, tokio::task::JoinHandle<()>), ServerError> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| ServerError::Http(format!("invalid pgwire_bind '{bind}': {e}")))?;

    validate_bind_admission(addr, users.is_empty(), allow_remote)?;

    let auth_mode = if users.is_empty() { "trust" } else { "md5" };
    let tls_min_label = tls.as_ref().map(|p| p.min_version.label());
    let mtls_on = tls.as_ref().is_some_and(|p| p.client_ca.is_some());
    let tls_state: Option<Arc<TlsReloadState>> = match tls {
        Some(paths) => Some(build_tls_state(&paths)?),
        None => None,
    };

    let listener = TcpListener::bind(addr)
        .await
        .map_err(|e| ServerError::Http(format!("pgwire bind {addr}: {e}")))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| ServerError::Http(format!("pgwire local_addr: {e}")))?;
    let require_tls = !local_addr.ip().is_loopback() || mtls_on;
    if require_tls && tls_state.is_none() {
        return Err(ServerError::Http(format!(
            "pgwire listener '{local_addr}' requires pgwire_tls_cert + pgwire_tls_key"
        )));
    }

    let tls_mode = if tls_state.is_some() { "on" } else { "off" };
    let tls_min = tls_min_label.unwrap_or("-");
    let mtls = if mtls_on { "on" } else { "off" };
    log_listener_mode(
        local_addr,
        ListenerMode {
            auth: auth_mode,
            tls: tls_mode,
        },
        tls_min,
        mtls,
    );

    // Track per-connection tasks so abort on the outer JoinHandle stops
    // active sessions in addition to the accept loop.
    let failures = Arc::new(session::FailureTracker::default());
    let factory = Arc::new(LaminarHandlerFactory::new(
        db,
        users,
        max_connections,
        require_tls,
    ));
    let pending_handshakes = Arc::new(Semaphore::new(MAX_PENDING_PGWIRE_HANDSHAKES));
    let watcher_state = tls_state.as_ref().map(Arc::clone);
    let watcher_disabled =
        std::env::var("LAMINAR_DISABLE_FILE_WATCH").is_ok_and(|v| v == "1" || v == "true");
    let handle = tokio::spawn(async move {
        let mut sessions: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        // Watcher in its own JoinSet so it doesn't count toward max_connections.
        let mut watcher_set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        if let (Some(state), false) = (watcher_state, watcher_disabled) {
            watcher_set.spawn(async move {
                watch_tls_files(state, std::time::Duration::from_millis(500)).await;
            });
        }
        loop {
            tokio::select! {
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {
                    // Reap completed sessions; nothing to do with the result.
                }
                Some(_) = watcher_set.join_next(), if !watcher_set.is_empty() => {}
                accepted = listener.accept() => {
                    match accepted {
                        Ok((sock, peer)) => {
                            let Some(pending) = admit_connection(
                                &pending_handshakes,
                                &failures,
                                peer,
                                max_auth_failures_per_min,
                            ) else {
                                drop(sock);
                                continue;
                            };
                            let handlers = factory.for_connection(pending);
                            // Snapshot the live acceptor so that an in-flight
                            // handshake completes against whatever cert was
                            // current when the socket was accepted, even if a
                            // hot-reload swaps it under us.
                            let tls_ref: Option<tokio_rustls::TlsAcceptor> =
                                tls_state.as_ref().map(|s| (*s.snapshot()).clone());
                            spawn_connection_session(
                                &mut sessions,
                                sock,
                                peer,
                                tls_ref,
                                handlers,
                                Arc::clone(&failures),
                                ListenerMode {
                                    auth: auth_mode,
                                    tls: tls_mode,
                                },
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "pgwire accept failed");
                            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        }
                    }
                }
            }
        }
    });
    Ok((local_addr, handle))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod integration_tests;
