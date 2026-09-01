//! PgWire session lifecycle: the shared query handler, startup authentication
//! (trust/MD5), per-connection admission and session permits, handler factory,
//! and auth-failure throttling.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::md5pass::{hash_md5_password, Md5PasswordAuthStartupHandler};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::cancel::DefaultCancelHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::{ClientInfo, ConnectionManager, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::info;

use laminar_db::LaminarDB;

use crate::config::Secret;

use super::cursor::ConnState;

pub(crate) const MAX_PENDING_PGWIRE_HANDSHAKES: usize = 64;

pub struct LaminarPgwireHandler {
    pub(super) db: Arc<LaminarDB>,
    connection_manager: Arc<ConnectionManager>,
}

impl LaminarPgwireHandler {
    fn new(db: Arc<LaminarDB>, connection_manager: Arc<ConnectionManager>) -> Self {
        Self {
            db,
            connection_manager,
        }
    }

    pub(super) fn conn_state<C: ClientInfo>(&self, client: &C) -> Arc<ConnState> {
        client
            .session_extensions()
            .get_or_insert_with(ConnState::default)
    }
}

#[async_trait]
impl NoopStartupHandler for LaminarPgwireHandler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(Arc::clone(&self.connection_manager))
    }

    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        info!(peer = %client.socket_addr(), "pgwire client connected");
        Ok(())
    }
}

/// Per-call salt + stored credential for the MD5 challenge flow. The
/// stored value is either plaintext (legacy) or `md5<32-hex>`, the same
/// format Postgres' `pg_authid` uses, where the hex is `md5(password ‖
/// user)`. The pre-hashed form lets operators avoid plaintext at rest.
#[derive(Debug)]
struct LaminarAuthSource {
    users: Arc<HashMap<String, Secret>>,
}

/// If `stored` is a `pg_authid`-style pre-hash, return the inner hex
/// (the bit after the `md5` tag). Lowercase hex only; uppercase or
/// other lengths fall back to plaintext handling.
pub(crate) fn parse_pre_hashed_md5(stored: &str) -> Option<&str> {
    let inner = stored.strip_prefix("md5")?;
    if inner.len() == 32 && inner.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f')) {
        Some(inner)
    } else {
        None
    }
}

/// MD5 challenge response when only the inner hash is known: the client
/// sends `md5{hex(md5(inner_hex || salt))}` and the server precomputes
/// the same string for comparison.
fn outer_md5_challenge(inner_hex: &str, salt: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(inner_hex.as_bytes());
    hasher.update(salt);
    format!("md5{:x}", hasher.finalize())
}

#[async_trait]
impl AuthSource for LaminarAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login.user().unwrap_or("");
        // Indistinguishable from a wrong-password failure: both branches must
        // surface the same wire error so a client can't probe which usernames
        // are configured. pgwire emits exactly this variant on bad password.
        let stored = self
            .users
            .get(user)
            .ok_or_else(|| PgWireError::InvalidPassword(user.to_string()))?;
        let salt: [u8; 4] = rand::random();
        let expected = match parse_pre_hashed_md5(stored.expose()) {
            Some(inner_hex) => outer_md5_challenge(inner_hex, &salt),
            None => hash_md5_password(user, stored.expose(), &salt),
        };
        Ok(Password::new(Some(salt.to_vec()), expected.into_bytes()))
    }
}

type Md5Handler = Md5PasswordAuthStartupHandler<LaminarAuthSource, DefaultServerParameterProvider>;

/// Startup-phase dispatch. `Md5` requires password auth; `Trust` accepts any
/// connection. Selected once at listener startup based on whether
/// `pgwire_users` is non-empty.
enum StartupAuth {
    Trust(Arc<LaminarPgwireHandler>),
    Md5(Arc<Md5Handler>),
}

/// Permit held for the full authenticated-session lifetime through the
/// per-connection extension store.
struct SessionPermit {
    _permit: OwnedSemaphorePermit,
}

/// Admission wrapper created per accepted socket. The pending-handshake
/// permit protects TLS negotiation and startup decoding, then is released as
/// soon as the first valid Startup packet has been classified.
struct StartupAdmission {
    auth: Arc<StartupAuth>,
    sessions: Arc<Semaphore>,
    pending: parking_lot::Mutex<Option<OwnedSemaphorePermit>>,
    require_tls: bool,
}

#[async_trait]
impl StartupHandler for StartupAdmission {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if matches!(&message, PgWireFrontendMessage::Startup(_)) {
            // Classification is complete. CancelRequest never reaches this
            // handler and therefore never consumes a normal session slot.
            self.pending.lock().take();

            if self.require_tls && !client.is_secure() {
                return Err(fatal_startup_error(
                    "08004",
                    "TLS is required for this pgwire listener",
                ));
            }

            let permit = Arc::clone(&self.sessions)
                .try_acquire_owned()
                .map_err(|_| fatal_startup_error("53300", "too many pgwire connections"))?;
            client
                .session_extensions()
                .insert(SessionPermit { _permit: permit });
        }

        self.auth.on_startup(client, message).await
    }
}

fn fatal_startup_error(code: &str, message: impl Into<String>) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "FATAL".into(),
        code.into(),
        message.into(),
    )))
}

#[async_trait]
impl StartupHandler for StartupAuth {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            Self::Trust(h) => h.on_startup(client, message).await,
            Self::Md5(h) => h.on_startup(client, message).await,
        }
    }
}

pub(super) struct LaminarHandlerFactory {
    handler: Arc<LaminarPgwireHandler>,
    startup: Arc<StartupAuth>,
    cancel: Arc<DefaultCancelHandler>,
    sessions: Arc<Semaphore>,
    require_tls: bool,
}

impl LaminarHandlerFactory {
    pub(super) fn new(
        db: Arc<LaminarDB>,
        users: HashMap<String, Secret>,
        max_connections: usize,
        require_tls: bool,
    ) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new());
        let handler = Arc::new(LaminarPgwireHandler::new(
            db,
            Arc::clone(&connection_manager),
        ));
        let startup = if users.is_empty() {
            Arc::new(StartupAuth::Trust(Arc::clone(&handler)))
        } else {
            let auth = LaminarAuthSource {
                users: Arc::new(users),
            };
            let md5 = Md5PasswordAuthStartupHandler::new(
                Arc::new(auth),
                Arc::new(DefaultServerParameterProvider::default()),
            )
            .with_connection_manager(Arc::clone(&connection_manager));
            Arc::new(StartupAuth::Md5(Arc::new(md5)))
        };
        let cancel = Arc::new(DefaultCancelHandler::new(connection_manager));
        Self {
            handler,
            startup,
            cancel,
            sessions: Arc::new(Semaphore::new(max_connections)),
            require_tls,
        }
    }

    pub(super) fn for_connection(
        &self,
        pending: OwnedSemaphorePermit,
    ) -> LaminarConnectionHandlers {
        LaminarConnectionHandlers {
            handler: Arc::clone(&self.handler),
            startup: Arc::new(StartupAdmission {
                auth: Arc::clone(&self.startup),
                sessions: Arc::clone(&self.sessions),
                pending: parking_lot::Mutex::new(Some(pending)),
                require_tls: self.require_tls,
            }),
            cancel: Arc::clone(&self.cancel),
        }
    }
}

pub(super) struct LaminarConnectionHandlers {
    handler: Arc<LaminarPgwireHandler>,
    startup: Arc<StartupAdmission>,
    cancel: Arc<DefaultCancelHandler>,
}

impl PgWireServerHandlers for LaminarConnectionHandlers {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.handler)
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }

    fn cancel_handler(&self) -> Arc<impl pgwire::api::cancel::CancelHandler> {
        Arc::clone(&self.cancel)
    }
}

/// Rolling-window auth-failure count per peer IP.
#[derive(Debug, Default)]
pub(super) struct FailureTracker {
    pub(super) inner: parking_lot::Mutex<
        HashMap<std::net::IpAddr, std::collections::VecDeque<std::time::Instant>>,
    >,
}

impl FailureTracker {
    pub(super) fn is_blocked(
        &self,
        ip: std::net::IpAddr,
        limit: u32,
        window: std::time::Duration,
    ) -> bool {
        if limit == 0 {
            return false;
        }
        let cutoff = std::time::Instant::now() - window;
        let mut inner = self.inner.lock();
        let Some(failures) = inner.get_mut(&ip) else {
            return false;
        };
        while failures.front().is_some_and(|t| *t < cutoff) {
            failures.pop_front();
        }
        let blocked = failures.len() >= limit as usize;
        if failures.is_empty() {
            inner.remove(&ip);
        }
        blocked
    }

    pub(super) fn record_failure(&self, ip: std::net::IpAddr) {
        let mut inner = self.inner.lock();
        // When full, evict the entry whose newest failure is oldest.
        if !inner.contains_key(&ip) && inner.len() >= MAX_TRACKED_IPS {
            if let Some(oldest) = inner
                .iter()
                .min_by_key(|(_, q)| q.back().copied())
                .map(|(k, _)| *k)
            {
                inner.remove(&oldest);
            }
        }
        inner
            .entry(ip)
            .or_default()
            .push_back(std::time::Instant::now());
    }
}

pub(super) const MAX_TRACKED_IPS: usize = 4096;

/// Stable audit code for a session's exit status.
pub(super) fn classify_outcome(result: &Result<(), std::io::Error>) -> &'static str {
    match result {
        Ok(()) => "ok",
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("28P01") {
                "auth_failed"
            } else if msg.contains("HandshakeFailure")
                || msg.contains("rustls")
                || msg.contains("tls")
            {
                "tls_failed"
            } else {
                "error"
            }
        }
    }
}
