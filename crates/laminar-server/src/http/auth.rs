//! HTTP authentication: console bearer-token policy, diagnostic-read principals, and
//! the middleware layers enforcing them.
//!
//! INVARIANT: token comparison is constant-time; duplicate or comma-joined bearer values
//! fail closed; the `?token=` query escape hatch is restricted to `/ws/` upgrade requests.

use std::sync::Arc;
#[cfg(feature = "cluster")]
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
#[cfg(feature = "cluster")]
use axum::http::{HeaderMap, Method};
use axum::response::IntoResponse;

#[cfg(feature = "cluster")]
use crate::config::ServerMode;
use crate::config::{Secret, ServerSection};

use super::error_response;
use super::state::AppState;

#[derive(Clone)]
pub(crate) struct HttpAuthPolicy {
    console_token: Option<Secret>,
    #[cfg(feature = "cluster")]
    diagnostic_read_token: Option<Secret>,
    #[cfg(feature = "cluster")]
    cluster_mode: bool,
}

impl HttpAuthPolicy {
    pub(crate) fn from_server(server: &ServerSection) -> Self {
        Self {
            console_token: server.console_token.clone(),
            #[cfg(feature = "cluster")]
            diagnostic_read_token: server.diagnostic_read_token.clone(),
            #[cfg(feature = "cluster")]
            cluster_mode: server.mode == ServerMode::Cluster,
        }
    }

    pub(super) fn console_token(&self) -> Option<&Secret> {
        self.console_token.as_ref()
    }

    #[cfg(feature = "cluster")]
    fn diagnostic_principal(&self, headers: &HeaderMap) -> DiagnosticAuth {
        let Some(token) = single_bearer_token(headers) else {
            return if self.console_token.is_none() && self.diagnostic_read_token.is_none() {
                DiagnosticAuth::Unconfigured
            } else {
                DiagnosticAuth::Unauthorized
            };
        };
        if self
            .console_token
            .as_ref()
            .is_some_and(|expected| secret_matches(token, expected))
        {
            return DiagnosticAuth::Authorized(DiagnosticPrincipal::Console);
        }
        if self
            .diagnostic_read_token
            .as_ref()
            .is_some_and(|expected| secret_matches(token, expected))
        {
            return DiagnosticAuth::Authorized(DiagnosticPrincipal::DiagnosticRead);
        }
        DiagnosticAuth::Unauthorized
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg(feature = "cluster")]
pub(crate) enum DiagnosticPrincipal {
    Console,
    DiagnosticRead,
}

#[cfg(feature = "cluster")]
enum DiagnosticAuth {
    Unconfigured,
    Unauthorized,
    Authorized(DiagnosticPrincipal),
}

#[cfg(feature = "cluster")]
pub(super) const DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW: usize = 8;
#[cfg(feature = "cluster")]
pub(super) const DIAGNOSTIC_READ_RATE_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(1);
#[cfg(feature = "cluster")]
pub(super) const DIAGNOSTIC_READ_DEADLINE: std::time::Duration = std::time::Duration::from_secs(2);

#[cfg(feature = "cluster")]
pub(crate) struct DiagnosticRateWindow {
    pub(super) starts: [Option<Instant>; DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW],
}

#[cfg(feature = "cluster")]
impl Default for DiagnosticRateWindow {
    fn default() -> Self {
        Self {
            starts: [None; DIAGNOSTIC_READ_MAX_STARTS_PER_WINDOW],
        }
    }
}

#[cfg(feature = "cluster")]
impl DiagnosticRateWindow {
    pub(super) fn try_start(&mut self, now: Instant) -> bool {
        for start in &mut self.starts {
            if start.is_some_and(|started| {
                now.checked_duration_since(started)
                    .is_some_and(|elapsed| elapsed >= DIAGNOSTIC_READ_RATE_WINDOW)
            }) {
                *start = None;
            }
        }
        let Some(slot) = self.starts.iter_mut().find(|start| start.is_none()) else {
            return false;
        };
        *slot = Some(now);
        true
    }
}

/// Single-flight + rate bound + deadline for local diagnostic reads.
#[cfg(feature = "cluster")]
pub(crate) struct DiagnosticReadGate {
    pub(super) permit: Arc<tokio::sync::Semaphore>,
    pub(super) rate: parking_lot::Mutex<DiagnosticRateWindow>,
}

#[cfg(feature = "cluster")]
impl DiagnosticReadGate {
    pub(crate) fn new() -> Self {
        Self {
            permit: Arc::new(tokio::sync::Semaphore::new(1)),
            rate: parking_lot::Mutex::new(DiagnosticRateWindow::default()),
        }
    }
}

/// Bearer-token gate for the control-plane API. When `server.console_token` is
/// configured, every request to a protected route must present the token
/// either as an `Authorization: Bearer <token>` header or as a `?token=<token>`
/// query parameter (the latter for browser WebSocket clients, which can't set
/// custom headers on the upgrade request). When no token is configured the
/// HTTP API is left open — loopback/dev only.
pub(super) async fn auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if let Some(expected) = state.auth_policy.console_token() {
        // The `?token=` query parameter exists for browser WebSocket clients,
        // which can't set the `Authorization` header on the upgrade request.
        // Restrict it to WS routes (`/ws/…`) so it can't leak into access
        // logs, referrers, or proxy caches on regular control-plane requests.
        let is_ws = req.uri().path().starts_with("/ws/");
        let authorized = single_bearer_token(req.headers())
            .is_some_and(|token| secret_matches(token, expected))
            || (is_ws
                && query_token(req.uri()).is_some_and(|token| secret_matches(&token, expected)));
        if !authorized {
            return error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }

    next.run(req).await
}

#[cfg(feature = "cluster")]
pub(super) async fn diagnostic_auth_middleware(
    State(state): State<Arc<AppState>>,
    mut req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    if !state.auth_policy.cluster_mode {
        return next.run(req).await;
    }
    if req.uri().scheme().is_some() || req.uri().authority().is_some() {
        return error_response(StatusCode::BAD_REQUEST, "invalid diagnostic request target")
            .into_response();
    }
    match state.auth_policy.diagnostic_principal(req.headers()) {
        DiagnosticAuth::Unconfigured => next.run(req).await,
        DiagnosticAuth::Unauthorized => {
            error_response(StatusCode::UNAUTHORIZED, "unauthorized").into_response()
        }
        DiagnosticAuth::Authorized(principal) => {
            req.extensions_mut().insert(principal);
            next.run(req).await
        }
    }
}

#[cfg(not(feature = "cluster"))]
pub(super) async fn diagnostic_auth_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let _ = state;
    next.run(req).await
}

#[cfg(feature = "cluster")]
pub(super) async fn diagnostic_bounds_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let bounded_get = req.method() == Method::GET
        && matches!(
            req.uri().path(),
            "/api/v1/cluster/local-evidence" | "/api/v1/cluster/local-checkpoint-barrier-timings"
        )
        && req.extensions().get::<DiagnosticPrincipal>().is_some();
    if !bounded_get {
        return next.run(req).await;
    }

    let Ok(_permit) = Arc::clone(&state.diagnostic_reads.permit).try_acquire_owned() else {
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "local diagnostic request already in progress",
        )
        .into_response();
    };
    if !state.diagnostic_reads.rate.lock().try_start(Instant::now()) {
        return error_response(
            StatusCode::TOO_MANY_REQUESTS,
            "local diagnostic request rate exceeded",
        )
        .into_response();
    }

    match tokio::time::timeout(DIAGNOSTIC_READ_DEADLINE, next.run(req)).await {
        Ok(response) => response,
        Err(_) => error_response(
            StatusCode::GATEWAY_TIMEOUT,
            "local diagnostic request timed out",
        )
        .into_response(),
    }
}

#[cfg(not(feature = "cluster"))]
pub(super) async fn diagnostic_bounds_middleware(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let _ = state;
    next.run(req).await
}

pub(super) async fn diagnostic_method_not_allowed() -> impl IntoResponse {
    error_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "diagnostic route requires GET",
    )
}

/// Extract exactly one bearer value. Duplicate fields and comma-joined values fail closed.
fn single_bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    let mut values = headers.get_all(axum::http::header::AUTHORIZATION).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    let token = value.to_str().ok()?.strip_prefix("Bearer ")?;
    (!token.contains(',')).then_some(token)
}

fn secret_matches(presented: &str, expected: &Secret) -> bool {
    let expected = expected.expose();
    presented.len() == expected.len() && ct_eq(presented, expected)
}

/// Extract and percent-decode the `token` query parameter, if present. Browser
/// WebSocket clients URL-encode the value, so it must be decoded before the
/// constant-time comparison.
pub(super) fn query_token(uri: &axum::http::Uri) -> Option<String> {
    let raw = uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == "token").then_some(value)
    })?;
    Some(
        percent_encoding::percent_decode_str(raw)
            .decode_utf8_lossy()
            .into_owned(),
    )
}

/// Constant-time string comparison so token validation doesn't leak the secret
/// through response timing.
fn ct_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    a.as_bytes().ct_eq(b.as_bytes()).unwrap_u8() == 1
}
