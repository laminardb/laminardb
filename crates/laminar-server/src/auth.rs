//! Authentication and authorization middleware for the HTTP API.
//!
//! When the `auth` feature is enabled, provides:
//! - JWT bearer token extraction from `Authorization` headers
//! - RBAC permission checks per endpoint
//! - Audit logging of auth events
//!
//! When `auth` is disabled, all requests are allowed (no-op middleware).

#[cfg(feature = "auth")]
mod enabled {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::extract::{ConnectInfo, State};
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::middleware::Next;
    use axum::response::IntoResponse;
    use axum::Json;
    use serde::Serialize;

    use laminar_auth::audit::{AuditAction, AuditEvent, AuditLogger};
    use laminar_auth::identity::{AuthError, AuthProvider, Credentials, Identity};
    use laminar_auth::rbac::{Permission, RbacPolicy};

    /// Shared auth state stored in axum extensions.
    pub struct AuthState {
        /// The authentication provider (e.g., JWT).
        pub provider: Box<dyn AuthProvider>,
        /// RBAC policy for permission checks.
        pub policy: RbacPolicy,
        /// Audit logger.
        pub audit: Arc<AuditLogger>,
    }

    /// Error response body for auth failures.
    #[derive(Debug, Serialize)]
    struct AuthErrorBody {
        error: String,
        code: &'static str,
    }

    /// Extract a bearer token from the `Authorization` header.
    fn extract_bearer(headers: &HeaderMap) -> Option<String> {
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(|t| t.trim().to_string())
    }

    /// Axum middleware: authenticate the request and inject [`Identity`] into
    /// request extensions.
    ///
    /// - Extracts `Authorization: Bearer <token>` header
    /// - Validates via the configured [`AuthProvider`]
    /// - Stores the [`Identity`] in request extensions for downstream handlers
    /// - Returns 401 Unauthorized on failure
    pub async fn auth_middleware(
        State(auth): State<Arc<AuthState>>,
        mut req: Request<axum::body::Body>,
        next: Next,
    ) -> impl IntoResponse {
        let source_ip = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ci| ci.0.ip());

        let token = match extract_bearer(req.headers()) {
            Some(t) => t,
            None => {
                let event = AuditEvent::new(AuditAction::LoginFailed, "anonymous")
                    .failed()
                    .with_detail("missing Authorization header");
                if let Some(ip) = source_ip {
                    auth.audit.log(event.with_source_ip(ip));
                } else {
                    auth.audit.log(event);
                }

                return (
                    StatusCode::UNAUTHORIZED,
                    Json(AuthErrorBody {
                        error: "missing or invalid Authorization header".to_string(),
                        code: "AUTH_REQUIRED",
                    }),
                )
                    .into_response();
            }
        };

        let identity = match auth
            .provider
            .authenticate(&Credentials::Bearer(token))
            .await
        {
            Ok(id) => {
                let mut event = AuditEvent::new(AuditAction::Login, &id.user_id);
                if let Some(ip) = source_ip {
                    event = event.with_source_ip(ip);
                }
                auth.audit.log(event);
                id
            }
            Err(e) => {
                let msg = match &e {
                    AuthError::InvalidCredentials(m) => m.clone(),
                    AuthError::Expired => "token expired".to_string(),
                    other => other.to_string(),
                };

                let mut event = AuditEvent::new(AuditAction::LoginFailed, "unknown")
                    .failed()
                    .with_detail(&msg);
                if let Some(ip) = source_ip {
                    event = event.with_source_ip(ip);
                }
                auth.audit.log(event);

                return (
                    StatusCode::UNAUTHORIZED,
                    Json(AuthErrorBody {
                        error: msg,
                        code: "AUTH_FAILED",
                    }),
                )
                    .into_response();
            }
        };

        // Store identity in extensions for handlers
        req.extensions_mut().insert(identity);
        next.run(req).await.into_response()
    }

    /// Create an axum middleware layer that checks a specific [`Permission`]
    /// before allowing the request through.
    ///
    /// Must be applied **after** [`auth_middleware`] (which injects the
    /// [`Identity`] into extensions).
    pub async fn require_permission(
        State(auth): State<Arc<AuthState>>,
        req: Request<axum::body::Body>,
        next: Next,
        permission: Permission,
    ) -> impl IntoResponse {
        let identity = match req.extensions().get::<Identity>() {
            Some(id) => id.clone(),
            None => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(AuthErrorBody {
                        error: "not authenticated".to_string(),
                        code: "AUTH_REQUIRED",
                    }),
                )
                    .into_response();
            }
        };

        if !auth.policy.is_permitted(&identity, &permission) {
            let source_ip = req
                .extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ci| ci.0.ip());
            let mut event = AuditEvent::new(AuditAction::AccessDenied, &identity.user_id)
                .failed()
                .with_detail(format!("missing permission: {permission:?}"));
            if let Some(ip) = source_ip {
                event = event.with_source_ip(ip);
            }
            auth.audit.log(event);

            return (
                StatusCode::FORBIDDEN,
                Json(AuthErrorBody {
                    error: format!("access denied: insufficient permissions ({permission:?})"),
                    code: "ACCESS_DENIED",
                }),
            )
                .into_response();
        }

        next.run(req).await.into_response()
    }
}

#[cfg(feature = "auth")]
pub use enabled::{auth_middleware, require_permission, AuthState};
