//! HTTP API for the LaminarDB server.
//!
//! Child modules own one API domain each: `ops` (health/readiness/metrics/reload),
//! `catalog` (query + catalog + lineage), `checkpoints`, `cluster_admin`,
//! `cluster_evidence` (local authority evidence), `ws` (stream subscriptions),
//! `auth` (token policy + middleware), `router` (route table + serving lifecycle),
//! `state` (shared app state + serving gate), and `json_encoding` (exact JSON
//! value contract shared by SQL responses and WS frames).

mod auth;
mod catalog;
mod checkpoints;
mod cluster_admin;
mod cluster_evidence;
mod json_encoding;
mod ops;
mod router;
mod state;
mod ws;

pub use router::{bind_listener, build_router, serve_listener};
pub use state::AppState;
#[cfg(feature = "cluster")]
pub use state::ClusterComponents;

#[cfg(feature = "cluster")]
pub(crate) use auth::DiagnosticReadGate;
pub(crate) use auth::HttpAuthPolicy;
pub(crate) use state::ServingGate;
pub(crate) use ws::ws_connection_slots;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: String,
}

fn error_response(status: StatusCode, msg: impl Into<String>) -> impl IntoResponse {
    (status, Json(ErrorBody { error: msg.into() }))
}

#[cfg(test)]
mod tests;
