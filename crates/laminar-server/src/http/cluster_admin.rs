//! Cluster administration endpoints: mode status, membership, vnode assignment,
//! leader, and cluster-wide pipeline start/stop fan-out.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Serialize;

use crate::config::ServerMode;

use super::error_response;
use super::state::AppState;

/// 404 returned by the cluster endpoints when the server is not running in
/// cluster mode (single-node, or compiled without the `cluster` feature).
#[cfg(feature = "cluster")]
pub(super) const CLUSTER_DISABLED_MSG: &str =
    "cluster endpoints are only available in cluster mode";
#[cfg(not(feature = "cluster"))]
pub(super) const CLUSTER_DISABLED_MSG: &str = "cluster endpoints require the `cluster` feature";

pub(super) async fn cluster_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let config = state.current_config.read();
    if config.server.mode != ServerMode::Cluster {
        return error_response(
            StatusCode::NOT_FOUND,
            "cluster endpoint is only available when server.mode = \"cluster\"",
        )
        .into_response();
    }

    let node_id = config.node_id.clone().unwrap_or_default();
    drop(config);

    #[derive(Serialize)]
    struct ClusterStatusResponse {
        mode: &'static str,
        node_id: String,
        pipeline_state: &'static str,
        #[cfg(feature = "cluster")]
        subscription_output: Option<SubscriptionOutputHealthResponse>,
    }

    #[cfg(feature = "cluster")]
    #[derive(Serialize)]
    struct SubscriptionOutputHealthResponse {
        active_readers: u64,
        pending_bytes: u64,
        retained_bytes: u64,
        orphan_bytes: u64,
        open_failures: u64,
        segment_write_failures: u64,
        manifest_failures: u64,
        integrity_failures: u64,
        stale_writer_rejections: u64,
        sequence_gaps: u64,
        lag_disconnects: u64,
    }

    #[cfg(feature = "cluster")]
    let subscription_output = state.db.cluster_subscription_output_health().map(|health| {
        SubscriptionOutputHealthResponse {
            active_readers: health.active_readers,
            pending_bytes: health.pending_bytes,
            retained_bytes: health.retained_bytes,
            orphan_bytes: health.orphan_bytes,
            open_failures: health.open_failures,
            segment_write_failures: health.segment_write_failures,
            manifest_failures: health.manifest_failures,
            integrity_failures: health.integrity_failures,
            stale_writer_rejections: health.stale_writer_rejections,
            sequence_gaps: health.sequence_gaps,
            lag_disconnects: health.lag_disconnects,
        }
    });

    let pipeline_state = state.db.pipeline_state();
    Json(ClusterStatusResponse {
        mode: "cluster",
        node_id,
        pipeline_state,
        #[cfg(feature = "cluster")]
        subscription_output,
    })
    .into_response()
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct PipelineControlParams {
    #[serde(default)]
    local: bool,
}

/// Fan a fire-and-forget pipeline-control POST out to every peer (so a
/// cluster-wide start/stop reaches the whole cluster). `local` short-circuits
/// the fan-out, and it is a no-op outside cluster mode. Peer failures are
/// logged, not fatal.
async fn fan_out_pipeline_control(state: &AppState, local: bool, path: &str) {
    #[cfg(feature = "cluster")]
    {
        if local {
            return;
        }
        let Some(cluster) = state.cluster.as_ref() else {
            return;
        };
        let self_id = cluster.controller.instance_id();
        let peers: Vec<String> = cluster
            .membership_rx
            .borrow()
            .iter()
            .filter(|m| self_id != m.id)
            .map(|m| m.rpc_address.clone())
            .collect();
        let token = state.auth_policy.console_token().cloned();
        let client = reqwest::Client::new();
        // Fan out concurrently with `join_all`; these are I/O-bound futures with
        // no CPU work, so there's nothing to gain from spawning a task each.
        let futures = peers.into_iter().map(|peer| {
            let client = client.clone();
            let token = token.clone();
            let url = format!("http://{peer}{path}");
            async move {
                let mut req = client
                    .post(&url)
                    .timeout(std::time::Duration::from_secs(10));
                if let Some(t) = token {
                    req = req.bearer_auth(t.expose());
                }
                if let Err(e) = req.send().await {
                    tracing::warn!("failed to forward pipeline control to {url}: {e}");
                }
            }
        });
        futures::future::join_all(futures).await;
    }
    #[cfg(not(feature = "cluster"))]
    let _ = (state, local, path);
}

pub(super) async fn stop_pipeline(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PipelineControlParams>,
) -> impl IntoResponse {
    // Stop locally first; only fan out to peers once this node succeeds, so a
    // local failure doesn't leave the cluster in a split (some stopped) state.
    if let Err(e) = state.db.stop_pipeline().await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    fan_out_pipeline_control(&state, params.local, "/api/v1/pipeline/stop?local=true").await;
    (
        StatusCode::OK,
        Json(serde_json::json!({ "message": "Pipeline suspended successfully" })),
    )
        .into_response()
}

pub(super) async fn start_pipeline(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PipelineControlParams>,
) -> impl IntoResponse {
    // Start locally first; only fan out to peers once this node succeeds.
    if let Err(e) = state.db.start().await {
        return error_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    fan_out_pipeline_control(&state, params.local, "/api/v1/pipeline/start?local=true").await;
    (
        StatusCode::OK,
        Json(serde_json::json!({ "message": "Pipeline started successfully" })),
    )
        .into_response()
}

pub(super) async fn pipeline_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let pipeline_state = state.db.pipeline_state();
    let mut body = serde_json::json!({ "pipeline_state": pipeline_state });
    // The panic is async (after the DDL/start call returned), so surface it here.
    if let Some(reason) = state.db.last_fault() {
        body["last_error"] = serde_json::Value::String(reason);
    }
    (StatusCode::OK, Json(body)).into_response()
}

/// `GET /api/v1/cluster/nodes` — current cluster membership.
pub(super) async fn cluster_nodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        // Clone the snapshot out of the watch guard so it isn't held across
        // serialization.
        let nodes: Vec<laminar_core::cluster::discovery::NodeInfo> =
            cluster.membership_rx.borrow().clone();
        Json(nodes).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}

/// `GET /api/v1/cluster/vnodes` — the latest vnode-to-instance assignment snapshot.
pub(super) async fn cluster_vnodes(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        let snapshot = match cluster.snapshot_store.load().await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "durable assignment snapshot is missing",
                )
                .into_response();
            }
            Err(error) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to load assignment snapshot: {error}"),
                )
                .into_response();
            }
        };
        Json(snapshot).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}

/// `GET /api/v1/cluster/leader` — the current leader's `NodeInfo` (if known)
/// and whether this node is the leader.
pub(super) async fn cluster_leader(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[cfg(feature = "cluster")]
    {
        #[derive(Serialize)]
        struct LeaderResponse {
            leader: Option<laminar_core::cluster::discovery::NodeInfo>,
            is_leader: bool,
        }

        let Some(cluster) = state.cluster.as_ref() else {
            return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
        };
        let leader_id = cluster.controller.current_leader();
        let is_leader = cluster.controller.is_leader();
        let leader = leader_id.and_then(|id| {
            cluster
                .membership_rx
                .borrow()
                .iter()
                .find(|n| n.id == id)
                .cloned()
        });
        Json(LeaderResponse { leader, is_leader }).into_response()
    }
    #[cfg(not(feature = "cluster"))]
    {
        let _ = state;
        error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
    }
}
