//! Query and catalog endpoints: SQL execution, source/stream/sink/MV listing,
//! connector registry, and the pipeline lineage graph.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};

use laminar_db::{ConnectorInfo, PipelineNodeType};

use super::error_response;
use super::json_encoding::batches_to_json_raw;
use super::state::AppState;

#[derive(Debug, Serialize)]
struct SourceResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    watermark_column: Option<String>,
}

#[derive(Debug, Serialize)]
struct StreamResponse {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sql: Option<String>,
}

#[derive(Debug, Serialize)]
struct SinkResponse {
    name: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct SqlRequest {
    sql: String,
}

#[derive(Debug, Serialize)]
struct SqlResponse {
    result_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    object_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows_affected: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Box<serde_json::value::RawValue>>,
    /// `true` when the result was capped at `MAX_SQL_RESULT_ROWS` or the
    /// collection timed out, so `data` is a prefix of the full result. Omitted
    /// when the result is complete.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    truncated: bool,
}

/// Trim `batches` to at most `cap` rows, returning the trimmed batches and
/// whether any rows were dropped (i.e. the input held more than `cap`).
pub(super) fn cap_result(
    batches: Vec<arrow_array::RecordBatch>,
    cap: usize,
) -> (Vec<arrow_array::RecordBatch>, bool) {
    let total: usize = batches.iter().map(arrow_array::RecordBatch::num_rows).sum();
    if total <= cap {
        return (batches, false);
    }
    let mut kept = 0;
    let mut out = Vec::with_capacity(batches.len());
    for b in batches {
        let room = cap - kept;
        if b.num_rows() >= room {
            out.push(b.slice(0, room));
            break;
        }
        kept += b.num_rows();
        out.push(b);
    }
    (out, true)
}

/// Hard cap on rows materialized into an HTTP `SELECT` response. Streaming
/// queries are unbounded; without a cap a single request could consume
/// arbitrary memory. UIs should paginate via SQL or use the WS subscription
/// for live tailing.
const MAX_SQL_RESULT_ROWS: usize = 1000;

/// Wall-clock budget for collecting rows from a streaming `Query` result.
/// Sparse/empty streams would otherwise block the HTTP request indefinitely;
/// we return whatever has arrived by the deadline.
const SQL_RESULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub(super) async fn execute_sql(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SqlRequest>,
) -> impl IntoResponse {
    match state.db.execute(&req.sql).await {
        Ok(result) => {
            use laminar_db::ExecuteResult;
            let resp = match result {
                ExecuteResult::Ddl(info) => SqlResponse {
                    result_type: info.statement_type,
                    object_name: Some(info.object_name),
                    rows_affected: None,
                    data: None,
                    truncated: false,
                },
                ExecuteResult::RowsAffected(n) => SqlResponse {
                    result_type: "rows_affected".to_string(),
                    object_name: None,
                    rows_affected: Some(n),
                    data: None,
                    truncated: false,
                },
                ExecuteResult::Metadata(batch) => {
                    let data = match batches_to_json_raw(&[batch]) {
                        Ok(json) => json,
                        Err(e) => {
                            return error_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("failed to serialize result: {e}"),
                            )
                            .into_response();
                        }
                    };
                    SqlResponse {
                        result_type: "metadata".to_string(),
                        object_name: None,
                        rows_affected: None,
                        data: Some(data),
                        truncated: false,
                    }
                }
                ExecuteResult::Query(mut handle) => {
                    let mut batches: Vec<arrow_array::RecordBatch> = Vec::new();
                    let mut total_rows = 0;
                    let mut timed_out = false;
                    if let Ok(mut sub) = handle.subscribe_raw() {
                        // Gather one batch *past* the cap so `cap_result` can tell
                        // "exactly at the cap" (complete) from "more rows exist".
                        let collect = async {
                            while let Ok(batch) = sub.recv_async().await {
                                total_rows += batch.num_rows();
                                batches.push(batch);
                                if total_rows > MAX_SQL_RESULT_ROWS {
                                    break;
                                }
                            }
                        };
                        timed_out = tokio::time::timeout(SQL_RESULT_TIMEOUT, collect)
                            .await
                            .is_err();
                    }
                    // Trim to the cap; `over_cap` true means rows were dropped.
                    // A timeout also leaves `data` a prefix of the full result.
                    let (batches, over_cap) = cap_result(batches, MAX_SQL_RESULT_ROWS);
                    let raw_data = if batches.is_empty() {
                        serde_json::value::RawValue::from_string("[]".to_string()).ok()
                    } else {
                        batches_to_json_raw(&batches).ok()
                    };
                    SqlResponse {
                        result_type: "query".to_string(),
                        object_name: None,
                        rows_affected: None,
                        data: raw_data,
                        truncated: over_cap || timed_out,
                    }
                }
            };
            Json(resp).into_response()
        }
        Err(e) => error_response(StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

pub(super) async fn list_sources(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sources: Vec<SourceResponse> = state
        .db
        .sources()
        .into_iter()
        .map(|s| SourceResponse {
            name: s.name,
            watermark_column: s.watermark_column,
        })
        .collect();
    Json(sources)
}

pub(super) async fn list_sinks(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let sinks: Vec<SinkResponse> = state
        .db
        .sinks()
        .into_iter()
        .map(|s| SinkResponse { name: s.name })
        .collect();
    Json(sinks)
}

pub(super) async fn list_streams(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let streams: Vec<StreamResponse> = state
        .db
        .streams()
        .into_iter()
        .map(|s| StreamResponse {
            name: s.name,
            sql: s.sql,
        })
        .collect();
    Json(streams)
}

pub(super) async fn get_stream(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let streams = state.db.streams();
    match streams.into_iter().find(|s| s.name == name) {
        Some(s) => Json(StreamResponse {
            name: s.name,
            sql: s.sql,
        })
        .into_response(),
        None => error_response(StatusCode::NOT_FOUND, format!("stream '{name}' not found"))
            .into_response(),
    }
}

pub(super) async fn list_mvs(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    // `MaterializedViewInfo` already serializes to `{name, sql, state}`.
    Json(state.db.materialized_views())
}

/// Connector catalog: the registered source and sink connector types and the
/// configuration keys each accepts. Drives the console's source-creation wizard.
#[derive(Debug, Serialize)]
struct ConnectorsResponse {
    sources: Vec<ConnectorInfo>,
    sinks: Vec<ConnectorInfo>,
}

pub(super) async fn list_connectors(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let registry = state.db.connector_registry();
    let sources: Vec<ConnectorInfo> = registry
        .list_sources()
        .iter()
        .filter_map(|name| registry.source_info(name))
        .collect();
    let sinks: Vec<ConnectorInfo> = registry
        .list_sinks()
        .iter()
        .filter_map(|name| registry.sink_info(name))
        .collect();
    Json(ConnectorsResponse { sources, sinks })
}

// ---------------------------------------------------------------------------
// Pipeline lineage graph
// ---------------------------------------------------------------------------

/// A node in the lineage graph returned by `GET /api/v1/graph`.
#[derive(Debug, Serialize)]
struct NodeResponse {
    name: String,
    /// `"Source"`, `"Stream"`, or `"Sink"`.
    node_type: String,
    sql: Option<String>,
}

/// A directed edge in the lineage graph returned by `GET /api/v1/graph`.
#[derive(Debug, Serialize)]
struct EdgeResponse {
    from: String,
    to: String,
}

#[derive(Debug, Serialize)]
struct GraphResponse {
    nodes: Vec<NodeResponse>,
    edges: Vec<EdgeResponse>,
}

/// Returns the pipeline lineage graph (sources → streams → sinks) as
/// `{ "nodes": [...], "edges": [...] }` for the console's lineage view.
pub(super) async fn get_graph(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let topology = state.db.pipeline_topology();
    let nodes = topology
        .nodes
        .into_iter()
        .map(|n| NodeResponse {
            name: n.name,
            node_type: match n.node_type {
                PipelineNodeType::Source => "Source",
                PipelineNodeType::Stream => "Stream",
                PipelineNodeType::Sink => "Sink",
            }
            .to_string(),
            sql: n.sql,
        })
        .collect();
    let edges = topology
        .edges
        .into_iter()
        .map(|e| EdgeResponse {
            from: e.from,
            to: e.to,
        })
        .collect();
    Json(GraphResponse { nodes, edges }).into_response()
}
