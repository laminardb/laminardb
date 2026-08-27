//! WebSocket stream subscriptions.
//!
//! One connection per `/ws/{name}` upgrade under a global slot budget. Data frames are
//! size-bounded incremental JSON with explicit sequence/offset metadata; the session
//! ends (with a best-effort close frame) on serving fence, client death by pong
//! deadline, oversized rows, or subscription errors.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use laminar_db::subscription::{
    ClusterSubscriptionFrameMetadata, PortalFrame, SubscriptionEnvelope,
};
use serde::Deserialize;
use tracing::warn;

use super::error_response;
use super::json_encoding::exact_json_encoder_options;
use super::state::AppState;

const MAX_WS_CONNECTIONS: usize = 10_000;
pub(super) const MAX_WS_FRAME_BYTES: usize = 1024 * 1024;
pub(super) const MAX_WS_CONTROL_FIELD_BYTES: usize = 4096;
const MAX_WS_SUBSCRIPTION_ID_BYTES: usize = 1024;
const MAX_WS_INBOUND_BYTES: usize = 4096;
const WS_HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(15);
const WS_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_UNANSWERED_WS_PINGS: u8 = 2;

pub(crate) fn ws_connection_slots() -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(MAX_WS_CONNECTIONS))
}

#[derive(Default)]
pub(super) struct WsPongDeadline {
    unanswered: u8,
}

impl WsPongDeadline {
    pub(super) fn before_ping(&mut self) -> bool {
        if self.unanswered >= MAX_UNANSWERED_WS_PINGS {
            return false;
        }
        self.unanswered += 1;
        true
    }

    pub(super) fn on_pong(&mut self) {
        self.unanswered = 0;
    }
}

pub(super) fn try_acquire_ws_slot(
    slots: &Arc<tokio::sync::Semaphore>,
) -> Option<tokio::sync::OwnedSemaphorePermit> {
    Arc::clone(slots).try_acquire_owned().ok()
}

pub(super) fn ws_error_json(name: &str, code: &str, message: &str, sequence: u64) -> String {
    let out = serde_json::json!({
        "type": "error",
        "subscription_id": name,
        "code": truncate_utf8(code, MAX_WS_CONTROL_FIELD_BYTES),
        "message": truncate_utf8(message, MAX_WS_CONTROL_FIELD_BYTES),
        "sequence": sequence.to_string(),
    })
    .to_string();
    debug_assert!(out.len() <= MAX_WS_FRAME_BYTES);
    out
}

pub(super) fn ws_gap_json(name: &str, skipped: u64, sequence: u64) -> String {
    let out = serde_json::json!({
        "type": "gap",
        "subscription_id": name,
        "code": "subscription_lagged",
        "message": format!("subscription lagged: skipped {skipped} messages"),
        "skipped_messages": skipped.to_string(),
        "sequence": sequence.to_string(),
    })
    .to_string();
    debug_assert!(out.len() <= MAX_WS_FRAME_BYTES);
    out
}

#[cfg(test)]
pub(super) fn ws_progress_json(
    name: &str,
    epoch: u64,
    checkpoint_id: u64,
    log_sequence: u64,
    through_sequence: u64,
    sequence: u64,
) -> String {
    ws_progress_json_with_metadata(
        name,
        epoch,
        checkpoint_id,
        log_sequence,
        through_sequence,
        sequence,
        None,
    )
}

#[allow(clippy::too_many_arguments)] // the compatibility progress envelope has six stable fields
pub(super) fn ws_progress_json_with_metadata(
    name: &str,
    epoch: u64,
    checkpoint_id: u64,
    log_sequence: u64,
    through_sequence: u64,
    sequence: u64,
    cluster: Option<&ClusterSubscriptionFrameMetadata>,
) -> String {
    let mut out = serde_json::json!({
        "type": "progress",
        "subscription_id": name,
        "epoch": epoch.to_string(),
        "checkpoint_id": checkpoint_id.to_string(),
        "log_sequence": log_sequence.to_string(),
        "through_log_sequence": through_sequence.to_string(),
        "sequence": sequence.to_string(),
    });
    if let Some(ClusterSubscriptionFrameMetadata::Progress {
        stream_generation, ..
    }) = cluster
    {
        out["stream_generation"] = serde_json::Value::String(stream_generation.to_string());
    }
    let out = out.to_string();
    debug_assert!(out.len() <= MAX_WS_FRAME_BYTES);
    out
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum WsFrameBuildError {
    TooLarge,
    Serialization(String),
}

#[derive(Default)]
pub(super) struct WsBatchFrameState {
    /// First row not yet included in a completed frame.
    pub(super) offset: usize,
    /// A row encoded while filling the previous frame that did not fit.
    pub(super) pending_row: Option<Vec<u8>>,
}

const WS_DATA_SUFFIX_FIXED_BYTES: usize =
    r#"],"sequence":"","log_sequence":"","row_offset":"","row_count":""}"#.len();

fn decimal_digits_u64(value: u64) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

fn decimal_digits_usize(value: usize) -> usize {
    if value == 0 {
        1
    } else {
        value.ilog10() as usize + 1
    }
}

fn ws_data_suffix_len(
    sequence: u64,
    log_sequence: u64,
    offset: usize,
    rows: usize,
    cluster_fields: &str,
) -> usize {
    WS_DATA_SUFFIX_FIXED_BYTES
        + decimal_digits_u64(sequence)
        + decimal_digits_u64(log_sequence)
        + decimal_digits_usize(offset)
        + decimal_digits_usize(rows)
        + cluster_fields.len()
}

fn ws_data_suffix(
    sequence: u64,
    log_sequence: u64,
    offset: usize,
    rows: usize,
    cluster_fields: &str,
) -> String {
    format!(
        "],\"sequence\":\"{sequence}\",\"log_sequence\":\"{log_sequence}\",\"row_offset\":\"{offset}\",\"row_count\":\"{rows}\"{cluster_fields}}}"
    )
}

fn ws_cluster_data_fields(cluster: Option<&ClusterSubscriptionFrameMetadata>) -> String {
    let Some(ClusterSubscriptionFrameMetadata::Data {
        stream_generation,
        partition,
        partition_sequence,
        committed_epoch,
    }) = cluster
    else {
        return String::new();
    };
    format!(
        ",\"stream_generation\":\"{stream_generation}\",\"partition\":\"{}\",\"partition_sequence\":\"{}\",\"committed_epoch\":\"{committed_epoch}\"",
        partition.get(),
        partition_sequence.get(),
    )
}

#[cfg(test)]
pub(super) fn next_ws_data_frame(
    name: &str,
    batch: &arrow_array::RecordBatch,
    state: &mut WsBatchFrameState,
    sequence: u64,
    log_sequence: u64,
) -> Result<Option<String>, WsFrameBuildError> {
    next_ws_data_frame_with_metadata(name, batch, state, sequence, log_sequence, None)
}

pub(super) fn next_ws_data_frame_with_metadata(
    name: &str,
    batch: &arrow_array::RecordBatch,
    state: &mut WsBatchFrameState,
    sequence: u64,
    log_sequence: u64,
    cluster: Option<&ClusterSubscriptionFrameMetadata>,
) -> Result<Option<String>, WsFrameBuildError> {
    if state.offset >= batch.num_rows() {
        debug_assert!(state.pending_row.is_none());
        return Ok(None);
    }

    let subid = serde_json::to_string(name)
        .map_err(|error| WsFrameBuildError::Serialization(error.to_string()))?;
    let prefix = format!("{{\"type\":\"data\",\"subscription_id\":{subid},\"data\":[");
    let cluster_fields = ws_cluster_data_fields(cluster);
    let frame_offset = state.offset;
    if prefix.len().saturating_add(ws_data_suffix_len(
        sequence,
        log_sequence,
        frame_offset,
        1,
        &cluster_fields,
    )) >= MAX_WS_FRAME_BYTES
    {
        return Err(WsFrameBuildError::TooLarge);
    }

    // Build one root encoder per output frame. It is deliberately local so no
    // non-Send Arrow encoder is retained across the socket write await.
    let root = arrow_array::StructArray::from(batch.clone());
    let root_field = Arc::new(arrow_schema::Field::new_struct(
        "",
        batch.schema().fields().clone(),
        false,
    ));
    let options = exact_json_encoder_options();
    let mut encoder = arrow_json::writer::make_encoder(&root_field, &root, &options)
        .map_err(|error| WsFrameBuildError::Serialization(error.to_string()))?;

    let mut bytes = Vec::with_capacity(MAX_WS_FRAME_BYTES.min(prefix.len() + 16 * 1024));
    bytes.extend_from_slice(prefix.as_bytes());
    let mut rows = 0_usize;

    while state.offset < batch.num_rows() {
        let row = state.pending_row.take().unwrap_or_else(|| {
            let mut row = Vec::new();
            encoder.encode(state.offset, &mut row);
            row
        });
        let separator_bytes = usize::from(rows != 0);
        let candidate_rows = rows + 1;
        let candidate_len = bytes
            .len()
            .saturating_add(separator_bytes)
            .saturating_add(row.len())
            .saturating_add(ws_data_suffix_len(
                sequence,
                log_sequence,
                frame_offset,
                candidate_rows,
                &cluster_fields,
            ));

        if candidate_len > MAX_WS_FRAME_BYTES {
            if rows == 0 {
                return Err(WsFrameBuildError::TooLarge);
            }
            state.pending_row = Some(row);
            break;
        }

        if separator_bytes != 0 {
            bytes.push(b',');
        }
        bytes.extend_from_slice(&row);
        state.offset += 1;
        rows = candidate_rows;
    }

    debug_assert!(rows > 0);
    bytes.extend_from_slice(
        ws_data_suffix(sequence, log_sequence, frame_offset, rows, &cluster_fields).as_bytes(),
    );
    debug_assert!(bytes.len() <= MAX_WS_FRAME_BYTES);
    let frame = String::from_utf8(bytes)
        .map_err(|error| WsFrameBuildError::Serialization(error.to_string()))?;
    Ok(Some(frame))
}

async fn ws_send(socket: &mut WebSocket, message: Message, state: &AppState) -> bool {
    if state.serving_rejection().is_some() {
        return false;
    }
    tokio::select! {
        biased;
        () = state.wait_for_serving_fence() => false,
        result = tokio::time::timeout(WS_WRITE_TIMEOUT, socket.send(message)) => {
            matches!(result, Ok(Ok(())))
        }
    }
}

/// Forward one portal batch as bounded data frames, advancing `seq`.
/// Returns `false` when the subscription session must end.
async fn forward_portal_batch(
    socket: &mut WebSocket,
    seq: &mut u64,
    name: &str,
    batch: &arrow_array::RecordBatch,
    log_sequence: u64,
    cluster: Option<&ClusterSubscriptionFrameMetadata>,
    state: &AppState,
) -> bool {
    let mut frame_state = WsBatchFrameState::default();
    while frame_state.offset < batch.num_rows() {
        let out = match next_ws_data_frame_with_metadata(
            name,
            batch,
            &mut frame_state,
            *seq,
            log_sequence,
            cluster,
        ) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(WsFrameBuildError::TooLarge) => {
                let out = ws_error_json(
                    name,
                    "row_too_large",
                    "one subscription row exceeds the WebSocket frame limit",
                    *seq,
                );
                let _ = ws_send(socket, Message::Text(out.into()), state).await;
                return false;
            }
            Err(WsFrameBuildError::Serialization(error)) => {
                warn!(stream = %name, error = %error, "serialize error");
                let message = format!("subscription batch serialization failed: {error}");
                let out = ws_error_json(name, "serialization_failed", &message, *seq);
                let _ = ws_send(socket, Message::Text(out.into()), state).await;
                return false;
            }
        };
        if !ws_send(socket, Message::Text(out.into()), state).await {
            return false;
        }
        let Some(next) = seq.checked_add(1) else {
            return false;
        };
        *seq = next;
    }
    true
}

/// Handle one inbound socket message. Returns `false` when the session must end.
async fn handle_inbound_message(
    socket: &mut WebSocket,
    pong_deadline: &mut WsPongDeadline,
    message: Option<Result<Message, axum::Error>>,
    name: &str,
    seq: u64,
    state: &AppState,
) -> bool {
    // `data` moves into `Pong`, so the inner `if` can't fold into the guard.
    #[allow(clippy::collapsible_match)]
    match message {
        Some(Ok(Message::Close(_))) | None => false,
        Some(Ok(Message::Ping(data))) => ws_send(socket, Message::Pong(data), state).await,
        Some(Ok(Message::Pong(_))) => {
            pong_deadline.on_pong();
            true
        }
        Some(Ok(Message::Text(_) | Message::Binary(_))) => {
            let out = ws_error_json(
                name,
                "unsupported_client_message",
                "subscription WebSocket accepts control frames only",
                seq,
            );
            let _ = ws_send(socket, Message::Text(out.into()), state).await;
            false
        }
        Some(Err(error)) => {
            warn!(stream = %name, %error, "WebSocket receive failed");
            false
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct WsSubscribeParams {
    as_of_epoch: Option<u64>,
}

pub(super) async fn ws_upgrade(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(params): Query<WsSubscribeParams>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if name.is_empty() || name.len() > MAX_WS_SUBSCRIPTION_ID_BYTES {
        return error_response(
            StatusCode::BAD_REQUEST,
            format!(
                "WebSocket subscription name must contain 1..={MAX_WS_SUBSCRIPTION_ID_BYTES} UTF-8 bytes"
            ),
        )
        .into_response();
    }
    let Some(slot) = try_acquire_ws_slot(&state.ws_slots) else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many WebSocket connections".to_string(),
        )
        .into_response();
    };

    let start = params.as_of_epoch.map_or(
        laminar_db::subscription::SubscribeStart::Tail,
        laminar_db::subscription::SubscribeStart::AsOfEpoch,
    );
    let portal = match state.db.open_subscription(&name, None, start).await {
        Ok(p) => p,
        Err(laminar_db::DbError::StreamNotFound(_)) => {
            return error_response(StatusCode::NOT_FOUND, format!("stream '{name}' not found"))
                .into_response();
        }
        Err(error @ laminar_db::DbError::SubscriptionReplayPruned { .. }) => {
            return error_response(StatusCode::GONE, error.to_string()).into_response();
        }
        Err(error @ laminar_db::DbError::SubscriptionEpochNotCommitted { .. }) => {
            return error_response(StatusCode::CONFLICT, error.to_string()).into_response();
        }
        Err(laminar_db::DbError::Subscription(error)) => {
            let status = match &error {
                laminar_db::subscription::ClusterSubscriptionError::UnsupportedPlan { .. } => {
                    StatusCode::BAD_REQUEST
                }
                laminar_db::subscription::ClusterSubscriptionError::GenerationMismatch
                | laminar_db::subscription::ClusterSubscriptionError::EpochNotCommitted {
                    ..
                } => StatusCode::CONFLICT,
                laminar_db::subscription::ClusterSubscriptionError::ReplayPruned { .. }
                | laminar_db::subscription::ClusterSubscriptionError::ResumeTokenExpired
                | laminar_db::subscription::ClusterSubscriptionError::RetentionLost => {
                    StatusCode::GONE
                }
                laminar_db::subscription::ClusterSubscriptionError::BackendUnavailable => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            return error_response(status, format!("[{}] {error}", error.code())).into_response();
        }
        Err(error) => {
            warn!(stream = %name, error = %error, "failed to open WebSocket subscription");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to subscribe to '{name}'"),
            )
            .into_response();
        }
    };
    if let Some(reason) = state.serving_rejection() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }

    let st = Arc::clone(&state);
    ws.max_message_size(MAX_WS_INBOUND_BYTES)
        .max_frame_size(MAX_WS_INBOUND_BYTES)
        .on_upgrade(move |socket| async move {
            let _slot = slot;
            st.server_metrics.ws_connections.inc();
            ws_client(socket, portal, name, Arc::clone(&st)).await;
            st.server_metrics.ws_connections.dec();
        })
        .into_response()
}

async fn ws_client(
    mut socket: WebSocket,
    mut portal: laminar_db::subscription::SubscriptionPortal,
    name: String,
    state: Arc<AppState>,
) {
    let mut heartbeat = tokio::time::interval(WS_HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pong_deadline = WsPongDeadline::default();
    let mut seq: u64 = 0;

    'subscription: loop {
        tokio::select! {
            biased;
            () = state.wait_for_serving_fence() => break,
            frame = portal.next_envelope() => {
                if forward_portal_envelope(
                    &mut socket,
                    &mut seq,
                    &name,
                    frame,
                    &state,
                ).await == WsFrameOutcome::Disconnect {
                    break 'subscription;
                }
            }
            _ = heartbeat.tick() => {
                if !pong_deadline.before_ping() {
                    break;
                }
                if !ws_send(
                    &mut socket,
                    Message::Ping(bytes::Bytes::new()),
                    &state,
                ).await {
                    break;
                }
            }
            msg = socket.recv() => {
                if !handle_inbound_message(
                    &mut socket,
                    &mut pong_deadline,
                    msg,
                    &name,
                    seq,
                    &state,
                ).await {
                    break;
                }
            }
        }
    }
    if state.serving_rejection().is_none() {
        let _ = ws_send(&mut socket, Message::Close(None), &state).await;
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum WsFrameOutcome {
    Continue,
    Disconnect,
}

async fn forward_portal_envelope(
    socket: &mut WebSocket,
    sequence: &mut u64,
    name: &str,
    envelope: Option<SubscriptionEnvelope>,
    state: &AppState,
) -> WsFrameOutcome {
    let Some(envelope) = envelope else {
        return WsFrameOutcome::Disconnect;
    };
    match envelope {
        SubscriptionEnvelope {
            frame:
                PortalFrame::Batch {
                    batch,
                    sequence: log_sequence,
                    lease: _lease,
                },
            cluster,
            ..
        } => {
            if batch.num_rows() == 0 {
                return WsFrameOutcome::Continue;
            }
            if forward_portal_batch(
                socket,
                sequence,
                name,
                &batch,
                log_sequence,
                cluster.as_ref(),
                state,
            )
            .await
            {
                WsFrameOutcome::Continue
            } else {
                WsFrameOutcome::Disconnect
            }
        }
        SubscriptionEnvelope {
            frame:
                PortalFrame::Barrier {
                    sequence: log_sequence,
                    epoch,
                    checkpoint_id,
                    through_sequence,
                },
            cluster,
            ..
        } => {
            let out = ws_progress_json_with_metadata(
                name,
                epoch,
                checkpoint_id,
                log_sequence,
                through_sequence,
                *sequence,
                cluster.as_ref(),
            );
            if !ws_send(socket, Message::Text(out.into()), state).await {
                return WsFrameOutcome::Disconnect;
            }
            let Some(next) = sequence.checked_add(1) else {
                return WsFrameOutcome::Disconnect;
            };
            *sequence = next;
            WsFrameOutcome::Continue
        }
        SubscriptionEnvelope {
            frame: PortalFrame::Lagged(skipped),
            ..
        } => {
            warn!(stream = %name, skipped, "WS client fell behind, disconnecting");
            let out = ws_gap_json(name, skipped, *sequence);
            let _ = ws_send(socket, Message::Text(out.into()), state).await;
            WsFrameOutcome::Disconnect
        }
        SubscriptionEnvelope {
            frame: PortalFrame::Error { message },
            error_code,
            ..
        } => {
            warn!(stream = %name, error = %message, "WS subscription failed, disconnecting");
            let out = ws_error_json(
                name,
                error_code.unwrap_or("subscription_failed"),
                &message,
                *sequence,
            );
            let _ = ws_send(socket, Message::Text(out.into()), state).await;
            WsFrameOutcome::Disconnect
        }
    }
}
