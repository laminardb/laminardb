//! Local authority-evidence endpoints for cluster diagnosis.
//!
//! Both routes read only process-local, bounded state retained by this exact process;
//! neither rereads the durable assignment snapshot nor treats shared publication as
//! local convergence. Responses are size-bounded, `no-store`, and re-checked against
//! the serving gate after capture to close the capture/response race.

use std::sync::Arc;

use axum::extract::State;
#[cfg(feature = "cluster")]
use axum::extract::{Extension, Query, RawQuery};
use axum::http::StatusCode;
use axum::response::IntoResponse;
#[cfg(feature = "cluster")]
use axum::response::Response;
#[cfg(feature = "cluster")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "cluster")]
use tracing::warn;

#[cfg(feature = "cluster")]
use super::auth::DiagnosticPrincipal;
use super::cluster_admin::CLUSTER_DISABLED_MSG;
use super::error_response;
use super::state::AppState;

#[cfg(feature = "cluster")]
pub(super) const LOCAL_EVIDENCE_SCHEMA_VERSION: &str = "laminardb-local-authority-evidence/v1";
#[cfg(feature = "cluster")]
pub(super) const MAX_LOCAL_EVIDENCE_RESPONSE_BYTES: usize = 4_096;
#[cfg(feature = "cluster")]
pub(super) const LOCAL_EVIDENCE_TOKEN_REQUIRED_MSG: &str =
    "local process authority evidence requires configured HTTP authentication";
#[cfg(feature = "cluster")]
const LOCAL_EVIDENCE_QUERY_MSG: &str = "local process authority evidence does not accept a query";
#[cfg(feature = "cluster")]
pub(super) const LOCAL_EVIDENCE_UNAVAILABLE_MSG: &str =
    "local process authority evidence is unavailable";
#[cfg(feature = "cluster")]
pub(super) const LOCAL_EVIDENCE_INVALID_MSG: &str = "local process authority evidence is invalid";
#[cfg(feature = "cluster")]
pub(super) const LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA_VERSION: &str =
    "laminardb-local-checkpoint-barrier-timings/v1";
#[cfg(feature = "cluster")]
pub(super) const MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES: usize = 64 * 1_024;
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_TOKEN_REQUIRED_MSG: &str =
    "local checkpoint barrier timings require configured HTTP authentication";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_UNAVAILABLE_MSG: &str =
    "local checkpoint barrier timings are unavailable";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG: &str =
    "local checkpoint barrier timings are invalid";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG: &str =
    "invalid local checkpoint barrier timing query";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_CONFLICT_MSG: &str =
    "local checkpoint barrier timing cursor conflicts with this process";
#[cfg(feature = "cluster")]
const LOCAL_CHECKPOINT_BARRIER_TIMINGS_OVERWRITTEN_MSG: &str =
    "local checkpoint barrier timing cursor has been overwritten";

#[cfg(feature = "cluster")]
#[derive(Serialize)]
struct LocalEvidenceResponse {
    schema_version: &'static str,
    evidence: laminar_core::cluster::control::LocalProcessAuthorityEvidence,
}

#[cfg(feature = "cluster")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LocalCheckpointBarrierTimingsQuery {
    after_sequence: u64,
    expected_node_id: Option<u64>,
    expected_boot_incarnation: Option<String>,
    expected_process_term: Option<u64>,
}

#[cfg(feature = "cluster")]
#[derive(Serialize)]
pub(super) struct LocalCheckpointBarrierTimingsResponse<'a> {
    schema_version: &'static str,
    process_identity: laminar_core::cluster::control::LocalProcessAuthorityIdentity,
    after_sequence: u64,
    page: LocalCheckpointBarrierTimingsPage<'a>,
}

#[cfg(feature = "cluster")]
#[derive(Serialize)]
struct LocalCheckpointBarrierTimingsPage<'a> {
    capacity: usize,
    oldest_retained_sequence: Option<u64>,
    next_sequence: u64,
    overwritten_record_count: u64,
    recording_loss_count: u64,
    metadata_exhausted: bool,
    has_more: bool,
    records: &'a [laminar_db::checkpoint_timing::CheckpointBarrierTimingRecord],
}

#[cfg(feature = "cluster")]
impl<'a> LocalCheckpointBarrierTimingsResponse<'a> {
    pub(super) fn new(
        after_sequence: u64,
        timing_page: &'a laminar_db::checkpoint_timing::CheckpointBarrierTimingPage,
    ) -> Self {
        let snapshot = &timing_page.snapshot;
        Self {
            schema_version: LOCAL_CHECKPOINT_BARRIER_TIMINGS_SCHEMA_VERSION,
            process_identity: timing_page.process,
            after_sequence,
            page: LocalCheckpointBarrierTimingsPage {
                capacity: snapshot.capacity,
                oldest_retained_sequence: snapshot.oldest_retained_sequence,
                next_sequence: snapshot.next_sequence,
                overwritten_record_count: snapshot.overwritten_record_count,
                recording_loss_count: snapshot.recording_loss_count,
                metadata_exhausted: snapshot.metadata_exhausted,
                has_more: snapshot.has_more,
                records: &snapshot.records,
            },
        }
    }
}

#[cfg(feature = "cluster")]
pub(super) fn local_checkpoint_barrier_timing_error_response(
    error: &laminar_db::checkpoint_timing::CheckpointBarrierTimingReadError,
) -> (StatusCode, &'static str) {
    use laminar_db::checkpoint_timing::{
        CheckpointBarrierTimingReadError, CheckpointBarrierTimingSnapshotError,
    };

    match error {
        CheckpointBarrierTimingReadError::ProcessIdentityMismatch { .. }
        | CheckpointBarrierTimingReadError::Snapshot(
            CheckpointBarrierTimingSnapshotError::CursorAhead { .. },
        ) => (
            StatusCode::CONFLICT,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_CONFLICT_MSG,
        ),
        CheckpointBarrierTimingReadError::Snapshot(
            CheckpointBarrierTimingSnapshotError::CursorOverwritten { .. },
        ) => (
            StatusCode::GONE,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_OVERWRITTEN_MSG,
        ),
        CheckpointBarrierTimingReadError::ProcessIdentityUnavailable
        | CheckpointBarrierTimingReadError::ProcessIdentityChanged { .. }
        | CheckpointBarrierTimingReadError::Snapshot(CheckpointBarrierTimingSnapshotError::Busy) => {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_UNAVAILABLE_MSG,
            )
        }
        CheckpointBarrierTimingReadError::ProcessIdentityRequired
        | CheckpointBarrierTimingReadError::LedgerProcessMismatch { .. }
        | CheckpointBarrierTimingReadError::Snapshot(
            CheckpointBarrierTimingSnapshotError::InvalidLimit { .. },
        ) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
        ),
    }
}

/// `no-store` JSON response for pre-encoded diagnostic bodies.
#[cfg(feature = "cluster")]
fn no_store_json_response(encoded: Vec<u8>) -> Response {
    let mut response = (StatusCode::OK, encoded).into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

/// `GET /api/v1/cluster/local-evidence` — bounded evidence retained by this exact process.
///
/// This route intentionally remains behind the normal startup/recovery serving gate. It never
/// rereads the durable assignment snapshot or treats shared publication as local convergence.
#[cfg(feature = "cluster")]
pub(super) async fn cluster_local_evidence(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<DiagnosticPrincipal>>,
    RawQuery(raw_query): RawQuery,
) -> impl IntoResponse {
    let Some(cluster) = state.cluster.as_ref() else {
        return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
    };
    let Some(Extension(_principal)) = principal else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            LOCAL_EVIDENCE_TOKEN_REQUIRED_MSG,
        )
        .into_response();
    };
    if raw_query.is_some() {
        return error_response(StatusCode::BAD_REQUEST, LOCAL_EVIDENCE_QUERY_MSG).into_response();
    }

    let evidence = match cluster
        .controller
        .read_local_process_authority_evidence()
        .await
    {
        Ok(evidence) => evidence,
        Err(laminar_core::cluster::control::LocalProcessAuthorityEvidenceError::Unavailable(
            error,
        )) => {
            warn!(%error, "local process authority evidence is unavailable");
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                LOCAL_EVIDENCE_UNAVAILABLE_MSG,
            )
            .into_response();
        }
        Err(laminar_core::cluster::control::LocalProcessAuthorityEvidenceError::Invalid(error)) => {
            warn!(%error, "local process authority evidence is invalid");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_EVIDENCE_INVALID_MSG,
            )
            .into_response();
        }
    };

    let adoption = &evidence.adopted_assignment;
    if evidence.participant.node_id == 0
        || evidence.participant.boot_incarnation.is_nil()
        || evidence.process_term == 0
        || !adoption.is_canonical()
        || adoption.participant != evidence.participant
    {
        warn!("local process authority evidence violated its canonical response contract");
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            LOCAL_EVIDENCE_INVALID_MSG,
        )
        .into_response();
    }

    let envelope = LocalEvidenceResponse {
        schema_version: LOCAL_EVIDENCE_SCHEMA_VERSION,
        evidence,
    };
    let encoded = match serde_json::to_vec(&envelope) {
        Ok(encoded) => encoded,
        Err(error) => {
            warn!(%error, "failed to serialize local process authority evidence");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_EVIDENCE_INVALID_MSG,
            )
            .into_response();
        }
    };
    if encoded.len() > MAX_LOCAL_EVIDENCE_RESPONSE_BYTES {
        warn!(
            encoded_bytes = encoded.len(),
            maximum_bytes = MAX_LOCAL_EVIDENCE_RESPONSE_BYTES,
            "local process authority evidence exceeded its response bound"
        );
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            LOCAL_EVIDENCE_INVALID_MSG,
        )
        .into_response();
    }

    // Close a capture/response race with terminal fencing or a newly started recovery.
    if let Some(reason) = state.serving_rejection() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }

    no_store_json_response(encoded)
}

#[cfg(not(feature = "cluster"))]
pub(super) async fn cluster_local_evidence(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let _ = state;
    error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
}

/// Continuation-cursor process identity: `None` bootstraps at sequence zero;
/// otherwise all three expected fields must be present and canonical.
#[cfg(feature = "cluster")]
fn expected_process_from_query(
    query: &LocalCheckpointBarrierTimingsQuery,
) -> Result<
    Option<laminar_core::cluster::control::LocalProcessAuthorityIdentity>,
    (StatusCode, &'static str),
> {
    match (
        query.expected_node_id,
        query.expected_boot_incarnation.as_deref(),
        query.expected_process_term,
    ) {
        (None, None, None) if query.after_sequence == 0 => Ok(None),
        (Some(node_id), Some(boot_incarnation), Some(process_term)) => {
            let Ok(boot_incarnation) = uuid::Uuid::parse_str(boot_incarnation) else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
                ));
            };
            let process = laminar_core::cluster::control::LocalProcessAuthorityIdentity {
                participant: laminar_core::checkpoint::CheckpointParticipant {
                    node_id,
                    boot_incarnation,
                },
                process_term,
            };
            if !process.is_canonical() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
                ));
            }
            Ok(Some(process))
        }
        _ => Err((
            StatusCode::BAD_REQUEST,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
        )),
    }
}

/// `GET /api/v1/cluster/local-checkpoint-barrier-timings` — bounded local pause evidence.
///
/// This route reads only the process-local fixed-capacity ledger. Sequence zero bootstraps the
/// current identity; every continuation cursor is inseparable from that returned identity,
/// preventing an old process's cursor from skipping records after a restart. It does not read
/// checkpoint authority or imply durable settlement.
#[cfg(feature = "cluster")]
pub(super) async fn cluster_local_checkpoint_barrier_timings(
    State(state): State<Arc<AppState>>,
    principal: Option<Extension<DiagnosticPrincipal>>,
    query: Result<
        Query<LocalCheckpointBarrierTimingsQuery>,
        axum::extract::rejection::QueryRejection,
    >,
) -> impl IntoResponse {
    use laminar_db::checkpoint_timing::MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS;

    let Some(_cluster) = state.cluster.as_ref() else {
        return error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response();
    };
    let Some(Extension(_principal)) = principal else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_TOKEN_REQUIRED_MSG,
        )
        .into_response();
    };

    let Query(query) = match query {
        Ok(query) => query,
        Err(_) => {
            // Query rejections can echo attacker-controlled field names. Keep access logs
            // limited to the fixed route/method/status/latency contract.
            warn!("rejected malformed local checkpoint barrier timing query");
            return error_response(
                StatusCode::BAD_REQUEST,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_QUERY_MSG,
            )
            .into_response();
        }
    };
    let expected_process = match expected_process_from_query(&query) {
        Ok(expected) => expected,
        Err((status, message)) => return error_response(status, message).into_response(),
    };

    let timing_page = match state.db.checkpoint_barrier_timing_snapshot(
        expected_process,
        query.after_sequence,
        MAX_CHECKPOINT_BARRIER_TIMING_PAGE_RECORDS,
    ) {
        Ok(page) => page,
        Err(error) => {
            let (status, message) = local_checkpoint_barrier_timing_error_response(&error);
            if status == StatusCode::INTERNAL_SERVER_ERROR {
                warn!(%error, "local checkpoint barrier timing page violated its contract");
            }
            return error_response(status, message).into_response();
        }
    };

    if expected_process.is_some_and(|expected| timing_page.process != expected)
        || timing_page
            .snapshot
            .records
            .iter()
            .any(|record| record.process != timing_page.process)
    {
        warn!("local checkpoint barrier timing response mixed process identities");
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
        )
        .into_response();
    }
    let envelope = LocalCheckpointBarrierTimingsResponse::new(query.after_sequence, &timing_page);
    let encoded = match serde_json::to_vec(&envelope) {
        Ok(encoded) => encoded,
        Err(error) => {
            warn!(%error, "failed to serialize local checkpoint barrier timings");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
            )
            .into_response();
        }
    };
    if encoded.len() > MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES {
        warn!(
            encoded_bytes = encoded.len(),
            maximum_bytes = MAX_LOCAL_CHECKPOINT_BARRIER_TIMINGS_RESPONSE_BYTES,
            "local checkpoint barrier timing response exceeded its bound"
        );
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            LOCAL_CHECKPOINT_BARRIER_TIMINGS_INVALID_MSG,
        )
        .into_response();
    }

    if let Some(reason) = state.serving_rejection() {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, reason).into_response();
    }

    no_store_json_response(encoded)
}

#[cfg(not(feature = "cluster"))]
pub(super) async fn cluster_local_checkpoint_barrier_timings(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let _ = state;
    error_response(StatusCode::NOT_FOUND, CLUSTER_DISABLED_MSG).into_response()
}
