//! Operator-state capture at the aligned cut and its deferred serialization.

use std::sync::Arc;
use std::time::Duration;

use laminar_core::checkpoint::CheckpointAttempt;

use super::{fail_after_mutable_capture, ConnectorPipelineCallback, MutableCheckpointCaptureGuard};
use crate::error::DbError;

pub(super) struct OperatorStateCapture {
    graph: crate::operator_graph::GraphStateCapture,
    materialized_views: crate::mv_store::MvCheckpointCapture,
    reference_tables: Option<crate::table_store::ReferenceTableCheckpointCapture>,
    serialization_permit: tokio::sync::OwnedSemaphorePermit,
}

struct EncodedOperatorState {
    frames: Vec<crate::checkpoint_coordinator::CapturedStateFrame>,
    managed_vnode_operators: Vec<crate::checkpoint_coordinator::ManagedVnodeOperator>,
    view_logs: Vec<crate::checkpoint_coordinator::CapturedViewLog>,
}

fn graph_capture_needs_mutable_guard(graph: &crate::operator_graph::GraphStateCapture) -> bool {
    !graph.whole.is_empty()
        || graph
            .vnodes
            .iter()
            .any(|(_, captured)| captured.state.is_some())
}

/// The managed vnode operators of a graph capture, by their checkpoint frame identity.
fn managed_vnode_operators(
    operators: Vec<(String, crate::operator::capability::OperatorStateClass)>,
) -> Result<Vec<crate::checkpoint_coordinator::ManagedVnodeOperator>, DbError> {
    use crate::checkpoint_coordinator::ManagedVnodePlacement;
    use crate::operator::capability::OperatorStateClass;

    operators
        .into_iter()
        .map(|(operator_id, placement)| {
            let placement = match placement {
                OperatorStateClass::GlobalSingleton => ManagedVnodePlacement::GlobalSingleton,
                OperatorStateClass::VnodeKeyed => ManagedVnodePlacement::VnodeKeyed,
                unsupported => {
                    return Err(DbError::Checkpoint(format!(
                        "managed operator '{operator_id}' has unsupported placement {unsupported:?}"
                    )));
                }
            };
            Ok(crate::checkpoint_coordinator::ManagedVnodeOperator {
                operator_id: format!("graph:{operator_id}"),
                placement,
            })
        })
        .collect()
}

impl OperatorStateCapture {
    fn encode(
        self,
        max_staged_bytes: u64,
        mut staged_bytes: u64,
    ) -> Result<EncodedOperatorState, DbError> {
        let Self {
            graph,
            materialized_views,
            reference_tables,
            serialization_permit,
        } = self;
        let managed_vnode_operators = managed_vnode_operators(graph.managed_vnode_operators)?;
        let mut frames = Vec::with_capacity(graph.whole.len() + graph.vnodes.len() + 1);
        for state in graph.whole {
            let encoded = state
                .state
                .materialize(&mut staged_bytes, max_staged_bytes)?;
            frames.push(crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::OperatorWhole {
                    operator_id: format!("graph:{}", state.operator_id),
                },
                state: Some(encoded),
            });
        }
        for (operator_id, captured) in graph.vnodes {
            let vnode = u16::try_from(captured.vnode).map_err(|_| {
                DbError::Checkpoint(format!(
                    "operator '{operator_id}' captured vnode {} outside the checkpoint ABI",
                    captured.vnode
                ))
            })?;
            let encoded = captured
                .state
                .map(|state| state.materialize(&mut staged_bytes, max_staged_bytes))
                .transpose()?;
            frames.push(crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::Vnode {
                    operator_id: format!("graph:{operator_id}"),
                    vnode,
                },
                state: encoded,
            });
        }

        let mv_capture_bytes = materialized_views.estimated_bytes();
        let mv_headroom = max_staged_bytes.checked_sub(staged_bytes).ok_or_else(|| {
            DbError::Checkpoint("view-log capture exceeded the staged-state budget".into())
        })?;
        let (view_logs, mv_retained_bytes) = materialized_views.encode(mv_headroom)?;
        staged_bytes = staged_bytes
            .checked_sub(mv_capture_bytes)
            .and_then(|bytes| bytes.checked_add(mv_retained_bytes))
            .filter(|bytes| *bytes <= max_staged_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "view-log ownership transfer exceeded its staged-state budget".into(),
                )
            })?;

        let reference_tables = reference_tables
            .map(|capture| {
                let capture_bytes = capture.estimated_bytes();
                let headroom = max_staged_bytes.checked_sub(staged_bytes).ok_or_else(|| {
                    DbError::Checkpoint(
                        "reference-table capture exceeded the staged-state budget".into(),
                    )
                })?;
                let (encoded, encoded_retained_bytes) = capture.encode(headroom)?;
                staged_bytes = staged_bytes
                    .checked_sub(capture_bytes)
                    .and_then(|bytes| bytes.checked_add(encoded_retained_bytes))
                    .filter(|bytes| *bytes <= max_staged_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "reference-table state ownership transfer exceeded its staged-state budget"
                                .into(),
                        )
                    })?;
                Ok::<bytes::Bytes, DbError>(encoded)
            })
            .transpose()?;
        if let Some(reference_tables) = reference_tables {
            frames.push(crate::checkpoint_coordinator::CapturedStateFrame {
                key: laminar_core::checkpoint::StateFrameKey::OperatorWhole {
                    operator_id: crate::table_store::REFERENCE_TABLE_CHECKPOINT_KEY.to_string(),
                },
                state: Some(reference_tables),
            });
        }

        frames.sort_unstable_by(|left, right| left.key.cmp(&right.key));
        if frames.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(DbError::Checkpoint(
                "checkpoint capture produced a duplicate logical state frame".into(),
            ));
        }

        // The permit is deliberately owned by the non-abortable worker. If its async waiter times
        // out, another checkpoint cannot capture a second image until this worker actually exits.
        drop(serialization_permit);
        Ok(EncodedOperatorState {
            frames,
            managed_vnode_operators,
            view_logs,
        })
    }
}

/// Immutable operator image captured at the aligned cut. Encoding is deliberately deferred to
/// the spawned durable tail so the callback can resume while Arrow IPC and rkyv run off-thread.
pub(super) struct CapturedOperatorState {
    image: OperatorStateCapture,
    estimated_bytes: u64,
    mutable_capture_guard: Option<MutableCheckpointCaptureGuard>,
}

pub(super) struct SerializedOperatorState {
    frames: Vec<crate::checkpoint_coordinator::CapturedStateFrame>,
    managed_vnode_operators: Vec<crate::checkpoint_coordinator::ManagedVnodeOperator>,
    view_logs: Vec<crate::checkpoint_coordinator::CapturedViewLog>,
    mutable_capture_guard: Option<MutableCheckpointCaptureGuard>,
}

impl SerializedOperatorState {
    /// Move the serialized state into `request`, returning the guard the durable tail owns.
    pub(super) fn install(
        self,
        request: &mut crate::checkpoint_coordinator::CheckpointRequest,
    ) -> Option<MutableCheckpointCaptureGuard> {
        request.state_frames = self.frames;
        request.managed_vnode_operators = self.managed_vnode_operators;
        request.view_logs = self.view_logs;
        self.mutable_capture_guard
    }

    #[cfg(test)]
    pub(super) fn accept_for_test(
        mut self,
    ) -> Vec<crate::checkpoint_coordinator::CapturedStateFrame> {
        if let Some(guard) = self.mutable_capture_guard.as_mut() {
            guard.disarm();
        }
        self.frames
    }
}

impl CapturedOperatorState {
    pub(super) async fn serialize_until(
        self,
        max_staged_bytes: u64,
        serialization_timeout: Duration,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<SerializedOperatorState, String> {
        let Self {
            image,
            estimated_bytes,
            mut mutable_capture_guard,
        } = self;
        let serialization_deadline =
            attempt_deadline.min(tokio::time::Instant::now() + serialization_timeout);
        let worker =
            tokio::task::spawn_blocking(move || image.encode(max_staged_bytes, estimated_bytes));
        let encoded = match tokio::time::timeout_at(serialization_deadline, worker).await {
            Err(_) => {
                let error = format!(
                    "[LDB-6017] checkpoint state serialization timed out ({serialization_timeout:?})"
                );
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
            Ok(Err(error)) => {
                let error = format!("checkpoint state serialization join failed: {error}");
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
            Ok(Ok(Err(error))) => {
                let error = format!("checkpoint state serialization failed: {error}");
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
            Ok(Ok(Ok(encoded))) => encoded,
        };
        Ok(SerializedOperatorState {
            frames: encoded.frames,
            managed_vnode_operators: encoded.managed_vnode_operators,
            view_logs: encoded.view_logs,
            mutable_capture_guard,
        })
    }
}

impl ConnectorPipelineCallback {
    pub(super) fn capture_operator_state_until(
        &mut self,
        attempt: CheckpointAttempt,
        attempt_deadline: tokio::time::Instant,
    ) -> Result<CapturedOperatorState, String> {
        if tokio::time::Instant::now() >= attempt_deadline {
            return Err(
                "[LDB-6017] checkpoint deadline expired before operator-state capture".into(),
            );
        }
        // Healthy admission permits only one checkpoint tail. Contention therefore means a
        // non-abortable encoder from a failed attempt still owns the image, or that the admission
        // invariant was breached. Waiting while the assignment read fence is held can exhaust a
        // rebalance writer's shorter deadline, so reject before touching mutable operator state.
        let serialization_permit = Arc::clone(&self.checkpoint_serialization_gate)
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::NoPermits => {
                    "[LDB-6017] prior checkpoint serialization is still active; refusing an \
                     overlapping mutable capture"
                        .to_string()
                }
                tokio::sync::TryAcquireError::Closed => {
                    "checkpoint serialization gate was closed".to_string()
                }
            })?;

        let capture_timer = self.prom.checkpoint_state_capture_duration.start_timer();
        if self
            .full_vnode_capture_needed
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.graph.force_full_vnode_capture();
        }
        let graph = match self.graph.capture_state(self.checkpoint_state_cap_bytes) {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                let reason = format!("snapshot failed: {error}");
                if error.requires_pipeline_halt() {
                    if self.pipeline_halt.is_none() {
                        self.pipeline_halt = Some(reason.clone());
                    }
                    self.shutdown_signal.notify_one();
                    return Err(reason);
                }
                return Err(self.fault_mutable_checkpoint_capture("operator state", &reason));
            }
        };
        let mut mutable_capture_guard = graph_capture_needs_mutable_guard(&graph)
            .then(|| MutableCheckpointCaptureGuard::new(Arc::clone(&self.checkpoint_fault)));

        let capture = self.capture_store_images(attempt, graph, serialization_permit);
        capture_timer.observe_duration();
        let (image, estimated_bytes) = match capture {
            Ok(capture) => capture,
            Err(error) => {
                self.full_vnode_capture_needed
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                self.mv_store.write().abort_view_logs(attempt.checkpoint_id);
                let error = format!("checkpoint image capture failed: {error}");
                return Err(fail_after_mutable_capture(
                    &mut mutable_capture_guard,
                    error,
                ));
            }
        };
        // Sealed view-log rows leave the stage, so a failure from here on must recover.
        if image.materialized_views.sealed_rows() && mutable_capture_guard.is_none() {
            mutable_capture_guard = Some(MutableCheckpointCaptureGuard::new(Arc::clone(
                &self.checkpoint_fault,
            )));
        }
        Ok(CapturedOperatorState {
            image,
            estimated_bytes,
            mutable_capture_guard,
        })
    }

    /// Capture the view logs and reference tables beside `graph` within the staged-state cap.
    /// A failure leaves the view logs to the caller's abort.
    fn capture_store_images(
        &self,
        attempt: CheckpointAttempt,
        graph: crate::operator_graph::GraphStateCapture,
        serialization_permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(OperatorStateCapture, u64), DbError> {
        let graph_estimate = graph.retained_bytes();
        let mv_budget = self
            .checkpoint_state_cap_bytes
            .checked_sub(graph_estimate)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "operator checkpoint capture estimate {graph_estimate} bytes exceeds the staged-state cap of {} bytes",
                    self.checkpoint_state_cap_bytes
                ))
            })?;
        let materialized_views = {
            let mut store = self.mv_store.write();
            store.advance_view_frontiers(|view| self.graph.stream_watermark(view));
            store.capture_view_logs(attempt.checkpoint_id, mv_budget)?
        };
        let table_budget = mv_budget
            .checked_sub(materialized_views.estimated_bytes())
            .ok_or_else(|| {
                DbError::Checkpoint("view-log capture estimate exceeded its admitted budget".into())
            })?;
        let reference_tables = self.table_store.read().capture_checkpoint(table_budget)?;
        let reference_table_estimate = reference_tables.as_ref().map_or(
            0,
            crate::table_store::ReferenceTableCheckpointCapture::estimated_bytes,
        );
        table_budget
            .checked_sub(reference_table_estimate)
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "reference-table checkpoint capture estimate exceeded its admitted budget"
                        .into(),
                )
            })?;
        let estimated_bytes = graph_estimate
            .checked_add(materialized_views.estimated_bytes())
            .and_then(|bytes| bytes.checked_add(reference_table_estimate))
            .ok_or_else(|| {
                DbError::Checkpoint("operator checkpoint capture estimate overflowed u64".into())
            })?;
        Ok((
            OperatorStateCapture {
                graph,
                materialized_views,
                reference_tables,
                serialization_permit,
            },
            estimated_bytes,
        ))
    }

    #[cfg(test)]
    pub(super) async fn capture_and_serialize_operator_state(
        &mut self,
    ) -> Result<Vec<crate::checkpoint_coordinator::CapturedStateFrame>, String> {
        let deadline = tokio::time::Instant::now() + self.serialization_timeout;
        let capture = self.capture_operator_state_until(CheckpointAttempt::new(1, 1), deadline)?;
        capture
            .serialize_until(
                self.checkpoint_state_cap_bytes,
                self.serialization_timeout,
                deadline,
            )
            .await
            .map(SerializedOperatorState::accept_for_test)
    }
}
