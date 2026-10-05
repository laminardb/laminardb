use super::restoration::MAX_OPERATOR_FRAME_BYTES;
use super::{ProcessFunctionOperator, VnodeCapture};
use crate::error::DbError;
use crate::operator::capability::{OperatorCapability, OperatorImplementation};
use crate::operator_graph::{
    CapturedVnodeState, GraphOperator, InputFrontier, ManagedStateAccountingSnapshot,
    OperatorCheckpoint, StateFrameCapture,
};
#[cfg(feature = "process-remote")]
use crate::process_function::ProcessHandler;
use crate::process_function::STATE_CODEC_VERSION;
use arrow::array::RecordBatch;
use async_trait::async_trait;
use laminar_core::serialization::BoundedBytesWriter;

#[async_trait]
impl GraphOperator for ProcessFunctionOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::ProcessFunction)
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        #[cfg(feature = "cluster")]
        let (prepared, retired) = self.vnode_transition.accounting();
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);
        Some(ManagedStateAccountingSnapshot {
            live: self.live_bytes,
            prepared,
            retired,
        })
    }

    #[cfg(feature = "cluster")]
    fn bind_startup_assignment(
        &mut self,
        assignment: &laminar_core::checkpoint::CheckpointAssignmentFence,
        owned_vnodes: &[u32],
    ) -> Result<(), DbError> {
        if !self.vnode_transition.is_idle() || self.checkpoint_drain_pending() {
            return Err(DbError::Checkpoint(
                "process assignment binding requires drained invocations and no staged transition"
                    .into(),
            ));
        }
        if !assignment.is_canonical()
            || assignment.vnode_count != self.vnode_count.get()
            || owned_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || owned_vnodes
                .iter()
                .any(|vnode| *vnode >= self.vnode_count.get())
        {
            return Err(DbError::Checkpoint(
                "process startup assignment or owned vnode roster is invalid".into(),
            ));
        }
        if self
            .assignment_fence
            .as_ref()
            .is_some_and(|current| current != assignment)
        {
            return Err(DbError::Checkpoint(
                "process startup cannot replace an installed assignment".into(),
            ));
        }
        if !self.metadata_restored
            && (self.next_activation_id != 0
                || self.next_timer_generation != 0
                || self.watermark_us != i64::MIN
                || self.key_count != 0
                || self.timer_count != 0)
        {
            return Err(DbError::Checkpoint(
                "process startup assignment requires fresh or restored state".into(),
            ));
        }
        for (slot, state) in self.state.iter().enumerate() {
            if state.is_empty() {
                continue;
            }
            let vnode = u32::try_from(slot)
                .map_err(|_| DbError::Checkpoint("process vnode is out of range".into()))?;
            if owned_vnodes.binary_search(&vnode).is_err() {
                return Err(DbError::Checkpoint(
                    "process startup image contains state outside local ownership".into(),
                ));
            }
        }
        if self.assignment_fence.is_none() {
            self.assignment_fence = Some(assignment.clone());
        }
        Ok(())
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.graph_budget = bytes;
    }

    #[cfg(feature = "cluster")]
    fn bind_process_execution_authority(
        &mut self,
        config: &crate::operator::sql_query::ClusterShuffleConfig,
        deadline: std::sync::Arc<laminar_core::cluster::control::LeaseDeadline>,
    ) -> Result<(), DbError> {
        if !matches!(
            self.execution,
            super::execution::ProcessExecution::AwaitingAssignment
        ) || !self.vnode_transition.is_idle()
            || self.checkpoint_drain_pending()
        {
            return Err(DbError::Checkpoint(
                "process execution binding requires a private startup graph".into(),
            ));
        }
        let assignment = self.assignment_fence.as_ref().ok_or_else(|| {
            DbError::Checkpoint("process execution requires bound startup state".into())
        })?;
        let authority =
            super::execution::ProcessExecutionAuthority::bind(config, assignment, deadline)?;
        self.execution = super::execution::ProcessExecution::SingleOwner(authority);
        Ok(())
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let frontiers = watermarks
            .iter()
            .copied()
            .map(|watermark| InputFrontier {
                watermark: (watermark != i64::MIN).then_some(watermark),
                idle: false,
            })
            .collect::<Vec<_>>();
        self.process_with_frontiers(inputs, &frontiers).await
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if inputs.len() > 1 || frontiers.len() != 1 {
            return Err(DbError::InvalidOperation(
                "process function requires exactly one input frontier".into(),
            ));
        }
        #[cfg(feature = "cluster")]
        let routed = self
            .route_owned_input(inputs)
            .map_err(|error| {
                if error.requires_pipeline_halt()
                    || error.requires_pipeline_recovery()
                    || error.is_shuffle_not_ready()
                {
                    error
                } else {
                    DbError::PipelineTerminal(format!(
                        "process function rejected routed input: {error}"
                    ))
                }
            })?
            .map(|batches| [batches]);
        #[cfg(feature = "cluster")]
        let inputs = routed.as_ref().map_or(inputs, |ports| ports.as_slice());
        #[cfg(feature = "process-remote")]
        if matches!(&self.handler, ProcessHandler::Remote(_)) {
            return self.process_remote(inputs, frontiers);
        }
        let start_id = self.next_activation_id;
        let old_watermark = self.watermark_us;
        let mut output = Vec::new();
        let mut rows = 0;
        let mut bytes = 0;
        let outcome = (|| {
            self.process_rows(inputs, &mut output, &mut rows, &mut bytes)?;
            if let Some(watermark_ms) = frontiers[0].watermark {
                self.watermark_us = self.watermark_us.max(watermark_ms.saturating_mul(1_000));
            }
            self.fire_due_timers(&mut output, &mut rows, &mut bytes)
        })();
        if let Err(error) = outcome {
            if self.next_activation_id != start_id {
                return Err(DbError::StatefulOperatorPartialApply(format!(
                    "process function accepted earlier activations before failure; recover from the committed checkpoint: {error}"
                )));
            }
            self.watermark_us = old_watermark;
            return Err(
                if error.requires_pipeline_halt() || error.requires_pipeline_recovery() {
                    error
                } else {
                    DbError::PipelineTerminal(format!(
                        "process function rejected an activation: {error}"
                    ))
                },
            );
        }
        Ok(output)
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        let mut output = input;
        #[cfg(feature = "process-remote")]
        if let Some(held_time_us) = self
            .remote
            .as_ref()
            .and_then(super::remote::RemoteExecution::held_time_us)
        {
            output = output.held_at(Some(held_time_us.saturating_sub(1).div_euclid(1_000)));
        }
        if let Some((at_us, ..)) = self.due.first() {
            if *at_us <= self.watermark_us {
                output.watermark = output
                    .watermark
                    .map(|watermark| watermark.min(at_us.saturating_sub(1).div_euclid(1_000)));
                output.idle = false;
            }
        }
        output
    }

    fn deferred_work_is_runnable(&self) -> bool {
        let due_now = self
            .due
            .first()
            .is_some_and(|timer| timer.0 <= self.watermark_us);
        #[cfg(feature = "process-remote")]
        if let Some(remote) = &self.remote {
            return remote.is_runnable(due_now);
        }
        due_now
    }

    fn wants_input(&self) -> bool {
        #[cfg(feature = "process-remote")]
        if let Some(remote) = &self.remote {
            return !remote.is_pending();
        }
        true
    }

    fn checkpoint_drain_pending(&self) -> bool {
        #[cfg(feature = "process-remote")]
        if let Some(remote) = &self.remote {
            return remote.is_pending();
        }
        false
    }

    fn advances_frontier_without_input(&self) -> bool {
        true
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        if self.checkpoint_drain_pending() {
            return Err(DbError::Checkpoint(
                "process worker invocation must drain before checkpoint capture".into(),
            ));
        }
        let frame = self.checkpoint_frame();
        let data = serde_json::to_vec(&frame)
            .map_err(|error| DbError::Checkpoint(format!("encode process checkpoint: {error}")))?;
        if data.len() > MAX_OPERATOR_FRAME_BYTES {
            return Err(DbError::Checkpoint(
                "process metadata frame exceeds its size bound".into(),
            ));
        }
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if !self.vnode_transition.is_idle() || self.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "process metadata restore requires an unbound operator without a vnode transition"
                    .into(),
            ));
        }
        if self.metadata_restored
            || self.next_activation_id != 0
            || self.next_timer_generation != 0
            || self.live_bytes != 0
            || self.watermark_us != i64::MIN
            || self.checkpoint_drain_pending()
        {
            return Err(DbError::Checkpoint(
                "process metadata restore requires a fresh operator before input admission".into(),
            ));
        }
        let frame = self.decode_metadata(&checkpoint.data)?;
        self.next_activation_id = frame.next_activation_id;
        self.next_timer_generation = frame.next_timer_generation;
        self.watermark_us = frame.watermark_us;
        self.metadata_restored = true;
        Ok(())
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        if self.checkpoint_drain_pending() {
            return Err(DbError::Checkpoint(
                "process worker invocation must drain before vnode capture".into(),
            ));
        }
        if vnode_count != self.vnode_count.get() {
            return Err(DbError::Checkpoint("process vnode domain changed".into()));
        }
        let mut remaining = max_capture_bytes;
        let mut captured = Vec::with_capacity(required_vnodes.len());
        for &vnode in required_vnodes {
            let state = self.state.get(vnode as usize).ok_or_else(|| {
                DbError::Checkpoint("process checkpoint requested invalid vnode".into())
            })?;
            let mut entries = state.iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            let mut writer =
                BoundedBytesWriter::new(usize::try_from(remaining).unwrap_or(usize::MAX));
            serde_json::to_writer(
                &mut writer,
                &VnodeCapture {
                    codec: STATE_CODEC_VERSION,
                    vnode,
                    entries,
                },
            )
            .map_err(|error| {
                DbError::Checkpoint(format!("process vnode capture budget exceeded: {error}"))
            })?;
            let bytes = writer.into_vec();
            remaining = remaining
                .checked_sub(u64::try_from(bytes.capacity()).unwrap_or(u64::MAX))
                .ok_or_else(|| {
                    DbError::Checkpoint("process vnode capture budget exceeded".into())
                })?;
            captured.push(CapturedVnodeState {
                vnode,
                state: Some(StateFrameCapture::encoded(bytes)),
            });
        }
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, bytes: &[u8]) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        if !self.vnode_transition.is_idle() || self.assignment_fence.is_some() {
            return Err(DbError::Checkpoint(
                "process vnode restore requires an unbound operator without a vnode transition"
                    .into(),
            ));
        }
        // RECOVERY: vnode bytes do not repeat the descriptor binding. Validate it in the whole
        // frame before installing keyed data, and keep worker proposals outside this restore cut.
        if !self.metadata_restored || self.checkpoint_drain_pending() {
            return Err(DbError::Checkpoint(
                "process vnode restore requires validated metadata and no pending invocation"
                    .into(),
            ));
        }
        if vnode_count != self.vnode_count.get() || vnode >= vnode_count {
            return Err(DbError::Checkpoint(
                "process restore vnode domain mismatch".into(),
            ));
        }
        let state_limit = self
            .descriptor
            .limits
            .max_state_bytes
            .min(self.graph_budget);
        let remaining_state_bytes = state_limit.checked_sub(self.live_bytes).ok_or_else(|| {
            DbError::Checkpoint("process restored state exceeds its budget".into())
        })?;
        if !self.state[vnode as usize].is_empty() {
            return Err(DbError::Checkpoint("process vnode restored twice".into()));
        }
        let mut restored = self.decode_vnode(
            vnode,
            bytes,
            self.next_timer_generation,
            remaining_state_bytes,
        )?;
        let key_count = self
            .key_count
            .checked_add(restored.state.len())
            .ok_or_else(|| {
                DbError::Checkpoint("process restored key accounting overflow".into())
            })?;
        let timer_count = self
            .timer_count
            .checked_add(restored.due.len())
            .ok_or_else(|| {
                DbError::Checkpoint("process restored timer accounting overflow".into())
            })?;
        if key_count > self.descriptor.limits.max_keys
            || timer_count > self.descriptor.limits.max_timers
        {
            return Err(DbError::Checkpoint(
                "process restored state or timer budget exceeded".into(),
            ));
        }
        self.due.append(&mut restored.due);
        self.state[vnode as usize] = restored.state;
        self.live_bytes += restored.live_bytes;
        self.key_count = key_count;
        self.timer_count = timer_count;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: crate::operator_graph::ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        let prepared = self.prepare_transition(&transition)?;
        self.vnode_transition = super::transition::ProcessVnodeTransition::Prepared(prepared);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        self.vnode_transition.abort();
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        self.publish_transition();
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        self.vnode_transition.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::{VnodeCapture, MAX_OPERATOR_FRAME_BYTES};
    use crate::process_function::operator::{KeyState, OperatorFrame, VnodeFrame};
    use crate::process_function::{ValueState, STATE_CODEC_VERSION};

    #[test]
    fn metadata_frame_bound_covers_maximum_field_widths() {
        let frame = OperatorFrame {
            codec: u32::MAX,
            descriptor_sha256: "f".repeat(64),
            partitioning_abi: u16::MAX,
            vnode_count: u32::MAX,
            next_activation_id: u64::MAX,
            next_timer_generation: u64::MAX,
            watermark_us: i64::MIN,
        };
        assert!(serde_json::to_vec(&frame).unwrap().len() <= MAX_OPERATOR_FRAME_BYTES);
    }

    #[test]
    fn vnode_capture_preserves_frame_encoding() {
        let entries = vec![(
            vec![1, 2],
            KeyState {
                key_text: "a".into(),
                value: ValueState::Value(7),
                timers: Default::default(),
            },
        )];
        let owned = serde_json::to_vec(&VnodeFrame {
            codec: STATE_CODEC_VERSION,
            vnode: 3,
            entries: entries.clone(),
        })
        .unwrap();
        let borrowed = serde_json::to_vec(&VnodeCapture {
            codec: STATE_CODEC_VERSION,
            vnode: 3,
            entries: entries.iter().map(|(key, state)| (key, state)).collect(),
        })
        .unwrap();
        assert_eq!(borrowed, owned);
        assert_eq!(
            borrowed,
            br#"{"codec":2,"vnode":3,"entries":[[[1,2],{"key_text":"a","value":{"Value":7},"timers":{}}]]}"#
        );
    }
}
