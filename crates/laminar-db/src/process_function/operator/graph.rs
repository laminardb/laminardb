use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use async_trait::async_trait;
use laminar_core::state::PARTITIONING_ABI_VERSION;
use rustc_hash::FxHashMap;

use super::{charged_key, OperatorFrame, ProcessFunctionOperator, VnodeCapture, VnodeFrame};
use crate::error::DbError;
use crate::operator::capability::{OperatorCapability, OperatorImplementation};
use crate::operator_graph::{
    CapturedVnodeState, GraphOperator, InputFrontier, ManagedStateAccountingSnapshot,
    OperatorCheckpoint, StateFrameCapture,
};
#[cfg(feature = "process-remote")]
use crate::process_function::ProcessHandler;
use crate::process_function::STATE_CODEC_VERSION;
#[async_trait]
impl GraphOperator for ProcessFunctionOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::fixed(OperatorImplementation::ProcessFunction)
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        Some(ManagedStateAccountingSnapshot {
            live: self.live_bytes,
            prepared: 0,
            retired: 0,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.graph_budget = bytes;
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
        #[cfg(feature = "process-remote")]
        if matches!(&self.handler, ProcessHandler::Remote(_)) {
            return self.process_remote(inputs, frontiers);
        }
        if inputs.len() > 1 || frontiers.len() != 1 {
            return Err(DbError::InvalidOperation(
                "process function requires exactly one input frontier".into(),
            ));
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
        let frame = OperatorFrame {
            codec: STATE_CODEC_VERSION,
            descriptor_sha256: self.descriptor_sha256.clone(),
            partitioning_abi: PARTITIONING_ABI_VERSION,
            vnode_count: self.vnode_count.get(),
            next_activation_id: self.next_activation_id,
            next_timer_generation: self.next_timer_generation,
            watermark_us: self.watermark_us,
        };
        let data = serde_json::to_vec(&frame)
            .map_err(|error| DbError::Checkpoint(format!("encode process checkpoint: {error}")))?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let frame: OperatorFrame = serde_json::from_slice(&checkpoint.data)
            .map_err(|error| DbError::Checkpoint(format!("decode process checkpoint: {error}")))?;
        if frame.codec != STATE_CODEC_VERSION
            || frame.descriptor_sha256 != self.descriptor_sha256
            || frame.partitioning_abi != PARTITIONING_ABI_VERSION
            || frame.vnode_count != self.vnode_count.get()
        {
            return Err(DbError::Checkpoint(
                "process function checkpoint binding or state codec mismatch".into(),
            ));
        }
        self.next_activation_id = frame.next_activation_id;
        self.next_timer_generation = frame.next_timer_generation;
        self.watermark_us = frame.watermark_us;
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
            let bytes = serde_json::to_vec(&VnodeCapture {
                codec: STATE_CODEC_VERSION,
                vnode,
                entries,
            })
            .map_err(|error| DbError::Checkpoint(format!("encode process vnode: {error}")))?;
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
        if vnode_count != self.vnode_count.get() || vnode >= vnode_count {
            return Err(DbError::Checkpoint(
                "process restore vnode domain mismatch".into(),
            ));
        }
        // A JSON string byte can expand to six escaped bytes. The fixed per-key and per-timer
        // charges cover framing, so a valid image cannot exceed this bound.
        let state_limit = self
            .descriptor
            .limits
            .max_state_bytes
            .min(self.graph_budget);
        let remaining_state_bytes = state_limit.checked_sub(self.live_bytes).ok_or_else(|| {
            DbError::Checkpoint("process restored state exceeds its budget".into())
        })?;
        let frame_limit = remaining_state_bytes.saturating_mul(6).saturating_add(128);
        if bytes.len() > frame_limit {
            return Err(DbError::Checkpoint(
                "process vnode frame exceeds state budget".into(),
            ));
        }
        let frame: VnodeFrame = serde_json::from_slice(bytes)
            .map_err(|error| DbError::Checkpoint(format!("decode process vnode: {error}")))?;
        if frame.codec != STATE_CODEC_VERSION || frame.vnode != vnode {
            return Err(DbError::Checkpoint(
                "process vnode codec or identity mismatch".into(),
            ));
        }
        if !self.state[vnode as usize].is_empty() {
            return Err(DbError::Checkpoint("process vnode restored twice".into()));
        }
        let mut previous: Option<Vec<u8>> = None;
        let mut restored = FxHashMap::default();
        let mut staged_due = std::collections::BTreeSet::new();
        let mut live_bytes = self.live_bytes;
        let mut key_count = self.key_count;
        let mut timer_count = self.timer_count;
        for (key, state) in frame.entries {
            let encoded_key = self
                .key_codec
                .encode_columns(&[Arc::new(StringArray::from(vec![state.key_text.as_str()]))])
                .map_err(|error| {
                    DbError::Checkpoint(format!("encode restored process key: {error}"))
                })?;
            if previous
                .as_deref()
                .is_some_and(|before| before >= key.as_slice())
                || self.vnode_for(&key) != vnode as usize
                || state.is_empty()
                || encoded_key.row(0).data() != key.as_slice()
            {
                return Err(DbError::Checkpoint(
                    "invalid process vnode key roster".into(),
                ));
            }
            previous = Some(key.clone());
            for (name, timer) in &state.timers {
                if !self
                    .descriptor
                    .timer_names
                    .iter()
                    .any(|declared| declared == name)
                    || timer.generation > self.next_timer_generation
                {
                    return Err(DbError::Checkpoint(
                        "invalid process timer in checkpoint".into(),
                    ));
                }
            }
            live_bytes = live_bytes
                .checked_add(charged_key(&key, &state)?)
                .ok_or_else(|| {
                    DbError::Checkpoint("process restored state byte accounting overflow".into())
                })?;
            key_count = key_count.checked_add(1).ok_or_else(|| {
                DbError::Checkpoint("process restored key accounting overflow".into())
            })?;
            timer_count = timer_count.checked_add(state.timers.len()).ok_or_else(|| {
                DbError::Checkpoint("process restored timer accounting overflow".into())
            })?;
            if live_bytes > state_limit
                || key_count > self.descriptor.limits.max_keys
                || timer_count > self.descriptor.limits.max_timers
            {
                return Err(DbError::Checkpoint(
                    "process restored state or timer budget exceeded".into(),
                ));
            }
            for (name, timer) in &state.timers {
                staged_due.insert((timer.at_us, key.clone(), name.clone(), timer.generation));
            }
            restored.insert(key, state);
        }
        self.due.append(&mut staged_due);
        self.state[vnode as usize] = restored;
        self.live_bytes = live_bytes;
        self.key_count = key_count;
        self.timer_count = timer_count;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{VnodeCapture, VnodeFrame};
    use crate::process_function::operator::KeyState;
    use crate::process_function::{ValueState, STATE_CODEC_VERSION};

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
