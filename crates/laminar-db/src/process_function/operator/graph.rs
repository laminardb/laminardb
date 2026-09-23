use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use async_trait::async_trait;
use laminar_core::state::PARTITIONING_ABI_VERSION;
use rustc_hash::FxHashMap;

use super::{charged_key, OperatorFrame, ProcessFunctionOperator, VnodeFrame};
use crate::error::DbError;
use crate::operator::capability::{OperatorCapability, OperatorImplementation};
use crate::operator_graph::{
    CapturedVnodeState, GraphOperator, InputFrontier, ManagedStateAccountingSnapshot,
    OperatorCheckpoint, StateFrameCapture,
};
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
        self.due
            .first()
            .is_some_and(|timer| timer.0 <= self.watermark_us)
    }

    fn advances_frontier_without_input(&self) -> bool {
        true
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
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
        if vnode_count != self.vnode_count.get() {
            return Err(DbError::Checkpoint("process vnode domain changed".into()));
        }
        let mut remaining = max_capture_bytes;
        let mut captured = Vec::with_capacity(required_vnodes.len());
        for &vnode in required_vnodes {
            let state = self.state.get(vnode as usize).ok_or_else(|| {
                DbError::Checkpoint("process checkpoint requested invalid vnode".into())
            })?;
            let mut entries = state
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<Vec<_>>();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            let bytes = serde_json::to_vec(&VnodeFrame {
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
        let mut previous: Option<&[u8]> = None;
        let mut restored = FxHashMap::default();
        for (key, state) in &frame.entries {
            let encoded_key = self
                .key_codec
                .encode_columns(&[Arc::new(StringArray::from(vec![state.key_text.as_str()]))])
                .map_err(|error| {
                    DbError::Checkpoint(format!("encode restored process key: {error}"))
                })?;
            if previous.is_some_and(|before| before >= key.as_slice())
                || self.vnode_for(key) != vnode as usize
                || state.is_empty()
                || encoded_key.row(0).data() != key.as_slice()
            {
                return Err(DbError::Checkpoint(
                    "invalid process vnode key roster".into(),
                ));
            }
            previous = Some(key);
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
            restored.insert(key.clone(), state.clone());
        }
        let delta = frame.entries.iter().try_fold(0usize, |sum, (key, state)| {
            sum.checked_add(charged_key(key, state)?).ok_or_else(|| {
                DbError::Checkpoint("process restored state byte accounting overflow".into())
            })
        })?;
        let live_bytes = self.live_bytes.checked_add(delta).ok_or_else(|| {
            DbError::Checkpoint("process restored state byte accounting overflow".into())
        })?;
        let key_count = self.key_count.checked_add(restored.len()).ok_or_else(|| {
            DbError::Checkpoint("process restored key accounting overflow".into())
        })?;
        let restored_timers = restored.values().try_fold(0usize, |count, state| {
            count.checked_add(state.timers.len()).ok_or_else(|| {
                DbError::Checkpoint("process restored timer accounting overflow".into())
            })
        })?;
        let timer_count = self
            .timer_count
            .checked_add(restored_timers)
            .ok_or_else(|| {
                DbError::Checkpoint("process restored timer accounting overflow".into())
            })?;
        if live_bytes
            > self
                .descriptor
                .limits
                .max_state_bytes
                .min(self.graph_budget)
            || key_count > self.descriptor.limits.max_keys
            || timer_count > self.descriptor.limits.max_timers
        {
            return Err(DbError::Checkpoint(
                "process restored state or timer budget exceeded".into(),
            ));
        }
        for (key, state) in frame.entries {
            for (name, timer) in &state.timers {
                self.due
                    .insert((timer.at_us, key.clone(), name.clone(), timer.generation));
            }
        }
        self.state[vnode as usize] = restored;
        self.live_bytes = live_bytes;
        self.key_count = key_count;
        self.timer_count = timer_count;
        Ok(())
    }
}
