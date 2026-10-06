use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::StringArray;
use laminar_core::state::PARTITIONING_ABI_VERSION;
use rustc_hash::FxHashMap;

use super::{charged_key, DueTimer, KeyState, OperatorFrame, ProcessFunctionOperator, VnodeFrame};
use crate::error::DbError;
use crate::process_function::STATE_CODEC_VERSION;

// Distributed metadata adds a bounded participant/frontier roster. Pending input drains first.
#[cfg(feature = "cluster")]
pub(super) const MAX_OPERATOR_FRAME_BYTES: usize = 256 * 1_024;
pub(super) const MAX_LOCAL_METADATA_FRAME_BYTES: usize = 512;
#[cfg(not(feature = "cluster"))]
pub(super) const MAX_OPERATOR_FRAME_BYTES: usize = MAX_LOCAL_METADATA_FRAME_BYTES;

pub(super) struct RestoredVnode {
    pub(super) state: FxHashMap<Vec<u8>, KeyState>,
    #[cfg(feature = "cluster")]
    pub(super) activation_sequence: Option<u64>,
    pub(super) due: BTreeSet<DueTimer>,
    pub(super) live_bytes: usize,
}

impl ProcessFunctionOperator {
    pub(super) fn checkpoint_frame(&self) -> OperatorFrame {
        OperatorFrame {
            codec: STATE_CODEC_VERSION,
            descriptor_sha256: self.descriptor_sha256.clone(),
            partitioning_abi: PARTITIONING_ABI_VERSION,
            vnode_count: self.vnode_count.get(),
            next_activation_id: self.next_activation_id,
            next_timer_generation: self.next_timer_generation,
            watermark_us: self.watermark_us,
            #[cfg(feature = "cluster")]
            activation_id_abi: self
                .execution
                .local_id()
                .map(|_| super::sequencing::CLUSTER_ACTIVATION_ID_ABI),
            #[cfg(feature = "cluster")]
            shuffle: self.shuffle.checkpoint(),
        }
    }

    pub(super) fn decode_metadata(&self, bytes: &[u8]) -> Result<OperatorFrame, DbError> {
        if bytes.len() > MAX_OPERATOR_FRAME_BYTES {
            return Err(DbError::Checkpoint(
                "process metadata frame exceeds its size bound".into(),
            ));
        }
        let frame: OperatorFrame = serde_json::from_slice(bytes)
            .map_err(|error| DbError::Checkpoint(format!("decode process checkpoint: {error}")))?;
        #[cfg(feature = "cluster")]
        if frame.shuffle.is_none() && bytes.len() > MAX_LOCAL_METADATA_FRAME_BYTES {
            return Err(DbError::Checkpoint(
                "process metadata frame exceeds its local size bound".into(),
            ));
        }
        if frame.codec != STATE_CODEC_VERSION
            || frame.descriptor_sha256 != self.descriptor_sha256
            || frame.partitioning_abi != PARTITIONING_ABI_VERSION
            || frame.vnode_count != self.vnode_count.get()
        {
            return Err(DbError::Checkpoint(
                "process function checkpoint binding or state codec mismatch".into(),
            ));
        }
        #[cfg(feature = "cluster")]
        if frame.activation_id_abi
            != self
                .execution
                .local_id()
                .map(|_| super::sequencing::CLUSTER_ACTIVATION_ID_ABI)
        {
            return Err(DbError::Checkpoint(
                if self.execution.local_id().is_some() {
                    "process cluster activation sequencing ABI mismatch"
                } else {
                    "distributed process restore requires cluster execution selection"
                }
                .into(),
            ));
        }
        #[cfg(feature = "cluster")]
        if let Some(shuffle) = &frame.shuffle {
            shuffle.validate(self.vnode_count.get(), frame.watermark_us)?;
            if shuffle
                .retained_bytes()
                .saturating_add(self.live_bytes)
                .saturating_add(self.cluster_retained_bytes())
                > self.graph_budget
            {
                return Err(DbError::Checkpoint(
                    "process shuffle restore exceeds its retained-state budget".into(),
                ));
            }
        }
        Ok(frame)
    }

    pub(super) fn decode_vnode(
        &self,
        vnode: u32,
        bytes: &[u8],
        timer_generation: u64,
        available_bytes: usize,
    ) -> Result<RestoredVnode, DbError> {
        // Escaped JSON bytes expand by at most six; fixed charges cover entry framing.
        if bytes.len() > available_bytes.saturating_mul(6).saturating_add(128) {
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
        #[cfg(feature = "cluster")]
        if frame.activation_sequence.is_some() != self.execution.local_id().is_some()
            || frame.activation_sequence.is_some_and(|sequence| {
                sequence != 0
                    && (sequence - 1)
                        .checked_mul(u64::from(self.vnode_count.get()))
                        .and_then(|base| base.checked_add(u64::from(vnode)))
                        .is_none()
            })
        {
            return Err(DbError::Checkpoint(
                "process vnode activation sequence is missing, incompatible, or exhausted".into(),
            ));
        }
        let mut previous: Option<Vec<u8>> = None;
        let mut restored = RestoredVnode {
            state: FxHashMap::default(),
            #[cfg(feature = "cluster")]
            activation_sequence: frame.activation_sequence,
            due: BTreeSet::new(),
            live_bytes: 0,
        };
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
                if !self.descriptor.timer_names.contains(name)
                    || timer.generation > timer_generation
                {
                    return Err(DbError::Checkpoint(
                        "invalid process timer in checkpoint".into(),
                    ));
                }
            }
            restored.live_bytes = restored
                .live_bytes
                .checked_add(charged_key(&key, &state)?)
                .ok_or_else(|| {
                    DbError::Checkpoint("process restored state byte accounting overflow".into())
                })?;
            if restored.live_bytes > available_bytes
                || restored.state.len() >= self.descriptor.limits.max_keys
                || restored.due.len().saturating_add(state.timers.len())
                    > self.descriptor.limits.max_timers
            {
                return Err(DbError::Checkpoint(
                    "process restored state or timer budget exceeded".into(),
                ));
            }
            for (name, timer) in &state.timers {
                restored
                    .due
                    .insert((timer.at_us, key.clone(), name.clone(), timer.generation));
            }
            restored.state.insert(key, state);
        }
        Ok(restored)
    }
}
