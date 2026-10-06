//! Vnode-owned callback sequencing for a callback order reproducible on replay.

use super::execution::ProcessExecution;
use super::{ProcessActivation, ProcessFunctionOperator};
use crate::error::DbError;

pub(super) const CLUSTER_ACTIVATION_ID_ABI: u16 = 1;

impl ProcessFunctionOperator {
    pub(super) fn reserve_activation_id(
        &mut self,
        key: &[u8],
        local_id: u64,
    ) -> Result<u64, DbError> {
        if matches!(self.execution, ProcessExecution::Local) {
            return Ok(local_id);
        }
        let vnode = self.vnode_for(key);
        let sequence = self.activation_sequences[vnode];
        // RECOVERY: the namespace is pipeline/operator/vnode, independent of its process owner.
        // The fixed vnode domain makes this encoding injective without hashes or tombstone keys.
        let id = sequence
            .checked_mul(u64::from(self.vnode_count.get()))
            .and_then(|base| base.checked_add(vnode as u64))
            .ok_or_else(|| {
                DbError::PipelineTerminal("process vnode activation ID exhausted".into())
            })?;
        self.activation_sequences[vnode] = sequence.checked_add(1).ok_or_else(|| {
            DbError::PipelineTerminal("process vnode activation sequence exhausted".into())
        })?;
        Ok(id)
    }

    pub(super) fn reclaim_activation_id(&mut self, id: u64) {
        if matches!(self.execution, ProcessExecution::Local) {
            return;
        }
        let count = u64::from(self.vnode_count.get());
        let vnode =
            usize::try_from(id % count).expect("encoded vnode fits its allocated state domain");
        let sequence = id / count;
        assert_eq!(self.activation_sequences[vnode] - 1, sequence);
        self.activation_sequences[vnode] = sequence;
    }

    pub(super) fn reserve_native_activation_ids(
        &mut self,
        activations: &mut [ProcessActivation],
    ) -> Result<(), DbError> {
        self.require_execution_current()?;
        if matches!(self.execution, ProcessExecution::Local) {
            return Ok(());
        }
        for index in 0..activations.len() {
            match self.reserve_activation_id(&activations[index].key, activations[index].id) {
                Ok(id) => activations[index].id = id,
                Err(error) => {
                    self.reclaim_activation_ids(&activations[..index]);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    pub(super) fn reclaim_activation_ids(&mut self, activations: &[ProcessActivation]) {
        for activation in activations.iter().rev() {
            self.reclaim_activation_id(activation.id);
        }
    }

    pub(super) fn cluster_retained_bytes(&self) -> usize {
        self.execution
            .retained_bytes()
            .saturating_add(self.shuffle.retained_bytes())
            .saturating_add(
                self.activation_sequences
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            )
    }
}
