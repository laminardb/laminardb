use laminar_core::checkpoint::{OutputPartitionId, PartitionFrontier, PartitionSequence};

use super::IncrementalAggState;
use crate::error::DbError;

impl IncrementalAggState {
    /// Capture the exclusive output sequence for a canonical owned-vnode iterator.
    pub(crate) fn output_frontiers(
        &self,
        vnodes: impl IntoIterator<Item = u32>,
    ) -> Result<Vec<PartitionFrontier>, DbError> {
        vnodes
            .into_iter()
            .map(|vnode| {
                let partition = u16::try_from(vnode).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "aggregate output vnode {vnode} exceeds the partition ID space"
                    ))
                })?;
                let sequence = self
                    .vnode_states
                    .get(vnode)
                    .map_or(0, |state| state.next_output_sequence);
                Ok(PartitionFrontier {
                    partition: OutputPartitionId::new(partition),
                    through_sequence: PartitionSequence::new(sequence),
                })
            })
            .collect()
    }
}
