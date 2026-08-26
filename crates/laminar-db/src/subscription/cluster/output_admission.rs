use std::collections::BTreeMap;

use laminar_core::checkpoint::{OutputPartitionId, PartitionSequence, StreamGeneration};

use super::output_state::PartitionBuffer;
use crate::error::DbError;
use crate::subscription::ClusterSubscriptionError;

pub(super) const MAX_OUTPUT_FRAME_BYTES: usize = 4 * 1024 * 1024;
pub(super) const MAX_PENDING_PARTITION_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_PENDING_OUTPUT_BYTES: usize = 256 * 1024 * 1024;
// INVARIANT: one hot stream may consume the process budget, but never exceed it.
pub(super) const MAX_PENDING_STREAM_BYTES: usize = MAX_PENDING_OUTPUT_BYTES;
pub(super) const MAX_PENDING_OUTPUT_FRAMES: usize = 65_536;

const COMMIT_BACKPRESSURE_NUMERATOR: usize = 3;
const COMMIT_BACKPRESSURE_DENOMINATOR: usize = 4;

#[derive(Default)]
pub(super) struct CyclePlan {
    pub(super) partitions: BTreeMap<(StreamGeneration, OutputPartitionId), PlanCount>,
    pub(super) streams: BTreeMap<StreamGeneration, PlanCount>,
    pub(super) retained_bytes: usize,
    pub(super) frame_count: usize,
}

#[derive(Default)]
pub(super) struct PlanCount {
    pub(super) retained_bytes: usize,
    pub(super) frame_count: usize,
}

impl CyclePlan {
    pub(super) fn add(
        &mut self,
        key: (StreamGeneration, OutputPartitionId),
        retained_bytes: usize,
    ) -> Result<(), DbError> {
        add_plan_count(self.partitions.entry(key).or_default(), retained_bytes)?;
        add_plan_count(self.streams.entry(key.0).or_default(), retained_bytes)?;
        self.retained_bytes = self
            .retained_bytes
            .checked_add(retained_bytes)
            .ok_or_else(|| DbError::Checkpoint("subscription output byte count overflow".into()))?;
        self.frame_count = self.frame_count.checked_add(1).ok_or_else(|| {
            DbError::Checkpoint("subscription output frame count overflow".into())
        })?;
        Ok(())
    }
}

fn add_plan_count(count: &mut PlanCount, retained_bytes: usize) -> Result<(), DbError> {
    count.retained_bytes = count
        .retained_bytes
        .checked_add(retained_bytes)
        .ok_or_else(|| DbError::Checkpoint("subscription output byte count overflow".into()))?;
    count.frame_count = count
        .frame_count
        .checked_add(1)
        .ok_or_else(|| DbError::Checkpoint("subscription output frame count overflow".into()))?;
    Ok(())
}

pub(super) fn at_commit_high_water(retained: usize, limit: usize) -> bool {
    retained >= limit / COMMIT_BACKPRESSURE_DENOMINATOR * COMMIT_BACKPRESSURE_NUMERATOR
}

pub(super) fn validate_partition_cut(
    partition_id: OutputPartitionId,
    partition: &PartitionBuffer,
    frontier: PartitionSequence,
) -> Result<(), DbError> {
    let Some(first) = partition.frames.first() else {
        return Ok(());
    };
    let mut expected = first.id.sequence;
    for frame in &partition.frames {
        if frame.id.partition != partition_id {
            return Err(ClusterSubscriptionError::ManifestCorrupt {
                reason: "buffered frame belongs to a different output partition".into(),
            }
            .into());
        }
        if frame.id.sequence != expected {
            return Err(ClusterSubscriptionError::PartitionSequenceGap {
                partition: partition_id,
                expected,
                actual: frame.id.sequence,
            }
            .into());
        }
        expected = expected.checked_next().map_err(|error| {
            DbError::Checkpoint(format!("advance subscription sequence: {error}"))
        })?;
    }
    if expected != frontier {
        return Err(ClusterSubscriptionError::PartitionSequenceGap {
            partition: partition_id,
            expected: frontier,
            actual: expected,
        }
        .into());
    }
    Ok(())
}

pub(super) fn resource_error(resource: &str, limit: usize) -> DbError {
    DbError::Checkpoint(format!(
        "cluster subscription {resource} reached its bounded {limit}-byte/count limit"
    ))
}
