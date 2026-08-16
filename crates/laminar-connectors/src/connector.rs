//! Connector traits — async `SourceConnector` / `SinkConnector`.

mod contracts;
mod coordinated_commit;
mod sink;
mod source;
mod source_batch;
mod task_tracking;

pub use contracts::{
    DeliveryGuarantee, SinkConsistency, SinkContract, SinkInputMode, SinkTopology,
    SourceConsistency, SourceContract, SourceInputMode, SourceRowPositionCapability,
    SourceTopology,
};
pub use coordinated_commit::{
    CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, CoordinatedCommitPayload, CoordinatedCommitter,
    MAX_COORDINATED_COMMIT_BATCH_BYTES, MAX_COORDINATED_COMMIT_BATCH_ENTRIES,
    MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
pub use sink::{SinkConnector, WriteResult};
pub use source::{
    SourceConnector, SourceDrainOutcome, SourceDrainRequest, SourceDrainResolution, SourcePosition,
    SourceStart,
};
pub use source_batch::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
    source_mutations, source_mutations_routed, source_row_positions, strip_source_mutations,
    strip_source_mutations_routed, strip_source_row_positions, SourceBatch, SourceBatchCursor,
    SourceMutation, SourceMutationView, SourceRowPositionRef, SourceRowPositionView,
    SourceRowPositions, SOURCE_MUTATION_COLUMN, SOURCE_ORDER_KEY_COLUMN, SOURCE_PARTITION_COLUMN,
    SOURCE_SUB_OFFSET_COLUMN,
};
pub use task_tracking::{
    ConnectorCancellationPolicy, ConnectorTaskAdmission, ConnectorTaskGuard, ConnectorTaskOwner,
    ConnectorTaskTracker,
};

#[cfg(test)]
#[allow(clippy::cast_possible_wrap)]
mod tests;
