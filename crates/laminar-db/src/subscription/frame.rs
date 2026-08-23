//! Partition-aware sidecar for durable aggregate subscription output.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use laminar_core::checkpoint::{OutputDistributionCertificate, OutputFrameId};

/// One Arrow batch whose logical subscription partition is preserved outside the user schema.
#[derive(Clone)]
pub(crate) struct PartitionedOutputBatch {
    pub(crate) id: OutputFrameId,
    pub(crate) batch: RecordBatch,
}

/// Complete output prepared by one certified stream operator in one graph cycle.
pub(crate) struct PreparedSubscriptionOutput {
    pub(crate) certificate: Arc<OutputDistributionCertificate>,
    pub(crate) frames: Vec<PartitionedOutputBatch>,
}
