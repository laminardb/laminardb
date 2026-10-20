//! Feature-gated access to local interval-join cycles for Criterion.

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use laminar_core::state::DEFAULT_KEY_GROUP_COUNT;
use laminar_sql::translator::StreamJoinConfig;

use super::interval_join::IntervalJoinOperator;
use crate::operator_graph::GraphOperator;
use crate::DbError;

/// Unprojected interval join over the default key-group topology, used only by the non-default
/// benchmark feature.
pub struct IntervalJoinBenchmark(IntervalJoinOperator);

impl IntervalJoinBenchmark {
    /// Build a local operator for `config`.
    #[must_use]
    pub fn new(config: StreamJoinConfig) -> Self {
        Self(IntervalJoinOperator::new_with_key_groups(
            "interval_join_bench",
            config,
            None,
            SessionContext::new(),
            DEFAULT_KEY_GROUP_COUNT,
        ))
    }

    /// Run one cycle: route `[left, right]` by vnode, probe and buffer each side, then evict
    /// rows closed by `watermark` on both inputs.
    ///
    /// # Errors
    /// Returns validation, state-budget, or output errors.
    pub async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.process(inputs, &[watermark, watermark]).await
    }
}
