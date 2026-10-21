//! Feature-gated access to local interval-join cycles and one-node cluster aggregate cycles for
//! Criterion.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
use laminar_core::cluster::control::LeaseDeadline;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId, VnodeRegistry, DEFAULT_KEY_GROUP_COUNT};
use laminar_sql::translator::StreamJoinConfig;

use super::interval_join::IntervalJoinOperator;
use super::sql_query::{ClusterShuffleConfig, SqlQueryOperator};
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

/// Keyed aggregate attached to a cluster scope whose one node owns every vnode, so every row is
/// local and the cycle measures routing and apply without network transport.
pub struct ClusterAggregateBenchmark(SqlQueryOperator);

impl ClusterAggregateBenchmark {
    /// Plan `sql` against tables registered in `ctx`; `colocated` selects the colocated input
    /// route instead of repartitioning.
    ///
    /// # Errors
    /// Returns planning, state-initialization, or shuffle-binding errors.
    pub async fn new(ctx: SessionContext, sql: &str, colocated: bool) -> Result<Self, DbError> {
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "cluster_aggregate_bench",
            sql,
            ctx,
            None,
            true,
            DEFAULT_KEY_GROUP_COUNT,
        );
        operator.initialize_managed_state().await?;
        operator.attach_cluster_shuffle(one_node_scope().await?);
        if colocated {
            operator.colocate_input();
        }
        Ok(Self(operator))
    }

    /// Run one cycle over `batch`: pre-aggregate, route, and apply.
    ///
    /// # Errors
    /// Returns routing, accumulator, or output errors.
    pub async fn process(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, DbError> {
        self.0.process(&[vec![batch.clone()]], &[i64::MIN]).await
    }
}

async fn one_node_scope() -> Result<ClusterShuffleConfig, DbError> {
    let setup = |error: &dyn std::fmt::Display| DbError::Pipeline(format!("bench scope: {error}"));
    let owners = vec![1_u64; usize::from(DEFAULT_KEY_GROUP_COUNT.get())];
    let registry = Arc::new(VnodeRegistry::new(u32::from(DEFAULT_KEY_GROUP_COUNT.get())));
    registry.set_assignment(owners.iter().copied().map(NodeId).collect());
    let incarnation = uuid::Uuid::from_u128(1);
    let receiver = Arc::new(
        ShuffleReceiver::bind(
            1,
            "127.0.0.1:0".parse().map_err(|e| setup(&e))?,
            incarnation,
        )
        .await
        .map_err(|e| setup(&e))?,
    );
    let sender = Arc::new(ShuffleSender::new(1, incarnation));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(3600)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .map_err(|e| setup(&e))?;
    sender
        .install_process_lease_deadline(deadline)
        .map_err(|e| setup(&e))?;
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &owners,
        vec![CheckpointParticipant {
            node_id: 1,
            boot_incarnation: incarnation,
        }],
    )
    .map_err(|e| setup(&e))?;
    sender
        .install_assignment_fence(&fence, &owners)
        .map_err(|e| setup(&e))?;
    receiver
        .install_assignment_fence(&fence, &owners)
        .map_err(|e| setup(&e))?;
    Ok(ClusterShuffleConfig {
        registry,
        sender,
        receiver,
        topology: None,
        self_id: NodeId(1),
    })
}
