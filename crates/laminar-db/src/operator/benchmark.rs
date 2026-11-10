//! Feature-gated access to local interval-join cycles, local EOWC window cycles, and one- and
//! two-node cluster aggregate cycles for Criterion.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use datafusion::prelude::SessionContext;
use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
use laminar_core::cluster::control::LeaseDeadline;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId, VnodeRegistry, DEFAULT_KEY_GROUP_COUNT};
use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{StreamJoinConfig, WindowOperatorConfig};

use super::eowc_query::EowcQueryOperator;
use super::interval_join::IntervalJoinOperator;
use super::sql_query::{ClusterShuffleConfig, SqlQueryOperator};
use crate::operator_graph::{GraphOperator, InputFrontier};
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

/// `EMIT ON WINDOW CLOSE` window aggregate over the default key-group topology, used only by the
/// non-default benchmark feature.
pub struct EowcWindowBenchmark(EowcQueryOperator);

impl EowcWindowBenchmark {
    /// Plan `sql` with `window` against tables registered in `ctx`.
    ///
    /// # Errors
    /// Returns planning or window-state initialization errors.
    pub async fn new(
        ctx: SessionContext,
        sql: &str,
        window: WindowOperatorConfig,
    ) -> Result<Self, DbError> {
        let mut operator = EowcQueryOperator::new(
            "eowc_window_bench",
            sql,
            Some(EmitClause::OnWindowClose),
            Some(window),
            ctx,
            DEFAULT_KEY_GROUP_COUNT,
            None,
        );
        operator.initialize_managed_state().await?;
        Ok(Self(operator))
    }

    /// Run one cycle: pre-aggregate `batch`, apply it to each row's windows, then close and emit
    /// every window that ends at or before `watermark`.
    ///
    /// # Errors
    /// Returns pre-aggregation, accumulator, or output errors.
    pub async fn process(
        &mut self,
        batch: RecordBatch,
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.process(&[vec![batch]], &[watermark]).await
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

/// Keyed aggregate on node 1 of a two-node cluster whose nodes own alternate vnodes. Node 2 is a
/// live loopback shuffle endpoint that only drains what it receives, so frames node 1 sends cross
/// the real transport.
pub struct TwoNodeAggregateBenchmark {
    operator: SqlQueryOperator,
    peer: Arc<ShuffleReceiver>,
    _peer_sender: Arc<ShuffleSender>,
    watermark: i64,
}

impl TwoNodeAggregateBenchmark {
    /// Plan `sql` against tables registered in `ctx`; `colocated` selects the colocated input
    /// route instead of repartitioning.
    ///
    /// # Errors
    /// Returns planning, state-initialization, or shuffle-binding errors.
    pub async fn new(ctx: SessionContext, sql: &str, colocated: bool) -> Result<Self, DbError> {
        let setup =
            |error: &dyn std::fmt::Display| DbError::Pipeline(format!("bench scope: {error}"));
        let owners: Vec<u64> = (0..u32::from(DEFAULT_KEY_GROUP_COUNT.get()))
            .map(two_node_owner)
            .collect();
        let registry = Arc::new(VnodeRegistry::new(u32::from(DEFAULT_KEY_GROUP_COUNT.get())));
        registry.set_assignment(owners.iter().copied().map(NodeId).collect());
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
            [1, 2]
                .into_iter()
                .map(|node_id| CheckpointParticipant {
                    node_id,
                    boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
                })
                .collect(),
        )
        .map_err(|e| setup(&e))?;
        let (sender, receiver) = loopback_endpoint(1, &fence, &owners).await?;
        let (peer_sender, peer) = loopback_endpoint(2, &fence, &owners).await?;
        sender.register_peer(2, peer.local_addr());
        peer_sender.register_peer(1, receiver.local_addr());
        let mut operator = SqlQueryOperator::new_with_key_groups(
            "cluster_aggregate_bench",
            sql,
            ctx,
            None,
            true,
            DEFAULT_KEY_GROUP_COUNT,
        );
        operator.initialize_managed_state().await?;
        operator.attach_cluster_shuffle(ClusterShuffleConfig {
            registry,
            sender,
            receiver,
            topology: None,
            self_id: NodeId(1),
        });
        if colocated {
            operator.colocate_input();
        }
        Ok(Self {
            operator,
            peer,
            _peer_sender: peer_sender,
            watermark: 0,
        })
    }

    /// The rows of `batch` whose group key, its first column, hashes to a vnode node 1 owns.
    ///
    /// # Errors
    /// Returns routing or Arrow filter errors.
    pub fn owned_rows(batch: &RecordBatch) -> Result<RecordBatch, DbError> {
        let vnodes = super::sql_query::hash_rows_to_vnodes(
            batch,
            1,
            u32::from(DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .map_err(|e| DbError::Pipeline(e.to_string()))?;
        let owned = arrow::array::BooleanArray::from(
            vnodes
                .iter()
                .map(|&vnode| two_node_owner(vnode) == 1)
                .collect::<Vec<_>>(),
        );
        arrow::compute::filter_record_batch(batch, &owned)
            .map_err(|e| DbError::Pipeline(e.to_string()))
    }

    /// Run `batch` (or no rows) with the node-local frontier moved forward by `advance`, then
    /// repeat empty cycles until node 1 has applied it and broadcast its frontier. Between cycles
    /// node 2 drains what it received and the step yields once, as the coordinator does while it
    /// waits for its next wake, so shuffle stream tasks run. Returns the output and the number of
    /// cycles the step took.
    ///
    /// # Errors
    /// Returns routing, shuffle, accumulator, or output errors, or a timeout when the send never
    /// completes.
    pub async fn step(
        &mut self,
        batch: Option<&RecordBatch>,
        advance: i64,
    ) -> Result<(Vec<RecordBatch>, usize), DbError> {
        self.watermark += advance;
        let frontier = [InputFrontier {
            watermark: Some(self.watermark),
            idle: false,
        }];
        let inputs = batch
            .map(|batch| vec![vec![batch.clone()]])
            .unwrap_or_default();
        let mut output = self
            .operator
            .process_with_frontiers(&inputs, &frontier)
            .await?;
        let mut cycles = 1;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                drop(self.peer.drain_available());
                tokio::task::yield_now().await;
                if !self.operator.checkpoint_drain_pending() {
                    return Ok::<_, DbError>(());
                }
                output.extend(self.operator.process_with_frontiers(&[], &frontier).await?);
                cycles += 1;
            }
        })
        .await
        .map_err(|_| DbError::Pipeline("two-node bench send timed out".into()))??;
        Ok((output, cycles))
    }
}

/// The node that owns `vnode` in the two-node benchmark: they alternate.
fn two_node_owner(vnode: u32) -> u64 {
    1 + u64::from(vnode % 2)
}

/// A shuffle sender and receiver for `node`, bound on loopback under `fence`.
async fn loopback_endpoint(
    node: u64,
    fence: &CheckpointAssignmentFence,
    owners: &[u64],
) -> Result<(Arc<ShuffleSender>, Arc<ShuffleReceiver>), DbError> {
    let setup = |error: &dyn std::fmt::Display| DbError::Pipeline(format!("bench scope: {error}"));
    let incarnation = uuid::Uuid::from_u128(u128::from(node));
    let receiver = Arc::new(
        ShuffleReceiver::bind(
            node,
            "127.0.0.1:0".parse().map_err(|e| setup(&e))?,
            incarnation,
        )
        .await
        .map_err(|e| setup(&e))?,
    );
    let sender = Arc::new(ShuffleSender::new(node, incarnation));
    sender
        .bind_process_lease_deadline_pair(
            &receiver,
            Arc::new(LeaseDeadline::live_for(Duration::from_secs(3600))),
        )
        .map_err(|e| setup(&e))?;
    sender
        .install_assignment_fence(fence, owners)
        .map_err(|e| setup(&e))?;
    receiver
        .install_assignment_fence(fence, owners)
        .map_err(|e| setup(&e))?;
    Ok((sender, receiver))
}
