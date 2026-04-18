//! Swaps DataFusion's `RepartitionExec::Hash` above an
//! `AggregateExec::Partial` for our [`ClusterRepartitionExec`], which
//! hashes across instances via the shuffle transport. Bails out (no
//! rewrite) when hash expressions aren't plain column refs.

#![allow(clippy::disallowed_types)]

use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result as DfResult;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId, VnodeRegistry};

use super::cluster_repartition::ClusterRepartitionExec;

/// Built once per cluster `SessionState`; a no-op on non-matching plans.
pub struct DistributedAggregateRule {
    registry: Arc<VnodeRegistry>,
    sender: Arc<ShuffleSender>,
    receiver: Arc<ShuffleReceiver>,
    self_id: NodeId,
}

impl DistributedAggregateRule {
    /// Build a rule that splits aggregates across the given shuffle peers.
    #[must_use]
    pub fn new(
        registry: Arc<VnodeRegistry>,
        sender: Arc<ShuffleSender>,
        receiver: Arc<ShuffleReceiver>,
        self_id: NodeId,
    ) -> Self {
        Self {
            registry,
            sender,
            receiver,
            self_id,
        }
    }
}

impl std::fmt::Debug for DistributedAggregateRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedAggregateRule")
            .field("self_id", &self.self_id)
            .finish_non_exhaustive()
    }
}

impl PhysicalOptimizerRule for DistributedAggregateRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let transformed = plan.transform_up(|node| {
            let Some(repart) = node.as_any().downcast_ref::<RepartitionExec>() else {
                return Ok(Transformed::no(node));
            };
            let Partitioning::Hash(hash_exprs, _) = repart.partitioning() else {
                return Ok(Transformed::no(node));
            };
            let input = repart.input();
            let Some(agg) = input.as_any().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if !matches!(agg.mode(), AggregateMode::Partial) {
                return Ok(Transformed::no(node));
            }

            let mut hash_columns = Vec::with_capacity(hash_exprs.len());
            for expr in hash_exprs {
                let Some(col) = expr.as_any().downcast_ref::<Column>() else {
                    return Ok(Transformed::no(node));
                };
                hash_columns.push(col.index());
            }

            let new_exec = ClusterRepartitionExec::try_new(
                Arc::clone(input),
                hash_columns,
                Arc::clone(&self.registry),
                Arc::clone(&self.sender),
                Arc::clone(&self.receiver),
                self.self_id,
            )?;
            Ok(Transformed::yes(Arc::new(new_exec) as Arc<dyn ExecutionPlan>))
        })?;
        Ok(transformed.data)
    }

    fn name(&self) -> &'static str {
        "DistributedAggregateRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::functions_aggregate::sum::sum_udaf;
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
    use datafusion::physical_plan::repartition::RepartitionExec;
    use std::net::SocketAddr;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn sample_batch() -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()
    }

    async fn build_rule() -> DistributedAggregateRule {
        let registry = Arc::new(VnodeRegistry::single_owner(4, NodeId(1)));
        let sender = Arc::new(ShuffleSender::new(1));
        let recv = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .await
                .unwrap(),
        );
        DistributedAggregateRule::new(registry, sender, recv, NodeId(1))
    }

    /// Build `Partial → RepartitionExec::Hash(k) → FinalPartitioned`
    /// by hand (the shape DF would emit for `SELECT k, SUM(v) ... GROUP BY k`).
    fn build_plan() -> Arc<dyn ExecutionPlan> {
        let schema = test_schema();
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![sample_batch()]], Arc::clone(&schema), None)
                .unwrap();

        let group_by = PhysicalGroupBy::new_single(vec![(col("k", &schema).unwrap(), "k".into())]);
        let sum_v = AggregateExprBuilder::new(sum_udaf(), vec![col("v", &schema).unwrap()])
            .alias("sum_v")
            .schema(Arc::clone(&schema))
            .build()
            .unwrap();

        let partial = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Partial,
                group_by.clone(),
                vec![sum_v.clone().into()],
                vec![None],
                input,
                Arc::clone(&schema),
            )
            .unwrap(),
        );
        let repart = Arc::new(
            RepartitionExec::try_new(
                partial,
                Partitioning::Hash(vec![col("k", &schema).unwrap()], 4),
            )
            .unwrap(),
        );
        Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                group_by,
                vec![sum_v.into()],
                vec![None],
                repart,
                schema,
            )
            .unwrap(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rule_splices_cluster_repartition_between_partial_and_final() {
        let rule = build_rule().await;
        let plan = build_plan();

        // Sanity: plan starts with a RepartitionExec in the middle.
        let before = format!("{}", datafusion::physical_plan::displayable(plan.as_ref()).indent(true));
        assert!(before.contains("RepartitionExec"));
        assert!(!before.contains("ClusterRepartitionExec"));

        let optimized = rule.optimize(plan, &ConfigOptions::new()).unwrap();
        let after = format!("{}", datafusion::physical_plan::displayable(optimized.as_ref()).indent(true));
        assert!(
            after.contains("ClusterRepartitionExec"),
            "optimizer must splice ClusterRepartitionExec; plan was:\n{after}",
        );
        // "ClusterRepartitionExec" contains "RepartitionExec" as a
        // substring — filter it out before checking.
        let stripped = after.replace("ClusterRepartitionExec", "«replaced»");
        assert!(
            !stripped.contains("RepartitionExec"),
            "rule must REPLACE the RepartitionExec, not add alongside; plan was:\n{after}",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rule_is_noop_without_aggregate_partial_upstream() {
        let rule = build_rule().await;

        // Plan shape: RepartitionExec(Hash) directly on top of a memory
        // source — no Partial aggregate anywhere.
        let schema = test_schema();
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![sample_batch()]], Arc::clone(&schema), None)
                .unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(
                input,
                Partitioning::Hash(vec![col("k", &schema).unwrap()], 2),
            )
            .unwrap(),
        );

        let optimized = rule.optimize(plan, &ConfigOptions::new()).unwrap();
        let rendered = format!(
            "{}",
            datafusion::physical_plan::displayable(optimized.as_ref()).indent(true),
        );
        assert!(
            !rendered.contains("ClusterRepartitionExec"),
            "rule must not rewrite when upstream isn't Partial; plan was:\n{rendered}",
        );
    }
}
