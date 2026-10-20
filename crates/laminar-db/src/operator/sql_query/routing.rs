//! Cluster routing of pre-aggregated input batches: local apply, remote shuffle (as per-batch
//! partials for a global aggregate), and the frontier frames every peer receives.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use laminar_core::shuffle::{ShuffleMessage, ShuffleRoutingError};
use laminar_core::state::{NodeId, VnodeAssignmentSnapshot};

use super::{AggClusterInputPlan, ClusterShuffleConfig, QueryState, SqlQueryOperator};
use crate::aggregate_state::IncrementalAggState;
use crate::error::DbError;
use crate::operator_graph::InputFrontier;

/// How the aggregate's input port receives rows, as the exchange plan decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateInputRoute {
    /// Hash every row on its group key and send it to the vnode owner.
    Repartition,
    /// The upstream already placed every row on the owner of its group key's vnode. Each batch is
    /// still checked against the assignment; a batch produced under an earlier assignment and
    /// buffered across a vnode transition is repartitioned like any other.
    Colocated,
}

impl SqlQueryOperator {
    pub(crate) fn colocate_input(&mut self) {
        self.input_route = AggregateInputRoute::Colocated;
    }

    pub(super) fn plan_cluster_batches(
        &self,
        batches: Vec<RecordBatch>,
        local_frontier: InputFrontier,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
        peers: &[u64],
    ) -> Result<AggClusterInputPlan, DbError> {
        let QueryState::Agg(aggregate) = &self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' is not initialized",
                self.op_name
            )));
        };
        let batches: Vec<RecordBatch> = batches
            .into_iter()
            .filter(|batch| batch.num_rows() != 0)
            .collect();
        let (local_batches, mut remote_data) =
            match self.remote_global_owner(aggregate, &batches, assignment, config.self_id)? {
                Some(owner) => (
                    Vec::new(),
                    BTreeMap::from([(
                        owner.0,
                        vec![ShuffleMessage::checkpointed_routed(
                            self.op_name.to_string(),
                            Arc::from([0]),
                            aggregate.global_partial_batch(&batches)?,
                        )],
                    )]),
                ),
                None => self.route_rows(aggregate, batches, config, assignment)?,
            };
        let mut outbound = Vec::new();
        for &peer in peers {
            let data = remote_data.remove(&peer);
            let has_data = data.as_ref().is_some_and(|messages| !messages.is_empty());
            if has_data && self.last_broadcast.idle && !local_frontier.idle {
                outbound.push((
                    peer,
                    ShuffleMessage::Frontier {
                        stage: self.op_name.to_string(),
                        watermark: self.last_broadcast.watermark,
                        idle: false,
                    },
                ));
            }
            if let Some(messages) = data {
                outbound.extend(messages.into_iter().map(|message| (peer, message)));
            }
            if has_data || self.last_broadcast != local_frontier {
                outbound.push((
                    peer,
                    ShuffleMessage::Frontier {
                        stage: self.op_name.to_string(),
                        watermark: local_frontier.watermark,
                        idle: local_frontier.idle,
                    },
                ));
            }
        }
        if !remote_data.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' routed data outside its peer roster",
                self.op_name
            )));
        }
        let effective_frontier = self.effective_cluster_frontier(local_frontier, None)?;
        Ok(AggClusterInputPlan {
            local_batches,
            outbound,
            local_frontier,
            effective_frontier,
        })
    }

    /// The owner of the global vnode when it is another node; only then do this node's rows
    /// travel, folded into partials. The owner applies its own rows directly.
    fn remote_global_owner(
        &self,
        aggregate: &IncrementalAggState,
        batches: &[RecordBatch],
        assignment: &VnodeAssignmentSnapshot,
        self_id: NodeId,
    ) -> Result<Option<NodeId>, DbError> {
        if aggregate.num_group_cols() != 0 || batches.is_empty() {
            return Ok(None);
        }
        let error = match assignment.owners().first() {
            Some(owner) if owner.is_unassigned() => {
                ShuffleRoutingError::UnassignedVnode { vnode: 0 }
            }
            Some(&owner) => return Ok((owner != self_id).then_some(owner)),
            None => ShuffleRoutingError::VnodeOutOfRange {
                vnode: 0,
                vnode_count: 0,
            },
        };
        Err(crate::operator::shuffle_routing_error(
            &format!("aggregate [{}] routing", self.op_name),
            &error,
        ))
    }

    fn route_rows(
        &self,
        aggregate: &IncrementalAggState,
        batches: Vec<RecordBatch>,
        config: &ClusterShuffleConfig,
        assignment: &VnodeAssignmentSnapshot,
    ) -> Result<RoutedRows, DbError> {
        let mut local_batches = Vec::new();
        let mut remote_data = BTreeMap::<u64, Vec<ShuffleMessage>>::new();
        for batch in batches {
            let row_vnodes = hash_rows_to_vnodes(
                &batch,
                aggregate.num_group_cols(),
                u32::from(self.key_group_count),
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!("aggregate [{}] routing", self.op_name),
                    &error,
                )
            })?;
            if self.input_route == AggregateInputRoute::Colocated {
                let owners = assignment.owners();
                if row_vnodes
                    .iter()
                    .all(|&vnode| owners.get(vnode as usize) == Some(&config.self_id))
                {
                    // The aggregate derives each group's vnode from the same key encoding.
                    local_batches.push((batch, None));
                    continue;
                }
                if let Some(metrics) = &self.prom {
                    metrics.colocated_routing_fallbacks_total.inc();
                }
            }
            let plan = laminar_core::shuffle::route_checkpointed_batch(
                &batch,
                &row_vnodes,
                assignment,
                config.self_id,
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!("aggregate [{}] routing", self.op_name),
                    &error,
                )
            })?;
            local_batches.extend(
                plan.local
                    .into_iter()
                    .map(|route| (route.batch, Some(route.vnode))),
            );
            for route in plan.remote {
                remote_data.entry(route.owner.0).or_default().push(
                    ShuffleMessage::checkpointed_routed(
                        self.op_name.to_string(),
                        route.routed_vnodes,
                        route.batch,
                    ),
                );
            }
        }
        Ok((local_batches, remote_data))
    }

    /// The schema every peer's shuffle batch carries: pre-aggregate rows, or for a global
    /// aggregate the partial rows each node folds from its own.
    pub(super) fn expected_shuffle_schema(&self) -> Result<SchemaRef, DbError> {
        let QueryState::Agg(aggregate) = &self.state else {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' received shuffle data before initialization",
                self.op_name
            )));
        };
        if aggregate.num_group_cols() == 0 {
            return aggregate.global_partial_schema();
        }
        if let Some(projection) = aggregate.compiled_projection() {
            return Ok(Arc::clone(&projection.output_schema));
        }
        aggregate.cached_pre_agg_physical().map_or_else(
            || {
                Err(DbError::Pipeline(format!(
                    "aggregate '{}' has no pre-aggregate schema",
                    self.op_name
                )))
            },
            |physical| Ok(physical.schema()),
        )
    }

    /// Apply batches a peer shuffled here. A global aggregate's peers send partials.
    pub(super) fn apply_remote_aggregate(
        &mut self,
        batches: &[(RecordBatch, Option<u32>)],
        watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.invalidate_local_aggregate_output_cache();
        match &mut self.state {
            QueryState::Agg(aggregate) if aggregate.num_group_cols() == 0 => {
                for (batch, _) in batches {
                    aggregate.merge_global_partials(batch, watermark)?;
                }
            }
            _ => return self.apply_routed_aggregate(batches, watermark),
        }
        self.emit_agg_output()
    }
}

/// Rows applied here with their uniform vnode, and messages per remote owner.
type RoutedRows = (
    Vec<(RecordBatch, Option<u32>)>,
    BTreeMap<u64, Vec<ShuffleMessage>>,
);

pub(crate) fn hash_rows_to_vnodes(
    batch: &RecordBatch,
    num_group_cols: usize,
    vnode_count: u32,
) -> Result<Vec<u32>, laminar_core::shuffle::ShuffleRoutingError> {
    if num_group_cols == 0 || batch.num_rows() == 0 {
        return Ok(vec![0; batch.num_rows()]);
    }
    let columns: Vec<usize> = (0..num_group_cols).collect();
    laminar_core::shuffle::row_vnodes(batch, &columns, vnode_count)
}
