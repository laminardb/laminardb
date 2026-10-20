//! Cluster routing of pre-aggregated input batches: local apply, remote shuffle, and the
//! frontier frames every peer receives.

use std::collections::BTreeMap;

use arrow::array::RecordBatch;
use laminar_core::shuffle::ShuffleMessage;
use laminar_core::state::VnodeAssignmentSnapshot;

use super::{AggClusterInputPlan, ClusterShuffleConfig, QueryState, SqlQueryOperator};
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
        let mut local_batches = Vec::new();
        let mut remote_data = BTreeMap::<u64, Vec<ShuffleMessage>>::new();
        for batch in batches.into_iter().filter(|batch| batch.num_rows() != 0) {
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
}

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
