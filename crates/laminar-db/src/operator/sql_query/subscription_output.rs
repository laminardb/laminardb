use std::sync::Arc;

use arrow::array::RecordBatch;
use laminar_core::checkpoint::{
    ChangelogMode, OutputDistribution, OutputDistributionCertificate, OutputPartitionId,
};
use laminar_core::state::VnodeAssignmentSnapshot;

use super::{ClusterShuffleConfig, QueryState, SqlQueryOperator};
use crate::aggregate_state::IncrementalAggState;
use crate::error::DbError;
use crate::operator::capability::{ManagedStateContract, OperatorStateClass};
use crate::operator_graph::GraphExecutionMode;
use crate::subscription::{CertifiedSubscriptionFrontiers, PreparedSubscriptionOutput};

impl SqlQueryOperator {
    pub(crate) fn attach_subscription_certificate(
        &mut self,
        certificate: Arc<OutputDistributionCertificate>,
    ) -> Result<(), DbError> {
        certificate
            .validate(self.key_group_count)
            .map_err(|error| {
                DbError::InvalidOperation(format!(
                    "aggregate '{}' has an invalid subscription certificate: {error}",
                    self.op_name
                ))
            })?;
        if certificate.stream_id != self.op_name.as_ref()
            || certificate.final_operator_id != format!("stream:{}", self.op_name)
        {
            return Err(DbError::InvalidOperation(format!(
                "aggregate '{}' subscription certificate names another final operator",
                self.op_name
            )));
        }
        if self.subscription_certificate.replace(certificate).is_some() {
            return Err(DbError::InvalidOperation(format!(
                "aggregate '{}' received more than one subscription certificate",
                self.op_name
            )));
        }
        Ok(())
    }

    pub(super) fn validate_cluster_aggregate(
        &self,
        aggregate: &IncrementalAggState,
    ) -> Result<(), DbError> {
        if let Some(certificate) = self.subscription_certificate.as_ref() {
            let distribution_matches = match certificate.distribution {
                OutputDistribution::VnodePartitioned { .. } => aggregate.num_group_cols() != 0,
                OutputDistribution::Singleton { partition } => {
                    aggregate.num_group_cols() == 0 && partition.get() == 0
                }
            };
            let changelog_matches = match certificate.changelog_mode {
                ChangelogMode::WeightedRetractInsert => self.emit_changelog,
                ChangelogMode::FullPartitionSnapshot => !self.emit_changelog,
                ChangelogMode::Append => false,
            };
            if !distribution_matches || !changelog_matches {
                return Err(DbError::InvalidOperation(format!(
                    "aggregate '{}' execution does not match its subscription output-distribution certificate",
                    self.op_name
                )));
            }
        }
        if self.cluster_shuffle.is_none() {
            return Ok(());
        }
        let expected_state_class = if aggregate.num_group_cols() == 0 {
            OperatorStateClass::GlobalSingleton
        } else {
            OperatorStateClass::VnodeKeyed
        };
        if self.capability.managed_state == Some(ManagedStateContract::SqlAggregateV1)
            && self.capability.state_class == expected_state_class
        {
            return Ok(());
        }
        Err(DbError::Pipeline(format!(
            "[{}] query '{}': initialized aggregate state does not match its immutable cluster capability ({:?}, {:?})",
            laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
            self.op_name,
            self.capability.state_class,
            self.capability.managed_state
        )))
    }

    pub(super) fn subscription_checkpoint_scope(
        &self,
    ) -> Result<(ClusterShuffleConfig, VnodeAssignmentSnapshot, Arc<[u64]>), DbError> {
        self.require_settled_subscription_output("capture a checkpoint")?;
        self.active_cluster_scope()
    }

    pub(super) fn capture_certified_subscription_frontiers(
        &self,
    ) -> Result<Option<CertifiedSubscriptionFrontiers>, DbError> {
        let Some(certificate) = self.subscription_certificate.as_ref() else {
            return Ok(None);
        };
        let QueryState::Agg(aggregate) = &self.state else {
            return Err(DbError::Checkpoint(format!(
                "certified subscription aggregate '{}' is not initialized",
                self.op_name
            )));
        };
        let (config, assignment, _) = self.subscription_checkpoint_scope()?;
        let vnodes = assignment
            .owners()
            .iter()
            .enumerate()
            .filter_map(|(vnode, owner)| {
                let vnode = u32::try_from(vnode).ok()?;
                let partition = u16::try_from(vnode).ok().map(OutputPartitionId::new)?;
                (*owner == config.self_id && certificate.distribution.contains(partition))
                    .then_some(vnode)
            });
        Ok(Some(CertifiedSubscriptionFrontiers {
            certificate: Arc::clone(certificate),
            frontiers: aggregate.output_frontiers(vnodes)?,
        }))
    }

    pub(super) fn require_vnode_state_boundary(&self, context: &str) -> Result<(), DbError> {
        self.require_settled_subscription_output(context)?;
        if self.pending_cluster_input.is_some() || self.last_broadcast != self.local_frontier {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot {context} across pending shuffle work",
                self.op_name
            )));
        }
        Ok(())
    }

    /// An owed snapshot does not hold a transition back: it stays owed and is emitted for the vnodes
    /// owned after it. A revoked vnode's unemitted rows were never committed output; its new owner
    /// restores the committed frame and replays.
    pub(super) fn begin_subscription_vnode_transition(&mut self) -> Result<(), DbError> {
        self.require_no_prepared_subscription_output("prepare a vnode transition")?;
        self.invalidate_local_aggregate_output_cache();
        Ok(())
    }

    pub(super) fn prepare_certified_subscription_output(
        &mut self,
    ) -> Option<Result<Vec<RecordBatch>, DbError>> {
        let stream_generation = self.subscription_certificate.as_ref()?.stream_generation;
        Some((|| {
            if self.prepared_aggregate_emission.is_some() {
                return Err(DbError::StatefulOperatorPartialApply(format!(
                    "aggregate '{}' attempted another output before the prior publication completed",
                    self.op_name
                )));
            }
            let QueryState::Agg(aggregate) = &mut self.state else {
                return Err(DbError::Pipeline(
                    "internal: emit_agg_output on non-agg".into(),
                ));
            };
            let prepared = aggregate.prepare_partitioned_emit(stream_generation)?;
            let batches = super::coalesce_aggregate_batches(
                &self.op_name,
                prepared.result_batches(),
                super::AggregateBatchCoalescing::PublishedOutput,
            )?;
            self.prepared_aggregate_emission = Some(prepared);
            self.drained_snapshot = super::DrainedSnapshot::Clear;
            Ok(batches)
        })())
    }

    pub(super) fn take_prepared_subscription_output_frames(
        &mut self,
    ) -> Option<PreparedSubscriptionOutput> {
        let prepared = self.prepared_aggregate_emission.as_mut()?;
        let certificate = Arc::clone(
            self.subscription_certificate
                .as_ref()
                .expect("prepared subscription output must retain its certificate"),
        );
        Some(PreparedSubscriptionOutput {
            certificate,
            frames: prepared.take_frames(),
        })
    }

    pub(super) fn publish_prepared_subscription_output(&mut self) {
        let Some(prepared) = self.prepared_aggregate_emission.take() else {
            return;
        };
        let QueryState::Agg(aggregate) = &mut self.state else {
            panic!("prepared aggregate output lost its aggregate state before publication");
        };
        aggregate.commit_partitioned_emit(prepared);
    }

    pub(super) fn discard_prepared_subscription_output(&mut self) {
        let Some(prepared) = self.prepared_aggregate_emission.take() else {
            return;
        };
        let QueryState::Agg(aggregate) = &mut self.state else {
            panic!("prepared aggregate output lost its aggregate state before abort");
        };
        aggregate.abort_partitioned_emit(prepared);
    }

    pub(super) fn prepared_subscription_output_bytes(&self) -> usize {
        self.prepared_aggregate_emission.as_ref().map_or(
            0,
            crate::aggregate_state::PreparedAggregateEmission::retained_bookkeeping_bytes,
        )
    }

    /// In a checkpoint drain a certified snapshot aggregate applies its queued peer events without
    /// emitting: each snapshot replaces the whole partition, and every snapshot a drain emits is
    /// staged against the bounded subscription output before the cut.
    pub(super) fn defers_drained_snapshots(&self) -> bool {
        self.execution_mode != GraphExecutionMode::Normal
            && !self.emit_changelog
            && self.subscription_certificate.is_some()
    }

    /// The drain's settle pass emits an owed snapshot, as does a later normal pass once the queue
    /// is empty; a drain pass keeps applying instead.
    pub(super) fn owed_snapshot_is_due(&self, has_input: bool) -> bool {
        self.drained_snapshot == super::DrainedSnapshot::Owed
            && !has_input
            && match self.execution_mode {
                GraphExecutionMode::CheckpointSettle => true,
                GraphExecutionMode::Normal => self.queued_remote_events == 0,
                GraphExecutionMode::CheckpointDrain => false,
            }
    }

    /// Pending sends, queued peer events and an owed snapshot all mean the output frontier has
    /// rows still to emit, so it cannot be idle.
    pub(super) fn has_unemitted_cluster_work(&self) -> bool {
        self.pending_cluster_input.is_some()
            || self.queued_remote_events != 0
            || self.drained_snapshot == super::DrainedSnapshot::Owed
    }

    fn require_no_prepared_subscription_output(&self, context: &str) -> Result<(), DbError> {
        if self.prepared_aggregate_emission.is_some() {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot {context} while subscription output publication is pending",
                self.op_name
            )));
        }
        Ok(())
    }

    /// Captured state must not run ahead of the output emitted for it.
    fn require_settled_subscription_output(&self, context: &str) -> Result<(), DbError> {
        self.require_no_prepared_subscription_output(context)?;
        if self.drained_snapshot == super::DrainedSnapshot::Owed {
            return Err(DbError::Checkpoint(format!(
                "aggregate '{}' cannot {context} while a drained snapshot is still owed",
                self.op_name
            )));
        }
        Ok(())
    }
}
