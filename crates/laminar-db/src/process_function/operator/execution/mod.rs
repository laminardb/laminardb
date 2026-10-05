//! Live process execution under a pinned, single-owner cluster assignment.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray};
use laminar_core::checkpoint::CheckpointAssignmentFence;
use laminar_core::cluster::control::LeaseDeadline;
use laminar_core::shuffle::route_checkpointed_batch;
use laminar_core::state::{PartitionKeyCodecV1, VnodeAssignmentSnapshot};

use super::ProcessFunctionOperator;
use crate::error::DbError;
use crate::operator::sql_query::ClusterShuffleConfig;

pub(super) enum ProcessExecution {
    Local,
    AwaitingAssignment,
    SingleOwner(ProcessExecutionAuthority),
}

pub(super) struct ProcessExecutionAuthority {
    config: ClusterShuffleConfig,
    assignment: VnodeAssignmentSnapshot,
    deadline: Arc<LeaseDeadline>,
    recovery_generation: u64,
}

impl ProcessExecutionAuthority {
    pub(super) fn bind(
        config: &ClusterShuffleConfig,
        fence: &CheckpointAssignmentFence,
        deadline: Arc<LeaseDeadline>,
    ) -> Result<Self, DbError> {
        let assignment = config.topology_snapshot()?;
        let owners = assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        if !fence.is_canonical()
            || assignment.version() != fence.assignment_version
            || !fence.matches_owner_map(&owners)
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || fence.participant_incarnation(config.self_id.0) != Some(config.sender.incarnation())
            || config.receiver.incarnation() != config.sender.incarnation()
            || config.sender.recovery_gen() != config.receiver.recovery_gen()
        {
            return Err(DbError::Checkpoint(
                "process execution authority differs from its installed assignment or incarnation"
                    .into(),
            ));
        }
        if assignment
            .owners()
            .iter()
            .any(|owner| *owner != config.self_id)
        {
            return Err(DbError::Unsupported(
                "multi-owner process input requires ordered cross-node shuffle qualification"
                    .into(),
            ));
        }
        match (
            config.sender.assignment_version(),
            config.receiver.assignment_version(),
            config.sender.active_assignment_digest(),
            config.receiver.active_assignment_digest(),
        ) {
            (0, 0, None, None) => {}
            (sender_version, receiver_version, Some(sender), Some(receiver))
                if sender_version == fence.assignment_version
                    && receiver_version == fence.assignment_version
                    && sender == fence.digest()
                    && receiver == sender => {}
            _ => {
                return Err(DbError::Checkpoint(
                    "process execution transport differs from its startup assignment".into(),
                ))
            }
        }
        config
            .sender
            .bind_process_lease_deadline_pair(&config.receiver, Arc::clone(&deadline))
            .map_err(|error| DbError::Checkpoint(format!("process execution lease: {error}")))?;
        Ok(Self {
            config: config.clone(),
            assignment,
            deadline,
            recovery_generation: config.sender.recovery_gen(),
        })
    }

    fn require_current(&self) -> Result<(), DbError> {
        // INVARIANT: certificates cannot change within an assignment version. Control publishes
        // newer registry/transport versions behind the graph rotation fence. These are atomic
        // checks; the record path never takes the registry or transport certificate locks.
        let version = self.assignment.version();
        if !self.deadline.is_live()
            || self.config.registry.assignment_version() != version
            || self.config.sender.assignment_version() != version
            || self.config.receiver.assignment_version() != version
            || self.config.sender.recovery_gen() != self.recovery_generation
            || self.config.receiver.recovery_gen() != self.recovery_generation
            || self.config.ensure_topology_current().is_err()
        {
            return Err(DbError::StatefulOperatorPartialApply("process execution lost its lease, assignment, topology, or recovery generation; recover the graph".into()));
        }
        Ok(())
    }
}

impl ProcessFunctionOperator {
    pub(crate) fn require_cluster_execution(&mut self) -> Result<(), DbError> {
        if !matches!(self.execution, ProcessExecution::Local) || self.next_activation_id != 0 {
            return Err(DbError::Checkpoint(
                "cluster process execution must be selected before input".into(),
            ));
        }
        self.execution = ProcessExecution::AwaitingAssignment;
        Ok(())
    }

    pub(super) fn require_execution_current(&self) -> Result<(), DbError> {
        match &self.execution {
            ProcessExecution::Local => Ok(()),
            ProcessExecution::AwaitingAssignment => Err(DbError::ShuffleNotReady(
                "process execution has no startup authority binding".into(),
            )),
            ProcessExecution::SingleOwner(authority) => {
                if self
                    .assignment_fence
                    .as_ref()
                    .map(|fence| fence.assignment_version)
                    != Some(authority.assignment.version())
                {
                    return Err(DbError::StatefulOperatorPartialApply(
                        "process state and execution assignment differ; recover the graph".into(),
                    ));
                }
                authority.require_current()
            }
        }
    }

    #[cfg(feature = "process-remote")]
    pub(super) fn execution_generations(&self) -> Result<(u64, u64), DbError> {
        self.require_execution_current()?;
        match &self.execution {
            ProcessExecution::SingleOwner(authority) => Ok((
                authority.assignment.version(),
                authority.recovery_generation,
            )),
            ProcessExecution::Local => Ok((0, 0)),
            ProcessExecution::AwaitingAssignment => unreachable!("unbound execution was rejected"),
        }
    }

    pub(super) fn route_owned_input(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<Option<Vec<RecordBatch>>, DbError> {
        self.require_execution_current()?;
        let ProcessExecution::SingleOwner(authority) = &self.execution else {
            return Ok(None);
        };
        let input_bytes = self.validate_routed_input(inputs)?;
        let mut routed = Vec::new();
        let mut routed_bytes = 0usize;
        for batch in inputs.iter().flatten() {
            let columns = self
                .key_indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect::<Vec<_>>();
            let keys = self.key_codec.encode_columns(&columns).map_err(|error| {
                DbError::InvalidOperation(format!("process function key encoding: {error}"))
            })?;
            let vnodes = keys
                .iter()
                .map(|key| PartitionKeyCodecV1::vnode_for_encoded(key.data(), self.vnode_count))
                .collect::<Vec<_>>();
            let routing_scratch = keys
                .size()
                .checked_add(
                    vnodes
                        .capacity()
                        .checked_mul(std::mem::size_of::<u32>())
                        .ok_or_else(|| {
                            DbError::BackpressureFail(
                                "process routing scratch accounting overflow".into(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    DbError::BackpressureFail("process routing scratch accounting overflow".into())
                })?;
            let temporary_limit = self
                .graph_budget
                .checked_sub(self.live_bytes)
                .and_then(|bytes| bytes.checked_sub(input_bytes))
                .and_then(|bytes| bytes.checked_sub(routing_scratch))
                .ok_or_else(|| {
                    DbError::BackpressureFail(
                        "process routing scratch exceeds its temporary state budget".into(),
                    )
                })?;
            let plan = route_checkpointed_batch(
                batch,
                &vnodes,
                &authority.assignment,
                authority.config.self_id,
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error("process input routing", &error)
            })?;
            if !plan.remote.is_empty() {
                return Err(DbError::ShuffleTerminal(
                    "single-owner process input routed outside local ownership".into(),
                ));
            }
            for route in plan.local {
                routed_bytes = routed_bytes
                    .checked_add(route.batch.get_array_memory_size())
                    .ok_or_else(|| {
                        DbError::BackpressureFail("process routed input byte count overflow".into())
                    })?;
                if routed_bytes > self.descriptor.limits.max_input_bytes
                    || routed_bytes > temporary_limit
                {
                    return Err(DbError::BackpressureFail(
                        "process input routing exceeds its temporary state budget".into(),
                    ));
                }
                routed.push(route.batch);
            }
        }
        self.require_execution_current()?;
        Ok(Some(routed))
    }

    fn validate_routed_input(&self, inputs: &[Vec<RecordBatch>]) -> Result<usize, DbError> {
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for batch in inputs.iter().flatten() {
            rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DbError::BackpressureFail("process input row count overflow".into())
            })?;
            bytes = bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| {
                    DbError::BackpressureFail("process input byte count overflow".into())
                })?;
            if rows > self.descriptor.limits.max_input_rows
                || bytes > self.descriptor.limits.max_input_bytes
            {
                return Err(DbError::BackpressureFail(
                    "process input budget exceeded".into(),
                ));
            }
            if batch.schema().as_ref() != self.descriptor.input_schema.as_ref() {
                return Err(DbError::InvalidOperation(
                    "process function input schema changed after registration".into(),
                ));
            }
            let time = batch
                .column(self.time_index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DbError::InvalidOperation("invalid event-time array".into()))?;
            for row in 0..batch.num_rows() {
                if self
                    .key_indices
                    .iter()
                    .any(|&index| batch.column(index).is_null(row))
                    || time.is_null(row)
                    || time.value(row) < self.watermark_us
                {
                    return Err(DbError::InvalidOperation(
                        "process function rejects null keys, null event time, or late input".into(),
                    ));
                }
            }
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests;
