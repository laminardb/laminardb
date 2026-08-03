//! Interval join operator for the `OperatorGraph`.
//!
//! Buffers left/right rows across cycles for
//! `right_ts BETWEEN left_ts AND left_ts + time_bound`; evicts on watermark advance.

use std::sync::Arc;

use std::collections::BTreeMap;
#[cfg(feature = "cluster")]
use std::collections::VecDeque;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_core::state::{KeyGroupCount, VnodeAssignmentSnapshot, VnodeRegistry, LOCAL_NODE_ID};
use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::interval_join::{
    execute_interval_join_cycle, join_type_tag, IntervalJoinOutputBudget, IntervalJoinState,
    JoinStateCheckpoint,
};
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, ManagedStateAccountingSnapshot, OperatorCheckpoint};

#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster")]
use crate::operator_graph::ManagedVnodeTransition;

#[derive(Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum JoinInputSide {
    Left,
    Right,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalJoinOperatorCheckpoint {
    join_type: u8,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    left_time_column: String,
    right_time_column: String,
    left_table: String,
    right_table: String,
    bound_ms: i64,
    shards: Vec<(u32, JoinStateCheckpoint)>,
    aligned_replay: Vec<(u64, JoinInputSide, i64, Vec<u8>)>,
    applied_left_watermark: i64,
    applied_right_watermark: i64,
}

#[cfg(feature = "cluster")]
struct PreparedIntervalJoinTransition {
    replacements: Vec<(u32, Option<Box<IntervalJoinState>>)>,
}

#[cfg(feature = "cluster")]
enum IntervalJoinTransitionCleanup {
    Aborted(PreparedIntervalJoinTransition),
    Published(PreparedIntervalJoinTransition),
}

pub(crate) struct IntervalJoinOperator {
    config: StreamJoinConfig,
    key_group_count: KeyGroupCount,
    local_assignment: VnodeAssignmentSnapshot,
    vnode_states: Vec<Option<Box<IntervalJoinState>>>,
    max_managed_state_bytes: usize,
    input_schemas: Option<(SchemaRef, SchemaRef)>,
    projection: ProjectingJoinState,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    aligned_replay: VecDeque<(u64, JoinInputSide, i64, crate::operator::RetainedBatch)>,
    #[cfg(feature = "cluster")]
    prepared_vnode_transition: Option<PreparedIntervalJoinTransition>,
    #[cfg(feature = "cluster")]
    vnode_transition_cleanup: Option<IntervalJoinTransitionCleanup>,
    applied_left_watermark: i64,
    applied_right_watermark: i64,
}

impl IntervalJoinOperator {
    #[cfg(test)]
    pub(crate) fn new(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self::new_with_key_groups(
            name,
            config,
            projection_sql,
            ctx,
            KeyGroupCount::try_from(1_u16).expect("one test key group is valid"),
        )
    }

    pub(crate) fn new_with_key_groups(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        key_group_count: KeyGroupCount,
    ) -> Self {
        let vnode_count = u32::from(key_group_count);
        let local_assignment =
            VnodeRegistry::single_owner(vnode_count, LOCAL_NODE_ID).versioned_snapshot();
        Self {
            config,
            key_group_count,
            local_assignment,
            vnode_states: std::iter::repeat_with(|| None)
                .take(usize::from(key_group_count.get()))
                .collect(),
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            input_schemas: None,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__interval_tmp"),
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            aligned_replay: VecDeque::new(),
            #[cfg(feature = "cluster")]
            prepared_vnode_transition: None,
            #[cfg(feature = "cluster")]
            vnode_transition_cleanup: None,
            applied_left_watermark: i64::MIN,
            applied_right_watermark: i64::MIN,
        }
    }

    pub(crate) fn set_input_schemas(&mut self, left: SchemaRef, right: SchemaRef) {
        debug_assert!(self.vnode_states.iter().all(Option::is_none));
        self.input_schemas = Some((left, right));
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        debug_assert!(self.vnode_states.iter().all(Option::is_none));
        self.key_group_count = KeyGroupCount::try_from(config.registry.vnode_count())
            .expect("vnode registry count must fit the checkpoint key-group ABI");
        self.vnode_states
            .resize_with(config.registry.vnode_count() as usize, || None);
        self.cluster_shuffle = Some(config);
    }

    fn accounted_state_bytes(&self) -> usize {
        self.vnode_states
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<Box<IntervalJoinState>>>())
            .saturating_add(32)
            .saturating_add(
                self.vnode_states
                    .iter()
                    .flatten()
                    .fold(0usize, |total, state| {
                        total.saturating_add(state.accounted_state_bytes())
                    }),
            )
    }

    #[cfg(feature = "cluster")]
    fn transition_accounted_bytes(transition: &PreparedIntervalJoinTransition) -> usize {
        transition
            .replacements
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, Option<Box<IntervalJoinState>>)>())
            .saturating_add(32)
            .saturating_add(
                transition
                    .replacements
                    .iter()
                    .filter_map(|(_, state)| state.as_ref())
                    .fold(0usize, |total, state| {
                        total.saturating_add(state.accounted_state_bytes())
                    }),
            )
    }

    #[cfg(feature = "cluster")]
    fn serialize_state(
        state: &mut IntervalJoinState,
        config: &StreamJoinConfig,
        context: &str,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, DbError> {
        let checkpoint = state.snapshot_checkpoint(config, max_encoded_bytes)?;
        // Live state, IPC staging, and the final archive are independently bounded by M. They
        // coexist only on this cold capture path, so its explicit peak envelope is at most 3M.
        debug_assert!(checkpoint.retained_ipc_bytes()? <= max_encoded_bytes);
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(max_encoded_bytes),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: archive serialization exceeded its {max_encoded_bytes}-byte budget: {error}"
                ))
            })
    }

    #[cfg(feature = "cluster")]
    fn deserialize_state(
        bytes: &[u8],
        config: &StreamJoinConfig,
        context: &str,
        max_state_bytes: usize,
    ) -> Result<IntervalJoinState, DbError> {
        let checkpoint = rkyv::from_bytes::<JoinStateCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))?;
        IntervalJoinState::from_checkpoint(&checkpoint, config, max_state_bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))
    }

    fn execute_shard_cycle(
        &mut self,
        vnode: u32,
        left: &[RecordBatch],
        right: &[RecordBatch],
        left_watermark: i64,
        right_watermark: i64,
        accounted_total: &mut usize,
        output_budget: &mut IntervalJoinOutputBudget,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_admission_watermark = self.applied_left_watermark;
        let right_admission_watermark = self.applied_right_watermark;
        let state_slots = self.vnode_states.len();
        let shard_bytes = self
            .vnode_states
            .get(vnode as usize)
            .and_then(Option::as_ref)
            .map_or(0, |state| state.accounted_state_bytes());
        let other_state_bytes = (*accounted_total).checked_sub(shard_bytes).ok_or_else(|| {
            DbError::BackpressureFail(format!(
                "interval join [{}] retained-state accounting underflow",
                self.projection.op_name
            ))
        })?;
        let shard_limit = self
            .max_managed_state_bytes
            .checked_sub(other_state_bytes)
            .ok_or_else(|| {
                DbError::BackpressureFail(format!(
                    "interval join [{}] already exceeds its {}-byte retained-state limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        let has_input = left.iter().any(|batch| batch.num_rows() > 0)
            || right.iter().any(|batch| batch.num_rows() > 0);
        let slot = self.vnode_states.get_mut(vnode as usize).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] routed vnode {vnode} outside its {}-vnode state table",
                self.projection.op_name, state_slots
            ))
        })?;
        if slot.is_none() && !has_input {
            return Ok(Vec::new());
        }
        if slot.is_none() && IntervalJoinState::new().accounted_state_bytes() > shard_limit {
            return Err(DbError::BackpressureFail(format!(
                "interval join [{}] cannot allocate vnode {vnode} state within its {shard_limit}-byte remaining limit",
                self.projection.op_name
            )));
        }
        if slot.is_none() {
            let mut state = IntervalJoinState::new();
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            *slot = Some(Box::new(state));
        }
        let state = slot.as_mut().expect("interval join state initialized");
        let result = execute_interval_join_cycle(
            state,
            left,
            right,
            &self.config,
            left_admission_watermark,
            right_admission_watermark,
            left_watermark,
            right_watermark,
            shard_limit,
            output_budget,
        );
        *accounted_total = other_state_bytes.saturating_add(state.accounted_state_bytes());
        result
    }

    fn execute_routed_shards(
        &mut self,
        routed: BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        left_watermark: i64,
        right_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut accounted_total = self.accounted_state_bytes();
        let mut output_budget = IntervalJoinOutputBudget::default();
        let mut output = Vec::new();
        let mut prior_shard_completed = false;
        for (vnode, [left, right]) in routed {
            match self.execute_shard_cycle(
                vnode,
                &left,
                &right,
                left_watermark,
                right_watermark,
                &mut accounted_total,
                &mut output_budget,
            ) {
                Ok(shard_output) => {
                    output.extend(shard_output);
                    prior_shard_completed = true;
                }
                Err(error) if prior_shard_completed => {
                    let error = if error.requires_pipeline_recovery()
                        || error.requires_pipeline_halt()
                    {
                        error
                    } else {
                        DbError::StatefulOperatorPartialApply(format!(
                            "interval join [{}] admitted an earlier vnode before vnode {vnode} failed: {error}",
                            self.projection.op_name
                        ))
                    };
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(output)
    }

    async fn project_output(
        &mut self,
        join_result: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.projection.apply(join_result).await.map_err(|error| {
            if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
                error
            } else {
                DbError::StatefulOperatorPartialApply(format!(
                    "interval join [{}] admitted input before post-projection failed: {error}",
                    self.projection.op_name
                ))
            }
        })
    }

    fn push_routed_batch(
        routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        vnode: u32,
        side: JoinInputSide,
        batch: RecordBatch,
    ) {
        let port = match side {
            JoinInputSide::Left => 0,
            JoinInputSide::Right => 1,
        };
        routed.entry(vnode).or_default()[port].push(batch);
    }

    fn add_resident_vnodes(&self, routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>) {
        for (vnode, state) in self.vnode_states.iter().enumerate() {
            if state.is_some() {
                routed.entry(vnode as u32).or_default();
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn post_shuffle_admission_error(&self, outbound_admitted: bool, error: DbError) -> DbError {
        if !outbound_admitted || error.requires_pipeline_recovery() {
            return error;
        }
        DbError::ShufflePartialSend(format!(
            "interval join [{}] failed after outbound shuffle data was admitted: {error}",
            self.projection.op_name
        ))
    }

    fn prevalidate_inputs(&self, inputs: &[Vec<RecordBatch>]) -> Result<(), DbError> {
        if self.config.left_keys.is_empty()
            || self.config.left_keys.len() != self.config.right_keys.len()
            || self.config.left_time_column.is_empty()
            || self.config.right_time_column.is_empty()
            || self.config.time_bound.is_zero()
            || i64::try_from(self.config.time_bound.as_millis()).is_err()
        {
            return Err(DbError::InvalidOperation(
                "interval join requires equal non-empty ordered key vectors, both event-time columns, and a positive finite time bound"
                    .into(),
            ));
        }

        for (port, side, key_names, time_name) in [
            (
                0,
                "left",
                self.config.left_keys.as_slice(),
                self.config.left_time_column.as_str(),
            ),
            (
                1,
                "right",
                self.config.right_keys.as_slice(),
                self.config.right_time_column.as_str(),
            ),
        ] {
            let expected_schema =
                self.input_schemas.as_ref().map(
                    |schemas| {
                        if port == 0 {
                            &schemas.0
                        } else {
                            &schemas.1
                        }
                    },
                );
            for (batch_index, batch) in inputs.get(port).into_iter().flatten().enumerate() {
                if let Some(expected) = expected_schema {
                    if batch.schema().as_ref() != expected.as_ref() {
                        return Err(DbError::SchemaMismatch(format!(
                            "interval join [{0}] {side} batch {batch_index} does not match its declared schema",
                            self.projection.op_name
                        )));
                    }
                }
                for key_name in key_names {
                    let index = batch.schema().index_of(key_name).map_err(|error| {
                        DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} key '{key_name}': {error}",
                            self.projection.op_name
                        ))
                    })?;
                    if !matches!(
                        batch.column(index).data_type(),
                        DataType::Utf8 | DataType::Int64
                    ) {
                        return Err(DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} key '{key_name}' must be Utf8 or Int64, found {}",
                            self.projection.op_name,
                            batch.column(index).data_type()
                        )));
                    }
                }
                let time_index = batch.schema().index_of(time_name).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join [{}] {side} event-time column '{time_name}': {error}",
                        self.projection.op_name
                    ))
                })?;
                let time_column = batch.column(time_index);
                if !matches!(time_column.data_type(), DataType::Timestamp(_, _))
                    || time_column.null_count() != 0
                {
                    return Err(DbError::SchemaMismatch(format!(
                        "interval join [{}] {side} event-time column '{time_name}' must be a non-null Timestamp, found {} with {} nulls",
                        self.projection.op_name,
                        time_column.data_type(),
                        time_column.null_count()
                    )));
                }
            }
        }
        Ok(())
    }

    fn route_local_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<BTreeMap<u32, [Vec<RecordBatch>; 2]>, DbError> {
        self.prevalidate_inputs(inputs)?;
        let mut routed = BTreeMap::new();
        let vnode_count = u32::from(self.key_group_count);

        for (side, batches) in [
            (
                JoinInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                JoinInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            let (side_name, key_names) = match side {
                JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
                JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
            };
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let key_indices = key_names
                    .iter()
                    .map(|key_name| {
                        batch.schema().index_of(key_name).map_err(|error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side_name} routing key '{key_name}': {error}",
                                self.projection.op_name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_vnodes =
                    laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count).map_err(
                        |error| {
                            DbError::Pipeline(format!(
                                "interval join [{}] {side_name} routing: {error}",
                                self.projection.op_name
                            ))
                        },
                    )?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &self.local_assignment,
                    LOCAL_NODE_ID,
                )
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "interval join [{}] {side_name} routing: {error}",
                        self.projection.op_name
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(DbError::Pipeline(format!(
                        "interval join [{}] local topology routed rows off-node",
                        self.projection.op_name
                    )));
                }
                for route in plan.local {
                    Self::push_routed_batch(&mut routed, route.vnode, side, route.batch);
                }
            }
        }
        Ok(routed)
    }

    #[cfg(feature = "cluster")]
    fn route_owned_batch(
        &self,
        config: &ClusterShuffleConfig,
        assignment: &laminar_core::state::VnodeAssignmentSnapshot,
        side: JoinInputSide,
        batch: &RecordBatch,
        declared_vnodes: Option<&[u32]>,
        routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>,
    ) -> Result<(), DbError> {
        let (side_name, key_names) = match side {
            JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
            JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
        };
        let key_indices = key_names
            .iter()
            .map(|key_name| {
                batch.schema().index_of(key_name).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join [{}] {side_name} routing key '{key_name}': {error}",
                        self.projection.op_name
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_vnodes =
            laminar_core::shuffle::row_vnodes(batch, &key_indices, config.registry.vnode_count())
                .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!(
                        "interval join [{}] {side_name} routing",
                        self.projection.op_name
                    ),
                    &error,
                )
            })?;
        let plan = laminar_core::shuffle::route_checkpointed_batch(
            batch,
            &row_vnodes,
            assignment,
            config.self_id,
        )
        .map_err(|error| {
            crate::operator::shuffle_routing_error(
                &format!(
                    "interval join [{}] {side_name} ownership validation",
                    self.projection.op_name
                ),
                &error,
            )
        })?;
        if !plan.remote.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received {side_name} shuffle data no longer owned by this node",
                self.projection.op_name
            )));
        }
        if let Some(declared) = declared_vnodes {
            let actual: Vec<u32> = plan.local.iter().map(|route| route.vnode).collect();
            if actual.as_slice() != declared {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] {side_name} shuffle vnode metadata {declared:?} does not match decoded rows {actual:?}",
                    self.projection.op_name
                )));
            }
        }
        for route in plan.local {
            Self::push_routed_batch(routed, route.vnode, side, route.batch);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn route_cluster_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<
        (
            BTreeMap<u32, [Vec<RecordBatch>; 2]>,
            Vec<laminar_core::shuffle::ReceivedBatch>,
            bool,
        ),
        DbError,
    > {
        self.prevalidate_inputs(inputs)?;
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has no cluster shuffle scope",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let mut routed = BTreeMap::new();
        let mut outbound = Vec::new();

        for (side, batches) in [
            (
                JoinInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                JoinInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            let (side_name, key_names) = match side {
                JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
                JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
            };
            let stage = format!("{}::{side_name}", self.projection.op_name);
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let key_indices = key_names
                    .iter()
                    .map(|key_name| {
                        batch.schema().index_of(key_name).map_err(|error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side_name} routing key '{key_name}': {error}",
                                self.projection.op_name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_vnodes = laminar_core::shuffle::row_vnodes(
                    batch,
                    &key_indices,
                    config.registry.vnode_count(),
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!(
                            "interval join [{}] {side_name} routing",
                            self.projection.op_name
                        ),
                        &error,
                    )
                })?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &assignment,
                    config.self_id,
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!(
                            "interval join [{}] {side_name} routing",
                            self.projection.op_name
                        ),
                        &error,
                    )
                })?;
                for route in plan.local {
                    Self::push_routed_batch(&mut routed, route.vnode, side, route.batch);
                }
                for route in plan.remote {
                    outbound.push((
                        route.owner.0,
                        laminar_core::shuffle::ShuffleMessage::checkpointed_routed(
                            stage.clone(),
                            route.routed_vnodes,
                            route.batch,
                        ),
                    ));
                }
            }
        }

        let outbound_admitted = !outbound.is_empty();
        crate::operator::send_shuffle_plan(
            &config.sender,
            assignment.version(),
            outbound,
            &format!("interval join [{}] shuffle", self.projection.op_name),
        )
        .await?;

        let mut admitted = Vec::new();
        for side in [JoinInputSide::Left, JoinInputSide::Right] {
            let side_name = match side {
                JoinInputSide::Left => "left",
                JoinInputSide::Right => "right",
            };
            let stage = format!("{}::{side_name}", self.projection.op_name);
            let received = config.receiver.drain_checkpointed_data_for(&stage);
            for batch in &received {
                self.route_owned_batch(
                    config,
                    &assignment,
                    side,
                    batch.batch(),
                    Some(batch.routed_vnodes()),
                    &mut routed,
                )
                .map_err(|error| self.post_shuffle_admission_error(outbound_admitted, error))?;
            }
            admitted.extend(received);
        }
        Ok((routed, admitted, outbound_admitted))
    }

    #[cfg(feature = "cluster")]
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        left_watermark: i64,
        right_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if !self.aligned_replay.is_empty() {
            return self.execute_aligned_replay().await;
        }
        let (mut routed, _admitted, outbound_admitted) = self.route_cluster_inputs(inputs).await?;
        let frontier_advanced = left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark;
        if frontier_advanced {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self
            .execute_routed_shards(routed, left_watermark, right_watermark)
            .map_err(|error| self.post_shuffle_admission_error(outbound_admitted, error))?;
        let output = self
            .project_output(output)
            .await
            .map_err(|error| self.post_shuffle_admission_error(outbound_admitted, error))?;
        self.applied_left_watermark = self.applied_left_watermark.max(left_watermark);
        self.applied_right_watermark = self.applied_right_watermark.max(right_watermark);
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    async fn execute_aligned_replay(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has aligned replay without cluster ownership",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        if config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
            || self
                .aligned_replay
                .iter()
                .any(|(version, _, _, _)| *version != assignment.version())
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] aligned replay crossed its assignment boundary",
                self.projection.op_name
            )));
        }
        let (_, side, watermark, batch) =
            self.aligned_replay.front().cloned().ok_or_else(|| {
                DbError::Checkpoint("interval join replay queue became empty".into())
            })?;
        let mut routed = BTreeMap::new();
        self.route_owned_batch(config, &assignment, side, batch.batch(), None, &mut routed)?;
        let (left_watermark, right_watermark) = match side {
            JoinInputSide::Left => (watermark, self.applied_right_watermark),
            JoinInputSide::Right => (self.applied_left_watermark, watermark),
        };
        if left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark
        {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self.execute_routed_shards(routed, left_watermark, right_watermark)?;
        let output = self.project_output(output).await.map_err(|error| {
            DbError::Checkpoint(format!(
                "interval join [{}] aligned replay requires recovery: {error}",
                self.projection.op_name
            ))
        })?;
        self.aligned_replay.pop_front().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] aligned replay disappeared after emission",
                self.projection.op_name
            ))
        })?;
        match side {
            JoinInputSide::Left => {
                self.applied_left_watermark = self.applied_left_watermark.max(watermark);
            }
            JoinInputSide::Right => {
                self.applied_right_watermark = self.applied_right_watermark.max(watermark);
            }
        }
        Ok(output)
    }
}

#[async_trait]
impl GraphOperator for IntervalJoinOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::bounded_interval_join()
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        #[cfg(feature = "cluster")]
        let (prepared, retired) = {
            let prepared = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, Self::transition_accounted_bytes);
            let (prepared, retired) = match self.vnode_transition_cleanup.as_ref() {
                Some(IntervalJoinTransitionCleanup::Aborted(transition)) => (
                    prepared.saturating_add(Self::transition_accounted_bytes(transition)),
                    0,
                ),
                Some(IntervalJoinTransitionCleanup::Published(transition)) => {
                    (prepared, Self::transition_accounted_bytes(transition))
                }
                None => (prepared, 0),
            };
            (prepared, retired)
        };
        #[cfg(not(feature = "cluster"))]
        let (prepared, retired) = (0, 0);

        Some(ManagedStateAccountingSnapshot {
            live: self.accounted_state_bytes(),
            prepared,
            retired,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let right_watermark = watermarks.get(1).copied().unwrap_or(left_watermark);

        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() {
            return self
                .process_cluster(inputs, left_watermark, right_watermark)
                .await;
        }

        let mut routed = self.route_local_inputs(inputs)?;
        let frontier_advanced = left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark;
        if frontier_advanced {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self.execute_routed_shards(routed, left_watermark, right_watermark)?;
        let output = self.project_output(output).await?;
        self.applied_left_watermark = self.applied_left_watermark.max(left_watermark);
        self.applied_right_watermark = self.applied_right_watermark.max(right_watermark);
        Ok(output)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        #[cfg(feature = "cluster")]
        let cluster_state = self.cluster_shuffle.is_some();
        #[cfg(not(feature = "cluster"))]
        let cluster_state = false;

        let mut shards = Vec::new();
        let mut retained_payload_bytes = 0usize;
        if !cluster_state {
            shards
                .try_reserve_exact(self.vnode_states.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] could not reserve checkpoint shard metadata",
                        self.projection.op_name
                    ))
                })?;
            for (vnode, state) in self.vnode_states.iter_mut().enumerate() {
                let Some(state) = state.as_mut() else {
                    continue;
                };
                let remaining = self
                    .max_managed_state_bytes
                    .checked_sub(retained_payload_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] checkpoint payload exhausted its {}-byte limit",
                            self.projection.op_name, self.max_managed_state_bytes
                        ))
                    })?;
                let checkpoint = state.snapshot_checkpoint(&self.config, remaining)?;
                retained_payload_bytes = retained_payload_bytes
                    .checked_add(checkpoint.retained_ipc_bytes()?)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] checkpoint payload accounting overflow",
                            self.projection.op_name
                        ))
                    })?;
                shards.push((vnode as u32, checkpoint));
            }
        }

        #[cfg(feature = "cluster")]
        let aligned_replay = {
            let mut encoded = Vec::new();
            encoded
                .try_reserve_exact(self.aligned_replay.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] could not reserve aligned replay metadata",
                        self.projection.op_name
                    ))
                })?;
            for (assignment, side, watermark, batch) in &self.aligned_replay {
                let remaining = self
                    .max_managed_state_bytes
                    .checked_sub(retained_payload_bytes)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] aligned replay exhausted its checkpoint limit",
                            self.projection.op_name
                        ))
                    })?;
                let bytes = laminar_core::serialization::serialize_batches_stream_bounded(
                    batch.batch().schema().as_ref(),
                    std::iter::once(batch.batch()),
                    remaining,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] aligned replay serialization within the cumulative checkpoint limit: {error}",
                        self.projection.op_name
                    ))
                })?;
                retained_payload_bytes = retained_payload_bytes
                    .checked_add(bytes.capacity())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] aligned replay byte accounting overflow",
                            self.projection.op_name
                        ))
                    })?;
                encoded.push((*assignment, *side, *watermark, bytes));
            }
            encoded
        };
        #[cfg(not(feature = "cluster"))]
        let aligned_replay = Vec::new();

        if shards.is_empty()
            && aligned_replay.is_empty()
            && self.applied_left_watermark == i64::MIN
            && self.applied_right_watermark == i64::MIN
        {
            return Ok(None);
        }
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        let checkpoint = IntervalJoinOperatorCheckpoint {
            join_type: join_type_tag(self.config.join_type),
            left_keys: self.config.left_keys.clone(),
            right_keys: self.config.right_keys.clone(),
            left_time_column: self.config.left_time_column.clone(),
            right_time_column: self.config.right_time_column.clone(),
            left_table: self.config.left_table.clone(),
            right_table: self.config.right_table.clone(),
            bound_ms,
            shards,
            aligned_replay,
            applied_left_watermark: self.applied_left_watermark,
            applied_right_watermark: self.applied_right_watermark,
        };
        // The retained IPC set is already <= M. Bound the final archive independently by M so a
        // valid image in (M/2, M] remains checkpointable; live + IPC + archive peak at <= 3M.
        debug_assert!(retained_payload_bytes <= self.max_managed_state_bytes);
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(self.max_managed_state_bytes),
        );
        let data = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "interval join [{}] archive serialization exceeded its {}-byte checkpoint limit: {error}",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        if self.vnode_states.iter().any(Option::is_some)
            || self.applied_left_watermark != i64::MIN
            || self.applied_right_watermark != i64::MIN
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint restore was applied more than once",
                self.projection.op_name
            )));
        }
        if checkpoint.data.len() > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint payload is {} bytes; restore limit is {} bytes",
                self.projection.op_name,
                checkpoint.data.len(),
                self.max_managed_state_bytes
            )));
        }
        let checkpoint = rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "interval join [{}] checkpoint deserialization: {error}",
                self.projection.op_name
            ))
        })?;
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        if checkpoint.join_type != join_type_tag(self.config.join_type)
            || checkpoint.left_keys != self.config.left_keys
            || checkpoint.right_keys != self.config.right_keys
            || checkpoint.left_time_column != self.config.left_time_column
            || checkpoint.right_time_column != self.config.right_time_column
            || checkpoint.left_table != self.config.left_table
            || checkpoint.right_table != self.config.right_table
            || checkpoint.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint configuration does not match the restored operator",
                self.projection.op_name
            )));
        }
        self.applied_left_watermark = checkpoint.applied_left_watermark;
        self.applied_right_watermark = checkpoint.applied_right_watermark;

        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() && !checkpoint.shards.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] cluster checkpoint contains whole-node join state",
                self.projection.op_name
            )));
        }
        #[cfg(not(feature = "cluster"))]
        if !checkpoint.aligned_replay.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint contains cluster shuffle replay",
                self.projection.op_name
            )));
        }

        let mut seen = vec![false; self.vnode_states.len()];
        for (vnode, _) in &checkpoint.shards {
            let slot = seen.get_mut(*vnode as usize).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint vnode {vnode} is outside its {}-vnode state table",
                    self.projection.op_name,
                    self.vnode_states.len()
                ))
            })?;
            if std::mem::replace(slot, true) {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint repeats vnode {vnode}",
                    self.projection.op_name
                )));
            }
        }

        for (vnode, state_checkpoint) in checkpoint.shards {
            let remaining = self
                .max_managed_state_bytes
                .checked_sub(self.accounted_state_bytes())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] restored state already exceeds its {}-byte limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?;
            let mut state =
                IntervalJoinState::from_checkpoint(&state_checkpoint, &self.config, remaining)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] vnode {vnode} restore: {error}",
                            self.projection.op_name
                        ))
                    })?;
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            self.vnode_states[vnode as usize] = Some(Box::new(state));
        }

        #[cfg(feature = "cluster")]
        {
            if !self.aligned_replay.is_empty() && !checkpoint.aligned_replay.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] aligned replay was applied more than once",
                    self.projection.op_name
                )));
            }
            for (assignment, side, watermark, bytes) in checkpoint.aligned_replay {
                let batch = laminar_core::serialization::deserialize_batch_stream(&bytes).map_err(
                    |error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] aligned replay restore: {error}",
                            self.projection.op_name
                        ))
                    },
                )?;
                self.aligned_replay.push_back((
                    assignment,
                    side,
                    watermark,
                    crate::operator::RetainedBatch::local(batch),
                ));
            }
        }

        Ok(())
    }

    fn watermark_hold(&self) -> Option<i64> {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).unwrap_or(i64::MAX);
        let right_only = matches!(
            self.config.join_type,
            laminar_sql::parser::join_parser::JoinType::RightSemi
                | laminar_sql::parser::join_parser::JoinType::RightAnti
        );
        let safe = if right_only {
            self.applied_left_watermark
                .min(self.applied_right_watermark)
        } else {
            self.applied_left_watermark
                .min(self.applied_right_watermark.saturating_sub(bound_ms))
        };
        #[cfg(feature = "cluster")]
        let safe = self
            .aligned_replay
            .iter()
            .map(|(_, side, watermark, _)| {
                if right_only || matches!(side, JoinInputSide::Left) {
                    *watermark
                } else {
                    watermark.saturating_sub(bound_ms)
                }
            })
            .min()
            .map_or(safe, |replay| safe.min(replay));
        Some(safe)
    }

    #[cfg(feature = "cluster")]
    fn restored_output_watermark(&self) -> Option<i64> {
        self.watermark_hold()
    }

    #[cfg(feature = "cluster")]
    fn wants_input(&self) -> bool {
        self.aligned_replay.is_empty()
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: crate::operator::RetainedBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        if self.cluster_shuffle.is_none() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received shuffle data without cluster ownership",
                self.projection.op_name
            )));
        }
        let side = if stage == format!("{}::left", self.projection.op_name) {
            JoinInputSide::Left
        } else if stage == format!("{}::right", self.projection.op_name) {
            JoinInputSide::Right
        } else {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] rejected unknown shuffle stage '{stage}'",
                self.projection.op_name
            )));
        };
        let assignment = batch.assignment_version().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] received unscoped shuffle data",
                self.projection.op_name
            ))
        })?;
        self.aligned_replay
            .push_back((assignment, side, watermark, batch));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_by_vnode(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<
        Option<std::collections::HashMap<u32, crate::checkpoint_coordinator::StagedSlice>>,
        DbError,
    > {
        use crate::checkpoint_coordinator::StagedSlice;

        if required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received a non-canonical vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.projection.op_name
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] cannot capture vnode state without cluster ownership",
                self.projection.op_name
            ))
        })?;
        if config.registry.vnode_count() != vnode_count {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] vnode count changed during capture",
                self.projection.op_name
            )));
        }
        let required: std::collections::HashSet<u32> = required_vnodes.iter().copied().collect();
        let unexpected: Vec<u32> = self
            .vnode_states
            .iter()
            .enumerate()
            .filter_map(|(vnode, state)| state.as_ref().map(|_| vnode as u32))
            .filter(|vnode| !required.contains(vnode))
            .collect();
        if !unexpected.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] retained unowned vnode state {unexpected:?}",
                self.projection.op_name
            )));
        }

        let mut captured = std::collections::HashMap::with_capacity(required_vnodes.len());
        let mut retained_encoded_bytes = 0usize;
        for &vnode in required_vnodes {
            let remaining = self
                .max_managed_state_bytes
                .checked_sub(retained_encoded_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] vnode checkpoints exhausted their {}-byte limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?;
            let bytes = if let Some(state) = self
                .vnode_states
                .get_mut(vnode as usize)
                .and_then(Option::as_mut)
            {
                Self::serialize_state(
                    state,
                    &self.config,
                    &format!(
                        "interval join [{}] vnode {vnode} checkpoint serialization",
                        self.projection.op_name
                    ),
                    remaining,
                )?
            } else {
                let mut empty = IntervalJoinState::new();
                Self::serialize_state(
                    &mut empty,
                    &self.config,
                    &format!(
                        "interval join [{}] vnode {vnode} empty checkpoint serialization",
                        self.projection.op_name
                    ),
                    remaining,
                )?
            };
            retained_encoded_bytes = retained_encoded_bytes
                .checked_add(bytes.capacity())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] vnode checkpoint byte accounting overflow",
                        self.projection.op_name
                    ))
                })?;
            captured.insert(vnode, StagedSlice::Bytes(bytes::Bytes::from(bytes)));
        }
        if captured.is_empty() {
            Ok(None)
        } else {
            Ok(Some(captured))
        }
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] already owns vnode transition state",
                self.projection.op_name
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] cannot transition without cluster ownership",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if transition.target.vnode_count != config.registry.vnode_count()
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] transition target does not match assignment {}",
                self.projection.op_name,
                assignment.version()
            )));
        }

        if self.vnode_states.len() != transition.target.vnode_count as usize {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] state table has {} slots for target vnode count {}",
                self.projection.op_name,
                self.vnode_states.len(),
                transition.target.vnode_count
            )));
        }

        let mut replacements = BTreeMap::new();
        for vnode in transition.revoked {
            if *vnode >= transition.target.vnode_count {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] revoked vnode {vnode} is outside the target vnode space",
                    self.projection.op_name
                )));
            }
            replacements.insert(*vnode, None);
        }
        let mut restore_vnodes = rustc_hash::FxHashSet::default();
        let mut restore_payload_bytes = 0usize;
        for restore in transition.restores {
            if !restore.deltas.is_empty() {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] vnode {} has an unsupported delta chain",
                    self.projection.op_name, restore.vnode
                )));
            }
            if !restore_vnodes.insert(restore.vnode) {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition repeats restored vnode {}",
                    self.projection.op_name, restore.vnode
                )));
            }
            if assignment
                .owners()
                .get(usize::try_from(restore.vnode).unwrap_or(usize::MAX))
                != Some(&config.self_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] restore vnode {} is not owned by this node",
                    self.projection.op_name, restore.vnode
                )));
            }
            restore_payload_bytes = restore_payload_bytes
                .checked_add(restore.base.len())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition restore payload accounting overflow",
                        self.projection.op_name
                    ))
                })?;
            if restore_payload_bytes > self.max_managed_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition restore payload exceeds its {}-byte limit",
                    self.projection.op_name, self.max_managed_state_bytes
                )));
            }
        }

        let live_bytes = self.accounted_state_bytes();
        let mut restored_bytes = 0usize;
        for restore in transition.restores {
            let remaining = self
                .max_managed_state_bytes
                .checked_sub(live_bytes.saturating_add(restored_bytes))
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition state exceeds its {}-byte limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?;
            let mut state = Self::deserialize_state(
                restore.base,
                &self.config,
                &format!(
                    "interval join [{}] vnode {} restore",
                    self.projection.op_name, restore.vnode
                ),
                remaining,
            )?;
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            state.validate_vnode(restore.vnode, transition.target.vnode_count, &self.config)?;
            restored_bytes = restored_bytes.saturating_add(state.accounted_state_bytes());
            replacements.insert(restore.vnode, Some(Box::new(state)));
        }
        for (vnode, state) in self.vnode_states.iter().enumerate() {
            let vnode = vnode as u32;
            if state.is_none() || replacements.contains_key(&vnode) {
                continue;
            }
            if assignment
                .owners()
                .get(usize::try_from(vnode).unwrap_or(usize::MAX))
                != Some(&config.self_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition retained unowned vnode {vnode}",
                    self.projection.op_name
                )));
            }
        }
        let prepared = PreparedIntervalJoinTransition {
            replacements: replacements.into_iter().collect(),
        };
        let total_bytes = live_bytes.saturating_add(Self::transition_accounted_bytes(&prepared));
        if total_bytes > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] prepared transition accounts {total_bytes} bytes; limit is {} bytes",
                self.projection.op_name, self.max_managed_state_bytes
            )));
        }
        self.prepared_vnode_transition = Some(prepared);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(self.vnode_transition_cleanup.is_none());
        self.vnode_transition_cleanup = Some(IntervalJoinTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let prepared = self
            .prepared_vnode_transition
            .take()
            .expect("interval join transition must be prepared before publication");
        assert!(self.vnode_transition_cleanup.is_none());
        let mut prepared = prepared;
        for (vnode, replacement) in &mut prepared.replacements {
            let slot = self
                .vnode_states
                .get_mut(*vnode as usize)
                .expect("prepared interval join vnode must have a state slot");
            std::mem::swap(slot, replacement);
        }
        self.vnode_transition_cleanup = Some(IntervalJoinTransitionCleanup::Published(prepared));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        self.vnode_transition_cleanup = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::time::Duration;

    #[cfg(feature = "cluster")]
    use arrow::array::Int64Array;

    #[cfg(feature = "cluster")]
    async fn single_owner_shuffle(
        vnode_count: u32,
    ) -> (
        ClusterShuffleConfig,
        laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::state::{NodeId, VnodeRegistry};

        let self_id = NodeId(1);
        let incarnation = uuid::Uuid::from_u128(1);
        let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                self_id.0,
                "127.0.0.1:0".parse().unwrap(),
                incarnation,
            )
            .await
            .unwrap(),
        );
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            self_id.0,
            incarnation,
        ));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();

        let owners = vec![self_id.0; usize::try_from(vnode_count).unwrap()];
        let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: incarnation,
            }],
        )
        .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();

        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                self_id,
            },
            fence,
        )
    }

    #[cfg(feature = "cluster")]
    async fn two_owner_shuffle() -> (
        ClusterShuffleConfig,
        Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let registry = Arc::new(VnodeRegistry::new(2));
        registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(1),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(2),
                },
            ],
        )
        .unwrap();
        let local_receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .unwrap(),
        );
        let remote_receiver = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        sender.register_peer(2, remote_receiver.local_addr());
        let local_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        local_receiver
            .install_process_lease_deadline(Arc::clone(&local_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(local_deadline)
            .unwrap();
        remote_receiver
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(
                60,
            ))))
            .unwrap();
        local_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        remote_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        sender.install_assignment_fence(&fence, &[1, 2]).unwrap();

        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver: local_receiver,
                self_id: NodeId(1),
            },
            remote_receiver,
        )
    }

    fn test_config() -> StreamJoinConfig {
        StreamJoinConfig {
            join_type: laminar_sql::parser::join_parser::JoinType::Inner,
            left_keys: vec!["id".to_string()],
            right_keys: vec!["id".to_string()],
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
        }
    }

    #[test]
    fn watermark_hold_uses_the_preserved_output_side() {
        use laminar_sql::parser::join_parser::JoinType;

        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            let mut config = test_config();
            config.join_type = join_type;
            let mut operator =
                IntervalJoinOperator::new("frontier", config, None, SessionContext::new());
            operator.applied_left_watermark = 2_000;
            operator.applied_right_watermark = 1_500;
            let expected = if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                1_500
            } else {
                1_400
            };
            assert_eq!(operator.watermark_hold(), Some(expected), "{join_type:?}");
        }
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn pending_right_replay_holds_left_oriented_output_by_the_join_bound() {
        use laminar_sql::parser::join_parser::JoinType;

        for (join_type, expected) in [(JoinType::Inner, 4_900), (JoinType::RightSemi, 5_000)] {
            let mut config = test_config();
            config.join_type = join_type;
            let mut operator =
                IntervalJoinOperator::new("replay-frontier", config, None, SessionContext::new());
            operator.applied_left_watermark = 10_000;
            operator.applied_right_watermark = 10_000;
            operator.aligned_replay.push_back((
                1,
                JoinInputSide::Right,
                5_000,
                crate::operator::RetainedBatch::local(right_batch(&["A"], &[5_000], &[1.0])),
            ));
            assert_eq!(operator.watermark_hold(), Some(expected), "{join_type:?}");
        }
    }

    fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn key_for_vnode(target: u32, vnode_count: u32) -> String {
        for candidate in 0..1_000 {
            let key = format!("vnode-{candidate}");
            let batch = left_batch(&[key.as_str()], &[100], &[1.0]);
            let vnodes = laminar_core::shuffle::row_vnodes(&batch, &[0], vnode_count).unwrap();
            if vnodes == [target] {
                return key;
            }
        }
        panic!("could not find a key for vnode {target}");
    }

    #[cfg(feature = "cluster")]
    fn composite_left_batch(regions: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("region", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hot"; regions.len()])),
                Arc::new(Int64Array::from(regions.to_vec())),
                Arc::new(TimestampMillisecondArray::from(vec![100; regions.len()])),
                Arc::new(Float64Array::from(vec![1.0; regions.len()])),
            ],
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    fn incompatible_left_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["bad"])),
                Arc::new(TimestampMillisecondArray::from(vec![110])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_interval_join() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

        let result = op
            .process(&[vec![left], vec![right]], &[0, 0])
            .await
            .unwrap();

        // A: |100 - 110| = 10 <= 100 -> match
        // B: |200 - 250| = 50 <= 100 -> match
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn local_join_routes_into_configured_vnodes() {
        let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
        let mut op = IntervalJoinOperator::new_with_key_groups(
            "local_vnodes",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        let key_zero = key_for_vnode(0, u32::from(key_group_count));
        let key_one = key_for_vnode(1, u32::from(key_group_count));
        let keys = [key_zero.as_str(), key_one.as_str()];

        let output = op
            .process(
                &[
                    vec![left_batch(&keys, &[100, 200], &[10.0, 20.0])],
                    vec![right_batch(&keys, &[110, 210], &[1.0, 2.0])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(op.vnode_states[0].is_some());
        assert!(op.vnode_states[1].is_some());
        assert!(op.vnode_states[2..].iter().all(Option::is_none));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_admits_current_batch_before_advancing_its_watermark() {
        let (shuffle, _) = single_owner_shuffle(8).await;
        let mut op = IntervalJoinOperator::new(
            "current_batch_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let output = op
            .process(
                &[
                    vec![left_batch(&["A"], &[100], &[1.0])],
                    vec![right_batch(&["A"], &[110], &[2.0])],
                ],
                &[300, 300],
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let error = op
            .process(
                &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
                &[300, 300],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_replay_sweeps_resident_vnodes_before_advancing_watermark() {
        use laminar_sql::parser::join_parser::JoinType;

        let (shuffle, _) = single_owner_shuffle(2).await;
        let assignment_version = shuffle.registry.assignment_version();
        let resident_key = key_for_vnode(0, 2);
        let replay_key = key_for_vnode(1, 2);
        let left = left_batch(&[resident_key.as_str()], &[100], &[1.0]);
        let right = right_batch(&[replay_key.as_str()], &[250], &[2.0]);
        let mut config = test_config();
        config.join_type = JoinType::LeftAnti;
        let mut op = IntervalJoinOperator::new(
            "replay_sweep",
            config,
            None,
            laminar_sql::create_session_context(),
        );
        op.set_input_schemas(left.schema(), right.schema());
        op.attach_cluster_shuffle(shuffle);

        let initial = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
        assert!(initial.is_empty());
        assert!(op.vnode_states[0].is_some());
        op.aligned_replay.push_back((
            assignment_version,
            JoinInputSide::Right,
            300,
            crate::operator::RetainedBatch::local(right),
        ));

        let output = op.execute_aligned_replay().await.unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(op.applied_right_watermark, 300);
        assert!(op.aligned_replay.is_empty());
        assert_eq!(op.vnode_states[0].as_ref().unwrap().buffered_rows(), (0, 0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn outbound_admission_turns_later_local_halt_into_recovery() {
        let (shuffle, remote_receiver) = two_owner_shuffle().await;
        let local_key = key_for_vnode(0, 2);
        let remote_key = key_for_vnode(1, 2);
        let mut op = IntervalJoinOperator::new(
            "post_shuffle_failure",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);
        op.max_managed_state_bytes = 1;

        let error = op
            .process(
                &[
                    vec![left_batch(
                        &[local_key.as_str(), remote_key.as_str()],
                        &[100, 100],
                        &[1.0, 2.0],
                    )],
                    vec![],
                ],
                &[0, 0],
            )
            .await
            .unwrap_err();

        assert!(matches!(error, DbError::ShufflePartialSend(_)));
        assert!(error.requires_pipeline_recovery());
        let received = tokio::time::timeout(Duration::from_secs(2), remote_receiver.recv())
            .await
            .expect("remote frame was not delivered")
            .expect("shuffle receiver closed");
        assert!(matches!(
            received.message(),
            laminar_core::shuffle::ShuffleMessage::Data { .. }
        ));
    }

    #[tokio::test]
    async fn test_cross_cycle_matching() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        // Cycle 1: only left data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let result = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
        assert!(result.is_empty());

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = op.process(&[vec![], vec![right]], &[0, 0]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
        assert!(op.vnode_states[0].is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn composite_keys_route_by_the_full_ordered_tuple() {
        let vnode_count = 64;
        let batch = composite_left_batch(&[1, 2]);
        let mut expected = laminar_core::shuffle::row_vnodes(&batch, &[0, 1], vnode_count).unwrap();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(expected.len(), 2, "test tuple hashes unexpectedly collided");

        let (shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut config = test_config();
        config.left_keys.push("region".into());
        config.right_keys.push("region".into());
        let mut op = IntervalJoinOperator::new(
            "composite_interval",
            config,
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);
        op.process(&[vec![batch], vec![]], &[0, 0]).await.unwrap();

        let actual = op
            .vnode_states
            .iter()
            .enumerate()
            .filter_map(|(vnode, state)| state.as_ref().map(|_| vnode as u32))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn later_vnode_failure_requires_recovery_after_prior_admission() {
        let (shuffle, _) = single_owner_shuffle(2).await;
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let mut retained = IntervalJoinState::new();
        let mut output_budget = IntervalJoinOutputBudget::default();
        execute_interval_join_cycle(
            &mut retained,
            &[left_batch(&["seed"], &[100], &[1.0])],
            &[],
            &op.config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
        )
        .unwrap();
        op.vnode_states[1] = Some(Box::new(retained));

        let mut routed = BTreeMap::new();
        routed.insert(0, [vec![left_batch(&["ok"], &[100], &[1.0])], vec![]]);
        routed.insert(1, [vec![incompatible_left_batch()], vec![]]);
        let error = op.execute_routed_shards(routed, 0, 0).unwrap_err();

        assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert_eq!(op.vnode_states[0].as_ref().unwrap().buffered_rows(), (1, 0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_watermark_sweep_failure_requires_recovery() {
        let (shuffle, _) = single_owner_shuffle(2).await;
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let mut retained = IntervalJoinState::new();
        let mut output_budget = IntervalJoinOutputBudget::default();
        for timestamp in (1_000..1_400).step_by(10) {
            execute_interval_join_cycle(
                &mut retained,
                &[left_batch(&["seed"], &[timestamp], &[1.0])],
                &[],
                &op.config,
                0,
                0,
                0,
                0,
                usize::MAX,
                &mut output_budget,
            )
            .unwrap();
        }
        op.vnode_states[0] = Some(Box::new(retained));

        // Force compaction to fail after the sweep has already removed old index entries.
        op.config.left_keys = vec!["missing".to_string()];
        let error = op.process(&[], &[0, 1_300]).await.unwrap_err();

        assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(
            op.vnode_states[0].as_ref().unwrap().buffered_rows(),
            (20, 0)
        );
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx.clone());

        // Buffer some data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ = op
            .process(&[vec![left], vec![right]], &[50, 50])
            .await
            .unwrap();
        assert_eq!(op.watermark_hold(), Some(-50));

        // Checkpoint
        let cp = op.checkpoint().unwrap().expect("should have state");
        assert!(!cp.data.is_empty());

        // Restore into a new operator
        let mut op2 = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        op2.restore(cp).unwrap();
        assert_eq!(op2.watermark_hold(), Some(-50));

        // New right data should match the restored left
        let right2 = right_batch(&["A"], &[120], &[2.0]);
        let result = op2
            .process(&[vec![], vec![right2]], &[50, 50])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn checkpoint_allows_ipc_over_half_budget_when_archive_fits() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        let wide_key = "x".repeat(64 * 1024);
        op.process(
            &[
                vec![left_batch(&[wide_key.as_str()], &[100], &[1.0])],
                vec![],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

        let ipc_bytes = op.vnode_states[0]
            .as_mut()
            .unwrap()
            .snapshot_checkpoint(&op.config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap()
            .retained_ipc_bytes()
            .unwrap();
        let limit = ipc_bytes
            .checked_add(ipc_bytes / 2)
            .and_then(|bytes| bytes.checked_sub(1))
            .unwrap();
        assert!(ipc_bytes > limit / 2);
        assert!(op.accounted_state_bytes() <= limit);
        op.set_managed_state_budget(limit);

        let checkpoint = op.checkpoint().unwrap().unwrap();
        assert!(checkpoint.data.capacity() <= limit);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_transition_preserves_checkpointed_admission_watermarks() {
        let vnode_count = 8;
        let (source_shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut source = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        source.attach_cluster_shuffle(source_shuffle);
        assert!(source
            .process(&[vec![], vec![]], &[300, 300])
            .await
            .unwrap()
            .is_empty());
        let checkpoint = source.checkpoint().unwrap().unwrap();

        let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
        let mut restored = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        restored.restore(checkpoint).unwrap();
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &[],
            })
            .unwrap();
        restored.publish_vnode_transition();
        restored.finish_vnode_transition();

        let error = restored
            .process(
                &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
                &[300, 300],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_capture_restore_preserves_cross_cycle_match() {
        let vnode_count = 8;
        let key_batch = left_batch(&["hot"], &[100], &[10.0]);
        let vnode = laminar_core::shuffle::row_vnodes(&key_batch, &[0], vnode_count).unwrap()[0];

        let (donor_shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut donor = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        donor.attach_cluster_shuffle(donor_shuffle);
        assert!(donor
            .process(&[vec![key_batch], vec![]], &[0, 0])
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            donor
                .vnode_states
                .iter()
                .enumerate()
                .filter_map(|(vnode, state)| state.as_ref().map(|_| vnode as u32))
                .collect::<Vec<_>>(),
            vec![vnode]
        );

        let slice = donor
            .checkpoint_by_vnode(&[vnode], vnode_count)
            .unwrap()
            .unwrap()
            .remove(&vnode)
            .unwrap();
        let crate::checkpoint_coordinator::StagedSlice::Bytes(slice) = slice else {
            panic!("interval join vnode capture must be materialized bytes");
        };

        let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
        let mut restored = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        let restores = [crate::operator_graph::ManagedVnodeRestore {
            vnode,
            base: &slice,
            deltas: &[],
        }];
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
            })
            .unwrap();
        let prepared = restored.managed_state_accounting().unwrap();
        assert!(prepared.prepared > 0);
        assert_eq!(prepared.retired, 0);
        restored.abort_vnode_transition();
        let aborted = restored.managed_state_accounting().unwrap();
        assert!(aborted.prepared > 0);
        assert_eq!(aborted.retired, 0);
        restored.finish_vnode_transition();
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
            })
            .unwrap();
        restored.publish_vnode_transition();
        restored.finish_vnode_transition();

        let output = restored
            .process(
                &[vec![], vec![right_batch(&["hot"], &[110], &[1.0])]],
                &[0, 0],
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            restored
                .vnode_states
                .iter()
                .enumerate()
                .filter_map(|(vnode, state)| state.as_ref().map(|_| vnode as u32))
                .collect::<Vec<_>>(),
            vec![vnode]
        );

        assert!(restored
            .process(&[vec![], vec![]], &[500, 500])
            .await
            .unwrap()
            .is_empty());
        let state = restored.vnode_states[vnode as usize].as_ref().unwrap();
        assert_eq!(state.buffered_rows(), (0, 0));
    }

    #[tokio::test]
    async fn post_projection_fault_requires_recovery_after_state_admission() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            Some(Arc::from("SELECT missing FROM __interval_tmp")),
            ctx,
        );

        let error = op
            .process(
                &[
                    vec![left_batch(&["A"], &[100], &[10.0])],
                    vec![right_batch(&["A"], &[110], &[1.0])],
                ],
                &[0, 0],
            )
            .await
            .unwrap_err();

        assert!(matches!(&error, DbError::StatefulOperatorPartialApply(_)));
        assert!(error.requires_pipeline_recovery());
        let checkpoint = op.checkpoint().unwrap().unwrap();
        let decoded = rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .unwrap();
        assert_eq!(decoded.shards.len(), 1);
        assert_eq!(decoded.shards[0].1.left_buffer_rows, 1);
        assert_eq!(decoded.shards[0].1.right_buffer_rows, 1);
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = IntervalJoinOperator::new("my_interval_join", test_config(), None, ctx);
        assert_eq!(&*op.projection.op_name, "my_interval_join");
    }
}
