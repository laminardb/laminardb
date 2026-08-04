//! Managed vnode-local temporal join execution.

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::row::Rows;
use async_trait::async_trait;
use laminar_connectors::connector::{
    source_mutations, source_mutations_routed, strip_source_mutations,
    strip_source_mutations_routed,
};
use laminar_core::state::{
    KeyGroupCount, NodeId, PartitionKeyCodecV1, VnodeAssignmentSnapshot, VnodeRegistry,
    LOCAL_NODE_ID,
};
use laminar_sql::temporal::{MAX_TEMPORAL_PROBES_PER_ROW, MAX_TEMPORAL_PROBE_HORIZON_MS};
use laminar_sql::translator::TemporalJoinTranslatorConfig;

use crate::error::DbError;
use crate::operator::capability::OperatorCapability;
use crate::operator_graph::{
    merge_input_frontiers, CapturedVnodeState, GraphOperator, InputFrontier,
    ManagedStateAccountingSnapshot, OperatorCheckpoint,
};
use crate::temporal_join_state::{
    TemporalJoinStateConfig, TemporalJoinVnodeState, TemporalStateLimits,
};

const ABSENT_VNODE: u8 = 0;
const PRESENT_VNODE: u8 = 1;
const OPERATOR_CHECKPOINT_VERSION: u8 = 1;
const PENDING_HOLD_ENTRY_CHARGE: usize = 64;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TemporalJoinOperatorCheckpoint {
    version: u8,
    left_watermark: Option<i64>,
    left_idle: bool,
    right_watermark: Option<i64>,
    right_idle: bool,
    maintenance_cursor: u32,
    maintenance_pending: bool,
    maintenance_remaining: u32,
    maintenance_rescan: bool,
    published_output_watermark: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TemporalJoinExecutionLimits {
    pub(crate) left_allowed_lateness_ms: i64,
    pub(crate) right_allowed_lateness_ms: i64,
    pub(crate) history_retention_ms: i64,
    pub(crate) max_pending_probes: usize,
    pub(crate) ready_probe_budget: NonZeroUsize,
    pub(crate) history_gc_budget: NonZeroUsize,
    pub(crate) maintenance_vnode_budget: NonZeroUsize,
}

#[derive(Clone, Copy)]
enum TemporalInputSide {
    Left,
    Right,
}

struct RoutedTemporalBatch {
    batch: RecordBatch,
    keys: Arc<Rows>,
    source_rows: Arc<[u32]>,
}

pub(crate) struct ManagedTemporalJoinOperator {
    name: Arc<str>,
    config: TemporalJoinTranslatorConfig,
    limits: TemporalJoinExecutionLimits,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    left_key_indices: Vec<usize>,
    right_key_indices: Vec<usize>,
    key_codec: Arc<PartitionKeyCodecV1>,
    vnode_count: NonZeroU32,
    left_time_index: usize,
    right_time_index: usize,
    minimum_probe_offset: i64,
    key_group_count: KeyGroupCount,
    local_assignment: VnodeAssignmentSnapshot,
    vnode_states: Vec<Option<Box<TemporalJoinVnodeState>>>,
    resident_vnodes: Vec<u32>,
    vnode_pending_holds: Vec<Option<i64>>,
    pending_hold_counts: BTreeMap<i64, usize>,
    checkpointed_vnodes: Vec<bool>,
    dirty_vnodes: Vec<bool>,
    retained_state_bytes: usize,
    max_managed_state_bytes: usize,
    frontiers: [InputFrontier; 2],
    restored_frontiers: Option<[InputFrontier; 2]>,
    whole_restored: bool,
    pending_frontiers: Option<[InputFrontier; 2]>,
    frontier_cursor: usize,
    frontier_remaining: usize,
    frontier_has_work: bool,
    maintenance_cursor: usize,
    maintenance_pending: bool,
    maintenance_remaining: usize,
    maintenance_rescan: bool,
    published_output_watermark: Option<i64>,
}

impl ManagedTemporalJoinOperator {
    pub(crate) fn try_new(
        name: &str,
        config: TemporalJoinTranslatorConfig,
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        key_group_count: KeyGroupCount,
        limits: TemporalJoinExecutionLimits,
    ) -> Result<Self, DbError> {
        if config.left_key_columns.is_empty()
            || config.left_key_columns.len() != config.right_key_columns.len()
        {
            return Err(DbError::Config(format!(
                "temporal join [{name}] requires paired equality keys"
            )));
        }
        let left_key_indices = config
            .left_key_columns
            .iter()
            .map(|column| column_index(&left_schema, column, name, "left key"))
            .collect::<Result<Vec<_>, _>>()?;
        let right_key_indices = config
            .right_key_columns
            .iter()
            .map(|column| column_index(&right_schema, column, name, "right key"))
            .collect::<Result<Vec<_>, _>>()?;
        let key_codec = Arc::new(
            PartitionKeyCodecV1::try_new(
                left_key_indices
                    .iter()
                    .map(|&index| left_schema.field(index).data_type().clone()),
            )
            .map_err(|error| {
                DbError::Config(format!(
                    "temporal join [{name}] key is not partitionable: {error}"
                ))
            })?,
        );
        let left_time_index = column_index(
            &left_schema,
            &config.left_time_column,
            name,
            "left event time",
        )?;
        let right_time_index = column_index(
            &right_schema,
            &config.right_time_column,
            name,
            "right event time",
        )?;
        let minimum_probe_offset = config
            .probe_schedule
            .offsets_ms()
            .iter()
            .copied()
            .min()
            .ok_or_else(|| DbError::Config("temporal probe schedule must not be empty".into()))?;
        let vnode_count = u32::from(key_group_count);
        let vnode_count_nonzero = NonZeroU32::new(vnode_count)
            .ok_or_else(|| DbError::Config("temporal vnode count must be nonzero".into()))?;
        let local_assignment =
            VnodeRegistry::single_owner(vnode_count, LOCAL_NODE_ID).versioned_snapshot();
        let operator = Self {
            name: Arc::from(name),
            config,
            limits,
            left_schema,
            right_schema,
            left_key_indices,
            right_key_indices,
            key_codec,
            vnode_count: vnode_count_nonzero,
            left_time_index,
            right_time_index,
            minimum_probe_offset,
            key_group_count,
            local_assignment,
            vnode_states: std::iter::repeat_with(|| None)
                .take(vnode_count as usize)
                .collect(),
            resident_vnodes: Vec::with_capacity(vnode_count as usize),
            vnode_pending_holds: vec![None; vnode_count as usize],
            pending_hold_counts: BTreeMap::new(),
            checkpointed_vnodes: vec![false; vnode_count as usize],
            dirty_vnodes: vec![false; vnode_count as usize],
            retained_state_bytes: 0,
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            frontiers: [InputFrontier::default(); 2],
            restored_frontiers: None,
            whole_restored: false,
            pending_frontiers: None,
            frontier_cursor: 0,
            frontier_remaining: 0,
            frontier_has_work: false,
            maintenance_cursor: 0,
            maintenance_pending: false,
            maintenance_remaining: 0,
            maintenance_rescan: false,
            published_output_watermark: None,
        };
        let validation = operator.state_config(0, operator.max_managed_state_bytes)?;
        let _ = TemporalJoinVnodeState::try_new(
            Arc::clone(&operator.left_schema),
            Arc::clone(&operator.right_schema),
            validation,
        )?;
        Ok(operator)
    }

    fn state_config(
        &self,
        vnode: u32,
        max_retained_bytes: usize,
    ) -> Result<TemporalJoinStateConfig, DbError> {
        Ok(TemporalJoinStateConfig {
            vnode,
            vnode_count: self.vnode_count,
            left_key_indices: self.left_key_indices.clone(),
            right_key_indices: self.right_key_indices.clone(),
            key_codec: Arc::clone(&self.key_codec),
            left_time_index: self.left_time_index,
            right_time_index: self.right_time_index,
            left_name: self.config.left_table.clone(),
            right_name: self.config.right_table.clone(),
            operator_name: self.name.to_string(),
            join_kind: self.config.join_kind,
            schedule: self.config.probe_schedule.clone(),
            emit_probe_metadata: self.config.probe_alias.is_some(),
            left_allowed_lateness_ms: self.limits.left_allowed_lateness_ms,
            right_allowed_lateness_ms: self.limits.right_allowed_lateness_ms,
            history_retention_ms: self.limits.history_retention_ms,
            limits: TemporalStateLimits {
                max_retained_bytes,
                max_pending_probes: self.limits.max_pending_probes,
                max_offsets_per_row: MAX_TEMPORAL_PROBES_PER_ROW,
                max_horizon_ms: MAX_TEMPORAL_PROBE_HORIZON_MS,
            },
        })
    }

    fn topology_charge(&self) -> Result<usize, DbError> {
        let vnode_slots = self
            .vnode_states
            .capacity()
            .checked_mul(std::mem::size_of::<Option<Box<TemporalJoinVnodeState>>>())
            .ok_or_else(|| self.accounting_error())?;
        let resident_roster = self
            .resident_vnodes
            .capacity()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| self.accounting_error())?;
        let pending_holds = self
            .vnode_pending_holds
            .capacity()
            .checked_mul(std::mem::size_of::<Option<i64>>())
            .and_then(|bytes| {
                bytes.checked_add(
                    self.pending_hold_counts
                        .len()
                        .checked_mul(PENDING_HOLD_ENTRY_CHARGE)?,
                )
            })
            .ok_or_else(|| self.accounting_error())?;
        let assignment = self
            .local_assignment
            .owners()
            .len()
            .checked_mul(std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>())
            .ok_or_else(|| self.accounting_error())?;
        let key_indices = self
            .left_key_indices
            .capacity()
            .checked_add(self.right_key_indices.capacity())
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<usize>()))
            .ok_or_else(|| self.accounting_error())?;
        let configured_keys = self
            .config
            .left_key_columns
            .capacity()
            .checked_add(self.config.right_key_columns.capacity())
            .and_then(|capacity| capacity.checked_mul(std::mem::size_of::<String>()))
            .and_then(|bytes| {
                self.config
                    .left_key_columns
                    .iter()
                    .chain(&self.config.right_key_columns)
                    .try_fold(bytes, |total, column| total.checked_add(column.capacity()))
            })
            .ok_or_else(|| self.accounting_error())?;
        let bool_rosters = self
            .checkpointed_vnodes
            .capacity()
            .div_ceil(8)
            .checked_add(self.dirty_vnodes.capacity().div_ceil(8))
            .ok_or_else(|| self.accounting_error())?;
        let strings = self
            .name
            .len()
            .checked_add(self.config.left_table.capacity())
            .and_then(|bytes| bytes.checked_add(self.config.right_table.capacity()))
            .and_then(|bytes| bytes.checked_add(self.config.left_time_column.capacity()))
            .and_then(|bytes| bytes.checked_add(self.config.right_time_column.capacity()))
            .and_then(|bytes| {
                bytes.checked_add(self.config.probe_alias.as_ref().map_or(0, String::capacity))
            })
            .ok_or_else(|| self.accounting_error())?;
        let schedule = self
            .config
            .probe_schedule
            .offsets_ms()
            .len()
            .checked_mul(std::mem::size_of::<i64>())
            .ok_or_else(|| self.accounting_error())?;
        [
            vnode_slots,
            resident_roster,
            pending_holds,
            assignment,
            key_indices,
            configured_keys,
            bool_rosters,
            strings,
            schedule,
        ]
        .into_iter()
        .try_fold(std::mem::size_of::<Self>(), |total, bytes| {
            total
                .checked_add(bytes)
                .ok_or_else(|| self.accounting_error())
        })
    }

    fn checked_accounted_state_bytes(&self) -> Result<usize, DbError> {
        self.topology_charge()?
            .checked_add(self.retained_state_bytes)
            .ok_or_else(|| self.accounting_error())
    }

    fn add_resident_vnode(&mut self, vnode: u32) {
        if let Err(index) = self.resident_vnodes.binary_search(&vnode) {
            self.resident_vnodes.insert(index, vnode);
        }
    }

    fn remove_resident_vnode(&mut self, vnode: u32) {
        if let Ok(index) = self.resident_vnodes.binary_search(&vnode) {
            self.resident_vnodes.remove(index);
        }
    }

    fn refresh_vnode_pending_hold(&mut self, vnode: u32) -> Result<(), DbError> {
        let index = vnode as usize;
        let previous = self.vnode_pending_holds[index];
        let current = self.vnode_states[index]
            .as_ref()
            .and_then(|state| state.pending_watermark_hold());
        if previous == current {
            return Ok(());
        }
        let next_count = current
            .map(|hold| {
                self.pending_hold_counts
                    .get(&hold)
                    .copied()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| self.accounting_error())
            })
            .transpose()?;
        if let Some(hold) = previous {
            match self.pending_hold_counts.get(&hold).copied() {
                Some(1) => {
                    self.pending_hold_counts.remove(&hold);
                }
                Some(count) => {
                    self.pending_hold_counts.insert(hold, count - 1);
                }
                None => return Err(self.accounting_error()),
            }
        }
        if let (Some(hold), Some(count)) = (current, next_count) {
            self.pending_hold_counts.insert(hold, count);
        }
        self.vnode_pending_holds[index] = current;
        Ok(())
    }

    fn accounting_error(&self) -> DbError {
        DbError::Pipeline(format!(
            "temporal join [{}] retained-state accounting overflow",
            self.name
        ))
    }

    fn validate_inputs(&self, inputs: &[Vec<RecordBatch>]) -> Result<(), DbError> {
        if inputs.len() > 2 {
            return Err(DbError::InvalidOperation(format!(
                "temporal join [{}] accepts exactly two input ports",
                self.name
            )));
        }
        for batch in inputs.first().into_iter().flatten() {
            let mutations = source_mutations(batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] left source metadata: {error}",
                    self.name
                ))
            })?;
            if mutations.is_some() {
                return Err(DbError::InvalidOperation(format!(
                    "temporal join [{}] left input must be append-only",
                    self.name
                )));
            }
            if batch.schema().as_ref() != self.left_schema.as_ref() {
                return Err(self.schema_error("left"));
            }
        }
        for batch in inputs.get(1).into_iter().flatten() {
            source_mutations(batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] right source metadata: {error}",
                    self.name
                ))
            })?;
            let positioned = strip_source_mutations(batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] right source metadata: {error}",
                    self.name
                ))
            })?;
            if positioned.schema().as_ref() != self.right_schema.as_ref() {
                return Err(self.schema_error("right"));
            }
        }
        Ok(())
    }

    fn schema_error(&self, side: &str) -> DbError {
        DbError::SchemaMismatch(format!(
            "temporal join [{}] {side} batch does not match its declared positioned schema",
            self.name
        ))
    }

    fn route_local_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]>, DbError> {
        self.validate_inputs(inputs)?;
        let mut routed: BTreeMap<u32, [Vec<RoutedTemporalBatch>; 2]> = BTreeMap::new();
        for (side, batches) in [
            (
                TemporalInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                TemporalInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            let key_indices = match side {
                TemporalInputSide::Left => &self.left_key_indices,
                TemporalInputSide::Right => &self.right_key_indices,
            };
            for batch in batches.iter().filter(|batch| batch.num_rows() != 0) {
                let columns = key_indices
                    .iter()
                    .map(|&index| Arc::clone(batch.column(index)))
                    .collect::<Vec<_>>();
                let keys = Arc::new(self.key_codec.encode_columns(&columns).map_err(|error| {
                    DbError::Pipeline(format!(
                        "temporal join [{}] local key encoding: {error}",
                        self.name
                    ))
                })?);
                let row_vnodes = keys
                    .iter()
                    .map(|key| PartitionKeyCodecV1::vnode_for_encoded(key.data(), self.vnode_count))
                    .collect::<Vec<_>>();
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &self.local_assignment,
                    LOCAL_NODE_ID,
                )
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "temporal join [{}] local routing: {error}",
                        self.name
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(DbError::Pipeline(format!(
                        "temporal join [{}] local topology routed rows off-node",
                        self.name
                    )));
                }
                let port = matches!(side, TemporalInputSide::Right) as usize;
                for route in plan.local {
                    routed.entry(route.vnode).or_default()[port].push(RoutedTemporalBatch {
                        batch: route.batch,
                        keys: Arc::clone(&keys),
                        source_rows: route.source_rows,
                    });
                }
            }
        }
        Ok(routed)
    }

    fn prepare_vnode(&mut self, vnode: u32, accounted_total: &mut usize) -> Result<usize, DbError> {
        let index = vnode as usize;
        if self.vnode_states[index].is_none() {
            let shard_limit = self
                .max_managed_state_bytes
                .checked_sub(*accounted_total)
                .and_then(|limit| limit.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
                .filter(|limit| *limit != 0)
                .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                    context: format!("temporal join [{}] vnode {vnode}", self.name),
                    accounted_bytes: *accounted_total,
                    limit_bytes: self.max_managed_state_bytes,
                })?;
            let config = self.state_config(vnode, shard_limit)?;
            let mut state = TemporalJoinVnodeState::try_new(
                Arc::clone(&self.left_schema),
                Arc::clone(&self.right_schema),
                config,
            )?;
            state.advance_left_frontier(self.frontiers[0].watermark, self.frontiers[0].idle)?;
            state.advance_right_frontier(self.frontiers[1].watermark, self.frontiers[1].idle)?;
            let state_bytes = state.accounted_state_bytes();
            let retained_state_bytes = self
                .retained_state_bytes
                .checked_add(state_bytes)
                .ok_or_else(|| self.accounting_error())?;
            let next_total = accounted_total
                .checked_add(state_bytes)
                .ok_or_else(|| self.accounting_error())?;
            self.retained_state_bytes = retained_state_bytes;
            *accounted_total = next_total;
            self.vnode_states[index] = Some(Box::new(state));
            self.add_resident_vnode(vnode);
            self.dirty_vnodes[index] = true;
        }
        let current = self.vnode_states[index]
            .as_ref()
            .expect("temporal vnode state initialized")
            .accounted_state_bytes();
        let other = accounted_total
            .checked_sub(current)
            .ok_or_else(|| self.accounting_error())?;
        let shard_limit = self
            .max_managed_state_bytes
            .checked_sub(other)
            .and_then(|limit| limit.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
            .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode {vnode}", self.name),
                accounted_bytes: *accounted_total,
                limit_bytes: self.max_managed_state_bytes,
            })?;
        let state = self.vnode_states[index]
            .as_mut()
            .expect("temporal vnode state initialized");
        state.set_retained_byte_limit(shard_limit)?;
        Ok(current)
    }

    fn refresh_vnode_accounting(
        &mut self,
        vnode: u32,
        previous: usize,
        accounted_total: &mut usize,
    ) -> Result<(), DbError> {
        let current = self.vnode_states[vnode as usize]
            .as_ref()
            .expect("temporal vnode state initialized")
            .accounted_state_bytes();
        let retained_state_bytes = self
            .retained_state_bytes
            .checked_sub(previous)
            .and_then(|bytes| bytes.checked_add(current))
            .ok_or_else(|| self.accounting_error())?;
        self.retained_state_bytes = retained_state_bytes;
        self.refresh_vnode_pending_hold(vnode)?;
        let next_total = self.checked_accounted_state_bytes()?;
        if next_total > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode {vnode}", self.name),
                accounted_bytes: next_total,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        *accounted_total = next_total;
        Ok(())
    }

    fn apply_right_batches(
        &mut self,
        vnode: u32,
        batches: &[RoutedTemporalBatch],
        accounted_total: &mut usize,
    ) -> Result<bool, DbError> {
        if batches.is_empty() {
            return Ok(false);
        }
        let mut applied = false;
        for routed in batches {
            let operations = source_mutations_routed(&routed.batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] routed right mutations: {error}",
                    self.name
                ))
            });
            let operations = match operations {
                Ok(operations) => operations,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            let positioned = strip_source_mutations_routed(&routed.batch).map_err(|error| {
                DbError::SchemaMismatch(format!(
                    "temporal join [{}] routed right mutations: {error}",
                    self.name
                ))
            });
            let positioned = match positioned {
                Ok(positioned) => positioned,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            let previous = self.prepare_vnode(vnode, accounted_total)?;
            let result = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("temporal vnode state initialized")
                .apply_right_batch_routed(
                    &positioned,
                    operations,
                    &routed.keys,
                    &routed.source_rows,
                );
            if let Err(error) = result {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            applied = true;
            self.dirty_vnodes[vnode as usize] = true;
            self.refresh_vnode_accounting(vnode, previous, accounted_total)
                .map_err(|error| self.after_apply_error(applied, vnode, error))?;
        }
        Ok(true)
    }

    fn apply_left_batches(
        &mut self,
        vnode: u32,
        batches: &[RoutedTemporalBatch],
        accounted_total: &mut usize,
    ) -> Result<(bool, Vec<RecordBatch>), DbError> {
        if batches.is_empty() {
            return Ok((false, Vec::new()));
        }
        let mut output = Vec::new();
        let mut applied = false;
        for routed in batches {
            let previous = self.prepare_vnode(vnode, accounted_total)?;
            let result = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("temporal vnode state initialized")
                .probe_left_batch_routed(&routed.batch, &routed.keys, &routed.source_rows);
            let result = match result {
                Ok(result) => result,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            };
            applied = true;
            if result.num_rows() != 0 {
                output.push(result);
            }
            self.dirty_vnodes[vnode as usize] = true;
            self.refresh_vnode_accounting(vnode, previous, accounted_total)
                .map_err(|error| self.after_apply_error(applied, vnode, error))?;
        }
        Ok((true, output))
    }

    fn apply_frontiers(
        &mut self,
        next: [InputFrontier; 2],
        accounted_total: &mut usize,
    ) -> Result<bool, DbError> {
        validate_frontier(self.frontiers[0], next[0], "left", &self.name)?;
        validate_frontier(self.frontiers[1], next[1], "right", &self.name)?;
        if self.pending_frontiers.is_none() && self.frontiers == next {
            return Ok(false);
        }
        if let Some(pending) = self.pending_frontiers {
            validate_frontier(pending[0], next[0], "left", &self.name)?;
            validate_frontier(pending[1], next[1], "right", &self.name)?;
        } else {
            self.pending_frontiers = Some(next);
            self.frontier_cursor = 0;
            self.frontier_remaining = self.resident_vnodes.len();
            self.frontier_has_work = false;
        }
        let target = self.pending_frontiers.expect("staged temporal frontiers");
        let resident_count = self.resident_vnodes.len();
        if self.frontier_remaining == 0 {
            self.frontiers = target;
            self.pending_frontiers = None;
            return Ok(true);
        }
        let visit_limit = self
            .limits
            .maintenance_vnode_budget
            .get()
            .min(self.frontier_remaining)
            .min(resident_count);
        let cursor = u32::try_from(self.frontier_cursor).map_err(|_| {
            DbError::Pipeline(format!(
                "temporal join [{}] frontier cursor exceeds u32",
                self.name
            ))
        })?;
        let mut roster_index = self
            .resident_vnodes
            .partition_point(|resident| *resident < cursor);
        if roster_index == resident_count {
            roster_index = 0;
        }
        let mut applied = false;
        for _ in 0..visit_limit {
            let vnode = self.resident_vnodes[roster_index];
            roster_index += 1;
            if roster_index == resident_count {
                roster_index = 0;
            }
            let previous = match self.prepare_vnode(vnode, accounted_total) {
                Ok(previous) => previous,
                Err(error) => {
                    return Err(self.after_apply_error(applied, vnode, error));
                }
            };
            let left = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("resident state")
                .advance_left_frontier(target[0].watermark, target[0].idle);
            if let Err(error) = left {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            applied = true;
            self.dirty_vnodes[vnode as usize] = true;
            let right = self.vnode_states[vnode as usize]
                .as_mut()
                .expect("resident state")
                .advance_right_frontier(target[1].watermark, target[1].idle);
            if let Err(error) = right {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            let state = self.vnode_states[vnode as usize]
                .as_ref()
                .expect("resident state");
            let has_work = state.has_ready_probes() || state.has_history_gc_work();
            if let Err(error) = self.refresh_vnode_accounting(vnode, previous, accounted_total) {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            self.frontier_has_work |= has_work;
            self.frontier_cursor = (vnode as usize + 1) % self.vnode_states.len();
            self.frontier_remaining -= 1;
        }
        if self.frontier_remaining == 0 {
            self.frontiers = target;
            self.pending_frontiers = None;
            self.maintenance_pending = self.frontier_has_work;
            self.maintenance_remaining = if self.frontier_has_work {
                resident_count
            } else {
                0
            };
            self.maintenance_rescan = false;
            self.frontier_has_work = false;
        }
        Ok(true)
    }

    fn drain_maintenance(
        &mut self,
        accounted_total: &mut usize,
    ) -> Result<(bool, Vec<RecordBatch>), DbError> {
        let resident_count = self.resident_vnodes.len();
        if resident_count == 0 || !self.maintenance_pending {
            self.maintenance_pending = false;
            self.maintenance_remaining = 0;
            self.maintenance_rescan = false;
            return Ok((false, Vec::new()));
        }
        if self.maintenance_remaining == 0 {
            if self.maintenance_rescan {
                self.maintenance_remaining = resident_count;
                self.maintenance_rescan = false;
            } else {
                self.maintenance_pending = false;
                return Ok((false, Vec::new()));
            }
        }
        let mut ready = self.limits.ready_probe_budget.get();
        let mut gc = self.limits.history_gc_budget.get();
        let visit_limit = self
            .limits
            .maintenance_vnode_budget
            .get()
            .min(self.maintenance_remaining)
            .min(resident_count);
        let cursor = u32::try_from(self.maintenance_cursor).map_err(|_| {
            DbError::Pipeline(format!(
                "temporal join [{}] maintenance cursor exceeds u32",
                self.name
            ))
        })?;
        let mut roster_index = self
            .resident_vnodes
            .partition_point(|resident| *resident < cursor);
        if roster_index == resident_count {
            roster_index = 0;
        }
        let mut visited = 0usize;
        let mut changed = false;
        let mut applied = false;
        let mut output = Vec::new();
        while visited < visit_limit && ready != 0 && gc != 0 {
            let vnode = self.resident_vnodes[roster_index];
            roster_index += 1;
            if roster_index == resident_count {
                roster_index = 0;
            }
            let previous = match self.prepare_vnode(vnode, accounted_total) {
                Ok(previous) => previous,
                Err(error) => {
                    return Err(self.after_apply_error(applied, vnode, error));
                }
            };
            let mut vnode_changed = false;
            let mut vnode_has_more = false;
            if ready != 0 {
                let drained = self.vnode_states[vnode as usize]
                    .as_mut()
                    .expect("resident state")
                    .drain_ready_probes(NonZeroUsize::new(ready).expect("positive ready budget"));
                let drained = match drained {
                    Ok(drained) => drained,
                    Err(error) => {
                        return Err(self.after_apply_error(applied, vnode, error));
                    }
                };
                ready -= drained.drained_probes;
                vnode_has_more |= drained.has_more;
                if drained.drained_probes != 0 {
                    vnode_changed = true;
                    applied = true;
                    if drained.output.num_rows() != 0 {
                        output.push(drained.output);
                    }
                }
            } else if self.vnode_states[vnode as usize]
                .as_ref()
                .expect("resident state")
                .has_ready_probes()
            {
                vnode_has_more = true;
            }
            if gc != 0 {
                let drained = self.vnode_states[vnode as usize]
                    .as_mut()
                    .expect("resident state")
                    .drain_history_gc(NonZeroUsize::new(gc).expect("positive GC budget"));
                let drained = match drained {
                    Ok(drained) => drained,
                    Err(error) => {
                        return Err(self.after_apply_error(applied, vnode, error));
                    }
                };
                gc -= drained.steps;
                vnode_has_more |= drained.has_more;
                if drained.steps != 0 {
                    vnode_changed = true;
                    applied = true;
                }
            } else if self.vnode_states[vnode as usize]
                .as_ref()
                .expect("resident state")
                .has_history_gc_work()
            {
                vnode_has_more = true;
            }
            if vnode_changed {
                self.dirty_vnodes[vnode as usize] = true;
                changed = true;
            }
            if let Err(error) = self.refresh_vnode_accounting(vnode, previous, accounted_total) {
                return Err(self.after_apply_error(applied, vnode, error));
            }
            self.maintenance_rescan |= vnode_has_more;
            self.maintenance_cursor = (vnode as usize + 1) % self.vnode_states.len();
            self.maintenance_remaining -= 1;
            visited += 1;
        }
        if self.maintenance_remaining == 0 {
            if self.maintenance_rescan {
                self.maintenance_remaining = resident_count;
                self.maintenance_rescan = false;
            } else {
                self.maintenance_pending = false;
            }
        }
        Ok((changed, output))
    }

    fn current_watermark_hold(&self) -> Option<i64> {
        let frontier_hold = self.frontiers[0].watermark.map(|watermark| {
            let left_floor = watermark.saturating_sub(self.limits.left_allowed_lateness_ms);
            left_floor.min(left_floor.saturating_add(self.minimum_probe_offset))
        });
        let pending_hold = self
            .pending_hold_counts
            .first_key_value()
            .map(|(hold, _)| *hold);
        let staged_hold = self
            .pending_frontiers
            .map(|_| self.published_output_watermark.unwrap_or(i64::MIN));
        [frontier_hold, pending_hold, staged_hold]
            .into_iter()
            .flatten()
            .min()
    }

    fn record_published_output_watermark(&mut self, input_frontiers: &[InputFrontier]) {
        let mut output = merge_input_frontiers(input_frontiers, i64::MIN);
        if let Some(hold) = self.current_watermark_hold() {
            output.watermark = output.watermark.map(|watermark| watermark.min(hold));
        }
        if let Some(watermark) = output.watermark.filter(|watermark| *watermark != i64::MIN) {
            self.published_output_watermark = Some(
                self.published_output_watermark
                    .map_or(watermark, |published| published.max(watermark)),
            );
        }
    }

    fn process_common(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: [InputFrontier; 2],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if (self.pending_frontiers.is_some() || self.maintenance_pending)
            && inputs.iter().any(|batches| !batches.is_empty())
        {
            return Err(DbError::InvalidOperation(format!(
                "temporal join [{}] received input while bounded work was pending",
                self.name
            )));
        }
        let routed = self.route_local_inputs(inputs)?;
        validate_frontier(self.frontiers[0], frontiers[0], "left", &self.name)?;
        validate_frontier(self.frontiers[1], frontiers[1], "right", &self.name)?;
        let mut accounted = self.checked_accounted_state_bytes()?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}]", self.name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        let mut applied = false;
        for (&vnode, sides) in &routed {
            match self.apply_right_batches(vnode, &sides[1], &mut accounted) {
                Ok(changed) => applied |= changed,
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            }
        }
        let mut output = Vec::new();
        for (&vnode, sides) in &routed {
            match self.apply_left_batches(vnode, &sides[0], &mut accounted) {
                Ok((changed, batches)) => {
                    applied |= changed;
                    output.extend(batches);
                }
                Err(error) => return Err(self.after_apply_error(applied, vnode, error)),
            }
        }
        if self.pending_frontiers.is_some() || !self.maintenance_pending {
            match self.apply_frontiers(frontiers, &mut accounted) {
                Ok(changed) => applied |= changed,
                Err(error) => return Err(self.after_apply_error(applied, 0, error)),
            }
        }
        if self.pending_frontiers.is_none() && self.maintenance_pending {
            match self.drain_maintenance(&mut accounted) {
                Ok((_, batches)) => output.extend(batches),
                Err(error) => return Err(self.after_apply_error(applied, 0, error)),
            }
        }
        self.record_published_output_watermark(&frontiers);
        Ok(output)
    }

    fn after_apply_error(&self, applied: bool, vnode: u32, error: DbError) -> DbError {
        if !applied || error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
            return error;
        }
        DbError::StatefulOperatorPartialApply(format!(
            "temporal join [{}] admitted state before vnode {vnode} failed: {error}",
            self.name
        ))
    }

    fn validate_vnode_roster(
        &self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<(), DbError> {
        if vnode_count != u32::from(self.key_group_count)
            || required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] received a non-canonical vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.name
            )));
        }
        if let Some(unowned) = self
            .resident_vnodes
            .iter()
            .copied()
            .find(|vnode| required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] retained unowned vnode state {unowned}",
                self.name
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl GraphOperator for ManagedTemporalJoinOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::managed_temporal_join()
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        Some(ManagedStateAccountingSnapshot {
            live: self.checked_accounted_state_bytes().unwrap_or(usize::MAX),
            prepared: 0,
            retired: 0,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        let accounted = self.checked_accounted_state_bytes()?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] topology", self.name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let frontier = |index: usize| InputFrontier {
            watermark: watermarks
                .get(index)
                .copied()
                .filter(|watermark| *watermark != i64::MIN),
            idle: false,
        };
        self.process_common(inputs, [frontier(0), frontier(1)])
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if frontiers.len() != 2 {
            return Err(DbError::InvalidOperation(format!(
                "temporal join [{}] requires two input frontiers",
                self.name
            )));
        }
        self.process_common(inputs, [frontiers[0], frontiers[1]])
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        if self.pending_frontiers.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot checkpoint during bounded frontier fanout",
                self.name
            )));
        }
        if self.frontiers == [InputFrontier::default(); 2]
            && self.maintenance_cursor == 0
            && !self.maintenance_pending
            && self.maintenance_remaining == 0
            && !self.maintenance_rescan
            && self.published_output_watermark.is_none()
        {
            return Ok(None);
        }
        let maintenance_cursor = u32::try_from(self.maintenance_cursor).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance cursor exceeds u32",
                self.name
            ))
        })?;
        let maintenance_remaining = u32::try_from(self.maintenance_remaining).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance sweep exceeds u32",
                self.name
            ))
        })?;
        let checkpoint = TemporalJoinOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            left_watermark: self.frontiers[0].watermark,
            left_idle: self.frontiers[0].idle,
            right_watermark: self.frontiers[1].watermark,
            right_idle: self.frontiers[1].idle,
            maintenance_cursor,
            maintenance_pending: self.maintenance_pending,
            maintenance_remaining,
            maintenance_rescan: self.maintenance_rescan,
            published_output_watermark: self.published_output_watermark,
        };
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(self.max_managed_state_bytes),
        );
        let data = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "temporal join [{}] operator checkpoint: {error}",
                    self.name
                ))
            })?;
        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        if self.whole_restored
            || !self.resident_vnodes.is_empty()
            || self.frontiers != [InputFrontier::default(); 2]
            || self.pending_frontiers.is_some()
            || self.published_output_watermark.is_some()
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint was restored more than once",
                self.name
            )));
        }
        if checkpoint.data.len() > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint exceeds its restore limit",
                self.name
            )));
        }
        let checkpoint = rkyv::from_bytes::<TemporalJoinOperatorCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint: {error}",
                self.name
            ))
        })?;
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] unsupported operator checkpoint version {}",
                self.name, checkpoint.version
            )));
        }
        let cursor = usize::try_from(checkpoint.maintenance_cursor).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance cursor exceeds usize",
                self.name
            ))
        })?;
        if cursor >= self.vnode_states.len() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] maintenance cursor {cursor} is outside its vnode table",
                self.name
            )));
        }
        let remaining = usize::try_from(checkpoint.maintenance_remaining).map_err(|_| {
            DbError::Checkpoint(format!(
                "temporal join [{}] maintenance sweep exceeds usize",
                self.name
            ))
        })?;
        if remaining > self.vnode_states.len()
            || (!checkpoint.maintenance_pending
                && (remaining != 0 || checkpoint.maintenance_rescan))
            || (checkpoint.maintenance_pending && remaining == 0 && !checkpoint.maintenance_rescan)
        {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] operator checkpoint has invalid maintenance state",
                self.name
            )));
        }
        self.frontiers = [
            InputFrontier {
                watermark: checkpoint.left_watermark,
                idle: checkpoint.left_idle,
            },
            InputFrontier {
                watermark: checkpoint.right_watermark,
                idle: checkpoint.right_idle,
            },
        ];
        self.maintenance_cursor = cursor;
        self.maintenance_pending = checkpoint.maintenance_pending;
        self.maintenance_remaining = remaining;
        self.maintenance_rescan = checkpoint.maintenance_rescan;
        self.published_output_watermark = checkpoint.published_output_watermark;
        self.whole_restored = true;
        Ok(())
    }

    fn wants_input(&self) -> bool {
        self.pending_frontiers.is_none() && !self.maintenance_pending
    }

    fn checkpoint_drain_pending(&self) -> bool {
        self.pending_frontiers.is_some()
    }

    fn watermark_hold(&self) -> Option<i64> {
        self.current_watermark_hold()
    }

    #[cfg(feature = "cluster")]
    fn restored_output_watermark(&self) -> Option<i64> {
        self.published_output_watermark
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        if self.pending_frontiers.is_some() {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] cannot capture vnodes during bounded frontier fanout",
                self.name
            )));
        }
        self.validate_vnode_roster(required_vnodes, vnode_count)?;
        let capture: Vec<bool> = required_vnodes
            .iter()
            .map(|vnode| {
                !self.checkpointed_vnodes[*vnode as usize] || self.dirty_vnodes[*vnode as usize]
            })
            .collect();
        let mut encoded_total = 0usize;
        let mut result = Vec::with_capacity(required_vnodes.len());
        for (&vnode, include) in required_vnodes.iter().zip(&capture) {
            let state = if *include {
                let remaining = self
                    .max_managed_state_bytes
                    .checked_sub(encoded_total)
                    .filter(|remaining| *remaining != 0)
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "temporal join [{}] vnode checkpoint limit exhausted",
                            self.name
                        ))
                    })?;
                let bytes = if let Some(state) = self.vnode_states[vnode as usize].as_ref() {
                    let mut bytes = Vec::with_capacity(remaining.min(4096));
                    bytes.push(PRESENT_VNODE);
                    let payload_budget = remaining
                        .checked_sub(1)
                        .filter(|budget| *budget != 0)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "temporal join [{}] vnode {vnode} checkpoint header exhausted its limit",
                                self.name
                            ))
                        })?;
                    bytes.extend(state.checkpoint(payload_budget)?);
                    bytes
                } else {
                    vec![ABSENT_VNODE]
                };
                encoded_total = encoded_total.checked_add(bytes.len()).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "temporal join [{}] vnode checkpoint accounting overflow",
                        self.name
                    ))
                })?;
                Some(bytes::Bytes::from(bytes))
            } else {
                None
            };
            result.push(CapturedVnodeState { vnode, state });
        }
        for (&vnode, include) in required_vnodes.iter().zip(capture) {
            self.checkpointed_vnodes[vnode as usize] = true;
            if include {
                self.dirty_vnodes[vnode as usize] = false;
            }
        }
        Ok(Some(result))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, bytes: &[u8]) -> Result<(), DbError> {
        if vnode_count != u32::from(self.key_group_count) || vnode >= vnode_count {
            return Err(DbError::Checkpoint(format!(
                "temporal join [{}] vnode {vnode} restore does not match its {vnode_count}-vnode topology",
                self.name
            )));
        }
        let (&tag, payload) = bytes.split_first().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "temporal join [{}] vnode {vnode} frame is empty",
                self.name
            ))
        })?;
        let current_bytes = self.vnode_states[vnode as usize]
            .as_ref()
            .map_or(0, |state| state.accounted_state_bytes());
        let replacement = match tag {
            ABSENT_VNODE if payload.is_empty() => None,
            ABSENT_VNODE => {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] absent vnode {vnode} frame has a payload",
                    self.name
                )))
            }
            PRESENT_VNODE if !payload.is_empty() => {
                let total = self.checked_accounted_state_bytes()?;
                let other = total
                    .checked_sub(current_bytes)
                    .ok_or_else(|| self.accounting_error())?;
                let limit = self
                    .max_managed_state_bytes
                    .checked_sub(other)
                    .and_then(|limit| limit.checked_sub(PENDING_HOLD_ENTRY_CHARGE))
                    .filter(|limit| *limit != 0)
                    .ok_or_else(|| DbError::ManagedStateBudgetExceeded {
                        context: format!("temporal join [{}] vnode {vnode} restore", self.name),
                        accounted_bytes: total,
                        limit_bytes: self.max_managed_state_bytes,
                    })?;
                let config = self.state_config(vnode, limit)?;
                let state = TemporalJoinVnodeState::restore(
                    Arc::clone(&self.left_schema),
                    Arc::clone(&self.right_schema),
                    config,
                    payload,
                )?;
                let (left_watermark, left_idle, right_watermark, right_idle) =
                    state.frontier_snapshot();
                if self.whole_restored
                    && !self.maintenance_pending
                    && (state.has_ready_probes() || state.has_history_gc_work())
                {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] whole checkpoint omits restored maintenance work",
                        self.name
                    )));
                }
                let restored = [
                    InputFrontier {
                        watermark: left_watermark,
                        idle: left_idle,
                    },
                    InputFrontier {
                        watermark: right_watermark,
                        idle: right_idle,
                    },
                ];
                if self.whole_restored && self.frontiers != restored {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] whole and vnode frontiers disagree",
                        self.name
                    )));
                }
                if self
                    .restored_frontiers
                    .is_some_and(|expected| expected != restored)
                {
                    return Err(DbError::Checkpoint(format!(
                        "temporal join [{}] restored vnode frontiers disagree",
                        self.name
                    )));
                }
                self.restored_frontiers = Some(restored);
                self.frontiers = restored;
                Some(Box::new(state))
            }
            PRESENT_VNODE => {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] present vnode {vnode} frame has no payload",
                    self.name
                )))
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "temporal join [{}] vnode {vnode} frame has unknown tag {tag}",
                    self.name
                )))
            }
        };
        let replacement_bytes = replacement
            .as_ref()
            .map_or(0, |state| state.accounted_state_bytes());
        self.retained_state_bytes = self
            .retained_state_bytes
            .checked_sub(current_bytes)
            .and_then(|bytes| bytes.checked_add(replacement_bytes))
            .ok_or_else(|| self.accounting_error())?;
        let present = replacement.is_some();
        self.vnode_states[vnode as usize] = replacement;
        if present {
            self.add_resident_vnode(vnode);
        } else {
            self.remove_resident_vnode(vnode);
        }
        self.refresh_vnode_pending_hold(vnode)?;
        let accounted = self.checked_accounted_state_bytes()?;
        if accounted > self.max_managed_state_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join [{}] vnode {vnode} restore", self.name),
                accounted_bytes: accounted,
                limit_bytes: self.max_managed_state_bytes,
            });
        }
        self.checkpointed_vnodes[vnode as usize] = false;
        self.dirty_vnodes[vnode as usize] = false;
        if !self.whole_restored {
            let restored_has_work = self.vnode_states[vnode as usize]
                .as_ref()
                .is_some_and(|state| state.has_ready_probes() || state.has_history_gc_work());
            self.maintenance_pending |= restored_has_work;
            if self.maintenance_pending {
                self.maintenance_remaining = self.resident_vnodes.len();
                self.maintenance_rescan = false;
            }
        }
        Ok(())
    }

    fn force_full_vnode_capture(&mut self) {
        self.checkpointed_vnodes.fill(false);
    }
}

fn column_index(
    schema: &SchemaRef,
    column: &str,
    operator: &str,
    role: &str,
) -> Result<usize, DbError> {
    schema.index_of(column).map_err(|error| {
        DbError::Config(format!(
            "temporal join [{operator}] {role} column '{column}': {error}"
        ))
    })
}

fn validate_frontier(
    previous: InputFrontier,
    next: InputFrontier,
    side: &str,
    operator: &str,
) -> Result<(), DbError> {
    if previous.watermark.is_some() && next.watermark.is_none() {
        return Err(DbError::Pipeline(format!(
            "temporal join [{operator}] {side} frontier became uninitialized"
        )));
    }
    if let (Some(previous), Some(next)) = (previous.watermark, next.watermark) {
        if next < previous {
            return Err(DbError::Pipeline(format!(
                "temporal join [{operator}] {side} frontier regressed from {previous} to {next}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BinaryArray, Int64Array, StringArray, TimestampMillisecondArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceBatch, SourceMutation, SourceRowPositionCapability, SourceRowPositions,
    };
    use laminar_sql::temporal::{TemporalJoinKind, TemporalProbeSchedule};

    fn visible_schemas() -> (SchemaRef, SchemaRef) {
        let left = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new(
                "trade_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("trade_id", DataType::Int64, false),
        ]));
        let right = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("venue", DataType::Utf8, false),
            Field::new(
                "quote_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Utf8, false),
        ]));
        (left, right)
    }

    fn limits(ready_probe_budget: usize) -> TemporalJoinExecutionLimits {
        TemporalJoinExecutionLimits {
            left_allowed_lateness_ms: 0,
            right_allowed_lateness_ms: 0,
            history_retention_ms: 10_000,
            max_pending_probes: 100,
            ready_probe_budget: NonZeroUsize::new(ready_probe_budget).unwrap(),
            history_gc_budget: NonZeroUsize::new(8).unwrap(),
            maintenance_vnode_budget: NonZeroUsize::new(1).unwrap(),
        }
    }

    fn config() -> TemporalJoinTranslatorConfig {
        TemporalJoinTranslatorConfig {
            left_table: "trades".into(),
            right_table: "quotes".into(),
            left_key_columns: vec!["symbol".into(), "venue".into()],
            right_key_columns: vec!["symbol".into(), "venue".into()],
            left_time_column: "trade_time".into(),
            right_time_column: "quote_time".into(),
            join_kind: TemporalJoinKind::Left,
            probe_schedule: TemporalProbeSchedule::as_of(),
            probe_alias: None,
        }
    }

    fn operator(ready_probe_budget: usize) -> (ManagedTemporalJoinOperator, SchemaRef, SchemaRef) {
        let (left_visible, right_visible) = visible_schemas();
        let left = schema_with_source_row_positions(&left_visible).unwrap();
        let right = schema_with_source_row_positions(&right_visible).unwrap();
        let operator = ManagedTemporalJoinOperator::try_new(
            "temporal",
            config(),
            Arc::clone(&left),
            Arc::clone(&right),
            KeyGroupCount::try_from(2_u16).unwrap(),
            limits(ready_probe_budget),
        )
        .unwrap();
        (operator, left, right)
    }

    fn positions(rows: usize, first: u64) -> SourceRowPositions {
        let partitions = std::iter::repeat_n(b"p0".as_slice(), rows);
        let orders: Vec<[u8; 8]> = (first..first + rows as u64).map(u64::to_be_bytes).collect();
        SourceRowPositions::try_new(
            BinaryArray::from_iter_values(partitions),
            BinaryArray::from_iter_values(orders.iter()),
            UInt32Array::from(vec![0; rows]),
        )
        .unwrap()
    }

    fn left_batch(keys: &[String], venues: &[&str], times: &[i64], ids: &[i64]) -> RecordBatch {
        let (visible, _) = visible_schemas();
        let rows = RecordBatch::try_new(
            Arc::clone(&visible),
            vec![
                Arc::new(StringArray::from_iter_values(keys)),
                Arc::new(StringArray::from(venues.to_vec())),
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(ids.to_vec())),
            ],
        )
        .unwrap();
        let positioned = schema_with_source_row_positions(&visible).unwrap();
        let mutations = schema_with_source_mutations_and_row_positions(&visible).unwrap();
        SourceBatch::positioned(rows, positions(keys.len(), 100))
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::Deterministic,
                &positioned,
                &mutations,
            )
            .unwrap()
    }

    fn right_batch(
        keys: &[String],
        venues: &[&str],
        times: &[i64],
        values: &[&str],
        mutations: &[SourceMutation],
    ) -> RecordBatch {
        let (_, visible) = visible_schemas();
        let rows = RecordBatch::try_new(
            Arc::clone(&visible),
            vec![
                Arc::new(StringArray::from_iter_values(keys)),
                Arc::new(StringArray::from(venues.to_vec())),
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(StringArray::from(values.to_vec())),
            ],
        )
        .unwrap();
        let positioned = schema_with_source_row_positions(&visible).unwrap();
        let mutation_schema = schema_with_source_mutations_and_row_positions(&visible).unwrap();
        SourceBatch::positioned(rows, positions(keys.len(), 1))
            .unwrap()
            .with_mutations(mutations.to_vec())
            .unwrap()
            .into_records_with_metadata(
                SourceRowPositionCapability::Deterministic,
                &positioned,
                &mutation_schema,
            )
            .unwrap()
    }

    fn key_for_vnode(target: u32) -> String {
        for candidate in 0..1_000 {
            let key = format!("key-{candidate}");
            let batch = left_batch(std::slice::from_ref(&key), &["X"], &[0], &[0]);
            if laminar_core::shuffle::row_vnodes(&batch, &[0, 1], 2).unwrap() == [target] {
                return key;
            }
        }
        panic!("could not find key for vnode {target}");
    }

    fn frontier(watermark: i64) -> [InputFrontier; 2] {
        [
            InputFrontier {
                watermark: Some(watermark),
                idle: false,
            },
            InputFrontier {
                watermark: Some(watermark),
                idle: false,
            },
        ]
    }

    #[tokio::test]
    async fn local_vnodes_share_one_bounded_path_for_asof_and_tombstones() {
        let key0 = key_for_vnode(0);
        let key1 = key_for_vnode(1);
        let (mut operator, _, _) = operator(1);
        let right = right_batch(
            &[key0.clone(), key0.clone(), key1.clone(), key0.clone()],
            &["X", "X", "X", "Y"],
            &[90, 110, 95, 100],
            &["old", "deleted", "live", "other-venue"],
            &[
                SourceMutation::Put,
                SourceMutation::Tombstone,
                SourceMutation::Put,
                SourceMutation::Put,
            ],
        );
        let left = left_batch(
            &[key0.clone(), key0.clone(), key1, key0],
            &["X", "X", "X", "Y"],
            &[100, 120, 120, 120],
            &[1, 2, 3, 4],
        );
        let fronts = frontier(200);
        let mut output = operator
            .process_with_frontiers(&[vec![left], vec![right]], &fronts)
            .await
            .unwrap();
        assert!(operator.checkpoint_drain_pending());
        let advanced = frontier(250);
        let drained = operator
            .process_with_frontiers(&[], &advanced)
            .await
            .unwrap();
        assert!(drained.iter().map(RecordBatch::num_rows).sum::<usize>() <= 1);
        output.extend(drained);
        while !operator.wants_input() {
            let drained = operator
                .process_with_frontiers(&[], &advanced)
                .await
                .unwrap();
            assert!(drained.iter().map(RecordBatch::num_rows).sum::<usize>() <= 1);
            output.extend(drained);
        }
        assert!(!operator.checkpoint_drain_pending());
        assert_eq!(operator.frontiers, fronts);
        output.extend(
            operator
                .process_with_frontiers(&[], &advanced)
                .await
                .unwrap(),
        );
        assert!(operator.checkpoint_drain_pending());
        while !operator.wants_input() {
            output.extend(
                operator
                    .process_with_frontiers(&[], &advanced)
                    .await
                    .unwrap(),
            );
        }
        assert!(operator.vnode_states.iter().all(Option::is_some));
        assert_eq!(operator.resident_vnodes, [0, 1]);
        assert_eq!(operator.frontiers, advanced);
        assert_eq!(operator.watermark_hold(), Some(250));

        let mut actual = BTreeMap::new();
        for batch in output {
            let ids = batch
                .column(batch.schema().index_of("trade_id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(batch.schema().index_of("value_quotes").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.insert(
                    ids.value(row),
                    (!values.is_null(row)).then(|| values.value(row).to_owned()),
                );
            }
        }
        assert_eq!(
            actual,
            BTreeMap::from([
                (1, Some("old".into())),
                (2, None),
                (3, Some("live".into())),
                (4, Some("other-venue".into())),
            ])
        );

        #[cfg(feature = "cluster")]
        {
            let (mut restored_cut, _, _) = self::operator(1);
            restored_cut.frontiers = [
                InputFrontier {
                    watermark: Some(100),
                    idle: false,
                },
                InputFrontier {
                    watermark: Some(50),
                    idle: true,
                },
            ];
            let restored_frontiers = restored_cut.frontiers;
            restored_cut.record_published_output_watermark(&restored_frontiers);
            let checkpoint = restored_cut.checkpoint().unwrap().unwrap();
            let (mut recovered, _, _) = self::operator(1);
            recovered.restore(checkpoint).unwrap();
            assert_eq!(recovered.restored_output_watermark(), Some(100));
        }

        let (_, left_schema, right_schema) = self::operator(1);
        let mut negative_config = config();
        negative_config.probe_schedule = TemporalProbeSchedule::list(vec![-50, 0]).unwrap();
        negative_config.probe_alias = Some("probe".into());
        let mut negative = ManagedTemporalJoinOperator::try_new(
            "negative",
            negative_config,
            left_schema,
            right_schema,
            KeyGroupCount::try_from(2_u16).unwrap(),
            limits(1),
        )
        .unwrap();
        negative.frontiers[0] = InputFrontier {
            watermark: Some(100),
            idle: false,
        };
        assert_eq!(negative.watermark_hold(), Some(50));
    }

    #[tokio::test]
    async fn vnode_frames_preserve_absence_and_force_full_capture() {
        let key = key_for_vnode(1);
        let (mut donor, _, _) = operator(8);
        let right = right_batch(
            std::slice::from_ref(&key),
            &["X"],
            &[90],
            &["live"],
            &[SourceMutation::Put],
        );
        donor
            .process_with_frontiers(&[Vec::new(), vec![right]], &[InputFrontier::default(); 2])
            .await
            .unwrap();
        donor.maintenance_cursor = 1;
        let whole = donor.checkpoint().unwrap().unwrap();
        let captured = donor.checkpoint_vnodes(&[0, 1], 2).unwrap().unwrap();
        assert_eq!(captured[0].state.as_deref().unwrap(), &[ABSENT_VNODE]);
        assert_eq!(captured[1].state.as_deref().unwrap()[0], PRESENT_VNODE);
        assert!(donor
            .checkpoint_vnodes(&[0, 1], 2)
            .unwrap()
            .unwrap()
            .iter()
            .all(|frame| frame.state.is_none()));

        let (mut restored, _, _) = operator(8);
        restored.restore(whole).unwrap();
        assert_eq!(restored.maintenance_cursor, 1);
        for frame in &captured {
            restored
                .restore_vnode(frame.vnode, 2, frame.state.as_deref().unwrap())
                .unwrap();
        }
        assert!(restored.vnode_states[0].is_none());
        assert!(restored.vnode_states[1].is_some());
        restored.force_full_vnode_capture();
        let recaptured = restored.checkpoint_vnodes(&[0, 1], 2).unwrap().unwrap();
        assert!(recaptured.iter().all(|frame| frame.state.is_some()));

        let left = left_batch(std::slice::from_ref(&key), &["X"], &[100], &[7]);
        let output = restored
            .process_with_frontiers(&[vec![left], Vec::new()], &frontier(200))
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        let value = output[0]
            .column(output[0].schema().index_of("value_quotes").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(value.value(0), "live");
    }
}
