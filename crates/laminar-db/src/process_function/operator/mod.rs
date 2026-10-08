use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::DataType;
use laminar_core::state::PartitionKeyCodecV1;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use super::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessHandler, TimerOperation, ValueMutation, ValueState,
};
use crate::error::DbError;

const KEY_CHARGE: usize = 96;
const TIMER_CHARGE: usize = 96;

#[derive(Clone, Default, Deserialize, Serialize)]
struct KeyState {
    key_text: String,
    value: ValueState,
    timers: BTreeMap<String, RegisteredTimer>,
}

impl KeyState {
    fn is_empty(&self) -> bool {
        self.value == ValueState::Absent && self.timers.is_empty()
    }

    fn view(&self) -> ValueState {
        self.value
    }
}

#[derive(Clone, Copy, Deserialize, Serialize)]
struct RegisteredTimer {
    at_us: i64,
    generation: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OperatorFrame {
    codec: u32,
    descriptor_sha256: String,
    partitioning_abi: u16,
    vnode_count: u32,
    next_activation_id: u64,
    next_timer_generation: u64,
    watermark_us: i64,
    #[cfg(feature = "cluster")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activation_id_abi: Option<u16>,
    #[cfg(feature = "cluster")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    shuffle: Option<execution::shuffle::Checkpoint>,
}

#[derive(Deserialize, Serialize)]
struct VnodeFrame {
    codec: u32,
    vnode: u32,
    #[cfg(feature = "cluster")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activation_sequence: Option<u64>,
    entries: Vec<(Vec<u8>, KeyState)>,
}

#[derive(Serialize)]
struct VnodeCapture<'a> {
    codec: u32,
    vnode: u32,
    #[cfg(feature = "cluster")]
    #[serde(skip_serializing_if = "Option::is_none")]
    activation_sequence: Option<u64>,
    entries: Vec<(&'a Vec<u8>, &'a KeyState)>,
}

type DueTimer = (i64, Vec<u8>, String, u64);

struct StagedKey {
    vnode: usize,
    key: Vec<u8>,
    before: KeyState,
    after: KeyState,
}

struct StagedResponse {
    keys: Vec<StagedKey>,
    output: Vec<RecordBatch>,
    output_rows: usize,
    output_bytes: usize,
    live_bytes: usize,
    key_count: usize,
    timer_count: usize,
    next_generation: u64,
}

/// Vnode-partitioned authoritative state for one process function.
pub(crate) struct ProcessFunctionOperator {
    descriptor: ProcessFunctionDescriptor,
    descriptor_sha256: String,
    handler: ProcessHandler,
    #[cfg(feature = "process-remote")]
    remote: Option<remote::RemoteExecution>,
    key_codec: PartitionKeyCodecV1,
    key_indices: Vec<usize>,
    time_index: usize,
    output_time_index: usize,
    vnode_count: NonZeroU32,
    state: Vec<FxHashMap<Vec<u8>, KeyState>>,
    #[cfg(feature = "cluster")]
    activation_sequences: Vec<u64>,
    due: BTreeSet<DueTimer>,
    next_activation_id: u64,
    next_timer_generation: u64,
    watermark_us: i64,
    live_bytes: usize,
    key_count: usize,
    timer_count: usize,
    graph_budget: usize,
    metadata_restored: bool,
    #[cfg(feature = "cluster")]
    assignment_fence: Option<laminar_core::checkpoint::CheckpointAssignmentFence>,
    #[cfg(feature = "cluster")]
    vnode_transition: transition::ProcessVnodeTransition,
    #[cfg(feature = "cluster")]
    execution: execution::ProcessExecution,
    #[cfg(feature = "cluster")]
    shuffle: execution::shuffle::ShuffleState,
}

impl ProcessFunctionOperator {
    #[cfg(feature = "benchmark-internals")]
    pub(crate) fn accepted_activations(&self) -> u64 {
        self.next_activation_id
    }

    pub(crate) fn new(
        descriptor: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
        vnode_count: u32,
    ) -> Result<Self, DbError> {
        if descriptor.runtime != super::ProcessRuntime::NativeRust {
            return Err(DbError::Unsupported(
                "native process operator requires the trusted native Rust runtime".into(),
            ));
        }
        Self::build(descriptor, ProcessHandler::Native(handler), vnode_count)
    }

    #[cfg(feature = "process-remote")]
    pub(crate) fn new_remote(
        descriptor: ProcessFunctionDescriptor,
        client: &Arc<crate::process_function::remote::RemoteProcessClient>,
        runtime: tokio::runtime::Handle,
        wake: Arc<tokio::sync::Notify>,
        operator_id: String,
        vnode_count: u32,
    ) -> Result<Self, DbError> {
        if descriptor.runtime == super::ProcessRuntime::NativeRust {
            return Err(DbError::InvalidOperation(
                "remote process operator requires a remote runtime".into(),
            ));
        }
        let mut operator = Self::build(
            descriptor,
            ProcessHandler::Remote(Arc::clone(client)),
            vnode_count,
        )?;
        operator.remote = Some(remote::RemoteExecution::new(
            runtime,
            wake,
            operator_id,
            client.max_in_flight(),
        ));
        Ok(operator)
    }

    fn build(
        descriptor: ProcessFunctionDescriptor,
        handler: ProcessHandler,
        vnode_count: u32,
    ) -> Result<Self, DbError> {
        let vnode_count = NonZeroU32::new(vnode_count)
            .ok_or_else(|| DbError::Config("process function requires nonzero vnodes".into()))?;
        let (key_codec, key_indices, time_index, output_time_index) =
            validate_descriptor(&descriptor)?;
        let descriptor_sha256 = descriptor.binding_sha256()?;
        let count = usize::try_from(vnode_count.get()).map_err(|_| {
            DbError::Config("process function vnode count exceeds addressable memory".into())
        })?;
        Ok(Self {
            descriptor,
            descriptor_sha256,
            handler,
            #[cfg(feature = "process-remote")]
            remote: None,
            key_codec,
            key_indices,
            time_index,
            output_time_index,
            vnode_count,
            state: (0..count).map(|_| FxHashMap::default()).collect(),
            #[cfg(feature = "cluster")]
            activation_sequences: Vec::new(),
            due: BTreeSet::new(),
            next_activation_id: 0,
            next_timer_generation: 0,
            watermark_us: i64::MIN,
            live_bytes: 0,
            key_count: 0,
            timer_count: 0,
            graph_budget: usize::MAX,
            metadata_restored: false,
            #[cfg(feature = "cluster")]
            assignment_fence: None,
            #[cfg(feature = "cluster")]
            vnode_transition: transition::ProcessVnodeTransition::Idle,
            #[cfg(feature = "cluster")]
            execution: execution::ProcessExecution::Local,
            #[cfg(feature = "cluster")]
            shuffle: execution::shuffle::ShuffleState::Unbound,
        })
    }

    fn state_for(&self, key: &[u8]) -> &KeyState {
        let vnode = self.vnode_for(key);
        self.state[vnode].get(key).unwrap_or(&EMPTY_KEY_STATE)
    }

    fn vnode_for(&self, key: &[u8]) -> usize {
        PartitionKeyCodecV1::vnode_for_encoded(key, self.vnode_count) as usize
    }

    fn activation_for_row(
        &self,
        batch: &RecordBatch,
        row: usize,
        key: Vec<u8>,
        event_time_us: i64,
        id: u64,
    ) -> ProcessActivation {
        let state = self.state_for(&key).view();
        let key_text = batch
            .column(self.key_indices[0])
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("validated UTF-8 process key")
            .value(row)
            .to_string();
        ProcessActivation {
            id,
            key: Arc::from(key),
            key_text,
            event_time_us,
            callback: ProcessCallback::Input(batch.slice(row, 1)),
            state,
        }
    }

    fn process_rows(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        output: &mut Vec<RecordBatch>,
        output_rows: &mut usize,
        output_bytes: &mut usize,
    ) -> Result<(), DbError> {
        let mut input_rows = 0usize;
        let mut input_bytes = 0usize;
        for batch in inputs.iter().flatten() {
            input_rows = input_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DbError::BackpressureFail("process input row count overflow".into())
            })?;
            input_bytes = input_bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| {
                    DbError::BackpressureFail("process input byte count overflow".into())
                })?;
            if input_rows > self.descriptor.limits.max_input_rows
                || input_bytes > self.descriptor.limits.max_input_bytes
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
            let columns = self
                .key_indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect::<Vec<_>>();
            let keys = self.key_codec.encode_columns(&columns).map_err(|error| {
                DbError::InvalidOperation(format!("process function key encoding: {error}"))
            })?;
            let time = batch
                .column(self.time_index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DbError::InvalidOperation("invalid event-time array".into()))?;
            let mut group = Vec::new();
            let mut group_keys = BTreeSet::new();
            for (row, key) in keys.iter().enumerate() {
                let key = key.data().to_vec();
                if self
                    .key_indices
                    .iter()
                    .any(|&index| batch.column(index).is_null(row))
                {
                    return Err(DbError::InvalidOperation(
                        "process function key contains NULL".into(),
                    ));
                }
                if time.is_null(row) || time.value(row) < self.watermark_us {
                    return Err(DbError::InvalidOperation(
                        "process function rejects null or late event time".into(),
                    ));
                }
                if group_keys.contains(&key) || group.len() == self.descriptor.limits.max_batch_rows
                {
                    self.accept_call(&mut group, output, output_rows, output_bytes)?;
                    group.clear();
                    group_keys.clear();
                }
                let id = self
                    .next_activation_id
                    .checked_add(u64::try_from(group.len()).unwrap_or(u64::MAX))
                    .ok_or_else(|| DbError::Pipeline("process activation ID exhausted".into()))?;
                group_keys.insert(key.clone());
                group.push(self.activation_for_row(batch, row, key, time.value(row), id));
            }
            if !group.is_empty() {
                self.accept_call(&mut group, output, output_rows, output_bytes)?;
            }
        }
        Ok(())
    }

    fn accept_call(
        &mut self,
        activations: &mut [ProcessActivation],
        output: &mut Vec<RecordBatch>,
        output_rows: &mut usize,
        output_bytes: &mut usize,
    ) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        self.reserve_native_activation_ids(activations)?;
        let outcome = self.invoke_and_apply(activations, output, output_rows, output_bytes);
        #[cfg(feature = "cluster")]
        if outcome.is_err() {
            self.reclaim_activation_ids(activations);
        }
        outcome
    }

    fn invoke_and_apply(
        &mut self,
        activations: &[ProcessActivation],
        output: &mut Vec<RecordBatch>,
        output_rows: &mut usize,
        output_bytes: &mut usize,
    ) -> Result<(), DbError> {
        #[cfg(feature = "cluster")]
        self.require_execution_current()?;
        let response = match &self.handler {
            ProcessHandler::Native(handler) => handler.invoke(activations)?,
            #[cfg(feature = "process-remote")]
            ProcessHandler::Remote(_) => {
                return Err(DbError::Pipeline(
                    "remote process invocation entered native execution".into(),
                ));
            }
        };
        #[cfg(feature = "cluster")]
        self.require_execution_current()?;
        if response.len() != activations.len() {
            return Err(DbError::InvalidOperation(
                "process function response count differs from activation count".into(),
            ));
        }
        let mut response_by_id = BTreeMap::new();
        for result in response {
            if response_by_id
                .insert(result.activation_id, result)
                .is_some()
            {
                return Err(DbError::InvalidOperation(
                    "process function response repeats an activation ID".into(),
                ));
            }
        }
        let mut staged = self.stage_results(
            activations,
            &mut response_by_id,
            *output_rows,
            *output_bytes,
        )?;
        let next_id = self
            .next_activation_id
            .checked_add(u64::try_from(activations.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| DbError::Pipeline("process activation ID exhausted".into()))?;
        #[cfg(feature = "cluster")]
        self.require_execution_current()?;
        self.commit_results(std::mem::take(&mut staged.keys));
        self.live_bytes = staged.live_bytes;
        self.key_count = staged.key_count;
        self.timer_count = staged.timer_count;
        self.next_timer_generation = staged.next_generation;
        self.next_activation_id = next_id;
        *output_rows = staged.output_rows;
        *output_bytes = staged.output_bytes;
        output.append(&mut staged.output);
        Ok(())
    }

    fn stage_results(
        &self,
        activations: &[ProcessActivation],
        response: &mut BTreeMap<u64, ProcessActivationResult>,
        mut rows: usize,
        mut bytes: usize,
    ) -> Result<StagedResponse, DbError> {
        let mut staged = Vec::with_capacity(activations.len());
        let mut output = Vec::new();
        let mut live_bytes = self.live_bytes;
        let mut generation = self.next_timer_generation;
        let mut key_count = self.key_count;
        let mut timer_count = self.timer_count;
        for activation in activations {
            let result = response.remove(&activation.id).ok_or_else(|| {
                DbError::InvalidOperation("process function omitted an activation ID".into())
            })?;
            let key = activation.key.to_vec();
            let before = self.state_for(&key).clone();
            let mut after = before.clone();
            after.key_text.clone_from(&activation.key_text);
            if let ProcessCallback::Timer { name } = &activation.callback {
                after.timers.remove(name);
            }
            apply_value_mutation(&mut after, result.value);
            generation = self.apply_timer_operations(&mut after, result.timers, generation)?;
            self.validate_output(activation, &result.output, &mut rows, &mut bytes)?;
            let old_charge = charged_key(&key, &before)?;
            let new_charge = charged_key(&key, &after)?;
            live_bytes = live_bytes
                .checked_sub(old_charge)
                .and_then(|n| n.checked_add(new_charge))
                .ok_or_else(|| {
                    DbError::BackpressureFail("process state byte accounting overflow".into())
                })?;
            key_count = key_count
                .checked_sub(usize::from(!before.is_empty()))
                .and_then(|count| count.checked_add(usize::from(!after.is_empty())))
                .ok_or_else(|| {
                    DbError::Pipeline("process key accounting invariant failed".into())
                })?;
            timer_count = timer_count
                .checked_sub(before.timers.len())
                .and_then(|count| count.checked_add(after.timers.len()))
                .ok_or_else(|| {
                    DbError::Pipeline("process timer accounting invariant failed".into())
                })?;
            output.extend(result.output);
            staged.push(StagedKey {
                vnode: self.vnode_for(&key),
                key,
                before,
                after,
            });
        }
        let limit = self
            .descriptor
            .limits
            .max_state_bytes
            .min(self.graph_budget);
        if live_bytes > limit
            || key_count > self.descriptor.limits.max_keys
            || timer_count > self.descriptor.limits.max_timers
        {
            return Err(DbError::BackpressureFail(
                "process function managed state or timer budget exceeded".into(),
            ));
        }
        Ok(StagedResponse {
            keys: staged,
            output,
            output_rows: rows,
            output_bytes: bytes,
            live_bytes,
            key_count,
            timer_count,
            next_generation: generation,
        })
    }

    fn apply_timer_operations(
        &self,
        state: &mut KeyState,
        operations: Vec<TimerOperation>,
        mut generation: u64,
    ) -> Result<u64, DbError> {
        for operation in operations {
            let name = match &operation {
                TimerOperation::Set { name, .. } | TimerOperation::Cancel { name } => name,
            };
            if !self
                .descriptor
                .timer_names
                .iter()
                .any(|declared| declared == name)
            {
                return Err(DbError::InvalidOperation(format!(
                    "process function used undeclared timer '{name}'"
                )));
            }
            match operation {
                TimerOperation::Set { name, at_us } => {
                    if at_us <= self.watermark_us {
                        return Err(DbError::InvalidOperation(
                            "process timer must be beyond the accepted watermark".into(),
                        ));
                    }
                    generation = generation.checked_add(1).ok_or_else(|| {
                        DbError::Pipeline("process timer generation exhausted".into())
                    })?;
                    state
                        .timers
                        .insert(name, RegisteredTimer { at_us, generation });
                }
                TimerOperation::Cancel { name } => {
                    state.timers.remove(&name);
                }
            }
        }
        Ok(generation)
    }

    fn validate_output(
        &self,
        activation: &ProcessActivation,
        batches: &[RecordBatch],
        rows: &mut usize,
        bytes: &mut usize,
    ) -> Result<(), DbError> {
        for batch in batches {
            if batch.schema().as_ref() != self.descriptor.output_schema.as_ref() {
                return Err(DbError::InvalidOperation(
                    "process function output schema mismatch".into(),
                ));
            }
            *rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DbError::BackpressureFail("process output row count overflow".into())
            })?;
            *bytes = bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| {
                    DbError::BackpressureFail("process output byte count overflow".into())
                })?;
            if *rows > self.descriptor.limits.max_output_rows
                || *bytes > self.descriptor.limits.max_output_bytes
            {
                return Err(DbError::BackpressureFail(
                    "process function output budget exceeded".into(),
                ));
            }
            let time = batch
                .column(self.output_time_index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DbError::InvalidOperation("invalid output time array".into()))?;
            for row in 0..time.len() {
                if time.is_null(row) || time.value(row) < activation.event_time_us {
                    return Err(DbError::InvalidOperation(
                        "process function emitted before its activation time".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn commit_results(&mut self, staged: Vec<StagedKey>) {
        for item in staged {
            for (name, timer) in item.before.timers {
                self.due
                    .remove(&(timer.at_us, item.key.clone(), name, timer.generation));
            }
            for (name, timer) in &item.after.timers {
                self.due.insert((
                    timer.at_us,
                    item.key.clone(),
                    name.clone(),
                    timer.generation,
                ));
            }
            if item.after.is_empty() {
                self.state[item.vnode].remove(&item.key);
            } else {
                self.state[item.vnode].insert(item.key, item.after);
            }
        }
    }

    fn fire_due_timers(
        &mut self,
        output: &mut Vec<RecordBatch>,
        output_rows: &mut usize,
        output_bytes: &mut usize,
    ) -> Result<(), DbError> {
        for _ in 0..self.descriptor.limits.max_timer_callbacks_per_step {
            let Some((at_us, key, name, generation)) = self.due.first().cloned() else {
                break;
            };
            if at_us > self.watermark_us {
                break;
            }
            let Some(timer) = self.state_for(&key).timers.get(&name) else {
                return Err(DbError::StatefulOperatorPartialApply(
                    "process timer index disagrees with managed state".into(),
                ));
            };
            if timer.generation != generation || timer.at_us != at_us {
                return Err(DbError::StatefulOperatorPartialApply(
                    "process timer generation disagrees with managed state".into(),
                ));
            }
            let activation = ProcessActivation {
                id: self.next_activation_id,
                state: self.state_for(&key).view(),
                key_text: self.state_for(&key).key_text.clone(),
                key: Arc::from(key),
                event_time_us: at_us,
                callback: ProcessCallback::Timer { name },
            };
            self.accept_call(&mut [activation], output, output_rows, output_bytes)?;
        }
        Ok(())
    }
}

static EMPTY_KEY_STATE: KeyState = KeyState {
    key_text: String::new(),
    value: ValueState::Absent,
    timers: BTreeMap::new(),
};

fn apply_value_mutation(state: &mut KeyState, mutation: ValueMutation) {
    match mutation {
        ValueMutation::Unchanged => {}
        ValueMutation::Set(value) => state.value = ValueState::Value(value),
        ValueMutation::SetNull => state.value = ValueState::Null,
        ValueMutation::Clear => state.value = ValueState::Absent,
    }
}

fn charged_key(key: &[u8], state: &KeyState) -> Result<usize, DbError> {
    if state.is_empty() {
        return Ok(0);
    }
    let timer_names = state.timers.keys().try_fold(0usize, |sum, name| {
        sum.checked_add(name.len()).ok_or_else(|| {
            DbError::BackpressureFail("process timer name accounting overflow".into())
        })
    })?;
    KEY_CHARGE
        .checked_add(key.len())
        .and_then(|n| n.checked_add(state.key_text.len()))
        .and_then(|n| n.checked_add(usize::from(state.value != ValueState::Absent) * 8))
        .and_then(|n| n.checked_add(state.timers.len().checked_mul(TIMER_CHARGE)?))
        .and_then(|n| n.checked_add(timer_names))
        .ok_or_else(|| DbError::BackpressureFail("process state byte accounting overflow".into()))
}

pub(super) fn validate_descriptor(
    descriptor: &ProcessFunctionDescriptor,
) -> Result<(PartitionKeyCodecV1, Vec<usize>, usize, usize), DbError> {
    super::canonical_fields(&descriptor.input_schema)?;
    super::canonical_fields(&descriptor.output_schema)?;
    if crate::catalog::schema_has_reserved_mutation_columns(&descriptor.input_schema)
        || crate::catalog::schema_has_reserved_mutation_columns(&descriptor.output_schema)
    {
        return Err(DbError::Unsupported(
            "process functions require append-only input and output schemas".into(),
        ));
    }
    if descriptor.version != 1
        || descriptor.function_id.is_empty()
        || descriptor.pipeline_state_id.is_empty()
        || descriptor.value_state_name.is_empty()
        || descriptor.implementation_digest.len() != 64
        || !descriptor
            .implementation_digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    {
        return Err(DbError::InvalidOperation(
            "invalid process function version, identity, state name, or SHA-256 digest".into(),
        ));
    }
    let limits = descriptor.limits;
    if descriptor.runtime == super::ProcessRuntime::RemotePython
        && descriptor.determinism == super::ProcessDeterminism::ReplaySafe
        && descriptor.python_environment.is_none()
    {
        return Err(DbError::Unsupported(
            "replay-safe Python requires a complete environment binding".into(),
        ));
    }
    if let Some(environment) = &descriptor.python_environment {
        if descriptor.runtime != super::ProcessRuntime::RemotePython {
            return Err(DbError::Unsupported(
                "environment binding requires the Python runtime".into(),
            ));
        }
        environment.validate()?;
    }
    if limits.max_batch_rows == 0
        || limits.max_input_rows == 0
        || limits.max_input_bytes == 0
        || limits.max_output_rows == 0
        || limits.max_output_bytes == 0
        || limits.max_state_bytes == 0
        || limits.max_keys == 0
        || limits.max_timer_callbacks_per_step == 0
    {
        return Err(DbError::InvalidOperation(
            "process function limits must be positive".into(),
        ));
    }
    let mut key_indices = Vec::with_capacity(descriptor.key_columns.len());
    let mut key_types = Vec::with_capacity(descriptor.key_columns.len());
    for name in &descriptor.key_columns {
        let index = descriptor.input_schema.index_of(name).map_err(|_| {
            DbError::InvalidOperation(format!("process key column '{name}' does not exist"))
        })?;
        let field = descriptor.input_schema.field(index);
        if field.is_nullable() || !field.metadata().is_empty() || key_indices.contains(&index) {
            return Err(DbError::InvalidOperation(format!(
                "process key column '{name}' is nullable, annotated, or repeated"
            )));
        }
        key_indices.push(index);
        key_types.push(field.data_type().clone());
    }
    let key_codec = PartitionKeyCodecV1::try_new(key_types).map_err(|error| {
        DbError::InvalidOperation(format!("unsupported process function key: {error}"))
    })?;
    if key_indices.len() != 1
        || descriptor.input_schema.field(key_indices[0]).data_type() != &DataType::Utf8
    {
        return Err(DbError::Unsupported(
            "process functions currently require one UTF-8 key column".into(),
        ));
    }
    let time_index = validate_time_column(&descriptor.input_schema, &descriptor.event_time_column)?;
    let output_time_index = validate_time_column(
        &descriptor.output_schema,
        &descriptor.output_event_time_column,
    )?;
    let mut timer_names = BTreeSet::new();
    for name in &descriptor.timer_names {
        if name.is_empty() || !timer_names.insert(name) {
            return Err(DbError::InvalidOperation(
                "process timer names must be nonempty and unique".into(),
            ));
        }
    }
    Ok((key_codec, key_indices, time_index, output_time_index))
}

fn validate_time_column(schema: &arrow_schema::Schema, name: &str) -> Result<usize, DbError> {
    let index = schema.index_of(name).map_err(|_| {
        DbError::InvalidOperation(format!("process event-time column '{name}' does not exist"))
    })?;
    let field = schema.field(index);
    if field.is_nullable()
        || !matches!(
            field.data_type(),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        )
    {
        return Err(DbError::InvalidOperation(format!(
            "process event-time column '{name}' must be nonnull UTC microseconds"
        )));
    }
    Ok(index)
}

#[cfg(feature = "cluster")]
mod execution;
mod graph;
#[cfg(feature = "process-remote")]
mod remote;
mod restoration;
#[cfg(feature = "cluster")]
mod sequencing;
#[cfg(feature = "cluster")]
mod transition;
