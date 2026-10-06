use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, TimestampMicrosecondArray};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinSet;
use uuid::Uuid;

use super::{ProcessFunctionOperator, StagedResponse};
use crate::error::DbError;
use crate::operator_graph::InputFrontier;
use crate::process_function::remote::{RemoteInvocationScope, RemoteProcessClient};
use crate::process_function::{
    ProcessActivation, ProcessActivationResult, ProcessCallback, ProcessHandler,
};

struct QueuedInput {
    id: u64,
    key: Vec<u8>,
    event_time_us: i64,
    row: RecordBatch,
}

struct CompletedCall {
    activations: Vec<ProcessActivation>,
    #[cfg(feature = "cluster")]
    generations: (u64, u64),
    result: Result<Vec<ProcessActivationResult>, DbError>,
}

struct CompletionGuard {
    activations: Option<Vec<ProcessActivation>>,
    #[cfg(feature = "cluster")]
    generations: (u64, u64),
    sender: mpsc::Sender<CompletedCall>,
    wake: Arc<Notify>,
    send_failed: Arc<AtomicBool>,
}

impl CompletionGuard {
    fn publish(&mut self, result: Result<Vec<ProcessActivationResult>, DbError>) {
        let Some(activations) = self.activations.take() else {
            return;
        };
        if self
            .sender
            .try_send(CompletedCall {
                activations,
                #[cfg(feature = "cluster")]
                generations: self.generations,
                result,
            })
            .is_err()
        {
            self.send_failed.store(true, Ordering::Release);
        }
        self.wake.notify_one();
    }
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        if self.activations.is_some() {
            self.publish(Err(DbError::StatefulOperatorPartialApply(
                "process worker client task ended before returning a result".into(),
            )));
        }
    }
}

/// One bounded graph-step roster. Only the compute thread mutates authoritative state; tasks
/// carry immutable snapshots and return proposals through a bounded channel.
pub(super) struct RemoteExecution {
    runtime: tokio::runtime::Handle,
    wake: Arc<Notify>,
    operator_id: String,
    max_in_flight: usize,
    queued: VecDeque<QueuedInput>,
    busy_keys: BTreeSet<Vec<u8>>,
    completed_tx: mpsc::Sender<CompletedCall>,
    completed_rx: mpsc::Receiver<CompletedCall>,
    send_failed: Arc<AtomicBool>,
    tasks: JoinSet<()>,
    running: usize,
    input_pending: bool,
    target_watermark_us: i64,
    held_time_us: Option<i64>,
    output_rows: usize,
    output_bytes: usize,
    timer_callbacks: usize,
}

impl RemoteExecution {
    pub(super) fn new(
        runtime: tokio::runtime::Handle,
        wake: Arc<Notify>,
        operator_id: String,
        max_in_flight: usize,
    ) -> Self {
        let (completed_tx, completed_rx) = mpsc::channel(max_in_flight);
        Self {
            runtime,
            wake,
            operator_id,
            max_in_flight,
            queued: VecDeque::new(),
            busy_keys: BTreeSet::new(),
            completed_tx,
            completed_rx,
            send_failed: Arc::new(AtomicBool::new(false)),
            tasks: JoinSet::new(),
            running: 0,
            input_pending: false,
            target_watermark_us: i64::MIN,
            held_time_us: None,
            output_rows: 0,
            output_bytes: 0,
            timer_callbacks: 0,
        }
    }

    pub(super) fn is_pending(&self) -> bool {
        self.input_pending || self.running != 0 || !self.queued.is_empty()
    }

    pub(super) fn is_runnable(&self, due_now: bool) -> bool {
        self.send_failed.load(Ordering::Acquire)
            || !self.completed_rx.is_empty()
            || (self.running == 0 && (!self.queued.is_empty() || due_now))
    }

    pub(super) fn held_time_us(&self) -> Option<i64> {
        self.held_time_us
    }

    fn begin_input(
        &mut self,
        operator: &mut ProcessFunctionOperator,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
    ) -> Result<(), DbError> {
        if self.is_pending() {
            return Err(DbError::StatefulOperatorPartialApply(
                "remote process accepted input while an earlier step remained pending".into(),
            ));
        }
        let mut rows = 0usize;
        let mut bytes = 0usize;
        let mut next_id = operator.next_activation_id;
        let mut queued = VecDeque::new();
        let mut held_time_us = None;
        for batch in inputs.iter().flatten() {
            rows = rows.checked_add(batch.num_rows()).ok_or_else(|| {
                DbError::BackpressureFail("process input row count overflow".into())
            })?;
            bytes = bytes
                .checked_add(batch.get_array_memory_size())
                .ok_or_else(|| {
                    DbError::BackpressureFail("process input byte count overflow".into())
                })?;
            if rows > operator.descriptor.limits.max_input_rows
                || bytes > operator.descriptor.limits.max_input_bytes
            {
                return Err(DbError::BackpressureFail(
                    "process input budget exceeded".into(),
                ));
            }
            if batch.schema().as_ref() != operator.descriptor.input_schema.as_ref() {
                return Err(DbError::InvalidOperation(
                    "process function input schema changed after registration".into(),
                ));
            }
            let columns = operator
                .key_indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect::<Vec<_>>();
            let keys = operator
                .key_codec
                .encode_columns(&columns)
                .map_err(|error| {
                    DbError::InvalidOperation(format!("process function key encoding: {error}"))
                })?;
            let time = batch
                .column(operator.time_index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| DbError::InvalidOperation("invalid event-time array".into()))?;
            for (row, key) in keys.iter().enumerate() {
                if operator
                    .key_indices
                    .iter()
                    .any(|&index| batch.column(index).is_null(row))
                {
                    return Err(DbError::InvalidOperation(
                        "process function key contains NULL".into(),
                    ));
                }
                if time.is_null(row) || time.value(row) < operator.watermark_us {
                    return Err(DbError::InvalidOperation(
                        "process function rejects null or late event time".into(),
                    ));
                }
                held_time_us =
                    Some(held_time_us.map_or(time.value(row), |old: i64| old.min(time.value(row))));
                queued.push_back(QueuedInput {
                    id: next_id,
                    key: key.data().to_vec(),
                    event_time_us: time.value(row),
                    row: batch.slice(row, 1),
                });
                next_id = next_id.checked_add(1).ok_or_else(|| {
                    DbError::PipelineTerminal("process activation ID exhausted".into())
                })?;
            }
        }
        #[cfg(feature = "cluster")]
        Self::reserve_queued_activation_ids(operator, &mut queued)?;
        operator.next_activation_id = next_id;
        self.queued = queued;
        self.held_time_us = held_time_us;
        self.input_pending = !self.queued.is_empty();
        self.target_watermark_us = frontier
            .watermark
            .map_or(i64::MIN, |ms| ms.saturating_mul(1_000));
        if !self.input_pending {
            operator.watermark_us = operator.watermark_us.max(self.target_watermark_us);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn reserve_queued_activation_ids(
        operator: &mut ProcessFunctionOperator,
        queued: &mut VecDeque<QueuedInput>,
    ) -> Result<(), DbError> {
        // IDs follow accepted channel order before the key-distinct scheduler changes batches.
        for (index, row) in queued.iter_mut().enumerate() {
            match operator.reserve_activation_id(&row.key, row.id) {
                Ok(id) => row.id = id,
                Err(error) => {
                    for row in queued.iter().take(index).rev() {
                        operator.reclaim_activation_id(row.id);
                    }
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn dispatch_input(
        &mut self,
        operator: &ProcessFunctionOperator,
        client: &Arc<RemoteProcessClient>,
    ) -> Result<(), DbError> {
        while self.running < self.max_in_flight && !self.queued.is_empty() {
            let Some(vnode) = self
                .queued
                .iter()
                .find(|row| !self.busy_keys.contains(&row.key))
                .map(|row| operator.vnode_for(&row.key))
            else {
                break;
            };
            let scan = self.queued.len();
            let mut activations = Vec::new();
            for _ in 0..scan {
                let Some(row) = self.queued.pop_front() else {
                    break;
                };
                if activations.len() == operator.descriptor.limits.max_batch_rows
                    || operator.vnode_for(&row.key) != vnode
                    || !self.busy_keys.insert(row.key.clone())
                {
                    self.queued.push_back(row);
                    continue;
                }
                activations.push(operator.activation_for_row(
                    &row.row,
                    0,
                    row.key,
                    row.event_time_us,
                    row.id,
                ));
            }
            if activations.is_empty() {
                break;
            }
            self.spawn_call(operator, client, vnode, activations)?;
        }
        Ok(())
    }

    fn dispatch_timers(
        &mut self,
        operator: &mut ProcessFunctionOperator,
        client: &Arc<RemoteProcessClient>,
    ) -> Result<(), DbError> {
        while self.running < self.max_in_flight
            && self.timer_callbacks < operator.descriptor.limits.max_timer_callbacks_per_step
        {
            let Some((at_us, key, name, generation)) = operator
                .due
                .iter()
                .find(|(at_us, key, ..)| {
                    *at_us <= operator.watermark_us && !self.busy_keys.contains(key)
                })
                .cloned()
            else {
                break;
            };
            let state = operator.state_for(&key);
            if state
                .timers
                .get(&name)
                .is_none_or(|timer| timer.generation != generation || timer.at_us != at_us)
            {
                return Err(DbError::StatefulOperatorPartialApply(
                    "process timer index disagrees with managed state".into(),
                ));
            }
            let activation = ProcessActivation {
                id: operator.next_activation_id,
                key: Arc::from(key.clone()),
                key_text: state.key_text.clone(),
                event_time_us: at_us,
                callback: ProcessCallback::Timer { name },
                state: state.view(),
            };
            let next_id = operator.next_activation_id.checked_add(1).ok_or_else(|| {
                DbError::PipelineTerminal("process activation ID exhausted".into())
            })?;
            #[cfg(feature = "cluster")]
            let activation = {
                let mut activation = activation;
                activation.id = operator.reserve_activation_id(&activation.key, activation.id)?;
                activation
            };
            operator.next_activation_id = next_id;
            self.busy_keys.insert(key);
            self.timer_callbacks += 1;
            let vnode = operator.vnode_for(&activation.key);
            self.spawn_call(operator, client, vnode, vec![activation])?;
        }
        Ok(())
    }

    fn spawn_call(
        &mut self,
        operator: &ProcessFunctionOperator,
        client: &Arc<RemoteProcessClient>,
        vnode: usize,
        activations: Vec<ProcessActivation>,
    ) -> Result<(), DbError> {
        let vnode = u32::try_from(vnode).map_err(|_| {
            DbError::PipelineTerminal("process vnode index exceeds protocol domain".into())
        })?;
        let batch_id = Uuid::new_v4();
        let mut attempt_id = Uuid::new_v4();
        if attempt_id == batch_id {
            attempt_id = Uuid::from_u128(batch_id.as_u128() ^ 1);
        }
        #[cfg(feature = "cluster")]
        let (owner_generation, recovery_generation) = operator.execution_generations()?;
        #[cfg(not(feature = "cluster"))]
        let (owner_generation, recovery_generation) = (0, 0);
        let scope = RemoteInvocationScope {
            operator_id: self.operator_id.clone(),
            vnode,
            vnode_count: operator.vnode_count.get(),
            owner_generation,
            recovery_generation,
            batch_id,
            attempt_id,
            input_watermark_us: (operator.watermark_us != i64::MIN)
                .then_some(operator.watermark_us),
        };
        let client = Arc::clone(client);
        let guard = CompletionGuard {
            activations: Some(activations),
            #[cfg(feature = "cluster")]
            generations: (owner_generation, recovery_generation),
            sender: self.completed_tx.clone(),
            wake: Arc::clone(&self.wake),
            send_failed: Arc::clone(&self.send_failed),
        };
        self.running += 1;
        self.tasks.spawn_on(
            async move {
                let mut guard = guard;
                let result = client
                    .invoke(&scope, guard.activations.as_deref().unwrap_or_default())
                    .await;
                guard.publish(result);
            },
            &self.runtime,
        );
        Ok(())
    }

    fn drain_completed(
        &mut self,
        operator: &mut ProcessFunctionOperator,
        output: &mut Vec<RecordBatch>,
    ) -> Result<(), DbError> {
        if self.send_failed.load(Ordering::Acquire) {
            return Err(DbError::StatefulOperatorPartialApply(
                "process worker completion channel lost a result".into(),
            ));
        }
        while let Ok(completed) = self.completed_rx.try_recv() {
            #[cfg(feature = "cluster")]
            if operator.execution_generations()? != completed.generations {
                return Err(DbError::StatefulOperatorPartialApply(
                    "process worker response belongs to a stale assignment or recovery generation"
                        .into(),
                ));
            }
            self.running = self.running.checked_sub(1).ok_or_else(|| {
                DbError::StatefulOperatorPartialApply(
                    "process invocation accounting underflow".into(),
                )
            })?;
            let response = completed.result.map_err(|error| {
                DbError::StatefulOperatorPartialApply(format!(
                    "process worker invocation failed; recover the current graph: {error}"
                ))
            })?;
            if response.len() != completed.activations.len() {
                return Err(DbError::PipelineTerminal(
                    "process worker response count differs from activation count".into(),
                ));
            }
            let mut by_id = BTreeMap::new();
            for result in response {
                if by_id.insert(result.activation_id, result).is_some() {
                    return Err(DbError::PipelineTerminal(
                        "process worker repeated an activation ID".into(),
                    ));
                }
            }
            let mut staged: StagedResponse = operator
                .stage_results(
                    &completed.activations,
                    &mut by_id,
                    self.output_rows,
                    self.output_bytes,
                )
                .map_err(|error| {
                    if error.requires_pipeline_halt() || error.requires_pipeline_recovery() {
                        error
                    } else {
                        DbError::PipelineTerminal(format!(
                            "process worker returned an invalid result: {error}"
                        ))
                    }
                })?;
            #[cfg(feature = "cluster")]
            operator.require_execution_current()?;
            operator.commit_results(std::mem::take(&mut staged.keys));
            operator.live_bytes = staged.live_bytes;
            operator.key_count = staged.key_count;
            operator.timer_count = staged.timer_count;
            operator.next_timer_generation = staged.next_generation;
            self.output_rows = staged.output_rows;
            self.output_bytes = staged.output_bytes;
            output.append(&mut staged.output);
            for activation in completed.activations {
                self.busy_keys.remove(activation.key.as_ref());
            }
        }
        while let Some(task) = self.tasks.try_join_next() {
            if task.is_err() {
                return Err(DbError::StatefulOperatorPartialApply(
                    "process worker client task ended without a response".into(),
                ));
            }
        }
        Ok(())
    }

    fn finish_input(&mut self, operator: &mut ProcessFunctionOperator) {
        if self.input_pending && self.queued.is_empty() && self.running == 0 {
            operator.watermark_us = operator.watermark_us.max(self.target_watermark_us);
            self.input_pending = false;
            self.held_time_us = None;
        }
    }

    fn finish_step_if_idle(&mut self) {
        if !self.is_pending() {
            self.output_rows = 0;
            self.output_bytes = 0;
            self.timer_callbacks = 0;
        }
    }
}

impl ProcessFunctionOperator {
    pub(super) fn process_remote(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if inputs.len() > 1 || frontiers.len() != 1 {
            return Err(DbError::InvalidOperation(
                "process function requires exactly one input frontier".into(),
            ));
        }
        let ProcessHandler::Remote(client) = &self.handler else {
            return Err(DbError::Pipeline(
                "native process entered remote execution".into(),
            ));
        };
        let client = Arc::clone(client);
        let Some(mut remote) = self.remote.take() else {
            return Err(DbError::StatefulOperatorPartialApply(
                "process worker scheduler is missing".into(),
            ));
        };
        let mut output = Vec::new();
        let result = (|| {
            if !inputs.iter().all(Vec::is_empty) {
                remote
                    .begin_input(self, inputs, frontiers[0])
                    .map_err(|error| {
                        if error.requires_pipeline_halt() || error.requires_pipeline_recovery() {
                            error
                        } else {
                            DbError::PipelineTerminal(format!(
                                "process function rejected an activation: {error}"
                            ))
                        }
                    })?;
            } else if !remote.is_pending() {
                let watermark = frontiers[0]
                    .watermark
                    .map_or(i64::MIN, |ms| ms.saturating_mul(1_000));
                remote.target_watermark_us = watermark;
                self.watermark_us = self.watermark_us.max(watermark);
            }
            remote.drain_completed(self, &mut output)?;
            remote.dispatch_input(self, &client)?;
            remote.finish_input(self);
            if !remote.input_pending {
                remote.dispatch_timers(self, &client)?;
            }
            remote.finish_step_if_idle();
            Ok(output)
        })();
        self.remote = Some(remote);
        result
    }
}
