use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::RecordBatch;
use laminar_core::shuffle::ShuffleMessage;
use laminar_core::state::PartitionKeyCodecV1;
use tokio::task::JoinSet;

use super::{
    Application, ApplicationPhase, PendingInput, ProcessExecution, ProcessFunctionOperator,
    ProcessShuffle,
};
use crate::error::DbError;
use crate::operator_graph::InputFrontier;

type Outbound = Vec<(u64, ShuffleMessage)>;
type SendOutcome = (Result<(), DbError>, Option<Outbound>);

pub(super) struct PendingSend {
    application: Option<Application>,
    outbound: Option<Outbound>,
    tasks: JoinSet<SendOutcome>,
    ready: Arc<AtomicBool>,
    bytes: usize,
    rows: usize,
}

struct SendWake {
    ready: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
}

impl Drop for SendWake {
    fn drop(&mut self) {
        // Also wakes the graph if the task is cancelled or panics before returning an outcome.
        self.ready.store(true, Ordering::Release);
        self.wake.notify_one();
    }
}

impl PendingSend {
    pub(super) fn retained_bytes(&self) -> usize {
        self.bytes.saturating_add(std::mem::size_of::<Self>())
    }

    pub(super) fn rows(&self) -> usize {
        self.rows
    }

    pub(super) fn hold_ms(&self) -> Option<i64> {
        self.application
            .as_ref()
            .and_then(|application| application.hold_ms)
    }

    pub(super) fn runnable(&self) -> bool {
        self.outbound.is_some() || self.ready.load(Ordering::Acquire)
    }

    fn start(
        &mut self,
        operator: &ProcessFunctionOperator,
        shuffle: &ProcessShuffle,
    ) -> Result<(), DbError> {
        operator.require_execution_current()?;
        let ProcessExecution::Distributed(authority) = &operator.execution else {
            return Err(DbError::StatefulOperatorPartialApply(
                "process shuffle lost its execution authority".into(),
            ));
        };
        let outbound = self.outbound.take().ok_or_else(|| {
            DbError::StatefulOperatorPartialApply(
                "process shuffle lost its retained send plan".into(),
            )
        })?;
        let sender = Arc::clone(&authority.config.sender);
        let topology = authority.config.topology;
        let version = authority.assignment.version();
        let recovery = authority.recovery_generation;
        let wake = SendWake {
            ready: Arc::clone(&self.ready),
            wake: Arc::clone(&shuffle.wake),
        };
        self.ready.store(false, Ordering::Release);
        self.tasks.spawn_on(
            async move {
                let _wake = wake;
                crate::operator::send_shuffle_plan_for_generation_retaining(
                    &sender,
                    topology,
                    version,
                    recovery,
                    outbound,
                    "process input shuffle",
                )
                .await
            },
            &shuffle.runtime,
        );
        Ok(())
    }

    pub(super) fn poll(
        &mut self,
        operator: &ProcessFunctionOperator,
    ) -> Result<Option<Application>, DbError> {
        let Some(outcome) = self.tasks.try_join_next() else {
            return Ok(None);
        };
        let (result, outbound) = outcome.map_err(|error| {
            DbError::ShufflePartialSend(format!(
                "process shuffle send task ended without a delivery outcome: {error}"
            ))
        })?;
        operator.require_execution_current()?;
        match result {
            Ok(()) => {
                if outbound.is_some() {
                    return Err(DbError::ShufflePartialSend(
                        "successful process shuffle retained an unexpected send plan".into(),
                    ));
                }
                self.application.take().map(Some).ok_or_else(|| {
                    DbError::ShufflePartialSend(
                        "process shuffle lost its local application cut".into(),
                    )
                })
            }
            Err(error) if error.is_shuffle_not_ready() => {
                self.outbound = Some(outbound.ok_or_else(|| {
                    DbError::ShufflePartialSend(
                        "retryable process shuffle lost its send plan".into(),
                    )
                })?);
                self.ready.store(false, Ordering::Release);
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}

impl ProcessShuffle {
    pub(super) fn retry_send(
        &self,
        send: &mut PendingSend,
        operator: &ProcessFunctionOperator,
    ) -> Result<(), DbError> {
        if send.outbound.is_some() {
            send.start(operator, self)?;
        }
        Ok(())
    }

    pub(super) fn begin_local(
        &mut self,
        operator: &mut ProcessFunctionOperator,
        inputs: &[Vec<RecordBatch>],
        supplied: InputFrontier,
        graph_budget: usize,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let has_data = inputs.iter().flatten().any(|batch| batch.num_rows() != 0);
        if supplied.idle && has_data {
            return Err(DbError::ShuffleTerminal(
                "process received data from an idle local channel".into(),
            ));
        }
        let frontier = if self.last_broadcast == self.local {
            crate::operator::frontier::normalize_restored_local_frontier(
                supplied,
                self.local,
                self.effective.watermark,
            )
        } else {
            self.local
        };
        super::input::validate_frontier(self.local, frontier)?;
        let (application, mut outbound, bytes, rows) =
            self.plan_local(operator, inputs, frontier, graph_budget)?;
        let broadcast = frontier != self.last_broadcast;
        if broadcast {
            for &peer in self.peers.keys() {
                outbound.push((
                    peer,
                    ShuffleMessage::Frontier {
                        stage: self.stage.clone(),
                        watermark: frontier.watermark,
                        idle: frontier.idle,
                    },
                ));
            }
        }
        let frontier_bytes = if broadcast {
            self.peers
                .len()
                .saturating_mul(std::mem::size_of::<(u64, ShuffleMessage)>() + self.stage.len())
        } else {
            0
        };
        let retained_bytes = bytes.saturating_add(frontier_bytes);
        let send_bytes = if outbound.is_empty() {
            0
        } else {
            std::mem::size_of::<PendingSend>()
        };
        if self
            .retained_bytes()
            .saturating_add(operator.live_bytes)
            .saturating_add(retained_bytes)
            .saturating_add(send_bytes)
            > graph_budget
        {
            return Err(super::input::budget_error());
        }
        if outbound.is_empty() {
            return self.apply(operator, application, graph_budget);
        }
        let mut send = PendingSend {
            application: Some(application),
            outbound: Some(outbound),
            tasks: JoinSet::new(),
            ready: Arc::new(AtomicBool::new(false)),
            bytes: retained_bytes,
            rows,
        };
        send.start(operator, self)?;
        self.pending = Some(PendingInput::Send(send));
        Ok(Vec::new())
    }

    fn plan_local(
        &self,
        operator: &ProcessFunctionOperator,
        inputs: &[Vec<RecordBatch>],
        frontier: InputFrontier,
        graph_budget: usize,
    ) -> Result<(Application, Outbound, usize, usize), DbError> {
        let input_bytes = operator.validate_routed_batches(inputs.iter().flatten())?;
        let ProcessExecution::Distributed(authority) = &operator.execution else {
            return Err(DbError::StatefulOperatorPartialApply(
                "process routing has no distributed authority".into(),
            ));
        };
        let available = graph_budget
            .checked_sub(operator.live_bytes)
            .and_then(|bytes| bytes.checked_sub(self.retained_bytes()))
            .and_then(|bytes| bytes.checked_sub(input_bytes))
            .ok_or_else(super::input::budget_error)?;
        let mut local = Vec::new();
        let mut outbound = Vec::new();
        let mut local_bytes = 0usize;
        let mut routed_bytes = 0usize;
        let mut rows = 0usize;
        let mut earliest_us = None;
        for batch in inputs
            .iter()
            .flatten()
            .filter(|batch| batch.num_rows() != 0)
        {
            let first_us = self.validate_local_time(operator, batch)?;
            earliest_us = Some(earliest_us.map_or(first_us, |before: i64| before.min(first_us)));
            let columns = operator
                .key_indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect::<Vec<_>>();
            let keys = operator
                .key_codec
                .encode_columns(&columns)
                .map_err(|error| {
                    DbError::ShuffleTerminal(format!("process routing keys: {error}"))
                })?;
            let vnodes = keys
                .iter()
                .map(|key| PartitionKeyCodecV1::vnode_for_encoded(key.data(), operator.vnode_count))
                .collect::<Vec<_>>();
            let scratch = keys
                .size()
                .saturating_add(vnodes.capacity().saturating_mul(std::mem::size_of::<u32>()));
            if scratch.saturating_add(routed_bytes) > available {
                return Err(super::input::budget_error());
            }
            let plan = laminar_core::shuffle::route_checkpointed_batch(
                batch,
                &vnodes,
                &authority.assignment,
                authority.config.self_id,
            )
            .map_err(|error| {
                crate::operator::shuffle_routing_error("process input routing", &error)
            })?;
            rows += batch.num_rows();
            for route in plan.local {
                let bytes = batch_charge(&route.batch);
                local_bytes = local_bytes.saturating_add(bytes);
                routed_bytes = routed_bytes.saturating_add(bytes);
                local.push(route.batch);
            }
            for route in plan.remote {
                routed_bytes = routed_bytes
                    .saturating_add(batch_charge(&route.batch))
                    .saturating_add(
                        route
                            .routed_vnodes
                            .len()
                            .saturating_mul(std::mem::size_of::<u32>()),
                    )
                    .saturating_add(self.stage.len())
                    .saturating_add(std::mem::size_of::<ShuffleMessage>());
                outbound.push((
                    route.owner.0,
                    ShuffleMessage::checkpointed_routed(
                        self.stage.clone(),
                        route.routed_vnodes,
                        route.batch,
                    ),
                ));
            }
            if routed_bytes > operator.descriptor.limits.max_input_bytes
                || scratch.saturating_add(routed_bytes) > available
            {
                return Err(super::input::budget_error());
            }
        }
        let local_rows = local.iter().map(RecordBatch::num_rows).sum();
        Ok((
            Application {
                phase: ApplicationPhase::Ready(local),
                retained: None,
                local_frontier: Some(frontier),
                bytes: local_bytes,
                rows: local_rows,
                hold_ms: earliest_us.map(|us| us.saturating_sub(1).div_euclid(1_000)),
            },
            outbound,
            routed_bytes,
            rows,
        ))
    }

    fn validate_local_time(
        &self,
        operator: &ProcessFunctionOperator,
        batch: &RecordBatch,
    ) -> Result<i64, DbError> {
        let time = batch
            .column(operator.time_index)
            .as_any()
            .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            .ok_or_else(|| {
                DbError::ShuffleTerminal("process routing has an invalid event-time array".into())
            })?;
        if self.local.watermark.is_some_and(|ms| {
            time.values()
                .iter()
                .any(|&us| us < ms.saturating_mul(1_000))
        }) {
            return Err(DbError::ShuffleTerminal(
                "process local data precedes its applied frontier".into(),
            ));
        }
        Ok(time.values().iter().copied().min().unwrap_or(i64::MAX))
    }
}

fn batch_charge(batch: &RecordBatch) -> usize {
    batch
        .get_array_memory_size()
        .saturating_add(std::mem::size_of::<RecordBatch>())
        .saturating_add(
            batch
                .num_columns()
                .saturating_mul(std::mem::size_of::<Arc<dyn arrow::array::Array>>()),
        )
}
