use std::collections::{BTreeMap, BTreeSet};

use arrow::array::RecordBatch;
use prost::Message;

use super::codec::{decode_batch, decode_mutation, decode_timer};
use super::wire::{self, worker_frame};
use super::MAX_INVOCATION_WIRE_BYTES;
use crate::error::DbError;
use crate::process_function::{
    ProcessActivation, ProcessActivationResult, ProcessFunctionDescriptor, TimerOperation,
    ValueMutation,
};

struct PendingResult {
    expected_outputs: usize,
    output: Vec<RecordBatch>,
    value: ValueMutation,
    timers: Vec<TimerOperation>,
}

struct ResponseAccumulator<'a> {
    descriptor: &'a ProcessFunctionDescriptor,
    expected: BTreeSet<u64>,
    results: BTreeMap<u64, PendingResult>,
    output_count: usize,
    output_rows: usize,
    output_bytes: usize,
    timer_count: usize,
    wire_bytes: usize,
}

impl<'a> ResponseAccumulator<'a> {
    fn new(
        descriptor: &'a ProcessFunctionDescriptor,
        activations: &[ProcessActivation],
        ack_bytes: usize,
    ) -> Self {
        Self {
            descriptor,
            expected: activations.iter().map(|activation| activation.id).collect(),
            results: BTreeMap::new(),
            output_count: 0,
            output_rows: 0,
            output_bytes: 0,
            timer_count: 0,
            wire_bytes: ack_bytes,
        }
    }

    fn accept(&mut self, frame: wire::WorkerFrame) -> Result<bool, DbError> {
        self.wire_bytes = self
            .wire_bytes
            .checked_add(frame.encoded_len())
            .filter(|bytes| *bytes <= MAX_INVOCATION_WIRE_BYTES)
            .ok_or_else(|| {
                DbError::BackpressureFail("process response wire budget exceeded".into())
            })?;
        match frame.kind {
            Some(worker_frame::Kind::Result(result)) => self.accept_result(result).map(|()| false),
            Some(worker_frame::Kind::Output(output)) => self.accept_output(&output).map(|()| false),
            Some(worker_frame::Kind::Complete(complete)) => {
                if complete.result_count as usize != self.expected.len()
                    || complete.output_count as usize != self.output_count
                    || self.results.len() != self.expected.len()
                    || self
                        .results
                        .values()
                        .any(|result| result.output.len() != result.expected_outputs)
                {
                    return Err(DbError::InvalidOperation(
                        "process response completion mismatch".into(),
                    ));
                }
                Ok(true)
            }
            Some(worker_frame::Kind::Failure(failure)) => Err(DbError::Pipeline(format!(
                "process worker failed: {}",
                failure.message
            ))),
            _ => Err(DbError::InvalidOperation(
                "invalid process worker response frame".into(),
            )),
        }
    }

    fn accept_result(&mut self, result: wire::Result) -> Result<(), DbError> {
        if !self.expected.contains(&result.activation_id)
            || self.results.contains_key(&result.activation_id)
            || result.output_count as usize > self.descriptor.limits.max_output_rows.max(1)
        {
            return Err(DbError::InvalidOperation(
                "unexpected process result ID or size".into(),
            ));
        }
        self.timer_count = self
            .timer_count
            .checked_add(result.timers.len())
            .filter(|count| *count <= self.descriptor.limits.max_timers)
            .ok_or_else(|| {
                DbError::BackpressureFail("process result timer limit exceeded".into())
            })?;
        let timers = result
            .timers
            .into_iter()
            .map(|timer| decode_timer(timer, self.descriptor))
            .collect::<Result<Vec<_>, _>>()?;
        self.results.insert(
            result.activation_id,
            PendingResult {
                expected_outputs: result.output_count as usize,
                output: Vec::new(),
                value: decode_mutation(result.mutation)?,
                timers,
            },
        );
        Ok(())
    }

    fn accept_output(&mut self, output: &wire::Output) -> Result<(), DbError> {
        let result = self
            .results
            .get_mut(&output.activation_id)
            .ok_or_else(|| DbError::InvalidOperation("process output precedes result".into()))?;
        if result.output.len() >= result.expected_outputs {
            return Err(DbError::InvalidOperation(
                "excess process output frame".into(),
            ));
        }
        let batch = decode_batch(
            &output.arrow_ipc,
            &self.descriptor.output_schema,
            self.descriptor.limits.max_output_rows,
            self.descriptor.limits.max_output_bytes,
        )?;
        self.output_count = self
            .output_count
            .checked_add(1)
            .filter(|count| *count <= self.descriptor.limits.max_output_rows.max(1))
            .ok_or_else(|| {
                DbError::BackpressureFail("process output batch limit exceeded".into())
            })?;
        self.output_rows = self
            .output_rows
            .checked_add(batch.num_rows())
            .filter(|rows| *rows <= self.descriptor.limits.max_output_rows)
            .ok_or_else(|| DbError::BackpressureFail("process output row limit exceeded".into()))?;
        self.output_bytes = self
            .output_bytes
            .checked_add(batch.get_array_memory_size())
            .filter(|bytes| *bytes <= self.descriptor.limits.max_output_bytes)
            .ok_or_else(|| {
                DbError::BackpressureFail("process output byte limit exceeded".into())
            })?;
        result.output.push(batch);
        Ok(())
    }

    fn finish(self) -> Vec<ProcessActivationResult> {
        self.results
            .into_iter()
            .map(|(activation_id, result)| ProcessActivationResult {
                activation_id,
                output: result.output,
                value: result.value,
                timers: result.timers,
            })
            .collect()
    }
}

pub(super) async fn read_response(
    descriptor: &ProcessFunctionDescriptor,
    activations: &[ProcessActivation],
    ack_bytes: usize,
    inbound: &mut tonic::Streaming<wire::WorkerFrame>,
) -> Result<Vec<ProcessActivationResult>, DbError> {
    let mut response = ResponseAccumulator::new(descriptor, activations, ack_bytes);
    loop {
        let frame = inbound
            .message()
            .await
            .map_err(|error| DbError::Pipeline(format!("process worker response: {error}")))?
            .ok_or_else(|| {
                DbError::InvalidOperation("process worker response is incomplete".into())
            })?;
        if response.accept(frame)? {
            if inbound
                .message()
                .await
                .map_err(|error| DbError::Pipeline(format!("process worker response: {error}")))?
                .is_some()
            {
                return Err(DbError::InvalidOperation(
                    "process frame after completion".into(),
                ));
            }
            return Ok(response.finish());
        }
    }
}
