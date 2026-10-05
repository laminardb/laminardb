use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use laminar_core::state::PartitionKeyCodecV1;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

use super::codec::{decode_activation, encode_batch, encode_mutation, encode_timer};
use super::wire::{self, host_frame, worker_frame};
use super::{MAX_FRAME_BYTES, MAX_INVOCATION_WIRE_BYTES, MAX_IN_FLIGHT, PROTOCOL_VERSION};
use crate::error::DbError;
use crate::process_function::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessFunctionDescriptor,
    ProcessRuntime,
};

/// Rust reference implementation of the v1 remote worker contract. It is stateless with
/// respect to keyed business state; all state arrives in each host activation snapshot.
#[derive(Clone)]
pub struct RustReferenceWorker {
    descriptor: Arc<ProcessFunctionDescriptor>,
    digest: [u8; 32],
    handler: Arc<dyn NativeProcessFunction>,
    credits: Arc<Semaphore>,
    #[cfg(all(test, feature = "cluster"))]
    scope_observer: Option<tokio::sync::mpsc::Sender<wire::Open>>,
}

impl RustReferenceWorker {
    /// Build a worker for a remote Rust descriptor and a bounded number of concurrent calls.
    ///
    /// # Errors
    /// Rejects incompatible descriptors and zero concurrency.
    pub fn new(
        descriptor: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
        max_in_flight: usize,
    ) -> Result<Self, DbError> {
        if descriptor.runtime != ProcessRuntime::RemoteRust
            || max_in_flight == 0
            || max_in_flight > MAX_IN_FLIGHT
        {
            return Err(DbError::InvalidOperation(
                "reference worker requires remote Rust and 1..=32 concurrent calls".into(),
            ));
        }
        let manifest = descriptor.to_manifest_json()?;
        let digest: [u8; 32] = Sha256::digest(manifest).into();
        Ok(Self {
            descriptor: Arc::new(descriptor),
            digest,
            handler,
            credits: Arc::new(Semaphore::new(max_in_flight)),
            #[cfg(all(test, feature = "cluster"))]
            scope_observer: None,
        })
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn with_scope_observer(
        mut self,
        observer: tokio::sync::mpsc::Sender<wire::Open>,
    ) -> Self {
        self.scope_observer = Some(observer);
        self
    }

    /// Serve an already bound loopback listener until shutdown. TLS and authentication are
    /// required before a nonlocal endpoint is admitted.
    ///
    /// # Errors
    /// Returns a transport error or rejects a listener bound to a nonloopback address.
    pub async fn serve_loopback(
        self,
        listener: TcpListener,
        shutdown: CancellationToken,
    ) -> Result<(), DbError> {
        let address = listener
            .local_addr()
            .map_err(|error| DbError::Pipeline(format!("worker listener address: {error}")))?;
        if !address.ip().is_loopback() {
            return Err(DbError::Unsupported(
                "process reference worker only accepts a loopback listener".into(),
            ));
        }
        tonic::transport::Server::builder()
            .add_service(
                wire::process_worker_server::ProcessWorkerServer::new(self)
                    .max_decoding_message_size(MAX_FRAME_BYTES + 64 * 1024)
                    .max_encoding_message_size(MAX_FRAME_BYTES + 64 * 1024),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), shutdown.cancelled())
            .await
            .map_err(|error| DbError::Pipeline(format!("process worker server: {error}")))
    }

    async fn read_request(
        &self,
        mut inbound: tonic::Streaming<wire::HostFrame>,
    ) -> Result<(Vec<u8>, Vec<ProcessActivation>), Status> {
        let first = tokio::time::timeout(Duration::from_secs(5), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("process open frame timed out"))??
            .ok_or_else(|| Status::invalid_argument("missing process open frame"))?;
        let first_len = first.encoded_len();
        let Some(host_frame::Kind::Open(open)) = first.kind else {
            return Err(Status::invalid_argument("process open must be first"));
        };
        self.validate_open(&open)?;
        #[cfg(all(test, feature = "cluster"))]
        if let Some(observer) = &self.scope_observer {
            observer
                .try_send(open.clone())
                .map_err(|_| Status::internal("test scope observer is full or closed"))?;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock precedes Unix epoch"))?
            .as_millis();
        let remaining_ms = u128::from(open.deadline_unix_ms).saturating_sub(now_ms);
        let remaining_ms = u64::try_from(remaining_ms)
            .map_err(|_| Status::invalid_argument("invalid process deadline"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(remaining_ms);
        let vnode_count = NonZeroU32::new(open.vnode_count)
            .ok_or_else(|| Status::invalid_argument("empty process vnode domain"))?;
        let mut total_wire_bytes = first_len;
        let mut total_input_bytes = 0usize;
        let mut activations = Vec::new();
        let mut ids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        loop {
            let frame = tokio::time::timeout_at(deadline, inbound.message())
                .await
                .map_err(|_| Status::deadline_exceeded("process request deadline expired"))??
                .ok_or_else(|| {
                    Status::invalid_argument("process invocation ended without completion")
                })?;
            total_wire_bytes = total_wire_bytes
                .checked_add(frame.encoded_len())
                .filter(|total| *total <= MAX_INVOCATION_WIRE_BYTES)
                .ok_or_else(|| {
                    Status::resource_exhausted("process request wire budget exceeded")
                })?;
            match frame.kind {
                Some(host_frame::Kind::Activation(raw)) => {
                    if activations.len() >= self.descriptor.limits.max_batch_rows {
                        return Err(Status::resource_exhausted(
                            "process batch row limit exceeded",
                        ));
                    }
                    let activation = decode_activation(raw, &self.descriptor)
                        .map_err(|error| Status::invalid_argument(error.to_string()))?;
                    if PartitionKeyCodecV1::vnode_for_encoded(&activation.key, vnode_count)
                        != open.vnode
                        || !ids.insert(activation.id)
                        || !keys.insert(activation.key.to_vec())
                    {
                        return Err(Status::invalid_argument(
                            "process batch contains another vnode, repeated key, or repeated ID",
                        ));
                    }
                    if let crate::process_function::ProcessCallback::Input(batch) =
                        &activation.callback
                    {
                        total_input_bytes = total_input_bytes
                            .checked_add(batch.get_array_memory_size())
                            .filter(|total| *total <= self.descriptor.limits.max_input_bytes)
                            .ok_or_else(|| {
                                Status::resource_exhausted("process input byte limit exceeded")
                            })?;
                    }
                    activations.push(activation);
                }
                Some(host_frame::Kind::End(end)) => {
                    if end.count as usize != activations.len() || activations.is_empty() {
                        return Err(Status::invalid_argument("process end count mismatch"));
                    }
                    if tokio::time::timeout_at(deadline, inbound.message())
                        .await
                        .map_err(|_| {
                            Status::deadline_exceeded("process request deadline expired")
                        })??
                        .is_some()
                    {
                        return Err(Status::invalid_argument("process frame after end"));
                    }
                    return Ok((open.attempt_id, activations));
                }
                _ => return Err(Status::invalid_argument("invalid process request frame")),
            }
        }
    }

    fn validate_open(&self, open: &wire::Open) -> Result<(), Status> {
        if open.protocol_version != PROTOCOL_VERSION
            || open.descriptor_sha256 != self.digest
            || open.operator_id.is_empty()
            || open.operator_id.len() > 128
            || open.vnode_count == 0
            || open.vnode >= open.vnode_count
            || open.batch_id.len() != 16
            || open.attempt_id.len() != 16
            || open.batch_id == open.attempt_id
            || open.deadline_unix_ms == 0
        {
            return Err(Status::failed_precondition(
                "process protocol, descriptor, or invocation scope mismatch",
            ));
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock precedes Unix epoch"))?
            .as_millis();
        if now_ms >= u128::from(open.deadline_unix_ms)
            || u128::from(open.deadline_unix_ms) - now_ms > 30_000
        {
            return Err(Status::deadline_exceeded(
                "process invocation deadline expired or exceeds 30 seconds",
            ));
        }
        Ok(())
    }

    fn encode_response(
        &self,
        attempt_id: Vec<u8>,
        activation_ids: &[u64],
        results: Vec<ProcessActivationResult>,
    ) -> Result<Vec<wire::WorkerFrame>, Status> {
        if results.len() != activation_ids.len() {
            return Err(Status::internal(
                "reference handler returned wrong result count",
            ));
        }
        let mut frames = Vec::new();
        frames.push(wire::WorkerFrame {
            kind: Some(worker_frame::Kind::Ack(wire::OpenAck {
                protocol_version: PROTOCOL_VERSION,
                descriptor_sha256: self.digest.to_vec(),
                attempt_id,
            })),
        });
        let expected = activation_ids.iter().copied().collect::<BTreeSet<_>>();
        let mut seen = BTreeSet::new();
        let mut output_rows = 0usize;
        let mut output_bytes = 0usize;
        let mut output_count = 0usize;
        let mut timer_count = 0usize;
        for result in results {
            if !expected.contains(&result.activation_id) || !seen.insert(result.activation_id) {
                return Err(Status::internal(
                    "reference handler returned invalid activation ID",
                ));
            }
            let output_len = result.output.len();
            let output_count_u32 = u32::try_from(output_len)
                .map_err(|_| Status::resource_exhausted("too many process output batches"))?;
            output_count = output_count
                .checked_add(output_len)
                .filter(|count| *count <= self.descriptor.limits.max_output_rows.max(1))
                .ok_or_else(|| Status::resource_exhausted("process output batch limit exceeded"))?;
            timer_count = timer_count
                .checked_add(result.timers.len())
                .filter(|count| *count <= self.descriptor.limits.max_timers)
                .ok_or_else(|| Status::resource_exhausted("process result timer limit exceeded"))?;
            frames.push(wire::WorkerFrame {
                kind: Some(worker_frame::Kind::Result(wire::Result {
                    activation_id: result.activation_id,
                    output_count: output_count_u32,
                    mutation: Some(encode_mutation(result.value)),
                    timers: result.timers.into_iter().map(encode_timer).collect(),
                })),
            });
            for batch in result.output {
                output_rows = output_rows
                    .checked_add(batch.num_rows())
                    .filter(|rows| *rows <= self.descriptor.limits.max_output_rows)
                    .ok_or_else(|| {
                        Status::resource_exhausted("process output row limit exceeded")
                    })?;
                output_bytes = output_bytes
                    .checked_add(batch.get_array_memory_size())
                    .filter(|bytes| *bytes <= self.descriptor.limits.max_output_bytes)
                    .ok_or_else(|| {
                        Status::resource_exhausted("process output byte limit exceeded")
                    })?;
                let ipc = encode_batch(&batch, &self.descriptor.output_schema)
                    .map_err(|error| Status::internal(error.to_string()))?;
                frames.push(wire::WorkerFrame {
                    kind: Some(worker_frame::Kind::Output(wire::Output {
                        activation_id: result.activation_id,
                        arrow_ipc: ipc,
                    })),
                });
            }
        }
        frames.push(wire::WorkerFrame {
            kind: Some(worker_frame::Kind::Complete(wire::Complete {
                result_count: u32::try_from(seen.len())
                    .map_err(|_| Status::resource_exhausted("process result count overflow"))?,
                output_count: u32::try_from(output_count)
                    .map_err(|_| Status::resource_exhausted("process output count overflow"))?,
            })),
        });
        frames.iter().try_fold(0usize, |sum, frame| {
            sum.checked_add(frame.encoded_len())
                .filter(|total| *total <= MAX_INVOCATION_WIRE_BYTES)
                .ok_or_else(|| Status::resource_exhausted("process response wire budget exceeded"))
        })?;
        Ok(frames)
    }
}

#[tonic::async_trait]
impl wire::process_worker_server::ProcessWorker for RustReferenceWorker {
    type ExchangeStream =
        std::pin::Pin<Box<dyn Stream<Item = Result<wire::WorkerFrame, Status>> + Send>>;

    async fn exchange(
        &self,
        request: Request<tonic::Streaming<wire::HostFrame>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        // INVARIANT: the permit remains with the response stream until it is sent or cancelled.
        let permit = self
            .credits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("process worker invocation slots full"))?;
        let inbound = request.into_inner();
        let (attempt_id, activations) = self.read_request(inbound).await?;
        let handler = Arc::clone(&self.handler);
        let activation_ids = activations.iter().map(|item| item.id).collect::<Vec<_>>();
        // A cancelled RPC can leave its blocking handler running. Ownership of the permit
        // moves into that task, so cancellation cannot oversubscribe the worker.
        let (permit, results) = tokio::task::spawn_blocking(move || {
            let results = handler.invoke(&activations);
            (permit, results)
        })
        .await
        .map_err(|error| Status::internal(format!("process handler task failed: {error}")))?;
        let frames = match results {
            Ok(results) => self.encode_response(attempt_id, &activation_ids, results)?,
            Err(error) => vec![wire::WorkerFrame {
                kind: Some(worker_frame::Kind::Failure(wire::Failure {
                    message: error.to_string(),
                })),
            }],
        };
        let stream = tokio_stream::iter(frames.into_iter().map(move |frame| {
            let _keep_credit = &permit;
            Ok(frame)
        }));
        Ok(Response::new(Box::pin(stream)))
    }
}
