use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use laminar_core::state::PartitionKeyCodecV1;
use prost::Message;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use super::codec::encode_activation;
use super::response::read_response;
use super::wire::{self, host_frame, worker_frame};
use super::{
    RemoteInvocationScope, MAX_FRAME_BYTES, MAX_INVOCATION_WIRE_BYTES, MAX_IN_FLIGHT,
    PROTOCOL_VERSION,
};
use crate::error::DbError;
use crate::process_function::{
    ProcessActivation, ProcessActivationResult, ProcessFunctionDescriptor, ProcessRuntime,
};

/// Bounded v1 gRPC/Arrow client. Clones share one connection pool and one invocation credit pool.
/// Only explicit IP loopback endpoints are accepted by this development transport.
#[derive(Clone)]
pub struct RemoteProcessClient {
    channel: Channel,
    descriptor: Arc<ProcessFunctionDescriptor>,
    digest: [u8; 32],
    credits: Arc<Semaphore>,
    max_in_flight: usize,
    timeout: Duration,
    python_replay_lifetime: Option<CancellationToken>,
}

impl RemoteProcessClient {
    pub(super) fn bind_python_replay_lifetime(&mut self, exited: CancellationToken) {
        self.python_replay_lifetime = Some(exited);
    }

    pub(crate) fn has_python_replay_binding(&self) -> bool {
        self.python_replay_lifetime
            .as_ref()
            .is_some_and(|exited| !exited.is_cancelled())
    }

    /// Immutable descriptor negotiated when this client connected.
    #[must_use]
    pub fn descriptor(&self) -> &ProcessFunctionDescriptor {
        &self.descriptor
    }

    /// Number of invocation credits shared by clones of this client.
    #[must_use]
    pub const fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Connect to a reference worker over an explicit loopback address.
    ///
    /// # Errors
    /// Rejects nonloopback/plaintext deployment, invalid bounds, or connection failure.
    pub async fn connect_loopback(
        endpoint: &str,
        descriptor: ProcessFunctionDescriptor,
        max_in_flight: usize,
        timeout: Duration,
    ) -> Result<Self, DbError> {
        let address = endpoint
            .strip_prefix("http://")
            .and_then(|value| value.parse::<std::net::SocketAddr>().ok())
            .filter(|address| address.ip().is_loopback())
            .ok_or_else(|| {
                DbError::Unsupported("process worker plaintext endpoint must be loopback".into())
            })?;
        if max_in_flight == 0
            || max_in_flight > MAX_IN_FLIGHT
            || timeout < Duration::from_millis(1)
            || timeout > Duration::from_secs(30)
            || descriptor.runtime == ProcessRuntime::NativeRust
        {
            return Err(DbError::InvalidOperation(
                "remote process client requires a remote runtime, deadline, and 1..=32 credits"
                    .into(),
            ));
        }
        let manifest = descriptor.to_manifest_json()?;
        let digest: [u8; 32] = Sha256::digest(manifest).into();
        let uri = format!("http://{address}");
        let channel = Endpoint::from_shared(uri)
            .map_err(|error| DbError::Config(format!("process worker endpoint: {error}")))?
            .connect_timeout(timeout)
            .connect()
            .await
            .map_err(|error| DbError::Pipeline(format!("connect process worker: {error}")))?;
        Ok(Self {
            channel,
            descriptor: Arc::new(descriptor),
            digest,
            credits: Arc::new(Semaphore::new(max_in_flight)),
            max_in_flight,
            timeout,
            python_replay_lifetime: None,
        })
    }

    /// Invoke one key-distinct, single-vnode batch. A complete validated response is returned;
    /// the caller must still validate and apply it under its authoritative ownership fence.
    ///
    /// # Errors
    /// Rejects malformed inputs/responses, budget excess, timeout, or transport failure.
    pub async fn invoke(
        &self,
        scope: &RemoteInvocationScope,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        if self
            .python_replay_lifetime
            .as_ref()
            .is_some_and(CancellationToken::is_cancelled)
        {
            return Err(DbError::Pipeline(
                "replay-bound Python worker has exited".into(),
            ));
        }
        // Bounded admission: callers retry on backpressure rather than queueing unbounded RPCs.
        let _permit = self.credits.try_acquire().map_err(|_| {
            DbError::BackpressureFail("process worker invocation slots full".into())
        })?;
        let request = self.encode_request(scope, activations)?;
        tokio::time::timeout(self.timeout, self.exchange(scope, activations, request))
            .await
            .map_err(|_| DbError::Pipeline("process worker invocation timed out".into()))?
    }

    fn encode_request(
        &self,
        scope: &RemoteInvocationScope,
        activations: &[ProcessActivation],
    ) -> Result<Vec<wire::HostFrame>, DbError> {
        let vnode_count = NonZeroU32::new(scope.vnode_count)
            .filter(|count| scope.vnode < count.get())
            .ok_or_else(|| DbError::InvalidOperation("invalid process invocation vnode".into()))?;
        if scope.operator_id.is_empty()
            || scope.operator_id.len() > 128
            || scope.batch_id == scope.attempt_id
            || activations.is_empty()
            || activations.len() > self.descriptor.limits.max_batch_rows
        {
            return Err(DbError::InvalidOperation(
                "invalid process invocation scope or size".into(),
            ));
        }
        let deadline_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DbError::Pipeline("system clock precedes Unix epoch".into()))?
            .as_millis()
            .checked_add(self.timeout.as_millis())
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| DbError::Pipeline("process invocation deadline overflow".into()))?;
        let mut frames = vec![wire::HostFrame {
            kind: Some(host_frame::Kind::Open(wire::Open {
                protocol_version: PROTOCOL_VERSION,
                descriptor_sha256: self.digest.to_vec(),
                operator_id: scope.operator_id.clone(),
                vnode: scope.vnode,
                owner_generation: scope.owner_generation,
                recovery_generation: scope.recovery_generation,
                batch_id: scope.batch_id.as_bytes().to_vec(),
                attempt_id: scope.attempt_id.as_bytes().to_vec(),
                input_watermark_us: scope.input_watermark_us,
                deadline_unix_ms,
                vnode_count: scope.vnode_count,
            })),
        }];
        let mut ids = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut input_bytes = 0usize;
        for activation in activations {
            if !ids.insert(activation.id)
                || !keys.insert(activation.key.as_ref())
                || PartitionKeyCodecV1::vnode_for_encoded(&activation.key, vnode_count)
                    != scope.vnode
            {
                return Err(DbError::InvalidOperation(
                    "process invocation has another vnode, repeated key, or repeated ID".into(),
                ));
            }
            let encoded = encode_activation(activation, &self.descriptor)?;
            if let Some(wire::activation::Callback::InputIpc(ipc)) = &encoded.callback {
                // A one-row Arrow slice can retain the parent batch's full buffers. Charge the
                // bytes actually sent rather than multiplying that allocation by row count.
                input_bytes = input_bytes
                    .checked_add(ipc.len())
                    .filter(|bytes| *bytes <= self.descriptor.limits.max_input_bytes)
                    .ok_or_else(|| {
                        DbError::BackpressureFail("process input IPC byte limit exceeded".into())
                    })?;
            }
            frames.push(wire::HostFrame {
                kind: Some(host_frame::Kind::Activation(encoded)),
            });
        }
        frames.push(wire::HostFrame {
            kind: Some(host_frame::Kind::End(wire::End {
                count: u32::try_from(activations.len()).map_err(|_| {
                    DbError::BackpressureFail("process batch count overflow".into())
                })?,
            })),
        });
        frames.iter().try_fold(0usize, |sum, frame| {
            sum.checked_add(frame.encoded_len())
                .filter(|bytes| *bytes <= MAX_INVOCATION_WIRE_BYTES)
                .ok_or_else(|| {
                    DbError::BackpressureFail("process request wire budget exceeded".into())
                })
        })?;
        Ok(frames)
    }

    async fn exchange(
        &self,
        scope: &RemoteInvocationScope,
        activations: &[ProcessActivation],
        frames: Vec<wire::HostFrame>,
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        let mut client =
            wire::process_worker_client::ProcessWorkerClient::new(self.channel.clone())
                .max_decoding_message_size(MAX_FRAME_BYTES + 64 * 1024)
                .max_encoding_message_size(MAX_FRAME_BYTES + 64 * 1024);
        let mut request = Request::new(tokio_stream::iter(frames));
        request.set_timeout(self.timeout);
        let mut inbound = client
            .exchange(request)
            .await
            .map_err(|error| DbError::Pipeline(format!("process worker exchange: {error}")))?
            .into_inner();
        let ack = inbound
            .message()
            .await
            .map_err(|error| DbError::Pipeline(format!("process worker response: {error}")))?
            .ok_or_else(|| DbError::Pipeline("process worker closed without response".into()))?;
        let ack_bytes = ack.encoded_len();
        match ack.kind {
            Some(worker_frame::Kind::Ack(ack))
                if ack.protocol_version == PROTOCOL_VERSION
                    && ack.descriptor_sha256 == self.digest
                    && ack.attempt_id == scope.attempt_id.as_bytes() => {}
            Some(worker_frame::Kind::Failure(failure)) => {
                return Err(DbError::Pipeline(format!(
                    "process worker failed: {}",
                    failure.message
                )));
            }
            _ => {
                return Err(DbError::InvalidOperation(
                    "invalid process worker acknowledgement".into(),
                ))
            }
        }
        read_response(&self.descriptor, activations, ack_bytes, &mut inbound).await
    }
}
