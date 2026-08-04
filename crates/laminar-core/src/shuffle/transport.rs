//! Cross-node shuffle transport over Tonic gRPC client-streaming.
//!
//! A leading `Hello` binds each stream to certified process incarnations and an
//! assignment. Per-peer and per-node byte admission propagates backpressure to
//! the HTTP/2 stream.

use super::message::ShuffleMessage;
#[cfg(feature = "cluster")]
use crate::checkpoint::CheckpointAttempt;

/// Secondary queue and holdover item bound; byte semaphores are the primary
/// admission control in cluster mode.
const SHUFFLE_RECV_QUEUE: usize = 256;
const MAX_STAGE_NAME_BYTES: usize = 4096;

/// Peer identifier on the wire; matches `cluster::discovery::NodeId`'s inner type.
pub type ShufflePeerId = u64;

const SCOPE_CANCELLED: &str = "shuffle assignment or recovery scope was cancelled";
const NONCANONICAL_BARRIER: &str =
    "shuffle checkpoint barrier must use one nonzero canonical checkpoint ID";

fn validate_checkpoint_barrier(
    barrier: crate::checkpoint::CheckpointBarrier,
) -> std::io::Result<()> {
    if barrier.is_canonical() {
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            NONCANONICAL_BARRIER,
        ))
    }
}

fn validate_frontier(stage: &str, watermark: Option<i64>) -> std::io::Result<()> {
    if stage.is_empty() || stage.len() > MAX_STAGE_NAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "shuffle stage scope is empty or too long",
        ));
    }
    if watermark == Some(i64::MIN) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "shuffle frontier uses the reserved uninitialized watermark",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct ScopeCancelled;

impl std::fmt::Display for ScopeCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(SCOPE_CANCELLED)
    }
}

impl std::error::Error for ScopeCancelled {}

#[cfg(feature = "cluster")]
fn scope_cancelled_io() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::ConnectionAborted, ScopeCancelled)
}

/// Whether an outbound shuffle operation was cancelled by an assignment or recovery scope
/// transition. Generic connection cancellation is deliberately not classified as a scope change.
#[must_use]
pub fn is_scope_cancelled(error: &std::io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<ScopeCancelled>())
        .is_some()
}

/// Gossip KV key under which a receiver publishes its listener address for peer
/// discovery.
#[cfg(feature = "cluster")]
pub const SHUFFLE_ADDR_KEY: &str = "shuffle:addr";

#[cfg(feature = "cluster")]
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    missing_docs
)]
pub(crate) mod shuffle_v1 {
    tonic::include_proto!("laminar.shuffle.v1");
}

/// Inbound staging shared by both builds. Ordered controls stop the data drainer
/// and remain under the same item bound as the receive queue.
struct Holdover {
    staged: parking_lot::Mutex<rustc_hash::FxHashMap<String, Vec<ReceivedBatch>>>,
    frontiers: parking_lot::Mutex<Vec<ReceivedFrontierCut>>,
    barriers: parking_lot::Mutex<BarrierHoldover>,
    /// Shared across data and controls so repeatedly draining the bounded receive
    /// queue cannot turn it into an unbounded secondary queue.
    items: std::sync::atomic::AtomicUsize,
    capacity: usize,
}

#[derive(Default)]
struct BarrierHoldover {
    staged: Vec<ReceivedShuffle>,
    #[cfg(feature = "cluster")]
    retired_through: Option<RetiredCheckpoint>,
}

#[cfg(feature = "cluster")]
#[derive(Clone, Copy)]
struct RetiredCheckpoint {
    attempt: CheckpointAttempt,
    assignment_digest: [u8; 32],
}

impl BarrierHoldover {
    #[cfg(feature = "cluster")]
    fn is_retired(
        &self,
        attempt: CheckpointAttempt,
        assignment_digest: Option<[u8; 32]>,
    ) -> std::io::Result<bool> {
        let Some(retired) = self.retired_through else {
            return Ok(false);
        };
        match attempt.checkpoint_id.cmp(&retired.attempt.checkpoint_id) {
            std::cmp::Ordering::Less => Ok(true),
            std::cmp::Ordering::Greater => Ok(false),
            std::cmp::Ordering::Equal
                if assignment_digest == Some(retired.assignment_digest) =>
            {
                Ok(true)
            }
            std::cmp::Ordering::Equal => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "retired checkpoint barrier {attempt:?} has a different assignment digest from its durable terminal outcome"
                ),
            )),
        }
    }
}

impl Holdover {
    fn new(capacity: usize) -> Self {
        Self {
            staged: parking_lot::Mutex::default(),
            frontiers: parking_lot::Mutex::default(),
            barriers: parking_lot::Mutex::default(),
            items: std::sync::atomic::AtomicUsize::new(0),
            capacity,
        }
    }

    fn try_reserve_item(&self) -> bool {
        self.items
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |items| (items < self.capacity).then_some(items + 1),
            )
            .is_ok()
    }

    fn release_items(&self, count: usize) {
        if count == 0 {
            return;
        }
        let released = self.items.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |items| items.checked_sub(count),
        );
        debug_assert!(
            released.is_ok(),
            "shuffle holdover item accounting underflow"
        );
    }

    #[cfg(feature = "cluster")]
    fn barrier_attempt(received: &ReceivedShuffle) -> Option<CheckpointAttempt> {
        let ShuffleMessage::Barrier(barrier) = received.message() else {
            return None;
        };
        let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
        attempt.is_canonical().then_some(attempt)
    }

    #[cfg(feature = "cluster")]
    fn is_retired_barrier(&self, received: &ReceivedShuffle) -> std::io::Result<bool> {
        let Some(attempt) = Self::barrier_attempt(received) else {
            return Ok(false);
        };
        self.barriers
            .lock()
            .is_retired(attempt, received.assignment_digest)
    }

    #[cfg(feature = "cluster")]
    fn is_retired_checkpoint_barrier(
        &self,
        barrier: crate::checkpoint::CheckpointBarrier,
        assignment_digest: [u8; 32],
    ) -> std::io::Result<bool> {
        let attempt = CheckpointAttempt::new(barrier.epoch, barrier.checkpoint_id);
        if !attempt.is_canonical() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                NONCANONICAL_BARRIER,
            ));
        }
        self.barriers
            .lock()
            .is_retired(attempt, Some(assignment_digest))
    }

    fn has_staged_barriers(&self) -> bool {
        !self.barriers.lock().staged.is_empty()
    }

    fn stage_barrier(&self, barrier: ReceivedShuffle) -> std::io::Result<bool> {
        let ShuffleMessage::Barrier(value) = barrier.message() else {
            return Ok(false);
        };
        if !value.is_canonical() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                NONCANONICAL_BARRIER,
            ));
        }
        let mut holdover = self.barriers.lock();
        #[cfg(feature = "cluster")]
        if let Some(attempt) = Self::barrier_attempt(&barrier) {
            if holdover.is_retired(attempt, barrier.assignment_digest)? {
                return Ok(false);
            }
        }
        holdover.staged.push(barrier);
        Ok(true)
    }

    fn take_staged_barriers(&self) -> Vec<ReceivedShuffle> {
        std::mem::take(&mut self.barriers.lock().staged)
    }

    fn has_staged_frontiers(&self) -> bool {
        !self.frontiers.lock().is_empty()
    }

    fn stage_frontier(&self, frontier: ReceivedFrontierCut) {
        self.frontiers.lock().push(frontier);
    }

    fn take_staged_frontiers(&self) -> Vec<ReceivedFrontierCut> {
        std::mem::take(&mut *self.frontiers.lock())
    }

    #[cfg(feature = "cluster")]
    fn clear_staged_frontiers(&self) {
        let frontiers = self.take_staged_frontiers();
        let item_count = frontiers.iter().map(ReceivedFrontierCut::item_count).sum();
        self.release_items(item_count);
    }

    #[cfg(feature = "cluster")]
    fn retire_checkpoint_attempt(
        &self,
        attempt: CheckpointAttempt,
        assignment_digest: [u8; 32],
    ) -> std::io::Result<()> {
        if !attempt.is_canonical() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "retired checkpoint attempt must use one nonzero canonical checkpoint ID",
            ));
        }

        let removed = {
            let mut holdover = self.barriers.lock();
            let retired = match holdover.retired_through {
                None => RetiredCheckpoint {
                    attempt,
                    assignment_digest,
                },
                Some(retired) if attempt.checkpoint_id > retired.attempt.checkpoint_id => {
                    RetiredCheckpoint {
                        attempt,
                        assignment_digest,
                    }
                }
                Some(retired) if attempt.checkpoint_id == retired.attempt.checkpoint_id => {
                    if assignment_digest != retired.assignment_digest {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "checkpoint retirement {attempt:?} has a different assignment digest from its high-water"
                            ),
                        ));
                    }
                    retired
                }
                Some(retired) => retired,
            };
            for barrier in &holdover.staged {
                let Some(candidate) = Self::barrier_attempt(barrier) else {
                    continue;
                };
                if candidate.checkpoint_id == retired.attempt.checkpoint_id
                    && barrier.assignment_digest != Some(retired.assignment_digest)
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "staged checkpoint barrier {candidate:?} has a different assignment digest from its durable terminal outcome"
                        ),
                    ));
                }
            }
            holdover.retired_through = Some(retired);
            let staged_before = holdover.staged.len();
            holdover.staged.retain(|barrier| {
                !Self::barrier_attempt(barrier).is_some_and(|candidate| {
                    candidate.checkpoint_id <= retired.attempt.checkpoint_id
                })
            });
            staged_before - holdover.staged.len()
        };
        self.release_items(removed);
        Ok(())
    }
}

impl Default for Holdover {
    fn default() -> Self {
        Self::new(SHUFFLE_RECV_QUEUE)
    }
}

/// A decoded shuffle batch together with the memory admission charged for its
/// source IPC payload.
///
/// The admission remains charged while this value lives, so consumers must
/// retain the envelope while processing shallow clones of the batch.
pub struct ReceivedBatch {
    batch: arrow_array::RecordBatch,
    routed_vnodes: std::sync::Arc<[u32]>,
    reservation: Option<std::sync::Arc<InboundReservation>>,
    peer: ShufflePeerId,
    sender_incarnation: uuid::Uuid,
    receiver_incarnation: uuid::Uuid,
    stream_id: uuid::Uuid,
    assignment_version: u64,
    recovery_gen: u64,
    checkpoint_sequence: u64,
}

/// Opaque ownership of the memory admission charged to a decoded shuffle batch.
/// The charge is released when the last clone is dropped.
#[must_use]
#[derive(Clone)]
pub struct ShuffleBatchAdmission(Option<std::sync::Arc<InboundReservation>>);

impl std::fmt::Debug for ShuffleBatchAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShuffleBatchAdmission")
            .field("admitted", &self.0.is_some())
            .finish()
    }
}

impl ReceivedBatch {
    /// Borrow the decoded batch without releasing its inbound admission.
    #[must_use]
    pub const fn batch(&self) -> &arrow_array::RecordBatch {
        &self.batch
    }

    /// Canonical receiver-owned vnode set carried with this batch.
    #[must_use]
    pub fn routed_vnodes(&self) -> &[u32] {
        &self.routed_vnodes
    }

    /// Peer and exact transport stream that delivered this batch.
    #[must_use]
    pub const fn peer(&self) -> ShufflePeerId {
        self.peer
    }

    /// Sender process incarnation bound by the stream handshake.
    #[must_use]
    pub const fn sender_incarnation(&self) -> uuid::Uuid {
        self.sender_incarnation
    }

    /// Receiver process incarnation bound by the stream handshake.
    #[must_use]
    pub const fn receiver_incarnation(&self) -> uuid::Uuid {
        self.receiver_incarnation
    }

    /// Connection identity bound by the stream handshake.
    #[must_use]
    pub const fn stream_id(&self) -> uuid::Uuid {
        self.stream_id
    }

    /// Assignment version bound by the stream handshake.
    #[must_use]
    pub const fn assignment_version(&self) -> u64 {
        self.assignment_version
    }

    /// Recovery generation bound by the stream handshake.
    #[must_use]
    pub const fn recovery_gen(&self) -> u64 {
        self.recovery_gen
    }

    /// Zero-based logical-frame sequence for this data batch.
    #[must_use]
    pub const fn checkpoint_sequence(&self) -> u64 {
        self.checkpoint_sequence
    }

    /// Split the decoded batch from its admission so a retaining consumer can keep the charge for
    /// exactly as long as shallow Arrow views remain live. Route metadata is not returned; callers
    /// that need it must inspect [`Self::routed_vnodes`] before consuming this envelope.
    pub fn into_parts(self) -> (arrow_array::RecordBatch, ShuffleBatchAdmission) {
        (self.batch, ShuffleBatchAdmission(self.reservation))
    }
}

impl std::fmt::Debug for ReceivedBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceivedBatch")
            .field("batch", &self.batch)
            .field("routed_vnodes", &self.routed_vnodes)
            .field("peer", &self.peer)
            .field("sender_incarnation", &self.sender_incarnation)
            .field("receiver_incarnation", &self.receiver_incarnation)
            .field("stream_id", &self.stream_id)
            .field("assignment_version", &self.assignment_version)
            .field("recovery_gen", &self.recovery_gen)
            .field("checkpoint_sequence", &self.checkpoint_sequence)
            .field("admitted", &self.reservation.is_some())
            .finish()
    }
}

/// A received shuffle message whose decoded memory remains admitted until the
/// envelope is dropped. Consumers retain it while processing shallow clones of
/// the message or its batch.
pub struct ReceivedShuffle {
    peer: ShufflePeerId,
    message: ShuffleMessage,
    reservation: Option<std::sync::Arc<InboundReservation>>,
    sender_incarnation: uuid::Uuid,
    receiver_incarnation: uuid::Uuid,
    stream_id: uuid::Uuid,
    assignment_version: u64,
    assignment_digest: Option<[u8; 32]>,
    recovery_gen: u64,
    checkpoint_sequence: u64,
}

impl ReceivedShuffle {
    /// Peer that sent this message.
    #[must_use]
    pub const fn peer(&self) -> ShufflePeerId {
        self.peer
    }

    /// Borrow the message without releasing its inbound admission.
    #[must_use]
    pub const fn message(&self) -> &ShuffleMessage {
        &self.message
    }

    /// Sender process incarnation bound by the stream handshake.
    #[must_use]
    pub const fn sender_incarnation(&self) -> uuid::Uuid {
        self.sender_incarnation
    }

    /// Receiver process incarnation bound by the stream handshake.
    #[must_use]
    pub const fn receiver_incarnation(&self) -> uuid::Uuid {
        self.receiver_incarnation
    }

    /// Connection identity bound by the stream handshake.
    #[must_use]
    pub const fn stream_id(&self) -> uuid::Uuid {
        self.stream_id
    }

    /// Assignment version carried by the stream that delivered this message.
    #[must_use]
    pub const fn assignment_version(&self) -> u64 {
        self.assignment_version
    }

    /// Exact checkpoint assignment-certificate digest carried by a barrier.
    #[must_use]
    pub const fn assignment_digest(&self) -> Option<[u8; 32]> {
        self.assignment_digest
    }

    /// Recovery generation carried by the stream that delivered this message.
    #[must_use]
    pub const fn recovery_gen(&self) -> u64 {
        self.recovery_gen
    }

    /// Data and frontiers carry their logical-frame sequence. A barrier carries the exclusive
    /// high-water sequence it closes.
    #[must_use]
    pub const fn checkpoint_sequence(&self) -> u64 {
        self.checkpoint_sequence
    }

    /// Split the message from any decoded-data admission after its transport
    /// scope has been validated.
    pub fn into_parts(self) -> (ShuffleMessage, ShuffleBatchAdmission) {
        (self.message, ShuffleBatchAdmission(self.reservation))
    }
}

impl std::fmt::Debug for ReceivedShuffle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceivedShuffle")
            .field("peer", &self.peer)
            .field("message", &self.message)
            .field("stream_id", &self.stream_id)
            .field("assignment_version", &self.assignment_version)
            .field("recovery_gen", &self.recovery_gen)
            .field("checkpoint_sequence", &self.checkpoint_sequence)
            .field("admitted", &self.reservation.is_some())
            .finish_non_exhaustive()
    }
}

/// A frontier and the same-stage batches from its ordered peer stream that precede it.
#[derive(Debug)]
pub struct ReceivedFrontierCut {
    preceding: Vec<ReceivedBatch>,
    frontier: ReceivedShuffle,
}

impl ReceivedFrontierCut {
    /// Batches that must be applied before the frontier.
    #[must_use]
    pub fn preceding(&self) -> &[ReceivedBatch] {
        &self.preceding
    }

    /// Frontier closing the returned batch prefix.
    #[must_use]
    pub const fn frontier(&self) -> &ReceivedShuffle {
        &self.frontier
    }

    /// Consume the cut in application order: batches first, then frontier.
    #[must_use]
    pub fn into_parts(self) -> (Vec<ReceivedBatch>, ReceivedShuffle) {
        (self.preceding, self.frontier)
    }

    fn item_count(&self) -> usize {
        self.preceding.len() + 1
    }

    fn stage(&self) -> &str {
        let ShuffleMessage::Frontier { stage, .. } = self.frontier.message() else {
            unreachable!("frontier cut contains a frontier message");
        };
        stage
    }
}

fn take_frontier_prefix(
    staged: &mut rustc_hash::FxHashMap<String, Vec<ReceivedBatch>>,
    stage: &str,
    frontier: &ReceivedShuffle,
) -> Vec<ReceivedBatch> {
    let Some(batches) = staged.remove(stage) else {
        return Vec::new();
    };
    let (preceding, remaining): (Vec<_>, Vec<_>) = batches.into_iter().partition(|batch| {
        batch.peer == frontier.peer
            && batch.sender_incarnation == frontier.sender_incarnation
            && batch.receiver_incarnation == frontier.receiver_incarnation
            && batch.assignment_version == frontier.assignment_version
            && batch.recovery_gen == frontier.recovery_gen
            && batch.checkpoint_sequence < frontier.checkpoint_sequence
    });
    if !remaining.is_empty() {
        staged.insert(stage.to_owned(), remaining);
    }
    preceding
}

#[cfg(feature = "cluster")]
struct InboundReservation {
    node: tokio::sync::OwnedSemaphorePermit,
    peer: tokio::sync::OwnedSemaphorePermit,
    wire_bytes: usize,
}

#[cfg(not(feature = "cluster"))]
struct InboundReservation;

#[cfg(feature = "cluster")]
mod grpc {
    use std::collections::hash_map::Entry;
    use std::collections::VecDeque;
    use std::io;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, OnceLock};

    use arrow_array::RecordBatch;
    use bytes::Bytes;
    use crossfire::{mpsc, AsyncRx, MAsyncTx};
    use futures::StreamExt as _;
    use parking_lot::{Mutex, RwLock};
    use rustc_hash::FxHashMap;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};
    use tokio::task::JoinHandle;
    use tokio_util::sync::CancellationToken;
    use tonic::transport::{Channel, Server};
    use tonic::Request;
    use uuid::Uuid;

    use super::shuffle_v1::shuffle_frame;
    use super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
    use super::shuffle_v1::shuffle_transport_server::{ShuffleTransport, ShuffleTransportServer};
    use super::shuffle_v1::{
        Barrier, Frontier as WireFrontier, HandshakeRequest, HandshakeResponse, Hello, RoutedData,
        ShuffleFrame, ShuffleSummary,
    };
    use super::{
        is_scope_cancelled, scope_cancelled_io, take_frontier_prefix, validate_checkpoint_barrier,
        validate_frontier, CheckpointAttempt, Holdover, InboundReservation, ReceivedBatch,
        ReceivedFrontierCut, ReceivedShuffle, ShuffleMessage, ShufflePeerId, MAX_STAGE_NAME_BYTES,
        NONCANONICAL_BARRIER, SCOPE_CANCELLED, SHUFFLE_ADDR_KEY, SHUFFLE_RECV_QUEUE,
    };
    use crate::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};
    use crate::cluster::control::{ClusterKv, LeaseDeadline};
    use crate::serialization::{serialize_batch_stream_bounded, BatchStreamDecoder};

    const SEND_QUEUE: usize = 256;

    const OUTBOUND_PEER_BUDGET_BYTES: usize = 32 * 1024 * 1024;
    const CHECKPOINTED_CONTROL_PEER_BUDGET_BYTES: usize = 256 * 1024;
    const CHECKPOINTED_CONTROL_NODE_BUDGET_BYTES: usize = 4 * 1024 * 1024;
    const OUTBOUND_NODE_BUDGET_BYTES: usize = 128 * 1024 * 1024;
    const INBOUND_PEER_BUDGET_BYTES: usize = 32 * 1024 * 1024;
    const INBOUND_NODE_BUDGET_BYTES: usize = 128 * 1024 * 1024;
    const MAX_SOURCE_SCHEMA_MEMORY_BYTES: usize = 256 * 1024;
    /// The encoded schema is the canonical protocol bound. It must fit entirely in fragment zero
    /// so a receiver can verify it before allocating an Arrow decoder.
    const MAX_SCHEMA_WIRE_BYTES: usize = 512 * 1024;
    /// Decoding uses fresh, right-sized strings and maps, but retain explicit allocator headroom
    /// above the sender bound so an accepted schema cannot fail only because capacities differ.
    const MAX_DECODED_SCHEMA_MEMORY_BYTES: usize = 1024 * 1024;
    const MAX_DECODED_ARRAY_STRUCTURE_BYTES: usize = 2 * MAX_DECODED_SCHEMA_MEMORY_BYTES;
    const MAX_ROUTE_METADATA_BYTES: usize =
        crate::state::MAX_KEY_GROUP_COUNT as usize * std::mem::size_of::<u32>();
    /// Frame envelopes, collection allocations, and protobuf bookkeeping beyond exact payload,
    /// schema, stage, and route bytes.
    const FRAME_METADATA_BYTES: usize = 64 * 1024;
    const RETAINED_BATCH_ENVELOPE_BYTES: usize = 4 * 1024;
    const INBOUND_BATCH_METADATA_BYTES: usize = MAX_DECODED_SCHEMA_MEMORY_BYTES
        + MAX_DECODED_ARRAY_STRUCTURE_BYTES
        + (2 * MAX_ROUTE_METADATA_BYTES)
        + (2 * MAX_STAGE_NAME_BYTES)
        + FRAME_METADATA_BYTES;
    const MAX_WIRE_PAYLOAD_BYTES: usize = 1024 * 1024;
    const _: () = assert!(MAX_SCHEMA_WIRE_BYTES + 8 <= MAX_WIRE_PAYLOAD_BYTES);
    const MAX_FRAGMENTS: usize =
        crate::shuffle::message::MAX_PAYLOAD_BYTES / MAX_WIRE_PAYLOAD_BYTES;
    const BLOCKING_IPC_THRESHOLD_BYTES: usize = 512 * 1024;
    const FRONTIER_WORKSPACE_BYTES: usize = 2 * MAX_STAGE_NAME_BYTES + 1024;
    /// One wire fragment plus protobuf metadata. Logical Arrow payloads are reassembled under a
    /// separate 16 MiB cap.
    const MAX_SHUFFLE_MESSAGE_BYTES: usize = 2 * 1024 * 1024;
    const OUTBOUND_DATA_WORKSPACE_BYTES: usize = crate::shuffle::message::MAX_PAYLOAD_BYTES
        + crate::shuffle::ROUTE_MAX_BATCH_BYTES
        + MAX_SOURCE_SCHEMA_MEMORY_BYTES
        + (2 * MAX_ROUTE_METADATA_BYTES)
        + (2 * MAX_STAGE_NAME_BYTES)
        + FRAME_METADATA_BYTES;
    // Tonic may own one decoded-but-not-yet-admitted frame per active stream. Keep this memory
    // finite while reserving the full certified roster.
    const UNADMITTED_FRAME_BUDGET_BYTES: usize = 256 * 1024 * 1024;
    const MAX_ACTIVE_STREAMS: usize = UNADMITTED_FRAME_BUDGET_BYTES / MAX_SHUFFLE_MESSAGE_BYTES;
    const _: () = assert!(MAX_ACTIVE_STREAMS >= crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS - 1);
    const MAX_TRACKED_PEERS: usize = 4096;
    const MAX_PENDING_HANDSHAKES: usize = 4096;
    const HANDSHAKE_TOKEN_TTL: std::time::Duration = std::time::Duration::from_secs(30);
    const PROCESS_LEASE_EXPIRED: &str = "shuffle process lease is no longer live";

    type InboundRx = AsyncRx<mpsc::Array<Inbound>>;
    type InboundTx = MAsyncTx<mpsc::Array<Inbound>>;

    fn io_err<E: std::fmt::Display>(e: E) -> io::Error {
        io::Error::other(e.to_string())
    }

    fn scope_cancelled_status() -> tonic::Status {
        tonic::Status::cancelled(SCOPE_CANCELLED)
    }

    fn status_io(status: tonic::Status) -> io::Error {
        if status.code() == tonic::Code::Cancelled && status.message() == SCOPE_CANCELLED {
            scope_cancelled_io()
        } else {
            io_err(status)
        }
    }

    fn process_lease_expired_io() -> io::Error {
        io::Error::new(io::ErrorKind::PermissionDenied, PROCESS_LEASE_EXPIRED)
    }

    fn process_lease_expired_status() -> tonic::Status {
        tonic::Status::failed_precondition(PROCESS_LEASE_EXPIRED)
    }

    struct ProcessLeaseGate {
        deadline: OnceLock<Arc<LeaseDeadline>>,
        cancelled: CancellationToken,
        watcher: Mutex<Option<tokio::task::AbortHandle>>,
    }

    impl Default for ProcessLeaseGate {
        fn default() -> Self {
            Self {
                deadline: OnceLock::new(),
                cancelled: CancellationToken::new(),
                watcher: Mutex::new(None),
            }
        }
    }

    impl ProcessLeaseGate {
        fn is_installed_deadline(&self, deadline: &Arc<LeaseDeadline>) -> bool {
            self.deadline
                .get()
                .is_some_and(|current| Arc::ptr_eq(current, deadline))
        }

        fn install(&self, deadline: Arc<LeaseDeadline>) -> io::Result<()> {
            if !deadline.is_live() {
                return Err(process_lease_expired_io());
            }
            let mut watcher_slot = self.watcher.lock();
            if let Some(current) = self.deadline.get() {
                return if Arc::ptr_eq(current, &deadline) {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "shuffle process lease deadline is already installed",
                    ))
                };
            }
            let runtime = tokio::runtime::Handle::try_current().map_err(|error| {
                io::Error::other(format!(
                    "shuffle process lease requires a Tokio runtime: {error}"
                ))
            })?;
            self.deadline.set(Arc::clone(&deadline)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "shuffle process lease deadline is already installed",
                )
            })?;
            let cancelled = self.cancelled.clone();
            let watcher = runtime.spawn(async move {
                deadline.wait_until_expired().await;
                cancelled.cancel();
            });
            *watcher_slot = Some(watcher.abort_handle());
            Ok(())
        }

        fn install_pair(
            first: &Self,
            second: &Self,
            deadline: Arc<LeaseDeadline>,
        ) -> io::Result<()> {
            let mut first_watcher = first.watcher.lock();
            let mut second_watcher = second.watcher.lock();
            if !deadline.is_live() {
                return Err(process_lease_expired_io());
            }
            for gate in [first, second] {
                if gate
                    .deadline
                    .get()
                    .is_some_and(|current| !Arc::ptr_eq(current, &deadline))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        "shuffle process lease deadline is already installed",
                    ));
                }
            }

            let install_first = first.deadline.get().is_none();
            let install_second = second.deadline.get().is_none();
            let runtime = if install_first || install_second {
                Some(tokio::runtime::Handle::try_current().map_err(|error| {
                    io::Error::other(format!(
                        "shuffle process lease requires a Tokio runtime: {error}"
                    ))
                })?)
            } else {
                None
            };

            if install_first {
                assert!(
                    first.deadline.set(Arc::clone(&deadline)).is_ok(),
                    "first shuffle lease gate changed while its watcher lock was held"
                );
            }
            if install_second {
                assert!(
                    second.deadline.set(Arc::clone(&deadline)).is_ok(),
                    "second shuffle lease gate changed while its watcher lock was held"
                );
            }

            if let Some(runtime) = runtime {
                if install_first {
                    let cancelled = first.cancelled.clone();
                    let first_deadline = Arc::clone(&deadline);
                    let watcher = runtime.spawn(async move {
                        first_deadline.wait_until_expired().await;
                        cancelled.cancel();
                    });
                    *first_watcher = Some(watcher.abort_handle());
                }
                if install_second {
                    let cancelled = second.cancelled.clone();
                    let watcher = runtime.spawn(async move {
                        deadline.wait_until_expired().await;
                        cancelled.cancel();
                    });
                    *second_watcher = Some(watcher.abort_handle());
                }
            }
            Ok(())
        }

        fn require_live_io(&self) -> io::Result<()> {
            match self.deadline.get() {
                Some(deadline) if deadline.is_live() => Ok(()),
                Some(_) => Err(process_lease_expired_io()),
                None => Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "shuffle process lease deadline is not installed",
                )),
            }
        }

        fn require_live_status(&self) -> Result<(), tonic::Status> {
            match self.deadline.get() {
                Some(deadline) if deadline.is_live() => Ok(()),
                Some(_) => Err(process_lease_expired_status()),
                None => Err(tonic::Status::failed_precondition(
                    "shuffle process lease deadline is not installed",
                )),
            }
        }

        async fn wait_until_lost(&self) {
            let Some(deadline) = self.deadline.get() else {
                return;
            };
            if !deadline.is_live() {
                return;
            }
            tokio::select! {
                biased;
                () = self.cancelled.cancelled() => {}
                () = deadline.wait_until_expired() => {}
            }
        }

        fn scope_token(&self, active: bool) -> CancellationToken {
            let token = self.cancelled.child_token();
            if !active || self.require_live_io().is_err() {
                token.cancel();
            }
            token
        }

        #[cfg(test)]
        fn install_live_for_test(&self) {
            self.deadline
                .set(Arc::new(LeaseDeadline::live_for(
                    std::time::Duration::from_secs(60),
                )))
                .expect("test process lease is installed once");
        }
    }

    impl Drop for ProcessLeaseGate {
        fn drop(&mut self) {
            if let Some(watcher) = self.watcher.get_mut().take() {
                watcher.abort();
            }
        }
    }

    fn cancelled_token() -> CancellationToken {
        let token = CancellationToken::new();
        token.cancel();
        token
    }

    fn rotate_scope_token(
        slot: &RwLock<CancellationToken>,
        process_lease: &ProcessLeaseGate,
        active: bool,
    ) {
        let mut token = slot.write();
        token.cancel();
        *token = process_lease.scope_token(active);
    }

    /// A message prepared before its sequence is fixed and inserted into the ordered queue.
    enum PreparedMessage {
        Barrier(CheckpointBarrier),
        Frontier {
            stage: String,
            watermark: Option<i64>,
            idle: bool,
        },
        Data {
            stage: String,
            routed_vnodes: Vec<u32>,
            arrow_ipc: Bytes,
        },
    }

    struct Outbound {
        gen: u64,
        assignment_version: u64,
        /// Data/frontier: this frame's sequence. `Barrier`: ordered frames enqueued so far.
        seq: u64,
        msg: PreparedMessage,
        /// Present only for checkpoint barriers admitted by `fan_out_barrier`.
        assignment_digest: Option<[u8; 32]>,
        _budget: OutboundReservation,
    }

    struct Encoded {
        frames: VecDeque<ShuffleFrame>,
        _budget: OutboundReservation,
    }

    struct OutboundReservation {
        peer: OwnedSemaphorePermit,
        node: OwnedSemaphorePermit,
    }

    impl OutboundReservation {
        fn shrink_to(&mut self, retained_bytes: usize) -> io::Result<()> {
            let peer_bytes = self.peer.num_permits();
            let node_bytes = self.node.num_permits();
            if peer_bytes != node_bytes || retained_bytes > peer_bytes {
                return Err(io::Error::other(
                    "shuffle outbound reservation accounting mismatch",
                ));
            }
            let release = peer_bytes - retained_bytes;
            if release != 0 {
                let peer = self
                    .peer
                    .split(release)
                    .expect("validated outbound peer reservation split");
                let node = self
                    .node
                    .split(release)
                    .expect("validated outbound node reservation split");
                drop((peer, node));
            }
            Ok(())
        }
    }

    struct Inbound {
        peer: ShufflePeerId,
        msg: ShuffleMessage,
        budget: Option<Arc<InboundReservation>>,
        fence: StreamFence,
        assignment_digest: Option<[u8; 32]>,
        checkpoint_sequence: u64,
    }

    impl Inbound {
        fn into_received(self) -> ReceivedShuffle {
            ReceivedShuffle {
                peer: self.peer,
                message: self.msg,
                reservation: self.budget,
                sender_incarnation: self.fence.sender_incarnation,
                receiver_incarnation: self.fence.receiver_incarnation,
                stream_id: self.fence.stream_id,
                assignment_version: self.fence.assignment_version,
                assignment_digest: self.assignment_digest,
                recovery_gen: self.fence.recovery_gen,
                checkpoint_sequence: self.checkpoint_sequence,
            }
        }
    }

    struct InboundBudget {
        node: Arc<Semaphore>,
        peers: Mutex<FxHashMap<ShufflePeerId, Arc<Semaphore>>>,
    }

    impl InboundBudget {
        fn new(node_capacity: usize) -> Self {
            Self {
                node: Arc::new(Semaphore::new(node_capacity)),
                peers: Mutex::new(FxHashMap::default()),
            }
        }

        async fn reserve_frame(
            &self,
            peer: ShufflePeerId,
            wire_bytes: usize,
            cancel: &CancellationToken,
        ) -> Result<InboundReservation, tonic::Status> {
            let bytes = wire_bytes
                .checked_add(crate::shuffle::ROUTE_MAX_BATCH_BYTES)
                .and_then(|bytes| bytes.checked_add(INBOUND_BATCH_METADATA_BYTES))
                // Reassembly owns its full preallocated Vec while the current prost Bytes frame
                // remains live in `run_stream`.
                .and_then(|bytes| bytes.checked_add(MAX_WIRE_PAYLOAD_BYTES))
                .ok_or_else(|| tonic::Status::resource_exhausted("shuffle frame is too large"))?;
            if wire_bytes == 0
                || wire_bytes > crate::shuffle::message::MAX_PAYLOAD_BYTES
                || bytes > INBOUND_PEER_BUDGET_BYTES
            {
                return Err(tonic::Status::resource_exhausted(
                    "shuffle frame exceeds its inbound byte budget",
                ));
            }
            let permits = u32::try_from(bytes)
                .map_err(|_| tonic::Status::resource_exhausted("shuffle frame is too large"))?;
            let peer_budget = {
                let mut peers = self.peers.lock();
                peers.retain(|known_peer, budget| {
                    *known_peer == peer
                        || Arc::strong_count(budget) > 1
                        || budget.available_permits() != INBOUND_PEER_BUDGET_BYTES
                });
                if let Some(budget) = peers.get(&peer) {
                    Arc::clone(budget)
                } else {
                    if peers.len() >= MAX_TRACKED_PEERS {
                        return Err(tonic::Status::resource_exhausted("too many shuffle peers"));
                    }
                    let budget = Arc::new(Semaphore::new(INBOUND_PEER_BUDGET_BYTES));
                    peers.insert(peer, Arc::clone(&budget));
                    budget
                }
            };
            let peer_permit = tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(scope_cancelled_status()),
                permit = peer_budget.acquire_many_owned(permits) => {
                    permit.map_err(|_| tonic::Status::unavailable("shuffle peer budget closed"))?
                }
            };
            let node_permit = tokio::select! {
                biased;
                () = cancel.cancelled() => return Err(scope_cancelled_status()),
                permit = Arc::clone(&self.node).acquire_many_owned(permits) => {
                    permit.map_err(|_| tonic::Status::unavailable("shuffle node budget closed"))?
                }
            };
            Ok(InboundReservation {
                node: node_permit,
                peer: peer_permit,
                wire_bytes,
            })
        }

        fn validate_decoded(batches: &[RecordBatch]) -> Result<usize, tonic::Status> {
            let bytes = batches.iter().try_fold(0usize, |total, batch| {
                let batch_bytes = crate::shuffle::routing::logical_batch_bytes(batch)
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                total.checked_add(batch_bytes).ok_or_else(|| {
                    tonic::Status::resource_exhausted("decoded shuffle payload size overflow")
                })
            })?;
            if bytes > crate::shuffle::ROUTE_MAX_BATCH_BYTES {
                return Err(tonic::Status::resource_exhausted(format!(
                    "decoded shuffle payload is {bytes} bytes; limit is {}",
                    crate::shuffle::ROUTE_MAX_BATCH_BYTES
                )));
            }
            Ok(bytes)
        }
    }

    impl InboundReservation {
        fn retain_decoded(
            &mut self,
            decoded_bytes: usize,
            metadata_bytes: usize,
        ) -> Result<(), tonic::Status> {
            let retained_bytes = self
                .wire_bytes
                .checked_add(decoded_bytes)
                .and_then(|bytes| bytes.checked_add(metadata_bytes))
                .ok_or_else(|| tonic::Status::internal("shuffle inbound accounting overflow"))?;
            let node_bytes = self.node.num_permits();
            let peer_bytes = self.peer.num_permits();
            if node_bytes != peer_bytes || retained_bytes > node_bytes {
                return Err(tonic::Status::internal(
                    "shuffle inbound reservation accounting mismatch",
                ));
            }

            let excess_bytes = node_bytes - retained_bytes;
            if excess_bytes != 0 {
                let node_excess = self
                    .node
                    .split(excess_bytes)
                    .expect("validated node reservation split");
                let peer_excess = self
                    .peer
                    .split(excess_bytes)
                    .expect("validated peer reservation split");
                drop((node_excess, peer_excess));
            }
            Ok(())
        }
    }

    fn decode_ipc_payload(
        decoder: &mut BatchStreamDecoder,
        payload: Vec<u8>,
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let mut batches = decoder.decode_chunk(payload)?;
        decoder.ensure_message_boundary()?;
        if batches.len() != 1 {
            return Err(arrow_schema::ArrowError::IpcError(format!(
                "logical shuffle payload decoded {} record batches; expected exactly one",
                batches.len()
            )));
        }
        Ok(batches.pop().expect("validated one decoded batch"))
    }

    async fn decode_ipc_payload_isolated<F>(
        payload: Vec<u8>,
        budget: InboundReservation,
        before_blocking_decode: F,
    ) -> Result<(RecordBatch, InboundReservation), String>
    where
        F: FnOnce() + Send + 'static,
    {
        if payload.len() >= BLOCKING_IPC_THRESHOLD_BYTES {
            tokio::task::spawn_blocking(move || {
                before_blocking_decode();
                let mut decoder = BatchStreamDecoder::new();
                let batch =
                    decode_ipc_payload(&mut decoder, payload).map_err(|error| error.to_string())?;
                Ok((batch, budget))
            })
            .await
            .map_err(|error| format!("shuffle decoder task: {error}"))?
        } else {
            let mut decoder = BatchStreamDecoder::new();
            let batch =
                decode_ipc_payload(&mut decoder, payload).map_err(|error| error.to_string())?;
            Ok((batch, budget))
        }
    }

    fn schema_memory_size(schema: &arrow_schema::Schema) -> usize {
        let fields = schema
            .fields()
            .iter()
            .fold(0usize, |bytes, field| bytes.saturating_add(field.size()));
        let metadata = schema.metadata().iter().fold(
            schema
                .metadata()
                .capacity()
                .saturating_mul(std::mem::size_of::<(String, String)>()),
            |bytes, (key, value)| {
                bytes
                    .saturating_add(key.capacity())
                    .saturating_add(value.capacity())
            },
        );
        std::mem::size_of_val(schema)
            .saturating_add(
                schema
                    .fields()
                    .len()
                    .saturating_mul(std::mem::size_of::<arrow_schema::FieldRef>()),
            )
            .saturating_add(fields)
            .saturating_add(metadata)
    }

    fn outbound_workspace_bytes(msg: &ShuffleMessage) -> io::Result<usize> {
        match msg {
            ShuffleMessage::Barrier(barrier) => {
                validate_checkpoint_barrier(*barrier)?;
                Ok(1024)
            }
            ShuffleMessage::Frontier {
                stage, watermark, ..
            } => {
                validate_frontier(stage, *watermark)?;
                Ok(FRONTIER_WORKSPACE_BYTES)
            }
            ShuffleMessage::Data {
                stage,
                routed_vnodes,
                batch,
            } => {
                if stage.is_empty() || stage.len() > MAX_STAGE_NAME_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "shuffle stage scope is empty or too long",
                    ));
                }
                let batch_bytes =
                    crate::shuffle::routing::logical_batch_bytes(batch).map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
                    })?;
                if batch_bytes > crate::shuffle::ROUTE_MAX_BATCH_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "shuffle batch is {batch_bytes} bytes; limit is {}",
                            crate::shuffle::ROUTE_MAX_BATCH_BYTES
                        ),
                    ));
                }
                if batch.num_rows() > crate::shuffle::ROUTE_MAX_BATCH_ROWS {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "shuffle batch exceeds the row-count bound",
                    ));
                }
                let schema_bytes = schema_memory_size(batch.schema().as_ref());
                if schema_bytes > MAX_SOURCE_SCHEMA_MEMORY_BYTES {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!(
                            "shuffle schema is {schema_bytes} bytes; limit is {MAX_SOURCE_SCHEMA_MEMORY_BYTES}"
                        ),
                    ));
                }
                let routes_are_canonical = routed_vnodes.windows(2).all(|pair| pair[0] < pair[1]);
                if !routes_are_canonical
                    || routed_vnodes.is_empty()
                    || routed_vnodes.len()
                        > usize::try_from(crate::state::MAX_KEY_GROUP_COUNT).unwrap_or(usize::MAX)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "shuffle route metadata is empty or non-canonical",
                    ));
                }
                Ok(OUTBOUND_DATA_WORKSPACE_BYTES)
            }
        }
    }

    fn encode_outbound_data(
        stage: String,
        routed_vnodes: Vec<u32>,
        batch: RecordBatch,
        logical_bytes: usize,
        schema_bytes: usize,
        mut budget: OutboundReservation,
    ) -> io::Result<(PreparedMessage, OutboundReservation)> {
        let initial_capacity = logical_bytes
            .saturating_add(schema_bytes)
            .saturating_add(FRAME_METADATA_BYTES)
            .min(crate::shuffle::message::MAX_PAYLOAD_BYTES);
        let payload = serialize_batch_stream_bounded(
            &batch,
            crate::shuffle::message::MAX_PAYLOAD_BYTES,
            initial_capacity,
        )
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("shuffle IPC encode: {error}"),
            )
        })?;
        // Encoding is complete; release the shallow batch clone before workspace admission is
        // reduced to the allocations retained by the queued wire message.
        drop(batch);
        let payload_len = payload.len();
        let payload_capacity = payload.capacity();
        if payload_len == 0
            || payload_len > crate::shuffle::message::MAX_PAYLOAD_BYTES
            || payload_capacity > crate::shuffle::message::MAX_PAYLOAD_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "shuffle IPC payload uses {payload_len} bytes ({payload_capacity} allocated); limit is {}",
                    crate::shuffle::message::MAX_PAYLOAD_BYTES
                ),
            ));
        }
        validate_ipc_schema_header(&payload, payload_len)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let route_bytes = routed_vnodes
            .capacity()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| io::Error::other("shuffle route accounting overflow"))?;
        let retained_bytes = payload_capacity
            .checked_add(stage.capacity())
            .and_then(|bytes| bytes.checked_add(route_bytes))
            .and_then(|bytes| bytes.checked_add(FRAME_METADATA_BYTES))
            .ok_or_else(|| io::Error::other("shuffle outbound accounting overflow"))?;
        budget.shrink_to(retained_bytes)?;
        Ok((
            PreparedMessage::Data {
                stage,
                routed_vnodes,
                arrow_ipc: Bytes::from(payload),
            },
            budget,
        ))
    }

    async fn prepare_outbound_message_with_hook<F>(
        msg: &ShuffleMessage,
        budget: OutboundReservation,
        before_blocking_encode: F,
    ) -> io::Result<(PreparedMessage, OutboundReservation)>
    where
        F: FnOnce() + Send + 'static,
    {
        match msg {
            ShuffleMessage::Barrier(barrier) => Ok((PreparedMessage::Barrier(*barrier), budget)),
            ShuffleMessage::Frontier {
                stage,
                watermark,
                idle,
            } => Ok((
                PreparedMessage::Frontier {
                    stage: stage.clone(),
                    watermark: *watermark,
                    idle: *idle,
                },
                budget,
            )),
            ShuffleMessage::Data {
                stage,
                routed_vnodes,
                batch,
            } => {
                let stage = stage.clone();
                let routed_vnodes = routed_vnodes.to_vec();
                let batch = batch.clone();
                let logical_bytes =
                    crate::shuffle::routing::logical_batch_bytes(&batch).map_err(|error| {
                        io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
                    })?;
                let schema_bytes = schema_memory_size(batch.schema().as_ref());
                let offload = logical_bytes >= BLOCKING_IPC_THRESHOLD_BYTES;
                if offload {
                    tokio::task::spawn_blocking(move || {
                        before_blocking_encode();
                        encode_outbound_data(
                            stage,
                            routed_vnodes,
                            batch,
                            logical_bytes,
                            schema_bytes,
                            budget,
                        )
                    })
                    .await
                    .map_err(|error| io::Error::other(format!("shuffle encoder task: {error}")))?
                } else {
                    encode_outbound_data(
                        stage,
                        routed_vnodes,
                        batch,
                        logical_bytes,
                        schema_bytes,
                        budget,
                    )
                }
            }
        }
    }

    async fn prepare_outbound_message(
        msg: &ShuffleMessage,
        budget: OutboundReservation,
    ) -> io::Result<(PreparedMessage, OutboundReservation)> {
        prepare_outbound_message_with_hook(msg, budget, || {}).await
    }

    /// Full identity of one ordered connection. Sequence continuity is scoped to both process
    /// incarnations and the assignment; `stream_id` distinguishes reconnects inside that scope.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct StreamFence {
        sender_node_id: ShufflePeerId,
        sender_incarnation: Uuid,
        receiver_incarnation: Uuid,
        stream_id: Uuid,
        assignment_version: u64,
        assignment_certificate_digest: [u8; 32],
        recovery_gen: u64,
    }

    /// Locally admitted assignment authority. The digest binds both the ordered owner map and
    /// every participant's exact process incarnation; a scalar version alone is not authority.
    #[derive(Debug)]
    struct InstalledAssignment {
        fence: CheckpointAssignmentFence,
        digest: [u8; 32],
        owners: Arc<[ShufflePeerId]>,
    }

    #[derive(Clone)]
    struct ScopeLease {
        assignment: Arc<InstalledAssignment>,
        recovery_gen: u64,
        cancel: CancellationToken,
    }

    impl ScopeLease {
        fn matches_fence(&self, fence: &StreamFence) -> bool {
            !self.cancel.is_cancelled()
                && self.assignment.fence.assignment_version == fence.assignment_version
                && self.assignment.digest == fence.assignment_certificate_digest
                && self.recovery_gen == fence.recovery_gen
        }
    }

    impl InstalledAssignment {
        fn for_process(
            fence: &CheckpointAssignmentFence,
            owners: &[ShufflePeerId],
            local_id: ShufflePeerId,
            local_incarnation: Uuid,
        ) -> io::Result<Arc<Self>> {
            if !fence.is_canonical()
                || !fence.matches_owner_map(owners)
                || owners.iter().any(|owner| !fence.contains(*owner))
                || fence.participant_incarnation(local_id) != Some(local_incarnation)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle assignment scope does not bind this process and exact owner map",
                ));
            }
            Ok(Arc::new(Self {
                digest: fence.digest(),
                fence: fence.clone(),
                owners: Arc::from(owners),
            }))
        }

        fn certifies(&self, node_id: ShufflePeerId, incarnation: Uuid) -> bool {
            self.fence.participant_incarnation(node_id) == Some(incarnation)
        }

        fn matches_stream_sender(&self, fence: &StreamFence) -> bool {
            self.fence.assignment_version == fence.assignment_version
                && self.digest == fence.assignment_certificate_digest
                && self.certifies(fence.sender_node_id, fence.sender_incarnation)
        }

        fn owns_vnode(&self, node_id: ShufflePeerId, vnode: u32) -> bool {
            usize::try_from(vnode)
                .ok()
                .and_then(|vnode| self.owners.get(vnode))
                .is_some_and(|owner| *owner == node_id)
        }
    }

    struct PendingHandshake {
        fence: StreamFence,
        issued_at: std::time::Instant,
    }

    #[derive(Default)]
    struct PendingHandshakes(Mutex<FxHashMap<ShufflePeerId, PendingHandshake>>);

    impl PendingHandshakes {
        fn clear(&self) {
            self.0.lock().clear();
        }
    }

    struct ActiveStreamEntry {
        owner: Arc<()>,
        cancel: CancellationToken,
    }

    #[derive(Default)]
    struct ActiveStreamRegistry {
        streams: Mutex<FxHashMap<ShufflePeerId, ActiveStreamEntry>>,
    }

    impl ActiveStreamRegistry {
        fn replace(
            self: &Arc<Self>,
            fence: &StreamFence,
            parent_cancel: &CancellationToken,
        ) -> ActiveStreamLease {
            let key = fence.sender_node_id;
            let cancel = parent_cancel.child_token();
            let owner = Arc::new(());
            let previous = self.streams.lock().insert(
                key,
                ActiveStreamEntry {
                    owner: Arc::clone(&owner),
                    cancel: cancel.clone(),
                },
            );
            if let Some(previous) = previous {
                previous.cancel.cancel();
            }
            ActiveStreamLease {
                registry: Arc::clone(self),
                key,
                owner,
                cancel,
                permit: None,
            }
        }
    }

    struct ActiveStreamLease {
        registry: Arc<ActiveStreamRegistry>,
        key: ShufflePeerId,
        owner: Arc<()>,
        cancel: CancellationToken,
        permit: Option<OwnedSemaphorePermit>,
    }

    impl ActiveStreamLease {
        async fn acquire_permit(&mut self, permits: &Arc<Semaphore>) -> Result<(), tonic::Status> {
            let permit = tokio::select! {
                biased;
                () = self.cancel.cancelled() => return Err(tonic::Status::cancelled(
                    "shuffle stream was superseded",
                )),
                permit = Arc::clone(permits).acquire_owned() => permit.map_err(|_| {
                    tonic::Status::unavailable("shuffle stream admission closed")
                })?,
            };
            self.permit = Some(permit);
            Ok(())
        }
    }

    impl Drop for ActiveStreamLease {
        fn drop(&mut self) {
            self.cancel.cancel();
            self.permit.take();
            let mut streams = self.registry.streams.lock();
            if streams
                .get(&self.key)
                .is_some_and(|entry| Arc::ptr_eq(&entry.owner, &self.owner))
            {
                streams.remove(&self.key);
            }
        }
    }

    struct FragmentAssembly {
        stage: String,
        routed_vnodes: Vec<u32>,
        recovery_gen: u64,
        seq: u64,
        fragment_count: u32,
        total_payload_bytes: usize,
        next_fragment: u32,
        payload: Vec<u8>,
        budget: InboundReservation,
    }

    struct CompleteData {
        stage: String,
        routed_vnodes: Vec<u32>,
        seq: u64,
        arrow_ipc: Vec<u8>,
        budget: InboundReservation,
    }

    fn validate_ipc_schema_header(
        payload: &[u8],
        total_payload_bytes: usize,
    ) -> Result<(), String> {
        const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

        if payload.len() < 8 || payload[..4] != CONTINUATION_MARKER {
            return Err("shuffle IPC must start with a modern Arrow schema message".into());
        }
        let metadata_len = usize::try_from(u32::from_le_bytes(
            payload[4..8]
                .try_into()
                .expect("validated Arrow IPC prefix length"),
        ))
        .map_err(|_| "shuffle IPC schema length is invalid".to_string())?;
        if metadata_len == 0 || metadata_len > MAX_SCHEMA_WIRE_BYTES {
            return Err("shuffle IPC schema message exceeds its wire bound".into());
        }
        let end = 8usize
            .checked_add(metadata_len)
            .ok_or_else(|| "shuffle IPC schema length overflow".to_string())?;
        if end > payload.len() || end > total_payload_bytes {
            return Err("shuffle IPC schema message is not complete in fragment zero".into());
        }
        let message = arrow_ipc::root_as_message(&payload[8..end])
            .map_err(|error| format!("invalid shuffle IPC schema message: {error}"))?;
        if message.header_type() != arrow_ipc::MessageHeader::Schema
            || message.header_as_schema().is_none()
            || message.bodyLength() != 0
        {
            return Err("shuffle IPC leading message is not a bodyless schema".into());
        }
        Ok(())
    }

    fn validate_fragment(fragment: &RoutedData) -> Result<usize, String> {
        let fragment_count = usize::try_from(fragment.fragment_count)
            .map_err(|_| "invalid shuffle fragment count".to_string())?;
        let total_payload_bytes = usize::try_from(fragment.total_payload_bytes)
            .map_err(|_| "invalid shuffle payload size".to_string())?;
        if fragment_count == 0
            || fragment_count > MAX_FRAGMENTS
            || total_payload_bytes == 0
            || total_payload_bytes > crate::shuffle::message::MAX_PAYLOAD_BYTES
            || fragment.arrow_ipc.is_empty()
            || fragment.arrow_ipc.len() > MAX_WIRE_PAYLOAD_BYTES
            || fragment_count != total_payload_bytes.div_ceil(MAX_WIRE_PAYLOAD_BYTES)
            || usize::try_from(fragment.fragment_index).unwrap_or(usize::MAX) >= fragment_count
        {
            return Err("invalid shuffle fragment bounds".into());
        }
        let index = usize::try_from(fragment.fragment_index)
            .map_err(|_| "invalid shuffle fragment index".to_string())?;
        let expected_len = if index + 1 == fragment_count {
            total_payload_bytes - index * MAX_WIRE_PAYLOAD_BYTES
        } else {
            MAX_WIRE_PAYLOAD_BYTES
        };
        if fragment.arrow_ipc.len() != expected_len {
            return Err("shuffle fragment length does not match its declared payload".into());
        }
        if index == 0 {
            validate_ipc_schema_header(&fragment.arrow_ipc, total_payload_bytes)?;
            let routes_are_canonical = fragment
                .routed_vnodes
                .windows(2)
                .all(|pair| pair[0] < pair[1]);
            if fragment.stage.is_empty()
                || fragment.stage.len() > MAX_STAGE_NAME_BYTES
                || !routes_are_canonical
                || fragment.routed_vnodes.len()
                    > usize::try_from(crate::state::MAX_KEY_GROUP_COUNT).unwrap_or(usize::MAX)
                || fragment.routed_vnodes.is_empty()
            {
                return Err("shuffle fragment-zero metadata is empty or non-canonical".into());
            }
        } else if !fragment.stage.is_empty() || !fragment.routed_vnodes.is_empty() {
            return Err("shuffle continuation fragment repeated logical metadata".into());
        }
        Ok(total_payload_bytes)
    }

    fn retained_batch_metadata_bytes(
        stage: &String,
        routed_vnodes: &[u32],
        batch: &RecordBatch,
    ) -> Result<usize, tonic::Status> {
        let schema_bytes = schema_memory_size(batch.schema().as_ref());
        if schema_bytes > MAX_DECODED_SCHEMA_MEMORY_BYTES {
            return Err(tonic::Status::resource_exhausted(
                "decoded shuffle schema exceeds its memory bound",
            ));
        }
        let structure_bytes = batch.columns().iter().try_fold(
            std::mem::size_of::<RecordBatch>()
                + batch
                    .num_columns()
                    .saturating_mul(std::mem::size_of::<arrow_array::ArrayRef>()),
            |total, column| {
                let array_bytes = column
                    .get_array_memory_size()
                    .saturating_sub(column.get_buffer_memory_size());
                total.checked_add(array_bytes).ok_or_else(|| {
                    tonic::Status::internal("shuffle array structure accounting overflow")
                })
            },
        )?;
        if structure_bytes > MAX_DECODED_ARRAY_STRUCTURE_BYTES {
            return Err(tonic::Status::resource_exhausted(
                "decoded shuffle array structure exceeds its memory bound",
            ));
        }
        let route_bytes = routed_vnodes
            .len()
            .checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| tonic::Status::internal("shuffle route accounting overflow"))?;
        let metadata_bytes = schema_bytes
            .checked_add(structure_bytes)
            .and_then(|bytes| bytes.checked_add(stage.capacity()))
            .and_then(|bytes| bytes.checked_add(route_bytes))
            .and_then(|bytes| bytes.checked_add(RETAINED_BATCH_ENVELOPE_BYTES))
            .ok_or_else(|| tonic::Status::internal("shuffle metadata accounting overflow"))?;
        if metadata_bytes > INBOUND_BATCH_METADATA_BYTES {
            return Err(tonic::Status::resource_exhausted(
                "decoded shuffle metadata exceeds its admission bound",
            ));
        }
        Ok(metadata_bytes)
    }

    fn push_fragment(
        assembly: &mut Option<FragmentAssembly>,
        fragment: &RoutedData,
        budget: Option<InboundReservation>,
    ) -> Result<Option<CompleteData>, String> {
        let total_payload_bytes = validate_fragment(fragment)?;

        if fragment.fragment_index == 0 {
            if assembly.is_some() {
                return Err("shuffle fragments interleaved across logical frames".into());
            }
            let budget = budget.ok_or_else(|| {
                "shuffle fragment zero was not admitted by byte budget".to_string()
            })?;
            *assembly = Some(FragmentAssembly {
                stage: fragment.stage.clone(),
                routed_vnodes: fragment.routed_vnodes.clone(),
                recovery_gen: fragment.recovery_gen,
                seq: fragment.seq,
                fragment_count: fragment.fragment_count,
                total_payload_bytes,
                next_fragment: 0,
                payload: Vec::with_capacity(total_payload_bytes),
                budget,
            });
        } else if budget.is_some() {
            return Err("shuffle continuation fragment carried a new byte reservation".into());
        }
        let current = assembly
            .as_mut()
            .ok_or_else(|| "shuffle fragment arrived without fragment zero".to_string())?;
        let first_metadata_changed = fragment.fragment_index == 0
            && (current.stage != fragment.stage || current.routed_vnodes != fragment.routed_vnodes);
        if first_metadata_changed
            || current.recovery_gen != fragment.recovery_gen
            || current.seq != fragment.seq
            || current.fragment_count != fragment.fragment_count
            || current.total_payload_bytes != total_payload_bytes
            || current.next_fragment != fragment.fragment_index
        {
            return Err("shuffle fragment metadata or order changed mid-frame".into());
        }
        current.payload.extend_from_slice(&fragment.arrow_ipc);
        current.next_fragment += 1;
        if current.next_fragment != current.fragment_count {
            return Ok(None);
        }
        let complete = assembly.take().expect("completed fragment assembly exists");
        if complete.payload.len() != complete.total_payload_bytes {
            return Err("reassembled shuffle payload length mismatch".into());
        }
        Ok(Some(CompleteData {
            stage: complete.stage,
            routed_vnodes: complete.routed_vnodes,
            seq: complete.seq,
            arrow_ipc: complete.payload,
            budget: complete.budget,
        }))
    }

    fn parse_uuid(raw: &[u8], field: &str) -> Result<Uuid, tonic::Status> {
        let value = Uuid::from_slice(raw)
            .map_err(|_| tonic::Status::invalid_argument(format!("invalid {field} UUID")))?;
        if value.is_nil() {
            return Err(tonic::Status::invalid_argument(format!("nil {field} UUID")));
        }
        Ok(value)
    }

    fn parse_certificate_digest(raw: &[u8], field: &str) -> Result<[u8; 32], tonic::Status> {
        let digest: [u8; 32] = raw
            .try_into()
            .map_err(|_| tonic::Status::invalid_argument(format!("invalid {field}")))?;
        if digest == [0; 32] {
            return Err(tonic::Status::invalid_argument(format!("zero {field}")));
        }
        Ok(digest)
    }

    fn hello_for(node_id: ShufflePeerId, fence: &StreamFence) -> Hello {
        Hello {
            node_id,
            sender_incarnation: fence.sender_incarnation.as_bytes().to_vec(),
            receiver_incarnation: fence.receiver_incarnation.as_bytes().to_vec(),
            stream_id: fence.stream_id.as_bytes().to_vec(),
            assignment_version: fence.assignment_version,
            recovery_gen: fence.recovery_gen,
            assignment_certificate_digest: fence.assignment_certificate_digest.to_vec(),
        }
    }

    fn fence_from_hello(hello: &Hello) -> Result<StreamFence, tonic::Status> {
        if hello.node_id == 0 || hello.assignment_version == 0 {
            return Err(tonic::Status::failed_precondition(
                "shuffle stream requires assigned nodes and a nonzero assignment version",
            ));
        }
        Ok(StreamFence {
            sender_node_id: hello.node_id,
            sender_incarnation: parse_uuid(&hello.sender_incarnation, "sender incarnation")?,
            receiver_incarnation: parse_uuid(&hello.receiver_incarnation, "receiver incarnation")?,
            stream_id: parse_uuid(&hello.stream_id, "stream id")?,
            assignment_version: hello.assignment_version,
            assignment_certificate_digest: parse_certificate_digest(
                &hello.assignment_certificate_digest,
                "assignment certificate digest",
            )?,
            recovery_gen: hello.recovery_gen,
        })
    }

    /// Frame one admitted, self-contained logical message without copying its IPC allocation.
    fn frame_message(out: Outbound) -> Result<Encoded, tonic::Status> {
        let Outbound {
            gen,
            assignment_version,
            seq,
            msg,
            assignment_digest,
            _budget: budget,
        } = out;
        let frames = match msg {
            PreparedMessage::Barrier(b) => {
                validate_checkpoint_barrier(b)
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                let assignment_digest = assignment_digest.ok_or_else(|| {
                    tonic::Status::failed_precondition(
                        "shuffle checkpoint barrier has no assignment certificate",
                    )
                })?;
                VecDeque::from([ShuffleFrame {
                    kind: Some(shuffle_frame::Kind::Barrier(Barrier {
                        checkpoint_id: b.checkpoint_id,
                        epoch: b.epoch,
                        flags: b.flags,
                        last_seq: seq,
                        assignment_version,
                        assignment_digest: assignment_digest.to_vec(),
                        recovery_gen: gen,
                    })),
                }])
            }
            PreparedMessage::Data {
                mut stage,
                routed_vnodes,
                arrow_ipc,
            } => {
                let total = arrow_ipc.len();
                if total == 0 || total > crate::shuffle::message::MAX_PAYLOAD_BYTES {
                    return Err(tonic::Status::resource_exhausted(format!(
                        "shuffle IPC payload is {total} bytes; limit is {}",
                        crate::shuffle::message::MAX_PAYLOAD_BYTES
                    )));
                }
                let fragment_count = total.div_ceil(MAX_WIRE_PAYLOAD_BYTES);
                if fragment_count == 0 || fragment_count > MAX_FRAGMENTS {
                    return Err(tonic::Status::resource_exhausted(
                        "shuffle IPC payload needs too many fragments",
                    ));
                }
                let fragment_count = u32::try_from(fragment_count)
                    .map_err(|_| tonic::Status::resource_exhausted("too many fragments"))?;
                let total_payload_bytes = u32::try_from(total)
                    .map_err(|_| tonic::Status::resource_exhausted("payload is too large"))?;
                let mut routes = routed_vnodes;
                let mut frames = VecDeque::with_capacity(fragment_count as usize);
                for index in 0..fragment_count {
                    let index = index as usize;
                    let start = index * MAX_WIRE_PAYLOAD_BYTES;
                    let end = (start + MAX_WIRE_PAYLOAD_BYTES).min(total);
                    let first = index == 0;
                    frames.push_back(ShuffleFrame {
                        kind: Some(shuffle_frame::Kind::Data(RoutedData {
                            stage: if first {
                                std::mem::take(&mut stage)
                            } else {
                                String::new()
                            },
                            routed_vnodes: if first {
                                std::mem::take(&mut routes)
                            } else {
                                Vec::new()
                            },
                            arrow_ipc: arrow_ipc.slice(start..end),
                            recovery_gen: gen,
                            seq,
                            fragment_index: u32::try_from(index)
                                .expect("fragment count is bounded by MAX_FRAGMENTS"),
                            fragment_count,
                            total_payload_bytes,
                        })),
                    });
                }
                frames
            }
            PreparedMessage::Frontier {
                stage,
                watermark,
                idle,
            } => {
                validate_frontier(&stage, watermark)
                    .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
                VecDeque::from([ShuffleFrame {
                    kind: Some(shuffle_frame::Kind::Frontier(WireFrontier {
                        stage,
                        watermark,
                        idle,
                        recovery_gen: gen,
                        seq,
                    })),
                }])
            }
        };
        Ok(Encoded {
            frames,
            _budget: budget,
        })
    }

    /// One lazily-opened client-streaming call to a peer. The driver task
    /// frames queued messages and feeds the gRPC request stream, flipping
    /// `alive=false` on the first transport/connect error so the next `send_to`
    /// reconnects.
    struct PeerConn {
        tx: MAsyncTx<mpsc::Array<Outbound>>,
        byte_budget: Arc<Semaphore>,
        control_byte_budget: Arc<Semaphore>,
        /// Sequence allocation and queue insertion must be one ordered operation. Without this,
        /// concurrent callers can enqueue sequence 1 before sequence 0.
        send_lock: tokio::sync::Mutex<()>,
        alive: Arc<AtomicBool>,
        driver: JoinHandle<()>,
        fence: StreamFence,
    }

    impl PeerConn {
        fn is_alive(&self) -> bool {
            // `is_finished()` also catches a driver cancelled without flipping
            // `alive` (an in-process restart drops its runtime), which would
            // otherwise zombie: fails every send yet never reconnects.
            self.alive.load(Ordering::Acquire) && !self.driver.is_finished()
        }
    }

    impl Drop for PeerConn {
        fn drop(&mut self) {
            self.driver.abort();
        }
    }

    struct OpenCall {
        local_id: ShufflePeerId,
        peer: ShufflePeerId,
        addr: SocketAddr,
        sender_incarnation: Uuid,
        assignment_version: u64,
        assignment_certificate_digest: [u8; 32],
        expected_receiver_incarnation: Uuid,
        recovery_gen: u64,
        current_assignment: Arc<AtomicU64>,
        current_recovery_gen: Arc<AtomicU64>,
        scope_cancel: CancellationToken,
    }

    type ConnectLock = Arc<tokio::sync::Mutex<()>>;

    /// Lazy pool of ordered streams per peer.
    pub struct ShuffleSender {
        local_id: ShufflePeerId,
        sender_incarnation: Uuid,
        peers: Mutex<FxHashMap<ShufflePeerId, SocketAddr>>,
        pool: Mutex<FxHashMap<ShufflePeerId, Arc<PeerConn>>>,
        /// Serialises reconnects per peer without head-of-line blocking unrelated peers.
        connect_locks: Mutex<FxHashMap<ShufflePeerId, ConnectLock>>,
        kv: Option<Arc<dyn ClusterKv>>,
        assignment: Arc<RwLock<Option<Arc<InstalledAssignment>>>>,
        assignment_version: Arc<AtomicU64>,
        scope_cancel: Arc<RwLock<CancellationToken>>,
        process_lease: Arc<ProcessLeaseGate>,
        /// True only for a transient durable-authority outage. The exact retained certificate
        /// may be reactivated without resetting its sequence domain.
        assignment_suspended: AtomicBool,
        /// Stamped onto every outbound data message; bumped by a coordinated rewind.
        recovery_gen: Arc<AtomicU64>,
        /// Ordered data/frontier frames enqueued per peer. Lives here, not on `PeerConn`, so it
        /// survives a reconnect that discards a queue and leaves a detectable gap.
        seqs: Mutex<FxHashMap<ShufflePeerId, u64>>,
        checkpointed_control_node_budget: Arc<Semaphore>,
        node_budget: Arc<Semaphore>,
    }

    impl std::fmt::Debug for ShuffleSender {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ShuffleSender")
                .field("local_id", &self.local_id)
                .finish_non_exhaustive()
        }
    }

    impl ShuffleSender {
        /// Empty sender; peers arrive via [`Self::register_peer`] or KV discovery.
        ///
        /// # Panics
        ///
        /// Panics when the node ID is zero or the process incarnation is nil.
        #[must_use]
        pub fn new(local_id: ShufflePeerId, incarnation: Uuid) -> Self {
            assert!(local_id != 0, "shuffle sender node id must be nonzero");
            assert!(
                !incarnation.is_nil(),
                "shuffle sender incarnation must be non-nil"
            );
            Self {
                local_id,
                sender_incarnation: incarnation,
                peers: Mutex::new(FxHashMap::default()),
                pool: Mutex::new(FxHashMap::default()),
                connect_locks: Mutex::new(FxHashMap::default()),
                kv: None,
                assignment: Arc::new(RwLock::new(None)),
                assignment_version: Arc::new(AtomicU64::new(0)),
                scope_cancel: Arc::new(RwLock::new(cancelled_token())),
                process_lease: Arc::new(ProcessLeaseGate::default()),
                assignment_suspended: AtomicBool::new(false),
                recovery_gen: Arc::new(AtomicU64::new(0)),
                seqs: Mutex::new(FxHashMap::default()),
                checkpointed_control_node_budget: Arc::new(Semaphore::new(
                    CHECKPOINTED_CONTROL_NODE_BUDGET_BYTES,
                )),
                node_budget: Arc::new(Semaphore::new(OUTBOUND_NODE_BUDGET_BYTES)),
            }
        }

        /// Node id bound into every outbound stream handshake.
        #[must_use]
        pub const fn local_id(&self) -> ShufflePeerId {
            self.local_id
        }

        /// Bind outbound admission to this process's renewable cluster lease.
        ///
        /// This must be installed before an assignment certificate is activated.
        ///
        /// # Errors
        /// Returns an error for an expired lease, a different previously installed deadline, or
        /// an already-active assignment.
        pub fn install_process_lease_deadline(
            &self,
            deadline: Arc<LeaseDeadline>,
        ) -> io::Result<()> {
            let _assignment = self.assignment.write();
            if self.assignment_version.load(Ordering::Acquire) != 0
                && !self.process_lease.is_installed_deadline(&deadline)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle process lease must be installed before assignment activation",
                ));
            }
            self.process_lease.install(deadline)
        }

        /// Bind both directions of one local shuffle fabric to the same process lease.
        ///
        /// Validation and installation are serialized across both handles, so an incompatible
        /// receiver cannot leave only the sender bound.
        ///
        /// # Errors
        /// Returns an error without changing either handle when the deadline is expired, either
        /// handle is already bound to a different deadline, or an active assignment prevents a
        /// new binding.
        pub fn bind_process_lease_deadline_pair(
            &self,
            receiver: &ShuffleReceiver,
            deadline: Arc<LeaseDeadline>,
        ) -> io::Result<()> {
            let _sender_assignment = self.assignment.write();
            let _receiver_assignment = receiver.assignment.write();
            if self.assignment_version.load(Ordering::Acquire) != 0
                && !self.process_lease.is_installed_deadline(&deadline)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle process lease must be installed before outbound assignment activation",
                ));
            }
            if receiver.assignment_version.load(Ordering::Acquire) != 0
                && !receiver.process_lease.is_installed_deadline(&deadline)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle process lease must be installed before inbound assignment activation",
                ));
            }
            ProcessLeaseGate::install_pair(
                self.process_lease.as_ref(),
                receiver.process_lease.as_ref(),
                deadline,
            )
        }

        #[cfg(test)]
        pub(crate) fn install_live_process_lease_for_test(&self) {
            self.process_lease.install_live_for_test();
        }

        /// Install the exact assignment certificate accepted by outbound shuffle streams.
        /// Changing scope closes all pooled streams and resets their scoped sequences.
        ///
        /// # Errors
        /// Returns an error for a malformed certificate, a same-version certificate conflict,
        /// or a certificate that does not bind this exact sender process.
        pub fn install_assignment_fence(
            &self,
            fence: &CheckpointAssignmentFence,
            owners: &[ShufflePeerId],
        ) -> io::Result<bool> {
            let next = InstalledAssignment::for_process(
                fence,
                owners,
                self.local_id,
                self.sender_incarnation,
            )?;
            // Publish and reset under one scope lock and the same lock order used by connection
            // installation / sequence allocation. A new-scope frame cannot reuse sequence zero.
            let mut assignment = self.assignment.write();
            self.process_lease.require_live_io()?;
            if let Some(current) = assignment.as_ref() {
                if next.fence.assignment_version < current.fence.assignment_version {
                    return Ok(false);
                }
                if next.fence.assignment_version == current.fence.assignment_version {
                    if next.digest == current.digest
                        && next.fence == current.fence
                        && next.owners == current.owners
                    {
                        if self.assignment_version.load(Ordering::Acquire)
                            == next.fence.assignment_version
                        {
                            return Ok(false);
                        }
                        if self.assignment_suspended.load(Ordering::Acquire) {
                            rotate_scope_token(&self.scope_cancel, &self.process_lease, true);
                            self.pool.lock().clear();
                            self.connect_locks.lock().clear();
                            self.assignment_suspended.store(false, Ordering::Release);
                            self.assignment_version
                                .store(next.fence.assignment_version, Ordering::Release);
                            return Ok(true);
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "an invalidated shuffle assignment requires a higher version",
                        ));
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "conflicting shuffle assignment certificate for an installed version",
                    ));
                }
            }
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            let mut pool = self.pool.lock();
            let mut seqs = self.seqs.lock();
            pool.clear();
            seqs.clear();
            self.connect_locks.lock().clear();
            self.peers
                .lock()
                .retain(|peer, _| next.fence.contains(*peer));
            let version = next.fence.assignment_version;
            *assignment = Some(next);
            self.assignment_suspended.store(false, Ordering::Release);
            rotate_scope_token(&self.scope_cancel, &self.process_lease, true);
            self.assignment_version.store(version, Ordering::Release);
            Ok(true)
        }

        /// Temporarily close outbound admission because the durable assignment authority could
        /// not be read. Unlike invalidation, this preserves the exact certificate and sequence
        /// counters so the same durable head can resume without manufacturing a delivery gap.
        pub fn suspend_assignment_fence(&self) {
            let assignment = self.assignment.write();
            if assignment.is_none() || self.assignment_version.load(Ordering::Acquire) == 0 {
                return;
            }
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            self.pool.lock().clear();
            self.connect_locks.lock().clear();
            self.assignment_suspended.store(true, Ordering::Release);
            self.assignment_version.store(0, Ordering::Release);
        }

        /// Reject every stream while a newer assignment is being adopted but has not yet earned
        /// an owner-complete process certificate. The last certificate is retained only as a
        /// monotonic conflict floor; it is inactive while [`Self::assignment_version`] is zero.
        pub fn invalidate_assignment_fence(&self) {
            let _assignment = self.assignment.write();
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            let mut pool = self.pool.lock();
            let mut seqs = self.seqs.lock();
            self.assignment_suspended.store(false, Ordering::Release);
            self.assignment_version.store(0, Ordering::Release);
            pool.clear();
            seqs.clear();
            self.connect_locks.lock().clear();
        }

        /// Assignment scope currently accepted for newly enqueued frames.
        #[must_use]
        pub fn assignment_version(&self) -> u64 {
            self.assignment_version.load(Ordering::Acquire)
        }

        /// Digest of the exact assignment certificate currently active for outbound streams.
        #[must_use]
        pub fn active_assignment_digest(&self) -> Option<[u8; 32]> {
            let assignment = self.assignment.read();
            assignment.as_ref().and_then(|installed| {
                (self.assignment_version.load(Ordering::Acquire)
                    == installed.fence.assignment_version)
                    .then_some(installed.digest)
            })
        }

        /// Advance the generation stamped onto outbound data frames. Called after a coordinated
        /// rewind so peers can discard anything this node produced before it.
        pub fn set_recovery_gen(&self, gen: u64) {
            // Recovery starts a new sequence domain. Publish, disconnect, and reset counters
            // under the same lock order used by connection installation.
            let _assignment = self.assignment.write();
            let previous = self.recovery_gen.load(Ordering::Acquire);
            if gen <= previous {
                return;
            }
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            let mut pool = self.pool.lock();
            let mut seqs = self.seqs.lock();
            self.recovery_gen.store(gen, Ordering::Release);
            pool.clear();
            seqs.clear();
            self.connect_locks.lock().clear();
            rotate_scope_token(
                &self.scope_cancel,
                &self.process_lease,
                self.assignment_version.load(Ordering::Acquire) != 0,
            );
        }

        /// Recovery scope currently accepted for newly enqueued frames.
        #[must_use]
        pub fn recovery_gen(&self) -> u64 {
            self.recovery_gen.load(Ordering::Acquire)
        }

        /// Process incarnation bound into every outbound stream handshake.
        #[must_use]
        pub const fn incarnation(&self) -> Uuid {
            self.sender_incarnation
        }

        /// Consume a sequence without sending, modelling a frame the transport discarded.
        #[cfg(test)]
        pub fn burn_seq_for_test(&self, peer: ShufflePeerId) {
            *self.seqs.lock().entry(peer).or_insert(0) += 1;
        }

        #[cfg(test)]
        pub(crate) fn tracked_resources_for_test(&self) -> (usize, usize, usize, usize) {
            (
                self.peers.lock().len(),
                self.pool.lock().len(),
                self.connect_locks.lock().len(),
                self.seqs.lock().len(),
            )
        }

        /// Sender that falls back to `kv` discovery for peers not previously
        /// registered.
        #[must_use]
        pub fn with_kv(local_id: ShufflePeerId, kv: Arc<dyn ClusterKv>, incarnation: Uuid) -> Self {
            let mut s = Self::new(local_id, incarnation);
            s.kv = Some(kv);
            s
        }

        /// Register (or update) a peer's shuffle address.
        pub fn register_peer(&self, peer: ShufflePeerId, addr: SocketAddr) {
            if peer == 0 || peer == self.local_id {
                return;
            }
            let mut peers = self.peers.lock();
            if !peers.contains_key(&peer) && peers.len() >= MAX_TRACKED_PEERS {
                tracing::warn!(peer, "shuffle peer address registry is full");
                return;
            }
            peers.insert(peer, addr);
        }

        /// Send `msg` to `peer`, opening a client-streaming call if necessary.
        ///
        /// # Errors
        /// Returns `io::Error` when the peer is unregistered/undiscoverable, the
        /// endpoint cannot be built, or the per-peer stream has shut down.
        pub async fn send_to(&self, peer: ShufflePeerId, msg: &ShuffleMessage) -> io::Result<()> {
            self.send_to_inner(peer, msg, None, None).await
        }

        /// Send only while the sender remains in `expected_assignment_version`.
        ///
        /// Routing paths use this to prevent data sliced with an old ownership publication from
        /// entering a stream opened under a newer assignment scope.
        ///
        /// # Errors
        /// Returns `io::Error` when the expected assignment is no longer current, in addition to
        /// the errors returned by [`Self::send_to`].
        pub async fn send_to_for_assignment(
            &self,
            peer: ShufflePeerId,
            expected_assignment_version: u64,
            msg: &ShuffleMessage,
        ) -> io::Result<()> {
            self.send_to_inner(peer, msg, Some(expected_assignment_version), None)
                .await
        }

        async fn send_to_inner(
            &self,
            peer: ShufflePeerId,
            msg: &ShuffleMessage,
            expected_assignment_version: Option<u64>,
            assignment_fence: Option<&CheckpointAssignmentFence>,
        ) -> io::Result<()> {
            if peer == 0 || peer == self.local_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle peer must be a different assigned node",
                ));
            }
            let admission_bytes = outbound_workspace_bytes(msg)?;
            if matches!(msg, ShuffleMessage::Barrier(_)) && assignment_fence.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle checkpoint barriers require an admitted assignment certificate",
                ));
            }
            let scope = self.current_scope(expected_assignment_version)?;
            let conn = self.connection_for(peer, &scope).await?;
            let _send_guard = tokio::select! {
                biased;
                () = scope.cancel.cancelled() => return Err(scope_cancelled_io()),
                guard = conn.send_lock.lock() => guard,
            };
            self.validate_scope(&scope, expected_assignment_version)?;
            let assignment = &scope.assignment;
            if !assignment.matches_stream_sender(&conn.fence)
                || !assignment.certifies(peer, conn.fence.receiver_incarnation)
            {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "shuffle stream no longer matches the installed assignment certificate",
                ));
            }
            if let Some(fence) = assignment_fence {
                if *fence != assignment.fence
                    || fence.digest() != conn.fence.assignment_certificate_digest
                    || fence.participant_incarnation(self.local_id) != Some(self.sender_incarnation)
                    || fence.participant_incarnation(peer) != Some(conn.fence.receiver_incarnation)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "shuffle barrier stream incarnations differ from its assignment certificate",
                    ));
                }
            }
            let assignment_version = assignment.fence.assignment_version;
            self.validate_scope(&scope, expected_assignment_version)?;
            if assignment_version == 0 || conn.fence.assignment_version != assignment_version {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "shuffle assignment changed while opening the stream",
                ));
            }
            if conn.fence.recovery_gen != scope.recovery_gen {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "shuffle recovery generation changed while opening the stream",
                ));
            }
            let admission_permits = u32::try_from(admission_bytes).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle admission is too large",
                )
            })?;
            let control = matches!(msg, ShuffleMessage::Barrier(_));
            let (peer_byte_budget, node_budget) = if control {
                (
                    &conn.control_byte_budget,
                    &self.checkpointed_control_node_budget,
                )
            } else {
                (&conn.byte_budget, &self.node_budget)
            };
            let peer_budget = tokio::select! {
                biased;
                () = scope.cancel.cancelled() => return Err(scope_cancelled_io()),
                permit = Arc::clone(peer_byte_budget).acquire_many_owned(admission_permits) => {
                    permit.map_err(|_| {
                        io::Error::new(io::ErrorKind::BrokenPipe, "shuffle byte budget closed")
                    })?
                }
            };
            let node_budget = tokio::select! {
                biased;
                () = scope.cancel.cancelled() => return Err(scope_cancelled_io()),
                permit = Arc::clone(node_budget).acquire_many_owned(admission_permits) => {
                    permit.map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "shuffle node byte budget closed",
                        )
                    })?
                }
            };
            let budget = OutboundReservation {
                peer: peer_budget,
                node: node_budget,
            };
            let (prepared, budget) = prepare_outbound_message(msg, budget).await?;
            self.validate_scope(&scope, expected_assignment_version)?;
            // Stamp ordered frames before enqueue. A frame the transport later discards leaves a
            // hole the peer can see; a barrier carries the running count.
            let gen = scope.recovery_gen;
            let seq = {
                // Parking-lot guards are deliberately scoped before the enqueue await. The
                // assignment read guard also serializes sequence allocation with scope rotation.
                let assignment = self.assignment.read();
                let current = assignment.as_ref().ok_or_else(scope_cancelled_io)?;
                self.validate_scope_locked(&scope, expected_assignment_version, current)?;
                let mut seqs = self.seqs.lock();
                self.validate_scope_locked(&scope, expected_assignment_version, current)?;
                if !scope.matches_fence(&conn.fence) {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionAborted,
                        "shuffle assignment changed before sequence allocation",
                    ));
                }
                let counter = seqs.entry(peer).or_insert(0);
                match msg {
                    ShuffleMessage::Data { .. } | ShuffleMessage::Frontier { .. } => {
                        let seq = *counter;
                        *counter = counter.checked_add(1).ok_or_else(|| {
                            io::Error::other("shuffle delivery sequence exhausted")
                        })?;
                        seq
                    }
                    ShuffleMessage::Barrier(_) => *counter,
                }
            };
            let out = Outbound {
                gen,
                assignment_version,
                seq,
                msg: prepared,
                assignment_digest: assignment_fence.map(CheckpointAssignmentFence::digest),
                _budget: budget,
            };
            self.validate_scope(&scope, expected_assignment_version)?;
            match conn.tx.try_send(out) {
                Ok(()) => self.process_lease.require_live_io(),
                Err(crossfire::TrySendError::Full(out)) => tokio::select! {
                    biased;
                    () = self.process_lease.wait_until_lost() => Err(process_lease_expired_io()),
                    () = scope.cancel.cancelled() => Err(scope_cancelled_io()),
                    result = conn.tx.send(out) => {
                        result.map_err(|_| io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            format!("shuffle stream to peer {peer} closed"),
                        ))?;
                        self.process_lease.require_live_io()
                    },
                },
                Err(crossfire::TrySendError::Disconnected(_)) => Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("shuffle stream to peer {peer} closed"),
                )),
            }
        }

        fn validate_expected_assignment(&self, expected: Option<u64>) -> io::Result<()> {
            let Some(expected) = expected else {
                return Ok(());
            };
            let current = self.assignment_version.load(Ordering::Acquire);
            if expected != 0 && expected == current {
                return Ok(());
            }
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                format!(
                    "shuffle assignment scope mismatch: routed at {expected}, sender at {current}"
                ),
            ))
        }

        fn current_assignment(&self) -> io::Result<Arc<InstalledAssignment>> {
            self.process_lease.require_live_io()?;
            let assignment = self.assignment.read().clone().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "shuffle assignment certificate is not installed",
                )
            })?;
            if self.assignment_version.load(Ordering::Acquire)
                != assignment.fence.assignment_version
            {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "shuffle assignment certificate is not active",
                ));
            }
            Ok(assignment)
        }

        fn current_scope(&self, expected: Option<u64>) -> io::Result<ScopeLease> {
            let assignment = self.assignment.read();
            let installed = assignment.as_ref().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotConnected,
                    "shuffle assignment certificate is not installed",
                )
            })?;
            let version = self.assignment_version.load(Ordering::Acquire);
            let recovery_gen = self.recovery_gen.load(Ordering::Acquire);
            let cancel = self.scope_cancel.read().clone();
            self.process_lease.require_live_io()?;
            if version == 0
                || version != installed.fence.assignment_version
                || cancel.is_cancelled()
            {
                return Err(scope_cancelled_io());
            }
            if expected.is_some_and(|expected| expected == 0 || expected != version) {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!(
                        "shuffle assignment scope mismatch: routed at {}, sender at {version}",
                        expected.unwrap_or_default()
                    ),
                ));
            }
            Ok(ScopeLease {
                assignment: Arc::clone(installed),
                recovery_gen,
                cancel,
            })
        }

        fn validate_scope(&self, scope: &ScopeLease, expected: Option<u64>) -> io::Result<()> {
            let assignment = self.assignment.read();
            let current = assignment.as_ref().ok_or_else(scope_cancelled_io)?;
            self.validate_scope_locked(scope, expected, current)
        }

        fn validate_scope_locked(
            &self,
            scope: &ScopeLease,
            expected: Option<u64>,
            current: &Arc<InstalledAssignment>,
        ) -> io::Result<()> {
            self.process_lease.require_live_io()?;
            if scope.cancel.is_cancelled() {
                return Err(scope_cancelled_io());
            }
            if !Arc::ptr_eq(current, &scope.assignment)
                || self.assignment_version.load(Ordering::Acquire)
                    != scope.assignment.fence.assignment_version
                || self.recovery_gen.load(Ordering::Acquire) != scope.recovery_gen
            {
                return Err(scope_cancelled_io());
            }
            self.validate_expected_assignment(expected)
        }

        /// Establish an exact-scope stream to every remote assignment participant without sending
        /// a frame or consuming a delivery sequence.
        ///
        /// # Errors
        /// Returns the first peer connection or handshake error after attempting the full roster.
        pub async fn establish_assignment_mesh(
            &self,
            assignment_fence: &CheckpointAssignmentFence,
        ) -> io::Result<()> {
            let installed = self.current_assignment()?;
            if *assignment_fence != installed.fence
                || assignment_fence.digest() != installed.digest
                || !assignment_fence.is_canonical()
                || assignment_fence.participant_incarnation(self.local_id)
                    != Some(self.sender_incarnation)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle mesh does not match the installed assignment certificate",
                ));
            }
            let scope = self.current_scope(Some(assignment_fence.assignment_version))?;
            let results = futures::future::join_all(
                assignment_fence
                    .participants
                    .iter()
                    .map(|participant| participant.node_id)
                    .filter(|peer| *peer != self.local_id)
                    .map(|peer| {
                        let peer_scope = scope.clone();
                        async move { (peer, self.connection_for(peer, &peer_scope).await) }
                    }),
            )
            .await;
            let mut first_error = None;
            for (peer, result) in results {
                if let Err(error) = result {
                    first_error.get_or_insert_with(|| {
                        io::Error::new(
                            error.kind(),
                            format!("shuffle assignment mesh peer {peer}: {error}"),
                        )
                    });
                }
            }
            if let Some(error) = first_error {
                return Err(error);
            }
            self.validate_scope(&scope, Some(assignment_fence.assignment_version))
        }

        /// Ship `barrier` to every required peer. All peers are attempted, but
        /// any failure rejects the cut so the coordinator can abort promptly.
        ///
        /// # Errors
        /// Returns the first peer error after attempting the full fan-out.
        pub async fn fan_out_barrier(
            &self,
            peers: &[ShufflePeerId],
            barrier: CheckpointBarrier,
            assignment_fence: &CheckpointAssignmentFence,
        ) -> io::Result<()> {
            validate_checkpoint_barrier(barrier)?;
            let installed = self.current_assignment()?;
            let expected_peers: Vec<_> = assignment_fence
                .participants
                .iter()
                .map(|participant| participant.node_id)
                .filter(|peer| *peer != self.local_id)
                .collect();
            let mut actual_peers = peers.to_vec();
            actual_peers.sort_unstable();
            let has_duplicates = actual_peers.windows(2).any(|pair| pair[0] == pair[1]);
            if *assignment_fence != installed.fence
                || assignment_fence.digest() != installed.digest
                || !assignment_fence.is_canonical()
                || !assignment_fence.contains(self.local_id)
                || assignment_fence.participant_incarnation(self.local_id)
                    != Some(self.sender_incarnation)
                || has_duplicates
                || actual_peers != expected_peers
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle barrier peers do not exactly cover the assignment roster",
                ));
            }
            self.validate_expected_assignment(Some(assignment_fence.assignment_version))?;
            let cid = barrier.checkpoint_id;
            let msg = ShuffleMessage::Barrier(barrier);
            let mut first_scope_cancel = None;
            let mut first_peer_error = None;
            let results = futures::future::join_all(peers.iter().map(|&peer| {
                let msg = &msg;
                async move {
                    (
                        peer,
                        self.send_to_inner(
                            peer,
                            msg,
                            Some(assignment_fence.assignment_version),
                            Some(assignment_fence),
                        )
                        .await,
                    )
                }
            }))
            .await;
            for (peer, result) in results {
                match result {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::warn!(
                            peer,
                            checkpoint_id = cid,
                            error = %e,
                            "shuffle barrier fan-out: required peer unreachable"
                        );
                        if is_scope_cancelled(&e) {
                            first_scope_cancel.get_or_insert(e);
                        } else {
                            first_peer_error.get_or_insert(e);
                        }
                    }
                }
            }
            if let Some(error) = first_peer_error.or(first_scope_cancel) {
                if is_scope_cancelled(&error) {
                    self.process_lease.require_live_io()?;
                }
                return Err(error);
            }
            Ok(())
        }

        /// Resolve and cache `peer`'s address from the KV; `None` when unavailable.
        async fn discover_peer(&self, peer: ShufflePeerId) -> Option<SocketAddr> {
            let kv = self.kv.as_ref()?;
            let raw = kv
                .read_from(crate::cluster::discovery::NodeId(peer), SHUFFLE_ADDR_KEY)
                .await?;
            let addr = match raw.parse::<SocketAddr>() {
                Ok(a) => a,
                Err(_) => tokio::net::lookup_host(&raw).await.ok()?.next()?,
            };
            self.peers.lock().insert(peer, addr);
            Some(addr)
        }

        async fn connection_for(
            &self,
            peer: ShufflePeerId,
            scope: &ScopeLease,
        ) -> io::Result<Arc<PeerConn>> {
            self.validate_scope(scope, None)?;
            let assignment = &scope.assignment;
            if !assignment.fence.contains(peer) || peer == self.local_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle peer is outside the installed assignment roster",
                ));
            }
            let assignment_version = assignment.fence.assignment_version;
            let recovery_gen = scope.recovery_gen;
            let key = peer;
            if let Some(existing) = self.pool.lock().get(&key).cloned() {
                if existing.is_alive()
                    && existing.fence.assignment_version == assignment_version
                    && existing.fence.assignment_certificate_digest == assignment.digest
                    && existing.fence.recovery_gen == recovery_gen
                {
                    return Ok(existing);
                }
            }
            let connect_lock = self
                .connect_locks
                .lock()
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone();
            let _connect_guard = tokio::select! {
                biased;
                () = self.process_lease.wait_until_lost() => {
                    return Err(process_lease_expired_io());
                }
                () = scope.cancel.cancelled() => return Err(scope_cancelled_io()),
                guard = connect_lock.lock() => guard,
            };
            self.validate_scope(scope, None)?;
            if let Some(existing) = self.pool.lock().get(&key).cloned() {
                if existing.is_alive()
                    && existing.fence.assignment_version == assignment_version
                    && existing.fence.assignment_certificate_digest == assignment.digest
                    && existing.fence.recovery_gen == recovery_gen
                {
                    return Ok(existing);
                }
            }
            // Purge a dead entry so we reopen the call below.
            self.pool
                .lock()
                .retain(|pool_key, connection| *pool_key != key || connection.is_alive());

            // Re-resolve on reconnect (peers may restart on a new port); fall back
            // to a statically registered address when there's no KV.
            let discovered = tokio::select! {
                biased;
                () = self.process_lease.wait_until_lost() => {
                    return Err(process_lease_expired_io());
                }
                () = scope.cancel.cancelled() => return Err(scope_cancelled_io()),
                discovered = self.discover_peer(peer) => discovered,
            };
            let addr = match discovered {
                Some(addr) => addr,
                None => self.peers.lock().get(&peer).copied().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("peer {peer} has no registered shuffle address"),
                    )
                })?,
            };

            tracing::debug!(peer, addr = %addr, "shuffle reconnecting to peer");
            let expected_receiver_incarnation = assignment
                .fence
                .participant_incarnation(peer)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!("shuffle peer {peer} is absent from the assignment certificate"),
                    )
                })?;
            let conn = Arc::new(
                open_call(OpenCall {
                    local_id: self.local_id,
                    peer,
                    addr,
                    sender_incarnation: self.sender_incarnation,
                    assignment_version,
                    assignment_certificate_digest: assignment.digest,
                    expected_receiver_incarnation,
                    recovery_gen,
                    current_assignment: Arc::clone(&self.assignment_version),
                    current_recovery_gen: Arc::clone(&self.recovery_gen),
                    scope_cancel: scope.cancel.clone(),
                })
                .await?,
            );
            self.validate_scope(scope, None)?;
            let mut pool = self.pool.lock();
            if scope.cancel.is_cancelled()
                || self.assignment_version.load(Ordering::Acquire) != assignment_version
                || self.recovery_gen.load(Ordering::Acquire) != recovery_gen
            {
                return Err(scope_cancelled_io());
            }
            pool.insert(key, Arc::clone(&conn));
            Ok(conn)
        }

        #[cfg(test)]
        pub(crate) fn disconnect_peer_for_test(&self, peer: ShufflePeerId) {
            self.pool.lock().remove(&peer);
        }

        #[cfg(test)]
        pub(crate) async fn hold_outbound_budget_for_test(
            &self,
            peer: ShufflePeerId,
        ) -> io::Result<OwnedSemaphorePermit> {
            let scope = self.current_scope(None)?;
            let conn = self.connection_for(peer, &scope).await?;
            Arc::clone(&conn.byte_budget)
                .acquire_many_owned(
                    u32::try_from(OUTBOUND_PEER_BUDGET_BYTES)
                        .expect("outbound test budget fits u32"),
                )
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "test budget closed"))
        }
    }

    /// Bind a proposed stream to the receiver's exact identity and scope.
    async fn negotiate_identity(
        client: &mut ShuffleTransportClient<Channel>,
        call: &OpenCall,
    ) -> io::Result<StreamFence> {
        let stream_id = Uuid::new_v4();
        let response = tokio::select! {
            biased;
            () = call.scope_cancel.cancelled() => return Err(scope_cancelled_io()),
            response = client.handshake(Request::new(HandshakeRequest {
                sender_node_id: call.local_id,
                sender_incarnation: call.sender_incarnation.as_bytes().to_vec(),
                stream_id: stream_id.as_bytes().to_vec(),
                assignment_version: call.assignment_version,
                recovery_gen: call.recovery_gen,
                assignment_certificate_digest: call.assignment_certificate_digest.to_vec(),
            })) => response.map_err(status_io)?.into_inner(),
        };
        let receiver_incarnation = parse_uuid(
            &response.receiver_incarnation,
            "handshake receiver incarnation",
        )
        .map_err(io_err)?;
        if response.receiver_node_id != call.peer
            || receiver_incarnation != call.expected_receiver_incarnation
            || response.sender_incarnation.as_slice() != call.sender_incarnation.as_bytes()
            || response.stream_id.as_slice() != stream_id.as_bytes()
            || response.assignment_version != call.assignment_version
            || response.assignment_certificate_digest.as_slice()
                != call.assignment_certificate_digest.as_slice()
            || response.recovery_gen != call.recovery_gen
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shuffle handshake response did not match the requested stream identity",
            ));
        }
        Ok(StreamFence {
            sender_node_id: call.local_id,
            sender_incarnation: call.sender_incarnation,
            receiver_incarnation,
            stream_id,
            assignment_version: call.assignment_version,
            assignment_certificate_digest: call.assignment_certificate_digest,
            recovery_gen: call.recovery_gen,
        })
    }

    /// Open a client-streaming call after the identity handshake completes.
    async fn open_call(call: OpenCall) -> io::Result<PeerConn> {
        let endpoint = crate::cluster::control::tls::client_endpoint(&call.addr.to_string())
            .map_err(io_err)?
            .tcp_nodelay(true);
        let channel = tokio::select! {
            biased;
            () = call.scope_cancel.cancelled() => return Err(scope_cancelled_io()),
            channel = endpoint.connect() => channel.map_err(io_err)?,
        };
        let mut client = ShuffleTransportClient::<Channel>::new(channel)
            .max_decoding_message_size(MAX_SHUFFLE_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_SHUFFLE_MESSAGE_BYTES);
        let fence = negotiate_identity(&mut client, &call).await?;
        let OpenCall {
            addr,
            current_assignment,
            current_recovery_gen,
            scope_cancel,
            ..
        } = call;
        let (tx, rx) = mpsc::bounded_async::<Outbound>(SEND_QUEUE);
        let alive = Arc::new(AtomicBool::new(true));
        let alive_for_driver = Arc::clone(&alive);

        // Request stream: a `Hello` chained onto an unfold over the per-peer
        // receiver, serializing dequeued messages here in the driver task.
        let hello = ShuffleFrame {
            kind: Some(shuffle_frame::Kind::Hello(hello_for(
                fence.sender_node_id,
                &fence,
            ))),
        };
        let stream_fence = fence;
        let outbound = futures::stream::once(async move { hello }).chain(futures::stream::unfold(
            (
                rx,
                current_assignment,
                current_recovery_gen,
                scope_cancel.clone(),
                stream_fence,
                None::<Encoded>,
            ),
            |(
                rx,
                current_assignment,
                current_recovery_gen,
                scope_cancel,
                stream_fence,
                mut encoded,
            )| async move {
                loop {
                    if let Some(pending) = encoded.as_mut() {
                        if let Some(frame) = pending.frames.pop_front() {
                            return Some((
                                frame,
                                (
                                    rx,
                                    current_assignment,
                                    current_recovery_gen,
                                    scope_cancel,
                                    stream_fence,
                                    encoded,
                                ),
                            ));
                        }
                        let _ = encoded.take();
                    }
                    let out = tokio::select! {
                        biased;
                        () = scope_cancel.cancelled() => return None,
                        out = rx.recv() => out.ok()?,
                    };
                    if current_assignment.load(Ordering::Acquire) != stream_fence.assignment_version
                        || current_recovery_gen.load(Ordering::Acquire) != stream_fence.recovery_gen
                    {
                        return None;
                    }
                    match frame_message(out) {
                        Ok(message) => encoded = Some(message),
                        Err(error) => {
                            tracing::warn!(%error, "shuffle frame construction failed; closing stream");
                            return None;
                        }
                    }
                }
            },
        ));

        let driver = tokio::spawn(async move {
            // A break here means frames already handed to tonic may never reach the peer. The
            // peer detects the hole from the sequence, so this only needs to be visible, not fatal.
            tokio::select! {
                biased;
                () = scope_cancel.cancelled() => {}
                result = client.shuffle(Request::new(outbound)) => {
                    if let Err(status) = result {
                        tracing::warn!(peer_addr = %addr, error = %status, "shuffle stream broke");
                    }
                }
            }
            alive_for_driver.store(false, Ordering::Release);
        });

        Ok(PeerConn {
            tx,
            byte_budget: Arc::new(Semaphore::new(OUTBOUND_PEER_BUDGET_BYTES)),
            control_byte_budget: Arc::new(Semaphore::new(CHECKPOINTED_CONTROL_PEER_BUDGET_BYTES)),
            send_lock: tokio::sync::Mutex::new(()),
            alive,
            driver,
            fence,
        })
    }

    struct PendingInbound<'a> {
        slot: &'a Mutex<Option<Inbound>>,
        ready: &'a AtomicBool,
        inbound: Option<Inbound>,
    }

    impl PendingInbound<'_> {
        fn take(mut self) -> Inbound {
            self.inbound.take().expect("pending shuffle inbound")
        }
    }

    impl Drop for PendingInbound<'_> {
        fn drop(&mut self) {
            if let Some(inbound) = self.inbound.take() {
                let mut slot = self.slot.lock();
                let replaced = slot.replace(inbound);
                assert!(replaced.is_none(), "shuffle deferred receive slot occupied");
                self.ready.store(true, Ordering::Release);
            }
        }
    }

    /// Inbound side of the shuffle fabric: a Tonic `ShuffleTransport` server that
    /// surfaces every received frame, attributed to its peer, on the bounded queue.
    pub struct ShuffleReceiver {
        local_id: ShufflePeerId,
        local_addr: SocketAddr,
        receiver_incarnation: Uuid,
        // `AsyncRx` is `Send` but `!Sync`, yet `Arc<ShuffleReceiver>` must be `Sync`.
        // Park it behind a `Mutex<Option<_>>` and hand it out via a take/return guard so
        // the single consumer never holds the guard across `.await`; the guard
        // restores it on drop so a cancelled `recv` can't strand it.
        rx: Mutex<Option<InboundRx>>,
        rx_returned: Arc<tokio::sync::Notify>,
        deferred_recv: Mutex<Option<Inbound>>,
        deferred_recv_ready: AtomicBool,
        #[cfg(test)]
        recv_deferred_pause: Mutex<Option<Arc<tokio::sync::Notify>>>,
        #[cfg(test)]
        assignment_wait_pause: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
        server: JoinHandle<()>,
        holdover: Arc<Holdover>,
        /// Monotonic notification that an in-band barrier reached the bounded receive queue.
        barrier_arrivals: Arc<AtomicU64>,
        barrier_reconciled: AtomicU64,
        /// Inbound data frames stamped below this are pre-rewind and discarded.
        recovery_gen: Arc<AtomicU64>,
        recovery_transition: Mutex<()>,
        assignment: Arc<RwLock<Option<Arc<InstalledAssignment>>>>,
        assignment_version: Arc<AtomicU64>,
        scope_cancel: Arc<RwLock<CancellationToken>>,
        process_lease: Arc<ProcessLeaseGate>,
        /// True only while an exact retained certificate is closed by a transient authority read
        /// failure. Delivery expectations remain intact for same-version reactivation.
        assignment_suspended: AtomicBool,
        assignment_resumed: tokio::sync::Notify,
        pending_handshakes: Arc<PendingHandshakes>,
        delivery: Arc<DeliveryTracker>,
        #[cfg(test)]
        active_streams: Arc<Semaphore>,
    }

    impl Drop for ShuffleReceiver {
        fn drop(&mut self) {
            // Abort the server task so the listener closes and in-flight peer
            // streams break — senders then observe the error and reconnect.
            self.server.abort();
        }
    }

    impl std::fmt::Debug for ShuffleReceiver {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ShuffleReceiver")
                .field("local_id", &self.local_id)
                .field("local_addr", &self.local_addr)
                .finish_non_exhaustive()
        }
    }

    impl ShuffleReceiver {
        /// Bind on `addr` and start serving; the resolved address is at
        /// [`Self::local_addr`].
        ///
        /// # Errors
        /// Returns `io::Error` on bind failure.
        pub async fn bind(
            local_id: ShufflePeerId,
            addr: SocketAddr,
            receiver_incarnation: Uuid,
        ) -> io::Result<Self> {
            if local_id == 0 || receiver_incarnation.is_nil() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle receiver requires a nonzero node and non-nil incarnation",
                ));
            }
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            let (tx, rx) = mpsc::bounded_async::<Inbound>(SHUFFLE_RECV_QUEUE);

            let recovery_gen = Arc::new(AtomicU64::new(0));
            let assignment = Arc::new(RwLock::new(None));
            let assignment_version = Arc::new(AtomicU64::new(0));
            let scope_cancel = Arc::new(RwLock::new(cancelled_token()));
            let process_lease = Arc::new(ProcessLeaseGate::default());
            let pending_handshakes = Arc::new(PendingHandshakes::default());
            let delivery = Arc::new(DeliveryTracker::default());
            let barrier_arrivals = Arc::new(AtomicU64::new(0));
            let holdover = Arc::new(Holdover::new(SHUFFLE_RECV_QUEUE));
            let inbound_budget = Arc::new(InboundBudget::new(INBOUND_NODE_BUDGET_BYTES));
            let active_streams = Arc::new(Semaphore::new(MAX_ACTIVE_STREAMS));
            let active_stream_registry = Arc::new(ActiveStreamRegistry::default());
            let service = ShuffleService {
                local_id,
                receiver_incarnation,
                assignment: Arc::clone(&assignment),
                assignment_version: Arc::clone(&assignment_version),
                scope_cancel: Arc::clone(&scope_cancel),
                process_lease: Arc::clone(&process_lease),
                pending_handshakes: Arc::clone(&pending_handshakes),
                tx,
                recovery_gen: Arc::clone(&recovery_gen),
                delivery: Arc::clone(&delivery),
                barrier_arrivals: Arc::clone(&barrier_arrivals),
                holdover: Arc::clone(&holdover),
                inbound_budget,
                active_streams: Arc::clone(&active_streams),
                active_stream_registry,
            };
            let incoming = futures::stream::unfold(listener, |listener| async move {
                let item = match listener.accept().await {
                    Ok((stream, _)) => {
                        let _ = stream.set_nodelay(true);
                        Ok(stream)
                    }
                    Err(e) => Err(e),
                };
                Some((item, listener))
            });
            // Apply TLS synchronously so a bad cert fails bind() rather than
            // silently never serving.
            let mut builder = Server::builder();
            if let Some(tls) = crate::cluster::control::tls::server_tls() {
                builder = builder
                    .tls_config(tls.clone())
                    .map_err(|e| io::Error::other(format!("cluster shuffle TLS config: {e}")))?;
            }
            let router = builder.add_service(
                ShuffleTransportServer::new(service)
                    .max_decoding_message_size(MAX_SHUFFLE_MESSAGE_BYTES)
                    .max_encoding_message_size(MAX_SHUFFLE_MESSAGE_BYTES),
            );
            let server = tokio::spawn(async move {
                let _ = router.serve_with_incoming(incoming).await;
            });

            Ok(Self {
                local_id,
                local_addr,
                receiver_incarnation,
                rx: Mutex::new(Some(rx)),
                rx_returned: Arc::new(tokio::sync::Notify::new()),
                deferred_recv: Mutex::new(None),
                deferred_recv_ready: AtomicBool::new(false),
                #[cfg(test)]
                recv_deferred_pause: Mutex::new(None),
                #[cfg(test)]
                assignment_wait_pause: Mutex::new(None),
                server,
                holdover,
                barrier_arrivals,
                barrier_reconciled: AtomicU64::new(0),
                recovery_gen,
                recovery_transition: Mutex::new(()),
                assignment,
                assignment_version,
                scope_cancel,
                process_lease,
                assignment_suspended: AtomicBool::new(false),
                assignment_resumed: tokio::sync::Notify::new(),
                pending_handshakes,
                delivery,
                #[cfg(test)]
                active_streams,
            })
        }

        /// Bind inbound admission to this process's renewable cluster lease.
        ///
        /// This must be installed before an assignment certificate is activated.
        ///
        /// # Errors
        /// Returns an error for an expired lease, a different previously installed deadline, or
        /// an already-active assignment.
        pub fn install_process_lease_deadline(
            &self,
            deadline: Arc<LeaseDeadline>,
        ) -> io::Result<()> {
            let _assignment = self.assignment.write();
            if self.assignment_version.load(Ordering::Acquire) != 0
                && !self.process_lease.is_installed_deadline(&deadline)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle process lease must be installed before assignment activation",
                ));
            }
            self.process_lease.install(deadline)
        }

        /// Install the exact assignment certificate accepted by inbound streams. Existing
        /// streams are rejected on their next frame; the new delivery domain starts at zero.
        ///
        /// # Errors
        /// Returns an error for a malformed certificate, a same-version certificate conflict,
        /// or a certificate that does not bind this exact receiver process.
        pub fn install_assignment_fence(
            &self,
            fence: &CheckpointAssignmentFence,
            owners: &[ShufflePeerId],
        ) -> io::Result<bool> {
            let next = InstalledAssignment::for_process(
                fence,
                owners,
                self.local_id,
                self.receiver_incarnation,
            )?;
            let mut assignment = self.assignment.write();
            self.process_lease.require_live_io()?;
            if let Some(current) = assignment.as_ref() {
                if next.fence.assignment_version < current.fence.assignment_version {
                    return Ok(false);
                }
                if next.fence.assignment_version == current.fence.assignment_version {
                    if next.digest == current.digest
                        && next.fence == current.fence
                        && next.owners == current.owners
                    {
                        if self.assignment_version.load(Ordering::Acquire)
                            == next.fence.assignment_version
                        {
                            return Ok(false);
                        }
                        if self.assignment_suspended.load(Ordering::Acquire) {
                            rotate_scope_token(&self.scope_cancel, &self.process_lease, true);
                            self.pending_handshakes.clear();
                            self.assignment_suspended.store(false, Ordering::Release);
                            self.assignment_version
                                .store(next.fence.assignment_version, Ordering::Release);
                            self.assignment_resumed.notify_waiters();
                            return Ok(true);
                        }
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "an invalidated shuffle assignment requires a higher version",
                        ));
                    }
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "conflicting shuffle assignment certificate for an installed version",
                    ));
                }
            }
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            self.pending_handshakes.clear();
            self.delivery.reset_assignment();
            self.holdover.clear_staged_frontiers();
            let version = next.fence.assignment_version;
            *assignment = Some(next);
            self.assignment_suspended.store(false, Ordering::Release);
            rotate_scope_token(&self.scope_cancel, &self.process_lease, true);
            self.assignment_version.store(version, Ordering::Release);
            self.assignment_resumed.notify_waiters();
            Ok(true)
        }

        /// Temporarily close inbound admission after a transient durable-authority read failure.
        /// Pending handshakes are discarded, while committed delivery expectations remain bound
        /// to the retained certificate for exact same-version reactivation.
        pub fn suspend_assignment_fence(&self) {
            let assignment = self.assignment.write();
            if assignment.is_none() || self.assignment_version.load(Ordering::Acquire) == 0 {
                return;
            }
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            self.pending_handshakes.clear();
            self.assignment_suspended.store(true, Ordering::Release);
            self.assignment_version.store(0, Ordering::Release);
        }

        /// Reject all streams while an adopted owner map is awaiting its owner-complete process
        /// certificate. Pending tokens and sequence expectations cannot cross this boundary.
        pub fn invalidate_assignment_fence(&self) {
            let _assignment = self.assignment.write();
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            self.assignment_suspended.store(false, Ordering::Release);
            self.assignment_version.store(0, Ordering::Release);
            self.pending_handshakes.clear();
            self.delivery.reset_assignment();
            self.holdover.clear_staged_frontiers();
            self.assignment_resumed.notify_waiters();
        }

        /// Assignment scope currently accepted for inbound streams and staged frames.
        #[must_use]
        pub fn assignment_version(&self) -> u64 {
            self.assignment_version.load(Ordering::Acquire)
        }

        /// Digest of the exact assignment certificate currently active for inbound streams.
        #[must_use]
        pub fn active_assignment_digest(&self) -> Option<[u8; 32]> {
            let assignment = self.assignment.read();
            assignment.as_ref().and_then(|installed| {
                (self.assignment_version.load(Ordering::Acquire)
                    == installed.fence.assignment_version)
                    .then_some(installed.digest)
            })
        }

        /// Advance the inbound recovery scope. Existing streams are rejected; the next exact
        /// handshake re-baselines sequence after the coordinated rewind.
        pub fn set_recovery_gen(&self, gen: u64) {
            let _transition = self.recovery_transition.lock();
            let _assignment = self.assignment.write();
            let previous = self.recovery_gen.load(Ordering::Acquire);
            if gen <= previous {
                return;
            }
            rotate_scope_token(&self.scope_cancel, &self.process_lease, false);
            self.pending_handshakes.clear();
            self.delivery.prepare_recovery(gen);
            self.holdover.clear_staged_frontiers();
            self.recovery_gen.store(gen, Ordering::Release);
            rotate_scope_token(
                &self.scope_cancel,
                &self.process_lease,
                self.assignment_version.load(Ordering::Acquire) != 0,
            );
            // The next admitted Hello carries the new generation and re-baselines only that peer.
            // Resetting all peers after publishing this atomic races new-generation delivery.
        }

        /// Recovery scope currently accepted for inbound streams and staged frames.
        #[must_use]
        pub fn recovery_gen(&self) -> u64 {
            self.recovery_gen.load(Ordering::Acquire)
        }

        /// Process incarnation bound into every accepted stream handshake.
        #[must_use]
        pub const fn incarnation(&self) -> Uuid {
            self.receiver_incarnation
        }

        /// Node id bound into every accepted stream handshake.
        #[must_use]
        pub const fn local_id(&self) -> ShufflePeerId {
            self.local_id
        }

        #[cfg(test)]
        pub(crate) fn active_streams_for_test(&self) -> usize {
            MAX_ACTIVE_STREAMS - self.active_streams.available_permits()
        }

        #[cfg(test)]
        pub(crate) fn committed_sequence_for_test(&self, peer: ShufflePeerId) -> Option<u64> {
            self.delivery
                .peers
                .lock()
                .get(&peer)
                .map(|state| state.expected)
        }

        #[cfg(test)]
        pub(crate) fn barrier_arrivals_for_test(&self) -> u64 {
            self.barrier_arrivals.load(Ordering::Acquire)
        }

        #[cfg(test)]
        pub(super) fn assignment_fence_for_test(&self) -> CheckpointAssignmentFence {
            self.assignment
                .read()
                .as_ref()
                .expect("test assignment")
                .fence
                .clone()
        }

        #[cfg(test)]
        pub(crate) fn pause_next_recv_after_defer_for_test(&self) -> Arc<tokio::sync::Notify> {
            let entered = Arc::new(tokio::sync::Notify::new());
            let replaced = self
                .recv_deferred_pause
                .lock()
                .replace(Arc::clone(&entered));
            assert!(
                replaced.is_none(),
                "shuffle receive pause already installed"
            );
            entered
        }

        #[cfg(test)]
        pub(super) fn pause_next_assignment_wait_for_test(
            &self,
        ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let replaced = self
                .assignment_wait_pause
                .lock()
                .replace((Arc::clone(&entered), Arc::clone(&release)));
            assert!(
                replaced.is_none(),
                "shuffle assignment wait pause already installed"
            );
            (entered, release)
        }

        #[cfg(test)]
        pub(super) async fn wait_while_assignment_suspended_for_test(&self) -> bool {
            self.wait_while_assignment_suspended().await
        }

        async fn wait_while_assignment_suspended(&self) -> bool {
            loop {
                let resumed = self.assignment_resumed.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if self.process_lease.require_live_io().is_err() {
                    return false;
                }
                if !self.assignment_suspended.load(Ordering::Acquire) {
                    return true;
                }
                #[cfg(test)]
                let assignment_wait_pause = { self.assignment_wait_pause.lock().take() };
                #[cfg(test)]
                if let Some((entered, release)) = assignment_wait_pause {
                    entered.notify_one();
                    release.notified().await;
                }
                tokio::select! {
                    biased;
                    () = self.process_lease.wait_until_lost() => return false,
                    () = &mut resumed => {}
                }
            }
        }

        fn consumption_scope(
            &self,
        ) -> Option<parking_lot::RwLockReadGuard<'_, Option<Arc<InstalledAssignment>>>> {
            let assignment = self.assignment.read();
            (!self.assignment_suspended.load(Ordering::Acquire)).then_some(assignment)
        }

        fn take_deferred_recv(&self) -> Option<Inbound> {
            if !self.deferred_recv_ready.load(Ordering::Acquire) {
                return None;
            }
            let mut slot = self.deferred_recv.lock();
            let inbound = slot.take();
            self.deferred_recv_ready.store(false, Ordering::Release);
            inbound
        }

        /// Cumulative delivery-loss incidents. A value above the recovered floor means an epoch
        /// must not seal; exact missing-frame magnitude is diagnostic and never trusted for this
        /// non-wrapping correctness signal.
        #[must_use]
        pub fn delivery_loss_incidents(&self) -> Arc<AtomicU64> {
            Arc::clone(&self.delivery.delivery_loss_incidents)
        }

        /// Delivery-loss incidents covered by a completed coordinated rewind. The callback uses
        /// this durable in-process floor so repaired incidents do not fault restored state again.
        #[must_use]
        pub fn recovered_delivery_loss_incidents(&self) -> Arc<AtomicU64> {
            Arc::clone(&self.delivery.recovered_delivery_loss_incidents)
        }

        /// Whether an admitted delivery loss has not yet been repaired by coordinated recovery.
        #[must_use]
        pub fn has_unrecovered_delivery_loss(&self) -> bool {
            self.delivery
                .delivery_loss_incidents
                .load(Ordering::Acquire)
                > self
                    .delivery
                    .recovered_delivery_loss_incidents
                    .load(Ordering::Acquire)
        }

        /// Mark the loss cutoff captured when `gen` began as repaired. Loss detected after the
        /// generation advanced remains above this floor and still faults the next checkpoint.
        ///
        /// Returns `false` when `gen` is not the currently prepared recovery generation.
        pub fn complete_recovery(&self, gen: u64) -> bool {
            self.recovery_gen.load(Ordering::Acquire) == gen && self.delivery.complete_recovery(gen)
        }

        /// Bind and publish the listener's address into `kv` for peer discovery.
        ///
        /// # Errors
        /// Returns `io::Error` on bind failure.
        pub async fn bind_with_kv(
            local_id: ShufflePeerId,
            addr: SocketAddr,
            kv: Arc<dyn ClusterKv>,
            receiver_incarnation: Uuid,
        ) -> io::Result<Self> {
            let recv = Self::bind(local_id, addr, receiver_incarnation).await?;
            kv.write(SHUFFLE_ADDR_KEY, recv.local_addr.to_string())
                .await;
            Ok(recv)
        }

        /// Local socket address the server is bound to.
        #[must_use]
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        /// Revalidate a queued envelope at the consumer boundary. A stream can pass its final
        /// atomic check immediately before an assignment/recovery transition and enqueue after
        /// it, so wire admission alone is not a sufficient fold fence.
        fn retain_queued_scope(
            &self,
            peer: ShufflePeerId,
            sender_incarnation: Uuid,
            receiver_incarnation: Uuid,
            assignment_version: u64,
            recovery_gen: u64,
        ) -> bool {
            if self.process_lease.require_live_io().is_err() {
                return false;
            }
            let Some(assignment) = self.consumption_scope() else {
                return false;
            };
            let Some(assignment) = assignment.as_ref() else {
                return false;
            };
            let current = self.retain_queued_scope_for_assignment(
                assignment,
                peer,
                sender_incarnation,
                receiver_incarnation,
                assignment_version,
                recovery_gen,
            );
            if self.process_lease.require_live_io().is_err() {
                return false;
            }
            current
        }

        fn retain_queued_scope_for_assignment(
            &self,
            assignment: &InstalledAssignment,
            peer: ShufflePeerId,
            sender_incarnation: Uuid,
            receiver_incarnation: Uuid,
            assignment_version: u64,
            recovery_gen: u64,
        ) -> bool {
            let current_recovery = self.recovery_gen.load(Ordering::Acquire);
            if receiver_incarnation != self.receiver_incarnation {
                self.delivery
                    .note_loss(peer, 1, "queued-receiver-incarnation");
                return false;
            }
            if recovery_gen < current_recovery {
                // A coordinated rewind deliberately abandons this generation. Replay regenerates
                // it, so dropping here is required but must not create a second recovery fault.
                return false;
            }
            let current_assignment = self.assignment_version.load(Ordering::Acquire);
            let current = recovery_gen == current_recovery
                && assignment_version == current_assignment
                && assignment.fence.assignment_version == assignment_version
                && assignment.certifies(peer, sender_incarnation)
                && assignment.certifies(self.local_id, receiver_incarnation)
                && self.delivery.matches_process_scope(
                    peer,
                    sender_incarnation,
                    receiver_incarnation,
                    assignment_version,
                    recovery_gen,
                );
            if !current {
                self.delivery.note_loss(peer, 1, "queued-stream-scope");
            }
            current
        }

        fn retain_received(&self, received: &ReceivedShuffle) -> bool {
            match self.holdover.is_retired_barrier(received) {
                Ok(true) => false,
                Err(_) => {
                    self.delivery
                        .note_loss(received.peer, 1, "retired-barrier-assignment-digest");
                    false
                }
                Ok(false) => self.retain_queued_scope(
                    received.peer,
                    received.sender_incarnation,
                    received.receiver_incarnation,
                    received.assignment_version,
                    received.recovery_gen,
                ),
            }
        }

        fn retain_batch(&self, received: &ReceivedBatch) -> bool {
            self.retain_queued_scope(
                received.peer,
                received.sender_incarnation,
                received.receiver_incarnation,
                received.assignment_version,
                received.recovery_gen,
            )
        }

        fn stage_frontier_if_current(&self, cut: ReceivedFrontierCut) -> bool {
            let frontier = cut.frontier();
            let assignment = self.assignment.read();
            let Some(assignment) = assignment.as_ref() else {
                return false;
            };
            let current = self.retain_queued_scope_for_assignment(
                assignment,
                frontier.peer,
                frontier.sender_incarnation,
                frontier.receiver_incarnation,
                frontier.assignment_version,
                frontier.recovery_gen,
            ) && self.process_lease.require_live_io().is_ok();
            if current {
                self.holdover.stage_frontier(cut);
            }
            current
        }

        /// Await the next `(peer_id, msg)`; `None` once the server stops and the
        /// queue drains. Concurrent callers serialise via `rx_returned`;
        /// cancellation-safe.
        pub async fn recv(&self) -> Option<ReceivedShuffle> {
            loop {
                if !self.wait_while_assignment_suspended().await {
                    return None;
                }

                // The receiver lease serializes deferred and live queue admission. A
                // cancelled receive can therefore require at most one deferred slot.
                let taken = { self.rx.lock().take() };
                let Some(rx) = taken else {
                    tokio::select! {
                        biased;
                        () = self.process_lease.wait_until_lost() => return None,
                        () = self.rx_returned.notified() => {}
                    }
                    continue;
                };
                let mut guard = RxReturnGuard {
                    slot: &self.rx,
                    notify: &self.rx_returned,
                    rx: Some(rx),
                };

                if let Some(inbound) = self.take_deferred_recv() {
                    let pending = PendingInbound {
                        slot: &self.deferred_recv,
                        ready: &self.deferred_recv_ready,
                        inbound: Some(inbound),
                    };
                    let received = pending.take().into_received();
                    drop(guard);
                    if self.retain_received(&received) {
                        return Some(received);
                    }
                    continue;
                }
                let inbound = tokio::select! {
                    biased;
                    () = self.process_lease.wait_until_lost() => return None,
                    result = guard.rx.as_mut()?.recv() => result.ok()?,
                };
                let pending = PendingInbound {
                    slot: &self.deferred_recv,
                    ready: &self.deferred_recv_ready,
                    inbound: Some(inbound),
                };
                #[cfg(test)]
                {
                    let pause = { self.recv_deferred_pause.lock().take() };
                    if let Some(entered) = pause {
                        entered.notify_one();
                        std::future::pending::<()>().await;
                    }
                }
                let received = pending.take().into_received();
                drop(guard);
                if self.retain_received(&received) {
                    return Some(received);
                }
            }
        }

        /// Drain every currently available admitted message without blocking;
        /// empty if a `recv()` holds the receiver.
        #[must_use]
        pub fn drain_available(&self) -> Vec<ReceivedShuffle> {
            let mut out = Vec::new();
            {
                let slot = self.rx.lock();
                if let Some(rx) = slot.as_ref() {
                    if let Some(item) = self.take_deferred_recv() {
                        let received = item.into_received();
                        if self.retain_received(&received) {
                            out.push(received);
                        }
                    }
                    while let Ok(item) = rx.try_recv() {
                        let received = item.into_received();
                        if self.retain_received(&received) {
                            out.push(received);
                        }
                    }
                }
            }
            out
        }

        /// Drain the inbound queue into `staged`: bucket data by stage and stop at the first
        /// ordered control. The control must be consumed before later data can pass it.
        /// Once the shared holdover is full, leave the bounded receive queue intact so transport
        /// admission backpressures peers.
        fn drain_inbound_into(&self, staged: &mut FxHashMap<String, Vec<ReceivedBatch>>) -> bool {
            if self.holdover.has_staged_barriers() || self.holdover.has_staged_frontiers() {
                return false;
            }
            let slot = self.rx.lock();
            let Some(rx) = slot.as_ref() else {
                return false;
            };
            while self.holdover.try_reserve_item() {
                let inbound = if let Some(deferred) = self.take_deferred_recv() {
                    deferred
                } else {
                    let Ok(inbound) = rx.try_recv() else {
                        self.holdover.release_items(1);
                        return true;
                    };
                    inbound
                };
                let received = inbound.into_received();
                if !self.retain_received(&received) {
                    self.holdover.release_items(1);
                    continue;
                }
                let ReceivedShuffle {
                    peer,
                    message,
                    reservation,
                    sender_incarnation,
                    receiver_incarnation,
                    stream_id,
                    assignment_version,
                    assignment_digest,
                    recovery_gen,
                    checkpoint_sequence,
                } = received;
                match message {
                    ShuffleMessage::Data {
                        stage: s,
                        routed_vnodes,
                        batch,
                    } => {
                        staged.entry(s).or_default().push(ReceivedBatch {
                            batch,
                            routed_vnodes,
                            reservation,
                            peer,
                            sender_incarnation,
                            receiver_incarnation,
                            stream_id,
                            assignment_version,
                            recovery_gen,
                            checkpoint_sequence,
                        });
                    }
                    ShuffleMessage::Frontier {
                        stage,
                        watermark,
                        idle,
                    } => {
                        let frontier = ReceivedShuffle {
                            peer,
                            message: ShuffleMessage::Frontier {
                                stage: stage.clone(),
                                watermark,
                                idle,
                            },
                            reservation,
                            sender_incarnation,
                            receiver_incarnation,
                            stream_id,
                            assignment_version,
                            assignment_digest,
                            recovery_gen,
                            checkpoint_sequence,
                        };
                        let preceding = take_frontier_prefix(staged, &stage, &frontier);
                        let cut = ReceivedFrontierCut {
                            preceding,
                            frontier,
                        };
                        let item_count = cut.item_count();
                        if self.stage_frontier_if_current(cut) {
                            return false;
                        }
                        self.holdover.release_items(item_count);
                    }
                    ShuffleMessage::Barrier(b) => {
                        let barrier = ReceivedShuffle {
                            peer,
                            message: ShuffleMessage::Barrier(b),
                            reservation,
                            sender_incarnation,
                            receiver_incarnation,
                            stream_id,
                            assignment_version,
                            assignment_digest,
                            recovery_gen,
                            checkpoint_sequence,
                        };
                        match self.holdover.stage_barrier(barrier) {
                            Ok(true) => return false,
                            Err(_) => self
                                .delivery
                                .note_loss(peer, 1, "barrier-holdover-protocol"),
                            Ok(false) => {}
                        }
                        self.holdover.release_items(1);
                    }
                }
            }
            false
        }

        /// Non-blocking drain of checkpointed operator batches for `stage`.
        #[must_use]
        pub fn drain_checkpointed_data_for(&self, stage: &str) -> Vec<ReceivedBatch> {
            let mut staged = self.holdover.staged.lock();
            let _ = self.drain_inbound_into(&mut staged);
            let mut batches = staged.remove(stage).unwrap_or_default();
            self.holdover.release_items(batches.len());
            batches.retain(|batch| self.retain_batch(batch));
            batches
        }

        /// Take the barriers stashed by [`Self::drain_checkpointed_data_for`].
        #[must_use]
        pub fn drain_staged_barriers(&self) -> Vec<ReceivedShuffle> {
            let mut barriers = self.holdover.take_staged_barriers();
            self.holdover.release_items(barriers.len());
            barriers.retain(|barrier| self.retain_received(barrier));
            barriers
        }

        /// Take the frontier that stopped the checkpointed data drainer.
        #[must_use]
        pub fn drain_staged_frontiers(&self) -> Vec<ReceivedFrontierCut> {
            let mut frontiers = self.holdover.take_staged_frontiers();
            let item_count = frontiers.iter().map(ReceivedFrontierCut::item_count).sum();
            self.holdover.release_items(item_count);
            frontiers.retain(|cut| self.retain_received(cut.frontier()));
            frontiers
        }

        /// Whether an in-band barrier currently blocks normal holdover draining.
        #[must_use]
        pub fn has_staged_checkpoint_barriers(&self) -> bool {
            self.holdover.has_staged_barriers()
        }

        /// Move available inbound frames into the bounded holdover through the first barrier.
        /// Data remains keyed for its owning stage.
        #[must_use]
        pub fn stage_checkpointed_inbound(&self) -> bool {
            if self.holdover.has_staged_barriers() {
                return true;
            }
            if self.holdover.has_staged_frontiers() {
                return false;
            }
            let arrived = self.barrier_arrivals.load(Ordering::Acquire);
            if arrived == self.barrier_reconciled.load(Ordering::Acquire) {
                return false;
            }
            let mut staged = self.holdover.staged.lock();
            let exhausted = self.drain_inbound_into(&mut staged);
            let has_barrier = self.holdover.has_staged_barriers();
            if exhausted && !has_barrier {
                self.barrier_reconciled.store(arrived, Ordering::Release);
            }
            has_barrier
        }

        /// Retire barrier markers through a certified terminal checkpoint without dropping data.
        /// # Errors
        /// Rejects a noncanonical attempt and exact markers whose assignment digest differs from
        /// the durable terminal outcome.
        pub fn retire_checkpoint_barriers(
            &self,
            attempt: CheckpointAttempt,
            assignment_digest: [u8; 32],
        ) -> io::Result<()> {
            self.holdover
                .retire_checkpoint_attempt(attempt, assignment_digest)
        }

        /// Re-stash a peer barrier pulled while aligning a *different* checkpoint, so a
        /// lagging node still sees it when it reaches that checkpoint.
        pub fn stash_barrier(&self, barrier: ReceivedShuffle) {
            debug_assert!(matches!(barrier.message(), ShuffleMessage::Barrier(_)));
            if !self.retain_received(&barrier) {
                return;
            }
            if !self.holdover.try_reserve_item() {
                self.delivery
                    .note_loss(barrier.peer, 1, "barrier-holdover-capacity");
                return;
            }
            let peer = barrier.peer;
            match self.holdover.stage_barrier(barrier) {
                Ok(true) => {}
                Err(_) => {
                    self.holdover.release_items(1);
                    self.delivery
                        .note_loss(peer, 1, "barrier-holdover-protocol");
                }
                Ok(false) => self.holdover.release_items(1),
            }
        }

        /// Drain every staged checkpointed operator batch.
        #[must_use]
        pub fn drain_checkpointed_staged(&self) -> Vec<(String, ReceivedBatch)> {
            let mut staged = self.holdover.staged.lock();
            let _ = self.drain_inbound_into(&mut staged);
            self.take_checkpointed_staged(&mut staged)
        }

        /// Drain only the checkpointed batches already in the holdover. Unlike
        /// [`Self::drain_checkpointed_staged`], this does not consume the live receive queue, whose
        /// data/barrier order must remain visible to checkpoint alignment.
        ///
        /// # Errors
        /// Returns a typed cancellation while assignment consumption is suspended, a process
        /// lease error after this receiver loses execution authority, or `WouldBlock` when an
        /// ordered frontier cut must be consumed first. An error leaves the holdover untouched.
        pub fn drain_checkpointed_holdover(&self) -> io::Result<Vec<(String, ReceivedBatch)>> {
            let mut staged = self.holdover.staged.lock();
            self.process_lease.require_live_io()?;
            let Some(assignment) = self.consumption_scope() else {
                self.process_lease.require_live_io()?;
                return Err(scope_cancelled_io());
            };
            let Some(assignment) = assignment.as_ref() else {
                self.process_lease.require_live_io()?;
                return Err(scope_cancelled_io());
            };
            if self.assignment_version.load(Ordering::Acquire) == 0 {
                self.process_lease.require_live_io()?;
                return Err(scope_cancelled_io());
            }
            if self.holdover.has_staged_frontiers() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "shuffle frontier cut must be consumed before checkpoint holdover transfer",
                ));
            }

            let item_count = staged.values().map(Vec::len).sum();
            let drained = staged
                .drain()
                .flat_map(|(stage, batches)| {
                    batches.into_iter().filter_map(move |batch| {
                        let current = self.retain_queued_scope_for_assignment(
                            assignment,
                            batch.peer,
                            batch.sender_incarnation,
                            batch.receiver_incarnation,
                            batch.assignment_version,
                            batch.recovery_gen,
                        );
                        current.then(|| (stage.clone(), batch))
                    })
                })
                .collect();
            self.holdover.release_items(item_count);
            Ok(drained)
        }

        fn take_checkpointed_staged(
            &self,
            staged: &mut FxHashMap<String, Vec<ReceivedBatch>>,
        ) -> Vec<(String, ReceivedBatch)> {
            let item_count = staged.values().map(Vec::len).sum();
            let drained = staged
                .drain()
                .flat_map(|(stage, batches)| {
                    batches.into_iter().filter_map(move |batch| {
                        self.retain_batch(&batch).then(|| (stage.clone(), batch))
                    })
                })
                .collect();
            self.holdover.release_items(item_count);
            drained
        }

        /// Empty the per-stage holdover, returning every buffered `(stage, batch)`.
        #[must_use]
        pub fn drain_all_staged(&self) -> Vec<(String, ReceivedBatch)> {
            let mut staged = self.holdover.staged.lock();
            let frontiers = self.holdover.take_staged_frontiers();
            let item_count = staged.values().map(Vec::len).sum::<usize>()
                + frontiers
                    .iter()
                    .map(ReceivedFrontierCut::item_count)
                    .sum::<usize>();
            let mut drained: Vec<_> = staged
                .drain()
                .flat_map(|(stage, batches)| {
                    batches.into_iter().filter_map(move |batch| {
                        self.retain_batch(&batch).then(|| (stage.clone(), batch))
                    })
                })
                .collect();
            for cut in frontiers {
                let stage = cut.stage().to_owned();
                drained.extend(
                    cut.preceding
                        .into_iter()
                        .filter(|batch| self.retain_batch(batch))
                        .map(|batch| (stage.clone(), batch)),
                );
            }
            self.holdover.release_items(item_count);
            drained
        }
    }

    /// Returns the receiver to the slot on drop so a cancelled `recv()` future
    /// doesn't strand it; wakes the next parked waiter.
    struct RxReturnGuard<'a> {
        slot: &'a Mutex<Option<InboundRx>>,
        notify: &'a tokio::sync::Notify,
        rx: Option<InboundRx>,
    }

    impl Drop for RxReturnGuard<'_> {
        fn drop(&mut self) {
            if let Some(rx) = self.rx.take() {
                *self.slot.lock() = Some(rx);
                // notify_one stores a permit; notify_waiters can lose wakeups.
                self.notify.notify_one();
            }
        }
    }

    /// What we expect next from a peer, carried across reconnects. Assignment and recovery
    /// transitions reset both sender and receiver to zero under their respective scope locks.
    struct PeerSeq {
        fence: StreamFence,
        expected: u64,
    }

    #[derive(Debug, Clone, Copy)]
    struct DataReservation {
        fence: StreamFence,
        seq: u64,
        expected: u64,
    }

    #[derive(Debug, Clone, Copy)]
    struct BarrierReservation {
        fence: StreamFence,
        last_seq: u64,
        expected: u64,
    }

    /// Resolves a prepared logical data or frontier frame exactly once. An unresolved admission
    /// is loss unless its exact assignment/recovery lifetime was deliberately cancelled.
    struct DataAdmission<'a> {
        tracker: &'a DeliveryTracker,
        reservation: Option<DataReservation>,
        cancel: &'a CancellationToken,
    }

    impl<'a> DataAdmission<'a> {
        fn new(
            tracker: &'a DeliveryTracker,
            reservation: DataReservation,
            cancel: &'a CancellationToken,
        ) -> Self {
            Self {
                tracker,
                reservation: Some(reservation),
                cancel,
            }
        }

        fn commit_after_enqueue(mut self) -> Result<(), tonic::Status> {
            let reservation = self.reservation.take().expect("unresolved data admission");
            self.tracker.commit_data(reservation)
        }

        fn cancel(mut self) {
            let _ = self.reservation.take();
        }
    }

    impl Drop for DataAdmission<'_> {
        fn drop(&mut self) {
            if let Some(reservation) = self.reservation.take() {
                if !self.cancel.is_cancelled() {
                    self.tracker.abort_data(reservation);
                }
            }
        }
    }

    /// Tracks delivery per peer and counts frames that were sent but never arrived.
    #[derive(Default)]
    struct DeliveryTracker {
        peers: Mutex<FxHashMap<ShufflePeerId, PeerSeq>>,
        ingress: Mutex<FxHashMap<ShufflePeerId, Arc<tokio::sync::Mutex<()>>>>,
        delivery_loss_incidents: Arc<AtomicU64>,
        recovered_delivery_loss_incidents: Arc<AtomicU64>,
        pending_recovery: Mutex<Option<(u64, u64)>>,
        completed_recovery_gen: AtomicU64,
    }

    impl DeliveryTracker {
        fn reset_assignment(&self) {
            self.peers.lock().clear();
            self.ingress.lock().clear();
        }

        fn ingress_lock(
            &self,
            peer: ShufflePeerId,
        ) -> Result<Arc<tokio::sync::Mutex<()>>, tonic::Status> {
            let mut ingress = self.ingress.lock();
            if let Some(lock) = ingress.get(&peer) {
                return Ok(Arc::clone(lock));
            }
            if ingress.len() >= MAX_TRACKED_PEERS {
                return Err(tonic::Status::resource_exhausted(
                    "too many shuffle ingress peers",
                ));
            }
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            ingress.insert(peer, Arc::clone(&lock));
            Ok(lock)
        }

        /// Capture, but do not yet forgive, the cumulative loss incidents repaired by this
        /// rewind.
        fn prepare_recovery(&self, gen: u64) {
            self.ingress.lock().clear();
            let mut pending = self.pending_recovery.lock();
            if pending.is_some_and(|(pending_gen, _)| pending_gen >= gen) {
                return;
            }
            *pending = Some((gen, self.delivery_loss_incidents.load(Ordering::Acquire)));
        }

        /// Promote only the cutoff captured for this exact recovery generation.
        fn complete_recovery(&self, gen: u64) -> bool {
            let mut pending = self.pending_recovery.lock();
            if self.completed_recovery_gen.load(Ordering::Acquire) == gen {
                return true;
            }
            let Some((pending_gen, cutoff)) = *pending else {
                return false;
            };
            if pending_gen != gen {
                return false;
            }
            // `u64::MAX` is a permanent fail-closed poison: once the incident counter is
            // exhausted, recovery must never make a later incident indistinguishable from the
            // recovered floor.
            self.recovered_delivery_loss_incidents
                .fetch_max(cutoff.min(u64::MAX - 1), Ordering::AcqRel);
            self.completed_recovery_gen.store(gen, Ordering::Release);
            *pending = None;
            true
        }

        /// Reconnects from the same process retain continuity. Process replacement is admitted
        /// only after assignment or recovery advances and opens a fresh zero-based domain.
        fn observe_hello(&self, fence: StreamFence) -> Result<(), tonic::Status> {
            let mut peers = self.peers.lock();
            match peers.entry(fence.sender_node_id) {
                Entry::Vacant(entry) => {
                    entry.insert(PeerSeq { fence, expected: 0 });
                    Ok(())
                }
                Entry::Occupied(mut entry) => {
                    let state = entry.get_mut();
                    let same_process_assignment = state.fence.sender_incarnation
                        == fence.sender_incarnation
                        && state.fence.receiver_incarnation == fence.receiver_incarnation
                        && state.fence.assignment_version == fence.assignment_version
                        && state.fence.assignment_certificate_digest
                            == fence.assignment_certificate_digest;
                    let expected = if same_process_assignment
                        && state.fence.recovery_gen == fence.recovery_gen
                    {
                        state.expected
                    } else if fence.assignment_version > state.fence.assignment_version {
                        // Assignment publication starts a new delivery domain at sequence zero.
                        // The atomic scope check in `admit_stream` proves this is the receiver's
                        // current assignment; retaining the old map entry avoids a clear/add race.
                        0
                    } else if state.fence.assignment_version == fence.assignment_version
                        && fence.recovery_gen > state.fence.recovery_gen
                    {
                        // Sender and receiver both reset their scoped sequence before admitting
                        // this generation. Starting elsewhere would hide a missing sequence zero.
                        0
                    } else {
                        return Err(tonic::Status::failed_precondition(
                            "shuffle sender scope changed without assignment or recovery advance",
                        ));
                    };
                    *state = PeerSeq { fence, expected };
                    Ok(())
                }
            }
        }

        fn validate_stream(&self, fence: &StreamFence) -> Result<(), tonic::Status> {
            let peers = self.peers.lock();
            if peers
                .get(&fence.sender_node_id)
                .is_some_and(|state| state.fence == *fence)
            {
                Ok(())
            } else {
                drop(peers);
                self.note_loss(fence.sender_node_id, 1, "stale-stream");
                Err(tonic::Status::failed_precondition(
                    "shuffle stream identity was superseded",
                ))
            }
        }

        /// Whether an already-enqueued frame still belongs to the current sender process and
        /// receiver scope. Reconnect stream IDs are intentionally ignored: frames queued by the
        /// predecessor connection remain ordered and valid for the same process.
        fn matches_process_scope(
            &self,
            peer: ShufflePeerId,
            sender_incarnation: Uuid,
            receiver_incarnation: Uuid,
            assignment_version: u64,
            recovery_gen: u64,
        ) -> bool {
            self.peers.lock().get(&peer).is_some_and(|state| {
                state.fence.sender_incarnation == sender_incarnation
                    && state.fence.receiver_incarnation == receiver_incarnation
                    && state.fence.assignment_version == assignment_version
                    && state.fence.recovery_gen == recovery_gen
            })
        }

        fn reject_protocol(&self, peer: ShufflePeerId, reason: &str) -> tonic::Status {
            self.note_loss(peer, 1, "identity");
            tonic::Status::failed_precondition(reason.to_string())
        }

        fn note_loss(&self, peer: ShufflePeerId, missing: u64, at: &str) {
            let exhausted = self
                .delivery_loss_incidents
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |incidents| {
                    incidents.checked_add(1)
                })
                .is_err();
            tracing::error!(
                peer,
                missing,
                at,
                loss_counter_exhausted = exhausted,
                "shuffle frames lost in transit; fencing the epoch"
            );
        }

        /// Reserve a sequence without acknowledging delivery. `None` is an already committed
        /// duplicate. The caller must hold this peer's ingress lock through `commit_data`.
        fn prepare_data(
            &self,
            fence: &StreamFence,
            seq: u64,
        ) -> Result<Option<DataReservation>, tonic::Status> {
            if seq == u64::MAX {
                return Err(
                    self.reject_protocol(fence.sender_node_id, "shuffle sequence exhausted")
                );
            }
            let peers = self.peers.lock();
            let Some(state) = peers.get(&fence.sender_node_id) else {
                drop(peers);
                return Err(self.reject_protocol(
                    fence.sender_node_id,
                    "shuffle data arrived before its exact Hello",
                ));
            };
            if state.fence != *fence {
                drop(peers);
                return Err(self.reject_protocol(
                    fence.sender_node_id,
                    "shuffle data stream identity was superseded",
                ));
            }
            if seq < state.expected {
                return Ok(None);
            }
            Ok(Some(DataReservation {
                fence: *fence,
                seq,
                expected: state.expected,
            }))
        }

        /// Commit only after every decoded/sliced batch has entered the consumer queue.
        fn commit_data(&self, reservation: DataReservation) -> Result<(), tonic::Status> {
            let next = reservation.seq.checked_add(1).ok_or_else(|| {
                self.reject_protocol(
                    reservation.fence.sender_node_id,
                    "shuffle sequence exhausted",
                )
            })?;
            let mut peers = self.peers.lock();
            let Some(state) = peers.get_mut(&reservation.fence.sender_node_id) else {
                drop(peers);
                return Err(self.reject_protocol(
                    reservation.fence.sender_node_id,
                    "shuffle delivery state disappeared before commit",
                ));
            };
            if state.fence != reservation.fence || state.expected != reservation.expected {
                drop(peers);
                return Err(self.reject_protocol(
                    reservation.fence.sender_node_id,
                    "shuffle delivery scope changed before commit",
                ));
            }
            state.expected = next;
            let missing = reservation.seq - reservation.expected;
            drop(peers);
            if missing > 0 {
                self.note_loss(reservation.fence.sender_node_id, missing, "data");
            }
            Ok(())
        }

        /// Resolve an admission that did not enqueue every slice. Advancing past the failed
        /// logical frame prevents its successor from counting the same hole a second time.
        fn abort_data(&self, reservation: DataReservation) {
            let mut peers = self.peers.lock();
            let Some(state) = peers.get_mut(&reservation.fence.sender_node_id) else {
                return;
            };
            if state.fence != reservation.fence || state.expected != reservation.expected {
                return;
            }
            let Some(next) = reservation.seq.checked_add(1) else {
                drop(peers);
                self.note_loss(
                    reservation.fence.sender_node_id,
                    1,
                    "data-admission-sequence-exhausted",
                );
                return;
            };
            state.expected = next;
            let missing = reservation.seq - reservation.expected + 1;
            drop(peers);
            self.note_loss(reservation.fence.sender_node_id, missing, "data-admission");
        }

        /// Validate a barrier without advancing delivery state. The caller commits immediately
        /// before queue publication so its loss fence is visible with the barrier.
        fn prepare_barrier(
            &self,
            fence: &StreamFence,
            last_seq: u64,
        ) -> Result<BarrierReservation, tonic::Status> {
            let peers = self.peers.lock();
            let Some(state) = peers.get(&fence.sender_node_id) else {
                drop(peers);
                return Err(self.reject_protocol(
                    fence.sender_node_id,
                    "shuffle barrier arrived before its exact Hello",
                ));
            };
            if state.fence != *fence {
                drop(peers);
                return Err(self.reject_protocol(
                    fence.sender_node_id,
                    "shuffle barrier stream identity was superseded",
                ));
            }
            if last_seq < state.expected {
                drop(peers);
                return Err(self.reject_protocol(
                    fence.sender_node_id,
                    "shuffle barrier high-water moved backwards",
                ));
            }
            Ok(BarrierReservation {
                fence: *fence,
                last_seq,
                expected: state.expected,
            })
        }

        fn commit_barrier(&self, reservation: BarrierReservation) -> Result<(), tonic::Status> {
            let mut peers = self.peers.lock();
            let Some(state) = peers.get_mut(&reservation.fence.sender_node_id) else {
                drop(peers);
                return Err(self.reject_protocol(
                    reservation.fence.sender_node_id,
                    "shuffle barrier state disappeared before commit",
                ));
            };
            if state.fence != reservation.fence || state.expected != reservation.expected {
                drop(peers);
                return Err(self.reject_protocol(
                    reservation.fence.sender_node_id,
                    "shuffle barrier scope changed before commit",
                ));
            }
            state.expected = reservation.last_seq;
            let missing = reservation.last_seq - reservation.expected;
            drop(peers);
            if missing > 0 {
                self.note_loss(reservation.fence.sender_node_id, missing, "barrier");
            }
            Ok(())
        }
    }

    /// `ShuffleTransport` service: the producer end of the inbound queue shared by
    /// every peer stream.
    struct ShuffleService {
        local_id: ShufflePeerId,
        receiver_incarnation: Uuid,
        assignment: Arc<RwLock<Option<Arc<InstalledAssignment>>>>,
        assignment_version: Arc<AtomicU64>,
        scope_cancel: Arc<RwLock<CancellationToken>>,
        process_lease: Arc<ProcessLeaseGate>,
        pending_handshakes: Arc<PendingHandshakes>,
        tx: InboundTx,
        recovery_gen: Arc<AtomicU64>,
        delivery: Arc<DeliveryTracker>,
        barrier_arrivals: Arc<AtomicU64>,
        holdover: Arc<Holdover>,
        inbound_budget: Arc<InboundBudget>,
        active_streams: Arc<Semaphore>,
        active_stream_registry: Arc<ActiveStreamRegistry>,
    }

    fn active_receiver_scope(
        assignment: &RwLock<Option<Arc<InstalledAssignment>>>,
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        scope_cancel: &RwLock<CancellationToken>,
        process_lease: &ProcessLeaseGate,
    ) -> Result<ScopeLease, tonic::Status> {
        let assignment = assignment.read();
        let installed = assignment.as_ref().ok_or_else(|| {
            tonic::Status::failed_precondition("shuffle assignment certificate is not installed")
        })?;
        let version = assignment_version.load(Ordering::Acquire);
        let recovery_gen = recovery_gen.load(Ordering::Acquire);
        let cancel = scope_cancel.read().clone();
        process_lease.require_live_status()?;
        if version == 0 || version != installed.fence.assignment_version || cancel.is_cancelled() {
            return Err(scope_cancelled_status());
        }
        Ok(ScopeLease {
            assignment: Arc::clone(installed),
            recovery_gen,
            cancel,
        })
    }

    #[tonic::async_trait]
    impl ShuffleTransport for ShuffleService {
        async fn handshake(
            &self,
            request: Request<HandshakeRequest>,
        ) -> Result<tonic::Response<HandshakeResponse>, tonic::Status> {
            let request = request.into_inner();
            if request.sender_node_id == 0
                || request.sender_node_id == self.local_id
                || request.assignment_version == 0
            {
                return Err(tonic::Status::failed_precondition(
                    "shuffle peers do not share an established assignment scope",
                ));
            }
            let sender_incarnation = parse_uuid(&request.sender_incarnation, "sender incarnation")?;
            let stream_id = parse_uuid(&request.stream_id, "stream id")?;
            let requested_digest = parse_certificate_digest(
                &request.assignment_certificate_digest,
                "assignment certificate digest",
            )?;
            let scope = active_receiver_scope(
                &self.assignment,
                &self.assignment_version,
                &self.recovery_gen,
                &self.scope_cancel,
                &self.process_lease,
            )?;
            if request.recovery_gen != scope.recovery_gen {
                return Err(tonic::Status::failed_precondition(
                    "shuffle peers do not share a recovery generation",
                ));
            }
            if request.assignment_version != scope.assignment.fence.assignment_version
                || requested_digest != scope.assignment.digest
                || !scope
                    .assignment
                    .certifies(request.sender_node_id, sender_incarnation)
            {
                return Err(tonic::Status::failed_precondition(
                    "shuffle sender is not certified by the installed assignment",
                ));
            }
            let fence = StreamFence {
                sender_node_id: request.sender_node_id,
                sender_incarnation,
                receiver_incarnation: self.receiver_incarnation,
                stream_id,
                assignment_version: scope.assignment.fence.assignment_version,
                assignment_certificate_digest: scope.assignment.digest,
                recovery_gen: scope.recovery_gen,
            };
            let now = std::time::Instant::now();
            let mut pending = self.pending_handshakes.0.lock();
            pending.retain(|_, handshake| {
                now.saturating_duration_since(handshake.issued_at) < HANDSHAKE_TOKEN_TTL
            });
            if pending.len() >= MAX_PENDING_HANDSHAKES {
                return Err(tonic::Status::resource_exhausted(
                    "too many unconsumed shuffle handshakes",
                ));
            }
            pending.insert(
                request.sender_node_id,
                PendingHandshake {
                    fence,
                    issued_at: now,
                },
            );
            drop(pending);
            if scope.cancel.is_cancelled() || self.process_lease.require_live_status().is_err() {
                self.pending_handshakes
                    .0
                    .lock()
                    .remove(&request.sender_node_id);
                return if scope.cancel.is_cancelled() {
                    Err(scope_cancelled_status())
                } else {
                    Err(process_lease_expired_status())
                };
            }
            Ok(tonic::Response::new(HandshakeResponse {
                receiver_node_id: self.local_id,
                receiver_incarnation: self.receiver_incarnation.as_bytes().to_vec(),
                sender_incarnation: sender_incarnation.as_bytes().to_vec(),
                stream_id: stream_id.as_bytes().to_vec(),
                assignment_version: scope.assignment.fence.assignment_version,
                recovery_gen: scope.recovery_gen,
                assignment_certificate_digest: scope.assignment.digest.to_vec(),
            }))
        }

        async fn shuffle(
            &self,
            request: Request<tonic::Streaming<ShuffleFrame>>,
        ) -> Result<tonic::Response<ShuffleSummary>, tonic::Status> {
            let summary = run_stream(self, request.into_inner()).await?;
            Ok(tonic::Response::new(summary))
        }
    }

    fn consume_handshake_token(
        pending: &PendingHandshakes,
        fence: &StreamFence,
        now: std::time::Instant,
    ) -> bool {
        pending
            .0
            .lock()
            .remove(&fence.sender_node_id)
            .is_some_and(|token| {
                token.fence == *fence
                    && now.saturating_duration_since(token.issued_at) < HANDSHAKE_TOKEN_TTL
            })
    }

    /// Consume the short-lived one-time handshake token and validate the leading `Hello`.
    async fn admit_stream(
        stream: &mut tonic::Streaming<ShuffleFrame>,
        receiver_incarnation: Uuid,
        assignment: &RwLock<Option<Arc<InstalledAssignment>>>,
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        scope_cancel: &RwLock<CancellationToken>,
        process_lease: &ProcessLeaseGate,
        pending_handshakes: &PendingHandshakes,
        delivery: &DeliveryTracker,
        active_streams: &Arc<Semaphore>,
        active_stream_registry: &Arc<ActiveStreamRegistry>,
    ) -> Result<
        (
            StreamFence,
            Arc<tokio::sync::Mutex<()>>,
            ScopeLease,
            ActiveStreamLease,
        ),
        tonic::Status,
    > {
        let scope = active_receiver_scope(
            assignment,
            assignment_version,
            recovery_gen,
            scope_cancel,
            process_lease,
        )?;
        let first = tokio::select! {
            biased;
            () = process_lease.wait_until_lost() => {
                return Err(process_lease_expired_status());
            }
            () = scope.cancel.cancelled() => return Err(scope_cancelled_status()),
            first = stream.message() => first?,
        }
        .ok_or_else(|| tonic::Status::invalid_argument("shuffle stream closed before Hello"))?;
        let Some(shuffle_frame::Kind::Hello(hello)) = first.kind else {
            return Err(tonic::Status::invalid_argument(
                "first shuffle frame must be Hello",
            ));
        };
        let fence = fence_from_hello(&hello)?;
        if fence.recovery_gen != scope.recovery_gen {
            return Err(tonic::Status::failed_precondition(
                "shuffle Hello targets a stale recovery generation",
            ));
        }
        if fence.receiver_incarnation != receiver_incarnation {
            return Err(tonic::Status::failed_precondition(
                "shuffle stream targets a stale receiver process",
            ));
        }
        if !scope.matches_fence(&fence) || !scope.assignment.matches_stream_sender(&fence) {
            return Err(tonic::Status::failed_precondition(
                "shuffle stream targets a stale or uncertified assignment",
            ));
        }
        if !consume_handshake_token(pending_handshakes, &fence, std::time::Instant::now()) {
            return Err(tonic::Status::failed_precondition(
                "shuffle stream did not consume its exact unexpired handshake",
            ));
        }
        let ingress = delivery.ingress_lock(fence.sender_node_id)?;
        let ingress_guard = tokio::select! {
            biased;
            () = process_lease.wait_until_lost() => {
                return Err(process_lease_expired_status());
            }
            () = scope.cancel.cancelled() => return Err(scope_cancelled_status()),
            guard = ingress.lock() => guard,
        };
        delivery.observe_hello(fence)?;
        let mut active_stream = active_stream_registry.replace(&fence, &scope.cancel);
        tokio::select! {
            biased;
            () = process_lease.wait_until_lost() => {
                return Err(process_lease_expired_status());
            }
            () = scope.cancel.cancelled() => return Err(scope_cancelled_status()),
            result = active_stream.acquire_permit(active_streams) => result?,
        }
        process_lease.require_live_status()?;
        drop(ingress_guard);
        Ok((fence, ingress, scope, active_stream))
    }

    fn validate_active_stream_scope(
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        delivery: &DeliveryTracker,
        fence: &StreamFence,
        cancel: &CancellationToken,
        process_lease: &ProcessLeaseGate,
    ) -> Result<(), tonic::Status> {
        process_lease.require_live_status()?;
        if cancel.is_cancelled() {
            return Err(scope_cancelled_status());
        }
        if recovery_gen.load(Ordering::Acquire) != fence.recovery_gen {
            return Err(tonic::Status::failed_precondition(
                "shuffle recovery generation changed while admitting a frame",
            ));
        }
        if assignment_version.load(Ordering::Acquire) != fence.assignment_version {
            return Err(tonic::Status::failed_precondition(
                "shuffle assignment changed while admitting a frame",
            ));
        }
        delivery.validate_stream(fence)
    }

    fn reject_stream_protocol(
        delivery: &DeliveryTracker,
        fence: &StreamFence,
        reason: &str,
    ) -> tonic::Status {
        delivery.reject_protocol(fence.sender_node_id, reason)
    }

    /// Publish preceding ordered-frame completeness before making the barrier observable. A failed
    /// barrier enqueue cannot seal a checkpoint, while delaying this commit until after enqueue
    /// would let the consumer observe the barrier before its loss fence.
    async fn publish_barrier(
        tx: &InboundTx,
        barrier_arrivals: &AtomicU64,
        holdover: &Holdover,
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        delivery: &DeliveryTracker,
        fence: StreamFence,
        barrier: CheckpointBarrier,
        assignment_digest: [u8; 32],
        last_seq: u64,
        cancel: &CancellationToken,
        process_lease: &ProcessLeaseGate,
    ) -> Result<bool, tonic::Status> {
        validate_active_stream_scope(
            assignment_version,
            recovery_gen,
            delivery,
            &fence,
            cancel,
            process_lease,
        )?;
        let retired = holdover
            .is_retired_checkpoint_barrier(barrier, assignment_digest)
            .map_err(|error| reject_stream_protocol(delivery, &fence, &error.to_string()))?;
        let reservation = delivery.prepare_barrier(&fence, last_seq)?;
        validate_active_stream_scope(
            assignment_version,
            recovery_gen,
            delivery,
            &fence,
            cancel,
            process_lease,
        )?;
        delivery.commit_barrier(reservation)?;
        if retired {
            return Ok(true);
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(scope_cancelled_status()),
            result = tx.send(Inbound {
                peer: fence.sender_node_id,
                msg: ShuffleMessage::Barrier(barrier),
                budget: None,
                fence,
                assignment_digest: Some(assignment_digest),
                checkpoint_sequence: last_seq,
            }) => {
                let enqueued = result.is_ok();
                if enqueued {
                    barrier_arrivals.fetch_add(1, Ordering::Release);
                }
                Ok(enqueued)
            },
        }
    }

    /// Forward decoded frames onto the inbound queue and summarize on half-close.
    async fn run_stream(
        service: &ShuffleService,
        mut stream: tonic::Streaming<ShuffleFrame>,
    ) -> Result<ShuffleSummary, tonic::Status> {
        let ShuffleService {
            receiver_incarnation,
            assignment,
            assignment_version,
            scope_cancel,
            process_lease,
            pending_handshakes,
            tx,
            barrier_arrivals,
            holdover,
            recovery_gen,
            delivery,
            inbound_budget,
            active_streams,
            active_stream_registry,
            ..
        } = service;
        let (fence, ingress, scope, active_stream) = admit_stream(
            &mut stream,
            *receiver_incarnation,
            assignment,
            assignment_version,
            recovery_gen,
            scope_cancel,
            process_lease,
            pending_handshakes,
            delivery,
            active_streams,
            active_stream_registry,
        )
        .await?;
        let stream_cancel = &active_stream.cancel;
        let peer = fence.sender_node_id;

        let mut assembly = None;
        let mut frames_received = 0u64;
        loop {
            let frame = tokio::select! {
                biased;
                () = stream_cancel.cancelled() => return Err(scope_cancelled_status()),
                frame = stream.message() => frame?,
            };
            let Some(frame) = frame else {
                break;
            };
            process_lease.require_live_status()?;
            let Some(kind) = frame.kind else {
                return Err(reject_stream_protocol(
                    delivery,
                    &fence,
                    "empty shuffle frame",
                ));
            };
            if assignment_version.load(Ordering::Acquire) != fence.assignment_version {
                return Err(tonic::Status::failed_precondition(
                    "shuffle assignment changed while the stream was active",
                ));
            }
            if recovery_gen.load(Ordering::Acquire) != fence.recovery_gen {
                return Err(tonic::Status::failed_precondition(
                    "shuffle recovery generation changed while the stream was active",
                ));
            }
            match kind {
                shuffle_frame::Kind::Hello(_) => {
                    let _ingress_guard = tokio::select! {
                        biased;
                        () = stream_cancel.cancelled() => return Err(scope_cancelled_status()),
                        guard = ingress.lock() => guard,
                    };
                    return Err(reject_stream_protocol(
                        delivery,
                        &fence,
                        "shuffle Hello is valid only as the leading stream frame",
                    ));
                }
                shuffle_frame::Kind::Barrier(b) => {
                    frames_received += 1;
                    if assembly.is_some() {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            "shuffle barrier arrived before its preceding batch completed",
                        ));
                    }
                    let barrier = CheckpointBarrier {
                        checkpoint_id: b.checkpoint_id,
                        epoch: b.epoch,
                        flags: b.flags,
                    };
                    if !barrier.is_canonical() {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            NONCANONICAL_BARRIER,
                        ));
                    }
                    let assignment_digest: [u8; 32] =
                        b.assignment_digest.as_slice().try_into().map_err(|_| {
                            reject_stream_protocol(
                                delivery,
                                &fence,
                                "shuffle barrier assignment digest is not SHA-256 sized",
                            )
                        })?;
                    if b.assignment_version != fence.assignment_version
                        || b.recovery_gen != fence.recovery_gen
                        || assignment_digest != fence.assignment_certificate_digest
                    {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            "shuffle barrier differs from its stream assignment or recovery scope",
                        ));
                    }
                    let _ingress_guard = tokio::select! {
                        biased;
                        () = stream_cancel.cancelled() => return Err(scope_cancelled_status()),
                        guard = ingress.lock() => guard,
                    };
                    if !publish_barrier(
                        tx,
                        barrier_arrivals,
                        holdover,
                        assignment_version,
                        recovery_gen,
                        delivery,
                        fence,
                        barrier,
                        assignment_digest,
                        b.last_seq,
                        stream_cancel,
                        process_lease,
                    )
                    .await?
                    {
                        break;
                    }
                }
                shuffle_frame::Kind::Frontier(v) => {
                    frames_received += 1;
                    if assembly.is_some() {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            "shuffle frontier arrived before its preceding batch completed",
                        ));
                    }
                    if v.recovery_gen != fence.recovery_gen {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            "shuffle frontier generation differs from its stream handshake",
                        ));
                    }
                    validate_frontier(&v.stage, v.watermark).map_err(|error| {
                        reject_stream_protocol(delivery, &fence, &error.to_string())
                    })?;
                    let _ingress_guard = tokio::select! {
                        biased;
                        () = stream_cancel.cancelled() => return Err(scope_cancelled_status()),
                        guard = ingress.lock() => guard,
                    };
                    validate_active_stream_scope(
                        assignment_version,
                        recovery_gen,
                        delivery,
                        &fence,
                        stream_cancel,
                        process_lease,
                    )?;
                    let Some(reservation) = delivery.prepare_data(&fence, v.seq)? else {
                        continue;
                    };
                    let mut reservation =
                        Some(DataAdmission::new(delivery, reservation, stream_cancel));
                    let forwarded = forward_frontier(
                        tx,
                        fence,
                        v.stage,
                        v.watermark,
                        v.idle,
                        v.seq,
                        stream_cancel,
                        process_lease,
                    )
                    .await;
                    match forwarded {
                        Ok(true) => {
                            if let Some(admission) = reservation.take() {
                                admission.commit_after_enqueue()?;
                            }
                        }
                        Ok(false) => break,
                        Err(status) => {
                            if status.code() == tonic::Code::Cancelled {
                                if let Some(admission) = reservation.take() {
                                    admission.cancel();
                                }
                            }
                            return Err(status);
                        }
                    }
                    validate_active_stream_scope(
                        assignment_version,
                        recovery_gen,
                        delivery,
                        &fence,
                        stream_cancel,
                        process_lease,
                    )?;
                }
                shuffle_frame::Kind::Data(v) => {
                    frames_received += 1;
                    if v.recovery_gen != fence.recovery_gen {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            "shuffle data generation differs from its stream handshake",
                        ));
                    }
                    let total_payload_bytes = validate_fragment(&v)
                        .map_err(|error| reject_stream_protocol(delivery, &fence, &error))?;
                    if (v.fragment_index == 0 && assembly.is_some())
                        || (v.fragment_index != 0 && assembly.is_none())
                    {
                        return Err(reject_stream_protocol(
                            delivery,
                            &fence,
                            "shuffle fragments interleaved or started without fragment zero",
                        ));
                    }
                    let budget = if v.fragment_index == 0 {
                        Some(
                            inbound_budget
                                .reserve_frame(peer, total_payload_bytes, stream_cancel)
                                .await?,
                        )
                    } else {
                        None
                    };
                    process_lease.require_live_status()?;
                    if assignment_version.load(Ordering::Acquire) != fence.assignment_version
                        || recovery_gen.load(Ordering::Acquire) != fence.recovery_gen
                    {
                        return Err(tonic::Status::failed_precondition(
                            "shuffle scope changed while the frame awaited memory admission",
                        ));
                    }
                    let complete = match push_fragment(&mut assembly, &v, budget) {
                        Ok(Some(complete)) => complete,
                        Ok(None) => continue,
                        Err(error) => {
                            return Err(reject_stream_protocol(delivery, &fence, &error));
                        }
                    };
                    // Do not retain the final prost frame across blocking decode or queue
                    // publication; its payload can be another full wire fragment.
                    drop(v);
                    let CompleteData {
                        stage,
                        routed_vnodes,
                        seq,
                        arrow_ipc,
                        budget,
                    } = complete;
                    let (batch, budget) = decode_ipc_payload_isolated(arrow_ipc, budget, || {})
                        .await
                        .map_err(|error| {
                            reject_stream_protocol(
                                delivery,
                                &fence,
                                &format!("invalid shuffle IPC: {error}"),
                            )
                        })?;
                    let decoded_bytes = InboundBudget::validate_decoded(std::slice::from_ref(
                        &batch,
                    ))
                    .map_err(|status| reject_stream_protocol(delivery, &fence, status.message()))?;
                    let _ingress_guard = tokio::select! {
                        biased;
                        () = stream_cancel.cancelled() => return Err(scope_cancelled_status()),
                        guard = ingress.lock() => guard,
                    };
                    validate_active_stream_scope(
                        assignment_version,
                        recovery_gen,
                        delivery,
                        &fence,
                        stream_cancel,
                        process_lease,
                    )?;
                    let Some(reservation) = delivery.prepare_data(&fence, seq)? else {
                        continue; // already committed duplicate
                    };
                    let mut reservation =
                        Some(DataAdmission::new(delivery, reservation, stream_cancel));
                    let forwarded = forward_routed_batch(
                        tx,
                        fence,
                        service.local_id,
                        &scope.assignment,
                        stage,
                        routed_vnodes,
                        batch,
                        budget,
                        decoded_bytes,
                        seq,
                        stream_cancel,
                        process_lease,
                    )
                    .await;
                    match forwarded {
                        Ok(true) => {
                            if let Some(admission) = reservation.take() {
                                admission.commit_after_enqueue()?;
                            }
                        }
                        Ok(false) => break,
                        Err(status) => {
                            if status.code() == tonic::Code::Cancelled {
                                if let Some(admission) = reservation.take() {
                                    admission.cancel();
                                }
                            }
                            return Err(status);
                        }
                    }
                    validate_active_stream_scope(
                        assignment_version,
                        recovery_gen,
                        delivery,
                        &fence,
                        stream_cancel,
                        process_lease,
                    )?;
                }
            }
        }
        if assembly.is_some() {
            return Err(reject_stream_protocol(
                delivery,
                &fence,
                "shuffle stream ended mid-fragment",
            ));
        }
        tracing::debug!(peer, frames_received, "shuffle inbound stream ended");
        Ok(ShuffleSummary { frames_received })
    }

    async fn forward_frontier(
        tx: &InboundTx,
        fence: StreamFence,
        stage: String,
        watermark: Option<i64>,
        idle: bool,
        checkpoint_sequence: u64,
        cancel: &CancellationToken,
        process_lease: &ProcessLeaseGate,
    ) -> Result<bool, tonic::Status> {
        process_lease.require_live_status()?;
        validate_frontier(&stage, watermark)
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(scope_cancelled_status()),
            result = tx.send(Inbound {
                peer: fence.sender_node_id,
                msg: ShuffleMessage::Frontier { stage, watermark, idle },
                budget: None,
                fence,
                assignment_digest: None,
                checkpoint_sequence,
            }) => Ok(result.is_ok()),
        }
    }

    /// Forward one logical batch after validating certified vnode ownership. `Ok(false)` when the
    /// queue has closed.
    async fn forward_routed_batch(
        tx: &InboundTx,
        fence: StreamFence,
        receiver_node_id: ShufflePeerId,
        assignment: &InstalledAssignment,
        stage: String,
        routed_vnodes: Vec<u32>,
        batch: RecordBatch,
        mut budget: InboundReservation,
        decoded_bytes: usize,
        checkpoint_sequence: u64,
        cancel: &CancellationToken,
        process_lease: &ProcessLeaseGate,
    ) -> Result<bool, tonic::Status> {
        process_lease.require_live_status()?;
        if routed_vnodes.is_empty() || !routed_vnodes.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(tonic::Status::invalid_argument(
                "shuffle route set is empty or non-canonical",
            ));
        }
        if let Some(vnode) = routed_vnodes
            .iter()
            .find(|vnode| !assignment.owns_vnode(receiver_node_id, **vnode))
        {
            return Err(tonic::Status::failed_precondition(format!(
                "shuffle vnode {vnode} is not owned by receiver {receiver_node_id}"
            )));
        }
        let routed_vnodes: Arc<[u32]> = routed_vnodes.into();
        let msg = ShuffleMessage::Data {
            stage,
            routed_vnodes,
            batch,
        };
        let ShuffleMessage::Data {
            stage,
            routed_vnodes,
            batch,
            ..
        } = &msg
        else {
            unreachable!("constructed data message")
        };
        let metadata_bytes = retained_batch_metadata_bytes(stage, routed_vnodes, batch)?;
        budget.retain_decoded(decoded_bytes, metadata_bytes)?;
        let budget = Arc::new(budget);
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(scope_cancelled_status()),
            result = tx.send(Inbound {
                peer: fence.sender_node_id,
                msg,
                budget: Some(budget),
                fence,
                assignment_digest: None,
                checkpoint_sequence,
            }) => Ok(result.is_ok()),
        }
    }

    #[cfg(test)]
    mod delivery_tests;

    #[cfg(test)]
    mod encode_tests;

    #[cfg(test)]
    mod fragment_tests;
}

#[cfg(feature = "cluster")]
pub use grpc::{ShuffleReceiver, ShuffleSender};

#[cfg(not(feature = "cluster"))]
mod shim {
    use std::io;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use crossfire::{mpsc, AsyncRx, MAsyncTx};
    use parking_lot::Mutex;
    use rustc_hash::FxHashMap;

    use super::{
        take_frontier_prefix, validate_checkpoint_barrier, validate_frontier, Holdover,
        ReceivedBatch, ReceivedFrontierCut, ReceivedShuffle, ShuffleMessage, ShufflePeerId,
        SHUFFLE_RECV_QUEUE,
    };
    use crate::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};

    type InboundRx = AsyncRx<mpsc::Array<ReceivedShuffle>>;
    type InboundTx = MAsyncTx<mpsc::Array<ReceivedShuffle>>;

    /// Outbound shuffle handle; without the cluster feature there is no transport.
    pub struct ShuffleSender {
        local_id: ShufflePeerId,
    }

    impl std::fmt::Debug for ShuffleSender {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ShuffleSender")
                .field("local_id", &self.local_id)
                .finish_non_exhaustive()
        }
    }

    impl ShuffleSender {
        /// Empty sender (no peer fabric without the cluster feature).
        #[must_use]
        pub fn new(local_id: ShufflePeerId, _incarnation: uuid::Uuid) -> Self {
            Self { local_id }
        }

        /// Node id retained by this networking-free handle.
        #[must_use]
        pub const fn local_id(&self) -> ShufflePeerId {
            self.local_id
        }

        /// No peer fabric without the cluster feature; matches the cluster build's API.
        pub fn set_recovery_gen(&self, _gen: u64) {}

        /// No peer fabric without the cluster feature; retain the clustered installation API.
        ///
        /// # Errors
        /// This networking-free implementation never errors.
        pub fn install_assignment_fence(
            &self,
            _fence: &CheckpointAssignmentFence,
            _owners: &[ShufflePeerId],
        ) -> io::Result<bool> {
            Ok(false)
        }

        /// No cluster fabric exists, so there is no assignment authority to invalidate.
        pub fn invalidate_assignment_fence(&self) {}

        /// No cluster fabric exists, so there is no assignment authority to suspend.
        pub fn suspend_assignment_fence(&self) {}

        /// No cluster fabric exists, so no assignment scope is active.
        #[must_use]
        pub const fn assignment_version(&self) -> u64 {
            0
        }

        /// No cluster fabric exists, so no assignment certificate is active.
        #[must_use]
        pub const fn active_assignment_digest(&self) -> Option<[u8; 32]> {
            None
        }

        /// No cluster fabric exists, so no recovery scope is active.
        #[must_use]
        pub const fn recovery_gen(&self) -> u64 {
            0
        }

        /// Preserve the cluster build's API; no address can create a transport in this build.
        pub fn register_peer(&self, _peer: ShufflePeerId, _addr: SocketAddr) {}

        /// # Errors
        /// Always errors because the no-cluster build has no shuffle transport.
        pub fn send_to(
            &self,
            peer: ShufflePeerId,
            msg: &ShuffleMessage,
        ) -> std::future::Ready<io::Result<()>> {
            let validation = match msg {
                ShuffleMessage::Barrier(barrier) => validate_checkpoint_barrier(*barrier),
                ShuffleMessage::Frontier {
                    stage, watermark, ..
                } => validate_frontier(stage, *watermark),
                ShuffleMessage::Data { .. } => Ok(()),
            };
            if let Err(error) = validation {
                return std::future::ready(Err(error));
            }
            std::future::ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "node {} cannot send shuffle to peer {peer}: cluster transport is disabled",
                    self.local_id
                ),
            )))
        }

        /// No cluster fabric exists, so only a local-only assignment has a complete mesh.
        ///
        /// # Errors
        /// Returns an unsupported error when the assignment names a remote participant.
        pub fn establish_assignment_mesh(
            &self,
            assignment_fence: &CheckpointAssignmentFence,
        ) -> std::future::Ready<io::Result<()>> {
            let result = if assignment_fence
                .participants
                .iter()
                .all(|participant| participant.node_id == self.local_id)
            {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "cluster shuffle mesh is disabled",
                ))
            };
            std::future::ready(result)
        }

        /// Fan out to every required peer, reporting any missing peer after all
        /// sends have been attempted.
        ///
        /// # Errors
        /// Returns the first peer error after attempting the full fan-out.
        pub async fn fan_out_barrier(
            &self,
            peers: &[ShufflePeerId],
            barrier: CheckpointBarrier,
            _assignment_fence: &CheckpointAssignmentFence,
        ) -> io::Result<()> {
            validate_checkpoint_barrier(barrier)?;
            let msg = ShuffleMessage::Barrier(barrier);
            let mut first_err = None;
            let results =
                futures::future::join_all(peers.iter().map(|&peer| self.send_to(peer, &msg))).await;
            for result in results {
                first_err = first_err.or(result.err());
            }
            match first_err {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }

        /// No cluster fabric exists in this build; preserve the guarded-send API for callers.
        ///
        /// # Errors
        /// Returns the same missing-peer error as [`Self::send_to`].
        pub async fn send_to_for_assignment(
            &self,
            peer: ShufflePeerId,
            _expected_assignment_version: u64,
            msg: &ShuffleMessage,
        ) -> io::Result<()> {
            self.send_to(peer, msg).await
        }
    }

    /// Inbound shuffle handle: the bounded queue + holdover so the drain/stage API
    /// works locally without a network. Parked behind a `Mutex<Option<_>>` for the
    /// same `Sync` reason as the gRPC build.
    pub struct ShuffleReceiver {
        local_id: ShufflePeerId,
        local_addr: SocketAddr,
        // Keeps the local queue open so `recv` remains cancellation-safe even though this build
        // has no producer fabric.
        _tx: InboundTx,
        rx: Mutex<Option<InboundRx>>,
        rx_returned: Arc<tokio::sync::Notify>,
        holdover: Arc<Holdover>,
    }

    impl std::fmt::Debug for ShuffleReceiver {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ShuffleReceiver")
                .field("local_id", &self.local_id)
                .field("local_addr", &self.local_addr)
                .finish_non_exhaustive()
        }
    }

    impl ShuffleReceiver {
        /// No peer fabric without the cluster feature; matches the cluster build's API.
        pub fn set_recovery_gen(&self, _gen: u64) {}

        /// No peer fabric without the cluster feature; retain the clustered installation API.
        ///
        /// # Errors
        /// This networking-free implementation never errors.
        pub fn install_assignment_fence(
            &self,
            _fence: &CheckpointAssignmentFence,
            _owners: &[ShufflePeerId],
        ) -> io::Result<bool> {
            Ok(false)
        }

        /// No cluster fabric exists, so there is no assignment authority to invalidate.
        pub fn invalidate_assignment_fence(&self) {}

        /// No cluster fabric exists, so there is no assignment authority to suspend.
        pub fn suspend_assignment_fence(&self) {}

        /// No cluster fabric exists, so no assignment scope is active.
        #[must_use]
        pub const fn assignment_version(&self) -> u64 {
            0
        }

        /// No cluster fabric exists, so no assignment certificate is active.
        #[must_use]
        pub const fn active_assignment_digest(&self) -> Option<[u8; 32]> {
            None
        }

        /// No cluster fabric exists, so no recovery scope is active.
        #[must_use]
        pub const fn recovery_gen(&self) -> u64 {
            0
        }

        /// No peer fabric without the cluster feature; nothing can be lost in transit.
        #[must_use]
        pub fn delivery_loss_incidents(&self) -> Arc<std::sync::atomic::AtomicU64> {
            Arc::new(std::sync::atomic::AtomicU64::new(0))
        }

        /// No peer fabric exists, so no delivery-loss incident has required recovery.
        #[must_use]
        pub fn recovered_delivery_loss_incidents(&self) -> Arc<std::sync::atomic::AtomicU64> {
            Arc::new(std::sync::atomic::AtomicU64::new(0))
        }

        /// No peer fabric exists, so no delivery loss can require recovery.
        #[must_use]
        pub const fn has_unrecovered_delivery_loss(&self) -> bool {
            false
        }

        /// # Errors
        /// Returns `io::Error` on bind failure.
        pub async fn bind(
            local_id: ShufflePeerId,
            addr: SocketAddr,
            _incarnation: uuid::Uuid,
        ) -> io::Result<Self> {
            // Resolve the address (incl. ephemeral port) by binding momentarily.
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            drop(listener);
            let (tx, rx) = mpsc::bounded_async::<ReceivedShuffle>(SHUFFLE_RECV_QUEUE);
            Ok(Self {
                local_id,
                local_addr,
                _tx: tx,
                rx: Mutex::new(Some(rx)),
                rx_returned: Arc::new(tokio::sync::Notify::new()),
                holdover: Arc::new(Holdover::default()),
            })
        }

        /// Local socket address resolved at bind time.
        #[must_use]
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        /// Await the next admitted message. `None` once all senders drop.
        pub async fn recv(&self) -> Option<ReceivedShuffle> {
            loop {
                let taken = { self.rx.lock().take() };
                let Some(rx) = taken else {
                    self.rx_returned.notified().await;
                    continue;
                };
                let mut guard = RxReturnGuard {
                    slot: &self.rx,
                    notify: &self.rx_returned,
                    rx: Some(rx),
                };
                return guard.rx.as_mut()?.recv().await.ok();
            }
        }

        /// Drain every currently available admitted message without blocking.
        #[must_use]
        pub fn drain_available(&self) -> Vec<ReceivedShuffle> {
            let mut out = Vec::new();
            let slot = self.rx.lock();
            if let Some(rx) = slot.as_ref() {
                while let Ok(item) = rx.try_recv() {
                    out.push(item);
                }
            }
            out
        }

        fn drain_inbound_into(&self, staged: &mut FxHashMap<String, Vec<ReceivedBatch>>) {
            if self.holdover.has_staged_barriers() || self.holdover.has_staged_frontiers() {
                return;
            }
            let slot = self.rx.lock();
            if let Some(rx) = slot.as_ref() {
                while self.holdover.try_reserve_item() {
                    let Ok(received) = rx.try_recv() else {
                        self.holdover.release_items(1);
                        break;
                    };
                    let ReceivedShuffle {
                        peer,
                        message,
                        reservation,
                        sender_incarnation,
                        receiver_incarnation,
                        stream_id,
                        assignment_version,
                        assignment_digest: _,
                        recovery_gen,
                        checkpoint_sequence,
                    } = received;
                    match message {
                        ShuffleMessage::Data {
                            stage,
                            routed_vnodes,
                            batch,
                        } => {
                            staged.entry(stage).or_default().push(ReceivedBatch {
                                batch,
                                routed_vnodes,
                                reservation,
                                peer,
                                sender_incarnation,
                                receiver_incarnation,
                                stream_id,
                                assignment_version,
                                recovery_gen,
                                checkpoint_sequence,
                            });
                        }
                        ShuffleMessage::Frontier {
                            stage,
                            watermark,
                            idle,
                        } => {
                            let frontier = ReceivedShuffle {
                                peer,
                                message: ShuffleMessage::Frontier {
                                    stage: stage.clone(),
                                    watermark,
                                    idle,
                                },
                                reservation,
                                sender_incarnation,
                                receiver_incarnation,
                                stream_id,
                                assignment_version,
                                assignment_digest: None,
                                recovery_gen,
                                checkpoint_sequence,
                            };
                            let preceding = take_frontier_prefix(staged, &stage, &frontier);
                            self.holdover.stage_frontier(ReceivedFrontierCut {
                                preceding,
                                frontier,
                            });
                            break;
                        }
                        ShuffleMessage::Barrier(barrier) => {
                            let barrier = ReceivedShuffle {
                                peer,
                                message: ShuffleMessage::Barrier(barrier),
                                reservation,
                                sender_incarnation,
                                receiver_incarnation,
                                stream_id,
                                assignment_version,
                                assignment_digest: None,
                                recovery_gen,
                                checkpoint_sequence,
                            };
                            match self.holdover.stage_barrier(barrier) {
                                Ok(true) => break,
                                Ok(false) | Err(_) => self.holdover.release_items(1),
                            }
                        }
                    }
                }
            }
        }

        /// Non-blocking drain of checkpointed batches for `stage`.
        #[must_use]
        pub fn drain_checkpointed_data_for(&self, stage: &str) -> Vec<ReceivedBatch> {
            let mut staged = self.holdover.staged.lock();
            self.drain_inbound_into(&mut staged);
            let batches = staged.remove(stage).unwrap_or_default();
            self.holdover.release_items(batches.len());
            batches
        }

        /// Take the barriers stashed by [`Self::drain_checkpointed_data_for`].
        #[must_use]
        pub fn drain_staged_barriers(&self) -> Vec<ReceivedShuffle> {
            let barriers = self.holdover.take_staged_barriers();
            self.holdover.release_items(barriers.len());
            barriers
        }

        /// Take the frontier that stopped the checkpointed data drainer.
        #[must_use]
        pub fn drain_staged_frontiers(&self) -> Vec<ReceivedFrontierCut> {
            let frontiers = self.holdover.take_staged_frontiers();
            let item_count = frontiers.iter().map(ReceivedFrontierCut::item_count).sum();
            self.holdover.release_items(item_count);
            frontiers
        }

        /// Empty the per-stage holdover, returning every buffered `(stage, batch)`.
        #[must_use]
        pub fn drain_all_staged(&self) -> Vec<(String, ReceivedBatch)> {
            let mut staged = self.holdover.staged.lock();
            let frontiers = self.holdover.take_staged_frontiers();
            let item_count = staged.values().map(Vec::len).sum::<usize>()
                + frontiers
                    .iter()
                    .map(ReceivedFrontierCut::item_count)
                    .sum::<usize>();
            let mut drained: Vec<_> = staged
                .drain()
                .flat_map(|(stage, batches)| {
                    batches
                        .into_iter()
                        .map(move |staged| (stage.clone(), staged))
                })
                .collect();
            for cut in frontiers {
                let stage = cut.stage().to_owned();
                drained.extend(
                    cut.preceding
                        .into_iter()
                        .map(|batch| (stage.clone(), batch)),
                );
            }
            self.holdover.release_items(item_count);
            drained
        }
    }

    /// Returns the receiver to the slot on drop so a cancelled `recv()` future
    /// doesn't strand it; wakes the next parked waiter.
    struct RxReturnGuard<'a> {
        slot: &'a Mutex<Option<InboundRx>>,
        notify: &'a tokio::sync::Notify,
        rx: Option<InboundRx>,
    }

    impl Drop for RxReturnGuard<'_> {
        fn drop(&mut self) {
            if let Some(rx) = self.rx.take() {
                *self.slot.lock() = Some(rx);
                self.notify.notify_one();
            }
        }
    }
}

#[cfg(not(feature = "cluster"))]
pub use shim::{ShuffleReceiver, ShuffleSender};

#[cfg(all(test, not(feature = "cluster")))]
mod shim_tests;

#[cfg(all(test, feature = "cluster"))]
mod tests;
