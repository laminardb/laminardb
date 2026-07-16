//! Cross-node shuffle transport over Tonic gRPC client-streaming.
//!
//! A leading `Hello` binds each stream to certified process incarnations and an
//! assignment. Per-peer and per-node byte admission propagates backpressure to
//! the HTTP/2 stream.

use super::message::ShuffleMessage;

/// Secondary queue and holdover item bound; byte semaphores are the primary
/// admission control in cluster mode.
const SHUFFLE_RECV_QUEUE: usize = 256;

/// Peer identifier on the wire; matches `cluster::discovery::NodeId`'s inner type.
pub type ShufflePeerId = u64;

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

/// Inbound staging shared by both builds: frames for another stage are bucketed
/// for that stage's drainer, and mid-cycle barriers are stashed for the aligning
/// checkpoint under the same item bound as the receive queue.
struct Holdover {
    staged: parking_lot::Mutex<rustc_hash::FxHashMap<String, Vec<ReceivedBatch>>>,
    staged_barriers: parking_lot::Mutex<Vec<ReceivedShuffle>>,
    /// Shared across data and barriers so repeatedly draining the bounded receive
    /// queue cannot turn it into an unbounded secondary queue.
    items: std::sync::atomic::AtomicUsize,
    capacity: usize,
}

impl Holdover {
    fn new(capacity: usize) -> Self {
        Self {
            staged: parking_lot::Mutex::default(),
            staged_barriers: parking_lot::Mutex::default(),
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

    /// Split the decoded batch from its admission so a retaining consumer can
    /// keep the charge for exactly as long as shallow Arrow views remain live.
    #[must_use]
    pub fn into_parts(self) -> (arrow_array::RecordBatch, ShuffleBatchAdmission) {
        (self.batch, ShuffleBatchAdmission(self.reservation))
    }
}

impl std::fmt::Debug for ReceivedBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReceivedBatch")
            .field("batch", &self.batch)
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

    /// Data carries its logical-frame sequence. A barrier carries the exclusive data high-water
    /// sequence it closes.
    #[must_use]
    pub const fn checkpoint_sequence(&self) -> u64 {
        self.checkpoint_sequence
    }

    /// Split the message from any decoded-data admission after its transport
    /// scope has been validated.
    #[must_use]
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
    use std::sync::Arc;

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
        Barrier, HandshakeRequest, HandshakeResponse, Hello, RoutedData, ShuffleFrame,
        ShuffleSummary,
    };
    use super::{
        Holdover, InboundReservation, ReceivedBatch, ReceivedShuffle, ShuffleMessage,
        ShufflePeerId, SHUFFLE_ADDR_KEY, SHUFFLE_RECV_QUEUE,
    };
    use crate::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};
    use crate::cluster::control::ClusterKv;
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
    const MAX_STAGE_NAME_BYTES: usize = 4096;
    const BLOCKING_IPC_THRESHOLD_BYTES: usize = 512 * 1024;
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
    const SCOPE_CANCELLED: &str = "shuffle assignment or recovery scope was cancelled";

    type InboundRx = AsyncRx<mpsc::Array<Inbound>>;
    type InboundTx = MAsyncTx<mpsc::Array<Inbound>>;

    fn io_err<E: std::fmt::Display>(e: E) -> io::Error {
        io::Error::other(e.to_string())
    }

    fn scope_cancelled_io() -> io::Error {
        io::Error::new(io::ErrorKind::ConnectionAborted, SCOPE_CANCELLED)
    }

    fn scope_cancelled_status() -> tonic::Status {
        tonic::Status::cancelled(SCOPE_CANCELLED)
    }

    fn cancelled_token() -> CancellationToken {
        let token = CancellationToken::new();
        token.cancel();
        token
    }

    fn rotate_scope_token(slot: &RwLock<CancellationToken>, active: bool) {
        let mut token = slot.write();
        token.cancel();
        *token = CancellationToken::new();
        if !active {
            token.cancel();
        }
    }

    /// A message prepared before its sequence is fixed and inserted into the ordered queue.
    enum PreparedMessage {
        Barrier(CheckpointBarrier),
        Data {
            stage: String,
            routed_vnodes: Vec<u32>,
            arrow_ipc: Bytes,
        },
    }

    struct Outbound {
        gen: u64,
        assignment_version: u64,
        /// Data: this frame's sequence. `Barrier`: frames enqueued to this peer so far.
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
            ShuffleMessage::Barrier(_) => Ok(1024),
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
            ShuffleMessage::Barrier(barrier) => {
                Ok((PreparedMessage::Barrier(barrier.clone()), budget))
            }
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
                            stage: first
                                .then(|| std::mem::take(&mut stage))
                                .unwrap_or_default(),
                            routed_vnodes: first
                                .then(|| std::mem::take(&mut routes))
                                .unwrap_or_default(),
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
        /// True only for a transient durable-authority outage. The exact retained certificate
        /// may be reactivated without resetting its sequence domain.
        assignment_suspended: AtomicBool,
        /// Stamped onto every outbound data message; bumped by a coordinated rewind.
        recovery_gen: Arc<AtomicU64>,
        /// Data frames enqueued per peer. Lives here, not on `PeerConn`, so it survives the
        /// reconnect that discards a queue — otherwise the loss would leave no trace.
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
                            rotate_scope_token(&self.scope_cancel, true);
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
            rotate_scope_token(&self.scope_cancel, false);
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
            rotate_scope_token(&self.scope_cancel, true);
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
            rotate_scope_token(&self.scope_cancel, false);
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
            rotate_scope_token(&self.scope_cancel, false);
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
            rotate_scope_token(&self.scope_cancel, false);
            let mut pool = self.pool.lock();
            let mut seqs = self.seqs.lock();
            self.recovery_gen.store(gen, Ordering::Release);
            pool.clear();
            seqs.clear();
            self.connect_locks.lock().clear();
            rotate_scope_token(
                &self.scope_cancel,
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
        #[allow(clippy::unused_async)]
        pub async fn register_peer(&self, peer: ShufflePeerId, addr: SocketAddr) {
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
            if matches!(msg, ShuffleMessage::Barrier(_)) && assignment_fence.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "shuffle checkpoint barriers require an admitted assignment certificate",
                ));
            }
            let scope = self.current_scope(expected_assignment_version)?;
            let admission_bytes = outbound_workspace_bytes(msg)?;
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
            // Stamp data before enqueue. A frame the transport later discards still leaves a hole
            // the peer can see; a barrier carries the running count.
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
                    ShuffleMessage::Data { .. } => {
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
            tokio::select! {
                biased;
                () = scope.cancel.cancelled() => Err(scope_cancelled_io()),
                result = conn.tx.send(out) => result.map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("shuffle stream to peer {peer} closed"),
                    )
                }),
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
            let mut first_err = None;
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
                        if first_err.is_none() {
                            first_err = Some(e);
                        }
                    }
                }
            }
            match first_err {
                Some(error) => Err(error),
                None => Ok(()),
            }
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
            })) => response.map_err(io_err)?.into_inner(),
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
        server: JoinHandle<()>,
        holdover: Arc<Holdover>,
        /// Inbound data frames stamped below this are pre-rewind and discarded.
        recovery_gen: Arc<AtomicU64>,
        recovery_transition: Mutex<()>,
        assignment: Arc<RwLock<Option<Arc<InstalledAssignment>>>>,
        assignment_version: Arc<AtomicU64>,
        scope_cancel: Arc<RwLock<CancellationToken>>,
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
            let pending_handshakes = Arc::new(PendingHandshakes::default());
            let delivery = Arc::new(DeliveryTracker::default());
            let inbound_budget = Arc::new(InboundBudget::new(INBOUND_NODE_BUDGET_BYTES));
            let active_streams = Arc::new(Semaphore::new(MAX_ACTIVE_STREAMS));
            let active_stream_registry = Arc::new(ActiveStreamRegistry::default());
            let service = ShuffleService {
                local_id,
                receiver_incarnation,
                assignment: Arc::clone(&assignment),
                assignment_version: Arc::clone(&assignment_version),
                scope_cancel: Arc::clone(&scope_cancel),
                pending_handshakes: Arc::clone(&pending_handshakes),
                tx,
                recovery_gen: Arc::clone(&recovery_gen),
                delivery: Arc::clone(&delivery),
                inbound_budget,
                active_streams: Arc::clone(&active_streams),
                active_stream_registry,
            };
            // Set TCP_NODELAY on each accepted connection.
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
                server,
                holdover: Arc::new(Holdover::new(SHUFFLE_RECV_QUEUE)),
                recovery_gen,
                recovery_transition: Mutex::new(()),
                assignment,
                assignment_version,
                scope_cancel,
                assignment_suspended: AtomicBool::new(false),
                assignment_resumed: tokio::sync::Notify::new(),
                pending_handshakes,
                delivery,
                #[cfg(test)]
                active_streams,
            })
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
                            rotate_scope_token(&self.scope_cancel, true);
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
            rotate_scope_token(&self.scope_cancel, false);
            self.pending_handshakes.clear();
            self.delivery.reset_assignment();
            let version = next.fence.assignment_version;
            *assignment = Some(next);
            self.assignment_suspended.store(false, Ordering::Release);
            rotate_scope_token(&self.scope_cancel, true);
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
            rotate_scope_token(&self.scope_cancel, false);
            self.pending_handshakes.clear();
            self.assignment_suspended.store(true, Ordering::Release);
            self.assignment_version.store(0, Ordering::Release);
        }

        /// Reject all streams while an adopted owner map is awaiting its owner-complete process
        /// certificate. Pending tokens and sequence expectations cannot cross this boundary.
        pub fn invalidate_assignment_fence(&self) {
            let _assignment = self.assignment.write();
            rotate_scope_token(&self.scope_cancel, false);
            self.assignment_suspended.store(false, Ordering::Release);
            self.assignment_version.store(0, Ordering::Release);
            self.pending_handshakes.clear();
            self.delivery.reset_assignment();
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
            rotate_scope_token(&self.scope_cancel, false);
            self.pending_handshakes.clear();
            self.delivery.prepare_recovery(gen);
            self.recovery_gen.store(gen, Ordering::Release);
            rotate_scope_token(
                &self.scope_cancel,
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

        async fn wait_while_assignment_suspended(&self) {
            loop {
                let resumed = self.assignment_resumed.notified();
                if !self.assignment_suspended.load(Ordering::Acquire) {
                    return;
                }
                resumed.await;
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
            let Some(assignment) = self.consumption_scope() else {
                return false;
            };
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
            let certified_process = assignment.as_ref().is_some_and(|installed| {
                installed.fence.assignment_version == assignment_version
                    && installed.certifies(peer, sender_incarnation)
                    && installed.certifies(self.local_id, receiver_incarnation)
            });
            let current = recovery_gen == current_recovery
                && assignment_version == current_assignment
                && certified_process
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
            self.retain_queued_scope(
                received.peer,
                received.sender_incarnation,
                received.receiver_incarnation,
                received.assignment_version,
                received.recovery_gen,
            )
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

        /// Await the next `(peer_id, msg)`; `None` once the server stops and the
        /// queue drains. Concurrent callers serialise via `rx_returned`;
        /// cancellation-safe.
        pub async fn recv(&self) -> Option<ReceivedShuffle> {
            loop {
                self.wait_while_assignment_suspended().await;

                // The receiver lease serializes deferred and live queue admission. A
                // cancelled receive can therefore require at most one deferred slot.
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
                let inbound = guard.rx.as_mut()?.recv().await.ok()?;
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

        /// Drain the inbound queue into `staged`: bucket data by stage,
        /// and stash the first `Barrier`. A staged barrier closes the normal drainer until
        /// alignment takes it, preserving every following frame's position relative to that cut.
        /// Once the shared holdover is full, leave the bounded receive queue intact so transport
        /// admission backpressures peers.
        fn drain_inbound_into(&self, staged: &mut FxHashMap<String, Vec<ReceivedBatch>>) {
            if !self.holdover.staged_barriers.lock().is_empty() {
                return;
            }
            let slot = self.rx.lock();
            let Some(rx) = slot.as_ref() else {
                return;
            };
            while self.holdover.try_reserve_item() {
                let inbound = if let Some(deferred) = self.take_deferred_recv() {
                    deferred
                } else {
                    let Ok(inbound) = rx.try_recv() else {
                        self.holdover.release_items(1);
                        break;
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
                        stage: s, batch, ..
                    } => {
                        staged.entry(s).or_default().push(ReceivedBatch {
                            batch,
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
                    ShuffleMessage::Barrier(b) => {
                        self.holdover.staged_barriers.lock().push(ReceivedShuffle {
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
                        });
                        break;
                    }
                }
            }
        }

        /// Non-blocking drain of checkpointed operator batches for `stage`.
        #[must_use]
        pub fn drain_checkpointed_data_for(&self, stage: &str) -> Vec<ReceivedBatch> {
            let mut staged = self.holdover.staged.lock();
            self.drain_inbound_into(&mut staged);
            let mut batches = staged.remove(stage).unwrap_or_default();
            self.holdover.release_items(batches.len());
            batches.retain(|batch| self.retain_batch(batch));
            batches
        }

        /// Take the barriers stashed by [`Self::drain_checkpointed_data_for`].
        #[must_use]
        pub fn drain_staged_barriers(&self) -> Vec<ReceivedShuffle> {
            let mut staged = self.holdover.staged_barriers.lock();
            let mut barriers = std::mem::take(&mut *staged);
            drop(staged);
            self.holdover.release_items(barriers.len());
            barriers.retain(|barrier| self.retain_received(barrier));
            barriers
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
            self.holdover.staged_barriers.lock().push(barrier);
        }

        /// Drain every staged checkpointed operator batch.
        #[must_use]
        pub fn drain_checkpointed_staged(&self) -> Vec<(String, ReceivedBatch)> {
            let mut staged = self.holdover.staged.lock();
            self.drain_inbound_into(&mut staged);
            self.take_checkpointed_staged(&mut staged)
        }

        /// Drain only the checkpointed batches already in the holdover. Unlike
        /// [`Self::drain_checkpointed_staged`], this does not consume the live receive queue, whose
        /// data/barrier order must remain visible to checkpoint alignment.
        #[must_use]
        pub fn drain_checkpointed_holdover(&self) -> Vec<(String, ReceivedBatch)> {
            let mut staged = self.holdover.staged.lock();
            self.take_checkpointed_staged(&mut staged)
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

    /// Resolves a prepared logical data frame exactly once. An unresolved admission is loss
    /// unless its exact assignment/recovery lifetime was deliberately cancelled.
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
        pending_handshakes: Arc<PendingHandshakes>,
        tx: InboundTx,
        recovery_gen: Arc<AtomicU64>,
        delivery: Arc<DeliveryTracker>,
        inbound_budget: Arc<InboundBudget>,
        active_streams: Arc<Semaphore>,
        active_stream_registry: Arc<ActiveStreamRegistry>,
    }

    fn active_receiver_scope(
        assignment: &RwLock<Option<Arc<InstalledAssignment>>>,
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        scope_cancel: &RwLock<CancellationToken>,
    ) -> Result<ScopeLease, tonic::Status> {
        let assignment = assignment.read();
        let installed = assignment.as_ref().ok_or_else(|| {
            tonic::Status::failed_precondition("shuffle assignment certificate is not installed")
        })?;
        let version = assignment_version.load(Ordering::Acquire);
        let recovery_gen = recovery_gen.load(Ordering::Acquire);
        let cancel = scope_cancel.read().clone();
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
            if scope.cancel.is_cancelled() {
                self.pending_handshakes
                    .0
                    .lock()
                    .remove(&request.sender_node_id);
                return Err(scope_cancelled_status());
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
        let scope =
            active_receiver_scope(assignment, assignment_version, recovery_gen, scope_cancel)?;
        let first = tokio::select! {
            biased;
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
            () = scope.cancel.cancelled() => return Err(scope_cancelled_status()),
            guard = ingress.lock() => guard,
        };
        delivery.observe_hello(fence)?;
        let mut active_stream = active_stream_registry.replace(&fence, &scope.cancel);
        active_stream.acquire_permit(active_streams).await?;
        drop(ingress_guard);
        Ok((fence, ingress, scope, active_stream))
    }

    fn validate_active_stream_scope(
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        delivery: &DeliveryTracker,
        fence: &StreamFence,
        cancel: &CancellationToken,
    ) -> Result<(), tonic::Status> {
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

    /// Publish preceding-data completeness before making the barrier observable. A failed
    /// barrier enqueue cannot seal a checkpoint, while delaying this commit until after enqueue
    /// would let the consumer observe the barrier before its loss fence.
    async fn publish_barrier(
        tx: &InboundTx,
        assignment_version: &AtomicU64,
        recovery_gen: &AtomicU64,
        delivery: &DeliveryTracker,
        fence: StreamFence,
        msg: ShuffleMessage,
        assignment_digest: [u8; 32],
        last_seq: u64,
        cancel: &CancellationToken,
    ) -> Result<bool, tonic::Status> {
        validate_active_stream_scope(assignment_version, recovery_gen, delivery, &fence, cancel)?;
        let reservation = delivery.prepare_barrier(&fence, last_seq)?;
        validate_active_stream_scope(assignment_version, recovery_gen, delivery, &fence, cancel)?;
        delivery.commit_barrier(reservation)?;
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(scope_cancelled_status()),
            result = tx.send(Inbound {
                peer: fence.sender_node_id,
                msg,
                budget: None,
                fence,
                assignment_digest: Some(assignment_digest),
                checkpoint_sequence: last_seq,
            }) => Ok(result.is_ok()),
        }
    }

    /// Forward decoded frames onto the inbound queue and summarize on half-close.
    #[allow(clippy::too_many_lines)]
    async fn run_stream(
        service: &ShuffleService,
        mut stream: tonic::Streaming<ShuffleFrame>,
    ) -> Result<ShuffleSummary, tonic::Status> {
        let ShuffleService {
            receiver_incarnation,
            assignment,
            assignment_version,
            scope_cancel,
            pending_handshakes,
            tx,
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
                    let msg = ShuffleMessage::Barrier(CheckpointBarrier {
                        checkpoint_id: b.checkpoint_id,
                        epoch: b.epoch,
                        flags: b.flags,
                    });
                    if !publish_barrier(
                        tx,
                        assignment_version,
                        recovery_gen,
                        delivery,
                        fence,
                        msg,
                        assignment_digest,
                        b.last_seq,
                        stream_cancel,
                    )
                    .await?
                    {
                        break;
                    }
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
    ) -> Result<bool, tonic::Status> {
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
    mod delivery_tests {
        use super::*;

        const ACQUIRE: Ordering = Ordering::Acquire;

        fn fence(sender: u128, receiver: u128, stream: u128, version: u64) -> StreamFence {
            StreamFence {
                sender_node_id: 7,
                sender_incarnation: Uuid::from_u128(sender),
                receiver_incarnation: Uuid::from_u128(receiver),
                stream_id: Uuid::from_u128(stream),
                assignment_version: version,
                assignment_certificate_digest: [1; 32],
                recovery_gen: 0,
            }
        }

        fn deliver(
            tracker: &DeliveryTracker,
            fence: &StreamFence,
            seq: u64,
        ) -> Result<bool, tonic::Status> {
            let Some(reservation) = tracker.prepare_data(fence, seq)? else {
                return Ok(false);
            };
            tracker.commit_data(reservation)?;
            Ok(true)
        }

        fn admitted_test_budget() -> InboundReservation {
            let bytes = crate::shuffle::message::MAX_PAYLOAD_BYTES;
            let permits = u32::try_from(bytes).unwrap();
            InboundReservation {
                node: Arc::new(Semaphore::new(bytes))
                    .try_acquire_many_owned(permits)
                    .unwrap(),
                peer: Arc::new(Semaphore::new(bytes))
                    .try_acquire_many_owned(permits)
                    .unwrap(),
                wire_bytes: 1,
            }
        }

        /// A sender process cannot reset its sequence inside the same assignment/recovery scope.
        #[test]
        fn sender_scope_replacement_rejection_does_not_infer_a_missing_frame() {
            let d = DeliveryTracker::default();
            let first = fence(1, 10, 100, 1);
            d.observe_hello(first).unwrap();
            assert!(deliver(&d, &first, 0).unwrap());
            let restarted = fence(2, 10, 200, 1);
            let error = d.observe_hello(restarted).unwrap_err();

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
        }

        #[test]
        fn handshake_token_is_both_single_use_and_age_bounded_at_consumption() {
            let stream = fence(1, 10, 100, 1);
            let pending = PendingHandshakes::default();
            let now = std::time::Instant::now();
            pending.0.lock().insert(
                stream.sender_node_id,
                PendingHandshake {
                    fence: stream,
                    issued_at: now - HANDSHAKE_TOKEN_TTL,
                },
            );

            assert!(!consume_handshake_token(&pending, &stream, now));
            assert!(
                pending.0.lock().is_empty(),
                "expired token must be consumed"
            );

            pending.0.lock().insert(
                stream.sender_node_id,
                PendingHandshake {
                    fence: stream,
                    issued_at: now,
                },
            );
            assert!(consume_handshake_token(&pending, &stream, now));
            assert!(!consume_handshake_token(&pending, &stream, now));
        }

        #[tokio::test]
        async fn active_stream_replacement_is_per_peer() {
            let registry = Arc::new(ActiveStreamRegistry::default());
            let parent = CancellationToken::new();
            let permits = Arc::new(Semaphore::new(1));
            let original = fence(1, 10, 100, 1);
            let mut original_lease = registry.replace(&original, &parent);
            original_lease.acquire_permit(&permits).await.unwrap();
            assert!(!original_lease.cancel.is_cancelled());
            assert_eq!(registry.streams.lock().len(), 1);

            let replacement = StreamFence {
                stream_id: Uuid::from_u128(101),
                ..original
            };
            let replacement_lease = registry.replace(&replacement, &parent);
            assert!(original_lease.cancel.is_cancelled());
            drop(original_lease);
            assert_eq!(registry.streams.lock().len(), 1);
            drop(replacement_lease);
            assert!(registry.streams.lock().is_empty());
        }

        #[test]
        fn assignment_advance_replaces_old_sender_scope_at_sequence_zero() {
            let d = DeliveryTracker::default();
            let first = fence(1, 10, 100, 1);
            d.observe_hello(first).unwrap();
            assert!(deliver(&d, &first, 0).unwrap());

            let next_assignment = fence(2, 10, 200, 2);
            d.observe_hello(next_assignment).unwrap();

            assert!(deliver(&d, &next_assignment, 0).unwrap());
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
        }

        #[test]
        fn assignment_and_recovery_scope_changes_clear_tracked_ingress_peers() {
            let d = DeliveryTracker::default();
            for peer in 1..=256 {
                let mut stream = fence(peer, 10, peer + 1_000, 1);
                stream.sender_node_id = u64::try_from(peer).unwrap();
                d.ingress_lock(stream.sender_node_id).unwrap();
                d.observe_hello(stream).unwrap();
            }
            assert_eq!(d.ingress.lock().len(), 256);
            assert_eq!(d.peers.lock().len(), 256);

            d.reset_assignment();
            assert!(d.ingress.lock().is_empty());
            assert!(d.peers.lock().is_empty());

            d.ingress_lock(7).unwrap();
            d.prepare_recovery(1);
            assert!(d.ingress.lock().is_empty());
        }

        /// A reconnect of the same incarnation keeps its expectation so a discarded outbound
        /// queue remains detectable.
        #[test]
        fn same_incarnation_reconnect_keeps_expectation() {
            let d = DeliveryTracker::default();
            let first = fence(1, 10, 100, 1);
            d.observe_hello(first).unwrap();
            assert!(deliver(&d, &first, 0).unwrap());
            let reconnect = fence(1, 10, 101, 1);
            d.observe_hello(reconnect).unwrap();
            assert!(deliver(&d, &reconnect, 2).unwrap()); // frame 1 died with the queue
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
        }

        /// Recovery resets to sequence zero; a missing first frame is visible at the barrier.
        #[test]
        fn recovery_does_not_rebaseline_past_missing_sequence_zero() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            d.observe_hello(stream).unwrap();
            assert!(deliver(&d, &stream, 0).unwrap());
            let mut rewound = fence(1, 10, 101, 1);
            rewound.recovery_gen = 1;
            d.observe_hello(rewound).unwrap();
            let barrier = d.prepare_barrier(&rewound, 1).unwrap();
            d.commit_barrier(barrier).unwrap();
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
        }

        #[test]
        fn cancelled_pre_enqueue_admission_is_recorded_once() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            d.observe_hello(stream).unwrap();
            let cancelled =
                DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);
            drop(cancelled);
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);

            let barrier = d.prepare_barrier(&stream, 1).unwrap();
            d.commit_barrier(barrier).unwrap();

            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
        }

        #[test]
        fn scope_rotation_does_not_report_transport_loss() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            d.observe_hello(stream).unwrap();
            let admission =
                DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);

            cancel.cancel();
            drop(admission);

            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
        }

        #[test]
        fn enqueued_frame_commits_expectation_after_scope_cancellation() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            d.observe_hello(stream).unwrap();
            let admission =
                DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);

            cancel.cancel();
            admission.commit_after_enqueue().unwrap();
            let barrier = d.prepare_barrier(&stream, 1).unwrap();
            d.commit_barrier(barrier).unwrap();

            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
        }

        #[test]
        fn exhausted_cancelled_admission_fails_closed_without_overflow() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            d.observe_hello(stream).unwrap();
            let exhausted = DataAdmission::new(
                &d,
                DataReservation {
                    fence: stream,
                    seq: u64::MAX,
                    expected: 0,
                },
                &cancel,
            );

            drop(exhausted);

            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
        }

        #[tokio::test]
        async fn barrier_loss_is_visible_before_the_barrier_can_be_dequeued() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            d.observe_hello(stream).unwrap();
            let assignment = AtomicU64::new(stream.assignment_version);
            let recovery = AtomicU64::new(stream.recovery_gen);
            let cancel = CancellationToken::new();
            let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
            let publish = publish_barrier(
                &tx,
                &assignment,
                &recovery,
                &d,
                stream,
                ShuffleMessage::Barrier(CheckpointBarrier::new(1, 1)),
                [1; 32],
                1,
                &cancel,
            );
            let consume = async {
                let received = rx.recv().await.unwrap();
                assert!(matches!(received.msg, ShuffleMessage::Barrier(_)));
                d.delivery_loss_incidents.load(ACQUIRE)
            };

            let (published, loss_at_visibility) = tokio::join!(publish, consume);

            assert!(published.unwrap());
            assert_eq!(loss_at_visibility, 1);
        }

        #[test]
        fn backward_barrier_high_water_is_a_protocol_fault() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            d.observe_hello(stream).unwrap();
            assert!(deliver(&d, &stream, 0).unwrap());

            let error = d.prepare_barrier(&stream, 0).unwrap_err();

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
        }

        #[test]
        fn maximum_barrier_high_water_records_one_nonwrapping_incident() {
            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            d.observe_hello(stream).unwrap();

            let barrier = d.prepare_barrier(&stream, u64::MAX).unwrap();
            d.commit_barrier(barrier).unwrap();
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);

            d.note_loss(stream.sender_node_id, 1, "test-successor");
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 2);
        }

        #[test]
        fn exhausted_loss_counter_is_permanently_fail_closed() {
            let d = DeliveryTracker::default();
            d.delivery_loss_incidents
                .store(u64::MAX - 1, Ordering::Release);

            d.note_loss(7, 1, "test-exhaustion");
            d.note_loss(7, 1, "test-after-exhaustion");
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), u64::MAX);

            d.prepare_recovery(1);
            assert!(d.complete_recovery(1));
            assert_eq!(
                d.recovered_delivery_loss_incidents.load(ACQUIRE),
                u64::MAX - 1
            );
            assert!(
                d.delivery_loss_incidents.load(ACQUIRE)
                    > d.recovered_delivery_loss_incidents.load(ACQUIRE)
            );
        }

        #[test]
        fn successor_hello_invalidates_predecessor_reservation_before_commit() {
            let d = DeliveryTracker::default();
            let predecessor = fence(1, 10, 100, 1);
            d.observe_hello(predecessor).unwrap();
            let reserved = d.prepare_data(&predecessor, 0).unwrap().unwrap();
            let successor = fence(1, 10, 101, 1);
            d.observe_hello(successor).unwrap();

            assert!(d.commit_data(reserved).is_err());
            assert!(deliver(&d, &successor, 0).unwrap());
        }

        #[tokio::test]
        async fn coalesced_multi_vnode_frame_has_one_atomic_queue_admission() {
            use arrow_array::{Int64Array, UInt32Array};
            use arrow_schema::{DataType, Field, Schema};

            let d = DeliveryTracker::default();
            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            d.observe_hello(stream).unwrap();
            let admission =
                DataAdmission::new(&d, d.prepare_data(&stream, 0).unwrap().unwrap(), &cancel);

            let schema = Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, false),
                Field::new("__laminar_vnode", DataType::UInt32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![10, 20])),
                    Arc::new(UInt32Array::from(vec![0, 1])),
                ],
            )
            .unwrap();
            let decoded_bytes =
                InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
            let budget = admitted_test_budget();
            let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
            let owners = [10, 10];
            let assignment_fence = CheckpointAssignmentFence::from_owner_map(
                1,
                &owners,
                vec![crate::checkpoint::CheckpointParticipant {
                    node_id: 10,
                    boot_incarnation: Uuid::from_u128(10),
                }],
            )
            .unwrap();
            let assignment = InstalledAssignment::for_process(
                &assignment_fence,
                &owners,
                10,
                Uuid::from_u128(10),
            )
            .unwrap();
            assert!(forward_routed_batch(
                &tx,
                stream,
                10,
                &assignment,
                "same-stage".to_string(),
                vec![0, 1],
                batch,
                budget,
                decoded_bytes,
                0,
                &cancel,
            )
            .await
            .unwrap());
            admission.commit_after_enqueue().unwrap();

            let received = rx.recv().await.unwrap();
            let ShuffleMessage::Data {
                stage,
                routed_vnodes,
                batch,
            } = received.msg
            else {
                panic!("expected one coalesced routed batch");
            };
            assert_eq!(stage, "same-stage");
            assert_eq!(&*routed_vnodes, &[0, 1]);
            assert_eq!(batch.num_rows(), 2);
            assert!(batch.schema().field_with_name("__laminar_vnode").is_ok());
            assert!(rx.try_recv().is_err());
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 0);
        }

        #[tokio::test]
        async fn foreign_route_is_rejected_before_queue_publication() {
            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            let owners = [10, 20];
            let assignment_fence = CheckpointAssignmentFence::from_owner_map(
                1,
                &owners,
                vec![
                    crate::checkpoint::CheckpointParticipant {
                        node_id: 10,
                        boot_incarnation: Uuid::from_u128(10),
                    },
                    crate::checkpoint::CheckpointParticipant {
                        node_id: 20,
                        boot_incarnation: Uuid::from_u128(20),
                    },
                ],
            )
            .unwrap();
            let assignment = InstalledAssignment::for_process(
                &assignment_fence,
                &owners,
                10,
                Uuid::from_u128(10),
            )
            .unwrap();
            let (tx, rx) = mpsc::bounded_async::<Inbound>(1);
            let batch = RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "value",
                    arrow_schema::DataType::Int64,
                    false,
                )])),
                vec![Arc::new(arrow_array::Int64Array::from(vec![10]))],
            )
            .unwrap();
            let decoded_bytes =
                InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
            let budget = admitted_test_budget();

            let error = forward_routed_batch(
                &tx,
                stream,
                10,
                &assignment,
                "stage".to_string(),
                vec![1],
                batch,
                budget,
                decoded_bytes,
                0,
                &cancel,
            )
            .await
            .unwrap_err();

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn coalesced_batch_with_any_foreign_vnode_is_rejected_atomically() {
            use arrow_array::{Int64Array, UInt32Array};
            use arrow_schema::{DataType, Field, Schema};

            let stream = fence(1, 10, 100, 1);
            let cancel = CancellationToken::new();
            let owners = [10, 20];
            let assignment_fence = CheckpointAssignmentFence::from_owner_map(
                1,
                &owners,
                vec![
                    crate::checkpoint::CheckpointParticipant {
                        node_id: 10,
                        boot_incarnation: Uuid::from_u128(10),
                    },
                    crate::checkpoint::CheckpointParticipant {
                        node_id: 20,
                        boot_incarnation: Uuid::from_u128(20),
                    },
                ],
            )
            .unwrap();
            let assignment = InstalledAssignment::for_process(
                &assignment_fence,
                &owners,
                10,
                Uuid::from_u128(10),
            )
            .unwrap();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("value", DataType::Int64, false),
                    Field::new("__laminar_vnode", DataType::UInt32, false),
                ])),
                vec![
                    Arc::new(Int64Array::from(vec![10, 20])),
                    Arc::new(UInt32Array::from(vec![0, 1])),
                ],
            )
            .unwrap();
            let decoded_bytes =
                InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
            let budget = admitted_test_budget();
            let (tx, rx) = mpsc::bounded_async::<Inbound>(2);

            let error = forward_routed_batch(
                &tx,
                stream,
                10,
                &assignment,
                "stage".to_string(),
                vec![0, 1],
                batch,
                budget,
                decoded_bytes,
                0,
                &cancel,
            )
            .await
            .unwrap_err();

            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(rx.try_recv().is_err());
        }

        #[test]
        fn superseded_stream_is_rejected_and_fences_the_epoch() {
            let d = DeliveryTracker::default();
            let old = fence(1, 10, 100, 1);
            let replacement = fence(1, 10, 101, 1);
            d.observe_hello(old).unwrap();
            d.observe_hello(replacement).unwrap();

            assert!(d.prepare_data(&old, 0).is_err());
            assert_eq!(d.delivery_loss_incidents.load(ACQUIRE), 1);
        }
    }

    #[cfg(test)]
    mod encode_tests {
        use super::*;
        use arrow::array::ArrayData;
        use arrow::buffer::Buffer;
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        async fn frame(msg: ShuffleMessage) -> Encoded {
            let workspace = outbound_workspace_bytes(&msg).unwrap();
            let permits = u32::try_from(workspace).unwrap();
            let peer = Arc::new(Semaphore::new(workspace))
                .try_acquire_many_owned(permits)
                .expect("test byte permit");
            let node = Arc::new(Semaphore::new(workspace))
                .try_acquire_many_owned(permits)
                .expect("test node byte permit");
            let budget = OutboundReservation { peer, node };
            let (prepared, budget) = prepare_outbound_message(&msg, budget).await.unwrap();
            frame_message(Outbound {
                gen: 0,
                assignment_version: 1,
                seq: 0,
                msg: prepared,
                assignment_digest: None,
                _budget: budget,
            })
            .unwrap()
        }

        fn payload(encoded: Encoded) -> Vec<u8> {
            encoded
                .frames
                .into_iter()
                .flat_map(|frame| match frame.kind.unwrap() {
                    shuffle_frame::Kind::Data(fragment) => fragment.arrow_ipc,
                    _ => panic!("expected data fragment"),
                })
                .collect()
        }

        #[tokio::test]
        async fn each_logical_batch_is_self_contained_without_stage_codec_state() {
            let batch = |name: &str| {
                let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
                arrow_array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
                    .unwrap()
            };
            for name in ["a", "b"] {
                let encoded = frame(ShuffleMessage::checkpointed("s".into(), 0, batch(name))).await;
                let decoded =
                    decode_ipc_payload(&mut BatchStreamDecoder::new(), payload(encoded)).unwrap();
                assert_eq!(decoded.schema().field(0).name(), name);
            }
        }

        #[tokio::test]
        async fn large_batch_is_offloaded_and_split_into_bounded_wire_frames() {
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Int64, false),
                Field::new("c", DataType::Int64, false),
            ]));
            let column = || Arc::new(Int64Array::from(vec![7; 65_536])) as arrow_array::ArrayRef;
            let batch = RecordBatch::try_new(schema, vec![column(), column(), column()]).unwrap();
            let msg = ShuffleMessage::checkpointed("stage".into(), 9, batch);
            let encoded = frame(msg).await;

            assert!(encoded.frames.len() > 1);
            let mut previous_end = None;
            for (index, frame) in encoded.frames.into_iter().enumerate() {
                let shuffle_frame::Kind::Data(fragment) = frame.kind.unwrap() else {
                    panic!("expected data fragment");
                };
                assert!(fragment.arrow_ipc.len() <= MAX_WIRE_PAYLOAD_BYTES);
                assert_eq!(fragment.seq, 0);
                if index == 0 {
                    assert_eq!(fragment.stage, "stage");
                    assert_eq!(fragment.routed_vnodes, vec![9]);
                } else {
                    assert!(fragment.stage.is_empty());
                    assert!(fragment.routed_vnodes.is_empty());
                }
                if let Some(end) = previous_end {
                    assert_eq!(fragment.arrow_ipc.as_ptr(), end);
                }
                previous_end = Some(
                    fragment
                        .arrow_ipc
                        .as_ptr()
                        .wrapping_add(fragment.arrow_ipc.len()),
                );
            }
        }

        #[tokio::test]
        async fn cancelled_blocking_encode_retains_admission_until_worker_exits() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7; 65_536]))])
                    .unwrap();
            let msg = ShuffleMessage::checkpointed("stage".into(), 9, batch);
            let workspace = outbound_workspace_bytes(&msg).unwrap();
            let permits = u32::try_from(workspace).unwrap();
            let peer = Arc::new(Semaphore::new(workspace));
            let node = Arc::new(Semaphore::new(workspace));
            let budget = OutboundReservation {
                peer: Arc::clone(&peer).try_acquire_many_owned(permits).unwrap(),
                node: Arc::clone(&node).try_acquire_many_owned(permits).unwrap(),
            };
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let task = tokio::spawn(async move {
                prepare_outbound_message_with_hook(&msg, budget, move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                })
                .await
            });

            started_rx.await.unwrap();
            task.abort();
            assert_eq!(peer.available_permits(), 0);
            assert_eq!(node.available_permits(), 0);
            release_tx.send(()).unwrap();
            let _ = task.await;
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while peer.available_permits() != workspace || node.available_permits() != workspace
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled encoder retained its admission after the worker exited");
        }

        #[test]
        fn oversized_decoded_batch_is_rejected_before_enqueue() {
            let rows = crate::shuffle::ROUTE_MAX_BATCH_BYTES / std::mem::size_of::<i64>() + 1;
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0; rows]))])
                    .unwrap();
            let msg = ShuffleMessage::checkpointed("stage".into(), 0, batch);

            assert_eq!(
                outbound_workspace_bytes(&msg).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }

        #[test]
        fn externally_owned_buffers_cannot_bypass_the_logical_batch_bound() {
            let rows = crate::shuffle::ROUTE_MAX_BATCH_BYTES / std::mem::size_of::<i64>() + 1;
            let bytes = Bytes::from(vec![0; rows * std::mem::size_of::<i64>()]);
            let data = ArrayData::builder(DataType::Int64)
                .len(rows)
                .add_buffer(Buffer::from(bytes))
                .build()
                .unwrap();
            let array = Int64Array::from(data);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                vec![Arc::new(array)],
            )
            .unwrap();
            let msg = ShuffleMessage::checkpointed("stage".into(), 0, batch);

            assert_eq!(
                outbound_workspace_bytes(&msg).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }

        #[test]
        fn oversized_schema_is_rejected_before_ipc_allocation() {
            let field_name = "x".repeat(MAX_SOURCE_SCHEMA_MEMORY_BYTES);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    field_name,
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
            )
            .unwrap();

            let error =
                outbound_workspace_bytes(&ShuffleMessage::checkpointed("stage".into(), 0, batch))
                    .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("schema"));
        }

        #[tokio::test]
        async fn accepted_near_limit_schema_round_trips_with_decode_headroom() {
            let field_name = "x".repeat(MAX_SOURCE_SCHEMA_MEMORY_BYTES - 4096);
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    field_name.clone(),
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(Vec::<i64>::new()))],
            )
            .unwrap();
            assert!(schema_memory_size(batch.schema().as_ref()) < MAX_SOURCE_SCHEMA_MEMORY_BYTES);

            let encoded = frame(ShuffleMessage::checkpointed("stage".into(), 0, batch)).await;
            let decoded =
                decode_ipc_payload(&mut BatchStreamDecoder::new(), payload(encoded)).unwrap();

            assert_eq!(decoded.schema().field(0).name(), &field_name);
            assert!(
                schema_memory_size(decoded.schema().as_ref()) < MAX_DECODED_SCHEMA_MEMORY_BYTES
            );
        }
    }

    #[cfg(test)]
    mod fragment_tests {
        use super::*;
        use crate::serialization::BatchStreamEncoder;

        #[test]
        fn received_envelope_retains_admission_through_consumer_fold() {
            let node = Arc::new(Semaphore::new(1));
            let peer = Arc::new(Semaphore::new(1));
            let reservation = Arc::new(InboundReservation {
                node: Arc::clone(&node).try_acquire_owned().unwrap(),
                peer: Arc::clone(&peer).try_acquire_owned().unwrap(),
                wire_bytes: 1,
            });
            let batch = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty()));
            let inbound = Inbound {
                peer: 7,
                msg: ShuffleMessage::checkpointed("stage".into(), 3, batch),
                budget: Some(reservation),
                fence: StreamFence {
                    sender_node_id: 7,
                    sender_incarnation: Uuid::from_u128(1),
                    receiver_incarnation: Uuid::from_u128(2),
                    stream_id: Uuid::from_u128(3),
                    assignment_version: 4,
                    assignment_certificate_digest: [4; 32],
                    recovery_gen: 5,
                },
                assignment_digest: None,
                checkpoint_sequence: 0,
            };

            let received = inbound.into_received();
            assert_eq!(node.available_permits(), 0);
            assert_eq!(peer.available_permits(), 0);

            assert_eq!(received.peer(), 7);
            assert_eq!(received.checkpoint_sequence(), 0);
            assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
            assert_eq!(node.available_permits(), 0);
            assert_eq!(peer.available_permits(), 0);
            drop(received);

            assert_eq!(node.available_permits(), 1);
            assert_eq!(peer.available_permits(), 1);
        }

        #[test]
        fn batch_admission_releases_after_last_retaining_consumer() {
            let node = Arc::new(Semaphore::new(1));
            let peer = Arc::new(Semaphore::new(1));
            let received = ReceivedBatch {
                batch: RecordBatch::new_empty(Arc::new(arrow_schema::Schema::empty())),
                reservation: Some(Arc::new(InboundReservation {
                    node: Arc::clone(&node).try_acquire_owned().unwrap(),
                    peer: Arc::clone(&peer).try_acquire_owned().unwrap(),
                    wire_bytes: 1,
                })),
                peer: 7,
                sender_incarnation: Uuid::from_u128(1),
                receiver_incarnation: Uuid::from_u128(2),
                stream_id: Uuid::from_u128(3),
                assignment_version: 4,
                recovery_gen: 5,
                checkpoint_sequence: 0,
            };

            let (batch, admission) = received.into_parts();
            let second_consumer = admission.clone();
            drop(batch);
            drop(admission);
            assert_eq!(node.available_permits(), 0);
            assert_eq!(peer.available_permits(), 0);

            drop(second_consumer);
            assert_eq!(node.available_permits(), 1);
            assert_eq!(peer.available_permits(), 1);
        }

        #[tokio::test]
        async fn cancelled_scope_releases_blocked_inbound_budget() {
            let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
            let held_node = Arc::clone(&budget.node)
                .acquire_many_owned(
                    u32::try_from(INBOUND_NODE_BUDGET_BYTES).expect("node budget fits u32"),
                )
                .await
                .unwrap();
            let cancel = CancellationToken::new();
            let mut reservation = Box::pin(budget.reserve_frame(9, 1, &cancel));

            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), &mut reservation)
                    .await
                    .is_err()
            );
            let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
            assert!(peer_budget.available_permits() < INBOUND_PEER_BUDGET_BYTES);

            cancel.cancel();
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), &mut reservation)
                .await
                .expect("cancelled inbound reservation remained blocked");
            let Err(error) = result else {
                panic!("cancelled inbound reservation was admitted");
            };

            assert_eq!(error.code(), tonic::Code::Cancelled);
            assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
            drop(held_node);
            assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
        }

        #[tokio::test]
        async fn cancelled_blocking_decode_retains_admission_until_worker_exits() {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow_array::Int64Array::from(vec![7; 200_000]))],
            )
            .unwrap();
            let payload = crate::serialization::serialize_batch_stream(&batch).unwrap();
            assert!(payload.len() >= BLOCKING_IPC_THRESHOLD_BYTES);
            let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
            let reservation = budget
                .reserve_frame(9, payload.len(), &CancellationToken::new())
                .await
                .unwrap();
            let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
            let admitted_node = budget.node.available_permits();
            let admitted_peer = peer_budget.available_permits();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let task = tokio::spawn(async move {
                decode_ipc_payload_isolated(payload, reservation, move || {
                    let _ = started_tx.send(());
                    let _ = release_rx.recv();
                })
                .await
            });

            started_rx.await.unwrap();
            task.abort();
            assert_eq!(budget.node.available_permits(), admitted_node);
            assert_eq!(peer_budget.available_permits(), admitted_peer);
            release_tx.send(()).unwrap();
            let _ = task.await;
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                while budget.node.available_permits() != INBOUND_NODE_BUDGET_BYTES
                    || peer_budget.available_permits() != INBOUND_PEER_BUDGET_BYTES
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled decoder retained its admission after the worker exited");
        }

        #[tokio::test]
        async fn aggregate_decoded_payload_expansion_is_rejected_and_releases_admission() {
            let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
            let reservation = budget
                .reserve_frame(9, 1, &CancellationToken::new())
                .await
                .unwrap();
            let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
            let rows =
                crate::shuffle::ROUTE_MAX_BATCH_BYTES / (2 * std::mem::size_of::<i64>()) + 1024;
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let make_batch = || {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(arrow_array::Int64Array::from(vec![0; rows]))],
                )
                .unwrap()
            };
            let batches = [make_batch(), make_batch()];
            assert!(
                batches
                    .iter()
                    .all(|batch| batch.get_array_memory_size()
                        <= crate::shuffle::ROUTE_MAX_BATCH_BYTES),
                "each decoded batch must fit by itself"
            );

            let error = InboundBudget::validate_decoded(&batches).unwrap_err();
            assert_eq!(error.code(), tonic::Code::ResourceExhausted);
            assert!(error.message().contains("decoded shuffle payload"));

            drop(reservation);
            assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
            assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
        }

        #[tokio::test]
        async fn many_tiny_decoded_batches_coexist_under_measured_admission() {
            const HELD_BATCHES: usize = 256;

            let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
            let wire_bytes = 128;
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
            )
            .unwrap();
            let decoded_bytes =
                InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
            let stage = "stage".to_string();
            let routed_vnodes = vec![1];
            let metadata_bytes =
                retained_batch_metadata_bytes(&stage, &routed_vnodes, &batch).unwrap();
            let retained_bytes = wire_bytes + decoded_bytes + metadata_bytes;
            let decode_reservation_bytes = wire_bytes
                + crate::shuffle::ROUTE_MAX_BATCH_BYTES
                + INBOUND_BATCH_METADATA_BYTES
                + MAX_WIRE_PAYLOAD_BYTES;
            let mut held = Vec::with_capacity(HELD_BATCHES);

            for count in 1..=HELD_BATCHES {
                assert!(budget.node.available_permits() >= decode_reservation_bytes);
                if let Some(peer_budget) = budget.peers.lock().get(&9) {
                    assert!(peer_budget.available_permits() >= decode_reservation_bytes);
                }
                let mut reservation = budget
                    .reserve_frame(9, wire_bytes, &CancellationToken::new())
                    .await
                    .unwrap();
                reservation
                    .retain_decoded(decoded_bytes, metadata_bytes)
                    .unwrap();
                held.push(Arc::new(reservation));

                let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
                assert_eq!(
                    budget.node.available_permits(),
                    INBOUND_NODE_BUDGET_BYTES - count * retained_bytes
                );
                assert_eq!(
                    peer_budget.available_permits(),
                    INBOUND_PEER_BUDGET_BYTES - count * retained_bytes
                );
            }

            let peer_budget = Arc::clone(budget.peers.lock().get(&9).unwrap());
            drop(held);
            assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
            assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
        }

        #[tokio::test]
        async fn decoded_admission_releases_exact_excess_and_last_holder_releases_rest() {
            let budget = InboundBudget::new(INBOUND_NODE_BUDGET_BYTES);
            let wire_bytes = 2048;
            let mut reservation = budget
                .reserve_frame(11, wire_bytes, &CancellationToken::new())
                .await
                .unwrap();
            let peer_budget = Arc::clone(budget.peers.lock().get(&11).unwrap());
            let decode_reservation_bytes = wire_bytes
                + crate::shuffle::ROUTE_MAX_BATCH_BYTES
                + INBOUND_BATCH_METADATA_BYTES
                + MAX_WIRE_PAYLOAD_BYTES;
            assert_eq!(
                budget.node.available_permits(),
                INBOUND_NODE_BUDGET_BYTES - decode_reservation_bytes
            );
            assert_eq!(
                peer_budget.available_permits(),
                INBOUND_PEER_BUDGET_BYTES - decode_reservation_bytes
            );

            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let decoded_bytes =
                InboundBudget::validate_decoded(std::slice::from_ref(&batch)).unwrap();
            let metadata_bytes =
                retained_batch_metadata_bytes(&"stage".to_string(), &vec![1], &batch).unwrap();
            reservation
                .retain_decoded(decoded_bytes, metadata_bytes)
                .unwrap();
            let retained_bytes = wire_bytes + decoded_bytes + metadata_bytes;
            assert_eq!(
                budget.node.available_permits(),
                INBOUND_NODE_BUDGET_BYTES - retained_bytes
            );
            assert_eq!(
                peer_budget.available_permits(),
                INBOUND_PEER_BUDGET_BYTES - retained_bytes
            );

            let reservation = Arc::new(reservation);
            let final_holder = Arc::clone(&reservation);
            drop(reservation);
            assert_eq!(
                budget.node.available_permits(),
                INBOUND_NODE_BUDGET_BYTES - retained_bytes
            );
            assert_eq!(
                peer_budget.available_permits(),
                INBOUND_PEER_BUDGET_BYTES - retained_bytes
            );
            drop(final_holder);
            assert_eq!(budget.node.available_permits(), INBOUND_NODE_BUDGET_BYTES);
            assert_eq!(peer_budget.available_permits(), INBOUND_PEER_BUDGET_BYTES);
        }

        #[test]
        fn ipc_message_may_not_span_logical_shuffle_payloads() {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
            let payload = encoder.encode(&batch).unwrap();
            let truncated = payload[..payload.len() - 1].to_vec();

            let error = decode_ipc_payload(&mut BatchStreamDecoder::new(), truncated).unwrap_err();
            assert!(error.to_string().contains("Unexpected End of Stream"));
        }

        #[test]
        fn one_logical_payload_rejects_multiple_complete_batches() {
            let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]));
            let batch = |value| {
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(arrow_array::Int64Array::from(vec![value]))],
                )
                .unwrap()
            };
            let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
            let mut payload = encoder.encode(&batch(1)).unwrap();
            payload.extend(encoder.encode(&batch(2)).unwrap());

            let error = decode_ipc_payload(&mut BatchStreamDecoder::new(), payload).unwrap_err();
            assert!(error.to_string().contains("expected exactly one"));
        }

        #[test]
        fn logical_payload_rejects_zero_batches() {
            let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Int64,
                false,
            )]);
            let mut encoder = BatchStreamEncoder::new(&schema).unwrap();
            let payload = encoder.finish().unwrap();

            let error = decode_ipc_payload(&mut BatchStreamDecoder::new(), payload).unwrap_err();
            assert!(error.to_string().contains("expected exactly one"));
        }

        fn fragment(index: u32, count: u32, total: u32, payload: Vec<u8>) -> RoutedData {
            RoutedData {
                stage: (index == 0).then(|| "stage".into()).unwrap_or_default(),
                routed_vnodes: (index == 0).then_some(vec![3]).unwrap_or_default(),
                arrow_ipc: payload.into(),
                recovery_gen: 4,
                seq: 12,
                fragment_index: index,
                fragment_count: count,
                total_payload_bytes: total,
            }
        }

        fn padded_ipc_payload(len: usize, fill: u8) -> Vec<u8> {
            let batch = RecordBatch::try_new(
                Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "value",
                    arrow_schema::DataType::Int64,
                    false,
                )])),
                vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
            )
            .unwrap();
            let mut payload = crate::serialization::serialize_batch_stream(&batch).unwrap();
            assert!(payload.len() <= len);
            payload.resize(len, fill);
            payload
        }

        #[test]
        fn ipc_schema_header_is_verified_before_arrow_decode() {
            let valid = padded_ipc_payload(1024, 0);
            assert!(validate_ipc_schema_header(&valid, valid.len()).is_ok());

            for len in 0..8 {
                assert!(validate_ipc_schema_header(&valid[..len], len).is_err());
            }

            let mut wrong_marker = valid.clone();
            wrong_marker[0] = 0;
            assert!(validate_ipc_schema_header(&wrong_marker, wrong_marker.len()).is_err());

            let mut zero_len = valid.clone();
            zero_len[4..8].copy_from_slice(&0u32.to_le_bytes());
            assert!(validate_ipc_schema_header(&zero_len, zero_len.len()).is_err());

            let mut over_limit = valid.clone();
            over_limit[4..8].copy_from_slice(
                &u32::try_from(MAX_SCHEMA_WIRE_BYTES + 1)
                    .unwrap()
                    .to_le_bytes(),
            );
            assert!(validate_ipc_schema_header(&over_limit, over_limit.len()).is_err());

            let schema_len =
                usize::try_from(u32::from_le_bytes(valid[4..8].try_into().unwrap())).unwrap();
            let truncated = &valid[..8 + schema_len - 1];
            assert!(validate_ipc_schema_header(truncated, valid.len()).is_err());

            let mut invalid_flatbuffer = vec![0xff; 4];
            invalid_flatbuffer.extend_from_slice(&8u32.to_le_bytes());
            invalid_flatbuffer.extend_from_slice(&[0; 8]);
            assert!(
                validate_ipc_schema_header(&invalid_flatbuffer, invalid_flatbuffer.len()).is_err()
            );

            let record_batch = &valid[8 + schema_len..];
            assert!(record_batch.len() >= 8);
            assert!(validate_ipc_schema_header(record_batch, record_batch.len()).is_err());
        }

        fn reservation() -> InboundReservation {
            let node = Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("test node permit");
            let peer = Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("test peer permit");
            InboundReservation {
                node,
                peer,
                wire_bytes: 1,
            }
        }

        #[test]
        fn fragments_reassemble_exactly_once_in_order() {
            let total = MAX_WIRE_PAYLOAD_BYTES + 3;
            let total_wire = u32::try_from(total).unwrap();
            let mut assembly = None;
            assert!(push_fragment(
                &mut assembly,
                &fragment(
                    0,
                    2,
                    total_wire,
                    padded_ipc_payload(MAX_WIRE_PAYLOAD_BYTES, 1),
                ),
                Some(reservation()),
            )
            .unwrap()
            .is_none());
            let complete =
                push_fragment(&mut assembly, &fragment(1, 2, total_wire, vec![2; 3]), None)
                    .unwrap()
                    .expect("logical frame completed");

            assert_eq!(complete.stage, "stage");
            assert_eq!(complete.routed_vnodes, vec![3]);
            assert_eq!(complete.seq, 12);
            assert_eq!(complete.arrow_ipc.len(), total);
            assert_eq!(complete.arrow_ipc[MAX_WIRE_PAYLOAD_BYTES], 2);
            assert!(assembly.is_none());
        }

        #[test]
        fn route_metadata_must_be_nonempty_and_canonical() {
            let payload = padded_ipc_payload(1024, 1);
            let valid = fragment(0, 1, u32::try_from(payload.len()).unwrap(), payload);
            assert!(validate_fragment(&valid).is_ok());

            for routed_vnodes in [Vec::new(), vec![3, 3], vec![4, 3]] {
                let mut malformed = valid.clone();
                malformed.routed_vnodes = routed_vnodes;
                assert!(validate_fragment(&malformed).is_err());
            }
        }

        #[test]
        fn malformed_or_interleaved_fragments_fail_closed() {
            let total = u32::try_from(MAX_WIRE_PAYLOAD_BYTES + 1).unwrap();

            let mut missing_zero = None;
            assert!(
                push_fragment(&mut missing_zero, &fragment(1, 2, total, vec![1]), None).is_err()
            );

            let mut reordered = None;
            push_fragment(
                &mut reordered,
                &fragment(0, 2, total, padded_ipc_payload(MAX_WIRE_PAYLOAD_BYTES, 1)),
                Some(reservation()),
            )
            .unwrap();
            let mut wrong = fragment(1, 2, total, vec![2]);
            wrong.seq += 1;
            assert!(push_fragment(&mut reordered, &wrong, None).is_err());

            let excessive_count = u32::try_from(MAX_FRAGMENTS + 1).unwrap();
            let mut excessive = None;
            assert!(push_fragment(
                &mut excessive,
                &fragment(0, excessive_count, 1, vec![1]),
                None,
            )
            .is_err());

            let mut oversized = None;
            assert!(push_fragment(
                &mut oversized,
                &fragment(0, 2, total, vec![1; MAX_WIRE_PAYLOAD_BYTES + 1],),
                None,
            )
            .is_err());
        }

        #[tokio::test]
        async fn concurrent_mid_fragment_streams_are_byte_bounded() {
            let budget = Arc::new(InboundBudget::new(INBOUND_NODE_BUDGET_BYTES));
            let wire_bytes = 3 * 1024 * 1024;
            let total = u32::try_from(wire_bytes).unwrap();
            let count = u32::try_from(wire_bytes / MAX_WIRE_PAYLOAD_BYTES).unwrap();
            let mut assemblies = Vec::new();
            for _ in 0..2 {
                let admitted = budget
                    .reserve_frame(9, wire_bytes, &CancellationToken::new())
                    .await
                    .unwrap();
                let mut assembly = None;
                push_fragment(
                    &mut assembly,
                    &fragment(
                        0,
                        count,
                        total,
                        padded_ipc_payload(MAX_WIRE_PAYLOAD_BYTES, 1),
                    ),
                    Some(admitted),
                )
                .unwrap();
                assemblies.push(assembly);
            }

            assert!(tokio::time::timeout(
                std::time::Duration::from_millis(20),
                budget.reserve_frame(9, wire_bytes, &CancellationToken::new()),
            )
            .await
            .is_err());

            assemblies.pop();
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                budget.reserve_frame(9, wire_bytes, &CancellationToken::new()),
            )
            .await
            .expect("released bytes unblock the peer")
            .unwrap();
        }
    }
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
        Holdover, ReceivedBatch, ReceivedShuffle, ShuffleMessage, ShufflePeerId, SHUFFLE_RECV_QUEUE,
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
        #[allow(clippy::unused_self)]
        pub fn set_recovery_gen(&self, _gen: u64) {}

        /// No peer fabric without the cluster feature; retain the clustered installation API.
        ///
        /// # Errors
        /// This networking-free implementation never errors.
        #[allow(clippy::unused_self)]
        pub fn install_assignment_fence(
            &self,
            _fence: &CheckpointAssignmentFence,
            _owners: &[ShufflePeerId],
        ) -> io::Result<bool> {
            Ok(false)
        }

        /// No cluster fabric exists, so there is no assignment authority to invalidate.
        #[allow(clippy::unused_self)]
        pub fn invalidate_assignment_fence(&self) {}

        /// No cluster fabric exists, so there is no assignment authority to suspend.
        #[allow(clippy::unused_self)]
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
        #[allow(clippy::unused_async)] // async to match the cluster build's API.
        #[allow(clippy::unused_self)]
        pub async fn register_peer(&self, _peer: ShufflePeerId, _addr: SocketAddr) {}

        /// # Errors
        /// Always errors because the no-cluster build has no shuffle transport.
        #[allow(clippy::unused_async)] // async to match the cluster build's API.
        pub async fn send_to(&self, peer: ShufflePeerId, _msg: &ShuffleMessage) -> io::Result<()> {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "node {} cannot send shuffle to peer {peer}: cluster transport is disabled",
                    self.local_id
                ),
            ))
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
        #[allow(clippy::unused_self)]
        pub fn set_recovery_gen(&self, _gen: u64) {}

        /// No peer fabric without the cluster feature; retain the clustered installation API.
        ///
        /// # Errors
        /// This networking-free implementation never errors.
        #[allow(clippy::unused_self)]
        pub fn install_assignment_fence(
            &self,
            _fence: &CheckpointAssignmentFence,
            _owners: &[ShufflePeerId],
        ) -> io::Result<bool> {
            Ok(false)
        }

        /// No cluster fabric exists, so there is no assignment authority to invalidate.
        #[allow(clippy::unused_self)]
        pub fn invalidate_assignment_fence(&self) {}

        /// No cluster fabric exists, so there is no assignment authority to suspend.
        #[allow(clippy::unused_self)]
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
        #[allow(clippy::unused_self)]
        pub fn delivery_loss_incidents(&self) -> Arc<std::sync::atomic::AtomicU64> {
            Arc::new(std::sync::atomic::AtomicU64::new(0))
        }

        /// No peer fabric exists, so no delivery-loss incident has required recovery.
        #[must_use]
        #[allow(clippy::unused_self)]
        pub fn recovered_delivery_loss_incidents(&self) -> Arc<std::sync::atomic::AtomicU64> {
            Arc::new(std::sync::atomic::AtomicU64::new(0))
        }

        /// No peer fabric exists, so no delivery loss can require recovery.
        #[must_use]
        #[allow(clippy::unused_self)]
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
                let rx = guard.rx.as_mut()?;
                return rx.recv().await.ok();
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
            if !self.holdover.staged_barriers.lock().is_empty() {
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
                        ShuffleMessage::Data { stage, batch, .. } => {
                            staged.entry(stage).or_default().push(ReceivedBatch {
                                batch,
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
                        ShuffleMessage::Barrier(barrier) => {
                            self.holdover.staged_barriers.lock().push(ReceivedShuffle {
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
                            });
                            break;
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
            let barriers = std::mem::take(&mut *self.holdover.staged_barriers.lock());
            self.holdover.release_items(barriers.len());
            barriers
        }

        /// Empty the per-stage holdover, returning every buffered `(stage, batch)`.
        #[must_use]
        pub fn drain_all_staged(&self) -> Vec<(String, ReceivedBatch)> {
            let mut staged = self.holdover.staged.lock();
            let item_count = staged.values().map(Vec::len).sum();
            let drained = staged
                .drain()
                .flat_map(|(stage, batches)| {
                    batches
                        .into_iter()
                        .map(move |staged| (stage.clone(), staged))
                })
                .collect();
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
mod shim_tests {
    use std::io;

    use super::*;
    use crate::checkpoint::CheckpointBarrier;

    #[tokio::test]
    async fn registered_peer_still_fails_when_transport_is_disabled() {
        let sender = ShuffleSender::new(1, uuid::Uuid::from_u128(2));
        sender
            .register_peer(2, "127.0.0.1:9000".parse().unwrap())
            .await;

        let error = sender
            .send_to(2, &ShuffleMessage::Barrier(CheckpointBarrier::new(1, 1)))
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }
}

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use std::io;
    use std::sync::Arc;

    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;
    use crate::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier, CheckpointParticipant};
    use uuid::Uuid;

    fn assignment_owners(nodes: &[ShufflePeerId]) -> Vec<ShufflePeerId> {
        let mut participants = nodes.to_vec();
        participants.sort_unstable();
        participants.dedup();
        let owner = participants
            .iter()
            .copied()
            .find(|node| *node == 2)
            .or_else(|| participants.last().copied())
            .expect("test assignment has a participant");
        vec![owner; 8]
    }

    fn assignment_fence(version: u64, nodes: &[ShufflePeerId]) -> CheckpointAssignmentFence {
        let mut nodes = nodes.to_vec();
        nodes.sort_unstable();
        nodes.dedup();
        let owners = assignment_owners(&nodes);
        CheckpointAssignmentFence::from_owner_map(
            version,
            &owners,
            nodes
                .iter()
                .copied()
                .map(|node_id| CheckpointParticipant {
                    node_id,
                    boot_incarnation: Uuid::from_u128(u128::from(node_id) + 1),
                })
                .collect(),
        )
        .unwrap()
    }

    async fn send_barrier(
        sender: &ShuffleSender,
        peers: &[ShufflePeerId],
        barrier: CheckpointBarrier,
    ) -> io::Result<()> {
        let mut nodes = peers.to_vec();
        nodes.push(sender.local_id());
        let fence = assignment_fence(sender.assignment_version(), &nodes);
        sender.fan_out_barrier(peers, barrier, &fence).await
    }

    async fn bind_on_loopback_with_incarnation(
        local_id: ShufflePeerId,
        incarnation: Uuid,
    ) -> ShuffleReceiver {
        let receiver = ShuffleReceiver::bind(local_id, "127.0.0.1:0".parse().unwrap(), incarnation)
            .await
            .expect("bind");
        let mut nodes = vec![1, local_id];
        if local_id == 1 {
            nodes.push(2);
        }
        nodes.sort_unstable();
        nodes.dedup();
        let mut fence = assignment_fence(1, &nodes);
        let participant = fence
            .participants
            .iter_mut()
            .find(|participant| participant.node_id == local_id)
            .expect("receiver belongs to its test assignment");
        participant.boot_incarnation = incarnation;
        receiver
            .install_assignment_fence(&fence, &assignment_owners(&nodes))
            .unwrap();
        receiver
    }

    async fn bind_on_loopback(local_id: ShufflePeerId) -> ShuffleReceiver {
        bind_on_loopback_with_incarnation(local_id, Uuid::from_u128(u128::from(local_id) + 1)).await
    }

    fn sender(local_id: ShufflePeerId) -> ShuffleSender {
        let sender = ShuffleSender::new(local_id, Uuid::from_u128(u128::from(local_id) + 1));
        let fence = assignment_fence(1, &[1, 2]);
        sender
            .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
            .unwrap();
        sender
    }

    fn one_row(value: i64) -> arrow_array::RecordBatch {
        arrow_array::RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap()
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shuffle state did not settle");
    }

    #[test]
    fn shuffle_assignment_admission_rejects_oversized_roster() {
        let maximum = u64::try_from(crate::checkpoint::MAX_CHECKPOINT_PARTICIPANTS).unwrap();
        let nodes = (1..=maximum).collect::<Vec<_>>();
        let mut forged = assignment_fence(1, &nodes);
        forged.participants.push(CheckpointParticipant {
            node_id: maximum + 1,
            boot_incarnation: Uuid::from_u128(u128::from(maximum + 1) + 1),
        });
        let sender = ShuffleSender::new(1, Uuid::from_u128(2));

        assert_eq!(
            sender
                .install_assignment_fence(&forged, &assignment_owners(&nodes))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(sender.assignment_version(), 0);
    }

    #[test]
    fn shuffle_assignment_admission_rejects_an_unbound_owner_vector() {
        let fence = assignment_fence(1, &[1, 2]);
        let sender = ShuffleSender::new(1, Uuid::from_u128(2));

        let error = sender
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(sender.assignment_version(), 0);
    }

    #[test]
    fn shuffle_assignment_admission_rejects_a_partitioning_abi_mismatch() {
        let owners = assignment_owners(&[1, 2]);
        let mut fence = assignment_fence(1, &[1, 2]);
        fence.partitioning_abi_version = crate::state::PARTITIONING_ABI_VERSION.saturating_add(1);
        let sender = ShuffleSender::new(1, Uuid::from_u128(2));

        let error = sender
            .install_assignment_fence(&fence, &owners)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(sender.assignment_version(), 0);
    }

    #[test]
    fn same_version_certificate_replacement_is_rejected_even_while_invalidated() {
        let sender = sender(1);
        let installed = assignment_fence(1, &[1, 2]);
        let conflicting =
            CheckpointAssignmentFence::from_owner_map(1, &[2, 1], installed.participants.clone())
                .unwrap();

        assert_eq!(
            sender
                .install_assignment_fence(&conflicting, &[2, 1])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        sender.invalidate_assignment_fence();
        assert_eq!(sender.assignment_version(), 0);
        assert_eq!(
            sender
                .install_assignment_fence(&conflicting, &[2, 1])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            sender
                .install_assignment_fence(&installed, &assignment_owners(&[1, 2]))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            sender.assignment_version(),
            0,
            "an invalidated version cannot be reactivated"
        );
        let successor = assignment_fence(2, &[1, 2]);
        assert!(sender
            .install_assignment_fence(&successor, &assignment_owners(&[1, 2]))
            .unwrap());
        assert_eq!(sender.assignment_version(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn transient_suspension_reactivates_exact_certificate_without_resetting_sequence() {
        use std::sync::atomic::Ordering as O;

        let fence = assignment_fence(1, &[1, 2]);
        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;

        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        let _ = receiver.recv().await.unwrap();

        sender.suspend_assignment_fence();
        receiver.suspend_assignment_fence();
        assert_eq!(sender.assignment_version(), 0);
        assert_eq!(receiver.assignment_version(), 0);
        let owners = assignment_owners(&[1, 2]);
        assert!(sender.install_assignment_fence(&fence, &owners).unwrap());
        assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());

        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(2)),
            )
            .await
            .unwrap();
        let received = receiver.recv().await.unwrap();
        assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
        assert_eq!(
            receiver.delivery_loss_incidents().load(O::Acquire),
            0,
            "same-version reactivation must preserve the sender and receiver sequence domain"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_churn_prunes_sender_peer_and_connection_state() {
        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        for peer in 2..=64 {
            sender.register_peer(peer, receiver.local_addr()).await;
        }
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        let _ = receiver.recv().await.unwrap();
        let before = sender.tracked_resources_for_test();
        assert_eq!(before.0, 63);
        assert!(before.1 > 0 && before.2 > 0 && before.3 > 0);

        for version in 2..=64 {
            let peer = version + 100;
            sender.register_peer(peer, receiver.local_addr()).await;
            let fence = assignment_fence(version, &[1, peer]);
            assert!(sender
                .install_assignment_fence(&fence, &assignment_owners(&[1, peer]))
                .unwrap());
            assert_eq!(sender.tracked_resources_for_test(), (1, 0, 0, 0));
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queued_envelope_survives_exact_same_version_suspension() {
        use std::sync::atomic::Ordering as O;

        let fence = assignment_fence(1, &[1, 2]);
        let owners = assignment_owners(&[1, 2]);
        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;

        let entered = receiver.pause_next_recv_after_defer_for_test();
        let mut blocked_recv = Box::pin(receiver.recv());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::select! {
                () = entered.notified() => {}
                received = &mut blocked_recv => {
                    panic!("receive completed before deferral: {received:?}");
                }
            }
        })
        .await
        .expect("receive did not enter the deferred slot");

        let mut competing_recv = Box::pin(receiver.recv());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut competing_recv)
                .await
                .is_err(),
            "a competing receive bypassed the receiver lease"
        );
        sender.suspend_assignment_fence();
        receiver.suspend_assignment_fence();
        drop(blocked_recv);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut competing_recv)
                .await
                .is_err(),
            "a competing receive consumed the deferred frame while suspended"
        );
        drop(competing_recv);
        assert!(sender.install_assignment_fence(&fence, &owners).unwrap());
        assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());

        let received = tokio::time::timeout(std::time::Duration::from_secs(1), receiver.recv())
            .await
            .expect("cancelled suspended receive did not retain its envelope")
            .expect("shuffle queue closed");
        assert!(matches!(received.message(), ShuffleMessage::Data { .. }));
        drop(received);

        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("state".into(), 0, one_row(2)),
            )
            .await
            .unwrap();
        wait_until(|| receiver.committed_sequence_for_test(1) == Some(2)).await;
        sender.suspend_assignment_fence();
        receiver.suspend_assignment_fence();
        assert!(sender.install_assignment_fence(&fence, &owners).unwrap());
        assert!(receiver.install_assignment_fence(&fence, &owners).unwrap());

        let drained = receiver.drain_checkpointed_staged();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, "state");
        assert_eq!(drained[0].1.batch().num_rows(), 1);
        assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn superseded_scope_cancels_blocked_outbound_budget_without_switching_scope() {
        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;
        let held_budget = sender.hold_outbound_budget_for_test(2).await.unwrap();
        let message = ShuffleMessage::checkpointed("stage".into(), 0, one_row(1));
        let mut blocked = Box::pin(sender.send_to_for_assignment(2, 1, &message));

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut blocked)
                .await
                .is_err()
        );

        let successor = assignment_fence(2, &[1, 2]);
        let owners = assignment_owners(&[1, 2]);
        assert!(receiver
            .install_assignment_fence(&successor, &owners)
            .unwrap());
        assert!(sender
            .install_assignment_fence(&successor, &owners)
            .unwrap());

        let error = tokio::time::timeout(std::time::Duration::from_secs(1), &mut blocked)
            .await
            .expect("superseded outbound admission remained blocked")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);

        sender
            .send_to_for_assignment(
                2,
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(2)),
            )
            .await
            .unwrap();
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.assignment_version(), 2);
        drop(held_budget);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_scope_cancellation_releases_idle_inbound_stream_slot() {
        use std::sync::atomic::Ordering as O;

        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        let _ = receiver.recv().await.unwrap();
        wait_until(|| receiver.active_streams_for_test() == 1).await;

        receiver.set_recovery_gen(1);

        wait_until(|| receiver.active_streams_for_test() == 0).await;
        assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_to_receiver_delivers_with_peer_attribution() {
        let recv = bind_on_loopback(2).await;
        let recv_addr = recv.local_addr();

        let sender = sender(1);
        sender.register_peer(2, recv_addr).await;
        send_barrier(&sender, &[2], CheckpointBarrier::new(1234, 1))
            .await
            .unwrap();

        let received = recv.recv().await.unwrap();
        let from = received.peer();
        assert_eq!(from, 1, "receiver attributes frame to sender id");
        assert_eq!(
            received.message(),
            &ShuffleMessage::Barrier(CheckpointBarrier::new(1234, 1))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn uncertified_network_handshakes_are_rejected_without_reporting_data_loss() {
        use super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
        use super::shuffle_v1::HandshakeRequest;

        let receiver = bind_on_loopback(2).await;
        let fence = assignment_fence(1, &[1, 2]);
        let endpoint =
            crate::cluster::control::tls::client_endpoint(&receiver.local_addr().to_string())
                .unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut client = ShuffleTransportClient::new(channel);
        let stream_id = Uuid::new_v4();
        let error = client
            .handshake(tonic::Request::new(HandshakeRequest {
                sender_node_id: 1,
                sender_incarnation: Uuid::from_u128(999).as_bytes().to_vec(),
                stream_id: stream_id.as_bytes().to_vec(),
                assignment_version: fence.assignment_version,
                recovery_gen: 0,
                assignment_certificate_digest: fence.digest().to_vec(),
            }))
            .await
            .unwrap_err();

        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        let stream_id = Uuid::new_v4();
        let error = client
            .handshake(tonic::Request::new(HandshakeRequest {
                sender_node_id: 1,
                sender_incarnation: Uuid::from_u128(2).as_bytes().to_vec(),
                stream_id: stream_id.as_bytes().to_vec(),
                assignment_version: fence.assignment_version,
                recovery_gen: 0,
                assignment_certificate_digest: [9; 32].to_vec(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            receiver
                .delivery_loss_incidents()
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "traffic rejected before stream admission is not delivered-data loss"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receiver_restart_is_rejected_until_recovery_generation_advances() {
        let receiver_v1 = bind_on_loopback(2).await;
        let receiver_v1_incarnation = receiver_v1.incarnation();
        let sender = sender(1);
        sender.register_peer(2, receiver_v1.local_addr()).await;

        for value in [10, 20] {
            sender
                .send_to(
                    2,
                    &ShuffleMessage::checkpointed(
                        "pipeline/stage/input-0".into(),
                        0,
                        one_row(value),
                    ),
                )
                .await
                .unwrap();
            let received = receiver_v1.recv().await.unwrap();
            let ShuffleMessage::Data { batch, .. } = received.message() else {
                panic!("expected data frame")
            };
            assert_eq!(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                value
            );
        }

        sender.disconnect_peer_for_test(2);
        drop(receiver_v1);
        let receiver_v2 = bind_on_loopback_with_incarnation(2, Uuid::from_u128(300)).await;
        sender.register_peer(2, receiver_v2.local_addr()).await;
        let error = sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(30)),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver_v2.recv())
                .await
                .is_err(),
            "same-generation receiver restart must not fold data"
        );

        let restarted_owners = assignment_owners(&[1, 2]);
        let stale_receiver_fence = CheckpointAssignmentFence::from_owner_map(
            2,
            &restarted_owners,
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: sender.incarnation(),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: receiver_v1_incarnation,
                },
            ],
        )
        .unwrap();
        assert_eq!(
            receiver_v2
                .install_assignment_fence(&stale_receiver_fence, &restarted_owners)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        let restarted_fence = CheckpointAssignmentFence::from_owner_map(
            2,
            &restarted_owners,
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: sender.incarnation(),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: receiver_v2.incarnation(),
                },
            ],
        )
        .unwrap();
        receiver_v2
            .install_assignment_fence(&restarted_fence, &restarted_owners)
            .unwrap();
        sender
            .install_assignment_fence(&restarted_fence, &restarted_owners)
            .unwrap();
        receiver_v2.set_recovery_gen(1);
        sender.set_recovery_gen(1);
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(30)),
            )
            .await
            .unwrap();
        let received = receiver_v2.recv().await.unwrap();
        let ShuffleMessage::Data { batch, .. } = received.message() else {
            panic!("expected data frame after receiver restart")
        };
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            30
        );

        sender
            .fan_out_barrier(&[2], CheckpointBarrier::new(1, 1), &restarted_fence)
            .await
            .unwrap();
        assert!(matches!(
            receiver_v2.recv().await.unwrap().message(),
            ShuffleMessage::Barrier(_)
        ));
        assert_eq!(
            receiver_v2
                .delivery_loss_incidents()
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "post-recovery receiver restart must reset the sender sequence exactly once"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_senders_preserve_sequence_and_every_record() {
        let receiver = bind_on_loopback(2).await;
        let sender = Arc::new(sender(1));
        sender.register_peer(2, receiver.local_addr()).await;

        let mut tasks = Vec::new();
        for value in 0..128i64 {
            let sender = Arc::clone(&sender);
            tasks.push(tokio::spawn(async move {
                sender
                    .send_to(
                        2,
                        &ShuffleMessage::checkpointed(
                            "pipeline/stage/input-0".into(),
                            0,
                            one_row(value),
                        ),
                    )
                    .await
            }));
        }
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
            .await
            .unwrap();

        let mut values = std::collections::BTreeSet::new();
        loop {
            let envelope = receiver.recv().await.unwrap();
            let ShuffleMessage::Data { batch, .. } = envelope.message() else {
                break;
            };
            values.insert(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
            );
        }
        assert_eq!(values, (0..128i64).collect());
        assert_eq!(
            receiver
                .delivery_loss_incidents()
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "concurrent enqueue order must equal assigned sequence order"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stale_assignment_stream_is_rejected_before_folding() {
        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap().message(),
            ShuffleMessage::Data { .. }
        ));

        let next_assignment = assignment_fence(2, &[1, 2]);
        receiver
            .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
            .unwrap();
        let _ = sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(999)),
            )
            .await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
                .await
                .is_err()
        );
        assert_eq!(
            receiver
                .delivery_loss_incidents()
                .load(std::sync::atomic::Ordering::Acquire),
            0,
            "a deliberate assignment transition is not transit loss"
        );

        sender
            .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
            .unwrap();
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(2)),
            )
            .await
            .unwrap();
        let envelope = receiver.recv().await.unwrap();
        let ShuffleMessage::Data { batch, .. } = envelope.message() else {
            panic!("expected current-assignment data")
        };
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_guard_rejects_stale_routing_before_enqueue() {
        let receiver = bind_on_loopback(2).await;
        let next_assignment = assignment_fence(2, &[1, 2]);
        receiver
            .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
            .unwrap();
        let sender = sender(1);
        sender
            .install_assignment_fence(&next_assignment, &assignment_owners(&[1, 2]))
            .unwrap();
        sender.register_peer(2, receiver.local_addr()).await;

        let error = sender
            .send_to_for_assignment(
                2,
                1,
                &ShuffleMessage::checkpointed("pipeline/stage/input-0".into(), 0, one_row(999)),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::ConnectionAborted);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), receiver.recv())
                .await
                .is_err(),
            "data routed with an old assignment must not enter the outbound queue"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn self_shuffle_is_rejected_before_connection_or_enqueue() {
        let receiver = bind_on_loopback(1).await;
        let sender = sender(1);
        sender.register_peer(1, receiver.local_addr()).await;

        let error = sender
            .send_to(
                1,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), receiver.recv())
                .await
                .is_err()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn barrier_holdover_is_bounded_faults_the_cut_and_reopens_after_drain() {
        use std::sync::atomic::Ordering as O;

        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;

        for checkpoint_id in 0..=SHUFFLE_RECV_QUEUE {
            send_barrier(
                &sender,
                &[2],
                CheckpointBarrier::new(u64::try_from(checkpoint_id + 1).unwrap(), 1),
            )
            .await
            .unwrap();
            let received = receiver.recv().await.unwrap();
            receiver.stash_barrier(received);
        }

        assert_eq!(
            receiver.delivery_loss_incidents().load(O::Acquire),
            1,
            "the first barrier beyond the holdover bound must fault the cut"
        );
        assert_eq!(receiver.drain_staged_barriers().len(), SHUFFLE_RECV_QUEUE);

        send_barrier(
            &sender,
            &[2],
            CheckpointBarrier::new(u64::try_from(SHUFFLE_RECV_QUEUE + 2).unwrap(), 1),
        )
        .await
        .unwrap();
        receiver.stash_barrier(receiver.recv().await.unwrap());
        assert_eq!(receiver.drain_staged_barriers().len(), 1);
        assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn data_holdover_is_bounded_and_backpressures_without_loss() {
        use std::sync::atomic::Ordering as O;

        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;

        for value in 0..=SHUFFLE_RECV_QUEUE {
            sender
                .send_to(
                    2,
                    &ShuffleMessage::checkpointed(
                        format!("stage-{value}"),
                        0,
                        one_row(i64::try_from(value).unwrap()),
                    ),
                )
                .await
                .unwrap();
        }
        wait_until(|| receiver.committed_sequence_for_test(1) == Some(SHUFFLE_RECV_QUEUE as u64))
            .await;
        assert!(receiver.drain_checkpointed_data_for("absent").is_empty());
        assert_eq!(receiver.drain_all_staged().len(), SHUFFLE_RECV_QUEUE);
        wait_until(|| {
            receiver.committed_sequence_for_test(1)
                == Some(u64::try_from(SHUFFLE_RECV_QUEUE + 1).unwrap())
        })
        .await;
        assert_eq!(
            receiver
                .drain_checkpointed_data_for(&format!("stage-{SHUFFLE_RECV_QUEUE}"))
                .len(),
            1
        );
        assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn staged_old_generation_is_discarded_without_refaulting_recovery() {
        use std::sync::atomic::Ordering as O;

        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;
        assert!(receiver.drain_checkpointed_data_for("other").is_empty());

        receiver.set_recovery_gen(1);

        assert!(receiver.drain_checkpointed_data_for("stage").is_empty());
        assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn staged_old_assignment_faults_instead_of_reaching_an_operator() {
        use std::sync::atomic::Ordering as O;

        let receiver = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, receiver.local_addr()).await;
        sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap();
        wait_until(|| receiver.committed_sequence_for_test(1) == Some(1)).await;
        assert!(receiver.drain_checkpointed_data_for("other").is_empty());

        let successor = assignment_fence(2, &[1, 2]);
        receiver
            .install_assignment_fence(&successor, &assignment_owners(&[1, 2]))
            .unwrap();

        assert!(receiver.drain_checkpointed_data_for("stage").is_empty());
        assert_eq!(receiver.delivery_loss_incidents().load(O::Acquire), 1);
    }

    /// A receiver past a coordinated rewind rejects the whole old-generation stream. Accepting
    /// even its barrier could align a checkpoint around data the receiver intentionally discarded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receiver_rejects_pre_rewind_stream_until_sender_catches_up() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();

        // Receiver rewinds to generation 5; the sender is still stamping generation 0.
        recv.set_recovery_gen(5);
        let error = sender
            .send_to(
                2,
                &ShuffleMessage::checkpointed("s".into(), 0, batch.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        let error = send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), recv.recv())
                .await
                .is_err()
        );

        // Once the sender catches up to the receiver's generation, data flows again.
        sender.set_recovery_gen(5);
        sender
            .send_to(2, &ShuffleMessage::checkpointed("s".into(), 0, batch))
            .await
            .unwrap();
        let received = recv.recv().await.unwrap();
        assert!(
            matches!(received.message(), ShuffleMessage::Data { .. }),
            "same-generation data frame must be delivered; got {received:?}"
        );
    }

    /// A frame discarded on reconnect or mid-stream must not vanish silently. The sequence makes
    /// the hole visible at the receiver.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receiver_records_a_delivery_loss_incident() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::atomic::Ordering as O;

        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;
        let incidents = recv.delivery_loss_incidents();

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let data = || ShuffleMessage::checkpointed("s".into(), 0, batch.clone());

        // Frame 0 lands. Frame 1 is "lost": burn its sequence without sending it.
        sender.send_to(2, &data()).await.unwrap();
        let _ = recv.recv().await.unwrap();
        sender.burn_seq_for_test(2);
        assert_eq!(
            incidents.load(O::Acquire),
            0,
            "no loss until the hole is observed"
        );

        // Frame 2 arrives with a gap where frame 1 should have been.
        sender.send_to(2, &data()).await.unwrap();
        let _ = recv.recv().await.unwrap();
        assert_eq!(
            incidents.load(O::Acquire),
            1,
            "the gap must record exactly one loss incident"
        );
    }

    #[tokio::test]
    async fn recovery_resolves_only_the_loss_cutoff_captured_for_its_generation() {
        use std::sync::atomic::Ordering as O;

        let recv = bind_on_loopback(2).await;
        let incidents = recv.delivery_loss_incidents();
        let recovered_incidents = recv.recovered_delivery_loss_incidents();

        incidents.store(2, O::Release);
        recv.set_recovery_gen(5);
        // This loss happened after the rewind began and must not be forgiven by generation 5.
        incidents.store(3, O::Release);

        assert!(!recv.complete_recovery(4));
        assert!(recv.complete_recovery(5));
        assert!(recv.complete_recovery(5), "completion must be idempotent");
        assert_eq!(recovered_incidents.load(O::Acquire), 2);

        recv.set_recovery_gen(6);
        incidents.store(4, O::Release);
        assert!(recv.complete_recovery(6));
        assert_eq!(recovered_incidents.load(O::Acquire), 3);
    }

    /// Re-publishing the current (or an older) recovery generation is not a rewind. Resetting
    /// delivery continuity here would let a lost frame disappear before the next checkpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repeated_current_recovery_generation_does_not_hide_sequence_gap() {
        use std::sync::atomic::Ordering as O;

        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;
        let incidents = recv.delivery_loss_incidents();

        recv.set_recovery_gen(5);
        sender.set_recovery_gen(5);
        sender
            .send_to(2, &ShuffleMessage::checkpointed("s".into(), 0, one_row(1)))
            .await
            .unwrap();
        let _ = recv.recv().await.unwrap();

        sender.burn_seq_for_test(2);
        recv.set_recovery_gen(5);
        recv.set_recovery_gen(4);
        sender
            .send_to(2, &ShuffleMessage::checkpointed("s".into(), 0, one_row(2)))
            .await
            .unwrap();
        let _ = recv.recv().await.unwrap();

        assert_eq!(
            incidents.load(O::Acquire),
            1,
            "equal or stale generation updates must not hide the sequence gap"
        );
    }

    /// The trailing-loss case the data-gap check cannot see: the last frames of an epoch are
    /// dropped and nothing follows them. The barrier's high-water mark exposes it while the
    /// checkpoint can still be fenced.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn barrier_high_water_mark_catches_trailing_loss() {
        use std::sync::atomic::Ordering as O;

        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;
        let incidents = recv.delivery_loss_incidents();

        // Two data frames enqueued, both dropped in transit; then the epoch's barrier.
        sender.burn_seq_for_test(2);
        sender.burn_seq_for_test(2);
        send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
            .await
            .unwrap();
        let received = recv.recv().await.unwrap();
        assert!(matches!(received.message(), ShuffleMessage::Barrier(_)));
        assert_eq!(
            incidents.load(O::Acquire),
            1,
            "the barrier must reveal frames that never arrived before the epoch seals"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_generation_starts_at_zero_and_detects_a_missing_first_frame() {
        use std::sync::atomic::Ordering as O;

        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        recv.set_recovery_gen(1);
        sender.set_recovery_gen(1);
        sender.register_peer(2, recv.local_addr()).await;

        // Sequence zero is allocated but never reaches the receiver. A new recovery Hello must
        // not rebaseline at the barrier's high-water mark.
        sender.burn_seq_for_test(2);
        send_barrier(&sender, &[2], CheckpointBarrier::new(1, 1))
            .await
            .unwrap();

        assert!(matches!(
            recv.recv().await.unwrap().message(),
            ShuffleMessage::Barrier(_)
        ));
        assert_eq!(
            recv.delivery_loss_incidents().load(O::Acquire),
            1,
            "recovery must retain a zero-based sequence domain"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn saturated_checkpointed_data_budget_does_not_block_barrier_control_admission() {
        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;
        let _held = sender.hold_outbound_budget_for_test(2).await.unwrap();

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            send_barrier(&sender, &[2], CheckpointBarrier::new(8, 4)),
        )
        .await
        .expect("checkpoint barrier waited on the data byte semaphore")
        .unwrap();

        let received = tokio::time::timeout(std::time::Duration::from_secs(1), recv.recv())
            .await
            .expect("checkpoint barrier did not reach the ordered stream")
            .expect("ordered stream closed");
        assert!(matches!(received.message(), ShuffleMessage::Barrier(_)));
    }

    /// A rewind deliberately discards in-flight frames, and a restarted peer restarts its
    /// sequence at zero. Neither is transit loss.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rewind_and_restart_do_not_count_as_loss() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};
        use std::sync::atomic::Ordering as O;

        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;
        let incidents = recv.delivery_loss_incidents();

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let data = || ShuffleMessage::checkpointed("s".into(), 0, batch.clone());

        sender.send_to(2, &data()).await.unwrap();
        let _ = recv.recv().await.unwrap();

        // A round discards whatever was queued, then both ends move to the new generation.
        sender.burn_seq_for_test(2);
        sender.burn_seq_for_test(2);
        recv.set_recovery_gen(9);
        sender.set_recovery_gen(9);

        sender.send_to(2, &data()).await.unwrap();
        let _ = recv.recv().await.unwrap();
        assert_eq!(
            incidents.load(O::Acquire),
            0,
            "a rewind's deliberate discards are not transit loss"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_reuses_stream_across_sends() {
        let recv = bind_on_loopback(2).await;
        let sender = sender(1);
        sender.register_peer(2, recv.local_addr()).await;

        for delta in [10u64, 20, 30, 40] {
            send_barrier(&sender, &[2], CheckpointBarrier::new(delta, 1))
                .await
                .unwrap();
        }

        let mut got = Vec::new();
        for _ in 0..4 {
            let received = recv.recv().await.unwrap();
            got.push(received.message().clone());
        }
        assert_eq!(
            got,
            vec![
                ShuffleMessage::Barrier(CheckpointBarrier::new(10, 1)),
                ShuffleMessage::Barrier(CheckpointBarrier::new(20, 1)),
                ShuffleMessage::Barrier(CheckpointBarrier::new(30, 1)),
                ShuffleMessage::Barrier(CheckpointBarrier::new(40, 1)),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_to_uncertified_peer_errors() {
        let sender = sender(1);
        let err = sender
            .send_to(
                99,
                &ShuffleMessage::checkpointed("stage".into(), 0, one_row(1)),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn barrier_fan_out_attempts_all_peers_but_rejects_a_partial_cut() {
        let receiver = bind_on_loopback(10).await;
        let sender = sender(1);
        sender.register_peer(10, receiver.local_addr()).await;
        let barrier = CheckpointBarrier::new(5, 1);

        let fence = assignment_fence(2, &[1, 10, 99]);
        receiver
            .install_assignment_fence(&fence, &assignment_owners(&[1, 10, 99]))
            .unwrap();
        sender
            .install_assignment_fence(&fence, &assignment_owners(&[1, 10, 99]))
            .unwrap();
        let error = sender
            .fan_out_barrier(&[99, 10], barrier, &fence)
            .await
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), receiver.recv())
            .await
            .expect("reachable peer did not receive its barrier")
            .expect("shuffle receiver closed");
        assert!(matches!(
            received.message(),
            ShuffleMessage::Barrier(received) if *received == barrier
        ));
    }

    #[tokio::test]
    async fn barrier_fan_out_rejects_an_incomplete_or_duplicate_roster() {
        let sender = sender(1);
        let fence = assignment_fence(1, &[1, 2]);
        let barrier = CheckpointBarrier::new(5, 1);

        for peers in [&[][..], &[2, 2][..]] {
            let error = sender
                .fan_out_barrier(peers, barrier, &fence)
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_discovers_peer_address_from_kv() {
        use crate::cluster::control::{ClusterKv, InMemoryKv};
        use crate::cluster::discovery::NodeId;

        // Peer 2 binds for real; its address is seeded into peer 1's KV so the
        // KV-backed sender resolves it on first send without an explicit
        // `register_peer`. End-to-end delivery proves the discovery glue.
        let recv = bind_on_loopback(2).await;
        let kv = Arc::new(InMemoryKv::new(NodeId(1)));
        kv.seed(NodeId(2), SHUFFLE_ADDR_KEY, recv.local_addr().to_string());
        let sender = ShuffleSender::with_kv(1, kv as Arc<dyn ClusterKv>, Uuid::from_u128(2));
        let fence = assignment_fence(1, &[1, 2]);
        sender
            .install_assignment_fence(&fence, &assignment_owners(&[1, 2]))
            .unwrap();

        send_barrier(&sender, &[2], CheckpointBarrier::new(7, 1))
            .await
            .unwrap();
        let received = recv.recv().await.unwrap();
        assert_eq!(received.peer(), 1);
        assert_eq!(
            received.message(),
            &ShuffleMessage::Barrier(CheckpointBarrier::new(7, 1))
        );
    }
}
