//! Cross-node shuffle transport over Tonic gRPC client-streaming.
//!
//! Each sender opens a client-streaming `Shuffle` RPC per peer and pushes a
//! forward-only stream of `ShuffleFrame`s; the receiver runs the
//! `ShuffleTransport` service, attributes every stream to the peer announced in
//! its leading `Hello`, and surfaces decoded [`ShuffleMessage`]s on a bounded
//! crossfire MPSC queue. Backpressure is the HTTP/2 flow-control window plus
//! that bounded queue. See [`super::message`] for the per-frame payloads and
//! [`crate::serialization`] for the Arrow IPC (de)serialization of `VnodeData`.
//!
//! The real gRPC path is compiled under the `cluster` feature (which
//! pulls in `tonic`/`prost`). A default build keeps the same public API via a
//! networking-free shim so the types referenced by `laminar-db`/`laminar-server`
//! signatures still compile without the cluster dependencies.

use super::message::ShuffleMessage;
use crate::checkpoint::barrier::CheckpointBarrier;

/// Bounded capacity for the inbound shuffle queue. One consumer per
/// [`ShuffleReceiver`] (the cluster repartition dispatcher) drains it; a slow
/// consumer parks the per-stream service handler on the bounded `send`, so
/// backpressure flows back over HTTP/2 to the sender.
const SHUFFLE_RECV_QUEUE: usize = 1024;

/// Peer-local identifier on the wire. Matches `cluster::discovery::NodeId`'s
/// inner type for seamless conversion.
pub type ShufflePeerId = u64;

/// Gossip KV key used by [`ShuffleReceiver::bind_with_kv`] to publish the
/// listener's socket address, and by [`ShuffleSender`] to discover peer
/// addresses on first contact. Value: the bound socket address formatted via
/// `SocketAddr::to_string()`.
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

// ---------------------------------------------------------------------------
// Per-stage / per-barrier holdover shared by both builds.
// ---------------------------------------------------------------------------

/// Inbound-side holdover state lifted out of [`ShuffleReceiver`] so both the
/// gRPC and default builds share the staging semantics that barrier alignment
/// depends on: frames pulled for another stage are bucketed for that stage's own
/// drainer, and barriers pulled mid-cycle are stashed (never dropped) for the
/// aligning checkpoint.
#[derive(Default)]
struct Holdover {
    staged: parking_lot::Mutex<rustc_hash::FxHashMap<String, Vec<arrow_array::RecordBatch>>>,
    staged_barriers: parking_lot::Mutex<Vec<(ShufflePeerId, CheckpointBarrier)>>,
}

// ===========================================================================
// gRPC implementation (cluster).
// ===========================================================================

#[cfg(feature = "cluster")]
mod grpc {
    use std::io;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use crossfire::{mpsc, AsyncRx, MAsyncTx};
    use futures::StreamExt as _;
    use parking_lot::Mutex;
    use rustc_hash::FxHashMap;
    use tokio::task::JoinHandle;
    use tonic::transport::{Channel, Server};
    use tonic::Request;

    use super::shuffle_v1::shuffle_frame;
    use super::shuffle_v1::shuffle_transport_client::ShuffleTransportClient;
    use super::shuffle_v1::shuffle_transport_server::{ShuffleTransport, ShuffleTransportServer};
    use super::shuffle_v1::{Barrier, Close, Hello, ShuffleFrame, ShuffleSummary, VnodeData};
    use super::{Holdover, ShuffleMessage, ShufflePeerId, SHUFFLE_ADDR_KEY, SHUFFLE_RECV_QUEUE};
    use crate::checkpoint::barrier::CheckpointBarrier;
    use crate::cluster::control::ClusterKv;
    use crate::serialization::{deserialize_batch_stream, serialize_batch_stream};

    /// Outbound queue capacity per peer. Bounds per-peer buffering before the
    /// HTTP/2 window applies its own backpressure.
    const SHUFFLE_SEND_QUEUE: usize = 1024;

    /// Inbound queue item flavor (kept as a `type` so the parked-behind-mutex
    /// receiver field doesn't trip clippy's `type_complexity`).
    type InboundRx = AsyncRx<mpsc::Array<(ShufflePeerId, ShuffleMessage)>>;
    type InboundTx = MAsyncTx<mpsc::Array<(ShufflePeerId, ShuffleMessage)>>;

    /// Map a `tonic::Status` / `tonic::transport::Error` (or any `Display`) into
    /// `io::Error` so the public API keeps its `io::Result` shape.
    fn io_err<E: std::fmt::Display>(e: E) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Encode a [`ShuffleMessage`] into the wire [`ShuffleFrame`]. Fails only on
    /// Arrow IPC encoding of a `VnodeData` batch.
    fn to_frame(msg: &ShuffleMessage) -> Result<ShuffleFrame, tonic::Status> {
        let kind = match msg {
            ShuffleMessage::Hello(node_id) => {
                shuffle_frame::Kind::Hello(Hello { node_id: *node_id })
            }
            ShuffleMessage::Barrier(b) => shuffle_frame::Kind::Barrier(Barrier {
                checkpoint_id: b.checkpoint_id,
                epoch: b.epoch,
                flags: b.flags,
            }),
            ShuffleMessage::VnodeData(stage, vnode, batch) => {
                let arrow_ipc = serialize_batch_stream(batch)
                    .map_err(|e| tonic::Status::internal(format!("shuffle ipc encode: {e}")))?;
                shuffle_frame::Kind::VnodeData(VnodeData {
                    stage: stage.clone(),
                    vnode: *vnode,
                    arrow_ipc,
                })
            }
            ShuffleMessage::Close(reason) => shuffle_frame::Kind::Close(Close {
                reason: reason.clone(),
            }),
        };
        Ok(ShuffleFrame { kind: Some(kind) })
    }

    /// Decode a wire [`ShuffleFrame`] into a [`ShuffleMessage`]. Fails on an empty
    /// oneof or a corrupt `VnodeData` IPC blob.
    fn from_frame(frame: ShuffleFrame) -> Result<ShuffleMessage, tonic::Status> {
        let kind = frame
            .kind
            .ok_or_else(|| tonic::Status::invalid_argument("empty shuffle frame"))?;
        Ok(match kind {
            shuffle_frame::Kind::Hello(h) => ShuffleMessage::Hello(h.node_id),
            shuffle_frame::Kind::Barrier(b) => ShuffleMessage::Barrier(CheckpointBarrier {
                checkpoint_id: b.checkpoint_id,
                epoch: b.epoch,
                flags: b.flags,
            }),
            shuffle_frame::Kind::VnodeData(v) => {
                let batch = deserialize_batch_stream(&v.arrow_ipc)
                    .map_err(|e| tonic::Status::invalid_argument(format!("shuffle ipc: {e}")))?;
                ShuffleMessage::VnodeData(v.stage, v.vnode, batch)
            }
            shuffle_frame::Kind::Close(c) => ShuffleMessage::Close(c.reason),
        })
    }

    /// One lazily-opened client-streaming call to a peer. The driver task pulls
    /// frames from `tx`'s queue and feeds the gRPC request stream; it flips
    /// `alive=false` on the first transport error (or connect failure), so the
    /// next `send_to` purges this entry and reconnects.
    struct PeerConn {
        tx: MAsyncTx<mpsc::Array<ShuffleFrame>>,
        alive: Arc<AtomicBool>,
        driver: JoinHandle<()>,
    }

    impl PeerConn {
        fn is_alive(&self) -> bool {
            self.alive.load(Ordering::Acquire)
        }
    }

    impl Drop for PeerConn {
        fn drop(&mut self) {
            self.driver.abort();
        }
    }

    /// Lazy pool of outbound client-streaming calls, keyed by peer id.
    pub struct ShuffleSender {
        local_id: ShufflePeerId,
        peers: Mutex<FxHashMap<ShufflePeerId, SocketAddr>>,
        pool: Mutex<FxHashMap<ShufflePeerId, Arc<PeerConn>>>,
        kv: Option<Arc<dyn ClusterKv>>,
    }

    impl std::fmt::Debug for ShuffleSender {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ShuffleSender")
                .field("local_id", &self.local_id)
                .finish_non_exhaustive()
        }
    }

    impl ShuffleSender {
        /// Empty sender. Peers are added via [`Self::register_peer`] or discovered
        /// via the KV (in [`Self::with_kv`]) before any `send_to`.
        #[must_use]
        pub fn new(local_id: ShufflePeerId) -> Self {
            Self {
                local_id,
                peers: Mutex::new(FxHashMap::default()),
                pool: Mutex::new(FxHashMap::default()),
                kv: None,
            }
        }

        /// Sender that falls back to `kv` (key [`SHUFFLE_ADDR_KEY`] on the peer's
        /// own state) when `send_to` targets a peer not previously registered.
        #[must_use]
        pub fn with_kv(local_id: ShufflePeerId, kv: Arc<dyn ClusterKv>) -> Self {
            let mut s = Self::new(local_id);
            s.kv = Some(kv);
            s
        }

        /// Register (or update) a peer's shuffle address.
        // Body is sync, but the signature stays async to match the contract
        // callers `.await`.
        #[allow(clippy::unused_async)]
        pub async fn register_peer(&self, peer: ShufflePeerId, addr: SocketAddr) {
            self.peers.lock().insert(peer, addr);
        }

        /// Send `msg` to `peer`, opening a client-streaming call if necessary.
        ///
        /// # Errors
        /// Returns `io::Error` when the peer is unregistered/undiscoverable, the
        /// endpoint cannot be built, or the per-peer stream has shut down.
        pub async fn send_to(&self, peer: ShufflePeerId, msg: &ShuffleMessage) -> io::Result<()> {
            let frame = to_frame(msg).map_err(io_err)?;
            let conn = self.connection_for(peer).await?;
            conn.tx.send(frame).await.map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("shuffle stream to peer {peer} closed"),
                )
            })
        }

        /// Ship `barrier` to every peer in order, short-circuiting on the first
        /// failure (the gossip side-channel is authoritative, so a partial
        /// fan-out is tolerable).
        ///
        /// # Errors
        /// Returns the first `io::Error` from any peer's `send_to`.
        pub async fn fan_out_barrier(
            &self,
            peers: &[ShufflePeerId],
            barrier: CheckpointBarrier,
        ) -> io::Result<()> {
            let msg = ShuffleMessage::Barrier(barrier);
            for &peer in peers {
                self.send_to(peer, &msg).await?;
            }
            Ok(())
        }

        /// Resolve `peer`'s address from the KV (`SHUFFLE_ADDR_KEY` on the peer's
        /// own state) and cache it. `None` when no KV, no entry, or unparseable.
        async fn discover_peer(&self, peer: ShufflePeerId) -> Option<SocketAddr> {
            let kv = self.kv.as_ref()?;
            let raw = kv
                .read_from(crate::cluster::discovery::NodeId(peer), SHUFFLE_ADDR_KEY)
                .await?;
            let addr: SocketAddr = raw.parse().ok()?;
            self.peers.lock().insert(peer, addr);
            Some(addr)
        }

        async fn connection_for(&self, peer: ShufflePeerId) -> io::Result<Arc<PeerConn>> {
            if let Some(existing) = self.pool.lock().get(&peer).cloned() {
                if existing.is_alive() {
                    return Ok(existing);
                }
            }
            // Purge a dead entry so we reopen the call below.
            self.pool.lock().retain(|p, c| *p != peer || c.is_alive());

            // Re-resolve on reconnect (peers may restart on a new port); fall
            // back to a statically registered address when there's no KV.
            let addr = match self.discover_peer(peer).await {
                Some(addr) => addr,
                None => self.peers.lock().get(&peer).copied().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("peer {peer} has no registered shuffle address"),
                    )
                })?,
            };

            let conn = Arc::new(open_call(self.local_id, addr)?);

            // Race: another task may have opened a live call meanwhile.
            let mut pool = self.pool.lock();
            if let Some(winner) = pool.get(&peer).cloned() {
                if winner.is_alive() {
                    return Ok(winner);
                }
            }
            pool.insert(peer, Arc::clone(&conn));
            Ok(conn)
        }
    }

    /// Open a client-streaming `Shuffle` call to `addr`, sending `Hello(local_id)`
    /// as the first frame. Connecting happens inside the driver task so this stays
    /// non-blocking; a connect failure flips `alive` so the next `send_to` retries.
    fn open_call(local_id: ShufflePeerId, addr: SocketAddr) -> io::Result<PeerConn> {
        let endpoint = crate::cluster::control::tls::client_endpoint(&addr.to_string())
            .map_err(io_err)?
            .tcp_nodelay(true);
        let (tx, rx) = mpsc::bounded_async::<ShuffleFrame>(SHUFFLE_SEND_QUEUE);
        let alive = Arc::new(AtomicBool::new(true));
        let alive_for_driver = Arc::clone(&alive);

        // Request stream: a single `Hello` chained onto an unfold over the
        // per-peer crossfire receiver (no `async-stream` dependency needed).
        let hello = to_frame(&ShuffleMessage::Hello(local_id)).map_err(io_err)?;
        let outbound = futures::stream::once(async move { hello })
            .chain(futures::stream::unfold(rx, |rx| async move {
                rx.recv().await.ok().map(|frame| (frame, rx))
            }));

        let driver = tokio::spawn(async move {
            let Ok(channel) = endpoint.connect().await else {
                alive_for_driver.store(false, Ordering::Release);
                return;
            };
            let mut client = ShuffleTransportClient::<Channel>::new(channel);
            // The call returns when the server responds to half-close or the
            // transport breaks; either way the peer connection is finished.
            let _ = client.shuffle(Request::new(outbound)).await;
            alive_for_driver.store(false, Ordering::Release);
        });

        Ok(PeerConn { tx, alive, driver })
    }

    /// Inbound side of the shuffle fabric: a Tonic `ShuffleTransport` server
    /// surfacing every received frame, attributed to its sending peer, on the
    /// bounded crossfire queue.
    pub struct ShuffleReceiver {
        local_id: ShufflePeerId,
        local_addr: SocketAddr,
        // crossfire's `AsyncRx` is `Send` but `!Sync` (it holds a
        // `PhantomData<Cell<()>>`), yet `Arc<ShuffleReceiver>` must be `Sync` — it
        // is embedded in DataFusion's `ClusterRepartitionExec`, whose
        // `ExecutionPlan` impl requires `Send + Sync`. Park the receiver behind a
        // `Mutex<Option<_>>` (which is `Sync` for any `Send` inner) and hand it out
        // via a take/return guard so the single async consumer never holds the
        // guard across `.await`. `rx_returned` wakes the next waiter; the guard
        // restores the receiver on drop so a cancelled `recv` can't strand it.
        rx: Mutex<Option<InboundRx>>,
        rx_returned: Arc<tokio::sync::Notify>,
        server: JoinHandle<()>,
        holdover: Arc<Holdover>,
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
        /// Bind on `addr` and start serving. The bound address (with any ephemeral
        /// port resolved) is exposed via [`Self::local_addr`].
        ///
        /// # Errors
        /// Returns `io::Error` on bind failure.
        pub async fn bind(local_id: ShufflePeerId, addr: SocketAddr) -> io::Result<Self> {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            let (tx, rx) =
                mpsc::bounded_async::<(ShufflePeerId, ShuffleMessage)>(SHUFFLE_RECV_QUEUE);

            let service = ShuffleService { tx };
            // Accept loop as a stream of `Result<TcpStream, io::Error>` for
            // `serve_with_incoming` — avoids the tokio-stream `net` feature.
            // nodelay is set per accepted connection.
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
            let server = tokio::spawn(async move {
                let mut builder = Server::builder();
                if let Some(tls) = crate::cluster::control::tls::server_tls() {
                    match builder.tls_config(tls.clone()) {
                        Ok(b) => builder = b,
                        Err(e) => {
                            tracing::error!(error = %e, "cluster shuffle TLS config failed");
                            return;
                        }
                    }
                }
                let _ = builder
                    .add_service(ShuffleTransportServer::new(service))
                    .serve_with_incoming(incoming)
                    .await;
            });

            Ok(Self {
                local_id,
                local_addr,
                rx: Mutex::new(Some(rx)),
                rx_returned: Arc::new(tokio::sync::Notify::new()),
                server,
                holdover: Arc::new(Holdover::default()),
            })
        }

        /// Bind and publish the listener's address into `kv` under
        /// [`SHUFFLE_ADDR_KEY`] for peer discovery.
        ///
        /// # Errors
        /// Returns `io::Error` on bind failure.
        pub async fn bind_with_kv(
            local_id: ShufflePeerId,
            addr: SocketAddr,
            kv: Arc<dyn ClusterKv>,
        ) -> io::Result<Self> {
            let recv = Self::bind(local_id, addr).await?;
            kv.write(SHUFFLE_ADDR_KEY, recv.local_addr.to_string())
                .await;
            Ok(recv)
        }

        /// Local socket address the server is bound to.
        #[must_use]
        pub fn local_addr(&self) -> SocketAddr {
            self.local_addr
        }

        /// Await the next `(peer_id, msg)`. `None` once the server task has stopped
        /// and every queued item is drained. Single-owner; concurrent callers
        /// serialise via `rx_returned`. Cancellation-safe — a dropped `recv()`
        /// future returns the receiver to its slot via the RAII guard.
        pub async fn recv(&self) -> Option<(ShufflePeerId, ShuffleMessage)> {
            loop {
                // Take the receiver out under a short lock dropped before `.await`.
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

        /// Drain every currently-available `(peer_id, msg)` without blocking. Empty
        /// when the queue is empty or a `recv()` currently holds the receiver.
        #[must_use]
        pub fn drain_available(&self) -> Vec<(ShufflePeerId, ShuffleMessage)> {
            let mut out = Vec::new();
            let slot = self.rx.lock();
            if let Some(rx) = slot.as_ref() {
                while let Ok(item) = rx.try_recv() {
                    out.push(item);
                }
            }
            out
        }

        /// Non-blocking drain of the [`ShuffleMessage::VnodeData`] batches for
        /// `stage`. Frames for other stages are bucketed for their own drainer;
        /// `Barrier` frames are stashed for the aligning checkpoint (never dropped
        /// — see `Holdover`). Empty if the queue is empty or a `recv()` holds it.
        #[must_use]
        pub fn drain_vnode_data_for(&self, stage: &str) -> Vec<RecordBatch> {
            let mut staged = self.holdover.staged.lock();
            {
                let slot = self.rx.lock();
                if let Some(rx) = slot.as_ref() {
                    while let Ok((from, msg)) = rx.try_recv() {
                        match msg {
                            ShuffleMessage::VnodeData(s, _vnode, batch) => {
                                staged.entry(s).or_default().push(batch);
                            }
                            ShuffleMessage::Barrier(b) => {
                                self.holdover.staged_barriers.lock().push((from, b));
                            }
                            _ => {} // Hello / Close
                        }
                    }
                }
            }
            staged.remove(stage).unwrap_or_default()
        }

        /// Stage `batch` under `stage` for a later [`Self::drain_vnode_data_for`] /
        /// [`Self::drain_all_staged`] — used when no operator for `stage` exists yet
        /// at drain time.
        pub fn stage_batch(&self, stage: String, batch: RecordBatch) {
            self.holdover
                .staged
                .lock()
                .entry(stage)
                .or_default()
                .push(batch);
        }

        /// Take the barriers stashed by [`Self::drain_vnode_data_for`] (peers that
        /// fanned out before this node began aligning).
        #[must_use]
        pub fn drain_staged_barriers(&self) -> Vec<(ShufflePeerId, CheckpointBarrier)> {
            std::mem::take(&mut self.holdover.staged_barriers.lock())
        }

        /// Empty the per-stage holdover, returning every buffered `(stage, batch)`.
        #[must_use]
        pub fn drain_all_staged(&self) -> Vec<(String, RecordBatch)> {
            let mut staged = self.holdover.staged.lock();
            staged
                .drain()
                .flat_map(|(stage, batches)| batches.into_iter().map(move |b| (stage.clone(), b)))
                .collect()
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

    /// The `ShuffleTransport` service object: holds the producer end of the inbound
    /// queue shared by every peer stream.
    struct ShuffleService {
        tx: InboundTx,
    }

    #[tonic::async_trait]
    impl ShuffleTransport for ShuffleService {
        async fn shuffle(
            &self,
            request: Request<tonic::Streaming<ShuffleFrame>>,
        ) -> Result<tonic::Response<ShuffleSummary>, tonic::Status> {
            let summary = run_stream(self.tx.clone(), request.into_inner()).await?;
            Ok(tonic::Response::new(summary))
        }
    }

    /// Read the leading `Hello`, then forward each decoded frame onto the bounded
    /// inbound queue, returning a summary when the client half-closes.
    async fn run_stream(
        tx: InboundTx,
        mut stream: tonic::Streaming<ShuffleFrame>,
    ) -> Result<ShuffleSummary, tonic::Status> {
        let first = stream
            .message()
            .await?
            .ok_or_else(|| tonic::Status::invalid_argument("shuffle stream closed before Hello"))?;
        let ShuffleMessage::Hello(peer) = from_frame(first)? else {
            return Err(tonic::Status::invalid_argument(
                "first shuffle frame must be Hello",
            ));
        };

        let mut frames_received = 0u64;
        while let Some(frame) = stream.message().await? {
            let msg = from_frame(frame)?;
            if matches!(msg, ShuffleMessage::Close(_)) {
                break;
            }
            frames_received += 1;

            if let ShuffleMessage::VnodeData(stage, default_vnode, batch) = msg {
                let schema = batch.schema();
                if let Some((col_idx, _field)) = schema.column_with_name("__laminar_vnode") {
                    let vnode_array = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<arrow_array::UInt32Array>()
                        .ok_or_else(|| {
                            tonic::Status::invalid_argument(
                                "vnode metadata column is not UInt32Array",
                            )
                        })?;
                    let row_vnodes: Vec<u32> = vnode_array.values().to_vec();

                    let mut projection: Vec<usize> = (0..schema.fields().len()).collect();
                    projection.remove(col_idx);
                    let batch_without_vnode = batch.project(&projection).map_err(|e| {
                        tonic::Status::internal(format!(
                            "Failed to project out vnode metadata: {e}"
                        ))
                    })?;

                    let slices = crate::shuffle::routing::slice_batch_by_vnodes(
                        &batch_without_vnode,
                        &row_vnodes,
                    );
                    let mut send_failed = false;
                    for (v, slice) in slices {
                        let sub_msg = ShuffleMessage::VnodeData(stage.clone(), v, slice);
                        if tx.send((peer, sub_msg)).await.is_err() {
                            send_failed = true;
                            break;
                        }
                    }
                    if send_failed {
                        break;
                    }
                } else if tx
                    .send((peer, ShuffleMessage::VnodeData(stage, default_vnode, batch)))
                    .await
                    .is_err()
                {
                    break;
                }
            } else if tx.send((peer, msg)).await.is_err() {
                break;
            }
        }
        Ok(ShuffleSummary { frames_received })
    }
}

#[cfg(feature = "cluster")]
pub use grpc::{ShuffleReceiver, ShuffleSender};

// ===========================================================================
// Default build: networking-free shim preserving the public API.
//
// The cluster shuffle is only exercised under `cluster`; a default
// build references these types only in signatures. The shim keeps the inbound
// crossfire queue + holdover staging so the surface compiles and behaves sanely
// (local-only) without pulling in tonic.
// ===========================================================================

#[cfg(not(feature = "cluster"))]
mod shim {
    use std::io;
    use std::net::SocketAddr;
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use crossfire::{mpsc, AsyncRx, MAsyncTx};
    use parking_lot::Mutex;
    use rustc_hash::FxHashMap;

    use super::{Holdover, ShuffleMessage, ShufflePeerId, SHUFFLE_RECV_QUEUE};
    use crate::checkpoint::barrier::CheckpointBarrier;

    type InboundRx = AsyncRx<mpsc::Array<(ShufflePeerId, ShuffleMessage)>>;
    type InboundTx = MAsyncTx<mpsc::Array<(ShufflePeerId, ShuffleMessage)>>;

    /// Outbound shuffle handle. Without the cluster feature there is no peer
    /// fabric, so sends to a non-local peer report the peer as unregistered.
    pub struct ShuffleSender {
        local_id: ShufflePeerId,
        peers: Mutex<FxHashMap<ShufflePeerId, SocketAddr>>,
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
        pub fn new(local_id: ShufflePeerId) -> Self {
            Self {
                local_id,
                peers: Mutex::new(FxHashMap::default()),
            }
        }

        /// Register (or update) a peer's shuffle address.
        #[allow(clippy::unused_async)] // async to match the cluster build's API.
        pub async fn register_peer(&self, peer: ShufflePeerId, addr: SocketAddr) {
            self.peers.lock().insert(peer, addr);
        }

        /// # Errors
        /// Errors for an unregistered peer; the no-cluster build has no transport,
        /// so registered peers are accepted as a no-op delivery.
        #[allow(clippy::unused_async)] // async to match the cluster build's API.
        pub async fn send_to(&self, peer: ShufflePeerId, _msg: &ShuffleMessage) -> io::Result<()> {
            if self.peers.lock().contains_key(&peer) {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {peer} has no registered shuffle address"),
                ))
            }
        }

        /// # Errors
        /// Returns the first `io::Error` from any peer's `send_to`.
        pub async fn fan_out_barrier(
            &self,
            peers: &[ShufflePeerId],
            barrier: CheckpointBarrier,
        ) -> io::Result<()> {
            let msg = ShuffleMessage::Barrier(barrier);
            for &peer in peers {
                self.send_to(peer, &msg).await?;
            }
            Ok(())
        }
    }

    /// Inbound shuffle handle. Holds the bounded crossfire queue + holdover so the
    /// drain/stage API compiles and behaves locally without a network. The
    /// receiver is parked behind a `Mutex<Option<_>>` for the same `Sync` reason as
    /// the gRPC build.
    pub struct ShuffleReceiver {
        local_id: ShufflePeerId,
        local_addr: SocketAddr,
        #[allow(dead_code)]
        tx: InboundTx,
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
        /// # Errors
        /// Returns `io::Error` on bind failure.
        pub async fn bind(local_id: ShufflePeerId, addr: SocketAddr) -> io::Result<Self> {
            // Resolve the address (incl. ephemeral port) by binding momentarily.
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            drop(listener);
            let (tx, rx) =
                mpsc::bounded_async::<(ShufflePeerId, ShuffleMessage)>(SHUFFLE_RECV_QUEUE);
            Ok(Self {
                local_id,
                local_addr,
                tx,
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

        /// Await the next `(peer_id, msg)`. `None` once all senders drop.
        pub async fn recv(&self) -> Option<(ShufflePeerId, ShuffleMessage)> {
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

        /// Drain every currently-available `(peer_id, msg)` without blocking.
        #[must_use]
        pub fn drain_available(&self) -> Vec<(ShufflePeerId, ShuffleMessage)> {
            let mut out = Vec::new();
            let slot = self.rx.lock();
            if let Some(rx) = slot.as_ref() {
                while let Ok(item) = rx.try_recv() {
                    out.push(item);
                }
            }
            out
        }

        /// Non-blocking drain of the `VnodeData` batches for `stage`; other-stage
        /// frames are bucketed and barriers stashed (never dropped).
        #[must_use]
        pub fn drain_vnode_data_for(&self, stage: &str) -> Vec<RecordBatch> {
            let mut staged = self.holdover.staged.lock();
            {
                let slot = self.rx.lock();
                if let Some(rx) = slot.as_ref() {
                    while let Ok((from, msg)) = rx.try_recv() {
                        match msg {
                            ShuffleMessage::VnodeData(s, _vnode, batch) => {
                                staged.entry(s).or_default().push(batch);
                            }
                            ShuffleMessage::Barrier(b) => {
                                self.holdover.staged_barriers.lock().push((from, b));
                            }
                            _ => {}
                        }
                    }
                }
            }
            staged.remove(stage).unwrap_or_default()
        }

        /// Stage `batch` under `stage` for a later drain.
        pub fn stage_batch(&self, stage: String, batch: RecordBatch) {
            self.holdover
                .staged
                .lock()
                .entry(stage)
                .or_default()
                .push(batch);
        }

        /// Take the barriers stashed by [`Self::drain_vnode_data_for`].
        #[must_use]
        pub fn drain_staged_barriers(&self) -> Vec<(ShufflePeerId, CheckpointBarrier)> {
            std::mem::take(&mut self.holdover.staged_barriers.lock())
        }

        /// Empty the per-stage holdover, returning every buffered `(stage, batch)`.
        #[must_use]
        pub fn drain_all_staged(&self) -> Vec<(String, RecordBatch)> {
            let mut staged = self.holdover.staged.lock();
            staged
                .drain()
                .flat_map(|(stage, batches)| batches.into_iter().map(move |b| (stage.clone(), b)))
                .collect()
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

#[cfg(all(test, feature = "cluster"))]
mod tests {
    use std::io;
    use std::sync::Arc;

    use super::*;

    async fn bind_on_loopback(local_id: ShufflePeerId) -> ShuffleReceiver {
        ShuffleReceiver::bind(local_id, "127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_to_receiver_delivers_with_peer_attribution() {
        let recv = bind_on_loopback(2).await;
        let recv_addr = recv.local_addr();

        let sender = ShuffleSender::new(1);
        sender.register_peer(2, recv_addr).await;
        sender
            .send_to(2, &ShuffleMessage::Hello(1234))
            .await
            .unwrap();

        let (from, msg) = recv.recv().await.unwrap();
        assert_eq!(from, 1, "receiver attributes frame to sender id");
        assert_eq!(msg, ShuffleMessage::Hello(1234));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sender_reuses_stream_across_sends() {
        let recv = bind_on_loopback(2).await;
        let sender = ShuffleSender::new(1);
        sender.register_peer(2, recv.local_addr()).await;

        for delta in [10u64, 20, 30, 40] {
            sender
                .send_to(2, &ShuffleMessage::Hello(delta))
                .await
                .unwrap();
        }

        let mut got = Vec::new();
        for _ in 0..4 {
            got.push(recv.recv().await.unwrap().1);
        }
        assert_eq!(
            got,
            vec![
                ShuffleMessage::Hello(10),
                ShuffleMessage::Hello(20),
                ShuffleMessage::Hello(30),
                ShuffleMessage::Hello(40),
            ]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_to_unregistered_peer_errors() {
        let sender = ShuffleSender::new(1);
        let err = sender
            .send_to(99, &ShuffleMessage::Hello(1))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
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
        let sender = ShuffleSender::with_kv(1, kv as Arc<dyn ClusterKv>);

        sender.send_to(2, &ShuffleMessage::Hello(7)).await.unwrap();
        let (from, msg) = recv.recv().await.unwrap();
        assert_eq!(from, 1);
        assert_eq!(msg, ShuffleMessage::Hello(7));
    }

    /// A peer restarting at a new address: the cached stream breaks, the next
    /// `send_to` reconnects against the freshly-registered address. Windows-only
    /// skip — the FIN-after-abort wakeup chain is not time-bounded under nextest
    /// parallelism there.
    #[cfg(not(windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_reconnects_after_peer_restart_at_new_address() {
        let recv_v1 = bind_on_loopback(2).await;
        let addr_v1 = recv_v1.local_addr();

        let sender = ShuffleSender::new(1);
        sender.register_peer(2, addr_v1).await;
        sender
            .send_to(2, &ShuffleMessage::Hello(111))
            .await
            .unwrap();
        let (from, msg) = recv_v1.recv().await.unwrap();
        assert_eq!(from, 1);
        assert_eq!(msg, ShuffleMessage::Hello(111));

        // Crash the peer.
        drop(recv_v1);

        // Peer restarts on a fresh ephemeral port.
        let recv_v2 = bind_on_loopback(2).await;
        let addr_v2 = recv_v2.local_addr();
        assert_ne!(addr_v1, addr_v2, "ephemeral rebind must pick a new port");
        sender.register_peer(2, addr_v2).await;

        // Reconnect + deliver to the restarted peer. Retry to absorb the time it
        // takes the old stream to flip dead after the server aborted.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let _ = sender.send_to(2, &ShuffleMessage::Hello(222)).await;
            if let Some((from, ShuffleMessage::Hello(222))) =
                tokio::time::timeout(std::time::Duration::from_millis(200), recv_v2.recv())
                    .await
                    .ok()
                    .flatten()
            {
                assert_eq!(from, 1);
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "did not deliver to restarted peer within 30s",
            );
        }
    }
}
