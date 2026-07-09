//! Cross-node shuffle transport over Tonic gRPC client-streaming.
//!
//! Each sender opens a client-streaming `Shuffle` RPC per peer; the receiver
//! attributes every stream to the peer in its leading `Hello` and surfaces
//! decoded [`ShuffleMessage`]s on a bounded crossfire queue. Backpressure is the
//! HTTP/2 window plus that queue.
//!
//! The gRPC path compiles under the `cluster` feature; a default build keeps the
//! same public API via a networking-free shim.

use super::message::ShuffleMessage;
use crate::checkpoint::barrier::CheckpointBarrier;

/// Inbound shuffle queue capacity; a full queue parks the service handler so
/// backpressure flows back over HTTP/2.
const SHUFFLE_RECV_QUEUE: usize = 1024;

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

// ---------------------------------------------------------------------------
// Per-stage / per-barrier holdover shared by both builds.
// ---------------------------------------------------------------------------

/// Inbound staging shared by both builds: frames for another stage are bucketed
/// for that stage's drainer, and mid-cycle barriers are stashed (never dropped)
/// for the aligning checkpoint.
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
    use std::collections::hash_map::Entry;
    use std::io;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    use crate::serialization::{BatchStreamDecoder, BatchStreamEncoder};

    /// Per-peer outbound queue capacity before the HTTP/2 window backpressures.
    const SHUFFLE_SEND_QUEUE: usize = 1024;

    /// gRPC encode/decode message-size cap for shuffle frames, both directions. tonic defaults to
    /// 4 MiB, which breaks the stream on any larger `VnodeData` frame — and a mid-stream break
    /// silently drops the queued frames (CL-2/CL-3). Sized above the 64 MiB per-`VnodeData` payload
    /// cap (`super::message::MAX_PAYLOAD_BYTES`) plus IPC schema/framing overhead.
    const MAX_SHUFFLE_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

    /// Inbound queue alias (a `type` so the parked receiver field dodges
    /// `type_complexity`).
    type InboundRx = AsyncRx<mpsc::Array<(ShufflePeerId, ShuffleMessage)>>;
    type InboundTx = MAsyncTx<mpsc::Array<(ShufflePeerId, ShuffleMessage)>>;

    /// Map any `Display` error into `io::Error` to keep the public `io::Result`
    /// shape.
    fn io_err<E: std::fmt::Display>(e: E) -> io::Error {
        io::Error::other(e.to_string())
    }

    /// Encode a [`ShuffleMessage`] into a wire [`ShuffleFrame`]. The per-stage
    /// [`BatchStreamEncoder`] writes the schema only on a stage's first
    /// `VnodeData`. Runs in the driver task, off the compute thread.
    fn encode_message(
        msg: &ShuffleMessage,
        encoders: &mut FxHashMap<String, BatchStreamEncoder>,
        recovery_gen: u64,
    ) -> Result<ShuffleFrame, tonic::Status> {
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
                let encoder = match encoders.entry(stage.clone()) {
                    Entry::Occupied(e) => {
                        let enc = e.into_mut();
                        // Fail loudly rather than desync the peer's IPC decoder.
                        let schema = batch.schema();
                        if !Arc::ptr_eq(enc.schema(), &schema) && *enc.schema() != schema {
                            return Err(tonic::Status::internal(format!(
                                "shuffle stage '{stage}' changed schema mid-connection",
                            )));
                        }
                        enc
                    }
                    Entry::Vacant(v) => {
                        v.insert(BatchStreamEncoder::new(&batch.schema()).map_err(|e| {
                            tonic::Status::internal(format!("shuffle ipc encoder init: {e}"))
                        })?)
                    }
                };
                let arrow_ipc = encoder
                    .encode(batch)
                    .map_err(|e| tonic::Status::internal(format!("shuffle ipc encode: {e}")))?;
                shuffle_frame::Kind::VnodeData(VnodeData {
                    stage: stage.clone(),
                    vnode: *vnode,
                    arrow_ipc,
                    recovery_gen,
                })
            }
            ShuffleMessage::Close(reason) => shuffle_frame::Kind::Close(Close {
                reason: reason.clone(),
            }),
        };
        Ok(ShuffleFrame { kind: Some(kind) })
    }

    /// One lazily-opened client-streaming call to a peer. The driver task
    /// serializes queued messages and feeds the gRPC request stream, flipping
    /// `alive=false` on the first transport/connect error so the next `send_to`
    /// reconnects. Buffering messages (not frames) keeps Arrow IPC serialization
    /// off the compute thread.
    struct PeerConn {
        tx: MAsyncTx<mpsc::Array<ShuffleMessage>>,
        alive: Arc<AtomicBool>,
        driver: JoinHandle<()>,
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

    /// Lazy pool of outbound client-streaming calls, keyed by peer id.
    pub struct ShuffleSender {
        local_id: ShufflePeerId,
        peers: Mutex<FxHashMap<ShufflePeerId, SocketAddr>>,
        pool: Mutex<FxHashMap<ShufflePeerId, Arc<PeerConn>>>,
        kv: Option<Arc<dyn ClusterKv>>,
        /// Stamped onto every outbound `VnodeData`; bumped by a coordinated rewind.
        recovery_gen: Arc<AtomicU64>,
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
        pub fn new(local_id: ShufflePeerId) -> Self {
            Self {
                local_id,
                peers: Mutex::new(FxHashMap::default()),
                pool: Mutex::new(FxHashMap::default()),
                kv: None,
                recovery_gen: Arc::new(AtomicU64::new(0)),
            }
        }

        /// Advance the generation stamped onto outbound data frames. Called after a coordinated
        /// rewind so peers can discard anything this node produced before it.
        pub fn set_recovery_gen(&self, gen: u64) {
            self.recovery_gen.fetch_max(gen, Ordering::AcqRel);
        }

        /// Sender that falls back to `kv` discovery for peers not previously
        /// registered.
        #[must_use]
        pub fn with_kv(local_id: ShufflePeerId, kv: Arc<dyn ClusterKv>) -> Self {
            let mut s = Self::new(local_id);
            s.kv = Some(kv);
            s
        }

        /// Register (or update) a peer's shuffle address.
        // Sync body; async signature matches the contract callers `.await`.
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
            let conn = self.connection_for(peer).await?;
            // Cheap clone (`RecordBatch` is an Arc bump); the driver serializes
            // off-thread.
            conn.tx.send(msg.clone()).await.map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    format!("shuffle stream to peer {peer} closed"),
                )
            })
        }

        /// Ship `barrier` to every peer, best-effort: an unreachable peer is
        /// logged and skipped so one failure can't wedge alignment for the
        /// others. A genuinely-down peer is handled by the align wait-set
        /// self-heal, not here.
        ///
        /// # Errors
        /// Returns the last `io::Error` only when NO peer could be reached; a
        /// partial fan-out succeeds.
        pub async fn fan_out_barrier(
            &self,
            peers: &[ShufflePeerId],
            barrier: CheckpointBarrier,
        ) -> io::Result<()> {
            let cid = barrier.checkpoint_id;
            let msg = ShuffleMessage::Barrier(barrier);
            let mut reached = 0usize;
            let mut last_err = None;
            for &peer in peers {
                match self.send_to(peer, &msg).await {
                    Ok(()) => reached += 1,
                    Err(e) => {
                        tracing::warn!(
                            peer,
                            checkpoint_id = cid,
                            error = %e,
                            "shuffle barrier fan-out: peer unreachable, skipping (best-effort)"
                        );
                        last_err = Some(e);
                    }
                }
            }
            match last_err {
                Some(e) if reached == 0 && !peers.is_empty() => Err(e),
                _ => Ok(()),
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

        async fn connection_for(&self, peer: ShufflePeerId) -> io::Result<Arc<PeerConn>> {
            if let Some(existing) = self.pool.lock().get(&peer).cloned() {
                if existing.is_alive() {
                    return Ok(existing);
                }
            }
            // Purge a dead entry so we reopen the call below.
            self.pool.lock().retain(|p, c| *p != peer || c.is_alive());

            // Re-resolve on reconnect (peers may restart on a new port); fall back
            // to a statically registered address when there's no KV.
            let addr = match self.discover_peer(peer).await {
                Some(addr) => addr,
                None => self.peers.lock().get(&peer).copied().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("peer {peer} has no registered shuffle address"),
                    )
                })?,
            };

            tracing::debug!(peer, addr = %addr, "shuffle reconnecting to peer");
            let conn = Arc::new(open_call(
                self.local_id,
                addr,
                Arc::clone(&self.recovery_gen),
            )?);

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
    /// first. Connecting happens in the driver task (non-blocking); a connect
    /// failure flips `alive` so the next `send_to` retries.
    fn open_call(
        local_id: ShufflePeerId,
        addr: SocketAddr,
        recovery_gen: Arc<AtomicU64>,
    ) -> io::Result<PeerConn> {
        let endpoint = crate::cluster::control::tls::client_endpoint(&addr.to_string())
            .map_err(io_err)?
            .tcp_nodelay(true);
        let (tx, rx) = mpsc::bounded_async::<ShuffleMessage>(SHUFFLE_SEND_QUEUE);
        let alive = Arc::new(AtomicBool::new(true));
        let alive_for_driver = Arc::clone(&alive);

        // Request stream: a `Hello` chained onto an unfold over the per-peer
        // receiver, serializing dequeued messages here in the driver task.
        let hello = ShuffleFrame {
            kind: Some(shuffle_frame::Kind::Hello(Hello { node_id: local_id })),
        };
        let encoders: FxHashMap<String, BatchStreamEncoder> = FxHashMap::default();
        let outbound = futures::stream::once(async move { hello }).chain(futures::stream::unfold(
            (rx, encoders, recovery_gen),
            |(rx, mut encoders, recovery_gen)| async move {
                let msg = rx.recv().await.ok()?;
                // Stamp at encode time, not enqueue time: a frame still queued when a rewind
                // bumps the generation is stale and must be dropped by the peer.
                let gen = recovery_gen.load(Ordering::Acquire);
                match encode_message(&msg, &mut encoders, gen) {
                    Ok(frame) => Some((frame, (rx, encoders, recovery_gen))),
                    Err(e) => {
                        // An unencodable batch would desync the IPC stream;
                        // half-close so the peer reconnects fresh.
                        tracing::warn!(error = %e, "shuffle frame encode failed; closing stream");
                        None
                    }
                }
            },
        ));

        let driver = tokio::spawn(async move {
            let Ok(channel) = endpoint.connect().await else {
                alive_for_driver.store(false, Ordering::Release);
                return;
            };
            let mut client = ShuffleTransportClient::<Channel>::new(channel)
                .max_decoding_message_size(MAX_SHUFFLE_MESSAGE_BYTES)
                .max_encoding_message_size(MAX_SHUFFLE_MESSAGE_BYTES);
            // Returns on server half-close ack or transport break; either way done.
            let _ = client.shuffle(Request::new(outbound)).await;
            alive_for_driver.store(false, Ordering::Release);
        });

        Ok(PeerConn { tx, alive, driver })
    }

    /// Inbound side of the shuffle fabric: a Tonic `ShuffleTransport` server that
    /// surfaces every received frame, attributed to its peer, on the bounded queue.
    pub struct ShuffleReceiver {
        local_id: ShufflePeerId,
        local_addr: SocketAddr,
        // `AsyncRx` is `Send` but `!Sync`, yet `Arc<ShuffleReceiver>` must be
        // `Sync` (it lives in DataFusion's `ClusterRepartitionExec`). Park it
        // behind a `Mutex<Option<_>>` and hand it out via a take/return guard so
        // the single consumer never holds the guard across `.await`; the guard
        // restores it on drop so a cancelled `recv` can't strand it.
        rx: Mutex<Option<InboundRx>>,
        rx_returned: Arc<tokio::sync::Notify>,
        server: JoinHandle<()>,
        holdover: Arc<Holdover>,
        /// Inbound data frames stamped below this are pre-rewind and discarded.
        recovery_gen: Arc<AtomicU64>,
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
        pub async fn bind(local_id: ShufflePeerId, addr: SocketAddr) -> io::Result<Self> {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let local_addr = listener.local_addr()?;
            let (tx, rx) =
                mpsc::bounded_async::<(ShufflePeerId, ShuffleMessage)>(SHUFFLE_RECV_QUEUE);

            let recovery_gen = Arc::new(AtomicU64::new(0));
            let service = ShuffleService {
                tx,
                recovery_gen: Arc::clone(&recovery_gen),
            };
            // Accept loop as a stream for `serve_with_incoming` (avoids
            // tokio-stream's `net` feature); nodelay is set per connection.
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
                rx: Mutex::new(Some(rx)),
                rx_returned: Arc::new(tokio::sync::Notify::new()),
                server,
                holdover: Arc::new(Holdover::default()),
                recovery_gen,
            })
        }

        /// Advance the generation below which inbound data frames are discarded. Called after a
        /// coordinated rewind so pre-rewind frames still in flight can't be folded onto the
        /// restored state and then re-applied by the sender's replay.
        pub fn set_recovery_gen(&self, gen: u64) {
            self.recovery_gen.fetch_max(gen, Ordering::AcqRel);
        }

        /// Bind and publish the listener's address into `kv` for peer discovery.
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

        /// Await the next `(peer_id, msg)`; `None` once the server stops and the
        /// queue drains. Concurrent callers serialise via `rx_returned`;
        /// cancellation-safe.
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

        /// Drain every currently-available `(peer_id, msg)` without blocking;
        /// empty if a `recv()` holds the receiver.
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

        /// Drain the inbound queue into `staged`: bucket `VnodeData` by stage,
        /// stash `Barrier`s (never dropped), discard `Hello`/`Close`.
        fn drain_inbound_into(&self, staged: &mut FxHashMap<String, Vec<RecordBatch>>) {
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

        /// Non-blocking drain of the `VnodeData` batches for `stage`; other stages
        /// stay bucketed for their own drainer.
        #[must_use]
        pub fn drain_vnode_data_for(&self, stage: &str) -> Vec<RecordBatch> {
            let mut staged = self.holdover.staged.lock();
            self.drain_inbound_into(&mut staged);
            staged.remove(stage).unwrap_or_default()
        }

        /// Drain every staged stage whose key starts with `prefix` in one lock
        /// cycle, leaving operator stages untouched.
        #[must_use]
        pub fn drain_staged_with_prefix(
            &self,
            prefix: &str,
        ) -> FxHashMap<String, Vec<RecordBatch>> {
            let mut staged = self.holdover.staged.lock();
            self.drain_inbound_into(&mut staged);
            let mut out: FxHashMap<String, Vec<RecordBatch>> = FxHashMap::default();
            staged.retain(|stage, batches| {
                if stage.starts_with(prefix) {
                    out.insert(stage.clone(), std::mem::take(batches));
                    false
                } else {
                    true
                }
            });
            out
        }

        /// Stage `batch` under `stage` for a later drain (no operator exists yet).
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

        /// Re-stash a peer barrier pulled while aligning a *different* checkpoint, so a
        /// lagging node still sees it when it reaches that checkpoint.
        pub fn stash_barrier(&self, from: ShufflePeerId, barrier: CheckpointBarrier) {
            self.holdover.staged_barriers.lock().push((from, barrier));
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

    /// `ShuffleTransport` service: the producer end of the inbound queue shared by
    /// every peer stream.
    struct ShuffleService {
        tx: InboundTx,
        recovery_gen: Arc<AtomicU64>,
    }

    #[tonic::async_trait]
    impl ShuffleTransport for ShuffleService {
        async fn shuffle(
            &self,
            request: Request<tonic::Streaming<ShuffleFrame>>,
        ) -> Result<tonic::Response<ShuffleSummary>, tonic::Status> {
            let summary = run_stream(
                self.tx.clone(),
                request.into_inner(),
                Arc::clone(&self.recovery_gen),
            )
            .await?;
            Ok(tonic::Response::new(summary))
        }
    }

    /// Read the leading `Hello`, then forward each decoded frame onto the inbound
    /// queue, returning a summary on half-close. `VnodeData` is decoded with
    /// per-stage [`BatchStreamDecoder`]s mirroring the sender's encoders.
    async fn run_stream(
        tx: InboundTx,
        mut stream: tonic::Streaming<ShuffleFrame>,
        recovery_gen: Arc<AtomicU64>,
    ) -> Result<ShuffleSummary, tonic::Status> {
        let first = stream
            .message()
            .await?
            .ok_or_else(|| tonic::Status::invalid_argument("shuffle stream closed before Hello"))?;
        let peer = match first.kind {
            Some(shuffle_frame::Kind::Hello(h)) => h.node_id,
            _ => {
                return Err(tonic::Status::invalid_argument(
                    "first shuffle frame must be Hello",
                ))
            }
        };

        let mut decoders: FxHashMap<String, BatchStreamDecoder> = FxHashMap::default();
        let mut frames_received = 0u64;
        while let Some(frame) = stream.message().await? {
            let kind = frame
                .kind
                .ok_or_else(|| tonic::Status::invalid_argument("empty shuffle frame"))?;
            match kind {
                shuffle_frame::Kind::Close(_) => break,
                shuffle_frame::Kind::Hello(h) => {
                    frames_received += 1;
                    if tx
                        .send((peer, ShuffleMessage::Hello(h.node_id)))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                shuffle_frame::Kind::Barrier(b) => {
                    frames_received += 1;
                    let msg = ShuffleMessage::Barrier(CheckpointBarrier {
                        checkpoint_id: b.checkpoint_id,
                        epoch: b.epoch,
                        flags: b.flags,
                    });
                    if tx.send((peer, msg)).await.is_err() {
                        break;
                    }
                }
                shuffle_frame::Kind::VnodeData(v) => {
                    frames_received += 1;
                    // Always decode, even when dropping: the per-stage IPC decoder is stateful
                    // (schema + continuation), so skipping a chunk desyncs the stream.
                    let batches = decoders
                        .entry(v.stage.clone())
                        .or_default()
                        .decode_chunk(v.arrow_ipc)
                        .map_err(|e| {
                            tonic::Status::invalid_argument(format!("shuffle ipc: {e}"))
                        })?;
                    // Produced before our last coordinated rewind: the sender will replay these
                    // records from the rewound offset, so folding them now would double-count.
                    if v.recovery_gen < recovery_gen.load(Ordering::Acquire) {
                        tracing::debug!(
                            peer,
                            stage = %v.stage,
                            frame_gen = v.recovery_gen,
                            "dropping pre-rewind shuffle frame"
                        );
                        continue;
                    }
                    let mut stream_broken = false;
                    for batch in batches {
                        if !forward_vnode_batch(&tx, peer, &v.stage, v.vnode, batch).await? {
                            stream_broken = true;
                            break;
                        }
                    }
                    if stream_broken {
                        break;
                    }
                }
            }
        }
        tracing::debug!(peer, frames_received, "shuffle inbound stream ended");
        Ok(ShuffleSummary { frames_received })
    }

    /// Forward one decoded `stage` batch onto the inbound queue: split per vnode
    /// when it carries the `__laminar_vnode` column, else emit whole under
    /// `default_vnode`. `Ok(false)` when the queue has closed.
    async fn forward_vnode_batch(
        tx: &InboundTx,
        peer: ShufflePeerId,
        stage: &str,
        default_vnode: u32,
        batch: RecordBatch,
    ) -> Result<bool, tonic::Status> {
        let schema = batch.schema();
        let Some((col_idx, _field)) = schema.column_with_name("__laminar_vnode") else {
            let msg = ShuffleMessage::VnodeData(stage.to_string(), default_vnode, batch);
            return Ok(tx.send((peer, msg)).await.is_ok());
        };

        let vnode_array = batch
            .column(col_idx)
            .as_any()
            .downcast_ref::<arrow_array::UInt32Array>()
            .ok_or_else(|| {
                tonic::Status::invalid_argument("vnode metadata column is not UInt32Array")
            })?;
        let row_vnodes: Vec<u32> = vnode_array.values().to_vec();

        let mut projection: Vec<usize> = (0..schema.fields().len()).collect();
        projection.remove(col_idx);
        let batch_without_vnode = batch.project(&projection).map_err(|e| {
            tonic::Status::internal(format!("Failed to project out vnode metadata: {e}"))
        })?;

        let slices =
            crate::shuffle::routing::slice_batch_by_vnodes(&batch_without_vnode, &row_vnodes);
        for (v, slice) in slices {
            let sub_msg = ShuffleMessage::VnodeData(stage.to_string(), v, slice);
            if tx.send((peer, sub_msg)).await.is_err() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    #[cfg(test)]
    mod encode_tests {
        use super::*;
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        #[test]
        fn schema_change_on_a_stage_is_rejected() {
            let batch = |name: &str| {
                let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
                arrow_array::RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))])
                    .unwrap()
            };
            let mut encoders = FxHashMap::default();
            let msg = ShuffleMessage::VnodeData("s".into(), 0, batch("a"));
            encode_message(&msg, &mut encoders, 0).unwrap();

            let changed = ShuffleMessage::VnodeData("s".into(), 0, batch("b"));
            let err = encode_message(&changed, &mut encoders, 0).unwrap_err();
            assert!(err.message().contains("changed schema"), "{err}");

            // A different stage with its own schema is fine.
            let other = ShuffleMessage::VnodeData("t".into(), 0, batch("b"));
            encode_message(&other, &mut encoders, 0).unwrap();
        }
    }
}

#[cfg(feature = "cluster")]
pub use grpc::{ShuffleReceiver, ShuffleSender};

// ===========================================================================
// Default build: networking-free shim preserving the public API.
//
// A default build references these types only in signatures. The shim keeps the
// inbound queue + holdover staging so the surface compiles and behaves locally
// without pulling in tonic.
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

    /// Outbound shuffle handle; without the cluster feature there is no peer
    /// fabric, so a non-local peer reports as unregistered.
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

        /// No peer fabric without the cluster feature; matches the cluster build's API.
        #[allow(clippy::unused_self)]
        pub fn set_recovery_gen(&self, _gen: u64) {}

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

        /// Best-effort fan-out (mirrors the cluster build): a partial fan-out succeeds;
        /// total failure returns the last error.
        ///
        /// # Errors
        /// Returns the last `io::Error` only when NO peer could be reached.
        pub async fn fan_out_barrier(
            &self,
            peers: &[ShufflePeerId],
            barrier: CheckpointBarrier,
        ) -> io::Result<()> {
            let msg = ShuffleMessage::Barrier(barrier);
            let mut reached = 0usize;
            let mut last_err = None;
            for &peer in peers {
                match self.send_to(peer, &msg).await {
                    Ok(()) => reached += 1,
                    Err(e) => last_err = Some(e),
                }
            }
            match last_err {
                Some(e) if reached == 0 && !peers.is_empty() => Err(e),
                _ => Ok(()),
            }
        }
    }

    /// Inbound shuffle handle: the bounded queue + holdover so the drain/stage API
    /// works locally without a network. Parked behind a `Mutex<Option<_>>` for the
    /// same `Sync` reason as the gRPC build.
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
        /// No peer fabric without the cluster feature; matches the cluster build's API.
        #[allow(clippy::unused_self)]
        pub fn set_recovery_gen(&self, _gen: u64) {}

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
        /// frames are bucketed and barriers stashed.
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

        /// Drain every staged stage whose key starts with `prefix` in one lock
        /// cycle; other-stage frames are bucketed and barriers stashed. Operator
        /// stages stay in `staged`.
        #[must_use]
        pub fn drain_staged_with_prefix(
            &self,
            prefix: &str,
        ) -> FxHashMap<String, Vec<RecordBatch>> {
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
            let mut out: FxHashMap<String, Vec<RecordBatch>> = FxHashMap::default();
            staged.retain(|stage, batches| {
                if stage.starts_with(prefix) {
                    out.insert(stage.clone(), std::mem::take(batches));
                    false
                } else {
                    true
                }
            });
            out
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

    /// A receiver past a coordinated rewind must discard data frames a peer stamped before it —
    /// otherwise a pre-rewind frame still in flight folds onto the restored state and the peer's
    /// replay counts the same records again. Barriers are never generation-dropped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn receiver_drops_pre_rewind_data_frames() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let recv = bind_on_loopback(2).await;
        let sender = ShuffleSender::new(1);
        sender.register_peer(2, recv.local_addr()).await;

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .unwrap();

        // Receiver rewinds to generation 5; the sender is still stamping generation 0.
        recv.set_recovery_gen(5);
        sender
            .send_to(2, &ShuffleMessage::VnodeData("s".into(), 0, batch.clone()))
            .await
            .unwrap();
        // A barrier still gets through, proving the stream is live and only data was dropped.
        sender
            .send_to(2, &ShuffleMessage::Barrier(CheckpointBarrier::new(1, 1)))
            .await
            .unwrap();
        let (_, msg) = recv.recv().await.unwrap();
        assert!(
            matches!(msg, ShuffleMessage::Barrier(_)),
            "pre-rewind data frame must be dropped, barrier must survive; got {msg:?}"
        );

        // Once the sender catches up to the receiver's generation, data flows again.
        sender.set_recovery_gen(5);
        sender
            .send_to(2, &ShuffleMessage::VnodeData("s".into(), 0, batch))
            .await
            .unwrap();
        let (_, msg) = recv.recv().await.unwrap();
        assert!(
            matches!(msg, ShuffleMessage::VnodeData(..)),
            "same-generation data frame must be delivered; got {msg:?}"
        );
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

    /// `drain_staged_with_prefix` lifts `__sub::` stages in one pass while
    /// leaving operator stages staged for their own drainer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drain_staged_with_prefix_lifts_subs_and_keeps_operator_stages() {
        use arrow_array::{Int64Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema};
        use rustc_hash::FxHashMap;

        use crate::checkpoint::barrier::CheckpointBarrier;

        fn batch(values: Vec<i64>) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
        }
        fn col(b: &RecordBatch) -> Vec<i64> {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        }

        let recv = bind_on_loopback(2).await;
        let sender = ShuffleSender::new(1);
        sender.register_peer(2, recv.local_addr()).await;

        // FIFO over one stream: two subscription stages, one operator stage, then
        // a trailing barrier. Once the barrier is observed, every prior frame has
        // been received and bucketed.
        for (stage, vals) in [
            ("__sub::alpha", vec![1, 2, 3]),
            ("__sub::beta", vec![4, 5, 6]),
            ("op_stage", vec![7, 8, 9]),
        ] {
            sender
                .send_to(2, &ShuffleMessage::VnodeData(stage.into(), 0, batch(vals)))
                .await
                .unwrap();
        }
        sender
            .send_to(
                2,
                &ShuffleMessage::Barrier(CheckpointBarrier {
                    checkpoint_id: 7,
                    epoch: 3,
                    flags: 0,
                }),
            )
            .await
            .unwrap();

        // Poll the single-lock-cycle drain until both sub stages and the trailing
        // barrier have arrived (loopback is near-instant; 2s is a wide margin).
        let mut subs: FxHashMap<String, Vec<RecordBatch>> = FxHashMap::default();
        let mut barriers = Vec::new();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while subs.len() < 2 || barriers.is_empty() {
            for (k, v) in recv.drain_staged_with_prefix("__sub::") {
                subs.entry(k).or_default().extend(v);
            }
            barriers.extend(recv.drain_staged_barriers());
            assert!(
                std::time::Instant::now() < deadline,
                "frames not delivered within 2s",
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }

        // Both subscription stages lifted, with their batches intact.
        assert_eq!(subs.len(), 2, "only the two __sub:: stages are returned");
        assert_eq!(col(&subs["__sub::alpha"][0]), vec![1, 2, 3]);
        assert_eq!(col(&subs["__sub::beta"][0]), vec![4, 5, 6]);

        // The barrier was stashed, not dropped, and attributed to its sender.
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].0, 1, "barrier attributed to sender peer 1");
        assert_eq!(barriers[0].1.checkpoint_id, 7);

        // The operator stage was left intact for its own drainer.
        let op = recv.drain_vnode_data_for("op_stage");
        assert_eq!(op.len(), 1);
        assert_eq!(col(&op[0]), vec![7, 8, 9]);

        // A second prefix drain finds nothing new.
        assert!(recv.drain_staged_with_prefix("__sub::").is_empty());
    }
}
