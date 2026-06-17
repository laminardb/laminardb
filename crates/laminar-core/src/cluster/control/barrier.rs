//! Cross-instance barrier protocol. Direct gRPC leader-to-follower calls
//! under `cluster`, falling back to gossip-KV announce/ack/poll.

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use parking_lot::Mutex;
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};

use crate::cluster::discovery::NodeId;
#[cfg(feature = "cluster")]
use crate::cluster::discovery::{NodeInfo, NodeState};
#[cfg(feature = "cluster")]
use tokio::sync::watch;

/// KV key for the leader's barrier announcement.
pub const ANNOUNCEMENT_KEY: &str = "control:barrier";

/// KV key for a follower's barrier ack.
pub const ACK_KEY: &str = "control:barrier-ack";

/// Gossip KV key used by follower barrier servers to advertise their bound address.
#[cfg(feature = "cluster")]
pub const BARRIER_ADDR_KEY: &str = "barrier:addr";

/// Barrier phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Align the shuffle, capture state locally, ack. The durable tail
    /// (sink pre-commit, manifest, uploads) runs after the ack.
    Prepare,
    /// Every node has aligned + captured this epoch (full-membership
    /// capture quorum). Pipelines may resume the next epoch; the epoch
    /// is NOT yet restorable.
    Aligned,
    /// Durability gate passed; commit sinks. The epoch is restorable.
    Commit,
    /// Prepare failed; roll back.
    Abort,
}

/// Leader-written barrier announcement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAnnouncement {
    /// Monotonic epoch id.
    pub epoch: u64,
    /// Coordinator-assigned checkpoint id.
    pub checkpoint_id: u64,
    /// Phase this announcement signals.
    pub phase: Phase,
    /// Reserved for unaligned/other flags.
    pub flags: u64,
    /// Cluster-wide minimum watermark at announce time: the `min`
    /// across every live node's local watermark, computed by the
    /// leader from follower acks (see `BarrierAck.local_watermark_ms`)
    /// plus the leader's own watermark. Populated on
    /// [`Phase::Commit`] announcements. `None` on `Prepare`/`Abort`
    /// (computed only after acks are in) and on legacy payloads
    /// deserialised via the `#[serde(default)]` fallback.
    ///
    /// Consumers consult this value instead of their local watermark
    /// when deciding whether an event-time window has closed
    /// cluster-wide — local progress on one node is stale if another
    /// node is still processing earlier events.
    #[serde(default)]
    pub min_watermark_ms: Option<i64>,
}

/// Follower ack. `ok = false` forces the leader to abort instead of wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarrierAck {
    /// Epoch being acknowledged.
    pub epoch: u64,
    /// `false` = snapshot failed locally; leader should abort.
    pub ok: bool,
    /// Free-text error; populated when `ok = false`.
    pub error: Option<String>,
    /// Follower's local watermark at ack time (ms since epoch or
    /// arbitrary monotonic domain, matching the source's event-time
    /// units). The leader folds this into the cluster-wide min
    /// emitted in the matching `Commit` announcement.
    ///
    /// `None` means the follower's watermark is unset (fresh boot,
    /// no source events yet) — treated as "infinity" by the leader:
    /// it doesn't cap the cluster min downward.
    #[serde(default)]
    pub local_watermark_ms: Option<i64>,
}

/// Outcome of `wait_for_quorum`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumOutcome {
    /// All expected peers acked with `ok = true`.
    Reached {
        /// Peers that acked successfully.
        acks: Vec<NodeId>,
        /// The minimum watermark across every successful ack's
        /// `local_watermark_ms` (ignoring `None` values). `None`
        /// means no follower reported a watermark — the leader
        /// falls back to its own local value for the Commit
        /// announcement.
        min_follower_watermark_ms: Option<i64>,
    },
    /// Deadline expired with at least one peer silent.
    TimedOut {
        /// Peers that did ack.
        got: Vec<NodeId>,
        /// Peers that didn't.
        missing: Vec<NodeId>,
    },
    /// At least one peer acked `ok = false`.
    Failed {
        /// `(peer, error_message)` for every failed ack.
        failures: Vec<(NodeId, String)>,
    },
}

/// Gossip-KV seam.
#[async_trait]
pub trait ClusterKv: Send + Sync + 'static {
    /// Write `value` to this instance's `key` slot (overwrites).
    async fn write(&self, key: &str, value: String);
    /// Read `key` from `who`'s slot.
    async fn read_from(&self, who: NodeId, key: &str) -> Option<String>;
    /// Every visible instance's value for `key`.
    async fn scan(&self, key: &str) -> Vec<(NodeId, String)>;
    /// Every visible instance's key-value pairs where the key starts with the given prefix.
    async fn scan_prefix(&self, prefix: &str) -> Vec<(NodeId, String, String)>;
    /// Whether the backend supports the subscription-interest scan the
    /// distributed SUBSCRIBE router needs (object stores return `false`).
    fn supports_subscription_routing(&self) -> bool {
        true
    }
}

/// In-memory KV for tests.
#[derive(Debug)]
pub struct InMemoryKv {
    local_id: NodeId,
    state: Mutex<FxHashMap<(NodeId, String), String>>,
}

impl InMemoryKv {
    /// Create a new in-memory KV identified as `local_id`.
    #[must_use]
    pub fn new(local_id: NodeId) -> Self {
        Self {
            local_id,
            state: Mutex::new(FxHashMap::default()),
        }
    }

    /// Seed a remote peer's state for tests.
    pub fn seed(&self, peer: NodeId, key: &str, value: String) {
        self.state.lock().insert((peer, key.to_string()), value);
    }
}

#[async_trait]
impl ClusterKv for InMemoryKv {
    async fn write(&self, key: &str, value: String) {
        self.state
            .lock()
            .insert((self.local_id, key.to_string()), value);
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        self.state.lock().get(&(who, key.to_string())).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        self.state
            .lock()
            .iter()
            .filter(|((_, k), _)| k == key)
            .map(|((n, _), v)| (*n, v.clone()))
            .collect()
    }

    async fn scan_prefix(&self, prefix: &str) -> Vec<(NodeId, String, String)> {
        self.state
            .lock()
            .iter()
            .filter(|((_, k), _)| k.starts_with(prefix))
            .map(|((n, k), v)| (*n, k.clone(), v.clone()))
            .collect()
    }
}

#[cfg(feature = "cluster")]
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::missing_const_for_fn,
    clippy::must_use_candidate,
    clippy::too_many_lines,
    missing_docs
)]
pub(crate) mod barrier_v1 {
    tonic::include_proto!("laminar.barrier.v1");
}

#[cfg(feature = "cluster")]
type BarrierFlavor = crossfire::mpsc::Array<BarrierAnnouncement>;

/// Per-peer barrier gRPC client pool, keyed by `NodeId`. Entries are evicted on
/// RPC failure so a restarted/moved peer is re-resolved on the next round.
#[cfg(feature = "cluster")]
type BarrierClientPool = Arc<
    parking_lot::Mutex<
        FxHashMap<
            NodeId,
            barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>,
        >,
    >,
>;

#[cfg(feature = "cluster")]
struct GrpcState {
    /// Latest gRPC-delivered announcement, fed in arrival order by the
    /// relay task draining the incoming queue. Latest-wins semantics
    /// (matching the gossip-KV fallback) so concurrent observers — the
    /// pipeline's resume gate and the background durable tail — never
    /// steal announcements from each other.
    latest_rx: watch::Receiver<Option<BarrierAnnouncement>>,
    #[allow(dead_code)]
    incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    pending_acks: Arc<parking_lot::Mutex<FxHashMap<u64, tokio::sync::oneshot::Sender<BarrierAck>>>>,
    completed_acks: Arc<parking_lot::Mutex<FxHashMap<u64, BarrierAck>>>,
    clients: BarrierClientPool,
    server_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    relay_handle: Arc<parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>>,
    advertise_addr: String,
}

#[cfg(feature = "cluster")]
type ActiveLeaderState = Option<(NodeId, watch::Receiver<Vec<NodeInfo>>)>;

#[cfg(feature = "cluster")]
struct GrpcBarrierServer {
    kv: Arc<dyn ClusterKv>,
    incoming_tx: crossfire::MAsyncTx<BarrierFlavor>,
    pending_acks: Arc<parking_lot::Mutex<FxHashMap<u64, tokio::sync::oneshot::Sender<BarrierAck>>>>,
    completed_acks: Arc<parking_lot::Mutex<FxHashMap<u64, BarrierAck>>>,
    leader_election: Arc<parking_lot::Mutex<ActiveLeaderState>>,
}

#[cfg(feature = "cluster")]
impl GrpcBarrierServer {
    async fn validate_leader(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<(), tonic::Status> {
        let leader_id_str = metadata
            .get("x-leader-id")
            .ok_or_else(|| tonic::Status::permission_denied("Missing leader identity"))?
            .to_str()
            .map_err(|_| tonic::Status::permission_denied("Invalid leader identity"))?;
        let leader_id_u64 = leader_id_str
            .parse::<u64>()
            .map_err(|_| tonic::Status::permission_denied("Invalid leader identity"))?;
        let sender_leader_id = NodeId(leader_id_u64);

        let election_state = self.leader_election.lock().clone();

        let observed_leader = if let Some((instance_id, members_rx)) = election_state {
            let members = members_rx.borrow();
            let mut ids: Vec<NodeId> = members
                .iter()
                .filter(|m| matches!(m.state, NodeState::Active))
                .map(|m| m.id)
                .collect();
            ids.push(instance_id);
            super::leader_of(&ids)
        } else {
            let live_nodes: Vec<NodeId> = self
                .kv
                .scan(BARRIER_ADDR_KEY)
                .await
                .into_iter()
                .map(|(id, _)| id)
                .collect();
            super::leader_of(&live_nodes)
        };

        if Some(sender_leader_id) != observed_leader {
            return Err(tonic::Status::permission_denied(
                "Sender is not the observed leader",
            ));
        }
        Ok(())
    }
}

#[cfg(feature = "cluster")]
#[tonic::async_trait]
impl barrier_v1::barrier_sync_server::BarrierSync for GrpcBarrierServer {
    async fn prepare(
        &self,
        request: tonic::Request<barrier_v1::PrepareRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        let validation_res = self.validate_leader(request.metadata()).await;
        let req = request.into_inner();

        {
            let mut completed = self.completed_acks.lock();
            if let Some(ack) = completed.remove(&req.epoch) {
                validation_res?;
                return Ok(tonic::Response::new(barrier_v1::Ack {
                    epoch: ack.epoch,
                    ok: ack.ok,
                    error: ack.error,
                    local_watermark_ms: ack.local_watermark_ms,
                }));
            }
        }

        let (tx, rx) = tokio::sync::oneshot::channel::<BarrierAck>();

        {
            let mut guard = self.pending_acks.lock();
            guard.insert(req.epoch, tx);
        }

        if let Err(status) = validation_res {
            let mut guard = self.pending_acks.lock();
            guard.remove(&req.epoch);
            return Err(status);
        }

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            phase: Phase::Prepare,
            flags: req.flags,
            min_watermark_ms: None,
        };

        if self.incoming_tx.send(ann).await.is_err() {
            let mut guard = self.pending_acks.lock();
            guard.remove(&req.epoch);
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }

        match tokio::time::timeout(Duration::from_secs(30), rx).await {
            Ok(Ok(ack)) => Ok(tonic::Response::new(barrier_v1::Ack {
                epoch: ack.epoch,
                ok: ack.ok,
                error: ack.error,
                local_watermark_ms: ack.local_watermark_ms,
            })),
            Ok(Err(_)) => Err(tonic::Status::internal("Ack sender dropped")),
            Err(_) => {
                let mut guard = self.pending_acks.lock();
                guard.remove(&req.epoch);
                Err(tonic::Status::deadline_exceeded(
                    "Follower checkpoint prepare timed out",
                ))
            }
        }
    }

    async fn aligned(
        &self,
        request: tonic::Request<barrier_v1::AlignedRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.validate_leader(request.metadata()).await?;
        let req = request.into_inner();

        // Unlike Commit/Abort, Aligned is mid-protocol: the epoch's ack
        // bookkeeping stays untouched — only the announcement is relayed
        // so the pipeline's resume gate can release.
        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            phase: Phase::Aligned,
            flags: req.flags,
            min_watermark_ms: req.min_watermark_ms,
        };
        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
        }))
    }

    async fn commit(
        &self,
        request: tonic::Request<barrier_v1::CommitRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.validate_leader(request.metadata()).await?;
        let req = request.into_inner();

        {
            let mut completed = self.completed_acks.lock();
            completed.remove(&req.epoch);
            completed.retain(|&epoch, _| epoch >= req.epoch);
        }

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            phase: Phase::Commit,
            flags: req.flags,
            min_watermark_ms: req.min_watermark_ms,
        };
        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
        }))
    }

    async fn abort(
        &self,
        request: tonic::Request<barrier_v1::AbortRequest>,
    ) -> Result<tonic::Response<barrier_v1::Ack>, tonic::Status> {
        self.validate_leader(request.metadata()).await?;
        let req = request.into_inner();

        {
            let mut completed = self.completed_acks.lock();
            completed.remove(&req.epoch);
            completed.retain(|&epoch, _| epoch >= req.epoch);
        }

        let ann = BarrierAnnouncement {
            epoch: req.epoch,
            checkpoint_id: req.checkpoint_id,
            phase: Phase::Abort,
            flags: req.flags,
            min_watermark_ms: None,
        };
        if self.incoming_tx.send(ann).await.is_err() {
            return Err(tonic::Status::aborted("Follower coordinator shutdown"));
        }
        Ok(tonic::Response::new(barrier_v1::Ack {
            epoch: req.epoch,
            ok: true,
            error: None,
            local_watermark_ms: None,
        }))
    }
}

#[cfg(feature = "cluster")]
async fn get_barrier_client(
    peer: NodeId,
    pool: &BarrierClientPool,
    kv: &Arc<dyn ClusterKv>,
) -> Option<barrier_v1::barrier_sync_client::BarrierSyncClient<tonic::transport::Channel>> {
    if let Some(client) = pool.lock().get(&peer) {
        return Some(client.clone());
    }

    let addr_str = kv.read_from(peer, BARRIER_ADDR_KEY).await?;
    let endpoint = super::tls::client_endpoint(&addr_str).ok()?;
    let channel = endpoint.connect_lazy();
    let client = barrier_v1::barrier_sync_client::BarrierSyncClient::new(channel);

    pool.lock().insert(peer, client.clone());
    Some(client)
}

/// Stamp the leader's identity into the request metadata so the
/// follower's `validate_leader` check can reject impostors.
#[cfg(feature = "cluster")]
fn stamp_leader_id<T>(req: &mut tonic::Request<T>, local_id: Option<NodeId>) {
    if let Some(lid) = local_id {
        if let Ok(val) = lid.0.to_string().parse() {
            req.metadata_mut().insert("x-leader-id", val);
        }
    }
}

/// Fan a non-Prepare phase announcement to one peer over gRPC. A failed
/// RPC evicts the pooled client so the next round re-resolves the peer.
#[cfg(feature = "cluster")]
async fn send_phase_rpc(
    peer: NodeId,
    clients_pool: BarrierClientPool,
    kv: Arc<dyn ClusterKv>,
    ann: BarrierAnnouncement,
    local_id: Option<NodeId>,
) -> Result<(), String> {
    let mut client = get_barrier_client(peer, &clients_pool, &kv)
        .await
        .ok_or_else(|| format!("failed to get client for peer {}", peer.0))?;
    let result = match ann.phase {
        Phase::Aligned => {
            let mut req = tonic::Request::new(barrier_v1::AlignedRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
                min_watermark_ms: ann.min_watermark_ms,
            });
            stamp_leader_id(&mut req, local_id);
            client
                .aligned(req)
                .await
                .map(|_| ())
                .map_err(|e| ("aligned", e))
        }
        Phase::Commit => {
            let mut req = tonic::Request::new(barrier_v1::CommitRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
                min_watermark_ms: ann.min_watermark_ms,
            });
            stamp_leader_id(&mut req, local_id);
            client
                .commit(req)
                .await
                .map(|_| ())
                .map_err(|e| ("commit", e))
        }
        Phase::Abort => {
            let mut req = tonic::Request::new(barrier_v1::AbortRequest {
                epoch: ann.epoch,
                checkpoint_id: ann.checkpoint_id,
                flags: ann.flags,
            });
            stamp_leader_id(&mut req, local_id);
            client
                .abort(req)
                .await
                .map(|_| ())
                .map_err(|e| ("abort", e))
        }
        Phase::Prepare => Ok(()),
    };
    result.map_err(|(rpc, e)| {
        clients_pool.lock().remove(&peer);
        format!("{rpc} RPC to peer {} failed: {e}", peer.0)
    })
}

/// Typed prepare-failure classification for the quorum wait:
/// `Unreachable` counts toward `TimedOut{missing}` (the peer cannot
/// participate), `Nack` toward `Failed` (a live follower answered
/// `ok = false`).
#[cfg(feature = "cluster")]
enum PeerFailure {
    Unreachable,
    Nack(String),
}

/// Cross-instance barrier coordination.
pub struct BarrierCoordinator {
    kv: Arc<dyn ClusterKv>,
    #[cfg(feature = "cluster")]
    grpc: Arc<parking_lot::Mutex<Option<Arc<GrpcState>>>>,
    #[cfg(feature = "cluster")]
    leader_election: Arc<parking_lot::Mutex<ActiveLeaderState>>,
}

impl std::fmt::Debug for BarrierCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BarrierCoordinator").finish_non_exhaustive()
    }
}

impl Drop for BarrierCoordinator {
    fn drop(&mut self) {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().take();
            if let Some(state) = grpc_opt {
                let handle_opt = state.server_handle.lock().take();
                if let Some(handle) = handle_opt {
                    handle.abort();
                }
                let relay_opt = state.relay_handle.lock().take();
                if let Some(handle) = relay_opt {
                    handle.abort();
                }
            }
        }
    }
}

impl BarrierCoordinator {
    /// Wrap a KV implementation.
    #[must_use]
    pub fn new(kv: Arc<dyn ClusterKv>) -> Self {
        Self {
            kv,
            #[cfg(feature = "cluster")]
            grpc: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            leader_election: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Configure the leader election state used to validate incoming leader identity.
    #[cfg(feature = "cluster")]
    pub fn set_leader_election(
        &mut self,
        instance_id: NodeId,
        members_rx: watch::Receiver<Vec<NodeInfo>>,
    ) {
        *self.leader_election.lock() = Some((instance_id, members_rx));
    }

    #[cfg(feature = "cluster")]
    async fn local_node_id(&self) -> Option<NodeId> {
        let grpc_opt = self.grpc.lock().clone();
        let state = grpc_opt?;
        let local_addr_str = state.advertise_addr.clone();
        for (node_id, addr) in self.kv.scan(BARRIER_ADDR_KEY).await {
            if addr == local_addr_str {
                return Some(node_id);
            }
        }
        None
    }

    /// Bind and run the follower's direct gRPC barrier sync server.
    ///
    /// # Errors
    /// Returns an error string on bind or socket address retrieval failures.
    #[cfg(feature = "cluster")]
    pub async fn start_server(
        &self,
        bind_addr: std::net::SocketAddr,
        advertise_host: Option<String>,
        query_handler: super::QueryHandlerSlot,
    ) -> Result<std::net::SocketAddr, String> {
        use super::query::query_service_server;
        use barrier_v1::barrier_sync_server::BarrierSyncServer;
        use std::net::TcpListener;
        use tonic::transport::Server;

        let listener = TcpListener::bind(bind_addr).map_err(|e| e.to_string())?;
        let local_addr = listener.local_addr().map_err(|e| e.to_string())?;
        listener.set_nonblocking(true).map_err(|e| e.to_string())?;
        let tokio_listener =
            tokio::net::TcpListener::from_std(listener).map_err(|e| e.to_string())?;

        let (incoming_tx, incoming_rx) = crossfire::mpsc::bounded_async::<BarrierAnnouncement>(128);
        let pending_acks = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
        let completed_acks = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));
        let clients = Arc::new(parking_lot::Mutex::new(FxHashMap::default()));

        let server_impl = GrpcBarrierServer {
            kv: Arc::clone(&self.kv),
            incoming_tx: incoming_tx.clone(),
            pending_acks: Arc::clone(&pending_acks),
            completed_acks: Arc::clone(&completed_acks),
            leader_election: Arc::clone(&self.leader_election),
        };

        // The pull-path query service shares this control-plane port; peers
        // reach it at the same address published under `BARRIER_ADDR_KEY`.
        let query_svc = query_service_server(query_handler);
        // Apply TLS synchronously so a bad cert fails start_server (before
        // publishing BARRIER_ADDR_KEY) rather than silently never serving.
        let mut builder = Server::builder();
        if let Some(tls) = super::tls::server_tls() {
            builder = builder
                .tls_config(tls.clone())
                .map_err(|e| format!("cluster control-plane TLS config: {e}"))?;
        }
        let router = builder
            .add_service(BarrierSyncServer::new(server_impl))
            .add_service(query_svc);
        let server_task = tokio::spawn(async move {
            let incoming_stream = tokio_stream::wrappers::TcpListenerStream::new(tokio_listener);
            let _ = router.serve_with_incoming(incoming_stream).await;
        });

        let advertise_addr = if let Some(ref host) = advertise_host {
            format!("{host}:{}", local_addr.port())
        } else if local_addr.ip().is_unspecified() {
            let hostname = gethostname::gethostname();
            let hostname = hostname.to_string_lossy();
            if hostname.is_empty() {
                local_addr.to_string()
            } else {
                format!("{hostname}:{}", local_addr.port())
            }
        } else {
            local_addr.to_string()
        };

        // Relay every gRPC-delivered announcement into a latest-wins
        // watch in arrival order. Observation is then non-destructive,
        // so the pipeline's resume gate and the background durable
        // tail can watch concurrently (matching the gossip-KV
        // fallback's read-latest semantics).
        let (latest_tx, latest_rx) = watch::channel::<Option<BarrierAnnouncement>>(None);
        let relay_task = tokio::spawn(async move {
            while let Ok(ann) = incoming_rx.recv().await {
                let _ = latest_tx.send(Some(ann));
            }
        });

        let grpc_state = Arc::new(GrpcState {
            latest_rx,
            incoming_tx,
            pending_acks,
            completed_acks,
            clients,
            server_handle: Arc::new(parking_lot::Mutex::new(Some(server_task))),
            relay_handle: Arc::new(parking_lot::Mutex::new(Some(relay_task))),
            advertise_addr: advertise_addr.clone(),
        });

        *self.grpc.lock() = Some(grpc_state);

        self.kv.write(BARRIER_ADDR_KEY, advertise_addr).await;

        Ok(local_addr)
    }

    /// Leader-side announce.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn announce(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                let local_id = self.local_node_id().await;
                if ann.phase == Phase::Prepare {
                    // Prepare gRPC calls are initiated by wait_for_quorum.
                    // Redundant calls here cause duplicate prepare executions and timeouts on followers.
                } else {
                    // A node's barrier address lingers in the KV after it dies,
                    // so announce to peers still Active in membership — a Commit
                    // RPC to a departed peer returns Err and wedges every epoch.
                    // The KV-fallback write below still lets a recovering peer
                    // observe the announcement.
                    let live: Option<FxHashSet<NodeId>> =
                        self.leader_election.lock().clone().map(|(_, members_rx)| {
                            members_rx
                                .borrow()
                                .iter()
                                .filter(|m| matches!(m.state, NodeState::Active))
                                .map(|m| m.id)
                                .collect()
                        });
                    let mut expected = Vec::new();
                    for (node_id, addr) in self.kv.scan(BARRIER_ADDR_KEY).await {
                        if addr == state.advertise_addr {
                            continue;
                        }
                        if live.as_ref().is_some_and(|live| !live.contains(&node_id)) {
                            continue;
                        }
                        expected.push(node_id);
                    }

                    let mut futures = Vec::new();
                    for peer in expected {
                        let clients_pool = Arc::clone(&state.clients);
                        let kv = Arc::clone(&self.kv);
                        let ann_clone = ann.clone();
                        futures.push(send_phase_rpc(peer, clients_pool, kv, ann_clone, local_id));
                    }
                    let results = futures::future::join_all(futures).await;
                    for res in results {
                        match res {
                            Ok(()) => {}
                            // Aligned is best-effort per peer: a missed
                            // delivery only delays that peer's pipeline
                            // resume until Commit (or its gate timeout) —
                            // never fail the announce, and never skip the
                            // KV write below.
                            Err(e) if ann.phase == Phase::Aligned => {
                                tracing::warn!(
                                    epoch = ann.epoch,
                                    error = %e,
                                    "aligned announcement RPC failed; peer resumes on Commit"
                                );
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }

                let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
                self.kv.write(ANNOUNCEMENT_KEY, json).await;
                return Ok(());
            }
        }

        let json = serde_json::to_string(ann).map_err(|e| e.to_string())?;
        self.kv.write(ANNOUNCEMENT_KEY, json).await;
        Ok(())
    }

    /// Watch over gRPC-delivered announcements, for push-driven waits
    /// (the decision wait and the Aligned resume gate). `None` until
    /// the gRPC server is started — gossip-KV-only deployments fall
    /// back to polling [`observe`](Self::observe).
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn announcement_watch(&self) -> Option<watch::Receiver<Option<BarrierAnnouncement>>> {
        self.grpc.lock().as_ref().map(|s| s.latest_rx.clone())
    }

    /// Follower-side observe — returns the *latest* announcement
    /// (non-destructive; repeated calls return the same value until a
    /// newer one arrives, matching the gossip-KV fallback). Callers
    /// already dedup by epoch/phase. The gRPC-delivered value and the
    /// gossip-KV value are merged by epoch (higher wins — epochs are
    /// never reused: failed ones are abandoned); within an epoch the
    /// gRPC value wins, since RPC arrival order is authoritative while
    /// gossip may lag.
    ///
    /// # Errors
    /// Returns a string on JSON decode failure.
    pub async fn observe(&self, leader: NodeId) -> Result<Option<BarrierAnnouncement>, String> {
        #[cfg(feature = "cluster")]
        let grpc_latest: Option<BarrierAnnouncement> = {
            let grpc_opt = self.grpc.lock().clone();
            grpc_opt.and_then(|state| state.latest_rx.borrow().clone())
        };
        #[cfg(not(feature = "cluster"))]
        let grpc_latest: Option<BarrierAnnouncement> = None;

        let kv_latest: Option<BarrierAnnouncement> =
            match self.kv.read_from(leader, ANNOUNCEMENT_KEY).await {
                Some(json) => match serde_json::from_str(&json) {
                    Ok(a) => Some(a),
                    // A decode failure on the KV path is fatal only when
                    // there is no gRPC value to fall back on.
                    Err(e) if grpc_latest.is_some() => {
                        tracing::warn!(error = %e, "corrupt gossip announcement; using gRPC value");
                        None
                    }
                    Err(e) => return Err(e.to_string()),
                },
                None => None,
            };

        Ok(match (grpc_latest, kv_latest) {
            (Some(g), Some(k)) => {
                if k.epoch > g.epoch {
                    Some(k)
                } else {
                    Some(g)
                }
            }
            (Some(g), None) => Some(g),
            (None, k) => k,
        })
    }

    /// Highest `(epoch, checkpoint_id)` any node has announced, across the gossiped
    /// per-node announcement keys — a reclaiming leader advances past it on rejoin.
    #[must_use]
    pub async fn max_announced(&self) -> Option<(u64, u64)> {
        self.kv
            .scan(ANNOUNCEMENT_KEY)
            .await
            .into_iter()
            .filter_map(|(_, json)| serde_json::from_str::<BarrierAnnouncement>(&json).ok())
            .map(|a| (a.epoch, a.checkpoint_id))
            .max()
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn ack(&self, ack: &BarrierAck) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                {
                    let mut completed = state.completed_acks.lock();
                    completed.insert(ack.epoch, ack.clone());
                }
                let tx_opt = {
                    let mut guard = state.pending_acks.lock();
                    guard.remove(&ack.epoch)
                };
                if let Some(tx) = tx_opt {
                    let _ = tx.send(ack.clone());
                }
                return Ok(());
            }
        }

        let json = serde_json::to_string(ack).map_err(|e| e.to_string())?;
        self.kv.write(ACK_KEY, json).await;
        Ok(())
    }

    /// Leader-side: wait until quorum or `deadline`.
    #[allow(clippy::too_many_lines)]
    // `PeerFailure` (module level, below) classifies each peer's
    // prepare outcome.
    pub async fn wait_for_quorum(
        &self,
        epoch: u64,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                let checkpoint_id =
                    match self
                        .kv
                        .scan(ANNOUNCEMENT_KEY)
                        .await
                        .into_iter()
                        .find(|(_, json)| {
                            serde_json::from_str::<BarrierAnnouncement>(json)
                                .is_ok_and(|a| a.epoch == epoch)
                        }) {
                        Some((_, json)) => serde_json::from_str::<BarrierAnnouncement>(&json)
                            .map_or(0, |a| a.checkpoint_id),
                        None => 0,
                    };

                let local_id = self.local_node_id().await;
                let mut futures = Vec::new();
                for &peer in expected {
                    let clients_pool = Arc::clone(&state.clients);
                    let kv = Arc::clone(&self.kv);
                    futures.push(async move {
                        let client_opt = get_barrier_client(peer, &clients_pool, &kv).await;
                        let Some(mut client) = client_opt else {
                            return Err((peer, PeerFailure::Unreachable));
                        };

                        let mut req = tonic::Request::new(barrier_v1::PrepareRequest {
                            epoch,
                            checkpoint_id,
                            flags: 0,
                        });
                        stamp_leader_id(&mut req, local_id);

                        match tokio::time::timeout(deadline, client.prepare(req)).await {
                            Ok(Ok(response)) => {
                                let ack = response.into_inner();
                                if ack.ok {
                                    Ok((peer, ack.local_watermark_ms))
                                } else {
                                    Err((
                                        peer,
                                        PeerFailure::Nack(ack.error.unwrap_or_else(|| {
                                            "Unknown prepare failure".to_string()
                                        })),
                                    ))
                                }
                            }
                            Ok(Err(status)) => {
                                clients_pool.lock().remove(&peer);
                                // Classify by gRPC status code, not message
                                // text: transport-level codes mean the peer
                                // cannot participate (same epistemic state
                                // as a timeout); anything else is a live
                                // server refusing the call.
                                match status.code() {
                                    tonic::Code::Unavailable
                                    | tonic::Code::DeadlineExceeded
                                    | tonic::Code::Cancelled
                                    | tonic::Code::Aborted => Err((peer, PeerFailure::Unreachable)),
                                    _ => Err((peer, PeerFailure::Nack(status.to_string()))),
                                }
                            }
                            Err(_) => {
                                clients_pool.lock().remove(&peer);
                                Err((peer, PeerFailure::Unreachable))
                            }
                        }
                    });
                }

                let results = futures::future::join_all(futures).await;

                let mut successful = Vec::new();
                let mut failures = Vec::new();
                let mut min_follower_wm: Option<i64> = None;
                let mut timed_out = Vec::new();

                for res in results {
                    match res {
                        Ok((peer, wm)) => {
                            successful.push(peer);
                            if let Some(w) = wm {
                                min_follower_wm = Some(match min_follower_wm {
                                    Some(cur) => cur.min(w),
                                    None => w,
                                });
                            }
                        }
                        Err((peer, PeerFailure::Unreachable)) => timed_out.push(peer),
                        Err((peer, PeerFailure::Nack(msg))) => failures.push((peer, msg)),
                    }
                }

                if !failures.is_empty() {
                    return QuorumOutcome::Failed { failures };
                }

                if !timed_out.is_empty() || successful.len() < expected.len() {
                    let got = successful;
                    let mut missing = timed_out;
                    for &peer in expected {
                        if !got.contains(&peer) && !missing.contains(&peer) {
                            missing.push(peer);
                        }
                    }
                    return QuorumOutcome::TimedOut { got, missing };
                }

                return QuorumOutcome::Reached {
                    acks: successful,
                    min_follower_watermark_ms: min_follower_wm,
                };
            }
        }

        let start = Instant::now();
        let expected_set: FxHashSet<NodeId> = expected.iter().copied().collect();
        let mut successful: Vec<NodeId> = Vec::new();
        let mut failures: Vec<(NodeId, String)> = Vec::new();
        let mut min_follower_wm: Option<i64>;

        loop {
            successful.clear();
            failures.clear();
            min_follower_wm = None;

            for (from, json) in self.kv.scan(ACK_KEY).await {
                if !expected_set.contains(&from) {
                    continue;
                }
                let Ok(ack) = serde_json::from_str::<BarrierAck>(&json) else {
                    continue;
                };
                if ack.epoch != epoch {
                    continue;
                }
                if ack.ok {
                    successful.push(from);
                    if let Some(wm) = ack.local_watermark_ms {
                        min_follower_wm = Some(match min_follower_wm {
                            Some(cur) => cur.min(wm),
                            None => wm,
                        });
                    }
                } else {
                    failures.push((from, ack.error.unwrap_or_default()));
                }
            }

            if !failures.is_empty() {
                return QuorumOutcome::Failed { failures };
            }
            if successful.len() == expected.len() {
                return QuorumOutcome::Reached {
                    acks: successful,
                    min_follower_watermark_ms: min_follower_wm,
                };
            }
            if start.elapsed() >= deadline {
                let got: FxHashSet<NodeId> = successful.iter().copied().collect();
                let missing: Vec<NodeId> = expected
                    .iter()
                    .copied()
                    .filter(|n| !got.contains(n))
                    .collect();
                return QuorumOutcome::TimedOut {
                    got: successful,
                    missing,
                };
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(id: NodeId) -> Arc<InMemoryKv> {
        Arc::new(InMemoryKv::new(id))
    }

    #[cfg(all(test, feature = "cluster"))]
    mod grpc_tests {
        use super::*;
        use std::net::SocketAddr;

        /// Observation is latest-wins (non-destructive), so wait for the
        /// expected phase specifically — earlier phases may linger.
        async fn wait_observe(
            coord: &BarrierCoordinator,
            leader: NodeId,
            phase: Phase,
        ) -> BarrierAnnouncement {
            for _ in 0..100 {
                if let Some(ann) = coord.observe(leader).await.unwrap() {
                    if ann.phase == phase {
                        return ann;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            panic!("timed out waiting for {phase:?} announcement from leader {leader:?}");
        }

        #[tokio::test]
        async fn test_grpc_barrier_flow() {
            let leader_kv = kv(NodeId(1));
            let follower_kv = kv(NodeId(2));
            let leader_coord = BarrierCoordinator::new(leader_kv.clone());
            let follower_coord = BarrierCoordinator::new(follower_kv.clone());

            let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
            let slot = || Arc::new(parking_lot::RwLock::new(None));
            let leader_addr = leader_coord.start_server(addr, None, slot()).await.unwrap();
            let bound_addr = follower_coord
                .start_server(addr, None, slot())
                .await
                .unwrap();

            leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, bound_addr.to_string());
            follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

            // Sequencing handshake: observation is latest-wins, so the
            // leader must not announce Commit until the follower has
            // observed Aligned (otherwise Commit may overwrite it).
            let (aligned_seen_tx, aligned_seen_rx) = tokio::sync::oneshot::channel::<()>();

            let follower_task = tokio::spawn(async move {
                let ann = wait_observe(&follower_coord, NodeId(1), Phase::Prepare).await;
                assert_eq!(ann.epoch, 1);
                assert_eq!(ann.checkpoint_id, 42);

                follower_coord
                    .ack(&BarrierAck {
                        epoch: 1,
                        ok: true,
                        error: None,
                        local_watermark_ms: Some(100),
                    })
                    .await
                    .unwrap();

                let aligned_ann = wait_observe(&follower_coord, NodeId(1), Phase::Aligned).await;
                assert_eq!(aligned_ann.epoch, 1);
                assert_eq!(aligned_ann.min_watermark_ms, Some(100));
                aligned_seen_tx.send(()).unwrap();

                let commit_ann = wait_observe(&follower_coord, NodeId(1), Phase::Commit).await;
                assert_eq!(commit_ann.min_watermark_ms, Some(100));
            });

            leader_coord
                .announce(&BarrierAnnouncement {
                    epoch: 1,
                    checkpoint_id: 42,
                    phase: Phase::Prepare,
                    flags: 0,
                    min_watermark_ms: None,
                })
                .await
                .unwrap();

            let outcome = leader_coord
                .wait_for_quorum(1, &[NodeId(2)], Duration::from_secs(5))
                .await;
            match outcome {
                QuorumOutcome::Reached {
                    acks,
                    min_follower_watermark_ms,
                } => {
                    assert_eq!(acks, vec![NodeId(2)]);
                    assert_eq!(min_follower_watermark_ms, Some(100));

                    // Two-level completion: resume gate first…
                    leader_coord
                        .announce(&BarrierAnnouncement {
                            epoch: 1,
                            checkpoint_id: 42,
                            phase: Phase::Aligned,
                            flags: 0,
                            min_watermark_ms: min_follower_watermark_ms,
                        })
                        .await
                        .unwrap();
                    aligned_seen_rx.await.unwrap();

                    // …then the restorable decision.
                    leader_coord
                        .announce(&BarrierAnnouncement {
                            epoch: 1,
                            checkpoint_id: 42,
                            phase: Phase::Commit,
                            flags: 0,
                            min_watermark_ms: min_follower_watermark_ms,
                        })
                        .await
                        .unwrap();
                }
                other => panic!("expected Reached, got {other:?}"),
            }

            follower_task.await.unwrap();
        }
    }

    /// The gRPC-vs-gossip merge in `observe`: a newer epoch's
    /// gossip-only announcement (the early `Prepare` is KV-only)
    /// supersedes an older epoch in the gRPC watch, while lagging
    /// gossip for the *same* epoch never masks the fresher gRPC value.
    /// Epochs are never reused — failed ones are abandoned — so epoch
    /// comparison alone orders the channels.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn observe_merges_grpc_and_gossip_by_epoch() {
        let leader_kv = kv(NodeId(1));
        let follower_kv = kv(NodeId(2));
        let leader_coord = BarrierCoordinator::new(leader_kv.clone());
        let follower_coord = BarrierCoordinator::new(follower_kv.clone());

        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let slot = || Arc::new(parking_lot::RwLock::new(None));
        let leader_addr = leader_coord.start_server(addr, None, slot()).await.unwrap();
        let bound_addr = follower_coord
            .start_server(addr, None, slot())
            .await
            .unwrap();
        leader_kv.seed(NodeId(2), BARRIER_ADDR_KEY, bound_addr.to_string());
        follower_kv.seed(NodeId(1), BARRIER_ADDR_KEY, leader_addr.to_string());

        // Epoch 5 aborts — delivered over gRPC, lands in the
        // follower's latest-wins watch.
        leader_coord
            .announce(&BarrierAnnouncement {
                epoch: 5,
                checkpoint_id: 9,
                phase: Phase::Abort,
                flags: 0,
                min_watermark_ms: None,
            })
            .await
            .unwrap();
        for _ in 0..100 {
            if let Some(ann) = follower_coord.observe(NodeId(1)).await.unwrap() {
                if ann.phase == Phase::Abort {
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // The next epoch's early Prepare reaches this follower via
        // gossip KV only (its prepare RPC comes later, at quorum time)
        // and must win the merge over the stale watch value.
        let next = serde_json::to_string(&BarrierAnnouncement {
            epoch: 6,
            checkpoint_id: 10,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        follower_kv.seed(NodeId(1), ANNOUNCEMENT_KEY, next);
        let got = follower_coord.observe(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(got.epoch, 6);
        assert_eq!(got.phase, Phase::Prepare);

        // Same-epoch lagging gossip must not mask the fresher gRPC
        // value (RPC arrival order is authoritative within an epoch).
        let stale = serde_json::to_string(&BarrierAnnouncement {
            epoch: 5,
            checkpoint_id: 9,
            phase: Phase::Prepare,
            flags: 0,
            min_watermark_ms: None,
        })
        .unwrap();
        follower_kv.seed(NodeId(1), ANNOUNCEMENT_KEY, stale);
        let got = follower_coord.observe(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(
            got.phase,
            Phase::Abort,
            "lagging gossip must not mask the fresher gRPC announcement",
        );
    }

    #[tokio::test]
    async fn leader_announces_follower_observes() {
        let leader_kv = kv(NodeId(1));
        let coord = BarrierCoordinator::new(leader_kv.clone());
        coord
            .announce(&BarrierAnnouncement {
                epoch: 5,
                checkpoint_id: 42,
                phase: Phase::Prepare,
                flags: 0,
                min_watermark_ms: None,
            })
            .await
            .unwrap();
        let got = coord.observe(NodeId(1)).await.unwrap().unwrap();
        assert_eq!(got.epoch, 5);
        assert_eq!(got.checkpoint_id, 42);
    }

    #[tokio::test]
    async fn observe_returns_none_when_leader_silent() {
        let k = kv(NodeId(1));
        let coord = BarrierCoordinator::new(k);
        assert!(coord.observe(NodeId(1)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn quorum_reached_when_all_ack_success() {
        let k = kv(NodeId(1));
        let ack_json = serde_json::to_string(&BarrierAck {
            epoch: 7,
            ok: true,
            error: None,
            local_watermark_ms: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json.clone());
        k.seed(NodeId(3), ACK_KEY, ack_json);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(7, &[NodeId(2), NodeId(3)], Duration::from_millis(200))
            .await;
        match outcome {
            QuorumOutcome::Reached {
                mut acks,
                min_follower_watermark_ms,
            } => {
                acks.sort_by_key(|n| n.0);
                assert_eq!(acks, vec![NodeId(2), NodeId(3)]);
                assert_eq!(
                    min_follower_watermark_ms, None,
                    "no follower reported a watermark — min is None"
                );
            }
            other => panic!("expected Reached, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn quorum_timeout_when_follower_silent() {
        let k = kv(NodeId(1));
        let ack_json = serde_json::to_string(&BarrierAck {
            epoch: 8,
            ok: true,
            error: None,
            local_watermark_ms: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, ack_json);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(8, &[NodeId(2), NodeId(3)], Duration::from_millis(150))
            .await;
        match outcome {
            QuorumOutcome::TimedOut { got, missing } => {
                assert_eq!(got, vec![NodeId(2)]);
                assert_eq!(missing, vec![NodeId(3)]);
            }
            other => panic!("expected TimedOut, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn quorum_fails_fast_on_reported_error() {
        let k = kv(NodeId(1));
        let good = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: true,
            error: None,
            local_watermark_ms: None,
        })
        .unwrap();
        let bad = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: false,
            error: Some("state snapshot failed: disk full".into()),
            local_watermark_ms: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, good);
        k.seed(NodeId(3), ACK_KEY, bad);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(9, &[NodeId(2), NodeId(3)], Duration::from_secs(2))
            .await;
        match outcome {
            QuorumOutcome::Failed { failures } => {
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].0, NodeId(3));
                assert!(failures[0].1.contains("disk full"));
            }
            other => panic!("expected Failed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn wrong_epoch_ack_is_ignored() {
        let k = kv(NodeId(1));
        let stale = serde_json::to_string(&BarrierAck {
            epoch: 9,
            ok: true,
            error: None,
            local_watermark_ms: None,
        })
        .unwrap();
        k.seed(NodeId(2), ACK_KEY, stale);

        let coord = BarrierCoordinator::new(k);
        let outcome = coord
            .wait_for_quorum(10, &[NodeId(2)], Duration::from_millis(100))
            .await;
        assert!(
            matches!(outcome, QuorumOutcome::TimedOut { .. }),
            "stale-epoch ack must not satisfy quorum"
        );
    }
}
