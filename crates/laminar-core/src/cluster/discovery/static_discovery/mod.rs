//! Static seed-list discovery with TCP heartbeats.

#![allow(clippy::disallowed_types)] // cold path: static discovery coordination
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::{
    publish_if_changed, Discovery, DiscoveryError, NodeId, NodeInfo, NodeMetadata, NodeState,
};

/// TCP connect timeout for heartbeat connections.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// TCP read/write timeout per I/O operation.
const IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum concurrent handler tasks in the listener.
const MAX_HANDLER_TASKS: usize = 64;

/// Maximum message size (1 MB).
const MAX_MESSAGE_SIZE: usize = 1_048_576;

/// Grace period after which `Left` peers disappear from published membership.
const LEFT_REAP_THRESHOLD: u32 = 30;

const DISCOVERY_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const STATIC_DISCOVERY_PROTOCOL_VERSION: u16 = 2;

fn current_node_metadata() -> NodeMetadata {
    NodeMetadata {
        version: env!("CARGO_PKG_VERSION").into(),
        ..NodeMetadata::default()
    }
}

struct AbortTaskOnDrop(tokio::task::AbortHandle);

impl AbortTaskOnDrop {
    fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for AbortTaskOnDrop {
    fn drop(&mut self) {
        self.abort();
    }
}

async fn join_task_bounded<T>(
    mut task: tokio::task::JoinHandle<T>,
    timeout: Duration,
    task_name: &'static str,
) -> Option<Result<T, tokio::task::JoinError>> {
    let abort_on_drop = AbortTaskOnDrop(task.abort_handle());
    if let Ok(result) = tokio::time::timeout(timeout, &mut task).await {
        Some(result)
    } else {
        tracing::warn!(
            task = task_name,
            ?timeout,
            "Discovery task did not stop in time"
        );
        abort_on_drop.abort();
        let _ = tokio::time::timeout(timeout.min(Duration::from_secs(1)), &mut task).await;
        None
    }
}

/// Configuration for static discovery.
#[derive(Debug, Clone)]
pub struct StaticDiscoveryConfig {
    /// This node's info.
    pub local_node: NodeInfo,
    /// Seed addresses to connect to.
    pub seeds: Vec<String>,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Number of missed heartbeats before marking as `Suspected`.
    pub suspect_threshold: u32,
    /// Number of missed heartbeats before marking as `Left`.
    pub dead_threshold: u32,
    /// Address to bind the heartbeat listener.
    pub listen_address: String,
    /// Durable, monotonically increasing process term for this stable node ID.
    pub process_generation: u64,
    /// Unique identity of this process incarnation.
    pub process_incarnation: uuid::Uuid,
}

impl Default for StaticDiscoveryConfig {
    fn default() -> Self {
        Self {
            local_node: NodeInfo {
                id: NodeId(1),
                name: "node-1".into(),
                rpc_address: "127.0.0.1:9000".into(),
                state: NodeState::Active,
                metadata: current_node_metadata(),
                last_heartbeat_ms: 0,
            },
            seeds: Vec::new(),
            heartbeat_interval: Duration::from_secs(1),
            suspect_threshold: 3,
            dead_threshold: 10,
            listen_address: "127.0.0.1:9002".into(),
            process_generation: 1,
            process_incarnation: uuid::Uuid::from_u128(1),
        }
    }
}

#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct StaticHeartbeat {
    protocol_version: u16,
    node: NodeInfo,
    process_generation: u64,
    process_incarnation: [u8; 16],
}

impl StaticHeartbeat {
    fn new(node: NodeInfo, process_generation: u64, process_incarnation: uuid::Uuid) -> Self {
        Self {
            protocol_version: STATIC_DISCOVERY_PROTOCOL_VERSION,
            node,
            process_generation,
            process_incarnation: process_incarnation.into_bytes(),
        }
    }

    fn identity(&self) -> ProcessIdentity {
        ProcessIdentity {
            generation: self.process_generation,
            incarnation: self.process_incarnation,
        }
    }

    fn validate(&self) -> Result<(), DiscoveryError> {
        if self.protocol_version != STATIC_DISCOVERY_PROTOCOL_VERSION {
            return Err(DiscoveryError::Serialization(format!(
                "static discovery protocol version must be {STATIC_DISCOVERY_PROTOCOL_VERSION}"
            )));
        }
        if self.process_generation == 0 {
            return Err(DiscoveryError::Serialization(
                "static discovery process generation must be nonzero".into(),
            ));
        }
        if self.process_incarnation == [0; 16] {
            return Err(DiscoveryError::Serialization(
                "static discovery process incarnation must be a non-nil UUID".into(),
            ));
        }
        if self.node.metadata.version != env!("CARGO_PKG_VERSION") {
            return Err(DiscoveryError::Serialization(format!(
                "static discovery node version must be {:?}",
                env!("CARGO_PKG_VERSION")
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProcessIdentity {
    generation: u64,
    incarnation: [u8; 16],
}

/// Internal per-peer tracking state.
#[derive(Debug)]
struct PeerState {
    info: NodeInfo,
    identity: ProcessIdentity,
    /// An equal-term/different-incarnation collision has no safe winner.
    excluded: bool,
    /// Hidden after the `Left` grace period while retaining the process-term watermark.
    reaped: bool,
    /// Missed *outbound* heartbeats (managed exclusively by the heartbeater).
    missed_heartbeats: u32,
    /// Counter that keeps incrementing after `Left` state. Used for reaping.
    left_ticks: u32,
}

#[derive(Debug, Default)]
struct StaticState {
    peers: HashMap<u64, PeerState>,
}

impl StaticState {
    fn peer_list(&self) -> Vec<NodeInfo> {
        self.peers
            .values()
            .filter(|peer| !peer.excluded && !peer.reaped)
            .map(|peer| peer.info.clone())
            .collect()
    }

    fn update_identity(info: &mut NodeInfo, remote: &NodeInfo, now: i64) {
        info.rpc_address.clone_from(&remote.rpc_address);
        info.name.clone_from(&remote.name);
        info.metadata = remote.metadata.clone();
        info.last_heartbeat_ms = now;
    }

    fn new_peer(
        heartbeat: StaticHeartbeat,
        now: i64,
        direction: ObservationDirection,
    ) -> PeerState {
        let identity = heartbeat.identity();
        let mut info = heartbeat.node;
        info.last_heartbeat_ms = now;
        if direction == ObservationDirection::Inbound {
            info.state = match info.state {
                NodeState::Draining => NodeState::Draining,
                NodeState::Left => NodeState::Left,
                _ => NodeState::Joining,
            };
        }
        PeerState {
            info,
            identity,
            excluded: false,
            reaped: false,
            missed_heartbeats: 0,
            left_ticks: 0,
        }
    }

    fn observe(
        &mut self,
        heartbeat: StaticHeartbeat,
        now: i64,
        direction: ObservationDirection,
    ) -> ObservationResult {
        let id = heartbeat.node.id.0;
        let incoming_identity = heartbeat.identity();
        let Some(peer) = self.peers.get_mut(&id) else {
            self.peers
                .insert(id, Self::new_peer(heartbeat, now, direction));
            return ObservationResult::Accepted(incoming_identity);
        };

        match incoming_identity.generation.cmp(&peer.identity.generation) {
            std::cmp::Ordering::Less => return ObservationResult::Ignored,
            std::cmp::Ordering::Greater => {
                *peer = Self::new_peer(heartbeat, now, direction);
                return ObservationResult::Accepted(incoming_identity);
            }
            std::cmp::Ordering::Equal => {}
        }

        if incoming_identity.incarnation != peer.identity.incarnation {
            peer.excluded = true;
            return ObservationResult::Collision;
        }
        if peer.excluded {
            return ObservationResult::Collision;
        }

        let remote = heartbeat.node;
        Self::update_identity(&mut peer.info, &remote, now);
        peer.info.state = match (peer.info.state, remote.state) {
            (NodeState::Left, _) | (_, NodeState::Left) => NodeState::Left,
            (NodeState::Draining, _) | (_, NodeState::Draining) => NodeState::Draining,
            (state, _) if direction == ObservationDirection::Inbound => state,
            (_, advertised) => advertised,
        };
        if direction == ObservationDirection::Outbound {
            peer.missed_heartbeats = 0;
            if peer.info.state != NodeState::Left {
                peer.left_ticks = 0;
            }
        }
        ObservationResult::Accepted(incoming_identity)
    }

    fn observe_inbound(&mut self, heartbeat: StaticHeartbeat, now: i64) -> ObservationResult {
        self.observe(heartbeat, now, ObservationDirection::Inbound)
    }

    fn observe_outbound(&mut self, heartbeat: StaticHeartbeat, now: i64) -> ObservationResult {
        self.observe(heartbeat, now, ObservationDirection::Outbound)
    }

    fn record_missed_heartbeat(
        &mut self,
        expected: SeedPeer,
        suspect_threshold: u32,
        dead_threshold: u32,
    ) {
        let Some(peer) = self.peers.get_mut(&expected.node_id) else {
            return;
        };
        if peer.identity != expected.identity || peer.excluded || peer.reaped {
            return;
        }
        peer.missed_heartbeats = peer.missed_heartbeats.saturating_add(1);
        if peer.missed_heartbeats >= dead_threshold {
            peer.info.state = NodeState::Left;
        } else if peer.missed_heartbeats >= suspect_threshold {
            peer.info.state = NodeState::Suspected;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservationResult {
    Accepted(ProcessIdentity),
    Ignored,
    Collision,
}

/// Static discovery implementation with TCP heartbeats.
#[derive(Debug)]
pub struct StaticDiscovery {
    config: StaticDiscoveryConfig,
    local_info: Arc<RwLock<NodeInfo>>,
    state: Arc<RwLock<StaticState>>,
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    cancel: CancellationToken,
    listener_handle: Option<tokio::task::JoinHandle<Result<(), DiscoveryError>>>,
    heartbeater_handle: Option<tokio::task::JoinHandle<()>>,
    started: bool,
}

impl StaticDiscovery {
    /// Create a new static discovery instance.
    #[must_use]
    pub fn new(config: StaticDiscoveryConfig) -> Self {
        debug_assert!(
            config.suspect_threshold < config.dead_threshold,
            "suspect_threshold ({}) must be less than dead_threshold ({})",
            config.suspect_threshold,
            config.dead_threshold,
        );
        let (tx, rx) = watch::channel(Vec::new());
        Self {
            local_info: Arc::new(RwLock::new(config.local_node.clone())),
            config,
            state: Arc::new(RwLock::new(StaticState::default())),
            membership_tx: tx,
            membership_rx: rx,
            cancel: CancellationToken::new(),
            listener_handle: None,
            heartbeater_handle: None,
            started: false,
        }
    }

    /// Serialize a heartbeat for transmission.
    fn serialize_heartbeat(heartbeat: &StaticHeartbeat) -> Result<Vec<u8>, DiscoveryError> {
        heartbeat.validate()?;
        rkyv::to_bytes::<rkyv::rancor::Error>(heartbeat)
            .map(|v| v.to_vec())
            .map_err(|e| DiscoveryError::Serialization(e.to_string()))
    }

    /// Deserialize and validate a heartbeat received from the network.
    fn deserialize_heartbeat(data: &[u8]) -> Result<StaticHeartbeat, DiscoveryError> {
        let heartbeat = rkyv::from_bytes::<StaticHeartbeat, rkyv::rancor::Error>(data)
            .map_err(|e| DiscoveryError::Serialization(e.to_string()))?;
        heartbeat.validate()?;
        Ok(heartbeat)
    }

    /// Send a heartbeat to a single seed address with connect + I/O timeouts.
    #[allow(clippy::cast_possible_truncation)]
    async fn send_heartbeat(address: &str, data: &[u8]) -> Result<Option<Vec<u8>>, DiscoveryError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Connect with timeout (C2 fix)
        let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
            .await
            .map_err(|_| DiscoveryError::Connection {
                address: address.into(),
                reason: "connect timeout".into(),
            })?
            .map_err(|e| DiscoveryError::Connection {
                address: address.into(),
                reason: e.to_string(),
            })?;

        // Validate message size before sending (W7 symmetric limit)
        if data.len() > MAX_MESSAGE_SIZE {
            return Err(DiscoveryError::Serialization(
                "message too large to send".into(),
            ));
        }

        // Write length-prefixed message with I/O timeout (W1 fix)
        let len = data.len() as u32;
        tokio::time::timeout(IO_TIMEOUT, async {
            stream.write_all(&len.to_be_bytes()).await?;
            stream.write_all(data).await
        })
        .await
        .map_err(|_| DiscoveryError::Connection {
            address: address.into(),
            reason: "write timeout".into(),
        })?
        .map_err(|e| DiscoveryError::Connection {
            address: address.into(),
            reason: e.to_string(),
        })?;

        // Read response with I/O timeout (W1 fix)
        let resp = tokio::time::timeout(IO_TIMEOUT, async {
            let mut len_buf = [0u8; 4];
            if stream.read_exact(&mut len_buf).await.is_err() {
                return Ok(None);
            }

            let resp_len = u32::from_be_bytes(len_buf) as usize;
            if resp_len > MAX_MESSAGE_SIZE {
                return Err(DiscoveryError::Serialization("response too large".into()));
            }
            let mut resp = vec![0u8; resp_len];
            stream.read_exact(&mut resp).await?;
            Ok(Some(resp))
        })
        .await
        .map_err(|_| DiscoveryError::Connection {
            address: address.into(),
            reason: "read timeout".into(),
        })?;

        resp.map_err(|e: DiscoveryError| e)
    }

    /// Run the heartbeat listener (accepts incoming heartbeats).
    ///
    /// Inbound traffic refreshes peer identity and propagates advertised
    /// draining/left states, but does not reset outbound failure counters.
    #[allow(clippy::cast_possible_truncation)]
    async fn run_listener(
        listener: TcpListener,
        local_info: Arc<RwLock<NodeInfo>>,
        local_identity: ProcessIdentity,
        state: Arc<RwLock<StaticState>>,
        membership_tx: watch::Sender<Vec<NodeInfo>>,
        cancel: CancellationToken,
    ) -> Result<(), DiscoveryError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Bound concurrent handler tasks (W3 fix)
        let semaphore = Arc::new(tokio::sync::Semaphore::new(MAX_HANDLER_TASKS));
        let mut handlers = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                () = cancel.cancelled() => {
                    handlers.shutdown().await;
                    break;
                },
                result = handlers.join_next(), if !handlers.is_empty() => {
                    if let Some(Err(error)) = result {
                        tracing::debug!(%error, "Static discovery heartbeat handler stopped");
                    }
                },
                accept = listener.accept() => {
                    let (mut stream, _) = match accept {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            handlers.shutdown().await;
                            return Err(error.into());
                        }
                    };
                    let local_info = Arc::clone(&local_info);
                    let state = Arc::clone(&state);
                    let membership_tx = membership_tx.clone();
                    let permit = Arc::clone(&semaphore);

                    handlers.spawn(async move {
                        // Acquire semaphore permit — drop-guard releases on exit
                        let Ok(_permit) = permit.try_acquire() else {
                            return; // at capacity, drop connection
                        };

                        // Wrap all handler I/O in a timeout (W1 fix)
                        let result = tokio::time::timeout(IO_TIMEOUT, async {
                            let mut len_buf = [0u8; 4];
                            if stream.read_exact(&mut len_buf).await.is_err() {
                                return;
                            }
                            let msg_len = u32::from_be_bytes(len_buf) as usize;
                            if msg_len > MAX_MESSAGE_SIZE {
                                return;
                            }
                            let mut data = vec![0u8; msg_len];
                            if stream.read_exact(&mut data).await.is_err() {
                                return;
                            }

                            if let Ok(remote_heartbeat) = Self::deserialize_heartbeat(&data) {
                                let local_snapshot = local_info.read().clone();
                                // Skip self — don't add ourselves to the peer list
                                if remote_heartbeat.node.id == local_snapshot.id {
                                    // Still respond so the heartbeater gets a reply
                                    let heartbeat = StaticHeartbeat {
                                        protocol_version: STATIC_DISCOVERY_PROTOCOL_VERSION,
                                        node: local_snapshot,
                                        process_generation: local_identity.generation,
                                        process_incarnation: local_identity.incarnation,
                                    };
                                    if let Ok(resp) = Self::serialize_heartbeat(&heartbeat) {
                                        let len = resp.len() as u32;
                                        let _ = stream.write_all(&len.to_be_bytes()).await;
                                        let _ = stream.write_all(&resp).await;
                                    }
                                    return;
                                }

                                let peer_list = {
                                    let now = chrono::Utc::now().timestamp_millis();
                                    let mut guard = state.write();
                                    guard.observe_inbound(remote_heartbeat, now);
                                    guard.peer_list()
                                };
                                publish_if_changed(&membership_tx, peer_list);
                            }

                            let local_snapshot = local_info.read().clone();
                            let heartbeat = StaticHeartbeat {
                                protocol_version: STATIC_DISCOVERY_PROTOCOL_VERSION,
                                node: local_snapshot,
                                process_generation: local_identity.generation,
                                process_incarnation: local_identity.incarnation,
                            };
                            if let Ok(resp) = Self::serialize_heartbeat(&heartbeat) {
                                let len = resp.len() as u32;
                                let _ = stream.write_all(&len.to_be_bytes()).await;
                                let _ = stream.write_all(&resp).await;
                            }
                        })
                        .await;

                        if result.is_err() {
                            // Handler timed out — connection dropped
                        }
                    });
                }
            }
        }

        Ok(())
    }

    /// Run the periodic heartbeat sender.
    ///
    /// Sends heartbeats concurrently to all seeds and uses the responses
    /// to track failure state.
    async fn run_heartbeater(config: StaticDiscoveryConfig, ctx: HeartbeatContext) {
        let mut interval = tokio::time::interval(config.heartbeat_interval);
        // Don't burst missed ticks — skip them to avoid thundering herd (W5 fix)
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Track the exact process identity reached through each seed so a stale
        // address cannot mark a newer replacement as failed.
        // Protected by a Mutex since concurrent heartbeat tasks need it.
        let seed_to_peer = Arc::new(parking_lot::Mutex::new(HashMap::<String, SeedPeer>::new()));

        loop {
            tokio::select! {
                () = ctx.cancel.cancelled() => break,
                _ = interval.tick() => {
                    let local_info = ctx.local_info.read().clone();
                    let local_id = local_info.id;
                    let heartbeat = StaticHeartbeat::new(
                        local_info,
                        config.process_generation,
                        config.process_incarnation,
                    );
                    let Ok(data) = Self::serialize_heartbeat(&heartbeat) else {
                        continue;
                    };
                    let data = Arc::new(data);

                    // Send heartbeats to all seeds concurrently (W5 fix)
                    let mut tasks = tokio::task::JoinSet::new();
                    for seed in &config.seeds {
                        let seed = seed.clone();
                        let data = Arc::clone(&data);
                        tasks.spawn(async move {
                            let result = Self::send_heartbeat(&seed, &data).await;
                            (seed, result)
                        });
                    }

                    // Collect results and update peer state
                    while !tasks.is_empty() {
                        let task = tokio::select! {
                            biased;
                            () = ctx.cancel.cancelled() => {
                                tasks.shutdown().await;
                                return;
                            }
                            task = tasks.join_next() => task,
                        };
                        let Some(Ok((seed, result))) = task else {
                            continue; // task panicked
                        };

                        if let Ok(Some(resp_data)) = result {
                            if let Ok(remote_heartbeat) = Self::deserialize_heartbeat(&resp_data) {
                                // Skip self
                                if remote_heartbeat.node.id == local_id {
                                    continue;
                                }

                                let now = chrono::Utc::now().timestamp_millis();
                                let remote_id = remote_heartbeat.node.id.0;
                                let observation =
                                    ctx.state.write().observe_outbound(remote_heartbeat, now);
                                match observation {
                                    ObservationResult::Accepted(identity) => {
                                        seed_to_peer.lock().insert(
                                            seed,
                                            SeedPeer {
                                                node_id: remote_id,
                                                identity,
                                            },
                                        );
                                    }
                                    ObservationResult::Ignored => {
                                        let expected = {
                                            seed_to_peer.lock().get(&seed).copied()
                                        };
                                        if let Some(expected) = expected {
                                            ctx.state.write().record_missed_heartbeat(
                                                expected,
                                                config.suspect_threshold,
                                                config.dead_threshold,
                                            );
                                        }
                                    }
                                    ObservationResult::Collision => {
                                        seed_to_peer.lock().remove(&seed);
                                    }
                                }
                            }
                        } else {
                            // Heartbeat failed — increment missed counter
                            let map = seed_to_peer.lock();
                            if let Some(seed_peer) = map.get(seed.as_str()).copied() {
                                drop(map);
                                ctx.state.write().record_missed_heartbeat(
                                    seed_peer,
                                    config.suspect_threshold,
                                    config.dead_threshold,
                                );
                            }
                        }
                    }

                    // Hide peers stuck in Left state while retaining their process-term
                    // watermark so a stale incarnation cannot reappear as current.
                    {
                        let mut state = ctx.state.write();
                        for peer in state.peers.values_mut() {
                            if peer.info.state == NodeState::Left && !peer.excluded {
                                peer.left_ticks = peer.left_ticks.saturating_add(1);
                                if peer.left_ticks >= LEFT_REAP_THRESHOLD {
                                    peer.reaped = true;
                                }
                            }
                        }
                    }

                    // Also clean up seed_to_peer for reaped peers (W2 fix)
                    {
                        let state = ctx.state.read();
                        let mut map = seed_to_peer.lock();
                        map.retain(|_, seed_peer| {
                            state.peers.get(&seed_peer.node_id).is_some_and(|peer| {
                                peer.identity == seed_peer.identity
                                    && !peer.excluded
                                    && !peer.reaped
                            })
                        });
                    }

                    let peer_list = ctx.state.read().peer_list();
                    publish_if_changed(&ctx.membership_tx, peer_list);
                }
            }
        }
    }

    fn start_with_bound_listener(&mut self, listener: TcpListener) -> Result<(), DiscoveryError> {
        let local_heartbeat = StaticHeartbeat::new(
            self.local_info.read().clone(),
            self.config.process_generation,
            self.config.process_incarnation,
        );
        local_heartbeat.validate()?;
        let local_identity = local_heartbeat.identity();

        // Create a fresh cancellation token so restart after stop() works (W4 fix)
        self.cancel = CancellationToken::new();

        let local_info = Arc::clone(&self.local_info);
        let state = Arc::clone(&self.state);
        let membership_tx = self.membership_tx.clone();
        let cancel = self.cancel.clone();

        // Binding is complete before either task starts, so startup cannot succeed with a
        // listener task that is already destined to fail on an address conflict.
        self.listener_handle = Some(tokio::spawn(Self::run_listener(
            listener,
            Arc::clone(&local_info),
            local_identity,
            Arc::clone(&state),
            membership_tx.clone(),
            cancel.clone(),
        )));
        self.heartbeater_handle = Some(tokio::spawn(Self::run_heartbeater(
            self.config.clone(),
            HeartbeatContext {
                local_info,
                state,
                membership_tx,
                cancel,
            },
        )));

        self.started = true;
        Ok(())
    }

    async fn stop_with_timeout(&mut self, timeout: Duration) {
        self.cancel.cancel();
        self.started = false;

        let listener_handle = self.listener_handle.take();
        let heartbeater_handle = self.heartbeater_handle.take();
        let stop_listener = async move {
            if let Some(handle) = listener_handle {
                match join_task_bounded(handle, timeout, "static-listener").await {
                    Some(Ok(Ok(()))) | None => {}
                    Some(Ok(Err(error))) => {
                        tracing::warn!(%error, "Static discovery listener stopped with an error");
                    }
                    Some(Err(error)) => {
                        tracing::debug!(%error, "Static discovery listener task stopped unexpectedly");
                    }
                }
            }
        };
        let stop_heartbeater = async move {
            if let Some(handle) = heartbeater_handle {
                if let Some(Err(error)) =
                    join_task_bounded(handle, timeout, "static-heartbeater").await
                {
                    tracing::debug!(%error, "Static discovery heartbeater task stopped unexpectedly");
                }
            }
        };
        tokio::join!(stop_listener, stop_heartbeater);
    }
}

#[derive(Debug, Clone, Copy)]
struct SeedPeer {
    node_id: u64,
    identity: ProcessIdentity,
}

/// Shared context for the heartbeater background task.
struct HeartbeatContext {
    local_info: Arc<RwLock<NodeInfo>>,
    state: Arc<RwLock<StaticState>>,
    membership_tx: watch::Sender<Vec<NodeInfo>>,
    cancel: CancellationToken,
}

impl Discovery for StaticDiscovery {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        if self.started {
            return Ok(());
        }
        let listener = TcpListener::bind(&self.config.listen_address)
            .await
            .map_err(|error| DiscoveryError::Bind(error.to_string()))?;
        self.start_with_bound_listener(listener)
    }

    // INVARIANT: discovery operations stay poll-driven for timeout and select cancellation.
    #[allow(clippy::unused_async_trait_impl)]
    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        Ok(self.state.read().peer_list())
    }

    // INVARIANT: announcement mutation starts only when the returned future is polled.
    #[allow(clippy::unused_async_trait_impl)]
    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        if !self.started {
            return Err(DiscoveryError::NotStarted);
        }
        StaticHeartbeat::new(
            info.clone(),
            self.config.process_generation,
            self.config.process_incarnation,
        )
        .validate()?;
        let mut local = self.local_info.write();
        let current_state = local.state;
        *local = info;
        local.state = match (current_state, local.state) {
            (NodeState::Left, _) | (_, NodeState::Left) => NodeState::Left,
            (NodeState::Draining, _) | (_, NodeState::Draining) => NodeState::Draining,
            (_, next) => next,
        };
        Ok(())
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        self.membership_rx.clone()
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        self.stop_with_timeout(DISCOVERY_SHUTDOWN_TIMEOUT).await;
        Ok(())
    }
}

impl Drop for StaticDiscovery {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.started = false;
        if let Some(handle) = self.listener_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.heartbeater_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests;
