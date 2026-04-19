//! MiniCluster — in-process harness for cluster-control integration
//! tests.
//!
//! Spawns N chitchat instances on loopback UDP, each wrapped in a
//! [`GossipDiscovery`](super::discovery::GossipDiscovery) +
//! [`ClusterController`](super::control::ClusterController) pair.
//! Shared by the integration test matrix in
//! `tests/cluster_integration.rs`; designed to be reusable from
//! downstream crates (laminar-db, laminar-server) as dev-dependency.
//!
//! ## Scope
//!
//! - Real chitchat gossip; `gossip_interval` tuned down to 50 ms to
//!   keep convergence under a second.
//! - Loopback UDP only; each node binds to `127.0.0.1:0` (ephemeral)
//!   and records the assigned port before handing off to chitchat.
//! - No LaminarDB engine — just the cluster primitives. Full engine
//!   harness is a follow-up when the checkpoint flow consumes the
//!   controller.
//!
//! ## Not production API
//!
//! Gated on `cluster-unstable`. The APIs here are test helpers;
//! expect churn.

use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chitchat::transport::{Socket, Transport, UdpTransport};
use object_store::{
    path::Path as OsPath, CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parking_lot::Mutex;
use rustc_hash::FxHashSet;
use tokio::sync::watch;

use super::control::{AssignmentSnapshotStore, ChitchatKv, ClusterController, ClusterKv};
use super::discovery::{
    Discovery, GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo, NodeMetadata, NodeState,
};

/// Shared per-cluster partition rules. Each (src, dst) pair in the
/// set causes sends from `src` to `dst` to be silently dropped. The
/// rules are bidirectional when set via [`Self::partition`]; one-way
/// partitions can be constructed by adding a single pair via
/// [`Self::drop_pair`].
pub struct NetworkRules {
    dropped: Mutex<FxHashSet<(SocketAddr, SocketAddr)>>,
}

impl std::fmt::Debug for NetworkRules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkRules")
            .field("drop_count", &self.dropped.lock().len())
            .finish()
    }
}

impl NetworkRules {
    /// Fresh rules with no partitions active.
    #[must_use]
    pub fn new() -> Self {
        Self {
            dropped: Mutex::new(FxHashSet::default()),
        }
    }

    /// Partition the cluster so no traffic flows between `side_a` and
    /// `side_b` in either direction. Within each side, traffic is
    /// unaffected.
    pub fn partition(&self, side_a: &[SocketAddr], side_b: &[SocketAddr]) {
        let mut set = self.dropped.lock();
        for a in side_a {
            for b in side_b {
                set.insert((*a, *b));
                set.insert((*b, *a));
            }
        }
    }

    /// Add a one-way drop rule for a specific (src, dst) pair.
    pub fn drop_pair(&self, src: SocketAddr, dst: SocketAddr) {
        self.dropped.lock().insert((src, dst));
    }

    /// Clear every active partition; full connectivity restored.
    pub fn heal(&self) {
        self.dropped.lock().clear();
    }

    /// True if the (src, dst) pair is currently partitioned.
    #[must_use]
    pub fn is_dropped(&self, src: SocketAddr, dst: SocketAddr) -> bool {
        self.dropped.lock().contains(&(src, dst))
    }
}

impl Default for NetworkRules {
    fn default() -> Self {
        Self::new()
    }
}

/// Fault mode for [`FaultyObjectStore`], flippable at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreFault {
    /// Pass through to the underlying store.
    None,
    /// Every write returns `Error::Generic`.
    FailWrites,
    /// Every read returns `Error::NotFound`.
    FailReads,
    /// Both reads and writes fail.
    FailAll,
}

impl ObjectStoreFault {
    fn fails_writes(self) -> bool {
        matches!(self, Self::FailWrites | Self::FailAll)
    }
    fn fails_reads(self) -> bool {
        matches!(self, Self::FailReads | Self::FailAll)
    }
}

/// Object store middleware with runtime-flippable fault injection.
pub struct FaultyObjectStore {
    inner: Arc<dyn ObjectStore>,
    fault: Mutex<ObjectStoreFault>,
}

impl std::fmt::Debug for FaultyObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultyObjectStore")
            .field("fault", &self.fault())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for FaultyObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultyObjectStore({:?})", self.fault())
    }
}

impl FaultyObjectStore {
    /// Wrap `inner` with fault injection disabled.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            fault: Mutex::new(ObjectStoreFault::None),
        }
    }

    /// Current fault mode.
    #[must_use]
    pub fn fault(&self) -> ObjectStoreFault {
        *self.fault.lock()
    }

    /// Switch fault mode. Takes effect on the next operation.
    pub fn set_fault(&self, mode: ObjectStoreFault) {
        *self.fault.lock() = mode;
    }

    fn check_write(&self) -> object_store::Result<()> {
        if self.fault().fails_writes() {
            return Err(object_store::Error::Generic {
                store: "FaultyObjectStore",
                source: "injected write failure".into(),
            });
        }
        Ok(())
    }

    fn check_read(&self, path: &OsPath) -> object_store::Result<()> {
        if self.fault().fails_reads() {
            return Err(object_store::Error::NotFound {
                path: path.to_string(),
                source: "injected read failure".into(),
            });
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for FaultyObjectStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.check_write()?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check_write()?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.check_read(location)?;
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
        // Under FailWrites, replace every incoming location with an
        // injected error; otherwise delegate.
        if self.fault().fails_writes() {
            use futures::StreamExt;
            locations
                .map(|_| {
                    Err(object_store::Error::Generic {
                        store: "FaultyObjectStore",
                        source: "injected write failure (delete_stream)".into(),
                    })
                })
                .boxed()
        } else {
            self.inner.delete_stream(locations)
        }
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.check_write()?;
        self.inner.copy_opts(from, to, options).await
    }
}

/// Chitchat transport that delegates to a real [`UdpTransport`] but
/// consults a shared [`NetworkRules`] before each send. Packets
/// destined for a partitioned peer are silently dropped.
pub struct PartitionableTransport {
    rules: Arc<NetworkRules>,
    inner: UdpTransport,
}

impl PartitionableTransport {
    /// Wrap the default [`UdpTransport`] with the given rule set.
    #[must_use]
    pub fn new(rules: Arc<NetworkRules>) -> Self {
        Self {
            rules,
            inner: UdpTransport,
        }
    }
}

#[async_trait]
impl Transport for PartitionableTransport {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let socket = self.inner.open(listen_addr).await?;
        Ok(Box::new(PartitionableSocket {
            my_addr: listen_addr,
            rules: Arc::clone(&self.rules),
            inner: socket,
        }))
    }
}

struct PartitionableSocket {
    my_addr: SocketAddr,
    rules: Arc<NetworkRules>,
    inner: Box<dyn Socket>,
}

#[async_trait]
impl Socket for PartitionableSocket {
    async fn send(
        &mut self,
        to: SocketAddr,
        msg: chitchat::ChitchatMessage,
    ) -> anyhow::Result<()> {
        if self.rules.is_dropped(self.my_addr, to) {
            // Silently drop — simulates a network partition. Chitchat's
            // phi-accrual observes the absence, eventually marks the
            // peer suspected.
            return Ok(());
        }
        self.inner.send(to, msg).await
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, chitchat::ChitchatMessage)> {
        self.inner.recv().await
    }
}

/// Returns a free loopback UDP port. Binds and drops a socket to
/// discover an ephemeral port; race with concurrent binders is
/// accepted for test use.
fn grab_port() -> u16 {
    let sock = UdpSocket::bind("127.0.0.1:0").expect("bind 127.0.0.1:0");
    let port = sock.local_addr().expect("local_addr").port();
    drop(sock);
    port
}

/// One instance of the mini-cluster: a gossip discovery + controller.
pub struct NodeHandle {
    /// This node's identity.
    pub instance_id: NodeId,
    /// Gossip address this node listens on.
    pub gossip_addr: String,
    /// The cluster control facade. `Arc`-shared so tests can observe
    /// leader status while the harness retains ownership.
    pub controller: Arc<ClusterController>,
    /// Underlying discovery. Kept to drive shutdown.
    discovery: GossipDiscovery,
}

impl NodeHandle {
    /// Gracefully stop this node. Before shutting down the gossip
    /// socket, publishes `state = Left` via chitchat so peers see the
    /// status change via the next gossip round (~100 ms), rather than
    /// waiting for phi-accrual to flag the missing heartbeats.
    ///
    /// This matches the contract other production gossip stacks
    /// (Serf, Akka cluster) provide — clean leave is a protocol
    /// primitive, not a timing hack. Tests that want to simulate a
    /// crash (no clean leave) should use [`Self::crash`] instead.
    pub async fn kill(mut self) {
        let left = NodeInfo {
            state: NodeState::Left,
            ..current_info(&self)
        };
        let _ = self.discovery.announce(left).await;
        // Brief pause so the Left announcement propagates via gossip
        // before the socket closes.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = self.discovery.stop().await;
    }

    /// Simulate a crash: drop the node without announcing. Peers rely
    /// on phi-accrual to eventually detect the failure, which in the
    /// default chitchat config can take tens of seconds. Use
    /// [`Self::kill`] for test scenarios that expect prompt failover.
    pub fn crash(self) {
        drop(self);
    }
}

fn current_info(node: &NodeHandle) -> NodeInfo {
    NodeInfo {
        id: node.instance_id,
        name: format!("minicluster-n{}", node.instance_id.0),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata {
            cores: 1,
            ..NodeMetadata::default()
        },
        last_heartbeat_ms: 0,
    }
}

/// Builder + wrapper for a set of in-process cluster nodes.
pub struct MiniCluster {
    /// The nodes in ID order: `nodes[0]` has the lowest
    /// `instance_id` and is the leader under normal operation.
    pub nodes: Vec<NodeHandle>,
    /// Shared network rules. `None` for plain [`Self::spawn`]
    /// clusters (default UDP transport); `Some` when built via
    /// [`Self::spawn_partitionable`].
    pub rules: Option<Arc<NetworkRules>>,
    /// Shared assignment snapshot store handed to every node's
    /// controller (so a snapshot written by one node is visible to
    /// all). `None` unless built via [`Self::spawn_with_snapshot`].
    pub snapshot: Option<Arc<AssignmentSnapshotStore>>,
}

impl MiniCluster {
    /// Spin up `n` nodes with the default UDP transport. See
    /// [`Self::spawn_partitionable`] for a variant that allows
    /// simulating network partitions.
    ///
    /// # Panics
    /// Panics if any chitchat instance fails to start (UDP bind
    /// failure, usually indicates port contention).
    pub async fn spawn(n: usize) -> Self {
        Self::spawn_inner(n, None, None).await
    }

    /// Spin up `n` nodes with a [`PartitionableTransport`] wrapping
    /// the default UDP transport. The returned cluster carries
    /// [`Self::rules`] so tests can call
    /// [`NetworkRules::partition`] / [`NetworkRules::heal`] during
    /// the test.
    ///
    /// # Panics
    /// Same as [`Self::spawn`].
    pub async fn spawn_partitionable(n: usize) -> Self {
        let rules = Arc::new(NetworkRules::new());
        Self::spawn_inner(n, Some(rules), None).await
    }

    /// Spin up `n` nodes sharing the given
    /// [`AssignmentSnapshotStore`]. Each node's controller is
    /// constructed with the same store, so a snapshot written by
    /// any node is visible to every other node and survives across
    /// a full cluster restart (provided the object store does).
    pub async fn spawn_with_snapshot(
        n: usize,
        snapshot: Arc<AssignmentSnapshotStore>,
    ) -> Self {
        Self::spawn_inner(n, None, Some(snapshot)).await
    }

    /// Join one new node with the given `instance_id` into an
    /// already-running cluster. Seeds from `nodes[0]` for discovery.
    /// Useful for testing rejoin-after-kill and elastic scale-up
    /// scenarios.
    ///
    /// # Panics
    /// Panics if the cluster is empty (nothing to seed from) or if
    /// chitchat fails to bind a loopback port.
    pub async fn join_node(&mut self, instance_id: NodeId) {
        assert!(!self.nodes.is_empty(), "cannot join empty cluster");
        // Seed from every currently-present node so the rejoiner has
        // multiple contact points regardless of which one answers
        // first.
        let seeds: Vec<String> = self
            .nodes
            .iter()
            .map(|n| n.gossip_addr.clone())
            .collect();
        let port = grab_port();
        let gossip_addr = format!("127.0.0.1:{port}");

        let local_node = NodeInfo {
            id: instance_id,
            name: format!("minicluster-rejoin-{}", instance_id.0),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Active,
            metadata: NodeMetadata {
                cores: 1,
                ..NodeMetadata::default()
            },
            last_heartbeat_ms: 0,
        };

        let cfg = GossipDiscoveryConfig {
            gossip_address: gossip_addr.clone(),
            seed_nodes: seeds,
            gossip_interval: Duration::from_millis(50),
            phi_threshold: 3.0,
            dead_node_grace_period: Duration::from_secs(1),
            cluster_id: "minicluster".to_string(),
            node_id: instance_id,
            local_node,
        };
        let mut discovery = GossipDiscovery::new(cfg);
        match &self.rules {
            Some(rules) => {
                let transport = PartitionableTransport::new(Arc::clone(rules));
                discovery
                    .start_with_transport(&transport)
                    .await
                    .expect("partitionable chitchat start on rejoin");
            }
            None => discovery
                .start()
                .await
                .expect("chitchat start on rejoin"),
        }

        let handle = discovery
            .chitchat_handle()
            .expect("chitchat handle available after start");
        let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
        let members_rx = discovery.membership_watch();
        let controller = Arc::new(ClusterController::new(
            instance_id,
            kv,
            self.snapshot.clone(),
            members_rx,
        ));

        self.nodes.push(NodeHandle {
            instance_id,
            gossip_addr,
            controller,
            discovery,
        });
    }

    async fn spawn_inner(
        n: usize,
        rules: Option<Arc<NetworkRules>>,
        snapshot: Option<Arc<AssignmentSnapshotStore>>,
    ) -> Self {
        assert!(n >= 1, "MiniCluster needs at least one node");

        let ports: Vec<u16> = (0..n).map(|_| grab_port()).collect();
        let seed = format!("127.0.0.1:{}", ports[0]);
        let transport = rules
            .as_ref()
            .map(|r| PartitionableTransport::new(Arc::clone(r)));

        let mut nodes = Vec::with_capacity(n);
        for (idx, port) in ports.iter().enumerate() {
            let instance_id = NodeId((idx as u64) + 1); // skip UNASSIGNED=0
            let gossip_addr = format!("127.0.0.1:{port}");

            let local_node = NodeInfo {
                id: instance_id,
                name: format!("minicluster-n{idx}"),
                rpc_address: String::new(),
                raft_address: String::new(),
                state: NodeState::Active,
                metadata: NodeMetadata {
                    cores: 1,
                    ..NodeMetadata::default()
                },
                last_heartbeat_ms: 0,
            };

            let seeds = if idx == 0 { Vec::new() } else { vec![seed.clone()] };
            // Aggressive timings: tests trade false-positive risk for
            // fast failover feedback. Production configs use the
            // chitchat defaults (phi=8.0, grace≈3s).
            let cfg = GossipDiscoveryConfig {
                gossip_address: gossip_addr.clone(),
                seed_nodes: seeds,
                gossip_interval: Duration::from_millis(50),
                phi_threshold: 3.0,
                dead_node_grace_period: Duration::from_secs(1),
                cluster_id: "minicluster".to_string(),
                node_id: instance_id,
                local_node,
            };
            let mut discovery = GossipDiscovery::new(cfg);
            match &transport {
                Some(t) => discovery
                    .start_with_transport(t)
                    .await
                    .expect("partitionable chitchat start"),
                None => discovery.start().await.expect("chitchat start on loopback"),
            }

            let handle = discovery
                .chitchat_handle()
                .expect("chitchat handle available after start");
            let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
            let members_rx: watch::Receiver<Vec<NodeInfo>> = discovery.membership_watch();
            let controller = Arc::new(ClusterController::new(
                instance_id,
                kv,
                snapshot.clone(),
                members_rx,
            ));

            nodes.push(NodeHandle {
                instance_id,
                gossip_addr,
                controller,
                discovery,
            });
        }
        Self { nodes, rules, snapshot }
    }

    /// Parse and collect node gossip addresses — useful for building
    /// partition rules via [`NetworkRules::partition`].
    ///
    /// # Panics
    /// Panics if any node's `gossip_addr` doesn't parse as a
    /// `SocketAddr` (only possible if the `MiniCluster` was constructed
    /// with malformed input).
    #[must_use]
    pub fn addrs(&self) -> Vec<SocketAddr> {
        self.nodes
            .iter()
            .map(|n| n.gossip_addr.parse().expect("valid gossip_addr"))
            .collect()
    }

    /// Wait until every node sees every other node as a peer, or
    /// until `deadline` passes.
    ///
    /// # Errors
    /// Returns `Err(String)` on timeout, describing which nodes
    /// haven't converged yet, or if a discovery `peers()` call fails.
    pub async fn wait_for_convergence(&self, deadline: Duration) -> Result<(), String> {
        let start = Instant::now();
        loop {
            let mut all_converged = true;
            let mut missing_summary = Vec::new();
            for node in &self.nodes {
                let peers = node
                    .discovery
                    .peers()
                    .await
                    .map_err(|e| format!("peers() failed on {}: {e}", node.instance_id.0))?;
                let expected = self.nodes.len() - 1;
                if peers.len() < expected {
                    all_converged = false;
                    missing_summary.push(format!(
                        "node {} sees {} peers (expected {})",
                        node.instance_id.0,
                        peers.len(),
                        expected
                    ));
                }
            }
            if all_converged {
                return Ok(());
            }
            if start.elapsed() >= deadline {
                return Err(format!(
                    "convergence timeout after {:?}: {}",
                    deadline,
                    missing_summary.join("; "),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Shut every node down cleanly.
    pub async fn shutdown(mut self) {
        for node in self.nodes.drain(..) {
            node.kill().await;
        }
    }
}
