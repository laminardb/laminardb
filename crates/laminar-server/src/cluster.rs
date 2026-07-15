//! Cluster (multi-node) mode startup orchestrator.

use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStoreExt;
use tokio::signal;
use tokio::sync::watch;
use tracing::{info, warn};

use laminar_core::cluster::discovery::{
    Discovery, DiscoveryError, GossipDiscovery, GossipDiscoveryConfig, NodeId, NodeInfo,
    NodeMetadata, NodeState, StaticDiscovery, StaticDiscoveryConfig,
};

const PROCESS_INCARNATION_TAG: &str = "laminardb.process-incarnation";
/// Enum dispatch — `Discovery` trait uses `async fn` (not dyn-compatible).
enum DiscoveryImpl {
    Static(StaticDiscovery),
    Gossip(GossipDiscovery),
}

impl DiscoveryImpl {
    async fn start(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.start().await,
            Self::Gossip(d) => d.start().await,
        }
    }

    async fn peers(&self) -> Result<Vec<NodeInfo>, DiscoveryError> {
        match self {
            Self::Static(d) => d.peers().await,
            Self::Gossip(d) => d.peers().await,
        }
    }

    async fn announce(&self, info: NodeInfo) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.announce(info).await,
            Self::Gossip(d) => d.announce(info).await,
        }
    }

    fn membership_watch(&self) -> watch::Receiver<Vec<NodeInfo>> {
        match self {
            Self::Static(d) => d.membership_watch(),
            Self::Gossip(d) => d.membership_watch(),
        }
    }

    async fn stop(&mut self) -> Result<(), DiscoveryError> {
        match self {
            Self::Static(d) => d.stop().await,
            Self::Gossip(d) => d.stop().await,
        }
    }
}

/// Watches membership changes and logs peer join/leave/crash events.
fn spawn_membership_watcher(
    local_node_id: &str,
    mut rx: watch::Receiver<Vec<NodeInfo>>,
) -> tokio::task::JoinHandle<()> {
    let local_name = local_node_id.to_string();
    tokio::spawn(async move {
        let mut known: HashMap<u64, (String, NodeState)> = HashMap::new();
        for node in rx.borrow_and_update().iter() {
            known.insert(node.id.0, (node.name.clone(), node.state));
        }

        loop {
            if rx.changed().await.is_err() {
                // Sender dropped — discovery shut down
                info!("[{local_name}] Membership watcher stopping (discovery shut down)");
                break;
            }

            let current_peers = rx.borrow_and_update().clone();

            let mut current: HashMap<u64, (String, NodeState)> = HashMap::new();
            for node in &current_peers {
                current.insert(node.id.0, (node.name.clone(), node.state));
            }

            for (id, (name, state)) in &current {
                if !known.contains_key(id) {
                    info!(
                        "[{local_name}] Peer joined: '{}' (id={}, state={})",
                        name, id, state
                    );
                }
            }

            for (id, (name, old_state)) in &known {
                if !current.contains_key(id) {
                    if *old_state == NodeState::Suspected {
                        warn!(
                            "[{local_name}] Peer crashed: '{}' (id={}, was suspected)",
                            name, id
                        );
                    } else {
                        warn!(
                            "[{local_name}] Peer left: '{}' (id={}, was {})",
                            name, id, old_state
                        );
                    }
                }
            }

            for (id, (name, new_state)) in &current {
                if let Some((_, old_state)) = known.get(id) {
                    if old_state != new_state {
                        let level = match new_state {
                            NodeState::Suspected => "WARN",
                            NodeState::Left | NodeState::Draining => "WARN",
                            _ => "INFO",
                        };
                        if level == "WARN" {
                            warn!(
                                "[{local_name}] Peer state changed: '{}' (id={}) {} -> {}",
                                name, id, old_state, new_state
                            );
                        } else {
                            info!(
                                "[{local_name}] Peer state changed: '{}' (id={}) {} -> {}",
                                name, id, old_state, new_state
                            );
                        }
                    }
                }
            }

            known = current;
        }
    })
}

use laminar_db::{LaminarDB, Profile};

use crate::cluster_config::ClusterConfig;
use crate::config::{DiscoverySection, ServerConfig};
use crate::server;

#[derive(Debug, thiserror::Error)]
pub enum ClusterStartupError {
    #[error("discovery failed: {0}")]
    Discovery(String),
    #[error("formation timeout: only {found} of {needed} peers discovered")]
    FormationTimeout { found: usize, needed: usize },
    #[error("engine construction failed: {0}")]
    EngineConstruction(String),
    #[error("HTTP startup failed: {0}")]
    HttpStartup(String),
    #[error("engine shutdown failed: {0}")]
    EngineShutdown(String),
}

struct ProcessLeaseRuntime {
    acquired: laminar_core::cluster::control::ProcessLease,
    deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    live_rx: watch::Receiver<bool>,
    shutdown: tokio_util::sync::CancellationToken,
    renewal_task: tokio::task::JoinHandle<()>,
    fence_task: Option<tokio::task::JoinHandle<()>>,
}

impl ProcessLeaseRuntime {
    fn is_live(&self) -> bool {
        self.deadline.is_live() && *self.live_rx.borrow() && !self.renewal_task.is_finished()
    }

    fn install_fence(
        &mut self,
        db: Arc<LaminarDB>,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        leader_lease_shutdown: Option<tokio_util::sync::CancellationToken>,
    ) {
        let mut live_rx = self.live_rx.clone();
        self.fence_task = Some(tokio::spawn(async move {
            loop {
                if !*live_rx.borrow_and_update() {
                    break;
                }
                if live_rx.changed().await.is_err() {
                    break;
                }
            }
            controller.fence_process_lease();
            db.fence_cluster_startup();
            if let Some(token) = leader_lease_shutdown {
                token.cancel();
            }
            tracing::error!(
                node = controller.instance_id().0,
                "stable node identity lease lost; database intake and cluster control fenced"
            );
        }));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatalogStartupAuthority {
    DurableLease,
    ActivePeer,
}

async fn wait_for_catalog_startup_authority(
    controller: &Arc<laminar_core::cluster::control::ClusterController>,
    timeout: std::time::Duration,
) -> Result<CatalogStartupAuthority, String> {
    let mut grants = controller
        .leader_grant_watch()
        .ok_or_else(|| "durable leader lease fencing is not installed".to_string())?;
    let mut members = controller.members_watch();
    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        if controller.capture_catalog_bootstrap_proof().is_some() {
            return Ok(CatalogStartupAuthority::DurableLease);
        }
        if members.borrow().iter().any(|member| {
            member.id != controller.instance_id() && matches!(member.state, NodeState::Active)
        }) {
            return Ok(CatalogStartupAuthority::ActivePeer);
        }

        tokio::select! {
            () = &mut deadline => {
                return Err(format!(
                    "timed out after {timeout:?} waiting for the durable catalog bootstrap lease or an active peer"
                ));
            }
            changed = grants.changed() => {
                if changed.is_err() {
                    return Err("durable leader lease manager stopped during catalog bootstrap".into());
                }
            }
            changed = members.changed() => {
                if changed.is_err() {
                    return Err("membership discovery stopped during catalog bootstrap".into());
                }
            }
        }
    }
}

impl Drop for ProcessLeaseRuntime {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.renewal_task.abort();
        if let Some(task) = &self.fence_task {
            task.abort();
        }
    }
}

pub struct ClusterHandle {
    db: Arc<LaminarDB>,
    discovery: DiscoveryImpl,
    api_handle: tokio::task::JoinHandle<()>,
    watcher_handle: Option<tokio::task::JoinHandle<()>>,
    membership_handle: tokio::task::JoinHandle<()>,
    /// This node's own membership record. Cloned and re-announced with
    /// [`NodeState::Draining`] on shutdown so peers stop routing to us.
    local_node: NodeInfo,
    /// Cluster control plane (gossip discovery only). `begin_drain` is
    /// called on shutdown so the leader excludes us from vnode
    /// assignment.
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    /// Durable vnode assignment snapshot. Polled on shutdown to block
    /// until the leader has reassigned every vnode we own.
    snapshot_store: Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    /// Fixed vnode cardinality used to validate the durable drain head before shutdown.
    vnode_count: u32,
    /// Cancels the leader-lease renewal loop on shutdown so a draining
    /// node stops renewing and its lease expires promptly.
    lease_shutdown_token: Option<tokio_util::sync::CancellationToken>,
    /// Keeps the stable-node process lease renewed for the lifetime of this runtime.
    _process_lease: ProcessLeaseRuntime,
    /// Snapshot watcher + leader rebalance controller tasks. Empty
    /// if the deployment has no `AssignmentSnapshotStore` (non-cluster
    /// or pre-configured legacy).
    rebalance_tasks: Vec<tokio::task::JoinHandle<()>>,
    /// Shutdown signal shared with [`Self::rebalance_tasks`]. Notified
    /// on [`Self::wait_for_shutdown`] so those loops observe the
    /// request and exit cleanly before we abort.
    rebalance_shutdown: Arc<tokio::sync::Notify>,
}

impl ClusterHandle {
    pub async fn wait_for_shutdown(mut self) -> Result<(), ClusterStartupError> {
        signal::ctrl_c()
            .await
            .map_err(|e| ClusterStartupError::Discovery(format!("signal handler: {e}")))?;

        info!("Received shutdown signal, shutting down cluster node...");

        // Graceful drain. Discovery and the rebalance control plane must
        // stay alive here: peers need to observe our Draining state and
        // the leader needs to rotate our vnodes away before we tear down.
        //
        // 1. Announce Draining so peers stop routing to us and the
        //    leader's `assignable_instances` drops us from assignment.
        let mut draining = self.local_node.clone();
        draining.state = NodeState::Draining;
        if let Err(e) = self.discovery.announce(draining).await {
            warn!("Failed to announce draining state: {e}");
        }

        // 2. Flip the local draining flag so that if we are the leader,
        //    our own rebalance controller excludes us from assignment.
        if let Some(controller) = &self.cluster_controller {
            controller.begin_drain();
        }
        // Stop renewing before waiting for reassignment. The local controller is already
        // ineligible to lead, and the durable lease must lapse so a peer can drive the drain.
        if let Some(token) = &self.lease_shutdown_token {
            token.cancel();
        }

        // 3. Block until the leader has reassigned every vnode we own,
        //    bounded so a stuck cluster can't wedge shutdown forever.
        if let Some(store) = &self.snapshot_store {
            if let Some(controller) = &self.cluster_controller {
                let me = laminar_core::state::NodeId(self.local_node.id.0);
                let drained = match controller.checkpoint_authority() {
                    Ok(authority) => {
                        laminar_db::rebalance::wait_until_drained(
                            store,
                            Some(&authority),
                            me,
                            self.vnode_count,
                            std::time::Duration::from_secs(1),
                            std::time::Duration::from_secs(30),
                        )
                        .await
                    }
                    Err(error) => {
                        warn!(%error, "Drain cannot certify assignment authority");
                        false
                    }
                };
                if drained {
                    info!("Drain complete: all owned vnodes reassigned");
                } else {
                    warn!("Drain timed out after 30s; proceeding with shutdown");
                }
            } else {
                info!("Control plane is inactive; skipping drain");
            }
        }

        // Tell rebalance tasks to exit at their next select point.
        // Fire all aborts before awaiting any so a slow responder
        // doesn't serialise the others.
        self.rebalance_shutdown.notify_waiters();
        for task in &self.rebalance_tasks {
            task.abort();
        }
        for task in self.rebalance_tasks.drain(..) {
            let _ = task.await;
        }

        // Settle checkpoint tails while the lease, membership, and discovery control plane are
        // still live. Tearing those down first can manufacture a leadership loss in the middle
        // of an otherwise clean durable cut.
        self.db
            .shutdown()
            .await
            .map_err(|error| ClusterStartupError::EngineShutdown(error.to_string()))?;

        // Stop membership watcher
        self.membership_handle.abort();

        // Stop discovery
        if let Err(e) = self.discovery.stop().await {
            warn!("Discovery stop error: {e}");
        }

        // Abort config watcher
        if let Some(wh) = &self.watcher_handle {
            wh.abort();
        }

        // Abort HTTP
        self.api_handle.abort();

        info!("Cluster node shutdown complete");
        Ok(())
    }
}

const PROCESS_LEASE_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const PROCESS_LEASE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const NAMESPACE_PROOF_KEY: &str = "control:shared-namespace-proof-v1";
const NAMESPACE_PROOF_VERSION: u8 = 1;
const NAMESPACE_PROOF_MAX_RECORD_BYTES: usize = 512;
const NAMESPACE_PROOF_MAX_SENTINEL_BYTES: u64 = 512;
const NAMESPACE_PROOF_MAX_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const NAMESPACE_PROOF_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const NAMESPACE_PROOF_READ_CONCURRENCY: usize = 16;

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct NamespaceProofRecord {
    version: u8,
    node_id: u64,
    boot_incarnation: uuid::Uuid,
    nonce: uuid::Uuid,
    roster_sha256: String,
}

impl NamespaceProofRecord {
    fn validate_identity(
        &self,
        participant: laminar_core::checkpoint::CheckpointParticipant,
    ) -> Result<(), String> {
        if self.version != NAMESPACE_PROOF_VERSION
            || self.node_id != participant.node_id
            || self.boot_incarnation != participant.boot_incarnation
            || self.nonce.is_nil()
            || self.roster_sha256.len() != 64
            || !self
                .roster_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(format!(
                "node {} published a stale or mismatched shared-namespace proof",
                participant.node_id
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum NamespaceProofStore {
    Checkpoint,
    State,
}

impl NamespaceProofStore {
    const fn name(self) -> &'static str {
        match self {
            Self::Checkpoint => "checkpoint",
            Self::State => "state",
        }
    }
}

fn namespace_proof_roster_sha256(
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
) -> String {
    use sha2::{Digest, Sha256};

    let mut hash = Sha256::new();
    hash.update(b"LAMINAR_SHARED_NAMESPACE_ROSTER_V1\0");
    hash.update(
        u64::try_from(participants.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for participant in participants {
        hash.update(participant.node_id.to_be_bytes());
        hash.update(participant.boot_incarnation.as_bytes());
    }
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn namespace_proof_path(store: NamespaceProofStore, node_id: u64) -> object_store::path::Path {
    // Object-store namespace equality is symmetric. Fixed per-node keys retained for the process
    // lifetime let a rolling joiner test both local clients against every active peer without
    // making peers rerun startup; the stable-node lease authorizes the next boot's overwrite.
    object_store::path::Path::from(format!(
        "cluster-namespace-proof/v1/{}/node={node_id}/sentinel",
        store.name()
    ))
}

fn namespace_proof_sentinel(
    store: NamespaceProofStore,
    record: &NamespaceProofRecord,
) -> bytes::Bytes {
    bytes::Bytes::from(format!(
        "LAMINAR_SHARED_NAMESPACE_V1\n{}\n{}\n{}\n{}\n{}\n",
        store.name(),
        record.node_id,
        record.boot_incarnation,
        record.nonce,
        record.roster_sha256
    ))
}

async fn write_namespace_proof_sentinel(
    object_store: &Arc<dyn object_store::ObjectStore>,
    store: NamespaceProofStore,
    record: &NamespaceProofRecord,
) -> Result<(), String> {
    let payload = namespace_proof_sentinel(store, record);
    if u64::try_from(payload.len()).unwrap_or(u64::MAX) > NAMESPACE_PROOF_MAX_SENTINEL_BYTES {
        return Err("shared-namespace sentinel exceeds its fixed size bound".into());
    }
    object_store
        .put(
            &namespace_proof_path(store, record.node_id),
            object_store::PutPayload::from(payload),
        )
        .await
        .map(|_| ())
        .map_err(|error| format!("write {} namespace sentinel: {error}", store.name()))
}

async fn read_namespace_proof_sentinel(
    object_store: &Arc<dyn object_store::ObjectStore>,
    store: NamespaceProofStore,
    record: &NamespaceProofRecord,
) -> Result<(), String> {
    let result = object_store
        .get(&namespace_proof_path(store, record.node_id))
        .await
        .map_err(|error| {
            format!(
                "read node {} {} namespace sentinel: {error}",
                record.node_id,
                store.name()
            )
        })?;
    if result.meta.size == 0 || result.meta.size > NAMESPACE_PROOF_MAX_SENTINEL_BYTES {
        return Err(format!(
            "node {} {} namespace sentinel is {} bytes; maximum is {}",
            record.node_id,
            store.name(),
            result.meta.size,
            NAMESPACE_PROOF_MAX_SENTINEL_BYTES
        ));
    }
    let bytes = result.bytes().await.map_err(|error| {
        format!(
            "read node {} {} namespace sentinel body: {error}",
            record.node_id,
            store.name()
        )
    })?;
    if bytes != namespace_proof_sentinel(store, record) {
        return Err(format!(
            "node {} {} namespace sentinel does not match its boot proof",
            record.node_id,
            store.name()
        ));
    }
    Ok(())
}

async fn verify_namespace_proof_visibility(
    control: &Arc<dyn laminar_core::cluster::control::ClusterKv>,
    checkpoint_store: &Arc<dyn object_store::ObjectStore>,
    state_store: &Arc<dyn object_store::ObjectStore>,
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
    local: laminar_core::checkpoint::CheckpointParticipant,
    roster_sha256: &str,
) -> Result<(), String> {
    use futures::StreamExt;

    let checks = futures::stream::iter(participants.iter().copied())
        .map(|participant| async move {
            let encoded = control
                .read_from_checked(NodeId(participant.node_id), NAMESPACE_PROOF_KEY)
                .await?
                .ok_or_else(|| {
                    format!(
                        "node {} has not published its shared-namespace proof",
                        participant.node_id
                    )
                })?;
            if encoded.len() > NAMESPACE_PROOF_MAX_RECORD_BYTES {
                return Err(format!(
                    "node {} shared-namespace proof exceeds {} bytes",
                    participant.node_id, NAMESPACE_PROOF_MAX_RECORD_BYTES
                ));
            }
            let record: NamespaceProofRecord = serde_json::from_str(&encoded).map_err(|error| {
                format!(
                    "decode node {} shared-namespace proof: {error}",
                    participant.node_id
                )
            })?;
            record.validate_identity(participant)?;
            if participant == local && record.roster_sha256 != roster_sha256 {
                return Err("local shared-namespace proof has the wrong startup roster".into());
            }
            tokio::try_join!(
                read_namespace_proof_sentinel(
                    checkpoint_store,
                    NamespaceProofStore::Checkpoint,
                    &record,
                ),
                read_namespace_proof_sentinel(state_store, NamespaceProofStore::State, &record),
            )?;
            Ok::<_, String>(())
        })
        .buffer_unordered(NAMESPACE_PROOF_READ_CONCURRENCY);
    let results: Vec<Result<(), String>> = checks.collect().await;
    for result in results {
        result?;
    }
    Ok(())
}

async fn wait_for_namespace_proof_visibility(
    control: &Arc<dyn laminar_core::cluster::control::ClusterKv>,
    checkpoint_store: &Arc<dyn object_store::ObjectStore>,
    state_store: &Arc<dyn object_store::ObjectStore>,
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
    local: laminar_core::checkpoint::CheckpointParticipant,
    roster_sha256: &str,
    deadline: tokio::time::Instant,
) -> Result<(), String> {
    let mut last_failure = "no verification attempt completed".to_string();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "shared-namespace peer visibility timed out: {last_failure}"
            ));
        }
        match tokio::time::timeout(
            remaining,
            verify_namespace_proof_visibility(
                control,
                checkpoint_store,
                state_store,
                participants,
                local,
                roster_sha256,
            ),
        )
        .await
        {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_failure = error,
            Err(_) => {
                return Err(format!(
                    "shared-namespace peer visibility timed out: {last_failure}"
                ));
            }
        }
        tokio::time::sleep(
            NAMESPACE_PROOF_RETRY_INTERVAL
                .min(deadline.saturating_duration_since(tokio::time::Instant::now())),
        )
        .await;
    }
}

async fn prove_shared_object_store_namespaces(
    local: laminar_core::checkpoint::CheckpointParticipant,
    participants: &[laminar_core::checkpoint::CheckpointParticipant],
    control: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    checkpoint_store: Arc<dyn object_store::ObjectStore>,
    state_store: Arc<dyn object_store::ObjectStore>,
    timeout: std::time::Duration,
) -> Result<(), ClusterStartupError> {
    if participants.is_empty()
        || participants.len() > laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
        || participants
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
        || participants
            .iter()
            .any(|participant| participant.node_id == 0 || participant.boot_incarnation.is_nil())
        || participants
            .iter()
            .filter(|participant| **participant == local)
            .count()
            != 1
    {
        return Err(ClusterStartupError::EngineConstruction(
            "shared-namespace proof requires one canonical exact startup roster".into(),
        ));
    }
    let timeout = timeout.min(NAMESPACE_PROOF_MAX_TIMEOUT);
    if timeout.is_zero() {
        return Err(ClusterStartupError::EngineConstruction(
            "shared-namespace proof timeout is zero".into(),
        ));
    }
    let roster_sha256 = namespace_proof_roster_sha256(participants);
    let record = NamespaceProofRecord {
        version: NAMESPACE_PROOF_VERSION,
        node_id: local.node_id,
        boot_incarnation: local.boot_incarnation,
        nonce: uuid::Uuid::new_v4(),
        roster_sha256: roster_sha256.clone(),
    };
    let deadline = tokio::time::Instant::now() + timeout;
    let proof = async {
        tokio::try_join!(
            write_namespace_proof_sentinel(
                &checkpoint_store,
                NamespaceProofStore::Checkpoint,
                &record,
            ),
            write_namespace_proof_sentinel(&state_store, NamespaceProofStore::State, &record),
        )?;
        let encoded = serde_json::to_string(&record).map_err(|error| error.to_string())?;
        if encoded.len() > NAMESPACE_PROOF_MAX_RECORD_BYTES {
            return Err("local shared-namespace proof exceeds its size bound".to_string());
        }
        control.write_checked(NAMESPACE_PROOF_KEY, encoded).await?;
        wait_for_namespace_proof_visibility(
            &control,
            &checkpoint_store,
            &state_store,
            participants,
            local,
            &roster_sha256,
            deadline,
        )
        .await
    };
    match tokio::time::timeout(timeout, proof).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ClusterStartupError::EngineConstruction(format!(
            "shared checkpoint/state namespace proof failed: {error}"
        ))),
        Err(_) => Err(ClusterStartupError::EngineConstruction(format!(
            "shared checkpoint/state namespace proof exceeded {timeout:?}"
        ))),
    }
}

#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
fn unix_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

fn start_process_lease_runtime(
    store: Arc<laminar_core::cluster::control::ProcessLeaseStore>,
    owner: uuid::Uuid,
    config: laminar_core::cluster::control::ProcessLeaseConfig,
    acquired: laminar_core::cluster::control::ProcessLease,
) -> Result<ProcessLeaseRuntime, ClusterStartupError> {
    let manager =
        laminar_core::cluster::control::ProcessLeaseManager::new(store, owner, config, &acquired)
            .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "start stable node identity lease renewal: {error}"
            ))
        })?;
    let live_rx = manager.live_watch();
    let deadline = manager.deadline();
    let shutdown = tokio_util::sync::CancellationToken::new();
    let renewal_task = manager.spawn(shutdown.clone());
    Ok(ProcessLeaseRuntime {
        acquired,
        deadline,
        live_rx,
        shutdown,
        renewal_task,
        fence_task: None,
    })
}

async fn acquire_process_lease(
    store: Arc<laminar_core::cluster::control::ProcessLeaseStore>,
    owner: uuid::Uuid,
    config: laminar_core::cluster::control::ProcessLeaseConfig,
) -> Result<ProcessLeaseRuntime, ClusterStartupError> {
    use laminar_core::cluster::control::ProcessLeaseOutcome;

    let deadline = std::time::Instant::now() + PROCESS_LEASE_ACQUIRE_TIMEOUT;
    let mut last_failure = "no acquisition attempt completed".to_string();
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Err(ClusterStartupError::EngineConstruction(format!(
                "stable node identity lease was not acquired within {PROCESS_LEASE_ACQUIRE_TIMEOUT:?}: {last_failure}"
            )));
        }
        let attempt_timeout = PROCESS_LEASE_IO_TIMEOUT.min(remaining);
        match tokio::time::timeout(
            attempt_timeout,
            store.try_acquire(owner, unix_time_millis()),
        )
        .await
        {
            Ok(Ok(ProcessLeaseOutcome::Acquired(acquired))) => {
                return start_process_lease_runtime(store, owner, config, acquired);
            }
            Ok(Ok(ProcessLeaseOutcome::Held(incumbent))) => {
                last_failure = format!(
                    "live boot {} owns term {} until {}",
                    incumbent.owner, incumbent.term, incumbent.expires_at_ms
                );
                let observation = store.observe_rival(&incumbent).map_err(|error| {
                    ClusterStartupError::EngineConstruction(format!(
                        "observe stable node identity lease: {error}"
                    ))
                })?;
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                let observation_time = config.ttl.min(remaining);
                tokio::time::sleep(observation_time).await;
                if observation_time < config.ttl {
                    continue;
                }
                match tokio::time::timeout(
                    PROCESS_LEASE_IO_TIMEOUT
                        .min(deadline.saturating_duration_since(std::time::Instant::now())),
                    store.try_takeover(owner, &observation, unix_time_millis()),
                )
                .await
                {
                    Ok(Ok(ProcessLeaseOutcome::Acquired(acquired))) => {
                        return start_process_lease_runtime(store, owner, config, acquired);
                    }
                    Ok(Ok(ProcessLeaseOutcome::Held(current))) => {
                        last_failure = format!(
                            "boot {} renewed or won term {} during takeover observation",
                            current.owner, current.term
                        );
                    }
                    Ok(Err(error)) => last_failure = error.to_string(),
                    Err(_) => {
                        last_failure =
                            "takeover verification exceeded the object-store I/O timeout".into();
                    }
                }
            }
            Ok(Err(error)) => {
                last_failure = error.to_string();
                tokio::time::sleep(
                    std::time::Duration::from_millis(250)
                        .min(deadline.saturating_duration_since(std::time::Instant::now())),
                )
                .await;
            }
            Err(_) => {
                last_failure = format!("object-store operation exceeded {attempt_timeout:?}");
            }
        }
    }
}

/// Start a LaminarDB server in cluster (multi-node) mode.
pub async fn start_cluster(
    config: ServerConfig,
    cluster_cfg: ClusterConfig,
    config_path: PathBuf,
) -> Result<ClusterHandle, ClusterStartupError> {
    let node_id_str = cluster_cfg.node_id.as_str().to_string();
    let node_id_num = numeric_node_id(&node_id_str);
    let node_id = NodeId(node_id_num);

    let bind_addr = &config.server.bind;
    let http_port = if let Some(colon) = bind_addr.rfind(':') {
        bind_addr[colon + 1..].parse::<u16>().unwrap_or(8080)
    } else {
        8080
    };

    // Extract the host part from bind address, handling IPv6.
    // Examples: "127.0.0.1:8080" → "127.0.0.1", "[::1]:8080" → "[::1]"
    let bind_host = if let Some(bracket_end) = bind_addr.rfind(']') {
        // IPv6: "[::1]:8080" — take up to and including ']'
        &bind_addr[..=bracket_end]
    } else if let Some(colon) = bind_addr.rfind(':') {
        // IPv4: "127.0.0.1:8080" — take before the last ':'
        &bind_addr[..colon]
    } else {
        bind_addr.as_str()
    };

    let host_trimmed = bind_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host_trimmed == "0.0.0.0" || host_trimmed == "::" || host_trimmed.is_empty();
    let advertise_host = if let Some(ref host) = cluster_cfg.discovery.advertise_host {
        host.clone()
    } else if ip_wildcard {
        let hostname = gethostname::gethostname();
        let hostname = hostname.to_string_lossy().into_owned();
        if hostname.is_empty() {
            "127.0.0.1".to_string()
        } else {
            hostname
        }
    } else {
        bind_host.to_string()
    };

    // Install control-plane mTLS (if configured) before any server/client binds.
    install_cluster_tls(&cluster_cfg.discovery)?;

    // Claim the stable node identity before discovery can publish a duplicate member. The
    // durable recovery authority is deliberately not published until the database runtime exists.
    if !laminar_core::state::StateBackendDurability::for_storage_url(&config.checkpoint.url)
        .satisfies(laminar_core::state::StateBackendDurability::ClusterShared)
    {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster mode requires ClusterShared checkpoint storage".into(),
        ));
    }
    let control_store = build_control_store(&config)?.ok_or_else(|| {
        ClusterStartupError::EngineConstruction(
            "cluster mode requires shared durable control storage; configure a checkpoint object-store URL"
                .into(),
        )
    })?;
    let process_incarnation = uuid::Uuid::new_v4();
    let process_lease_config = laminar_core::cluster::control::ProcessLeaseConfig::default();
    let process_lease_ttl_ms =
        i64::try_from(process_lease_config.ttl.as_millis()).map_err(|_| {
            ClusterStartupError::EngineConstruction(
                "process lease TTL exceeds i64 milliseconds".into(),
            )
        })?;
    let process_lease_store = Arc::new(laminar_core::cluster::control::ProcessLeaseStore::new(
        Arc::clone(&control_store),
        node_id,
        process_lease_ttl_ms,
    ));
    let mut process_lease = acquire_process_lease(
        process_lease_store,
        process_incarnation,
        process_lease_config,
    )
    .await?;
    info!(
        term = process_lease.acquired.term,
        ttl_seconds = process_lease_config.ttl.as_secs(),
        "Stable node identity lease acquired"
    );

    // Bind ShuffleReceiver first to discover port and publish it in metadata tags.
    let bind_addr: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid shuffle bind host: {e}"))
    })?;
    let shuffle_receiver = Arc::new(
        laminar_core::shuffle::ShuffleReceiver::bind(node_id.0, bind_addr, process_incarnation)
            .await
            .map_err(|e| ClusterStartupError::EngineConstruction(format!("shuffle bind: {e}")))?,
    );
    let shuffle_advertise = shuffle_advertise_addr(shuffle_receiver.local_addr(), &advertise_host);

    let mut local_node = NodeInfo {
        id: node_id,
        name: node_id_str.clone(),
        rpc_address: format!("{advertise_host}:{http_port}"),
        // `NodeInfo` retains this legacy wire field, but LaminarDB has no Raft
        // transport and must not advertise a service that is not bound.
        raft_address: String::new(),
        state: NodeState::Joining,
        metadata: NodeMetadata {
            cores: num_cpus(),
            memory_bytes: 0,
            failure_domain: cluster_cfg.discovery.failure_domain.clone(),
            tags: std::collections::HashMap::new(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        last_heartbeat_ms: 0,
    };
    local_node.metadata.tags.insert(
        laminar_core::shuffle::SHUFFLE_ADDR_KEY.to_string(),
        shuffle_advertise.clone(),
    );
    local_node.metadata.tags.insert(
        PROCESS_INCARNATION_TAG.to_string(),
        process_incarnation.to_string(),
    );

    // 1. Start discovery layer
    info!(
        "Starting cluster discovery (strategy: {})",
        cluster_cfg.discovery.strategy
    );

    let mut discovery: DiscoveryImpl = match cluster_cfg.discovery.strategy.as_str() {
        "gossip" => {
            let gossip_config = GossipDiscoveryConfig {
                gossip_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                seed_nodes: cluster_cfg.discovery.seeds.clone(),
                gossip_interval: std::time::Duration::from_secs(1),
                phi_threshold: 8.0,
                dead_node_grace_period: std::time::Duration::from_secs(60),
                cluster_id: "laminardb".to_string(),
                node_id,
                process_generation: process_lease.acquired.term,
                local_node: local_node.clone(),
                advertise_host: cluster_cfg.discovery.advertise_host.clone(),
            };
            DiscoveryImpl::Gossip(GossipDiscovery::new(gossip_config))
        }
        "static" => {
            let static_config = StaticDiscoveryConfig {
                local_node: local_node.clone(),
                seeds: cluster_cfg.discovery.seeds.clone(),
                heartbeat_interval: std::time::Duration::from_secs(1),
                suspect_threshold: 3,
                dead_threshold: 10,
                listen_address: format!("{bind_host}:{}", cluster_cfg.discovery.gossip_port),
                process_generation: process_lease.acquired.term,
                process_incarnation,
            };
            DiscoveryImpl::Static(StaticDiscovery::new(static_config))
        }
        strategy => {
            return Err(ClusterStartupError::Discovery(format!(
                "unsupported discovery strategy {strategy:?}; expected \"gossip\" or \"static\""
            )));
        }
    };

    discovery
        .start()
        .await
        .map_err(|e| ClusterStartupError::Discovery(e.to_string()))?;
    info!("Discovery layer started");

    // 2. Wait for expected membership. Seeds include self by
    // convention (every node lists the full cluster), so the target
    // is `seeds.len() - 1`. An empty seed list is always a config
    // error in cluster mode — fail fast instead of hanging.
    if cluster_cfg.discovery.seeds.is_empty() {
        return Err(ClusterStartupError::Discovery(
            "cluster mode requires [discovery].seeds — list every node's \
             gossip address including this one (e.g. [\"node-0:7946\", \
             \"node-1:7946\"]); expected membership is derived from it"
                .into(),
        ));
    }
    let expected_peers = cluster_cfg.discovery.seeds.len().saturating_sub(1);
    let deadline = std::time::Instant::now() + cluster_cfg.formation_timeout;
    let mut last_seen = 0usize;
    let peers: Vec<NodeInfo> = loop {
        if let Ok(discovered) = discovery.peers().await {
            let eligible: Vec<NodeInfo> = discovered
                .into_iter()
                .filter(|peer| matches!(peer.state, NodeState::Joining | NodeState::Active))
                .collect();
            last_seen = eligible.len();
            if eligible.len() >= expected_peers {
                break eligible;
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(ClusterStartupError::FormationTimeout {
                found: last_seen,
                needed: expected_peers,
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    };
    info!(
        "Discovered {} peer(s) (expected {})",
        peers.len(),
        expected_peers
    );
    let roster_timeout = cluster_cfg
        .formation_timeout
        .min(NAMESPACE_PROOF_MAX_TIMEOUT);
    let startup_participants = match tokio::time::timeout(
        roster_timeout,
        assignment_seed_participants(
            laminar_core::state::NodeId(node_id.0),
            process_incarnation,
            &peers,
            &control_store,
            process_lease_ttl_ms,
        ),
    )
    .await
    {
        Ok(Ok(participants)) => participants,
        Ok(Err(error)) => {
            let _ = discovery.stop().await;
            return Err(error);
        }
        Err(_) => {
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "exact startup roster lease validation exceeded {roster_timeout:?}"
            )));
        }
    };
    let expected_participants = expected_peers + 1;
    if startup_participants.len() != expected_participants {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::Discovery(format!(
            "startup roster contains {} participants but the configured exact roster requires {expected_participants}",
            startup_participants.len()
        )));
    }

    // Build LaminarDB with Profile::Cluster
    let mut builder = LaminarDB::builder();
    builder = builder
        .profile(Profile::Cluster)
        .delivery_guarantee(config.server.delivery);
    if let Some(ref token) = config.server.console_token {
        builder = builder.http_auth_token(token.expose());
    }
    if let Some(path) = config.state.local_storage_dir() {
        builder = builder.storage_dir(path);
    }

    config
        .state
        .validate()
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("state backend: {e}")))?;
    if !config
        .state
        .durability_scope()
        .satisfies(laminar_core::state::StateBackendDurability::ClusterShared)
    {
        return Err(ClusterStartupError::EngineConstruction(
            "cluster mode requires ClusterShared state storage".into(),
        ));
    }
    let state_proof_store = config
        .state
        .build_object_store()
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!(
                "state namespace proof object store: {e}"
            ))
        })?
        .ok_or_else(|| {
            ClusterStartupError::EngineConstruction(
                "cluster mode requires an object-store-backed state namespace".into(),
            )
        })?;
    let state_vnode_capacity = match &config.state {
        laminar_core::state::StateBackendConfig::ObjectStore { vnode_capacity, .. } => {
            *vnode_capacity
        }
        _ => {
            return Err(ClusterStartupError::EngineConstruction(
                "cluster mode requires an object-store-backed state namespace".into(),
            ));
        }
    };
    let state_backend = cluster_state_backend(
        Arc::clone(&state_proof_store),
        node_id,
        state_vnode_capacity,
    );

    // Build the vnode registry. If a shared `AssignmentSnapshot` already exists,
    // every node adopts it; otherwise the first peer CAS-creates it and losers
    // re-load and adopt the winner.
    let (vnode_registry, snapshot_store) = resolve_vnode_assignment(
        node_id,
        &peers,
        config.state.vnode_capacity(),
        Some(Arc::clone(&control_store)),
        &startup_participants,
    )
    .await?;

    // Discovery/barrier traffic stays on the low-latency KV. Coordinated recovery always uses
    // the shared object store so generation, phases, and acknowledgements survive process loss.
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    let (controller_kv, recovery_kv): (Option<Arc<dyn ClusterKv>>, Option<Arc<dyn ClusterKv>>) =
        match discovery {
            DiscoveryImpl::Gossip(ref gossip) => {
                let fast = gossip
                    .chitchat_handle()
                    .map(|handle| Arc::new(ChitchatKv::from_handle(handle)) as Arc<dyn ClusterKv>);
                let durable = Arc::new(ObjectStoreClusterKv::new(
                    process_lease.acquired.clone(),
                    Arc::clone(&process_lease.deadline),
                    process_lease_ttl_ms,
                    Arc::clone(&control_store),
                    discovery.membership_watch(),
                )) as Arc<dyn ClusterKv>;
                (fast, Some(durable))
            }
            DiscoveryImpl::Static(_) => {
                let durable = Arc::new(ObjectStoreClusterKv::new(
                    process_lease.acquired.clone(),
                    Arc::clone(&process_lease.deadline),
                    process_lease_ttl_ms,
                    Arc::clone(&control_store),
                    discovery.membership_watch(),
                )) as Arc<dyn ClusterKv>;
                (Some(Arc::clone(&durable)), Some(durable))
            }
        };

    let namespace_control = controller_kv.as_ref().cloned().ok_or_else(|| {
        ClusterStartupError::EngineConstruction(
            "cluster discovery did not provide a namespace-proof control channel".into(),
        )
    })?;
    let local_participant = laminar_core::checkpoint::CheckpointParticipant {
        node_id: node_id.0,
        boot_incarnation: process_incarnation,
    };
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before shared-namespace proof".into(),
        ));
    }
    if let Err(error) = prove_shared_object_store_namespaces(
        local_participant,
        &startup_participants,
        namespace_control,
        Arc::clone(&control_store),
        state_proof_store,
        cluster_cfg.formation_timeout,
    )
    .await
    {
        let _ = discovery.stop().await;
        return Err(error);
    }
    if !process_lease.is_live() {
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during shared-namespace proof".into(),
        ));
    }
    info!(
        participants = startup_participants.len(),
        "Shared checkpoint and state namespaces verified"
    );

    let cluster_controller = match (controller_kv, recovery_kv) {
        (Some(kv), Some(recovery_kv)) => {
            let controller = install_cluster_controller(
                node_id,
                kv,
                recovery_kv,
                snapshot_store.clone(),
                discovery.membership_watch(),
                bind_host,
                &advertise_host,
                process_incarnation,
            )
            .await?;
            // Hand the controller this node's own locality so the topology-
            // aware rebalancer can place self correctly (peers' localities
            // arrive via gossip; self is folded in by id only).
            controller.set_self_locality(laminar_core::state::Locality::parse(
                local_node.metadata.failure_domain.as_deref().unwrap_or(""),
            ));
            controller.set_process_lease_deadline(Arc::clone(&process_lease.deadline));
            builder = builder.cluster_controller(Arc::clone(&controller));
            Some(controller)
        }
        (None, None) => {
            return Err(ClusterStartupError::EngineConstruction(
                "cluster mode requires shared durable control storage; configure a checkpoint object-store URL"
                    .into(),
            ));
        }
        _ => {
            return Err(ClusterStartupError::EngineConstruction(
                "cluster control requires both a discovery KV and durable recovery storage".into(),
            ));
        }
    };

    // LaminarDB derives the participant namespace from the installed controller. Keep the base
    // URL shared so durable commit decisions remain visible to every node.
    builder = builder.incremental_emit(config.server.incremental_emit);
    builder =
        server::apply_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint, true);

    builder = builder
        .state_backend(Arc::clone(&state_backend))
        .vnode_registry(Arc::clone(&vnode_registry));

    // Durable cluster 2PC decision store, on the shared control-plane bucket
    // (not the per-node state path) so commit decisions are cluster-wide.
    // Without this the leader's `Commit` announcement is the only commit
    // signal — ephemeral, so a mid-2PC leader crash produces split state.
    let decision_store = Arc::new(
        laminar_core::cluster::control::CheckpointDecisionStore::new(Arc::clone(&control_store)),
    );
    builder = builder.decision_store(decision_store);

    // Hand the builder the same snapshot store resolved in `resolve_vnode_assignment`
    // so the snapshot watcher and rebalance controller share one backing object.
    if let Some(snap_store) = snapshot_store.clone() {
        builder = builder.assignment_snapshot_store(snap_store);
    }

    // Catalog sealing and leader fencing share one append-only CAS sequence. A catalog write can
    // therefore linearize before a takeover or be rejected by it; there is no check-then-write
    // gap across independent objects.
    let lease_cfg = laminar_core::cluster::control::LeaderLeaseConfig::default();
    let ttl_ms = i64::try_from(lease_cfg.ttl.as_millis()).map_err(|_| {
        ClusterStartupError::EngineConstruction(
            "leader lease TTL exceeds the durable diagnostic range".into(),
        )
    })?;
    let lease_store = Arc::new(laminar_core::cluster::control::LeaderLeaseStore::new(
        Arc::clone(&control_store),
        ttl_ms,
    ));
    let catalog_store = Arc::new(laminar_core::cluster::control::CatalogManifestStore::new(
        Arc::clone(&lease_store),
    ));
    builder = builder.catalog_manifest_store(Arc::clone(&catalog_store));

    // Shuffle fabric. ShuffleReceiver was bound at startup.
    let shuffle_sender = build_shuffle_sender(
        node_id.0,
        &discovery,
        shuffle_advertise.clone(),
        discovery.membership_watch(),
        process_incarnation,
    )
    .await;

    // Streaming aggregates go through the row-shuffle bridge driven by
    // `IncrementalAggState`; the DataFusion-native aggregate-rewrite path was removed.
    builder = builder
        .shuffle_sender(Arc::clone(&shuffle_sender))
        .shuffle_receiver(Arc::clone(&shuffle_receiver))
        .target_partitions(1);

    let db = builder
        .build()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(e.to_string()))?;

    let startup_controller = cluster_controller
        .as_ref()
        .expect("cluster controller was required before database construction");
    if !process_lease.is_live() {
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost while constructing the database runtime".into(),
        ));
    }
    startup_controller
        .publish_leased_recovery_incarnation(&process_lease.acquired)
        .await
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "publish leased recovery process incarnation: {error}"
            ))
        })?;
    if !process_lease.is_live() {
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during recovery identity publication".into(),
        ));
    }

    // Build prometheus registry before start() so connectors register on it.
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let pipeline_name = config
        .pipelines
        .first()
        .map_or("default", |p| p.name.as_str())
        .to_string();
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), hostname),
        ("pipeline".into(), pipeline_name),
    ]));
    let engine_metrics = Arc::new(laminar_db::EngineMetrics::new(&registry));
    db.set_engine_metrics(engine_metrics);
    db.set_prometheus_registry(Arc::clone(&registry));

    // Fenced leader lease. Wiring the watch into the controller makes
    // `is_leader()` lease-aware, so every leader-gated path (checkpoint, 2PC,
    // rebalance, committer) inherits fencing: a stale candidate whose lease
    // expired stops being the leader. Renewal is gated on `is_gossip_leader` so
    // the lease owner converges to the gossip candidate. Wired before `start()`.
    let lease_shutdown_token: Option<tokio_util::sync::CancellationToken> = match cluster_controller
        .as_ref()
    {
        Some(controller) => {
            controller.set_leader_lease_store(Arc::clone(&lease_store));
            let manager = laminar_core::cluster::control::LeaderLeaseManager::new(
                Arc::clone(&lease_store),
                &process_lease.acquired,
                lease_cfg,
            )
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!("leader lease manager: {error}"))
            })?;
            controller
                .set_leader_lease_watch(
                    manager.lease_watch(),
                    manager.owner().clone(),
                    manager.deadline(),
                )
                .map_err(ClusterStartupError::EngineConstruction)?;
            let token = tokio_util::sync::CancellationToken::new();
            let candidacy = controller.leader_candidacy_watch();
            let _lease_handle = manager.spawn(token.clone(), candidacy);
            info!(
                "Leader lease manager started (ttl={}s)",
                lease_cfg.ttl.as_secs()
            );
            Some(token)
        }
        None => None,
    };

    process_lease.install_fence(
        Arc::clone(&db),
        Arc::clone(startup_controller),
        lease_shutdown_token.clone(),
    );
    if !process_lease.is_live() {
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before pipeline startup".into(),
        ));
    }

    let catalog_startup = async {
        let authority =
            wait_for_catalog_startup_authority(startup_controller, cluster_cfg.formation_timeout)
                .await?;
        server::execute_config_ddl(&db, &config, true)
            .await
            .map_err(|error| error.to_string())?;
        Ok::<_, String>(authority)
    }
    .await;
    match catalog_startup {
        Ok(CatalogStartupAuthority::DurableLease) => {
            info!("Cluster catalog sealed under the durable leader lease");
        }
        Ok(CatalogStartupAuthority::ActivePeer) => {
            info!("Cluster catalog replayed after observing an active peer");
        }
        Err(error) => {
            if let Some(token) = &lease_shutdown_token {
                token.cancel();
            }
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "cluster catalog startup: {error}"
            )));
        }
    }
    if !process_lease.is_live() {
        if let Some(token) = &lease_shutdown_token {
            token.cancel();
        }
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost during catalog bootstrap".into(),
        ));
    }

    // Coordinated recovery is the only cluster fault path. Before start() so an early
    // fault is observed.
    db.fence_cluster_startup();
    db.enable_coordinated_recovery().map_err(|error| {
        ClusterStartupError::EngineConstruction(format!(
            "cluster recovery monitor initialization: {error}"
        ))
    })?;

    if let Err(error) = db.start().await {
        if let Some(token) = &lease_shutdown_token {
            token.cancel();
        }
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "pipeline start: {error}"
        )));
    }
    info!("Pipeline started");

    let rebalance_config = laminar_db::rebalance::RebalanceConfig {
        placement_isolation_tier: cluster_cfg.discovery.placement_isolation_tier,
        ..laminar_db::rebalance::RebalanceConfig::default()
    };

    // Re-acquire the stored assignment through the standard adopt path (a restart boots
    // unassigned). Re-load each attempt so a shed that raced the boot wins; bounded retry so a
    // deferred adoption can't strand a static-discovery node (no snapshot watcher there).
    if let Some(snap_store) = snapshot_store.clone() {
        for attempt in 0u32..5 {
            let adoption_deadline =
                tokio::time::Instant::now() + rebalance_config.checkpoint_timeout;
            match tokio::time::timeout_at(adoption_deadline, snap_store.load()).await {
                Ok(Ok(Some(durable_head))) => {
                    let recovering_drain = durable_head.draining;
                    let snapshot = match tokio::time::timeout_at(
                        adoption_deadline,
                        startup_committed_assignment(
                            snap_store.as_ref(),
                            Some(startup_controller),
                            durable_head,
                        ),
                    )
                    .await
                    {
                        Ok(Ok(snapshot)) => snapshot,
                        Ok(Err(error)) => {
                            if let Some(token) = &lease_shutdown_token {
                                token.cancel();
                            }
                            let _ = db.shutdown().await;
                            let _ = discovery.stop().await;
                            return Err(error);
                        }
                        Err(_) => {
                            if let Some(token) = &lease_shutdown_token {
                                token.cancel();
                            }
                            let _ = db.shutdown().await;
                            let _ = discovery.stop().await;
                            return Err(ClusterStartupError::EngineConstruction(
                                "startup assignment authority audit timed out".into(),
                            ));
                        }
                    };
                    if vnode_registry.assignment_version() >= snapshot.version {
                        break; // already adopted (watcher raced us)
                    }
                    let committed_version = snapshot.version;
                    let adoption = match db
                        .adopt_assignment_snapshot(snapshot, adoption_deadline)
                        .await
                    {
                        Ok(adoption) => adoption,
                        Err(error) => {
                            if let Some(token) = &lease_shutdown_token {
                                token.cancel();
                            }
                            let _ = db.shutdown().await;
                            let _ = discovery.stop().await;
                            return Err(ClusterStartupError::EngineConstruction(format!(
                                "assignment state recovery: {error}"
                            )));
                        }
                    };
                    info!(
                        version = adoption.version,
                        adopted = adoption.adopted,
                        newly_acquired = adoption.newly_acquired.len(),
                        rehydrated = adoption.rehydrated,
                        "startup assignment adoption"
                    );
                    if adoption.adopted {
                        if recovering_drain {
                            info!(
                                committed_version,
                                "startup adopted the retained committed assignment; durable drain abort remains fenced"
                            );
                        }
                        break;
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, attempt, "startup snapshot load failed");
                }
                Err(_) => tracing::warn!(
                    attempt,
                    timeout = ?rebalance_config.checkpoint_timeout,
                    "startup snapshot load timed out"
                ),
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    match catalog_store.load().await {
        Ok(Some(_)) => {}
        Ok(None) => {
            if let Some(token) = &lease_shutdown_token {
                token.cancel();
            }
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(
                "cluster catalog is not sealed before readiness announcement".into(),
            ));
        }
        Err(error) => {
            if let Some(token) = &lease_shutdown_token {
                token.cancel();
            }
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "verify sealed cluster catalog before readiness announcement: {error}"
            )));
        }
    }
    if !process_lease.is_live() {
        if let Some(token) = &lease_shutdown_token {
            token.cancel();
        }
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(
            "stable node identity lease was lost before announcing cluster readiness".into(),
        ));
    }

    // The pipeline and shuffle receiver are ready, but source intake remains fenced. Publish
    // readiness now so every owner can certify the same assignment before any node emits.
    let mut active = local_node.clone();
    active.state = NodeState::Active;
    if let Err(error) = discovery.announce(active.clone()).await {
        if let Some(token) = &lease_shutdown_token {
            token.cancel();
        }
        let _ = db.shutdown().await;
        let _ = discovery.stop().await;
        return Err(ClusterStartupError::EngineConstruction(format!(
            "announce cluster runtime readiness: {error}"
        )));
    }
    if let Some(ref controller) = cluster_controller {
        controller.set_active(true);
    }

    // Rebalance and assignment certification use the same durable snapshot in gossip and static
    // discovery modes.
    let rebalance_shutdown = Arc::new(tokio::sync::Notify::new());
    let mut rebalance_tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    if let (Some(snap_store), Some(controller)) =
        (snapshot_store.clone(), cluster_controller.as_ref())
    {
        rebalance_tasks.push(laminar_db::rebalance::spawn_snapshot_watcher(
            Arc::clone(&db),
            Arc::clone(&snap_store),
            Arc::clone(&vnode_registry),
            Arc::clone(&rebalance_shutdown),
            rebalance_config,
            Some(Arc::clone(controller)),
        ));
        rebalance_tasks.push(laminar_db::rebalance::spawn_rebalance_controller(
            Arc::clone(&db),
            Arc::clone(controller),
            snap_store,
            Arc::clone(&vnode_registry),
            Arc::clone(&rebalance_shutdown),
            rebalance_config,
        ));
        info!("Rebalance control plane started");
    }

    let startup_controller = cluster_controller
        .as_ref()
        .expect("cluster controller was required before database construction");
    let startup_gate = async {
        wait_for_startup_assignment_fence(startup_controller, &vnode_registry).await?;
        let authority_deadline = tokio::time::Instant::now() + rebalance_config.checkpoint_timeout;
        db.finish_cluster_startup(authority_deadline)
            .await
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "cluster startup recovery fence: {error}"
                ))
            })
    }
    .await;
    let intake_open = match startup_gate {
        Ok(open) => open,
        Err(error) => {
            startup_controller.set_active(false);
            let mut left = active.clone();
            left.state = NodeState::Left;
            let _ = discovery.announce(left).await;
            rebalance_shutdown.notify_waiters();
            for task in &rebalance_tasks {
                task.abort();
            }
            for task in rebalance_tasks.drain(..) {
                let _ = task.await;
            }
            let _ = db.shutdown().await;
            if let Some(token) = &lease_shutdown_token {
                token.cancel();
            }
            let _ = discovery.stop().await;
            return Err(error);
        }
    };
    if intake_open {
        info!("Cluster assignment certified; source intake opened");
    } else {
        info!("Cluster source intake remains fenced for coordinated recovery");
        if tokio::time::timeout(STARTUP_RECOVERY_TIMEOUT, async {
            while db.cluster_intake_fenced() || startup_controller.is_recovering() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .is_err()
        {
            startup_controller.set_active(false);
            let mut left = active.clone();
            left.state = NodeState::Left;
            let _ = discovery.announce(left).await;
            rebalance_shutdown.notify_waiters();
            for task in &rebalance_tasks {
                task.abort();
            }
            for task in rebalance_tasks.drain(..) {
                let _ = task.await;
            }
            if let Some(token) = &lease_shutdown_token {
                token.cancel();
            }
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::EngineConstruction(format!(
                "coordinated startup recovery did not release intake within {STARTUP_RECOVERY_TIMEOUT:?}"
            )));
        }
    }

    // Bind the data/control API only after the exact assignment and any required rewind have
    // released intake. This prevents readiness, queries, reloads, and subscriptions from exposing
    // restored-but-not-yet-coordinated state.
    let cluster_components = crate::http::ClusterComponents {
        controller: cluster_controller.clone(),
        snapshot_store: snapshot_store.clone(),
        membership_rx: discovery.membership_watch(),
    };
    let api_start = server::start_http_api(
        Arc::clone(&db),
        registry,
        config_path.clone(),
        config,
        Some(cluster_components),
    )
    .await;
    let (app_state, api_handle) = match api_start {
        Ok(started) => started,
        Err(error) => {
            startup_controller.set_active(false);
            let mut left = active.clone();
            left.state = NodeState::Left;
            let _ = discovery.announce(left).await;
            rebalance_shutdown.notify_waiters();
            for task in &rebalance_tasks {
                task.abort();
            }
            for task in rebalance_tasks.drain(..) {
                let _ = task.await;
            }
            if let Some(token) = &lease_shutdown_token {
                token.cancel();
            }
            let _ = db.shutdown().await;
            let _ = discovery.stop().await;
            return Err(ClusterStartupError::HttpStartup(error.to_string()));
        }
    };
    let watcher_handle = server::spawn_config_watcher(&app_state, config_path);
    let membership_rx = discovery.membership_watch();
    let membership_handle = spawn_membership_watcher(&node_id_str, membership_rx);
    info!("Membership watcher started");

    info!("Cluster node '{node_id_str}' started");

    Ok(ClusterHandle {
        db,
        discovery,
        api_handle,
        watcher_handle,
        membership_handle,
        local_node: active,
        cluster_controller,
        snapshot_store,
        vnode_count: vnode_registry.vnode_count(),
        lease_shutdown_token,
        _process_lease: process_lease,
        rebalance_tasks,
        rebalance_shutdown,
    })
}

fn cluster_state_backend(
    store: Arc<dyn object_store::ObjectStore>,
    node_id: NodeId,
    vnode_capacity: u32,
) -> Arc<dyn laminar_core::state::StateBackend> {
    Arc::new(laminar_core::state::ObjectStoreBackend::cluster_shared(
        store,
        node_id.to_string(),
        vnode_capacity,
    ))
}

const STARTUP_ASSIGNMENT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);
const STARTUP_RECOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

async fn wait_for_startup_assignment_fence(
    controller: &laminar_core::cluster::control::ClusterController,
    registry: &laminar_core::state::VnodeRegistry,
) -> Result<(), ClusterStartupError> {
    let mut fence_rx = controller.checkpoint_assignment_watch();
    let wait = async {
        loop {
            let assignment = registry.versioned_snapshot();
            let version = assignment.version();
            let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
            if controller
                .checkpoint_assignment_fence(version)
                .is_some_and(|fence| fence.matches_owner_map(&owners))
            {
                return Ok(());
            }
            fence_rx.changed().await.map_err(|_| {
                ClusterStartupError::EngineConstruction(
                    "cluster assignment certification channel closed during startup".into(),
                )
            })?;
        }
    };
    tokio::time::timeout(STARTUP_ASSIGNMENT_TIMEOUT, wait)
        .await
        .map_err(|_| {
            ClusterStartupError::EngineConstruction(format!(
                "cluster assignment {} was not certified within {STARTUP_ASSIGNMENT_TIMEOUT:?}",
                registry.assignment_version()
            ))
        })?
}

/// Stable numeric identity shared by cluster runtime and offline checkpoint validation.
pub(crate) fn numeric_node_id(node_id: &str) -> u64 {
    // xxhash3 is deterministic across Rust/compiler versions. Avoid the UNASSIGNED sentinel.
    let hash = xxhash_rust::xxh3::xxh3_64(node_id.as_bytes());
    if hash == 0 {
        1
    } else {
        hash
    }
}

fn num_cpus() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// Boot-time vnode assignment. If an `AssignmentSnapshot` exists in
/// shared storage (written by a prior cluster incarnation or a peer
/// that raced here first), every node adopts it — the fresh node
/// doesn't fight over vnodes that are already claimed. Otherwise we
/// compute a round-robin split of this node's known peers and
/// CAS-create the snapshot; losers of the CAS race re-load and adopt.
///
/// Returns the registry plus the snapshot store (when one is
/// available) so the `ClusterController` can watch for future
/// rotations. `None` store means the deployment is on a non-object-
/// store state backend (in-process), where no snapshot is possible
/// or needed.
async fn assignment_seed_participants(
    self_id: laminar_core::state::NodeId,
    self_incarnation: uuid::Uuid,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    store: &Arc<dyn object_store::ObjectStore>,
    process_lease_ttl_ms: i64,
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    use laminar_core::cluster::control::ProcessLeaseStore;

    let advertised = advertised_startup_participants(self_id, self_incarnation, peers)?;
    let mut participants = Vec::with_capacity(advertised.len());
    for participant in advertised {
        let node = NodeId(participant.node_id);
        let boot_incarnation = participant.boot_incarnation;
        let lease = ProcessLeaseStore::new(Arc::clone(store), node, process_lease_ttl_ms)
            .load()
            .await
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "load process lease for node {}: {error}",
                    node.0
                ))
            })?
            .ok_or_else(|| {
                ClusterStartupError::EngineConstruction(format!(
                    "node {} has no durable process lease",
                    node.0
                ))
            })?;
        if lease.owner != boot_incarnation {
            return Err(ClusterStartupError::EngineConstruction(format!(
                "node {} advertised process {} but durable lease belongs to {}",
                node.0, boot_incarnation, lease.owner
            )));
        }
        participants.push(participant);
    }
    Ok(participants)
}

fn advertised_startup_participants(
    self_id: laminar_core::state::NodeId,
    self_incarnation: uuid::Uuid,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
) -> Result<Vec<laminar_core::checkpoint::CheckpointParticipant>, ClusterStartupError> {
    let mut advertised = Vec::with_capacity(peers.len() + 1);
    advertised.push((self_id, self_incarnation));
    for peer in peers {
        let incarnation = peer
            .metadata
            .tags
            .get(PROCESS_INCARNATION_TAG)
            .ok_or_else(|| {
                ClusterStartupError::EngineConstruction(format!(
                    "peer {} did not advertise its process incarnation",
                    peer.id.0
                ))
            })?
            .parse()
            .map_err(|error| {
                ClusterStartupError::EngineConstruction(format!(
                    "peer {} advertised an invalid process incarnation: {error}",
                    peer.id.0
                ))
            })?;
        advertised.push((peer.id, incarnation));
    }
    advertised.sort_unstable_by_key(|(node, _)| node.0);
    if advertised.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err(ClusterStartupError::EngineConstruction(
            "initial assignment participant roster contains duplicate node ids".into(),
        ));
    }
    if advertised.len() > laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS {
        return Err(ClusterStartupError::EngineConstruction(format!(
            "startup roster has {} participants; maximum is {}",
            advertised.len(),
            laminar_core::checkpoint::MAX_CHECKPOINT_PARTICIPANTS
        )));
    }
    Ok(advertised
        .into_iter()
        .map(
            |(node, boot_incarnation)| laminar_core::checkpoint::CheckpointParticipant {
                node_id: node.0,
                boot_incarnation,
            },
        )
        .collect())
}

async fn startup_committed_assignment(
    store: &laminar_core::cluster::control::AssignmentSnapshotStore,
    controller: Option<&laminar_core::cluster::control::ClusterController>,
    head: laminar_core::cluster::control::AssignmentSnapshot,
) -> Result<laminar_core::cluster::control::AssignmentSnapshot, ClusterStartupError> {
    laminar_db::rebalance::audit_assignment_snapshot_authority(store, controller, &head)
        .await
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "audit startup assignment {} authority: {error}",
                head.version
            ))
        })?;
    if !head.draining {
        return Ok(head);
    }
    let prior_version = head.version.checked_sub(1).ok_or_else(|| {
        ClusterStartupError::EngineConstruction(
            "draining assignment has no retained committed predecessor".into(),
        )
    })?;
    let prior = store
        .load_version(prior_version)
        .await
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "load retained assignment {prior_version} before draining head {}: {error}",
                head.version
            ))
        })?
        .ok_or_else(|| {
            ClusterStartupError::EngineConstruction(format!(
                "draining assignment {} has no retained committed predecessor {prior_version}",
                head.version
            ))
        })?;
    if prior.draining {
        return Err(ClusterStartupError::EngineConstruction(format!(
            "draining assignment {} has a draining predecessor {prior_version}",
            head.version
        )));
    }
    laminar_db::rebalance::audit_assignment_snapshot_authority(store, controller, &prior)
        .await
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!(
                "audit retained assignment {prior_version} authority: {error}"
            ))
        })?;
    Ok(prior)
}

async fn resolve_vnode_assignment(
    self_id: laminar_core::cluster::discovery::NodeId,
    peers: &[laminar_core::cluster::discovery::NodeInfo],
    vnode_count: u32,
    control_store: Option<Arc<dyn object_store::ObjectStore>>,
    startup_participants: &[laminar_core::checkpoint::CheckpointParticipant],
) -> Result<
    (
        Arc<laminar_core::state::VnodeRegistry>,
        Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    ),
    ClusterStartupError,
> {
    use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};
    use laminar_core::state::{rendezvous_assignment, NodeId, VnodeRegistry};

    let peer_ids: Vec<NodeId> = peers
        .iter()
        .map(|p| NodeId(p.id.0))
        .chain(std::iter::once(NodeId(self_id.0)))
        .collect();
    let assignment: Arc<[NodeId]> = rendezvous_assignment(vnode_count, &peer_ids);

    let Some(store) = control_store else {
        // No shared store — fall back to node-local round-robin.
        let registry = VnodeRegistry::new(vnode_count);
        registry.set_assignment(Arc::clone(&assignment));
        return Ok((Arc::new(registry), None));
    };
    let snapshot_store = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&store)));

    // Snapshot exists → restart or joiner. Boot owning nothing: the stored snapshot may be
    // stale (a shed can race the restart), and acting on assumed ownership bypasses the adopt
    // protocol. `start_cluster` explicitly adopts the stored snapshot after `db.start()`.
    if let Some(existing) = snapshot_store
        .load()
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot load: {e}")))?
    {
        existing.to_vnode_vec(vnode_count).map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("stored assignment: {error}"))
        })?;
        let registry = VnodeRegistry::new_unassigned(vnode_count);
        info!(
            stored_version = existing.version,
            "found stored assignment snapshot; booting unassigned — adopt runs after start"
        );
        return Ok((Arc::new(registry), Some(snapshot_store)));
    }

    // Nothing stored yet — propose ours and CAS-create. A racing peer
    // may win; if so, re-load and adopt the winner.
    let proposal = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&assignment),
            startup_participants.to_vec(),
        )
        .map_err(|error| {
            ClusterStartupError::EngineConstruction(format!("initial assignment snapshot: {error}"))
        })?;
    let winner = match snapshot_store
        .save_if_absent(&proposal)
        .await
        .map_err(|e| ClusterStartupError::EngineConstruction(format!("snapshot save: {e}")))?
    {
        Some(w) => {
            info!("Created assignment snapshot v{}", w.version);
            w
        }
        None => {
            let w = snapshot_store
                .load()
                .await
                .map_err(|e| {
                    ClusterStartupError::EngineConstruction(format!("snapshot re-load: {e}"))
                })?
                .ok_or_else(|| {
                    ClusterStartupError::EngineConstruction(
                        "snapshot CAS lost but re-load returned None".into(),
                    )
                })?;
            info!("Adopted snapshot v{} after CAS race", w.version);
            w
        }
    };
    let registry = VnodeRegistry::new_unassigned(vnode_count);
    let winning_assignment = winner.to_vnode_vec(vnode_count).map_err(|error| {
        ClusterStartupError::EngineConstruction(format!("winning assignment: {error}"))
    })?;
    registry.set_assignment_and_version(winning_assignment.into(), winner.version);
    Ok((Arc::new(registry), Some(snapshot_store)))
}

/// Build the shared, cluster-wide control-plane object store (assignment
/// snapshot + `ObjectStoreClusterKv`). It must be reachable by every node, so
/// it comes from the checkpoint bucket — not the per-node `[state]` path. Falls
/// back to the state backend's store for single-host/local setups.
fn build_control_store(
    config: &ServerConfig,
) -> Result<Option<Arc<dyn object_store::ObjectStore>>, ClusterStartupError> {
    if !config.checkpoint.url.is_empty() {
        let store = laminar_core::storage::object_store_builder::build_object_store(
            &config.checkpoint.url,
            &config.checkpoint.storage,
        )
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("control-plane object store: {e}"))
        })?;
        return Ok(Some(store));
    }
    config.state.build_object_store().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("control-plane object store: {e}"))
    })
}

/// Build a `ClusterController` from a ready KV handle and start its barrier sync
/// server. Shared by the gossip and static discovery paths.
async fn install_cluster_controller(
    node_id: NodeId,
    kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    recovery_kv: Arc<dyn laminar_core::cluster::control::ClusterKv>,
    snapshot_store: Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    members_rx: watch::Receiver<Vec<NodeInfo>>,
    bind_host: &str,
    advertise_host: &str,
    recovery_incarnation: uuid::Uuid,
) -> Result<Arc<laminar_core::cluster::control::ClusterController>, ClusterStartupError> {
    use laminar_core::cluster::control::ClusterController;

    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node_id,
        kv,
        recovery_kv,
        snapshot_store,
        members_rx,
        recovery_incarnation,
    ));
    controller.set_active(false);

    let bind: std::net::SocketAddr = format!("{bind_host}:0").parse().map_err(|e| {
        ClusterStartupError::EngineConstruction(format!("invalid barrier sync bind host: {e}"))
    })?;
    let bound = controller
        .start_barrier_server(bind, Some(advertise_host.to_string()))
        .await
        .map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("barrier sync server bind: {e}"))
        })?;
    info!("Barrier sync gRPC server listening on {bound}");
    info!(
        "ClusterController installed (leader={})",
        controller.is_leader()
    );
    Ok(controller)
}

struct StaticClusterKv {
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
}

impl StaticClusterKv {
    fn new(membership_rx: watch::Receiver<Vec<NodeInfo>>) -> Self {
        Self { membership_rx }
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for StaticClusterKv {
    async fn write(&self, _key: &str, _value: String) {}

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        let peers = self.membership_rx.borrow();
        let peer = peers.iter().find(|p| p.id == who)?;
        peer.metadata.tags.get(key).cloned()
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        let peers = self.membership_rx.borrow();
        peers
            .iter()
            .filter_map(|p| p.metadata.tags.get(key).map(|v| (p.id, v.clone())))
            .collect()
    }
}

/// Load control-plane mTLS material and install it process-wide before any
/// server/client binds. No-op when unconfigured; validation guarantees the
/// fields are all-or-nothing.
fn install_cluster_tls(d: &DiscoverySection) -> Result<(), ClusterStartupError> {
    let (Some(cert), Some(key), Some(ca), Some(name)) = (
        &d.cluster_tls_cert,
        &d.cluster_tls_key,
        &d.cluster_tls_client_ca,
        &d.cluster_tls_server_name,
    ) else {
        return Ok(());
    };
    let read = |p: &std::path::Path| {
        std::fs::read(p).map_err(|e| {
            ClusterStartupError::EngineConstruction(format!("read {}: {e}", p.display()))
        })
    };
    let tls = laminar_core::cluster::control::ClusterTls::from_pem(
        &read(cert)?,
        &read(key)?,
        &read(ca)?,
        name,
    );
    laminar_core::cluster::control::set_cluster_tls(tls);
    info!("cluster control-plane mTLS enabled (server_name={name})");
    Ok(())
}

/// Build an outbound shuffle sender. When gossip discovery is active,
/// publish `advertise_addr` under `SHUFFLE_ADDR_KEY` so peers find us, and
/// give the sender a KV handle for reverse lookup. Static discovery
/// uses `StaticClusterKv` to query peer shuffle addresses from TCP heartbeat metadata.
async fn build_shuffle_sender(
    node_id: u64,
    discovery: &DiscoveryImpl,
    advertise_addr: String,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    process_incarnation: uuid::Uuid,
) -> Arc<laminar_core::shuffle::ShuffleSender> {
    use laminar_core::cluster::control::{ChitchatKv, ClusterKv};
    use laminar_core::shuffle::{ShuffleSender, SHUFFLE_ADDR_KEY};

    let sender = match discovery {
        DiscoveryImpl::Gossip(gossip) => {
            if let Some(handle) = gossip.chitchat_handle() {
                let kv: Arc<dyn ClusterKv> = Arc::new(ChitchatKv::from_handle(handle));
                kv.write(SHUFFLE_ADDR_KEY, advertise_addr).await;
                ShuffleSender::with_kv(node_id, kv, process_incarnation)
            } else {
                ShuffleSender::new(node_id, process_incarnation)
            }
        }
        DiscoveryImpl::Static(_) => {
            let kv: Arc<dyn ClusterKv> = Arc::new(StaticClusterKv::new(membership_rx.clone()));
            ShuffleSender::with_kv(node_id, kv, process_incarnation)
        }
    };
    let sender = Arc::new(sender);

    // Static advertises shuffle addrs in heartbeat metadata; register peers as they appear.
    if matches!(discovery, DiscoveryImpl::Static(_)) {
        let sender_clone = Arc::clone(&sender);
        let mut rx = membership_rx;
        tokio::spawn(async move {
            loop {
                let members = rx.borrow().clone();
                for node in members {
                    if node.id.0 != node_id {
                        if let Some(addr) = node
                            .metadata
                            .tags
                            .get(SHUFFLE_ADDR_KEY)
                            .and_then(|a| a.parse::<std::net::SocketAddr>().ok())
                        {
                            sender_clone.register_peer(node.id.0, addr).await;
                        }
                    }
                }
                if rx.changed().await.is_err() {
                    break;
                }
            }
        });
    }

    sender
}

/// Compute the address peers should use to reach our `ShuffleReceiver`.
///
/// The receiver binds to `0.0.0.0:0` (any interface, ephemeral port), so
/// `local_addr.ip()` is the wildcard — publishing it unchanged leaves remote
/// senders unable to connect. Use the configured advertise host (matching the
/// HTTP/barrier endpoints for NAT/container deployments), falling back to
/// `gethostname` when it is itself a wildcard, keeping the actual bound port.
fn shuffle_advertise_addr(local_addr: std::net::SocketAddr, advertise_host: &str) -> String {
    let port = local_addr.port();
    let host = advertise_host.trim_start_matches('[').trim_end_matches(']');
    let ip_wildcard = host == "0.0.0.0" || host == "::" || host.is_empty();
    if !ip_wildcard {
        return format!("{advertise_host}:{port}");
    }
    let hostname = gethostname::gethostname();
    let hostname = hostname.to_string_lossy();
    if hostname.is_empty() {
        local_addr.to_string()
    } else {
        format!("{hostname}:{port}")
    }
}

const OBJECT_STORE_CONTROL_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const OBJECT_STORE_CONTROL_MAX_VALUE_BYTES: u64 = 1024 * 1024;
const OBJECT_STORE_CONTROL_MAX_KEY_BYTES: usize = 1024;
const OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES: u64 =
    OBJECT_STORE_CONTROL_MAX_VALUE_BYTES * 6 + 16 * 1024;
const OBJECT_STORE_CONTROL_VERSION: u8 = 2;
const OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN: usize = 2;
const OBJECT_STORE_CONTROL_MAX_LIST_RECORDS: usize = 4096;
const OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS: usize = 4;
const OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS: usize = 256;
const OBJECT_STORE_CONTROL_MAX_PRUNE_BATCHES: usize = 4;
const RECOVERY_GENERATION_KEY: &str = "control:recovery-gen";

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct ObjectStoreControlRecord {
    version: u8,
    node: u64,
    owner: uuid::Uuid,
    term: u64,
    sequence: u64,
    key: String,
    value: String,
}

impl ObjectStoreControlRecord {
    fn validate(
        &self,
        lease: &laminar_core::cluster::control::ProcessLease,
        key: &str,
        sequence: u64,
    ) -> Result<(), String> {
        if self.version != OBJECT_STORE_CONTROL_VERSION
            || self.node != lease.node.0
            || self.owner != lease.owner
            || self.term != lease.term
            || sequence == 0
            || self.sequence != sequence
            || self.key != key
            || u64::try_from(self.value.len()).unwrap_or(u64::MAX)
                > OBJECT_STORE_CONTROL_MAX_VALUE_BYTES
        {
            return Err("control record does not match its durable path and process lease".into());
        }
        Ok(())
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RecoveryGenerationRecord {
    version: u8,
    generation: u64,
    writer_node: u64,
    writer_owner: uuid::Uuid,
    writer_term: u64,
}

fn object_store_control_key_digest(key: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(key.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn object_store_control_key_prefix(
    lease: &laminar_core::cluster::control::ProcessLease,
    key: &str,
) -> String {
    format!(
        "cluster-control-kv/v{OBJECT_STORE_CONTROL_VERSION}/node={}/term={:020}/owner={}/key={}/",
        lease.node.0,
        lease.term,
        lease.owner,
        object_store_control_key_digest(key)
    )
}

fn object_store_control_record_path(
    lease: &laminar_core::cluster::control::ProcessLease,
    key: &str,
    sequence: u64,
) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{}v{sequence:020}.json",
        object_store_control_key_prefix(lease, key)
    ))
}

const RECOVERY_GENERATION_PREFIX: &str = "cluster-control-kv/v2/recovery-generation/";

fn recovery_generation_path(generation: u64) -> object_store::path::Path {
    object_store::path::Path::from(format!(
        "{RECOVERY_GENERATION_PREFIX}v{generation:020}.json"
    ))
}

fn sequence_from_path(prefix: &str, path: &object_store::path::Path) -> Result<u64, String> {
    let raw = path
        .as_ref()
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('v'))
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .ok_or_else(|| format!("invalid control record path {path}"))?;
    if raw.len() != 20 || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("invalid control record sequence in {path}"));
    }
    raw.parse::<u64>()
        .map_err(|error| format!("invalid control record sequence in {path}: {error}"))
}

fn retain_oldest_control_record(
    oldest: &mut BinaryHeap<(u64, String)>,
    sequence: u64,
    path: &object_store::path::Path,
) {
    let candidate = (sequence, path.to_string());
    if oldest.len() < OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS {
        oldest.push(candidate);
    } else if oldest.peek().is_some_and(|largest| &candidate < largest) {
        oldest.pop();
        oldest.push(candidate);
    }
}

async fn list_control_sequences(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<Vec<u64>, String> {
    use futures::StreamExt;

    let prefix_path = object_store::path::Path::from(prefix);
    let mut entries = store.list(Some(&prefix_path));
    let mut sequences = Vec::new();
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| error.to_string())?;
        if sequences.len() == OBJECT_STORE_CONTROL_MAX_LIST_RECORDS {
            return Err(format!(
                "control history exceeds the fixed {OBJECT_STORE_CONTROL_MAX_LIST_RECORDS}-record scan bound"
            ));
        }
        sequences.push(sequence_from_path(prefix, &entry.location)?);
    }
    sequences.sort_unstable();
    sequences.dedup();
    Ok(sequences)
}

async fn prune_control_history_batch(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &str,
) -> Result<bool, String> {
    use futures::StreamExt;

    let prefix_path = object_store::path::Path::from(prefix);
    let mut entries = store.list(Some(&prefix_path));
    let mut oldest = BinaryHeap::with_capacity(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS);
    let mut total = 0usize;
    while let Some(entry) = entries.next().await {
        let entry = entry.map_err(|error| error.to_string())?;
        let sequence = sequence_from_path(prefix, &entry.location)?;
        total = total.saturating_add(1);
        retain_oldest_control_record(&mut oldest, sequence, &entry.location);
    }
    let delete_count = total
        .saturating_sub(OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN)
        .min(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS);
    let deletions = futures::stream::iter(
        oldest
            .into_sorted_vec()
            .into_iter()
            .take(delete_count)
            .map(|(_, path)| Ok::<_, object_store::Error>(object_store::path::Path::from(path))),
    )
    .boxed();
    let mut results = store.delete_stream(deletions);
    while let Some(result) = results.next().await {
        if let Err(error) = result {
            if !matches!(error, object_store::Error::NotFound { .. }) {
                return Err(error.to_string());
            }
        }
    }
    Ok(total.saturating_sub(delete_count) <= OBJECT_STORE_CONTROL_HISTORY_TO_RETAIN)
}

struct ObjectStoreClusterKv {
    local_id: NodeId,
    local_lease: laminar_core::cluster::control::ProcessLease,
    local_lease_deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
    process_lease_ttl_ms: i64,
    store: Arc<dyn object_store::ObjectStore>,
    membership_rx: watch::Receiver<Vec<NodeInfo>>,
    sequence_states:
        std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<Option<u64>>>>>,
    prune_states: Arc<std::sync::Mutex<std::collections::HashMap<String, bool>>>,
}

impl ObjectStoreClusterKv {
    fn new(
        local_lease: laminar_core::cluster::control::ProcessLease,
        local_lease_deadline: Arc<laminar_core::cluster::control::LeaseDeadline>,
        process_lease_ttl_ms: i64,
        store: Arc<dyn object_store::ObjectStore>,
        membership_rx: watch::Receiver<Vec<NodeInfo>>,
    ) -> Self {
        debug_assert!(!local_lease.node.is_unassigned());
        debug_assert!(!local_lease.owner.is_nil());
        debug_assert!(local_lease.term > 0);
        debug_assert!(process_lease_ttl_ms > 0);
        Self {
            local_id: local_lease.node,
            local_lease,
            local_lease_deadline,
            process_lease_ttl_ms,
            store,
            membership_rx,
            sequence_states: std::sync::Mutex::new(std::collections::HashMap::new()),
            prune_states: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    fn visible_ids(&self) -> Vec<NodeId> {
        let mut ids: Vec<NodeId> = {
            let members = self.membership_rx.borrow();
            members.iter().map(|member| member.id).collect()
        };
        ids.push(self.local_id);
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    fn validate_key_and_value(key: &str, value: &str) -> Result<(), String> {
        if key.is_empty() || key.len() > OBJECT_STORE_CONTROL_MAX_KEY_BYTES {
            return Err(format!(
                "control key is {} bytes; expected 1..={OBJECT_STORE_CONTROL_MAX_KEY_BYTES}",
                key.len()
            ));
        }
        // An empty value is intentional: the controller uses it as the durable clear/tombstone
        // signal for a completed recovery round.
        if u64::try_from(value.len()).unwrap_or(u64::MAX) > OBJECT_STORE_CONTROL_MAX_VALUE_BYTES {
            return Err(format!(
                "control value is {} bytes; maximum is {OBJECT_STORE_CONTROL_MAX_VALUE_BYTES}",
                value.len()
            ));
        }
        Ok(())
    }

    async fn load_process_lease(
        &self,
        node: NodeId,
    ) -> Result<Option<laminar_core::cluster::control::ProcessLease>, String> {
        laminar_core::cluster::control::ProcessLeaseStore::new(
            Arc::clone(&self.store),
            node,
            self.process_lease_ttl_ms,
        )
        .load()
        .await
        .map_err(|error| error.to_string())
    }

    fn same_process_term(
        left: &laminar_core::cluster::control::ProcessLease,
        right: &laminar_core::cluster::control::ProcessLease,
    ) -> bool {
        left.node == right.node && left.owner == right.owner && left.term == right.term
    }

    fn require_live_local_deadline(&self) -> Result<(), String> {
        if self.local_lease_deadline.is_live() {
            Ok(())
        } else {
            Err("local process lease deadline expired".into())
        }
    }

    async fn require_local_process_term(&self) -> Result<(), String> {
        self.require_live_local_deadline()?;
        let current = self
            .load_process_lease(self.local_id)
            .await?
            .ok_or_else(|| "local process lease is absent".to_string())?;
        if !Self::same_process_term(&current, &self.local_lease) {
            return Err("local process lease owner or term changed".into());
        }
        self.require_live_local_deadline()
    }

    fn sequence_state(&self, prefix: &str) -> Arc<tokio::sync::Mutex<Option<u64>>> {
        let mut states = self
            .sequence_states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            states
                .entry(prefix.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None))),
        )
    }

    async fn put_create(
        &self,
        path: &object_store::path::Path,
        bytes: Vec<u8>,
    ) -> Result<bool, String> {
        let options = object_store::PutOptions {
            mode: object_store::PutMode::Create,
            ..object_store::PutOptions::default()
        };
        match self
            .store
            .put_opts(
                path,
                object_store::PutPayload::from(bytes::Bytes::from(bytes)),
                options,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    fn schedule_prune(&self, prefix: String) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        {
            let mut states = self
                .prune_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(pending) = states.get_mut(&prefix) {
                *pending = true;
                return;
            }
            states.insert(prefix.clone(), false);
        }
        let store = Arc::clone(&self.store);
        let prune_states = Arc::clone(&self.prune_states);
        runtime.spawn(async move {
            loop {
                let prune = async {
                    for _ in 0..OBJECT_STORE_CONTROL_MAX_PRUNE_BATCHES {
                        match tokio::time::timeout(
                            OBJECT_STORE_CONTROL_IO_TIMEOUT,
                            prune_control_history_batch(&store, &prefix),
                        )
                        .await
                        {
                            Ok(Ok(true)) => return Ok(()),
                            Ok(Ok(false)) => tokio::task::yield_now().await,
                            Ok(Err(error)) => return Err(error),
                            Err(_) => {
                                return Err(format!(
                                    "prune timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
                                ));
                            }
                        }
                    }
                    Err("history still exceeds the bounded prune budget".to_string())
                }
                .await;
                if let Err(error) = prune {
                    warn!(%prefix, %error, "object-store control history prune failed");
                }

                let rerun = {
                    let mut states = prune_states
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match states.get_mut(&prefix) {
                        Some(pending) if *pending => {
                            *pending = false;
                            true
                        }
                        Some(_) => {
                            states.remove(&prefix);
                            false
                        }
                        None => false,
                    }
                };
                if !rerun {
                    return;
                }
                tokio::task::yield_now().await;
            }
        });
    }

    async fn read_control_record(
        &self,
        lease: &laminar_core::cluster::control::ProcessLease,
        key: &str,
        sequence: u64,
    ) -> Result<ObjectStoreControlRecord, String> {
        let path = object_store_control_record_path(lease, key, sequence);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| error.to_string())?;
        if result.meta.size > OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES {
            return Err(format!(
                "control envelope is {} bytes; maximum is {OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES}",
                result.meta.size
            ));
        }
        let bytes = result.bytes().await.map_err(|error| error.to_string())?;
        let record: ObjectStoreControlRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        record.validate(lease, key, sequence)?;
        let canonical = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err("control record body is not canonically encoded".into());
        }
        Ok(record)
    }

    async fn write_control_value(&self, key: &str, value: String) -> Result<(), String> {
        Self::validate_key_and_value(key, &value)?;
        self.require_local_process_term().await?;
        let prefix = object_store_control_key_prefix(&self.local_lease, key);
        let state = self.sequence_state(&prefix);
        let mut durable_sequence = state.lock().await;
        if durable_sequence.is_none() {
            let sequences = match list_control_sequences(&self.store, &prefix).await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_prune(prefix.clone());
                    return Err(error);
                }
            };
            *durable_sequence = Some(sequences.last().copied().unwrap_or(0));
        }

        for _ in 0..OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS {
            let sequence = durable_sequence
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| "control record sequence exhausted".to_string())?;
            *durable_sequence = Some(sequence);
            let record = ObjectStoreControlRecord {
                version: OBJECT_STORE_CONTROL_VERSION,
                node: self.local_id.0,
                owner: self.local_lease.owner,
                term: self.local_lease.term,
                sequence,
                key: key.to_string(),
                value: value.clone(),
            };
            let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
            if u64::try_from(encoded.len()).unwrap_or(u64::MAX)
                > OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES
            {
                return Err("control envelope exceeds its canonical size bound".into());
            }
            let path = object_store_control_record_path(&self.local_lease, key, sequence);
            if self.put_create(&path, encoded).await? {
                drop(durable_sequence);
                self.schedule_prune(prefix);
                return self.require_local_process_term().await;
            }

            let existing = self
                .read_control_record(&self.local_lease, key, sequence)
                .await?;
            if existing.value == value {
                drop(durable_sequence);
                self.schedule_prune(prefix);
                return self.require_local_process_term().await;
            }
            let sequences = match list_control_sequences(&self.store, &prefix).await {
                Ok(sequences) => sequences,
                Err(error) => {
                    self.schedule_prune(prefix.clone());
                    return Err(error);
                }
            };
            let head = sequences.last().copied().unwrap_or(sequence);
            *durable_sequence = Some((*durable_sequence).unwrap_or(0).max(head));
        }
        self.schedule_prune(prefix);
        Err(format!(
            "control record create conflicted {OBJECT_STORE_CONTROL_MAX_WRITE_ATTEMPTS} times"
        ))
    }

    async fn read_control_value(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        Self::validate_key_and_value(key, "")?;
        let Some(before) = self.load_process_lease(who).await? else {
            return Ok(None);
        };
        let prefix = object_store_control_key_prefix(&before, key);
        let sequences = match list_control_sequences(&self.store, &prefix).await {
            Ok(sequences) => sequences,
            Err(error) => {
                self.schedule_prune(prefix);
                return Err(error);
            }
        };
        let value = if let Some(sequence) = sequences.last().copied() {
            Some(
                self.read_control_record(&before, key, sequence)
                    .await?
                    .value,
            )
        } else {
            None
        };
        let after = self
            .load_process_lease(who)
            .await?
            .ok_or_else(|| "process lease vanished during control read".to_string())?;
        if !Self::same_process_term(&before, &after) {
            return Err("process lease owner or term changed during control read".into());
        }
        if !sequences.is_empty() {
            self.schedule_prune(prefix);
        }
        Ok(value)
    }

    async fn read_target_value(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        if key == RECOVERY_GENERATION_KEY {
            // Recovery generation is the one cluster-global exception: keeping it outside a
            // process term prevents a full-cluster restart from resetting the recovery epoch.
            self.load_recovery_generation()
                .await
                .map(|generation| generation.map(|value| value.to_string()))
        } else {
            self.read_control_value(who, key).await
        }
    }

    async fn load_recovery_generation(&self) -> Result<Option<u64>, String> {
        let sequences = match list_control_sequences(&self.store, RECOVERY_GENERATION_PREFIX).await
        {
            Ok(sequences) => sequences,
            Err(error) => {
                self.schedule_prune(RECOVERY_GENERATION_PREFIX.to_string());
                return Err(error);
            }
        };
        let Some(generation) = sequences.last().copied() else {
            return Ok(None);
        };
        if generation == 0 {
            return Err("recovery generation record cannot be zero".into());
        }
        let path = recovery_generation_path(generation);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|error| error.to_string())?;
        if result.meta.size > 512 {
            return Err(format!(
                "recovery generation envelope is {} bytes; maximum is 512",
                result.meta.size
            ));
        }
        let bytes = result.bytes().await.map_err(|error| error.to_string())?;
        let record: RecoveryGenerationRecord =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if record.version != OBJECT_STORE_CONTROL_VERSION
            || record.generation != generation
            || record.writer_node == 0
            || record.writer_owner.is_nil()
            || record.writer_term == 0
        {
            return Err("recovery generation record does not match its durable path".into());
        }
        let canonical = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err("recovery generation body is not canonically encoded".into());
        }
        Ok(Some(generation))
    }

    async fn write_recovery_generation(&self, value: String) -> Result<(), String> {
        let generation = value
            .parse::<u64>()
            .map_err(|error| format!("invalid recovery generation: {error}"))?;
        if generation == 0 || value != generation.to_string() {
            return Err("recovery generation must be a canonical nonzero u64".into());
        }
        self.require_local_process_term().await?;
        if let Some(current) = self.load_recovery_generation().await? {
            if current > generation {
                return Err(format!(
                    "recovery generation {generation} regresses durable generation {current}"
                ));
            }
            if current == generation {
                return Ok(());
            }
        }
        let record = RecoveryGenerationRecord {
            version: OBJECT_STORE_CONTROL_VERSION,
            generation,
            writer_node: self.local_id.0,
            writer_owner: self.local_lease.owner,
            writer_term: self.local_lease.term,
        };
        let encoded = serde_json::to_vec(&record).map_err(|error| error.to_string())?;
        let path = recovery_generation_path(generation);
        let created = self.put_create(&path, encoded).await?;
        if !created {
            let observed = self.load_recovery_generation().await?;
            if observed != Some(generation) {
                return Err("recovery generation create conflicted with a newer marker".into());
            }
        }
        self.schedule_prune(RECOVERY_GENERATION_PREFIX.to_string());
        let observed = self.load_recovery_generation().await?;
        if observed != Some(generation) {
            return Err(format!(
                "recovery generation {generation} was superseded during publication"
            ));
        }
        self.require_local_process_term().await
    }
}

#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for ObjectStoreClusterKv {
    async fn write(&self, key: &str, value: String) {
        if let Err(error) = self.write_checked(key, value).await {
            warn!(%error, %key, "object-store control write failed");
        }
    }

    async fn write_checked(&self, key: &str, value: String) -> Result<(), String> {
        let write = async {
            if key == RECOVERY_GENERATION_KEY {
                self.write_recovery_generation(value).await
            } else {
                self.write_control_value(key, value).await
            }
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, write).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "write timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }

    async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
        match self.read_from_checked(who, key).await {
            Ok(value) => value,
            Err(error) => {
                warn!(node = who.0, %key, %error, "object-store control read failed");
                None
            }
        }
    }

    async fn read_from_checked(&self, who: NodeId, key: &str) -> Result<Option<String>, String> {
        let read = async {
            self.require_local_process_term().await?;
            let value = self.read_target_value(who, key).await?;
            self.require_local_process_term().await?;
            Ok(value)
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, read).await {
            Ok(value) => value,
            Err(_) => Err(format!(
                "read timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }

    async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
        match self.scan_checked(key).await {
            Ok(values) => values,
            Err(error) => {
                warn!(%key, %error, "object-store control scan failed");
                Vec::new()
            }
        }
    }

    async fn scan_checked(&self, key: &str) -> Result<Vec<(NodeId, String)>, String> {
        let scan = async {
            self.require_local_process_term().await?;
            if key == RECOVERY_GENERATION_KEY {
                let value = self.read_target_value(self.local_id, key).await?;
                self.require_local_process_term().await?;
                return Ok(value.map_or_else(Vec::new, |value| vec![(self.local_id, value)]));
            }
            let futures = self.visible_ids().into_iter().map(|id| {
                let key = key.to_string();
                async move {
                    let value = self.read_target_value(id, &key).await?;
                    Ok::<_, String>((id, value))
                }
            });
            let joined = futures::future::join_all(futures).await;
            let mut results = Vec::new();
            for result in joined {
                let (id, value) = result?;
                if let Some(value) = value {
                    results.push((id, value));
                }
            }
            self.require_local_process_term().await?;
            Ok(results)
        };
        match tokio::time::timeout(OBJECT_STORE_CONTROL_IO_TIMEOUT, scan).await {
            Ok(result) => result,
            Err(_) => Err(format!(
                "scan timed out after {OBJECT_STORE_CONTROL_IO_TIMEOUT:?}"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DelayedControlPutStore {
        inner: Arc<dyn object_store::ObjectStore>,
        blocked_path: parking_lot::Mutex<Option<object_store::path::Path>>,
        entered: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        completed: Arc<tokio::sync::Semaphore>,
        blocked_get_path: parking_lot::Mutex<Option<object_store::path::Path>>,
        get_entered: tokio::sync::Semaphore,
        get_release: tokio::sync::Semaphore,
    }

    impl DelayedControlPutStore {
        fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
            Self {
                inner,
                blocked_path: parking_lot::Mutex::new(None),
                entered: Arc::new(tokio::sync::Semaphore::new(0)),
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                completed: Arc::new(tokio::sync::Semaphore::new(0)),
                blocked_get_path: parking_lot::Mutex::new(None),
                get_entered: tokio::sync::Semaphore::new(0),
                get_release: tokio::sync::Semaphore::new(0),
            }
        }

        fn block_once(&self, path: object_store::path::Path) {
            *self.blocked_path.lock() = Some(path);
        }

        async fn wait_until_blocked(&self) {
            self.entered.acquire().await.unwrap().forget();
        }

        fn release(&self) {
            self.release.add_permits(1);
        }

        async fn wait_until_completed(&self) {
            self.completed.acquire().await.unwrap().forget();
        }

        fn block_get_once(&self, path: object_store::path::Path) {
            *self.blocked_get_path.lock() = Some(path);
        }

        async fn wait_until_get_blocked(&self) {
            self.get_entered.acquire().await.unwrap().forget();
        }

        fn release_get(&self) {
            self.get_release.add_permits(1);
        }
    }

    impl std::fmt::Debug for DelayedControlPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("DelayedControlPutStore")
                .finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for DelayedControlPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("DelayedControlPutStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for DelayedControlPutStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let should_block = {
                let mut blocked_path = self.blocked_path.lock();
                if blocked_path.as_ref() == Some(location) {
                    blocked_path.take();
                    true
                } else {
                    false
                }
            };
            if should_block {
                self.entered.add_permits(1);
                let inner = Arc::clone(&self.inner);
                let release = Arc::clone(&self.release);
                let completed = Arc::clone(&self.completed);
                let location = location.clone();
                let (sender, receiver) = tokio::sync::oneshot::channel();
                tokio::spawn(async move {
                    let result = match release.acquire().await {
                        Ok(permit) => {
                            permit.forget();
                            inner.put_opts(&location, payload, options).await
                        }
                        Err(error) => Err(object_store::Error::Generic {
                            store: "DelayedControlPutStore",
                            source: Box::new(error),
                        }),
                    };
                    completed.add_permits(1);
                    let _ = sender.send(result);
                });
                return receiver
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "DelayedControlPutStore",
                        source: Box::new(error),
                    })?;
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let should_block = {
                let mut blocked_path = self.blocked_get_path.lock();
                if blocked_path.as_ref() == Some(location) {
                    blocked_path.take();
                    true
                } else {
                    false
                }
            };
            if should_block {
                self.get_entered.add_permits(1);
                self.get_release
                    .acquire()
                    .await
                    .map_err(|error| object_store::Error::Generic {
                        store: "DelayedControlPutStore",
                        source: Box::new(error),
                    })?
                    .forget();
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    async fn acquire_test_process_lease(
        store: Arc<dyn object_store::ObjectStore>,
        node: NodeId,
        owner: uuid::Uuid,
        ttl_ms: i64,
    ) -> laminar_core::cluster::control::ProcessLease {
        use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

        let authority = ProcessLeaseStore::new(store, node, ttl_ms);
        let ProcessLeaseOutcome::Acquired(lease) = authority.try_acquire(owner, 1).await.unwrap()
        else {
            panic!("test process lease was not acquired");
        };
        lease
    }

    async fn take_over_test_process_lease(
        store: Arc<dyn object_store::ObjectStore>,
        incumbent: &laminar_core::cluster::control::ProcessLease,
        replacement: uuid::Uuid,
        ttl_ms: i64,
    ) -> laminar_core::cluster::control::ProcessLease {
        use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

        let authority = ProcessLeaseStore::new(store, incumbent.node, ttl_ms);
        let observation = authority.observe_rival(incumbent).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(
            u64::try_from(ttl_ms).unwrap() + 2,
        ))
        .await;
        let ProcessLeaseOutcome::Acquired(lease) = authority
            .try_takeover(replacement, &observation, 100)
            .await
            .unwrap()
        else {
            panic!("test process lease was not taken over");
        };
        lease
    }

    fn live_test_process_deadline() -> Arc<laminar_core::cluster::control::LeaseDeadline> {
        Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        ))
    }

    #[derive(Clone)]
    struct NamespaceProofTestKv {
        local_id: NodeId,
        values: Arc<parking_lot::Mutex<HashMap<(NodeId, String), String>>>,
    }

    #[async_trait::async_trait]
    impl laminar_core::cluster::control::ClusterKv for NamespaceProofTestKv {
        async fn write(&self, key: &str, value: String) {
            self.values
                .lock()
                .insert((self.local_id, key.to_string()), value);
        }

        async fn read_from(&self, who: NodeId, key: &str) -> Option<String> {
            self.values.lock().get(&(who, key.to_string())).cloned()
        }

        async fn scan(&self, key: &str) -> Vec<(NodeId, String)> {
            self.values
                .lock()
                .iter()
                .filter(|((_, stored_key), _)| stored_key == key)
                .map(|((node, _), value)| (*node, value.clone()))
                .collect()
        }
    }

    fn namespace_proof_participants() -> [laminar_core::checkpoint::CheckpointParticipant; 2] {
        [
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(11),
            },
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(22),
            },
        ]
    }

    fn namespace_proof_test_kvs() -> [Arc<dyn laminar_core::cluster::control::ClusterKv>; 2] {
        let values = Arc::new(parking_lot::Mutex::new(HashMap::new()));
        [
            Arc::new(NamespaceProofTestKv {
                local_id: NodeId(1),
                values: Arc::clone(&values),
            }),
            Arc::new(NamespaceProofTestKv {
                local_id: NodeId(2),
                values,
            }),
        ]
    }

    async fn run_two_node_namespace_proof(
        participants: &[laminar_core::checkpoint::CheckpointParticipant; 2],
        controls: &[Arc<dyn laminar_core::cluster::control::ClusterKv>; 2],
        checkpoint_stores: [Arc<dyn object_store::ObjectStore>; 2],
        state_stores: [Arc<dyn object_store::ObjectStore>; 2],
        timeout: std::time::Duration,
    ) -> [Result<(), ClusterStartupError>; 2] {
        let first = prove_shared_object_store_namespaces(
            participants[0],
            participants,
            Arc::clone(&controls[0]),
            Arc::clone(&checkpoint_stores[0]),
            Arc::clone(&state_stores[0]),
            timeout,
        );
        let second = prove_shared_object_store_namespaces(
            participants[1],
            participants,
            Arc::clone(&controls[1]),
            Arc::clone(&checkpoint_stores[1]),
            Arc::clone(&state_stores[1]),
            timeout,
        );
        let (first, second) = tokio::join!(first, second);
        [first, second]
    }

    async fn namespace_marker_count(
        store: &Arc<dyn object_store::ObjectStore>,
        role: NamespaceProofStore,
    ) -> usize {
        use futures::StreamExt;

        let prefix =
            object_store::path::Path::from(format!("cluster-namespace-proof/v1/{}/", role.name()));
        let mut entries = store.list(Some(&prefix));
        let mut count = 0;
        while let Some(entry) = entries.next().await {
            entry.unwrap();
            count += 1;
        }
        count
    }

    #[test]
    fn test_cluster_startup_error_display() {
        let errors: Vec<ClusterStartupError> = vec![
            ClusterStartupError::Discovery("connection refused".into()),
            ClusterStartupError::FormationTimeout {
                found: 1,
                needed: 3,
            },
            ClusterStartupError::EngineConstruction("build failed".into()),
            ClusterStartupError::HttpStartup("port in use".into()),
        ];
        for err in &errors {
            assert!(!err.to_string().is_empty());
        }
    }

    #[test]
    fn test_formation_timeout_includes_counts() {
        let err = ClusterStartupError::FormationTimeout {
            found: 1,
            needed: 3,
        };
        let msg = err.to_string();
        assert!(msg.contains('1'));
        assert!(msg.contains('3'));
    }

    #[tokio::test]
    async fn cluster_state_seal_records_runtime_node_id() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let node_id = NodeId(73);
        let backend = cluster_state_backend(Arc::clone(&store), node_id, 1);
        let attempt = laminar_core::state::CheckpointAttempt::new(9, 17);

        backend
            .write_partial(attempt, 0, 0, bytes::Bytes::from_static(b"state"))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, None, &[0], &[])
            .await
            .unwrap());

        let path = object_store::path::Path::from(format!(
            "state-v2/epoch={}/checkpoint={}/_SEAL",
            attempt.epoch, attempt.checkpoint_id
        ));
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        let seal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let expected = node_id.to_string();
        assert_eq!(seal["instance_id"].as_str(), Some(expected.as_str()));
    }

    #[tokio::test]
    async fn object_store_control_kv_survives_reconstruction() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let first = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx.clone(),
        );
        first
            .write_checked("control:recover", "release-13".into())
            .await
            .unwrap();
        drop(first);

        let replacement = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        assert_eq!(
            replacement.read_from(NodeId(7), "control:recover").await,
            Some("release-13".into())
        );
        replacement
            .write_checked("control:recover", "release-14".into())
            .await
            .unwrap();
        assert_eq!(
            replacement.read_from(NodeId(7), "control:recover").await,
            Some("release-14".into())
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_rejects_oversized_values_before_body_read() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let oversized = usize::try_from(OBJECT_STORE_CONTROL_MAX_ENVELOPE_BYTES + 1).unwrap();
        store
            .put(
                &object_store_control_record_path(&lease, "control:oversized", 1),
                object_store::PutPayload::from(bytes::Bytes::from(vec![0; oversized])),
            )
            .await
            .unwrap();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let kv = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        let error = kv
            .read_from_checked(NodeId(7), "control:oversized")
            .await
            .unwrap_err();
        assert!(error.contains("maximum"), "{error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_rejects_write_after_local_lease_deadline_expires() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = ObjectStoreClusterKv::new(
            lease.clone(),
            Arc::new(laminar_core::cluster::control::LeaseDeadline::fenced()),
            1_000,
            Arc::clone(&store),
            members_rx,
        );
        let error = kv
            .write_checked("control:recover", "must-not-publish".into())
            .await
            .unwrap_err();
        assert!(error.contains("deadline expired"), "{error}");
        assert!(list_control_sequences(
            &store,
            &object_store_control_key_prefix(&lease, "control:recover")
        )
        .await
        .unwrap()
        .is_empty());
    }

    #[tokio::test]
    async fn object_store_control_kv_ignores_delayed_previous_term_write() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = Arc::new(ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        let stale_path = object_store_control_record_path(&first_lease, "control:recover", 1);
        delayed.block_once(stale_path.clone());
        let stale_writer = {
            let first = Arc::clone(&first);
            tokio::spawn(
                async move { first.write_checked("control:recover", "stale".into()).await },
            )
        };
        delayed.wait_until_blocked().await;
        stale_writer.abort();
        let _ = stale_writer.await;

        let replacement_lease = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;
        let replacement = ObjectStoreClusterKv::new(
            replacement_lease,
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx,
        );
        replacement
            .write_checked("control:recover", "current".into())
            .await
            .unwrap();

        delayed.release();
        delayed.wait_until_completed().await;
        assert!(inner.get(&stale_path).await.is_ok());
        assert_eq!(
            replacement.read_from(NodeId(7), "control:recover").await,
            Some("current".into())
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_delayed_lower_sequence_cannot_regress_same_term() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = Arc::new(ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        let delayed_path = object_store_control_record_path(&lease, "control:recover", 1);
        delayed.block_once(delayed_path.clone());
        let delayed_writer = {
            let kv = Arc::clone(&kv);
            tokio::spawn(async move {
                kv.write_checked("control:recover", "sequence-1".into())
                    .await
            })
        };
        delayed.wait_until_blocked().await;
        delayed_writer.abort();
        let _ = delayed_writer.await;

        kv.write_checked("control:recover", "sequence-2".into())
            .await
            .unwrap();
        delayed.release();
        delayed.wait_until_completed().await;
        assert!(inner.get(&delayed_path).await.is_ok());
        assert_eq!(
            kv.read_from(NodeId(7), "control:recover").await,
            Some("sequence-2".into())
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_revalidates_lease_after_record_body_read() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(Arc::clone(&inner)));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = Arc::new(ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx,
        ));
        first
            .write_checked("control:recover", "old-term".into())
            .await
            .unwrap();
        delayed.block_get_once(object_store_control_record_path(
            &first_lease,
            "control:recover",
            1,
        ));
        let reader = {
            let first = Arc::clone(&first);
            tokio::spawn(async move { first.read_from_checked(NodeId(7), "control:recover").await })
        };
        delayed.wait_until_get_blocked().await;
        let _replacement = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;
        delayed.release_get();
        let error = reader.await.unwrap().unwrap_err();
        assert!(error.contains("changed during control read"), "{error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_stale_local_term_cannot_read_or_scan() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx,
        );
        first
            .write_checked("control:recover", "old-term".into())
            .await
            .unwrap();
        let _replacement = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;

        let read_error = first
            .read_from_checked(NodeId(7), "control:recover")
            .await
            .unwrap_err();
        assert!(read_error.contains("owner or term changed"), "{read_error}");
        let scan_error = first.scan_checked("control:recover").await.unwrap_err();
        assert!(scan_error.contains("owner or term changed"), "{scan_error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_rejects_noncanonical_record_body() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let record = ObjectStoreControlRecord {
            version: OBJECT_STORE_CONTROL_VERSION,
            node: lease.node.0,
            owner: lease.owner,
            term: lease.term,
            sequence: 1,
            key: "control:recover".into(),
            value: "release".into(),
        };
        store
            .put(
                &object_store_control_record_path(&lease, &record.key, record.sequence),
                object_store::PutPayload::from(bytes::Bytes::from(
                    serde_json::to_vec_pretty(&record).unwrap(),
                )),
            )
            .await
            .unwrap();
        let kv = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        let error = kv
            .read_from_checked(NodeId(7), "control:recover")
            .await
            .unwrap_err();
        assert!(error.contains("canonically encoded"), "{error}");
    }

    #[tokio::test]
    async fn object_store_control_kv_prunes_history_but_retains_highest_two() {
        use laminar_core::cluster::control::ClusterKv;

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx.clone(),
        );
        for sequence in 1..=5 {
            kv.write_checked("control:recover", format!("value-{sequence}"))
                .await
                .unwrap();
        }
        let prefix = object_store_control_key_prefix(&lease, "control:recover");
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if list_control_sequences(&store, &prefix).await.unwrap() == [4, 5] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            kv.read_from(NodeId(7), "control:recover").await,
            Some("value-5".into())
        );
        let reconstructed = ObjectStoreClusterKv::new(
            lease,
            live_test_process_deadline(),
            1_000,
            Arc::clone(&store),
            members_rx,
        );
        reconstructed
            .write_checked("control:recover", "value-6".into())
            .await
            .unwrap();
        assert_eq!(
            reconstructed.read_from(NodeId(7), "control:recover").await,
            Some("value-6".into())
        );
    }

    #[test]
    fn object_store_control_kv_prune_selection_is_order_independent() {
        let prefix = "cluster-control-kv/v2/test/";
        let mut oldest = BinaryHeap::new();
        for index in 0..300u64 {
            let sequence = (index * 73) % 300 + 1;
            let path = object_store::path::Path::from(format!("{prefix}v{sequence:020}.json"));
            retain_oldest_control_record(&mut oldest, sequence, &path);
        }
        assert_eq!(
            oldest
                .into_sorted_vec()
                .into_iter()
                .map(|(sequence, _)| sequence)
                .collect::<Vec<_>>(),
            (1..=u64::try_from(OBJECT_STORE_CONTROL_PRUNE_BATCH_RECORDS).unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn object_store_control_kv_pruning_coalesces_per_prefix() {
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            1_000,
        )
        .await;
        let kv = ObjectStoreClusterKv::new(
            lease.clone(),
            live_test_process_deadline(),
            1_000,
            store,
            members_rx,
        );
        let key_prefix = object_store_control_key_prefix(&lease, "control:recover");
        let generation_prefix = RECOVERY_GENERATION_PREFIX.to_string();

        kv.schedule_prune(key_prefix.clone());
        kv.schedule_prune(key_prefix.clone());
        kv.schedule_prune(generation_prefix.clone());
        {
            let states = kv
                .prune_states
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(states.len(), 2);
            assert_eq!(states.get(&key_prefix), Some(&true));
            assert_eq!(states.get(&generation_prefix), Some(&false));
        }

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if kv
                    .prune_states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn recovery_generation_ignores_delayed_lower_marker_and_survives_new_term() {
        use laminar_core::cluster::control::ClusterKv;

        let inner: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let delayed = Arc::new(DelayedControlPutStore::new(inner));
        let store: Arc<dyn object_store::ObjectStore> = delayed.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let ttl_ms = 1;
        let first_lease = acquire_test_process_lease(
            Arc::clone(&store),
            NodeId(7),
            uuid::Uuid::from_u128(71),
            ttl_ms,
        )
        .await;
        let first = Arc::new(ObjectStoreClusterKv::new(
            first_lease.clone(),
            live_test_process_deadline(),
            ttl_ms,
            Arc::clone(&store),
            members_rx.clone(),
        ));
        delayed.block_once(recovery_generation_path(1));
        let delayed_writer = {
            let first = Arc::clone(&first);
            tokio::spawn(async move {
                first
                    .write_checked(RECOVERY_GENERATION_KEY, "1".into())
                    .await
            })
        };
        delayed.wait_until_blocked().await;
        delayed_writer.abort();
        let _ = delayed_writer.await;
        first
            .write_checked(RECOVERY_GENERATION_KEY, "2".into())
            .await
            .unwrap();
        delayed.release();
        delayed.wait_until_completed().await;
        assert_eq!(
            first.read_from(NodeId(7), RECOVERY_GENERATION_KEY).await,
            Some("2".into())
        );

        let replacement_lease = take_over_test_process_lease(
            Arc::clone(&store),
            &first_lease,
            uuid::Uuid::from_u128(72),
            ttl_ms,
        )
        .await;
        let replacement = ObjectStoreClusterKv::new(
            replacement_lease,
            live_test_process_deadline(),
            ttl_ms,
            store,
            members_rx,
        );
        assert_eq!(
            replacement
                .read_from(NodeId(7), RECOVERY_GENERATION_KEY)
                .await,
            Some("2".into())
        );
        replacement
            .write_checked(RECOVERY_GENERATION_KEY, "3".into())
            .await
            .unwrap();
        assert_eq!(
            replacement
                .read_from(NodeId(7), RECOVERY_GENERATION_KEY)
                .await,
            Some("3".into())
        );
    }

    #[tokio::test]
    async fn shared_namespace_proof_retains_bounded_boot_markers_in_both_stores() {
        let checkpoint: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participants = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &participants,
            &controls,
            [Arc::clone(&checkpoint), Arc::clone(&checkpoint)],
            [Arc::clone(&state), Arc::clone(&state)],
            std::time::Duration::from_secs(2),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));

        for node_id in [1, 2] {
            for (store, role) in [
                (&checkpoint, NamespaceProofStore::Checkpoint),
                (&state, NamespaceProofStore::State),
            ] {
                let marker = store
                    .get(&namespace_proof_path(role, node_id))
                    .await
                    .expect("boot marker must remain available to rolling joiners");
                assert!(marker.meta.size <= NAMESPACE_PROOF_MAX_SENTINEL_BYTES);
            }
        }
    }

    #[tokio::test]
    async fn rolling_restart_uses_active_peers_retained_markers() {
        let checkpoint: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let initial = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &initial,
            &controls,
            [Arc::clone(&checkpoint), Arc::clone(&checkpoint)],
            [Arc::clone(&state), Arc::clone(&state)],
            std::time::Duration::from_secs(2),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
        let active_peer_record = controls[1]
            .read_from(NodeId(2), NAMESPACE_PROOF_KEY)
            .await
            .unwrap();

        let restarted = [
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(111),
            },
            initial[1],
        ];
        prove_shared_object_store_namespaces(
            restarted[0],
            &restarted,
            Arc::clone(&controls[0]),
            Arc::clone(&checkpoint),
            Arc::clone(&state),
            std::time::Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(
            controls[1].read_from(NodeId(2), NAMESPACE_PROOF_KEY).await,
            Some(active_peer_record),
            "the active peer must not rerun startup for a rolling joiner"
        );
        assert_eq!(
            namespace_marker_count(&checkpoint, NamespaceProofStore::Checkpoint).await,
            2
        );
        assert_eq!(
            namespace_marker_count(&state, NamespaceProofStore::State).await,
            2
        );
    }

    #[tokio::test]
    async fn shared_namespace_proof_rejects_split_state_namespaces() {
        let checkpoint: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state_a: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state_b: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participants = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &participants,
            &controls,
            [Arc::clone(&checkpoint), checkpoint],
            [state_a, state_b],
            std::time::Duration::from_millis(250),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_err()));
    }

    #[tokio::test]
    async fn shared_namespace_proof_rejects_split_checkpoint_namespaces() {
        let checkpoint_a: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let checkpoint_b: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let state: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let participants = namespace_proof_participants();
        let controls = namespace_proof_test_kvs();
        let results = run_two_node_namespace_proof(
            &participants,
            &controls,
            [checkpoint_a, checkpoint_b],
            [Arc::clone(&state), state],
            std::time::Duration::from_millis(250),
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_err()));
    }

    #[tokio::test]
    async fn failed_process_lease_candidate_cannot_overwrite_active_incarnation() {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, ProcessLeaseOutcome, ProcessLeaseStore,
        };

        let node = NodeId(7);
        let recovery_impl = Arc::new(InMemoryKv::new(node));
        let recovery: Arc<dyn ClusterKv> = recovery_impl.clone();
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let active_owner = uuid::Uuid::from_u128(1);
        let active = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&recovery),
            Arc::clone(&recovery),
            None,
            members_rx.clone(),
            active_owner,
        );
        let lease_store =
            ProcessLeaseStore::new(Arc::new(object_store::memory::InMemory::new()), node, 1_000);
        let ProcessLeaseOutcome::Acquired(active_lease) =
            lease_store.try_acquire(active_owner, 0).await.unwrap()
        else {
            panic!("first process must acquire its stable identity");
        };
        active
            .publish_leased_recovery_incarnation(&active_lease)
            .await
            .unwrap();

        let candidate_owner = uuid::Uuid::from_u128(2);
        let candidate = ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&recovery),
            Arc::clone(&recovery),
            None,
            members_rx,
            candidate_owner,
        );
        let ProcessLeaseOutcome::Held(incumbent) = lease_store
            .try_acquire(candidate_owner, 10_000)
            .await
            .unwrap()
        else {
            panic!("client wall time must not let a candidate steal the lease");
        };
        assert!(candidate
            .publish_leased_recovery_incarnation(&incumbent)
            .await
            .is_err());
        assert_eq!(
            recovery_impl
                .read_from(node, "control:recovery-incarnation")
                .await,
            Some(active_owner.to_string())
        );
    }

    #[tokio::test]
    async fn startup_uses_retained_committed_assignment_when_head_is_draining() {
        use std::collections::BTreeMap;

        use laminar_core::checkpoint::{CheckpointParticipant, LeaderProof, LeaderProofOwner};
        use laminar_core::cluster::control::{AssignmentSnapshot, AssignmentSnapshotStore};

        let object_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let store = AssignmentSnapshotStore::new(object_store);
        let participant = CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::from_u128(11),
        };
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant])
            .unwrap();
        store.save_if_absent(&committed).await.unwrap();
        let draining = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(22),
                }],
                LeaderProof {
                    owner: LeaderProofOwner {
                        node_id: participant.node_id,
                        boot_id: participant.boot_incarnation,
                        process_term: 1,
                    },
                    fencing_token: 1,
                },
            )
            .unwrap();
        store
            .save_if_version(&draining, committed.version)
            .await
            .unwrap();

        let selected = startup_committed_assignment(&store, None, draining)
            .await
            .unwrap();
        assert_eq!(selected, committed);
        assert!(!selected.draining);
    }

    #[tokio::test]
    async fn assignment_seed_rejects_peer_tag_that_is_not_durable_lease_owner() {
        use laminar_core::cluster::control::{ProcessLeaseOutcome, ProcessLeaseStore};

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let self_id = NodeId(1);
        let peer_id = NodeId(2);
        let self_boot = uuid::Uuid::from_u128(11);
        let durable_peer_boot = uuid::Uuid::from_u128(22);
        for (node, boot) in [(self_id, self_boot), (peer_id, durable_peer_boot)] {
            let lease = ProcessLeaseStore::new(Arc::clone(&store), node, 1_000);
            assert!(matches!(
                lease.try_acquire(boot, 0).await.unwrap(),
                ProcessLeaseOutcome::Acquired(_)
            ));
        }

        let mut peer = NodeInfo {
            id: peer_id,
            name: "peer".into(),
            rpc_address: String::new(),
            raft_address: String::new(),
            state: NodeState::Joining,
            metadata: NodeMetadata::default(),
            last_heartbeat_ms: 0,
        };
        peer.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            durable_peer_boot.to_string(),
        );
        let participants =
            assignment_seed_participants(self_id, self_boot, &[peer.clone()], &store, 1_000)
                .await
                .unwrap();
        assert_eq!(
            participants,
            vec![
                laminar_core::checkpoint::CheckpointParticipant {
                    node_id: self_id.0,
                    boot_incarnation: self_boot,
                },
                laminar_core::checkpoint::CheckpointParticipant {
                    node_id: peer_id.0,
                    boot_incarnation: durable_peer_boot,
                },
            ]
        );

        peer.metadata.tags.insert(
            PROCESS_INCARNATION_TAG.into(),
            uuid::Uuid::from_u128(222).to_string(),
        );
        let error = assignment_seed_participants(self_id, self_boot, &[peer], &store, 1_000)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("durable lease belongs"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn startup_waits_for_exact_local_assignment_certificate() {
        use laminar_core::checkpoint::CheckpointAssignmentFence;
        use laminar_core::cluster::control::{
            CheckpointParticipant, ClusterController, ClusterKv, InMemoryKv,
        };
        use laminar_core::state::{NodeId as StateNodeId, VnodeRegistry};

        let node = NodeId(7);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node.0)));

        let publish = Arc::clone(&controller);
        tokio::spawn(async move {
            tokio::task::yield_now().await;
            publish.publish_checkpoint_assignment_fence(Some(
                CheckpointAssignmentFence::from_owner_map(
                    1,
                    &[node.0],
                    vec![CheckpointParticipant {
                        node_id: node.0,
                        boot_incarnation: publish.recovery_incarnation(),
                    }],
                )
                .unwrap(),
            ));
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            wait_for_startup_assignment_fence(&controller, &registry),
        )
        .await
        .expect("startup wait did not observe the assignment certificate")
        .unwrap();
    }
}
