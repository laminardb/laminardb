//! Shared cluster harness for `cluster_e2e_*` tests: per-node checkpoint dirs,
//! a shared state tier, and a CAS-capable control store. Bootstrap DDL through
//! [`ClusterEngineHarness::bootstrap_catalog`] before startup.

#![cfg(feature = "cluster")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourceBatch,
    SourceConnector, SourceConsistency, SourceContract, SourceDrainRequest, SourceDrainResolution,
    SourcePosition, SourceStart, SourceTopology, WriteResult,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::changelog::WEIGHT_COLUMN;
use laminar_core::cluster::control::barrier::BARRIER_ADDR_KEY;
use laminar_core::cluster::control::process_lease::ProcessLeaseAuthority;
use laminar_core::cluster::control::{
    AssignmentSnapshot, AssignmentSnapshotStore, CatalogManifestStore, CheckpointDecisionStore,
    CheckpointParticipant, LeaderLeaseConfig, LeaderLeaseManager, LeaderLeaseStore, ProcessLease,
    ProcessLeaseConfig, ProcessLeaseManager, ProcessLeaseOutcome, ProcessLeaseStore,
    VerifiedClusterNamespaces,
};
use laminar_core::cluster::testing::MiniCluster;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{
    rendezvous_assignment, NodeId, ObjectStoreBackend, StateBackend, VnodeRegistry,
};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{ClusterStartupDisposition, LaminarDB};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const CONVERGENCE_DEADLINE: Duration = Duration::from_secs(10);
const ASSIGNMENT_CERTIFICATION_DEADLINE: Duration = Duration::from_secs(60);
const CONTROL_STORE_CONTRACT_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_LEASE_TTL: Duration = Duration::from_secs(2);
const TEST_LEASE_RENEW_INTERVAL: Duration = Duration::from_millis(500);
pub const TEST_SOURCE_DDL: &str =
    "CREATE SOURCE src (key BIGINT, value BIGINT) WITH ('connector' = 'cluster-harness-scripted')";
pub const TEST_AGGREGATE_SINK_DDL: &str =
    "CREATE SINK observed_totals FROM totals WITH ('connector' = 'cluster-harness-observer')";

pub struct ScriptedClusterHarnessLog {
    rows: parking_lot::Mutex<Vec<(i64, i64)>>,
    resume_gate: tokio::sync::watch::Sender<bool>,
}

impl Default for ScriptedClusterHarnessLog {
    fn default() -> Self {
        let (resume_gate, _receiver) = tokio::sync::watch::channel(true);
        Self {
            rows: parking_lot::Mutex::new(Vec::new()),
            resume_gate,
        }
    }
}

impl ScriptedClusterHarnessLog {
    pub fn append(&self, rows: &[(i64, i64)]) {
        self.rows.lock().extend_from_slice(rows);
    }

    pub fn block_resumes(&self) {
        let _ = self.resume_gate.send_replace(false);
    }

    pub fn release_resumes(&self) {
        let _ = self.resume_gate.send_replace(true);
    }

    fn len(&self) -> usize {
        self.rows.lock().len()
    }

    fn read(&self, cursor: usize, max_records: usize) -> Vec<(i64, i64)> {
        let rows = self.rows.lock();
        let end = cursor.saturating_add(max_records).min(rows.len());
        rows[cursor..end].to_vec()
    }

    async fn wait_for_resume_release(&self) {
        let mut gate = self.resume_gate.subscribe();
        while !*gate.borrow_and_update() {
            gate.changed()
                .await
                .expect("cluster harness owns the resume gate sender");
        }
    }
}

#[derive(Default)]
pub struct ClusterHarnessSourceState {
    polls: std::sync::atomic::AtomicU64,
    resume_starts: std::sync::atomic::AtomicU64,
    last_resume_cursor: std::sync::atomic::AtomicU64,
    last_checkpoint_cursor: std::sync::atomic::AtomicU64,
    failure_requested: std::sync::atomic::AtomicBool,
    failure_observed: std::sync::atomic::AtomicBool,
    drain_resolutions: parking_lot::Mutex<Vec<SourceDrainResolution>>,
}

impl ClusterHarnessSourceState {
    #[must_use]
    pub fn polls(&self) -> u64 {
        self.polls.load(std::sync::atomic::Ordering::Acquire)
    }

    #[must_use]
    pub fn drain_finishes(&self) -> u64 {
        u64::try_from(self.drain_resolutions.lock().len()).expect("drain count fits u64")
    }

    #[must_use]
    pub fn drain_resolutions(&self) -> Vec<SourceDrainResolution> {
        self.drain_resolutions.lock().clone()
    }

    #[must_use]
    pub fn resume_starts(&self) -> u64 {
        self.resume_starts
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[must_use]
    pub fn last_resume_cursor(&self) -> u64 {
        self.last_resume_cursor
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[must_use]
    pub fn last_checkpoint_cursor(&self) -> u64 {
        self.last_checkpoint_cursor
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

struct ScriptedClusterHarnessSource {
    state: Arc<ClusterHarnessSourceState>,
    log: Arc<ScriptedClusterHarnessLog>,
    cursor: usize,
    vnode_registry: Option<Arc<VnodeRegistry>>,
    self_id: Option<NodeId>,
    source_identity: Option<String>,
    reconciled_assignment_version: u64,
}

impl ScriptedClusterHarnessSource {
    fn owns_shared_split(&self) -> bool {
        self.vnode_registry
            .as_ref()
            .zip(self.self_id)
            .is_some_and(|(registry, self_id)| registry.owner(0) == self_id)
    }

    fn checked_cursor(&self, raw_cursor: &str, context: &str) -> Result<usize, ConnectorError> {
        let cursor = raw_cursor.parse::<usize>().map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "scripted source {context} cursor '{raw_cursor}' is invalid: {error}"
            ))
        })?;
        if cursor > self.log.len() {
            return Err(ConnectorError::ConfigurationError(format!(
                "scripted source {context} cursor {cursor} exceeds durable log length {}",
                self.log.len()
            )));
        }
        Ok(cursor)
    }

    fn reconcile_assignment_handoff(&mut self) -> Result<(), ConnectorError> {
        let Some((registry, self_id, source_identity)) = self
            .vnode_registry
            .as_ref()
            .zip(self.self_id)
            .zip(self.source_identity.as_deref())
            .map(|((registry, self_id), source_identity)| (registry, self_id, source_identity))
        else {
            return Ok(());
        };
        let published = registry.versioned_snapshot();
        if published.version() == self.reconciled_assignment_version {
            return Ok(());
        }
        let acquired = published.owners().first().copied() == Some(self_id)
            && published
                .owner_changed_version(0)
                .is_some_and(|changed| changed > self.reconciled_assignment_version);
        if acquired {
            let handoff = published.committed_source_handoff().ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "scripted source '{source_identity}' acquired its split without a committed handoff"
                ))
            })?;
            let state = handoff.source(source_identity).ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "committed checkpoint {:?} has no handoff state for scripted source '{source_identity}'",
                    handoff.attempt()
                ))
            })?;
            let checkpoint = state.checkpoint();
            if checkpoint
                .source_assignment_version
                .map(std::num::NonZeroU64::get)
                != Some(handoff.checkpoint_assignment_version())
            {
                return Err(ConnectorError::ConfigurationError(format!(
                    "scripted source '{source_identity}' handoff assignment does not match its checkpoint fence"
                )));
            }
            let raw_cursor = checkpoint.offsets.get("cursor").ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "scripted source '{source_identity}' handoff is missing cursor"
                ))
            })?;
            self.cursor = self.checked_cursor(raw_cursor, "handoff")?;
            self.state.last_resume_cursor.store(
                u64::try_from(self.cursor).expect("source cursor fits u64"),
                std::sync::atomic::Ordering::Release,
            );
        }
        self.reconciled_assignment_version = published.version();
        Ok(())
    }
}

#[async_trait]
impl SourceConnector for ScriptedClusterHarnessSource {
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        match request.into_parts().1 {
            SourcePosition::Initial => {
                self.cursor = 0;
            }
            SourcePosition::Resume { checkpoint, .. } => {
                let raw_cursor = checkpoint.get_offset("cursor").ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "scripted source recovery checkpoint is missing cursor".into(),
                    )
                })?;
                let cursor = self.checked_cursor(raw_cursor, "recovery")?;
                self.cursor = cursor;
                self.state.last_resume_cursor.store(
                    u64::try_from(cursor).expect("source cursor fits u64"),
                    std::sync::atomic::Ordering::Release,
                );
                self.state
                    .resume_starts
                    .fetch_add(1, std::sync::atomic::Ordering::Release);
                self.log.wait_for_resume_release().await;
            }
        }
        self.reconciled_assignment_version = self
            .vnode_registry
            .as_ref()
            .map_or(0, |registry| registry.assignment_version());
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.reconcile_assignment_handoff()?;
        if !self.owns_shared_split() {
            return Ok(None);
        }
        self.state
            .polls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if self
            .state
            .failure_requested
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.state
                .failure_observed
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(ConnectorError::Internal(
                "cluster harness injected a terminal data-plane failure".into(),
            ));
        }

        let rows = self.log.read(self.cursor, max_records.max(1));
        if rows.is_empty() {
            return Ok(None);
        }
        let keys = rows.iter().map(|(key, _)| *key).collect::<Vec<_>>();
        let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        self.cursor += rows.len();
        let records = RecordBatch::try_new(
            self.schema(),
            vec![
                Arc::new(Int64Array::from(keys)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .map_err(|error| ConnectorError::Internal(format!("scripted source batch: {error}")))?;
        Ok(Some(SourceBatch::new(records)))
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        if self.owns_shared_split() {
            checkpoint.set_offset("cursor", self.cursor.to_string());
            self.state.last_checkpoint_cursor.store(
                u64::try_from(self.cursor).expect("source cursor fits u64"),
                std::sync::atomic::Ordering::Release,
            );
        }
        if let Some(version) = self
            .vnode_registry
            .as_ref()
            .and_then(|registry| std::num::NonZeroU64::new(registry.assignment_version()))
        {
            checkpoint.bind_assignment_version(version);
        }
        checkpoint
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
        ))
    }

    fn set_vnode_assignment(
        &mut self,
        source_identity: &str,
        registry: Arc<VnodeRegistry>,
        self_id: NodeId,
    ) -> Result<(), ConnectorError> {
        self.vnode_registry = Some(registry);
        self.self_id = Some(self_id);
        self.source_identity = Some(source_identity.to_owned());
        Ok(())
    }

    fn begin_drain(
        &mut self,
        _request: &SourceDrainRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn poll_drain_ready(
        &mut self,
        _round: laminar_core::checkpoint::AssignmentDrainId,
    ) -> Result<bool, ConnectorError> {
        Ok(true)
    }

    async fn finish_drain(
        &mut self,
        resolution: SourceDrainResolution,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.state.drain_resolutions.lock().push(resolution);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggregateObservation {
    pub node_id: NodeId,
    pub total: i64,
    pub weight: i64,
}

#[derive(Default)]
pub struct ClusterHarnessSinkState {
    observations: parking_lot::Mutex<Vec<AggregateObservation>>,
}

impl ClusterHarnessSinkState {
    #[must_use]
    pub fn observations(&self) -> Vec<AggregateObservation> {
        self.observations.lock().clone()
    }
}

struct ClusterHarnessObserverSink {
    node_id: NodeId,
    state: Arc<ClusterHarnessSinkState>,
}

#[async_trait]
impl SinkConnector for ClusterHarnessObserverSink {
    fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::FullChangelog,
        ))
    }

    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let total_index = batch.schema().index_of("total").map_err(|error| {
            ConnectorError::SchemaMismatch(format!("aggregate total column: {error}"))
        })?;
        let totals = batch
            .column(total_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch("aggregate total column is not Int64".into())
            })?;
        let weight_index = batch.schema().index_of(WEIGHT_COLUMN).map_err(|error| {
            ConnectorError::SchemaMismatch(format!("aggregate weight column: {error}"))
        })?;
        let weights = batch
            .column(weight_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch("aggregate weight column is not Int64".into())
            })?;

        let mut observations = self.state.observations.lock();
        for row in 0..batch.num_rows() {
            if totals.is_null(row) {
                return Err(ConnectorError::SchemaMismatch(
                    "aggregate observer received a null total".into(),
                ));
            }
            observations.push(AggregateObservation {
                node_id: self.node_id,
                total: totals.value(row),
                weight: weights.value(row),
            });
        }
        drop(observations);

        Ok(WriteResult::new(
            batch.num_rows(),
            u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX),
        ))
    }

    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]))
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

struct ControlLeaseRuntime {
    process_shutdown: CancellationToken,
    process_task: Option<tokio::task::JoinHandle<()>>,
    leader_shutdown: CancellationToken,
    leader_task: Option<tokio::task::JoinHandle<()>>,
}

impl ControlLeaseRuntime {
    async fn shutdown(&mut self) {
        self.leader_shutdown.cancel();
        self.process_shutdown.cancel();
        if let Some(task) = self.leader_task.take() {
            let _ = task.await;
        }
        if let Some(task) = self.process_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ControlLeaseRuntime {
    fn drop(&mut self) {
        self.leader_shutdown.cancel();
        self.process_shutdown.cancel();
        if let Some(task) = self.leader_task.take() {
            task.abort();
        }
        if let Some(task) = self.process_task.take() {
            task.abort();
        }
    }
}

/// Per-node engine state. Owned by [`ClusterEngineHarness`].
pub struct NodeRuntime {
    pub db: Arc<LaminarDB>,
    pub instance_id: NodeId,
    pub vnode_registry: Arc<VnodeRegistry>,
    pub state_backend: Arc<dyn StateBackend>,
    pub shuffle_sender: Arc<ShuffleSender>,
    pub shuffle_receiver: Arc<ShuffleReceiver>,
    pub assignment_snapshot_store: Arc<AssignmentSnapshotStore>,
    pub source_state: Arc<ClusterHarnessSourceState>,
    pub rebalance_shutdown: CancellationToken,
    pub rebalance_tasks: Vec<tokio::task::JoinHandle<()>>,
    control_leases: ControlLeaseRuntime,
}

impl NodeRuntime {
    /// Vnodes this node currently owns.
    #[must_use]
    pub fn owned_vnodes(&self) -> Vec<u32> {
        laminar_core::state::owned_vnodes(&self.vnode_registry, self.instance_id)
    }
}

/// Two-or-more-node cluster of real LaminarDB engines on a `MiniCluster`.
pub struct ClusterEngineHarness {
    /// Gossip + ClusterController layer.
    pub cluster: MiniCluster,
    /// Per-node engine state, in `cluster.nodes` order.
    pub nodes: Vec<NodeRuntime>,
    /// Shared state backend dir. Survives `shutdown_keep_dirs`.
    shared_state_dir: TempDir,
    /// Per-node checkpoint dirs. Survives `shutdown_keep_dirs`.
    checkpoint_dirs: Vec<TempDir>,
    /// Shared cluster control storage. Survives `shutdown_keep_dirs`.
    control_store: Arc<dyn ObjectStore>,
    /// Shared immutable startup catalog authority.
    pub catalog_manifest_store: Arc<CatalogManifestStore>,
    /// Durable scripted input shared by every test connector instance.
    pub source_log: Arc<ScriptedClusterHarnessLog>,
    /// Aggregate output written through the normal cluster sink path.
    pub sink_state: Arc<ClusterHarnessSinkState>,
}

impl ClusterEngineHarness {
    /// Spawn `n` nodes with `vnode_count` vnodes placed by rendezvous hashing. Returns after
    /// gossip converges; `db.start()` is deferred to `start_all`.
    ///
    /// # Panics
    /// On convergence timeout, leader-election mismatch, or engine
    /// build failure.
    pub async fn spawn(n: usize, vnode_count: u32) -> Self {
        Self::spawn_with_control_store(n, vnode_count, Arc::new(InMemory::new())).await
    }

    /// Spawn with an explicit CAS-capable cluster control store.
    pub async fn spawn_with_control_store(
        n: usize,
        vnode_count: u32,
        control_store: Arc<dyn ObjectStore>,
    ) -> Self {
        let shared_state_dir = tempfile::tempdir().expect("shared state tempdir");
        let checkpoint_dirs: Vec<TempDir> = (0..n)
            .map(|_| tempfile::tempdir().expect("checkpoint tempdir"))
            .collect();
        Self::spawn_with_dirs(
            n,
            vnode_count,
            shared_state_dir,
            checkpoint_dirs,
            control_store,
        )
        .await
    }

    /// Like `spawn`, reusing dirs from `shutdown_keep_dirs`.
    pub async fn spawn_with_dirs(
        n: usize,
        vnode_count: u32,
        shared_state_dir: TempDir,
        checkpoint_dirs: Vec<TempDir>,
        control_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::spawn_inner(
            n,
            vnode_count,
            shared_state_dir,
            checkpoint_dirs,
            control_store,
        )
        .await
    }

    async fn spawn_inner(
        n: usize,
        vnode_count: u32,
        shared_state_dir: TempDir,
        checkpoint_dirs: Vec<TempDir>,
        control_store: Arc<dyn ObjectStore>,
    ) -> Self {
        assert_eq!(checkpoint_dirs.len(), n, "one checkpoint dir per node");

        let shared_store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(shared_state_dir.path())
                .expect("LocalFileSystem over shared state dir"),
        );
        let leader_config = LeaderLeaseConfig {
            ttl: TEST_LEASE_TTL,
            renew_interval: TEST_LEASE_RENEW_INTERVAL,
        };
        let leader_store = Arc::new(LeaderLeaseStore::new(
            Arc::clone(&control_store),
            i64::try_from(TEST_LEASE_TTL.as_millis()).expect("test lease TTL fits i64"),
        ));
        leader_store
            .verify_store_contract(CONTROL_STORE_CONTRACT_TIMEOUT)
            .await
            .expect("cluster control store fencing contract");

        // Shared snapshot store — one CAS-creator wins, peers adopt.
        let snapshot_store = Arc::new(AssignmentSnapshotStore::new(Arc::clone(&control_store)));

        let cluster = MiniCluster::spawn_with_snapshot(n, Arc::clone(&snapshot_store)).await;
        cluster
            .wait_for_convergence(CONVERGENCE_DEADLINE)
            .await
            .expect("gossip convergence");

        // Before an assignment roster is installed, the lowest id is the cold-start candidate.
        assert!(cluster.nodes[0].controller.is_leader());

        let process_config = ProcessLeaseConfig {
            ttl: TEST_LEASE_TTL,
            renew_interval: TEST_LEASE_RENEW_INTERVAL,
        };
        let process_lease_authority = Arc::new(
            ProcessLeaseAuthority::new(Arc::clone(&control_store), TEST_LEASE_TTL)
                .expect("shared process lease authority"),
        );
        let process_leases = futures::future::join_all(cluster.nodes.iter().map(|node| {
            let store = process_lease_authority.store_for(node.instance_id);
            let owner = node.controller.recovery_incarnation();
            async move {
                let (lease, acquisition_started_at) = acquire_process_lease(&store, owner).await;
                (store, lease, acquisition_started_at)
            }
        }))
        .await;

        let process_authorities: Vec<_> = process_leases
            .iter()
            .map(|(_, lease, _)| (lease.node, lease.owner, lease.term))
            .collect();
        let peer_ids: Vec<NodeId> = cluster.nodes.iter().map(|node| node.instance_id).collect();
        let mut participants: Vec<CheckpointParticipant> = process_leases
            .iter()
            .map(|(_, lease, _)| CheckpointParticipant {
                node_id: lease.node.0,
                boot_incarnation: lease.owner,
            })
            .collect();
        participants.sort_unstable_by_key(|participant| participant.node_id);

        let catalog_store = Arc::new(CatalogManifestStore::new(Arc::clone(&leader_store)));
        let mut control_leases = Vec::with_capacity(n);
        let mut leader_managers = Vec::with_capacity(n);
        for (node, (process_store, process_lease, acquisition_started_at)) in
            cluster.nodes.iter().zip(process_leases)
        {
            assert_eq!(process_lease.node, node.instance_id);
            assert_eq!(
                process_lease.owner,
                node.controller.recovery_incarnation(),
                "process lease must bind the runtime controller"
            );
            node.controller
                .set_process_lease_authority(Arc::clone(&process_lease_authority))
                .expect("install shared process lease authority");
            let process_manager = ProcessLeaseManager::new(
                process_store,
                process_lease.owner,
                process_config,
                acquisition_started_at,
                &process_lease,
            )
            .expect("process lease manager");
            node.controller
                .set_process_lease_deadline(process_manager.deadline())
                .expect("install process lease deadline");
            node.controller
                .publish_leased_recovery_incarnation(&process_lease)
                .await
                .expect("publish leased recovery incarnation");

            node.controller
                .set_leader_lease_store(Arc::clone(&leader_store));
            let leader_manager =
                LeaderLeaseManager::new(Arc::clone(&leader_store), &process_lease, leader_config)
                    .expect("leader lease manager");
            node.controller
                .set_leader_lease_watch(
                    leader_manager.lease_watch(),
                    leader_manager.owner().clone(),
                    leader_manager.deadline(),
                )
                .expect("install leader lease watch");

            let process_shutdown = CancellationToken::new();
            let process_task = process_manager.spawn(process_shutdown.clone());
            let leader_shutdown = CancellationToken::new();
            control_leases.push(ControlLeaseRuntime {
                process_shutdown,
                process_task: Some(process_task),
                leader_shutdown,
                leader_task: None,
            });
            leader_managers.push(leader_manager);
        }

        // Resolve the assignment before starting leader contenders. A one-vnode assignment can
        // exclude the cold-start node, so granting that node a term first creates a real but
        // transient mismatch between elected assignment leader and durable lease owner.
        let initial_assignment =
            resolve_initial_assignment(&snapshot_store, vnode_count, &peer_ids, &participants)
                .await;
        let retained_snapshot = snapshot_store
            .load()
            .await
            .expect("load resolved assignment")
            .expect("resolved assignment is durable");
        if initial_assignment.is_some() {
            let fence = retained_snapshot
                .assignment_fence()
                .expect("canonical initial assignment fence");
            assert!(fence.participants.iter().all(|participant| {
                process_authorities.iter().any(|(node, boot, _)| {
                    node.0 == participant.node_id && *boot == participant.boot_incarnation
                })
            }));
            for node in &cluster.nodes {
                node.controller
                    .publish_checkpoint_assignment_fence(Some(fence.clone()));
            }
        }

        for ((node, leader_manager), runtime) in cluster
            .nodes
            .iter()
            .zip(leader_managers)
            .zip(&mut control_leases)
        {
            runtime.leader_task = Some(leader_manager.spawn(
                runtime.leader_shutdown.clone(),
                node.controller.leader_candidacy_watch(),
            ));
        }

        let expected_leader = cluster.nodes[0]
            .controller
            .current_leader()
            .expect("assignment elects one leader");
        assert!(cluster
            .nodes
            .iter()
            .all(|node| node.controller.current_leader() == Some(expected_leader)));
        let (_, expected_boot, expected_process_term) = process_authorities
            .iter()
            .find(|(node, _, _)| *node == expected_leader)
            .copied()
            .expect("elected leader has a process lease");
        let expected_controller = cluster
            .nodes
            .iter()
            .find(|node| node.instance_id == expected_leader)
            .expect("elected leader has a runtime controller");
        let deadline = std::time::Instant::now() + CONVERGENCE_DEADLINE;
        loop {
            if let Some(proof) = expected_controller
                .controller
                .capture_catalog_bootstrap_proof()
            {
                let durable = leader_store
                    .load()
                    .await
                    .expect("read durable test leader grant");
                if proof.owner.node_id == expected_leader.0
                    && proof.owner.boot_id == expected_boot
                    && proof.owner.process_term == expected_process_term
                    && durable.is_some_and(|grant| grant.matches_proof(&proof))
                {
                    break;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "assignment-elected process did not acquire the exact durable leader grant",
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Mirror production cluster startup: the barrier service publishes its process-bound
        // control-plane address through the cluster KV before the DB starts.
        for node in &cluster.nodes {
            node.controller.install_local_leader_proof_provider();
            node.controller
                .start_barrier_server(
                    "127.0.0.1:0".parse().expect("loopback control-plane bind"),
                    Some("127.0.0.1".to_string()),
                )
                .await
                .expect("cluster control-plane server start");
        }

        let verified_namespaces: Vec<VerifiedClusterNamespaces> =
            futures::future::join_all(cluster.nodes.iter().map(|node| {
                laminar_core::cluster::control::prove_shared_object_store_namespaces(
                    CheckpointParticipant {
                        node_id: node.instance_id.0,
                        boot_incarnation: node.controller.recovery_incarnation(),
                    },
                    &participants,
                    Arc::clone(node.controller.kv()),
                    Arc::clone(&control_store),
                    Arc::clone(&shared_store),
                    Duration::from_secs(5),
                )
            }))
            .await
            .into_iter()
            .collect::<Result<_, _>>()
            .expect("shared checkpoint/state namespace proof");

        let source_log = Arc::new(ScriptedClusterHarnessLog::default());
        let sink_state = Arc::new(ClusterHarnessSinkState::default());

        // Bind receivers up front so senders can cross-register addresses below.
        let mut receivers: Vec<Arc<ShuffleReceiver>> = Vec::with_capacity(n);
        for nh in &cluster.nodes {
            let recv = ShuffleReceiver::bind(
                nh.instance_id.0,
                "127.0.0.1:0".parse().unwrap(),
                nh.controller.recovery_incarnation(),
            )
            .await
            .expect("ShuffleReceiver::bind");
            receivers.push(Arc::new(recv));
        }

        let mut control_leases = control_leases.into_iter();
        let mut verified_namespaces = verified_namespaces.into_iter();
        let mut node_runtimes = Vec::with_capacity(n);
        for (idx, nh) in cluster.nodes.iter().enumerate() {
            let self_id = nh.instance_id;
            let verified_namespaces = verified_namespaces
                .next()
                .expect("one namespace proof per cluster node");

            let sender = ShuffleSender::new(self_id.0, nh.controller.recovery_incarnation());
            for (p_idx, p_nh) in cluster.nodes.iter().enumerate() {
                if p_idx == idx {
                    continue;
                }
                sender.register_peer(p_nh.instance_id.0, receivers[p_idx].local_addr());
            }
            let sender = Arc::new(sender);

            // No pre-call to `set_authoritative_version`: exercise the real
            // wiring that lifts the snapshot version into the fence on `db.start()`.
            let state_backend: Arc<dyn StateBackend> =
                Arc::new(ObjectStoreBackend::cluster_shared(
                    verified_namespaces.state_store(),
                    self_id.0.to_string(),
                    vnode_count,
                ));

            let registry = Arc::new(VnodeRegistry::new_unassigned(vnode_count));
            if let Some((assignment, version)) = &initial_assignment {
                registry.set_assignment_and_version(Arc::clone(assignment), *version);
            }

            let cp_cfg = StreamCheckpointConfig {
                // Cluster at-least-once admission requires a periodic coordinator. Keep the
                // interval outside the test horizon so manual checkpoints remain deterministic.
                interval_ms: Some(3_600_000),
                max_retained: Some(3),
                ..StreamCheckpointConfig::default()
            };

            // Keep decisions and their recovery capsules beside the leader authority, matching
            // the production cluster object-store namespace.
            let decision_store = Arc::new(CheckpointDecisionStore::new(Arc::clone(&control_store)));
            let source_state = Arc::new(ClusterHarnessSourceState::default());
            let connector_source_state = Arc::clone(&source_state);
            let connector_source_log = Arc::clone(&source_log);
            let connector_sink_state = Arc::clone(&sink_state);
            let connector_node_id = self_id;

            let builder = LaminarDB::builder()
                .storage_dir(checkpoint_dirs[idx].path().to_path_buf())
                .checkpoint(cp_cfg)
                .cluster_controller(Arc::clone(&nh.controller))
                .state_backend(Arc::clone(&state_backend))
                .vnode_registry(Arc::clone(&registry))
                .shuffle_sender(Arc::clone(&sender))
                .shuffle_receiver(Arc::clone(&receivers[idx]))
                .decision_store(Arc::clone(&decision_store))
                .assignment_snapshot_store(Arc::clone(&snapshot_store))
                .catalog_manifest_store(Arc::clone(&catalog_store))
                .verified_cluster_namespaces(verified_namespaces)
                .register_connector(move |registry| {
                    let source_state = Arc::clone(&connector_source_state);
                    let source_log = Arc::clone(&connector_source_log);
                    registry.register_source(
                        "cluster-harness-scripted",
                        ConnectorInfo {
                            name: "cluster-harness-scripted".into(),
                            display_name: "Cluster harness scripted source".into(),
                            version: "1".into(),
                            is_source: true,
                            is_sink: false,
                            config_keys: vec![],
                        },
                        Arc::new(move |_| {
                            Ok(Box::new(ScriptedClusterHarnessSource {
                                state: Arc::clone(&source_state),
                                log: Arc::clone(&source_log),
                                cursor: 0,
                                vnode_registry: None,
                                self_id: None,
                                source_identity: None,
                                reconciled_assignment_version: 0,
                            }))
                        }),
                    )?;
                    let sink_state = Arc::clone(&connector_sink_state);
                    registry.register_sink(
                        "cluster-harness-observer",
                        ConnectorInfo {
                            name: "cluster-harness-observer".into(),
                            display_name: "Cluster harness aggregate observer".into(),
                            version: "1".into(),
                            is_source: false,
                            is_sink: true,
                            config_keys: vec![],
                        },
                        Arc::new(move |_config, _prometheus| {
                            Ok(Box::new(ClusterHarnessObserverSink {
                                node_id: connector_node_id,
                                state: Arc::clone(&sink_state),
                            }))
                        }),
                    )?;
                    Ok(())
                })
                // Mirror production: DataFusion partitions track vnode count.
                .target_partitions(vnode_count as usize);
            let db = builder.build().await.expect("LaminarDB::builder().build()");

            node_runtimes.push(NodeRuntime {
                db,
                instance_id: self_id,
                vnode_registry: Arc::clone(&registry),
                state_backend: Arc::clone(&state_backend),
                shuffle_sender: Arc::clone(&sender),
                shuffle_receiver: Arc::clone(&receivers[idx]),
                assignment_snapshot_store: Arc::clone(&snapshot_store),
                source_state,
                rebalance_shutdown: CancellationToken::new(),
                rebalance_tasks: Vec::new(),
                control_leases: control_leases.next().expect("one lease runtime per node"),
            });
        }

        Self {
            cluster,
            nodes: node_runtimes,
            shared_state_dir,
            checkpoint_dirs,
            control_store,
            catalog_manifest_store: catalog_store,
            source_log,
            sink_state,
        }
    }

    /// Return the shared control store used by leases, assignments, and checkpoint decisions.
    pub fn control_store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.control_store)
    }

    /// Start every pipeline under the production assignment fence, certify shuffle ownership,
    /// then open source intake.
    pub async fn start_all(&mut self) {
        for node in &self.nodes {
            node.db.fence_cluster_startup();
            node.db
                .enable_coordinated_recovery()
                .expect("enable cluster recovery monitor");
        }
        for node in &self.nodes {
            node.db.start().await.expect("db.start()");
        }
        // Fast timings so tests don't wait the 5s production debounce.
        let cfg = laminar_db::rebalance::RebalanceConfig::test_defaults();
        for (node, cluster_node) in self.nodes.iter().zip(&self.cluster.nodes) {
            if node.vnode_registry.assignment_version() != 0 {
                continue;
            }
            let deadline = tokio::time::Instant::now() + cfg.checkpoint_timeout;
            let head = node
                .assignment_snapshot_store
                .load()
                .await
                .expect("load retained startup assignment")
                .expect("retained startup assignment");
            let committed = laminar_db::rebalance::startup_committed_assignment(
                node.assignment_snapshot_store.as_ref(),
                Some(cluster_node.controller.as_ref()),
                head,
            )
            .await
            .expect("audit retained startup assignment");
            let adoption = node
                .db
                .adopt_assignment_snapshot(committed, deadline)
                .await
                .expect("adopt retained startup assignment");
            assert!(adoption.adopted, "retained startup assignment was deferred");
        }
        for (idx, nh) in self.cluster.nodes.iter().enumerate() {
            let node = &mut self.nodes[idx];
            let watcher = laminar_db::rebalance::spawn_snapshot_watcher(
                Arc::clone(&node.db),
                Arc::clone(&node.assignment_snapshot_store),
                Arc::clone(&node.vnode_registry),
                node.rebalance_shutdown.clone(),
                cfg,
                Some(Arc::clone(&nh.controller)),
            );
            let controller = laminar_db::rebalance::spawn_rebalance_controller(
                Arc::clone(&node.db),
                Arc::clone(&nh.controller),
                Arc::clone(&node.assignment_snapshot_store),
                Arc::clone(&node.vnode_registry),
                node.rebalance_shutdown.clone(),
                cfg,
            );
            node.rebalance_tasks.push(watcher);
            node.rebalance_tasks.push(controller);
        }
        // Gate on every controller seeing full membership and every peer's published
        // control-plane address. Discovery polls chitchat on its own cadence, so without this a
        // checkpoint can fire before `members_rx` is populated or a peer's barrier endpoint is
        // resolvable.
        let expected = self.cluster.nodes.len();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let membership_ready = self
                .cluster
                .nodes
                .iter()
                .all(|n| n.controller.live_instances().len() == expected);
            let mut control_plane_ready = true;
            'observers: for observer in &self.cluster.nodes {
                for peer in &self.cluster.nodes {
                    if observer
                        .controller
                        .kv()
                        .read_from(peer.instance_id, BARRIER_ADDR_KEY)
                        .await
                        .is_none()
                    {
                        control_plane_ready = false;
                        break 'observers;
                    }
                }
            }
            if membership_ready && control_plane_ready {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "controllers never converged on membership and control-plane addresses",
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let deadline = std::time::Instant::now() + ASSIGNMENT_CERTIFICATION_DEADLINE;
        loop {
            let assignments_ready =
                self.nodes
                    .iter()
                    .zip(&self.cluster.nodes)
                    .all(|(runtime, cluster_node)| {
                        let version = runtime.vnode_registry.assignment_version();
                        cluster_node
                            .controller
                            .checkpoint_assignment_fence(version)
                            .is_some_and(|fence| {
                                if fence.contains(cluster_node.instance_id.0) {
                                    let digest = Some(fence.digest());
                                    runtime.shuffle_sender.assignment_version() == version
                                        && runtime.shuffle_receiver.assignment_version() == version
                                        && runtime.shuffle_sender.active_assignment_digest()
                                            == digest
                                        && runtime.shuffle_receiver.active_assignment_digest()
                                            == digest
                                } else {
                                    runtime.shuffle_sender.assignment_version() == 0
                                        && runtime.shuffle_receiver.assignment_version() == 0
                                        && runtime
                                            .shuffle_sender
                                            .active_assignment_digest()
                                            .is_none()
                                        && runtime
                                            .shuffle_receiver
                                            .active_assignment_digest()
                                            .is_none()
                                        && runtime.db.cluster_intake_fenced()
                                }
                            })
                    });
            if assignments_ready {
                break;
            }
            if std::time::Instant::now() >= deadline {
                let mut details = Vec::with_capacity(self.nodes.len());
                for (runtime, cluster_node) in self.nodes.iter().zip(&self.cluster.nodes) {
                    details.push(format!(
                        "node {}: recovering={}, leader={:?}, fence={:?}, sender=({}, {:?}), receiver=({}, {:?}), adoptions={:?}",
                        runtime.instance_id.0,
                        cluster_node.controller.is_recovering(),
                        cluster_node.controller.current_leader(),
                        cluster_node.controller.checkpoint_assignment_fence(
                            runtime.vnode_registry.assignment_version(),
                        ),
                        runtime.shuffle_sender.assignment_version(),
                        runtime.shuffle_sender.active_assignment_digest(),
                        runtime.shuffle_receiver.assignment_version(),
                        runtime.shuffle_receiver.active_assignment_digest(),
                        cluster_node.controller.read_adopted_assignments().await,
                    ));
                }
                panic!(
                    "shuffle assignment certificates never became active within {:?}: {}",
                    ASSIGNMENT_CERTIFICATION_DEADLINE,
                    details.join("; "),
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let authority_deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut recovery_fenced = false;
        for node in &self.nodes {
            let disposition = node
                .db
                .finish_cluster_startup(authority_deadline)
                .await
                .expect("cluster startup authority activation");
            recovery_fenced |= disposition == ClusterStartupDisposition::RecoveryFenced;
        }
        tokio::time::timeout(Duration::from_secs(60), async {
            loop {
                if self
                    .nodes
                    .iter()
                    .zip(&self.cluster.nodes)
                    .all(|(runtime, cluster_node)| {
                        let fence = cluster_node
                            .controller
                            .checkpoint_assignment_fence(
                                runtime.vnode_registry.assignment_version(),
                            )
                            .expect("startup assignment fence");
                        let owns_vnodes = fence.contains(runtime.instance_id.0);
                        runtime.db.cluster_intake_fenced() != owns_vnodes
                            && (!recovery_fenced
                                || !owns_vnodes
                                || !cluster_node.controller.is_recovering())
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("cluster startup must align source intake with assignment ownership");
        for (node, cluster_node) in self.nodes.iter().zip(&self.cluster.nodes) {
            let fence = cluster_node
                .controller
                .checkpoint_assignment_fence(node.vnode_registry.assignment_version())
                .expect("startup assignment fence");
            assert_eq!(
                node.db.cluster_intake_fenced(),
                !fence.contains(node.instance_id.0),
                "source-intake fence disagrees with ownership on node {}",
                node.instance_id.0
            );
        }
        self.await_leader_idx().await;
    }

    async fn await_leader_idx(&self) -> usize {
        let deadline = std::time::Instant::now() + CONVERGENCE_DEADLINE;
        loop {
            let leaders = self
                .cluster
                .nodes
                .iter()
                .enumerate()
                .filter_map(|(index, node)| node.controller.is_leader().then_some(index))
                .collect::<Vec<_>>();
            if let [leader] = leaders.as_slice() {
                return *leader;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "durable leader lease did not converge: observed {leaders:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Index of the current assignment-certified durable leader in `nodes`.
    #[must_use]
    pub fn leader_idx(&self) -> usize {
        self.cluster
            .nodes
            .iter()
            .position(|n| n.controller.is_leader())
            .expect("at least one leader after convergence")
    }

    /// Convenience: every non-leader index.
    pub fn follower_idxs(&self) -> Vec<usize> {
        let leader = self.leader_idx();
        (0..self.cluster.nodes.len())
            .filter(|i| *i != leader)
            .collect()
    }

    /// Seal the complete catalog on the durable leader, then replay that exact manifest on peers.
    pub async fn bootstrap_catalog(&self, ddl: &[&str]) -> Result<(), laminar_db::DbError> {
        let entries = ddl.iter().map(|sql| (*sql).to_owned()).collect::<Vec<_>>();
        let leader = self.await_leader_idx().await;
        self.nodes[leader]
            .db
            .execute_cluster_bootstrap_batch(&entries)
            .await?;
        for follower in (0..self.nodes.len()).filter(|index| *index != leader) {
            self.nodes[follower]
                .db
                .execute_cluster_bootstrap_batch(&entries)
                .await?;
        }
        Ok(())
    }

    /// Inject a terminal source failure on the vnode-zero owner, drop its runtime without DB
    /// shutdown, and fail its in-process gossip peer. Subprocess tests cover operating-system hard
    /// kills separately.
    pub async fn fail_node_runtime(&mut self, index: usize) -> NodeId {
        let mut runtime = self.nodes.remove(index);
        let cluster_node = self.cluster.nodes.remove(index);
        assert_eq!(runtime.instance_id, cluster_node.instance_id);
        assert_eq!(
            runtime.vnode_registry.owner(0),
            runtime.instance_id,
            "terminal scripted-source injection requires the vnode-zero owner"
        );

        runtime
            .source_state
            .failure_requested
            .store(true, std::sync::atomic::Ordering::Release);
        let source_state = Arc::clone(&runtime.source_state);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !source_state
                .failure_observed
                .load(std::sync::atomic::Ordering::Acquire)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("source must observe the injected terminal failure before runtime crash");

        runtime.rebalance_shutdown.cancel();
        for task in &runtime.rebalance_tasks {
            task.abort();
        }
        for task in runtime.rebalance_tasks.drain(..) {
            let _ = task.await;
        }

        let instance_id = cluster_node.instance_id;
        cluster_node.crash().await;
        drop(runtime);

        instance_id
    }

    /// Shut down and return the durable stores for a restart scenario.
    pub async fn shutdown_keep_dirs(self) -> (TempDir, Vec<TempDir>, Arc<dyn ObjectStore>) {
        let Self {
            cluster,
            nodes,
            shared_state_dir,
            checkpoint_dirs,
            control_store,
            catalog_manifest_store: _,
            source_log: _,
            sink_state: _,
        } = self;
        for mut node in nodes {
            node.rebalance_shutdown.cancel();
            for task in &node.rebalance_tasks {
                task.abort();
            }
            for task in node.rebalance_tasks.drain(..) {
                let _ = task.await;
            }
            let _ = node.db.shutdown().await;
            node.control_leases.shutdown().await;
        }
        cluster.shutdown().await;
        (shared_state_dir, checkpoint_dirs, control_store)
    }

    /// Drop the cluster cleanly. Tempdirs are removed.
    pub async fn shutdown(self) {
        let _ = self.shutdown_keep_dirs().await;
    }
}

async fn acquire_process_lease(
    store: &Arc<ProcessLeaseStore>,
    owner: uuid::Uuid,
) -> (ProcessLease, std::time::Instant) {
    let acquisition_started_at = std::time::Instant::now();
    match store
        .try_acquire(owner, unix_time_millis())
        .await
        .expect("acquire process lease")
    {
        ProcessLeaseOutcome::Acquired(lease) => (lease, acquisition_started_at),
        ProcessLeaseOutcome::Held(incumbent) => {
            let observation = store
                .observe_rival(&incumbent)
                .expect("observe prior process lease");
            tokio::time::sleep(TEST_LEASE_TTL).await;
            let takeover_started_at = std::time::Instant::now();
            match store
                .try_takeover(owner, &observation, unix_time_millis())
                .await
                .expect("take over prior process lease")
            {
                ProcessLeaseOutcome::Acquired(lease) => (lease, takeover_started_at),
                ProcessLeaseOutcome::Held(current) => panic!(
                    "prior process lease was renewed during clean restart: node={}, term={}",
                    current.node.0, current.term
                ),
            }
        }
    }
}

fn unix_time_millis() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis();
    i64::try_from(millis).expect("current Unix time fits i64 milliseconds")
}

/// CAS-create the first assignment. Existing clusters boot unassigned and adopt after start.
async fn resolve_initial_assignment(
    store: &AssignmentSnapshotStore,
    vnode_count: u32,
    peer_ids: &[NodeId],
    participants: &[CheckpointParticipant],
) -> Option<(Arc<[NodeId]>, u64)> {
    if let Some(snapshot) = store.load().await.expect("load snapshot") {
        snapshot
            .to_vnode_vec(vnode_count)
            .expect("snapshot cardinality");
        return None;
    }

    let fresh = rendezvous_assignment(vnode_count, peer_ids);
    let owner_ids: std::collections::BTreeSet<u64> = fresh.iter().map(|owner| owner.0).collect();
    let owner_participants: Vec<_> = participants
        .iter()
        .copied()
        .filter(|participant| owner_ids.contains(&participant.node_id))
        .collect();
    assert_eq!(owner_participants.len(), owner_ids.len());
    let snap = AssignmentSnapshot::empty()
        .next_for_participants(
            AssignmentSnapshot::vnodes_from_vec(&fresh),
            owner_participants,
        )
        .expect("canonical assignment snapshot");
    let winner = match store.save_if_absent(&snap).await.expect("save_if_absent") {
        Some(winner) => winner,
        None => store
            .load()
            .await
            .expect("load after CAS loss")
            .expect("snapshot present after CAS loss"),
    };
    let version = winner.version;
    let assignment = winner
        .to_vnode_vec(vnode_count)
        .expect("snapshot cardinality")
        .into();
    Some((assignment, version))
}

/// Diagnostic tuple `(instance_id, owned_vnodes)`.
pub type NodeIdView = (NodeId, Vec<u32>);
