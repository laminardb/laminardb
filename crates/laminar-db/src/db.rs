//! The main `LaminarDB` database facade.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use laminar_core::streaming;
use laminar_sql::parser::{parse_streaming_sql, ShowCommand, StreamingStatement};
use laminar_sql::planner::StreamingPlanner;
use laminar_sql::register_streaming_functions;
use laminar_sql::translator::{AsofJoinTranslatorConfig, JoinOperatorConfig};

use crate::builder::LaminarDbBuilder;
use crate::catalog::SourceCatalog;
use crate::config::LaminarConfig;
use crate::error::DbError;
use crate::handle::{
    DdlInfo, ExecuteResult, QueryHandle, QueryInfo, SinkInfo, SourceHandle, SourceInfo,
    UntypedSourceHandle,
};
use crate::pipeline::ControlMsg;
use crate::pipeline_lifecycle::url_to_checkpoint_prefix;
use crate::sql_utils;

/// Cloneable async sender for the live-DDL control channel.
pub(crate) type ControlMsgTx = crossfire::MAsyncTx<crossfire::mpsc::Array<ControlMsg>>;

/// Lifecycle state of a [`LaminarDB`] instance, stored as `AtomicU8`.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DbState {
    Created = 0,
    Starting = 1,
    Running = 2,
    ShuttingDown = 3,
    Stopped = 4,
}

impl DbState {
    pub(crate) fn from_u8(raw: u8) -> Option<Self> {
        Some(match raw {
            0 => Self::Created,
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::ShuttingDown,
            4 => Self::Stopped,
            _ => return None,
        })
    }

    pub(crate) fn load(atomic: &std::sync::atomic::AtomicU8) -> Self {
        Self::from_u8(atomic.load(std::sync::atomic::Ordering::Acquire)).unwrap_or(Self::Stopped)
    }

    pub(crate) fn store(self, atomic: &std::sync::atomic::AtomicU8) {
        atomic.store(self as u8, std::sync::atomic::Ordering::Release);
    }

    /// Atomically transition `current -> new`; returns the observed state on
    /// failure so callers can claim an exclusive transition.
    pub(crate) fn compare_exchange(
        current: Self,
        new: Self,
        atomic: &std::sync::atomic::AtomicU8,
    ) -> Result<Self, Self> {
        use std::sync::atomic::Ordering;
        atomic
            .compare_exchange(
                current as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|v| Self::from_u8(v).unwrap_or(Self::Stopped))
            .map_err(|v| Self::from_u8(v).unwrap_or(Self::Stopped))
    }
}

fn cache_entries_from_memory(mem: laminar_sql::parser::lookup_table::ByteSize) -> usize {
    (mem.as_bytes() / 256).max(1024) as usize
}

/// The main `LaminarDB` database handle.
///
/// Provides a unified interface for SQL execution, data ingestion,
/// and result consumption. All streaming infrastructure (sources, sinks,
/// channels, subscriptions) is managed internally.
pub struct LaminarDB {
    pub(crate) catalog: Arc<SourceCatalog>,
    pub(crate) planner: parking_lot::Mutex<StreamingPlanner>,
    pub(crate) ctx: SessionContext,
    pub(crate) config: LaminarConfig,
    pub(crate) config_vars: Arc<HashMap<String, String>>,
    pub(crate) shutdown: std::sync::atomic::AtomicBool,
    /// Populated by `start()`.
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    pub(crate) connector_manager: parking_lot::Mutex<crate::connector_manager::ConnectorManager>,
    pub(crate) connector_registry: Arc<laminar_connectors::registry::ConnectorRegistry>,
    pub(crate) mv_registry: parking_lot::Mutex<laminar_core::mv::MvRegistry>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    pub(crate) runtime_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    pub(crate) engine_metrics:
        parking_lot::Mutex<Option<Arc<crate::engine_metrics::EngineMetrics>>>,
    pub(crate) prometheus_registry: parking_lot::Mutex<Option<Arc<prometheus::Registry>>>,
    pub(crate) start_time: std::time::Instant,
    pub(crate) session_properties: parking_lot::Mutex<HashMap<String, String>>,
    /// Min of all source watermarks.
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    /// The registry of lookup table snapshots.
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    /// Assembled AI subsystem (registry + providers + cache + call log).
    /// `None` unless `[ai]`/`[models]` are configured. Set once in the builder.
    pub(crate) ai_runtime: Option<Arc<crate::ai::AiRuntime>>,
    /// Main runtime handle the AI inference workers spawn on. Set with `ai_runtime`.
    pub(crate) ai_handle: Option<tokio::runtime::Handle>,
    /// Live-DDL channel to the running coordinator. `None` outside `start..shutdown`.
    pub(crate) control_tx: parking_lot::Mutex<Option<ControlMsgTx>>,
    pub(crate) mv_store: Arc<parking_lot::RwLock<crate::mv_store::MvStore>>,
    /// Activates leader/follower checkpoint flow when set. `None` in embedded mode.
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::ClusterController>>>,
    /// Pair with `vnode_registry`; the coordinator writes per-vnode durability
    /// markers each checkpoint and runs the gate when both are installed.
    pub(crate) state_backend:
        parking_lot::Mutex<Option<Arc<dyn laminar_core::state::StateBackend>>>,
    pub(crate) vnode_registry: parking_lot::Mutex<Option<Arc<laminar_core::state::VnodeRegistry>>>,
    /// Applied to both `self.ctx` and the pipeline-side `OperatorGraph` context.
    pub(crate) physical_optimizer_rules:
        Arc<[Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>]>,
    /// `target_partitions` override; cluster mode sets this to `vnode_count`.
    pub(crate) pipeline_target_partitions: Option<usize>,
    /// Used by `SqlQueryOperator` to row-shuffle pre-aggregate batches to owners.
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_sender:
        parking_lot::Mutex<Option<Arc<laminar_core::shuffle::ShuffleSender>>>,
    /// `Arc`-wrapped so the subscription-router task can hold a weak handle and
    /// read the receiver lazily (installed after the controller).
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_receiver:
        Arc<parking_lot::Mutex<Option<Arc<laminar_core::shuffle::ShuffleReceiver>>>>,
    #[cfg(feature = "cluster")]
    pub(crate) decision_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::CheckpointDecisionStore>>>,
    #[cfg(feature = "cluster")]
    pub(crate) assignment_snapshot_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>>,
    /// Cluster-wide catalog manifest store (`catalog/manifest.json` on the
    /// shared object store). Persisted on each successful checkpoint and
    /// replayed at boot so a node rebuilds MVs/sources it lacks locally.
    #[cfg(feature = "cluster")]
    pub(crate) catalog_manifest_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::CatalogManifestStore>>>,
    /// Committed per-vnode state staged by [`Self::adopt_assignment_snapshot`]
    /// for vnodes this node newly acquired in a rebalance. Operators that
    /// adopt the per-vnode state backend drain this on their next cycle to
    /// resume from the last committed epoch instead of empty state.
    ///
    /// `Arc`-wrapped so the same handle is shared with the `OperatorGraph`
    /// (via [`ClusterShuffleConfig`](crate::operator::sql_query::ClusterShuffleConfig)),
    /// which drains and applies the staged slices into live operators each cycle.
    #[cfg(feature = "cluster")]
    pub(crate) rehydrated_vnode_state: Arc<parking_lot::Mutex<HashMap<u32, RehydratedVnode>>>,
    /// Hands `db.checkpoint()` requests to the pipeline callback so it can capture
    /// operator state before the manifest is packed. When `None`, falls back to
    /// the direct coordinator path — only valid for stateless engines.
    pub(crate) force_ckpt_tx: parking_lot::Mutex<Option<ForceCheckpointTx>>,
    pub(crate) subscription_registry: Arc<crate::subscription::SubscriptionRegistry>,
    /// Cluster mode: stream/MV name → subscribing node ids, refreshed from
    /// gossip by the router task and read each cycle by producers.
    #[cfg(feature = "cluster")]
    pub(crate) active_subs:
        Arc<parking_lot::RwLock<std::collections::HashMap<String, std::collections::HashSet<u64>>>>,
    /// Stream output schemas resolved at `start()`; consulted by SUBSCRIBE WHERE.
    pub(crate) stream_schemas:
        parking_lot::RwLock<std::collections::HashMap<String, arrow_schema::SchemaRef>>,
}

/// Reply channel for a single `db.checkpoint()` request.
pub(crate) type ForceCheckpointReply =
    crossfire::oneshot::TxOneshot<Result<crate::checkpoint_coordinator::CheckpointResult, DbError>>;

/// `db.checkpoint()` → pipeline callback request channel. Cold path; the cap
/// is generous enough that callers never wait.
pub(crate) type ForceCheckpointTx =
    crossfire::MAsyncTx<crossfire::mpsc::Array<ForceCheckpointReply>>;

pub(crate) type ForceCheckpointRx =
    crossfire::AsyncRx<crossfire::mpsc::Array<ForceCheckpointReply>>;

pub(crate) const FORCE_CHECKPOINT_CHANNEL_CAPACITY: usize = 64;

/// Subscription-router timer period. Drains run every tick, so this bounds
/// cross-node SUBSCRIBE delivery latency.
#[cfg(feature = "cluster")]
const SUB_ROUTER_TICK: std::time::Duration = std::time::Duration::from_millis(10);
/// Refresh the gossip interest cache ~every 500ms while subscriptions exist.
#[cfg(feature = "cluster")]
const SUB_REFRESH_ACTIVE_TICKS: u64 = 50;
/// Back off to ~5s when there is no subscription activity anywhere.
#[cfg(feature = "cluster")]
const SUB_REFRESH_IDLE_TICKS: u64 = 500;

pub(crate) struct SourceWatermarkState {
    pub(crate) extractor: laminar_core::time::EventTimeExtractor,
    pub(crate) generator: Box<dyn laminar_core::time::WatermarkGenerator>,
    pub(crate) column: String,
}

/// Keep rows at/after the watermark. `Ok(None)` = all rows late;
/// `Err` = schema drift (missing/non-timestamp column).
pub(crate) fn filter_late_rows(
    batch: &RecordBatch,
    column: &str,
    watermark: i64,
) -> Result<Option<RecordBatch>, laminar_core::time::FilterError> {
    laminar_core::time::filter_batch_by_timestamp(
        batch,
        column,
        watermark,
        laminar_core::time::ThresholdOp::GreaterEq,
    )
}

pub(crate) use laminar_core::time::parse_duration_str;

/// Committed state for a single vnode, staged during rebalance adoption
/// so newly-acquired vnodes resume from the last committed epoch.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
pub struct RehydratedVnode {
    /// Committed epoch the partial was read from.
    pub epoch: u64,
    /// The vnode's `partial.bin` bytes at `epoch`.
    pub bytes: bytes::Bytes,
}

/// Summary of a single [`LaminarDB::adopt_assignment_snapshot`] call.
///
/// Returned so the rebalance control plane can log and meter what each
/// adoption moved — in particular the vnodes this node gained and how
/// much of their committed state it rehydrated from durable storage.
#[cfg(feature = "cluster")]
#[derive(Debug, Default)]
pub struct SnapshotAdoption {
    /// `false` when the snapshot was stale (≤ the current registry
    /// version) or no registry was installed — nothing changed.
    pub adopted: bool,
    /// The snapshot version this call considered.
    pub version: u64,
    /// Vnodes this node owns now but did not before this rotation.
    pub newly_acquired: Vec<u32>,
    /// How many of `newly_acquired` had committed state read back.
    pub rehydrated: usize,
    /// Committed epoch the rehydration read from, if any.
    pub rehydration_epoch: Option<u64>,
}

impl LaminarDB {
    /// Create an embedded in-memory database with default settings.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    pub fn open() -> Result<Self, DbError> {
        Self::open_with_config(LaminarConfig::default())
    }

    /// Create with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    pub fn open_with_config(config: LaminarConfig) -> Result<Self, DbError> {
        Self::open_with_config_and_vars(config, HashMap::new())
    }

    /// Create with custom configuration and config variables for SQL substitution.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn open_with_config_and_vars(
        config: LaminarConfig,
        config_vars: HashMap<String, String>,
    ) -> Result<Self, DbError> {
        Self::open_with_config_and_vars_and_rules(config, config_vars, &[], None)
    }

    /// Same as [`Self::open_with_config_and_vars`] but also installs
    /// the given physical-optimizer rules on the `DataFusion` session.
    #[allow(clippy::unnecessary_wraps)]
    pub(crate) fn open_with_config_and_vars_and_rules(
        config: LaminarConfig,
        config_vars: HashMap<String, String>,
        extra_optimizer_rules: &[Arc<
            dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync,
        >],
        target_partitions: Option<usize>,
    ) -> Result<Self, DbError> {
        // One-time crossfire backoff tuning. No-op on multi-core; on single-core
        // VMs this swaps spin-loops for yields (~2x channel throughput).
        // Idempotent via an internal atomic — safe to call on every instance.
        crossfire::detect_backoff_cfg();

        let lookup_registry = Arc::new(laminar_sql::datafusion::LookupTableRegistry::new());

        // Build a SessionContext with the LookupJoinExtensionPlanner wired
        // into the physical planner so LookupJoinNode → LookupJoinExec works.
        let ctx = {
            let mut session_config = laminar_sql::datafusion::base_session_config();
            if let Some(n) = target_partitions {
                session_config = session_config.with_target_partitions(n);
            }
            let extension_planner: Arc<
                dyn datafusion::physical_planner::ExtensionPlanner + Send + Sync,
            > = Arc::new(laminar_sql::datafusion::LookupJoinExtensionPlanner::new(
                Arc::clone(&lookup_registry),
            ));
            let query_planner: Arc<dyn datafusion::execution::context::QueryPlanner + Send + Sync> =
                Arc::new(LookupQueryPlanner { extension_planner });
            let mut state_builder = datafusion::execution::SessionStateBuilder::new()
                .with_config(session_config)
                .with_default_features()
                .with_query_planner(query_planner);
            #[cfg(feature = "cluster")]
            {
                state_builder = state_builder.with_physical_optimizer_rule(Arc::new(
                    laminar_sql::datafusion::cluster_repartition::DistributedJoinRule,
                ));
            }
            for rule in extra_optimizer_rules {
                state_builder = state_builder.with_physical_optimizer_rule(Arc::clone(rule));
            }
            SessionContext::new_with_state(state_builder.build())
        };
        register_streaming_functions(&ctx);

        let catalog = Arc::new(SourceCatalog::new(
            config.default_buffer_size,
            config.default_backpressure,
        ));

        let connector_registry = Arc::new(laminar_connectors::registry::ConnectorRegistry::new());
        Self::register_builtin_connectors(&connector_registry);
        #[cfg(feature = "cluster")]
        let mut physical_rules = extra_optimizer_rules.to_vec();
        #[cfg(feature = "cluster")]
        {
            physical_rules.push(Arc::new(
                laminar_sql::datafusion::cluster_repartition::DistributedJoinRule,
            ));
        }
        #[cfg(not(feature = "cluster"))]
        let physical_rules = extra_optimizer_rules.to_vec();

        Ok(Self {
            catalog,
            planner: parking_lot::Mutex::new(StreamingPlanner::new()),
            ctx,
            config,
            config_vars: Arc::new(config_vars),
            shutdown: std::sync::atomic::AtomicBool::new(false),
            coordinator: Arc::new(tokio::sync::Mutex::new(None)),
            connector_manager: parking_lot::Mutex::new(
                crate::connector_manager::ConnectorManager::new(),
            ),
            connector_registry,
            mv_registry: parking_lot::Mutex::new(laminar_core::mv::MvRegistry::new()),
            table_store: Arc::new(parking_lot::RwLock::new(
                crate::table_store::TableStore::new(),
            )),
            state: Arc::new(std::sync::atomic::AtomicU8::new(DbState::Created as u8)),
            runtime_handle: parking_lot::Mutex::new(None),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            engine_metrics: parking_lot::Mutex::new(None),
            prometheus_registry: parking_lot::Mutex::new(None),
            start_time: std::time::Instant::now(),
            session_properties: parking_lot::Mutex::new(HashMap::new()),
            pipeline_watermark: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            lookup_registry,
            ai_runtime: None,
            ai_handle: None,
            control_tx: parking_lot::Mutex::new(None),
            mv_store: Arc::new(parking_lot::RwLock::new(crate::mv_store::MvStore::new())),
            #[cfg(feature = "cluster")]
            cluster_controller: parking_lot::Mutex::new(None),
            state_backend: parking_lot::Mutex::new(None),
            vnode_registry: parking_lot::Mutex::new(None),
            physical_optimizer_rules: physical_rules.into(),
            pipeline_target_partitions: target_partitions,
            #[cfg(feature = "cluster")]
            shuffle_sender: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            shuffle_receiver: Arc::new(parking_lot::Mutex::new(None)),
            #[cfg(feature = "cluster")]
            decision_store: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            assignment_snapshot_store: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            catalog_manifest_store: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            rehydrated_vnode_state: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            force_ckpt_tx: parking_lot::Mutex::new(None),
            subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
            #[cfg(feature = "cluster")]
            active_subs: Arc::new(parking_lot::RwLock::new(std::collections::HashMap::new())),
            stream_schemas: parking_lot::RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// Install the AI subsystem and the runtime handle its inference workers
    /// spawn on. Called by the builder before the engine is shared; the handle
    /// must be the main multi-threaded runtime.
    pub(crate) fn set_ai_runtime(
        &mut self,
        runtime: Arc<crate::ai::AiRuntime>,
        handle: tokio::runtime::Handle,
    ) {
        // Register the laminar.models / laminar.ai_calls catalog views. A
        // failure here is non-fatal — inference still works, the views just
        // aren't queryable.
        if let Err(e) = crate::ai_catalog::register_ai_catalog(&self.ctx, &runtime) {
            tracing::warn!(error = %e, "failed to register laminar.* AI catalog views");
        }
        self.ai_runtime = Some(runtime);
        self.ai_handle = Some(handle);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_sender(&self, sender: Arc<laminar_core::shuffle::ShuffleSender>) {
        *self.shuffle_sender.lock() = Some(sender);
        self.update_sql_cluster_context();
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_receiver(
        &self,
        receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) {
        *self.shuffle_receiver.lock() = Some(receiver);
        self.update_sql_cluster_context();
    }

    #[cfg(feature = "cluster")]
    fn update_sql_cluster_context(&self) {
        if let (Some(registry), Some(sender), Some(receiver)) = (
            self.vnode_registry.lock().as_ref(),
            self.shuffle_sender.lock().as_ref(),
            self.shuffle_receiver.lock().as_ref(),
        ) {
            let self_id = self
                .cluster_controller
                .lock()
                .as_ref()
                .map_or(laminar_core::state::NodeId(0), |c| {
                    laminar_core::state::NodeId(c.instance_id().0)
                });
            laminar_sql::datafusion::cluster_repartition::set_cluster_context(
                Arc::clone(registry),
                Arc::clone(sender),
                Arc::clone(receiver),
                self_id,
            );
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_decision_store(
        &self,
        store: Arc<laminar_core::cluster::control::CheckpointDecisionStore>,
    ) {
        *self.decision_store.lock() = Some(store);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_assignment_snapshot_store(
        &self,
        store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    ) {
        *self.assignment_snapshot_store.lock() = Some(store);
    }

    /// Install the shared catalog manifest store. Enables catalog-manifest
    /// persistence (on checkpoint) and boot-time replay.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_catalog_manifest_store(
        &self,
        store: Arc<laminar_core::cluster::control::CatalogManifestStore>,
    ) {
        *self.catalog_manifest_store.lock() = Some(store);
    }

    /// Publish this node's current catalog DDL to the shared
    /// `catalog/manifest.json`. Best-effort and cluster-only — a failure is
    /// logged, never propagated, since the manifest is an availability aid,
    /// not a correctness gate. Called after each successful checkpoint.
    #[cfg(feature = "cluster")]
    pub(crate) async fn persist_catalog_manifest(&self) {
        let Some(store) = self.catalog_manifest_store.lock().clone() else {
            return;
        };
        let entries: Vec<laminar_core::cluster::control::CatalogManifestEntry> = self
            .connector_manager
            .lock()
            .ordered_ddl()
            .into_iter()
            .map(|(name, ddl)| laminar_core::cluster::control::CatalogManifestEntry { name, ddl })
            .collect();
        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as i64);
        let manifest = laminar_core::cluster::control::CatalogManifest {
            // Wall-clock as a monotonic-in-practice diagnostic version; the
            // object is overwritten in place, so the value is informational.
            version: now_ms.unsigned_abs(),
            updated_at_ms: now_ms,
            entries,
        };
        if let Err(e) = store.save(&manifest).await {
            tracing::warn!(error = %e, "catalog manifest persist failed");
        }
    }

    /// Replay catalog DDL from the shared `catalog/manifest.json`, recreating
    /// any object this node doesn't already have locally. Best-effort and
    /// cluster-only; runs at boot before the pipeline starts so the operator
    /// graph for rebalanced-in MVs exists. A per-entry replay failure is
    /// logged and skipped — the node still serves the objects that did rebuild.
    #[cfg(feature = "cluster")]
    pub(crate) async fn restore_catalog_from_manifest(&self) {
        let Some(store) = self.catalog_manifest_store.lock().clone() else {
            return;
        };
        let manifest = match store.load().await {
            Ok(Some(m)) => m,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, "catalog manifest load failed — skipping replay");
                return;
            }
        };
        // Detach the manifest store during replay: `execute` otherwise
        // re-persists this node's still-partial catalog after every DDL, so a
        // mid-replay failure would truncate the shared manifest. Reinstalled below.
        let detached = self.catalog_manifest_store.lock().take();
        for entry in &manifest.entries {
            if self.catalog_object_exists(&entry.name) {
                continue;
            }
            match self.execute(&entry.ddl).await {
                Ok(_) => tracing::info!(name = %entry.name, "replayed catalog DDL from manifest"),
                Err(e) => tracing::warn!(
                    name = %entry.name, error = %e,
                    "catalog manifest replay failed for object"
                ),
            }
        }
        *self.catalog_manifest_store.lock() = detached;
    }

    /// Whether a catalog object with `name` is already registered locally
    /// (any of source/sink/stream/table). Drives idempotent manifest replay.
    #[cfg(feature = "cluster")]
    fn catalog_object_exists(&self, name: &str) -> bool {
        let mgr = self.connector_manager.lock();
        mgr.sources().contains_key(name)
            || mgr.sinks().contains_key(name)
            || mgr.streams().contains_key(name)
            || mgr.tables().contains_key(name)
    }

    /// Adopt a new vnode assignment snapshot atomically across the registry,
    /// state-backend fence, and coordinator, then rehydrate committed state
    /// for any vnodes this node newly acquired. Idempotent for versions ≤ the
    /// current registry version.
    ///
    /// Rehydration (the durable read of newly-acquired vnodes' partials) runs
    /// *after* the coordinator lock is released so a slow object store can't
    /// stall the checkpoint cadence; the recovered bytes are staged in
    /// [`Self::rehydrated_vnode_state`] for operators to drain.
    #[cfg(feature = "cluster")]
    pub async fn adopt_assignment_snapshot(
        &self,
        snapshot: laminar_core::cluster::control::AssignmentSnapshot,
    ) -> SnapshotAdoption {
        let Some(registry) = self.vnode_registry.lock().clone() else {
            return SnapshotAdoption::default();
        };
        if snapshot.version <= registry.assignment_version() {
            return SnapshotAdoption {
                adopted: false,
                version: snapshot.version,
                ..SnapshotAdoption::default()
            };
        }
        let vnode_count = registry.vnode_count();
        let new_assignment: Arc<[laminar_core::state::NodeId]> =
            snapshot.to_vnode_vec(vnode_count).into();

        let self_id = self
            .cluster_controller
            .lock()
            .as_ref()
            .map_or(laminar_core::state::NodeId(0), |c| {
                laminar_core::state::NodeId(c.instance_id().0)
            });

        // Hold the coord mutex so registry + fence updates land between
        // epochs. Snapshot the owned set on both sides of the swap to
        // derive exactly which vnodes this node gained.
        let mut guard = self.coordinator.lock().await;
        let old_owned = laminar_core::state::owned_vnodes(&registry, self_id);
        registry.set_assignment_and_version(new_assignment, snapshot.version);
        let new_owned = laminar_core::state::owned_vnodes(&registry, self_id);
        if let Some(backend) = self.state_backend.lock().clone() {
            backend.set_authoritative_version(snapshot.version);
        }
        if let Some(coord) = guard.as_mut() {
            coord.set_assignment_version(snapshot.version);
            coord.set_vnode_set(new_owned.clone());
            coord.set_gate_vnode_set((0..vnode_count).collect());
        }
        drop(guard);

        // Newly-acquired vnodes = new \ old.
        let old_set: std::collections::HashSet<u32> = old_owned.into_iter().collect();
        let newly_acquired: Vec<u32> = new_owned
            .into_iter()
            .filter(|v| !old_set.contains(v))
            .collect();

        // Mark every newly-acquired vnode `Restoring` up front — before the
        // (possibly slow) durable read below — so the operator suppresses
        // emission for their keys from the moment the shuffle starts routing
        // their rows here. Vnodes with no durable state are flipped back to
        // `Active` immediately after the read; the ones we stage stay
        // `Restoring` until the graph merges their state in.
        if !newly_acquired.is_empty() {
            registry.mark_restoring(&newly_acquired);
        }

        let mut adoption = SnapshotAdoption {
            adopted: true,
            version: snapshot.version,
            newly_acquired: newly_acquired.clone(),
            rehydrated: 0,
            rehydration_epoch: None,
        };

        // Pull the last committed state for the gained vnodes off the
        // shared durable backend so they don't resume from empty state.
        // Clone the Arc out first so the (non-Send) lock guard is dropped
        // before the rehydration await.
        let backend = self.state_backend.lock().clone();
        if let (false, Some(backend)) = (newly_acquired.is_empty(), backend) {
            let report = crate::recovery_manager::VnodeRehydrator::new(backend.as_ref())
                .rehydrate(&newly_acquired)
                .await;
            adoption.rehydrated = report.restored.len();
            adoption.rehydration_epoch = report.epoch;
            // Vnodes with no durable state to apply serve immediately — flip
            // them back to `Active` so their emission isn't gated forever. The
            // graph flips the staged ones once it merges their state in.
            let no_state: Vec<u32> = newly_acquired
                .iter()
                .copied()
                .filter(|v| !report.restored.contains_key(v))
                .collect();
            if !no_state.is_empty() {
                registry.mark_active(&no_state);
            }
            if let Some(epoch) = report.epoch {
                let mut staged = self.rehydrated_vnode_state.lock();
                for (vnode, bytes) in report.restored {
                    staged.insert(vnode, RehydratedVnode { epoch, bytes });
                }
            }
        } else if !newly_acquired.is_empty() {
            // No backend / nothing to rehydrate — clear the Restoring marks we
            // optimistically set so emission isn't gated with no state coming.
            registry.mark_active(&newly_acquired);
        }

        tracing::info!(
            version = snapshot.version,
            newly_acquired = adoption.newly_acquired.len(),
            rehydrated = adoption.rehydrated,
            rehydration_epoch = ?adoption.rehydration_epoch,
            "adopted assignment snapshot",
        );
        adoption
    }

    /// Snapshot of committed per-vnode state staged for newly-acquired
    /// vnodes during the most recent rebalance adoptions. Keyed by vnode.
    /// Drained by operators that adopt the per-vnode state backend; exposed
    /// for inspection and tests.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn rehydrated_vnode_state(&self) -> HashMap<u32, RehydratedVnode> {
        self.rehydrated_vnode_state.lock().clone()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_cluster_controller(
        &self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        // Serve this node's local MV/table rows to peers. Weak store handles
        // (not `self`) avoid a reference cycle.
        controller.register_query_handler(Arc::new(DbQueryHandler {
            mv_store: Arc::downgrade(&self.mv_store),
            table_store: Arc::downgrade(&self.table_store),
            // Isolated context: a pushed `filter_sql` is compiled with only its
            // temp table visible, so it can't reference other registered tables.
            filter_ctx: SessionContext::new(),
        }));
        self.spawn_subscription_router(&controller);
        *self.cluster_controller.lock() = Some(controller);
        self.update_sql_cluster_context();
    }

    /// Router task: refreshes the gossip interest cache (cadence backs off when
    /// idle) and drains remote `__sub::` batches to local subscribers. Weak
    /// handles, so it exits once the `LaminarDB` is dropped.
    #[cfg(feature = "cluster")]
    fn spawn_subscription_router(
        &self,
        controller: &Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        use std::collections::{HashMap, HashSet};

        // Needs a running runtime; some unit tests install a controller without
        // one. Skip — distributed routing is server-mode only.
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }

        let kv = Arc::clone(controller.kv());

        // Skip entirely on backends that can't discover subscription interest
        // (e.g. object store), rather than advertising interest nothing reads.
        if !kv.supports_subscription_routing() {
            tracing::info!(
                "distributed SUBSCRIBE routing disabled: coordination backend has \
                 no subscription-interest discovery"
            );
            return;
        }

        let active_subs = Arc::downgrade(&self.active_subs);
        let subscription_registry = Arc::downgrade(&self.subscription_registry);
        let shuffle_receiver = Arc::downgrade(&self.shuffle_receiver);

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(SUB_ROUTER_TICK);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut advertised: HashSet<String> = HashSet::new();
            let mut until_refresh: u64 = 0;
            loop {
                tick.tick().await;

                // Upgrade weak handles; bail once the DB has been dropped.
                let (Some(active_subs), Some(registry), Some(receiver_slot)) = (
                    active_subs.upgrade(),
                    subscription_registry.upgrade(),
                    shuffle_receiver.upgrade(),
                ) else {
                    break;
                };

                let local_names = registry.active_subscription_names();

                // Refresh the producer-side interest cache when due.
                if until_refresh == 0 {
                    let mut map: HashMap<String, HashSet<u64>> = HashMap::new();
                    for (node_id, key, value) in kv.scan_prefix("sub:").await {
                        if !value.is_empty() {
                            if let Some(name) = key.strip_prefix("sub:") {
                                map.entry(name.to_string()).or_default().insert(node_id.0);
                            }
                        }
                    }
                    let idle = map.is_empty() && local_names.is_empty();
                    *active_subs.write() = map;
                    until_refresh = if idle {
                        SUB_REFRESH_IDLE_TICKS
                    } else {
                        SUB_REFRESH_ACTIVE_TICKS
                    };
                }
                until_refresh = until_refresh.saturating_sub(1);

                // Advertise newly-added / clear newly-removed local interests
                // (rare; only on subscription lifecycle changes).
                for name in &local_names {
                    if advertised.insert(name.clone()) {
                        kv.write(&format!("sub:{name}"), "active".to_string()).await;
                    }
                }
                let removed: Vec<String> = advertised
                    .iter()
                    .filter(|n| !local_names.contains(*n))
                    .cloned()
                    .collect();
                for name in removed {
                    kv.write(&format!("sub:{name}"), String::new()).await;
                    advertised.remove(&name);
                }

                // Publish remote batches for locally-subscribed streams; one lock
                // cycle drains every `__sub::` stage (dropped subs fall through
                // `send_batch` as a no-op rather than piling up in `staged`).
                if !local_names.is_empty() {
                    let receiver = receiver_slot.lock().clone();
                    if let Some(receiver) = receiver {
                        for (stage, batches) in receiver
                            .drain_staged_with_prefix(crate::subscription::REMOTE_STAGE_PREFIX)
                        {
                            if let Some(name) =
                                crate::subscription::stream_from_remote_stage(&stage)
                            {
                                for batch in batches {
                                    registry.send_batch(name, batch);
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    pub(crate) fn set_state_backend(&self, backend: Arc<dyn laminar_core::state::StateBackend>) {
        *self.state_backend.lock() = Some(backend);
    }

    pub(crate) fn set_vnode_registry(&self, registry: Arc<laminar_core::state::VnodeRegistry>) {
        *self.vnode_registry.lock() = Some(registry);
        #[cfg(feature = "cluster")]
        self.update_sql_cluster_context();
    }

    /// The underlying `DataFusion` `SessionContext`.
    #[must_use]
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Returns a fluent builder for constructing a [`LaminarDB`].
    #[must_use]
    pub fn builder() -> LaminarDbBuilder {
        LaminarDbBuilder::new()
    }

    /// Register built-in connectors based on enabled features.
    #[allow(unused_variables)]
    fn register_builtin_connectors(registry: &laminar_connectors::registry::ConnectorRegistry) {
        #[cfg(feature = "kafka")]
        {
            laminar_connectors::kafka::register_kafka_source(registry);
            laminar_connectors::kafka::register_kafka_sink(registry);
        }
        #[cfg(feature = "postgres-cdc")]
        {
            laminar_connectors::cdc::postgres::register_postgres_cdc_source(registry);
        }
        #[cfg(feature = "postgres-sink")]
        {
            laminar_connectors::postgres::register_postgres_sink(registry);
        }
        #[cfg(feature = "delta-lake")]
        {
            laminar_connectors::lakehouse::register_delta_lake_sink(registry);
            laminar_connectors::lakehouse::register_delta_lake_source(registry);
        }
        #[cfg(feature = "iceberg")]
        {
            laminar_connectors::lakehouse::register_iceberg_sink(registry);
            laminar_connectors::lakehouse::register_iceberg_source(registry);
        }
        #[cfg(feature = "websocket")]
        {
            laminar_connectors::websocket::register_websocket_source(registry);
            laminar_connectors::websocket::register_websocket_sink(registry);
        }
        #[cfg(feature = "mysql-cdc")]
        {
            laminar_connectors::cdc::mysql::register_mysql_cdc_source(registry);
        }
        #[cfg(feature = "mongodb-cdc")]
        {
            laminar_connectors::mongodb::register_mongodb_cdc_source(registry);
            laminar_connectors::mongodb::register_mongodb_sink(registry);
        }
        #[cfg(feature = "files")]
        {
            laminar_connectors::files::register_file_source(registry);
            laminar_connectors::files::register_file_sink(registry);
        }
        #[cfg(feature = "otel")]
        {
            laminar_connectors::otel::register_otel_source(registry);
        }
        #[cfg(feature = "nats")]
        {
            laminar_connectors::nats::register_nats_source(registry);
            laminar_connectors::nats::register_nats_sink(registry);
        }
    }

    /// Handle `CREATE LOOKUP TABLE` by registering the table in the
    /// `TableStore`, `ConnectorManager`, `DataFusion` catalog, and lookup
    /// registry.
    fn handle_register_lookup_table(
        &self,
        info: laminar_sql::planner::LookupTableInfo,
    ) -> Result<ExecuteResult, DbError> {
        use laminar_sql::parser::lookup_table::ConnectorType;

        if info.primary_key.len() != 1 {
            return Err(DbError::InvalidOperation(
                "Lookup table requires a single-column primary key".into(),
            ));
        }
        let pk = info.primary_key[0].clone();

        // Register in TableStore for PK-based upsert
        let cache_mode = info.properties.cache_memory.map(|mem| {
            let max_entries = cache_entries_from_memory(mem);
            crate::table_cache_mode::TableCacheMode::Partial { max_entries }
        });
        if let Some(cache) = cache_mode {
            self.table_store.write().create_table_with_cache(
                &info.name,
                info.arrow_schema.clone(),
                &pk,
                cache,
            )?;
        } else {
            self.table_store
                .write()
                .create_table(&info.name, info.arrow_schema.clone(), &pk)?;
        }

        // For external connectors: register in ConnectorManager so
        // start_connector_pipeline() handles snapshot + CDC loading
        if !matches!(info.properties.connector, ConnectorType::Static) {
            self.register_lookup_connector(&info, &pk)?;
        }

        // Register in DataFusion for SELECT/JOIN queries
        {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                info.name.clone(),
                info.arrow_schema.clone(),
                self.table_store.clone(),
            );
            let _ = self.ctx.deregister_table(&info.name);
            self.ctx
                .register_table(&info.name, Arc::new(provider))
                .map_err(|e| {
                    DbError::InvalidOperation(format!("Failed to register lookup table: {e}"))
                })?;
        }

        // Register snapshot in the lookup registry so the physical
        // planner can build LookupJoinExec nodes for JOIN queries.
        if let Some(batch) = self.table_store.read().to_record_batch(&info.name) {
            self.lookup_registry.register(
                &info.name,
                laminar_sql::datafusion::LookupSnapshot { batch },
            );
        }

        // Register the logical optimizer rule so JOINs referencing
        // this table are rewritten to LookupJoinNode.
        self.refresh_lookup_optimizer_rule();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE LOOKUP TABLE".to_string(),
            object_name: info.name,
        }))
    }

    /// Register an external connector for a lookup table in the
    /// `ConnectorManager` and `TableStore`.
    #[allow(clippy::unnecessary_wraps)]
    fn register_lookup_connector(
        &self,
        info: &laminar_sql::planner::LookupTableInfo,
        pk: &str,
    ) -> Result<(), DbError> {
        use laminar_sql::parser::lookup_table::ConnectorType;

        let connector_type_str = match &info.properties.connector {
            ConnectorType::Postgres => "postgres",
            ConnectorType::PostgresCdc => "postgres-cdc",
            ConnectorType::MysqlCdc => "mysql-cdc",
            ConnectorType::Redis => "redis",
            ConnectorType::S3Parquet => "s3-parquet",
            ConnectorType::DeltaLake => "delta-lake",
            ConnectorType::Custom(s) => s.as_str(),
            ConnectorType::Static => unreachable!(),
        };

        self.table_store
            .write()
            .set_connector(&info.name, connector_type_str);

        let refresh = match info.properties.strategy {
            laminar_sql::parser::lookup_table::LookupStrategy::Replicated
            | laminar_sql::parser::lookup_table::LookupStrategy::Partitioned => {
                // Standalone postgres uses snapshot-only (no CDC slot needed).
                if matches!(info.properties.connector, ConnectorType::Postgres) {
                    Some(laminar_connectors::reference::RefreshMode::SnapshotOnly)
                } else {
                    Some(laminar_connectors::reference::RefreshMode::SnapshotPlusCdc)
                }
            }
            laminar_sql::parser::lookup_table::LookupStrategy::OnDemand => {
                Some(laminar_connectors::reference::RefreshMode::Manual)
            }
        };

        // Build connector options and format options from raw WITH clause.
        // Keys consumed by LookupTableProperties are excluded; keys starting
        // with "format." are routed to format_options (prefix stripped).
        let consumed = [
            "connector",
            "strategy",
            "cache.memory",
            "cache.disk",
            "cache.ttl",
            "pushdown",
            "format",
        ];
        let mut connector_options = HashMap::with_capacity(info.raw_options.len());
        let mut format_options = HashMap::with_capacity(4);
        for (k, v) in &info.raw_options {
            let lower = k.to_lowercase();
            if consumed.contains(&lower.as_str()) {
                continue;
            }
            if let Some(suffix) = lower.strip_prefix("format.") {
                format_options.insert(suffix.to_string(), v.clone());
            } else {
                connector_options.insert(k.clone(), v.clone());
            }
        }

        // The on-demand lookup cache is byte-weighted; carry the user's
        // cache_memory budget through as bytes (no lossy entry-count conversion).
        let cache_max_bytes = info
            .properties
            .cache_memory
            .map(|m| usize::try_from(m.as_bytes()).unwrap_or(usize::MAX));

        // cache_ttl is specified in seconds; carry it as a Duration so the
        // partial lookup cache expires stale entries lazily on read.
        let cache_ttl = info
            .properties
            .cache_ttl
            .map(std::time::Duration::from_secs);

        self.connector_manager
            .lock()
            .register_table(crate::connector_manager::TableRegistration {
                name: info.name.clone(),
                primary_key: pk.to_string(),
                connector_type: Some(connector_type_str.to_string()),
                connector_options,
                format: info.raw_options.get("format").cloned(),
                format_options,
                refresh,
                cache_max_bytes,
                cache_ttl,
            });

        Ok(())
    }

    /// Replaces the `LookupJoinRewriteRule` on the `DataFusion` context
    /// with one that knows the current set of registered lookup tables.
    fn refresh_lookup_optimizer_rule(&self) {
        use laminar_sql::planner::lookup_join::{LookupColumnPruningRule, LookupJoinRewriteRule};
        use laminar_sql::planner::predicate_split::{
            PlanPushdownMode, PlanSourceCapabilities, PredicateSplitterRule,
            SourceCapabilitiesRegistry,
        };

        // Remove old rules if present
        self.ctx.remove_optimizer_rule("lookup_join_rewrite");
        self.ctx.remove_optimizer_rule("predicate_splitter");
        self.ctx.remove_optimizer_rule("lookup_column_pruning");

        let tables = self.planner.lock().lookup_tables_cloned();
        if tables.is_empty() {
            return;
        }

        // Build capabilities registry from table properties
        let mut caps_registry = SourceCapabilitiesRegistry::default();
        for (name, info) in &tables {
            let mode = match info.properties.pushdown_mode {
                laminar_sql::parser::lookup_table::PushdownMode::Enabled
                | laminar_sql::parser::lookup_table::PushdownMode::Auto => PlanPushdownMode::Full,
                laminar_sql::parser::lookup_table::PushdownMode::Disabled => PlanPushdownMode::None,
            };
            let pk_set: std::collections::HashSet<String> =
                info.primary_key.iter().cloned().collect();
            caps_registry.register(
                name.clone(),
                PlanSourceCapabilities {
                    pushdown_mode: mode,
                    eq_columns: pk_set,
                    range_columns: std::collections::HashSet::new(),
                    in_columns: std::collections::HashSet::new(),
                    supports_null_check: false,
                },
            );
        }

        // Register rules in order: rewrite → predicate split → column pruning
        self.ctx
            .add_optimizer_rule(Arc::new(LookupJoinRewriteRule::new(tables)));
        self.ctx
            .add_optimizer_rule(Arc::new(PredicateSplitterRule::new(caps_registry)));
        self.ctx
            .add_optimizer_rule(Arc::new(LookupColumnPruningRule));
    }

    /// Returns the connector registry for registering custom connectors.
    ///
    /// Use this to register user-defined source/sink connectors before
    /// calling `start()`.
    #[must_use]
    pub fn connector_registry(&self) -> &laminar_connectors::registry::ConnectorRegistry {
        &self.connector_registry
    }

    /// Register a custom scalar UDF on the `SessionContext`.
    ///
    /// Called by `LaminarDbBuilder::build()` after construction.
    pub(crate) fn register_custom_udf(&self, udf: datafusion_expr::ScalarUDF) {
        self.ctx.register_udf(udf);
    }

    /// Register a custom aggregate UDF (UDAF) on the `SessionContext`.
    ///
    /// Called by `LaminarDbBuilder::build()` after construction.
    pub(crate) fn register_custom_udaf(&self, udaf: datafusion_expr::AggregateUDF) {
        self.ctx.register_udaf(udaf);
    }

    /// Registers a Delta Lake table as a `DataFusion` `TableProvider`.
    ///
    /// After registration, the table can be queried via SQL:
    /// ```sql
    /// SELECT * FROM my_delta_table WHERE id > 100
    /// ```
    ///
    /// # Arguments
    ///
    /// * `name` - SQL table name (e.g., `"trades"`)
    /// * `table_uri` - Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`)
    /// * `storage_options` - Storage credentials and configuration
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the table cannot be opened or registered.
    #[cfg(feature = "delta-lake")]
    pub async fn register_delta_table(
        &self,
        name: &str,
        table_uri: &str,
        storage_options: HashMap<String, String>,
    ) -> Result<(), DbError> {
        laminar_connectors::lakehouse::delta_table_provider::register_delta_table(
            &self.ctx,
            name,
            table_uri,
            storage_options,
        )
        .await
        .map_err(DbError::from)
    }

    /// Execute a SQL statement.
    ///
    /// Supports:
    /// - `CREATE SOURCE` / `CREATE SINK` — registers sources and sinks
    /// - `DROP SOURCE` / `DROP SINK` — removes sources and sinks
    /// - `SHOW SOURCES` / `SHOW SINKS` / `SHOW QUERIES` — list registered objects
    /// - `DESCRIBE source_name` — show source schema
    /// - `SELECT ...` — execute a streaming query
    /// - `INSERT INTO source_name VALUES (...)` — insert data
    /// - `CREATE MATERIALIZED VIEW` — create a streaming materialized view
    /// - `EXPLAIN SELECT ...` — show query plan
    ///
    /// # Errors
    ///
    /// Returns `DbError` if SQL parsing, planning, or execution fails.
    pub async fn execute(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::Shutdown);
        }

        // Apply config variable substitution
        let resolved = if self.config_vars.is_empty() {
            sql.to_string()
        } else {
            sql_utils::resolve_config_vars(sql, &self.config_vars, true)?
        };

        // Split into multiple statements
        let stmts = sql_utils::split_statements(&resolved);
        if stmts.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        // Execute each statement, return the last result (or first error)
        let mut last_result = None;
        for stmt_sql in &stmts {
            last_result = Some(self.execute_single(stmt_sql).await?);
        }

        last_result.ok_or_else(|| DbError::InvalidOperation("Empty SQL statement".into()))
    }

    /// Execute a single SQL statement.
    #[allow(clippy::too_many_lines)]
    async fn execute_single(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let statements = parse_streaming_sql(sql)?;

        if statements.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        let statement = &statements[0];

        let result = match statement {
            StreamingStatement::CreateSource(create) => {
                let result = self.handle_create_source(create).await?;
                if let ExecuteResult::Ddl(ref info) = result {
                    self.connector_manager
                        .lock()
                        .store_ddl(&info.object_name, sql);
                }
                Ok(result)
            }
            StreamingStatement::CreateSink(create) => {
                let result = self.handle_create_sink(create)?;
                if let ExecuteResult::Ddl(ref info) = result {
                    self.connector_manager
                        .lock()
                        .store_ddl(&info.object_name, sql);
                }
                Ok(result)
            }
            StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                query_sql,
                retention_bytes,
                ..
            } => {
                let result = self
                    .handle_create_stream(
                        name,
                        query,
                        emit_clause.as_ref(),
                        query_sql,
                        *retention_bytes,
                    )
                    .await?;
                if let ExecuteResult::Ddl(ref info) = result {
                    self.connector_manager
                        .lock()
                        .store_ddl(&info.object_name, sql);
                }
                Ok(result)
            }
            StreamingStatement::CreateContinuousQuery { .. }
            | StreamingStatement::CreateLookupTable(_)
            | StreamingStatement::DropLookupTable { .. } => self.handle_query(sql).await,
            StreamingStatement::Standard(stmt) => {
                if let sqlparser::ast::Statement::CreateTable(ct) = stmt.as_ref() {
                    let result = self.handle_create_table(ct)?;
                    if let ExecuteResult::Ddl(ref info) = result {
                        self.connector_manager
                            .lock()
                            .store_ddl(&info.object_name, sql);
                    }
                    Ok(result)
                } else if let sqlparser::ast::Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    names,
                    if_exists,
                    ..
                } = stmt.as_ref()
                {
                    self.handle_drop_table(names, *if_exists)
                } else if let sqlparser::ast::Statement::Set(set_stmt) = stmt.as_ref() {
                    self.handle_set(set_stmt)
                } else {
                    self.handle_query(sql).await
                }
            }
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => self.handle_insert_into(table_name, columns, values).await,
            StreamingStatement::DropSource {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_source(name, *if_exists, *cascade),
            StreamingStatement::DropSink {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_sink(name, *if_exists, *cascade),
            StreamingStatement::DropStream {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_stream(name, *if_exists, *cascade),
            StreamingStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            } => self.handle_drop_materialized_view(name, *if_exists, *cascade),
            StreamingStatement::Show(cmd) => {
                let batch = match cmd {
                    ShowCommand::Sources => self.build_show_sources(),
                    ShowCommand::Sinks => self.build_show_sinks(),
                    ShowCommand::Queries => self.build_show_queries(),
                    ShowCommand::MaterializedViews => self.build_show_materialized_views(),
                    ShowCommand::Streams => self.build_show_streams(),
                    ShowCommand::Tables => self.build_show_tables(),
                    ShowCommand::CheckpointStatus => self.build_show_checkpoint_status().await?,
                    ShowCommand::CreateSource { name } => {
                        self.build_show_create_source(&name.to_string())?
                    }
                    ShowCommand::CreateSink { name } => {
                        self.build_show_create_sink(&name.to_string())?
                    }
                };
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Checkpoint => {
                let result = self.checkpoint().await?;
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "CHECKPOINT".to_string(),
                    object_name: format!("checkpoint_{}", result.checkpoint_id),
                }))
            }
            StreamingStatement::RestoreCheckpoint { checkpoint_id } => {
                self.handle_restore_checkpoint(*checkpoint_id)
            }
            StreamingStatement::Describe { name, .. } => {
                let name_str = name.to_string();
                let batch = self.build_describe(&name_str)?;
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Explain {
                statement, analyze, ..
            } => {
                if *analyze {
                    self.handle_explain_analyze(statement, sql).await
                } else {
                    self.handle_explain(statement)
                }
            }
            StreamingStatement::CreateMaterializedView {
                name,
                query,
                emit_clause,
                or_replace,
                if_not_exists,
                query_sql,
                ..
            } => {
                let result = self
                    .handle_create_materialized_view(
                        sql,
                        name,
                        query,
                        emit_clause.clone(),
                        *or_replace,
                        *if_not_exists,
                        query_sql,
                    )
                    .await?;
                if let ExecuteResult::Ddl(ref info) = result {
                    self.connector_manager
                        .lock()
                        .store_ddl(&info.object_name, sql);
                }
                Ok(result)
            }
            StreamingStatement::AlterSource { name, operation } => {
                self.handle_alter_source(name, operation)
            }
            StreamingStatement::Subscribe(_) => Err(DbError::InvalidOperation(
                "SUBSCRIBE requires the pgwire endpoint, not HTTP /api/v1/sql".into(),
            )),
            StreamingStatement::DeclareCursorForSubscribe { .. } => Err(DbError::InvalidOperation(
                "DECLARE CURSOR FOR SUBSCRIBE requires the pgwire endpoint, not HTTP /api/v1/sql"
                    .into(),
            )),
        };

        #[cfg(feature = "cluster")]
        if let Ok(ExecuteResult::Ddl(ref info)) = &result {
            if info.statement_type != "CHECKPOINT" {
                self.persist_catalog_manifest().await;
            }
        }

        result
    }

    /// Handle INSERT INTO statement.
    ///
    /// Inserts SQL VALUES into a registered source, a `TableStore`-managed
    /// table (with PK upsert), or a plain `DataFusion` `MemTable`.
    async fn handle_insert_into(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        _columns: &[sqlparser::ast::Ident],
        values: &[Vec<sqlparser::ast::Expr>],
    ) -> Result<ExecuteResult, DbError> {
        let name = table_name.to_string();

        // Try inserting into a registered source
        if let Some(entry) = self.catalog.get_source(&name) {
            let batch = sql_utils::sql_values_to_record_batch(&entry.schema, values)?;
            entry
                .push_and_buffer(batch)
                .map_err(|e| DbError::InsertError(format!("Failed to push to source: {e}")))?;
            return Ok(ExecuteResult::RowsAffected(values.len() as u64));
        }

        // Try inserting into a TableStore-managed table (with PK upsert).
        // Single lock scope avoids TOCTOU race between has_table/schema/upsert.
        {
            let mut ts = self.table_store.write();
            if ts.has_table(&name) {
                let schema = ts
                    .table_schema(&name)
                    .ok_or_else(|| DbError::TableNotFound(name.clone()))?;
                let batch = sql_utils::sql_values_to_record_batch(&schema, values)?;
                ts.upsert(&name, &batch)?;
                drop(ts); // release before sync (which may also lock)

                self.sync_table_to_datafusion(&name)?;
                return Ok(ExecuteResult::RowsAffected(values.len() as u64));
            }
        }

        // Otherwise, insert into a DataFusion MemTable
        // Look up the table provider
        let table = self
            .ctx
            .table_provider(&name)
            .await
            .map_err(|_| DbError::TableNotFound(name.clone()))?;

        let schema = table.schema();
        let batch = sql_utils::sql_values_to_record_batch(&schema, values)?;

        // Deregister the old table, then re-register with the new data
        self.ctx
            .deregister_table(&name)
            .map_err(|e| DbError::InsertError(format!("Failed to deregister table: {e}")))?;

        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![batch]])
                .map_err(|e| DbError::InsertError(format!("Failed to create table: {e}")))?;

        self.ctx
            .register_table(&name, Arc::new(mem_table))
            .map_err(|e| DbError::InsertError(format!("Failed to register table: {e}")))?;

        Ok(ExecuteResult::RowsAffected(values.len() as u64))
    }

    /// Handle RESTORE FROM CHECKPOINT statement (not yet implemented).
    ///
    /// Will eventually stop the pipeline, reload state from the checkpoint
    /// manifest, seek source offsets, and restart the pipeline.
    #[allow(clippy::unused_self)] // will use self when restore is implemented
    fn handle_restore_checkpoint(&self, _checkpoint_id: u64) -> Result<ExecuteResult, DbError> {
        Err(DbError::Unsupported(
            "RESTORE FROM CHECKPOINT is not yet implemented — \
             requires pipeline stop, state reload from manifest, \
             source offset seek, and pipeline restart"
                .to_string(),
        ))
    }

    /// Get a session property value.
    #[must_use]
    pub fn get_session_property(&self, key: &str) -> Option<String> {
        self.session_properties
            .lock()
            .get(&key.to_lowercase())
            .cloned()
    }

    /// Get all session properties.
    #[must_use]
    pub fn session_properties(&self) -> HashMap<String, String> {
        self.session_properties.lock().clone()
    }

    /// Set a session property. Keys are lowercased.
    pub fn set_session_property(&self, key: &str, value: &str) {
        self.session_properties
            .lock()
            .insert(key.to_lowercase(), value.to_string());
    }

    /// Subscribe to a named stream or materialized view.
    ///
    /// # Errors
    ///
    /// Returns `DbError::StreamNotFound` if the stream is not registered.
    pub fn subscribe<T: crate::handle::FromBatch>(
        &self,
        name: &str,
    ) -> Result<crate::handle::TypedSubscription<T>, DbError> {
        let sub = self
            .catalog
            .get_stream_subscription(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_string()))?;
        Ok(crate::handle::TypedSubscription::from_raw(sub))
    }

    /// Subscribe to a named stream's output.
    ///
    /// # Errors
    ///
    /// Returns `DbError::StreamNotFound` if the stream doesn't exist.
    #[cfg(feature = "api")]
    pub fn subscribe_raw(
        &self,
        name: &str,
    ) -> Result<laminar_core::streaming::Subscription<crate::catalog::ArrowRecord>, DbError> {
        self.catalog
            .get_stream_subscription(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_string()))
    }

    /// Schema a `SUBSCRIBE` against `name` would emit, plus a `filterable`
    /// flag that's `false` only when the schema came from a `StreamEntry`
    /// sink placeholder (`Schema::empty`) — a `WHERE` clause can't compile
    /// against that and must be rejected.
    ///
    /// Lookup order: MV registry → `start()`-resolved stream output →
    /// `StreamEntry` sink (placeholder). A bare SOURCE is intentionally not
    /// resolved: only streams/MVs defined over it publish to the registry, so
    /// subscribing to the source directly would block forever — it falls
    /// through to `None`, which the caller surfaces as `StreamNotFound`.
    #[must_use]
    pub fn lookup_subscription_schema(
        &self,
        name: &str,
    ) -> Option<(arrow_schema::SchemaRef, bool)> {
        if let Some(mv) = self.mv_registry.lock().get(name).cloned() {
            return Some((mv.schema, true));
        }
        if let Some(schema) = self.stream_schemas.read().get(name).cloned() {
            return Some((schema, true));
        }
        if let Some(entry) = self.catalog.get_stream_entry(name) {
            return Some((entry.sink.schema(), false));
        }
        None
    }

    /// Open a SUBSCRIBE portal against a named MV or resolved stream. A bare
    /// SOURCE is not subscribable (surfaced as `StreamNotFound`).
    /// `filter_sql` is rejected on streams (their schema is opaque).
    ///
    /// # Errors
    /// `StreamNotFound` for unknown `name`; `Pipeline` for subscriber-cap
    /// or filter-compile failures; `InvalidOperation` when `AsOfEpoch(n)`
    /// is requested but `n` is no longer retained.
    pub async fn open_subscription(
        &self,
        name: &str,
        filter_sql: Option<&str>,
        start: crate::subscription::SubscribeStart,
    ) -> Result<crate::subscription::SubscriptionPortal, DbError> {
        let attached = self.subscription_registry.subscriber_count(name);
        if attached >= crate::subscription::MAX_SUBSCRIBERS_PER_MV {
            return Err(DbError::Pipeline(format!(
                "subscriber cap reached for '{name}' ({attached}/{})",
                crate::subscription::MAX_SUBSCRIBERS_PER_MV
            )));
        }

        let (schema, filterable) = self
            .lookup_subscription_schema(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_string()))?;

        let filter = match filter_sql {
            None => None,
            Some(_) if !filterable => {
                return Err(DbError::Pipeline(format!(
                    "WHERE on '{name}' is not supported: stream output schema \
                     was not resolved (likely a planner failure at start())"
                )));
            }
            Some(sql) => Some(crate::filter_compile::compile(&self.ctx, sql, &schema).await?),
        };

        let (replay, rx) = self
            .subscription_registry
            .subscribe(name, start)
            .map_err(|e| {
                let requested = match start {
                    crate::subscription::SubscribeStart::AsOfEpoch(n) => n,
                    crate::subscription::SubscribeStart::Tail => 0,
                };
                DbError::InvalidOperation(format!(
                    "epoch {requested} for stream '{name}' is no longer retained \
                     (earliest retained is {})",
                    e.earliest_retained
                ))
            })?;
        Ok(match filter {
            Some(phys) => crate::subscription::SubscriptionPortal::open_with_filter(
                name, schema, replay, rx, phys,
            ),
            None => crate::subscription::SubscriptionPortal::open(name, schema, replay, rx),
        })
    }

    /// Handle EXPLAIN statement — show the streaming query plan.
    fn handle_explain(&self, statement: &StreamingStatement) -> Result<ExecuteResult, DbError> {
        let mut planner = self.planner.lock();

        // Plan the inner statement to extract streaming info
        let plan_result = planner.plan(statement);

        let mut rows: Vec<(String, String)> = Vec::new();

        match plan_result {
            Ok(plan) => {
                rows.push((
                    "plan_type".into(),
                    match &plan {
                        laminar_sql::planner::StreamingPlan::Query(_) => "Query",
                        laminar_sql::planner::StreamingPlan::RegisterSource(_) => "RegisterSource",
                        laminar_sql::planner::StreamingPlan::RegisterSink(_) => "RegisterSink",
                        laminar_sql::planner::StreamingPlan::Standard(_) => "Standard",
                        laminar_sql::planner::StreamingPlan::RegisterLookupTable(_) => {
                            "RegisterLookupTable"
                        }
                        laminar_sql::planner::StreamingPlan::DropLookupTable { .. } => {
                            "DropLookupTable"
                        }
                    }
                    .into(),
                ));
                match &plan {
                    laminar_sql::planner::StreamingPlan::Query(qp) => {
                        if let Some(name) = &qp.name {
                            rows.push(("query_name".into(), name.clone()));
                        }
                        if let Some(wc) = &qp.window_config {
                            rows.push(("window".into(), format!("{wc}")));
                        }
                        if let Some(jcs) = &qp.join_config {
                            if jcs.len() == 1 {
                                rows.push(("join".into(), format!("{}", jcs[0])));
                            } else {
                                for (i, jc) in jcs.iter().enumerate() {
                                    rows.push((format!("join_step_{}", i + 1), format!("{jc}")));
                                }
                            }
                        }
                        if let Some(oc) = &qp.order_config {
                            rows.push(("order_by".into(), format!("{oc:?}")));
                        }
                        if let Some(fc) = &qp.frame_config {
                            rows.push((
                                "frame_functions".into(),
                                format!("{}", fc.functions.len()),
                            ));
                        }
                        if let Some(ec) = &qp.emit_clause {
                            rows.push(("emit".into(), format!("{ec}")));
                        }
                    }
                    laminar_sql::planner::StreamingPlan::RegisterSource(info) => {
                        rows.push(("source".into(), info.name.clone()));
                    }
                    laminar_sql::planner::StreamingPlan::RegisterSink(info) => {
                        rows.push(("sink".into(), info.name.clone()));
                    }
                    laminar_sql::planner::StreamingPlan::Standard(_) => {
                        rows.push(("execution".into(), "DataFusion pass-through".into()));
                    }
                    laminar_sql::planner::StreamingPlan::RegisterLookupTable(info) => {
                        rows.push(("lookup_table".into(), info.name.clone()));
                    }
                    laminar_sql::planner::StreamingPlan::DropLookupTable { name } => {
                        rows.push(("drop_lookup_table".into(), name.clone()));
                    }
                }
            }
            Err(e) => {
                // Even if planning fails, show what we know
                rows.push(("error".into(), format!("{e}")));
                rows.push((
                    "statement".into(),
                    format!("{:?}", std::mem::discriminant(statement)),
                ));
            }
        }

        let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
        let values: Vec<&str> = rows.iter().map(|(_, v)| v.as_str()).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_key", DataType::Utf8, false),
            Field::new("plan_value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .map_err(|e| DbError::InvalidOperation(format!("explain metadata: {e}")))?;

        Ok(ExecuteResult::Metadata(batch))
    }

    /// Handle EXPLAIN ANALYZE: run the plan and collect execution metrics.
    async fn handle_explain_analyze(
        &self,
        statement: &StreamingStatement,
        original_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        // First get the normal EXPLAIN output
        let explain_result = self.handle_explain(statement)?;
        let mut rows: Vec<(String, String)> = Vec::new();

        if let ExecuteResult::Metadata(explain_batch) = &explain_result {
            let keys_col = explain_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>();
            let vals_col = explain_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>();
            if let (Some(keys), Some(vals)) = (keys_col, vals_col) {
                for i in 0..explain_batch.num_rows() {
                    rows.push((keys.value(i).to_string(), vals.value(i).to_string()));
                }
            }
        }

        // Extract the inner SQL from the original EXPLAIN ANALYZE statement
        let upper = original_sql.to_uppercase();
        let inner_start = upper.find("ANALYZE").map_or(0, |pos| pos + "ANALYZE".len());
        let inner_sql = original_sql[inner_start..].trim();

        // Try to execute the inner query via DataFusion and collect metrics
        let start = std::time::Instant::now();
        match self.ctx.sql(inner_sql).await {
            Ok(df) => match df.collect().await {
                Ok(batches) => {
                    let elapsed = start.elapsed();
                    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                    rows.push(("rows_produced".into(), total_rows.to_string()));
                    rows.push(("execution_time_ms".into(), elapsed.as_millis().to_string()));
                    rows.push(("batches_processed".into(), batches.len().to_string()));
                }
                Err(e) => {
                    let elapsed = start.elapsed();
                    rows.push(("execution_time_ms".into(), elapsed.as_millis().to_string()));
                    rows.push(("analyze_error".into(), format!("{e}")));
                }
            },
            Err(e) => {
                rows.push(("analyze_error".into(), format!("{e}")));
            }
        }

        let keys: Vec<&str> = rows.iter().map(|(k, _)| k.as_str()).collect();
        let values: Vec<&str> = rows.iter().map(|(_, v)| v.as_str()).collect();

        let schema = Arc::new(Schema::new(vec![
            Field::new("plan_key", DataType::Utf8, false),
            Field::new("plan_value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .map_err(|e| DbError::InvalidOperation(format!("explain analyze metadata: {e}")))?;

        Ok(ExecuteResult::Metadata(batch))
    }

    /// Handle a streaming or standard SQL query.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn handle_query(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        // Synchronous planning under the lock — released before any await
        let plan = {
            let statements = parse_streaming_sql(sql)?;
            if statements.is_empty() {
                return Err(DbError::InvalidOperation("Empty SQL statement".into()));
            }
            let mut planner = self.planner.lock();
            planner
                .plan(&statements[0])
                .map_err(laminar_sql::Error::from)?
        };

        match plan {
            laminar_sql::planner::StreamingPlan::RegisterSource(info) => {
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DDL".to_string(),
                    object_name: info.name,
                }))
            }
            laminar_sql::planner::StreamingPlan::RegisterSink(info) => {
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DDL".to_string(),
                    object_name: info.name,
                }))
            }
            laminar_sql::planner::StreamingPlan::Query(query_plan) => {
                // Check for ASOF join — DataFusion can't parse ASOF syntax
                if let Some(asof_config) = Self::extract_asof_config(&query_plan) {
                    return self.execute_asof_query(&asof_config, sql).await;
                }

                let plan_sql = query_plan.statement.to_string();
                let logical_plan = self.ctx.state().create_logical_plan(&plan_sql).await?;

                // DataFusion interpreted execution.
                let df = self.ctx.execute_logical_plan(logical_plan).await?;
                let stream = df.execute_stream().await?;

                Ok(self.bridge_query_stream(sql, stream))
            }
            laminar_sql::planner::StreamingPlan::Standard(stmt) => {
                // Async execution without the lock
                let sql_str = stmt.to_string();
                let df = self.ctx.sql(&sql_str).await?;
                let stream = df.execute_stream().await?;

                Ok(self.bridge_query_stream(sql, stream))
            }
            laminar_sql::planner::StreamingPlan::RegisterLookupTable(info) => {
                self.handle_register_lookup_table(info)
            }
            laminar_sql::planner::StreamingPlan::DropLookupTable { name } => {
                self.table_store.write().drop_table(&name);
                self.connector_manager.lock().unregister_table(&name);
                let _ = self.ctx.deregister_table(&name);
                self.lookup_registry.unregister(&name);
                self.refresh_lookup_optimizer_rule();
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DROP LOOKUP TABLE".to_string(),
                    object_name: name,
                }))
            }
        }
    }

    /// Bridge a `DataFusion` `SendableRecordBatchStream` into the streaming
    /// subscription infrastructure and return a `QueryHandle`.
    fn bridge_query_stream(
        &self,
        sql: &str,
        stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> ExecuteResult {
        let query_id = self.catalog.register_query(sql);
        let schema = stream.schema();

        let source_cfg = streaming::SourceConfig::with_buffer_size(self.config.default_buffer_size);
        let (source, sink) =
            streaming::create_with_config::<crate::catalog::ArrowRecord>(source_cfg);

        let subscription = sink.subscribe();

        let cancel_token = tokio_util::sync::CancellationToken::new();
        let cancel_token_clone = cancel_token.clone();

        let source_clone = source.clone();
        let catalog = Arc::clone(&self.catalog);
        let query_id_clone = query_id;
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            let mut stream = stream;
            loop {
                tokio::select! {
                    () = cancel_token_clone.cancelled() => {
                        break;
                    }
                    result = stream.next() => {
                        match result {
                            Some(Ok(batch)) => {
                                if source_clone.push_arrow(batch).is_err() {
                                    break;
                                }
                            }
                            _ => break,
                        }
                    }
                }
            }
            drop(source_clone);
            catalog.deactivate_query(query_id_clone);
        });

        ExecuteResult::Query(QueryHandle {
            id: query_id,
            schema,
            sql: sql.to_string(),
            subscription: Some(subscription),
            active: true,
            cancel_token,
        })
    }

    /// Extract an ASOF join config from a query plan, if present.
    fn extract_asof_config(
        plan: &laminar_sql::planner::QueryPlan,
    ) -> Option<AsofJoinTranslatorConfig> {
        plan.join_config.as_ref()?.iter().find_map(|jc| {
            if let JoinOperatorConfig::Asof(cfg) = jc {
                Some(cfg.clone())
            } else {
                None
            }
        })
    }

    /// Execute an ASOF join query by fetching left/right tables separately
    /// and performing the join in-process (bypasses `DataFusion`'s SQL parser
    /// which doesn't understand ASOF syntax).
    async fn execute_asof_query(
        &self,
        asof_config: &AsofJoinTranslatorConfig,
        original_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
        let left_sql = format!("SELECT * FROM {}", asof_config.left_table);
        let right_sql = format!("SELECT * FROM {}", asof_config.right_table);

        let left_batches = self
            .ctx
            .sql(&left_sql)
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.left_table, &e))?
            .collect()
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.left_table, &e))?;

        let right_batches = self
            .ctx
            .sql(&right_sql)
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.right_table, &e))?
            .collect()
            .await
            .map_err(|e| DbError::query_pipeline(&asof_config.right_table, &e))?;

        let result_batch =
            crate::asof_batch::execute_asof_join_batch(&left_batches, &right_batches, asof_config)?;

        if result_batch.num_rows() == 0 {
            let query_id = self.catalog.register_query(original_sql);
            self.catalog.deactivate_query(query_id);
            return Ok(ExecuteResult::Query(QueryHandle {
                id: query_id,
                schema: result_batch.schema(),
                sql: original_sql.to_string(),
                subscription: None,
                active: false,
                cancel_token: tokio_util::sync::CancellationToken::new(),
            }));
        }

        let schema = result_batch.schema();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![result_batch]])
                .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;

        let _ = self.ctx.deregister_table("__asof_result");
        self.ctx
            .register_table("__asof_result", Arc::new(mem_table))
            .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;

        let df = self
            .ctx
            .sql("SELECT * FROM __asof_result")
            .await
            .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| DbError::query_pipeline("ASOF join", &e))?;

        let _ = self.ctx.deregister_table("__asof_result");

        Ok(self.bridge_query_stream(original_sql, stream))
    }

    /// Get a typed source handle for pushing data.
    ///
    /// The source must have been created via `CREATE SOURCE`.
    ///
    /// # Errors
    ///
    /// Returns `DbError::SourceNotFound` if the source is not registered.
    /// Returns `DbError::SchemaMismatch` if the Rust type's schema does not
    /// match the source's SQL schema.
    pub fn source<T: laminar_core::streaming::Record>(
        &self,
        name: &str,
    ) -> Result<SourceHandle<T>, DbError> {
        let entry = self
            .catalog
            .get_source(name)
            .ok_or_else(|| DbError::SourceNotFound(name.to_string()))?;
        SourceHandle::new(entry)
    }

    /// Get an untyped source handle for pushing `RecordBatch` data.
    ///
    /// # Errors
    ///
    /// Returns `DbError::SourceNotFound` if the source is not registered.
    pub fn source_untyped(&self, name: &str) -> Result<UntypedSourceHandle, DbError> {
        let entry = self
            .catalog
            .get_source(name)
            .ok_or_else(|| DbError::SourceNotFound(name.to_string()))?;
        Ok(UntypedSourceHandle::new(entry))
    }

    /// List all registered sources.
    pub fn sources(&self) -> Vec<SourceInfo> {
        let names = self.catalog.list_sources();
        names
            .into_iter()
            .filter_map(|name| {
                self.catalog.get_source(&name).map(|e| SourceInfo {
                    name: e.name.clone(),
                    schema: e.schema.clone(),
                    watermark_column: e.watermark_column.clone(),
                })
            })
            .collect()
    }

    /// List all registered sinks.
    pub fn sinks(&self) -> Vec<SinkInfo> {
        self.catalog
            .list_sinks()
            .into_iter()
            .map(|name| SinkInfo { name })
            .collect()
    }

    /// List all registered materialized views with their SQL and state.
    pub fn materialized_views(&self) -> Vec<crate::handle::MaterializedViewInfo> {
        let registry = self.mv_registry.lock();
        registry
            .views()
            .map(crate::handle::MaterializedViewInfo::from)
            .collect()
    }

    /// List all registered streams with their SQL definitions.
    pub fn streams(&self) -> Vec<crate::handle::StreamInfo> {
        let mgr = self.connector_manager.lock();
        mgr.streams()
            .iter()
            .map(|(name, reg)| crate::handle::StreamInfo {
                name: name.clone(),
                sql: Some(reg.query_sql.clone()),
            })
            .collect()
    }

    /// Build the pipeline topology graph from registered sources, streams,
    /// and sinks.
    ///
    /// Returns a `PipelineTopology` with nodes for every source, stream,
    /// and sink, plus edges derived from stream SQL `FROM` references and
    /// sink `input` fields.
    pub fn pipeline_topology(&self) -> crate::handle::PipelineTopology {
        use crate::handle::{PipelineEdge, PipelineNode, PipelineNodeType};

        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Collect source names for FROM matching
        let source_names = self.catalog.list_sources();

        // Source nodes
        for name in &source_names {
            let schema = self.catalog.get_source(name).map(|e| e.schema.clone());
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Source,
                schema,
                sql: None,
            });
        }

        // Stream nodes + edges from SQL FROM references
        let mgr = self.connector_manager.lock();
        let stream_names: Vec<String> = mgr.streams().keys().cloned().collect();
        for (name, reg) in mgr.streams() {
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Stream,
                schema: None,
                sql: Some(reg.query_sql.clone()),
            });

            // Extract FROM references by checking which known sources/streams
            // appear in the query SQL. This is a lightweight heuristic that
            // avoids a full SQL parse.
            let sql_upper = reg.query_sql.to_uppercase();
            for src in &source_names {
                if sql_upper.contains(&src.to_uppercase()) {
                    edges.push(PipelineEdge {
                        from: src.clone(),
                        to: name.clone(),
                    });
                }
            }
            // Also check for stream-to-stream references (cascading)
            for other in &stream_names {
                if other != name && sql_upper.contains(&other.to_uppercase()) {
                    edges.push(PipelineEdge {
                        from: other.clone(),
                        to: name.clone(),
                    });
                }
            }
        }

        // Sink nodes + edges from input field
        for (name, reg) in mgr.sinks() {
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Sink,
                schema: None,
                sql: None,
            });

            // Sinks read from their `input` field
            if !reg.input.is_empty() {
                edges.push(PipelineEdge {
                    from: reg.input.clone(),
                    to: name.clone(),
                });
            }
        }

        // Also add catalog-only sinks (no connector type) that aren't
        // already in the connector manager
        let cm_sink_names: std::collections::HashSet<&String> = mgr.sinks().keys().collect();
        for name in self.catalog.list_sinks() {
            if !cm_sink_names.contains(&name) {
                // Check if there's a sink entry in the catalog with input info
                if let Some(input) = self.catalog.get_sink_input(&name) {
                    nodes.push(PipelineNode {
                        name: name.clone(),
                        node_type: PipelineNodeType::Sink,
                        schema: None,
                        sql: None,
                    });
                    if !input.is_empty() {
                        edges.push(PipelineEdge {
                            from: input,
                            to: name,
                        });
                    }
                }
            }
        }

        drop(mgr);

        crate::handle::PipelineTopology { nodes, edges }
    }

    /// List all active queries.
    pub fn queries(&self) -> Vec<QueryInfo> {
        self.catalog
            .list_queries()
            .into_iter()
            .map(|(id, sql, active)| QueryInfo { id, sql, active })
            .collect()
    }

    /// Returns whether streaming checkpointing is enabled.
    #[must_use]
    pub fn is_checkpoint_enabled(&self) -> bool {
        self.config.checkpoint.is_some()
    }

    /// Returns a checkpoint store instance, if checkpointing is configured.
    ///
    /// Returns an [`ObjectStoreCheckpointStore`](laminar_core::storage::ObjectStoreCheckpointStore)
    /// when `object_store_url` is set, otherwise a
    /// [`FileSystemCheckpointStore`](laminar_core::storage::FileSystemCheckpointStore).
    pub fn checkpoint_store(&self) -> Option<Box<dyn laminar_core::storage::CheckpointStore>> {
        let cp_config = self.config.checkpoint.as_ref()?;
        let max_retained = cp_config.max_retained.unwrap_or(3);
        // Pass the runtime vnode count through so manifest validation
        // checks against the real invariant, not a hardcoded default.
        let vnode_count = self.vnode_registry.lock().as_ref().map_or(
            laminar_core::storage::checkpoint_manifest::DEFAULT_VNODE_COUNT,
            |r| u16::try_from(r.vnode_count()).unwrap_or(u16::MAX),
        );

        if let Some(ref url) = self.config.object_store_url {
            let obj_store = laminar_core::storage::object_store_builder::build_object_store(
                url,
                &self.config.object_store_options,
            )
            .ok()?;
            let prefix = url_to_checkpoint_prefix(url);
            Some(Box::new(
                laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    obj_store,
                    prefix,
                    max_retained,
                )
                .with_vnode_count(vnode_count),
            ))
        } else {
            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));
            Some(Box::new(
                laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    &data_dir,
                    max_retained,
                )
                .with_vnode_count(vnode_count),
            ))
        }
    }

    /// Triggers a streaming checkpoint that persists source offsets, sink
    /// positions, and operator state to disk via the
    /// [`CheckpointCoordinator`](crate::checkpoint_coordinator::CheckpointCoordinator).
    ///
    /// Returns the checkpoint result on success, including the checkpoint ID,
    /// epoch, and duration.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if checkpointing is not enabled, the
    /// coordinator has not been initialized (call `start()` first), or the
    /// checkpoint operation fails.
    pub async fn checkpoint(
        &self,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        if self.config.checkpoint.is_none() {
            return Err(DbError::Checkpoint(
                "checkpointing is not enabled".to_string(),
            ));
        }

        #[cfg(feature = "cluster")]
        {
            let leader_opt = {
                let cc_guard = self.cluster_controller.lock();
                cc_guard.as_ref().and_then(|cc| {
                    if cc.is_leader() {
                        None
                    } else {
                        cc.current_leader().and_then(|leader_id| {
                            let watch = cc.members_watch();
                            let members = watch.borrow();
                            members
                                .iter()
                                .find(|m| m.id == leader_id)
                                .map(|m| m.rpc_address.clone())
                        })
                    }
                })
            };
            if let Some(leader_rpc) = leader_opt {
                tracing::info!(
                    "Forwarding checkpoint request to leader node at HTTP address {}",
                    leader_rpc
                );
                return self.forward_checkpoint_to_leader(&leader_rpc).await;
            }
        }

        // When the streaming pipeline is live, route through the
        // pipeline callback so it captures operator state (via the same
        // path that the periodic checkpoint timer uses). Without this,
        // the manifest has an empty `operator_states` map and restart
        // loses everything the `IncrementalAggState` accumulators held.
        let tx = self.force_ckpt_tx.lock().clone();
        let result = if let Some(tx) = tx {
            let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
            tx.send(reply_tx).await.map_err(|_| {
                DbError::Checkpoint(
                    "pipeline callback receiver closed — engine may be shutting down".into(),
                )
            })?;
            reply_rx.await.map_err(|_| {
                DbError::Checkpoint("pipeline callback dropped oneshot before replying".into())
            })?
        } else {
            // Fallback: no running pipeline (e.g., engine built but not yet
            // started). Drive the coordinator directly. Operator state will
            // be empty, but restart from this manifest is still well-defined
            // because there's nothing to restore anyway.
            let mut guard = self.coordinator.lock().await;
            let coord = guard.as_mut().ok_or_else(|| {
                DbError::Checkpoint("coordinator not initialized — call start() first".to_string())
            })?;
            coord
                .checkpoint(crate::checkpoint_coordinator::CheckpointRequest::default())
                .await
        };

        // Refresh the shared catalog manifest on every successful checkpoint so
        // a node joining later can rebuild this node's MVs/sources. Best-effort.
        #[cfg(feature = "cluster")]
        if matches!(&result, Ok(r) if r.success) {
            self.persist_catalog_manifest().await;
        }

        result
    }

    #[cfg(feature = "cluster")]
    async fn forward_checkpoint_to_leader(
        &self,
        addr: &str,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        #[derive(serde::Deserialize)]
        struct ForwardedCheckpointResponse {
            success: bool,
            checkpoint_id: u64,
            epoch: u64,
            duration_ms: u64,
            error: Option<String>,
        }

        let mut req = reqwest::Client::new()
            .post(format!("http://{addr}/api/v1/checkpoint"))
            .timeout(std::time::Duration::from_secs(10));
        if let Some(token) = &self.config.http_auth_token {
            req = req.bearer_auth(token.expose());
        }
        let resp = req.send().await.map_err(|e| {
            DbError::Checkpoint(format!(
                "failed to forward checkpoint to leader at {addr}: {e}"
            ))
        })?;

        let status = resp.status();
        let body = resp.text().await.map_err(|e| {
            DbError::Checkpoint(format!("failed to read leader checkpoint response: {e}"))
        })?;

        // The leader returns a `CheckpointResponse` body even when the
        // checkpoint itself failed (HTTP 500 + `success: false`), so parse it so
        // that structured failure reaches the follower. A body that isn't a
        // `CheckpointResponse` (e.g. a 401 error payload) is an auth/transport
        // failure — surface the status and body instead.
        match serde_json::from_str::<ForwardedCheckpointResponse>(&body) {
            Ok(response) => Ok(crate::checkpoint_coordinator::CheckpointResult {
                success: response.success,
                checkpoint_id: response.checkpoint_id,
                epoch: response.epoch,
                duration: std::time::Duration::from_millis(response.duration_ms),
                error: response.error,
            }),
            Err(_) => Err(DbError::Checkpoint(format!(
                "leader rejected checkpoint ({status}): {body}"
            ))),
        }
    }

    /// Returns checkpoint performance statistics.
    ///
    /// Returns `None` if the checkpoint coordinator has not been initialized.
    pub async fn checkpoint_stats(&self) -> Option<crate::checkpoint_coordinator::CheckpointStats> {
        let guard = self.coordinator.lock().await;
        guard
            .as_ref()
            .map(crate::checkpoint_coordinator::CheckpointCoordinator::stats)
    }
}

impl std::fmt::Debug for LaminarDB {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaminarDB")
            .field("sources", &self.catalog.list_sources().len())
            .field("sinks", &self.catalog.list_sinks().len())
            .field("materialized_views", &self.mv_registry.lock().len())
            .field("checkpoint_enabled", &self.is_checkpoint_enabled())
            .field("shutdown", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// Wraps `DefaultPhysicalPlanner` with lookup join extension support.
struct LookupQueryPlanner {
    extension_planner: Arc<dyn datafusion::physical_planner::ExtensionPlanner + Send + Sync>,
}

impl std::fmt::Debug for LookupQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LookupQueryPlanner").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl datafusion::execution::context::QueryPlanner for LookupQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &datafusion::logical_expr::LogicalPlan,
        session_state: &datafusion::execution::SessionState,
    ) -> datafusion_common::Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        use datafusion::physical_planner::PhysicalPlanner;
        let planner =
            datafusion::physical_planner::DefaultPhysicalPlanner::with_extension_planners(vec![
                Arc::clone(&self.extension_planner),
            ]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Serves a node's local MV / reference-table rows to peers (pull path). Weak
/// store handles, so it never keeps the database alive.
#[cfg(feature = "cluster")]
struct DbQueryHandler {
    mv_store: std::sync::Weak<parking_lot::RwLock<crate::mv_store::MvStore>>,
    table_store: std::sync::Weak<parking_lot::RwLock<crate::table_store::TableStore>>,
    /// Isolated context for compiling a pushed `filter_sql` (only the temp table
    /// is visible), so a crafted predicate can't reference other tables.
    filter_ctx: SessionContext,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_core::cluster::control::RemoteQueryHandler for DbQueryHandler {
    async fn remote_scan(
        &self,
        table_name: &str,
        projection: Option<Vec<usize>>,
        filter_sql: Option<String>,
    ) -> Result<arrow::array::RecordBatch, String> {
        let batch = self
            .mv_store
            .upgrade()
            .and_then(|s| s.read().to_record_batch(table_name))
            .or_else(|| {
                self.table_store
                    .upgrade()
                    .and_then(|s| s.read().to_record_batch(table_name))
            })
            .ok_or_else(|| format!("table '{table_name}' not found"))?;

        // Apply the pushed predicate before projecting (it may reference dropped
        // columns); on any failure skip it — the coordinator re-applies it.
        let batch = match filter_sql {
            Some(sql) => {
                let schema = batch.schema();
                match crate::filter_compile::compile(&self.filter_ctx, &sql, &schema).await {
                    Ok(expr) => match crate::filter_compile::apply(&batch, expr.as_ref()) {
                        Ok(Some(filtered)) => filtered,
                        Ok(None) => arrow::array::RecordBatch::new_empty(schema),
                        Err(e) => {
                            tracing::debug!(table = table_name, error = %e,
                                "remote_scan: skipping pushed filter (apply failed)");
                            batch
                        }
                    },
                    Err(e) => {
                        tracing::debug!(table = table_name, error = %e,
                            "remote_scan: skipping pushed filter (compile failed)");
                        batch
                    }
                }
            }
            None => batch,
        };

        match projection {
            Some(proj) => batch.project(&proj).map_err(|e| e.to_string()),
            None => Ok(batch),
        }
    }
}

#[cfg(test)]
mod tests;
