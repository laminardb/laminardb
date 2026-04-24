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

/// Lifecycle state of a [`LaminarDB`] instance.
///
/// Stored as `AtomicU8` on [`LaminarDB::state`] so transitions are
/// lock-free; the repr is fixed so `as u8` / `TryFrom<u8>` give the
/// same bytes the atomic sees.
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
    /// Decode a byte previously stored into the atomic. Returns `None`
    /// if the byte isn't a known variant — always a bug, because only
    /// `Self::store_into` / `DbState::* as u8` write this atomic.
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
}

// Compat aliases — remaining call sites still read/write raw bytes via
// the atomic. The enum variants and these constants share the same
// byte values (`#[repr(u8)]` above), so both work until the mechanical
// rewrite lands.
pub(crate) const STATE_CREATED: u8 = DbState::Created as u8;
pub(crate) const STATE_STARTING: u8 = DbState::Starting as u8;
pub(crate) const STATE_RUNNING: u8 = DbState::Running as u8;
pub(crate) const STATE_SHUTTING_DOWN: u8 = DbState::ShuttingDown as u8;
pub(crate) const STATE_STOPPED: u8 = DbState::Stopped as u8;

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
    /// Unified checkpoint coordinator (populated by `start()`).
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    pub(crate) connector_manager: parking_lot::Mutex<crate::connector_manager::ConnectorManager>,
    pub(crate) connector_registry: Arc<laminar_connectors::registry::ConnectorRegistry>,
    pub(crate) mv_registry: parking_lot::Mutex<laminar_core::mv::MvRegistry>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    /// Handle to the background processing task (if running).
    pub(crate) runtime_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Signal to stop the processing loop.
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    /// Prometheus engine metrics. `None` until `set_engine_metrics()` is called.
    pub(crate) engine_metrics:
        parking_lot::Mutex<Option<Arc<crate::engine_metrics::EngineMetrics>>>,
    /// Shared Prometheus registry. `None` until `set_prometheus_registry()` is called.
    pub(crate) prometheus_registry: parking_lot::Mutex<Option<Arc<prometheus::Registry>>>,
    /// Instant when the database was created, for uptime calculation.
    pub(crate) start_time: std::time::Instant,
    /// Session properties set via `SET key = value`.
    pub(crate) session_properties: parking_lot::Mutex<HashMap<String, String>>,
    /// Global pipeline watermark (min of all source watermarks).
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    /// Shared lookup table registry for physical planning of lookup joins.
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    /// Control channel sender for live DDL to the running coordinator.
    /// `None` before `start()` or after `shutdown()`.
    pub(crate) control_tx: parking_lot::Mutex<Option<ControlMsgTx>>,
    /// Materialized view result store (shared with compute thread and query threads).
    pub(crate) mv_store: Arc<parking_lot::RwLock<crate::mv_store::MvStore>>,
    /// Cluster control facade, installed by `LaminarDbBuilder::cluster_controller`.
    /// `None` in embedded / single-instance mode; `Some` activates the
    /// leader / follower checkpoint flow when the coordinator starts.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) cluster_controller:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::ClusterController>>>,
    /// State backend for durability-gate participation. Installed by
    /// `LaminarDbBuilder::state_backend`. When paired with a
    /// `vnode_registry` the coordinator writes per-vnode markers each
    /// checkpoint and the gate runs.
    pub(crate) state_backend:
        parking_lot::Mutex<Option<Arc<dyn laminar_core::state::StateBackend>>>,
    /// Vnode topology + assignment. Installed by
    /// `LaminarDbBuilder::vnode_registry`. Needed for the coordinator to
    /// know which vnodes this instance owns.
    pub(crate) vnode_registry: parking_lot::Mutex<Option<Arc<laminar_core::state::VnodeRegistry>>>,
    /// Extra physical optimizer rules from the builder, applied to both
    /// `self.ctx` and the pipeline-side `OperatorGraph` context.
    pub(crate) physical_optimizer_rules:
        Arc<[Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>]>,
    /// `target_partitions` override from the builder, mirrored into the
    /// pipeline-side `SessionContext`.
    pub(crate) pipeline_target_partitions: Option<usize>,
    /// Outbound shuffle handle. Installed via `LaminarDbBuilder::shuffle_sender`;
    /// used by `SqlQueryOperator` to row-shuffle pre-aggregate batches to vnode
    /// owners.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) shuffle_sender:
        parking_lot::Mutex<Option<Arc<laminar_core::shuffle::ShuffleSender>>>,
    /// Inbound shuffle handle. Installed via `LaminarDbBuilder::shuffle_receiver`.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) shuffle_receiver:
        parking_lot::Mutex<Option<Arc<laminar_core::shuffle::ShuffleReceiver>>>,
    /// Shared commit-marker store. Installed via the builder.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) decision_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::CheckpointDecisionStore>>>,
    /// Shared vnode-assignment snapshot store. Installed via the
    /// builder; the rebalance tasks (spawned by the caller) watch it.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) assignment_snapshot_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>>,
    /// Forwards `db.checkpoint()` requests to the running pipeline so
    /// the callback captures operator state before the manifest is
    /// packed. `None` before `start()` or after `shutdown()`; when
    /// `None`, `db.checkpoint()` falls back to the direct coordinator
    /// path (only valid when the engine has no stateful operators).
    pub(crate) force_ckpt_tx: parking_lot::Mutex<Option<ForceCheckpointTx>>,
}

/// Oneshot reply carrying the full `CheckpointResult` back to the
/// caller of `db.checkpoint()`. Created per-request.
///
/// Uses crossfire's oneshot (matches `sink_task`) rather than
/// `tokio::sync::oneshot` so the whole checkpoint plumbing uses a
/// consistent primitive family.
pub(crate) type ForceCheckpointReply =
    crossfire::oneshot::TxOneshot<Result<crate::checkpoint_coordinator::CheckpointResult, DbError>>;

/// Channel used by `db.checkpoint()` to hand a reply sender to the
/// running pipeline callback. Bounded at 64 — cold path (one item per
/// manual `db.checkpoint()` call); the cap is generous enough that the
/// caller is never expected to wait.
pub(crate) type ForceCheckpointTx =
    crossfire::MAsyncTx<crossfire::mpsc::Array<ForceCheckpointReply>>;

/// Paired receive-side of [`ForceCheckpointTx`], held by
/// `ConnectorPipelineCallback`.
pub(crate) type ForceCheckpointRx =
    crossfire::AsyncRx<crossfire::mpsc::Array<ForceCheckpointReply>>;

/// Capacity of the force-checkpoint request channel.
pub(crate) const FORCE_CHECKPOINT_CHANNEL_CAPACITY: usize = 64;

pub(crate) struct SourceWatermarkState {
    pub(crate) extractor: laminar_core::time::EventTimeExtractor,
    pub(crate) generator: Box<dyn laminar_core::time::WatermarkGenerator>,
    pub(crate) column: String,
}

pub(crate) fn filter_late_rows(
    batch: &RecordBatch,
    column: &str,
    watermark: i64,
) -> Option<RecordBatch> {
    match laminar_core::time::filter_batch_by_timestamp(
        batch,
        column,
        watermark,
        laminar_core::time::ThresholdOp::GreaterEq,
    ) {
        Ok(out) => out,
        Err(e) => {
            // Schema drift — drop the batch rather than silently admit late rows.
            tracing::error!(%column, error = %e, "filter_late_rows: dropping batch");
            None
        }
    }
}

pub(crate) use laminar_core::time::parse_duration_str;

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
            state: Arc::new(std::sync::atomic::AtomicU8::new(STATE_CREATED)),
            runtime_handle: parking_lot::Mutex::new(None),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            engine_metrics: parking_lot::Mutex::new(None),
            prometheus_registry: parking_lot::Mutex::new(None),
            start_time: std::time::Instant::now(),
            session_properties: parking_lot::Mutex::new(HashMap::new()),
            pipeline_watermark: Arc::new(std::sync::atomic::AtomicI64::new(i64::MIN)),
            lookup_registry,
            control_tx: parking_lot::Mutex::new(None),
            mv_store: Arc::new(parking_lot::RwLock::new(crate::mv_store::MvStore::new())),
            #[cfg(feature = "cluster-unstable")]
            cluster_controller: parking_lot::Mutex::new(None),
            state_backend: parking_lot::Mutex::new(None),
            vnode_registry: parking_lot::Mutex::new(None),
            physical_optimizer_rules: extra_optimizer_rules.to_vec().into(),
            pipeline_target_partitions: target_partitions,
            #[cfg(feature = "cluster-unstable")]
            shuffle_sender: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster-unstable")]
            shuffle_receiver: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster-unstable")]
            decision_store: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster-unstable")]
            assignment_snapshot_store: parking_lot::Mutex::new(None),
            force_ckpt_tx: parking_lot::Mutex::new(None),
        })
    }

    /// Install the outbound shuffle handle used by cluster-mode streaming
    /// aggregates to route pre-aggregate rows to vnode owners. Called by
    /// [`LaminarDbBuilder::shuffle_sender`]. Must be set before `start()`
    /// to take effect.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn set_shuffle_sender(&self, sender: Arc<laminar_core::shuffle::ShuffleSender>) {
        *self.shuffle_sender.lock() = Some(sender);
    }

    /// Install the inbound shuffle handle. Pair with
    /// [`Self::set_shuffle_sender`]; neither alone is a no-op.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn set_shuffle_receiver(
        &self,
        receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) {
        *self.shuffle_receiver.lock() = Some(receiver);
    }

    /// Install the commit-marker store. Must be called before `start()`.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn set_decision_store(
        &self,
        store: Arc<laminar_core::cluster::control::CheckpointDecisionStore>,
    ) {
        *self.decision_store.lock() = Some(store);
    }

    /// Install the assignment snapshot store. Must be called before `start()`.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn set_assignment_snapshot_store(
        &self,
        store: Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    ) {
        *self.assignment_snapshot_store.lock() = Some(store);
    }

    /// Adopt a new assignment snapshot: update the registry, backend
    /// fence, and coordinator under the coordinator mutex so the
    /// change lands strictly between checkpoints. Idempotent for
    /// versions at or below the current registry version.
    #[cfg(feature = "cluster-unstable")]
    pub async fn adopt_assignment_snapshot(
        &self,
        snapshot: laminar_core::cluster::control::AssignmentSnapshot,
    ) {
        let Some(registry) = self.vnode_registry.lock().clone() else {
            return;
        };
        if snapshot.version <= registry.assignment_version() {
            return;
        }
        let vnode_count = registry.vnode_count();
        let new_assignment: Arc<[laminar_core::state::NodeId]> =
            snapshot.to_vnode_vec(vnode_count).into();

        // Hold the coord mutex so the registry and fence update land
        // between epochs from the coordinator's perspective.
        let mut guard = self.coordinator.lock().await;
        registry.set_assignment_and_version(new_assignment, snapshot.version);
        if let Some(backend) = self.state_backend.lock().clone() {
            backend.set_authoritative_version(snapshot.version);
        }
        if let Some(coord) = guard.as_mut() {
            coord.set_assignment_version(snapshot.version);
            let self_id = self
                .cluster_controller
                .lock()
                .as_ref()
                .map_or(laminar_core::state::NodeId(0), |c| {
                    laminar_core::state::NodeId(c.instance_id().0)
                });
            coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, self_id));
            coord.set_gate_vnode_set((0..vnode_count).collect());
        }
        drop(guard);

        tracing::info!(version = snapshot.version, "adopted assignment snapshot",);
    }

    /// Install the cluster control facade. Called by
    /// [`LaminarDbBuilder::cluster_controller`](crate::LaminarDbBuilder::cluster_controller)
    /// before the pipeline starts; the `CheckpointCoordinator` picks
    /// it up when constructed in `pipeline_lifecycle`.
    #[cfg(feature = "cluster-unstable")]
    pub(crate) fn set_cluster_controller(
        &self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) {
        *self.cluster_controller.lock() = Some(controller);
    }

    /// Install a state backend so the checkpoint coordinator publishes
    /// per-vnode durability markers and the gate runs. Pair with
    /// [`Self::set_vnode_registry`]; either alone is a no-op.
    pub(crate) fn set_state_backend(&self, backend: Arc<dyn laminar_core::state::StateBackend>) {
        *self.state_backend.lock() = Some(backend);
    }

    /// Install a vnode registry so the checkpoint coordinator knows
    /// which vnodes this instance owns. Pair with
    /// [`Self::set_state_backend`]; either alone is a no-op.
    pub(crate) fn set_vnode_registry(&self, registry: Arc<laminar_core::state::VnodeRegistry>) {
        *self.vnode_registry.lock() = Some(registry);
    }

    /// Underlying `DataFusion` `SessionContext`. Primarily for tests
    /// that need to compile SQL through the same session the engine
    /// uses.
    #[must_use]
    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a fluent builder for constructing a `LaminarDB`.
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
                laminar_sql::datafusion::LookupSnapshot {
                    batch,
                    key_columns: info.primary_key.clone(),
                },
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

        let cache_max = info.properties.cache_memory.map(cache_entries_from_memory);

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
                cache_max_entries: cache_max,
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

        match statement {
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
                ..
            } => self.handle_create_stream(name, query, emit_clause.as_ref(), query_sql),
            StreamingStatement::CreateContinuousQuery { .. }
            | StreamingStatement::CreateLookupTable(_)
            | StreamingStatement::DropLookupTable { .. } => self.handle_query(sql).await,
            StreamingStatement::Standard(stmt) => {
                if let sqlparser::ast::Statement::CreateTable(ct) = stmt.as_ref() {
                    self.handle_create_table(ct)
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
                or_replace,
                if_not_exists,
                query_sql,
                ..
            } => {
                self.handle_create_materialized_view(
                    sql,
                    name,
                    query,
                    *or_replace,
                    *if_not_exists,
                    query_sql,
                )
                .await
            }
            StreamingStatement::AlterSource { name, operation } => {
                self.handle_alter_source(name, operation)
            }
        }
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

        let source_clone = source.clone();
        tokio::spawn(async move {
            use tokio_stream::StreamExt;
            let mut stream = stream;
            while let Some(result) = stream.next().await {
                match result {
                    Ok(batch) => {
                        if source_clone.push_arrow(batch).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            drop(source_clone);
        });

        ExecuteResult::Query(QueryHandle {
            id: query_id,
            schema,
            sql: sql.to_string(),
            subscription: Some(subscription),
            active: true,
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
            return Ok(ExecuteResult::Query(QueryHandle {
                id: query_id,
                schema: result_batch.schema(),
                sql: original_sql.to_string(),
                subscription: None,
                active: false,
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
    /// Returns an [`ObjectStoreCheckpointStore`](laminar_storage::ObjectStoreCheckpointStore)
    /// when `object_store_url` is set, otherwise a
    /// [`FileSystemCheckpointStore`](laminar_storage::FileSystemCheckpointStore).
    pub fn checkpoint_store(&self) -> Option<Box<dyn laminar_storage::CheckpointStore>> {
        let cp_config = self.config.checkpoint.as_ref()?;
        let max_retained = cp_config.max_retained.unwrap_or(3);
        // Pass the runtime vnode count through so manifest validation
        // checks against the real invariant, not a hardcoded default.
        let vnode_count = self.vnode_registry.lock().as_ref().map_or(
            laminar_storage::checkpoint_manifest::DEFAULT_VNODE_COUNT,
            |r| u16::try_from(r.vnode_count()).unwrap_or(u16::MAX),
        );

        if let Some(ref url) = self.config.object_store_url {
            let obj_store = laminar_storage::object_store_builder::build_object_store(
                url,
                &self.config.object_store_options,
            )
            .ok()?;
            let prefix = url_to_checkpoint_prefix(url);
            Some(Box::new(
                laminar_storage::checkpoint_store::ObjectStoreCheckpointStore::new(
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
                laminar_storage::checkpoint_store::FileSystemCheckpointStore::new(
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

        // When the streaming pipeline is live, route through the
        // pipeline callback so it captures operator state (via the same
        // path that the periodic checkpoint timer uses). Without this,
        // the manifest has an empty `operator_states` map and restart
        // loses everything the `IncrementalAggState` accumulators held.
        let tx = self.force_ckpt_tx.lock().clone();
        if let Some(tx) = tx {
            let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
            tx.send(reply_tx).await.map_err(|_| {
                DbError::Checkpoint(
                    "pipeline callback receiver closed — engine may be shutting down".into(),
                )
            })?;
            return reply_rx.await.map_err(|_| {
                DbError::Checkpoint("pipeline callback dropped oneshot before replying".into())
            })?;
        }

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

#[cfg(test)]
mod tests;
