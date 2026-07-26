//! The main `LaminarDB` database facade.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::SessionContext;
use laminar_core::catalog::CatalogObjectKind;
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
use crate::sql_utils;

/// Cloneable async sender for the live-DDL control channel.
pub(crate) type ControlMsgTx = crossfire::MAsyncTx<crossfire::mpsc::Array<ControlMsg>>;

/// Lifecycle state of a [`LaminarDB`] instance.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DbState {
    Created = 0,
    Starting = 1,
    Running = 2,
    ShuttingDown = 3,
    Stopped = 4,
    /// Runtime fault. Recoverable faults may restart from the catalog; terminal resource
    /// exhaustion remains faulted for operator intervention. Reason in `LaminarDB::last_fault`.
    Faulted = 5,
}

/// Deployment scope fixed when the database is constructed.
///
/// Cluster support being compiled into the process does not select cluster semantics. The
/// builder resolves this value once and separately verifies that cluster-only handles agree with
/// it, so admission cannot change because a controller slot happens to be populated later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeMode {
    Local,
    Cluster,
}

impl RuntimeMode {
    pub(crate) const fn is_cluster(self) -> bool {
        matches!(self, Self::Cluster)
    }
}

impl DbState {
    pub(crate) fn from_u8(raw: u8) -> Option<Self> {
        Some(match raw {
            0 => Self::Created,
            1 => Self::Starting,
            2 => Self::Running,
            3 => Self::ShuttingDown,
            4 => Self::Stopped,
            5 => Self::Faulted,
            _ => return None,
        })
    }

    pub(crate) fn load(atomic: &std::sync::atomic::AtomicU8) -> Self {
        Self::from_u8(atomic.load(std::sync::atomic::Ordering::Acquire)).unwrap_or(Self::Stopped)
    }

    pub(crate) fn store(self, atomic: &std::sync::atomic::AtomicU8) {
        atomic.store(self as u8, std::sync::atomic::Ordering::Release);
    }

    /// Atomically transition `current → new`; returns the observed state on failure.
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

const DB_IO_WORKER_THREADS: usize = 2;
// Cluster recovery has a finite, non-recursive async call chain that exceeds Tokio's 2 MiB
// default worker stack. Keep the larger stack out of embedded and single-node runtimes.
const CLUSTER_IO_WORKER_STACK_BYTES: usize = 4 * 1024 * 1024;

struct DbControlRuntimeInner {
    handle: tokio::runtime::Handle,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

/// Lazily-created executor for connector I/O and lifecycle ownership.
///
/// The runtime itself lives on a detached owner thread, so dropping a DB from async code never
/// blocks on Tokio shutdown. The fixed two-worker size keeps control and feedback traffic live
/// while one connector future is temporarily busy without adding a public tuning dimension.
pub(crate) struct DbControlRuntime {
    worker_stack_bytes: Option<usize>,
    inner: parking_lot::Mutex<Option<DbControlRuntimeInner>>,
}

impl DbControlRuntime {
    fn new(runtime_mode: RuntimeMode) -> Self {
        Self {
            worker_stack_bytes: runtime_mode
                .is_cluster()
                .then_some(CLUSTER_IO_WORKER_STACK_BYTES),
            inner: parking_lot::Mutex::new(None),
        }
    }

    pub(crate) fn handle(&self) -> Result<tokio::runtime::Handle, DbError> {
        let mut runtime_slot = self.inner.lock();
        if let Some(runtime) = runtime_slot.as_ref() {
            return Ok(runtime.handle.clone());
        }

        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let worker_stack_bytes = self.worker_stack_bytes;
        let owner = std::thread::Builder::new()
            .name("laminar-io-owner".into())
            .spawn(move || {
                let mut builder = tokio::runtime::Builder::new_multi_thread();
                builder
                    .worker_threads(DB_IO_WORKER_THREADS)
                    .thread_name("laminar-io")
                    .enable_all();
                if let Some(bytes) = worker_stack_bytes {
                    builder.thread_stack_size(bytes);
                }
                let runtime = match builder.build() {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                if ready_tx.send(Ok(runtime.handle().clone())).is_err() {
                    return;
                }
                runtime.block_on(async {
                    let _ = shutdown_rx.await;
                });
                runtime.shutdown_timeout(std::time::Duration::from_secs(10));
            })
            .map_err(|error| {
                DbError::Pipeline(format!("failed to spawn LaminarDB I/O runtime: {error}"))
            })?;
        // The shutdown sender is the nonblocking owner. The OS thread terminates after it fires.
        drop(owner);

        let handle = ready_rx
            .recv()
            .map_err(|_| {
                DbError::Pipeline("LaminarDB I/O runtime exited during initialization".into())
            })?
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "failed to initialize LaminarDB I/O runtime: {error}"
                ))
            })?;
        *runtime_slot = Some(DbControlRuntimeInner {
            handle: handle.clone(),
            shutdown: shutdown_tx,
        });
        Ok(handle)
    }
}

impl Drop for DbControlRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.inner.get_mut().take() {
            let _ = runtime.shutdown.send(());
        }
    }
}

pub(crate) fn canonical_object_name(name: &sqlparser::ast::ObjectName) -> Result<String, DbError> {
    let [part] = name.0.as_slice() else {
        return Err(DbError::InvalidOperation(format!(
            "catalog identifiers must be unqualified: {name}"
        )));
    };
    let ident = part.as_ident().ok_or_else(|| {
        DbError::InvalidOperation(format!(
            "dynamic catalog identifier is not supported: {name}"
        ))
    })?;
    Ok(ident.value.clone())
}

pub(crate) fn exact_table_reference(name: &str) -> datafusion::common::TableReference {
    // `Into<TableReference>` reparses `&str` and lowercases it independently of session config.
    datafusion::common::TableReference::bare(name)
}

/// The main `LaminarDB` database handle.
///
/// Unified interface for SQL execution, data ingestion, and result consumption.
pub struct LaminarDB {
    runtime_mode: RuntimeMode,
    pub(crate) catalog: Arc<SourceCatalog>,
    pub(crate) planner: parking_lot::Mutex<StreamingPlanner>,
    pub(crate) ctx: SessionContext,
    pub(crate) config: LaminarConfig,
    pub(crate) config_vars: Arc<HashMap<String, String>>,
    pub(crate) shutdown: std::sync::atomic::AtomicBool,
    pub(crate) coordinator:
        Arc<tokio::sync::Mutex<Option<crate::checkpoint_coordinator::CheckpointCoordinator>>>,
    pub(crate) connector_manager: parking_lot::Mutex<crate::connector_manager::ConnectorManager>,
    pub(crate) connector_registry: Arc<laminar_connectors::registry::ConnectorRegistry>,
    pub(crate) mv_registry: parking_lot::Mutex<laminar_core::mv::MvRegistry>,
    pub(crate) table_store: Arc<parking_lot::RwLock<crate::table_store::TableStore>>,
    pub(crate) state: Arc<std::sync::atomic::AtomicU8>,
    /// Last recoverable runtime fault, panic, or terminal resource-exhaustion reason;
    /// cleared on a clean start. Surfaced via `pipeline_status`/`/ready`.
    pub(crate) last_fault: Arc<parking_lot::Mutex<Option<String>>>,
    /// Permanent per-instance fence set when catalog cleanup cannot prove that every backing
    /// registration was removed. Unlike a normal runtime fault, this is never restartable.
    pub(crate) catalog_cleanup_fenced: std::sync::atomic::AtomicBool,
    /// Deterministic provider-deregistration failure used to exercise the terminal cleanup fence.
    #[cfg(test)]
    pub(crate) catalog_cleanup_deregister_fault: parking_lot::Mutex<Option<String>>,
    /// Self-ref set by `enable_supervision`; empty `Weak` (default) disables auto-restart.
    pub(crate) supervisor_self: Arc<parking_lot::Mutex<std::sync::Weak<LaminarDB>>>,
    /// Auto-restart timestamps within the sliding window; bounds restart storms.
    pub(crate) restart_history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    /// Serializes start/stop/shutdown ownership across awaits. Runtime-handle `None` means an
    /// owner is currently joining only while this lock is held; another caller must never treat
    /// it as completed teardown.
    pub(crate) lifecycle_lock: tokio::sync::Mutex<()>,
    pub(crate) control_runtime: DbControlRuntime,
    pub(crate) startup_attempt:
        parking_lot::Mutex<Option<Arc<crate::pipeline_lifecycle::StartupAttempt>>>,
    /// Serializes topology DDL through manifest persistence; catalog reads take a shared guard so
    /// they cannot observe a tentative create that may still roll back.
    pub(crate) topology_ddl_lock: tokio::sync::RwLock<()>,
    /// Typed ownership for every user-visible catalog identifier.
    pub(crate) catalog_namespace: parking_lot::Mutex<HashMap<String, CatalogObjectKind>>,
    #[cfg(test)]
    pub(crate) topology_planning_gate:
        parking_lot::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(test)]
    pub(crate) stop_after_claim_gate:
        parking_lot::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(all(test, feature = "cluster"))]
    pub(crate) catalog_seal_gate:
        parking_lot::Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    /// Kept inside an async mutex while joined. A cancelled stop future drops only the guard,
    /// never the sole watcher handle, so a retry cannot publish a false terminal state.
    pub(crate) runtime_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Decoupled coordinated-commit committer task. Awaited in place so cancellation cannot lose
    /// the sole handle while an issued external commit is still completing.
    pub(crate) committer_handle: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// OS-released exclusive lock for a local checkpoint namespace. Deployment
    /// identity prevents reuse after reset; this lock prevents two live processes from writing
    /// divergent cuts into the same deployment.
    pub(crate) checkpoint_namespace_lock: parking_lot::Mutex<Option<std::fs::File>>,
    /// Every sink actor in the active generation, retained from spawn until terminal observation.
    /// A replacement cannot start while any prior actor can still mutate an external system.
    pub(crate) owned_sink_handles: Arc<parking_lot::Mutex<Vec<crate::sink_task::SinkTaskHandle>>>,
    /// Every source task in the active or draining generation. A replacement cannot start until
    /// the stable supervisor has observed actual task exit, including after Tokio cancellation.
    pub(crate) owned_source_tasks: crate::pipeline::streaming_coordinator::OwnedSourceTasks,
    /// Connector children admitted before an actor existed remain fenced across startup failure.
    pub(crate) owned_connector_task_fences: crate::connector_task_fence::OwnedConnectorTaskFences,
    pub(crate) shutdown_signal: Arc<tokio::sync::Notify>,
    /// Persistent terminal cancellation for the currently installed compute runtime. Unlike a
    /// notification permit, cancellation cannot be lost while the coordinator is between awaits.
    pub(crate) runtime_shutdown: parking_lot::RwLock<tokio_util::sync::CancellationToken>,
    pub(crate) engine_metrics:
        parking_lot::Mutex<Option<Arc<crate::engine_metrics::EngineMetrics>>>,
    /// Process-lifetime, fixed-capacity barrier timing evidence. Pipeline recovery generations share
    /// this ledger; an OS process restart creates a new sequence domain.
    #[cfg(feature = "cluster")]
    pub(crate) checkpoint_barrier_timings:
        Arc<crate::checkpoint_timing::CheckpointBarrierTimingLedger>,
    pub(crate) prometheus_registry: parking_lot::Mutex<Option<Arc<prometheus::Registry>>>,
    pub(crate) start_time: std::time::Instant,
    pub(crate) session_properties: parking_lot::Mutex<HashMap<String, String>>,
    /// Min of all source watermarks.
    pub(crate) pipeline_watermark: Arc<std::sync::atomic::AtomicI64>,
    pub(crate) lookup_registry: Arc<laminar_sql::datafusion::LookupTableRegistry>,
    /// `None` unless `[ai]`/`[models]` are configured.
    pub(crate) ai_runtime: Option<Arc<crate::ai::AiRuntime>>,
    /// Runtime the AI inference workers spawn on; set alongside `ai_runtime`.
    pub(crate) ai_handle: Option<tokio::runtime::Handle>,
    /// Live-DDL channel; `None` outside `start..shutdown`.
    pub(crate) control_tx: parking_lot::Mutex<Option<ControlMsgTx>>,
    pub(crate) mv_store: Arc<parking_lot::RwLock<crate::mv_store::MvStore>>,
    /// `None` in embedded mode.
    #[cfg(feature = "cluster")]
    pub(crate) cluster_controller:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::ClusterController>>>,
    /// When set, the next start restores to this cluster-agreed epoch instead of the
    /// local latest (taken in `start_inner`).
    #[cfg(feature = "cluster")]
    pub(crate) recover_target_epoch: parking_lot::Mutex<Option<u64>>,
    /// Database-owned recovery supervisor. It outlives restartable pipeline generations and is
    /// aborted if the facade is dropped without a graceful terminal shutdown.
    #[cfg(feature = "cluster")]
    pub(crate) recovery_monitor: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// Prevents a timed-out coordinated stop/start from overlapping a later lifecycle attempt.
    /// Cleared by the owning lifecycle thread only after its future actually finishes.
    #[cfg(feature = "cluster")]
    pub(crate) coordinated_lifecycle_active: Arc<std::sync::atomic::AtomicBool>,
    /// Persistent ownership fence from the first observed recovery fault through the committed
    /// recovery release. Unlike `coordinated_lifecycle_active`, this spans the gap between a
    /// completed stop and its durable stopped report, so an operator start cannot invalidate the
    /// report's quiescence claim.
    #[cfg(feature = "cluster")]
    pub(crate) coordinated_recovery_fenced: Arc<std::sync::atomic::AtomicBool>,
    /// Opaque local fault request retained through durable publication and cleared only while an
    /// authorized committed recovery Release is consumed. A newer fault atomically replaces it.
    #[cfg(feature = "cluster")]
    pub(crate) pending_recovery_fault: Arc<std::sync::atomic::AtomicU64>,
    /// Epoch the most recent start restored from (`None` = started fresh). A rejoin fault
    /// is reported only when prior local state existed — a fresh joiner lost no window.
    #[cfg(feature = "cluster")]
    pub(crate) last_recovery_epoch: parking_lot::Mutex<Option<u64>>,
    /// Holds source intake closed during a coordinated round until every node has restarted
    /// and rebound its shuffle receiver (the restore quorum). Sources re-read + re-shuffle the
    /// replay window on restart; without this gate a node that restarts first shuffles into a
    /// peer whose receiver isn't up yet and the fire-and-forget frames are lost.
    #[cfg(feature = "cluster")]
    pub(crate) source_gate: Arc<std::sync::atomic::AtomicBool>,
    /// One-way local data-plane fence after stable process-lease loss.
    #[cfg(feature = "cluster")]
    pub(crate) cluster_authority_revoked: std::sync::atomic::AtomicBool,
    /// Serializes source/shuffle authority grants with terminal revocation.
    #[cfg(feature = "cluster")]
    pub(crate) cluster_authority_transition: parking_lot::Mutex<()>,
    /// Paired with `vnode_registry`; the coordinator gates commits when both are installed.
    pub(crate) state_backend:
        parking_lot::Mutex<Option<Arc<dyn laminar_core::state::StateBackend>>>,
    pub(crate) vnode_registry: parking_lot::Mutex<Option<Arc<laminar_core::state::VnodeRegistry>>>,
    pub(crate) physical_optimizer_rules:
        Arc<[Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>]>,
    /// `target_partitions` override. Streaming plans currently require one reusable partition.
    pub(crate) pipeline_target_partitions: Option<usize>,
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_sender:
        parking_lot::Mutex<Option<Arc<laminar_core::shuffle::ShuffleSender>>>,
    #[cfg(feature = "cluster")]
    pub(crate) shuffle_receiver:
        Arc<parking_lot::Mutex<Option<Arc<laminar_core::shuffle::ShuffleReceiver>>>>,
    #[cfg(feature = "cluster")]
    pub(crate) decision_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::CheckpointDecisionStore>>>,
    #[cfg(feature = "cluster")]
    pub(crate) assignment_snapshot_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>>,
    /// Create-once cluster catalog inventory, sealed at bootstrap and replayed at boot.
    #[cfg(feature = "cluster")]
    pub(crate) catalog_manifest_store:
        parking_lot::Mutex<Option<Arc<laminar_core::cluster::control::CatalogManifestStore>>>,
    /// Pre-built shared checkpoint namespace installed during cluster construction.
    #[cfg(feature = "cluster")]
    cluster_checkpoint_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    /// Vnode state staged during rebalance adoption; operators drain it each cycle to resume
    /// from the last committed epoch. Shared with `OperatorGraph` via `ClusterShuffleConfig`.
    #[cfg(feature = "cluster")]
    pub(crate) rehydrated_vnode_state: Arc<parking_lot::Mutex<HashMap<u32, RehydratedVnode>>>,
    /// Vnodes lost on rebalance adoption; operators drain this each cycle and drop the stale
    /// in-memory state so a later re-acquire merges into empty state (no additive double-count).
    #[cfg(feature = "cluster")]
    pub(crate) pending_revoke_vnodes: Arc<parking_lot::Mutex<rustc_hash::FxHashSet<u32>>>,
    /// Process incarnation for which local vnode state has been restored or initialized.
    #[cfg(feature = "cluster")]
    local_state_incarnation: parking_lot::Mutex<Option<uuid::Uuid>>,
    /// Serializes successor preparation with assignment-certificate publication. Remote state
    /// reads do not hold the compute-cycle fence, but an old certificate must never be re-opened
    /// while a newer audited assignment is being prepared.
    #[cfg(feature = "cluster")]
    pub(crate) assignment_adoption_lock: tokio::sync::Mutex<()>,
    /// Changes whenever local assignment authority is suspended or invalidated. The snapshot
    /// watcher uses it to reject a certificate computed from a head read before that closure.
    #[cfg(feature = "cluster")]
    pub(crate) assignment_authority_revision: std::sync::atomic::AtomicU64,
    /// Linearizes assignment publication with compute-cycle entry so staged
    /// revoke/rehydration state is applied before any row observes new ownership.
    #[cfg(feature = "cluster")]
    pub(crate) rotation_execution_fence: Arc<tokio::sync::RwLock<()>>,
    /// Routes `db.checkpoint()` requests to the streaming coordinator for exact-attempt
    /// barrier admission. `None` means no running pipeline can produce a valid cut.
    pub(crate) force_ckpt_tx: parking_lot::Mutex<Option<ForceCheckpointTx>>,
    pub(crate) subscription_registry: Arc<crate::subscription::SubscriptionRegistry>,
    /// Resolved at `start()`; consulted by SUBSCRIBE WHERE.
    pub(crate) stream_schemas:
        parking_lot::RwLock<std::collections::HashMap<String, arrow_schema::SchemaRef>>,
}

impl Drop for LaminarDB {
    fn drop(&mut self) {
        #[cfg(feature = "cluster")]
        if let Some(monitor) = self.recovery_monitor.get_mut().take() {
            monitor.abort();
        }
        if matches!(
            DbState::load(&self.state),
            DbState::Starting | DbState::Running | DbState::ShuttingDown | DbState::Faulted
        ) {
            // Dropping Tokio handles detaches their tasks. A local decision hard-link already in
            // a blocking filesystem worker could therefore outlive this facade. Keep the OS lock
            // until process exit rather than let another in-process deployment acquire the same
            // checkpoint namespace while old work can still publish. Graceful stop/shutdown
            // clears the lock before reaching a terminal state and does not leak it.
            if let Some(lock) = self.checkpoint_namespace_lock.get_mut().take() {
                tracing::error!(
                    "dropping an active checkpoint writer; retaining its namespace lock until process exit"
                );
                std::mem::forget(lock);
            }
        }
    }
}

#[cfg(feature = "cluster")]
fn checkpoint_participant_for_runtime(db: &LaminarDB) -> Option<u64> {
    if !db.is_cluster_runtime() {
        return None;
    }
    db.cluster_controller
        .lock()
        .as_ref()
        .map(|controller| controller.instance_id().0)
}

#[cfg(feature = "cluster")]
tokio::task_local! {
    static CATALOG_MANIFEST_REPLAY: ();
}

#[cfg(feature = "cluster")]
tokio::task_local! {
    static CATALOG_BOOTSTRAP: ();
}

#[cfg(feature = "cluster")]
struct CatalogBootstrapGuard<'a> {
    db: &'a LaminarDB,
    created: Vec<(String, CatalogObjectKind)>,
    sealed: bool,
}

#[cfg(feature = "cluster")]
impl CatalogBootstrapGuard<'_> {
    fn record(&mut self, name: String, kind: CatalogObjectKind) {
        self.created.push((name, kind));
    }

    fn sealed(mut self) {
        self.sealed = true;
    }
}

#[cfg(feature = "cluster")]
impl Drop for CatalogBootstrapGuard<'_> {
    fn drop(&mut self) {
        if !self.sealed {
            for (name, kind) in self.created.iter().rev() {
                self.db
                    .rollback_catalog_create_or_fence(name, *kind, "catalog bootstrap rollback");
            }
        }
    }
}

#[cfg(feature = "cluster")]
pub(crate) fn catalog_manifest_replay_active() -> bool {
    CATALOG_MANIFEST_REPLAY.try_with(|()| ()).is_ok()
}

#[cfg(feature = "cluster")]
fn catalog_bootstrap_active() -> bool {
    CATALOG_BOOTSTRAP.try_with(|()| ()).is_ok()
}

#[cfg(not(feature = "cluster"))]
pub(crate) const fn catalog_manifest_replay_active() -> bool {
    false
}

fn is_topology_ddl(statement: &StreamingStatement) -> bool {
    match statement {
        StreamingStatement::CreateSource(_)
        | StreamingStatement::CreateSink(_)
        | StreamingStatement::CreateContinuousQuery { .. }
        | StreamingStatement::DropSource { .. }
        | StreamingStatement::DropSink { .. }
        | StreamingStatement::DropMaterializedView { .. }
        | StreamingStatement::CreateMaterializedView { .. }
        | StreamingStatement::CreateStream { .. }
        | StreamingStatement::DropStream { .. }
        | StreamingStatement::AlterSource { .. }
        | StreamingStatement::CreateLookupTable(_)
        | StreamingStatement::DropLookupTable { .. } => true,
        StreamingStatement::Standard(statement) => matches!(
            statement.as_ref(),
            sqlparser::ast::Statement::CreateTable(_)
                | sqlparser::ast::Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    ..
                }
                | sqlparser::ast::Statement::AlterTable { .. }
        ),
        _ => false,
    }
}

fn mutates_database(statement: &StreamingStatement) -> bool {
    is_topology_ddl(statement)
        || matches!(
            statement,
            StreamingStatement::InsertInto { .. }
                | StreamingStatement::Checkpoint
                | StreamingStatement::RestoreCheckpoint { .. }
        )
        || matches!(
            statement,
            StreamingStatement::Standard(statement)
                if matches!(statement.as_ref(), sqlparser::ast::Statement::Set(_))
        )
}

fn reads_catalog(statement: &StreamingStatement) -> bool {
    matches!(
        statement,
        StreamingStatement::Standard(statement)
            if matches!(statement.as_ref(), sqlparser::ast::Statement::Query(_))
    ) || matches!(
        statement,
        StreamingStatement::InsertInto { .. }
            | StreamingStatement::Show(_)
            | StreamingStatement::Describe { .. }
            | StreamingStatement::Explain { .. }
    )
}

#[cfg(feature = "cluster")]
fn catalog_create_identity(
    statement: &StreamingStatement,
) -> Result<Option<(String, CatalogObjectKind, &'static str)>, DbError> {
    let identity = match statement {
        StreamingStatement::CreateSource(create) => (
            canonical_object_name(&create.name)?,
            CatalogObjectKind::Source,
            "CREATE SOURCE",
        ),
        StreamingStatement::CreateSink(create) => (
            canonical_object_name(&create.name)?,
            CatalogObjectKind::Sink,
            "CREATE SINK",
        ),
        StreamingStatement::CreateMaterializedView { name, .. } => (
            canonical_object_name(name)?,
            CatalogObjectKind::MaterializedView,
            "CREATE MATERIALIZED VIEW",
        ),
        StreamingStatement::CreateStream { name, .. } => (
            canonical_object_name(name)?,
            CatalogObjectKind::Stream,
            "CREATE STREAM",
        ),
        StreamingStatement::CreateLookupTable(create) => (
            canonical_object_name(&create.name)?,
            CatalogObjectKind::LookupTable,
            "CREATE LOOKUP TABLE",
        ),
        StreamingStatement::Standard(statement) => {
            let sqlparser::ast::Statement::CreateTable(create) = statement.as_ref() else {
                return Ok(None);
            };
            (
                canonical_object_name(&create.name)?,
                CatalogObjectKind::Table,
                "CREATE TABLE",
            )
        }
        _ => return Ok(None),
    };
    Ok(Some(identity))
}

#[cfg(feature = "cluster")]
fn validate_cluster_catalog_create(
    sql: &str,
    statement: &StreamingStatement,
) -> Result<(String, CatalogObjectKind, &'static str), DbError> {
    if matches!(
        statement,
        StreamingStatement::CreateStream {
            retention_bytes: Some(_),
            ..
        }
    ) {
        return Err(DbError::Unsupported(
            "CREATE STREAM RETAIN HISTORY is not supported in cluster runtime until replay is globally ordered and checkpoint-aligned"
                .into(),
        ));
    }
    if catalog_ddl_contains_comment(sql)? {
        return Err(DbError::InvalidOperation(
            "cluster catalog DDL cannot persist SQL comments; submit one canonical typed definition"
                .into(),
        ));
    }
    if let Some(key) = sensitive_catalog_property(statement) {
        return Err(DbError::InvalidOperation(format!(
            "cluster catalog DDL cannot persist secret property '{key}'; use $${{ENV_VAR}} in server TOML or ${{ENV_VAR}} through the SQL API (without a default), or omit it for a connector environment fallback"
        )));
    }
    if connector_source_requires_schema_discovery(statement) {
        return Err(DbError::InvalidOperation(
            "cluster connector sources require an explicit column schema; runtime schema discovery is not a durable catalog identity"
                .into(),
        ));
    }
    let identity = catalog_create_identity(statement)?.ok_or_else(|| {
        DbError::InvalidOperation(
            "cluster catalog bootstrap accepts only reversible typed CREATE statements".into(),
        )
    })?;
    if matches!(
        identity.1,
        CatalogObjectKind::Table | CatalogObjectKind::LookupTable
    ) {
        return Err(DbError::InvalidOperation(format!(
            "{} is not supported in cluster mode until reference-table rows and source positions are atomically replicated through distributed state",
            identity.2
        )));
    }
    Ok(identity)
}

#[cfg(feature = "cluster")]
fn uri_contains_unsupported_secret(value: &str, allow_reference: bool) -> bool {
    if value.contains("://") {
        return laminar_connectors::security::value_contains_uri_secret(value, allow_reference);
    }
    {
        let lower = value.to_ascii_lowercase();
        lower.split_whitespace().any(|part| {
            part.strip_prefix("password=")
                .or_else(|| part.strip_prefix("pwd="))
                .is_some_and(|password| {
                    let password = password.trim_matches(|ch| ch == '\'' || ch == '"');
                    !(password.is_empty()
                        || allow_reference
                            && laminar_connectors::security::is_env_reference(password))
                })
        })
    }
}

#[cfg(feature = "cluster")]
fn catalog_property_contains_unsupported_secret(
    key: &str,
    value: &str,
    allow_reference: bool,
) -> bool {
    let lower = key.to_ascii_lowercase();
    let normalized = lower.replace(['.', '-'], "_");
    let secret_key = laminar_connectors::security::is_secret_option_key(key);
    if secret_key {
        return !(allow_reference && laminar_connectors::security::is_env_reference(value));
    }
    if value.contains("://")
        && laminar_connectors::security::value_contains_uri_secret(value, allow_reference)
    {
        return true;
    }
    (normalized.contains("connection")
        || normalized == "uri"
        || normalized.ends_with("_uri")
        || normalized == "url"
        || normalized.ends_with("_url")
        || normalized == "dsn")
        && uri_contains_unsupported_secret(value, allow_reference)
}

#[cfg(feature = "cluster")]
fn catalog_ddl_contains_comment(sql: &str) -> Result<bool, DbError> {
    use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

    let tokens = Tokenizer::new(&sqlparser::dialect::GenericDialect {}, sql)
        .tokenize()
        .map_err(|error| {
            DbError::InvalidOperation(format!("catalog DDL tokenization failed: {error}"))
        })?;
    Ok(tokens.into_iter().any(|token| {
        matches!(
            token,
            Token::Whitespace(
                Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)
            )
        )
    }))
}

#[cfg(feature = "cluster")]
fn sensitive_catalog_property(statement: &StreamingStatement) -> Option<String> {
    fn find<'a>(
        options: impl IntoIterator<Item = (&'a String, &'a String)>,
        allow_reference: bool,
    ) -> Option<String> {
        options
            .into_iter()
            .find(|(key, value)| {
                catalog_property_contains_unsupported_secret(key, value, allow_reference)
            })
            .map(|(key, _)| key.clone())
    }

    match statement {
        StreamingStatement::CreateSource(create) => find(create.with_options.iter(), true)
            .or_else(|| find(create.connector_options.iter(), true))
            .or_else(|| {
                create
                    .format
                    .as_ref()
                    .and_then(|format| find(format.options.iter(), true))
            }),
        StreamingStatement::CreateSink(create) => find(create.with_options.iter(), true)
            .or_else(|| find(create.connector_options.iter(), true))
            .or_else(|| find(create.output_options.iter(), true))
            .or_else(|| {
                create
                    .format
                    .as_ref()
                    .and_then(|format| find(format.options.iter(), true))
            }),
        StreamingStatement::CreateLookupTable(create) => find(create.with_options.iter(), false),
        StreamingStatement::Standard(statement) => {
            let sqlparser::ast::Statement::CreateTable(create) = statement.as_ref() else {
                return None;
            };
            let sqlparser::ast::CreateTableOptions::With(options) = &create.table_options else {
                return None;
            };
            options.iter().find_map(|option| {
                let sqlparser::ast::SqlOption::KeyValue { key, value } = option else {
                    return None;
                };
                let key = key.to_string();
                let value = value.to_string();
                catalog_property_contains_unsupported_secret(&key, value.trim_matches('\''), false)
                    .then_some(key)
            })
        }
        _ => None,
    }
}

#[cfg(feature = "cluster")]
fn connector_source_requires_schema_discovery(statement: &StreamingStatement) -> bool {
    let StreamingStatement::CreateSource(create) = statement else {
        return false;
    };
    let has_connector = create.connector_type.is_some()
        || create
            .with_options
            .keys()
            .any(|key| key.eq_ignore_ascii_case("connector"));
    has_connector && (create.columns.is_empty() || create.has_wildcard)
}

#[cfg(not(feature = "cluster"))]
const fn checkpoint_participant_for_runtime(_db: &LaminarDB) -> Option<u64> {
    None
}

/// Reply channel for a single `db.checkpoint()` request.
pub(crate) type ForceCheckpointReply =
    crossfire::oneshot::TxOneshot<Result<crate::checkpoint_coordinator::CheckpointResult, DbError>>;

pub(crate) type ForceCheckpointTx =
    crossfire::MAsyncTx<crossfire::mpsc::Array<ForceCheckpointReply>>;

pub(crate) type ForceCheckpointRx =
    crossfire::AsyncRx<crossfire::mpsc::Array<ForceCheckpointReply>>;

pub(crate) const FORCE_CHECKPOINT_CHANNEL_CAPACITY: usize = 64;

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

/// Committed vnode state staged during rebalance adoption for deferred apply.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
pub struct RehydratedVnode {
    /// Committed epoch the chain head was read from.
    pub epoch: u64,
    /// Recovery chain (oldest→newest decoded-as-bytes partials): a FULL base plus any delta partials.
    pub chain: Vec<bytes::Bytes>,
}

/// Summary of a single [`LaminarDB::adopt_assignment_snapshot`] call.
#[cfg(feature = "cluster")]
#[derive(Debug, Default)]
pub struct SnapshotAdoption {
    /// `false` when the snapshot was stale or no registry was installed.
    pub adopted: bool,
    /// The snapshot version considered.
    pub version: u64,
    /// Vnodes this node gained in this rotation.
    pub newly_acquired: Vec<u32>,
    /// How many of `newly_acquired` had committed state read back.
    pub rehydrated: usize,
    /// Committed epoch the rehydration read from, if any.
    pub rehydration_epoch: Option<u64>,
}

/// Result of certifying a clustered process at startup.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterStartupDisposition {
    /// This process owns vnodes and may serve its certified assignment.
    Serving,
    /// This process owns no vnodes and remains data-plane fenced while watching assignments.
    Idle,
    /// A coordinated recovery must release this process before it may serve data.
    RecoveryFenced,
}

/// Result of publishing one locally serialized assignment-authority certificate.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AssignmentAuthorityActivation {
    /// The exact certificate was installed and published.
    pub(crate) installed: bool,
    /// Source intake was opened; recovery keeps it closed even after installation.
    pub(crate) intake_open: bool,
    /// Authority revision observed at the local publication point.
    pub(crate) revision: u64,
}

#[cfg(feature = "cluster")]
fn owned_vnode_indices(
    assignment: &[laminar_core::state::NodeId],
    self_id: laminar_core::state::NodeId,
) -> Result<Vec<u32>, DbError> {
    assignment
        .iter()
        .enumerate()
        .filter(|(_, owner)| **owner == self_id)
        .map(|(vnode, _)| {
            u32::try_from(vnode).map_err(|_| {
                DbError::Checkpoint(
                    "vnode assignment is too large to encode a u32 vnode identifier".into(),
                )
            })
        })
        .collect()
}

impl LaminarDB {
    pub(crate) const fn runtime_mode(&self) -> RuntimeMode {
        self.runtime_mode
    }

    /// Whether this instance was constructed with distributed runtime semantics.
    pub(crate) const fn is_cluster_runtime(&self) -> bool {
        self.runtime_mode().is_cluster()
    }

    /// Vnodes revoked by a rebalance and staged for operator-state drop on the next cycle; drained
    /// once the compute thread applies the revoke. Observability/test hook.
    #[cfg(feature = "cluster")]
    #[doc(hidden)]
    #[must_use]
    pub fn pending_revoke_vnode_count(&self) -> usize {
        self.pending_revoke_vnodes.lock().len()
    }

    /// Create an embedded in-memory database with default settings.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    pub fn open() -> Result<Arc<Self>, DbError> {
        Self::open_with_config(LaminarConfig::default())
    }

    /// Create with custom configuration.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if `DataFusion` context creation fails.
    pub fn open_with_config(config: LaminarConfig) -> Result<Arc<Self>, DbError> {
        let db = Self::open_with_config_and_vars(config, HashMap::new())?;
        db.connector_registry.freeze();
        Ok(Arc::new(db))
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
        Self::open_with_config_and_vars_and_rules(
            config,
            config_vars,
            &[],
            None,
            RuntimeMode::Local,
        )
    }

    /// Same as [`Self::open_with_config_and_vars`] but also installs
    /// the given physical-optimizer rules on the `DataFusion` session.
    #[allow(clippy::unnecessary_wraps)]
    #[allow(clippy::too_many_lines)] // flat field-init of a large struct
    pub(crate) fn open_with_config_and_vars_and_rules(
        mut config: LaminarConfig,
        config_vars: HashMap<String, String>,
        extra_optimizer_rules: &[Arc<
            dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync,
        >],
        target_partitions: Option<usize>,
        runtime_mode: RuntimeMode,
    ) -> Result<Self, DbError> {
        if let Some(checkpoint) = config.checkpoint.as_mut() {
            let max_state_data_bytes = checkpoint.max_staged_bytes.unwrap_or(
                laminar_core::storage::checkpoint_store::DEFAULT_MAX_CHECKPOINT_STATE_BYTES,
            );
            laminar_core::storage::checkpoint_store::validate_max_checkpoint_state_bytes(
                max_state_data_bytes,
            )
            .map_err(|error| DbError::Config(format!("checkpoint.max_staged_bytes: {error}")))?;
            checkpoint.max_staged_bytes = Some(max_state_data_bytes);
        }

        // One-time crossfire backoff tuning; idempotent, only helps single-core VMs.
        crossfire::detect_backoff_cfg();

        let lookup_registry = Arc::new(laminar_sql::datafusion::LookupTableRegistry::new());

        // Wire the LookupJoinExtensionPlanner so LookupJoinNode → LookupJoinExec.
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
        Self::register_builtin_connectors(&connector_registry)?;
        let physical_rules = extra_optimizer_rules.to_vec();

        Ok(Self {
            runtime_mode,
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
            last_fault: Arc::new(parking_lot::Mutex::new(None)),
            catalog_cleanup_fenced: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            catalog_cleanup_deregister_fault: parking_lot::Mutex::new(None),
            supervisor_self: Arc::new(parking_lot::Mutex::new(std::sync::Weak::new())),
            restart_history: Arc::new(parking_lot::Mutex::new(Vec::new())),
            lifecycle_lock: tokio::sync::Mutex::new(()),
            control_runtime: DbControlRuntime::new(runtime_mode),
            startup_attempt: parking_lot::Mutex::new(None),
            topology_ddl_lock: tokio::sync::RwLock::new(()),
            catalog_namespace: parking_lot::Mutex::new(HashMap::new()),
            #[cfg(test)]
            topology_planning_gate: parking_lot::Mutex::new(None),
            #[cfg(test)]
            stop_after_claim_gate: parking_lot::Mutex::new(None),
            #[cfg(all(test, feature = "cluster"))]
            catalog_seal_gate: parking_lot::Mutex::new(None),
            runtime_handle: tokio::sync::Mutex::new(None),
            committer_handle: tokio::sync::Mutex::new(None),
            checkpoint_namespace_lock: parking_lot::Mutex::new(None),
            owned_sink_handles: Arc::new(parking_lot::Mutex::new(Vec::new())),
            owned_source_tasks: Arc::new(parking_lot::Mutex::new(Vec::new())),
            owned_connector_task_fences: Arc::new(parking_lot::Mutex::new(Vec::new())),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            runtime_shutdown: parking_lot::RwLock::new(tokio_util::sync::CancellationToken::new()),
            engine_metrics: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            checkpoint_barrier_timings: Arc::new(
                crate::checkpoint_timing::CheckpointBarrierTimingLedger::new(),
            ),
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
            #[cfg(feature = "cluster")]
            recover_target_epoch: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            recovery_monitor: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            coordinated_lifecycle_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            coordinated_recovery_fenced: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            pending_recovery_fault: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            #[cfg(feature = "cluster")]
            last_recovery_epoch: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            source_gate: Arc::new(std::sync::atomic::AtomicBool::new(
                runtime_mode.is_cluster(),
            )),
            #[cfg(feature = "cluster")]
            cluster_authority_revoked: std::sync::atomic::AtomicBool::new(false),
            #[cfg(feature = "cluster")]
            cluster_authority_transition: parking_lot::Mutex::new(()),
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
            cluster_checkpoint_object_store: None,
            #[cfg(feature = "cluster")]
            rehydrated_vnode_state: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            #[cfg(feature = "cluster")]
            pending_revoke_vnodes: Arc::new(parking_lot::Mutex::new(
                rustc_hash::FxHashSet::default(),
            )),
            #[cfg(feature = "cluster")]
            local_state_incarnation: parking_lot::Mutex::new(None),
            #[cfg(feature = "cluster")]
            assignment_adoption_lock: tokio::sync::Mutex::new(()),
            #[cfg(feature = "cluster")]
            assignment_authority_revision: std::sync::atomic::AtomicU64::new(0),
            #[cfg(feature = "cluster")]
            rotation_execution_fence: Arc::new(tokio::sync::RwLock::new(())),
            force_ckpt_tx: parking_lot::Mutex::new(None),
            subscription_registry: Arc::new(crate::subscription::SubscriptionRegistry::new()),
            stream_schemas: parking_lot::RwLock::new(std::collections::HashMap::new()),
        })
    }

    /// Install the AI subsystem. Called by the builder; `handle` must be the
    /// main multi-threaded runtime.
    pub(crate) fn set_ai_runtime(
        &mut self,
        runtime: Arc<crate::ai::AiRuntime>,
        handle: tokio::runtime::Handle,
    ) {
        // Non-fatal: inference still works if catalog view registration fails.
        if let Err(e) = crate::ai_catalog::register_ai_catalog(&self.ctx, &runtime) {
            tracing::warn!(error = %e, "failed to register laminar.* AI catalog views");
        }
        self.ai_runtime = Some(runtime);
        self.ai_handle = Some(handle);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_sender(&self, sender: Arc<laminar_core::shuffle::ShuffleSender>) {
        *self.shuffle_sender.lock() = Some(sender);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_receiver(
        &self,
        receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) {
        *self.shuffle_receiver.lock() = Some(receiver);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn invalidate_shuffle_assignment_fence(&self) {
        self.assignment_authority_revision
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Some(receiver) = self.shuffle_receiver.lock().as_ref() {
            receiver.invalidate_assignment_fence();
        }
        if let Some(sender) = self.shuffle_sender.lock().as_ref() {
            sender.invalidate_assignment_fence();
        }
    }

    /// Close shuffle admission during temporary assignment-authority uncertainty without
    /// destroying the retained certificate's delivery sequence domain.
    #[cfg(feature = "cluster")]
    pub(crate) fn suspend_shuffle_assignment_fence(&self) {
        self.assignment_authority_revision
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if let Some(receiver) = self.shuffle_receiver.lock().as_ref() {
            receiver.suspend_assignment_fence();
        }
        if let Some(sender) = self.shuffle_sender.lock().as_ref() {
            sender.suspend_assignment_fence();
        }
    }

    #[cfg(feature = "cluster")]
    fn withdraw_assignment_authority(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
    ) {
        self.set_source_gate(true);
        controller.publish_checkpoint_drain_transition(None);
        controller.publish_checkpoint_assignment_fence(None);
        self.suspend_shuffle_assignment_fence();
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn install_shuffle_assignment_fence(
        &self,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<(), DbError> {
        let _transition = self.cluster_authority_transition.lock();
        if self
            .cluster_authority_revoked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(DbError::Checkpoint(
                "shuffle assignment install has terminally revoked process authority".into(),
            ));
        }
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("shuffle assignment install has no cluster controller".into())
        })?;
        if !controller.process_lease_is_live() {
            self.invalidate_shuffle_assignment_fence();
            return Err(DbError::Checkpoint(
                "shuffle assignment install has no live process lease".into(),
            ));
        }
        let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("shuffle assignment install has no vnode registry".into())
        })?;
        let assignment = registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        // The caller installs under the assignment-adoption and execution write fences, then
        // publishes this certificate through the controller. Requiring it to be visible first
        // exposes authority before the transport has actually switched scopes.
        if !fence.is_canonical()
            || fence.assignment_version != assignment.version()
            || fence.vnode_count != registry.vnode_count()
            || !fence.matches_owner_map(&owners)
            || fence.participant_incarnation(controller.instance_id().0)
                != Some(controller.recovery_incarnation())
        {
            self.invalidate_shuffle_assignment_fence();
            return Err(DbError::Checkpoint(format!(
                "shuffle assignment certificate does not match local assignment {} and process {}",
                assignment.version(),
                controller.instance_id().0
            )));
        }

        let receiver = self.shuffle_receiver.lock().clone();
        let sender = self.shuffle_sender.lock().clone();
        let expected_digest = fence.digest();
        let receiver_exact = receiver.as_ref().is_none_or(|endpoint| {
            endpoint.assignment_version() == fence.assignment_version
                && endpoint.active_assignment_digest() == Some(expected_digest)
        });
        let sender_exact = sender.as_ref().is_none_or(|endpoint| {
            endpoint.assignment_version() == fence.assignment_version
                && endpoint.active_assignment_digest() == Some(expected_digest)
        });
        if receiver_exact && sender_exact {
            if !controller.process_lease_is_live() {
                self.invalidate_shuffle_assignment_fence();
                return Err(DbError::Checkpoint(
                    "shuffle assignment install lost its process lease".into(),
                ));
            }
            return Ok(());
        }
        let endpoint_conflict = receiver.as_ref().is_some_and(|endpoint| {
            endpoint.local_id() != controller.instance_id().0
                || endpoint.incarnation() != controller.recovery_incarnation()
                || endpoint.assignment_version() > fence.assignment_version
                || (endpoint.assignment_version() == fence.assignment_version
                    && endpoint.active_assignment_digest() != Some(expected_digest))
        }) || sender.as_ref().is_some_and(|endpoint| {
            endpoint.local_id() != controller.instance_id().0
                || endpoint.incarnation() != controller.recovery_incarnation()
                || endpoint.assignment_version() > fence.assignment_version
                || (endpoint.assignment_version() == fence.assignment_version
                    && endpoint.active_assignment_digest() != Some(expected_digest))
        });
        if endpoint_conflict {
            self.invalidate_shuffle_assignment_fence();
            return Err(DbError::Checkpoint(
                "shuffle endpoint identity conflicts with the certified assignment".into(),
            ));
        }

        // Background and operator shuffle paths hold the execution read fence. Activate inbound
        // first, then make outbound admission the local transition's linearization point.
        let result = (|| {
            if let Some(receiver) = receiver.as_ref() {
                receiver
                    .install_assignment_fence(fence, &owners)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "failed to install receiver shuffle assignment certificate: {error}"
                        ))
                    })?;
                if receiver.assignment_version() != fence.assignment_version
                    || receiver.active_assignment_digest() != Some(expected_digest)
                {
                    return Err(DbError::Checkpoint(
                        "receiver did not activate the exact shuffle assignment certificate".into(),
                    ));
                }
            }
            if let Some(sender) = sender.as_ref() {
                sender
                    .install_assignment_fence(fence, &owners)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "failed to install sender shuffle assignment certificate: {error}"
                        ))
                    })?;
                if sender.assignment_version() != fence.assignment_version
                    || sender.active_assignment_digest() != Some(expected_digest)
                {
                    return Err(DbError::Checkpoint(
                        "sender did not activate the exact shuffle assignment certificate".into(),
                    ));
                }
            }
            Ok(())
        })();
        if result.is_ok() && !controller.process_lease_is_live() {
            self.invalidate_shuffle_assignment_fence();
            return Err(DbError::Checkpoint(
                "shuffle assignment install lost its process lease".into(),
            ));
        }
        if result.is_err() {
            // Keep watcher caches coherent with a partial endpoint install. Direct endpoint
            // invalidation would close transport authority without advancing the shared revision.
            self.invalidate_shuffle_assignment_fence();
        }
        result
    }

    #[cfg(feature = "cluster")]
    async fn assignment_recovery_admission(
        &self,
        controller: &laminar_core::cluster::control::ClusterController,
        deadline: tokio::time::Instant,
    ) -> Result<Option<laminar_core::cluster::control::RecoveryAdmissionSnapshot>, DbError> {
        let snapshot =
            tokio::time::timeout_at(deadline, controller.read_recovery_admission_snapshot())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "recovery admission audit timed out before opening intake".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "recovery admission authority is unavailable: {error}"
                    ))
                })?;
        let (committed_generation, committed_epoch) = match snapshot.committed_release() {
            None => (0, 0),
            Some(release) => match release.phase {
                laminar_core::cluster::control::RecoverPhase::ReleaseCommitted { epoch } => {
                    (release.round.id.generation, epoch)
                }
                _ => {
                    return Err(DbError::Checkpoint(
                        "latest recovery terminal is not a committed Release".into(),
                    ));
                }
            },
        };
        let local_generation = self.shuffle_recovery_generation()?;
        if !snapshot.fault_inventory().faults().is_empty() {
            controller.set_recovering(true);
            return Ok(None);
        }
        let transport_is_current =
            local_generation.is_none_or(|generation| generation == committed_generation);
        let restored_epoch = *self.last_recovery_epoch.lock();
        let state_is_current =
            committed_epoch == 0 || restored_epoch.is_some_and(|epoch| epoch >= committed_epoch);
        if transport_is_current && state_is_current {
            return Ok(Some(snapshot));
        }

        controller.set_recovering(true);
        tokio::time::timeout_at(
            deadline,
            crate::coordinated_recovery::request_local_fault(
                controller,
                &self.pending_recovery_fault,
            ),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(
                "recovery fault publication timed out for stale recovery admission".into(),
            )
        })?
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "could not publish recovery fault for stale recovery admission: {error}"
            ))
        })?;
        tracing::debug!(
            ?local_generation,
            committed_generation,
            ?restored_epoch,
            committed_epoch,
            "assignment intake remains fenced for coordinated recovery"
        );
        Ok(None)
    }

    /// Install shuffle authority, publish its controller certificate, and conditionally open
    /// source intake at one local assignment/execution boundary.
    ///
    /// `expected_revision` must have been captured before any durable reads used to derive
    /// `fence`. A concurrent closure advances the revision and this method leaves that closure in
    /// force. The controller certificate is deliberately published only after both endpoints are
    /// installed.
    #[cfg(feature = "cluster")]
    pub(crate) async fn activate_assignment_authority(
        &self,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        drain_transition: Option<laminar_core::checkpoint::AssignmentDrainTransition>,
        expected_revision: u64,
        deadline: tokio::time::Instant,
    ) -> Result<AssignmentAuthorityActivation, DbError> {
        if drain_transition
            .as_ref()
            .is_some_and(|transition| transition.predecessor != *fence)
        {
            return Err(DbError::Checkpoint(
                "assignment drain transition does not bind the installed predecessor certificate"
                    .into(),
            ));
        }
        let _adoption = tokio::time::timeout_at(deadline, self.assignment_adoption_lock.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint("timed out serializing assignment authority activation".into())
            })?;
        let _execution = tokio::time::timeout_at(
            deadline,
            Arc::clone(&self.rotation_execution_fence).write_owned(),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint("timed out draining the prior assignment execution scope".into())
        })?;
        let revision = self
            .assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire);
        if revision != expected_revision || tokio::time::Instant::now() >= deadline {
            return Ok(AssignmentAuthorityActivation {
                installed: false,
                intake_open: false,
                revision,
            });
        }

        // Installing a certificate briefly serializes source and shuffle authority. During a
        // drain, preserve an already-open predecessor execution scope: its source tasks are held
        // by the drain protocol, while its coordinator must still consume the FIFO boundary and
        // checkpoint barrier. Fresh and target-only processes enter with this gate closed and
        // remain closed until the terminal assignment is adopted.
        let intake_was_closed = self.cluster_intake_fenced();
        self.set_source_gate(true);
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("assignment activation has no cluster controller".into())
        })?;
        if !controller.process_lease_is_live() {
            self.revoke_cluster_authority();
            return Ok(AssignmentAuthorityActivation {
                installed: false,
                intake_open: false,
                revision: self
                    .assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire),
            });
        }
        let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("assignment activation has no vnode registry".into())
        })?;
        let assignment = registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let local_id = controller.instance_id().0;
        let local_incarnation = fence.participant_incarnation(local_id);
        if !fence.is_canonical()
            || fence.assignment_version != assignment.version()
            || fence.vnode_count != registry.vnode_count()
            || !fence.matches_owner_map(&owners)
            || local_incarnation
                .is_some_and(|incarnation| incarnation != controller.recovery_incarnation())
            || (local_incarnation.is_none() && owners.contains(&local_id))
        {
            return Err(DbError::Checkpoint(format!(
                "assignment certificate does not match local assignment {} and process {}",
                assignment.version(),
                local_id
            )));
        }

        // A live process outside the owner roster is control-plane ready, but has no source,
        // compute, shuffle, checkpoint, or recovery authority. Retain the audited certificate so
        // its watcher can observe a later assignment that grants ownership.
        if local_incarnation.is_none() {
            let shuffle_active = self
                .shuffle_receiver
                .lock()
                .as_ref()
                .is_some_and(|receiver| receiver.assignment_version() != 0)
                || self
                    .shuffle_sender
                    .lock()
                    .as_ref()
                    .is_some_and(|sender| sender.assignment_version() != 0);
            let revision = if shuffle_active {
                self.invalidate_shuffle_assignment_fence();
                self.assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire)
            } else {
                expected_revision
            };
            controller.publish_checkpoint_drain_transition(drain_transition);
            controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
            if !controller.process_lease_is_live()
                || self
                    .assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire)
                    != revision
            {
                if controller.process_lease_is_live() {
                    self.withdraw_assignment_authority(&controller);
                } else {
                    self.revoke_cluster_authority();
                }
                return Ok(AssignmentAuthorityActivation {
                    installed: false,
                    intake_open: false,
                    revision: self
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire),
                });
            }
            return Ok(AssignmentAuthorityActivation {
                installed: true,
                intake_open: false,
                revision,
            });
        }
        if let Err(error) = self.install_shuffle_assignment_fence(fence) {
            controller.publish_checkpoint_drain_transition(None);
            controller.publish_checkpoint_assignment_fence(None);
            return Err(error);
        }
        if self
            .assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire)
            != expected_revision
        {
            self.withdraw_assignment_authority(&controller);
            return Ok(AssignmentAuthorityActivation {
                installed: false,
                intake_open: false,
                revision: self
                    .assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire),
            });
        }

        let source_drain_active = drain_transition.is_some();
        let expected_drain_transition = drain_transition.clone();
        controller.publish_checkpoint_drain_transition(drain_transition);
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
        if self
            .assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire)
            != expected_revision
        {
            self.withdraw_assignment_authority(&controller);
            return Ok(AssignmentAuthorityActivation {
                installed: false,
                intake_open: false,
                revision: self
                    .assignment_authority_revision
                    .load(std::sync::atomic::Ordering::Acquire),
            });
        }
        let mut intake_open = false;
        let preserve_predecessor_execution = source_drain_active && !intake_was_closed;
        if !controller.is_recovering() && (!source_drain_active || preserve_predecessor_execution) {
            // Recovery authority and active faults must come from one durable view. Reading them
            // independently can pair an old terminal with the empty fault set created by a newer
            // committed Release.
            let recovery_admission = match self
                .assignment_recovery_admission(&controller, deadline)
                .await
            {
                Ok(Some(snapshot)) => snapshot,
                Ok(None) => {
                    return Ok(AssignmentAuthorityActivation {
                        installed: true,
                        intake_open: false,
                        revision: expected_revision,
                    });
                }
                Err(error) => {
                    self.withdraw_assignment_authority(&controller);
                    return Err(error);
                }
            };

            if controller.is_recovering() {
                return Ok(AssignmentAuthorityActivation {
                    installed: true,
                    intake_open: false,
                    revision: expected_revision,
                });
            }
            let shuffle_sender = { self.shuffle_sender.lock().clone() };
            if let Some(sender) = shuffle_sender {
                let mut retry_delay = std::time::Duration::from_millis(25);
                let mut last_error = None;
                loop {
                    if !controller.process_lease_is_live() {
                        self.revoke_cluster_authority();
                        return Ok(AssignmentAuthorityActivation {
                            installed: false,
                            intake_open: false,
                            revision: self
                                .assignment_authority_revision
                                .load(std::sync::atomic::Ordering::Acquire),
                        });
                    }
                    if self
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire)
                        != expected_revision
                        || controller
                            .checkpoint_assignment_fence(fence.assignment_version)
                            .as_ref()
                            != Some(fence)
                        || controller.checkpoint_drain_transition() != expected_drain_transition
                    {
                        return Ok(AssignmentAuthorityActivation {
                            installed: false,
                            intake_open: false,
                            revision: self
                                .assignment_authority_revision
                                .load(std::sync::atomic::Ordering::Acquire),
                        });
                    }
                    if tokio::time::Instant::now() >= deadline {
                        self.withdraw_assignment_authority(&controller);
                        return Err(DbError::Checkpoint(format!(
                            "assignment shuffle mesh did not become ready before the activation deadline{}",
                            last_error
                                .as_ref()
                                .map(|error| format!("; last error: {error}"))
                                .unwrap_or_default()
                        )));
                    }
                    match tokio::time::timeout_at(deadline, sender.establish_assignment_mesh(fence))
                        .await
                    {
                        Ok(Ok(())) => break,
                        Ok(Err(error)) => last_error = Some(error.to_string()),
                        Err(_) => {
                            self.withdraw_assignment_authority(&controller);
                            return Err(DbError::Checkpoint(format!(
                                "assignment shuffle mesh readiness timed out{}",
                                last_error
                                    .as_ref()
                                    .map(|error| format!("; last error: {error}"))
                                    .unwrap_or_default()
                            )));
                        }
                    }
                    let wake = tokio::time::Instant::now()
                        .checked_add(retry_delay)
                        .map_or(deadline, |wake| wake.min(deadline));
                    tokio::time::sleep_until(wake).await;
                    retry_delay = retry_delay
                        .checked_mul(2)
                        .unwrap_or(std::time::Duration::from_millis(250))
                        .min(std::time::Duration::from_millis(250));
                }
            }

            let expected_leader = expected_drain_transition
                .as_ref()
                .filter(|_| preserve_predecessor_execution)
                .map(|transition| transition.leader.clone());
            let audited_leader = match controller
                .audit_assignment_leader_authority(fence, expected_leader.as_ref(), deadline)
                .await
            {
                Ok(proof) => proof,
                Err(error) => {
                    self.withdraw_assignment_authority(&controller);
                    return Err(DbError::Checkpoint(format!(
                        "assignment leader authority audit failed before opening intake: {error}"
                    )));
                }
            };
            let recovery_admission_current = match tokio::time::timeout_at(
                deadline,
                controller.recovery_admission_is_current(&recovery_admission, &audited_leader),
            )
            .await
            {
                Ok(Ok(current)) => current,
                Ok(Err(error)) => {
                    self.withdraw_assignment_authority(&controller);
                    return Err(DbError::Checkpoint(format!(
                        "recovery admission revalidation failed before opening intake: {error}"
                    )));
                }
                Err(_) => {
                    self.withdraw_assignment_authority(&controller);
                    return Err(DbError::Checkpoint(
                        "recovery admission revalidation timed out before opening intake".into(),
                    ));
                }
            };
            if !recovery_admission_current {
                self.withdraw_assignment_authority(&controller);
                return Ok(AssignmentAuthorityActivation {
                    installed: false,
                    intake_open: false,
                    revision: self
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire),
                });
            }
            let authority_unchanged = || {
                tokio::time::Instant::now() < deadline
                    && self
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire)
                        == expected_revision
                    && !controller.is_recovering()
                    && controller.process_lease_is_live()
                    && controller.current_leader().map(|leader| leader.0)
                        == Some(audited_leader.owner.node_id)
                    && controller
                        .checkpoint_assignment_fence(fence.assignment_version)
                        .as_ref()
                        == Some(fence)
                    && controller.checkpoint_drain_transition() == expected_drain_transition
            };
            if !authority_unchanged() {
                self.withdraw_assignment_authority(&controller);
                return Ok(AssignmentAuthorityActivation {
                    installed: false,
                    intake_open: false,
                    revision: self
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire),
                });
            }
            self.set_source_gate(false);
            if authority_unchanged() {
                intake_open = true;
            } else {
                self.withdraw_assignment_authority(&controller);
                return Ok(AssignmentAuthorityActivation {
                    installed: false,
                    intake_open: false,
                    revision: self
                        .assignment_authority_revision
                        .load(std::sync::atomic::Ordering::Acquire),
                });
            }
        }

        Ok(AssignmentAuthorityActivation {
            installed: true,
            intake_open,
            revision: expected_revision,
        })
    }

    /// Drop buffered shuffle slices and stashed barriers before a rewind: their senders
    /// rewind and replay them, so folding a buffered copy afterwards double-counts.
    #[cfg(feature = "cluster")]
    pub(crate) fn purge_shuffle_receiver_buffers(&self) {
        let Some(receiver) = self.shuffle_receiver.lock().clone() else {
            return;
        };
        let queued = receiver.drain_available().len();
        let staged = receiver.drain_all_staged().len();
        let barriers = receiver.drain_staged_barriers().len();
        if queued + staged + barriers > 0 {
            tracing::info!(
                queued,
                staged,
                barriers,
                "purged stale shuffle buffers before coordinated rewind"
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

    /// Install the shared catalog manifest store.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_catalog_manifest_store(
        &self,
        store: Arc<laminar_core::cluster::control::CatalogManifestStore>,
    ) {
        *self.catalog_manifest_store.lock() = Some(store);
    }

    /// Install the shared checkpoint namespace before the database is published behind `Arc`.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_cluster_checkpoint_object_store(
        &mut self,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<(), DbError> {
        if !self.is_cluster_runtime() || self.cluster_controller.lock().is_none() {
            return Err(DbError::Config(
                "cluster checkpoint object store requires a cluster controller".into(),
            ));
        }
        self.cluster_checkpoint_object_store = Some(store);
        Ok(())
    }

    /// Clone the immutable cluster checkpoint namespace selected at construction.
    #[cfg(feature = "cluster")]
    pub(crate) fn cluster_checkpoint_object_store(
        &self,
    ) -> Option<Arc<dyn object_store::ObjectStore>> {
        self.cluster_checkpoint_object_store.clone()
    }

    #[cfg(feature = "cluster")]
    fn catalog_manifest_inventory(
        &self,
    ) -> Result<Vec<laminar_core::cluster::control::CatalogManifestEntry>, DbError> {
        let ordered = self.connector_manager.lock().ordered_ddl();
        let namespace = self.catalog_namespace.lock();
        if ordered.len() != namespace.len() {
            return Err(DbError::Pipeline(format!(
                "typed catalog has {} objects but the durable DDL inventory has {}",
                namespace.len(),
                ordered.len()
            )));
        }
        ordered
            .into_iter()
            .map(|(canonical_name, ddl)| {
                let kind = namespace.get(&canonical_name).copied().ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "DDL inventory entry '{canonical_name}' has no typed catalog owner"
                    ))
                })?;
                Ok(laminar_core::cluster::control::CatalogManifestEntry {
                    canonical_name,
                    kind,
                    ddl,
                })
            })
            .collect()
    }

    #[cfg(feature = "cluster")]
    fn exact_bootstrap_noop(
        &self,
        sql: &str,
        statement: &StreamingStatement,
    ) -> Result<Option<ExecuteResult>, DbError> {
        let Some((name, kind, statement_type)) = catalog_create_identity(statement)? else {
            return Ok(None);
        };
        let local_ddl = self
            .connector_manager
            .lock()
            .get_ddl(&name)
            .map(str::to_owned);
        let local_kind = self.catalog_namespace.lock().get(&name).copied();
        if local_ddl.is_none() && local_kind.is_none() {
            return Ok(None);
        }
        if local_ddl.as_deref() != Some(sql) || local_kind != Some(kind) {
            return Err(DbError::Pipeline(format!(
                "cluster bootstrap definition for '{name}' differs from the durable typed catalog"
            )));
        }
        Ok(Some(ExecuteResult::Ddl(DdlInfo {
            statement_type: statement_type.to_string(),
            object_name: name,
            applied: false,
        })))
    }

    #[cfg(feature = "cluster")]
    fn validate_catalog_seal_authority(
        &self,
        proof: Option<&laminar_core::cluster::control::LeaderProof>,
    ) -> Result<(), DbError> {
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Pipeline(
                "[LDB-6043] cluster catalog bootstrap requires a cluster controller".into(),
            )
        })?;
        let Some(proof) = proof else {
            return Err(DbError::Pipeline(
                "[LDB-6043] cluster catalog bootstrap requires the active durable leader lease"
                    .into(),
            ));
        };
        if !controller.catalog_bootstrap_proof_is_live(proof) {
            return Err(DbError::Pipeline(
                "[LDB-6043] cluster catalog bootstrap lost its durable leader lease before sealing"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Replay catalog DDL from the shared manifest before cluster startup.
    ///
    /// # Errors
    /// Fails closed when the manifest cannot be loaded, a local definition conflicts with it, or
    /// any entry cannot be recreated. A node must never start with a partial cluster topology.
    #[cfg(feature = "cluster")]
    pub(crate) async fn restore_catalog_from_manifest(
        &self,
    ) -> Result<Option<laminar_core::cluster::control::CatalogManifest>, DbError> {
        self.connector_registry.freeze();
        let Some(store) = self.catalog_manifest_store.lock().clone() else {
            return Ok(None);
        };
        let Some(manifest) = store.load().await.map_err(|error| {
            DbError::Pipeline(format!(
                "[{}] catalog manifest load failed: {error}",
                laminar_core::error_codes::RECOVERY_FAILED
            ))
        })?
        else {
            return Ok(None);
        };

        let mut replay_guard = CatalogBootstrapGuard {
            db: self,
            created: Vec::new(),
            sealed: false,
        };
        for entry in &manifest.entries {
            if catalog_ddl_contains_comment(&entry.ddl)? {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' contains SQL comments rather than one canonical typed definition",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            }
            let statements = parse_streaming_sql(&entry.ddl).map_err(|error| {
                DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' is not valid topology DDL: {error}",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                ))
            })?;
            if statements.len() != 1 {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' must contain exactly one typed CREATE statement",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            }
            let Some((name, kind, _)) = catalog_create_identity(&statements[0])? else {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' must contain exactly one typed CREATE statement",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            };
            if name != entry.canonical_name || kind != entry.kind {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' does not match its typed DDL identity",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            }
            if connector_source_requires_schema_discovery(&statements[0]) {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest source '{}' lacks an explicit durable schema",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            }
            if let Some(key) = sensitive_catalog_property(&statements[0]) {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' contains secret property '{key}'",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            }
        }

        for entry in &manifest.entries {
            let local_ddl = self
                .connector_manager
                .lock()
                .get_ddl(&entry.canonical_name)
                .map(str::to_owned);
            if let Some(local_ddl) = local_ddl {
                let local_kind = self
                    .catalog_namespace
                    .lock()
                    .get(&entry.canonical_name)
                    .copied();
                if local_ddl != entry.ddl || local_kind != Some(entry.kind) {
                    return Err(DbError::Pipeline(format!(
                        "[{}] local catalog definition for '{}' conflicts with catalog manifest",
                        laminar_core::error_codes::RECOVERY_FAILED,
                        entry.canonical_name
                    )));
                }
                continue;
            }
            CATALOG_MANIFEST_REPLAY
                .scope((), self.execute_single_already_gated(&entry.ddl))
                .await
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "[{}] catalog manifest replay failed for '{}': {error}",
                        laminar_core::error_codes::RECOVERY_FAILED,
                        entry.canonical_name
                    ))
                })?;
            let replayed_ddl = self
                .connector_manager
                .lock()
                .get_ddl(&entry.canonical_name)
                .map(str::to_owned);
            let replayed_kind = self
                .catalog_namespace
                .lock()
                .get(&entry.canonical_name)
                .copied();
            if replayed_ddl.as_deref() != Some(entry.ddl.as_str())
                || replayed_kind != Some(entry.kind)
            {
                return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' completed without installing its exact \
                     durable DDL identity",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
            }
            replay_guard.record(entry.canonical_name.clone(), entry.kind);
            tracing::info!(name = %entry.canonical_name, "replayed catalog DDL from manifest");
        }

        let local_inventory = self.catalog_manifest_inventory()?;
        if local_inventory.as_slice() != manifest.entries.as_slice() {
            return Err(DbError::Pipeline(format!(
                "[{}] local catalog DDL inventory does not match the ordered catalog manifest \
                 (local entries: {}, manifest entries: {}); refusing cluster startup",
                laminar_core::error_codes::RECOVERY_FAILED,
                local_inventory.len(),
                manifest.entries.len()
            )));
        }
        replay_guard.sealed();
        Ok(Some(manifest))
    }

    /// Atomically adopt a new vnode assignment across the registry, state-backend
    /// fence, and coordinator, then rehydrate committed state for newly-acquired
    /// vnodes. Idempotent for versions ≤ the current registry version.
    ///
    /// Rehydration runs after the coordinator lock is released so a slow
    /// object-store read can't stall the checkpoint cadence.
    ///
    /// # Errors
    /// Returns a checkpoint error when the end-to-end deadline expires or source-offset handoff
    /// or vnode-state rehydration fails. The assignment is not published in that case, so the
    /// same snapshot remains retryable.
    #[cfg(feature = "cluster")]
    pub async fn adopt_assignment_snapshot(
        &self,
        snapshot: laminar_core::cluster::control::AssignmentSnapshot,
        deadline: tokio::time::Instant,
    ) -> Result<SnapshotAdoption, DbError> {
        let version = snapshot.version;
        tokio::time::timeout_at(deadline, async {
            let _adoption = self.assignment_adoption_lock.lock().await;
            self.adopt_assignment_snapshot_locked(snapshot, deadline)
                .await
        })
        .await
        .unwrap_or_else(|_| {
            Err(DbError::Checkpoint(format!(
                "assignment {version} adoption exceeded its end-to-end deadline"
            )))
        })
    }

    #[cfg(feature = "cluster")]
    async fn adopt_assignment_snapshot_locked(
        &self,
        snapshot: laminar_core::cluster::control::AssignmentSnapshot,
        deadline: tokio::time::Instant,
    ) -> Result<SnapshotAdoption, DbError> {
        if snapshot.draining {
            return Err(DbError::Checkpoint(format!(
                "assignment {} is a draining generation and cannot publish ownership",
                snapshot.version
            )));
        }
        if !snapshot.has_canonical_participants() {
            return Err(DbError::Checkpoint(format!(
                "assignment {} has no canonical process roster",
                snapshot.version
            )));
        }
        let Some(registry) = self.vnode_registry.lock().clone() else {
            return Ok(SnapshotAdoption::default());
        };
        let vnode_count = registry.vnode_count();
        let new_assignment: Arc<[laminar_core::state::NodeId]> = snapshot
            .to_vnode_vec(vnode_count)
            .map_err(|error| DbError::Checkpoint(error.to_string()))?
            .into();
        let controller = self.cluster_controller.lock().clone();
        let assignment_snapshot_store = self.assignment_snapshot_store.lock().clone();
        if let Some(store) = assignment_snapshot_store.as_ref() {
            crate::rebalance::audit_assignment_snapshot_authority(
                store,
                controller.as_deref(),
                &snapshot,
            )
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "assignment {} authority audit failed: {error}",
                    snapshot.version
                ))
            })?;
        }
        if snapshot.version <= registry.assignment_version() {
            return Ok(SnapshotAdoption {
                adopted: false,
                version: snapshot.version,
                ..SnapshotAdoption::default()
            });
        }
        // The target is now durable-authority-audited and still newer. Close every predecessor
        // admission path before reading its process roster, source handoff, or vnode state. Scope
        // cancellation releases compute cycles blocked in shuffle so the final write fence can
        // drain; any later error deliberately leaves this authority closed for a full retry.
        self.set_source_gate(true);
        if let Some(controller) = controller.as_ref() {
            controller.publish_checkpoint_assignment_fence(None);
        }
        self.invalidate_shuffle_assignment_fence();
        let predecessor_drain = Arc::clone(&self.rotation_execution_fence)
            .write_owned()
            .await;
        drop(predecessor_drain);

        let self_id = controller
            .as_ref()
            .map_or(laminar_core::state::NodeId(0), |controller| {
                laminar_core::state::NodeId(controller.instance_id().0)
            });
        let observed_assignment = registry.versioned_snapshot();
        let observed_version = observed_assignment.version();
        let current_incarnation = controller
            .as_ref()
            .map(|controller| controller.recovery_incarnation());
        let local_state_is_current = current_incarnation
            .is_some_and(|incarnation| *self.local_state_incarnation.lock() == Some(incarnation));
        let force_local_restore = if observed_version == 0 || local_state_is_current {
            false
        } else if let (Some(store), Some(incarnation)) =
            (assignment_snapshot_store, current_incarnation)
        {
            let prior = store
                .load_version(observed_version)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "failed to load assignment {observed_version} process roster: {error}"
                    ))
                })?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "assignment {observed_version} process roster is unavailable"
                    ))
                })?;
            crate::rebalance::audit_assignment_snapshot_authority(
                &store,
                controller.as_deref(),
                &prior,
            )
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "assignment {observed_version} authority audit failed: {error}"
                ))
            })?;
            prior
                .to_vnode_vec(vnode_count)
                .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            prior
                .participants
                .binary_search_by_key(&self_id.0, |participant| participant.node_id)
                .ok()
                .and_then(|index| prior.participants.get(index))
                .map(|participant| participant.boot_incarnation)
                != Some(incarnation)
        } else if current_incarnation.is_some() {
            return Err(DbError::Checkpoint(format!(
                "cannot verify local state incarnation for assignment {observed_version} without durable assignment history"
            )));
        } else {
            false
        };
        // Hold the coord mutex so registry + fence updates land between epochs.
        let guard = self.coordinator.lock().await;
        // Re-check under the lock: a concurrent adopt may have advanced the version,
        // which we must not regress.
        if snapshot.version <= registry.assignment_version() {
            return Ok(SnapshotAdoption {
                adopted: false,
                version: snapshot.version,
                ..SnapshotAdoption::default()
            });
        }

        let prepared_assignment = registry.versioned_snapshot();
        let prepared_from_version = prepared_assignment.version();
        if prepared_from_version != observed_version {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6053] assignment base advanced from {observed_version} to {prepared_from_version} while preparing target {}",
                snapshot.version
            )));
        }
        let old_owned = owned_vnode_indices(prepared_assignment.owners(), self_id)?;
        let old_set: std::collections::HashSet<u32> = old_owned.iter().copied().collect();
        let new_owned = owned_vnode_indices(&new_assignment, self_id)?;
        let skipped_assignment_generation =
            snapshot.version > prepared_from_version.saturating_add(1);
        // Compute from the new assignment before publishing it, so the Restoring marks
        // below land before the ownership flip.
        let newly_acquired: Vec<u32> = (0..vnode_count)
            .filter(|&v| {
                new_assignment.get(v as usize).copied() == Some(self_id)
                    && (force_local_restore
                        || skipped_assignment_generation
                        || !old_set.contains(&v))
            })
            .collect();
        let source_handoff_required = !newly_acquired.is_empty();

        // Snapshot immutable recovery handles at the epoch boundary, then release the coordinator
        // mutex before decision-store, seal, readiness, and vnode reads. Remote object-store
        // latency must not stall checkpoint admission.
        let handoff_reader = if !source_handoff_required {
            None
        } else if let Some(coord) = guard.as_ref() {
            Some(coord.cluster_handoff_reader()?.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6052] assignment {} requires an active cluster recovery namespace",
                    snapshot.version
                ))
            })?)
        } else {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6052] cannot acquire {} vnodes for assignment {} without a live checkpoint coordinator",
                newly_acquired.len(), snapshot.version
            )));
        };

        drop(guard);
        // Stage the sealed source offsets and read all newly-owned state before publishing the
        // assignment. Any failure leaves the current version intact and retryable.
        let source_handoff = if let Some(reader) = handoff_reader.as_ref() {
            reader.acquired_source_handoff().await.map_err(|e| {
                DbError::Checkpoint(format!(
                    "[LDB-6052] source-offset handoff read failed for assignment {}: {e}",
                    snapshot.version
                ))
            })?
        } else {
            None
        };
        let prepared_outcome = source_handoff
            .as_ref()
            .map(|handoff| handoff.outcome.clone());
        if let Some(outcome) = prepared_outcome.as_ref() {
            let outcome_fence = outcome.assignment_fence.as_ref().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6054] cluster source handoff Commit outcome has no assignment certificate"
                        .into(),
                )
            })?;
            if outcome_fence.assignment_version > snapshot.version {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6054] assignment {} is older than durable checkpoint fence {}; refresh the assignment snapshot before adoption",
                    snapshot.version, outcome_fence.assignment_version
                )));
            }
        }
        let mut rehydration = if let Some(handoff) = source_handoff
            .as_ref()
            .filter(|_| !newly_acquired.is_empty())
        {
            let backend = self.state_backend.lock().clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6050] cluster assignment adoption requires a state backend".into(),
                )
            })?;
            let attempt = laminar_core::state::CheckpointAttempt::new(
                handoff.outcome.epoch,
                handoff.outcome.checkpoint_id,
            );
            crate::recovery_manager::VnodeRehydrator::new(backend.as_ref())
                .rehydrate_at(&newly_acquired, attempt)
                .await?
        } else {
            crate::recovery_manager::VnodeRehydration::default()
        };

        let observed_outcome = if let Some(reader) = handoff_reader.as_ref() {
            reader.highest_commit_outcome().await?
        } else {
            None
        };

        // Re-acquire the epoch boundary and discard the prepared adoption if another rotation won,
        // the coordinator namespace changed, or a newer durable cut appeared during the reads.
        let _execution_guard = Arc::clone(&self.rotation_execution_fence)
            .write_owned()
            .await;
        let mut guard = self.coordinator.lock().await;
        if tokio::time::Instant::now() >= deadline {
            return Err(DbError::Checkpoint(format!(
                "assignment {} adoption reached its deadline before publication",
                snapshot.version
            )));
        }
        let current_version = registry.assignment_version();
        if snapshot.version <= current_version {
            return Ok(SnapshotAdoption {
                adopted: false,
                version: snapshot.version,
                ..SnapshotAdoption::default()
            });
        }
        if current_version != prepared_from_version {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6053] assignment base advanced from {prepared_from_version} to {current_version} while preparing target {}; retrying from the new owner set",
                snapshot.version
            )));
        }
        if source_handoff_required {
            let current_reader = guard
                .as_ref()
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6052] checkpoint coordinator disappeared while preparing assignment {}",
                        snapshot.version
                    ))
                })?
                .cluster_handoff_reader()?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "[LDB-6052] cluster recovery namespace disappeared while preparing assignment {}",
                        snapshot.version
                    ))
                })?;
            let prepared_reader = handoff_reader.as_ref().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6052] assignment {} lost its prepared cluster recovery namespace",
                    snapshot.version
                ))
            })?;
            if !prepared_reader.same_namespace(&current_reader) {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6053] checkpoint recovery namespace changed while preparing assignment {}; retrying the complete source/state handoff",
                    snapshot.version
                )));
            }
            if observed_outcome != prepared_outcome {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6053] durable checkpoint Commit outcome advanced while preparing assignment {}; retrying the complete source/state handoff",
                    snapshot.version
                )));
            }
        }
        if let Some(handoff) = source_handoff.as_ref() {
            let expected_attempt = laminar_core::state::CheckpointAttempt::new(
                handoff.outcome.epoch,
                handoff.outcome.checkpoint_id,
            );
            if handoff.sources.attempt() != expected_attempt {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6054] committed source handoff {:?} does not match durable outcome {expected_attempt:?}",
                    handoff.sources.attempt()
                )));
            }
            for (source_name, _) in handoff.sources.sources() {
                if self.catalog.get_source(source_name).is_none() {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6054] committed source handoff names unknown catalog source '{source_name}'"
                    )));
                }
            }
            let controller = controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6054] committed source handoff has no live cluster controller".into(),
                )
            })?;
            match (
                controller.cluster_min_watermark(),
                handoff.sources.recovery_watermark_frontier(),
            ) {
                (Some(current), Some(recovered)) if current > recovered => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6054] live committed cluster watermark {current} is ahead of source handoff frontier {recovered}"
                    )));
                }
                (Some(current), None) => {
                    return Err(DbError::Checkpoint(format!(
                        "[LDB-6054] {:?} source handoff without a numeric frontier cannot replace committed cluster watermark {current}",
                        handoff.sources.cluster_watermark()
                    )));
                }
                _ => {}
            }
        }
        let new_set: std::collections::HashSet<u32> = new_owned.iter().copied().collect();
        let revoked: Vec<u32> = old_set.difference(&new_set).copied().collect();
        let rehydration_attempt = rehydration.attempt;
        let adoption = SnapshotAdoption {
            adopted: true,
            version: snapshot.version,
            newly_acquired: newly_acquired.clone(),
            rehydrated: rehydration.restored.len(),
            rehydration_epoch: rehydration_attempt.map(|attempt| attempt.epoch),
        };

        // Stage revoke and rehydration work while the compute-cycle write fence is held, then
        // publish ownership before releasing either staging mutex. The next cycle must therefore
        // apply both maps before any local or shuffled row can observe the new owner set.
        let mut pending_revoke = self.pending_revoke_vnodes.lock();
        let mut staged_rehydration = self.rehydrated_vnode_state.lock();
        pending_revoke.extend(revoked);
        staged_rehydration.retain(|vnode, _| new_set.contains(vnode));
        if let Some(attempt) = rehydration_attempt {
            for vnode in &newly_acquired {
                staged_rehydration.insert(
                    *vnode,
                    RehydratedVnode {
                        epoch: attempt.epoch,
                        chain: rehydration.restored.remove(vnode).unwrap_or_default(),
                    },
                );
            }
            registry.mark_restoring(&newly_acquired);
        }

        if let Some(watermark) = source_handoff
            .as_ref()
            .and_then(|handoff| handoff.sources.recovery_watermark_frontier())
        {
            controller
                .as_ref()
                .expect("validated source handoff has a cluster controller")
                .publish_cluster_min_watermark(watermark);
        }
        if let Some(handoff) = source_handoff {
            registry.set_assignment_and_version_with_source_handoff(
                new_assignment,
                snapshot.version,
                handoff.sources,
            );
        } else if source_handoff_required {
            // Genesis has no committed cut. Keep that distinct from a committed
            // empty cut so sources may use their start-captured numeric baseline.
            registry.set_assignment_and_version(new_assignment, snapshot.version);
            registry.mark_active(&newly_acquired);
        } else {
            registry.set_assignment_and_version_carrying_source_handoff(
                new_assignment,
                snapshot.version,
            );
        }
        if let Some(backend) = self.state_backend.lock().clone() {
            backend.set_authoritative_version(snapshot.version);
        }
        if let Some(coord) = guard.as_mut() {
            coord.set_assignment_version(snapshot.version);
            coord.set_vnode_set(new_owned.clone());
            coord.set_gate_vnode_set((0..vnode_count).collect());
        }
        if let Some(incarnation) = current_incarnation {
            *self.local_state_incarnation.lock() = Some(incarnation);
        }
        drop(staged_rehydration);
        drop(pending_revoke);
        drop(guard);

        tracing::info!(
            version = snapshot.version,
            newly_acquired = adoption.newly_acquired.len(),
            rehydrated = adoption.rehydrated,
            rehydration_epoch = ?adoption.rehydration_epoch,
            "adopted assignment snapshot",
        );
        Ok(adoption)
    }

    /// Wait for every local source to publish an exact FIFO drain receipt.
    #[cfg(feature = "cluster")]
    pub(crate) async fn prepare_local_source_drain(
        &self,
        transition: &laminar_core::checkpoint::AssignmentDrainTransition,
        participant: laminar_core::checkpoint::CheckpointParticipant,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        crate::pipeline::streaming_coordinator::prepare_owned_source_drain(
            &self.owned_source_tasks,
            transition,
            participant,
            deadline,
        )
        .await
        .map_err(|error| DbError::Checkpoint(format!("source drain failed: {error}")))
    }

    /// Resolve the exact local source drain after target commit or abort.
    #[cfg(feature = "cluster")]
    pub(crate) async fn resolve_local_source_drain(
        &self,
        round: laminar_core::checkpoint::AssignmentDrainId,
        outcome: laminar_connectors::connector::SourceDrainOutcome,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        crate::pipeline::streaming_coordinator::resolve_owned_source_drain(
            &self.owned_source_tasks,
            laminar_connectors::connector::SourceDrainResolution { round, outcome },
            deadline,
        )
        .await
        .map_err(|error| DbError::Checkpoint(format!("source drain resolution failed: {error}")))
    }

    /// Validate a draining generation without changing local ownership. Source intake is held by
    /// the source-task drain protocol until the durable outcome is resolved.
    #[cfg(feature = "cluster")]
    pub(crate) fn validate_source_drain_snapshot(
        &self,
        snapshot: &laminar_core::cluster::control::AssignmentSnapshot,
    ) -> Result<(), DbError> {
        if !snapshot.draining {
            return Err(DbError::Checkpoint(format!(
                "assignment {} is not a draining generation",
                snapshot.version
            )));
        }
        if !snapshot.has_canonical_participants() {
            return Err(DbError::Checkpoint(format!(
                "draining assignment {} has no canonical process roster",
                snapshot.version
            )));
        }
        let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "draining assignment {} cannot be validated without a vnode registry",
                snapshot.version
            ))
        })?;
        let expected_version = registry
            .assignment_version()
            .checked_add(1)
            .ok_or_else(|| DbError::Checkpoint("assignment version overflow".into()))?;
        if snapshot.version != expected_version {
            return Err(DbError::Checkpoint(format!(
                "draining assignment {} is not the exact successor of local assignment {}",
                snapshot.version,
                registry.assignment_version()
            )));
        }
        snapshot
            .to_vnode_vec(registry.vnode_count())
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        tracing::info!(
            version = snapshot.version,
            "validated global source-drain generation"
        );
        Ok(())
    }

    /// Staged vnode state from the most recent rebalance adoptions, keyed by vnode.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn rehydrated_vnode_state(&self) -> HashMap<u32, RehydratedVnode> {
        self.rehydrated_vnode_state.lock().clone()
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_cluster_controller(
        &self,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
    ) -> Result<(), DbError> {
        if !self.is_cluster_runtime() {
            return Err(DbError::Config(
                "cluster controller cannot be installed on a local runtime".into(),
            ));
        }
        *self.cluster_controller.lock() = Some(controller);
        Ok(())
    }

    pub(crate) fn set_state_backend(&self, backend: Arc<dyn laminar_core::state::StateBackend>) {
        *self.state_backend.lock() = Some(backend);
    }

    pub(crate) fn set_vnode_registry(&self, registry: Arc<laminar_core::state::VnodeRegistry>) {
        *self.vnode_registry.lock() = Some(registry);
    }

    /// Collect this node's physical table slice without invoking distributed fan-out.
    ///
    /// This is intended for cluster diagnostics and placement validation. It deliberately returns
    /// immutable batches rather than exposing the mutable `DataFusion` catalog.
    ///
    /// # Errors
    /// Returns an error when the local table cannot be resolved, planned, or collected.
    #[cfg(feature = "cluster")]
    pub async fn collect_local_table(&self, name: &str) -> Result<Vec<RecordBatch>, DbError> {
        const LOCAL_SCAN_NAME: &str = "__laminar_local_table_scan";

        let _catalog_guard = self.topology_ddl_lock.read().await;
        let provider = self.ctx.table_provider(exact_table_reference(name)).await?;
        let context = SessionContext::new();
        context.register_table(exact_table_reference(LOCAL_SCAN_NAME), provider)?;
        Ok(context
            .sql(&format!("SELECT * FROM {LOCAL_SCAN_NAME}"))
            .await?
            .collect()
            .await?)
    }

    /// Returns a fluent builder for constructing a [`LaminarDB`].
    #[must_use]
    pub fn builder() -> LaminarDbBuilder {
        LaminarDbBuilder::new()
    }

    #[allow(unused_variables)]
    fn register_builtin_connectors(
        registry: &laminar_connectors::registry::ConnectorRegistry,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        laminar_connectors::generator::register_generator_source(registry)?;
        #[cfg(feature = "kafka")]
        {
            laminar_connectors::kafka::register_kafka_source(registry)?;
            laminar_connectors::kafka::register_kafka_sink(registry)?;
        }
        #[cfg(feature = "postgres-cdc")]
        {
            laminar_connectors::postgres::register_postgres_cdc_source(registry)?;
        }
        #[cfg(feature = "postgres-sink")]
        {
            laminar_connectors::postgres::register_postgres_sink(registry)?;
        }
        #[cfg(feature = "delta-lake")]
        {
            laminar_connectors::lakehouse::register_delta_lake_sink(registry)?;
            laminar_connectors::lakehouse::register_delta_lake_source(registry)?;
        }
        #[cfg(feature = "iceberg")]
        {
            laminar_connectors::lakehouse::register_iceberg_sink(registry)?;
            laminar_connectors::lakehouse::register_iceberg_source(registry)?;
        }
        #[cfg(feature = "websocket")]
        {
            laminar_connectors::websocket::register_websocket_source(registry)?;
            laminar_connectors::websocket::register_websocket_sink(registry)?;
        }
        #[cfg(feature = "mongodb-cdc")]
        {
            laminar_connectors::mongodb::register_mongodb_cdc_source(registry)?;
            laminar_connectors::mongodb::register_mongodb_sink(registry)?;
        }
        #[cfg(feature = "files")]
        {
            laminar_connectors::files::register_file_source(registry)?;
            laminar_connectors::files::register_file_sink(registry)?;
        }
        #[cfg(feature = "otel")]
        {
            laminar_connectors::otel::register_otel_source(registry)?;
        }
        #[cfg(feature = "nats")]
        {
            laminar_connectors::nats::register_nats_source(registry)?;
            laminar_connectors::nats::register_nats_sink(registry)?;
        }
        Ok(())
    }

    fn handle_register_lookup_table(
        &self,
        info: laminar_sql::planner::LookupTableInfo,
    ) -> Result<ExecuteResult, DbError> {
        use laminar_sql::parser::lookup_table::LookupConnector;

        self.preflight_lookup_connector(&info.properties)?;
        if info.primary_key.len() != 1 {
            return Err(DbError::InvalidOperation(
                "Lookup table requires a single-column primary key".into(),
            ));
        }
        let pk = info.primary_key[0].clone();

        self.table_store
            .write()
            .create_table(&info.name, info.arrow_schema.clone(), &pk)?;

        if matches!(&info.properties.connector, LookupConnector::External(_)) {
            self.register_lookup_connector(&info, &pk);
        }

        {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                info.name.clone(),
                info.arrow_schema.clone(),
                self.table_store.clone(),
            );
            match self
                .ctx
                .register_table(exact_table_reference(&info.name), Arc::new(provider))
            {
                Ok(None) => {}
                Ok(Some(previous)) => {
                    let _ = self
                        .ctx
                        .register_table(exact_table_reference(&info.name), previous);
                    return Err(DbError::InvalidOperation(format!(
                        "cannot create lookup table '{}': its provider was claimed concurrently",
                        info.name
                    )));
                }
                Err(error) => {
                    return Err(DbError::InvalidOperation(format!(
                        "failed to register lookup table '{}': {error}",
                        info.name
                    )));
                }
            }
        }

        if let Some(batch) = self.table_store.read().to_record_batch(&info.name)? {
            self.lookup_registry.register(
                &info.name,
                laminar_sql::datafusion::LookupSnapshot { batch },
            );
        }

        self.refresh_lookup_optimizer_rule();

        Ok(ExecuteResult::Ddl(DdlInfo {
            statement_type: "CREATE LOOKUP TABLE".to_string(),
            object_name: info.name,
            applied: true,
        }))
    }

    fn preflight_lookup_connector(
        &self,
        properties: &laminar_sql::parser::lookup_table::LookupTableProperties,
    ) -> Result<(), DbError> {
        use laminar_sql::parser::lookup_table::{LookupConnector, LookupStrategy};

        if properties.strategy == LookupStrategy::OnDemand
            && self.config.delivery_guarantee
                == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
        {
            return Err(DbError::InvalidOperation(
                "on-demand LOOKUP TABLE is incompatible with exactly-once delivery because \
                 external lookup results are not checkpointed"
                    .into(),
            ));
        }
        let connector = match &properties.connector {
            LookupConnector::Static => {
                if properties.strategy == LookupStrategy::OnDemand {
                    return Err(DbError::InvalidOperation(
                        "static LOOKUP TABLE supports only the replicated strategy".into(),
                    ));
                }
                return Ok(());
            }
            LookupConnector::External(name) => name,
        };
        let (available, capability) = match properties.strategy {
            LookupStrategy::Replicated => (
                self.connector_registry.has_table_source(connector),
                "snapshot-capable table source",
            ),
            LookupStrategy::OnDemand => (
                self.connector_registry.has_lookup_source(connector),
                "on-demand lookup source",
            ),
        };
        if !available {
            return Err(DbError::InvalidOperation(format!(
                "LOOKUP TABLE connector '{connector}' has no registered {capability} required \
                 by strategy '{}'",
                properties.strategy
            )));
        }
        Ok(())
    }

    fn register_lookup_connector(&self, info: &laminar_sql::planner::LookupTableInfo, pk: &str) {
        use laminar_sql::parser::lookup_table::LookupConnector;

        let connector_type = match &info.properties.connector {
            LookupConnector::External(name) => name.clone(),
            LookupConnector::Static => unreachable!(),
        };

        self.table_store
            .write()
            .set_connector(&info.name, &connector_type);

        // Keys consumed by LookupTableProperties are excluded; "format.*" keys
        // go to format_options with the prefix stripped.
        let consumed = [
            "connector",
            "strategy",
            "cache.memory",
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

        // Carry as bytes; the partial lookup cache is byte-weighted, not entry-counted.
        let cache_max_bytes = info
            .properties
            .cache_memory
            .map(|m| usize::try_from(m.as_bytes()).unwrap_or(usize::MAX));

        let cache_ttl = info
            .properties
            .cache_ttl
            .map(std::time::Duration::from_secs);

        self.connector_manager
            .lock()
            .register_table(crate::connector_manager::TableRegistration {
                name: info.name.clone(),
                primary_key: pk.to_string(),
                connector_type: Some(connector_type),
                connector_options,
                format: info.raw_options.get("format").cloned(),
                format_options,
                on_demand: matches!(
                    info.properties.strategy,
                    laminar_sql::parser::lookup_table::LookupStrategy::OnDemand
                ),
                cache_max_bytes,
                cache_ttl,
            });
    }

    /// Rebuild the lookup optimizer rules for the current set of registered tables.
    pub(crate) fn refresh_lookup_optimizer_rule(&self) {
        use laminar_sql::planner::lookup_join::{LookupColumnPruningRule, LookupJoinRewriteRule};
        use laminar_sql::planner::predicate_split::{
            PlanPushdownMode, PlanSourceCapabilities, PredicateSplitterRule,
            SourceCapabilitiesRegistry,
        };

        self.ctx.remove_optimizer_rule("lookup_join_rewrite");
        self.ctx.remove_optimizer_rule("predicate_splitter");
        self.ctx.remove_optimizer_rule("lookup_column_pruning");

        let tables = self.planner.lock().lookup_tables_cloned();
        if tables.is_empty() {
            return;
        }

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

        self.ctx
            .add_optimizer_rule(Arc::new(LookupJoinRewriteRule::new(tables)));
        self.ctx
            .add_optimizer_rule(Arc::new(PredicateSplitterRule::new(caps_registry)));
        self.ctx
            .add_optimizer_rule(Arc::new(LookupColumnPruningRule));
    }

    /// Returns the frozen connector registry for factory lookup and deployment introspection.
    /// Custom factories must be registered through [`LaminarDbBuilder::register_connector`].
    #[must_use]
    pub fn connector_registry(&self) -> &laminar_connectors::registry::ConnectorRegistry {
        &self.connector_registry
    }

    /// Register a custom scalar UDF. Called by the builder after construction.
    pub(crate) fn register_custom_udf(&self, udf: datafusion_expr::ScalarUDF) {
        self.ctx.register_udf(udf);
    }

    /// Register a custom aggregate UDF. Called by the builder after construction.
    pub(crate) fn register_custom_udaf(&self, udaf: datafusion_expr::AggregateUDF) {
        self.ctx.register_udaf(udaf);
    }

    /// Execute a SQL statement.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if SQL parsing, planning, or execution fails.
    pub async fn execute(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::Shutdown);
        }

        let stmts = sql_utils::split_statements(sql);
        if stmts.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }

        let mut last_result = None;
        for stmt_sql in &stmts {
            last_result = Some(self.execute_single(stmt_sql).await?);
        }

        last_result.ok_or_else(|| DbError::InvalidOperation("Empty SQL statement".into()))
    }

    /// Apply and durably seal the complete startup catalog as one immutable batch.
    /// Existing sealed catalogs accept exact replay/no-op definitions only.
    ///
    /// # Errors
    /// Returns an error for a partial or divergent bootstrap, unsafe catalog mutation, lost leader
    /// authority, or manifest sealing failure. Local creates are rolled back on every error.
    #[cfg(feature = "cluster")]
    pub async fn execute_cluster_bootstrap_batch(
        &self,
        sql: &[String],
    ) -> Result<Vec<ExecuteResult>, DbError> {
        self.ensure_catalog_cleanup_unfenced("cluster catalog bootstrap")?;
        self.connector_registry.freeze();
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::Shutdown);
        }
        if DbState::load(&self.state) != DbState::Created {
            return Err(DbError::InvalidOperation(
                "cluster catalog bootstrap is only valid before pipeline startup".into(),
            ));
        }

        let mut parsed = Vec::new();
        for batch_entry in sql {
            for stmt_sql in sql_utils::split_statements(batch_entry) {
                let mut statements = parse_streaming_sql(stmt_sql)?;
                if statements.len() != 1 {
                    return Err(DbError::InvalidOperation(
                        "cluster bootstrap entries must contain exactly one SQL statement".into(),
                    ));
                }
                let statement = statements.pop().ok_or_else(|| {
                    DbError::InvalidOperation(
                        "cluster bootstrap entries must contain exactly one SQL statement".into(),
                    )
                })?;
                let (name, kind, _) = validate_cluster_catalog_create(stmt_sql, &statement)?;
                parsed.push((stmt_sql.to_owned(), statement, name, kind));
            }
        }
        {
            let mut names = std::collections::HashSet::with_capacity(parsed.len());
            for (_, _, name, _) in &parsed {
                if !names.insert(name.as_str()) {
                    return Err(DbError::InvalidOperation(format!(
                        "cluster bootstrap defines '{name}' more than once"
                    )));
                }
            }
        }

        let _topology_ddl = self.topology_ddl_lock.write().await;
        self.ensure_catalog_cleanup_unfenced("cluster catalog bootstrap")?;
        self.ensure_coordinated_recovery_mutation_unfenced("cluster catalog bootstrap")?;
        if DbState::load(&self.state) != DbState::Created {
            return Err(DbError::InvalidOperation(
                "cluster catalog bootstrap is only valid before pipeline startup".into(),
            ));
        }

        if let Some(manifest) = self.restore_catalog_from_manifest().await? {
            let requested = parsed
                .iter()
                .map(|(ddl, _, canonical_name, kind)| {
                    laminar_core::cluster::control::CatalogManifestEntry {
                        canonical_name: canonical_name.clone(),
                        kind: *kind,
                        ddl: ddl.clone(),
                    }
                })
                .collect::<Vec<_>>();
            if requested.as_slice() != manifest.entries.as_slice() {
                return Err(DbError::Pipeline(format!(
                    "configured cluster catalog must exactly match the complete ordered sealed inventory (configured entries: {}, sealed entries: {})",
                    requested.len(),
                    manifest.entries.len()
                )));
            }
            let mut results = Vec::with_capacity(parsed.len());
            for (stmt_sql, statement, name, _) in &parsed {
                let result = self
                    .exact_bootstrap_noop(stmt_sql, statement)?
                    .ok_or_else(|| {
                        DbError::Pipeline(format!(
                            "sealed cluster catalog rejects startup addition '{name}'"
                        ))
                    })?;
                results.push(result);
            }
            return Ok(results);
        }

        if !self.catalog_manifest_inventory()?.is_empty() {
            return Err(DbError::Pipeline(
                "cannot seal a new cluster catalog over uncommitted local topology".into(),
            ));
        }
        let store = self.catalog_manifest_store.lock().clone().ok_or_else(|| {
            DbError::Pipeline("cluster catalog manifest store is not configured".into())
        })?;
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Pipeline(
                "[LDB-6043] cluster catalog bootstrap requires a cluster controller".into(),
            )
        })?;
        let leader_proof = controller
            .capture_catalog_bootstrap_proof()
            .ok_or_else(|| {
                DbError::Pipeline(
                    "[LDB-6043] cluster catalog bootstrap requires the active durable leader lease"
                        .into(),
                )
            })?;
        self.validate_catalog_seal_authority(Some(&leader_proof))?;

        let mut bootstrap_guard = CatalogBootstrapGuard {
            db: self,
            created: Vec::with_capacity(parsed.len()),
            sealed: false,
        };
        let mut results = Vec::with_capacity(parsed.len());
        for (stmt_sql, statement, name, kind) in &parsed {
            let result = CATALOG_BOOTSTRAP
                .scope((), self.execute_parsed_single(stmt_sql, statement))
                .await?;
            let ExecuteResult::Ddl(info) = &result else {
                return Err(DbError::Pipeline(format!(
                    "cluster catalog create '{name}' returned a non-DDL result"
                )));
            };
            if !info.applied || info.object_name != *name {
                return Err(DbError::Pipeline(format!(
                    "cluster catalog create '{name}' did not apply exactly once"
                )));
            }
            bootstrap_guard.record(name.clone(), *kind);
            results.push(result);
        }

        let manifest = laminar_core::cluster::control::CatalogManifest::new(
            self.catalog_manifest_inventory()?,
        )
        .map_err(|error| {
            DbError::Pipeline(format!("invalid cluster catalog inventory: {error}"))
        })?;

        #[cfg(test)]
        let catalog_seal_gate = { self.catalog_seal_gate.lock().clone() };
        #[cfg(test)]
        if let Some((entered, release)) = catalog_seal_gate {
            entered.notify_one();
            release.notified().await;
        }
        self.validate_catalog_seal_authority(Some(&leader_proof))?;
        store
            .seal(&manifest, &leader_proof)
            .await
            .map_err(|error| DbError::Pipeline(format!("catalog manifest seal failed: {error}")))?;
        bootstrap_guard.sealed();
        Ok(results)
    }

    /// Apply one startup catalog definition and seal it as the complete inventory.
    /// Prefer [`Self::execute_cluster_bootstrap_batch`] for server configuration.
    ///
    /// # Errors
    /// Returns an error when the definition is invalid or the catalog batch cannot be sealed.
    #[cfg(feature = "cluster")]
    pub async fn execute_cluster_bootstrap(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let entries = vec![sql.to_owned()];
        self.execute_cluster_bootstrap_batch(&entries)
            .await?
            .into_iter()
            .last()
            .ok_or_else(|| DbError::InvalidOperation("Empty SQL statement".into()))
    }

    async fn execute_single(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let statements = parse_streaming_sql(sql)?;
        if statements.is_empty() {
            return Err(DbError::InvalidOperation("Empty SQL statement".into()));
        }
        let statement = &statements[0];
        if mutates_database(statement) {
            self.ensure_catalog_cleanup_unfenced("database mutation")?;
        }
        if is_topology_ddl(statement) {
            let _topology_ddl = self.topology_ddl_lock.write().await;
            self.ensure_catalog_cleanup_unfenced("database mutation")?;
            #[cfg(feature = "cluster")]
            self.ensure_coordinated_recovery_mutation_unfenced("database mutation")?;
            self.execute_parsed_single(sql, statement).await
        } else if reads_catalog(statement) {
            let _topology_read = self.topology_ddl_lock.read().await;
            if mutates_database(statement) {
                self.ensure_catalog_cleanup_unfenced("database mutation")?;
                #[cfg(feature = "cluster")]
                self.ensure_coordinated_recovery_mutation_unfenced("database mutation")?;
            }
            self.execute_parsed_single(sql, statement).await
        } else if mutates_database(statement) {
            let _topology_read = self.topology_ddl_lock.read().await;
            self.ensure_catalog_cleanup_unfenced("database mutation")?;
            #[cfg(feature = "cluster")]
            self.ensure_coordinated_recovery_mutation_unfenced("database mutation")?;
            self.execute_parsed_single(sql, statement).await
        } else {
            self.execute_parsed_single(sql, statement).await
        }
    }

    #[cfg(feature = "cluster")]
    async fn execute_single_already_gated(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let statements = parse_streaming_sql(sql)?;
        if statements.len() != 1 || !is_topology_ddl(&statements[0]) {
            return Err(DbError::InvalidOperation(
                "catalog manifest entries must contain exactly one topology DDL statement".into(),
            ));
        }
        self.execute_parsed_single(sql, &statements[0]).await
    }

    async fn execute_parsed_single(
        &self,
        sql: &str,
        statement: &StreamingStatement,
    ) -> Result<ExecuteResult, DbError> {
        if self.is_cluster_runtime()
            && matches!(
                statement,
                StreamingStatement::CreateStream {
                    retention_bytes: Some(_),
                    ..
                }
            )
        {
            return Err(DbError::Unsupported(
                "CREATE STREAM RETAIN HISTORY is not supported in cluster runtime until replay is globally ordered and checkpoint-aligned"
                    .into(),
            ));
        }

        #[cfg(feature = "cluster")]
        if is_topology_ddl(statement) && !catalog_manifest_replay_active() {
            let store_configured = self.catalog_manifest_store.lock().is_some();
            let cluster_runtime = self.is_cluster_runtime();
            if store_configured || cluster_runtime {
                validate_cluster_catalog_create(sql, statement)?;
                if !store_configured {
                    return Err(DbError::Pipeline(
                        "cluster topology DDL requires a catalog manifest store".into(),
                    ));
                }
                if !catalog_bootstrap_active() {
                    return Err(DbError::Pipeline(
                        "[LDB-6043] configured cluster topology can change only through startup bootstrap/replay until a replicated topology-version barrier is implemented"
                            .into(),
                    ));
                }
            }
        }

        let result = match statement {
            StreamingStatement::CreateSource(create) => {
                let result = self.handle_create_source(create).await?;
                if let ExecuteResult::Ddl(ref info) = result {
                    if info.applied {
                        self.connector_manager
                            .lock()
                            .store_ddl(&info.object_name, sql);
                    }
                }
                Ok(result)
            }
            StreamingStatement::CreateSink(create) => {
                let result = self.handle_create_sink(create)?;
                if let ExecuteResult::Ddl(ref info) = result {
                    if info.applied {
                        self.connector_manager
                            .lock()
                            .store_ddl(&info.object_name, sql);
                    }
                }
                Ok(result)
            }
            StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                if_not_exists,
                query_sql,
                retention_bytes,
                ..
            } => {
                let result = self
                    .handle_create_stream(
                        sql,
                        name,
                        query,
                        emit_clause.as_ref(),
                        *if_not_exists,
                        query_sql,
                        *retention_bytes,
                    )
                    .await?;
                if let ExecuteResult::Ddl(ref info) = result {
                    if info.applied {
                        self.connector_manager
                            .lock()
                            .store_ddl(&info.object_name, sql);
                    }
                }
                Ok(result)
            }
            StreamingStatement::CreateContinuousQuery { .. } => Err(DbError::InvalidOperation(
                "CREATE CONTINUOUS QUERY has no typed catalog/drop lifecycle; use CREATE STREAM"
                    .into(),
            )),
            StreamingStatement::DropLookupTable { name, if_exists } => {
                self.handle_drop_lookup_table(name, *if_exists)
            }
            StreamingStatement::CreateLookupTable(create) => {
                if create.or_replace {
                    return Err(DbError::InvalidOperation(
                        "CREATE OR REPLACE LOOKUP TABLE is not atomic; use DROP LOOKUP TABLE followed by CREATE LOOKUP TABLE"
                            .into(),
                    ));
                }
                let name = canonical_object_name(&create.name)?;
                let Some(reservation) = self.reserve_catalog_name(
                    &name,
                    CatalogObjectKind::LookupTable,
                    create.if_not_exists,
                )?
                else {
                    return Ok(ExecuteResult::Ddl(DdlInfo {
                        statement_type: "CREATE LOOKUP TABLE".into(),
                        object_name: name,
                        applied: false,
                    }));
                };
                let result = self.handle_query(sql).await?;
                if let ExecuteResult::Ddl(ref info) = result {
                    if info.applied {
                        self.connector_manager
                            .lock()
                            .store_ddl(&info.object_name, sql);
                    }
                }
                reservation.commit();
                Ok(result)
            }
            StreamingStatement::Standard(stmt) => {
                if let sqlparser::ast::Statement::CreateTable(ct) = stmt.as_ref() {
                    let result = self.handle_create_table(ct)?;
                    if let ExecuteResult::Ddl(ref info) = result {
                        if info.applied {
                            self.connector_manager
                                .lock()
                                .store_ddl(&info.object_name, sql);
                        }
                    }
                    Ok(result)
                } else if let sqlparser::ast::Statement::Drop {
                    object_type: sqlparser::ast::ObjectType::Table,
                    names,
                    if_exists,
                    cascade,
                    ..
                } = stmt.as_ref()
                {
                    self.handle_drop_table(names, *if_exists, *cascade)
                } else if let sqlparser::ast::Statement::Set(set_stmt) = stmt.as_ref() {
                    self.handle_set(set_stmt)
                } else if matches!(stmt.as_ref(), sqlparser::ast::Statement::AlterTable { .. }) {
                    Err(DbError::InvalidOperation(
                        "ALTER TABLE is disabled until catalog/provider changes are transactional"
                            .into(),
                    ))
                } else if matches!(stmt.as_ref(), sqlparser::ast::Statement::Query(_)) {
                    self.handle_query(sql).await
                } else {
                    Err(DbError::InvalidOperation(format!(
                        "unsupported standard SQL statement; catalog mutation is not typed or transactional: {stmt}"
                    )))
                }
            }
            StreamingStatement::InsertInto {
                table_name,
                columns,
                values,
            } => self.handle_insert_into(table_name, columns, values),
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
            } => self.handle_drop_stream(name, *if_exists, *cascade).await,
            StreamingStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            } => {
                self.handle_drop_materialized_view(name, *if_exists, *cascade)
                    .await
            }
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
                        self.build_show_create_source(&canonical_object_name(name)?)?
                    }
                    ShowCommand::CreateSink { name } => {
                        self.build_show_create_sink(&canonical_object_name(name)?)?
                    }
                };
                Ok(ExecuteResult::Metadata(batch))
            }
            StreamingStatement::Checkpoint => {
                let result = self.checkpoint().await?;
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "CHECKPOINT".to_string(),
                    object_name: format!("checkpoint_{}", result.checkpoint_id),
                    applied: true,
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
                    if info.applied {
                        self.connector_manager
                            .lock()
                            .store_ddl(&info.object_name, sql);
                    }
                }
                Ok(result)
            }
            StreamingStatement::AlterSource { .. } => Err(DbError::InvalidOperation(
                "ALTER SOURCE is disabled until catalog/provider changes are transactional".into(),
            )),
            StreamingStatement::Subscribe(_) => Err(DbError::InvalidOperation(
                "SUBSCRIBE requires the pgwire endpoint, not HTTP /api/v1/sql".into(),
            )),
            StreamingStatement::DeclareCursorForSubscribe { .. } => Err(DbError::InvalidOperation(
                "DECLARE CURSOR FOR SUBSCRIBE requires the pgwire endpoint, not HTTP /api/v1/sql"
                    .into(),
            )),
        };

        result
    }

    fn handle_insert_into(
        &self,
        table_name: &sqlparser::ast::ObjectName,
        columns: &[sqlparser::ast::Ident],
        values: &[Vec<sqlparser::ast::Expr>],
    ) -> Result<ExecuteResult, DbError> {
        let name = canonical_object_name(table_name)?;
        if !columns.is_empty() {
            return Err(DbError::InvalidOperation(
                "INSERT column lists are unsupported until projection and default-value semantics are implemented"
                    .into(),
            ));
        }
        if values.is_empty() {
            return Err(DbError::InvalidOperation(
                "INSERT requires at least one VALUES row".into(),
            ));
        }

        if let Some(entry) = self.catalog.get_source(&name) {
            let batch = sql_utils::sql_values_to_record_batch(&entry.schema, values)?;
            entry
                .push_and_buffer(batch)
                .map_err(|e| DbError::InsertError(format!("Failed to push to source: {e}")))?;
            return Ok(ExecuteResult::RowsAffected(values.len() as u64));
        }

        // Single lock scope avoids TOCTOU between has_table/schema/upsert.
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

        Err(DbError::InvalidOperation(format!(
            "INSERT target '{name}' is not a typed mutable source or table"
        )))
    }

    #[allow(clippy::unused_self)] // will use self when implemented
    fn handle_restore_checkpoint(&self, _checkpoint_id: u64) -> Result<ExecuteResult, DbError> {
        Err(DbError::Unsupported(
            "RESTORE FROM CHECKPOINT is not yet implemented — \
             requires pipeline stop, state reload from manifest, \
             source offset seek, and pipeline restart"
                .to_string(),
        ))
    }

    /// Return a session property value.
    #[must_use]
    pub fn get_session_property(&self, key: &str) -> Option<String> {
        self.session_properties
            .lock()
            .get(&key.to_lowercase())
            .cloned()
    }

    /// Return all session properties.
    #[must_use]
    pub fn session_properties(&self) -> HashMap<String, String> {
        self.session_properties.lock().clone()
    }

    /// Set a session property (keys are lowercased).
    pub fn set_session_property(&self, key: &str, value: &str) {
        self.session_properties
            .lock()
            .insert(key.to_lowercase(), value.to_string());
    }

    /// Subscribe to a named stream or materialized view.
    ///
    /// # Errors
    ///
    /// Returns `DbError::StreamNotFound` if the object or its output schema is unresolved.
    pub async fn subscribe<T: crate::handle::FromBatch>(
        &self,
        name: &str,
    ) -> Result<crate::handle::TypedSubscription<T>, DbError> {
        let portal = self
            .open_subscription(name, None, crate::subscription::SubscribeStart::Tail)
            .await?;
        Ok(crate::handle::TypedSubscription::new(portal))
    }

    fn ensure_subscription_runtime_supported(&self) -> Result<(), DbError> {
        if self.is_cluster_runtime() {
            return Err(DbError::Unsupported(
                "SUBSCRIBE is not supported in cluster runtime until delivery is sequenced and checkpoint-aligned"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Schema a `SUBSCRIBE` against `name` would emit.
    /// A stream is visible here only after its physical output schema has been
    /// resolved. Bare sources and unresolved streams are not subscribable.
    #[must_use]
    pub fn lookup_subscription_schema(&self, name: &str) -> Option<arrow_schema::SchemaRef> {
        if let Some(mv) = self.mv_registry.lock().get(name).cloned() {
            return Some(mv.schema);
        }
        if let Some(schema) = self.stream_schemas.read().get(name).cloned() {
            return Some(schema);
        }
        None
    }

    /// Open a SUBSCRIBE portal against a named MV or resolved stream. A bare
    /// SOURCE is not subscribable (surfaced as `StreamNotFound`).
    ///
    /// # Errors
    /// `Unsupported` in cluster runtime; `StreamNotFound` for unknown `name`;
    /// `Pipeline` for subscriber-cap
    /// or filter-compile failures; `InvalidOperation` when `AsOfEpoch(n)`
    /// is not committed or is no longer retained.
    pub async fn open_subscription(
        &self,
        name: &str,
        filter_sql: Option<&str>,
        start: crate::subscription::SubscribeStart,
    ) -> Result<crate::subscription::SubscriptionPortal, DbError> {
        self.ensure_subscription_runtime_supported()?;

        // Serialize schema resolution, filter compilation, and cursor attachment
        // with topology DDL. Otherwise DROP/recreate can leave a portal attached
        // to an orphaned log with the previous object's schema.
        let _topology = self.topology_ddl_lock.read().await;

        // SUBSCRIBE to an incremental MV delivers consolidated snapshots (plain rows), not the
        // raw `__weight` changelog.

        let schema = self
            .lookup_subscription_schema(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_string()))?;

        let filter = match filter_sql {
            None => None,
            Some(sql) => Some(crate::filter_compile::compile(&self.ctx, sql, &schema).await?),
        };

        let reader =
            self.subscription_registry
                .subscribe(name, start)
                .map_err(|error| match error {
                    crate::subscription::SubscriptionOpenError::ReplayPruned {
                        earliest_retained,
                    } => {
                        let requested = match start {
                            crate::subscription::SubscribeStart::AsOfEpoch(n) => n,
                            crate::subscription::SubscribeStart::Tail => 0,
                        };
                        DbError::SubscriptionReplayPruned {
                            name: name.to_string(),
                            requested,
                            earliest_retained,
                        }
                    }
                    crate::subscription::SubscriptionOpenError::EpochNotCommitted {
                        requested,
                        latest_committed,
                    } => DbError::SubscriptionEpochNotCommitted {
                        name: name.to_string(),
                        requested,
                        latest_committed,
                    },
                    crate::subscription::SubscriptionOpenError::Capacity { attached, limit } => {
                        DbError::Pipeline(format!(
                            "subscriber cap reached for '{name}' ({attached}/{limit})"
                        ))
                    }
                })?;

        Ok(match filter {
            Some(phys) => crate::subscription::SubscriptionPortal::open_with_filter(
                name, schema, reader, phys,
            ),
            None => crate::subscription::SubscriptionPortal::open(name, schema, reader),
        })
    }

    fn handle_explain(&self, statement: &StreamingStatement) -> Result<ExecuteResult, DbError> {
        let mut planner = self.planner.lock();
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

    async fn handle_explain_analyze(
        &self,
        statement: &StreamingStatement,
        original_sql: &str,
    ) -> Result<ExecuteResult, DbError> {
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

        let upper = original_sql.to_uppercase();
        let inner_start = upper.find("ANALYZE").map_or(0, |pos| pos + "ANALYZE".len());
        let inner_sql = original_sql[inner_start..].trim();

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

    #[allow(clippy::too_many_lines)]
    pub(crate) async fn handle_query(&self, sql: &str) -> Result<ExecuteResult, DbError> {
        let plan = {
            let statements = parse_streaming_sql(sql)?;
            if statements.is_empty() {
                return Err(DbError::InvalidOperation("Empty SQL statement".into()));
            }
            match &statements[0] {
                StreamingStatement::CreateContinuousQuery { .. } => {
                    return Err(DbError::InvalidOperation(
                        "CREATE CONTINUOUS QUERY has no typed catalog/drop lifecycle; use CREATE STREAM"
                            .into(),
                    ));
                }
                StreamingStatement::CreateLookupTable(statement) => {
                    self.ensure_offline_topology_ddl_allowed("CREATE LOOKUP TABLE")?;
                    let name = canonical_object_name(&statement.name)?;
                    if self.catalog_namespace.lock().get(&name)
                        != Some(&CatalogObjectKind::LookupTable)
                    {
                        return Err(DbError::InvalidOperation(
                            "CREATE LOOKUP TABLE must pass typed namespace admission".into(),
                        ));
                    }
                }
                StreamingStatement::DropLookupTable { name, if_exists } => {
                    return self.handle_drop_lookup_table(name, *if_exists);
                }
                _ => {}
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
                    applied: true,
                }))
            }
            laminar_sql::planner::StreamingPlan::RegisterSink(info) => {
                Ok(ExecuteResult::Ddl(DdlInfo {
                    statement_type: "DDL".to_string(),
                    object_name: info.name,
                    applied: true,
                }))
            }
            laminar_sql::planner::StreamingPlan::Query(query_plan) => {
                if let Some(asof_config) = Self::extract_asof_config(&query_plan) {
                    return self.execute_asof_query(&asof_config, sql).await;
                }

                let plan_sql = query_plan.statement.to_string();
                let logical_plan = self.ctx.state().create_logical_plan(&plan_sql).await?;
                let df = self.ctx.execute_logical_plan(logical_plan).await?;
                let stream = df.execute_stream().await?;

                Ok(self.bridge_query_stream(sql, stream))
            }
            laminar_sql::planner::StreamingPlan::Standard(stmt) => {
                let sql_str = stmt.to_string();
                let df = self.ctx.sql(&sql_str).await?;
                let stream = df.execute_stream().await?;

                Ok(self.bridge_query_stream(sql, stream))
            }
            laminar_sql::planner::StreamingPlan::RegisterLookupTable(info) => {
                self.handle_register_lookup_table(info)
            }
            laminar_sql::planner::StreamingPlan::DropLookupTable { .. } => Err(
                DbError::InvalidOperation("lookup drop bypassed typed catalog admission".into()),
            ),
        }
    }

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

        let _ = self
            .ctx
            .deregister_table(exact_table_reference("__asof_result"));
        self.ctx
            .register_table(exact_table_reference("__asof_result"), Arc::new(mem_table))
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

        let _ = self
            .ctx
            .deregister_table(exact_table_reference("__asof_result"));

        Ok(self.bridge_query_stream(original_sql, stream))
    }

    /// Get a typed handle for pushing data to a registered source.
    ///
    /// # Errors
    ///
    /// Returns `DbError::SourceNotFound` if the source is not registered.
    /// Returns `DbError::SchemaMismatch` if the Rust type's schema doesn't match.
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

    /// List registered sources.
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

    /// List registered sinks.
    pub fn sinks(&self) -> Vec<SinkInfo> {
        self.catalog
            .list_sinks()
            .into_iter()
            .map(|name| SinkInfo { name })
            .collect()
    }

    /// List registered materialized views.
    pub fn materialized_views(&self) -> Vec<crate::handle::MaterializedViewInfo> {
        let registry = self.mv_registry.lock();
        registry
            .views()
            .map(crate::handle::MaterializedViewInfo::from)
            .collect()
    }

    /// List registered streams.
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

    /// Build the pipeline topology graph (nodes + edges) from registered sources, streams, and sinks.
    pub fn pipeline_topology(&self) -> crate::handle::PipelineTopology {
        use crate::handle::{PipelineEdge, PipelineNode, PipelineNodeType};

        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        let source_names = self.catalog.list_sources();

        for name in &source_names {
            let schema = self.catalog.get_source(name).map(|e| e.schema.clone());
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Source,
                schema,
                sql: None,
            });
        }

        let mgr = self.connector_manager.lock();
        let stream_names: Vec<String> = mgr.streams().keys().cloned().collect();
        for (name, reg) in mgr.streams() {
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Stream,
                schema: None,
                sql: Some(reg.query_sql.clone()),
            });

            // Lightweight heuristic: substring match instead of a full parse.
            let sql_upper = reg.query_sql.to_uppercase();
            for src in &source_names {
                if sql_upper.contains(&src.to_uppercase()) {
                    edges.push(PipelineEdge {
                        from: src.clone(),
                        to: name.clone(),
                    });
                }
            }
            for other in &stream_names {
                if other != name && sql_upper.contains(&other.to_uppercase()) {
                    edges.push(PipelineEdge {
                        from: other.clone(),
                        to: name.clone(),
                    });
                }
            }
        }

        for (name, reg) in mgr.sinks() {
            nodes.push(PipelineNode {
                name: name.clone(),
                node_type: PipelineNodeType::Sink,
                schema: None,
                sql: None,
            });

            if !reg.input.is_empty() {
                edges.push(PipelineEdge {
                    from: reg.input.clone(),
                    to: name.clone(),
                });
            }
        }

        let cm_sink_names: std::collections::HashSet<&String> = mgr.sinks().keys().collect();
        for name in self.catalog.list_sinks() {
            if !cm_sink_names.contains(&name) {
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

    /// List active queries.
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

    /// Stable participant identity used to namespace checkpoint manifests.
    ///
    /// Local runtimes return `None` and keep the historical unprefixed layout. Cluster runtimes
    /// use the controller's numeric instance id; decision markers intentionally do not use this
    /// namespace because they are cluster-wide.
    pub(crate) fn checkpoint_participant(&self) -> Option<u64> {
        checkpoint_participant_for_runtime(self)
    }

    /// Stable logical partition count used by checkpoint and state identity.
    /// Local runtimes have one key group. Cluster runtimes use their exact registry or the
    /// fixed cluster default when no keyed state topology is installed.
    pub(crate) fn checkpoint_key_groups(&self) -> laminar_core::state::KeyGroupCount {
        let runtime_default = match self.runtime_mode() {
            RuntimeMode::Local => laminar_core::state::LOCAL_KEY_GROUP_COUNT,
            RuntimeMode::Cluster => laminar_core::state::DEFAULT_CLUSTER_KEY_GROUP_COUNT,
        };
        self.vnode_registry
            .lock()
            .as_ref()
            .map_or(runtime_default, |registry| {
                laminar_core::state::KeyGroupCount::try_from(registry.vnode_count())
                    .expect("builder validated the vnode registry key-group count")
            })
    }

    /// Return a checkpoint store for the resolved runtime configuration, if any.
    pub(crate) fn checkpoint_store(
        &self,
    ) -> Result<Option<Box<dyn laminar_core::storage::CheckpointStore>>, DbError> {
        let Some(cp_config) = self.config.checkpoint.as_ref() else {
            return Ok(None);
        };
        let key_group_count = self.checkpoint_key_groups();
        let participant = self.checkpoint_participant();
        let participant_id = participant.unwrap_or(0);
        let max_state_data_bytes = cp_config.max_staged_bytes.ok_or_else(|| {
            DbError::Config("checkpoint.max_staged_bytes was not resolved at construction".into())
        })?;

        #[cfg(feature = "cluster")]
        if let Some(object_store) = self.cluster_checkpoint_object_store() {
            return Ok(Some(Box::new(
                laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    object_store,
                    participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
                )
                .with_max_state_data_bytes(max_state_data_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id),
            )));
        }

        if let Some(url) = self
            .config
            .object_store_url
            .as_deref()
            .filter(|url| url.starts_with("file://"))
        {
            let root = laminar_core::storage::object_store_builder::file_url_path(url)
                .map_err(|error| DbError::Checkpoint(format!("checkpoint storage URL: {error}")))?;
            let checkpoint_dir =
                participant.map_or(root.clone(), |id| root.join("nodes").join(id.to_string()));
            Ok(Some(Box::new(
                laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    checkpoint_dir,
                )
                .with_max_state_data_bytes(max_state_data_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id),
            )))
        } else if let Some(ref url) = self.config.object_store_url {
            let obj_store = laminar_core::storage::object_store_builder::build_object_store(
                url,
                &self.config.object_store_options,
            )
            .map_err(|error| DbError::Checkpoint(format!("checkpoint object store: {error}")))?;
            Ok(Some(Box::new(
                laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    obj_store,
                    participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
                )
                .with_max_state_data_bytes(max_state_data_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id),
            )))
        } else {
            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));
            let checkpoint_dir = participant.map_or(data_dir.clone(), |id| {
                data_dir.join("nodes").join(id.to_string())
            });
            Ok(Some(Box::new(
                laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    checkpoint_dir,
                )
                .with_max_state_data_bytes(max_state_data_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id),
            )))
        }
    }

    /// Trigger a checkpoint that persists source offsets, sink positions, and operator state.
    ///
    /// # Errors
    ///
    /// Returns `DbError::Checkpoint` if checkpointing is disabled, no live
    /// manual-checkpoint route exists, cluster leadership cannot be resolved,
    /// or the checkpoint fails.
    pub async fn checkpoint(
        &self,
    ) -> Result<crate::checkpoint_coordinator::CheckpointResult, DbError> {
        self.ensure_catalog_cleanup_unfenced("manual checkpoint")?;
        #[cfg(feature = "cluster")]
        self.ensure_coordinated_recovery_mutation_unfenced("manual checkpoint")?;
        if self.config.checkpoint.is_none() {
            return Err(DbError::Checkpoint(
                "checkpointing is not enabled".to_string(),
            ));
        }
        if DbState::load(&self.state) != DbState::Running {
            return Err(DbError::Checkpoint(
                "manual checkpoint coordinator is not running — a fully running pipeline is required"
                    .into(),
            ));
        }

        #[cfg(feature = "cluster")]
        if self.is_cluster_runtime() {
            let leader_rpc: Result<Option<String>, DbError> = {
                let cc_guard = self.cluster_controller.lock();
                let cc = cc_guard.as_ref().ok_or_else(|| {
                    DbError::Checkpoint("cluster runtime has no cluster controller".into())
                })?;
                match cc {
                    cc if cc.is_leader() => Ok(None),
                    cc => {
                        let leader_id = cc.current_leader().ok_or_else(|| {
                            DbError::Checkpoint(
                                "cannot route checkpoint: cluster leader is unresolved".into(),
                            )
                        })?;
                        let watch = cc.members_watch();
                        let members = watch.borrow();
                        let address = members
                            .iter()
                            .find(|member| member.id == leader_id)
                            .map(|member| member.rpc_address.trim())
                            .filter(|address| !address.is_empty())
                            .ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "cannot route checkpoint: RPC address for cluster leader \
                                     {leader_id} is unresolved"
                                ))
                            })?;
                        Ok(Some(address.to_owned()))
                    }
                }
            };
            if let Some(leader_rpc) = leader_rpc? {
                tracing::info!(
                    "Forwarding checkpoint request to leader node at HTTP address {}",
                    leader_rpc
                );
                return self.forward_checkpoint_to_leader(&leader_rpc).await;
            }
        }

        // Route through the live streaming coordinator so the manual checkpoint observes the
        // same source, operator, and sink cut as a periodic barrier checkpoint.
        let tx = self.force_ckpt_tx.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(
                "manual checkpoint coordinator is not running — call start() first".into(),
            )
        })?;
        let (reply_tx, reply_rx) = crossfire::oneshot::oneshot();
        tx.send(reply_tx).await.map_err(|_| {
            DbError::Checkpoint(
                "manual checkpoint receiver closed — engine may be shutting down".into(),
            )
        })?;
        let result = reply_rx.await.map_err(|_| {
            DbError::Checkpoint("manual checkpoint ended without a terminal reply".into())
        })?;

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
            failure_disposition:
                Option<crate::checkpoint_coordinator::CheckpointFailureDisposition>,
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

        // The leader returns a `CheckpointResponse` body even on failure (HTTP 500 +
        // `success: false`), so parse it to relay structured failure. A non-`CheckpointResponse`
        // body (e.g. a 401 payload) is an auth/transport failure — surface status and body.
        match serde_json::from_str::<ForwardedCheckpointResponse>(&body) {
            Ok(response) => Ok(crate::checkpoint_coordinator::CheckpointResult {
                success: response.success,
                checkpoint_id: response.checkpoint_id,
                epoch: response.epoch,
                duration: std::time::Duration::from_millis(response.duration_ms),
                error: response.error,
                failure_disposition: response.failure_disposition,
            }),
            Err(_) => Err(DbError::Checkpoint(format!(
                "leader rejected checkpoint ({status}): {body}"
            ))),
        }
    }

    /// Returns checkpoint performance statistics, or `None` if the coordinator is not initialized.
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
            .field("runtime_mode", &self.runtime_mode)
            .field("sources", &self.catalog.list_sources().len())
            .field("sinks", &self.catalog.list_sinks().len())
            .field("materialized_views", &self.mv_registry.lock().len())
            .field("checkpoint_enabled", &self.is_checkpoint_enabled())
            .field("shutdown", &self.is_closed())
            .finish_non_exhaustive()
    }
}

/// `DefaultPhysicalPlanner` with lookup-join extension support.
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
