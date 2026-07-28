//! Pipeline lifecycle: start, close, shutdown.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::FutureExt;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, DeliveryGuarantee, SinkConnector, SinkConsistency, SinkContract,
    SinkTopology, SourceConsistency, SourceContract, SourceTopology,
};
use laminar_core::state::StateBackendDurability;
use rustc_hash::FxHashMap;

use crate::connector_task_fence::ConnectorTaskFenceRegistration;
#[cfg(feature = "cluster")]
use crate::db::ClusterStartupDisposition;
use crate::db::{exact_table_reference, DbState, LaminarDB, RuntimeMode, SourceWatermarkState};
use crate::error::DbError;
use crate::pipeline::streaming_coordinator::TrackedSourceRegistration;

/// Bound recovery amplification while keeping every delta parent strictly inside the retained
/// predecessor window. A single retained predecessor cannot support a delta chain, so it stays on
/// full per-vnode snapshots.
#[cfg(feature = "cluster")]
const fn cluster_delta_chain_bound(max_retained: usize) -> Option<u32> {
    match max_retained {
        0 | 1 => None,
        2 => Some(1),
        3 => Some(2),
        4 => Some(3),
        _ => Some(4),
    }
}

/// Maximum number of physical partial artifacts a writer-conformant vnode restore may fetch.
/// One FULL is always required; retention of two or more cuts permits one direct REFERENCE edge,
/// and the remaining edges are the independently bounded consecutive DELTAs.
#[cfg(feature = "cluster")]
pub(crate) const fn max_artifacts_per_cluster_vnode_chain(max_retained: usize) -> usize {
    let reference_edge = if max_retained >= 2 { 1 } else { 0 };
    let delta_edges = match cluster_delta_chain_bound(max_retained) {
        Some(bound) => bound as usize,
        None => 0,
    };
    1 + reference_edge + delta_edges
}

/// Absolute reader limit across every currently accepted retention setting. The local
/// `max_retained` value is not yet part of cluster identity, so using its narrower limit could
/// strand a valid checkpoint produced by a differently configured participant.
#[cfg(feature = "cluster")]
pub(crate) const MAX_ARTIFACTS_PER_CLUSTER_VNODE_CHAIN: usize =
    max_artifacts_per_cluster_vnode_chain(usize::MAX);

const fn required_recovery_scope(runtime: RuntimeMode) -> StateBackendDurability {
    match runtime {
        RuntimeMode::Local => StateBackendDurability::NodeDurable,
        RuntimeMode::Cluster => StateBackendDurability::ClusterShared,
    }
}

const EXACT_SINK_PROTOCOL: &str =
    "exactly-once external sinks require checkpoint-committable consistency, coordinated phase \
     1, participant-complete sealed markers, and a namespaced exact external cursor";
const CLUSTER_BEST_EFFORT: &str =
    "cluster mode requires at_least_once delivery; best_effort has no defined \
     rebalance/state-loss contract";
#[cfg(feature = "cluster")]
const CLUSTER_COMPUTE_THREAD_STACK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
enum PipelineLifecycleAuthority {
    Public,
    #[cfg(feature = "cluster")]
    CoordinatedRecovery,
}

#[derive(Clone, Copy)]
enum StartupFailureKind {
    Config,
    Checkpoint,
    Connector,
    InvalidOperation,
    Shutdown,
    Pipeline,
}

#[derive(Clone)]
struct StartupFailure {
    kind: StartupFailureKind,
    message: Arc<str>,
}

impl StartupFailure {
    fn capture(error: DbError) -> Self {
        let (kind, message) = match error {
            DbError::Config(message) => (StartupFailureKind::Config, message),
            DbError::Checkpoint(message) => (StartupFailureKind::Checkpoint, message),
            DbError::CheckpointStore(error) => (StartupFailureKind::Checkpoint, error.to_string()),
            DbError::Connector(message) => (StartupFailureKind::Connector, message),
            DbError::ConnectorOp(error) => (StartupFailureKind::Connector, error.to_string()),
            DbError::InvalidOperation(message) => (StartupFailureKind::InvalidOperation, message),
            DbError::Shutdown => (StartupFailureKind::Shutdown, String::new()),
            DbError::Pipeline(message) => (StartupFailureKind::Pipeline, message),
            error => (StartupFailureKind::Pipeline, error.to_string()),
        };
        Self {
            kind,
            message: Arc::from(message),
        }
    }

    fn to_error(&self) -> DbError {
        let message = self.message.to_string();
        match self.kind {
            StartupFailureKind::Config => DbError::Config(message),
            StartupFailureKind::Checkpoint => DbError::Checkpoint(message),
            StartupFailureKind::Connector => DbError::Connector(message),
            StartupFailureKind::InvalidOperation => DbError::InvalidOperation(message),
            StartupFailureKind::Shutdown => DbError::Shutdown,
            StartupFailureKind::Pipeline => DbError::Pipeline(message),
        }
    }
}

#[derive(Clone)]
enum StartupOutcome {
    Success,
    Failed(StartupFailure),
}

pub(crate) struct StartupAttempt {
    outcome: parking_lot::Mutex<Option<StartupOutcome>>,
    notify: tokio::sync::Notify,
}

impl StartupAttempt {
    fn new() -> Self {
        Self {
            outcome: parking_lot::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn complete(&self, result: Result<(), DbError>) {
        let outcome = match result {
            Ok(()) => StartupOutcome::Success,
            Err(error) => StartupOutcome::Failed(StartupFailure::capture(error)),
        };
        let mut stored = self.outcome.lock();
        if stored.is_some() {
            return;
        }
        *stored = Some(outcome);
        drop(stored);
        self.notify.notify_waiters();
    }

    fn is_complete(&self) -> bool {
        self.outcome.lock().is_some()
    }

    async fn wait(&self) -> Result<(), DbError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(outcome) = self.outcome.lock().clone() {
                return match outcome {
                    StartupOutcome::Success => Ok(()),
                    StartupOutcome::Failed(error) => Err(error.to_error()),
                };
            }
            notified.await;
        }
    }
}

/// Hand a compute fault to cluster recovery without retaining pipeline lifecycle ownership.
///
/// Recovery stops the pipeline by joining its watcher, so this path must never wait for an active
/// recovery announcement to clear. The request stays latched after publication until an authorized
/// committed Release consumes it; a failed round leaves it available for retry.
#[cfg(feature = "cluster")]
async fn report_cluster_compute_fault(
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    pending: Arc<std::sync::atomic::AtomicU64>,
) {
    let Some(controller) = controller else {
        tracing::error!("cluster compute fault has no recovery controller; intake remains fenced");
        return;
    };
    if let Err(error) =
        crate::coordinated_recovery::request_local_fault(&controller, &pending).await
    {
        tracing::warn!(%error, "cluster compute fault queued for monitor retry");
    }
}

/// Queue only a fault that won lifecycle ownership before this runtime generation was cancelled.
#[cfg(feature = "cluster")]
fn queue_owned_cluster_compute_fault(
    controller: &laminar_core::cluster::control::ClusterController,
    pending: &std::sync::atomic::AtomicU64,
    owns_fault_state: bool,
    runtime_shutdown: &tokio_util::sync::CancellationToken,
) -> Result<bool, String> {
    if !owns_fault_state || runtime_shutdown.is_cancelled() {
        return Ok(false);
    }
    crate::coordinated_recovery::queue_local_fault(controller, pending)?;
    Ok(true)
}

/// Validate source durability and placement before the connector performs I/O.
fn admit_source_contract(
    contract: SourceContract,
    delivery: DeliveryGuarantee,
    checkpointing_enabled: bool,
    runtime: RuntimeMode,
) -> Result<(), &'static str> {
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::BestEffort {
        return Err(CLUSTER_BEST_EFFORT);
    }
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::ExactlyOnce {
        return Err(
            "[LDB-0013] cluster exactly-once is not admitted until supported connectors have \
             certified term-fenced source handoff and external sink cursor commits",
        );
    }
    if delivery == DeliveryGuarantee::ExactlyOnce && !contract.is_exact_delivery_certified() {
        return Err(
            "[LDB-5037] exactly-once source delivery is not production-certified for this \
             connector contract",
        );
    }
    if contract.consistency == SourceConsistency::CommitCoupled {
        if delivery == DeliveryGuarantee::ExactlyOnce {
            return Err(
                "exactly-once commit-coupled sources require a certified in-flight \
                 transaction/barrier checkpoint cut, which is not implemented",
            );
        }
        if delivery != DeliveryGuarantee::AtLeastOnce {
            return Err("commit-coupled sources currently support only at-least-once delivery");
        }
        if !checkpointing_enabled {
            return Err(
                "commit-coupled sources require checkpointing so upstream retention can advance",
            );
        }
    }

    if delivery != DeliveryGuarantee::BestEffort
        && contract.consistency == SourceConsistency::Ephemeral
    {
        return Err("at-least-once and exactly-once delivery require replayable sources");
    }

    if runtime == RuntimeMode::Cluster {
        match contract.topology {
            SourceTopology::Splittable => {}
            SourceTopology::NodeLocalIngress => {
                return Err(
                    "cluster node-local ingress has no defined rebalance/state-loss contract",
                );
            }
            SourceTopology::Singleton => {
                return Err(
                    "cluster singleton sources require fenced singleton placement, which is not implemented",
                );
            }
        }
    }

    Ok(())
}

fn validate_source_recovery_assignment(
    source: &str,
    assignment_scoped: bool,
    checkpoint: Option<&laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint>,
    expected_assignment: Option<std::num::NonZeroU64>,
) -> Result<(), DbError> {
    let captured = checkpoint.and_then(|checkpoint| checkpoint.source_assignment_version);
    match (assignment_scoped, expected_assignment, captured) {
        (true, None, _) => Err(DbError::Checkpoint(format!(
            "[LDB-6055] cluster-assigned source '{source}' recovery has no authoritative assignment fence"
        ))),
        (true, Some(_), None) => Err(DbError::Checkpoint(format!(
            "[LDB-6055] cluster-assigned source '{source}' recovery checkpoint is missing its assignment version"
        ))),
        (true, Some(expected), Some(captured)) if captured != expected => {
            Err(DbError::Checkpoint(format!(
                "[LDB-6055] source '{source}' recovery checkpoint captured assignment version {captured}, committed fence is {expected}"
            )))
        }
        (false, _, Some(captured)) => Err(DbError::Checkpoint(format!(
            "[LDB-6055] non-assigned source '{source}' recovery checkpoint unexpectedly carries assignment version {captured}"
        ))),
        _ => Ok(()),
    }
}

/// Validate sink durability, placement, and changelog semantics before I/O.
fn admit_sink_contract(
    contract: SinkContract,
    delivery: DeliveryGuarantee,
    runtime: RuntimeMode,
    carries_changelog: bool,
) -> Result<(), &'static str> {
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::BestEffort {
        return Err(CLUSTER_BEST_EFFORT);
    }
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::ExactlyOnce {
        return Err(
            "[LDB-0013] cluster exactly-once is not admitted until supported connectors have \
             certified term-fenced source handoff and external sink cursor commits",
        );
    }
    match (delivery, contract.consistency) {
        (DeliveryGuarantee::ExactlyOnce, SinkConsistency::CheckpointCommittable) => {}
        (DeliveryGuarantee::ExactlyOnce, _) => return Err(EXACT_SINK_PROTOCOL),
        (_, SinkConsistency::CheckpointCommittable) => {
            return Err(
                "checkpoint-committable sinks require global exactly-once delivery; running the \
                 coordinated protocol under a weaker label is not supported",
            );
        }
        _ => {}
    }

    if delivery == DeliveryGuarantee::AtLeastOnce
        && contract.consistency == SinkConsistency::Ephemeral
    {
        return Err("at-least-once delivery requires a durably acknowledged sink");
    }

    if runtime == RuntimeMode::Cluster {
        match contract.topology {
            SinkTopology::MultiWriter => {}
            SinkTopology::NodeLocalEgress => {
                return Err(
                    "cluster node-local egress has no defined rebalance/state-loss contract",
                );
            }
            SinkTopology::Singleton => {
                return Err(
                    "cluster singleton sinks require fenced singleton placement, which is not implemented",
                );
            }
        }
    }

    if carries_changelog && !contract.accepts_full_changelog() {
        return Err(
            "the input carries deletes/retractions and requires FullChangelog sink semantics; \
             append-only or keyed-upsert alone is insufficient",
        );
    }

    Ok(())
}

/// Immutable facts required to admit one configured sink before external I/O.
#[derive(Clone, Copy)]
struct SinkAdmissionContext<'a> {
    config: &'a ConnectorConfig,
    name: &'a str,
    input: &'a str,
    delivery: DeliveryGuarantee,
    runtime: RuntimeMode,
    carries_changelog: bool,
    checkpointing_enabled: bool,
    state_backend_scope: StateBackendDurability,
}

struct PreparedSink {
    name: String,
    connector: Box<dyn SinkConnector>,
    config: ConnectorConfig,
    filter_expr: Option<String>,
    input: String,
    contract: SinkContract,
    write_timeout: std::time::Duration,
    flush_interval: std::time::Duration,
    requires_recovery_on_error: bool,
    task_fence: ConnectorTaskFenceRegistration,
}

type PipelineSink = (
    String,
    crate::sink_task::SinkTaskHandle,
    Option<String>,
    String,
    SinkContract,
);

struct PipelineSinkSetup {
    sinks: Vec<PipelineSink>,
    sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    coordinated_committer: Option<crate::coordinated_committer::CoordinatedCommitter>,
    committer_poll: std::time::Duration,
    committer_notify: Arc<tokio::sync::Notify>,
    #[cfg(feature = "cluster")]
    callback_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
}

struct PipelineRecoveryState {
    graph: crate::operator_graph::OperatorGraph,
    recovered_mv_store: crate::mv_store::MvStore,
    recovered_source_wms: rustc_hash::FxHashMap<String, i64>,
    recovered_watermark_frontier: Option<i64>,
    recovered_all_sources_idle: bool,
    source_watermark_recovery_selected: bool,
    #[cfg(feature = "cluster")]
    reconciled_source_handoff_version: Option<u64>,
    restored_reference_tables: bool,
}

struct PipelineWatermarks {
    stream_entries: Vec<Arc<crate::catalog::StreamEntry>>,
    watermark_states: FxHashMap<String, SourceWatermarkState>,
    source_entries: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    source_ids: FxHashMap<String, usize>,
    tracker: Option<laminar_core::time::WatermarkTracker>,
}

struct PipelineRuntimeSetup {
    sources: Vec<TrackedSourceRegistration>,
    config: crate::pipeline::PipelineConfig,
    callback: crate::pipeline_callback::ConnectorPipelineCallback,
    force_checkpoint_rx: crate::db::ForceCheckpointRx,
    checkpoint_complete_rx:
        crossfire::AsyncRx<crossfire::mpsc::Array<crate::pipeline::CheckpointCompletion>>,
    checkpoint_in_flight: Arc<std::sync::atomic::AtomicU64>,
    coordinated_commit_admission: Option<crate::checkpoint_coordinator::CoordinatedCommitAdmission>,
    #[cfg(feature = "cluster")]
    source_process_authority: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    runtime_mode: RuntimeMode,
}

struct PreparedPipelineRuntime {
    runtime: PipelineRuntimeSetup,
    coordinated_committer: Option<crate::coordinated_committer::CoordinatedCommitter>,
    committer_poll: std::time::Duration,
    committer_notify: Arc<tokio::sync::Notify>,
}

type ReferenceTableRuntimeSource = (
    String,
    Box<dyn laminar_connectors::reference::ReferenceTableSource>,
);

async fn close_reference_table_sources(
    table_sources: &mut [ReferenceTableRuntimeSource],
) -> Result<(), DbError> {
    let mut first_error = None;
    for (name, source) in table_sources {
        if let Err(error) = source.close().await {
            first_error.get_or_insert_with(|| {
                DbError::Connector(format!("Table '{name}' snapshot close error: {error}"))
            });
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn create_reference_table_sources(
    connector_registry: &laminar_connectors::registry::ConnectorRegistry,
    table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
    table_store: &parking_lot::RwLock<crate::table_store::TableStore>,
    restored_complete_inventory: bool,
) -> Result<Vec<ReferenceTableRuntimeSource>, DbError> {
    if restored_complete_inventory {
        return Ok(Vec::new());
    }

    let mut registrations: Vec<_> = table_regs
        .iter()
        .filter(|(_, registration)| {
            registration.connector_type.is_some() && !registration.on_demand
        })
        .collect();
    registrations.sort_unstable_by_key(|(name, _)| *name);

    let mut sources = Vec::with_capacity(registrations.len());
    for (name, registration) in registrations {
        let result = (|| {
            let config = crate::connector_manager::build_table_config(registration)?;
            let schema = table_store.read().table_schema(name).ok_or_else(|| {
                DbError::Pipeline(format!("Reference table '{name}' has no registered schema"))
            })?;
            connector_registry
                .create_table_source(&config, schema)
                .map_err(|error| {
                    DbError::Connector(format!("Cannot create table source '{name}': {error}"))
                })
        })();

        match result {
            Ok(source) => sources.push((name.clone(), source)),
            Err(error) => {
                if let Err(close_error) = close_reference_table_sources(&mut sources).await {
                    tracing::warn!(%close_error, "Failed to close table sources after startup error");
                }
                return Err(error);
            }
        }
    }
    Ok(sources)
}

async fn hydrate_reference_table_sources(
    mut table_sources: Vec<ReferenceTableRuntimeSource>,
    table_store: &parking_lot::RwLock<crate::table_store::TableStore>,
) -> Result<Vec<String>, DbError> {
    let mut prepared = Vec::with_capacity(table_sources.len());
    let mut names = Vec::with_capacity(table_sources.len());
    let mut hydration_error = None;

    for (name, source) in &mut table_sources {
        let mut batches = Vec::new();
        loop {
            match source.poll_snapshot().await {
                Ok(Some(batch)) => batches.push(batch),
                Ok(None) => break,
                Err(error) => {
                    hydration_error = Some(DbError::Connector(format!(
                        "Table '{name}' snapshot error: {error}"
                    )));
                    break;
                }
            }
        }
        if hydration_error.is_some() {
            break;
        }

        match table_store.read().prepare_snapshot(name, &batches) {
            Ok(snapshot) => {
                prepared.push(snapshot);
                names.push(name.clone());
            }
            Err(error) => {
                hydration_error = Some(DbError::Connector(format!(
                    "Table '{name}' snapshot validation error: {error}"
                )));
                break;
            }
        }
    }

    let close_result = close_reference_table_sources(&mut table_sources).await;
    if let Some(error) = hydration_error {
        if let Err(close_error) = close_result {
            tracing::warn!(%close_error, "Failed to close table sources after snapshot error");
        }
        return Err(error);
    }
    close_result?;

    table_store
        .write()
        .install_prepared_snapshots(prepared)
        .map_err(|error| DbError::Connector(format!("Table snapshot install error: {error}")))?;
    Ok(names)
}

/// Resolve and validate a sink before any external I/O. Keeping this boundary separate from the
/// bounded open stage makes it impossible for one connector to become active before every sink is
/// known to be admissible.
fn admit_sink(
    sink: &dyn SinkConnector,
    context: SinkAdmissionContext<'_>,
) -> Result<(SinkContract, Option<u64>), DbError> {
    let SinkAdmissionContext {
        config,
        name,
        input,
        delivery,
        runtime,
        carries_changelog,
        checkpointing_enabled,
        state_backend_scope,
    } = context;
    let contract = sink.contract(config).map_err(|e| {
        DbError::Config(format!(
            "sink '{name}' (type '{}') has an invalid contract: {e}",
            config.connector_type()
        ))
    })?;

    admit_sink_contract(contract, delivery, runtime, carries_changelog).map_err(|reason| {
        let detail = format!(
            "sink '{name}' is not admissible in {runtime:?} mode with {delivery} delivery: \
             {reason} (contract: {contract:?})"
        );
        if carries_changelog && !contract.accepts_full_changelog() {
            DbError::MaterializedView(format!(
                "[LDB-1300] {detail}. Route '{input}' to a FullChangelog sink or disable \
                 incremental emission."
            ))
        } else {
            DbError::Config(format!("[LDB-5035] {detail}"))
        }
    })?;

    if delivery == DeliveryGuarantee::ExactlyOnce {
        if !checkpointing_enabled {
            return Err(DbError::Config(format!(
                "[LDB-5035] sink '{name}' cannot run exactly-once without checkpointing"
            )));
        }
        let required_scope = required_recovery_scope(runtime);
        if !state_backend_scope.satisfies(required_scope) {
            return Err(DbError::Config(format!(
                "[LDB-5035] sink '{name}' cannot run exactly-once: prepared participant markers \
                 require {required_scope:?} state, but the configured backend is \
                 {state_backend_scope:?}"
            )));
        }
        if sink.as_coordinated_committer().is_none() {
            return Err(DbError::Config(format!(
                "[LDB-5035] sink '{name}' claims {contract:?} but does not implement the complete \
                 coordinated exact protocol: {EXACT_SINK_PROTOCOL}"
            )));
        }
    } else if sink.as_coordinated_committer().is_some() {
        return Err(DbError::Config(format!(
            "[LDB-5035] sink '{name}' exposes a coordinated committer outside global \
             exactly-once delivery"
        )));
    }

    let configured_timeout = config
        .get_parsed::<u64>("sink.write.timeout.ms")
        .map_err(|e| {
            DbError::Connector(format!(
                "Invalid 'sink.write.timeout.ms' for sink '{name}': {e}"
            ))
        })?;
    if configured_timeout == Some(0) {
        return Err(DbError::Connector(format!(
            "sink '{name}': sink.write.timeout.ms must be > 0"
        )));
    }

    Ok((contract, configured_timeout))
}

async fn close_opened_sinks(
    sinks: &mut [PreparedSink],
    cleanup_timeout: std::time::Duration,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
) {
    let cleanup_deadline = tokio::time::Instant::now() + cleanup_timeout;
    futures::future::join_all(sinks.iter_mut().rev().map(|prepared| {
        close_opened_sink(
            prepared,
            cleanup_deadline,
            #[cfg(feature = "cluster")]
            process_authority,
        )
    }))
    .await;
}

async fn close_opened_sink(
    prepared: &mut PreparedSink,
    cleanup_deadline: tokio::time::Instant,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
) {
    #[cfg(feature = "cluster")]
    if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
        return;
    }
    if tokio::time::Instant::now() >= cleanup_deadline {
        tracing::warn!(
            sink = %prepared.name,
            "sink close skipped after the pipeline-startup cleanup deadline"
        );
        return;
    }

    let mut close = std::pin::pin!(prepared.connector.close());
    #[cfg(feature = "cluster")]
    let close_result = if let Some(controller) = process_authority {
        tokio::select! {
            biased;
            () = controller.wait_for_process_lease_loss() => return,
            result = tokio::time::timeout_at(cleanup_deadline, close.as_mut()) => result,
        }
    } else {
        tokio::time::timeout_at(cleanup_deadline, close.as_mut()).await
    };
    #[cfg(not(feature = "cluster"))]
    let close_result = tokio::time::timeout_at(cleanup_deadline, close.as_mut()).await;
    match close_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(
                sink = %prepared.name,
                %error,
                "sink close failed while rolling back pipeline startup"
            );
        }
        Err(_) => {
            tracing::warn!(
                sink = %prepared.name,
                "sink close exceeded the shared pipeline-startup cleanup deadline"
            );
        }
    }
}

enum SinkOpenOutcome<T> {
    Completed(T),
    Deadline,
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

enum SinkOpenFailure {
    Connector(String),
    Retired(String),
    #[cfg(feature = "cluster")]
    ProcessAuthorityLost,
}

async fn await_sink_open<T>(
    deadline: tokio::time::Instant,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
    future: impl std::future::Future<Output = T>,
) -> SinkOpenOutcome<T> {
    if tokio::time::Instant::now() >= deadline {
        return SinkOpenOutcome::Deadline;
    }
    let mut operation = std::pin::pin!(future);

    #[cfg(feature = "cluster")]
    if let Some(controller) = process_authority {
        if !controller.process_lease_is_live() {
            return SinkOpenOutcome::ProcessAuthorityLost;
        }
        return tokio::select! {
            biased;
            () = controller.wait_for_process_lease_loss() => {
                SinkOpenOutcome::ProcessAuthorityLost
            }
            () = tokio::time::sleep_until(deadline) => {
                SinkOpenOutcome::Deadline
            }
            result = &mut operation => {
                if controller.process_lease_is_live() {
                    SinkOpenOutcome::Completed(result)
                } else {
                    SinkOpenOutcome::ProcessAuthorityLost
                }
            }
        };
    }

    match tokio::time::timeout_at(deadline, operation.as_mut()).await {
        Ok(result) => SinkOpenOutcome::Completed(result),
        Err(_) => SinkOpenOutcome::Deadline,
    }
}

async fn open_prepared_sinks(
    sinks: &mut [PreparedSink],
    open_timeout: std::time::Duration,
    #[cfg(feature = "cluster")] process_authority: Option<
        &laminar_core::cluster::control::ClusterController,
    >,
) -> Result<(), DbError> {
    let open_deadline = tokio::time::Instant::now() + open_timeout;
    let mut index = 0;
    while index < sinks.len() {
        if tokio::time::Instant::now() >= open_deadline {
            #[cfg(feature = "cluster")]
            if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
                // A generic close may publish. Cluster startup therefore drops the unopened
                // generation instead of beginning cleanup that could cross the authority fence.
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{}': cluster process lease expired during sink open",
                    sinks[index].name
                )));
            }
            // Tokio's timeout polls its inner future once even at an expired deadline. Do not
            // construct or poll another connector open after the shared startup budget is gone.
            close_opened_sinks(
                &mut sinks[..index],
                crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                #[cfg(feature = "cluster")]
                process_authority,
            )
            .await;
            return Err(DbError::Connector(format!(
                "Failed to open sink '{}': shared {open_timeout:?} sink-open stage deadline was exhausted before open began",
                sinks[index].name
            )));
        }
        let prepared = &mut sinks[index];
        let name = prepared.name.clone();
        let cancellation_policy = prepared.connector.cancellation_policy();
        let open_error = {
            let open = prepared.connector.open(&prepared.config);
            match await_sink_open(
                open_deadline,
                #[cfg(feature = "cluster")]
                process_authority,
                open,
            )
            .await
            {
                SinkOpenOutcome::Completed(Ok(())) => None,
                SinkOpenOutcome::Completed(Err(error)) => {
                    if error.is_outcome_unknown() {
                        Some(SinkOpenFailure::Retired(error.to_string()))
                    } else {
                        Some(SinkOpenFailure::Connector(error.to_string()))
                    }
                }
                SinkOpenOutcome::Deadline => Some(
                    if cancellation_policy == ConnectorCancellationPolicy::RetireConnector {
                        SinkOpenFailure::Retired(format!(
                            "exceeded the shared {open_timeout:?} sink-open stage deadline"
                        ))
                    } else {
                        SinkOpenFailure::Connector(format!(
                            "exceeded the shared {open_timeout:?} sink-open stage deadline"
                        ))
                    },
                ),
                #[cfg(feature = "cluster")]
                SinkOpenOutcome::ProcessAuthorityLost => {
                    Some(SinkOpenFailure::ProcessAuthorityLost)
                }
            }
        };
        match open_error {
            Some(SinkOpenFailure::Connector(error)) => {
                #[cfg(feature = "cluster")]
                if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
                    return Err(DbError::Connector(format!(
                        "Failed to open sink '{name}': cluster process lease expired during sink open"
                    )));
                }
                // A failed/cancelled open may already hold resources, so include the current sink.
                close_opened_sinks(
                    &mut sinks[..=index],
                    crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                    #[cfg(feature = "cluster")]
                    process_authority,
                )
                .await;
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{name}': {error}"
                )));
            }
            Some(SinkOpenFailure::Retired(error)) => {
                #[cfg(feature = "cluster")]
                if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
                    return Err(DbError::Connector(format!(
                        "Failed to open sink '{name}': cluster process lease expired during sink open"
                    )));
                }
                // Dropping a timed-out open makes this generation terminal. Clean up only
                // connectors whose opens completed; never invoke another method on the retired
                // candidate.
                close_opened_sinks(
                    &mut sinks[..index],
                    crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
                    #[cfg(feature = "cluster")]
                    process_authority,
                )
                .await;
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{name}': {error}"
                )));
            }
            #[cfg(feature = "cluster")]
            Some(SinkOpenFailure::ProcessAuthorityLost) => {
                // Generic close may flush or publish. Once cluster authority is gone, drop the
                // connector generation without invoking any further connector operation.
                return Err(DbError::Connector(format!(
                    "Failed to open sink '{name}': cluster process lease expired during sink open"
                )));
            }
            None => {}
        }
        index += 1;
    }

    #[cfg(feature = "cluster")]
    if process_authority.is_some_and(|controller| !controller.process_lease_is_live()) {
        return Err(DbError::Connector(
            "cluster process lease expired after the sink-open stage".into(),
        ));
    }
    Ok(())
}

/// Resolve a query's output schema by planning it. On `ASOF JOIN` failure,
/// retries with the schema-equivalent rewrite. Returns `None` if the query
/// still can't be planned (e.g. a dependency isn't registered yet).
pub(crate) async fn plan_output_schema(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Option<arrow_schema::SchemaRef> {
    let plan = if let Ok(plan) = ctx.state().create_logical_plan(sql).await {
        plan
    } else {
        let rewritten = crate::sql_analysis::rewrite_asof_joins_for_planning(sql)?;
        ctx.state().create_logical_plan(&rewritten).await.ok()?
    };
    let fields: Vec<_> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| (**f).clone())
        .collect();
    Some(Arc::new(arrow_schema::Schema::new(fields)))
}

async fn resolve_stream_output_schemas(
    ctx: &datafusion::prelude::SessionContext,
    stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
) -> Result<HashMap<String, arrow_schema::SchemaRef>, DbError> {
    use datafusion::datasource::empty::EmptyTable;

    let mut out: HashMap<String, arrow_schema::SchemaRef> =
        HashMap::with_capacity(stream_regs.len());
    let mut pending: Vec<&crate::connector_manager::StreamRegistration> =
        stream_regs.values().collect();
    let mut placeholders: Vec<String> = Vec::new();

    let result: Result<(), DbError> = async {
        while !pending.is_empty() {
            let mut next: Vec<&crate::connector_manager::StreamRegistration> = Vec::new();
            let mut progressed = false;
            for reg in pending {
                let Some(schema) = plan_output_schema(ctx, &reg.query_sql).await else {
                    next.push(reg);
                    continue;
                };

                if !ctx
                    .table_exist(exact_table_reference(&reg.name))
                    .unwrap_or(false)
                {
                    ctx.register_table(
                        exact_table_reference(&reg.name),
                        Arc::new(EmptyTable::new(schema.clone())),
                    )
                    .map_err(|e| {
                        DbError::Pipeline(format!(
                            "could not register placeholder for stream '{}': {e}",
                            reg.name
                        ))
                    })?;
                    placeholders.push(reg.name.clone());
                }
                out.insert(reg.name.clone(), schema);
                progressed = true;
            }

            if !progressed {
                let mut unresolved: Vec<&str> = next.iter().map(|r| r.name.as_str()).collect();
                unresolved.sort_unstable();
                // For ASOF joins, report the rewritten-plan error (the raw planner
                // just says "AsOf unsupported", which masks the real blocker).
                let sql = &next[0].query_sql;
                let err = match crate::sql_analysis::rewrite_asof_joins_for_planning(sql) {
                    Some(rewritten) => ctx.state().create_logical_plan(&rewritten).await.err(),
                    None => ctx.state().create_logical_plan(sql).await.err(),
                }
                .map_or_else(|| "unknown error".to_string(), |e| e.to_string());
                return Err(DbError::Pipeline(format!(
                    "unresolvable stream dependency among [{}]: {err}",
                    unresolved.join(", ")
                )));
            }
            pending = next;
        }
        Ok(())
    }
    .await;

    for name in &placeholders {
        let _ = ctx.deregister_table(exact_table_reference(name));
    }

    result.map(|()| out)
}

/// Prune timestamps outside `window`; if under `max_restarts`, record `now` and return
/// the 0-based attempt index within the window. `None` once the budget is exhausted.
fn claim_restart_slot(
    history: &mut Vec<std::time::Instant>,
    now: std::time::Instant,
    max_restarts: usize,
    window: std::time::Duration,
) -> Option<usize> {
    history.retain(|t| now.duration_since(*t) < window);
    if history.len() >= max_restarts {
        None
    } else {
        let attempt = history.len();
        history.push(now);
        Some(attempt)
    }
}

/// Exponential backoff `initial * 2^attempt`, saturating and capped at `max`.
fn backoff_for_attempt(
    initial: std::time::Duration,
    max: std::time::Duration,
    attempt: usize,
) -> std::time::Duration {
    let shift = u32::try_from(attempt).unwrap_or(u32::MAX).min(20);
    initial.saturating_mul(1u32 << shift).min(max)
}

/// Drive supervised restart independently of the watcher that observed the fault.
///
/// The database control runtime remains stable across caller-runtime teardown and is also where
/// the replacement watcher, connector tasks, and committer are spawned.
fn spawn_supervised_restart(
    db: Arc<LaminarDB>,
    history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
) -> Result<tokio::task::JoinHandle<()>, DbError> {
    let handle = db.control_runtime.handle()?;
    Ok(handle.spawn(attempt_supervised_restart(db, history, metrics)))
}

/// One recover-from-checkpoint restart, honoring the restart budget.
async fn attempt_supervised_restart(
    db: Arc<LaminarDB>,
    history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
) {
    if let Err(error) = db.ensure_catalog_cleanup_unfenced("supervised restart") {
        tracing::error!(%error, "supervisor rejected restart of terminally fenced database");
        return;
    }
    let policy = db.config.restart_policy.clone();
    let slot = {
        let mut hist = history.lock();
        claim_restart_slot(
            &mut hist,
            std::time::Instant::now(),
            policy.max_restarts,
            policy.window,
        )
    };
    let Some(attempt) = slot else {
        tracing::error!(
            max = policy.max_restarts,
            "pipeline faulted too many times within the restart window; \
             staying faulted for manual recovery"
        );
        return;
    };
    let backoff = backoff_for_attempt(policy.initial_backoff, policy.max_backoff, attempt);
    tokio::time::sleep(backoff).await;
    // A concurrent stop/shutdown moves the state out of Faulted; don't fight it — and don't count a
    // restart that won't happen, so a benign stop during backoff doesn't inflate the metric.
    if !matches!(DbState::load(&db.state), DbState::Faulted) {
        return;
    }
    if let Some(ref m) = metrics {
        m.pipeline_restarts_total.inc();
    }
    // Capture the reason before start() clears `last_fault`, so it survives in the log.
    let fault = db.last_fault().unwrap_or_else(|| "unknown".to_string());
    tracing::warn!(
        fault = %fault, ?backoff,
        "auto-restarting faulted pipeline from last checkpoint"
    );
    if let Err(e) = db.start().await {
        tracing::error!(error = %e, "auto-restart failed; pipeline left non-running");
    }
}

struct StartupDriverGuard {
    attempt: Arc<StartupAttempt>,
    state: Arc<std::sync::atomic::AtomicU8>,
    last_fault: Arc<parking_lot::Mutex<Option<String>>>,
    armed: bool,
}

impl StartupDriverGuard {
    fn new(db: &LaminarDB, attempt: Arc<StartupAttempt>) -> Self {
        Self {
            attempt,
            state: Arc::clone(&db.state),
            last_fault: Arc::clone(&db.last_fault),
            armed: true,
        }
    }

    fn finish(mut self, mut result: Result<(), DbError>) {
        let observed = DbState::load(&self.state);
        if result.is_ok() && observed != DbState::Running {
            result = Err(DbError::Pipeline(format!(
                "startup driver completed in {observed:?} instead of Running"
            )));
            DbState::Faulted.store(&self.state);
        } else if result.is_err() && observed == DbState::Starting {
            self.last_fault
                .lock()
                .get_or_insert_with(|| "startup failed before publishing a terminal state".into());
            DbState::Faulted.store(&self.state);
        }
        self.attempt.complete(result);
        self.armed = false;
    }
}

impl Drop for StartupDriverGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let message =
            "startup driver was cancelled before terminal cleanup; pipeline remains fenced";
        self.last_fault.lock().get_or_insert(message.into());
        DbState::Faulted.store(&self.state);
        self.attempt
            .complete(Err(DbError::Pipeline(message.into())));
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic")
}

fn publish_runtime_fault_state(state: &std::sync::atomic::AtomicU8) -> bool {
    loop {
        let observed = DbState::load(state);
        match observed {
            DbState::Starting | DbState::Running => {
                if DbState::compare_exchange(observed, DbState::Faulted, state).is_ok() {
                    return true;
                }
            }
            DbState::Faulted | DbState::Created | DbState::ShuttingDown | DbState::Stopped => {
                return false;
            }
        }
    }
}

impl LaminarDB {
    #[cfg(feature = "cluster")]
    fn validate_boot_vnode_recovery_target(
        &self,
        recovered_attempt: laminar_core::state::CheckpointAttempt,
        recovered_assignment_version: u64,
    ) -> Result<Option<u64>, DbError> {
        let registry = self
            .vnode_registry
            .lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                DbError::Checkpoint("[LDB-6031] cluster recovery has no vnode registry".into())
            })?;
        let assignment = registry.versioned_snapshot();
        let Some(handoff_attempt) = assignment.source_handoff_attempt() else {
            return Ok(None);
        };
        let matches_installed_handoff = handoff_attempt == recovered_attempt
            && assignment.source_handoff_assignment_version() == Some(recovered_assignment_version);
        // The registry handoff records the cut used to acquire this assignment; it is not a
        // moving latest-checkpoint pointer. A later committed checkpoint may therefore supersede
        // it without changing ownership. Its fence must be no older than the handoff publication
        // and no newer than the registry generation this process is rebuilding.
        let supersedes_installed_handoff = recovered_attempt.relation_to(handoff_attempt)
            == laminar_core::state::CheckpointAttemptRelation::Newer
            && assignment
                .source_handoff_installed_version()
                .is_some_and(|installed| installed <= recovered_assignment_version)
            && recovered_assignment_version <= assignment.version();
        if !matches_installed_handoff && !supersedes_installed_handoff {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6031] recovered vnode attempt {recovered_attempt:?} at assignment \
                 {recovered_assignment_version} does not match installed source handoff \
                 {handoff_attempt:?} at assignment {:?}",
                assignment.source_handoff_assignment_version()
            )));
        }
        Ok(assignment.source_handoff_installed_version())
    }

    #[cfg(feature = "cluster")]
    fn validate_fresh_cluster_vnode_start(&self) -> Result<(), DbError> {
        let registry = self
            .vnode_registry
            .lock()
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                DbError::Checkpoint("[LDB-6031] cluster recovery has no vnode registry".into())
            })?;
        if self.has_unapplied_vnode_transition(&registry)
            || registry.versioned_snapshot().has_committed_handoff()
        {
            return Err(DbError::Checkpoint(
                "[LDB-6031] cluster startup found committed or staged vnode state but no exact \
                 recovered checkpoint; refusing a fresh graph"
                    .into(),
            ));
        }
        Ok(())
    }

    /// Prepare the success marker for a graph generation with no vnode transition callbacks.
    /// Startup holds `assignment_adoption_lock`, so the registry cannot be overtaken while durable
    /// history is revalidated. The marker is installed at the compute-generation ready boundary.
    #[cfg(feature = "cluster")]
    async fn prepare_graph_ready_vnode_state_binding(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Option<crate::vnode_transition_staging::InstalledVnodeStateBinding>, DbError> {
        let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(
                "cluster graph readiness has no vnode registry for installed-state binding".into(),
            )
        })?;
        let assignment = registry.versioned_snapshot();
        if assignment.version() == 0 || self.has_unapplied_vnode_transition(&registry) {
            return Ok(None);
        }

        let pipeline_identity = self
            .coordinator
            .lock()
            .await
            .as_ref()
            .map(crate::checkpoint_coordinator::CheckpointCoordinator::bound_pipeline_identity)
            .transpose()?;
        let Some(pipeline_identity) = pipeline_identity else {
            return Ok(None);
        };
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint(
                "cluster graph readiness has no controller for assignment validation".into(),
            )
        })?;
        let store = self
            .assignment_snapshot_store
            .lock()
            .clone()
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "cluster graph readiness has no durable assignment history".into(),
                )
            })?;
        let durable = tokio::time::timeout_at(deadline, store.load_version(assignment.version()))
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "assignment {} history read timed out at graph readiness",
                    assignment.version()
                ))
            })?
            .map_err(|error| DbError::Checkpoint(error.to_string()))?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "assignment {} is absent from durable history at graph readiness",
                    assignment.version()
                ))
            })?;
        tokio::time::timeout_at(
            deadline,
            crate::rebalance::audit_assignment_snapshot_authority(
                &store,
                Some(controller.as_ref()),
                &durable,
            ),
        )
        .await
        .map_err(|_| {
            DbError::Checkpoint(format!(
                "assignment {} authority audit timed out at graph readiness",
                assignment.version()
            ))
        })?
        .map_err(DbError::Checkpoint)?;
        let durable_owners = durable
            .to_vnode_vec(registry.vnode_count())
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        if durable.draining
            || durable.version != assignment.version()
            || durable_owners.as_slice() != assignment.owners()
        {
            return Err(DbError::Checkpoint(format!(
                "assignment {} durable history does not match the graph-ready registry",
                assignment.version()
            )));
        }
        let fence = durable
            .assignment_fence()
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        match fence.participant_incarnation(controller.instance_id().0) {
            Some(boot_incarnation) if boot_incarnation == controller.recovery_incarnation() => {}
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "assignment {} names a different local process incarnation at graph readiness",
                    assignment.version()
                )));
            }
            None => return Ok(None),
        }

        // Revalidate after external I/O even though startup still owns assignment adoption.
        let current = registry.versioned_snapshot();
        if current.version() != assignment.version()
            || current.owners() != assignment.owners()
            || self.has_unapplied_vnode_transition(&registry)
        {
            return Err(DbError::Checkpoint(format!(
                "assignment {} changed or gained vnode work before graph-ready publication",
                assignment.version()
            )));
        }
        Ok(Some(
            crate::vnode_transition_staging::InstalledVnodeStateBinding::new(
                fence,
                pipeline_identity,
            )?,
        ))
    }

    #[cfg(feature = "cluster")]
    async fn publish_boot_vnode_restore_transition(
        &self,
        registry: &laminar_core::state::VnodeRegistry,
        target_assignment: &laminar_core::state::VnodeAssignmentSnapshot,
        target_fence: laminar_core::checkpoint::CheckpointAssignmentFence,
        participant: laminar_core::checkpoint::CheckpointParticipant,
        restore_cut: crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut,
        loaded: crate::recovery_manager::vnode_chains::LoadedVnodeChains,
    ) -> Result<(), DbError> {
        // Pin the registry publication through staging. The startup assignment lock prevents a
        // competing adopter from beginning, while this read guard also makes the helper itself
        // reject a target that changed during the backend read.
        let current_assignment = registry.read_assignment();
        if current_assignment.version() != target_assignment.version()
            || current_assignment.owners() != target_assignment.owners()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6031] boot vnode recovery target assignment {} changed to {} before \
                 staging",
                target_assignment.version(),
                current_assignment.version()
            )));
        }
        let local_node_id = participant.node_id;
        let transition = Arc::new(
            crate::vnode_transition_staging::PendingVnodeTransition::boot_recovery(
                target_fence,
                target_assignment.owners(),
                participant,
                restore_cut.pipeline_identity().clone(),
                restore_cut,
                loaded,
            )?,
        );
        let target_owned = transition.acquired_vnodes().to_vec();
        let stale_unowned: Vec<u32> = registry
            .restoring_vnodes()
            .into_iter()
            .filter(|vnode| {
                current_assignment.owners().get(*vnode as usize).copied()
                    != Some(laminar_core::state::NodeId(local_node_id))
            })
            .collect();
        drop(current_assignment);

        // The caller owns the startup assignment and generation fences. Publish lifecycle and
        // the complete immutable replacement while the exact target assignment remains pinned.
        // Reserve the new owned roster before exposing its transition so hot-path readiness can
        // observe a conservative false positive, never a false Active state. Only lifecycle
        // entries proven unowned by this exact target may be released from stale boot work.
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6031] boot vnode staging has no cluster controller".into())
        })?;
        self.publish_local_vnode_state_report(&controller, target_assignment, false)
            .await?;
        let mut pending = self.pending_vnode_transition.lock();
        registry.mark_restoring(&target_owned);
        *pending = Some(transition);
        registry.mark_active(&stale_unowned);
        Ok(())
    }

    /// Fence new work and wake the running pipeline without waiting for teardown.
    pub fn close(&self) {
        let runtime_shutdown = self.runtime_shutdown.write();
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Release);
        runtime_shutdown.cancel();
        self.shutdown_signal.notify_one();
    }

    /// Prepare each boot-owned vnode's chain at the exact committed cut the sources resume from.
    #[cfg(feature = "cluster")]
    async fn prepare_boot_vnode_restore_transition(
        &self,
        restore_cut: &crate::checkpoint_coordinator::ValidatedClusterVnodeRestoreCut,
    ) -> Result<(), DbError> {
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("[LDB-6031] cluster recovery has no live cluster controller".into())
        })?;
        let self_id = controller.instance_id();
        let registry = match self.vnode_registry.lock().as_ref() {
            Some(registry) => Arc::clone(registry),
            None => {
                return Err(DbError::Checkpoint(
                    "[LDB-6031] cluster recovery has no vnode registry".into(),
                ));
            }
        };
        let target_assignment = registry.versioned_snapshot();
        let owned = laminar_core::state::owned_vnodes(&registry, self_id);
        if target_assignment.version() == 0 {
            let has_pending_transition = self.pending_vnode_transition.lock().is_some();
            let has_installed_state = self.installed_vnode_state.lock().is_some();
            if !owned.is_empty()
                || target_assignment
                    .owners()
                    .iter()
                    .any(|owner| *owner != laminar_core::state::NodeId::UNASSIGNED)
                || has_pending_transition
                || has_installed_state
                || registry.any_restoring()
                || target_assignment.has_committed_handoff()
            {
                return Err(DbError::Checkpoint(
                    "[LDB-6031] unassigned cluster boot has retained vnode lifecycle state".into(),
                ));
            }
            // Existing clusters intentionally construct replacement processes unassigned. Source
            // intake remains fenced while the audited durable assignment is adopted after the
            // graph and coordinator start; that adoption loads the exact committed vnode cut.
            // Do not invent assignment-zero authority or mark any vnode active here.
            tracing::info!(
                epoch = restore_cut.attempt().epoch,
                checkpoint_id = restore_cut.attempt().checkpoint_id,
                "cluster recovery: deferring vnode restore until durable assignment adoption"
            );
            return Ok(());
        }
        let assignment_store = self
            .assignment_snapshot_store
            .lock()
            .clone()
            .ok_or_else(|| {
                DbError::Checkpoint(
                    "[LDB-6031] boot vnode recovery has no durable assignment history".into(),
                )
            })?;
        let target_snapshot = assignment_store
            .load_version(target_assignment.version())
            .await
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "[LDB-6031] load boot assignment {}: {error}",
                    target_assignment.version()
                ))
            })?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6031] boot assignment {} is unavailable",
                    target_assignment.version()
                ))
            })?;
        crate::rebalance::audit_assignment_snapshot_authority(
            &assignment_store,
            Some(controller.as_ref()),
            &target_snapshot,
        )
        .await
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "[LDB-6031] boot assignment {} authority audit failed: {error}",
                target_assignment.version()
            ))
        })?;
        let target_owners = target_snapshot
            .to_vnode_vec(registry.vnode_count())
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        if target_snapshot.version != target_assignment.version()
            || target_owners.as_slice() != target_assignment.owners()
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6031] boot assignment {} does not match the installed owner map",
                target_assignment.version()
            )));
        }
        let target_fence = target_snapshot
            .assignment_fence()
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        let attempt = restore_cut.attempt();
        tracing::info!(
            owned = owned.len(),
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            "cluster recovery: rehydrating boot-owned vnodes from chains"
        );
        if owned.is_empty() {
            // The audited target proves that this replacement graph owns no vnode state. Retire
            // any prior graph's exact transition only after pinning that target; preparation
            // failures above deliberately leave the prior Arc and lifecycle untouched.
            let current_assignment = registry.read_assignment();
            if current_assignment.version() != target_assignment.version()
                || current_assignment.owners() != target_assignment.owners()
            {
                return Err(DbError::Checkpoint(format!(
                    "[LDB-6031] boot vnode recovery target assignment {} changed to {} before empty-state publication",
                    target_assignment.version(),
                    current_assignment.version()
                )));
            }
            let stale_unowned: Vec<u32> = registry
                .restoring_vnodes()
                .into_iter()
                .filter(|vnode| {
                    current_assignment.owners().get(*vnode as usize).copied()
                        != Some(laminar_core::state::NodeId(self_id.0))
                })
                .collect();
            let mut pending = self.pending_vnode_transition.lock();
            let mut installed = self.installed_vnode_state.lock();
            pending.take();
            installed.take();
            registry.mark_active(&stale_unowned);
            return Ok(());
        }
        let Some(backend) = self.state_backend.lock().clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6031] cluster recovery requires a durable state backend but none is \
                 wired — refusing to start with staged source offsets and empty aggregate state"
                    .to_string(),
            ));
        };
        let max_partial_bytes = self.checkpoint_state_artifact_cap_bytes()?;
        let loaded =
            crate::recovery_manager::vnode_chains::SealedVnodeChainReader::from_validated_head(
                backend.as_ref(),
                restore_cut.restore_head(),
                max_partial_bytes,
                MAX_ARTIFACTS_PER_CLUSTER_VNODE_CHAIN,
            )?
            .load_at(&owned, attempt)
            .await?;
        self.publish_boot_vnode_restore_transition(
            &registry,
            &target_assignment,
            target_fence,
            laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: controller.recovery_incarnation(),
            },
            restore_cut.clone(),
            loaded,
        )
        .await?;
        tracing::info!(
            staged = owned.len(),
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            "cluster recovery: staged boot-owned vnodes for operator recovery"
        );
        Ok(())
    }

    /// Returns `true` if the database has been shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Enable auto-restart from the last checkpoint on a fault. Without it, a fault parks
    /// in `Faulted` for manual restart (the embedded default).
    pub fn enable_supervision(self: &Arc<Self>) {
        *self.supervisor_self.lock() = Arc::downgrade(self);
    }

    /// Make the next [`Self::start`] restore to `epoch` (the cluster-agreed cut) instead
    /// of the local latest. Cleared on start.
    #[cfg(feature = "cluster")]
    pub fn set_recover_target_epoch(&self, epoch: u64) {
        *self.recover_target_epoch.lock() = Some(epoch);
    }

    /// Open or close the source-intake gate. Closed (`true`) during a coordinated round until
    /// the restore quorum, so no node re-shuffles its replay into a peer that hasn't rebound.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_source_gate(&self, closed: bool) {
        if closed {
            self.source_gate
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let _transition = self.cluster_authority_transition.lock();
        if self
            .pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
        {
            self.source_gate
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        let process_authority_live = !self.is_cluster_runtime()
            || self
                .cluster_controller
                .lock()
                .as_ref()
                .is_some_and(|controller| controller.process_lease_is_live());
        if !process_authority_live {
            self.source_gate
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return;
        }
        if !self
            .cluster_authority_revoked
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.source_gate
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Keep clustered source intake closed while startup restores state and certifies assignment.
    /// Call before [`Self::start`]; [`Self::finish_cluster_startup`] is the only startup path that
    /// opens the gate.
    #[cfg(feature = "cluster")]
    pub fn fence_cluster_startup(&self) {
        self.set_source_gate(true);
        if let Some(controller) = self.cluster_controller.lock().as_ref() {
            controller.set_recovering(true);
        }
    }

    /// Retain exclusive lifecycle ownership across a coordinated stop/report/start/release round.
    #[cfg(feature = "cluster")]
    pub(crate) fn fence_coordinated_recovery_lifecycle(&self) {
        let _lifecycle_claim = self.startup_attempt.lock();
        self.set_source_gate(true);
        self.coordinated_recovery_fenced
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Release lifecycle ownership after terminal consumption unless a replacement fault won the
    /// transition; its outstanding latch keeps public mutation fenced.
    #[cfg(feature = "cluster")]
    pub(crate) fn release_coordinated_recovery_lifecycle(&self) {
        let _lifecycle_claim = self.startup_attempt.lock();
        self.coordinated_recovery_fenced
            .store(false, std::sync::atomic::Ordering::Release);
        if self
            .pending_recovery_fault
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
        {
            self.coordinated_recovery_fenced
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    #[cfg(feature = "cluster")]
    fn ensure_pipeline_lifecycle_authorized(
        &self,
        authority: PipelineLifecycleAuthority,
        operation: &str,
    ) -> Result<(), DbError> {
        if self
            .coordinated_recovery_fenced
            .load(std::sync::atomic::Ordering::Acquire)
            && authority == PipelineLifecycleAuthority::Public
        {
            return Err(DbError::InvalidOperation(format!(
                "pipeline {operation} is fenced by coordinated recovery"
            )));
        }
        Ok(())
    }

    #[cfg(not(feature = "cluster"))]
    fn ensure_pipeline_lifecycle_authorized(
        authority: PipelineLifecycleAuthority,
        operation: &str,
    ) {
        let _ = (authority, operation);
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn ensure_coordinated_recovery_mutation_unfenced(
        &self,
        operation: &str,
    ) -> Result<(), DbError> {
        self.ensure_pipeline_lifecycle_authorized(PipelineLifecycleAuthority::Public, operation)
    }

    /// Permanently withdraw this process's clustered data-plane authority after lease loss.
    #[cfg(feature = "cluster")]
    pub fn revoke_cluster_authority(&self) {
        if !self.is_cluster_runtime() {
            return;
        }
        let _transition = self.cluster_authority_transition.lock();
        let first_revocation = !self
            .cluster_authority_revoked
            .swap(true, std::sync::atomic::Ordering::AcqRel);
        self.source_gate
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if first_revocation {
            self.invalidate_shuffle_assignment_fence();
        }
        let controller = self.cluster_controller.lock().clone();
        if let Some(controller) = controller.as_ref() {
            controller.fence_process_lease();
        }
    }

    /// Whether clustered source and shuffle intake is still fenced.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn cluster_intake_fenced(&self) -> bool {
        self.cluster_authority_revoked
            .load(std::sync::atomic::Ordering::Acquire)
            || self.source_gate.load(std::sync::atomic::Ordering::Acquire)
            || (self.is_cluster_runtime()
                && self
                    .cluster_controller
                    .lock()
                    .as_ref()
                    .is_none_or(|controller| !controller.process_lease_is_live()))
    }

    /// Whether a clustered runtime is held by coordinated recovery lifecycle authority.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn coordinated_recovery_in_progress(&self) -> bool {
        self.is_cluster_runtime()
            && self
                .coordinated_recovery_fenced
                .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Finish clustered startup after the exact assignment fence is available.
    ///
    /// Fresh nodes open intake only when no durable recovery round exists. A process that restored
    /// local state, or one that observes an active/stale round, remains fenced and requests a full
    /// coordinated rewind.
    ///
    /// # Errors
    ///
    /// Returns a checkpoint error when the local assignment has not been certified. Intake remains
    /// closed when recovery authority is unavailable so the monitor can retry without admitting
    /// records.
    #[cfg(feature = "cluster")]
    pub async fn finish_cluster_startup(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<ClusterStartupDisposition, DbError> {
        let authority_revision = self
            .assignment_authority_revision
            .load(std::sync::atomic::Ordering::Acquire);
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("cluster startup has no recovery controller".into())
        })?;
        let registry =
            self.vnode_registry.lock().clone().ok_or_else(|| {
                DbError::Checkpoint("cluster startup has no vnode assignment".into())
            })?;
        let assignment = registry.versioned_snapshot();
        let assignment_version = assignment.version();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let assignment_fence = controller
            .checkpoint_assignment_fence(assignment_version)
            .filter(|fence| fence.matches_owner_map(&owners))
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "cluster assignment {assignment_version} is not certified for source intake"
                ))
            })?;
        let local_id = controller.instance_id().0;
        let local_incarnation = assignment_fence.participant_incarnation(local_id);
        let idle = match local_incarnation {
            Some(incarnation) if incarnation == controller.recovery_incarnation() => false,
            Some(_) => {
                return Err(DbError::Checkpoint(format!(
                    "cluster assignment {assignment_version} certifies another incarnation of process {local_id}"
                )));
            }
            None if owners.contains(&local_id) => {
                return Err(DbError::Checkpoint(format!(
                    "cluster assignment {assignment_version} gives process {local_id} ownership without checkpoint authority"
                )));
            }
            None => true,
        };
        if idle {
            controller.set_recovering(false);
            let drain_transition = controller
                .checkpoint_drain_transition()
                .filter(|transition| transition.predecessor == assignment_fence);
            let activation = self
                .activate_assignment_authority(
                    &assignment_fence,
                    drain_transition,
                    authority_revision,
                    deadline,
                )
                .await?;
            if !activation.installed {
                self.set_source_gate(true);
                return Ok(ClusterStartupDisposition::RecoveryFenced);
            }
            return Ok(ClusterStartupDisposition::Idle);
        }
        let pending_fault =
            match tokio::time::timeout_at(deadline, controller.read_fault_reports()).await {
                Err(_) => {
                    controller.set_recovering(true);
                    return Err(DbError::Checkpoint(
                        "cluster startup recovery fault audit timed out".into(),
                    ));
                }
                Ok(Err(error)) => {
                    controller.set_recovering(true);
                    return Err(DbError::Checkpoint(format!(
                        "cluster startup recovery fault audit failed: {error}"
                    )));
                }
                Ok(Ok(reports)) => reports.iter().any(|(_, sequence)| *sequence != 0),
            };
        let Ok(active) = tokio::time::timeout_at(deadline, controller.observe_recover()).await
        else {
            return Err(DbError::Checkpoint(
                "cluster startup recovery authority read timed out".into(),
            ));
        };
        let active = match active {
            Ok(active) => active,
            Err(error) => {
                controller.set_recovering(true);
                tracing::error!(%error, "startup recovery authority is not currently valid");
                tokio::time::timeout_at(
                    deadline,
                    crate::coordinated_recovery::request_local_fault(
                        &controller,
                        &self.pending_recovery_fault,
                    ),
                )
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(
                            "startup recovery fault publication timed out".into(),
                        )
                    })?
                    .map_err(|report_error| {
                        DbError::Checkpoint(format!(
                            "startup recovery authority failed ({error}); fault publication failed: {report_error}"
                        ))
                    })?;
                return Ok(ClusterStartupDisposition::RecoveryFenced);
            }
        };
        let open_intake = if pending_fault {
            controller.set_recovering(true);
            false
        } else if let Some(active) = active {
            controller.set_recovering(true);
            if !controller.recovery_driver_is_current(&active.round)
                || !controller.recovery_round_contains_current_process(&active.round)
            {
                tokio::time::timeout_at(
                    deadline,
                    crate::coordinated_recovery::request_local_fault(
                        &controller,
                        &self.pending_recovery_fault,
                    ),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint("startup recovery fault publication timed out".into())
                })?
                .map_err(DbError::Checkpoint)?;
            }
            false
        } else if self.last_recovery_epoch.lock().is_some() {
            controller.set_recovering(true);
            tokio::time::timeout_at(
                deadline,
                crate::coordinated_recovery::request_local_fault(
                    &controller,
                    &self.pending_recovery_fault,
                ),
            )
            .await
            .map_err(|_| {
                DbError::Checkpoint("startup recovery fault publication timed out".into())
            })?
            .map_err(DbError::Checkpoint)?;
            false
        } else {
            controller.set_recovering(false);
            true
        };

        let drain_transition = controller
            .checkpoint_drain_transition()
            .filter(|transition| transition.predecessor == assignment_fence);
        let activation = self
            .activate_assignment_authority(
                &assignment_fence,
                drain_transition,
                authority_revision,
                deadline,
            )
            .await?;
        if !activation.installed {
            controller.set_recovering(true);
            self.set_source_gate(true);
            return Ok(ClusterStartupDisposition::RecoveryFenced);
        }
        Ok(if open_intake && activation.intake_open {
            ClusterStartupDisposition::Serving
        } else {
            ClusterStartupDisposition::RecoveryFenced
        })
    }

    /// Advance both shuffle directions to the coordinated recovery generation. Old streams are
    /// rejected so pre-rewind frames cannot be folded and then replayed.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_recovery_gen(&self, gen: u64) {
        // Fence inbound old-generation streams before outbound streams can emit the new one.
        if let Some(receiver) = self.shuffle_receiver.lock().as_ref() {
            receiver.set_recovery_gen(gen);
        }
        if let Some(sender) = self.shuffle_sender.lock().as_ref() {
            sender.set_recovery_gen(gen);
        }
    }

    /// Import the last recovery generation that reached an irrevocable cluster Release before a
    /// fresh pipeline starts. The allocation high-watermark is deliberately not used: a driver
    /// can reserve a generation and fail before any data plane rewinds to it.
    ///
    /// # Errors
    /// Returns an error when durable recovery authority is unavailable, the process lease is
    /// lost, or an already-active transport conflicts with the committed terminal.
    #[cfg(feature = "cluster")]
    pub async fn prepare_cluster_startup_recovery_generation(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
            DbError::Checkpoint("cluster recovery generation bootstrap has no controller".into())
        })?;
        if !controller.process_lease_is_live() {
            return Err(DbError::Checkpoint(
                "cluster recovery generation bootstrap lost its process lease".into(),
            ));
        }
        let terminal =
            tokio::time::timeout_at(deadline, controller.latest_committed_recover_release())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "committed recovery Release lookup exceeded its deadline".into(),
                    )
                })?
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "committed recovery Release authority is unavailable: {error}"
                    ))
                })?;
        let committed = terminal
            .as_ref()
            .map_or(0, |release| release.round.id.generation);
        if !controller.process_lease_is_live() {
            return Err(DbError::Checkpoint(
                "cluster recovery generation bootstrap lost its process lease".into(),
            ));
        }
        let current = self.shuffle_recovery_generation()?.unwrap_or(0);
        if current == committed {
            return Ok(());
        }
        let assignment_active = self
            .shuffle_receiver
            .lock()
            .as_ref()
            .is_some_and(|receiver| receiver.assignment_version() != 0)
            || self
                .shuffle_sender
                .lock()
                .as_ref()
                .is_some_and(|sender| sender.assignment_version() != 0);
        let exact_terminal_participant = terminal.as_ref().is_some_and(|release| {
            controller.recovery_round_requires_current_process_stop(&release.round)
        });
        if current != 0 || assignment_active || exact_terminal_participant {
            return Err(DbError::Checkpoint(format!(
                "startup shuffle recovery generation {current} conflicts with committed generation {committed}"
            )));
        }
        self.set_shuffle_recovery_gen(committed);
        let installed = self.shuffle_recovery_generation()?.unwrap_or(committed);
        if installed != committed {
            return Err(DbError::Checkpoint(format!(
                "startup shuffle recovery generation {installed} does not match committed generation {committed}"
            )));
        }
        if !controller.process_lease_is_live() {
            return Err(DbError::Checkpoint(
                "cluster recovery generation bootstrap lost its process lease".into(),
            ));
        }
        if committed != 0 {
            let epoch = terminal.as_ref().and_then(|release| match release.phase {
                laminar_core::cluster::control::RecoverPhase::ReleaseCommitted { epoch } => {
                    Some(epoch)
                }
                _ => None,
            });
            tracing::info!(
                generation = committed,
                ?epoch,
                "restored committed shuffle recovery generation"
            );
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn shuffle_recovery_generation(&self) -> Result<Option<u64>, DbError> {
        let receiver = self.shuffle_receiver.lock().clone();
        let sender = self.shuffle_sender.lock().clone();
        let receiver_generation = receiver.as_ref().map(|receiver| receiver.recovery_gen());
        let sender_generation = sender.as_ref().map(|sender| sender.recovery_gen());
        if let (Some(receiver), Some(sender)) = (receiver_generation, sender_generation) {
            if receiver != sender {
                return Err(DbError::Checkpoint(format!(
                    "shuffle endpoints disagree on recovery generation: receiver {receiver}, sender {sender}"
                )));
            }
        }
        Ok(receiver_generation.or(sender_generation))
    }

    /// Resolve only the cumulative shuffle-loss cutoff captured when this exact generation
    /// started. This must succeed before publishing local `Release` readiness.
    #[cfg(feature = "cluster")]
    pub(crate) fn complete_shuffle_recovery(&self, gen: u64) -> bool {
        self.shuffle_receiver
            .lock()
            .as_ref()
            .is_none_or(|receiver| receiver.complete_recovery(gen))
    }

    /// Start the database-owned per-node recovery supervisor once. Coordinated recovery is the only cluster
    /// fault path — a local-only restart rewinds one node while peers advance, an
    /// inconsistent cut.
    ///
    /// # Errors
    ///
    /// Returns an error if the database-owned control runtime cannot be initialized.
    #[cfg(feature = "cluster")]
    pub fn enable_coordinated_recovery(self: &Arc<Self>) -> Result<(), DbError> {
        if !self.is_cluster_runtime() || self.cluster_controller.lock().is_none() {
            return Err(DbError::Config(
                "coordinated recovery requires a cluster runtime and controller".into(),
            ));
        }
        let runtime = self.control_runtime.handle()?;
        let mut owned = self.recovery_monitor.lock();
        if owned.as_ref().is_some_and(|monitor| !monitor.is_finished()) {
            return Ok(());
        }
        if owned.take().is_some() {
            tracing::warn!("replacing an unexpectedly terminated coordinated recovery supervisor");
        }
        *owned = Some(crate::coordinated_recovery::spawn_monitor(self, &runtime));
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn quiesce_recovery_monitor_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let Some(mut monitor) = self.recovery_monitor.lock().take() else {
            return Ok(());
        };
        match tokio::time::timeout_at(deadline, &mut monitor).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(DbError::Pipeline(format!(
                "coordinated recovery supervisor failed during shutdown: {error}"
            ))),
            Err(_) => {
                *self.recovery_monitor.lock() = Some(monitor);
                Err(DbError::Pipeline(
                    "coordinated recovery supervisor did not quiesce before the shutdown deadline"
                        .into(),
                ))
            }
        }
    }

    /// Close resources created by an unsuccessful `start_inner` attempt.
    async fn cleanup_failed_start(&self) -> Result<(), DbError> {
        const CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
        self.quiesce_committer_until(deadline).await?;
        self.quiesce_checkpoint_decision_until(deadline).await?;
        {
            let mut coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
                .await
                .map_err(|_| {
                    DbError::Checkpoint(
                        "failed-start cleanup could not reacquire checkpoint coordinator ownership; durability fences remain held"
                            .into(),
                    )
            })?;
            if let Some(coordinator) = coordinator.as_mut() {
                let expected_sinks = coordinator.committable_sink_names()?;
                coordinator
                    .audit_sink_open_witness_topology(expected_sinks)
                    .await?;
                tokio::time::timeout_at(deadline, coordinator.reconcile_prepared_on_init())
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "failed-start checkpoint reconciliation exceeded {CLEANUP_TIMEOUT:?}; durability fences remain held"
                        ))
                    })??;
                coordinator
                    .reconcile_sink_open_witness_until(deadline)
                    .await?;
                coordinator.clear_sinks()?;
            }
        }
        *self.control_tx.lock() = None;
        *self.force_ckpt_tx.lock() = None;
        self.quiesce_connector_generation_until(deadline).await?;
        *self.checkpoint_namespace_lock.lock() = None;
        Ok(())
    }

    async fn await_startup_attempt_until(
        &self,
        attempt: &StartupAttempt,
        deadline: tokio::time::Instant,
        operation: &str,
    ) -> Result<(), DbError> {
        match tokio::time::timeout_at(deadline, attempt.wait()).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                tracing::debug!(%error, %operation, "startup reached a failed terminal state");
                Ok(())
            }
            Err(_) => Err(DbError::Pipeline(format!(
                "{operation} could not observe terminal startup before its deadline; startup remains fenced"
            ))),
        }
    }

    async fn quiesce_committer_until(&self, deadline: tokio::time::Instant) -> Result<(), DbError> {
        let mut owned = tokio::time::timeout_at(deadline, self.committer_handle.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "teardown could not acquire coordinated-committer ownership; deployment fences remain held"
                        .into(),
                )
            })?;
        let Some(handle) = owned.as_mut() else {
            return Ok(());
        };
        handle.abort();
        match tokio::time::timeout_at(deadline, handle).await {
            Ok(_) => {
                owned.take();
                Ok(())
            }
            Err(_) => Err(DbError::Checkpoint(
                "coordinated committer did not terminate before the teardown deadline; deployment fences remain held"
                    .into(),
            )),
        }
    }

    async fn quiesce_connector_generation_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let (sources, sinks, startup) = tokio::join!(
            self.quiesce_owned_source_tasks_until(deadline),
            self.quiesce_owned_sink_handles_until(deadline),
            self.quiesce_owned_connector_task_fences_until(deadline),
        );
        let mut failures = Vec::new();
        if let Err(error) = sources {
            failures.push(format!("sources: {error}"));
        }
        if let Err(error) = sinks {
            failures.push(format!("sinks: {error}"));
        }
        if let Err(error) = startup {
            failures.push(format!("startup: {error}"));
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DbError::Connector(format!(
                "connector generation remains fenced: {}",
                failures.join("; ")
            )))
        }
    }

    async fn quiesce_owned_connector_task_fences_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let fences = {
            let mut owned = self.owned_connector_task_fences.lock();
            owned.retain(|fence| !fence.is_finished());
            owned.clone()
        };
        if fences.is_empty() {
            return Ok(());
        }

        futures::future::join_all(fences.iter().map(|fence| fence.wait_until(deadline))).await;
        let unresolved_names = {
            let mut owned = self.owned_connector_task_fences.lock();
            owned.retain(|fence| !fence.is_finished());
            owned
                .iter()
                .map(|fence| fence.name().to_owned())
                .collect::<Vec<_>>()
        };
        if unresolved_names.is_empty() {
            Ok(())
        } else {
            Err(DbError::Connector(format!(
                "cannot replace a pipeline while pre-actor connector tasks remain unresolved: {}",
                unresolved_names.join(", ")
            )))
        }
    }

    async fn quiesce_owned_source_tasks_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let tasks = self.owned_source_tasks.lock().clone();
        if tasks.is_empty() {
            return Ok(());
        }

        // Signal every task before awaiting any one task. Aborting retires the owned connector
        // generation; its lease remains fenced until the stable supervisor observes task exit.
        for task in &tasks {
            task.request_shutdown();
            task.abort();
        }
        let completions =
            futures::future::join_all(tasks.iter().map(|task| task.wait_until(deadline))).await;
        for (task, finished) in tasks.iter().zip(completions) {
            if finished {
                task.log_terminal_outcome();
            }
        }

        let unresolved_names = {
            let mut owned = self.owned_source_tasks.lock();
            owned.retain(|task| !task.is_finished());
            owned
                .iter()
                .map(|task| task.name().to_owned())
                .collect::<Vec<_>>()
        };
        if unresolved_names.is_empty() {
            return Ok(());
        }
        Err(DbError::Connector(format!(
            "cannot replace a pipeline while prior source tasks remain unresolved: {}",
            unresolved_names.join(", ")
        )))
    }

    async fn quiesce_owned_sink_handles_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let handles = {
            let mut owned = self.owned_sink_handles.lock();
            owned.retain(crate::sink_task::SinkTaskHandle::has_unresolved_task);
            owned.clone()
        };
        if handles.is_empty() {
            return Ok(());
        }
        if deadline <= tokio::time::Instant::now() {
            return Err(DbError::Connector(
                "sink-generation quiescence budget was exhausted before terminal cleanup began; \
                 prior actors remain fenced"
                    .into(),
            ));
        }

        // Poll every close in the same turn so independent actors share one restart budget. Each
        // close has its own wrapper at the shared deadline: one slow actor cannot discard an
        // already-published sticky failure from another actor. Cancellation leaves the DB-owned
        // handles in place and any close that crossed admission continues in its stable driver.
        let close_results =
            futures::future::join_all(handles.iter().cloned().map(|handle| async move {
                let name = handle.name().to_owned();
                let result = tokio::time::timeout_at(deadline, handle.close()).await;
                (name, result)
            }))
            .await;
        let mut failures = close_results
            .into_iter()
            .filter_map(|(name, result)| match result {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(format!("{name}: {error}")),
                Err(_) => Some(format!(
                    "{name}: shared sink-generation close deadline expired"
                )),
            })
            .collect::<Vec<_>>();

        // A close result is not a terminal proof: timeout, disconnection, or a panicked close
        // driver can publish immediately while the actor or a connector child remains live.
        // Spend the remainder of the same generation deadline observing both proofs before the
        // registry decides whether replacement is safe.
        futures::future::join_all(
            handles
                .iter()
                .map(|handle| handle.wait_terminal_until(deadline)),
        )
        .await;

        let unresolved_names = {
            let mut owned = self.owned_sink_handles.lock();
            owned.retain(crate::sink_task::SinkTaskHandle::has_unresolved_task);
            owned
                .iter()
                .map(|handle| handle.name().to_owned())
                .collect::<Vec<_>>()
        };
        if unresolved_names.is_empty() {
            for failure in failures {
                tracing::warn!(%failure, "terminal sink cleanup reported an error");
            }
            return Ok(());
        }
        failures.push(format!("still active: {}", unresolved_names.join(", ")));
        Err(DbError::Connector(format!(
            "cannot replace a pipeline while prior sink actors remain unresolved: {}",
            failures.join("; ")
        )))
    }

    /// Publish `Running` only if the compute watcher did not fault during startup.
    ///
    /// The compute thread publishes a competing `Starting -> Faulted` transition before exit and
    /// the watcher reinforces it. Ignoring a lost CAS here would let coordinated recovery
    /// acknowledge a process whose compute loop has already died.
    fn finish_start_transition(&self) -> Result<(), DbError> {
        // Hold the generation read lock through publication. Every cancellation path takes the
        // write lock, so cancellation and `Starting -> Running` have one linearization order.
        let runtime_shutdown = self.runtime_shutdown.read();
        if self.is_closed() || runtime_shutdown.is_cancelled() {
            return match DbState::compare_exchange(
                DbState::Starting,
                DbState::Created,
                &self.state,
            ) {
                Ok(_) | Err(DbState::Created) => Err(DbError::Shutdown),
                Err(DbState::Faulted) => Err(DbError::Pipeline(format!(
                    "pipeline faulted while its cancelled generation was leaving startup: {}",
                    self.last_fault()
                        .unwrap_or_else(|| "compute loop exited without a fault reason".into())
                ))),
                Err(observed) => Err(DbError::InvalidOperation(format!(
                    "cancelled pipeline startup completed from an unexpected lifecycle state: {observed:?}"
                ))),
            };
        }
        match DbState::compare_exchange(DbState::Starting, DbState::Running, &self.state) {
            Ok(_) => Ok(()),
            Err(DbState::Faulted) => Err(DbError::Pipeline(format!(
                "pipeline faulted while entering the runtime control loop: {}",
                self.last_fault()
                    .unwrap_or_else(|| "compute loop exited without a fault reason".into())
            ))),
            Err(observed) => Err(DbError::InvalidOperation(format!(
                "pipeline startup completed from an unexpected lifecycle state: {observed:?}"
            ))),
        }
    }

    /// Start the streaming pipeline. Idempotent if already running. On failure
    /// (or recovering from `Faulted`) it rebuilds from the surviving catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline cannot be started.
    pub async fn start(self: &Arc<Self>) -> Result<(), DbError> {
        self.start_with_lifecycle_authority(PipelineLifecycleAuthority::Public)
            .await
    }

    /// Recovery-owned restart after an exact stopped quorum. The persistent recovery lifecycle
    /// fence rejects public starts until the round's committed release, while this path alone may
    /// rebuild the still-gated data plane for `Start`.
    #[cfg(feature = "cluster")]
    pub(crate) async fn start_for_coordinated_recovery(self: &Arc<Self>) -> Result<(), DbError> {
        self.start_with_lifecycle_authority(PipelineLifecycleAuthority::CoordinatedRecovery)
            .await
    }

    async fn start_with_lifecycle_authority(
        self: &Arc<Self>,
        authority: PipelineLifecycleAuthority,
    ) -> Result<(), DbError> {
        if self.is_closed() {
            return Err(DbError::Shutdown);
        }
        self.ensure_catalog_cleanup_unfenced("pipeline start")?;
        #[cfg(feature = "cluster")]
        self.ensure_pipeline_lifecycle_authorized(authority, "start")?;
        #[cfg(not(feature = "cluster"))]
        Self::ensure_pipeline_lifecycle_authorized(authority, "start");
        self.connector_registry.freeze();
        let runtime = self.control_runtime.handle()?;
        let attempt = {
            let mut owned = self.startup_attempt.lock();
            #[cfg(feature = "cluster")]
            self.ensure_pipeline_lifecycle_authorized(authority, "start")?;
            #[cfg(not(feature = "cluster"))]
            Self::ensure_pipeline_lifecycle_authorized(authority, "start");
            loop {
                // Cleanup may publish Created/Faulted just before the owner publishes its sticky
                // result. The registered incomplete attempt remains authoritative through that
                // narrow interval; never overlap it with a replacement generation.
                if let Some(in_flight) = owned.as_ref().filter(|attempt| !attempt.is_complete()) {
                    break Arc::clone(in_flight);
                }
                match DbState::load(&self.state) {
                    DbState::Running => return Ok(()),
                    DbState::Starting => {
                        break owned.clone().ok_or_else(|| {
                            DbError::Pipeline(
                                "pipeline is Starting without an owned startup attempt; restart is fenced"
                                    .into(),
                            )
                        })?;
                    }
                    DbState::Stopped => {
                        return Err(DbError::InvalidOperation(
                            "Cannot start a stopped pipeline. Create a new LaminarDB instance."
                                .into(),
                        ));
                    }
                    DbState::ShuttingDown => {
                        return Err(DbError::InvalidOperation(
                            "cannot start pipeline: shutdown/stop in progress".into(),
                        ));
                    }
                    claimed @ (DbState::Created | DbState::Faulted) => {
                        // A compute fault publishes the cluster recovery fence before Faulted.
                        // Re-read that fence after observing the state so a public restart cannot
                        // slip through the fence-before-state publication window.
                        #[cfg(feature = "cluster")]
                        self.ensure_pipeline_lifecycle_authorized(authority, "start")?;
                        #[cfg(not(feature = "cluster"))]
                        Self::ensure_pipeline_lifecycle_authorized(authority, "start");
                        let attempt = Arc::new(StartupAttempt::new());
                        // Publish ownership before Starting so stop/shutdown can always find the
                        // exact attempt they must await.
                        *owned = Some(Arc::clone(&attempt));
                        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
                        let db = Arc::clone(self);
                        let driver_attempt = Arc::clone(&attempt);
                        let emergency_attempt = Arc::clone(&attempt);
                        let driver_runtime = runtime.clone();
                        let startup_thread = match std::thread::Builder::new()
                            .name("laminar-start".into())
                            .spawn(move || {
                                if !matches!(start_rx.recv(), Ok(true)) {
                                    return;
                                }
                                let result =
                                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                        driver_runtime.block_on(db.clone().drive_start_attempt(
                                            driver_attempt,
                                            claimed == DbState::Faulted,
                                        ));
                                    }));
                                if result.is_err() && !emergency_attempt.is_complete() {
                                    let message =
                                        "startup owner thread panicked before terminal cleanup";
                                    db.last_fault.lock().get_or_insert(message.into());
                                    DbState::Faulted.store(&db.state);
                                    emergency_attempt
                                        .complete(Err(DbError::Pipeline(message.into())));
                                }
                            }) {
                            Ok(thread) => thread,
                            Err(error) => {
                                *owned = None;
                                return Err(DbError::Pipeline(format!(
                                    "failed to spawn startup owner thread: {error}"
                                )));
                            }
                        };
                        // The attempt is the durable owner; the short-lived OS thread is detached.
                        drop(startup_thread);
                        if DbState::compare_exchange(claimed, DbState::Starting, &self.state)
                            .is_err()
                        {
                            let _ = start_tx.send(false);
                            *owned = None;
                            continue;
                        }
                        if start_tx.send(true).is_err() {
                            let message = "startup owner thread exited before accepting ownership";
                            self.last_fault.lock().get_or_insert(message.into());
                            DbState::Faulted.store(&self.state);
                            attempt.complete(Err(DbError::Pipeline(message.into())));
                        }
                        break attempt;
                    }
                }
            }
        };
        attempt.wait().await
    }

    async fn drive_start_attempt(
        self: Arc<Self>,
        attempt: Arc<StartupAttempt>,
        starting_from_fault: bool,
    ) {
        let terminal = StartupDriverGuard::new(&self, Arc::clone(&attempt));
        let result =
            std::panic::AssertUnwindSafe(Box::pin(self.run_claimed_start(starting_from_fault)))
                .catch_unwind()
                .await;
        let result = match result {
            Ok(result) => result,
            Err(panic) => {
                let reason = format!("startup driver panicked: {}", panic_message(panic.as_ref()));
                *self.last_fault.lock() = Some(reason.clone());
                let cleanup = std::panic::AssertUnwindSafe(self.cleanup_failed_start())
                    .catch_unwind()
                    .await;
                DbState::Faulted.store(&self.state);
                match cleanup {
                    Ok(Ok(())) => Err(DbError::Pipeline(reason)),
                    Ok(Err(error)) => Err(DbError::Pipeline(format!(
                        "{reason}; failed-start cleanup remains fenced: {error}"
                    ))),
                    Err(cleanup_panic) => Err(DbError::Pipeline(format!(
                        "{reason}; failed-start cleanup panicked: {}",
                        panic_message(cleanup_panic.as_ref())
                    ))),
                }
            }
        };
        terminal.finish(result);
    }

    async fn run_claimed_start(&self, starting_from_fault: bool) -> Result<(), DbError> {
        const FAULT_RESTART_QUIESCE_TIMEOUT: std::time::Duration =
            std::time::Duration::from_secs(10);
        let _topology = self.topology_ddl_lock.write().await;
        let _lifecycle = self.lifecycle_lock.lock().await;
        self.ensure_catalog_cleanup_unfenced("pipeline start")?;
        if DbState::load(&self.state) != DbState::Starting {
            return Err(DbError::Pipeline(
                "startup ownership was superseded before the driver entered the lifecycle".into(),
            ));
        }

        let generation_quiesce_deadline =
            tokio::time::Instant::now() + FAULT_RESTART_QUIESCE_TIMEOUT;
        if let Err(error) = self
            .quiesce_committer_until(generation_quiesce_deadline)
            .await
        {
            if starting_from_fault {
                DbState::Faulted.store(&self.state);
            } else {
                DbState::Created.store(&self.state);
            }
            return Err(error);
        }
        if let Err(error) = self
            .quiesce_connector_generation_until(generation_quiesce_deadline)
            .await
        {
            if starting_from_fault {
                DbState::Faulted.store(&self.state);
            } else {
                DbState::Created.store(&self.state);
            }
            return Err(error);
        }

        if starting_from_fault {
            let deadline = tokio::time::Instant::now() + FAULT_RESTART_QUIESCE_TIMEOUT;
            if let Err(error) = self.quiesce_checkpoint_decision_until(deadline).await {
                // Retain the old coordinator and deployment fence. A supervisor/manual retry may
                // resume once the owned decision write reaches a terminal state.
                DbState::Faulted.store(&self.state);
                return Err(error);
            }
        }

        // Clear on entry, not after start_inner — otherwise a panic during this
        // startup (watcher → Faulted + reason) would be immediately overwritten.
        *self.last_fault.lock() = None;
        {
            let mut guard = self.engine_metrics.lock();
            if guard.is_none() {
                *guard = Some(Arc::new(crate::engine_metrics::EngineMetrics::new(
                    &prometheus::Registry::new(),
                )));
            }
        }

        #[cfg(feature = "cluster")]
        if let Err(error) = self.restore_catalog_from_manifest().await {
            if let Err(cleanup_error) =
                self.ensure_catalog_cleanup_unfenced("catalog bootstrap rollback")
            {
                // `CatalogBootstrapGuard` has already tried to remove every replayed object.
                // An incomplete rollback is a terminal per-instance fence: never turn it into a
                // retryable startup failure by publishing `Created` over the guard's `Faulted`.
                DbState::Faulted.store(&self.state);
                return Err(DbError::Pipeline(format!(
                    "{error}; catalog bootstrap rollback remains terminally fenced: {cleanup_error}"
                )));
            }

            // No runtime resources have been constructed and catalog rollback completed. Publish
            // a retryable state only if no concurrent fault superseded this startup generation.
            return match DbState::compare_exchange(DbState::Starting, DbState::Created, &self.state)
            {
                Ok(_) => Err(error),
                Err(observed) => Err(DbError::Pipeline(format!(
                    "{error}; catalog bootstrap rollback completed but startup was superseded by \
                     lifecycle state {observed:?}: {}",
                    self.last_fault()
                        .unwrap_or_else(|| "no fault reason was recorded".into())
                ))),
            };
        }

        // Drain a shutdown permit a prior fault's `notify_one()` left with no
        // waiter, so the new coordinator's `notified()` doesn't fire at once.
        tokio::select! {
            biased;
            () = self.shutdown_signal.notified() => {}
            () = std::future::ready(()) => {}
        }

        match self.start_inner().await {
            Ok(()) => {
                // CAS, not store: don't clobber a Faulted set by the watcher if the compute thread
                // already panicked during startup. Losing that CAS is a failed start, not success.
                match self.finish_start_transition() {
                    Ok(()) => Ok(()),
                    Err(error) => match self.cleanup_failed_start().await {
                        Ok(()) => Err(error),
                        Err(cleanup_error) => {
                            DbState::Faulted.store(&self.state);
                            Err(DbError::Pipeline(format!(
                                "{error}; failed-start cleanup remains fenced: {cleanup_error}"
                            )))
                        }
                    },
                }
            }
            Err(e) => {
                match self.cleanup_failed_start().await {
                    Ok(()) => {
                        // Reset so a retry re-runs startup rather than silently returning Ok.
                        DbState::Created.store(&self.state);
                        Err(e)
                    }
                    Err(cleanup_error) => {
                        DbState::Faulted.store(&self.state);
                        Err(DbError::Pipeline(format!(
                            "{e}; failed-start cleanup remains fenced: {cleanup_error}"
                        )))
                    }
                }
            }
        }
    }

    fn validate_startup_durability(
        &self,
        startup_runtime: RuntimeMode,
    ) -> Result<
        (
            Option<Arc<dyn object_store::ObjectStore>>,
            StateBackendDurability,
        ),
        DbError,
    > {
        #[cfg(feature = "cluster")]
        if startup_runtime == RuntimeMode::Cluster
            && (!self.mv_registry.lock().is_empty() || !self.mv_store.read().is_empty())
        {
            return Err(DbError::InvalidOperation(format!(
                "[{}] cluster startup found materialized state without a planner-certified distribution and assignment-fenced checkpoint/read lifecycle",
                laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
            )));
        }

        // A renewable lease can identify the current leader, but the object-store
        // decision CAS and external sink transaction do not atomically consume its
        // term/token. Until those writes are term-fenced end to end, admitting
        // cluster EO would allow an expired leader to finalize a checkpoint.
        // Keep this duplicate of the builder admission because tests and embedders
        // can construct a database through lower-level paths.
        if startup_runtime == RuntimeMode::Cluster
            && self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
        {
            return Err(DbError::Config(
                "[LDB-0013] cluster exactly-once is not admitted: checkpoint decisions are \
                 term-fenced, but supported connectors do not yet provide a certified \
                 term-fenced source handoff and external sink cursor commit. Use cluster \
                 at_least_once, or exactly_once in embedded/single-node mode"
                    .into(),
            ));
        }
        #[cfg(feature = "cluster")]
        let has_injected_decision_store = self.decision_store.lock().is_some();
        #[cfg(not(feature = "cluster"))]
        let has_injected_decision_store = false;
        if startup_runtime == RuntimeMode::Local
            && self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
            && (self
                .config
                .object_store_url
                .as_deref()
                .is_some_and(|url| !url.starts_with("file://"))
                || has_injected_decision_store)
        {
            return Err(DbError::Config(
                "[LDB-0014] a local replay-capable deployment with a shared cloud checkpoint \
                 namespace or injected decision store is not admitted until its writer lease is \
                 term-fenced. Use a built-in or file:// local checkpoint directory, or \
                 best_effort delivery"
                    .into(),
            ));
        }

        #[cfg(feature = "cluster")]
        let injected_cluster_checkpoint_store = self.cluster_checkpoint_object_store();
        #[cfg(not(feature = "cluster"))]
        let injected_cluster_checkpoint_store: Option<Arc<dyn object_store::ObjectStore>> = None;

        if self.config.checkpoint.is_some()
            && self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
        {
            // Without an object-store URL the checkpoint store is a local directory and thus
            // survives a same-node process restart. Explicit URLs are classified fail-closed;
            // notably memory:// cannot own source acknowledgements under a replay guarantee.
            let checkpoint_scope = if injected_cluster_checkpoint_store.is_some() {
                StateBackendDurability::ClusterShared
            } else {
                match self.config.object_store_url.as_deref() {
                    Some(url) if url.starts_with("file://") => StateBackendDurability::NodeDurable,
                    Some(url) => StateBackendDurability::for_storage_url(url),
                    None => StateBackendDurability::NodeDurable,
                }
            };
            let required = required_recovery_scope(startup_runtime);
            if !checkpoint_scope.satisfies(required) {
                return Err(DbError::Config(format!(
                    "[LDB-5036] {startup_runtime:?} {:?} delivery requires {required:?} \
                     checkpoint/decision storage, but the configured checkpoint store is \
                     {checkpoint_scope:?}; use the built-in checkpoint data_dir for \
                     node-local recovery, or a supported shared object store",
                    self.config.delivery_guarantee
                )));
            }
        }

        let state_backend_scope = self
            .state_backend
            .lock()
            .as_ref()
            .map_or(StateBackendDurability::Volatile, |backend| {
                backend.durability_scope()
            });
        match startup_runtime {
            // A cluster peer must be able to recover a failed node's vnodes.
            RuntimeMode::Cluster
                if !state_backend_scope.satisfies(StateBackendDurability::ClusterShared) =>
            {
                return Err(DbError::Config(format!(
                    "[LDB-0011] cluster mode requires ClusterShared state so a peer can \
                         recover a failed node's vnodes; the configured backend is \
                         {state_backend_scope:?}. Use shared cloud storage (s3://, gs://, or \
                         az://); in-process, local paths, and file:// storage are not \
                         cluster-shared."
                )));
            }
            // Local exact recovery may use the same node's durable filesystem.
            RuntimeMode::Local
                if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
                    && !state_backend_scope.satisfies(StateBackendDurability::NodeDurable) =>
            {
                return Err(DbError::Config(format!(
                    "[LDB-5035] local exactly-once delivery requires NodeDurable state, but the \
                     configured backend is {state_backend_scope:?}; configure a local path, \
                     file:// URL, or shared cloud object store"
                )));
            }
            _ => {}
        }
        Ok((injected_cluster_checkpoint_store, state_backend_scope))
    }

    async fn initialize_checkpointing(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
        startup_runtime: RuntimeMode,
        injected_cluster_checkpoint_store: Option<Arc<dyn object_store::ObjectStore>>,
        state_backend_scope: StateBackendDurability,
    ) -> Result<Option<laminar_core::checkpoint::PipelineIdentity>, DbError> {
        #[cfg(not(feature = "cluster"))]
        let _ = state_backend_scope;

        let participant = self.checkpoint_participant();
        let bound_pipeline_identity =
            if self.config.checkpoint.is_some() || startup_runtime == RuntimeMode::Cluster {
                let identity_registrations = crate::pipeline_identity::PipelineRegistrations::new(
                    source_regs.values(),
                    sink_regs.values(),
                    stream_regs.values(),
                    table_regs.values(),
                );
                let identity_context = crate::pipeline_identity::PipelineIdentityContext::new(
                    &self.config,
                    &self.catalog,
                    &self.connector_registry,
                    identity_registrations,
                    self.checkpoint_key_groups().get(),
                    participant.is_some(),
                );
                Some(crate::pipeline_identity::compute(&identity_context)?)
            } else {
                None
            };
        if let Some(ref cp_config) = self.config.checkpoint {
            use crate::checkpoint_coordinator::{
                CheckpointConfig as CkpConfig, CheckpointCoordinator,
            };

            let max_retained = cp_config.max_retained.unwrap_or(3);
            if max_retained == 0 {
                return Err(DbError::Config(
                    "checkpoint.max_retained must be greater than zero".into(),
                ));
            }
            let max_staged_bytes = cp_config.max_staged_bytes.ok_or_else(|| {
                DbError::Config(
                    "checkpoint.max_staged_bytes was not resolved at construction".into(),
                )
            })?;
            if cp_config.interval_ms == Some(0) {
                return Err(DbError::Config(
                    "checkpoint.interval_ms must be greater than zero; use None for manual-only"
                        .into(),
                ));
            }
            if cp_config.timeout_ms == Some(0) {
                return Err(DbError::Config(
                    "checkpoint.timeout_ms must be greater than zero".into(),
                ));
            }
            let key_group_count = self.checkpoint_key_groups();

            let data_dir = cp_config
                .data_dir
                .clone()
                .or_else(|| self.config.storage_dir.clone())
                .unwrap_or_else(|| std::path::PathBuf::from("./data"));
            let explicit_file_checkpoint_root = self
                .config
                .object_store_url
                .as_deref()
                .filter(|url| url.starts_with("file://"))
                .map(|url| {
                    laminar_core::storage::object_store_builder::file_url_path(url)
                        .map_err(|error| DbError::Config(format!("object store: {error}")))
                })
                .transpose()?;
            let local_checkpoint_root = explicit_file_checkpoint_root.as_ref().unwrap_or(&data_dir);
            let uses_local_checkpoint_store = injected_cluster_checkpoint_store.is_none()
                && (self.config.object_store_url.is_none()
                    || explicit_file_checkpoint_root.is_some());
            if startup_runtime == RuntimeMode::Local
                && uses_local_checkpoint_store
                && self.checkpoint_namespace_lock.lock().is_none()
            {
                laminar_core::durable_fs::ensure_durable_directory(local_checkpoint_root).map_err(
                    |error| {
                        DbError::Config(format!(
                            "create local checkpoint directory {}: {error}",
                            local_checkpoint_root.display()
                        ))
                    },
                )?;
                let lock_path = local_checkpoint_root.join(".laminardb-checkpoint.lock");
                let lock = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&lock_path)
                    .map_err(|error| {
                        DbError::Config(format!(
                            "[LDB-0014] open checkpoint namespace lock {}: {error}",
                            lock_path.display()
                        ))
                    })?;
                lock.try_lock().map_err(|error| {
                    DbError::Config(format!(
                        "[LDB-0014] checkpoint namespace {} is already owned by \
                         another live process: {error}",
                        local_checkpoint_root.display()
                    ))
                })?;
                *self.checkpoint_namespace_lock.lock() = Some(lock);
            }
            let participant_id = participant.unwrap_or(0);
            let pipeline_identity = bound_pipeline_identity.clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "checkpoint startup did not derive the pipeline identity".into(),
                )
            })?;

            let (store, decision_backing): (
                Box<dyn laminar_core::storage::CheckpointStore>,
                Option<Arc<dyn object_store::ObjectStore>>,
            ) = if let Some(obj) = injected_cluster_checkpoint_store.as_ref() {
                let cs = laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    Arc::clone(obj),
                    participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
                )
                .with_max_state_data_bytes(max_staged_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), Some(Arc::clone(obj)))
            } else if let Some(file_root) = explicit_file_checkpoint_root.as_ref() {
                laminar_core::durable_fs::ensure_durable_directory(file_root).map_err(|error| {
                    DbError::Config(format!(
                        "create file checkpoint directory {}: {error}",
                        file_root.display()
                    ))
                })?;
                let checkpoint_dir = participant.map_or(file_root.clone(), |id| {
                    file_root.join("nodes").join(id.to_string())
                });
                let cs = laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    checkpoint_dir,
                )
                .with_max_state_data_bytes(max_staged_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), None)
            } else if let Some(ref url) = self.config.object_store_url {
                let obj = laminar_core::storage::object_store_builder::build_object_store(
                    url,
                    &self.config.object_store_options,
                )
                .map_err(|e| DbError::Config(format!("object store: {e}")))?;
                let cs = laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    Arc::clone(&obj),
                    participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
                )
                .with_max_state_data_bytes(max_staged_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), Some(obj))
            } else {
                laminar_core::durable_fs::ensure_durable_directory(&data_dir).map_err(|e| {
                    DbError::Config(format!("data dir {}: {e}", data_dir.display()))
                })?;
                let checkpoint_dir = participant.map_or(data_dir.clone(), |id| {
                    data_dir.join("nodes").join(id.to_string())
                });
                let cs = laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    checkpoint_dir,
                )
                .with_max_state_data_bytes(max_staged_bytes)?
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), None)
            };

            let defaults = CkpConfig::default();
            let config = CkpConfig {
                max_retained,
                checkpoint_timeout: cp_config.timeout_ms.map_or(
                    defaults.checkpoint_timeout,
                    std::time::Duration::from_millis,
                ),
                max_staged_bytes,
                ..defaults
            };
            let mut coord = CheckpointCoordinator::new(config, store).await?;
            #[cfg(feature = "cluster")]
            if startup_runtime == RuntimeMode::Cluster
                && state_backend_scope.satisfies(StateBackendDurability::ClusterShared)
            {
                let delta_chain_bound = cluster_delta_chain_bound(max_retained);
                coord.configure_state_ancestry(delta_chain_bound);
            }
            coord.bind_pipeline_identity(pipeline_identity.clone())?;
            if let Some(ref prom) = *self.engine_metrics.lock() {
                coord.set_metrics(Arc::clone(prom));
            }

            #[cfg(feature = "cluster")]
            if let Some(controller) = self.cluster_controller.lock().clone() {
                if coord.participant_id() != controller.instance_id().0 {
                    return Err(DbError::Config(format!(
                        "[LDB-0012] checkpoint store participant {} does not match cluster \
                         instance {}",
                        coord.participant_id(),
                        controller.instance_id().0
                    )));
                }
                coord.set_cluster_controller(controller);
            }

            let ds = {
                #[cfg(feature = "cluster")]
                {
                    if let Some(injected) = self.decision_store.lock().clone() {
                        injected
                    } else if let Some(backing) = decision_backing.as_ref() {
                        Arc::new(
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                                Arc::clone(backing),
                            ),
                        )
                    } else {
                        Arc::new(
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::local_filesystem(
                                local_checkpoint_root,
                            )
                            .map_err(|error| {
                                DbError::Config(format!(
                                    "open durable local checkpoint metadata store: {error}"
                                ))
                            })?,
                        )
                    }
                }
                #[cfg(not(feature = "cluster"))]
                {
                    if let Some(backing) = decision_backing.as_ref() {
                        Arc::new(
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                                Arc::clone(backing),
                            ),
                        )
                    } else {
                        Arc::new(
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::local_filesystem(
                                local_checkpoint_root,
                            )
                            .map_err(|error| {
                                DbError::Config(format!(
                                    "open durable local checkpoint metadata store: {error}"
                                ))
                            })?,
                        )
                    }
                }
            };
            let deployment_id = ds.load_or_create_deployment_id().await.map_err(|error| {
                DbError::Checkpoint(format!(
                    "load/create durable deployment identity before checkpoint startup: {error}"
                ))
            })?;
            coord.set_decision_store(ds)?;
            coord.bind_deployment_id(deployment_id.clone())?;

            let state_backend = self.state_backend.lock().clone();
            let vnode_registry = self.vnode_registry.lock().clone();
            if let (Some(backend), Some(registry)) = (state_backend, vnode_registry) {
                backend
                    .bind_state_namespace(&deployment_id, &pipeline_identity)
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "bind state backend namespace before recovery: {error}"
                        ))
                    })?;
                let owner = {
                    #[cfg(feature = "cluster")]
                    {
                        self.cluster_controller
                            .lock()
                            .as_ref()
                            .map_or(laminar_core::state::NodeId(0), |c| {
                                laminar_core::state::NodeId(c.instance_id().0)
                            })
                    }
                    #[cfg(not(feature = "cluster"))]
                    {
                        laminar_core::state::NodeId(0)
                    }
                };
                let version = registry.assignment_version();
                backend.set_authoritative_version(version);
                coord.set_state_backend(backend)?;
                coord.set_assignment_version(version);
                coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, owner));
                coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
            }

            *self.coordinator.lock().await = Some(coord);
        }
        Ok(bound_pipeline_identity)
    }
    async fn start_inner(&self) -> Result<(), DbError> {
        let runtime_shutdown = tokio_util::sync::CancellationToken::new();
        *self.runtime_shutdown.write() = runtime_shutdown.clone();
        if self.is_closed() {
            runtime_shutdown.cancel();
            return Err(DbError::Shutdown);
        }

        let (source_regs, sink_regs, stream_regs, table_regs, has_external) = {
            let mgr = self.connector_manager.lock();
            (
                mgr.sources().clone(),
                mgr.sinks().clone(),
                mgr.streams().clone(),
                mgr.tables().clone(),
                mgr.has_external_connectors(),
            )
        };

        for (name, reg) in &source_regs {
            tracing::debug!(source = %name, connector_type = ?reg.connector_type, "Registered source");
        }
        for (name, reg) in &sink_regs {
            tracing::debug!(sink = %name, connector_type = ?reg.connector_type, "Registered sink");
        }

        let startup_runtime = self.runtime_mode();

        let (injected_cluster_checkpoint_store, state_backend_scope) =
            self.validate_startup_durability(startup_runtime)?;

        let pipeline_identity = self
            .initialize_checkpointing(
                &source_regs,
                &sink_regs,
                &stream_regs,
                &table_regs,
                startup_runtime,
                injected_cluster_checkpoint_store,
                state_backend_scope,
            )
            .await?;

        #[cfg(feature = "cluster")]
        if startup_runtime == RuntimeMode::Cluster {
            let prepared_witnesses = {
                let coordinator = self.coordinator.lock().await;
                match coordinator.as_ref() {
                    Some(coordinator) => coordinator.prepared_checkpoint_witnesses().await?,
                    None => Vec::new(),
                }
            };
            if !prepared_witnesses.is_empty() {
                let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                    DbError::Checkpoint(
                        "cluster Prepared recovery requires an installed controller".into(),
                    )
                })?;
                self.fence_coordinated_recovery_lifecycle();
                controller.set_recovering(true);
                crate::coordinated_recovery::request_local_fault(
                    &controller,
                    &self.pending_recovery_fault,
                )
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "could not request coordinated recovery for Prepared checkpoint inventory: {error}"
                        ))
                    })?;
                tracing::warn!(
                    prepared_attempts = prepared_witnesses.len(),
                    "connector startup deferred until coordinated recovery settles Prepared checkpoints"
                );
                return Ok(());
            }
        }

        if has_external || !stream_regs.is_empty() {
            tracing::info!(
                sources = source_regs.len(),
                sinks = sink_regs.len(),
                streams = stream_regs.len(),
                tables = table_regs.len(),
                has_external,
                "Starting pipeline"
            );
            self.start_connector_pipeline(
                source_regs,
                sink_regs,
                stream_regs,
                table_regs,
                has_external,
                pipeline_identity,
                runtime_shutdown,
            )
            .await?;
        } else {
            tracing::info!(
                sources = source_regs.len(),
                sinks = sink_regs.len(),
                "Starting in embedded (in-memory) mode — no streams"
            );
        }

        Ok(())
    }

    pub(crate) async fn revalidate_persisted_cluster_query_shapes(
        &self,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
    ) -> Result<bool, DbError> {
        let mut streams: Vec<_> = stream_regs.values().collect();
        streams.sort_by(|left, right| left.name.cmp(&right.name));
        let mut has_ownership_partitioned_state = false;
        for stream in streams {
            let plan = crate::ddl::PlannedStreamingQuery {
                emit_clause: stream.emit_clause.clone(),
                window_config: stream.window_config.clone(),
                order_config: stream.order_config.clone(),
                join_config: stream.join_config.clone(),
                has_analytic: stream.has_analytic,
                has_frame: stream.has_frame,
            };
            has_ownership_partitioned_state |= self
                .validate_cluster_query_shape(
                    "persisted stream",
                    &stream.name,
                    &stream.query_sql,
                    &plan,
                )
                .await?;
        }
        Ok(has_ownership_partitioned_state)
    }

    fn build_connector_operator_graph(
        &self,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
        pipeline_identity: Option<&laminar_core::checkpoint::PipelineIdentity>,
    ) -> Result<crate::operator_graph::OperatorGraph, DbError> {
        use crate::operator_graph::OperatorGraph;

        #[cfg(not(feature = "cluster"))]
        let _ = pipeline_identity;
        let ctx = {
            use datafusion::execution::SessionStateBuilder;
            let mut session_config = laminar_sql::datafusion::base_session_config();
            if let Some(n) = self.pipeline_target_partitions {
                session_config = session_config.with_target_partitions(n);
            }
            let query_planner = Arc::clone(self.ctx.state().query_planner());
            let mut state_builder = SessionStateBuilder::new()
                .with_config(session_config)
                .with_default_features()
                .with_query_planner(query_planner);
            for rule in self.physical_optimizer_rules.iter() {
                state_builder = state_builder.with_physical_optimizer_rule(Arc::clone(rule));
            }
            let context =
                datafusion::prelude::SessionContext::new_with_state(state_builder.build());
            for rule in self.ctx.state().optimizers() {
                context.add_optimizer_rule(Arc::clone(rule));
            }
            context
        };
        laminar_sql::register_streaming_functions(&ctx);

        let lookup_tables: Vec<(String, arrow::datatypes::SchemaRef)> = {
            let ts = self.table_store.read();
            ts.table_names()
                .into_iter()
                .filter_map(|name| {
                    let schema = ts.table_schema(&name)?;
                    Some((name, schema))
                })
                .collect()
        };
        // Record only tables that actually registered into `ctx`, so the graph's reference-table
        // set can't name a table the DataFusion context is missing (enrich detection would then
        // build SQL against a non-existent table).
        let mut reference_table_names = rustc_hash::FxHashSet::default();
        for (name, schema) in lookup_tables {
            let provider = crate::table_provider::ReferenceTableProvider::new(
                name.clone(),
                schema,
                self.table_store.clone(),
            );
            if let Err(e) = ctx.register_table(exact_table_reference(&name), Arc::new(provider)) {
                tracing::warn!(
                    table = %name,
                    error = %e,
                    "failed to register lookup table in operator graph context"
                );
            } else {
                reference_table_names.insert(name);
            }
        }

        let mut graph = OperatorGraph::new(ctx);
        graph.set_lookup_registry(Arc::clone(&self.lookup_registry));
        graph.set_reference_tables(reference_table_names);
        if let Some(ref prom) = *self.engine_metrics.lock() {
            graph.set_metrics(Arc::clone(prom));
        }
        if let (Some(runtime), Some(handle)) = (&self.ai_runtime, &self.ai_handle) {
            graph.set_ai_runtime(Arc::clone(runtime), handle.clone());
        }

        #[cfg(feature = "cluster")]
        {
            let sender = self.shuffle_sender.lock().clone();
            let receiver = self.shuffle_receiver.lock().clone();
            let registry = self.vnode_registry.lock().clone();
            let controller = self.cluster_controller.lock().clone();
            if let (Some(sender), Some(receiver), Some(registry), Some(controller)) =
                (sender, receiver, registry, controller)
            {
                let self_id = laminar_core::state::NodeId(controller.instance_id().0);
                graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
                    registry,
                    sender,
                    receiver,
                    self_id,
                });
                let pipeline_identity = pipeline_identity.cloned().ok_or_else(|| {
                    DbError::Checkpoint(
                        "[LDB-6051] cluster graph has no bound pipeline identity".into(),
                    )
                })?;
                graph.set_pipeline_identity(pipeline_identity);
                graph.set_pending_vnode_transition_handle(Arc::clone(
                    &self.pending_vnode_transition,
                ));
                graph.set_installed_vnode_state_handle(Arc::clone(&self.installed_vnode_state));
                graph.set_rotation_execution_fence(Arc::clone(&self.rotation_execution_fence));
                // With a durable backend, per-vnode partials are the authoritative agg checkpoint;
                // the whole-node manifest copy is one node's slices and traps boot recovery.
                let has_shared_state = self.state_backend.lock().as_ref().is_some_and(|backend| {
                    backend
                        .durability_scope()
                        .satisfies(StateBackendDurability::ClusterShared)
                });
                if has_shared_state {
                    graph.set_vnode_partials_authoritative();
                    tracing::info!(
                        "cluster agg: per-vnode partials authoritative (no manifest copy)"
                    );
                    if let Some(chain_bound) = self
                        .config
                        .checkpoint
                        .as_ref()
                        .and_then(|cp| cluster_delta_chain_bound(cp.max_retained.unwrap_or(3)))
                    {
                        graph.set_delta_chain_bound(chain_bound);
                        tracing::info!(
                            delta_chain_bound = chain_bound,
                            "bounded incremental vnode checkpoints enabled"
                        );
                    }
                }
            }
        }

        // The connector manager contains only externally configured sources. Plain SQL-created
        // sources are bridged directly from the catalog, but managed operators must still plan
        // against their schemas before source connectors and checkpoint recovery are built.
        for name in self.catalog.list_sources() {
            if let Some(entry) = self.catalog.get_source(&name) {
                graph.register_source_schema(name, entry.schema.clone());
            }
        }

        let partial_lookup_tables: rustc_hash::FxHashMap<String, Vec<String>> = table_regs
            .values()
            .filter(|r| r.on_demand)
            .filter_map(|r| {
                let schema = self.table_store.read().table_schema(&r.name)?;
                let cols = schema.fields().iter().map(|f| f.name().clone()).collect();
                Some((r.name.clone(), cols))
            })
            .collect();
        graph.set_partial_lookup_tables(partial_lookup_tables);
        graph.set_runtime_handle(
            self.ai_handle
                .clone()
                .unwrap_or_else(tokio::runtime::Handle::current),
        );

        // Seed incremental MVs up front so a `changelog ⋈ static dim` consumer detects its source
        // regardless of the (HashMap-ordered) build loop below.
        graph.set_incremental_tables(
            stream_regs
                .values()
                .filter(|r| r.incremental)
                .map(|r| r.name.clone())
                .collect(),
        );

        let mut ordered_streams: Vec<_> = stream_regs.values().collect();
        ordered_streams.sort_by(|left, right| left.name.cmp(&right.name));
        for reg in ordered_streams {
            graph.add_query(
                reg.name.clone(),
                reg.query_sql.clone(),
                reg.emit_clause.clone(),
                reg.window_config.clone(),
                reg.order_config.clone(),
                reg.join_config.clone(),
                reg.incremental,
            );
        }
        graph.take_build_errors()?;

        for tcfg in graph.temporal_join_configs() {
            if self.lookup_registry.get_entry(&tcfg.table_name).is_none() {
                let initial_batch = self
                    .table_store
                    .read()
                    .to_record_batch(&tcfg.table_name)?
                    .or_else(|| {
                        self.catalog
                            .get_source(&tcfg.table_name)
                            .map(|e| RecordBatch::new_empty(e.schema.clone()))
                    })
                    .unwrap_or_else(|| {
                        RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()))
                    });
                let key_columns = vec![tcfg.table_key_column.clone()];
                let key_indices: Vec<usize> = key_columns
                    .iter()
                    .filter_map(|k| initial_batch.schema().index_of(k).ok())
                    .collect();

                // If the AS OF clause didn't resolve a version column, pick the first
                // timestamp/int column that isn't the join key.
                let resolved_version_col = if tcfg.table_version_column.is_empty() {
                    let schema = initial_batch.schema();
                    schema
                        .fields()
                        .iter()
                        .find(|f| {
                            f.name() != &tcfg.table_key_column
                                && matches!(
                                    f.data_type(),
                                    arrow::datatypes::DataType::Int64
                                        | arrow::datatypes::DataType::Timestamp(_, _)
                                )
                        })
                        .map(|f| f.name().clone())
                        .unwrap_or_default()
                } else {
                    tcfg.table_version_column.clone()
                };

                let Ok(version_col_idx) = initial_batch.schema().index_of(&resolved_version_col)
                else {
                    if !initial_batch.schema().fields().is_empty() {
                        tracing::warn!(
                            table=%tcfg.table_name,
                            version_col=%resolved_version_col,
                            "Version column not found in temporal table schema; \
                             will resolve on first CDC batch"
                        );
                    }
                    // Index built on first CDC update.
                    self.lookup_registry.register_versioned(
                        &tcfg.table_name,
                        laminar_sql::datafusion::VersionedLookupState {
                            batch: initial_batch,
                            index: Arc::new(
                                laminar_sql::datafusion::lookup_join_exec::VersionedIndex::default(
                                ),
                            ),
                            key_columns,
                            version_column: resolved_version_col,
                            stream_time_column: tcfg.stream_time_column.clone(),
                            max_versions_per_key: usize::MAX,
                        },
                    );
                    continue;
                };
                let index = Arc::new(
                    laminar_sql::datafusion::lookup_join_exec::VersionedIndex::build(
                        &initial_batch,
                        &key_indices,
                        version_col_idx,
                        usize::MAX,
                    )
                    .unwrap_or_default(),
                );
                self.lookup_registry.register_versioned(
                    &tcfg.table_name,
                    laminar_sql::datafusion::VersionedLookupState {
                        batch: initial_batch,
                        index,
                        key_columns,
                        version_column: resolved_version_col,
                        stream_time_column: tcfg.stream_time_column.clone(),
                        max_versions_per_key: usize::MAX,
                    },
                );
            }
        }
        Ok(graph)
    }

    fn build_pipeline_sources(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        checkpointing_enabled: bool,
        runtime_mode: RuntimeMode,
        prom_registry: Option<&Arc<prometheus::Registry>>,
    ) -> Result<Vec<TrackedSourceRegistration>, DbError> {
        use crate::connector_manager::build_source_config;
        use crate::pipeline::SourceRegistration;
        use laminar_connectors::connector::SourceConnector as _;
        let mut sources: Vec<TrackedSourceRegistration> = Vec::new();
        for (name, reg) in source_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_source_config(reg)?;

            if let Some(entry) = self.catalog.get_source(name) {
                let schema_str = crate::pipeline_callback::encode_arrow_schema(&entry.schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }

            let source = self
                .connector_registry
                .create_source(&config, prom_registry)
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create source '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            #[cfg(feature = "cluster")]
            let mut source = source;
            let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                Arc::<str>::from(format!("source:{name}")),
                source.terminal_task_tracker(),
                &self.owned_connector_task_fences,
            );
            let contract = source.contract(&config).map_err(|e| {
                DbError::Config(format!(
                    "source '{name}' (type '{}') has an invalid contract: {e}",
                    config.connector_type()
                ))
            })?;
            admit_source_contract(
                contract,
                self.config.delivery_guarantee,
                checkpointing_enabled,
                runtime_mode,
            )
            .map_err(|reason| {
                DbError::Config(format!(
                    "source '{name}' is not admissible in {runtime_mode:?} mode with {} delivery: \
                     {reason} (contract: {contract:?})",
                    self.config.delivery_guarantee
                ))
            })?;
            let assignment_scoped = cfg!(feature = "cluster")
                && runtime_mode == RuntimeMode::Cluster
                && contract.topology == SourceTopology::Splittable;
            #[cfg(feature = "cluster")]
            if assignment_scoped {
                let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
                    DbError::Config(format!("cluster source '{name}' has no vnode registry"))
                })?;
                let self_id = self
                    .cluster_controller
                    .lock()
                    .as_ref()
                    .map(|controller| laminar_core::state::NodeId(controller.instance_id().0))
                    .ok_or_else(|| {
                        DbError::Config(format!(
                            "cluster source '{name}' has no cluster controller identity"
                        ))
                    })?;
                source
                    .set_vnode_assignment(name, registry, self_id)
                    .map_err(|error| {
                        DbError::Config(format!(
                            "source '{name}' rejected cluster vnode assignment: {error}"
                        ))
                    })?;
            }
            sources.push(TrackedSourceRegistration::from_captured(
                SourceRegistration {
                    name: name.clone(),
                    connector: source,
                    config,
                    contract,
                    assignment_scoped,
                    position: laminar_connectors::connector::SourcePosition::Initial,
                },
                task_fence,
            ));
        }

        let bridged_names: rustc_hash::FxHashSet<String> =
            sources.iter().map(|s| s.name.clone()).collect();
        for (name, reg) in source_regs {
            if reg.connector_type.is_some() {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(name) {
                let subscription = entry.sink.subscribe();
                let connector = crate::catalog_connector::CatalogSourceConnector::new(
                    subscription,
                    entry.schema.clone(),
                    entry.data_notify(),
                );
                let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                    Arc::<str>::from(format!("source:{name}")),
                    connector.terminal_task_tracker(),
                    &self.owned_connector_task_fences,
                );
                let config = laminar_connectors::config::ConnectorConfig::new("catalog-bridge");
                let contract = connector.contract(&config).map_err(|e| {
                    DbError::Config(format!("source '{name}' has an invalid contract: {e}"))
                })?;
                admit_source_contract(
                    contract,
                    self.config.delivery_guarantee,
                    checkpointing_enabled,
                    runtime_mode,
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "source '{name}' is not admissible in {runtime_mode:?} mode with {} \
                         delivery: {reason} (contract: {contract:?})",
                        self.config.delivery_guarantee
                    ))
                })?;
                sources.push(TrackedSourceRegistration::from_captured(
                    SourceRegistration {
                        name: name.clone(),
                        connector: Box::new(connector),
                        config,
                        contract,
                        assignment_scoped: false,
                        position: laminar_connectors::connector::SourcePosition::Initial,
                    },
                    task_fence,
                ));
            }
        }
        for name in self.catalog.list_sources() {
            if bridged_names.contains(&name) || source_regs.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                let subscription = entry.sink.subscribe();
                let connector = crate::catalog_connector::CatalogSourceConnector::new(
                    subscription,
                    entry.schema.clone(),
                    entry.data_notify(),
                );
                let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                    Arc::<str>::from(format!("source:{name}")),
                    connector.terminal_task_tracker(),
                    &self.owned_connector_task_fences,
                );
                let config = laminar_connectors::config::ConnectorConfig::new("catalog-bridge");
                let contract = connector.contract(&config).map_err(|e| {
                    DbError::Config(format!("source '{name}' has an invalid contract: {e}"))
                })?;
                admit_source_contract(
                    contract,
                    self.config.delivery_guarantee,
                    checkpointing_enabled,
                    runtime_mode,
                )
                .map_err(|reason| {
                    DbError::Config(format!(
                        "source '{name}' is not admissible in {runtime_mode:?} mode with {} \
                         delivery: {reason} (contract: {contract:?})",
                        self.config.delivery_guarantee
                    ))
                })?;
                sources.push(TrackedSourceRegistration::from_captured(
                    SourceRegistration {
                        name: name.clone(),
                        connector: Box::new(connector),
                        config,
                        contract,
                        assignment_scoped: false,
                        position: laminar_connectors::connector::SourcePosition::Initial,
                    },
                    task_fence,
                ));
            }
        }
        Ok(sources)
    }
    async fn prepare_pipeline_sinks(
        &self,
        sources: &[TrackedSourceRegistration],
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        stream_output_schemas: &HashMap<String, arrow_schema::SchemaRef>,
        runtime_mode: RuntimeMode,
        checkpointing_enabled: bool,
        pipeline_checkpoint_timeout: std::time::Duration,
        prom_registry: Option<&Arc<prometheus::Registry>>,
    ) -> Result<PipelineSinkSetup, DbError> {
        use crate::connector_manager::build_sink_config;
        let (sink_event_tx, sink_event_rx) =
            laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(
                crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
            );
        // Names whose output carries a Z-set changelog: incremental MVs, plus any stream/view whose
        // query reads a changelog-carrying name (a projection/filter forwards the changelog). A
        // non-capable sink over one of these silently drops retractions, so it's rejected below.
        let changelog_carrying: rustc_hash::FxHashSet<String> = {
            let mut set: rustc_hash::FxHashSet<String> = stream_regs
                .iter()
                .filter(|(_, r)| r.incremental)
                .map(|(n, _)| n.clone())
                .collect();
            loop {
                let mut added = false;
                for (name, reg) in stream_regs {
                    if !set.contains(name)
                        && crate::sql_analysis::extract_table_references(&reg.query_sql)
                            .iter()
                            .any(|t| set.contains(t.as_str()))
                    {
                        set.insert(name.clone());
                        added = true;
                    }
                }
                if !added {
                    break;
                }
            }
            set
        };

        let mut prepared_sinks = Vec::new();
        for (name, reg) in sink_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_sink_config(reg, self.config.delivery_guarantee)?;
            let upstream_schema = stream_output_schemas.get(&reg.input).cloned().or_else(|| {
                self.catalog
                    .get_source(&reg.input)
                    .map(|e| e.schema.clone())
            });
            if let Some(schema) = upstream_schema {
                let schema_str = crate::pipeline_callback::encode_arrow_schema(&schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }
            let sink = self
                .connector_registry
                .create_sink(&config, prom_registry)
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create sink '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
            let task_fence = ConnectorTaskFenceRegistration::capture_registered(
                Arc::<str>::from(format!("sink:{name}")),
                sink.terminal_task_tracker(),
                &self.owned_connector_task_fences,
            );

            let carries_changelog = changelog_carrying.contains(&reg.input);
            let state_backend_scope = self
                .state_backend
                .lock()
                .as_ref()
                .map_or(StateBackendDurability::Volatile, |backend| {
                    backend.durability_scope()
                });
            let (contract, configured_timeout) = admit_sink(
                sink.as_ref(),
                SinkAdmissionContext {
                    config: &config,
                    name,
                    input: &reg.input,
                    delivery: self.config.delivery_guarantee,
                    runtime: runtime_mode,
                    carries_changelog,
                    checkpointing_enabled,
                    state_backend_scope,
                },
            )?;
            let write_timeout = configured_timeout.map_or(
                sink.suggested_write_timeout(),
                std::time::Duration::from_millis,
            );
            if write_timeout.is_zero() {
                return Err(DbError::Connector(format!(
                    "sink '{name}': write_timeout must be > 0 \
                     (check 'sink.write.timeout.ms' or the sink's \
                     suggested_write_timeout)"
                )));
            }
            let flush_interval = sink.flush_interval();
            if flush_interval.is_zero() {
                return Err(DbError::Connector(format!(
                    "sink '{name}': flush_interval must be > 0"
                )));
            }
            prepared_sinks.push(PreparedSink {
                name: name.clone(),
                connector: sink,
                config,
                filter_expr: reg.filter_expr.clone(),
                input: reg.input.clone(),
                contract,
                write_timeout,
                flush_interval,
                requires_recovery_on_error: contract.is_checkpoint_committable()
                    || self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
                    || runtime_mode == RuntimeMode::Cluster,
                task_fence,
            });
        }

        #[cfg(feature = "cluster")]
        let callback_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let sink_process_authority = if runtime_mode == RuntimeMode::Cluster {
            let controller = callback_controller.clone().ok_or_else(|| {
                DbError::Config(
                    "cluster sink runtime requires a cluster controller with process lease authority"
                        .into(),
                )
            })?;
            if controller.process_lease_deadline().is_none() {
                return Err(DbError::Config(
                    "cluster sink runtime requires one shared process lease deadline before open"
                        .into(),
                ));
            }
            Some(controller)
        } else {
            None
        };

        let expected_committable_sinks = prepared_sinks
            .iter()
            .filter(|sink| sink.contract.is_checkpoint_committable())
            .map(|sink| sink.name.clone())
            .collect();
        {
            let coordinator = self.coordinator.lock().await;
            if let Some(coordinator) = coordinator.as_ref() {
                coordinator
                    .audit_sink_open_witness_topology(expected_committable_sinks)
                    .await?;
            }
        }

        // Opening is one atomic startup stage: a slow connector consumes the remaining shared
        // checkpoint-derived budget rather than receiving a fresh timeout of its own. Cluster
        // opens use the exact authority later installed in the actor and callback.
        open_prepared_sinks(
            &mut prepared_sinks,
            pipeline_checkpoint_timeout,
            #[cfg(feature = "cluster")]
            sink_process_authority.as_deref(),
        )
        .await?;

        let mut sinks: Vec<(
            String,
            crate::sink_task::SinkTaskHandle,
            Option<String>,
            String, // input stream name (FROM clause target)
            SinkContract,
        )> = Vec::with_capacity(prepared_sinks.len());
        for prepared in prepared_sinks {
            let PreparedSink {
                name,
                connector,
                filter_expr,
                input,
                contract,
                write_timeout,
                flush_interval,
                requires_recovery_on_error,
                task_fence,
                config: _,
            } = prepared;
            let terminal_tasks = task_fence.tracker();
            let sink_id: std::sync::Arc<str> = std::sync::Arc::from(name.as_str());
            let handle =
                crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
                    name: name.clone(),
                    sink_id,
                    connector,
                    contract,
                    requires_recovery_on_error,
                    channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
                    flush_interval,
                    write_timeout,
                    event_tx: sink_event_tx.clone(),
                    terminal_tasks,
                    #[cfg(feature = "cluster")]
                    process_authority: sink_process_authority.clone(),
                });
            {
                let mut owned = self.owned_sink_handles.lock();
                debug_assert!(!owned.iter().any(|known| known.same_actor(&handle)));
                owned.push(handle.clone());
            }
            sinks.push((name, handle, filter_expr, input, contract));
            task_fence.handoff();
        }
        drop(sink_event_tx);

        let (coordinated_committer, committer_poll, committer_notify) = {
            let mut guard = self.coordinator.lock().await;
            if let Some(coord) = guard.as_mut() {
                coord.set_assignment_scoped_sources(
                    sources
                        .iter()
                        .filter(|source| source.assignment_scoped)
                        .map(|source| source.name.clone()),
                );
                for (name, handle, _, _, _) in &sinks {
                    coord.register_sink(name.clone(), handle.clone());
                }
                (
                    coord.coordinated_committer()?,
                    crate::checkpoint_coordinator::CheckpointCoordinator::committer_poll_interval(),
                    coord.committer_notify(),
                )
            } else {
                (
                    None,
                    std::time::Duration::from_secs(1),
                    Arc::new(tokio::sync::Notify::new()),
                )
            }
        };

        #[cfg(feature = "cluster")]
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                // Stopped-quorum recovery records Abort before recovery-owned Start. Reconcile
                // that terminal decision before selecting the coordinated cut.
                if runtime_mode == RuntimeMode::Cluster {
                    coord.reconcile_prepared_on_init().await?;
                }
            }
        }

        Ok(PipelineSinkSetup {
            sinks,
            sink_event_rx,
            coordinated_committer,
            committer_poll,
            committer_notify,
            #[cfg(feature = "cluster")]
            callback_controller,
        })
    }
    fn restore_reference_table_checkpoint(
        &self,
        recovered: &crate::recovery_manager::RecoveredState,
    ) -> Result<bool, DbError> {
        let checkpoint = recovered
            .manifest
            .operator_states
            .get(crate::table_store::REFERENCE_TABLE_CHECKPOINT_KEY);
        let has_reference_tables = !self.table_store.read().table_names().is_empty();

        match (has_reference_tables, checkpoint) {
            (true, Some(state)) => {
                let bytes = state.decode_inline().ok_or_else(|| {
                    DbError::Checkpoint(
                        "reference-table checkpoint is not inline after sidecar resolution".into(),
                    )
                })?;
                let restored = self.table_store.write().restore_checkpoint(&bytes)?;
                if !restored {
                    return Err(DbError::Checkpoint(
                        "reference-table checkpoint did not cover the complete catalog".into(),
                    ));
                }
                Ok(true)
            }
            (true, None) => Err(DbError::Checkpoint(format!(
                "recovered checkpoint {} has no atomic reference-table state",
                recovered.manifest.checkpoint_id
            ))),
            (false, Some(_)) => Err(DbError::Checkpoint(
                "recovered checkpoint contains reference-table state but the catalog has no tables"
                    .into(),
            )),
            (false, None) => Ok(false),
        }
    }

    async fn recover_pipeline_state(
        &self,
        mut graph: crate::operator_graph::OperatorGraph,
        sources: &mut [TrackedSourceRegistration],
        runtime_mode: RuntimeMode,
        vnode_state_report_timeout: std::time::Duration,
    ) -> Result<PipelineRecoveryState, DbError> {
        #[cfg(not(feature = "cluster"))]
        let _ = vnode_state_report_timeout;
        #[cfg(feature = "cluster")]
        if runtime_mode == RuntimeMode::Cluster {
            // A new graph generation has no installed vnode state until fresh initialization or
            // exact-cut callbacks complete. A stopped/faulted graph must never lend its marker to
            // the replacement generation. Startup holds assignment_adoption_lock, so publish the
            // durable withdrawal before clearing the marker; otherwise a remote rotation could
            // consume a stale true report during recovery preparation.
            let registry = self.vnode_registry.lock().clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "cluster pipeline recovery has no vnode registry for readiness withdrawal"
                        .into(),
                )
            })?;
            let assignment = registry.versioned_snapshot();
            if assignment.version() != 0 {
                let controller = self.cluster_controller.lock().clone().ok_or_else(|| {
                    DbError::Checkpoint(
                        "cluster pipeline recovery has no controller for readiness withdrawal"
                            .into(),
                    )
                })?;
                let deadline = tokio::time::Instant::now() + vnode_state_report_timeout;
                tokio::time::timeout_at(
                    deadline,
                    self.publish_local_vnode_state_report(&controller, &assignment, false),
                )
                .await
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "assignment {} vnode-state readiness withdrawal timed out before pipeline recovery",
                        assignment.version()
                    ))
                })??;
            }
            self.installed_vnode_state.lock().take();
        }
        // Must run BEFORE begin_initial_epoch so the epoch reflects the recovered state.
        // Hoist watermarks now so generators are seeded before watermark-state construction;
        // without this, generators restart at i64::MIN while offsets resume mid-stream.
        let mut recovered_mv_store = self.mv_store.read().fresh_image()?;
        let mut recovered_source_wms: rustc_hash::FxHashMap<String, i64> =
            rustc_hash::FxHashMap::default();
        let mut recovered_watermark_frontier = None;
        #[cfg(feature = "cluster")]
        let mut recovered_all_sources_idle = false;
        #[cfg(not(feature = "cluster"))]
        let recovered_all_sources_idle = false;
        // A generation with no coordinator is also a fresh recovery image; reset any watermark
        // retained by the previous in-process generation before accepting new rows.
        let mut source_watermark_recovery_selected = true;
        #[cfg(feature = "cluster")]
        let mut startup_reconciled_source_handoff_version = None;
        let mut restored_reference_tables = false;
        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                // Restore to the cluster-agreed epoch if one was armed, else the local
                // latest. Take it owned first so the guard isn't held across the await.
                #[cfg(feature = "cluster")]
                let recover_target = self.recover_target_epoch.lock().take();
                #[cfg(feature = "cluster")]
                let recovery = match recover_target {
                    Some(target) => coord.recover_to_epoch(target).await,
                    None => coord.recover().await,
                };
                #[cfg(not(feature = "cluster"))]
                let recovery = coord.recover().await;
                // Local RecoveryManager first closes every valid, outcome-less Prepared witness
                // with a create-once Abort. Reconciliation can then safely finalize or roll back
                // the exact terminal winner without maintaining a second settlement protocol.
                if runtime_mode == RuntimeMode::Local && recovery.is_ok() {
                    coord.reconcile_prepared_on_init().await?;
                }
                #[cfg(feature = "cluster")]
                {
                    *self.last_recovery_epoch.lock() = match &recovery {
                        Ok(Some(recovered)) => Some(recovered.epoch()),
                        _ => None,
                    };
                    // A genesis rewind has no durable engine cursor. Keep every source's atomic
                    // startup request at Initial; the connector applies its configured initial
                    // policy as part of start rather than becoming active and then rewinding.
                    if recover_target == Some(0) && matches!(&recovery, Ok(None)) {
                        for src in sources.iter_mut() {
                            src.position = laminar_connectors::connector::SourcePosition::Initial;
                        }
                        tracing::info!("genesis rewind: sources will start at initial position");
                    }
                }
                match recovery {
                    Ok(Some(recovered)) => {
                        #[cfg(feature = "cluster")]
                        let recovered_assignment = if runtime_mode == RuntimeMode::Cluster {
                            let capsule = recovered.cluster_capsule().ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "[LDB-6041] cluster recovery selected checkpoint {} without its recovery capsule",
                                    recovered.manifest.checkpoint_id
                                ))
                            })?;
                            Some(
                                std::num::NonZeroU64::new(
                                    capsule.assignment_fence.assignment_version,
                                )
                                .ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "[LDB-6055] recovered cluster assignment fence is zero"
                                            .into(),
                                    )
                                })?,
                            )
                        } else {
                            None
                        };
                        #[cfg(not(feature = "cluster"))]
                        let recovered_assignment = None;
                        for source in sources.iter() {
                            validate_source_recovery_assignment(
                                &source.name,
                                source.assignment_scoped,
                                recovered.manifest.source_offsets.get(&source.name),
                                recovered_assignment,
                            )?;
                        }

                        source_watermark_recovery_selected = true;
                        recovered_watermark_frontier = recovered.manifest.watermark;
                        #[cfg(feature = "cluster")]
                        {
                            recovered_all_sources_idle =
                                recovered.cluster_capsule().is_some_and(|capsule| {
                                    capsule.cluster_watermark
                                        == laminar_core::checkpoint::CheckpointWatermark::Idle
                                });
                        }
                        recovered_source_wms = recovered
                            .manifest
                            .source_watermarks
                            .iter()
                            .filter(|(_, &wm)| wm != i64::MIN)
                            .map(|(name, &wm)| (name.clone(), wm))
                            .collect();
                        let recovered_attempt = laminar_core::state::CheckpointAttempt::new(
                            recovered.manifest.epoch,
                            recovered.manifest.checkpoint_id,
                        );
                        for src in sources.iter_mut() {
                            if !src.contract.supports_replay() {
                                continue;
                            }
                            let manifest_cp = recovered.manifest.source_offsets.get(&src.name);
                            let restored = manifest_cp
                                .map(crate::checkpoint_coordinator::connector_to_source_checkpoint);
                            if let Some(restored) = restored {
                                tracing::info!(
                                    source = %src.name,
                                    "attaching checkpoint offsets for source recovery"
                                );
                                src.position =
                                    laminar_connectors::connector::SourcePosition::Resume {
                                        attempt: recovered_attempt,
                                        checkpoint: restored,
                                    };
                            } else {
                                return Err(DbError::Checkpoint(format!(
                                    "[LDB-6042] recovered checkpoint {} has no restorable \
                                     offset for replayable source '{}'",
                                    recovered.manifest.checkpoint_id, src.name
                                )));
                            }
                        }
                        let op_keys: Vec<&String> =
                            recovered.manifest.operator_states.keys().collect();
                        let instance_hint = {
                            #[cfg(feature = "cluster")]
                            {
                                self.cluster_controller
                                    .lock()
                                    .as_ref()
                                    .map_or(0, |c| c.instance_id().0)
                            }
                            #[cfg(not(feature = "cluster"))]
                            {
                                0u64
                            }
                        };
                        tracing::info!(
                            instance = instance_hint,
                            count = op_keys.len(),
                            keys = ?op_keys,
                            "manifest operator_states summary"
                        );
                        restored_reference_tables =
                            self.restore_reference_table_checkpoint(&recovered)?;
                        if let Some(op) = recovered.manifest.operator_states.get("operator_graph") {
                            if let Some(bytes) = op.decode_inline() {
                                match graph.restore_from_bytes(&bytes) {
                                    Ok((restored_graph, n)) => {
                                        graph = restored_graph;
                                        tracing::info!(
                                            queries = n,
                                            "Restored operator graph state from checkpoint"
                                        );
                                    }
                                    Err(e) => {
                                        // Source offsets are already staged; resuming with
                                        // empty operator state would silently lose in-flight
                                        // windows. Fail loud so the intent to start fresh
                                        // must be explicit.
                                        return Err(DbError::Checkpoint(format!(
                                            "[LDB-6029] operator graph restore failed: \
                                             {e} — refusing to start with checkpointed \
                                             source offsets and empty operator state"
                                        )));
                                    }
                                }
                            } else {
                                return Err(DbError::Checkpoint(
                                    "[LDB-6029] operator graph checkpoint is not inline after \
                                     sidecar resolution"
                                        .to_string(),
                                ));
                            }
                        } else if recovered
                            .manifest
                            .operator_states
                            .contains_key("stream_executor")
                        {
                            return Err(DbError::Checkpoint(
                                "[LDB-6029] legacy stream_executor checkpoint is unsupported; \
                                 explicit checkpoint reset is required"
                                    .to_string(),
                            ));
                        }

                        let mut mv_states = HashMap::new();
                        for (key, state) in &recovered.manifest.operator_states {
                            let Some(name) =
                                key.strip_prefix(crate::mv_store::CHECKPOINT_KEY_PREFIX)
                            else {
                                continue;
                            };
                            let bytes = state.try_decode_inline().map_err(|error| {
                                DbError::Checkpoint(format!(
                                    "MV checkpoint '{name}' is corrupt: {error}"
                                ))
                            })?;
                            let bytes = bytes.ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "MV checkpoint '{name}' is not inline after sidecar resolution"
                                ))
                            })?;
                            mv_states.insert(name.to_string(), bytes);
                        }
                        recovered_mv_store = {
                            let live = self.mv_store.read();
                            live.recovery_image(&mv_states)
                        }?;

                        // Rebuild aggregates from each boot-owned vnode's chain at the recovered
                        // cut — the same epoch the source offsets resume from.
                        #[cfg(feature = "cluster")]
                        if runtime_mode == RuntimeMode::Cluster {
                            let recovered_assignment_version = recovered_assignment
                                .ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "[LDB-6031] cluster checkpoint has no assignment version"
                                            .into(),
                                    )
                                })?
                                .get();
                            let reconciled_source_handoff_version = self
                                .validate_boot_vnode_recovery_target(
                                    recovered_attempt,
                                    recovered_assignment_version,
                                )?;
                            let restore_cut = recovered.vnode_restore_cut().ok_or_else(|| {
                                DbError::Checkpoint(format!(
                                    "[LDB-6041] recovered checkpoint {} has no validated vnode-state seal",
                                    recovered.manifest.checkpoint_id
                                ))
                            })?;
                            self.prepare_boot_vnode_restore_transition(restore_cut)
                                .await?;
                            startup_reconciled_source_handoff_version =
                                reconciled_source_handoff_version;
                        }

                        if !mv_states.is_empty() {
                            tracing::info!(
                                mvs = mv_states.len(),
                                "Restored MV state from checkpoint"
                            );
                        }
                        tracing::info!(
                            checkpoint_id = recovered.manifest.checkpoint_id,
                            epoch = recovered.epoch(),
                            "Recovered from unified checkpoint"
                        );
                    }
                    Ok(None) => {
                        #[cfg(feature = "cluster")]
                        if runtime_mode == RuntimeMode::Cluster {
                            self.validate_fresh_cluster_vnode_start()?;
                        }
                        source_watermark_recovery_selected = true;
                        tracing::info!("No checkpoint found, starting fresh");
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        #[cfg(feature = "cluster")]
        if runtime_mode == RuntimeMode::Cluster && self.coordinator.lock().await.is_none() {
            self.validate_fresh_cluster_vnode_start()?;
        }

        Ok(PipelineRecoveryState {
            graph,
            recovered_mv_store,
            recovered_source_wms,
            recovered_watermark_frontier,
            recovered_all_sources_idle,
            source_watermark_recovery_selected,
            #[cfg(feature = "cluster")]
            reconciled_source_handoff_version: startup_reconciled_source_handoff_version,
            restored_reference_tables,
        })
    }
    async fn initialize_reference_tables(
        &self,
        table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        restored_reference_tables: bool,
    ) -> Result<(), DbError> {
        use crate::connector_manager::build_table_config;
        let table_sources = create_reference_table_sources(
            &self.connector_registry,
            table_regs,
            &self.table_store,
            restored_reference_tables,
        )
        .await?;
        let mut tables_to_publish = if restored_reference_tables {
            self.table_store.read().table_names()
        } else {
            hydrate_reference_table_sources(table_sources, &self.table_store).await?
        };

        {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                // Recovery and Prepared-manifest reconciliation completed before epoch admission.
                coord.reconcile_sink_open_witness().await?;
                coord.begin_initial_epoch().await?;
            }
        }

        tables_to_publish.sort_unstable();
        tables_to_publish.dedup();
        for name in tables_to_publish {
            self.sync_table_to_datafusion(&name)?;
            // Setup built the Versioned (temporal-join) state before the
            // snapshot existed; rebuild it over the snapshot now instead of
            // downgrading it to a plain Snapshot.
            let entry = self.lookup_registry.get_entry(&name);
            if let Some(laminar_sql::datafusion::RegisteredLookup::Versioned(v)) = &entry {
                if let Some(batch) = self.table_store.read().to_record_batch(&name)? {
                    if let Some(state) = crate::pipeline_callback::rebuild_versioned_state(v, batch)
                    {
                        self.lookup_registry.register_versioned(&name, state);
                    }
                }
            } else if let Some(batch) = self.table_store.read().to_record_batch(&name)? {
                self.lookup_registry
                    .register(&name, laminar_sql::datafusion::LookupSnapshot { batch });
            }
        }

        for (name, reg) in table_regs {
            if !reg.on_demand {
                continue;
            }
            let capacity_bytes = reg.cache_max_bytes.unwrap_or(64 * 1024 * 1024);
            let schema = self.table_store.read().table_schema(name).ok_or_else(|| {
                DbError::Pipeline(format!(
                    "On-demand lookup table '{name}' has no registered schema"
                ))
            })?;
            let pk_csv = &reg.primary_key;
            let pk_cols: Vec<String> = pk_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let key_sort_fields: Vec<arrow::row::SortField> = pk_cols
                .iter()
                .map(|col| {
                    schema
                        .field_with_name(col)
                        .map(|f| arrow::row::SortField::new(f.data_type().clone()))
                        .map_err(|error| {
                            DbError::Pipeline(format!(
                                "On-demand lookup table '{name}' has invalid key column \
                                 '{col}': {error}"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;

            let cache = Arc::new(laminar_core::lookup::lookup_cache::LookupMemoryCache::new(
                0,
                laminar_core::lookup::lookup_cache::LookupMemoryCacheConfig {
                    capacity_bytes,
                    ttl: reg.cache_ttl,
                },
            ));
            let mut config = build_table_config(reg)?;
            config.set("_primary_key_columns", pk_csv.as_str());
            let lookup_source = match self
                .connector_registry
                .create_lookup_source(config, Some(Arc::clone(&schema)))
                .await
            {
                Some(Ok(source)) => source,
                Some(Err(error)) => {
                    return Err(DbError::Connector(format!(
                        "Cannot create on-demand lookup source '{name}': {error}"
                    )));
                }
                None => {
                    return Err(DbError::Connector(format!(
                        "On-demand lookup source factory for '{name}' disappeared after DDL admission"
                    )));
                }
            };

            let projection = crate::sql_analysis::compute_lookup_projection(
                &schema,
                &pk_cols,
                name.as_str(),
                stream_regs.values().map(|r| r.query_sql.as_str()),
            );

            self.lookup_registry.register_partial(
                name,
                laminar_sql::datafusion::PartialLookupState {
                    lookup_cache: cache,
                    schema,
                    key_columns: pk_cols,
                    key_sort_fields,
                    source: Some(lookup_source),
                    fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                    projection,
                },
            );
            tracing::info!(
                table = %name,
                capacity_bytes,
                ttl = ?reg.cache_ttl,
                pk = %pk_csv,
                "registered on-demand lookup table (partial cache)"
            );
        }

        Ok(())
    }
    fn prepare_pipeline_watermarks(
        &self,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        recovered_source_wms: &FxHashMap<String, i64>,
        recovered_watermark_frontier: Option<i64>,
        recovered_all_sources_idle: bool,
        source_watermark_recovery_selected: bool,
    ) -> Result<PipelineWatermarks, DbError> {
        let stream_entries: Vec<_> = self
            .catalog
            .list_streams()
            .into_iter()
            .map(|name| {
                if !stream_regs.contains_key(&name) {
                    return Err(DbError::Pipeline(format!(
                        "catalog stream '{name}' has no executable registration"
                    )));
                }
                self.catalog.get_stream_entry(&name).ok_or_else(|| {
                    DbError::Pipeline(format!(
                        "catalog stream '{name}' disappeared during startup"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;

        // `LAMINAR_MAX_FUTURE_SKEW_MS=0` disables the future-skew ceiling (legacy unbounded).
        let future_skew_ms = match std::env::var("LAMINAR_MAX_FUTURE_SKEW_MS") {
            Ok(v) => v.parse::<i64>().unwrap_or_else(|_| {
                tracing::warn!(
                    value = %v,
                    "invalid LAMINAR_MAX_FUTURE_SKEW_MS (expected an integer); \
                     using the default"
                );
                laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS
            }),
            Err(_) => laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS,
        };
        let source_names = self.catalog.list_sources();
        let mut watermark_states: FxHashMap<String, SourceWatermarkState> =
            FxHashMap::with_capacity_and_hasher(source_names.len(), rustc_hash::FxBuildHasher);
        let mut source_entries_for_wm: FxHashMap<String, Arc<crate::catalog::SourceEntry>> =
            FxHashMap::with_capacity_and_hasher(source_names.len(), rustc_hash::FxBuildHasher);
        let mut source_ids: FxHashMap<String, usize> =
            FxHashMap::with_capacity_and_hasher(source_names.len(), rustc_hash::FxBuildHasher);
        for name in source_names {
            if let Some(entry) = self.catalog.get_source(&name) {
                if let (Some(col), Some(dur)) =
                    (&entry.watermark_column, entry.max_out_of_orderness)
                {
                    let extractor = laminar_core::time::EventTimeExtractor::from_column(col)
                        .with_mode(laminar_core::time::ExtractionMode::Max);
                    let generator: Box<dyn laminar_core::time::WatermarkGenerator> = if entry
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        Box::new(laminar_core::time::ProcessingTimeGenerator::new())
                    } else {
                        Box::new(
                            laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(dur)
                                .with_max_future_skew(future_skew_ms),
                        )
                    };
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col.clone(),
                        },
                    );
                }
                source_entries_for_wm.insert(name, entry);
            }
        }

        // Fallback watermark path for sources configured through the programmatic API.
        for name in self.catalog.list_sources() {
            if watermark_states.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                if let Some(col) = entry.source.event_time_column() {
                    let extractor = laminar_core::time::EventTimeExtractor::from_column(&col)
                        .with_mode(laminar_core::time::ExtractionMode::Max);
                    let ooo_bound = entry
                        .source
                        .max_out_of_orderness()
                        .unwrap_or(std::time::Duration::ZERO);
                    let generator: Box<dyn laminar_core::time::WatermarkGenerator> = if entry
                        .is_processing_time
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        Box::new(laminar_core::time::ProcessingTimeGenerator::new())
                    } else {
                        Box::new(
                            laminar_core::time::BoundedOutOfOrdernessGenerator::from_duration(
                                ooo_bound,
                            )
                            .with_max_future_skew(future_skew_ms),
                        )
                    };
                    let id = source_ids.len();
                    source_ids.insert(name.clone(), id);
                    watermark_states.insert(
                        name.clone(),
                        SourceWatermarkState {
                            extractor,
                            generator,
                            column: col,
                        },
                    );
                }
            }
        }

        // LAMINAR_SOURCE_IDLE_TIMEOUT_MS > 0 enables idle-source detection; unset/0 = disabled.
        let idle_timeout_ms: Option<u64> = match std::env::var("LAMINAR_SOURCE_IDLE_TIMEOUT_MS") {
            Ok(v) => match v.parse::<u64>() {
                Ok(0) => None,
                Ok(ms) => Some(ms),
                Err(_) => {
                    tracing::warn!(
                        value = %v,
                        "invalid LAMINAR_SOURCE_IDLE_TIMEOUT_MS (expected a non-negative \
                         integer); idle-source detection disabled"
                    );
                    None
                }
            },
            Err(_) => None,
        };
        let mut tracker = if source_ids.is_empty() {
            None
        } else {
            let mut t = laminar_core::time::WatermarkTracker::new(source_ids.len());
            if let Some(ms) = idle_timeout_ms {
                let d = std::time::Duration::from_millis(ms);
                for id in 0..source_ids.len() {
                    t.set_idle_timeout(id, Some(d));
                }
            }
            Some(t)
        };

        // Mixed watermarked/un-watermarked sources: un-watermarked ones inherit
        // the global clock, so joins/windows close on the watermarked source's
        // time. Surface the mismatch rather than silently dropping late rows.
        let registered = self.catalog.list_sources();
        let unwatermarked: Vec<&str> = registered
            .iter()
            .filter(|n| !source_ids.contains_key(*n))
            .map(String::as_str)
            .collect();
        if !source_ids.is_empty() && !unwatermarked.is_empty() {
            tracing::warn!(
                watermarked = source_ids.len(),
                unwatermarked = unwatermarked.len(),
                unwatermarked_names = ?unwatermarked,
                "Pipeline mixes watermarked and un-watermarked sources. An un-watermarked \
                 source in a join/window inherits the global watermark — time-based \
                 operators may behave unexpectedly. Add `WATERMARK FOR` to the missing \
                 sources or split into separate pipelines."
            );
        }

        if source_watermark_recovery_selected {
            let mut tracker_watermarks = vec![None; source_ids.len()];
            for (name, &source_id) in &source_ids {
                let recovered = recovered_source_wms
                    .get(name)
                    .copied()
                    .or(recovered_watermark_frontier);
                tracker_watermarks[source_id] = recovered;
                if let (Some(state), Some(watermark)) = (watermark_states.get_mut(name), recovered)
                {
                    state.generator.restore_watermark_for_recovery(watermark);
                }
            }
            if let Some(tracker) = tracker.as_mut() {
                let idle_sources = vec![recovered_all_sources_idle; source_ids.len()];
                tracker
                    .restore_for_recovery(
                        &tracker_watermarks,
                        &idle_sources,
                        recovered_watermark_frontier,
                    )
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "failed to restore the committed watermark tracker: {error}"
                        ))
                    })?;
            }
            self.pipeline_watermark.store(
                recovered_watermark_frontier.unwrap_or(i64::MIN),
                std::sync::atomic::Ordering::Release,
            );
            tracing::info!(
                sources = tracker_watermarks.len(),
                pipeline_watermark = ?recovered_watermark_frontier,
                all_sources_idle = recovered_all_sources_idle,
                "restored exact watermark frontier from checkpoint"
            );
        }

        Ok(PipelineWatermarks {
            stream_entries,
            watermark_states,
            source_entries: source_entries_for_wm,
            source_ids,
            tracker,
        })
    }

    async fn prepare_pipeline_runtime(
        &self,
        sources: Vec<TrackedSourceRegistration>,
        mut graph: crate::operator_graph::OperatorGraph,
        sink_setup: PipelineSinkSetup,
        watermarks: PipelineWatermarks,
        config: crate::pipeline::PipelineConfig,
        runtime_mode: RuntimeMode,
        #[cfg(feature = "cluster")] reconciled_source_handoff_version: Option<u64>,
    ) -> Result<PreparedPipelineRuntime, DbError> {
        let PipelineSinkSetup {
            sinks,
            sink_event_rx,
            coordinated_committer,
            committer_poll,
            committer_notify,
            #[cfg(feature = "cluster")]
            callback_controller,
        } = sink_setup;
        let PipelineWatermarks {
            stream_entries,
            watermark_states,
            source_entries,
            source_ids,
            tracker,
        } = watermarks;

        graph.set_query_budget_ns(config.query_budget_ns);
        graph.set_max_input_buf_batches(config.max_input_buf_batches);
        graph.set_max_input_buf_bytes(config.max_input_buf_bytes);
        graph.set_backpressure_policy(config.backpressure_policy);
        graph.set_shared_source_isolation(
            config.shared_source_isolation,
            config.max_replay_buffer_bytes,
        );

        let pending_sink_filter_compiles = sinks
            .iter()
            .filter(|(_, _, filter_sql, _, _)| filter_sql.is_some())
            .count();
        let source_name_arcs: rustc_hash::FxHashMap<usize, Arc<str>> = source_ids
            .iter()
            .map(|(name, &source_id)| (source_id, Arc::<str>::from(name.as_str())))
            .collect();
        let source_wms_buf = rustc_hash::FxHashMap::with_capacity_and_hasher(
            source_name_arcs.len(),
            rustc_hash::FxBuildHasher,
        );
        let prom = self
            .engine_metrics
            .lock()
            .clone()
            .expect("EngineMetrics must be set before start()");

        let (force_checkpoint_tx, force_checkpoint_rx) =
            crossfire::mpsc::bounded_async::<crate::db::ForceCheckpointReply>(
                crate::db::FORCE_CHECKPOINT_CHANNEL_CAPACITY,
            );
        *self.force_ckpt_tx.lock() = Some(force_checkpoint_tx);
        let (checkpoint_complete_tx, checkpoint_complete_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(16);

        let checkpoint_committable_sinks = sinks
            .iter()
            .any(|(_, handle, _, _, _)| handle.checkpoint_committable());
        let checkpoint_in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (
            epoch_allocator,
            quorum_timeout,
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            max_staged_bytes,
            coordinated_commit_admission,
        ) = {
            let coordinator = self.coordinator.lock().await;
            match coordinator.as_ref() {
                Some(coordinator) => {
                    let checkpoint = coordinator.config();
                    (
                        Some(coordinator.epoch_allocator()),
                        checkpoint.quorum_timeout,
                        checkpoint.checkpoint_timeout,
                        checkpoint.cleanup_timeout,
                        checkpoint.max_staged_bytes,
                        coordinator.coordinated_commit_admission(),
                    )
                }
                None => (
                    None,
                    std::time::Duration::from_secs(3),
                    std::time::Duration::from_secs(120),
                    crate::checkpoint_coordinator::CheckpointConfig::default().cleanup_timeout,
                    u64::MAX,
                    None,
                ),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let _ = quorum_timeout;

        let named_stream_names = stream_entries
            .iter()
            .map(|entry| Arc::from(entry.name.as_str()))
            .collect();
        #[cfg(feature = "cluster")]
        let source_process_authority = (runtime_mode == RuntimeMode::Cluster)
            .then(|| callback_controller.clone())
            .flatten();
        #[cfg(feature = "cluster")]
        let vnode_registry = self.vnode_registry.lock().clone();
        #[cfg(feature = "cluster")]
        let (shuffle_delivery_loss_incidents, shuffle_recovered_delivery_loss_incidents) = self
            .shuffle_receiver
            .lock()
            .as_ref()
            .map_or((None, None), |receiver| {
                (
                    Some(receiver.delivery_loss_incidents()),
                    Some(receiver.recovered_delivery_loss_incidents()),
                )
            });

        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            graph,
            stream_entries,
            sinks,
            owned_sink_handles: Arc::clone(&self.owned_sink_handles),
            watermark_states,
            source_entries_for_wm: source_entries,
            source_ids,
            source_name_arcs,
            source_wms_buf,
            tracker,
            prom,
            #[cfg(feature = "cluster")]
            checkpoint_barrier_timings: Arc::clone(&self.checkpoint_barrier_timings),
            pipeline_watermark: Arc::clone(&self.pipeline_watermark),
            coordinator: Arc::clone(&self.coordinator),
            table_store: self.table_store.clone(),
            mv_store_has_any: self.mv_store.read().has_any_handle(),
            mv_store: self.mv_store.clone(),
            filter_ctx: laminar_sql::create_session_context(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles,
            delivery_guarantee: config.delivery_guarantee,
            serialization_timeout: checkpoint_timeout,
            checkpoint_state_cap_bytes: max_staged_bytes,
            checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            sink_event_rx,
            sink_timed_out: false,
            sink_fault: None,
            checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
            last_checkpoint_admission_failure: None,
            checkpoint_admission_recovering: false,
            shutdown_signal: Arc::clone(&self.shutdown_signal),
            #[cfg(feature = "cluster")]
            vnode_registry,
            #[cfg(feature = "cluster")]
            reconciled_source_handoff_version,
            #[cfg(feature = "cluster")]
            cluster_controller: callback_controller,
            #[cfg(feature = "cluster")]
            follower_tail: Arc::default(),
            #[cfg(feature = "cluster")]
            barrier_injectors: Vec::new(),
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents,
            #[cfg(feature = "cluster")]
            shuffle_recovered_delivery_loss_incidents,
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents_seen: 0,
            #[cfg(feature = "cluster")]
            pending_follower_checkpoint: None,
            #[cfg(feature = "cluster")]
            checkpoint_leader_proofs: rustc_hash::FxHashMap::default(),
            subscription_registry: Arc::clone(&self.subscription_registry),
            named_stream_names,
            checkpoint_complete_tx,
            checkpoint_tail_tasks: tokio::task::JoinSet::new(),
            checkpoint_in_flight: Arc::clone(&checkpoint_in_flight),
            #[cfg(feature = "cluster")]
            delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            last_vnode_capture_epoch: None,
            epoch_allocator,
            #[cfg(feature = "cluster")]
            quorum_timeout,
            checkpoint_committable_sinks,
            #[cfg(feature = "cluster")]
            intake_gate: Arc::clone(&self.source_gate),
            #[cfg(not(feature = "cluster"))]
            intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        Ok(PreparedPipelineRuntime {
            runtime: PipelineRuntimeSetup {
                sources,
                config,
                callback,
                force_checkpoint_rx,
                checkpoint_complete_rx,
                checkpoint_in_flight,
                coordinated_commit_admission,
                #[cfg(feature = "cluster")]
                source_process_authority,
                runtime_mode,
            },
            coordinated_committer,
            committer_poll,
            committer_notify,
        })
    }

    async fn launch_pipeline_runtime(
        &self,
        setup: PipelineRuntimeSetup,
        shutdown: Arc<tokio::sync::Notify>,
        runtime_shutdown: tokio_util::sync::CancellationToken,
        #[cfg(feature = "cluster")] startup_generation_fence: Option<
            tokio::sync::OwnedRwLockWriteGuard<()>,
        >,
    ) -> Result<(), DbError> {
        let PipelineRuntimeSetup {
            sources,
            config: pipeline_config,
            callback,
            force_checkpoint_rx: force_ckpt_rx,
            checkpoint_complete_rx,
            checkpoint_in_flight,
            coordinated_commit_admission,
            #[cfg(feature = "cluster")]
            source_process_authority,
            runtime_mode,
        } = setup;
        let (control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
        *self.control_tx.lock() = Some(control_tx);

        #[cfg(feature = "cluster")]
        let source_gate = Arc::clone(&self.source_gate);
        #[cfg(not(feature = "cluster"))]
        let source_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let coordinator = crate::pipeline::StreamingCoordinator::new_with_tracked_source_registry(
            sources,
            pipeline_config,
            Arc::clone(&shutdown),
            control_rx,
            source_gate,
            #[cfg(feature = "cluster")]
            source_process_authority,
            Arc::clone(&self.owned_source_tasks),
            runtime_mode,
        )
        .await?
        .with_terminal_shutdown(runtime_shutdown.clone())
        .with_force_checkpoint_rx(force_ckpt_rx)
        .with_checkpoint_complete_rx(checkpoint_complete_rx)
        .with_checkpoint_admission(checkpoint_in_flight)
        .with_coordinated_commit_admission(coordinated_commit_admission);

        let (done_tx, done_rx) = crossfire::oneshot::oneshot::<crate::pipeline::ExitReason>();
        let (startup_tx, startup_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
        // Captured by the compute thread so an operator panic is recorded
        // (surfaced via pipeline status) rather than only logged.
        let fault_slot = Arc::clone(&self.last_fault);
        let fault_state = Arc::clone(&self.state);
        let fault_metrics = self.engine_metrics.lock().clone();
        #[cfg(feature = "cluster")]
        let compute_fault_source_gate = Arc::clone(&self.source_gate);
        #[cfg(feature = "cluster")]
        let compute_fault_recovery_fence = Arc::clone(&self.coordinated_recovery_fenced);
        #[cfg(feature = "cluster")]
        let compute_fault_is_cluster = runtime_mode == RuntimeMode::Cluster;
        #[cfg(feature = "cluster")]
        let compute_fault_installed_vnode_state = Arc::clone(&self.installed_vnode_state);
        #[cfg(feature = "cluster")]
        let compute_fault_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let compute_fault_pending = Arc::clone(&self.pending_recovery_fault);
        #[cfg(feature = "cluster")]
        let compute_fault_runtime_shutdown = runtime_shutdown.clone();
        let compute_thread = std::thread::Builder::new().name("laminar-compute".into());
        #[cfg(feature = "cluster")]
        let compute_thread = if runtime_mode == RuntimeMode::Cluster {
            // Windows' default thread stack is too small for the clustered graph/control
            // lifecycle. Keep this explicit and bounded: there is one compute thread per running
            // pipeline, and the cluster I/O workers use the same 4 MiB policy.
            compute_thread.stack_size(CLUSTER_COMPUTE_THREAD_STACK_BYTES)
        } else {
            compute_thread
        };
        match compute_thread
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        startup_tx.send(Err(format!("compute runtime: {e}")));
                        return;
                    }
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    rt.block_on(async move {
                        Box::pin(coordinator.run_with_ready(callback, startup_tx)).await
                    })
                }));
                // Runtime shutdown waits for non-abortable `spawn_blocking` filesystem work.
                // Publish neither clean completion nor a fault until those workers are gone;
                // otherwise lifecycle teardown could release the exact namespace lock while
                // an old local decision hard-link was still able to appear.
                drop(rt);
                let exit = match result {
                    Ok(exit) => exit,
                    Err(panic) => {
                        let msg = panic
                            .downcast_ref::<String>()
                            .map(String::as_str)
                            .or_else(|| panic.downcast_ref::<&str>().copied())
                            .unwrap_or("unknown");
                        tracing::error!(panic = msg, "laminar-compute thread panicked");
                        crate::pipeline::ExitReason::Fault(msg.to_string())
                    }
                };
                let exit = match exit {
                    crate::pipeline::ExitReason::Shutdown => {
                        crate::pipeline::ExitReason::Shutdown
                    }
                    crate::pipeline::ExitReason::Fault(reason) => {
                        #[cfg(feature = "cluster")]
                        if compute_fault_is_cluster {
                            // Fence public restarts before publishing Faulted. The state CAS
                            // then orders this fault against a concurrent coordinated stop.
                            compute_fault_source_gate
                                .store(true, std::sync::atomic::Ordering::SeqCst);
                            compute_fault_recovery_fence
                                .store(true, std::sync::atomic::Ordering::Release);
                            compute_fault_installed_vnode_state.lock().take();
                        }

                        // Publish before notifying the watcher. This closes the ready-send ->
                        // watcher-scheduled window in which start() could otherwise report
                        // Running after the compute loop had already exited.
                        #[cfg(feature = "cluster")]
                        let owns_fault_state = publish_runtime_fault_state(&fault_state);
                        #[cfg(not(feature = "cluster"))]
                        publish_runtime_fault_state(&fault_state);
                        #[cfg(feature = "cluster")]
                        let covered_by_terminal_stop = compute_fault_is_cluster
                            && !owns_fault_state
                            && compute_fault_runtime_shutdown.is_cancelled()
                            && DbState::load(&fault_state) == DbState::ShuttingDown;
                        #[cfg(not(feature = "cluster"))]
                        let covered_by_terminal_stop = false;

                        if covered_by_terminal_stop {
                            crate::pipeline::ExitReason::Shutdown
                        } else {
                            tracing::error!(
                                reason = %reason,
                                "pipeline faulted on a fatal cycle error; recovering from last checkpoint"
                            );
                            *fault_slot.lock() = Some(reason.clone());
                            #[cfg(feature = "cluster")]
                            if compute_fault_is_cluster {
                                if let Some(controller) = compute_fault_controller.as_deref() {
                                    if let Err(error) = queue_owned_cluster_compute_fault(
                                        controller,
                                        &compute_fault_pending,
                                        owns_fault_state,
                                        &compute_fault_runtime_shutdown,
                                    ) {
                                        tracing::error!(
                                            %error,
                                            "could not allocate a recovery fault request"
                                        );
                                    }
                                }
                                // A Release may have raced the close-before-queue edge.
                                compute_fault_source_gate
                                    .store(true, std::sync::atomic::Ordering::SeqCst);
                                compute_fault_recovery_fence
                                    .store(true, std::sync::atomic::Ordering::Release);
                            }
                            if let Some(ref metrics) = fault_metrics {
                                metrics.pipeline_faults_total.inc();
                            }
                            crate::pipeline::ExitReason::Fault(reason)
                        }
                    }
                };
                done_tx.send(exit);
            })
        {
            Ok(_) => {}
            Err(e) => {
                return Err(DbError::Config(format!(
                    "failed to spawn compute thread: {e}"
                )));
            }
        }

        match startup_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(_)) if runtime_shutdown.is_cancelled() || self.is_closed() => {
                let _ = done_rx.await;
                return Err(DbError::Shutdown);
            }
            Ok(Err(e)) => {
                let _ = done_rx.await;
                return Err(DbError::Config(e));
            }
            Err(_) => {
                let _ = done_rx.await;
                return Err(DbError::Config(
                    "compute thread exited before entering the runtime control loop".into(),
                ));
            }
        }

        // Readiness transfers the recovered MV image and fully wired graph to the live loop. The
        // caller installed any pre-audited no-work success marker before launch, so an immediate
        // runtime fault can only clear it, never race a post-ready write that resurrects it.
        #[cfg(feature = "cluster")]
        drop(startup_generation_fence);

        let watcher_state = Arc::clone(&self.state);
        let watcher_shutdown = Arc::clone(&self.shutdown_signal);
        let watcher_fault = Arc::clone(&self.last_fault);
        let watcher_supervisor = Arc::clone(&self.supervisor_self);
        let watcher_restart_history = Arc::clone(&self.restart_history);
        let watcher_metrics = self.engine_metrics.lock().clone();
        #[cfg(feature = "cluster")]
        let watcher_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let watcher_source_gate = Arc::clone(&self.source_gate);
        #[cfg(feature = "cluster")]
        let watcher_recovery_fence = Arc::clone(&self.coordinated_recovery_fenced);
        #[cfg(feature = "cluster")]
        let watcher_is_cluster = runtime_mode == RuntimeMode::Cluster;
        #[cfg(feature = "cluster")]
        let watcher_installed_vnode_state = Arc::clone(&self.installed_vnode_state);
        #[cfg(feature = "cluster")]
        let watcher_pending_compute_fault = Arc::clone(&self.pending_recovery_fault);
        #[cfg(feature = "cluster")]
        let watcher_runtime_shutdown = runtime_shutdown.clone();
        let handle = tokio::spawn(async move {
            let exit = done_rx.await.unwrap_or_else(|_| {
                crate::pipeline::ExitReason::Fault(
                    "compute thread exited without a terminal result".to_string(),
                )
            });
            match exit {
                crate::pipeline::ExitReason::Shutdown => {
                    // Lifecycle ownership finalizes the state only after every remote decision
                    // writer has settled. The watcher cannot prove that merely because the
                    // compute thread exited, so a timed-out stop remains ShuttingDown until retry.
                }
                crate::pipeline::ExitReason::Fault(reason) => {
                    tracing::error!(%reason, "laminar-compute thread exited with a recoverable fault");
                    watcher_fault.lock().get_or_insert(reason);
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        // Also cover a lost terminal channel or a compute-thread exit before
                        // its normal fault publication reached this watcher.
                        watcher_source_gate.store(true, std::sync::atomic::Ordering::SeqCst);
                        watcher_recovery_fence.store(true, std::sync::atomic::Ordering::Release);
                        watcher_installed_vnode_state.lock().take();
                    }
                    publish_runtime_fault_state(&watcher_state);
                    watcher_shutdown.notify_one();
                    // Cluster mode: report the fault and let the leader drive a global
                    // restart; the monitor restores this node. A local restart would rewind
                    // only this node while peers advanced — an inconsistent cut.
                    #[cfg(feature = "cluster")]
                    if watcher_is_cluster {
                        tokio::select! {
                            biased;
                            () = watcher_runtime_shutdown.cancelled() => {}
                            () = report_cluster_compute_fault(
                                watcher_controller,
                                watcher_pending_compute_fault,
                            ) => {}
                        }
                        return;
                    }
                    // Auto-restart if supervised; otherwise the pipeline stays Faulted.
                    let supervised = watcher_supervisor.lock().upgrade();
                    if let Some(db) = supervised {
                        let _ =
                            spawn_supervised_restart(db, watcher_restart_history, watcher_metrics);
                    }
                }
            }
        });

        *self.runtime_handle.lock().await = Some(handle);
        Ok(())
    }

    async fn start_coordinated_committer(
        &self,
        committer: Option<crate::coordinated_committer::CoordinatedCommitter>,
        poll_interval: std::time::Duration,
        notify: Arc<tokio::sync::Notify>,
    ) -> Result<(), DbError> {
        let Some(mut committer) = committer else {
            return Ok(());
        };

        let state = Arc::clone(&self.state);
        let handle = tokio::spawn(async move {
            let mut tick = tokio::time::interval(poll_interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await;

            if let Err(error) = committer.commit_ready().await {
                tracing::warn!(%error, "initial coordinated committer pass failed; will retry");
            }
            loop {
                tokio::select! {
                    () = notify.notified() => {}
                    _ = tick.tick() => {}
                }
                if matches!(
                    DbState::load(&state),
                    DbState::Stopped | DbState::Faulted | DbState::Created
                ) {
                    break;
                }
                if let Err(error) = committer.commit_ready().await {
                    tracing::warn!(%error, "coordinated committer pass failed; will retry");
                }
            }
        });

        let mut owned_handle = self.committer_handle.lock().await;
        if owned_handle.is_some() {
            handle.abort();
            let _ = handle.await;
            return Err(DbError::Checkpoint(
                "cannot start a coordinated committer while a prior generation is still owned"
                    .into(),
            ));
        }
        *owned_handle = Some(handle);
        Ok(())
    }

    async fn start_connector_pipeline(
        &self,
        source_regs: HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: HashMap<String, crate::connector_manager::TableRegistration>,
        has_external: bool,
        pipeline_identity: Option<laminar_core::checkpoint::PipelineIdentity>,
        runtime_shutdown: tokio_util::sync::CancellationToken,
    ) -> Result<(), DbError> {
        use crate::pipeline::{CheckpointSchedule, PipelineConfig};

        let runtime_mode = self.runtime_mode();

        #[cfg(feature = "cluster")]
        let startup_assignment_guard = if runtime_mode == RuntimeMode::Cluster {
            Some(self.assignment_adoption_lock.lock().await)
        } else {
            None
        };

        #[cfg(feature = "cluster")]
        let startup_generation_fence = if runtime_mode == RuntimeMode::Cluster {
            let generation_fence = Arc::clone(&self.rotation_execution_fence)
                .write_owned()
                .await;
            Some(generation_fence)
        } else {
            None
        };

        self.revalidate_persisted_cluster_query_shapes(&stream_regs)
            .await?;

        let checkpoint_schedule =
            self.config
                .checkpoint
                .as_ref()
                .map_or(CheckpointSchedule::Disabled, |config| {
                    config
                        .interval_ms
                        .map_or(CheckpointSchedule::Manual, |interval_ms| {
                            CheckpointSchedule::Periodic(std::time::Duration::from_millis(
                                interval_ms,
                            ))
                        })
                });
        let checkpointing_enabled = checkpoint_schedule.is_enabled();
        let pipeline_checkpoint_timeout = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|config| config.timeout_ms)
            .map_or(
                crate::checkpoint_coordinator::CheckpointConfig::default().checkpoint_timeout,
                std::time::Duration::from_millis,
            );

        let stream_output_schemas = resolve_stream_output_schemas(&self.ctx, &stream_regs).await?;
        {
            let mut schemas = self.stream_schemas.write();
            schemas.clear();
            schemas.extend(
                stream_output_schemas
                    .iter()
                    .map(|(name, schema)| (name.clone(), Arc::clone(schema))),
            );
        }

        let mut graph = self.build_connector_operator_graph(
            &stream_regs,
            &table_regs,
            pipeline_identity.as_ref(),
        )?;
        for (name, schema) in &stream_output_schemas {
            graph.register_intermediate_schema(name, schema);
        }
        let graph = graph.initialize_managed_state().await?;

        let prom_registry = self.prometheus_registry.lock().clone();
        let mut sources = self.build_pipeline_sources(
            &source_regs,
            checkpointing_enabled,
            runtime_mode,
            prom_registry.as_ref(),
        )?;

        let sink_setup = self
            .prepare_pipeline_sinks(
                &sources,
                &sink_regs,
                &stream_regs,
                &stream_output_schemas,
                runtime_mode,
                checkpointing_enabled,
                pipeline_checkpoint_timeout,
                prom_registry.as_ref(),
            )
            .await?;
        let PipelineRecoveryState {
            graph,
            recovered_mv_store,
            recovered_source_wms,
            recovered_watermark_frontier,
            recovered_all_sources_idle,
            source_watermark_recovery_selected,
            #[cfg(feature = "cluster")]
                reconciled_source_handoff_version: startup_reconciled_source_handoff_version,
            restored_reference_tables,
        } = self
            .recover_pipeline_state(
                graph,
                &mut sources,
                runtime_mode,
                pipeline_checkpoint_timeout,
            )
            .await?;
        let previous_mv_store = {
            let mut live = self.mv_store.write();
            std::mem::replace(&mut *live, recovered_mv_store)
        };
        drop(previous_mv_store);

        if source_watermark_recovery_selected {
            for source_name in self.catalog.list_sources() {
                if let Some(entry) = self.catalog.get_source(&source_name) {
                    entry.source.restore_watermark_for_recovery(
                        recovered_source_wms
                            .get(&source_name)
                            .copied()
                            .or(recovered_watermark_frontier)
                            .unwrap_or(i64::MIN),
                    );
                }
            }
        }

        self.initialize_reference_tables(&table_regs, &stream_regs, restored_reference_tables)
            .await?;
        let watermarks = self.prepare_pipeline_watermarks(
            &stream_regs,
            &recovered_source_wms,
            recovered_watermark_frontier,
            recovered_all_sources_idle,
            source_watermark_recovery_selected,
        )?;
        let max_poll = self.config.default_buffer_size.min(1024);
        tracing::info!(
            sources = sources.len(),
            sinks = sink_setup.sinks.len(),
            streams = stream_regs.len(),
            watermark_sources = watermarks.source_ids.len(),
            "Starting event-driven connector pipeline"
        );

        let drain_budget_ns = self.config.pipeline_drain_budget_ns.unwrap_or(1_000_000);
        let query_budget_ns = self.config.pipeline_query_budget_ns.unwrap_or(8_000_000);
        let pipeline_config = PipelineConfig {
            max_poll_records: max_poll,
            channel_capacity: self.config.pipeline_channel_capacity.unwrap_or(64),
            fallback_poll_interval: if has_external {
                std::time::Duration::from_millis(10)
            } else {
                std::time::Duration::from_millis(1)
            },
            checkpoint_schedule,
            batch_window: self
                .config
                .pipeline_batch_window
                .unwrap_or(if has_external {
                    std::time::Duration::from_millis(5)
                } else {
                    std::time::Duration::ZERO
                }),
            checkpoint_timeout: pipeline_checkpoint_timeout,
            delivery_guarantee: self.config.delivery_guarantee,
            cycle_budget_ns: 10_000_000_u64.max(drain_budget_ns + query_budget_ns),
            drain_budget_ns,
            query_budget_ns,
            background_budget_ns: 5_000_000, // 5ms
            max_input_buf_batches: self.config.pipeline_max_input_buf_batches.unwrap_or(256),
            max_input_buf_bytes: self.config.pipeline_max_input_buf_bytes,
            backpressure_policy: self.config.pipeline_backpressure_policy,
            shared_source_isolation: self.config.shared_source_isolation,
            max_replay_buffer_bytes: 256 * 1024 * 1024,
        };

        let PreparedPipelineRuntime {
            runtime,
            coordinated_committer,
            committer_poll,
            committer_notify,
        } = self
            .prepare_pipeline_runtime(
                sources,
                graph,
                sink_setup,
                watermarks,
                pipeline_config,
                runtime_mode,
                #[cfg(feature = "cluster")]
                startup_reconciled_source_handoff_version,
            )
            .await?;

        #[cfg(feature = "cluster")]
        let graph_ready_vnode_state = if runtime_mode == RuntimeMode::Cluster {
            self.prepare_graph_ready_vnode_state_binding(
                tokio::time::Instant::now() + pipeline_checkpoint_timeout,
            )
            .await?
        } else {
            None
        };
        #[cfg(feature = "cluster")]
        if let Some(installed) = graph_ready_vnode_state.as_ref() {
            *self.installed_vnode_state.lock() = Some(installed.clone());
        }
        let launch = self
            .launch_pipeline_runtime(
                runtime,
                Arc::clone(&self.shutdown_signal),
                runtime_shutdown,
                #[cfg(feature = "cluster")]
                startup_generation_fence,
            )
            .await;
        #[cfg(feature = "cluster")]
        let launch = launch.inspect_err(|_| {
            if let Some(expected) = graph_ready_vnode_state.as_ref() {
                let mut installed = self.installed_vnode_state.lock();
                if installed.as_ref() == Some(expected) {
                    installed.take();
                }
            }
        });
        launch?;

        // Readiness has transferred the exact captured assignment and its staged state to the
        // live graph. A watcher may now prepare a successor generation.
        #[cfg(feature = "cluster")]
        drop(startup_assignment_guard);

        self.start_coordinated_committer(coordinated_committer, committer_poll, committer_notify)
            .await
    }
    async fn quiesce_checkpoint_decision_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6038] teardown could not acquire checkpoint coordinator ownership; durable decision writes remain fenced"
                        .into(),
                )
            })?;
        if let Some(coordinator) = coordinator.as_mut() {
            coordinator
                .quiesce_pending_decision_write_until(deadline)
                .await?;
        }
        Ok(())
    }

    async fn reconcile_sink_open_witness_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<(), DbError> {
        let mut coordinator = tokio::time::timeout_at(deadline, self.coordinator.lock())
            .await
            .map_err(|_| {
                DbError::Checkpoint(
                    "[LDB-6050] teardown could not acquire checkpoint coordinator ownership for sink-open reconciliation"
                        .into(),
                )
            })?;
        if let Some(coordinator) = coordinator.as_mut() {
            coordinator
                .reconcile_sink_open_witness_until(deadline)
                .await?;
        }
        Ok(())
    }

    /// Shut down the streaming pipeline gracefully. Idempotent.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the watcher task panicked.
    pub async fn shutdown(&self) -> Result<(), DbError> {
        const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
        // Terminal intent takes precedence over a concurrent restartable stop. Publishing it
        // before lifecycle arbitration also prevents a brief stop-created state from admitting
        // a new startup while shutdown is queued behind that stop.
        self.close();
        #[cfg(feature = "cluster")]
        if self.is_cluster_runtime() {
            self.installed_vnode_state.lock().take();
        }
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        #[cfg(feature = "cluster")]
        self.quiesce_recovery_monitor_until(deadline).await?;
        let first_shutdown = loop {
            let startup = {
                let owned = self.startup_attempt.lock();
                if let Some(in_flight) = owned.as_ref().filter(|attempt| !attempt.is_complete()) {
                    Arc::clone(in_flight)
                } else {
                    match DbState::load(&self.state) {
                        DbState::Stopped => return Ok(()),
                        DbState::Starting => {
                            return Err(DbError::Pipeline(
                                "shutdown found Starting without an incomplete owned startup attempt"
                                    .into(),
                            ));
                        }
                        DbState::ShuttingDown => break false,
                        observed @ (DbState::Created | DbState::Running | DbState::Faulted) => {
                            if DbState::compare_exchange(
                                observed,
                                DbState::ShuttingDown,
                                &self.state,
                            )
                            .is_ok()
                            {
                                break true;
                            }
                            continue;
                        }
                    }
                }
            };
            self.await_startup_attempt_until(&startup, deadline, "pipeline shutdown")
                .await?;
        };
        self.runtime_shutdown.write().cancel();
        if first_shutdown {
            *self.force_ckpt_tx.lock() = None;
            self.shutdown_signal.notify_one();
        }

        let _topology = tokio::time::timeout_at(deadline, self.topology_ddl_lock.write())
            .await
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "pipeline shutdown could not acquire topology ownership within \
                     {SHUTDOWN_TIMEOUT:?}; catalog mutation remains fenced"
                ))
            })?;
        let _lifecycle = tokio::time::timeout_at(deadline, self.lifecycle_lock.lock())
            .await
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "pipeline shutdown could not acquire lifecycle ownership within \
                     {SHUTDOWN_TIMEOUT:?}; startup/stop remains fenced"
                ))
            })?;

        let mut runtime_handle = tokio::time::timeout_at(deadline, self.runtime_handle.lock())
            .await
            .map_err(|_| {
                DbError::Pipeline(
                    "pipeline shutdown could not reacquire runtime watcher ownership; runtime remains fenced in ShuttingDown"
                        .into(),
                )
            })?;
        let mut watcher_error = None;
        if let Some(handle) = runtime_handle.as_mut() {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(Ok(())) => {
                    runtime_handle.take();
                }
                Ok(Err(error)) => {
                    runtime_handle.take();
                    watcher_error = Some(DbError::Pipeline(format!(
                        "pipeline watcher failed during shutdown: {error}"
                    )));
                }
                Err(_) => {
                    return Err(DbError::Pipeline(format!(
                        "pipeline shutdown exceeded {SHUTDOWN_TIMEOUT:?}; runtime is still \
                         draining and remains fenced in ShuttingDown; retry shutdown"
                    )));
                }
            }
        }
        drop(runtime_handle);
        if watcher_error.is_none() {
            if let Some(fault) = self.last_fault.lock().clone() {
                watcher_error = Some(DbError::Pipeline(format!(
                    "pipeline faulted while shutting down: {fault}"
                )));
            } else {
                tracing::info!("Pipeline shut down cleanly");
            }
        }

        // Compute has stopped producing new checkpoint work. Keep every deployment/state fence
        // until an already-issued remote decision create reaches a terminal client-side state.
        self.quiesce_checkpoint_decision_until(deadline).await?;
        self.quiesce_committer_until(deadline).await?;
        self.reconcile_sink_open_witness_until(deadline).await?;
        self.quiesce_connector_generation_until(deadline).await?;

        *self.checkpoint_namespace_lock.lock() = None;
        DbState::Stopped.store(&self.state);
        watcher_error.map_or(Ok(()), Err)
    }

    /// Stop the streaming pipeline so it can be restarted.
    ///
    /// # Errors
    /// Returns [`DbError::InvalidOperation`] if the pipeline is still starting
    /// or the coordinator does not exit within the stop timeout.
    pub async fn stop_pipeline(&self) -> Result<(), DbError> {
        self.stop_pipeline_with_lifecycle_authority(PipelineLifecycleAuthority::Public)
            .await
    }

    /// Recovery-owned stop after the lifecycle fence is published.
    #[cfg(feature = "cluster")]
    pub(crate) async fn stop_pipeline_for_coordinated_recovery(&self) -> Result<(), DbError> {
        self.stop_pipeline_with_lifecycle_authority(PipelineLifecycleAuthority::CoordinatedRecovery)
            .await
    }

    async fn stop_pipeline_with_lifecycle_authority(
        &self,
        authority: PipelineLifecycleAuthority,
    ) -> Result<(), DbError> {
        const STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
        let first_stop = loop {
            let startup = {
                let owned = self.startup_attempt.lock();
                #[cfg(feature = "cluster")]
                self.ensure_pipeline_lifecycle_authorized(authority, "stop")?;
                #[cfg(not(feature = "cluster"))]
                Self::ensure_pipeline_lifecycle_authorized(authority, "stop");
                if let Some(in_flight) = owned.as_ref().filter(|attempt| !attempt.is_complete()) {
                    Arc::clone(in_flight)
                } else {
                    match DbState::load(&self.state) {
                        DbState::Created | DbState::Stopped => {
                            #[cfg(feature = "cluster")]
                            if self.is_cluster_runtime() {
                                self.installed_vnode_state.lock().take();
                            }
                            return Ok(());
                        }
                        DbState::Starting => {
                            return Err(DbError::InvalidOperation(
                                "pipeline stop found Starting without an incomplete owned startup attempt"
                                    .into(),
                            ));
                        }
                        DbState::ShuttingDown => break false,
                        observed @ (DbState::Running | DbState::Faulted) => {
                            #[cfg(feature = "cluster")]
                            if self.is_cluster_runtime() {
                                // Adoption revalidates this slot while holding the same mutex.
                                // Clear it before retiring Running so no cold preparation can
                                // publish against a graph generation that stop is removing.
                                self.installed_vnode_state.lock().take();
                            }
                            if DbState::compare_exchange(
                                observed,
                                DbState::ShuttingDown,
                                &self.state,
                            )
                            .is_ok()
                            {
                                break true;
                            }
                            continue;
                        }
                    }
                }
            };
            self.await_startup_attempt_until(&startup, deadline, "pipeline stop")
                .await
                .map_err(|error| DbError::InvalidOperation(error.to_string()))?;
        };
        self.runtime_shutdown.write().cancel();
        if first_stop {
            *self.force_ckpt_tx.lock() = None;
            // Clear up front so DDL during/after shutdown registers for the next start()
            // instead of hot-adding into the dying coordinator's channel.
            *self.control_tx.lock() = None;
            self.shutdown_signal.notify_one();
        }

        #[cfg(test)]
        {
            let stop_after_claim_gate = { self.stop_after_claim_gate.lock().clone() };
            if let Some((entered, release)) = stop_after_claim_gate {
                entered.notify_one();
                release.notified().await;
            }
        }

        let _topology = tokio::time::timeout_at(deadline, self.topology_ddl_lock.write())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(
                    "pipeline stop could not acquire topology ownership within 10s; catalog mutation remains fenced"
                        .into(),
                )
            })?;
        let _lifecycle = tokio::time::timeout_at(deadline, self.lifecycle_lock.lock())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(
                    "pipeline stop could not acquire lifecycle ownership within 10s; an earlier \
                     lifecycle operation remains fenced"
                        .into(),
                )
            })?;
        let mut runtime_handle = tokio::time::timeout_at(deadline, self.runtime_handle.lock())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(
                    "pipeline stop could not reacquire runtime watcher ownership; runtime remains fenced in ShuttingDown"
                        .into(),
                )
            })?;
        if let Some(handle) = runtime_handle.as_mut() {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(Ok(())) => {
                    runtime_handle.take();
                    tracing::info!("Pipeline stopped cleanly");
                }
                Ok(Err(e)) => {
                    runtime_handle.take();
                    tracing::warn!(error = %e, "Pipeline task panicked during stop");
                }
                Err(_) => {
                    tracing::warn!(
                        "Pipeline stop still draining after 10s; will finalize when the coordinator exits"
                    );
                    return Err(DbError::InvalidOperation(
                        "pipeline stop is taking longer than expected; coordinator still \
                         draining, retry shortly"
                            .into(),
                    ));
                }
            }
        }
        drop(runtime_handle);

        // Do not announce Created or release the exclusive deployment lock while a timed-out
        // decision create can still mutate the recovery frontier. A later stop retry resumes here.
        self.quiesce_checkpoint_decision_until(deadline).await?;
        self.quiesce_committer_until(deadline).await?;
        self.reconcile_sink_open_witness_until(deadline).await?;
        self.quiesce_connector_generation_until(deadline).await?;

        *self.checkpoint_namespace_lock.lock() = None;
        if self.is_closed() {
            // A concurrent shutdown owns the terminal transition. Leaving ShuttingDown in place
            // is intentional if that shutdown was cancelled; a retry must finish its teardown.
            return Ok(());
        }
        match DbState::compare_exchange(DbState::ShuttingDown, DbState::Created, &self.state) {
            Ok(_) | Err(DbState::Created | DbState::Stopped) => Ok(()),
            Err(observed) => Err(DbError::InvalidOperation(format!(
                "pipeline stop completed from unexpected lifecycle state {observed:?}; restart remains fenced"
            ))),
        }
    }
}

#[cfg(test)]
mod connector_admission_tests;

#[cfg(test)]
mod resolver_tests;

#[cfg(test)]
mod checkpoint_namespace_lock_tests;

#[cfg(test)]
mod mv_recovery_lifecycle_tests;

#[cfg(all(test, feature = "cluster"))]
mod cluster_fault_watcher_tests;

#[cfg(all(test, feature = "cluster"))]
mod boot_vnode_recovery_tests;

#[cfg(test)]
mod reference_table_recovery_tests;

#[cfg(test)]
mod supervisor_tests;
