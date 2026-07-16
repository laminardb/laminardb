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

#[cfg(feature = "cluster")]
use crate::db::ClusterStartupDisposition;
use crate::db::{exact_table_reference, DbState, LaminarDB, RuntimeMode, SourceWatermarkState};
use crate::error::DbError;

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

const fn required_recovery_scope(runtime: RuntimeMode) -> StateBackendDurability {
    match runtime {
        RuntimeMode::Local => StateBackendDurability::NodeDurable,
        RuntimeMode::Cluster => StateBackendDurability::ClusterShared,
    }
}

const EXACT_SINK_PROTOCOL: &str =
    "exactly-once external sinks require checkpoint-committable consistency, coordinated phase \
     1, participant-complete sealed markers, and a namespaced exact external cursor";

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
/// recovery announcement to clear. A successful restore clears the report; a failed round leaves
/// it available for retry.
#[cfg(feature = "cluster")]
async fn report_cluster_compute_fault(
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
) -> bool {
    let Some(controller) = controller else {
        return false;
    };
    crate::coordinated_recovery::report_local_fault(&controller)
        .await
        .is_ok()
}

/// Validate source durability and placement before the connector performs I/O.
fn admit_source_contract(
    contract: SourceContract,
    delivery: DeliveryGuarantee,
    checkpointing_enabled: bool,
    runtime: RuntimeMode,
) -> Result<(), &'static str> {
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
            SourceTopology::NodeLocalIngress
                if delivery == DeliveryGuarantee::BestEffort
                    && contract.consistency == SourceConsistency::Ephemeral => {}
            SourceTopology::NodeLocalIngress if delivery == DeliveryGuarantee::BestEffort => {
                return Err(
                    "cluster node-local ingress must be ephemeral because node-local recovery cursors cannot form one global replay cut",
                );
            }
            SourceTopology::NodeLocalIngress => {
                return Err(
                    "cluster node-local ingress is supported only with best-effort delivery",
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
            SinkTopology::NodeLocalEgress if delivery == DeliveryGuarantee::BestEffort => {}
            SinkTopology::NodeLocalEgress => {
                return Err(
                    "cluster node-local egress is supported only with best-effort delivery",
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

async fn close_opened_sinks(sinks: &mut [PreparedSink], cleanup_timeout: std::time::Duration) {
    for prepared in sinks.iter_mut().rev() {
        let cleanup_deadline = tokio::time::Instant::now() + cleanup_timeout;
        let cancellation_policy = prepared.connector.cancellation_policy();
        let mut close = std::pin::pin!(prepared.connector.close());
        match tokio::time::timeout_at(cleanup_deadline, close.as_mut()).await {
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
                    "sink close exceeded its pipeline-startup cleanup deadline"
                );
                if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
                    if let Err(error) = close.await {
                        tracing::warn!(
                            sink = %prepared.name,
                            %error,
                            "cancellation-unsafe sink close failed after the cleanup deadline"
                        );
                    }
                }
            }
        }
    }
}

async fn open_prepared_sinks(
    sinks: &mut [PreparedSink],
    open_timeout: std::time::Duration,
) -> Result<(), DbError> {
    let open_deadline = tokio::time::Instant::now() + open_timeout;
    let mut index = 0;
    while index < sinks.len() {
        if tokio::time::Instant::now() >= open_deadline {
            // Tokio's timeout polls its inner future once even at an expired deadline. Do not
            // construct or poll another connector open after the shared startup budget is gone.
            close_opened_sinks(
                &mut sinks[..index],
                crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
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
            let mut open = std::pin::pin!(prepared.connector.open(&prepared.config));
            match tokio::time::timeout_at(open_deadline, open.as_mut()).await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => {
                    if cancellation_policy == ConnectorCancellationPolicy::CompleteStarted {
                        match open.as_mut().await {
                            Ok(()) => tracing::warn!(
                                sink = %name,
                                "cancellation-unsafe sink open completed after its deadline"
                            ),
                            Err(error) => tracing::warn!(
                                sink = %name,
                                %error,
                                "cancellation-unsafe sink open failed after its deadline"
                            ),
                        }
                    }
                    Some(format!(
                        "exceeded the shared {open_timeout:?} sink-open stage deadline"
                    ))
                }
            }
        };
        if let Some(error) = open_error {
            // A failed/cancelled open may already hold resources, so include the current sink.
            close_opened_sinks(
                &mut sinks[..=index],
                crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
            )
            .await;
            return Err(DbError::Connector(format!(
                "Failed to open sink '{name}': {error}"
            )));
        }
        index += 1;
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

impl LaminarDB {
    /// Shut down the database gracefully.
    pub fn close(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Stage each boot-owned vnode's chain at the exact recovered attempt the source offsets
    /// resume from. A missing backend is fatal (offsets staged, state absent).
    #[cfg(feature = "cluster")]
    async fn stage_owned_vnodes_from_chains(
        &self,
        attempt: laminar_core::state::CheckpointAttempt,
    ) -> Result<(), DbError> {
        let Some(self_id) = self
            .cluster_controller
            .lock()
            .as_ref()
            .map(|c| c.instance_id())
        else {
            return Err(DbError::Checkpoint(
                "[LDB-6031] cluster recovery has no live cluster controller".into(),
            ));
        };
        let owned = match self.vnode_registry.lock().as_ref() {
            Some(registry) => laminar_core::state::owned_vnodes(registry, self_id),
            None => {
                return Err(DbError::Checkpoint(
                    "[LDB-6031] cluster recovery has no vnode registry".into(),
                ));
            }
        };
        tracing::info!(
            owned = owned.len(),
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            "cluster recovery: rehydrating boot-owned vnodes from chains"
        );
        if owned.is_empty() {
            return Ok(());
        }
        let Some(backend) = self.state_backend.lock().clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6031] cluster recovery requires a durable state backend but none is \
                 wired — refusing to start with staged source offsets and empty aggregate state"
                    .to_string(),
            ));
        };
        let report = crate::recovery_manager::VnodeRehydrator::new(backend.as_ref())
            .rehydrate_at(&owned, attempt)
            .await?;
        let mut staged = self.rehydrated_vnode_state.lock();
        for (vnode, chain) in report.restored {
            staged.insert(
                vnode,
                crate::db::RehydratedVnode {
                    epoch: attempt.epoch,
                    chain,
                },
            );
        }
        tracing::info!(
            staged = staged.len(),
            epoch = attempt.epoch,
            checkpoint_id = attempt.checkpoint_id,
            "cluster recovery: staged boot-owned vnodes for aggregate recovery"
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
        self.source_gate
            .store(closed, std::sync::atomic::Ordering::SeqCst);
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

    /// Whether clustered source and shuffle intake is still fenced.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn cluster_intake_fenced(&self) -> bool {
        self.source_gate.load(std::sync::atomic::Ordering::Acquire)
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
        let active = match tokio::time::timeout_at(deadline, controller.observe_recover()).await {
            Err(_) => {
                return Err(DbError::Checkpoint(
                    "cluster startup recovery authority read timed out".into(),
                ));
            }
            Ok(active) => active,
        };
        let active = match active {
            Ok(active) => active,
            Err(error) => {
                controller.set_recovering(true);
                tracing::error!(%error, "startup recovery authority is not currently valid");
                tokio::time::timeout_at(
                    deadline,
                    crate::coordinated_recovery::report_local_fault(&controller),
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
                    crate::coordinated_recovery::report_local_fault(&controller),
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
                crate::coordinated_recovery::report_local_fault(&controller),
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
                tokio::time::timeout_at(deadline, coordinator.reconcile_prepared_on_init())
                    .await
                    .map_err(|_| {
                        DbError::Checkpoint(format!(
                            "failed-start checkpoint reconciliation exceeded {CLEANUP_TIMEOUT:?}; durability fences remain held"
                        ))
                    })??;
                coordinator.clear_sinks();
            }
        }
        *self.control_tx.lock() = None;
        *self.force_ckpt_tx.lock() = None;
        self.quiesce_connector_generation_until(deadline).await?;
        *self.exact_deployment_lock.lock() = None;
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
        let (sources, sinks) = tokio::join!(
            self.quiesce_owned_source_tasks_until(deadline),
            self.quiesce_owned_sink_handles_until(deadline),
        );
        match (sources, sinks) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
            (Err(source_error), Err(sink_error)) => Err(DbError::Connector(format!(
                "connector generation remains fenced: sources: {source_error}; sinks: {sink_error}"
            ))),
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

        // Signal every task before awaiting any one task. Cancel-safe tasks may be aborted, but
        // their lease remains fenced until the stable supervisor observes the JoinHandle exit.
        for task in &tasks {
            task.request_shutdown();
            task.abort_if_cancel_safe();
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
        let handles = self.owned_sink_handles.lock().clone();
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

        // Poll every close in the same turn so independent actors share one restart budget. The
        // DB-owned registry is never moved across this await; cancellation leaves every fence in
        // place and each close that started continues in its terminal driver.
        let closes = futures::future::join_all(handles.iter().cloned().map(|handle| async move {
            let name = handle.name().to_owned();
            let result = handle.close().await;
            (name, result)
        }));
        let results = tokio::time::timeout_at(deadline, closes).await;
        let mut failures = match results {
            Ok(results) => results
                .into_iter()
                .filter_map(|(name, result)| result.err().map(|error| format!("{name}: {error}")))
                .collect::<Vec<_>>(),
            Err(_) => vec!["shared sink-generation quiescence deadline expired".to_string()],
        };

        let unresolved_names = {
            let mut owned = self.owned_sink_handles.lock();
            owned.retain(|handle| handle.has_unresolved_task());
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
        if self.is_closed() {
            return Err(DbError::Shutdown);
        }
        self.ensure_catalog_cleanup_unfenced("pipeline start")?;
        self.connector_registry.freeze();
        let runtime = self.control_runtime.handle()?;
        let attempt = {
            let mut owned = self.startup_attempt.lock();
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
                        let attempt = Arc::new(StartupAttempt::new());
                        // Publish ownership before Starting so stop/shutdown can always find the
                        // exact attempt they must await.
                        *owned = Some(Arc::clone(&attempt));
                        let (start_tx, start_rx) = std::sync::mpsc::sync_channel(1);
                        let db = Arc::clone(self);
                        let driver_attempt = Arc::clone(&attempt);
                        let emergency_attempt = Arc::clone(&attempt);
                        let driver_runtime = runtime.clone();
                        let owner = match std::thread::Builder::new()
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
                            Ok(owner) => owner,
                            Err(error) => {
                                *owned = None;
                                return Err(DbError::Pipeline(format!(
                                    "failed to spawn startup owner thread: {error}"
                                )));
                            }
                        };
                        // The attempt is the durable owner; the short-lived OS thread is detached.
                        drop(owner);
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
        let result = std::panic::AssertUnwindSafe(self.run_claimed_start(starting_from_fault))
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

    #[allow(clippy::too_many_lines)]
    async fn start_inner(&self) -> Result<(), DbError> {
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
            && self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && (self.config.object_store_url.is_some() || has_injected_decision_store)
        {
            return Err(DbError::Config(
                "[LDB-0014] local exactly-once with a shared/object-store checkpoint namespace \
                 or an injected decision store is not admitted until the deployment lease is \
                 term-fenced. Use the built-in local checkpoint directory (node-durable), or \
                 at_least_once delivery"
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
                self.config.object_store_url.as_deref().map_or(
                    StateBackendDurability::NodeDurable,
                    StateBackendDurability::for_storage_url,
                )
            };
            let required = required_recovery_scope(startup_runtime);
            if !checkpoint_scope.satisfies(required) {
                return Err(DbError::Config(format!(
                    "[LDB-5036] {startup_runtime:?} {:?} delivery requires {required:?} \
                     checkpoint/decision storage, but the configured checkpoint store is \
                     {checkpoint_scope:?}",
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
            if cp_config.max_staged_bytes == Some(0) {
                return Err(DbError::Config(
                    "checkpoint.max_staged_bytes must be greater than zero".into(),
                ));
            }
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
            if startup_runtime == RuntimeMode::Local
                && self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
                && self.exact_deployment_lock.lock().is_none()
            {
                std::fs::create_dir_all(&data_dir).map_err(|error| {
                    DbError::Config(format!(
                        "create exactly-once checkpoint directory {}: {error}",
                        data_dir.display()
                    ))
                })?;
                let lock_path = data_dir.join(".laminardb-exact.lock");
                let lock = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&lock_path)
                    .map_err(|error| {
                        DbError::Config(format!(
                            "[LDB-0014] open exactly-once deployment lock {}: {error}",
                            lock_path.display()
                        ))
                    })?;
                lock.try_lock().map_err(|error| {
                    DbError::Config(format!(
                        "[LDB-0014] exactly-once checkpoint namespace {} is already owned by \
                         another live process: {error}",
                        data_dir.display()
                    ))
                })?;
                *self.exact_deployment_lock.lock() = Some(lock);
            }
            let participant = self.checkpoint_participant();
            let participant_id = participant.unwrap_or(0);
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
                key_group_count.get(),
                participant.is_some(),
            );
            let pipeline_identity = crate::pipeline_identity::compute(&identity_context)?;

            let (store, decision_backing): (
                Box<dyn laminar_core::storage::CheckpointStore>,
                Arc<dyn object_store::ObjectStore>,
            ) = if let Some(obj) = injected_cluster_checkpoint_store.as_ref() {
                let cs = laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    Arc::clone(obj),
                    participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
                )
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), Arc::clone(obj))
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
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), obj)
            } else {
                std::fs::create_dir_all(&data_dir).map_err(|e| {
                    DbError::Config(format!("data dir {}: {e}", data_dir.display()))
                })?;
                let obj: Arc<dyn object_store::ObjectStore> = Arc::new(
                    object_store::local::LocalFileSystem::new_with_prefix(&data_dir)
                        .map_err(|e| DbError::Config(format!("local fs: {e}")))?,
                );
                let checkpoint_dir = participant.map_or(data_dir.clone(), |id| {
                    data_dir.join("nodes").join(id.to_string())
                });
                let cs = laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(
                    checkpoint_dir,
                )
                .with_key_group_count(key_group_count)
                .with_participant_id(participant_id);
                (Box::new(cs), obj)
            };

            let defaults = CkpConfig::default();
            let config = CkpConfig {
                max_retained,
                checkpoint_timeout: cp_config.timeout_ms.map_or(
                    defaults.checkpoint_timeout,
                    std::time::Duration::from_millis,
                ),
                max_staged_bytes: cp_config
                    .max_staged_bytes
                    .unwrap_or(defaults.max_staged_bytes),
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
                    self.decision_store.lock().clone().unwrap_or_else(|| {
                        Arc::new(if startup_runtime == RuntimeMode::Local {
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::local_single_writer(
                                Arc::clone(&decision_backing),
                            )
                        } else {
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                                Arc::clone(&decision_backing),
                            )
                        })
                    })
                }
                #[cfg(not(feature = "cluster"))]
                {
                    Arc::new(
                        laminar_core::checkpoint_decision::CheckpointDecisionStore::local_single_writer(
                            Arc::clone(&decision_backing),
                        ),
                    )
                }
            };
            let deployment_id = ds.load_or_create_deployment_id().await.map_err(|error| {
                DbError::Checkpoint(format!(
                    "load/create durable deployment identity before checkpoint startup: {error}"
                ))
            })?;
            coord.set_decision_store(ds)?;
            coord.bind_deployment_id(deployment_id.clone())?;

            if let (Some(backend), Some(registry)) = (
                self.state_backend.lock().clone(),
                self.vnode_registry.lock().clone(),
            ) {
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

    /// Build and start the unified pipeline with sources, sinks, and streams.
    #[allow(clippy::too_many_lines)]
    async fn start_connector_pipeline(
        &self,
        source_regs: HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: HashMap<String, crate::connector_manager::TableRegistration>,
        has_external: bool,
    ) -> Result<(), DbError> {
        use crate::connector_manager::{
            build_sink_config, build_source_config, build_table_config,
        };
        use crate::operator_graph::OperatorGraph;
        use crate::pipeline::{PipelineConfig, SourceRegistration};
        use laminar_connectors::connector::SourceConnector as _;

        let runtime_mode = self.runtime_mode();

        #[cfg(feature = "cluster")]
        let startup_generation_fence = if runtime_mode == RuntimeMode::Cluster {
            let generation_fence = Arc::clone(&self.rotation_execution_fence)
                .write_owned()
                .await;
            self.rehydrated_vnode_state.lock().clear();
            self.pending_revoke_vnodes.lock().clear();
            Some(generation_fence)
        } else {
            None
        };

        self.revalidate_persisted_cluster_query_shapes(&stream_regs)
            .await?;

        let checkpointing_enabled = self.config.checkpoint.is_some();
        let pipeline_checkpoint_timeout = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|config| config.timeout_ms)
            .map_or(
                crate::checkpoint_coordinator::CheckpointConfig::default().checkpoint_timeout,
                std::time::Duration::from_millis,
            );

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
                graph.set_rehydration_handle(Arc::clone(&self.rehydrated_vnode_state));
                graph.set_revoke_handle(Arc::clone(&self.pending_revoke_vnodes));
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

        for name in source_regs.keys() {
            if let Some(entry) = self.catalog.get_source(name) {
                graph.register_source_schema(name.clone(), entry.schema.clone());
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

        let prom_registry = self.prometheus_registry.lock().clone();

        let mut sources: Vec<SourceRegistration> = Vec::new();
        for (name, reg) in &source_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let mut config = build_source_config(reg)?;

            if let Some(entry) = self.catalog.get_source(name) {
                let schema_str = crate::pipeline_callback::encode_arrow_schema(&entry.schema);
                config.set("_arrow_schema".to_string(), schema_str);
            }

            #[cfg_attr(not(feature = "cluster"), allow(unused_mut))]
            let mut source = self
                .connector_registry
                .create_source(&config, prom_registry.as_ref())
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create source '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;
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
            sources.push(SourceRegistration {
                name: name.clone(),
                connector: source,
                config,
                contract,
                assignment_scoped,
                position: laminar_connectors::connector::SourcePosition::Initial,
            });
        }

        let bridged_names: rustc_hash::FxHashSet<String> =
            sources.iter().map(|s| s.name.clone()).collect();
        for (name, reg) in &source_regs {
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
                sources.push(SourceRegistration {
                    name: name.clone(),
                    connector: Box::new(connector),
                    config,
                    contract,
                    assignment_scoped: false,
                    position: laminar_connectors::connector::SourcePosition::Initial,
                });
            }
        }
        for name in self.catalog.list_sources() {
            if bridged_names.contains(&name) || source_regs.contains_key(&name) {
                continue;
            }
            if let Some(entry) = self.catalog.get_source(&name) {
                graph.register_source_schema(name.clone(), entry.schema.clone());
                let subscription = entry.sink.subscribe();
                let connector = crate::catalog_connector::CatalogSourceConnector::new(
                    subscription,
                    entry.schema.clone(),
                    entry.data_notify(),
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
                sources.push(SourceRegistration {
                    name: name.clone(),
                    connector: Box::new(connector),
                    config,
                    contract,
                    assignment_scoped: false,
                    position: laminar_connectors::connector::SourcePosition::Initial,
                });
            }
        }

        let stream_output_schemas = resolve_stream_output_schemas(&self.ctx, &stream_regs).await?;
        {
            let mut schemas = self.stream_schemas.write();
            schemas.clear();
            schemas.extend(
                stream_output_schemas
                    .iter()
                    .map(|(k, v)| (k.clone(), Arc::clone(v))),
            );
        }

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
                for (name, reg) in &stream_regs {
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
        for (name, reg) in &sink_regs {
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
                .create_sink(&config, prom_registry.as_ref())
                .map_err(|e| {
                    DbError::Connector(format!(
                        "Cannot create sink '{}' (type '{}'): {e}",
                        name,
                        config.connector_type()
                    ))
                })?;

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
            });
        }

        // Opening is one atomic startup stage: a slow connector consumes the remaining shared
        // checkpoint-derived budget rather than receiving a fresh timeout of its own.
        open_prepared_sinks(&mut prepared_sinks, pipeline_checkpoint_timeout).await?;

        #[allow(clippy::type_complexity)]
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
                config: _,
            } = prepared;
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
                });
            {
                let mut owned = self.owned_sink_handles.lock();
                debug_assert!(!owned.iter().any(|known| known.same_actor(&handle)));
                owned.push(handle.clone());
            }
            sinks.push((name, handle, filter_expr, input, contract));
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
            let guard = self.coordinator.lock().await;
            if let Some(ref coord) = *guard {
                // Cluster recovery never synthesizes a decision. Resolve the highest Prepared
                // participant artifact against the shared authority before selecting a
                // coordinated cut (especially a targeted rewind).
                if runtime_mode == RuntimeMode::Cluster {
                    coord.reconcile_prepared_on_init().await?;
                }
            }
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
                        for src in &mut sources {
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
                        for source in &sources {
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
                        for src in &mut sources {
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
                        let table_checkpoint = recovered
                            .manifest
                            .operator_states
                            .get(crate::table_store::REFERENCE_TABLE_CHECKPOINT_KEY);
                        let has_reference_tables =
                            !self.table_store.read().table_names().is_empty();
                        match (has_reference_tables, table_checkpoint) {
                            (true, Some(state)) => {
                                let bytes = state.decode_inline().ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "reference-table checkpoint is not inline after sidecar resolution"
                                            .into(),
                                    )
                                })?;
                                restored_reference_tables =
                                    self.table_store.write().restore_checkpoint(&bytes)?;
                                if !restored_reference_tables {
                                    return Err(DbError::Checkpoint(
                                        "reference-table checkpoint did not cover the complete catalog"
                                            .into(),
                                    ));
                                }
                            }
                            (true, None) => {
                                return Err(DbError::Checkpoint(format!(
                                    "recovered checkpoint {} has no atomic reference-table state",
                                    recovered.manifest.checkpoint_id
                                )));
                            }
                            (false, Some(_)) => {
                                return Err(DbError::Checkpoint(
                                    "recovered checkpoint contains reference-table state but the catalog has no tables"
                                        .into(),
                                ));
                            }
                            (false, None) => {}
                        }
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
                            self.stage_owned_vnodes_from_chains(recovered_attempt)
                                .await?;
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
                        source_watermark_recovery_selected = true;
                        tracing::info!("No checkpoint found, starting fresh");
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
                #[cfg(feature = "cluster")]
                if source_watermark_recovery_selected {
                    startup_reconciled_source_handoff_version =
                        self.vnode_registry.lock().as_ref().and_then(|registry| {
                            registry
                                .versioned_snapshot()
                                .source_handoff_installed_version()
                        });
                }
            }
        }

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

        let table_sources = create_reference_table_sources(
            &self.connector_registry,
            &table_regs,
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
            let guard = self.coordinator.lock().await;
            if let Some(ref coord) = *guard {
                // Recovery and Prepared-manifest reconciliation completed before epoch admission.
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

        for (name, reg) in &table_regs {
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

        let max_poll = self.config.default_buffer_size.min(1024);
        let checkpoint_interval = self
            .config
            .checkpoint
            .as_ref()
            .and_then(|c| c.interval_ms)
            .map(std::time::Duration::from_millis);

        tracing::info!(
            sources = sources.len(),
            sinks = sinks.len(),
            streams = stream_regs.len(),
            watermark_sources = source_ids.len(),
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
            checkpoint_interval,
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

        if pipeline_config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && pipeline_config.checkpoint_interval.is_none()
        {
            return Err(DbError::Config(
                "[LDB-5032] exactly-once requires checkpointing to be enabled. \
                     Set checkpoint.interval.ms in the pipeline configuration."
                    .into(),
            ));
        }

        let shutdown = self.shutdown_signal.clone();

        let pipeline_watermark = Arc::clone(&self.pipeline_watermark);
        let coordinator = Arc::clone(&self.coordinator);
        let table_store_for_loop = self.table_store.clone();
        graph.set_query_budget_ns(pipeline_config.query_budget_ns);
        graph.set_max_input_buf_batches(pipeline_config.max_input_buf_batches);
        graph.set_max_input_buf_bytes(pipeline_config.max_input_buf_bytes);
        graph.set_backpressure_policy(pipeline_config.backpressure_policy);
        graph.set_shared_source_isolation(
            pipeline_config.shared_source_isolation,
            pipeline_config.max_replay_buffer_bytes,
        );

        let sinks_pending_filter_count = sinks
            .iter()
            .filter(|(_, _, filter_sql, _, _)| filter_sql.is_some())
            .count();

        let source_name_arcs: rustc_hash::FxHashMap<usize, Arc<str>> = source_ids
            .iter()
            .map(|(name, &sid)| (sid, Arc::<str>::from(name.as_str())))
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

        let (force_ckpt_tx, force_ckpt_rx) = crossfire::mpsc::bounded_async::<
            crate::db::ForceCheckpointReply,
        >(crate::db::FORCE_CHECKPOINT_CHANNEL_CAPACITY);
        *self.force_ckpt_tx.lock() = Some(force_ckpt_tx);

        let (checkpoint_complete_tx, checkpoint_complete_rx) =
            crossfire::mpsc::bounded_async::<crate::pipeline::CheckpointCompletion>(16);
        // Checkpoint tails are serialized. Completion currently has no admission-ordered
        // sequencer, so allowing multiple durable tails could acknowledge epoch N+1 before N and
        // move a source cursor backwards when N finishes later.
        let has_checkpoint_committable_sink = sinks
            .iter()
            .any(|(_, handle, _, _, _)| handle.checkpoint_committable());
        let checkpoint_in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let staged_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (
            epoch_allocator,
            ckpt_quorum_timeout,
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            max_staged_bytes,
            coordinated_commit_admission,
        ) = {
            let guard = coordinator.lock().await;
            match guard.as_ref() {
                Some(coord) => {
                    let cfg = coord.config();
                    (
                        Some(coord.epoch_allocator()),
                        cfg.quorum_timeout,
                        cfg.checkpoint_timeout,
                        cfg.cleanup_timeout,
                        cfg.max_staged_bytes.max(1), // 0 would pause admission permanently
                        coord.coordinated_commit_admission(),
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
        let _ = ckpt_quorum_timeout;

        let named_stream_names: rustc_hash::FxHashSet<Arc<str>> = stream_entries
            .iter()
            .map(|entry| Arc::from(entry.name.as_str()))
            .collect();

        // Snapshot the controller once: locking the same `parking_lot::Mutex` twice
        // within the struct literal below would deadlock (the first guard lives until
        // the statement ends).
        #[cfg(feature = "cluster")]
        let callback_controller = self.cluster_controller.lock().clone();
        #[cfg(feature = "cluster")]
        let callback_vnode_registry = self.vnode_registry.lock().clone();
        #[cfg(feature = "cluster")]
        let (
            callback_shuffle_delivery_loss_incidents,
            callback_shuffle_recovered_delivery_loss_incidents,
        ) = self
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
            source_entries_for_wm,
            source_ids,
            source_name_arcs,
            source_wms_buf,
            tracker,
            prom,
            pipeline_watermark,
            coordinator,
            table_store: table_store_for_loop,
            mv_store_has_any: self.mv_store.read().has_any_handle(),
            mv_store: self.mv_store.clone(),
            filter_ctx: laminar_sql::create_session_context(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles: sinks_pending_filter_count,
            delivery_guarantee: pipeline_config.delivery_guarantee,
            serialization_timeout: checkpoint_timeout,
            checkpoint_state_cap_bytes: max_staged_bytes,
            checkpoint_serialization_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            sink_event_rx,
            sink_timed_out: false,
            sink_fault: None,
            checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
            shutdown_signal: Arc::clone(&self.shutdown_signal),
            #[cfg(feature = "cluster")]
            vnode_registry: callback_vnode_registry,
            #[cfg(feature = "cluster")]
            reconciled_source_handoff_version: startup_reconciled_source_handoff_version,
            #[cfg(feature = "cluster")]
            cluster_controller: callback_controller,
            #[cfg(feature = "cluster")]
            follower_tail: Arc::default(),
            #[cfg(feature = "cluster")]
            barrier_injectors: Vec::new(),
            #[cfg(feature = "cluster")]
            shuffle_delivery_loss_incidents: callback_shuffle_delivery_loss_incidents,
            #[cfg(feature = "cluster")]
            shuffle_recovered_delivery_loss_incidents:
                callback_shuffle_recovered_delivery_loss_incidents,
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
            staged_bytes: Arc::clone(&staged_bytes),
            #[cfg(feature = "cluster")]
            delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(feature = "cluster")]
            last_vnode_capture_epoch: None,
            epoch_allocator,
            #[cfg(feature = "cluster")]
            quorum_timeout: ckpt_quorum_timeout,
            checkpoint_committable_sinks: has_checkpoint_committable_sink,
            #[cfg(feature = "cluster")]
            intake_gate: Arc::clone(&self.source_gate),
            #[cfg(not(feature = "cluster"))]
            intake_gate: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        {
            let (control_tx, control_rx) =
                crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
            *self.control_tx.lock() = Some(control_tx);

            #[cfg(feature = "cluster")]
            let source_gate = Arc::clone(&self.source_gate);
            #[cfg(not(feature = "cluster"))]
            let source_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let coordinator = crate::pipeline::StreamingCoordinator::new_with_source_registry(
                sources,
                pipeline_config,
                Arc::clone(&shutdown),
                control_rx,
                source_gate,
                Arc::clone(&self.owned_source_tasks),
                runtime_mode == RuntimeMode::Cluster,
            )
            .await?
            .with_force_checkpoint_rx(force_ckpt_rx)
            .with_checkpoint_complete_rx(checkpoint_complete_rx)
            .with_checkpoint_admission(checkpoint_in_flight, staged_bytes, max_staged_bytes)
            .with_coordinated_commit_admission(coordinated_commit_admission);

            let (done_tx, done_rx) = crossfire::oneshot::oneshot::<crate::pipeline::ExitReason>();
            let (startup_tx, startup_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
            // Captured by the compute thread so an operator panic is recorded
            // (surfaced via pipeline status) rather than only logged.
            let fault_slot = Arc::clone(&self.last_fault);
            let fault_state = Arc::clone(&self.state);
            let fault_metrics = self.engine_metrics.lock().clone();
            match std::thread::Builder::new()
                .name("laminar-compute".into())
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
                    match &exit {
                        crate::pipeline::ExitReason::Shutdown => {}
                        crate::pipeline::ExitReason::Fault(reason) => {
                            tracing::error!(
                                reason = %reason,
                                "pipeline faulted on a fatal cycle error; recovering from last checkpoint"
                            );
                            // Publish before notifying the watcher. This closes the ready-send ->
                            // watcher-scheduled window in which start() could otherwise report
                            // Running after the compute loop had already exited.
                            *fault_slot.lock() = Some(reason.clone());
                            DbState::Faulted.store(&fault_state);
                            if let Some(ref m) = fault_metrics {
                                m.pipeline_faults_total.inc();
                            }
                        }
                    }
                    done_tx.send(exit);
                }) {
                Ok(_) => {}
                Err(e) => {
                    return Err(DbError::Config(format!(
                        "failed to spawn compute thread: {e}"
                    )));
                }
            }

            match startup_rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(DbError::Config(e)),
                Err(_) => {
                    return Err(DbError::Config(
                        "compute thread exited before entering the runtime control loop".into(),
                    ));
                }
            }

            // Readiness transfers the recovered MV image and fully wired graph to the live loop.
            // Release only now: a subsequent assignment may safely stage into the graph's shared
            // handles, and a first cycle already waiting on the read fence can then proceed.
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
                        DbState::Faulted.store(&watcher_state);
                        watcher_shutdown.notify_one();
                        // Cluster mode: report the fault and let the leader drive a global
                        // restart; the monitor restores this node. A local restart would rewind
                        // only this node while peers advanced — an inconsistent cut.
                        #[cfg(feature = "cluster")]
                        if report_cluster_compute_fault(watcher_controller).await {
                            return;
                        }
                        // Auto-restart if supervised; otherwise the pipeline stays Faulted.
                        let supervised = watcher_supervisor.lock().upgrade();
                        if let Some(db) = supervised {
                            let _ = spawn_supervised_restart(
                                db,
                                watcher_restart_history,
                                watcher_metrics,
                            );
                        }
                    }
                }
            });

            *self.runtime_handle.lock().await = Some(handle);
        }

        // Start the designated committer only after recovery, reconciliation,
        // source restoration, and the compute runtime all succeeded. Its first
        // pass runs immediately to close any recovery/external-commit crash window. New
        // checkpoints wake it directly; the interval is only a safety net.
        if let Some(mut committer) = coordinated_committer {
            let state = Arc::clone(&self.state);
            let handle = tokio::spawn(async move {
                let mut tick = tokio::time::interval(committer_poll);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                tick.tick().await;

                if let Err(e) = committer.commit_ready().await {
                    tracing::warn!(error = %e, "initial coordinated committer pass failed; will retry");
                }
                loop {
                    tokio::select! {
                        () = committer_notify.notified() => {}
                        _ = tick.tick() => {}
                    }
                    if matches!(
                        DbState::load(&state),
                        DbState::Stopped | DbState::Faulted | DbState::Created
                    ) {
                        break;
                    }
                    if let Err(e) = committer.commit_ready().await {
                        tracing::warn!(error = %e, "coordinated committer pass failed; will retry");
                    }
                }
            });
            let mut guard = self.committer_handle.lock().await;
            if guard.is_some() {
                handle.abort();
                let _ = handle.await;
                return Err(DbError::Checkpoint(
                    "cannot start a coordinated committer while a prior generation is still owned"
                        .into(),
                ));
            }
            *guard = Some(handle);
        }
        Ok(())
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
        self.quiesce_connector_generation_until(deadline).await?;

        *self.exact_deployment_lock.lock() = None;
        DbState::Stopped.store(&self.state);
        watcher_error.map_or(Ok(()), Err)
    }

    /// Stop the streaming pipeline so it can be restarted.
    ///
    /// # Errors
    /// Returns [`DbError::InvalidOperation`] if the pipeline is still starting
    /// or the coordinator does not exit within the stop timeout.
    pub async fn stop_pipeline(&self) -> Result<(), DbError> {
        const STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
        let first_stop = loop {
            let startup = {
                let owned = self.startup_attempt.lock();
                if let Some(in_flight) = owned.as_ref().filter(|attempt| !attempt.is_complete()) {
                    Arc::clone(in_flight)
                } else {
                    match DbState::load(&self.state) {
                        DbState::Created | DbState::Stopped => return Ok(()),
                        DbState::Starting => {
                            return Err(DbError::InvalidOperation(
                                "pipeline stop found Starting without an incomplete owned startup attempt"
                                    .into(),
                            ));
                        }
                        DbState::ShuttingDown => break false,
                        observed @ (DbState::Running | DbState::Faulted) => {
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
        self.quiesce_connector_generation_until(deadline).await?;

        *self.exact_deployment_lock.lock() = None;
        if self.is_closed() {
            // A concurrent shutdown owns the terminal transition. Leaving ShuttingDown in place
            // is intentional if that shutdown was cancelled; a retry must finish its teardown.
            return Ok(());
        }
        match DbState::compare_exchange(DbState::ShuttingDown, DbState::Created, &self.state) {
            Ok(_) | Err(DbState::Created) | Err(DbState::Stopped) => Ok(()),
            Err(observed) => Err(DbError::InvalidOperation(format!(
                "pipeline stop completed from unexpected lifecycle state {observed:?}; restart remains fenced"
            ))),
        }
    }
}

#[cfg(test)]
mod connector_admission_tests {
    #[cfg(feature = "cluster")]
    use super::cluster_delta_chain_bound;
    use super::LaminarDB;
    use super::{
        admit_sink, admit_sink_contract, admit_source_contract, close_opened_sinks,
        open_prepared_sinks, validate_source_recovery_assignment, PreparedSink, RuntimeMode,
        SinkAdmissionContext, EXACT_SINK_PROTOCOL,
    };
    use crate::db::DbState;
    use crate::pipeline::PipelineConfig;
    use arrow_array::RecordBatch;
    use arrow_schema::{Schema, SchemaRef};
    use async_trait::async_trait;
    use laminar_connectors::config::ConnectorConfig;
    use laminar_connectors::connector::{
        ConnectorCancellationPolicy, DeliveryGuarantee, SinkConnector, SinkConsistency,
        SinkContract, SinkInputMode, SinkTopology, SourceConnector, SourceConsistency,
        SourceContract, SourceTopology, WriteResult,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_core::state::StateBackendDurability;
    use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[cfg(feature = "cluster")]
    #[test]
    fn cluster_delta_chain_bound_is_derived_from_retention() {
        assert_eq!(cluster_delta_chain_bound(0), None);
        assert_eq!(cluster_delta_chain_bound(1), None);
        assert_eq!(cluster_delta_chain_bound(2), Some(1));
        assert_eq!(cluster_delta_chain_bound(3), Some(2));
        assert_eq!(cluster_delta_chain_bound(4), Some(3));
        assert_eq!(cluster_delta_chain_bound(5), Some(4));
        assert_eq!(cluster_delta_chain_bound(usize::MAX), Some(4));
    }

    #[test]
    fn recovered_source_assignment_scope_fails_closed() {
        let expected = std::num::NonZeroU64::new(7).unwrap();
        let mut checkpoint = ConnectorCheckpoint::new();

        let error =
            validate_source_recovery_assignment("events", true, Some(&checkpoint), Some(expected))
                .unwrap_err();
        assert!(error.to_string().contains("missing its assignment version"));

        checkpoint.source_assignment_version = std::num::NonZeroU64::new(6);
        let error =
            validate_source_recovery_assignment("events", true, Some(&checkpoint), Some(expected))
                .unwrap_err();
        assert!(error.to_string().contains("committed fence is 7"));

        checkpoint.source_assignment_version = Some(expected);
        validate_source_recovery_assignment("events", true, Some(&checkpoint), Some(expected))
            .unwrap();

        let error = validate_source_recovery_assignment("events", true, Some(&checkpoint), None)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("no authoritative assignment fence"));

        let error = validate_source_recovery_assignment("local", false, Some(&checkpoint), None)
            .unwrap_err();
        assert!(error.to_string().contains("non-assigned source 'local'"));

        checkpoint.source_assignment_version = None;
        validate_source_recovery_assignment("local", false, Some(&checkpoint), None).unwrap();
        validate_source_recovery_assignment("local", false, Some(&checkpoint), Some(expected))
            .unwrap();
    }

    #[test]
    fn startup_transition_publishes_running_from_starting() {
        let db = LaminarDB::open().unwrap();
        DbState::Starting.store(&db.state);

        db.finish_start_transition().unwrap();

        assert_eq!(DbState::load(&db.state), DbState::Running);
    }

    #[test]
    fn startup_transition_fails_closed_when_compute_loop_faulted() {
        let db = LaminarDB::open().unwrap();
        DbState::Faulted.store(&db.state);
        *db.last_fault.lock() = Some("injected startup fault".into());

        let error = db.finish_start_transition().unwrap_err();

        assert!(
            error.to_string().contains("injected startup fault"),
            "unexpected startup error: {error}"
        );
        assert_eq!(DbState::load(&db.state), DbState::Faulted);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_and_concurrent_start_wait_for_one_owned_attempt() {
        let db = LaminarDB::open().unwrap();
        let topology = db.topology_ddl_lock.write().await;

        let first_db = Arc::clone(&db);
        let first = tokio::spawn(async move { first_db.start().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while DbState::load(&db.state) != DbState::Starting {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("startup attempt was not registered");
        let owned = db
            .startup_attempt
            .lock()
            .clone()
            .expect("Starting must publish its attempt first");

        first.abort();
        let _ = first.await;
        assert_eq!(DbState::load(&db.state), DbState::Starting);

        let second_db = Arc::clone(&db);
        let second = tokio::spawn(async move { second_db.start().await });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        assert!(Arc::ptr_eq(
            &owned,
            db.startup_attempt.lock().as_ref().unwrap()
        ));

        drop(topology);
        tokio::time::timeout(Duration::from_secs(5), second)
            .await
            .expect("owned startup did not finish")
            .expect("concurrent start task panicked")
            .expect("owned startup failed");
        assert_eq!(DbState::load(&db.state), DbState::Running);
        db.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_stop_cannot_downgrade_completed_shutdown() {
        let db = LaminarDB::open().unwrap();
        db.start().await.unwrap();

        // Hold the second lifecycle fence so shutdown owns ShuttingDown and the topology lock,
        // while the trailing stop deterministically records that it observed the same teardown.
        let lifecycle = db.lifecycle_lock.lock().await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        *db.stop_after_claim_gate.lock() = Some((Arc::clone(&entered), Arc::clone(&release)));

        let shutdown_db = Arc::clone(&db);
        let shutdown = tokio::spawn(async move { shutdown_db.shutdown().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while DbState::load(&db.state) != DbState::ShuttingDown {
                tokio::task::yield_now().await;
            }
            while db.topology_ddl_lock.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shutdown did not acquire lifecycle ownership");

        let stop_db = Arc::clone(&db);
        let stop = tokio::spawn(async move { stop_db.stop_pipeline().await });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .expect("stop did not observe the in-progress shutdown");

        drop(lifecycle);
        tokio::time::timeout(Duration::from_secs(5), shutdown)
            .await
            .expect("shutdown remained blocked")
            .expect("shutdown task panicked")
            .expect("shutdown failed");
        assert_eq!(DbState::load(&db.state), DbState::Stopped);

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), stop)
            .await
            .expect("trailing stop remained blocked")
            .expect("stop task panicked")
            .expect("trailing stop failed");
        assert_eq!(DbState::load(&db.state), DbState::Stopped);
        *db.stop_after_claim_gate.lock() = None;
    }

    #[cfg(feature = "cluster")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coordinated_recovery_supervisor_is_owned_and_replaces_a_terminated_task() {
        let db = LaminarDB::open().unwrap();
        let runtime = db.control_runtime.handle().unwrap();
        let finished = runtime.spawn(async {});
        tokio::time::timeout(Duration::from_secs(2), async {
            while !finished.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("injected recovery task did not terminate");
        *db.recovery_monitor.lock() = Some(finished);

        db.enable_coordinated_recovery().unwrap();
        assert!(db
            .recovery_monitor
            .lock()
            .as_ref()
            .is_some_and(|monitor| !monitor.is_finished()));

        db.shutdown().await.unwrap();
        assert!(db.recovery_monitor.lock().is_none());
    }

    #[tokio::test]
    async fn cancelled_source_quiescence_retains_generation_fence() {
        let db = LaminarDB::open().unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(async move {
            let _ = release_rx.await;
        });
        let lease = crate::pipeline::streaming_coordinator::SourceTaskLease::supervise(
            Arc::from("draining-source"),
            ConnectorCancellationPolicy::CompleteStarted,
            Arc::new(tokio::sync::Notify::new()),
            Arc::new(AtomicBool::new(false)),
            join,
            &tokio::runtime::Handle::current(),
        );
        db.owned_source_tasks.lock().push(lease.clone());

        let waiting_db = Arc::clone(&db);
        let quiesce = tokio::spawn(async move {
            waiting_db
                .quiesce_owned_source_tasks_until(
                    tokio::time::Instant::now() + Duration::from_secs(60),
                )
                .await
        });
        tokio::task::yield_now().await;
        quiesce.abort();
        let _ = quiesce.await;

        assert_eq!(db.owned_source_tasks.lock().len(), 1);
        assert!(!lease.is_finished());

        release_tx.send(()).unwrap();
        db.quiesce_owned_source_tasks_until(tokio::time::Instant::now() + Duration::from_secs(1))
            .await
            .expect("a retry must observe terminal completion and release the source fence");
        assert!(db.owned_source_tasks.lock().is_empty());
    }

    #[test]
    fn source_contract_admission_matrix_is_fail_closed() {
        let consistencies = [
            SourceConsistency::Ephemeral,
            SourceConsistency::Replayable,
            SourceConsistency::CommitCoupled,
        ];
        let topologies = [
            SourceTopology::Singleton,
            SourceTopology::Splittable,
            SourceTopology::NodeLocalIngress,
        ];
        let deliveries = [
            DeliveryGuarantee::BestEffort,
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ];
        let runtimes = [RuntimeMode::Local, RuntimeMode::Cluster];
        let certifications = [false, true];

        for consistency in consistencies {
            for topology in topologies {
                for delivery in deliveries {
                    for runtime in runtimes {
                        for checkpointing_enabled in [false, true] {
                            for certified in certifications {
                                let mut contract = if certified {
                                    laminar_connectors::generator::GeneratorSource::default()
                                        .contract(&ConnectorConfig::new("generator"))
                                        .expect("static generator contract")
                                } else {
                                    SourceContract::new(consistency, topology)
                                };
                                contract.consistency = consistency;
                                contract.topology = topology;
                                let expected = match consistency {
                                    SourceConsistency::Ephemeral => {
                                        delivery == DeliveryGuarantee::BestEffort
                                    }
                                    SourceConsistency::Replayable => true,
                                    SourceConsistency::CommitCoupled => {
                                        delivery == DeliveryGuarantee::AtLeastOnce
                                            && checkpointing_enabled
                                    }
                                };
                                let expected = expected
                                    && (delivery != DeliveryGuarantee::ExactlyOnce || certified)
                                    && !(runtime == RuntimeMode::Cluster
                                        && delivery == DeliveryGuarantee::ExactlyOnce)
                                    && (runtime != RuntimeMode::Cluster
                                        || topology == SourceTopology::Splittable
                                        || (topology == SourceTopology::NodeLocalIngress
                                            && delivery == DeliveryGuarantee::BestEffort
                                            && consistency == SourceConsistency::Ephemeral));

                                assert_eq!(
                                    admit_source_contract(
                                        contract,
                                        delivery,
                                        checkpointing_enabled,
                                        runtime,
                                    )
                                    .is_ok(),
                                    expected,
                                    "contract={contract:?}, delivery={delivery:?}, \
                                     checkpointing_enabled={checkpointing_enabled}, \
                                     runtime={runtime:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn cluster_singleton_is_rejected_even_for_best_effort() {
        let contract =
            SourceContract::new(SourceConsistency::Replayable, SourceTopology::Singleton);
        let error = admit_source_contract(
            contract,
            DeliveryGuarantee::BestEffort,
            true,
            RuntimeMode::Cluster,
        )
        .unwrap_err();
        assert!(error.contains("fenced singleton placement"));
    }

    #[test]
    fn cluster_node_local_replay_cursor_is_rejected() {
        let contract = SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::NodeLocalIngress,
        );
        let error = admit_source_contract(
            contract,
            DeliveryGuarantee::BestEffort,
            true,
            RuntimeMode::Cluster,
        )
        .unwrap_err();
        assert!(error.contains("must be ephemeral"), "{error}");
    }

    #[test]
    fn commit_coupled_exactly_once_requires_a_certified_barrier_cut() {
        let mut contract = laminar_connectors::generator::GeneratorSource::default()
            .contract(&ConnectorConfig::new("generator"))
            .expect("static generator contract");
        contract.consistency = SourceConsistency::CommitCoupled;
        let error = admit_source_contract(
            contract,
            DeliveryGuarantee::ExactlyOnce,
            true,
            RuntimeMode::Local,
        )
        .unwrap_err();

        assert!(error.contains("certified in-flight transaction/barrier checkpoint cut"));
    }

    #[test]
    fn deterministic_generator_is_admitted_for_local_exact_delivery() {
        let source = laminar_connectors::generator::GeneratorSource::default();
        let contract = source
            .contract(&ConnectorConfig::new("generator"))
            .expect("static generator contract");

        assert!(contract.is_exact_delivery_certified());
        admit_source_contract(
            contract,
            DeliveryGuarantee::ExactlyOnce,
            true,
            RuntimeMode::Local,
        )
        .expect("the certified deterministic generator must remain locally admissible");
    }

    #[cfg(feature = "mongodb-cdc")]
    #[test]
    fn mongodb_contract_is_rejected_for_exact_delivery() {
        let source = laminar_connectors::mongodb::MongoDbCdcSource::new(
            laminar_connectors::mongodb::MongoDbSourceConfig::new(
                "mongodb://localhost:27017",
                "production",
                "events",
            ),
            None,
        );
        let contract = source
            .contract(&ConnectorConfig::new("mongodb-cdc"))
            .expect("valid MongoDB contract");

        assert!(!contract.is_exact_delivery_certified());
        let error = admit_source_contract(
            contract,
            DeliveryGuarantee::ExactlyOnce,
            true,
            RuntimeMode::Local,
        )
        .expect_err("MongoDB event replay is not production-certified for exact delivery");
        assert!(error.contains(laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED));
    }

    #[cfg(feature = "kafka")]
    #[test]
    fn kafka_contract_is_rejected_for_exact_delivery() {
        let source = laminar_connectors::kafka::KafkaSource::new(
            Arc::new(Schema::empty()),
            laminar_connectors::kafka::KafkaSourceConfig::default(),
            None,
        );
        let contract = source
            .contract(&ConnectorConfig::new("kafka"))
            .expect("static Kafka contract");

        assert!(!contract.is_exact_delivery_certified());
        let error = admit_source_contract(
            contract,
            DeliveryGuarantee::ExactlyOnce,
            true,
            RuntimeMode::Local,
        )
        .expect_err("Kafka source recovery is not production-certified for exact delivery");
        assert!(error.contains(laminar_core::error_codes::EXACTLY_ONCE_SOURCE_UNCERTIFIED));
    }

    #[test]
    fn sink_contract_admission_matrix_is_fail_closed() {
        let consistencies = [
            SinkConsistency::Ephemeral,
            SinkConsistency::DurableAtLeastOnce,
            SinkConsistency::CheckpointCommittable,
        ];
        let topologies = [
            SinkTopology::Singleton,
            SinkTopology::MultiWriter,
            SinkTopology::NodeLocalEgress,
        ];
        let input_modes = [
            SinkInputMode::AppendOnly,
            SinkInputMode::KeyedUpsert,
            SinkInputMode::FullChangelog,
        ];
        let deliveries = [
            DeliveryGuarantee::BestEffort,
            DeliveryGuarantee::AtLeastOnce,
            DeliveryGuarantee::ExactlyOnce,
        ];
        let runtimes = [RuntimeMode::Local, RuntimeMode::Cluster];

        for consistency in consistencies {
            for topology in topologies {
                for input_mode in input_modes {
                    for delivery in deliveries {
                        for runtime in runtimes {
                            for carries_changelog in [false, true] {
                                let contract = SinkContract::new(consistency, topology, input_mode);
                                let durable = delivery != DeliveryGuarantee::AtLeastOnce
                                    || consistency != SinkConsistency::Ephemeral;
                                let placed = runtime != RuntimeMode::Cluster
                                    || topology == SinkTopology::MultiWriter
                                    || (topology == SinkTopology::NodeLocalEgress
                                        && delivery == DeliveryGuarantee::BestEffort);
                                let input_compatible =
                                    !carries_changelog || input_mode.accepts_full_changelog();
                                let protocol_compatible =
                                    if delivery == DeliveryGuarantee::ExactlyOnce {
                                        consistency == SinkConsistency::CheckpointCommittable
                                    } else {
                                        consistency != SinkConsistency::CheckpointCommittable
                                    };
                                let expected = protocol_compatible
                                    && durable
                                    && placed
                                    && input_compatible
                                    && !(runtime == RuntimeMode::Cluster
                                        && delivery == DeliveryGuarantee::ExactlyOnce);

                                assert_eq!(
                                    admit_sink_contract(
                                        contract,
                                        delivery,
                                        runtime,
                                        carries_changelog,
                                    )
                                    .is_ok(),
                                    expected,
                                    "contract={contract:?}, delivery={delivery:?}, \
                                     runtime={runtime:?}, carries_changelog={carries_changelog}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    struct OpenProbeSink {
        contract: SinkContract,
        opened: Arc<AtomicBool>,
        schema: SchemaRef,
        exact_protocol: bool,
    }

    struct StartupProbeSink {
        open_delay: Duration,
        close_delay: Duration,
        open_calls: Arc<AtomicU64>,
        close_calls: Arc<AtomicU64>,
        schema: SchemaRef,
        cancellation_policy: ConnectorCancellationPolicy,
    }

    #[async_trait]
    impl SinkConnector for StartupProbeSink {
        fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
            self.cancellation_policy
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
            Ok(SinkContract::new(
                SinkConsistency::DurableAtLeastOnce,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ))
        }

        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            self.open_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.open_delay).await;
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(0, 0))
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.close_delay).await;
            Ok(())
        }
    }

    fn prepared_startup_probe(
        name: &str,
        delay: Duration,
        open_calls: Arc<AtomicU64>,
        close_calls: Arc<AtomicU64>,
    ) -> PreparedSink {
        prepared_lifecycle_probe(name, delay, Duration::ZERO, open_calls, close_calls)
    }

    fn prepared_lifecycle_probe(
        name: &str,
        open_delay: Duration,
        close_delay: Duration,
        open_calls: Arc<AtomicU64>,
        close_calls: Arc<AtomicU64>,
    ) -> PreparedSink {
        prepared_lifecycle_probe_with_policy(
            name,
            open_delay,
            close_delay,
            open_calls,
            close_calls,
            ConnectorCancellationPolicy::CancelSafe,
        )
    }

    fn prepared_lifecycle_probe_with_policy(
        name: &str,
        open_delay: Duration,
        close_delay: Duration,
        open_calls: Arc<AtomicU64>,
        close_calls: Arc<AtomicU64>,
        cancellation_policy: ConnectorCancellationPolicy,
    ) -> PreparedSink {
        PreparedSink {
            name: name.into(),
            connector: Box::new(StartupProbeSink {
                open_delay,
                close_delay,
                open_calls,
                close_calls,
                schema: Arc::new(Schema::empty()),
                cancellation_policy,
            }),
            config: ConnectorConfig::new("startup-probe"),
            filter_expr: None,
            input: "input".into(),
            contract: SinkContract::new(
                SinkConsistency::DurableAtLeastOnce,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            write_timeout: Duration::from_secs(1),
            flush_interval: Duration::from_secs(5),
            requires_recovery_on_error: true,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn sink_open_stage_uses_one_deadline_and_rolls_back_current_and_prior() {
        let prior_open = Arc::new(AtomicU64::new(0));
        let prior_close = Arc::new(AtomicU64::new(0));
        let current_open = Arc::new(AtomicU64::new(0));
        let current_close = Arc::new(AtomicU64::new(0));
        let mut sinks = vec![
            prepared_startup_probe(
                "prior",
                Duration::from_secs(6),
                Arc::clone(&prior_open),
                Arc::clone(&prior_close),
            ),
            prepared_startup_probe(
                "current",
                Duration::from_secs(6),
                Arc::clone(&current_open),
                Arc::clone(&current_close),
            ),
        ];

        let error = open_prepared_sinks(&mut sinks, Duration::from_secs(10))
            .await
            .expect_err("the second sink must consume the remaining shared startup budget");

        let message = error.to_string();
        assert!(
            message.contains("Failed to open sink 'current'")
                && message.contains("shared 10s sink-open stage deadline"),
            "unexpected error: {error}"
        );
        assert_eq!(prior_open.load(Ordering::SeqCst), 1);
        assert_eq!(current_open.load(Ordering::SeqCst), 1);
        assert_eq!(prior_close.load(Ordering::SeqCst), 1);
        assert_eq!(current_close.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_sink_open_budget_never_polls_connector() {
        let open_calls = Arc::new(AtomicU64::new(0));
        let close_calls = Arc::new(AtomicU64::new(0));
        let mut sinks = vec![prepared_startup_probe(
            "unattempted",
            Duration::ZERO,
            Arc::clone(&open_calls),
            Arc::clone(&close_calls),
        )];

        let error = open_prepared_sinks(&mut sinks, Duration::ZERO)
            .await
            .expect_err("an expired shared budget must reject before polling open");

        assert!(
            error
                .to_string()
                .contains("deadline was exhausted before open began"),
            "unexpected error: {error}"
        );
        assert_eq!(open_calls.load(Ordering::SeqCst), 0);
        assert_eq!(close_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_unsafe_sink_open_finishes_before_startup_rollback() {
        let open_calls = Arc::new(AtomicU64::new(0));
        let close_calls = Arc::new(AtomicU64::new(0));
        let mut sinks = vec![prepared_lifecycle_probe_with_policy(
            "complete-started",
            Duration::from_secs(12),
            Duration::ZERO,
            Arc::clone(&open_calls),
            Arc::clone(&close_calls),
            ConnectorCancellationPolicy::CompleteStarted,
        )];
        let started = tokio::time::Instant::now();

        let error = open_prepared_sinks(&mut sinks, Duration::from_secs(10))
            .await
            .expect_err("late open must still fail the startup stage");

        assert!(error
            .to_string()
            .contains("shared 10s sink-open stage deadline"));
        assert_eq!(
            tokio::time::Instant::now() - started,
            Duration::from_secs(12)
        );
        assert_eq!(open_calls.load(Ordering::SeqCst), 1);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sink_startup_cleanup_gives_every_connector_a_terminal_attempt() {
        let first_close = Arc::new(AtomicU64::new(0));
        let second_close = Arc::new(AtomicU64::new(0));
        let mut sinks = vec![
            prepared_lifecycle_probe(
                "first",
                Duration::ZERO,
                PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT + Duration::from_secs(1),
                Arc::new(AtomicU64::new(0)),
                Arc::clone(&first_close),
            ),
            prepared_lifecycle_probe(
                "second",
                Duration::ZERO,
                PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT + Duration::from_secs(1),
                Arc::new(AtomicU64::new(0)),
                Arc::clone(&second_close),
            ),
        ];
        let started = tokio::time::Instant::now();
        close_opened_sinks(
            &mut sinks,
            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
        )
        .await;

        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT * 2,
            "each started connector must receive its own terminal cleanup budget"
        );
        assert_eq!(first_close.load(Ordering::SeqCst), 1);
        assert_eq!(second_close.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn coordinated_commit_is_rejected_under_at_least_once_before_open() {
        let opened = Arc::new(AtomicBool::new(false));
        let sink = OpenProbeSink {
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            opened: Arc::clone(&opened),
            schema: Arc::new(Schema::empty()),
            exact_protocol: true,
        };
        let config = ConnectorConfig::new("iceberg");

        let error = admit_sink(
            &sink,
            SinkAdmissionContext {
                config: &config,
                name: "iceberg_out",
                input: "input",
                delivery: DeliveryGuarantee::AtLeastOnce,
                runtime: RuntimeMode::Local,
                carries_changelog: false,
                checkpointing_enabled: true,
                state_backend_scope: StateBackendDurability::Volatile,
            },
        )
        .expect_err("the exact descriptor/cursor path must not activate under ALO");

        assert!(error.to_string().contains("require global exactly-once"));
        assert!(!opened.load(Ordering::SeqCst));
    }

    #[async_trait]
    impl SinkConnector for OpenProbeSink {
        fn contract(&self, _config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
            Ok(self.contract)
        }

        async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
            self.opened.store(true, Ordering::SeqCst);
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            Ok(WriteResult::new(0, 0))
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(1)
        }

        fn as_coordinated_committer(
            &self,
        ) -> Option<&dyn laminar_connectors::connector::CoordinatedCommitter> {
            self.exact_protocol.then_some(self)
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl laminar_connectors::connector::CoordinatedCommitter for OpenProbeSink {
        async fn commit_aggregated(
            &self,
            _batch: laminar_connectors::connector::CoordinatedCommitBatch,
            _context: laminar_connectors::connector::CoordinatedCommitContext,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn committed_cursor(
            &self,
            _namespace: &laminar_connectors::connector::CoordinatedCommitNamespace,
        ) -> Result<Option<laminar_connectors::connector::CoordinatedCommitCursor>, ConnectorError>
        {
            Ok(None)
        }
    }

    #[test]
    fn complete_exact_protocol_is_admitted_without_opening() {
        let opened = Arc::new(AtomicBool::new(false));
        let sink = OpenProbeSink {
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            opened: Arc::clone(&opened),
            schema: Arc::new(Schema::empty()),
            exact_protocol: true,
        };

        let config = ConnectorConfig::new("exact-probe");
        admit_sink(
            &sink,
            SinkAdmissionContext {
                config: &config,
                name: "exact_out",
                input: "input",
                delivery: DeliveryGuarantee::ExactlyOnce,
                runtime: RuntimeMode::Local,
                carries_changelog: false,
                checkpointing_enabled: true,
                state_backend_scope: StateBackendDurability::NodeDurable,
            },
        )
        .unwrap();
        assert!(!opened.load(Ordering::SeqCst));
    }

    #[test]
    fn checkpoint_committable_contract_without_committer_is_rejected_before_open() {
        let opened = Arc::new(AtomicBool::new(false));
        let sink = OpenProbeSink {
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            opened: Arc::clone(&opened),
            schema: Arc::new(Schema::empty()),
            exact_protocol: false,
        };

        let config = ConnectorConfig::new("incomplete-exact-probe");
        let error = admit_sink(
            &sink,
            SinkAdmissionContext {
                config: &config,
                name: "exact_out",
                input: "input",
                delivery: DeliveryGuarantee::ExactlyOnce,
                runtime: RuntimeMode::Local,
                carries_changelog: false,
                checkpointing_enabled: true,
                state_backend_scope: StateBackendDurability::NodeDurable,
            },
        )
        .unwrap_err();

        assert!(error.to_string().contains("does not implement"));
        assert!(!opened.load(Ordering::SeqCst));
    }

    #[test]
    fn exact_state_scope_is_runtime_aware_and_checked_before_open() {
        let cases = [
            (RuntimeMode::Local, StateBackendDurability::Volatile, false),
            (
                RuntimeMode::Local,
                StateBackendDurability::NodeDurable,
                true,
            ),
            (
                RuntimeMode::Cluster,
                StateBackendDurability::NodeDurable,
                false,
            ),
            (
                RuntimeMode::Cluster,
                StateBackendDurability::ClusterShared,
                false,
            ),
        ];

        for (runtime, scope, expected_admission) in cases {
            let opened = Arc::new(AtomicBool::new(false));
            let sink = OpenProbeSink {
                contract: SinkContract::new(
                    SinkConsistency::CheckpointCommittable,
                    SinkTopology::MultiWriter,
                    SinkInputMode::AppendOnly,
                ),
                opened: Arc::clone(&opened),
                schema: Arc::new(Schema::empty()),
                exact_protocol: true,
            };

            let config = ConnectorConfig::new("exact-probe");
            let result = admit_sink(
                &sink,
                SinkAdmissionContext {
                    config: &config,
                    name: "exact_out",
                    input: "input",
                    delivery: DeliveryGuarantee::ExactlyOnce,
                    runtime,
                    carries_changelog: false,
                    checkpointing_enabled: true,
                    state_backend_scope: scope,
                },
            );

            assert_eq!(
                result.is_ok(),
                expected_admission,
                "runtime={runtime:?}, scope={scope:?}, result={result:?}"
            );
            assert!(!opened.load(Ordering::SeqCst));
        }
    }

    #[test]
    fn exact_rejection_precedes_open_for_network_and_file_sink_contracts() {
        let candidates = [
            (
                "kafka",
                SinkContract::new(
                    SinkConsistency::DurableAtLeastOnce,
                    SinkTopology::Singleton,
                    SinkInputMode::AppendOnly,
                ),
            ),
            (
                "postgres",
                SinkContract::new(
                    SinkConsistency::DurableAtLeastOnce,
                    SinkTopology::Singleton,
                    SinkInputMode::AppendOnly,
                ),
            ),
            (
                "file",
                SinkContract::new(
                    SinkConsistency::DurableAtLeastOnce,
                    SinkTopology::Singleton,
                    SinkInputMode::AppendOnly,
                ),
            ),
        ];

        for (name, contract) in candidates {
            let opened = Arc::new(AtomicBool::new(false));
            let sink = OpenProbeSink {
                contract,
                opened: Arc::clone(&opened),
                schema: Arc::new(Schema::empty()),
                exact_protocol: false,
            };
            let config = ConnectorConfig::new(name);
            let error = admit_sink(
                &sink,
                SinkAdmissionContext {
                    config: &config,
                    name,
                    input: "input",
                    delivery: DeliveryGuarantee::ExactlyOnce,
                    runtime: RuntimeMode::Local,
                    carries_changelog: false,
                    checkpointing_enabled: true,
                    state_backend_scope: StateBackendDurability::NodeDurable,
                },
            )
            .unwrap_err();

            assert!(error.to_string().contains(EXACT_SINK_PROTOCOL));
            assert!(
                !opened.load(Ordering::SeqCst),
                "{name} was opened before admission"
            );
        }
    }
}

#[cfg(test)]
mod resolver_tests {
    use super::resolve_stream_output_schemas;
    use crate::connector_manager::StreamRegistration;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;
    use std::time::Duration;

    fn ctx_with_payments() -> SessionContext {
        let ctx = SessionContext::new_with_config(laminar_sql::datafusion::base_session_config());
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("method", DataType::Utf8, false),
            Field::new("amount_usd", DataType::Float64, false),
            Field::new("status", DataType::Utf8, false),
            Field::new(
                "event_time",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                false,
            ),
        ]));
        ctx.register_table("payments", Arc::new(EmptyTable::new(schema)))
            .unwrap();
        ctx.register_udf(datafusion_expr::ScalarUDF::from(
            laminar_sql::datafusion::TumbleWindowStart::new(),
        ));
        ctx
    }

    fn reg(name: &str, sql: &str, windowed: bool) -> StreamRegistration {
        StreamRegistration {
            name: name.to_string(),
            query_sql: sql.to_string(),
            emit_clause: None,
            // Resolver only checks `is_some()`; the size doesn't matter.
            window_config: windowed.then(|| {
                laminar_sql::translator::WindowOperatorConfig::tumbling(
                    "event_time".into(),
                    Duration::ZERO,
                )
            }),
            order_config: None,
            join_config: None,
            has_analytic: false,
            has_frame: false,
            incremental: false,
        }
    }

    #[tokio::test]
    async fn windowed_stream_schema_matches_user_select() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "agg".to_string(),
            reg(
                "agg",
                "SELECT region, COUNT(*) AS n FROM payments \
                 GROUP BY tumble(event_time, INTERVAL '1' MINUTE), region",
                true,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let names: Vec<&str> = out["agg"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["region", "n"]);
    }

    #[tokio::test]
    async fn windowed_stream_with_explicit_window_columns() {
        let ctx = ctx_with_payments();
        ctx.register_udf(datafusion_expr::ScalarUDF::from(
            laminar_sql::datafusion::TumbleWindowEnd::new(),
        ));
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "agg".to_string(),
            reg(
                "agg",
                "SELECT \
                    tumble(event_time, INTERVAL '1' MINUTE)     AS window_start, \
                    tumble_end(event_time, INTERVAL '1' MINUTE) AS window_end, \
                    region, \
                    COUNT(*) AS n \
                 FROM payments \
                 GROUP BY \
                    tumble(event_time, INTERVAL '1' MINUTE), \
                    tumble_end(event_time, INTERVAL '1' MINUTE), \
                    region",
                true,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let names: Vec<&str> = out["agg"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["window_start", "window_end", "region", "n"]);
        assert_eq!(
            out["agg"].field(0).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            out["agg"].field(1).data_type(),
            &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
    }

    #[tokio::test]
    async fn non_windowed_stream_has_no_prefix() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "passthrough".to_string(),
            reg(
                "passthrough",
                "SELECT region, amount_usd FROM payments",
                false,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let names: Vec<&str> = out["passthrough"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, vec!["region", "amount_usd"]);
    }

    #[tokio::test]
    async fn chained_streams_resolve_via_iterative_planning() {
        // `b` reads from `a`; iteration order doesn't matter — the loop
        // re-tries `b` after `a` is registered.
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "b".to_string(),
            reg("b", "SELECT region, n + 1 AS n_plus_one FROM a", false),
        );
        regs.insert(
            "a".to_string(),
            reg(
                "a",
                "SELECT region, COUNT(*) AS n FROM payments GROUP BY region",
                false,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        let b_names: Vec<&str> = out["b"]
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(b_names, vec!["region", "n_plus_one"]);

        // Placeholders must not leak into the public ctx — `subscribe()`
        // is the data path for streams; `SELECT * FROM <stream>` should
        // not silently return zero rows from a left-over EmptyTable.
        assert!(!ctx.table_exist("a").unwrap_or(false));
        assert!(!ctx.table_exist("b").unwrap_or(false));
    }

    #[tokio::test]
    async fn case_distinct_chained_streams_resolve_exactly() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        regs.insert(
            "foo".to_string(),
            reg("foo", "SELECT region, n FROM Foo", false),
        );
        regs.insert(
            "Foo".to_string(),
            reg(
                "Foo",
                "SELECT region, COUNT(*) AS n FROM payments GROUP BY region",
                false,
            ),
        );

        let out = resolve_stream_output_schemas(&ctx, &regs).await.unwrap();
        assert!(out.contains_key("Foo"));
        assert!(out.contains_key("foo"));
    }

    #[tokio::test]
    async fn unresolvable_streams_surface_planner_error() {
        let ctx = ctx_with_payments();
        let mut regs = std::collections::HashMap::new();
        // Cycle: a→b, b→a. Planning stalls; we report the unresolved set.
        regs.insert("a".to_string(), reg("a", "SELECT * FROM b", false));
        regs.insert("b".to_string(), reg("b", "SELECT * FROM a", false));

        let err = resolve_stream_output_schemas(&ctx, &regs)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("unresolvable stream dependency"), "got: {err}");
        assert!(err.contains('a') && err.contains('b'), "got: {err}");
    }
}

#[cfg(test)]
mod exact_deployment_lock_tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use laminar_connectors::connector::DeliveryGuarantee;
    use laminar_core::state::{NodeId, ObjectStoreBackend, VnodeRegistry};
    use laminar_core::storage::checkpoint_manifest::DurableCheckpointPhase;
    use laminar_core::storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};

    fn exact_builder_with_roots(
        state_dir: &std::path::Path,
        checkpoint_dir: &std::path::Path,
    ) -> crate::builder::LaminarDbBuilder {
        std::fs::create_dir_all(state_dir).unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(state_dir).unwrap());
        let backend = Arc::new(ObjectStoreBackend::node_durable(store, "node-0", 1));
        crate::db::LaminarDB::builder()
            .storage_dir(checkpoint_dir)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: Some(1_000),
                data_dir: Some(checkpoint_dir.to_owned()),
                ..Default::default()
            })
            .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
            .state_backend(backend)
            .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, NodeId(0))))
    }

    fn exact_builder(root: &std::path::Path) -> crate::builder::LaminarDbBuilder {
        exact_builder_with_roots(&root.join("state"), &root.join("checkpoints"))
    }

    async fn exact_db(root: &std::path::Path) -> Arc<crate::db::LaminarDB> {
        exact_builder(root).build().await.unwrap()
    }

    async fn install_generator_pipeline(db: &Arc<crate::db::LaminarDB>) {
        db.execute(
            "CREATE SOURCE generated_source (seq BIGINT, ts_ms BIGINT, value VARCHAR) WITH \
             ('connector' = 'generator', 'rows.per.second' = '1000', 'max.rows' = '1')",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM generated_stream AS SELECT seq FROM generated_source")
            .await
            .unwrap();
    }

    async fn wait_for_processing_cycle(db: &Arc<crate::db::LaminarDB>) {
        let metrics = db.engine_metrics.lock().clone().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while metrics.cycles.get() == 0 || metrics.events_ingested.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the generator processing cycle must complete");
    }

    #[tokio::test]
    async fn second_local_exact_process_cannot_share_checkpoint_namespace() {
        let root = tempfile::tempdir().unwrap();
        let first = exact_db(root.path()).await;
        let second = exact_db(root.path()).await;

        first.start().await.unwrap();
        let error = second
            .start()
            .await
            .expect_err("the deployment lock must reject a second live writer");
        assert!(error.to_string().contains("[LDB-0014]"), "{error}");

        first.shutdown().await.unwrap();
        second
            .start()
            .await
            .expect("OS lock must be released by clean shutdown");
        second.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn local_startup_settles_prepared_witness_before_reconciliation() {
        let root = tempfile::tempdir().unwrap();
        let checkpoint_dir = root.path().join("checkpoints");
        let first = exact_db(root.path()).await;
        install_generator_pipeline(&first).await;
        first.start().await.unwrap();
        wait_for_processing_cycle(&first).await;
        let committed = first.checkpoint().await.unwrap();
        assert!(committed.success, "{:?}", committed.error);
        first.shutdown().await.unwrap();

        // Retain committed N and model a crash after a distinct N+1 Prepared manifest became
        // durable but before its create-once terminal outcome became visible.
        let manifest_store = FileSystemCheckpointStore::new(&checkpoint_dir);
        let committed_manifest = manifest_store
            .load_by_id(committed.checkpoint_id)
            .await
            .unwrap()
            .unwrap();
        let prepared_id = committed.checkpoint_id + 1;
        let prepared_epoch = committed.epoch + 1;
        let mut prepared = committed_manifest.clone();
        prepared.checkpoint_id = prepared_id;
        prepared.epoch = prepared_epoch;
        prepared.durable_phase = DurableCheckpointPhase::Prepared;
        manifest_store.save(&prepared).await.unwrap();

        let restarted = exact_db(root.path()).await;
        install_generator_pipeline(&restarted).await;
        restarted
            .start()
            .await
            .expect("startup recovery must settle the Prepared witness before reconciliation");

        let decision_backing: Arc<dyn object_store::ObjectStore> = Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(&checkpoint_dir).unwrap(),
        );
        let decisions =
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(decision_backing);
        let winner = decisions
            .outcome(prepared_epoch)
            .await
            .unwrap()
            .expect("recovery must publish a terminal winner");
        assert_eq!(winner.checkpoint_id, prepared_id);
        assert_eq!(
            winner.verdict,
            laminar_core::checkpoint_decision::CheckpointVerdict::Abort
        );
        restarted.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn startup_rejects_a_state_root_from_another_deployment_before_installing_coordinator() {
        let root = tempfile::tempdir().unwrap();
        let state_dir = root.path().join("state");
        let first = exact_builder_with_roots(&state_dir, &root.path().join("checkpoint-a"))
            .build()
            .await
            .unwrap();
        first.start().await.unwrap();
        first.shutdown().await.unwrap();

        let second = exact_builder_with_roots(&state_dir, &root.path().join("checkpoint-b"))
            .build()
            .await
            .unwrap();
        let error = second.start().await.unwrap_err();
        assert!(
            error.to_string().contains("belongs to deployment"),
            "{error}"
        );
        assert!(second.coordinator.lock().await.is_none());
    }

    #[tokio::test]
    async fn recovered_global_watermark_floors_a_source_without_a_source_watermark() {
        let root = tempfile::tempdir().unwrap();
        let db = exact_builder(root.path())
            .delivery_guarantee(DeliveryGuarantee::BestEffort)
            .build()
            .await
            .unwrap();
        db.execute(
            "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id FROM trades")
            .await
            .unwrap();
        db.start().await.unwrap();

        let metrics = db.engine_metrics.lock().clone().unwrap();
        let cycles_before = metrics.cycles.get();
        let events_before = metrics.events_ingested.get();
        let input = db.source_untyped("trades").unwrap();
        let schema = input.schema();
        input
            .push_arrow(
                arrow_array::RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(arrow_array::Int64Array::from(vec![1])),
                        arrow_array::new_null_array(schema.field(1).data_type(), 1),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while metrics.cycles.get() == cycles_before
                || metrics.events_ingested.get() == events_before
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the null-event-time input cycle must complete");

        let source = db.catalog.get_source("trades").unwrap();
        assert_eq!(source.source.current_watermark(), i64::MIN);
        db.pipeline_watermark.store(1_500, Ordering::Release);
        let checkpoint = db.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{:?}", checkpoint.error);
        assert_eq!(source.source.current_watermark(), i64::MIN);

        db.stop_pipeline().await.unwrap();
        db.pipeline_watermark.store(i64::MIN, Ordering::Release);
        source.source.restore_watermark_for_recovery(i64::MIN);
        db.start().await.unwrap();

        assert_eq!(
            db.pipeline_watermark.load(Ordering::Acquire),
            1_500,
            "the durable global frontier must seed the rebuilt tracker",
        );
        assert_eq!(
            source.source.current_watermark(),
            1_500,
            "a source missing a per-source value must inherit the durable frontier",
        );
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn local_exact_file_url_is_rejected_until_its_root_is_lock_fenced() {
        let root = tempfile::tempdir().unwrap();
        let object_root = root.path().join("remote-shaped-checkpoints");
        std::fs::create_dir_all(&object_root).unwrap();
        let db = exact_builder(root.path())
            .object_store_url(format!("file://{}", object_root.display()))
            .build()
            .await
            .unwrap();

        let error = db
            .start()
            .await
            .expect_err("file:// is not protected by the data-dir deployment lock");
        assert!(error.to_string().contains("[LDB-0014]"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn local_exact_builder_rejects_an_injected_cluster_decision_store() {
        let root = tempfile::tempdir().unwrap();
        let decision_store = Arc::new(
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::new(
                object_store::memory::InMemory::new(),
            )),
        );
        let result = exact_builder(root.path())
            .decision_store(decision_store)
            .build()
            .await;
        let Err(error) = result else {
            panic!("a cluster decision store without a controller must fail at admission");
        };
        assert!(error.to_string().contains("cluster-only stores"), "{error}");
    }
}

#[cfg(test)]
mod mv_recovery_lifecycle_tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use laminar_core::streaming::StreamCheckpointConfig;

    use crate::config::LaminarConfig;
    use crate::db::LaminarDB;

    async fn install_generator_mvs(db: &Arc<LaminarDB>, max_rows: u64) {
        db.execute(&format!(
            "CREATE SOURCE generated (seq BIGINT, ts_ms BIGINT, value VARCHAR) WITH \
             ('connector' = 'generator', 'rows.per.second' = '1000', 'max.rows' = '{max_rows}')"
        ))
        .await
        .unwrap();
        db.execute("CREATE MATERIALIZED VIEW committed AS SELECT seq FROM generated")
            .await
            .unwrap();
        db.execute("CREATE MATERIALIZED VIEW empty_at_cut AS SELECT seq FROM generated")
            .await
            .unwrap();
    }

    fn update_mv(db: &LaminarDB, name: &str, values: Vec<i64>) {
        let schema = db
            .mv_store
            .read()
            .to_record_batch(name)
            .unwrap()
            .unwrap()
            .schema();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
            .expect("test MV batch");
        db.mv_store
            .write()
            .update_cycle(name, &[batch])
            .expect("test MV update");
    }

    fn mv_values(db: &LaminarDB, name: &str) -> Vec<i64> {
        let batch = db.mv_store.read().to_record_batch(name).unwrap().unwrap();
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn checkpoint_config(dir: &std::path::Path) -> LaminarConfig {
        LaminarConfig {
            storage_dir: Some(dir.to_path_buf()),
            checkpoint: Some(StreamCheckpointConfig {
                interval_ms: None,
                data_dir: Some(dir.to_path_buf()),
                ..StreamCheckpointConfig::default()
            }),
            ..LaminarConfig::default()
        }
    }

    #[tokio::test]
    async fn restart_installs_the_exact_committed_mv_image() {
        let dir = tempfile::tempdir().unwrap();
        let db = LaminarDB::open_with_config(checkpoint_config(dir.path())).unwrap();
        install_generator_mvs(&db, 1).await;
        db.start().await.unwrap();

        let metrics = db.engine_metrics.lock().clone().unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while metrics.cycles.get() == 0 || metrics.events_ingested.get() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bounded generator cycle");
        let empty_image = db.mv_store.read().fresh_image().unwrap();
        let previous = {
            let mut live = db.mv_store.write();
            std::mem::replace(&mut *live, empty_image)
        };
        drop(previous);

        update_mv(&db, "committed", vec![1]);
        let checkpoint = db.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{:?}", checkpoint.error);
        update_mv(&db, "committed", vec![2]);
        update_mv(&db, "empty_at_cut", vec![9]);

        db.stop_pipeline().await.unwrap();
        db.start().await.unwrap();

        assert_eq!(mv_values(&db, "committed"), [1]);
        assert!(mv_values(&db, "empty_at_cut").is_empty());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restart_with_no_checkpoint_installs_an_empty_mv_image() {
        let dir = tempfile::tempdir().unwrap();
        let db = LaminarDB::open_with_config(checkpoint_config(dir.path())).unwrap();
        install_generator_mvs(&db, 0).await;
        db.start().await.unwrap();
        update_mv(&db, "committed", vec![1]);

        db.stop_pipeline().await.unwrap();
        db.start().await.unwrap();

        assert!(mv_values(&db, "committed").is_empty());
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restart_without_a_coordinator_installs_an_empty_mv_image() {
        let db = LaminarDB::open().unwrap();
        install_generator_mvs(&db, 0).await;
        db.start().await.unwrap();
        update_mv(&db, "committed", vec![1]);
        db.pipeline_watermark.store(42, Ordering::Release);
        let source = db.catalog.get_source("generated").unwrap();
        source.source.restore_watermark_for_recovery(42);

        db.stop_pipeline().await.unwrap();
        db.start().await.unwrap();

        assert!(mv_values(&db, "committed").is_empty());
        assert_eq!(db.pipeline_watermark.load(Ordering::Acquire), i64::MIN);
        assert_eq!(source.source.current_watermark(), i64::MIN);
        db.shutdown().await.unwrap();
    }
}

#[cfg(all(test, feature = "cluster"))]
mod cluster_fault_watcher_tests {
    use super::report_cluster_compute_fault;
    use crate::db::DbState;
    use crate::{ClusterStartupDisposition, LaminarDB};
    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
    use laminar_connectors::connector::{
        SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceStart,
        SourceTopology,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::cluster::control::controller::{
        RecoveryAnnouncement, RecoveryFault, RecoveryRound,
    };
    use laminar_core::cluster::control::{
        CatalogManifest, CatalogManifestEntry, CatalogManifestStore, CatalogObjectKind,
        CheckpointParticipant, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
        LeaderLeaseStore, LeaseDeadline, LeaseOutcome, RecoverPhase,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};
    use laminar_core::state::{
        InProcessBackend, NodeId as StateNodeId, ObjectStoreBackend, VnodeRegistry,
    };
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    struct IdleClusterTestSource;

    static REJECTING_SPLITTABLE_STARTED: AtomicBool = AtomicBool::new(false);

    struct RejectingSplittableSource;

    #[async_trait]
    impl SourceConnector for IdleClusterTestSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(None)
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
            ]))
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
            ))
        }

        fn set_vnode_assignment(
            &mut self,
            _source_identity: &str,
            _registry: Arc<VnodeRegistry>,
            _self_id: StateNodeId,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl SourceConnector for RejectingSplittableSource {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            REJECTING_SPLITTABLE_STARTED.store(true, Ordering::Release);
            Ok(())
        }

        async fn poll_batch(
            &mut self,
            _max_records: usize,
        ) -> Result<Option<SourceBatch>, ConnectorError> {
            Ok(None)
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
            ]))
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
            ))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    async fn startup_db() -> (
        Arc<LaminarDB>,
        Arc<ClusterController>,
        Arc<InMemoryKv>,
        RecoveryRound,
        Arc<CatalogManifestStore>,
        laminar_core::checkpoint::LeaderProof,
    ) {
        let node_id = NodeId(7);
        let kv = Arc::new(InMemoryKv::new(node_id));
        let controller_kv: Arc<dyn ClusterKv> = kv.clone();
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new(
            node_id,
            controller_kv,
            None,
            members_rx,
        ));
        controller.publish_recovery_incarnation().await.unwrap();
        let checkpoint_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&checkpoint_store), 10_000));
        let owner = LeaderLeaseOwner {
            node: node_id,
            boot: controller.recovery_incarnation(),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            panic!("empty test authority must grant leadership");
        };
        let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease.clone()));
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))));
        controller
            .set_leader_lease_watch(
                lease_rx,
                owner,
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));
        controller.set_active(true);
        let manifest_store = Arc::new(CatalogManifestStore::new(authority));
        let registry = Arc::new(VnodeRegistry::single_owner(1, StateNodeId(node_id.0)));
        let round = RecoveryRound::new(
            1,
            lease.proof(),
            CheckpointAssignmentFence::from_owner_map(
                1,
                &[node_id.0],
                vec![CheckpointParticipant {
                    node_id: node_id.0,
                    boot_incarnation: controller.recovery_incarnation(),
                }],
            )
            .unwrap(),
            vec![RecoveryFault {
                reporter: node_id,
                sequence: 1,
            }],
        )
        .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(round.assignment_fence.clone()));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(checkpoint_store)
            .catalog_manifest_store(Arc::clone(&manifest_store))
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: Some(3_600_000),
                ..laminar_core::streaming::StreamCheckpointConfig::default()
            })
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(registry)
            .register_connector(|registry| {
                registry.register_source(
                    "idle-cluster-test",
                    ConnectorInfo {
                        name: "idle-cluster-test".into(),
                        display_name: "Idle cluster test source".into(),
                        version: "1".into(),
                        is_source: true,
                        is_sink: false,
                        config_keys: vec![],
                    },
                    Arc::new(|_| Ok(Box::new(IdleClusterTestSource))),
                )?;
                registry.register_source(
                    "rejecting-splittable-test",
                    ConnectorInfo {
                        name: "rejecting-splittable-test".into(),
                        display_name: "Rejecting splittable test source".into(),
                        version: "1".into(),
                        is_source: true,
                        is_sink: false,
                        config_keys: vec![],
                    },
                    Arc::new(|_| Ok(Box::new(RejectingSplittableSource))),
                )
            })
            .build()
            .await
            .unwrap();
        db.fence_cluster_startup();
        (db, controller, kv, round, manifest_store, lease.proof())
    }

    #[tokio::test]
    async fn fresh_certified_cluster_startup_opens_intake() {
        let (db, controller, _kv, _round, _manifest_store, _proof) = startup_db().await;

        assert_eq!(
            db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            ClusterStartupDisposition::Serving
        );
        assert!(!db.source_gate.load(std::sync::atomic::Ordering::Acquire));
        assert!(!controller.is_recovering());
    }

    #[tokio::test]
    async fn durable_fault_before_startup_audit_keeps_intake_closed() {
        let (db, controller, _kv, _round, _manifest_store, _proof) = startup_db().await;
        controller.report_fault(9).await.unwrap();

        assert_eq!(
            db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            ClusterStartupDisposition::RecoveryFenced
        );
        assert!(db.cluster_intake_fenced());
        assert!(controller.is_recovering());
    }

    #[tokio::test]
    async fn zero_vnode_worker_finishes_startup_idle_and_data_plane_fenced() {
        let local = StateNodeId(7);
        let owner = StateNodeId(8);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(local));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(local, kv, None, members_rx));
        controller.publish_recovery_incarnation().await.unwrap();
        controller.set_active(true);
        let fence = CheckpointAssignmentFence::from_owner_map(
            1,
            &[owner.0],
            vec![CheckpointParticipant {
                node_id: owner.0,
                boot_incarnation: uuid::Uuid::from_u128(88),
            }],
        )
        .unwrap();
        controller.publish_checkpoint_assignment_fence(Some(fence.clone()));
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            local.0,
            controller.recovery_incarnation(),
        ));
        let db = LaminarDB::builder()
            .cluster_controller(Arc::clone(&controller))
            .cluster_checkpoint_object_store(Arc::new(object_store::memory::InMemory::new()))
            .state_backend(Arc::new(InProcessBackend::new(1)))
            .vnode_registry(Arc::new(VnodeRegistry::single_owner(1, owner)))
            .shuffle_sender(Arc::clone(&sender))
            .build()
            .await
            .unwrap();
        db.fence_cluster_startup();

        assert_eq!(
            db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            ClusterStartupDisposition::Idle
        );
        assert!(db.cluster_intake_fenced());
        assert_eq!(sender.assignment_version(), 0);
        assert!(sender.active_assignment_digest().is_none());
        assert_eq!(controller.checkpoint_assignment_fence(1), Some(fence));
        assert!(!controller.is_recovering());
        assert!(controller.read_fault_reports().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn splittable_source_without_assignment_hook_fails_before_start() {
        REJECTING_SPLITTABLE_STARTED.store(false, Ordering::Release);
        let (db, _controller, _kv, _round, manifest_store, proof) = startup_db().await;
        let state_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        *db.state_backend.lock() = Some(Arc::new(ObjectStoreBackend::cluster_shared(
            state_store,
            "node-7",
            1,
        )));
        manifest_store
            .seal(
                &CatalogManifest::new(vec![
                    CatalogManifestEntry {
                        canonical_name: "unsafe_input".into(),
                        kind: CatalogObjectKind::Source,
                        ddl: "CREATE SOURCE unsafe_input (id BIGINT) WITH ('connector' = \
                              'rejecting-splittable-test')"
                            .into(),
                    },
                    CatalogManifestEntry {
                        canonical_name: "unsafe_output".into(),
                        kind: CatalogObjectKind::Stream,
                        ddl: "CREATE STREAM unsafe_output AS SELECT id FROM unsafe_input".into(),
                    },
                ])
                .unwrap(),
                &proof,
            )
            .await
            .unwrap();

        let error = db.start().await.unwrap_err().to_string();
        assert!(
            error.contains("rejected cluster vnode assignment"),
            "{error}"
        );
        assert!(
            error.contains("does not implement vnode assignment"),
            "{error}"
        );
        assert!(
            !REJECTING_SPLITTABLE_STARTED.load(Ordering::Acquire),
            "source I/O must not start before assignment admission succeeds"
        );
    }

    #[tokio::test]
    async fn manifest_replay_cleanup_fault_remains_terminal_after_start_returns() {
        let (db, _controller, _kv, _round, manifest_store, proof) = startup_db().await;
        manifest_store
            .seal(
                &CatalogManifest::new(vec![
                    CatalogManifestEntry {
                        canonical_name: "fenced".into(),
                        kind: CatalogObjectKind::Source,
                        ddl: "CREATE SOURCE fenced (id BIGINT)".into(),
                    },
                    CatalogManifestEntry {
                        canonical_name: "broken".into(),
                        kind: CatalogObjectKind::Stream,
                        ddl: "CREATE STREAM broken AS SELECT id FROM missing_source".into(),
                    },
                ])
                .unwrap(),
                &proof,
            )
            .await
            .unwrap();
        *db.catalog_cleanup_deregister_fault.lock() = Some("fenced".into());

        let start_error = db.start().await.unwrap_err();
        let start_error = start_error.to_string();
        assert!(
            start_error.contains("catalog manifest replay failed for 'broken'"),
            "{start_error}"
        );
        assert!(start_error.contains("[LDB-6044]"), "{start_error}");
        assert!(
            start_error.contains("catalog bootstrap rollback remains terminally fenced"),
            "{start_error}"
        );
        assert_eq!(DbState::load(&db.state), DbState::Faulted);
        assert!(db
            .catalog_cleanup_fenced
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(db.ctx.table_exist("fenced").unwrap());
        assert_eq!(
            db.catalog_namespace.lock().get("fenced"),
            Some(&CatalogObjectKind::Source)
        );
        let terminal_reason = db.last_fault().expect("terminal cleanup reason");
        assert!(terminal_reason.contains("[LDB-6044]"));

        let retry_error = db.start().await.unwrap_err();
        assert!(retry_error.to_string().contains("[LDB-6044]"));
        assert_eq!(DbState::load(&db.state), DbState::Faulted);
        assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn assignment_closure_wins_while_startup_waits_to_open_intake() {
        let (db, controller, _kv, _round, _manifest_store, _proof) = startup_db().await;
        let execution = Arc::clone(&db.rotation_execution_fence).read_owned().await;
        let starting = {
            let db = Arc::clone(&db);
            tokio::spawn(async move {
                db.finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(2))
                    .await
            })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if db.assignment_adoption_lock.try_lock().is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("startup must reach the serialized activation boundary");

        db.set_source_gate(true);
        controller.publish_checkpoint_assignment_fence(None);
        db.suspend_shuffle_assignment_fence();
        drop(execution);

        assert_eq!(
            starting.await.unwrap().unwrap(),
            ClusterStartupDisposition::RecoveryFenced
        );
        assert!(db.cluster_intake_fenced());
        assert_eq!(controller.checkpoint_assignment_fence(1), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipeline_startup_holds_generation_fence_until_graph_publication() {
        let (db, _controller, _kv, _round, manifest_store, proof) = startup_db().await;
        let state_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::memory::InMemory::new());
        *db.state_backend.lock() = Some(Arc::new(ObjectStoreBackend::cluster_shared(
            state_store,
            "node-7",
            1,
        )));
        manifest_store
            .seal(
                &CatalogManifest::new(vec![
                    CatalogManifestEntry {
                        canonical_name: "trades".into(),
                        kind: CatalogObjectKind::Source,
                        ddl: "CREATE SOURCE trades (id BIGINT) WITH ('connector' = 'idle-cluster-test')"
                            .into(),
                    },
                    CatalogManifestEntry {
                        canonical_name: "out".into(),
                        kind: CatalogObjectKind::Stream,
                        ddl: "CREATE STREAM out AS SELECT id FROM trades".into(),
                    },
                ])
                .unwrap(),
                &proof,
            )
            .await
            .unwrap();
        db.pending_revoke_vnodes.lock().insert(0);

        // Stall startup after its staging reset. This makes the old reset-only fence gap
        // deterministic without adding a production test hook.
        let table_store = db.table_store.write();
        let starting = {
            let db = Arc::clone(&db);
            tokio::spawn(async move { db.start().await })
        };
        tokio::time::timeout(Duration::from_secs(1), async {
            while !db.pending_revoke_vnodes.lock().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("startup must reset prior-generation vnode staging");

        let competing_generation = tokio::time::timeout(
            Duration::from_millis(100),
            Arc::clone(&db.rotation_execution_fence).write_owned(),
        )
        .await;
        assert!(
            competing_generation.is_err(),
            "assignment rotation entered while startup still owned an unpublished graph"
        );

        drop(table_store);
        starting.await.unwrap().unwrap();
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn restored_or_active_recovery_startup_stays_fenced_and_reports() {
        let (restored, controller, _kv, _round, _manifest_store, _proof) = startup_db().await;
        *restored.last_recovery_epoch.lock() = Some(9);
        assert_eq!(
            restored
                .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            ClusterStartupDisposition::RecoveryFenced
        );
        assert!(restored
            .source_gate
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(!controller.read_fault_reports().await.unwrap().is_empty());

        let (active, controller, _kv, round, _manifest_store, _proof) = startup_db().await;
        controller.announce_recover_prepare(&round).await.unwrap();
        assert_eq!(
            active
                .finish_cluster_startup(tokio::time::Instant::now() + Duration::from_secs(1))
                .await
                .unwrap(),
            ClusterStartupDisposition::RecoveryFenced
        );
        assert!(active
            .source_gate
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(controller.is_recovering());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_recovery_does_not_block_compute_fault_handoff() {
        let (_db, controller, _kv, round, _manifest_store, _proof) = startup_db().await;
        controller.announce_recover_prepare(&round).await.unwrap();
        assert_eq!(
            controller.observe_recover().await.unwrap(),
            Some(RecoveryAnnouncement {
                round: round.clone(),
                phase: RecoverPhase::Prepare,
            })
        );

        let handed_off = tokio::time::timeout(
            Duration::from_secs(1),
            report_cluster_compute_fault(Some(Arc::clone(&controller))),
        )
        .await
        .expect("fault handoff waited for the active recovery round to clear");

        assert!(handed_off);
        assert!(controller
            .read_fault_reports()
            .await
            .unwrap()
            .into_iter()
            .any(|(node, sequence)| node == controller.instance_id() && sequence > 0));
        assert_eq!(
            controller.observe_recover().await.unwrap(),
            Some(RecoveryAnnouncement {
                round,
                phase: RecoverPhase::Prepare,
            })
        );
    }
}

#[cfg(test)]
mod reference_table_recovery_tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_connectors::config::ConnectorInfo;
    use laminar_connectors::error::ConnectorError;
    use laminar_connectors::reference::ReferenceTableSource;
    use laminar_connectors::registry::ConnectorRegistry;

    use super::{
        create_reference_table_sources, hydrate_reference_table_sources,
        ReferenceTableRuntimeSource,
    };
    use crate::connector_manager::TableRegistration;
    use crate::table_store::TableStore;

    struct CountingSnapshotSource {
        polls: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        batches: VecDeque<RecordBatch>,
        fail_poll: bool,
        fail_close: bool,
    }

    #[async_trait::async_trait]
    impl ReferenceTableSource for CountingSnapshotSource {
        async fn poll_snapshot(&mut self) -> Result<Option<RecordBatch>, ConnectorError> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.fail_poll {
                self.fail_poll = false;
                return Err(ConnectorError::ReadError(
                    "injected snapshot failure".into(),
                ));
            }
            Ok(self.batches.pop_front())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            if self.fail_close {
                Err(ConnectorError::ReadError("injected close failure".into()))
            } else {
                Ok(())
            }
        }
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]))
    }

    fn batch(id: i32, value: &str) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(vec![id])),
                Arc::new(StringArray::from(vec![value])),
            ],
        )
        .unwrap()
    }

    fn runtime_source(
        name: &str,
        polls: Arc<AtomicUsize>,
        closes: Arc<AtomicUsize>,
        batches: Vec<RecordBatch>,
        fail_poll: bool,
        fail_close: bool,
    ) -> ReferenceTableRuntimeSource {
        (
            name.into(),
            Box::new(CountingSnapshotSource {
                polls,
                closes,
                batches: batches.into(),
                fail_poll,
                fail_close,
            }),
        )
    }

    fn registration(name: &str) -> TableRegistration {
        TableRegistration {
            name: name.into(),
            primary_key: "id".into(),
            connector_type: Some("mock".into()),
            connector_options: HashMap::new(),
            format: None,
            format_options: HashMap::new(),
            on_demand: false,
            cache_max_bytes: None,
            cache_ttl: None,
        }
    }

    #[tokio::test]
    async fn complete_table_restore_skips_source_construction() {
        let mut table_store = TableStore::new();
        table_store.create_table("t", schema(), "id").unwrap();
        table_store.upsert("t", &batch(1, "checkpoint")).unwrap();
        table_store.set_ready("t", true);
        let table_store = parking_lot::RwLock::new(table_store);

        let factory_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&factory_calls);
        let registry = ConnectorRegistry::new();
        registry
            .register_table_source(
                "mock",
                ConnectorInfo {
                    name: "mock".into(),
                    display_name: "Mock".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(move |_, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(Box::new(CountingSnapshotSource {
                        polls: Arc::new(AtomicUsize::new(0)),
                        closes: Arc::new(AtomicUsize::new(0)),
                        batches: VecDeque::new(),
                        fail_poll: false,
                        fail_close: false,
                    }))
                }),
            )
            .unwrap();
        let registrations = HashMap::from([("t".into(), registration("t"))]);
        let sources = create_reference_table_sources(&registry, &registrations, &table_store, true)
            .await
            .unwrap();

        assert!(sources.is_empty());
        assert_eq!(factory_calls.load(Ordering::SeqCst), 0);
        let restored = table_store.read().to_record_batch("t").unwrap().unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(
            restored
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
    }

    #[tokio::test]
    async fn later_source_construction_failure_closes_prior_sources() {
        let mut table_store = TableStore::new();
        table_store.create_table("a", schema(), "id").unwrap();
        table_store.create_table("b", schema(), "id").unwrap();
        let table_store = parking_lot::RwLock::new(table_store);

        let factory_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&factory_calls);
        let closes = Arc::new(AtomicUsize::new(0));
        let source_closes = Arc::clone(&closes);
        let registry = ConnectorRegistry::new();
        registry
            .register_table_source(
                "mock",
                ConnectorInfo {
                    name: "mock".into(),
                    display_name: "Mock".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(move |_, _| {
                    if calls.fetch_add(1, Ordering::SeqCst) == 1 {
                        return Err(ConnectorError::ConfigurationError(
                            "injected factory failure".into(),
                        ));
                    }
                    Ok(Box::new(CountingSnapshotSource {
                        polls: Arc::new(AtomicUsize::new(0)),
                        closes: Arc::clone(&source_closes),
                        batches: VecDeque::new(),
                        fail_poll: false,
                        fail_close: false,
                    }))
                }),
            )
            .unwrap();
        let registrations = HashMap::from([
            ("b".into(), registration("b")),
            ("a".into(), registration("a")),
        ]);

        let result =
            create_reference_table_sources(&registry, &registrations, &table_store, false).await;
        let error = match result {
            Ok(_) => panic!("the second table-source factory must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("injected factory failure"));
        assert_eq!(factory_calls.load(Ordering::SeqCst), 2);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn fresh_start_exhausts_upstream_snapshot_and_marks_table_ready() {
        let mut table_store = TableStore::new();
        table_store.create_table("t", schema(), "id").unwrap();
        let table_store = parking_lot::RwLock::new(table_store);

        let polls = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let sources = vec![runtime_source(
            "t",
            Arc::clone(&polls),
            Arc::clone(&closes),
            vec![batch(2, "upstream")],
            false,
            false,
        )];
        let names = hydrate_reference_table_sources(sources, &table_store)
            .await
            .unwrap();

        assert_eq!(names, ["t"]);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(table_store.read().is_ready("t"));
        assert_eq!(table_store.read().table_row_count("t"), 1);
    }

    #[tokio::test]
    async fn snapshot_poll_failure_closes_every_source_without_mutation() {
        let mut table_store = TableStore::new();
        table_store.create_table("a", schema(), "id").unwrap();
        table_store.create_table("b", schema(), "id").unwrap();
        let table_store = parking_lot::RwLock::new(table_store);

        let a_polls = Arc::new(AtomicUsize::new(0));
        let b_polls = Arc::new(AtomicUsize::new(0));
        let a_closes = Arc::new(AtomicUsize::new(0));
        let b_closes = Arc::new(AtomicUsize::new(0));
        let sources = vec![
            runtime_source(
                "a",
                Arc::clone(&a_polls),
                Arc::clone(&a_closes),
                Vec::new(),
                true,
                false,
            ),
            runtime_source(
                "b",
                Arc::clone(&b_polls),
                Arc::clone(&b_closes),
                vec![batch(2, "upstream")],
                false,
                false,
            ),
        ];

        let error = hydrate_reference_table_sources(sources, &table_store)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected snapshot failure"));
        assert_eq!(a_polls.load(Ordering::SeqCst), 1);
        assert_eq!(b_polls.load(Ordering::SeqCst), 0);
        assert_eq!(a_closes.load(Ordering::SeqCst), 1);
        assert_eq!(b_closes.load(Ordering::SeqCst), 1);
        assert_eq!(table_store.read().table_row_count("a"), 0);
        assert_eq!(table_store.read().table_row_count("b"), 0);
        assert!(!table_store.read().is_ready("a"));
        assert!(!table_store.read().is_ready("b"));
    }

    #[tokio::test]
    async fn snapshot_close_failure_prevents_install() {
        let mut table_store = TableStore::new();
        table_store.create_table("t", schema(), "id").unwrap();
        let table_store = parking_lot::RwLock::new(table_store);

        let polls = Arc::new(AtomicUsize::new(0));
        let closes = Arc::new(AtomicUsize::new(0));
        let sources = vec![runtime_source(
            "t",
            Arc::clone(&polls),
            Arc::clone(&closes),
            vec![batch(2, "upstream")],
            false,
            true,
        )];

        let error = hydrate_reference_table_sources(sources, &table_store)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("injected close failure"));
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert_eq!(table_store.read().table_row_count("t"), 0);
        assert!(!table_store.read().is_ready("t"));
    }
}

#[cfg(test)]
mod supervisor_tests {
    use super::{backoff_for_attempt, claim_restart_slot, spawn_supervised_restart};
    use crate::config::RestartPolicy;
    use crate::db::{DbState, LaminarDB};
    use laminar_core::catalog::CatalogObjectKind;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn restart_budget_caps_within_window_and_prunes_stale() {
        let p = RestartPolicy::default();
        let mut hist = Vec::new();
        let now = Instant::now();
        for i in 0..p.max_restarts {
            assert_eq!(
                claim_restart_slot(&mut hist, now, p.max_restarts, p.window),
                Some(i)
            );
        }
        assert_eq!(
            claim_restart_slot(&mut hist, now, p.max_restarts, p.window),
            None
        );
        // A window later the stale entries are pruned, freeing the budget again.
        let later = now + p.window * 2;
        assert_eq!(
            claim_restart_slot(&mut hist, later, p.max_restarts, p.window),
            Some(0)
        );
        assert_eq!(hist.len(), 1);
    }

    #[test]
    fn backoff_grows_exponentially_capped() {
        let init = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        assert_eq!(
            backoff_for_attempt(init, max, 0),
            Duration::from_millis(100)
        );
        assert_eq!(
            backoff_for_attempt(init, max, 1),
            Duration::from_millis(200)
        );
        assert_eq!(
            backoff_for_attempt(init, max, 3),
            Duration::from_millis(800)
        );
        assert_eq!(
            backoff_for_attempt(init, max, 4),
            max,
            "1600ms capped at 1s"
        );
        assert_eq!(
            backoff_for_attempt(init, max, 1000),
            max,
            "huge attempt must not overflow"
        );
    }

    // Drives the real watcher path and transfers startup to its owned driver.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn supervised_restart_recovers_faulted_pipeline() {
        let db = LaminarDB::open().unwrap();
        db.enable_supervision();
        db.execute(
            "CREATE SOURCE trades (id BIGINT, ts TIMESTAMP, \
             WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM out AS SELECT id FROM trades")
            .await
            .unwrap();

        DbState::Faulted.store(&db.state);
        *db.last_fault.lock() = Some("operator boom".to_string());
        db.shutdown_signal.notify_one();

        let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(
            &prometheus::Registry::new(),
        ));
        let join = spawn_supervised_restart(
            Arc::clone(&db),
            Arc::clone(&db.restart_history),
            Some(Arc::clone(&metrics)),
        )
        .expect("spawn restart thread");
        join.await.expect("restart task");

        assert_eq!(db.pipeline_state(), "Running");
        assert!(db.last_fault().is_none());
        assert_eq!(metrics.pipeline_restarts_total.get(), 1);
        db.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn incomplete_catalog_cleanup_is_terminal_across_stop_and_supervision() {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE TABLE fenced (id BIGINT PRIMARY KEY)")
            .await
            .unwrap();
        *db.catalog_cleanup_deregister_fault.lock() = Some("fenced".into());

        let drop_error = db.execute("DROP TABLE fenced").await.unwrap_err();
        assert!(drop_error.to_string().contains("[LDB-6044]"));
        assert_eq!(DbState::load(&db.state), DbState::Faulted);
        assert!(db.ctx.table_exist("fenced").unwrap());
        assert_eq!(
            db.catalog_namespace.lock().get("fenced"),
            Some(&CatalogObjectKind::Table)
        );
        let terminal_reason = db.last_fault().expect("terminal reason");

        db.stop_pipeline().await.unwrap();
        let start_error = db.start().await.unwrap_err();
        assert!(start_error.to_string().contains("[LDB-6044]"));
        assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));

        let create_error = db
            .execute("CREATE TABLE fenced (id BIGINT PRIMARY KEY)")
            .await
            .unwrap_err();
        assert!(create_error.to_string().contains("[LDB-6044]"));
        assert_eq!(
            db.catalog_namespace.lock().get("fenced"),
            Some(&CatalogObjectKind::Table)
        );

        let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(
            &prometheus::Registry::new(),
        ));
        let join = spawn_supervised_restart(
            Arc::clone(&db),
            Arc::clone(&db.restart_history),
            Some(Arc::clone(&metrics)),
        )
        .expect("spawn restart thread");
        join.await.expect("restart task");

        assert!(db.restart_history.lock().is_empty());
        assert_eq!(metrics.pipeline_restarts_total.get(), 0);
        assert_eq!(db.last_fault().as_deref(), Some(terminal_reason.as_str()));
    }
}
