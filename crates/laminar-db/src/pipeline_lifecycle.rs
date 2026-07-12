//! Pipeline lifecycle: start, close, shutdown.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SinkConsistency, SinkContract, SinkTopology,
    SourceConsistency, SourceContract, SourceTopology,
};
use laminar_core::{state::StateBackendDurability, streaming};
use rustc_hash::FxHashMap;

use crate::db::{DbState, LaminarDB, SourceWatermarkState};
use crate::error::DbError;

/// Runtime placement boundary used for connector admission.
///
/// `Cluster` means a controller is actually installed on this DB instance. A
/// binary compiled with cluster support but running without a controller is
/// still a local runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RuntimeMode {
    Local,
    Cluster,
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
    crate::coordinated_recovery::report_local_fault(&controller).await;
    true
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
            "[LDB-0013] cluster exactly-once is not admitted until the leader term is \
             atomically bound to checkpoint decisions and external sink commits",
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
            SourceTopology::NodeLocalIngress if delivery == DeliveryGuarantee::BestEffort => {}
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

/// Validate sink durability, placement, and changelog semantics before I/O.
fn admit_sink_contract(
    contract: SinkContract,
    delivery: DeliveryGuarantee,
    runtime: RuntimeMode,
    carries_changelog: bool,
) -> Result<(), &'static str> {
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::ExactlyOnce {
        return Err(
            "[LDB-0013] cluster exactly-once is not admitted until the leader term is \
             atomically bound to checkpoint decisions and external sink commits",
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
    requires_recovery_on_error: bool,
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

async fn close_opened_sinks(sinks: &mut [PreparedSink], cleanup_deadline: tokio::time::Instant) {
    for prepared in sinks.iter_mut().rev() {
        match tokio::time::timeout_at(cleanup_deadline, prepared.connector.close()).await {
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
}

async fn open_prepared_sinks(
    sinks: &mut [PreparedSink],
    open_timeout: std::time::Duration,
) -> Result<(), DbError> {
    let open_deadline = tokio::time::Instant::now() + open_timeout;
    let mut index = 0;
    while index < sinks.len() {
        let prepared = &mut sinks[index];
        let name = prepared.name.clone();
        let open_error =
            match tokio::time::timeout_at(open_deadline, prepared.connector.open(&prepared.config))
                .await
            {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some(format!(
                    "exceeded the shared {open_timeout:?} sink-open stage deadline"
                )),
            };
        if let Some(error) = open_error {
            // A failed/cancelled open may already hold resources, so include the current sink.
            // Previously opened and current connectors share one fresh rollback deadline.
            let cleanup_deadline = tokio::time::Instant::now()
                + crate::pipeline::PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT;
            close_opened_sinks(&mut sinks[..=index], cleanup_deadline).await;
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

                if !ctx.table_exist(&reg.name).unwrap_or(false) {
                    ctx.register_table(&reg.name, Arc::new(EmptyTable::new(schema.clone())))
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
        let _ = ctx.deregister_table(name);
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

/// Restart on a dedicated thread: `start()` is `!Send`, so `block_on` drives it while its
/// inner `tokio::spawn` of the next watcher still targets this runtime. Call from a runtime.
fn spawn_supervised_restart(
    db: Arc<LaminarDB>,
    history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
) -> std::io::Result<std::thread::JoinHandle<()>> {
    let handle = tokio::runtime::Handle::current();
    std::thread::Builder::new()
        .name("laminar-restart".into())
        .spawn(move || handle.block_on(attempt_supervised_restart(db, history, metrics)))
}

/// One recover-from-checkpoint restart, honoring the restart budget.
async fn attempt_supervised_restart(
    db: Arc<LaminarDB>,
    history: Arc<parking_lot::Mutex<Vec<std::time::Instant>>>,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
) {
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

impl LaminarDB {
    /// Shut down the database gracefully.
    pub fn close(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Replay demoted vnode partials into the operator graph on restart.
    ///
    /// Fails loud on any unrecoverable vnode: source offsets are already staged,
    /// so resuming with empty state would silently corrupt that aggregate.
    #[cfg(feature = "state-tier")]
    async fn rehydrate_cold_vnodes(
        &self,
        graph: &mut crate::operator_graph::OperatorGraph,
        cold_map: &[(String, Vec<u32>)],
        recovered_attempt: laminar_core::state::CheckpointAttempt,
    ) -> Result<(), DbError> {
        let Some(backend) = self.state_backend.lock().clone() else {
            return Err(DbError::Checkpoint(
                "[LDB-6030] demoted vnodes recorded but no state backend is \
                 wired — cannot recover them on restart"
                    .to_string(),
            ));
        };
        let mut all_cold: Vec<u32> = cold_map
            .iter()
            .flat_map(|(_, vs)| vs.iter().copied())
            .collect();
        all_cold.sort_unstable();
        all_cold.dedup();
        let rehy = crate::recovery_manager::VnodeRehydrator::new(backend.as_ref())
            .rehydrate_at(&all_cold, recovered_attempt)
            .await?;

        let (mut applied, mut lost) = (0usize, 0usize);
        for (op_name, cold_vnodes) in cold_map {
            for &v in cold_vnodes {
                let Some(chain_bytes) = rehy.restored.get(&v) else {
                    tracing::error!(operator = %op_name, vnode = v, "demoted-vnode partial missing on restart");
                    lost += 1;
                    continue;
                };
                let chain: Vec<crate::vnode_partial::VnodePartial> = chain_bytes
                    .iter()
                    .filter_map(|b| crate::vnode_partial::VnodePartial::decode(b).ok())
                    .collect();
                if chain.len() != chain_bytes.len() {
                    tracing::error!(operator = %op_name, vnode = v, "demoted-vnode chain link decode failed");
                    lost += 1;
                    continue;
                }
                // Absence of a FULL base means the operator had no groups in this vnode.
                if let Some((base, deltas)) =
                    crate::recovery_manager::resolve_op_chain(&chain, op_name)
                {
                    match graph.apply_vnode_chain(op_name, v, base, &deltas) {
                        Ok(()) => applied += 1,
                        Err(e) => {
                            tracing::error!(operator = %op_name, vnode = v, error = %e, "demoted-vnode apply failed");
                            lost += 1;
                        }
                    }
                }
            }
        }
        if lost > 0 {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6030] {lost} demoted vnode slice(s) unrecoverable on restart \
                 — refusing to start with staged source offsets and lost state \
                 (see per-vnode errors above)"
            )));
        }
        tracing::info!(applied, "rehydrated demoted vnodes from durable partials");
        Ok(())
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
            return Ok(());
        };
        let owned = match self.vnode_registry.lock().as_ref() {
            Some(registry) => laminar_core::state::owned_vnodes(registry, self_id),
            None => return Ok(()),
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

    /// A source's recovery offsets from the manifest and its decision-bound sealed cluster
    /// handoff. Explicit empty checkpoints remain present so pre-first-record sources resume
    /// from their connector-defined initial position rather than appearing uncheckpointed.
    #[cfg(feature = "cluster")]
    fn recovery_source_checkpoint(
        source: &str,
        manifest_cp: Option<&laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint>,
        handoff: &HashMap<String, HashMap<String, String>>,
        _epoch: u64,
    ) -> Option<laminar_connectors::checkpoint::SourceCheckpoint> {
        use laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint;
        let participated = manifest_cp.is_some() || handoff.contains_key(source);
        let mut cp = manifest_cp
            .cloned()
            .unwrap_or_else(|| ConnectorCheckpoint::with_offsets(HashMap::new()));
        let fill: Vec<(String, String)> = handoff
            .get(source)
            .into_iter()
            .flatten()
            .filter(|(key, _)| !cp.offsets.contains_key(*key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if !fill.is_empty() {
            tracing::info!(
                source,
                filled = fill.len(),
                "recovery filled partitions from handoff"
            );
            cp.offsets.extend(fill);
        }
        if cp.offsets.is_empty() && !participated {
            return None;
        }
        Some(crate::checkpoint_coordinator::connector_to_source_checkpoint(&cp))
    }

    /// Returns `true` if the database has been shut down.
    pub fn is_closed(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Connector add/remove DDL can't take effect while serving — connectors are
    /// built only at `start()`. `Starting` is allowed so catalog-manifest replay
    /// (which runs before `start_inner` builds connectors) is picked up.
    pub(crate) fn connector_ddl_rejected(&self) -> bool {
        matches!(
            DbState::load(&self.state),
            DbState::Starting | DbState::Running | DbState::ShuttingDown
        )
    }

    /// A checkpoint identity is immutable for one running topology. Live topology evolution needs
    /// a versioned topology barrier/savepoint migration protocol, so checkpointed runtimes reject
    /// it until that protocol exists.
    pub(crate) fn checkpointed_topology_ddl_rejected(&self) -> bool {
        self.config.checkpoint.is_some()
            && matches!(
                DbState::load(&self.state),
                DbState::Starting | DbState::Running | DbState::ShuttingDown
            )
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

    /// Stamp this node's shuffle fabric with the round's generation. Outbound frames carry it and
    /// inbound frames below it are discarded, so a pre-rewind frame still in flight can't be
    /// folded onto the restored state and then re-applied by the sender's replay.
    #[cfg(feature = "cluster")]
    pub(crate) fn set_shuffle_recovery_gen(&self, gen: u64) {
        if let Some(sender) = self.shuffle_sender.lock().as_ref() {
            sender.set_recovery_gen(gen);
        }
        if let Some(receiver) = self.shuffle_receiver.lock().as_ref() {
            receiver.set_recovery_gen(gen);
        }
    }

    /// Report this process's restart as a fault so the leader rewinds every node to the
    /// sealed cut. A kill-9'd process cannot report at death, so records shuffled between
    /// the last seal and the kill double-fold on survivors at replay and its own inbound
    /// shuffle is lost; the rejoin report drives the round the death could not. No-op when
    /// no controller is wired.
    #[cfg(feature = "cluster")]
    pub async fn report_rejoin_fault(&self) {
        // A boot that restored no local state is a fresh joiner (cluster formation,
        // scale-out, wiped disk): it lost no in-flight window, and a round here would
        // genesis-rewind live peers for nothing.
        if self.last_recovery_epoch.lock().is_none() {
            tracing::info!("no prior local state; skipping rejoin fault report");
            return;
        }
        // A coordinated round that raced the startup adopt loop already replayed this
        // process's window.
        if self
            .coordinated_restores
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            tracing::info!("already restored by a coordinated round; skipping rejoin fault report");
            return;
        }
        let Some(controller) = self.cluster_controller.lock().clone() else {
            return;
        };
        crate::coordinated_recovery::report_local_fault(&controller).await;
    }

    /// Start the per-node recovery monitor once. Coordinated recovery is the only cluster
    /// fault path — a local-only restart rewinds one node while peers advance, an
    /// inconsistent cut. Must be called from a Tokio runtime.
    #[cfg(feature = "cluster")]
    pub fn enable_coordinated_recovery(self: &Arc<Self>) {
        if self
            .recovery_monitor_started
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        crate::coordinated_recovery::spawn_monitor(self);
    }

    /// Close resources created by an unsuccessful `start_inner` attempt.
    async fn cleanup_failed_start(&self) -> Result<(), DbError> {
        const CLEANUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
        let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
        if let Some(handle) = self.committer_handle.lock().take() {
            handle.abort();
        }
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
        let handles = std::mem::take(&mut *self.startup_sink_handles.lock());
        for handle in handles {
            if let Err(error) = handle.close().await {
                tracing::warn!(
                    sink = %handle.name(),
                    %error,
                    "sink close failed while cleaning up unsuccessful pipeline start"
                );
            }
        }
        *self.control_tx.lock() = None;
        *self.force_ckpt_tx.lock() = None;
        *self.exact_deployment_lock.lock() = None;
        Ok(())
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
    pub async fn start(&self) -> Result<(), DbError> {
        const FAULT_RESTART_QUIESCE_TIMEOUT: std::time::Duration =
            std::time::Duration::from_secs(10);
        let _lifecycle = self.lifecycle_lock.lock().await;
        // CAS-claim the start so a supervisor racing a manual start can't both enter
        // start_inner and spawn two pipelines over the same state.
        let starting_from_fault = loop {
            match DbState::load(&self.state) {
                DbState::Running | DbState::Starting => return Ok(()),
                DbState::Stopped => {
                    return Err(DbError::InvalidOperation(
                        "Cannot start a stopped pipeline. Create a new LaminarDB instance.".into(),
                    ));
                }
                DbState::ShuttingDown => {
                    return Err(DbError::InvalidOperation(
                        "cannot start pipeline: shutdown/stop in progress".into(),
                    ));
                }
                // Faulted and Created are both startable; a lost CAS re-reads.
                claimed @ (DbState::Created | DbState::Faulted) => {
                    if DbState::compare_exchange(claimed, DbState::Starting, &self.state).is_ok() {
                        break claimed == DbState::Faulted;
                    }
                }
            }
        };

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
        self.restore_catalog_from_manifest().await;

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

        #[cfg(feature = "cluster")]
        let startup_runtime = if self.cluster_controller.lock().is_some() {
            RuntimeMode::Cluster
        } else {
            RuntimeMode::Local
        };
        #[cfg(not(feature = "cluster"))]
        let startup_runtime = RuntimeMode::Local;

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
                "[LDB-0013] cluster exactly-once is not admitted: the durable leader lease is \
                 not atomically bound to checkpoint decisions or sink commits. Use cluster \
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

        if self.config.checkpoint.is_some()
            && self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
        {
            // Without an object-store URL the checkpoint store is a local directory and thus
            // survives a same-node process restart. Explicit URLs are classified fail-closed;
            // notably memory:// cannot own source acknowledgements under a replay guarantee.
            let checkpoint_scope = self.config.object_store_url.as_deref().map_or(
                StateBackendDurability::NodeDurable,
                StateBackendDurability::for_storage_url,
            );
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
            if cp_config.max_in_flight_epochs == Some(0) || cp_config.max_staged_bytes == Some(0) {
                return Err(DbError::Config(
                    "checkpoint in-flight, staged-byte, and uncommitted-epoch caps must be \
                     greater than zero"
                        .into(),
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
            if let Some(chain_max) = cp_config.delta_chain_max {
                if startup_runtime != RuntimeMode::Cluster {
                    return Err(DbError::Config(
                        "checkpoint.delta_chain_max is supported only in cluster mode".into(),
                    ));
                }
                if chain_max == 0 || max_retained < 2 || chain_max as usize >= max_retained {
                    return Err(DbError::Config(format!(
                        "checkpoint.delta_chain_max must be > 0 and < max_retained \
                         ({max_retained})"
                    )));
                }
            }
            let vnode_count = self.vnode_registry.lock().as_ref().map_or(
                laminar_core::storage::checkpoint_manifest::DEFAULT_VNODE_COUNT,
                |r| u16::try_from(r.vnode_count()).unwrap_or(u16::MAX),
            );

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
                identity_registrations,
                vnode_count,
                participant.is_some(),
            );
            let pipeline_identity = crate::pipeline_identity::compute(&identity_context)?;

            let (store, decision_backing): (
                Box<dyn laminar_core::storage::CheckpointStore>,
                Arc<dyn object_store::ObjectStore>,
            ) = if let Some(ref url) = self.config.object_store_url {
                let obj = laminar_core::storage::object_store_builder::build_object_store(
                    url,
                    &self.config.object_store_options,
                )
                .map_err(|e| DbError::Config(format!("object store: {e}")))?;
                let cs = laminar_core::storage::checkpoint_store::ObjectStoreCheckpointStore::new(
                    Arc::clone(&obj),
                    participant.map_or_else(String::new, |id| format!("nodes/{id}/")),
                )
                .with_vnode_count(vnode_count)
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
                .with_vnode_count(vnode_count)
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
                max_in_flight_epochs: cp_config
                    .max_in_flight_epochs
                    .unwrap_or(defaults.max_in_flight_epochs),
                max_staged_bytes: cp_config
                    .max_staged_bytes
                    .unwrap_or(defaults.max_staged_bytes),
                ..defaults
            };
            let mut coord = CheckpointCoordinator::new(config, store).await?;
            coord.bind_pipeline_identity(pipeline_identity)?;
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

            if let (Some(backend), Some(registry)) = (
                self.state_backend.lock().clone(),
                self.vnode_registry.lock().clone(),
            ) {
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
                // Coordinator and backend must agree on the same generation —
                // the coordinator stamps writes, the backend rejects stale ones.
                // Without the backend call the fence is a silent no-op.
                let version = registry.assignment_version();
                backend.set_authoritative_version(version);
                coord.set_state_backend(backend);
                coord.set_assignment_version(version);
                coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, owner));
                // Leader gate covers all instances; 2PC only fires when every follower committed.
                coord.set_gate_vnode_set((0..registry.vnode_count()).collect());
            }

            let ds = {
                #[cfg(feature = "cluster")]
                {
                    self.decision_store.lock().clone().unwrap_or_else(|| {
                        Arc::new(
                            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                                Arc::clone(&decision_backing),
                            ),
                        )
                    })
                }
                #[cfg(not(feature = "cluster"))]
                {
                    Arc::new(
                        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
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
            coord.bind_deployment_id(deployment_id)?;

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
        use laminar_connectors::reference::{ReferenceTableSource, RefreshMode};

        #[cfg(feature = "cluster")]
        let runtime_mode = if self.cluster_controller.lock().is_some() {
            RuntimeMode::Cluster
        } else {
            RuntimeMode::Local
        };
        #[cfg(not(feature = "cluster"))]
        let runtime_mode = RuntimeMode::Local;
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
            if let Err(e) = ctx.register_table(&name, Arc::new(provider)) {
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
        graph.set_max_state_bytes(self.config.max_state_bytes_per_operator);
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
                if self.state_backend.lock().is_some() {
                    graph.set_vnode_partials_authoritative();
                    tracing::info!(
                        "cluster agg: per-vnode partials authoritative (no manifest copy)"
                    );
                }
                // Incremental delta checkpoints (opt-in). Startup validation proved the chain
                // bound fits strictly inside the retention window.
                if let Some(cp) = self.config.checkpoint.as_ref() {
                    if let Some(chain_max) = cp.delta_chain_max {
                        graph.set_delta_chain_max(chain_max);
                        // Enabling delta makes the chain the primary aggregate checkpoint.
                        tracing::info!(
                            delta_chain_max = chain_max,
                            "delta checkpoints enabled (chain is the primary aggregate checkpoint)"
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
            .filter(|r| matches!(r.refresh, Some(RefreshMode::Manual)))
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

        // `set_state_tier` must precede `add_query` so operators built below pick up the sender.
        #[cfg(feature = "state-tier")]
        let state_tier_sender: Option<crate::state_tier::TierTx> = {
            match self.config.state_tier_dir.clone() {
                Some(dir) => {
                    let has_backend = self.state_backend.lock().is_some();
                    if graph.vnode_count().is_none() {
                        if let Some(registry) = self.vnode_registry.lock().clone() {
                            // Single-node (no controller): take vnode count from the registry.
                            graph.set_vnode_count(registry.vnode_count());
                        }
                    }
                    let has_topology = graph.vnode_count().is_some();
                    if has_backend && has_topology {
                        let handle = self
                            .ai_handle
                            .clone()
                            .unwrap_or_else(tokio::runtime::Handle::current);
                        let metrics = self.engine_metrics.lock().clone();
                        match crate::state_tier::StateTierStore::open(&dir, metrics) {
                            Ok(store) => {
                                let sender =
                                    crate::state_tier::spawn_worker(&handle, Arc::new(store), 256);
                                graph.set_state_tier(sender.clone());
                                // Group demotion: dirty-track agg deltas (no delta chain) so idle groups
                                // are demotable to cold-only durable partials, merged back on recovery.
                                if self.config.state_tier_group_demotion {
                                    graph.enable_group_delta_tracking();
                                }
                                tracing::info!(dir = %dir.display(), "state cold tier enabled");
                                Some(sender)
                            }
                            Err(e) => {
                                tracing::error!(error = %e, dir = %dir.display(), "failed to open state cold tier — demotion disabled");
                                None
                            }
                        }
                    } else {
                        tracing::warn!(
                            has_backend,
                            has_topology,
                            "state_tier_dir set but demotion disabled — the tier \
                             needs a durable [state] backend (holds demoted state \
                             for restart) and a vnode registry (single-node: a \
                             single-owner registry)"
                        );
                        None
                    }
                }
                None => None,
            }
        };
        #[cfg(feature = "state-tier")]
        if let Some(ref sender) = state_tier_sender {
            let mut guard = self.coordinator.lock().await;
            if let Some(ref mut coord) = *guard {
                coord.set_state_tier(sender.clone());
            }
        }

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
                None,
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
                    .to_record_batch(&tcfg.table_name)
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
                .create_source(&config, prom_registry.as_deref())
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
            #[cfg(feature = "cluster")]
            if let (Some(registry), Some(self_id)) = (
                self.vnode_registry.lock().clone(),
                self.cluster_controller
                    .lock()
                    .as_ref()
                    .map(|c| laminar_core::state::NodeId(c.instance_id().0)),
            ) {
                source.set_vnode_assignment(registry, self_id);
            }
            // WebSocket extraction can synthesize event time from an inbound JSON field. Kafka
            // uses the SQL `WATERMARK FOR` declaration as its single event-time authority.
            if let Some(entry) = self.catalog.get_source(name) {
                if entry.source.event_time_column().is_none() {
                    if let Some(col) = config.get("event.time.field") {
                        entry.source.set_event_time_column(col);
                    }
                }
            }

            sources.push(SourceRegistration {
                name: name.clone(),
                connector: source,
                config,
                contract,
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
                .create_sink(&config, prom_registry.as_deref())
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
            prepared_sinks.push(PreparedSink {
                name: name.clone(),
                connector: sink,
                config,
                filter_expr: reg.filter_expr.clone(),
                input: reg.input.clone(),
                contract,
                write_timeout,
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
                    flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
                    write_timeout,
                    event_tx: sink_event_tx.clone(),
                });
            self.startup_sink_handles.lock().push(handle.clone());
            sinks.push((name, handle, filter_expr, input, contract));
        }
        drop(sink_event_tx);

        let mut table_sources: Vec<(String, Box<dyn ReferenceTableSource>, RefreshMode)> =
            Vec::new();
        for (name, reg) in &table_regs {
            if reg.connector_type.is_none() {
                continue;
            }
            let config = build_table_config(reg)?;
            let source = self
                .connector_registry
                .create_table_source(&config)
                .map_err(|e| {
                    DbError::Connector(format!("Cannot create table source '{name}': {e}"))
                })?;
            let mode = reg.refresh.clone().unwrap_or(RefreshMode::SnapshotPlusCdc);
            table_sources.push((name.clone(), source, mode));
        }

        let (coordinated_committer, committer_poll, committer_notify) = {
            let mut guard = self.coordinator.lock().await;
            if let Some(coord) = guard.as_mut() {
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

        {
            let guard = self.coordinator.lock().await;
            if let Some(ref coord) = *guard {
                // Sinks are registered, so resolve an unambiguous prior prepare before choosing
                // the recovery cut. Finalizing or rolling back after recovery could change the
                // durable frontier underneath state/source restoration.
                coord.reconcile_prepared_on_init().await?;
            }
        }

        // Must run BEFORE begin_initial_epoch so the epoch reflects the recovered state.
        // Hoist watermarks now so generators are seeded before watermark-state construction;
        // without this, generators restart at i64::MIN while offsets resume mid-stream.
        let mut recovered_source_wms: rustc_hash::FxHashMap<String, i64> =
            rustc_hash::FxHashMap::default();
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
                        recovered_source_wms = recovered
                            .manifest
                            .source_watermarks
                            .iter()
                            .filter(|(_, &wm)| wm != i64::MIN)
                            .map(|(name, &wm)| (name.clone(), wm))
                            .collect();
                        for (name, source, _) in &mut table_sources {
                            if let Some(cp) = recovered.manifest.table_offsets.get(name) {
                                let restored =
                                    crate::checkpoint_coordinator::connector_to_source_checkpoint(
                                        cp,
                                    );
                                if let Err(e) = source.restore(&restored).await {
                                    return Err(DbError::Checkpoint(format!(
                                        "table source restore failed for '{name}': {e}"
                                    )));
                                }
                            }
                        }
                        // Each manifest is participant-scoped, so the validated seal-bound union
                        // fills partitions captured by other participants.
                        let recovered_attempt = laminar_core::state::CheckpointAttempt::new(
                            recovered.manifest.epoch,
                            recovered.manifest.checkpoint_id,
                        );
                        #[cfg(feature = "cluster")]
                        let empty_handoff = HashMap::new();
                        #[cfg(feature = "cluster")]
                        let handoff = recovered.cluster_source_handoff().unwrap_or(&empty_handoff);
                        for src in &mut sources {
                            if !src.contract.supports_replay() {
                                continue;
                            }
                            let manifest_cp = recovered.manifest.source_offsets.get(&src.name);
                            #[cfg(feature = "cluster")]
                            let restored = Self::recovery_source_checkpoint(
                                &src.name,
                                manifest_cp,
                                handoff,
                                recovered.epoch(),
                            );
                            #[cfg(not(feature = "cluster"))]
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
                        if let Some(op) = recovered.manifest.operator_states.get("operator_graph") {
                            if let Some(bytes) = op.decode_inline() {
                                match graph.restore_from_bytes(&bytes) {
                                    Ok(n) => {
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

                        // Cold-tier vnodes are wiped on restart; replay them from durable partials.
                        // Only the demoting operator's slice is applied — others restored from
                        // the manifest (double-applying would corrupt state).
                        #[cfg(feature = "state-tier")]
                        {
                            let cold_map = graph.take_tier_cold_vnodes();
                            if !cold_map.is_empty() {
                                self.rehydrate_cold_vnodes(
                                    &mut graph,
                                    &cold_map,
                                    recovered_attempt,
                                )
                                .await?;
                            }
                        }

                        // Rebuild aggregates from each boot-owned vnode's chain at the recovered
                        // cut — the same epoch the source offsets resume from.
                        #[cfg(feature = "cluster")]
                        if self.state_backend.lock().is_some() {
                            self.stage_owned_vnodes_from_chains(recovered_attempt)
                                .await?;
                        }

                        // The manifest's MV rows are one writer's slice: restoring keyed rows
                        // on a cluster node plants ghost keys the distributed union double
                        // counts; adopt's force-emit rebuilds them. Append MVs keep restoring.
                        #[cfg(feature = "cluster")]
                        let skip_keyed = self.cluster_controller.lock().is_some()
                            && self.state_backend.lock().is_some();
                        #[cfg(not(feature = "cluster"))]
                        let skip_keyed = false;
                        let prefix = crate::mv_store::CHECKPOINT_KEY_PREFIX;
                        let mut store = self.mv_store.write();
                        let mut restored = 0usize;
                        let mut skipped = 0usize;
                        for (key, op) in &recovered.manifest.operator_states {
                            if let Some(name) = key.strip_prefix(prefix) {
                                if skip_keyed && store.is_keyed_changelog(name) {
                                    skipped += 1;
                                    continue;
                                }
                                if let Some(bytes) = op.decode_inline() {
                                    match store.restore_from_ipc(name, &bytes) {
                                        Ok(true) => restored += 1,
                                        Ok(false) => {
                                            return Err(DbError::Checkpoint(format!(
                                                "MV checkpoint '{name}' has no matching \
                                                 registered materialized view"
                                            )));
                                        }
                                        Err(e) => {
                                            return Err(DbError::Checkpoint(format!(
                                                "MV restore failed for '{name}': {e}"
                                            )));
                                        }
                                    }
                                } else {
                                    return Err(DbError::Checkpoint(format!(
                                        "MV checkpoint '{name}' is not inline after sidecar \
                                             resolution"
                                    )));
                                }
                            }
                        }
                        if restored > 0 || skipped > 0 {
                            tracing::info!(
                                mvs = restored,
                                skipped_keyed = skipped,
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
                        tracing::info!("No checkpoint found, starting fresh");
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
        }

        {
            let guard = self.coordinator.lock().await;
            if let Some(ref coord) = *guard {
                // Reconciliation already ran after sink registration and before recovery.
                coord.begin_initial_epoch().await?;
            }
        }

        for (name, source, mode) in &mut table_sources {
            if matches!(mode, RefreshMode::Manual) {
                continue;
            }
            while let Some(batch) = source
                .poll_snapshot()
                .await
                .map_err(|e| DbError::Connector(format!("Table '{name}' snapshot error: {e}")))?
            {
                self.table_store
                    .write()
                    .upsert(name, &batch)
                    .map_err(|e| DbError::Connector(format!("Table '{name}' upsert error: {e}")))?;
            }
            self.sync_table_to_datafusion(name)?;
            {
                let mut ts = self.table_store.write();
                ts.rebuild_xor_filter(name);
                ts.set_ready(name, true);
            }
            // Setup built the Versioned (temporal-join) state before the
            // snapshot existed; rebuild it over the snapshot now instead of
            // downgrading it to a plain Snapshot.
            let entry = self.lookup_registry.get_entry(name);
            if let Some(laminar_sql::datafusion::RegisteredLookup::Versioned(v)) = &entry {
                if let Some(batch) = self.table_store.read().to_record_batch(name) {
                    if let Some(state) = crate::pipeline_callback::rebuild_versioned_state(v, batch)
                    {
                        self.lookup_registry.register_versioned(name, state);
                    }
                }
            } else if let Some(batch) = self.table_store.read().to_record_batch(name) {
                self.lookup_registry
                    .register(name, laminar_sql::datafusion::LookupSnapshot { batch });
            }
        }

        for (name, _source, mode) in &mut table_sources {
            if !matches!(mode, RefreshMode::Manual) {
                continue;
            }
            let Some(reg) = table_regs.get(name.as_str()) else {
                continue;
            };
            let capacity_bytes = reg.cache_max_bytes.unwrap_or(64 * 1024 * 1024);
            let Some(schema) = self.table_store.read().table_schema(name) else {
                continue;
            };
            let pk_csv = &reg.primary_key;
            let pk_cols: Vec<String> = pk_csv
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let key_sort_fields: Vec<arrow::row::SortField> = pk_cols
                .iter()
                .filter_map(|col| {
                    schema
                        .field_with_name(col)
                        .ok()
                        .map(|f| arrow::row::SortField::new(f.data_type().clone()))
                })
                .collect();

            let cache = Arc::new(laminar_core::lookup::lookup_cache::LookupMemoryCache::new(
                0,
                laminar_core::lookup::lookup_cache::LookupMemoryCacheConfig {
                    capacity_bytes,
                    ttl: reg.cache_ttl,
                },
            ));
            let lookup_source = if let Ok(mut config) = build_table_config(reg) {
                config.set("_primary_key_columns", pk_csv.as_str());
                match self
                    .connector_registry
                    .create_lookup_source(config, Some(Arc::clone(&schema)))
                    .await
                {
                    Some(Ok(src)) => Some(src),
                    Some(Err(e)) => {
                        tracing::warn!(
                            table = %name, error = %e,
                            "lookup source creation failed; cache-only mode"
                        );
                        None
                    }
                    None => None,
                }
            } else {
                None
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
                    source: lookup_source,
                    fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(16)),
                    projection,
                },
            );
            *mode = RefreshMode::SnapshotPlusCdc;
            tracing::info!(
                table = %name,
                capacity_bytes,
                ttl = ?reg.cache_ttl,
                pk = %pk_csv,
                "registered on-demand lookup table (partial cache)"
            );
        }

        let mut stream_sources: Vec<(String, streaming::Source<crate::catalog::ArrowRecord>)> =
            Vec::new();
        for reg in stream_regs.values() {
            if let Some(src) = self.catalog.get_stream_source(&reg.name) {
                stream_sources.push((reg.name.clone(), src));
            }
        }

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

        // Fallback watermark path for sources that use the programmatic API
        // or connector properties instead of `WATERMARK FOR`.
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

        if !recovered_source_wms.is_empty() {
            let mut combined = i64::MIN;
            for (name, wm) in &recovered_source_wms {
                if let Some(state) = watermark_states.get_mut(name) {
                    let _ = state.generator.advance_watermark(*wm);
                }
                if let (Some(t), Some(&id)) = (tracker.as_mut(), source_ids.get(name)) {
                    if let Some(global) = t.update_source(id, *wm) {
                        combined = combined.max(global.timestamp());
                    }
                }
            }
            if combined != i64::MIN {
                self.pipeline_watermark
                    .store(combined, std::sync::atomic::Ordering::SeqCst);
                tracing::info!(
                    sources = recovered_source_wms.len(),
                    pipeline_watermark = combined,
                    "Restored watermarks from checkpoint"
                );
            }
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
            subscriptions = stream_sources.len(),
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

        {
            use laminar_connectors::connector::DeliveryGuarantee;

            if pipeline_config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
                && pipeline_config.checkpoint_interval.is_none()
            {
                return Err(DbError::Config(
                    "[LDB-5032] exactly-once requires checkpointing to be enabled. \
                     Set checkpoint.interval.ms in the pipeline configuration."
                        .into(),
                ));
            }

            // Drive the pre-rotation drain off the sinks, not the DB-level guarantee (set per sink
            // by the server): an EO sink can't dedup a rotation dup, so rotation pauses the source.
            let has_eo_sink = sinks
                .iter()
                .any(|(_, h, _, _, _)| h.checkpoint_committable());
            self.rotation_drain_required
                .store(has_eo_sink, std::sync::atomic::Ordering::Release);
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
        // Cap in-flight epochs at 1 when any sink is checkpoint-committable. DDL-configured sinks
        // declare this through their admitted contract, so the runtime cannot pipeline output
        // from a successor epoch into a still-open transaction or staged descriptor.
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
            max_in_flight_epochs,
            max_staged_bytes,
            coordinated_commit_admission,
        ) = {
            let guard = coordinator.lock().await;
            match guard.as_ref() {
                Some(coord) => {
                    let cfg = coord.config();
                    // Delta epochs chain onto the prior durable one and capture is destructive, so a
                    // pipelined epoch bakes in a not-yet-known failure's gap. Serialize (depth 1).
                    let delta_enabled = self
                        .config
                        .checkpoint
                        .as_ref()
                        .and_then(|c| c.delta_chain_max)
                        .is_some();
                    let depth = if has_checkpoint_committable_sink
                        || pipeline_config.delivery_guarantee
                            == laminar_connectors::connector::DeliveryGuarantee::ExactlyOnce
                        || delta_enabled
                    {
                        1
                    } else {
                        cfg.max_in_flight_epochs.max(1)
                    };
                    (
                        Some(coord.epoch_allocator()),
                        cfg.quorum_timeout,
                        cfg.checkpoint_timeout,
                        cfg.cleanup_timeout,
                        depth,
                        cfg.max_staged_bytes.max(1), // 0 would pause admission permanently
                        coord.coordinated_commit_admission(),
                    )
                }
                None => (
                    None,
                    std::time::Duration::from_secs(3),
                    std::time::Duration::from_secs(120),
                    crate::checkpoint_coordinator::CheckpointConfig::default().cleanup_timeout,
                    1,
                    u64::MAX,
                    None,
                ),
            }
        };
        #[cfg(not(feature = "cluster"))]
        let _ = ckpt_quorum_timeout;

        let static_stream_names: rustc_hash::FxHashSet<Arc<str>> = stream_sources
            .iter()
            .map(|(name, _)| Arc::from(name.as_str()))
            .collect();

        // Snapshot the controller once: locking the same `parking_lot::Mutex` twice
        // within the struct literal below would deadlock (the first guard lives until
        // the statement ends).
        #[cfg(feature = "cluster")]
        let callback_controller = self.cluster_controller.lock().clone();

        let callback = crate::pipeline_callback::ConnectorPipelineCallback {
            graph,
            stream_sources,
            sinks,
            watermark_states,
            source_entries_for_wm,
            source_ids,
            source_name_arcs,
            source_wms_buf,
            tracker,
            prom,
            pipeline_watermark,
            coordinator,
            table_sources,
            table_store: table_store_for_loop,
            mv_store_has_any: self.mv_store.read().has_any_handle(),
            mv_store: self.mv_store.clone(),
            lookup_registry: Arc::clone(&self.lookup_registry),
            filter_ctx: laminar_sql::create_session_context(),
            compiled_sink_filters: Vec::new(),
            pending_sink_filter_compiles: sinks_pending_filter_count,
            delivery_guarantee: pipeline_config.delivery_guarantee,
            serialization_timeout: checkpoint_timeout,
            checkpoint_timeout,
            checkpoint_cleanup_timeout,
            sink_event_rx,
            sink_timed_out: false,
            sink_fault: None,
            checkpoint_fault: Arc::new(parking_lot::Mutex::new(None)),
            shutdown_signal: Arc::clone(&self.shutdown_signal),
            #[cfg(feature = "cluster")]
            converged_rx: callback_controller.as_ref().map(|cc| cc.converged_watch()),
            #[cfg(feature = "cluster")]
            cluster_controller: callback_controller,
            #[cfg(feature = "cluster")]
            follower_tail: Arc::default(),
            #[cfg(feature = "cluster")]
            barrier_injectors: Vec::new(),
            #[cfg(feature = "cluster")]
            shuffle_lost: self
                .shuffle_receiver
                .lock()
                .as_ref()
                .map(|r| r.lost_frames()),
            #[cfg(feature = "cluster")]
            shuffle_lost_seen: 0,
            #[cfg(feature = "cluster")]
            pending_follower_checkpoint: None,
            subscription_registry: Arc::clone(&self.subscription_registry),
            #[cfg(feature = "cluster")]
            active_subs: Arc::clone(&self.active_subs),
            #[cfg(feature = "cluster")]
            sub_route: std::sync::OnceLock::new(),
            static_stream_names,
            checkpoint_complete_tx,
            checkpoint_tail_tasks: tokio::task::JoinSet::new(),
            checkpoint_in_flight: Arc::clone(&checkpoint_in_flight),
            staged_bytes: Arc::clone(&staged_bytes),
            #[cfg(feature = "cluster")]
            delta_rebase_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            epoch_allocator,
            #[cfg(feature = "cluster")]
            quorum_timeout: ckpt_quorum_timeout,
            checkpoint_committable_sinks: has_checkpoint_committable_sink,
            state_memory_budget_bytes: self.config.state_memory_budget_bytes,
            // Backdated so the first cycle probes immediately.
            state_budget_probe_at: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(3600))
                .unwrap_or_else(std::time::Instant::now),
            state_budget_exceeded: false,
            #[cfg(feature = "state-tier")]
            state_tier: state_tier_sender,
            #[cfg(feature = "state-tier")]
            state_tier_group_demotion: self.config.state_tier_group_demotion,
        };

        {
            let (control_tx, control_rx) =
                crossfire::mpsc::bounded_async::<crate::pipeline::ControlMsg>(64);
            *self.control_tx.lock() = Some(control_tx);

            #[cfg(feature = "cluster")]
            let source_gate = Arc::clone(&self.source_gate);
            #[cfg(not(feature = "cluster"))]
            let source_gate = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let coordinator = crate::pipeline::StreamingCoordinator::new(
                sources,
                pipeline_config,
                Arc::clone(&shutdown),
                control_rx,
                source_gate,
            )
            .await?
            .with_force_checkpoint_rx(force_ckpt_rx)
            .with_checkpoint_complete_rx(checkpoint_complete_rx)
            .with_checkpoint_admission(
                checkpoint_in_flight,
                max_in_flight_epochs,
                staged_bytes,
                max_staged_bytes,
            )
            .with_coordinated_commit_admission(coordinated_commit_admission);

            let (done_tx, done_rx) = crossfire::oneshot::oneshot::<()>();
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
                            coordinator.run_with_ready(callback, startup_tx).await
                        })
                    }));
                    // Runtime shutdown waits for non-abortable `spawn_blocking` filesystem work.
                    // Publish neither clean completion nor a fault until those workers are gone;
                    // otherwise lifecycle teardown could release the exact namespace lock while
                    // an old local decision hard-link was still able to appear.
                    drop(rt);
                    // Panic and fault both drop `done_tx` unsent so the watcher faults.
                    let fault_reason = match result {
                        Ok(crate::pipeline::ExitReason::Shutdown) => None,
                        Ok(crate::pipeline::ExitReason::Fault(reason)) => {
                            tracing::error!(
                                reason = %reason,
                                "pipeline faulted on a fatal cycle error; recovering from last checkpoint"
                            );
                            Some(reason)
                        }
                        Err(panic) => {
                            let msg = panic
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| panic.downcast_ref::<&str>().copied())
                                .unwrap_or("unknown");
                            tracing::error!(panic = msg, "laminar-compute thread panicked");
                            Some(msg.to_string())
                        }
                    };
                    if let Some(reason) = fault_reason {
                        // Publish the fault before dropping `done_tx`. In particular, this closes
                        // the ready-send -> watcher-scheduled window in which start() could
                        // otherwise report Running after the compute loop had already exited.
                        *fault_slot.lock() = Some(reason);
                        DbState::Faulted.store(&fault_state);
                        if let Some(ref m) = fault_metrics {
                            m.pipeline_faults_total.inc();
                        }
                        return;
                    }
                    done_tx.send(());
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

            let watcher_state = Arc::clone(&self.state);
            let watcher_shutdown = Arc::clone(&self.shutdown_signal);
            let watcher_fault = Arc::clone(&self.last_fault);
            let watcher_supervisor = Arc::clone(&self.supervisor_self);
            let watcher_restart_history = Arc::clone(&self.restart_history);
            let watcher_metrics = self.engine_metrics.lock().clone();
            #[cfg(feature = "cluster")]
            let watcher_controller = self.cluster_controller.lock().clone();
            let handle = tokio::spawn(async move {
                if done_rx.await.is_ok() {
                    // Lifecycle ownership finalizes the state only after every remote decision
                    // writer has settled. The watcher cannot prove that merely because the
                    // compute thread exited, so a timed-out stop remains ShuttingDown until retry.
                } else {
                    tracing::error!("laminar-compute thread exited unexpectedly");
                    watcher_fault
                        .lock()
                        .get_or_insert_with(|| "compute thread exited unexpectedly".to_string());
                    // Faulted, not Stopped — recoverable via a later start().
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
                        let _ =
                            spawn_supervised_restart(db, watcher_restart_history, watcher_metrics);
                    }
                }
            });

            *self.runtime_handle.lock() = Some(handle);
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
            let mut guard = self.committer_handle.lock();
            if let Some(previous) = guard.replace(handle) {
                previous.abort();
            }
        }
        self.startup_sink_handles.lock().clear();
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
        let deadline = tokio::time::Instant::now() + SHUTDOWN_TIMEOUT;
        let _lifecycle = tokio::time::timeout_at(deadline, self.lifecycle_lock.lock())
            .await
            .map_err(|_| {
                DbError::Pipeline(format!(
                    "pipeline shutdown could not acquire lifecycle ownership within \
                     {SHUTDOWN_TIMEOUT:?}; startup/stop remains fenced"
                ))
            })?;

        let state = DbState::load(&self.state);
        if matches!(state, DbState::Stopped) {
            return Ok(());
        }

        // A prior timed-out call leaves the watcher handle and deployment lock owned here. A
        // retry must resume waiting instead of returning a false success while the compute thread
        // is still live.
        if !matches!(state, DbState::ShuttingDown) {
            DbState::ShuttingDown.store(&self.state);

            *self.force_ckpt_tx.lock() = None;

            self.shutdown_signal.notify_one();
        }

        let handle = self.runtime_handle.lock().take();
        let mut watcher_error = None;
        if let Some(mut handle) = handle {
            match tokio::time::timeout_at(deadline, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    watcher_error = Some(DbError::Pipeline(format!(
                        "pipeline watcher failed during shutdown: {error}"
                    )));
                }
                Err(_) => {
                    // Dropping this JoinHandle would detach only the watcher while the dedicated
                    // compute thread continued. Retain every owner/fence and let a later call
                    // resume the wait.
                    *self.runtime_handle.lock() = Some(handle);
                    return Err(DbError::Pipeline(format!(
                        "pipeline shutdown exceeded {SHUTDOWN_TIMEOUT:?}; runtime is still \
                         draining and remains fenced in ShuttingDown; retry shutdown"
                    )));
                }
            }
        }
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

        let committer = self.committer_handle.lock().take();
        if let Some(handle) = committer {
            handle.abort();
            let _ = handle.await;
        }

        *self.exact_deployment_lock.lock() = None;
        DbState::Stopped.store(&self.state);
        self.close();
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
        let _lifecycle = tokio::time::timeout_at(deadline, self.lifecycle_lock.lock())
            .await
            .map_err(|_| {
                DbError::InvalidOperation(
                    "pipeline stop could not acquire lifecycle ownership within 10s; an earlier \
                     lifecycle operation remains fenced"
                        .into(),
                )
            })?;
        let first_stop = loop {
            match DbState::load(&self.state) {
                DbState::Running | DbState::Faulted => {
                    let observed = DbState::load(&self.state);
                    if DbState::compare_exchange(observed, DbState::ShuttingDown, &self.state)
                        .is_ok()
                    {
                        break true;
                    }
                }
                DbState::ShuttingDown => break false,
                DbState::Created | DbState::Stopped => return Ok(()),
                DbState::Starting => {
                    return Err(DbError::InvalidOperation(
                        "cannot stop pipeline while startup is in progress".into(),
                    ));
                }
            }
        };

        if first_stop {
            *self.force_ckpt_tx.lock() = None;
            // Clear up front so DDL during/after shutdown registers for the next start()
            // instead of hot-adding into the dying coordinator's channel.
            *self.control_tx.lock() = None;
            self.shutdown_signal.notify_one();
        }

        let handle = self.runtime_handle.lock().take();
        if let Some(mut handle) = handle {
            match tokio::time::timeout_at(deadline, &mut handle).await {
                Ok(Ok(())) => tracing::info!("Pipeline stopped cleanly"),
                Ok(Err(e)) => tracing::warn!(error = %e, "Pipeline task panicked during stop"),
                Err(_) => {
                    // Still draining. Re-store the watcher handle and retain ShuttingDown plus
                    // every durability fence; a later stop call resumes the join and quiescence.
                    tracing::warn!(
                        "Pipeline stop still draining after 10s; will finalize when the coordinator exits"
                    );
                    *self.runtime_handle.lock() = Some(handle);
                    return Err(DbError::InvalidOperation(
                        "pipeline stop is taking longer than expected; coordinator still \
                         draining, retry shortly"
                            .into(),
                    ));
                }
            }
        }

        // Do not announce Created or release the exclusive deployment lock while a timed-out
        // decision create can still mutate the recovery frontier. A later stop retry resumes here.
        self.quiesce_checkpoint_decision_until(deadline).await?;

        let committer = self.committer_handle.lock().take();
        if let Some(handle) = committer {
            handle.abort();
            let _ = handle.await;
        }

        *self.exact_deployment_lock.lock() = None;
        DbState::Created.store(&self.state);
        Ok(())
    }
}

#[cfg(test)]
mod connector_admission_tests {
    use super::LaminarDB;
    use super::{
        admit_sink, admit_sink_contract, admit_source_contract, close_opened_sinks,
        open_prepared_sinks, PreparedSink, RuntimeMode, SinkAdmissionContext, EXACT_SINK_PROTOCOL,
    };
    use crate::db::DbState;
    use crate::pipeline::PipelineConfig;
    use arrow_array::RecordBatch;
    use arrow_schema::{Schema, SchemaRef};
    use async_trait::async_trait;
    use laminar_connectors::config::ConnectorConfig;
    use laminar_connectors::connector::{
        DeliveryGuarantee, SinkConnector, SinkConsistency, SinkContract, SinkInputMode,
        SinkTopology, SourceConsistency, SourceContract, SourceTopology, WriteResult,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_core::state::StateBackendDurability;
    #[cfg(feature = "cluster")]
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

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

    #[cfg(feature = "cluster")]
    #[test]
    fn cluster_recovery_preserves_an_explicit_empty_source_checkpoint() {
        let handoff = HashMap::from([("kafka".to_owned(), HashMap::new())]);
        let restored = LaminarDB::recovery_source_checkpoint("kafka", None, &handoff, 7)
            .expect("sealed source participation must survive before the first record");
        assert!(restored.offsets().is_empty());

        assert!(LaminarDB::recovery_source_checkpoint("missing", None, &handoff, 7).is_none());
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

        for consistency in consistencies {
            for topology in topologies {
                for delivery in deliveries {
                    for runtime in runtimes {
                        for checkpointing_enabled in [false, true] {
                            let contract = SourceContract::new(consistency, topology);
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
                                && !(runtime == RuntimeMode::Cluster
                                    && delivery == DeliveryGuarantee::ExactlyOnce)
                                && (runtime != RuntimeMode::Cluster
                                    || topology == SourceTopology::Splittable
                                    || (topology == SourceTopology::NodeLocalIngress
                                        && delivery == DeliveryGuarantee::BestEffort));

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
                                 checkpointing_enabled={checkpointing_enabled}, runtime={runtime:?}"
                            );
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
    fn commit_coupled_exactly_once_requires_a_certified_barrier_cut() {
        let contract =
            SourceContract::new(SourceConsistency::CommitCoupled, SourceTopology::Singleton);
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
    }

    #[async_trait]
    impl SinkConnector for StartupProbeSink {
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
        PreparedSink {
            name: name.into(),
            connector: Box::new(StartupProbeSink {
                open_delay,
                close_delay,
                open_calls,
                close_calls,
                schema: Arc::new(Schema::empty()),
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

    #[tokio::test(start_paused = true)]
    async fn sink_startup_cleanup_uses_one_fresh_deadline_for_all_connectors() {
        let first_close = Arc::new(AtomicU64::new(0));
        let second_close = Arc::new(AtomicU64::new(0));
        let mut sinks = vec![
            prepared_lifecycle_probe(
                "first",
                Duration::ZERO,
                Duration::from_secs(10),
                Arc::new(AtomicU64::new(0)),
                Arc::clone(&first_close),
            ),
            prepared_lifecycle_probe(
                "second",
                Duration::ZERO,
                Duration::from_secs(10),
                Arc::new(AtomicU64::new(0)),
                Arc::clone(&second_close),
            ),
        ];
        let started = tokio::time::Instant::now();
        let deadline = started + PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT;

        close_opened_sinks(&mut sinks, deadline).await;

        assert_eq!(
            tokio::time::Instant::now().duration_since(started),
            PipelineConfig::CONNECTOR_STARTUP_CLEANUP_TIMEOUT,
            "sequential closes must consume one shared budget, not reset it"
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
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn committed_checkpoint_id(
            &self,
            _namespace: &laminar_connectors::connector::CoordinatedCommitNamespace,
        ) -> Result<Option<u64>, ConnectorError> {
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
        let ctx = SessionContext::new();
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
    use std::sync::Arc;

    use laminar_connectors::connector::DeliveryGuarantee;
    use laminar_core::state::{NodeId, ObjectStoreBackend, VnodeRegistry};

    fn exact_builder(root: &std::path::Path) -> crate::builder::LaminarDbBuilder {
        let state_dir = root.join("state");
        std::fs::create_dir_all(&state_dir).unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&state_dir).unwrap());
        let backend = Arc::new(ObjectStoreBackend::node_durable(store, "node-0", 4));
        crate::db::LaminarDB::builder()
            .storage_dir(root.join("checkpoints"))
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: Some(1_000),
                data_dir: Some(root.join("checkpoints")),
                ..Default::default()
            })
            .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
            .state_backend(backend)
            .vnode_registry(Arc::new(VnodeRegistry::single_owner(4, NodeId(0))))
    }

    async fn exact_db(root: &std::path::Path) -> crate::db::LaminarDB {
        exact_builder(root).build().await.unwrap()
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
    async fn local_exact_rejects_an_injected_decision_store_with_erased_provenance() {
        let root = tempfile::tempdir().unwrap();
        let decision_store = Arc::new(
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::new(
                object_store::memory::InMemory::new(),
            )),
        );
        let db = exact_builder(root.path())
            .decision_store(decision_store)
            .build()
            .await
            .unwrap();

        let error = db
            .start()
            .await
            .expect_err("custom decision-store provenance cannot prove local process fencing");
        assert!(error.to_string().contains("[LDB-0014]"), "{error}");
    }
}

#[cfg(all(test, feature = "cluster"))]
mod cluster_fault_watcher_tests {
    use super::report_cluster_compute_fault;
    use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv, RecoverPhase};
    use laminar_core::cluster::discovery::{NodeId, NodeInfo};
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn active_recovery_does_not_block_compute_fault_handoff() {
        let node_id = NodeId(7);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
        controller.announce_recover_prepare(42).await;
        assert!(matches!(
            controller.observe_recover().await,
            Some((RecoverPhase::Prepare, 42))
        ));

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
            .into_iter()
            .any(|(node, sequence)| node == node_id && sequence > 0));
        assert!(matches!(
            controller.observe_recover().await,
            Some((RecoverPhase::Prepare, 42))
        ));
    }
}

#[cfg(test)]
mod supervisor_tests {
    use super::{backoff_for_attempt, claim_restart_slot, spawn_supervised_restart};
    use crate::config::RestartPolicy;
    use crate::db::{DbState, LaminarDB};
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

    // Drives the real watcher path (thread + block_on + start) on a multi-thread runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn supervised_restart_recovers_faulted_pipeline() {
        let db = Arc::new(LaminarDB::open().unwrap());
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
        tokio::task::spawn_blocking(move || join.join().expect("restart thread"))
            .await
            .unwrap();

        assert_eq!(db.pipeline_state(), "Running");
        assert!(db.last_fault().is_none());
        assert_eq!(metrics.pipeline_restarts_total.get(), 1);
        db.shutdown().await.unwrap();
    }
}
