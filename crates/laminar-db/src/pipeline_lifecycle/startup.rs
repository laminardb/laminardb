use super::{
    panic_message, publish_runtime_fault_state, required_recovery_scope, Arc,
    CheckpointStorageScope, DbError, DbState, DeliveryGuarantee, FutureExt, HashMap, LaminarDB,
    PipelineLifecycleAuthority, RuntimeMode, StartupAttempt, StartupDriverGuard,
};
#[cfg(feature = "cluster")]
use super::{report_cluster_terminal_halt, retire_cluster_compute_generation};
use laminar_core::storage_location::StorageProvider;

fn checkpoint_store(
    backing: Arc<dyn object_store::ObjectStore>,
    max_node_data_bytes: u64,
    key_group_count: laminar_core::state::KeyGroupCount,
    participant_id: u64,
    exclusive_writer: bool,
) -> Result<Box<dyn laminar_core::checkpoint::CheckpointStore>, DbError> {
    let store = laminar_core::checkpoint::ObjectStoreCheckpointStore::new(backing, "")
        .with_max_node_data_bytes(max_node_data_bytes)?
        .with_key_group_count(key_group_count)
        .with_participant_id(participant_id);
    let store = if exclusive_writer {
        store.with_exclusive_writer()
    } else {
        store
    };
    Ok(Box::new(store))
}

impl LaminarDB {
    /// Publish `Running` only if the compute watcher did not fault during startup.
    ///
    /// The compute thread publishes a competing `Starting -> Faulted` transition before exit and
    /// the watcher reinforces it. Ignoring a lost CAS here would let coordinated recovery
    /// acknowledge a process whose compute loop has already died.
    pub(super) fn finish_start_transition(&self) -> Result<(), DbError> {
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
        self.ensure_terminal_halt_allows_start(PipelineLifecycleAuthority::CoordinatedRecovery)?;
        // RECOVERY: a durable Start is published only after cluster-wide artifact settlement.
        *self.startup_checkpoint_artifact_audit.lock() = None;
        self.start_with_lifecycle_authority(PipelineLifecycleAuthority::CoordinatedRecovery)
            .await
    }

    pub(super) async fn start_with_lifecycle_authority(
        self: &Arc<Self>,
        authority: PipelineLifecycleAuthority,
    ) -> Result<(), DbError> {
        if self.is_closed() {
            return Err(DbError::Shutdown);
        }
        self.ensure_terminal_halt_allows_start(authority)?;
        #[cfg(feature = "cluster")]
        self.ensure_pipeline_lifecycle_authorized(authority, "start")?;
        #[cfg(not(feature = "cluster"))]
        Self::ensure_pipeline_lifecycle_authorized(authority, "start");
        self.connector_registry.freeze();
        let runtime = self.control_runtime.handle()?;
        let attempt = {
            let mut owned = self.startup_attempt.lock();
            self.ensure_terminal_halt_allows_start(authority)?;
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
                        self.ensure_terminal_halt_allows_start(authority)?;
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
                                            authority,
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

    pub(super) async fn drive_start_attempt(
        self: Arc<Self>,
        attempt: Arc<StartupAttempt>,
        starting_from_fault: bool,
        authority: PipelineLifecycleAuthority,
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
                    Ok(Err(error)) if error.requires_pipeline_halt() => {
                        tracing::error!(
                            panic = %reason,
                            cleanup_error = %error,
                            "terminal cleanup error superseded a generic startup panic"
                        );
                        Err(error)
                    }
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
        let result = self.normalize_start_result_for_terminal_latch(result);
        self.terminalize_start_attempt_if_needed(authority, &result)
            .await;
        terminal.finish(result);
    }

    pub(super) fn normalize_start_result_for_terminal_latch(
        &self,
        result: Result<(), DbError>,
    ) -> Result<(), DbError> {
        if !self
            .terminal_pipeline_halt
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return result;
        }
        match result {
            Err(error) if error.requires_pipeline_halt() => Err(error),
            Ok(()) | Err(_) => Err(DbError::PipelineTerminal(
                self.last_fault()
                    .unwrap_or_else(|| "terminal startup failure".into()),
            )),
        }
    }

    /// A startup owner outlives callers that time out while awaiting it. Terminalization therefore
    /// belongs here, before the sticky attempt result is published, rather than in a recovery
    /// monitor that may already have dropped its receiver.
    #[cfg_attr(
        not(feature = "cluster"),
        allow(unknown_lints, clippy::unused_async, clippy::unused_async_trait_impl)
    )]
    pub(super) async fn terminalize_start_attempt_if_needed(
        &self,
        authority: PipelineLifecycleAuthority,
        result: &Result<(), DbError>,
    ) {
        let Err(error) = result else {
            return;
        };
        if !error.requires_pipeline_halt() {
            return;
        }
        let _ = authority;
        let reason = if self
            .terminal_pipeline_halt
            .load(std::sync::atomic::Ordering::Acquire)
        {
            self.last_fault().unwrap_or_else(|| error.to_string())
        } else {
            error.to_string()
        };
        *self.last_fault.lock() = Some(reason.clone());

        #[cfg(feature = "cluster")]
        if self.is_cluster_runtime() {
            self.latch_local_terminal_pipeline_halt();
            let controller = self.cluster_controller.lock().clone();
            if let Some(controller) = controller.as_deref() {
                controller.set_recovering(true);
                if let Err(publication_error) = crate::coordinated_recovery::queue_local_fault(
                    controller,
                    &self.pending_recovery_fault,
                ) {
                    tracing::error!(
                        %publication_error,
                        "could not retain terminal startup fault request"
                    );
                }
            }
            let generation = Arc::clone(&self.rotation_execution_fence)
                .write_owned()
                .await;
            retire_cluster_compute_generation(
                &self.pending_vnode_transition,
                &self.installed_vnode_state,
            );
            publish_runtime_fault_state(&self.state);
            drop(generation);
            tracing::error!(
                %reason,
                "pipeline startup hit a permanent error; awaiting durable terminal fencing"
            );
            report_cluster_terminal_halt(controller, Arc::clone(&self.pending_recovery_fault))
                .await;
            self.latch_durable_terminal_recovery_fence();
            return;
        }

        self.terminal_pipeline_halt
            .store(true, std::sync::atomic::Ordering::SeqCst);
        publish_runtime_fault_state(&self.state);
        tracing::error!(
            %reason,
            "pipeline startup hit a permanent error; automatic restart is disabled"
        );
    }

    pub(super) async fn run_claimed_start(&self, starting_from_fault: bool) -> Result<(), DbError> {
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
            .quiesce_connector_generation_until(generation_quiesce_deadline)
            .await
        {
            if starting_from_fault || error.requires_pipeline_halt() {
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
                if error.requires_pipeline_halt() {
                    tracing::error!(
                        %cleanup_error,
                        "catalog rollback also failed after a permanent startup error"
                    );
                    return Err(error);
                }
                if cleanup_error.requires_pipeline_halt() {
                    return Err(DbError::PipelineTerminal(format!(
                        "{error}; catalog bootstrap rollback remains terminally fenced: {cleanup_error}"
                    )));
                }
                return Err(DbError::Pipeline(format!(
                    "{error}; catalog bootstrap rollback remains terminally fenced: {cleanup_error}"
                )));
            }

            // No runtime resources have been constructed and catalog rollback completed. Publish
            // a retryable state only if no concurrent fault superseded this startup generation.
            if error.requires_pipeline_halt() {
                DbState::Faulted.store(&self.state);
                return Err(error);
            }
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
                        Ok(()) => {
                            if error.requires_pipeline_halt() {
                                DbState::Faulted.store(&self.state);
                            }
                            Err(error)
                        }
                        Err(cleanup_error) => {
                            DbState::Faulted.store(&self.state);
                            if error.requires_pipeline_halt() {
                                tracing::error!(
                                    %cleanup_error,
                                    "failed-start cleanup also failed after a permanent startup error"
                                );
                                Err(error)
                            } else if cleanup_error.requires_pipeline_halt() {
                                tracing::error!(
                                    startup_error = %error,
                                    %cleanup_error,
                                    "terminal cleanup error superseded a generic startup failure"
                                );
                                Err(cleanup_error)
                            } else {
                                Err(DbError::Pipeline(format!(
                                    "{error}; failed-start cleanup remains fenced: {cleanup_error}"
                                )))
                            }
                        }
                    },
                }
            }
            Err(e) => {
                match self.cleanup_failed_start().await {
                    Ok(()) => {
                        if e.requires_pipeline_halt() {
                            DbState::Faulted.store(&self.state);
                        } else {
                            // Reset so a retry re-runs startup rather than silently returning Ok.
                            DbState::Created.store(&self.state);
                        }
                        Err(e)
                    }
                    Err(cleanup_error) => {
                        DbState::Faulted.store(&self.state);
                        if e.requires_pipeline_halt() {
                            tracing::error!(
                                %cleanup_error,
                                "failed-start cleanup also failed after a permanent startup error"
                            );
                            Err(e)
                        } else if cleanup_error.requires_pipeline_halt() {
                            tracing::error!(
                                startup_error = %e,
                                %cleanup_error,
                                "terminal cleanup error superseded a generic startup failure"
                            );
                            Err(cleanup_error)
                        } else {
                            Err(DbError::Pipeline(format!(
                                "{e}; failed-start cleanup remains fenced: {cleanup_error}"
                            )))
                        }
                    }
                }
            }
        }
    }

    pub(super) fn validate_startup_durability(
        &self,
        startup_runtime: RuntimeMode,
    ) -> Result<Option<Arc<dyn object_store::ObjectStore>>, DbError> {
        #[cfg(feature = "cluster")]
        if startup_runtime == RuntimeMode::Cluster
            && (!self.mv_registry.lock().is_empty() || !self.mv_store.read().is_empty())
        {
            return Err(DbError::InvalidOperation(format!(
                "[{}] cluster startup found materialized state without a planner-certified distribution and assignment-fenced checkpoint/read lifecycle",
                laminar_core::error_codes::CLUSTER_STATE_LIFECYCLE_UNSUPPORTED,
            )));
        }

        #[cfg(feature = "cluster")]
        let has_injected_decision_store = self.decision_store.lock().is_some();
        #[cfg(not(feature = "cluster"))]
        let has_injected_decision_store = false;
        if startup_runtime == RuntimeMode::Local
            && self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
            && (self.config.object_store_url.as_deref().is_some_and(|url| {
                StorageProvider::detect_uri(url) != Some(StorageProvider::Local)
            }) || has_injected_decision_store)
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
                CheckpointStorageScope::ClusterShared
            } else {
                match self.config.object_store_url.as_deref() {
                    Some(url) => CheckpointStorageScope::for_url(url),
                    None => CheckpointStorageScope::NodeDurable,
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

        Ok(injected_cluster_checkpoint_store)
    }

    pub(super) async fn initialize_checkpointing(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        table_regs: &HashMap<String, crate::connector_manager::TableRegistration>,
        startup_runtime: RuntimeMode,
        injected_cluster_checkpoint_store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Result<Option<laminar_core::checkpoint::PipelineIdentity>, DbError> {
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
                );
                Some(crate::pipeline_identity::compute(&identity_context)?)
            } else {
                None
            };
        if let Some(ref cp_config) = self.config.checkpoint {
            use crate::checkpoint_coordinator::{
                CheckpointConfig as CkpConfig, CheckpointCoordinator,
            };

            let max_node_data_bytes = cp_config.max_node_data_bytes.ok_or_else(|| {
                DbError::Config(
                    "checkpoint.max_node_data_bytes was not resolved at construction".into(),
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
                .filter(|url| StorageProvider::detect_uri(url) == Some(StorageProvider::Local))
                .map(|url| {
                    laminar_core::checkpoint::object_store_builder::file_url_path(url)
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
            let participant_id = participant.unwrap_or(laminar_core::state::LOCAL_NODE_ID.0);
            let pipeline_identity = bound_pipeline_identity.clone().ok_or_else(|| {
                DbError::Checkpoint(
                    "checkpoint startup did not derive the pipeline identity".into(),
                )
            })?;

            let checkpoint_backing = self
                .checkpoint_object_store()?
                .ok_or_else(|| DbError::Checkpoint("checkpoint object store is disabled".into()))?;
            let probe_timeout = std::time::Duration::from_secs(10);
            let probe = if uses_local_checkpoint_store {
                laminar_core::checkpoint::probe_object_store_conditional_create(
                    checkpoint_backing.as_ref(),
                    "",
                    probe_timeout,
                )
                .await
            } else {
                laminar_core::checkpoint::probe_object_store_conditional_update(
                    checkpoint_backing.as_ref(),
                    "",
                    probe_timeout,
                )
                .await
            };
            probe.map_err(|error| {
                DbError::Config(format!(
                    "checkpoint object store does not provide required conditional writes: {error}"
                ))
            })?;
            let store = checkpoint_store(
                Arc::clone(&checkpoint_backing),
                max_node_data_bytes,
                key_group_count,
                participant_id,
                uses_local_checkpoint_store,
            )?;
            let decision_backing = (!uses_local_checkpoint_store).then_some(checkpoint_backing);

            let defaults = CkpConfig::default();
            let config = CkpConfig {
                checkpoint_timeout: cp_config.timeout_ms.map_or(
                    defaults.checkpoint_timeout,
                    std::time::Duration::from_millis,
                ),
                max_node_data_bytes,
                ..defaults
            };
            let mut coord = CheckpointCoordinator::new(config, store)?;
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

            let vnode_registry = self.vnode_registry.lock().clone();
            if let Some(registry) = vnode_registry {
                let owner = {
                    #[cfg(feature = "cluster")]
                    {
                        self.cluster_controller
                            .lock()
                            .as_ref()
                            .map_or(laminar_core::state::LOCAL_NODE_ID, |c| {
                                laminar_core::state::NodeId(c.instance_id().0)
                            })
                    }
                    #[cfg(not(feature = "cluster"))]
                    {
                        laminar_core::state::LOCAL_NODE_ID
                    }
                };
                let version = registry.assignment_version();
                coord.set_assignment_version(version);
                if startup_runtime == RuntimeMode::Cluster {
                    coord.set_vnode_set(laminar_core::state::owned_vnodes(&registry, owner));
                }
            }

            *self.coordinator.lock().await = Some(coord);
        }
        Ok(bound_pipeline_identity)
    }
}
