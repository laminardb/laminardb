use super::{checked_pipeline_deadline, Arc, DbError, LaminarDB, RuntimeMode, StorageProvider};

pub(super) fn checkpoint_store(
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

pub(super) fn validate_checkpoint_timing(
    config: &laminar_core::streaming::StreamCheckpointConfig,
) -> Result<(), DbError> {
    if config.interval_ms == Some(0) {
        return Err(DbError::Config(
            "checkpoint.interval_ms must be greater than zero; use None for manual-only".into(),
        ));
    }
    let Some(timeout_ms) = config.timeout_ms else {
        return Ok(());
    };
    if timeout_ms == 0 {
        return Err(DbError::Config(
            "checkpoint.timeout_ms must be greater than zero".into(),
        ));
    }
    checked_pipeline_deadline(std::time::Duration::from_millis(timeout_ms), "checkpoint").map_err(
        |_| DbError::Config("checkpoint.timeout_ms exceeds the platform clock range".into()),
    )?;
    Ok(())
}

impl LaminarDB {
    pub(in crate::pipeline_lifecycle) async fn initialize_checkpointing(
        &self,
        identity_registrations: crate::pipeline_identity::PipelineRegistrations<'_>,
        startup_runtime: RuntimeMode,
        injected_cluster_checkpoint_store: Option<Arc<dyn object_store::ObjectStore>>,
        expected_deployment: Option<&str>,
    ) -> Result<Option<laminar_core::checkpoint::PipelineIdentity>, DbError> {
        let participant = self.checkpoint_participant();
        let bound_pipeline_identity =
            if self.config.checkpoint.is_some() || startup_runtime == RuntimeMode::Cluster {
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
            validate_checkpoint_timing(cp_config)?;
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
            let deployment_id = if let Some(expected) = expected_deployment {
                ds.load_deployment_id()
                    .await
                    .map_err(|error| {
                        DbError::Checkpoint(format!("load committed deployment identity: {error}"))
                    })?
                    .filter(|actual| actual == expected)
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "committed topology requires its existing exact deployment identity"
                                .into(),
                        )
                    })?
            } else {
                ds.load_or_create_deployment_id().await.map_err(|error| {
                    DbError::Checkpoint(format!(
                    "load/create durable deployment identity before checkpoint startup: {error}"
                ))
                })?
            };
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
