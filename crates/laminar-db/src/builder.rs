//! Fluent builder for `LaminarDB` construction.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use datafusion_expr::{AggregateUDF, ScalarUDF};
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

use crate::config::LaminarConfig;
use crate::db::{LaminarDB, RuntimeMode};
use crate::error::DbError;
use crate::profile::Profile;

/// Callback for registering custom connectors.
type ConnectorCallback = Box<
    dyn FnOnce(
            &laminar_connectors::registry::ConnectorRegistry,
        ) -> Result<(), laminar_connectors::error::ConnectorError>
        + Send,
>;

/// Fluent builder for constructing a [`LaminarDB`] instance.
///
/// # Example
///
/// ```rust,ignore
/// let db = LaminarDB::builder()
///     .config_var("KAFKA_BROKERS", "localhost:9092")
///     .buffer_size(131072)
///     .build()
///     .await?;
/// ```
pub struct LaminarDbBuilder {
    config: LaminarConfig,
    config_vars: HashMap<String, String>,
    connector_callbacks: Vec<ConnectorCallback>,
    profile: Profile,
    profile_explicit: bool,
    delivery_explicit: bool,
    object_store_url: Option<String>,
    object_store_options: HashMap<String, String>,
    custom_udfs: Vec<ScalarUDF>,
    custom_udafs: Vec<AggregateUDF>,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<std::sync::Arc<laminar_core::cluster::control::ClusterController>>,
    #[cfg(feature = "cluster")]
    shuffle_sender: Option<std::sync::Arc<laminar_core::shuffle::ShuffleSender>>,
    #[cfg(feature = "cluster")]
    shuffle_receiver: Option<std::sync::Arc<laminar_core::shuffle::ShuffleReceiver>>,
    #[cfg(feature = "cluster")]
    decision_store: Option<std::sync::Arc<laminar_core::cluster::control::CheckpointDecisionStore>>,
    #[cfg(feature = "cluster")]
    assignment_snapshot_store:
        Option<std::sync::Arc<laminar_core::cluster::control::AssignmentSnapshotStore>>,
    #[cfg(feature = "cluster")]
    catalog_manifest_store:
        Option<std::sync::Arc<laminar_core::cluster::control::CatalogManifestStore>>,
    #[cfg(feature = "cluster")]
    cluster_checkpoint_object_store: Option<Arc<dyn object_store::ObjectStore>>,
    state_backend: Option<std::sync::Arc<dyn laminar_core::state::StateBackend>>,
    vnode_registry: Option<std::sync::Arc<laminar_core::state::VnodeRegistry>>,
    physical_optimizer_rules: Vec<
        std::sync::Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
    >,
    target_partitions: Option<usize>,
    ai_runtime: Option<std::sync::Arc<crate::ai::AiRuntime>>,
}

impl LaminarDbBuilder {
    /// Create a new builder with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: LaminarConfig::default(),
            config_vars: HashMap::new(),
            connector_callbacks: Vec::new(),
            profile: Profile::default(),
            profile_explicit: false,
            delivery_explicit: false,
            object_store_url: None,
            object_store_options: HashMap::new(),
            custom_udfs: Vec::new(),
            custom_udafs: Vec::new(),
            #[cfg(feature = "cluster")]
            cluster_controller: None,
            #[cfg(feature = "cluster")]
            shuffle_sender: None,
            #[cfg(feature = "cluster")]
            shuffle_receiver: None,
            #[cfg(feature = "cluster")]
            decision_store: None,
            #[cfg(feature = "cluster")]
            assignment_snapshot_store: None,
            #[cfg(feature = "cluster")]
            catalog_manifest_store: None,
            #[cfg(feature = "cluster")]
            cluster_checkpoint_object_store: None,
            state_backend: None,
            vnode_registry: None,
            physical_optimizer_rules: Vec::new(),
            target_partitions: None,
            ai_runtime: None,
        }
    }

    /// Install the AI subsystem; required for `ai_*` SQL functions.
    #[must_use]
    pub fn ai(mut self, runtime: std::sync::Arc<crate::ai::AiRuntime>) -> Self {
        self.ai_runtime = Some(runtime);
        self
    }

    /// Override `target_partitions`.
    #[must_use]
    pub fn target_partitions(mut self, n: usize) -> Self {
        self.target_partitions = Some(n);
        self
    }

    /// Register an additional `PhysicalOptimizerRule` on the session state.
    #[must_use]
    pub fn physical_optimizer_rule(
        mut self,
        rule: std::sync::Arc<
            dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync,
        >,
    ) -> Self {
        self.physical_optimizer_rules.push(rule);
        self
    }

    /// Install a state backend; must be paired with [`Self::vnode_registry`].
    #[must_use]
    pub fn state_backend(
        mut self,
        backend: std::sync::Arc<dyn laminar_core::state::StateBackend>,
    ) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// Install a vnode registry; must be paired with [`Self::state_backend`].
    #[must_use]
    pub fn vnode_registry(
        mut self,
        registry: std::sync::Arc<laminar_core::state::VnodeRegistry>,
    ) -> Self {
        self.vnode_registry = Some(registry);
        self
    }

    /// Install the cluster control facade; selects cluster runtime semantics when the profile is
    /// inferred. An explicitly selected profile must be [`Profile::Cluster`].
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn cluster_controller(
        mut self,
        controller: std::sync::Arc<laminar_core::cluster::control::ClusterController>,
    ) -> Self {
        self.cluster_controller = Some(controller);
        self
    }

    /// Install the outbound shuffle handle; pair with [`Self::shuffle_receiver`].
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn shuffle_sender(
        mut self,
        sender: std::sync::Arc<laminar_core::shuffle::ShuffleSender>,
    ) -> Self {
        self.shuffle_sender = Some(sender);
        self
    }

    /// Install the inbound shuffle handle; pair with [`Self::shuffle_sender`].
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn shuffle_receiver(
        mut self,
        receiver: std::sync::Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) -> Self {
        self.shuffle_receiver = Some(receiver);
        self
    }

    /// Install the commit-marker store for cross-instance 2PC.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn decision_store(
        mut self,
        store: std::sync::Arc<laminar_core::cluster::control::CheckpointDecisionStore>,
    ) -> Self {
        self.decision_store = Some(store);
        self
    }

    /// Install the assignment-snapshot store for dynamic rebalance.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn assignment_snapshot_store(
        mut self,
        store: std::sync::Arc<laminar_core::cluster::control::AssignmentSnapshotStore>,
    ) -> Self {
        self.assignment_snapshot_store = Some(store);
        self
    }

    /// Install the catalog-manifest store for cluster-wide DDL replay.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn catalog_manifest_store(
        mut self,
        store: std::sync::Arc<laminar_core::cluster::control::CatalogManifestStore>,
    ) -> Self {
        self.catalog_manifest_store = Some(store);
        self
    }

    /// Install a pre-built shared checkpoint object store for low-level cluster wiring.
    ///
    /// This is an alternative to [`Self::object_store_url`], not an override. A cluster
    /// controller is required so the store can only be used with cluster runtime semantics.
    #[cfg(feature = "cluster")]
    #[doc(hidden)]
    #[must_use]
    pub fn cluster_checkpoint_object_store(
        mut self,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        self.cluster_checkpoint_object_store = Some(store);
        self
    }

    /// Set a config variable for `${VAR}` substitution in SQL.
    #[must_use]
    pub fn config_var(mut self, key: &str, value: &str) -> Self {
        self.config_vars.insert(key.to_string(), value.to_string());
        self
    }

    /// Set the bearer token presented when forwarding requests to the cluster
    /// leader's HTTP API.
    #[must_use]
    pub fn http_auth_token(mut self, token: impl Into<String>) -> Self {
        self.config.http_auth_token = Some(crate::config::SecretString::new(token));
        self
    }

    /// Set the default buffer size for streaming channels.
    #[must_use]
    pub fn buffer_size(mut self, size: usize) -> Self {
        self.config.default_buffer_size = size;
        self
    }

    /// Set the default backpressure strategy.
    #[must_use]
    pub fn backpressure(mut self, strategy: BackpressureStrategy) -> Self {
        self.config.default_backpressure = strategy;
        self
    }

    /// Set the storage directory for WAL and checkpoints.
    #[must_use]
    pub fn storage_dir(mut self, path: impl Into<PathBuf>) -> Self {
        self.config.storage_dir = Some(path.into());
        self
    }

    /// Set checkpoint configuration.
    #[must_use]
    pub fn checkpoint(mut self, config: StreamCheckpointConfig) -> Self {
        self.config.checkpoint = Some(config);
        self
    }

    /// Select dirty-only changelog emission for keyed running aggregates.
    #[must_use]
    pub fn incremental_emit(mut self, enabled: bool) -> Self {
        self.config.incremental_emit = enabled;
        self
    }

    /// Set the deployment profile.
    ///
    /// See [`Profile`] for the available tiers. [`Profile::Cluster`] also requires a cluster
    /// controller at build time.
    #[must_use]
    pub fn profile(mut self, profile: Profile) -> Self {
        self.profile = profile;
        self.profile_explicit = true;
        self
    }

    /// Set the object-store URL for durable checkpoints.
    ///
    /// Required when using [`Profile::Durable`] or
    /// [`Profile::Cluster`].
    #[must_use]
    pub fn object_store_url(mut self, url: impl Into<String>) -> Self {
        self.object_store_url = Some(url.into());
        self
    }

    /// Set explicit credential/config overrides for the object store.
    ///
    /// Keys are backend-specific (e.g., `aws_access_key_id`, `aws_region`).
    /// These supplement environment-variable-based credential resolution.
    #[must_use]
    pub fn object_store_options(mut self, opts: HashMap<String, String>) -> Self {
        self.object_store_options = opts;
        self
    }

    /// Set the end-to-end delivery guarantee for the pipeline.
    #[must_use]
    pub fn delivery_guarantee(
        mut self,
        guarantee: laminar_connectors::connector::DeliveryGuarantee,
    ) -> Self {
        self.config.delivery_guarantee = guarantee;
        self.delivery_explicit = true;
        self
    }

    /// Register a custom scalar UDF; available in SQL after `build()`.
    #[must_use]
    pub fn register_udf(mut self, udf: ScalarUDF) -> Self {
        self.custom_udfs.push(udf);
        self
    }

    /// Register a custom aggregate UDF; available in SQL after `build()`.
    #[must_use]
    pub fn register_udaf(mut self, udaf: AggregateUDF) -> Self {
        self.custom_udafs.push(udaf);
        self
    }

    /// Source → coordinator channel capacity (default 64).
    #[must_use]
    pub fn pipeline_channel_capacity(mut self, capacity: usize) -> Self {
        self.config.pipeline_channel_capacity = Some(capacity);
        self
    }

    /// Micro-batch coalescing window (default 5ms for connectors, 0 for embedded).
    #[must_use]
    pub fn pipeline_batch_window(mut self, window: std::time::Duration) -> Self {
        self.config.pipeline_batch_window = Some(window);
        self
    }

    /// Max drain time per cycle in nanoseconds (default 1ms).
    #[must_use]
    pub fn pipeline_drain_budget_ns(mut self, ns: u64) -> Self {
        self.config.pipeline_drain_budget_ns = Some(ns);
        self
    }

    /// Per-query execution budget in nanoseconds (default 8ms).
    #[must_use]
    pub fn pipeline_query_budget_ns(mut self, ns: u64) -> Self {
        self.config.pipeline_query_budget_ns = Some(ns);
        self
    }

    /// Per-port operator input-buffer cap in batches (default 256).
    #[must_use]
    pub fn pipeline_max_input_buf_batches(mut self, batches: usize) -> Self {
        self.config.pipeline_max_input_buf_batches = Some(batches);
        self
    }

    /// Per-port operator input-buffer cap in bytes.
    #[must_use]
    pub fn pipeline_max_input_buf_bytes(mut self, bytes: usize) -> Self {
        self.config.pipeline_max_input_buf_bytes = Some(bytes);
        self
    }

    /// Backpressure policy (default `Backpressure`).
    #[must_use]
    pub fn pipeline_backpressure_policy(
        mut self,
        policy: crate::config::BackpressurePolicy,
    ) -> Self {
        self.config.pipeline_backpressure_policy = policy;
        self
    }

    /// Auto-restart policy used when supervision is enabled.
    #[must_use]
    pub fn restart_policy(mut self, policy: crate::config::RestartPolicy) -> Self {
        self.config.restart_policy = policy;
        self
    }

    /// Register custom connectors; the callback runs after built-ins are wired and must propagate
    /// registration errors. The registry is frozen before [`Self::build`] returns.
    #[must_use]
    pub fn register_connector(
        mut self,
        f: impl FnOnce(
                &laminar_connectors::registry::ConnectorRegistry,
            ) -> Result<(), laminar_connectors::error::ConnectorError>
            + Send
            + 'static,
    ) -> Self {
        self.connector_callbacks.push(Box::new(f));
        self
    }

    /// Build the `LaminarDB` instance.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if database creation fails.
    #[allow(clippy::unused_async)]
    pub async fn build(mut self) -> Result<Arc<LaminarDB>, DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_checkpoint_object_store.is_some() && self.object_store_url.is_some() {
            return Err(DbError::Config(
                "cluster_checkpoint_object_store conflicts with object_store_url; configure exactly one checkpoint object store"
                    .into(),
            ));
        }

        self.config.object_store_url = self.object_store_url.take();
        self.config.object_store_options = std::mem::take(&mut self.object_store_options);

        if !self.profile_explicit {
            self.profile = Profile::from_config(&self.config, false);
        }

        let runtime_mode = self.resolve_runtime_mode()?;
        if runtime_mode.is_cluster() && !self.profile_explicit {
            self.profile = Profile::Cluster;
        }
        if runtime_mode.is_cluster() && !self.delivery_explicit {
            self.config.delivery_guarantee =
                laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce;
        }

        self.profile
            .validate_features()
            .map_err(|e| DbError::Config(e.to_string()))?;
        #[cfg(feature = "cluster")]
        let profile_object_store = self.config.object_store_url.as_deref().or_else(|| {
            self.cluster_checkpoint_object_store
                .as_ref()
                .map(|_| "injected-cluster-checkpoint-store")
        });
        #[cfg(not(feature = "cluster"))]
        let profile_object_store = self.config.object_store_url.as_deref();
        self.profile
            .validate_config(&self.config, profile_object_store)
            .map_err(|e| DbError::Config(e.to_string()))?;

        Self::validate_cluster_delivery(runtime_mode, self.config.delivery_guarantee)?;

        Self::validate_backpressure(&self.config)?;
        self.validate_state_topology(runtime_mode)?;
        #[cfg(feature = "cluster")]
        self.bind_cluster_process_lease(runtime_mode)?;

        self.profile.apply_defaults(&mut self.config);

        let mut db = LaminarDB::open_with_config_and_vars_and_rules(
            self.config,
            self.config_vars,
            &self.physical_optimizer_rules,
            self.target_partitions,
            runtime_mode,
        )?;
        if let Some(runtime) = self.ai_runtime {
            let handle = tokio::runtime::Handle::try_current().map_err(|_| {
                DbError::InvalidOperation(
                    "LaminarDB::build() with an AI runtime must run inside a Tokio runtime"
                        .to_string(),
                )
            })?;
            db.set_ai_runtime(runtime, handle);
        }
        for callback in self.connector_callbacks {
            callback(db.connector_registry())?;
        }
        db.connector_registry().freeze();
        for udf in self.custom_udfs {
            db.register_custom_udf(udf);
        }
        for udaf in self.custom_udafs {
            db.register_custom_udaf(udaf);
        }
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller {
            db.set_cluster_controller(controller)?;
        }
        #[cfg(feature = "cluster")]
        if let Some(store) = self.cluster_checkpoint_object_store {
            db.set_cluster_checkpoint_object_store(store)?;
        }
        #[cfg(feature = "cluster")]
        if let Some(sender) = self.shuffle_sender {
            db.set_shuffle_sender(sender);
        }
        #[cfg(feature = "cluster")]
        if let Some(receiver) = self.shuffle_receiver {
            db.set_shuffle_receiver(receiver);
        }
        #[cfg(feature = "cluster")]
        if let Some(store) = self.decision_store {
            db.set_decision_store(store);
        }
        #[cfg(feature = "cluster")]
        if let Some(store) = self.assignment_snapshot_store {
            db.set_assignment_snapshot_store(store);
        }
        #[cfg(feature = "cluster")]
        if let Some(store) = self.catalog_manifest_store {
            db.set_catalog_manifest_store(store);
        }
        if let Some(backend) = self.state_backend {
            db.set_state_backend(backend);
        }
        if let Some(registry) = self.vnode_registry {
            db.set_vnode_registry(registry);
        }
        Ok(Arc::new(db))
    }

    fn validate_state_topology(&self, runtime_mode: RuntimeMode) -> Result<(), DbError> {
        match (&self.state_backend, &self.vnode_registry) {
            (Some(_), None) => {
                return Err(DbError::Config(
                    "state_backend is set but vnode_registry is missing".into(),
                ));
            }
            (None, Some(_)) => {
                return Err(DbError::Config(
                    "vnode_registry is set but state_backend is missing".into(),
                ));
            }
            _ => {}
        }
        if let (Some(backend), Some(registry)) =
            (self.state_backend.as_ref(), self.vnode_registry.as_ref())
        {
            let backend_capacity = backend.key_group_capacity();
            let backend_key_groups =
                laminar_core::state::KeyGroupCount::try_from(backend_capacity).map_err(|_| {
                    DbError::Config(format!(
                        "state_backend key-group capacity must be between 1 and {}, got {backend_capacity}",
                        laminar_core::state::MAX_KEY_GROUP_COUNT
                    ))
                })?;
            let registry_count = registry.vnode_count();
            let registry_key_groups = laminar_core::state::KeyGroupCount::try_from(registry_count)
                .map_err(|_| {
                    DbError::Config(format!(
                        "vnode_registry count must be between 1 and {}, got {registry_count}",
                        laminar_core::state::MAX_KEY_GROUP_COUNT
                    ))
                })?;
            if backend_key_groups != registry_key_groups {
                return Err(DbError::Config(format!(
                    "state_backend key-group capacity {backend_key_groups} does not match vnode_registry count {registry_key_groups}"
                )));
            }
            if !runtime_mode.is_cluster()
                && registry_key_groups != laminar_core::state::LOCAL_KEY_GROUP_COUNT
            {
                return Err(DbError::Config(format!(
                    "local runtime requires exactly {} key group; got {registry_key_groups}. Multi-key-group ownership requires cluster mode",
                    laminar_core::state::LOCAL_KEY_GROUP_COUNT
                )));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn bind_cluster_process_lease(&self, runtime_mode: RuntimeMode) -> Result<(), DbError> {
        if !runtime_mode.is_cluster() {
            return Ok(());
        }
        let controller = self.cluster_controller.as_ref().ok_or_else(|| {
            DbError::Config("cluster runtime requires a cluster controller".into())
        })?;
        let deadline = controller.process_lease_deadline().ok_or_else(|| {
            DbError::Config(
                "cluster runtime requires one shared process lease deadline before construction"
                    .into(),
            )
        })?;
        if !deadline.is_live() {
            return Err(DbError::Config(
                "cluster runtime process lease deadline is already expired".into(),
            ));
        }
        match (&self.shuffle_sender, &self.shuffle_receiver) {
            (Some(sender), Some(receiver)) => {
                sender
                    .bind_process_lease_deadline_pair(receiver, deadline)
                    .map_err(|error| {
                        DbError::Config(format!(
                            "shuffle process lease does not match the controller: {error}"
                        ))
                    })?;
            }
            (None, None) => {}
            _ => {
                return Err(DbError::Config(
                    "cluster shuffle sender and receiver must be installed together".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_backpressure(config: &LaminarConfig) -> Result<(), DbError> {
        use crate::config::BackpressurePolicy;
        use laminar_connectors::connector::DeliveryGuarantee;

        let policy = config.pipeline_backpressure_policy;
        if policy == BackpressurePolicy::Backpressure {
            return Ok(());
        }

        let has_count_cap = config.pipeline_max_input_buf_batches.is_none_or(|c| c > 0);
        let has_byte_cap = config.pipeline_max_input_buf_bytes.is_some_and(|b| b > 0);
        if !has_count_cap && !has_byte_cap {
            return Err(DbError::Config(format!(
                "backpressure_policy={policy:?} requires at least one of \
                 pipeline_max_input_buf_batches (>0) or pipeline_max_input_buf_bytes"
            )));
        }

        if policy == BackpressurePolicy::ShedOldest
            && config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
        {
            return Err(DbError::Config(
                "ShedOldest drops data; it is incompatible with exactly-once \
                 delivery. Use Backpressure or Fail, or downgrade the guarantee."
                    .into(),
            ));
        }
        Ok(())
    }

    /// Resolve the distributed execution scope exactly once and reject partial or contradictory
    /// cluster wiring before constructing the database.
    fn resolve_runtime_mode(&self) -> Result<RuntimeMode, DbError> {
        #[cfg(not(feature = "cluster"))]
        {
            if self.profile == Profile::Cluster {
                return Err(DbError::Config(
                    "Profile::Cluster requires a cluster controller and a cluster-enabled build"
                        .into(),
                ));
            }
            return Ok(RuntimeMode::Local);
        }

        #[cfg(feature = "cluster")]
        {
            let has_controller = self.cluster_controller.is_some();
            let has_cluster_only_handle = self.shuffle_sender.is_some()
                || self.shuffle_receiver.is_some()
                || self.decision_store.is_some()
                || self.assignment_snapshot_store.is_some()
                || self.catalog_manifest_store.is_some()
                || self.cluster_checkpoint_object_store.is_some();

            if has_cluster_only_handle && !has_controller {
                return Err(DbError::Config(
                    "cluster-only stores and shuffle handles require a cluster controller".into(),
                ));
            }
            if self.profile == Profile::Cluster && !has_controller {
                return Err(DbError::Config(
                    "Profile::Cluster requires a cluster controller".into(),
                ));
            }
            if self.profile_explicit && self.profile != Profile::Cluster && has_controller {
                return Err(DbError::Config(format!(
                    "profile {} cannot be combined with a cluster controller; select Profile::Cluster or omit the explicit profile",
                    self.profile
                )));
            }

            Ok(if has_controller {
                RuntimeMode::Cluster
            } else {
                RuntimeMode::Local
            })
        }
    }

    /// Validate delivery semantics whose correctness depends on the runtime mode.
    ///
    /// Checkpoint decisions are term-fenced. Cluster exactly-once remains closed until supported
    /// connectors also provide certified term-fenced source handoff and external sink cursor
    /// commit across reassignment.
    fn validate_cluster_delivery(
        runtime_mode: RuntimeMode,
        guarantee: laminar_connectors::connector::DeliveryGuarantee,
    ) -> Result<(), DbError> {
        use laminar_connectors::connector::DeliveryGuarantee;

        if !runtime_mode.is_cluster() {
            return Ok(());
        }
        match guarantee {
            DeliveryGuarantee::BestEffort => Err(DbError::Config(
                "cluster mode requires at_least_once delivery; best_effort has no defined \
                 rebalance/state-loss contract"
                    .into(),
            )),
            DeliveryGuarantee::AtLeastOnce => Ok(()),
            DeliveryGuarantee::ExactlyOnce => Err(DbError::Config(
                "[LDB-0013] cluster exactly-once is not admitted: checkpoint decisions are \
                 term-fenced, but supported connectors do not yet provide a certified \
                 term-fenced source handoff and external sink cursor commit. Use cluster \
                 at_least_once, or exactly_once in embedded/single-node mode"
                    .into(),
            )),
        }
    }
}

impl Default for LaminarDbBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LaminarDbBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LaminarDbBuilder")
            .field("config", &self.config)
            .field("profile", &self.profile)
            .field("profile_explicit", &self.profile_explicit)
            .field("delivery_explicit", &self.delivery_explicit)
            .field("object_store_url", &self.object_store_url)
            .field(
                "object_store_options_count",
                &self.object_store_options.len(),
            )
            .field("config_vars_count", &self.config_vars.len())
            .field("connector_callbacks", &self.connector_callbacks.len())
            .field("custom_udfs", &self.custom_udfs.len())
            .field("custom_udafs", &self.custom_udafs.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "cluster")]
    fn test_cluster_controller_without_deadline(
    ) -> Arc<laminar_core::cluster::control::ClusterController> {
        use laminar_core::cluster::control::{ClusterController, ClusterKv, InMemoryKv};
        use laminar_core::cluster::discovery::{NodeId, NodeInfo};

        let node = NodeId(1);
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::<NodeInfo>::new());
        Arc::new(ClusterController::new(node, kv, None, members_rx))
    }

    #[cfg(feature = "cluster")]
    fn test_cluster_controller() -> Arc<laminar_core::cluster::control::ClusterController> {
        use laminar_core::cluster::control::LeaseDeadline;

        let controller = test_cluster_controller_without_deadline();
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();
        controller
    }

    #[cfg(feature = "cluster")]
    fn test_cluster_checkpoint_store() -> Arc<dyn object_store::ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    #[tokio::test]
    async fn test_default_builder() {
        let db = LaminarDbBuilder::new().build().await.unwrap();
        assert!(!db.is_closed());
        assert!(!db.is_cluster_runtime());
        #[cfg(feature = "cluster")]
        assert!(!db.cluster_intake_fenced());
    }

    #[tokio::test]
    async fn test_shed_oldest_requires_cap() {
        use crate::config::BackpressurePolicy;
        let err = LaminarDbBuilder::new()
            .pipeline_backpressure_policy(BackpressurePolicy::ShedOldest)
            .pipeline_max_input_buf_batches(0)
            .build()
            .await
            .expect_err("ShedOldest with no caps must be rejected");
        assert!(err.to_string().contains("requires at least one"), "{err}");
    }

    #[tokio::test]
    async fn test_shed_oldest_rejects_exactly_once() {
        use crate::config::BackpressurePolicy;
        use laminar_connectors::connector::DeliveryGuarantee;
        let err = LaminarDbBuilder::new()
            .pipeline_backpressure_policy(BackpressurePolicy::ShedOldest)
            .pipeline_max_input_buf_batches(64)
            .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
            .build()
            .await
            .expect_err("ShedOldest + ExactlyOnce must be rejected");
        assert!(err.to_string().contains("exactly-once"), "{err}");
    }

    #[test]
    fn cluster_delivery_admission_fails_closed_without_durable_term_binding() {
        use laminar_connectors::connector::DeliveryGuarantee;

        assert!(LaminarDbBuilder::validate_cluster_delivery(
            RuntimeMode::Cluster,
            DeliveryGuarantee::AtLeastOnce,
        )
        .is_ok());

        let error = LaminarDbBuilder::validate_cluster_delivery(
            RuntimeMode::Cluster,
            DeliveryGuarantee::ExactlyOnce,
        )
        .expect_err("cluster EO must remain closed until connectors consume the fence end to end");
        assert!(error.to_string().contains("[LDB-0013]"), "{error}");
        assert!(
            error.to_string().contains("external sink cursor commit"),
            "{error}"
        );

        assert!(LaminarDbBuilder::validate_cluster_delivery(
            RuntimeMode::Local,
            DeliveryGuarantee::ExactlyOnce,
        )
        .is_ok());
    }

    #[tokio::test]
    async fn cluster_profile_without_controller_is_rejected() {
        let error = LaminarDbBuilder::new()
            .profile(Profile::Cluster)
            .object_store_url("memory://checkpoint-test")
            .build()
            .await
            .expect_err("cluster profile must not degrade into a local runtime");
        assert!(error.to_string().contains("cluster controller"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn controller_selects_cluster_runtime_when_profile_is_inferred() {
        let checkpoint_store = test_cluster_checkpoint_store();
        let db = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .cluster_checkpoint_object_store(Arc::clone(&checkpoint_store))
            .build()
            .await
            .unwrap();
        assert!(db.is_cluster_runtime());
        assert_eq!(db.config.default_buffer_size, 262_144);
        assert!(Arc::ptr_eq(
            &db.cluster_checkpoint_object_store().unwrap(),
            &checkpoint_store
        ));
        assert_eq!(
            db.config.delivery_guarantee,
            laminar_connectors::connector::DeliveryGuarantee::AtLeastOnce
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_runtime_rejects_a_controller_without_process_lease_authority() {
        let error = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller_without_deadline())
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .build()
            .await
            .expect_err("cluster construction must not fail open without a process lease clock");

        assert!(
            error.to_string().contains("shared process lease deadline"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_builder_binds_shuffle_to_the_controller_deadline() {
        use laminar_core::cluster::control::LeaseDeadline;

        let controller = test_cluster_controller();
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            1,
            uuid::Uuid::from_u128(1),
        ));
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                1,
                "127.0.0.1:0".parse().unwrap(),
                uuid::Uuid::from_u128(1),
            )
            .await
            .unwrap(),
        );
        let _db = LaminarDbBuilder::new()
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .shuffle_sender(Arc::clone(&sender))
            .shuffle_receiver(Arc::clone(&receiver))
            .build()
            .await
            .unwrap();
        let different = Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(60)));

        assert!(sender
            .install_process_lease_deadline(Arc::clone(&different))
            .is_err());
        assert!(receiver.install_process_lease_deadline(different).is_err());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_shuffle_binding_failure_does_not_partially_bind_sender() {
        use laminar_core::cluster::control::LeaseDeadline;

        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            1,
            uuid::Uuid::from_u128(1),
        ));
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                1,
                "127.0.0.1:0".parse().unwrap(),
                uuid::Uuid::from_u128(1),
            )
            .await
            .unwrap(),
        );
        receiver
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .unwrap();

        let error = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .shuffle_sender(Arc::clone(&sender))
            .shuffle_receiver(receiver)
            .build()
            .await
            .expect_err("an incompatible receiver lease must reject the pair");
        assert!(error.to_string().contains("already installed"), "{error}");

        sender
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(60),
            )))
            .expect("failed pair validation must leave the sender unbound");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn stateless_cluster_uses_default_key_group_topology() {
        let db = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .build()
            .await
            .unwrap();

        assert_eq!(
            db.checkpoint_key_groups(),
            laminar_core::state::DEFAULT_CLUSTER_KEY_GROUP_COUNT
        );
        assert!(db.state_backend.lock().is_none());
        assert!(db.vnode_registry.lock().is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn fresh_cluster_database_keeps_intake_fenced() {
        let db = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .build()
            .await
            .unwrap();

        assert!(db.cluster_intake_fenced());
        db.fence_cluster_startup();
        db.fence_cluster_startup();
        assert!(db.cluster_intake_fenced());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn inferred_cluster_requires_shared_checkpoint_storage() {
        let error = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .build()
            .await
            .expect_err("cluster runtime must not retain bare-metal storage admission");
        assert!(error.to_string().contains("object_store_url"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn complete_cluster_profile_selects_cluster_runtime() {
        let db = LaminarDbBuilder::new()
            .profile(Profile::Cluster)
            .object_store_url("memory://checkpoint-test")
            .cluster_controller(test_cluster_controller())
            .build()
            .await
            .unwrap();
        assert!(db.is_cluster_runtime());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn explicit_local_profile_rejects_cluster_controller() {
        let error = LaminarDbBuilder::new()
            .profile(Profile::BareMetal)
            .cluster_controller(test_cluster_controller())
            .build()
            .await
            .expect_err("explicit local profile and cluster wiring must not disagree");
        assert!(error.to_string().contains("cannot be combined"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn explicit_cluster_best_effort_is_rejected() {
        let error = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .delivery_guarantee(laminar_connectors::connector::DeliveryGuarantee::BestEffort)
            .build()
            .await
            .expect_err("explicit cluster best-effort must fail closed");
        assert!(
            error.to_string().contains("requires at_least_once"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_only_handle_without_controller_is_rejected() {
        let error = LaminarDbBuilder::new()
            .shuffle_sender(Arc::new(laminar_core::shuffle::ShuffleSender::new(
                1,
                uuid::Uuid::from_u128(1),
            )))
            .build()
            .await
            .expect_err("partial cluster wiring must not create a local runtime");
        assert!(
            error.to_string().contains("require a cluster controller"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_checkpoint_store_without_controller_is_rejected() {
        let error = LaminarDbBuilder::new()
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .build()
            .await
            .expect_err("shared cluster checkpoint wiring must select a controller authority");
        assert!(
            error.to_string().contains("require a cluster controller"),
            "{error}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_checkpoint_store_conflicts_with_url() {
        let error = LaminarDbBuilder::new()
            .cluster_controller(test_cluster_controller())
            .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
            .object_store_url("memory://checkpoint-test")
            .build()
            .await
            .expect_err("two checkpoint namespace authorities must be rejected");
        assert!(error.to_string().contains("conflicts"), "{error}");
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_exactly_once_build_is_rejected_before_runtime_start() {
        use laminar_connectors::connector::DeliveryGuarantee;

        let error = LaminarDbBuilder::new()
            .profile(Profile::Cluster)
            .object_store_url("memory://checkpoint-test")
            .cluster_controller(test_cluster_controller())
            .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
            .build()
            .await
            .expect_err("cluster EO must fail at database admission");
        assert!(error.to_string().contains("[LDB-0013]"), "{error}");
    }

    #[tokio::test]
    async fn injected_vnode_registry_must_fit_checkpoint_abi() {
        let vnode_count = laminar_core::state::MAX_KEY_GROUP_COUNT + 1;
        let error = LaminarDbBuilder::new()
            .state_backend(Arc::new(laminar_core::state::InProcessBackend::new(1)))
            .vnode_registry(Arc::new(laminar_core::state::VnodeRegistry::new(
                vnode_count,
            )))
            .build()
            .await
            .expect_err("oversized injected vnode registry must be rejected");
        assert!(
            error
                .to_string()
                .contains("vnode_registry count must be between 1"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn injected_state_backend_capacity_must_fit_checkpoint_abi() {
        let error = LaminarDbBuilder::new()
            .state_backend(Arc::new(laminar_core::state::InProcessBackend::new(0)))
            .vnode_registry(Arc::new(laminar_core::state::VnodeRegistry::new(1)))
            .build()
            .await
            .expect_err("invalid backend topology must be rejected at admission");
        assert!(
            error
                .to_string()
                .contains("state_backend key-group capacity must be between 1"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn state_backend_capacity_must_match_registry() {
        let error = LaminarDbBuilder::new()
            .state_backend(Arc::new(laminar_core::state::InProcessBackend::new(2)))
            .vnode_registry(Arc::new(laminar_core::state::VnodeRegistry::new(1)))
            .build()
            .await
            .expect_err("backend and registry must describe one topology");
        assert!(
            error.to_string().contains(
                "state_backend key-group capacity 2 does not match vnode_registry count 1"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn local_runtime_rejects_multi_key_group_registry() {
        let error = LaminarDbBuilder::new()
            .state_backend(Arc::new(laminar_core::state::InProcessBackend::new(2)))
            .vnode_registry(Arc::new(laminar_core::state::VnodeRegistry::new(2)))
            .build()
            .await
            .expect_err("local runtime must not expose cluster rescaling dimensions");
        assert!(
            error.to_string().contains("requires exactly 1 key group"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_valid_shed_oldest_builds() {
        use crate::config::BackpressurePolicy;
        let db = LaminarDbBuilder::new()
            .pipeline_backpressure_policy(BackpressurePolicy::ShedOldest)
            .pipeline_max_input_buf_batches(64)
            .build()
            .await
            .unwrap();
        assert!(!db.is_closed());
    }

    #[tokio::test]
    async fn test_builder_with_config_vars() {
        let db = LaminarDbBuilder::new()
            .config_var("KAFKA_BROKERS", "localhost:9092")
            .config_var("GROUP_ID", "test-group")
            .build()
            .await
            .unwrap();
        assert!(!db.is_closed());
    }

    #[tokio::test]
    async fn test_builder_with_options() {
        let db = LaminarDbBuilder::new()
            .buffer_size(131_072)
            .build()
            .await
            .unwrap();
        assert!(!db.is_closed());
    }

    #[tokio::test]
    async fn test_builder_from_laminardb() {
        let db = LaminarDB::builder().build().await.unwrap();
        assert!(!db.is_closed());
    }

    #[test]
    fn test_builder_debug() {
        let builder = LaminarDbBuilder::new().config_var("K", "V");
        let debug = format!("{builder:?}");
        assert!(debug.contains("LaminarDbBuilder"));
        assert!(debug.contains("config_vars_count: 1"));
    }

    #[tokio::test]
    async fn test_builder_register_udf() {
        use std::any::Any;
        use std::hash::{Hash, Hasher};

        use arrow::datatypes::DataType;
        use datafusion_expr::{
            ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
        };

        /// Trivial UDF that returns 42.
        #[derive(Debug)]
        struct FortyTwo {
            signature: Signature,
        }

        impl FortyTwo {
            fn new() -> Self {
                Self {
                    signature: Signature::new(TypeSignature::Nullary, Volatility::Immutable),
                }
            }
        }

        impl PartialEq for FortyTwo {
            fn eq(&self, _: &Self) -> bool {
                true
            }
        }

        impl Eq for FortyTwo {}

        impl Hash for FortyTwo {
            fn hash<H: Hasher>(&self, state: &mut H) {
                "forty_two".hash(state);
            }
        }

        impl ScalarUDFImpl for FortyTwo {
            fn as_any(&self) -> &dyn Any {
                self
            }
            fn name(&self) -> &'static str {
                "forty_two"
            }
            fn signature(&self) -> &Signature {
                &self.signature
            }
            fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
                Ok(DataType::Int64)
            }
            fn invoke_with_args(
                &self,
                _args: ScalarFunctionArgs,
            ) -> datafusion_common::Result<ColumnarValue> {
                Ok(ColumnarValue::Scalar(
                    datafusion_common::ScalarValue::Int64(Some(42)),
                ))
            }
        }

        let udf = ScalarUDF::new_from_impl(FortyTwo::new());
        let db = LaminarDB::builder()
            .register_udf(udf)
            .build()
            .await
            .unwrap();

        // Verify the UDF is queryable
        let result = db.execute("SELECT forty_two()").await;
        assert!(result.is_ok(), "UDF should be callable: {result:?}");
    }
}
