//! Fluent builder for `LaminarDB` construction.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::path::PathBuf;

use datafusion_expr::{AggregateUDF, ScalarUDF};
use laminar_core::streaming::{BackpressureStrategy, StreamCheckpointConfig};

use crate::config::LaminarConfig;
use crate::db::LaminarDB;
use crate::error::DbError;
use crate::profile::Profile;

/// Callback for registering custom connectors.
type ConnectorCallback = Box<dyn FnOnce(&laminar_connectors::registry::ConnectorRegistry) + Send>;

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

    /// Install the cluster control facade; activates cluster-mode checkpoint/shuffle.
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

    /// Cap total operator state held in memory. Crossing the budget pauses
    /// source intake (backpressure, not failure) until state drains below it.
    #[must_use]
    pub fn state_memory_budget_bytes(mut self, bytes: usize) -> Self {
        self.config.state_memory_budget_bytes = Some(bytes);
        self
    }

    /// Enable the disk cold tier at `dir`. With a memory budget set, operator
    /// state approaching the budget is demoted here instead of backpressuring,
    /// and fetched back on demand. Requires the `state-tier` build feature.
    #[cfg(feature = "state-tier")]
    #[must_use]
    pub fn state_tier_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.state_tier_dir = Some(dir.into());
        self
    }

    /// Set checkpoint configuration.
    #[must_use]
    pub fn checkpoint(mut self, config: StreamCheckpointConfig) -> Self {
        self.config.checkpoint = Some(config);
        self
    }

    /// Set the deployment profile.
    ///
    /// See [`Profile`] for the available tiers.
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

    /// Register custom connectors; the callback runs after built-ins are wired.
    #[must_use]
    pub fn register_connector(
        mut self,
        f: impl FnOnce(&laminar_connectors::registry::ConnectorRegistry) + Send + 'static,
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
    pub async fn build(mut self) -> Result<LaminarDB, DbError> {
        self.config.object_store_url = self.object_store_url;
        self.config.object_store_options = self.object_store_options;

        if !self.profile_explicit {
            self.profile = Profile::from_config(&self.config, false);
        }

        self.profile
            .validate_features()
            .map_err(|e| DbError::Config(e.to_string()))?;
        self.profile
            .validate_config(&self.config, self.config.object_store_url.as_deref())
            .map_err(|e| DbError::Config(e.to_string()))?;

        Self::validate_backpressure(&self.config)?;

        // state_backend and vnode_registry must be paired or both absent.
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

        self.profile.apply_defaults(&mut self.config);

        let mut db = LaminarDB::open_with_config_and_vars_and_rules(
            self.config,
            self.config_vars,
            &self.physical_optimizer_rules,
            self.target_partitions,
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
            callback(db.connector_registry());
        }
        for udf in self.custom_udfs {
            db.register_custom_udf(udf);
        }
        for udaf in self.custom_udafs {
            db.register_custom_udaf(udaf);
        }
        #[cfg(feature = "cluster")]
        if let Some(controller) = self.cluster_controller {
            db.set_cluster_controller(controller);
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
        Ok(db)
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

    #[tokio::test]
    async fn test_default_builder() {
        let db = LaminarDbBuilder::new().build().await.unwrap();
        assert!(!db.is_closed());
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
