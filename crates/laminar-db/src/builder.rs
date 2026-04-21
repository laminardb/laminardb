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
    /// Cluster control facade installed at cluster-mode startup.
    /// Stays `None` in embedded / single-instance builds.
    #[cfg(feature = "cluster-unstable")]
    cluster_controller: Option<std::sync::Arc<laminar_core::cluster::control::ClusterController>>,
    /// Outbound shuffle handle for cluster-mode streaming aggregates.
    /// Pair with `shuffle_receiver`; without it, streaming aggregates
    /// run single-node even when the cluster controller is installed
    /// (see Phase 0a in `docs/plans/cluster-production-readiness.md`).
    #[cfg(feature = "cluster-unstable")]
    shuffle_sender: Option<std::sync::Arc<laminar_core::shuffle::ShuffleSender>>,
    /// Inbound shuffle handle for cluster-mode streaming aggregates.
    #[cfg(feature = "cluster-unstable")]
    shuffle_receiver: Option<std::sync::Arc<laminar_core::shuffle::ShuffleReceiver>>,
    /// Durable cluster 2PC decision store. Written by the leader after
    /// the prepare quorum but before announcing `Commit`, so a new
    /// leader elected mid-2PC can recover the cluster vote. Required
    /// for cross-instance sink 2PC correctness. Without it, recovery
    /// falls back to blanket Abort on any Pending manifest.
    #[cfg(feature = "cluster-unstable")]
    decision_store:
        Option<std::sync::Arc<laminar_core::cluster::control::CheckpointDecisionStore>>,
    /// Optional state backend. When paired with `vnode_registry`, the
    /// coordinator writes per-vnode durability markers each checkpoint
    /// and consults `epoch_complete` before committing sinks.
    state_backend: Option<std::sync::Arc<dyn laminar_core::state::StateBackend>>,
    /// Optional vnode topology. See `state_backend`.
    vnode_registry: Option<std::sync::Arc<laminar_core::state::VnodeRegistry>>,
    /// Extra physical optimizer rules installed on the `SessionState`
    /// at construction.
    physical_optimizer_rules: Vec<
        std::sync::Arc<dyn datafusion::physical_optimizer::PhysicalOptimizerRule + Send + Sync>,
    >,
    /// Override for `target_partitions`; cluster mode sets this to
    /// `vnode_count`. Default 1 for single-instance streaming.
    target_partitions: Option<usize>,
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
            #[cfg(feature = "cluster-unstable")]
            cluster_controller: None,
            #[cfg(feature = "cluster-unstable")]
            shuffle_sender: None,
            #[cfg(feature = "cluster-unstable")]
            shuffle_receiver: None,
            #[cfg(feature = "cluster-unstable")]
            decision_store: None,
            state_backend: None,
            vnode_registry: None,
            physical_optimizer_rules: Vec::new(),
            target_partitions: None,
        }
    }

    /// Override `target_partitions`; requires a distributed-aware
    /// physical optimizer rule to replace `RepartitionExec`.
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

    /// Install a state backend. Pair with [`Self::vnode_registry`];
    /// without the registry this call has no effect.
    #[must_use]
    pub fn state_backend(
        mut self,
        backend: std::sync::Arc<dyn laminar_core::state::StateBackend>,
    ) -> Self {
        self.state_backend = Some(backend);
        self
    }

    /// Install a vnode registry. Pair with [`Self::state_backend`];
    /// without the backend this call has no effect.
    #[must_use]
    pub fn vnode_registry(
        mut self,
        registry: std::sync::Arc<laminar_core::state::VnodeRegistry>,
    ) -> Self {
        self.vnode_registry = Some(registry);
        self
    }

    /// Install a cluster control facade. Activates cluster-mode
    /// checkpoint / shuffle semantics inside the engine. Called from
    /// `laminar-server`'s cluster startup path after discovery has
    /// converged.
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub fn cluster_controller(
        mut self,
        controller: std::sync::Arc<laminar_core::cluster::control::ClusterController>,
    ) -> Self {
        self.cluster_controller = Some(controller);
        self
    }

    /// Install the outbound shuffle handle used by cluster-mode streaming
    /// aggregates. Rows whose group key hashes to a remote vnode are
    /// shipped through this sender. Pair with [`Self::shuffle_receiver`];
    /// either alone is a no-op.
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub fn shuffle_sender(
        mut self,
        sender: std::sync::Arc<laminar_core::shuffle::ShuffleSender>,
    ) -> Self {
        self.shuffle_sender = Some(sender);
        self
    }

    /// Install the inbound shuffle handle used by cluster-mode streaming
    /// aggregates. Remote partial-aggregate rows arrive here and are
    /// drained into the local accumulator each cycle.
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub fn shuffle_receiver(
        mut self,
        receiver: std::sync::Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) -> Self {
        self.shuffle_receiver = Some(receiver);
        self
    }

    /// Install the durable cluster 2PC decision store. The leader
    /// writes `Decision::Committed` here after the prepare quorum but
    /// before announcing `Commit`, so a new leader elected mid-2PC
    /// can read the cluster vote from shared storage instead of
    /// defaulting to Abort (which could split state against
    /// already-committed followers).
    #[cfg(feature = "cluster-unstable")]
    #[must_use]
    pub fn decision_store(
        mut self,
        store: std::sync::Arc<laminar_core::cluster::control::CheckpointDecisionStore>,
    ) -> Self {
        self.decision_store = Some(store);
        self
    }

    /// Set a config variable for `${VAR}` substitution in SQL.
    #[must_use]
    pub fn config_var(mut self, key: &str, value: &str) -> Self {
        self.config_vars.insert(key.to_string(), value.to_string());
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

    /// Set the S3 storage class tiering configuration.
    #[must_use]
    pub fn tiering(mut self, tiering: crate::config::TieringConfig) -> Self {
        self.config.tiering = Some(tiering);
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

    /// Register a custom scalar UDF with the database.
    ///
    /// The UDF will be available in SQL queries after `build()`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use datafusion_expr::ScalarUDF;
    ///
    /// let db = LaminarDB::builder()
    ///     .register_udf(my_scalar_udf)
    ///     .build()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn register_udf(mut self, udf: ScalarUDF) -> Self {
        self.custom_udfs.push(udf);
        self
    }

    /// Register a custom aggregate UDF (UDAF) with the database.
    ///
    /// The UDAF will be available in SQL queries after `build()`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use datafusion_expr::AggregateUDF;
    ///
    /// let db = LaminarDB::builder()
    ///     .register_udaf(my_aggregate_udf)
    ///     .build()
    ///     .await?;
    /// ```
    #[must_use]
    pub fn register_udaf(mut self, udaf: AggregateUDF) -> Self {
        self.custom_udafs.push(udaf);
        self
    }

    /// Source → coordinator channel capacity (default 64). Increase for
    /// burst absorption at the cost of memory.
    #[must_use]
    pub fn pipeline_channel_capacity(mut self, capacity: usize) -> Self {
        self.config.pipeline_channel_capacity = Some(capacity);
        self
    }

    /// Micro-batch coalescing window (default 5ms for connectors, 0 for
    /// embedded). Larger values amortize per-cycle SQL overhead.
    #[must_use]
    pub fn pipeline_batch_window(mut self, window: std::time::Duration) -> Self {
        self.config.pipeline_batch_window = Some(window);
        self
    }

    /// Max time draining the source channel per cycle, in nanoseconds
    /// (default 1ms). Increase to process more messages per SQL execution.
    #[must_use]
    pub fn pipeline_drain_budget_ns(mut self, ns: u64) -> Self {
        self.config.pipeline_drain_budget_ns = Some(ns);
        self
    }

    /// Per-query execution budget in nanoseconds (default 8ms). When
    /// exceeded, remaining queries are deferred to the next cycle.
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

    /// Register custom connectors with the `ConnectorRegistry`.
    ///
    /// The callback is invoked after the database is created and built-in
    /// connectors are registered. Use it to add user-defined source/sink
    /// implementations.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = LaminarDB::builder()
    ///     .register_connector(|registry| {
    ///         registry.register_source("my-source", info, factory);
    ///     })
    ///     .build()
    ///     .await?;
    /// ```
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
        // Forward object store settings into the config before profile detection.
        self.config.object_store_url = self.object_store_url;
        self.config.object_store_options = self.object_store_options;

        // Auto-detect profile from config if not explicitly set.
        if !self.profile_explicit {
            self.profile = Profile::from_config(&self.config, false);
        }

        // Validate profile feature gates and config requirements.
        self.profile
            .validate_features()
            .map_err(|e| DbError::Config(e.to_string()))?;
        self.profile
            .validate_config(&self.config, self.config.object_store_url.as_deref())
            .map_err(|e| DbError::Config(e.to_string()))?;

        Self::validate_backpressure(&self.config)?;

        // Apply profile defaults for fields the user hasn't set.
        self.profile.apply_defaults(&mut self.config);

        let db = LaminarDB::open_with_config_and_vars_and_rules(
            self.config,
            self.config_vars,
            &self.physical_optimizer_rules,
            self.target_partitions,
        )?;
        for callback in self.connector_callbacks {
            callback(db.connector_registry());
        }
        for udf in self.custom_udfs {
            db.register_custom_udf(udf);
        }
        for udaf in self.custom_udafs {
            db.register_custom_udaf(udaf);
        }
        #[cfg(feature = "cluster-unstable")]
        if let Some(controller) = self.cluster_controller {
            db.set_cluster_controller(controller);
        }
        #[cfg(feature = "cluster-unstable")]
        if let Some(sender) = self.shuffle_sender {
            db.set_shuffle_sender(sender);
        }
        #[cfg(feature = "cluster-unstable")]
        if let Some(receiver) = self.shuffle_receiver {
            db.set_shuffle_receiver(receiver);
        }
        #[cfg(feature = "cluster-unstable")]
        if let Some(store) = self.decision_store {
            db.set_decision_store(store);
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

        // Non-default policy needs at least one finite, non-zero cap.
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
