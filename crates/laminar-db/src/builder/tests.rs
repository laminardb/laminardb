use super::*;

#[test]
fn runtime_builder_options_are_preserved() {
    let builder = LaminarDbBuilder::new()
        .pipeline_max_managed_state_bytes(123_456)
        .source_idle_timeout(std::time::Duration::from_secs(5))
        .event_time_max_future_skew(std::time::Duration::from_secs(30));

    assert_eq!(
        builder.config.pipeline_max_managed_state_bytes,
        Some(123_456)
    );
    assert_eq!(
        builder.config.source_idle_timeout,
        Some(std::time::Duration::from_secs(5))
    );
    assert_eq!(
        builder.config.event_time_max_future_skew,
        std::time::Duration::from_secs(30)
    );
}

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

#[cfg(feature = "cluster")]
async fn test_verified_cluster_namespaces(
    controller: &laminar_core::cluster::control::ClusterController,
    checkpoint_store: Arc<dyn object_store::ObjectStore>,
) -> laminar_core::cluster::control::VerifiedClusterNamespaces {
    use laminar_core::checkpoint::CheckpointParticipant;
    use laminar_core::cluster::control::{
        prove_shared_object_store_namespaces, ClusterKv, InMemoryKv,
    };

    let participant = CheckpointParticipant {
        node_id: controller.instance_id().0,
        boot_incarnation: controller.recovery_incarnation(),
    };
    let control: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(controller.instance_id()));
    prove_shared_object_store_namespaces(
        participant,
        &[participant],
        control,
        checkpoint_store,
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn test_default_builder() {
    let db = LaminarDbBuilder::new().build().await.unwrap();
    assert!(!db.is_closed());
    assert!(!db.is_cluster_runtime());
    assert_eq!(
        db.checkpoint_key_groups(),
        laminar_core::state::DEFAULT_KEY_GROUP_COUNT
    );
    let registry = db.vnode_registry.lock().clone().unwrap();
    assert_eq!(
        laminar_core::state::owned_vnodes(&registry, laminar_core::state::LOCAL_NODE_ID).len(),
        usize::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get())
    );
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

#[tokio::test]
async fn local_replay_delivery_rejects_cloud_checkpoint_storage_at_build() {
    use laminar_connectors::connector::DeliveryGuarantee;

    let error = LaminarDbBuilder::new()
        .checkpoint(StreamCheckpointConfig::default())
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .object_store_url("s3://shared-checkpoints/deployment")
        .build()
        .await
        .expect_err("an unfenced local cloud writer must fail before startup");
    assert!(error.to_string().contains("[LDB-0014]"), "{error}");
}

#[tokio::test]
async fn checkpoint_url_conflicts_with_checkpoint_data_dir() {
    let error = LaminarDbBuilder::new()
        .checkpoint(StreamCheckpointConfig {
            data_dir: Some(std::path::PathBuf::from("checkpoint-data")),
            ..StreamCheckpointConfig::default()
        })
        .object_store_url("memory://checkpoint-test")
        .build()
        .await
        .expect_err("two checkpoint authorities must be rejected");
    assert!(error.to_string().contains("conflicts"), "{error}");
}

#[tokio::test]
async fn checkpoint_node_data_budget_is_resolved() {
    for max_node_data_bytes in [17, isize::MAX as u64] {
        let directory = tempfile::tempdir().unwrap();
        let db = LaminarDbBuilder::new()
            .storage_dir(directory.path())
            .checkpoint(StreamCheckpointConfig {
                max_node_data_bytes: Some(max_node_data_bytes),
                ..StreamCheckpointConfig::default()
            })
            .build()
            .await
            .unwrap();

        assert_eq!(
            db.config
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.max_node_data_bytes),
            Some(max_node_data_bytes)
        );
    }

    let directory = tempfile::tempdir().unwrap();
    let error = LaminarDbBuilder::new()
        .storage_dir(directory.path())
        .checkpoint(StreamCheckpointConfig {
            max_node_data_bytes: Some((isize::MAX as u64) + 1),
            ..StreamCheckpointConfig::default()
        })
        .build()
        .await
        .expect_err("an allocation-unrepresentable checkpoint budget must be rejected");
    assert!(
        error
            .to_string()
            .contains("exceeds this process address space"),
        "{error}"
    );
}

#[tokio::test]
async fn checkpoint_node_data_budget_uses_the_core_default() {
    let directory = tempfile::tempdir().unwrap();
    let db = LaminarDbBuilder::new()
        .storage_dir(directory.path())
        .checkpoint(StreamCheckpointConfig::default())
        .build()
        .await
        .unwrap();
    let expected =
        laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES;

    assert_eq!(
        db.config
            .checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.max_node_data_bytes),
        Some(expected)
    );
}

#[tokio::test]
async fn explicit_profile_cannot_bypass_file_url_validation() {
    let error = LaminarDbBuilder::new()
        .profile(Profile::BareMetal)
        .object_store_url("file://./relative")
        .build()
        .await
        .expect_err("malformed checkpoint URLs must fail independently of tuning profile");
    assert!(
        error.to_string().contains("invalid object store URL"),
        "{error}"
    );
}

#[tokio::test]
async fn malformed_file_authority_cannot_bypass_file_url_validation() {
    let error = LaminarDbBuilder::new()
        .profile(Profile::BareMetal)
        .object_store_url("FILE://%")
        .build()
        .await
        .expect_err("a malformed file URL must not fall back to a local path");
    assert!(
        error.to_string().contains("invalid object store URL"),
        "{error}"
    );
}

#[test]
fn cluster_delivery_defers_exact_connector_certification() {
    use laminar_connectors::connector::DeliveryGuarantee;

    assert!(LaminarDbBuilder::validate_cluster_delivery(
        RuntimeMode::Cluster,
        DeliveryGuarantee::AtLeastOnce,
    )
    .is_ok());
    assert!(LaminarDbBuilder::validate_cluster_delivery(
        RuntimeMode::Cluster,
        DeliveryGuarantee::ExactlyOnce,
    )
    .is_ok());
    assert!(LaminarDbBuilder::validate_cluster_delivery(
        RuntimeMode::Cluster,
        DeliveryGuarantee::BestEffort,
    )
    .is_err());

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
    let controller = test_cluster_controller();
    let verified_namespaces =
        test_verified_cluster_namespaces(controller.as_ref(), Arc::clone(&checkpoint_store)).await;
    let db = LaminarDbBuilder::new()
        .cluster_controller(controller)
        .verified_cluster_namespaces(verified_namespaces)
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
async fn injected_checkpoint_store_conflicts_with_checkpoint_data_dir() {
    let error = LaminarDbBuilder::new()
        .cluster_controller(test_cluster_controller())
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .checkpoint(StreamCheckpointConfig {
            data_dir: Some(std::path::PathBuf::from("checkpoint-data")),
            ..StreamCheckpointConfig::default()
        })
        .build()
        .await
        .expect_err("two checkpoint authorities must be rejected");
    assert!(error.to_string().contains("conflict"), "{error}");
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
        laminar_core::state::DEFAULT_KEY_GROUP_COUNT
    );
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
    assert!(
        error.to_string().contains("VerifiedClusterNamespaces"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn complete_cluster_profile_selects_cluster_runtime() {
    let controller = test_cluster_controller();
    let verified_namespaces =
        test_verified_cluster_namespaces(controller.as_ref(), test_cluster_checkpoint_store())
            .await;
    let db = LaminarDbBuilder::new()
        .profile(Profile::Cluster)
        .cluster_controller(controller)
        .verified_cluster_namespaces(verified_namespaces)
        .build()
        .await
        .unwrap();
    assert!(db.is_cluster_runtime());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_runtime_rejects_node_local_checkpoint_url() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().display().to_string().replace('\\', "/");
    let url = if path.starts_with('/') {
        format!("file://{path}")
    } else {
        format!("file:///{path}")
    };

    let error = LaminarDbBuilder::new()
        .profile(Profile::Cluster)
        .object_store_url(url)
        .cluster_controller(test_cluster_controller())
        .build()
        .await
        .expect_err("cluster recovery must not depend on node-local checkpoint storage");
    assert!(error.to_string().contains("[LDB-0011]"), "{error}");
    assert!(
        error
            .to_string()
            .contains("successful shared namespace proof"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_runtime_rejects_shared_checkpoint_url_without_proof() {
    let error = LaminarDbBuilder::new()
        .profile(Profile::Cluster)
        .object_store_url("s3://checkpoint-test/cluster")
        .cluster_controller(test_cluster_controller())
        .build()
        .await
        .expect_err("a shared URL must not bypass the exact namespace proof");
    assert!(error.to_string().contains("[LDB-0011]"), "{error}");
    assert!(
        error
            .to_string()
            .contains("successful shared namespace proof"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_runtime_rejects_namespace_proof_from_another_process() {
    let proved_controller = test_cluster_controller();
    let verified_namespaces = test_verified_cluster_namespaces(
        proved_controller.as_ref(),
        test_cluster_checkpoint_store(),
    )
    .await;
    let runtime_controller = test_cluster_controller();

    let error = LaminarDbBuilder::new()
        .cluster_controller(runtime_controller)
        .verified_cluster_namespaces(verified_namespaces)
        .build()
        .await
        .expect_err("a namespace proof must remain bound to its process incarnation");
    assert!(error.to_string().contains("[LDB-0011]"), "{error}");
    assert!(error.to_string().contains("belong to node"), "{error}");
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
    assert!(error.to_string().contains("conflict"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn cluster_exactly_once_build_reaches_connector_admission() {
    use laminar_connectors::connector::DeliveryGuarantee;

    let db = LaminarDbBuilder::new()
        .profile(Profile::Cluster)
        .cluster_controller(test_cluster_controller())
        .cluster_checkpoint_object_store(test_cluster_checkpoint_store())
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
        .build()
        .await
        .expect("runtime-level admission must defer to concrete connector contracts");
    assert_eq!(db.config.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
}

#[tokio::test]
async fn injected_vnode_registry_must_fit_checkpoint_abi() {
    let vnode_count = laminar_core::state::MAX_KEY_GROUP_COUNT + 1;
    let error = LaminarDbBuilder::new()
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
async fn local_runtime_accepts_owned_multi_key_group_registry() {
    let db = LaminarDbBuilder::new()
        .vnode_registry(Arc::new(laminar_core::state::VnodeRegistry::single_owner(
            2,
            laminar_core::state::LOCAL_NODE_ID,
        )))
        .build()
        .await
        .expect("local runtimes use the same multi-key-group topology");
    assert_eq!(db.checkpoint_key_groups().get(), 2);
}

#[tokio::test]
async fn local_runtime_rejects_nonlocal_vnode_ownership() {
    for registry in [
        Arc::new(laminar_core::state::VnodeRegistry::new(2)),
        Arc::new(laminar_core::state::VnodeRegistry::single_owner(
            2,
            laminar_core::state::NodeId(9),
        )),
    ] {
        let error = LaminarDbBuilder::new()
            .vnode_registry(registry)
            .build()
            .await
            .expect_err("local runtimes must own every configured vnode");
        assert!(error.to_string().contains("must be owned by 1"), "{error}");
    }
}

#[tokio::test]
async fn configured_key_groups_build_an_owned_local_registry() {
    let configured = laminar_core::state::KeyGroupCount::try_from(64_u16).unwrap();
    let db = LaminarDbBuilder::new()
        .key_groups(configured)
        .build()
        .await
        .unwrap();
    let registry = db.vnode_registry.lock().clone().unwrap();
    assert_eq!(registry.vnode_count(), 64);
    assert_eq!(
        laminar_core::state::owned_vnodes(&registry, laminar_core::state::LOCAL_NODE_ID).len(),
        64
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
async fn builder_preserves_custom_function_registry_for_graph_contexts() {
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
        fn as_any(&self) -> &dyn std::any::Any {
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

    let udf = ScalarUDF::new_from_impl(FortyTwo::new()).with_aliases(["abs"]);
    let udaf = AggregateUDF::new_from_impl(laminar_sql::datafusion::JsonAgg::new())
        .with_aliases(["custom_json_agg"]);
    let db = LaminarDB::builder()
        .register_udf(udf)
        .register_udaf(udaf)
        .build()
        .await
        .unwrap();

    // Verify the UDF is queryable
    let result = db.execute("SELECT forty_two()").await;
    assert!(result.is_ok(), "UDF should be callable: {result:?}");

    let graph_ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&graph_ctx);
    db.register_custom_functions_into(&graph_ctx);
    let graph_state = graph_ctx.state();
    assert_eq!(graph_state.scalar_functions()["abs"].name(), "forty_two");
    assert!(graph_state
        .aggregate_functions()
        .contains_key("custom_json_agg"));
}

#[tokio::test]
async fn builder_rejects_reserved_core_window_function_names_and_aliases() {
    let canonical = ScalarUDF::new_from_impl(laminar_sql::datafusion::TumbleWindowStart::new());
    let canonical_error = LaminarDB::builder()
        .register_udf(canonical)
        .build()
        .await
        .unwrap_err();
    assert!(canonical_error
        .to_string()
        .contains("reserved CoreWindow marker name or alias 'tumble'"));

    let alias = AggregateUDF::new_from_impl(laminar_sql::datafusion::JsonAgg::new())
        .with_aliases(["SeSsIoN"]);
    let alias_error = LaminarDB::builder()
        .register_udaf(alias)
        .build()
        .await
        .unwrap_err();
    assert!(alias_error
        .to_string()
        .contains("reserved CoreWindow marker name or alias 'SeSsIoN'"));
}
