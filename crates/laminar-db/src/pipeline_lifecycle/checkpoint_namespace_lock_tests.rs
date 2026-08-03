use std::sync::atomic::Ordering;
use std::sync::Arc;

use laminar_connectors::connector::DeliveryGuarantee;

fn exact_builder_for_checkpoint(
    checkpoint_dir: &std::path::Path,
) -> crate::builder::LaminarDbBuilder {
    crate::db::LaminarDB::builder()
        .storage_dir(checkpoint_dir)
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: Some(1_000),
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::ExactlyOnce)
}

fn exact_builder(root: &std::path::Path) -> crate::builder::LaminarDbBuilder {
    exact_builder_for_checkpoint(&root.join("checkpoints"))
}

async fn exact_db(root: &std::path::Path) -> Arc<crate::db::LaminarDB> {
    exact_builder(root).build().await.unwrap()
}

async fn install_generator_pipeline(db: &Arc<crate::db::LaminarDB>) {
    db.execute(
        "CREATE SOURCE generated_source (seq BIGINT NOT NULL, ts_ms BIGINT NOT NULL, \
         value VARCHAR NOT NULL) FROM GENERATOR \
         ('rows.per.second' = '1000', 'max.rows' = '1')",
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
async fn startup_uses_one_checkpoint_state_budget_for_admission_and_storage() {
    let root = tempfile::tempdir().unwrap();
    let max_node_data_bytes = 29;
    let db = crate::db::LaminarDB::builder()
        .storage_dir(root.path())
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            max_node_data_bytes: Some(max_node_data_bytes),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();

    db.start().await.unwrap();
    {
        let coordinator = db.coordinator.lock().await;
        let coordinator = coordinator
            .as_ref()
            .expect("checkpoint configuration must install a coordinator");
        assert_eq!(
            coordinator.config().max_node_data_bytes,
            max_node_data_bytes
        );
        assert_eq!(
            coordinator.store().max_node_data_bytes(),
            max_node_data_bytes
        );
    }
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn checkpoint_does_not_invent_a_watermark_for_an_uninitialized_channel() {
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
        i64::MIN,
        "channel progress, not the process-local aggregate, defines the durable frontier",
    );
    assert_eq!(
        source.source.current_watermark(),
        i64::MIN,
        "an uninitialized source channel must remain uninitialized after recovery",
    );
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_exact_file_url_uses_the_durable_locked_namespace() {
    let root = tempfile::tempdir().unwrap();
    let object_root = root.path().join("remote-shaped-checkpoints");
    std::fs::create_dir_all(&object_root).unwrap();
    let normalized = object_root.display().to_string().replace('\\', "/");
    let checkpoint_url = if normalized.starts_with('/') {
        format!("file://{normalized}")
    } else {
        format!("file:///{normalized}")
    };
    let first = exact_builder(root.path())
        .object_store_url(checkpoint_url.clone())
        .build()
        .await
        .unwrap();
    let second = exact_builder(root.path())
        .object_store_url(checkpoint_url)
        .build()
        .await
        .unwrap();
    install_generator_pipeline(&first).await;
    install_generator_pipeline(&second).await;

    first.start().await.unwrap();
    assert!(first.checkpoint_namespace_lock.lock().is_some());
    wait_for_processing_cycle(&first).await;
    let checkpoint = first.checkpoint().await.unwrap();
    assert!(checkpoint.success, "{:?}", checkpoint.error);
    assert!(object_root
        .join("nodes")
        .join("1")
        .join("checkpoints")
        .join(format!("{:020}", checkpoint.checkpoint_id))
        .join("manifest.json")
        .is_file());
    let error = second
        .start()
        .await
        .expect_err("a second process must not share the file:// checkpoint root");
    assert!(error.to_string().contains("[LDB-0014]"), "{error}");

    let decisions =
        laminar_core::checkpoint_decision::CheckpointDecisionStore::local_filesystem(&object_root)
            .unwrap();
    assert!(decisions.load_or_create_deployment_id().await.is_ok());

    first.shutdown().await.unwrap();
    second.start().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_at_least_once_file_url_is_node_durable_and_exclusive() {
    let root = tempfile::tempdir().unwrap();
    let object_root = root.path().join("remote-shaped-checkpoints");
    std::fs::create_dir_all(&object_root).unwrap();
    let normalized = object_root.display().to_string().replace('\\', "/");
    let checkpoint_url = if normalized.starts_with('/') {
        format!("file://{normalized}")
    } else {
        format!("file:///{normalized}")
    };
    let first = exact_builder(root.path())
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .object_store_url(checkpoint_url.clone())
        .build()
        .await
        .unwrap();
    let second = exact_builder(root.path())
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .object_store_url(checkpoint_url)
        .build()
        .await
        .unwrap();

    first.start().await.unwrap();
    assert!(first.checkpoint_namespace_lock.lock().is_some());
    let error = second
        .start()
        .await
        .expect_err("a second process must not share the file:// checkpoint root");
    assert!(error.to_string().contains("[LDB-0014]"), "{error}");

    first.shutdown().await.unwrap();
    second.start().await.unwrap();
    second.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_at_least_once_rejects_an_unfenced_shared_checkpoint_namespace() {
    let root = tempfile::tempdir().unwrap();
    let error = exact_builder(root.path())
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .object_store_url("s3://shared-checkpoints/deployment")
        .build()
        .await
        .expect_err("a shared replay writer requires a remote fencing lease");
    assert!(error.to_string().contains("[LDB-0014]"), "{error}");
}

#[tokio::test]
async fn local_best_effort_cannot_bypass_a_replay_namespace_lock() {
    let root = tempfile::tempdir().unwrap();
    let checkpoint_dir = root.path().join("checkpoints");
    let best_effort = exact_builder_for_checkpoint(&checkpoint_dir)
        .delivery_guarantee(DeliveryGuarantee::BestEffort)
        .build()
        .await
        .unwrap();
    let replay = exact_builder_for_checkpoint(&checkpoint_dir)
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .unwrap();

    best_effort.start().await.unwrap();
    assert!(best_effort.checkpoint_namespace_lock.lock().is_some());
    let error = replay
        .start()
        .await
        .expect_err("best-effort must not bypass the checkpoint namespace lease");
    assert!(error.to_string().contains("[LDB-0014]"), "{error}");

    best_effort.shutdown().await.unwrap();
    replay.start().await.unwrap();
    replay.shutdown().await.unwrap();
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
