//! Public checkpoint-path integration tests.

use std::sync::Arc;
use std::time::Duration;

use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::checkpoint::checkpoint_manifest::CHECKPOINT_MANIFEST_VERSION;
use laminar_core::checkpoint::{CheckpointStore, ObjectStoreCheckpointStore, StateFrameKey};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::{LaminarConfig, LaminarDB};

fn checkpoint_config(directory: &std::path::Path, guarantee: DeliveryGuarantee) -> LaminarConfig {
    LaminarConfig {
        storage_dir: Some(directory.to_path_buf()),
        checkpoint: Some(StreamCheckpointConfig {
            interval_ms: None,
            ..StreamCheckpointConfig::default()
        }),
        delivery_guarantee: guarantee,
        ..LaminarConfig::default()
    }
}

async fn run_manual_checkpoint(guarantee: DeliveryGuarantee) {
    let directory = tempfile::tempdir().unwrap();
    let db = LaminarDB::open_with_config(checkpoint_config(directory.path(), guarantee)).unwrap();

    db.execute(
        "CREATE SOURCE sensors (seq BIGINT NOT NULL, ts_ms BIGINT NOT NULL, \
         value VARCHAR NOT NULL) FROM GENERATOR \
         ('rows.per.second' = '1000', 'max.rows' = '8')",
    )
    .await
    .unwrap();
    db.execute(
        "CREATE STREAM totals AS \
         SELECT value, COUNT(*) AS records FROM sensors GROUP BY value",
    )
    .await
    .unwrap();
    db.start().await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if db
                .stream_metrics("totals")
                .is_some_and(|metrics| metrics.total_events > 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the stateful stream must process input before checkpointing");

    let result = db.checkpoint().await.unwrap();
    assert!(result.success, "checkpoint failed: {:?}", result.error);
    assert_eq!(result.checkpoint_id, result.epoch);

    let object_store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap());
    let store = ObjectStoreCheckpointStore::new(object_store, "");
    let manifest = store
        .load_manifest(result.checkpoint_id)
        .await
        .unwrap()
        .expect("the exact committed participant manifest must exist");

    assert_eq!(manifest.version, CHECKPOINT_MANIFEST_VERSION);
    assert_eq!(manifest.checkpoint_id, result.checkpoint_id);
    assert_eq!(manifest.epoch, result.epoch);
    assert!(manifest
        .state_frames
        .iter()
        .any(|frame| matches!(&frame.key, StateFrameKey::Vnode { .. })));

    let checkpoint_directory = directory.path().join(format!(
        "nodes/{}/checkpoints/{:020}",
        manifest.participant_id, manifest.checkpoint_id
    ));
    assert!(checkpoint_directory.join("manifest.json").is_file());
    assert!(checkpoint_directory.join("node-data.bin").is_file());

    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn at_least_once_persists_one_node_checkpoint_object() {
    run_manual_checkpoint(DeliveryGuarantee::AtLeastOnce).await;
}

#[tokio::test]
async fn exactly_once_persists_one_node_checkpoint_object() {
    run_manual_checkpoint(DeliveryGuarantee::ExactlyOnce).await;
}

#[tokio::test]
async fn replay_guarantees_require_checkpointing() {
    let db = LaminarDB::open_with_config(LaminarConfig {
        delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
        checkpoint: None,
        ..LaminarConfig::default()
    })
    .unwrap();
    db.execute(
        "CREATE SOURCE input (seq BIGINT NOT NULL, ts_ms BIGINT NOT NULL, \
         value VARCHAR NOT NULL) FROM GENERATOR ('max.rows' = '1')",
    )
    .await
    .unwrap();
    db.execute("CREATE STREAM output AS SELECT seq FROM input")
        .await
        .unwrap();

    let error = db
        .start()
        .await
        .expect_err("at-least-once must not start without checkpoint storage");
    assert!(error.to_string().contains("[LDB-5032]"), "{error}");
}
