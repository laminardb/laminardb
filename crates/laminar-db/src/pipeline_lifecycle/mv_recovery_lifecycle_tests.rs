use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use laminar_core::streaming::StreamCheckpointConfig;

use crate::config::LaminarConfig;
use crate::db::LaminarDB;

async fn install_generator_mvs(db: &Arc<LaminarDB>, max_rows: u64) {
    db.execute(&format!(
        "CREATE SOURCE generated (seq BIGINT NOT NULL, ts_ms BIGINT NOT NULL, \
         value VARCHAR NOT NULL) FROM GENERATOR \
         ('rows.per.second' = '1000', 'max.rows' = '{max_rows}')"
    ))
    .await
    .unwrap();
    for view in ["committed", "empty_at_cut"] {
        db.execute(&format!(
            "CREATE MATERIALIZED VIEW {view} AS SELECT seq FROM generated \
             WITH (retention = '1000 rows')"
        ))
        .await
        .unwrap();
    }
}

fn update_mv(db: &LaminarDB, name: &str, values: Vec<i64>) {
    let schema = Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
        .expect("test MV batch");
    db.mv_store
        .write()
        .update_cycle(name, &[batch])
        .expect("test MV update");
}

async fn mv_values(db: &LaminarDB, name: &str) -> Vec<i64> {
    let batch = crate::mv_store::read_views(&db.mv_store, &[name.to_owned()])
        .await
        .unwrap()
        .remove(name)
        .unwrap();
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

    assert_eq!(mv_values(&db, "committed").await, [1]);
    assert!(mv_values(&db, "empty_at_cut").await.is_empty());
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

    assert!(mv_values(&db, "committed").await.is_empty());
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

    assert!(mv_values(&db, "committed").await.is_empty());
    assert_eq!(db.pipeline_watermark.load(Ordering::Acquire), i64::MIN);
    assert_eq!(source.source.current_watermark(), i64::MIN);
    db.shutdown().await.unwrap();
}

/// Committed append-log rows stay in the checkpoint store, so a restart under a memory limit
/// they would exceed reads them back without loading them into memory.
#[tokio::test]
async fn committed_append_rows_restore_without_counting_against_memory_limits() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = LaminarDB::open_with_config(checkpoint_config(dir.path())).unwrap();
        install_generator_mvs(&db, 0).await;
        db.start().await.unwrap();
        update_mv(&db, "committed", vec![10, 20]);
        let checkpoint = db.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{:?}", checkpoint.error);
        db.shutdown().await.unwrap();
    }
    let mut config = checkpoint_config(dir.path());
    config.materialized_view_max_rows = 1;
    let db = LaminarDB::open_with_config(config).unwrap();
    install_generator_mvs(&db, 0).await;
    db.start().await.unwrap();
    assert_eq!(mv_values(&db, "committed").await, [10, 20]);
    assert!(mv_values(&db, "empty_at_cut").await.is_empty());
    assert_eq!(db.mv_store.read().total_bytes(), 0);
    db.shutdown().await.unwrap();
}
