use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use laminar_core::streaming::StreamCheckpointConfig;

use crate::config::LaminarConfig;
use crate::db::LaminarDB;

async fn install_generator_mvs(db: &Arc<LaminarDB>, max_rows: u64) {
    db.execute(&format!(
        "CREATE SOURCE generated (seq BIGINT, ts_ms BIGINT, value VARCHAR) WITH \
         ('connector' = 'generator', 'rows.per.second' = '1000', 'max.rows' = '{max_rows}')"
    ))
    .await
    .unwrap();
    db.execute("CREATE MATERIALIZED VIEW committed AS SELECT seq FROM generated")
        .await
        .unwrap();
    db.execute("CREATE MATERIALIZED VIEW empty_at_cut AS SELECT seq FROM generated")
        .await
        .unwrap();
}

fn update_mv(db: &LaminarDB, name: &str, values: Vec<i64>) {
    let schema = db
        .mv_store
        .read()
        .to_record_batch(name)
        .unwrap()
        .unwrap()
        .schema();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))])
        .expect("test MV batch");
    db.mv_store
        .write()
        .update_cycle(name, &[batch])
        .expect("test MV update");
}

fn mv_values(db: &LaminarDB, name: &str) -> Vec<i64> {
    let batch = db.mv_store.read().to_record_batch(name).unwrap().unwrap();
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

    assert_eq!(mv_values(&db, "committed"), [1]);
    assert!(mv_values(&db, "empty_at_cut").is_empty());
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

    assert!(mv_values(&db, "committed").is_empty());
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

    assert!(mv_values(&db, "committed").is_empty());
    assert_eq!(db.pipeline_watermark.load(Ordering::Acquire), i64::MIN);
    assert_eq!(source.source.current_watermark(), i64::MIN);
    db.shutdown().await.unwrap();
}
