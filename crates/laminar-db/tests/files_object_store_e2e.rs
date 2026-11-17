//! End-to-end: a files source over an S3 directory -> a keyed view, across a checkpointed
//! restart. Needs S3 on localhost:9000 with a `warehouse` bucket, which
//! `tests/docker/iceberg-compose.yml` provides (start it with its own `-p` project name).
#![cfg(all(feature = "files", feature = "aws"))]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, Int64Array};
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_db::{ExecuteResult, LaminarDB};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt as _, PutPayload};

const ENDPOINT: &str = "http://localhost:9000";
const BUCKET: &str = "warehouse";
const CONVERGE: Duration = Duration::from_secs(30);

fn bucket() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name(BUCKET)
            .with_endpoint(ENDPOINT)
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_region("us-east-1")
            .with_allow_http(true)
            .build()
            .expect("S3 client"),
    )
}

async fn put_ids(store: &dyn ObjectStore, key: &str, ids: &[i64]) {
    let body: String = ids.iter().map(|id| format!("{{\"id\":{id}}}\n")).collect();
    store
        .put(&ObjectPath::from(key), PutPayload::from(body.into_bytes()))
        .await
        .expect("put object");
}

fn statements(prefix: &str, splits: Option<u32>) -> Vec<String> {
    let splits = splits.map_or_else(String::new, |splits| {
        format!(",\n                'splits' = '{splits}'")
    });
    vec![
        format!(
            "CREATE SOURCE events (id BIGINT NOT NULL) FROM FILES (
                'path' = 's3://{BUCKET}/{prefix}',
                'poll_interval' = '50ms',
                'storage.aws_endpoint' = '{ENDPOINT}',
                'storage.aws_access_key_id' = 'minioadmin',
                'storage.aws_secret_access_key' = '${{E2E_S3_SECRET}}',
                'storage.aws_region' = 'us-east-1',
                'storage.aws_allow_http' = 'true'{splits}
             ) FORMAT JSON"
        ),
        "CREATE MATERIALIZED VIEW deliveries AS \
         SELECT id, COUNT(*) AS deliveries FROM events GROUP BY id"
            .to_string(),
    ]
}

async fn open(storage: &Path) -> Arc<LaminarDB> {
    LaminarDB::builder()
        .storage_dir(storage)
        .config_var("E2E_S3_SECRET", "minioadmin")
        .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
            interval_ms: None,
            ..Default::default()
        })
        .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
        .build()
        .await
        .expect("open database")
}

/// Open and register `statements`, waiting for the previous generation's locks.
async fn reopen(storage: &Path, statements: &[String]) -> Arc<LaminarDB> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let db = open(storage).await;
        let mut failure = None;
        for statement in statements {
            if let Err(error) = db.execute(statement).await {
                failure = Some(format!("{statement}: {error}"));
                break;
            }
        }
        match failure {
            None => return db,
            Some(error) if error.contains("LDB-0014") && tokio::time::Instant::now() < deadline => {
                drop(db);
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Some(error) => panic!("{error}"),
        }
    }
}

async fn checkpoint(db: &LaminarDB) {
    let result = db.checkpoint().await.expect("checkpoint");
    assert!(result.success, "checkpoint failed: {result:?}");
}

/// Deliveries per id once every id in `ids` has arrived.
async fn wait_for_ids(db: &LaminarDB, ids: std::ops::RangeInclusive<i64>) -> BTreeMap<i64, i64> {
    let deadline = tokio::time::Instant::now() + CONVERGE;
    loop {
        let deliveries = deliveries(db).await;
        if ids.clone().all(|id| deliveries.contains_key(&id)) {
            return deliveries;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "ids {ids:?} never all arrived: {deliveries:?}; fault {:?}",
            db.last_fault()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn deliveries(db: &LaminarDB) -> BTreeMap<i64, i64> {
    let ExecuteResult::Query(mut query) = db
        .execute("SELECT id, deliveries FROM deliveries")
        .await
        .expect("query")
    else {
        panic!("expected a query result");
    };
    let batches = tokio::time::timeout(CONVERGE, query.subscribe_raw().expect("rows").read_all())
        .await
        .expect("rows in time")
        .expect("query");
    let column = |batch: &arrow::array::RecordBatch, index: usize| {
        let cast = arrow::compute::cast(batch.column(index), &arrow::datatypes::DataType::Int64)
            .expect("int64 column");
        let array = cast.as_any().downcast_ref::<Int64Array>().expect("int64");
        (0..array.len())
            .map(|row| array.value(row))
            .collect::<Vec<_>>()
    };
    batches
        .iter()
        .flat_map(|batch| column(batch, 0).into_iter().zip(column(batch, 1)))
        .collect()
}

/// Objects read before the checkpoint are in its manifest by key, so none is read again after the
/// restart; objects below the directory are not read at all.
async fn reads_each_object_once_across_a_checkpointed_restart(splits: Option<u32>) {
    let store = bucket();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let prefix = format!("files-e2e/{nanos}");
    put_ids(store.as_ref(), &format!("{prefix}/a.json"), &[1, 2, 3]).await;
    put_ids(store.as_ref(), &format!("{prefix}/b.json"), &[4, 5]).await;
    put_ids(store.as_ref(), &format!("{prefix}/nested/x.json"), &[99]).await;
    let dir = tempfile::tempdir().expect("dir");
    let storage = dir.path().join("storage");
    let ddl = statements(&prefix, splits);

    let db = reopen(&storage, &ddl).await;
    db.start().await.expect("start");
    wait_for_ids(&db, 1..=5).await;
    checkpoint(&db).await;
    db.shutdown().await.expect("shutdown");
    drop(db);

    put_ids(store.as_ref(), &format!("{prefix}/c.json"), &[6]).await;
    let db = reopen(&storage, &ddl).await;
    db.start().await.expect("restart");
    wait_for_ids(&db, 1..=6).await;
    // Objects are read in key order, so a re-read of `a.json` or `b.json` would come before
    // `c.json`; the barrier of a checkpoint after id 6 settles anything still in flight.
    checkpoint(&db).await;
    let deliveries = deliveries(&db).await;
    db.shutdown().await.expect("shutdown");
    assert_eq!(deliveries, (1..=6).map(|id| (id, 1)).collect());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs S3 on localhost:9000 (tests/docker/iceberg-compose.yml)"]
async fn an_s3_directory_feeds_sql_and_resumes_after_its_checkpoint() {
    reads_each_object_once_across_a_checkpointed_restart(None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs S3 on localhost:9000 (tests/docker/iceberg-compose.yml)"]
async fn a_split_s3_directory_feeds_sql_and_resumes_after_its_checkpoint() {
    reads_each_object_once_across_a_checkpointed_restart(Some(4)).await;
}
