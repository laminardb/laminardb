//! Delta designated-committer integration test (append mode, MinIO via Docker).
//!
//! N writers each write Parquet and hand `Add` descriptors to the committer,
//! which commits them as ONE Delta transaction; re-commit is idempotent.
//!
//!   cargo test -p laminar-connectors --test delta_coordinated_commit \
//!     --features delta-lake-s3 -- --ignored
#![cfg(feature = "delta-lake-s3")]
#![allow(clippy::disallowed_types)] // test code: std::HashMap is fine

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{CoordinatedCommitter, DeliveryGuarantee, SinkConnector};
use laminar_connectors::lakehouse::delta_table_provider::register_delta_table;
use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn batch(ids: std::ops::Range<i64>) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![Arc::new(Int64Array::from(ids.collect::<Vec<_>>()))],
    )
    .unwrap()
}

async fn open_writer(
    path: &str,
    writer_id: &str,
    storage: &HashMap<String, String>,
) -> DeltaLakeSink {
    let mut cfg = DeltaLakeSinkConfig::new(path);
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce; // append is the default mode
    cfg.writer_id = writer_id.to_string();
    cfg.storage_options = storage.clone();
    let mut sink = DeltaLakeSink::new(cfg, None);
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .expect("open delta sink against MinIO");
    sink
}

async fn count_rows(path: &str, storage: HashMap<String, String>) -> usize {
    let ctx = datafusion::prelude::SessionContext::new();
    register_delta_table(&ctx, "t", path, storage)
        .await
        .unwrap();
    ctx.sql("SELECT id FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

#[tokio::test]
#[ignore = "requires Docker (MinIO S3)"]
async fn designated_committer_one_commit_from_many_writers() {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let container = GenericImage::new("minio/minio", "latest")
        .with_exposed_port(9000.tcp())
        .with_wait_for(WaitFor::message_on_stderr("API:"))
        .with_entrypoint("sh")
        .with_cmd(["-c", "mkdir -p /data/warehouse && minio server /data"])
        .with_env_var("MINIO_ROOT_USER", "minioadmin")
        .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
        .start()
        .await
        .expect("start MinIO container");
    let port = container
        .get_host_port_ipv4(9000.tcp())
        .await
        .expect("MinIO host port");

    let mut storage = HashMap::new();
    for (k, v) in [
        ("aws_access_key_id", "minioadmin"),
        ("aws_secret_access_key", "minioadmin"),
        ("aws_region", "us-east-1"),
        ("aws_allow_http", "true"),
        ("aws_s3_allow_unsafe_rename", "true"),
    ] {
        storage.insert(k.to_string(), v.to_string());
    }
    storage.insert(
        "aws_endpoint".to_string(),
        format!("http://127.0.0.1:{port}"),
    );

    let path = "s3://warehouse/events";

    // Pre-create the empty table so each writer's RecordBatchWriter has a schema.
    laminar_connectors::lakehouse::delta_io::open_or_create_table(
        path,
        storage.clone(),
        Some(&schema()),
    )
    .await
    .expect("create delta table");

    // Three writers, each writing distinct rows; collect their descriptors.
    let mut descriptors = Vec::new();
    for w in 0..3i64 {
        let mut writer = open_writer(path, &format!("w{w}"), &storage).await;
        writer.begin_epoch(1).await.unwrap();
        writer
            .write_batch(&batch(w * 10..w * 10 + 10))
            .await
            .unwrap();
        let descriptor = writer
            .pre_commit(1)
            .await
            .unwrap()
            .expect("coordinated sink yields a descriptor");
        descriptors.push(descriptor);
        writer.close().await.unwrap();
    }

    // One designated commit for all writers' files.
    let committer = open_writer(path, "committer", &storage).await;
    committer
        .commit_aggregated(1, descriptors.clone())
        .await
        .unwrap();
    assert_eq!(count_rows(path, storage.clone()).await, 30);

    // Re-commit the same epoch — idempotent, no duplicate rows.
    committer.commit_aggregated(1, descriptors).await.unwrap();
    assert_eq!(count_rows(path, storage).await, 30);
}
