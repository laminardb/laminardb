//! S3 / MinIO integration test for the Delta upsert (collapse → MERGE) path.
//!
//! Lives here rather than in the `delta` module's `#[cfg(test)]` so that
//! `testcontainers`/`bollard` link only into this small integration binary, not
//! the lib test binary — bundling them there overflowed the coverage build's
//! linker (`--all-features` + instrumentation). Ignored by default; needs Docker.
#![cfg(feature = "delta-lake-s3")]
#![allow(clippy::disallowed_types)] // test code: std::HashMap is fine

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{DeliveryGuarantee, SinkConnector};
use laminar_connectors::lakehouse::delta_table_provider::register_delta_table;
use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig, DeltaWriteMode};
use laminar_core::changelog::WEIGHT_COLUMN;

/// A Z-set changelog batch shaped like aggregating-MV output:
/// `[region: Utf8, total: Int64, __weight: Int64]`.
fn zset_changelog(rows: &[(&str, i64, i64)]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(WEIGHT_COLUMN, DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn run_epoch(sink: &mut DeltaLakeSink, epoch: u64, batch: &RecordBatch) {
    sink.begin_epoch(epoch).await.unwrap();
    sink.write_batch(batch).await.unwrap();
    sink.pre_commit(epoch).await.unwrap();
    sink.commit_epoch(epoch).await.unwrap();
}

/// Read the table back via `register_delta_table` (which registers the S3
/// object store with the DataFusion session) and return `(region, total)`.
async fn read_regions(path: &str, storage: HashMap<String, String>) -> Vec<(String, i64)> {
    let ctx = datafusion::prelude::SessionContext::new();
    register_delta_table(&ctx, "t", path, storage)
        .await
        .unwrap();
    let batches = ctx
        .sql("SELECT region, total FROM t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut out = Vec::new();
    for b in &batches {
        // DataFusion may return strings as Utf8View; cast to concrete types.
        let region = arrow_cast::cast(
            b.column(b.schema().index_of("region").unwrap()),
            &DataType::Utf8,
        )
        .unwrap();
        let total = arrow_cast::cast(
            b.column(b.schema().index_of("total").unwrap()),
            &DataType::Int64,
        )
        .unwrap();
        let regions = region.as_any().downcast_ref::<StringArray>().unwrap();
        let totals = total.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((regions.value(i).to_string(), totals.value(i)));
        }
    }
    out.sort();
    out
}

/// The upsert collapse → MERGE path over real object-store I/O (MinIO via
/// Docker), including the multi-update-per-key-in-one-epoch cardinality case.
#[tokio::test]
#[ignore = "requires Docker (MinIO S3)"]
async fn upsert_against_minio_s3_object_store() {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    // MinIO's FS backend treats a directory under /data as a bucket, so we
    // override the entrypoint to pre-create the bucket before the server starts
    // (the official image has no auto-create env).
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
        // Single writer: skip the DynamoDB lock; delta-rs allows this opt-in.
        ("aws_s3_allow_unsafe_rename", "true"),
    ] {
        storage.insert(k.to_string(), v.to_string());
    }
    storage.insert(
        "aws_endpoint".to_string(),
        format!("http://127.0.0.1:{port}"),
    );

    let mut cfg = DeltaLakeSinkConfig::new("s3://warehouse/agg");
    cfg.write_mode = DeltaWriteMode::Upsert;
    cfg.merge_key_columns = vec!["region".to_string()];
    cfg.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    cfg.writer_id = "minio-writer".to_string();
    cfg.storage_options = storage.clone();

    let mut sink = DeltaLakeSink::new(cfg, None);
    sink.open(&ConnectorConfig::new("delta-lake"))
        .await
        .expect("open Delta sink against MinIO");

    run_epoch(
        &mut sink,
        1,
        &zset_changelog(&[("east", 10, 1), ("west", 5, 1)]),
    )
    .await;
    run_epoch(
        &mut sink,
        2,
        &zset_changelog(&[
            ("east", 10, -1),
            ("east", 30, 1),
            ("west", 5, -1),
            ("north", 7, 1),
        ]),
    )
    .await;
    // Multiple updates to one key in a single epoch — the cardinality case —
    // over real object-store I/O.
    run_epoch(
        &mut sink,
        3,
        &zset_changelog(&[
            ("east", 30, -1),
            ("east", 40, 1),
            ("east", 40, -1),
            ("east", 55, 1),
        ]),
    )
    .await;

    assert_eq!(
        read_regions("s3://warehouse/agg", storage).await,
        vec![("east".to_string(), 55), ("north".to_string(), 7)]
    );

    sink.close().await.unwrap();
}
