//! Full-engine e2e: a SQL pipeline feeds a coordinated Iceberg sink; a checkpoint
//! seals the epoch and the spawned designated-committer task commits to a real
//! Iceberg REST catalog exactly once. Requires Docker (tests/docker/iceberg-compose.yml):
//!
//!   docker compose -f tests/docker/iceberg-compose.yml up -d --wait
//!   cargo test -p laminar-db --test iceberg_coordinated_e2e --features iceberg -- --ignored
//!
//! Unlike the in-crate coordinator test, this drives the real engine end to end:
//! CREATE SINK detects the coordinated capability, register_sink wires it, start()
//! spawns the committer task, and the catalog is the source of truth.
#![cfg(feature = "iceberg")]

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;
use laminar_connectors::lakehouse::iceberg_io;
use laminar_core::state::{InProcessBackend, NodeId, VnodeRegistry};
use laminar_core::streaming::StreamCheckpointConfig;
use laminar_db::LaminarDbBuilder;

const VNODES: u32 = 4;

/// `(snapshot_count, total_rows)` for the table, or `None` if it doesn't exist yet.
async fn catalog_state(table: &str) -> Option<(usize, usize)> {
    let mut cc = ConnectorConfig::new("iceberg");
    cc.set("catalog.uri", "http://localhost:8181");
    cc.set("warehouse", "s3://warehouse/wh");
    cc.set("storage.type", "s3");
    cc.set("namespace", "laminar_test");
    cc.set("table.name", table);
    cc.set("catalog.property.s3.endpoint", "http://localhost:9000");
    cc.set("catalog.property.s3.access-key-id", "minioadmin");
    cc.set("catalog.property.s3.secret-access-key", "minioadmin");
    cc.set("catalog.property.s3.region", "us-east-1");
    cc.set("catalog.property.s3.path-style-access", "true");
    let cfg = IcebergSinkConfig::from_config(&cc).ok()?;
    let catalog = iceberg_io::build_catalog(&cfg.catalog).await.ok()?;
    let table = iceberg_io::load_table(catalog.as_ref(), "laminar_test", table)
        .await
        .ok()?;
    let snapshots = table.metadata().snapshots().count();
    let rows: usize = iceberg_io::scan_table(&table, None, &[])
        .await
        .ok()?
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    Some((snapshots, rows))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn sql_pipeline_commits_through_designated_committer() {
    if std::net::TcpStream::connect("127.0.0.1:8181").is_err() {
        eprintln!("skipping: Iceberg REST not reachable on 127.0.0.1:8181");
        return;
    }
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("e2e_sql_{unique}");
    let storage = tempfile::tempdir().unwrap();

    let db = LaminarDbBuilder::new()
        .storage_dir(storage.path())
        .checkpoint(StreamCheckpointConfig {
            interval_ms: None, // manual checkpoints only
            ..StreamCheckpointConfig::default()
        })
        .state_backend(Arc::new(InProcessBackend::new(VNODES)))
        .vnode_registry(Arc::new(VnodeRegistry::single_owner(VNODES, NodeId(0))))
        .build()
        .await
        .expect("build db");

    db.execute("CREATE SOURCE events (id BIGINT)")
        .await
        .expect("create source");
    db.execute("CREATE STREAM proj AS SELECT id FROM events")
        .await
        .expect("create stream");
    let ddl_sink = format!(
        "CREATE SINK ice FROM proj WITH (\
            'connector' = 'iceberg', \
            'catalog.uri' = 'http://localhost:8181', \
            'warehouse' = 's3://warehouse/wh', \
            'storage.type' = 's3', \
            'namespace' = 'laminar_test', \
            'table.name' = '{table}', \
            'auto.create' = 'true', \
            'catalog.property.s3.endpoint' = 'http://localhost:9000', \
            'catalog.property.s3.access-key-id' = 'minioadmin', \
            'catalog.property.s3.secret-access-key' = 'minioadmin', \
            'catalog.property.s3.region' = 'us-east-1', \
            'catalog.property.s3.path-style-access' = 'true')"
    );
    db.execute(&ddl_sink).await.expect("create sink");
    db.start().await.expect("start");

    // Push three rows through the SQL pipeline and let them drain to the sink.
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))]).unwrap();
    let source = db.source_untyped("events").unwrap();
    source.push_arrow(batch).unwrap();
    for _ in 0..40 {
        if source.pending() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Checkpoint seals the epoch with the sink's commit descriptor.
    let result = db.checkpoint().await.expect("checkpoint");
    assert!(result.success, "checkpoint failed: {:?}", result.error);

    // The committer task polls (~1s) and lands one coordinated snapshot.
    let mut committed = None;
    for _ in 0..40 {
        if let Some((snaps, rows)) = catalog_state(&table).await {
            if rows >= 3 {
                committed = Some((snaps, rows));
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    db.shutdown().await.ok();

    let (snapshots, rows) = committed.expect("rows committed to catalog within timeout");
    assert_eq!(rows, 3, "every pipeline row must be committed");
    assert_eq!(snapshots, 1, "exactly one coordinated snapshot");
}
