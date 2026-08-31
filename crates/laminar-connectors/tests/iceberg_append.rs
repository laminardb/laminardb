#![cfg(feature = "iceberg-core")]

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use futures_util::StreamExt;
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, CoordinatedCommitPayload, CoordinatedCommitter, DeliveryGuarantee,
    SinkConnector, SinkRuntimeContext, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::lakehouse::iceberg::IcebergSink;
use laminar_connectors::lakehouse::iceberg_config::{IcebergSinkConfig, IcebergSourceConfig};
use laminar_connectors::lakehouse::iceberg_io;
use laminar_connectors::lakehouse::IcebergSource;
use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
use laminar_core::checkpoint::CheckpointAttempt;

const DEPLOYMENT_ID: &str = "018f0000-0000-7000-8000-000000000001";
const SINK_ID: &str = "iceberg-integration";

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn require_catalog() {
    let address = std::net::SocketAddr::from(([127, 0, 0, 1], 8181));
    std::net::TcpStream::connect_timeout(&address, Duration::from_secs(2))
        .expect("Iceberg REST catalog must be reachable on 127.0.0.1:8181");
}

fn config(table: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://warehouse/wh");
    config.set("storage.type", "s3");
    config.set("namespace", "laminar_test");
    config.set("table.name", table);
    config.set("auto.create", "true");
    config.set("storage.endpoint", "http://localhost:9000");
    config.set("storage.region", "us-east-1");
    config.set("storage.path_style", "true");
    config.set("storage.property.s3.access-key-id", "minioadmin");
    config.set("storage.property.s3.secret-access-key", "minioadmin");
    config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
    config
}

async fn open_sink(table: &str) -> IcebergSink {
    let connector_config = config(table);
    let mut sink = IcebergSink::new(
        IcebergSinkConfig::from_config(&connector_config).unwrap(),
        None,
    );
    sink.open(&connector_config).await.unwrap();
    sink
}

async fn open_coordinated_sink(table: &str) -> IcebergSink {
    open_coordinated_participant(table, 1).await
}

async fn open_coordinated_participant(table: &str, participant_id: u64) -> IcebergSink {
    let mut connector_config = config(table);
    connector_config.set("delivery.guarantee", "exactly-once");
    let mut sink = IcebergSink::new(
        IcebergSinkConfig::from_config(&connector_config).unwrap(),
        None,
    );
    sink.bind_runtime_context(SinkRuntimeContext {
        deployment_id: DEPLOYMENT_ID.into(),
        sink_id: SINK_ID.into(),
        participant_id,
    })
    .unwrap();
    sink.open(&connector_config).await.unwrap();
    sink
}

async fn admit_and_begin_epoch(sink: &mut IcebergSink, epoch: u64) {
    assert!(
        sink.checkpoint_artifact_intent(epoch)
            .await
            .unwrap()
            .is_some(),
        "coordinated Iceberg epochs require durable artifact evidence"
    );
    sink.begin_epoch(epoch).await.unwrap();
}

fn commit_namespace() -> CoordinatedCommitNamespace {
    CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT_ID, SINK_ID).unwrap()
}

fn commit_batch(
    checkpoint_id: u64,
    expected_predecessor: CoordinatedCommitCursor,
    fencing_token: u64,
    descriptor: Vec<u8>,
) -> CoordinatedCommitBatch {
    commit_participant_batch(
        checkpoint_id,
        expected_predecessor,
        fencing_token,
        vec![(1, descriptor)],
    )
}

fn empty_commit_batch(
    checkpoint_id: u64,
    expected_predecessor: CoordinatedCommitCursor,
    fencing_token: u64,
) -> CoordinatedCommitBatch {
    let target = CheckpointAttempt::canonical(checkpoint_id);
    CoordinatedCommitBatch {
        namespace: commit_namespace(),
        expected_predecessor,
        fencing_token,
        target,
        entries: vec![CoordinatedCommitPayload {
            attempt: target,
            participant_id: 1,
            payload: None,
        }],
    }
}

fn commit_participant_batch(
    checkpoint_id: u64,
    expected_predecessor: CoordinatedCommitCursor,
    fencing_token: u64,
    descriptors: Vec<(u64, Vec<u8>)>,
) -> CoordinatedCommitBatch {
    let target = CheckpointAttempt::canonical(checkpoint_id);
    CoordinatedCommitBatch {
        namespace: commit_namespace(),
        expected_predecessor,
        fencing_token,
        target,
        entries: descriptors
            .into_iter()
            .map(|(participant_id, descriptor)| CoordinatedCommitPayload {
                attempt: target,
                participant_id,
                payload: Some(descriptor),
            })
            .collect(),
    }
}

fn commit_context() -> CoordinatedCommitContext {
    CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(30))
}

async fn append_ids(table: &str, ids: Vec<i64>) {
    let mut sink = open_sink(table).await;
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    sink.write_batch(&batch).await.unwrap();
    sink.flush().await.unwrap();
    sink.close().await.unwrap();
}

fn source_config(table: &str) -> ConnectorConfig {
    let mut config = config(table);
    config.set("read.mode", "append");
    config.set("read.bootstrap", "initial");
    config.set("poll.interval", "1ms");
    config
}

async fn start_source(table: &str, position: SourcePosition) -> IcebergSource {
    let config = source_config(table);
    let mut source = IcebergSource::new(IcebergSourceConfig::from_config(&config).unwrap(), None);
    source
        .start(SourceStart::new(config, position, DeliveryGuarantee::AtLeastOnce).unwrap())
        .await
        .unwrap();
    source
}

async fn drain_ids(source: &mut IcebergSource) -> Vec<i64> {
    let mut ids = Vec::new();
    while let Some(batch) = source.poll_batch(4).await.unwrap() {
        let values = batch
            .records
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        ids.extend(values.values());
    }
    ids
}

async fn inspect(table: &str) -> (usize, usize) {
    let config = IcebergSinkConfig::from_config(&config(table)).unwrap();
    let catalog = iceberg_io::build_catalog(&config.catalog, &config.storage)
        .await
        .unwrap();
    let table = iceberg_io::load_table(
        catalog.as_ref(),
        &config.catalog.namespace,
        &config.catalog.table_name,
    )
    .await
    .unwrap();
    let snapshots = table.metadata().snapshots().count();
    let mut stream = table.scan().build().unwrap().to_arrow().await.unwrap();
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    (snapshots, rows)
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn concurrent_appends_are_visible_from_multiple_writers() {
    require_catalog();
    let table = format!("events_{}", uuid::Uuid::new_v4().simple());
    open_sink(&table).await.close().await.unwrap();

    tokio::join!(
        append_ids(&table, (0..10).collect()),
        append_ids(&table, (10..20).collect()),
        append_ids(&table, (20..30).collect()),
    );

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 3);
    assert_eq!(rows, 30);
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn append_source_bootstrap_and_resume_do_not_duplicate_snapshots() {
    require_catalog();
    let table = format!("source_{}", uuid::Uuid::new_v4().simple());
    append_ids(&table, (0..6).collect()).await;

    let mut initial = start_source(&table, SourcePosition::Initial).await;
    assert_eq!(drain_ids(&mut initial).await, (0..6).collect::<Vec<_>>());
    let bootstrap_cursor = initial.checkpoint();
    initial.close().await.unwrap();

    append_ids(&table, (10..14).collect()).await;
    append_ids(&table, (20..24).collect()).await;
    let mut resumed = start_source(
        &table,
        SourcePosition::Resume {
            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(2),
            checkpoint: bootstrap_cursor,
        },
    )
    .await;
    assert_eq!(
        drain_ids(&mut resumed).await,
        vec![10, 11, 12, 13, 20, 21, 22, 23]
    );
    let resumed_cursor = resumed.checkpoint();
    resumed.close().await.unwrap();

    let mut replay = start_source(
        &table,
        SourcePosition::Resume {
            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(3),
            checkpoint: resumed_cursor,
        },
    )
    .await;
    assert!(drain_ids(&mut replay).await.is_empty());
    replay.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn coordinated_checkpoint_restart_replay_soak() {
    require_catalog();
    let table = format!("coordinated_{}", uuid::Uuid::new_v4().simple());
    open_sink(&table).await.close().await.unwrap();
    let mut predecessor = CoordinatedCommitCursor {
        checkpoint_id: 0,
        fencing_token: 0,
    };

    for checkpoint_id in 1..=8 {
        let fencing_token = checkpoint_id + 100;
        let mut writer = open_coordinated_sink(&table).await;
        admit_and_begin_epoch(&mut writer, checkpoint_id).await;
        writer
            .write_batch(
                &RecordBatch::try_new(
                    schema(),
                    vec![Arc::new(Int64Array::from(vec![
                        checkpoint_id as i64,
                        checkpoint_id as i64 + 1_000,
                    ]))],
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let descriptor = writer
            .pre_commit(checkpoint_id)
            .await
            .unwrap()
            .expect("non-empty checkpoint must produce a descriptor");
        let batch = commit_batch(checkpoint_id, predecessor, fencing_token, descriptor);
        writer
            .commit_aggregated(batch.clone(), commit_context())
            .await
            .unwrap();
        writer.close().await.unwrap();

        let mut restarted = open_coordinated_sink(&table).await;
        restarted
            .commit_aggregated(batch, commit_context())
            .await
            .unwrap();
        let committed = CoordinatedCommitCursor {
            checkpoint_id,
            fencing_token,
        };
        assert_eq!(
            restarted
                .committed_cursor(&commit_namespace())
                .await
                .unwrap(),
            Some(committed)
        );
        restarted.close().await.unwrap();
        predecessor = committed;
    }

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 8);
    assert_eq!(rows, 16);
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn coordinated_empty_checkpoint_replay_is_one_fenced_snapshot() {
    require_catalog();
    let table = format!("empty_{}", uuid::Uuid::new_v4().simple());
    open_sink(&table).await.close().await.unwrap();
    let batch = empty_commit_batch(
        1,
        CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        101,
    );

    let mut writer = open_coordinated_sink(&table).await;
    admit_and_begin_epoch(&mut writer, 1).await;
    assert!(writer.pre_commit(1).await.unwrap().is_none());
    writer
        .commit_aggregated(batch.clone(), commit_context())
        .await
        .unwrap();
    writer.close().await.unwrap();

    let mut restarted = open_coordinated_sink(&table).await;
    restarted
        .commit_aggregated(batch, commit_context())
        .await
        .unwrap();
    assert_eq!(
        restarted
            .committed_cursor(&commit_namespace())
            .await
            .unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 1,
            fencing_token: 101,
        })
    );
    restarted.close().await.unwrap();

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 1);
    assert_eq!(rows, 0);
}

#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn coordinated_multi_participant_checkpoint_is_one_snapshot() {
    require_catalog();
    let table = format!("cluster_{}", uuid::Uuid::new_v4().simple());
    open_sink(&table).await.close().await.unwrap();
    let mut first = open_coordinated_participant(&table, 1).await;
    let mut second = open_coordinated_participant(&table, 2).await;
    admit_and_begin_epoch(&mut first, 1).await;
    admit_and_begin_epoch(&mut second, 1).await;
    first
        .write_batch(
            &RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap(),
        )
        .await
        .unwrap();
    second
        .write_batch(
            &RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vec![3, 4]))]).unwrap(),
        )
        .await
        .unwrap();
    let first_descriptor = first.pre_commit(1).await.unwrap().unwrap();
    let second_descriptor = second.pre_commit(1).await.unwrap().unwrap();
    let batch = commit_participant_batch(
        1,
        CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        101,
        vec![(1, first_descriptor), (2, second_descriptor)],
    );
    first
        .commit_aggregated(batch.clone(), commit_context())
        .await
        .unwrap();
    second
        .commit_aggregated(batch, commit_context())
        .await
        .unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();

    let (snapshots, rows) = inspect(&table).await;
    assert_eq!(snapshots, 1);
    assert_eq!(rows, 4);
}
