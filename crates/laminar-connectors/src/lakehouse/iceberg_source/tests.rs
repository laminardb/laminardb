use super::*;
use crate::config::ConnectorConfig;

fn connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("catalog.warehouse", "s3://test/wh");
    config.set("namespace", "test");
    config.set("table.name", "events");
    config
}

fn test_source_config() -> IcebergSourceConfig {
    IcebergSourceConfig::from_config(&connector_config()).unwrap()
}

#[test]
fn new_source_has_no_uncommitted_scan_state() {
    let source = IcebergSource::new(test_source_config(), None);
    assert!(source.schema.is_none());
    #[cfg(feature = "iceberg-core")]
    {
        assert!(source.cursor.is_none());
        assert!(source.pending.is_none());
        assert!(source.scan.is_none());
    }
    assert_eq!(
        source.checkpoint.get_metadata("connector_type"),
        Some("iceberg")
    );
}

#[tokio::test]
async fn snapshot_resume_fails_before_catalog_io() {
    let mut source = IcebergSource::new(test_source_config(), None);
    let error = source
        .start(
            SourceStart::new(
                connector_config(),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint: SourceCheckpoint::new(),
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("bounded snapshot mode must not accept append cursors");
    assert!(error.to_string().contains("read.mode=append"));
    assert_eq!(source.state, ConnectorState::Created);
}

#[tokio::test]
async fn changelog_fails_before_catalog_io() {
    let mut config = connector_config();
    config.set("read.mode", "changelog");
    let mut source = IcebergSource::new(IcebergSourceConfig::from_config(&config).unwrap(), None);
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("unsupported changelog must fail before opening the catalog");
    assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
    assert_eq!(source.state, ConnectorState::Created);
}

#[test]
fn contracts_match_typed_read_modes() {
    let source = IcebergSource::new(test_source_config(), None);
    let snapshot = source.contract(&connector_config()).unwrap();
    assert_eq!(snapshot.consistency, SourceConsistency::Ephemeral);
    assert_eq!(snapshot.topology, SourceTopology::Singleton);

    let mut append_config = connector_config();
    append_config.set("read.mode", "append");
    let append = source.contract(&append_config).unwrap();
    assert_eq!(append.consistency, SourceConsistency::Replayable);
    assert_eq!(append.input_mode, SourceInputMode::AppendOnly);
}

#[cfg(feature = "iceberg-core")]
#[tokio::test]
async fn completed_scan_observes_files_and_storage_bytes() {
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    let fixture = create_test_table(false).await;
    let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, Some("read"))]).await;
    let mut source = IcebergSource::new(test_source_config(), None);
    source.table = Some(table);
    source.start_initial_scan().unwrap();

    while source.poll_batch(1).await.unwrap().is_some() {}

    assert_eq!(source.metrics.read_files.get(), 1);
    assert!(source.metrics.read_storage_bytes.get() > 0);
}
