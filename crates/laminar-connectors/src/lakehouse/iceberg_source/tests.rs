use super::*;
use crate::config::ConnectorConfig;

fn test_source_config() -> IcebergSourceConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://test/wh");
    config.set("namespace", "test");
    config.set("table.name", "dim_customers");
    IcebergSourceConfig::from_config(&config).unwrap()
}

#[test]
fn test_new_source() {
    let source = IcebergSource::new(test_source_config(), None);
    assert!(source.schema.is_none());
    assert!(source.last_snapshot_id.is_none());
    assert!(source.buffer.is_empty());
}

#[test]
fn test_checkpoint_round_trip() {
    let mut source = IcebergSource::new(test_source_config(), None);
    source.last_snapshot_id = Some(42);

    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("snapshot_id"), Some("42"));
    assert_eq!(cp.get_metadata("connector_type"), Some("iceberg"));
}

#[tokio::test]
async fn resume_fails_before_opening_the_ephemeral_source() {
    let mut source = IcebergSource::new(test_source_config(), None);
    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("iceberg"),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint: SourceCheckpoint::new(),
                },
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .expect_err("ephemeral Iceberg source must reject recovery");
    assert!(error.to_string().contains("ephemeral"));
    assert_eq!(source.state, ConnectorState::Created);
}

#[test]
fn source_contract_requires_pinned_snapshot() {
    let unpinned = IcebergSource::new(test_source_config(), None);
    let error = unpinned
        .contract(&ConnectorConfig::new("iceberg"))
        .unwrap_err();
    assert!(error.to_string().contains("snapshot.id"));

    let mut config = test_source_config();
    config.snapshot_id = Some(42);
    let source = IcebergSource::new(config, None);
    let contract = source.contract(&ConnectorConfig::new("iceberg")).unwrap();
    assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
    assert_eq!(contract.topology, SourceTopology::Singleton);
    assert_eq!(contract.input_mode, SourceInputMode::AppendOnly);
}
