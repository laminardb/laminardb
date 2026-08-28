use std::sync::Arc;
use std::time::Duration;

use arrow_schema::{DataType, Field, Schema};

use super::*;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn test_connector_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "http://localhost:8181");
    config.set("warehouse", "s3://test/wh");
    config.set("namespace", "test");
    config.set("table.name", "events");
    config
}

fn test_config() -> IcebergSinkConfig {
    IcebergSinkConfig::from_config(&test_connector_config()).unwrap()
}

#[test]
fn new_sink_has_no_schema_or_active_epoch() {
    let mut sink = IcebergSink::new(test_config(), None);
    assert!(sink.schema.is_none());
    #[cfg(feature = "iceberg-core")]
    assert!(sink.active_epoch.get_mut().is_none());
}

#[test]
fn append_contract_is_at_least_once_by_default() {
    let sink = IcebergSink::new(test_config(), None);
    let contract = sink.contract(&test_connector_config()).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert!(sink.as_coordinated_committer().is_none());
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(30));
}

#[test]
fn exactly_once_append_exposes_checkpoint_committer() {
    let mut connector = test_connector_config();
    connector.set("delivery.guarantee", "exactly-once");
    let sink = IcebergSink::new(IcebergSinkConfig::from_config(&connector).unwrap(), None);
    let contract = sink.contract(&connector).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::CheckpointCommittable);
    assert!(sink.as_coordinated_committer().is_some());
    assert!(!contract.is_cluster_exact_delivery_certified());
}

#[test]
fn runtime_identity_must_be_canonical_and_nonzero() {
    let mut sink = IcebergSink::new(test_config(), None);
    let invalid = SinkRuntimeContext {
        deployment_id: "not-a-uuid".into(),
        sink_id: "events".into(),
        participant_id: 1,
    };
    assert!(sink.bind_runtime_context(invalid).is_err());

    let valid = SinkRuntimeContext {
        deployment_id: "018f0000-0000-7000-8000-000000000001".into(),
        sink_id: "events".into(),
        participant_id: 7,
    };
    sink.bind_runtime_context(valid.clone()).unwrap();
    assert_eq!(sink.runtime_context, Some(valid));
}

#[test]
fn local_storage_contract_is_singleton() {
    let sink = IcebergSink::new(test_config(), None);
    let mut config = test_connector_config();
    config.set("warehouse", "file:///tmp/iceberg");
    config.set("storage.type", "fs");
    assert_eq!(
        sink.contract(&config).unwrap().topology,
        SinkTopology::Singleton
    );
}

#[tokio::test]
async fn unsupported_mutation_modes_reject_before_file_creation() {
    for mode in ["merge-on-read", "copy-on-write"] {
        let directory = tempfile::tempdir().unwrap();
        let warehouse = format!(
            "file:///{}",
            directory.path().display().to_string().replace('\\', "/")
        );
        let mut connector = test_connector_config();
        connector.set("catalog.warehouse", warehouse);
        connector.set("storage.type", "fs");
        connector.set("write.mode", mode);
        let mut sink = IcebergSink::new(IcebergSinkConfig::from_config(&connector).unwrap(), None);

        let error = sink
            .open(&connector)
            .await
            .expect_err("unsupported mutation mode must fail before catalog or file I/O");
        assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
        assert_eq!(sink.state, ConnectorState::Created);
        #[cfg(feature = "iceberg-core")]
        assert!(sink.active_epoch.get_mut().is_none());
        assert!(directory.path().read_dir().unwrap().next().is_none());
    }
}

#[test]
fn schema_helper_remains_available_for_open_tests() {
    assert_eq!(test_schema().fields().len(), 1);
}

#[cfg(feature = "iceberg-core")]
#[test]
fn field_ids_are_authoritative_and_nested_ids_are_validated() {
    use std::collections::HashMap;

    const FIELD_ID: &str = parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    let with_id = |name: &str, data_type: DataType, id: &str| {
        Field::new(name, data_type, true)
            .with_metadata(HashMap::from([(FIELD_ID.to_string(), id.to_string())]))
    };
    let source_schema = Schema::new(vec![with_id("old_name", DataType::Int64, "1")]);
    let renamed_target = with_id("new_name", DataType::Int64, "1");
    assert_eq!(source_field_index(&source_schema, &renamed_target), Some(0));

    let source_nested = with_id(
        "payload",
        DataType::Struct(vec![Arc::new(with_id("value", DataType::Int64, "3"))].into()),
        "2",
    );
    let target_nested = with_id(
        "payload",
        DataType::Struct(vec![Arc::new(with_id("value", DataType::Int64, "4"))].into()),
        "2",
    );
    assert!(validate_supplied_field_id(&source_nested, &target_nested)
        .unwrap_err()
        .to_string()
        .contains("field ID 3, expected 4"));
}
