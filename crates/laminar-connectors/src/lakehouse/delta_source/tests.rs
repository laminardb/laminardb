use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn test_config() -> DeltaSourceConfig {
    DeltaSourceConfig::new("/tmp/delta_source_test")
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

#[allow(clippy::cast_precision_loss)]
fn test_batch(n: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<&str> = (0..n).map(|_| "test").collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

#[test]
fn test_new_defaults() {
    let source = DeltaSource::new(test_config(), None);
    assert_eq!(source.state(), ConnectorState::Created);
    assert_eq!(source.current_version(), -1);
    assert!(source.schema.is_none());
}

#[test]
fn cdf_contract_is_full_changelog() {
    let source = DeltaSource::new(test_config(), None);
    assert_eq!(
        source
            .contract(&ConnectorConfig::new("delta-lake"))
            .unwrap()
            .input_mode,
        SourceInputMode::FullChangelog
    );

    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", "/tmp/delta_source_test");
    config.set("read.mode", "snapshot");
    let error = source.contract(&config).unwrap_err();
    assert!(error.to_string().contains("read.mode"));
}

#[tokio::test]
async fn start_rejects_removed_option_before_opening_table() {
    let mut source = DeltaSource::new(test_config(), None);
    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", "/tmp/delta_source_test");
    config.set("cdf.enabled", "true");
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cdf.enabled"));
    assert_eq!(source.state(), ConnectorState::Created);
}

#[cfg(all(
    feature = "delta-lake",
    not(feature = "delta-lake-glue"),
    not(feature = "delta-lake-unity")
))]
#[tokio::test]
async fn start_routes_catalog_locations_through_the_resolver() {
    use super::super::delta_config::DeltaCatalogType;

    let mut glue_config = test_config();
    glue_config.catalog_type = DeltaCatalogType::Glue;
    glue_config.catalog_database = Some("analytics".into());
    let mut glue_source = DeltaSource::new(glue_config, None);
    let glue_error = glue_source
        .start(
            SourceStart::new(
                ConnectorConfig::new("delta-lake"),
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(glue_error.to_string().contains("delta-lake-glue"));

    let mut unity_config = DeltaSourceConfig::new("uc://catalog.schema.events");
    unity_config.catalog_type = DeltaCatalogType::Unity {
        workspace_url: "https://workspace.example".into(),
        access_token: "test-token".into(),
    };
    unity_config.catalog_name = Some("catalog".into());
    unity_config.catalog_schema = Some("schema".into());
    let mut unity_source = DeltaSource::new(unity_config, None);
    let unity_error = unity_source
        .start(
            SourceStart::new(
                ConnectorConfig::new("delta-lake"),
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(unity_error.to_string().contains("delta-lake-unity"));
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn reopen_uses_the_location_resolved_at_start() {
    use std::collections::HashMap;

    let temp_dir = tempfile::TempDir::new().unwrap();
    let table_path = temp_dir.path().to_string_lossy().into_owned();
    super::super::delta_io::open_or_create_table(&table_path, HashMap::new(), Some(&test_schema()))
        .await
        .unwrap();

    let mut source = DeltaSource::new(DeltaSourceConfig::new(&table_path), None);
    source
        .start(
            SourceStart::new(
                ConnectorConfig::new("delta-lake"),
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(source.resolved_table_path, table_path);

    source.table = None;
    source.config.table_path = temp_dir
        .path()
        .join("unresolved-location")
        .to_string_lossy()
        .into_owned();
    source.reopen_table().await.unwrap();
    assert_eq!(source.table.as_ref().and_then(DeltaTable::version), Some(0));
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn cdf_scan_retains_the_live_table_handle() {
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
    use deltalake::TableProperty;

    let schema = test_schema();
    let delta_schema: deltalake::kernel::StructType = schema.as_ref().try_into_kernel().unwrap();
    let table = DeltaTable::new_in_memory()
        .create()
        .with_columns(delta_schema.fields().cloned())
        .with_configuration_property(TableProperty::EnableChangeDataFeed, Some("true"))
        .await
        .unwrap()
        .write(vec![test_batch(2)])
        .await
        .unwrap();
    assert_eq!(table.version(), Some(1));

    let mut source = DeltaSource::new(test_config(), None);
    source.state = ConnectorState::Running;
    source.schema = Some(super::super::delta_io::get_table_schema(&table).unwrap());
    source.current_version = 0;
    source.known_latest_version = 1;
    source.table = Some(table);

    let batch = source.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.records.num_rows(), 2);
    assert_eq!(source.table.as_ref().and_then(DeltaTable::version), Some(1));
}

#[cfg(feature = "delta-lake")]
#[test]
fn starting_version_is_the_first_version_read() {
    assert_eq!(initial_current_version(Some(5), 9), 4);
    assert_eq!(initial_current_version(None, 9), 9);
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn multi_batch_commit_drains_before_advancing_the_cursor() {
    let mut source = DeltaSource::new(test_config(), None);
    source.state = ConnectorState::Running;
    source.current_version = 6;
    source.inflight_version = Some(7);
    source.pending_batches.push_back(test_batch(1));
    source.pending_batches.push_back(test_batch(2));
    source.pending_batches.push_back(test_batch(3));

    assert!(source.checkpoint_ready().unwrap());

    assert_eq!(
        source
            .poll_batch(100)
            .await
            .unwrap()
            .unwrap()
            .records
            .num_rows(),
        1
    );
    assert_eq!(source.current_version(), 6);
    assert_eq!(source.checkpoint().get_offset("delta_version"), Some("6"));

    assert_eq!(
        source
            .poll_batch(100)
            .await
            .unwrap()
            .unwrap()
            .records
            .num_rows(),
        2
    );
    assert_eq!(source.current_version(), 6);

    assert_eq!(
        source
            .poll_batch(100)
            .await
            .unwrap()
            .unwrap()
            .records
            .num_rows(),
        3
    );
    assert_eq!(source.current_version(), 7);
    assert!(source.inflight_version.is_none());
    assert!(source.pending_batches.is_empty());
    assert_eq!(source.checkpoint().get_offset("delta_version"), Some("7"));
}

#[test]
fn test_checkpoint_roundtrip() {
    let mut source = DeltaSource::new(test_config(), None);
    source.current_version = 42;

    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("delta_version"), Some("42"));
}

#[tokio::test]
async fn resume_fails_before_opening_the_ephemeral_source() {
    let mut source = DeltaSource::new(test_config(), None);
    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("delta-lake"),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint: SourceCheckpoint::new(),
                },
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .expect_err("ephemeral Delta source must reject recovery");
    assert!(error.to_string().contains("ephemeral"));
    assert_eq!(source.state(), ConnectorState::Created);
}

#[test]
fn test_schema_empty_when_none() {
    let source = DeltaSource::new(test_config(), None);
    let schema = source.schema();
    assert_eq!(schema.fields().len(), 0);
}

#[tokio::test]
async fn test_poll_not_running() {
    let mut source = DeltaSource::new(test_config(), None);
    // state is Created, not Running
    let result = source.poll_batch(100).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_poll_returns_buffered_batches() {
    let mut source = DeltaSource::new(test_config(), None);
    source.state = ConnectorState::Running;

    // Manually buffer some batches.
    source.pending_batches.push_back(test_batch(5));
    source.pending_batches.push_back(test_batch(3));

    let batch1 = source.poll_batch(100).await.unwrap();
    assert!(batch1.is_some());
    assert_eq!(batch1.unwrap().records.num_rows(), 5);

    let batch2 = source.poll_batch(100).await.unwrap();
    assert!(batch2.is_some());
    assert_eq!(batch2.unwrap().records.num_rows(), 3);

    assert_eq!(source.records_read, 8);
}

#[test]
fn test_poll_interval_is_stored() {
    let mut config = test_config();
    config.poll_interval = std::time::Duration::from_millis(500);
    let source = DeltaSource::new(config, None);
    assert_eq!(
        source.config().poll_interval,
        std::time::Duration::from_millis(500)
    );
}

#[test]
fn test_debug_output() {
    let source = DeltaSource::new(test_config(), None);
    let debug = format!("{source:?}");
    assert!(debug.contains("DeltaSource"));
    assert!(debug.contains("table_path: \"<configured>\""));
    assert!(!debug.contains("/tmp/delta_source_test"));
}

#[tokio::test]
async fn test_close() {
    let mut source = DeltaSource::new(test_config(), None);
    source.state = ConnectorState::Running;
    source.pending_batches.push_back(test_batch(5));

    source.close().await.unwrap();
    assert_eq!(source.state(), ConnectorState::Closed);
    assert!(source.pending_batches.is_empty());
}

/// D020: Source `start()` must error without delta-lake feature.
#[cfg(not(feature = "delta-lake"))]
#[tokio::test]
async fn test_open_requires_feature() {
    let mut source = DeltaSource::new(test_config(), None);
    let connector_config = crate::config::ConnectorConfig::new("delta-lake");
    let result = source
        .start(
            SourceStart::new(
                connector_config,
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("delta-lake"), "error: {err}");
}
