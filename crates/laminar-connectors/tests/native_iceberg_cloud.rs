//! Provider-native Iceberg REST catalog and warehouse integration.

#![cfg(all(feature = "iceberg-core", feature = "iceberg-catalog-rest"))]

mod cloud_test_support;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
use laminar_connectors::connector::{
    DeliveryGuarantee, SinkConnector, SourceConnector, SourcePosition, SourceStart,
};
use laminar_connectors::lakehouse::iceberg::IcebergSink;
use laminar_connectors::lakehouse::iceberg_config::{IcebergSinkConfig, IcebergSourceConfig};
use laminar_connectors::lakehouse::{iceberg_io, IcebergSource};

use cloud_test_support::{DependencyVersions, EvidenceOutcome, NativeCloudContext};

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn connector_config(context: &NativeCloudContext, table: &str) -> Result<ConnectorConfig, ()> {
    let catalog_uri = std::env::var("LAMINAR_ICEBERG_CATALOG_URI")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(())?;
    let namespace = std::env::var("LAMINAR_ICEBERG_NAMESPACE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "laminar_native_tests".into());
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", catalog_uri);
    config.set("warehouse", &context.test_url);
    config.set(
        "storage.type",
        match context.provider_id {
            "aws" => "s3",
            "azure" => "azure",
            "gcs" => "gcs",
            _ => return Err(()),
        },
    );
    config.set("namespace", namespace);
    config.set("table.name", table);
    config.set("auto.create", "true");
    config.set("storage.connect_timeout", "30s");
    config.set("storage.request_timeout", "60s");
    if context.provider_id == "aws" {
        if let Ok(region) = std::env::var("AWS_REGION") {
            if !region.trim().is_empty() {
                config.set("storage.region", region);
            }
        }
    }
    config.set("_arrow_schema", encode_arrow_schema_ipc(&schema()));
    Ok(config)
}

async fn append(config: &ConnectorConfig, ids: Vec<i64>) -> Result<(), ()> {
    let mut sink = IcebergSink::new(
        IcebergSinkConfig::from_config(config).map_err(|_| ())?,
        None,
    );
    sink.open(config).await.map_err(|_| ())?;
    let batch = RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
    sink.write_batch(&batch).await.map_err(|_| ())?;
    sink.flush().await.map_err(|_| ())?;
    sink.close().await.map_err(|_| ())
}

async fn read_ids(config: &ConnectorConfig) -> Result<Vec<i64>, ()> {
    let mut source_config = config.clone();
    source_config.set("read.mode", "append");
    source_config.set("read.bootstrap", "initial");
    source_config.set("poll.interval", "1ms");
    let mut source = IcebergSource::new(
        IcebergSourceConfig::from_config(&source_config).map_err(|_| ())?,
        None,
    );
    source
        .start(
            SourceStart::new(
                source_config,
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .map_err(|_| ())?,
        )
        .await
        .map_err(|_| ())?;
    let mut ids = Vec::new();
    while let Some(batch) = source.poll_batch(64).await.map_err(|_| ())? {
        let values = batch
            .records
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(())?;
        ids.extend(values.values());
    }
    source.close().await.map_err(|_| ())?;
    ids.sort_unstable();
    Ok(ids)
}

async fn cleanup(config: &ConnectorConfig) -> Result<(), ()> {
    tokio::time::timeout(Duration::from_secs(120), cleanup_inner(config))
        .await
        .map_err(|_| ())?
}

async fn cleanup_inner(config: &ConnectorConfig) -> Result<(), ()> {
    let parsed = IcebergSinkConfig::from_config(config).map_err(|_| ())?;
    let catalog = iceberg_io::build_catalog(&parsed.catalog, &parsed.storage)
        .await
        .map_err(|_| ())?;
    let namespace = iceberg::NamespaceIdent::from_strs(
        parsed
            .catalog
            .namespace
            .split('.')
            .map(str::to_owned)
            .collect::<Vec<_>>(),
    )
    .map_err(|_| ())?;
    let table = iceberg::TableIdent::new(namespace, parsed.catalog.table_name);
    catalog.purge_table(&table).await.map_err(|_| ())
}

struct RunResult {
    concurrent_retry_count: u64,
}

async fn run(config: &ConnectorConfig) -> Result<RunResult, ()> {
    append(config, vec![0, 1, 2]).await?;
    if read_ids(config).await? != vec![0, 1, 2] {
        return Err(());
    }
    append(config, vec![3, 4, 5]).await?;

    let (left, right) = tokio::join!(append(config, vec![6]), append(config, vec![7]));
    let mut retries = 0;
    if left.is_err() {
        retries += 1;
        append(config, vec![6]).await?;
    }
    if right.is_err() {
        retries += 1;
        append(config, vec![7]).await?;
    }
    if read_ids(config).await? != (0..=7).collect::<Vec<_>>() {
        return Err(());
    }
    // Rebuild both catalog and source to prove metadata and data-file resolution after restart.
    if read_ids(config).await? != (0..=7).collect::<Vec<_>>() {
        return Err(());
    }
    Ok(RunResult {
        concurrent_retry_count: retries,
    })
}

#[tokio::test]
#[ignore = "requires an explicit native-cloud marker and pre-provisioned REST catalog"]
// The arms differ in provider-specific builds; all-features makes each `cfg!` true.
#[allow(clippy::match_like_matches_macro)]
async fn native_iceberg_append_scan_restart() {
    let feature_enabled = match std::env::var("LAMINAR_NATIVE_CLOUD_PROVIDER").as_deref() {
        Ok("aws") => cfg!(feature = "iceberg-storage-s3"),
        Ok("azure") => cfg!(feature = "iceberg-storage-azure"),
        Ok("gcs") => cfg!(feature = "iceberg-storage-gcs"),
        _ => false,
    };
    let context = NativeCloudContext::load(
        "iceberg-native-integration",
        "native_iceberg_append_scan_restart",
        feature_enabled,
    )
    .unwrap_or_else(|reason| panic!("required native Iceberg setup is incomplete: {reason}"));
    let Some(context) = context else {
        return;
    };
    let table = format!("native_{}", uuid::Uuid::new_v4().simple());
    let config = connector_config(&context, &table)
        .unwrap_or_else(|_| panic!("LAMINAR_ICEBERG_CATALOG_URI is required"));
    let result = tokio::time::timeout(Duration::from_secs(600), run(&config))
        .await
        .map_err(|_| ())
        .and_then(|result| result);
    let cleanup = cleanup(&config).await;
    let passed = result.is_ok() && cleanup.is_ok();
    let retry_count = result
        .as_ref()
        .map_or(0, |result| result.concurrent_retry_count);
    let capabilities = serde_json::json!({
        "rest_catalog_load": result.is_ok(),
        "sink_append": result.is_ok(),
        "source_scan": result.is_ok(),
        "metadata_reload": result.is_ok(),
        "fresh_catalog_and_client": result.is_ok(),
        "concurrent_writers_completed": result.is_ok(),
        "concurrent_commit_retry_count": retry_count,
        "fault_soak": false,
        "experimental": context.provider_id == "azure"
    });
    let evidence = context.evidence(
        DependencyVersions {
            deltalake: None,
            iceberg: Some("0.10.1"),
            opendal: Some("0.57.0"),
        },
        capabilities,
        EvidenceOutcome {
            iterations: 4,
            process_kill_count: 0,
            recovery_bound_ms: 60_000,
            conditional_create: None,
            stale_cas: None,
            restart: result.is_ok(),
            delivery_contract: "iceberg-append-at-least-once-integration",
            records_produced: 8,
            records_committed: u64::from(result.is_ok()) * 8,
            records_recovered: u64::from(result.is_ok()) * 8,
            duplicates: result.is_ok().then_some(0),
            losses: result.is_ok().then_some(0),
            passed,
            cleanup_result: if cleanup.is_ok() { "passed" } else { "failed" }.into(),
            failure: (!passed).then_some("native Iceberg integration or cleanup failed"),
        },
    );
    context
        .write_evidence(&evidence)
        .expect("native Iceberg evidence artifact must be written");
    assert!(passed, "native Iceberg integration failed");
}
