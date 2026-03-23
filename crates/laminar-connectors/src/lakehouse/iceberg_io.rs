//! Feature-gated I/O operations for Apache Iceberg.
//!
//! Contains catalog construction, table loading, scanning, and writing
//! functions. All code requires the `iceberg` feature.
#![allow(clippy::disallowed_types)] // cold path: lakehouse I/O
#![cfg(feature = "iceberg")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tokio_stream::StreamExt;

use super::iceberg_config::{IcebergCatalogConfig, IcebergCatalogType};
use crate::error::ConnectorError;

/// Selects the correct `OpenDalStorageFactory` based on the warehouse URL scheme.
fn storage_factory_for_warehouse(warehouse: &str) -> Arc<dyn iceberg::io::StorageFactory> {
    if warehouse.starts_with("s3://") || warehouse.starts_with("s3a://") {
        // Note: configured_scheme must be the bare scheme name (e.g. "s3"),
        // NOT "s3://". The iceberg-storage-opendal crate's create_operator()
        // builds the prefix via `format!("{}://{}/", configured_scheme, bucket)`,
        // so including "://" here would produce "s3://://bucket/" — an invalid URL.
        let scheme = if warehouse.starts_with("s3a://") {
            "s3a".to_string()
        } else {
            "s3".to_string()
        };
        Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: scheme,
            customized_credential_load: None,
        })
    } else {
        Arc::new(OpenDalStorageFactory::Fs)
    }
}

/// Builds a REST catalog from configuration.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if catalog initialization fails.
pub async fn build_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, ConnectorError> {
    match config.catalog_type {
        IcebergCatalogType::Rest => build_rest_catalog(config).await,
    }
}

async fn build_rest_catalog(
    config: &IcebergCatalogConfig,
) -> Result<Arc<dyn Catalog>, ConnectorError> {
    let storage_factory = storage_factory_for_warehouse(&config.warehouse);

    let mut props = HashMap::new();
    props.insert("uri".to_string(), config.catalog_uri.clone());
    props.insert("warehouse".to_string(), config.warehouse.clone());

    for (k, v) in &config.properties {
        props.insert(k.clone(), v.clone());
    }

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(storage_factory)
        .load("laminardb", props)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("iceberg catalog: {e}")))?;

    Ok(Arc::new(catalog))
}

/// Loads an Iceberg table from the catalog.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the table cannot be loaded.
pub async fn load_table(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
) -> Result<Table, ConnectorError> {
    let ns = iceberg::NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| ConnectorError::ConfigurationError(format!("invalid namespace: {e}")))?;

    let ident = TableIdent::new(ns, table_name.to_string());

    catalog
        .load_table(&ident)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("load table '{table_name}': {e}")))
}

/// Scans a table and returns all record batches for the current snapshot.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` on scan failure.
pub async fn scan_table(
    table: &Table,
    snapshot_id: Option<i64>,
    select_columns: &[String],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let mut scan_builder = table.scan();

    if let Some(sid) = snapshot_id {
        scan_builder = scan_builder.snapshot_id(sid);
    }

    if select_columns.is_empty() {
        scan_builder = scan_builder.select_all();
    } else {
        scan_builder = scan_builder.select(select_columns.iter().map(String::as_str));
    }

    let scan = scan_builder
        .build()
        .map_err(|e| ConnectorError::ReadError(format!("build scan: {e}")))?;

    let stream = scan
        .to_arrow()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("scan to arrow: {e}")))?;

    let mut batches = Vec::new();
    let mut stream = std::pin::pin!(stream);
    while let Some(result) = stream.next().await {
        let batch = result.map_err(|e| ConnectorError::ReadError(format!("read batch: {e}")))?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Returns the current snapshot ID of a table, if any.
#[must_use]
pub fn current_snapshot_id(table: &Table) -> Option<i64> {
    table.metadata().current_snapshot().map(|s| s.snapshot_id())
}

/// Commits data files to an Iceberg table via a fast-append transaction.
///
/// When `epoch_metadata` is provided, also stores `writer_id` and `epoch`
/// in table properties for exactly-once recovery. Both the data append
/// and property update are committed atomically in a single transaction.
///
/// Returns the updated table with the new snapshot.
///
/// # Errors
///
/// Returns `ConnectorError::TransactionError` on commit failure.
pub async fn commit_data_files(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<iceberg::spec::DataFile>,
    epoch_metadata: Option<(&str, u64)>,
) -> Result<Table, ConnectorError> {
    let tx = Transaction::new(table);
    let tx: Transaction = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .map_err(|e| ConnectorError::TransactionError(format!("apply fast_append: {e}")))?;

    // Chain epoch metadata into the same atomic transaction.
    let tx = if let Some((writer_id, epoch)) = epoch_metadata {
        let key = format!("laminardb.writer.{writer_id}.last_epoch");
        tx.update_table_properties()
            .set(key, epoch.to_string())
            .apply(tx)
            .map_err(|e| {
                ConnectorError::TransactionError(format!("apply update_table_properties: {e}"))
            })?
    } else {
        tx
    };

    tx.commit(catalog)
        .await
        .map_err(|e| ConnectorError::TransactionError(format!("commit: {e}")))
}

/// Reads the last committed epoch for a writer from Iceberg table properties.
///
/// Returns `None` if the writer has never committed to this table.
#[must_use]
pub fn get_last_committed_epoch(table: &Table, writer_id: &str) -> Option<u64> {
    let key = format!("laminardb.writer.{writer_id}.last_epoch");
    table.metadata().properties().get(&key)?.parse().ok()
}

/// Creates an Iceberg table if it does not already exist.
///
/// # Errors
///
/// Returns `ConnectorError` on creation failure.
pub async fn ensure_table_exists(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    arrow_schema: &arrow_schema::SchemaRef,
) -> Result<(), ConnectorError> {
    let ns = iceberg::NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| ConnectorError::ConfigurationError(format!("invalid namespace: {e}")))?;

    let ident = TableIdent::new(ns.clone(), table_name.to_string());

    if catalog
        .table_exists(&ident)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("table_exists: {e}")))?
    {
        return Ok(());
    }

    let iceberg_schema = iceberg::arrow::arrow_schema_to_schema(arrow_schema).map_err(|e| {
        ConnectorError::SchemaMismatch(format!("arrow→iceberg schema conversion: {e}"))
    })?;

    if !catalog
        .namespace_exists(&ns)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("namespace_exists: {e}")))?
    {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .map_err(|e| ConnectorError::WriteError(format!("create namespace: {e}")))?;
    }

    let creation = iceberg::TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .build();

    catalog
        .create_table(&ns, creation)
        .await
        .map_err(|e| ConnectorError::WriteError(format!("create table: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_factory_dispatch_s3() {
        let factory = storage_factory_for_warehouse("s3://bucket/warehouse");
        let debug = format!("{factory:?}");
        assert!(debug.contains("S3"), "expected S3 factory, got: {debug}");
    }

    #[test]
    fn test_storage_factory_dispatch_s3a() {
        let factory = storage_factory_for_warehouse("s3a://bucket/warehouse");
        let debug = format!("{factory:?}");
        assert!(debug.contains("S3"), "expected S3 factory, got: {debug}");
    }

    #[test]
    fn test_storage_factory_dispatch_local() {
        let factory = storage_factory_for_warehouse("/tmp/warehouse");
        let debug = format!("{factory:?}");
        assert!(debug.contains("Fs"), "expected Fs factory, got: {debug}");
    }

    #[test]
    fn test_current_snapshot_id_empty_table() {
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![])
            .build()
            .unwrap();

        let creation = iceberg::TableCreation::builder()
            .name("test_table".to_string())
            .schema(schema)
            .location("s3://test/location".to_string())
            .build();

        let metadata = iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
            .unwrap()
            .build()
            .unwrap();

        let table = Table::builder()
            .metadata(metadata.metadata)
            .identifier(TableIdent::new(
                iceberg::NamespaceIdent::new("test".to_string()),
                "t".to_string(),
            ))
            .file_io(iceberg::io::FileIO::new_with_memory())
            .build()
            .unwrap();

        assert!(current_snapshot_id(&table).is_none());
    }
}
