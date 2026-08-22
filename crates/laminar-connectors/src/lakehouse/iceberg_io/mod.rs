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

/// Selects the `OpenDalStorageFactory` for the table-data URLs the catalog
/// will return. Explicit `storage.type` wins; otherwise inferred from the
/// `s3://` / `s3a://` / `file://` warehouse URL.
fn storage_factory(
    warehouse: &str,
    storage_type: Option<&str>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    let scheme = storage_type
        .map(str::to_lowercase)
        .or_else(|| {
            if warehouse.starts_with("s3a://") {
                Some("s3a".to_string())
            } else if warehouse.starts_with("s3://") {
                Some("s3".to_string())
            } else if warehouse.starts_with("file://") {
                Some("fs".to_string())
            } else {
                None
            }
        })
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "[LDB-5100] cannot infer storage backend from warehouse '{warehouse}'; \
                 set storage.type = 's3' | 's3a' | 'fs'"
            ))
        })?;

    let factory: Arc<dyn iceberg::io::StorageFactory> = match scheme.as_str() {
        "s3" | "s3a" => Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }),
        "fs" => Arc::new(OpenDalStorageFactory::Fs),
        other => {
            return Err(ConnectorError::ConfigurationError(format!(
                "[LDB-5101] unsupported storage.type '{other}'; expected s3 | s3a | fs"
            )));
        }
    };
    Ok(factory)
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
    let storage_factory = storage_factory(&config.warehouse, config.storage_type.as_deref())?;

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

/// Append `data_files` in one Iceberg transaction.
///
/// # Errors
/// Returns [`ConnectorError::OutcomeUnknown`] when the catalog does not acknowledge the commit.
pub async fn commit_data_files_append(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<iceberg::spec::DataFile>,
) -> Result<Table, ConnectorError> {
    let tx = Transaction::new(table);
    let tx = if data_files.is_empty() {
        tx
    } else {
        tx.fast_append()
            .add_data_files(data_files)
            .apply(tx)
            .map_err(|e| ConnectorError::TransactionError(format!("apply fast_append: {e}")))?
    };
    tx.commit(catalog)
        .await
        .map_err(|error| iceberg_commit_error(&error))
}

fn iceberg_commit_error(error: &iceberg::Error) -> ConnectorError {
    use iceberg::ErrorKind;

    let kind = error.kind();
    let retryable = error.retryable();
    match kind {
        ErrorKind::CatalogCommitConflicts => {
            ConnectorError::WriteError(format!("Iceberg catalog commit conflict: {error}"))
        }
        ErrorKind::PreconditionFailed
        | ErrorKind::DataInvalid
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceNotFound
        | ErrorKind::TableNotFound
        | ErrorKind::FeatureUnsupported => ConnectorError::TransactionError(format!(
            "Iceberg catalog rejected commit ({kind}): {error}"
        )),
        ErrorKind::Unexpected => ConnectorError::outcome_unknown(
            format!("Iceberg catalog commit failed after dispatch and may have applied: {error}"),
            retryable,
        ),
        _ => ConnectorError::outcome_unknown(
            format!("Iceberg catalog returned an unclassified commit failure: {error}"),
            retryable,
        ),
    }
}

/// Creates an Iceberg table (and namespace) if it does not already exist.
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

    // Ensure the namespace exists before probing the table: a HEAD on a table in
    // a missing namespace returns 400 (not 404) on some REST catalogs. Creation
    // tolerates a concurrent creator (N writers may auto-create the
    // same namespace at once) by re-checking existence on failure.
    if !catalog
        .namespace_exists(&ns)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("namespace_exists: {e}")))?
    {
        if let Err(e) = catalog.create_namespace(&ns, HashMap::new()).await {
            if !catalog.namespace_exists(&ns).await.unwrap_or(false) {
                return Err(ConnectorError::WriteError(format!("create namespace: {e}")));
            }
        }
    }

    if catalog
        .table_exists(&ident)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("table_exists: {e}")))?
    {
        return Ok(());
    }

    // Pipeline-derived Arrow schemas don't carry `PARQUET:field_id`
    // metadata; let iceberg-rust assign sequential IDs.
    let iceberg_schema = iceberg::arrow::arrow_schema_to_schema_auto_assign_ids(arrow_schema)
        .map_err(|e| {
            ConnectorError::SchemaMismatch(format!("arrow→iceberg schema conversion: {e}"))
        })?;

    let creation = iceberg::TableCreation::builder()
        .name(table_name.to_string())
        .schema(iceberg_schema)
        .build();

    // Same race tolerance for the table itself.
    if let Err(e) = catalog.create_table(&ns, creation).await {
        if !catalog.table_exists(&ident).await.unwrap_or(false) {
            return Err(ConnectorError::WriteError(format!("create table: {e}")));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
