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

    // configured_scheme is the bare scheme ("s3"), NOT "s3://" —
    // iceberg-storage-opendal formats the prefix as `{scheme}://{bucket}/`.
    let factory: Arc<dyn iceberg::io::StorageFactory> = match scheme.as_str() {
        "s3" | "s3a" => Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: scheme,
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

/// Table property holding the highest epoch the designated committer sealed.
const COORDINATED_EPOCH_PROP: &str = "laminardb.commit.epoch";

/// Serialize this writer's `data_files` into a commit descriptor for handoff to
/// the designated committer. Uses `table`'s partition type and format version.
///
/// # Errors
/// Returns `ConnectorError::WriteError` if a data file cannot be serialized.
pub fn encode_commit_descriptor(
    table: &Table,
    data_files: Vec<iceberg::spec::DataFile>,
) -> Result<Vec<u8>, ConnectorError> {
    let meta = table.metadata();
    let partition_type = meta.default_partition_type().clone();
    let format_version = meta.format_version();
    let json: Vec<String> = data_files
        .into_iter()
        .map(|df| iceberg::spec::serialize_data_file_to_json(df, &partition_type, format_version))
        .collect::<Result<_, _>>()
        .map_err(|e| ConnectorError::WriteError(format!("serialize data file: {e}")))?;
    super::commit_descriptor::encode(json)
}

/// Decode and flatten every writer's commit descriptor into one set of data
/// files, ready for a single `fast_append`.
///
/// # Errors
/// Returns `ConnectorError::TransactionError` on a malformed/incompatible descriptor.
pub fn decode_commit_descriptors(
    table: &Table,
    descriptors: &[Vec<u8>],
) -> Result<Vec<iceberg::spec::DataFile>, ConnectorError> {
    let meta = table.metadata();
    let partition_type = meta.default_partition_type().clone();
    let spec_id = meta.default_partition_spec_id();
    let schema = meta.current_schema().as_ref().clone();

    let mut out = Vec::new();
    for bytes in descriptors {
        let json: Vec<String> = super::commit_descriptor::decode(bytes)?;
        for entry in &json {
            out.push(
                iceberg::spec::deserialize_data_file_from_json(
                    entry,
                    spec_id,
                    &partition_type,
                    &schema,
                )
                .map_err(|e| {
                    ConnectorError::TransactionError(format!("deserialize data file: {e}"))
                })?,
            );
        }
    }
    Ok(out)
}

/// Highest epoch the designated committer has sealed, per table properties.
#[must_use]
pub fn coordinated_committed_epoch(table: &Table) -> Option<u64> {
    table
        .metadata()
        .properties()
        .get(COORDINATED_EPOCH_PROP)?
        .parse()
        .ok()
}

/// Append `data_files` and stamp the coordinated-commit epoch in one atomic
/// transaction. The epoch property is the idempotency guard: a re-run after
/// leader failover is skipped by the caller once the table reflects `epoch`.
///
/// # Errors
/// Returns `ConnectorError::TransactionError` on commit failure.
pub async fn commit_data_files_coordinated(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<iceberg::spec::DataFile>,
    epoch: u64,
) -> Result<Table, ConnectorError> {
    let tx = Transaction::new(table);
    let tx: Transaction = tx
        .fast_append()
        .add_data_files(data_files)
        .apply(tx)
        .map_err(|e| ConnectorError::TransactionError(format!("apply fast_append: {e}")))?;
    let tx = tx
        .update_table_properties()
        .set(COORDINATED_EPOCH_PROP.to_string(), epoch.to_string())
        .apply(tx)
        .map_err(|e| {
            ConnectorError::TransactionError(format!("apply update_table_properties: {e}"))
        })?;
    tx.commit(catalog)
        .await
        .map_err(|e| ConnectorError::TransactionError(format!("commit: {e}")))
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
    // tolerates a concurrent creator (N coordinated writers may auto-create the
    // same namespace at once) by re-checking existence on failure.
    if !catalog
        .namespace_exists(&ns)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("namespace_exists: {e}")))?
    {
        if let Err(e) = catalog.create_namespace(&ns, HashMap::new()).await {
            if !catalog.namespace_exists(&ns).await.unwrap_or(false) {
                return Err(ConnectorError::WriteError(format!(
                    "create namespace: {e}"
                )));
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
        if !catalog
            .table_exists(&ident)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("table_exists: {e}")))?
        {
            return Err(ConnectorError::WriteError(format!("create table: {e}")));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_factory_infers_s3_from_warehouse_url() {
        let f = storage_factory("s3://bucket/warehouse", None).unwrap();
        assert!(format!("{f:?}").contains("S3"));
    }

    #[test]
    fn test_storage_factory_infers_s3a_from_warehouse_url() {
        let f = storage_factory("s3a://bucket/warehouse", None).unwrap();
        assert!(format!("{f:?}").contains("S3"));
    }

    #[test]
    fn test_storage_factory_infers_fs_from_file_url() {
        let f = storage_factory("file:///tmp/warehouse", None).unwrap();
        assert!(format!("{f:?}").contains("Fs"));
    }

    #[test]
    fn test_storage_factory_bare_path_requires_explicit_storage_type() {
        // Trimmed `/` and `./` inference: REST catalogs use logical names
        // and we don't want a silent default to local fs.
        let err = storage_factory("/tmp/warehouse", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5100"), "got: {err}");
    }

    #[test]
    fn test_storage_factory_explicit_overrides_inference() {
        // Lakekeeper-style: warehouse is a name, storage backend is S3.
        let f = storage_factory("demo", Some("s3")).unwrap();
        assert!(format!("{f:?}").contains("S3"));
    }

    #[test]
    fn test_storage_factory_unknown_warehouse_without_storage_type_errors() {
        let err = storage_factory("demo", None).unwrap_err().to_string();
        assert!(err.contains("LDB-5100"), "got: {err}");
    }

    #[test]
    fn test_storage_factory_rejects_unknown_storage_type() {
        let err = storage_factory("demo", Some("hdfs"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5101"), "got: {err}");
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
