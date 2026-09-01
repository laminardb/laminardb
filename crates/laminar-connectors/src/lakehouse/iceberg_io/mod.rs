//! Feature-gated I/O operations for Apache Iceberg.
//!
//! Contains catalog construction, table loading, scanning, and writing
//! functions. All code requires the `iceberg` feature.
#![allow(clippy::disallowed_types)] // cold path: lakehouse I/O
#![cfg(feature = "iceberg-core")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use futures_util::StreamExt;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
#[cfg(any(
    feature = "iceberg-storage-s3",
    feature = "iceberg-storage-gcs",
    feature = "iceberg-storage-azure",
    feature = "iceberg-storage-fs"
))]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

#[cfg(feature = "iceberg-catalog-rest")]
use super::iceberg_config::IcebergStorageEncryption;
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
use super::iceberg_config::IcebergStorageType;
use super::iceberg_config::{IcebergCatalogConfig, IcebergCatalogType, IcebergStorageConfig};
use crate::error::ConnectorError;
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
use crate::storage::{StorageLocation, StorageProvider};

const COMPAT_SCAN_MAX_BATCHES: usize = 65_536;
const COMPAT_SCAN_MAX_BYTES: usize = 64 * 1024 * 1024;
const COMPAT_SCAN_CONCURRENCY: usize = 4;
const COMPAT_SCAN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const WRITE_DATA_PATH_PROPERTY: &str = "write.data.path";
const WRITE_FOLDER_STORAGE_PATH_PROPERTY: &str = "write.folder-storage.path";

pub(crate) fn checked_deadline(
    timeout: std::time::Duration,
    setting: &str,
) -> Result<tokio::time::Instant, ConnectorError> {
    tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "[LDB-ICEBERG-DEADLINE-OVERFLOW] {setting} exceeds the platform clock range"
            ))
        })
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum CatalogAccess {
    Read,
    Write { auto_create: bool },
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CatalogCapabilities {
    pub(crate) idempotency_key_lifetime: Option<std::time::Duration>,
}

#[derive(Clone, Default)]
pub(crate) struct CatalogSession {
    #[cfg(feature = "iceberg-catalog-rest")]
    rest_authentication: Option<rest_catalog::RestAuthentication>,
}

pub(crate) struct BuiltCatalog {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) capabilities: CatalogCapabilities,
    pub(crate) session: CatalogSession,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AtomicTableRequirements {
    #[cfg(feature = "iceberg-catalog-rest")]
    pub(crate) table_uuid: uuid::Uuid,
    #[cfg(feature = "iceberg-catalog-rest")]
    pub(crate) schema_id: i32,
    #[cfg(feature = "iceberg-catalog-rest")]
    pub(crate) partition_spec_id: i32,
    #[cfg(feature = "iceberg-catalog-rest")]
    pub(crate) sort_order_id: i64,
}

impl AtomicTableRequirements {
    #[cfg(feature = "iceberg-catalog-rest")]
    pub(crate) fn from_table(table: &Table) -> Self {
        Self {
            table_uuid: table.metadata().uuid(),
            schema_id: table.metadata().current_schema_id(),
            partition_spec_id: table.metadata().default_partition_spec_id(),
            sort_order_id: table.metadata().default_sort_order_id(),
        }
    }

    #[cfg(not(feature = "iceberg-catalog-rest"))]
    pub(crate) fn from_table(_table: &Table) -> Self {
        Self {}
    }
}

mod append;
pub use append::commit_data_files_append;
pub(crate) use append::{
    commit_generated_data_files_append, iceberg_commit_error, GeneratedAppendError,
};

#[cfg(any(
    feature = "iceberg-storage-s3",
    feature = "iceberg-storage-gcs",
    feature = "iceberg-storage-azure",
    feature = "iceberg-storage-fs"
))]
mod bounded_storage;

#[cfg(feature = "iceberg-catalog-rest")]
mod rest_catalog;

mod single_dispatch;
pub(crate) use single_dispatch::SingleDispatchCatalog;

#[cfg(any(
    feature = "iceberg-storage-s3",
    feature = "iceberg-storage-gcs",
    feature = "iceberg-storage-azure",
    feature = "iceberg-storage-fs"
))]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
use bounded_storage::BoundedStorageFactory;

pub(crate) fn external_error_summary(error: &iceberg::Error) -> String {
    format!(
        "{} ({})",
        error.kind(),
        if error.retryable() {
            "retryable"
        } else {
            "terminal"
        }
    )
}

mod table_creation;

/// Selects storage capabilities for the table-data URLs the catalog returns.
/// The resolving factory dispatches on each returned URL, while the configured
/// type keeps feature-disabled failures deterministic before catalog I/O.
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
fn storage_factory(
    warehouse: &str,
    config: &IcebergStorageConfig,
    configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    let storage_type = config
        .storage_type
        .or_else(|| infer_storage_type(warehouse))
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "[LDB-5100] cannot infer storage backend from catalog warehouse; set storage.type explicitly"
                    .into(),
            )
        })?;

    match storage_type {
        IcebergStorageType::S3 => s3_storage_factory(config, configured_properties),
        IcebergStorageType::Gcs => gcs_storage_factory(config, configured_properties),
        IcebergStorageType::Azure => azure_storage_factory(config, configured_properties),
        IcebergStorageType::Fs => fs_storage_factory(config, configured_properties),
    }
}

#[cfg(any(test, feature = "iceberg-catalog-rest"))]
fn infer_storage_type(warehouse: &str) -> Option<IcebergStorageType> {
    match StorageLocation::parse(warehouse).ok()?.provider {
        StorageProvider::AwsS3 => Some(IcebergStorageType::S3),
        StorageProvider::Gcs => Some(IcebergStorageType::Gcs),
        StorageProvider::AzureAdls => Some(IcebergStorageType::Azure),
        StorageProvider::Local => Some(IcebergStorageType::Fs),
    }
}

#[cfg(feature = "iceberg-storage-s3")]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
#[allow(clippy::unnecessary_wraps)] // Matches the fail-closed feature-disabled signature.
fn s3_storage_factory(
    storage: &IcebergStorageConfig,
    configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Ok(Arc::new(BoundedStorageFactory::new(
        OpenDalResolvingStorageFactory::new(),
        storage.connect_timeout,
        storage.request_timeout,
        configured_properties,
    )))
}

#[cfg(not(feature = "iceberg-storage-s3"))]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
fn s3_storage_factory(
    _storage: &IcebergStorageConfig,
    _configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Err(missing_storage_feature("s3", "iceberg"))
}

#[cfg(feature = "iceberg-storage-gcs")]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
#[allow(clippy::unnecessary_wraps)] // Matches the fail-closed feature-disabled signature.
fn gcs_storage_factory(
    storage: &IcebergStorageConfig,
    configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Ok(Arc::new(BoundedStorageFactory::new(
        OpenDalResolvingStorageFactory::new(),
        storage.connect_timeout,
        storage.request_timeout,
        configured_properties,
    )))
}

#[cfg(not(feature = "iceberg-storage-gcs"))]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
fn gcs_storage_factory(
    _storage: &IcebergStorageConfig,
    _configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Err(missing_storage_feature("gcs", "iceberg-gcs"))
}

#[cfg(feature = "iceberg-storage-azure")]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
#[allow(clippy::unnecessary_wraps)] // Matches the fail-closed feature-disabled signature.
fn azure_storage_factory(
    storage: &IcebergStorageConfig,
    configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Ok(Arc::new(BoundedStorageFactory::new(
        OpenDalResolvingStorageFactory::new(),
        storage.connect_timeout,
        storage.request_timeout,
        configured_properties,
    )))
}

#[cfg(not(feature = "iceberg-storage-azure"))]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
fn azure_storage_factory(
    _storage: &IcebergStorageConfig,
    _configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Err(missing_storage_feature("azure", "iceberg-azure"))
}

#[cfg(feature = "iceberg-storage-fs")]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
#[allow(clippy::unnecessary_wraps)] // Matches the fail-closed feature-disabled signature.
fn fs_storage_factory(
    storage: &IcebergStorageConfig,
    configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Ok(Arc::new(BoundedStorageFactory::new(
        OpenDalResolvingStorageFactory::new(),
        storage.connect_timeout,
        storage.request_timeout,
        configured_properties,
    )))
}

#[cfg(not(feature = "iceberg-storage-fs"))]
#[cfg(any(test, feature = "iceberg-catalog-rest"))]
fn fs_storage_factory(
    _storage: &IcebergStorageConfig,
    _configured_properties: &HashMap<String, String>,
) -> Result<Arc<dyn iceberg::io::StorageFactory>, ConnectorError> {
    Err(missing_storage_feature("fs", "iceberg-storage-fs"))
}

#[cfg(all(
    any(test, feature = "iceberg-catalog-rest"),
    any(
        not(feature = "iceberg-storage-s3"),
        not(feature = "iceberg-storage-gcs"),
        not(feature = "iceberg-storage-azure"),
        not(feature = "iceberg-storage-fs")
    )
))]
fn missing_storage_feature(storage: &str, feature: &str) -> ConnectorError {
    ConnectorError::FeatureUnsupported(format!(
        "iceberg.storage.{storage}: build with the '{feature}' feature"
    ))
}

/// Builds a REST catalog from configuration.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if catalog initialization fails.
#[cfg_attr(not(feature = "iceberg-catalog-rest"), allow(clippy::unused_async))]
pub async fn build_catalog(
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
) -> Result<Arc<dyn Catalog>, ConnectorError> {
    Ok(
        build_catalog_for_access(config, storage, CatalogAccess::Read)
            .await?
            .catalog,
    )
}

pub(crate) async fn build_catalog_for_access(
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
    access: CatalogAccess,
) -> Result<BuiltCatalog, ConnectorError> {
    build_catalog_for_access_with_metrics(config, storage, access, None).await
}

#[cfg(feature = "iceberg-catalog-rest")]
pub(crate) async fn build_catalog_for_access_with_metrics(
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
    access: CatalogAccess,
    credential_refresh_failures: Option<prometheus::IntCounter>,
) -> Result<BuiltCatalog, ConnectorError> {
    match config.catalog_type {
        IcebergCatalogType::Rest => {
            rest_catalog::build(config, storage, access, credential_refresh_failures).await
        }
        other => Err(unsupported_catalog(other)),
    }
}

#[cfg(not(feature = "iceberg-catalog-rest"))]
pub(crate) fn build_catalog_for_access_with_metrics(
    config: &IcebergCatalogConfig,
    _storage: &IcebergStorageConfig,
    access: CatalogAccess,
    _credential_refresh_failures: Option<prometheus::IntCounter>,
) -> std::future::Ready<Result<BuiltCatalog, ConnectorError>> {
    if let CatalogAccess::Write { auto_create } = access {
        let _ = auto_create;
    }
    std::future::ready(Err(unsupported_catalog(config.catalog_type)))
}

fn unsupported_catalog(catalog_type: IcebergCatalogType) -> ConnectorError {
    let message = match catalog_type {
        IcebergCatalogType::Rest => {
            "iceberg.catalog.rest: build with the 'iceberg-catalog-rest' feature"
        }
        IcebergCatalogType::Glue => {
            "iceberg.catalog.glue: released catalog APIs are not enabled in this build"
        }
        IcebergCatalogType::Hms => {
            "iceberg.catalog.hms: released catalog APIs are not enabled in this build"
        }
        IcebergCatalogType::S3Tables => {
            "iceberg.catalog.s3tables: released catalog APIs are not enabled in this build"
        }
        IcebergCatalogType::Sql => {
            "iceberg.catalog.sql: released catalog APIs are not enabled in this build"
        }
    };
    ConnectorError::FeatureUnsupported(message.into())
}

#[cfg(feature = "iceberg-catalog-rest")]
pub(crate) async fn build_publication_catalog(
    catalog: Arc<dyn Catalog>,
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
    capabilities: &CatalogCapabilities,
    session: &CatalogSession,
    idempotency_key: Option<uuid::Uuid>,
    requirements: AtomicTableRequirements,
) -> Result<Option<Arc<dyn Catalog>>, ConnectorError> {
    if session.rest_authentication.is_none() {
        return Ok(None);
    }
    rest_catalog::build_publication(
        catalog,
        config,
        storage,
        session,
        capabilities,
        idempotency_key,
        requirements,
    )
    .await
    .map(Some)
}

#[cfg(not(feature = "iceberg-catalog-rest"))]
pub(crate) async fn build_publication_catalog(
    _catalog: Arc<dyn Catalog>,
    _config: &IcebergCatalogConfig,
    _storage: &IcebergStorageConfig,
    _capabilities: &CatalogCapabilities,
    _session: &CatalogSession,
    _idempotency_key: Option<uuid::Uuid>,
    _requirements: AtomicTableRequirements,
) -> Result<Option<Arc<dyn Catalog>>, ConnectorError> {
    Ok(None)
}

#[cfg(feature = "iceberg-catalog-rest")]
fn rest_properties(
    config: &IcebergCatalogConfig,
    storage: &IcebergStorageConfig,
) -> Result<HashMap<String, String>, ConnectorError> {
    for key in storage.properties.keys() {
        let normalized = key.to_ascii_lowercase();
        if config.properties.contains_key(key)
            || matches!(
                normalized.as_str(),
                "token"
                    | "credential"
                    | "oauth2-server-uri"
                    | "scope"
                    | "audience"
                    | "resource"
                    | "prefix"
                    | "uri"
                    | "warehouse"
                    | "disable-header-redaction"
            )
            || normalized.starts_with("header.")
        {
            return Err(ConnectorError::ConfigurationError(format!(
                "storage.property.{key} overlaps Iceberg REST catalog configuration"
            )));
        }
    }
    let mut properties = config.properties.clone();
    properties.extend(storage.properties.clone());
    if let Some(prefix) = &config.prefix {
        properties.insert("prefix".into(), prefix.clone());
    }
    if let Some(uri) = &config.oauth2_server_uri {
        properties.insert("oauth2-server-uri".into(), uri.clone());
    }
    if let Some(scope) = &config.oauth2_scope {
        properties.insert("scope".into(), scope.clone());
    }
    apply_storage_properties(
        &mut properties,
        storage,
        storage
            .storage_type
            .or_else(|| infer_storage_type(&config.warehouse)),
    );
    Ok(properties)
}

#[cfg(feature = "iceberg-catalog-rest")]
fn validate_storage_options(
    warehouse: &str,
    storage: &IcebergStorageConfig,
) -> Result<(), ConnectorError> {
    let storage_type = storage
        .storage_type
        .or_else(|| infer_storage_type(warehouse));
    if storage_type == Some(IcebergStorageType::Azure) && storage.endpoint.is_some() {
        return Err(ConnectorError::FeatureUnsupported(
            "iceberg.storage.azure.endpoint: iceberg-storage-opendal 0.10.1 derives the ADLS endpoint from the table URL and exposes no endpoint property"
                .into(),
        ));
    }
    if storage_type != Some(IcebergStorageType::S3)
        && (storage.region.is_some()
            || storage.path_style
            || storage.encryption != IcebergStorageEncryption::None)
    {
        return Err(ConnectorError::ConfigurationError(
            "storage.region, storage.path_style, and storage.encryption are valid only for Iceberg S3 storage"
                .into(),
        ));
    }
    if storage_type == Some(IcebergStorageType::Fs) && storage.endpoint.is_some() {
        return Err(ConnectorError::ConfigurationError(
            "storage.endpoint is not valid for Iceberg filesystem storage".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "iceberg-catalog-rest")]
fn apply_storage_properties(
    properties: &mut HashMap<String, String>,
    storage: &IcebergStorageConfig,
    storage_type: Option<IcebergStorageType>,
) {
    if let Some(endpoint) = &storage.endpoint {
        match storage_type {
            Some(IcebergStorageType::S3) => {
                properties.insert("s3.endpoint".into(), endpoint.clone());
            }
            Some(IcebergStorageType::Gcs) => {
                properties.insert("gcs.service.path".into(), endpoint.clone());
            }
            Some(IcebergStorageType::Azure | IcebergStorageType::Fs) | None => {}
        }
    }
    if storage_type == Some(IcebergStorageType::S3) {
        if let Some(region) = &storage.region {
            properties.insert("s3.region".into(), region.clone());
        }
        properties.insert(
            "s3.path-style-access".into(),
            storage.path_style.to_string(),
        );
        match storage.encryption {
            IcebergStorageEncryption::None => {}
            IcebergStorageEncryption::Sse => {
                properties.insert("s3.sse.type".into(), "s3".into());
            }
            IcebergStorageEncryption::Kms => {
                properties.insert("s3.sse.type".into(), "kms".into());
                if let Some(key) = &storage.kms_key {
                    properties.insert("s3.sse.key".into(), key.clone());
                }
            }
        }
    }
}

/// Loads an Iceberg table from the catalog.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the table cannot be loaded, or
/// `ConnectorError::FeatureUnsupported` when loading requires an unavailable capability.
pub async fn load_table(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
) -> Result<Table, ConnectorError> {
    load_table_with_timeout(
        catalog,
        namespace,
        table_name,
        std::time::Duration::from_secs(30),
    )
    .await
}

/// Loads an Iceberg table within a bounded catalog operation.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the table cannot be loaded before the deadline, or
/// `ConnectorError::FeatureUnsupported` when loading requires an unavailable capability.
pub async fn load_table_with_timeout(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    timeout: std::time::Duration,
) -> Result<Table, ConnectorError> {
    let ns = iceberg::NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| ConnectorError::ConfigurationError(format!("invalid namespace: {e}")))?;

    let ident = TableIdent::new(ns, table_name.to_string());

    let table = tokio::time::timeout(timeout, catalog.load_table(&ident))
        .await
        .map_err(|_| {
            ConnectorError::ReadError(format!(
                "[LDB-ICEBERG-CATALOG-TIMEOUT] load table '{table_name}' exceeded {timeout:?}"
            ))
        })?
        .map_err(|error| catalog_load_error(table_name, &error))?;
    validate_loaded_table_locations(&table)?;
    Ok(table)
}

fn catalog_load_error(table_name: &str, error: &iceberg::Error) -> ConnectorError {
    if error.kind() == iceberg::ErrorKind::FeatureUnsupported {
        return ConnectorError::FeatureUnsupported(
            "[LDB-ICEBERG-TABLE-LOAD-UNSUPPORTED] Iceberg table load requires a catalog or storage capability unavailable in this build"
                .into(),
        );
    }
    ConnectorError::ReadError(format!(
        "[LDB-ICEBERG-CATALOG-LOAD] load table '{table_name}' failed ({})",
        error.kind()
    ))
}

pub(crate) fn validate_loaded_table_locations(table: &Table) -> Result<(), ConnectorError> {
    validate_credential_free_location("table location", table.metadata().location())?;
    let metadata_location = table
        .metadata_location()
        .filter(|location| !location.is_empty())
        .ok_or_else(|| {
            ConnectorError::ReadError(
                "[LDB-ICEBERG-METADATA-LOCATION-MISSING] loaded table has no durable metadata location"
                    .into(),
            )
        })?;
    validate_credential_free_location("metadata location", metadata_location)?;
    for (label, property) in [
        ("data location", WRITE_DATA_PATH_PROPERTY),
        (
            "folder storage location",
            WRITE_FOLDER_STORAGE_PATH_PROPERTY,
        ),
    ] {
        if let Some(location) = table.metadata().properties().get(property) {
            validate_credential_free_location(label, location)?;
        }
    }
    Ok(())
}

pub(crate) fn effective_data_location(table: &Table) -> String {
    effective_data_location_from_metadata(table.metadata())
}

pub(crate) fn effective_data_location_from_metadata(
    metadata: &iceberg::spec::TableMetadata,
) -> String {
    // COMPAT: this is the precedence used by Iceberg's DefaultLocationGenerator.
    metadata
        .properties()
        .get(WRITE_DATA_PATH_PROPERTY)
        .or_else(|| {
            metadata
                .properties()
                .get(WRITE_FOLDER_STORAGE_PATH_PROPERTY)
        })
        .cloned()
        .unwrap_or_else(|| format!("{}/data", metadata.location()))
}

pub(crate) fn validate_credential_free_location(
    label: &str,
    location: &str,
) -> Result<(), ConnectorError> {
    if crate::security::value_contains_uri_secret(location, false) {
        return Err(ConnectorError::ReadError(format!(
            "[LDB-ICEBERG-CREDENTIAL-LOCATION] catalog {label} must not embed credentials"
        )));
    }
    Ok(())
}

/// Scans a table into a compatibility buffer with fixed file, batch, byte, and time bounds.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` when the snapshot is absent, a bound is exceeded, or the
/// scan fails.
#[deprecated(since = "0.30.0", note = "use IcebergSource for streaming reads")]
pub async fn scan_table(
    table: &Table,
    snapshot_id: Option<i64>,
    select_columns: &[String],
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let snapshot = match snapshot_id {
        Some(snapshot_id) => table
            .metadata()
            .snapshot_by_id(snapshot_id)
            .ok_or_else(|| {
                ConnectorError::ReadError(format!(
                    "[LDB-ICEBERG-SNAPSHOT-MISSING] snapshot {snapshot_id} does not exist"
                ))
            })?,
        None => match table.metadata().current_snapshot() {
            Some(snapshot) => snapshot,
            None => return Ok(Vec::new()),
        },
    };
    let mut builder = table
        .scan()
        .snapshot_id(snapshot.snapshot_id())
        .with_batch_size(Some(8_192))
        .with_concurrency_limit(COMPAT_SCAN_CONCURRENCY);
    builder = if select_columns.is_empty() {
        builder.select_all()
    } else {
        builder.select(select_columns.iter().map(String::as_str))
    };
    let scan = builder.build().map_err(|error| {
        super::iceberg_scan::connector_scan_error("build compatibility Iceberg scan", &error)
    })?;
    let deadline = tokio::time::Instant::now() + COMPAT_SCAN_TIMEOUT;
    super::iceberg_scan::preflight_snapshot(
        table,
        snapshot,
        super::iceberg_scan::ManifestReadLimits::fixed(),
        deadline,
    )
    .await?;
    let tasks = super::iceberg_scan::plan_files(
        &scan,
        super::iceberg_scan::DEFAULT_MAX_PLANNED_FILES,
        deadline,
    )
    .await?;
    let reader = table
        .reader_builder()
        .with_batch_size(8_192)
        .with_data_file_concurrency_limit(COMPAT_SCAN_CONCURRENCY)
        .build()
        .read(tasks)
        .map_err(|error| {
            super::iceberg_scan::connector_scan_error("create compatibility Iceberg reader", &error)
        })?;
    collect_compat_scan(reader.stream(), deadline).await
}

async fn collect_compat_scan(
    mut stream: iceberg::scan::ArrowRecordBatchStream,
    deadline: tokio::time::Instant,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    let mut batches = Vec::new();
    let mut bytes = 0_usize;
    while let Some(result) = tokio::time::timeout_at(deadline, stream.next())
        .await
        .map_err(|_| {
            ConnectorError::ReadError(
                "[LDB-ICEBERG-COMPAT-SCAN-TIMEOUT] compatibility scan exceeded 30 seconds".into(),
            )
        })?
    {
        let batch = result.map_err(|error| {
            super::iceberg_scan::connector_scan_error("compatibility Iceberg read failed", &error)
        })?;
        if batches.len() == COMPAT_SCAN_MAX_BATCHES {
            return Err(compat_scan_limit_error());
        }
        bytes = batch.columns().iter().try_fold(bytes, |total, column| {
            total
                .checked_add(column.get_array_memory_size())
                .ok_or_else(compat_scan_limit_error)
        })?;
        if bytes > COMPAT_SCAN_MAX_BYTES {
            return Err(compat_scan_limit_error());
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn compat_scan_limit_error() -> ConnectorError {
    ConnectorError::ReadError(
        "[LDB-ICEBERG-COMPAT-SCAN-LIMIT] compatibility scan exceeded its fixed buffer bounds"
            .into(),
    )
}

/// Returns the current snapshot ID of a table, if any.
#[must_use]
pub fn current_snapshot_id(table: &Table) -> Option<i64> {
    table.metadata().current_snapshot().map(|s| s.snapshot_id())
}

/// Creates an Iceberg table (and namespace) if it does not already exist.
///
/// # Errors
///
/// Returns `ConnectorError` on creation failure.
pub async fn ensure_table_exists(
    catalog: &dyn Catalog,
    config: &super::iceberg_config::IcebergSinkConfig,
    arrow_schema: &arrow_schema::SchemaRef,
) -> Result<(), ConnectorError> {
    let namespace = &config.catalog.namespace;
    let table_name = &config.catalog.table_name;
    let ns = iceberg::NamespaceIdent::from_strs(namespace.split('.').collect::<Vec<_>>())
        .map_err(|e| ConnectorError::ConfigurationError(format!("invalid namespace: {e}")))?;
    let ident = TableIdent::new(ns.clone(), table_name.clone());
    let creation = table_creation::build_table_creation(config, arrow_schema)?;

    // Ensure the namespace exists before probing the table: a HEAD on a table in
    // a missing namespace returns 400 (not 404) on some REST catalogs. Creation
    // tolerates a concurrent creator (N writers may auto-create the
    // same namespace at once) by re-checking existence on failure.
    if !catalog.namespace_exists(&ns).await.map_err(|error| {
        ConnectorError::ReadError(format!(
            "inspect Iceberg namespace ({})",
            external_error_summary(&error)
        ))
    })? {
        if let Err(error) = catalog.create_namespace(&ns, HashMap::new()).await {
            if !catalog.namespace_exists(&ns).await.unwrap_or(false) {
                return Err(ConnectorError::WriteError(format!(
                    "create Iceberg namespace ({})",
                    external_error_summary(&error)
                )));
            }
        }
    }

    if catalog.table_exists(&ident).await.map_err(|error| {
        ConnectorError::ReadError(format!(
            "inspect Iceberg table ({})",
            external_error_summary(&error)
        ))
    })? {
        return Ok(());
    }

    // Same race tolerance for the table itself.
    if let Err(error) = catalog.create_table(&ns, creation).await {
        if !catalog.table_exists(&ident).await.unwrap_or(false) {
            return Err(ConnectorError::WriteError(format!(
                "create Iceberg table ({})",
                external_error_summary(&error)
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests;
