//! Delta Lake I/O integration module.
//!
//! This module provides the actual I/O operations for Delta Lake tables via the
//! `deltalake` crate. All functions are feature-gated behind `delta-lake`.
//!
//! # Architecture
//!
//! The I/O module is separate from the business logic in [`delta.rs`](super::delta)
//! to allow:
//! - Testing business logic without the `deltalake` dependency
//! - Clean separation of concerns (buffering/epoch management vs. actual writes)
//! - Easy mocking for unit tests
//!
//! Coordinated exactly-once publication uses runtime-owned, stable transaction
//! namespaces. Ordinary direct writes do not emit writer-local transaction
//! actions because a process-random identity cannot deduplicate recovery.

#[cfg(feature = "delta-lake")]
use std::collections::{BTreeMap, HashMap, HashSet};

#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(feature = "delta-lake")]
use std::time::Duration;

#[cfg(feature = "delta-lake")]
use arrow_array::RecordBatch;

#[cfg(feature = "delta-lake")]
use arrow_schema::SchemaRef;

// delta_kernel's TryIntoKernel trait is re-exported via deltalake.
#[cfg(feature = "delta-lake")]
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;

#[cfg(feature = "delta-lake")]
use deltalake::kernel::transaction::CommitProperties;

#[cfg(feature = "delta-lake")]
use deltalake::kernel::Transaction;

#[cfg(feature = "delta-lake")]
use deltalake::operations::write::SchemaMode;

#[cfg(feature = "delta-lake")]
use deltalake::protocol::SaveMode;

#[cfg(feature = "delta-lake")]
use deltalake::DeltaTable;

#[cfg(feature = "delta-lake")]
use tracing::{debug, info, warn};

#[cfg(feature = "delta-lake")]
use url::Url;

#[cfg(feature = "delta-lake")]
use crate::error::ConnectorError;

#[cfg(feature = "delta-lake")]
use crate::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, MAX_COORDINATED_COMMIT_BATCH_BYTES,
};

#[cfg(feature = "delta-lake")]
use super::commit_descriptor::{DeltaCommitDescriptor, DeltaTableBinding};

#[cfg(feature = "delta-lake")]
const SET_TRANSACTION_RETENTION: &str = "delta.setTransactionRetentionDuration";

#[cfg(feature = "delta-lake")]
const COORDINATED_HEAD_CONCURRENCY: usize = 16;

// Coordinated publication uses a separate metadata-client budget. The caller
// deadline owns preparation; after preparation, one conditional catalog
// attempt is allowed to finish without cancellation. Bounding both the HTTP
// and optimistic retry layers makes that terminal fence finite.
#[cfg(feature = "delta-lake")]
const COORDINATED_REQUEST_TIMEOUT: &str = "30s";
#[cfg(feature = "delta-lake")]
const COORDINATED_CONNECT_TIMEOUT: &str = "10s";
#[cfg(feature = "delta-lake")]
const COORDINATED_RETRY_TIMEOUT: &str = "30s";
#[cfg(feature = "delta-lake")]
const COORDINATED_HTTP_MAX_RETRIES: &str = "0";
#[cfg(feature = "delta-lake")]
const COORDINATED_MAX_BACKOFF: &str = "1s";
#[cfg(feature = "delta-lake")]
const COORDINATED_TERMINAL_IO_HORIZON: Duration = Duration::from_secs(24 * 60 * 60);
#[cfg(feature = "delta-lake")]
const COORDINATED_CLOCK_SKEW_MARGIN: Duration = Duration::from_secs(5 * 60);
#[cfg(feature = "delta-lake")]
const MIN_COORDINATED_DELETED_FILE_RETENTION: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[cfg(feature = "delta-lake")]
pub(super) fn bound_coordinated_storage_options(
    mut options: HashMap<String, String>,
) -> HashMap<String, String> {
    options.retain(|key, _| {
        !matches!(
            key.to_ascii_lowercase().as_str(),
            "timeout"
                | "aws_timeout"
                | "azure_timeout"
                | "google_timeout"
                | "connect_timeout"
                | "aws_connect_timeout"
                | "azure_connect_timeout"
                | "google_connect_timeout"
                | "max_retries"
                | "retry_timeout"
                | "max_backoff"
                | "backoff.max_backoff"
                | "backoff_config.max_backoff"
        )
    });
    options.insert("timeout".into(), COORDINATED_REQUEST_TIMEOUT.into());
    options.insert("connect_timeout".into(), COORDINATED_CONNECT_TIMEOUT.into());
    options.insert("retry_timeout".into(), COORDINATED_RETRY_TIMEOUT.into());
    options.insert("max_retries".into(), COORDINATED_HTTP_MAX_RETRIES.into());
    options.insert("max_backoff".into(), COORDINATED_MAX_BACKOFF.into());
    options
}

#[cfg(feature = "delta-lake")]
fn effective_values<F>(
    options: &HashMap<String, String>,
    aliases: &[&str],
    environment_keys: &[&str],
    environment: &F,
) -> Vec<String>
where
    F: Fn(&str) -> Option<String>,
{
    let explicit: Vec<String> = options
        .iter()
        .filter(|(key, _)| aliases.iter().any(|alias| key.eq_ignore_ascii_case(alias)))
        .map(|(_, value)| value.clone())
        .collect();
    if !explicit.is_empty() {
        return explicit;
    }
    environment_keys
        .iter()
        .filter_map(|key| environment(key))
        .collect()
}

#[cfg(feature = "delta-lake")]
fn has_effective_value<F>(
    options: &HashMap<String, String>,
    aliases: &[&str],
    environment_keys: &[&str],
    environment: &F,
) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    effective_values(options, aliases, environment_keys, environment)
        .iter()
        .any(|value| !value.trim().is_empty())
}

#[cfg(feature = "delta-lake")]
fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_s3_options<F>(
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    if has_effective_value(
        options,
        &[
            "endpoint",
            "endpoint_url",
            "aws_endpoint",
            "aws_endpoint_url",
        ],
        &["AWS_ENDPOINT", "AWS_ENDPOINT_URL"],
        environment,
    ) {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once does not admit custom S3 endpoints until their atomic-create behavior passes the release fault suite"
                .into(),
        ));
    }
    let conditional_put = effective_values(
        options,
        &["conditional_put", "aws_conditional_put"],
        &["AWS_CONDITIONAL_PUT"],
        environment,
    );
    if conditional_put
        .iter()
        .any(|value| !value.trim().eq_ignore_ascii_case("etag"))
    {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once requires native S3 ETag conditional put; Dynamo and disabled conditional-put modes are not certified"
                .into(),
        ));
    }
    if has_effective_value(
        options,
        &["s3_locking_provider", "aws_s3_locking_provider"],
        &["AWS_S3_LOCKING_PROVIDER"],
        environment,
    ) || effective_values(
        options,
        &["allow_unsafe_rename", "aws_s3_allow_unsafe_rename"],
        &["AWS_S3_ALLOW_UNSAFE_RENAME"],
        environment,
    )
    .iter()
    .any(|value| is_truthy(value))
    {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once requires the native conditional-put log store; locking-provider and unsafe-rename modes are not certified"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_azure_options<F>(
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    if has_effective_value(
        options,
        &["endpoint", "azure_endpoint", "azure_storage_endpoint"],
        &["AZURE_ENDPOINT", "AZURE_STORAGE_ENDPOINT"],
        environment,
    ) || effective_values(
        options,
        &[
            "use_emulator",
            "azure_use_emulator",
            "azure_storage_use_emulator",
        ],
        &["AZURE_USE_EMULATOR", "AZURE_STORAGE_USE_EMULATOR"],
        environment,
    )
    .iter()
    .any(|value| is_truthy(value))
    {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once does not admit custom Azure endpoints or emulators until their atomic-create behavior passes the release fault suite"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_gcs_options<F>(
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    if has_effective_value(
        options,
        &[
            "google_service_account",
            "google_service_account_path",
            "service_account",
            "service_account_path",
        ],
        &["GOOGLE_SERVICE_ACCOUNT", "GOOGLE_SERVICE_ACCOUNT_PATH"],
        environment,
    ) {
        return Err(ConnectorError::ConfigurationError(
            "Delta exactly-once does not admit GCS service-account path files because they can override the storage endpoint; use workload identity, application-default credentials, or an inline key"
                .into(),
        ));
    }
    for key in effective_values(
        options,
        &["google_service_account_key", "service_account_key"],
        &["GOOGLE_SERVICE_ACCOUNT_KEY"],
        environment,
    ) {
        let document: serde_json::Value = serde_json::from_str(&key).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "invalid GCS service-account key for Delta exactly-once: {error}"
            ))
        })?;
        if document.get("gcs_base_url").is_some() {
            return Err(ConnectorError::ConfigurationError(
                "Delta exactly-once does not admit a custom gcs_base_url until its atomic-create behavior passes the release fault suite"
                    .into(),
            ));
        }
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_storage_preflight_with_env<F>(
    table_path: &str,
    options: &HashMap<String, String>,
    environment: &F,
) -> Result<(), ConnectorError>
where
    F: Fn(&str) -> Option<String>,
{
    let scheme = table_path
        .split_once("://")
        .map_or("file", |(scheme, _)| scheme)
        .to_ascii_lowercase();
    match scheme.as_str() {
        "s3" | "s3a" => validate_coordinated_s3_options(options, environment),
        "az" | "azure" | "abfs" | "abfss" => {
            validate_coordinated_azure_options(options, environment)
        }
        "gs" | "gcs" => validate_coordinated_gcs_options(options, environment),
        _ => Ok(()),
    }
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_coordinated_storage_preflight(
    table_path: &str,
    options: &HashMap<String, String>,
) -> Result<(), ConnectorError> {
    validate_coordinated_storage_preflight_with_env(table_path, options, &|key| {
        std::env::var(key).ok()
    })
}

#[cfg(feature = "delta-lake")]
fn is_certified_coordinated_log_store(name: &str) -> bool {
    name == "DefaultLogStore"
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_log_store(table: &DeltaTable) -> Result<(), ConnectorError> {
    let log_store = table.log_store();
    if !is_certified_coordinated_log_store(&log_store.name()) {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta exactly-once requires the single-step atomic-create DefaultLogStore; '{}' is not certified",
            log_store.name()
        )));
    }
    validate_coordinated_storage_preflight(
        log_store.config().location().as_str(),
        &log_store.config().options().raw,
    )
}

/// Preserves the typed delta-rs commit failure until the sink can classify
/// publication certainty. Local planning failures have not dispatched a
/// catalog commit.
#[cfg(feature = "delta-lake")]
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeltaWriteAttemptError {
    /// Local validation or query construction failed before commit dispatch.
    #[error(transparent)]
    Local(#[from] ConnectorError),
    /// delta-rs entered the write operation and returned its typed failure.
    #[error(transparent)]
    Delta(#[from] deltalake::DeltaTableError),
}

#[cfg(feature = "delta-lake")]
impl DeltaWriteAttemptError {
    /// True only when delta-rs proves this attempt lost an optimistic race and
    /// did not publish a commit.
    pub(crate) fn is_definite_optimistic_conflict(&self) -> bool {
        matches!(self, Self::Delta(error) if is_definite_prewrite_conflict(error))
    }
}

#[cfg(feature = "delta-lake")]
fn is_definite_prewrite_conflict(error: &deltalake::DeltaTableError) -> bool {
    use deltalake::kernel::transaction::{CommitConflictError, TransactionError};

    matches!(
        error,
        deltalake::DeltaTableError::Transaction {
            source: TransactionError::CommitConflict(
                CommitConflictError::ConcurrentAppend
                    | CommitConflictError::ConcurrentDeleteRead
                    | CommitConflictError::ConcurrentDeleteDelete
                    | CommitConflictError::ConcurrentTransaction
            )
        }
    )
}

#[cfg(feature = "delta-lake")]
fn object_store_error_has_retryable_transport(error: &deltalake::ObjectStoreError) -> bool {
    use delta_object_store::client::{HttpError, HttpErrorKind};

    // Typed permanent/conditional variants fail closed. A Generic wrapper is
    // retryable only when its source chain contains a typed transport failure.
    let deltalake::ObjectStoreError::Generic { source, .. } = error else {
        return false;
    };
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(source.as_ref());
    while let Some(current) = cause {
        if let Some(http) = current.downcast_ref::<HttpError>() {
            return matches!(
                http.kind(),
                HttpErrorKind::Connect
                    | HttpErrorKind::Request
                    | HttpErrorKind::Timeout
                    | HttpErrorKind::Interrupted
            );
        }
        cause = current.source();
    }
    false
}

#[cfg(feature = "delta-lake")]
pub(crate) fn delta_error_has_retryable_transport(error: &deltalake::DeltaTableError) -> bool {
    match error {
        deltalake::DeltaTableError::ObjectStore { source }
        | deltalake::DeltaTableError::Transaction {
            source: deltalake::kernel::transaction::TransactionError::ObjectStore { source },
        } => object_store_error_has_retryable_transport(source),
        _ => false,
    }
}

#[cfg(feature = "delta-lake")]
pub(crate) fn is_definite_coordinated_nonpublication(error: &deltalake::DeltaTableError) -> bool {
    use deltalake::kernel::transaction::TransactionError;

    is_definite_prewrite_conflict(error)
        || matches!(
            error,
            deltalake::DeltaTableError::VersionAlreadyExists(_)
                | deltalake::DeltaTableError::Transaction {
                    source: TransactionError::VersionAlreadyExists(_)
                        | TransactionError::MaxCommitAttempts(_)
                }
        )
}

#[cfg(all(feature = "delta-lake", test))]
#[derive(Clone)]
pub(super) struct DelayedCoordinatedCatalogCommit {
    pub(super) started: Arc<tokio::sync::Notify>,
    pub(super) release: Arc<tokio::sync::Notify>,
}

#[cfg(all(feature = "delta-lake", test))]
tokio::task_local! {
    pub(super) static DELAY_COORDINATED_CATALOG_COMMIT: DelayedCoordinatedCatalogCommit;
}

#[cfg(feature = "delta-lake")]
pub(super) const MAX_COORDINATED_ADD_ACTIONS: usize = 4_096;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_PATH_BYTES: usize = 1_024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_STATS_BYTES: usize = 1024 * 1024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_PARTITION_ENTRIES: usize = 1_024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_PARTITION_BYTES: usize = 256 * 1024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_TAG_ENTRIES: usize = 1_024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_TAG_BYTES: usize = 256 * 1024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_TABLE_ID_BYTES: usize = 1_024;

/// Converts a path string to a URL.
#[cfg(feature = "delta-lake")]
fn path_to_url(path: &str) -> Result<Url, ConnectorError> {
    // If it already looks like a URL, parse it directly.
    if path.contains("://") {
        Url::parse(path)
            .map_err(|e| ConnectorError::ConfigurationError(format!("invalid URL '{path}': {e}")))
    } else {
        // Local path - convert to file URL.
        // First canonicalize if it exists, otherwise use as-is.
        let path_buf = std::path::Path::new(path);
        let normalized = if path_buf.exists() {
            std::fs::canonicalize(path_buf).map_err(|e| {
                ConnectorError::ConfigurationError(format!("invalid path '{path}': {e}"))
            })?
        } else {
            // For new tables, the path might not exist yet.
            // Use absolute path if possible.
            if path_buf.is_absolute() {
                path_buf.to_path_buf()
            } else {
                std::env::current_dir()
                    .map_err(|e| {
                        ConnectorError::ConfigurationError(format!("cannot get current dir: {e}"))
                    })?
                    .join(path_buf)
            }
        };

        Url::from_directory_path(&normalized).map_err(|()| {
            ConnectorError::ConfigurationError(format!(
                "cannot convert path to URL: {}",
                normalized.display()
            ))
        })
    }
}

/// Opens an existing Delta Lake table or creates a new one.
///
/// # Arguments
///
/// * `table_path` - Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`)
/// * `storage_options` - Storage credentials and configuration
/// * `schema` - Optional Arrow schema for table creation (required if table doesn't exist)
///
/// # Returns
///
/// The opened `DeltaTable` handle.
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the table cannot be opened or created.
#[cfg(feature = "delta-lake")]
#[allow(clippy::implicit_hasher)]
pub async fn open_or_create_table(
    table_path: &str,
    storage_options: HashMap<String, String>,
    schema: Option<&SchemaRef>,
) -> Result<DeltaTable, ConnectorError> {
    info!(table_path, "opening Delta Lake table");

    let url = path_to_url(table_path)?;

    // Try to open or initialize the table. `url` and `storage_options` are
    // not referenced after this call, so move rather than clone; repeated
    // conflict-recovery opens otherwise copy the complete option map.
    let table = DeltaTable::try_from_url_with_storage_options(url, storage_options)
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("failed to open table: {e}")))?;

    // Check if the table is initialized (has state).
    if table.version().is_some() {
        info!(
            table_path,
            version = table.version(),
            "opened existing Delta Lake table"
        );
        return Ok(table);
    }

    // Table doesn't exist — create if we have a schema, otherwise defer to first write_batch().
    let Some(schema) = schema else {
        info!(
            table_path,
            "table does not exist yet; will create on first write"
        );
        return Ok(table);
    };

    info!(table_path, "creating new Delta Lake table");

    // Convert Arrow schema to Delta Lake schema using TryIntoKernel.
    let delta_schema: deltalake::kernel::StructType = schema
        .as_ref()
        .try_into_kernel()
        .map_err(|e| ConnectorError::SchemaMismatch(format!("schema conversion failed: {e}")))?;

    // Create the table.
    let table = table
        .create()
        .with_columns(delta_schema.fields().cloned())
        .await
        .map_err(|e| ConnectorError::ConnectionFailed(format!("failed to create table: {e}")))?;

    info!(
        table_path,
        version = table.version(),
        "created new Delta Lake table"
    );

    Ok(table)
}

/// Writes batches to a Delta Lake table.
///
/// # Arguments
///
/// * `table` - The Delta Lake table handle (consumed and returned)
/// * `batches` - Record batches to write
/// * `save_mode` - Delta Lake save mode (Append, Overwrite, etc.)
/// * `partition_columns` - Optional partition column name slice
/// * `schema_evolution` - If true, auto-merge new columns into the table schema
///
/// # Returns
///
/// A tuple of (updated table handle, new Delta version).
///
/// # Errors
///
/// Preserves a typed delta-rs failure when the write operation fails.
#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn write_batches(
    table: DeltaTable,
    batches: Vec<RecordBatch>,
    save_mode: SaveMode,
    partition_columns: Option<&[String]>,
    schema_evolution: bool,
    target_file_size: Option<usize>,
    writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
) -> Result<(DeltaTable, i64), DeltaWriteAttemptError> {
    if batches.is_empty() {
        debug!("no batches to write, skipping");
        let version = table.version().unwrap_or(0);
        return Ok((table, version));
    }

    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();

    debug!(
        total_rows,
        num_batches = batches.len(),
        "writing batches to Delta Lake"
    );

    let mut write_builder = table
        .write(batches)
        .with_save_mode(save_mode)
        .with_commit_properties(CommitProperties::default());

    // Forward target file size to delta-rs so Parquet files match the
    // user's configured size, not just the internal default.
    if let Some(size) = target_file_size {
        write_builder = write_builder.with_target_file_size(size);
    }

    // Enable schema evolution (additive column merge) if requested.
    if schema_evolution {
        write_builder = write_builder.with_schema_mode(SchemaMode::Merge);
    }

    // Add partition columns if specified.
    if let Some(cols) = partition_columns {
        if !cols.is_empty() {
            write_builder = write_builder.with_partition_columns(cols.to_vec());
        }
    }

    if let Some(props) = writer_properties {
        write_builder = write_builder.with_writer_properties(props);
    }

    // Execute the write.
    let table = write_builder.await.map_err(DeltaWriteAttemptError::Delta)?;

    let version = table.version().unwrap_or(0);

    info!(version, total_rows, "committed Delta Lake transaction");

    Ok((table, version))
}

#[cfg(feature = "delta-lake")]
fn coordinated_transaction_ids(external_key: &str) -> (String, String) {
    (
        format!("{external_key}.checkpoint"),
        format!("{external_key}.fence"),
    )
}

#[cfg(feature = "delta-lake")]
fn reject_transaction_retention(table: &DeltaTable) -> Result<(), ConnectorError> {
    let snapshot = table.snapshot().map_err(|error| {
        ConnectorError::TransactionError(format!("read Delta snapshot: {error}"))
    })?;
    if let Some(value) = snapshot
        .metadata()
        .configuration()
        .get(SET_TRANSACTION_RETENTION)
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta coordinated commits require durable transaction cursors; table property \
             '{SET_TRANSACTION_RETENTION}'='{value}' can expire them"
        )));
    }
    Ok(())
}

/// Read the atomic checkpoint/fencing cursor for one coordinated namespace.
///
/// Both Delta `txn` actions must be present or absent. A partial pair is
/// corruption, never a fresh target. Delta transaction versions are treated as
/// opaque persisted values and checked explicitly rather than assumed to be
/// monotonic.
///
/// # Errors
/// Returns an error when the table cursor is unreadable, partial, or outside
/// Laminar's checkpoint/fencing ranges.
#[cfg(feature = "delta-lake")]
pub async fn get_coordinated_cursor(
    table: &DeltaTable,
    external_key: &str,
) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
    reject_transaction_retention(table)?;
    let snapshot = table.snapshot().map_err(|error| {
        ConnectorError::TransactionError(format!("read Delta snapshot: {error}"))
    })?;
    let log_store = table.log_store();
    let (checkpoint_id, fencing_token) = coordinated_transaction_ids(external_key);
    let (checkpoint, token) = tokio::try_join!(
        async {
            snapshot
                .transaction_version(log_store.as_ref(), &checkpoint_id)
                .await
                .map_err(|error| {
                    ConnectorError::TransactionError(format!(
                        "read Delta checkpoint cursor '{checkpoint_id}': {error}"
                    ))
                })
        },
        async {
            snapshot
                .transaction_version(log_store.as_ref(), &fencing_token)
                .await
                .map_err(|error| {
                    ConnectorError::TransactionError(format!(
                        "read Delta fencing cursor '{fencing_token}': {error}"
                    ))
                })
        }
    )?;

    match (checkpoint, token) {
        (None, None) => Ok(None),
        (Some(checkpoint), Some(token)) => {
            let checkpoint_id = u64::try_from(checkpoint).map_err(|_| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated checkpoint cursor '{external_key}' is negative"
                ))
            })?;
            let fencing_token = u64::try_from(token).map_err(|_| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated fencing token '{external_key}' is negative"
                ))
            })?;
            if fencing_token == 0 {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta coordinated fencing token '{external_key}' is zero"
                )));
            }
            Ok(Some(CoordinatedCommitCursor {
                checkpoint_id,
                fencing_token,
            }))
        }
        _ => Err(ConnectorError::TransactionError(format!(
            "Delta coordinated cursor '{external_key}' is corrupt: checkpoint and fence \
             transaction actions must be present together"
        ))),
    }
}

#[cfg(feature = "delta-lake")]
#[derive(serde::Serialize)]
struct DeltaProtocolFingerprint {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
}

#[cfg(feature = "delta-lake")]
#[derive(serde::Serialize)]
struct DeltaWriteMetadataFingerprint<'a> {
    table_id: &'a str,
    schema: &'a deltalake::kernel::StructType,
    partition_columns: &'a [String],
    configuration: BTreeMap<&'a str, &'a str>,
    protocol: DeltaProtocolFingerprint,
}

#[cfg(feature = "delta-lake")]
fn sorted_protocol_features<T: ToString>(features: Option<&[T]>) -> Vec<String> {
    let mut features: Vec<String> = features
        .unwrap_or_default()
        .iter()
        .map(ToString::to_string)
        .collect();
    features.sort_unstable();
    features
}

#[cfg(feature = "delta-lake")]
pub(super) fn coordinated_table_binding(
    table: &DeltaTable,
) -> Result<DeltaTableBinding, ConnectorError> {
    let snapshot = table.snapshot().map_err(|error| {
        ConnectorError::TransactionError(format!("read Delta staging snapshot: {error}"))
    })?;
    let metadata = snapshot.metadata();
    let table_id = metadata.id();
    if table_id.is_empty() || table_id.len() > MAX_COORDINATED_TABLE_ID_BYTES {
        return Err(ConnectorError::TransactionError(
            "Delta table id is empty or exceeds the coordinated descriptor limit".into(),
        ));
    }
    let schema = metadata.parse_schema().map_err(|error| {
        ConnectorError::TransactionError(format!("parse Delta table schema: {error}"))
    })?;
    let configuration = metadata
        .configuration()
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let protocol = snapshot.protocol();
    let fingerprint = DeltaWriteMetadataFingerprint {
        table_id,
        schema: &schema,
        partition_columns: metadata.partition_columns(),
        configuration,
        protocol: DeltaProtocolFingerprint {
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: sorted_protocol_features(protocol.reader_features()),
            writer_features: sorted_protocol_features(protocol.writer_features()),
        },
    };
    let write_metadata_sha256 = laminar_core::checkpoint::canonical_json_sha256(&fingerprint)
        .map_err(|error| {
            ConnectorError::TransactionError(format!("canonicalize Delta write metadata: {error}"))
        })?;
    Ok(DeltaTableBinding {
        table_id: table_id.to_owned(),
        write_metadata_sha256,
    })
}

#[cfg(feature = "delta-lake")]
fn validate_table_binding(binding: &DeltaTableBinding) -> Result<(), ConnectorError> {
    if binding.table_id.is_empty() || binding.table_id.len() > MAX_COORDINATED_TABLE_ID_BYTES {
        return Err(ConnectorError::TransactionError(
            "Delta coordinated descriptor has an invalid table id".into(),
        ));
    }
    if binding.write_metadata_sha256.len() != 64
        || !binding
            .write_metadata_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ConnectorError::TransactionError(
            "Delta coordinated descriptor has a non-canonical metadata digest".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn ensure_publication_deadline(
    deadline: tokio::time::Instant,
    operation: &str,
) -> Result<(), ConnectorError> {
    if deadline <= tokio::time::Instant::now() {
        Err(ConnectorError::TransactionError(format!(
            "Delta coordinated publication deadline elapsed during {operation}; the external outcome must be reconciled from its cursor"
        )))
    } else {
        Ok(())
    }
}

/// Serialize table-bound `Add` actions into one durable descriptor.
#[cfg(feature = "delta-lake")]
pub(super) fn encode_commit_descriptor(
    binding: &DeltaTableBinding,
    adds: &[deltalake::kernel::Add],
) -> Result<Vec<u8>, ConnectorError> {
    super::commit_descriptor::encode(binding, adds)
}

#[cfg(feature = "delta-lake")]
pub(super) fn encoded_add_array_len(
    adds: &[deltalake::kernel::Add],
) -> Result<usize, ConnectorError> {
    super::commit_descriptor::encoded_add_array_len(adds)
}

#[cfg(feature = "delta-lake")]
fn validate_descriptor_batch_lengths(
    lengths: impl IntoIterator<Item = usize>,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let mut total_bytes = 0usize;
    for (index, length) in lengths.into_iter().enumerate() {
        if index % 64 == 0 {
            ensure_publication_deadline(deadline, "descriptor batch admission")?;
        }
        if length > crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated descriptor exceeds the fixed {} byte per-participant limit",
                crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES
            )));
        }
        total_bytes = total_bytes.checked_add(length).ok_or_else(|| {
            ConnectorError::TransactionError(
                "Delta coordinated descriptor byte count overflow".into(),
            )
        })?;
        if total_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated descriptors exceed the fixed {MAX_COORDINATED_COMMIT_BATCH_BYTES} byte batch limit"
            )));
        }
    }
    ensure_publication_deadline(deadline, "descriptor batch admission")
}

#[cfg(feature = "delta-lake")]
fn decode_commit_descriptors_until(
    descriptors: &[Vec<u8>],
    deadline: tokio::time::Instant,
) -> Result<Option<DeltaCommitDescriptor>, ConnectorError> {
    validate_descriptor_batch_lengths(descriptors.iter().map(Vec::len), deadline)?;

    let mut binding = None;
    let mut adds = Vec::new();
    for bytes in descriptors {
        ensure_publication_deadline(deadline, "descriptor decoding")?;
        let descriptor = super::commit_descriptor::decode(bytes)?;
        ensure_publication_deadline(deadline, "descriptor decoding")?;
        validate_table_binding(&descriptor.binding)?;
        if descriptor.adds.is_empty() {
            return Err(ConnectorError::TransactionError(
                "Delta coordinated payload contains an empty descriptor".into(),
            ));
        }
        match &binding {
            Some(expected) if expected != &descriptor.binding => {
                return Err(ConnectorError::TransactionError(
                    "Delta coordinated descriptors bind different table metadata".into(),
                ));
            }
            None => binding = Some(descriptor.binding),
            Some(_) => {}
        }
        let projected = adds
            .len()
            .checked_add(descriptor.adds.len())
            .ok_or_else(|| {
                ConnectorError::TransactionError(
                    "Delta coordinated Add action count overflow".into(),
                )
            })?;
        if projected > MAX_COORDINATED_ADD_ACTIONS {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated publication exceeds the fixed {MAX_COORDINATED_ADD_ACTIONS} Add action limit"
            )));
        }
        adds.extend(descriptor.adds);
    }
    Ok(binding.map(|binding| DeltaCommitDescriptor { binding, adds }))
}

#[cfg(all(feature = "delta-lake", test))]
pub(super) fn decode_commit_descriptors(
    descriptors: &[Vec<u8>],
) -> Result<Option<DeltaCommitDescriptor>, ConnectorError> {
    decode_commit_descriptors_until(
        descriptors,
        tokio::time::Instant::now() + Duration::from_secs(30),
    )
}

#[cfg(feature = "delta-lake")]
#[derive(Clone)]
struct CoordinatedObject {
    path: deltalake::Path,
    expected_size: u64,
}

#[cfg(feature = "delta-lake")]
fn decode_percent_once(value: &str) -> Result<Option<String>, ConnectorError> {
    fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut changed = false;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                decoded.push((high << 4) | low);
                index += 3;
                changed = true;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    if !changed {
        return Ok(None);
    }
    String::from_utf8(decoded).map(Some).map_err(|_| {
        ConnectorError::TransactionError(
            "Delta coordinated Add path contains non-UTF-8 percent encoding".into(),
        )
    })
}

#[cfg(feature = "delta-lake")]
fn validate_path_segment(segment: &str, first: bool) -> Result<String, ConnectorError> {
    let mut current = segment.to_owned();
    for _ in 0..=4 {
        let trimmed = current.trim_end_matches(['.', ' ']);
        if trimmed.len() != current.len()
            || trimmed.is_empty()
            || trimmed == "."
            || trimmed == ".."
            || current.contains('/')
            || current.contains('\\')
            || (first && trimmed.eq_ignore_ascii_case("_delta_log"))
            || (first
                && trimmed.as_bytes().get(1) == Some(&b':')
                && trimmed.as_bytes()[0].is_ascii_alphabetic())
        {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add path has an unsafe segment: '{segment}'"
            )));
        }
        let Some(decoded) = decode_percent_once(&current)? else {
            return Ok(current.to_ascii_lowercase());
        };
        if decoded == current {
            return Ok(current.to_ascii_lowercase());
        }
        current = decoded;
    }
    Err(ConnectorError::TransactionError(format!(
        "Delta coordinated Add path has excessive percent-encoding depth: '{segment}'"
    )))
}

#[cfg(feature = "delta-lake")]
fn bounded_map_bytes<'a>(
    entries: impl Iterator<Item = (&'a String, Option<&'a String>)>,
    limit: usize,
    context: &str,
) -> Result<(), ConnectorError> {
    let mut total = 0usize;
    for (key, value) in entries {
        total = total
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.map_or(0, String::len)))
            .ok_or_else(|| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated Add {context} byte count overflow"
                ))
            })?;
        if total > limit {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add {context} exceeds the fixed {limit} byte limit"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)] // A descriptor is validated in one fail-closed pass.
fn validate_coordinated_descriptors(
    adds: &[deltalake::kernel::Add],
    partition_columns: &[String],
    deadline: tokio::time::Instant,
) -> Result<Vec<CoordinatedObject>, ConnectorError> {
    if adds.len() > MAX_COORDINATED_ADD_ACTIONS {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated publication exceeds the fixed {MAX_COORDINATED_ADD_ACTIONS} Add action limit"
        )));
    }
    let expected_partitions: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    let mut normalized_paths = HashSet::with_capacity(adds.len());
    let mut objects = Vec::with_capacity(adds.len());

    for (index, add) in adds.iter().enumerate() {
        if index % 64 == 0 {
            ensure_publication_deadline(deadline, "descriptor validation")?;
        }
        let raw_path = add.path.as_str();
        if raw_path.is_empty()
            || raw_path.len() > MAX_COORDINATED_PATH_BYTES
            || raw_path.starts_with('/')
            || raw_path.starts_with('\\')
            || raw_path.ends_with('/')
            || raw_path.contains('\\')
            || Url::parse(raw_path).is_ok()
        {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add path must be a non-empty relative object path: \
                 '{raw_path}'"
            )));
        }
        let path = deltalake::Path::parse(raw_path).map_err(|error| {
            ConnectorError::TransactionError(format!(
                "invalid Delta coordinated Add path '{raw_path}': {error}"
            ))
        })?;
        let mut normalized_path = String::with_capacity(raw_path.len());
        for (segment_index, segment) in raw_path.split('/').enumerate() {
            if segment_index != 0 {
                normalized_path.push('/');
            }
            normalized_path.push_str(&validate_path_segment(segment, segment_index == 0)?);
        }
        if !path
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
        {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add path is not a Parquet data file: '{raw_path}'"
            )));
        }
        if !normalized_paths.insert(normalized_path) {
            return Err(ConnectorError::TransactionError(format!(
                "duplicate Windows-equivalent Delta coordinated Add path '{raw_path}'"
            )));
        }
        let expected_size = u64::try_from(add.size).map_err(|_| {
            ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' has negative size {}",
                add.size
            ))
        })?;
        if expected_size == 0 {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' has zero size"
            )));
        }
        if add.modification_time < 0 {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' has negative modification time {}",
                add.modification_time
            )));
        }
        if !add.data_change {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' must be a data change"
            )));
        }
        if add.deletion_vector.is_some() {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated append Add '{raw_path}' cannot reference a deletion vector (the fixed limit is zero)"
            )));
        }
        if add.partition_values.len() > MAX_COORDINATED_PARTITION_ENTRIES {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' exceeds the fixed {MAX_COORDINATED_PARTITION_ENTRIES} partition entry limit"
            )));
        }
        if add.partition_values.len() != expected_partitions.len()
            || !add
                .partition_values
                .keys()
                .all(|column| expected_partitions.contains(column.as_str()))
        {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' partition values do not match the live table"
            )));
        }
        bounded_map_bytes(
            add.partition_values
                .iter()
                .map(|(key, value)| (key, value.as_ref())),
            MAX_COORDINATED_PARTITION_BYTES,
            "partition metadata",
        )?;
        if let Some(stats) = &add.stats {
            if stats.len() > MAX_COORDINATED_STATS_BYTES {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta coordinated Add '{raw_path}' statistics exceed the fixed {MAX_COORDINATED_STATS_BYTES} byte limit"
                )));
            }
            let value: serde_json::Value = serde_json::from_str(stats).map_err(|error| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated Add '{raw_path}' has invalid statistics: {error}"
                ))
            })?;
            if !value.is_object() {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta coordinated Add '{raw_path}' statistics must be a JSON object"
                )));
            }
        }
        if let Some(tags) = &add.tags {
            if tags.len() > MAX_COORDINATED_TAG_ENTRIES {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta coordinated Add '{raw_path}' exceeds the fixed {MAX_COORDINATED_TAG_ENTRIES} tag entry limit"
                )));
            }
            bounded_map_bytes(
                tags.iter().map(|(key, value)| (key, value.as_ref())),
                MAX_COORDINATED_TAG_BYTES,
                "tag metadata",
            )?;
        }
        if add
            .clustering_provider
            .as_ref()
            .is_some_and(|provider| provider.len() > 256)
        {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add '{raw_path}' clustering provider exceeds 256 bytes"
            )));
        }

        objects.push(CoordinatedObject {
            path,
            expected_size,
        });
    }

    ensure_publication_deadline(deadline, "descriptor validation")?;
    Ok(objects)
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_retention(retention: Duration) -> Result<(), ConnectorError> {
    if retention < MIN_COORDINATED_DELETED_FILE_RETENTION {
        return Err(ConnectorError::ConfigurationError(format!(
            "Delta exactly-once requires deleted-file retention of at least {MIN_COORDINATED_DELETED_FILE_RETENTION:?}; table config is {retention:?}",
        )));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
async fn validate_coordinated_objects(
    table: &DeltaTable,
    objects: Vec<CoordinatedObject>,
    deleted_file_retention: Duration,
    required_alive_until: chrono::DateTime<chrono::Utc>,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    if objects.is_empty() {
        return Ok(());
    }
    validate_coordinated_retention(deleted_file_retention)?;

    let retention = chrono::Duration::from_std(deleted_file_retention).map_err(|_| {
        ConnectorError::TransactionError(
            "Delta deleted-file retention duration exceeds the supported clock range".into(),
        )
    })?;
    let objects = Arc::new(objects);
    let next = Arc::new(AtomicUsize::new(0));
    let store = table.log_store().object_store(None);
    let mut workers = tokio::task::JoinSet::new();

    for _ in 0..COORDINATED_HEAD_CONCURRENCY.min(objects.len()) {
        let objects = Arc::clone(&objects);
        let next = Arc::clone(&next);
        let store = Arc::clone(&store);
        workers.spawn(async move {
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(object) = objects.get(index) else {
                    return Ok::<(), ConnectorError>(());
                };
                let metadata = tokio::time::timeout_at(deadline, store.head(&object.path))
                    .await
                    .map_err(|_| {
                        ConnectorError::TransactionError(format!(
                            "Delta coordinated object HEAD exceeded the publication deadline for '{}'",
                            object.path
                        ))
                    })?
                    .map_err(|error| {
                        ConnectorError::TransactionError(format!(
                            "HEAD Delta coordinated object '{}': {error}",
                            object.path
                        ))
                    })?;
                if metadata.size != object.expected_size {
                    return Err(ConnectorError::TransactionError(format!(
                        "Delta coordinated object '{}' size mismatch: descriptor {}, physical {}",
                        object.path, object.expected_size, metadata.size
                    )));
                }
                let vacuum_eligible_at = metadata
                    .last_modified
                    .checked_add_signed(retention)
                    .ok_or_else(|| {
                        ConnectorError::TransactionError(format!(
                            "Delta coordinated object '{}' recovery horizon exceeds the supported \
                             clock range",
                            object.path
                        ))
                    })?;
                if vacuum_eligible_at <= required_alive_until {
                    return Err(ConnectorError::TransactionError(format!(
                        "Delta coordinated object '{}' is at or inside the recovery-horizon safety \
                         margin: vacuum eligible at {vacuum_eligible_at}, publication must remain \
                         safe through {required_alive_until}",
                        object.path
                    )));
                }
            }
        });
    }

    while let Some(result) = workers.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                workers.abort_all();
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                return Err(ConnectorError::Internal(format!(
                    "Delta coordinated object validation worker failed: {error}"
                )));
            }
        }
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
enum PreparedCoordinatedPublication {
    AlreadyCommitted,
    Commit {
        adds: Vec<deltalake::kernel::Add>,
        binding: Option<DeltaTableBinding>,
        cursor: CoordinatedCommitCursor,
        descriptor_count: usize,
    },
}

#[cfg(feature = "delta-lake")]
struct CoordinatedPublicationOutcome {
    descriptor_count: usize,
}

#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)] // Keep the ordered prepare/admit/reconcile protocol contiguous.
async fn publish_coordinated<F>(
    table: &DeltaTable,
    external_key: &str,
    deadline: tokio::time::Instant,
    prepare: F,
) -> Result<CoordinatedPublicationOutcome, ConnectorError>
where
    F: Fn(
            Option<CoordinatedCommitCursor>,
        ) -> Result<PreparedCoordinatedPublication, ConnectorError>
        + Send,
{
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::kernel::Action;
    use deltalake::protocol::DeltaOperation;
    use deltalake::table::config::TablePropertiesExt as _;

    ensure_publication_deadline(deadline, "admission")?;
    let publication_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
    let budget_on_clock = chrono::Duration::from_std(publication_budget).map_err(|_| {
        ConnectorError::TransactionError(
            "Delta coordinated publication budget exceeds the supported clock range".into(),
        )
    })?;
    let terminal_horizon =
        chrono::Duration::from_std(COORDINATED_TERMINAL_IO_HORIZON).map_err(|_| {
            ConnectorError::TransactionError(
                "Delta coordinated terminal I/O horizon exceeds the supported clock range".into(),
            )
        })?;
    let clock_skew_margin =
        chrono::Duration::from_std(COORDINATED_CLOCK_SKEW_MARGIN).map_err(|_| {
            ConnectorError::TransactionError(
                "Delta coordinated clock-skew margin exceeds the supported clock range".into(),
            )
        })?;
    let required_alive_until = chrono::Utc::now()
        .checked_add_signed(budget_on_clock)
        .and_then(|deadline| deadline.checked_add_signed(terminal_horizon))
        .and_then(|horizon| horizon.checked_add_signed(clock_skew_margin))
        .ok_or_else(|| {
            ConnectorError::TransactionError(
                "Delta coordinated recovery horizon exceeds the supported clock range".into(),
            )
        })?;

    // Cursor filtering and the commit base share one freshly updated snapshot.
    let mut current = table.clone();
    validate_coordinated_log_store(&current)?;
    tokio::time::timeout_at(deadline, current.update_state())
        .await
        .map_err(|_| {
            ConnectorError::WriteError(
                "Delta coordinated snapshot refresh exceeded the publication deadline without dispatching a commit"
                    .into(),
            )
        })?
        .map_err(|error| {
            ConnectorError::WriteError(format!(
                "refresh Delta coordinated publication snapshot before commit dispatch: {error}"
            ))
        })?;
    ensure_publication_deadline(deadline, "snapshot refresh")?;
    let observed = tokio::time::timeout_at(deadline, get_coordinated_cursor(&current, external_key))
        .await
        .map_err(|_| {
            ConnectorError::WriteError(
                "Delta coordinated cursor read exceeded the publication deadline without dispatching a commit"
                    .into(),
            )
        })??;
    ensure_publication_deadline(deadline, "cursor read")?;
    let PreparedCoordinatedPublication::Commit {
        adds,
        binding,
        cursor,
        descriptor_count,
    } = prepare(observed)?
    else {
        current.version().ok_or_else(|| {
            ConnectorError::TransactionError(
                "Delta coordinated cursor exists on an unversioned table".into(),
            )
        })?;
        return Ok(CoordinatedPublicationOutcome {
            descriptor_count: 0,
        });
    };
    if cursor.fencing_token == 0 {
        return Err(ConnectorError::TransactionError(
            "Delta coordinated fencing token must be non-zero".into(),
        ));
    }
    let checkpoint_id = i64::try_from(cursor.checkpoint_id).map_err(|_| {
        ConnectorError::TransactionError(
            "checkpoint id exceeds Delta transaction-version range".into(),
        )
    })?;
    let fencing_token = i64::try_from(cursor.fencing_token).map_err(|_| {
        ConnectorError::TransactionError(
            "fencing token exceeds Delta transaction-version range".into(),
        )
    })?;

    let snapshot = current
        .snapshot()
        .map_err(|error| ConnectorError::TransactionError(format!("snapshot: {error}")))?;
    if adds.is_empty() {
        if binding.is_some() {
            return Err(ConnectorError::TransactionError(
                "empty Delta coordinated publication unexpectedly carries a table binding".into(),
            ));
        }
    } else {
        let binding = binding.as_ref().ok_or_else(|| {
            ConnectorError::TransactionError(
                "non-empty Delta coordinated publication has no table binding".into(),
            )
        })?;
        let current_binding = coordinated_table_binding(&current)?;
        if binding != &current_binding {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated descriptor table binding changed before publication (staged table '{}', live table '{}')",
                binding.table_id, current_binding.table_id
            )));
        }
    }
    let partition_columns = snapshot.metadata().partition_columns().clone();
    let objects = validate_coordinated_descriptors(&adds, &partition_columns, deadline)?;
    validate_coordinated_objects(
        &current,
        objects,
        snapshot.table_config().deleted_file_retention_duration(),
        required_alive_until,
        deadline,
    )
    .await?;
    ensure_publication_deadline(deadline, "object validation")?;

    let partition_by = (!partition_columns.is_empty()).then_some(partition_columns);
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by,
        predicate: None,
    };
    let actions: Vec<Action> = adds.into_iter().map(Action::Add).collect();
    let (checkpoint_transaction_id, fence_transaction_id) =
        coordinated_transaction_ids(external_key);
    // Checkpoint recovery owns optimistic retries. delta-rs receives one
    // conditional catalog attempt so a timed-out terminal fence has a bounded
    // amount of provider work and cannot amplify descriptor HEAD traffic.
    let props = CommitProperties::default()
        .with_max_retries(0)
        .with_application_transactions(vec![
            Transaction::new(checkpoint_transaction_id, checkpoint_id),
            Transaction::new(fence_transaction_id, fencing_token),
        ]);
    ensure_publication_deadline(deadline, "catalog commit preparation")?;
    let pre_commit = CommitBuilder::from(props).with_actions(actions).build(
        Some(snapshot),
        current.log_store(),
        operation,
    );
    let prepared_commit = tokio::time::timeout_at(
        deadline,
        pre_commit.into_prepared_commit_future(),
    )
    .await
    .map_err(|_| {
        ConnectorError::WriteError(
            "Delta coordinated commit preparation exceeded the publication deadline without dispatching a catalog write"
                .into(),
        )
    })?
    .map_err(|error| {
        if delta_error_has_retryable_transport(&error) {
            ConnectorError::WriteError(format!(
                "prepare Delta coordinated commit before catalog dispatch: {error}"
            ))
        } else {
            ConnectorError::TransactionError(format!(
                "prepare Delta coordinated commit before catalog dispatch: {error}"
            ))
        }
    })?;
    ensure_publication_deadline(deadline, "catalog commit admission")?;

    #[cfg(test)]
    if let Ok(delay) = DELAY_COORDINATED_CATALOG_COMMIT.try_with(Clone::clone) {
        delay.started.notify_one();
        delay.release.notified().await;
    }

    // Only PreparedCommit may outlive the caller: it performs conflict checks
    // and the atomic log write. Dropping the returned PostCommit deliberately
    // keeps snapshot refresh, checkpoints, and log cleanup off this path.
    match prepared_commit.await {
        Ok(_post_commit) => Ok(CoordinatedPublicationOutcome { descriptor_count }),
        Err(error) => {
            let reconciliation = async {
                let mut reconciled = current.clone();
                reconciled.update_state().await.map_err(|refresh_error| {
                    format!("refresh after prepared-commit failure: {refresh_error}")
                })?;
                get_coordinated_cursor(&reconciled, external_key)
                    .await
                    .map_err(|cursor_error| {
                        format!("read cursor after prepared-commit failure: {cursor_error}")
                    })
            }
            .await;
            if reconciliation.as_ref() == Ok(&Some(cursor)) {
                return Ok(CoordinatedPublicationOutcome { descriptor_count });
            }
            if is_definite_coordinated_nonpublication(&error) {
                return Err(ConnectorError::WriteError(format!(
                    "Delta coordinated optimistic collision did not publish: {error}"
                )));
            }
            let retryable = delta_error_has_retryable_transport(&error);
            let reconciliation = match reconciliation {
                Ok(observed) => format!("reconciliation observed cursor {observed:?}"),
                Err(reconciliation_error) => reconciliation_error,
            };
            Err(ConnectorError::outcome_unknown(
                format!(
                    "Delta coordinated catalog write was dispatched but its outcome is not known: {error}; {reconciliation}"
                ),
                retryable,
            ))
        }
    }
}

/// Filter a validated checkpoint batch against one freshly observed cursor,
/// then publish the remaining Adds and target cursor from that same snapshot.
///
/// # Errors
/// Returns `ConnectorError::TransactionError` on validation or commit failure.
#[cfg(feature = "delta-lake")]
pub(super) async fn commit_batch_coordinated(
    table: &DeltaTable,
    batch: &CoordinatedCommitBatch,
    deadline: tokio::time::Instant,
) -> Result<usize, ConnectorError> {
    let external_key = batch.namespace.external_key();
    let outcome = publish_coordinated(table, &external_key, deadline, |observed_cursor| {
        batch
            .validate_observed_cursor(observed_cursor)
            .map_err(|error| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated cursor continuity check failed: {error}"
                ))
            })?;
        if let Some(cursor) = observed_cursor {
            if cursor.checkpoint_id == batch.target.checkpoint_id
                && cursor.fencing_token == batch.fencing_token
            {
                return Ok(PreparedCoordinatedPublication::AlreadyCommitted);
            }
            if cursor.checkpoint_id > batch.target.checkpoint_id
                && cursor.fencing_token == batch.fencing_token
            {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta coordinated cursor {} is already above exact target {}; the target cannot be inferred as the timed-out publication",
                    cursor.checkpoint_id, batch.target.checkpoint_id
                )));
            }
        }

        let observed_checkpoint_id = observed_cursor.map_or(0, |cursor| cursor.checkpoint_id);
        let descriptors: Vec<Vec<u8>> = batch
            .entries
            .iter()
            .filter(|entry| entry.attempt.checkpoint_id > observed_checkpoint_id)
            .filter_map(|entry| entry.payload.clone())
            .collect();
        let descriptor = decode_commit_descriptors_until(&descriptors, deadline)?;
        let (binding, adds) = descriptor.map_or((None, Vec::new()), |descriptor| {
            (Some(descriptor.binding), descriptor.adds)
        });
        Ok(PreparedCoordinatedPublication::Commit {
            adds: adds.clone(),
            binding: binding.clone(),
            cursor: CoordinatedCommitCursor {
                checkpoint_id: batch.target.checkpoint_id,
                fencing_token: batch.fencing_token,
            },
            descriptor_count: descriptors.len(),
        })
    })
    .await?;
    Ok(outcome.descriptor_count)
}

/// Low-level cursor primitive retained only for focused unit tests. Production
/// must use `commit_batch_coordinated` so overlap filtering occurs after refresh.
#[cfg(all(feature = "delta-lake", test))]
pub(super) async fn commit_adds_coordinated(
    table: &DeltaTable,
    adds: Vec<deltalake::kernel::Add>,
    external_key: &str,
    cursor: CoordinatedCommitCursor,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let binding = (!adds.is_empty())
        .then(|| coordinated_table_binding(table))
        .transpose()?;
    publish_coordinated(table, external_key, deadline, |observed_cursor| {
        if let Some(observed) = observed_cursor {
            if observed.fencing_token > cursor.fencing_token {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta fencing token {} is stale; target already records {}",
                    cursor.fencing_token, observed.fencing_token
                )));
            }
            if observed.checkpoint_id > cursor.checkpoint_id {
                return Err(ConnectorError::TransactionError(format!(
                    "Delta checkpoint cursor would roll back from {} to {}",
                    observed.checkpoint_id, cursor.checkpoint_id
                )));
            }
            if observed.checkpoint_id == cursor.checkpoint_id {
                if observed.fencing_token != cursor.fencing_token {
                    return Err(ConnectorError::TransactionError(format!(
                        "Delta checkpoint {} already records fencing token {}; it cannot \
                             change to {}",
                        cursor.checkpoint_id, observed.fencing_token, cursor.fencing_token
                    )));
                }
                return Ok(PreparedCoordinatedPublication::AlreadyCommitted);
            }
        }
        Ok(PreparedCoordinatedPublication::Commit {
            adds: adds.clone(),
            binding: binding.clone(),
            cursor,
            descriptor_count: 1,
        })
    })
    .await?;
    Ok(())
}

/// Returns the table's partition columns, or an empty list if the snapshot is
/// unavailable. Best-effort: used for clustering diagnostics, never for
/// correctness, so a missing snapshot is not an error.
#[cfg(feature = "delta-lake")]
#[must_use]
pub fn get_partition_columns(table: &DeltaTable) -> Vec<String> {
    match table.snapshot() {
        Ok(snapshot) => snapshot.snapshot().metadata().partition_columns().clone(),
        Err(_) => Vec::new(),
    }
}

/// Extracts the Arrow schema from a Delta Lake table.
///
/// # Arguments
///
/// * `table` - The Delta Lake table handle
///
/// # Returns
///
/// The table's Arrow schema.
///
/// # Errors
///
/// Returns `ConnectorError::SchemaMismatch` if schema extraction fails.
#[cfg(feature = "delta-lake")]
pub fn get_table_schema(table: &DeltaTable) -> Result<SchemaRef, ConnectorError> {
    let state = table
        .snapshot()
        .map_err(|e| ConnectorError::SchemaMismatch(format!("table has no snapshot: {e}")))?;

    // Use the pre-computed Arrow schema from the EagerSnapshot.
    Ok(state.snapshot().arrow_schema())
}

/// Returns the latest committed version via the log store.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` on failure.
#[cfg(feature = "delta-lake")]
pub async fn get_latest_version(table: &mut DeltaTable) -> Result<i64, ConnectorError> {
    let log_store = table.log_store();
    let current = table.version().unwrap_or(0);
    log_store
        .get_latest_version(current)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to get latest version: {e}")))
}

/// Reads record batches from a specific Delta Lake table version.
///
/// Loads the requested version, applies a `LIMIT` to bound memory usage,
/// then streams results via `execute_stream` to avoid materializing the
/// entire version in memory.
///
/// # Arguments
///
/// * `table` - Mutable reference to the Delta Lake table handle
/// * `version` - The table version to read
/// * `max_records` - Maximum number of records to return. Pass `usize::MAX`
///   to read all records (unbounded).
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the version cannot be loaded or scanned.
///
/// Returns `(batches, fully_consumed)` — `fully_consumed` is `false` when
/// `max_records` truncated the result and more rows remain.
#[cfg(feature = "delta-lake")]
pub async fn read_batches_at_version(
    table: &mut DeltaTable,
    version: i64,
    max_records: usize,
) -> Result<(Vec<RecordBatch>, bool), ConnectorError> {
    use datafusion::prelude::SessionContext;
    use tokio_stream::StreamExt;

    // Load the specific version.
    table
        .load_version(version)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to load version {version}: {e}")))?;

    debug!(version, "Delta Lake: loaded version for reading");

    // Build a DeltaTableProvider via the builder and register it with DataFusion.
    let provider =
        table.table_provider().build().await.map_err(|e| {
            ConnectorError::ReadError(format!("failed to build table provider: {e}"))
        })?;

    let ctx = SessionContext::new();
    ctx.register_table("delta_source_scan", Arc::new(provider))
        .map_err(|e| ConnectorError::ReadError(format!("failed to register scan table: {e}")))?;

    // Apply LIMIT to bound memory: prevents OOM on large versions.
    let df = ctx
        .sql("SELECT * FROM delta_source_scan")
        .await
        .map_err(|e| ConnectorError::ReadError(format!("scan query failed: {e}")))?;

    let df = if max_records < usize::MAX {
        df.limit(0, Some(max_records))
            .map_err(|e| ConnectorError::ReadError(format!("limit failed: {e}")))?
    } else {
        df
    };

    // Stream results instead of collect() to avoid materializing everything.
    let mut stream = df
        .execute_stream()
        .await
        .map_err(|e| ConnectorError::ReadError(format!("stream execution failed: {e}")))?;

    let mut batches = Vec::new();
    let mut total_rows: usize = 0;

    while let Some(result) = stream.next().await {
        let batch =
            result.map_err(|e| ConnectorError::ReadError(format!("stream batch failed: {e}")))?;
        if batch.num_rows() == 0 {
            continue;
        }
        total_rows += batch.num_rows();
        batches.push(batch);

        // Respect max_records even between DataFusion batches.
        if total_rows >= max_records {
            break;
        }
    }

    // If we stopped due to max_records, probe whether the stream has more.
    // Without this, a version with exactly max_records rows would be
    // misclassified as truncated and re-read forever.
    let fully_consumed = if total_rows >= max_records {
        stream.next().await.is_none()
    } else {
        true
    };

    debug!(
        version,
        num_batches = batches.len(),
        total_rows,
        fully_consumed,
        "Delta Lake: scanned version"
    );

    Ok((batches, fully_consumed))
}

/// Reads only the rows added in a specific Delta Lake version.
///
/// Parses `_delta_log/{version:020}.json` for `add` actions, then reads
/// only those Parquet files via the table's object store. This is
/// `O(new_files)` per version, not `O(table_size)`.
///
/// For version 0, delegates to [`read_batches_at_version`] (full snapshot).
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` if the version cannot be loaded or read.
///
/// Returns `(batches, fully_consumed)` — see [`read_batches_at_version`].
#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)]
pub async fn read_version_diff(
    table: &mut DeltaTable,
    version: i64,
    max_records: usize,
    partition_filter: Option<&str>,
) -> Result<(Vec<RecordBatch>, bool), ConnectorError> {
    // Maximum file size (256 MB) for direct in-memory Parquet reads.
    // Files larger than this fall back to DataFusion's streaming scan.
    const MAX_DIRECT_READ_BYTES: u64 = 256 * 1024 * 1024;

    // For version 0, read the full snapshot (no previous version to diff).
    if version <= 0 {
        return read_batches_at_version(table, version, max_records).await;
    }

    // Read the commit JSON via delta-rs's LogStore API (handles path
    // resolution, checkpoints, and retries correctly).
    let log_store = table.log_store();
    let store = log_store.object_store(None);

    let commit_data = log_store
        .read_commit_entry(version)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("read commit {version}: {e}")))?
        .ok_or_else(|| {
            ConnectorError::ReadError(format!(
                "version {version} not available (cleaned up or never existed)"
            ))
        })?;
    let commit_str = std::str::from_utf8(&commit_data)
        .map_err(|e| ConnectorError::ReadError(format!("commit log is not valid UTF-8: {e}")))?;

    // Each line in the commit JSON is a separate action object.
    // Collect both add and remove actions to compute the net-new files.
    let mut added_paths = Vec::new();
    let mut removed_paths = std::collections::HashSet::new();
    for line in commit_str.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Ok(obj) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(add) = obj.get("add") {
                if let Some(path) = add.get("path").and_then(|p| p.as_str()) {
                    added_paths.push(decode_delta_path(path));
                }
            }
            if let Some(remove) = obj.get("remove") {
                if let Some(path) = remove.get("path").and_then(|p| p.as_str()) {
                    removed_paths.insert(decode_delta_path(path));
                }
            }
        }
    }

    // Exclude any added file whose path also appears in a remove action.
    added_paths.retain(|p| !removed_paths.contains(p));

    if added_paths.is_empty() {
        debug!(
            version,
            num_removed = removed_paths.len(),
            "Delta Lake: no net-new add actions in version"
        );
        return Ok((Vec::new(), true));
    }

    debug!(
        version,
        num_added_files = added_paths.len(),
        num_removed_files = removed_paths.len(),
        "Delta Lake: reading added files"
    );

    // Load the version so we have the correct schema.
    table
        .load_version(version)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("failed to load version {version}: {e}")))?;

    let table_schema = table
        .snapshot()
        .map(|s| s.snapshot().arrow_schema())
        .map_err(|e| ConnectorError::ReadError(format!("no snapshot at version {version}: {e}")))?;

    // Filter file paths by partition predicate if provided.
    // Supports simple Hive-style equality: "col = 'val'" matches "col=val/" in path.
    let added_paths = if let Some(filter) = partition_filter {
        filter_paths_by_partition(&added_paths, filter)
    } else {
        added_paths
    };

    // Read each added Parquet file as raw bytes via delta-rs's object_store,
    // then parse with parquet's in-memory ArrowReaderBuilder (avoids the
    // object_store 0.12 vs 0.13 version mismatch).
    let mut batches = Vec::new();
    let mut total_rows: usize = 0;

    for file_path in &added_paths {
        if total_rows >= max_records {
            break;
        }

        let obj_path = deltalake::Path::from(file_path.as_str());

        // Check file size before downloading. Large files fall back to
        // DataFusion scan to avoid OOM on multi-GB Parquet files.
        let file_meta = store
            .head(&obj_path)
            .await
            .map_err(|e| ConnectorError::ReadError(format!("failed to stat '{file_path}': {e}")))?;
        if file_meta.size > MAX_DIRECT_READ_BYTES {
            warn!(
                file_path,
                file_size = file_meta.size,
                "file too large for direct read, falling back to DataFusion scan"
            );
            return read_batches_at_version(table, version, max_records).await;
        }

        let file_bytes = get_with_retry(&store, &obj_path, file_path).await?;

        let parquet_reader =
            deltalake::parquet::arrow::arrow_reader::ArrowReaderBuilder::try_new(file_bytes)
                .map_err(|e| {
                    ConnectorError::ReadError(format!(
                        "failed to open Parquet file '{file_path}': {e}"
                    ))
                })?;

        // Read one extra row to probe whether the version is fully consumed.
        let remaining = max_records.saturating_sub(total_rows).saturating_add(1);
        let reader = parquet_reader.with_limit(remaining).build().map_err(|e| {
            ConnectorError::ReadError(format!("failed to build reader for '{file_path}': {e}"))
        })?;

        for result in reader {
            let batch: RecordBatch = result.map_err(|e| {
                ConnectorError::ReadError(format!("Parquet read error in '{file_path}': {e}"))
            })?;
            if batch.num_rows() == 0 {
                continue;
            }

            // Align the batch schema to the table schema (added files may
            // predate schema evolution and have fewer columns).
            let batch = if batch.schema() == table_schema {
                batch
            } else {
                align_batch_to_schema(&batch, &table_schema)?
            };

            total_rows += batch.num_rows();
            batches.push(batch);

            if total_rows >= max_records {
                break;
            }
        }
    }

    // We probed one extra row per file. If total_rows > max_records, there's
    // more data — trim the excess and report not fully consumed.
    let fully_consumed = total_rows <= max_records;
    if !fully_consumed {
        // Trim the last batch to remove the probe row(s).
        let excess = total_rows - max_records;
        let len = batches.len();
        if len > 0 {
            let last = &batches[len - 1];
            if last.num_rows() > excess {
                batches[len - 1] = last.slice(0, last.num_rows() - excess);
            } else {
                batches.pop();
            }
        }
    }

    debug!(
        version,
        num_batches = batches.len(),
        fully_consumed,
        num_added_files = added_paths.len(),
        "Delta Lake: read version diff"
    );

    Ok((batches, fully_consumed))
}

/// Reads a file from `object_store` with retry (3x, exponential backoff).
/// Does not retry 404s.
#[cfg(feature = "delta-lake")]
async fn get_with_retry(
    store: &Arc<dyn deltalake::ObjectStore>,
    path: &deltalake::Path,
    display_path: &str,
) -> Result<bytes::Bytes, ConnectorError> {
    let backoff = [200u64, 1000, 4000];
    let mut last_err = None;

    for attempt in 0..=backoff.len() {
        match store.get(path).await {
            Ok(result) => {
                return result.bytes().await.map_err(|e| {
                    ConnectorError::ReadError(format!(
                        "failed to read bytes of '{display_path}': {e}"
                    ))
                });
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found") || msg.contains("404") {
                    return Err(ConnectorError::ReadError(format!(
                        "file not found '{display_path}': {e}"
                    )));
                }
                if let Some(&delay) = backoff.get(attempt) {
                    warn!(
                        attempt = attempt + 1,
                        delay_ms = delay,
                        error = %e,
                        path = display_path,
                        "object_store read failed, retrying"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                last_err = Some(e);
            }
        }
    }

    Err(ConnectorError::ReadError(format!(
        "failed to read '{display_path}' after {} attempts: {}",
        backoff.len() + 1,
        last_err.map_or_else(|| "unknown".to_string(), |e| e.to_string())
    )))
}

/// Filters file paths by a Hive-style partition predicate.
///
/// Supports simple equality predicates: `col = 'val'` matches paths
/// containing `col=val/`. Multiple predicates joined by `AND` are all
/// required to match. Predicates that can't be parsed are ignored
/// (all paths pass through).
#[cfg(feature = "delta-lake")]
fn filter_paths_by_partition(paths: &[String], filter: &str) -> Vec<String> {
    // Parse simple "col = 'val'" or "col = val" predicates from AND-joined expressions.
    let mut required_segments: Vec<String> = Vec::new();
    for clause in filter
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .split(" AND ")
    {
        let clause = clause.trim();
        if let Some((col, val)) = clause.split_once('=') {
            let col = col.trim();
            let val = val.trim().trim_matches('\'').trim_matches('"');
            if !col.is_empty() && !val.is_empty() {
                required_segments.push(format!("{col}={val}"));
            }
        }
    }

    if required_segments.is_empty() {
        return paths.to_vec();
    }

    paths
        .iter()
        .filter(|path| required_segments.iter().all(|seg| path.contains(seg)))
        .cloned()
        .collect()
}

/// Percent-decodes a file path from a Delta Lake commit JSON.
///
/// Delta Lake spec requires paths in `add`/`remove` actions to be
/// percent-encoded (e.g., `part%3D1/file.parquet` for `part=1/file.parquet`).
#[cfg(feature = "delta-lake")]
fn decode_delta_path(encoded: &str) -> String {
    url::Url::parse(&format!("file:///{encoded}")).map_or_else(
        |_| encoded.to_string(),
        |u| {
            let p = u.path();
            p.strip_prefix('/').unwrap_or(p).to_string()
        },
    )
}

/// Aligns a `RecordBatch` to a target schema by adding null columns for
/// missing fields. Used when reading Parquet files that predate schema
/// evolution (fewer columns than the current table schema).
#[cfg(feature = "delta-lake")]
fn align_batch_to_schema(
    batch: &RecordBatch,
    target_schema: &SchemaRef,
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::new_null_array;

    let mut columns = Vec::with_capacity(target_schema.fields().len());
    for field in target_schema.fields() {
        if let Ok(col_idx) = batch.schema().index_of(field.name()) {
            columns.push(batch.column(col_idx).clone());
        } else {
            columns.push(new_null_array(field.data_type(), batch.num_rows()));
        }
    }

    RecordBatch::try_new(target_schema.clone(), columns).map_err(|e| {
        ConnectorError::ReadError(format!("failed to align batch to table schema: {e}"))
    })
}

/// Reads CDF batches for a version range via `scan_cdf()`.
///
/// `scan_cdf(self)` consumes the `DeltaTable` — caller must re-open afterward.
/// Output includes `_change_type`, `_commit_version`, `_commit_timestamp`.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` on scan failure.
#[cfg(feature = "delta-lake")]
pub async fn read_cdf_batches(
    table: DeltaTable,
    start_version: i64,
    end_version: i64,
) -> Result<Vec<RecordBatch>, ConnectorError> {
    use datafusion::prelude::SessionContext;
    use tokio_stream::StreamExt;

    debug!(start_version, end_version, "reading CDF batches");

    let ctx = SessionContext::new();

    // Clone session state so the RwLockReadGuard is dropped before await.
    let session_state = ctx.state();

    let cdf_builder = table
        .scan_cdf()
        .with_starting_version(start_version)
        .with_ending_version(end_version);

    let plan = cdf_builder
        .build(&session_state, None)
        .await
        .map_err(|e| ConnectorError::ReadError(format!("CDF scan build failed: {e}")))?;

    // Execute the plan via DataFusion to get record batches.
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
        .map_err(|e| ConnectorError::ReadError(format!("CDF stream execution failed: {e}")))?;

    let mut batches = Vec::new();
    while let Some(result) = stream.next().await {
        let batch: RecordBatch = result
            .map_err(|e| ConnectorError::ReadError(format!("CDF stream batch failed: {e}")))?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }

    debug!(
        start_version,
        end_version,
        num_batches = batches.len(),
        "CDF scan complete"
    );

    Ok(batches)
}

/// Maps CDF `_change_type` → `_op` (`I`/`U`/`D`), drops `update_preimage`
/// rows and CDF metadata columns (`_change_type`, `_commit_version`,
/// `_commit_timestamp`). Returns `None` if all rows were preimages.
///
/// # Errors
///
/// Returns `ConnectorError::ReadError` on Arrow operation failure.
#[cfg(feature = "delta-lake")]
pub fn map_cdf_to_changelog(batch: &RecordBatch) -> Result<Option<RecordBatch>, ConnectorError> {
    use arrow_array::StringArray;

    let schema = batch.schema();
    let Ok(ct_idx) = schema.index_of("_change_type") else {
        return Ok(Some(batch.clone()));
    };

    let change_type = batch
        .column(ct_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| ConnectorError::ReadError("_change_type is not Utf8".into()))?;

    // Build filter (drop preimage rows) and mapped _op values in one pass.
    let (keep, ops): (Vec<bool>, Vec<Option<&str>>) = (0..batch.num_rows())
        .map(|i| match change_type.value(i) {
            "update_postimage" => (true, Some("U")),
            "delete" => (true, Some("D")),
            "update_preimage" => (false, Some("")),
            _ => (true, Some("I")), // insert + unknown → I
        })
        .unzip();

    let filter = arrow_array::BooleanArray::from(keep);
    let filtered = arrow_select::filter::filter_record_batch(batch, &filter)
        .map_err(|e| ConnectorError::ReadError(format!("CDF filter failed: {e}")))?;
    if filtered.num_rows() == 0 {
        return Ok(None);
    }

    // Build _op column from filtered ops.
    let op_arr: StringArray = ops.into_iter().collect();
    let op_filtered = arrow_select::filter::filter(&op_arr, &filter)
        .map_err(|e| ConnectorError::ReadError(format!("CDF op filter: {e}")))?;

    // Rebuild batch: keep user columns, drop CDF metadata, append _op.
    let cdf_meta = ["_change_type", "_commit_version", "_commit_timestamp"];
    let mut fields = Vec::new();
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    for (i, field) in filtered.schema().fields().iter().enumerate() {
        if !cdf_meta.contains(&field.name().as_str()) {
            fields.push(field.clone());
            columns.push(filtered.column(i).clone());
        }
    }
    fields.push(Arc::new(arrow_schema::Field::new(
        "_op",
        arrow_schema::DataType::Utf8,
        false,
    )));
    columns.push(op_filtered);

    RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map(Some)
        .map_err(|e| ConnectorError::ReadError(format!("CDF batch rebuild: {e}")))
}

/// Result of a MERGE (upsert) operation.
#[cfg(feature = "delta-lake")]
#[derive(Debug)]
pub struct MergeResult {
    /// Number of rows inserted.
    pub rows_inserted: usize,
    /// Number of rows updated.
    pub rows_updated: usize,
    /// Number of rows deleted.
    pub rows_deleted: usize,
}

/// Atomic changelog MERGE: inserts, updates, and deletes in one Delta commit.
///
/// The source batch must contain an `_op` column (Utf8) with values:
/// - `"I"`, `"U"`, `"r"` → upsert (update if matched, insert if not)
/// - `"D"` → delete matched rows
///
/// Columns prefixed with `_` are excluded from SET clauses but remain
/// in the source `DataFrame` for predicate filtering.
///
/// # Errors
///
/// Returns `ConnectorError::WriteError` if the merge fails.
#[cfg(feature = "delta-lake")]
#[allow(clippy::too_many_lines)]
pub(crate) async fn merge_changelog(
    table: DeltaTable,
    source_batch: RecordBatch,
    key_columns: &[String],
    schema_evolution: bool,
    writer_properties: Option<deltalake::parquet::file::properties::WriterProperties>,
    ctx: &datafusion::prelude::SessionContext,
) -> Result<(DeltaTable, MergeResult), DeltaWriteAttemptError> {
    use datafusion::prelude::*;
    use deltalake::kernel::transaction::CommitProperties;

    const CDC_COLUMNS: &[&str] = &["_op", "_ts_ms"];

    if source_batch.num_rows() == 0 {
        return Ok((
            table,
            MergeResult {
                rows_inserted: 0,
                rows_updated: 0,
                rows_deleted: 0,
            },
        ));
    }

    debug!(
        key_columns = ?key_columns,
        source_rows = source_batch.num_rows(),
        "performing atomic changelog MERGE"
    );

    let source_df = ctx.read_batch(source_batch).map_err(|e| {
        ConnectorError::WriteError(format!("failed to create source DataFrame: {e}"))
    })?;

    // Join predicate: target.k1 = source.k1 AND ...
    let predicate = key_columns
        .iter()
        .map(|k| col(format!("target.{k}")).eq(col(format!("source.{k}"))))
        .reduce(Expr::and)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError("merge requires at least one key column".into())
        })?;

    let source_schema = source_df.schema().clone();
    let key_set: std::collections::HashSet<&str> = key_columns.iter().map(String::as_str).collect();

    // Exclude CDC metadata columns from SET clauses (preserve user columns like _id).
    let all_user_columns: Vec<String> = source_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|name| !CDC_COLUMNS.contains(&name.as_str()))
        .collect();

    let non_key_user_columns: Vec<String> = all_user_columns
        .iter()
        .filter(|c| !key_set.contains(c.as_str()))
        .cloned()
        .collect();

    // Predicates for conditional clause execution.
    let upsert_pred = col("source._op").in_list(vec![lit("I"), lit("U"), lit("r")], false);
    let delete_pred = col("source._op").eq(lit("D"));

    let non_key_for_update = non_key_user_columns;
    let all_for_insert = all_user_columns;

    let mut merge_builder = table
        .merge(source_df, predicate)
        .with_source_alias("source")
        .with_target_alias("target")
        .with_commit_properties(CommitProperties::default())
        .when_matched_update(|update| {
            let mut u = update.predicate(upsert_pred.clone());
            for col_name in &non_key_for_update {
                u = u.update(col_name.as_str(), col(format!("source.{col_name}")));
            }
            u
        })
        .map_err(|e| ConnectorError::WriteError(format!("merge matched-update failed: {e}")))?
        .when_matched_delete(|delete| delete.predicate(delete_pred))
        .map_err(|e| ConnectorError::WriteError(format!("merge matched-delete failed: {e}")))?
        .when_not_matched_insert(|insert| {
            let mut ins = insert.predicate(upsert_pred);
            for col_name in &all_for_insert {
                ins = ins.set(col_name.as_str(), col(format!("source.{col_name}")));
            }
            ins
        })
        .map_err(|e| ConnectorError::WriteError(format!("merge not-matched-insert failed: {e}")))?;

    if schema_evolution {
        merge_builder = merge_builder.with_merge_schema(true);
    }

    if let Some(props) = writer_properties {
        merge_builder = merge_builder.with_writer_properties(props);
    }

    let (table, metrics) = merge_builder.await.map_err(DeltaWriteAttemptError::Delta)?;

    let result = MergeResult {
        rows_inserted: metrics.num_target_rows_inserted,
        rows_updated: metrics.num_target_rows_updated,
        rows_deleted: metrics.num_target_rows_deleted,
    };

    info!(
        rows_inserted = result.rows_inserted,
        rows_updated = result.rows_updated,
        rows_deleted = result.rows_deleted,
        "Delta Lake changelog MERGE complete"
    );

    Ok((table, result))
}

/// Resolves catalog-aware table URI and merges catalog-specific storage options.
///
/// - `None`: returns table path and storage options as-is.
/// - `Glue`: calls AWS Glue API to resolve the table's S3 location.
/// - `Unity`: injects workspace URL and access token into storage options.
///
/// # Errors
///
/// Returns `ConnectorError` if catalog resolution fails.
#[cfg(feature = "delta-lake")]
#[allow(clippy::implicit_hasher, clippy::unused_async)]
pub async fn resolve_catalog_options(
    catalog: &super::delta_config::DeltaCatalogType,
    #[allow(unused_variables)] catalog_database: Option<&str>,
    #[allow(unused_variables)] catalog_name: Option<&str>,
    _catalog_schema: Option<&str>,
    table_path: &str,
    base_storage_options: &HashMap<String, String>,
) -> Result<(String, HashMap<String, String>), ConnectorError> {
    use super::delta_config::DeltaCatalogType;

    match catalog {
        DeltaCatalogType::None => Ok((table_path.to_string(), base_storage_options.clone())),
        #[cfg(feature = "delta-lake-glue")]
        DeltaCatalogType::Glue => {
            use deltalake::DataCatalog;
            let database = catalog_database.ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "Glue catalog requires 'catalog.database'".into(),
                )
            })?;
            let glue = deltalake_catalog_glue::GlueDataCatalog::from_env()
                .await
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to init Glue catalog: {e}"))
                })?;
            let resolved = glue
                .get_table_storage_location(catalog_name.map(String::from), database, table_path)
                .await
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!(
                        "Glue catalog lookup failed for '{database}.{table_path}': {e}"
                    ))
                })?;
            info!(
                glue_database = database,
                table = table_path,
                resolved_path = %resolved,
                "resolved table path via Glue catalog"
            );
            Ok((resolved, base_storage_options.clone()))
        }
        #[cfg(not(feature = "delta-lake-glue"))]
        DeltaCatalogType::Glue => Err(ConnectorError::ConfigurationError(
            "Glue catalog requires the 'delta-lake-glue' feature. \
             Build with: cargo build --features delta-lake-glue"
                .into(),
        )),
        #[cfg(feature = "delta-lake-unity")]
        DeltaCatalogType::Unity {
            workspace_url,
            access_token,
        } => {
            // Resolve the table's actual storage location from Unity Catalog
            // via REST API, then return that direct path (s3://, az://, gs://)
            // instead of the uc:// URI. This bypasses delta-rs's built-in
            // uc:// handling which requires credential vending — a feature
            // that is denied outside Databricks compute environments.
            let full_name = table_path.strip_prefix("uc://").unwrap_or(table_path);

            let storage_location = super::unity_catalog::get_table_storage_location(
                workspace_url,
                access_token,
                full_name,
            )
            .await?;

            Ok((storage_location, base_storage_options.clone()))
        }
        #[cfg(not(feature = "delta-lake-unity"))]
        DeltaCatalogType::Unity { .. } => Err(ConnectorError::ConfigurationError(
            "Unity catalog requires the 'delta-lake-unity' feature. \
             Build with: cargo build --features delta-lake-unity"
                .into(),
        )),
    }
}

// ============================================================================
// Integration tests (require delta-lake feature)
// ============================================================================

#[cfg(all(test, feature = "delta-lake"))]
mod tests;
