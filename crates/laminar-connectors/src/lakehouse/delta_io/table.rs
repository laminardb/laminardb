//! Table opening, direct writes, and durable coordinated cursor reads.

#[cfg(feature = "delta-lake")]
use std::sync::Arc;

use super::{
    debug, info, CommitProperties, ConnectorError, CoordinatedCommitCursor, DeltaTable,
    DeltaWriteAttemptError, HashMap, RecordBatch, SaveMode, SchemaMode, SchemaRef, StorageProvider,
    Url, SET_TRANSACTION_RETENTION,
};
#[cfg(feature = "delta-lake")]
use arrow_schema::{DataType, Schema, TimeUnit};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
#[cfg(feature = "delta-lake")]
use deltalake::kernel::schema::cast::cast_record_batch;

/// Widens top-level millisecond timestamp columns to microseconds.
///
/// Delta Lake physically stores microseconds and its kernel rejects Arrow
/// `Timestamp(Millisecond, _)` during schema conversion (there is no
/// schema-level normalizer upstream). Engine outputs that model time in
/// milliseconds — for example temporal-probe `probe_time` — are widened once
/// at this storage boundary; values scale by `1_000` and remain
/// instant-identical. Timezone metadata is preserved. Nested timestamps
/// inside composite types are not rewritten and keep failing conversion,
/// matching kernel behavior.
#[cfg(feature = "delta-lake")]
fn widen_millisecond_timestamps(schema: &SchemaRef) -> SchemaRef {
    let needs_widening = schema.fields().iter().any(|field| {
        matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        )
    });
    if !needs_widening {
        return Arc::clone(schema);
    }
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Timestamp(TimeUnit::Millisecond, tz) => Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            ),
            _ => Arc::clone(field),
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Widens one batch toward its widened schema using the kernel cast kernel
/// (`cast_record_batch`, the same mechanism delta-rs applies in its own
/// `DataFusion` sink). Strict and no column addition: schema validation is not
/// weakened, and a timestamp overflow surfaces as a typed write failure.
#[cfg(feature = "delta-lake")]
fn widen_batch_millisecond_timestamps(batch: RecordBatch) -> Result<RecordBatch, ConnectorError> {
    let target = widen_millisecond_timestamps(&batch.schema());
    if Arc::ptr_eq(&target, &batch.schema()) {
        return Ok(batch);
    }
    cast_record_batch(&batch, target, false, false).map_err(|error| {
        ConnectorError::SchemaMismatch(format!("millisecond timestamp widening failed: {error}"))
    })
}

/// Converts a path string to a URL.
#[cfg(feature = "delta-lake")]
pub(super) fn path_to_url(path: &str) -> Result<Url, ConnectorError> {
    // If it already looks like a URL, parse it directly.
    if path.contains("://") {
        Url::parse(&StorageProvider::canonical_uri(path))
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

    // Convert Arrow schema to Delta Lake schema using TryIntoKernel, widening
    // millisecond timestamps to the microseconds Delta physically stores.
    let delta_schema: deltalake::kernel::StructType = widen_millisecond_timestamps(schema)
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

    let batches = batches
        .into_iter()
        .map(widen_batch_millisecond_timestamps)
        .collect::<Result<Vec<_>, _>>()
        .map_err(DeltaWriteAttemptError::Local)?;

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
pub(super) fn coordinated_transaction_ids(external_key: &str) -> (String, String) {
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
