//! Admitted schema validation, changelog normalization, and retained-batch accounting.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use crate::changelog::collapse_changelog;
use crate::error::ConnectorError;

use super::super::sink_config::{
    quote_sql_identifier, validate_sql_identifier, PostgresSinkConfig,
};
use super::super::types::postgres_type;
#[cfg(feature = "postgres-sink")]
use super::super::types::{arrow_column_to_pg_array, validate_postgres_array_values};
#[cfg(feature = "postgres-sink")]
use super::postgres_dispatched_write_error;
use super::PostgresSink;

const CHANGELOG_METADATA_COLUMNS: &[&str] = &["_op", "_ts_ms", "__weight"];

impl PostgresSink {
    /// Splits a changelog `RecordBatch` into insert and delete batches.
    ///
    /// Uses the `_op` metadata column:
    /// - `"I"` (insert), `"U"` (update-after), `"r"` (snapshot read) → insert batch
    /// - `"D"` (delete) → delete batch
    ///
    /// The returned batches exclude the exact changelog metadata field set.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the `_op` column is
    /// missing or not a string type.
    pub fn split_changelog_batch(
        batch: &RecordBatch,
    ) -> Result<(RecordBatch, RecordBatch), ConnectorError> {
        let op_idx = batch.schema().index_of("_op").map_err(|_| {
            ConnectorError::ConfigurationError(
                "changelog mode requires '_op' column in input schema".into(),
            )
        })?;

        let op_array = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("'_op' column must be String (Utf8) type".into())
            })?;

        validate_operation_values(op_array)?;

        let mut insert_indices = Vec::new();
        let mut delete_indices = Vec::new();

        for i in 0..op_array.len() {
            match op_array.value(i) {
                "I" | "U" | "U+" | "R" | "r" => {
                    insert_indices.push(u32::try_from(i).map_err(|_| {
                        ConnectorError::Internal(
                            "PostgreSQL changelog batch exceeds UInt32 row indexing".into(),
                        )
                    })?);
                }
                "D" | "U-" => {
                    delete_indices.push(u32::try_from(i).map_err(|_| {
                        ConnectorError::Internal(
                            "PostgreSQL changelog batch exceeds UInt32 row indexing".into(),
                        )
                    })?);
                }
                _ => unreachable!("operation values validated above"),
            }
        }

        let insert_batch = filter_batch_by_indices(batch, &insert_indices)?;
        let delete_batch = filter_batch_by_indices(batch, &delete_indices)?;

        Ok((insert_batch, delete_batch))
    }
}

fn is_changelog_metadata(name: &str) -> bool {
    CHANGELOG_METADATA_COLUMNS.contains(&name)
}

pub(super) fn quoted_user_columns(schema: &SchemaRef) -> Vec<String> {
    user_fields(schema)
        .iter()
        .map(|field| quote_sql_identifier(field.name()))
        .collect()
}

/// Returns writable fields, excluding only the defined changelog metadata columns.
pub(super) fn user_fields(schema: &SchemaRef) -> Vec<&Arc<Field>> {
    schema
        .fields()
        .iter()
        .filter(|field| !is_changelog_metadata(field.name()))
        .collect()
}

/// Builds a schema containing only user-visible columns.
pub(super) fn build_user_schema(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|field| !is_changelog_metadata(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

pub(super) fn validate_sink_schema(
    schema: &SchemaRef,
    config: &PostgresSinkConfig,
) -> Result<(), ConnectorError> {
    config.validate()?;
    let fields = user_fields(schema);
    if fields.is_empty() {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL sink schema has no writable columns".into(),
        ));
    }

    let mut names = std::collections::HashSet::new();
    for field in schema.fields() {
        validate_sql_identifier(field.name(), "PostgreSQL sink column name")?;
        if !names.insert(field.name()) {
            return Err(ConnectorError::ConfigurationError(format!(
                "PostgreSQL sink schema contains duplicate column '{}'",
                field.name()
            )));
        }
        if !is_changelog_metadata(field.name()) {
            postgres_type(field.data_type()).map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "PostgreSQL sink column '{}': {error}",
                    field.name()
                ))
            })?;
        }
    }

    let op = schema.field_with_name("_op").ok();
    let weight = schema.field_with_name("__weight").ok();
    let timestamp = schema.field_with_name("_ts_ms").ok();
    if config.changelog_mode {
        if op.is_none() && weight.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL changelog mode requires an Utf8 '_op' or Int64 '__weight' column"
                    .into(),
            ));
        }
    } else if op.is_some() || weight.is_some() || timestamp.is_some() {
        return Err(ConnectorError::ConfigurationError(
            "PostgreSQL sink '_op', '_ts_ms', and '__weight' columns require changelog.mode=true"
                .into(),
        ));
    }
    if let Some(field) = op {
        if field.data_type() != &DataType::Utf8 || field.is_nullable() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL changelog '_op' must be non-null Utf8".into(),
            ));
        }
    }
    if let Some(field) = weight {
        if field.data_type() != &DataType::Int64 || field.is_nullable() {
            return Err(ConnectorError::ConfigurationError(
                "PostgreSQL changelog '__weight' must be non-null Int64".into(),
            ));
        }
    }

    for primary_key in &config.primary_key_columns {
        let field = schema.field_with_name(primary_key).map_err(|_| {
            ConnectorError::ConfigurationError(format!(
                "primary key column '{primary_key}' is not present in PostgreSQL sink schema"
            ))
        })?;
        if is_changelog_metadata(primary_key) {
            return Err(ConnectorError::ConfigurationError(format!(
                "primary key column '{primary_key}' is reserved changelog metadata"
            )));
        }
        if field.is_nullable() {
            return Err(ConnectorError::ConfigurationError(format!(
                "primary key column '{primary_key}' must be non-nullable"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "postgres-sink")]
pub(super) fn validate_input_batch(
    batch: &RecordBatch,
    expected_schema: &SchemaRef,
    config: &PostgresSinkConfig,
) -> Result<(), ConnectorError> {
    if batch.schema().as_ref() != expected_schema.as_ref() {
        return Err(ConnectorError::SchemaMismatch(
            "PostgreSQL sink batch schema does not match its admitted Arrow schema".into(),
        ));
    }
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !is_changelog_metadata(field.name()) {
            validate_postgres_array_values(column.as_ref())?;
        }
    }
    for primary_key in &config.primary_key_columns {
        let index = batch.schema().index_of(primary_key).map_err(|_| {
            ConnectorError::SchemaMismatch(format!(
                "primary key column '{primary_key}' is absent from PostgreSQL sink batch"
            ))
        })?;
        if batch.column(index).null_count() != 0 {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL primary key column '{primary_key}' contains NULL"
            )));
        }
    }
    if config.changelog_mode {
        validate_changelog_input(batch)?;
    }
    Ok(())
}

fn validate_operation_values(operations: &arrow_array::StringArray) -> Result<(), ConnectorError> {
    for row in 0..operations.len() {
        if operations.is_null(row) {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL changelog '_op' is NULL at row {row}"
            )));
        }
        let operation = operations.value(row);
        if !matches!(operation, "I" | "U" | "U+" | "R" | "r" | "D" | "U-") {
            return Err(ConnectorError::SchemaMismatch(format!(
                "PostgreSQL changelog operation '{operation}' at row {row} is not supported"
            )));
        }
    }
    Ok(())
}

pub(super) fn validate_changelog_input(batch: &RecordBatch) -> Result<(), ConnectorError> {
    let schema = batch.schema();
    let mut encoding_found = false;
    if let Ok(index) = schema.index_of("_op") {
        encoding_found = true;
        let operations = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("PostgreSQL changelog '_op' must be Utf8".into())
            })?;
        validate_operation_values(operations)?;
    }
    if let Ok(index) = schema.index_of("__weight") {
        encoding_found = true;
        let weights = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "PostgreSQL changelog '__weight' must be Int64".into(),
                )
            })?;
        if weights.null_count() != 0 {
            return Err(ConnectorError::SchemaMismatch(
                "PostgreSQL changelog '__weight' contains NULL".into(),
            ));
        }
    }
    if !encoding_found {
        return Err(ConnectorError::ConfigurationError(
            "PostgreSQL changelog input requires '_op' or '__weight'".into(),
        ));
    }
    Ok(())
}

/// Filters a `RecordBatch` to include only rows at the given indices.
///
/// Also strips the exact changelog metadata field set from the output.
fn filter_batch_by_indices(
    batch: &RecordBatch,
    indices: &[u32],
) -> Result<RecordBatch, ConnectorError> {
    if indices.is_empty() {
        let user_schema = Arc::new(Schema::new(
            batch
                .schema()
                .fields()
                .iter()
                .filter(|field| !is_changelog_metadata(field.name()))
                .cloned()
                .collect::<Vec<_>>(),
        ));
        return Ok(RecordBatch::new_empty(user_schema));
    }

    let indices_array = arrow_array::UInt32Array::from(indices.to_vec());

    let user_schema = Arc::new(Schema::new(
        batch
            .schema()
            .fields()
            .iter()
            .filter(|field| !is_changelog_metadata(field.name()))
            .cloned()
            .collect::<Vec<_>>(),
    ));

    let filtered_columns: Vec<Arc<dyn arrow_array::Array>> = batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !is_changelog_metadata(field.name()))
        .map(|(i, _)| {
            arrow_select::take::take(batch.column(i), &indices_array, None)
                .map_err(|e| ConnectorError::Internal(format!("arrow take failed: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(user_schema, filtered_columns)
        .map_err(|e| ConnectorError::Internal(format!("batch construction failed: {e}")))
}

/// Strips only the defined changelog metadata columns from a `RecordBatch`.
pub(super) fn strip_metadata_columns(batch: &RecordBatch) -> Result<RecordBatch, ConnectorError> {
    let schema = batch.schema();
    let user_indices: Vec<usize> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| !is_changelog_metadata(field.name()))
        .map(|(i, _)| i)
        .collect();

    if user_indices.len() == schema.fields().len() {
        return Ok(batch.clone());
    }

    let user_schema = Arc::new(Schema::new(
        user_indices
            .iter()
            .map(|&i| schema.field(i).clone())
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<Arc<dyn Array>> = user_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();

    RecordBatch::try_new(user_schema, columns)
        .map_err(|e| ConnectorError::Internal(format!("strip metadata: {e}")))
}

/// Collapse one ordinary upsert flush to the last-arriving row per configured
/// key, then remove the normalized changelog marker before binding UNNEST
/// parameters. `PostgreSQL` rejects duplicate conflict keys in one statement.
pub(super) fn collapse_upsert_batch(
    batch: &RecordBatch,
    primary_key_columns: &[String],
) -> Result<RecordBatch, ConnectorError> {
    let collapsed = collapse_changelog(batch, primary_key_columns)?;
    strip_metadata_columns(&collapsed)
}

/// Executes an UNNEST-based INSERT/UPSERT using Arrow column arrays as parameters.
#[cfg(feature = "postgres-sink")]
pub(super) async fn execute_unnest<C>(
    client: &C,
    sql: &str,
    batch: &RecordBatch,
) -> Result<u64, ConnectorError>
where
    C: tokio_postgres::GenericClient + Sync,
{
    let params: Vec<Box<dyn postgres_types::ToSql + Sync + Send>> = (0..batch.num_columns())
        .map(|i| arrow_column_to_pg_array(batch.column(i)))
        .collect::<Result<_, _>>()?;

    let param_refs: Vec<&(dyn postgres_types::ToSql + Sync)> = params
        .iter()
        .map(|p| p.as_ref() as &(dyn postgres_types::ToSql + Sync))
        .collect();

    client
        .execute(sql, &param_refs)
        .await
        .map_err(|error| postgres_dispatched_write_error("UNNEST execute", &error))
}

#[cfg(any(feature = "postgres-sink", test))]
pub(super) fn retained_batch_bytes(batch: &RecordBatch) -> usize {
    let batch_overhead = std::mem::size_of::<RecordBatch>().saturating_add(
        batch
            .num_columns()
            .saturating_mul(std::mem::size_of::<Arc<dyn Array>>()),
    );
    batch
        .columns()
        .iter()
        .fold(batch_overhead, |total, column| {
            total.saturating_add(column.get_array_memory_size())
        })
}

#[cfg(feature = "postgres-sink")]
pub(super) fn retained_batch_bytes_u64(batch: &RecordBatch) -> u64 {
    u64::try_from(retained_batch_bytes(batch)).unwrap_or(u64::MAX)
}

/// Validates a single batch before mutation, then reports whether already-buffered input must be
/// flushed before admission. Equality is allowed; addition fails closed on overflow.
#[cfg(any(feature = "postgres-sink", test))]
pub(super) fn requires_preflush(
    buffered_bytes: usize,
    incoming_bytes: usize,
    retained_limit: usize,
) -> Result<bool, ConnectorError> {
    if incoming_bytes > retained_limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "PostgreSQL sink input batch retains {incoming_bytes} bytes, exceeding the fixed \
             {retained_limit}-byte per-sink buffer limit; split the batch upstream"
        )));
    }

    Ok(buffered_bytes
        .checked_add(incoming_bytes)
        .is_none_or(|total| total > retained_limit))
}
