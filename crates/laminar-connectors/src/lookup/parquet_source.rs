//! Parquet file lookup source (F-LOOKUP-008).
//!
//! Loads a Parquet file into an in-memory `HashMap` at construction time,
//! providing a [`LookupSource`] implementation suitable for static or
//! slowly-changing dimension tables stored as Parquet.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use laminar_connectors::lookup::parquet_source::{ParquetLookupSource, ParquetLookupSourceConfig};
//!
//! let config = ParquetLookupSourceConfig {
//!     path: "/data/customers.parquet".into(),
//!     primary_key_columns: vec!["customer_id".into()],
//!     batch_size: 8192,
//! };
//!
//! let source = ParquetLookupSource::from_path(config).unwrap();
//! ```

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_cast::display::ArrayFormatter;
use arrow_schema::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};

/// Configuration for [`ParquetLookupSource`].
#[derive(Debug, Clone)]
pub struct ParquetLookupSourceConfig {
    /// Path to the Parquet file (local filesystem).
    pub path: String,
    /// Column names that form the primary key.
    pub primary_key_columns: Vec<String>,
    /// Batch size for reading the Parquet file (default: 8192).
    pub batch_size: usize,
}

impl Default for ParquetLookupSourceConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            primary_key_columns: Vec::new(),
            batch_size: 8192,
        }
    }
}

/// A [`LookupSource`] that loads a Parquet file into memory.
///
/// All data is loaded eagerly at construction time. Lookups are served
/// from an in-memory `HashMap` keyed by the serialized primary key
/// columns. The values are serialized Arrow IPC row bytes.
///
/// This is appropriate for dimension tables that fit in memory and are
/// read-mostly or static.
pub struct ParquetLookupSource {
    config: ParquetLookupSourceConfig,
    /// Primary key bytes → serialized row bytes.
    data: HashMap<Vec<u8>, Vec<u8>>,
    /// Schema of the loaded Parquet file.
    schema: SchemaRef,
    /// Number of rows loaded.
    row_count: u64,
}

impl fmt::Debug for ParquetLookupSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetLookupSource")
            .field("config", &self.config)
            .field("data_len", &self.data.len())
            .field("schema", &self.schema)
            .field("row_count", &self.row_count)
            .finish()
    }
}

/// Serialize primary key columns from a `RecordBatch` row into bytes.
///
/// Concatenates the display representation of each PK column for the
/// given row index, separated by `\0` to avoid key collisions.
fn serialize_pk(
    batch: &RecordBatch,
    pk_indices: &[usize],
    row: usize,
) -> Result<Vec<u8>, LookupError> {
    let mut key = Vec::new();
    for (i, &col_idx) in pk_indices.iter().enumerate() {
        if i > 0 {
            key.push(0); // separator
        }
        let col = batch.column(col_idx);
        let formatter = ArrayFormatter::try_new(col.as_ref(), &arrow_cast::display::FormatOptions::default())
            .map_err(|e| LookupError::Internal(format!("format pk: {e}")))?;
        let display = formatter.value(row);
        key.extend_from_slice(format!("{display}").as_bytes());
    }
    Ok(key)
}

/// Serialize all columns of a single row into bytes.
///
/// Uses a simple format: column values separated by `\x01`.
fn serialize_row(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<u8>, LookupError> {
    let mut buf = Vec::new();
    for (i, col) in batch.columns().iter().enumerate() {
        if i > 0 {
            buf.push(1); // separator
        }
        let formatter = ArrayFormatter::try_new(col.as_ref(), &arrow_cast::display::FormatOptions::default())
            .map_err(|e| LookupError::Internal(format!("format row: {e}")))?;
        let display = formatter.value(row);
        buf.extend_from_slice(format!("{display}").as_bytes());
    }
    Ok(buf)
}

impl ParquetLookupSource {
    /// Load a Parquet file from the local filesystem.
    ///
    /// Reads all record batches, extracts primary key columns, and stores
    /// each row in an in-memory `HashMap`.
    ///
    /// # Errors
    ///
    /// Returns [`LookupError::Internal`] if the file cannot be opened,
    /// read, or if the primary key columns are not found in the schema.
    pub fn from_path(config: ParquetLookupSourceConfig) -> Result<Self, LookupError> {
        let file = std::fs::File::open(&config.path)
            .map_err(|e| LookupError::Internal(format!("open {}: {e}", config.path)))?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| LookupError::Internal(format!("parquet reader: {e}")))?;

        let schema = builder.schema().clone();

        // Resolve PK column indices
        let pk_indices: Vec<usize> = config
            .primary_key_columns
            .iter()
            .map(|name| {
                schema
                    .index_of(name)
                    .map_err(|_| LookupError::Internal(format!("pk column not found: {name}")))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let reader = builder
            .with_batch_size(config.batch_size)
            .build()
            .map_err(|e| LookupError::Internal(format!("build reader: {e}")))?;

        let mut data = HashMap::new();
        let mut row_count = 0u64;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| LookupError::Internal(format!("read batch: {e}")))?;

            for row in 0..batch.num_rows() {
                let key = serialize_pk(&batch, &pk_indices, row)?;
                let value = serialize_row(&batch, row)?;
                data.insert(key, value);
                row_count += 1;
            }
        }

        Ok(Self {
            config,
            data,
            schema: Arc::clone(&schema),
            row_count,
        })
    }

    /// Returns the Arrow schema of the loaded Parquet file.
    #[must_use]
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl LookupSource for ParquetLookupSource {
    fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> impl Future<Output = Result<Vec<Option<Vec<u8>>>, LookupError>> + Send {
        let results: Vec<Option<Vec<u8>>> = keys
            .iter()
            .map(|k| self.data.get::<[u8]>(k).cloned())
            .collect();
        async move { Ok(results) }
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            supports_batch_lookup: true,
            max_batch_size: 0,
        }
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn source_name(&self) -> &str {
        "parquet"
    }

    fn estimated_row_count(&self) -> Option<u64> {
        Some(self.row_count)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    /// Create a temp Parquet file with customer data and return the path.
    fn write_temp_parquet(
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().expect("temp file");
        let writer_file = file.reopen().expect("reopen");
        let mut writer =
            ArrowWriter::try_new(writer_file, Arc::clone(schema), None).expect("arrow writer");
        for batch in batches {
            writer.write(batch).expect("write batch");
        }
        writer.close().expect("close writer");
        file
    }

    fn customer_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("tier", DataType::Utf8, false),
        ]))
    }

    fn customer_batch(ids: &[i64], names: &[&str], tiers: &[&str]) -> RecordBatch {
        let schema = customer_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(StringArray::from(tiers.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_single_pk_hit_miss() {
        let batch = customer_batch(&[1, 2, 3], &["Alice", "Bob", "Carol"], &["gold", "silver", "bronze"]);
        let schema = customer_schema();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        // Hit
        let results = source.query(&[b"1"], &[], &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_some());

        // Miss
        let results = source.query(&[b"999"], &[], &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn test_composite_pk() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("city", DataType::Utf8, false),
            Field::new("population", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["US", "US", "UK"])),
                Arc::new(StringArray::from(vec!["NYC", "LA", "London"])),
                Arc::new(Int64Array::from(vec![8_000_000, 4_000_000, 9_000_000])),
            ],
        )
        .unwrap();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["region".into(), "city".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        // Composite key: "US\0NYC"
        let key = b"US\0NYC";
        let results = source.query(&[key.as_slice()], &[], &[]).await.unwrap();
        assert!(results[0].is_some());

        // Composite key miss
        let key = b"US\0Chicago";
        let results = source.query(&[key.as_slice()], &[], &[]).await.unwrap();
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn test_empty_parquet() {
        let schema = customer_schema();
        // Write an empty batch
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(Vec::<i64>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(StringArray::from(Vec::<&str>::new())),
            ],
        )
        .unwrap();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        assert_eq!(source.estimated_row_count(), Some(0));

        let results = source.query(&[b"1"], &[], &[]).await.unwrap();
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn test_batch_query() {
        let batch = customer_batch(&[1, 2, 3], &["Alice", "Bob", "Carol"], &["gold", "silver", "bronze"]);
        let schema = customer_schema();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        let keys: Vec<&[u8]> = vec![b"1", b"999", b"3"];
        let results = source.query(&keys, &[], &[]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results[0].is_some()); // 1 exists
        assert!(results[1].is_none()); // 999 missing
        assert!(results[2].is_some()); // 3 exists
    }

    #[test]
    fn test_row_count_estimate() {
        let batch = customer_batch(&[1, 2, 3], &["Alice", "Bob", "Carol"], &["gold", "silver", "bronze"]);
        let schema = customer_schema();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        assert_eq!(source.estimated_row_count(), Some(3));
    }

    #[test]
    fn test_source_name() {
        let batch = customer_batch(&[1], &["Alice"], &["gold"]);
        let schema = customer_schema();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        assert_eq!(source.source_name(), "parquet");
    }

    #[test]
    fn test_capabilities() {
        let batch = customer_batch(&[1], &["Alice"], &["gold"]);
        let schema = customer_schema();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        let caps = source.capabilities();
        assert!(!caps.supports_predicate_pushdown);
        assert!(!caps.supports_projection_pushdown);
        assert!(caps.supports_batch_lookup);
    }

    #[tokio::test]
    async fn test_health_check() {
        let batch = customer_batch(&[1], &["Alice"], &["gold"]);
        let schema = customer_schema();
        let file = write_temp_parquet(&schema, &[batch]);

        let config = ParquetLookupSourceConfig {
            path: file.path().to_string_lossy().into_owned(),
            primary_key_columns: vec!["customer_id".into()],
            batch_size: 8192,
        };
        let source = ParquetLookupSource::from_path(config).unwrap();

        assert!(source.health_check().await.is_ok());
    }
}
