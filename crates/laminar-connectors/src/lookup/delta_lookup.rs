//! Delta Lake on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` backed by a `DataFusion` `TableProvider`.
//! Used as the `source` field in `PartialLookupState` for tables too
//! large to snapshot into memory.

#[cfg(feature = "delta-lake")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use arrow_array::RecordBatch;
#[cfg(feature = "delta-lake")]
use arrow_row::{RowConverter, SortField};
#[cfg(feature = "delta-lake")]
use arrow_schema::SchemaRef;
#[cfg(feature = "delta-lake")]
use datafusion::prelude::SessionContext;

#[cfg(feature = "delta-lake")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "delta-lake")]
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};

/// Configuration for [`DeltaLookupSource`].
#[cfg(feature = "delta-lake")]
#[derive(Debug, Clone)]
pub struct DeltaLookupSourceConfig {
    /// Table path (resolved, post-catalog).
    pub table_path: String,
    /// Storage options (credentials, etc.).
    pub storage_options: std::collections::HashMap<String, String>,
    /// Primary key column names.
    pub primary_key_columns: Vec<String>,
    /// `DataFusion` table name (registered in session context).
    pub table_name: String,
}

/// Delta Lake lookup source for on-demand/partial cache mode.
#[cfg(feature = "delta-lake")]
pub struct DeltaLookupSource {
    ctx: Arc<SessionContext>,
    config: DeltaLookupSourceConfig,
    schema: SchemaRef,
    pk_sort_fields: Vec<SortField>,
    query_count: AtomicU64,
    row_count: AtomicU64,
    error_count: AtomicU64,
}

#[cfg(feature = "delta-lake")]
impl DeltaLookupSource {
    /// Opens the Delta table and registers it as a `DataFusion` `TableProvider`.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the table cannot be opened or registered.
    pub async fn open(config: DeltaLookupSourceConfig) -> Result<Self, LookupError> {
        let ctx = SessionContext::new();

        crate::lakehouse::delta_table_provider::register_delta_table(
            &ctx,
            &config.table_name,
            &config.table_path,
            config.storage_options.clone(),
        )
        .await
        .map_err(|e| LookupError::Connection(format!("register delta table: {e}")))?;

        let table = ctx
            .table(&config.table_name)
            .await
            .map_err(|e| LookupError::Internal(format!("get table: {e}")))?;
        let schema: SchemaRef = Arc::new(table.schema().as_arrow().clone());

        let pk_sort_fields: Vec<SortField> = config
            .primary_key_columns
            .iter()
            .map(|col_name| {
                let idx = schema.index_of(col_name).map_err(|_| {
                    LookupError::Internal(format!("pk column not found: {col_name}"))
                })?;
                Ok(SortField::new(schema.field(idx).data_type().clone()))
            })
            .collect::<Result<Vec<_>, LookupError>>()?;

        Ok(Self {
            ctx: Arc::new(ctx),
            config,
            schema,
            pk_sort_fields,
            query_count: AtomicU64::new(0),
            row_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
        })
    }

    /// Build a SQL WHERE clause from decoded PK column arrays.
    fn build_where_clause(
        &self,
        pk_arrays: &[Arc<dyn arrow_array::Array>],
    ) -> Result<String, LookupError> {
        use arrow_cast::display::{ArrayFormatter, FormatOptions};
        use arrow_schema::DataType;

        let mut conditions = Vec::with_capacity(self.config.primary_key_columns.len());
        for (col_name, array) in self.config.primary_key_columns.iter().zip(pk_arrays) {
            if array.is_null(0) {
                conditions.push(format!("\"{col_name}\" IS NULL"));
                continue;
            }
            let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
                .map_err(|e| LookupError::Internal(format!("format pk: {e}")))?;
            let value = formatter.value(0).to_string();
            match array.data_type() {
                // Numeric and boolean: unquoted literals.
                dt if dt.is_numeric() || matches!(dt, DataType::Boolean) => {
                    conditions.push(format!("\"{col_name}\" = {value}"));
                }
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                    let escaped = value.replace('\'', "''");
                    conditions.push(format!("\"{col_name}\" = '{escaped}'"));
                }
                DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => {
                    conditions.push(format!("\"{col_name}\" = '{value}'"));
                }
                dt => {
                    return Err(LookupError::Internal(format!(
                        "unsupported PK data type for lookup: {dt} (column \"{col_name}\")"
                    )));
                }
            }
        }
        Ok(conditions.join(" AND "))
    }

    /// Total queries executed.
    #[must_use]
    pub fn query_count(&self) -> u64 {
        self.query_count.load(Ordering::Relaxed)
    }

    /// Total rows returned.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count.load(Ordering::Relaxed)
    }

    /// Total query errors.
    #[must_use]
    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }
}

#[cfg(feature = "delta-lake")]
impl LookupSource for DeltaLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        use tokio_stream::StreamExt;

        let mut results = Vec::with_capacity(keys.len());
        let converter = RowConverter::new(self.pk_sort_fields.clone())
            .map_err(|e| LookupError::Internal(format!("row converter: {e}")))?;
        let parser = converter.parser();

        for key_bytes in keys {
            let row = parser.parse(key_bytes);
            let pk_arrays = converter
                .convert_rows(std::iter::once(row))
                .map_err(|e| LookupError::Internal(format!("decode key: {e}")))?;
            let where_clause = self.build_where_clause(&pk_arrays)?;

            let sql = format!(
                "SELECT * FROM \"{}\" WHERE {} LIMIT 1",
                self.config.table_name, where_clause
            );

            let df = self.ctx.sql(&sql).await.map_err(|e| {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                LookupError::Query(format!("delta lookup query failed: {e}"))
            })?;

            let mut stream = df.execute_stream().await.map_err(|e| {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                LookupError::Query(format!("execute stream: {e}"))
            })?;

            let mut found = None;
            while let Some(batch_result) = stream.next().await {
                match batch_result {
                    Ok(batch) if batch.num_rows() > 0 => {
                        self.row_count.fetch_add(1, Ordering::Relaxed);
                        found = Some(batch.slice(0, 1));
                        break;
                    }
                    Err(e) => {
                        self.error_count.fetch_add(1, Ordering::Relaxed);
                        return Err(LookupError::Query(format!("stream error: {e}")));
                    }
                    _ => {}
                }
            }

            results.push(found);
        }

        self.query_count.fetch_add(1, Ordering::Relaxed);
        Ok(results)
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
        "delta-lake"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        self.ctx
            .table(&self.config.table_name)
            .await
            .map_err(|e| LookupError::Connection(format!("health check: {e}")))?;
        Ok(())
    }
}

#[cfg(all(test, feature = "delta-lake"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    async fn create_delta_table(path: &str, batches: Vec<RecordBatch>) {
        use crate::lakehouse::delta_io;
        use deltalake::protocol::SaveMode;

        let schema = test_schema();
        let table = delta_io::open_or_create_table(path, HashMap::new(), Some(&schema))
            .await
            .unwrap();

        delta_io::write_batches(
            table,
            batches,
            "test-writer",
            1,
            SaveMode::Append,
            None,
            false,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_open_and_query() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        create_delta_table(table_path, vec![test_batch(&[1, 2, 3], &["A", "B", "C"])]).await;

        let config = DeltaLookupSourceConfig {
            table_path: table_path.to_string(),
            storage_options: HashMap::new(),
            primary_key_columns: vec!["id".into()],
            table_name: "test_lookup".to_string(),
        };
        let source = DeltaLookupSource::open(config).await.unwrap();

        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let key_col = Arc::new(Int64Array::from(vec![2i64]));
        let rows = converter.convert_columns(&[key_col]).unwrap();
        let key_bytes = rows.row(0);

        let results = source.query(&[key_bytes.as_ref()], &[], &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        let batch = results[0].as_ref().unwrap();
        assert_eq!(batch.num_rows(), 1);

        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 2);
        assert_eq!(source.query_count(), 1);
        assert_eq!(source.row_count(), 1);
    }

    #[tokio::test]
    async fn test_query_miss() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        create_delta_table(table_path, vec![test_batch(&[1], &["A"])]).await;

        let config = DeltaLookupSourceConfig {
            table_path: table_path.to_string(),
            storage_options: HashMap::new(),
            primary_key_columns: vec!["id".into()],
            table_name: "test_miss".to_string(),
        };
        let source = DeltaLookupSource::open(config).await.unwrap();

        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let key_col = Arc::new(Int64Array::from(vec![999i64]));
        let rows = converter.convert_columns(&[key_col]).unwrap();
        let key_bytes = rows.row(0);

        let results = source.query(&[key_bytes.as_ref()], &[], &[]).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_none());
        assert_eq!(source.row_count(), 0);
    }
}
