//! Delta Lake on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` backed by a `DataFusion` `TableProvider`.
//! Used as the `source` field in `PartialLookupState` for tables too
//! large to snapshot into memory.

#[cfg(feature = "delta-lake")]
use std::collections::HashMap;
#[cfg(feature = "delta-lake")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use arrow_array::{Array, RecordBatch};
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

/// Maximum keys folded into a single batched lookup query. Larger key sets
/// are chunked into multiple `WHERE pk IN (...)` scans so the generated SQL
/// (and any IN-list the planner materialises) stays bounded.
#[cfg(feature = "delta-lake")]
const MAX_KEYS_PER_QUERY: usize = 1024;

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
        if config.primary_key_columns.is_empty() {
            return Err(LookupError::Internal(
                "primary_key_columns must not be empty".into(),
            ));
        }
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

        // Clustering awareness: an on-demand lookup is only cheap if the
        // dimension is partitioned/clustered on the lookup key, so a miss
        // fetch prunes to O(matching files) instead of full-scanning. Delta
        // exposes partition columns in metadata but not Z-ORDER clustering, so
        // this is a best-effort warning (never an error): if none of the key
        // columns is a partition column, the table *may* still be Z-ordered,
        // but if it is not, every miss full-scans the table.
        if let Ok(table) = crate::lakehouse::delta_io::open_or_create_table(
            &config.table_path,
            config.storage_options.clone(),
            None,
        )
        .await
        {
            let partition_columns = crate::lakehouse::delta_io::get_partition_columns(&table);
            let key_is_partitioned = config
                .primary_key_columns
                .iter()
                .any(|k| partition_columns.contains(k));
            if !key_is_partitioned {
                tracing::warn!(
                    table = %config.table_path,
                    primary_key = ?config.primary_key_columns,
                    partition_columns = ?partition_columns,
                    "delta lookup table is not partitioned on the lookup key; unless it is \
                     Z-ORDER clustered on the key, every cache-miss fetch will full-scan the \
                     table. Cluster the dimension on the lookup key for bounded per-fetch cost."
                );
            }
        }

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

    /// Format one PK cell as a SQL literal, or `None` when the cell is NULL
    /// (the caller turns that into `IS NULL`).
    fn format_cell(
        col_name: &str,
        array: &dyn Array,
        row: usize,
    ) -> Result<Option<String>, LookupError> {
        use arrow_cast::display::{ArrayFormatter, FormatOptions};
        use arrow_schema::DataType;

        if array.is_null(row) {
            return Ok(None);
        }
        let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())
            .map_err(|e| LookupError::Internal(format!("format pk: {e}")))?;
        let value = formatter.value(row).to_string();
        match array.data_type() {
            // Numeric and boolean: unquoted literals.
            dt if dt.is_numeric() || matches!(dt, DataType::Boolean) => Ok(Some(value)),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                let escaped = value.replace('\'', "''");
                Ok(Some(format!("'{escaped}'")))
            }
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => {
                Ok(Some(format!("'{value}'")))
            }
            dt => Err(LookupError::Internal(format!(
                "unsupported PK data type for lookup: {dt} (column \"{col_name}\")"
            ))),
        }
    }

    /// Build a single SQL WHERE clause covering every key in `pk_arrays`
    /// (column-major; each array has `n_keys` rows). A single-column PK folds
    /// into one `"pk" IN (...)` list so the planner can prune manifests/files;
    /// a composite PK becomes an OR of per-key AND-groups. Duplicate keys are
    /// de-duplicated so the generated SQL stays compact.
    fn build_batch_where_clause(
        &self,
        pk_arrays: &[Arc<dyn Array>],
        n_keys: usize,
    ) -> Result<String, LookupError> {
        let pk_cols = &self.config.primary_key_columns;

        if pk_cols.len() == 1 {
            let col = &pk_cols[0];
            let array = pk_arrays[0].as_ref();
            let mut seen = std::collections::HashSet::new();
            let mut lits: Vec<String> = Vec::new();
            let mut has_null = false;
            for row in 0..n_keys {
                match Self::format_cell(col, array, row)? {
                    Some(lit) => {
                        if seen.insert(lit.clone()) {
                            lits.push(lit);
                        }
                    }
                    None => has_null = true,
                }
            }
            let mut clause = String::new();
            if !lits.is_empty() {
                clause = format!("\"{col}\" IN ({})", lits.join(", "));
            }
            if has_null {
                let null_cond = format!("\"{col}\" IS NULL");
                clause = if clause.is_empty() {
                    null_cond
                } else {
                    format!("{clause} OR {null_cond}")
                };
            }
            // n_keys > 0 guarantees at least one branch fired.
            return Ok(clause);
        }

        let mut seen = std::collections::HashSet::new();
        let mut groups: Vec<String> = Vec::new();
        for row in 0..n_keys {
            let mut conds = Vec::with_capacity(pk_cols.len());
            for (ci, col) in pk_cols.iter().enumerate() {
                match Self::format_cell(col, pk_arrays[ci].as_ref(), row)? {
                    Some(lit) => conds.push(format!("\"{col}\" = {lit}")),
                    None => conds.push(format!("\"{col}\" IS NULL")),
                }
            }
            let group = format!("({})", conds.join(" AND "));
            if seen.insert(group.clone()) {
                groups.push(group);
            }
        }
        Ok(groups.join(" OR "))
    }

    /// Re-encode the PK columns of a fetched batch with the same `RowConverter`
    /// used to decode the input keys, so each returned row can be matched back
    /// to the exact key bytes that requested it.
    fn index_batch_by_key(
        &self,
        converter: &RowConverter,
        batch: &RecordBatch,
        batch_idx: usize,
        index: &mut HashMap<Vec<u8>, (usize, usize)>,
    ) -> Result<(), LookupError> {
        let pk_cols: Vec<Arc<dyn Array>> = self
            .config
            .primary_key_columns
            .iter()
            .map(|name| {
                let idx = batch.schema().index_of(name).map_err(|_| {
                    LookupError::Internal(format!("pk column not found in result: {name}"))
                })?;
                Ok(Arc::clone(batch.column(idx)))
            })
            .collect::<Result<_, LookupError>>()?;

        let rows = converter
            .convert_columns(&pk_cols)
            .map_err(|e| LookupError::Internal(format!("encode result keys: {e}")))?;

        for row in 0..batch.num_rows() {
            let key = rows.row(row).as_ref().to_vec();
            // First row wins per key (mirrors the old `LIMIT 1` semantics).
            index.entry(key).or_insert((batch_idx, row));
        }
        Ok(())
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
        if keys.is_empty() {
            self.query_count.fetch_add(1, Ordering::Relaxed);
            return Ok(Vec::new());
        }

        let converter = RowConverter::new(self.pk_sort_fields.clone())
            .map_err(|e| LookupError::Internal(format!("row converter: {e}")))?;
        let parser = converter.parser();

        // Map each distinct input key to the single fetched row that satisfies
        // it. Misses simply never appear in the index. `chunk` keeps the IN-list
        // (and the materialised batches) bounded for very large key sets.
        let mut index: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
        let mut fetched: Vec<RecordBatch> = Vec::new();

        for chunk in keys.chunks(MAX_KEYS_PER_QUERY) {
            let parsed = chunk.iter().map(|k| parser.parse(k));
            let pk_arrays = converter
                .convert_rows(parsed)
                .map_err(|e| LookupError::Internal(format!("decode keys: {e}")))?;

            let where_clause = self.build_batch_where_clause(&pk_arrays, chunk.len())?;
            let sql = format!(
                "SELECT * FROM \"{}\" WHERE {}",
                self.config.table_name, where_clause
            );

            let df = self.ctx.sql(&sql).await.map_err(|e| {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                LookupError::Query(format!("delta lookup query failed: {e}"))
            })?;
            let batches = df.collect().await.map_err(|e| {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                LookupError::Query(format!("collect lookup results: {e}"))
            })?;

            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let batch_idx = fetched.len();
                self.index_batch_by_key(&converter, &batch, batch_idx, &mut index)?;
                fetched.push(batch);
            }
        }

        // Align outputs to the input key order; duplicate input keys each get
        // their own single-row slice of the shared fetched batch.
        let mut hits = 0u64;
        let results: Vec<Option<RecordBatch>> = keys
            .iter()
            .map(|key| {
                index.get(*key).map(|&(bi, row)| {
                    hits += 1;
                    fetched[bi].slice(row, 1)
                })
            })
            .collect();

        self.row_count.fetch_add(hits, Ordering::Relaxed);
        self.query_count.fetch_add(1, Ordering::Relaxed);
        Ok(results)
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_predicate_pushdown: false,
            supports_projection_pushdown: false,
            // Genuinely batched now: N keys fold into ceil(N / MAX_KEYS_PER_QUERY)
            // pruned scans. 0 = the caller need not pre-chunk; we chunk internally.
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

    fn int_keys(ids: &[i64]) -> Vec<Vec<u8>> {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let col = Arc::new(Int64Array::from(ids.to_vec()));
        let rows = converter.convert_columns(&[col]).unwrap();
        (0..ids.len())
            .map(|i| rows.row(i).as_ref().to_vec())
            .collect()
    }

    #[tokio::test]
    async fn test_batch_query_aligns_results_to_keys() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        create_delta_table(table_path, vec![test_batch(&[1, 2, 3], &["A", "B", "C"])]).await;

        let config = DeltaLookupSourceConfig {
            table_path: table_path.to_string(),
            storage_options: HashMap::new(),
            primary_key_columns: vec!["id".into()],
            table_name: "test_batch".to_string(),
        };
        let source = DeltaLookupSource::open(config).await.unwrap();

        // Out-of-table-order, with a miss in the middle, in a single batch.
        let keys = int_keys(&[3, 1, 999, 2]);
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        let results = source.query(&key_refs, &[], &[]).await.unwrap();
        assert_eq!(results.len(), 4);

        let id_of = |b: &RecordBatch| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        };
        assert_eq!(id_of(results[0].as_ref().unwrap()), 3);
        assert_eq!(id_of(results[1].as_ref().unwrap()), 1);
        assert!(results[2].is_none(), "999 should miss");
        assert_eq!(id_of(results[3].as_ref().unwrap()), 2);

        // One batched query, three hits.
        assert_eq!(source.query_count(), 1);
        assert_eq!(source.row_count(), 3);
    }

    #[tokio::test]
    async fn test_batch_query_duplicate_keys_each_resolve() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        create_delta_table(table_path, vec![test_batch(&[1, 2], &["A", "B"])]).await;

        let config = DeltaLookupSourceConfig {
            table_path: table_path.to_string(),
            storage_options: HashMap::new(),
            primary_key_columns: vec!["id".into()],
            table_name: "test_dup".to_string(),
        };
        let source = DeltaLookupSource::open(config).await.unwrap();

        let keys = int_keys(&[1, 1, 2]);
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();

        let results = source.query(&key_refs, &[], &[]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(Option::is_some));
        // Duplicate key 1 resolves twice — both aligned outputs present.
        assert_eq!(source.query_count(), 1);
    }

    #[tokio::test]
    async fn test_batch_query_empty_keys() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        create_delta_table(table_path, vec![test_batch(&[1], &["A"])]).await;

        let config = DeltaLookupSourceConfig {
            table_path: table_path.to_string(),
            storage_options: HashMap::new(),
            primary_key_columns: vec!["id".into()],
            table_name: "test_empty".to_string(),
        };
        let source = DeltaLookupSource::open(config).await.unwrap();

        let results = source.query(&[], &[], &[]).await.unwrap();
        assert!(results.is_empty());
    }
}
