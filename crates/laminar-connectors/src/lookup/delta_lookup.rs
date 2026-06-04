//! Delta Lake on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` backed by a `DataFusion` `TableProvider`. A
//! batched, typed `pk IN (...)` filter folds all missed keys of a probe into
//! one file-/partition-pruned scan; [`KeyAligner`](laminar_core::lookup::KeyAligner) handles key decode and
//! result realignment.

#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use arrow_array::{Array, ArrayRef, RecordBatch};
#[cfg(feature = "delta-lake")]
use arrow_row::SortField;
#[cfg(feature = "delta-lake")]
use arrow_schema::SchemaRef;
#[cfg(feature = "delta-lake")]
use datafusion::common::ScalarValue;
#[cfg(feature = "delta-lake")]
use datafusion::prelude::{col, Expr, SessionContext};

#[cfg(feature = "delta-lake")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "delta-lake")]
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
#[cfg(feature = "delta-lake")]
use laminar_core::lookup::KeyAligner;

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
    table_name: String,
    schema: SchemaRef,
    aligner: KeyAligner,
}

#[cfg(feature = "delta-lake")]
impl DeltaLookupSource {
    /// Opens the Delta table and registers it as a `DataFusion` `TableProvider`.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the table cannot be opened/registered or a
    /// primary key column is missing from the schema.
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

        let pk_sort_fields = pk_sort_fields(&schema, &config.primary_key_columns)?;
        let aligner = KeyAligner::new(pk_sort_fields, config.primary_key_columns.clone())?;

        warn_if_unclustered(&config).await;

        Ok(Self {
            ctx: Arc::new(ctx),
            table_name: config.table_name,
            schema,
            aligner,
        })
    }
}

/// Resolve the `RowConverter` sort fields for the primary-key columns.
#[cfg(feature = "delta-lake")]
fn pk_sort_fields(
    schema: &SchemaRef,
    pk_columns: &[String],
) -> Result<Vec<SortField>, LookupError> {
    pk_columns
        .iter()
        .map(|name| {
            let idx = schema
                .index_of(name)
                .map_err(|_| LookupError::Internal(format!("pk column not found: {name}")))?;
            Ok(SortField::new(schema.field(idx).data_type().clone()))
        })
        .collect()
}

/// Build a typed `pk IN (...)` (single column) or OR-of-AND-groups (composite)
/// filter from the decoded primary-key columns. Using typed `Expr` literals
/// (not string SQL) keeps type handling and escaping correct.
#[cfg(feature = "delta-lake")]
fn build_in_list_filter(
    pk_columns: &[String],
    pk_arrays: &[ArrayRef],
) -> Result<Expr, LookupError> {
    let n = if pk_arrays.is_empty() {
        0
    } else {
        pk_arrays[0].len()
    };
    let scalar = |arr: &ArrayRef, row: usize| {
        ScalarValue::try_from_array(arr, row)
            .map(|sv| Expr::Literal(sv, None))
            .map_err(|e| LookupError::Internal(format!("scalar from key: {e}")))
    };

    if pk_columns.len() == 1 {
        let column = col(&pk_columns[0]);
        let arr = &pk_arrays[0];
        let mut lits = Vec::new();
        let mut has_null = false;
        for row in 0..n {
            if arr.is_null(row) {
                has_null = true;
            } else {
                lits.push(scalar(arr, row)?);
            }
        }
        let mut filter = (!lits.is_empty()).then(|| column.clone().in_list(lits, false));
        if has_null {
            let is_null = column.is_null();
            filter = Some(match filter {
                Some(f) => f.or(is_null),
                None => is_null,
            });
        }
        return filter.ok_or_else(|| LookupError::Internal("no keys to look up".into()));
    }

    let mut groups: Vec<Expr> = Vec::with_capacity(n);
    for row in 0..n {
        let mut conj: Option<Expr> = None;
        for (ci, name) in pk_columns.iter().enumerate() {
            let term = if pk_arrays[ci].is_null(row) {
                col(name).is_null()
            } else {
                col(name).eq(scalar(&pk_arrays[ci], row)?)
            };
            conj = Some(match conj {
                Some(c) => c.and(term),
                None => term,
            });
        }
        if let Some(c) = conj {
            groups.push(c);
        }
    }
    let mut it = groups.into_iter();
    it.next()
        .map(|first| it.fold(first, Expr::or))
        .ok_or_else(|| LookupError::Internal("no keys to look up".into()))
}

/// Best-effort clustering diagnostic: an on-demand lookup is only cheap if the
/// dimension is partitioned/clustered on the key. Delta exposes partition
/// columns (not Z-ORDER), so this is a warning, never an error.
#[cfg(feature = "delta-lake")]
async fn warn_if_unclustered(config: &DeltaLookupSourceConfig) {
    let Ok(table) = crate::lakehouse::delta_io::open_or_create_table(
        &config.table_path,
        config.storage_options.clone(),
        None,
    )
    .await
    else {
        return;
    };
    let partition_columns = crate::lakehouse::delta_io::get_partition_columns(&table);
    if !config
        .primary_key_columns
        .iter()
        .any(|k| partition_columns.contains(k))
    {
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

#[cfg(feature = "delta-lake")]
impl LookupSource for DeltaLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let pk_arrays = self.aligner.decode_keys(keys)?;
        let filter = build_in_list_filter(self.aligner.pk_columns(), &pk_arrays)?;

        let mut df = self
            .ctx
            .table(&self.table_name)
            .await
            .map_err(|e| LookupError::Query(format!("open delta table: {e}")))?
            .filter(filter)
            .map_err(|e| LookupError::Query(format!("apply lookup filter: {e}")))?;
        let original_names = if projection.is_empty() {
            None
        } else {
            Some(projection_names(&self.schema, projection)?)
        };
        // Projection pushdown: select only the requested columns (the optimizer
        // pushes this into the Parquet scan). The projection must carry the
        // key columns so realignment works, then we project them out if unrequested.
        if !projection.is_empty() {
            let mut names = projection_names(&self.schema, projection)?;
            for pk in self.aligner.pk_columns() {
                if !names.contains(pk) {
                    names.push(pk.clone());
                }
            }
            let refs: Vec<&str> = names.iter().map(String::as_str).collect();
            df = df
                .select_columns(&refs)
                .map_err(|e| LookupError::Query(format!("apply lookup projection: {e}")))?;
        }
        let batches = df
            .collect()
            .await
            .map_err(|e| LookupError::Query(format!("collect lookup results: {e}")))?;

        let aligned = self
            .aligner
            .align(keys, &batches)
            .map_err(|e| LookupError::Internal(format!("align lookup results: {e}")))?;

        if let Some(orig_names) = original_names {
            let mut projected_aligned = Vec::with_capacity(aligned.len());
            for maybe_batch in aligned {
                if let Some(batch) = maybe_batch {
                    let indices: Vec<usize> = orig_names
                        .iter()
                        .map(|name| {
                            batch.schema().index_of(name).map_err(|e| {
                                LookupError::Internal(format!(
                                    "column not found in aligned schema: {e}"
                                ))
                            })
                        })
                        .collect::<Result<Vec<usize>, LookupError>>()?;
                    let projected = batch.project(&indices).map_err(|e| {
                        LookupError::Internal(format!("project aligned batch: {e}"))
                    })?;
                    projected_aligned.push(Some(projected));
                } else {
                    projected_aligned.push(None);
                }
            }
            Ok(projected_aligned)
        } else {
            Ok(aligned)
        }
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_batch_lookup: true,
            supports_projection_pushdown: true,
            ..LookupSourceCapabilities::none()
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
            .table(&self.table_name)
            .await
            .map(|_| ())
            .map_err(|e| LookupError::Connection(format!("health check: {e}")))
    }
}

#[cfg(all(test, feature = "delta-lake"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_row::RowConverter;
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

    fn int_keys(ids: &[i64]) -> Vec<Vec<u8>> {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let rows = converter
            .convert_columns(&[Arc::new(Int64Array::from(ids.to_vec()))])
            .unwrap();
        (0..ids.len())
            .map(|i| rows.row(i).as_ref().to_vec())
            .collect()
    }

    async fn create_delta_table(path: &str, batches: Vec<RecordBatch>) {
        use crate::lakehouse::delta_io;
        use deltalake::protocol::SaveMode;

        let table = delta_io::open_or_create_table(path, HashMap::new(), Some(&test_schema()))
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

    async fn open_source(path: &str, table_name: &str) -> DeltaLookupSource {
        DeltaLookupSource::open(DeltaLookupSourceConfig {
            table_path: path.to_string(),
            storage_options: HashMap::new(),
            primary_key_columns: vec!["id".into()],
            table_name: table_name.to_string(),
        })
        .await
        .unwrap()
    }

    fn id_at(batch: &RecordBatch) -> i64 {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test]
    async fn batched_lookup_aligns_hits_and_misses() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();
        create_delta_table(path, vec![test_batch(&[1, 2, 3], &["A", "B", "C"])]).await;
        let source = open_source(path, "lk").await;

        // Out-of-table-order with a miss; one batched fetch.
        let keys = int_keys(&[3, 1, 999, 2]);
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let results = source.query(&key_refs, &[], &[]).await.unwrap();

        assert_eq!(results.len(), 4);
        assert_eq!(id_at(results[0].as_ref().unwrap()), 3);
        assert_eq!(id_at(results[1].as_ref().unwrap()), 1);
        assert!(results[2].is_none());
        assert_eq!(id_at(results[3].as_ref().unwrap()), 2);
    }

    /// The Phase 3 exit criterion: a batched `pk IN (...)` must prune
    /// non-matching partition files (per-key cost O(matching files), not
    /// O(table)) on a table clustered on the key.
    #[tokio::test]
    async fn in_list_prunes_partition_files() {
        use datafusion::physical_plan::collect;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_str().unwrap();

        // 8 distinct keys → 8 partition directories (one Parquet file each).
        {
            use crate::lakehouse::delta_io;
            use deltalake::protocol::SaveMode;
            let t = delta_io::open_or_create_table(path, HashMap::new(), None)
                .await
                .unwrap();
            delta_io::write_batches(
                t,
                vec![test_batch(
                    &[0, 1, 2, 3, 4, 5, 6, 7],
                    &["a", "b", "c", "d", "e", "f", "g", "h"],
                )],
                "w",
                1,
                SaveMode::Append,
                Some(&["id".to_string()]),
                false,
                None,
                false,
                None,
            )
            .await
            .unwrap();
        }

        // Correctness across partition files via the source.
        let source = open_source(path, "lk").await;
        let keys = int_keys(&[5, 2, 100]);
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let results = source.query(&key_refs, &[], &[]).await.unwrap();
        assert!(results[0].is_some() && results[1].is_some() && results[2].is_none());

        // Pruning: the IN-list query reads fewer than all 8 files. (`next`
        // provider reports files read via `count_files_scanned`.)
        let ctx = SessionContext::new();
        crate::lakehouse::delta_table_provider::register_delta_table(
            &ctx,
            "lk",
            path,
            HashMap::new(),
        )
        .await
        .unwrap();
        let plan = ctx
            .sql("SELECT * FROM \"lk\" WHERE \"id\" IN (2, 5)")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let _ = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
        let scanned = sum_plan_metric(&plan, "count_files_scanned");
        assert!(
            scanned > 0 && scanned < 8,
            "expected pruning, scanned={scanned}"
        );
    }

    fn sum_plan_metric(
        plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        name: &str,
    ) -> usize {
        let mut total = plan
            .metrics()
            .and_then(|m| m.sum_by_name(name))
            .map_or(0, |v| v.as_usize());
        for child in plan.children() {
            total += sum_plan_metric(child, name);
        }
        total
    }
}
