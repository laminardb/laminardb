//! Delta Lake on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` backed by a `DataFusion` `TableProvider`. A
//! batched, typed `pk IN (...)` filter folds all missed keys of a probe into
//! one file-/partition-pruned scan; [`KeyAligner`] handles key decode and
//! result realignment.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_row::SortField;
use arrow_schema::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::prelude::{col, Expr, SessionContext};

use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
use laminar_core::lookup::KeyAligner;

/// Configuration for [`DeltaLookupSource`].
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
pub struct DeltaLookupSource {
    ctx: Arc<SessionContext>,
    table_name: String,
    schema: SchemaRef,
    aligner: KeyAligner,
}

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
            primary_key = ?config.primary_key_columns,
            partition_columns = ?partition_columns,
            "delta lookup table is not partitioned on the lookup key; unless it is \
             Z-ORDER clustered on the key, every cache-miss fetch will full-scan the \
             table. Cluster the dimension on the lookup key for bounded per-fetch cost."
        );
    }
}

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

    fn source_name(&self) -> &'static str {
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

#[cfg(test)]
mod tests;
