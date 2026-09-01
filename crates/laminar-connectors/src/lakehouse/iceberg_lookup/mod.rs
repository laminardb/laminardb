//! Iceberg on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` via the native Iceberg scan with a batched
//! `pk IN (...)` filter pushed down (`with_filter`), so all missed keys of a
//! probe fold into one manifest-pruned scan. [`KeyAligner`] handles key decode
//! and result realignment.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_row::SortField;
use arrow_schema::SchemaRef;
use futures_util::StreamExt;
use iceberg::expr::{Predicate as IcebergPredicate, Reference};
use iceberg::spec::Datum;
use iceberg::Catalog;

use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
use laminar_core::lookup::KeyAligner;

use crate::lakehouse::iceberg_config::{
    validate_io_timeout, IcebergCatalogConfig, IcebergStorageConfig,
};
use crate::lakehouse::iceberg_scan::{
    connector_scan_error, plan_files, preflight_snapshot, ManifestReadLimits,
    DEFAULT_MAX_PLANNED_FILES,
};

const MAX_LOOKUP_KEYS: usize = 4_096;
const MAX_LOOKUP_KEY_BYTES: usize = 4 * 1024 * 1024;
const MAX_LOOKUP_RESULT_BYTES: usize = 64 * 1024 * 1024;
const LOOKUP_SCAN_CONCURRENCY: usize = 4;

/// Configuration for [`IcebergLookupSource`].
#[derive(Debug, Clone)]
pub struct IcebergLookupSourceConfig {
    /// Shared catalog connection settings (also carries namespace + table).
    pub catalog: IcebergCatalogConfig,
    /// Table data-storage settings, separate from catalog authentication.
    pub storage: IcebergStorageConfig,
    /// Primary key column names.
    pub primary_key_columns: Vec<String>,
}

/// Iceberg lookup source for on-demand/partial cache mode.
pub struct IcebergLookupSource {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    table_name: String,
    catalog_request_timeout: std::time::Duration,
    storage_request_timeout: std::time::Duration,
    schema: SchemaRef,
    aligner: KeyAligner,
}

impl IcebergLookupSource {
    /// Opens the catalog, loads the table, and derives the Arrow schema.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the catalog/table cannot be opened or a primary
    /// key column is missing from the table schema.
    pub async fn open(config: IcebergLookupSourceConfig) -> Result<Self, LookupError> {
        validate_lookup_config(&config)?;
        let catalog = crate::lakehouse::iceberg_io::build_catalog(&config.catalog, &config.storage)
            .await
            .map_err(|e| LookupError::Connection(format!("iceberg catalog: {e}")))?;
        let table = crate::lakehouse::iceberg_io::load_table_with_timeout(
            catalog.as_ref(),
            &config.catalog.namespace,
            &config.catalog.table_name,
            config.catalog.request_timeout,
        )
        .await
        .map_err(|e| LookupError::Connection(format!("load iceberg table: {e}")))?;

        let iceberg_schema = table.current_schema_ref();
        let schema: SchemaRef = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&iceberg_schema)
                .map_err(|e| LookupError::Internal(format!("iceberg schema to arrow: {e}")))?,
        );

        let pk_sort_fields = config
            .primary_key_columns
            .iter()
            .map(|name| {
                let idx = schema
                    .index_of(name)
                    .map_err(|_| LookupError::Internal(format!("pk column not found: {name}")))?;
                Ok(SortField::new(schema.field(idx).data_type().clone()))
            })
            .collect::<Result<Vec<_>, LookupError>>()?;
        let aligner = KeyAligner::new(pk_sort_fields, config.primary_key_columns)?;

        Ok(Self {
            catalog,
            namespace: config.catalog.namespace,
            table_name: config.catalog.table_name,
            catalog_request_timeout: config.catalog.request_timeout,
            storage_request_timeout: config.storage.request_timeout,
            schema,
            aligner,
        })
    }

    /// Convert one PK cell into an Iceberg [`Datum`], or `None` when NULL.
    fn cell_to_datum(
        col_name: &str,
        array: &dyn Array,
        row: usize,
    ) -> Result<Option<Datum>, LookupError> {
        use arrow_array::{
            BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
            Int8Array, LargeStringArray, StringArray, StringViewArray,
        };
        use arrow_schema::DataType;

        if array.is_null(row) {
            return Ok(None);
        }

        macro_rules! downcast {
            ($ty:ty) => {
                array.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                    LookupError::Internal(format!("pk column '{col_name}' downcast failed"))
                })?
            };
        }

        let datum = match array.data_type() {
            DataType::Int8 => Datum::int(i32::from(downcast!(Int8Array).value(row))),
            DataType::Int16 => Datum::int(i32::from(downcast!(Int16Array).value(row))),
            DataType::Int32 => Datum::int(downcast!(Int32Array).value(row)),
            DataType::Int64 => Datum::long(downcast!(Int64Array).value(row)),
            DataType::Float32 => Datum::float(downcast!(Float32Array).value(row)),
            DataType::Float64 => Datum::double(downcast!(Float64Array).value(row)),
            DataType::Boolean => Datum::bool(downcast!(BooleanArray).value(row)),
            DataType::Utf8 => Datum::string(downcast!(StringArray).value(row)),
            DataType::LargeUtf8 => Datum::string(downcast!(LargeStringArray).value(row)),
            DataType::Utf8View => Datum::string(downcast!(StringViewArray).value(row)),
            dt => {
                return Err(LookupError::Internal(format!(
                    "unsupported PK data type for iceberg lookup: {dt} (column \"{col_name}\")"
                )));
            }
        };
        Ok(Some(datum))
    }

    /// Build a single Iceberg predicate over the decoded key columns: a
    /// single-column PK folds into `pk IN (...)`; a composite PK becomes an OR
    /// of per-key AND-groups.
    fn build_key_predicate(
        pk_cols: &[String],
        pk_arrays: &[ArrayRef],
        n_keys: usize,
    ) -> Result<IcebergPredicate, LookupError> {
        if pk_cols.len() == 1 {
            let col = &pk_cols[0];
            let array = pk_arrays[0].as_ref();
            let mut datums = Vec::with_capacity(n_keys);
            let mut has_null = false;
            for row in 0..n_keys {
                match Self::cell_to_datum(col, array, row)? {
                    Some(d) => datums.push(d),
                    None => has_null = true,
                }
            }
            let mut pred: Option<IcebergPredicate> =
                (!datums.is_empty()).then(|| Reference::new(col.clone()).is_in(datums));
            if has_null {
                let null_pred = Reference::new(col.clone()).is_null();
                pred = Some(match pred {
                    Some(p) => p.or(null_pred),
                    None => null_pred,
                });
            }
            return pred.ok_or_else(|| LookupError::Internal("no keys to look up".into()));
        }

        let mut groups: Vec<IcebergPredicate> = Vec::with_capacity(n_keys);
        for row in 0..n_keys {
            let mut conj: Option<IcebergPredicate> = None;
            for (ci, col) in pk_cols.iter().enumerate() {
                let term = match Self::cell_to_datum(col, pk_arrays[ci].as_ref(), row)? {
                    Some(d) => Reference::new(col.clone()).equal_to(d),
                    None => Reference::new(col.clone()).is_null(),
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
            .map(|first| it.fold(first, IcebergPredicate::or))
            .ok_or_else(|| LookupError::Internal("no keys to look up".into()))
    }
}

fn validate_lookup_config(config: &IcebergLookupSourceConfig) -> Result<(), LookupError> {
    if config.primary_key_columns.is_empty() || config.primary_key_columns.len() > 128 {
        return Err(LookupError::Internal(
            "Iceberg lookup requires between 1 and 128 primary-key columns".into(),
        ));
    }
    let mut names = std::collections::HashSet::with_capacity(config.primary_key_columns.len());
    for name in &config.primary_key_columns {
        if name.is_empty() || !names.insert(name) {
            return Err(LookupError::Internal(
                "Iceberg lookup primary-key columns must be nonempty and distinct".into(),
            ));
        }
    }
    for (name, timeout) in [
        ("catalog.connect_timeout", config.catalog.connect_timeout),
        ("catalog.request_timeout", config.catalog.request_timeout),
        ("storage.request_timeout", config.storage.request_timeout),
        ("storage.connect_timeout", config.storage.connect_timeout),
    ] {
        validate_io_timeout(name, timeout)
            .map_err(|error| LookupError::Internal(error.to_string()))?;
    }
    Ok(())
}

fn validate_lookup_keys(keys: &[&[u8]]) -> Result<(), LookupError> {
    if keys.len() > MAX_LOOKUP_KEYS {
        return Err(LookupError::Query(format!(
            "Iceberg lookup received {} keys, exceeding the fixed {MAX_LOOKUP_KEYS}-key batch limit",
            keys.len()
        )));
    }
    let bytes = keys.iter().try_fold(0_usize, |total, key| {
        total
            .checked_add(key.len())
            .ok_or_else(|| LookupError::Query("Iceberg lookup key byte count overflow".into()))
    })?;
    if bytes > MAX_LOOKUP_KEY_BYTES {
        return Err(LookupError::Query(format!(
            "Iceberg lookup received {bytes} key bytes, exceeding the fixed {MAX_LOOKUP_KEY_BYTES}-byte batch limit"
        )));
    }
    Ok(())
}

async fn read_lookup_batches(
    mut stream: iceberg::scan::ArrowRecordBatchStream,
    max_rows: usize,
    deadline: tokio::time::Instant,
    timeout: std::time::Duration,
) -> Result<Vec<RecordBatch>, LookupError> {
    let mut batches = Vec::new();
    let mut rows = 0_usize;
    let mut bytes = 0_usize;
    loop {
        let next = tokio::time::timeout_at(deadline, stream.next())
            .await
            .map_err(|_| LookupError::Timeout(timeout))?;
        let Some(result) = next else {
            return Ok(batches);
        };
        let batch = result.map_err(|error| {
            LookupError::Query(
                connector_scan_error("Iceberg lookup data read failed", &error).to_string(),
            )
        })?;
        rows = rows
            .checked_add(batch.num_rows())
            .ok_or_else(|| LookupError::Query("Iceberg lookup result row count overflow".into()))?;
        if rows > max_rows {
            return Err(LookupError::Query(format!(
                "Iceberg lookup returned more than {max_rows} distinct-key rows"
            )));
        }
        bytes = batch.columns().iter().try_fold(bytes, |total, column| {
            total
                .checked_add(column.get_array_memory_size())
                .ok_or_else(|| {
                    LookupError::Query("Iceberg lookup result byte count overflow".into())
                })
        })?;
        if bytes > MAX_LOOKUP_RESULT_BYTES {
            return Err(LookupError::Query(format!(
                "Iceberg lookup retained {bytes} result bytes, exceeding the fixed {MAX_LOOKUP_RESULT_BYTES}-byte limit"
            )));
        }
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
}

fn project_aligned_rows(
    aligned: Vec<Option<RecordBatch>>,
    names: &[String],
) -> Result<Vec<Option<RecordBatch>>, LookupError> {
    aligned
        .into_iter()
        .map(|batch| {
            batch
                .map(|batch| {
                    let indices = names
                        .iter()
                        .map(|name| {
                            batch.schema().index_of(name).map_err(|_| {
                                LookupError::Internal(format!(
                                    "Iceberg lookup result omitted projected column '{name}'"
                                ))
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    batch.project(&indices).map_err(|error| {
                        LookupError::Internal(format!(
                            "project aligned Iceberg lookup row: {error}"
                        ))
                    })
                })
                .transpose()
        })
        .collect()
}

impl LookupSource for IcebergLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        validate_lookup_keys(keys)?;

        let pk_arrays = self.aligner.decode_keys(keys)?;
        let predicate =
            Self::build_key_predicate(self.aligner.pk_columns(), &pk_arrays, keys.len())?;

        // Reload the table per query so lookups see the latest snapshot
        // (eventually-consistent freshness; the cache layer adds TTL on top).
        let table = crate::lakehouse::iceberg_io::load_table_with_timeout(
            self.catalog.as_ref(),
            &self.namespace,
            &self.table_name,
            self.catalog_request_timeout,
        )
        .await
        .map_err(|e| LookupError::Query(format!("load iceberg table: {e}")))?;

        let requested_names = projection_names(&self.schema, projection)?;
        let mut scan_names = requested_names.clone();
        if !projection.is_empty() {
            for key_name in self.aligner.pk_columns() {
                if !scan_names.contains(key_name) {
                    scan_names.push(key_name.clone());
                }
            }
        }
        let project_after_alignment = scan_names != requested_names;
        let Some(snapshot) = table.metadata().current_snapshot() else {
            return Ok(vec![None; keys.len()]);
        };
        let mut builder = table
            .scan()
            .snapshot_id(snapshot.snapshot_id())
            .with_filter(predicate)
            .with_concurrency_limit(LOOKUP_SCAN_CONCURRENCY);
        builder = builder.select(&scan_names);
        let scan = builder.build().map_err(|error| {
            LookupError::Query(
                connector_scan_error("build Iceberg lookup scan", &error).to_string(),
            )
        })?;
        let deadline = crate::lakehouse::iceberg_io::checked_deadline(
            self.storage_request_timeout,
            "storage.request_timeout",
        )
        .map_err(|error| LookupError::Query(error.to_string()))?;
        preflight_snapshot(&table, snapshot, ManifestReadLimits::fixed(), deadline)
            .await
            .map_err(|error| LookupError::Query(error.to_string()))?;
        let tasks = plan_files(&scan, DEFAULT_MAX_PLANNED_FILES, deadline)
            .await
            .map_err(|error| LookupError::Query(error.to_string()))?;
        let reader = table
            .reader_builder()
            .with_batch_size(8_192)
            .with_data_file_concurrency_limit(LOOKUP_SCAN_CONCURRENCY)
            .build()
            .read(tasks)
            .map_err(|error| {
                LookupError::Query(
                    connector_scan_error("create Iceberg lookup reader", &error).to_string(),
                )
            })?;
        let unique_keys = keys
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len();
        let batches = read_lookup_batches(
            reader.stream(),
            unique_keys,
            deadline,
            self.storage_request_timeout,
        )
        .await?;
        let aligned = self.aligner.align(keys, &batches)?;
        if project_after_alignment {
            project_aligned_rows(aligned, &requested_names)
        } else {
            Ok(aligned)
        }
    }

    fn capabilities(&self) -> LookupSourceCapabilities {
        LookupSourceCapabilities {
            supports_batch_lookup: true,
            supports_projection_pushdown: true,
            max_batch_size: MAX_LOOKUP_KEYS,
            ..LookupSourceCapabilities::none()
        }
    }

    fn source_name(&self) -> &'static str {
        "iceberg"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        crate::lakehouse::iceberg_io::load_table_with_timeout(
            self.catalog.as_ref(),
            &self.namespace,
            &self.table_name,
            self.catalog_request_timeout,
        )
        .await
        .map(|_| ())
        .map_err(|e| LookupError::Connection(format!("health check: {e}")))
    }
}

#[cfg(test)]
mod tests;
