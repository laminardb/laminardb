//! Iceberg on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` backed by the native Iceberg table scan with
//! predicate pushdown. Mirrors [`super::delta_lookup`]: a batched
//! `pk IN (...)` filter folds N missed keys into one (file-/manifest-pruned)
//! scan, and results are realigned to the input key order by re-encoding the
//! fetched rows' primary-key columns with the same `RowConverter` used to
//! decode the input keys.

#[cfg(feature = "iceberg")]
use std::collections::HashMap;
#[cfg(feature = "iceberg")]
use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(feature = "iceberg")]
use std::sync::Arc;

#[cfg(feature = "iceberg")]
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "iceberg")]
use arrow_row::{RowConverter, SortField};
#[cfg(feature = "iceberg")]
use arrow_schema::SchemaRef;
#[cfg(feature = "iceberg")]
use iceberg::expr::{Predicate as IcebergPredicate, Reference};
#[cfg(feature = "iceberg")]
use iceberg::spec::Datum;
#[cfg(feature = "iceberg")]
use iceberg::Catalog;

#[cfg(feature = "iceberg")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "iceberg")]
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSource, LookupSourceCapabilities};

#[cfg(feature = "iceberg")]
use crate::lakehouse::iceberg_config::IcebergCatalogConfig;

/// Maximum keys folded into a single batched scan; larger key sets are chunked
/// so the pushed-down `IN` predicate stays bounded.
#[cfg(feature = "iceberg")]
const MAX_KEYS_PER_QUERY: usize = 1024;

/// Configuration for [`IcebergLookupSource`].
#[cfg(feature = "iceberg")]
#[derive(Debug, Clone)]
pub struct IcebergLookupSourceConfig {
    /// Shared catalog connection settings (also carries namespace + table).
    pub catalog: IcebergCatalogConfig,
    /// Primary key column names.
    pub primary_key_columns: Vec<String>,
}

/// Iceberg lookup source for on-demand/partial cache mode.
#[cfg(feature = "iceberg")]
pub struct IcebergLookupSource {
    catalog: Arc<dyn Catalog>,
    namespace: String,
    table_name: String,
    primary_key_columns: Vec<String>,
    schema: SchemaRef,
    pk_sort_fields: Vec<SortField>,
    query_count: AtomicU64,
    row_count: AtomicU64,
    error_count: AtomicU64,
}

#[cfg(feature = "iceberg")]
impl IcebergLookupSource {
    /// Opens the catalog, loads the table, and derives the Arrow schema.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the catalog/table cannot be opened or a primary
    /// key column is missing from the table schema.
    pub async fn open(config: IcebergLookupSourceConfig) -> Result<Self, LookupError> {
        if config.primary_key_columns.is_empty() {
            return Err(LookupError::Internal(
                "primary_key_columns must not be empty".into(),
            ));
        }

        let catalog = crate::lakehouse::iceberg_io::build_catalog(&config.catalog)
            .await
            .map_err(|e| LookupError::Connection(format!("iceberg catalog: {e}")))?;

        let table = crate::lakehouse::iceberg_io::load_table(
            catalog.as_ref(),
            &config.catalog.namespace,
            &config.catalog.table_name,
        )
        .await
        .map_err(|e| LookupError::Connection(format!("load iceberg table: {e}")))?;

        let iceberg_schema = table.current_schema_ref();
        let schema: SchemaRef = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(&iceberg_schema)
                .map_err(|e| LookupError::Internal(format!("iceberg schema to arrow: {e}")))?,
        );

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
            catalog,
            namespace: config.catalog.namespace,
            table_name: config.catalog.table_name,
            primary_key_columns: config.primary_key_columns,
            schema,
            pk_sort_fields,
            query_count: AtomicU64::new(0),
            row_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
        })
    }

    /// Convert one PK cell into an Iceberg [`Datum`], or `None` when NULL (the
    /// caller turns that into an `IS NULL` predicate).
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

    /// Build a single Iceberg predicate covering every key in `pk_arrays`
    /// (column-major; each array has `n_keys` rows). A single-column PK folds
    /// into `pk IN (...)`; a composite PK becomes an OR of per-key AND-groups.
    fn build_key_predicate(
        pk_cols: &[String],
        pk_arrays: &[Arc<dyn Array>],
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
            let mut pred: Option<IcebergPredicate> = if datums.is_empty() {
                None
            } else {
                Some(Reference::new(col.clone()).is_in(datums))
            };
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
        let first = it
            .next()
            .ok_or_else(|| LookupError::Internal("no keys to look up".into()))?;
        Ok(it.fold(first, IcebergPredicate::or))
    }

    /// Re-encode the PK columns of a fetched batch with the same `RowConverter`
    /// used to decode the input keys, so each row maps back to its key bytes.
    fn index_batch_by_key(
        &self,
        converter: &RowConverter,
        batch: &RecordBatch,
        batch_idx: usize,
        index: &mut HashMap<Vec<u8>, (usize, usize)>,
    ) -> Result<(), LookupError> {
        let pk_cols: Vec<Arc<dyn Array>> = self
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

#[cfg(feature = "iceberg")]
impl LookupSource for IcebergLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        _projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        use tokio_stream::StreamExt;

        if keys.is_empty() {
            self.query_count.fetch_add(1, Ordering::Relaxed);
            return Ok(Vec::new());
        }

        let converter = RowConverter::new(self.pk_sort_fields.clone())
            .map_err(|e| LookupError::Internal(format!("row converter: {e}")))?;
        let parser = converter.parser();

        // Reload the table once per query so lookups see the latest snapshot
        // (eventually-consistent freshness; the cache layer adds TTL on top).
        let table = crate::lakehouse::iceberg_io::load_table(
            self.catalog.as_ref(),
            &self.namespace,
            &self.table_name,
        )
        .await
        .map_err(|e| {
            self.error_count.fetch_add(1, Ordering::Relaxed);
            LookupError::Query(format!("load iceberg table: {e}"))
        })?;

        let mut index: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
        let mut fetched: Vec<RecordBatch> = Vec::new();

        for chunk in keys.chunks(MAX_KEYS_PER_QUERY) {
            let parsed = chunk.iter().map(|k| parser.parse(k));
            let pk_arrays = converter
                .convert_rows(parsed)
                .map_err(|e| LookupError::Internal(format!("decode keys: {e}")))?;
            let predicate =
                Self::build_key_predicate(&self.primary_key_columns, &pk_arrays, chunk.len())?;

            let scan = table
                .scan()
                .with_filter(predicate)
                .select_all()
                .build()
                .map_err(|e| {
                    self.error_count.fetch_add(1, Ordering::Relaxed);
                    LookupError::Query(format!("build iceberg scan: {e}"))
                })?;
            let stream = scan.to_arrow().await.map_err(|e| {
                self.error_count.fetch_add(1, Ordering::Relaxed);
                LookupError::Query(format!("iceberg scan to arrow: {e}"))
            })?;
            let mut stream = std::pin::pin!(stream);
            while let Some(result) = stream.next().await {
                let batch = result.map_err(|e| {
                    self.error_count.fetch_add(1, Ordering::Relaxed);
                    LookupError::Query(format!("read iceberg batch: {e}"))
                })?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let batch_idx = fetched.len();
                self.index_batch_by_key(&converter, &batch, batch_idx, &mut index)?;
                fetched.push(batch);
            }
        }

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
            supports_batch_lookup: true,
            max_batch_size: 0,
        }
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn source_name(&self) -> &str {
        "iceberg"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        crate::lakehouse::iceberg_io::load_table(
            self.catalog.as_ref(),
            &self.namespace,
            &self.table_name,
        )
        .await
        .map(|_| ())
        .map_err(|e| LookupError::Connection(format!("health check: {e}")))
    }
}

#[cfg(all(test, feature = "iceberg"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    fn int_pk_arrays(ids: &[i64]) -> Vec<Arc<dyn Array>> {
        vec![Arc::new(Int64Array::from(ids.to_vec()))]
    }

    #[test]
    fn test_cell_to_datum_int_and_string() {
        let ints = Int64Array::from(vec![42i64]);
        let datum = IcebergLookupSource::cell_to_datum("id", &ints, 0)
            .unwrap()
            .unwrap();
        assert_eq!(format!("{datum}"), "42");

        let strs = StringArray::from(vec!["abc"]);
        let datum = IcebergLookupSource::cell_to_datum("name", &strs, 0)
            .unwrap()
            .unwrap();
        assert!(format!("{datum}").contains("abc"));
    }

    #[test]
    fn test_cell_to_datum_null_is_none() {
        let arr = Int64Array::from(vec![None, Some(1)]);
        assert!(IcebergLookupSource::cell_to_datum("id", &arr, 0)
            .unwrap()
            .is_none());
        assert!(IcebergLookupSource::cell_to_datum("id", &arr, 1)
            .unwrap()
            .is_some());
    }

    #[test]
    fn test_cell_to_datum_unsupported_type_errors() {
        // Binary keys are not supported as Iceberg lookup predicates.
        let arr = arrow_array::BinaryArray::from(vec![b"x".as_ref()]);
        assert!(IcebergLookupSource::cell_to_datum("k", &arr, 0).is_err());
    }

    #[test]
    fn test_build_predicate_single_col_in_list() {
        let cols = vec!["id".to_string()];
        let arrays = int_pk_arrays(&[1, 2, 3]);
        let pred = IcebergLookupSource::build_key_predicate(&cols, &arrays, 3).unwrap();
        let s = format!("{pred}");
        // A single-column key folds into an IN predicate over the three values.
        assert!(
            s.to_uppercase().contains("IN"),
            "expected IN predicate: {s}"
        );
        assert!(s.contains("id"), "predicate should reference id: {s}");
    }

    #[test]
    fn test_build_predicate_composite_col_or_of_and() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ];
        let pred = IcebergLookupSource::build_key_predicate(&cols, &arrays, 2).unwrap();
        let s = format!("{pred}").to_uppercase();
        // Composite key → (a = .. AND b = ..) OR (a = .. AND b = ..)
        assert!(s.contains("AND"), "expected AND-groups: {s}");
        assert!(s.contains("OR"), "expected OR across keys: {s}");
    }

    #[test]
    fn test_build_predicate_with_null_key() {
        let cols = vec!["id".to_string()];
        let arrays: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![Some(1), None]))];
        let pred = IcebergLookupSource::build_key_predicate(&cols, &arrays, 2).unwrap();
        let s = format!("{pred}").to_uppercase();
        assert!(
            s.contains("NULL"),
            "null key should add an IS NULL term: {s}"
        );
    }
}
