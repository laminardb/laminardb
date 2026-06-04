//! Iceberg on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` via the native Iceberg scan with a batched
//! `pk IN (...)` filter pushed down (`with_filter`), so all missed keys of a
//! probe fold into one manifest-pruned scan. [`KeyAligner`](laminar_core::lookup::KeyAligner) handles key decode
//! and result realignment.

#[cfg(feature = "iceberg")]
use std::sync::Arc;

#[cfg(feature = "iceberg")]
use arrow_array::{Array, ArrayRef, RecordBatch};
#[cfg(feature = "iceberg")]
use arrow_row::SortField;
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
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
#[cfg(feature = "iceberg")]
use laminar_core::lookup::KeyAligner;

#[cfg(feature = "iceberg")]
use crate::lakehouse::iceberg_config::IcebergCatalogConfig;

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
    schema: SchemaRef,
    aligner: KeyAligner,
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

#[cfg(feature = "iceberg")]
impl LookupSource for IcebergLookupSource {
    async fn query(
        &self,
        keys: &[&[u8]],
        _predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        use tokio_stream::StreamExt;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let pk_arrays = self.aligner.decode_keys(keys)?;
        let predicate =
            Self::build_key_predicate(self.aligner.pk_columns(), &pk_arrays, keys.len())?;

        // Reload the table per query so lookups see the latest snapshot
        // (eventually-consistent freshness; the cache layer adds TTL on top).
        let table = crate::lakehouse::iceberg_io::load_table(
            self.catalog.as_ref(),
            &self.namespace,
            &self.table_name,
        )
        .await
        .map_err(|e| LookupError::Query(format!("load iceberg table: {e}")))?;

        // Projection pushdown: select only the requested columns (always incl.
        // the key, so realignment still works), else every column.
        let mut builder = table.scan().with_filter(predicate);
        builder = if projection.is_empty() {
            builder.select_all()
        } else {
            builder.select(projection_names(&self.schema, projection)?)
        };
        let scan = builder
            .build()
            .map_err(|e| LookupError::Query(format!("build iceberg scan: {e}")))?;
        let stream = scan
            .to_arrow()
            .await
            .map_err(|e| LookupError::Query(format!("iceberg scan to arrow: {e}")))?;

        let mut batches = Vec::new();
        let mut stream = std::pin::pin!(stream);
        while let Some(result) = stream.next().await {
            batches
                .push(result.map_err(|e| LookupError::Query(format!("read iceberg batch: {e}")))?);
        }

        self.aligner.align(keys, &batches)
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

    #[test]
    fn cell_to_datum_null_and_unsupported() {
        let arr = Int64Array::from(vec![None, Some(1)]);
        assert!(IcebergLookupSource::cell_to_datum("id", &arr, 0)
            .unwrap()
            .is_none());
        assert!(IcebergLookupSource::cell_to_datum("id", &arr, 1)
            .unwrap()
            .is_some());
        // Binary keys are not supported as Iceberg predicates.
        let bin = arrow_array::BinaryArray::from(vec![b"x".as_ref()]);
        assert!(IcebergLookupSource::cell_to_datum("k", &bin, 0).is_err());
    }

    #[test]
    fn single_col_predicate_is_in_list() {
        let cols = vec!["id".to_string()];
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
        let s = format!(
            "{}",
            IcebergLookupSource::build_key_predicate(&cols, &arrays, 3).unwrap()
        )
        .to_uppercase();
        assert!(s.contains("IN") && s.contains("id".to_uppercase().as_str()));
    }

    #[test]
    fn composite_predicate_is_or_of_and() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ];
        let s = format!(
            "{}",
            IcebergLookupSource::build_key_predicate(&cols, &arrays, 2).unwrap()
        )
        .to_uppercase();
        assert!(s.contains("AND") && s.contains("OR"));
    }

    #[test]
    fn null_key_adds_is_null_term() {
        let cols = vec!["id".to_string()];
        let arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None]))];
        let s = format!(
            "{}",
            IcebergLookupSource::build_key_predicate(&cols, &arrays, 2).unwrap()
        )
        .to_uppercase();
        assert!(s.contains("NULL"));
    }
}
