//! `MongoDB` on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` via a multi-get on the indexed key
//! (`find({ pk: { $in: [keys] } })`), so all missed keys of a probe fold into
//! one round trip. `MongoDB` is schemaless, so the source projects each
//! returned document into the table's **declared** Arrow schema (from
//! `CREATE LOOKUP TABLE`); [`KeyAligner`] handles key decode and realignment.
//!
//! v1 limits: single-column key; declared column types Int32/Int64/Float64/
//! Boolean/Utf8 (others render as a string/JSON fallback or NULL).

#[cfg(feature = "mongodb-cdc")]
use std::sync::Arc;

#[cfg(feature = "mongodb-cdc")]
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "mongodb-cdc")]
use arrow_row::SortField;
#[cfg(feature = "mongodb-cdc")]
use arrow_schema::{DataType, SchemaRef};
#[cfg(feature = "mongodb-cdc")]
use mongodb::bson::{doc, Bson, Document};
#[cfg(feature = "mongodb-cdc")]
use mongodb::Client;

#[cfg(feature = "mongodb-cdc")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "mongodb-cdc")]
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
#[cfg(feature = "mongodb-cdc")]
use laminar_core::lookup::KeyAligner;

/// Configuration for [`MongoLookupSource`].
#[cfg(feature = "mongodb-cdc")]
#[derive(Debug, Clone)]
pub struct MongoLookupSourceConfig {
    /// `MongoDB` connection URI.
    pub connection_uri: String,
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Primary key field (v1: exactly one).
    pub primary_key_columns: Vec<String>,
    /// Declared Arrow schema (the projection target).
    pub schema: SchemaRef,
}

/// `MongoDB` lookup source for on-demand/partial cache mode.
#[cfg(feature = "mongodb-cdc")]
pub struct MongoLookupSource {
    client: Client,
    database: String,
    collection: String,
    pk_field: String,
    schema: SchemaRef,
    aligner: KeyAligner,
}

#[cfg(feature = "mongodb-cdc")]
impl MongoLookupSource {
    /// Connects to `MongoDB` and validates the declared key column.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the client cannot be built, the key is not a
    /// single column, or the key column is missing from the declared schema.
    pub async fn open(config: MongoLookupSourceConfig) -> Result<Self, LookupError> {
        if config.primary_key_columns.len() != 1 {
            return Err(LookupError::Internal(format!(
                "mongodb lookup requires exactly one primary key column, got {}",
                config.primary_key_columns.len()
            )));
        }
        let pk_field = config.primary_key_columns[0].clone();

        let pk_idx = config.schema.index_of(&pk_field).map_err(|_| {
            LookupError::Internal(format!("pk column not in declared schema: {pk_field}"))
        })?;
        let pk_sort_fields = vec![SortField::new(
            config.schema.field(pk_idx).data_type().clone(),
        )];
        let aligner = KeyAligner::new(pk_sort_fields, config.primary_key_columns)?;

        let client_options = mongodb::options::ClientOptions::parse(&config.connection_uri)
            .await
            .map_err(|e| LookupError::Connection(format!("mongodb client options: {e}")))?;
        let client = Client::with_options(client_options)
            .map_err(|e| LookupError::Connection(format!("mongodb client: {e}")))?;

        Ok(Self {
            client,
            database: config.database,
            collection: config.collection,
            pk_field,
            schema: config.schema,
            aligner,
        })
    }

    /// Convert one decoded PK cell into a BSON value for the `$in` array, or
    /// `None` when NULL (a NULL key is dropped — it can never match).
    fn cell_to_bson(array: &dyn Array, row: usize) -> Result<Option<Bson>, LookupError> {
        use arrow_array::{
            BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray,
            StringViewArray,
        };

        fn downcast<T: 'static>(array: &dyn Array) -> Result<&T, LookupError> {
            array
                .as_any()
                .downcast_ref::<T>()
                .ok_or_else(|| LookupError::Internal("pk column downcast failed".into()))
        }

        if array.is_null(row) {
            return Ok(None);
        }

        let bson = match array.data_type() {
            DataType::Int32 => Bson::Int32(downcast::<Int32Array>(array)?.value(row)),
            DataType::Int64 => Bson::Int64(downcast::<Int64Array>(array)?.value(row)),
            DataType::Float64 => Bson::Double(downcast::<Float64Array>(array)?.value(row)),
            DataType::Boolean => Bson::Boolean(downcast::<BooleanArray>(array)?.value(row)),
            DataType::Utf8 => Bson::String(downcast::<StringArray>(array)?.value(row).to_string()),
            DataType::LargeUtf8 => {
                Bson::String(downcast::<LargeStringArray>(array)?.value(row).to_string())
            }
            DataType::Utf8View => {
                Bson::String(downcast::<StringViewArray>(array)?.value(row).to_string())
            }
            dt => {
                return Err(LookupError::Internal(format!(
                    "unsupported PK data type for mongodb lookup: {dt}"
                )));
            }
        };
        Ok(Some(bson))
    }

    /// Project fetched documents into one Arrow `RecordBatch` matching
    /// `schema` (the full declared schema, or its projection). Missing or
    /// incompatible fields become NULL.
    fn docs_to_batch(schema: &SchemaRef, docs: &[Document]) -> Result<RecordBatch, LookupError> {
        use arrow_array::builder::{
            BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        };

        let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let name = field.name().as_str();
            let array: Arc<dyn Array> = match field.data_type() {
                DataType::Int32 => {
                    let mut b = Int32Builder::with_capacity(docs.len());
                    for d in docs {
                        b.append_option(
                            bson_as_i64(d.get(name)).and_then(|v| i32::try_from(v).ok()),
                        );
                    }
                    Arc::new(b.finish())
                }
                DataType::Int64 => {
                    let mut b = Int64Builder::with_capacity(docs.len());
                    for d in docs {
                        b.append_option(bson_as_i64(d.get(name)));
                    }
                    Arc::new(b.finish())
                }
                DataType::Float64 => {
                    let mut b = Float64Builder::with_capacity(docs.len());
                    for d in docs {
                        b.append_option(bson_as_f64(d.get(name)));
                    }
                    Arc::new(b.finish())
                }
                DataType::Boolean => {
                    let mut b = BooleanBuilder::with_capacity(docs.len());
                    for d in docs {
                        b.append_option(d.get(name).and_then(Bson::as_bool));
                    }
                    Arc::new(b.finish())
                }
                _ => {
                    // Utf8 (and any non-native declared type): extended-JSON text.
                    let mut b = StringBuilder::with_capacity(docs.len(), docs.len() * 16);
                    for d in docs {
                        match d.get(name) {
                            None | Some(Bson::Null) => b.append_null(),
                            Some(v) => b.append_value(bson_to_string(v)),
                        }
                    }
                    Arc::new(b.finish())
                }
            };
            columns.push(array);
        }
        RecordBatch::try_new(Arc::clone(schema), columns)
            .map_err(|e| LookupError::Internal(format!("arrow batch construction: {e}")))
    }
}

#[cfg(feature = "mongodb-cdc")]
impl LookupSource for MongoLookupSource {
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
        let pk_array = pk_arrays[0].as_ref();
        let mut in_values: Vec<Bson> = Vec::with_capacity(keys.len());
        for row in 0..pk_array.len() {
            if let Some(b) = Self::cell_to_bson(pk_array, row)? {
                in_values.push(b);
            }
        }

        let filter = doc! { &self.pk_field: doc! { "$in": in_values } };
        let collection = self
            .client
            .database(&self.database)
            .collection::<Document>(&self.collection);

        // Projection pushdown: ask Mongo for only the requested fields (always
        // incl. the key), and build the batch in the matching projected schema.
        let mut find = collection.find(filter);
        let out_schema = if projection.is_empty() {
            Arc::clone(&self.schema)
        } else {
            let names = projection_names(&self.schema, projection)?;
            let mut proj_doc = Document::new();
            for name in &names {
                proj_doc.insert(name.clone(), 1);
            }
            find = find.projection(proj_doc);
            let idx: Vec<usize> = projection.iter().map(|&c| c as usize).collect();
            Arc::new(
                self.schema
                    .project(&idx)
                    .map_err(|e| LookupError::Internal(format!("project mongodb schema: {e}")))?,
            )
        };

        let mut cursor = find
            .await
            .map_err(|e| LookupError::Query(format!("mongodb find: {e}")))?;
        let mut docs: Vec<Document> = Vec::new();
        while let Some(next) = cursor.next().await {
            docs.push(next.map_err(|e| LookupError::Query(format!("mongodb cursor: {e}")))?);
        }

        let batches = if docs.is_empty() {
            Vec::new()
        } else {
            vec![Self::docs_to_batch(&out_schema, &docs)?]
        };
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
        "mongodb"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        self.client
            .database(&self.database)
            .run_command(doc! { "ping": 1 })
            .await
            .map(|_| ())
            .map_err(|e| LookupError::Connection(format!("health check: {e}")))
    }
}

/// Extract an integer-valued BSON field as `i64` (Int32/Int64/Double).
#[cfg(feature = "mongodb-cdc")]
fn bson_as_i64(b: Option<&Bson>) -> Option<i64> {
    match b? {
        Bson::Int32(v) => Some(i64::from(*v)),
        Bson::Int64(v) => Some(*v),
        #[allow(clippy::cast_possible_truncation)]
        Bson::Double(v) => Some(*v as i64),
        _ => None,
    }
}

/// Extract a float-valued BSON field as `f64` (Double/Int32/Int64).
#[cfg(feature = "mongodb-cdc")]
fn bson_as_f64(b: Option<&Bson>) -> Option<f64> {
    match b? {
        Bson::Double(v) => Some(*v),
        Bson::Int32(v) => Some(f64::from(*v)),
        #[allow(clippy::cast_precision_loss)]
        Bson::Int64(v) => Some(*v as f64),
        _ => None,
    }
}

/// Render a BSON value as a string cell (scalars verbatim, others as JSON).
#[cfg(feature = "mongodb-cdc")]
fn bson_to_string(b: &Bson) -> String {
    match b {
        Bson::String(s) => s.clone(),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::Int32(v) => v.to_string(),
        Bson::Int64(v) => v.to_string(),
        Bson::Double(v) => v.to_string(),
        Bson::Boolean(v) => v.to_string(),
        other => other.to_string(),
    }
}

#[cfg(all(test, feature = "mongodb-cdc"))]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};

    #[test]
    fn cell_to_bson_types_and_null() {
        assert_eq!(
            MongoLookupSource::cell_to_bson(&Int64Array::from(vec![7i64]), 0).unwrap(),
            Some(Bson::Int64(7))
        );
        assert_eq!(
            MongoLookupSource::cell_to_bson(&StringArray::from(vec!["k"]), 0).unwrap(),
            Some(Bson::String("k".into()))
        );
        let nullable = Int64Array::from(vec![None, Some(1)]);
        assert!(MongoLookupSource::cell_to_bson(&nullable, 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn cell_to_bson_rejects_unsupported_type() {
        assert!(
            MongoLookupSource::cell_to_bson(&arrow_array::Date32Array::from(vec![1]), 0).is_err()
        );
    }

    #[test]
    fn bson_numeric_coercion() {
        assert_eq!(bson_as_i64(Some(&Bson::Double(3.9))), Some(3));
        assert_eq!(bson_as_f64(Some(&Bson::Int64(5))), Some(5.0));
        assert_eq!(bson_as_i64(Some(&Bson::String("x".into()))), None);
        assert_eq!(bson_as_i64(None), None);
    }
}
