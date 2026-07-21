//! `MongoDB` on-demand lookup source for cache-miss fallback.
//!
//! Implements `LookupSource` via a multi-get on the indexed key
//! (`find({ pk: { $in: [keys] } })`), so all missed keys of a probe fold into
//! one round trip. `MongoDB` is schemaless, so the source projects each
//! returned document into the table's **declared** Arrow schema (from
//! `CREATE LOOKUP TABLE`); [`KeyAligner`] handles key decode and realignment.
//!
//! v1 limits: single-column key; declared column types Int32/Int64/Float64/
//! Boolean/Utf8/LargeUtf8.

#[cfg(feature = "mongodb-cdc")]
use std::sync::Arc;
#[cfg(feature = "mongodb-cdc")]
use std::time::Duration;

#[cfg(feature = "mongodb-cdc")]
use arrow_array::{Array, RecordBatch};
#[cfg(feature = "mongodb-cdc")]
use arrow_row::SortField;
#[cfg(feature = "mongodb-cdc")]
use arrow_schema::{DataType, SchemaRef};
#[cfg(feature = "mongodb-cdc")]
use mongodb::bson::{doc, Bson, Document, RawDocumentBuf};
#[cfg(feature = "mongodb-cdc")]
use mongodb::{Client, IndexModel};

#[cfg(feature = "mongodb-cdc")]
use laminar_core::lookup::predicate::Predicate;
#[cfg(feature = "mongodb-cdc")]
use laminar_core::lookup::source::{
    projection_names, ColumnId, LookupError, LookupSource, LookupSourceCapabilities,
};
#[cfg(feature = "mongodb-cdc")]
use laminar_core::lookup::KeyAligner;

#[cfg(feature = "mongodb-cdc")]
const LOOKUP_SERVER_TIMEOUT: Duration = Duration::from_secs(25);
#[cfg(feature = "mongodb-cdc")]
const MAX_LOOKUP_KEYS: usize = 4_096;
#[cfg(feature = "mongodb-cdc")]
const MAX_LOOKUP_KEY_BYTES: usize = 4 * 1024 * 1024;
#[cfg(feature = "mongodb-cdc")]
const MAX_LOOKUP_COMMAND_BYTES: usize = 8 * 1024 * 1024;
#[cfg(feature = "mongodb-cdc")]
const MAX_LOOKUP_RESULT_BYTES: usize = 64 * 1024 * 1024;

#[cfg(feature = "mongodb-cdc")]
fn sharded_lookup_not_admitted() -> LookupError {
    LookupError::Internal(
        "mongodb sharded lookup through mongos/load-balanced topology is not admitted until \
         shard-key routing and cluster-global uniqueness certification are implemented"
            .into(),
    )
}

#[cfg(feature = "mongodb-cdc")]
fn validate_lookup_server_topology(hello: &Document) -> Result<(), LookupError> {
    if matches!(hello.get_str("msg"), Ok("isdbgrid")) {
        return Err(sharded_lookup_not_admitted());
    }
    Ok(())
}

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
fn validate_lookup_keys(keys: &[&[u8]]) -> Result<(), LookupError> {
    if keys.len() > MAX_LOOKUP_KEYS {
        return Err(LookupError::Query(format!(
            "mongodb lookup received {} keys, exceeding the fixed {MAX_LOOKUP_KEYS}-key batch limit",
            keys.len()
        )));
    }
    let bytes = keys.iter().try_fold(0_usize, |total, key| {
        total
            .checked_add(key.len())
            .ok_or_else(|| LookupError::Query("mongodb lookup key byte count overflow".into()))
    })?;
    if bytes > MAX_LOOKUP_KEY_BYTES {
        return Err(LookupError::Query(format!(
            "mongodb lookup received {bytes} key bytes, exceeding the fixed {MAX_LOOKUP_KEY_BYTES}-byte batch limit"
        )));
    }
    Ok(())
}

#[cfg(feature = "mongodb-cdc")]
fn validate_lookup_command(
    database: &str,
    collection: &str,
    filter: &Document,
    projection: Option<&Document>,
    limit: i64,
) -> Result<(), LookupError> {
    #[derive(serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct FindCommand<'a> {
        find: &'a str,
        #[serde(rename = "$db")]
        database: &'a str,
        filter: &'a Document,
        #[serde(skip_serializing_if = "Option::is_none")]
        projection: Option<&'a Document>,
        limit: i64,
        #[serde(rename = "maxTimeMS")]
        max_time_ms: u64,
    }

    let max_time_ms = u64::try_from(LOOKUP_SERVER_TIMEOUT.as_millis()).map_err(|_| {
        LookupError::Internal("mongodb lookup server timeout exceeds u64 milliseconds".into())
    })?;
    let command = FindCommand {
        find: collection,
        database,
        filter,
        projection,
        limit,
        max_time_ms,
    };
    let bytes = mongodb::bson::to_vec(&command)
        .map_err(|error| LookupError::Query(format!("serialize mongodb find command: {error}")))?;
    if bytes.len() > MAX_LOOKUP_COMMAND_BYTES {
        return Err(LookupError::Query(format!(
            "mongodb lookup command is {} bytes, exceeding the fixed {MAX_LOOKUP_COMMAND_BYTES}-byte limit",
            bytes.len()
        )));
    }
    Ok(())
}

#[cfg(feature = "mongodb-cdc")]
fn has_usable_unique_lookup_index(index: &IndexModel, pk_field: &str) -> bool {
    if index.keys.len() != 1 {
        return false;
    }
    let Some((field, direction)) = index.keys.iter().next() else {
        return false;
    };
    let is_ascending_or_descending = match direction {
        Bson::Int32(value) => *value == 1 || *value == -1,
        Bson::Int64(value) => *value == 1 || *value == -1,
        Bson::Double(value) => value.abs().total_cmp(&1.0).is_eq(),
        _ => false,
    };
    if field != pk_field || !is_ascending_or_descending {
        return false;
    }

    let Some(options) = index.options.as_ref() else {
        return false;
    };
    if options.hidden == Some(true)
        || options.partial_filter_expression.is_some()
        || options.collation.is_some()
    {
        return false;
    }

    options.unique == Some(true) || (pk_field == "_id" && options.name.as_deref() == Some("_id_"))
}

#[cfg(feature = "mongodb-cdc")]
async fn verify_unique_lookup_index(
    client: Client,
    database: String,
    collection: String,
    pk_field: String,
) -> Result<(), LookupError> {
    use tokio_stream::StreamExt;

    let hello = client
        .database("admin")
        .run_command(doc! { "hello": 1 })
        .await
        .map_err(|error| {
            LookupError::Connection(format!("inspect mongodb lookup topology: {error}"))
        })?;
    validate_lookup_server_topology(&hello)?;

    let collection_handle = client
        .database(&database)
        .collection::<Document>(&collection);
    let mut indexes = collection_handle
        .list_indexes()
        .max_time(LOOKUP_SERVER_TIMEOUT)
        .await
        .map_err(|error| {
            LookupError::Connection(format!(
                "inspect mongodb lookup indexes for {database}.{collection}: {error}"
            ))
        })?;
    while let Some(next) = indexes.next().await {
        let index = next.map_err(|error| {
            LookupError::Connection(format!(
                "read mongodb lookup indexes for {database}.{collection}: {error}"
            ))
        })?;
        if has_usable_unique_lookup_index(&index, &pk_field) {
            return Ok(());
        }
    }

    Err(LookupError::Internal(format!(
        "mongodb lookup requires a visible, non-partial, simple-collation unique single-field ascending/descending index on '{pk_field}' in {database}.{collection}"
    )))
}

#[cfg(feature = "mongodb-cdc")]
impl MongoLookupSource {
    /// Connects to `MongoDB` and validates the declared key column and index.
    ///
    /// # Errors
    ///
    /// Returns `LookupError` if the client cannot be built, the key declaration
    /// is invalid, or the collection has no usable unique index on that key.
    pub async fn open(config: MongoLookupSourceConfig) -> Result<Self, LookupError> {
        if config.connection_uri.trim().is_empty()
            || config.database.trim().is_empty()
            || config.collection.trim().is_empty()
        {
            return Err(LookupError::Internal(
                "mongodb lookup requires non-empty connection_uri, database, and collection".into(),
            ));
        }
        if config.collection == "*" {
            return Err(LookupError::Internal(
                "mongodb lookup does not support collection='*'".into(),
            ));
        }
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

        for field in config.schema.fields() {
            match field.data_type() {
                DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Boolean
                | DataType::Utf8
                | DataType::LargeUtf8 => {}
                dt => {
                    return Err(LookupError::Internal(format!(
                        "unsupported field data type in schema for mongodb lookup: {dt}"
                    )));
                }
            }
        }

        let pk_sort_fields = vec![SortField::new(
            config.schema.field(pk_idx).data_type().clone(),
        )];
        let aligner = KeyAligner::new(pk_sort_fields, config.primary_key_columns)?;

        let connection_uri = config.connection_uri.clone();
        let mut client_options =
            tokio::spawn(
                async move { mongodb::options::ClientOptions::parse(&connection_uri).await },
            )
            .await
            .map_err(|error| {
                LookupError::Internal(format!("mongodb client-options task failed: {error}"))
            })?
            .map_err(|e| LookupError::Connection(format!("mongodb client options: {e}")))?;
        if client_options.load_balanced == Some(true) {
            return Err(sharded_lookup_not_admitted());
        }
        super::sink::harden_mongodb_tls(&mut client_options)
            .map_err(|e| LookupError::Connection(e.to_string()))?;
        client_options.connect_timeout = Some(
            client_options
                .connect_timeout
                .unwrap_or(LOOKUP_SERVER_TIMEOUT)
                .min(LOOKUP_SERVER_TIMEOUT),
        );
        client_options.server_selection_timeout = Some(
            client_options
                .server_selection_timeout
                .unwrap_or(LOOKUP_SERVER_TIMEOUT)
                .min(LOOKUP_SERVER_TIMEOUT),
        );
        let client = Client::with_options(client_options)
            .map_err(|e| LookupError::Connection(format!("mongodb client: {e}")))?;

        let index_client = client.clone();
        let index_database = config.database.clone();
        let index_collection = config.collection.clone();
        let index_pk_field = pk_field.clone();
        tokio::spawn(async move {
            verify_unique_lookup_index(
                index_client,
                index_database,
                index_collection,
                index_pk_field,
            )
            .await
        })
        .await
        .map_err(|error| {
            LookupError::Internal(format!("mongodb index-check task failed: {error}"))
        })??;

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
            BooleanBuilder, Float64Builder, Int32Builder, Int64Builder, LargeStringBuilder,
            StringBuilder,
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
                DataType::LargeUtf8 => {
                    let mut b = LargeStringBuilder::with_capacity(docs.len(), docs.len() * 16);
                    for d in docs {
                        match d.get(name) {
                            None | Some(Bson::Null) => b.append_null(),
                            Some(v) => b.append_value(bson_to_string(v)),
                        }
                    }
                    Arc::new(b.finish())
                }
                DataType::Utf8 => {
                    let mut b = StringBuilder::with_capacity(docs.len(), docs.len() * 16);
                    for d in docs {
                        match d.get(name) {
                            None | Some(Bson::Null) => b.append_null(),
                            Some(v) => b.append_value(bson_to_string(v)),
                        }
                    }
                    Arc::new(b.finish())
                }
                _ => {
                    return Err(LookupError::Internal(format!(
                        "unsupported field data type: {:?}",
                        field.data_type()
                    )));
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
        validate_lookup_keys(keys)?;

        let pk_arrays = self.aligner.decode_keys(keys)?;
        let pk_array = pk_arrays[0].as_ref();
        let mut in_values: Vec<Bson> = Vec::with_capacity(keys.len());
        for row in 0..pk_array.len() {
            if let Some(b) = Self::cell_to_bson(pk_array, row)? {
                in_values.push(b);
            }
        }
        if in_values.is_empty() {
            return Ok(vec![None; keys.len()]);
        }
        let result_limit = i64::try_from(in_values.len()).map_err(|_| {
            LookupError::Internal("mongodb lookup key count exceeds i64::MAX".into())
        })?;

        let filter = doc! { &self.pk_field: doc! { "$in": in_values } };

        // Projection pushdown: ask Mongo for only the requested fields (always
        // incl. the key), and build the batch in the matching projected schema.
        let mut project_needed = false;
        let (out_schema, projection_document) =
            if projection.is_empty() {
                (Arc::clone(&self.schema), None)
            } else {
                let mut names = projection_names(&self.schema, projection)?;
                let mut idx: Vec<usize> = projection.iter().map(|&c| c as usize).collect();
                if !names.contains(&self.pk_field) {
                    names.push(self.pk_field.clone());
                    let pk_idx = self
                        .schema
                        .index_of(&self.pk_field)
                        .map_err(|e| LookupError::Internal(format!("pk column index: {e}")))?;
                    idx.push(pk_idx);
                    project_needed = true;
                }

                let mut proj_doc = Document::new();
                for name in &names {
                    proj_doc.insert(name.clone(), 1);
                }
                (
                    Arc::new(self.schema.project(&idx).map_err(|e| {
                        LookupError::Internal(format!("project mongodb schema: {e}"))
                    })?),
                    Some(proj_doc),
                )
            };

        validate_lookup_command(
            &self.database,
            &self.collection,
            &filter,
            projection_document.as_ref(),
            result_limit,
        )?;

        // The runtime may time out and drop query(). Keep every cancellation-unsafe MongoDB
        // driver future in an owned task so it is still polled to completion in that case.
        let client = self.client.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let task_schema = Arc::clone(&out_schema);
        let batches = tokio::spawn(async move {
            let collection = client
                .database(&database)
                .collection::<RawDocumentBuf>(&collection);
            let mut find = collection
                .find(filter)
                .limit(result_limit)
                .max_time(LOOKUP_SERVER_TIMEOUT);
            if let Some(projection) = projection_document {
                find = find.projection(projection);
            }
            let mut cursor = find
                .await
                .map_err(|e| LookupError::Query(format!("mongodb find: {e}")))?;
            let mut docs: Vec<Document> = Vec::new();
            let mut result_bytes = 0_usize;
            while let Some(next) = cursor.next().await {
                let raw = next.map_err(|e| LookupError::Query(format!("mongodb cursor: {e}")))?;
                result_bytes = result_bytes
                    .checked_add(raw.as_bytes().len())
                    .ok_or_else(|| {
                        LookupError::Query("mongodb lookup result byte count overflow".into())
                    })?;
                if result_bytes > MAX_LOOKUP_RESULT_BYTES {
                    return Err(LookupError::Query(format!(
                        "mongodb lookup result is at least {result_bytes} bytes, exceeding the fixed {MAX_LOOKUP_RESULT_BYTES}-byte limit"
                    )));
                }
                docs.push(raw.to_document().map_err(|error| {
                    LookupError::Query(format!("decode mongodb lookup result: {error}"))
                })?);
            }
            if docs.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(vec![Self::docs_to_batch(&task_schema, &docs)?])
            }
        })
        .await
        .map_err(|error| LookupError::Internal(format!("mongodb lookup task failed: {error}")))??;
        let aligned = self.aligner.align(keys, &batches)?;

        if project_needed {
            let orig_names = projection_names(&self.schema, projection)?;
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
            max_batch_size: MAX_LOOKUP_KEYS,
            ..LookupSourceCapabilities::none()
        }
    }

    fn source_name(&self) -> &'static str {
        "mongodb"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn health_check(&self) -> Result<(), LookupError> {
        let client = self.client.clone();
        let database = self.database.clone();
        tokio::spawn(async move {
            client
                .database(&database)
                .run_command(doc! { "ping": 1 })
                .await
                .map(|_| ())
                .map_err(|e| LookupError::Connection(format!("health check: {e}")))
        })
        .await
        .map_err(|error| LookupError::Internal(format!("mongodb health task failed: {error}")))?
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
    use arrow_schema::{Field, Schema};
    use mongodb::options::{Collation, IndexOptions};

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

    #[test]
    fn lookup_key_limits_are_enforced_before_decode() {
        assert!(validate_lookup_keys(&[&b"a"[..], &b"bc"[..]]).is_ok());

        let too_many = vec![&b""[..]; MAX_LOOKUP_KEYS + 1];
        assert!(validate_lookup_keys(&too_many).is_err());

        let oversized = vec![0_u8; MAX_LOOKUP_KEY_BYTES + 1];
        assert!(validate_lookup_keys(&[oversized.as_slice()]).is_err());
    }

    #[test]
    fn lookup_command_limit_leaves_bson_headroom() {
        let small = doc! { "id": doc! { "$in": [1_i64, 2_i64] } };
        assert!(validate_lookup_command("db", "items", &small, None, 2).is_ok());

        let oversized = doc! {
            "id": doc! { "$in": ["x".repeat(MAX_LOOKUP_COMMAND_BYTES)] }
        };
        let error = validate_lookup_command("db", "items", &oversized, None, 1)
            .expect_err("oversized command must be rejected before I/O");
        assert!(error.to_string().contains("lookup command"));
    }

    #[test]
    fn lookup_index_must_uniquely_cover_the_key() {
        let unique = IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        assert!(has_usable_unique_lookup_index(&unique, "id"));

        let non_unique = IndexModel::builder().keys(doc! { "id": 1 }).build();
        assert!(!has_usable_unique_lookup_index(&non_unique, "id"));

        let compound = IndexModel::builder()
            .keys(doc! { "id": 1, "tenant": 1 })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        assert!(!has_usable_unique_lookup_index(&compound, "id"));

        let partial = IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    .partial_filter_expression(doc! { "active": true })
                    .build(),
            )
            .build();
        assert!(!has_usable_unique_lookup_index(&partial, "id"));

        let hidden = IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(IndexOptions::builder().unique(true).hidden(true).build())
            .build();
        assert!(!has_usable_unique_lookup_index(&hidden, "id"));

        let collated = IndexModel::builder()
            .keys(doc! { "id": 1 })
            .options(
                IndexOptions::builder()
                    .unique(true)
                    .collation(Collation::builder().locale("en").build())
                    .build(),
            )
            .build();
        assert!(!has_usable_unique_lookup_index(&collated, "id"));

        let hashed = IndexModel::builder()
            .keys(doc! { "id": "hashed" })
            .options(IndexOptions::builder().unique(true).build())
            .build();
        assert!(!has_usable_unique_lookup_index(&hashed, "id"));

        let implicit_id = IndexModel::builder()
            .keys(doc! { "_id": 1 })
            .options(
                IndexOptions::builder()
                    .name(Some("_id_".to_owned()))
                    .build(),
            )
            .build();
        assert!(has_usable_unique_lookup_index(&implicit_id, "_id"));
    }

    #[test]
    fn mongos_topology_is_rejected_before_index_admission() {
        let error = validate_lookup_server_topology(&doc! {
            "msg": "isdbgrid",
            "isWritablePrimary": true
        })
        .expect_err("mongos cannot prove cluster-global key uniqueness");
        assert!(error.to_string().contains("shard-key routing"));
        assert!(error.to_string().contains("cluster-global uniqueness"));

        assert!(validate_lookup_server_topology(&doc! {
            "setName": "rs0",
            "isWritablePrimary": true
        })
        .is_ok());
        assert!(validate_lookup_server_topology(&doc! {
            "isWritablePrimary": true
        })
        .is_ok());
    }

    #[tokio::test]
    async fn load_balanced_topology_is_rejected_before_client_construction() {
        let config = MongoLookupSourceConfig {
            connection_uri: "mongodb://localhost:27017/?loadBalanced=true&tls=false".into(),
            database: "db".into(),
            collection: "items".into(),
            primary_key_columns: vec!["id".into()],
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        };
        let error = MongoLookupSource::open(config)
            .await
            .err()
            .expect("load-balanced lookup must fail before server selection");
        assert!(error.to_string().contains("load-balanced topology"));
        assert!(error.to_string().contains("cluster-global uniqueness"));
    }

    #[tokio::test]
    async fn wildcard_collection_is_rejected_before_network_io() {
        let config = MongoLookupSourceConfig {
            connection_uri: "mongodb://localhost:27017".into(),
            database: "db".into(),
            collection: "*".into(),
            primary_key_columns: vec!["id".into()],
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        };
        let error = MongoLookupSource::open(config)
            .await
            .err()
            .expect("wildcard lookup must be rejected");
        assert!(error.to_string().contains("collection='*'"));
    }
}
