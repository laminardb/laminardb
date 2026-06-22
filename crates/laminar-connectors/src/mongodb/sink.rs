//! `MongoDB` sink connector implementation.
//!
//! Implements [`SinkConnector`] for writing Arrow `RecordBatch` data to
//! `MongoDB` collections. Supports insert, upsert, replace, and CDC replay
//! write modes, with optional time series collection support.
//!
//! # Architecture
//!
//! - **Ring 0**: No sink code — data arrives via SPSC channel (~5ns push)
//! - **Ring 1**: Batch buffering, write dispatch, flush management
//! - **Ring 2**: Connection pool, collection creation, write concern config
//!
//! # Batching
//!
//! Writes are buffered up to `batch_size` records and flushed when:
//! - The batch is full
//! - `flush_interval` has elapsed
//! - A shutdown signal or epoch boundary is reached
//!
//! Insert mode uses `insert_many` for batch efficiency. Upsert, replace,
//! and CDC replay modes issue individual operations per document.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use tracing::{debug, info};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;

use super::config::MongoDbSinkConfig;
use super::metrics::MongoDbSinkMetrics;
use super::timeseries::CollectionKind;
use super::write_model::WriteMode;

/// `MongoDB` sink connector.
///
/// Writes Arrow `RecordBatch` records to a `MongoDB` collection using
/// configurable write modes. Supports standard and time series collections.
pub struct MongoDbSink {
    /// Sink configuration.
    config: MongoDbSinkConfig,

    /// Arrow schema for input batches.
    schema: SchemaRef,

    /// Connector lifecycle state.
    state: ConnectorState,

    /// Buffered records awaiting flush.
    buffer: Vec<RecordBatch>,

    /// Total rows in buffer.
    buffered_rows: usize,

    /// Last flush time.
    last_flush: Instant,

    /// Sink metrics.
    metrics: MongoDbSinkMetrics,

    /// `MongoDB` client (feature-gated).
    #[cfg(feature = "mongodb-cdc")]
    client: Option<mongodb::Client>,

    /// Target collection handle (feature-gated).
    #[cfg(feature = "mongodb-cdc")]
    collection: Option<mongodb::Collection<mongodb::bson::Document>>,
}

impl MongoDbSink {
    /// Creates a new `MongoDB` sink connector.
    #[must_use]
    pub fn new(
        schema: SchemaRef,
        config: MongoDbSinkConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Self {
        let buf_capacity = (config.batch_size / 128).max(4);
        Self {
            config,
            schema,
            state: ConnectorState::Created,
            buffer: Vec::with_capacity(buf_capacity),
            buffered_rows: 0,
            last_flush: Instant::now(),
            metrics: MongoDbSinkMetrics::new(registry),
            #[cfg(feature = "mongodb-cdc")]
            client: None,
            #[cfg(feature = "mongodb-cdc")]
            collection: None,
        }
    }

    /// Creates a new sink from a generic [`ConnectorConfig`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the configuration is invalid.
    pub fn from_config(
        schema: SchemaRef,
        config: &ConnectorConfig,
    ) -> Result<Self, ConnectorError> {
        let mongo_config = MongoDbSinkConfig::from_config(config)?;
        Ok(Self::new(schema, mongo_config, None))
    }

    /// Returns a reference to the sink configuration.
    #[must_use]
    pub fn config(&self) -> &MongoDbSinkConfig {
        &self.config
    }

    /// Returns the number of buffered rows.
    #[must_use]
    pub fn buffered_rows(&self) -> usize {
        self.buffered_rows
    }

    /// Returns whether a flush is needed based on batch size or interval.
    #[must_use]
    fn should_flush(&self) -> bool {
        self.buffered_rows >= self.config.batch_size
            || self.last_flush.elapsed() >= self.config.flush_interval()
    }

    /// Converts buffered Arrow batches to JSON documents for writing.
    ///
    /// Returns `(docs, byte_estimate)`. The byte estimate is accumulated
    /// during conversion to avoid a redundant serialization pass later.
    fn batches_to_json_docs(&self) -> (Vec<serde_json::Value>, u64) {
        let mut docs = Vec::with_capacity(self.buffered_rows);
        let mut byte_estimate: u64 = 0;

        for batch in &self.buffer {
            for row_idx in 0..batch.num_rows() {
                let mut doc = serde_json::Map::new();

                for (col_idx, field) in batch.schema().fields().iter().enumerate() {
                    let col = batch.column(col_idx);
                    let value = arrow_value_to_json(col, row_idx);
                    doc.insert(field.name().clone(), value);
                }

                let val = serde_json::Value::Object(doc);
                byte_estimate += serde_json::to_string(&val).map_or(0, |s| s.len() as u64);
                docs.push(val);
            }
        }

        (docs, byte_estimate)
    }

    /// Arrow batches → BSON documents directly (no `serde_json::Value` hop), for the
    /// insert/upsert/replace paths. Returns `(docs, byte_estimate)`.
    #[cfg(feature = "mongodb-cdc")]
    fn batches_to_bson_docs(&self) -> (Vec<mongodb::bson::Document>, u64) {
        let mut docs = Vec::with_capacity(self.buffered_rows);
        let mut bytes: u64 = 0;
        for batch in &self.buffer {
            let schema = batch.schema();
            for row in 0..batch.num_rows() {
                let mut doc = mongodb::bson::Document::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let value = arrow_value_to_bson(batch.column(col_idx), row);
                    bytes += bson_size(&value) + field.name().len() as u64;
                    doc.insert(field.name().clone(), value);
                }
                docs.push(doc);
            }
        }
        (docs, bytes)
    }

    /// Clears the internal buffer after a flush.
    fn clear_buffer(&mut self) {
        self.buffer.clear();
        self.buffered_rows = 0;
        self.last_flush = Instant::now();
    }

    /// Internal flush that returns a [`WriteResult`] with actual counts.
    ///
    /// Both `write_batch` (on auto-flush) and `flush` delegate here.
    async fn flush_inner(&mut self) -> Result<WriteResult, ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        // CDC replay parses a JSON envelope (_op/_full_document/_update_desc), so it
        // keeps the serde_json path; every other mode goes Arrow → BSON directly.
        #[cfg(feature = "mongodb-cdc")]
        let (doc_count, byte_estimate) = if matches!(self.config.write_mode, WriteMode::CdcReplay) {
            let (docs, bytes) = self.batches_to_json_docs();
            let n = docs.len();
            self.write_cdc_docs(&docs).await?;
            (n, bytes)
        } else {
            let (docs, bytes) = self.batches_to_bson_docs();
            let n = docs.len();
            self.write_bson_docs(docs).await?;
            (n, bytes)
        };

        #[cfg(not(feature = "mongodb-cdc"))]
        let (doc_count, byte_estimate) = {
            let (docs, bytes) = self.batches_to_json_docs();
            debug!(
                count = docs.len(),
                "flush (no-op without mongodb-cdc feature)"
            );
            (docs.len(), bytes)
        };

        self.metrics.record_flush(doc_count as u64, byte_estimate);
        self.clear_buffer();

        Ok(WriteResult::new(doc_count, byte_estimate))
    }
}

/// Extracts a JSON value from an Arrow array at the given row index.
fn arrow_value_to_json(col: &dyn arrow_array::Array, row: usize) -> serde_json::Value {
    use arrow_array::{BooleanArray, LargeStringArray, StringArray};

    if col.is_null(row) {
        return serde_json::Value::Null;
    }

    match col.data_type() {
        DataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(row))
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => json_from_primitive(col, row),
        DataType::Float32 | DataType::Float64 => json_from_float(col, row),
        DataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(row).to_string())
        }
        DataType::Timestamp(..) => serde_json::json!({
            "$date": { "$numberLong": timestamp_millis(col, row).to_string() }
        }),
        _ => {
            // Fallback: use Arrow's display format.
            let formatted = arrow_cast::display::ArrayFormatter::try_new(
                col,
                &arrow_cast::display::FormatOptions::default(),
            );
            match formatted {
                Ok(fmt) => serde_json::Value::String(fmt.value(row).to_string()),
                Err(_) => serde_json::Value::Null,
            }
        }
    }
}

/// Helper to extract a JSON number from a primitive Arrow array.
fn json_from_primitive(col: &dyn arrow_array::Array, row: usize) -> serde_json::Value {
    let formatted = arrow_cast::display::ArrayFormatter::try_new(
        col,
        &arrow_cast::display::FormatOptions::default(),
    );
    match formatted {
        Ok(fmt) => {
            let s = fmt.value(row).to_string();
            if let Ok(n) = s.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::String(s)
            }
        }
        Err(_) => serde_json::Value::Null,
    }
}

/// Helper to extract a JSON number from a float Arrow array.
fn json_from_float(col: &dyn arrow_array::Array, row: usize) -> serde_json::Value {
    let formatted = arrow_cast::display::ArrayFormatter::try_new(
        col,
        &arrow_cast::display::FormatOptions::default(),
    );
    match formatted {
        Ok(fmt) => {
            let s = fmt.value(row).to_string();
            if let Ok(n) = s.parse::<f64>() {
                serde_json::json!(n)
            } else {
                serde_json::Value::String(s)
            }
        }
        Err(_) => serde_json::Value::Null,
    }
}

/// Milliseconds since epoch for a timestamp column cell, normalizing the unit.
/// Sub-millisecond precision is truncated (BSON dates are millisecond-resolution).
fn timestamp_millis(col: &dyn arrow_array::Array, row: usize) -> i64 {
    use arrow_array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let DataType::Timestamp(unit, _) = col.data_type() else {
        return 0;
    };
    let a = col.as_any();
    match unit {
        arrow_schema::TimeUnit::Second => {
            a.downcast_ref::<TimestampSecondArray>().unwrap().value(row) * 1000
        }
        arrow_schema::TimeUnit::Millisecond => a
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(row),
        arrow_schema::TimeUnit::Microsecond => {
            a.downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(row)
                / 1000
        }
        arrow_schema::TimeUnit::Nanosecond => {
            a.downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row)
                / 1_000_000
        }
    }
}

/// Convert one Arrow cell straight to BSON for the insert/upsert/replace paths,
/// skipping the `serde_json::Value` intermediate (and the number string round-trip)
/// that the CDC path still needs. Integers stay width-faithful; timestamps become
/// BSON dates; anything without a native BSON scalar falls back to a display string.
#[cfg(feature = "mongodb-cdc")]
fn arrow_value_to_bson(col: &dyn arrow_array::Array, row: usize) -> mongodb::bson::Bson {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use mongodb::bson::Bson;

    if col.is_null(row) {
        return Bson::Null;
    }
    let a = col.as_any();
    let i32_of = |v: i32| Bson::Int32(v);
    match col.data_type() {
        DataType::Boolean => Bson::Boolean(a.downcast_ref::<BooleanArray>().unwrap().value(row)),
        DataType::Int8 => i32_of(i32::from(a.downcast_ref::<Int8Array>().unwrap().value(row))),
        DataType::Int16 => i32_of(i32::from(
            a.downcast_ref::<Int16Array>().unwrap().value(row),
        )),
        DataType::Int32 => i32_of(a.downcast_ref::<Int32Array>().unwrap().value(row)),
        DataType::Int64 => Bson::Int64(a.downcast_ref::<Int64Array>().unwrap().value(row)),
        DataType::UInt8 => i32_of(i32::from(
            a.downcast_ref::<UInt8Array>().unwrap().value(row),
        )),
        DataType::UInt16 => i32_of(i32::from(
            a.downcast_ref::<UInt16Array>().unwrap().value(row),
        )),
        DataType::UInt32 => Bson::Int64(i64::from(
            a.downcast_ref::<UInt32Array>().unwrap().value(row),
        )),
        DataType::UInt64 => {
            let v = a.downcast_ref::<UInt64Array>().unwrap().value(row);
            i64::try_from(v).map_or_else(|_| Bson::String(v.to_string()), Bson::Int64)
        }
        DataType::Float32 => Bson::Double(f64::from(
            a.downcast_ref::<Float32Array>().unwrap().value(row),
        )),
        DataType::Float64 => Bson::Double(a.downcast_ref::<Float64Array>().unwrap().value(row)),
        DataType::Utf8 => Bson::String(
            a.downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::LargeUtf8 => Bson::String(
            a.downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::Timestamp(..) => Bson::DateTime(mongodb::bson::DateTime::from_millis(
            timestamp_millis(col, row),
        )),
        _ => match arrow_cast::display::ArrayFormatter::try_new(
            col,
            &arrow_cast::display::FormatOptions::default(),
        ) {
            Ok(fmt) => Bson::String(fmt.value(row).to_string()),
            Err(_) => Bson::Null,
        },
    }
}

/// Rough wire-size of a scalar BSON value, for the throughput metric (avoids a
/// serialization pass just to count bytes).
#[cfg(feature = "mongodb-cdc")]
fn bson_size(v: &mongodb::bson::Bson) -> u64 {
    use mongodb::bson::Bson;
    match v {
        Bson::String(s) => s.len() as u64 + 5,
        Bson::Boolean(_) | Bson::Int32(_) => 5,
        Bson::Int64(_) | Bson::Double(_) | Bson::DateTime(_) => 9,
        Bson::Null => 1,
        _ => 16,
    }
}

/// Convert a JSON value to BSON, interpreting `MongoDB` Extended JSON (e.g. the
/// `$date` emitted for timestamp columns). serde's structural `to_bson`/`to_document`
/// would store `$date` as a literal sub-document, not a BSON date.
#[cfg(feature = "mongodb-cdc")]
fn json_to_bson(value: &serde_json::Value) -> Result<mongodb::bson::Bson, ConnectorError> {
    mongodb::bson::Bson::try_from(value.clone())
        .map_err(|e| ConnectorError::WriteError(format!("JSON to BSON: {e}")))
}

#[cfg(feature = "mongodb-cdc")]
fn json_to_bson_document(
    value: &serde_json::Value,
) -> Result<mongodb::bson::Document, ConnectorError> {
    match json_to_bson(value)? {
        mongodb::bson::Bson::Document(doc) => Ok(doc),
        other => Err(ConnectorError::WriteError(format!(
            "expected a BSON document, got {:?}",
            other.element_type()
        ))),
    }
}

#[async_trait]
impl SinkConnector for MongoDbSink {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if !config.properties().is_empty() {
            self.config = MongoDbSinkConfig::from_config(config)?;
        }
        self.config.validate()?;

        #[cfg(feature = "mongodb-cdc")]
        {
            self.connect().await?;
        }

        self.state = ConnectorState::Running;
        info!(
            database = %self.config.database,
            collection = %self.config.collection,
            write_mode = ?self.config.write_mode,
            ordered = self.config.ordered,
            "MongoDB sink opened"
        );

        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        self.buffer.push(batch.clone());
        self.buffered_rows += rows;

        if self.should_flush() {
            return self.flush_inner().await;
        }

        // Just buffered, nothing written yet.
        Ok(WriteResult::new(0, 0))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::new(Duration::from_secs(30)).with_idempotent();

        if matches!(self.config.write_mode, WriteMode::Upsert { .. }) {
            caps = caps.with_upsert();
        }
        if matches!(self.config.write_mode, WriteMode::CdcReplay) {
            caps = caps.with_changelog();
        }

        caps
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flush_inner().await.map(|_| ())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        // Flush remaining buffered data.
        if !self.buffer.is_empty() {
            self.flush().await?;
        }

        self.state = ConnectorState::Closed;
        info!("MongoDB sink closed");
        Ok(())
    }
}

// ── Feature-gated I/O (real MongoDB driver) ──

#[cfg(feature = "mongodb-cdc")]
impl MongoDbSink {
    /// Connects to `MongoDB` and sets up the target collection with write concern.
    async fn connect(&mut self) -> Result<(), ConnectorError> {
        use mongodb::options::{ClientOptions, CollectionOptions};

        let client_options = ClientOptions::parse(&self.config.connection_uri)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("parse URI: {e}")))?;

        let client = mongodb::Client::with_options(client_options)
            .map_err(|e| ConnectorError::ConnectionFailed(format!("create client: {e}")))?;

        let db = client.database(&self.config.database);

        // Ensure time series collection exists if configured.
        if let CollectionKind::TimeSeries(ref ts_config) = self.config.collection_kind {
            self.ensure_timeseries_collection(&db, ts_config).await?;
        }

        // Apply write concern from configuration.
        let wc = {
            use super::config::WriteConcernLevel;
            let mut wc = mongodb::options::WriteConcern::default();
            wc.w = Some(match &self.config.write_concern.w {
                WriteConcernLevel::Majority => mongodb::options::Acknowledgment::Majority,
                WriteConcernLevel::Nodes(n) => mongodb::options::Acknowledgment::Nodes(*n),
            });
            wc.journal = Some(self.config.write_concern.journal);
            wc.w_timeout = self
                .config
                .write_concern
                .timeout_ms
                .map(std::time::Duration::from_millis);
            wc
        };

        let coll_opts = CollectionOptions::builder().write_concern(wc).build();

        let collection = db
            .collection_with_options::<mongodb::bson::Document>(&self.config.collection, coll_opts);

        self.client = Some(client);
        self.collection = Some(collection);

        Ok(())
    }

    /// Ensures a time series collection exists with the correct configuration.
    async fn ensure_timeseries_collection(
        &self,
        db: &mongodb::Database,
        ts_config: &super::timeseries::TimeSeriesConfig,
    ) -> Result<(), ConnectorError> {
        use mongodb::bson::doc;

        let mut ts_opts = doc! {
            "timeField": &ts_config.time_field,
        };

        if let Some(ref meta) = ts_config.meta_field {
            ts_opts.insert("metaField", meta);
        }

        match ts_config.granularity {
            super::timeseries::TimeSeriesGranularity::Seconds => {
                ts_opts.insert("granularity", "seconds");
            }
            super::timeseries::TimeSeriesGranularity::Minutes => {
                ts_opts.insert("granularity", "minutes");
            }
            super::timeseries::TimeSeriesGranularity::Hours => {
                ts_opts.insert("granularity", "hours");
            }
            super::timeseries::TimeSeriesGranularity::Custom {
                bucket_max_span_seconds,
                bucket_rounding_seconds,
            } => {
                ts_opts.insert("bucketMaxSpanSeconds", i64::from(bucket_max_span_seconds));
                ts_opts.insert("bucketRoundingSeconds", i64::from(bucket_rounding_seconds));
            }
        }

        let mut create_opts = doc! {
            "create": &self.config.collection,
            "timeseries": ts_opts,
        };

        if let Some(ttl) = ts_config.expire_after_seconds {
            #[allow(clippy::cast_possible_wrap)]
            create_opts.insert("expireAfterSeconds", ttl as i64);
        }

        // Try creating; ignore "already exists" errors.
        match db.run_command(create_opts).await {
            Ok(_) => {
                info!(
                    collection = %self.config.collection,
                    time_field = %ts_config.time_field,
                    granularity = %ts_config.granularity,
                    "created time series collection"
                );
            }
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("already exists") && !msg.contains("NamespaceExists") {
                    return Err(ConnectorError::ConnectionFailed(format!(
                        "create time series collection: {e}"
                    )));
                }
                debug!(
                    collection = %self.config.collection,
                    "time series collection already exists"
                );
            }
        }

        Ok(())
    }

    /// Extracts a CDC envelope field that may be a JSON string (from Utf8 Arrow
    /// columns) or already a JSON object. Parses strings into objects for BSON
    /// conversion.
    fn parse_cdc_field<'a>(
        val: &'a serde_json::Value,
        field: &str,
    ) -> Result<std::borrow::Cow<'a, serde_json::Value>, ConnectorError> {
        let v = val.get(field).ok_or_else(|| {
            ConnectorError::WriteError(format!("CDC event missing {field} field"))
        })?;
        match v {
            serde_json::Value::Object(_) => Ok(std::borrow::Cow::Borrowed(v)),
            serde_json::Value::String(s) => {
                let parsed: serde_json::Value = serde_json::from_str(s)
                    .map_err(|e| ConnectorError::WriteError(format!("parse {field} JSON: {e}")))?;
                Ok(std::borrow::Cow::Owned(parsed))
            }
            _ => Err(ConnectorError::WriteError(format!(
                "{field} must be a JSON object or JSON string, got {v}"
            ))),
        }
    }

    /// Writes JSON value documents to `MongoDB` using the configured write mode.
    ///
    /// Accepts `serde_json::Value` directly (no intermediate string round-trip).
    /// Insert/upsert/replace from documents already in BSON (no JSON hop). CDC
    /// replay goes through [`write_cdc_docs`](Self::write_cdc_docs) instead.
    async fn write_bson_docs(
        &self,
        docs: Vec<mongodb::bson::Document>,
    ) -> Result<(), ConnectorError> {
        use mongodb::bson::{doc, Bson, Document};

        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;
        let count = docs.len() as u64;

        match &self.config.write_mode {
            WriteMode::Insert => {
                let opts = mongodb::options::InsertManyOptions::builder()
                    .ordered(Some(self.config.ordered))
                    .build();
                collection
                    .insert_many(docs)
                    .with_options(opts)
                    .await
                    .map_err(|e| {
                        self.metrics.record_error();
                        ConnectorError::WriteError(format!("insert_many: {e}"))
                    })?;
                self.metrics.record_inserts(count);
            }

            WriteMode::Upsert { ref key_fields } => {
                for bson_doc in docs {
                    let mut filter = Document::new();
                    for key in key_fields {
                        if let Some(v) = bson_doc.get(key) {
                            filter.insert(key, v.clone());
                        }
                    }
                    if filter.is_empty() {
                        return Err(ConnectorError::WriteError(format!(
                            "upsert filter is empty: none of the key_fields {key_fields:?} \
                             exist in the document"
                        )));
                    }
                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(Some(true))
                        .build();
                    collection
                        .replace_one(filter, bson_doc)
                        .with_options(opts)
                        .await
                        .map_err(|e| {
                            self.metrics.record_error();
                            ConnectorError::WriteError(format!("upsert: {e}"))
                        })?;
                }
                self.metrics.record_upserts(count);
            }

            WriteMode::Replace { upsert_on_missing } => {
                let upsert = *upsert_on_missing;
                for bson_doc in docs {
                    let filter = match bson_doc.get("_id") {
                        Some(id) if *id != Bson::Null => doc! { "_id": id.clone() },
                        _ => {
                            return Err(ConnectorError::WriteError(
                                "Replace mode requires a non-null _id field in document"
                                    .to_string(),
                            ));
                        }
                    };
                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(Some(upsert))
                        .build();
                    collection
                        .replace_one(filter, bson_doc)
                        .with_options(opts)
                        .await
                        .map_err(|e| {
                            self.metrics.record_error();
                            ConnectorError::WriteError(format!("replace: {e}"))
                        })?;
                }
                // replace_one with upsert is an upsert; count it like Upsert mode.
                self.metrics.record_upserts(count);
            }

            WriteMode::CdcReplay => {
                return Err(ConnectorError::Internal(
                    "CDC replay must go through write_cdc_docs".to_string(),
                ));
            }
        }

        self.metrics.record_bulk_write();
        Ok(())
    }

    /// CDC replay: each doc is a changelog envelope (`_op`/`_full_document`/
    /// `_document_key`/`_update_desc`) applied as the matching `MongoDB` op.
    #[allow(clippy::too_many_lines)]
    async fn write_cdc_docs(&self, docs: &[serde_json::Value]) -> Result<(), ConnectorError> {
        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;

        for val in docs {
            let op = val.get("_op").and_then(|v| v.as_str()).unwrap_or("I");

            match op {
                "I" => {
                    let full_doc = Self::parse_cdc_field(val, "_full_document")?;
                    let bson_doc = json_to_bson_document(full_doc.as_ref())?;
                    collection
                        .insert_one(bson_doc)
                        .await
                        .map_err(|e| ConnectorError::WriteError(format!("cdc insert: {e}")))?;
                    self.metrics.record_inserts(1);
                }
                "U" => {
                    let dk = Self::parse_cdc_field(val, "_document_key")?;
                    let ud = Self::parse_cdc_field(val, "_update_desc")?;
                    let filter = json_to_bson_document(dk.as_ref())?;

                    // Transform updateDescription into update operators.
                    // Raw format: { "updatedFields": {...}, "removedFields": [...] }
                    // Required:   { "$set": {...}, "$unset": {...} }
                    let mut update = mongodb::bson::Document::new();
                    if let Some(updated) = ud.get("updatedFields") {
                        update.insert("$set", json_to_bson(updated)?);
                    }
                    if let Some(removed) = ud.get("removedFields").and_then(|v| v.as_array()) {
                        if !removed.is_empty() {
                            let unset_doc: mongodb::bson::Document = removed
                                .iter()
                                .filter_map(|f| f.as_str())
                                .map(|f| {
                                    (f.to_string(), mongodb::bson::Bson::String(String::new()))
                                })
                                .collect();
                            update.insert("$unset", unset_doc);
                        }
                    }
                    if update.is_empty() {
                        continue;
                    }

                    collection
                        .update_one(filter, update)
                        .await
                        .map_err(|e| ConnectorError::WriteError(format!("cdc update: {e}")))?;
                    self.metrics.record_upserts(1);
                }
                "R" => {
                    let dk = Self::parse_cdc_field(val, "_document_key")?;
                    let full_doc = Self::parse_cdc_field(val, "_full_document")?;
                    let filter = json_to_bson_document(dk.as_ref())?;
                    let replacement = json_to_bson_document(full_doc.as_ref())?;
                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(Some(true))
                        .build();
                    collection
                        .replace_one(filter, replacement)
                        .with_options(opts)
                        .await
                        .map_err(|e| ConnectorError::WriteError(format!("cdc replace: {e}")))?;
                    self.metrics.record_upserts(1);
                }
                "D" => {
                    let dk = Self::parse_cdc_field(val, "_document_key")?;
                    let filter = json_to_bson_document(dk.as_ref())?;
                    collection
                        .delete_one(filter)
                        .await
                        .map_err(|e| ConnectorError::WriteError(format!("cdc delete: {e}")))?;
                    self.metrics.record_deletes(1);
                }
                _ => {
                    debug!(op = op, "lifecycle event — no write issued");
                }
            }
        }

        self.metrics.record_bulk_write();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_batch(n: usize) -> RecordBatch {
        #[allow(clippy::cast_possible_wrap)]
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<String> = (0..n).map(|i| format!("user_{i}")).collect();

        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_new_sink() {
        let config = MongoDbSinkConfig::new("mongodb://localhost:27017", "db", "coll");
        let sink = MongoDbSink::new(test_schema(), config, None);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[test]
    fn test_sink_capabilities_insert() {
        let config = MongoDbSinkConfig::default();
        let sink = MongoDbSink::new(test_schema(), config, None);
        let caps = sink.capabilities();
        assert!(caps.idempotent);
        assert!(!caps.upsert);
        assert!(!caps.changelog);
    }

    #[test]
    fn test_sink_capabilities_upsert() {
        let mut config = MongoDbSinkConfig::default();
        config.write_mode = WriteMode::Upsert {
            key_fields: vec!["id".to_string()],
        };
        let sink = MongoDbSink::new(test_schema(), config, None);
        let caps = sink.capabilities();
        assert!(caps.upsert);
    }

    #[test]
    fn test_sink_capabilities_cdc_replay() {
        let mut config = MongoDbSinkConfig::default();
        config.write_mode = WriteMode::CdcReplay;
        let sink = MongoDbSink::new(test_schema(), config, None);
        let caps = sink.capabilities();
        assert!(caps.changelog);
    }

    #[test]
    fn test_batches_to_json() {
        let config = MongoDbSinkConfig::default();
        let mut sink = MongoDbSink::new(test_schema(), config, None);
        sink.buffer.push(test_batch(3));
        sink.buffered_rows = 3;

        let (docs, byte_estimate) = sink.batches_to_json_docs();
        assert_eq!(docs.len(), 3);
        assert!(byte_estimate > 0);

        assert_eq!(docs[0]["id"], 0);
        assert_eq!(docs[0]["name"], "user_0");
    }

    #[cfg(feature = "mongodb-cdc")]
    #[test]
    fn timestamp_column_becomes_bson_date() {
        let arr = arrow_array::TimestampMillisecondArray::from(vec![1_700_000_000_000_i64]);
        let json = arrow_value_to_json(&arr, 0);
        match json_to_bson(&json).unwrap() {
            mongodb::bson::Bson::DateTime(dt) => {
                assert_eq!(dt.timestamp_millis(), 1_700_000_000_000);
            }
            other => panic!("expected BSON DateTime, got {other:?}"),
        }
    }

    #[cfg(feature = "mongodb-cdc")]
    #[test]
    fn arrow_to_bson_maps_scalar_types() {
        use mongodb::bson::Bson;
        let ts = arrow_array::TimestampMillisecondArray::from(vec![Some(1_700_000_000_000), None]);
        assert!(
            matches!(arrow_value_to_bson(&ts, 0), Bson::DateTime(dt) if dt.timestamp_millis() == 1_700_000_000_000)
        );
        assert_eq!(arrow_value_to_bson(&ts, 1), Bson::Null);
        assert_eq!(
            arrow_value_to_bson(&Int64Array::from(vec![42]), 0),
            Bson::Int64(42)
        );
        assert_eq!(
            arrow_value_to_bson(&StringArray::from(vec!["x"]), 0),
            Bson::String("x".to_string())
        );
    }

    #[test]
    fn test_should_flush_batch_size() {
        let mut config = MongoDbSinkConfig::default();
        config.batch_size = 100;
        let mut sink = MongoDbSink::new(test_schema(), config, None);

        assert!(!sink.should_flush());
        sink.buffered_rows = 100;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_clear_buffer() {
        let config = MongoDbSinkConfig::default();
        let mut sink = MongoDbSink::new(test_schema(), config, None);

        sink.buffer.push(test_batch(5));
        sink.buffered_rows = 5;

        sink.clear_buffer();
        assert_eq!(sink.buffered_rows, 0);
        assert!(sink.buffer.is_empty());
    }

    #[test]
    fn test_arrow_value_to_json_types() {
        use arrow_array::*;

        // Int64
        let arr = Int64Array::from(vec![42]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::json!(42));

        // String
        let arr = StringArray::from(vec!["hello"]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::json!("hello"));

        // Boolean
        let arr = BooleanArray::from(vec![true]);
        let val = arrow_value_to_json(&arr, 0);
        assert_eq!(val, serde_json::json!(true));

        // Null
        let arr = Int64Array::from(vec![None::<i64>]);
        let val = arrow_value_to_json(&arr, 0);
        assert!(val.is_null());
    }
}
