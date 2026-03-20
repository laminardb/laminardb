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
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use tracing::{debug, info};

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SinkConnector, SinkConnectorCapabilities, WriteResult};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::config::MongoDbSinkConfig;
use super::metrics::MongoSinkMetrics;
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
    metrics: MongoSinkMetrics,

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
    pub fn new(schema: SchemaRef, config: MongoDbSinkConfig) -> Self {
        let buf_capacity = (config.batch_size / 128).max(4);
        Self {
            config,
            schema,
            state: ConnectorState::Created,
            buffer: Vec::with_capacity(buf_capacity),
            buffered_rows: 0,
            last_flush: Instant::now(),
            metrics: MongoSinkMetrics::new(),
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
        Ok(Self::new(schema, mongo_config))
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

        let (docs, byte_estimate) = self.batches_to_json_docs();
        let doc_count = docs.len();

        #[cfg(feature = "mongodb-cdc")]
        {
            self.write_docs(&docs).await?;
        }

        #[cfg(not(feature = "mongodb-cdc"))]
        {
            debug!(
                count = doc_count,
                "flush (no-op without mongodb-cdc feature)"
            );
        }

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

#[async_trait]
impl SinkConnector for MongoDbSink {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
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

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => HealthStatus::Healthy,
            ConnectorState::Created => HealthStatus::Unknown,
            ConnectorState::Closed | ConnectorState::Failed => {
                HealthStatus::Unhealthy("closed".to_string())
            }
            _ => HealthStatus::Degraded(format!("state: {}", self.state)),
        }
    }

    fn metrics(&self) -> ConnectorMetrics {
        self.metrics.to_connector_metrics()
    }

    fn capabilities(&self) -> SinkConnectorCapabilities {
        let mut caps = SinkConnectorCapabilities::default().with_idempotent();

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
    #[allow(clippy::too_many_lines)]
    async fn write_docs(&self, docs: &[serde_json::Value]) -> Result<(), ConnectorError> {
        use mongodb::bson::{doc, Document};

        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;

        match &self.config.write_mode {
            WriteMode::Insert => {
                let bson_docs: Vec<Document> = docs
                    .iter()
                    .map(|v| {
                        mongodb::bson::to_document(v)
                            .map_err(|e| ConnectorError::WriteError(format!("to BSON: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                let opts = mongodb::options::InsertManyOptions::builder()
                    .ordered(Some(self.config.ordered))
                    .build();

                collection
                    .insert_many(bson_docs)
                    .with_options(opts)
                    .await
                    .map_err(|e| {
                        self.metrics.record_error();
                        ConnectorError::WriteError(format!("insert_many: {e}"))
                    })?;

                self.metrics.record_inserts(docs.len() as u64);
            }

            WriteMode::Upsert { ref key_fields } => {
                for val in docs {
                    let bson_doc = mongodb::bson::to_document(val)
                        .map_err(|e| ConnectorError::WriteError(format!("to BSON: {e}")))?;

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

                self.metrics.record_upserts(docs.len() as u64);
            }

            WriteMode::Replace { upsert_on_missing } => {
                for val in docs {
                    let bson_doc = mongodb::bson::to_document(val)
                        .map_err(|e| ConnectorError::WriteError(format!("to BSON: {e}")))?;

                    // Use _id as the filter for replacement.
                    let filter = match bson_doc.get("_id") {
                        Some(id) if *id != mongodb::bson::Bson::Null => {
                            doc! { "_id": id.clone() }
                        }
                        _ => {
                            return Err(ConnectorError::WriteError(
                                "Replace mode requires a non-null _id field in document"
                                    .to_string(),
                            ));
                        }
                    };

                    let opts = mongodb::options::ReplaceOptions::builder()
                        .upsert(Some(*upsert_on_missing))
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
            }

            WriteMode::CdcReplay => {
                // CDC replay processes each document based on its _op field.
                for val in docs {
                    let op = val.get("_op").and_then(|v| v.as_str()).unwrap_or("I");

                    match op {
                        "I" => {
                            let full_doc = Self::parse_cdc_field(val, "_full_document")?;
                            let bson_doc = mongodb::bson::to_document(full_doc.as_ref())
                                .map_err(|e| ConnectorError::WriteError(format!("to BSON: {e}")))?;
                            collection.insert_one(bson_doc).await.map_err(|e| {
                                ConnectorError::WriteError(format!("cdc insert: {e}"))
                            })?;
                            self.metrics.record_inserts(1);
                        }
                        "U" => {
                            let dk = Self::parse_cdc_field(val, "_document_key")?;
                            let ud = Self::parse_cdc_field(val, "_update_desc")?;
                            let filter = mongodb::bson::to_document(dk.as_ref()).map_err(|e| {
                                ConnectorError::WriteError(format!("filter BSON: {e}"))
                            })?;
                            let update = mongodb::bson::to_document(ud.as_ref()).map_err(|e| {
                                ConnectorError::WriteError(format!("update BSON: {e}"))
                            })?;
                            collection.update_one(filter, update).await.map_err(|e| {
                                ConnectorError::WriteError(format!("cdc update: {e}"))
                            })?;
                        }
                        "R" => {
                            let dk = Self::parse_cdc_field(val, "_document_key")?;
                            let full_doc = Self::parse_cdc_field(val, "_full_document")?;
                            let filter = mongodb::bson::to_document(dk.as_ref()).map_err(|e| {
                                ConnectorError::WriteError(format!("filter BSON: {e}"))
                            })?;
                            let replacement = mongodb::bson::to_document(full_doc.as_ref())
                                .map_err(|e| {
                                    ConnectorError::WriteError(format!("replace BSON: {e}"))
                                })?;
                            let opts = mongodb::options::ReplaceOptions::builder()
                                .upsert(Some(true))
                                .build();
                            collection
                                .replace_one(filter, replacement)
                                .with_options(opts)
                                .await
                                .map_err(|e| {
                                    ConnectorError::WriteError(format!("cdc replace: {e}"))
                                })?;
                        }
                        "D" => {
                            let dk = Self::parse_cdc_field(val, "_document_key")?;
                            let filter = mongodb::bson::to_document(dk.as_ref()).map_err(|e| {
                                ConnectorError::WriteError(format!("filter BSON: {e}"))
                            })?;
                            collection.delete_one(filter).await.map_err(|e| {
                                ConnectorError::WriteError(format!("cdc delete: {e}"))
                            })?;
                            self.metrics.record_deletes(1);
                        }
                        _ => {
                            debug!(op = op, "lifecycle event — no write issued");
                        }
                    }
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
        let sink = MongoDbSink::new(test_schema(), config);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[test]
    fn test_sink_capabilities_insert() {
        let config = MongoDbSinkConfig::default();
        let sink = MongoDbSink::new(test_schema(), config);
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
        let sink = MongoDbSink::new(test_schema(), config);
        let caps = sink.capabilities();
        assert!(caps.upsert);
    }

    #[test]
    fn test_sink_capabilities_cdc_replay() {
        let mut config = MongoDbSinkConfig::default();
        config.write_mode = WriteMode::CdcReplay;
        let sink = MongoDbSink::new(test_schema(), config);
        let caps = sink.capabilities();
        assert!(caps.changelog);
    }

    #[test]
    fn test_batches_to_json() {
        let config = MongoDbSinkConfig::default();
        let mut sink = MongoDbSink::new(test_schema(), config);
        sink.buffer.push(test_batch(3));
        sink.buffered_rows = 3;

        let (docs, byte_estimate) = sink.batches_to_json_docs();
        assert_eq!(docs.len(), 3);
        assert!(byte_estimate > 0);

        assert_eq!(docs[0]["id"], 0);
        assert_eq!(docs[0]["name"], "user_0");
    }

    #[test]
    fn test_should_flush_batch_size() {
        let mut config = MongoDbSinkConfig::default();
        config.batch_size = 100;
        let mut sink = MongoDbSink::new(test_schema(), config);

        assert!(!sink.should_flush());
        sink.buffered_rows = 100;
        assert!(sink.should_flush());
    }

    #[test]
    fn test_clear_buffer() {
        let config = MongoDbSinkConfig::default();
        let mut sink = MongoDbSink::new(test_schema(), config);

        sink.buffer.push(test_batch(5));
        sink.buffered_rows = 5;

        sink.clear_buffer();
        assert_eq!(sink.buffered_rows, 0);
        assert!(sink.buffer.is_empty());
    }

    #[test]
    fn test_health_check() {
        let config = MongoDbSinkConfig::default();
        let mut sink = MongoDbSink::new(test_schema(), config);

        assert_eq!(sink.health_check(), HealthStatus::Unknown);

        sink.state = ConnectorState::Running;
        assert_eq!(sink.health_check(), HealthStatus::Healthy);

        sink.state = ConnectorState::Closed;
        assert!(matches!(sink.health_check(), HealthStatus::Unhealthy(_)));
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
