//! `MongoDB` sink connector implementation.
//!
//! Implements [`SinkConnector`] for writing Arrow `RecordBatch` data to
//! `MongoDB` collections. Supports insert, upsert, and CDC replay
//! write modes, with optional time series collection support.
//!
//! Writes are bounded by fixed retained-memory and bulk limits and flushed when
//! a bound is reached, `flush_interval` elapses, or shutdown begins.
//! Keyed and CDC flushes use one ordered `MongoDB` 8+ bulk operation, preserving
//! input order without a network round trip per row.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use tracing::{debug, info};

use crate::changelog::collapse_changelog;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use laminar_core::changelog::WEIGHT_COLUMN;

use super::config::MongoDbSinkConfig;
use super::metrics::MongoDbSinkMetrics;
use super::timeseries::CollectionKind;
use super::write_model::WriteMode;

// One budget covers retained Arrow, materialized BSON/write models, conversion scratch, and the
// driver's command serialization. The multiplier reserves space for the materialized document,
// the encoded command, and conversion/driver scratch instead of pretending those allocations are
// independent. These are implementation limits, not user tuning knobs.
const MAX_BUFFERED_RETAINED_BYTES: usize = 16 * 1024 * 1024;
const MAX_SINK_WORKING_SET_BYTES: usize = 64 * 1024 * 1024;
const MAX_STANDARD_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_TIMESERIES_DOCUMENT_BYTES: usize = 4 * 1024 * 1024;
const MAX_DOCUMENTS_PER_FLUSH: usize = 1_000;
const MATERIALIZED_BYTE_CHARGE: usize = 3;
const WRITE_MODEL_OVERHEAD_BYTES: usize = 256;
const CDC_ROW_OVERHEAD_BYTES: usize = 512;
const MONGODB_8_WIRE_VERSION: i32 = 25;
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const MIN_WRITE_TIMEOUT: Duration = Duration::from_millis(100);
const MAX_DRIVER_TIMEOUT_HEADROOM: Duration = Duration::from_secs(1);
const NAMESPACE_EXISTS_CODE: i32 = 48;

fn is_supported_mongodb_arrow_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Timestamp(..)
    )
}

pub(super) fn harden_mongodb_tls(
    options: &mut mongodb::options::ClientOptions,
) -> Result<(), ConnectorError> {
    use mongodb::options::{Tls, TlsOptions};

    match options.tls.as_ref() {
        Some(Tls::Enabled(tls)) if tls.allow_invalid_certificates == Some(true) => {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB connection.uri must not set tlsInsecure=true or \
                 tlsAllowInvalidCertificates=true"
                    .into(),
            ));
        }
        Some(Tls::Enabled(_) | Tls::Disabled) => {}
        None => options.tls = Some(Tls::Enabled(TlsOptions::default())),
    }
    Ok(())
}

fn is_namespace_exists(error: &mongodb::error::Error) -> bool {
    matches!(
        error.kind.as_ref(),
        mongodb::error::ErrorKind::Command(command) if is_namespace_exists_code(command.code)
    )
}

fn is_namespace_exists_code(code: i32) -> bool {
    code == NAMESPACE_EXISTS_CODE
}

fn validate_existing_timeseries_spec(
    spec: &mongodb::results::CollectionSpecification,
    expected: &super::timeseries::TimeSeriesConfig,
) -> Result<(), ConnectorError> {
    use super::timeseries::TimeSeriesGranularity;
    use mongodb::options::TimeseriesGranularity as DriverGranularity;
    use mongodb::results::CollectionType;

    if spec.collection_type != CollectionType::Timeseries {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB collection '{}' is not a time series collection",
            spec.name
        )));
    }

    let actual = spec.options.timeseries.as_ref().ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' has no time series options",
            spec.name
        ))
    })?;

    if actual.time_field != expected.time_field {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' uses time field '{}', expected '{}'",
            spec.name, actual.time_field, expected.time_field
        )));
    }
    if actual.meta_field != expected.meta_field {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' uses meta field {:?}, expected {:?}",
            spec.name, actual.meta_field, expected.meta_field
        )));
    }

    let granularity_matches = match expected.granularity {
        TimeSeriesGranularity::Seconds => match actual.granularity.as_ref() {
            Some(granularity) => granularity == &DriverGranularity::Seconds,
            None => actual.bucket_max_span.is_none() && actual.bucket_rounding.is_none(),
        },
        TimeSeriesGranularity::Minutes => {
            actual.granularity.as_ref() == Some(&DriverGranularity::Minutes)
        }
        TimeSeriesGranularity::Hours => {
            actual.granularity.as_ref() == Some(&DriverGranularity::Hours)
        }
        TimeSeriesGranularity::Custom {
            bucket_max_span_seconds,
            bucket_rounding_seconds,
        } => {
            actual.granularity.is_none()
                && actual.bucket_max_span
                    == Some(Duration::from_secs(u64::from(bucket_max_span_seconds)))
                && actual.bucket_rounding
                    == Some(Duration::from_secs(u64::from(bucket_rounding_seconds)))
        }
    };
    if !granularity_matches {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' has incompatible granularity",
            spec.name
        )));
    }

    let expected_ttl = expected.expire_after_seconds.map(Duration::from_secs);
    if spec.options.expire_after_seconds != expected_ttl {
        return Err(ConnectorError::ConfigurationError(format!(
            "existing MongoDB time series collection '{}' uses TTL {:?}, expected {:?}",
            spec.name, spec.options.expire_after_seconds, expected_ttl
        )));
    }

    Ok(())
}

/// `MongoDB` sink connector.
///
/// Writes Arrow `RecordBatch` records to a `MongoDB` collection using
/// configurable write modes. Supports standard and time series collections.
pub struct MongoDbSink {
    config: MongoDbSinkConfig,
    schema: SchemaRef,
    state: ConnectorState,
    buffer: Vec<RecordBatch>,
    buffered_rows: usize,
    buffered_retained_bytes: usize,
    write_timeout: Duration,
    metrics: MongoDbSinkMetrics,

    /// Admission authority and terminal observer for bulk operations owned by this generation.
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,

    client: Option<mongodb::Client>,
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
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            config,
            schema,
            state: ConnectorState::Created,
            buffer: Vec::with_capacity(4),
            buffered_rows: 0,
            buffered_retained_bytes: 0,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            metrics: MongoDbSinkMetrics::new(registry),
            task_owner,
            task_tracker,
            client: None,
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
        Self::validate_schema(&schema, &mongo_config)?;
        let mut sink = Self::new(schema, mongo_config, None);
        sink.write_timeout = Self::configured_write_timeout(config)?;
        Ok(sink)
    }

    pub(crate) fn from_connector_config(
        config: &ConnectorConfig,
        registry: Option<&prometheus::Registry>,
    ) -> Result<Self, ConnectorError> {
        let (sink_config, schema, write_timeout) = Self::decode_connector_config(config)?;
        let mut sink = Self::new(schema, sink_config, registry);
        sink.write_timeout = write_timeout;
        Ok(sink)
    }

    fn decode_connector_config(
        config: &ConnectorConfig,
    ) -> Result<(MongoDbSinkConfig, SchemaRef, Duration), ConnectorError> {
        let sink_config = MongoDbSinkConfig::from_config(config)?;
        if config.get("_arrow_schema").is_none() {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB sink requires the engine-injected '_arrow_schema'".into(),
            ));
        }
        let schema = config.arrow_schema().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "invalid MongoDB sink '_arrow_schema' encoding".into(),
            )
        })?;
        Self::validate_schema(&schema, &sink_config)?;
        let write_timeout = Self::configured_write_timeout(config)?;
        Ok((sink_config, schema, write_timeout))
    }

    fn configured_write_timeout(config: &ConnectorConfig) -> Result<Duration, ConnectorError> {
        let timeout = config
            .get_parsed::<u64>("sink.write.timeout.ms")?
            .map_or(DEFAULT_WRITE_TIMEOUT, Duration::from_millis);
        if timeout < MIN_WRITE_TIMEOUT {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB sink.write.timeout.ms must be at least {}",
                MIN_WRITE_TIMEOUT.as_millis()
            )));
        }
        Ok(timeout)
    }

    fn driver_timeout(&self) -> Duration {
        let headroom = (self.write_timeout / 5).min(MAX_DRIVER_TIMEOUT_HEADROOM);
        self.write_timeout.checked_sub(headroom).unwrap()
    }

    #[allow(clippy::too_many_lines)]
    fn validate_schema(
        schema: &SchemaRef,
        config: &MongoDbSinkConfig,
    ) -> Result<(), ConnectorError> {
        if schema.fields().is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "MongoDB sink schema must contain at least one field".into(),
            ));
        }

        let mut names = std::collections::HashSet::with_capacity(schema.fields().len());
        for field in schema.fields() {
            if !names.insert(field.name()) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB sink schema contains duplicate field '{}'",
                    field.name()
                )));
            }
            if !is_supported_mongodb_arrow_type(field.data_type()) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB sink field '{}' has unsupported Arrow type {:?}",
                    field.name(),
                    field.data_type()
                )));
            }
        }

        match &config.write_mode {
            WriteMode::Upsert { key_fields } => {
                for key in key_fields {
                    match schema.field_with_name(key) {
                        Ok(field) if !field.is_nullable() => {}
                        Ok(_) => {
                            return Err(ConnectorError::ConfigurationError(format!(
                                "upsert key field '{key}' must be non-nullable"
                            )));
                        }
                        Err(_) => {
                            return Err(ConnectorError::ConfigurationError(format!(
                                "upsert key field '{key}' is not present in MongoDB sink schema"
                            )));
                        }
                    }
                }
            }
            WriteMode::CdcReplay => {
                for field in [
                    "_namespace",
                    "_op",
                    "_document_key",
                    "_full_document",
                    "_update_desc",
                ] {
                    match schema.field_with_name(field) {
                        Ok(schema_field) if schema_field.data_type() == &DataType::Utf8 => {}
                        Ok(schema_field) => {
                            return Err(ConnectorError::ConfigurationError(format!(
                                "MongoDB CDC replay field '{field}' must be Utf8, got {:?}",
                                schema_field.data_type()
                            )));
                        }
                        Err(_) => {
                            return Err(ConnectorError::ConfigurationError(format!(
                                "MongoDB CDC replay schema must contain '{field}'"
                            )));
                        }
                    }
                }
                for field in ["_namespace", "_op", "_document_key"] {
                    if schema
                        .field_with_name(field)
                        .is_ok_and(arrow_schema::Field::is_nullable)
                    {
                        return Err(ConnectorError::ConfigurationError(format!(
                            "MongoDB CDC replay field '{field}' must be non-nullable"
                        )));
                    }
                }
            }
            WriteMode::Insert => {}
        }

        if let CollectionKind::TimeSeries(time_series) = &config.collection_kind {
            let time_field = schema
                .field_with_name(&time_series.time_field)
                .map_err(|_| {
                    ConnectorError::ConfigurationError(format!(
                        "time series field '{}' is not present in MongoDB sink schema",
                        time_series.time_field
                    ))
                })?;
            if !matches!(time_field.data_type(), DataType::Timestamp(..)) {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB time series field '{}' must be an Arrow Timestamp",
                    time_series.time_field
                )));
            }
            if time_field.is_nullable() {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB time series field '{}' must be non-nullable",
                    time_series.time_field
                )));
            }
            if let Some(meta_field) = &time_series.meta_field {
                if schema.field_with_name(meta_field).is_err() {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "time series metadata field '{meta_field}' is not present in MongoDB sink schema"
                    )));
                }
            }
        }

        Ok(())
    }

    fn apply_connector_config(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let (sink_config, schema, write_timeout) = Self::decode_connector_config(config)?;
        self.config = sink_config;
        self.schema = schema;
        self.write_timeout = write_timeout;
        Ok(())
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

    fn retain_batch(&mut self, batch: RecordBatch, retained_bytes: usize) {
        self.buffered_rows = self.buffered_rows.saturating_add(batch.num_rows());
        self.buffered_retained_bytes = self.buffered_retained_bytes.saturating_add(retained_bytes);
        self.buffer.push(batch);
    }

    fn take_buffer(&mut self) -> (Vec<RecordBatch>, usize) {
        let retained_bytes = self.buffered_retained_bytes;
        self.buffered_rows = 0;
        self.buffered_retained_bytes = 0;
        (std::mem::take(&mut self.buffer), retained_bytes)
    }

    /// Converts CDC rows one at a time and drops each JSON staging value before advancing.
    fn batches_to_cdc_writes(
        batches: &[RecordBatch],
        retained_bytes: usize,
        working_set_limit: usize,
        expected_namespace: &str,
    ) -> Result<(Vec<CdcWrite>, u64), ConnectorError> {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        let mut writes = Vec::with_capacity(row_count);
        let mut encoded_bytes = 0;

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let (value, staging_bytes) = cdc_row_value(batch, row_idx)?;
                ensure_working_set(
                    retained_bytes,
                    encoded_bytes,
                    writes.len().saturating_add(1),
                    staging_bytes,
                    working_set_limit,
                    "MongoDB CDC flush",
                )?;
                let (mut row_writes, row_bytes) = Self::prepare_cdc_writes(
                    std::slice::from_ref(&value),
                    usize::MAX,
                    expected_namespace,
                )?;
                let row_bytes = usize::try_from(row_bytes).map_err(|_| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC encoded byte count exceeds this platform".into(),
                    )
                })?;
                encoded_bytes = checked_converted_total(
                    encoded_bytes,
                    row_bytes,
                    usize::MAX,
                    "MongoDB CDC flush",
                )?;
                let write = row_writes.pop().ok_or_else(|| {
                    ConnectorError::Internal("MongoDB CDC row produced no write disposition".into())
                })?;
                ensure_working_set(
                    retained_bytes,
                    encoded_bytes,
                    writes.len().saturating_add(1),
                    staging_bytes,
                    working_set_limit,
                    "MongoDB CDC flush",
                )?;
                writes.push(write);
            }
        }

        Ok((writes, encoded_bytes))
    }

    /// Arrow batches → BSON documents directly (no `serde_json::Value` hop), for the
    /// insert/upsert paths. Returns `(docs, byte_estimate)`.
    fn batches_to_bson_docs(
        batches: &[RecordBatch],
        retained_bytes: usize,
        working_set_limit: usize,
        document_limit: usize,
    ) -> Result<(Vec<mongodb::bson::Document>, u64), ConnectorError> {
        let row_count = batches.iter().map(RecordBatch::num_rows).sum();
        let mut docs = Vec::with_capacity(row_count);
        let mut bytes: u64 = 0;
        for batch in batches {
            let schema = batch.schema();
            for row in 0..batch.num_rows() {
                let mut doc = mongodb::bson::Document::new();
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let value = arrow_value_to_bson(batch.column(col_idx), row)?;
                    doc.insert(field.name().clone(), value);
                }
                let document_bytes =
                    encoded_document_size(&doc, document_limit, "MongoDB sink document")?;
                bytes = checked_converted_total(
                    bytes,
                    document_bytes,
                    usize::MAX,
                    "MongoDB sink flush",
                )?;
                ensure_working_set(
                    retained_bytes,
                    bytes,
                    docs.len().saturating_add(1),
                    0,
                    working_set_limit,
                    "MongoDB sink flush",
                )?;
                docs.push(doc);
            }
        }
        Ok((docs, bytes))
    }

    /// Internal flush that returns a [`WriteResult`] with actual counts.
    ///
    /// Both `write_batch` (on auto-flush) and `flush` delegate here.
    async fn flush_inner(&mut self) -> Result<WriteResult, ConnectorError> {
        if self.buffer.is_empty() {
            return Ok(WriteResult::new(0, 0));
        }

        // Drain before any await. A timeout makes the bulk-write outcome unknown, retires this
        // connector generation, and replays from the last durable engine checkpoint.
        let (mut pending, retained_bytes) = self.take_buffer();
        let write_result: Result<(usize, u64), ConnectorError> =
            if matches!(self.config.write_mode, WriteMode::CdcReplay) {
                match Self::batches_to_cdc_writes(
                    &pending,
                    retained_bytes,
                    MAX_SINK_WORKING_SET_BYTES,
                    &format!("{}.{}", self.config.database, self.config.collection),
                ) {
                    Ok((writes, bytes)) => {
                        let count = writes.len();
                        pending.clear();
                        self.execute_cdc_writes(writes)
                            .await
                            .map(|()| (count, bytes))
                    }
                    Err(error) => Err(error),
                }
            } else {
                match Self::collapse_changelog_buffer_for_upsert(&self.config, &mut pending)
                    .and_then(|collapsed| {
                        let retained_bytes = pending.iter().fold(0_usize, |total, batch| {
                            total.saturating_add(retained_batch_bytes(batch))
                        });
                        Self::batches_to_bson_docs(
                            &pending,
                            retained_bytes,
                            MAX_SINK_WORKING_SET_BYTES,
                            if matches!(&self.config.collection_kind, CollectionKind::TimeSeries(_))
                            {
                                MAX_TIMESERIES_DOCUMENT_BYTES
                            } else {
                                MAX_STANDARD_DOCUMENT_BYTES
                            },
                        )
                        .map(|(docs, bytes)| (docs, collapsed, bytes))
                    }) {
                    Ok((docs, collapsed, bytes)) => {
                        let count = docs.len();
                        pending.clear();
                        self.write_bson_docs(docs, collapsed, bytes)
                            .await
                            .map(|()| (count, bytes))
                    }
                    Err(error) => Err(error),
                }
            };

        pending.clear();
        self.buffer = pending;

        let (doc_count, byte_estimate) = write_result?;
        self.metrics.record_flush(doc_count as u64, byte_estimate);
        Ok(WriteResult::new(doc_count, byte_estimate))
    }

    /// Collapse a Z-set changelog buffer (an incremental MV: rows carry `__weight`) into a single
    /// key-unique `{U,D}` batch before upsert, so retract+insert nets per key and a removed group
    /// becomes a delete. No-op unless the write mode is upsert and the buffer is a Z-set changelog.
    /// `Ok(true)` if the buffer was a Z-set changelog and got collapsed; `Ok(false)` for a plain
    /// (non-changelog) upsert, so the caller doesn't interpret `_op` on rows we didn't produce.
    fn collapse_changelog_buffer_for_upsert(
        config: &MongoDbSinkConfig,
        buffer: &mut Vec<RecordBatch>,
    ) -> Result<bool, ConnectorError> {
        let key_fields = match &config.write_mode {
            WriteMode::Upsert { key_fields } => key_fields.clone(),
            _ => return Ok(false),
        };
        // Gate on buffer[0] — it supplies the concat schema (concat requires all batches match it);
        // `any` could pass on a later batch's weight while buffer[0] lacks it, failing the concat.
        let Some(first) = buffer.first() else {
            return Ok(false);
        };
        if first.schema().index_of(WEIGHT_COLUMN).is_err() {
            return Ok(false);
        }
        let schema = first.schema();
        let combined = arrow_select::concat::concat_batches(&schema, buffer.iter())
            .map_err(|e| ConnectorError::Internal(format!("concat changelog: {e}")))?;
        *buffer = vec![collapse_changelog(&combined, &key_fields)?];
        Ok(true)
    }

    async fn write_batch_with_retained_limit(
        &mut self,
        batch: &RecordBatch,
        retained_limit: usize,
    ) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Running.to_string(),
                actual: self.state.to_string(),
            });
        }
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ConnectorError::SchemaMismatch(format!(
                "MongoDB sink expected {:?}, got {:?}",
                self.schema,
                batch.schema()
            )));
        }

        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(WriteResult::new(0, 0));
        }

        let retained_bytes = retained_batch_bytes(batch);
        // Reject a single oversized input before flushing or otherwise mutating existing state.
        let _ = requires_preflush(0, retained_bytes, retained_limit)?;

        let mut offset = 0;
        let mut result = WriteResult::new(0, 0);
        while offset < rows {
            let available_rows = MAX_DOCUMENTS_PER_FLUSH - self.buffered_rows;
            let chunk_rows = available_rows.min(rows - offset);
            let chunk = batch.slice(offset, chunk_rows);
            let chunk_retained_bytes = retained_batch_bytes(&chunk);

            if requires_preflush(
                self.buffered_retained_bytes,
                chunk_retained_bytes,
                retained_limit,
            )? {
                let flushed = self
                    .flush_inner()
                    .await
                    .map_err(|error| mongo_partial_batch_error(&result, error))?;
                accumulate_write_result(&mut result, &flushed);
            }

            self.retain_batch(chunk, chunk_retained_bytes);
            offset += chunk_rows;
            if self.buffered_rows == MAX_DOCUMENTS_PER_FLUSH {
                let flushed = self
                    .flush_inner()
                    .await
                    .map_err(|error| mongo_partial_batch_error(&result, error))?;
                accumulate_write_result(&mut result, &flushed);
            }
        }

        Ok(result)
    }
}

fn cdc_row_value(
    batch: &RecordBatch,
    row: usize,
) -> Result<(serde_json::Value, usize), ConnectorError> {
    use arrow_array::{Array, StringArray};

    let mut value = serde_json::Map::with_capacity(5);
    let mut staging_bytes = CDC_ROW_OVERHEAD_BYTES;
    for field_name in [
        "_namespace",
        "_op",
        "_document_key",
        "_full_document",
        "_update_desc",
    ] {
        let column_index = batch.schema().index_of(field_name).map_err(|_| {
            ConnectorError::SchemaMismatch(format!(
                "MongoDB CDC replay batch is missing '{field_name}'"
            ))
        })?;
        let column = batch
            .column(column_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                ConnectorError::SchemaMismatch(format!(
                    "MongoDB CDC replay field '{field_name}' must be Utf8"
                ))
            })?;
        let (field_value, payload_bytes) = if column.is_null(row) {
            (serde_json::Value::Null, 0)
        } else {
            let text = column.value(row);
            (serde_json::Value::String(text.to_owned()), text.len())
        };
        staging_bytes = staging_bytes
            .checked_add(
                std::mem::size_of::<serde_json::Value>()
                    .saturating_add(payload_bytes)
                    .saturating_mul(3),
            )
            .ok_or_else(|| {
                ConnectorError::ConfigurationError("MongoDB CDC staging byte count overflow".into())
            })?;
        value.insert(field_name.to_string(), field_value);
    }
    Ok((serde_json::Value::Object(value), staging_bytes))
}

/// Milliseconds since epoch for a timestamp column cell, normalizing the unit.
/// Sub-millisecond precision is floored toward the preceding millisecond.
fn timestamp_millis(col: &dyn arrow_array::Array, row: usize) -> Result<i64, ConnectorError> {
    use arrow_array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    let DataType::Timestamp(unit, _) = col.data_type() else {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB timestamp conversion received a non-timestamp Arrow column".into(),
        ));
    };
    let a = col.as_any();
    let millis = match unit {
        arrow_schema::TimeUnit::Second => a
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .value(row)
            .checked_mul(1000)
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "MongoDB timestamp seconds overflow millisecond range".into(),
                )
            })?,
        arrow_schema::TimeUnit::Millisecond => a
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .value(row),
        arrow_schema::TimeUnit::Microsecond => a
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(row)
            .div_euclid(1000),
        arrow_schema::TimeUnit::Nanosecond => a
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row)
            .div_euclid(1_000_000),
    };
    Ok(millis)
}

/// Convert one Arrow cell straight to BSON for the insert/upsert paths,
/// skipping the `serde_json::Value` intermediate (and the number string round-trip)
/// that the CDC path still needs. Integers stay width-faithful and timestamps become
/// BSON dates; unsupported types are rejected during schema validation.
fn arrow_value_to_bson(
    col: &dyn arrow_array::Array,
    row: usize,
) -> Result<mongodb::bson::Bson, ConnectorError> {
    use arrow_array::{
        BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeStringArray, StringArray, UInt16Array, UInt32Array, UInt8Array,
    };
    use mongodb::bson::Bson;

    if !is_supported_mongodb_arrow_type(col.data_type()) {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB sink cannot convert unsupported Arrow type {:?}",
            col.data_type()
        )));
    }
    if col.is_null(row) {
        return Ok(Bson::Null);
    }
    let a = col.as_any();
    let i32_of = |v: i32| Bson::Int32(v);
    let value = match col.data_type() {
        DataType::Null => Bson::Null,
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
            timestamp_millis(col, row)?,
        )),
        _ => unreachable!("unsupported Arrow type rejected before conversion"),
    };
    Ok(value)
}

fn retained_batch_bytes(batch: &RecordBatch) -> usize {
    batch.columns().iter().fold(0, |total, column| {
        total.saturating_add(column.get_array_memory_size())
    })
}

fn clamp_client_timeout(configured: Option<Duration>, limit: Duration) -> Duration {
    configured
        .filter(|timeout| !timeout.is_zero())
        .map_or(limit, |timeout| timeout.min(limit))
}

fn requires_preflush(
    buffered_bytes: usize,
    incoming_bytes: usize,
    retained_limit: usize,
) -> Result<bool, ConnectorError> {
    if incoming_bytes > retained_limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB sink input batch retains {incoming_bytes} bytes, exceeding the fixed \
             {retained_limit}-byte per-sink buffer limit; split the batch upstream"
        )));
    }

    Ok(buffered_bytes
        .checked_add(incoming_bytes)
        .is_none_or(|total| total > retained_limit))
}

fn accumulate_write_result(total: &mut WriteResult, flushed: &WriteResult) {
    total.records_written = total
        .records_written
        .saturating_add(flushed.records_written);
    total.bytes_written = total.bytes_written.saturating_add(flushed.bytes_written);
}

fn mongo_partial_batch_error(completed: &WriteResult, error: ConnectorError) -> ConnectorError {
    if completed.records_written == 0 {
        return error;
    }
    let retryable = error.is_transient();
    ConnectorError::outcome_unknown(
        format!(
            "MongoDB batch failed after {} records and {} bytes were already written: {error}",
            completed.records_written, completed.bytes_written
        ),
        retryable,
    )
}

fn checked_converted_total(
    current: u64,
    incoming: usize,
    limit: usize,
    context: &str,
) -> Result<u64, ConnectorError> {
    let incoming = u64::try_from(incoming).unwrap_or(u64::MAX);
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    let total = current.checked_add(incoming).ok_or_else(|| {
        ConnectorError::ConfigurationError(format!("{context} encoded byte count overflow"))
    })?;
    if total > limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "{context} encodes to {total} bytes, exceeding the fixed {limit}-byte conversion \
             limit; split the batch upstream"
        )));
    }
    Ok(total)
}

fn working_set_charge(
    retained_bytes: usize,
    encoded_bytes: u64,
    model_count: usize,
    staging_bytes: usize,
) -> Option<usize> {
    let encoded_bytes = usize::try_from(encoded_bytes).ok()?;
    retained_bytes
        .checked_add(encoded_bytes.checked_mul(MATERIALIZED_BYTE_CHARGE)?)?
        .checked_add(model_count.checked_mul(WRITE_MODEL_OVERHEAD_BYTES)?)?
        .checked_add(staging_bytes)
}

fn ensure_working_set(
    retained_bytes: usize,
    encoded_bytes: u64,
    model_count: usize,
    staging_bytes: usize,
    limit: usize,
    context: &str,
) -> Result<(), ConnectorError> {
    let charge = working_set_charge(retained_bytes, encoded_bytes, model_count, staging_bytes)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("{context} working-set byte count overflow"))
        })?;
    if charge > limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "{context} requires a conservative {charge}-byte working set, exceeding the fixed \
             {limit}-byte per-sink limit; split the batch upstream"
        )));
    }
    Ok(())
}

fn encoded_document_size(
    document: &mongodb::bson::Document,
    limit: usize,
    context: &str,
) -> Result<usize, ConnectorError> {
    let encoded = mongodb::bson::to_vec(document).map_err(|error| {
        ConnectorError::ConfigurationError(format!(
            "{context} cannot be represented as BSON: {error}"
        ))
    })?;
    if encoded.len() > limit {
        return Err(ConnectorError::ConfigurationError(format!(
            "{context} encodes to {} bytes, exceeding MongoDB's {limit}-byte BSON document \
             limit",
            encoded.len()
        )));
    }
    Ok(encoded.len())
}

fn account_bson_document(
    total: &mut u64,
    document: &mongodb::bson::Document,
    converted_limit: usize,
    context: &str,
) -> Result<(), ConnectorError> {
    let bytes = encoded_document_size(document, MAX_STANDARD_DOCUMENT_BYTES, context)?;
    *total = checked_converted_total(*total, bytes, converted_limit, "MongoDB CDC flush")?;
    Ok(())
}

/// Convert source-envelope JSON to BSON while interpreting `MongoDB` Extended JSON.
/// Serde's structural conversion would store `$date` as a literal sub-document.
fn json_to_bson(value: &serde_json::Value) -> Result<mongodb::bson::Bson, ConnectorError> {
    mongodb::bson::Bson::try_from(value.clone())
        .map_err(|e| ConnectorError::ConfigurationError(format!("JSON to BSON: {e}")))
}

fn json_to_bson_document(
    value: &serde_json::Value,
) -> Result<mongodb::bson::Document, ConnectorError> {
    match json_to_bson(value)? {
        mongodb::bson::Bson::Document(doc) => Ok(doc),
        other => Err(ConnectorError::ConfigurationError(format!(
            "expected a BSON document, got {:?}",
            other.element_type()
        ))),
    }
}

fn validate_cdc_document_key(
    document: &mongodb::bson::Document,
) -> Result<mongodb::bson::Document, ConnectorError> {
    if document.is_empty() || !document.contains_key("_id") {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC replay document key must contain '_id' and every target shard-key field"
                .into(),
        ));
    }
    let id = document.get("_id").expect("_id presence checked above");
    if id == &mongodb::bson::Bson::Null {
        return Err(ConnectorError::ConfigurationError(
            "MongoDB CDC replay '_id' must be non-null".into(),
        ));
    }
    let mut filter = mongodb::bson::Document::new();
    for (field, value) in document {
        if field.starts_with('$') {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC document-key field '{field}' must not be an operator"
            )));
        }
        // Explicit equality prevents a document-valued key from being interpreted as a query
        // operator while retaining every shard-key component for targeted writes.
        filter.insert(field.clone(), mongodb::bson::doc! { "$eq": value.clone() });
    }
    Ok(filter)
}

fn document_key_value<'a>(
    document: &'a mongodb::bson::Document,
    field: &str,
) -> Option<&'a mongodb::bson::Bson> {
    if let Some(value) = document.get(field) {
        return Some(value);
    }
    let mut path = field.split('.');
    let mut value = document.get(path.next()?)?;
    for component in path {
        value = value.as_document()?.get(component)?;
    }
    Some(value)
}

fn validate_cdc_replacement_key(
    document_key: &mongodb::bson::Document,
    replacement: &mongodb::bson::Document,
) -> Result<(), ConnectorError> {
    for (field, expected) in document_key {
        let actual = document_key_value(replacement, field).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "MongoDB CDC replacement is missing document-key field '{field}'"
            ))
        })?;
        if actual != expected {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC replacement document-key field '{field}' does not match the event"
            )));
        }
    }
    Ok(())
}

#[async_trait]
impl SinkConnector for MongoDbSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let (cfg, schema, _) = if config.properties().is_empty() {
            (
                self.config.clone(),
                Arc::clone(&self.schema),
                self.write_timeout,
            )
        } else {
            Self::decode_connector_config(config)?
        };
        cfg.validate()?;
        Self::validate_schema(&schema, &cfg)?;
        let (topology, input_mode) = match cfg.write_mode {
            WriteMode::Insert => (SinkTopology::MultiWriter, SinkInputMode::AppendOnly),
            WriteMode::Upsert { .. } | WriteMode::CdcReplay => {
                (SinkTopology::Singleton, SinkInputMode::FullChangelog)
            }
        };
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            topology,
            input_mode,
        ))
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Created.to_string(),
                actual: self.state.to_string(),
            });
        }
        if config.properties().is_empty() {
            self.config.validate()?;
            Self::validate_schema(&self.schema, &self.config)?;
        } else {
            self.apply_connector_config(config)?;
        }
        self.connect().await?;

        self.state = ConnectorState::Running;
        info!(
            database = %self.config.database,
            collection = %self.config.collection,
            write_mode = ?self.config.write_mode,
            "MongoDB sink opened"
        );

        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.write_batch_with_retained_limit(batch, MAX_BUFFERED_RETAINED_BYTES)
            .await
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.write_timeout
    }

    fn flush_interval(&self) -> Duration {
        self.config.flush_interval()
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: ConnectorState::Running.to_string(),
                actual: self.state.to_string(),
            });
        }
        self.flush_inner().await.map(|_| ())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        if self.state == ConnectorState::Closed {
            return Ok(());
        }

        let flush_result = if self.state == ConnectorState::Running && !self.buffer.is_empty() {
            self.flush_inner().await.map(|_| ())
        } else {
            drop(self.take_buffer());
            Ok(())
        };
        self.collection = None;
        self.client = None;
        self.state = ConnectorState::Closed;
        info!("MongoDB sink closed");
        flush_result
    }
}

#[derive(Debug)]
enum CdcWrite {
    Insert {
        filter: mongodb::bson::Document,
        replacement: mongodb::bson::Document,
    },
    Update {
        filter: mongodb::bson::Document,
        update: mongodb::bson::Document,
    },
    Replace {
        filter: mongodb::bson::Document,
        replacement: mongodb::bson::Document,
    },
    Delete {
        filter: mongodb::bson::Document,
    },
    Noop,
}

#[derive(Default)]
struct BulkCounts {
    inserts: u64,
    upserts: u64,
    deletes: u64,
}

enum MongoOperationOutcome<T> {
    Completed(T),
    Deadline,
    TaskFailed(tokio::task::JoinError),
}

fn spawn_mongo_sink_operation<F, T>(
    task_owner: &ConnectorTaskOwner,
    operation: F,
) -> Result<tokio::task::JoinHandle<T>, ConnectorError>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let guard = task_owner.track().ok_or_else(|| {
        ConnectorError::Internal(
            "MongoDB sink task generation was sealed before bulk operation admission".into(),
        )
    })?;
    // The driver has no supported socket/operation timeout. Dropping this handle therefore
    // detaches the operation; its guard remains live until the driver future actually exits.
    Ok(tokio::spawn(async move {
        let _guard = guard;
        operation.await
    }))
}

async fn await_mongo_sink_operation<F, T>(
    task_owner: &ConnectorTaskOwner,
    deadline: tokio::time::Instant,
    operation: F,
) -> Result<MongoOperationOutcome<T>, ConnectorError>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let mut task = spawn_mongo_sink_operation(task_owner, operation)?;
    match tokio::time::timeout_at(deadline, &mut task).await {
        Ok(Ok(result)) => Ok(MongoOperationOutcome::Completed(result)),
        Ok(Err(error)) => Ok(MongoOperationOutcome::TaskFailed(error)),
        Err(_) => Ok(MongoOperationOutcome::Deadline),
    }
}

fn cdc_bulk_models(
    namespace: &mongodb::Namespace,
    writes: Vec<CdcWrite>,
) -> (Vec<mongodb::options::WriteModel>, BulkCounts) {
    use mongodb::options::{DeleteOneModel, ReplaceOneModel, UpdateOneModel, WriteModel};

    let mut models = Vec::with_capacity(writes.len());
    let mut counts = BulkCounts::default();
    for write in writes {
        match write {
            CdcWrite::Insert {
                filter,
                replacement,
            } => {
                models.push(WriteModel::ReplaceOne(
                    ReplaceOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .replacement(replacement)
                        .upsert(true)
                        .build(),
                ));
                counts.inserts = counts.inserts.saturating_add(1);
            }
            CdcWrite::Update { filter, update } => {
                models.push(WriteModel::UpdateOne(
                    UpdateOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .update(update)
                        .build(),
                ));
                counts.upserts = counts.upserts.saturating_add(1);
            }
            CdcWrite::Replace {
                filter,
                replacement,
            } => {
                models.push(WriteModel::ReplaceOne(
                    ReplaceOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .replacement(replacement)
                        .upsert(true)
                        .build(),
                ));
                counts.upserts = counts.upserts.saturating_add(1);
            }
            CdcWrite::Delete { filter } => {
                models.push(WriteModel::DeleteOne(
                    DeleteOneModel::builder()
                        .namespace(namespace.clone())
                        .filter(filter)
                        .build(),
                ));
                counts.deletes = counts.deletes.saturating_add(1);
            }
            CdcWrite::Noop => {}
        }
    }
    (models, counts)
}

impl MongoDbSink {
    /// Connects to `MongoDB` and sets up the target collection with write concern.
    async fn connect(&mut self) -> Result<(), ConnectorError> {
        use mongodb::options::{ClientOptions, CollectionOptions};

        let mut client_options = ClientOptions::parse(&self.config.connection_uri)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("parse URI: {e}")))?;
        harden_mongodb_tls(&mut client_options)?;
        let driver_timeout = self.driver_timeout();
        client_options.connect_timeout = Some(clamp_client_timeout(
            client_options.connect_timeout,
            driver_timeout,
        ));
        client_options.server_selection_timeout = Some(clamp_client_timeout(
            client_options.server_selection_timeout,
            driver_timeout,
        ));

        let wc = {
            let mut wc = mongodb::options::WriteConcern::default();
            wc.w = Some(mongodb::options::Acknowledgment::Majority);
            wc.journal = Some(true);
            wc.w_timeout = Some(driver_timeout);
            wc
        };
        // Client::bulk_write is client-scoped, so its inherited concern must live on the client.
        client_options.write_concern = Some(wc.clone());
        let client = mongodb::Client::with_options(client_options)
            .map_err(|e| ConnectorError::ConnectionFailed(format!("create client: {e}")))?;

        let db = client.database(&self.config.database);
        let hello = db
            .run_command(mongodb::bson::doc! { "hello": 1 })
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "verify MongoDB bulk-write capability: {error}"
                ))
            })?;
        let max_wire_version = hello.get_i32("maxWireVersion").map_err(|_| {
            ConnectorError::ConnectionFailed(
                "MongoDB hello response omitted integer maxWireVersion".into(),
            )
        })?;
        if max_wire_version < MONGODB_8_WIRE_VERSION {
            return Err(ConnectorError::ConfigurationError(format!(
                "MongoDB sink requires MongoDB 8.0+ ordered bulk_write (maxWireVersion >= \
                 {MONGODB_8_WIRE_VERSION}); server reported {max_wire_version}"
            )));
        }

        match &self.config.collection_kind {
            CollectionKind::Standard => self.validate_standard_collection(&db).await?,
            CollectionKind::TimeSeries(ts_config) => {
                self.ensure_timeseries_collection(&db, ts_config).await?;
            }
        }

        let coll_opts = CollectionOptions::builder().write_concern(wc).build();

        let collection = db
            .collection_with_options::<mongodb::bson::Document>(&self.config.collection, coll_opts);

        self.client = Some(client);
        self.collection = Some(collection);

        Ok(())
    }

    /// Reject an existing view or time-series collection when standard mode was requested.
    async fn validate_standard_collection(
        &self,
        db: &mongodb::Database,
    ) -> Result<(), ConnectorError> {
        use futures_util::TryStreamExt;
        use mongodb::bson::doc;
        use mongodb::results::CollectionType;

        let mut collections = db
            .list_collections()
            .filter(doc! { "name": &self.config.collection })
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "inspect existing MongoDB collection '{}': {error}",
                    self.config.collection
                ))
            })?;
        if let Some(spec) = collections.try_next().await.map_err(|error| {
            ConnectorError::ConnectionFailed(format!(
                "read existing MongoDB collection '{}': {error}",
                self.config.collection
            ))
        })? {
            if spec.collection_type != CollectionType::Collection {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB standard sink target '{}' already exists as {:?}",
                    self.config.collection, spec.collection_type
                )));
            }
        }
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
            let ttl = i64::try_from(ttl).map_err(|_| {
                ConnectorError::ConfigurationError(
                    "time series expire_after_seconds exceeds MongoDB's signed 64-bit range".into(),
                )
            })?;
            create_opts.insert("expireAfterSeconds", ttl);
        }

        // A concurrent creator is safe only when it created the same collection shape.
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
                if !is_namespace_exists(&e) {
                    return Err(ConnectorError::ConnectionFailed(format!(
                        "create time series collection: {e}"
                    )));
                }
                self.validate_existing_timeseries_collection(db, ts_config)
                    .await?;
                debug!(
                    collection = %self.config.collection,
                    "matching time series collection already exists"
                );
            }
        }

        Ok(())
    }

    async fn validate_existing_timeseries_collection(
        &self,
        db: &mongodb::Database,
        expected: &super::timeseries::TimeSeriesConfig,
    ) -> Result<(), ConnectorError> {
        use futures_util::TryStreamExt;
        use mongodb::bson::doc;

        let mut collections = db
            .list_collections()
            .filter(doc! { "name": &self.config.collection })
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "inspect existing MongoDB collection '{}': {error}",
                    self.config.collection
                ))
            })?;
        let spec = collections
            .try_next()
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "read existing MongoDB collection '{}': {error}",
                    self.config.collection
                ))
            })?
            .ok_or_else(|| {
                ConnectorError::ConfigurationError(format!(
                    "MongoDB reported NamespaceExists for collection '{}', but its metadata was not returned",
                    self.config.collection
                ))
            })?;

        validate_existing_timeseries_spec(&spec, expected)
    }

    /// Extracts a CDC envelope field that may be a JSON string (from Utf8 Arrow
    /// columns) or already a JSON object. Parses strings into objects for BSON
    /// conversion.
    fn parse_cdc_field<'a>(
        val: &'a serde_json::Value,
        field: &str,
    ) -> Result<std::borrow::Cow<'a, serde_json::Value>, ConnectorError> {
        let v = val.get(field).ok_or_else(|| {
            ConnectorError::ConfigurationError(format!("CDC event missing {field} field"))
        })?;
        match v {
            serde_json::Value::Object(_) => Ok(std::borrow::Cow::Borrowed(v)),
            serde_json::Value::String(s) => {
                let parsed: serde_json::Value = serde_json::from_str(s).map_err(|e| {
                    ConnectorError::ConfigurationError(format!("parse {field} JSON: {e}"))
                })?;
                Ok(std::borrow::Cow::Owned(parsed))
            }
            _ => Err(ConnectorError::ConfigurationError(format!(
                "{field} must be a JSON object or JSON string, got {v}"
            ))),
        }
    }

    /// Writes JSON value documents to `MongoDB` using the configured write mode.
    ///
    /// Accepts `serde_json::Value` directly (no intermediate string round-trip).
    /// Insert/upsert from documents already in BSON (no JSON hop).
    #[allow(clippy::too_many_lines)]
    async fn write_bson_docs(
        &self,
        docs: Vec<mongodb::bson::Document>,
        from_changelog: bool,
        encoded_bytes: u64,
    ) -> Result<(), ConnectorError> {
        use mongodb::bson::Document;
        use mongodb::options::{DeleteOneModel, InsertOneModel, ReplaceOneModel, WriteModel};

        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;
        let namespace = collection.namespace();
        let mut models = Vec::with_capacity(docs.len());
        let mut counts = BulkCounts::default();
        let mut model_bytes = encoded_bytes;

        match &self.config.write_mode {
            WriteMode::Upsert { key_fields } => {
                for document in &docs {
                    for key in key_fields {
                        match document.get(key) {
                            Some(value) if *value != mongodb::bson::Bson::Null => {}
                            _ => {
                                return Err(ConnectorError::ConfigurationError(format!(
                                    "MongoDB upsert document requires a non-null key field '{key}'"
                                )));
                            }
                        }
                    }
                }
            }
            WriteMode::Insert | WriteMode::CdcReplay => {}
        }

        match &self.config.write_mode {
            WriteMode::Insert => {
                counts.inserts = docs.len() as u64;
                models.extend(docs.into_iter().map(|document| {
                    WriteModel::InsertOne(
                        InsertOneModel::builder()
                            .namespace(namespace.clone())
                            .document(document)
                            .build(),
                    )
                }));
            }

            WriteMode::Upsert { ref key_fields } => {
                for mut bson_doc in docs {
                    // Only a collapsed changelog carries a synthesized `_op` (U/D): route D to a
                    // delete. A plain upsert keeps its columns verbatim (a user `_op` is not a delete).
                    let is_delete = from_changelog && matches!(bson_doc.get_str("_op"), Ok("D"));
                    if from_changelog {
                        bson_doc.remove("_op");
                    }
                    let mut filter = Document::new();
                    for key in key_fields {
                        let value = bson_doc.get(key).ok_or_else(|| {
                            ConnectorError::ConfigurationError(format!(
                                "MongoDB upsert document is missing key field '{key}'"
                            ))
                        })?;
                        filter.insert(key, value.clone());
                    }
                    let filter_bytes = encoded_document_size(
                        &filter,
                        MAX_STANDARD_DOCUMENT_BYTES,
                        "MongoDB upsert filter",
                    )?;
                    model_bytes = checked_converted_total(
                        model_bytes,
                        filter_bytes,
                        usize::MAX,
                        "MongoDB sink bulk",
                    )?;
                    if is_delete {
                        models.push(WriteModel::DeleteOne(
                            DeleteOneModel::builder()
                                .namespace(namespace.clone())
                                .filter(filter)
                                .build(),
                        ));
                        counts.deletes = counts.deletes.saturating_add(1);
                    } else {
                        models.push(WriteModel::ReplaceOne(
                            ReplaceOneModel::builder()
                                .namespace(namespace.clone())
                                .filter(filter)
                                .replacement(bson_doc)
                                .upsert(true)
                                .build(),
                        ));
                        counts.upserts = counts.upserts.saturating_add(1);
                    }
                    ensure_working_set(
                        0,
                        model_bytes,
                        models.len(),
                        0,
                        MAX_SINK_WORKING_SET_BYTES,
                        "MongoDB sink bulk",
                    )?;
                }
            }

            WriteMode::CdcReplay => {
                return Err(ConnectorError::Internal(
                    "CDC replay must use prepared CDC writes".to_string(),
                ));
            }
        }

        self.execute_bulk_models(models, counts, "MongoDB sink bulk_write")
            .await?;
        Ok(())
    }

    async fn execute_bulk_models(
        &self,
        models: Vec<mongodb::options::WriteModel>,
        counts: BulkCounts,
        context: &str,
    ) -> Result<(), ConnectorError> {
        if models.is_empty() {
            return Ok(());
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("client not initialized".to_string()))?;
        let driver_timeout = self.driver_timeout();
        let deadline = tokio::time::Instant::now() + driver_timeout;
        let operation_client = client.clone();
        match await_mongo_sink_operation(&self.task_owner, deadline, async move {
            operation_client.bulk_write(models).ordered(true).await
        })
        .await?
        {
            MongoOperationOutcome::Completed(Ok(_)) => {}
            MongoOperationOutcome::Completed(Err(error)) => {
                self.metrics.record_error();
                return Err(classify_mongo_bulk_failure(
                    context,
                    MongoBulkFailure::Driver(&error),
                ));
            }
            MongoOperationOutcome::Deadline => {
                self.metrics.record_error();
                return Err(classify_mongo_bulk_failure(
                    context,
                    MongoBulkFailure::Deadline(driver_timeout),
                ));
            }
            MongoOperationOutcome::TaskFailed(error) => {
                self.metrics.record_error();
                return Err(ConnectorError::outcome_unknown(
                    format!(
                        "{context} task terminated before its MongoDB outcome was observed: {error}"
                    ),
                    false,
                ));
            }
        }
        self.metrics.record_bulk_write();
        self.metrics.record_inserts(counts.inserts);
        self.metrics.record_upserts(counts.upserts);
        self.metrics.record_deletes(counts.deletes);
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    fn prepare_cdc_writes(
        docs: &[serde_json::Value],
        converted_limit: usize,
        expected_namespace: &str,
    ) -> Result<(Vec<CdcWrite>, u64), ConnectorError> {
        use mongodb::bson::{doc, Bson, Document};

        let mut writes = Vec::with_capacity(docs.len());
        let mut bytes = 0;

        for value in docs {
            let namespace = value
                .get("_namespace")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC replay event requires a non-null string '_namespace'".into(),
                    )
                })?;
            if namespace != expected_namespace {
                return Err(ConnectorError::ConfigurationError(format!(
                    "MongoDB CDC replay namespace '{namespace}' does not match fixed target \
                     '{expected_namespace}'"
                )));
            }
            let op = value
                .get("_op")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError(
                        "MongoDB CDC replay event requires a non-null string '_op'".into(),
                    )
                })?;

            let write = match op {
                "I" | "R" => {
                    let key = Self::parse_cdc_field(value, "_document_key")?;
                    let full_document = Self::parse_cdc_field(value, "_full_document")?;
                    let key = json_to_bson_document(key.as_ref())?;
                    let filter = validate_cdc_document_key(&key)?;
                    let replacement = json_to_bson_document(full_document.as_ref())?;
                    validate_cdc_replacement_key(&key, &replacement)?;
                    account_bson_document(
                        &mut bytes,
                        &filter,
                        converted_limit,
                        "MongoDB CDC document key",
                    )?;
                    account_bson_document(
                        &mut bytes,
                        &replacement,
                        converted_limit,
                        "MongoDB CDC replacement document",
                    )?;
                    if op == "I" {
                        CdcWrite::Insert {
                            filter,
                            replacement,
                        }
                    } else {
                        CdcWrite::Replace {
                            filter,
                            replacement,
                        }
                    }
                }
                "U" => {
                    let key = Self::parse_cdc_field(value, "_document_key")?;
                    let key = json_to_bson_document(key.as_ref())?;
                    let filter = validate_cdc_document_key(&key)?;

                    if value
                        .get("_full_document")
                        .is_some_and(|document| !document.is_null())
                    {
                        let full_document = Self::parse_cdc_field(value, "_full_document")?;
                        let replacement = json_to_bson_document(full_document.as_ref())?;
                        validate_cdc_replacement_key(&key, &replacement)?;
                        account_bson_document(
                            &mut bytes,
                            &filter,
                            converted_limit,
                            "MongoDB CDC document key",
                        )?;
                        account_bson_document(
                            &mut bytes,
                            &replacement,
                            converted_limit,
                            "MongoDB CDC replacement document",
                        )?;
                        writes.push(CdcWrite::Replace {
                            filter,
                            replacement,
                        });
                        continue;
                    }

                    let description = Self::parse_cdc_field(value, "_update_desc")?;
                    if let Some(disambiguated) = description
                        .get("disambiguated_paths")
                        .or_else(|| description.get("disambiguatedPaths"))
                    {
                        let paths = disambiguated.as_object().ok_or_else(|| {
                            ConnectorError::ConfigurationError(
                                "MongoDB CDC disambiguated_paths must be an object".into(),
                            )
                        })?;
                        if !paths.is_empty() {
                            return Err(ConnectorError::ConfigurationError(
                                "MongoDB CDC replay cannot safely apply ambiguous field paths; \
                                 use a full-document update mode"
                                    .into(),
                            ));
                        }
                    }

                    let mut update = Document::new();
                    if let Some(updated) = description
                        .get("updated_fields")
                        .or_else(|| description.get("updatedFields"))
                    {
                        update.insert("$set", Bson::Document(json_to_bson_document(updated)?));
                    }

                    if let Some(removed) = description
                        .get("removed_fields")
                        .or_else(|| description.get("removedFields"))
                    {
                        let fields = removed.as_array().ok_or_else(|| {
                            ConnectorError::ConfigurationError(
                                "MongoDB CDC removed_fields must be an array".into(),
                            )
                        })?;
                        if !fields.is_empty() {
                            let mut unset = Document::new();
                            for field in fields {
                                let field = field.as_str().ok_or_else(|| {
                                    ConnectorError::ConfigurationError(
                                        "MongoDB CDC removed_fields entries must be strings".into(),
                                    )
                                })?;
                                unset.insert(field, "");
                            }
                            update.insert("$unset", unset);
                        }
                    }

                    if let Some(truncated) = description
                        .get("truncated_arrays")
                        .or_else(|| description.get("truncatedArrays"))
                    {
                        let arrays = truncated.as_array().ok_or_else(|| {
                            ConnectorError::ConfigurationError(
                                "MongoDB CDC truncated_arrays must be an array".into(),
                            )
                        })?;
                        if !arrays.is_empty() {
                            let mut push = Document::new();
                            for array in arrays {
                                let field = array
                                    .get("field")
                                    .and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| {
                                        ConnectorError::ConfigurationError(
                                            "MongoDB CDC truncated array requires 'field'".into(),
                                        )
                                    })?;
                                let new_size = array
                                    .get("new_size")
                                    .or_else(|| array.get("newSize"))
                                    .and_then(serde_json::Value::as_u64)
                                    .and_then(|size| i64::try_from(size).ok())
                                    .ok_or_else(|| {
                                        ConnectorError::ConfigurationError(
                                            "MongoDB CDC truncated array requires an i64 'new_size'"
                                                .into(),
                                        )
                                    })?;
                                push.insert(
                                    field,
                                    doc! { "$each": Bson::Array(Vec::new()), "$slice": new_size },
                                );
                            }
                            update.insert("$push", push);
                        }
                    }

                    account_bson_document(
                        &mut bytes,
                        &filter,
                        converted_limit,
                        "MongoDB CDC document key",
                    )?;
                    if update.is_empty() {
                        CdcWrite::Noop
                    } else {
                        account_bson_document(
                            &mut bytes,
                            &update,
                            converted_limit,
                            "MongoDB CDC update document",
                        )?;
                        CdcWrite::Update { filter, update }
                    }
                }
                "D" => {
                    let key = Self::parse_cdc_field(value, "_document_key")?;
                    let key = json_to_bson_document(key.as_ref())?;
                    let filter = validate_cdc_document_key(&key)?;
                    account_bson_document(
                        &mut bytes,
                        &filter,
                        converted_limit,
                        "MongoDB CDC document key",
                    )?;
                    CdcWrite::Delete { filter }
                }
                "DROP" | "RENAME" | "INVALIDATE" | "DROP_DATABASE" => {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC replay cannot apply lifecycle operation '{op}' to fixed \
                         destination '{expected_namespace}'"
                    )));
                }
                other => {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC replay does not support operation '{other}'"
                    )));
                }
            };
            writes.push(write);
        }

        Ok((writes, bytes))
    }

    async fn execute_cdc_writes(&self, writes: Vec<CdcWrite>) -> Result<(), ConnectorError> {
        let collection = self
            .collection
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("collection not initialized".to_string()))?;
        let namespace = collection.namespace();
        let (models, counts) = cdc_bulk_models(&namespace, writes);
        self.execute_bulk_models(models, counts, "MongoDB CDC bulk_write")
            .await
    }
}

#[derive(Clone, Copy)]
enum MongoBulkFailure<'a> {
    Driver(&'a mongodb::error::Error),
    Deadline(Duration),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MongoBulkFailureShape {
    PreCommandTransient,
    PreCommandTerminal,
    Transport,
    Command,
    WriteRejected,
    WriteConcern,
    Bulk {
        partial: bool,
        write_errors: bool,
        write_concern_errors: bool,
    },
    Deadline,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
struct MongoBulkFailureFacts {
    no_writes: bool,
    retryable_signal: bool,
    shape: MongoBulkFailureShape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MongoBulkDisposition {
    DefinitelyNotApplied { retryable: bool },
    OutcomeUnknown { retryable: bool },
}

fn mongo_bulk_failure_facts(error: &mongodb::error::Error) -> MongoBulkFailureFacts {
    use mongodb::error::{
        ErrorKind, WriteFailure, NO_WRITES_PERFORMED, RETRYABLE_ERROR, RETRYABLE_WRITE_ERROR,
        SYSTEM_OVERLOADED_ERROR,
    };

    let retryable_label = error.contains_label(RETRYABLE_WRITE_ERROR)
        || (error.contains_label(SYSTEM_OVERLOADED_ERROR) && error.contains_label(RETRYABLE_ERROR));
    let transient_cause = mongo_error_chain_has_transient_cause(error);
    let shape = match error.kind.as_ref() {
        ErrorKind::ServerSelection { .. } | ErrorKind::DnsResolve { .. } => {
            MongoBulkFailureShape::PreCommandTransient
        }
        ErrorKind::InvalidArgument { .. }
        | ErrorKind::Authentication { .. }
        | ErrorKind::BsonSerialization(_)
        | ErrorKind::SessionsNotSupported
        | ErrorKind::InvalidTlsConfig { .. }
        | ErrorKind::IncompatibleServer { .. }
        | ErrorKind::Shutdown => MongoBulkFailureShape::PreCommandTerminal,
        ErrorKind::Io(_) | ErrorKind::ConnectionPoolCleared { .. } => {
            MongoBulkFailureShape::Transport
        }
        ErrorKind::Command(_) => MongoBulkFailureShape::Command,
        ErrorKind::Write(WriteFailure::WriteError(_)) => MongoBulkFailureShape::WriteRejected,
        ErrorKind::Write(WriteFailure::WriteConcernError(_)) => MongoBulkFailureShape::WriteConcern,
        ErrorKind::BulkWrite(bulk) => MongoBulkFailureShape::Bulk {
            partial: bulk.partial_result.is_some(),
            write_errors: !bulk.write_errors.is_empty(),
            write_concern_errors: !bulk.write_concern_errors.is_empty(),
        },
        _ => MongoBulkFailureShape::Unknown,
    };

    MongoBulkFailureFacts {
        no_writes: error.contains_label(NO_WRITES_PERFORMED),
        retryable_signal: retryable_label || transient_cause,
        shape,
    }
}

fn mongo_error_chain_has_transient_cause(error: &mongodb::error::Error) -> bool {
    use std::error::Error as _;

    use mongodb::error::ErrorKind;

    let mut current = Some(error);
    while let Some(error) = current {
        if matches!(
            error.kind.as_ref(),
            ErrorKind::Io(_)
                | ErrorKind::ConnectionPoolCleared { .. }
                | ErrorKind::ServerSelection { .. }
                | ErrorKind::DnsResolve { .. }
        ) {
            return true;
        }
        current = error
            .source()
            .and_then(|source| source.downcast_ref::<mongodb::error::Error>());
    }
    false
}

fn classify_mongo_bulk_facts(facts: MongoBulkFailureFacts) -> MongoBulkDisposition {
    use MongoBulkDisposition::{DefinitelyNotApplied, OutcomeUnknown};
    use MongoBulkFailureShape::{
        Bulk, Command, Deadline, PreCommandTerminal, PreCommandTransient, Transport, Unknown,
        WriteConcern, WriteRejected,
    };

    let partial_result = matches!(facts.shape, Bulk { partial: true, .. });
    // A nested NoWritesPerformed label only describes the failed wire batch. Earlier wire
    // batches are still applied when the driver reports a partial result on the wrapper error.
    if facts.no_writes && !partial_result {
        let retryable = match facts.shape {
            PreCommandTerminal
            | Bulk {
                write_errors: true, ..
            } => false,
            PreCommandTransient | Transport => true,
            _ => facts.retryable_signal,
        };
        return DefinitelyNotApplied { retryable };
    }

    match facts.shape {
        PreCommandTransient => DefinitelyNotApplied { retryable: true },
        PreCommandTerminal => DefinitelyNotApplied { retryable: false },
        WriteRejected => DefinitelyNotApplied {
            retryable: facts.retryable_signal,
        },
        Bulk {
            partial,
            write_errors: true,
            write_concern_errors,
        } => {
            if partial || write_concern_errors {
                OutcomeUnknown { retryable: false }
            } else {
                DefinitelyNotApplied { retryable: false }
            }
        }
        Transport | Deadline => OutcomeUnknown { retryable: true },
        Command | Bulk { .. } | WriteConcern | Unknown => OutcomeUnknown {
            retryable: facts.retryable_signal,
        },
    }
}

fn classify_mongo_bulk_failure(context: &str, failure: MongoBulkFailure<'_>) -> ConnectorError {
    let (facts, detail) = match failure {
        MongoBulkFailure::Driver(error) => (mongo_bulk_failure_facts(error), error.to_string()),
        MongoBulkFailure::Deadline(timeout) => (
            MongoBulkFailureFacts {
                no_writes: false,
                retryable_signal: true,
                shape: MongoBulkFailureShape::Deadline,
            },
            format!("timed out after {timeout:?}"),
        ),
    };

    match classify_mongo_bulk_facts(facts) {
        MongoBulkDisposition::DefinitelyNotApplied { retryable: true } => {
            ConnectorError::WriteError(format!(
                "{context} failed without applying any ordered bulk write: {detail}"
            ))
        }
        MongoBulkDisposition::DefinitelyNotApplied { retryable: false } => {
            ConnectorError::ConfigurationError(format!(
                "{context} was rejected without applying any ordered bulk write: {detail}"
            ))
        }
        MongoBulkDisposition::OutcomeUnknown { retryable } => ConnectorError::outcome_unknown(
            format!(
                "{context} failed after dispatch; MongoDB may have applied part or all of the \
                 ordered bulk: {detail}"
            ),
            retryable,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use futures_util::FutureExt as _;

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

    fn test_config() -> MongoDbSinkConfig {
        MongoDbSinkConfig::new("mongodb://localhost:27017", "db", "coll")
    }

    #[test]
    fn test_new_sink() {
        let config = MongoDbSinkConfig::new("mongodb://localhost:27017", "db", "coll");
        let sink = MongoDbSink::new(test_schema(), config, None);
        assert_eq!(sink.buffered_rows(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_wait_before_bulk_task_first_poll_keeps_operation_tracked() {
        let sink = MongoDbSink::new(test_schema(), test_config(), None);
        let terminal = sink.terminal_task_tracker().unwrap();
        let polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_polled = Arc::clone(&polled);
        let release = Arc::new(tokio::sync::Notify::new());
        let task_release = Arc::clone(&release);

        let wait = await_mongo_sink_operation(
            &sink.task_owner,
            tokio::time::Instant::now() + Duration::from_secs(60),
            async move {
                task_polled.store(true, std::sync::atomic::Ordering::Release);
                task_release.notified().await;
            },
        );
        assert!(
            wait.now_or_never().is_none(),
            "operation wait must still be pending"
        );
        assert!(
            !polled.load(std::sync::atomic::Ordering::Acquire),
            "the spawned bulk task must not have reached its first poll"
        );

        drop(sink);
        assert!(!terminal.is_terminated());
        tokio::task::yield_now().await;
        assert!(polled.load(std::sync::atomic::Ordering::Acquire));

        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
            .await
            .expect("tracker must resolve only after the detached operation exits");
    }

    #[tokio::test]
    async fn bulk_deadline_keeps_generation_non_terminal_until_operation_exit() {
        let sink = MongoDbSink::new(test_schema(), test_config(), None);
        let terminal = sink.terminal_task_tracker().unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let task_release = Arc::clone(&release);

        let outcome =
            await_mongo_sink_operation(&sink.task_owner, tokio::time::Instant::now(), async move {
                task_release.notified().await;
            })
            .await
            .unwrap();
        assert!(matches!(outcome, MongoOperationOutcome::Deadline));

        drop(sink);
        assert!(!terminal.is_terminated());
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
            .await
            .expect("deadline must not publish terminal state before the operation exits");
    }

    #[test]
    fn mongo_bulk_failure_classification_table() {
        use MongoBulkDisposition::{DefinitelyNotApplied, OutcomeUnknown};
        use MongoBulkFailureShape::{
            Bulk, Command, Deadline, Transport, Unknown, WriteConcern, WriteRejected,
        };

        let facts = |shape, no_writes, retryable_signal| MongoBulkFailureFacts {
            no_writes,
            retryable_signal,
            shape,
        };
        let bulk = |partial, write_errors, write_concern_errors| Bulk {
            partial,
            write_errors,
            write_concern_errors,
        };

        let cases = [
            (
                "no writes performed",
                facts(Transport, true, true),
                DefinitelyNotApplied { retryable: true },
            ),
            (
                "partial permanent rejection",
                facts(bulk(true, true, false), false, false),
                OutcomeUnknown { retryable: false },
            ),
            (
                "partial transport failure",
                // The nested retry wrote nothing, but an earlier wire batch still succeeded.
                facts(bulk(true, false, false), true, true),
                OutcomeUnknown { retryable: true },
            ),
            (
                "deadline",
                facts(Deadline, false, true),
                OutcomeUnknown { retryable: true },
            ),
            (
                "first ordered write rejected",
                facts(bulk(false, true, false), false, false),
                DefinitelyNotApplied { retryable: false },
            ),
            (
                "unknown terminal failure",
                facts(Unknown, false, false),
                OutcomeUnknown { retryable: false },
            ),
            (
                "retryable write concern failure",
                facts(WriteConcern, false, true),
                OutcomeUnknown { retryable: true },
            ),
            (
                "retryable server rejection",
                facts(Command, false, true),
                OutcomeUnknown { retryable: true },
            ),
            (
                "per-item write rejection",
                facts(WriteRejected, false, false),
                DefinitelyNotApplied { retryable: false },
            ),
        ];

        for (name, facts, expected) in cases {
            assert_eq!(classify_mongo_bulk_facts(facts), expected, "{name}");
        }
    }

    #[test]
    fn later_chunk_failure_preserves_partial_application() {
        let completed = WriteResult::new(7, 256);
        let error = mongo_partial_batch_error(
            &completed,
            ConnectorError::ConfigurationError("later chunk rejected".into()),
        );
        assert!(error.is_outcome_unknown());
        assert!(!error.is_transient());
        assert!(error.to_string().contains("7 records and 256 bytes"));

        let before_output = mongo_partial_batch_error(
            &WriteResult::new(0, 0),
            ConnectorError::ConfigurationError("rejected before output".into()),
        );
        assert!(matches!(
            before_output,
            ConnectorError::ConfigurationError(_)
        ));
    }

    #[test]
    fn mongo_transport_and_deadline_errors_require_retirement() {
        let driver_error: mongodb::error::Error =
            std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset").into();
        let transport = classify_mongo_bulk_failure(
            "MongoDB sink bulk_write",
            MongoBulkFailure::Driver(&driver_error),
        );
        assert!(transport.is_outcome_unknown());
        assert!(transport.is_transient());

        let deadline = classify_mongo_bulk_failure(
            "MongoDB sink bulk_write",
            MongoBulkFailure::Deadline(Duration::from_secs(1)),
        );
        assert!(deadline.is_outcome_unknown());
        assert!(deadline.is_transient());
        assert!(deadline.to_string().contains("timed out after 1s"));
    }

    #[test]
    fn mongo_driver_partial_result_is_classified_as_applied() {
        let mut bulk = mongodb::error::BulkWriteError::default();
        bulk.partial_result = Some(mongodb::error::PartialBulkWriteResult::Summary(
            mongodb::results::SummaryBulkWriteResult::default(),
        ));
        let driver_error: mongodb::error::Error = mongodb::error::ErrorKind::BulkWrite(bulk).into();

        assert_eq!(
            classify_mongo_bulk_facts(mongo_bulk_failure_facts(&driver_error)),
            MongoBulkDisposition::OutcomeUnknown { retryable: false }
        );
    }

    #[test]
    fn unsupported_arrow_types_fail_schema_validation() {
        for data_type in [DataType::Binary, DataType::UInt64] {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                data_type.clone(),
                false,
            )]));
            let error = MongoDbSink::validate_schema(&schema, &test_config()).unwrap_err();
            assert!(error.to_string().contains("unsupported Arrow type"));
        }
    }

    #[test]
    fn engine_schema_replaces_programmatic_schema_and_validates_upsert_keys() {
        let engine_schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Utf8, false),
            Field::new("sequence", DataType::Int64, false),
        ]));
        let mut config = ConnectorConfig::new("mongodb-sink");
        config.set("connection.uri", "mongodb://localhost:27017");
        config.set("database", "db");
        config.set("collection", "out");
        config.set("write.mode", "upsert");
        config.set("write.mode.key_fields", "tenant");
        config.set(
            "_arrow_schema",
            crate::config::encode_arrow_schema_ipc(engine_schema.as_ref()),
        );

        let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
        sink.apply_connector_config(&config).unwrap();
        assert_eq!(sink.schema, engine_schema);

        config.set("write.mode.key_fields", "missing");
        let error = sink.apply_connector_config(&config).unwrap_err();
        assert!(error.to_string().contains("missing"));
    }

    #[test]
    fn test_sink_contract_insert() {
        let config = test_config();
        let sink = MongoDbSink::new(test_schema(), config, None);
        let contract = sink.contract(&ConnectorConfig::new("mongodb")).unwrap();
        assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
        assert_eq!(contract.topology, SinkTopology::MultiWriter);
        assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
        assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_sink_contract_upsert() {
        let mut config = test_config();
        config.write_mode = WriteMode::Upsert {
            key_fields: vec!["id".to_string()],
        };
        let sink = MongoDbSink::new(test_schema(), config, None);
        let contract = sink.contract(&ConnectorConfig::new("mongodb")).unwrap();
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
    }

    #[test]
    fn test_sink_contract_cdc_replay() {
        let mut config = test_config();
        config.write_mode = WriteMode::CdcReplay;
        let sink = MongoDbSink::new(super::super::mongodb_cdc_envelope_schema(), config, None);
        let contract = sink.contract(&ConnectorConfig::new("mongodb")).unwrap();
        assert_eq!(contract.topology, SinkTopology::Singleton);
        assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
    }

    #[test]
    fn batches_to_bson_are_direct_and_working_set_bounded() {
        let batches = vec![test_batch(3)];
        let retained = retained_batch_bytes(&batches[0]);
        let (docs, byte_estimate) = MongoDbSink::batches_to_bson_docs(
            &batches,
            retained,
            MAX_SINK_WORKING_SET_BYTES,
            MAX_STANDARD_DOCUMENT_BYTES,
        )
        .unwrap();
        assert_eq!(docs.len(), 3);
        assert!(byte_estimate > 0);
        assert_eq!(docs[0].get_i64("id").unwrap(), 0);
        assert_eq!(docs[0].get_str("name").unwrap(), "user_0");
    }

    #[test]
    fn arrow_to_bson_maps_scalar_types() {
        use mongodb::bson::Bson;
        let ts = arrow_array::TimestampMillisecondArray::from(vec![Some(1_700_000_000_000), None]);
        assert!(
            matches!(arrow_value_to_bson(&ts, 0).unwrap(), Bson::DateTime(dt) if dt.timestamp_millis() == 1_700_000_000_000)
        );
        assert_eq!(arrow_value_to_bson(&ts, 1).unwrap(), Bson::Null);
        assert_eq!(
            arrow_value_to_bson(&Int64Array::from(vec![42]), 0).unwrap(),
            Bson::Int64(42)
        );
        assert_eq!(
            arrow_value_to_bson(&StringArray::from(vec!["x"]), 0).unwrap(),
            Bson::String("x".to_string())
        );
    }

    #[test]
    fn timestamp_conversion_floors_pre_epoch_values_and_rejects_overflow() {
        let micros = arrow_array::TimestampMicrosecondArray::from(vec![-1]);
        assert_eq!(timestamp_millis(&micros, 0).unwrap(), -1);
        let nanos = arrow_array::TimestampNanosecondArray::from(vec![-1]);
        assert_eq!(timestamp_millis(&nanos, 0).unwrap(), -1);
        let seconds = arrow_array::TimestampSecondArray::from(vec![i64::MAX]);
        assert!(timestamp_millis(&seconds, 0).is_err());
    }

    #[test]
    fn take_buffer_resets_all_accounting() {
        let config = MongoDbSinkConfig::default();
        let mut sink = MongoDbSink::new(test_schema(), config, None);

        let batch = test_batch(5);
        let retained = retained_batch_bytes(&batch);
        sink.buffer.push(batch);
        sink.buffered_rows = 5;
        sink.buffered_retained_bytes = retained;

        let (pending, pending_retained) = sink.take_buffer();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending_retained, retained);
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
        assert!(sink.buffer.is_empty());
    }

    #[test]
    fn configured_flush_interval_is_runtime_timer_authority() {
        let mut config = test_config();
        config.flush_interval_ms = 37;
        let sink = MongoDbSink::new(test_schema(), config, None);
        assert_eq!(sink.flush_interval(), Duration::from_millis(37));
    }

    #[test]
    fn runtime_write_budget_derives_driver_headroom_and_clamps_client_timeouts() {
        let mut connector = ConnectorConfig::new("mongodb-sink");
        connector.set("sink.write.timeout.ms", "500");
        let timeout = MongoDbSink::configured_write_timeout(&connector).unwrap();
        let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
        sink.write_timeout = timeout;

        assert_eq!(sink.suggested_write_timeout(), Duration::from_millis(500));
        assert_eq!(sink.driver_timeout(), Duration::from_millis(400));
        assert_eq!(
            clamp_client_timeout(Some(Duration::from_secs(2)), sink.driver_timeout()),
            Duration::from_millis(400)
        );
        assert_eq!(
            clamp_client_timeout(Some(Duration::from_millis(50)), sink.driver_timeout()),
            Duration::from_millis(50)
        );
        assert_eq!(
            clamp_client_timeout(Some(Duration::ZERO), sink.driver_timeout()),
            Duration::from_millis(400)
        );

        connector.set("sink.write.timeout.ms", "99");
        assert!(MongoDbSink::configured_write_timeout(&connector).is_err());
    }

    #[test]
    fn mongodb_tls_defaults_to_verified_and_rejects_insecure_mode() {
        use mongodb::options::{ClientOptions, Tls, TlsOptions};

        let mut defaults = ClientOptions::default();
        harden_mongodb_tls(&mut defaults).unwrap();
        assert!(matches!(defaults.tls, Some(Tls::Enabled(_))));

        let mut explicit_plaintext = ClientOptions::default();
        explicit_plaintext.tls = Some(Tls::Disabled);
        harden_mongodb_tls(&mut explicit_plaintext).unwrap();
        assert_eq!(explicit_plaintext.tls, Some(Tls::Disabled));

        let mut insecure = ClientOptions::default();
        insecure.tls = Some(Tls::Enabled(
            TlsOptions::builder()
                .allow_invalid_certificates(true)
                .build(),
        ));
        let error = harden_mongodb_tls(&mut insecure).unwrap_err();
        assert!(error.to_string().contains("tlsInsecure"));
    }

    #[test]
    fn namespace_exists_detection_uses_server_error_code() {
        assert!(is_namespace_exists_code(48));
        assert!(!is_namespace_exists_code(47));
        assert!(!is_namespace_exists_code(49));
    }

    #[test]
    fn retained_limit_counts_variable_width_memory_and_allows_exact_boundary() {
        let narrow = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        )
        .unwrap();
        let wide_value = "x".repeat(4096);
        let wide = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(StringArray::from(vec![wide_value.as_str()])),
            ],
        )
        .unwrap();
        let narrow_bytes = retained_batch_bytes(&narrow);
        let wide_bytes = retained_batch_bytes(&wide);
        assert!(wide_bytes > narrow_bytes);

        let exact_limit = narrow_bytes + wide_bytes;
        assert!(!requires_preflush(narrow_bytes, wide_bytes, exact_limit).unwrap());
        assert!(requires_preflush(narrow_bytes, wide_bytes, exact_limit - 1).unwrap());
        assert!(requires_preflush(usize::MAX, 1, usize::MAX).unwrap());
    }

    #[test]
    fn converted_limit_allows_exact_boundary_and_rejects_crossing() {
        assert_eq!(checked_converted_total(5, 7, 12, "test").unwrap(), 12);
        let error = checked_converted_total(5, 8, 12, "test").unwrap_err();
        assert!(!error.is_transient());
    }

    #[test]
    fn one_working_set_budget_covers_retained_models_and_staging() {
        let retained = 1_024;
        let encoded = 2_048;
        let models = 3;
        let staging = 512;
        let exact = retained
            + encoded * MATERIALIZED_BYTE_CHARGE
            + models * WRITE_MODEL_OVERHEAD_BYTES
            + staging;
        assert_eq!(
            working_set_charge(retained, encoded as u64, models, staging),
            Some(exact)
        );
        ensure_working_set(retained, encoded as u64, models, staging, exact, "test").unwrap();
        let error =
            ensure_working_set(retained, encoded as u64, models, staging, exact - 1, "test")
                .unwrap_err();
        assert!(error.to_string().contains("working set"));
        assert!(working_set_charge(usize::MAX, 1, 1, 1).is_none());
    }

    #[test]
    fn bson_document_limit_uses_exact_encoded_size() {
        let document = mongodb::bson::doc! { "id": 1_i64, "name": "value" };
        let exact = mongodb::bson::to_vec(&document).unwrap().len();
        assert_eq!(
            encoded_document_size(&document, exact, "test document").unwrap(),
            exact
        );
        let error = encoded_document_size(&document, exact - 1, "test document").unwrap_err();
        assert!(!error.is_transient());
        assert!(error.to_string().contains("BSON document"));
    }

    #[test]
    fn time_series_conversion_enforces_four_mib_document_limit() {
        let value = "x".repeat(MAX_TIMESERIES_DOCUMENT_BYTES);
        let batch = RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![value])),
            ],
        )
        .unwrap();
        let retained = retained_batch_bytes(&batch);
        let error = MongoDbSink::batches_to_bson_docs(
            &[batch],
            retained,
            usize::MAX,
            MAX_TIMESERIES_DOCUMENT_BYTES,
        )
        .unwrap_err();
        assert!(error.to_string().contains("4194304"));
    }

    #[tokio::test]
    async fn oversized_batch_rejection_preserves_existing_buffer() {
        let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;
        let existing = test_batch(1);
        sink.write_batch_with_retained_limit(&existing, usize::MAX)
            .await
            .unwrap();
        let rows_before = sink.buffered_rows;
        let bytes_before = sink.buffered_retained_bytes;
        let batches_before = sink.buffer.len();

        let incoming = test_batch(2);
        let incoming_bytes = retained_batch_bytes(&incoming);
        let error = sink
            .write_batch_with_retained_limit(&incoming, incoming_bytes - 1)
            .await
            .expect_err("oversized batch must fail before admission");

        assert!(!error.is_transient());
        assert_eq!(sink.buffered_rows, rows_before);
        assert_eq!(sink.buffered_retained_bytes, bytes_before);
        assert_eq!(sink.buffer.len(), batches_before);
    }

    #[tokio::test]
    async fn automatic_flush_error_clears_buffer_and_accounting() {
        let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;
        let error = sink
            .write_batch_with_retained_limit(&test_batch(MAX_DOCUMENTS_PER_FLUSH), usize::MAX)
            .await
            .expect_err("missing collection must fail the automatic flush");

        assert!(matches!(error, ConnectorError::Internal(_)));
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
    }

    #[tokio::test]
    async fn lifecycle_and_schema_are_checked_before_write() {
        let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
        let state_error = sink.write_batch(&test_batch(1)).await.unwrap_err();
        assert!(matches!(state_error, ConnectorError::InvalidState { .. }));

        sink.state = ConnectorState::Running;
        let other_schema = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let other_batch =
            RecordBatch::try_new(other_schema, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        let schema_error = sink.write_batch(&other_batch).await.unwrap_err();
        assert!(matches!(schema_error, ConnectorError::SchemaMismatch(_)));
    }

    #[tokio::test]
    async fn close_releases_resources_but_returns_pending_flush_error() {
        let mut sink = MongoDbSink::new(test_schema(), test_config(), None);
        sink.state = ConnectorState::Running;
        sink.write_batch_with_retained_limit(&test_batch(1), usize::MAX)
            .await
            .unwrap();

        let error = sink.close().await.expect_err("pending flush must fail");
        assert!(matches!(error, ConnectorError::Internal(_)));
        assert_eq!(sink.state, ConnectorState::Closed);
        assert!(sink.buffer.is_empty());
        assert_eq!(sink.buffered_rows, 0);
        assert_eq!(sink.buffered_retained_bytes, 0);
        assert!(sink.collection.is_none());
        assert!(sink.client.is_none());
    }

    #[test]
    fn cdc_insert_is_prepared_as_document_keyed_idempotent_upsert() {
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "I",
            "_document_key": r#"{"_id":"a"}"#,
            "_full_document": r#"{"_id":"a","value":1}"#,
            "_update_desc": null
        })];
        let (writes, bytes) =
            MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
        assert!(bytes > 0);
        match &writes[0] {
            CdcWrite::Insert {
                filter,
                replacement,
            } => {
                assert_eq!(
                    filter.get_document("_id").unwrap().get_str("$eq").unwrap(),
                    "a"
                );
                assert_eq!(replacement.get_i32("value").unwrap(), 1);
            }
            _ => panic!("insert must be a keyed upsert plan"),
        }
    }

    #[test]
    fn cdc_replay_requires_id_but_accepts_complete_sharded_and_document_keys() {
        let missing_id = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "D",
            "_document_key": r#"{"tenant":"a"}"#,
            "_full_document": null,
            "_update_desc": null
        })];
        let error =
            MongoDbSink::prepare_cdc_writes(&missing_id, MAX_SINK_WORKING_SET_BYTES, "db.coll")
                .unwrap_err();
        assert!(error.to_string().contains("must contain '_id'"));

        for key in [
            r#"{"_id":"a","tenant":"t"}"#,
            r#"{"_id":{"tenant":"t","sequence":1}}"#,
        ] {
            let rows = vec![serde_json::json!({
                "_namespace": "db.coll",
                "_op": "D",
                "_document_key": key,
                "_full_document": null,
                "_update_desc": null
            })];
            MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
        }
    }

    #[test]
    fn cdc_replay_rejects_cross_namespace_rows() {
        let rows = vec![serde_json::json!({
            "_namespace": "source.events",
            "_op": "D",
            "_document_key": r#"{"_id":"a"}"#,
            "_full_document": null,
            "_update_desc": null
        })];
        let error =
            MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "target.events")
                .unwrap_err();
        assert!(error.to_string().contains("fixed target"));
    }

    #[test]
    fn cdc_replay_rejects_replacement_key_drift() {
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "I",
            "_document_key": r#"{"_id":"a","tenant":"source"}"#,
            "_full_document": r#"{"_id":"a","tenant":"other","value":1}"#,
            "_update_desc": null
        })];
        let error = MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll")
            .unwrap_err();
        assert!(error.to_string().contains("tenant"), "{error}");
    }

    #[test]
    fn cdc_bulk_models_preserve_mixed_operation_order() {
        use mongodb::options::WriteModel;

        let writes = vec![
            CdcWrite::Insert {
                filter: mongodb::bson::doc! { "_id": "a" },
                replacement: mongodb::bson::doc! { "_id": "a", "v": 1 },
            },
            CdcWrite::Update {
                filter: mongodb::bson::doc! { "_id": "a" },
                update: mongodb::bson::doc! { "$set": { "v": 2 } },
            },
            CdcWrite::Noop,
            CdcWrite::Delete {
                filter: mongodb::bson::doc! { "_id": "a" },
            },
            CdcWrite::Replace {
                filter: mongodb::bson::doc! { "_id": "b" },
                replacement: mongodb::bson::doc! { "_id": "b", "v": 3 },
            },
        ];
        let (models, counts) = cdc_bulk_models(&mongodb::Namespace::new("db", "out"), writes);

        assert_eq!(models.len(), 4);
        assert!(matches!(&models[0], WriteModel::ReplaceOne(model) if model.upsert == Some(true)));
        assert!(matches!(&models[1], WriteModel::UpdateOne(_)));
        assert!(matches!(&models[2], WriteModel::DeleteOne(_)));
        assert!(matches!(&models[3], WriteModel::ReplaceOne(model) if model.upsert == Some(true)));
        assert_eq!(counts.inserts, 1);
        assert_eq!(counts.upserts, 2);
        assert_eq!(counts.deletes, 1);
    }

    #[test]
    fn cdc_update_accepts_source_shape_and_preserves_array_truncation() {
        let update_description = serde_json::json!({
            "updated_fields": {"name": "new"},
            "removed_fields": ["obsolete"],
            "truncated_arrays": [{"field": "items", "new_size": 2}]
        });
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "U",
            "_document_key": r#"{"_id":"a"}"#,
            "_full_document": null,
            "_update_desc": update_description.to_string()
        })];
        let (writes, _) =
            MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
        match &writes[0] {
            CdcWrite::Update { update, .. } => {
                assert!(update.contains_key("$set"));
                assert!(update.contains_key("$unset"));
                assert!(update.contains_key("$push"));
            }
            _ => panic!("update event must produce an update plan"),
        }
    }

    #[test]
    fn cdc_unknown_operation_fails_closed() {
        for operation in ["FUTURE_OP", "DROP", "RENAME", "INVALIDATE", "DROP_DATABASE"] {
            let rows = vec![serde_json::json!({
                "_namespace": "db.coll",
                "_op": operation
            })];
            let error =
                MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll")
                    .unwrap_err();
            assert!(!error.is_transient());
            assert!(error.to_string().contains(operation), "{error}");
        }
    }

    #[test]
    fn cdc_ambiguous_update_paths_fail_closed() {
        let update_description = serde_json::json!({
            "updated_fields": {"a.b": 1},
            "removed_fields": [],
            "truncated_arrays": [],
            "disambiguated_paths": {"a.b": ["a.b"]}
        });
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "U",
            "_document_key": r#"{"_id":"a"}"#,
            "_full_document": null,
            "_update_desc": update_description.to_string()
        })];

        let error = MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll")
            .unwrap_err();
        assert!(!error.is_transient());
        assert!(error.to_string().contains("ambiguous field paths"));
    }

    #[test]
    fn cdc_full_document_update_uses_idempotent_replacement() {
        let update_description = serde_json::json!({
            "updated_fields": {"a.b": 1},
            "removed_fields": [],
            "truncated_arrays": [],
            "disambiguated_paths": {"a.b": ["a.b"]}
        });
        let rows = vec![serde_json::json!({
            "_namespace": "db.coll",
            "_op": "U",
            "_document_key": r#"{"_id":"a"}"#,
            "_full_document": r#"{"_id":"a","a.b":1}"#,
            "_update_desc": update_description.to_string()
        })];

        let (writes, _) =
            MongoDbSink::prepare_cdc_writes(&rows, MAX_SINK_WORKING_SET_BYTES, "db.coll").unwrap();
        assert!(matches!(&writes[0], CdcWrite::Replace { .. }));
    }
}
