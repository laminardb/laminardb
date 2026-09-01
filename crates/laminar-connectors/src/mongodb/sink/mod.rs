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

mod bulk;
mod cdc;
mod conversion;
mod failure;
mod lifecycle;
mod validation;

#[cfg(test)]
use bulk::{await_mongo_sink_operation, MongoOperationOutcome};
use bulk::{cdc_bulk_models, CdcWrite};
use conversion::{
    account_bson_document, accumulate_write_result, arrow_value_to_bson, cdc_row_value,
    checked_converted_total, clamp_client_timeout, encoded_document_size, ensure_working_set,
    json_to_bson_document, mongo_partial_batch_error, requires_preflush, retained_batch_bytes,
    validate_cdc_document_key, validate_cdc_replacement_key,
};
#[cfg(test)]
use conversion::{timestamp_millis, working_set_charge};
#[cfg(test)]
use failure::{
    classify_mongo_bulk_facts, mongo_bulk_failure_facts, MongoBulkDisposition,
    MongoBulkFailureFacts, MongoBulkFailureShape,
};
use failure::{classify_mongo_bulk_failure, MongoBulkFailure};
pub(super) use validation::harden_mongodb_tls;
#[cfg(test)]
use validation::is_namespace_exists_code;
use validation::{
    is_namespace_exists, is_supported_mongodb_arrow_type, validate_existing_timeseries_spec,
};

/// Writes Arrow record batches to a `MongoDB` collection.
///
/// Supports standard and time series collections in insert, upsert, and CDC replay modes.
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

#[cfg(test)]
mod tests;
