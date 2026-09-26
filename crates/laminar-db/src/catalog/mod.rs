//! Source and sink catalog for tracking registered streaming objects.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use parking_lot::RwLock;
use tokio::sync::Notify;

use laminar_core::streaming::{
    self, BackpressureStrategy, SourceConfig, StreamingError, WaitStrategy,
};

use crate::source_admission::{AdmittedInputCounter, OrderedInputOffset, SourceInstance};

pub(crate) fn schema_has_reserved_mutation_columns(schema: &Schema) -> bool {
    schema.fields().iter().any(|field| {
        ["_op", "__op", laminar_core::changelog::WEIGHT_COLUMN]
            .iter()
            .any(|reserved| field.name().eq_ignore_ascii_case(reserved))
    })
}

pub(crate) fn validate_source_batch(
    source_name: &str,
    expected_schema: &SchemaRef,
    primary_key: &[String],
    primary_key_indices: &[usize],
    batch: &RecordBatch,
) -> Result<(), StreamingError> {
    let actual_schema = batch.schema();
    if !Arc::ptr_eq(&actual_schema, expected_schema)
        && actual_schema.as_ref() != expected_schema.as_ref()
    {
        return Err(StreamingError::SchemaMismatch {
            expected: expected_schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
            actual: actual_schema
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect(),
        });
    }
    if primary_key.len() != primary_key_indices.len() {
        return Err(StreamingError::InvalidConfig(format!(
            "source '{source_name}' primary-key metadata is inconsistent"
        )));
    }
    for (column, &index) in primary_key.iter().zip(primary_key_indices) {
        let null_count = batch.column(index).null_count();
        if null_count != 0 {
            return Err(StreamingError::InvalidConfig(format!(
                "source '{source_name}' primary-key column '{column}' contains {null_count} null value(s)"
            )));
        }
    }
    Ok(())
}

/// Record type for Arrow-based streaming subscriptions.
#[derive(Clone, Debug)]
pub struct ArrowRecord {
    pub(crate) batch: RecordBatch,
}

impl laminar_core::streaming::Record for ArrowRecord {
    fn schema() -> SchemaRef {
        // This is a placeholder; the actual schema is on the SourceEntry.
        // Source intake uses push_arrow; the count-bounded query-output bridge uses Record.
        Arc::new(arrow::datatypes::Schema::empty())
    }

    fn to_record_batch(&self) -> RecordBatch {
        self.batch.clone()
    }
}

/// Recent snapshot history, independently bounded by count and retained Arrow bytes.
struct SnapshotRing {
    batches: VecDeque<(RecordBatch, usize)>,
    bytes: usize,
    capacity: usize,
    max_bytes: usize,
}

impl SnapshotRing {
    fn new(capacity: usize, max_bytes: usize) -> Self {
        Self {
            batches: VecDeque::new(),
            bytes: 0,
            capacity: capacity.max(1),
            max_bytes,
        }
    }

    fn push(&mut self, batch: RecordBatch) {
        let bytes = streaming::retained_arrow_bytes(&batch);
        // INVARIANT: successful source admission already checked this batch against the
        // same byte limit. Eviction changes history only after queue admission succeeds.
        debug_assert!(bytes <= self.max_bytes);
        while self.batches.len() >= self.capacity || self.bytes > self.max_bytes - bytes {
            if let Some((_, evicted_bytes)) = self.batches.pop_front() {
                self.bytes -= evicted_bytes;
            }
        }
        self.batches.push_back((batch, bytes));
        self.bytes += bytes;
    }

    fn snapshot(&self) -> Vec<RecordBatch> {
        self.batches
            .iter()
            .map(|(batch, _)| batch.clone())
            .collect()
    }
}

/// A registered source in the catalog.
pub struct SourceEntry {
    /// Source name.
    pub name: String,
    /// Arrow schema.
    pub schema: SchemaRef,
    /// Primary-key columns in declaration order.
    pub primary_key: Vec<String>,
    primary_key_indices: Vec<usize>,
    /// Watermark column name, if configured.
    pub watermark_column: Option<String>,
    /// Maximum out-of-orderness for watermark generation.
    pub max_out_of_orderness: Option<Duration>,
    /// Whether this source uses `PROCTIME()` watermarks.
    pub is_processing_time: std::sync::atomic::AtomicBool,
    pub(crate) source: streaming::Source<ArrowRecord>,
    pub(crate) sink: streaming::Sink<ArrowRecord>,
    buffer: parking_lot::Mutex<SnapshotRing>,
    /// Wakeup handle for `db.insert()` event-driven notification.
    data_notify: Arc<Notify>,
    /// Native-issued identity of this source instance (fresh after restart).
    source_instance: SourceInstance,
    /// Monotonic native admission ordinal for pushed batches.
    admitted_input_offset: AdmittedInputCounter,
    /// Whether this source is a managed push source owned by a downstream
    /// context layer. Only declared sources have their native instance/coordinate
    /// bound into checkpoint metadata, so connector sources are byte-identical.
    managed_push: std::sync::atomic::AtomicBool,
}

impl SourceEntry {
    /// Push a batch to both the channel and the snapshot ring.
    pub(crate) fn push_and_buffer(
        &self,
        batch: RecordBatch,
    ) -> Result<(), laminar_core::streaming::StreamingError> {
        self.admit_arrow(batch).map(|_| ())
    }

    /// Admit a batch and return its native ordered input offset.
    ///
    /// The ordinal is reserved only after the native channel accepted the batch,
    /// so a rejected or backpressured enqueue produces no offset and no receipt.
    pub(crate) fn admit_arrow(
        &self,
        batch: RecordBatch,
    ) -> Result<OrderedInputOffset, laminar_core::streaming::StreamingError> {
        validate_source_batch(
            &self.name,
            &self.schema,
            &self.primary_key,
            &self.primary_key_indices,
            &batch,
        )?;
        // Serialize admission and history publication so concurrent producers cannot leave
        // accepted batches waiting outside either owner or publish snapshots out of order.
        let mut buffer = self.buffer.lock();
        self.source.push_arrow(batch.clone())?;
        buffer.push(batch);
        drop(buffer);
        self.data_notify.notify_one();
        self.managed_push
            .store(true, std::sync::atomic::Ordering::Release);
        Ok(self.admitted_input_offset.next_offset())
    }

    /// Declare this source as a managed push source owned by a context layer.
    ///
    /// Until declared, checkpoint metadata is left byte-identical for connector
    /// sources. Declaring does not fabricate progress; it only opts the source
    /// into native instance/coordinate capture.
    pub(crate) fn declare_managed_push(&self) {
        self.managed_push
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Whether this source is a declared managed push source.
    pub(crate) fn is_managed_push(&self) -> bool {
        self.managed_push.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn snapshot(&self) -> Vec<RecordBatch> {
        self.buffer.lock().snapshot()
    }

    pub(crate) fn data_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.data_notify)
    }

    pub(crate) fn is_backpressured(&self) -> bool {
        crate::metrics::is_backpressured(self.source.pending(), self.source.capacity())
            || crate::metrics::is_backpressured(
                self.source.queued_arrow_bytes(),
                self.source.max_queued_bytes(),
            )
    }

    /// Native-issued identity of this source instance.
    pub(crate) fn source_instance(&self) -> &SourceInstance {
        &self.source_instance
    }

    /// Current committed native admission ordinal.
    pub(crate) fn committed_input_offset(&self) -> OrderedInputOffset {
        self.admitted_input_offset.current()
    }
}

pub(crate) struct SinkEntry {
    pub(crate) input: String,
}

pub(crate) struct QueryEntry {
    pub(crate) id: u64,
    pub(crate) sql: String,
    pub(crate) active: bool,
}

pub(crate) struct StreamEntry {
    pub(crate) name: String,
    emitted_rows: AtomicU64,
}

impl StreamEntry {
    pub(crate) fn record_emitted_rows(&self, rows: u64) {
        self.emitted_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub(crate) fn emitted_rows(&self) -> u64 {
        self.emitted_rows.load(Ordering::Relaxed)
    }
}

/// Central registry of sources, sinks, streams, and queries.
pub struct SourceCatalog {
    sources: RwLock<HashMap<String, Arc<SourceEntry>>>,
    sinks: RwLock<HashMap<String, SinkEntry>>,
    streams: RwLock<HashMap<String, Arc<StreamEntry>>>,
    queries: RwLock<HashMap<u64, QueryEntry>>,
    next_query_id: AtomicU64,
    /// Distinguishes process generations for native source instance identity.
    ///
    /// A restart is a new process with a new pid, so the same catalog name issues
    /// a fresh instance and cannot reuse a dead offset. If pid reuse ever becomes
    /// observable, extend this nonce from a supervised boot id instead.
    source_instance_nonce: uuid::Uuid,
    next_source_instance: AtomicU64,
    default_buffer_size: usize,
    default_backpressure: BackpressureStrategy,
    push_source_max_bytes: usize,
}

impl SourceCatalog {
    /// Create a catalog with the given defaults for new sources.
    #[must_use]
    pub fn new(buffer_size: usize, backpressure: BackpressureStrategy) -> Self {
        Self {
            sources: RwLock::new(HashMap::new()),
            sinks: RwLock::new(HashMap::new()),
            streams: RwLock::new(HashMap::new()),
            queries: RwLock::new(HashMap::new()),
            next_query_id: AtomicU64::new(1),
            source_instance_nonce: uuid::Uuid::new_v4(),
            next_source_instance: AtomicU64::new(1),
            default_buffer_size: buffer_size,
            default_backpressure: backpressure,
            push_source_max_bytes: streaming::DEFAULT_SOURCE_MAX_QUEUED_BYTES,
        }
    }

    pub(crate) fn from_config(config: &crate::LaminarConfig) -> Self {
        Self {
            push_source_max_bytes: config.push_source_max_bytes,
            ..Self::new(config.default_buffer_size, config.default_backpressure)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn register_source(
        &self,
        name: &str,
        schema: SchemaRef,
        primary_key: Vec<String>,
        watermark_column: Option<String>,
        max_out_of_orderness: Option<Duration>,
        buffer_size: Option<usize>,
        backpressure: Option<BackpressureStrategy>,
    ) -> Result<Arc<SourceEntry>, crate::DbError> {
        let mut sources = self.sources.write();
        if sources.contains_key(name) {
            return Err(crate::DbError::SourceAlreadyExists(name.to_string()));
        }

        let mut primary_key_indices = Vec::with_capacity(primary_key.len());
        for column in &primary_key {
            let index = schema.index_of(column).map_err(|_| {
                crate::DbError::InvalidOperation(format!(
                    "source '{name}' primary-key column '{column}' is absent from its schema"
                ))
            })?;
            if primary_key_indices.contains(&index) {
                return Err(crate::DbError::InvalidOperation(format!(
                    "source '{name}' primary key repeats column '{column}'"
                )));
            }
            if schema.field(index).is_nullable() {
                return Err(crate::DbError::InvalidOperation(format!(
                    "source '{name}' primary-key column '{column}' must be non-nullable"
                )));
            }
            primary_key_indices.push(index);
        }

        let buf_size = buffer_size.unwrap_or(self.default_buffer_size);
        let bp = backpressure.unwrap_or(self.default_backpressure);

        // Channel buffer is at least 1024 to avoid blocking on small snapshot rings.
        let channel_buf = buf_size.max(1024);
        let config = SourceConfig {
            channel: streaming::ChannelConfig {
                buffer_size: channel_buf,
                backpressure: bp,
                wait_strategy: WaitStrategy::SpinYield,
                track_stats: false,
            },
            name: Some(name.to_string()),
            max_queued_bytes: self.push_source_max_bytes,
        };

        let (source, sink) = streaming::create_with_config::<ArrowRecord>(config);

        let ordinal = self.next_source_instance.fetch_add(1, Ordering::Relaxed);
        let source_instance = SourceInstance::issue(name, self.source_instance_nonce, ordinal);

        let entry = Arc::new(SourceEntry {
            name: name.to_string(),
            schema,
            primary_key,
            primary_key_indices,
            watermark_column,
            max_out_of_orderness,
            is_processing_time: std::sync::atomic::AtomicBool::new(false),
            source,
            sink,
            buffer: parking_lot::Mutex::new(SnapshotRing::new(
                buf_size,
                self.push_source_max_bytes,
            )),
            data_notify: Arc::new(Notify::new()),
            source_instance,
            admitted_input_offset: AdmittedInputCounter::default(),
            managed_push: std::sync::atomic::AtomicBool::new(false),
        });

        sources.insert(name.to_string(), Arc::clone(&entry));
        Ok(entry)
    }

    #[cfg(test)]
    pub(crate) fn register_source_or_replace(
        &self,
        name: &str,
        schema: SchemaRef,
        primary_key: Vec<String>,
        watermark_column: Option<String>,
        max_out_of_orderness: Option<Duration>,
        buffer_size: Option<usize>,
        backpressure: Option<BackpressureStrategy>,
    ) -> Arc<SourceEntry> {
        // Remove existing if present
        self.sources.write().remove(name);
        // Safe to unwrap since we just removed any conflict
        self.register_source(
            name,
            schema,
            primary_key,
            watermark_column,
            max_out_of_orderness,
            buffer_size,
            backpressure,
        )
        .unwrap()
    }

    /// Look up a registered source by name.
    pub fn get_source(&self, name: &str) -> Option<Arc<SourceEntry>> {
        self.sources.read().get(name).cloned()
    }

    /// Returns `true` if the source existed.
    pub fn drop_source(&self, name: &str) -> bool {
        self.sources.write().remove(name).is_some()
    }

    pub(crate) fn register_sink(&self, name: &str, input: &str) -> Result<(), crate::DbError> {
        let mut sinks = self.sinks.write();
        if sinks.contains_key(name) {
            return Err(crate::DbError::SinkAlreadyExists(name.to_string()));
        }
        sinks.insert(
            name.to_string(),
            SinkEntry {
                input: input.to_string(),
            },
        );
        Ok(())
    }

    /// Returns `true` if the sink existed.
    pub fn drop_sink(&self, name: &str) -> bool {
        self.sinks.write().remove(name).is_some()
    }

    pub(crate) fn register_stream(&self, name: &str) -> Result<(), crate::DbError> {
        let mut streams = self.streams.write();
        if streams.contains_key(name) {
            return Err(crate::DbError::StreamAlreadyExists(name.to_string()));
        }

        streams.insert(
            name.to_string(),
            Arc::new(StreamEntry {
                name: name.to_string(),
                emitted_rows: AtomicU64::new(0),
            }),
        );
        Ok(())
    }

    pub(crate) fn get_stream_entry(&self, name: &str) -> Option<Arc<StreamEntry>> {
        self.streams.read().get(name).cloned()
    }

    /// Returns `true` if the stream existed.
    pub fn drop_stream(&self, name: &str) -> bool {
        self.streams.write().remove(name).is_some()
    }

    /// All registered stream names.
    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    /// All registered source names.
    pub fn list_sources(&self) -> Vec<String> {
        self.sources.read().keys().cloned().collect()
    }

    /// All registered sink names.
    pub fn list_sinks(&self) -> Vec<String> {
        self.sinks.read().keys().cloned().collect()
    }

    /// Input source/table name for a sink, if registered.
    pub fn get_sink_input(&self, name: &str) -> Option<String> {
        self.sinks.read().get(name).map(|e| e.input.clone())
    }

    pub(crate) fn register_query(&self, sql: &str) -> u64 {
        let id = self.next_query_id.fetch_add(1, Ordering::Relaxed);
        let mut queries = self.queries.write();
        queries.insert(
            id,
            QueryEntry {
                id,
                sql: sql.to_string(),
                active: true,
            },
        );
        id
    }

    pub(crate) fn deactivate_query(&self, id: u64) -> bool {
        // Cap retained deactivated queries so finished SELECTs can't accumulate
        // unboundedly; over the cap, the oldest (lowest id) is dropped.
        const MAX_INACTIVE_QUERIES: usize = 100;
        let mut queries = self.queries.write();
        if let Some(entry) = queries.get_mut(&id) {
            let was_active = entry.active;
            entry.active = false;
            if was_active {
                let inactive_count = queries.values().filter(|q| !q.active).count();
                if inactive_count > MAX_INACTIVE_QUERIES {
                    let oldest_inactive_id =
                        queries.values().filter(|q| !q.active).map(|q| q.id).min();
                    if let Some(oldest_id) = oldest_inactive_id {
                        queries.remove(&oldest_id);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub(crate) fn list_queries(&self) -> Vec<(u64, String, bool)> {
        self.queries
            .read()
            .values()
            .map(|q| (q.id, q.sql.clone(), q.active))
            .collect()
    }

    /// Schema for DESCRIBE queries.
    pub fn describe_source(&self, name: &str) -> Option<SchemaRef> {
        self.sources.read().get(name).map(|e| e.schema.clone())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_tests;
