//! # Lookup Join Operators
//!
//! Join streaming events with external reference tables (dimension tables).
//!
//! Lookup joins enrich streaming data with information from slowly-changing
//! external tables. Unlike stream-stream joins (F019), lookup joins:
//! - Have one streaming side (events) and one static/slowly-changing side (table)
//! - Use caching to avoid repeated lookups for the same key
//! - Support TTL-based cache expiration for slowly-changing data
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   LookupJoinOperator                        │
//! │                                                             │
//! │  ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
//! │  │ Event   │───▶│ Extract │───▶│ Cache   │──┐              │
//! │  │ Stream  │    │   Key   │    │ Lookup  │  │ hit          │
//! │  └─────────┘    └─────────┘    └────┬────┘  │              │
//! │                                      │miss  ▼              │
//! │                              ┌───────┴──────────┐          │
//! │                              │  Table Loader    │          │
//! │                              │  (async lookup)  │          │
//! │                              └───────┬──────────┘          │
//! │                                      │                     │
//! │                                      ▼                     │
//! │                              ┌───────────────┐             │
//! │                              │ Update Cache  │             │
//! │                              └───────┬───────┘             │
//! │                                      │                     │
//! │                                      ▼                     │
//! │  ┌──────────────────────────────────────────────────────┐ │
//! │  │                    Join & Emit                        │ │
//! │  │  - Inner: only emit if lookup found                   │ │
//! │  │  - Left: emit all events (null for missing lookups)   │ │
//! │  └──────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Example
//!
//! ```rust,no_run
//! use laminar_core::operator::lookup_join::{
//!     LookupJoinOperator, LookupJoinConfig, LookupJoinType,
//! };
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # struct MyTableLoader;
//! # impl MyTableLoader { fn new() -> Self { Self } }
//!
//! // Create a lookup join that enriches orders with customer data
//! let config = LookupJoinConfig::builder()
//!     .stream_key_column("customer_id".to_string())
//!     .lookup_key_column("id".to_string())
//!     .cache_ttl(Duration::from_secs(300))  // 5 minute cache
//!     .join_type(LookupJoinType::Left)
//!     .build()
//!     .unwrap();
//!
//! // let operator = LookupJoinOperator::new(config, Arc::new(loader));
//! ```
//!
//! ## SQL Syntax
//!
//! ```sql
//! SELECT o.*, c.name, c.tier
//! FROM orders o
//! JOIN customers c  -- Reference table
//!     ON o.customer_id = c.id;
//! ```
//!
//! ## Cache Management
//!
//! The operator maintains an internal cache in the state store:
//! - Cache entries are keyed by `lkc:<key_hash>`
//! - Each entry stores the lookup result and insertion timestamp
//! - TTL-based expiration using processing time (not event time)
//! - Timer-based cleanup of expired entries
//!
//! ## Performance Considerations
//!
//! - First lookup for a key incurs async table loader latency
//! - Subsequent lookups within TTL are sub-microsecond (state store)
//! - Cache size is bounded by unique keys in the stream
//! - For high-cardinality keys, consider shorter TTL to limit memory

use super::{
    Event, Operator, OperatorContext, OperatorError, OperatorState, Output, OutputVec, Timer,
    TimerKey,
};
use crate::state::StateStoreExt;
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use fxhash::FxHashMap;
use rkyv::{
    rancor::Error as RkyvError, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Type of lookup join to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LookupJoinType {
    /// Inner join - only emit events where lookup succeeds.
    #[default]
    Inner,
    /// Left outer join - emit all events, with nulls for failed lookups.
    Left,
}

impl LookupJoinType {
    /// Returns true if failed lookups should still emit output.
    #[must_use]
    pub fn emits_on_miss(&self) -> bool {
        matches!(self, LookupJoinType::Left)
    }
}

/// Configuration for a lookup join operator.
#[derive(Debug, Clone)]
pub struct LookupJoinConfig {
    /// Column name in the stream to use as lookup key.
    pub stream_key_column: String,
    /// Column name in the lookup table that matches the stream key.
    pub lookup_key_column: String,
    /// Time-to-live for cached lookup results.
    pub cache_ttl: Duration,
    /// Type of join to perform.
    pub join_type: LookupJoinType,
    /// Maximum number of entries to cache (0 = unlimited).
    pub max_cache_size: usize,
    /// Operator ID for checkpointing.
    pub operator_id: Option<String>,
}

impl LookupJoinConfig {
    /// Creates a new builder for lookup join configuration.
    #[must_use]
    pub fn builder() -> LookupJoinConfigBuilder {
        LookupJoinConfigBuilder::default()
    }
}

/// Builder for [`LookupJoinConfig`].
#[derive(Debug, Default)]
pub struct LookupJoinConfigBuilder {
    stream_key_column: Option<String>,
    lookup_key_column: Option<String>,
    cache_ttl: Option<Duration>,
    join_type: Option<LookupJoinType>,
    max_cache_size: Option<usize>,
    operator_id: Option<String>,
}

impl LookupJoinConfigBuilder {
    /// Sets the stream key column name.
    #[must_use]
    pub fn stream_key_column(mut self, column: String) -> Self {
        self.stream_key_column = Some(column);
        self
    }

    /// Sets the lookup key column name.
    #[must_use]
    pub fn lookup_key_column(mut self, column: String) -> Self {
        self.lookup_key_column = Some(column);
        self
    }

    /// Sets the cache TTL.
    #[must_use]
    pub fn cache_ttl(mut self, ttl: Duration) -> Self {
        self.cache_ttl = Some(ttl);
        self
    }

    /// Sets the join type.
    #[must_use]
    pub fn join_type(mut self, join_type: LookupJoinType) -> Self {
        self.join_type = Some(join_type);
        self
    }

    /// Sets the maximum cache size (0 = unlimited).
    #[must_use]
    pub fn max_cache_size(mut self, size: usize) -> Self {
        self.max_cache_size = Some(size);
        self
    }

    /// Sets a custom operator ID.
    #[must_use]
    pub fn operator_id(mut self, id: String) -> Self {
        self.operator_id = Some(id);
        self
    }

    /// Builds the configuration.
    ///
    /// # Errors
    ///
    /// Returns `OperatorError::ConfigError` if required fields
    /// (`stream_key_column`, `lookup_key_column`) are not set.
    pub fn build(self) -> Result<LookupJoinConfig, OperatorError> {
        Ok(LookupJoinConfig {
            stream_key_column: self.stream_key_column.ok_or_else(|| {
                OperatorError::ConfigError("stream_key_column is required".into())
            })?,
            lookup_key_column: self.lookup_key_column.ok_or_else(|| {
                OperatorError::ConfigError("lookup_key_column is required".into())
            })?,
            cache_ttl: self.cache_ttl.unwrap_or(Duration::from_secs(300)),
            join_type: self.join_type.unwrap_or_default(),
            max_cache_size: self.max_cache_size.unwrap_or(0),
            operator_id: self.operator_id,
        })
    }
}

/// State key prefix for cache entries.
const CACHE_STATE_PREFIX: &[u8; 4] = b"lkc:";

/// Timer key prefix for cache TTL expiration.
const CACHE_TIMER_PREFIX: u8 = 0x40;

/// Static counter for generating unique operator IDs.
static LOOKUP_OPERATOR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A cached lookup entry stored in state.
#[derive(Debug, Clone, Archive, RkyvSerialize, RkyvDeserialize)]
pub struct CacheEntry {
    /// When this entry was inserted (processing time in microseconds).
    pub inserted_at: i64,
    /// Whether the lookup found a result.
    pub found: bool,
    /// Serialized lookup result (Arrow IPC format).
    pub data: Vec<u8>,
}

impl CacheEntry {
    /// Creates a new cache entry with a found result.
    fn found(inserted_at: i64, batch: &RecordBatch) -> Result<Self, OperatorError> {
        let data = Self::serialize_batch(batch)?;
        Ok(Self {
            inserted_at,
            found: true,
            data,
        })
    }

    /// Creates a new cache entry for a not-found result.
    fn not_found(inserted_at: i64) -> Self {
        Self {
            inserted_at,
            found: false,
            data: Vec::new(),
        }
    }

    /// Checks if this entry has expired.
    fn is_expired(&self, now: i64, ttl_us: i64) -> bool {
        now - self.inserted_at > ttl_us
    }

    /// Serializes a record batch to bytes.
    fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>, OperatorError> {
        let mut buf = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        }
        Ok(buf)
    }

    /// Deserializes a record batch from bytes.
    fn deserialize_batch(data: &[u8]) -> Result<RecordBatch, OperatorError> {
        let cursor = std::io::Cursor::new(data);
        let mut reader = arrow_ipc::reader::StreamReader::try_new(cursor, None)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        reader
            .next()
            .ok_or_else(|| OperatorError::SerializationFailed("Empty batch data".to_string()))?
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))
    }

    /// Returns the cached batch if found.
    ///
    /// # Errors
    ///
    /// Returns `OperatorError::SerializationFailed` if the batch data is invalid.
    pub fn to_batch(&self) -> Result<Option<RecordBatch>, OperatorError> {
        if self.found {
            Ok(Some(Self::deserialize_batch(&self.data)?))
        } else {
            Ok(None)
        }
    }
}

/// Metrics for tracking lookup join operations.
#[derive(Debug, Clone, Default)]
pub struct LookupJoinMetrics {
    /// Number of events processed.
    pub events_processed: u64,
    /// Number of cache hits.
    pub cache_hits: u64,
    /// Number of cache misses (required lookup).
    pub cache_misses: u64,
    /// Number of successful lookups.
    pub lookups_found: u64,
    /// Number of lookups that returned not found.
    pub lookups_not_found: u64,
    /// Number of lookup errors.
    pub lookup_errors: u64,
    /// Number of joined events emitted.
    pub events_emitted: u64,
    /// Number of events dropped (inner join with no match).
    pub events_dropped: u64,
    /// Number of cache entries expired.
    pub cache_expirations: u64,
}

impl LookupJoinMetrics {
    /// Creates new metrics.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets all counters.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Result of a synchronous lookup operation.
///
/// Since the `Operator` trait is synchronous but `TableLoader` is async,
/// the lookup join must handle lookups differently. This type represents
/// lookups that can be resolved from cache.
#[derive(Debug, Clone)]
pub enum SyncLookupResult {
    /// Cache hit - result is available.
    CacheHit(Option<RecordBatch>),
    /// Cache miss - lookup is pending (must be provided externally).
    CacheMiss,
}

/// Lookup join operator that enriches streaming events with external table data.
///
/// This operator caches lookup results in the state store to minimize
/// external lookups. The cache uses processing-time-based TTL for expiration.
///
/// # Async Lookup Handling
///
/// Since the [`Operator`] trait is synchronous, async table lookups must be
/// handled externally. Use [`LookupJoinOperator::pending_lookups`] to get
/// keys that need to be looked up, and [`LookupJoinOperator::provide_lookup`]
/// to provide the results.
///
/// For simple synchronous use cases, use `LookupJoinOperator::process_with_lookup`
/// which accepts a closure for lookups.
pub struct LookupJoinOperator {
    /// Configuration.
    config: LookupJoinConfig,
    /// Operator ID.
    operator_id: String,
    /// Metrics.
    metrics: LookupJoinMetrics,
    /// Output schema (lazily initialized).
    output_schema: Option<SchemaRef>,
    /// Stream schema (captured from first event).
    stream_schema: Option<SchemaRef>,
    /// Lookup table schema (captured from first lookup result).
    lookup_schema: Option<SchemaRef>,
    /// Cache TTL in microseconds.
    cache_ttl_us: i64,
    /// Keys pending external lookup.
    pending_keys: Vec<Vec<u8>>,
    /// Events waiting for pending lookups.
    pending_events: Vec<(Event, Vec<u8>)>,
    /// In-memory cache of deserialized batches to avoid repeated Arrow IPC deserialization.
    /// Maps cache key bytes to the deserialized `RecordBatch` wrapped in `Arc` for cheap cloning.
    batch_cache: FxHashMap<Vec<u8>, Option<Arc<RecordBatch>>>,
}

impl LookupJoinOperator {
    /// Creates a new lookup join operator.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Duration.as_micros() fits i64 for practical values
    pub fn new(config: LookupJoinConfig) -> Self {
        let operator_id = config.operator_id.clone().unwrap_or_else(|| {
            let num = LOOKUP_OPERATOR_COUNTER.fetch_add(1, Ordering::Relaxed);
            format!("lookup_join_{num}")
        });

        let cache_ttl_us = config.cache_ttl.as_micros() as i64;

        Self {
            config,
            operator_id,
            metrics: LookupJoinMetrics::new(),
            output_schema: None,
            stream_schema: None,
            lookup_schema: None,
            cache_ttl_us,
            pending_keys: Vec::new(),
            pending_events: Vec::new(),
            batch_cache: FxHashMap::default(),
        }
    }

    /// Creates a new lookup join operator with explicit ID.
    #[must_use]
    pub fn with_id(mut config: LookupJoinConfig, operator_id: String) -> Self {
        config.operator_id = Some(operator_id);
        Self::new(config)
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &LookupJoinConfig {
        &self.config
    }

    /// Returns the metrics.
    #[must_use]
    pub fn metrics(&self) -> &LookupJoinMetrics {
        &self.metrics
    }

    /// Resets the metrics.
    pub fn reset_metrics(&mut self) {
        self.metrics.reset();
    }

    /// Returns keys that need external lookup.
    ///
    /// After processing events, check this for keys that weren't in the cache.
    /// Provide lookup results via [`provide_lookup`](Self::provide_lookup).
    #[must_use]
    pub fn pending_lookups(&self) -> &[Vec<u8>] {
        &self.pending_keys
    }

    /// Provides a lookup result for a pending key.
    ///
    /// Call this after performing the async lookup to complete processing
    /// of pending events.
    ///
    /// # Returns
    ///
    /// Output events that can now be emitted after the lookup result is available.
    pub fn provide_lookup(
        &mut self,
        key: &[u8],
        result: Option<&RecordBatch>,
        ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        // Update cache
        let cache_key = Self::make_cache_key(key);
        let entry = if let Some(batch) = result {
            // Capture lookup schema on first result
            if self.lookup_schema.is_none() {
                self.lookup_schema = Some(batch.schema());
                self.update_output_schema();
            }
            self.metrics.lookups_found += 1;
            let Ok(e) = CacheEntry::found(ctx.processing_time, batch) else {
                self.metrics.lookup_errors += 1;
                return output;
            };
            e
        } else {
            self.metrics.lookups_not_found += 1;
            CacheEntry::not_found(ctx.processing_time)
        };

        // Store in cache
        if ctx.state.put_typed(&cache_key, &entry).is_err() {
            return output;
        }

        // Populate in-memory batch cache
        self.batch_cache
            .insert(cache_key.clone(), result.map(|b| Arc::new(b.clone())));

        // Register TTL timer
        let expiry_time = ctx.processing_time + self.cache_ttl_us;
        let timer_key = Self::make_timer_key(&cache_key);
        ctx.timers
            .register_timer(expiry_time, Some(timer_key), Some(ctx.operator_index));

        // Process pending events for this key
        let events_to_process: Vec<_> = self
            .pending_events
            .iter()
            .filter(|(_, k)| k == key)
            .map(|(e, _)| e.clone())
            .collect();

        self.pending_events.retain(|(_, k)| k != key);
        self.pending_keys.retain(|k| k != key);

        for event in events_to_process {
            if let Some(joined) = self.create_joined_event(&event, result) {
                self.metrics.events_emitted += 1;
                output.push(Output::Event(joined));
            } else if self.config.join_type.emits_on_miss() {
                // Left join: emit with nulls
                if let Some(joined) = self.create_unmatched_event(&event) {
                    self.metrics.events_emitted += 1;
                    output.push(Output::Event(joined));
                }
            } else {
                self.metrics.events_dropped += 1;
            }
        }

        output
    }

    /// Processes an event with a synchronous lookup function.
    ///
    /// This is a convenience method for cases where lookups can be done
    /// synchronously (e.g., from an in-memory table).
    pub fn process_with_lookup<F>(
        &mut self,
        event: &Event,
        ctx: &mut OperatorContext,
        lookup_fn: F,
    ) -> OutputVec
    where
        F: FnOnce(&[u8]) -> Option<RecordBatch>,
    {
        self.metrics.events_processed += 1;

        // Capture stream schema
        if self.stream_schema.is_none() {
            self.stream_schema = Some(event.data.schema());
        }

        // Extract join key
        let Some(key) = Self::extract_key(&event.data, &self.config.stream_key_column) else {
            return OutputVec::new();
        };

        // Check cache first
        let cache_key = Self::make_cache_key(&key);
        if let Some(cached) = self.lookup_cache(&cache_key, ctx) {
            self.metrics.cache_hits += 1;
            return self.emit_result(event, cached.as_ref().map(AsRef::as_ref), ctx);
        }

        // Cache miss - perform lookup
        self.metrics.cache_misses += 1;
        let result = lookup_fn(&key);

        // Update cache
        let entry = if let Some(ref batch) = result {
            if self.lookup_schema.is_none() {
                self.lookup_schema = Some(batch.schema());
                self.update_output_schema();
            }
            self.metrics.lookups_found += 1;
            let Ok(e) = CacheEntry::found(ctx.processing_time, batch) else {
                self.metrics.lookup_errors += 1;
                return OutputVec::new();
            };
            e
        } else {
            self.metrics.lookups_not_found += 1;
            CacheEntry::not_found(ctx.processing_time)
        };

        if ctx.state.put_typed(&cache_key, &entry).is_ok() {
            // Populate in-memory batch cache (Arc wrap for cheap cache-hit cloning)
            self.batch_cache.insert(
                cache_key.clone(),
                result.as_ref().map(|b| Arc::new(b.clone())),
            );

            // Register TTL timer
            let expiry_time = ctx.processing_time + self.cache_ttl_us;
            let timer_key = Self::make_timer_key(&cache_key);
            ctx.timers
                .register_timer(expiry_time, Some(timer_key), Some(ctx.operator_index));
        }

        self.emit_result(event, result.as_ref(), ctx)
    }

    /// Looks up a key in the cache.
    ///
    /// Returns:
    /// - `None` if cache miss (key not found or expired)
    /// - `Some(None)` if cached "not found" result
    /// - `Some(Some(batch))` if cached found result
    ///
    /// Uses an in-memory batch cache to avoid repeated Arrow IPC deserialization
    /// when the same cache key is looked up multiple times.
    #[allow(clippy::option_option)]
    fn lookup_cache(
        &mut self,
        cache_key: &[u8],
        ctx: &OperatorContext,
    ) -> Option<Option<Arc<RecordBatch>>> {
        let entry: CacheEntry = ctx.state.get_typed(cache_key).ok()??;

        // Check if expired
        if entry.is_expired(ctx.processing_time, self.cache_ttl_us) {
            self.batch_cache.remove(cache_key);
            return None;
        }

        // Check in-memory batch cache first — Arc::clone is a cheap ref count bump
        if let Some(cached) = self.batch_cache.get(cache_key) {
            return Some(cached.clone());
        }

        // Deserialize and cache the batch (one-time Arc wrap)
        let result = entry.to_batch().ok()?;
        let arc_result = result.map(Arc::new);
        self.batch_cache
            .insert(cache_key.to_vec(), arc_result.clone());
        Some(arc_result)
    }

    /// Emits the join result for an event.
    fn emit_result(
        &mut self,
        event: &Event,
        lookup_result: Option<&RecordBatch>,
        _ctx: &mut OperatorContext,
    ) -> OutputVec {
        let mut output = OutputVec::new();

        if let Some(lookup_batch) = lookup_result {
            // Found - create joined output
            if let Some(joined) = self.create_joined_event(event, Some(lookup_batch)) {
                self.metrics.events_emitted += 1;
                output.push(Output::Event(joined));
            }
        } else if self.config.join_type.emits_on_miss() {
            // Left join - emit with nulls
            if let Some(joined) = self.create_unmatched_event(event) {
                self.metrics.events_emitted += 1;
                output.push(Output::Event(joined));
            }
        } else {
            // Inner join - drop event
            self.metrics.events_dropped += 1;
        }

        output
    }

    /// Extracts the join key value from a record batch.
    fn extract_key(batch: &RecordBatch, column_name: &str) -> Option<Vec<u8>> {
        let column_index = batch.schema().index_of(column_name).ok()?;
        let column = batch.column(column_index);

        // Handle different column types
        if let Some(string_array) = column.as_any().downcast_ref::<StringArray>() {
            if string_array.is_empty() || string_array.is_null(0) {
                return None;
            }
            return Some(string_array.value(0).as_bytes().to_vec());
        }

        if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            if int_array.is_empty() || int_array.is_null(0) {
                return None;
            }
            return Some(int_array.value(0).to_le_bytes().to_vec());
        }

        None
    }

    /// Creates a cache key from a lookup key.
    fn make_cache_key(key: &[u8]) -> Vec<u8> {
        let key_hash = fxhash::hash64(key);
        let mut cache_key = Vec::with_capacity(12);
        cache_key.extend_from_slice(CACHE_STATE_PREFIX);
        cache_key.extend_from_slice(&key_hash.to_be_bytes());
        cache_key
    }

    /// Creates a timer key for cache TTL expiration.
    fn make_timer_key(cache_key: &[u8]) -> TimerKey {
        let mut key = TimerKey::new();
        key.push(CACHE_TIMER_PREFIX);
        key.extend_from_slice(cache_key);
        key
    }

    /// Parses a timer key to extract the cache key.
    fn parse_timer_key(key: &[u8]) -> Option<Vec<u8>> {
        if key.is_empty() || key[0] != CACHE_TIMER_PREFIX {
            return None;
        }
        Some(key[1..].to_vec())
    }

    /// Updates the output schema when both input schemas are known.
    fn update_output_schema(&mut self) {
        if let (Some(stream), Some(lookup)) = (&self.stream_schema, &self.lookup_schema) {
            let mut fields: Vec<Field> =
                Vec::with_capacity(stream.fields().len() + lookup.fields().len());
            fields.extend(stream.fields().iter().map(|f| f.as_ref().clone()));

            // Add lookup fields, prefixing duplicates
            for field in lookup.fields() {
                let name = if stream.field_with_name(field.name()).is_ok() {
                    format!("lookup_{}", field.name())
                } else {
                    field.name().clone()
                };
                fields.push(Field::new(
                    name,
                    field.data_type().clone(),
                    true, // Nullable for left joins
                ));
            }

            self.output_schema = Some(Arc::new(Schema::new(fields)));
        }
    }

    /// Creates a joined event from stream event and lookup result.
    fn create_joined_event(&self, event: &Event, lookup: Option<&RecordBatch>) -> Option<Event> {
        let lookup_batch = lookup?;
        let schema = self.output_schema.as_ref()?;

        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(event.data.num_columns() + lookup_batch.num_columns());
        columns.extend(event.data.columns().iter().cloned());
        for column in lookup_batch.columns() {
            columns.push(Arc::clone(column));
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event::new(event.timestamp, joined_batch))
    }

    /// Creates an unmatched event for left joins (with null lookup columns).
    fn create_unmatched_event(&self, event: &Event) -> Option<Event> {
        let schema = self.output_schema.as_ref()?;
        let lookup_schema = self.lookup_schema.as_ref()?;

        let num_rows = event.data.num_rows();
        let mut columns: Vec<ArrayRef> =
            Vec::with_capacity(event.data.num_columns() + lookup_schema.fields().len());
        columns.extend(event.data.columns().iter().cloned());

        // Add null columns for lookup side
        for field in lookup_schema.fields() {
            columns.push(Self::create_null_array(field.data_type(), num_rows));
        }

        let joined_batch = RecordBatch::try_new(Arc::clone(schema), columns).ok()?;

        Some(Event::new(event.timestamp, joined_batch))
    }

    /// Creates a null array of the given type and length.
    fn create_null_array(data_type: &DataType, num_rows: usize) -> ArrayRef {
        match data_type {
            DataType::Utf8 => Arc::new(StringArray::from(vec![None::<&str>; num_rows])) as ArrayRef,
            // Default to Int64 for unknown types
            _ => Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef,
        }
    }

    /// Handles cache TTL timer expiration.
    fn handle_cache_expiry(&mut self, cache_key: &[u8], ctx: &mut OperatorContext) -> OutputVec {
        if ctx.state.delete(cache_key).is_ok() {
            self.batch_cache.remove(cache_key);
            self.metrics.cache_expirations += 1;
        }
        OutputVec::new()
    }
}

impl Operator for LookupJoinOperator {
    fn process(&mut self, event: &Event, ctx: &mut OperatorContext) -> OutputVec {
        self.metrics.events_processed += 1;

        // Capture stream schema
        if self.stream_schema.is_none() {
            self.stream_schema = Some(event.data.schema());
        }

        // Extract join key
        let Some(key) = Self::extract_key(&event.data, &self.config.stream_key_column) else {
            return OutputVec::new();
        };

        // Check cache
        let cache_key = Self::make_cache_key(&key);
        if let Some(cached) = self.lookup_cache(&cache_key, ctx) {
            self.metrics.cache_hits += 1;
            return self.emit_result(event, cached.as_ref().map(AsRef::as_ref), ctx);
        }

        // Cache miss - add to pending
        self.metrics.cache_misses += 1;
        if !self.pending_keys.contains(&key) {
            self.pending_keys.push(key.clone());
        }
        self.pending_events.push((event.clone(), key));

        OutputVec::new()
    }

    fn on_timer(&mut self, timer: Timer, ctx: &mut OperatorContext) -> OutputVec {
        if let Some(cache_key) = Self::parse_timer_key(&timer.key) {
            return self.handle_cache_expiry(&cache_key, ctx);
        }
        OutputVec::new()
    }

    fn checkpoint(&self) -> OperatorState {
        let checkpoint_data = (
            self.config.stream_key_column.clone(),
            self.config.lookup_key_column.clone(),
            self.metrics.events_processed,
            self.metrics.cache_hits,
            self.metrics.cache_misses,
            self.metrics.lookups_found,
            self.metrics.lookups_not_found,
        );

        let data = rkyv::to_bytes::<RkyvError>(&checkpoint_data)
            .map(|v| v.to_vec())
            .unwrap_or_default();

        OperatorState {
            operator_id: self.operator_id.clone(),
            data,
        }
    }

    fn restore(&mut self, state: OperatorState) -> Result<(), OperatorError> {
        type CheckpointData = (String, String, u64, u64, u64, u64, u64);

        if state.operator_id != self.operator_id {
            return Err(OperatorError::StateAccessFailed(format!(
                "Operator ID mismatch: expected {}, got {}",
                self.operator_id, state.operator_id
            )));
        }

        let archived = rkyv::access::<rkyv::Archived<CheckpointData>, RkyvError>(&state.data)
            .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;
        let (_, _, events_processed, cache_hits, cache_misses, lookups_found, lookups_not_found) =
            rkyv::deserialize::<CheckpointData, RkyvError>(archived)
                .map_err(|e| OperatorError::SerializationFailed(e.to_string()))?;

        self.metrics.events_processed = events_processed;
        self.metrics.cache_hits = cache_hits;
        self.metrics.cache_misses = cache_misses;
        self.metrics.lookups_found = lookups_found;
        self.metrics.lookups_not_found = lookups_not_found;
        self.batch_cache.clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{InMemoryStore, StateStore};
    use crate::time::{BoundedOutOfOrdernessGenerator, TimerService, WatermarkGenerator};
    use arrow_schema::{DataType, Field, Schema};
    use std::collections::HashMap;

    fn create_order_event(timestamp: i64, customer_id: &str, amount: i64) -> Event {
        let schema = Arc::new(Schema::new(vec![
            Field::new("customer_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![customer_id])),
                Arc::new(Int64Array::from(vec![amount])),
            ],
        )
        .unwrap();
        Event::new(timestamp, batch)
    }

    fn create_customer_batch(id: &str, name: &str, tier: &str) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("tier", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
                Arc::new(StringArray::from(vec![tier])),
            ],
        )
        .unwrap()
    }

    fn create_test_context<'a>(
        timers: &'a mut TimerService,
        state: &'a mut dyn StateStore,
        watermark_gen: &'a mut dyn WatermarkGenerator,
    ) -> OperatorContext<'a> {
        OperatorContext {
            event_time: 0,
            processing_time: 1_000_000, // 1 second in microseconds
            timers,
            state,
            watermark_generator: watermark_gen,
            operator_index: 0,
        }
    }

    fn create_lookup_table() -> HashMap<Vec<u8>, RecordBatch> {
        let mut table = HashMap::new();
        table.insert(
            b"cust_1".to_vec(),
            create_customer_batch("cust_1", "Alice", "gold"),
        );
        table.insert(
            b"cust_2".to_vec(),
            create_customer_batch("cust_2", "Bob", "silver"),
        );
        table.insert(
            b"cust_3".to_vec(),
            create_customer_batch("cust_3", "Charlie", "bronze"),
        );
        table
    }

    #[test]
    fn test_lookup_join_type_properties() {
        assert!(!LookupJoinType::Inner.emits_on_miss());
        assert!(LookupJoinType::Left.emits_on_miss());
    }

    #[test]
    fn test_config_builder() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .cache_ttl(Duration::from_secs(60))
            .join_type(LookupJoinType::Left)
            .max_cache_size(1000)
            .operator_id("test_op".to_string())
            .build()
            .unwrap();

        assert_eq!(config.stream_key_column, "customer_id");
        assert_eq!(config.lookup_key_column, "id");
        assert_eq!(config.cache_ttl, Duration::from_secs(60));
        assert_eq!(config.join_type, LookupJoinType::Left);
        assert_eq!(config.max_cache_size, 1000);
        assert_eq!(config.operator_id, Some("test_op".to_string()));
    }

    #[test]
    fn test_inner_join_basic() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .cache_ttl(Duration::from_secs(300))
            .join_type(LookupJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());
        let lookup_table = create_lookup_table();

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process order for existing customer
        let order = create_order_event(1000, "cust_1", 100);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);

        let outputs =
            operator.process_with_lookup(&order, &mut ctx, |key| lookup_table.get(key).cloned());

        // Should emit joined event
        assert_eq!(
            outputs
                .iter()
                .filter(|o| matches!(o, Output::Event(_)))
                .count(),
            1
        );

        // Check output schema has both stream and lookup columns
        if let Some(Output::Event(event)) = outputs.first() {
            assert_eq!(event.data.num_columns(), 5); // 2 stream + 3 lookup
        }

        assert_eq!(operator.metrics().events_processed, 1);
        assert_eq!(operator.metrics().events_emitted, 1);
        assert_eq!(operator.metrics().lookups_found, 1);
    }

    #[test]
    fn test_inner_join_no_match() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .join_type(LookupJoinType::Inner)
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());
        let lookup_table = create_lookup_table();

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process order for non-existing customer
        let order = create_order_event(1000, "cust_999", 100);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);

        let outputs =
            operator.process_with_lookup(&order, &mut ctx, |key| lookup_table.get(key).cloned());

        // Inner join should emit nothing for missing lookup
        assert_eq!(outputs.len(), 0);
        assert_eq!(operator.metrics().events_dropped, 1);
        assert_eq!(operator.metrics().lookups_not_found, 1);
    }

    #[test]
    fn test_left_join_no_match() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .join_type(LookupJoinType::Left)
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // First, process an event that matches to establish lookup schema
        let order1 = create_order_event(1000, "cust_1", 100);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        let lookup_table = create_lookup_table();
        operator.process_with_lookup(&order1, &mut ctx, |key| lookup_table.get(key).cloned());

        // Now process order for non-existing customer
        let order2 = create_order_event(2000, "cust_999", 200);
        ctx.processing_time = 2_000_000;
        let outputs = operator.process_with_lookup(&order2, &mut ctx, |_| None);

        // Left join should emit event with nulls
        assert_eq!(
            outputs
                .iter()
                .filter(|o| matches!(o, Output::Event(_)))
                .count(),
            1
        );

        if let Some(Output::Event(event)) = outputs.first() {
            assert_eq!(event.data.num_columns(), 5); // Stream cols + null lookup cols
        }
    }

    #[test]
    fn test_cache_hit() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .cache_ttl(Duration::from_secs(300))
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());
        let lookup_table = create_lookup_table();

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // First lookup - cache miss
        let order1 = create_order_event(1000, "cust_1", 100);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        operator.process_with_lookup(&order1, &mut ctx, |key| lookup_table.get(key).cloned());

        assert_eq!(operator.metrics().cache_misses, 1);
        assert_eq!(operator.metrics().cache_hits, 0);

        // Second lookup - cache hit
        let order2 = create_order_event(2000, "cust_1", 200);
        ctx.processing_time = 2_000_000;
        operator.process_with_lookup(&order2, &mut ctx, |key| lookup_table.get(key).cloned());

        assert_eq!(operator.metrics().cache_misses, 1); // Still 1
        assert_eq!(operator.metrics().cache_hits, 1); // Now 1
    }

    #[test]
    fn test_cache_expiry() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .cache_ttl(Duration::from_secs(1)) // 1 second TTL
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());
        let lookup_table = create_lookup_table();

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // First lookup at t=1s
        let order1 = create_order_event(1000, "cust_1", 100);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
        ctx.processing_time = 1_000_000; // 1 second in microseconds
        operator.process_with_lookup(&order1, &mut ctx, |key| lookup_table.get(key).cloned());

        assert_eq!(operator.metrics().cache_misses, 1);

        // Second lookup at t=1.5s - should still hit cache
        let order2 = create_order_event(2000, "cust_1", 200);
        ctx.processing_time = 1_500_000;
        operator.process_with_lookup(&order2, &mut ctx, |key| lookup_table.get(key).cloned());

        assert_eq!(operator.metrics().cache_hits, 1);

        // Third lookup at t=3s - cache should have expired
        let order3 = create_order_event(3000, "cust_1", 300);
        ctx.processing_time = 3_000_000;
        operator.process_with_lookup(&order3, &mut ctx, |key| lookup_table.get(key).cloned());

        assert_eq!(operator.metrics().cache_misses, 2); // New miss
    }

    #[test]
    fn test_cache_timer_cleanup() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .cache_ttl(Duration::from_secs(1))
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());
        let lookup_table = create_lookup_table();

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Perform lookup
        let order = create_order_event(1000, "cust_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            ctx.processing_time = 1_000_000;
            operator.process_with_lookup(&order, &mut ctx, |key| lookup_table.get(key).cloned());
        }

        // State should have cache entry
        assert!(state.len() > 0);

        // Fire timer after TTL
        let registered_timers = timers.poll_timers(2_000_001);
        assert!(!registered_timers.is_empty());

        for timer_reg in registered_timers {
            let timer = Timer {
                key: timer_reg.key.unwrap_or_default(),
                timestamp: timer_reg.timestamp,
            };
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            ctx.processing_time = timer_reg.timestamp;
            operator.on_timer(timer, &mut ctx);
        }

        assert_eq!(operator.metrics().cache_expirations, 1);
    }

    #[test]
    fn test_checkpoint_restore() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config.clone(), "test_join".to_string());

        // Simulate activity
        operator.metrics.events_processed = 100;
        operator.metrics.cache_hits = 80;
        operator.metrics.cache_misses = 20;
        operator.metrics.lookups_found = 15;
        operator.metrics.lookups_not_found = 5;

        // Checkpoint
        let checkpoint = operator.checkpoint();

        // Restore to new operator
        let mut restored = LookupJoinOperator::with_id(config, "test_join".to_string());
        restored.restore(checkpoint).unwrap();

        assert_eq!(restored.metrics().events_processed, 100);
        assert_eq!(restored.metrics().cache_hits, 80);
        assert_eq!(restored.metrics().cache_misses, 20);
        assert_eq!(restored.metrics().lookups_found, 15);
        assert_eq!(restored.metrics().lookups_not_found, 5);
    }

    #[test]
    fn test_integer_key_lookup() {
        fn create_int_key_event(timestamp: i64, key: i64, value: i64) -> Event {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![key])),
                    Arc::new(Int64Array::from(vec![value])),
                ],
            )
            .unwrap();
            Event::new(timestamp, batch)
        }

        fn create_int_key_lookup(key: i64, data: &str) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("data", DataType::Utf8, false),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![key])),
                    Arc::new(StringArray::from(vec![data])),
                ],
            )
            .unwrap()
        }

        let config = LookupJoinConfig::builder()
            .stream_key_column("key".to_string())
            .lookup_key_column("key".to_string())
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());

        let mut lookup_table: HashMap<Vec<u8>, RecordBatch> = HashMap::new();
        lookup_table.insert(
            42i64.to_le_bytes().to_vec(),
            create_int_key_lookup(42, "matched"),
        );

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        let event = create_int_key_event(1000, 42, 100);
        let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);

        let outputs =
            operator.process_with_lookup(&event, &mut ctx, |key| lookup_table.get(key).cloned());

        assert_eq!(
            outputs
                .iter()
                .filter(|o| matches!(o, Output::Event(_)))
                .count(),
            1
        );
        assert_eq!(operator.metrics().lookups_found, 1);
    }

    #[test]
    fn test_async_lookup_flow() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process event - should go to pending
        let order = create_order_event(1000, "cust_1", 100);
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.process(&order, &mut ctx);
            assert!(outputs.is_empty()); // No output yet
        }

        // Check pending lookups
        assert_eq!(operator.pending_lookups().len(), 1);
        assert_eq!(operator.pending_lookups()[0], b"cust_1");

        // Provide lookup result
        let lookup_result = create_customer_batch("cust_1", "Alice", "gold");
        {
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            let outputs = operator.provide_lookup(b"cust_1", Some(&lookup_result), &mut ctx);

            // Now should emit
            assert_eq!(
                outputs
                    .iter()
                    .filter(|o| matches!(o, Output::Event(_)))
                    .count(),
                1
            );
        }

        // Pending should be cleared
        assert!(operator.pending_lookups().is_empty());
    }

    #[test]
    fn test_cache_entry_serialization() {
        let batch = create_customer_batch("test", "Test", "gold");
        let entry = CacheEntry::found(1_000_000, &batch).unwrap();

        assert!(entry.found);
        assert_eq!(entry.inserted_at, 1_000_000);

        // Verify round-trip
        let restored = entry.to_batch().unwrap().unwrap();
        assert_eq!(restored.num_rows(), 1);
        assert_eq!(restored.num_columns(), 3);
    }

    #[test]
    fn test_cache_entry_expiry() {
        let batch = create_customer_batch("test", "Test", "gold");
        let entry = CacheEntry::found(1_000_000, &batch).unwrap();

        // Not expired at t=1.5s with 1s TTL
        assert!(!entry.is_expired(1_500_000, 1_000_000));

        // Expired at t=2.5s with 1s TTL
        assert!(entry.is_expired(2_500_000, 1_000_000));
    }

    #[test]
    fn test_not_found_cache_entry() {
        let entry = CacheEntry::not_found(1_000_000);

        assert!(!entry.found);
        assert!(entry.data.is_empty());
        assert!(entry.to_batch().unwrap().is_none());
    }

    #[test]
    fn test_metrics_reset() {
        let mut metrics = LookupJoinMetrics::new();
        metrics.events_processed = 100;
        metrics.cache_hits = 50;

        metrics.reset();

        assert_eq!(metrics.events_processed, 0);
        assert_eq!(metrics.cache_hits, 0);
    }

    #[test]
    fn test_multiple_events_same_key() {
        let config = LookupJoinConfig::builder()
            .stream_key_column("customer_id".to_string())
            .lookup_key_column("id".to_string())
            .cache_ttl(Duration::from_secs(300))
            .build()
            .unwrap();

        let mut operator = LookupJoinOperator::with_id(config, "test_join".to_string());
        let lookup_table = create_lookup_table();

        let mut timers = TimerService::new();
        let mut state = InMemoryStore::new();
        let mut watermark_gen = BoundedOutOfOrdernessGenerator::new(100);

        // Process multiple events with same key
        for i in 0..5 {
            let order = create_order_event(1000 + i * 100, "cust_1", 100 + i);
            let mut ctx = create_test_context(&mut timers, &mut state, &mut watermark_gen);
            ctx.processing_time = 1_000_000 + i * 100_000;
            let outputs = operator
                .process_with_lookup(&order, &mut ctx, |key| lookup_table.get(key).cloned());
            assert_eq!(
                outputs
                    .iter()
                    .filter(|o| matches!(o, Output::Event(_)))
                    .count(),
                1
            );
        }

        // Should have 1 miss and 4 hits
        assert_eq!(operator.metrics().cache_misses, 1);
        assert_eq!(operator.metrics().cache_hits, 4);
        assert_eq!(operator.metrics().events_emitted, 5);
    }
}
