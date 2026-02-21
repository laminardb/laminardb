# F-SCHEMA-010: Dead Letter Queue & Error Handling

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SCHEMA-010 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F-SCHEMA-001 (Schema Traits), F025 (Kafka Source) |
| **Crate** | laminar-connector-api (traits + types), laminar-connectors (Kafka DLQ impl) |
| **Created** | 2026-02-21 |
| **Updated** | 2026-02-21 |

## Summary

Implement configurable error handling for source connectors with three
strategies: skip, dead-letter, and fail. When a record fails to decode (schema
mismatch, malformed payload, type conversion error), the error strategy
determines whether the record is silently dropped, routed to a dead letter
queue for later inspection, or causes the pipeline to halt. The dead letter
queue writes poisoned records with full diagnostic metadata (raw bytes, error
message, source name, partition, offset, timestamp) to a configurable
destination. Error detection happens in Ring 1 during `FormatDecoder::decode_batch()`;
DLQ writes happen asynchronously in Ring 2 and never block the hot path.

## Goals

- Configurable `ON ERROR` clause in `CREATE SOURCE` DDL with three strategies: skip, dead_letter, fail
- Dead letter records include raw bytes, error message, source metadata, and timestamp
- Rate limiting via token bucket (`max_errors_per_second`) to prevent DLQ flooding
- Error metrics tracking per source: total errors, errors per second, last error timestamp
- Ring 1 error isolation: poisoned records never reach Ring 0
- Ring 2 async DLQ writes: never block the decode hot path
- DLQ destination is pluggable: Kafka topic (primary), file, or in-memory buffer

## Non-Goals

- DLQ replay/reprocessing automation (manual re-ingestion from DLQ topic is expected)
- Dead letter queues for sink write failures (separate feature; this spec covers source decode errors only)
- Schema correction or auto-repair of malformed records
- DLQ for non-connector sources (in-memory `Source<T>::push()` errors are handled by the push API)
- Cross-source DLQ aggregation (each source has its own DLQ configuration)

## Technical Design

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Error Handling Flow                                   │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Ring 1: Decode + Error Detection                                 │  │
│  │                                                                   │  │
│  │  for record in raw_batch:                                         │  │
│  │      match decoder.decode_one(record):                            │  │
│  │          Ok(row) ──────────────────────────────┐                  │  │
│  │          Err(e) ────┐                          │                  │  │
│  │                     ▼                          ▼                  │  │
│  │              ┌──────────────┐         ┌───────────────┐           │  │
│  │              │ ErrorRouter  │         │ Good Batch    │           │  │
│  │              │              │         │ (RecordBatch) │──► Ring 0 │  │
│  │              │ strategy:    │         └───────────────┘           │  │
│  │              │  Skip → drop │                                     │  │
│  │              │  DLQ  → enq  │                                     │  │
│  │              │  Fail → halt │                                     │  │
│  │              └──────┬───────┘                                     │  │
│  │                     │ (DLQ path)                                  │  │
│  └─────────────────────┼─────────────────────────────────────────────┘  │
│                        │                                                │
│                        ▼                                                │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │  Ring 2: Async DLQ Writer                                         │  │
│  │                                                                   │  │
│  │  ┌───────────┐     ┌──────────────┐     ┌──────────────────────┐ │  │
│  │  │ MPSC      │────►│ Rate Limiter │────►│ DLQ Destination      │ │  │
│  │  │ Channel   │     │ (token bucket│     │ (Kafka topic / file) │ │  │
│  │  │ (bounded) │     │  100 err/s)  │     └──────────────────────┘ │  │
│  │  └───────────┘     └──────────────┘                               │  │
│  │                                                                   │  │
│  │  Rate-limited records are counted in metrics but not written.     │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Ring Integration

| Operation | Ring | Latency Budget | Allocations |
|-----------|------|----------------|-------------|
| `decode_batch()` error detection | Ring 1 | Microseconds (per batch) | Minimal (error path only) |
| Error strategy evaluation (skip/dlq/fail) | Ring 1 | Nanoseconds (branch) | None |
| `DeadLetterRecord` construction | Ring 1 | Nanoseconds (struct build) | One allocation per poisoned record |
| MPSC channel send to DLQ writer | Ring 1 | Nanoseconds (bounded, try_send) | None (pre-allocated channel) |
| Error metrics counter update | Ring 1 | Nanoseconds (atomic increment) | None |
| Token bucket rate check | Ring 2 | Nanoseconds | None |
| DLQ write to Kafka/file | Ring 2 | Milliseconds (async I/O) | Allowed |
| Good batch delivery to Ring 0 | Ring 0 | < 500ns (per record) | Zero |

### API Design

```rust
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

/// Error handling strategy for source decode failures.
///
/// Configured via `ON ERROR` clause in `CREATE SOURCE` DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorStrategy {
    /// Silently skip poisoned records. Errors are counted in metrics
    /// but records are discarded without routing to a DLQ.
    Skip,

    /// Route poisoned records to a dead letter queue for inspection.
    /// The DLQ destination and behavior are controlled by `DeadLetterConfig`.
    DeadLetter {
        config: DeadLetterConfig,
    },

    /// Halt the pipeline immediately on the first decode error.
    /// The error is propagated to the caller as `ConnectorError`.
    Fail,
}

impl Default for ErrorStrategy {
    /// Default strategy is Skip (matches Kafka consumer defaults).
    fn default() -> Self {
        Self::Skip
    }
}

/// Configuration for dead letter queue routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterConfig {
    /// Destination topic or path for dead letter records.
    ///
    /// For Kafka sources: a Kafka topic name (e.g., "trades-dlq").
    /// For file sources: a directory path (e.g., "/data/dlq/").
    pub destination: String,

    /// Maximum error records to write per second.
    ///
    /// Uses a token bucket algorithm. Records exceeding the rate are
    /// counted in metrics but not written to the DLQ destination.
    /// Default: 100. Set to 0 for unlimited.
    pub max_errors_per_second: u32,

    /// Whether to include the raw message bytes in DLQ records.
    ///
    /// When true, the original undecoded bytes are preserved for debugging.
    /// When false, only the error message and metadata are written.
    /// Default: true.
    pub include_raw_message: bool,

    /// Maximum size (bytes) of raw message to include in DLQ records.
    ///
    /// Messages larger than this are truncated. Prevents DLQ flooding
    /// from extremely large malformed payloads. Default: 1 MB.
    pub max_raw_message_size: usize,
}

impl Default for DeadLetterConfig {
    fn default() -> Self {
        Self {
            destination: String::new(),
            max_errors_per_second: 100,
            include_raw_message: true,
            max_raw_message_size: 1_048_576, // 1 MB
        }
    }
}

/// A record routed to the dead letter queue.
///
/// Contains the original raw bytes (if configured), the decode error,
/// and source metadata for debugging and replay.
#[derive(Debug, Clone)]
pub struct DeadLetterRecord {
    /// Raw message bytes from the source (may be truncated).
    /// None if `include_raw_message` is false.
    pub raw_bytes: Option<Vec<u8>>,

    /// The error message describing why decoding failed.
    pub error_message: String,

    /// The error category for structured filtering.
    pub error_kind: DeadLetterErrorKind,

    /// Name of the source that produced this record.
    pub source_name: String,

    /// Timestamp when the error was detected.
    pub timestamp: SystemTime,

    /// Source partition (if applicable, e.g., Kafka partition).
    pub partition: Option<String>,

    /// Source offset within the partition (if applicable).
    pub offset: Option<String>,

    /// Raw message key (if applicable).
    pub key: Option<Vec<u8>>,

    /// Additional source-specific metadata headers.
    pub headers: Vec<(String, String)>,
}

/// Categories of decode errors for structured DLQ filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeadLetterErrorKind {
    /// JSON/Avro/CSV parsing failure.
    MalformedPayload,
    /// Record does not match expected schema (missing fields, extra fields).
    SchemaMismatch,
    /// A field value could not be converted to the target Arrow type.
    TypeConversion,
    /// Schema ID in message header is unknown.
    UnknownSchemaId,
    /// Record exceeds size limits.
    PayloadTooLarge,
    /// Unclassified error.
    Other,
}

/// Per-source error metrics, updated atomically from Ring 1.
///
/// These metrics are exposed via `SourceConnector::metrics()` and the
/// pipeline observability API (`db.source_metrics()`).
pub struct ErrorMetrics {
    /// Total decode errors since source was opened.
    pub errors_total: AtomicU64,
    /// Errors in the current sliding window (for rate calculation).
    errors_window: AtomicU64,
    /// Timestamp of the window start (epoch millis).
    window_start_ms: AtomicU64,
    /// Timestamp of the last error (epoch millis). 0 = no errors.
    pub last_error_ms: AtomicU64,
    /// Total records skipped (Skip strategy).
    pub skipped_total: AtomicU64,
    /// Total records sent to DLQ.
    pub dlq_total: AtomicU64,
    /// Total records rate-limited (would have gone to DLQ but exceeded rate).
    pub rate_limited_total: AtomicU64,
}

impl ErrorMetrics {
    /// Create new zeroed metrics.
    pub fn new() -> Self {
        Self {
            errors_total: AtomicU64::new(0),
            errors_window: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(0),
            last_error_ms: AtomicU64::new(0),
            skipped_total: AtomicU64::new(0),
            dlq_total: AtomicU64::new(0),
            rate_limited_total: AtomicU64::new(0),
        }
    }

    /// Record an error occurrence.
    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_error_ms.store(now, Ordering::Relaxed);

        // Rolling window: reset every second
        let window = self.window_start_ms.load(Ordering::Relaxed);
        if now - window >= 1000 {
            self.errors_window.store(1, Ordering::Relaxed);
            self.window_start_ms.store(now, Ordering::Relaxed);
        } else {
            self.errors_window.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns the approximate error rate (errors per second).
    pub fn errors_per_second(&self) -> u64 {
        self.errors_window.load(Ordering::Relaxed)
    }

    /// Returns a snapshot of all metrics for observability.
    pub fn snapshot(&self) -> ErrorMetricsSnapshot {
        ErrorMetricsSnapshot {
            errors_total: self.errors_total.load(Ordering::Relaxed),
            errors_per_second: self.errors_per_second(),
            last_error_ms: self.last_error_ms.load(Ordering::Relaxed),
            skipped_total: self.skipped_total.load(Ordering::Relaxed),
            dlq_total: self.dlq_total.load(Ordering::Relaxed),
            rate_limited_total: self.rate_limited_total.load(Ordering::Relaxed),
        }
    }
}

/// Immutable snapshot of error metrics for reporting.
#[derive(Debug, Clone)]
pub struct ErrorMetricsSnapshot {
    pub errors_total: u64,
    pub errors_per_second: u64,
    pub last_error_ms: u64,
    pub skipped_total: u64,
    pub dlq_total: u64,
    pub rate_limited_total: u64,
}

/// Token bucket rate limiter for DLQ writes.
///
/// Prevents a burst of malformed records from flooding the DLQ destination.
pub struct TokenBucket {
    /// Maximum tokens (errors allowed per second).
    max_tokens: u32,
    /// Current available tokens.
    tokens: f64,
    /// Last refill timestamp.
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a new token bucket with the given rate (tokens per second).
    /// A rate of 0 means unlimited.
    pub fn new(max_per_second: u32) -> Self {
        Self {
            max_tokens: max_per_second,
            tokens: max_per_second as f64,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns true if allowed.
    pub fn try_acquire(&mut self) -> bool {
        if self.max_tokens == 0 {
            return true; // unlimited
        }
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.max_tokens as f64)
            .min(self.max_tokens as f64);
        self.last_refill = now;
    }
}
```

### SQL Interface

```sql
-- Source with dead letter queue (full configuration)
CREATE SOURCE trades (
    symbol  VARCHAR NOT NULL,
    price   DOUBLE NOT NULL,
    ts      TIMESTAMP NOT NULL
) FROM KAFKA (
    brokers = 'broker1:9092',
    topic   = 'trades'
)
FORMAT JSON
ON ERROR = 'dead_letter' WITH (
    dead_letter_topic     = 'trades-dlq',
    max_errors_per_second = 100,
    include_raw_message   = true
);

-- Source with skip strategy (default)
CREATE SOURCE logs (
    level   VARCHAR,
    message VARCHAR,
    ts      TIMESTAMP
) FROM KAFKA (
    brokers = 'localhost:9092',
    topic   = 'app-logs'
)
FORMAT JSON
ON ERROR = 'skip';

-- Source with fail strategy (strict mode for production)
CREATE SOURCE payments (
    id      BIGINT NOT NULL,
    amount  DOUBLE NOT NULL,
    ts      TIMESTAMP NOT NULL
) FROM KAFKA (
    brokers = 'broker1:9092',
    topic   = 'payments'
)
FORMAT AVRO
USING SCHEMA REGISTRY (url = 'http://schema-registry:8081')
ON ERROR = 'fail';

-- Query error metrics for a source
SELECT * FROM laminar_catalog.source_errors
WHERE source_name = 'trades';
-- ┌─────────────┬──────────────┬──────────────────┬──────────┬──────────┬────────────────┐
-- │ source_name │ errors_total │ errors_per_second│ dlq_total│ skipped  │ rate_limited   │
-- ├─────────────┼──────────────┼──────────────────┼──────────┼──────────┼────────────────┤
-- │ trades      │ 47           │ 2                │ 42       │ 0        │ 5              │
-- └─────────────┴──────────────┴──────────────────┴──────────┴──────────┴────────────────┘

-- Alter error strategy on an existing source
ALTER SOURCE trades ON ERROR = 'skip';
```

**Parser additions** in `streaming_parser.rs`:

```rust
/// ON ERROR clause parsed from CREATE SOURCE DDL.
#[derive(Debug, Clone)]
pub struct OnErrorClause {
    /// The error strategy: "skip", "dead_letter", or "fail".
    pub strategy: String,
    /// Strategy-specific options (dead_letter_topic, max_errors_per_second, etc.)
    pub options: HashMap<String, String>,
}

impl OnErrorClause {
    /// Convert parsed DDL clause to the runtime ErrorStrategy.
    pub fn to_error_strategy(&self) -> Result<ErrorStrategy, ConnectorError> {
        match self.strategy.as_str() {
            "skip" => Ok(ErrorStrategy::Skip),
            "fail" => Ok(ErrorStrategy::Fail),
            "dead_letter" => {
                let destination = self.options.get("dead_letter_topic")
                    .cloned()
                    .unwrap_or_default();
                if destination.is_empty() {
                    return Err(ConnectorError::ConfigurationError(
                        "dead_letter strategy requires 'dead_letter_topic' option".into()
                    ));
                }
                Ok(ErrorStrategy::DeadLetter {
                    config: DeadLetterConfig {
                        destination,
                        max_errors_per_second: self.options.get("max_errors_per_second")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(100),
                        include_raw_message: self.options.get("include_raw_message")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(true),
                        max_raw_message_size: self.options.get("max_raw_message_size")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(1_048_576),
                    },
                })
            }
            other => Err(ConnectorError::ConfigurationError(
                format!("unknown error strategy: '{other}'. Expected 'skip', 'dead_letter', or 'fail'")
            )),
        }
    }
}
```

### Data Structures

```rust
use tokio::sync::mpsc;

/// The error router sits in Ring 1 between the FormatDecoder and the
/// good-batch output path. It inspects decode errors and routes them
/// according to the configured strategy.
pub struct ErrorRouter {
    /// The configured error handling strategy.
    strategy: ErrorStrategy,
    /// Per-source error metrics (shared with observability API).
    metrics: Arc<ErrorMetrics>,
    /// Channel to send DLQ records to the async writer (Ring 2).
    /// None if strategy is not DeadLetter.
    dlq_sender: Option<mpsc::Sender<DeadLetterRecord>>,
    /// Source name for DLQ record metadata.
    source_name: String,
}

impl ErrorRouter {
    /// Create a new error router for the given strategy.
    ///
    /// If the strategy is DeadLetter, also returns the receiver end
    /// of the DLQ channel (to be handed to the DLQ writer task).
    pub fn new(
        source_name: String,
        strategy: ErrorStrategy,
        metrics: Arc<ErrorMetrics>,
    ) -> (Self, Option<mpsc::Receiver<DeadLetterRecord>>) {
        let (dlq_sender, dlq_receiver) = if matches!(strategy, ErrorStrategy::DeadLetter { .. }) {
            let (tx, rx) = mpsc::channel(4096); // bounded channel
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        (
            Self {
                strategy,
                metrics,
                dlq_sender,
                source_name,
            },
            dlq_receiver,
        )
    }

    /// Handle a decode error for a single record.
    ///
    /// Returns `Ok(())` if the error was handled (skip or DLQ).
    /// Returns `Err(ConnectorError)` if the strategy is Fail.
    pub fn handle_error(
        &self,
        error: &SerdeError,
        raw_record: &RawRecord,
    ) -> Result<(), ConnectorError> {
        self.metrics.record_error();

        match &self.strategy {
            ErrorStrategy::Skip => {
                self.metrics.skipped_total.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            ErrorStrategy::DeadLetter { config } => {
                let record = DeadLetterRecord {
                    raw_bytes: if config.include_raw_message {
                        let bytes = &raw_record.value;
                        if bytes.len() > config.max_raw_message_size {
                            Some(bytes[..config.max_raw_message_size].to_vec())
                        } else {
                            Some(bytes.clone())
                        }
                    } else {
                        None
                    },
                    error_message: error.to_string(),
                    error_kind: classify_error(error),
                    source_name: self.source_name.clone(),
                    timestamp: SystemTime::now(),
                    partition: raw_record.partition.as_ref().map(|p| p.id.clone()),
                    offset: raw_record.partition.as_ref().map(|p| p.offset.clone()),
                    key: raw_record.key.clone(),
                    headers: Vec::new(),
                };

                // Non-blocking send; if channel is full, count as rate-limited
                if let Some(sender) = &self.dlq_sender {
                    match sender.try_send(record) {
                        Ok(()) => {
                            self.metrics.dlq_total.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            self.metrics.rate_limited_total.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Ok(())
            }
            ErrorStrategy::Fail => {
                Err(ConnectorError::Serde(SerdeError::MalformedInput(
                    format!(
                        "decode error in source '{}' (ON ERROR = fail): {}",
                        self.source_name, error
                    )
                )))
            }
        }
    }
}

/// Classify a SerdeError into a DLQ error kind for structured filtering.
fn classify_error(error: &SerdeError) -> DeadLetterErrorKind {
    match error {
        SerdeError::MalformedInput(_) | SerdeError::Json(_) | SerdeError::Csv(_) => {
            DeadLetterErrorKind::MalformedPayload
        }
        SerdeError::MissingField(_) | SerdeError::SchemaIncompatible { .. } => {
            DeadLetterErrorKind::SchemaMismatch
        }
        SerdeError::TypeConversion { .. } => {
            DeadLetterErrorKind::TypeConversion
        }
        SerdeError::SchemaNotFound { .. } => {
            DeadLetterErrorKind::UnknownSchemaId
        }
        _ => DeadLetterErrorKind::Other,
    }
}

/// Async DLQ writer task that runs in Ring 2.
///
/// Receives DeadLetterRecords from the ErrorRouter via MPSC channel,
/// applies rate limiting, and writes to the configured destination.
pub struct DeadLetterWriter {
    /// Receiver for DLQ records from Ring 1.
    receiver: mpsc::Receiver<DeadLetterRecord>,
    /// Rate limiter.
    rate_limiter: TokenBucket,
    /// The destination writer (Kafka producer, file writer, etc.)
    destination: Box<dyn DeadLetterDestination>,
    /// Metrics reference for rate-limit counting.
    metrics: Arc<ErrorMetrics>,
}

impl DeadLetterWriter {
    /// Run the DLQ writer loop until the channel is closed.
    pub async fn run(mut self) {
        while let Some(record) = self.receiver.recv().await {
            if self.rate_limiter.try_acquire() {
                if let Err(e) = self.destination.write(record).await {
                    // DLQ write failures are logged but never propagated
                    // to the pipeline -- the hot path must not be affected.
                    tracing::warn!(
                        error = %e,
                        "failed to write dead letter record"
                    );
                }
            } else {
                self.metrics.rate_limited_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Trait for DLQ destination backends.
///
/// Implementations handle the actual I/O of writing dead letter records
/// to Kafka topics, files, or in-memory buffers.
#[async_trait]
pub trait DeadLetterDestination: Send + Sync {
    /// Write a dead letter record to the destination.
    async fn write(&mut self, record: DeadLetterRecord) -> Result<(), ConnectorError>;

    /// Flush any buffered records.
    async fn flush(&mut self) -> Result<(), ConnectorError>;

    /// Close the destination and release resources.
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

/// Kafka-based DLQ destination.
///
/// Writes dead letter records as JSON to a Kafka topic.
pub struct KafkaDlqDestination {
    producer: KafkaProducer,
    topic: String,
}

/// DLQ record serialized to JSON for the Kafka destination.
///
/// ```json
/// {
///   "source_name": "trades",
///   "error_message": "missing field: price",
///   "error_kind": "SchemaMismatch",
///   "timestamp": "2026-02-21T14:30:00Z",
///   "partition": "3",
///   "offset": "12345",
///   "raw_bytes_base64": "eyJzeW1ib2wiOiAiQUFQTCJ9"
/// }
/// ```

/// File-based DLQ destination (for non-Kafka sources).
pub struct FileDlqDestination {
    dir: PathBuf,
    current_file: Option<BufWriter<File>>,
    records_in_file: usize,
    max_records_per_file: usize,
}

/// In-memory DLQ destination (for testing and development).
pub struct InMemoryDlqDestination {
    records: Vec<DeadLetterRecord>,
    max_records: usize,
}
```

### Integration with `FormatDecoder::decode_batch()`

The error router wraps the existing `FormatDecoder` to intercept errors:

```rust
/// A decoder wrapper that routes errors through the ErrorRouter.
///
/// Wraps any FormatDecoder and intercepts per-record errors during
/// batch decoding. Good records are collected into the output batch;
/// bad records are routed to the error handler.
pub struct ErrorHandlingDecoder {
    /// The underlying format decoder.
    inner: Box<dyn FormatDecoder>,
    /// The error router for this source.
    router: ErrorRouter,
}

impl ErrorHandlingDecoder {
    /// Decode a batch with error handling.
    ///
    /// Unlike the underlying decoder which fails the entire batch on
    /// any error, this wrapper decodes record-by-record, routing
    /// failures through the ErrorRouter and collecting successes.
    pub fn decode_batch_with_errors(
        &self,
        records: &[RawRecord],
    ) -> Result<RecordBatch, ConnectorError> {
        // First, try the fast path: decode entire batch at once.
        match self.inner.decode_batch(records) {
            Ok(batch) => return Ok(batch),
            Err(_) => {
                // Fall back to per-record decoding on batch failure.
            }
        }

        // Slow path: decode one at a time, routing errors.
        let mut good_batches = Vec::new();
        for record in records {
            match self.inner.decode_one(record) {
                Ok(batch) => good_batches.push(batch),
                Err(e) => {
                    let serde_err = match e {
                        SchemaError::DecodeError(msg) => SerdeError::MalformedInput(msg),
                        other => SerdeError::MalformedInput(other.to_string()),
                    };
                    self.router.handle_error(&serde_err, record)?;
                }
            }
        }

        // Concatenate good batches into a single RecordBatch
        if good_batches.is_empty() {
            Ok(RecordBatch::new_empty(self.inner.output_schema()))
        } else {
            arrow::compute::concat_batches(
                &self.inner.output_schema(),
                good_batches.iter(),
            ).map_err(|e| ConnectorError::Internal(e.to_string()))
        }
    }
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ConnectorError::Serde(MalformedInput)` | Decode failure with `Fail` strategy | Pipeline halts; fix upstream data and restart |
| DLQ channel full | Burst of errors exceeds channel capacity (4096) | Record counted as rate-limited; increase channel size or reduce error rate |
| DLQ destination write failure | Kafka producer error writing to DLQ topic | Logged via `tracing::warn`; record lost; pipeline continues |
| Rate limit exceeded | Errors exceed `max_errors_per_second` | Record counted in `rate_limited_total`; not written to DLQ |
| `ConfigurationError` | `dead_letter` strategy without `dead_letter_topic` | Rejected at DDL parse time with clear error message |
| DLQ destination unreachable | Kafka DLQ topic does not exist or broker is down | Logged; records accumulated in channel up to capacity, then rate-limited |

## Implementation Plan

| Step | Description | Estimated Effort |
|------|-------------|-----------------|
| 1 | Define `ErrorStrategy`, `DeadLetterConfig`, `DeadLetterRecord`, `DeadLetterErrorKind` types | 0.5 day |
| 2 | Implement `ErrorMetrics` with atomic counters and sliding window | 0.5 day |
| 3 | Implement `TokenBucket` rate limiter | 0.25 day |
| 4 | Implement `ErrorRouter` with skip/dlq/fail routing | 0.5 day |
| 5 | Implement `ErrorHandlingDecoder` wrapper with batch/per-record fallback | 0.5 day |
| 6 | Implement `DeadLetterDestination` trait and `KafkaDlqDestination` | 0.5 day |
| 7 | Implement `DeadLetterWriter` async task (Ring 2) | 0.25 day |
| 8 | Parse `ON ERROR` clause in `streaming_parser.rs` | 0.25 day |
| 9 | Wire error strategy into connector pipeline (`start_connector_pipeline`) | 0.5 day |
| 10 | Implement `InMemoryDlqDestination` for testing | 0.25 day |
| 11 | Implement `laminar_catalog.source_errors` virtual table | 0.25 day |
| 12 | Integration tests and end-to-end validation | 0.75 day |

## Testing Strategy

### Unit Tests

| Module | Tests | What |
|--------|-------|------|
| `error_strategy` | 4 | Default is Skip, parse from DDL, DeadLetter requires topic, Fail variant |
| `error_router` | 8 | Skip drops record, DLQ enqueues record, Fail returns error, channel full counts rate-limited, raw bytes truncation, error classification, metrics increment, handle_error returns Ok for non-Fail |
| `error_metrics` | 6 | Atomic increment, errors_per_second rolling window, snapshot correctness, concurrent access, last_error_ms updated, zero initial state |
| `token_bucket` | 5 | Acquire within rate, deny over rate, refill after time, unlimited (rate=0), burst handling |
| `dead_letter_record` | 4 | Construction with all fields, truncated raw bytes, no raw bytes when disabled, error_kind classification |
| `error_handling_decoder` | 6 | Fast path (no errors), slow path (partial errors), all-error batch returns empty, Fail halts on first error, Skip drops all bad records, DLQ enqueues all bad records |

### Integration Tests

| Test | What |
|------|------|
| `test_skip_strategy_drops_bad_records` | Source with ON ERROR=skip processes good records and silently drops bad ones |
| `test_dlq_routes_to_kafka_topic` | Source with ON ERROR=dead_letter writes poisoned records to DLQ Kafka topic; verify DLQ messages contain correct metadata |
| `test_fail_strategy_halts_pipeline` | Source with ON ERROR=fail stops on first decode error with descriptive error |
| `test_rate_limiting_caps_dlq_writes` | With max_errors_per_second=10, burst of 100 errors only writes ~10 to DLQ |
| `test_dlq_does_not_block_hot_path` | DLQ destination is slow (simulated); pipeline throughput is not affected |
| `test_error_metrics_exposed` | `db.source_metrics("trades")` returns correct error counts |
| `test_alter_source_error_strategy` | `ALTER SOURCE trades ON ERROR = 'skip'` changes strategy at runtime |
| `test_dlq_record_format` | DLQ records contain correct JSON structure with all metadata fields |

### Property Tests

| Test | What |
|------|------|
| `prop_good_records_always_delivered` | For any mix of good and bad records, all good records reach Ring 0 |
| `prop_error_count_matches_bad_records` | `errors_total` equals the number of records that failed to decode |
| `prop_rate_limiter_never_exceeds_rate` | Over any 1-second window, DLQ writes never exceed `max_errors_per_second` |

## Success Criteria

| Metric | Target |
|--------|--------|
| Error routing overhead (Skip strategy) | < 50ns per poisoned record |
| Error routing overhead (DLQ strategy) | < 200ns per poisoned record (channel send) |
| Ring 0 latency impact | Zero (DLQ writes are async in Ring 2) |
| Good record throughput with 10% error rate | >= 95% of clean throughput |
| DLQ record completeness | All metadata fields populated (raw bytes, error, source, partition, offset, timestamp) |
| Rate limiter accuracy | Within 10% of configured rate over 10-second windows |
| ON ERROR DDL parsing | All three strategies parse correctly with options |

## Competitive Comparison

| Feature | LaminarDB | Flink | RisingWave | Materialize | ClickHouse |
|---------|-----------|-------|------------|-------------|------------|
| DLQ support | Native SQL (`ON ERROR`) | Side outputs (Java API) | Not supported | Not supported | `input_format_allow_errors_*` settings |
| Error strategies | Skip, DLQ, Fail | Side outputs only | Skip (implicit) | Fail only | Skip with limits |
| SQL DDL config | `ON ERROR = 'dead_letter' WITH (...)` | None (programmatic) | None | None | Server settings |
| Rate limiting | Token bucket per source | None | N/A | N/A | `input_format_allow_errors_ratio` |
| Error metrics | Per-source atomic counters, virtual table | JMX/metrics | None | None | system.errors table |
| Raw message preservation | Configurable with size limit | Via side output | N/A | N/A | Not preserved |
| Hot path impact | Zero (Ring 2 async) | Low (side output) | N/A | N/A | Low (inline) |
| DLQ destination | Kafka, file, in-memory (pluggable) | Any sink (side output) | N/A | N/A | N/A |
| Runtime strategy change | `ALTER SOURCE ... ON ERROR` | Requires job restart | N/A | N/A | Requires restart |

## Files

- `crates/laminar-connectors/src/error_handling/mod.rs` -- NEW: `ErrorStrategy`, `DeadLetterConfig`, `ErrorRouter`, `DeadLetterRecord`, `DeadLetterErrorKind`
- `crates/laminar-connectors/src/error_handling/metrics.rs` -- NEW: `ErrorMetrics`, `ErrorMetricsSnapshot`
- `crates/laminar-connectors/src/error_handling/rate_limiter.rs` -- NEW: `TokenBucket`
- `crates/laminar-connectors/src/error_handling/writer.rs` -- NEW: `DeadLetterWriter`, `DeadLetterDestination` trait
- `crates/laminar-connectors/src/error_handling/decoder.rs` -- NEW: `ErrorHandlingDecoder` wrapper
- `crates/laminar-connectors/src/error_handling/destinations/kafka.rs` -- NEW: `KafkaDlqDestination`
- `crates/laminar-connectors/src/error_handling/destinations/file.rs` -- NEW: `FileDlqDestination`
- `crates/laminar-connectors/src/error_handling/destinations/memory.rs` -- NEW: `InMemoryDlqDestination`
- `crates/laminar-sql/src/parser/streaming_parser.rs` -- `ON ERROR` clause parsing
- `crates/laminar-sql/src/catalog/source_errors.rs` -- NEW: `laminar_catalog.source_errors` virtual table
- `crates/laminar-db/src/db.rs` -- Wire error strategy into `start_connector_pipeline()`
- `crates/laminar-connectors/src/error.rs` -- Add DLQ-specific error variants if needed

## References

- [schema-inference-design.md](../../../research/schema-inference-design.md) -- Section 7.3: Dead-Letter Handling
- [extensible-schema-traits.md](../../../research/extensible-schema-traits.md) -- FormatDecoder trait (error path)
- [F-CONN-003: Avro Hardening](../F-CONN-003-avro-hardening.md) -- SerdeError variants used by classify_error
- [F-OBS-001: Pipeline Observability](../F-OBS-001-pipeline-observability.md) -- ErrorMetrics integration with observability API
- [Confluent Kafka Error Handling](https://docs.confluent.io/platform/current/clients/consumer.html#error-handling)
- [Flink Side Outputs](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/side_output/)
