//! Dead letter queue and error handling for source connectors (F-SCHEMA-010).
//!
//! Provides configurable error handling strategies for source decode failures:
//!
//! - **Skip** — silently discard poisoned records (counted in metrics)
//! - **Dead Letter** — route poisoned records to a DLQ destination for inspection
//! - **Fail** — halt the pipeline on the first decode error
//!
//! # Architecture
//!
//! ```text
//! Ring 1: ErrorHandlingDecoder wraps FormatDecoder
//!   decode_batch() → good records → Ring 0
//!                  → bad records  → ErrorRouter → MPSC channel
//!
//! Ring 2: DeadLetterWriter (async task)
//!   MPSC channel → TokenBucket → DeadLetterDestination
//! ```
//!
//! Error detection happens in Ring 1 during `FormatDecoder::decode_batch()`.
//! DLQ writes happen asynchronously in Ring 2 and never block the hot path.

pub mod decoder;
pub mod metrics;
pub mod rate_limiter;
pub mod writer;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use tokio::sync::mpsc;

use crate::error::{ConnectorError, SerdeError};
use crate::schema::error::SchemaError;
use crate::schema::types::RawRecord;

pub use decoder::ErrorHandlingDecoder;
pub use metrics::{ErrorMetrics, ErrorMetricsSnapshot};
pub use rate_limiter::TokenBucket;
pub use writer::{DeadLetterDestination, DeadLetterWriter, InMemoryDlqDestination};

// ── Error Strategy ──────────────────────────────────────────────────

/// Error handling strategy for source decode failures.
///
/// Configured via `ON ERROR` clause in `CREATE SOURCE` DDL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorStrategy {
    /// Silently skip poisoned records. Errors are counted in metrics
    /// but records are discarded without routing to a DLQ.
    Skip,

    /// Route poisoned records to a dead letter queue for inspection.
    /// The DLQ destination and behavior are controlled by [`DeadLetterConfig`].
    DeadLetter {
        /// Configuration for DLQ routing.
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

// ── Dead Letter Config ──────────────────────────────────────────────

/// Configuration for dead letter queue routing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetterConfig {
    /// Destination topic or path for dead letter records.
    ///
    /// For Kafka sources: a Kafka topic name (e.g., `"trades-dlq"`).
    /// For file sources: a directory path (e.g., `"/data/dlq/"`).
    pub destination: String,

    /// Maximum error records to write per second.
    ///
    /// Uses a token bucket algorithm. Records exceeding the rate are
    /// counted in metrics but not written to the DLQ destination.
    /// Default: 100. Set to 0 for unlimited.
    pub max_errors_per_second: u32,

    /// Whether to include the raw message bytes in DLQ records.
    ///
    /// When `true`, the original undecoded bytes are preserved for debugging.
    /// When `false`, only the error message and metadata are written.
    /// Default: `true`.
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

// ── Dead Letter Record ──────────────────────────────────────────────

/// A record routed to the dead letter queue.
///
/// Contains the original raw bytes (if configured), the decode error,
/// and source metadata for debugging and replay.
#[derive(Debug, Clone)]
pub struct DeadLetterRecord {
    /// Raw message bytes from the source (may be truncated).
    /// `None` if `include_raw_message` is `false`.
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

// ── Dead Letter Error Kind ──────────────────────────────────────────

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

impl std::fmt::Display for DeadLetterErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedPayload => write!(f, "MalformedPayload"),
            Self::SchemaMismatch => write!(f, "SchemaMismatch"),
            Self::TypeConversion => write!(f, "TypeConversion"),
            Self::UnknownSchemaId => write!(f, "UnknownSchemaId"),
            Self::PayloadTooLarge => write!(f, "PayloadTooLarge"),
            Self::Other => write!(f, "Other"),
        }
    }
}

// ── Error Router ────────────────────────────────────────────────────

/// The error router sits in Ring 1 between the `FormatDecoder` and the
/// good-batch output path. It inspects decode errors and routes them
/// according to the configured strategy.
pub struct ErrorRouter {
    /// The configured error handling strategy.
    strategy: ErrorStrategy,
    /// Per-source error metrics (shared with observability API).
    metrics: Arc<ErrorMetrics>,
    /// Channel to send DLQ records to the async writer (Ring 2).
    /// `None` if strategy is not `DeadLetter`.
    dlq_sender: Option<mpsc::Sender<DeadLetterRecord>>,
    /// Source name for DLQ record metadata.
    source_name: String,
}

impl ErrorRouter {
    /// Creates a new error router for the given strategy.
    ///
    /// If the strategy is `DeadLetter`, also returns the receiver end
    /// of the DLQ channel (to be handed to the DLQ writer task).
    pub fn new(
        source_name: String,
        strategy: ErrorStrategy,
        metrics: Arc<ErrorMetrics>,
    ) -> (Self, Option<mpsc::Receiver<DeadLetterRecord>>) {
        let (dlq_sender, dlq_receiver) = if matches!(strategy, ErrorStrategy::DeadLetter { .. }) {
            let (tx, rx) = mpsc::channel(4096);
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

    /// Handles a decode error for a single record.
    ///
    /// Returns `Ok(())` if the error was handled (skip or DLQ).
    ///
    /// # Errors
    ///
    /// Returns `SchemaError::DecodeError` if the strategy is `Fail`.
    pub fn handle_error(
        &self,
        error: &SerdeError,
        raw_record: &RawRecord,
    ) -> Result<(), SchemaError> {
        self.metrics.record_error();

        match &self.strategy {
            ErrorStrategy::Skip => {
                self.metrics.skipped_total.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            ErrorStrategy::DeadLetter { config } => {
                let record = build_dlq_record(error, raw_record, &self.source_name, config);

                // Non-blocking send; if channel is full, count as rate-limited
                if let Some(sender) = &self.dlq_sender {
                    match sender.try_send(record) {
                        Ok(()) => {
                            self.metrics.dlq_total.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            self.metrics
                                .rate_limited_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Ok(())
            }
            ErrorStrategy::Fail => Err(SchemaError::DecodeError(format!(
                "decode error in source '{}' (ON ERROR = fail): {}",
                self.source_name, error
            ))),
        }
    }

    /// Returns the configured error strategy.
    #[must_use]
    pub fn strategy(&self) -> &ErrorStrategy {
        &self.strategy
    }

    /// Returns the error metrics.
    #[must_use]
    pub fn metrics(&self) -> &Arc<ErrorMetrics> {
        &self.metrics
    }

    /// Returns the source name.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }
}

/// Builds a [`DeadLetterRecord`] from a decode error and raw record.
fn build_dlq_record(
    error: &SerdeError,
    raw_record: &RawRecord,
    source_name: &str,
    config: &DeadLetterConfig,
) -> DeadLetterRecord {
    let raw_bytes = if config.include_raw_message {
        let bytes = &raw_record.value;
        if bytes.len() > config.max_raw_message_size {
            Some(bytes[..config.max_raw_message_size].to_vec())
        } else {
            Some(bytes.clone())
        }
    } else {
        None
    };

    DeadLetterRecord {
        raw_bytes,
        error_message: error.to_string(),
        error_kind: classify_error(error),
        source_name: source_name.to_string(),
        timestamp: SystemTime::now(),
        partition: None,
        offset: None,
        key: raw_record.key.clone(),
        headers: Vec::new(),
    }
}

/// Classifies a [`SerdeError`] into a [`DeadLetterErrorKind`] for structured
/// DLQ filtering.
fn classify_error(error: &SerdeError) -> DeadLetterErrorKind {
    match error {
        SerdeError::MalformedInput(_) | SerdeError::Json(_) | SerdeError::Csv(_) => {
            DeadLetterErrorKind::MalformedPayload
        }
        SerdeError::MissingField(_) | SerdeError::SchemaIncompatible { .. } => {
            DeadLetterErrorKind::SchemaMismatch
        }
        SerdeError::TypeConversion { .. } => DeadLetterErrorKind::TypeConversion,
        SerdeError::SchemaNotFound { .. } => DeadLetterErrorKind::UnknownSchemaId,
        _ => DeadLetterErrorKind::Other,
    }
}

// ── OnErrorClause (DDL parsing support) ─────────────────────────────

/// `ON ERROR` clause parsed from `CREATE SOURCE` DDL.
#[derive(Debug, Clone)]
pub struct OnErrorClause {
    /// The error strategy: `"skip"`, `"dead_letter"`, or `"fail"`.
    pub strategy: String,
    /// Strategy-specific options (`dead_letter_topic`, `max_errors_per_second`, etc.)
    pub options: std::collections::HashMap<String, String>,
}

impl OnErrorClause {
    /// Converts a parsed DDL clause to the runtime [`ErrorStrategy`].
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` if the strategy is unknown
    /// or the `dead_letter` strategy is missing the `dead_letter_topic` option.
    pub fn to_error_strategy(&self) -> Result<ErrorStrategy, ConnectorError> {
        match self.strategy.as_str() {
            "skip" => Ok(ErrorStrategy::Skip),
            "fail" => Ok(ErrorStrategy::Fail),
            "dead_letter" => {
                let destination = self
                    .options
                    .get("dead_letter_topic")
                    .cloned()
                    .unwrap_or_default();
                if destination.is_empty() {
                    return Err(ConnectorError::ConfigurationError(
                        "dead_letter strategy requires 'dead_letter_topic' option".into(),
                    ));
                }
                Ok(ErrorStrategy::DeadLetter {
                    config: DeadLetterConfig {
                        destination,
                        max_errors_per_second: self
                            .options
                            .get("max_errors_per_second")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(100),
                        include_raw_message: self
                            .options
                            .get("include_raw_message")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(true),
                        max_raw_message_size: self
                            .options
                            .get("max_raw_message_size")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(1_048_576),
                    },
                })
            }
            other => Err(ConnectorError::ConfigurationError(format!(
                "unknown error strategy: '{other}'. Expected 'skip', 'dead_letter', or 'fail'"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── ErrorStrategy tests ──

    #[test]
    fn test_default_strategy_is_skip() {
        assert_eq!(ErrorStrategy::default(), ErrorStrategy::Skip);
    }

    #[test]
    fn test_error_strategy_variants() {
        let skip = ErrorStrategy::Skip;
        let fail = ErrorStrategy::Fail;
        let dlq = ErrorStrategy::DeadLetter {
            config: DeadLetterConfig::default(),
        };
        assert_ne!(skip, fail);
        assert_ne!(skip, dlq);
        assert_ne!(fail, dlq);
    }

    // ── DeadLetterConfig tests ──

    #[test]
    fn test_dead_letter_config_defaults() {
        let cfg = DeadLetterConfig::default();
        assert!(cfg.destination.is_empty());
        assert_eq!(cfg.max_errors_per_second, 100);
        assert!(cfg.include_raw_message);
        assert_eq!(cfg.max_raw_message_size, 1_048_576);
    }

    // ── DeadLetterErrorKind tests ──

    #[test]
    fn test_error_kind_display() {
        assert_eq!(
            DeadLetterErrorKind::MalformedPayload.to_string(),
            "MalformedPayload"
        );
        assert_eq!(
            DeadLetterErrorKind::SchemaMismatch.to_string(),
            "SchemaMismatch"
        );
        assert_eq!(
            DeadLetterErrorKind::TypeConversion.to_string(),
            "TypeConversion"
        );
        assert_eq!(
            DeadLetterErrorKind::UnknownSchemaId.to_string(),
            "UnknownSchemaId"
        );
        assert_eq!(
            DeadLetterErrorKind::PayloadTooLarge.to_string(),
            "PayloadTooLarge"
        );
        assert_eq!(DeadLetterErrorKind::Other.to_string(), "Other");
    }

    // ── classify_error tests ──

    #[test]
    fn test_classify_malformed_input() {
        let err = SerdeError::MalformedInput("bad json".into());
        assert_eq!(classify_error(&err), DeadLetterErrorKind::MalformedPayload);
    }

    #[test]
    fn test_classify_json_error() {
        let err = SerdeError::Json("unexpected token".into());
        assert_eq!(classify_error(&err), DeadLetterErrorKind::MalformedPayload);
    }

    #[test]
    fn test_classify_csv_error() {
        let err = SerdeError::Csv("missing delimiter".into());
        assert_eq!(classify_error(&err), DeadLetterErrorKind::MalformedPayload);
    }

    #[test]
    fn test_classify_missing_field() {
        let err = SerdeError::MissingField("price".into());
        assert_eq!(classify_error(&err), DeadLetterErrorKind::SchemaMismatch);
    }

    #[test]
    fn test_classify_type_conversion() {
        let err = SerdeError::TypeConversion {
            field: "age".into(),
            expected: "Int64".into(),
            message: "not a number".into(),
        };
        assert_eq!(classify_error(&err), DeadLetterErrorKind::TypeConversion);
    }

    #[test]
    fn test_classify_schema_not_found() {
        let err = SerdeError::SchemaNotFound { schema_id: 42 };
        assert_eq!(classify_error(&err), DeadLetterErrorKind::UnknownSchemaId);
    }

    #[test]
    fn test_classify_other() {
        let err = SerdeError::UnsupportedFormat("xml".into());
        assert_eq!(classify_error(&err), DeadLetterErrorKind::Other);
    }

    // ── ErrorRouter tests ──

    #[test]
    fn test_router_skip_drops_record() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, rx) =
            ErrorRouter::new("src".into(), ErrorStrategy::Skip, Arc::clone(&metrics));
        assert!(rx.is_none());

        let err = SerdeError::MalformedInput("bad".into());
        let record = RawRecord::new(b"bad data".to_vec());
        let result = router.handle_error(&err, &record);
        assert!(result.is_ok());
        assert_eq!(metrics.errors_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.skipped_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_router_fail_returns_error() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, _rx) =
            ErrorRouter::new("src".into(), ErrorStrategy::Fail, Arc::clone(&metrics));

        let err = SerdeError::MalformedInput("bad".into());
        let record = RawRecord::new(b"bad data".to_vec());
        let result = router.handle_error(&err, &record);
        assert!(result.is_err());
        assert_eq!(metrics.errors_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_router_dlq_enqueues_record() {
        let metrics = Arc::new(ErrorMetrics::new());
        let config = DeadLetterConfig {
            destination: "test-dlq".into(),
            ..DeadLetterConfig::default()
        };
        let (router, rx) = ErrorRouter::new(
            "src".into(),
            ErrorStrategy::DeadLetter { config },
            Arc::clone(&metrics),
        );
        let mut rx = rx.unwrap();

        let err = SerdeError::MalformedInput("bad json".into());
        let record = RawRecord::new(b"raw bytes".to_vec());
        let result = router.handle_error(&err, &record);
        assert!(result.is_ok());

        let dlq_rec = rx.try_recv().unwrap();
        assert_eq!(dlq_rec.source_name, "src");
        assert_eq!(dlq_rec.raw_bytes, Some(b"raw bytes".to_vec()));
        assert_eq!(dlq_rec.error_kind, DeadLetterErrorKind::MalformedPayload);
        assert_eq!(metrics.dlq_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_router_dlq_truncates_raw_bytes() {
        let metrics = Arc::new(ErrorMetrics::new());
        let config = DeadLetterConfig {
            destination: "test-dlq".into(),
            max_raw_message_size: 4,
            ..DeadLetterConfig::default()
        };
        let (router, rx) = ErrorRouter::new(
            "src".into(),
            ErrorStrategy::DeadLetter { config },
            Arc::clone(&metrics),
        );
        let mut rx = rx.unwrap();

        let err = SerdeError::MalformedInput("bad".into());
        let record = RawRecord::new(b"long payload data".to_vec());
        router.handle_error(&err, &record).unwrap();

        let dlq_rec = rx.try_recv().unwrap();
        assert_eq!(dlq_rec.raw_bytes, Some(b"long".to_vec()));
    }

    #[test]
    fn test_router_dlq_no_raw_bytes_when_disabled() {
        let metrics = Arc::new(ErrorMetrics::new());
        let config = DeadLetterConfig {
            destination: "test-dlq".into(),
            include_raw_message: false,
            ..DeadLetterConfig::default()
        };
        let (router, rx) = ErrorRouter::new(
            "src".into(),
            ErrorStrategy::DeadLetter { config },
            Arc::clone(&metrics),
        );
        let mut rx = rx.unwrap();

        let err = SerdeError::MalformedInput("bad".into());
        let record = RawRecord::new(b"raw bytes".to_vec());
        router.handle_error(&err, &record).unwrap();

        let dlq_rec = rx.try_recv().unwrap();
        assert!(dlq_rec.raw_bytes.is_none());
    }

    // ── OnErrorClause tests ──

    #[test]
    fn test_on_error_clause_skip() {
        let clause = OnErrorClause {
            strategy: "skip".into(),
            options: HashMap::new(),
        };
        let strategy = clause.to_error_strategy().unwrap();
        assert_eq!(strategy, ErrorStrategy::Skip);
    }

    #[test]
    fn test_on_error_clause_fail() {
        let clause = OnErrorClause {
            strategy: "fail".into(),
            options: HashMap::new(),
        };
        let strategy = clause.to_error_strategy().unwrap();
        assert_eq!(strategy, ErrorStrategy::Fail);
    }

    #[test]
    fn test_on_error_clause_dead_letter() {
        let mut options = HashMap::new();
        options.insert("dead_letter_topic".into(), "trades-dlq".into());
        options.insert("max_errors_per_second".into(), "50".into());

        let clause = OnErrorClause {
            strategy: "dead_letter".into(),
            options,
        };
        let strategy = clause.to_error_strategy().unwrap();
        match strategy {
            ErrorStrategy::DeadLetter { config } => {
                assert_eq!(config.destination, "trades-dlq");
                assert_eq!(config.max_errors_per_second, 50);
            }
            _ => panic!("expected DeadLetter"),
        }
    }

    #[test]
    fn test_on_error_clause_dead_letter_missing_topic() {
        let clause = OnErrorClause {
            strategy: "dead_letter".into(),
            options: HashMap::new(),
        };
        let result = clause.to_error_strategy();
        assert!(result.is_err());
    }

    #[test]
    fn test_on_error_clause_unknown_strategy() {
        let clause = OnErrorClause {
            strategy: "ignore".into(),
            options: HashMap::new(),
        };
        let result = clause.to_error_strategy();
        assert!(result.is_err());
    }
}
