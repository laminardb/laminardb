//! Error-handling decoder wrapper.
//!
//! [`ErrorHandlingDecoder`] wraps a [`FormatDecoder`] and intercepts
//! per-record errors during batch decoding. Good records are collected
//! into the output batch; bad records are routed to the
//! [`ErrorRouter`].

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::SerdeError;
use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::FormatDecoder;
use crate::schema::types::RawRecord;

use super::ErrorRouter;

/// A decoder wrapper that routes errors through the [`ErrorRouter`].
///
/// Wraps any [`FormatDecoder`] and intercepts per-record errors during
/// batch decoding. Good records are collected into the output batch;
/// bad records are routed to the error handler.
pub struct ErrorHandlingDecoder {
    /// The underlying format decoder.
    inner: Box<dyn FormatDecoder>,
    /// The error router for this source.
    router: ErrorRouter,
}

impl ErrorHandlingDecoder {
    /// Creates a new error-handling decoder wrapper.
    #[must_use]
    pub fn new(inner: Box<dyn FormatDecoder>, router: ErrorRouter) -> Self {
        Self { inner, router }
    }

    /// Returns the output schema of the underlying decoder.
    #[must_use]
    pub fn output_schema(&self) -> SchemaRef {
        self.inner.output_schema()
    }

    /// Decodes a batch with error handling.
    ///
    /// First tries the fast path: decode the entire batch at once.
    /// If that fails, falls back to per-record decoding, routing
    /// failures through the `ErrorRouter` and collecting successes.
    ///
    /// # Errors
    ///
    /// Returns `SchemaError` if the error strategy is `Fail` and any
    /// record fails to decode.
    pub fn decode_batch_with_errors(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        if records.is_empty() {
            return Ok(RecordBatch::new_empty(self.inner.output_schema()));
        }

        // Fast path: decode entire batch at once.
        if let Ok(batch) = self.inner.decode_batch(records) {
            return Ok(batch);
        }
        // Fall back to per-record decoding on batch failure.

        // Slow path: decode one at a time, routing errors.
        let mut good_batches = Vec::new();
        for record in records {
            match self.inner.decode_one(record) {
                Ok(batch) => good_batches.push(batch),
                Err(e) => {
                    let serde_err = schema_error_to_serde(&e);
                    self.router.handle_error(&serde_err, record)?;
                }
            }
        }

        // Concatenate good batches into a single RecordBatch
        if good_batches.is_empty() {
            Ok(RecordBatch::new_empty(self.inner.output_schema()))
        } else {
            arrow_select::concat::concat_batches(&self.inner.output_schema(), good_batches.iter())
                .map_err(|e| SchemaError::DecodeError(format!("failed to concat batches: {e}")))
        }
    }

    /// Returns a reference to the error router.
    #[must_use]
    pub fn router(&self) -> &ErrorRouter {
        &self.router
    }
}

/// Converts a `SchemaError` to a `SerdeError` for error classification.
fn schema_error_to_serde(err: &SchemaError) -> SerdeError {
    match err {
        SchemaError::DecodeError(msg)
        | SchemaError::Incompatible(msg)
        | SchemaError::InferenceFailed(msg) => SerdeError::MalformedInput(msg.clone()),
        other => SerdeError::MalformedInput(other.to_string()),
    }
}

impl FormatDecoder for ErrorHandlingDecoder {
    fn output_schema(&self) -> SchemaRef {
        self.inner.output_schema()
    }

    fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
        self.decode_batch_with_errors(records)
    }

    fn decode_one(&self, record: &RawRecord) -> SchemaResult<RecordBatch> {
        match self.inner.decode_one(record) {
            Ok(batch) => Ok(batch),
            Err(e) => {
                let serde_err = schema_error_to_serde(&e);
                self.router.handle_error(&serde_err, record)?;
                // If handle_error returned Ok, the record was skipped/DLQ'd
                Ok(RecordBatch::new_empty(self.inner.output_schema()))
            }
        }
    }

    fn format_name(&self) -> &str {
        self.inner.format_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{Field, Schema};

    use crate::error_handling::metrics::ErrorMetrics;
    use crate::error_handling::{DeadLetterConfig, ErrorStrategy};

    /// A test decoder that fails on records containing "BAD".
    struct TestDecoder {
        schema: SchemaRef,
    }

    impl TestDecoder {
        fn new() -> Self {
            Self {
                schema: Arc::new(Schema::new(vec![Field::new(
                    "val",
                    arrow_schema::DataType::Utf8,
                    true,
                )])),
            }
        }
    }

    impl FormatDecoder for TestDecoder {
        fn output_schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn decode_batch(&self, records: &[RawRecord]) -> SchemaResult<RecordBatch> {
            // If any record is bad, fail the whole batch
            for r in records {
                if r.value.starts_with(b"BAD") {
                    return Err(SchemaError::DecodeError("bad record".into()));
                }
            }
            let values: Vec<Option<&str>> = records
                .iter()
                .map(|r| Some(std::str::from_utf8(&r.value).unwrap_or("")))
                .collect();
            let arr = StringArray::from(values);
            Ok(RecordBatch::try_new(Arc::clone(&self.schema), vec![Arc::new(arr)]).unwrap())
        }

        fn decode_one(&self, record: &RawRecord) -> SchemaResult<RecordBatch> {
            if record.value.starts_with(b"BAD") {
                return Err(SchemaError::DecodeError("bad record".into()));
            }
            let arr = StringArray::from(vec![std::str::from_utf8(&record.value).unwrap_or("")]);
            Ok(RecordBatch::try_new(Arc::clone(&self.schema), vec![Arc::new(arr)]).unwrap())
        }

        fn format_name(&self) -> &'static str {
            "test"
        }
    }

    #[test]
    fn test_fast_path_no_errors() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, _rx) =
            ErrorRouter::new("test".into(), ErrorStrategy::Skip, Arc::clone(&metrics));
        let decoder = ErrorHandlingDecoder::new(Box::new(TestDecoder::new()), router);

        let records = vec![
            RawRecord::new(b"hello".to_vec()),
            RawRecord::new(b"world".to_vec()),
        ];
        let batch = decoder.decode_batch_with_errors(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            metrics
                .errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn test_slow_path_skip_strategy() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, _rx) =
            ErrorRouter::new("test".into(), ErrorStrategy::Skip, Arc::clone(&metrics));
        let decoder = ErrorHandlingDecoder::new(Box::new(TestDecoder::new()), router);

        let records = vec![
            RawRecord::new(b"hello".to_vec()),
            RawRecord::new(b"BAD data".to_vec()),
            RawRecord::new(b"world".to_vec()),
        ];
        let batch = decoder.decode_batch_with_errors(&records).unwrap();
        assert_eq!(batch.num_rows(), 2); // only good records
        assert_eq!(
            metrics
                .errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics
                .skipped_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_fail_strategy_halts() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, _rx) =
            ErrorRouter::new("test".into(), ErrorStrategy::Fail, Arc::clone(&metrics));
        let decoder = ErrorHandlingDecoder::new(Box::new(TestDecoder::new()), router);

        let records = vec![
            RawRecord::new(b"hello".to_vec()),
            RawRecord::new(b"BAD data".to_vec()),
        ];
        let result = decoder.decode_batch_with_errors(&records);
        assert!(result.is_err());
    }

    #[test]
    fn test_dlq_strategy_enqueues() {
        let metrics = Arc::new(ErrorMetrics::new());
        let config = DeadLetterConfig {
            destination: "test-dlq".into(),
            ..DeadLetterConfig::default()
        };
        let (router, rx) = ErrorRouter::new(
            "test".into(),
            ErrorStrategy::DeadLetter { config },
            Arc::clone(&metrics),
        );
        let decoder = ErrorHandlingDecoder::new(Box::new(TestDecoder::new()), router);

        let records = vec![
            RawRecord::new(b"hello".to_vec()),
            RawRecord::new(b"BAD data".to_vec()),
            RawRecord::new(b"world".to_vec()),
        ];
        let batch = decoder.decode_batch_with_errors(&records).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // Check DLQ record was sent
        let mut rx = rx.unwrap();
        let dlq_record = rx.try_recv().unwrap();
        assert_eq!(dlq_record.error_message, "malformed input: bad record");
        assert_eq!(dlq_record.raw_bytes, Some(b"BAD data".to_vec()));
    }

    #[test]
    fn test_all_errors_returns_empty_batch() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, _rx) =
            ErrorRouter::new("test".into(), ErrorStrategy::Skip, Arc::clone(&metrics));
        let decoder = ErrorHandlingDecoder::new(Box::new(TestDecoder::new()), router);

        let records = vec![
            RawRecord::new(b"BAD one".to_vec()),
            RawRecord::new(b"BAD two".to_vec()),
        ];
        let batch = decoder.decode_batch_with_errors(&records).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(
            metrics
                .errors_total
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
    }

    #[test]
    fn test_empty_batch() {
        let metrics = Arc::new(ErrorMetrics::new());
        let (router, _rx) =
            ErrorRouter::new("test".into(), ErrorStrategy::Skip, Arc::clone(&metrics));
        let decoder = ErrorHandlingDecoder::new(Box::new(TestDecoder::new()), router);

        let records: Vec<RawRecord> = vec![];
        let batch = decoder.decode_batch_with_errors(&records).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }
}
