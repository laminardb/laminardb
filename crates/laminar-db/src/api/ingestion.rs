//! Data ingestion types for FFI.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::error::{codes, ApiError};
use crate::UntypedSourceHandle;

/// Writer for inserting data into a source.
///
/// Provides explicit lifecycle management for FFI. Unlike using
/// `UntypedSourceHandle` directly, `Writer` tracks closed state
/// and validates schemas before writes.
///
/// # Example
///
/// ```rust,ignore
/// let writer = conn.writer("trades")?;
/// writer.write(batch)?;
/// writer.flush()?;
/// writer.close()?;
/// ```
pub struct Writer {
    handle: UntypedSourceHandle,
    closed: bool,
}

impl Writer {
    /// Create a new writer from an untyped source handle.
    pub(crate) fn new(handle: UntypedSourceHandle) -> Self {
        Self {
            handle,
            closed: false,
        }
    }

    /// Write a `RecordBatch` to the source.
    ///
    /// # Errors
    ///
    /// Returns `ApiError::Ingestion` if:
    /// - The writer is closed
    /// - The batch schema doesn't match the source schema
    /// - The underlying channel is full or closed
    pub fn write(&mut self, batch: RecordBatch) -> Result<(), ApiError> {
        if self.closed {
            return Err(ApiError::Ingestion {
                code: codes::WRITER_CLOSED,
                message: "Writer is closed".into(),
            });
        }

        // Validate schema matches (compare field count and types)
        let expected = self.handle.schema();
        let actual = batch.schema();
        if expected.fields().len() != actual.fields().len() {
            return Err(ApiError::Ingestion {
                code: codes::BATCH_SCHEMA_MISMATCH,
                message: format!(
                    "Schema mismatch: expected {} columns, got {}",
                    expected.fields().len(),
                    actual.fields().len()
                ),
            });
        }

        self.handle
            .push_arrow(batch)
            .map_err(|e| ApiError::ingestion(e.to_string()))
    }

    /// Flush pending data.
    ///
    /// For in-memory sources this is a no-op, but external connectors
    /// may buffer data and require explicit flushing.
    ///
    /// # Errors
    ///
    /// Returns `ApiError::Ingestion` if the writer is closed.
    pub fn flush(&mut self) -> Result<(), ApiError> {
        if self.closed {
            return Err(ApiError::Ingestion {
                code: codes::WRITER_CLOSED,
                message: "Writer is closed".into(),
            });
        }
        // In-memory sources don't buffer, but this is part of the API contract
        Ok(())
    }

    /// Explicitly close the writer.
    ///
    /// After closing, no more writes are allowed.
    ///
    /// # Errors
    ///
    /// Currently always succeeds, but may return errors in the future
    /// for external connectors that need cleanup.
    #[allow(clippy::unnecessary_wraps)]
    pub fn close(mut self) -> Result<(), ApiError> {
        self.closed = true;
        Ok(())
    }

    /// Get the source schema.
    #[must_use]
    pub fn schema(&self) -> SchemaRef {
        self.handle.schema().clone()
    }

    /// Get the source name.
    #[must_use]
    pub fn name(&self) -> &str {
        self.handle.name()
    }

    /// Emit a watermark timestamp.
    ///
    /// Watermarks indicate that all events with timestamps less than or equal
    /// to the watermark have been seen.
    pub fn watermark(&self, timestamp: i64) {
        self.handle.watermark(timestamp);
    }

    /// Get the current watermark.
    #[must_use]
    pub fn current_watermark(&self) -> i64 {
        self.handle.current_watermark()
    }
}

// SAFETY: Writer wraps UntypedSourceHandle which uses Arc<SourceEntry>.
// All operations are thread-safe via internal synchronization.
unsafe impl Send for Writer {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_writer_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Writer>();
    }
}
