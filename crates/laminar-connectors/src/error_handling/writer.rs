//! Async DLQ writer task and destination trait.
//!
//! The [`DeadLetterWriter`] runs in Ring 2 as a `tokio::spawn` task.
//! It receives [`DeadLetterRecord`]s from the [`ErrorRouter`](super::ErrorRouter)
//! via an MPSC channel, applies rate limiting, and writes to a pluggable
//! [`DeadLetterDestination`].

use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::mpsc;

use crate::error::ConnectorError;

use super::metrics::ErrorMetrics;
use super::rate_limiter::TokenBucket;
use super::DeadLetterRecord;

/// Trait for DLQ destination backends.
///
/// Implementations handle the actual I/O of writing dead letter records
/// to Kafka topics, files, or in-memory buffers.
#[async_trait]
pub trait DeadLetterDestination: Send + Sync {
    /// Writes a dead letter record to the destination.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the write fails.
    async fn write(&mut self, record: DeadLetterRecord) -> Result<(), ConnectorError>;

    /// Flushes any buffered records.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if the flush fails.
    async fn flush(&mut self) -> Result<(), ConnectorError>;

    /// Closes the destination and releases resources.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError` if closing fails.
    async fn close(&mut self) -> Result<(), ConnectorError>;
}

/// Async DLQ writer task that runs in Ring 2.
///
/// Receives `DeadLetterRecord`s from the `ErrorRouter` via MPSC channel,
/// applies rate limiting via a [`TokenBucket`], and writes to the
/// configured [`DeadLetterDestination`].
pub struct DeadLetterWriter {
    /// Receiver for DLQ records from Ring 1.
    receiver: mpsc::Receiver<DeadLetterRecord>,
    /// Rate limiter.
    rate_limiter: TokenBucket,
    /// The destination writer.
    destination: Box<dyn DeadLetterDestination>,
    /// Metrics reference for rate-limit counting.
    metrics: Arc<ErrorMetrics>,
}

impl DeadLetterWriter {
    /// Creates a new DLQ writer.
    pub fn new(
        receiver: mpsc::Receiver<DeadLetterRecord>,
        max_errors_per_second: u32,
        destination: Box<dyn DeadLetterDestination>,
        metrics: Arc<ErrorMetrics>,
    ) -> Self {
        Self {
            receiver,
            rate_limiter: TokenBucket::new(max_errors_per_second),
            destination,
            metrics,
        }
    }

    /// Runs the DLQ writer loop until the channel is closed.
    ///
    /// DLQ write failures are logged but never propagated to the pipeline.
    pub async fn run(mut self) {
        while let Some(record) = self.receiver.recv().await {
            if self.rate_limiter.try_acquire() {
                if let Err(e) = self.destination.write(record).await {
                    tracing::warn!(
                        error = %e,
                        "failed to write dead letter record"
                    );
                }
            } else {
                self.metrics
                    .rate_limited_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        // Channel closed — flush and close the destination
        if let Err(e) = self.destination.flush().await {
            tracing::warn!(error = %e, "failed to flush DLQ destination");
        }
        if let Err(e) = self.destination.close().await {
            tracing::warn!(error = %e, "failed to close DLQ destination");
        }
    }
}

/// In-memory DLQ destination for testing and development.
///
/// Stores records in a `Vec` with a configurable capacity limit.
/// Records beyond the limit are silently dropped.
#[derive(Debug)]
pub struct InMemoryDlqDestination {
    records: Vec<DeadLetterRecord>,
    max_records: usize,
}

impl InMemoryDlqDestination {
    /// Creates a new in-memory DLQ destination.
    #[must_use]
    pub fn new(max_records: usize) -> Self {
        Self {
            records: Vec::new(),
            max_records,
        }
    }

    /// Returns the stored records.
    #[must_use]
    pub fn records(&self) -> &[DeadLetterRecord] {
        &self.records
    }

    /// Returns the number of stored records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns `true` if no records have been stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Drains all stored records.
    pub fn drain(&mut self) -> Vec<DeadLetterRecord> {
        std::mem::take(&mut self.records)
    }
}

#[async_trait]
impl DeadLetterDestination for InMemoryDlqDestination {
    async fn write(&mut self, record: DeadLetterRecord) -> Result<(), ConnectorError> {
        if self.records.len() < self.max_records {
            self.records.push(record);
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    use crate::error_handling::DeadLetterErrorKind;

    fn sample_record() -> DeadLetterRecord {
        DeadLetterRecord {
            raw_bytes: Some(b"bad data".to_vec()),
            error_message: "parse error".into(),
            error_kind: DeadLetterErrorKind::MalformedPayload,
            source_name: "test-source".into(),
            timestamp: SystemTime::now(),
            partition: Some("0".into()),
            offset: Some("42".into()),
            key: None,
            headers: Vec::new(),
        }
    }

    #[tokio::test]
    async fn test_in_memory_destination_write() {
        let mut dest = InMemoryDlqDestination::new(100);
        assert!(dest.is_empty());

        dest.write(sample_record()).await.unwrap();
        assert_eq!(dest.len(), 1);
        assert_eq!(dest.records()[0].source_name, "test-source");
    }

    #[tokio::test]
    async fn test_in_memory_destination_capacity_limit() {
        let mut dest = InMemoryDlqDestination::new(2);
        dest.write(sample_record()).await.unwrap();
        dest.write(sample_record()).await.unwrap();
        dest.write(sample_record()).await.unwrap(); // over limit

        assert_eq!(dest.len(), 2); // capped at max
    }

    #[tokio::test]
    async fn test_in_memory_destination_drain() {
        let mut dest = InMemoryDlqDestination::new(100);
        dest.write(sample_record()).await.unwrap();
        dest.write(sample_record()).await.unwrap();

        let drained = dest.drain();
        assert_eq!(drained.len(), 2);
        assert!(dest.is_empty());
    }

    #[tokio::test]
    async fn test_writer_processes_records() {
        let (tx, rx) = mpsc::channel(16);
        let metrics = Arc::new(ErrorMetrics::new());

        // We need to wrap dest to inspect it after the writer runs.
        // Use a shared Arc<Mutex<>> for testing.
        let shared_records = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let shared_clone = Arc::clone(&shared_records);

        struct SharedDest {
            inner: Arc<tokio::sync::Mutex<Vec<DeadLetterRecord>>>,
        }

        #[async_trait]
        impl DeadLetterDestination for SharedDest {
            async fn write(&mut self, record: DeadLetterRecord) -> Result<(), ConnectorError> {
                self.inner.lock().await.push(record);
                Ok(())
            }
            async fn flush(&mut self) -> Result<(), ConnectorError> {
                Ok(())
            }
            async fn close(&mut self) -> Result<(), ConnectorError> {
                Ok(())
            }
        }

        let writer = DeadLetterWriter::new(rx, 0, Box::new(SharedDest { inner: shared_clone }), Arc::clone(&metrics));
        let handle = tokio::spawn(writer.run());

        tx.send(sample_record()).await.unwrap();
        tx.send(sample_record()).await.unwrap();
        drop(tx); // close channel

        handle.await.unwrap();
        let records = shared_records.lock().await;
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn test_writer_rate_limiting() {
        let (tx, rx) = mpsc::channel(16);
        let metrics = Arc::new(ErrorMetrics::new());
        let shared_records = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let shared_clone = Arc::clone(&shared_records);

        struct SharedDest {
            inner: Arc<tokio::sync::Mutex<Vec<DeadLetterRecord>>>,
        }

        #[async_trait]
        impl DeadLetterDestination for SharedDest {
            async fn write(&mut self, record: DeadLetterRecord) -> Result<(), ConnectorError> {
                self.inner.lock().await.push(record);
                Ok(())
            }
            async fn flush(&mut self) -> Result<(), ConnectorError> {
                Ok(())
            }
            async fn close(&mut self) -> Result<(), ConnectorError> {
                Ok(())
            }
        }

        // Rate limit to 2/sec
        let writer = DeadLetterWriter::new(
            rx,
            2,
            Box::new(SharedDest { inner: shared_clone }),
            Arc::clone(&metrics),
        );
        let handle = tokio::spawn(writer.run());

        // Send 5 records quickly
        for _ in 0..5 {
            tx.send(sample_record()).await.unwrap();
        }
        drop(tx);

        handle.await.unwrap();
        let records = shared_records.lock().await;
        // At most 2 should have been written (rate limited)
        assert!(records.len() <= 2);
        // The rest should be rate-limited
        assert!(metrics.rate_limited_total.load(Ordering::Relaxed) >= 3);
    }
}
