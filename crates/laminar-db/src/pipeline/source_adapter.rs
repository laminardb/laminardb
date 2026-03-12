//! Source I/O thread adapter for thread-per-core mode.
//!
//! Bridges an async [`SourceConnector`] to a core thread's SPSC inbox.
//! Each source runs on a dedicated `std::thread` with a single-threaded
//! tokio runtime, pushing [`CoreMessage::Event`] into the target core's
//! inbox after converting `SourceBatch` → [`Event`].
#![allow(clippy::disallowed_types)] // cold path

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use arrow_array::RecordBatch;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{DeliveryGuarantee, SourceConnector};
use laminar_core::checkpoint::{BarrierPollHandle, CheckpointBarrierInjector};
use laminar_core::operator::Event;
use laminar_core::tpc::{CoreMessage, SpscQueue};

/// Per-source metrics (lock-free atomic reads from any thread).
#[derive(Debug)]
pub struct SourceIoMetrics {
    /// Total batches received from the connector.
    pub batches: AtomicU64,
    /// Total records received.
    pub records: AtomicU64,
    /// Total errors during poll.
    pub errors: AtomicU64,
    /// Last poll duration in nanoseconds.
    pub last_poll_ns: AtomicU64,
}

impl Default for SourceIoMetrics {
    fn default() -> Self {
        Self {
            batches: AtomicU64::new(0),
            records: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_poll_ns: AtomicU64::new(0),
        }
    }
}

/// Bridges an async `SourceConnector` to a core thread's SPSC inbox.
///
/// Runs a dedicated `std::thread` with a single-threaded tokio runtime.
/// Polls the connector and pushes `CoreMessage::Event` into the target
/// core's inbox. Barrier detection is handled inline after each poll.
pub struct SourceIoThread {
    thread: Option<JoinHandle<Option<Box<dyn SourceConnector>>>>,
    shutdown: Arc<AtomicBool>,
    /// Barrier injector for this source (coordinator uses this to trigger barriers).
    pub injector: CheckpointBarrierInjector,
    /// Lock-free metrics.
    pub metrics: Arc<SourceIoMetrics>,
    /// Watch receiver for checkpoint snapshots captured at barrier points.
    pub checkpoint_rx: tokio::sync::watch::Receiver<SourceCheckpoint>,
}

impl SourceIoThread {
    /// Spawn a new source I/O thread.
    ///
    /// The thread opens the connector, polls for batches, and pushes events
    /// into the target core's inbox. Barrier detection happens inline.
    ///
    /// The `core_thread` handle is used to `unpark()` the core after each
    /// inbox push, waking it from `park_timeout()` sleep.
    ///
    /// # Errors
    ///
    /// Returns `std::io::Error` if the OS thread cannot be spawned.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        source_idx: usize,
        name: String,
        connector: Box<dyn SourceConnector>,
        config: ConnectorConfig,
        target_inbox: Arc<SpscQueue<CoreMessage>>,
        max_poll_records: usize,
        fallback_poll_interval: Duration,
        core_thread: thread::Thread,
        restore_checkpoint: Option<SourceCheckpoint>,
        delivery_guarantee: DeliveryGuarantee,
    ) -> std::io::Result<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let injector = CheckpointBarrierInjector::new();
        let barrier_handle = injector.handle();
        let metrics = Arc::new(SourceIoMetrics::default());
        let (cp_tx, cp_rx) = tokio::sync::watch::channel(SourceCheckpoint::new(0));

        let shutdown_clone = Arc::clone(&shutdown);
        let metrics_clone = Arc::clone(&metrics);

        let thread = thread::Builder::new()
            .name(format!("laminar-src-{name}"))
            .spawn(move || {
                source_io_main(
                    source_idx,
                    name,
                    connector,
                    config,
                    target_inbox,
                    max_poll_records,
                    fallback_poll_interval,
                    shutdown_clone,
                    barrier_handle,
                    metrics_clone,
                    cp_tx,
                    core_thread,
                    restore_checkpoint,
                    delivery_guarantee,
                )
            })?;

        Ok(Self {
            thread: Some(thread),
            shutdown,
            injector,
            metrics,
            checkpoint_rx: cp_rx,
        })
    }

    /// Signal shutdown and join the thread, returning the connector for cleanup.
    pub fn shutdown_and_join(&mut self) -> Option<Box<dyn SourceConnector>> {
        self.shutdown.store(true, Ordering::Release);
        self.thread.take().and_then(|h| h.join().ok()).flatten()
    }
}

impl Drop for SourceIoThread {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Extract the maximum timestamp from a `RecordBatch`.
///
/// Scans for a column named `event_time` or `timestamp` (i64 or
/// `TimestampMillisecond`) and returns the MAX value across all rows.
/// Using MAX ensures watermark correctness for out-of-order batches.
/// If not found, falls back to wall-clock time.
fn extract_timestamp(batch: &RecordBatch) -> i64 {
    use arrow::compute::kernels::aggregate::max as arrow_max;
    use arrow_array::Array;

    for name in &["event_time", "timestamp"] {
        if let Ok(col_idx) = batch.schema().index_of(name) {
            let col = batch.column(col_idx);
            // Try i64 array — use max across all rows
            if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                if !arr.is_empty() {
                    if let Some(max_val) = arrow_max(arr) {
                        return max_val;
                    }
                }
            }
            // Try TimestampMillisecond array — use max across all rows
            if let Some(arr) = col
                .as_any()
                .downcast_ref::<arrow_array::TimestampMillisecondArray>()
            {
                if !arr.is_empty() {
                    if let Some(max_val) = arrow_max(arr) {
                        return max_val;
                    }
                }
            }
        }
    }

    // Fallback: wall-clock time in milliseconds
    #[allow(clippy::cast_possible_truncation)]
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// Main loop for the source I/O thread.
#[allow(
    clippy::too_many_arguments,
    clippy::needless_pass_by_value,
    clippy::too_many_lines
)]
fn source_io_main(
    source_idx: usize,
    name: String,
    mut connector: Box<dyn SourceConnector>,
    config: ConnectorConfig,
    target_inbox: Arc<SpscQueue<CoreMessage>>,
    max_poll_records: usize,
    fallback_poll_interval: Duration,
    shutdown: Arc<AtomicBool>,
    barrier_handle: BarrierPollHandle,
    metrics: Arc<SourceIoMetrics>,
    cp_tx: tokio::sync::watch::Sender<SourceCheckpoint>,
    core_thread: thread::Thread,
    restore_checkpoint: Option<SourceCheckpoint>,
    delivery_guarantee: DeliveryGuarantee,
) -> Option<Box<dyn SourceConnector>> {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!("Source '{name}': failed to create tokio runtime: {e}");
            return Some(connector);
        }
    };

    rt.block_on(async {
        // Open the connector
        if let Err(e) = connector.open(&config).await {
            tracing::error!("Source '{name}': failed to open connector: {e}");
            return None;
        }

        // Restore checkpoint offsets AFTER open but BEFORE first poll.
        // For Kafka, this calls consumer.assign() to seek to the checkpoint
        // position. The consumer must still be owned by the source (not yet
        // moved to a background reader task) for assign() to take effect.
        if let Some(ref checkpoint) = restore_checkpoint {
            match connector.restore(checkpoint).await {
                Ok(()) => {
                    tracing::info!(
                        source = %name,
                        "restored source from checkpoint"
                    );
                }
                Err(e) => {
                    if matches!(delivery_guarantee, DeliveryGuarantee::ExactlyOnce) {
                        tracing::error!(
                            source = %name,
                            error = %e,
                            "[LDB-5030] source checkpoint restore failed under exactly-once \
                             — cannot guarantee delivery semantics"
                        );
                        return None;
                    }
                    tracing::warn!(
                        source = %name,
                        error = %e,
                        "source checkpoint restore failed, starting from group offsets"
                    );
                }
            }
        }

        let notify = connector.data_ready_notify();
        let mut epoch: u64 = 0;

        loop {
            if shutdown.load(Ordering::Acquire) {
                break;
            }

            // Wait for data or timeout
            if let Some(ref notifier) = notify {
                tokio::select! {
                    biased;
                    () = notifier.notified() => {}
                    () = tokio::time::sleep(fallback_poll_interval) => {}
                }
            } else {
                tokio::time::sleep(fallback_poll_interval).await;
            }

            if shutdown.load(Ordering::Acquire) {
                break;
            }

            // Poll for a batch
            let poll_start = std::time::Instant::now();
            match connector.poll_batch(max_poll_records).await {
                Ok(Some(batch)) => {
                    let num_rows = batch.records.num_rows();
                    metrics.batches.fetch_add(1, Ordering::Relaxed);
                    #[allow(clippy::cast_possible_truncation)]
                    metrics
                        .records
                        .fetch_add(num_rows as u64, Ordering::Relaxed);
                    #[allow(clippy::cast_possible_truncation)]
                    metrics
                        .last_poll_ns
                        .store(poll_start.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    // Convert SourceBatch → Event
                    let timestamp = extract_timestamp(&batch.records);
                    let event = Event::new(timestamp, batch.records);

                    // Push to core inbox with tiered backoff under backpressure
                    let mut msg = CoreMessage::Event { source_idx, event };
                    let mut backoff = 0u32;
                    loop {
                        match target_inbox.push(msg) {
                            Ok(()) => {
                                // Wake the core thread from park_timeout() sleep
                                core_thread.unpark();
                                break;
                            }
                            Err(returned) => {
                                if shutdown.load(Ordering::Acquire) {
                                    break;
                                }
                                msg = returned;
                                backoff += 1;
                                match backoff {
                                    0..=63 => std::hint::spin_loop(),
                                    64..=255 => std::thread::yield_now(),
                                    _ => std::thread::park_timeout(
                                        std::time::Duration::from_micros(100),
                                    ),
                                }
                            }
                        }
                    }
                }
                Ok(None) => {
                    // No data available
                    #[allow(clippy::cast_possible_truncation)]
                    metrics
                        .last_poll_ns
                        .store(poll_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    metrics.errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("Source '{name}': poll error: {e}");
                }
            }

            // Publish current offsets for timer-based checkpoints.
            // watch::send is a lock-free atomic pointer swap (~5ns).
            let _ = cp_tx.send(connector.checkpoint());

            // Check for pending barrier
            if let Some(barrier) = barrier_handle.poll(epoch) {
                epoch += 1;
                // Capture checkpoint at barrier point
                let _ = cp_tx.send(connector.checkpoint());
                // Forward barrier to core with same backpressure as events.
                // A dropped barrier breaks Chandy-Lamport alignment — the
                // coordinator would wait 30 s then abandon the checkpoint.
                let mut bmsg = CoreMessage::Barrier {
                    source_idx,
                    barrier,
                };
                let mut barrier_backoff = 0u32;
                loop {
                    match target_inbox.push(bmsg) {
                        Ok(()) => {
                            core_thread.unpark();
                            break;
                        }
                        Err(returned) => {
                            if shutdown.load(Ordering::Acquire) {
                                break;
                            }
                            bmsg = returned;
                            barrier_backoff += 1;
                            match barrier_backoff {
                                0..=63 => std::hint::spin_loop(),
                                64..=255 => std::thread::yield_now(),
                                _ => {
                                    std::thread::park_timeout(std::time::Duration::from_micros(
                                        100,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Close connector
        if let Err(e) = connector.close().await {
            tracing::warn!("Source '{name}': close error: {e}");
        }

        Some(connector)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_extract_timestamp_event_time() {
        let schema = Schema::new(vec![Field::new("event_time", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![42_000]))],
        )
        .unwrap();
        assert_eq!(extract_timestamp(&batch), 42_000);
    }

    #[test]
    fn test_extract_timestamp_column() {
        let schema = Schema::new(vec![Field::new("timestamp", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![99_000]))],
        )
        .unwrap();
        assert_eq!(extract_timestamp(&batch), 99_000);
    }

    #[test]
    fn test_extract_timestamp_millis() {
        let schema = Schema::new(vec![Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        )]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(TimestampMillisecondArray::from(vec![123_456]))],
        )
        .unwrap();
        assert_eq!(extract_timestamp(&batch), 123_456);
    }

    #[test]
    fn test_extract_timestamp_fallback() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(Int64Array::from(vec![1]))])
                .unwrap();
        let ts = extract_timestamp(&batch);
        // Should be approximately current time in millis
        #[allow(clippy::cast_possible_truncation)]
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        assert!((ts - now).abs() < 1000);
    }

    #[test]
    fn test_source_io_metrics_default() {
        let metrics = SourceIoMetrics::default();
        assert_eq!(metrics.batches.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.records.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.errors.load(Ordering::Relaxed), 0);
    }
}
