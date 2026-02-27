//! Per-source tokio task with exclusive ownership (no `Arc<Mutex>`).
//!
//! Each source connector runs in its own task. Data is pushed to the
//! coordinator through a bounded `mpsc` channel. The task awaits on
//! the source's `data_ready_notify()` handle when available, falling
//! back to a timer when the source doesn't provide one.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{mpsc, Notify};

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::connector::SourceConnector;

use super::config::PipelineConfig;
use super::metrics::SourceTaskMetrics;
use super::source_event::SourceEvent;

/// Handle returned when a source task is spawned.
pub struct SourceTaskHandle {
    /// Task join handle.
    pub join: tokio::task::JoinHandle<Box<dyn SourceConnector>>,
    /// Shutdown signal for this task.
    pub shutdown: Arc<Notify>,
    /// Per-source metrics (lock-free atomic reads).
    pub metrics: Arc<SourceTaskMetrics>,
    /// Watch receiver for checkpoint snapshots.
    pub checkpoint_rx: tokio::sync::watch::Receiver<SourceCheckpoint>,
}

/// Spawns a per-source task that polls `connector` and sends events to `tx`.
///
/// Returns a [`SourceTaskHandle`] for shutdown, metrics, and checkpointing.
#[must_use]
pub fn spawn_source_task(
    idx: usize,
    name: String,
    mut connector: Box<dyn SourceConnector>,
    tx: mpsc::Sender<SourceEvent>,
    config: &PipelineConfig,
) -> SourceTaskHandle {
    let shutdown = Arc::new(Notify::new());
    let shutdown_rx = Arc::clone(&shutdown);
    let metrics = Arc::new(SourceTaskMetrics::default());
    let metrics_tx = Arc::clone(&metrics);
    let max_poll = config.max_poll_records;
    let fallback_interval = config.fallback_poll_interval;

    // Lock-free checkpoint channel: source task writes, coordinator reads.
    let initial_cp = connector.checkpoint();
    let (cp_tx, cp_rx) = tokio::sync::watch::channel(initial_cp);

    // Capture the data_ready_notify handle before we move into the task.
    let data_notify = connector.data_ready_notify();

    let join = tokio::spawn(async move {
        tracing::debug!(source = %name, idx, "Source task started");

        loop {
            tokio::select! {
                biased;

                () = shutdown_rx.notified() => {
                    tracing::debug!(source = %name, idx, "Source task shutdown");
                    break;
                }

                () = wait_for_data(data_notify.as_ref(), fallback_interval) => {}
            }

            // Poll the connector (exclusive ownership — no lock).
            let poll_start = Instant::now();
            match connector.poll_batch(max_poll).await {
                Ok(Some(batch)) => {
                    #[allow(clippy::cast_possible_truncation)]
                    let row_count = batch.records.num_rows() as u64;
                    #[allow(clippy::cast_possible_truncation)]
                    let latency_ns = poll_start.elapsed().as_nanos() as u64;
                    metrics_tx.record_poll(row_count, latency_ns);

                    let _ = cp_tx.send(connector.checkpoint());

                    // Send batch to coordinator. If the channel is full,
                    // backpressure naturally stalls this task.
                    if tx
                        .send(SourceEvent::Batch {
                            idx,
                            batch: batch.records,
                        })
                        .await
                        .is_err()
                    {
                        tracing::debug!(source = %name, "Coordinator dropped, stopping");
                        break;
                    }
                }
                Ok(None) => {
                    // No data available right now. For sources without
                    // Notify, the timer fallback already throttles us.
                    // For Notify sources this means a spurious wake.
                }
                Err(e) => {
                    metrics_tx.record_error();
                    let msg = format!("{e}");
                    tracing::warn!(source = %name, error = %e, "Source poll error");
                    if tx
                        .send(SourceEvent::Error { idx, message: msg })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }

        connector
    });

    SourceTaskHandle {
        join,
        shutdown,
        metrics,
        checkpoint_rx: cp_rx,
    }
}

/// Awaits the source's `Notify` handle if present, otherwise falls back to a
/// timer-based sleep. The coordinator's batch window provides the actual
/// micro-batching delay; this function is purely about wake-up signalling.
async fn wait_for_data(notify: Option<&Arc<Notify>>, fallback: std::time::Duration) {
    match notify {
        Some(n) => n.notified().await,
        None => tokio::time::sleep(fallback).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_connectors::testing::MockSourceConnector;

    #[tokio::test]
    async fn test_source_task_produces_batches() {
        let connector = Box::new(MockSourceConnector::with_batches(3, 5));
        let (tx, mut rx) = mpsc::channel(16);
        let config = PipelineConfig {
            fallback_poll_interval: std::time::Duration::from_millis(1),
            batch_window: std::time::Duration::ZERO,
            ..PipelineConfig::default()
        };

        let handle = spawn_source_task(0, "test".to_string(), connector, tx, &config);

        let mut batch_count = 0;
        let mut total_rows = 0u64;
        while let Some(event) = rx.recv().await {
            match event {
                SourceEvent::Batch { batch, .. } => {
                    batch_count += 1;
                    total_rows += batch.num_rows() as u64;
                    if batch_count == 3 {
                        break;
                    }
                }
                SourceEvent::Error { message, .. } => {
                    panic!("Unexpected error: {message}");
                }
                SourceEvent::Exhausted { .. } => break,
            }
        }

        handle.shutdown.notify_one();
        let _ = handle.join.await;

        assert_eq!(batch_count, 3);
        assert_eq!(total_rows, 15);

        let snap = handle.metrics.snapshot();
        assert_eq!(snap.batches, 3);
        assert_eq!(snap.records, 15);
        assert_eq!(snap.errors, 0);
    }

    #[tokio::test]
    async fn test_source_task_shutdown() {
        // Source with many batches — we shut it down before it finishes.
        let connector = Box::new(MockSourceConnector::with_batches(1_000_000, 1));
        let (tx, _rx) = mpsc::channel(4);
        let config = PipelineConfig {
            fallback_poll_interval: std::time::Duration::from_millis(1),
            batch_window: std::time::Duration::ZERO,
            ..PipelineConfig::default()
        };

        let handle = spawn_source_task(0, "test".to_string(), connector, tx, &config);
        // Give it a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        handle.shutdown.notify_one();
        let connector = handle.join.await.unwrap();
        // Should get the connector back for cleanup.
        drop(connector);
    }
}
