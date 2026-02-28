//! Per-sink task that owns a [`SinkConnector`] and processes commands via a
//! bounded channel.
//!
//! This decouples the pipeline loop from individual sink I/O, eliminating
//! `Arc<Mutex>` contention between pipeline writes and checkpoint operations.
//!
//! Each sink runs in its own tokio task and processes commands sequentially:
//! - `WriteBatch` — write a `RecordBatch` to the sink
//! - `Flush` — explicitly flush buffered data
//! - `PreCommit` — checkpoint 2PC phase 1
//! - `CommitEpoch` — checkpoint 2PC phase 2
//! - `RollbackEpoch` — abort a failed epoch
//! - `Close` — flush + close the connector

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::error::ConnectorError;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Default capacity for the sink command channel.
const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// Default periodic flush interval for sink tasks.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Commands sent to a sink's dedicated task.
#[allow(dead_code)]
pub(crate) enum SinkCommand {
    /// Write a batch to the sink.
    WriteBatch { batch: RecordBatch },
    /// Explicitly flush buffered data.
    Flush {
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    /// Checkpoint 2PC phase 1: flush and prepare.
    PreCommit {
        epoch: u64,
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    /// Checkpoint 2PC phase 2: finalize transaction.
    CommitEpoch {
        epoch: u64,
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    /// Abort a failed epoch.
    RollbackEpoch { epoch: u64 },
    /// Flush + close the connector and exit the task.
    Close,
}

/// Handle for sending commands to a sink's dedicated task.
///
/// The handle is cheaply cloneable (wraps `mpsc::Sender` + `Arc` metadata).
/// Both the pipeline loop and the checkpoint coordinator can hold handles
/// to the same sink task without contending on a mutex.
#[derive(Clone)]
pub(crate) struct SinkTaskHandle {
    /// Sink name (for logging).
    name: Arc<str>,
    /// Command channel sender.
    tx: mpsc::Sender<SinkCommand>,
    /// Whether this sink supports exactly-once semantics.
    exactly_once: bool,
    /// Background task join handle (owned by the first handle only).
    #[allow(dead_code)]
    task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
}

impl SinkTaskHandle {
    /// Spawns a new sink task and returns a handle.
    pub fn spawn(name: String, connector: Box<dyn SinkConnector>, exactly_once: bool) -> Self {
        Self::spawn_with_options(
            name,
            connector,
            exactly_once,
            DEFAULT_CHANNEL_CAPACITY,
            DEFAULT_FLUSH_INTERVAL,
        )
    }

    /// Spawns a new sink task with custom channel capacity and flush interval.
    pub fn spawn_with_options(
        name: String,
        connector: Box<dyn SinkConnector>,
        exactly_once: bool,
        channel_capacity: usize,
        flush_interval: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity);
        let task_name = name.clone();
        let handle = tokio::spawn(run_sink_task(task_name, connector, rx, flush_interval));

        Self {
            name: Arc::from(name),
            tx,
            exactly_once,
            task: Arc::new(tokio::sync::Mutex::new(Some(handle))),
        }
    }

    /// Sends a batch to be written. Non-blocking unless the channel is full,
    /// in which case backpressure is applied via the bounded channel.
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.tx
            .send(SinkCommand::WriteBatch { batch })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })
    }

    /// Requests an explicit flush and waits for acknowledgment.
    #[allow(dead_code)]
    pub async fn flush(&self) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::Flush { ack: ack_tx })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })?;
        ack_rx.await.map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "sink task '{}' dropped flush acknowledgment",
                self.name
            ))
        })?
    }

    /// Checkpoint 2PC phase 1: pre-commit.
    pub async fn pre_commit(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::PreCommit { epoch, ack: ack_tx })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })?;
        ack_rx.await.map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "sink task '{}' dropped pre-commit acknowledgment",
                self.name
            ))
        })?
    }

    /// Checkpoint 2PC phase 2: commit epoch.
    pub async fn commit_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::CommitEpoch { epoch, ack: ack_tx })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })?;
        ack_rx.await.map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "sink task '{}' dropped commit acknowledgment",
                self.name
            ))
        })?
    }

    /// Abort a failed epoch (fire-and-forget).
    pub async fn rollback_epoch(&self, epoch: u64) {
        let _ = self.tx.send(SinkCommand::RollbackEpoch { epoch }).await;
    }

    /// Signals the sink task to close and waits for it to finish.
    #[allow(dead_code)]
    pub async fn close(&self) {
        let _ = self.tx.send(SinkCommand::Close).await;
        let mut guard = self.task.lock().await;
        if let Some(handle) = guard.take() {
            let _ = tokio::time::timeout(Duration::from_secs(30), handle).await;
        }
    }

    /// Returns whether this sink supports exactly-once semantics.
    pub fn exactly_once(&self) -> bool {
        self.exactly_once
    }
}

/// Main loop for a sink task.
///
/// Owns the `SinkConnector` exclusively — no external locking needed.
async fn run_sink_task(
    name: String,
    mut sink: Box<dyn SinkConnector>,
    mut rx: mpsc::Receiver<SinkCommand>,
    flush_interval: Duration,
) {
    let mut flush_timer = tokio::time::interval(flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first immediate tick
    flush_timer.tick().await;

    loop {
        tokio::select! {
            cmd = rx.recv() => {
                let Some(cmd) = cmd else {
                    // Channel closed — shut down gracefully
                    tracing::debug!(sink = %name, "Sink command channel closed");
                    if let Err(e) = sink.flush().await {
                        tracing::warn!(sink = %name, error = %e, "Sink flush failed on channel close");
                    }
                    if let Err(e) = sink.close().await {
                        tracing::warn!(sink = %name, error = %e, "Sink close failed on channel close");
                    }
                    break;
                };
                match cmd {
                    SinkCommand::WriteBatch { batch } => {
                        if let Err(e) = sink.write_batch(&batch).await {
                            tracing::warn!(
                                sink = %name,
                                error = %e,
                                "Sink write error"
                            );
                        }
                    }
                    SinkCommand::Flush { ack } => {
                        let result = sink.flush().await;
                        let _ = ack.send(result);
                    }
                    SinkCommand::PreCommit { epoch, ack } => {
                        let result = sink.pre_commit(epoch).await;
                        let _ = ack.send(result);
                    }
                    SinkCommand::CommitEpoch { epoch, ack } => {
                        let result = sink.commit_epoch(epoch).await;
                        let _ = ack.send(result);
                    }
                    SinkCommand::RollbackEpoch { epoch } => {
                        if let Err(e) = sink.rollback_epoch(epoch).await {
                            tracing::warn!(
                                sink = %name,
                                epoch,
                                error = %e,
                                "[LDB-6004] Sink rollback failed"
                            );
                        }
                    }
                    SinkCommand::Close => {
                        if let Err(e) = sink.flush().await {
                            tracing::warn!(
                                sink = %name,
                                error = %e,
                                "Sink flush failed during close"
                            );
                        }
                        if let Err(e) = sink.close().await {
                            tracing::warn!(
                                sink = %name,
                                error = %e,
                                "Sink close failed"
                            );
                        }
                        tracing::debug!(sink = %name, "Sink task closed");
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                if let Err(e) = sink.flush().await {
                    tracing::warn!(
                        sink = %name,
                        error = %e,
                        "Periodic sink flush error"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_connectors::connector::WriteResult;
    use laminar_connectors::health::HealthStatus;
    use laminar_connectors::metrics::ConnectorMetrics;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Minimal mock sink for testing the task infrastructure.
    struct CountingSink {
        writes: Arc<AtomicU64>,
        flushes: Arc<AtomicU64>,
        schema: arrow::datatypes::SchemaRef,
    }

    impl CountingSink {
        fn new() -> (Self, Arc<AtomicU64>, Arc<AtomicU64>) {
            let writes = Arc::new(AtomicU64::new(0));
            let flushes = Arc::new(AtomicU64::new(0));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            (
                Self {
                    writes: Arc::clone(&writes),
                    flushes: Arc::clone(&flushes),
                    schema,
                },
                writes,
                flushes,
            )
        }
    }

    #[async_trait::async_trait]
    impl SinkConnector for CountingSink {
        async fn open(
            &mut self,
            _config: &laminar_connectors::config::ConnectorConfig,
        ) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn write_batch(
            &mut self,
            _batch: &RecordBatch,
        ) -> Result<WriteResult, ConnectorError> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            Ok(WriteResult {
                records_written: 1,
                bytes_written: 0,
            })
        }

        async fn flush(&mut self) -> Result<(), ConnectorError> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn health_check(&self) -> HealthStatus {
            HealthStatus::Healthy
        }

        fn metrics(&self) -> ConnectorMetrics {
            ConnectorMetrics::default()
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    #[tokio::test]
    async fn test_sink_task_write_and_close() {
        let (sink, writes, _flushes) = CountingSink::new();
        let handle = SinkTaskHandle::spawn("test".into(), Box::new(sink), false);

        handle.write_batch(test_batch()).await.unwrap();
        handle.write_batch(test_batch()).await.unwrap();
        handle.close().await;

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_sink_task_flush() {
        let (sink, _writes, flushes) = CountingSink::new();
        let handle = SinkTaskHandle::spawn("test".into(), Box::new(sink), false);

        handle.flush().await.unwrap();
        handle.close().await;

        // At least 1 explicit flush + 1 from close
        assert!(flushes.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_sink_task_handle_clone() {
        let (sink, writes, _flushes) = CountingSink::new();
        let handle1 = SinkTaskHandle::spawn("test".into(), Box::new(sink), false);
        let handle2 = handle1.clone();

        handle1.write_batch(test_batch()).await.unwrap();
        handle2.write_batch(test_batch()).await.unwrap();
        handle1.close().await;

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }
}
