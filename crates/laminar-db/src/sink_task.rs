//! Per-sink task that owns a [`SinkConnector`] and processes commands
//! sequentially. Failures and timeouts are reported out of band via the
//! [`SinkEvent`] channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::error::ConnectorError;
use laminar_core::streaming::Producer;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// Default capacity for the sink command channel.
pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// Default periodic flush interval for sink tasks.
pub(crate) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Capacity of the sink event channel. Events are exception-path; a
/// generous bound is fine and avoids unbounded growth on stuck drains.
pub(crate) const SINK_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Out-of-band events emitted by a sink task. Drained once per cycle by
/// the pipeline callback to update metrics and suppress checkpoints.
/// Fields are read via `Debug` when the drain loop logs them.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) enum SinkEvent {
    WriteError {
        sink_id: Arc<str>,
        epoch: u64,
        rows: usize,
        error: String,
    },
    WriteTimeout {
        sink_id: Arc<str>,
        epoch: u64,
        rows: usize,
        timeout: Duration,
    },
    ChannelClosed {
        sink_id: Arc<str>,
    },
}

/// Configuration for spawning a sink task.
pub(crate) struct SinkTaskConfig {
    pub name: String,
    /// Stable identifier carried in [`SinkEvent`]s.
    pub sink_id: Arc<str>,
    pub connector: Box<dyn SinkConnector>,
    pub exactly_once: bool,
    pub channel_capacity: usize,
    pub flush_interval: Duration,
    pub write_timeout: Duration,
    pub event_tx: Producer<SinkEvent>,
}

/// Commands sent to a sink's dedicated task.
pub(crate) enum SinkCommand {
    /// Write a batch to the sink.
    WriteBatch { batch: RecordBatch },
    /// Begin a new epoch (starts Kafka transaction for exactly-once sinks).
    BeginEpoch {
        epoch: u64,
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    /// Explicitly flush buffered data (test-only).
    #[cfg(test)]
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
    RollbackEpoch {
        epoch: u64,
        ack: oneshot::Sender<Result<(), ConnectorError>>,
    },
    /// No-op barrier: acks once every prior command has been processed.
    /// Used by the pipeline to make sure write results have surfaced as
    /// `SinkEvent`s before checkpoint suppression decisions.
    Sync { ack: oneshot::Sender<()> },
    /// Flush + close the connector and exit the task (test-only).
    #[cfg(test)]
    Close,
}

/// Handle for sending commands to a sink's dedicated task.
#[derive(Clone)]
pub(crate) struct SinkTaskHandle {
    name: Arc<str>,
    sink_id: Arc<str>,
    tx: mpsc::Sender<SinkCommand>,
    exactly_once: bool,
    /// Held so `close()` can join the task; implicit shutdown happens
    /// when the command channel drops.
    #[allow(dead_code)]
    task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    /// Used by `write_batch` to emit `ChannelClosed` if the task is gone.
    event_tx: Producer<SinkEvent>,
}

impl SinkTaskHandle {
    /// Spawns a sink task and returns a handle.
    ///
    /// # Panics
    ///
    /// Panics if `config.channel_capacity` is 0.
    pub fn spawn(config: SinkTaskConfig) -> Self {
        assert!(
            config.channel_capacity > 0,
            "sink channel_capacity must be > 0"
        );
        let SinkTaskConfig {
            name,
            sink_id,
            connector,
            exactly_once,
            channel_capacity,
            flush_interval,
            write_timeout,
            event_tx,
        } = config;
        let (tx, rx) = mpsc::channel(channel_capacity);
        let task_sink_id = Arc::clone(&sink_id);
        let task_event_tx = event_tx.clone();
        let task_name = name.clone();
        let handle = tokio::spawn(run_sink_task(SinkTaskInner {
            name: task_name,
            sink_id: task_sink_id,
            sink: connector,
            rx,
            flush_interval,
            write_timeout,
            event_tx: task_event_tx,
        }));

        Self {
            name: Arc::from(name),
            sink_id,
            tx,
            exactly_once,
            task: Arc::new(tokio::sync::Mutex::new(Some(handle))),
            event_tx,
        }
    }

    /// Sends a batch to be written. Backpressures via the bounded channel
    /// when the sink task is behind. On channel-closed, emits
    /// `SinkEvent::ChannelClosed` and returns an error.
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.tx
            .send(SinkCommand::WriteBatch { batch })
            .await
            .map_err(|_| {
                let _ = self.event_tx.try_push(SinkEvent::ChannelClosed {
                    sink_id: Arc::clone(&self.sink_id),
                });
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })
    }

    /// Sends a no-op `Sync` barrier and waits for the sink task to ack.
    /// When it returns, every previously queued command has been processed.
    pub async fn sync(&self) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::Sync { ack: ack_tx })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })?;
        ack_rx.await.map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "sink task '{}' dropped sync acknowledgment",
                self.name
            ))
        })
    }

    /// Begins a new epoch (starts a Kafka transaction for exactly-once sinks).
    ///
    /// Must be called before `write_batch()` for each epoch when using
    /// exactly-once delivery. For at-least-once sinks this is a no-op.
    pub async fn begin_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::BeginEpoch { epoch, ack: ack_tx })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })?;
        ack_rx.await.map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "sink task '{}' dropped begin-epoch acknowledgment",
                self.name
            ))
        })?
    }

    /// Requests an explicit flush and waits for acknowledgment.
    ///
    /// Not called on the normal shutdown path (channel drop triggers
    /// flush implicitly in `run_sink_task`). Available for manual use.
    #[cfg(test)]
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

    /// Abort a failed epoch.
    pub async fn rollback_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(SinkCommand::RollbackEpoch { epoch, ack: ack_tx })
            .await
            .map_err(|_| {
                ConnectorError::ConnectionFailed(format!(
                    "sink task '{}' closed unexpectedly",
                    self.name
                ))
            })?;
        ack_rx.await.map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "sink task '{}' dropped rollback acknowledgment",
                self.name
            ))
        })?
    }

    /// Signals the sink task to close and waits for it to finish (30s timeout).
    ///
    /// Not called on the normal shutdown path — dropping the `SinkTaskHandle`
    /// closes the command channel, which triggers flush+close in `run_sink_task`.
    /// This method is for explicit shutdown when you need to wait for completion.
    #[cfg(test)]
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

/// Owned state for a single sink task. Constructed by `spawn`.
struct SinkTaskInner {
    name: String,
    sink_id: Arc<str>,
    sink: Box<dyn SinkConnector>,
    rx: mpsc::Receiver<SinkCommand>,
    flush_interval: Duration,
    write_timeout: Duration,
    event_tx: Producer<SinkEvent>,
}

/// Main loop for a sink task. Owns the `SinkConnector` exclusively.
/// `epoch_poisoned` is local: `pre_commit`/`commit_epoch` reject poisoned
/// epochs, which the coordinator surfaces and turns into `rollback_sinks`.
#[allow(clippy::too_many_lines)]
async fn run_sink_task(mut inner: SinkTaskInner) {
    let mut flush_timer = tokio::time::interval(inner.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the first immediate tick.
    flush_timer.tick().await;

    let mut current_epoch: u64 = 0;
    let epoch_poisoned = AtomicBool::new(false);

    loop {
        tokio::select! {
            cmd = inner.rx.recv() => {
                let Some(cmd) = cmd else {
                    // Channel closed — shut down gracefully.
                    tracing::debug!(sink = %inner.name, "Sink command channel closed");
                    if let Err(e) = inner.sink.flush().await {
                        tracing::warn!(sink = %inner.name, error = %e,
                            "Sink flush failed on channel close");
                    }
                    if let Err(e) = inner.sink.close().await {
                        tracing::warn!(sink = %inner.name, error = %e,
                            "Sink close failed on channel close");
                    }
                    break;
                };
                match cmd {
                    SinkCommand::WriteBatch { batch } => {
                        let rows = batch.num_rows();
                        match tokio::time::timeout(
                            inner.write_timeout,
                            inner.sink.write_batch(&batch),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => {
                                epoch_poisoned.store(true, Ordering::Release);
                                tracing::warn!(
                                    sink = %inner.name, error = %e, rows,
                                    "Sink write error — epoch poisoned"
                                );
                                let _ = inner.event_tx.try_push(SinkEvent::WriteError {
                                    sink_id: Arc::clone(&inner.sink_id),
                                    epoch: current_epoch,
                                    rows,
                                    error: e.to_string(),
                                });
                            }
                            Err(_elapsed) => {
                                epoch_poisoned.store(true, Ordering::Release);
                                tracing::error!(
                                    sink = %inner.name,
                                    timeout_secs = inner.write_timeout.as_secs(),
                                    rows,
                                    "Sink write I/O timed out — epoch poisoned"
                                );
                                let _ = inner.event_tx.try_push(SinkEvent::WriteTimeout {
                                    sink_id: Arc::clone(&inner.sink_id),
                                    epoch: current_epoch,
                                    rows,
                                    timeout: inner.write_timeout,
                                });
                            }
                        }
                    }
                    SinkCommand::BeginEpoch { epoch, ack } => {
                        let result = inner.sink.begin_epoch(epoch).await;
                        if result.is_ok() {
                            current_epoch = epoch;
                            epoch_poisoned.store(false, Ordering::Release);
                        }
                        let _ = ack.send(result);
                    }
                    #[cfg(test)]
                    SinkCommand::Flush { ack } => {
                        let result = inner.sink.flush().await;
                        let _ = ack.send(result);
                    }
                    SinkCommand::PreCommit { epoch, ack } => {
                        let result = if epoch_poisoned.load(Ordering::Acquire) {
                            Err(ConnectorError::WriteError(
                                "epoch poisoned by prior write failure".into(),
                            ))
                        } else {
                            inner.sink.pre_commit(epoch).await
                        };
                        let _ = ack.send(result);
                    }
                    SinkCommand::CommitEpoch { epoch, ack } => {
                        let result = if epoch_poisoned.load(Ordering::Acquire) {
                            Err(ConnectorError::WriteError(
                                "epoch poisoned by prior write failure".into(),
                            ))
                        } else {
                            inner.sink.commit_epoch(epoch).await
                        };
                        let _ = ack.send(result);
                    }
                    SinkCommand::RollbackEpoch { epoch, ack } => {
                        let result = inner.sink.rollback_epoch(epoch).await;
                        if let Err(ref e) = result {
                            tracing::warn!(
                                sink = %inner.name, epoch, error = %e,
                                "[LDB-6004] Sink rollback failed"
                            );
                        }
                        let _ = ack.send(result);
                    }
                    SinkCommand::Sync { ack } => {
                        let _ = ack.send(());
                    }
                    #[cfg(test)]
                    SinkCommand::Close => {
                        if let Err(e) = inner.sink.flush().await {
                            tracing::warn!(sink = %inner.name, error = %e,
                                "Sink flush failed during close");
                        }
                        if let Err(e) = inner.sink.close().await {
                            tracing::warn!(sink = %inner.name, error = %e,
                                "Sink close failed");
                        }
                        tracing::debug!(sink = %inner.name, "Sink task closed");
                        break;
                    }
                }
            }
            _ = flush_timer.tick() => {
                match tokio::time::timeout(inner.flush_interval, inner.sink.flush()).await {
                    Ok(Err(e)) => {
                        tracing::warn!(sink = %inner.name, error = %e,
                            "Periodic sink flush error");
                    }
                    Err(_elapsed) => {
                        tracing::warn!(sink = %inner.name,
                            "periodic flush timed out — sink may be stuck");
                    }
                    Ok(Ok(())) => {}
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
    use laminar_core::streaming::AsyncConsumer;
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

        fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
            laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
        }
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn spawn_with_defaults(
        name: &str,
        connector: Box<dyn SinkConnector>,
        write_timeout: Duration,
    ) -> (SinkTaskHandle, AsyncConsumer<SinkEvent>) {
        let (event_tx, event_rx) =
            laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
        let handle = SinkTaskHandle::spawn(SinkTaskConfig {
            name: name.into(),
            sink_id: Arc::from(name),
            connector,
            exactly_once: false,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            write_timeout,
            event_tx,
        });
        (handle, event_rx)
    }

    #[tokio::test]
    async fn test_sink_task_write_and_close() {
        let (sink, writes, _flushes) = CountingSink::new();
        let (handle, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));

        handle.write_batch(test_batch()).await.unwrap();
        handle.write_batch(test_batch()).await.unwrap();
        handle.close().await;

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_sink_task_flush() {
        let (sink, _writes, flushes) = CountingSink::new();
        let (handle, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));

        handle.flush().await.unwrap();
        handle.close().await;

        // At least 1 explicit flush + 1 from close
        assert!(flushes.load(Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_sink_task_handle_clone() {
        let (sink, writes, _flushes) = CountingSink::new();
        let (handle1, _events) =
            spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));
        let handle2 = handle1.clone();

        handle1.write_batch(test_batch()).await.unwrap();
        handle2.write_batch(test_batch()).await.unwrap();
        handle1.close().await;

        assert_eq!(writes.load(Ordering::Relaxed), 2);
    }

    /// Sink whose `write_batch` sleeps longer than the configured timeout.
    struct SlowSink {
        schema: arrow::datatypes::SchemaRef,
        sleep: Duration,
    }

    #[async_trait::async_trait]
    impl SinkConnector for SlowSink {
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
            tokio::time::sleep(self.sleep).await;
            Ok(WriteResult::new(1, 0))
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
            laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_sink_task_write_timeout_emits_event() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let sink = SlowSink {
            schema,
            sleep: Duration::from_secs(60),
        };
        let (handle, events) =
            spawn_with_defaults("slow", Box::new(sink), Duration::from_millis(50));

        handle.write_batch(test_batch()).await.unwrap();
        // With paused time, sleep auto-advances when all tasks are
        // blocked on time, firing the sink task's 50ms timeout first.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let event = events
            .try_recv()
            .expect("expected a SinkEvent::WriteTimeout");
        match event {
            SinkEvent::WriteTimeout {
                sink_id,
                rows,
                timeout,
                ..
            } => {
                assert_eq!(&*sink_id, "slow");
                assert_eq!(rows, 3);
                assert_eq!(timeout, Duration::from_millis(50));
            }
            other => panic!("expected WriteTimeout, got {other:?}"),
        }
    }

    /// Verifies channel-closed errors emit a `SinkEvent::ChannelClosed`.
    #[tokio::test]
    async fn test_sink_task_channel_closed_emits_event() {
        let (sink, _writes, _flushes) = CountingSink::new();
        let (handle, events) = spawn_with_defaults("dead", Box::new(sink), Duration::from_secs(5));

        // Force the task to drop by closing the handle, which sends Close
        // and then awaits the join handle. After this, the command channel
        // is closed.
        handle.close().await;

        // The next write_batch should fail and emit ChannelClosed.
        let err = handle.write_batch(test_batch()).await.unwrap_err();
        assert!(matches!(err, ConnectorError::ConnectionFailed(_)));

        let event = events
            .try_recv()
            .expect("expected SinkEvent::ChannelClosed");
        assert!(matches!(event, SinkEvent::ChannelClosed { sink_id } if &*sink_id == "dead"));
    }
}
