//! Per-sink task that owns a [`SinkConnector`] and processes commands
//! sequentially. Failures and timeouts are reported out of band via the
//! [`SinkEvent`] channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use crossfire::{mpsc, oneshot, AsyncRx, MAsyncTx};
use laminar_connectors::connector::SinkConnector;
use laminar_connectors::error::ConnectorError;
use laminar_core::streaming::Producer;
use tokio::task::JoinHandle;

type SinkCommandTx = MAsyncTx<mpsc::Array<SinkCommand>>;
type SinkCommandRx = AsyncRx<mpsc::Array<SinkCommand>>;

/// Default capacity for the sink command channel.
pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 128;

/// Default periodic flush interval for sink tasks.
pub(crate) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) const SINK_EVENT_CHANNEL_CAPACITY: usize = 1024;

/// Out-of-band events emitted by a sink task; drained once per cycle.
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

pub(crate) struct SinkTaskConfig {
    pub name: String,
    pub sink_id: Arc<str>,
    pub connector: Box<dyn SinkConnector>,
    pub exactly_once: bool,
    pub coordinated_commit: bool,
    pub channel_capacity: usize,
    pub flush_interval: Duration,
    pub write_timeout: Duration,
    pub event_tx: Producer<SinkEvent>,
}

pub(crate) enum SinkCommand {
    WriteBatch {
        batch: RecordBatch,
    },
    BeginEpoch {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    #[cfg(test)]
    Flush {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    PreCommit {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<Option<Vec<u8>>, ConnectorError>>,
    },
    CommitEpoch {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Designated-committer path: aggregate every writer's descriptor for the
    /// epoch into one external commit (coordinated-commit sinks only).
    CommitAggregated {
        epoch: u64,
        descriptors: Vec<Vec<u8>>,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Highest epoch already committed externally, for committer cursor resume.
    CommittedThrough {
        ack: oneshot::TxOneshot<Result<Option<u64>, ConnectorError>>,
    },
    /// `force = false` keeps a healthy sink's pending transactional output —
    /// sources don't rewind on a live abort so those rows must not be discarded.
    RollbackEpoch {
        epoch: u64,
        force: bool,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Acks once all prior commands have been processed.
    Sync {
        ack: oneshot::TxOneshot<()>,
    },
    #[cfg(test)]
    Close,
}

/// Handle for sending commands to a sink's dedicated task.
#[derive(Clone)]
pub(crate) struct SinkTaskHandle {
    name: Arc<str>,
    sink_id: Arc<str>,
    tx: SinkCommandTx,
    exactly_once: bool,
    coordinated_commit: bool,
    // `close()` extracts the handle under the lock then awaits outside it — lock never spans `.await`.
    #[allow(dead_code)]
    task: Arc<parking_lot::Mutex<Option<JoinHandle<()>>>>,
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
            coordinated_commit,
            channel_capacity,
            flush_interval,
            write_timeout,
            event_tx,
        } = config;
        let (tx, rx) = mpsc::bounded_async::<SinkCommand>(channel_capacity);
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
            coordinated_commit,
            task: Arc::new(parking_lot::Mutex::new(Some(handle))),
            event_tx,
        }
    }

    fn closed_err(&self) -> ConnectorError {
        ConnectorError::ConnectionFailed(format!("sink task '{}' closed unexpectedly", self.name))
    }

    fn ack_dropped_err(&self, op: &'static str) -> ConnectorError {
        ConnectorError::ConnectionFailed(format!(
            "sink task '{}' dropped {op} acknowledgment",
            self.name
        ))
    }

    /// Send a batch; backpressures when the sink is behind.
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<(), ConnectorError> {
        self.tx
            .send(SinkCommand::WriteBatch { batch })
            .await
            .map_err(|_| {
                let _ = self.event_tx.try_push(SinkEvent::ChannelClosed {
                    sink_id: Arc::clone(&self.sink_id),
                });
                self.closed_err()
            })
    }

    /// Wait until all previously queued commands have been processed.
    pub async fn sync(&self) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::Sync { ack: ack_tx })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx.await.map_err(|_| self.ack_dropped_err("sync"))
    }

    /// Begin a new epoch; starts a transaction for exactly-once sinks.
    pub async fn begin_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::BeginEpoch { epoch, ack: ack_tx })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx
            .await
            .map_err(|_| self.ack_dropped_err("begin-epoch"))?
    }

    #[cfg(test)]
    pub async fn flush(&self) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::Flush { ack: ack_tx })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx.await.map_err(|_| self.ack_dropped_err("flush"))?
    }

    /// 2PC phase 1: flush and prepare. Returns the connector's commit
    /// descriptor for `coordinated_commit` sinks, else `None`.
    pub async fn pre_commit(&self, epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::PreCommit { epoch, ack: ack_tx })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx
            .await
            .map_err(|_| self.ack_dropped_err("pre-commit"))?
    }

    /// Designated-committer commit of aggregated descriptors for `epoch`.
    pub async fn commit_aggregated(
        &self,
        epoch: u64,
        descriptors: Vec<Vec<u8>>,
    ) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::CommitAggregated {
                epoch,
                descriptors,
                ack: ack_tx,
            })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx
            .await
            .map_err(|_| self.ack_dropped_err("commit-aggregated"))?
    }

    /// Highest epoch already committed externally (committer cursor resume).
    pub async fn committed_through(&self) -> Result<Option<u64>, ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::CommittedThrough { ack: ack_tx })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx
            .await
            .map_err(|_| self.ack_dropped_err("committed-through"))?
    }

    /// 2PC phase 2: finalize the transaction.
    pub async fn commit_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::CommitEpoch { epoch, ack: ack_tx })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx.await.map_err(|_| self.ack_dropped_err("commit"))?
    }

    /// Roll back unconditionally (restart/recovery path).
    pub async fn rollback_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::RollbackEpoch {
                epoch,
                force: true,
                ack: ack_tx,
            })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx.await.map_err(|_| self.ack_dropped_err("rollback"))?
    }

    /// Live coordination failure: keep pending output unless the epoch is poisoned.
    pub async fn abandon_epoch(&self, epoch: u64) -> Result<(), ConnectorError> {
        let (ack_tx, ack_rx) = oneshot::oneshot();
        self.tx
            .send(SinkCommand::RollbackEpoch {
                epoch,
                force: false,
                ack: ack_tx,
            })
            .await
            .map_err(|_| self.closed_err())?;
        ack_rx.await.map_err(|_| self.ack_dropped_err("abandon"))?
    }

    #[cfg(test)]
    pub async fn close(&self) {
        let _ = self.tx.send(SinkCommand::Close).await;
        let handle = self.task.lock().take();
        if let Some(handle) = handle {
            let _ = tokio::time::timeout(Duration::from_secs(30), handle).await;
        }
    }

    pub fn exactly_once(&self) -> bool {
        self.exactly_once
    }

    pub fn coordinated_commit(&self) -> bool {
        self.coordinated_commit
    }
}

struct SinkTaskInner {
    name: String,
    sink_id: Arc<str>,
    sink: Box<dyn SinkConnector>,
    rx: SinkCommandRx,
    flush_interval: Duration,
    write_timeout: Duration,
    event_tx: Producer<SinkEvent>,
}

// `epoch_poisoned` causes `pre_commit`/`commit_epoch` to fail so the coordinator rolls back.
async fn run_sink_task(mut inner: SinkTaskInner) {
    let mut flush_timer = tokio::time::interval(inner.flush_interval);
    flush_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    flush_timer.tick().await; // skip the first immediate tick

    let preserves_pending_on_abandon = inner.sink.capabilities().preserves_pending_on_abandon;
    let mut current_epoch: u64 = 0;
    let epoch_poisoned = AtomicBool::new(false);

    loop {
        tokio::select! {
            cmd = inner.rx.recv() => {
                let Ok(cmd) = cmd else {
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
                let stop = handle_sink_command(
                    &mut inner,
                    cmd,
                    &mut current_epoch,
                    &epoch_poisoned,
                    preserves_pending_on_abandon,
                )
                .await;
                if stop {
                    break;
                }
            }
            _ = flush_timer.tick() => {
                if let Err(e) = inner.sink.flush().await {
                    tracing::warn!(sink = %inner.name, error = %e, "Periodic sink flush error");
                }
            }
        }
    }
}

/// Dispatch a single sink command. Returns `true` when the task should stop.
async fn handle_sink_command(
    inner: &mut SinkTaskInner,
    cmd: SinkCommand,
    current_epoch: &mut u64,
    epoch_poisoned: &AtomicBool,
    preserves_pending_on_abandon: bool,
) -> bool {
    match cmd {
        SinkCommand::WriteBatch { batch } => {
            handle_write_batch(inner, batch, *current_epoch, epoch_poisoned).await;
        }
        SinkCommand::BeginEpoch { epoch, ack } => {
            let result = inner.sink.begin_epoch(epoch).await;
            if result.is_ok() {
                *current_epoch = epoch;
                epoch_poisoned.store(false, Ordering::Release);
            }
            ack.send(result);
        }
        #[cfg(test)]
        SinkCommand::Flush { ack } => {
            let result = inner.sink.flush().await;
            ack.send(result);
        }
        SinkCommand::PreCommit { epoch, ack } => {
            let result = if epoch_poisoned.load(Ordering::Acquire) {
                Err(ConnectorError::WriteError(
                    "epoch poisoned by prior write failure".into(),
                ))
            } else {
                inner.sink.pre_commit(epoch).await
            };
            ack.send(result);
        }
        SinkCommand::CommitEpoch { epoch, ack } => {
            let result = if epoch_poisoned.load(Ordering::Acquire) {
                Err(ConnectorError::WriteError(
                    "epoch poisoned by prior write failure".into(),
                ))
            } else {
                inner.sink.commit_epoch(epoch).await
            };
            ack.send(result);
        }
        SinkCommand::CommitAggregated {
            epoch,
            descriptors,
            ack,
        } => {
            let result = match inner.sink.as_coordinated_committer() {
                Some(committer) => committer.commit_aggregated(epoch, descriptors).await,
                None => Err(ConnectorError::InvalidState {
                    expected: "coordinated committer".into(),
                    actual: format!("sink '{}' is not coordinated", inner.name),
                }),
            };
            ack.send(result);
        }
        SinkCommand::CommittedThrough { ack } => {
            let result = match inner.sink.as_coordinated_committer() {
                Some(committer) => committer.committed_through().await,
                None => Ok(None),
            };
            ack.send(result);
        }
        SinkCommand::RollbackEpoch { epoch, force, ack } => {
            let result = handle_rollback_epoch(
                inner,
                epoch,
                force,
                preserves_pending_on_abandon,
                epoch_poisoned,
            )
            .await;
            ack.send(result);
        }
        SinkCommand::Sync { ack } => {
            ack.send(());
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
            return true;
        }
    }
    false
}

/// Write a batch under the configured timeout; poisons the epoch on error.
async fn handle_write_batch(
    inner: &mut SinkTaskInner,
    batch: RecordBatch,
    current_epoch: u64,
    epoch_poisoned: &AtomicBool,
) {
    let rows = batch.num_rows();
    match tokio::time::timeout(inner.write_timeout, inner.sink.write_batch(&batch)).await {
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

/// Roll back (or intentionally keep) a coordinated epoch's pending output.
async fn handle_rollback_epoch(
    inner: &mut SinkTaskInner,
    epoch: u64,
    force: bool,
    preserves_pending_on_abandon: bool,
    epoch_poisoned: &AtomicBool,
) -> Result<(), ConnectorError> {
    if !(force || !preserves_pending_on_abandon || epoch_poisoned.load(Ordering::Acquire)) {
        tracing::debug!(
            sink = %inner.name, epoch,
            "coordination rollback — keeping pending sink \
             output for the next epoch's commit"
        );
        return Ok(());
    }
    let result = inner.sink.rollback_epoch(epoch).await;
    if let Err(ref e) = result {
        tracing::warn!(
            sink = %inner.name, epoch, error = %e,
            "[LDB-6004] Sink rollback failed"
        );
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_connectors::connector::WriteResult;
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
            coordinated_commit: false,
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

    /// Records `rollback_epoch` calls; `preserves` controls the
    /// abandon capability, `fail_writes` poisons the epoch.
    struct RollbackProbeSink {
        rollbacks: Arc<AtomicU64>,
        preserves: bool,
        fail_writes: bool,
        schema: arrow::datatypes::SchemaRef,
    }

    impl RollbackProbeSink {
        fn new(preserves: bool, fail_writes: bool) -> (Self, Arc<AtomicU64>) {
            let rollbacks = Arc::new(AtomicU64::new(0));
            let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
            (
                Self {
                    rollbacks: Arc::clone(&rollbacks),
                    preserves,
                    fail_writes,
                    schema,
                },
                rollbacks,
            )
        }
    }

    #[async_trait::async_trait]
    impl SinkConnector for RollbackProbeSink {
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
            if self.fail_writes {
                Err(ConnectorError::WriteError("synthetic".into()))
            } else {
                Ok(WriteResult {
                    records_written: 1,
                    bytes_written: 0,
                })
            }
        }

        async fn rollback_epoch(&mut self, _epoch: u64) -> Result<(), ConnectorError> {
            self.rollbacks.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn schema(&self) -> arrow::datatypes::SchemaRef {
            Arc::clone(&self.schema)
        }

        fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
            let caps = laminar_connectors::connector::SinkConnectorCapabilities::new(
                Duration::from_secs(5),
            );
            if self.preserves {
                caps.with_preserves_pending_on_abandon()
            } else {
                caps
            }
        }
    }

    /// The abandon/rollback decision matrix: a healthy sink that
    /// declares `preserves_pending_on_abandon` keeps its pending output
    /// (no connector rollback); without the capability — or once the
    /// epoch is poisoned — the connector rolls back. Forced rollback
    /// (restart/recovery) always rolls back.
    #[tokio::test]
    async fn abandon_rollback_decision_matrix() {
        // Preserving + healthy → abandon keeps pending output.
        let (sink, rollbacks) = RollbackProbeSink::new(true, false);
        let (handle, _ev) = spawn_with_defaults("p", Box::new(sink), Duration::from_secs(5));
        handle.abandon_epoch(1).await.unwrap();
        assert_eq!(
            rollbacks.load(Ordering::Relaxed),
            0,
            "preserving sink kept output"
        );
        // …but a forced rollback still rolls back.
        handle.rollback_epoch(1).await.unwrap();
        assert_eq!(
            rollbacks.load(Ordering::Relaxed),
            1,
            "forced rollback is unconditional"
        );
        handle.close().await;

        // Non-preserving + healthy → abandon rolls back.
        let (sink, rollbacks) = RollbackProbeSink::new(false, false);
        let (handle, _ev) = spawn_with_defaults("np", Box::new(sink), Duration::from_secs(5));
        handle.abandon_epoch(1).await.unwrap();
        assert_eq!(
            rollbacks.load(Ordering::Relaxed),
            1,
            "non-preserving sink rolled back"
        );
        handle.close().await;

        // Preserving + poisoned epoch → abandon rolls back anyway.
        let (sink, rollbacks) = RollbackProbeSink::new(true, true);
        let (handle, _ev) = spawn_with_defaults("poison", Box::new(sink), Duration::from_secs(5));
        handle.write_batch(test_batch()).await.unwrap();
        handle.sync().await.unwrap(); // write error lands → epoch poisoned
        handle.abandon_epoch(1).await.unwrap();
        assert_eq!(
            rollbacks.load(Ordering::Relaxed),
            1,
            "poisoned epoch forces a real rollback even on a preserving sink",
        );
        handle.close().await;
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
