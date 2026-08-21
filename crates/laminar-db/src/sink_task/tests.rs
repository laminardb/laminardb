use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use crossfire::{mpsc, oneshot};
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, ConnectorTaskTracker, CoordinatedCommitBatch,
    CoordinatedCommitContext, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitter, SinkConnector, SinkContract,
};
use laminar_connectors::error::ConnectorError;
use tokio::time::Instant;

use super::actor::{
    spawn_sink_actor, supervise_sink_task, OwnedSinkTask, SinkActorLifetime, SinkActorState,
    SinkCloseState,
};
use super::close::{spawn_sink_close_driver_future, wait_for_sink_close};
#[cfg(feature = "cluster")]
use super::operation::{await_connector_operation_fenced, ConnectorOperationOutcome};
use super::operation::{bounded_connector_operation, operation_deadline};
use super::protocol::{SinkCommand, SinkOperation, SINK_CLOSE_TIMEOUT};
use super::*;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use laminar_connectors::connector::{
    ConnectorTaskOwner, SinkConsistency, SinkInputMode, SinkTopology, WriteResult,
};
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;
use laminar_core::streaming::AsyncConsumer;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "cluster")]
use crossfire::AsyncTxTrait as _;

fn supervise_test_actor<F>(
    actor: F,
    terminal_tasks: Option<ConnectorTaskTracker>,
) -> (OwnedSinkTask, Arc<SinkActorState>)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let runtime = tokio::runtime::Handle::current();
    let actor_state = Arc::new(SinkActorState::new());
    let actor = spawn_sink_actor(&runtime, actor, Arc::clone(&actor_state));
    let task = supervise_sink_task(actor, terminal_tasks, Arc::clone(&actor_state), &runtime);
    (task, actor_state)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_blocking_completion_is_a_deadline_and_retires_the_generation() {
    let deadline = Instant::now() + Duration::from_millis(5);
    let (result, retire) = bounded_connector_operation(
        "late-completion",
        "flush",
        deadline,
        ConnectorCancellationPolicy::RetireConnector,
        #[cfg(feature = "cluster")]
        None,
        || async {
            std::thread::sleep(Duration::from_millis(25));
            Ok::<_, ConnectorError>(())
        },
    )
    .await;

    assert!(result.unwrap_err().to_string().contains("deadline"));
    assert!(retire);
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_blocking_completion_cannot_cross_the_process_fence() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let node = laminar_core::state::NodeId(91);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = ClusterController::new(node, kv, None, members_rx);
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();

    let outcome = await_connector_operation_fenced(
        &controller,
        Instant::now() + Duration::from_millis(5),
        async {
            std::thread::sleep(Duration::from_millis(25));
            7_u64
        },
    )
    .await;
    assert!(matches!(outcome, ConnectorOperationOutcome::Deadline));
}

/// Minimal mock sink for testing the task infrastructure.
struct CountingSink {
    writes: Arc<AtomicU64>,
    flushes: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

struct AlternatingTrackerSink {
    _first_owner: ConnectorTaskOwner,
    _second_owner: ConnectorTaskOwner,
    first_tracker: ConnectorTaskTracker,
    second_tracker: ConnectorTaskTracker,
    tracker_calls: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for AlternatingTrackerSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        let call = self.tracker_calls.fetch_add(1, Ordering::SeqCst);
        Some(if call == 0 {
            self.first_tracker.clone()
        } else {
            self.second_tracker.clone()
        })
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(1)
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
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

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
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

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[cfg(feature = "cluster")]
struct InFlightWriteGuard {
    cancellations: Arc<AtomicU64>,
    completed: bool,
}

#[cfg(feature = "cluster")]
impl Drop for InFlightWriteGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellations.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[cfg(feature = "cluster")]
struct AuthorityBlockingSink {
    policy: ConnectorCancellationPolicy,
    writes: Arc<AtomicU64>,
    flushes: Arc<AtomicU64>,
    completions: Arc<AtomicU64>,
    cancellations: Arc<AtomicU64>,
    gate: Arc<tokio::sync::Semaphore>,
    schema: arrow::datatypes::SchemaRef,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl SinkConnector for AuthorityBlockingSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        self.policy
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let mut guard = InFlightWriteGuard {
            cancellations: Arc::clone(&self.cancellations),
            completed: false,
        };
        let permit = self.gate.acquire().await.unwrap();
        permit.forget();
        guard.completed = true;
        self.completions.fetch_add(1, Ordering::SeqCst);
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(60)
    }
}

#[cfg(feature = "cluster")]
struct AuthoritySinkProbe {
    handle: SinkTaskHandle,
    events: AsyncConsumer<SinkEvent>,
    controller: Arc<ClusterController>,
    writes: Arc<AtomicU64>,
    flushes: Arc<AtomicU64>,
    completions: Arc<AtomicU64>,
    cancellations: Arc<AtomicU64>,
}

#[cfg(feature = "cluster")]
fn authority_sink_probe(node: u64, policy: ConnectorCancellationPolicy) -> AuthoritySinkProbe {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let node_id = laminar_core::state::NodeId(node);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node_id));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node_id, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let writes = Arc::new(AtomicU64::new(0));
    let flushes = Arc::new(AtomicU64::new(0));
    let completions = Arc::new(AtomicU64::new(0));
    let cancellations = Arc::new(AtomicU64::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let sink = AuthorityBlockingSink {
        policy,
        writes: Arc::clone(&writes),
        flushes: Arc::clone(&flushes),
        completions: Arc::clone(&completions),
        cancellations: Arc::clone(&cancellations),
        gate: Arc::clone(&gate),
        schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
    };
    let (event_tx, events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "authority-probe".into(),
        sink_id: Arc::from("authority-probe"),
        connector: Box::new(sink),
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: 8,
        flush_interval: Duration::from_secs(60),
        write_timeout: Duration::from_secs(60),
        event_tx,
        terminal_tasks: None,
        process_authority: Some(Arc::clone(&controller)),
    });
    AuthoritySinkProbe {
        handle,
        events,
        controller,
        writes,
        flushes,
        completions,
        cancellations,
    }
}

#[cfg(feature = "cluster")]
async fn wait_for_actor_queue(handle: &SinkTaskHandle, expected: usize) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.tx.len() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sink command did not reach the actor queue");
}

#[cfg(feature = "cluster")]
async fn wait_for_actor_exit(handle: &SinkTaskHandle) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while !handle.actor_state.finished.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sink actor did not terminate after process lease loss");
}

#[cfg(feature = "cluster")]
async fn wait_for_connector_write(writes: &AtomicU64) {
    tokio::time::timeout(Duration::from_secs(1), async {
        while writes.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sink connector write did not start");
}

#[cfg(feature = "cluster")]
async fn receive_authority_write_error(events: &mut AsyncConsumer<SinkEvent>) {
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("sink did not report process authority loss")
        .expect("sink event channel closed unexpectedly");
    assert!(matches!(
        event,
        SinkEvent::WriteError {
            sink_id,
            epoch: 0,
            rows: 3,
            error,
        } if &*sink_id == "authority-probe" && error.contains("process authority")
    ));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_wakes_idle_sink_actor() {
    let probe = authority_sink_probe(31, ConnectorCancellationPolicy::CancelSafe);

    probe.controller.fence_process_lease();
    wait_for_actor_exit(&probe.handle).await;

    assert_eq!(probe.writes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
    assert_eq!(probe.cancellations.load(Ordering::SeqCst), 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_cancels_cancel_safe_write_and_rejects_queued_commands() {
    let mut probe = authority_sink_probe(32, ConnectorCancellationPolicy::CancelSafe);
    probe.handle.write_batch(test_batch()).await.unwrap();
    wait_for_connector_write(&probe.writes).await;

    probe.handle.write_batch(test_batch()).await.unwrap();
    let flush_handle = probe.handle.clone();
    let queued_flush = tokio::spawn(async move { flush_handle.flush().await });
    wait_for_actor_queue(&probe.handle, 2).await;

    probe.controller.fence_process_lease();
    wait_for_actor_exit(&probe.handle).await;

    let error = queued_flush.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("process authority"), "{error}");
    receive_authority_write_error(&mut probe.events).await;
    receive_authority_write_error(&mut probe.events).await;
    assert_eq!(probe.writes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
    assert_eq!(probe.cancellations.load(Ordering::SeqCst), 1);
    assert!(probe.handle.epoch_poisoned.load(Ordering::Acquire));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn process_lease_loss_retires_started_write_and_rejects_queued_commands() {
    let mut probe = authority_sink_probe(33, ConnectorCancellationPolicy::RetireConnector);
    probe.handle.write_batch(test_batch()).await.unwrap();
    wait_for_connector_write(&probe.writes).await;

    probe.handle.write_batch(test_batch()).await.unwrap();
    let flush_handle = probe.handle.clone();
    let queued_flush = tokio::spawn(async move { flush_handle.flush().await });
    wait_for_actor_queue(&probe.handle, 2).await;

    probe.controller.fence_process_lease();
    wait_for_actor_exit(&probe.handle).await;

    let error = queued_flush.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("process authority"), "{error}");
    receive_authority_write_error(&mut probe.events).await;
    receive_authority_write_error(&mut probe.events).await;
    assert_eq!(probe.writes.load(Ordering::SeqCst), 1);
    assert_eq!(probe.flushes.load(Ordering::SeqCst), 0);
    assert_eq!(probe.completions.load(Ordering::SeqCst), 0);
    assert_eq!(probe.cancellations.load(Ordering::SeqCst), 1);
    assert!(probe.handle.epoch_poisoned.load(Ordering::Acquire));
}

struct ShutdownFailureSink {
    fail_flush: bool,
    fail_close: bool,
    closes: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

impl ShutdownFailureSink {
    fn new(fail_flush: bool, fail_close: bool) -> (Self, Arc<AtomicU64>) {
        let closes = Arc::new(AtomicU64::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        (
            Self {
                fail_flush,
                fail_close,
                closes: Arc::clone(&closes),
                schema,
            },
            closes,
        )
    }
}

#[async_trait::async_trait]
impl SinkConnector for ShutdownFailureSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if self.fail_flush {
            Err(ConnectorError::WriteError("injected shutdown flush".into()))
        } else {
            Ok(())
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        if self.fail_close {
            Err(ConnectorError::ConnectionFailed(
                "injected connector close".into(),
            ))
        } else {
            Ok(())
        }
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

struct FailFirstFlushSink {
    flushes: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for FailFirstFlushSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        let call = self.flushes.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Err(ConnectorError::WriteError(
                "injected deferred acknowledgement failure".into(),
            ))
        } else {
            Ok(())
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

fn test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap()
}

fn at_least_once_contract() -> SinkContract {
    SinkContract::new(
        SinkConsistency::DurableAtLeastOnce,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    )
}

fn checkpoint_committable_contract() -> SinkContract {
    SinkContract::new(
        SinkConsistency::CheckpointCommittable,
        SinkTopology::MultiWriter,
        SinkInputMode::AppendOnly,
    )
}

fn spawn_fail_first_periodic_flush(
    requires_recovery_on_error: bool,
) -> (SinkTaskHandle, AsyncConsumer<SinkEvent>, Arc<AtomicU64>) {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let flushes = Arc::new(AtomicU64::new(0));
    let sink = FailFirstFlushSink {
        flushes: Arc::clone(&flushes),
        schema,
    };
    let (event_tx, event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "deferred-ack".into(),
        sink_id: Arc::from("deferred-ack"),
        connector: Box::new(sink),
        contract: at_least_once_contract(),
        requires_recovery_on_error,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_secs(5),
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    (handle, event_rx, flushes)
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
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout,
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    (handle, event_rx)
}

#[tokio::test]
async fn test_sink_task_write_and_close() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (handle, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));

    handle.write_batch(test_batch()).await.unwrap();
    handle.write_batch(test_batch()).await.unwrap();
    handle.close().await.unwrap();

    assert_eq!(writes.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn spawn_uses_the_exact_captured_connector_tracker() {
    let (first_owner, first_tracker) = ConnectorTaskOwner::new();
    let first_guard = first_owner.track().expect("first tracker generation");
    let (second_owner, second_tracker) = ConnectorTaskOwner::new();
    let second_guard = second_owner.track().expect("second tracker generation");
    let tracker_calls = Arc::new(AtomicU64::new(0));
    let connector: Box<dyn SinkConnector> = Box::new(AlternatingTrackerSink {
        _first_owner: first_owner,
        _second_owner: second_owner,
        first_tracker,
        second_tracker,
        tracker_calls: Arc::clone(&tracker_calls),
        schema: Arc::new(Schema::empty()),
    });
    let terminal_tasks = connector.terminal_task_tracker();
    let (event_tx, _events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);

    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "exact-tracker".into(),
        sink_id: Arc::from("exact-tracker"),
        connector,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);

    let close = tokio::spawn({
        let handle = handle.clone();
        async move { handle.close().await }
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !handle.actor_state.finished.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("sink actor did not finish close");
    assert!(!close.is_finished());

    drop(first_guard);
    tokio::time::timeout(Duration::from_secs(1), close)
        .await
        .expect("captured tracker did not release terminal supervision")
        .expect("close task panicked")
        .expect("sink close failed");
    assert_eq!(tracker_calls.load(Ordering::SeqCst), 1);
    drop(second_guard);
}

#[tokio::test]
async fn close_linearizes_before_a_waiting_write_admission() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (handle, _events) =
        spawn_with_defaults("close-race", Box::new(sink), Duration::from_secs(5));

    let admission = handle.admission.lock().await;
    let close_handle = handle.clone();
    let close = tokio::spawn(async move { close_handle.close().await });
    tokio::task::yield_now().await;
    let write_handle = handle.clone();
    let write = tokio::spawn(async move { write_handle.write_batch(test_batch()).await });
    tokio::task::yield_now().await;
    drop(admission);

    close.await.unwrap().unwrap();
    assert!(write.await.unwrap().is_err());
    assert_eq!(
        writes.load(Ordering::Acquire),
        0,
        "a write queued behind Close must never be acknowledged"
    );
}

#[tokio::test]
async fn repeated_close_is_idempotent() {
    let (sink, _writes, _flushes) = CountingSink::new();
    let (handle, _events) =
        spawn_with_defaults("repeated-close", Box::new(sink), Duration::from_secs(5));

    handle.close().await.unwrap();
    handle.close().await.unwrap();
}

struct GatedCloseSink {
    close_started: Arc<tokio::sync::Semaphore>,
    close_release: Arc<tokio::sync::Semaphore>,
    closes: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for GatedCloseSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.close_started.add_permits(1);
        self.close_release.acquire().await.unwrap().forget();
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test]
async fn cancelling_close_after_enqueue_keeps_one_terminal_driver() {
    let close_started = Arc::new(tokio::sync::Semaphore::new(0));
    let close_release = Arc::new(tokio::sync::Semaphore::new(0));
    let closes = Arc::new(AtomicU64::new(0));
    let sink = GatedCloseSink {
        close_started: Arc::clone(&close_started),
        close_release: Arc::clone(&close_release),
        closes: Arc::clone(&closes),
        schema: Arc::new(Schema::empty()),
    };
    let (handle, _events) =
        spawn_with_defaults("cancel-close-ack", Box::new(sink), Duration::from_secs(5));

    let caller_handle = handle.clone();
    let caller = tokio::spawn(async move { caller_handle.close().await });
    close_started.acquire().await.unwrap().forget();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(handle.has_unresolved_task());

    close_release.add_permits(1);
    handle
        .close()
        .await
        .expect("a retry observes the original driver's terminal result");
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_close_before_admission_does_not_publish_a_partial_close() {
    let (sink, _writes, _flushes) = CountingSink::new();
    let (handle, _events) = spawn_with_defaults(
        "cancel-close-admission",
        Box::new(sink),
        Duration::from_secs(5),
    );
    let admission = handle.admission.lock().await;

    let caller_handle = handle.clone();
    let caller = tokio::spawn(async move { caller_handle.close().await });
    tokio::task::yield_now().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(!handle.closing.load(Ordering::Acquire));

    drop(admission);
    handle.close().await.unwrap();
}

struct GatedWriteSink {
    write_started: tokio::sync::mpsc::UnboundedSender<()>,
    write_release: Arc<tokio::sync::Semaphore>,
    closes: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for GatedWriteSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let _ = self.write_started.send(());
        self.write_release.acquire().await.unwrap().forget();
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test]
async fn cancelling_close_while_enqueue_is_full_preserves_the_actor_fence() {
    let (write_started_tx, mut write_started_rx) = tokio::sync::mpsc::unbounded_channel();
    let write_release = Arc::new(tokio::sync::Semaphore::new(0));
    let closes = Arc::new(AtomicU64::new(0));
    let sink = GatedWriteSink {
        write_started: write_started_tx,
        write_release: Arc::clone(&write_release),
        closes: Arc::clone(&closes),
        schema: Arc::new(Schema::empty()),
    };
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "cancel-close-enqueue".into(),
        sink_id: Arc::from("cancel-close-enqueue"),
        connector: Box::new(sink),
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: 1,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    handle.write_batch(test_batch()).await.unwrap();
    write_started_rx.recv().await.unwrap();
    handle.write_batch(test_batch()).await.unwrap();

    let caller_handle = handle.clone();
    let caller = tokio::spawn(async move { caller_handle.close().await });
    while !handle.closing.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(handle.has_unresolved_task());

    write_release.add_permits(2);
    handle.close().await.unwrap();
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_sink_task_flush() {
    let (sink, _writes, flushes) = CountingSink::new();
    let (handle, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));

    handle.flush().await.unwrap();
    handle.close().await.unwrap();

    // At least 1 explicit flush + 1 from close
    assert!(flushes.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(start_paused = true)]
async fn configured_interval_bounds_low_volume_buffer_residence() {
    let (sink, _writes, flushes) = CountingSink::new();
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "low-volume".into(),
        sink_id: Arc::from("low-volume"),
        connector: Box::new(sink),
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_millis(250),
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    handle.write_batch(test_batch()).await.unwrap();
    handle.sync().await.unwrap();
    tokio::time::advance(Duration::from_millis(249)).await;
    tokio::task::yield_now().await;
    assert_eq!(flushes.load(Ordering::Acquire), 0);

    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(flushes.load(Ordering::Acquire), 1);

    handle.close().await.unwrap();
}

#[tokio::test]
async fn close_ack_reports_flush_and_connector_failures_after_attempting_both() {
    for (fail_flush, fail_close, expected) in [
        (true, false, &["shutdown flush"][..]),
        (false, true, &["connector close"][..]),
        (true, true, &["shutdown flush", "connector close"][..]),
    ] {
        let (sink, closes) = ShutdownFailureSink::new(fail_flush, fail_close);
        let (handle, _events) =
            spawn_with_defaults("failing", Box::new(sink), Duration::from_secs(5));

        let error = handle.close().await.unwrap_err().to_string();
        for needle in expected {
            assert!(error.contains(needle), "missing '{needle}' in '{error}'");
        }
        let repeated = handle.close().await.unwrap_err().to_string();
        for needle in expected {
            assert!(
                repeated.contains(needle),
                "repeated close lost terminal failure '{needle}' in '{repeated}'"
            );
        }
        assert_eq!(
            closes.load(Ordering::SeqCst),
            1,
            "connector close must run exactly once and persist its terminal result"
        );
    }
}

struct PanicCloseSink {
    closes: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for PanicCloseSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        panic!("injected close panic");
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test]
async fn close_panic_is_terminal_and_persisted_without_a_second_command() {
    let closes = Arc::new(AtomicU64::new(0));
    let sink = PanicCloseSink {
        closes: Arc::clone(&closes),
        schema: Arc::new(Schema::empty()),
    };
    let (handle, _events) =
        spawn_with_defaults("panic-close", Box::new(sink), Duration::from_secs(5));

    let first = handle.close().await.unwrap_err().to_string();
    let second = handle.close().await.unwrap_err().to_string();
    assert!(first.contains("close") || first.contains("join"), "{first}");
    assert_eq!(first, second);
    assert_eq!(closes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exactly_once_task_never_periodically_or_implicitly_flushes() {
    let (sink, _writes, flushes) = CountingSink::new();
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "eo".into(),
        sink_id: Arc::from("eo"),
        connector: Box::new(sink),
        contract: checkpoint_committable_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: Duration::from_millis(5),
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    handle.close().await.unwrap();

    assert_eq!(
        flushes.load(Ordering::Relaxed),
        0,
        "exactly-once data may only flush through checkpoint protocol commands"
    );
}

#[tokio::test(start_paused = true)]
async fn durable_at_least_once_output_has_no_checkpoint_epoch_gate() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "alo-ungated".into(),
        sink_id: Arc::from("alo-ungated"),
        connector: Box::new(sink),
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_millis(25),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    let started = Instant::now();
    assert_eq!(
        handle
            .wait_for_write_gate_until(Some(started + Duration::from_secs(5)))
            .await
            .unwrap(),
        None
    );
    assert_eq!(Instant::now(), started);
    handle.write_batch(test_batch()).await.unwrap();
    handle.sync().await.unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn sealed_write_waits_through_begin_until_successor_group_publication() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "epoch-gated".into(),
        sink_id: Arc::from("epoch-gated"),
        connector: Box::new(sink),
        contract: checkpoint_committable_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_millis(25),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    handle.begin_epoch_until(7, deadline).await.unwrap();
    let initial = handle.begun_epoch_admission(7).unwrap();
    handle.publish_open_epoch(initial).unwrap();
    handle.write_batch(test_batch()).await.unwrap();
    handle.sync().await.unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 1);

    let sealed = handle.seal_epoch_until(initial, deadline).await.unwrap();
    let pending = tokio::spawn({
        let handle = handle.clone();
        async move { handle.write_batch(test_batch()).await }
    });
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    tokio::time::advance(Duration::from_secs(1)).await;
    tokio::task::yield_now().await;
    assert!(
        !pending.is_finished(),
        "checkpoint gate wait must not consume the 25ms connector write budget"
    );

    // A connector Begin ack is preparation only. The group publication is the write boundary.
    handle.begin_epoch_until(19, deadline).await.unwrap();
    tokio::task::yield_now().await;
    assert!(!pending.is_finished());
    assert_eq!(writes.load(Ordering::SeqCst), 1);
    let successor = handle.begun_epoch_admission(19).unwrap();
    assert_eq!(successor.generation, sealed.generation);
    handle.publish_open_epoch(successor).unwrap();

    pending.await.unwrap().unwrap();
    handle.sync().await.unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 2);
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn actor_rejects_private_write_after_successful_pre_commit() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (event_tx, mut events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "prepared-write".into(),
        sink_id: Arc::from("prepared-write"),
        connector: Box::new(sink),
        contract: checkpoint_committable_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    let deadline = Instant::now() + Duration::from_secs(5);
    handle.begin_epoch_until(23, deadline).await.unwrap();
    let admission = handle.begun_epoch_admission(23).unwrap();
    handle.publish_open_epoch(admission).unwrap();
    handle.pre_commit_until(23, deadline).await.unwrap();

    handle
        .tx
        .send_with_timer(
            SinkCommand {
                deadline,
                operation: SinkOperation::WriteBatch {
                    epoch: Some(admission),
                    batch: test_batch(),
                },
            },
            tokio::time::sleep_until(deadline),
        )
        .await
        .unwrap();
    handle.sync().await.unwrap();

    assert_eq!(writes.load(Ordering::SeqCst), 0);
    assert!(matches!(
        events.recv().await.unwrap(),
        SinkEvent::WriteError {
            epoch: 23,
            rows: 3,
            error,
            ..
        } if error.contains("Prepared")
    ));
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn periodic_flush_failure_poison_rejects_durable_checkpoint_flush() {
    let (handle, mut events, flushes) = spawn_fail_first_periodic_flush(true);

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let event = events.recv().await.unwrap();
    assert!(matches!(
        event,
        SinkEvent::FlushError {
            sink_id,
            epoch: 0,
            operation: "periodic flush",
            error,
        } if &*sink_id == "deferred-ack"
            && error.contains("deferred acknowledgement failure")
    ));

    let error = handle.flush().await.unwrap_err().to_string();
    assert!(error.contains("poisoned"), "{error}");
    assert_eq!(
        flushes.load(Ordering::SeqCst),
        1,
        "checkpoint flush must reject the sticky failure without observing a false-empty queue"
    );
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn best_effort_periodic_flush_failure_does_not_permanently_poison_checkpoints() {
    let (handle, mut events, flushes) = spawn_fail_first_periodic_flush(false);

    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    assert!(matches!(
        events.recv().await.unwrap(),
        SinkEvent::FlushError {
            operation: "periodic flush",
            ..
        }
    ));

    handle.flush().await.unwrap();
    assert_eq!(
        flushes.load(Ordering::SeqCst),
        2,
        "best-effort policy must report loss but allow a later state checkpoint to recover"
    );
    handle.close().await.unwrap();
}

#[tokio::test]
async fn test_sink_task_handle_clone() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (handle1, _events) = spawn_with_defaults("test", Box::new(sink), Duration::from_secs(5));
    let handle2 = handle1.clone();

    handle1.write_batch(test_batch()).await.unwrap();
    handle2.write_batch(test_batch()).await.unwrap();
    handle1.close().await.unwrap();

    assert_eq!(writes.load(Ordering::Relaxed), 2);
}

/// Records `rollback_epoch` calls.
struct RollbackProbeSink {
    rollbacks: Arc<AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

impl RollbackProbeSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let rollbacks = Arc::new(AtomicU64::new(0));
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        (
            Self {
                rollbacks: Arc::clone(&rollbacks),
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

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult {
            records_written: 1,
            bytes_written: 0,
        })
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

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

/// Without a durable external decision, rollback discards local staged output.
#[tokio::test]
async fn rollback_discards_staged_output() {
    let (sink, rollbacks) = RollbackProbeSink::new();
    let (handle, _ev) = spawn_with_defaults("rollback", Box::new(sink), Duration::from_secs(5));
    handle.rollback_epoch(1).await.unwrap();
    assert_eq!(
        rollbacks.load(Ordering::Relaxed),
        1,
        "rollback must discard staged output"
    );
    handle.close().await.unwrap();
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

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        tokio::time::sleep(self.sleep).await;
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test(start_paused = true)]
async fn test_sink_task_write_timeout_emits_event() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let sink = SlowSink {
        schema,
        sleep: Duration::from_secs(60),
    };
    let (handle, events) = spawn_with_defaults("slow", Box::new(sink), Duration::from_millis(50));

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

struct LateBlockingWriteSink {
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for LateBlockingWriteSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        std::thread::sleep(Duration::from_millis(25));
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_millis(5)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_blocking_write_is_a_timeout_and_retires_the_actor() {
    let sink = LateBlockingWriteSink {
        schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
    };
    let (handle, mut events) =
        spawn_with_defaults("late-write", Box::new(sink), Duration::from_millis(5));

    handle.write_batch(test_batch()).await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("late write did not report its deadline")
        .expect("sink event channel closed unexpectedly");

    assert!(matches!(
        event,
        SinkEvent::WriteTimeout { sink_id, rows: 3, .. } if &*sink_id == "late-write"
    ));
    assert!(
        handle
            .wait_terminal_until(Instant::now() + Duration::from_secs(1))
            .await,
        "late write did not retire its connector generation"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_late_blocking_write_cannot_cross_its_deadline() {
    use laminar_core::cluster::control::{ClusterKv, InMemoryKv, LeaseDeadline};

    let node = laminar_core::state::NodeId(92);
    let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
    let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(node, kv, None, members_rx));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
        .unwrap();
    let sink = LateBlockingWriteSink {
        schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
    };
    let (event_tx, mut events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "cluster-late-write".into(),
        sink_id: Arc::from("cluster-late-write"),
        connector: Box::new(sink),
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_millis(5),
        event_tx,
        terminal_tasks: None,
        process_authority: Some(controller),
    });

    handle.write_batch(test_batch()).await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .expect("cluster late write did not report its deadline")
        .expect("sink event channel closed unexpectedly");

    assert!(matches!(
        event,
        SinkEvent::WriteTimeout { sink_id, rows: 3, .. }
            if &*sink_id == "cluster-late-write"
    ));
    assert!(
        handle
            .wait_terminal_until(Instant::now() + Duration::from_secs(1))
            .await,
        "cluster late write did not retire its connector generation"
    );
}

struct RetiredWriteSink {
    schema: arrow::datatypes::SchemaRef,
    completed: Arc<AtomicBool>,
    flushes: Arc<AtomicU64>,
    closed: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl SinkConnector for RetiredWriteSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::RetireConnector
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        self.completed.store(true, Ordering::Release);
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test(start_paused = true)]
async fn timed_out_write_retires_actor_without_late_completion_or_cleanup() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let completed = Arc::new(AtomicBool::new(false));
    let flushes = Arc::new(AtomicU64::new(0));
    let closed = Arc::new(AtomicBool::new(false));
    let sink = RetiredWriteSink {
        schema,
        completed: Arc::clone(&completed),
        flushes: Arc::clone(&flushes),
        closed: Arc::clone(&closed),
    };
    let (handle, events) =
        spawn_with_defaults("retired-write", Box::new(sink), Duration::from_millis(50));

    handle.write_batch(test_batch()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        events.try_recv(),
        Ok(SinkEvent::WriteTimeout { sink_id, .. }) if &*sink_id == "retired-write"
    ));
    assert!(!completed.load(Ordering::Acquire));
    assert!(
        !handle.has_unresolved_task(),
        "the retired actor must terminate after dropping the overdue write"
    );

    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;
    assert!(!completed.load(Ordering::Acquire));
    assert_eq!(flushes.load(Ordering::Acquire), 0);
    assert!(!closed.load(Ordering::Acquire));
}

struct UnknownOutcomeSink {
    schema: arrow::datatypes::SchemaRef,
    flushes: Arc<AtomicU64>,
    closes: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl SinkConnector for UnknownOutcomeSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Err(ConnectorError::outcome_unknown(
            "remote acknowledgement was lost",
            true,
        ))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test]
async fn connector_reported_unknown_outcome_retires_even_cancel_safe_generation() {
    let flushes = Arc::new(AtomicU64::new(0));
    let closes = Arc::new(AtomicU64::new(0));
    let sink = UnknownOutcomeSink {
        schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        flushes: Arc::clone(&flushes),
        closes: Arc::clone(&closes),
    };
    let (handle, events) =
        spawn_with_defaults("unknown-outcome", Box::new(sink), Duration::from_secs(5));

    handle.write_batch(test_batch()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.has_unresolved_task() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("retired sink terminal proof did not settle");
    assert!(!handle.has_unresolved_task());
    assert!(matches!(
        events.try_recv(),
        Ok(SinkEvent::WriteError { sink_id, error, .. })
            if &*sink_id == "unknown-outcome" && error.contains("outcome unknown")
    ));
    let close_error = handle.close().await.unwrap_err().to_string();
    assert!(
        close_error.contains("retired before close"),
        "{close_error}"
    );
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
    assert_eq!(closes.load(Ordering::SeqCst), 0);
}

struct UnknownProtocolSink {
    schema: arrow::datatypes::SchemaRef,
    closes: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl SinkConnector for UnknownProtocolSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        Err(ConnectorError::outcome_unknown(
            "flush acknowledgement was lost",
            true,
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test]
async fn acked_unknown_protocol_outcome_is_reported_before_actor_retirement() {
    let closes = Arc::new(AtomicU64::new(0));
    let sink = UnknownProtocolSink {
        schema: Arc::new(Schema::empty()),
        closes: Arc::clone(&closes),
    };
    let (handle, _events) =
        spawn_with_defaults("unknown-flush", Box::new(sink), Duration::from_secs(5));

    let error = handle.flush().await.unwrap_err();
    assert!(error.is_outcome_unknown(), "{error}");
    assert!(error.to_string().contains("flush acknowledgement was lost"));
    for _ in 0..100 {
        if !handle.has_unresolved_task() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(!handle.has_unresolved_task());
    let close_error = handle.close().await.unwrap_err().to_string();
    assert!(
        close_error.contains("retired before close"),
        "{close_error}"
    );
    assert_eq!(closes.load(Ordering::SeqCst), 0);
}

/// A slow write holds the actor while a following protocol command waits in the queue.
/// The queued command must retain its enqueue-time deadline and must not call the connector
/// after that deadline has elapsed.
struct QueueDeadlineSink {
    schema: arrow::datatypes::SchemaRef,
    flushes: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl SinkConnector for QueueDeadlineSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        self.flushes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test(start_paused = true)]
async fn queued_protocol_command_cannot_refresh_its_deadline() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let flushes = Arc::new(AtomicU64::new(0));
    let sink = QueueDeadlineSink {
        schema,
        flushes: Arc::clone(&flushes),
    };
    let (handle, _events) =
        spawn_with_defaults("queued", Box::new(sink), Duration::from_millis(50));

    handle.write_batch(test_batch()).await.unwrap();
    let error = handle.flush().await.unwrap_err().to_string();
    assert!(error.contains("end-to-end deadline"), "{error}");

    // Fence behind the expired flush to prove the actor inspected it without invoking the
    // connector. A fresh per-operation timeout in the actor would increment this counter.
    handle.sync().await.unwrap();
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
    handle.close().await.unwrap();
}

struct QueueCommitDeadlineSink {
    schema: arrow::datatypes::SchemaRef,
    write_started: Arc<AtomicBool>,
    write_gate: Arc<tokio::sync::Notify>,
    observed_remaining: Arc<parking_lot::Mutex<Option<Duration>>>,
}

#[async_trait::async_trait]
impl SinkConnector for QueueCommitDeadlineSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        self.write_started.store(true, Ordering::Release);
        self.write_gate.notified().await;
        Ok(WriteResult::new(1, 0))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_millis(100)
    }

    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        Some(self)
    }
}

#[async_trait::async_trait]
impl CoordinatedCommitter for QueueCommitDeadlineSink {
    async fn commit_aggregated(
        &self,
        _batch: CoordinatedCommitBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        *self.observed_remaining.lock() = Some(context.remaining());
        Ok(())
    }

    async fn committed_cursor(
        &self,
        _namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        Ok(None)
    }
}

#[tokio::test(start_paused = true)]
async fn queued_coordinated_commit_receives_only_its_remaining_budget() {
    use laminar_connectors::connector::{CoordinatedCommitNamespace, CoordinatedCommitPayload};
    use laminar_core::checkpoint::{CheckpointAttempt, PipelineIdentity};

    let write_started = Arc::new(AtomicBool::new(false));
    let write_gate = Arc::new(tokio::sync::Notify::new());
    let observed_remaining = Arc::new(parking_lot::Mutex::new(None));
    let sink = QueueCommitDeadlineSink {
        schema: Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)])),
        write_started: Arc::clone(&write_started),
        write_gate: Arc::clone(&write_gate),
        observed_remaining: Arc::clone(&observed_remaining),
    };
    let (event_tx, _events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "queued-commit".into(),
        sink_id: Arc::from("queued-commit"),
        connector: Box::new(sink),
        contract: checkpoint_committable_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_millis(100),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });
    let epoch_deadline = Instant::now() + Duration::from_secs(1);
    handle.begin_epoch_until(101, epoch_deadline).await.unwrap();
    let admission = handle.begun_epoch_admission(101).unwrap();
    handle.publish_open_epoch(admission).unwrap();
    handle.write_batch(test_batch()).await.unwrap();
    while !write_started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    let attempt = CheckpointAttempt::canonical(101);
    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "queued-commit",
    )
    .unwrap();
    let commit = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .commit_aggregated(CoordinatedCommitBatch {
                    namespace,
                    expected_predecessor: CoordinatedCommitCursor {
                        checkpoint_id: 0,
                        fencing_token: 0,
                    },
                    fencing_token: 1,
                    target: attempt,
                    entries: vec![CoordinatedCommitPayload {
                        attempt,
                        participant_id: 0,
                        payload: None,
                    }],
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(40)).await;
    write_gate.notify_waiters();
    commit.await.unwrap().unwrap();

    let remaining = observed_remaining.lock().unwrap();
    assert!(remaining <= Duration::from_millis(60), "got {remaining:?}");
    assert!(
        remaining > Duration::ZERO,
        "commit reached connector expired"
    );
    handle.close().await.unwrap();
}

struct SlowFlushSink {
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl SinkConnector for SlowFlushSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

struct CloseDeadlineSink {
    schema: arrow::datatypes::SchemaRef,
    close_calls: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl SinkConnector for CloseDeadlineSink {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        ConnectorCancellationPolicy::CancelSafe
    }

    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(1, 0))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        tokio::time::sleep(Duration::from_secs(60)).await;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn suggested_write_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[tokio::test(start_paused = true)]
async fn close_uses_one_deadline_and_never_starts_late_connector_close() {
    let close_calls = Arc::new(AtomicU64::new(0));
    let sink = CloseDeadlineSink {
        schema: Arc::new(Schema::empty()),
        close_calls: Arc::clone(&close_calls),
    };
    let (handle, _events) =
        spawn_with_defaults("close-deadline", Box::new(sink), Duration::from_secs(5));
    let started = Instant::now();
    let admission = handle.admission.lock().await;
    let close_handle = handle.clone();
    let close = tokio::spawn(async move { close_handle.close().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(5)).await;
    drop(admission);

    close
        .await
        .unwrap()
        .expect_err("shutdown flush must consume the one close deadline");

    assert_eq!(Instant::now() - started, SINK_CLOSE_TIMEOUT);
    assert_eq!(close_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(start_paused = true)]
async fn protocol_connector_operation_uses_configured_budget() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "slow-flush".into(),
        sink_id: Arc::from("slow-flush"),
        connector: Box::new(SlowFlushSink { schema }),
        // Avoid a second implicit flush during close; explicit flush is still valid here.
        contract: checkpoint_committable_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_millis(25),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    let started = Instant::now();
    let error = handle.flush().await.unwrap_err().to_string();
    assert!(error.contains("end-to-end deadline"), "{error}");
    assert_eq!(Instant::now() - started, Duration::from_millis(25));
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn protocol_operation_uses_earlier_caller_deadline() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let handle = SinkTaskHandle::spawn(SinkTaskConfig {
        name: "attempt-clamped-flush".into(),
        sink_id: Arc::from("attempt-clamped-flush"),
        connector: Box::new(SlowFlushSink { schema }),
        contract: checkpoint_committable_contract(),
        requires_recovery_on_error: true,
        channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        flush_interval: DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
        terminal_tasks: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    });

    let started = Instant::now();
    let deadline = started + Duration::from_millis(25);
    let error = handle.flush_until(deadline).await.unwrap_err().to_string();
    assert!(error.contains("end-to-end deadline"), "{error}");
    assert_eq!(Instant::now() - started, Duration::from_millis(25));
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_caller_deadline_never_enqueues_protocol_command() {
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let flushes = Arc::new(AtomicU64::new(0));
    let sink = QueueDeadlineSink {
        schema,
        flushes: Arc::clone(&flushes),
    };
    let (handle, _events) = spawn_with_defaults("expired", Box::new(sink), Duration::from_secs(5));

    let error = handle
        .flush_until(Instant::now())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("end-to-end deadline"), "{error}");
    handle.sync().await.unwrap();
    assert_eq!(flushes.load(Ordering::SeqCst), 0);
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn expired_write_deadline_never_enqueues_batch() {
    let (sink, writes, _flushes) = CountingSink::new();
    let (handle, _events) =
        spawn_with_defaults("expired-write", Box::new(sink), Duration::from_secs(5));

    let error = handle
        .write_batch_until(test_batch(), Instant::now())
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("end-to-end deadline"), "{error}");
    handle.sync().await.unwrap();
    assert_eq!(writes.load(Ordering::SeqCst), 0);
    handle.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn write_enqueue_timeout_poison_rejects_checkpoint_flush() {
    let write_timeout = Duration::from_millis(25);
    let (event_tx, events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);

    // Keep the sole queue slot occupied without running an actor. This isolates the handle's
    // enqueue deadline from connector execution and makes the dropped command deterministic.
    let (filler_ack, _filler_rx) = oneshot::oneshot();
    tx.send(SinkCommand {
        deadline: operation_deadline(Duration::from_secs(60)),
        operation: SinkOperation::Sync { ack: filler_ack },
    })
    .await
    .unwrap();
    let epoch_poisoned = Arc::new(AtomicBool::new(false));
    let (task, actor_state) = supervise_test_actor(async {}, None);
    let terminal_state = Arc::clone(&task.terminal_state);
    let handle = SinkTaskHandle {
        name: Arc::from("saturated"),
        sink_id: Arc::from("saturated"),
        tx,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        write_timeout,
        closing: Arc::new(AtomicBool::new(false)),
        admission: Arc::new(tokio::sync::Mutex::new(())),
        task: Arc::new(parking_lot::Mutex::new(Some(task))),
        close_state: Arc::new(SinkCloseState::new()),
        terminal_state,
        actor_state,
        runtime: tokio::runtime::Handle::current(),
        event_tx,
        epoch_poisoned: Arc::clone(&epoch_poisoned),
        epoch_gate: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let error = handle.write_batch(test_batch()).await.unwrap_err();
    assert!(error.to_string().contains("write enqueue"), "{error}");
    assert!(epoch_poisoned.load(Ordering::Acquire));

    let flush_error = handle.flush().await.unwrap_err();
    assert!(
        flush_error.to_string().contains("poisoned"),
        "{flush_error}"
    );
    assert!(matches!(
        events.try_recv(),
        Ok(SinkEvent::WriteEnqueueTimeout {
            sink_id,
            rows: 3,
            timeout,
        }) if &*sink_id == "saturated" && timeout == write_timeout
    ));

    drop(rx);
}

#[tokio::test(start_paused = true)]
async fn cancelled_actor_is_retained_until_uncooperative_join_is_terminal() {
    let (event_tx, _events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
    let (filler_ack, _filler_rx) = oneshot::oneshot();
    tx.send(SinkCommand {
        deadline: operation_deadline(Duration::from_secs(60)),
        operation: SinkOperation::Sync { ack: filler_ack },
    })
    .await
    .unwrap();

    let started = Arc::new(AtomicBool::new(false));
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let task_started = Arc::clone(&started);
    let task_gate = Arc::clone(&gate);
    let actor_state = Arc::new(SinkActorState::new());
    let actor_lifetime = SinkActorLifetime(Arc::clone(&actor_state));
    let task = tokio::task::spawn_blocking(move || {
        let _lifetime = actor_lifetime;
        task_started.store(true, Ordering::Release);
        let (lock, released) = &*task_gate;
        let mut ready = lock.lock().unwrap();
        while !*ready {
            ready = released.wait(ready).unwrap();
        }
    });
    while !started.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    let task = supervise_sink_task(
        task,
        None,
        Arc::clone(&actor_state),
        &tokio::runtime::Handle::current(),
    );
    let terminal_state = Arc::clone(&task.terminal_state);
    let handle = SinkTaskHandle {
        name: Arc::from("uncooperative-cancel-safe"),
        sink_id: Arc::from("uncooperative-cancel-safe"),
        tx,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        write_timeout: Duration::from_secs(1),
        closing: Arc::new(AtomicBool::new(false)),
        admission: Arc::new(tokio::sync::Mutex::new(())),
        task: Arc::new(parking_lot::Mutex::new(Some(task))),
        close_state: Arc::new(SinkCloseState::new()),
        terminal_state,
        actor_state,
        runtime: tokio::runtime::Handle::current(),
        event_tx,
        epoch_poisoned: Arc::new(AtomicBool::new(false)),
        epoch_gate: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    let close = tokio::spawn({
        let handle = handle.clone();
        async move { handle.close().await }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(SINK_CLOSE_TIMEOUT).await;
    let error = close
        .await
        .unwrap()
        .expect_err("the public close deadline must remain bounded");
    let unresolved_before_release = handle.has_unresolved_task();

    let (lock, released) = &*gate;
    *lock.lock().unwrap() = true;
    released.notify_all();
    assert!(error.to_string().contains("enqueue"), "{error}");
    assert!(
        unresolved_before_release,
        "aborting an uncooperative task must not erase the replacement fence"
    );
    let repeated = handle
        .close()
        .await
        .expect_err("terminal timeout result must persist after the actor exits");
    assert!(repeated.to_string().contains("enqueue"), "{repeated}");
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.has_unresolved_task() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal supervisor did not observe the released blocking actor");
    assert!(!handle.has_unresolved_task());
    drop(rx);
}

#[tokio::test]
async fn connector_child_task_holds_replacement_fence_after_actor_exit() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("terminal task owner must be live");
    drop(owner);

    let (event_tx, _events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
    drop(rx);
    let runtime = tokio::runtime::Handle::current();
    let (task, actor_state) = supervise_test_actor(async {}, Some(tracker));
    let terminal_state = Arc::clone(&task.terminal_state);
    let handle = SinkTaskHandle {
        name: Arc::from("terminal-child"),
        sink_id: Arc::from("terminal-child"),
        tx,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        write_timeout: Duration::from_secs(1),
        closing: Arc::new(AtomicBool::new(false)),
        admission: Arc::new(tokio::sync::Mutex::new(())),
        task: Arc::new(parking_lot::Mutex::new(Some(task))),
        close_state: Arc::new(SinkCloseState::new()),
        terminal_state,
        actor_state,
        runtime,
        event_tx,
        epoch_poisoned: Arc::new(AtomicBool::new(false)),
        epoch_gate: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    while !handle.actor_state.finished.load(Ordering::Acquire) {
        tokio::task::yield_now().await;
    }

    let error = handle
        .close()
        .await
        .expect_err("retired actor must fail close");
    assert!(
        error.to_string().contains("retired before close"),
        "{error}"
    );
    assert!(handle.has_unresolved_task());

    drop(guard);
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.has_unresolved_task() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal child did not release replacement fence");
}

#[test]
fn abort_before_first_poll_drops_sink_actor_before_publishing_terminal() {
    struct DropProbe {
        terminal: Arc<parking_lot::Mutex<Option<Arc<SinkActorState>>>>,
        dropped: Arc<AtomicBool>,
        terminal_was_finished: Arc<AtomicBool>,
    }

    impl std::future::Future for DropProbe {
        type Output = ();

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            panic!("sink actor was polled before its immediate abort");
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let terminal = self
                .terminal
                .lock()
                .clone()
                .expect("terminal state must be installed before abort");
            self.terminal_was_finished
                .store(terminal.finished.load(Ordering::Acquire), Ordering::Release);
            self.dropped.store(true, Ordering::Release);
        }
    }

    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    executor.block_on(async {
        let terminal_slot = Arc::new(parking_lot::Mutex::new(None));
        let dropped = Arc::new(AtomicBool::new(false));
        let terminal_was_finished = Arc::new(AtomicBool::new(false));
        let terminal = Arc::new(SinkActorState::new());
        let join = spawn_sink_actor(
            &tokio::runtime::Handle::current(),
            DropProbe {
                terminal: Arc::clone(&terminal_slot),
                dropped: Arc::clone(&dropped),
                terminal_was_finished: Arc::clone(&terminal_was_finished),
            },
            Arc::clone(&terminal),
        );
        *terminal_slot.lock() = Some(Arc::clone(&terminal));

        join.abort();
        assert!(join
            .await
            .expect_err("the unpolled sink actor must be cancelled")
            .is_cancelled());
        assert!(dropped.load(Ordering::Acquire));
        assert!(!terminal_was_finished.load(Ordering::Acquire));
        assert!(terminal.finished.load(Ordering::Acquire));
    });
}

#[tokio::test]
async fn cancelled_terminal_supervisor_cannot_publish_false_terminal() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("live connector child");
    drop(owner);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let (task, actor_state) = supervise_test_actor(
        async move {
            let _ = release_rx.await;
        },
        Some(tracker),
    );
    task.terminal_join.abort();
    let terminal_state = Arc::clone(&task.terminal_state);
    let (event_tx, _events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let (tx, _rx) = mpsc::bounded_async::<SinkCommand>(1);
    let handle = SinkTaskHandle {
        name: Arc::from("cancelled-terminal-supervisor"),
        sink_id: Arc::from("cancelled-terminal-supervisor"),
        tx,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        write_timeout: Duration::from_secs(1),
        closing: Arc::new(AtomicBool::new(false)),
        admission: Arc::new(tokio::sync::Mutex::new(())),
        task: Arc::new(parking_lot::Mutex::new(Some(task))),
        close_state: Arc::new(SinkCloseState::new()),
        terminal_state,
        actor_state,
        runtime: tokio::runtime::Handle::current(),
        event_tx,
        epoch_poisoned: Arc::new(AtomicBool::new(false)),
        epoch_gate: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    assert!(handle.has_unresolved_task());
    assert!(
        !handle
            .wait_terminal_until(Instant::now() + Duration::from_millis(20))
            .await,
        "supervisor cancellation must not substitute for actor exit"
    );

    let _ = release_tx.send(());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !handle.actor_state.finished.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor did not exit after release");
    assert!(handle.has_unresolved_task());
    assert!(
        !handle
            .wait_terminal_until(Instant::now() + Duration::from_millis(20))
            .await,
        "actor exit must not substitute for connector-child termination"
    );

    drop(guard);
    assert!(
        handle
            .wait_terminal_until(Instant::now() + Duration::from_secs(1))
            .await
    );
    assert!(!handle.has_unresolved_task());
}

#[tokio::test]
async fn close_driver_panic_is_sticky_but_terminal_proof_remains_observable() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("live connector child");
    drop(owner);
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let runtime = tokio::runtime::Handle::current();
    let (task, actor_state) = supervise_test_actor(
        async move {
            let _ = release_rx.await;
        },
        Some(tracker),
    );
    let terminal_state = Arc::clone(&task.terminal_state);
    let (event_tx, _events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let (tx, _rx) = mpsc::bounded_async::<SinkCommand>(1);
    let handle = SinkTaskHandle {
        name: Arc::from("panicked-close-driver"),
        sink_id: Arc::from("panicked-close-driver"),
        tx,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        write_timeout: Duration::from_secs(1),
        closing: Arc::new(AtomicBool::new(true)),
        admission: Arc::new(tokio::sync::Mutex::new(())),
        task: Arc::new(parking_lot::Mutex::new(None)),
        close_state: Arc::new(SinkCloseState::new()),
        terminal_state,
        actor_state,
        runtime: runtime.clone(),
        event_tx,
        epoch_poisoned: Arc::new(AtomicBool::new(false)),
        epoch_gate: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };
    let state = Arc::clone(&handle.close_state);
    spawn_sink_close_driver_future(
        Arc::clone(&handle.name),
        Arc::clone(&state),
        async move {
            let _task = task;
            panic!("injected terminal driver panic")
        },
        &runtime,
    );

    let error = wait_for_sink_close(
        handle.name(),
        Arc::clone(&state),
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .expect_err("driver panic must publish an immediate close failure");
    assert!(error.to_string().contains("terminal close driver panicked"));
    assert!(handle.has_unresolved_task());

    let _ = release_tx.send(());
    drop(guard);
    tokio::time::timeout(Duration::from_secs(1), async {
        while handle.has_unresolved_task() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached terminal proof did not observe actor and child termination");
    let repeated = handle
        .close()
        .await
        .expect_err("driver panic result must remain sticky");
    assert!(repeated
        .to_string()
        .contains("terminal close driver panicked"));
}

#[tokio::test]
async fn checkpoint_flush_actor_rechecks_shared_poison() {
    let (sink, _writes, flushes) = CountingSink::new();
    let (handle, _events) = spawn_with_defaults("poisoned", Box::new(sink), Duration::from_secs(5));
    handle.epoch_poisoned.store(true, Ordering::Release);

    // Bypass SinkTaskHandle::flush to exercise the actor-side race check directly.
    let (ack_tx, ack_rx) = oneshot::oneshot();
    handle
        .tx
        .send(SinkCommand {
            deadline: operation_deadline(Duration::from_secs(5)),
            operation: SinkOperation::Flush { ack: ack_tx },
        })
        .await
        .unwrap();
    let error = ack_rx.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("poisoned"), "{error}");
    assert_eq!(flushes.load(Ordering::Acquire), 0);

    handle.close().await.unwrap();
}

/// Verifies channel-closed errors emit a `SinkEvent::ChannelClosed`.
#[tokio::test]
async fn test_sink_task_channel_closed_emits_event() {
    let (event_tx, events) =
        laminar_core::streaming::channel::channel::<SinkEvent>(SINK_EVENT_CHANNEL_CAPACITY);
    let (tx, rx) = mpsc::bounded_async::<SinkCommand>(1);
    drop(rx);
    let (task, actor_state) = supervise_test_actor(async {}, None);
    let terminal_state = Arc::clone(&task.terminal_state);
    let handle = SinkTaskHandle {
        name: Arc::from("dead"),
        sink_id: Arc::from("dead"),
        tx,
        contract: at_least_once_contract(),
        requires_recovery_on_error: true,
        write_timeout: Duration::from_secs(5),
        closing: Arc::new(AtomicBool::new(false)),
        admission: Arc::new(tokio::sync::Mutex::new(())),
        task: Arc::new(parking_lot::Mutex::new(Some(task))),
        close_state: Arc::new(SinkCloseState::new()),
        terminal_state,
        actor_state,
        runtime: tokio::runtime::Handle::current(),
        event_tx,
        epoch_poisoned: Arc::new(AtomicBool::new(false)),
        epoch_gate: None,
        #[cfg(feature = "cluster")]
        process_authority: None,
    };

    // A disconnected actor must reject the write, poison the replay-required epoch, and
    // report the unexpected channel loss.
    let err = handle.write_batch(test_batch()).await.unwrap_err();
    assert!(matches!(err, ConnectorError::ConnectionFailed(_)));
    let flush_error = handle.flush().await.unwrap_err();
    assert!(
        flush_error.to_string().contains("poisoned"),
        "{flush_error}"
    );

    let event = events
        .try_recv()
        .expect("expected SinkEvent::ChannelClosed");
    assert!(matches!(event, SinkEvent::ChannelClosed { sink_id } if &*sink_id == "dead"));
}
