//! `SourceConnector` implementation for the OTel OTLP/gRPC receiver.
//!
//! Spawns a tonic gRPC server that accepts OTLP export RPCs for traces,
//! metrics, or logs (one signal type per source), converts to Arrow
//! `RecordBatch`, and delivers them via `poll_batch()`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use crossfire::{mpsc, AsyncRx, TryRecvError};
use tokio::net::TcpListener;
use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector, SourceConsistency,
    SourceContract, SourcePosition, SourceStart, SourceTopology,
};
use crate::error::ConnectorError;

use super::config::{OtelSignal, OtelSourceConfig};
use super::schema::{logs_schema, metrics_schema, traces_schema};
use super::server::OtelReceiver;

const SERVER_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

struct TrackedServerTask {
    handle: Option<JoinHandle<Result<(), ConnectorError>>>,
}

struct TaskExitNotify(Arc<Notify>);

impl Drop for TaskExitNotify {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

enum ServerWait {
    Completed(Result<(), ConnectorError>),
    TimedOut,
}

impl TrackedServerTask {
    fn spawn(
        owner: &ConnectorTaskOwner,
        exit_notify: Arc<Notify>,
        future: impl std::future::Future<Output = Result<(), ConnectorError>> + Send + 'static,
    ) -> Result<Self, ConnectorError> {
        let task_guard = owner.track().ok_or_else(|| {
            ConnectorError::Internal("OTel source task generation is already retired".into())
        })?;
        let handle = tokio::spawn(async move {
            let _task_guard = task_guard;
            let _exit_notify = TaskExitNotify(exit_notify);
            future.await
        });
        Ok(Self {
            handle: Some(handle),
        })
    }

    async fn wait_until(&mut self, deadline: tokio::time::Instant) -> ServerWait {
        let Some(handle) = self.handle.as_mut() else {
            return ServerWait::Completed(Ok(()));
        };
        match tokio::time::timeout_at(deadline, handle).await {
            Ok(_) if tokio::time::Instant::now() >= deadline => {
                self.handle.take();
                ServerWait::TimedOut
            }
            Ok(Ok(result)) => {
                self.handle.take();
                ServerWait::Completed(result)
            }
            Ok(Err(error)) => {
                self.handle.take();
                ServerWait::Completed(Err(ConnectorError::Internal(format!(
                    "OTel gRPC server task failed: {error}"
                ))))
            }
            Err(_) => ServerWait::TimedOut,
        }
    }

    async fn take_finished(&mut self) -> Option<Result<(), ConnectorError>> {
        let handle = self.handle.as_ref()?;
        if !handle.is_finished() {
            return None;
        }
        let result = self.handle.take()?.await;
        Some(result.unwrap_or_else(|error| {
            Err(ConnectorError::Internal(format!(
                "OTel gRPC server task failed: {error}"
            )))
        }))
    }

    fn abort(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

/// OTel OTLP/gRPC source connector.
///
/// Binds a gRPC server on the configured port and receives telemetry
/// data from `OpenTelemetry` exporters and collectors. Each source
/// handles exactly one signal type (traces, metrics, or logs).
pub struct OtelSource {
    config: OtelSourceConfig,
    schema: SchemaRef,
    state: ConnectorState,
    batch_rx: Option<AsyncRx<mpsc::Array<RecordBatch>>>,
    data_ready: Arc<Notify>,
    server_task: Option<TrackedServerTask>,
    shutdown_tx: Option<watch::Sender<bool>>,
    /// Monotonic counter of records received (spans, data points, or log records).
    records_received: Arc<AtomicU64>,
    requests_received: Arc<AtomicU64>,
    checkpoint_seq: u64,
    server_failure: Option<String>,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

impl OtelSource {
    /// Create a new OTel source with the given default schema.
    #[must_use]
    pub fn new(schema: SchemaRef, _registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            config: OtelSourceConfig::default(),
            schema,
            state: ConnectorState::Created,
            batch_rx: None,
            data_ready: Arc::new(Notify::new()),
            server_task: None,
            shutdown_tx: None,
            records_received: Arc::new(AtomicU64::new(0)),
            requests_received: Arc::new(AtomicU64::new(0)),
            checkpoint_seq: 0,
            server_failure: None,
            task_owner,
            task_tracker,
        }
    }

    fn request_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown_tx.take() {
            shutdown.send_replace(true);
        }
        self.batch_rx.take();
    }

    async fn observe_server_exit(&mut self) {
        if self.server_failure.is_some() || self.state != ConnectorState::Running {
            return;
        }
        let Some(server) = self.server_task.as_mut() else {
            return;
        };
        let Some(result) = server.take_finished().await else {
            return;
        };
        self.server_task.take();
        self.server_failure = Some(match result {
            Ok(()) => "OTel gRPC server stopped unexpectedly".into(),
            Err(ConnectorError::ConnectionFailed(message) | ConnectorError::Internal(message)) => {
                message
            }
            Err(error) => error.to_string(),
        });
        self.state = ConnectorState::Failed;
    }

    fn terminal_server_error(message: &str) -> ConnectorError {
        ConnectorError::InvalidState {
            expected: "live OTLP gRPC server".into(),
            actual: format!("server generation terminated: {message}"),
        }
    }
}

impl Drop for OtelSource {
    fn drop(&mut self) {
        self.request_shutdown();
        if let Some(server) = self.server_task.as_mut() {
            server.abort();
        }
    }
}

#[async_trait]
impl SourceConnector for OtelSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let SourceStart {
            config, position, ..
        } = request;
        if let SourcePosition::Resume { attempt, .. } = position {
            return Err(ConnectorError::ConfigurationError(format!(
                "OTLP is an ephemeral source and cannot resume checkpoint attempt {attempt:?}"
            )));
        }
        if !matches!(self.state, ConnectorState::Created | ConnectorState::Closed)
            || self.server_task.is_some()
        {
            return Err(ConnectorError::InvalidState {
                expected: "Created or fully closed".into(),
                actual: format!("{}", self.state),
            });
        }

        let candidate_config = OtelSourceConfig::from_config(&config)?;

        let candidate_schema = match candidate_config.signals {
            OtelSignal::Traces => traces_schema(),
            OtelSignal::Metrics => metrics_schema(),
            OtelSignal::Logs => logs_schema(),
        };

        let (batch_tx, batch_rx) =
            mpsc::bounded_async::<RecordBatch>(candidate_config.channel_capacity);

        let addr = candidate_config.socket_addr();

        // Bind the TCP listener here so port conflicts fail start(),
        // not silently inside the background task.
        let listener = TcpListener::bind(&addr)
            .await
            .map_err(|e| ConnectorError::ConnectionFailed(format!("failed to bind {addr}: {e}")))?;
        let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service_guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal("OTel source task generation is already retired".into())
        })?;

        let receiver = OtelReceiver::new(
            batch_tx,
            Arc::clone(&candidate_schema),
            Arc::clone(&self.data_ready),
            Arc::clone(&self.records_received),
            Arc::clone(&self.requests_received),
            candidate_config.batch_size,
            service_guard,
        );

        // Spawn signal-specific gRPC server on the already-bound listener
        let server_task = match candidate_config.signals {
            OtelSignal::Traces => spawn_grpc_server(
                &self.task_owner,
                TraceServiceServer::new(receiver),
                incoming,
                shutdown_rx,
                Arc::clone(&self.data_ready),
            ),
            OtelSignal::Metrics => spawn_grpc_server(
                &self.task_owner,
                MetricsServiceServer::new(receiver),
                incoming,
                shutdown_rx,
                Arc::clone(&self.data_ready),
            ),
            OtelSignal::Logs => spawn_grpc_server(
                &self.task_owner,
                LogsServiceServer::new(receiver),
                incoming,
                shutdown_rx,
                Arc::clone(&self.data_ready),
            ),
        }?;

        self.config = candidate_config;
        self.schema = candidate_schema;
        self.batch_rx = Some(batch_rx);
        self.shutdown_tx = Some(shutdown_tx);
        self.server_task = Some(server_task);
        self.server_failure = None;
        self.state = ConnectorState::Running;

        tracing::info!(
            %addr,
            signals = ?self.config.signals,
            batch_size = self.config.batch_size,
            "OTel source connector started"
        );

        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(error) = &self.server_failure {
            return Err(Self::terminal_server_error(error));
        }
        let rx = self.batch_rx.as_ref().ok_or(ConnectorError::InvalidState {
            expected: "Running".into(),
            actual: format!("{}", self.state),
        })?;

        let mut total_rows = 0usize;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let mut disconnected = false;

        loop {
            match rx.try_recv() {
                Ok(batch) => {
                    total_rows += batch.num_rows();
                    batches.push(batch);
                    if total_rows >= max_records {
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        self.observe_server_exit().await;

        if batches.is_empty() {
            return if let Some(error) = &self.server_failure {
                Err(Self::terminal_server_error(error))
            } else if disconnected {
                self.state = ConnectorState::Closed;
                Err(ConnectorError::Closed)
            } else {
                Ok(None)
            };
        }

        self.checkpoint_seq += 1;

        if batches.len() == 1 {
            return Ok(Some(SourceBatch::new(batches.into_iter().next().unwrap())));
        }

        let schema = batches[0].schema();
        let combined =
            arrow_select::concat::concat_batches(&schema, batches.iter()).map_err(|e| {
                ConnectorError::ReadError(format!("failed to concatenate OTel batches: {e}"))
            })?;

        Ok(Some(SourceBatch::new(combined)))
    }

    async fn discover_schema(
        &mut self,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        let Some(sig) = properties
            .get("signals")
            .or_else(|| properties.get("signal"))
        else {
            return Ok(());
        };
        let signal = OtelSignal::parse(sig).map_err(|e| {
            ConnectorError::ConfigurationError(format!("invalid OTel signal '{sig}': {e}"))
        })?;
        self.schema = match signal {
            OtelSignal::Traces => traces_schema(),
            OtelSignal::Metrics => metrics_schema(),
            OtelSignal::Logs => logs_schema(),
        };
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new();
        cp.set_offset("batch_sequence", self.checkpoint_seq.to_string());
        cp.set_offset(
            "records_received",
            self.records_received.load(Ordering::Relaxed).to_string(),
        );
        cp.set_offset(
            "requests_received",
            self.requests_received.load(Ordering::Relaxed).to_string(),
        );
        cp.set_metadata("connector", "otel");
        cp.set_metadata("signals", format!("{:?}", self.config.signals));
        cp
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        tracing::info!("OTel source connector shutting down");

        self.request_shutdown();

        let mut completed = false;
        let mut close_error = self
            .server_failure
            .as_ref()
            .map(|error| Self::terminal_server_error(error));
        if let Some(server) = self.server_task.as_mut() {
            let deadline = tokio::time::Instant::now() + SERVER_CLOSE_TIMEOUT;
            match server.wait_until(deadline).await {
                ServerWait::Completed(Ok(())) => completed = true,
                ServerWait::Completed(Err(error)) => {
                    completed = true;
                    tracing::warn!(%error, "OTel gRPC server task failed while closing");
                    close_error = Some(error);
                }
                ServerWait::TimedOut => {
                    server.abort();
                    completed = true;
                    close_error = Some(ConnectorError::Internal(
                        "OTel gRPC server exceeded its close deadline; connector generation retired"
                            .into(),
                    ));
                    tracing::warn!("OTel gRPC server exceeded its close deadline and was aborted");
                }
            }
        }
        if completed {
            self.server_task.take();
        }

        if let Some(error) = close_error {
            self.state = ConnectorState::Failed;
            Err(error)
        } else {
            self.state = ConnectorState::Closed;
            Ok(())
        }
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Ephemeral,
            SourceTopology::NodeLocalIngress,
        ))
    }
}

/// Spawn a tonic gRPC server on a pre-bound listener with graceful shutdown.
fn spawn_grpc_server<S>(
    owner: &ConnectorTaskOwner,
    svc: S,
    incoming: TcpIncoming,
    mut shutdown_rx: watch::Receiver<bool>,
    data_ready: Arc<Notify>,
) -> Result<TrackedServerTask, ConnectorError>
where
    S: tonic::codegen::Service<
            tonic::codegen::http::Request<tonic::body::Body>,
            Response = tonic::codegen::http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    TrackedServerTask::spawn(owner, data_ready, async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_rx.wait_for(|&v| v).await;
            })
            .await
            .map_err(|error| {
                tracing::error!(%error, "OTel gRPC server exited with error");
                ConnectorError::ConnectionFailed(format!(
                    "OTel gRPC server exited with error: {error}"
                ))
            })
    })
}

impl std::fmt::Debug for OtelSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OtelSource")
            .field("state", &self.state)
            .field("config", &self.config)
            .field(
                "records_received",
                &self.records_received.load(Ordering::Relaxed),
            )
            .field(
                "requests_received",
                &self.requests_received.load(Ordering::Relaxed),
            )
            .field("checkpoint_seq", &self.checkpoint_seq)
            .field(
                "server_running",
                &self
                    .server_task
                    .as_ref()
                    .and_then(|task| task.handle.as_ref().map(|handle| !handle.is_finished())),
            )
            .field("has_shutdown_tx", &self.shutdown_tx.is_some())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::DeliveryGuarantee;

    fn start_request() -> SourceStart {
        let mut config = ConnectorConfig::new("otel");
        config.set("bind.address", "127.0.0.1");
        config.set("port", "0");
        SourceStart {
            config,
            position: SourcePosition::Initial,
            delivery: DeliveryGuarantee::BestEffort,
        }
    }

    #[tokio::test]
    async fn clean_close_can_restart_the_same_tracked_generation() {
        let mut source = OtelSource::new(traces_schema(), None);
        let terminal = source.terminal_task_tracker().unwrap();

        source.start(start_request()).await.unwrap();
        assert!(source.server_task.is_some());
        source.close().await.unwrap();
        assert!(source.server_task.is_none());
        assert!(!terminal.is_terminated(), "the generation owner is live");

        source.start(start_request()).await.unwrap();
        assert!(source.server_task.is_some());
        drop(source);

        tokio::time::timeout(Duration::from_secs(2), terminal.wait_terminated())
            .await
            .expect("restarted OTel server generation did not terminate");
    }

    #[tokio::test]
    async fn aborted_close_waiter_aborts_the_owned_server_task() {
        let mut source = OtelSource::new(traces_schema(), None);
        let terminal = source.terminal_task_tracker().unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        source.server_task = Some(
            TrackedServerTask::spawn(
                &source.task_owner,
                Arc::clone(&source.data_ready),
                async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    Ok(())
                },
            )
            .unwrap(),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        source.shutdown_tx = Some(shutdown_tx);
        source.state = ConnectorState::Running;
        started_rx.await.expect("test server task started");

        let close = tokio::spawn(async move { source.close().await });
        tokio::task::yield_now().await;
        assert!(!close.is_finished(), "close must be joining the server");
        close.abort();
        assert!(
            close
                .await
                .expect_err("close waiter cancelled")
                .is_cancelled(),
            "close waiter must be cancelled"
        );

        assert!(*shutdown_rx.borrow(), "close must publish shutdown");
        tokio::time::timeout(Duration::from_secs(2), terminal.wait_terminated())
            .await
            .expect("cancelled close left the server generation live");
        assert!(
            release_tx.send(()).is_err(),
            "the aborted server must drop its test receiver"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn close_timeout_aborts_the_server_task() {
        let mut source = OtelSource::new(traces_schema(), None);
        let terminal = source.terminal_task_tracker().unwrap();
        source.server_task = Some(
            TrackedServerTask::spawn(
                &source.task_owner,
                Arc::clone(&source.data_ready),
                std::future::pending::<Result<(), ConnectorError>>(),
            )
            .unwrap(),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        source.shutdown_tx = Some(shutdown_tx);
        source.state = ConnectorState::Running;

        let error = source.close().await.unwrap_err();

        assert!(*shutdown_rx.borrow(), "close must publish shutdown");
        assert!(error.to_string().contains("close deadline"), "{error}");
        assert_eq!(source.state, ConnectorState::Failed);
        assert!(
            source.server_task.is_none(),
            "the aborted task must not block restart"
        );
        assert!(matches!(
            source.start(start_request()).await,
            Err(ConnectorError::InvalidState { .. })
        ));
        drop(source);
        tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
            .await
            .expect("aborted OTel server generation did not terminate");
    }

    #[tokio::test]
    async fn late_server_completion_is_a_close_timeout() {
        let owner = ConnectorTaskOwner::new().0;
        let mut task = TrackedServerTask::spawn(&owner, Arc::new(Notify::new()), async {
            std::thread::sleep(Duration::from_millis(25));
            Ok(())
        })
        .unwrap();

        let result = task
            .wait_until(tokio::time::Instant::now() + Duration::from_millis(5))
            .await;

        assert!(matches!(result, ServerWait::TimedOut));
        assert!(task.handle.is_none());
    }

    #[tokio::test]
    async fn unexpected_server_exit_fails_live_polling() {
        let mut source = OtelSource::new(traces_schema(), None);
        let (_batch_tx, batch_rx) = mpsc::bounded_async::<RecordBatch>(1);
        source.batch_rx = Some(batch_rx);
        source.server_task = Some(
            TrackedServerTask::spawn(&source.task_owner, Arc::clone(&source.data_ready), async {
                Err(ConnectorError::ConnectionFailed(
                    "accept loop failed".into(),
                ))
            })
            .unwrap(),
        );
        source.state = ConnectorState::Running;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !source
                .server_task
                .as_ref()
                .and_then(|task| task.handle.as_ref())
                .is_some_and(tokio::task::JoinHandle::is_finished)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("test server failure did not become observable");
        tokio::time::timeout(Duration::from_secs(1), source.data_ready.notified())
            .await
            .expect("terminal server exit did not wake source polling");

        let error = source.poll_batch(1).await.unwrap_err();

        assert!(error.to_string().contains("accept loop failed"), "{error}");
        assert!(
            !error.is_transient(),
            "terminal generation errors must not retry"
        );
        assert_eq!(source.state, ConnectorState::Failed);
        assert!(source.server_task.is_none());
    }
}
