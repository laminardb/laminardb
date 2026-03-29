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
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::JoinHandle;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;

use crate::checkpoint::SourceCheckpoint;
use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{SourceBatch, SourceConnector};
use crate::error::ConnectorError;
use crate::health::HealthStatus;
use crate::metrics::ConnectorMetrics;

use super::config::{OtelSignal, OtelSourceConfig};
use super::schema::{logs_schema, metrics_schema, traces_schema};
use super::server::OtelReceiver;

/// OTel OTLP/gRPC source connector.
///
/// Binds a gRPC server on the configured port and receives telemetry
/// data from `OpenTelemetry` exporters and collectors. Each source
/// handles exactly one signal type (traces, metrics, or logs).
pub struct OtelSource {
    config: OtelSourceConfig,
    schema: SchemaRef,
    state: ConnectorState,
    batch_rx: Option<mpsc::Receiver<RecordBatch>>,
    data_ready: Arc<Notify>,
    server_handle: Option<JoinHandle<()>>,
    shutdown_tx: Option<watch::Sender<bool>>,
    /// Monotonic counter of records received (spans, data points, or log records).
    records_received: Arc<AtomicU64>,
    requests_received: Arc<AtomicU64>,
    checkpoint_seq: u64,
}

impl OtelSource {
    /// Create a new OTel source with the given default schema.
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            config: OtelSourceConfig::default(),
            schema,
            state: ConnectorState::Created,
            batch_rx: None,
            data_ready: Arc::new(Notify::new()),
            server_handle: None,
            shutdown_tx: None,
            records_received: Arc::new(AtomicU64::new(0)),
            requests_received: Arc::new(AtomicU64::new(0)),
            checkpoint_seq: 0,
        }
    }
}

#[async_trait]
impl SourceConnector for OtelSource {
    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.state = ConnectorState::Initializing;
        self.config = OtelSourceConfig::from_config(config)?;

        self.schema = match self.config.signals {
            OtelSignal::Traces => traces_schema(),
            OtelSignal::Metrics => metrics_schema(),
            OtelSignal::Logs => logs_schema(),
        };

        let (batch_tx, batch_rx) = mpsc::channel(self.config.channel_capacity);
        self.batch_rx = Some(batch_rx);

        let addr = self.config.socket_addr().parse().map_err(|e| {
            ConnectorError::ConfigurationError(format!(
                "invalid bind address '{}': {e}",
                self.config.socket_addr()
            ))
        })?;

        let send_timeout = Duration::from_secs(5);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let schema = Arc::clone(&self.schema);
        let data_ready = Arc::clone(&self.data_ready);
        let records_received = Arc::clone(&self.records_received);
        let requests_received = Arc::clone(&self.requests_received);
        let batch_size = self.config.batch_size;

        let receiver = OtelReceiver::new(
            batch_tx,
            schema,
            data_ready,
            records_received,
            requests_received,
            send_timeout,
            batch_size,
        );

        // Spawn signal-specific gRPC server
        let server_handle = match self.config.signals {
            OtelSignal::Traces => {
                spawn_grpc_server(TraceServiceServer::new(receiver), addr, shutdown_rx)
            }
            OtelSignal::Metrics => {
                spawn_grpc_server(MetricsServiceServer::new(receiver), addr, shutdown_rx)
            }
            OtelSignal::Logs => {
                spawn_grpc_server(LogsServiceServer::new(receiver), addr, shutdown_rx)
            }
        };

        self.server_handle = Some(server_handle);
        self.state = ConnectorState::Running;

        tracing::info!(
            addr = %self.config.socket_addr(),
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
        let rx = self.batch_rx.as_mut().ok_or(ConnectorError::InvalidState {
            expected: "Running".into(),
            actual: format!("{}", self.state),
        })?;

        let mut total_rows = 0usize;
        let mut batches: Vec<RecordBatch> = Vec::new();

        loop {
            match rx.try_recv() {
                Ok(batch) => {
                    total_rows += batch.num_rows();
                    batches.push(batch);
                    if total_rows >= max_records {
                        break;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.state = ConnectorState::Closed;
                    return Err(ConnectorError::Closed);
                }
            }
        }

        if batches.is_empty() {
            return Ok(None);
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

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut cp = SourceCheckpoint::new(self.checkpoint_seq);
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

    async fn restore(&mut self, _checkpoint: &SourceCheckpoint) -> Result<(), ConnectorError> {
        tracing::warn!(
            "OTel source restore: push-only transport, data gap expected since last checkpoint"
        );
        Ok(())
    }

    fn health_check(&self) -> HealthStatus {
        match self.state {
            ConnectorState::Running => {
                if self
                    .server_handle
                    .as_ref()
                    .is_some_and(|h| !h.is_finished())
                {
                    HealthStatus::Healthy
                } else {
                    HealthStatus::Unhealthy("gRPC server task exited".into())
                }
            }
            ConnectorState::Created | ConnectorState::Initializing => HealthStatus::Unknown,
            ConnectorState::Paused | ConnectorState::Recovering => {
                HealthStatus::Degraded(format!("{}", self.state))
            }
            ConnectorState::Closed | ConnectorState::Failed => {
                HealthStatus::Unhealthy(format!("{}", self.state))
            }
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn metrics(&self) -> ConnectorMetrics {
        ConnectorMetrics {
            records_total: self.records_received.load(Ordering::Relaxed),
            bytes_total: 0,
            errors_total: 0,
            lag: 0,
            custom: vec![
                (
                    "requests_received".into(),
                    self.requests_received.load(Ordering::Relaxed) as f64,
                ),
                ("checkpoint_seq".into(), self.checkpoint_seq as f64),
            ],
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        tracing::info!("OTel source connector shutting down");

        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }

        self.batch_rx.take();

        if let Some(handle) = self.server_handle.take() {
            if tokio::time::timeout(Duration::from_secs(5), handle)
                .await
                .is_err()
            {
                tracing::warn!("OTel gRPC server did not shut down within 5s");
            }
        }

        self.state = ConnectorState::Closed;
        Ok(())
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    fn supports_replay(&self) -> bool {
        false
    }
}

/// Spawn a tonic gRPC server with graceful shutdown for any service type.
fn spawn_grpc_server<S>(
    svc: S,
    addr: std::net::SocketAddr,
    mut shutdown_rx: watch::Receiver<bool>,
) -> JoinHandle<()>
where
    S: tonic::codegen::Service<
            tonic::codegen::http::Request<tonic::body::BoxBody>,
            Response = tonic::codegen::http::Response<tonic::body::BoxBody>,
            Error = std::convert::Infallible,
        > + tonic::server::NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    tokio::spawn(async move {
        tracing::info!(%addr, "OTel gRPC server starting");
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async move {
                let _ = shutdown_rx.wait_for(|&v| v).await;
            })
            .await
        {
            tracing::error!(error = %e, "OTel gRPC server exited with error");
        }
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
                &self.server_handle.as_ref().map(|h| !h.is_finished()),
            )
            .field("has_shutdown_tx", &self.shutdown_tx.is_some())
            .finish_non_exhaustive()
    }
}
