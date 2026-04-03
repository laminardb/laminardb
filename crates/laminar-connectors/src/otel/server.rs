//! Tonic gRPC server implementing OTLP trace, metrics, and logs services.
//!
//! Each receiver converts its respective protobuf request to Arrow
//! `RecordBatch`, splits oversized batches, and forwards them through
//! a bounded channel to the `OtelSource` connector.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use tokio::sync::{mpsc, Notify};
use tonic::{Request, Response, Status};

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};

use super::convert::{logs_request_to_batch, metrics_request_to_batch, trace_request_to_batch};

// ── Shared helpers ──

/// Current wall-clock time as nanoseconds since Unix epoch.
fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| {
            // Nanos from epoch fits in i64 until ~2262.
            #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
            {
                d.as_nanos() as i64
            }
        })
}

/// Split a batch into chunks of at most `batch_size` rows and send each
/// through the channel. Returns the total number of rows sent.
async fn split_and_send(
    batch: RecordBatch,
    batch_size: usize,
    tx: &mpsc::Sender<RecordBatch>,
    notify: &Notify,
    timeout: Duration,
) -> Result<usize, Status> {
    let total = batch.num_rows();
    if total == 0 {
        return Ok(0);
    }

    let mut offset = 0;
    while offset < total {
        let len = (total - offset).min(batch_size);
        let chunk = batch.slice(offset, len);

        match tokio::time::timeout(timeout, tx.send(chunk)).await {
            Ok(Ok(())) => {
                notify.notify_one();
                offset += len;
            }
            Ok(Err(_)) => {
                return Err(Status::unavailable("OTel receiver is shutting down"));
            }
            Err(_) => {
                tracing::warn!(
                    sent = offset,
                    remaining = total - offset,
                    "OTel batch send timed out — downstream backpressure"
                );
                return Err(Status::resource_exhausted(
                    "pipeline backpressure: retry with backoff",
                ));
            }
        }
    }

    Ok(total)
}

// ── Receiver struct (shared across all signal types) ──

/// OTLP receiver that converts protobuf to Arrow and pushes to a channel.
///
/// One instance is created per signal type; the tonic service trait
/// (`TraceService`, `MetricsService`, `LogsService`) is impl'd below.
pub struct OtelReceiver {
    batch_tx: mpsc::Sender<RecordBatch>,
    schema: SchemaRef,
    data_ready: Arc<Notify>,
    records_received: Arc<AtomicU64>,
    requests_received: Arc<AtomicU64>,
    send_timeout: Duration,
    batch_size: usize,
}

impl OtelReceiver {
    /// Create a new receiver.
    pub fn new(
        batch_tx: mpsc::Sender<RecordBatch>,
        schema: SchemaRef,
        data_ready: Arc<Notify>,
        records_received: Arc<AtomicU64>,
        requests_received: Arc<AtomicU64>,
        send_timeout: Duration,
        batch_size: usize,
    ) -> Self {
        Self {
            batch_tx,
            schema,
            data_ready,
            records_received,
            requests_received,
            send_timeout,
            batch_size,
        }
    }

    /// Common export path: convert → split → send → count.
    async fn handle_batch(&self, batch: Option<RecordBatch>) -> Result<usize, Status> {
        let Some(batch) = batch else {
            return Ok(0);
        };
        let n = split_and_send(
            batch,
            self.batch_size,
            &self.batch_tx,
            &self.data_ready,
            self.send_timeout,
        )
        .await?;
        #[allow(clippy::cast_possible_truncation)]
        self.records_received.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }
}

// ── Trait impls ──

#[tonic::async_trait]
impl TraceService for OtelReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.requests_received.fetch_add(1, Ordering::Relaxed);
        let batch = trace_request_to_batch(&request.into_inner(), &self.schema, now_nanos())
            .map_err(|e| Status::internal(format!("batch conversion failed: {e}")))?;
        self.handle_batch(batch).await?;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for OtelReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.requests_received.fetch_add(1, Ordering::Relaxed);
        let batch = metrics_request_to_batch(&request.into_inner(), &self.schema, now_nanos())
            .map_err(|e| Status::internal(format!("batch conversion failed: {e}")))?;
        self.handle_batch(batch).await?;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl LogsService for OtelReceiver {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        self.requests_received.fetch_add(1, Ordering::Relaxed);
        let batch = logs_request_to_batch(&request.into_inner(), &self.schema, now_nanos())
            .map_err(|e| Status::internal(format!("batch conversion failed: {e}")))?;
        self.handle_batch(batch).await?;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
