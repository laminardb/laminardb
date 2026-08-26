//! Bounded multi-active reader over committed partition output segments.

mod authority;
mod gateway;
mod pin;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use laminar_core::checkpoint::{
    CheckpointStore, OutputDistributionCertificate, OutputPartitionId, PartitionSequence,
    StreamGeneration,
};
use laminar_core::cluster::control::LeaderLeaseStore;
use parking_lot::Mutex;
use tokio::sync::{mpsc, OwnedSemaphorePermit};

use authority::GatewayCursor;
use gateway::run_gateway;
use pin::{acquire_replay_pin, release_replay_pin, GatewayReplayPin};

use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};

pub(super) const MANIFEST_REFRESH_INTERVAL: Duration = Duration::from_millis(250);
pub(super) const GATEWAY_IO_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const GATEWAY_SEND_TIMEOUT: Duration = Duration::from_secs(5);
pub(super) const MAX_GATEWAY_QUEUE_FRAMES: usize = 64;
pub(super) const MAX_GATEWAY_QUEUE_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_GATEWAY_SEGMENT_READS: usize = 4;
// One decoded segment may be draining while the next reads remain in flight.
#[cfg(feature = "benchmark-internals")]
pub(super) const MAX_GATEWAY_RETAINED_SEGMENTS: usize = MAX_GATEWAY_SEGMENT_READS + 1;
pub(super) const MAX_GATEWAY_CATCHUP_CHECKPOINTS: usize = 256;

/// One verified frame returned to the backend-neutral portal.
pub(crate) enum ClusterReaderFrame {
    Batch {
        batch: RecordBatch,
        delivery_sequence: u64,
        stream_generation: StreamGeneration,
        partition: OutputPartitionId,
        partition_sequence: PartitionSequence,
        committed_epoch: u64,
        permit: Arc<OwnedSemaphorePermit>,
    },
    Progress {
        delivery_sequence: u64,
        through_sequence: u64,
        stream_generation: StreamGeneration,
        epoch: u64,
        checkpoint_id: u64,
    },
}

/// Result of one portal read from the committed gateway task.
pub(crate) enum ClusterReaderRead {
    Frame(ClusterReaderFrame),
    Terminal(ClusterSubscriptionError),
}

/// Connection-local handle for a bounded committed-output gateway task.
pub(crate) struct ClusterSubscriptionReader {
    receiver: mpsc::Receiver<ClusterReaderFrame>,
    terminal: Arc<Mutex<Option<ClusterSubscriptionError>>>,
    cancel: Option<tokio::sync::oneshot::Sender<()>>,
    _task: tokio::task::JoinHandle<()>,
}

impl std::fmt::Debug for ClusterSubscriptionReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClusterSubscriptionReader")
            .field("queued_frames", &self.receiver.len())
            .finish_non_exhaustive()
    }
}

impl ClusterSubscriptionReader {
    /// Resolve the exact attachment cut before publishing a portal.
    pub(crate) async fn open(
        authority: Arc<LeaderLeaseStore>,
        store: Arc<dyn CheckpointStore>,
        certificate: Arc<OutputDistributionCertificate>,
        start: SubscribeStart,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Self, DbError> {
        validate_replay_start(&certificate, start)?;
        let replay_pin = acquire_replay_pin(&authority, &certificate, start).await?;
        let cursor = tokio::time::timeout(
            GATEWAY_IO_TIMEOUT,
            GatewayCursor::open(&authority, &store, &certificate, start),
        )
        .await
        .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?;
        let cursor = match cursor {
            Ok(cursor) => cursor,
            Err(error) => {
                release_replay_pin(&authority, replay_pin.as_ref()).await;
                return Err(error);
            }
        };
        Ok(spawn_reader(
            authority,
            store,
            certificate,
            cursor,
            replay_pin,
            metrics,
        ))
    }

    pub(crate) async fn next(&mut self) -> ClusterReaderRead {
        match self.receiver.recv().await {
            Some(frame) => ClusterReaderRead::Frame(frame),
            None => ClusterReaderRead::Terminal(take_terminal(&self.terminal)),
        }
    }

    pub(crate) fn try_next(&mut self) -> Option<ClusterReaderRead> {
        match self.receiver.try_recv() {
            Ok(frame) => Some(ClusterReaderRead::Frame(frame)),
            Err(mpsc::error::TryRecvError::Empty) => None,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                Some(ClusterReaderRead::Terminal(take_terminal(&self.terminal)))
            }
        }
    }

    #[cfg(feature = "benchmark-internals")]
    pub(super) fn queued_frames(&self) -> usize {
        self.receiver.len()
    }

    #[cfg(feature = "benchmark-internals")]
    #[allow(clippy::used_underscore_binding)] // benchmark teardown awaits the dormant handle
    pub(super) async fn close_for_benchmark(mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        let _ = (&mut self._task).await;
    }

    #[cfg(feature = "benchmark-internals")]
    #[allow(clippy::used_underscore_binding)] // benchmark capture freezes the dormant handle
    pub(super) async fn capture_queue_for_benchmark(mut self) -> (usize, usize, usize) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
        let _ = (&mut self._task).await;
        let queued_slots = self.receiver.len();
        let mut batch_frames = 0;
        let mut arrow_bytes = 0;
        while let Ok(frame) = self.receiver.try_recv() {
            if let ClusterReaderFrame::Batch { batch, .. } = frame {
                batch_frames += 1;
                arrow_bytes += batch.get_array_memory_size();
            }
        }
        (queued_slots, batch_frames, arrow_bytes)
    }
}

fn validate_replay_start(
    certificate: &OutputDistributionCertificate,
    start: SubscribeStart,
) -> Result<(), DbError> {
    let SubscribeStart::AsOfEpoch(requested) = start else {
        return Ok(());
    };
    if requested == 0 {
        return Err(ClusterSubscriptionError::EpochNotCommitted { requested }.into());
    }
    if certificate.history_retention_bytes == 0 {
        return Err(ClusterSubscriptionError::ReplayPruned { requested }.into());
    }
    Ok(())
}

fn spawn_reader(
    authority: Arc<LeaderLeaseStore>,
    store: Arc<dyn CheckpointStore>,
    certificate: Arc<OutputDistributionCertificate>,
    cursor: GatewayCursor,
    replay_pin: Option<laminar_core::cluster::control::SubscriptionReplayPin>,
    metrics: Option<Arc<EngineMetrics>>,
) -> ClusterSubscriptionReader {
    let (sender, receiver) = mpsc::channel(MAX_GATEWAY_QUEUE_FRAMES);
    let (cancel, cancel_rx) = tokio::sync::oneshot::channel();
    let terminal = Arc::new(Mutex::new(None));
    let task_terminal = Arc::clone(&terminal);
    let generation = certificate.stream_generation;
    let active_reader = ActiveReaderGuard::new(metrics.clone(), generation);
    let task = tokio::spawn(async move {
        let _active_reader = active_reader;
        let mut replay_pin = replay_pin.map(GatewayReplayPin::new);
        let result = tokio::select! {
            result = run_gateway(
                Arc::clone(&authority),
                store,
                certificate,
                cursor,
                &sender,
                metrics.as_ref(),
                &mut replay_pin,
            ) => result,
            _ = cancel_rx => Ok(()),
        };
        release_replay_pin(&authority, replay_pin.as_ref().map(|pin| &pin.pin)).await;
        if let Err(error) = result {
            record_terminal_error(metrics.as_deref(), &error);
            tracing::warn!(
                stream_generation = %generation,
                error_code = error.code(),
                error = %error,
                "committed cluster subscription terminated"
            );
            *task_terminal.lock() = Some(error);
        }
    });
    ClusterSubscriptionReader {
        receiver,
        terminal,
        cancel: Some(cancel),
        _task: task,
    }
}

fn take_terminal(terminal: &Mutex<Option<ClusterSubscriptionError>>) -> ClusterSubscriptionError {
    terminal
        .lock()
        .take()
        .unwrap_or(ClusterSubscriptionError::BackendUnavailable)
}

struct ActiveReaderGuard {
    metrics: Option<Arc<EngineMetrics>>,
    generation: StreamGeneration,
}

impl ActiveReaderGuard {
    fn new(metrics: Option<Arc<EngineMetrics>>, generation: StreamGeneration) -> Self {
        if let Some(metrics) = metrics.as_ref() {
            metrics.cluster_subscription.active_readers.inc();
        }
        Self {
            metrics,
            generation,
        }
    }
}

impl Drop for ActiveReaderGuard {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics.as_ref() {
            metrics.cluster_subscription.active_readers.dec();
        }
        tracing::info!(
            stream_generation = %self.generation,
            "closed committed cluster subscription"
        );
    }
}

impl Drop for ClusterSubscriptionReader {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }
    }
}

fn record_terminal_error(metrics: Option<&EngineMetrics>, error: &ClusterSubscriptionError) {
    let Some(metrics) = metrics else {
        return;
    };
    let metrics = &metrics.cluster_subscription;
    match error {
        ClusterSubscriptionError::ManifestCorrupt { .. } => {
            metrics.manifest_failures_total.inc();
            metrics.integrity_failures_total.inc();
        }
        ClusterSubscriptionError::SegmentMissing { .. }
        | ClusterSubscriptionError::SegmentCorrupt { .. }
        | ClusterSubscriptionError::SchemaMismatch
        | ClusterSubscriptionError::ConflictingDuplicateSequence => {
            metrics.integrity_failures_total.inc();
        }
        ClusterSubscriptionError::PartitionSequenceGap { .. } => {
            metrics.sequence_gaps_total.inc();
            metrics.integrity_failures_total.inc();
        }
        ClusterSubscriptionError::SubscriberLagged => {
            metrics.gateway_lag_disconnects_total.inc();
        }
        ClusterSubscriptionError::ReplayPruned { .. } => {
            metrics.replay_pruned_total.inc();
        }
        ClusterSubscriptionError::UnsupportedPlan { .. }
        | ClusterSubscriptionError::GenerationMismatch
        | ClusterSubscriptionError::EpochNotCommitted { .. }
        | ClusterSubscriptionError::StaleOutputWriter
        | ClusterSubscriptionError::AssignmentChanged
        | ClusterSubscriptionError::BackendUnavailable
        | ClusterSubscriptionError::ResumeTokenInvalid
        | ClusterSubscriptionError::ResumeTokenExpired
        | ClusterSubscriptionError::RetentionLost
        | ClusterSubscriptionError::ProtocolVersion { .. } => {}
    }
}
