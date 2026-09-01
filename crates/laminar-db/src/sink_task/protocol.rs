//! Sink task protocol surface: channel/event constants, the event stream,
//! task configuration, client commands, and the epoch-gate admission types.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use crossfire::{mpsc, oneshot, AsyncRx, MAsyncTx};

/// Bounded command channel between the handle and the sink actor.
pub(super) type SinkCommandTx = MAsyncTx<mpsc::Array<SinkCommand>>;
pub(super) type SinkCommandRx = AsyncRx<mpsc::Array<SinkCommand>>;
#[cfg(feature = "cluster")]
use laminar_core::cluster::control::ClusterController;

pub(super) use laminar_connectors::connector::{
    ConnectorTaskTracker, CoordinatedCommitBatch, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, SinkConnector, SinkContract,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::streaming::Producer;
use tokio::time::Instant;

pub(crate) const DEFAULT_CHANNEL_CAPACITY: usize = 32;

/// Default periodic flush interval for sink tasks.
#[cfg(test)]
pub(crate) const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) const SINK_EVENT_CHANNEL_CAPACITY: usize = 1024;
pub(super) const SINK_CLOSE_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub(crate) enum SinkEvent {
    FlushError {
        sink_id: Arc<str>,
        epoch: u64,
        operation: &'static str,
        error: String,
    },
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
    WriteEnqueueTimeout {
        sink_id: Arc<str>,
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
    /// Typed contract already validated by pipeline admission.
    pub contract: SinkContract,
    /// Whether an asynchronous sink failure requires replay/recovery. Best-effort local
    /// pipelines report the loss but deliberately do not leave future state checkpoints wedged.
    pub requires_recovery_on_error: bool,
    pub channel_capacity: usize,
    pub flush_interval: Duration,
    pub write_timeout: Duration,
    pub event_tx: Producer<SinkEvent>,
    /// Exact generation proof captured when the connector was created.
    pub terminal_tasks: Option<ConnectorTaskTracker>,
    #[cfg(feature = "cluster")]
    pub process_authority: Option<Arc<ClusterController>>,
}

pub(crate) struct SinkCommand {
    /// One deadline created before enqueue and shared by queueing, connector I/O and ack.
    pub(super) deadline: Instant,
    pub(super) operation: SinkOperation,
}

pub(crate) enum SinkOperation {
    WriteBatch {
        /// Exact epoch generation admitted by the handle-side write gate. Non-committable sinks
        /// do not participate in epoch gating and leave this unset.
        epoch: Option<SinkEpochAdmission>,
        batch: RecordBatch,
    },
    BeginEpoch {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    ArtifactIntent {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<Option<Vec<u8>>, ConnectorError>>,
    },
    /// Flush buffered rows without transaction semantics — used to durably land an
    /// at-least-once sink's buffer at checkpoint (CP-5).
    Flush {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    PreCommit {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<Option<Vec<u8>>, ConnectorError>>,
    },
    /// Designated-committer path: aggregate every writer's descriptor for the
    /// epoch into one external commit (coordinated-commit sinks only).
    CommitAggregated {
        batch: CoordinatedCommitBatch,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Highest exact checkpoint and authority committed in this external namespace.
    CommittedCursor {
        namespace: CoordinatedCommitNamespace,
        ack: oneshot::TxOneshot<Result<Option<CoordinatedCommitCursor>, ConnectorError>>,
    },
    RollbackEpoch {
        epoch: u64,
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Acks once all prior commands have been processed.
    Sync {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
    /// Close the connector (abort open transaction, flush) and exit the task. The task reports
    /// the connector result before terminating so shutdown cannot confuse enqueue with durability.
    Close {
        ack: oneshot::TxOneshot<Result<(), ConnectorError>>,
    },
}

/// Handle-side admission state for checkpoint-committable sink epochs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SinkEpochGateState {
    Unopened,
    Open(SinkEpochAdmission),
    Sealed(SinkEpochAdmission),
    Opening(SinkEpochAdmission),
    Begun(SinkEpochAdmission),
    Failed { generation: u64 },
}

/// Exact handle-side generation admitted for one writable sink epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SinkEpochAdmission {
    pub(crate) epoch: u64,
    pub(crate) generation: u64,
}

pub(super) struct SinkBeginGateGuard {
    pub(super) gate: tokio::sync::watch::Sender<SinkEpochGateState>,
    pub(super) admission: SinkEpochAdmission,
    pub(super) disarmed: bool,
}

impl SinkBeginGateGuard {
    pub(super) fn disarm(&mut self) {
        self.disarmed = true;
    }
}

impl Drop for SinkBeginGateGuard {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        self.gate.send_if_modified(|state| {
            if *state == SinkEpochGateState::Opening(self.admission) {
                *state = SinkEpochGateState::Failed {
                    generation: self.admission.generation,
                };
                true
            } else {
                false
            }
        });
    }
}
