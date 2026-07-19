//! Streaming connector pipeline. Each source connector runs as a tokio task
//! pushing batches via crossfire mpsc to the `StreamingCoordinator`, which
//! drives SQL execution cycles, routes results to sinks, and manages
//! checkpoint barriers. See the `streaming_coordinator` submodule for the
//! runtime topology.

pub mod callback;
pub mod config;
pub mod streaming_coordinator;

pub(crate) use callback::CheckpointCompletion;
pub use callback::{
    BarrierOutcome, CheckpointAssignmentAdmission, CheckpointControlOutcome, CycleError,
    CycleOutcome, PipelineCallback, SkipReason, SourceRegistration,
};
pub use config::PipelineConfig;
pub use streaming_coordinator::{ExitReason, StreamingCoordinator, StreamingCoordinatorRuntime};

use laminar_sql::parser::EmitClause;
use laminar_sql::translator::{JoinOperatorConfig, OrderOperatorConfig, WindowOperatorConfig};

const CONTROL_MUTATION_PENDING: u8 = 0;
const CONTROL_MUTATION_APPLIED: u8 = 1;
const CONTROL_MUTATION_CANCELLED: u8 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlMutationState {
    Pending,
    Applied,
    Cancelled,
}

#[derive(Debug)]
pub(crate) struct ControlMutation {
    state: std::sync::atomic::AtomicU8,
}

impl ControlMutation {
    pub(crate) fn new() -> Self {
        Self {
            state: std::sync::atomic::AtomicU8::new(CONTROL_MUTATION_PENDING),
        }
    }

    pub(crate) fn try_apply(&self) -> bool {
        self.state
            .compare_exchange(
                CONTROL_MUTATION_PENDING,
                CONTROL_MUTATION_APPLIED,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn cancel(&self) -> ControlMutationState {
        match self.state.compare_exchange(
            CONTROL_MUTATION_PENDING,
            CONTROL_MUTATION_CANCELLED,
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
        ) {
            Ok(_) | Err(CONTROL_MUTATION_CANCELLED) => ControlMutationState::Cancelled,
            Err(CONTROL_MUTATION_APPLIED) => ControlMutationState::Applied,
            Err(_) => unreachable!("control mutation contains an invalid state"),
        }
    }

    pub(crate) fn state(&self) -> ControlMutationState {
        match self.state.load(std::sync::atomic::Ordering::Acquire) {
            CONTROL_MUTATION_PENDING => ControlMutationState::Pending,
            CONTROL_MUTATION_APPLIED => ControlMutationState::Applied,
            CONTROL_MUTATION_CANCELLED => ControlMutationState::Cancelled,
            _ => unreachable!("control mutation contains an invalid state"),
        }
    }
}

/// Opaque live-DDL message used by the streaming coordinator.
#[derive(Debug)]
pub struct ControlMsg(ControlMsgKind);

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum ControlMsgKind {
    AddStream {
        name: String,
        sql: String,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        order_config: Option<OrderOperatorConfig>,
        join_config: Option<Vec<JoinOperatorConfig>>,
        incremental: bool,
        reply: tokio::sync::oneshot::Sender<Result<(), crate::error::DbError>>,
        mutation: std::sync::Arc<ControlMutation>,
    },
    DropStreams {
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<(), crate::error::DbError>>,
        mutation: std::sync::Arc<ControlMutation>,
    },
}

impl ControlMsg {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_stream(
        name: String,
        sql: String,
        emit_clause: Option<EmitClause>,
        window_config: Option<WindowOperatorConfig>,
        order_config: Option<OrderOperatorConfig>,
        join_config: Option<Vec<JoinOperatorConfig>>,
        incremental: bool,
        reply: tokio::sync::oneshot::Sender<Result<(), crate::error::DbError>>,
        mutation: std::sync::Arc<ControlMutation>,
    ) -> Self {
        Self(ControlMsgKind::AddStream {
            name,
            sql,
            emit_clause,
            window_config,
            order_config,
            join_config,
            incremental,
            reply,
            mutation,
        })
    }

    pub(crate) fn drop_streams(
        names: Vec<String>,
        reply: tokio::sync::oneshot::Sender<Result<(), crate::error::DbError>>,
        mutation: std::sync::Arc<ControlMutation>,
    ) -> Self {
        Self(ControlMsgKind::DropStreams {
            names,
            reply,
            mutation,
        })
    }

    pub(crate) fn into_kind(self) -> ControlMsgKind {
        self.0
    }
}
