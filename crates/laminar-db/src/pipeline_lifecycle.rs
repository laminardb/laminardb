//! Pipeline lifecycle: start, close, shutdown.
#![allow(clippy::disallowed_types)] // cold path

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, DeliveryGuarantee, SinkConnector, SinkConsistency, SinkContract,
    SinkTopology, SourceConsistency, SourceContract, SourceInputMode, SourceRowPositionCapability,
    SourceTopology,
};
use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::checkpoint::{channel_progress_frontier, SINGLETON_WATERMARK_CHANNEL};
use rustc_hash::FxHashMap;

use crate::catalog::schema_has_reserved_mutation_columns;
use crate::connector_task_fence::ConnectorTaskFenceRegistration;
use crate::db::{
    exact_table_reference, DbState, LaminarDB, RecoveredInputChannelProgress, RuntimeMode,
    SourceWatermarkState,
};
#[cfg(feature = "cluster")]
use crate::db::{ClusterStartupDisposition, StartupCheckpointArtifactAudit};
use crate::error::DbError;
use crate::pipeline::streaming_coordinator::{admit_append_only_source, TrackedSourceRegistration};

const fn required_recovery_scope(runtime: RuntimeMode) -> CheckpointStorageScope {
    match runtime {
        RuntimeMode::Local => CheckpointStorageScope::NodeDurable,
        RuntimeMode::Cluster => CheckpointStorageScope::ClusterShared,
    }
}

const EXACT_SINK_PROTOCOL: &str =
    "exactly-once external sinks require checkpoint-committable consistency, coordinated phase \
     1, an immutable committed checkpoint index, and a namespaced exact external cursor";
const CLUSTER_BEST_EFFORT: &str =
    "cluster mode requires at_least_once delivery; best_effort has no defined \
     rebalance/state-loss contract";
const KEYED_SOURCE_PRIMARY_KEY: &str =
    "[LDB-5038] keyed-upsert sources require an explicit CREATE SOURCE PRIMARY KEY";
#[cfg(feature = "cluster")]
const CLUSTER_COMPUTE_THREAD_STACK_BYTES: usize = 4 * 1024 * 1024;
const PUBLIC_PIPELINE_STOP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
#[cfg(feature = "cluster")]
const COORDINATED_RECOVERY_STOP_FINALIZATION_MARGIN: std::time::Duration =
    std::time::Duration::from_secs(30);

fn checked_pipeline_deadline(
    timeout: std::time::Duration,
    context: &str,
) -> Result<tokio::time::Instant, DbError> {
    tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| {
            DbError::InvalidOperation(format!(
                "{context} timeout {timeout:?} exceeds the platform clock range"
            ))
        })
}

#[cfg(feature = "cluster")]
fn configured_checkpoint_timeout(config: &crate::config::LaminarConfig) -> std::time::Duration {
    config
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.timeout_ms)
        .map_or(
            crate::checkpoint_coordinator::CheckpointConfig::default().checkpoint_timeout,
            std::time::Duration::from_millis,
        )
}

#[cfg(feature = "cluster")]
fn coordinated_recovery_stop_ceiling(
    checkpoint_timeout: std::time::Duration,
    cleanup_timeout: std::time::Duration,
) -> std::time::Duration {
    checkpoint_timeout
        .saturating_add(cleanup_timeout)
        .saturating_add(cleanup_timeout)
        .saturating_add(COORDINATED_RECOVERY_STOP_FINALIZATION_MARGIN)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PipelineLifecycleAuthority {
    Public,
    #[cfg(feature = "cluster")]
    CoordinatedRecovery,
}

#[derive(Clone, Copy)]
enum StartupFailureKind {
    Config,
    Checkpoint,
    Connector,
    InvalidOperation,
    Shutdown,
    Pipeline,
    PipelineTerminal,
    BackpressureFail,
    ShuffleTerminal,
    ManagedStateBudgetExceeded {
        accounted_bytes: usize,
        limit_bytes: usize,
    },
}

#[derive(Clone)]
struct StartupFailure {
    kind: StartupFailureKind,
    message: Arc<str>,
}

impl StartupFailure {
    fn capture(error: DbError) -> Self {
        let (kind, message) = match error {
            DbError::Config(message) => (StartupFailureKind::Config, message),
            DbError::Checkpoint(message) => (StartupFailureKind::Checkpoint, message),
            DbError::CheckpointStore(error) => (StartupFailureKind::Checkpoint, error.to_string()),
            DbError::Connector(message) => (StartupFailureKind::Connector, message),
            DbError::ConnectorOp(error) => (StartupFailureKind::Connector, error.to_string()),
            DbError::InvalidOperation(message) => (StartupFailureKind::InvalidOperation, message),
            DbError::Shutdown => (StartupFailureKind::Shutdown, String::new()),
            DbError::Pipeline(message) => (StartupFailureKind::Pipeline, message),
            DbError::PipelineTerminal(message) => (StartupFailureKind::PipelineTerminal, message),
            DbError::BackpressureFail(message) => (StartupFailureKind::BackpressureFail, message),
            DbError::ShuffleTerminal(message) => (StartupFailureKind::ShuffleTerminal, message),
            DbError::ManagedStateBudgetExceeded {
                context,
                accounted_bytes,
                limit_bytes,
            } => (
                StartupFailureKind::ManagedStateBudgetExceeded {
                    accounted_bytes,
                    limit_bytes,
                },
                context,
            ),
            error => (StartupFailureKind::Pipeline, error.to_string()),
        };
        Self {
            kind,
            message: Arc::from(message),
        }
    }

    fn to_error(&self) -> DbError {
        let message = self.message.to_string();
        match self.kind {
            StartupFailureKind::Config => DbError::Config(message),
            StartupFailureKind::Checkpoint => DbError::Checkpoint(message),
            StartupFailureKind::Connector => DbError::Connector(message),
            StartupFailureKind::InvalidOperation => DbError::InvalidOperation(message),
            StartupFailureKind::Shutdown => DbError::Shutdown,
            StartupFailureKind::Pipeline => DbError::Pipeline(message),
            StartupFailureKind::PipelineTerminal => DbError::PipelineTerminal(message),
            StartupFailureKind::BackpressureFail => DbError::BackpressureFail(message),
            StartupFailureKind::ShuffleTerminal => DbError::ShuffleTerminal(message),
            StartupFailureKind::ManagedStateBudgetExceeded {
                accounted_bytes,
                limit_bytes,
            } => DbError::ManagedStateBudgetExceeded {
                context: message,
                accounted_bytes,
                limit_bytes,
            },
        }
    }
}

#[derive(Clone)]
enum StartupOutcome {
    Success,
    Failed(StartupFailure),
}

pub(crate) struct StartupAttempt {
    outcome: parking_lot::Mutex<Option<StartupOutcome>>,
    notify: tokio::sync::Notify,
}

impl StartupAttempt {
    fn new() -> Self {
        Self {
            outcome: parking_lot::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn complete(&self, result: Result<(), DbError>) {
        let outcome = match result {
            Ok(()) => StartupOutcome::Success,
            Err(error) => StartupOutcome::Failed(StartupFailure::capture(error)),
        };
        let mut stored = self.outcome.lock();
        if stored.is_some() {
            return;
        }
        *stored = Some(outcome);
        drop(stored);
        self.notify.notify_waiters();
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.outcome.lock().is_some()
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn completed_success_for_test() -> Arc<Self> {
        let attempt = Arc::new(Self::new());
        attempt.complete(Ok(()));
        attempt
    }

    async fn wait(&self) -> Result<(), DbError> {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(outcome) = self.outcome.lock().clone() {
                return match outcome {
                    StartupOutcome::Success => Ok(()),
                    StartupOutcome::Failed(error) => Err(error.to_error()),
                };
            }
            notified.await;
        }
    }
}

mod cluster_faults;
#[cfg(feature = "cluster")]
pub(crate) use cluster_faults::report_cluster_terminal_halt;
#[cfg(feature = "cluster")]
use cluster_faults::{queue_owned_cluster_compute_fault, report_cluster_compute_fault};
mod source_admission;
use source_admission::{
    admit_sink_contract, admit_source_contract, admit_source_recovery_contract,
    admit_temporal_source_contract, has_only_ordered_interval_consumers,
    has_only_temporal_right_consumers, validate_source_recovery_assignment,
    OrderedIntervalAdmissions, PipelineRecoveryState, PipelineSinkSetup, PipelineWatermarks,
    PreparedSink, SinkAdmissionContext, TemporalSourceRole,
};
mod watermarks;
use watermarks::{
    physical_recovered_input_channel_progress, recovered_source_watermark,
    restore_source_watermark_state, validate_recovered_input_channels,
    validate_recovered_source_watermark, PipelineRuntimeSetup, PreparedPipelineRuntime,
    ReferenceTableRuntimeSource,
};
mod reference_sources;
use reference_sources::{create_reference_table_sources, hydrate_reference_table_sources};
mod sink_admission;
#[cfg(test)]
use sink_admission::close_opened_sinks;
use sink_admission::{admit_sink, open_prepared_sinks};
mod output_schema;
pub(crate) use output_schema::{
    plan_output_schema, plan_temporal_output_schema, resolve_stream_output_schemas,
};
mod supervision;
#[cfg(test)]
use supervision::{backoff_for_attempt, claim_restart_slot};
#[cfg(feature = "cluster")]
use supervision::{
    latch_cluster_terminal_data_plane, publish_cluster_compute_fault_state,
    publish_cluster_terminal_compute_halt_state, retire_cluster_compute_generation,
    retire_cluster_compute_generation_until, PendingVnodeTransitionLaunchGuard,
};
use supervision::{
    panic_message, publish_runtime_fault_state, runtime_exit_is_covered_by_terminal_stop,
    spawn_supervised_restart, StartupDriverGuard,
};
mod authority;
mod cluster_startup;
mod operator_graph;
mod reference_tables;
mod runtime_launch;
mod runtime_preparation;
mod shutdown;
mod sink_preparation;
mod source_contracts;
mod startup;
mod startup_preparation;
mod state_recovery;

#[cfg(all(test, feature = "cluster"))]
mod recovery_stop_timeout_tests;

#[cfg(test)]
mod startup_failure_tests;

#[cfg(test)]
mod connector_admission_tests;

#[cfg(test)]
mod resolver_tests;

#[cfg(test)]
mod checkpoint_namespace_lock_tests;

#[cfg(test)]
mod mv_recovery_lifecycle_tests;

#[cfg(all(test, feature = "cluster"))]
mod cluster_fault_watcher_tests;

#[cfg(test)]
mod reference_table_recovery_tests;

#[cfg(test)]
mod supervisor_tests;
