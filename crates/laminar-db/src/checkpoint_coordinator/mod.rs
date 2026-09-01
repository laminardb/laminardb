//! Checkpoint capture and participant persistence.

#![allow(clippy::disallowed_types)] // checkpoint control path

mod allocation;
mod artifacts;
mod attempt;
mod attempt_failure;
mod capture;
mod commit_index;
mod coordinator;
#[cfg(feature = "cluster")]
mod follower_completion;
#[cfg(feature = "cluster")]
mod follower_prepare;
#[cfg(feature = "cluster")]
mod follower_protocol;
#[cfg(feature = "cluster")]
mod handoff;
mod protocol;
mod recovery;
mod request;
mod retention;
mod sink_artifact_intents;
mod sink_commit;
pub(crate) mod sink_epoch_admission;
mod sink_protocol;
#[cfg(feature = "cluster")]
mod subscription_output;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use crate::error::DbError;
pub(crate) use allocation::EpochAllocator;
#[cfg(feature = "cluster")]
use allocation::SinkEpochReservation;
use allocation::{checked_successor_epoch, require_canonical_attempt};
use bytes::Bytes;
pub use coordinator::CheckpointStats;
use coordinator::DurationHistogram;
pub(crate) use coordinator::{connector_to_source_checkpoint, source_to_connector_checkpoint};
use futures::{stream::FuturesUnordered, StreamExt};
use laminar_core::checkpoint::{
    channel_progress_frontiers_by_source, checkpoint_artifact_identity_sha256,
    checkpoint_descriptor_sha256, checkpoint_manifest_bytes, checkpoint_sha256,
    classify_channel_progress, ByteRange, ChannelProgress, CheckpointAttempt, CheckpointManifest,
    CheckpointScope, CheckpointStore, CheckpointWatermark, CommittedCheckpointIndex,
    CommittedCheckpointRef, CommittedParticipantRef, ConnectorCheckpoint, LeaderProof,
    PipelineIdentity, PreparedSinkArtifactIntent, PreparedSinkDescriptor, ReferencedStateChunk,
    StateChunkId, StateFrame, StateFrameKey, COMMITTED_CHECKPOINT_INDEX_VERSION,
};
use laminar_core::checkpoint_decision::{
    CheckpointArtifactInventory, CheckpointArtifactInventoryUpdateResult,
};
pub use protocol::{CheckpointFailureDisposition, CheckpointPhase, CheckpointResult};
#[cfg(feature = "cluster")]
pub(crate) use protocol::{FollowerPrepareOutcome, PrepareQuorum, QuorumPeer};
pub(crate) use protocol::{QuorumStage, SinkEpochPublication};
pub use request::{CapturedStateFrame, CheckpointConfig, CheckpointRequest};
pub(crate) use request::{ManagedVnodeOperator, ManagedVnodePlacement};
use retention::GcRequest;
use sha2::{Digest, Sha256};

const MAX_RETENTION_IO_CONCURRENCY: usize = 8;
const REFERENCED_CHUNK_REBASE_THRESHOLD: usize = 64;

pub(crate) struct RegisteredSink {
    name: String,
    handle: crate::sink_task::SinkTaskHandle,
    abort_cleaner: Option<Arc<dyn laminar_connectors::connector::CoordinatedAbortCleaner>>,
    abort_cleaner_retired: std::sync::atomic::AtomicBool,
}

struct PackedCheckpoint {
    manifest: CheckpointManifest,
    node_data: Vec<Bytes>,
}

#[cfg(test)]
mod artifact_tests;

#[cfg(test)]
mod external_sink_tests;

#[cfg(test)]
mod outcome_tests {
    use super::*;
    use laminar_core::checkpoint::ObjectStoreCheckpointStore;
    #[cfg(feature = "cluster")]
    use laminar_core::checkpoint_decision::CheckpointDecisionStore;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn outcome_recording_keeps_internal_and_prometheus_counts_in_lockstep() {
        let store = ObjectStoreCheckpointStore::new(Arc::new(InMemory::new()), "metrics");
        let mut coordinator =
            CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store)).unwrap();
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
        coordinator.set_metrics(Arc::clone(&metrics));

        coordinator.record_checkpoint_outcome(
            true,
            CheckpointAttempt::canonical(1),
            Duration::from_millis(10),
            Some(37),
        );
        coordinator.record_checkpoint_outcome(
            false,
            CheckpointAttempt::canonical(2),
            Duration::from_millis(20),
            None,
        );

        let stats = coordinator.stats();
        assert_eq!((stats.completed, stats.failed), (1, 1));
        assert_eq!(stats.last_duration, Some(Duration::from_millis(20)));
        assert_eq!(metrics.checkpoints_completed.get(), 1);
        assert_eq!(metrics.checkpoints_failed.get(), 1);
        assert_eq!(metrics.checkpoint_duration.get_sample_count(), 2);
        assert_eq!(metrics.checkpoint_epoch.get(), 2);
        assert_eq!(metrics.checkpoint_size_bytes.get(), 37);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn follower_installs_leader_certified_sink_epoch_without_allocating() {
        let objects: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let decisions = Arc::new(CheckpointDecisionStore::new(objects));
        let leader = EpochAllocator::new();
        leader.bind_decision_store(Arc::clone(&decisions)).unwrap();
        let follower = EpochAllocator::new();
        follower.bind_decision_store(decisions).unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);

        let leader_attempt = leader.reserve_sink_epoch_until(deadline).await.unwrap();
        follower
            .reserve_certified_sink_epoch_until(leader_attempt, deadline)
            .await
            .unwrap();
        follower.mark_sink_epoch_ready(leader_attempt).unwrap();
        leader.mark_sink_epoch_ready(leader_attempt).unwrap();

        assert_eq!(
            follower.consume_sink_epoch_until(deadline).await.unwrap(),
            leader_attempt
        );
        assert_eq!(
            leader.consume_sink_epoch_until(deadline).await.unwrap(),
            leader_attempt
        );
        assert_eq!(
            leader.reserve_sink_epoch_until(deadline).await.unwrap(),
            CheckpointAttempt::canonical(2),
            "the follower must not consume a second global checkpoint ID"
        );
    }
}

#[cfg(test)]
mod sparse_capture_tests;

#[cfg(all(test, feature = "cluster"))]
mod subscription_output_tests;

#[cfg(all(test, feature = "cluster"))]
mod handoff_tests;

struct ActiveSinkArtifactIntents {
    attempt: CheckpointAttempt,
    by_sink: BTreeMap<String, Option<Vec<u8>>>,
}

pub struct CheckpointCoordinator {
    config: CheckpointConfig,
    store: Arc<dyn CheckpointStore>,
    allocator: Arc<EpochAllocator>,
    phase: CheckpointPhase,
    sinks: Vec<RegisteredSink>,
    assignment_version: u64,
    assignment_scoped_sources: HashSet<String>,
    owned_vnodes: Vec<u32>,
    pipeline_identity: Option<PipelineIdentity>,
    deployment_id: Option<String>,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    active_sink_witness: Option<laminar_core::checkpoint_decision::CheckpointSinkOpenWitness>,
    active_sink_artifact_intents: Option<ActiveSinkArtifactIntents>,
    prepared: HashMap<CheckpointAttempt, (Arc<CheckpointManifest>, Bytes)>,
    last_committed_manifest: Option<Arc<CheckpointManifest>>,
    last_committed_ref: Option<CommittedCheckpointRef>,
    last_committed_source_watermarks: Option<(CommittedCheckpointRef, BTreeMap<String, i64>)>,
    failure_requires_recovery: bool,
    local_watermark: laminar_core::checkpoint::CheckpointWatermark,
    #[cfg(feature = "cluster")]
    cluster_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    #[cfg(feature = "cluster")]
    recovery_graph_payload_limit: usize,
    checkpoints_completed: u64,
    checkpoints_failed: u64,
    last_checkpoint_duration: Option<Duration>,
    duration_histogram: DurationHistogram,
    total_bytes_written: u64,
    prom: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    gc_requests: tokio::sync::watch::Sender<Option<GcRequest>>,
    gc_task: tokio::task::JoinHandle<()>,
}
