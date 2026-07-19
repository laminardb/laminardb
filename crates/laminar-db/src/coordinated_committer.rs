//! Decoupled designated committer: off the barrier path, the leader reads each
//! writer's descriptor for sealed epochs and runs one external commit per sink.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitCursor, CoordinatedCommitNamespace,
    CoordinatedCommitPayload, MAX_COORDINATED_COMMIT_BATCH_BYTES,
    MAX_COORDINATED_COMMIT_BATCH_ENTRIES, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES,
};
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{canonical_json_bytes, canonical_json_sha256};
use laminar_core::checkpoint_decision::{CheckpointOutcome, CheckpointScope, CheckpointVerdict};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use laminar_core::state::{CheckpointAttempt, CheckpointSealInventory, StateBackend};
use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

#[cfg(feature = "cluster")]
use crate::cluster_recovery_capsule::{
    assemble_capsule, checked_participant_ready_total, participant_from_ready_key,
    ParticipantReady, MAX_PARTICIPANT_READY_BYTES, MAX_PARTICIPANT_READY_READ_CONCURRENCY,
    PARTICIPANT_READY_PREFIX,
};
use crate::error::DbError;
use crate::sink_task::SinkTaskHandle;

const PREPARED_MARKER_VERSION: u32 = 2;

fn checked_committer_floor_after(epoch: u64) -> Result<u64, DbError> {
    epoch.checked_add(1).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "[LDB-6050] checkpoint epoch space exhausted at {epoch} while advancing the coordinated-commit prune floor"
        ))
    })
}
const MAX_PREPARED_HEADER_BYTES: usize = 64 * 1024;
const MAX_PREPARED_MARKER_BYTES: u64 = (PREPARED_MARKER_MAGIC.len()
    + std::mem::size_of::<u32>()
    + MAX_PREPARED_HEADER_BYTES
    + MAX_COORDINATED_COMMIT_PAYLOAD_BYTES) as u64;
const MAX_DESCRIPTOR_READ_CONCURRENCY: usize = 4;
const MAX_SEAL_READ_CONCURRENCY: usize = 8;
const PREPARED_MARKER_MAGIC: &[u8; 8] = b"LDBCM2\0\0";

struct CommitInventory {
    attempts: Vec<CheckpointAttempt>,
    bindings: CommitBindings,
    observed_cursors: FxHashMap<String, CoordinatedCommitCursor>,
}

struct CommitBindings {
    outcomes: FxHashMap<CheckpointAttempt, CheckpointOutcome>,
    seals: FxHashMap<CheckpointAttempt, CheckpointSealInventory>,
}

struct RetainedOutcomeContinuity {
    artifact_before_epoch: u64,
    committed_checkpoint_id: Option<u64>,
    committed_anchor: Option<CheckpointOutcome>,
}

/// Immutable runtime-owned header around a connector's opaque committable. The payload follows
/// as raw bytes; serializing it as a JSON integer array would amplify a valid marker into
/// hundreds of MiB and make the encoder/decoder limits disagree.
#[derive(Debug, Serialize, Deserialize)]
struct PreparedSinkMarkerHeader {
    version: u32,
    attempt: CheckpointAttempt,
    pipeline_identity: PipelineIdentity,
    deployment_id: String,
    sink_id: String,
    participant_id: u64,
    payload_present: bool,
    payload_len: u64,
    payload_sha256: String,
}

fn payload_digest(payload: Option<&[u8]>) -> String {
    let mut digest = Sha256::new();
    match payload {
        Some(payload) => {
            digest.update([1]);
            digest.update((payload.len() as u64).to_be_bytes());
            digest.update(payload);
        }
        None => digest.update([0]),
    }
    let digest = digest.finalize();
    format!("{digest:x}")
}

fn outcome_cursor(outcome: &CheckpointOutcome) -> Result<CoordinatedCommitCursor, DbError> {
    let fencing_token = match outcome.scope {
        CheckpointScope::Local => 1,
        CheckpointScope::Cluster => {
            outcome
                .leader_proof
                .as_ref()
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "committer: cluster checkpoint {} has no leader proof for external fencing",
                        outcome.checkpoint_id
                    ))
                })?
                .fencing_token
        }
    };
    if fencing_token == 0 {
        return Err(DbError::Checkpoint(format!(
            "committer: checkpoint {} has a zero external fencing token",
            outcome.checkpoint_id
        )));
    }
    Ok(CoordinatedCommitCursor {
        checkpoint_id: outcome.checkpoint_id,
        fencing_token,
    })
}

/// Create-once descriptor key for one namespace participant within an attempt.
pub(crate) fn descriptor_key(
    namespace: &CoordinatedCommitNamespace,
    participant_id: u64,
) -> String {
    format!(
        "protocol={PREPARED_MARKER_VERSION}/namespace={}/participant={participant_id}",
        namespace.external_key()
    )
}

fn descriptor_namespace_prefix(namespace: &CoordinatedCommitNamespace) -> String {
    format!(
        "protocol={PREPARED_MARKER_VERSION}/namespace={}/participant=",
        namespace.external_key()
    )
}

/// Wrap one connector committable (including an explicit empty marker) in the
/// exact runtime namespace before persisting it.
pub(crate) fn encode_prepared_marker(
    namespace: &CoordinatedCommitNamespace,
    attempt: CheckpointAttempt,
    participant_id: u64,
    payload: Option<&[u8]>,
) -> Result<Vec<u8>, DbError> {
    if payload.is_some_and(|bytes| bytes.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES) {
        return Err(DbError::Checkpoint(format!(
            "coordinated sink '{}' descriptor exceeds {} bytes",
            namespace.sink_id, MAX_COORDINATED_COMMIT_PAYLOAD_BYTES
        )));
    }
    let marker = PreparedSinkMarkerHeader {
        version: PREPARED_MARKER_VERSION,
        attempt,
        pipeline_identity: namespace.pipeline_identity.clone(),
        deployment_id: namespace.deployment_id.clone(),
        sink_id: namespace.sink_id.clone(),
        participant_id,
        payload_present: payload.is_some(),
        payload_len: payload.map_or(0, |bytes| bytes.len() as u64),
        payload_sha256: payload_digest(payload),
    };
    let header = serde_json::to_vec(&marker)
        .map_err(|error| DbError::Checkpoint(format!("encode prepared sink marker: {error}")))?;
    if header.len() > MAX_PREPARED_HEADER_BYTES {
        return Err(DbError::Checkpoint(
            "prepared sink marker metadata exceeds its bounded header size".into(),
        ));
    }
    let header_len = u32::try_from(header.len())
        .map_err(|_| DbError::Checkpoint("prepared sink marker header length overflow".into()))?;
    let payload_bytes = payload.unwrap_or_default();
    let mut encoded =
        Vec::with_capacity(PREPARED_MARKER_MAGIC.len() + 4 + header.len() + payload_bytes.len());
    encoded.extend_from_slice(PREPARED_MARKER_MAGIC);
    encoded.extend_from_slice(&header_len.to_be_bytes());
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(payload_bytes);
    Ok(encoded)
}

pub(crate) fn decode_prepared_marker(
    key: &str,
    bytes: &[u8],
    expected_attempt: CheckpointAttempt,
    namespace: &CoordinatedCommitNamespace,
) -> Result<CoordinatedCommitPayload, DbError> {
    if bytes.len() as u64 > MAX_PREPARED_MARKER_BYTES {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' exceeds the bounded envelope size"
        )));
    }
    if bytes.len() < PREPARED_MARKER_MAGIC.len() + 4
        || &bytes[..PREPARED_MARKER_MAGIC.len()] != PREPARED_MARKER_MAGIC
    {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' has invalid protocol magic"
        )));
    }
    let header_offset = PREPARED_MARKER_MAGIC.len();
    let header_len = u32::from_be_bytes(
        bytes[header_offset..header_offset + 4]
            .try_into()
            .map_err(|_| DbError::Checkpoint(format!("prepared sink marker '{key}' truncated")))?,
    ) as usize;
    if header_len > MAX_PREPARED_HEADER_BYTES || bytes.len() < header_offset + 4 + header_len {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' has invalid header length"
        )));
    }
    let payload_offset = header_offset + 4 + header_len;
    let payload_bytes = &bytes[payload_offset..];
    let marker: PreparedSinkMarkerHeader =
        serde_json::from_slice(&bytes[header_offset + 4..payload_offset]).map_err(|error| {
            DbError::Checkpoint(format!("decode prepared sink marker '{key}': {error}"))
        })?;
    if marker.version != PREPARED_MARKER_VERSION {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' has version {}, expected {PREPARED_MARKER_VERSION}",
            marker.version
        )));
    }
    if marker.attempt != expected_attempt
        || marker.pipeline_identity != namespace.pipeline_identity
        || marker.deployment_id != namespace.deployment_id
        || marker.sink_id != namespace.sink_id
        || descriptor_key(namespace, marker.participant_id) != key
    {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' does not match its sealed attempt/namespace"
        )));
    }
    if marker.payload_len > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES as u64
        || marker.payload_len != payload_bytes.len() as u64
        || (!marker.payload_present && marker.payload_len != 0)
    {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' payload length/presence mismatch"
        )));
    }
    let payload = marker.payload_present.then(|| payload_bytes.to_vec());
    if marker.payload_sha256 != payload_digest(payload.as_deref()) {
        return Err(DbError::Checkpoint(format!(
            "prepared sink marker '{key}' payload checksum mismatch"
        )));
    }
    Ok(CoordinatedCommitPayload {
        attempt: marker.attempt,
        participant_id: marker.participant_id,
        payload,
    })
}

/// Drives aggregated commits for coordinated-commit sinks on the leader.
pub(crate) struct CoordinatedCommitter {
    backend: Arc<dyn StateBackend>,
    sinks: Vec<(String, SinkTaskHandle)>,
    pipeline_identity: PipelineIdentity,
    deployment_id: String,
    /// External cursor binds the globally unique checkpoint id to its exact authority.
    committed_through: FxHashMap<String, CoordinatedCommitCursor>,
    /// Lowest uncommitted epoch, published for the coordinator's prune clamp.
    floor: Arc<AtomicU64>,
    /// Exact pending count shared with checkpoint admission, plus whether the initial external
    /// cursor/durable inventory reconciliation is trustworthy for the current leadership term.
    lag: Arc<AtomicU64>,
    lag_known: Arc<AtomicBool>,
    progress: Arc<tokio::sync::Notify>,
    /// Cursors seeded from each sink's external commit state on the first pass.
    seeded: bool,
    /// Lag (sealed − committed) past which a loud warning fires.
    max_uncommitted_epochs: u64,
    metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    decision_store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    outcome_scope: CheckpointScope,
    storage_timeout: std::time::Duration,
    #[cfg(feature = "cluster")]
    controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
}

impl CoordinatedCommitter {
    pub(crate) fn new(
        backend: Arc<dyn StateBackend>,
        sinks: Vec<(String, SinkTaskHandle)>,
        pipeline_identity: PipelineIdentity,
        deployment_id: String,
        floor: Arc<AtomicU64>,
    ) -> Self {
        Self {
            backend,
            sinks,
            pipeline_identity,
            deployment_id,
            committed_through: FxHashMap::default(),
            floor,
            lag: Arc::new(AtomicU64::new(0)),
            lag_known: Arc::new(AtomicBool::new(false)),
            progress: Arc::new(tokio::sync::Notify::new()),
            seeded: false,
            max_uncommitted_epochs: u64::MAX,
            metrics: None,
            decision_store: None,
            outcome_scope: CheckpointScope::Local,
            storage_timeout: std::time::Duration::from_secs(120),
            #[cfg(feature = "cluster")]
            controller: None,
        }
    }

    pub(crate) fn with_metrics(
        mut self,
        metrics: Option<Arc<crate::engine_metrics::EngineMetrics>>,
    ) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn with_max_uncommitted_epochs(mut self, cap: u64) -> Self {
        self.max_uncommitted_epochs = cap;
        self
    }

    pub(crate) fn with_lag_state(
        mut self,
        lag: Arc<AtomicU64>,
        lag_known: Arc<AtomicBool>,
        progress: Arc<tokio::sync::Notify>,
    ) -> Self {
        self.lag = lag;
        self.lag_known = lag_known;
        self.progress = progress;
        self
    }

    fn mark_lag_unknown(&self) {
        self.lag_known.store(false, Ordering::Release);
        self.progress.notify_one();
    }

    fn publish_lag(&self, lag: u64) {
        self.lag.store(lag, Ordering::Release);
        self.lag_known.store(true, Ordering::Release);
        self.progress.notify_one();
    }

    pub(crate) fn with_decision_store(
        mut self,
        store: Option<Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>>,
    ) -> Self {
        self.decision_store = store;
        self
    }

    pub(crate) fn with_storage_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.storage_timeout = timeout;
        self
    }

    async fn storage_io<T, E>(
        &self,
        context: &str,
        future: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, DbError>
    where
        E: std::fmt::Display,
    {
        tokio::time::timeout(self.storage_timeout, future)
            .await
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "committer: {context} timed out after {:?}",
                    self.storage_timeout
                ))
            })?
            .map_err(|error| DbError::Checkpoint(format!("committer: {context}: {error}")))
    }

    /// Restrict committing to the cluster leader (the single designated committer).
    #[cfg(feature = "cluster")]
    pub(crate) fn with_cluster_controller(
        mut self,
        controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
    ) -> Self {
        self.outcome_scope = if controller.is_some() {
            CheckpointScope::Cluster
        } else {
            CheckpointScope::Local
        };
        self.controller = controller;
        self
    }

    /// Commit every sealed-but-uncommitted epoch. Per-sink isolated: a sink that
    /// fails stops at its cursor while others proceed; the first error is returned.
    pub(crate) async fn commit_ready(&mut self) -> Result<(), DbError> {
        // Only the designated committer (the lease-fenced leader) commits, so
        // writers never race on the shared catalog. `is_leader` is lease-aware,
        // so a stale/partitioned candidate stands down here. Drop the seed so a
        // regained leadership re-reads the catalog cursor, not a stale one.
        #[cfg(feature = "cluster")]
        if self.controller.as_ref().is_some_and(|c| !c.is_leader()) {
            self.seeded = false;
            self.mark_lag_unknown();
            return Ok(());
        }

        let inventory = match self.load_commit_inventory().await {
            Ok(inventory) => inventory,
            Err(error) => {
                self.mark_lag_unknown();
                return Err(error);
            }
        };
        let CommitInventory {
            attempts: committed_attempts,
            bindings,
            observed_cursors,
        } = inventory;
        self.committed_through = observed_cursors;
        self.seeded = true;
        let high_epoch = committed_attempts.last().map(|attempt| attempt.epoch);

        let mut first_err: Option<DbError> = None;
        for (name, handle) in &self.sinks {
            let cursor =
                self.committed_through
                    .get(name)
                    .copied()
                    .unwrap_or(CoordinatedCommitCursor {
                        checkpoint_id: 0,
                        fencing_token: 0,
                    });
            let sealed: Vec<CheckpointAttempt> = committed_attempts
                .iter()
                .copied()
                .filter(|attempt| attempt.checkpoint_id > cursor.checkpoint_id)
                .collect();
            let Some(&target) = sealed.last() else {
                continue;
            };
            match self
                .commit_sealed(handle, name, cursor, &sealed, target, &bindings)
                .await
            {
                Ok(committed_cursor) => {
                    self.committed_through
                        .insert(name.clone(), committed_cursor);
                }
                Err(e) => {
                    first_err.get_or_insert(e); // leave the cursor; retry next pass
                }
            }
        }

        // Publish the prune floor (lowest uncommitted epoch) + lag metric, and
        // warn if the committer is falling behind so storage isn't growing blind.
        let min_committed = self.min_committed();
        let first_uncommitted = committed_attempts
            .iter()
            .find(|attempt| attempt.checkpoint_id > min_committed)
            .map(|attempt| attempt.epoch);
        let floor_epoch = match (first_uncommitted, high_epoch) {
            (Some(epoch), _) => Some(epoch),
            (None, Some(epoch)) => Some(checked_committer_floor_after(epoch)?),
            (None, None) => None,
        };
        if let Some(floor_epoch) = floor_epoch {
            self.floor.store(floor_epoch, Ordering::Release);
        }
        let lag = committed_attempts
            .iter()
            .filter(|attempt| attempt.checkpoint_id > min_committed)
            .count() as u64;
        self.publish_lag(lag);
        if let Some(m) = &self.metrics {
            m.coordinated_committer_lag_epochs
                .set(i64::try_from(lag).unwrap_or(i64::MAX));
        }
        if lag > self.max_uncommitted_epochs {
            tracing::warn!(
                lag,
                cap = self.max_uncommitted_epochs,
                "[LDB-6030] coordinated committer is falling behind — descriptors and \
                 data files are accumulating in object storage"
            );
        }

        first_err.map_or(Ok(()), Err)
    }

    #[allow(clippy::too_many_lines)] // One snapshot audit establishes a single commit frontier.
    async fn load_commit_inventory(&self) -> Result<CommitInventory, DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "committer: coordinated commit requires a checkpoint decision store".into(),
            )
        })?;
        #[cfg(feature = "cluster")]
        let (outcomes, retention) = if self.outcome_scope == CheckpointScope::Cluster {
            let controller = self.controller.as_ref().ok_or_else(|| {
                DbError::Checkpoint(
                    "committer: cluster outcome inventory requires a cluster controller".into(),
                )
            })?;
            let authority = controller.checkpoint_authority().map_err(|error| {
                DbError::Checkpoint(format!(
                    "committer: cluster outcome inventory requires the exact checkpoint authority: {error}"
                ))
            })?;
            let outcomes = self
                .storage_io(
                    "cluster checkpoint outcome inventory read",
                    authority.cluster_outcomes(),
                )
                .await?;
            let boundary = self
                .storage_io(
                    "cluster checkpoint outcome retention boundary read",
                    authority.cluster_outcome_retention_boundary(),
                )
                .await?;
            let committed_checkpoint_id = boundary
                .committed_anchor
                .as_ref()
                .map(|outcome| outcome.checkpoint_id);
            (
                outcomes,
                RetainedOutcomeContinuity {
                    artifact_before_epoch: boundary.artifact_before_epoch,
                    committed_checkpoint_id,
                    committed_anchor: boundary.committed_anchor,
                },
            )
        } else {
            let outcomes = self
                .storage_io(
                    "checkpoint outcome inventory read",
                    decision_store.outcomes(),
                )
                .await?;
            let boundary = self
                .storage_io(
                    "checkpoint outcome retention boundary read",
                    decision_store.outcome_retention_boundary(),
                )
                .await?;
            (
                outcomes,
                RetainedOutcomeContinuity {
                    artifact_before_epoch: boundary.before_epoch,
                    committed_checkpoint_id: boundary.committed_checkpoint_id,
                    committed_anchor: None,
                },
            )
        };
        #[cfg(not(feature = "cluster"))]
        let (outcomes, retention) = {
            let outcomes = self
                .storage_io(
                    "checkpoint outcome inventory read",
                    decision_store.outcomes(),
                )
                .await?;
            let boundary = self
                .storage_io(
                    "checkpoint outcome retention boundary read",
                    decision_store.outcome_retention_boundary(),
                )
                .await?;
            (
                outcomes,
                RetainedOutcomeContinuity {
                    artifact_before_epoch: boundary.before_epoch,
                    committed_checkpoint_id: boundary.committed_checkpoint_id,
                    committed_anchor: None,
                },
            )
        };
        self.validate_outcome_headers(&outcomes)?;
        if let Some(anchor) = retention.committed_anchor.as_ref() {
            self.validate_outcome_headers(std::slice::from_ref(anchor))?;
            if retention.committed_checkpoint_id != Some(anchor.checkpoint_id) {
                return Err(DbError::Checkpoint(
                    "committer: compacted commit anchor does not match its continuity checkpoint"
                        .into(),
                ));
            }
        }
        let committed: Vec<CheckpointOutcome> = outcomes
            .into_iter()
            // A floor may advance after outcomes() selected its stable view. Apply the separately
            // audited scalar boundary before any seal read or connector call.
            .filter(|outcome| {
                outcome.epoch >= retention.artifact_before_epoch && outcome.is_commit()
            })
            .collect();
        let committed_attempts: Vec<CheckpointAttempt> = committed
            .iter()
            .map(|outcome| CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id))
            .collect();
        let mut outcome_bindings = FxHashMap::default();
        for outcome in committed {
            let attempt = CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id);
            if outcome_bindings.insert(attempt, outcome).is_some() {
                return Err(DbError::Checkpoint(format!(
                    "committer: duplicate durable commit outcome for epoch {} checkpoint {}",
                    attempt.epoch, attempt.checkpoint_id
                )));
            }
        }
        let observed_cursors = self.read_external_cursors().await?;
        let min_observed = self
            .sinks
            .iter()
            .map(|(name, _)| {
                observed_cursors
                    .get(name)
                    .map_or(0, |cursor| cursor.checkpoint_id)
            })
            .min()
            .unwrap_or(0);
        let seals = self
            .load_pending_seals(&committed_attempts, min_observed)
            .await?;
        self.validate_commit_continuity(
            &committed_attempts,
            retention.committed_checkpoint_id,
            retention.committed_anchor.as_ref(),
            &observed_cursors,
            &outcome_bindings,
            &seals,
        )?;
        #[cfg(feature = "cluster")]
        self.validate_cluster_recovery_capsules(&committed_attempts, &outcome_bindings, &seals)
            .await?;
        Ok(CommitInventory {
            attempts: committed_attempts,
            bindings: CommitBindings {
                outcomes: outcome_bindings,
                seals,
            },
            observed_cursors,
        })
    }

    fn validate_outcome_headers(&self, outcomes: &[CheckpointOutcome]) -> Result<(), DbError> {
        for outcome in outcomes {
            if outcome.deployment_id != self.deployment_id {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} outcome deployment '{}' does not match committer deployment '{}'",
                    outcome.checkpoint_id, outcome.deployment_id, self.deployment_id
                )));
            }
            if outcome.scope != self.outcome_scope {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} outcome scope {:?} does not match active runtime scope {:?}",
                    outcome.checkpoint_id, outcome.scope, self.outcome_scope
                )));
            }
        }
        if let Some(pair) = outcomes
            .windows(2)
            .find(|pair| pair[0].checkpoint_id >= pair[1].checkpoint_id)
        {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint outcome order regresses from epoch {} ID {} to epoch {} ID {}",
                pair[0].epoch, pair[0].checkpoint_id, pair[1].epoch, pair[1].checkpoint_id
            )));
        }
        Ok(())
    }

    async fn load_pending_seals(
        &self,
        committed_attempts: &[CheckpointAttempt],
        min_observed: u64,
    ) -> Result<FxHashMap<CheckpointAttempt, CheckpointSealInventory>, DbError> {
        let pending = committed_attempts
            .iter()
            .copied()
            .filter(|attempt| attempt.checkpoint_id > min_observed);
        let reads = futures::stream::iter(pending.map(|attempt| async move {
            let inventory = self
                .storage_io(
                    &format!(
                        "read seal inventory for checkpoint {}",
                        attempt.checkpoint_id
                    ),
                    self.backend.checkpoint_seal_inventory(attempt),
                )
                .await?;
            Ok::<_, DbError>((attempt, inventory))
        }))
        .buffer_unordered(MAX_SEAL_READ_CONCURRENCY);
        tokio::pin!(reads);
        let mut seals = FxHashMap::default();
        while let Some((attempt, inventory)) = reads.try_next().await? {
            if let Some(inventory) = inventory {
                if seals.insert(attempt, inventory).is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "committer: duplicate seal inventory for checkpoint {}",
                        attempt.checkpoint_id
                    )));
                }
            }
        }
        Ok(seals)
    }

    /// Re-read every external cursor on every pass. Besides recovering an ambiguous prior commit,
    /// this detects a live target-catalog rollback instead of trusting stale memory.
    async fn read_external_cursors(
        &self,
    ) -> Result<FxHashMap<String, CoordinatedCommitCursor>, DbError> {
        let mut observed_cursors = FxHashMap::default();
        for (name, handle) in &self.sinks {
            let namespace = CoordinatedCommitNamespace::try_new(
                self.pipeline_identity.clone(),
                self.deployment_id.clone(),
                name.clone(),
            )
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            let observed = handle.committed_cursor(namespace).await.map_err(|error| {
                DbError::Checkpoint(format!(
                    "committer: external cursor read failed for sink '{name}': {error}"
                ))
            })?;
            let prior = self.committed_through.get(name).copied();
            if self.seeded {
                match (prior, observed) {
                    (Some(prior), None) => {
                        return Err(DbError::Checkpoint(format!(
                            "committer: external cursor for sink '{name}' rolled back from {} to 0",
                            prior.checkpoint_id
                        )));
                    }
                    (Some(prior), Some(observed))
                        if observed.checkpoint_id < prior.checkpoint_id =>
                    {
                        return Err(DbError::Checkpoint(format!(
                            "committer: external cursor for sink '{name}' rolled back from {} to {}",
                            prior.checkpoint_id, observed.checkpoint_id
                        )));
                    }
                    (Some(prior), Some(observed))
                        if observed.checkpoint_id == prior.checkpoint_id
                            && observed.fencing_token != prior.fencing_token =>
                    {
                        return Err(DbError::Checkpoint(format!(
                            "committer: external cursor for sink '{name}' changed fencing token at checkpoint {} from {} to {}",
                            observed.checkpoint_id,
                            prior.fencing_token,
                            observed.fencing_token
                        )));
                    }
                    (Some(prior), Some(observed))
                        if observed.checkpoint_id > prior.checkpoint_id
                            && observed.fencing_token < prior.fencing_token =>
                    {
                        return Err(DbError::Checkpoint(format!(
                            "committer: external cursor for sink '{name}' advanced checkpoint {} to {} while fencing token regressed from {} to {}",
                            prior.checkpoint_id,
                            observed.checkpoint_id,
                            prior.fencing_token,
                            observed.fencing_token
                        )));
                    }
                    _ => {}
                }
            }
            if let Some(observed) = observed {
                observed_cursors.insert(name.clone(), observed);
            }
        }
        Ok(observed_cursors)
    }

    fn validate_commit_continuity(
        &self,
        committed_attempts: &[CheckpointAttempt],
        continuity_checkpoint_id: Option<u64>,
        continuity_outcome: Option<&CheckpointOutcome>,
        observed_cursors: &FxHashMap<String, CoordinatedCommitCursor>,
        outcomes: &FxHashMap<CheckpointAttempt, CheckpointOutcome>,
        seals: &FxHashMap<CheckpointAttempt, CheckpointSealInventory>,
    ) -> Result<(), DbError> {
        let min_observed = self
            .sinks
            .iter()
            .map(|(name, _)| {
                observed_cursors
                    .get(name)
                    .map_or(0, |cursor| cursor.checkpoint_id)
            })
            .min()
            .unwrap_or(0);
        // Every commit outcome still ahead of at least one sink must retain its exact seal and
        // participant inventory.
        for attempt in committed_attempts
            .iter()
            .filter(|attempt| attempt.checkpoint_id > min_observed)
        {
            if !seals.contains_key(attempt) {
                return Err(DbError::Checkpoint(format!(
                    "committer: durable commit outcome for epoch {} checkpoint {} has no exact state \
                     seal; external publication cannot skip the missing cut",
                    attempt.epoch, attempt.checkpoint_id
                )));
            }
        }

        let lowest = continuity_checkpoint_id.or_else(|| {
            committed_attempts
                .first()
                .map(|attempt| attempt.checkpoint_id)
        });
        let highest = committed_attempts
            .last()
            .map(|attempt| attempt.checkpoint_id);
        for (name, _) in &self.sinks {
            let Some(cursor) = observed_cursors.get(name).copied() else {
                // Once GC retains an anchor, zero proves target rollback: historical external
                // commits below the horizon can no longer be reconstructed.
                let valid = continuity_checkpoint_id.is_none()
                    && committed_attempts
                        .first()
                        .is_none_or(|attempt| seals.contains_key(attempt));
                if !valid {
                    return Err(DbError::Checkpoint(format!(
                        "committer: external cursor 0 for sink '{name}' is incompatible with \
                         retained outcome continuity {lowest:?}..={highest:?}"
                    )));
                }
                continue;
            };
            if cursor.fencing_token == 0 {
                return Err(DbError::Checkpoint(format!(
                    "committer: external cursor for sink '{name}' has a zero fencing token"
                )));
            }
            let live_attempt = committed_attempts
                .binary_search_by_key(&cursor.checkpoint_id, |attempt| attempt.checkpoint_id)
                .ok()
                .map(|index| committed_attempts[index]);
            let valid_checkpoint =
                continuity_checkpoint_id == Some(cursor.checkpoint_id) || live_attempt.is_some();
            if !valid_checkpoint || highest.is_some_and(|highest| cursor.checkpoint_id > highest) {
                return Err(DbError::Checkpoint(format!(
                    "committer: external cursor {} for sink '{name}' is incompatible with \
                     retained outcome continuity {lowest:?}..={highest:?}",
                    cursor.checkpoint_id
                )));
            }
            if let Some(attempt) = live_attempt {
                let outcome = outcomes.get(&attempt).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "committer: external cursor checkpoint {} has no authoritative outcome binding",
                        cursor.checkpoint_id
                    ))
                })?;
                let expected = outcome_cursor(outcome)?;
                if cursor != expected {
                    return Err(DbError::Checkpoint(format!(
                        "committer: external cursor checkpoint {} fencing token {} for sink '{name}' does not match authoritative token {}",
                        cursor.checkpoint_id, cursor.fencing_token, expected.fencing_token
                    )));
                }
            } else if let Some(anchor) = continuity_outcome {
                let expected = outcome_cursor(anchor)?;
                if cursor != expected {
                    return Err(DbError::Checkpoint(format!(
                        "committer: compacted external cursor checkpoint {} fencing token {} for sink '{name}' does not match authoritative token {}",
                        cursor.checkpoint_id, cursor.fencing_token, expected.fencing_token
                    )));
                }
            } else if self.outcome_scope == CheckpointScope::Local {
                if cursor.fencing_token != 1 {
                    return Err(DbError::Checkpoint(format!(
                        "committer: compacted local cursor checkpoint {} for sink '{name}' has fencing token {}, expected 1",
                        cursor.checkpoint_id, cursor.fencing_token
                    )));
                }
            } else {
                return Err(DbError::Checkpoint(format!(
                    "committer: compacted cluster cursor checkpoint {} for sink '{name}' has no exact authoritative outcome anchor",
                    cursor.checkpoint_id
                )));
            }
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    #[allow(clippy::too_many_lines)] // Capsule validation is one cohesive fail-closed recovery protocol.
    async fn validate_cluster_recovery_capsules(
        &self,
        committed_attempts: &[CheckpointAttempt],
        outcomes: &FxHashMap<CheckpointAttempt, CheckpointOutcome>,
        seals: &FxHashMap<CheckpointAttempt, CheckpointSealInventory>,
    ) -> Result<(), DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "committer: cluster recovery-capsule validation requires a checkpoint decision store"
                    .into(),
            )
        })?;

        for attempt in committed_attempts {
            let Some(inventory) = seals.get(attempt) else {
                continue;
            };
            let outcome = outcomes.get(attempt).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: checkpoint {} has no durable commit outcome binding",
                    attempt.checkpoint_id
                ))
            })?;
            if outcome.scope != CheckpointScope::Cluster {
                continue;
            }

            let reference = outcome.recovery_capsule.as_ref().ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: cluster checkpoint {} commit outcome has no recovery capsule",
                    attempt.checkpoint_id
                ))
            })?;
            let capsule = self
                .storage_io(
                    &format!(
                        "load recovery capsule for checkpoint {}",
                        attempt.checkpoint_id
                    ),
                    decision_store.load_recovery_capsule(reference),
                )
                .await?;

            if capsule.attempt != *attempt {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} recovery capsule attempt {:?} does not match {:?}",
                    attempt.checkpoint_id, capsule.attempt, attempt
                )));
            }
            if capsule.deployment_id != self.deployment_id
                || capsule.deployment_id != outcome.deployment_id
            {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} recovery capsule deployment '{}' does not match outcome deployment '{}'",
                    attempt.checkpoint_id, capsule.deployment_id, outcome.deployment_id
                )));
            }
            if Some(&capsule.assignment_fence) != outcome.assignment_fence.as_ref()
                || inventory.assignment_fence.as_ref() != Some(&capsule.assignment_fence)
            {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} recovery capsule assignment certificate does not match the outcome and sealed certificate",
                    attempt.checkpoint_id
                )));
            }
            if inventory.descriptor_leader_proof().map_err(|error| {
                DbError::Checkpoint(format!(
                    "committer: checkpoint {} has invalid descriptor provenance: {error}",
                    attempt.checkpoint_id
                ))
            })? != outcome.leader_proof.as_ref()
            {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} descriptor authority does not match its durable outcome",
                    attempt.checkpoint_id
                )));
            }

            let seal_inventory_sha256 = canonical_json_sha256(inventory).map_err(|error| {
                DbError::Checkpoint(format!(
                    "committer: canonicalize seal inventory for checkpoint {}: {error}",
                    attempt.checkpoint_id
                ))
            })?;
            if capsule.seal_inventory_sha256 != seal_inventory_sha256 {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} recovery capsule seal inventory digest does not match the durable seal",
                    attempt.checkpoint_id
                )));
            }

            let readiness = self
                .read_cluster_readiness_inventory(inventory, *attempt)
                .await?;
            let reproduced = assemble_capsule(
                inventory,
                readiness,
                &self.deployment_id,
                &self.pipeline_identity,
                capsule.cluster_watermark,
                capsule.recovery_watermark_frontier,
            )?;
            if reproduced != capsule {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} sealed participant readiness inventory does not reproduce the committed recovery capsule",
                    attempt.checkpoint_id
                )));
            }
        }
        Ok(())
    }

    /// Lowest committed checkpoint id across sinks (0 if any sink has committed nothing).
    fn min_committed(&self) -> u64 {
        self.sinks
            .iter()
            .map(|(name, _)| {
                self.committed_through
                    .get(name)
                    .map_or(0, |cursor| cursor.checkpoint_id)
            })
            .min()
            .unwrap_or(0)
    }

    /// Commit the `sealed` epochs' descriptors for one sink in transactions
    /// bounded at checkpoint boundaries. `target` is the highest attempt.
    async fn commit_sealed(
        &self,
        handle: &SinkTaskHandle,
        name: &str,
        predecessor: CoordinatedCommitCursor,
        sealed: &[CheckpointAttempt],
        target: CheckpointAttempt,
        bindings: &CommitBindings,
    ) -> Result<CoordinatedCommitCursor, DbError> {
        self.commit_sealed_with_limits(
            handle,
            name,
            predecessor,
            sealed,
            target,
            bindings,
            MAX_COORDINATED_COMMIT_BATCH_BYTES,
            MAX_COORDINATED_COMMIT_BATCH_ENTRIES,
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)] // One bounded commit transaction keeps its frontier and batching limits explicit.
    async fn commit_sealed_with_limits(
        &self,
        handle: &SinkTaskHandle,
        name: &str,
        predecessor: CoordinatedCommitCursor,
        sealed: &[CheckpointAttempt],
        target: CheckpointAttempt,
        bindings: &CommitBindings,
        max_batch_bytes: usize,
        max_batch_entries: usize,
    ) -> Result<CoordinatedCommitCursor, DbError> {
        if sealed.last().copied() != Some(target) {
            return Err(DbError::Checkpoint(
                "committer: aggregate target does not match the highest sealed attempt".into(),
            ));
        }
        let namespace = CoordinatedCommitNamespace::try_new(
            self.pipeline_identity.clone(),
            self.deployment_id.clone(),
            name.to_owned(),
        )
        .map_err(|error| DbError::Checkpoint(error.to_string()))?;
        let prefix = descriptor_namespace_prefix(&namespace);
        let mut entries = Vec::new();
        let mut batch_descriptor_bytes = 0usize;
        let mut batch_target = None;
        let mut batch_fencing_token = None;
        let mut batch_predecessor = predecessor;
        for &attempt in sealed {
            let outcome = bindings.outcomes.get(&attempt).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: checkpoint {} has no durable commit outcome binding",
                    attempt.checkpoint_id
                ))
            })?;
            let attempt_cursor = outcome_cursor(outcome)?;
            let inventory = bindings.seals.get(&attempt).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: checkpoint {} has no cached exact seal inventory",
                    attempt.checkpoint_id
                ))
            })?;
            let (mut attempt_entries, attempt_descriptor_bytes) = self
                .load_attempt_entries(name, &namespace, &prefix, attempt, outcome, inventory)
                .await?;
            if attempt_descriptor_bytes > max_batch_bytes
                || attempt_entries.len() > max_batch_entries
            {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} cannot fit in one bounded external commit",
                    attempt.checkpoint_id
                )));
            }

            let combined_bytes = batch_descriptor_bytes
                .checked_add(attempt_descriptor_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "committer: coordinated commit batch byte count overflow".into(),
                    )
                })?;
            let combined_entries = entries
                .len()
                .checked_add(attempt_entries.len())
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "committer: coordinated commit batch entry count overflow".into(),
                    )
                })?;
            if !entries.is_empty()
                && (combined_bytes > max_batch_bytes || combined_entries > max_batch_entries)
            {
                let flushed_target = batch_target.expect("non-empty commit batch has a target");
                let flushed_fencing_token =
                    batch_fencing_token.expect("non-empty commit batch has an authority token");
                self.submit_batch(
                    handle,
                    namespace.clone(),
                    batch_predecessor,
                    flushed_target,
                    flushed_fencing_token,
                    std::mem::take(&mut entries),
                )
                .await?;
                batch_predecessor = CoordinatedCommitCursor {
                    checkpoint_id: flushed_target.checkpoint_id,
                    fencing_token: flushed_fencing_token,
                };
                batch_descriptor_bytes = 0;
            }
            batch_descriptor_bytes = batch_descriptor_bytes
                .checked_add(attempt_descriptor_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "committer: coordinated commit batch byte count overflow".into(),
                    )
                })?;
            entries.append(&mut attempt_entries);
            batch_target = Some(attempt);
            batch_fencing_token = Some(attempt_cursor.fencing_token);
        }

        let final_target = batch_target.ok_or_else(|| {
            DbError::Checkpoint("committer: empty sealed checkpoint batch".into())
        })?;
        let final_fencing_token = batch_fencing_token.ok_or_else(|| {
            DbError::Checkpoint("committer: empty sealed checkpoint authority".into())
        })?;
        self.submit_batch(
            handle,
            namespace,
            batch_predecessor,
            final_target,
            final_fencing_token,
            entries,
        )
        .await?;
        Ok(CoordinatedCommitCursor {
            checkpoint_id: final_target.checkpoint_id,
            fencing_token: final_fencing_token,
        })
    }

    fn validate_attempt_inventory(
        &self,
        name: &str,
        prefix: &str,
        attempt: CheckpointAttempt,
        outcome: &CheckpointOutcome,
        inventory: &CheckpointSealInventory,
    ) -> Result<Vec<String>, DbError> {
        if inventory.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "committer: seal inventory identity mismatch for checkpoint {}",
                attempt.checkpoint_id
            )));
        }
        if outcome.epoch != attempt.epoch || outcome.checkpoint_id != attempt.checkpoint_id {
            return Err(DbError::Checkpoint(format!(
                "committer: outcome identity mismatch for checkpoint {}",
                attempt.checkpoint_id
            )));
        }
        if outcome.deployment_id != self.deployment_id {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} outcome deployment '{}' does not match committer deployment '{}'",
                attempt.checkpoint_id, outcome.deployment_id, self.deployment_id
            )));
        }
        if outcome.scope != self.outcome_scope {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} outcome scope {:?} does not match active runtime scope {:?}",
                attempt.checkpoint_id, outcome.scope, self.outcome_scope
            )));
        }
        if !matches!(&outcome.verdict, CheckpointVerdict::Commit) {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} is bound to an abort outcome",
                attempt.checkpoint_id
            )));
        }
        match outcome.scope {
            CheckpointScope::Local if inventory.assignment_fence.is_some() => {
                return Err(DbError::Checkpoint(format!(
                    "committer: local checkpoint {} has a cluster assignment certificate",
                    attempt.checkpoint_id
                )));
            }
            CheckpointScope::Cluster if inventory.assignment_fence != outcome.assignment_fence => {
                return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} outcome assignment certificate does not match the sealed certificate",
                    attempt.checkpoint_id
                )));
            }
            _ => {}
        }
        let descriptor_leader = inventory.descriptor_leader_proof().map_err(|error| {
            DbError::Checkpoint(format!(
                "committer: checkpoint {} has invalid descriptor provenance: {error}",
                attempt.checkpoint_id
            ))
        })?;
        match outcome.scope {
            CheckpointScope::Local if descriptor_leader.is_some() => {
                return Err(DbError::Checkpoint(format!(
                    "committer: local checkpoint {} has certified cluster descriptors",
                    attempt.checkpoint_id
                )));
            }
            CheckpointScope::Cluster if descriptor_leader != outcome.leader_proof.as_ref() => {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} descriptor authority does not match its durable outcome",
                    attempt.checkpoint_id
                )));
            }
            _ => {}
        }
        let required: Vec<String> = inventory
            .required_descriptors
            .iter()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        if required.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "committer: sealed checkpoint {} has no participant marker for sink '{name}'",
                attempt.checkpoint_id
            )));
        }
        if required.len() > MAX_COORDINATED_COMMIT_BATCH_ENTRIES {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} has {} markers for sink '{name}', exceeding the \
                 per-transaction limit of {MAX_COORDINATED_COMMIT_BATCH_ENTRIES}",
                attempt.checkpoint_id,
                required.len()
            )));
        }
        Ok(required)
    }

    #[cfg(feature = "cluster")]
    async fn read_cluster_readiness_inventory(
        &self,
        inventory: &CheckpointSealInventory,
        attempt: CheckpointAttempt,
    ) -> Result<Vec<(String, ParticipantReady)>, DbError> {
        if inventory.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "committer: seal inventory identity mismatch for checkpoint {}",
                attempt.checkpoint_id
            )));
        }
        let keys = inventory
            .required_descriptors
            .iter()
            .filter(|key| key.starts_with(PARTICIPANT_READY_PREFIX))
            .cloned()
            .collect::<Vec<_>>();
        let reads = futures::stream::iter(keys.into_iter().map(|key| async move {
            let participant_id = participant_from_ready_key(&key).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: checkpoint {} has non-canonical participant readiness key '{key}'",
                    attempt.checkpoint_id
                ))
            })?;
            let descriptor = inventory.sealed_descriptor(&key).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: participant readiness marker '{key}' has no sealed provenance"
                ))
            })?;
            if descriptor
                .writer
                .as_ref()
                .map(|writer| writer.participant.node_id)
                != Some(participant_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "committer: participant readiness marker '{key}' was not written by participant {participant_id}"
                )));
            }
            let bytes = self
                .storage_io(
                    &format!(
                        "read participant {participant_id} readiness for epoch {} checkpoint {}",
                        attempt.epoch, attempt.checkpoint_id
                    ),
                    self.backend.read_sealed_commit_descriptor_bounded(
                        attempt,
                        descriptor,
                        MAX_PARTICIPANT_READY_BYTES,
                    ),
                )
                .await?
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "committer: sealed participant readiness marker '{key}' is missing for checkpoint {}",
                        attempt.checkpoint_id
                    ))
            })?;
            Ok::<_, DbError>((key, participant_id, bytes))
        }))
        .buffer_unordered(MAX_PARTICIPANT_READY_READ_CONCURRENCY);
        tokio::pin!(reads);
        let mut retained_bytes = 0;
        let mut records = Vec::new();
        while let Some(record) = reads.try_next().await? {
            let (key, participant_id, bytes) = record;
            retained_bytes = checked_participant_ready_total(retained_bytes, bytes.len())?;
            let marker = serde_json::from_slice::<ParticipantReady>(&bytes).map_err(|error| {
                DbError::Checkpoint(format!(
                    "committer: participant {participant_id} readiness for checkpoint {} is corrupt: {error}",
                    attempt.checkpoint_id,
                ))
            })?;
            let canonical = canonical_json_bytes(&marker).map_err(|error| {
                DbError::Checkpoint(format!(
                    "committer: canonicalize participant {participant_id} readiness for checkpoint {}: {error}",
                    attempt.checkpoint_id
                ))
            })?;
            if bytes.as_ref() != canonical.as_slice() {
                return Err(DbError::Checkpoint(format!(
                    "committer: participant {participant_id} readiness for checkpoint {} does not use canonical ParticipantReady encoding",
                    attempt.checkpoint_id
                )));
            }
            records.push((key, marker));
        }
        Ok(records)
    }

    async fn load_attempt_entries(
        &self,
        name: &str,
        namespace: &CoordinatedCommitNamespace,
        prefix: &str,
        attempt: CheckpointAttempt,
        outcome: &CheckpointOutcome,
        inventory: &CheckpointSealInventory,
    ) -> Result<(Vec<CoordinatedCommitPayload>, usize), DbError> {
        let required =
            self.validate_attempt_inventory(name, prefix, attempt, outcome, inventory)?;

        let descriptors = futures::stream::iter(required.into_iter().map(|key| async move {
            let sealed = inventory.sealed_descriptor(&key).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: sealed marker '{key}' has no descriptor attestation"
                ))
            })?;
            let bytes = self
                .storage_io(
                    &format!(
                        "read descriptor '{key}' for epoch {} checkpoint {}",
                        attempt.epoch, attempt.checkpoint_id
                    ),
                    self.backend.read_sealed_commit_descriptor_bounded(
                        attempt,
                        sealed,
                        MAX_PREPARED_MARKER_BYTES,
                    ),
                )
                .await?;
            Ok::<_, DbError>((key, bytes))
        }))
        .buffer_unordered(MAX_DESCRIPTOR_READ_CONCURRENCY);
        tokio::pin!(descriptors);
        let mut entries = Vec::new();
        let mut descriptor_bytes = 0usize;
        while let Some((key, bytes)) = descriptors.try_next().await? {
            let bytes = bytes.ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: sealed marker '{key}' is missing for checkpoint {}",
                    attempt.checkpoint_id
                ))
            })?;
            descriptor_bytes = descriptor_bytes.checked_add(bytes.len()).ok_or_else(|| {
                DbError::Checkpoint("committer: checkpoint descriptor byte count overflow".into())
            })?;
            if descriptor_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} descriptors for sink '{name}' exceed {} bytes",
                    attempt.checkpoint_id, MAX_COORDINATED_COMMIT_BATCH_BYTES
                )));
            }
            let entry = decode_prepared_marker(&key, &bytes, attempt, namespace)?;
            match inventory.assignment_fence.as_ref() {
                Some(_) => {
                    let descriptor = inventory.sealed_descriptor(&key).ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "committer: sealed marker '{key}' has no descriptor provenance"
                        ))
                    })?;
                    if descriptor
                        .writer
                        .as_ref()
                        .map(|writer| writer.participant.node_id)
                        != Some(entry.participant_id)
                    {
                        return Err(DbError::Checkpoint(format!(
                            "committer: sealed marker '{key}' was not written by participant {}",
                            entry.participant_id
                        )));
                    }
                }
                None if inventory
                    .sealed_descriptor(&key)
                    .is_some_and(|descriptor| descriptor.writer.is_some()) =>
                {
                    return Err(DbError::Checkpoint(format!(
                        "committer: local marker '{key}' has cluster writer provenance"
                    )));
                }
                None => {}
            }
            entries.push(entry);
        }
        entries.sort_unstable_by_key(|entry| entry.participant_id);
        let actual_participants: Vec<u64> =
            entries.iter().map(|entry| entry.participant_id).collect();
        match &outcome.verdict {
            CheckpointVerdict::Commit => {}
            CheckpointVerdict::Abort => {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} is bound to an abort outcome",
                    attempt.checkpoint_id
                )));
            }
        }
        let expected_participants = outcome.assignment_fence.as_ref().map_or_else(
            || vec![0],
            laminar_core::checkpoint::CheckpointAssignmentFence::participant_ids,
        );
        if actual_participants != expected_participants {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} sink '{name}' participants {actual_participants:?} do not match outcome participants {:?}",
                attempt.checkpoint_id, expected_participants
            )));
        }
        Ok((entries, descriptor_bytes))
    }

    async fn submit_batch(
        &self,
        handle: &SinkTaskHandle,
        namespace: CoordinatedCommitNamespace,
        expected_predecessor: CoordinatedCommitCursor,
        target: CheckpointAttempt,
        fencing_token: u64,
        mut entries: Vec<CoordinatedCommitPayload>,
    ) -> Result<(), DbError> {
        entries.sort_unstable_by_key(|entry| (entry.attempt.checkpoint_id, entry.participant_id));
        handle
            .commit_aggregated(CoordinatedCommitBatch {
                namespace,
                expected_predecessor,
                fencing_token,
                target,
                entries,
            })
            .await
            .map_err(|e| {
                DbError::Checkpoint(format!(
                    "committer: commit through epoch {} checkpoint {}: {e}",
                    target.epoch, target.checkpoint_id
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use bytes::Bytes;
    use laminar_connectors::connector::{
        CoordinatedCommitter as CommitterTrait, SinkConnector, SinkConsistency, SinkContract,
        SinkInputMode, SinkTopology, WriteResult,
    };
    use laminar_connectors::error::ConnectorError;
    #[cfg(feature = "cluster")]
    use laminar_core::checkpoint::CheckpointWatermark;
    #[cfg(feature = "cluster")]
    use laminar_core::state::ObjectStoreBackend;
    use laminar_core::state::{InProcessBackend, StateBackend};
    use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt, PutPayload};
    use parking_lot::Mutex;

    #[cfg(feature = "cluster")]
    use crate::cluster_recovery_capsule::{
        participant_ready_key, ParticipantReady, PARTICIPANT_READY_VERSION,
    };
    use crate::sink_task::{SinkTaskConfig, SinkTaskHandle, DEFAULT_CHANNEL_CAPACITY};

    type Recorded = Arc<Mutex<Vec<CoordinatedCommitBatch>>>;
    type ExternalCursor = Arc<Mutex<Option<CoordinatedCommitCursor>>>;
    const TEST_SINK_ID: &str = "external";

    #[test]
    fn prune_floor_rejects_epoch_exhaustion() {
        let error = checked_committer_floor_after(u64::MAX).unwrap_err();
        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("epoch space exhausted"));
        assert_eq!(
            checked_committer_floor_after(u64::MAX - 1).unwrap(),
            u64::MAX
        );
    }

    struct RecordingSink {
        schema: SchemaRef,
        recorded: Recorded,
        committed: ExternalCursor,
    }

    #[async_trait::async_trait]
    impl SinkConnector for RecordingSink {
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
            Ok(WriteResult::new(0, 0))
        }
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
        fn suggested_write_timeout(&self) -> Duration {
            Duration::from_secs(5)
        }
        fn as_coordinated_committer(&self) -> Option<&dyn CommitterTrait> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl CommitterTrait for RecordingSink {
        async fn commit_aggregated(
            &self,
            batch: CoordinatedCommitBatch,
            _context: laminar_connectors::connector::CoordinatedCommitContext,
        ) -> Result<(), ConnectorError> {
            *self.committed.lock() = Some(CoordinatedCommitCursor {
                checkpoint_id: batch.target.checkpoint_id,
                fencing_token: batch.fencing_token,
            });
            self.recorded.lock().push(batch);
            Ok(())
        }

        async fn committed_cursor(
            &self,
            _namespace: &CoordinatedCommitNamespace,
        ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
            Ok(*self.committed.lock())
        }
    }

    fn spawn_recording_sink_with_cursor(
        recorded: Recorded,
        committed: ExternalCursor,
    ) -> SinkTaskHandle {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let (event_tx, _rx) = laminar_core::streaming::channel::channel(
            crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
        );
        SinkTaskHandle::spawn(SinkTaskConfig {
            name: TEST_SINK_ID.into(),
            sink_id: Arc::from(TEST_SINK_ID),
            connector: Box::new(RecordingSink {
                schema,
                recorded,
                committed,
            }),
            contract: SinkContract::new(
                SinkConsistency::CheckpointCommittable,
                SinkTopology::MultiWriter,
                SinkInputMode::AppendOnly,
            ),
            requires_recovery_on_error: true,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            flush_interval: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            event_tx,
            terminal_tasks: None,
            #[cfg(feature = "cluster")]
            process_authority: None,
        })
    }

    fn spawn_recording_sink(recorded: Recorded) -> SinkTaskHandle {
        spawn_recording_sink_with_cursor(recorded, Arc::new(Mutex::new(None)))
    }

    fn external_cursor(checkpoint_id: u64, fencing_token: u64) -> ExternalCursor {
        Arc::new(Mutex::new(Some(CoordinatedCommitCursor {
            checkpoint_id,
            fencing_token,
        })))
    }

    fn identity() -> PipelineIdentity {
        PipelineIdentity::empty()
    }

    fn deployment_id() -> String {
        "018f0000-0000-7000-8000-000000000001".into()
    }

    fn namespace() -> CoordinatedCommitNamespace {
        CoordinatedCommitNamespace::try_new(identity(), deployment_id(), TEST_SINK_ID).unwrap()
    }

    #[cfg(feature = "cluster")]
    fn descriptor_object_path(attempt: CheckpointAttempt, key: &str) -> object_store::path::Path {
        object_store::path::Path::from(format!(
            "state-v2/epoch={}/checkpoint={}/commit/{key}",
            attempt.epoch, attempt.checkpoint_id
        ))
    }

    fn assignment_fence(
        version: u64,
        participants: &[u64],
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

        let participants = participants
            .iter()
            .map(|node_id| CheckpointParticipant {
                node_id: *node_id,
                boot_incarnation: format!("00000000-0000-0000-0000-{node_id:012x}")
                    .parse()
                    .unwrap(),
            })
            .collect::<Vec<_>>();
        let owners = participants
            .iter()
            .map(|participant| participant.node_id)
            .collect::<Vec<_>>();
        CheckpointAssignmentFence::from_owner_map(version, &owners, participants).unwrap()
    }

    fn leader_proof(
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        node_id: u64,
        process_term: u64,
        fencing_token: u64,
    ) -> laminar_core::checkpoint::LeaderProof {
        laminar_core::checkpoint::LeaderProof {
            owner: laminar_core::checkpoint::LeaderProofOwner {
                node_id,
                boot_id: fence
                    .participant_incarnation(node_id)
                    .expect("test leader belongs to the assignment certificate"),
                process_term,
            },
            fencing_token,
        }
    }

    async fn seal<B: StateBackend>(
        backend: &Arc<B>,
        attempt: CheckpointAttempt,
        markers: &[(u64, Option<&[u8]>)],
    ) {
        seal_with_fence(backend, attempt, markers, &[], None, None).await;
    }

    async fn seal_with_fence<B: StateBackend>(
        backend: &Arc<B>,
        attempt: CheckpointAttempt,
        markers: &[(u64, Option<&[u8]>)],
        readiness_participants: &[u64],
        assignment_fence: Option<&laminar_core::checkpoint::CheckpointAssignmentFence>,
        leader_proof: Option<&laminar_core::checkpoint::LeaderProof>,
    ) {
        assert_eq!(
            assignment_fence.is_some(),
            leader_proof.is_some(),
            "cluster test descriptors require their exact outcome leader proof"
        );
        let assignment_version = assignment_fence.map_or(0, |fence| fence.assignment_version);
        backend.set_authoritative_version(assignment_version);
        let namespace = namespace();
        let mut keys = Vec::new();
        let mut required_vnodes = Vec::new();
        let vnode_owners = assignment_fence.map_or_else(Vec::new, |fence| {
            let owners = fence.participant_ids();
            assert!(
                fence.matches_owner_map(&owners),
                "test assignment helper uses one vnode per participant"
            );
            owners
        });
        if let Some(fence) = assignment_fence {
            for (vnode, &owner) in vnode_owners.iter().enumerate() {
                let vnode = u32::try_from(vnode).unwrap();
                backend
                    .write_certified_partial(
                        attempt,
                        vnode,
                        fence,
                        owner,
                        Bytes::from_static(b"test-vnode-state"),
                    )
                    .await
                    .unwrap();
                required_vnodes.push(vnode);
            }
        }
        for &(participant_id, payload) in markers {
            let key = descriptor_key(&namespace, participant_id);
            let marker =
                encode_prepared_marker(&namespace, attempt, participant_id, payload).unwrap();
            match (assignment_fence, leader_proof) {
                (Some(fence), Some(proof)) => backend
                    .write_certified_commit_descriptor(
                        attempt,
                        &key,
                        fence,
                        participant_id,
                        proof,
                        Bytes::from(marker),
                    )
                    .await
                    .unwrap(),
                (None, None) => backend
                    .write_commit_descriptor(attempt, &key, Bytes::from(marker))
                    .await
                    .unwrap(),
                _ => unreachable!("descriptor provenance shape was checked above"),
            }
            keys.push(key);
        }
        #[cfg(feature = "cluster")]
        for &participant_id in readiness_participants {
            let ready_key = participant_ready_key(participant_id);
            let ready = ParticipantReady {
                version: PARTICIPANT_READY_VERSION,
                attempt,
                participant_id,
                assignment_fence: assignment_fence
                    .expect("readiness requires an assignment fence")
                    .clone(),
                deployment_id: deployment_id(),
                pipeline_identity: identity(),
                owned_vnodes: vnode_owners
                    .iter()
                    .enumerate()
                    .filter_map(|(vnode, owner)| {
                        (*owner == participant_id).then(|| u32::try_from(vnode).unwrap())
                    })
                    .collect(),
                source_offsets: Default::default(),
                source_metadata: Default::default(),
                source_assignment_versions: Default::default(),
                source_watermarks: Default::default(),
                local_watermark: CheckpointWatermark::Uninitialized,
                manifest_sha256: format!("{participant_id:064x}"),
                portable_state_sha256: identity().sha256,
            };
            backend
                .write_certified_commit_descriptor(
                    attempt,
                    &ready_key,
                    assignment_fence.expect("readiness requires an assignment fence"),
                    participant_id,
                    leader_proof.expect("readiness requires an exact leader proof"),
                    Bytes::from(canonical_json_bytes(&ready).unwrap()),
                )
                .await
                .unwrap();
            keys.push(ready_key);
        }
        #[cfg(not(feature = "cluster"))]
        let _ = readiness_participants;
        keys.sort_unstable();
        assert!(backend
            .seal_checkpoint(attempt, assignment_fence, &required_vnodes, &keys)
            .await
            .unwrap());
    }

    async fn decisions_on(
        store: Arc<dyn ObjectStore>,
    ) -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
        let identity_json = format!(r#"{{"version":1,"id":"{}"}}"#, deployment_id());
        store
            .put(
                &object_store::path::Path::from("checkpoint-deployment/identity.json"),
                PutPayload::from_bytes(Bytes::from(identity_json)),
            )
            .await
            .unwrap();
        Arc::new(laminar_core::checkpoint_decision::CheckpointDecisionStore::new(store))
    }

    async fn decisions() -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
        decisions_on(Arc::new(InMemory::new())).await
    }

    #[cfg(feature = "cluster")]
    struct ClusterDecisions {
        backing: Arc<dyn ObjectStore>,
        capsules: Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore>,
        authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
        controller: Arc<laminar_core::cluster::control::ClusterController>,
        owner: laminar_core::cluster::control::LeaderLeaseOwner,
        proof: laminar_core::checkpoint::LeaderProof,
    }

    #[cfg(feature = "cluster")]
    impl std::ops::Deref for ClusterDecisions {
        type Target = laminar_core::checkpoint_decision::CheckpointDecisionStore;

        fn deref(&self) -> &Self::Target {
            &self.capsules
        }
    }

    #[cfg(feature = "cluster")]
    fn cluster_controller(
        authority: Arc<laminar_core::cluster::control::LeaderLeaseStore>,
        owner: &laminar_core::cluster::control::LeaderLeaseOwner,
        lease: laminar_core::cluster::control::LeaderLease,
    ) -> Arc<laminar_core::cluster::control::ClusterController> {
        use laminar_core::cluster::control::{
            ClusterController, ClusterKv, InMemoryKv, LeaseDeadline,
        };
        use tokio::sync::watch;

        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(owner.node));
        let (_members_tx, members_rx) = watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new(owner.node, kv, None, members_rx));
        controller.set_leader_lease_store(authority);
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))))
            .unwrap();
        let (_lease_tx, lease_rx) = watch::channel(Some(lease));
        controller
            .set_leader_lease_watch(
                lease_rx,
                owner.clone(),
                Arc::new(LeaseDeadline::live_for(Duration::from_secs(60))),
            )
            .unwrap();
        controller
    }

    #[cfg(feature = "cluster")]
    async fn cluster_decisions(
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        leader_id: u64,
    ) -> ClusterDecisions {
        use laminar_core::cluster::control::{LeaderLeaseOwner, LeaderLeaseStore, LeaseOutcome};

        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let capsules = decisions_on(Arc::clone(&backing)).await;
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&backing), 1));
        let owner = LeaderLeaseOwner {
            node: laminar_core::cluster::discovery::NodeId(leader_id),
            boot: fence
                .participant_incarnation(leader_id)
                .expect("test leader belongs to the assignment certificate"),
            process_term: 1,
        };
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            unreachable!("fresh test authority must grant its first lease")
        };
        let proof = lease.proof();
        let controller = cluster_controller(Arc::clone(&authority), &owner, lease);
        ClusterDecisions {
            backing,
            capsules,
            authority,
            controller,
            owner,
            proof,
        }
    }

    #[cfg(feature = "cluster")]
    async fn advance_cluster_term(
        decisions: &mut ClusterDecisions,
        next_owner: laminar_core::cluster::control::LeaderLeaseOwner,
    ) {
        use laminar_core::cluster::control::LeaseOutcome;

        let current = decisions.authority.load().await.unwrap().unwrap();
        let observation = decisions
            .authority
            .observe_rival(&next_owner, &current)
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        let LeaseOutcome::Acquired(lease) = decisions
            .authority
            .try_takeover(&next_owner, &observation, 0)
            .await
            .unwrap()
        else {
            panic!("replacement test term must acquire")
        };
        decisions.proof = lease.proof();
        decisions.controller =
            cluster_controller(Arc::clone(&decisions.authority), &next_owner, lease);
        decisions.owner = next_owner;
    }

    async fn record_local_commit(
        store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
    ) {
        store
            .record_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Commit,
                None,
            )
            .await
            .unwrap();
    }

    async fn record_local_abort(
        store: &laminar_core::checkpoint_decision::CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
    ) {
        store
            .record_outcome(
                epoch,
                checkpoint_id,
                CheckpointScope::Local,
                None,
                None,
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }

    #[cfg(feature = "cluster")]
    async fn record_cluster_commit<B: StateBackend>(
        store: &ClusterDecisions,
        backend: &Arc<B>,
        epoch: u64,
        checkpoint_id: u64,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        record_cluster_commit_with_inventory_digest(
            store,
            backend,
            epoch,
            checkpoint_id,
            fence,
            None,
        )
        .await;
    }

    #[cfg(feature = "cluster")]
    async fn record_cluster_commit_with_inventory_digest<B: StateBackend>(
        store: &ClusterDecisions,
        backend: &Arc<B>,
        epoch: u64,
        checkpoint_id: u64,
        fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
        seal_inventory_sha256: Option<String>,
    ) {
        let attempt = CheckpointAttempt::new(epoch, checkpoint_id);
        let inventory = backend
            .checkpoint_seal_inventory(attempt)
            .await
            .unwrap()
            .expect("cluster test commit has an exact seal");
        let mut readiness = Vec::new();
        for participant_id in fence.participant_ids() {
            let key = participant_ready_key(participant_id);
            let bytes = backend
                .read_commit_descriptor(attempt, &key)
                .await
                .unwrap()
                .expect("cluster test readiness descriptor exists");
            let ready = serde_json::from_slice::<ParticipantReady>(&bytes).unwrap();
            readiness.push((key, ready));
        }
        let mut capsule = assemble_capsule(
            &inventory,
            readiness,
            &deployment_id(),
            &identity(),
            CheckpointWatermark::Uninitialized,
            None,
        )
        .unwrap();
        // One negative test deliberately binds the outcome to a different assignment. Preserve
        // that fixture while deriving every normal capsule from its exact readiness descriptors.
        capsule.assignment_fence = fence.clone();
        if let Some(seal_inventory_sha256) = seal_inventory_sha256 {
            capsule.seal_inventory_sha256 = seal_inventory_sha256;
        }
        let capsule_ref = store.create_recovery_capsule(&capsule).await.unwrap();
        store
            .authority
            .record_cluster_outcome(
                &store.proof,
                epoch,
                checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Commit,
                Some(capsule_ref),
            )
            .await
            .unwrap();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn batches_sealed_epochs_into_one_commit() {
        let backend = Arc::new(InProcessBackend::new(2));
        let first = CheckpointAttempt::new(1, 11);
        let second = CheckpointAttempt::new(2, 22);
        let fence = assignment_fence(3, &[7, 9]);
        let decisions = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            first,
            &[(7, Some(b"e1")), (9, None)],
            &[7, 9],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;
        seal_with_fence(
            &backend,
            second,
            &[(7, Some(b"e2")), (9, None)],
            &[7, 9],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        record_cluster_commit(&decisions, &backend, 1, 11, &fence).await;
        record_cluster_commit(&decisions, &backend, 2, 22, &fence).await;
        let floor = Arc::new(AtomicU64::new(0));
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::clone(&floor),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        committer.commit_ready().await.unwrap();

        let batches = recorded.lock().clone();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].namespace, namespace());
        assert_eq!(
            batches[0].expected_predecessor,
            CoordinatedCommitCursor {
                checkpoint_id: 0,
                fencing_token: 0,
            }
        );
        assert_eq!(batches[0].fencing_token, 1);
        assert_eq!(batches[0].target, second);
        assert_eq!(batches[0].entries.len(), 4);
        assert_eq!(
            batches[0]
                .entries
                .iter()
                .map(|entry| (entry.attempt, entry.participant_id, entry.payload.clone()))
                .collect::<Vec<_>>(),
            vec![
                (first, 7, Some(b"e1".to_vec())),
                (first, 9, None),
                (second, 7, Some(b"e2".to_vec())),
                (second, 9, None),
            ]
        );
        assert_eq!(floor.load(Ordering::Acquire), 3);

        // A second pass with no new sealed epochs is a no-op (cursor advanced).
        committer.commit_ready().await.unwrap();
        assert_eq!(recorded.lock().len(), 1);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn split_batches_use_their_flushed_targets_authority() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempts = [
            CheckpointAttempt::new(1, 11),
            CheckpointAttempt::new(2, 22),
            CheckpointAttempt::new(3, 33),
        ];
        let tokens = [1, 2, 3];
        let fence = assignment_fence(3, &[7]);
        let mut outcomes = cluster_decisions(&fence, 7).await;
        for (index, attempt) in attempts.into_iter().enumerate() {
            assert_eq!(outcomes.proof.fencing_token, tokens[index]);
            seal_with_fence(
                &backend,
                attempt,
                &[(7, Some(b"payload"))],
                &[7],
                Some(&fence),
                Some(&outcomes.proof),
            )
            .await;
            record_cluster_commit(
                &outcomes,
                &backend,
                attempt.epoch,
                attempt.checkpoint_id,
                &fence,
            )
            .await;
            if index + 1 < attempts.len() {
                let next_owner = laminar_core::cluster::control::LeaderLeaseOwner {
                    node: outcomes.owner.node,
                    boot: outcomes.owner.boot,
                    process_term: outcomes.owner.process_term + 1,
                };
                advance_cluster_term(&mut outcomes, next_owner).await;
            }
        }
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle.clone())],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&outcomes.controller)))
        .with_decision_store(Some(Arc::clone(&outcomes.capsules)));
        let inventory = committer.load_commit_inventory().await.unwrap();

        let cursor = committer
            .commit_sealed_with_limits(
                &handle,
                TEST_SINK_ID,
                CoordinatedCommitCursor {
                    checkpoint_id: 0,
                    fencing_token: 0,
                },
                &inventory.attempts,
                attempts[2],
                &inventory.bindings,
                MAX_COORDINATED_COMMIT_BATCH_BYTES,
                1,
            )
            .await
            .unwrap();

        assert_eq!(
            cursor,
            CoordinatedCommitCursor {
                checkpoint_id: 33,
                fencing_token: 3,
            }
        );
        let batches = recorded.lock();
        assert_eq!(batches.len(), 3);
        assert_eq!(
            batches
                .iter()
                .map(|batch| (
                    batch.expected_predecessor,
                    batch.target.checkpoint_id,
                    batch.fencing_token,
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    CoordinatedCommitCursor {
                        checkpoint_id: 0,
                        fencing_token: 0,
                    },
                    11,
                    1,
                ),
                (
                    CoordinatedCommitCursor {
                        checkpoint_id: 11,
                        fencing_token: 1,
                    },
                    22,
                    2,
                ),
                (
                    CoordinatedCommitCursor {
                        checkpoint_id: 22,
                        fencing_token: 2,
                    },
                    33,
                    3,
                ),
            ]
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn live_cluster_cursor_must_match_its_outcome_authority() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(1, 11);
        let fence = assignment_fence(3, &[7]);
        let mut outcomes = cluster_decisions(&fence, 7).await;
        let next_owner = laminar_core::cluster::control::LeaderLeaseOwner {
            node: outcomes.owner.node,
            boot: outcomes.owner.boot,
            process_term: outcomes.owner.process_term + 1,
        };
        advance_cluster_term(&mut outcomes, next_owner).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload"))],
            &[7],
            Some(&fence),
            Some(&outcomes.proof),
        )
        .await;
        record_cluster_commit(
            &outcomes,
            &backend,
            attempt.epoch,
            attempt.checkpoint_id,
            &fence,
        )
        .await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink_with_cursor(
            Arc::clone(&recorded),
            external_cursor(attempt.checkpoint_id, 1),
        );
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&outcomes.controller)))
        .with_decision_store(Some(Arc::clone(&outcomes.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match authoritative token 2"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn external_commit_rejects_capsule_bound_to_another_seal_inventory() {
        let backend = Arc::new(InProcessBackend::new(2));
        let attempt = CheckpointAttempt::new(1, 11);
        let fence = assignment_fence(3, &[7, 9]);
        let decisions = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload")), (9, None)],
            &[7, 9],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        record_cluster_commit_with_inventory_digest(
            &decisions,
            &backend,
            attempt.epoch,
            attempt.checkpoint_id,
            &fence,
            Some("ff".repeat(32)),
        )
        .await;
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(
                TEST_SINK_ID.into(),
                spawn_recording_sink(Arc::clone(&recorded)),
            )],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("recovery capsule seal inventory digest does not match"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn current_leader_finishes_commit_selected_by_predecessor_proof() {
        let backend = Arc::new(InProcessBackend::new(2));
        let attempt = CheckpointAttempt::new(1, 11);
        let fence = assignment_fence(3, &[7, 9]);
        let mut outcomes = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"old-leader")), (9, None)],
            &[7, 9],
            Some(&fence),
            Some(&outcomes.proof),
        )
        .await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        // The immutable outcome is certified by predecessor node 7. Node 9 is the current
        // designated committer and must finish it without asking whether proof 7 is still live.
        record_cluster_commit(&outcomes, &backend, 1, 11, &fence).await;
        let successor = laminar_core::cluster::control::LeaderLeaseOwner {
            node: laminar_core::cluster::discovery::NodeId(9),
            boot: fence.participant_incarnation(9).unwrap(),
            process_term: 1,
        };
        advance_cluster_term(&mut outcomes, successor).await;
        assert!(outcomes.controller.is_leader());

        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&outcomes.controller)))
        .with_decision_store(Some(Arc::clone(&outcomes.capsules)));

        committer.commit_ready().await.unwrap();

        let batches = recorded.lock();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].target, attempt);
    }

    #[tokio::test]
    async fn skips_abort_outcome_with_partial_descriptor() {
        let backend = Arc::new(InProcessBackend::new(2));
        let first = CheckpointAttempt::new(1, 11);
        let abandoned = CheckpointAttempt::new(2, 19);
        let third = CheckpointAttempt::new(3, 31);
        seal(&backend, first, &[(0, Some(b"e1"))]).await;
        // Epoch 2 wrote a descriptor but durably selected abort (and was never sealed).
        let namespace = namespace();
        let orphan_key = descriptor_key(&namespace, 0);
        let orphan = encode_prepared_marker(&namespace, abandoned, 0, Some(b"orphan")).unwrap();
        backend
            .write_commit_descriptor(abandoned, &orphan_key, Bytes::from(orphan))
            .await
            .unwrap();
        seal(&backend, third, &[(0, Some(b"e3"))]).await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions().await;
        record_local_commit(&decisions, 1, 11).await;
        record_local_abort(&decisions, 2, 19).await;
        record_local_commit(&decisions, 3, 31).await;
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        committer.commit_ready().await.unwrap();

        // Epochs 1 and 3 batch into one commit keyed by 3; epoch 2's aborted
        // descriptor must not enter seal validation or external commit.
        let batches = recorded.lock().clone();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].fencing_token, 1);
        assert_eq!(batches[0].target, third);
        assert_eq!(
            batches[0]
                .entries
                .iter()
                .filter_map(|entry| entry.payload.as_deref())
                .collect::<Vec<_>>(),
            vec![b"e1".as_slice(), b"e3".as_slice()]
        );
    }

    /// On restart/failover a fresh committer seeds its cursor from the sink's
    /// external commit state and must not re-commit already-committed epochs.
    #[tokio::test]
    async fn restart_resumes_from_exact_external_cursor() {
        let backend = Arc::new(InProcessBackend::new(2));
        seal(&backend, CheckpointAttempt::new(1, 11), &[(0, Some(b"e1"))]).await;
        seal(&backend, CheckpointAttempt::new(2, 22), &[(0, Some(b"e2"))]).await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions().await;
        record_local_commit(&decisions, 1, 11).await;
        record_local_commit(&decisions, 2, 22).await;

        let mut first = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle.clone())],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(Arc::clone(&decisions)));
        first.commit_ready().await.unwrap();
        assert_eq!(recorded.lock().len(), 1);

        // Fresh committer (restart) over the same sink — seeds from committed_through.
        let mut restarted = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));
        restarted.commit_ready().await.unwrap();
        assert_eq!(
            recorded.lock().len(),
            1,
            "restart must not re-commit already-committed epochs"
        );
    }

    #[tokio::test]
    async fn live_local_cursor_uses_the_fixed_local_authority() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(1, 11);
        seal(&backend, attempt, &[(0, Some(b"payload"))]).await;
        let outcomes = decisions().await;
        record_local_commit(&outcomes, attempt.epoch, attempt.checkpoint_id).await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink_with_cursor(
            Arc::clone(&recorded),
            external_cursor(attempt.checkpoint_id, 2),
        );
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(outcomes));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match authoritative token 1"));
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn outcome_gc_anchor_is_cursor_continuity_only() {
        let backend = Arc::new(InProcessBackend::new(1));
        let anchor = CheckpointAttempt::new(1, 11);
        let aborted = CheckpointAttempt::new(2, 22);
        let live = CheckpointAttempt::new(3, 33);
        seal(&backend, anchor, &[(0, Some(b"e1"))]).await;
        seal(&backend, aborted, &[(0, Some(b"must-not-commit"))]).await;
        seal(&backend, live, &[(0, Some(b"e3"))]).await;

        let outcomes = decisions().await;
        record_local_commit(&outcomes, anchor.epoch, anchor.checkpoint_id).await;
        record_local_abort(&outcomes, aborted.epoch, aborted.checkpoint_id).await;
        record_local_commit(&outcomes, live.epoch, live.checkpoint_id).await;
        assert_eq!(outcomes.prune_outcomes_before(3).await.unwrap(), 3);
        let boundary = outcomes.outcome_retention_boundary().await.unwrap();
        assert_eq!(boundary.committed_checkpoint_id, Some(anchor.checkpoint_id));
        assert_eq!(boundary.highest_closed_epoch, Some(aborted.epoch));

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink_with_cursor(
            Arc::clone(&recorded),
            external_cursor(anchor.checkpoint_id, 1),
        );
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(outcomes));

        committer.commit_ready().await.unwrap();

        let batches = recorded.lock();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].expected_predecessor,
            CoordinatedCommitCursor {
                checkpoint_id: anchor.checkpoint_id,
                fencing_token: 1,
            }
        );
        assert_eq!(batches[0].target, live);
        assert_eq!(batches[0].entries.len(), 1);
        assert_eq!(batches[0].entries[0].attempt, live);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn terminal_compaction_preserves_every_lagging_commit_for_external_publication() {
        let backend = Arc::new(InProcessBackend::new(1));
        let fence = assignment_fence(3, &[7]);
        let decisions = cluster_decisions(&fence, 7).await;
        let commits = [1_u64, 3, 20, 40, 60, 80].map(|epoch| CheckpointAttempt::new(epoch, epoch));
        let anchor = commits[0];
        let live_commits = &commits[1..];

        for epoch in 1..=80 {
            let attempt = CheckpointAttempt::new(epoch, epoch);
            if commits.contains(&attempt) {
                seal_with_fence(
                    &backend,
                    attempt,
                    &[(7, Some(b"live"))],
                    &[7],
                    Some(&fence),
                    Some(&decisions.proof),
                )
                .await;
                record_cluster_commit(&decisions, &backend, epoch, epoch, &fence).await;
            } else {
                decisions
                    .authority
                    .record_cluster_outcome(
                        &decisions.proof,
                        epoch,
                        epoch,
                        fence.clone(),
                        CheckpointVerdict::Abort,
                        None,
                    )
                    .await
                    .unwrap();
            }
            if epoch == 3 {
                assert_eq!(
                    decisions
                        .authority
                        .prune_cluster_outcomes_before(&decisions.proof, 3, |_| async { Ok(()) })
                        .await
                        .unwrap(),
                    3
                );
            }
        }

        let boundary = decisions
            .authority
            .cluster_outcome_retention_boundary()
            .await
            .unwrap();
        assert_eq!(boundary.artifact_before_epoch, 3);
        assert!(
            boundary.terminal_before_epoch > boundary.artifact_before_epoch,
            "automatic terminal compaction must not advance artifact retention"
        );
        assert!(decisions
            .authority
            .cluster_outcome(4)
            .await
            .unwrap()
            .is_none());
        let retained_commits = decisions
            .authority
            .cluster_outcomes()
            .await
            .unwrap()
            .into_iter()
            .filter(CheckpointOutcome::is_commit)
            .map(|outcome| CheckpointAttempt::new(outcome.epoch, outcome.checkpoint_id))
            .collect::<Vec<_>>();
        assert_eq!(retained_commits.as_slice(), live_commits);

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink_with_cursor(
            Arc::clone(&recorded),
            external_cursor(anchor.checkpoint_id, decisions.proof.fencing_token),
        );
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        committer.commit_ready().await.unwrap();

        let batches = recorded.lock();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].expected_predecessor,
            CoordinatedCommitCursor {
                checkpoint_id: anchor.checkpoint_id,
                fencing_token: decisions.proof.fencing_token,
            }
        );
        assert_eq!(batches[0].target, *live_commits.last().unwrap());
        let submitted_commits = batches[0]
            .entries
            .iter()
            .map(|entry| entry.attempt)
            .collect::<Vec<_>>();
        assert_eq!(submitted_commits.as_slice(), live_commits);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn compacted_cluster_cursor_must_match_anchor_authority() {
        let backend = Arc::new(InProcessBackend::new(1));
        let anchor = CheckpointAttempt::new(1, 11);
        let live = CheckpointAttempt::new(3, 33);
        let fence = assignment_fence(3, &[7]);
        let mut decisions = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            anchor,
            &[(7, Some(b"payload"))],
            &[7],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;
        record_cluster_commit(
            &decisions,
            &backend,
            anchor.epoch,
            anchor.checkpoint_id,
            &fence,
        )
        .await;
        let next_owner = laminar_core::cluster::control::LeaderLeaseOwner {
            node: decisions.owner.node,
            boot: decisions.owner.boot,
            process_term: decisions.owner.process_term + 1,
        };
        advance_cluster_term(&mut decisions, next_owner).await;
        seal_with_fence(
            &backend,
            live,
            &[(7, Some(b"payload"))],
            &[7],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;
        record_cluster_commit(&decisions, &backend, live.epoch, live.checkpoint_id, &fence).await;
        assert_eq!(
            decisions
                .authority
                .prune_cluster_outcomes_before(&decisions.proof, live.epoch, |_| async { Ok(()) })
                .await
                .unwrap(),
            live.epoch
        );

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink_with_cursor(
            Arc::clone(&recorded),
            external_cursor(anchor.checkpoint_id, 2),
        );
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("compacted external cursor checkpoint 11 fencing token 2"));
        assert!(error.to_string().contains("authoritative token 1"));
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn tampered_marker_checksum_fails_without_external_commit() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(4, 44);
        let namespace = namespace();
        let key = descriptor_key(&namespace, 0);
        let mut marker = encode_prepared_marker(&namespace, attempt, 0, Some(b"original")).unwrap();
        *marker.last_mut().unwrap() = b'!';
        backend
            .write_commit_descriptor(attempt, &key, Bytes::from(marker))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, None, &[], &[key])
            .await
            .unwrap());

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions().await;
        record_local_commit(&decisions, 4, 44).await;
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error.to_string().contains("checksum mismatch"));
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn local_outcome_rejects_a_cluster_assignment_seal() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(4, 45);
        let fence = assignment_fence(4, &[1]);
        let proof = leader_proof(&fence, 1, 1, 1);
        seal_with_fence(
            &backend,
            attempt,
            &[(1, Some(b"payload"))],
            &[],
            Some(&fence),
            Some(&proof),
        )
        .await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions().await;
        record_local_commit(&decisions, 4, 45).await;
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error.to_string().contains("cluster assignment certificate"));
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn caught_up_history_still_rejects_a_different_deployment() {
        let backend = Arc::new(InProcessBackend::new(1));
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle =
            spawn_recording_sink_with_cursor(Arc::clone(&recorded), external_cursor(44, 1));
        let decisions = decisions().await;
        record_local_commit(&decisions, 4, 44).await;
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            "018f0000-0000-7000-8000-000000000099".into(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match committer deployment"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_committer_ignores_forged_standalone_outcome_key() {
        let fence = assignment_fence(3, &[7]);
        let decisions = cluster_decisions(&fence, 7).await;
        let malformed = serde_json::json!({
            "version": 2,
            "scope": "cluster",
            "epoch": 4,
            "checkpoint_id": 44,
            "deployment_id": deployment_id(),
            "assignment_fence": null,
            "leader_proof": {
                "owner": {
                    "node_id": 7,
                    "boot_id": "00000000-0000-0000-0000-000000000007",
                    "process_term": 1
                },
                "fencing_token": 1
            },
            "recovery_capsule": null,
            "verdict": "commit"
        });
        decisions
            .backing
            .put(
                &object_store::path::Path::from("checkpoint-outcomes/epoch=4/outcome"),
                PutPayload::from_bytes(Bytes::from(serde_json::to_vec(&malformed).unwrap())),
            )
            .await
            .unwrap();

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let mut committer = CoordinatedCommitter::new(
            Arc::new(InProcessBackend::new(1)) as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        committer.commit_ready().await.unwrap();
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_commit_rejects_marker_written_by_another_certified_participant() {
        let backend = Arc::new(InProcessBackend::new(2));
        let attempt = CheckpointAttempt::new(5, 54);
        let fence = assignment_fence(4, &[7, 9]);
        let decisions = cluster_decisions(&fence, 7).await;
        backend.set_authoritative_version(fence.assignment_version);
        backend
            .write_certified_partial(
                attempt,
                0,
                &fence,
                7,
                Bytes::from_static(b"test-vnode-state"),
            )
            .await
            .unwrap();
        backend
            .write_certified_partial(
                attempt,
                1,
                &fence,
                9,
                Bytes::from_static(b"test-vnode-state"),
            )
            .await
            .unwrap();

        let namespace = namespace();
        let marker_key = descriptor_key(&namespace, 7);
        let marker = encode_prepared_marker(&namespace, attempt, 7, Some(b"payload")).unwrap();
        backend
            .write_certified_commit_descriptor(
                attempt,
                &marker_key,
                &fence,
                9,
                &decisions.proof,
                Bytes::from(marker),
            )
            .await
            .unwrap();
        let mut keys = vec![marker_key];
        for participant_id in [7, 9] {
            let ready_key = participant_ready_key(participant_id);
            let ready = ParticipantReady {
                version: PARTICIPANT_READY_VERSION,
                attempt,
                participant_id,
                assignment_fence: fence.clone(),
                deployment_id: deployment_id(),
                pipeline_identity: identity(),
                owned_vnodes: match participant_id {
                    7 => vec![0],
                    9 => vec![1],
                    _ => unreachable!("test readiness participant belongs to the assignment"),
                },
                source_offsets: Default::default(),
                source_metadata: Default::default(),
                source_assignment_versions: Default::default(),
                source_watermarks: Default::default(),
                local_watermark: CheckpointWatermark::Uninitialized,
                manifest_sha256: format!("{participant_id:064x}"),
                portable_state_sha256: identity().sha256,
            };
            backend
                .write_certified_commit_descriptor(
                    attempt,
                    &ready_key,
                    &fence,
                    participant_id,
                    &decisions.proof,
                    Bytes::from(canonical_json_bytes(&ready).unwrap()),
                )
                .await
                .unwrap();
            keys.push(ready_key);
        }
        keys.sort_unstable();
        assert!(backend
            .seal_checkpoint(attempt, Some(&fence), &[0, 1], &keys)
            .await
            .unwrap());
        record_cluster_commit(
            &decisions,
            &backend,
            attempt.epoch,
            attempt.checkpoint_id,
            &fence,
        )
        .await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(
                TEST_SINK_ID.into(),
                spawn_recording_sink(Arc::clone(&recorded)),
            )],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("was not written by participant 7"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn external_commit_rejects_participants_outside_the_outcome() {
        let backend = Arc::new(InProcessBackend::new(2));
        let attempt = CheckpointAttempt::new(5, 55);
        let fence = assignment_fence(4, &[7, 9]);
        let decisions = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload"))],
            &[7, 9],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        record_cluster_commit(&decisions, &backend, 5, 55, &fence).await;
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("do not match outcome participants"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn external_commit_rejects_deleted_sealed_readiness() {
        let raw = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = raw.clone();
        let backend = Arc::new(ObjectStoreBackend::cluster_shared(store, "node-7", 2));
        let attempt = CheckpointAttempt::new(5, 56);
        let fence = assignment_fence(4, &[7, 9]);
        let decisions = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload")), (9, None)],
            &[7, 9],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        record_cluster_commit(&decisions, &backend, 5, 56, &fence).await;
        let deleted_key = participant_ready_key(9);
        raw.delete(&descriptor_object_path(attempt, &deleted_key))
            .await
            .unwrap();
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("sealed participant readiness marker"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn external_commit_rejects_mutated_sealed_readiness() {
        let raw = Arc::new(InMemory::new());
        let store: Arc<dyn ObjectStore> = raw.clone();
        let backend = Arc::new(ObjectStoreBackend::cluster_shared(store, "node-7", 2));
        let attempt = CheckpointAttempt::new(5, 57);
        let fence = assignment_fence(4, &[7, 9]);
        let decisions = cluster_decisions(&fence, 7).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload")), (9, None)],
            &[7, 9],
            Some(&fence),
            Some(&decisions.proof),
        )
        .await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        record_cluster_commit(&decisions, &backend, 5, 57, &fence).await;

        let mutated_key = participant_ready_key(9);
        let mut mutated = serde_json::from_slice::<ParticipantReady>(
            &backend
                .read_commit_descriptor(attempt, &mutated_key)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        mutated.manifest_sha256 = "ff".repeat(32);
        raw.delete(&descriptor_object_path(attempt, &mutated_key))
            .await
            .unwrap();
        backend
            .write_certified_commit_descriptor(
                attempt,
                &mutated_key,
                &fence,
                9,
                &decisions.proof,
                Bytes::from(canonical_json_bytes(&mutated).unwrap()),
            )
            .await
            .unwrap();

        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(
                TEST_SINK_ID.into(),
                spawn_recording_sink(Arc::clone(&recorded)),
            )],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("attestation does not match the checkpoint seal"));
        assert!(recorded.lock().is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn external_commit_rejects_a_different_outcome_assignment() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(6, 66);
        let sealed_fence = assignment_fence(4, &[7]);
        let decision_fence = assignment_fence(5, &[7]);
        let decisions = cluster_decisions(&decision_fence, 7).await;
        seal_with_fence(
            &backend,
            attempt,
            &[(7, Some(b"payload"))],
            &[7],
            Some(&sealed_fence),
            Some(&decisions.proof),
        )
        .await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        record_cluster_commit(&decisions, &backend, 6, 66, &decision_fence).await;
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_cluster_controller(Some(Arc::clone(&decisions.controller)))
        .with_decision_store(Some(Arc::clone(&decisions.capsules)));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error
            .to_string()
            .contains("assignment certificate does not match"));
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn durable_commit_outcome_without_exact_seal_fails_closed() {
        let backend = Arc::new(InProcessBackend::new(1));
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions().await;
        record_local_commit(&decisions, 4, 44).await;
        let lag = Arc::new(AtomicU64::new(7));
        let lag_known = Arc::new(AtomicBool::new(true));
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_lag_state(
            Arc::clone(&lag),
            Arc::clone(&lag_known),
            Arc::new(tokio::sync::Notify::new()),
        )
        .with_decision_store(Some(decisions));

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error.to_string().contains("has no exact state seal"));
        assert!(!lag_known.load(Ordering::Acquire));
        assert_eq!(lag.load(Ordering::Acquire), 7);
        assert!(recorded.lock().is_empty());
    }

    #[tokio::test]
    async fn live_external_cursor_rollback_fails_closed() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(1, 11);
        seal(&backend, attempt, &[(0, Some(b"e1"))]).await;
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let cursor: ExternalCursor = Arc::new(Mutex::new(None));
        let handle = spawn_recording_sink_with_cursor(Arc::clone(&recorded), Arc::clone(&cursor));
        let decisions = decisions().await;
        record_local_commit(&decisions, 1, 11).await;
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![(TEST_SINK_ID.into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        committer.commit_ready().await.unwrap();
        assert_eq!(
            *cursor.lock(),
            Some(CoordinatedCommitCursor {
                checkpoint_id: attempt.checkpoint_id,
                fencing_token: 1,
            })
        );
        *cursor.lock() = None;

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error.to_string().contains("rolled back from 11 to 0"));
        assert_eq!(recorded.lock().len(), 1);
    }
}
