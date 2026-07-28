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
            let inventory = self
                .storage_io(
                    "cluster checkpoint outcome inventory read",
                    authority.cluster_outcome_inventory(),
                )
                .await?;
            let boundary = inventory.retention_boundary;
            let committed_checkpoint_id = boundary
                .committed_anchor
                .as_ref()
                .map(|outcome| outcome.checkpoint_id);
            (
                inventory.outcomes,
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
                capsule.vnode_restore_contract.clone(),
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
mod tests;
