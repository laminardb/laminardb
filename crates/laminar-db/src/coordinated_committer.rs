//! Decoupled designated committer: off the barrier path, the leader reads each
//! writer's descriptor for sealed epochs and runs one external commit per sink.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use laminar_connectors::connector::{
    CoordinatedCommitBatch, CoordinatedCommitNamespace, CoordinatedCommitPayload,
};
use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use laminar_core::state::{CheckpointAttempt, StateBackend};
use laminar_core::storage::checkpoint_manifest::PipelineIdentity;

use crate::error::DbError;
use crate::sink_task::SinkTaskHandle;

const PREPARED_MARKER_VERSION: u32 = 2;
// Committables are metadata, not a bulk-data transport. A fixed private bound
// keeps connector bugs off the checkpoint control plane.
const MAX_PREPARED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_PREPARED_HEADER_BYTES: usize = 64 * 1024;
const MAX_COMMIT_BATCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_COMMIT_BATCH_ENTRIES: usize = 4_096;
const MAX_DESCRIPTOR_READ_CONCURRENCY: usize = 4;
const PREPARED_MARKER_MAGIC: &[u8; 8] = b"LDBCM2\0\0";

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
    if payload.is_some_and(|bytes| bytes.len() > MAX_PREPARED_PAYLOAD_BYTES) {
        return Err(DbError::Checkpoint(format!(
            "coordinated sink '{}' descriptor exceeds {} bytes",
            namespace.sink_id, MAX_PREPARED_PAYLOAD_BYTES
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
    let max_encoded = PREPARED_MARKER_MAGIC
        .len()
        .saturating_add(4)
        .saturating_add(MAX_PREPARED_HEADER_BYTES)
        .saturating_add(MAX_PREPARED_PAYLOAD_BYTES);
    if bytes.len() > max_encoded {
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
    if marker.payload_len > MAX_PREPARED_PAYLOAD_BYTES as u64
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
    /// External cursor is the globally unique checkpoint id, not a reusable epoch.
    committed_through: FxHashMap<String, u64>,
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

        let (all_decided, observed_cursors) = match self.load_commit_inventory().await {
            Ok(inventory) => inventory,
            Err(error) => {
                self.mark_lag_unknown();
                return Err(error);
            }
        };
        self.committed_through = observed_cursors;
        self.seeded = true;
        let high_epoch = all_decided.last().map(|attempt| attempt.epoch);

        let mut first_err: Option<DbError> = None;
        for (name, handle) in &self.sinks {
            let cursor = self.committed_through.get(name).copied().unwrap_or(0);
            let sealed: Vec<CheckpointAttempt> = all_decided
                .iter()
                .copied()
                .filter(|attempt| attempt.checkpoint_id > cursor)
                .collect();
            let Some(&target) = sealed.last() else {
                continue;
            };
            match self
                .commit_sealed(handle, name, cursor, &sealed, target)
                .await
            {
                Ok(()) => {
                    self.committed_through
                        .insert(name.clone(), target.checkpoint_id);
                }
                Err(e) => {
                    first_err.get_or_insert(e); // leave the cursor; retry next pass
                }
            }
        }

        // Publish the prune floor (lowest uncommitted epoch) + lag metric, and
        // warn if the committer is falling behind so storage isn't growing blind.
        let min_committed = self.min_committed();
        let floor_epoch = all_decided
            .iter()
            .find(|attempt| attempt.checkpoint_id > min_committed)
            .map_or_else(
                || high_epoch.map(|epoch| epoch.saturating_add(1)),
                |attempt| Some(attempt.epoch),
            );
        if let Some(floor_epoch) = floor_epoch {
            self.floor.store(floor_epoch, Ordering::Release);
        }
        let lag = all_decided
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

    async fn load_commit_inventory(
        &self,
    ) -> Result<(Vec<CheckpointAttempt>, FxHashMap<String, u64>), DbError> {
        let decision_store = self.decision_store.as_ref().ok_or_else(|| {
            DbError::Checkpoint(
                "committer: coordinated commit requires a checkpoint decision store".into(),
            )
        })?;
        let decisions = self
            .storage_io(
                "decision inventory read",
                decision_store.committed_decisions(),
            )
            .await?;
        let all_decided: Vec<CheckpointAttempt> = decisions
            .iter()
            .map(|decision| CheckpointAttempt::new(decision.epoch, decision.checkpoint_id))
            .collect();
        let observed_cursors = self.read_external_cursors().await?;
        let sealed: FxHashSet<CheckpointAttempt> = self
            .storage_io(
                "list sealed checkpoints",
                self.backend.sealed_checkpoints(0),
            )
            .await?
            .into_iter()
            .collect();
        self.validate_commit_continuity(&all_decided, &observed_cursors, &sealed)?;
        Ok((all_decided, observed_cursors))
    }

    /// Re-read every external cursor on every pass. Besides recovering an ambiguous prior commit,
    /// this detects a live target-catalog rollback instead of trusting stale memory.
    async fn read_external_cursors(&self) -> Result<FxHashMap<String, u64>, DbError> {
        let mut observed_cursors = FxHashMap::default();
        for (name, handle) in &self.sinks {
            let namespace = CoordinatedCommitNamespace::try_new(
                self.pipeline_identity.clone(),
                self.deployment_id.clone(),
                name.clone(),
            )
            .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            let observed = handle
                .committed_checkpoint_id(namespace)
                .await
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "committer: external cursor read failed for sink '{name}': {error}"
                    ))
                })?
                .unwrap_or(0);
            let prior = self.committed_through.get(name).copied().unwrap_or(0);
            if self.seeded && observed < prior {
                return Err(DbError::Checkpoint(format!(
                    "committer: external cursor for sink '{name}' rolled back from {prior} to \
                     {observed}"
                )));
            }
            observed_cursors.insert(name.clone(), observed);
        }
        Ok(observed_cursors)
    }

    fn validate_commit_continuity(
        &self,
        all_decided: &[CheckpointAttempt],
        observed_cursors: &FxHashMap<String, u64>,
        sealed: &FxHashSet<CheckpointAttempt>,
    ) -> Result<(), DbError> {
        let min_observed = self
            .sinks
            .iter()
            .map(|(name, _)| observed_cursors.get(name).copied().unwrap_or(0))
            .min()
            .unwrap_or(0);
        // Every decision still ahead of at least one sink must retain its exact seal and
        // participant inventory. Older decisions may be the single GC continuity anchor.
        for attempt in all_decided
            .iter()
            .filter(|attempt| attempt.checkpoint_id > min_observed)
        {
            if !sealed.contains(attempt) {
                return Err(DbError::Checkpoint(format!(
                    "committer: durable decision for epoch {} checkpoint {} has no exact state \
                     seal; external publication cannot skip the missing cut",
                    attempt.epoch, attempt.checkpoint_id
                )));
            }
        }

        let lowest = all_decided.first().map(|attempt| attempt.checkpoint_id);
        let highest = all_decided.last().map(|attempt| attempt.checkpoint_id);
        for (name, cursor) in observed_cursors {
            let valid = if *cursor == 0 {
                // A fresh target is valid only when the first retained decision still has its
                // seal. If it is the GC anchor, zero proves catalog rollback.
                all_decided
                    .first()
                    .is_none_or(|attempt| sealed.contains(attempt))
            } else {
                all_decided
                    .binary_search_by_key(cursor, |attempt| attempt.checkpoint_id)
                    .is_ok()
            };
            if !valid || highest.is_some_and(|highest| *cursor > highest) {
                return Err(DbError::Checkpoint(format!(
                    "committer: external cursor {cursor} for sink '{name}' is incompatible with \
                     retained decision continuity {lowest:?}..={highest:?}"
                )));
            }
        }
        Ok(())
    }

    /// Lowest committed checkpoint id across sinks (0 if any sink has committed nothing).
    fn min_committed(&self) -> u64 {
        self.sinks
            .iter()
            .map(|(name, _)| self.committed_through.get(name).copied().unwrap_or(0))
            .min()
            .unwrap_or(0)
    }

    /// Commit the `sealed` epochs' descriptors for one sink in transactions
    /// bounded at checkpoint boundaries. `target` is the highest attempt.
    async fn commit_sealed(
        &self,
        handle: &SinkTaskHandle,
        name: &str,
        predecessor: u64,
        sealed: &[CheckpointAttempt],
        target: CheckpointAttempt,
    ) -> Result<(), DbError> {
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
        let mut batch_predecessor = predecessor;
        for &attempt in sealed {
            let (mut attempt_entries, attempt_descriptor_bytes) = self
                .load_attempt_entries(name, &namespace, &prefix, attempt)
                .await?;

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
                && (combined_bytes > MAX_COMMIT_BATCH_BYTES
                    || combined_entries > MAX_COMMIT_BATCH_ENTRIES)
            {
                let flushed_target = batch_target.expect("non-empty commit batch has a target");
                self.submit_batch(
                    handle,
                    namespace.clone(),
                    batch_predecessor,
                    flushed_target,
                    std::mem::take(&mut entries),
                )
                .await?;
                batch_predecessor = flushed_target.checkpoint_id;
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
        }

        self.submit_batch(
            handle,
            namespace,
            batch_predecessor,
            batch_target.ok_or_else(|| {
                DbError::Checkpoint("committer: empty sealed checkpoint batch".into())
            })?,
            entries,
        )
        .await
    }

    async fn load_attempt_entries(
        &self,
        name: &str,
        namespace: &CoordinatedCommitNamespace,
        prefix: &str,
        attempt: CheckpointAttempt,
    ) -> Result<(Vec<CoordinatedCommitPayload>, usize), DbError> {
        let inventory = self
            .storage_io(
                &format!(
                    "read seal inventory for checkpoint {}",
                    attempt.checkpoint_id
                ),
                self.backend.checkpoint_seal_inventory(attempt),
            )
            .await?
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "committer: checkpoint {} was listed sealed but its seal disappeared",
                    attempt.checkpoint_id
                ))
            })?;
        if inventory.attempt != attempt {
            return Err(DbError::Checkpoint(format!(
                "committer: seal inventory identity mismatch for checkpoint {}",
                attempt.checkpoint_id
            )));
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
        if required.len() > MAX_COMMIT_BATCH_ENTRIES {
            return Err(DbError::Checkpoint(format!(
                "committer: checkpoint {} has {} markers for sink '{name}', exceeding the \
                 per-transaction limit of {MAX_COMMIT_BATCH_ENTRIES}",
                attempt.checkpoint_id,
                required.len()
            )));
        }

        let descriptors = futures::stream::iter(required.into_iter().map(|key| async move {
            let bytes = self
                .storage_io(
                    &format!(
                        "read descriptor '{key}' for epoch {} checkpoint {}",
                        attempt.epoch, attempt.checkpoint_id
                    ),
                    self.backend.read_commit_descriptor(attempt, &key),
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
            if descriptor_bytes > MAX_COMMIT_BATCH_BYTES {
                return Err(DbError::Checkpoint(format!(
                    "committer: checkpoint {} descriptors for sink '{name}' exceed {} bytes",
                    attempt.checkpoint_id, MAX_COMMIT_BATCH_BYTES
                )));
            }
            entries.push(decode_prepared_marker(&key, &bytes, attempt, namespace)?);
        }
        Ok((entries, descriptor_bytes))
    }

    async fn submit_batch(
        &self,
        handle: &SinkTaskHandle,
        namespace: CoordinatedCommitNamespace,
        expected_predecessor: u64,
        target: CheckpointAttempt,
        mut entries: Vec<CoordinatedCommitPayload>,
    ) -> Result<(), DbError> {
        entries.sort_unstable_by_key(|entry| (entry.attempt.checkpoint_id, entry.participant_id));
        handle
            .commit_aggregated(CoordinatedCommitBatch {
                namespace,
                expected_predecessor,
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
    use laminar_core::state::{InProcessBackend, StateBackend};
    use object_store::memory::InMemory;
    use parking_lot::Mutex;

    use crate::sink_task::{SinkTaskConfig, SinkTaskHandle, DEFAULT_CHANNEL_CAPACITY};

    type Recorded = Arc<Mutex<Vec<CoordinatedCommitBatch>>>;

    struct RecordingSink {
        schema: SchemaRef,
        recorded: Recorded,
        committed: Arc<AtomicU64>,
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
        ) -> Result<(), ConnectorError> {
            self.committed
                .fetch_max(batch.target.checkpoint_id, Ordering::Release);
            self.recorded.lock().push(batch);
            Ok(())
        }

        async fn committed_checkpoint_id(
            &self,
            _namespace: &CoordinatedCommitNamespace,
        ) -> Result<Option<u64>, ConnectorError> {
            let c = self.committed.load(Ordering::Acquire);
            Ok((c > 0).then_some(c))
        }
    }

    fn spawn_recording_sink_with_cursor(
        recorded: Recorded,
        committed: Arc<AtomicU64>,
    ) -> SinkTaskHandle {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let (event_tx, _rx) = laminar_core::streaming::channel::channel(
            crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
        );
        SinkTaskHandle::spawn(SinkTaskConfig {
            name: "ice".into(),
            sink_id: Arc::from("ice"),
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
        })
    }

    fn spawn_recording_sink(recorded: Recorded) -> SinkTaskHandle {
        spawn_recording_sink_with_cursor(recorded, Arc::new(AtomicU64::new(0)))
    }

    fn identity() -> PipelineIdentity {
        PipelineIdentity::empty()
    }

    fn deployment_id() -> String {
        "018f0000-0000-7000-8000-000000000001".into()
    }

    fn namespace() -> CoordinatedCommitNamespace {
        CoordinatedCommitNamespace::try_new(identity(), deployment_id(), "ice").unwrap()
    }

    async fn seal(
        backend: &InProcessBackend,
        attempt: CheckpointAttempt,
        markers: &[(u64, Option<&[u8]>)],
    ) {
        let namespace = namespace();
        let mut keys = Vec::new();
        for &(participant_id, payload) in markers {
            let key = descriptor_key(&namespace, participant_id);
            let marker =
                encode_prepared_marker(&namespace, attempt, participant_id, payload).unwrap();
            backend
                .write_commit_descriptor(attempt, &key, 0, Bytes::from(marker))
                .await
                .unwrap();
            keys.push(key);
        }
        keys.sort_unstable();
        assert!(backend
            .seal_checkpoint(attempt, 0, &[], &keys)
            .await
            .unwrap());
    }

    fn decisions() -> Arc<laminar_core::checkpoint_decision::CheckpointDecisionStore> {
        Arc::new(
            laminar_core::checkpoint_decision::CheckpointDecisionStore::new(Arc::new(
                InMemory::new(),
            )),
        )
    }

    #[tokio::test]
    async fn batches_sealed_epochs_into_one_commit() {
        let backend = Arc::new(InProcessBackend::new(2));
        let first = CheckpointAttempt::new(1, 11);
        let second = CheckpointAttempt::new(2, 22);
        seal(&backend, first, &[(7, Some(b"e1")), (9, None)]).await;
        seal(&backend, second, &[(7, Some(b"e2")), (9, None)]).await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions();
        decisions.record_committed(1, 11).await.unwrap();
        decisions.record_committed(2, 22).await.unwrap();
        let floor = Arc::new(AtomicU64::new(0));
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
            identity(),
            deployment_id(),
            Arc::clone(&floor),
        )
        .with_decision_store(Some(decisions));

        committer.commit_ready().await.unwrap();

        let batches = recorded.lock().clone();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].namespace, namespace());
        assert_eq!(batches[0].expected_predecessor, 0);
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

    #[tokio::test]
    async fn skips_abandoned_epoch_with_partial_descriptor() {
        let backend = Arc::new(InProcessBackend::new(2));
        let first = CheckpointAttempt::new(1, 11);
        let abandoned = CheckpointAttempt::new(2, 19);
        let third = CheckpointAttempt::new(3, 31);
        seal(&backend, first, &[(0, Some(b"e1"))]).await;
        // Epoch 2 wrote a descriptor but was abandoned (never sealed).
        let namespace = namespace();
        let orphan_key = descriptor_key(&namespace, 0);
        let orphan = encode_prepared_marker(&namespace, abandoned, 0, Some(b"orphan")).unwrap();
        backend
            .write_commit_descriptor(abandoned, &orphan_key, 0, Bytes::from(orphan))
            .await
            .unwrap();
        seal(&backend, third, &[(0, Some(b"e3"))]).await;

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions();
        decisions.record_committed(1, 11).await.unwrap();
        decisions.record_committed(3, 31).await.unwrap();
        let mut committer = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        committer.commit_ready().await.unwrap();

        // Epochs 1 and 3 batch into one commit keyed by 3; epoch 2's orphan
        // descriptor (never sealed) must NOT be committed.
        let batches = recorded.lock().clone();
        assert_eq!(batches.len(), 1);
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
        let decisions = decisions();
        decisions.record_committed(1, 11).await.unwrap();
        decisions.record_committed(2, 22).await.unwrap();

        let mut first = CoordinatedCommitter::new(
            Arc::clone(&backend) as Arc<dyn StateBackend>,
            vec![("ice".into(), handle.clone())],
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
            vec![("ice".into(), handle)],
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
    async fn tampered_marker_checksum_fails_without_external_commit() {
        let backend = Arc::new(InProcessBackend::new(1));
        let attempt = CheckpointAttempt::new(4, 44);
        let namespace = namespace();
        let key = descriptor_key(&namespace, 0);
        let mut marker = encode_prepared_marker(&namespace, attempt, 0, Some(b"original")).unwrap();
        *marker.last_mut().unwrap() = b'!';
        backend
            .write_commit_descriptor(attempt, &key, 0, Bytes::from(marker))
            .await
            .unwrap();
        assert!(backend
            .seal_checkpoint(attempt, 0, &[], &[key])
            .await
            .unwrap());

        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions();
        decisions.record_committed(4, 44).await.unwrap();
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
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
    async fn durable_decision_without_exact_seal_fails_closed() {
        let backend = Arc::new(InProcessBackend::new(1));
        let recorded: Recorded = Arc::new(Mutex::new(Vec::new()));
        let handle = spawn_recording_sink(Arc::clone(&recorded));
        let decisions = decisions();
        decisions.record_committed(4, 44).await.unwrap();
        let lag = Arc::new(AtomicU64::new(7));
        let lag_known = Arc::new(AtomicBool::new(true));
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
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
        let cursor = Arc::new(AtomicU64::new(0));
        let handle = spawn_recording_sink_with_cursor(Arc::clone(&recorded), Arc::clone(&cursor));
        let decisions = decisions();
        decisions.record_committed(1, 11).await.unwrap();
        let mut committer = CoordinatedCommitter::new(
            backend as Arc<dyn StateBackend>,
            vec![("ice".into(), handle)],
            identity(),
            deployment_id(),
            Arc::new(AtomicU64::new(0)),
        )
        .with_decision_store(Some(decisions));

        committer.commit_ready().await.unwrap();
        assert_eq!(cursor.load(Ordering::Acquire), attempt.checkpoint_id);
        cursor.store(0, Ordering::Release);

        let error = committer.commit_ready().await.unwrap_err();
        assert!(error.to_string().contains("rolled back from 11 to 0"));
        assert_eq!(recorded.lock().len(), 1);
    }
}
