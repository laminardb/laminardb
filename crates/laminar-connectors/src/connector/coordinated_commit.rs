//! Bounded, fenced coordinated-commit batches and cursor validation.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::error::ConnectorError;

/// Fixed control-plane bound for one connector's coordinated-commit payload.
///
/// Connectors must keep prepared metadata at or below this limit before
/// returning it to the checkpoint runtime. Bulk records belong in the sink's
/// data plane, referenced by the bounded payload.
pub const MAX_COORDINATED_COMMIT_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Fixed aggregate control-plane bound for one designated commit call.
pub const MAX_COORDINATED_COMMIT_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// Fixed participant-marker bound for one designated commit call.
pub const MAX_COORDINATED_COMMIT_BATCH_ENTRIES: usize = 4_096;

/// Stable external commit namespace for one deployment incarnation of a logical pipeline sink.
///
/// The configured external target already scopes its metadata. The create-once deployment id
/// prevents checkpoint-store resets or two
/// identically configured deployments from sharing a cursor. Pipeline identity plus sink id then
/// binds that deployment to one recovery-compatible logical writer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCommitNamespace {
    /// Canonical logical-pipeline identity used by checkpoint recovery.
    pub pipeline_identity: laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity,
    /// Create-once UUID stored with checkpoint decisions and shared by every cluster member.
    pub deployment_id: String,
    /// Stable sink registration id within the pipeline.
    pub sink_id: String,
}

impl CoordinatedCommitNamespace {
    /// Construct and validate a namespace before any external metadata lookup.
    ///
    /// # Errors
    /// Returns a configuration error for a malformed pipeline digest or empty
    /// sink id.
    pub fn try_new(
        pipeline_identity: laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity,
        deployment_id: impl Into<String>,
        sink_id: impl Into<String>,
    ) -> Result<Self, ConnectorError> {
        let deployment_id = deployment_id.into();
        let sink_id = sink_id.into();
        if pipeline_identity.sha256.len() != 64
            || !pipeline_identity
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit requires a canonical lowercase SHA-256 pipeline identity"
                    .into(),
            ));
        }
        if sink_id.is_empty() {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit sink id cannot be empty".into(),
            ));
        }
        let parsed_deployment = uuid::Uuid::parse_str(&deployment_id).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "coordinated commit deployment id is not a UUID: {error}"
            ))
        })?;
        if parsed_deployment.is_nil() || parsed_deployment.to_string() != deployment_id {
            return Err(ConnectorError::ConfigurationError(
                "coordinated commit deployment id must be a canonical non-nil UUID".into(),
            ));
        }
        Ok(Self {
            pipeline_identity,
            deployment_id,
            sink_id,
        })
    }

    /// Bounded, filesystem/catalog-safe key for external transaction metadata.
    #[must_use]
    pub fn external_key(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.pipeline_identity.canonical_version.to_be_bytes());
        digest.update(self.pipeline_identity.sha256.as_bytes());
        digest.update([0]);
        digest.update(self.deployment_id.as_bytes());
        digest.update([0]);
        digest.update(self.sink_id.as_bytes());
        let digest = digest.finalize();
        format!("ldb-c3-{digest:x}")
    }
}

/// Exact external commit position and the authority that published it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedCommitCursor {
    /// Highest globally unique checkpoint id atomically reflected by the sink.
    pub checkpoint_id: u64,
    /// Monotonic authority token that fenced earlier designated committers.
    pub fencing_token: u64,
}

/// One participant's validated prepared marker for one exact attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCommitPayload {
    /// Exact checkpoint attempt that admitted this marker.
    pub attempt: laminar_core::checkpoint::CheckpointAttempt,
    /// Stable nonzero runtime participant ID.
    pub participant_id: u64,
    /// Connector-specific committable, or `None` for an explicitly empty cut.
    pub payload: Option<Vec<u8>>,
}

/// Exact batch submitted to a designated external-sink committer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatedCommitBatch {
    /// External cursor namespace.
    pub namespace: CoordinatedCommitNamespace,
    /// Exact external cursor that must precede this batch. The zero cursor names
    /// an empty target. A different authority at the predecessor checkpoint is
    /// a conflicting history and must fail closed.
    pub expected_predecessor: CoordinatedCommitCursor,
    /// Non-zero authority token that the external commit must persist atomically.
    pub fencing_token: u64,
    /// Highest exact attempt atomically covered by this commit.
    pub target: laminar_core::checkpoint::CheckpointAttempt,
    /// Every prepared participant marker through `target`, including empty ones.
    pub entries: Vec<CoordinatedCommitPayload>,
}

/// Runtime-owned deadline for one designated external publication.
///
/// The deadline is created before the command enters the sink actor, so a
/// connector sees the actual budget left after queueing rather than a second,
/// connector-local timeout window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CoordinatedCommitContext {
    deadline: tokio::time::Instant,
}

impl CoordinatedCommitContext {
    /// Create a context from the sink actor's absolute end-to-end deadline.
    #[must_use]
    pub const fn new(deadline: tokio::time::Instant) -> Self {
        Self { deadline }
    }

    /// Absolute monotonic publication deadline.
    #[must_use]
    pub const fn deadline(self) -> tokio::time::Instant {
        self.deadline
    }

    /// Budget still available at the point the connector starts publication.
    #[must_use]
    pub fn remaining(self) -> std::time::Duration {
        self.deadline
            .saturating_duration_since(tokio::time::Instant::now())
    }
}

impl CoordinatedCommitBatch {
    /// Collision-resistant identity for one exact ordered publication cut.
    /// Every variable-length field is length framed so distinct batches cannot
    /// share an input byte stream before hashing.
    #[must_use]
    pub fn exact_fingerprint(&self) -> [u8; 32] {
        fn update_length(hasher: &mut Sha256, length: usize) {
            let source = length.to_be_bytes();
            let mut encoded = [0_u8; 16];
            let start = encoded.len() - source.len();
            encoded[start..].copy_from_slice(&source);
            hasher.update(encoded);
        }

        fn update_framed(hasher: &mut Sha256, bytes: &[u8]) {
            update_length(hasher, bytes.len());
            hasher.update(bytes);
        }

        let mut hasher = Sha256::new();
        update_framed(&mut hasher, b"laminardb/coordinated-commit-batch/v1");
        update_framed(&mut hasher, self.namespace.external_key().as_bytes());
        hasher.update(self.expected_predecessor.checkpoint_id.to_be_bytes());
        hasher.update(self.expected_predecessor.fencing_token.to_be_bytes());
        hasher.update(self.fencing_token.to_be_bytes());
        hasher.update(self.target.epoch.to_be_bytes());
        hasher.update(self.target.checkpoint_id.to_be_bytes());
        update_length(&mut hasher, self.entries.len());
        for entry in &self.entries {
            hasher.update(entry.attempt.epoch.to_be_bytes());
            hasher.update(entry.attempt.checkpoint_id.to_be_bytes());
            hasher.update(entry.participant_id.to_be_bytes());
            match &entry.payload {
                Some(payload) => {
                    hasher.update([1]);
                    update_framed(&mut hasher, payload);
                }
                None => hasher.update([0]),
            }
        }
        hasher.finalize().into()
    }

    /// Validate canonical attempt/participant order and all fixed control-plane bounds.
    /// This check is independent of external state and must run before connector I/O.
    ///
    /// # Errors
    /// Returns a diagnostic when the batch is malformed or exceeds a fixed bound.
    pub fn validate_shape(&self) -> Result<(), String> {
        use laminar_core::checkpoint::CheckpointAttemptRelation;

        if !self.target.is_canonical() {
            return Err(
                "coordinated batch target must use one nonzero canonical checkpoint ID".into(),
            );
        }
        if let Some(entry) = self
            .entries
            .iter()
            .find(|entry| entry.participant_id == 0 || !entry.attempt.is_canonical())
        {
            return Err(format!(
                "coordinated batch entry must use a nonzero participant and canonical checkpoint ID; got participant {}",
                entry.participant_id
            ));
        }
        if self.expected_predecessor.checkpoint_id >= self.target.checkpoint_id {
            return Err(format!(
                "invalid coordinated batch predecessor {} for target {}",
                self.expected_predecessor.checkpoint_id, self.target.checkpoint_id
            ));
        }
        if (self.expected_predecessor.checkpoint_id == 0)
            != (self.expected_predecessor.fencing_token == 0)
        {
            return Err(
                "coordinated batch predecessor must be either an exact non-zero cursor or the zero cursor"
                    .into(),
            );
        }
        if self.fencing_token == 0 {
            return Err("coordinated batch fencing token must be non-zero".into());
        }
        if self.entries.is_empty() || self.entries.len() > MAX_COORDINATED_COMMIT_BATCH_ENTRIES {
            return Err(format!(
                "coordinated batch entry count must be in 1..={MAX_COORDINATED_COMMIT_BATCH_ENTRIES}"
            ));
        }

        let mut total_payload_bytes = 0usize;
        let mut previous: Option<&CoordinatedCommitPayload> = None;
        for entry in &self.entries {
            if entry.attempt.checkpoint_id <= self.expected_predecessor.checkpoint_id
                || entry.attempt.checkpoint_id > self.target.checkpoint_id
            {
                return Err(
                    "coordinated batch entries do not cover the predecessor-to-target interval"
                        .into(),
                );
            }
            if let Some(payload) = &entry.payload {
                if payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
                    return Err(format!(
                        "coordinated participant payload exceeds the fixed {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES} byte limit"
                    ));
                }
                total_payload_bytes = total_payload_bytes
                    .checked_add(payload.len())
                    .ok_or_else(|| "coordinated batch payload byte count overflow".to_owned())?;
                if total_payload_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
                    return Err(format!(
                        "coordinated batch payloads exceed the fixed {MAX_COORDINATED_COMMIT_BATCH_BYTES} byte limit"
                    ));
                }
            }

            if let Some(previous) = previous {
                match entry.attempt.relation_to(previous.attempt) {
                    CheckpointAttemptRelation::Exact
                        if entry.participant_id > previous.participant_id => {}
                    CheckpointAttemptRelation::Newer => {}
                    CheckpointAttemptRelation::Exact => {
                        return Err(
                            "coordinated batch contains a duplicate or out-of-order attempt/participant key"
                                .into(),
                        );
                    }
                    CheckpointAttemptRelation::Older | CheckpointAttemptRelation::Conflict => {
                        return Err(
                            "coordinated batch attempts are not in coherent epoch/checkpoint order"
                                .into(),
                        );
                    }
                }
            }
            previous = Some(entry);
        }
        if previous.map(|entry| entry.attempt) != Some(self.target) {
            return Err("coordinated batch target is not its final exact attempt".into());
        }
        Ok(())
    }

    /// Validate a cursor freshly read from the external target against this
    /// exact batch. Advancing overlap is safe only at an attempt named by the
    /// batch; rollback or an unproven gap would skip output.
    ///
    /// # Errors
    ///
    /// Returns a diagnostic when the batch is malformed, the observed cursor
    /// proves rollback, or an overlap cannot be tied to an exact batch entry.
    pub fn validate_observed_cursor(
        &self,
        observed: Option<CoordinatedCommitCursor>,
    ) -> Result<(), String> {
        self.validate_shape()?;
        let Some(observed) = observed else {
            return if self.expected_predecessor.checkpoint_id == 0 {
                Ok(())
            } else {
                Err(format!(
                    "external cursor is absent below expected predecessor {}",
                    self.expected_predecessor.checkpoint_id
                ))
            };
        };
        if observed.fencing_token == 0 {
            return Err("external cursor contains a zero fencing token".into());
        }
        if observed.fencing_token > self.fencing_token {
            return Err(format!(
                "external fencing token {} is newer than designated committer token {}",
                observed.fencing_token, self.fencing_token
            ));
        }
        if observed.checkpoint_id >= self.target.checkpoint_id
            && observed.fencing_token != self.fencing_token
        {
            return Err(format!(
                "external cursor at or above target {} has fencing token {}, expected {}",
                self.target.checkpoint_id, observed.fencing_token, self.fencing_token
            ));
        }
        if observed.checkpoint_id < self.expected_predecessor.checkpoint_id {
            return Err(format!(
                "external cursor rolled back from expected predecessor {} to {}",
                self.expected_predecessor.checkpoint_id, observed.checkpoint_id
            ));
        }
        if observed.checkpoint_id == self.expected_predecessor.checkpoint_id
            && observed != self.expected_predecessor
        {
            return Err(format!(
                "external cursor checkpoint {} has fencing token {}, expected predecessor token {}",
                observed.checkpoint_id,
                observed.fencing_token,
                self.expected_predecessor.fencing_token
            ));
        }
        if observed.checkpoint_id > self.expected_predecessor.checkpoint_id
            && observed.fencing_token < self.expected_predecessor.fencing_token
        {
            return Err(format!(
                "external cursor advanced past predecessor {} while fencing token regressed from {} to {}",
                self.expected_predecessor.checkpoint_id,
                self.expected_predecessor.fencing_token,
                observed.fencing_token
            ));
        }
        if observed.checkpoint_id < self.target.checkpoint_id
            && observed.checkpoint_id != self.expected_predecessor.checkpoint_id
            && !self
                .entries
                .iter()
                .any(|entry| entry.attempt.checkpoint_id == observed.checkpoint_id)
        {
            return Err(format!(
                "external cursor {} is not an exact attempt in batch {}..={}",
                observed.checkpoint_id,
                self.expected_predecessor.checkpoint_id,
                self.target.checkpoint_id
            ));
        }
        Ok(())
    }
}

/// Leader-side commit for checkpoint-committable sinks.
///
/// The designated committer aggregates every writer's `pre_commit` descriptor
/// for an epoch into one external commit. Must be idempotent: re-running with
/// the same inputs after a leader failover is a no-op once the target already
/// reflects the epoch.
#[async_trait]
pub trait CoordinatedCommitter: Send + Sync {
    /// Atomically commit the validated participant markers and advance the
    /// namespaced external cursor to the batch's exact target. Empty markers
    /// still advance the cursor.
    async fn commit_aggregated(
        &self,
        batch: CoordinatedCommitBatch,
        context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError>;

    /// Highest checkpoint and fencing authority committed in `namespace`.
    /// A metadata read error must be returned, never converted to an absent
    /// cursor, because that could duplicate a previously committed batch.
    async fn committed_cursor(
        &self,
        namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError>;
}
