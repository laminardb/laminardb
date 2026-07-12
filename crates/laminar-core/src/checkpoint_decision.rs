//! Durable commit marker for checkpoint 2PC.
//!
//! Recovery needs to distinguish "we decided to commit this epoch and
//! crashed mid-commit" from "we never reached the commit point". The
//! coordinator writes this marker at the irrevocable commit point;
//! a matching marker on restart = re-drive commit, absence = roll back. In
//! cluster mode the marker also carries the leader's decision across
//! leader re-election.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use tokio_stream::StreamExt;

/// Per-epoch commit marker store. Presence means committed; absence
/// means the leader never reached the commit point (safe to abort).
pub struct CheckpointDecisionStore {
    store: Arc<dyn ObjectStore>,
    /// Serializes candidates from this instance so its durable creates cannot reorder.
    reservation_lock: tokio::sync::Mutex<()>,
    /// Highest reservation observed or attempted by this store instance.
    reservation_hint: AtomicU64,
    deployment_id: tokio::sync::OnceCell<String>,
}

impl std::fmt::Debug for CheckpointDecisionStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointDecisionStore")
            .finish_non_exhaustive()
    }
}

/// Errors raised by [`CheckpointDecisionStore`] operations.
#[derive(Debug, thiserror::Error)]
pub enum DecisionError {
    /// Underlying object-store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// A persisted decision is malformed or conflicts with the requested cut.
    #[error("checkpoint decision conflict: {0}")]
    Conflict(String),
}

/// Runtime scope of a durable checkpoint decision.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommitDecisionScope {
    /// Embedded or standalone recovery domain.
    Local,
    /// Multi-participant cluster recovery domain.
    Cluster,
}

/// Durable commit decision bound to one concrete checkpoint attempt.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CommitDecision {
    /// Decision payload format.
    pub version: u32,
    /// Recovery domain that created the decision.
    pub scope: CommitDecisionScope,
    /// Committed epoch.
    pub epoch: u64,
    /// Exact checkpoint attempt selected for that epoch.
    pub checkpoint_id: u64,
    /// Durable deployment incarnation that owns this decision.
    pub deployment_id: String,
    /// Canonical (sorted, duplicate-free) checkpoint participant IDs.
    ///
    /// A participant absent from this set was not required to persist a local manifest for this
    /// cut. Recovery uses that distinction to avoid treating an intentionally shed node as
    /// participant-local storage loss.
    pub participants: Vec<u64>,
    /// Participant whose prepared manifest is the canonical manifest for this decision.
    pub manifest_participant_id: u64,
    /// Vnode-assignment generation captured by the checkpoint durability gate.
    ///
    /// Local runtimes use generation `0`; cluster decisions require a non-zero generation.
    pub assignment_version: u64,
}

// v3 binds the manifest owner and exact assignment participant set to the durable decision.
const COMMIT_DECISION_VERSION: u32 = 3;

impl CommitDecision {
    fn validate_shape(&self, path_epoch: u64) -> Result<(), DecisionError> {
        if self.version != COMMIT_DECISION_VERSION {
            return Err(DecisionError::Conflict(format!(
                "decision for epoch {path_epoch} has unsupported version {}; expected \
                 {COMMIT_DECISION_VERSION}",
                self.version
            )));
        }
        if self.epoch == 0 || self.epoch != path_epoch {
            return Err(DecisionError::Conflict(format!(
                "decision path epoch {path_epoch} does not match non-zero payload epoch {}",
                self.epoch
            )));
        }
        if self.checkpoint_id == 0 {
            return Err(DecisionError::Conflict(format!(
                "decision for epoch {path_epoch} has checkpoint ID 0"
            )));
        }
        if self.participants.is_empty() {
            return Err(DecisionError::Conflict(format!(
                "decision for epoch {path_epoch} has no participants"
            )));
        }
        if self.participants.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(DecisionError::Conflict(format!(
                "decision for epoch {path_epoch} participants are not canonical"
            )));
        }
        if self
            .participants
            .binary_search(&self.manifest_participant_id)
            .is_err()
        {
            return Err(DecisionError::Conflict(format!(
                "decision for epoch {path_epoch} manifest participant {} is absent from \
                 participants {:?}",
                self.manifest_participant_id, self.participants
            )));
        }

        match self.scope {
            CommitDecisionScope::Local
                if self.participants != [0]
                    || self.manifest_participant_id != 0
                    || self.assignment_version != 0 =>
            {
                return Err(DecisionError::Conflict(format!(
                    "local decision for epoch {path_epoch} must use participant 0 and assignment \
                     version 0"
                )));
            }
            CommitDecisionScope::Cluster if self.assignment_version == 0 => {
                return Err(DecisionError::Conflict(format!(
                    "cluster decision for epoch {path_epoch} requires a non-zero assignment \
                     version"
                )));
            }
            _ => {}
        }
        Ok(())
    }
}

/// Durable allocator state for globally unique checkpoint IDs.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct CheckpointIdReservation {
    /// Reservation payload format.
    version: u32,
    /// Reserved checkpoint ID, matching the object name.
    checkpoint_id: u64,
}

const CHECKPOINT_ID_RESERVATION_VERSION: u32 = 1;

/// Create-once identity for one durable checkpoint/decision-store incarnation.
/// A storage reset deliberately creates a new value so surviving external sinks cannot mistake
/// a restarted checkpoint-id sequence for their prior writer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct DeploymentIdentity {
    version: u32,
    id: String,
}

const DEPLOYMENT_IDENTITY_VERSION: u32 = 1;

impl CheckpointDecisionStore {
    /// Wrap an existing object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            reservation_lock: tokio::sync::Mutex::new(()),
            reservation_hint: AtomicU64::new(0),
            deployment_id: tokio::sync::OnceCell::new(),
        }
    }

    fn path(epoch: u64) -> OsPath {
        OsPath::from(format!("checkpoint-decisions/epoch={epoch}/commit"))
    }

    fn reservation_root() -> OsPath {
        OsPath::from("checkpoint-id-reservations/")
    }

    fn reservation_path(checkpoint_id: u64) -> OsPath {
        OsPath::from(format!("checkpoint-id-reservations/id={checkpoint_id}"))
    }

    fn deployment_identity_path() -> OsPath {
        OsPath::from("checkpoint-deployment/identity.json")
    }

    fn decode_deployment_identity(bytes: &[u8]) -> Result<String, DecisionError> {
        let identity: DeploymentIdentity = serde_json::from_slice(bytes)
            .map_err(|error| DecisionError::Conflict(format!("deployment identity: {error}")))?;
        if identity.version != DEPLOYMENT_IDENTITY_VERSION {
            return Err(DecisionError::Conflict(format!(
                "deployment identity version {} is unsupported (expected \
                 {DEPLOYMENT_IDENTITY_VERSION})",
                identity.version
            )));
        }
        let parsed = uuid::Uuid::parse_str(&identity.id).map_err(|error| {
            DecisionError::Conflict(format!("deployment identity is not a UUID: {error}"))
        })?;
        let canonical = parsed.to_string();
        if canonical != identity.id || parsed.is_nil() {
            return Err(DecisionError::Conflict(
                "deployment identity must be a canonical non-nil UUID".into(),
            ));
        }
        Ok(canonical)
    }

    async fn read_deployment_identity(&self) -> Result<Option<String>, DecisionError> {
        let result = match self.store.get(&Self::deployment_identity_path()).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        Self::decode_deployment_identity(&bytes).map(Some)
    }

    /// Load the checkpoint namespace's create-once deployment incarnation, creating it when the
    /// durable store is empty. Concurrent cluster members converge through object-store CAS.
    ///
    /// # Errors
    /// Object-store I/O or a malformed/conflicting persisted identity.
    pub async fn load_or_create_deployment_id(&self) -> Result<String, DecisionError> {
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        let _guard = self.reservation_lock.lock().await;
        if let Some(identity) = self.deployment_id.get() {
            return Ok(identity.clone());
        }
        if let Some(identity) = self.read_deployment_identity().await? {
            let _ = self.deployment_id.set(identity.clone());
            return Ok(identity);
        }

        let identity = DeploymentIdentity {
            version: DEPLOYMENT_IDENTITY_VERSION,
            id: uuid::Uuid::now_v7().to_string(),
        };
        let payload = serde_json::to_vec(&identity)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(
                &Self::deployment_identity_path(),
                PutPayload::from(payload),
                options,
            )
            .await
        {
            Ok(_) => {
                let _ = self.deployment_id.set(identity.id.clone());
                Ok(identity.id)
            }
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                let stored = self.read_deployment_identity().await?.ok_or_else(|| {
                    DecisionError::Conflict(
                        "deployment identity disappeared after create conflict".into(),
                    )
                })?;
                let _ = self.deployment_id.set(stored.clone());
                Ok(stored)
            }
            Err(error) => Err(DecisionError::Io(error.to_string())),
        }
    }

    fn reservation_id(location: &str) -> Result<u64, DecisionError> {
        let value = location
            .strip_prefix("checkpoint-id-reservations/id=")
            .ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "malformed checkpoint ID reservation path: {location}"
                ))
            })?;
        if value.is_empty() || value.contains('/') {
            return Err(DecisionError::Conflict(format!(
                "malformed checkpoint ID reservation path: {location}"
            )));
        }
        let checkpoint_id = value.parse::<u64>().map_err(|_| {
            DecisionError::Conflict(format!(
                "malformed checkpoint ID reservation path: {location}"
            ))
        })?;
        if checkpoint_id == 0 {
            return Err(DecisionError::Conflict(
                "checkpoint ID reservation cannot contain ID 0".to_owned(),
            ));
        }
        Ok(checkpoint_id)
    }

    fn encode_reservation(reservation: CheckpointIdReservation) -> Result<Bytes, DecisionError> {
        serde_json::to_vec(&reservation)
            .map(Bytes::from)
            .map_err(|e| DecisionError::Conflict(e.to_string()))
    }

    async fn initialize_reservation_hint(&self) -> Result<(), DecisionError> {
        if self.reservation_hint.load(Ordering::Acquire) != 0 {
            return Ok(());
        }

        let root = Self::reservation_root();
        let mut entries = self.store.list(Some(&root));
        let mut highest = 0_u64;
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| DecisionError::Io(e.to_string()))?;
            highest = highest.max(Self::reservation_id(entry.location.as_ref())?);
        }
        self.reservation_hint.fetch_max(highest, Ordering::AcqRel);
        Ok(())
    }

    fn next_reservation_candidate(&self) -> Result<u64, DecisionError> {
        let prior = self
            .reservation_hint
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |last| {
                last.checked_add(1)
            })
            .map_err(|_| DecisionError::Conflict("checkpoint ID space exhausted u64".to_owned()))?;
        Ok(prior + 1)
    }

    /// Allocate a globally unique, monotonically increasing checkpoint ID.
    ///
    /// Each allocation is an immutable, versioned reservation object. A store
    /// instance lists the durable reservations once to seed a local hint, then
    /// uses `PutMode::Create` to atomically claim candidates. Create races retry
    /// with the next candidate, so concurrent coordinators cannot return the
    /// same ID. IDs may have gaps when a caller crashes after allocation. These
    /// reservation objects are permanent: deleting them can permit ID reuse.
    ///
    /// # Errors
    /// Object-store I/O, a malformed durable reservation path, or exhaustion
    /// of the `u64` ID space.
    pub async fn allocate_checkpoint_id(&self) -> Result<u64, DecisionError> {
        let _guard = self.reservation_lock.lock().await;
        self.initialize_reservation_hint().await?;

        loop {
            let checkpoint_id = self.next_reservation_candidate()?;
            let reservation = CheckpointIdReservation {
                version: CHECKPOINT_ID_RESERVATION_VERSION,
                checkpoint_id,
            };
            let opts = PutOptions {
                mode: PutMode::Create,
                ..PutOptions::default()
            };
            match self
                .store
                .put_opts(
                    &Self::reservation_path(checkpoint_id),
                    PutPayload::from(Self::encode_reservation(reservation)?),
                    opts,
                )
                .await
            {
                Ok(_) => return Ok(checkpoint_id),
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(DecisionError::Io(e.to_string())),
            }
        }
    }

    /// Epoch segment of a `checkpoint-decisions/epoch={N}/...` marker, if it has that shape.
    /// Callers keep their own parse-failure policy (`highest_committed` errors, `prune_before` skips).
    fn epoch_segment(loc: &str) -> Option<&str> {
        loc.strip_prefix("checkpoint-decisions/")?
            .split('/')
            .next()?
            .strip_prefix("epoch=")
    }

    /// CAS-create the commit marker for `epoch`. `Ok(true)` means our
    /// write landed; `Ok(false)` means someone else recorded first
    /// (idempotent — retries after commit are cheap no-ops).
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn record_committed(
        &self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<bool, DecisionError> {
        self.record_committed_scoped(epoch, checkpoint_id, CommitDecisionScope::Local, &[0], 0, 0)
            .await
    }

    /// CAS-create the commit marker for a participant-complete cluster checkpoint.
    ///
    /// `participants` is treated as a set and canonicalized before persistence, so retries from
    /// independent coordinators produce byte-equivalent metadata regardless of discovery order.
    /// The canonical manifest participant must belong to that set. Cluster decisions require a
    /// non-zero assignment version; local callers should use [`Self::record_committed`].
    ///
    /// # Errors
    /// Object-store I/O or invalid/conflicting decision metadata.
    pub async fn record_committed_for_participants(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        participants: &[u64],
        manifest_participant_id: u64,
        assignment_version: u64,
    ) -> Result<bool, DecisionError> {
        self.record_committed_scoped(
            epoch,
            checkpoint_id,
            CommitDecisionScope::Cluster,
            participants,
            manifest_participant_id,
            assignment_version,
        )
        .await
    }

    async fn record_committed_scoped(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CommitDecisionScope,
        participants: &[u64],
        manifest_participant_id: u64,
        assignment_version: u64,
    ) -> Result<bool, DecisionError> {
        let mut participants = participants.to_vec();
        participants.sort_unstable();
        participants.dedup();
        let mut decision = CommitDecision {
            version: COMMIT_DECISION_VERSION,
            scope,
            epoch,
            checkpoint_id,
            deployment_id: String::new(),
            participants,
            manifest_participant_id,
            assignment_version,
        };
        // Validate caller-controlled metadata before creating the durable deployment identity.
        decision.validate_shape(epoch)?;
        decision.deployment_id = self.load_or_create_deployment_id().await?;
        let payload =
            serde_json::to_vec(&decision).map_err(|e| DecisionError::Conflict(e.to_string()))?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(
                &Self::path(epoch),
                PutPayload::from(Bytes::from(payload)),
                opts,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = self.decision(epoch).await?.ok_or_else(|| {
                    DecisionError::Conflict(format!(
                        "decision for epoch {epoch} disappeared during idempotence check"
                    ))
                })?;
                if existing == decision {
                    Ok(false)
                } else {
                    Err(DecisionError::Conflict(format!(
                        "epoch {epoch} already binds checkpoint {} with participants {:?}, \
                         manifest participant {}, and assignment version {}; cannot bind \
                         checkpoint {checkpoint_id} with participants {:?}, manifest participant \
                         {manifest_participant_id}, and assignment version {assignment_version}",
                        existing.checkpoint_id,
                        existing.participants,
                        existing.manifest_participant_id,
                        existing.assignment_version,
                        decision.participants,
                    )))
                }
            }
            Err(e) => Err(DecisionError::Io(e.to_string())),
        }
    }

    /// Load the commit decision for `epoch`.
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn decision(&self, epoch: u64) -> Result<Option<CommitDecision>, DecisionError> {
        let result = match self.store.get(&Self::path(epoch)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => return Err(DecisionError::Io(e.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|e| DecisionError::Io(e.to_string()))?;
        let decision: CommitDecision = serde_json::from_slice(&bytes)
            .map_err(|e| DecisionError::Conflict(format!("epoch {epoch}: {e}")))?;
        decision.validate_shape(epoch)?;
        let expected_deployment = self.load_or_create_deployment_id().await?;
        if decision.deployment_id != expected_deployment {
            return Err(DecisionError::Conflict(format!(
                "decision for epoch {epoch} belongs to deployment {}, current deployment is {}",
                decision.deployment_id, expected_deployment
            )));
        }
        Ok(Some(decision))
    }

    /// True iff a valid commit decision exists for `epoch`.
    ///
    /// # Errors
    /// Returns an error when the decision cannot be read or validated.
    pub async fn is_committed(&self, epoch: u64) -> Result<bool, DecisionError> {
        Ok(self.decision(epoch).await?.is_some())
    }

    /// Decision with the highest globally unique checkpoint ID, or `None`.
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn highest_committed(&self) -> Result<Option<CommitDecision>, DecisionError> {
        Ok(self.committed_decisions().await?.pop())
    }

    /// Retained durable decisions ordered by globally unique checkpoint ID.
    ///
    /// The first item may be the GC continuity anchor immediately below the
    /// retained seal window. Callers use it to reject external cursor rollback.
    ///
    /// # Errors
    /// Object-store I/O, malformed marker names, or malformed decision bodies.
    pub async fn committed_decisions(&self) -> Result<Vec<CommitDecision>, DecisionError> {
        let root = OsPath::from("checkpoint-decisions/");
        let mut entries = self.store.list(Some(&root));
        let mut epochs = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| DecisionError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            let Some(seg) = Self::epoch_segment(loc) else {
                continue;
            };
            // A non-numeric epoch= marker is store corruption; skipping it would silently
            // report a lower committed epoch and rewind past committed data.
            let epoch = seg.parse::<u64>().map_err(|_| {
                DecisionError::Io(format!("malformed checkpoint-decision marker: {loc}"))
            })?;
            epochs.push(epoch);
        }
        epochs.sort_unstable();
        epochs.dedup();
        let mut decisions = Vec::with_capacity(epochs.len());
        for epoch in epochs {
            let decision = self.decision(epoch).await?.ok_or_else(|| {
                DecisionError::Conflict(format!(
                    "decision for epoch {epoch} disappeared during inventory"
                ))
            })?;
            decisions.push(decision);
        }
        decisions.sort_unstable_by_key(|decision| decision.checkpoint_id);
        if let Some(pair) = decisions
            .windows(2)
            .find(|pair| pair[0].checkpoint_id == pair[1].checkpoint_id)
        {
            return Err(DecisionError::Conflict(format!(
                "globally unique checkpoint ID {} is bound to both epoch {} and epoch {}",
                pair[0].checkpoint_id, pair[0].epoch, pair[1].epoch
            )));
        }
        Ok(decisions)
    }

    /// Delete commit markers for `epoch < before`, retaining the newest victim
    /// as a durable continuity anchor. The anchor proves the minimum external
    /// cursor after older seals/descriptors are collected and prevents a target
    /// catalog rollback from being mistaken for a fresh namespace.
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn prune_before(&self, before: u64) -> Result<(), DecisionError> {
        if before == 0 {
            return Ok(());
        }
        let root = OsPath::from("checkpoint-decisions/");
        let mut entries = self.store.list(Some(&root));
        let mut victims: Vec<(u64, OsPath)> = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| DecisionError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            let Some(seg) = Self::epoch_segment(loc) else {
                continue;
            };
            let epoch = seg.parse::<u64>().map_err(|_| {
                DecisionError::Conflict(format!("malformed checkpoint-decision marker: {loc}"))
            })?;
            if epoch < before {
                victims.push((epoch, entry.location));
            }
        }
        victims.sort_unstable_by_key(|(epoch, _)| *epoch);
        // Keep exactly one predecessor below the live retention window.
        victims.pop();
        for (_, victim) in victims {
            match self.store.delete(&victim).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => tracing::warn!(error = %e, "decision prune: delete failed"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    fn store_in(dir: &std::path::Path) -> CheckpointDecisionStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        CheckpointDecisionStore::new(fs)
    }

    #[tokio::test]
    async fn absent_before_recorded() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(!s.is_committed(1).await.unwrap());
    }

    #[tokio::test]
    async fn deployment_identity_is_create_once_across_store_instances() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let first = CheckpointDecisionStore::new(Arc::clone(&object_store));
        let second = CheckpointDecisionStore::new(object_store);

        let first_id = first.load_or_create_deployment_id().await.unwrap();
        let second_id = second.load_or_create_deployment_id().await.unwrap();

        assert_eq!(first_id, second_id);
        assert!(!uuid::Uuid::parse_str(&first_id).unwrap().is_nil());
    }

    #[tokio::test]
    async fn independent_decision_stores_get_distinct_deployment_identities() {
        let first = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let second = CheckpointDecisionStore::new(Arc::new(InMemory::new()));

        assert_ne!(
            first.load_or_create_deployment_id().await.unwrap(),
            second.load_or_create_deployment_id().await.unwrap()
        );
    }

    #[tokio::test]
    async fn record_then_read() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.record_committed(5, 50).await.unwrap());
        assert!(s.is_committed(5).await.unwrap());
        let decision = s.decision(5).await.unwrap().unwrap();
        assert_eq!(decision.checkpoint_id, 50);
        assert_eq!(decision.scope, CommitDecisionScope::Local);
        assert_eq!(decision.participants, [0]);
        assert_eq!(decision.manifest_participant_id, 0);
        assert_eq!(decision.assignment_version, 0);
    }

    #[tokio::test]
    async fn cluster_decision_canonicalizes_and_round_trips_participants() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        assert!(s
            .record_committed_for_participants(5, 50, &[9, 3, 9, 5], 5, 42)
            .await
            .unwrap());
        let decision = s.decision(5).await.unwrap().unwrap();

        assert_eq!(decision.version, COMMIT_DECISION_VERSION);
        assert_eq!(decision.scope, CommitDecisionScope::Cluster);
        assert_eq!(decision.participants, [3, 5, 9]);
        assert_eq!(decision.manifest_participant_id, 5);
        assert_eq!(decision.assignment_version, 42);
    }

    #[tokio::test]
    async fn cluster_decision_rejects_invalid_participant_metadata() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        for (participants, manifest_participant, assignment_version) in [
            (Vec::new(), 3, 1),
            (vec![3, 5], 7, 1),
            (vec![3, 5], 3, 0),
            (vec![0, 3], 3, 0),
        ] {
            assert!(matches!(
                s.record_committed_for_participants(
                    5,
                    50,
                    &participants,
                    manifest_participant,
                    assignment_version
                )
                .await,
                Err(DecisionError::Conflict(_))
            ));
        }
    }

    #[test]
    fn durable_decision_shape_requires_canonical_participants() {
        let base = CommitDecision {
            version: COMMIT_DECISION_VERSION,
            scope: CommitDecisionScope::Cluster,
            epoch: 5,
            checkpoint_id: 50,
            deployment_id: uuid::Uuid::now_v7().to_string(),
            participants: vec![3, 5],
            manifest_participant_id: 3,
            assignment_version: 7,
        };
        assert!(base.validate_shape(5).is_ok());

        for participants in [vec![5, 3], vec![3, 3], Vec::new()] {
            let malformed = CommitDecision {
                participants,
                ..base.clone()
            };
            assert!(matches!(
                malformed.validate_shape(5),
                Err(DecisionError::Conflict(_))
            ));
        }

        for malformed in [
            CommitDecision {
                version: COMMIT_DECISION_VERSION - 1,
                ..base.clone()
            },
            CommitDecision {
                epoch: 0,
                ..base.clone()
            },
            CommitDecision {
                checkpoint_id: 0,
                ..base.clone()
            },
            CommitDecision {
                manifest_participant_id: 7,
                ..base.clone()
            },
            CommitDecision {
                assignment_version: 0,
                ..base.clone()
            },
        ] {
            assert!(matches!(
                malformed.validate_shape(5),
                Err(DecisionError::Conflict(_))
            ));
        }

        let malformed_local = CommitDecision {
            scope: CommitDecisionScope::Local,
            participants: vec![0, 3],
            manifest_participant_id: 0,
            assignment_version: 0,
            ..base
        };
        assert!(matches!(
            malformed_local.validate_shape(5),
            Err(DecisionError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn cluster_decision_accepts_zero_as_a_real_node_id() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());

        store
            .record_committed_for_participants(5, 50, &[2, 0, 1], 0, 7)
            .await
            .unwrap();
        let decision = store.decision(5).await.unwrap().unwrap();

        assert_eq!(decision.participants, [0, 1, 2]);
        assert_eq!(decision.manifest_participant_id, 0);
        assert_eq!(decision.assignment_version, 7);
    }

    #[tokio::test]
    async fn second_record_is_noop() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.record_committed(7, 70).await.unwrap());
        assert!(!s.record_committed(7, 70).await.unwrap());
        assert!(s.is_committed(7).await.unwrap());
    }

    #[tokio::test]
    async fn epochs_are_independent() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        s.record_committed(1, 10).await.unwrap();
        assert!(s.is_committed(1).await.unwrap());
        assert!(!s.is_committed(2).await.unwrap());
    }

    #[tokio::test]
    async fn highest_committed_picks_max() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert_eq!(s.highest_committed().await.unwrap(), None);
        s.record_committed(3, 30).await.unwrap();
        s.record_committed(7, 70).await.unwrap();
        s.record_committed(5, 50).await.unwrap();
        assert_eq!(s.highest_committed().await.unwrap().unwrap().epoch, 7);
    }

    #[tokio::test]
    async fn prune_drops_older() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        for e in 1..=5 {
            s.record_committed(e, e * 10).await.unwrap();
        }
        s.prune_before(4).await.unwrap();
        for e in 1..=2 {
            assert!(
                !s.is_committed(e).await.unwrap(),
                "epoch {e} should be pruned"
            );
        }
        for e in 3..=5 {
            assert!(s.is_committed(e).await.unwrap(), "epoch {e} should remain");
        }
        assert_eq!(
            s.committed_decisions()
                .await
                .unwrap()
                .into_iter()
                .map(|decision| decision.epoch)
                .collect::<Vec<_>>(),
            vec![3, 4, 5]
        );
    }

    #[tokio::test]
    async fn conflicting_checkpoint_for_epoch_is_rejected() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        s.record_committed(9, 90).await.unwrap();
        assert!(matches!(
            s.record_committed(9, 91).await,
            Err(DecisionError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn duplicate_checkpoint_id_across_epochs_is_rejected_from_inventory() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        store.record_committed(8, 90).await.unwrap();
        store.record_committed(9, 90).await.unwrap();

        let error = store.committed_decisions().await.unwrap_err();
        assert!(error.to_string().contains("checkpoint ID 90"));
        assert!(error.to_string().contains("epoch 8"));
        assert!(error.to_string().contains("epoch 9"));
    }

    #[tokio::test]
    async fn conflicting_cluster_metadata_for_epoch_is_rejected() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        s.record_committed_for_participants(9, 90, &[3, 5], 3, 12)
            .await
            .unwrap();

        assert!(matches!(
            s.record_committed_for_participants(9, 90, &[3, 5], 5, 12)
                .await,
            Err(DecisionError::Conflict(_))
        ));
        assert!(matches!(
            s.record_committed_for_participants(9, 90, &[3, 5], 3, 13)
                .await,
            Err(DecisionError::Conflict(_))
        ));
    }

    #[tokio::test]
    async fn checkpoint_ids_start_at_one_and_increase() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 1);
        assert_eq!(s.allocate_checkpoint_id().await.unwrap(), 2);
        drop(s);

        let restarted = store_in(dir.path());
        assert_eq!(restarted.allocate_checkpoint_id().await.unwrap(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_checkpoint_id_allocations_are_unique_and_monotonic() {
        const ALLOCATIONS: u64 = 64;

        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..ALLOCATIONS {
            let object_store = Arc::clone(&object_store);
            tasks.spawn(async move {
                CheckpointDecisionStore::new(object_store)
                    .allocate_checkpoint_id()
                    .await
                    .unwrap()
            });
        }

        let mut allocated = Vec::with_capacity(usize::try_from(ALLOCATIONS).unwrap());
        while let Some(result) = tasks.join_next().await {
            allocated.push(result.unwrap());
        }
        allocated.sort_unstable();

        assert_eq!(allocated, (1..=ALLOCATIONS).collect::<Vec<_>>());
        assert!(allocated.iter().all(|id| *id != 0));
    }
}
