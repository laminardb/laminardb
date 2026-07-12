//! Durable write-ahead decision intent and commit marker for checkpoint 2PC.
//!
//! Recovery needs to distinguish "we decided to commit this epoch and
//! crashed mid-commit" from "we never reached the commit point". The
//! coordinator first persists an immutable canonical intent, then creates the
//! matching commit marker at the irrevocable commit point. A matching marker
//! on restart means re-drive commit; an intent without its marker means the
//! outcome remains in doubt and recovery must fail closed; absence of both is
//! safe to roll back. In cluster mode the records also carry the leader's
//! decision across leader re-election.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use rustc_hash::FxHashMap;
use tokio_stream::StreamExt;

/// Per-epoch write-ahead intent and commit-marker store.
///
/// A valid commit marker means committed. Absence is safe to abort only when no matching durable
/// intent exists; an intent without its marker fences recovery until the final create is resolved.
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
    /// The immutable inventory changed while it was being enumerated, normally because GC
    /// retired a resolved pair. Callers may retry the complete audit from a fresh LIST.
    #[error("checkpoint decision inventory changed during audit: {0}")]
    InventoryChanged(String),
    /// The write-ahead intent is durable but its matching commit create has no terminal outcome.
    #[error(
        "checkpoint decision outcome is in doubt for epoch {epoch}, checkpoint {checkpoint_id}"
    )]
    InDoubt {
        /// Epoch whose commit create may still become visible.
        epoch: u64,
        /// Exact checkpoint attempt bound by the durable intent.
        checkpoint_id: u64,
    },
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

/// Immutable tombstone for every raw decision epoch below `before_epoch`. The full canonical
/// continuity anchor is embedded in the floor itself, so deleted/corrupt raw objects cannot be
/// recreated with different metadata. Floors are never overwritten; readers select the greatest
/// horizon within the current deployment namespace.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct DecisionGcFloor {
    version: u32,
    deployment_id: String,
    before_epoch: u64,
    anchor: Option<CommitDecision>,
}

const DECISION_GC_FLOOR_VERSION: u32 = 1;

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

    fn intent_path(epoch: u64) -> OsPath {
        OsPath::from(format!("checkpoint-decision-intents/epoch={epoch}/intent"))
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

    fn gc_root(deployment_id: &str) -> OsPath {
        OsPath::from(format!(
            "checkpoint-decision-gc/deployment={deployment_id}/"
        ))
    }

    fn gc_floor_path(deployment_id: &str, before_epoch: u64) -> OsPath {
        OsPath::from(format!(
            "checkpoint-decision-gc/deployment={deployment_id}/horizon={before_epoch}/floor"
        ))
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

    /// Epoch segment of a canonical `checkpoint-decisions/epoch={N}/commit` marker.
    fn epoch_segment(loc: &str) -> Option<&str> {
        loc.strip_prefix("checkpoint-decisions/")?
            .strip_suffix("/commit")?
            .strip_prefix("epoch=")
    }

    /// Epoch segment of a canonical write-ahead decision-intent object.
    fn intent_epoch_segment(loc: &str) -> Option<&str> {
        loc.strip_prefix("checkpoint-decision-intents/")?
            .strip_suffix("/intent")?
            .strip_prefix("epoch=")
    }

    fn gc_horizon_segment<'a>(loc: &'a str, deployment_id: &str) -> Option<&'a str> {
        loc.strip_prefix(&format!(
            "checkpoint-decision-gc/deployment={deployment_id}/"
        ))?
        .strip_suffix("/floor")?
        .strip_prefix("horizon=")
    }

    async fn canonical_decision(
        &self,
        epoch: u64,
        checkpoint_id: u64,
        scope: CommitDecisionScope,
        participants: &[u64],
        manifest_participant_id: u64,
        assignment_version: u64,
    ) -> Result<CommitDecision, DecisionError> {
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
        Ok(decision)
    }

    async fn read_decision_record(
        &self,
        path: &OsPath,
        epoch: u64,
        label: &str,
    ) -> Result<Option<CommitDecision>, DecisionError> {
        let result = match self.store.get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        let decision: CommitDecision = serde_json::from_slice(&bytes)
            .map_err(|error| DecisionError::Conflict(format!("{label} epoch {epoch}: {error}")))?;
        decision.validate_shape(epoch)?;
        let canonical = serde_json::to_vec(&decision)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "{label} for epoch {epoch} does not use the canonical decision body"
            )));
        }
        let expected_deployment = self.load_or_create_deployment_id().await?;
        if decision.deployment_id != expected_deployment {
            return Err(DecisionError::Conflict(format!(
                "{label} for epoch {epoch} belongs to deployment {}, current deployment is {}",
                decision.deployment_id, expected_deployment
            )));
        }
        Ok(Some(decision))
    }

    async fn create_decision_record(
        &self,
        path: &OsPath,
        decision: &CommitDecision,
        label: &str,
    ) -> Result<bool, DecisionError> {
        let payload = serde_json::to_vec(decision)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(path, PutPayload::from(payload), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                let existing = self
                    .read_decision_record(path, decision.epoch, label)
                    .await?
                    .ok_or_else(|| {
                        DecisionError::Conflict(format!(
                            "{label} for epoch {} disappeared during idempotence check",
                            decision.epoch
                        ))
                    })?;
                if existing == *decision {
                    Ok(false)
                } else {
                    Err(DecisionError::Conflict(format!(
                        "{label} for epoch {} already binds checkpoint {} with participants {:?}, \
                         manifest participant {}, and assignment version {}; cannot bind \
                         checkpoint {} with participants {:?}, manifest participant {}, and \
                         assignment version {}",
                        decision.epoch,
                        existing.checkpoint_id,
                        existing.participants,
                        existing.manifest_participant_id,
                        existing.assignment_version,
                        decision.checkpoint_id,
                        decision.participants,
                        decision.manifest_participant_id,
                        decision.assignment_version,
                    )))
                }
            }
            Err(error) => Err(DecisionError::Io(error.to_string())),
        }
    }

    async fn decision_intent(&self, epoch: u64) -> Result<Option<CommitDecision>, DecisionError> {
        self.read_decision_record(&Self::intent_path(epoch), epoch, "decision intent")
            .await
    }

    async fn read_gc_floor(
        &self,
        deployment_id: &str,
        before_epoch: u64,
    ) -> Result<Option<DecisionGcFloor>, DecisionError> {
        let path = Self::gc_floor_path(deployment_id, before_epoch);
        let result = match self.store.get(&path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(DecisionError::Io(error.to_string())),
        };
        let bytes = result
            .bytes()
            .await
            .map_err(|error| DecisionError::Io(error.to_string()))?;
        let floor: DecisionGcFloor = serde_json::from_slice(&bytes).map_err(|error| {
            DecisionError::Conflict(format!("decision GC floor {before_epoch}: {error}"))
        })?;
        if floor.version != DECISION_GC_FLOOR_VERSION
            || floor.before_epoch == 0
            || floor.before_epoch != before_epoch
        {
            return Err(DecisionError::Conflict(format!(
                "decision GC floor path horizon {before_epoch} does not match valid versioned body horizon {}",
                floor.before_epoch
            )));
        }
        if let Some(anchor) = floor.anchor.as_ref() {
            anchor.validate_shape(anchor.epoch)?;
            if anchor.epoch >= before_epoch || anchor.deployment_id != floor.deployment_id {
                return Err(DecisionError::Conflict(format!(
                    "decision GC floor {before_epoch} has invalid anchor epoch {} checkpoint {}",
                    anchor.epoch, anchor.checkpoint_id
                )));
            }
        }
        if floor.deployment_id != deployment_id {
            return Err(DecisionError::Conflict(format!(
                "decision GC floor {before_epoch} belongs to deployment {}, current deployment is {deployment_id}",
                floor.deployment_id
            )));
        }
        let canonical = serde_json::to_vec(&floor)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(DecisionError::Conflict(format!(
                "decision GC floor {before_epoch} does not use its canonical body"
            )));
        }
        Ok(Some(floor))
    }

    async fn list_gc_horizons(&self, deployment_id: &str) -> Result<Vec<u64>, DecisionError> {
        let mut entries = self.store.list(Some(&Self::gc_root(deployment_id)));
        let mut horizons = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| DecisionError::Io(error.to_string()))?;
            let location = entry.location.as_ref();
            let horizon = Self::gc_horizon_segment(location, deployment_id)
                .and_then(|segment| segment.parse::<u64>().ok())
                .filter(|horizon| *horizon > 0)
                .ok_or_else(|| {
                    DecisionError::Conflict(format!(
                        "malformed checkpoint-decision GC floor: {location}"
                    ))
                })?;
            horizons.push(horizon);
        }
        horizons.sort_unstable();
        horizons.dedup();
        Ok(horizons)
    }

    async fn latest_gc_floor(&self) -> Result<Option<DecisionGcFloor>, DecisionError> {
        const FLOOR_RETRIES: usize = 3;
        for attempt in 0..FLOOR_RETRIES {
            match self.latest_gc_floor_once().await {
                Err(DecisionError::InventoryChanged(_)) if attempt + 1 < FLOOR_RETRIES => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(DecisionError::InventoryChanged(
            "decision GC floor exhausted stability retries".into(),
        ))
    }

    async fn latest_gc_floor_once(&self) -> Result<Option<DecisionGcFloor>, DecisionError> {
        let deployment_id = self.load_or_create_deployment_id().await?;
        let horizons = self.list_gc_horizons(&deployment_id).await?;
        let Some(horizon) = horizons.last().copied() else {
            return Ok(None);
        };
        self.read_gc_floor(&deployment_id, horizon)
            .await?
            .ok_or_else(|| {
                DecisionError::InventoryChanged(format!(
                    "decision GC floor {horizon} disappeared during inventory"
                ))
            })
            .map(Some)
    }

    /// Greatest durable GC horizon for the current deployment, or zero before compaction.
    /// Followers use this read-only check before deleting participant-local artifacts.
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting floor metadata.
    pub async fn gc_floor_horizon(&self) -> Result<u64, DecisionError> {
        Ok(self
            .latest_gc_floor()
            .await?
            .map_or(0, |floor| floor.before_epoch))
    }

    fn retained_by_floor(decision: &CommitDecision, floor: Option<&DecisionGcFloor>) -> bool {
        let Some(floor) = floor else {
            return true;
        };
        decision.epoch >= floor.before_epoch
    }

    async fn ensure_not_tombstoned(&self, decision: &CommitDecision) -> Result<(), DecisionError> {
        let floor = self.latest_gc_floor().await?;
        if Self::retained_by_floor(decision, floor.as_ref()) {
            Ok(())
        } else {
            let Some(floor) = floor else {
                return Err(DecisionError::Conflict(
                    "decision was rejected without a durable GC floor".into(),
                ));
            };
            Err(DecisionError::Conflict(format!(
                "decision epoch {} checkpoint {} is below durable GC horizon {}",
                decision.epoch, decision.checkpoint_id, floor.before_epoch
            )))
        }
    }

    async fn create_gc_floor(&self, floor: &DecisionGcFloor) -> Result<(), DecisionError> {
        let path = Self::gc_floor_path(&floor.deployment_id, floor.before_epoch);
        let payload = serde_json::to_vec(floor)
            .map(Bytes::from)
            .map_err(|error| DecisionError::Conflict(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(payload), options)
            .await
        {
            Ok(_) => Ok(()),
            Err(
                object_store::Error::Precondition { .. }
                | object_store::Error::AlreadyExists { .. },
            ) => {
                let existing = self
                    .read_gc_floor(&floor.deployment_id, floor.before_epoch)
                    .await?
                    .ok_or_else(|| {
                        DecisionError::InventoryChanged(format!(
                            "decision GC floor {} disappeared after create conflict",
                            floor.before_epoch
                        ))
                    })?;
                if existing == *floor {
                    Ok(())
                } else {
                    Err(DecisionError::Conflict(format!(
                        "decision GC floor {} already exists with a different continuity anchor",
                        floor.before_epoch
                    )))
                }
            }
            Err(error) => Err(DecisionError::Io(error.to_string())),
        }
    }

    /// CAS-create the write-ahead intent and then the commit marker for `epoch`.
    ///
    /// `Ok(true)` means this call created the commit marker; `Ok(false)` means an identical marker
    /// already existed (idempotent retries after commit are cheap no-ops).
    ///
    /// # Errors
    /// Object-store I/O or invalid/conflicting decision metadata.
    pub async fn record_committed(
        &self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<bool, DecisionError> {
        self.record_committed_scoped(epoch, checkpoint_id, CommitDecisionScope::Local, &[0], 0, 0)
            .await
    }

    /// CAS-create the intent and commit marker for a participant-complete cluster checkpoint.
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
        let decision = self
            .canonical_decision(
                epoch,
                checkpoint_id,
                scope,
                participants,
                manifest_participant_id,
                assignment_version,
            )
            .await?;

        // The commit create is not invoked until the canonical intent create has completed. If
        // cancellation happens after that point, recovery observes the durable intent and fails
        // closed until it completes the exact commit marker. A remote intent create that outlives
        // its process still requires a term-fenced authority for a linearizable negative proof;
        // exactly-once admission therefore excludes remote/shared decision stores for now.
        self.ensure_not_tombstoned(&decision).await?;
        self.create_decision_record(&Self::intent_path(epoch), &decision, "decision intent")
            .await?;
        // Retention can advance while intent creation is in flight. Recheck before the final
        // commit create; a stale/tombstoned intent remains inert under the durable floor.
        self.ensure_not_tombstoned(&decision).await?;
        let created = self
            .create_decision_record(&Self::path(epoch), &decision, "commit decision")
            .await?;
        // A GC floor published during the final create wins. Do not report a tombstoned decision
        // as committed even though its late raw bytes may remain for a later sweep.
        self.ensure_not_tombstoned(&decision).await?;
        Ok(created)
    }

    /// Load the commit decision for `epoch`.
    ///
    /// # Errors
    /// Object-store I/O or a malformed/conflicting decision body.
    pub async fn decision(&self, epoch: u64) -> Result<Option<CommitDecision>, DecisionError> {
        const FLOOR_RETRIES: usize = 3;
        for attempt in 0..FLOOR_RETRIES {
            let floor_before = self.latest_gc_floor().await?;
            if floor_before
                .as_ref()
                .is_some_and(|floor| epoch < floor.before_epoch)
            {
                let floor_after = self.latest_gc_floor().await?;
                if floor_before == floor_after {
                    return Ok(None);
                }
                if attempt + 1 < FLOOR_RETRIES {
                    tokio::task::yield_now().await;
                    continue;
                }
                return Err(DecisionError::InventoryChanged(
                    "decision GC floor kept advancing during tombstoned exact lookup".into(),
                ));
            }
            let decision = match self
                .read_decision_record(&Self::path(epoch), epoch, "decision")
                .await
            {
                Ok(decision) => decision,
                Err(error) => {
                    let floor_after = self.latest_gc_floor().await?;
                    if floor_after
                        .as_ref()
                        .is_some_and(|floor| epoch < floor.before_epoch)
                        && floor_after != floor_before
                    {
                        if attempt + 1 < FLOOR_RETRIES {
                            tokio::task::yield_now().await;
                            continue;
                        }
                        return Err(DecisionError::InventoryChanged(
                            "decision became tombstoned during exact lookup".into(),
                        ));
                    }
                    return Err(error);
                }
            };
            let floor_after = self.latest_gc_floor().await?;
            if floor_before != floor_after {
                if attempt + 1 < FLOOR_RETRIES {
                    tokio::task::yield_now().await;
                    continue;
                }
                return Err(DecisionError::InventoryChanged(
                    "decision GC floor kept advancing during exact lookup".into(),
                ));
            }
            return Ok(
                decision.filter(|decision| Self::retained_by_floor(decision, floor_after.as_ref()))
            );
        }
        Err(DecisionError::InventoryChanged(
            "decision exact lookup exhausted floor retries".into(),
        ))
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
    /// Object-store I/O, malformed/conflicting inventory, or an unresolved commit intent.
    pub async fn highest_committed(&self) -> Result<Option<CommitDecision>, DecisionError> {
        Ok(self.recovery_decisions().await?.pop())
    }

    async fn list_committed_epochs(&self) -> Result<Vec<u64>, DecisionError> {
        let root = OsPath::from("checkpoint-decisions/");
        let mut entries = self.store.list(Some(&root));
        let mut epochs = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| DecisionError::Io(error.to_string()))?;
            let location = entry.location.as_ref();
            let segment = Self::epoch_segment(location).ok_or_else(|| {
                DecisionError::Conflict(format!("malformed checkpoint-decision marker: {location}"))
            })?;
            let epoch = segment.parse::<u64>().map_err(|_| {
                DecisionError::Conflict(format!("malformed checkpoint-decision marker: {location}"))
            })?;
            epochs.push(epoch);
        }
        epochs.sort_unstable();
        epochs.dedup();
        Ok(epochs)
    }

    async fn load_committed_inventory_from(
        &self,
        min_epoch: u64,
    ) -> Result<Vec<CommitDecision>, DecisionError> {
        let epochs = self.list_committed_epochs().await?;
        let mut decisions = Vec::with_capacity(epochs.len());
        for epoch in epochs.into_iter().filter(|epoch| *epoch >= min_epoch) {
            let decision = match self
                .read_decision_record(&Self::path(epoch), epoch, "decision")
                .await
            {
                Ok(decision) => decision,
                Err(error) => {
                    if self
                        .latest_gc_floor()
                        .await?
                        .is_some_and(|floor| epoch < floor.before_epoch)
                    {
                        return Err(DecisionError::InventoryChanged(format!(
                            "decision epoch {epoch} became tombstoned during decode"
                        )));
                    }
                    return Err(error);
                }
            }
            .ok_or_else(|| {
                DecisionError::InventoryChanged(format!(
                    "decision for epoch {epoch} disappeared during inventory"
                ))
            })?;
            decisions.push(decision);
        }
        decisions.sort_unstable_by_key(|decision| decision.checkpoint_id);
        Ok(decisions)
    }

    async fn list_intent_epochs(&self) -> Result<Vec<u64>, DecisionError> {
        let root = OsPath::from("checkpoint-decision-intents/");
        let mut entries = self.store.list(Some(&root));
        let mut epochs = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| DecisionError::Io(error.to_string()))?;
            let location = entry.location.as_ref();
            let segment = Self::intent_epoch_segment(location).ok_or_else(|| {
                DecisionError::Conflict(format!("malformed checkpoint-decision intent: {location}"))
            })?;
            let epoch = segment.parse::<u64>().map_err(|_| {
                DecisionError::Conflict(format!("malformed checkpoint-decision intent: {location}"))
            })?;
            epochs.push(epoch);
        }
        epochs.sort_unstable();
        epochs.dedup();
        Ok(epochs)
    }

    async fn load_intent_inventory_from(
        &self,
        min_epoch: u64,
    ) -> Result<Vec<CommitDecision>, DecisionError> {
        let epochs = self.list_intent_epochs().await?;
        let mut intents = Vec::with_capacity(epochs.len());
        for epoch in epochs.into_iter().filter(|epoch| *epoch >= min_epoch) {
            let intent = match self.decision_intent(epoch).await {
                Ok(intent) => intent,
                Err(error) => {
                    if self
                        .latest_gc_floor()
                        .await?
                        .is_some_and(|floor| epoch < floor.before_epoch)
                    {
                        return Err(DecisionError::InventoryChanged(format!(
                            "decision intent epoch {epoch} became tombstoned during decode"
                        )));
                    }
                    return Err(error);
                }
            }
            .ok_or_else(|| {
                DecisionError::InventoryChanged(format!(
                    "decision intent for epoch {epoch} disappeared during inventory"
                ))
            })?;
            intents.push(intent);
        }
        intents.sort_unstable_by_key(|intent| intent.checkpoint_id);
        Ok(intents)
    }

    fn validate_unique_checkpoint_ids(
        records: &[CommitDecision],
        label: &str,
    ) -> Result<(), DecisionError> {
        if let Some(pair) = records
            .windows(2)
            .find(|pair| pair[0].checkpoint_id == pair[1].checkpoint_id)
        {
            return Err(DecisionError::Conflict(format!(
                "globally unique checkpoint ID {} is bound to both epoch {} and epoch {} in the {label} inventory",
                pair[0].checkpoint_id, pair[0].epoch, pair[1].epoch
            )));
        }
        Ok(())
    }

    fn validate_cross_inventory_ids(
        decisions: &[CommitDecision],
        intents: &[CommitDecision],
    ) -> Result<(), DecisionError> {
        let decision_epochs: FxHashMap<u64, u64> = decisions
            .iter()
            .map(|decision| (decision.checkpoint_id, decision.epoch))
            .collect();
        if let Some(intent) = intents.iter().find(|intent| {
            decision_epochs
                .get(&intent.checkpoint_id)
                .is_some_and(|epoch| *epoch != intent.epoch)
        }) {
            let decision_epoch = decision_epochs[&intent.checkpoint_id];
            return Err(DecisionError::Conflict(format!(
                "globally unique checkpoint ID {} is bound to decision epoch {} and intent epoch {}",
                intent.checkpoint_id, decision_epoch, intent.epoch
            )));
        }
        Ok(())
    }

    fn validate_global_attempt_order(
        decisions: &[CommitDecision],
        intents: &[CommitDecision],
    ) -> Result<(), DecisionError> {
        let mut attempts: Vec<(u64, u64)> = decisions
            .iter()
            .chain(intents)
            .map(|record| (record.checkpoint_id, record.epoch))
            .collect();
        attempts.sort_unstable();
        attempts.dedup();
        if let Some(pair) = attempts.windows(2).find(|pair| pair[0].1 >= pair[1].1) {
            return Err(DecisionError::Conflict(format!(
                "checkpoint attempt order regresses from ID {} epoch {} to ID {} epoch {}",
                pair[0].0, pair[0].1, pair[1].0, pair[1].1
            )));
        }
        Ok(())
    }

    async fn load_audited_inventories_once(
        &self,
    ) -> Result<
        (
            Vec<CommitDecision>,
            Vec<CommitDecision>,
            Option<DecisionGcFloor>,
        ),
        DecisionError,
    > {
        let floor_before = self.latest_gc_floor().await?;
        let min_epoch = floor_before.as_ref().map_or(0, |floor| floor.before_epoch);
        // Intent first matches publication order. A writer that starts between LISTs can add a
        // resolved commit to the later inventory; commit-first can instead miss C then observe I
        // and manufacture false InDoubt.
        let mut intents = self.load_intent_inventory_from(min_epoch).await?;
        let mut decisions = self.load_committed_inventory_from(min_epoch).await?;
        let floor_after = self.latest_gc_floor().await?;
        if floor_before != floor_after {
            return Err(DecisionError::InventoryChanged(
                "decision GC floor advanced during inventory audit".into(),
            ));
        }
        if let Some(anchor) = floor_after.as_ref().and_then(|floor| floor.anchor.as_ref()) {
            decisions.push(anchor.clone());
        }
        intents.sort_unstable_by_key(|intent| intent.checkpoint_id);
        decisions.sort_unstable_by_key(|decision| decision.checkpoint_id);
        Self::validate_unique_checkpoint_ids(&intents, "decision intent")?;
        Self::validate_unique_checkpoint_ids(&decisions, "decision")?;
        Self::validate_cross_inventory_ids(&decisions, &intents)?;
        Ok((intents, decisions, floor_after))
    }

    async fn recovery_snapshot_with_floor(
        &self,
    ) -> Result<(Vec<CommitDecision>, Option<DecisionGcFloor>), DecisionError> {
        const INVENTORY_RETRIES: usize = 3;
        for attempt in 0..INVENTORY_RETRIES {
            match self.recovery_snapshot_once().await {
                Err(DecisionError::InventoryChanged(_)) if attempt + 1 < INVENTORY_RETRIES => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(DecisionError::InventoryChanged(
            "decision inventory exhausted stability retries".into(),
        ))
    }

    /// Audited recovery view of durable decisions.
    ///
    /// Every write-ahead intent must have a byte-identical canonical commit marker before a caller
    /// may select any recovery frontier. An unresolved newer intent therefore prevents fallback to
    /// an older committed cut while its final create may still become visible.
    ///
    /// # Errors
    /// Object-store I/O, malformed/conflicting inventory, or an unresolved commit intent.
    pub async fn recovery_snapshot(&self) -> Result<Vec<CommitDecision>, DecisionError> {
        self.recovery_snapshot_with_floor()
            .await
            .map(|(decisions, _)| decisions)
    }

    async fn recovery_snapshot_once(
        &self,
    ) -> Result<(Vec<CommitDecision>, Option<DecisionGcFloor>), DecisionError> {
        let (intents, decisions, floor) = self.load_audited_inventories_once().await?;
        let by_epoch: FxHashMap<u64, &CommitDecision> = decisions
            .iter()
            .map(|decision| (decision.epoch, decision))
            .collect();
        for intent in &intents {
            match by_epoch.get(&intent.epoch) {
                Some(decision) if *decision == intent => {}
                Some(decision) => {
                    return Err(DecisionError::Conflict(format!(
                        "decision intent for epoch {} checkpoint {} does not match committed checkpoint {}",
                        intent.epoch, intent.checkpoint_id, decision.checkpoint_id
                    )));
                }
                None => {
                    return Err(DecisionError::InDoubt {
                        epoch: intent.epoch,
                        checkpoint_id: intent.checkpoint_id,
                    });
                }
            }
        }
        Self::validate_global_attempt_order(&decisions, &intents)?;
        Ok((decisions, floor))
    }

    /// Live recovery decisions, excluding the compacted continuity-only anchor below the GC
    /// horizon. Recovery must never select an anchor whose manifest/state artifacts were deleted.
    ///
    /// # Errors
    /// Object-store I/O, malformed/conflicting inventory, or an unresolved commit intent.
    pub async fn recovery_decisions(&self) -> Result<Vec<CommitDecision>, DecisionError> {
        let (mut decisions, floor) = self.recovery_snapshot_with_floor().await?;
        if let Some(floor) = floor {
            decisions.retain(|decision| decision.epoch >= floor.before_epoch);
            if decisions.is_empty() {
                return Err(DecisionError::Conflict(format!(
                    "decision GC floor {} proves committed history, but no live recovery decision remains at or above it",
                    floor.before_epoch
                )));
            }
        }
        Ok(decisions)
    }

    /// Complete every durable write-ahead intent that does not yet have its matching commit marker.
    ///
    /// Intent creation happens only after the exact state seal and participant inventory are
    /// durable, so restart may safely re-drive the idempotent final create. Callers must invoke
    /// this explicitly before recovery or rollback; ordinary inventory reads remain fail-closed.
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting intent/decision inventory.
    pub async fn resolve_in_doubt(&self) -> Result<usize, DecisionError> {
        const INVENTORY_RETRIES: usize = 3;
        for attempt in 0..INVENTORY_RETRIES {
            match self.resolve_in_doubt_once().await {
                Err(DecisionError::InventoryChanged(_)) if attempt + 1 < INVENTORY_RETRIES => {
                    tokio::task::yield_now().await;
                }
                result => return result,
            }
        }
        Err(DecisionError::InventoryChanged(
            "in-doubt resolution exhausted stability retries".into(),
        ))
    }

    async fn resolve_in_doubt_once(&self) -> Result<usize, DecisionError> {
        let (intents, decisions, starting_floor) = self.load_audited_inventories_once().await?;
        let by_epoch: FxHashMap<u64, &CommitDecision> = decisions
            .iter()
            .map(|decision| (decision.epoch, decision))
            .collect();
        for intent in &intents {
            if let Some(decision) = by_epoch.get(&intent.epoch) {
                if *decision != intent {
                    return Err(DecisionError::Conflict(format!(
                        "decision intent for epoch {} checkpoint {} does not match committed checkpoint {}",
                        intent.epoch, intent.checkpoint_id, decision.checkpoint_id
                    )));
                }
            }
        }
        Self::validate_global_attempt_order(&decisions, &intents)?;
        let mut resolved = 0usize;
        for intent in intents {
            match by_epoch.get(&intent.epoch) {
                Some(decision) if *decision == &intent => {}
                Some(decision) => {
                    return Err(DecisionError::Conflict(format!(
                        "decision intent for epoch {} checkpoint {} does not match committed checkpoint {}",
                        intent.epoch, intent.checkpoint_id, decision.checkpoint_id
                    )));
                }
                None => {
                    if !Self::retained_by_floor(&intent, self.latest_gc_floor().await?.as_ref()) {
                        continue;
                    }
                    self.create_decision_record(
                        &Self::path(intent.epoch),
                        &intent,
                        "commit decision",
                    )
                    .await?;
                    if Self::retained_by_floor(&intent, self.latest_gc_floor().await?.as_ref()) {
                        resolved += 1;
                    }
                }
            }
        }
        if self.latest_gc_floor().await? != starting_floor {
            return Err(DecisionError::InventoryChanged(
                "decision GC floor advanced during in-doubt resolution".into(),
            ));
        }
        Ok(resolved)
    }

    /// Retained durable decisions ordered by globally unique checkpoint ID.
    ///
    /// The first item may be the GC continuity anchor immediately below the
    /// retained seal window. Callers use it to reject external cursor rollback.
    ///
    /// # Errors
    /// Object-store I/O, malformed marker names/bodies, conflicting intent, or an unresolved
    /// commit create.
    pub async fn committed_decisions(&self) -> Result<Vec<CommitDecision>, DecisionError> {
        self.recovery_snapshot().await
    }

    /// Publish an immutable GC floor for `epoch < before`, embedding the greatest-checkpoint-ID
    /// victim as a canonical continuity anchor, then best-effort delete tombstoned raw pairs.
    /// The floor is authoritative before any artifact deletion, so stale or late creates below it
    /// remain inert. An unresolved intent blocks floor advancement.
    ///
    /// # Errors
    /// Object-store I/O or malformed/conflicting decision inventory. Returns the effective durable
    /// floor (which may be higher when another pruner advanced it concurrently).
    pub async fn prune_before(&self, before: u64) -> Result<u64, DecisionError> {
        if before == 0 {
            return Ok(self
                .latest_gc_floor()
                .await?
                .map_or(0, |floor| floor.before_epoch));
        }
        if let Some(floor) = self.latest_gc_floor().await? {
            if floor.before_epoch >= before {
                return Ok(floor.before_epoch);
            }
        }

        // This audit rejects every unmatched intent before floor publication. The returned view
        // also carries the prior embedded anchor, which participates in max-ID compaction.
        let (decisions, observed_floor) = self.recovery_snapshot_with_floor().await?;
        if let Some(floor) = observed_floor.as_ref() {
            if floor.before_epoch >= before {
                return Ok(floor.before_epoch);
            }
        }
        if !decisions.iter().any(|decision| decision.epoch >= before) {
            return Err(DecisionError::Conflict(format!(
                "cannot advance decision GC floor to {before}: no live recovery decision would remain"
            )));
        }
        let anchor = decisions
            .iter()
            .filter(|decision| decision.epoch < before)
            .max_by_key(|decision| decision.checkpoint_id)
            .cloned();
        let floor = DecisionGcFloor {
            version: DECISION_GC_FLOOR_VERSION,
            deployment_id: self.load_or_create_deployment_id().await?,
            before_epoch: before,
            anchor,
        };
        self.create_gc_floor(&floor).await?;

        // A concurrent higher floor supersedes this plan. Reload it and sweep against the
        // effective tombstone rather than deleting from a stale victim set.
        let effective = self.latest_gc_floor().await?.ok_or_else(|| {
            DecisionError::InventoryChanged(
                "decision GC floor disappeared immediately after publication".into(),
            )
        })?;
        if effective.before_epoch < before {
            return Err(DecisionError::InventoryChanged(format!(
                "decision GC floor regressed to {} after publishing {before}",
                effective.before_epoch
            )));
        }

        let raw_intent_epochs = self.list_intent_epochs().await?;
        let raw_decision_epochs = self.list_committed_epochs().await?;
        for epoch in raw_intent_epochs
            .into_iter()
            .filter(|epoch| *epoch < effective.before_epoch)
        {
            if let Err(error) = self.store.delete(&Self::intent_path(epoch)).await {
                if !matches!(&error, object_store::Error::NotFound { .. }) {
                    tracing::warn!(
                        epoch,
                        %error,
                        "decision prune: tombstoned intent delete failed"
                    );
                }
            }
        }
        for epoch in raw_decision_epochs
            .into_iter()
            .filter(|epoch| *epoch < effective.before_epoch)
        {
            match self.store.delete(&Self::path(epoch)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => tracing::warn!(
                    epoch,
                    %error,
                    "decision prune: tombstoned commit delete failed"
                ),
            }
        }
        for horizon in self
            .list_gc_horizons(&effective.deployment_id)
            .await?
            .into_iter()
            .filter(|horizon| *horizon < effective.before_epoch)
        {
            if let Err(error) = self
                .store
                .delete(&Self::gc_floor_path(&effective.deployment_id, horizon))
                .await
            {
                if !matches!(&error, object_store::Error::NotFound { .. }) {
                    tracing::warn!(
                        horizon,
                        effective = effective.before_epoch,
                        %error,
                        "decision prune: superseded GC floor delete failed"
                    );
                }
            }
        }
        Ok(effective.before_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    struct CommitGateStore {
        inner: Arc<dyn ObjectStore>,
        puts: Arc<parking_lot::Mutex<Vec<String>>>,
        release_commit: Arc<tokio::sync::Notify>,
    }

    impl std::fmt::Debug for CommitGateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CommitGateStore").finish_non_exhaustive()
        }
    }

    impl std::fmt::Display for CommitGateStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CommitGateStore")
        }
    }

    #[async_trait]
    impl ObjectStore for CommitGateStore {
        async fn put_opts(
            &self,
            location: &OsPath,
            payload: PutPayload,
            options: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.lock().push(location.to_string());
            if location == &CheckpointDecisionStore::path(5) {
                self.release_commit.notified().await;
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &OsPath,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &OsPath,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<OsPath>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<OsPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&OsPath>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&OsPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &OsPath,
            to: &OsPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    fn store_in(dir: &std::path::Path) -> CheckpointDecisionStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        CheckpointDecisionStore::new(fs)
    }

    #[test]
    fn inventory_paths_require_canonical_protocol_names() {
        assert_eq!(
            CheckpointDecisionStore::epoch_segment("checkpoint-decisions/epoch=5/commit"),
            Some("5")
        );
        assert_eq!(
            CheckpointDecisionStore::intent_epoch_segment(
                "checkpoint-decision-intents/epoch=5/intent"
            ),
            Some("5")
        );
        assert_eq!(
            CheckpointDecisionStore::epoch_segment("checkpoint-decisions/epoch=5/other"),
            None
        );
        assert_eq!(
            CheckpointDecisionStore::intent_epoch_segment(
                "checkpoint-decision-intents/epoch=5/other"
            ),
            None
        );
    }

    #[tokio::test]
    async fn decision_body_must_use_canonical_encoding() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let decision = store
            .canonical_decision(5, 50, CommitDecisionScope::Local, &[0], 0, 0)
            .await
            .unwrap();
        let mut body = serde_json::to_vec(&decision).unwrap();
        body.push(b'\n');
        store
            .store
            .put_opts(
                &CheckpointDecisionStore::intent_path(5),
                PutPayload::from(Bytes::from(body)),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();

        let error = store.recovery_snapshot().await.unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(error.to_string().contains("canonical decision body"));
    }

    async fn persist_local_intent_only(
        store: &CheckpointDecisionStore,
        epoch: u64,
        checkpoint_id: u64,
    ) -> CommitDecision {
        let decision = store
            .canonical_decision(epoch, checkpoint_id, CommitDecisionScope::Local, &[0], 0, 0)
            .await
            .unwrap();
        store
            .create_decision_record(
                &CheckpointDecisionStore::intent_path(epoch),
                &decision,
                "decision intent",
            )
            .await
            .unwrap();
        decision
    }

    async fn put_raw_decision_record(
        store: &CheckpointDecisionStore,
        path: OsPath,
        body: &'static [u8],
    ) {
        store
            .store
            .put_opts(
                &path,
                PutPayload::from(Bytes::from_static(body)),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn commit_create_is_not_invoked_until_intent_is_durable() {
        let puts = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let release_commit = Arc::new(tokio::sync::Notify::new());
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let gated: Arc<dyn ObjectStore> = Arc::new(CommitGateStore {
            inner: backing,
            puts: Arc::clone(&puts),
            release_commit: Arc::clone(&release_commit),
        });
        let store = Arc::new(CheckpointDecisionStore::new(gated));
        let commit_path = CheckpointDecisionStore::path(5).to_string();
        let record = {
            let store = Arc::clone(&store);
            tokio::spawn(async move { store.record_committed(5, 50).await })
        };

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if puts.lock().iter().any(|path| path == &commit_path) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        let intent_path = CheckpointDecisionStore::intent_path(5).to_string();
        let protocol_puts: Vec<String> = puts
            .lock()
            .iter()
            .filter(|path| {
                path.as_str() == intent_path.as_str() || path.as_str() == commit_path.as_str()
            })
            .cloned()
            .collect();
        assert_eq!(protocol_puts, vec![intent_path, commit_path]);
        assert!(matches!(
            store.highest_committed().await,
            Err(DecisionError::InDoubt {
                epoch: 5,
                checkpoint_id: 50
            })
        ));

        release_commit.notify_one();
        assert!(record.await.unwrap().unwrap());
        assert_eq!(
            store
                .highest_committed()
                .await
                .unwrap()
                .unwrap()
                .checkpoint_id,
            50
        );
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
        assert_eq!(s.decision_intent(5).await.unwrap(), Some(decision.clone()));
        assert_eq!(decision.checkpoint_id, 50);
        assert_eq!(decision.scope, CommitDecisionScope::Local);
        assert_eq!(decision.participants, [0]);
        assert_eq!(decision.manifest_participant_id, 0);
        assert_eq!(decision.assignment_version, 0);
    }

    #[tokio::test]
    async fn unresolved_intent_blocks_every_recovery_frontier() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        store.record_committed(3, 30).await.unwrap();
        persist_local_intent_only(&store, 5, 50).await;

        for error in [
            store.recovery_snapshot().await.unwrap_err(),
            store.committed_decisions().await.unwrap_err(),
            store.highest_committed().await.unwrap_err(),
        ] {
            assert!(matches!(
                error,
                DecisionError::InDoubt {
                    epoch: 5,
                    checkpoint_id: 50
                }
            ));
        }
        assert_eq!(store.decision(5).await.unwrap(), None);
    }

    #[tokio::test]
    async fn matching_commit_resolves_a_durable_intent() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let intent = persist_local_intent_only(&store, 5, 50).await;
        assert!(matches!(
            store.highest_committed().await,
            Err(DecisionError::InDoubt { .. })
        ));

        assert_eq!(store.resolve_in_doubt().await.unwrap(), 1);
        assert_eq!(store.resolve_in_doubt().await.unwrap(), 0);

        assert_eq!(store.highest_committed().await.unwrap(), Some(intent));
    }

    #[tokio::test]
    async fn conflicting_intent_and_commit_fail_the_recovery_snapshot() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        persist_local_intent_only(&store, 5, 50).await;
        let conflicting = store
            .canonical_decision(5, 51, CommitDecisionScope::Local, &[0], 0, 0)
            .await
            .unwrap();
        store
            .create_decision_record(
                &CheckpointDecisionStore::path(5),
                &conflicting,
                "commit decision",
            )
            .await
            .unwrap();

        let error = store.recovery_snapshot().await.unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(error.to_string().contains("does not match"), "{error}");
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
        for e in 1..=3 {
            assert!(
                !s.is_committed(e).await.unwrap(),
                "epoch {e} should not be a live recovery decision"
            );
            assert_eq!(
                s.decision_intent(e).await.unwrap(),
                None,
                "epoch {e} raw intent should be pruned after compaction"
            );
        }
        for e in 4..=5 {
            assert!(s.is_committed(e).await.unwrap(), "epoch {e} should remain");
            assert!(
                s.decision_intent(e).await.unwrap().is_some(),
                "epoch {e} intent should remain"
            );
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
    async fn tombstoned_malformed_raw_records_are_ignored_before_decode() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        for epoch in 1..=3 {
            store.record_committed(epoch, epoch * 10).await.unwrap();
        }
        assert_eq!(store.prune_before(3).await.unwrap(), 3);

        put_raw_decision_record(
            &store,
            CheckpointDecisionStore::intent_path(1),
            b"not-json-intent",
        )
        .await;
        put_raw_decision_record(
            &store,
            CheckpointDecisionStore::path(1),
            b"not-json-decision",
        )
        .await;

        assert_eq!(
            store
                .committed_decisions()
                .await
                .unwrap()
                .into_iter()
                .map(|decision| decision.epoch)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            store
                .recovery_decisions()
                .await
                .unwrap()
                .into_iter()
                .map(|decision| decision.epoch)
                .collect::<Vec<_>>(),
            vec![3]
        );
    }

    #[tokio::test]
    async fn advancing_floor_carries_anchor_but_excludes_it_from_live_recovery() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let anchor = store
            .canonical_decision(1, 10, CommitDecisionScope::Local, &[0], 0, 0)
            .await
            .unwrap();
        store.record_committed(1, 10).await.unwrap();
        store.record_committed(5, 50).await.unwrap();

        assert_eq!(store.prune_before(3).await.unwrap(), 3);
        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        let first_floor = store
            .read_gc_floor(&deployment_id, 3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_floor.anchor, Some(anchor.clone()));
        assert!(matches!(
            store
                .store
                .get(&CheckpointDecisionStore::intent_path(1))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(matches!(
            store.store.get(&CheckpointDecisionStore::path(1)).await,
            Err(object_store::Error::NotFound { .. })
        ));

        assert_eq!(store.prune_before(4).await.unwrap(), 4);
        let second_floor = store.latest_gc_floor().await.unwrap().unwrap();
        assert_eq!(second_floor.before_epoch, 4);
        assert_eq!(second_floor.anchor, Some(anchor));
        assert_eq!(
            store
                .committed_decisions()
                .await
                .unwrap()
                .into_iter()
                .map(|decision| decision.epoch)
                .collect::<Vec<_>>(),
            vec![1, 5]
        );
        assert_eq!(
            store
                .recovery_decisions()
                .await
                .unwrap()
                .into_iter()
                .map(|decision| decision.epoch)
                .collect::<Vec<_>>(),
            vec![5]
        );
    }

    #[tokio::test]
    async fn unresolved_intent_below_requested_horizon_blocks_floor_creation() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let intent = persist_local_intent_only(&store, 2, 20).await;
        store.record_committed(5, 50).await.unwrap();

        assert!(matches!(
            store.prune_before(4).await,
            Err(DecisionError::InDoubt {
                epoch: 2,
                checkpoint_id: 20
            })
        ));
        assert_eq!(store.gc_floor_horizon().await.unwrap(), 0);
        assert_eq!(store.decision_intent(2).await.unwrap(), Some(intent));
    }

    #[tokio::test]
    async fn floor_fails_closed_when_all_live_decision_records_disappear() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        store.record_committed(1, 10).await.unwrap();
        store.record_committed(5, 50).await.unwrap();
        store.prune_before(4).await.unwrap();

        store
            .store
            .delete(&CheckpointDecisionStore::intent_path(5))
            .await
            .unwrap();
        store
            .store
            .delete(&CheckpointDecisionStore::path(5))
            .await
            .unwrap();

        let error = store.recovery_decisions().await.unwrap_err();
        assert!(matches!(error, DecisionError::Conflict(_)));
        assert!(
            error
                .to_string()
                .contains("no live recovery decision remains"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn exact_lookup_below_floor_is_none_even_if_raw_decision_reappears() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let tombstoned = store
            .canonical_decision(1, 10, CommitDecisionScope::Local, &[0], 0, 0)
            .await
            .unwrap();
        store.record_committed(1, 10).await.unwrap();
        store.record_committed(5, 50).await.unwrap();
        store.prune_before(4).await.unwrap();

        store
            .create_decision_record(
                &CheckpointDecisionStore::path(1),
                &tombstoned,
                "commit decision",
            )
            .await
            .unwrap();

        assert_eq!(store.decision(1).await.unwrap(), None);
        assert!(!store.is_committed(1).await.unwrap());
    }

    #[tokio::test]
    async fn stale_lower_floor_does_not_override_the_highest_horizon() {
        let store = CheckpointDecisionStore::new(Arc::new(InMemory::new()));
        let stale_anchor = store
            .canonical_decision(1, 10, CommitDecisionScope::Local, &[0], 0, 0)
            .await
            .unwrap();
        store.record_committed(1, 10).await.unwrap();
        store.record_committed(5, 50).await.unwrap();
        assert_eq!(store.prune_before(4).await.unwrap(), 4);

        let deployment_id = store.load_or_create_deployment_id().await.unwrap();
        store
            .create_gc_floor(&DecisionGcFloor {
                version: DECISION_GC_FLOOR_VERSION,
                deployment_id,
                before_epoch: 2,
                anchor: Some(stale_anchor),
            })
            .await
            .unwrap();

        assert_eq!(store.gc_floor_horizon().await.unwrap(), 4);
        assert_eq!(store.prune_before(3).await.unwrap(), 4);
        assert_eq!(store.decision(2).await.unwrap(), None);
    }

    #[tokio::test]
    async fn prune_never_removes_an_unresolved_intent() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let intent = persist_local_intent_only(&store, 2, 20).await;

        assert!(matches!(
            store.prune_before(100).await,
            Err(DecisionError::InDoubt {
                epoch: 2,
                checkpoint_id: 20
            })
        ));
        assert_eq!(store.gc_floor_horizon().await.unwrap(), 0);

        assert_eq!(store.decision_intent(2).await.unwrap(), Some(intent));
        assert!(matches!(
            store.recovery_snapshot().await,
            Err(DecisionError::InDoubt {
                epoch: 2,
                checkpoint_id: 20
            })
        ));
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
