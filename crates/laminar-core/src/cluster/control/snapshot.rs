//! Durable vnode→instance assignment snapshots. One object per
//! version at `control/assignment-snapshots/v{N:020}.json`. Chitchat
//! carries the ephemeral copy; these files survive full-cluster
//! restart.
//!
//! Rotation and drain finalization use `PutMode::Create` on separate per-version paths. The
//! append-only winner works on every backend, including `LocalFileSystem`, without relying on
//! conditional overwrite support.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_stream::StreamExt;

use crate::checkpoint::{
    AssignmentDrainTransition, CheckpointAssignmentFence, CheckpointParticipant, LeaderProof,
    MAX_CHECKPOINT_PARTICIPANTS,
};
use crate::cluster::discovery::NodeId;
use crate::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};

const SNAPSHOT_PREFIX: &str = "control/assignment-snapshots/";
const RECOVERY_PROPOSAL_PREFIX: &str = "control/assignment-recovery-proposals/v1/";
const RECOVERY_MATERIALIZATION_RELATIVE_PREFIX: &str = "recovery-materializations/v1/";
const RECOVERY_MATERIALIZATION_PREFIX: &str =
    "control/assignment-snapshots/recovery-materializations/v1/";
const DRAIN_FINALIZATION_PREFIX: &str = "control/assignment-drain-finalizations/";
const SNAPSHOT_VERSION_WIDTH: usize = 20;
const DRAIN_FINALIZATION_VERSION: u16 = 1;
const RECOVERY_MATERIALIZATION_VERSION: u16 = 1;
const MAX_RECOVERY_PROPOSAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECOVERY_MATERIALIZATION_BYTES: u64 = 8 * 1024 * 1024 + 1024;
const RECOVERY_PROPOSAL_GC_BATCH: usize = 64;
const RECOVERY_PROPOSAL_GC_MAX_BATCHES: usize = 4;

fn snapshot_path(version: u64) -> OsPath {
    // Fixed-width so lexicographic list order matches numeric order.
    OsPath::from(format!(
        "{SNAPSHOT_PREFIX}v{version:0width$}.json",
        width = SNAPSHOT_VERSION_WIDTH
    ))
}

fn drain_finalization_path(version: u64) -> OsPath {
    OsPath::from(format!(
        "{DRAIN_FINALIZATION_PREFIX}v{version:0width$}.json",
        width = SNAPSHOT_VERSION_WIDTH
    ))
}

fn recovery_proposal_path(reference: &AssignmentSnapshotRef) -> OsPath {
    OsPath::from(format!(
        "{RECOVERY_PROPOSAL_PREFIX}v{:0width$}/sha256={}.json",
        reference.version,
        reference.sha256,
        width = SNAPSHOT_VERSION_WIDTH
    ))
}

fn recovery_proposal_version_prefix(version: u64) -> OsPath {
    OsPath::from(format!(
        "{RECOVERY_PROPOSAL_PREFIX}v{version:0width$}/",
        width = SNAPSHOT_VERSION_WIDTH
    ))
}

fn recovery_materialization_path(version: u64) -> OsPath {
    OsPath::from(format!(
        "{RECOVERY_MATERIALIZATION_PREFIX}v{version:0width$}.json",
        width = SNAPSHOT_VERSION_WIDTH
    ))
}

fn current_time_millis() -> i64 {
    #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

fn version_from_file(name: &str, kind: &str, minimum: u64) -> Result<u64, SnapshotError> {
    let Some(number) = name
        .strip_prefix('v')
        .and_then(|name| name.strip_suffix(".json"))
    else {
        return Err(SnapshotError::Invalid(format!(
            "non-canonical {kind} filename {name}"
        )));
    };
    if number.len() != SNAPSHOT_VERSION_WIDTH || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(SnapshotError::Invalid(format!(
            "non-canonical {kind} filename {name}"
        )));
    }
    let version = number.parse::<u64>().map_err(|error| {
        SnapshotError::Invalid(format!("invalid {kind} filename {name}: {error}"))
    })?;
    if version < minimum {
        return Err(SnapshotError::Invalid(format!(
            "{kind} version must be at least {minimum}"
        )));
    }
    Ok(version)
}

/// Durable vnode-to-instance assignment snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentSnapshot {
    /// Monotonic version. Writers bump on each update.
    pub version: u64,
    /// Key encoding, hashing, and key-group mapping contract used by this owner map.
    pub partitioning_abi_version: u16,
    /// Vnode id → owning instance. `BTreeMap` (not `Vec`) so snapshots
    /// with different `vnode_count` are still deserializable — sparse
    /// indices surface as missing keys the caller can diagnose.
    pub vnodes: BTreeMap<u32, NodeId>,
    /// Exact process roster certified for this assignment generation. A stable node restart
    /// changes this roster and therefore requires a new version even when vnode owners do not.
    pub participants: Vec<CheckpointParticipant>,
    /// Wall-clock timestamp of the last update, millis since epoch.
    pub updated_at_ms: i64,
    /// Pre-rotation drain phase: when set, this snapshot carries the *intended*
    /// next assignment but ownership has NOT changed yet. Nodes mark the vnodes
    /// they are about to lose as draining (pausing those source partitions) so the
    /// pre-rotation checkpoint is a clean cut; the leader then publishes the same
    /// assignment with `draining = false` to commit the rotation.
    #[serde(default)]
    pub draining: bool,
    /// Exact predecessor, successor, and durable leader term for a draining generation.
    /// Present if and only if `draining` is true.
    #[serde(default)]
    pub drain_transition: Option<AssignmentDrainTransition>,
}

/// Exact immutable reference to a staged failure-recovery successor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssignmentSnapshotRef {
    /// Assignment generation carried by the referenced snapshot.
    pub version: u64,
    /// Lowercase hexadecimal SHA-256 of its canonical JSON body.
    pub sha256: String,
    /// Exact canonical body length.
    pub encoded_len: u64,
}

/// Create-only logical assignment head for an authority-selected recovery proposal.
///
/// Recovery uses a separate namespace from graceful-drain intents. A delayed drain writer may
/// still win the raw snapshot key after losing leadership, but it cannot replace this immutable
/// materialization winner for the same assignment version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryMaterialization {
    protocol_version: u16,
    proposal: AssignmentSnapshotRef,
    snapshot: AssignmentSnapshot,
}

impl RecoveryMaterialization {
    fn new(
        proposal: AssignmentSnapshotRef,
        snapshot: AssignmentSnapshot,
    ) -> Result<Self, SnapshotError> {
        let materialization = Self {
            protocol_version: RECOVERY_MATERIALIZATION_VERSION,
            proposal,
            snapshot,
        };
        materialization.validate()?;
        Ok(materialization)
    }

    fn validate(&self) -> Result<(), SnapshotError> {
        self.proposal.validate()?;
        if self.protocol_version != RECOVERY_MATERIALIZATION_VERSION {
            return Err(SnapshotError::Invalid(format!(
                "unsupported recovery materialization version {}",
                self.protocol_version
            )));
        }
        let (_, actual_reference) = self.snapshot.encode_recovery_proposal()?;
        if actual_reference != self.proposal {
            return Err(SnapshotError::Invalid(
                "recovery materialization body does not match its proposal reference".into(),
            ));
        }
        Ok(())
    }
}

impl AssignmentSnapshotRef {
    /// Validate the reference independently of the staged object.
    ///
    /// # Errors
    /// Rejects a non-successor version, malformed digest, or unsafe encoded length.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        if self.version < 2 {
            return Err(SnapshotError::Invalid(
                "recovery proposal must be a successor generation".into(),
            ));
        }
        if self.sha256.len() != 64
            || !self
                .sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(SnapshotError::Invalid(
                "recovery proposal SHA-256 must be 64 lowercase hexadecimal characters".into(),
            ));
        }
        if self.encoded_len == 0
            || self.encoded_len
                > u64::try_from(MAX_RECOVERY_PROPOSAL_BYTES)
                    .expect("recovery proposal byte limit fits u64")
        {
            return Err(SnapshotError::Invalid(format!(
                "recovery proposal encoded length {} is outside 1..={MAX_RECOVERY_PROPOSAL_BYTES}",
                self.encoded_len
            )));
        }
        Ok(())
    }
}

/// Immutable winner that settles one draining snapshot without changing its certified target
/// version. The original transition remains append-only at the snapshot path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DrainFinalization {
    protocol_version: u16,
    transition_digest: [u8; 32],
    proposal: AssignmentSnapshot,
}

impl DrainFinalization {
    fn new(
        draining: &AssignmentSnapshot,
        proposal: AssignmentSnapshot,
    ) -> Result<Self, SnapshotError> {
        let transition = draining.drain_transition.as_ref().ok_or_else(|| {
            SnapshotError::Invalid("drain finalization requires a draining transition".into())
        })?;
        let finalization = Self {
            protocol_version: DRAIN_FINALIZATION_VERSION,
            transition_digest: transition.digest(),
            proposal,
        };
        finalization.validate_against(draining)?;
        Ok(finalization)
    }

    fn validate_against(&self, draining: &AssignmentSnapshot) -> Result<(), SnapshotError> {
        draining.validate()?;
        self.proposal.validate()?;
        let transition = draining.drain_transition.as_ref().ok_or_else(|| {
            SnapshotError::Invalid("drain finalization requires a draining transition".into())
        })?;
        if self.protocol_version != DRAIN_FINALIZATION_VERSION
            || self.transition_digest != transition.digest()
            || !draining.draining
            || self.proposal.draining
            || self.proposal.version != draining.version
            || self.proposal.drain_transition.is_some()
        {
            return Err(SnapshotError::Invalid(
                "drain finalization does not preserve the exact transition identity".into(),
            ));
        }
        let proposed_fence = self.proposal.assignment_fence()?;
        let predecessor = &transition.predecessor;
        let commits_target = proposed_fence == transition.target;
        let aborts_to_predecessor = proposed_fence.assignment_version
            == transition.target.assignment_version
            && proposed_fence.vnode_count == predecessor.vnode_count
            && proposed_fence.assignment_digest == predecessor.assignment_digest
            && proposed_fence.participants == predecessor.participants;
        if !commits_target && !aborts_to_predecessor {
            return Err(SnapshotError::Invalid(
                "drain finalization is neither the certified target nor exact predecessor rollback"
                    .into(),
            ));
        }
        Ok(())
    }
}

impl AssignmentSnapshot {
    /// Empty snapshot at version 0.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            version: 0,
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnodes: BTreeMap::new(),
            participants: Vec::new(),
            updated_at_ms: 0,
            draining: false,
            drain_transition: None,
        }
    }

    /// Next snapshot with bumped version and current wall-clock time.
    #[must_use]
    pub fn next(&self, vnodes: BTreeMap<u32, NodeId>) -> Result<Self, SnapshotError> {
        self.next_for_participants(vnodes, self.participants.clone())
    }

    /// Next snapshot bound to the supplied canonical process roster.
    pub fn next_for_participants(
        &self,
        vnodes: BTreeMap<u32, NodeId>,
        participants: Vec<CheckpointParticipant>,
    ) -> Result<Self, SnapshotError> {
        let version = self
            .version
            .checked_add(1)
            .ok_or_else(|| SnapshotError::Invalid("assignment snapshot version overflow".into()))?;
        let next = Self {
            version,
            partitioning_abi_version: self.partitioning_abi_version,
            vnodes,
            participants,
            updated_at_ms: current_time_millis(),
            draining: false,
            drain_transition: None,
        };
        next.validate()?;
        Ok(next)
    }

    fn validate_assignment(&self) -> Result<(), SnapshotError> {
        if self.partitioning_abi_version != PARTITIONING_ABI_VERSION {
            return Err(SnapshotError::Invalid(format!(
                "assignment snapshot partitioning ABI {} does not match runtime ABI {PARTITIONING_ABI_VERSION}",
                self.partitioning_abi_version
            )));
        }
        if self.participants.len() > MAX_CHECKPOINT_PARTICIPANTS {
            return Err(SnapshotError::Invalid(format!(
                "assignment snapshot has {} participants; maximum is {MAX_CHECKPOINT_PARTICIPANTS}",
                self.participants.len()
            )));
        }
        let vnode_count = u32::try_from(self.vnodes.len()).map_err(|_| {
            SnapshotError::Invalid("assignment snapshot has more than u32::MAX key groups".into())
        })?;
        KeyGroupCount::try_from(vnode_count).map_err(|_| {
            SnapshotError::Invalid(format!(
                "assignment snapshot key-group count must be between 1 and {}, got {vnode_count}",
                crate::state::MAX_KEY_GROUP_COUNT
            ))
        })?;
        let dense = !self.vnodes.is_empty()
            && self
                .vnodes
                .keys()
                .copied()
                .zip(0_u32..)
                .all(|(actual, expected)| actual == expected);
        let canonical_participants = !self.participants.is_empty()
            && self
                .participants
                .windows(2)
                .all(|pair| pair[0].node_id < pair[1].node_id)
            && self.participants.iter().all(|participant| {
                participant.node_id != 0 && !participant.boot_incarnation.is_nil()
            })
            && {
                let owners: BTreeSet<u64> = self.vnodes.values().map(|owner| owner.0).collect();
                owners.len() == self.participants.len()
                    && self
                        .participants
                        .iter()
                        .all(|participant| owners.contains(&participant.node_id))
            };
        if self.version == 0 || !dense || !canonical_participants {
            return Err(SnapshotError::Invalid(
                "assignment snapshot is not canonical".into(),
            ));
        }
        Ok(())
    }

    /// Validate the durable owner map, process roster, and optional drain transition.
    pub fn validate(&self) -> Result<(), SnapshotError> {
        self.validate_assignment()?;
        match (self.draining, self.drain_transition.as_ref()) {
            (false, None) => Ok(()),
            (true, Some(transition)) => {
                let target = self.assignment_fence_unchecked()?;
                if !transition.is_canonical() || transition.target != target {
                    return Err(SnapshotError::Invalid(
                        "draining snapshot does not match its exact target transition".into(),
                    ));
                }
                Ok(())
            }
            _ => Err(SnapshotError::Invalid(
                "assignment drain flag and transition disagree".into(),
            )),
        }
    }

    fn encode_recovery_proposal(&self) -> Result<(Vec<u8>, AssignmentSnapshotRef), SnapshotError> {
        self.validate()?;
        if self.draining || self.drain_transition.is_some() || self.version < 2 {
            return Err(SnapshotError::Invalid(
                "recovery proposal must be a committed successor generation".into(),
            ));
        }
        let encoded = serde_json::to_vec(self)?;
        if encoded.len() > MAX_RECOVERY_PROPOSAL_BYTES {
            return Err(SnapshotError::Invalid(format!(
                "encoded recovery proposal is {} bytes; maximum is {MAX_RECOVERY_PROPOSAL_BYTES}",
                encoded.len()
            )));
        }
        let reference = AssignmentSnapshotRef {
            version: self.version,
            sha256: format!("{:x}", Sha256::digest(&encoded)),
            encoded_len: u64::try_from(encoded.len()).map_err(|_| {
                SnapshotError::Invalid("recovery proposal encoded length overflow".into())
            })?,
        };
        reference.validate()?;
        Ok((encoded, reference))
    }

    fn assignment_fence_unchecked(&self) -> Result<CheckpointAssignmentFence, SnapshotError> {
        let owners: Vec<u64> = self.vnodes.values().map(|owner| owner.0).collect();
        CheckpointAssignmentFence::from_owner_map(self.version, &owners, self.participants.clone())
            .map_err(SnapshotError::Invalid)
    }

    /// Exact checkpoint certificate represented by this snapshot.
    pub fn assignment_fence(&self) -> Result<CheckpointAssignmentFence, SnapshotError> {
        self.validate_assignment()?;
        self.assignment_fence_unchecked()
    }

    /// Create the exact successor as a leader-fenced draining generation.
    ///
    /// # Errors
    /// Rejects a non-committed predecessor, invalid successor, or leader proof outside both
    /// certified rosters.
    pub fn next_draining(
        &self,
        vnodes: BTreeMap<u32, NodeId>,
        participants: Vec<CheckpointParticipant>,
        leader: LeaderProof,
    ) -> Result<Self, SnapshotError> {
        self.validate()?;
        if self.draining {
            return Err(SnapshotError::Invalid(
                "cannot start a drain from a draining assignment".into(),
            ));
        }
        let predecessor = self.assignment_fence()?;
        let mut target = self.next_for_participants(vnodes, participants)?;
        let target_fence = target.assignment_fence()?;
        target.drain_transition = Some(
            AssignmentDrainTransition::new(predecessor, target_fence, leader)
                .map_err(SnapshotError::Invalid)?,
        );
        target.draining = true;
        target.validate()?;
        Ok(target)
    }

    /// Convert a draining generation into its committed target without changing identity.
    pub fn committed_target(&self) -> Result<Self, SnapshotError> {
        self.validate()?;
        if !self.draining {
            return Err(SnapshotError::Invalid(
                "only a draining assignment has a target to commit".into(),
            ));
        }
        let mut committed = self.clone();
        committed.draining = false;
        committed.drain_transition = None;
        committed.updated_at_ms = current_time_millis();
        committed.validate()?;
        Ok(committed)
    }

    /// Convert a draining generation into a committed rollback of its predecessor map.
    pub fn aborted_target(&self, predecessor: &Self) -> Result<Self, SnapshotError> {
        self.validate()?;
        predecessor.validate()?;
        let transition = self.drain_transition.as_ref().ok_or_else(|| {
            SnapshotError::Invalid("draining assignment has no transition".into())
        })?;
        if predecessor.draining
            || predecessor.assignment_fence()? != transition.predecessor
            || self.version != predecessor.version.saturating_add(1)
        {
            return Err(SnapshotError::Invalid(
                "drain rollback does not match the exact predecessor".into(),
            ));
        }
        let mut aborted = predecessor.clone();
        aborted.version = self.version;
        aborted.updated_at_ms = current_time_millis();
        aborted.draining = false;
        aborted.drain_transition = None;
        aborted.validate()?;
        Ok(aborted)
    }

    /// Whether the durable process roster is canonical and covers every vnode owner.
    #[must_use]
    pub fn has_canonical_participants(&self) -> bool {
        self.validate().is_ok()
    }

    /// Convert a `Vec<NodeId>` (one entry per vnode id, dense) into the
    /// `BTreeMap` shape this snapshot uses. Mirrors the layout returned
    /// by `rendezvous_assignment`.
    #[must_use]
    pub fn vnodes_from_vec(assignment: &[NodeId]) -> BTreeMap<u32, NodeId> {
        #[allow(clippy::cast_possible_truncation)]
        assignment
            .iter()
            .enumerate()
            .map(|(i, n)| (i as u32, *n))
            .collect()
    }

    /// Convert the canonical owner map to a dense vector of exactly `vnode_count` entries.
    pub fn to_vnode_vec(&self, vnode_count: u32) -> Result<Vec<NodeId>, SnapshotError> {
        self.validate()?;
        if usize::try_from(vnode_count).ok() != Some(self.vnodes.len()) {
            return Err(SnapshotError::Invalid(format!(
                "assignment {} vnode cardinality {} does not match runtime cardinality {vnode_count}",
                self.version,
                self.vnodes.len()
            )));
        }
        (0..vnode_count)
            .map(|v| {
                self.vnodes.get(&v).copied().ok_or_else(|| {
                    SnapshotError::Invalid(format!(
                        "assignment {} is missing vnode {v}",
                        self.version
                    ))
                })
            })
            .collect()
    }
}

/// I/O wrapper for [`AssignmentSnapshot`] on an object store.
pub struct AssignmentSnapshotStore {
    store: Arc<dyn ObjectStore>,
    /// Exact kind returned by the last successful head inventory/load. Snapshot watchers audit
    /// that same version immediately, so they can reuse this immutable provenance instead of
    /// issuing a speculative overlay GET. Unknown versions still take the verified fallback.
    last_loaded_head: parking_lot::Mutex<Option<(u64, SnapshotHeadKind)>>,
}

struct AssignmentVersionInventory {
    versions: Vec<u64>,
    recovery_materializations: BTreeSet<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SnapshotHeadKind {
    Raw,
    Recovery,
}

impl std::fmt::Debug for AssignmentSnapshotStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssignmentSnapshotStore")
            .finish_non_exhaustive()
    }
}

/// Errors loading or saving an [`AssignmentSnapshot`].
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// Underlying object store I/O failure.
    #[error("object store I/O: {0}")]
    Io(String),
    /// JSON de/serialization failure.
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Snapshot metadata, owner map, or process roster is non-canonical.
    #[error("invalid snapshot: {0}")]
    Invalid(String),
}

impl AssignmentSnapshotStore {
    /// Wrap a pre-constructed object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            last_loaded_head: parking_lot::Mutex::new(None),
        }
    }

    /// Stage one committed successor under its canonical content address.
    ///
    /// Identical retries converge on the same immutable object. This does not change the durable
    /// assignment head; callers publish the returned reference through their fencing authority
    /// before materialization.
    ///
    /// # Errors
    /// Rejects an invalid/non-committed successor or a write that cannot be reconciled exactly.
    pub async fn stage_recovery_proposal(
        &self,
        proposal: &AssignmentSnapshot,
    ) -> Result<AssignmentSnapshotRef, SnapshotError> {
        let (encoded, reference) = proposal.encode_recovery_proposal()?;
        let path = recovery_proposal_path(&reference);
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let put_error = self
            .store
            .put_opts(
                &path,
                PutPayload::from(Bytes::copy_from_slice(&encoded)),
                options,
            )
            .await
            .err();

        match self.load_recovery_proposal(&reference).await {
            Ok(stored) if stored == *proposal => Ok(reference),
            Ok(_) => Err(SnapshotError::Invalid(format!(
                "recovery proposal '{}' differs from the proposed snapshot",
                reference.sha256
            ))),
            Err(reconcile_error) => {
                if let Some(put_error) = put_error {
                    Err(SnapshotError::Io(format!(
                        "recovery proposal write failed ({put_error}); reconciliation failed ({reconcile_error})"
                    )))
                } else {
                    Err(reconcile_error)
                }
            }
        }
    }

    /// Load and verify one exact immutable recovery proposal.
    ///
    /// # Errors
    /// Rejects a missing, malformed, non-canonical, or reference-mismatched object.
    pub async fn load_recovery_proposal(
        &self,
        reference: &AssignmentSnapshotRef,
    ) -> Result<AssignmentSnapshot, SnapshotError> {
        reference.validate()?;
        let result = match self.store.get(&recovery_proposal_path(reference)).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => {
                return Err(SnapshotError::Invalid(format!(
                    "recovery proposal '{}' is missing",
                    reference.sha256
                )));
            }
            Err(error) => return Err(SnapshotError::Io(error.to_string())),
        };
        if result.meta.size != reference.encoded_len {
            return Err(SnapshotError::Invalid(format!(
                "recovery proposal '{}' is {} bytes, expected {}",
                reference.sha256, result.meta.size, reference.encoded_len
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| SnapshotError::Io(error.to_string()))?;
        if u64::try_from(bytes.len()).ok() != Some(reference.encoded_len) {
            return Err(SnapshotError::Invalid(format!(
                "recovery proposal '{}' payload length changed while reading",
                reference.sha256
            )));
        }
        let proposal: AssignmentSnapshot = serde_json::from_slice(&bytes).map_err(|error| {
            SnapshotError::Invalid(format!("recovery proposal '{}': {error}", reference.sha256))
        })?;
        let (canonical, actual_reference) = proposal.encode_recovery_proposal()?;
        if actual_reference != *reference || canonical.as_slice() != bytes.as_ref() {
            return Err(SnapshotError::Invalid(format!(
                "recovery proposal '{}' does not match its content-addressed reference",
                reference.sha256
            )));
        }
        Ok(proposal)
    }

    async fn load_recovery_materialization(
        &self,
        version: u64,
    ) -> Result<Option<RecoveryMaterialization>, SnapshotError> {
        let result = match self
            .store
            .get(&recovery_materialization_path(version))
            .await
        {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(SnapshotError::Io(error.to_string())),
        };
        if result.meta.size == 0 || result.meta.size > MAX_RECOVERY_MATERIALIZATION_BYTES {
            return Err(SnapshotError::Invalid(format!(
                "recovery materialization is {} bytes; expected 1..={MAX_RECOVERY_MATERIALIZATION_BYTES}",
                result.meta.size
            )));
        }
        let bytes = result
            .bytes()
            .await
            .map_err(|error| SnapshotError::Io(error.to_string()))?;
        let materialization: RecoveryMaterialization = serde_json::from_slice(&bytes)?;
        materialization.validate()?;
        if materialization.proposal.version != version {
            return Err(SnapshotError::Invalid(format!(
                "recovery materialization path version {version} references proposal version {}",
                materialization.proposal.version
            )));
        }
        let canonical = serde_json::to_vec(&materialization)?;
        if canonical.as_slice() != bytes.as_ref() {
            return Err(SnapshotError::Invalid(format!(
                "recovery materialization {version} does not use its canonical body"
            )));
        }
        Ok(Some(materialization))
    }

    /// Verify and publish a staged successor as the monotonic durable assignment head.
    ///
    /// The create-only materialization is the version reservation. It lives outside the raw
    /// graceful-drain snapshot namespace, so a drain write already in flight under a superseded
    /// leader cannot occupy or replace the recovery winner. Readers always prefer this record for
    /// its exact version.
    ///
    /// # Errors
    /// Rejects an invalid proposal reference or a durable head other than its predecessor.
    pub(super) async fn materialize_recovery(
        &self,
        reference: &AssignmentSnapshotRef,
    ) -> Result<RotateOutcome, SnapshotError> {
        let proposal = self.load_recovery_proposal(reference).await?;
        let predecessor_version = reference.version.checked_sub(1).ok_or_else(|| {
            SnapshotError::Invalid("recovery proposal has no predecessor generation".into())
        })?;
        let head = self.list_versions().await?.last().copied();
        if head != Some(predecessor_version) && head != Some(reference.version) {
            return Err(SnapshotError::Invalid(format!(
                "recovery materialization requires durable head {predecessor_version} or {}, observed {head:?}",
                reference.version
            )));
        }

        let materialization = RecoveryMaterialization::new(reference.clone(), proposal.clone())?;
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        let result = self
            .store
            .put_opts(
                &recovery_materialization_path(reference.version),
                PutPayload::from(Bytes::from(serde_json::to_vec(&materialization)?)),
                options,
            )
            .await;
        let winner = self
            .load_recovery_materialization(reference.version)
            .await?
            .ok_or_else(|| {
                SnapshotError::Io(format!(
                    "recovery materialization {} was not durably visible",
                    reference.version
                ))
            })?;
        let winner_snapshot = winner.snapshot.clone();
        if result.is_ok() {
            if winner != materialization || winner_snapshot != proposal {
                return Err(SnapshotError::Invalid(format!(
                    "recovery materialization {} changed after its create succeeded",
                    reference.version
                )));
            }
            return Ok(RotateOutcome::Rotated);
        }
        Ok(RotateOutcome::Conflict(winner_snapshot))
    }

    /// Enumerate raw snapshots and recovery materializations with one object-store LIST.
    async fn list_version_inventory(&self) -> Result<AssignmentVersionInventory, SnapshotError> {
        let prefix = OsPath::from(SNAPSHOT_PREFIX);
        let mut entries = self.store.list(Some(&prefix));
        let mut versions = Vec::new();
        let mut recovery_materializations = BTreeSet::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|e| SnapshotError::Io(e.to_string()))?;
            let loc = entry.location.as_ref();
            // Accept only canonical fixed-width snapshot names. Unrelated siblings are ignored,
            // but a snapshot-like name with another shape is a split-history risk and fails load.
            let Some(rest) = loc.strip_prefix(SNAPSHOT_PREFIX) else {
                continue;
            };
            if let Some(name) = rest.strip_prefix(RECOVERY_MATERIALIZATION_RELATIVE_PREFIX) {
                let version = version_from_file(name, "recovery materialization", 2)?;
                versions.push(version);
                recovery_materializations.insert(version);
                continue;
            }
            if !rest.starts_with('v') {
                continue;
            }
            versions.push(version_from_file(rest, "assignment snapshot", 1)?);
        }
        versions.sort_unstable();
        versions.dedup();
        if versions.windows(2).any(|pair| {
            pair[0]
                .checked_add(1)
                .is_none_or(|expected| expected != pair[1])
        }) {
            return Err(SnapshotError::Invalid(
                "assignment snapshot versions are not contiguous".into(),
            ));
        }
        Ok(AssignmentVersionInventory {
            versions,
            recovery_materializations,
        })
    }

    /// Scan the shared history prefix and return every logical version in ascending order.
    async fn list_versions(&self) -> Result<Vec<u64>, SnapshotError> {
        Ok(self.list_version_inventory().await?.versions)
    }

    async fn list_drain_finalization_versions(&self) -> Result<Vec<u64>, SnapshotError> {
        let prefix = OsPath::from(DRAIN_FINALIZATION_PREFIX);
        let mut entries = self.store.list(Some(&prefix));
        let mut versions = Vec::new();
        while let Some(entry) = entries.next().await {
            let entry = entry.map_err(|error| SnapshotError::Io(error.to_string()))?;
            let location = entry.location.as_ref();
            let Some(rest) = location.strip_prefix(DRAIN_FINALIZATION_PREFIX) else {
                continue;
            };
            let Some(number) = rest
                .strip_prefix('v')
                .and_then(|name| name.strip_suffix(".json"))
            else {
                return Err(SnapshotError::Invalid(format!(
                    "non-canonical drain finalization filename {rest}"
                )));
            };
            if number.len() != SNAPSHOT_VERSION_WIDTH
                || !number.bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(SnapshotError::Invalid(format!(
                    "non-canonical drain finalization filename {rest}"
                )));
            }
            let version = number.parse::<u64>().map_err(|error| {
                SnapshotError::Invalid(format!(
                    "invalid drain finalization filename {rest}: {error}"
                ))
            })?;
            if version == 0 {
                return Err(SnapshotError::Invalid(
                    "assignment drain finalization version zero is not durable".into(),
                ));
            }
            versions.push(version);
        }
        versions.sort_unstable();
        versions.dedup();
        Ok(versions)
    }

    /// Load the current (highest-versioned) snapshot; `Ok(None)` on
    /// fresh cluster.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load(&self) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let inventory = self.list_version_inventory().await?;
        let Some(&latest) = inventory.versions.last() else {
            return Ok(None);
        };
        if inventory.recovery_materializations.contains(&latest) {
            let materialization = self
                .load_recovery_materialization(latest)
                .await?
                .ok_or_else(|| {
                    SnapshotError::Io(format!(
                        "listed recovery materialization {latest} disappeared before load"
                    ))
                })?;
            self.last_loaded_head
                .lock()
                .replace((latest, SnapshotHeadKind::Recovery));
            return Ok(Some(materialization.snapshot));
        }
        let loaded = self.load_base_version(latest).await?;
        if loaded.is_some() {
            let mut last_loaded_head = self.last_loaded_head.lock();
            if *last_loaded_head != Some((latest, SnapshotHeadKind::Recovery)) {
                last_loaded_head.replace((latest, SnapshotHeadKind::Raw));
            }
        }
        Ok(loaded)
    }

    /// Load a specific version's snapshot. `Ok(None)` if that version
    /// was never written or has been pruned.
    ///
    /// # Errors
    /// Object-store I/O or JSON decode failure.
    pub async fn load_version(
        &self,
        version: u64,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        if let Some(materialization) = self.load_recovery_materialization(version).await? {
            return Ok(Some(materialization.snapshot));
        }
        self.load_base_version(version).await
    }

    async fn load_base_version(
        &self,
        version: u64,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let Some(snapshot) = self.load_snapshot_object(version).await? else {
            return Ok(None);
        };
        if !snapshot.draining {
            return Ok(Some(snapshot));
        }
        match self.load_drain_finalization(version).await? {
            Some(finalization) => {
                finalization.validate_against(&snapshot)?;
                Ok(Some(finalization.proposal))
            }
            None => Ok(Some(snapshot)),
        }
    }

    /// Load the immutable drain transition underlying a materialized assignment version.
    ///
    /// A terminal `load_version` result intentionally contains only the installed assignment.
    /// Cluster readers use this accessor to bind that materialized result back to the shared
    /// authority decision before adoption. Ordinary assignment versions return `None`.
    ///
    /// # Errors
    /// Object-store I/O, JSON decode failure, or a malformed base snapshot.
    pub async fn load_drain_transition(
        &self,
        version: u64,
    ) -> Result<Option<AssignmentDrainTransition>, SnapshotError> {
        let last_loaded_head = *self.last_loaded_head.lock();
        match last_loaded_head {
            Some((loaded, SnapshotHeadKind::Recovery)) if loaded == version => return Ok(None),
            Some((loaded, SnapshotHeadKind::Raw)) if loaded == version => {
                return Ok(self
                    .load_snapshot_object(version)
                    .await?
                    .and_then(|snapshot| snapshot.drain_transition));
            }
            _ => {}
        }
        if self.load_recovery_materialization(version).await?.is_some() {
            return Ok(None);
        }
        Ok(self
            .load_snapshot_object(version)
            .await?
            .and_then(|snapshot| snapshot.drain_transition))
    }

    async fn load_snapshot_object(
        &self,
        version: u64,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        let path = snapshot_path(version);
        match self.store.get(&path).await {
            Ok(res) => {
                let bytes = res
                    .bytes()
                    .await
                    .map_err(|e| SnapshotError::Io(e.to_string()))?;
                let snap: AssignmentSnapshot = serde_json::from_slice(&bytes)?;
                if snap.version != version {
                    return Err(SnapshotError::Invalid(format!(
                        "snapshot path version {version} contains payload version {}",
                        snap.version
                    )));
                }
                snap.validate()?;
                Ok(Some(snap))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(SnapshotError::Io(e.to_string())),
        }
    }

    async fn load_drain_finalization(
        &self,
        version: u64,
    ) -> Result<Option<DrainFinalization>, SnapshotError> {
        let path = drain_finalization_path(version);
        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result
                    .bytes()
                    .await
                    .map_err(|error| SnapshotError::Io(error.to_string()))?;
                let finalization: DrainFinalization = serde_json::from_slice(&bytes)?;
                if finalization.proposal.version != version {
                    return Err(SnapshotError::Invalid(format!(
                        "drain finalization path version {version} contains payload version {}",
                        finalization.proposal.version
                    )));
                }
                Ok(Some(finalization))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(SnapshotError::Io(error.to_string())),
        }
    }

    async fn create_if_absent(
        &self,
        snapshot: &AssignmentSnapshot,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        snapshot.validate()?;
        let path = snapshot_path(snapshot.version);
        let bytes = serde_json::to_vec_pretty(snapshot)?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self
            .store
            .put_opts(&path, PutPayload::from(Bytes::from(bytes)), opts)
            .await
        {
            Ok(_) => Ok(Some(snapshot.clone())),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(None),
            Err(e) => Err(SnapshotError::Io(e.to_string())),
        }
    }

    async fn prune_recovery_proposals_for_version(
        &self,
        version: u64,
    ) -> Result<(), SnapshotError> {
        let prefix = recovery_proposal_version_prefix(version);
        for _ in 0..RECOVERY_PROPOSAL_GC_MAX_BATCHES {
            let mut entries = self.store.list(Some(&prefix));
            let mut candidates = Vec::with_capacity(RECOVERY_PROPOSAL_GC_BATCH);
            while candidates.len() < RECOVERY_PROPOSAL_GC_BATCH {
                let Some(entry) = entries.next().await else {
                    break;
                };
                candidates.push(
                    entry
                        .map_err(|error| SnapshotError::Io(error.to_string()))?
                        .location,
                );
            }
            if candidates.is_empty() {
                return Ok(());
            }
            let deletions =
                futures::stream::iter(candidates.into_iter().map(Ok::<_, object_store::Error>));
            let mut results = self.store.delete_stream(Box::pin(deletions));
            while let Some(result) = results.next().await {
                if let Err(error) = result {
                    if !matches!(error, object_store::Error::NotFound { .. }) {
                        return Err(SnapshotError::Io(error.to_string()));
                    }
                }
            }
            tokio::task::yield_now().await;
        }

        let mut remaining = self.store.list(Some(&prefix));
        match remaining.next().await {
            None => Ok(()),
            Some(Ok(_)) => Err(SnapshotError::Io(format!(
                "recovery proposal garbage for assignment {version} exceeds the bounded cleanup budget"
            ))),
            Some(Err(error)) => Err(SnapshotError::Io(error.to_string())),
        }
    }

    /// CAS-create the version-one seed. `Ok(None)` means another initial writer won.
    ///
    /// # Errors
    /// Object-store I/O or JSON encode failure.
    pub async fn save_if_absent(
        &self,
        snapshot: &AssignmentSnapshot,
    ) -> Result<Option<AssignmentSnapshot>, SnapshotError> {
        if snapshot.version != 1 {
            return Err(SnapshotError::Invalid(format!(
                "save_if_absent only accepts the version-one seed, got {}",
                snapshot.version
            )));
        }
        if let Some(head) = self
            .list_versions()
            .await?
            .last()
            .copied()
            .filter(|head| *head != 1)
        {
            return Err(SnapshotError::Invalid(format!(
                "cannot seed assignment history with durable head {head}"
            )));
        }
        self.create_if_absent(snapshot).await
    }

    /// Rotate to `snapshot` assuming the current durable version is
    /// `prior_version`. Returns [`RotateOutcome::Conflict`] carrying
    /// the winner's snapshot if a racer produced `prior_version + 1`
    /// first.
    ///
    /// # Errors
    /// Object-store I/O, JSON encode, or a non-monotonic version bump
    /// (caller bug).
    pub async fn save_if_version(
        &self,
        snapshot: &AssignmentSnapshot,
        prior_version: u64,
    ) -> Result<RotateOutcome, SnapshotError> {
        snapshot.validate()?;
        let expected = prior_version
            .checked_add(1)
            .ok_or_else(|| SnapshotError::Invalid("assignment snapshot version overflow".into()))?;
        if snapshot.version != expected {
            return Err(SnapshotError::Invalid(format!(
                "save_if_version requires monotonic +1 bump: prior={prior_version}, \
                 proposed={}",
                snapshot.version,
            )));
        }
        let head = self.list_versions().await?.last().copied();
        if head == Some(expected) {
            let winner = self.load_version(expected).await?.ok_or_else(|| {
                SnapshotError::Io("durable head disappeared while loading CAS winner".into())
            })?;
            return Ok(RotateOutcome::Conflict(winner));
        }
        if head != Some(prior_version) {
            return Err(SnapshotError::Invalid(format!(
                "save_if_version requires durable head {prior_version}, observed {head:?}"
            )));
        }
        if self.create_if_absent(snapshot).await?.is_some() {
            return Ok(RotateOutcome::Rotated);
        }
        let winner = self.load_version(snapshot.version).await?.ok_or_else(|| {
            SnapshotError::Io("CAS conflict but load of winner returned None".into())
        })?;
        Ok(RotateOutcome::Conflict(winner))
    }

    /// Append exactly one immutable winner for a draining object: its target or a rollback.
    ///
    /// The object version is intentionally unchanged: source receipts certify the target
    /// assignment version, so committing the map under another version would discard the very
    /// identity they proved. `PutMode::Create` makes commit versus abort a store-level race with
    /// one winner on local and cloud backends; the original transition remains auditable.
    /// Cluster callers must first serialize the verdict through `LeaderLeaseStore`; this method
    /// only materializes that already-authoritative verdict.
    ///
    /// # Errors
    /// Rejects a stale/non-draining expected value, an unrelated proposal, or a non-head object.
    pub async fn finalize_drain(
        &self,
        draining: &AssignmentSnapshot,
        proposal: &AssignmentSnapshot,
    ) -> Result<RotateOutcome, SnapshotError> {
        let finalization = DrainFinalization::new(draining, proposal.clone())?;
        if self.list_versions().await?.last().copied() != Some(draining.version) {
            return Err(SnapshotError::Invalid(format!(
                "draining assignment {} is no longer the durable head",
                draining.version
            )));
        }

        let current = self
            .load_snapshot_object(draining.version)
            .await?
            .ok_or_else(|| SnapshotError::Io("draining assignment disappeared".into()))?;
        if current != *draining {
            let winner = self
                .load_version(draining.version)
                .await?
                .ok_or_else(|| SnapshotError::Io("drain conflict winner disappeared".into()))?;
            return Ok(RotateOutcome::Conflict(winner));
        }
        if let Some(winner) = self.load_drain_finalization(draining.version).await? {
            winner.validate_against(draining)?;
            return Ok(RotateOutcome::Conflict(winner.proposal));
        }

        let path = drain_finalization_path(draining.version);
        let payload = PutPayload::from(Bytes::from(serde_json::to_vec_pretty(&finalization)?));
        let options = PutOptions {
            mode: PutMode::Create,
            ..PutOptions::default()
        };
        match self.store.put_opts(&path, payload, options).await {
            Ok(_) => Ok(RotateOutcome::Rotated),
            Err(error) => match self.load_drain_finalization(draining.version).await {
                Ok(Some(winner)) => {
                    winner.validate_against(draining)?;
                    Ok(RotateOutcome::Conflict(winner.proposal))
                }
                Ok(None) | Err(_) => Err(SnapshotError::Io(error.to_string())),
            },
        }
    }

    /// Delete every snapshot object with `version < before`.
    /// Idempotent — missing objects are tolerated.
    ///
    /// # Errors
    /// Object-store I/O.
    pub async fn prune_before(&self, before: u64) -> Result<(), SnapshotError> {
        if before == 0 {
            return Ok(());
        }
        let inventory = self.list_version_inventory().await?;
        for version in inventory.versions {
            if version >= before {
                break;
            }
            // Remove every winning and losing staged body while the version marker still exists.
            // A crash leaves that marker discoverable, so the next retention pass resumes GC
            // instead of leaking an orphaned body of up to 8 MiB.
            self.prune_recovery_proposals_for_version(version).await?;
            match self.store.delete(&snapshot_path(version)).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(SnapshotError::Io(e.to_string())),
            }
            if inventory.recovery_materializations.contains(&version) {
                match self
                    .store
                    .delete(&recovery_materialization_path(version))
                    .await
                {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                    Err(error) => return Err(SnapshotError::Io(error.to_string())),
                }
            }
        }
        // Finalization records are in a separate append-only namespace. Scan it independently so
        // a prior failure after deleting the snapshot can be repaired without leaking orphans.
        for version in self.list_drain_finalization_versions().await? {
            if version >= before {
                break;
            }
            let path = drain_finalization_path(version);
            match self.store.delete(&path).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Err(SnapshotError::Io(error.to_string())),
            }
        }
        Ok(())
    }
}

/// Outcome of [`AssignmentSnapshotStore::save_if_version`].
#[derive(Debug, Clone)]
pub enum RotateOutcome {
    /// Our write landed. The snapshot we passed in is now canonical.
    Rotated,
    /// Another writer (a racing leader) won the CAS. The attached
    /// snapshot is what's currently durable; the caller must adopt it
    /// rather than retry with a stale view.
    Conflict(AssignmentSnapshot),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::LeaderProofOwner;
    use object_store::local::LocalFileSystem;
    use tempfile::tempdir;
    use uuid::Uuid;

    fn participant(node_id: u64, boot: u128) -> CheckpointParticipant {
        CheckpointParticipant {
            node_id,
            boot_incarnation: Uuid::from_u128(boot),
        }
    }

    fn leader(node_id: u64, boot: u128, token: u64) -> LeaderProof {
        LeaderProof {
            owner: LeaderProofOwner {
                node_id,
                boot_id: Uuid::from_u128(boot),
                process_term: 1,
            },
            fencing_token: token,
        }
    }

    fn participants_for(vnodes: &BTreeMap<u32, NodeId>) -> Vec<CheckpointParticipant> {
        vnodes
            .values()
            .map(|owner| owner.0)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|node| participant(node, u128::from(node)))
            .collect()
    }

    fn snapshot(vnodes: BTreeMap<u32, NodeId>) -> AssignmentSnapshot {
        let participants = participants_for(&vnodes);
        AssignmentSnapshot::empty()
            .next_for_participants(vnodes, participants)
            .unwrap()
    }

    fn next_snapshot(
        current: &AssignmentSnapshot,
        vnodes: BTreeMap<u32, NodeId>,
    ) -> AssignmentSnapshot {
        let participants = participants_for(&vnodes);
        current.next_for_participants(vnodes, participants).unwrap()
    }

    fn store_in(dir: &std::path::Path) -> AssignmentSnapshotStore {
        let fs: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
        AssignmentSnapshotStore::new(fs)
    }

    async fn put_raw(store: &AssignmentSnapshotStore, path: OsPath, snapshot: &AssignmentSnapshot) {
        let bytes = serde_json::to_vec(snapshot).unwrap();
        store
            .store
            .put(&path, PutPayload::from(Bytes::from(bytes)))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn load_missing_returns_none() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());
        assert!(s.load().await.unwrap().is_none());
    }

    #[test]
    fn assignment_generation_persists_exact_process_roster() {
        let vnodes = BTreeMap::from([(0, NodeId(1)), (1, NodeId(2))]);
        let first = AssignmentSnapshot::empty()
            .next_for_participants(vnodes.clone(), vec![participant(1, 11), participant(2, 22)])
            .unwrap();
        assert!(first.has_canonical_participants());

        let restarted = first
            .next_for_participants(vnodes, vec![participant(1, 11), participant(2, 222)])
            .unwrap();
        assert_eq!(restarted.version, first.version + 1);
        assert_eq!(restarted.vnodes, first.vnodes);
        assert_ne!(restarted.participants, first.participants);
        assert!(restarted.has_canonical_participants());
    }

    #[test]
    fn assignment_generation_rejects_zero_vnode_participants() {
        let error = AssignmentSnapshot::empty()
            .next_for_participants(
                BTreeMap::from([(0, NodeId(1))]),
                vec![participant(1, 11), participant(2, 22)],
            )
            .unwrap_err();

        assert!(matches!(error, SnapshotError::Invalid(message) if message.contains("canonical")));
    }

    #[test]
    fn assignment_snapshot_requires_partitioning_abi() {
        let snapshot = snapshot(BTreeMap::from([(0, NodeId(1))]));
        assert_eq!(snapshot.partitioning_abi_version, PARTITIONING_ABI_VERSION);

        let mut value = serde_json::to_value(snapshot).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("partitioning_abi_version");
        assert!(serde_json::from_value::<AssignmentSnapshot>(value).is_err());
    }

    #[test]
    fn assignment_snapshot_rejects_more_than_the_partitioning_abi_limit() {
        let vnodes = (0..=u32::from(u16::MAX))
            .map(|key_group| (key_group, NodeId(1)))
            .collect();

        assert!(matches!(
            AssignmentSnapshot::empty()
                .next_for_participants(vnodes, vec![participant(1, 11)]),
            Err(SnapshotError::Invalid(message)) if message.contains("key-group count")
        ));
    }

    #[tokio::test]
    async fn durable_assignment_rejects_wrong_partitioning_abi() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let mut snapshot = snapshot(BTreeMap::from([(0, NodeId(1))]));
        snapshot.partitioning_abi_version = PARTITIONING_ABI_VERSION + 1;

        assert!(matches!(
            store.save_if_absent(&snapshot).await,
            Err(SnapshotError::Invalid(message)) if message.contains("partitioning ABI")
        ));

        put_raw(&store, snapshot_path(1), &snapshot).await;
        assert!(matches!(
            store.load().await,
            Err(SnapshotError::Invalid(message)) if message.contains("partitioning ABI")
        ));
    }

    #[tokio::test]
    async fn save_if_absent_then_load_roundtrip() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, NodeId(1));
        vnodes.insert(1, NodeId(2));
        let snap = snapshot(vnodes);

        assert_eq!(s.save_if_absent(&snap).await.unwrap().as_ref(), Some(&snap),);
        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, snap);
    }

    #[test]
    fn snapshot_path_is_canonical_across_the_u64_range() {
        assert_eq!(
            snapshot_path(1).as_ref(),
            "control/assignment-snapshots/v00000000000000000001.json"
        );
        assert_eq!(
            snapshot_path(u64::MAX).as_ref(),
            "control/assignment-snapshots/v18446744073709551615.json"
        );
    }

    #[test]
    fn next_rejects_generation_overflow() {
        let mut current = snapshot(BTreeMap::from([(0, NodeId(1))]));
        current.version = u64::MAX;
        assert!(matches!(
            current.next(current.vnodes.clone()),
            Err(SnapshotError::Invalid(message)) if message.contains("overflow")
        ));
    }

    #[tokio::test]
    async fn seed_write_rejects_non_seed_generation() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
        let second = first.next(first.vnodes.clone()).unwrap();

        assert!(matches!(
            store.save_if_absent(&second).await,
            Err(SnapshotError::Invalid(message)) if message.contains("version-one seed")
        ));
        assert!(store.load().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn seed_write_rejects_retained_nonempty_history() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&first).await.unwrap();
        let second = first.next(first.vnodes.clone()).unwrap();
        store.save_if_version(&second, first.version).await.unwrap();
        let third = second.next(second.vnodes.clone()).unwrap();
        store.save_if_version(&third, second.version).await.unwrap();
        store.prune_before(3).await.unwrap();

        assert!(matches!(
            store.save_if_absent(&first).await,
            Err(SnapshotError::Invalid(message)) if message.contains("durable head 3")
        ));
        assert_eq!(store.list_versions().await.unwrap(), vec![3]);
    }

    #[tokio::test]
    async fn save_rejects_noncanonical_owner_map_and_roster() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let canonical = snapshot(BTreeMap::from([(0, NodeId(1))]));

        let mut sparse = canonical.clone();
        sparse.vnodes = BTreeMap::from([(1, NodeId(1))]);
        assert!(matches!(
            store.save_if_absent(&sparse).await,
            Err(SnapshotError::Invalid(_))
        ));

        let mut uncovered = canonical;
        uncovered.participants.clear();
        assert!(matches!(
            store.save_if_absent(&uncovered).await,
            Err(SnapshotError::Invalid(_))
        ));
    }

    #[tokio::test]
    async fn durable_assignment_rejects_oversized_participant_roster() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let maximum = u64::try_from(MAX_CHECKPOINT_PARTICIPANTS).unwrap();
        let participants = (1..=maximum + 1)
            .map(|node_id| participant(node_id, u128::from(node_id)))
            .collect();
        let oversized = AssignmentSnapshot {
            version: 1,
            partitioning_abi_version: PARTITIONING_ABI_VERSION,
            vnodes: BTreeMap::from([(0, NodeId(1))]),
            participants,
            updated_at_ms: 1,
            draining: false,
            drain_transition: None,
        };

        assert!(matches!(
            store.save_if_absent(&oversized).await,
            Err(SnapshotError::Invalid(message)) if message.contains("maximum is 129")
        ));

        put_raw(&store, snapshot_path(1), &oversized).await;
        assert!(matches!(
            store.load().await,
            Err(SnapshotError::Invalid(message)) if message.contains("maximum is 129")
        ));
    }

    #[tokio::test]
    async fn load_rejects_path_payload_version_mismatch() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
        put_raw(&store, snapshot_path(2), &first).await;

        assert!(matches!(
            store.load_version(2).await,
            Err(SnapshotError::Invalid(message)) if message.contains("payload version")
        ));
    }

    #[tokio::test]
    async fn load_rejects_generation_gap() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
        let second = first.next(first.vnodes.clone()).unwrap();
        let third = second.next(second.vnodes.clone()).unwrap();
        put_raw(&store, snapshot_path(1), &first).await;
        put_raw(&store, snapshot_path(3), &third).await;

        assert!(matches!(
            store.load().await,
            Err(SnapshotError::Invalid(message)) if message.contains("not contiguous")
        ));
    }

    #[tokio::test]
    async fn load_rejects_noncanonical_snapshot_filename() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
        put_raw(
            &store,
            OsPath::from("control/assignment-snapshots/v1.json"),
            &first,
        )
        .await;

        assert!(matches!(
            store.load().await,
            Err(SnapshotError::Invalid(message)) if message.contains("filename")
        ));
    }

    #[tokio::test]
    async fn load_returns_highest_version() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut v1_map = BTreeMap::new();
        v1_map.insert(0, NodeId(1));
        let v1 = snapshot(v1_map);
        s.save_if_absent(&v1).await.unwrap();

        let mut v2_map = BTreeMap::new();
        v2_map.insert(0, NodeId(2));
        let v2 = next_snapshot(&v1, v2_map);
        // Rotate via save_if_version — the canonical post-boot path.
        assert!(matches!(
            s.save_if_version(&v2, v1.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded.version, 2);
        assert_eq!(loaded.vnodes.get(&0), Some(&NodeId(2)));

        // Older version is still readable directly until pruned.
        let v1_loaded = s.load_version(1).await.unwrap().unwrap();
        assert_eq!(v1_loaded, v1);
    }

    #[tokio::test]
    async fn save_if_absent_first_writer_wins() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut first_map = BTreeMap::new();
        first_map.insert(0, NodeId(1));
        first_map.insert(1, NodeId(2));
        let first = snapshot(first_map);

        let winner = s.save_if_absent(&first).await.unwrap();
        assert_eq!(winner.as_ref(), Some(&first), "first writer must win");

        // Second writer attempts a different assignment; should be
        // rejected without mutating the store.
        let mut second_map = BTreeMap::new();
        second_map.insert(0, NodeId(99));
        let second = snapshot(second_map);
        let rejected = s.save_if_absent(&second).await.unwrap();
        assert!(rejected.is_none(), "second writer must lose the CAS");

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, first, "stored snapshot is the first writer's");
    }

    #[tokio::test]
    async fn save_if_version_rejects_non_monotonic_bump() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut m = BTreeMap::new();
        m.insert(0, NodeId(1));
        let v1 = snapshot(m);
        s.save_if_absent(&v1).await.unwrap();

        // Caller builds v3 but claims prior=1 — enforcing monotonic +1
        // catches accidental gap-skipping bugs before they land on
        // durable storage.
        let mut m2 = BTreeMap::new();
        m2.insert(0, NodeId(2));
        let v2 = next_snapshot(&v1, m2);
        let mut m3 = BTreeMap::new();
        m3.insert(0, NodeId(3));
        let v3 = next_snapshot(&v2, m3);
        let err = s.save_if_version(&v3, 1).await.unwrap_err();
        assert!(
            matches!(err, SnapshotError::Invalid(msg) if msg.contains("monotonic")),
            "non-monotonic bump must surface a clear error",
        );
    }

    #[tokio::test]
    async fn save_if_version_rejects_future_prior_without_punching_a_gap() {
        let dir = tempdir().unwrap();
        let store = store_in(dir.path());
        let first = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&first).await.unwrap();
        let second = first.next(first.vnodes.clone()).unwrap();
        store.save_if_version(&second, first.version).await.unwrap();
        let third = second.next(second.vnodes.clone()).unwrap();
        store.save_if_version(&third, second.version).await.unwrap();
        let fourth = third.next(third.vnodes.clone()).unwrap();
        let fifth = fourth.next(fourth.vnodes.clone()).unwrap();
        let sixth = fifth.next(fifth.vnodes.clone()).unwrap();

        assert!(matches!(
            store.save_if_version(&sixth, fifth.version).await,
            Err(SnapshotError::Invalid(message)) if message.contains("durable head 5")
        ));
        assert_eq!(store.list_versions().await.unwrap(), vec![1, 2, 3]);
        assert_eq!(store.load().await.unwrap().unwrap(), third);
    }

    #[tokio::test]
    async fn save_if_version_succeeds_on_match() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut v1_map = BTreeMap::new();
        v1_map.insert(0, NodeId(1));
        let first = snapshot(v1_map);
        s.save_if_absent(&first).await.unwrap();

        let mut v2_map = BTreeMap::new();
        v2_map.insert(0, NodeId(2));
        let second = next_snapshot(&first, v2_map);
        let outcome = s.save_if_version(&second, first.version).await.unwrap();
        assert!(matches!(outcome, RotateOutcome::Rotated));

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, second);
    }

    #[tokio::test]
    async fn save_if_version_conflict_surfaces_winner() {
        // Two racing rotations both propose v2 from v1. CAS at
        // `v{2}.json` picks one; the loser reloads and finds the
        // winner's canonical snapshot.
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        let mut seed = BTreeMap::new();
        seed.insert(0, NodeId(1));
        let v1 = snapshot(seed);
        s.save_if_absent(&v1).await.unwrap();

        let mut winner_map = BTreeMap::new();
        winner_map.insert(0, NodeId(10));
        let winner = next_snapshot(&v1, winner_map);
        assert!(matches!(
            s.save_if_version(&winner, v1.version).await.unwrap(),
            RotateOutcome::Rotated,
        ));

        let mut loser_map = BTreeMap::new();
        loser_map.insert(0, NodeId(20));
        let loser = next_snapshot(&v1, loser_map);
        match s.save_if_version(&loser, v1.version).await.unwrap() {
            RotateOutcome::Conflict(current) => {
                assert_eq!(
                    current, winner,
                    "conflict must surface the winner's snapshot",
                );
            }
            RotateOutcome::Rotated => {
                panic!("stale-token update must not win the CAS");
            }
        }

        let loaded = s.load().await.unwrap().unwrap();
        assert_eq!(loaded, winner, "stored snapshot is the CAS winner's");
    }

    #[tokio::test]
    async fn prune_before_drops_old_versions() {
        let dir = tempdir().unwrap();
        let s = store_in(dir.path());

        // Seed v1..=v4 by repeatedly rotating.
        let mut m = BTreeMap::new();
        m.insert(0, NodeId(1));
        let mut current = snapshot(m);
        s.save_if_absent(&current).await.unwrap();
        for _ in 0..3 {
            let next = current.next(current.vnodes.clone()).unwrap();
            s.save_if_version(&next, current.version).await.unwrap();
            current = next;
        }

        s.prune_before(3).await.unwrap();

        assert!(s.load_version(1).await.unwrap().is_none());
        assert!(s.load_version(2).await.unwrap().is_none());
        assert!(s.load_version(3).await.unwrap().is_some());
        assert!(s.load_version(4).await.unwrap().is_some());
        // `load()` still returns the most recent surviving snapshot.
        assert_eq!(s.load().await.unwrap().unwrap().version, 4);
    }

    #[tokio::test]
    async fn prune_stops_at_first_delete_failure_without_punching_a_gap() {
        use crate::cluster::testing::{FaultyObjectStore, ObjectStoreFault};
        use object_store::memory::InMemory;

        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let faulty = Arc::new(FaultyObjectStore::new(inner));
        let wrapped: Arc<dyn ObjectStore> = faulty.clone();
        let store = AssignmentSnapshotStore::new(wrapped);
        let mut current = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&current).await.unwrap();
        for _ in 0..2 {
            let next = current.next(current.vnodes.clone()).unwrap();
            store.save_if_version(&next, current.version).await.unwrap();
            current = next;
        }

        faulty.set_fault(ObjectStoreFault::FailWrites);
        assert!(matches!(
            store.prune_before(3).await,
            Err(SnapshotError::Io(_))
        ));
        faulty.set_fault(ObjectStoreFault::None);
        assert_eq!(store.list_versions().await.unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn empty_starts_at_version_zero() {
        let s = AssignmentSnapshot::empty();
        assert_eq!(s.version, 0);
        assert!(s.vnodes.is_empty());
    }

    #[test]
    fn next_bumps_version() {
        let mut vnodes = BTreeMap::new();
        vnodes.insert(0, NodeId(1));
        let s = snapshot(vnodes);
        assert_eq!(s.version, 1);
    }

    #[test]
    fn roundtrip_vec_conversions() {
        let assignment = vec![NodeId(1), NodeId(2), NodeId(1), NodeId(2)];
        let map = AssignmentSnapshot::vnodes_from_vec(&assignment);
        let snap = snapshot(map);
        let back = snap
            .to_vnode_vec(u32::try_from(assignment.len()).expect("test len fits u32"))
            .unwrap();
        assert_eq!(back, assignment);
    }

    #[test]
    fn dense_conversion_rejects_smaller_and_larger_runtime_cardinality() {
        let snap = snapshot(BTreeMap::from([(0, NodeId(1)), (1, NodeId(1))]));
        for count in [1, 3] {
            assert!(matches!(
                snap.to_vnode_vec(count),
                Err(SnapshotError::Invalid(message)) if message.contains("vnode cardinality")
            ));
        }
    }

    #[tokio::test]
    async fn recovery_proposal_stage_and_materialization_are_idempotent() {
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let store = AssignmentSnapshotStore::new(backing);
        let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&predecessor).await.unwrap();
        let proposal = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));

        let first_reference = store.stage_recovery_proposal(&proposal).await.unwrap();
        let retry_reference = store.stage_recovery_proposal(&proposal).await.unwrap();
        assert_eq!(retry_reference, first_reference);
        assert_eq!(
            store
                .load_recovery_proposal(&first_reference)
                .await
                .unwrap(),
            proposal
        );
        assert!(matches!(
            store.materialize_recovery(&first_reference).await.unwrap(),
            RotateOutcome::Rotated
        ));
        assert!(matches!(
            store.materialize_recovery(&first_reference).await.unwrap(),
            RotateOutcome::Conflict(existing) if existing == proposal
        ));
    }

    #[tokio::test]
    async fn recovery_materialization_surfaces_a_different_same_version_winner() {
        let backing: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let store = AssignmentSnapshotStore::new(backing);
        let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&predecessor).await.unwrap();
        let winner = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));
        let loser = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(3))]));
        let winner_reference = store.stage_recovery_proposal(&winner).await.unwrap();
        let loser_reference = store.stage_recovery_proposal(&loser).await.unwrap();

        assert!(matches!(
            store.materialize_recovery(&winner_reference).await.unwrap(),
            RotateOutcome::Rotated
        ));
        assert!(matches!(
            store.materialize_recovery(&loser_reference).await.unwrap(),
            RotateOutcome::Conflict(existing) if existing == winner
        ));
    }

    #[tokio::test]
    async fn recovery_retention_removes_winning_and_losing_staged_bodies() {
        let backing = Arc::new(object_store::memory::InMemory::new());
        let store = AssignmentSnapshotStore::new(backing.clone());
        let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&predecessor).await.unwrap();
        let winner = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));
        let loser = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(3))]));
        let winner_reference = store.stage_recovery_proposal(&winner).await.unwrap();
        let loser_reference = store.stage_recovery_proposal(&loser).await.unwrap();
        assert!(matches!(
            store.materialize_recovery(&winner_reference).await.unwrap(),
            RotateOutcome::Rotated
        ));
        let successor = next_snapshot(&winner, winner.vnodes.clone());
        assert!(matches!(
            store
                .save_if_version(&successor, winner.version)
                .await
                .unwrap(),
            RotateOutcome::Rotated
        ));

        store.prune_before(successor.version).await.unwrap();

        for reference in [&winner_reference, &loser_reference] {
            assert!(matches!(
                backing.get(&recovery_proposal_path(reference)).await,
                Err(object_store::Error::NotFound { .. })
            ));
        }
        assert!(matches!(
            backing
                .get(&recovery_materialization_path(winner.version))
                .await,
            Err(object_store::Error::NotFound { .. })
        ));
        assert!(store.load_version(winner.version).await.unwrap().is_none());
        assert_eq!(store.load().await.unwrap(), Some(successor));
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_a_tampered_staged_body() {
        let backing = Arc::new(object_store::memory::InMemory::new());
        let store = AssignmentSnapshotStore::new(backing.clone());
        let predecessor = snapshot(BTreeMap::from([(0, NodeId(1))]));
        store.save_if_absent(&predecessor).await.unwrap();
        let proposal = next_snapshot(&predecessor, BTreeMap::from([(0, NodeId(2))]));
        let reference = store.stage_recovery_proposal(&proposal).await.unwrap();
        let (mut tampered, encoded_reference) = proposal.encode_recovery_proposal().unwrap();
        assert_eq!(encoded_reference, reference);
        let marker = b"\"updated_at_ms\":";
        let value_start = tampered
            .windows(marker.len())
            .position(|window| window == marker)
            .unwrap()
            + marker.len();
        let digit = tampered[value_start..]
            .iter()
            .position(u8::is_ascii_digit)
            .map(|offset| value_start + offset)
            .unwrap();
        tampered[digit] = if tampered[digit] == b'9' { b'8' } else { b'9' };
        backing
            .put(
                &recovery_proposal_path(&reference),
                PutPayload::from(Bytes::from(tampered)),
            )
            .await
            .unwrap();

        assert!(matches!(
            store.load_recovery_proposal(&reference).await,
            Err(SnapshotError::Invalid(message)) if message.contains("content-addressed reference")
        ));
        assert!(store.materialize_recovery(&reference).await.is_err());
        assert_eq!(store.load().await.unwrap(), Some(predecessor));
    }

    #[test]
    fn draining_survives_roundtrip() {
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant(1, 1)])
            .unwrap();
        assert!(!committed.draining);

        let drain = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![participant(2, 2)],
                leader(1, 1, 7),
            )
            .unwrap();
        let json = serde_json::to_vec(&drain).unwrap();
        let back: AssignmentSnapshot = serde_json::from_slice(&json).unwrap();
        back.validate().unwrap();
        assert!(back.draining);
        assert_eq!(back.drain_transition, drain.drain_transition);
        assert_eq!(back.version, drain.version);
    }

    #[tokio::test]
    async fn drain_finalization_commits_the_certified_target_version() {
        let directory = tempdir().unwrap();
        let store = store_in(directory.path());
        let committed = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant(1, 1)])
            .unwrap();
        store.save_if_absent(&committed).await.unwrap();
        let drain = committed
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![participant(2, 2)],
                leader(1, 1, 7),
            )
            .unwrap();
        store
            .save_if_version(&drain, committed.version)
            .await
            .unwrap();
        let transition = drain.drain_transition.as_ref().unwrap().clone();
        let target = drain.committed_target().unwrap();

        assert!(matches!(
            store.finalize_drain(&drain, &target).await.unwrap(),
            RotateOutcome::Rotated
        ));
        let loaded = store.load().await.unwrap().unwrap();
        assert_eq!(loaded, target);
        assert_eq!(loaded.version, drain.version);
        assert_eq!(
            loaded.assignment_fence().unwrap(),
            transition.target.clone()
        );
        assert_eq!(
            store.load_drain_transition(drain.version).await.unwrap(),
            Some(transition)
        );
    }

    #[tokio::test]
    async fn concurrent_drain_commit_and_abort_have_one_append_only_winner() {
        let memory: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let left = AssignmentSnapshotStore::new(Arc::clone(&memory));
        let right = AssignmentSnapshotStore::new(memory);
        let predecessor = AssignmentSnapshot::empty()
            .next_for_participants(BTreeMap::from([(0, NodeId(1))]), vec![participant(1, 1)])
            .unwrap();
        left.save_if_absent(&predecessor).await.unwrap();
        let drain = predecessor
            .next_draining(
                BTreeMap::from([(0, NodeId(2))]),
                vec![participant(2, 2)],
                leader(1, 1, 9),
            )
            .unwrap();
        left.save_if_version(&drain, predecessor.version)
            .await
            .unwrap();
        let commit = drain.committed_target().unwrap();
        let abort = drain.aborted_target(&predecessor).unwrap();

        let (commit_result, abort_result) = tokio::join!(
            left.finalize_drain(&drain, &commit),
            right.finalize_drain(&drain, &abort)
        );
        let outcomes = [commit_result.unwrap(), abort_result.unwrap()];
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, RotateOutcome::Rotated))
                .count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| matches!(outcome, RotateOutcome::Conflict(_)))
                .count(),
            1
        );
        let loaded = left.load().await.unwrap().unwrap();
        assert!(loaded == commit || loaded == abort);
    }
}
