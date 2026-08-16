//! Durable vnode→instance assignment snapshots. One object per
//! version at `control/assignment-snapshots/v{N:020}.json`. Chitchat
//! carries the ephemeral copy; these files survive full-cluster
//! restart.
//!
//! Rotation and drain finalization use `PutMode::Create` on separate per-version paths. The
//! append-only winner works on every backend, including `LocalFileSystem`, without relying on
//! conditional overwrite support.

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use tokio_stream::StreamExt;

use crate::checkpoint::AssignmentDrainTransition;

mod model;

pub use model::{AssignmentSnapshot, AssignmentSnapshotRef};
use model::{DrainFinalization, RecoveryMaterialization};

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
        "{SNAPSHOT_PREFIX}v{version:0SNAPSHOT_VERSION_WIDTH$}.json"
    ))
}

fn drain_finalization_path(version: u64) -> OsPath {
    OsPath::from(format!(
        "{DRAIN_FINALIZATION_PREFIX}v{version:0SNAPSHOT_VERSION_WIDTH$}.json"
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
        "{RECOVERY_PROPOSAL_PREFIX}v{version:0SNAPSHOT_VERSION_WIDTH$}/"
    ))
}

fn recovery_materialization_path(version: u64) -> OsPath {
    OsPath::from(format!(
        "{RECOVERY_MATERIALIZATION_PREFIX}v{version:0SNAPSHOT_VERSION_WIDTH$}.json"
    ))
}

fn current_time_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
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
        Ok(RotateOutcome::Conflict(Box::new(winner_snapshot)))
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
            return Ok(RotateOutcome::Conflict(Box::new(winner)));
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
        Ok(RotateOutcome::Conflict(Box::new(winner)))
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
            return Ok(RotateOutcome::Conflict(Box::new(winner)));
        }
        if let Some(winner) = self.load_drain_finalization(draining.version).await? {
            winner.validate_against(draining)?;
            return Ok(RotateOutcome::Conflict(Box::new(winner.proposal)));
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
                    Ok(RotateOutcome::Conflict(Box::new(winner.proposal)))
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
    Conflict(Box<AssignmentSnapshot>),
}

#[cfg(test)]
mod tests;
