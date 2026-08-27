//! Bounded durable replay pins for committed cluster subscriptions.

use std::time::Duration;

#[cfg(test)]
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::checkpoint::{LeaderProof, StreamGeneration};

use super::{now_millis, ClusterCheckpointAuthorityError, LeaderLeaseStore, LeaseError};

const REGISTRY_PATH: &str = "control/subscription-replay-pins/v1.json";
const REGISTRY_VERSION: u16 = 1;
const MAX_REGISTRY_BYTES: u64 = 256 * 1024;
const MAX_REPLAY_PINS: usize = 1_024;
const MAX_CAS_ATTEMPTS: usize = 16;
const REPLAY_PIN_TTL_MS: i64 = 120_000;

/// Interval at which an active gateway must renew a durable replay pin.
pub const SUBSCRIPTION_REPLAY_PIN_RENEW_INTERVAL: Duration = Duration::from_secs(30);

/// Exact internal lease protecting one stream generation's replay start epoch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionReplayPin {
    id: Uuid,
    stream_generation: StreamGeneration,
    epoch: u64,
}

impl SubscriptionReplayPin {
    /// Stream generation protected by this pin.
    #[must_use]
    pub const fn stream_generation(&self) -> StreamGeneration {
        self.stream_generation
    }

    /// Oldest committed epoch required by this replay.
    #[must_use]
    pub const fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// Result of atomically attaching a replay pin to the shared retention floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionReplayPinAcquire {
    /// The requested epoch is now protected.
    Acquired(SubscriptionReplayPin),
    /// Cleanup authority had already made the requested epoch ineligible.
    Pruned {
        /// Epochs below this exclusive floor cannot acquire a new pin.
        artifact_before_epoch: u64,
    },
    /// The fixed shared pin roster is full after expired leases were removed.
    Capacity,
    /// A leader-fenced cleanup floor is being committed; the caller may retry.
    Contended,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayPinSlot {
    id: Uuid,
    stream_generation: StreamGeneration,
    epoch: u64,
    expires_at_ms: i64,
}

impl ReplayPinSlot {
    fn matches(&self, pin: &SubscriptionReplayPin) -> bool {
        self.id == pin.id
            && self.stream_generation == pin.stream_generation
            && self.epoch == pin.epoch
    }

    fn validate(&self) -> Result<(), LeaseError> {
        if self.id.is_nil()
            || self.stream_generation.digest().as_bytes() == &[0; 32]
            || self.epoch == 0
            || self.expires_at_ms <= 0
        {
            return Err(LeaseError::Invalid(
                "subscription replay pin is not canonical".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingCleanupFloor {
    id: Uuid,
    leader_proof: LeaderProof,
    selected_epoch: u64,
}

impl PendingCleanupFloor {
    fn validate(&self) -> Result<(), LeaseError> {
        if self.id.is_nil() || !self.leader_proof.is_canonical() || self.selected_epoch == 0 {
            return Err(LeaseError::Invalid(
                "pending subscription cleanup floor is not canonical".into(),
            ));
        }
        Ok(())
    }

    fn committed(&self) -> SubscriptionCleanupCommit {
        SubscriptionCleanupCommit {
            id: self.id,
            leader_proof: self.leader_proof.clone(),
            selected_epoch: self.selected_epoch,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SubscriptionCleanupCommit {
    id: Uuid,
    leader_proof: LeaderProof,
    selected_epoch: u64,
}

impl SubscriptionCleanupCommit {
    pub(super) fn validate(&self) -> Result<(), LeaseError> {
        if self.id.is_nil() || !self.leader_proof.is_canonical() || self.selected_epoch == 0 {
            return Err(LeaseError::Invalid(
                "subscription cleanup commit is not canonical".into(),
            ));
        }
        Ok(())
    }

    fn matches(&self, pending: &PendingCleanupFloor) -> bool {
        self.id == pending.id
            && self.leader_proof == pending.leader_proof
            && self.selected_epoch == pending.selected_epoch
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayPinRegistry {
    version: u16,
    revision: u64,
    artifact_before_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_cleanup: Option<PendingCleanupFloor>,
    pins: Vec<ReplayPinSlot>,
}

impl ReplayPinRegistry {
    fn empty() -> Self {
        Self {
            version: REGISTRY_VERSION,
            revision: 0,
            artifact_before_epoch: 0,
            pending_cleanup: None,
            pins: Vec::new(),
        }
    }

    fn validate(&self, persisted: bool) -> Result<(), LeaseError> {
        if self.version != REGISTRY_VERSION
            || (persisted && self.revision == 0)
            || self.pins.len() > MAX_REPLAY_PINS
            || self.pins.windows(2).any(|pair| pair[0].id >= pair[1].id)
        {
            return Err(LeaseError::Invalid(
                "subscription replay pin registry is not canonical".into(),
            ));
        }
        if let Some(pending) = &self.pending_cleanup {
            pending.validate()?;
            if pending.selected_epoch <= self.artifact_before_epoch
                || self
                    .pins
                    .iter()
                    .any(|pin| pin.epoch < pending.selected_epoch)
            {
                return Err(LeaseError::Invalid(
                    "pending subscription cleanup floor crosses committed state or a replay pin"
                        .into(),
                ));
            }
        }
        for pin in &self.pins {
            pin.validate()?;
            if pin.epoch < self.artifact_before_epoch {
                return Err(LeaseError::Invalid(
                    "subscription replay pin lies below the cleanup floor".into(),
                ));
            }
        }
        Ok(())
    }

    fn prune_expired(&mut self, now_ms: i64) -> bool {
        let before = self.pins.len();
        self.pins.retain(|pin| pin.expires_at_ms > now_ms);
        self.pins.len() != before
    }
}

struct VersionedRegistry {
    registry: ReplayPinRegistry,
    update_version: Option<UpdateVersion>,
}

struct RegistryMutation<T> {
    value: T,
    changed: bool,
}

impl<T> RegistryMutation<T> {
    const fn unchanged(value: T) -> Self {
        Self {
            value,
            changed: false,
        }
    }

    const fn changed(value: T) -> Self {
        Self {
            value,
            changed: true,
        }
    }
}

enum CleanupFloorPreparation {
    Committed(u64),
    Pending(PendingCleanupFloor),
}

impl LeaderLeaseStore {
    /// Acquire one bounded durable replay pin before loading an historical checkpoint.
    ///
    /// # Errors
    ///
    /// Returns an authority error when the identity is non-canonical or the durable
    /// replay-pin registry cannot be validated or updated.
    pub async fn acquire_subscription_replay_pin(
        &self,
        stream_generation: StreamGeneration,
        epoch: u64,
    ) -> Result<SubscriptionReplayPinAcquire, ClusterCheckpointAuthorityError> {
        if stream_generation.digest().as_bytes() == &[0; 32] || epoch == 0 {
            return Err(LeaseError::Invalid(
                "subscription replay pin identity is not canonical".into(),
            )
            .into());
        }
        let pin = SubscriptionReplayPin {
            id: Uuid::new_v4(),
            stream_generation,
            epoch,
        };
        self.mutate_subscription_replay_registry(|registry, now_ms| {
            if epoch < registry.artifact_before_epoch {
                return Ok(RegistryMutation::unchanged(
                    SubscriptionReplayPinAcquire::Pruned {
                        artifact_before_epoch: registry.artifact_before_epoch,
                    },
                ));
            }
            if registry
                .pending_cleanup
                .as_ref()
                .is_some_and(|pending| epoch < pending.selected_epoch)
            {
                return Ok(RegistryMutation::unchanged(
                    SubscriptionReplayPinAcquire::Contended,
                ));
            }
            if registry.pins.iter().any(|slot| slot.matches(&pin)) {
                return Ok(RegistryMutation::unchanged(
                    SubscriptionReplayPinAcquire::Acquired(pin.clone()),
                ));
            }
            if registry.pins.len() == MAX_REPLAY_PINS {
                return Ok(RegistryMutation::unchanged(
                    SubscriptionReplayPinAcquire::Capacity,
                ));
            }
            let expires_at_ms = replay_pin_expiry(now_ms)?;
            registry.pins.push(ReplayPinSlot {
                id: pin.id,
                stream_generation,
                epoch,
                expires_at_ms,
            });
            registry.pins.sort_unstable_by_key(|slot| slot.id);
            Ok(RegistryMutation::changed(
                SubscriptionReplayPinAcquire::Acquired(pin.clone()),
            ))
        })
        .await
        .map_err(Into::into)
    }

    /// Renew an exact replay pin. `false` means it expired or was fenced by cleanup.
    ///
    /// # Errors
    ///
    /// Returns an authority error when the durable replay-pin registry cannot be
    /// validated or updated.
    pub async fn renew_subscription_replay_pin(
        &self,
        pin: &SubscriptionReplayPin,
    ) -> Result<bool, ClusterCheckpointAuthorityError> {
        let pin = pin.clone();
        self.mutate_subscription_replay_registry(|registry, now_ms| {
            if pin.epoch < registry.artifact_before_epoch {
                registry.pins.retain(|slot| !slot.matches(&pin));
                return Ok(RegistryMutation::changed(false));
            }
            let Some(slot) = registry.pins.iter_mut().find(|slot| slot.matches(&pin)) else {
                return Ok(RegistryMutation::unchanged(false));
            };
            slot.expires_at_ms = replay_pin_expiry(now_ms)?;
            Ok(RegistryMutation::changed(true))
        })
        .await
        .map_err(Into::into)
    }

    /// Release an exact replay pin. Expired or already-released pins are idempotent.
    ///
    /// # Errors
    ///
    /// Returns an authority error when the durable replay-pin registry cannot be
    /// validated or updated.
    pub async fn release_subscription_replay_pin(
        &self,
        pin: &SubscriptionReplayPin,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let pin = pin.clone();
        self.mutate_subscription_replay_registry(|registry, _| {
            let before = registry.pins.len();
            registry.pins.retain(|slot| !slot.matches(&pin));
            Ok(if registry.pins.len() == before {
                RegistryMutation::unchanged(())
            } else {
                RegistryMutation::changed(())
            })
        })
        .await
        .map_err(Into::into)
    }

    /// Advance cleanup authority without crossing an unexpired replay pin.
    ///
    /// # Errors
    ///
    /// Returns an authority error when the leader proof is invalid or stale, or the
    /// durable replay-pin registry cannot be validated or updated.
    pub async fn reserve_subscription_cleanup_floor(
        &self,
        proof: &LeaderProof,
        requested_epoch: u64,
    ) -> Result<u64, ClusterCheckpointAuthorityError> {
        if !proof.is_canonical() || requested_epoch == 0 {
            return Err(LeaseError::Invalid(
                "subscription cleanup authority is not canonical".into(),
            )
            .into());
        }
        let preparation = self
            .prepare_subscription_cleanup_floor(proof, requested_epoch)
            .await?;
        let pending = match preparation {
            CleanupFloorPreparation::Committed(floor) => return Ok(floor),
            CleanupFloorPreparation::Pending(pending) => pending,
        };
        if let Err(error) = self
            .commit_subscription_cleanup_floor(proof, &pending)
            .await
        {
            // A pending slot is not a cleanup authority. Reconciliation removes it if this proof
            // lost the authority CAS, while preserving it if the marker became durable.
            let _ = self
                .mutate_subscription_replay_registry(|_, _| Ok(RegistryMutation::unchanged(())))
                .await;
            return Err(error);
        }
        self.mutate_subscription_replay_registry(|registry, _| {
            if registry.artifact_before_epoch < pending.selected_epoch {
                return Err(LeaseError::Invalid(
                    "committed subscription cleanup marker was not applied to its registry".into(),
                ));
            }
            Ok(RegistryMutation::unchanged(registry.artifact_before_epoch))
        })
        .await
        .map_err(Into::into)
    }

    async fn prepare_subscription_cleanup_floor(
        &self,
        proof: &LeaderProof,
        requested_epoch: u64,
    ) -> Result<CleanupFloorPreparation, ClusterCheckpointAuthorityError> {
        self.require_subscription_cleanup_leader(proof).await?;
        let proof = proof.clone();
        self.mutate_subscription_replay_registry(|registry, _| {
            if let Some(pending) = &registry.pending_cleanup {
                if pending.leader_proof != proof {
                    return Err(LeaseError::Fenced(
                        "another leader term owns the pending subscription cleanup floor".into(),
                    ));
                }
                return Ok(RegistryMutation::unchanged(
                    CleanupFloorPreparation::Pending(pending.clone()),
                ));
            }
            if registry.artifact_before_epoch >= requested_epoch {
                return Ok(RegistryMutation::unchanged(
                    CleanupFloorPreparation::Committed(registry.artifact_before_epoch),
                ));
            }
            let pending = {
                let pinned = registry.pins.iter().map(|pin| pin.epoch).min();
                let selected = pinned.map_or(requested_epoch, |epoch| requested_epoch.min(epoch));
                let selected = registry.artifact_before_epoch.max(selected);
                if selected == registry.artifact_before_epoch {
                    return Ok(RegistryMutation::unchanged(
                        CleanupFloorPreparation::Committed(selected),
                    ));
                }
                PendingCleanupFloor {
                    id: Uuid::new_v4(),
                    leader_proof: proof.clone(),
                    selected_epoch: selected,
                }
            };
            pending.validate()?;
            registry.pending_cleanup = Some(pending.clone());
            Ok(RegistryMutation::changed(CleanupFloorPreparation::Pending(
                pending,
            )))
        })
        .await
        .map_err(|error| match error {
            LeaseError::Fenced(_) => ClusterCheckpointAuthorityError::Fenced,
            error => error.into(),
        })
    }

    async fn commit_subscription_cleanup_floor(
        &self,
        proof: &LeaderProof,
        pending: &PendingCleanupFloor,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        pending.validate()?;
        if pending.leader_proof != *proof {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        for _ in 0..MAX_CAS_ATTEMPTS {
            let published = self
                .load_published_authority_head()
                .await?
                .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
            let current = &published.record;
            if current
                .subscription_cleanup_commit
                .as_ref()
                .is_some_and(|commit| commit.matches(pending))
            {
                return Ok(());
            }
            if !current.lease.matches_proof(proof) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let registry = read_registry(self.store.as_ref()).await?;
            if registry.registry.pending_cleanup.as_ref() != Some(pending) {
                return Err(ClusterCheckpointAuthorityError::Fenced);
            }
            let mut lease = current.lease.clone();
            lease.seq = lease
                .seq
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("leader authority sequence exhausted".into()))?;
            let mut next = current.preserve_with_lease(lease);
            next.subscription_cleanup_commit = Some(pending.committed());
            next.validate()?;
            match self
                .create_authority_record(Some(&published), &next)
                .await?
            {
                super::AuthorityCreateOutcome::Created
                | super::AuthorityCreateOutcome::ExistingIdentical => return Ok(()),
                super::AuthorityCreateOutcome::Contended(winner)
                    if winner
                        .subscription_cleanup_commit
                        .as_ref()
                        .is_some_and(|commit| commit.matches(pending)) =>
                {
                    return Ok(());
                }
                super::AuthorityCreateOutcome::Contended(winner)
                    if !winner.lease.matches_proof(proof) =>
                {
                    return Err(ClusterCheckpointAuthorityError::Fenced);
                }
                super::AuthorityCreateOutcome::Contended(_) => tokio::task::yield_now().await,
            }
        }
        Err(LeaseError::Io(format!(
            "subscription cleanup authority update exceeded {MAX_CAS_ATTEMPTS} attempts"
        ))
        .into())
    }

    async fn require_subscription_cleanup_leader(
        &self,
        proof: &LeaderProof,
    ) -> Result<(), ClusterCheckpointAuthorityError> {
        let current = self
            .load_record()
            .await?
            .ok_or(ClusterCheckpointAuthorityError::Fenced)?;
        if !current.lease.matches_proof(proof) {
            return Err(ClusterCheckpointAuthorityError::Fenced);
        }
        Ok(())
    }

    async fn mutate_subscription_replay_registry<T>(
        &self,
        mut mutation: impl FnMut(&mut ReplayPinRegistry, i64) -> Result<RegistryMutation<T>, LeaseError>,
    ) -> Result<T, LeaseError> {
        let mut last_error = None;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let mut versioned = read_registry(self.store.as_ref()).await?;
            let reconciled = self
                .reconcile_subscription_cleanup(&mut versioned.registry)
                .await?;
            let now_ms = now_millis();
            if now_ms == i64::MAX {
                return Err(LeaseError::Invalid(
                    "subscription replay pin clock is unavailable".into(),
                ));
            }
            let pruned = versioned.registry.prune_expired(now_ms);
            let result = mutation(&mut versioned.registry, now_ms)?;
            if !reconciled && !pruned && !result.changed {
                return Ok(result.value);
            }
            versioned.registry.revision = versioned
                .registry
                .revision
                .checked_add(1)
                .ok_or_else(|| LeaseError::Invalid("replay pin revision overflowed".into()))?;
            versioned.registry.validate(false)?;
            match write_registry(
                self.store.as_ref(),
                &versioned.registry,
                versioned.update_version.as_ref(),
            )
            .await
            {
                Ok(()) => return Ok(result.value),
                Err(error) => {
                    last_error = Some(error);
                    tokio::task::yield_now().await;
                }
            }
        }
        Err(LeaseError::Io(format!(
            "subscription replay pin update exceeded {MAX_CAS_ATTEMPTS} attempts: {}",
            last_error.map_or_else(|| "unknown conflict".into(), |error| error.to_string())
        )))
    }

    async fn reconcile_subscription_cleanup(
        &self,
        registry: &mut ReplayPinRegistry,
    ) -> Result<bool, LeaseError> {
        let Some(pending) = registry.pending_cleanup.clone() else {
            return Ok(false);
        };
        let current = self.load_record().await?.ok_or_else(|| {
            LeaseError::Invalid(
                "pending subscription cleanup floor lost the leader authority head".into(),
            )
        })?;
        if current
            .subscription_cleanup_commit
            .as_ref()
            .is_some_and(|commit| commit.matches(&pending))
        {
            registry.artifact_before_epoch = pending.selected_epoch;
            registry.pending_cleanup = None;
            return Ok(true);
        }
        if current.lease.matches_proof(&pending.leader_proof) {
            return Ok(false);
        }
        registry.pending_cleanup = None;
        Ok(true)
    }
}

fn replay_pin_expiry(now_ms: i64) -> Result<i64, LeaseError> {
    now_ms
        .checked_add(REPLAY_PIN_TTL_MS)
        .ok_or_else(|| LeaseError::Invalid("subscription replay pin expiry overflowed".into()))
}

async fn read_registry(
    store: &dyn object_store::ObjectStore,
) -> Result<VersionedRegistry, LeaseError> {
    let result = match store.get(&Path::from(REGISTRY_PATH)).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => {
            return Ok(VersionedRegistry {
                registry: ReplayPinRegistry::empty(),
                update_version: None,
            });
        }
        Err(error) => return Err(LeaseError::Io(error.to_string())),
    };
    if result.meta.size == 0 || result.meta.size > MAX_REGISTRY_BYTES {
        return Err(LeaseError::Invalid(format!(
            "subscription replay pin registry is {} bytes; maximum is {MAX_REGISTRY_BYTES}",
            result.meta.size
        )));
    }
    let update_version = UpdateVersion {
        e_tag: result.meta.e_tag.clone(),
        version: result.meta.version.clone(),
    };
    if update_version.e_tag.is_none() && update_version.version.is_none() {
        return Err(LeaseError::Invalid(
            "subscription replay pin store omitted its conditional update version".into(),
        ));
    }
    let bytes = result
        .bytes()
        .await
        .map_err(|error| LeaseError::Io(error.to_string()))?;
    let registry: ReplayPinRegistry = serde_json::from_slice(&bytes)?;
    registry.validate(true)?;
    if encode_registry(&registry)?.as_ref() != bytes.as_ref() {
        return Err(LeaseError::Invalid(
            "subscription replay pin registry does not use its canonical body".into(),
        ));
    }
    Ok(VersionedRegistry {
        registry,
        update_version: Some(update_version),
    })
}

async fn write_registry(
    store: &dyn object_store::ObjectStore,
    registry: &ReplayPinRegistry,
    expected: Option<&UpdateVersion>,
) -> Result<(), LeaseError> {
    let mode = expected.map_or(PutMode::Create, |version| PutMode::Update(version.clone()));
    store
        .put_opts(
            &Path::from(REGISTRY_PATH),
            PutPayload::from(encode_registry(registry)?),
            PutOptions {
                mode,
                ..PutOptions::default()
            },
        )
        .await
        .map(|_| ())
        .map_err(|error| LeaseError::Io(error.to_string()))
}

fn encode_registry(registry: &ReplayPinRegistry) -> Result<Bytes, LeaseError> {
    registry.validate(false)?;
    let bytes = serde_json::to_vec(registry)?;
    let length = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    if bytes.is_empty() || length > MAX_REGISTRY_BYTES {
        return Err(LeaseError::Invalid(format!(
            "encoded subscription replay pin registry is {length} bytes; maximum is {MAX_REGISTRY_BYTES}"
        )));
    }
    Ok(Bytes::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::SubscriptionDigest;
    use crate::cluster::control::{LeaderLeaseOwner, LeaseOutcome};
    use crate::cluster::discovery::NodeId;
    use std::time::Instant;

    fn generation(byte: u8) -> StreamGeneration {
        StreamGeneration::from_digest(SubscriptionDigest::from_bytes([byte; 32]))
    }

    fn owner(node: u64) -> LeaderLeaseOwner {
        LeaderLeaseOwner {
            node: NodeId(node),
            boot: Uuid::from_u128(u128::from(node)),
            process_term: 1,
        }
    }

    async fn takeover(
        authority: &LeaderLeaseStore,
        lease: &super::super::LeaderLease,
        successor: &LeaderLeaseOwner,
    ) -> super::super::LeaderLease {
        let observation = super::super::LeaderLeaseObservation {
            lease: lease.clone(),
            started: Instant::now()
                .checked_sub(Duration::from_millis(2))
                .unwrap(),
        };
        let LeaseOutcome::Acquired(takeover) = authority
            .try_takeover(successor, &observation, 2)
            .await
            .unwrap()
        else {
            panic!("elapsed rival observation must permit takeover");
        };
        takeover
    }

    #[tokio::test]
    async fn active_pin_serializes_with_cleanup_floor() {
        let authority =
            LeaderLeaseStore::new(Arc::new(object_store::memory::InMemory::new()), 30_000);
        let owner = owner(1);
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            panic!("empty authority must grant the leader term");
        };
        let SubscriptionReplayPinAcquire::Acquired(pin) = authority
            .acquire_subscription_replay_pin(generation(1), 5)
            .await
            .unwrap()
        else {
            panic!("empty replay registry must accept a pin");
        };

        assert_eq!(
            authority
                .reserve_subscription_cleanup_floor(&lease.proof(), 9)
                .await
                .unwrap(),
            5
        );
        authority
            .release_subscription_replay_pin(&pin)
            .await
            .unwrap();
        assert_eq!(
            authority
                .reserve_subscription_cleanup_floor(&lease.proof(), 9)
                .await
                .unwrap(),
            9
        );
        assert!(matches!(
            authority
                .acquire_subscription_replay_pin(generation(2), 8)
                .await
                .unwrap(),
            SubscriptionReplayPinAcquire::Pruned {
                artifact_before_epoch: 9
            }
        ));
    }

    #[tokio::test]
    async fn concurrent_gateways_preserve_both_pins() {
        let authority = Arc::new(LeaderLeaseStore::new(
            Arc::new(object_store::memory::InMemory::new()),
            30_000,
        ));
        let (left, right) = tokio::join!(
            authority.acquire_subscription_replay_pin(generation(1), 4),
            authority.acquire_subscription_replay_pin(generation(2), 7),
        );
        assert!(matches!(
            left.unwrap(),
            SubscriptionReplayPinAcquire::Acquired(_)
        ));
        assert!(matches!(
            right.unwrap(),
            SubscriptionReplayPinAcquire::Acquired(_)
        ));

        let registry = read_registry(authority.store.as_ref()).await.unwrap();
        assert_eq!(registry.registry.pins.len(), 2);
        assert_eq!(
            registry.registry.pins.iter().map(|pin| pin.epoch).min(),
            Some(4)
        );
    }

    #[test]
    fn legacy_registry_without_a_pending_floor_round_trips_canonically() {
        let legacy = Bytes::from_static(
            br#"{"version":1,"revision":1,"artifact_before_epoch":0,"pins":[]}"#,
        );
        let registry: ReplayPinRegistry = serde_json::from_slice(&legacy).unwrap();
        assert!(registry.pending_cleanup.is_none());
        assert_eq!(encode_registry(&registry).unwrap(), legacy);
    }

    #[tokio::test]
    async fn takeover_before_cleanup_marker_discards_the_uncommitted_floor() {
        let authority = LeaderLeaseStore::new(Arc::new(object_store::memory::InMemory::new()), 1);
        let incumbent = owner(1);
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must grant the leader term");
        };
        let CleanupFloorPreparation::Pending(pending) = authority
            .prepare_subscription_cleanup_floor(&lease.proof(), 9)
            .await
            .unwrap()
        else {
            panic!("a new cleanup floor must prepare a pending slot");
        };

        let successor = owner(2);
        takeover(&authority, &lease, &successor).await;
        assert!(matches!(
            authority
                .commit_subscription_cleanup_floor(&lease.proof(), &pending)
                .await,
            Err(ClusterCheckpointAuthorityError::Fenced)
        ));
        assert!(matches!(
            authority
                .acquire_subscription_replay_pin(generation(2), 8)
                .await
                .unwrap(),
            SubscriptionReplayPinAcquire::Acquired(_)
        ));
        let registry = read_registry(authority.store.as_ref()).await.unwrap();
        assert_eq!(registry.registry.artifact_before_epoch, 0);
        assert!(registry.registry.pending_cleanup.is_none());
    }

    #[tokio::test]
    async fn committed_cleanup_marker_survives_takeover_and_is_helped_to_completion() {
        let authority = LeaderLeaseStore::new(Arc::new(object_store::memory::InMemory::new()), 1);
        let incumbent = owner(1);
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&incumbent, 0).await.unwrap()
        else {
            panic!("empty authority must grant the leader term");
        };
        let CleanupFloorPreparation::Pending(pending) = authority
            .prepare_subscription_cleanup_floor(&lease.proof(), 9)
            .await
            .unwrap()
        else {
            panic!("a new cleanup floor must prepare a pending slot");
        };
        assert_eq!(
            authority
                .acquire_subscription_replay_pin(generation(2), 8)
                .await
                .unwrap(),
            SubscriptionReplayPinAcquire::Contended
        );
        authority
            .commit_subscription_cleanup_floor(&lease.proof(), &pending)
            .await
            .unwrap();

        let successor = owner(2);
        takeover(&authority, &lease, &successor).await;
        assert!(matches!(
            authority
                .acquire_subscription_replay_pin(generation(3), 8)
                .await
                .unwrap(),
            SubscriptionReplayPinAcquire::Pruned {
                artifact_before_epoch: 9
            }
        ));
        let registry = read_registry(authority.store.as_ref()).await.unwrap();
        assert_eq!(registry.registry.artifact_before_epoch, 9);
        assert!(registry.registry.pending_cleanup.is_none());
    }
}
