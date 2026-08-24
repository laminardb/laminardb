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
struct ReplayPinRegistry {
    version: u16,
    revision: u64,
    artifact_before_epoch: u64,
    pins: Vec<ReplayPinSlot>,
}

impl ReplayPinRegistry {
    fn empty() -> Self {
        Self {
            version: REGISTRY_VERSION,
            revision: 0,
            artifact_before_epoch: 0,
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
        self.require_subscription_cleanup_leader(proof).await?;
        let floor = self
            .mutate_subscription_replay_registry(|registry, _| {
                let pinned = registry.pins.iter().map(|pin| pin.epoch).min();
                let selected = pinned.map_or(requested_epoch, |epoch| requested_epoch.min(epoch));
                let selected = registry.artifact_before_epoch.max(selected);
                if selected == registry.artifact_before_epoch {
                    return Ok(RegistryMutation::unchanged(selected));
                }
                registry.artifact_before_epoch = selected;
                Ok(RegistryMutation::changed(selected))
            })
            .await?;
        self.require_subscription_cleanup_leader(proof).await?;
        Ok(floor)
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
            let now_ms = now_millis();
            if now_ms == i64::MAX {
                return Err(LeaseError::Invalid(
                    "subscription replay pin clock is unavailable".into(),
                ));
            }
            let pruned = versioned.registry.prune_expired(now_ms);
            let result = mutation(&mut versioned.registry, now_ms)?;
            if !pruned && !result.changed {
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

    fn generation(byte: u8) -> StreamGeneration {
        StreamGeneration::from_digest(SubscriptionDigest::from_bytes([byte; 32]))
    }

    #[tokio::test]
    async fn active_pin_serializes_with_cleanup_floor() {
        let authority =
            LeaderLeaseStore::new(Arc::new(object_store::memory::InMemory::new()), 30_000);
        let owner = LeaderLeaseOwner {
            node: NodeId(1),
            boot: Uuid::from_u128(1),
            process_term: 1,
        };
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
}
