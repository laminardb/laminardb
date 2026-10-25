//! Cluster cleanup honours durable replay pins, not this process's read pins. Each node holds
//! one durable pin at its read floor from startup, so a committed read still pins in process,
//! and cleanup keeps every cut a read on any node may hold. Reads stop if the pin lapses.

use std::sync::Arc;
use std::time::{Duration, Instant};

use laminar_core::checkpoint::{CommittedCheckpointRef, StreamGeneration, SubscriptionDigest};
use laminar_core::cluster::control::{
    LeaderLeaseStore, SubscriptionReplayPin, SubscriptionReplayPinAcquire,
    SUBSCRIPTION_REPLAY_PIN_RENEW_INTERVAL,
};

use super::committed_reads::{CommittedCuts, ReadPins};
use crate::error::DbError;

/// How often the node pin follows the oldest cut this process's reads hold.
const ADVANCE_INTERVAL: Duration = Duration::from_secs(5);
/// Unrenewed for this long, the durable pin may have expired, so reads stop.
const LAPSE: Duration = Duration::from_secs(90);
/// Attempts at the first pin while cleanup moves its floor.
const ESTABLISH_ATTEMPTS: u32 = 8;

/// Pin the cluster's cleanup floor for this node's reads, then hold the pin until the
/// coordinator publishing `cuts` is dropped.
pub(crate) async fn start_cluster_read_pin(
    pins: Arc<ReadPins>,
    mut cuts: CommittedCuts,
    authority: Arc<LeaderLeaseStore>,
    participant_id: u64,
) -> Result<(), DbError> {
    pins.close();
    let generation = StreamGeneration::from_digest(SubscriptionDigest::for_bytes(
        b"laminardb-view-read-pin-v1",
        &participant_id.to_le_bytes(),
    ));
    let mut node = NodePin {
        pins,
        authority,
        generation,
        held: None,
        renewed: Instant::now(),
    };
    node.establish().await?;
    tokio::spawn(async move {
        let mut advance = tokio::time::interval(ADVANCE_INTERVAL);
        loop {
            let tick = tokio::select! {
                changed = cuts.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    false
                }
                _ = advance.tick() => true,
            };
            let Some(latest) = cuts.borrow().as_ref().map(|cut| cut.reference.clone()) else {
                continue;
            };
            if tick || node.held.is_none() {
                node.follow(latest).await;
            }
        }
        node.release().await;
    });
    Ok(())
}

struct NodePin {
    pins: Arc<ReadPins>,
    authority: Arc<LeaderLeaseStore>,
    generation: StreamGeneration,
    held: Option<SubscriptionReplayPin>,
    renewed: Instant,
}

impl NodePin {
    /// Pin the oldest epoch cleanup still keeps, which no cut a read can resolve precedes.
    async fn establish(&mut self) -> Result<(), DbError> {
        let mut epoch = 1;
        for _ in 0..ESTABLISH_ATTEMPTS {
            let acquired = self
                .authority
                .acquire_subscription_replay_pin(self.generation, epoch)
                .await
                .map_err(|error| DbError::Checkpoint(format!("committed-read pin: {error}")))?;
            match acquired {
                SubscriptionReplayPinAcquire::Acquired(pin) => {
                    self.pins.open_at(pin.epoch());
                    self.hold(pin);
                    return Ok(());
                }
                SubscriptionReplayPinAcquire::Pruned {
                    artifact_before_epoch,
                } => epoch = artifact_before_epoch.max(1),
                SubscriptionReplayPinAcquire::Contended => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                SubscriptionReplayPinAcquire::Capacity => {
                    return Err(DbError::Checkpoint(
                        "committed-read pin: the cluster replay pin roster is full".into(),
                    ));
                }
            }
        }
        Err(DbError::Checkpoint(
            "committed-read pin: cleanup kept moving past every epoch it was asked to keep".into(),
        ))
    }

    /// Move the durable pin to the oldest cut a local read holds, or to `latest`, and keep it
    /// renewed. The local floor rises before the new pin exists; the old pin, at or below it,
    /// protects the gap until the new one replaces it.
    async fn follow(&mut self, latest: CommittedCheckpointRef) {
        let Some(held) = self.held.clone() else {
            if let Some(pin) = self.acquire(latest.epoch).await {
                self.pins.open_at(pin.epoch());
                self.hold(pin);
            }
            return;
        };
        let floor = self.pins.reserve_floor(latest).epoch;
        if floor > held.epoch() {
            if let Some(pin) = self.acquire(floor).await {
                self.hold(pin);
                self.release_pin(&held).await;
                return;
            }
        }
        if self.renewed.elapsed() < SUBSCRIPTION_REPLAY_PIN_RENEW_INTERVAL {
            return;
        }
        match self.authority.renew_subscription_replay_pin(&held).await {
            Ok(true) => self.renewed = Instant::now(),
            Ok(false) => self.lose("expired"),
            Err(error) if self.renewed.elapsed() >= LAPSE => {
                self.lose(&format!("unrenewed: {error}"));
            }
            Err(error) => tracing::warn!(%error, "renewing the committed-read pin failed"),
        }
    }

    async fn acquire(&self, epoch: u64) -> Option<SubscriptionReplayPin> {
        match self
            .authority
            .acquire_subscription_replay_pin(self.generation, epoch)
            .await
        {
            Ok(SubscriptionReplayPinAcquire::Acquired(pin)) => Some(pin),
            Ok(SubscriptionReplayPinAcquire::Contended) => None,
            Ok(outcome) => {
                tracing::warn!(epoch, ?outcome, "committed-read pin was not acquired");
                None
            }
            Err(error) => {
                tracing::warn!(epoch, %error, "acquiring the committed-read pin failed");
                None
            }
        }
    }

    fn hold(&mut self, pin: SubscriptionReplayPin) {
        self.held = Some(pin);
        self.renewed = Instant::now();
    }

    /// Stop reads until a new pin protects them; a read in flight may lose its objects.
    fn lose(&mut self, reason: &str) {
        tracing::warn!(
            reason,
            "the committed-read pin lapsed; committed reads pause"
        );
        self.pins.close();
        self.held = None;
    }

    async fn release_pin(&self, pin: &SubscriptionReplayPin) {
        if let Err(error) = self.authority.release_subscription_replay_pin(pin).await {
            tracing::warn!(%error, "releasing a committed-read pin failed; it expires instead");
        }
    }

    async fn release(&mut self) {
        if let Some(pin) = self.held.take() {
            self.release_pin(&pin).await;
        }
    }
}
