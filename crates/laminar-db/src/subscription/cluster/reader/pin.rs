//! Durable replay-pin lifecycle for one gateway reader.

use std::time::Instant;

use laminar_core::checkpoint::{OutputDistributionCertificate, StreamGeneration};
use laminar_core::cluster::control::{
    LeaderLeaseStore, SubscriptionReplayPin, SubscriptionReplayPinAcquire,
    SUBSCRIPTION_REPLAY_PIN_RENEW_INTERVAL,
};

use super::authority::map_authority_error;
use super::GATEWAY_IO_TIMEOUT;
use crate::error::DbError;
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};

pub(super) struct GatewayReplayPin {
    pub(super) pin: SubscriptionReplayPin,
    renewed_at: Instant,
}

impl GatewayReplayPin {
    pub(super) fn new(pin: SubscriptionReplayPin) -> Self {
        Self {
            pin,
            renewed_at: Instant::now(),
        }
    }
}

pub(super) async fn acquire_replay_pin(
    authority: &LeaderLeaseStore,
    certificate: &OutputDistributionCertificate,
    start: SubscribeStart,
) -> Result<Option<SubscriptionReplayPin>, DbError> {
    let SubscribeStart::AsOfEpoch(requested) = start else {
        return Ok(None);
    };
    let acquired = tokio::time::timeout(
        GATEWAY_IO_TIMEOUT,
        authority.acquire_subscription_replay_pin(certificate.stream_generation, requested),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
    .map_err(map_authority_error)?;
    match acquired {
        SubscriptionReplayPinAcquire::Acquired(pin) => Ok(Some(pin)),
        SubscriptionReplayPinAcquire::Pruned { .. } => {
            Err(ClusterSubscriptionError::ReplayPruned { requested }.into())
        }
        SubscriptionReplayPinAcquire::Capacity | SubscriptionReplayPinAcquire::Contended => {
            Err(ClusterSubscriptionError::BackendUnavailable.into())
        }
    }
}

pub(super) async fn renew_replay_pin(
    authority: &LeaderLeaseStore,
    replay_pin: &mut Option<GatewayReplayPin>,
) -> Result<(), ClusterSubscriptionError> {
    let Some(pin) = replay_pin.as_mut() else {
        return Ok(());
    };
    if pin.renewed_at.elapsed() < SUBSCRIPTION_REPLAY_PIN_RENEW_INTERVAL {
        return Ok(());
    }
    let renewed = tokio::time::timeout(
        GATEWAY_IO_TIMEOUT,
        authority.renew_subscription_replay_pin(&pin.pin),
    )
    .await
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?
    .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?;
    if !renewed {
        return Err(ClusterSubscriptionError::RetentionLost);
    }
    pin.renewed_at = Instant::now();
    Ok(())
}

pub(super) async fn finish_initial_replay(
    authority: &LeaderLeaseStore,
    replay_pin: &mut Option<GatewayReplayPin>,
    generation: StreamGeneration,
) -> Result<(), ClusterSubscriptionError> {
    let Some(pin) = replay_pin.take() else {
        return Ok(());
    };
    let epoch = pin.pin.epoch();
    let released = tokio::time::timeout(
        GATEWAY_IO_TIMEOUT,
        authority.release_subscription_replay_pin(&pin.pin),
    )
    .await;
    if let Ok(Ok(())) = released {
        tracing::info!(
            stream_generation = %generation,
            replay_start_epoch = epoch,
            "completed committed cluster subscription replay"
        );
        return Ok(());
    }
    *replay_pin = Some(pin);
    Err(ClusterSubscriptionError::BackendUnavailable)
}

pub(super) async fn release_replay_pin(
    authority: &LeaderLeaseStore,
    pin: Option<&SubscriptionReplayPin>,
) {
    let Some(pin) = pin else {
        return;
    };
    let result = tokio::time::timeout(
        GATEWAY_IO_TIMEOUT,
        authority.release_subscription_replay_pin(pin),
    )
    .await;
    if !matches!(result, Ok(Ok(()))) {
        tracing::warn!(
            stream_generation = %pin.stream_generation(),
            replay_start_epoch = pin.epoch(),
            "subscription replay pin release deferred to lease expiry"
        );
    }
}
