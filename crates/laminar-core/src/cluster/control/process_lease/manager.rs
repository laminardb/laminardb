//! Renewal-loop ownership and terminal lease-loss publication.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use uuid::Uuid;

use super::super::lease_deadline::LeaseDeadline;
use super::{now_millis, ProcessLease, ProcessLeaseError, ProcessLeaseOutcome, ProcessLeaseStore};

/// Internal renewal timings for the stable-node lease.
#[derive(Debug, Clone, Copy)]
pub struct ProcessLeaseConfig {
    /// Lease lifetime.
    pub ttl: Duration,
    /// Renewal cadence.
    pub renew_interval: Duration,
}

impl Default for ProcessLeaseConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(15),
            renew_interval: Duration::from_secs(5),
        }
    }
}

/// Renews an already-acquired process lease and publishes terminal lease loss.
pub struct ProcessLeaseManager {
    store: Arc<ProcessLeaseStore>,
    owner: Uuid,
    config: ProcessLeaseConfig,
    initial_valid_until: std::time::Instant,
    live_tx: watch::Sender<bool>,
    deadline: Arc<LeaseDeadline>,
}

impl ProcessLeaseManager {
    /// Construct a renewal manager for an acquired lease.
    ///
    /// # Errors
    /// Rejects a lease that does not match the store namespace or owner.
    pub fn new(
        store: Arc<ProcessLeaseStore>,
        owner: Uuid,
        config: ProcessLeaseConfig,
        acquisition_started_at: Instant,
        initial: &ProcessLease,
    ) -> Result<Self, ProcessLeaseError> {
        initial.validate(store.node)?;
        let store_ttl = u64::try_from(store.ttl_ms)
            .ok()
            .filter(|ttl| *ttl > 0)
            .map(Duration::from_millis)
            .ok_or_else(|| {
                ProcessLeaseError::Invalid("process lease TTL must be positive".into())
            })?;
        if initial.owner != owner
            || config.ttl.is_zero()
            || config.ttl != store_ttl
            || config.renew_interval.is_zero()
            || config.renew_interval >= config.ttl
        {
            return Err(ProcessLeaseError::Invalid(
                "renewal manager requires this boot's lease, the exact store TTL, and a renewal interval below TTL".into(),
            ));
        }
        let (live_tx, _live_rx) = watch::channel(true);
        let ttl = config.ttl;
        let now = Instant::now();
        if acquisition_started_at > now {
            return Err(ProcessLeaseError::Invalid(
                "process lease acquisition start is in the future".into(),
            ));
        }
        let initial_valid_until = acquisition_started_at
            .checked_add(ttl)
            .ok_or_else(|| ProcessLeaseError::Invalid("process lease TTL overflows time".into()))?;
        if now >= initial_valid_until {
            return Err(ProcessLeaseError::Invalid(
                "process lease acquisition response arrived after its local deadline".into(),
            ));
        }
        let deadline = Arc::new(LeaseDeadline::uninitialized());
        deadline.extend_until(initial_valid_until);
        if !deadline.is_live() {
            return Err(ProcessLeaseError::Invalid(
                "process lease acquisition response arrived after its local deadline".into(),
            ));
        }
        Ok(Self {
            store,
            owner,
            config,
            initial_valid_until,
            live_tx,
            deadline,
        })
    }

    /// Watch terminal ownership status.
    #[must_use]
    pub fn live_watch(&self) -> watch::Receiver<bool> {
        self.live_tx.subscribe()
    }

    /// Shared monotonic deadline for hot-path fencing.
    #[must_use]
    pub fn deadline(&self) -> Arc<LeaseDeadline> {
        Arc::clone(&self.deadline)
    }

    /// Spawn the renewal loop. Once ownership is uncertain past its expiry or a rival is observed,
    /// the watch becomes false and this manager never reacquires.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut valid_until = self.initial_valid_until;
            let mut ticker = tokio::time::interval(self.config.renew_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            // The acquisition itself is the first successful tick.
            ticker.tick().await;
            loop {
                let now = std::time::Instant::now();
                if now >= valid_until {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                }
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => {
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    },
                    () = tokio::time::sleep_until(tokio::time::Instant::from_std(valid_until)) => {
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    }
                    _ = ticker.tick() => {}
                }

                let now = std::time::Instant::now();
                if now >= valid_until {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                }
                let attempt_started_at = Instant::now();
                let Some(attempt_valid_until) = attempt_started_at.checked_add(self.config.ttl)
                else {
                    self.deadline.fence();
                    self.live_tx.send_replace(false);
                    return;
                };
                let renewal = tokio::time::timeout_at(
                    tokio::time::Instant::from_std(valid_until),
                    self.store.try_acquire(self.owner, now_millis()),
                )
                .await;
                match renewal {
                    Ok(Ok(ProcessLeaseOutcome::Acquired(_))) => {
                        let response_at = Instant::now();
                        if response_at >= valid_until || response_at >= attempt_valid_until {
                            self.deadline.fence();
                            self.live_tx.send_replace(false);
                            return;
                        }
                        valid_until = attempt_valid_until;
                        self.deadline.extend_until(attempt_valid_until);
                    }
                    Ok(Ok(ProcessLeaseOutcome::Held(rival))) => {
                        tracing::error!(
                            node = self.store.node.0,
                            owner = %rival.owner,
                            term = rival.term,
                            "stable node identity lease was lost"
                        );
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    }
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "stable node identity lease renewal failed");
                    }
                    Err(_) => {
                        self.deadline.fence();
                        self.live_tx.send_replace(false);
                        return;
                    }
                }
            }
        })
    }
}
