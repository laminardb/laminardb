//! Process-local renewal loop that converges the durable leader lease to gossip candidacy.

use super::*;

/// Acquires and renews leadership while candidacy remains true.
pub struct LeaderLeaseManager {
    store: Arc<LeaderLeaseStore>,
    owner: LeaderLeaseOwner,
    config: LeaderLeaseConfig,
    lease_tx: watch::Sender<Option<LeaderLease>>,
    deadline_tx: watch::Sender<Arc<LeaseDeadline>>,
}

#[cfg(feature = "cluster")]
enum LeaseOperationEvent {
    Shutdown,
    Candidacy(Result<(), watch::error::RecvError>),
    Deadline,
    Completed {
        result: Result<LeaseOutcome, LeaseError>,
        valid_until: tokio::time::Instant,
    },
}

#[cfg(feature = "cluster")]
struct LeaderLeaseExitGuard {
    shutdown: tokio_util::sync::CancellationToken,
    unexpected_exit: tokio_util::sync::CancellationToken,
    lease_tx: watch::Sender<Option<LeaderLease>>,
    deadline_tx: watch::Sender<Arc<LeaseDeadline>>,
}

#[cfg(feature = "cluster")]
impl Drop for LeaderLeaseExitGuard {
    fn drop(&mut self) {
        self.deadline_tx.borrow().fence();
        self.lease_tx.send_replace(None);
        if !self.shutdown.is_cancelled() {
            tracing::error!(
                "leader lease manager exited without intentional shutdown; fencing process authority"
            );
            self.unexpected_exit.cancel();
        }
    }
}

#[cfg(feature = "cluster")]
async fn wait_for_deadline(deadline: Option<tokio::time::Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        std::future::pending::<()>().await;
    }
}

impl std::fmt::Debug for LeaderLeaseManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaderLeaseManager")
            .field("owner", &self.owner)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LeaderLeaseManager {
    /// Construct a manager bound to this boot's acquired stable-node process lease.
    ///
    /// # Errors
    /// Rejects invalid ownership or inconsistent renewal timings.
    pub fn new(
        store: Arc<LeaderLeaseStore>,
        process_lease: &ProcessLease,
        config: LeaderLeaseConfig,
    ) -> Result<Self, LeaseError> {
        let owner = LeaderLeaseOwner::from_process_lease(process_lease)?;
        let ttl_ms = i64::try_from(config.ttl.as_millis())
            .map_err(|_| LeaseError::Invalid("lease TTL exceeds diagnostic range".into()))?;
        let exact_ttl = u64::try_from(ttl_ms)
            .ok()
            .map(Duration::from_millis)
            .is_some_and(|ttl| ttl == config.ttl);
        if ttl_ms <= 0
            || !exact_ttl
            || config.renew_interval.is_zero()
            || config.renew_interval >= config.ttl
            || ttl_ms != store.ttl_ms
        {
            return Err(LeaseError::Invalid(
                "manager requires a renewal interval below the store's matching TTL".into(),
            ));
        }
        let (lease_tx, _lease_rx) = watch::channel(None);
        let (deadline_tx, _deadline_rx) = watch::channel(Arc::new(LeaseDeadline::uninitialized()));
        Ok(Self {
            store,
            owner,
            config,
            lease_tx,
            deadline_tx,
        })
    }

    /// Exact process incarnation this manager may publish.
    #[must_use]
    pub fn owner(&self) -> &LeaderLeaseOwner {
        &self.owner
    }

    /// Subscribe to the locally authorized leader record.
    #[must_use]
    pub fn lease_watch(&self) -> watch::Receiver<Option<LeaderLease>> {
        self.lease_tx.subscribe()
    }

    /// Snapshot the current local-monotonic deadline generation.
    ///
    /// This is useful before the manager starts and in fixed-generation tests. A running manager
    /// may replace it after any discontinuous grant; runtime consumers must subscribe through
    /// [`Self::deadline_watch`].
    #[must_use]
    pub fn deadline(&self) -> Arc<LeaseDeadline> {
        self.deadline_tx.borrow().clone()
    }

    /// Subscribe to generation-scoped local leader deadlines.
    ///
    /// A discontinuous grant permanently fences the previous deadline and publishes a fresh
    /// inactive one before another durable fencing term can become locally authoritative.
    #[must_use]
    pub fn deadline_watch(&self) -> watch::Receiver<Arc<LeaseDeadline>> {
        self.deadline_tx.subscribe()
    }

    #[cfg(feature = "cluster")]
    pub(super) fn withdraw(&self) -> bool {
        let deadline = self.deadline_tx.borrow().clone();
        let had_grant = self.lease_tx.borrow().is_some() || deadline.is_live();
        // Always rotate the generation. An acquisition may have crossed its deadline after the
        // durable response but before local publication, leaving an expired, never-published
        // deadline that `is_live()` cannot distinguish from a fresh inactive one.
        deadline.fence();
        self.lease_tx.send_replace(None);
        self.deadline_tx
            .send_replace(Arc::new(LeaseDeadline::uninitialized()));
        had_grant
    }

    #[cfg(feature = "cluster")]
    fn fence(&self) {
        self.deadline_tx.borrow().fence();
        self.lease_tx.send_replace(None);
    }

    #[cfg(feature = "cluster")]
    fn withdraw_for_new_term(
        &self,
        ticker: &mut tokio::time::Interval,
        valid_until: &mut Option<tokio::time::Instant>,
        observation: &mut Option<LeaderLeaseObservation>,
        held_token: &mut Option<u64>,
        rotate_not_before: &mut Option<tokio::time::Instant>,
    ) {
        // Permanently fence the process-local grant before forgetting its fencing token. A later
        // acquisition runs through `begin_new_term` and a fresh deadline generation, so neither
        // half of the expired proof can become live again.
        let had_grant = self.withdraw();
        *valid_until = None;
        *observation = None;
        *held_token = None;
        let now = tokio::time::Instant::now();
        if had_grant {
            let retry_at = now
                .checked_add(self.config.ttl)
                .unwrap_or_else(tokio::time::Instant::now);
            // An old-proof checkpoint tail is still allowed to win the authority CAS while no
            // newer term exists. Preserve one full TTL for that bounded settlement before the
            // same process rotates the durable token.
            if rotate_not_before.is_none_or(|current| retry_at > current) {
                *rotate_not_before = Some(retry_at);
            }
        }
        if let Some(retry_at) = *rotate_not_before {
            if retry_at > now {
                ticker.reset_at(retry_at);
            } else {
                *rotate_not_before = None;
                ticker.reset_immediately();
            }
        } else {
            ticker.reset_immediately();
        }
    }

    #[cfg(feature = "cluster")]
    async fn attempt_lease(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        candidate: &mut watch::Receiver<LeaderCandidacy>,
        valid_until: Option<tokio::time::Instant>,
        observation: Option<&LeaderLeaseObservation>,
        held_token: Option<u64>,
    ) -> LeaseOperationEvent {
        let Some(attempt_valid_until) = tokio::time::Instant::now().checked_add(self.config.ttl)
        else {
            return LeaseOperationEvent::Deadline;
        };
        let operation_deadline = valid_until.map_or(attempt_valid_until, |current| {
            current.min(attempt_valid_until)
        });
        let operation = async {
            if let Some(observation) = observation {
                Box::pin(
                    self.store
                        .try_takeover(&self.owner, observation, now_millis()),
                )
                .await
            } else if let Some(token) = held_token {
                Box::pin(self.store.renew_exact(&self.owner, token, now_millis())).await
            } else {
                Box::pin(self.store.begin_new_term(&self.owner, now_millis())).await
            }
        };
        tokio::select! {
            biased;
            () = shutdown.cancelled() => LeaseOperationEvent::Shutdown,
            changed = candidate.changed() => LeaseOperationEvent::Candidacy(changed),
            () = wait_for_deadline(Some(operation_deadline)) => LeaseOperationEvent::Deadline,
            result = operation => LeaseOperationEvent::Completed {
                result,
                valid_until: attempt_valid_until,
            },
        }
    }

    #[cfg(feature = "cluster")]
    async fn wait_for_candidacy_change(
        &self,
        shutdown: &tokio_util::sync::CancellationToken,
        candidate: &mut watch::Receiver<LeaderCandidacy>,
    ) -> bool {
        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                self.fence();
                false
            }
            changed = candidate.changed() => {
                if changed.is_err() {
                    self.fence();
                    return false;
                }
                true
            }
        }
    }

    #[cfg(feature = "cluster")]
    async fn run(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        mut candidate: watch::Receiver<LeaderCandidacy>,
    ) {
        let mut ticker = tokio::time::interval(self.config.renew_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut valid_until: Option<tokio::time::Instant> = None;
        let mut observation: Option<LeaderLeaseObservation> = None;
        // A newly constructed manager and every locally withdrawn grant need a new durable
        // fencing token. Only uninterrupted renewals may preserve the current token.
        let mut held_token = None;
        let mut rotate_not_before = None;
        let mut candidacy_generation = candidate.borrow().generation;

        loop {
            let candidacy = *candidate.borrow_and_update();
            if candidacy.generation != candidacy_generation {
                self.withdraw_for_new_term(
                    &mut ticker,
                    &mut valid_until,
                    &mut observation,
                    &mut held_token,
                    &mut rotate_not_before,
                );
                candidacy_generation = candidacy.generation;
            }
            if !candidacy.eligible {
                self.withdraw_for_new_term(
                    &mut ticker,
                    &mut valid_until,
                    &mut observation,
                    &mut held_token,
                    &mut rotate_not_before,
                );
                if !self
                    .wait_for_candidacy_change(&shutdown, &mut candidate)
                    .await
                {
                    return;
                }
                continue;
            }

            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    self.fence();
                    return;
                }
                changed = candidate.changed() => {
                    if changed.is_err() {
                        self.fence();
                        return;
                    }
                    continue;
                }
                () = wait_for_deadline(valid_until) => {
                    tracing::warn!(
                        owner = ?self.owner,
                        "leader lease local deadline expired; withdrawing and rotating the fencing token"
                    );
                    self.withdraw_for_new_term(
                        &mut ticker,
                        &mut valid_until,
                        &mut observation,
                        &mut held_token,
                        &mut rotate_not_before,
                    );
                    continue;
                }
                _ = ticker.tick() => {}
            }

            let (result, attempt_valid_until) = match Box::pin(self.attempt_lease(
                &shutdown,
                &mut candidate,
                valid_until,
                observation.as_ref(),
                held_token,
            ))
            .await
            {
                LeaseOperationEvent::Shutdown => {
                    self.fence();
                    return;
                }
                LeaseOperationEvent::Deadline => {
                    tracing::warn!(
                        owner = ?self.owner,
                        "leader lease operation exceeded its local deadline; withdrawing and rotating the fencing token"
                    );
                    self.withdraw_for_new_term(
                        &mut ticker,
                        &mut valid_until,
                        &mut observation,
                        &mut held_token,
                        &mut rotate_not_before,
                    );
                    continue;
                }
                LeaseOperationEvent::Candidacy(changed) => {
                    if changed.is_err() {
                        self.fence();
                        return;
                    }
                    continue;
                }
                LeaseOperationEvent::Completed {
                    result,
                    valid_until: attempt_valid_until,
                } => {
                    let response_at = tokio::time::Instant::now();
                    if response_at >= attempt_valid_until
                        || valid_until.is_some_and(|current| response_at >= current)
                    {
                        tracing::warn!(
                            owner = ?self.owner,
                            "leader lease response arrived after its local deadline; withdrawing and rotating the fencing token"
                        );
                        self.withdraw_for_new_term(
                            &mut ticker,
                            &mut valid_until,
                            &mut observation,
                            &mut held_token,
                            &mut rotate_not_before,
                        );
                        continue;
                    }
                    (result, attempt_valid_until)
                }
            };

            match result {
                Ok(LeaseOutcome::Acquired(lease)) if lease.owner == self.owner => {
                    let publication_at = tokio::time::Instant::now();
                    if publication_at >= attempt_valid_until
                        || valid_until.is_some_and(|current| publication_at >= current)
                    {
                        tracing::warn!(
                            owner = ?self.owner,
                            "leader lease publication crossed its local deadline; withdrawing and rotating the fencing token"
                        );
                        self.withdraw_for_new_term(
                            &mut ticker,
                            &mut valid_until,
                            &mut observation,
                            &mut held_token,
                            &mut rotate_not_before,
                        );
                        continue;
                    }
                    observation = None;
                    valid_until = Some(attempt_valid_until);
                    held_token = Some(lease.token);
                    rotate_not_before = None;
                    let deadline = self.deadline_tx.borrow().clone();
                    deadline.extend_until(attempt_valid_until.into_std());
                    if !deadline.is_live() {
                        tracing::warn!(
                            owner = ?self.owner,
                            "leader lease deadline expired during local publication; withdrawing and rotating the fencing token"
                        );
                        self.withdraw_for_new_term(
                            &mut ticker,
                            &mut valid_until,
                            &mut observation,
                            &mut held_token,
                            &mut rotate_not_before,
                        );
                        continue;
                    }
                    self.lease_tx.send_replace(Some(lease));
                }
                Ok(LeaseOutcome::Acquired(_)) => {
                    self.fence();
                    return;
                }
                Ok(LeaseOutcome::Held(rival)) => {
                    self.withdraw();
                    valid_until = None;
                    held_token = None;
                    let unchanged = observation
                        .as_ref()
                        .is_some_and(|observed| observed.lease.has_same_liveness_identity(&rival));
                    if !unchanged {
                        match self.store.observe_rival(&self.owner, &rival) {
                            Ok(new_observation) => observation = Some(new_observation),
                            Err(error) => {
                                tracing::warn!(%error, "leader lease observation rejected");
                                observation = None;
                            }
                        }
                    }
                }
                Err(LeaseError::Fenced(error)) => {
                    tracing::warn!(%error, "leader lease renewal was fenced");
                    self.fence();
                    return;
                }
                Err(error) => {
                    tracing::warn!(%error, "leader lease operation failed");
                }
            }
        }
    }

    /// Spawn the renewal loop. Loss of candidacy withdraws the current local grant so this
    /// manager can contend again later. A missed local deadline withdraws the grant and rotates
    /// its fencing token before reacquisition. Shutdown or an invalid lease outcome terminally
    /// fences the manager.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        candidate: watch::Receiver<LeaderCandidacy>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(self.run(shutdown, candidate))
    }

    /// Spawn the renewal loop and signal any exit not preceded by intentional shutdown.
    ///
    /// The guard lives inside the same task as the manager future, so panic and forced task abort
    /// cannot detach a still-running inner lease writer.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn spawn_supervised(
        self,
        shutdown: tokio_util::sync::CancellationToken,
        candidate: watch::Receiver<LeaderCandidacy>,
        unexpected_exit: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let run_shutdown = shutdown.clone();
            let _exit_guard = LeaderLeaseExitGuard {
                shutdown,
                unexpected_exit,
                lease_tx: self.lease_tx.clone(),
                deadline_tx: self.deadline_tx.clone(),
            };
            self.run(run_shutdown, candidate).await;
        })
    }
}
