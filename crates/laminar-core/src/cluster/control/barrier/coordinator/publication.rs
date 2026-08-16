//! Barrier publication, direct observation, validation, and acknowledgements.

use super::super::{
    clustered_phase_roster, install_prepare_fanout, is_terminal_phase, merge_observed_announcement,
    preflight_prepare_fanout, prepare_fanout_budget, prepare_fanout_plan, require_aligned_quorum,
    retire_prepare_fanout, send_local_phase_notification, send_phase_notifications,
    validate_ack_attempt, validate_announcement_attempt, validate_publication_order,
    validate_scanned_announcements, watch, AnnouncementPublicationState, Arc, BarrierAck,
    BarrierAnnouncement, BarrierCoordinator, BarrierIdentity, Duration, GrpcState, NodeId, Phase,
    PrepareFanoutBudget, ACK_KEY, ANNOUNCEMENT_KEY, PREPARE_RPC_TIMEOUT,
};

#[cfg(feature = "cluster")]
struct ClusterPublicationPlan {
    grpc: Option<Arc<GrpcState>>,
    prepare_roster: Option<Vec<NodeId>>,
    prepare_budget: Option<PrepareFanoutBudget>,
    phase_roster: Option<Vec<NodeId>>,
    json: String,
}

impl BarrierCoordinator {
    /// Leader-side announcement for terminal, aligned, and assignment-less local/KV phases.
    ///
    /// # Errors
    /// Assignment-certified Prepare must use [`Self::announce_prepare`]. Assignment-certified
    /// reversible phases require a started leased barrier server, and Aligned requires the exact
    /// Prepare to have completed [`Self::wait_for_quorum`]. Other errors propagate validation,
    /// encoding, and publication failures.
    pub async fn announce(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        #[cfg(feature = "cluster")]
        if ann.phase == Phase::Prepare && ann.assignment_fence.is_some() {
            return Err(
                "assignment-certified Prepare requires an explicit quorum retry window".into(),
            );
        }
        self.announce_inner(ann, None).await
    }

    /// Durably publish one assignment-certified Prepare and immediately start its direct fan-out.
    ///
    /// # Errors
    /// Rejects a different phase, an assignment-less announcement, a zero/indivisible quorum
    /// window, malformed authority, conflicting in-flight Prepare state, or publication failure.
    #[cfg(feature = "cluster")]
    pub async fn announce_prepare(
        &self,
        ann: &BarrierAnnouncement,
        quorum_window: Duration,
    ) -> Result<(), String> {
        let attempt_deadline = tokio::time::Instant::now() + PREPARE_RPC_TIMEOUT.max(quorum_window);
        self.announce_prepare_until(ann, attempt_deadline, quorum_window)
            .await
    }

    /// Publish Prepare with an exact absolute attempt deadline and an independent short retry
    /// window. Durable admission/publication latency therefore cannot refresh fan-out lifetime.
    ///
    /// # Errors
    /// Rejects an expired attempt deadline or invalid retry window in addition to
    /// [`Self::announce_prepare`] errors.
    #[cfg(feature = "cluster")]
    pub async fn announce_prepare_until(
        &self,
        ann: &BarrierAnnouncement,
        attempt_deadline: tokio::time::Instant,
        retry_window: Duration,
    ) -> Result<(), String> {
        if ann.phase != Phase::Prepare || ann.assignment_fence.is_none() {
            return Err("explicit Prepare fan-out requires an assignment certificate".into());
        }
        let budget = prepare_fanout_budget(attempt_deadline, retry_window)?;
        self.announce_inner(ann, Some(budget)).await
    }

    pub(super) async fn announce_inner(
        &self,
        ann: &BarrierAnnouncement,
        prepare_budget: Option<PrepareFanoutBudget>,
    ) -> Result<(), String> {
        validate_announcement_attempt(ann)?;
        #[cfg(feature = "cluster")]
        {
            let plan = self
                .prepare_cluster_publication(ann, prepare_budget)
                .await?;
            self.publish_cluster_announcement(ann, plan).await
        }

        #[cfg(not(feature = "cluster"))]
        {
            let _ = prepare_budget;
            self.publish_local_announcement(ann).await
        }
    }

    #[cfg(feature = "cluster")]
    async fn prepare_cluster_publication(
        &self,
        ann: &BarrierAnnouncement,
        prepare_budget: Option<PrepareFanoutBudget>,
    ) -> Result<ClusterPublicationPlan, String> {
        self.validate_reversible_announcement(ann).await?;
        let (prepare_roster, prepare_budget) = prepare_fanout_plan(ann, prepare_budget)?;
        let grpc = self.grpc.lock().clone();
        let process_bound = self.local_process.get().is_some();
        match (process_bound, ann.assignment_fence.is_some()) {
            (true, false) => {
                return Err(format!(
                    "process-bound cluster {:?} requires an assignment certificate",
                    ann.phase
                ));
            }
            (false, true) => {
                return Err(format!(
                    "assignment-certified {:?} requires a process-bound leased endpoint",
                    ann.phase
                ));
            }
            (true, true) | (false, false) => {}
        }
        if ann.assignment_fence.is_some()
            && matches!(ann.phase, Phase::Prepare | Phase::Aligned)
            && grpc.is_none()
        {
            return Err(format!(
                "assignment-certified {:?} requires a started leased barrier server",
                ann.phase
            ));
        }
        let phase_roster = grpc
            .as_ref()
            .filter(|_| ann.phase != Phase::Prepare)
            .map(|state| clustered_phase_roster(ann, state.local_process.get().copied()))
            .transpose()?
            .flatten();
        let json = serde_json::to_string(ann).map_err(|error| error.to_string())?;
        Ok(ClusterPublicationPlan {
            grpc,
            prepare_roster,
            prepare_budget,
            phase_roster,
            json,
        })
    }

    #[cfg(feature = "cluster")]
    async fn publish_cluster_announcement(
        &self,
        ann: &BarrierAnnouncement,
        plan: ClusterPublicationPlan,
    ) -> Result<(), String> {
        let mut publication = self.publication.lock().await;
        if !publication.initialized {
            publication.latest = self.scan_latest_announcement().await?;
            publication.initialized = true;
        }
        if let Some(current) = publication.latest.as_ref() {
            validate_publication_order(current, ann)?;
        }

        let Some(state) = plan.grpc.as_ref() else {
            publication.latest = Some(ann.clone());
            return self
                .kv
                .write_checked(ANNOUNCEMENT_KEY, plan.json)
                .await
                .map_err(|error| format!("publish barrier announcement: {error}"));
        };
        let certified_prepare = ann.phase == Phase::Prepare && plan.prepare_roster.is_some();
        if certified_prepare {
            self.require_live_bound_process_lease()?;
            self.require_exact_local_leader_proof(ann)?;
        }
        let replace_prepare_fanout = if plan.prepare_roster.is_some() {
            preflight_prepare_fanout(state, ann)?
        } else {
            false
        };
        if ann.phase == Phase::Aligned && plan.phase_roster.is_some() {
            return self.publish_certified_aligned(ann, plan, publication).await;
        }
        self.publish_durable_first(
            ann,
            plan,
            certified_prepare,
            replace_prepare_fanout,
            publication,
        )
        .await
    }

    #[cfg(feature = "cluster")]
    async fn publish_certified_aligned(
        &self,
        ann: &BarrierAnnouncement,
        plan: ClusterPublicationPlan,
        mut publication: tokio::sync::MutexGuard<'_, AnnouncementPublicationState>,
    ) -> Result<(), String> {
        let state = plan.grpc.expect("certified Aligned has a gRPC state");
        self.require_live_bound_process_lease()?;
        self.require_exact_local_leader_proof(ann)?;
        require_aligned_quorum(&state, ann)?;
        let process_lease = self
            .process_lease_deadline
            .get()
            .cloned()
            .ok_or_else(|| "process lease deadline is not installed".to_string())?;
        // Admission advances before cancellable I/O so an ambiguous error cannot reopen Prepare.
        publication.latest = Some(ann.clone());

        let expected = plan
            .phase_roster
            .expect("certified Aligned has an assignment roster");
        let remote = send_phase_notifications(&state, &self.kv, ann, expected);
        let local_result = send_local_phase_notification(&state, ann, &process_lease);
        let durable = self.kv.write_checked(ANNOUNCEMENT_KEY, plan.json);
        tokio::pin!(remote);
        tokio::pin!(durable);
        let mut completed_remote = None;
        let durable_result = tokio::select! {
            result = &mut durable => result,
            results = &mut remote => {
                completed_remote = Some(results);
                durable.await
            }
        };
        let authority_result = self
            .require_live_bound_process_lease()
            .and_then(|()| self.require_exact_local_leader_proof(ann));
        drop(publication);
        authority_result?;

        let direct_results = match completed_remote {
            Some(results) => results,
            None => remote.await,
        };
        if let Err(error) = local_result {
            tracing::warn!(epoch = ann.epoch, %error, "local aligned delivery failed; durable observation remains available");
        }
        for error in direct_results.into_iter().filter_map(Result::err) {
            tracing::warn!(epoch = ann.epoch, %error, "aligned delivery failed; durable observation remains available");
        }
        self.require_live_bound_process_lease()?;
        self.require_exact_local_leader_proof(ann)?;
        durable_result.map_err(|error| format!("publish barrier announcement: {error}"))
    }

    #[cfg(feature = "cluster")]
    async fn publish_durable_first(
        &self,
        ann: &BarrierAnnouncement,
        plan: ClusterPublicationPlan,
        certified_prepare: bool,
        replace_prepare_fanout: bool,
        mut publication: tokio::sync::MutexGuard<'_, AnnouncementPublicationState>,
    ) -> Result<(), String> {
        let state = plan
            .grpc
            .expect("durable-first cluster publication has a gRPC state");
        if is_terminal_phase(ann.phase) {
            retire_prepare_fanout(&state);
        }
        publication.latest = Some(ann.clone());
        self.kv
            .write_checked(ANNOUNCEMENT_KEY, plan.json)
            .await
            .map_err(|error| format!("publish barrier announcement: {error}"))?;
        if ann.phase == Phase::Prepare {
            if certified_prepare {
                self.require_live_bound_process_lease()?;
                self.require_exact_local_leader_proof(ann)?;
            }
            if let Some(expected) = plan.prepare_roster.filter(|_| replace_prepare_fanout) {
                install_prepare_fanout(
                    &state,
                    &self.kv,
                    ann,
                    expected,
                    plan.prepare_budget
                        .expect("certified Prepare budget was validated"),
                );
            }
            drop(publication);
            return Ok(());
        }

        drop(publication);
        for result in
            send_phase_notifications(&state, &self.kv, ann, plan.phase_roster.unwrap_or_default())
                .await
        {
            match result {
                Ok(()) => {}
                Err(error) if ann.phase == Phase::Aligned => {
                    tracing::warn!(epoch = ann.epoch, %error, "aligned delivery failed; durable observation remains available");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "cluster"))]
    async fn publish_local_announcement(&self, ann: &BarrierAnnouncement) -> Result<(), String> {
        let json = serde_json::to_string(ann).map_err(|error| error.to_string())?;
        let mut publication = self.publication.lock().await;
        if !publication.initialized {
            publication.latest = self.scan_latest_announcement().await?;
            publication.initialized = true;
        }
        if let Some(current) = publication.latest.as_ref() {
            validate_publication_order(current, ann)?;
        }
        publication.latest = Some(ann.clone());
        self.kv
            .write_checked(ANNOUNCEMENT_KEY, json)
            .await
            .map_err(|error| format!("publish barrier announcement: {error}"))
    }

    /// Watch over gRPC-delivered announcements, for push-driven waits
    /// (the decision wait and the Aligned resume gate). `None` until
    /// the gRPC server is started — gossip-KV-only deployments fall
    /// back to polling the merged gossip history.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub fn announcement_watch(&self) -> Option<watch::Receiver<Option<BarrierAnnouncement>>> {
        self.grpc.lock().as_ref().map(|s| s.latest_rx.clone())
    }

    /// Merge the latest direct and gossip announcements without consulting remote authority.
    /// Callers may inspect the result, but must validate a matching reversible phase before use.
    /// Observation is non-destructive, and direct plus gossip histories must remain related by
    /// canonical checkpoint ID. Terminal durable KV values remain the decision authority.
    ///
    /// # Errors
    /// Returns a string on transport, decode, or conflicting-history failure.
    pub(in crate::cluster::control) async fn observe_hint(
        &self,
        leader: NodeId,
    ) -> Result<Option<BarrierAnnouncement>, String> {
        #[cfg(feature = "cluster")]
        let grpc_latest: Option<BarrierAnnouncement> = {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(error) = grpc_opt
                .as_ref()
                .and_then(|state| state.merge_error.lock().clone())
            {
                return Err(error);
            }
            grpc_opt.and_then(|state| state.latest_rx.borrow().clone())
        };
        #[cfg(not(feature = "cluster"))]
        let grpc_latest: Option<BarrierAnnouncement> = None;

        let kv_latest: Option<BarrierAnnouncement> =
            match self.kv.read_from_checked(leader, ANNOUNCEMENT_KEY).await? {
                Some(json) => Some(serde_json::from_str(&json).map_err(|error| {
                    format!("malformed durable barrier announcement from {leader}: {error}")
                })?),
                None => None,
            };

        let observed = match (grpc_latest, kv_latest) {
            (Some(g), Some(k)) => Some(merge_observed_announcement(g, k)?),
            (Some(g), None) => Some(g),
            (None, k) => k,
        };
        if let Some(announcement) = observed.as_ref() {
            validate_announcement_attempt(announcement)?;
        }
        Ok(observed)
    }

    #[cfg(feature = "cluster")]
    /// Validate one merged announcement immediately before a caller uses it.
    pub(in crate::cluster::control) async fn validate_observed(
        &self,
        announcement: &BarrierAnnouncement,
    ) -> Result<(), String> {
        self.validate_reversible_announcement(announcement).await
    }

    pub(in crate::cluster::control::barrier) async fn scan_latest_announcement(
        &self,
    ) -> Result<Option<BarrierAnnouncement>, String> {
        let mut announcements = Vec::new();
        for (node, json) in self.kv.scan_checked(ANNOUNCEMENT_KEY).await? {
            let announcement: BarrierAnnouncement = serde_json::from_str(&json)
                .map_err(|error| format!("malformed barrier announcement from {node}: {error}"))?;
            announcements.push(announcement);
        }
        validate_scanned_announcements(announcements)
    }

    /// Follower-side ack.
    ///
    /// # Errors
    /// Returns a string on JSON encode failure.
    pub async fn ack(&self, ack: &BarrierAck) -> Result<(), String> {
        validate_ack_attempt(ack)?;
        #[cfg(feature = "cluster")]
        {
            let grpc_opt = self.grpc.lock().clone();
            if let Some(state) = grpc_opt {
                let identity = BarrierIdentity::from_ack(ack);
                let (cached, waiters) = {
                    let mut prepare = state.prepare_acks.lock();
                    let cached = prepare.record_ack(identity, ack);
                    let waiters = prepare.pending.remove(&identity).unwrap_or_default();
                    (cached, waiters)
                };
                for waiter in waiters {
                    let _ = waiter.response.send(cached.clone());
                }
                return Ok(());
            }
        }

        let json = serde_json::to_string(ack).map_err(|e| e.to_string())?;
        self.kv.write(ACK_KEY, json).await;
        Ok(())
    }
}
