//! Recovery prepare, start, release, stopped quorum, and terminal cleanup.

use super::*;

impl ClusterController {
    pub(super) async fn durable_recovery_proof_is_current(
        &self,
        proof: &LeaderProof,
    ) -> Result<bool, String> {
        if !proof.is_canonical() {
            return Ok(false);
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| format!("durable recovery authority is unavailable: {error}"))?;
        let Some(lease) = authority
            .load()
            .await
            .map_err(|error| format!("durable recovery authority read failed: {error}"))?
        else {
            return Ok(false);
        };
        Ok(lease.matches_proof(proof))
    }

    pub(super) async fn recovery_driver_proof_is_current(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, String> {
        Ok(round.id.driver == self.instance_id
            && self.proof_is_live(&round.leader_proof)
            && self
                .durable_recovery_proof_is_current(&round.leader_proof)
                .await?)
    }

    pub(super) async fn require_recovery_driver_proof(
        &self,
        round: &RecoveryRound,
        boundary: &str,
    ) -> Result<(), String> {
        if self.recovery_driver_proof_is_current(round).await? {
            Ok(())
        } else {
            Err(format!(
                "recovery driver proof is no longer live at {boundary}"
            ))
        }
    }

    /// Audit that this process still owns the recovery round's exact local and durable leader
    /// proof. A same-node fencing-term rotation is supersession just like a driver transfer.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryControlError::Superseded`] when the exact proof is no longer current,
    /// [`RecoveryControlError::Uncertain`] for retryable authority I/O, and
    /// [`RecoveryControlError::Conflict`] for unavailable or invalid authority state.
    pub async fn audit_recovery_driver_proof(
        &self,
        round: &RecoveryRound,
        boundary: &str,
    ) -> Result<(), RecoveryControlError> {
        if round.id.driver != self.instance_id || !self.proof_is_live(&round.leader_proof) {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery driver proof is no longer live at {boundary}"
            )));
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?;
        let current = authority.load().await.map_err(|error| match error {
            super::super::LeaseError::Io(reason) => RecoveryControlError::Uncertain(reason),
            error => RecoveryControlError::Conflict(error.to_string()),
        })?;
        if !current.is_some_and(|lease| lease.matches_proof(&round.leader_proof)) {
            return Err(RecoveryControlError::Superseded(format!(
                "durable recovery driver proof changed at {boundary}"
            )));
        }
        if round.id.driver != self.instance_id || !self.proof_is_live(&round.leader_proof) {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery driver proof changed while auditing {boundary}"
            )));
        }
        Ok(())
    }

    pub(super) async fn recovery_evidence_roster_matches(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, String> {
        let candidates = round
            .faults
            .iter()
            .filter(|fault| !round.assignment_fence.contains(fault.reporter.0))
            .map(|fault| fault.reporter.0)
            .collect::<Vec<_>>();
        let available = if candidates.is_empty() {
            Vec::new()
        } else {
            self.available_recovery_participant_incarnations(&candidates)
                .await?
        };
        Ok(available == round.evidence_participants)
    }

    /// Announce phase 1 with the immutable stopped/evidence roster.
    ///
    /// # Errors
    /// Returns an error unless this node is the round's current leader and driver.
    pub async fn announce_recover_prepare(&self, round: &RecoveryRound) -> Result<(), String> {
        round.validate()?;
        if round.id.driver != self.instance_id {
            return Err("only the current leader may prepare its recovery round".into());
        }
        self.require_recovery_driver_proof(round, "Prepare preflight")
            .await?;
        if !self.recovery_evidence_roster_matches(round).await? {
            return Err("available recovery evidence roster changed before Prepare".into());
        }
        if !self
            .recovery_stopped_incarnations_match(round)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("recovery stopped-process roster changed before Prepare".into());
        }
        let _guard = self.recovery_writes.lock().await;
        self.require_recovery_driver_proof(round, "Prepare publication")
            .await?;
        if !self.recovery_evidence_roster_matches(round).await? {
            return Err("available recovery evidence roster changed during Prepare".into());
        }
        if let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
        {
            if let Some(active) = parse_recovery_announcement(&raw)? {
                if active.round.has_terminal_fault() && active.round != *round {
                    return Err("active terminal recovery Prepare cannot be superseded".into());
                }
            }
        }
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Prepare,
        };
        let encoded = encode_recovery_announcement(&announcement)?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Prepare read-back")
            .await
    }

    /// Transition the identical prepared round to `Start` with a target bound into the
    /// announcement. A missing or different `Prepare` is never upgraded.
    ///
    /// # Errors
    /// Returns an error on lost leadership, an invalid round, or a mismatched prior phase.
    pub async fn announce_recover_start(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<(), String> {
        round.validate()?;
        if round.has_terminal_fault() {
            return Err("terminal recovery fault is permanently retained in Prepare".into());
        }
        self.audit_recovery_faults_control(round)
            .await
            .map_err(|error| error.to_string())?;
        if round.id.driver != self.instance_id {
            return Err("only the current leader may start its recovery round".into());
        }
        self.require_recovery_driver_proof(round, "Start preflight")
            .await?;
        let _guard = self.recovery_writes.lock().await;
        if !self.recovery_incarnations_match(round).await? {
            return Err(
                "recovery driver or process-incarnation roster changed before Start".into(),
            );
        }
        let current = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
            .ok_or_else(|| "recovery Prepare disappeared before Start".to_string())?;
        let prepared = parse_recovery_announcement(&current)?
            .ok_or_else(|| "recovery Prepare was cleared before Start".to_string())?;
        if prepared.round != *round || prepared.phase != RecoverPhase::Prepare {
            return Err("recovery Start does not match the exact active Prepare".into());
        }
        self.audit_recovery_faults_control(round)
            .await
            .map_err(|error| error.to_string())?;
        self.require_recovery_driver_proof(round, "Start publication")
            .await?;
        let announcement = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Start { epoch },
        };
        let encoded = encode_recovery_announcement(&announcement)?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Start read-back")
            .await
    }

    /// Transition the identical `Start` to a pending `Release`. Source gates remain closed until
    /// the leader commits the exact compact readiness roster.
    ///
    /// # Errors
    /// Returns an error on lost leadership, a changed incarnation roster, or a mismatched Start.
    pub async fn announce_recover_release(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<(), String> {
        round.validate()?;
        if round.has_terminal_fault() {
            return Err("terminal recovery fault cannot advance to Release".into());
        }
        if round.id.driver != self.instance_id {
            return Err("only the current leader may release its recovery round".into());
        }
        self.require_recovery_driver_proof(round, "Release preflight")
            .await?;
        let _guard = self.recovery_writes.lock().await;
        if !self.recovery_incarnations_match(round).await? {
            return Err(
                "recovery driver or process-incarnation roster changed before Release".into(),
            );
        }
        let current = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
            .ok_or_else(|| "recovery Start disappeared before Release".to_string())?;
        let started = parse_recovery_announcement(&current)?
            .ok_or_else(|| "recovery Start was cleared before Release".to_string())?;
        if started.round != *round || started.phase != (RecoverPhase::Start { epoch }) {
            return Err("recovery Release does not match the exact active Start target".into());
        }
        self.require_recovery_driver_proof(round, "Release publication")
            .await?;
        let release = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch },
        };
        let encoded = encode_recovery_announcement(&release)?;
        self.write_recovery_value_exact("control:recover", encoded)
            .await?;
        self.require_recovery_driver_proof(round, "Release read-back")
            .await
    }

    /// Commit a pending release after every frozen owner published its compact readiness record.
    ///
    /// A pending attempt audits the frozen fault set before returning incomplete readiness. Once
    /// complete, the process roster and fault set are validated under the driver's phase-transition
    /// mutex before admitting the content-addressed terminal into durable leader authority.
    ///
    /// # Errors
    /// Returns a classified uncertain, conflict, or superseded outcome. Missing readiness remains
    /// a normal pending status.
    pub async fn try_commit_recover_release(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<ReleaseCommitStatus, RecoveryControlError> {
        release.validate().map_err(RecoveryControlError::Conflict)?;
        let RecoverPhase::Release { epoch } = release.phase else {
            return Err(RecoveryControlError::Conflict(
                "release commit must bind a pending Release target".into(),
            ));
        };
        let round = &release.round;
        if round.id.driver != self.instance_id {
            return Err(RecoveryControlError::Superseded(
                "only the current leader may commit its recovery Release".into(),
            ));
        }
        self.audit_recovery_driver_proof(round, "Release commit preflight")
            .await?;
        match self.read_release_ready(release).await? {
            ReleaseReadyStatus::Complete => {}
            ReleaseReadyStatus::Pending { missing } => {
                self.audit_pending_release_faults_control(release).await?;
                return Ok(ReleaseCommitStatus::Pending { missing });
            }
        }
        let committed = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::ReleaseCommitted { epoch },
        };
        let authority = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?;
        let reference = authority
            .stage_recovery_release_terminal(&committed)
            .await
            .map_err(RecoveryControlError::from_authority)?;

        let _guard = self.recovery_writes.lock().await;
        if !self.recovery_incarnations_match_control(round).await? {
            return Err(RecoveryControlError::Superseded(
                "recovery process-incarnation roster changed before Release commit".into(),
            ));
        }
        self.audit_recovery_faults_control(round).await?;
        let Some(current) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Err(RecoveryControlError::Superseded(
                "pending recovery Release disappeared before commit".into(),
            ));
        };
        let active = match parse_recovery_announcement(&current) {
            Ok(Some(active)) => active,
            Ok(None) => {
                return Err(RecoveryControlError::Superseded(
                    "pending recovery Release was cleared before commit".into(),
                ));
            }
            Err(reason) => return Err(RecoveryControlError::Conflict(reason)),
        };
        if active != *release {
            let error = if active.round.id.generation > round.id.generation {
                RecoveryControlError::Superseded(
                    "a newer recovery intent replaced the pending Release".into(),
                )
            } else {
                RecoveryControlError::Conflict(
                    "recovery Release commit does not match the exact pending intent".into(),
                )
            };
            return Err(error);
        }
        self.audit_recovery_driver_proof(round, "Release commit publication")
            .await?;
        match Box::pin(authority.record_recovery_release_commit(&round.leader_proof, reference))
            .await
            .map_err(RecoveryControlError::from_authority)?
        {
            super::super::leader_lease::RecordRecoveryReleaseCommitResult::Created(_)
            | super::super::leader_lease::RecordRecoveryReleaseCommitResult::Unchanged(_) => {
                Ok(ReleaseCommitStatus::Committed {
                    terminal: committed,
                })
            }
            super::super::leader_lease::RecordRecoveryReleaseCommitResult::Conflict { winner } => {
                if winner.generation() > round.id.generation {
                    Err(RecoveryControlError::Superseded(format!(
                        "recovery release generation {} replaced generation {}",
                        winner.generation(),
                        round.id.generation
                    )))
                } else {
                    Err(RecoveryControlError::Conflict(format!(
                        "recovery release generation {} has a different durable winner",
                        round.id.generation
                    )))
                }
            }
            super::super::leader_lease::RecordRecoveryReleaseCommitResult::FaultsChanged => {
                Err(RecoveryControlError::Superseded(
                    "recovery fault inventory changed at Release authority admission".into(),
                ))
            }
        }
    }

    /// Active recovery announcement with semantic failures separated from uncertain I/O.
    ///
    /// # Errors
    /// Classifies malformed state, superseded authority, and retryable durable I/O separately.
    pub async fn observe_recover_control(
        &self,
    ) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
        let Some(current_driver) = self.current_leader() else {
            return Ok(None);
        };
        let Some(raw) = self
            .read_recovery_value(current_driver, "control:recover")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Ok(None);
        };
        let Some(announcement) =
            parse_recovery_announcement(&raw).map_err(RecoveryControlError::Conflict)?
        else {
            return Ok(None);
        };
        if matches!(announcement.phase, RecoverPhase::ReleaseCommitted { .. }) {
            return Err(RecoveryControlError::Conflict(
                "committed recovery release appeared in the mutable intent slot".into(),
            ));
        }
        if announcement.round.id.driver != current_driver {
            return Err(RecoveryControlError::Conflict(format!(
                "recovery publisher {current_driver} is not declared driver {}",
                announcement.round.id.driver
            )));
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?;
        let Some(authority_before) = authority.load().await.map_err(|error| match error {
            super::super::LeaseError::Io(reason) => RecoveryControlError::Uncertain(reason),
            error => RecoveryControlError::Conflict(error.to_string()),
        })?
        else {
            return Err(RecoveryControlError::Superseded(
                "durable recovery authority has no leader".into(),
            ));
        };
        if !authority_before.matches_proof(&announcement.round.leader_proof) {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery phase from {current_driver} does not match durable leader authority"
            )));
        }
        let Some(authority_after) = authority.load().await.map_err(|error| match error {
            super::super::LeaseError::Io(reason) => RecoveryControlError::Uncertain(reason),
            error => RecoveryControlError::Conflict(error.to_string()),
        })?
        else {
            return Err(RecoveryControlError::Superseded(
                "durable recovery authority vanished during observation".into(),
            ));
        };
        if self.current_leader() != Some(current_driver)
            || !authority_after.matches_proof(&announcement.round.leader_proof)
        {
            return Err(RecoveryControlError::Superseded(format!(
                "recovery authority changed while observing {current_driver}"
            )));
        }
        Ok(Some(announcement))
    }

    /// Latest irrevocable recovery release admitted by the append-only leader authority.
    ///
    /// # Errors
    /// Classifies missing/corrupt terminal state as conflict, takeover as supersession, and
    /// durable I/O as uncertainty.
    pub async fn latest_committed_recover_release(
        &self,
    ) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .latest_recovery_release_terminal()
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Resolve the exact committed terminal for one pending release intent across leader
    /// takeover. An older terminal is unrelated; a same-generation divergence is corruption and a
    /// newer terminal supersedes the caller.
    ///
    /// # Errors
    /// Returns a classified conflict, supersession, or uncertain durable read.
    pub async fn observe_committed_recover_release(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<Option<RecoveryAnnouncement>, RecoveryControlError> {
        round.validate().map_err(RecoveryControlError::Conflict)?;
        let expected = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::ReleaseCommitted { epoch },
        };
        let Some(terminal) = self.latest_committed_recover_release().await? else {
            return Ok(None);
        };
        match terminal.round.id.generation.cmp(&round.id.generation) {
            std::cmp::Ordering::Less => Ok(None),
            std::cmp::Ordering::Greater => Err(RecoveryControlError::Superseded(format!(
                "recovery release generation {} replaced generation {}",
                terminal.round.id.generation, round.id.generation
            ))),
            std::cmp::Ordering::Equal if terminal == expected => Ok(Some(terminal)),
            std::cmp::Ordering::Equal => Err(RecoveryControlError::Conflict(format!(
                "committed recovery release generation {} differs from the expected round",
                round.id.generation
            ))),
        }
    }

    /// Best-effort cleanup for this driver's mutable `Release` discovery hint after the exact
    /// terminal is irrevocably present in leader authority. Cleanup never contributes to commit
    /// validity; followers resolve the terminal from authority even if this write is uncertain.
    ///
    /// # Errors
    /// Classifies malformed or divergent local intent separately from retryable I/O.
    pub async fn retire_committed_recover_release_hint(
        &self,
        round: &RecoveryRound,
        epoch: u64,
    ) -> Result<bool, RecoveryControlError> {
        round.validate().map_err(RecoveryControlError::Conflict)?;
        if round.id.driver != self.instance_id {
            return Err(RecoveryControlError::Conflict(
                "only the publishing driver may retire its recovery Release hint".into(),
            ));
        }
        if self
            .observe_committed_recover_release(round, epoch)
            .await?
            .is_none()
        {
            return Ok(false);
        }
        let pending = RecoveryAnnouncement {
            round: round.clone(),
            phase: RecoverPhase::Release { epoch },
        };
        let _guard = self.recovery_writes.lock().await;
        let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await
            .map_err(RecoveryControlError::Uncertain)?
        else {
            return Ok(false);
        };
        let active = parse_recovery_announcement(&raw)
            .map_err(RecoveryControlError::Conflict)?
            .ok_or_else(|| {
                RecoveryControlError::Conflict(
                    "recovery Release hint decoded as an empty announcement".into(),
                )
            })?;
        if active != pending {
            return if active.round.id.generation > round.id.generation {
                Err(RecoveryControlError::Superseded(
                    "a newer recovery intent replaced the committed Release hint".into(),
                ))
            } else {
                Err(RecoveryControlError::Conflict(
                    "mutable recovery intent differs from its committed Release".into(),
                ))
            };
        }
        self.write_recovery_value_exact("control:recover", String::new())
            .await
            .map_err(RecoveryControlError::Uncertain)?;
        Ok(true)
    }

    /// Active nonterminal recovery announcement from the locally elected driver.
    ///
    /// # Errors
    /// Returns a display-stable classified control error.
    pub async fn observe_recover(&self) -> Result<Option<RecoveryAnnouncement>, String> {
        self.observe_recover_control()
            .await
            .map_err(|error| error.to_string())
    }

    /// Whether the round's declared driver is the current elected leader in this local view.
    #[must_use]
    pub fn recovery_driver_is_current(&self, round: &RecoveryRound) -> bool {
        self.current_leader() == Some(round.id.driver)
    }

    /// Whether the assignment-owner quorum names this exact process, not only its stable node id.
    #[must_use]
    pub fn recovery_round_contains_current_process(&self, round: &RecoveryRound) -> bool {
        round.owner_incarnation(self.instance_id) == Some(self.recovery_incarnation)
    }

    /// Whether this exact owner or evidence-reporter process must stop for the round's Prepare.
    #[must_use]
    pub fn recovery_round_requires_current_process_stop(&self, round: &RecoveryRound) -> bool {
        round.stopped_participant_incarnation(self.instance_id) == Some(self.recovery_incarnation)
    }

    /// Ack phase 1 for the exact frozen round.
    ///
    /// # Errors
    /// Returns an error for invalid state or when this node is outside the stopped roster.
    pub async fn announce_stopped(&self, round: &RecoveryRound) -> Result<(), String> {
        round.validate()?;
        if !round.contains_stopped_participant(self.instance_id) {
            return Err("node outside recovery stopped roster cannot acknowledge Prepare".into());
        }
        if !self.recovery_round_requires_current_process_stop(round) {
            return Err("Prepare acknowledgement has a stale local process incarnation".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("Prepare acknowledgement came from a superseded local process".into());
        }
        let report = RecoveryStoppedReport::new(
            round,
            CheckpointParticipant {
                node_id: self.instance_id.0,
                boot_incarnation: self.recovery_incarnation,
            },
        )?;
        let encoded = encode_recovery_stopped_report(&report, round)?;
        self.write_recovery_value_exact(RECOVERY_STOPPED_REPORT_KEY, encoded)
            .await
    }

    /// Point-read only the still-missing members of an exact stopped roster.
    ///
    /// # Errors
    /// Returns a conflict for a noncanonical or out-of-round subset and preserves the same
    /// uncertainty/conflict/supersession classification used by recovery quorum polling.
    pub async fn read_stopped(
        &self,
        round: &RecoveryRound,
        participants: &[NodeId],
    ) -> Result<Vec<RecoveryStoppedReport>, RecoveryControlError> {
        if participants.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || participants.iter().any(NodeId::is_unassigned)
        {
            return Err(RecoveryControlError::Conflict(
                "recovery stopped-report subset requires canonical participants".into(),
            ));
        }
        let roster = participants
            .iter()
            .map(|node| {
                round
                    .stopped_participant_incarnation(*node)
                    .map(|boot_incarnation| CheckpointParticipant {
                        node_id: node.0,
                        boot_incarnation,
                    })
                    .ok_or_else(|| {
                        RecoveryControlError::Conflict(format!(
                            "node {node} is outside the recovery stopped roster"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.read_recovery_stopped_reports(round, &roster).await
    }

    /// Clear only this driver's still-identical recovery announcement. The per-controller lock
    /// makes the read/clear conditional with respect to a concurrent local phase transition.
    ///
    /// # Errors
    /// Returns an error when the visible local announcement is malformed.
    pub async fn clear_recover(&self, round: &RecoveryRound) -> Result<bool, String> {
        if round.has_terminal_fault() {
            return Err("terminal recovery Prepare cannot be cleared automatically".into());
        }
        let _guard = self.recovery_writes.lock().await;
        let Some(raw) = self
            .read_recovery_value(self.instance_id, "control:recover")
            .await?
        else {
            return Ok(false);
        };
        let Some(active) = parse_recovery_announcement(&raw)? else {
            return Ok(false);
        };
        if active.round != *round {
            return Ok(false);
        }
        if matches!(active.phase, RecoverPhase::Release { .. }) {
            return Ok(false);
        }
        self.write_recovery_value_exact("control:recover", String::new())
            .await?;
        Ok(true)
    }

    /// Wait until the merged barrier history yields an announcement matching `pred`, or `timeout`
    /// expires (→ `Ok(None)`). Observation remains side-effect free; event-time progress must come
    /// from immutable checkpoint authority. Push-driven off the gRPC announcement watch when available; gossip-KV-only
    /// deployments (and KV-only announcements) are covered by a
    /// fallback poll — 250ms with the watch, 25ms without.
    ///
    /// # Errors
    /// Returns the first observation error instead of converting a known protocol or transport
    /// failure into a timeout.
    #[cfg(feature = "cluster")]
    pub async fn wait_for_barrier<F>(
        &self,
        mut pred: F,
        timeout: Duration,
    ) -> Result<Option<BarrierAnnouncement>, String>
    where
        F: FnMut(&BarrierAnnouncement) -> bool,
    {
        let mut watch = self.barrier.announcement_watch();
        // Recomputed per iteration: when the watch sender drops
        // mid-wait, the fallback must tighten to the no-watch cadence.
        let poll_for = |watch: &Option<_>| {
            if watch.is_some() {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(25)
            }
        };
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let observed =
                match tokio::time::timeout_at(deadline, self.observe_barrier_matching(&mut pred))
                    .await
                {
                    Ok(observed) => observed?,
                    Err(_) => return Ok(None),
                };
            if let Some(ann) = observed {
                return Ok(Some(ann));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(None);
            }
            let poll = poll_for(&watch);
            let pushed = async {
                match watch.as_mut() {
                    Some(w) => w.changed().await.is_ok(),
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                ok = pushed => {
                    if !ok {
                        // Sender gone (server shutdown) — degrade to
                        // polling instead of spinning on the error.
                        watch = None;
                    }
                }
                () = tokio::time::sleep(poll) => {}
                () = tokio::time::sleep_until(deadline) => return Ok(None),
            }
        }
    }

    /// Leader-side: poll until quorum or `deadline`.
    pub async fn wait_for_quorum(
        &self,
        prepare: &BarrierAnnouncement,
        expected: &[NodeId],
        deadline: Duration,
    ) -> QuorumOutcome {
        self.barrier
            .wait_for_quorum(prepare, expected, deadline)
            .await
    }

    /// Assignment snapshot store, if configured.
    #[must_use]
    pub fn snapshot_store(&self) -> Option<&AssignmentSnapshotStore> {
        self.snapshot.as_deref()
    }
}
