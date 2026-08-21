//! Fault admission, inventories, release readiness, and recovered acknowledgements.

use super::*;

impl ClusterController {
    /// Allocate the next process-local recovery-fault request identity.
    ///
    /// Shared authority converts this ordinal into a cluster-wide fault sequence. Retries retain
    /// the original ordinal; exhaustion fails closed instead of wrapping.
    ///
    /// # Errors
    /// Returns an error if the process-local sequence is exhausted or becomes noncanonical.
    pub fn next_recovery_fault_request(&self) -> Result<RecoveryFaultRequest, String> {
        let sequence = self
            .recovery_fault_request_sequence
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| "recovery fault request sequence exhausted".to_string())?;
        Ok(RecoveryFaultRequest {
            sequence: std::num::NonZeroU64::new(sequence)
                .ok_or_else(|| "recovery fault request allocator produced zero".to_string())?,
        })
    }

    /// Reconstitute one previously allocated request for a bounded retry.
    ///
    /// # Errors
    /// Rejects zero and ordinals that this controller process has not allocated.
    pub fn recovery_fault_request(&self, sequence: u64) -> Result<RecoveryFaultRequest, String> {
        let sequence = std::num::NonZeroU64::new(sequence)
            .ok_or_else(|| "recovery fault request sequence must be nonzero".to_string())?;
        if sequence.get() >= self.recovery_fault_request_sequence.load(Ordering::Acquire) {
            return Err("recovery fault request was not allocated by this process".into());
        }
        Ok(RecoveryFaultRequest { sequence })
    }

    pub(super) fn recovery_fault_publisher(&self) -> Result<RecoveryFaultPublisher, String> {
        let publisher = RecoveryFaultPublisher {
            participant: CheckpointParticipant {
                node_id: self.instance_id.0,
                boot_incarnation: self.recovery_incarnation,
            },
            process_term: self.recovery_process_term.load(Ordering::Acquire),
        };
        publisher.validate()?;
        Ok(publisher)
    }

    pub(super) fn capture_live_local_process_authority(
        &self,
    ) -> Result<LocalProcessAuthorityIdentity, String> {
        let _transition = self.process_authority_transition.lock();
        self.capture_live_local_process_authority_locked()
    }

    pub(super) fn capture_live_local_process_authority_locked(
        &self,
    ) -> Result<LocalProcessAuthorityIdentity, String> {
        if !self.recovery_process_lease_is_live() {
            return Err("local process lease authority is not live".into());
        }
        let publisher = self.recovery_fault_publisher()?;
        if !self.recovery_process_lease_is_live() {
            return Err("local process lease authority changed while sampling identity".into());
        }
        Ok(LocalProcessAuthorityIdentity {
            participant: publisher.participant,
            process_term: publisher.process_term,
        })
    }

    pub(super) async fn recovery_fault_publisher_is_current(
        &self,
        publisher: RecoveryFaultPublisher,
    ) -> Result<bool, RecoveryControlError> {
        if !self.recovery_process_lease_is_live() {
            return Ok(false);
        }
        let authority = self.process_lease_authority.get().ok_or_else(|| {
            RecoveryControlError::Conflict("process lease authority is not installed".into())
        })?;
        let deadline = tokio::time::Instant::now()
            .checked_add(RECOVERY_CONTROL_IO_TIMEOUT)
            .ok_or_else(|| {
                RecoveryControlError::Conflict(
                    "recovery fault process-lease deadline overflow".into(),
                )
            })?;
        let current = authority
            .verify_current_participant_term(
                publisher.participant,
                publisher.process_term,
                deadline,
            )
            .await
            .map_err(RecoveryControlError::from_process_authority)?;
        Ok(current && self.recovery_process_lease_is_live())
    }

    pub(super) async fn recovery_incarnation_is_current(&self) -> Result<bool, String> {
        let Some(raw) = self
            .read_recovery_value(self.instance_id, RECOVERY_INCARNATION_KEY)
            .await?
        else {
            return Ok(false);
        };
        let observed = Uuid::parse_str(&raw)
            .map_err(|error| format!("invalid local recovery incarnation: {error}"))?;
        Ok(observed == self.recovery_incarnation)
    }

    /// Resolve the exact live boot identity for every canonical participant.
    ///
    /// # Errors
    /// Fails closed on a missing, malformed, nil, duplicate, or unexpected participant identity.
    pub async fn recovery_participant_incarnations(
        &self,
        participants: &[u64],
    ) -> Result<Vec<CheckpointParticipant>, String> {
        let available = self
            .available_recovery_participant_incarnations(participants)
            .await?;
        if available.len() != participants.len() {
            let available_ids: std::collections::BTreeSet<u64> = available
                .iter()
                .map(|participant| participant.node_id)
                .collect();
            let missing = participants
                .iter()
                .find(|node_id| !available_ids.contains(node_id))
                .copied()
                .unwrap_or(0);
            return Err(format!(
                "node {missing} has no current recovery incarnation"
            ));
        }
        Ok(available)
    }

    /// Resolve every currently readable boot identity among a canonical candidate set. Missing
    /// candidates are omitted so placement can exclude lease-revoked or not-yet-started nodes
    /// without weakening exact checkpoint/recovery roster validation.
    ///
    /// # Errors
    /// Fails closed on malformed input, a durable scan error, or a malformed/duplicate identity.
    pub async fn available_recovery_participant_incarnations(
        &self,
        participants: &[u64],
    ) -> Result<Vec<CheckpointParticipant>, String> {
        if participants.is_empty()
            || participants.contains(&0)
            || participants.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err("recovery incarnation roster requires canonical participants".into());
        }
        let expected: std::collections::BTreeSet<u64> = participants.iter().copied().collect();
        let mut reported = std::collections::BTreeMap::new();
        for (node, raw) in self.scan_recovery_values(RECOVERY_INCARNATION_KEY).await? {
            if !expected.contains(&node.0) {
                continue;
            }
            let incarnation = Uuid::parse_str(&raw).map_err(|error| {
                format!("invalid recovery incarnation published by {node}: {error}")
            })?;
            if incarnation.is_nil() {
                return Err(format!("nil recovery incarnation published by {node}"));
            }
            if reported.insert(node.0, incarnation).is_some() {
                return Err(format!(
                    "duplicate recovery incarnation published by {node}"
                ));
            }
        }
        Ok(participants
            .iter()
            .filter_map(|node_id| {
                reported
                    .get(node_id)
                    .copied()
                    .map(|incarnation| CheckpointParticipant {
                        node_id: *node_id,
                        boot_incarnation: incarnation,
                    })
            })
            .collect())
    }

    /// Whether every current assignment-owner boot identity still equals the frozen round.
    ///
    /// # Errors
    /// Returns an error when the current incarnation roster is unavailable or malformed.
    pub async fn recovery_incarnations_match(&self, round: &RecoveryRound) -> Result<bool, String> {
        Ok(self
            .recovery_participant_incarnations(&round.assignment_fence.participant_ids())
            .await?
            == round.assignment_fence.participants)
    }

    /// Whether every owner and evidence reporter still has the boot identity frozen for stopping.
    ///
    /// This check belongs only to the Prepare/stopped-evidence boundary. Evidence reporters do not
    /// join restore or release liveness quorums after their stopped reports are durable.
    ///
    /// # Errors
    /// Returns an error when the current incarnation roster is unavailable or malformed.
    pub async fn recovery_stopped_incarnations_match(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, RecoveryControlError> {
        self.recovery_roster_incarnations_match_control(&round.stopped_roster())
            .await
    }

    pub(super) async fn recovery_incarnations_match_control(
        &self,
        round: &RecoveryRound,
    ) -> Result<bool, RecoveryControlError> {
        self.recovery_roster_incarnations_match_control(&round.assignment_fence.participants)
            .await
    }

    pub(super) async fn recovery_roster_incarnations_match_control(
        &self,
        roster: &[CheckpointParticipant],
    ) -> Result<bool, RecoveryControlError> {
        let expected: std::collections::BTreeSet<u64> = roster
            .iter()
            .map(|participant| participant.node_id)
            .collect();
        let mut reported = std::collections::BTreeMap::new();
        for (node, raw) in self
            .scan_recovery_values(RECOVERY_INCARNATION_KEY)
            .await
            .map_err(RecoveryControlError::Uncertain)?
        {
            if !expected.contains(&node.0) {
                continue;
            }
            let incarnation = Uuid::parse_str(&raw).map_err(|error| {
                RecoveryControlError::Conflict(format!(
                    "invalid recovery incarnation published by {node}: {error}"
                ))
            })?;
            if incarnation.is_nil() || reported.insert(node.0, incarnation).is_some() {
                return Err(RecoveryControlError::Conflict(format!(
                    "noncanonical recovery incarnation published by {node}"
                )));
            }
        }
        if reported.len() != roster.len() {
            return Ok(false);
        }
        Ok(roster.iter().all(|participant| {
            reported.get(&participant.node_id) == Some(&participant.boot_incarnation)
        }))
    }

    pub(super) async fn read_recovery_fault_inventory_control(
        &self,
    ) -> Result<RecoveryFaultInventory, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .recovery_fault_inventory()
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Atomically observed shared-authority recovery-fault inventory.
    ///
    /// # Errors
    /// Returns an error when the leader authority is unavailable or malformed.
    pub async fn read_recovery_fault_inventory(&self) -> Result<RecoveryFaultInventory, String> {
        self.read_recovery_fault_inventory_control()
            .await
            .map_err(|error| error.to_string())
    }

    /// Coherent committed-release and fault view from shared recovery authority.
    ///
    /// # Errors
    /// Returns a classified error when the authority or immutable terminal cannot be validated.
    pub async fn read_recovery_admission_snapshot(
        &self,
    ) -> Result<RecoveryAdmissionSnapshot, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .recovery_admission_snapshot()
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Confirm this recovery view still has the same terminal, no active faults, and the exact
    /// audited leader term.
    ///
    /// # Errors
    /// Returns a classified error when the current shared authority cannot be validated.
    pub async fn recovery_admission_is_current(
        &self,
        snapshot: &RecoveryAdmissionSnapshot,
        leader_proof: &LeaderProof,
    ) -> Result<bool, RecoveryControlError> {
        self.checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .recovery_admission_is_current(snapshot, leader_proof)
            .await
            .map_err(RecoveryControlError::from_authority)
    }

    /// Publish this process's fault request so the leader drives a recovery round.
    ///
    /// # Errors
    /// Fails when the process lease is stale or the request cannot be ordered in shared authority.
    pub async fn report_fault(
        &self,
        request: RecoveryFaultRequest,
    ) -> Result<RecoveryFaultReportOutcome, String> {
        self.report_fault_with_disposition(request, RecoveryFaultDisposition::Recoverable)
            .await
    }

    /// Publish a deterministic fault that automatic recovery must never consume.
    ///
    /// The durable marker cannot be downgraded by a later process or ordinary fault report. The
    /// current operational reset boundary is replacement of the cluster authority namespace.
    ///
    /// # Errors
    /// Fails when the process lease is stale or the request cannot be ordered in shared authority.
    pub async fn report_terminal_fault(
        &self,
        request: RecoveryFaultRequest,
    ) -> Result<RecoveryFaultReportOutcome, String> {
        self.report_fault_with_disposition(request, RecoveryFaultDisposition::Terminal)
            .await
    }

    pub(super) async fn report_fault_with_disposition(
        &self,
        request: RecoveryFaultRequest,
        disposition: RecoveryFaultDisposition,
    ) -> Result<RecoveryFaultReportOutcome, String> {
        let seq = request.sequence();
        let _guard = self.recovery_writes.lock().await;
        let publisher = self.recovery_fault_publisher()?;
        if !self
            .recovery_fault_publisher_is_current(publisher)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("stable node process lease is no longer current".into());
        }
        let authority = self
            .checkpoint_authority()
            .map_err(|error| error.to_string())?;
        let result =
            Box::pin(authority.record_recovery_fault_with_disposition(publisher, seq, disposition))
                .await
                .map_err(|error| RecoveryControlError::from_authority(error).to_string())?;
        if !self
            .recovery_fault_publisher_is_current(publisher)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("stable node process lease changed while publishing recovery fault".into());
        }
        match result {
            super::super::leader_lease::RecordRecoveryFaultResult::Active => {
                Ok(RecoveryFaultReportOutcome::Active)
            }
            super::super::leader_lease::RecordRecoveryFaultResult::AlreadyCleared => {
                Ok(RecoveryFaultReportOutcome::AlreadyCleared)
            }
            super::super::leader_lease::RecordRecoveryFaultResult::CoveredByNewerRequest => {
                Ok(RecoveryFaultReportOutcome::CoveredByNewerRequest)
            }
            super::super::leader_lease::RecordRecoveryFaultResult::TerminalFenceActive => {
                Ok(RecoveryFaultReportOutcome::TerminalFenceActive)
            }
            super::super::leader_lease::RecordRecoveryFaultResult::Superseded => {
                Err("recovery fault request was superseded by a newer local process".into())
            }
        }
    }

    /// Validate this process's atomically released fault while retaining the local fault-write
    /// fence. The caller must hold the returned guard through its source-gate transition.
    ///
    /// `Ok(None)` means the terminal is no longer current, a newer active fault exists anywhere,
    /// or the local active/tombstoned slot does not match it. An exact tombstone is idempotent
    /// success only while the shared fault inventory remains settled.
    ///
    /// # Errors
    /// Returns an error for a nonterminal release, malformed state, or failed durable I/O.
    pub async fn begin_recovery_release(
        &self,
        terminal: &RecoveryAnnouncement,
    ) -> Result<Option<RecoveryReleaseGuard<'_>>, RecoveryControlError> {
        terminal
            .validate()
            .map_err(RecoveryControlError::Conflict)?;
        if !matches!(terminal.phase, RecoverPhase::ReleaseCommitted { .. }) {
            return Err(RecoveryControlError::Conflict(
                "recovery fault consumption requires a terminal Release".into(),
            ));
        }
        let guard = self.recovery_writes.lock().await;
        let publisher = self
            .recovery_fault_publisher()
            .map_err(RecoveryControlError::Conflict)?;
        if !self.recovery_fault_publisher_is_current(publisher).await? {
            return Err(RecoveryControlError::Superseded(
                "stable node process lease is no longer current".into(),
            ));
        }
        let authorized = self
            .checkpoint_authority()
            .map_err(|error| RecoveryControlError::Conflict(error.to_string()))?
            .authorize_recovery_release(publisher, terminal)
            .await
            .map_err(RecoveryControlError::from_authority)?;
        if !authorized {
            return Ok(None);
        }
        if !self.recovery_fault_publisher_is_current(publisher).await? {
            return Err(RecoveryControlError::Superseded(
                "stable node process lease changed while authorizing recovery Release".into(),
            ));
        }
        Ok(Some(RecoveryReleaseGuard {
            _write_guard: guard,
        }))
    }

    /// This stable node's active durable fault sequence, when present.
    ///
    /// # Errors
    /// Returns an error when shared authority is unavailable or malformed.
    pub async fn read_local_fault_report_control(
        &self,
    ) -> Result<Option<u64>, RecoveryControlError> {
        Ok(self
            .read_local_recovery_fault_control()
            .await?
            .map(|fault| fault.sequence))
    }

    /// This stable node's complete active durable fault, including its terminal disposition.
    ///
    /// # Errors
    /// Returns an error when shared authority is unavailable or malformed.
    pub async fn read_local_recovery_fault_control(
        &self,
    ) -> Result<Option<RecoveryFault>, RecoveryControlError> {
        Ok(self
            .read_recovery_fault_inventory_control()
            .await?
            .faults
            .into_iter()
            .find(|fault| fault.reporter == self.instance_id))
    }

    /// This process's durable nonzero fault report with a display-stable error.
    ///
    /// # Errors
    /// Returns an error when the point read fails or the local slot is malformed.
    pub async fn read_local_fault_report(&self) -> Result<Option<u64>, String> {
        self.read_local_fault_report_control()
            .await
            .map_err(|error| error.to_string())
    }

    /// Each active stable-node report and its globally monotonic authority sequence.
    ///
    /// # Errors
    /// Returns an error when shared authority is unavailable or malformed.
    pub async fn read_fault_reports(&self) -> Result<Vec<(NodeId, u64)>, String> {
        Ok(self
            .read_recovery_fault_inventory()
            .await?
            .faults
            .into_iter()
            .map(|fault| (fault.reporter, fault.sequence))
            .collect())
    }

    pub(super) async fn audit_recovery_faults_control(
        &self,
        round: &RecoveryRound,
    ) -> Result<(), RecoveryControlError> {
        let inventory = self.read_recovery_fault_inventory_control().await?;
        if inventory.revision == round.fault_revision && inventory.faults == round.faults {
            Ok(())
        } else {
            Err(RecoveryControlError::Superseded(
                "recovery fault set changed before Release commit".into(),
            ))
        }
    }

    pub(super) async fn audit_pending_release_faults_control(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<(), RecoveryControlError> {
        let release_id =
            RecoveryReleaseId::for_pending(release).map_err(RecoveryControlError::Conflict)?;
        let mut cached = self.pending_release_fault_audit.lock().await;
        let now = tokio::time::Instant::now();
        if cached
            .as_ref()
            .is_some_and(|(audited, valid_until)| audited == &release_id && now < *valid_until)
        {
            return Ok(());
        }
        self.audit_recovery_faults_control(&release.round).await?;
        *cached = Some((
            release_id,
            tokio::time::Instant::now() + PENDING_RELEASE_FAULT_AUDIT_INTERVAL,
        ));
        Ok(())
    }

    /// Publish that this node restored the exact frozen recovery round.
    ///
    /// # Errors
    /// Returns an error for an invalid round or when this node is outside its frozen quorum.
    pub async fn announce_recovered(&self, start: &RecoveryAnnouncement) -> Result<(), String> {
        start.validate()?;
        if !matches!(start.phase, RecoverPhase::Start { .. }) {
            return Err("restore acknowledgement must bind a Start target".into());
        }
        if !start.round.contains_owner(self.instance_id) {
            return Err("node outside recovery quorum cannot acknowledge restore".into());
        }
        if start.round.owner_incarnation(self.instance_id) != Some(self.recovery_incarnation) {
            return Err("restore acknowledgement has a stale local process incarnation".into());
        }
        if !self.recovery_incarnation_is_current().await? {
            return Err("restore acknowledgement came from a superseded local process".into());
        }
        let encoded = serde_json::to_string(&RecoveryAnnouncementAck {
            announcement: start.clone(),
            incarnation: self.recovery_incarnation,
        })
        .map_err(|error| format!("could not encode recovery ack: {error}"))?;
        self.write_recovery_value_exact("control:recovered", encoded)
            .await
    }

    /// Each visible node's exact restored recovery round.
    ///
    /// # Errors
    /// Fails closed when any visible acknowledgement is malformed.
    pub async fn read_recovered(&self) -> Result<Vec<(NodeId, RecoveryAnnouncement)>, String> {
        self.read_recovery_announcement_map("control:recovered")
            .await
    }

    /// Publish that this exact process is prepared for the pending release intent.
    ///
    /// Readiness is published only after local recovery state, shuffle loss accounting, and
    /// assignment transport authority are installed while source intake remains closed.
    ///
    /// # Errors
    /// Returns an error for a non-Release phase, changed local fault, or stale process.
    pub async fn announce_release_ready(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<(), RecoveryControlError> {
        let release_id =
            RecoveryReleaseId::for_pending(release).map_err(RecoveryControlError::Conflict)?;
        if release.round.owner_incarnation(self.instance_id) != Some(self.recovery_incarnation) {
            return Err(RecoveryControlError::Superseded(
                "release readiness has a stale local process incarnation".into(),
            ));
        }
        if !self
            .recovery_incarnation_is_current()
            .await
            .map_err(RecoveryControlError::Uncertain)?
        {
            return Err(RecoveryControlError::Superseded(
                "release readiness came from a superseded local process".into(),
            ));
        }
        if self.read_local_fault_report_control().await?
            != release.round.fault_sequence(self.instance_id)
        {
            return Err(RecoveryControlError::Superseded(
                "local fault set changed before release readiness".into(),
            ));
        }
        match self.observe_recover_control().await? {
            Some(active) if active == *release => {}
            _ => {
                return Err(RecoveryControlError::Superseded(
                    "release readiness no longer matches the active intent".into(),
                ));
            }
        }
        let participant = CheckpointParticipant {
            node_id: self.instance_id.0,
            boot_incarnation: self.recovery_incarnation,
        };
        let encoded = encode_release_ready_ack(&RecoveryReleaseReadyAck {
            release: release_id,
            participant,
        })
        .map_err(RecoveryControlError::Conflict)?;
        self.write_recovery_value_exact(RELEASE_READY_ACK_KEY, encoded)
            .await
            .map_err(RecoveryControlError::Uncertain)
    }

    /// Point-read the exact frozen owner roster's compact readiness records.
    ///
    /// Unrelated visible nodes are never scanned. Older records count as missing; malformed,
    /// same-generation divergent, or newer records are explicit conflicts.
    ///
    /// # Errors
    /// Returns an error only when an exact point read is transport-uncertain.
    pub(super) async fn read_release_ready(
        &self,
        release: &RecoveryAnnouncement,
    ) -> Result<ReleaseReadyStatus, RecoveryControlError> {
        let expected =
            RecoveryReleaseId::for_pending(release).map_err(RecoveryControlError::Conflict)?;
        let reads = futures::stream::iter(
            release
                .round
                .assignment_fence
                .participants
                .iter()
                .copied()
                .map(|participant| async move {
                    let value = self
                        .read_recovery_value(NodeId(participant.node_id), RELEASE_READY_ACK_KEY)
                        .await;
                    (participant, value)
                }),
        )
        .buffer_unordered(CONTROL_ROSTER_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut reads = reads;
        reads.sort_unstable_by_key(|(participant, _)| participant.node_id);
        let mut missing = Vec::new();
        for (participant, value) in reads {
            let raw = value.map_err(RecoveryControlError::Uncertain)?;
            let Some(raw) = raw else {
                missing.push(NodeId(participant.node_id));
                continue;
            };
            let ack = parse_release_ready_ack(&raw, NodeId(participant.node_id))
                .map_err(RecoveryControlError::Conflict)?;
            if ack.participant != participant {
                return Err(RecoveryControlError::Conflict(format!(
                    "release readiness from {} does not match the frozen process",
                    participant.node_id
                )));
            }
            if ack.release != expected {
                if ack.release.generation < expected.generation {
                    missing.push(NodeId(participant.node_id));
                    continue;
                }
                if ack.release.generation > expected.generation {
                    return Err(RecoveryControlError::Superseded(format!(
                        "release readiness from {} has newer generation {}",
                        participant.node_id, ack.release.generation
                    )));
                }
                return Err(RecoveryControlError::Conflict(format!(
                    "release readiness from {} conflicts with generation {}",
                    participant.node_id, expected.generation
                )));
            }
        }
        if !missing.is_empty() {
            return Ok(ReleaseReadyStatus::Pending { missing });
        }
        Ok(ReleaseReadyStatus::Complete)
    }
}
