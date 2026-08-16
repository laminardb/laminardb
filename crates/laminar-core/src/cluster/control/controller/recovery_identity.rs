//! Durable recovery values, generations, incarnations, and local authority evidence.

use super::*;

impl ClusterController {
    pub(super) async fn write_recovery_value(
        &self,
        key: &str,
        value: String,
    ) -> Result<(), String> {
        if !self.process_lease_is_live() {
            return Err("stable node process lease is no longer live".into());
        }
        tokio::time::timeout(
            RECOVERY_CONTROL_IO_TIMEOUT,
            self.recovery_kv.write_checked(key, value),
        )
        .await
        .map_err(|_| format!("recovery control write for {key} timed out"))?
        .map_err(|error| format!("recovery control write for {key} failed: {error}"))
    }

    pub(super) async fn write_recovery_value_exact(
        &self,
        key: &str,
        value: String,
    ) -> Result<(), String> {
        self.write_recovery_value(key, value.clone()).await?;
        let observed = self
            .read_recovery_value(self.instance_id, key)
            .await?
            .ok_or_else(|| format!("recovery control value for {key} vanished after write"))?;
        if observed != value {
            return Err(format!(
                "recovery control read-back mismatch for {key}; write was not durable"
            ));
        }
        Ok(())
    }

    pub(super) async fn read_recovery_value(
        &self,
        node: NodeId,
        key: &str,
    ) -> Result<Option<String>, String> {
        tokio::time::timeout(
            RECOVERY_CONTROL_IO_TIMEOUT,
            self.recovery_kv.read_from_checked(node, key),
        )
        .await
        .map_err(|_| format!("recovery control read for {key} from {node} timed out"))?
        .map_err(|error| format!("recovery control read for {key} from {node} failed: {error}"))
    }

    pub(super) async fn scan_recovery_values(
        &self,
        key: &str,
    ) -> Result<Vec<(NodeId, String)>, String> {
        tokio::time::timeout(
            RECOVERY_CONTROL_IO_TIMEOUT,
            self.recovery_kv.scan_checked(key),
        )
        .await
        .map_err(|_| format!("recovery control scan for {key} timed out"))?
        .map_err(|error| format!("recovery control scan for {key} failed: {error}"))
    }

    /// Highest durable recovery generation replicated by any visible participant.
    ///
    /// # Errors
    /// Fails closed when the durable scan is unavailable or contains malformed state.
    pub async fn max_recovery_generation(&self) -> Result<u64, String> {
        let mut maximum = 0;
        for (node, raw) in self.scan_recovery_values("control:recovery-gen").await? {
            let generation = raw.parse::<u64>().map_err(|error| {
                format!("invalid replicated recovery generation from {node}: {error}")
            })?;
            maximum = maximum.max(generation);
        }
        Ok(maximum)
    }

    /// Monotonically persist this participant's recovery generation and read it back exactly.
    ///
    /// # Errors
    /// Rejects zero, regression, unavailable durable storage, or a mismatched read-back.
    pub async fn adopt_recovery_generation(&self, generation: u64) -> Result<(), String> {
        if generation == 0 {
            return Err("recovery generation must be nonzero".into());
        }
        let current = self
            .read_recovery_value(self.instance_id, "control:recovery-gen")
            .await?
            .map(|raw| {
                raw.parse::<u64>()
                    .map_err(|error| format!("invalid local recovery generation: {error}"))
            })
            .transpose()?;
        if let Some(current) = current {
            if current > generation {
                return Err(format!(
                    "local recovery generation {current} is newer than proposed generation {generation}"
                ));
            }
            if current == generation {
                return Ok(());
            }
        }
        self.write_recovery_value_exact("control:recovery-gen", generation.to_string())
            .await
    }

    pub(super) async fn read_recovery_stopped_reports(
        &self,
        round: &RecoveryRound,
        roster: &[CheckpointParticipant],
    ) -> Result<Vec<RecoveryStoppedReport>, RecoveryControlError> {
        round.validate().map_err(RecoveryControlError::Conflict)?;
        if roster
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
            || roster.iter().any(|participant| {
                round.stopped_participant_incarnation(NodeId(participant.node_id))
                    != Some(participant.boot_incarnation)
            })
        {
            return Err(RecoveryControlError::Conflict(
                "recovery stopped-report read roster is not a canonical round subset".into(),
            ));
        }
        let reads = futures::stream::iter(roster.iter().copied().map(|participant| async move {
            let value = self
                .read_recovery_value(NodeId(participant.node_id), RECOVERY_STOPPED_REPORT_KEY)
                .await;
            (participant, value)
        }))
        .buffer_unordered(CONTROL_ROSTER_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        let mut reports = Vec::new();
        for (participant, value) in reads {
            let Some(raw) = value.map_err(RecoveryControlError::Uncertain)? else {
                continue;
            };
            let publisher = NodeId(participant.node_id);
            let report = parse_recovery_stopped_report_shape(&raw, publisher)
                .map_err(RecoveryControlError::Conflict)?;
            if report.round_id.generation < round.id.generation {
                continue;
            }
            if report.round_id.generation > round.id.generation {
                // A slot alone is not authority: corroborate that its exact publishing boot
                // durably adopted at least this generation. A stale or partially written newer
                // value remains pending for the old round instead of forcing abandon loops.
                let adopted = self
                    .read_recovery_value(publisher, "control:recovery-gen")
                    .await
                    .map_err(RecoveryControlError::Uncertain)?;
                let incarnation = self
                    .read_recovery_value(publisher, RECOVERY_INCARNATION_KEY)
                    .await
                    .map_err(RecoveryControlError::Uncertain)?;
                let Some((adopted, incarnation)) = adopted.zip(incarnation) else {
                    continue;
                };
                let adopted = adopted.parse::<u64>().map_err(|error| {
                    RecoveryControlError::Conflict(format!(
                        "invalid replicated recovery generation from {publisher}: {error}"
                    ))
                })?;
                let incarnation = Uuid::parse_str(&incarnation).map_err(|error| {
                    RecoveryControlError::Conflict(format!(
                        "invalid recovery incarnation published by {publisher}: {error}"
                    ))
                })?;
                if adopted >= report.round_id.generation
                    && incarnation == report.publisher.boot_incarnation
                {
                    reports.push(report);
                }
                continue;
            }
            report.validate_semantics(round).map_err(|error| {
                RecoveryControlError::Conflict(format!(
                    "same-generation recovery stopped report from {publisher} conflicts with the exact frozen round: {error}"
                ))
            })?;
            reports.push(report);
        }
        reports.sort_unstable_by_key(|report| report.publisher.node_id);
        Ok(reports)
    }

    pub(super) async fn read_recovery_announcement_map(
        &self,
        key: &str,
    ) -> Result<Vec<(NodeId, RecoveryAnnouncement)>, String> {
        let mut announcements = Vec::new();
        for (node, raw) in self.scan_recovery_values(key).await? {
            let announcement = parse_recovery_announcement_ack(&raw, node)?;
            announcements.push((node, announcement));
        }
        Ok(announcements)
    }

    /// This process's boot-unique recovery identity.
    #[must_use]
    pub fn recovery_incarnation(&self) -> Uuid {
        self.recovery_incarnation
    }

    /// Try to capture this process's live local identity without waiting or performing I/O.
    ///
    /// The process-authority transition lock is acquired with `try_lock`; checkpoint timing
    /// instrumentation can therefore fail closed instead of extending the measured pause. The
    /// lease is checked before and after the atomic identity load.
    ///
    /// # Errors
    /// Returns an error when an authority transition is in progress, the local lease is not live,
    /// or the installed process identity is non-canonical.
    pub fn try_live_local_process_authority_identity(
        &self,
    ) -> Result<LocalProcessAuthorityIdentity, String> {
        let _transition = self
            .process_authority_transition
            .try_lock()
            .ok_or_else(|| "local process authority transition is in progress".to_string())?;
        self.capture_live_local_process_authority_locked()
    }

    /// Read this process's exact local assignment evidence with one bounded checked-KV operation.
    ///
    /// Only the local stable-node slot is read; this method never scans shared assignment or
    /// recovery records. The process identity and sampled lease are captured before the read and
    /// revalidated immediately afterward. The retained report is then validated against the current
    /// boot, and identity is revalidated again around the sampled assignment fence. A canonical
    /// report from an older boot makes the view unavailable rather than being attributed to this
    /// process. A returned adoption also matches the exact locally audited assignment fence sampled
    /// before and after the final identity revalidation.
    ///
    /// # Errors
    /// Fails closed when the process term is unpublished, the lease is not live, the bounded
    /// durable read fails or times out, retained bytes are malformed or non-canonical, a
    /// current-boot record contradicts the local identity, or identity changes during the read.
    /// Checked-storage errors are unavailable even when a backend internally conflates a malformed
    /// outer envelope with I/O. `Invalid` is reserved for logical values returned successfully to
    /// this method that then fail payload bounds, canonicality, current-slot validation, or the
    /// sampled same-version audited fence.
    pub async fn read_local_process_authority_evidence(
        &self,
    ) -> Result<LocalProcessAuthorityEvidence, LocalProcessAuthorityEvidenceError> {
        let before = self
            .capture_live_local_process_authority()
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;
        let node = NodeId(before.participant.node_id);
        let adopted_raw = self
            .read_recovery_value(node, ADOPTED_ASSIGNMENT_KEY)
            .await
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;

        let after_read = self
            .capture_live_local_process_authority()
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;
        if after_read != before {
            return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                "local process authority changed while reading retained evidence".into(),
            ));
        }

        let adopted_raw = adopted_raw.ok_or_else(|| {
            LocalProcessAuthorityEvidenceError::Unavailable(
                "local process has no durable assignment adoption".into(),
            )
        })?;
        let adoption = parse_local_adopted_assignment(&adopted_raw, before.participant)
            .map_err(LocalProcessAuthorityEvidenceError::Invalid)?
            .ok_or_else(|| {
                LocalProcessAuthorityEvidenceError::Unavailable(
                    "durable assignment adoption belongs to a prior local boot".into(),
                )
            })?;
        let expected_fence = match self.checkpoint_assignment_fence(adoption.assignment_version) {
            Some(fence) if adoption.matches_fence(&fence) => fence,
            Some(_) => {
                return Err(LocalProcessAuthorityEvidenceError::Invalid(
                    "durable local adoption contradicts the same-version audited assignment fence"
                        .into(),
                ));
            }
            None => {
                return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                    "matching locally audited assignment fence is unavailable".into(),
                ));
            }
        };

        let after = self
            .capture_live_local_process_authority()
            .map_err(LocalProcessAuthorityEvidenceError::Unavailable)?;
        if after != before {
            return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                "local process authority changed while reading retained evidence".into(),
            ));
        }
        match self.checkpoint_assignment_fence(adoption.assignment_version) {
            Some(fence) if fence == expected_fence && adoption.matches_fence(&fence) => {}
            Some(_) => {
                return Err(LocalProcessAuthorityEvidenceError::Invalid(
                    "locally audited assignment fence changed or contradicted its durable adoption at the same version"
                        .into(),
                ));
            }
            None => {
                return Err(LocalProcessAuthorityEvidenceError::Unavailable(
                    "matching locally audited assignment fence changed during evidence capture"
                        .into(),
                ));
            }
        }

        Ok(LocalProcessAuthorityEvidence {
            participant: after.participant,
            process_term: after.process_term,
            adopted_assignment: adoption,
        })
    }

    /// Publish and read back this process's incarnation metadata.
    ///
    /// This does not grant recovery-write authority. Cluster startup must use
    /// [`Self::publish_leased_recovery_incarnation`] to bind the durable process term.
    ///
    /// # Errors
    /// Returns an error when the control write/read is unavailable or the read-back differs.
    pub async fn publish_recovery_incarnation(&self) -> Result<(), String> {
        self.write_recovery_value(
            RECOVERY_INCARNATION_KEY,
            self.recovery_incarnation.to_string(),
        )
        .await?;
        let observed = self
            .read_recovery_value(self.instance_id, RECOVERY_INCARNATION_KEY)
            .await?
            .ok_or_else(|| "recovery incarnation was not readable after publication".to_string())?;
        let observed = Uuid::parse_str(&observed)
            .map_err(|error| format!("invalid recovery incarnation after publication: {error}"))?;
        if observed != self.recovery_incarnation {
            return Err("recovery incarnation read-back mismatch".into());
        }
        Ok(())
    }

    /// Publish the recovery identity only when it matches an acquired stable-node lease.
    ///
    /// # Errors
    /// Rejects a lease for another node or boot identity, or a failed durable publication.
    pub async fn publish_leased_recovery_incarnation(
        &self,
        lease: &super::super::ProcessLease,
    ) -> Result<(), String> {
        lease
            .validate(self.instance_id)
            .map_err(|error| error.to_string())?;
        if lease.node != self.instance_id || lease.owner != self.recovery_incarnation {
            return Err("process lease does not bind this recovery incarnation".into());
        }
        if !self.recovery_process_lease_is_live() {
            return Err("live process lease deadline is not installed".into());
        }
        let publisher = RecoveryFaultPublisher {
            participant: CheckpointParticipant {
                node_id: lease.node.0,
                boot_incarnation: lease.owner,
            },
            process_term: lease.term,
        };
        if !self
            .recovery_fault_publisher_is_current(publisher)
            .await
            .map_err(|error| error.to_string())?
        {
            return Err("process lease is not the current durable stable-node term".into());
        }
        if !self.recovery_process_lease_is_live() {
            return Err("process lease changed before publishing recovery identity".into());
        }
        self.barrier.install_local_process_lease(lease)?;
        if let Err(error) = self.publish_recovery_incarnation().await {
            self.fence_process_lease();
            return Err(error);
        }
        if !self.recovery_process_lease_is_live() {
            self.fence_process_lease();
            return Err("process lease changed while publishing recovery identity".into());
        }
        match self.recovery_fault_publisher_is_current(publisher).await {
            Ok(true) if self.recovery_process_lease_is_live() => {}
            Ok(_) => {
                self.fence_process_lease();
                return Err("process lease changed while publishing recovery identity".into());
            }
            Err(error) => {
                self.fence_process_lease();
                return Err(format!(
                    "process lease authority became uncertain after recovery publication: {error}"
                ));
            }
        }
        self.recovery_process_term
            .store(lease.term, Ordering::Release);
        Ok(())
    }
}
