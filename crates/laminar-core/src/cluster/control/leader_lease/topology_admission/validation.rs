use super::*;

impl LeaderAuthorityRecord {
    pub(in crate::cluster::control::leader_lease) fn validate_topology_admission(
        &self,
    ) -> Result<(), LeaseError> {
        if self.version < TOPOLOGY_ADMISSION_RECORD_VERSION
            && (!self.topology_operations.is_empty() || self.assignment_drain_reservation.is_some())
        {
            return Err(LeaseError::Invalid(
                "admission evidence requires authority format 14".into(),
            ));
        }
        if self.topology_operations.len() > MAX_TOPOLOGY_OPERATIONS {
            return Err(LeaseError::Invalid(
                "topology request journal exceeds its fixed bound".into(),
            ));
        }
        let mut identities = BTreeSet::new();
        let mut planned = 0;
        let mut previous = 0;
        for operation in &self.topology_operations {
            if operation.preparation.as_ref().is_some_and(|preparation| {
                preparation.certificates.iter().any(|certificate| {
                    certificate.protocol_version
                        == crate::cluster::control::topology::TOPOLOGY_SUBMISSION_PROTOCOL_VERSION
                })
            }) && self.version < TOPOLOGY_SUBMISSION_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "complete topology capability requires authority format 23".into(),
                ));
            }
            operation
                .validate(self.lease.seq)
                .map_err(|e| LeaseError::Invalid(e.to_string()))?;
            if !identities.insert(operation.operation_id.get())
                || operation.admitted_sequence <= previous
                || self
                    .topology_baseline
                    .as_ref()
                    .is_some_and(|baseline| baseline.operation_id == operation.operation_id)
            {
                return Err(LeaseError::Invalid(
                    "topology request journal is not canonical".into(),
                ));
            }
            previous = operation.admitted_sequence;
            if operation
                .activation
                .as_ref()
                .is_some_and(|activation| activation.recovery_round.is_some())
                && self.version < TOPOLOGY_RECOVERY_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "topology-bound recovery requires authority format 22".into(),
                ));
            }
            if operation.activation.is_some() && self.version < TOPOLOGY_INSTALLATION_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "topology installation/Release requires authority format 21".into(),
                ));
            }
            if (operation.commit.is_some()
                || operation.target_preparations.iter().any(|receipt| {
                    receipt.protocol_version
                        == crate::cluster::control::topology::TOPOLOGY_COMMIT_PROTOCOL_VERSION
                }))
                && self.version < TOPOLOGY_COMMIT_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "topology Commit capability requires authority format 20".into(),
                ));
            }
            if !operation.target_preparations.is_empty()
                && self.version < TOPOLOGY_TARGET_PREPARATION_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "target preparation observations require authority format 19".into(),
                ));
            }
            if operation.migration_root.is_some()
                && self.version < TOPOLOGY_MIGRATION_ROOT_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "migration roots require authority format 17".into(),
                ));
            }
            if operation.cut.is_some() && self.version < TOPOLOGY_CUT_RECORD_VERSION {
                return Err(LeaseError::Invalid(
                    "checkpoint-bound topology requires authority format 15".into(),
                ));
            }
            if operation.preparation.is_some() && self.version < TOPOLOGY_PREPARATION_RECORD_VERSION
            {
                return Err(LeaseError::Invalid(
                    "participant preparation requires authority format 16".into(),
                ));
            }
            if operation.blocks_admission() {
                planned += 1;
                if operation.is_preparing() && !self.lease.matches_proof(&operation.admitted_by) {
                    return Err(LeaseError::Invalid(
                        "planned topology belongs to an obsolete term".into(),
                    ));
                }
                if let Some(cut) = &operation.cut {
                    if cut.committed.is_none()
                        && self.active_checkpoint_artifacts.as_ref() != Some(&cut.inventory)
                    {
                        return Err(LeaseError::Invalid(
                            "unsettled topology cut lost its admitted artifacts".into(),
                        ));
                    }
                }
            }
        }
        if let Some(reservation) = &self.assignment_drain_reservation {
            reservation
                .proposal
                .validate()
                .map_err(|e| LeaseError::Invalid(e.to_string()))?;
            if !reservation.transition.is_canonical()
                || reservation.proposal.version != reservation.transition.target.assignment_version
                || reservation.authority_sequence == 0
                || reservation.authority_sequence > self.lease.seq
            {
                return Err(LeaseError::Invalid(
                    "invalid assignment drain reservation".into(),
                ));
            }
        }
        if planned > 1
            || (planned != 0
                && (self.assignment_drain_reservation.is_some()
                    || self
                        .active_checkpoint_artifacts
                        .as_ref()
                        .is_some_and(|active| {
                            self.topology_operations
                                .iter()
                                .filter(|entry| entry.is_preparing())
                                .all(|entry| {
                                    entry
                                        .cut
                                        .as_ref()
                                        .is_none_or(|cut| cut.inventory != *active)
                                })
                        })
                    || (self.assignment_handoff_pin.is_some()
                        && !self.pending_topology_handoff_is_exact())
                    || (self
                        .topology_operations
                        .iter()
                        .any(TopologyAdmissionStatus::is_preparing)
                        && self.recovery_fault_slots.iter().any(|slot| slot.active))))
        {
            return Err(LeaseError::Invalid(
                "topology preparation overlaps incompatible authority".into(),
            ));
        }
        Ok(())
    }
}
