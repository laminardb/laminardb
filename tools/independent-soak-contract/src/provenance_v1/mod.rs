#![cfg_attr(not(test), allow(dead_code))]

//! Pure grouped-output identity and authority projection for the independent oracle.
//!
//! This module is intentionally disconnected from the LaminarDB runtime. It freezes the inputs a
//! future producer must receive without claiming that those inputs have a production lifecycle.

use std::fmt;

use sha2::{Digest, Sha256};

use super::wire_v1;

const OPERATION_ID_DOMAIN: &[u8; 44] = b"laminardb/grouped-count-sum/operation-id/v1\0";
const MAX_ID_LEN: usize = 128;
const MAX_SHARD_ID_LEN: usize = 64;
const MAX_ASSIGNMENT_PARTICIPANTS: usize = 129;

#[derive(Clone, Debug)]
pub(super) struct GroupedCountSumOperationIdContextV1 {
    invariant_prefix: Sha256,
}

impl GroupedCountSumOperationIdContextV1 {
    pub(super) fn new(
        deployment_uuid: &[u8; 16],
        pipeline_incarnation_id: &[u8; 16],
        pipeline_identity_sha256: &[u8; 32],
        sink_id: &str,
        operator_id: &str,
        output_id: &str,
    ) -> Result<Self, ProvenanceError> {
        require_nonzero(deployment_uuid, "deployment_uuid")?;
        require_nonzero(pipeline_incarnation_id, "pipeline_incarnation_id")?;
        require_nonzero(pipeline_identity_sha256, "pipeline_identity_sha256")?;
        validate_text(sink_id, MAX_ID_LEN, "sink_id")?;
        validate_text(operator_id, MAX_ID_LEN, "operator_id")?;
        validate_text(output_id, MAX_ID_LEN, "output_id")?;

        let mut invariant_prefix = Sha256::new();
        invariant_prefix.update(OPERATION_ID_DOMAIN);
        invariant_prefix.update(deployment_uuid);
        invariant_prefix.update(pipeline_incarnation_id);
        invariant_prefix.update(wire_v1::PIPELINE_IDENTITY_VERSION.to_be_bytes());
        invariant_prefix.update(pipeline_identity_sha256);
        update_u8_text(&mut invariant_prefix, sink_id);
        update_u8_text(&mut invariant_prefix, operator_id);
        update_u8_text(&mut invariant_prefix, output_id);
        Ok(Self { invariant_prefix })
    }

    pub(super) fn derive(
        &self,
        canonical_group_key: &[u8],
        checked_count: u64,
    ) -> Result<[u8; 32], ProvenanceError> {
        let key_len = u32::try_from(canonical_group_key.len())
            .map_err(|_| ProvenanceError::CanonicalGroupKeyTooLong)?;
        if !(1..=i64::MAX as u64).contains(&checked_count) {
            return Err(ProvenanceError::InvalidCheckedCount(checked_count));
        }

        let mut hasher = self.invariant_prefix.clone();
        hasher.update(key_len.to_be_bytes());
        hasher.update(canonical_group_key);
        hasher.update(checked_count.to_be_bytes());
        Ok(hasher.finalize().into())
    }
}

fn update_u8_text(hasher: &mut Sha256, value: &str) {
    hasher.update([value.len() as u8]);
    hasher.update(value.as_bytes());
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct PipelineIdentityRef<'a> {
    pub deployment_uuid: &'a [u8; 16],
    pub pipeline_incarnation_id: &'a [u8; 16],
    pub pipeline_identity_version: u16,
    pub pipeline_identity_sha256: &'a [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct VnodeOwnerRef<'a> {
    pub vnode: u16,
    pub node_id: u64,
    pub boot_uuid: &'a [u8; 16],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AssignmentParticipantRef<'a> {
    pub node_id: u64,
    pub boot_uuid: &'a [u8; 16],
}

#[derive(Clone, Copy, Debug)]
pub(super) struct AssignmentCertificateView<'a> {
    pub version: u64,
    pub certificate_sha256: &'a [u8; 32],
    pub vnode_count: u16,
    /// Complete certificate roster in ascending vnode order.
    pub owners: &'a [VnodeOwnerRef<'a>],
    /// Exact certificate participants in strictly ascending node-ID order.
    pub participants: &'a [AssignmentParticipantRef<'a>],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ProcessLeaseView<'a> {
    pub node_id: u64,
    pub boot_uuid: &'a [u8; 16],
    pub durable_process_term: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RecoveryTerminal {
    Commit,
    Abort,
    Pending,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RecoveryCheckpointView<'a> {
    pub immutable: bool,
    pub terminal: RecoveryTerminal,
    pub identity: PipelineIdentityRef<'a>,
    pub epoch: u64,
    pub checkpoint_id: u64,
    pub committed_index_sha256: &'a [u8; 32],
    pub base_assignment_version: u64,
    pub base_assignment_certificate_sha256: &'a [u8; 32],
}

#[derive(Clone, Copy, Debug)]
pub(super) struct WriterIntervalInput<'a> {
    pub current_interval_id: &'a [u8; 16],
    pub predecessor_interval_id: Option<&'a [u8; 16]>,
    pub claimed_writer: ProcessLeaseView<'a>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct OutputMarkerInput<'a> {
    pub identity: PipelineIdentityRef<'a>,
    pub current_assignment: AssignmentCertificateView<'a>,
    pub current_process: Option<ProcessLeaseView<'a>>,
    pub recovery: RecoveryCheckpointView<'a>,
    pub interval: WriterIntervalInput<'a>,
    pub topology_sha256: &'a [u8; 32],
    pub sink_id: &'a str,
    pub operator_id: &'a str,
    pub output_id: &'a str,
    pub shard_id: &'a str,
    /// Exact shard plan in strictly ascending vnode order.
    pub planned_vnodes: &'a [u16],
}

#[derive(Clone, Copy, Debug)]
pub(super) struct PreparedOutputAuthorityV1<'a> {
    current_interval_id: &'a [u8; 16],
    predecessor_interval_id: Option<&'a [u8; 16]>,
    identity: PipelineIdentityRef<'a>,
    current_assignment_version: u64,
    current_assignment_certificate_sha256: &'a [u8; 32],
    current_process: ProcessLeaseView<'a>,
    recovery: RecoveryCheckpointView<'a>,
    topology_sha256: &'a [u8; 32],
    sink_id: &'a str,
    operator_id: &'a str,
    output_id: &'a str,
    shard_id: &'a str,
    vnode_count: u16,
    vnode_bitmap: &'a [u8],
}

impl PreparedOutputAuthorityV1<'_> {
    pub(super) fn marker_ref(&self) -> wire_v1::MarkerRef<'_> {
        wire_v1::MarkerRef {
            current_interval_id: self.current_interval_id,
            predecessor_interval_id: self.predecessor_interval_id,
            deployment_uuid: self.identity.deployment_uuid,
            pipeline_incarnation_id: self.identity.pipeline_incarnation_id,
            pipeline_identity_version: self.identity.pipeline_identity_version,
            pipeline_identity_sha256: self.identity.pipeline_identity_sha256,
            key_to_vnode_abi_version: wire_v1::KEY_TO_VNODE_ABI_VERSION,
            sink_partitioning_abi_version: wire_v1::SINK_PARTITIONING_ABI_VERSION,
            vnode_count: self.vnode_count,
            current_assignment_version: self.current_assignment_version,
            current_assignment_sha256: self.current_assignment_certificate_sha256,
            writer_node_id: self.current_process.node_id,
            writer_boot_uuid: self.current_process.boot_uuid,
            durable_process_term: self.current_process.durable_process_term,
            recovery_epoch: self.recovery.epoch,
            recovery_checkpoint_id: self.recovery.checkpoint_id,
            committed_index_sha256: self.recovery.committed_index_sha256,
            recovery_base_assignment_version: self.recovery.base_assignment_version,
            recovery_base_assignment_sha256: self.recovery.base_assignment_certificate_sha256,
            topology_sha256: self.topology_sha256,
            sink_id: self.sink_id,
            operator_id: self.operator_id,
            output_id: self.output_id,
            shard_id: self.shard_id,
            vnode_bitmap: self.vnode_bitmap,
        }
    }

    pub(super) fn project_data_header<'a>(
        &'a self,
        operation_id: &'a [u8; 32],
        admission_sequence: u64,
    ) -> Result<wire_v1::DataHeaderRef<'a>, ProvenanceError> {
        require_nonzero(operation_id, "operation_id")?;
        Ok(wire_v1::DataHeaderRef {
            operation_id,
            writer_interval_id: self.current_interval_id,
            admission_sequence,
        })
    }
}

pub(super) fn prepare_output_authority_v1<'a>(
    input: OutputMarkerInput<'a>,
    vnode_bitmap: &'a mut [u8],
) -> Result<PreparedOutputAuthorityV1<'a>, ProvenanceError> {
    validate_pipeline_identity(input.identity)?;
    validate_text(input.sink_id, MAX_ID_LEN, "sink_id")?;
    validate_text(input.operator_id, MAX_ID_LEN, "operator_id")?;
    validate_text(input.output_id, MAX_ID_LEN, "output_id")?;
    validate_text(input.shard_id, MAX_SHARD_ID_LEN, "shard_id")?;
    require_nonzero(input.topology_sha256, "topology_sha256")?;
    validate_assignment(input.current_assignment)?;

    let expected_bitmap_len = usize::from(input.current_assignment.vnode_count).div_ceil(8);
    if vnode_bitmap.len() != expected_bitmap_len {
        return Err(ProvenanceError::InvalidVnodeBitmapLength {
            expected: expected_bitmap_len,
            actual: vnode_bitmap.len(),
        });
    }

    let current_process = input
        .current_process
        .ok_or(ProvenanceError::MissingCurrentProcess)?;
    validate_process(current_process)?;
    validate_process(input.interval.claimed_writer)?;
    if current_process != input.interval.claimed_writer {
        return Err(ProvenanceError::ClaimedWriterMismatch);
    }

    validate_recovery(input.identity, input.recovery, input.current_assignment)?;
    require_nonzero(input.interval.current_interval_id, "current_interval_id")?;
    if let Some(predecessor) = input.interval.predecessor_interval_id {
        require_nonzero(predecessor, "predecessor_interval_id")?;
        if predecessor == input.interval.current_interval_id {
            return Err(ProvenanceError::SelfPredecessorInterval);
        }
    }

    if input.planned_vnodes.is_empty() {
        return Err(ProvenanceError::EmptyPlannedVnodes);
    }
    vnode_bitmap.fill(0);
    let mut previous = None;
    for vnode in input.planned_vnodes {
        if previous.is_some_and(|prior| prior >= *vnode) {
            return Err(ProvenanceError::NonCanonicalPlannedVnodes);
        }
        previous = Some(*vnode);
        let owner = input
            .current_assignment
            .owners
            .get(usize::from(*vnode))
            .ok_or(ProvenanceError::PlannedVnodeOutOfRange(*vnode))?;
        if owner.vnode != *vnode {
            return Err(ProvenanceError::AssignmentRosterNotCanonical);
        }
        if owner.node_id != current_process.node_id || owner.boot_uuid != current_process.boot_uuid
        {
            return Err(ProvenanceError::WriterDoesNotOwnPlannedVnode(*vnode));
        }
        vnode_bitmap[usize::from(*vnode) / 8] |= 1 << (*vnode % 8);
    }

    Ok(PreparedOutputAuthorityV1 {
        current_interval_id: input.interval.current_interval_id,
        predecessor_interval_id: input.interval.predecessor_interval_id,
        identity: input.identity,
        current_assignment_version: input.current_assignment.version,
        current_assignment_certificate_sha256: input.current_assignment.certificate_sha256,
        current_process,
        recovery: input.recovery,
        topology_sha256: input.topology_sha256,
        sink_id: input.sink_id,
        operator_id: input.operator_id,
        output_id: input.output_id,
        shard_id: input.shard_id,
        vnode_count: input.current_assignment.vnode_count,
        vnode_bitmap,
    })
}

fn validate_pipeline_identity(identity: PipelineIdentityRef<'_>) -> Result<(), ProvenanceError> {
    require_nonzero(identity.deployment_uuid, "deployment_uuid")?;
    require_nonzero(identity.pipeline_incarnation_id, "pipeline_incarnation_id")?;
    if identity.pipeline_identity_version != wire_v1::PIPELINE_IDENTITY_VERSION {
        return Err(ProvenanceError::UnsupportedPipelineIdentityVersion(
            identity.pipeline_identity_version,
        ));
    }
    require_nonzero(
        identity.pipeline_identity_sha256,
        "pipeline_identity_sha256",
    )
}

fn validate_assignment(assignment: AssignmentCertificateView<'_>) -> Result<(), ProvenanceError> {
    if assignment.version == 0 {
        return Err(ProvenanceError::ZeroField("current_assignment_version"));
    }
    require_nonzero(
        assignment.certificate_sha256,
        "current_assignment_certificate_sha256",
    )?;
    if assignment.vnode_count == 0 || assignment.owners.len() != usize::from(assignment.vnode_count)
    {
        return Err(ProvenanceError::IncompleteAssignmentRoster);
    }
    if assignment.participants.is_empty()
        || assignment.participants.len() > MAX_ASSIGNMENT_PARTICIPANTS
        || assignment
            .participants
            .windows(2)
            .any(|pair| pair[0].node_id >= pair[1].node_id)
    {
        return Err(ProvenanceError::AssignmentParticipantsNotCanonical);
    }
    for participant in assignment.participants {
        if participant.node_id == 0 {
            return Err(ProvenanceError::ZeroField("assignment_participant_node_id"));
        }
        require_nonzero(participant.boot_uuid, "assignment_participant_boot_uuid")?;
    }
    let mut participant_seen = [false; MAX_ASSIGNMENT_PARTICIPANTS];
    for (expected_vnode, owner) in assignment.owners.iter().enumerate() {
        if usize::from(owner.vnode) != expected_vnode {
            return Err(ProvenanceError::AssignmentRosterNotCanonical);
        }
        if owner.node_id == 0 {
            return Err(ProvenanceError::ZeroField("assignment_owner_node_id"));
        }
        require_nonzero(owner.boot_uuid, "assignment_owner_boot_uuid")?;
        let participant_index = assignment
            .participants
            .binary_search_by_key(&owner.node_id, |participant| participant.node_id)
            .map_err(|_| ProvenanceError::AssignmentParticipantSetMismatch)?;
        participant_seen[participant_index] = true;
        let participant = &assignment.participants[participant_index];
        if owner.boot_uuid != participant.boot_uuid {
            return Err(ProvenanceError::AssignmentOwnerBootMismatch);
        }
    }
    if participant_seen[..assignment.participants.len()]
        .iter()
        .any(|seen| !seen)
    {
        return Err(ProvenanceError::AssignmentParticipantSetMismatch);
    }
    if assignment_certificate_digest(assignment) != *assignment.certificate_sha256 {
        return Err(ProvenanceError::AssignmentCertificateDigestMismatch);
    }
    Ok(())
}

/// Independent projection of `CheckpointAssignmentFence::owner_map_digest()` and `.digest()`.
/// Literal tests pin this copy to the production domains, endian order, and participant binding.
fn assignment_certificate_digest(assignment: AssignmentCertificateView<'_>) -> [u8; 32] {
    let vnode_count = u32::from(assignment.vnode_count);
    let mut owner_map = Sha256::new();
    owner_map.update(b"laminardb-vnode-owner-map-v2\0");
    owner_map.update(wire_v1::KEY_TO_VNODE_ABI_VERSION.to_le_bytes());
    owner_map.update(vnode_count.to_le_bytes());
    owner_map.update((assignment.owners.len() as u64).to_le_bytes());
    for owner in assignment.owners {
        owner_map.update(owner.node_id.to_le_bytes());
    }
    let owner_map_digest: [u8; 32] = owner_map.finalize().into();

    let mut certificate = Sha256::new();
    certificate.update(b"laminardb-checkpoint-assignment-v3\0");
    certificate.update(assignment.version.to_le_bytes());
    certificate.update(wire_v1::KEY_TO_VNODE_ABI_VERSION.to_le_bytes());
    certificate.update(vnode_count.to_le_bytes());
    certificate.update(owner_map_digest);
    certificate.update((assignment.participants.len() as u64).to_le_bytes());
    for participant in assignment.participants {
        certificate.update(participant.node_id.to_le_bytes());
        certificate.update(participant.boot_uuid);
    }
    certificate.finalize().into()
}

fn validate_process(process: ProcessLeaseView<'_>) -> Result<(), ProvenanceError> {
    if process.node_id == 0 {
        return Err(ProvenanceError::ZeroField("writer_node_id"));
    }
    require_nonzero(process.boot_uuid, "writer_boot_uuid")?;
    if process.durable_process_term == 0 {
        return Err(ProvenanceError::ZeroField("durable_process_term"));
    }
    Ok(())
}

fn validate_recovery(
    identity: PipelineIdentityRef<'_>,
    recovery: RecoveryCheckpointView<'_>,
    current_assignment: AssignmentCertificateView<'_>,
) -> Result<(), ProvenanceError> {
    if !recovery.immutable {
        return Err(ProvenanceError::RecoveryNotImmutable);
    }
    if recovery.terminal != RecoveryTerminal::Commit {
        return Err(ProvenanceError::RecoveryNotCommitted);
    }
    validate_pipeline_identity(recovery.identity)?;
    if recovery.identity != identity {
        return Err(ProvenanceError::RecoveryIdentityMismatch);
    }
    if recovery.epoch == 0
        || recovery.checkpoint_id == 0
        || recovery.epoch != recovery.checkpoint_id
    {
        return Err(ProvenanceError::InvalidRecoveryAttempt);
    }
    require_nonzero(recovery.committed_index_sha256, "committed_index_sha256")?;
    if recovery.base_assignment_version == 0 {
        return Err(ProvenanceError::ZeroField(
            "recovery_base_assignment_version",
        ));
    }
    require_nonzero(
        recovery.base_assignment_certificate_sha256,
        "recovery_base_assignment_certificate_sha256",
    )?;
    if current_assignment.version < recovery.base_assignment_version {
        return Err(ProvenanceError::CurrentAssignmentBeforeRecoveryBase);
    }
    if current_assignment.version == recovery.base_assignment_version
        && current_assignment.certificate_sha256 != recovery.base_assignment_certificate_sha256
    {
        return Err(ProvenanceError::EqualAssignmentVersionDigestMismatch);
    }
    Ok(())
}

fn validate_text(value: &str, maximum: usize, field: &'static str) -> Result<(), ProvenanceError> {
    if value.is_empty() || value.as_bytes().contains(&0) {
        return Err(ProvenanceError::InvalidText(field));
    }
    if value.len() > maximum {
        return Err(ProvenanceError::TextLimitExceeded(field));
    }
    Ok(())
}

fn require_nonzero(bytes: &[u8], field: &'static str) -> Result<(), ProvenanceError> {
    if bytes.iter().all(|byte| *byte == 0) {
        Err(ProvenanceError::ZeroField(field))
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ProvenanceError {
    ZeroField(&'static str),
    InvalidText(&'static str),
    TextLimitExceeded(&'static str),
    UnsupportedPipelineIdentityVersion(u16),
    CanonicalGroupKeyTooLong,
    InvalidCheckedCount(u64),
    InvalidVnodeBitmapLength { expected: usize, actual: usize },
    IncompleteAssignmentRoster,
    AssignmentRosterNotCanonical,
    AssignmentParticipantsNotCanonical,
    AssignmentParticipantSetMismatch,
    AssignmentOwnerBootMismatch,
    AssignmentCertificateDigestMismatch,
    MissingCurrentProcess,
    ClaimedWriterMismatch,
    RecoveryNotImmutable,
    RecoveryNotCommitted,
    RecoveryIdentityMismatch,
    InvalidRecoveryAttempt,
    CurrentAssignmentBeforeRecoveryBase,
    EqualAssignmentVersionDigestMismatch,
    SelfPredecessorInterval,
    EmptyPlannedVnodes,
    NonCanonicalPlannedVnodes,
    PlannedVnodeOutOfRange(u16),
    WriterDoesNotOwnPlannedVnode(u16),
}

impl fmt::Display for ProvenanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for ProvenanceError {}

#[cfg(test)]
mod tests;
