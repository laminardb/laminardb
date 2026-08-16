//! Bounded recovery-control wire encoding and validation.

use super::*;

pub(super) fn sha256_hex(encoded: &[u8]) -> String {
    use std::fmt::Write as _;

    let digest = Sha256::digest(encoded);
    let mut hex = String::with_capacity(64);
    for byte in digest {
        write!(&mut hex, "{byte:02x}").expect("writing to a String cannot fail");
    }
    hex
}

pub(super) fn recovery_round_sha256(round: &RecoveryRound) -> Result<String, String> {
    let encoded = serde_json::to_vec(&RecoveryStoppedRoundDigestInput {
        protocol: "laminardb-recovery-stopped-round-v3",
        round,
    })
    .map_err(|error| format!("could not encode recovery stopped round identity: {error}"))?;
    Ok(sha256_hex(&encoded))
}

pub(super) fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Bounded durable proof that one exact predecessor process reached the global source frontier
/// for an assignment handoff. The round binds the version, vnode-owner map, and complete
/// boot-incarnation roster; a version alone is not an assignment identity.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DrainAck {
    pub(super) protocol_version: u16,
    participant: CheckpointParticipant,
    pub(super) round: AssignmentDrainId,
}

impl DrainAck {
    pub(super) fn for_transition(
        participant: CheckpointParticipant,
        transition: &AssignmentDrainTransition,
    ) -> Self {
        Self {
            protocol_version: DRAIN_ACK_PROTOCOL_VERSION,
            participant,
            round: transition.id(),
        }
    }

    pub(super) fn is_canonical(&self) -> bool {
        self.protocol_version == DRAIN_ACK_PROTOCOL_VERSION
            && self.participant.node_id != 0
            && !self.participant.boot_incarnation.is_nil()
            && self.round.is_canonical()
    }

    pub(super) fn matches_transition(&self, transition: &AssignmentDrainTransition) -> bool {
        self.is_canonical()
            && self.round == transition.id()
            && transition
                .predecessor
                .participant_incarnation(self.participant.node_id)
                == Some(self.participant.boot_incarnation)
    }
}

pub(super) fn encode_drain_ack(ack: &DrainAck) -> Result<String, String> {
    if !ack.is_canonical() {
        return Err("drain acknowledgement is not canonical".into());
    }
    let encoded = serde_json::to_string(ack)
        .map_err(|error| format!("could not encode drain acknowledgement: {error}"))?;
    if encoded.len() > MAX_DRAIN_ACK_BYTES {
        return Err(format!(
            "drain acknowledgement is {} bytes; maximum is {MAX_DRAIN_ACK_BYTES}",
            encoded.len()
        ));
    }
    Ok(encoded)
}

pub(super) fn parse_drain_ack(raw: &str, publisher: NodeId) -> Result<DrainAck, String> {
    if raw.len() > MAX_DRAIN_ACK_BYTES {
        return Err(format!(
            "drain acknowledgement from {publisher} is {} bytes; maximum is {MAX_DRAIN_ACK_BYTES}",
            raw.len()
        ));
    }
    let ack: DrainAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid drain acknowledgement from {publisher}: {error}"))?;
    if !ack.is_canonical() || ack.participant.node_id != publisher.0 {
        return Err(format!(
            "drain acknowledgement from {publisher} has a non-canonical publisher"
        ));
    }
    if encode_drain_ack(&ack)? != raw {
        return Err(format!(
            "drain acknowledgement from {publisher} is not canonically encoded"
        ));
    }
    Ok(ack)
}

pub(super) fn parse_recovery_announcement(
    raw: &str,
) -> Result<Option<RecoveryAnnouncement>, String> {
    if raw.is_empty() {
        return Ok(None);
    }
    validate_recovery_announcement_size(raw.len())?;
    let announcement: RecoveryAnnouncement = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery announcement: {error}"))?;
    announcement.validate()?;
    if encode_recovery_announcement(&announcement)? != raw {
        return Err("recovery announcement is not canonically encoded".into());
    }
    Ok(Some(announcement))
}

pub(super) fn validate_recovery_announcement_size(encoded_len: usize) -> Result<(), String> {
    if encoded_len == 0 || encoded_len > MAX_RECOVERY_ANNOUNCEMENT_BYTES {
        return Err(format!(
            "recovery announcement is {encoded_len} bytes; maximum is {MAX_RECOVERY_ANNOUNCEMENT_BYTES}"
        ));
    }
    Ok(())
}

pub(super) fn encode_recovery_announcement(
    announcement: &RecoveryAnnouncement,
) -> Result<String, String> {
    announcement.validate()?;
    let encoded = serde_json::to_string(announcement)
        .map_err(|error| format!("could not encode recovery announcement: {error}"))?;
    validate_recovery_announcement_size(encoded.len())?;
    Ok(encoded)
}

pub(super) fn validate_stopped_report_size(encoded_len: usize) -> Result<(), String> {
    if encoded_len > MAX_RECOVERY_STOPPED_REPORT_BYTES {
        return Err(format!(
            "recovery stopped report is {encoded_len} bytes; maximum is {MAX_RECOVERY_STOPPED_REPORT_BYTES}"
        ));
    }
    Ok(())
}

pub(super) fn encode_recovery_stopped_report(
    report: &RecoveryStoppedReport,
    round: &RecoveryRound,
) -> Result<String, String> {
    report.validate_semantics(round)?;
    encode_recovery_stopped_report_shape(report)
}

fn encode_recovery_stopped_report_shape(report: &RecoveryStoppedReport) -> Result<String, String> {
    report.validate_shape()?;
    let encoded = serde_json::to_string(report)
        .map_err(|error| format!("could not encode recovery stopped report: {error}"))?;
    validate_stopped_report_size(encoded.len())?;
    Ok(encoded)
}

#[cfg(test)]
pub(super) fn parse_recovery_stopped_report(
    raw: &str,
    publisher: NodeId,
    round: &RecoveryRound,
) -> Result<RecoveryStoppedReport, String> {
    let report = parse_recovery_stopped_report_shape(raw, publisher)?;
    report.validate_semantics(round)?;
    Ok(report)
}

pub(super) fn parse_recovery_stopped_report_shape(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryStoppedReport, String> {
    validate_stopped_report_size(raw.len())?;
    let report: RecoveryStoppedReport = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery stopped report from {publisher}: {error}"))?;
    report.validate_shape()?;
    if report.publisher.node_id != publisher.0 {
        return Err(format!(
            "recovery stopped report from {publisher} names publisher {}",
            report.publisher.node_id
        ));
    }
    if encode_recovery_stopped_report_shape(&report)? != raw {
        return Err(format!(
            "recovery stopped report from {publisher} is not canonically encoded"
        ));
    }
    Ok(report)
}

pub(super) fn parse_recovery_announcement_ack(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryAnnouncement, String> {
    let ack: RecoveryAnnouncementAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid recovery phase acknowledgement: {error}"))?;
    ack.announcement.validate()?;
    if ack.announcement.round.owner_incarnation(publisher) != Some(ack.incarnation) {
        return Err(format!(
            "recovery phase acknowledgement from {publisher} has a stale process incarnation"
        ));
    }
    Ok(ack.announcement)
}

pub(super) fn parse_local_adopted_assignment(
    raw: &str,
    participant: CheckpointParticipant,
) -> Result<Option<CheckpointAssignmentAdoption>, String> {
    if raw.is_empty() || raw.len() > MAX_ADOPTED_ASSIGNMENT_BYTES {
        return Err(format!(
            "local adopted assignment is {} bytes; expected 1..={MAX_ADOPTED_ASSIGNMENT_BYTES}",
            raw.len()
        ));
    }
    let adoption: CheckpointAssignmentAdoption = serde_json::from_str(raw)
        .map_err(|error| format!("invalid local adopted assignment: {error}"))?;
    if !adoption.is_canonical() || adoption.participant.node_id != participant.node_id {
        return Err("local adopted assignment has a non-canonical publisher".into());
    }
    let canonical = serde_json::to_string(&adoption).map_err(|error| {
        format!("could not canonically encode local adopted assignment: {error}")
    })?;
    if canonical != raw {
        return Err("local adopted assignment is not canonically encoded".into());
    }
    if adoption.participant.boot_incarnation != participant.boot_incarnation {
        return Ok(None);
    }
    Ok(Some(adoption))
}

pub(super) fn encode_release_ready_ack(ack: &RecoveryReleaseReadyAck) -> Result<String, String> {
    if !ack.is_canonical() {
        return Err("release readiness acknowledgement is not canonical".into());
    }
    let encoded = serde_json::to_string(ack)
        .map_err(|error| format!("could not encode release readiness: {error}"))?;
    if encoded.len() > MAX_RELEASE_READY_ACK_BYTES {
        return Err(format!(
            "release readiness is {} bytes; maximum is {MAX_RELEASE_READY_ACK_BYTES}",
            encoded.len()
        ));
    }
    Ok(encoded)
}

pub(super) fn parse_release_ready_ack(
    raw: &str,
    publisher: NodeId,
) -> Result<RecoveryReleaseReadyAck, String> {
    if raw.len() > MAX_RELEASE_READY_ACK_BYTES {
        return Err(format!(
            "release readiness from {publisher} is {} bytes; maximum is {MAX_RELEASE_READY_ACK_BYTES}",
            raw.len()
        ));
    }
    let ack: RecoveryReleaseReadyAck = serde_json::from_str(raw)
        .map_err(|error| format!("invalid release readiness from {publisher}: {error}"))?;
    if !ack.is_canonical() || ack.participant.node_id != publisher.0 {
        return Err(format!(
            "release readiness from {publisher} has a non-canonical publisher"
        ));
    }
    if encode_release_ready_ack(&ack)? != raw {
        return Err(format!(
            "release readiness from {publisher} is not canonically encoded"
        ));
    }
    Ok(ack)
}
