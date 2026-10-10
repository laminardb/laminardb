//! Slot claims. A fresh source commits a claim in a checkpoint before it creates the slot
//! `<slot.name>_<claim>`, a name never reused, and a restarted source adopts only the slot its
//! committed cursor names. Other slots under the prefix are reported, never dropped.
//!
//! No row is emitted under a claimed cursor in either mode: rows wait until a cursor naming the
//! created slot commits. A restart from the claim may copy or stream from a new slot or a later
//! point, which is only exact when nothing was emitted before.

mod creation;

use crate::connector::SourceBatch;

use super::super::config::{PostgresCdcConfig, SnapshotMode};
use super::super::postgres_io::slots::{self, PrefixSlot, SlotFacts, SlotHolder};
use super::super::postgres_io::{inspect_source, ControlConnection, PostgresCheckpointBinding};
use super::checkpoint::{validate_live_binding, CursorPhase};
use super::{Arc, ConnectorError, Lsn, Phase, PostgresCdcSource};

pub(super) use creation::ClaimTask;
use creation::{ClaimJob, ClaimOutcome};

const CLAIM_ID_HEX_DIGITS: usize = 16;

/// A claim on one slot name: a random identifier and the slot named after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SlotClaim {
    id: String,
    slot: String,
}

impl SlotClaim {
    /// A fresh claim under `prefix`.
    pub(super) fn generate(prefix: &str) -> Self {
        let id = format!("{:016x}", rand::random::<u64>());
        Self {
            slot: format!("{prefix}_{id}"),
            id,
        }
    }

    /// The claim `id` under `prefix`, when `id` is 16 lower-case hexadecimal digits.
    pub(super) fn parse(prefix: &str, id: &str) -> Option<Self> {
        let valid = id.len() == CLAIM_ID_HEX_DIGITS
            && id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
        valid.then(|| Self {
            slot: format!("{prefix}_{id}"),
            id: id.to_string(),
        })
    }

    /// The claim that slot `name` was created for, when it is a claim slot under `prefix`.
    pub(super) fn from_slot(prefix: &str, name: &str) -> Option<Self> {
        let id = name.strip_prefix(prefix)?.strip_prefix('_')?;
        Self::parse(prefix, id)
    }

    pub(super) fn id(&self) -> &str {
        &self.id
    }

    pub(super) fn slot(&self) -> &str {
        &self.slot
    }

    /// `application_name` of every session this claim opens during one source incarnation.
    pub(super) fn application_name(&self, incarnation: &str) -> String {
        format!("laminar:{}:{incarnation}", self.id)
    }

    /// Whether `holder` is a session of this claim left by another incarnation of the source.
    fn is_stale_holder(&self, holder: &SlotHolder, incarnation: &str) -> bool {
        holder
            .application_name
            .strip_prefix("laminar:")
            .and_then(|rest| rest.strip_prefix(self.id.as_str()))
            .and_then(|rest| rest.strip_prefix(':'))
            .is_some_and(|other| !other.is_empty() && other != incarnation)
    }
}

/// A random identifier of one `start()`, telling this source's sessions from stale ones.
pub(super) fn incarnation() -> String {
    format!("{:08x}", rand::random::<u32>())
}

/// What an operator does to reset a source whose slot cannot be resumed.
pub(super) fn reset_instructions(slot: &str) -> String {
    format!(
        "drop slot '{slot}' with {}, clear downstream targets and this pipeline's checkpoints, \
         and start the source again",
        slots::drop_statement(slot)
    )
}

/// Report that `slot`, claimed but unusable for `reason`, is left behind for a new claim.
pub(super) fn warn_orphaning(slot: &str, reason: &str) {
    tracing::warn!(
        slot,
        %reason,
        drop = %slots::drop_statement(slot),
        "the claimed PostgreSQL replication slot cannot be used; claiming a new slot name and \
         leaving this slot as an orphan"
    );
}

/// What a committed cursor does with the slot it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ResumeAction {
    /// Create the claimed slot.
    Create,
    /// Stream the existing slot from this position.
    Adopt(Lsn),
    /// Leave the slot to the operator as an orphan and claim a fresh name; the reason says why.
    NewClaim(String),
    /// End a stale session of this claim, then decide again.
    TerminateStale(SlotHolder),
    /// Another consumer holds the slot; retry later.
    Busy(SlotHolder),
    /// The cursor cannot be resumed.
    FailClosed(String),
}

/// The restart matrix: the action a committed cursor `phase` takes on its claimed slot.
pub(super) fn resume_action(
    phase: CursorPhase,
    slot: Option<&SlotFacts>,
    mode: SnapshotMode,
    claim: &SlotClaim,
    incarnation: &str,
) -> ResumeAction {
    let name = claim.slot();
    match (phase, slot) {
        (CursorPhase::Snapshot { .. }, _) => ResumeAction::FailClosed(format!(
            "the checkpoint was captured inside the initial snapshot of slot '{name}', which \
             cannot be resumed: {}",
            reset_instructions(name)
        )),
        (CursorPhase::Claimed, None) => ResumeAction::Create,
        (CursorPhase::Streaming { .. }, None) => ResumeAction::FailClosed(format!(
            "cannot resume PostgreSQL CDC slot '{name}': the slot is missing and the WAL it \
             retained is gone: {}",
            reset_instructions(name)
        )),
        (
            _,
            Some(SlotFacts {
                holder: Some(holder),
                ..
            }),
        ) if claim.is_stale_holder(holder, incarnation) => {
            ResumeAction::TerminateStale(holder.clone())
        }
        (
            _,
            Some(SlotFacts {
                holder: Some(holder),
                ..
            }),
        ) => ResumeAction::Busy(holder.clone()),
        (CursorPhase::Claimed, Some(facts)) => claimed_action(facts, mode),
        (
            CursorPhase::Streaming {
                consistent_point,
                lsn,
            },
            Some(facts),
        ) => streaming_action(facts, name, consistent_point, lsn),
    }
}

/// Nothing was emitted under a claimed cursor and no slot feedback was sent, so `never` mode
/// adopts a healthy slot from its consistent point; `initial` mode needs the snapshot only a new
/// slot exports.
fn claimed_action(facts: &SlotFacts, mode: SnapshotMode) -> ResumeAction {
    if let Some(problem) = &facts.unusable {
        return ResumeAction::NewClaim(problem.clone());
    }
    match (mode, facts.confirmed_flush_lsn) {
        (SnapshotMode::Never, Some(confirmed)) => ResumeAction::Adopt(confirmed),
        (SnapshotMode::Never, None) => ResumeAction::NewClaim("has no confirmed position".into()),
        (SnapshotMode::Initial, _) => ResumeAction::NewClaim(
            "can no longer provide the exported snapshot the initial copy needs".into(),
        ),
    }
}

fn streaming_action(
    facts: &SlotFacts,
    name: &str,
    consistent_point: Lsn,
    lsn: Lsn,
) -> ResumeAction {
    let reset = reset_instructions(name);
    if let Some(problem) = &facts.unusable {
        return ResumeAction::FailClosed(format!(
            "cannot resume PostgreSQL CDC slot '{name}': the slot {problem}: {reset}"
        ));
    }
    let Some(confirmed) = facts.confirmed_flush_lsn else {
        return ResumeAction::FailClosed(format!(
            "cannot resume PostgreSQL CDC slot '{name}': the slot has no durable position: \
             {reset}"
        ));
    };
    if confirmed < consistent_point {
        return ResumeAction::FailClosed(format!(
            "cannot resume PostgreSQL CDC slot '{name}': it is at {confirmed}, before the \
             consistent point {consistent_point} of the slot this checkpoint was taken from: \
             {reset}"
        ));
    }
    if confirmed > lsn {
        return ResumeAction::FailClosed(format!(
            "cannot resume PostgreSQL CDC checkpoint at {lsn}: slot '{name}' has already \
             advanced to {confirmed}; required WAL may have been reclaimed: {reset}"
        ));
    }
    ResumeAction::Adopt(lsn)
}

/// The retryable error for a slot held by a session that is not a stale one of this claim.
pub(super) fn busy(slot: &str, holder: &SlotHolder) -> ConnectorError {
    ConnectorError::ConnectionFailed(format!(
        "PostgreSQL replication slot '{slot}' is in use by {holder}; stop that consumer or wait \
         for it to exit"
    ))
}

/// Inspect the claimed slot under the committed `binding` and apply [`resume_action`], ending
/// at most one stale session of the claim.
///
/// # Errors
/// Returns an error when the live identity drifted from `binding` or the catalog is unreachable.
pub(super) async fn settle(
    control: &ControlConnection,
    config: &PostgresCdcConfig,
    claim: &SlotClaim,
    incarnation: &str,
    phase: CursorPhase,
    binding: &PostgresCheckpointBinding,
) -> Result<ResumeAction, ConnectorError> {
    let decide = || async {
        let inspected = inspect_source(control.client(), config, Some(claim.slot())).await?;
        validate_live_binding(binding, &inspected.binding(config), "resume checkpoint")?;
        Ok::<_, ConnectorError>(resume_action(
            phase,
            inspected.slot.as_ref(),
            config.snapshot_mode,
            claim,
            incarnation,
        ))
    };
    let action = decide().await?;
    let ResumeAction::TerminateStale(holder) = action else {
        return Ok(action);
    };
    tracing::warn!(
        slot = claim.slot(),
        %holder,
        "terminating a stale session of this PostgreSQL CDC source's own slot claim"
    );
    slots::terminate_holder(control.client(), claim.slot(), &holder).await?;
    Ok(match decide().await? {
        ResumeAction::TerminateStale(holder) => ResumeAction::Busy(holder),
        action => action,
    })
}

/// Slots under `prefix` that belong to other claims.
pub(super) fn other_claims<'a>(
    slots: &'a [PrefixSlot],
    prefix: &'a str,
    claim: &'a SlotClaim,
) -> impl Iterator<Item = &'a PrefixSlot> {
    slots.iter().filter(move |slot| {
        SlotClaim::from_slot(prefix, &slot.name).is_some_and(|other| other != *claim)
    })
}

/// Warn about every other claim's slot under the prefix, returning how many are orphaned
/// (inactive). `None` when they could not be listed.
pub(super) async fn report_orphans(
    client: &tokio_postgres::Client,
    prefix: &str,
    claim: &SlotClaim,
) -> Option<usize> {
    let listed = match slots::prefix_slots(client, prefix).await {
        Ok(listed) => listed,
        Err(error) => {
            tracing::warn!(prefix, %error, "could not list PostgreSQL CDC slots sharing slot.name");
            return None;
        }
    };
    let mut orphans = 0;
    for slot in other_claims(&listed, prefix, claim) {
        if let Some(holder) = &slot.holder {
            tracing::warn!(
                slot = %slot.name,
                holder = %holder,
                "another consumer streams from a slot under this source's slot.name prefix; \
                 LaminarDB leaves it alone"
            );
            continue;
        }
        orphans += 1;
        tracing::warn!(
            slot = %slot.name,
            retained_bytes = ?slot.retained_bytes,
            inactive_since = ?slot.inactive_since,
            drop = %slots::drop_statement(&slot.name),
            "orphaned PostgreSQL CDC slot retains WAL; LaminarDB never drops slots, so drop it \
             once no pipeline restores from a checkpoint that names it"
        );
    }
    Some(orphans)
}

impl PostgresCdcSource {
    fn claim(&self) -> Result<&SlotClaim, ConnectorError> {
        self.claim
            .as_ref()
            .ok_or_else(|| ConnectorError::Internal("PostgreSQL CDC has no slot claim".into()))
    }

    /// The exact replication slot this source owns.
    pub(super) fn slot_name(&self) -> Result<&str, ConnectorError> {
        self.claim().map(SlotClaim::slot)
    }

    /// Hold intake under `claim` and spawn the task that creates its slot once a checkpoint
    /// carrying the claim commits (`committed` when one already has).
    pub(super) fn begin_claim(
        &mut self,
        claim: SlotClaim,
        committed: bool,
    ) -> Result<(), ConnectorError> {
        let (Some(binding), Some(layout)) = (&self.checkpoint_binding, &self.layout) else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC claims a slot before startup bound the source".into(),
            ));
        };
        let job = ClaimJob {
            config: self.config.clone(),
            application_name: claim.application_name(&self.incarnation),
            incarnation: self.incarnation.clone(),
            claim: claim.clone(),
            binding: binding.clone(),
            layout: layout.clone(),
            admission: self.task_owner.admission(),
            data_ready: Arc::clone(&self.data_ready),
        };
        let task = ClaimTask::spawn(job, committed, &self.task_owner)?;
        tracing::info!(
            slot = claim.slot(),
            committed,
            "PostgreSQL CDC claimed a replication slot name; the slot is created after a \
             checkpoint commits the claim"
        );
        self.claim = Some(claim);
        self.phase = Phase::Claiming(task);
        Ok(())
    }

    /// Install the slot once the creation task finished; until then intake stays held.
    pub(super) async fn poll_claim(&mut self) -> Result<Option<SourceBatch>, ConnectorError> {
        if !matches!(&self.phase, Phase::Claiming(task) if task.is_finished()) {
            return Ok(None);
        }
        let Phase::Claiming(task) = std::mem::replace(&mut self.phase, Phase::Idle) else {
            return Err(ConnectorError::Internal(
                "PostgreSQL CDC left the claim phase while polling it".into(),
            ));
        };
        let outcome = match task.outcome().await {
            Ok(outcome) => outcome,
            Err(error) => return Err(self.fail(error)),
        };
        match outcome {
            ClaimOutcome::Created {
                consistent_point,
                snapshot: Some(reader),
            } => {
                self.consistent_point = Some(consistent_point);
                self.phase = Phase::Snapshot(reader);
            }
            ClaimOutcome::Created {
                consistent_point,
                snapshot: None,
            }
            | ClaimOutcome::Adopted(consistent_point) => {
                self.consistent_point = Some(consistent_point);
                self.polled_lsn = consistent_point;
                self.phase = Phase::AwaitingStream { released: false };
            }
            ClaimOutcome::NewClaim => {
                let claim = SlotClaim::generate(&self.config.slot_name);
                if let Err(error) = self.begin_claim(claim, false) {
                    return Err(self.fail(error));
                }
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
