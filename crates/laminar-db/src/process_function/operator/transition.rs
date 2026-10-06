use std::collections::{BTreeMap, BTreeSet};

use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
use laminar_core::state::PartitionKeyCodecV1;
use rustc_hash::FxHashMap;

use super::execution::{shuffle::ShuffleState, ProcessExecution};
use super::restoration::MAX_LOCAL_METADATA_FRAME_BYTES;
use super::{
    charged_key, DueTimer, KeyState, OperatorFrame, ProcessFunctionOperator, TIMER_CHARGE,
};
use crate::error::DbError;
use crate::operator_graph::{
    GraphOperator, InputFrontier, ManagedVnodeTransition, ManagedVnodeTransitionMode,
};

pub(super) enum ProcessVnodeTransition {
    Idle,
    Prepared(PreparedProcessTransition),
    Aborted(PreparedProcessTransition),
    Retired(PreparedProcessTransition),
}

pub(super) struct PreparedProcessTransition {
    slots: Vec<VnodeSlot>,
    due: BTreeSet<DueTimer>,
    metadata: OperatorFrame,
    execution: Option<ProcessExecution>,
    shuffle: ShuffleState,
    assignment_fence: Option<CheckpointAssignmentFence>,
    live_bytes: usize,
    key_count: usize,
    timer_count: usize,
    prepared_bytes: usize,
    retired_bytes: usize,
}

struct VnodeSlot {
    vnode: usize,
    state: FxHashMap<Vec<u8>, KeyState>,
    activation_sequence: Option<u64>,
}

impl ProcessVnodeTransition {
    pub(super) fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub(super) fn accounting(&self) -> (usize, usize) {
        match self {
            Self::Idle => (0, 0),
            Self::Prepared(state) | Self::Aborted(state) => (state.prepared_bytes, 0),
            Self::Retired(state) => (0, state.retired_bytes),
        }
    }

    pub(super) fn abort(&mut self) {
        if matches!(self, Self::Prepared(_)) {
            let Self::Prepared(prepared) = std::mem::replace(self, Self::Idle) else {
                unreachable!("checked prepared process transition");
            };
            *self = Self::Aborted(prepared);
        }
    }

    pub(super) fn finish(&mut self) {
        assert!(
            !matches!(self, Self::Prepared(_)),
            "process vnode transition must abort or publish before cleanup"
        );
        *self = Self::Idle;
    }
}

impl ProcessFunctionOperator {
    fn validate_transition(&self, transition: &ManagedVnodeTransition<'_>) -> Result<(), DbError> {
        if !self.vnode_transition.is_idle() || self.checkpoint_drain_pending() {
            return Err(DbError::Checkpoint(
                "process vnode transition requires finished cleanup and drained invocations".into(),
            ));
        }
        if !transition.predecessor.is_canonical()
            || !transition.target.is_canonical()
            || transition.predecessor.vnode_count != self.vnode_count.get()
            || transition.target.vnode_count != self.vnode_count.get()
        {
            return Err(DbError::Checkpoint(
                "process vnode transition has an invalid assignment domain".into(),
            ));
        }
        match transition.mode {
            ManagedVnodeTransitionMode::Live => {
                // The graph validates exact owner rosters and donor provenance. This participant
                // binds preparation to the installed cut before publication under graph authority.
                if self.assignment_fence.as_ref() != Some(transition.predecessor)
                    || transition.predecessor.assignment_version.checked_add(1)
                        != Some(transition.target.assignment_version)
                {
                    return Err(DbError::Checkpoint(
                        "process vnode transition does not match its installed assignment".into(),
                    ));
                }
            }
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                let owners = predecessor_owners
                    .iter()
                    .map(|owner| owner.0)
                    .collect::<Vec<_>>();
                if !transition.predecessor.matches_owner_map(&owners)
                    || transition.predecessor.assignment_version
                        >= transition.target.assignment_version
                    || !transition.revoked.is_empty()
                    || transition.restores.is_empty()
                    || self.assignment_fence.is_some()
                    || self.metadata_restored
                    || self.next_activation_id != 0
                    || self.next_timer_generation != 0
                    || self.watermark_us != i64::MIN
                    || self.state.iter().any(|state| !state.is_empty())
                {
                    return Err(DbError::Checkpoint(
                        "process checkpoint bootstrap requires a fresh operator and exact predecessor"
                            .into(),
                    ));
                }
            }
        }
        if transition
            .revoked
            .iter()
            .any(|&vnode| vnode >= self.vnode_count.get())
            || transition
                .restores
                .windows(2)
                .any(|pair| pair[0].vnode >= pair[1].vnode)
        {
            return Err(DbError::Checkpoint(
                "invalid process transition vnode roster".into(),
            ));
        }
        for restore in transition.restores {
            if restore.vnode >= self.vnode_count.get()
                || transition.revoked.contains(&restore.vnode)
                || !self.state[restore.vnode as usize].is_empty()
                || !transition.predecessor.contains(restore.participant_id)
            {
                return Err(DbError::Checkpoint(
                    "process acquired vnode has an invalid donor or overlaps live state".into(),
                ));
            }
            if let ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } =
                transition.mode
            {
                if predecessor_owners[restore.vnode as usize].0 != restore.participant_id {
                    return Err(DbError::Checkpoint(
                        "process vnode donor does not own its predecessor slot".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn transition_metadata(
        &self,
        transition: &ManagedVnodeTransition<'_>,
    ) -> Result<(OperatorFrame, BTreeMap<u64, OperatorFrame>, InputFrontier), DbError> {
        let expected = transition
            .restores
            .iter()
            .map(|restore| restore.participant_id)
            .collect::<BTreeSet<_>>();
        let mut donors = BTreeMap::new();
        let mut metadata = self.checkpoint_frame();
        let mut frontier = match transition.mode {
            ManagedVnodeTransitionMode::Live => Some(self.transition_frontier(
                &metadata,
                transition.predecessor,
                self.execution.local_id().map(|node| node.0),
            )?),
            ManagedVnodeTransitionMode::CheckpointBootstrap { .. } => None,
        };
        for restore in transition.whole_restores {
            if !expected.contains(&restore.participant_id)
                || donors.contains_key(&restore.participant_id)
            {
                return Err(DbError::Checkpoint(
                    "invalid process donor metadata roster".into(),
                ));
            }
            let frame = self.decode_metadata(restore.state)?;
            let donor_frontier = self.transition_frontier(
                &frame,
                transition.predecessor,
                Some(restore.participant_id),
            )?;
            if frontier.is_some_and(|frontier| frontier != donor_frontier) {
                return Err(DbError::Checkpoint(
                    "process donor watermarks/frontiers do not describe one drained cut".into(),
                ));
            }
            frontier = Some(donor_frontier);
            metadata.next_activation_id = metadata.next_activation_id.max(frame.next_activation_id);
            metadata.next_timer_generation = metadata
                .next_timer_generation
                .max(frame.next_timer_generation);
            donors.insert(restore.participant_id, frame);
        }
        if donors.len() != expected.len() {
            return Err(DbError::Checkpoint(
                "process vnode transition requires every donor's bound metadata".into(),
            ));
        }
        let frontier = frontier.ok_or_else(|| {
            DbError::Checkpoint("process transition has no drained frontier cut".into())
        })?;
        metadata.watermark_us = frontier
            .watermark
            .map_or(i64::MIN, |ms| ms.saturating_mul(1_000));
        metadata.shuffle = None;
        Ok((metadata, donors, frontier))
    }

    fn transition_frontier(
        &self,
        frame: &OperatorFrame,
        predecessor: &CheckpointAssignmentFence,
        participant: Option<u64>,
    ) -> Result<InputFrontier, DbError> {
        match (&frame.shuffle, self.execution.local_id()) {
            (Some(shuffle), Some(_)) => shuffle.frontier_at_cut(
                predecessor,
                participant.ok_or_else(|| {
                    DbError::Checkpoint("process cut has no local participant".into())
                })?,
            ),
            (Some(_), None) => Err(DbError::Checkpoint(
                "distributed process transition requires cluster execution selection".into(),
            )),
            (None, Some(_)) if predecessor.participants.len() > 1 => Err(DbError::Checkpoint(
                "distributed process donor is missing its shuffle frontier cut".into(),
            )),
            (None, _) if frame.watermark_us != i64::MIN && frame.watermark_us % 1_000 != 0 => {
                // Saturated microseconds do not identify the original millisecond cut.
                Err(DbError::Checkpoint(
                    "process transfer cannot reconstruct an exact frontier from its watermark"
                        .into(),
                ))
            }
            (None, _) => Ok(InputFrontier {
                watermark: (frame.watermark_us != i64::MIN)
                    .then_some(frame.watermark_us.div_euclid(1_000)),
                idle: false,
            }),
        }
    }

    fn transition_preflight_bytes(
        &self,
        transition: &ManagedVnodeTransition<'_>,
    ) -> Result<(usize, usize), DbError> {
        let retained_live_bytes = self
            .state
            .iter()
            .fold(self.live_bytes, |bytes, state| {
                bytes.saturating_add(
                    state
                        .capacity()
                        .saturating_mul(std::mem::size_of::<(Vec<u8>, KeyState)>()),
                )
            })
            .saturating_add(due_charge(self.due.iter()))
            .saturating_add(self.cluster_retained_bytes());
        let payload_bytes = transition
            .restores
            .iter()
            .map(|restore| restore.state.len())
            .chain(
                transition
                    .whole_restores
                    .iter()
                    .map(|restore| restore.state.len()),
            )
            .try_fold(0usize, usize::checked_add)
            .unwrap_or(usize::MAX)
            .saturating_add(
                transition
                    .whole_restores
                    .iter()
                    .fold(0usize, |bytes, restore| {
                        bytes
                            .saturating_add(restore.state.len().saturating_mul(6))
                            .saturating_add(std::mem::size_of::<OperatorFrame>() + 64)
                    }),
            );
        // Bound the replacement channel tree and its temporary decoded frontier roster.
        let topology_bytes = if self.execution.local_id().is_some() {
            transition
                .target
                .participants
                .len()
                .saturating_mul(256)
                .saturating_add(4_096)
                .saturating_add(self.execution.retained_bytes().saturating_mul(2))
        } else {
            0
        };
        self.check_transition_budget(
            retained_live_bytes
                .saturating_add(payload_bytes)
                .saturating_add(topology_bytes),
        )?;
        Ok((retained_live_bytes, payload_bytes))
    }

    pub(super) fn prepare_transition(
        &self,
        transition: &ManagedVnodeTransition<'_>,
    ) -> Result<PreparedProcessTransition, DbError> {
        self.validate_transition(transition)?;
        let (retained_live_bytes, payload_bytes) = self.transition_preflight_bytes(transition)?;
        let (metadata, donors, frontier) = self.transition_metadata(transition)?;
        let (execution, shuffle) =
            self.prepare_transition_execution(transition.target, frontier)?;
        let slot_count = transition.revoked.len() + transition.restores.len();
        let scaffold_bytes = std::mem::size_of::<PreparedProcessTransition>()
            .saturating_add(slot_count.saturating_mul(std::mem::size_of::<VnodeSlot>()))
            .saturating_add(
                transition
                    .target
                    .participants
                    .len()
                    .max(transition.predecessor.participants.len())
                    .saturating_mul(std::mem::size_of::<CheckpointParticipant>()),
            )
            .saturating_add(MAX_LOCAL_METADATA_FRAME_BYTES);
        let fixed_bytes = scaffold_bytes
            .saturating_add(
                execution
                    .as_ref()
                    .map_or(0, ProcessExecution::retained_bytes),
            )
            .saturating_add(shuffle.retained_bytes());
        self.check_transition_budget(
            retained_live_bytes
                .saturating_add(fixed_bytes)
                .saturating_add(payload_bytes),
        )?;
        let retired_bytes = scaffold_bytes
            .saturating_add(due_charge(self.due.iter()))
            .saturating_add(self.shuffle.retained_bytes())
            .saturating_add(
                execution
                    .as_ref()
                    .map_or(0, |_| self.execution.retained_bytes()),
            );
        let mut prepared = PreparedProcessTransition {
            slots: Vec::with_capacity(slot_count),
            due: BTreeSet::new(),
            metadata,
            execution,
            shuffle,
            assignment_fence: Some(transition.target.clone()),
            live_bytes: self.live_bytes,
            key_count: self.key_count,
            timer_count: self.timer_count,
            prepared_bytes: fixed_bytes,
            retired_bytes,
        };
        for &vnode in transition.revoked {
            let state = &self.state[vnode as usize];
            prepared.retired_bytes = prepared.retired_bytes.saturating_add(
                state
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(Vec<u8>, KeyState)>()),
            );
            for (key, state) in state {
                let bytes = charged_key(key, state)?;
                prepared.live_bytes = prepared
                    .live_bytes
                    .checked_sub(bytes)
                    .ok_or_else(accounting_error)?;
                prepared.timer_count = prepared
                    .timer_count
                    .checked_sub(state.timers.len())
                    .ok_or_else(accounting_error)?;
                prepared.retired_bytes = prepared.retired_bytes.saturating_add(bytes);
            }
            prepared.key_count = prepared
                .key_count
                .checked_sub(state.len())
                .ok_or_else(accounting_error)?;
            prepared.slots.push(VnodeSlot {
                vnode: vnode as usize,
                state: FxHashMap::default(),
                activation_sequence: self.activation_sequences.get(vnode as usize).map(|_| 0),
            });
        }
        let retained = || {
            self.due.iter().filter(|(_, key, ..)| {
                let vnode = PartitionKeyCodecV1::vnode_for_encoded(key, self.vnode_count);
                !transition.revoked.contains(&vnode)
            })
        };
        prepared.prepared_bytes = prepared
            .prepared_bytes
            .saturating_add(due_charge(retained()));
        self.check_transition_budget(
            retained_live_bytes
                .saturating_add(prepared.prepared_bytes)
                .saturating_add(payload_bytes),
        )?;
        prepared.due.extend(retained().cloned());
        self.stage_acquired_vnodes(
            transition,
            &donors,
            payload_bytes,
            retained_live_bytes,
            &mut prepared,
        )?;
        self.check_transition_budget(
            prepared
                .live_bytes
                .saturating_add(prepared.retired_bytes)
                .saturating_add(payload_bytes),
        )?;
        Ok(prepared)
    }

    fn stage_acquired_vnodes(
        &self,
        transition: &ManagedVnodeTransition<'_>,
        donors: &BTreeMap<u64, OperatorFrame>,
        payload_bytes: usize,
        retained_live_bytes: usize,
        prepared: &mut PreparedProcessTransition,
    ) -> Result<(), DbError> {
        for restore in transition.restores {
            // Reserve decode headroom in addition to retained live/prepared state and borrowed payloads.
            let decode_bytes = restore.state.len().saturating_mul(6).saturating_add(128);
            self.check_transition_budget(
                retained_live_bytes
                    .saturating_add(prepared.prepared_bytes)
                    .saturating_add(payload_bytes)
                    .saturating_add(decode_bytes),
            )?;
            let state_limit = self
                .descriptor
                .limits
                .max_state_bytes
                .min(self.graph_budget);
            let available = state_limit
                .checked_sub(prepared.live_bytes)
                .ok_or_else(accounting_error)?;
            let donor = donors
                .get(&restore.participant_id)
                .expect("validated process donor metadata roster");
            let mut restored = self.decode_vnode(
                restore.vnode,
                restore.state,
                donor.next_timer_generation,
                available,
            )?;
            prepared.live_bytes += restored.live_bytes;
            prepared.key_count = prepared
                .key_count
                .checked_add(restored.state.len())
                .ok_or_else(accounting_error)?;
            prepared.timer_count = prepared
                .timer_count
                .checked_add(restored.due.len())
                .ok_or_else(accounting_error)?;
            if prepared.key_count > self.descriptor.limits.max_keys
                || prepared.timer_count > self.descriptor.limits.max_timers
            {
                return Err(DbError::Checkpoint(
                    "process transition exceeds key or timer budget".into(),
                ));
            }
            prepared.prepared_bytes = prepared
                .prepared_bytes
                .saturating_add(restored.live_bytes)
                .saturating_add(
                    restored
                        .state
                        .capacity()
                        .saturating_mul(std::mem::size_of::<(Vec<u8>, KeyState)>()),
                )
                .saturating_add(due_charge(restored.due.iter()));
            self.check_transition_budget(
                retained_live_bytes
                    .saturating_add(prepared.prepared_bytes)
                    .saturating_add(payload_bytes),
            )?;
            prepared.due.append(&mut restored.due);
            prepared.slots.push(VnodeSlot {
                vnode: restore.vnode as usize,
                state: restored.state,
                activation_sequence: restored.activation_sequence,
            });
        }
        Ok(())
    }

    fn check_transition_budget(&self, accounted_bytes: usize) -> Result<(), DbError> {
        if accounted_bytes > self.graph_budget {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: "process vnode transition retained state and decode".into(),
                accounted_bytes,
                limit_bytes: self.graph_budget,
            });
        }
        Ok(())
    }

    pub(super) fn publish_transition(&mut self) {
        let ProcessVnodeTransition::Prepared(mut prepared) =
            std::mem::replace(&mut self.vnode_transition, ProcessVnodeTransition::Idle)
        else {
            panic!("process vnode transition must be prepared before publication");
        };
        // INVARIANT: the graph holds the rotation fence. Every slot and timer index is already
        // allocated; displaced allocations stay owned until finish runs outside authority locks.
        for slot in &mut prepared.slots {
            std::mem::swap(&mut self.state[slot.vnode], &mut slot.state);
            if let Some(sequence) = &mut slot.activation_sequence {
                std::mem::swap(&mut self.activation_sequences[slot.vnode], sequence);
            }
        }
        std::mem::swap(&mut self.due, &mut prepared.due);
        std::mem::swap(&mut self.assignment_fence, &mut prepared.assignment_fence);
        std::mem::swap(&mut self.shuffle, &mut prepared.shuffle);
        if let Some(execution) = &mut prepared.execution {
            std::mem::swap(&mut self.execution, execution);
        }
        std::mem::swap(
            &mut self.next_activation_id,
            &mut prepared.metadata.next_activation_id,
        );
        std::mem::swap(
            &mut self.next_timer_generation,
            &mut prepared.metadata.next_timer_generation,
        );
        std::mem::swap(&mut self.watermark_us, &mut prepared.metadata.watermark_us);
        std::mem::swap(&mut self.live_bytes, &mut prepared.live_bytes);
        std::mem::swap(&mut self.key_count, &mut prepared.key_count);
        std::mem::swap(&mut self.timer_count, &mut prepared.timer_count);
        self.metadata_restored = true;
        self.vnode_transition = ProcessVnodeTransition::Retired(prepared);
    }
}

fn due_charge<'a>(timers: impl Iterator<Item = &'a DueTimer>) -> usize {
    timers.fold(0usize, |bytes, (_, key, name, _)| {
        bytes
            .saturating_add(TIMER_CHARGE)
            .saturating_add(key.len())
            .saturating_add(name.len())
    })
}

fn accounting_error() -> DbError {
    DbError::Checkpoint("process vnode transition state accounting invariant failed".into())
}
