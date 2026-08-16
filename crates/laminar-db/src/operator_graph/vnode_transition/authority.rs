//! Assignment and shuffle authority captured across vnode-state preparation.

use std::sync::Arc;

use rustc_hash::FxHashSet;

use crate::error::DbError;
use crate::operator::sql_query::ClusterShuffleConfig;

pub(super) struct VnodeTransitionAuthoritySnapshot {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    pub(super) self_id: laminar_core::state::NodeId,
    pub(super) assignment: laminar_core::state::VnodeAssignmentSnapshot,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    transport_digest: Option<[u8; 32]>,
}

impl VnodeTransitionAuthoritySnapshot {
    pub(super) fn capture(
        config: &ClusterShuffleConfig,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> Result<Self, DbError> {
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if target.assignment_version != assignment.version()
            || !target.matches_owner_map(&owners)
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != config.receiver.incarnation()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode transition target/process does not match assignment {}",
                assignment.version()
            )));
        }
        let transport_digest = if target.contains(config.self_id.0) {
            match (
                config.sender.assignment_version(),
                config.receiver.assignment_version(),
                config.sender.active_assignment_digest(),
                config.receiver.active_assignment_digest(),
            ) {
                (sender_version, receiver_version, Some(sender), Some(receiver))
                    if target.participant_incarnation(config.self_id.0)
                        == Some(config.sender.incarnation())
                        && sender_version == assignment.version()
                        && receiver_version == assignment.version()
                        && sender == receiver
                        && sender == target.digest() =>
                {
                    Some(sender)
                }
                _ => {
                    return Err(DbError::ShuffleNotReady(
                        "[LDB-6051] vnode transition requires matching active sender and receiver assignment certificates"
                            .into(),
                    ));
                }
            }
        } else {
            if target.participant_incarnation(config.self_id.0).is_some()
                || config.sender.assignment_version() != 0
                || config.receiver.assignment_version() != 0
                || config.sender.active_assignment_digest().is_some()
                || config.receiver.active_assignment_digest().is_some()
            {
                return Err(DbError::ShuffleNotReady(
                    "[LDB-6051] zero-owner vnode transition requires inactive shuffle endpoints"
                        .into(),
                ));
            }
            None
        };
        Ok(Self {
            registry: Arc::clone(&config.registry),
            self_id: config.self_id,
            assignment,
            sender: Arc::clone(&config.sender),
            receiver: Arc::clone(&config.receiver),
            transport_digest,
        })
    }

    pub(super) fn owns(&self, vnode: u32) -> bool {
        self.assignment.owners().get(vnode as usize).copied() == Some(self.self_id)
    }

    pub(super) fn contains_vnode(&self, vnode: u32) -> bool {
        self.assignment.owners().get(vnode as usize).is_some()
    }

    pub(super) fn revalidate_for_publication(&self) -> Result<(), DbError> {
        let transport_matches = self.transport_digest.map_or_else(
            || {
                self.sender.assignment_version() == 0
                    && self.receiver.assignment_version() == 0
                    && self.sender.active_assignment_digest().is_none()
                    && self.receiver.active_assignment_digest().is_none()
            },
            |digest| {
                self.sender.assignment_version() == self.assignment.version()
                    && self.receiver.assignment_version() == self.assignment.version()
                    && self.sender.active_assignment_digest() == Some(digest)
                    && self.receiver.active_assignment_digest() == Some(digest)
            },
        );
        if !transport_matches {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] shuffle assignment certificate changed before vnode-state publication"
                    .into(),
            ));
        }
        let current = self.registry.versioned_snapshot();
        if current.version() != self.assignment.version()
            || current.owners() != self.assignment.owners()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode assignment changed from {} before vnode-state publication",
                self.assignment.version()
            )));
        }
        Ok(())
    }
}

pub(super) struct FinalOwnerExitAuthoritySnapshot {
    registry: Arc<laminar_core::state::VnodeRegistry>,
    self_id: laminar_core::state::NodeId,
    pub(super) assignment: laminar_core::state::VnodeAssignmentSnapshot,
    sender: Arc<laminar_core::shuffle::ShuffleSender>,
    receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
}

impl FinalOwnerExitAuthoritySnapshot {
    pub(super) fn capture(
        config: &ClusterShuffleConfig,
        transition: &laminar_core::checkpoint::AssignmentDrainTransition,
        revoked: &FxHashSet<u32>,
    ) -> Result<Self, DbError> {
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        if !transition.is_canonical()
            || transition.target.assignment_version != assignment.version()
            || transition.target.vnode_count != config.registry.vnode_count()
            || !transition.target.matches_owner_map(&owners)
        {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] final-owner-exit authority does not match target assignment {}",
                assignment.version()
            )));
        }
        if transition.target.contains(config.self_id.0) {
            return Err(DbError::Checkpoint(format!(
                "[LDB-6051] final-owner-exit target still certifies local node {}",
                config.self_id.0
            )));
        }
        let predecessor_incarnation = transition
            .predecessor
            .participant_incarnation(config.self_id.0)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "[LDB-6051] final-owner-exit predecessor does not certify local node {}",
                    config.self_id.0
                ))
            })?;
        if config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != predecessor_incarnation
            || config.receiver.incarnation() != predecessor_incarnation
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit predecessor process identity does not match local shuffle endpoints"
                    .into(),
            ));
        }
        if config.sender.assignment_version() != 0
            || config.receiver.assignment_version() != 0
            || config.sender.active_assignment_digest().is_some()
            || config.receiver.active_assignment_digest().is_some()
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit cleanup requires inactive shuffle endpoints".into(),
            ));
        }
        if revoked.is_empty()
            || revoked.iter().any(|vnode| {
                assignment
                    .owners()
                    .get(*vnode as usize)
                    .is_none_or(|owner| *owner == config.self_id)
            })
        {
            return Err(DbError::Checkpoint(
                "[LDB-6051] final-owner-exit revoke roster is empty, out of range, or still target-owned"
                    .into(),
            ));
        }
        Ok(Self {
            registry: Arc::clone(&config.registry),
            self_id: config.self_id,
            assignment,
            sender: Arc::clone(&config.sender),
            receiver: Arc::clone(&config.receiver),
        })
    }

    pub(super) fn revalidate_for_publication(&self) -> Result<(), DbError> {
        if self.sender.assignment_version() != 0
            || self.receiver.assignment_version() != 0
            || self.sender.active_assignment_digest().is_some()
            || self.receiver.active_assignment_digest().is_some()
        {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] shuffle authority became active before final-owner-exit publication"
                    .into(),
            ));
        }
        let current = self.registry.versioned_snapshot();
        if current.version() != self.assignment.version()
            || current.owners() != self.assignment.owners()
        {
            return Err(DbError::ShuffleNotReady(format!(
                "[LDB-6051] vnode assignment changed from {} before final-owner-exit publication",
                self.assignment.version()
            )));
        }
        if current.owners().contains(&self.self_id) {
            return Err(DbError::ShuffleNotReady(
                "[LDB-6051] final-owner-exit target regained ownership".into(),
            ));
        }
        Ok(())
    }
}
