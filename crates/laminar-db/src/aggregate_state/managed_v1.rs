//! Concrete in-memory model for one managed grouped `COUNT(*)`/`SUM(Int64)` vnode.
//!
//! This is the placement-neutral conformance subject for atomic batch mutation, portable FULL or
//! EMPTY restore, and replacement-style vnode publication. It is deliberately not connected to
//! cluster execution or admission and makes no resident-memory or backend-latency claim.

#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "DKS-P1-001: remove when the cluster lifecycle becomes the production consumer"
    )
)]

use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::atomic::{AtomicU64, Ordering};

use laminar_core::state::{PartitionKeyCodecV1, PartitionKeySchemaV1, MAX_KEY_GROUP_COUNT};

use super::artifact_v1::{
    self, AggregateContractV1, AggregateObjectBudget, AggregateRow, ArtifactContext, ArtifactError,
    ArtifactKind, CountSumStateV1, STATE_WIDTH,
};

/// Logical payload limits for one vnode image.
///
/// These cover canonical key and value bytes only. A future runtime store must additionally
/// reserve allocator/container and backend-owned memory before this model can support admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ManagedVnodeLimits {
    pub(super) max_rows: u64,
    pub(super) max_encoded_key_bytes: u64,
    pub(super) max_logical_payload_bytes: u64,
}

/// Immutable plan and partition identity for one concrete managed state table/vnode.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ManagedVnodeIdentityV1 {
    routing_schema: PartitionKeySchemaV1,
    contract: AggregateContractV1,
    operator_identity_sha256: [u8; 32],
    state_table_identity_sha256: [u8; 32],
    vnode_count: NonZeroU32,
    vnode: u32,
}

impl ManagedVnodeIdentityV1 {
    pub(super) fn try_new(
        routing_schema: PartitionKeySchemaV1,
        contract: AggregateContractV1,
        operator_identity_sha256: [u8; 32],
        state_table_identity_sha256: [u8; 32],
        vnode_count: NonZeroU32,
        vnode: u32,
    ) -> Result<Self, ArtifactError> {
        if vnode_count.get() > MAX_KEY_GROUP_COUNT
            || vnode >= vnode_count.get()
            || operator_identity_sha256 == [0; 32]
            || state_table_identity_sha256 == [0; 32]
            || !contract.matches_routing_schema(&routing_schema)
        {
            return Err(ArtifactError::Invalid("managed vnode identity"));
        }
        Ok(Self {
            routing_schema,
            contract,
            operator_identity_sha256,
            state_table_identity_sha256,
            vnode_count,
            vnode,
        })
    }
}

/// Exact assignment authority installed for a live vnode image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ManagedAssignmentFence {
    version: u64,
    certificate_sha256: [u8; 32],
}

impl ManagedAssignmentFence {
    pub(super) fn try_new(
        version: u64,
        certificate_sha256: [u8; 32],
    ) -> Result<Self, ArtifactError> {
        if version == 0 || certificate_sha256 == [0; 32] {
            return Err(ArtifactError::Invalid("managed assignment fence"));
        }
        Ok(Self {
            version,
            certificate_sha256,
        })
    }
}

/// One source-ordered append for an already encoded group key.
#[derive(Clone, Copy, Debug)]
pub(super) struct GroupAppend<'a> {
    pub(super) key: &'a [u8],
    pub(super) sum_inputs: &'a [Option<i64>],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ManagedVnodeServingState {
    Active,
    Revoked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ManagedVnodePrecondition {
    instance_id: NonZeroU64,
    assignment: ManagedAssignmentFence,
    lifecycle_revision: u64,
    serving_state: ManagedVnodeServingState,
}

#[derive(Debug)]
enum PreparedManagedVnodeActionV1 {
    Replace(ManagedCountSumVnodeV1),
    AdvanceRetainedFence {
        target_assignment: ManagedAssignmentFence,
        next_lifecycle_revision: u64,
    },
}

/// Off-side lifecycle change tied to one exact live shard incarnation and revision.
#[derive(Debug)]
pub(super) struct PreparedManagedVnodeChangeV1 {
    predecessor: ManagedVnodePrecondition,
    action: PreparedManagedVnodeActionV1,
}

impl PreparedManagedVnodeChangeV1 {
    fn target_assignment(&self) -> ManagedAssignmentFence {
        match &self.action {
            PreparedManagedVnodeActionV1::Replace(replacement) => replacement.assignment,
            PreparedManagedVnodeActionV1::AdvanceRetainedFence {
                target_assignment, ..
            } => *target_assignment,
        }
    }
}

/// One concrete, canonically sorted managed vnode image.
#[derive(Debug)]
pub(super) struct ManagedCountSumVnodeV1 {
    instance_id: NonZeroU64,
    identity: ManagedVnodeIdentityV1,
    assignment: ManagedAssignmentFence,
    lifecycle_revision: u64,
    serving_state: ManagedVnodeServingState,
    limits: ManagedVnodeLimits,
    entries: BTreeMap<Vec<u8>, CountSumStateV1>,
    logical_payload_bytes: u64,
}

static NEXT_MANAGED_VNODE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn allocate_instance_id() -> Result<NonZeroU64, ArtifactError> {
    let value = NEXT_MANAGED_VNODE_INSTANCE_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| ArtifactError::ArithmeticOverflow)?;
    NonZeroU64::new(value).ok_or(ArtifactError::ArithmeticOverflow)
}

impl ManagedCountSumVnodeV1 {
    pub(super) fn empty(
        identity: ManagedVnodeIdentityV1,
        assignment: ManagedAssignmentFence,
        limits: ManagedVnodeLimits,
    ) -> Result<Self, ArtifactError> {
        Ok(Self {
            instance_id: allocate_instance_id()?,
            identity,
            assignment,
            lifecycle_revision: 0,
            serving_state: ManagedVnodeServingState::Active,
            limits,
            entries: BTreeMap::new(),
            logical_payload_bytes: 0,
        })
    }

    /// Preflight the complete logical batch, then publish every group mutation together.
    /// Duplicate group keys retain their source occurrence order.
    pub(super) fn apply_append_batch(
        &mut self,
        assignment: ManagedAssignmentFence,
        appends: &[GroupAppend<'_>],
    ) -> Result<(), ArtifactError> {
        if self.serving_state != ManagedVnodeServingState::Active {
            return Err(ArtifactError::Invalid("managed vnode is not active"));
        }
        if assignment != self.assignment {
            return Err(ArtifactError::Invalid("managed vnode assignment fence"));
        }
        if appends.is_empty() {
            return Ok(());
        }

        let mut prepared = BTreeMap::<Vec<u8>, CountSumStateV1>::new();
        let mut new_rows = 0_u64;
        let mut new_payload_bytes = 0_u64;
        for append in appends {
            if append.sum_inputs.is_empty() {
                return Err(ArtifactError::Invalid("empty managed append"));
            }
            let key_len =
                u64::try_from(append.key.len()).map_err(|_| ArtifactError::ArithmeticOverflow)?;
            if key_len > self.limits.max_encoded_key_bytes {
                return Err(ArtifactError::Limit("managed encoded key byte limit"));
            }
            if PartitionKeyCodecV1::vnode_for_encoded(append.key, self.identity.vnode_count)
                != self.identity.vnode
            {
                return Err(ArtifactError::Invalid("managed append row vnode"));
            }

            let already_prepared = prepared.get(append.key).copied();
            let current = already_prepared
                .or_else(|| self.entries.get(append.key).copied())
                .unwrap_or_else(CountSumStateV1::empty);
            let candidate = current.preview_append(append.sum_inputs)?;
            self.identity.contract.validate_state(candidate)?;
            if already_prepared.is_none() && !self.entries.contains_key(append.key) {
                let candidate_new_rows = new_rows
                    .checked_add(1)
                    .ok_or(ArtifactError::ArithmeticOverflow)?;
                if u64::try_from(self.entries.len())
                    .map_err(|_| ArtifactError::ArithmeticOverflow)?
                    .checked_add(candidate_new_rows)
                    .ok_or(ArtifactError::ArithmeticOverflow)?
                    > self.limits.max_rows
                {
                    return Err(ArtifactError::Limit("managed row limit"));
                }
                let candidate_new_payload_bytes = new_payload_bytes
                    .checked_add(key_len)
                    .and_then(|bytes| bytes.checked_add(STATE_WIDTH as u64))
                    .ok_or(ArtifactError::ArithmeticOverflow)?;
                if self
                    .logical_payload_bytes
                    .checked_add(candidate_new_payload_bytes)
                    .ok_or(ArtifactError::ArithmeticOverflow)?
                    > self.limits.max_logical_payload_bytes
                {
                    return Err(ArtifactError::Limit("managed logical payload byte limit"));
                }
                new_rows = candidate_new_rows;
                new_payload_bytes = candidate_new_payload_bytes;
            }
            if let Some(state) = prepared.get_mut(append.key) {
                *state = candidate;
            } else {
                // This key's resource charges are checked before its first owned copy. A later
                // row can still reject the batch, but every earlier entry remains scratch-only.
                prepared.insert(append.key.to_vec(), candidate);
            }
        }

        let current_rows =
            u64::try_from(self.entries.len()).map_err(|_| ArtifactError::ArithmeticOverflow)?;
        if current_rows
            .checked_add(new_rows)
            .ok_or(ArtifactError::ArithmeticOverflow)?
            > self.limits.max_rows
        {
            return Err(ArtifactError::Limit("managed row limit"));
        }
        let next_payload_bytes = self
            .logical_payload_bytes
            .checked_add(new_payload_bytes)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        if next_payload_bytes > self.limits.max_logical_payload_bytes {
            return Err(ArtifactError::Limit("managed logical payload byte limit"));
        }
        let next_lifecycle_revision = self
            .lifecycle_revision
            .checked_add(1)
            .ok_or(ArtifactError::ArithmeticOverflow)?;

        for (key, state) in prepared {
            self.entries.insert(key, state);
        }
        self.logical_payload_bytes = next_payload_bytes;
        self.lifecycle_revision = next_lifecycle_revision;
        Ok(())
    }

    /// Encode one immutable FULL or explicit EMPTY portable artifact.
    pub(super) fn freeze_full(
        &self,
        context: ArtifactContext<'_>,
        budget: &mut AggregateObjectBudget,
    ) -> Result<Vec<u8>, ArtifactError> {
        if self.serving_state != ManagedVnodeServingState::Active {
            return Err(ArtifactError::Invalid("managed vnode is not active"));
        }
        let expected_kind = if self.entries.is_empty() {
            ArtifactKind::Empty
        } else {
            ArtifactKind::Full
        };
        if context.kind != expected_kind
            || context.parent.is_some()
            || context.assignment_version != self.assignment.version
            || context.assignment_certificate_sha256 != self.assignment.certificate_sha256
            || context.vnode_count != self.identity.vnode_count
            || context.vnode != self.identity.vnode
            || context.routing_schema != &self.identity.routing_schema
            || context.contract != self.identity.contract
            || context.operator_identity_sha256 != self.identity.operator_identity_sha256
            || context.state_table_identity_sha256 != self.identity.state_table_identity_sha256
        {
            return Err(ArtifactError::Invalid("managed freeze context"));
        }

        let mut rows = Vec::new();
        rows.try_reserve_exact(self.entries.len())
            .map_err(|_| ArtifactError::Allocation)?;
        rows.extend(
            self.entries
                .iter()
                .map(|(key, state)| AggregateRow { key, state: *state }),
        );
        artifact_v1::encode(context, &rows, budget)
    }

    /// Decode and build an off-side authoritative FULL or EMPTY replacement.
    pub(super) fn prepare_replacement(
        &self,
        bytes: &[u8],
        context: ArtifactContext<'_>,
        target_assignment: ManagedAssignmentFence,
        budget: &mut AggregateObjectBudget,
    ) -> Result<PreparedManagedVnodeChangeV1, ArtifactError> {
        if !matches!(context.kind, ArtifactKind::Full | ArtifactKind::Empty)
            || context.parent.is_some()
            || context.assignment_version > target_assignment.version
            || target_assignment.version < self.assignment.version
            || (target_assignment.version == self.assignment.version
                && target_assignment != self.assignment)
            || (context.assignment_version == self.assignment.version
                && context.assignment_certificate_sha256 != self.assignment.certificate_sha256)
            || (context.assignment_version == target_assignment.version
                && context.assignment_certificate_sha256 != target_assignment.certificate_sha256)
            || context.vnode_count != self.identity.vnode_count
            || context.vnode != self.identity.vnode
            || context.routing_schema != &self.identity.routing_schema
            || context.contract != self.identity.contract
            || context.operator_identity_sha256 != self.identity.operator_identity_sha256
            || context.state_table_identity_sha256 != self.identity.state_table_identity_sha256
        {
            return Err(ArtifactError::Invalid("managed restore context"));
        }

        let decoded = artifact_v1::decode(bytes, context, budget)?;
        let replacement_lifecycle_revision = self
            .lifecycle_revision
            .checked_add(1)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        let logical_payload_bytes = decoded
            .key_bytes()
            .checked_add(decoded.state_bytes())
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        if decoded.row_count() > self.limits.max_rows {
            return Err(ArtifactError::Limit("managed row limit"));
        }
        if logical_payload_bytes > self.limits.max_logical_payload_bytes {
            return Err(ArtifactError::Limit("managed logical payload byte limit"));
        }

        let mut entries = BTreeMap::new();
        for row in decoded.rows() {
            let row = row?;
            let key_len =
                u64::try_from(row.key.len()).map_err(|_| ArtifactError::ArithmeticOverflow)?;
            if key_len > self.limits.max_encoded_key_bytes {
                return Err(ArtifactError::Limit("managed encoded key byte limit"));
            }
            entries.insert(row.key.to_vec(), row.state);
        }

        Ok(PreparedManagedVnodeChangeV1 {
            predecessor: self.precondition(),
            action: PreparedManagedVnodeActionV1::Replace(Self {
                instance_id: self.instance_id,
                identity: self.identity.clone(),
                assignment: target_assignment,
                lifecycle_revision: replacement_lifecycle_revision,
                serving_state: ManagedVnodeServingState::Active,
                limits: self.limits,
                entries,
                logical_payload_bytes,
            }),
        })
    }

    /// Prepare a destructive revoke. Publication leaves this local shard explicitly non-serving.
    pub(super) fn prepare_revoke(
        &self,
        target_assignment: ManagedAssignmentFence,
    ) -> Result<PreparedManagedVnodeChangeV1, ArtifactError> {
        if self.serving_state != ManagedVnodeServingState::Active {
            return Err(ArtifactError::Invalid("managed vnode is not active"));
        }
        if target_assignment.version <= self.assignment.version {
            return Err(ArtifactError::Invalid("managed revoke assignment version"));
        }
        let next_lifecycle_revision = self
            .lifecycle_revision
            .checked_add(1)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        Ok(PreparedManagedVnodeChangeV1 {
            predecessor: self.precondition(),
            action: PreparedManagedVnodeActionV1::Replace(Self {
                instance_id: self.instance_id,
                identity: self.identity.clone(),
                assignment: target_assignment,
                lifecycle_revision: next_lifecycle_revision,
                serving_state: ManagedVnodeServingState::Revoked,
                limits: self.limits,
                entries: BTreeMap::new(),
                logical_payload_bytes: 0,
            }),
        })
    }

    /// Prepare a state-preserving assignment advance for a vnode retained by this owner.
    pub(super) fn prepare_retained_fence(
        &self,
        target_assignment: ManagedAssignmentFence,
    ) -> Result<PreparedManagedVnodeChangeV1, ArtifactError> {
        if self.serving_state != ManagedVnodeServingState::Active {
            return Err(ArtifactError::Invalid("managed vnode is not active"));
        }
        if target_assignment.version <= self.assignment.version {
            return Err(ArtifactError::Invalid(
                "managed retained assignment version",
            ));
        }
        let next_lifecycle_revision = self
            .lifecycle_revision
            .checked_add(1)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        Ok(PreparedManagedVnodeChangeV1 {
            predecessor: self.precondition(),
            action: PreparedManagedVnodeActionV1::AdvanceRetainedFence {
                target_assignment,
                next_lifecycle_revision,
            },
        })
    }

    fn precondition(&self) -> ManagedVnodePrecondition {
        ManagedVnodePrecondition {
            instance_id: self.instance_id,
            assignment: self.assignment,
            lifecycle_revision: self.lifecycle_revision,
            serving_state: self.serving_state,
        }
    }

    fn validate_prepared_change(
        &self,
        prepared: &PreparedManagedVnodeChangeV1,
    ) -> Result<(), ArtifactError> {
        if let PreparedManagedVnodeActionV1::Replace(replacement) = &prepared.action {
            if replacement.identity != self.identity || replacement.limits != self.limits {
                return Err(ArtifactError::Invalid("managed replacement destination"));
            }
        }
        if prepared.predecessor != self.precondition() {
            return Err(ArtifactError::Invalid("stale managed vnode change"));
        }
        Ok(())
    }

    fn publish_prevalidated_change(
        &mut self,
        prepared: PreparedManagedVnodeChangeV1,
    ) -> Option<Self> {
        match prepared.action {
            PreparedManagedVnodeActionV1::Replace(replacement) => {
                Some(std::mem::replace(self, replacement))
            }
            PreparedManagedVnodeActionV1::AdvanceRetainedFence {
                target_assignment,
                next_lifecycle_revision,
            } => {
                self.assignment = target_assignment;
                self.lifecycle_revision = next_lifecycle_revision;
                None
            }
        }
    }

    #[cfg(test)]
    pub(super) fn state(&self, key: &[u8]) -> Option<CountSumStateV1> {
        self.entries.get(key).copied()
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    pub(super) const fn logical_payload_bytes(&self) -> u64 {
        self.logical_payload_bytes
    }

    #[cfg(test)]
    pub(super) const fn assignment_version(&self) -> u64 {
        self.assignment.version
    }

    #[cfg(test)]
    pub(super) const fn is_active(&self) -> bool {
        matches!(self.serving_state, ManagedVnodeServingState::Active)
    }
}

/// Publish one caller-supplied lifecycle batch after validating every supplied participant.
///
/// The mutable borrows exclude concurrent changes between preflight and the allocation-free,
/// infallible publication loop. Replaced images are returned for out-of-fence destruction. The
/// future graph owner must prove this batch matches its complete authoritative vnode roster.
pub(super) fn publish_prepared_changes(
    live: &mut [&mut ManagedCountSumVnodeV1],
    prepared: Vec<PreparedManagedVnodeChangeV1>,
) -> Result<Vec<ManagedCountSumVnodeV1>, ArtifactError> {
    if live.len() != prepared.len() || live.is_empty() {
        return Err(ArtifactError::Invalid("managed transition participant set"));
    }
    let predecessor_assignment = prepared[0].predecessor.assignment;
    let target_assignment = prepared[0].target_assignment();
    if prepared.iter().any(|change| {
        change.predecessor.assignment != predecessor_assignment
            || change.target_assignment() != target_assignment
    }) {
        return Err(ArtifactError::Invalid("managed transition assignment set"));
    }
    for (state, change) in live.iter().zip(&prepared) {
        state.validate_prepared_change(change)?;
    }

    let replacement_count = prepared
        .iter()
        .filter(|change| matches!(&change.action, PreparedManagedVnodeActionV1::Replace(_)))
        .count();
    let mut retired = Vec::new();
    retired
        .try_reserve_exact(replacement_count)
        .map_err(|_| ArtifactError::Allocation)?;

    for (state, change) in live.iter_mut().zip(prepared) {
        if let Some(previous) = state.publish_prevalidated_change(change) {
            retired.push(previous);
        }
    }
    Ok(retired)
}

#[cfg(test)]
mod tests;
