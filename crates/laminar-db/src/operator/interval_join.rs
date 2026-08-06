//! Interval join operator for the `OperatorGraph`.
//!
//! Buffers left/right rows across cycles for
//! `right_ts BETWEEN left_ts AND left_ts + time_bound`; evicts on watermark advance.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::collections::BTreeMap;
#[cfg(feature = "cluster")]
use std::collections::VecDeque;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_core::state::{KeyGroupCount, VnodeAssignmentSnapshot, VnodeRegistry, LOCAL_NODE_ID};
use laminar_sql::translator::StreamJoinConfig;

use crate::error::DbError;
use crate::interval_join::{
    execute_interval_join_cycle, join_type_tag, ArchivedJoinStateCheckpoint,
    IntervalJoinCheckpointCapture, IntervalJoinOutputBudget, IntervalJoinState,
    JoinStateCheckpoint, HEAP_ALLOCATION_CHARGE,
};
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{
    CapturedVnodeState, EncodedStateFrame, InputFrontier, StateFrameCapture,
};
use crate::operator_graph::{GraphOperator, ManagedStateAccountingSnapshot, OperatorCheckpoint};

#[cfg(feature = "cluster")]
use crate::operator::sql_query::ClusterShuffleConfig;
#[cfg(feature = "cluster")]
use crate::operator_graph::{ManagedVnodeTransition, ManagedVnodeTransitionMode};

const OPERATOR_CHECKPOINT_VERSION: u8 = 1;
const ABSENT_VNODE: u8 = 0;
const PRESENT_VNODE: u8 = 1;
const VNODE_FRAME_HEADER_LEN: usize = std::mem::align_of::<ArchivedJoinStateCheckpoint>();
const ABSENT_VNODE_FRAME: [u8; VNODE_FRAME_HEADER_LEN] = [ABSENT_VNODE; VNODE_FRAME_HEADER_LEN];

#[derive(Clone, Copy, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum JoinInputSide {
    Left,
    Right,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct IntervalJoinOperatorCheckpoint {
    version: u8,
    join_type: u8,
    left_keys: Vec<String>,
    right_keys: Vec<String>,
    left_time_column: String,
    right_time_column: String,
    left_table: String,
    right_table: String,
    bound_ms: i64,
    aligned_replay: Vec<(u64, JoinInputSide, i64, Vec<u8>)>,
    applied_left_watermark: i64,
    applied_right_watermark: i64,
    applied_left_idle: bool,
    applied_right_idle: bool,
    assignment_version: Option<u64>,
    owner_map_digest: Option<[u8; 32]>,
    participant_id: Option<u64>,
}

#[cfg(feature = "cluster")]
type CapturedAlignedReplay = (u64, JoinInputSide, i64, crate::operator::RetainedBatch);

struct IntervalJoinOperatorCheckpointCapture {
    checkpoint: IntervalJoinOperatorCheckpoint,
    #[cfg(feature = "cluster")]
    aligned_replay: VecDeque<CapturedAlignedReplay>,
    retained_bytes: u64,
}

impl IntervalJoinOperatorCheckpointCapture {
    const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn calculate_retained_bytes_for(
        left_keys_capacity: usize,
        right_keys_capacity: usize,
        string_capacities: impl IntoIterator<Item = usize>,
        #[cfg(feature = "cluster")] aligned_replay_capacity: usize,
        #[cfg(feature = "cluster")] aligned_replay_batch_bytes: impl IntoIterator<Item = Option<usize>>,
    ) -> Result<u64, DbError> {
        fn allocation(bytes: usize) -> Result<usize, DbError> {
            bytes
                .checked_add(if bytes == 0 {
                    0
                } else {
                    HEAP_ALLOCATION_CHARGE
                })
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "interval join whole-state capture accounting overflow".into(),
                    )
                })
        }

        fn roster<T>(capacity: usize) -> Result<usize, DbError> {
            allocation(
                capacity
                    .checked_mul(std::mem::size_of::<T>())
                    .ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join whole-state roster accounting overflow".into(),
                        )
                    })?,
            )
        }

        fn add(total: &mut usize, bytes: usize) -> Result<(), DbError> {
            *total = total.checked_add(bytes).ok_or_else(|| {
                DbError::Checkpoint("interval join whole-state capture accounting overflow".into())
            })?;
            Ok(())
        }

        let mut bytes = std::mem::size_of::<Self>();
        add(&mut bytes, roster::<String>(left_keys_capacity)?)?;
        add(&mut bytes, roster::<String>(right_keys_capacity)?)?;
        for capacity in string_capacities {
            add(&mut bytes, allocation(capacity)?)?;
        }

        #[cfg(feature = "cluster")]
        {
            add(
                &mut bytes,
                roster::<CapturedAlignedReplay>(aligned_replay_capacity)?,
            )?;
            for batch_bytes in aligned_replay_batch_bytes {
                add(
                    &mut bytes,
                    batch_bytes.ok_or_else(|| {
                        DbError::Checkpoint(
                            "interval join aligned replay capture accounting overflow".into(),
                        )
                    })?,
                )?;
            }
        }

        u64::try_from(bytes).map_err(|_| {
            DbError::Checkpoint("interval join whole-state capture exceeds u64".into())
        })
    }

    fn calculate_retained_bytes(&self) -> Result<u64, DbError> {
        Self::calculate_retained_bytes_for(
            self.checkpoint.left_keys.capacity(),
            self.checkpoint.right_keys.capacity(),
            self.checkpoint
                .left_keys
                .iter()
                .chain(&self.checkpoint.right_keys)
                .chain([
                    &self.checkpoint.left_time_column,
                    &self.checkpoint.right_time_column,
                    &self.checkpoint.left_table,
                    &self.checkpoint.right_table,
                ])
                .map(String::capacity),
            #[cfg(feature = "cluster")]
            self.aligned_replay.capacity(),
            #[cfg(feature = "cluster")]
            self.aligned_replay
                .iter()
                .map(|(_, _, _, batch)| batch.heap_bytes()),
        )
    }

    fn encode(self, max_working_bytes: usize, context: &str) -> Result<Vec<u8>, DbError> {
        #[cfg(feature = "cluster")]
        let mut capture = self;
        #[cfg(not(feature = "cluster"))]
        let capture = self;
        #[cfg(feature = "cluster")]
        let mut remaining = max_working_bytes;
        #[cfg(not(feature = "cluster"))]
        let remaining = max_working_bytes;
        #[cfg(feature = "cluster")]
        {
            let mut aligned_replay = Vec::new();
            aligned_replay
                .try_reserve_exact(capture.aligned_replay.len())
                .map_err(|_| {
                    DbError::Checkpoint(format!(
                        "{context}: aligned replay roster cannot be reserved"
                    ))
                })?;
            let roster_bytes = aligned_replay
                .capacity()
                .checked_mul(std::mem::size_of::<(u64, JoinInputSide, i64, Vec<u8>)>())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "{context}: aligned replay roster accounting overflow"
                    ))
                })?;
            remaining = remaining.checked_sub(roster_bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: aligned replay roster exceeded its cumulative serialization budget"
                ))
            })?;

            while let Some((assignment, side, watermark, batch)) =
                capture.aligned_replay.pop_front()
            {
                let bytes = laminar_core::serialization::serialize_batches_stream_bounded(
                    batch.batch().schema().as_ref(),
                    std::iter::once(batch.batch()),
                    remaining,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "{context}: aligned replay serialization within the cumulative budget: {error}"
                    ))
                })?;
                remaining = remaining.checked_sub(bytes.capacity()).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "{context}: aligned replay byte accounting overflow"
                    ))
                })?;
                aligned_replay.push((assignment, side, watermark, bytes));
            }
            capture.checkpoint.aligned_replay = aligned_replay;
        }

        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(remaining),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&capture.checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: archive serialization exceeded the remaining {remaining}-byte cumulative budget: {error}"
                ))
            })
    }
}

#[derive(Clone, Copy)]
struct IntervalHandoffCut {
    left_watermark: i64,
    right_watermark: i64,
}

struct PreparedIntervalJoinTransition {
    replacements: Vec<(u32, Option<Box<IntervalJoinState>>)>,
    local_assignment: VnodeAssignmentSnapshot,
    handoff_cut: Option<IntervalHandoffCut>,
}

enum IntervalJoinTransitionCleanup {
    Aborted(PreparedIntervalJoinTransition),
    Published(PreparedIntervalJoinTransition),
}

pub(crate) struct IntervalJoinOperator {
    config: StreamJoinConfig,
    key_group_count: KeyGroupCount,
    local_assignment: VnodeAssignmentSnapshot,
    vnode_states: Vec<Option<Box<IntervalJoinState>>>,
    checkpointed_vnodes: Vec<bool>,
    dirty_vnodes: Vec<bool>,
    max_managed_state_bytes: usize,
    input_schemas: Option<(SchemaRef, SchemaRef)>,
    projection: ProjectingJoinState,
    #[cfg(feature = "cluster")]
    cluster_shuffle: Option<ClusterShuffleConfig>,
    #[cfg(feature = "cluster")]
    aligned_replay: VecDeque<(u64, JoinInputSide, i64, crate::operator::RetainedBatch)>,
    prepared_vnode_transition: Option<PreparedIntervalJoinTransition>,
    vnode_transition_cleanup: Option<IntervalJoinTransitionCleanup>,
    applied_left_watermark: i64,
    applied_right_watermark: i64,
    applied_left_idle: bool,
    applied_right_idle: bool,
}

impl IntervalJoinOperator {
    #[cfg(test)]
    pub(crate) fn new(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self::new_with_key_groups(
            name,
            config,
            projection_sql,
            ctx,
            KeyGroupCount::try_from(1_u16).expect("one test key group is valid"),
        )
    }

    pub(crate) fn new_with_key_groups(
        name: &str,
        config: StreamJoinConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
        key_group_count: KeyGroupCount,
    ) -> Self {
        let vnode_count = u32::from(key_group_count);
        let local_assignment =
            VnodeRegistry::single_owner(vnode_count, LOCAL_NODE_ID).versioned_snapshot();
        Self {
            config,
            key_group_count,
            local_assignment,
            vnode_states: std::iter::repeat_with(|| None)
                .take(usize::from(key_group_count.get()))
                .collect(),
            checkpointed_vnodes: vec![false; usize::from(key_group_count.get())],
            dirty_vnodes: vec![false; usize::from(key_group_count.get())],
            max_managed_state_bytes: crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES,
            input_schemas: None,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__interval_tmp"),
            #[cfg(feature = "cluster")]
            cluster_shuffle: None,
            #[cfg(feature = "cluster")]
            aligned_replay: VecDeque::new(),
            prepared_vnode_transition: None,
            vnode_transition_cleanup: None,
            applied_left_watermark: i64::MIN,
            applied_right_watermark: i64::MIN,
            applied_left_idle: false,
            applied_right_idle: false,
        }
    }

    pub(crate) fn set_input_schemas(&mut self, left: SchemaRef, right: SchemaRef) {
        debug_assert!(self.vnode_states.iter().all(Option::is_none));
        self.input_schemas = Some((left, right));
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn attach_cluster_shuffle(&mut self, config: ClusterShuffleConfig) {
        debug_assert!(self.vnode_states.iter().all(Option::is_none));
        self.key_group_count = KeyGroupCount::try_from(config.registry.vnode_count())
            .expect("vnode registry count must fit the checkpoint key-group ABI");
        self.vnode_states
            .resize_with(config.registry.vnode_count() as usize, || None);
        self.checkpointed_vnodes
            .resize(config.registry.vnode_count() as usize, false);
        self.dirty_vnodes
            .resize(config.registry.vnode_count() as usize, false);
        self.local_assignment = config.registry.versioned_snapshot();
        self.cluster_shuffle = Some(config);
    }

    fn accounted_state_bytes(&self) -> usize {
        self.vnode_states
            .capacity()
            .saturating_mul(std::mem::size_of::<Option<Box<IntervalJoinState>>>())
            .saturating_add(32)
            .saturating_add(std::mem::size_of::<VnodeAssignmentSnapshot>())
            .saturating_add(
                self.vnode_states
                    .iter()
                    .flatten()
                    .fold(0usize, |total, state| {
                        total.saturating_add(state.accounted_state_bytes())
                    }),
            )
    }

    fn transition_accounted_bytes(transition: &PreparedIntervalJoinTransition) -> usize {
        transition
            .replacements
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, Option<Box<IntervalJoinState>>)>())
            .saturating_add(32)
            .saturating_add(std::mem::size_of::<VnodeAssignmentSnapshot>())
            .saturating_add(
                transition
                    .replacements
                    .iter()
                    .filter_map(|(_, state)| state.as_ref())
                    .fold(0usize, |total, state| {
                        total.saturating_add(state.accounted_state_bytes())
                    }),
            )
    }

    fn capture_operator_checkpoint(
        &self,
        max_capture_bytes: u64,
    ) -> Result<Option<IntervalJoinOperatorCheckpointCapture>, DbError> {
        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_none() && !self.aligned_replay.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] has cluster replay without an attached cluster scope",
                self.projection.op_name
            )));
        }
        #[cfg(feature = "cluster")]
        let aligned_replay_is_empty = self.aligned_replay.is_empty();
        #[cfg(not(feature = "cluster"))]
        let aligned_replay_is_empty = true;

        if aligned_replay_is_empty
            && self.applied_left_watermark == i64::MIN
            && self.applied_right_watermark == i64::MIN
            && !self.applied_left_idle
            && !self.applied_right_idle
        {
            return Ok(None);
        }
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        let preflight_bytes = IntervalJoinOperatorCheckpointCapture::calculate_retained_bytes_for(
            self.config.left_keys.len(),
            self.config.right_keys.len(),
            self.config
                .left_keys
                .iter()
                .chain(&self.config.right_keys)
                .chain([
                    &self.config.left_time_column,
                    &self.config.right_time_column,
                    &self.config.left_table,
                    &self.config.right_table,
                ])
                .map(String::len),
            #[cfg(feature = "cluster")]
            self.aligned_replay.len(),
            #[cfg(feature = "cluster")]
            self.aligned_replay
                .iter()
                .map(|(_, _, _, batch)| batch.heap_bytes()),
        )?;
        if preflight_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] whole checkpoint capture requires at least {preflight_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }
        #[cfg(feature = "cluster")]
        let assignment_identity = self
            .cluster_shuffle
            .as_ref()
            .map(|_| self.checkpoint_assignment_identity())
            .transpose()?;
        #[cfg(not(feature = "cluster"))]
        let assignment_identity: Option<(u64, [u8; 32], u64)> = None;
        let checkpoint = IntervalJoinOperatorCheckpoint {
            version: OPERATOR_CHECKPOINT_VERSION,
            join_type: join_type_tag(self.config.join_type),
            left_keys: self.config.left_keys.clone(),
            right_keys: self.config.right_keys.clone(),
            left_time_column: self.config.left_time_column.clone(),
            right_time_column: self.config.right_time_column.clone(),
            left_table: self.config.left_table.clone(),
            right_table: self.config.right_table.clone(),
            bound_ms,
            aligned_replay: Vec::new(),
            applied_left_watermark: self.applied_left_watermark,
            applied_right_watermark: self.applied_right_watermark,
            applied_left_idle: self.applied_left_idle,
            applied_right_idle: self.applied_right_idle,
            assignment_version: assignment_identity.map(|identity| identity.0),
            owner_map_digest: assignment_identity.map(|identity| identity.1),
            participant_id: assignment_identity.map(|identity| identity.2),
        };
        #[cfg(feature = "cluster")]
        let mut aligned_replay = VecDeque::new();
        #[cfg(feature = "cluster")]
        aligned_replay
            .try_reserve_exact(self.aligned_replay.len())
            .map_err(|_| {
                DbError::Checkpoint(format!(
                    "interval join [{}] aligned replay capture roster cannot be reserved",
                    self.projection.op_name
                ))
            })?;
        let retained_bytes = IntervalJoinOperatorCheckpointCapture::calculate_retained_bytes_for(
            checkpoint.left_keys.capacity(),
            checkpoint.right_keys.capacity(),
            checkpoint
                .left_keys
                .iter()
                .chain(&checkpoint.right_keys)
                .chain([
                    &checkpoint.left_time_column,
                    &checkpoint.right_time_column,
                    &checkpoint.left_table,
                    &checkpoint.right_table,
                ])
                .map(String::capacity),
            #[cfg(feature = "cluster")]
            aligned_replay.capacity(),
            #[cfg(feature = "cluster")]
            self.aligned_replay
                .iter()
                .map(|(_, _, _, batch)| batch.heap_bytes()),
        )?;
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] whole checkpoint capture retains {retained_bytes} bytes; capture headroom is {max_capture_bytes} bytes",
                self.projection.op_name
            )));
        }
        #[cfg(feature = "cluster")]
        aligned_replay.extend(self.aligned_replay.iter().map(
            |(assignment, side, watermark, batch)| (*assignment, *side, *watermark, batch.clone()),
        ));
        let capture = IntervalJoinOperatorCheckpointCapture {
            checkpoint,
            #[cfg(feature = "cluster")]
            aligned_replay,
            retained_bytes,
        };
        debug_assert_eq!(capture.calculate_retained_bytes()?, retained_bytes);
        Ok(Some(capture))
    }

    fn encode_state_capture(
        capture: IntervalJoinCheckpointCapture,
        context: &str,
        max_encoded_bytes: usize,
    ) -> Result<EncodedStateFrame, DbError> {
        let checkpoint = capture.encode(max_encoded_bytes)?;
        let retained_checkpoint_bytes = checkpoint.retained_ipc_bytes()?;
        let archive_budget = max_encoded_bytes
            .checked_sub(retained_checkpoint_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: encoded checkpoint retains {retained_checkpoint_bytes} bytes; state-frame budget is {max_encoded_bytes} bytes"
                ))
            })?;
        let mut bounded = laminar_core::serialization::BoundedBytesWriter::new(archive_budget);
        let mut header = [0_u8; VNODE_FRAME_HEADER_LEN];
        header[0] = PRESENT_VNODE;
        std::io::Write::write_all(&mut bounded, &header).map_err(|error| {
            DbError::Checkpoint(format!("{context}: vnode frame header: {error}"))
        })?;
        let writer = rkyv::ser::writer::IoWriter::new(bounded);
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| EncodedStateFrame::from_vec(bytes.into_inner().into_vec()))
            .map_err(|error| {
                DbError::Checkpoint(format!(
                    "{context}: archive serialization exceeded its {archive_budget}-byte headroom within the {max_encoded_bytes}-byte state-frame budget: {error}"
                ))
            })
    }

    #[cfg(test)]
    fn serialize_state(
        state: &mut IntervalJoinState,
        config: &StreamJoinConfig,
        context: &str,
        max_encoded_bytes: usize,
    ) -> Result<Vec<u8>, DbError> {
        Self::encode_state_capture(
            state.capture_checkpoint(config)?,
            context,
            max_encoded_bytes,
        )
        .map(|bytes| bytes.into_bytes().to_vec())
    }

    fn deserialize_state(
        bytes: &[u8],
        config: &StreamJoinConfig,
        context: &str,
        max_state_bytes: usize,
        cut: Option<IntervalHandoffCut>,
    ) -> Result<IntervalJoinState, DbError> {
        let checkpoint = rkyv::from_bytes::<JoinStateCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))?;
        if let Some(cut) = cut {
            Self::validate_handoff_cutoffs(
                checkpoint.left_evicted_cutoff,
                checkpoint.right_evicted_cutoff,
                checkpoint.left_buffer_rows != 0,
                checkpoint.right_buffer_rows != 0,
                config,
                context,
                cut,
            )?;
        }
        IntervalJoinState::from_checkpoint(&checkpoint, config, max_state_bytes)
            .map_err(|error| DbError::Checkpoint(format!("{context}: {error}")))
    }

    fn decode_vnode_frame(
        bytes: &[u8],
        config: &StreamJoinConfig,
        context: &str,
        max_state_bytes: usize,
        cut: Option<IntervalHandoffCut>,
    ) -> Result<Option<IntervalJoinState>, DbError> {
        if bytes.len() < VNODE_FRAME_HEADER_LEN {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode frame header is truncated"
            )));
        }
        let (header, payload) = bytes.split_at(VNODE_FRAME_HEADER_LEN);
        if header[1..].iter().any(|byte| *byte != 0) {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode frame header is malformed"
            )));
        }
        let tag = header[0];
        match tag {
            ABSENT_VNODE if payload.is_empty() => Ok(None),
            ABSENT_VNODE => Err(DbError::Checkpoint(format!(
                "{context}: absent vnode frame has a payload"
            ))),
            PRESENT_VNODE if payload.is_empty() => Err(DbError::Checkpoint(format!(
                "{context}: present vnode frame has no payload"
            ))),
            PRESENT_VNODE => {
                Self::deserialize_state(payload, config, context, max_state_bytes, cut).map(Some)
            }
            _ => Err(DbError::Checkpoint(format!(
                "{context}: vnode frame has unknown tag {tag}"
            ))),
        }
    }

    fn validate_handoff_cutoffs(
        left_evicted_cutoff: i64,
        right_evicted_cutoff: i64,
        left_nonempty: bool,
        right_nonempty: bool,
        config: &StreamJoinConfig,
        context: &str,
        cut: IntervalHandoffCut,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let expected_left_cutoff = cut.right_watermark.saturating_sub(bound_ms);
        let expected_right_cutoff = cut.left_watermark;
        if left_evicted_cutoff > expected_left_cutoff
            || right_evicted_cutoff > expected_right_cutoff
            || (left_nonempty && left_evicted_cutoff != expected_left_cutoff)
            || (right_nonempty && right_evicted_cutoff != expected_right_cutoff)
        {
            return Err(DbError::Checkpoint(format!(
                "{context}: vnode eviction state is inconsistent with the portable handoff cut"
            )));
        }
        Ok(())
    }

    fn validate_checkpoint_config(
        &self,
        checkpoint: &IntervalJoinOperatorCheckpoint,
    ) -> Result<(), DbError> {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "interval join [{}] configured time bound exceeds the supported millisecond range",
                self.projection.op_name
            ))
        })?;
        if checkpoint.version != OPERATOR_CHECKPOINT_VERSION
            || checkpoint.join_type != join_type_tag(self.config.join_type)
            || checkpoint.left_keys != self.config.left_keys
            || checkpoint.right_keys != self.config.right_keys
            || checkpoint.left_time_column != self.config.left_time_column
            || checkpoint.right_time_column != self.config.right_time_column
            || checkpoint.left_table != self.config.left_table
            || checkpoint.right_table != self.config.right_table
            || checkpoint.bound_ms != bound_ms
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint version or configuration does not match the operator",
                self.projection.op_name
            )));
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_assignment_identity(&self) -> Result<(u64, [u8; 32], u64), DbError> {
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has no cluster assignment",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners = assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect::<Vec<_>>();
        let owner_map_digest =
            laminar_core::checkpoint::CheckpointAssignmentFence::owner_map_digest(
                u32::from(self.key_group_count),
                &owners,
            );
        let sender_digest = config.sender.active_assignment_digest();
        let receiver_digest = config.receiver.active_assignment_digest();
        if assignment.version() != self.local_assignment.version()
            || assignment.owners() != self.local_assignment.owners()
            || config.sender.local_id() != config.self_id.0
            || config.receiver.local_id() != config.self_id.0
            || config.sender.incarnation() != config.receiver.incarnation()
            || config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
            || sender_digest.is_none()
            || sender_digest != receiver_digest
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint assignment is not active",
                self.projection.op_name
            )));
        }
        Ok((assignment.version(), owner_map_digest, config.self_id.0))
    }

    #[cfg(feature = "cluster")]
    fn portable_handoff_cut(
        &self,
        transition: &ManagedVnodeTransition<'_>,
        requires_handoff_cut: bool,
    ) -> Result<Option<IntervalHandoffCut>, DbError> {
        if !requires_handoff_cut {
            return Ok(None);
        }
        let mut donors = std::collections::BTreeSet::new();
        for restore in transition.restores {
            let predecessor_owner = match transition.mode {
                ManagedVnodeTransitionMode::Live => self
                    .local_assignment
                    .owners()
                    .get(restore.vnode as usize)
                    .copied(),
                ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                    predecessor_owners.get(restore.vnode as usize).copied()
                }
            };
            if !transition.predecessor.contains(restore.participant_id)
                || predecessor_owner.map(|owner| owner.0) != Some(restore.participant_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] vnode {} restore has invalid donor {}",
                    self.projection.op_name, restore.vnode, restore.participant_id
                )));
            }
            donors.insert(restore.participant_id);
        }
        if donors.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] handoff transition has no acquired vnode frames",
                self.projection.op_name
            )));
        }
        if transition.whole_restores.is_empty() {
            return Ok(None);
        }

        let mut whole_donors = std::collections::BTreeSet::new();
        let mut common: Option<IntervalHandoffCut> = None;
        for restore in transition.whole_restores {
            if !whole_donors.insert(restore.participant_id)
                || restore.state.len() > self.max_managed_state_bytes
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] has an invalid whole frame for donor {}",
                    self.projection.op_name, restore.participant_id
                )));
            }
            let checkpoint =
                rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
                    restore.state,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] donor {} whole checkpoint: {error}",
                        self.projection.op_name, restore.participant_id
                    ))
                })?;
            self.validate_checkpoint_config(&checkpoint)?;
            if !checkpoint.aligned_replay.is_empty()
                || checkpoint.assignment_version != Some(transition.predecessor.assignment_version)
                || checkpoint.owner_map_digest != Some(transition.predecessor.assignment_digest)
                || checkpoint.participant_id != Some(restore.participant_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] donor {} whole checkpoint is not a portable predecessor cut",
                    self.projection.op_name, restore.participant_id
                )));
            }
            let cut = IntervalHandoffCut {
                left_watermark: checkpoint.applied_left_watermark,
                right_watermark: checkpoint.applied_right_watermark,
            };
            if let Some(expected) = &mut common {
                if expected.left_watermark != cut.left_watermark
                    || expected.right_watermark != cut.right_watermark
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] donor whole checkpoints disagree on the handoff watermarks",
                        self.projection.op_name
                    )));
                }
            } else {
                common = Some(cut);
            }
        }
        if whole_donors != donors {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] whole checkpoints do not exactly cover acquired vnode donors",
                self.projection.op_name
            )));
        }
        Ok(common)
    }

    #[cfg(feature = "cluster")]
    fn restored_handoff_cut_evidence(
        &self,
        state: &IntervalJoinState,
        context: &str,
    ) -> Result<Option<IntervalHandoffCut>, DbError> {
        let (left_rows, right_rows) = state.buffered_rows();
        let (left_evicted_cutoff, right_evicted_cutoff) = state.evicted_cutoffs();
        if left_rows == 0
            && right_rows == 0
            && left_evicted_cutoff == i64::MIN
            && right_evicted_cutoff == i64::MIN
        {
            return Ok(None);
        }
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context}: configured time bound exceeds the supported millisecond range"
            ))
        })?;
        let right_watermark = if left_evicted_cutoff == i64::MIN {
            i64::MIN
        } else {
            left_evicted_cutoff.checked_add(bound_ms).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "{context}: left eviction cutoff cannot be converted to a handoff watermark"
                ))
            })?
        };
        Ok(Some(IntervalHandoffCut {
            left_watermark: right_evicted_cutoff,
            right_watermark,
        }))
    }

    fn execute_shard_cycle(
        &mut self,
        vnode: u32,
        left: &[RecordBatch],
        right: &[RecordBatch],
        left_watermark: i64,
        right_watermark: i64,
        accounted_total: &mut usize,
        output_budget: &mut IntervalJoinOutputBudget,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_admission_watermark = self.applied_left_watermark;
        let right_admission_watermark = self.applied_right_watermark;
        let state_slots = self.vnode_states.len();
        let shard_bytes = self
            .vnode_states
            .get(vnode as usize)
            .and_then(Option::as_ref)
            .map_or(0, |state| state.accounted_state_bytes());
        let other_state_bytes = (*accounted_total).checked_sub(shard_bytes).ok_or_else(|| {
            DbError::BackpressureFail(format!(
                "interval join [{}] retained-state accounting underflow",
                self.projection.op_name
            ))
        })?;
        let shard_limit = self
            .max_managed_state_bytes
            .checked_sub(other_state_bytes)
            .ok_or_else(|| {
                DbError::BackpressureFail(format!(
                    "interval join [{}] already exceeds its {}-byte retained-state limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        let has_input = left.iter().any(|batch| batch.num_rows() > 0)
            || right.iter().any(|batch| batch.num_rows() > 0);
        let slot = self.vnode_states.get_mut(vnode as usize).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] routed vnode {vnode} outside its {}-vnode state table",
                self.projection.op_name, state_slots
            ))
        })?;
        if slot.is_none() && !has_input {
            return Ok(Vec::new());
        }
        if slot.is_none() && IntervalJoinState::new().accounted_state_bytes() > shard_limit {
            return Err(DbError::BackpressureFail(format!(
                "interval join [{}] cannot allocate vnode {vnode} state within its {shard_limit}-byte remaining limit",
                self.projection.op_name
            )));
        }
        if slot.is_none() {
            let mut state = IntervalJoinState::new();
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            *slot = Some(Box::new(state));
        }
        let state = slot.as_mut().expect("interval join state initialized");
        let result = execute_interval_join_cycle(
            state,
            left,
            right,
            &self.config,
            left_admission_watermark,
            right_admission_watermark,
            left_watermark,
            right_watermark,
            shard_limit,
            output_budget,
        );
        *accounted_total = other_state_bytes.saturating_add(state.accounted_state_bytes());
        if result.is_ok() {
            self.dirty_vnodes[vnode as usize] = true;
        }
        result
    }

    fn execute_routed_shards(
        &mut self,
        routed: BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        left_watermark: i64,
        right_watermark: i64,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let mut accounted_total = self.accounted_state_bytes();
        let mut output_budget = IntervalJoinOutputBudget::default();
        let mut output = Vec::new();
        let mut prior_shard_completed = false;
        for (vnode, [left, right]) in routed {
            match self.execute_shard_cycle(
                vnode,
                &left,
                &right,
                left_watermark,
                right_watermark,
                &mut accounted_total,
                &mut output_budget,
            ) {
                Ok(shard_output) => {
                    output.extend(shard_output);
                    prior_shard_completed = true;
                }
                Err(error) if prior_shard_completed => {
                    let error = if error.requires_pipeline_recovery()
                        || error.requires_pipeline_halt()
                    {
                        error
                    } else {
                        DbError::StatefulOperatorPartialApply(format!(
                            "interval join [{}] admitted an earlier vnode before vnode {vnode} failed: {error}",
                            self.projection.op_name
                        ))
                    };
                    return Err(error);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(output)
    }

    async fn project_output(
        &mut self,
        join_result: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.projection.apply(join_result).await.map_err(|error| {
            if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
                error
            } else {
                DbError::StatefulOperatorPartialApply(format!(
                    "interval join [{}] admitted input before post-projection failed: {error}",
                    self.projection.op_name
                ))
            }
        })
    }

    fn push_routed_batch(
        routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>,
        vnode: u32,
        side: JoinInputSide,
        batch: RecordBatch,
    ) {
        let port = match side {
            JoinInputSide::Left => 0,
            JoinInputSide::Right => 1,
        };
        routed.entry(vnode).or_default()[port].push(batch);
    }

    fn add_resident_vnodes(&self, routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>) {
        for (state, vnode) in self.vnode_states.iter().zip(0_u32..) {
            if state.is_some() {
                routed.entry(vnode).or_default();
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn post_shuffle_admission_error(&self, outbound_admitted: bool, error: DbError) -> DbError {
        if !outbound_admitted || error.requires_pipeline_recovery() {
            return error;
        }
        DbError::ShufflePartialSend(format!(
            "interval join [{}] failed after outbound shuffle data was admitted: {error}",
            self.projection.op_name
        ))
    }

    fn prevalidate_inputs(&self, inputs: &[Vec<RecordBatch>]) -> Result<(), DbError> {
        if self.config.left_keys.is_empty()
            || self.config.left_keys.len() != self.config.right_keys.len()
            || self.config.left_time_column.is_empty()
            || self.config.right_time_column.is_empty()
            || self.config.time_bound.is_zero()
            || i64::try_from(self.config.time_bound.as_millis()).is_err()
        {
            return Err(DbError::InvalidOperation(
                "interval join requires equal non-empty ordered key vectors, both event-time columns, and a positive finite time bound"
                    .into(),
            ));
        }

        for (port, side, key_names, time_name) in [
            (
                0,
                "left",
                self.config.left_keys.as_slice(),
                self.config.left_time_column.as_str(),
            ),
            (
                1,
                "right",
                self.config.right_keys.as_slice(),
                self.config.right_time_column.as_str(),
            ),
        ] {
            let expected_schema =
                self.input_schemas
                    .as_ref()
                    .map(|schemas| if port == 0 { &schemas.0 } else { &schemas.1 });
            for (batch_index, batch) in inputs.get(port).into_iter().flatten().enumerate() {
                if let Some(expected) = expected_schema {
                    if batch.schema().as_ref() != expected.as_ref() {
                        return Err(DbError::SchemaMismatch(format!(
                            "interval join [{0}] {side} batch {batch_index} does not match its declared schema",
                            self.projection.op_name
                        )));
                    }
                }
                for key_name in key_names {
                    let index = batch.schema().index_of(key_name).map_err(|error| {
                        DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} key '{key_name}': {error}",
                            self.projection.op_name
                        ))
                    })?;
                    if !matches!(
                        batch.column(index).data_type(),
                        DataType::Utf8 | DataType::Int64
                    ) {
                        return Err(DbError::SchemaMismatch(format!(
                            "interval join [{}] {side} key '{key_name}' must be Utf8 or Int64, found {}",
                            self.projection.op_name,
                            batch.column(index).data_type()
                        )));
                    }
                }
                let time_index = batch.schema().index_of(time_name).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join [{}] {side} event-time column '{time_name}': {error}",
                        self.projection.op_name
                    ))
                })?;
                let time_column = batch.column(time_index);
                if !matches!(time_column.data_type(), DataType::Timestamp(_, _))
                    || time_column.null_count() != 0
                {
                    return Err(DbError::SchemaMismatch(format!(
                        "interval join [{}] {side} event-time column '{time_name}' must be a non-null Timestamp, found {} with {} nulls",
                        self.projection.op_name,
                        time_column.data_type(),
                        time_column.null_count()
                    )));
                }
            }
        }
        Ok(())
    }

    fn route_local_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<BTreeMap<u32, [Vec<RecordBatch>; 2]>, DbError> {
        self.prevalidate_inputs(inputs)?;
        let mut routed = BTreeMap::new();
        let vnode_count = u32::from(self.key_group_count);

        for (side, batches) in [
            (
                JoinInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                JoinInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            let (side_name, key_names) = match side {
                JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
                JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
            };
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let key_indices = key_names
                    .iter()
                    .map(|key_name| {
                        batch.schema().index_of(key_name).map_err(|error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side_name} routing key '{key_name}': {error}",
                                self.projection.op_name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_vnodes =
                    laminar_core::shuffle::row_vnodes(batch, &key_indices, vnode_count).map_err(
                        |error| {
                            DbError::Pipeline(format!(
                                "interval join [{}] {side_name} routing: {error}",
                                self.projection.op_name
                            ))
                        },
                    )?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &self.local_assignment,
                    LOCAL_NODE_ID,
                )
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "interval join [{}] {side_name} routing: {error}",
                        self.projection.op_name
                    ))
                })?;
                if !plan.remote.is_empty() {
                    return Err(DbError::Pipeline(format!(
                        "interval join [{}] local topology routed rows off-node",
                        self.projection.op_name
                    )));
                }
                for route in plan.local {
                    Self::push_routed_batch(&mut routed, route.vnode, side, route.batch);
                }
            }
        }
        Ok(routed)
    }

    #[cfg(feature = "cluster")]
    fn route_owned_batch(
        &self,
        config: &ClusterShuffleConfig,
        assignment: &laminar_core::state::VnodeAssignmentSnapshot,
        side: JoinInputSide,
        batch: &RecordBatch,
        declared_vnodes: Option<&[u32]>,
        routed: &mut BTreeMap<u32, [Vec<RecordBatch>; 2]>,
    ) -> Result<(), DbError> {
        let (side_name, key_names) = match side {
            JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
            JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
        };
        let key_indices = key_names
            .iter()
            .map(|key_name| {
                batch.schema().index_of(key_name).map_err(|error| {
                    DbError::SchemaMismatch(format!(
                        "interval join [{}] {side_name} routing key '{key_name}': {error}",
                        self.projection.op_name
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let row_vnodes =
            laminar_core::shuffle::row_vnodes(batch, &key_indices, config.registry.vnode_count())
                .map_err(|error| {
                crate::operator::shuffle_routing_error(
                    &format!(
                        "interval join [{}] {side_name} routing",
                        self.projection.op_name
                    ),
                    &error,
                )
            })?;
        let plan = laminar_core::shuffle::route_checkpointed_batch(
            batch,
            &row_vnodes,
            assignment,
            config.self_id,
        )
        .map_err(|error| {
            crate::operator::shuffle_routing_error(
                &format!(
                    "interval join [{}] {side_name} ownership validation",
                    self.projection.op_name
                ),
                &error,
            )
        })?;
        if !plan.remote.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received {side_name} shuffle data no longer owned by this node",
                self.projection.op_name
            )));
        }
        if let Some(declared) = declared_vnodes {
            let actual: Vec<u32> = plan.local.iter().map(|route| route.vnode).collect();
            if actual.as_slice() != declared {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] {side_name} shuffle vnode metadata {declared:?} does not match decoded rows {actual:?}",
                    self.projection.op_name
                )));
            }
        }
        for route in plan.local {
            Self::push_routed_batch(routed, route.vnode, side, route.batch);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    async fn route_cluster_inputs(
        &self,
        inputs: &[Vec<RecordBatch>],
    ) -> Result<
        (
            BTreeMap<u32, [Vec<RecordBatch>; 2]>,
            Vec<laminar_core::shuffle::ReceivedBatch>,
            bool,
        ),
        DbError,
    > {
        self.prevalidate_inputs(inputs)?;
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has no cluster shuffle scope",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let mut routed = BTreeMap::new();
        let mut outbound = Vec::new();

        for (side, batches) in [
            (
                JoinInputSide::Left,
                inputs.first().map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
            (
                JoinInputSide::Right,
                inputs.get(1).map_or(&[] as &[RecordBatch], Vec::as_slice),
            ),
        ] {
            let (side_name, key_names) = match side {
                JoinInputSide::Left => ("left", self.config.left_keys.as_slice()),
                JoinInputSide::Right => ("right", self.config.right_keys.as_slice()),
            };
            let stage = format!("{}::{side_name}", self.projection.op_name);
            for batch in batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let key_indices = key_names
                    .iter()
                    .map(|key_name| {
                        batch.schema().index_of(key_name).map_err(|error| {
                            DbError::SchemaMismatch(format!(
                                "interval join [{}] {side_name} routing key '{key_name}': {error}",
                                self.projection.op_name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_vnodes = laminar_core::shuffle::row_vnodes(
                    batch,
                    &key_indices,
                    config.registry.vnode_count(),
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!(
                            "interval join [{}] {side_name} routing",
                            self.projection.op_name
                        ),
                        &error,
                    )
                })?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &row_vnodes,
                    &assignment,
                    config.self_id,
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!(
                            "interval join [{}] {side_name} routing",
                            self.projection.op_name
                        ),
                        &error,
                    )
                })?;
                for route in plan.local {
                    Self::push_routed_batch(&mut routed, route.vnode, side, route.batch);
                }
                for route in plan.remote {
                    outbound.push((
                        route.owner.0,
                        laminar_core::shuffle::ShuffleMessage::checkpointed_routed(
                            stage.clone(),
                            route.routed_vnodes,
                            route.batch,
                        ),
                    ));
                }
            }
        }

        let outbound_admitted = !outbound.is_empty();
        crate::operator::send_shuffle_plan(
            &config.sender,
            assignment.version(),
            outbound,
            &format!("interval join [{}] shuffle", self.projection.op_name),
        )
        .await?;

        let mut admitted = Vec::new();
        for side in [JoinInputSide::Left, JoinInputSide::Right] {
            let side_name = match side {
                JoinInputSide::Left => "left",
                JoinInputSide::Right => "right",
            };
            let stage = format!("{}::{side_name}", self.projection.op_name);
            let received = config.receiver.drain_checkpointed_data_for(&stage);
            for batch in &received {
                self.route_owned_batch(
                    config,
                    &assignment,
                    side,
                    batch.batch(),
                    Some(batch.routed_vnodes()),
                    &mut routed,
                )
                .map_err(|error| self.post_shuffle_admission_error(outbound_admitted, error))?;
            }
            admitted.extend(received);
        }
        Ok((routed, admitted, outbound_admitted))
    }

    #[cfg(feature = "cluster")]
    async fn process_cluster(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        left_frontier: InputFrontier,
        right_frontier: InputFrontier,
    ) -> Result<Vec<RecordBatch>, DbError> {
        if !self.aligned_replay.is_empty() {
            return self.execute_aligned_replay().await;
        }
        let left_watermark = left_frontier.watermark.unwrap_or(i64::MIN);
        let right_watermark = right_frontier.watermark.unwrap_or(i64::MIN);
        let (mut routed, _admitted, outbound_admitted) = self.route_cluster_inputs(inputs).await?;
        let frontier_advanced = left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark;
        if frontier_advanced {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self
            .execute_routed_shards(routed, left_watermark, right_watermark)
            .map_err(|error| self.post_shuffle_admission_error(outbound_admitted, error))?;
        let output = self
            .project_output(output)
            .await
            .map_err(|error| self.post_shuffle_admission_error(outbound_admitted, error))?;
        self.applied_left_watermark = self.applied_left_watermark.max(left_watermark);
        self.applied_right_watermark = self.applied_right_watermark.max(right_watermark);
        self.applied_left_idle = left_frontier.idle;
        self.applied_right_idle = right_frontier.idle;
        Ok(output)
    }

    #[cfg(feature = "cluster")]
    async fn execute_aligned_replay(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] has aligned replay without cluster ownership",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        if config.sender.assignment_version() != assignment.version()
            || config.receiver.assignment_version() != assignment.version()
            || self
                .aligned_replay
                .iter()
                .any(|(version, _, _, _)| *version != assignment.version())
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] aligned replay crossed its assignment boundary",
                self.projection.op_name
            )));
        }
        let (_, side, watermark, batch) =
            self.aligned_replay.front().cloned().ok_or_else(|| {
                DbError::Checkpoint("interval join replay queue became empty".into())
            })?;
        let mut routed = BTreeMap::new();
        self.route_owned_batch(config, &assignment, side, batch.batch(), None, &mut routed)?;
        let (left_watermark, right_watermark) = match side {
            JoinInputSide::Left => (watermark, self.applied_right_watermark),
            JoinInputSide::Right => (self.applied_left_watermark, watermark),
        };
        if left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark
        {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self.execute_routed_shards(routed, left_watermark, right_watermark)?;
        let output = self.project_output(output).await.map_err(|error| {
            DbError::Checkpoint(format!(
                "interval join [{}] aligned replay requires recovery: {error}",
                self.projection.op_name
            ))
        })?;
        self.aligned_replay.pop_front().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] aligned replay disappeared after emission",
                self.projection.op_name
            ))
        })?;
        match side {
            JoinInputSide::Left => {
                self.applied_left_watermark = self.applied_left_watermark.max(watermark);
                self.applied_left_idle = false;
            }
            JoinInputSide::Right => {
                self.applied_right_watermark = self.applied_right_watermark.max(watermark);
                self.applied_right_idle = false;
            }
        }
        Ok(output)
    }
}

#[async_trait]
impl GraphOperator for IntervalJoinOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::bounded_interval_join()
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_aligned_replay_pending(&self) -> bool {
        !self.aligned_replay.is_empty()
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        let (prepared, retired) = {
            let prepared = self
                .prepared_vnode_transition
                .as_ref()
                .map_or(0, Self::transition_accounted_bytes);
            let (prepared, retired) = match self.vnode_transition_cleanup.as_ref() {
                Some(IntervalJoinTransitionCleanup::Aborted(transition)) => (
                    prepared.saturating_add(Self::transition_accounted_bytes(transition)),
                    0,
                ),
                Some(IntervalJoinTransitionCleanup::Published(transition)) => {
                    (prepared, Self::transition_accounted_bytes(transition))
                }
                None => (prepared, 0),
            };
            (prepared, retired)
        };
        Some(ManagedStateAccountingSnapshot {
            live: self.accounted_state_bytes(),
            prepared,
            retired,
        })
    }

    fn set_managed_state_budget(&mut self, bytes: usize) {
        self.max_managed_state_bytes = bytes;
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_watermark = watermarks.first().copied().unwrap_or(i64::MIN);
        let right_watermark = watermarks.get(1).copied().unwrap_or(left_watermark);
        self.process_with_frontiers(
            inputs,
            &[
                InputFrontier {
                    watermark: (left_watermark != i64::MIN).then_some(left_watermark),
                    idle: false,
                },
                InputFrontier {
                    watermark: (right_watermark != i64::MIN).then_some(right_watermark),
                    idle: false,
                },
            ],
        )
        .await
    }

    async fn process_with_frontiers(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_frontier = frontiers.first().copied().unwrap_or_default();
        let right_frontier = frontiers.get(1).copied().unwrap_or(left_frontier);
        let left_watermark = left_frontier.watermark.unwrap_or(i64::MIN);
        let right_watermark = right_frontier.watermark.unwrap_or(i64::MIN);

        #[cfg(feature = "cluster")]
        if self.cluster_shuffle.is_some() {
            return self
                .process_cluster(inputs, left_frontier, right_frontier)
                .await;
        }

        let mut routed = self.route_local_inputs(inputs)?;
        let frontier_advanced = left_watermark > self.applied_left_watermark
            || right_watermark > self.applied_right_watermark;
        if frontier_advanced {
            self.add_resident_vnodes(&mut routed);
        }
        let output = self.execute_routed_shards(routed, left_watermark, right_watermark)?;
        let output = self.project_output(output).await?;
        self.applied_left_watermark = self.applied_left_watermark.max(left_watermark);
        self.applied_right_watermark = self.applied_right_watermark.max(right_watermark);
        self.applied_left_idle = left_frontier.idle;
        self.applied_right_idle = right_frontier.idle;
        Ok(output)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let Some(capture) = self.capture_operator_checkpoint(u64::MAX)? else {
            return Ok(None);
        };
        let context = format!(
            "interval join [{}] whole checkpoint serialization",
            self.projection.op_name
        );
        capture
            .encode(self.max_managed_state_bytes, &context)
            .map(|data| Some(OperatorCheckpoint { data }))
    }

    fn checkpoint_capture(
        &mut self,
        max_capture_bytes: u64,
    ) -> Result<Option<StateFrameCapture>, DbError> {
        let Some(capture) = self.capture_operator_checkpoint(max_capture_bytes)? else {
            return Ok(None);
        };
        let retained_bytes = capture.retained_bytes();
        let max_managed_state_bytes = self.max_managed_state_bytes;
        let context = format!(
            "interval join [{}] whole checkpoint serialization",
            self.projection.op_name
        );
        Ok(Some(StateFrameCapture::deferred(
            retained_bytes,
            move |remaining| {
                capture
                    .encode(remaining.min(max_managed_state_bytes), &context)
                    .map(EncodedStateFrame::from_vec)
            },
        )))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        if self.applied_left_watermark != i64::MIN
            || self.applied_right_watermark != i64::MIN
            || self.applied_left_idle
            || self.applied_right_idle
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint restore was applied more than once",
                self.projection.op_name
            )));
        }
        if checkpoint.data.len() > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint payload is {} bytes; restore limit is {} bytes",
                self.projection.op_name,
                checkpoint.data.len(),
                self.max_managed_state_bytes
            )));
        }
        let checkpoint = rkyv::from_bytes::<IntervalJoinOperatorCheckpoint, rkyv::rancor::Error>(
            &checkpoint.data,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "interval join [{}] checkpoint deserialization: {error}",
                self.projection.op_name
            ))
        })?;
        self.validate_checkpoint_config(&checkpoint)?;
        #[cfg(feature = "cluster")]
        let expected_identity = self
            .cluster_shuffle
            .as_ref()
            .map(|_| self.checkpoint_assignment_identity())
            .transpose()?;
        #[cfg(not(feature = "cluster"))]
        let expected_identity: Option<(u64, [u8; 32], u64)> = None;
        if let Some(expected) = expected_identity {
            if checkpoint.assignment_version != Some(expected.0)
                || checkpoint.owner_map_digest != Some(expected.1)
                || checkpoint.participant_id != Some(expected.2)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] checkpoint assignment does not match the restored operator",
                    self.projection.op_name
                )));
            }
        } else if checkpoint.assignment_version.is_some()
            || checkpoint.owner_map_digest.is_some()
            || checkpoint.participant_id.is_some()
            || !checkpoint.aligned_replay.is_empty()
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] checkpoint contains cluster state without an attached cluster scope",
                self.projection.op_name
            )));
        }

        #[cfg(feature = "cluster")]
        let decoded_replay = checkpoint
            .aligned_replay
            .into_iter()
            .map(|(assignment, side, watermark, bytes)| {
                laminar_core::serialization::deserialize_batch_stream(&bytes)
                    .map(|batch| {
                        (
                            assignment,
                            side,
                            watermark,
                            crate::operator::RetainedBatch::local(batch),
                        )
                    })
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "interval join [{}] aligned replay restore: {error}",
                            self.projection.op_name
                        ))
                    })
            })
            .collect::<Result<VecDeque<_>, DbError>>()?;
        #[cfg(feature = "cluster")]
        if !self.aligned_replay.is_empty() && !decoded_replay.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] aligned replay was applied more than once",
                self.projection.op_name
            )));
        }

        self.applied_left_watermark = checkpoint.applied_left_watermark;
        self.applied_right_watermark = checkpoint.applied_right_watermark;
        self.applied_left_idle = checkpoint.applied_left_idle;
        self.applied_right_idle = checkpoint.applied_right_idle;
        #[cfg(feature = "cluster")]
        self.aligned_replay.extend(decoded_replay);

        Ok(())
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        let bound_ms = i64::try_from(self.config.time_bound.as_millis()).unwrap_or(i64::MAX);
        let right_only = matches!(
            self.config.join_type,
            laminar_sql::parser::join_parser::JoinType::RightSemi
                | laminar_sql::parser::join_parser::JoinType::RightAnti
        );
        let safe = if right_only {
            self.applied_left_watermark
                .min(self.applied_right_watermark)
        } else {
            self.applied_left_watermark
                .min(self.applied_right_watermark.saturating_sub(bound_ms))
        };
        let mut output = input.with_watermark_ceiling(Some(safe));
        output.idle = self.applied_left_idle && self.applied_right_idle;
        #[cfg(feature = "cluster")]
        let replay_hold = self
            .aligned_replay
            .iter()
            .map(|(_, side, watermark, _)| {
                if right_only || matches!(side, JoinInputSide::Left) {
                    *watermark
                } else {
                    watermark.saturating_sub(bound_ms)
                }
            })
            .min();
        #[cfg(feature = "cluster")]
        let output = output.held_at(replay_hold);
        output
    }

    #[cfg(feature = "cluster")]
    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        Some(self.output_frontier(InputFrontier {
            watermark: Some(i64::MAX),
            idle: false,
        }))
    }

    #[cfg(feature = "cluster")]
    fn wants_input(&self) -> bool {
        self.aligned_replay.is_empty()
    }

    #[cfg(feature = "cluster")]
    fn stage_checkpointed_shuffle(
        &mut self,
        stage: &str,
        batch: crate::operator::RetainedBatch,
        watermark: i64,
    ) -> Result<(), DbError> {
        if self.cluster_shuffle.is_none() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received shuffle data without cluster ownership",
                self.projection.op_name
            )));
        }
        let side = if stage == format!("{}::left", self.projection.op_name) {
            JoinInputSide::Left
        } else if stage == format!("{}::right", self.projection.op_name) {
            JoinInputSide::Right
        } else {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] rejected unknown shuffle stage '{stage}'",
                self.projection.op_name
            )));
        };
        let assignment = batch.assignment_version().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] received unscoped shuffle data",
                self.projection.op_name
            ))
        })?;
        self.aligned_replay
            .push_back((assignment, side, watermark, batch));
        Ok(())
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        if u32::from(self.key_group_count) != vnode_count
            || required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes.iter().any(|vnode| *vnode >= vnode_count)
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] received a non-canonical vnode roster {required_vnodes:?} for vnode_count {vnode_count}",
                self.projection.op_name
            )));
        }
        if let Some(unowned) = self
            .vnode_states
            .iter()
            .zip(0_u32..)
            .find_map(|(state, vnode)| {
                (state.is_some() && required_vnodes.binary_search(&vnode).is_err()).then_some(vnode)
            })
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] retained unowned vnode state {unowned}",
                self.projection.op_name
            )));
        }

        let capture_plan = required_vnodes
            .iter()
            .map(|vnode| {
                !self.checkpointed_vnodes[*vnode as usize] || self.dirty_vnodes[*vnode as usize]
            })
            .collect::<Vec<_>>();
        let absent_frame_bytes = required_vnodes
            .iter()
            .zip(&capture_plan)
            .filter(|(vnode, capture)| **capture && self.vnode_states[**vnode as usize].is_none())
            .count()
            .checked_mul(VNODE_FRAME_HEADER_LEN)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] absent vnode frame accounting overflow",
                    self.projection.op_name
                ))
            })?;
        let mut captured = Vec::with_capacity(required_vnodes.len());
        let mut retained_capture_bytes = 0_u64;
        let operator_remaining = Arc::new(AtomicUsize::new(
            self.max_managed_state_bytes
                .checked_sub(absent_frame_bytes)
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] absent vnode frames exceed their {}-byte checkpoint limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?,
        ));
        for (&vnode, capture) in required_vnodes.iter().zip(&capture_plan) {
            let state = if *capture {
                if let Some(state) = self.vnode_states[vnode as usize].as_mut() {
                    let remaining_capture_bytes = max_capture_bytes
                        .checked_sub(retained_capture_bytes)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join [{}] checkpoint captures exhausted their {max_capture_bytes}-byte capture budget",
                                self.projection.op_name
                            ))
                        })?;
                    let state_bytes =
                        u64::try_from(state.accounted_state_bytes()).unwrap_or(u64::MAX);
                    if state_bytes > remaining_capture_bytes {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] vnode {vnode} retains {state_bytes} bytes; remaining capture budget is {remaining_capture_bytes} bytes",
                            self.projection.op_name
                        )));
                    }
                    let state_capture = state.capture_checkpoint(&self.config)?;
                    let retained_bytes =
                        u64::try_from(state_capture.retained_bytes()).unwrap_or(u64::MAX);
                    retained_capture_bytes = retained_capture_bytes
                        .checked_add(retained_bytes)
                        .ok_or_else(|| {
                            DbError::Checkpoint(format!(
                                "interval join [{}] checkpoint capture byte accounting overflow",
                                self.projection.op_name
                            ))
                        })?;
                    if retained_capture_bytes > max_capture_bytes {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] checkpoint captures retain {retained_capture_bytes} bytes; capture budget is {max_capture_bytes} bytes",
                            self.projection.op_name
                        )));
                    }
                    let max_managed_state_bytes = self.max_managed_state_bytes;
                    let operator_remaining = Arc::clone(&operator_remaining);
                    let context = format!(
                        "interval join [{}] vnode {vnode} checkpoint serialization",
                        self.projection.op_name
                    );
                    Some(StateFrameCapture::deferred(
                        retained_bytes,
                        move |remaining| {
                            let logical_remaining = operator_remaining.load(Ordering::Relaxed);
                            let limit = remaining
                                .min(max_managed_state_bytes)
                                .min(logical_remaining);
                            let encoded =
                                Self::encode_state_capture(state_capture, &context, limit)?;
                            operator_remaining
                                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                                    remaining.checked_sub(encoded.payload_len())
                                })
                                .map_err(|_| {
                                    DbError::Checkpoint(format!(
                                        "{context}: vnode checkpoints exhausted their {max_managed_state_bytes}-byte limit"
                                    ))
                                })?;
                            Ok(encoded)
                        },
                    ))
                } else {
                    Some(StateFrameCapture::encoded_static(&ABSENT_VNODE_FRAME))
                }
            } else {
                None
            };
            captured.push(CapturedVnodeState { vnode, state });
        }

        for (vnode, (checkpointed, dirty)) in (0_u32..).zip(
            self.checkpointed_vnodes
                .iter_mut()
                .zip(&mut self.dirty_vnodes),
        ) {
            if required_vnodes.binary_search(&vnode).is_err() {
                *checkpointed = false;
                *dirty = false;
            }
        }
        for (&vnode, capture) in required_vnodes.iter().zip(capture_plan) {
            self.checkpointed_vnodes[vnode as usize] = true;
            if capture {
                self.dirty_vnodes[vnode as usize] = false;
            }
        }
        Ok(Some(captured))
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        if u32::from(self.key_group_count) != vnode_count || vnode >= vnode_count {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] vnode {vnode} restore does not match its {vnode_count}-vnode topology",
                self.projection.op_name
            )));
        }
        let current_bytes = self.vnode_states[vnode as usize]
            .as_ref()
            .map_or(0, |state| state.accounted_state_bytes());
        let other_bytes = self.accounted_state_bytes().saturating_sub(current_bytes);
        let remaining = self
            .max_managed_state_bytes
            .checked_sub(other_bytes)
            .ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "interval join [{}] restored state exceeds its {}-byte limit",
                    self.projection.op_name, self.max_managed_state_bytes
                ))
            })?;
        let replacement = Self::decode_vnode_frame(
            state,
            &self.config,
            &format!(
                "interval join [{}] vnode {vnode} restore",
                self.projection.op_name
            ),
            remaining,
            None,
        )?;
        self.vnode_states[vnode as usize] = if let Some(mut replacement) = replacement {
            replacement.validate_vnode(vnode, vnode_count, &self.config)?;
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                replacement.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            Some(Box::new(replacement))
        } else {
            None
        };
        self.checkpointed_vnodes[vnode as usize] = false;
        self.dirty_vnodes[vnode as usize] = false;
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.prepared_vnode_transition.is_some() || self.vnode_transition_cleanup.is_some() {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] already owns vnode transition state",
                self.projection.op_name
            )));
        }
        let config = self.cluster_shuffle.as_ref().ok_or_else(|| {
            DbError::Checkpoint(format!(
                "interval join [{}] cannot transition without cluster ownership",
                self.projection.op_name
            ))
        })?;
        let assignment = config.registry.versioned_snapshot();
        let owners: Vec<u64> = assignment.owners().iter().map(|owner| owner.0).collect();
        let installed_owners: Vec<u64> = self
            .local_assignment
            .owners()
            .iter()
            .map(|owner| owner.0)
            .collect();
        let checkpoint_bootstrap = match transition.mode {
            ManagedVnodeTransitionMode::Live => false,
            ManagedVnodeTransitionMode::CheckpointBootstrap { predecessor_owners } => {
                let predecessor_owner_ids = predecessor_owners
                    .iter()
                    .map(|owner| owner.0)
                    .collect::<Vec<_>>();
                if !transition
                    .predecessor
                    .matches_owner_map(&predecessor_owner_ids)
                {
                    return Err(DbError::Checkpoint(format!(
                        "interval join [{}] checkpoint bootstrap has an invalid predecessor owner map",
                        self.projection.op_name
                    )));
                }
                true
            }
        };
        let target_contains_self = assignment.owners().contains(&config.self_id);
        let endpoints_match_process = config.sender.local_id() == config.self_id.0
            && config.receiver.local_id() == config.self_id.0
            && config.sender.incarnation() == config.receiver.incarnation();
        let active_transport = endpoints_match_process
            && config.sender.assignment_version() == assignment.version()
            && config.receiver.assignment_version() == assignment.version()
            && config.sender.active_assignment_digest() == Some(transition.target.digest())
            && config.receiver.active_assignment_digest()
                == config.sender.active_assignment_digest()
            && transition.target.participant_incarnation(config.self_id.0)
                == Some(config.sender.incarnation());
        let inactive_transport = endpoints_match_process
            && config.sender.assignment_version() == 0
            && config.receiver.assignment_version() == 0
            && config.sender.active_assignment_digest().is_none()
            && config.receiver.active_assignment_digest().is_none()
            && !transition.target.contains(config.self_id.0);
        if transition.target.vnode_count != config.registry.vnode_count()
            || transition.target.assignment_version != assignment.version()
            || !transition.target.matches_owner_map(&owners)
            || target_contains_self != transition.target.contains(config.self_id.0)
            || (target_contains_self && !active_transport)
            || (!target_contains_self && !inactive_transport)
            || transition.predecessor.assignment_version.checked_add(1)
                != Some(transition.target.assignment_version)
            || if checkpoint_bootstrap {
                self.local_assignment.version() != assignment.version()
                    || self.local_assignment.owners() != assignment.owners()
                    || self.vnode_states.iter().any(Option::is_some)
                    || self.applied_left_watermark != i64::MIN
                    || self.applied_right_watermark != i64::MIN
                    || self.applied_left_idle
                    || self.applied_right_idle
                    || !self.aligned_replay.is_empty()
            } else {
                transition.predecessor.assignment_version != self.local_assignment.version()
                    || !transition.predecessor.matches_owner_map(&installed_owners)
            }
        {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] transition target does not match assignment {}",
                self.projection.op_name,
                assignment.version()
            )));
        }

        let fresh_acquirer = target_contains_self
            && (checkpoint_bootstrap
                || transition
                    .predecessor
                    .participant_incarnation(config.self_id.0)
                    != Some(config.sender.incarnation()));
        if fresh_acquirer {
            let target_owned = assignment
                .owners()
                .iter()
                .zip(0_u32..)
                .filter_map(|(owner, vnode)| (*owner == config.self_id).then_some(vnode))
                .collect::<Vec<_>>();
            let restored = transition
                .restores
                .iter()
                .map(|restore| restore.vnode)
                .collect::<Vec<_>>();
            if !transition.revoked.is_empty()
                || target_owned.is_empty()
                || restored != target_owned
                || restored.windows(2).any(|pair| pair[0] >= pair[1])
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] fresh-owner transition does not contain every acquired vnode frame",
                    self.projection.op_name
                )));
            }
        }
        let pristine_restore_target = !transition.restores.is_empty()
            && self.vnode_states.iter().all(Option::is_none)
            && self.applied_left_watermark == i64::MIN
            && self.applied_right_watermark == i64::MIN
            && !self.applied_left_idle
            && !self.applied_right_idle
            && self.aligned_replay.is_empty();
        let requires_handoff_cut = fresh_acquirer || pristine_restore_target;
        let mut handoff_cut = self.portable_handoff_cut(&transition, requires_handoff_cut)?;
        let derive_handoff_cut = requires_handoff_cut && handoff_cut.is_none();
        let restore_cut = if transition.restores.is_empty() || derive_handoff_cut {
            None
        } else {
            Some(handoff_cut.unwrap_or(IntervalHandoffCut {
                left_watermark: self.applied_left_watermark,
                right_watermark: self.applied_right_watermark,
            }))
        };

        if self.vnode_states.len() != transition.target.vnode_count as usize {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] state table has {} slots for target vnode count {}",
                self.projection.op_name,
                self.vnode_states.len(),
                transition.target.vnode_count
            )));
        }

        let mut replacements = BTreeMap::new();
        for vnode in transition.revoked {
            if *vnode >= transition.target.vnode_count {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] revoked vnode {vnode} is outside the target vnode space",
                    self.projection.op_name
                )));
            }
            replacements.insert(*vnode, None);
        }
        let mut restore_vnodes = rustc_hash::FxHashSet::default();
        let mut restore_payload_bytes = 0usize;
        for restore in transition.restores {
            if !restore_vnodes.insert(restore.vnode) {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition repeats restored vnode {}",
                    self.projection.op_name, restore.vnode
                )));
            }
            if assignment
                .owners()
                .get(usize::try_from(restore.vnode).unwrap_or(usize::MAX))
                != Some(&config.self_id)
            {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] restore vnode {} is not owned by this node",
                    self.projection.op_name, restore.vnode
                )));
            }
            restore_payload_bytes = restore_payload_bytes
                .checked_add(restore.state.len())
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition restore payload accounting overflow",
                        self.projection.op_name
                    ))
                })?;
            if restore_payload_bytes > self.max_managed_state_bytes {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition restore payload exceeds its {}-byte limit",
                    self.projection.op_name, self.max_managed_state_bytes
                )));
            }
        }

        let live_bytes = self.accounted_state_bytes();
        let mut restored_bytes = 0usize;
        let mut recovered_cut: Option<IntervalHandoffCut> = None;
        for restore in transition.restores {
            let remaining = self
                .max_managed_state_bytes
                .checked_sub(
                    live_bytes
                        .saturating_add(restore_payload_bytes)
                        .saturating_add(restored_bytes),
                )
                .ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "interval join [{}] transition state exceeds its {}-byte limit",
                        self.projection.op_name, self.max_managed_state_bytes
                    ))
                })?;
            let context = format!(
                "interval join [{}] vnode {} restore",
                self.projection.op_name, restore.vnode
            );
            let Some(mut state) = Self::decode_vnode_frame(
                restore.state,
                &self.config,
                &context,
                remaining,
                restore_cut,
            )?
            else {
                replacements.insert(restore.vnode, None);
                continue;
            };
            if derive_handoff_cut {
                if let Some(candidate) = self.restored_handoff_cut_evidence(&state, &context)? {
                    if recovered_cut.is_some_and(|expected| {
                        expected.left_watermark != candidate.left_watermark
                            || expected.right_watermark != candidate.right_watermark
                    }) {
                        return Err(DbError::Checkpoint(format!(
                            "interval join [{}] restored vnode frames disagree on the handoff watermarks",
                            self.projection.op_name
                        )));
                    }
                    recovered_cut = Some(candidate);
                }
            }
            if let Some((left_schema, right_schema)) = &self.input_schemas {
                state.seed_input_schemas(
                    left_schema.clone(),
                    right_schema.clone(),
                    &self.config,
                )?;
            }
            state.validate_vnode(restore.vnode, transition.target.vnode_count, &self.config)?;
            restored_bytes = restored_bytes.saturating_add(state.accounted_state_bytes());
            replacements.insert(restore.vnode, Some(Box::new(state)));
        }
        if derive_handoff_cut {
            let cut = recovered_cut.unwrap_or(IntervalHandoffCut {
                left_watermark: i64::MIN,
                right_watermark: i64::MIN,
            });
            for (vnode, state) in replacements
                .iter()
                .filter_map(|(vnode, state)| state.as_deref().map(|state| (*vnode, state)))
            {
                let (left_rows, right_rows) = state.buffered_rows();
                let (left_evicted_cutoff, right_evicted_cutoff) = state.evicted_cutoffs();
                Self::validate_handoff_cutoffs(
                    left_evicted_cutoff,
                    right_evicted_cutoff,
                    left_rows != 0,
                    right_rows != 0,
                    &self.config,
                    &format!(
                        "interval join [{}] vnode {vnode} restore",
                        self.projection.op_name
                    ),
                    cut,
                )?;
            }
            handoff_cut = Some(cut);
        }
        for ((state, owner), vnode) in self
            .vnode_states
            .iter()
            .zip(assignment.owners())
            .zip(0_u32..)
        {
            if state.is_none() || replacements.contains_key(&vnode) {
                continue;
            }
            if owner != &config.self_id {
                return Err(DbError::Checkpoint(format!(
                    "interval join [{}] transition retained unowned vnode {vnode}",
                    self.projection.op_name
                )));
            }
        }
        let prepared = PreparedIntervalJoinTransition {
            replacements: replacements.into_iter().collect(),
            local_assignment: assignment,
            handoff_cut,
        };
        let total_bytes = live_bytes
            .saturating_add(restore_payload_bytes)
            .saturating_add(Self::transition_accounted_bytes(&prepared));
        if total_bytes > self.max_managed_state_bytes {
            return Err(DbError::Checkpoint(format!(
                "interval join [{}] prepared transition accounts {total_bytes} bytes; limit is {} bytes",
                self.projection.op_name, self.max_managed_state_bytes
            )));
        }
        self.prepared_vnode_transition = Some(prepared);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        let Some(prepared) = self.prepared_vnode_transition.take() else {
            return;
        };
        assert!(self.vnode_transition_cleanup.is_none());
        self.vnode_transition_cleanup = Some(IntervalJoinTransitionCleanup::Aborted(prepared));
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        let prepared = self
            .prepared_vnode_transition
            .take()
            .expect("interval join transition must be prepared before publication");
        assert!(self.vnode_transition_cleanup.is_none());
        let mut prepared = prepared;
        for (vnode, replacement) in &mut prepared.replacements {
            let slot = self
                .vnode_states
                .get_mut(*vnode as usize)
                .expect("prepared interval join vnode must have a state slot");
            std::mem::swap(slot, replacement);
            self.checkpointed_vnodes[*vnode as usize] = false;
            self.dirty_vnodes[*vnode as usize] = false;
        }
        std::mem::swap(&mut self.local_assignment, &mut prepared.local_assignment);
        if let Some(mut cut) = prepared.handoff_cut {
            std::mem::swap(&mut self.applied_left_watermark, &mut cut.left_watermark);
            std::mem::swap(&mut self.applied_right_watermark, &mut cut.right_watermark);
            self.applied_left_idle = false;
            self.applied_right_idle = false;
            prepared.handoff_cut = Some(cut);
        }
        self.vnode_transition_cleanup = Some(IntervalJoinTransitionCleanup::Published(prepared));
    }

    #[cfg(feature = "cluster")]
    fn finish_vnode_transition(&mut self) {
        self.vnode_transition_cleanup = None;
    }

    fn force_full_vnode_capture(&mut self) {
        self.checkpointed_vnodes.fill(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::time::Duration;

    #[cfg(feature = "cluster")]
    use arrow::array::Int64Array;

    fn materialize_capture(capture: StateFrameCapture) -> Result<bytes::Bytes, DbError> {
        let mut staged_bytes = capture.retained_bytes();
        capture.materialize(&mut staged_bytes, u64::MAX)
    }

    #[cfg(feature = "cluster")]
    async fn single_owner_shuffle(
        vnode_count: u32,
    ) -> (
        ClusterShuffleConfig,
        laminar_core::checkpoint::CheckpointAssignmentFence,
    ) {
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::state::{NodeId, VnodeRegistry};

        let self_id = NodeId(1);
        let incarnation = uuid::Uuid::from_u128(1);
        let registry = Arc::new(VnodeRegistry::single_owner(vnode_count, self_id));
        registry.set_assignment(vec![self_id; vnode_count as usize].into());
        let receiver = Arc::new(
            laminar_core::shuffle::ShuffleReceiver::bind(
                self_id.0,
                "127.0.0.1:0".parse().unwrap(),
                incarnation,
            )
            .await
            .unwrap(),
        );
        let sender = Arc::new(laminar_core::shuffle::ShuffleSender::new(
            self_id.0,
            incarnation,
        ));
        let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        receiver
            .install_process_lease_deadline(Arc::clone(&deadline))
            .unwrap();
        sender.install_process_lease_deadline(deadline).unwrap();

        let owners = vec![self_id.0; usize::try_from(vnode_count).unwrap()];
        let fence = laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &owners,
            vec![laminar_core::checkpoint::CheckpointParticipant {
                node_id: self_id.0,
                boot_incarnation: incarnation,
            }],
        )
        .unwrap();
        sender.install_assignment_fence(&fence, &owners).unwrap();
        receiver.install_assignment_fence(&fence, &owners).unwrap();

        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver,
                self_id,
            },
            fence,
        )
    }

    #[cfg(feature = "cluster")]
    fn install_single_owner_predecessor(
        operator: &mut IntervalJoinOperator,
        target: &laminar_core::checkpoint::CheckpointAssignmentFence,
    ) -> laminar_core::checkpoint::CheckpointAssignmentFence {
        let owners = vec![1; target.vnode_count as usize];
        let registry =
            VnodeRegistry::single_owner(target.vnode_count, laminar_core::state::NodeId(1));
        let predecessor_version = target.assignment_version - 1;
        if predecessor_version > registry.assignment_version() {
            registry.set_assignment_and_version(
                vec![laminar_core::state::NodeId(1); target.vnode_count as usize].into(),
                predecessor_version,
            );
        }
        operator.local_assignment = registry.versioned_snapshot();
        laminar_core::checkpoint::CheckpointAssignmentFence::from_owner_map(
            target.assignment_version - 1,
            &owners,
            target.participants.clone(),
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    async fn two_owner_shuffle() -> (
        ClusterShuffleConfig,
        Arc<laminar_core::shuffle::ShuffleReceiver>,
    ) {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::cluster::control::LeaseDeadline;
        use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let registry = Arc::new(VnodeRegistry::new(2));
        registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
        let fence = CheckpointAssignmentFence::from_owner_map(
            registry.assignment_version(),
            &[1, 2],
            vec![
                CheckpointParticipant {
                    node_id: 1,
                    boot_incarnation: uuid::Uuid::from_u128(1),
                },
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(2),
                },
            ],
        )
        .unwrap();
        let local_receiver = Arc::new(
            ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
                .await
                .unwrap(),
        );
        let remote_receiver = Arc::new(
            ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
                .await
                .unwrap(),
        );
        let sender = Arc::new(ShuffleSender::new(1, uuid::Uuid::from_u128(1)));
        sender.register_peer(2, remote_receiver.local_addr());
        let local_deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        local_receiver
            .install_process_lease_deadline(Arc::clone(&local_deadline))
            .unwrap();
        sender
            .install_process_lease_deadline(local_deadline)
            .unwrap();
        remote_receiver
            .install_process_lease_deadline(Arc::new(LeaseDeadline::live_for(Duration::from_secs(
                60,
            ))))
            .unwrap();
        local_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        remote_receiver
            .install_assignment_fence(&fence, &[1, 2])
            .unwrap();
        sender.install_assignment_fence(&fence, &[1, 2]).unwrap();

        (
            ClusterShuffleConfig {
                registry,
                sender,
                receiver: local_receiver,
                self_id: NodeId(1),
            },
            remote_receiver,
        )
    }

    fn test_config() -> StreamJoinConfig {
        StreamJoinConfig {
            join_type: laminar_sql::parser::join_parser::JoinType::Inner,
            left_keys: vec!["id".to_string()],
            right_keys: vec!["id".to_string()],
            left_time_column: "ts".to_string(),
            right_time_column: "ts".to_string(),
            left_table: "left_stream".to_string(),
            right_table: "right_stream".to_string(),
            time_bound: Duration::from_millis(100),
        }
    }

    fn unconstrained_frontier() -> InputFrontier {
        InputFrontier {
            watermark: Some(i64::MAX),
            idle: true,
        }
    }

    #[test]
    fn output_frontier_uses_the_preserved_output_side() {
        use laminar_sql::parser::join_parser::JoinType;

        for join_type in [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
        ] {
            let mut config = test_config();
            config.join_type = join_type;
            let mut operator =
                IntervalJoinOperator::new("frontier", config, None, SessionContext::new());
            operator.applied_left_watermark = 2_000;
            operator.applied_right_watermark = 1_500;
            operator.applied_left_idle = true;
            operator.applied_right_idle = true;
            let expected = if matches!(join_type, JoinType::RightSemi | JoinType::RightAnti) {
                1_500
            } else {
                1_400
            };
            let output = operator.output_frontier(unconstrained_frontier());
            assert_eq!(output.watermark, Some(expected), "{join_type:?}");
            assert!(output.idle, "{join_type:?}");
        }
    }

    #[tokio::test]
    async fn all_input_idle_is_checkpointed_and_restored() {
        let mut operator =
            IntervalJoinOperator::new("idle-frontier", test_config(), None, SessionContext::new());
        let frontiers = [
            InputFrontier {
                watermark: Some(200),
                idle: true,
            },
            InputFrontier {
                watermark: Some(400),
                idle: true,
            },
        ];
        operator
            .process_with_frontiers(&[Vec::new(), Vec::new()], &frontiers)
            .await
            .unwrap();

        let checkpoint = operator.checkpoint().unwrap().unwrap();
        let mut restored =
            IntervalJoinOperator::new("idle-frontier", test_config(), None, SessionContext::new());
        restored.restore(checkpoint).unwrap();

        assert!(restored.applied_left_idle);
        assert!(restored.applied_right_idle);
        let output = restored.output_frontier(InputFrontier {
            watermark: Some(i64::MAX),
            idle: false,
        });
        assert_eq!(
            output,
            InputFrontier {
                watermark: Some(200),
                idle: true,
            }
        );
        #[cfg(feature = "cluster")]
        assert_eq!(restored.restored_output_frontier(), Some(output));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn whole_checkpoint_capture_defers_and_preserves_its_replay_cut() {
        let (scope, fence) = single_owner_shuffle(1).await;
        let mut operator = IntervalJoinOperator::new(
            "replay-checkpoint",
            test_config(),
            None,
            SessionContext::new(),
        );
        operator.attach_cluster_shuffle(scope.clone());
        operator.applied_left_watermark = 7_000;
        operator.applied_right_watermark = 6_000;
        operator.applied_left_idle = true;
        operator.aligned_replay.push_back((
            fence.assignment_version,
            JoinInputSide::Right,
            5_000,
            crate::operator::RetainedBatch::restored_channel(
                right_batch(&["A"], &[5_000], &[1.0]),
                1,
                fence.assignment_version,
                1,
                Arc::from([0_u32]),
            ),
        ));

        let retained_bytes = operator
            .capture_operator_checkpoint(u64::MAX)
            .unwrap()
            .unwrap()
            .retained_bytes();
        assert!(retained_bytes > 0);
        assert!(operator.checkpoint_capture(retained_bytes - 1).is_err());
        let capture = operator
            .checkpoint_capture(retained_bytes)
            .unwrap()
            .unwrap();
        assert!(matches!(&capture, StateFrameCapture::Deferred { .. }));

        operator.applied_left_watermark = 9_000;
        operator.applied_right_watermark = 9_000;
        operator.applied_left_idle = false;
        operator.applied_right_idle = true;
        operator.aligned_replay.clear();

        let checkpoint = OperatorCheckpoint {
            data: materialize_capture(capture).unwrap().to_vec(),
        };
        let mut restored = IntervalJoinOperator::new(
            "replay-checkpoint",
            test_config(),
            None,
            SessionContext::new(),
        );
        restored.attach_cluster_shuffle(scope);
        restored.restore(checkpoint).unwrap();

        assert_eq!(restored.applied_left_watermark, 7_000);
        assert_eq!(restored.applied_right_watermark, 6_000);
        assert!(restored.applied_left_idle);
        assert!(!restored.applied_right_idle);
        let (assignment, side, watermark, batch) = restored.aligned_replay.front().unwrap();
        assert_eq!(*assignment, fence.assignment_version);
        assert!(matches!(side, JoinInputSide::Right));
        assert_eq!(*watermark, 5_000);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(restored.aligned_replay.len(), 1);
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn pending_right_replay_holds_left_oriented_output_by_the_join_bound() {
        use laminar_sql::parser::join_parser::JoinType;

        for (join_type, expected) in [(JoinType::Inner, 4_900), (JoinType::RightSemi, 5_000)] {
            let mut config = test_config();
            config.join_type = join_type;
            let mut operator =
                IntervalJoinOperator::new("replay-frontier", config, None, SessionContext::new());
            operator.applied_left_watermark = 10_000;
            operator.applied_right_watermark = 10_000;
            operator.aligned_replay.push_back((
                1,
                JoinInputSide::Right,
                5_000,
                crate::operator::RetainedBatch::local(right_batch(&["A"], &[5_000], &[1.0])),
            ));
            let output = operator.output_frontier(unconstrained_frontier());
            assert_eq!(output.watermark, Some(expected), "{join_type:?}");
            assert!(!output.idle, "{join_type:?}");
        }
    }

    fn left_batch(ids: &[&str], timestamps: &[i64], values: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn right_batch(ids: &[&str], timestamps: &[i64], amounts: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("amount", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(Float64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn key_for_vnode(target: u32, vnode_count: u32) -> String {
        for candidate in 0..1_000 {
            let key = format!("vnode-{candidate}");
            let batch = left_batch(&[key.as_str()], &[100], &[1.0]);
            let vnodes = laminar_core::shuffle::row_vnodes(&batch, &[0], vnode_count).unwrap();
            if vnodes == [target] {
                return key;
            }
        }
        panic!("could not find a key for vnode {target}");
    }

    #[cfg(feature = "cluster")]
    fn composite_left_batch(regions: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("region", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["hot"; regions.len()])),
                Arc::new(Int64Array::from(regions.to_vec())),
                Arc::new(TimestampMillisecondArray::from(vec![100; regions.len()])),
                Arc::new(Float64Array::from(vec![1.0; regions.len()])),
            ],
        )
        .unwrap()
    }

    #[cfg(feature = "cluster")]
    fn incompatible_left_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("price", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["bad"])),
                Arc::new(TimestampMillisecondArray::from(vec![110])),
                Arc::new(Int64Array::from(vec![1])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_basic_interval_join() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let left = left_batch(&["A", "B"], &[100, 200], &[10.0, 20.0]);
        let right = right_batch(&["A", "B"], &[110, 250], &[1.0, 2.0]);

        let result = op
            .process(&[vec![left], vec![right]], &[0, 0])
            .await
            .unwrap();

        // A: |100 - 110| = 10 <= 100 -> match
        // B: |200 - 250| = 50 <= 100 -> match
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn local_join_routes_into_configured_vnodes() {
        let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
        let mut op = IntervalJoinOperator::new_with_key_groups(
            "local_vnodes",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        let key_zero = key_for_vnode(0, u32::from(key_group_count));
        let key_one = key_for_vnode(1, u32::from(key_group_count));
        let keys = [key_zero.as_str(), key_one.as_str()];

        let output = op
            .process(
                &[
                    vec![left_batch(&keys, &[100, 200], &[10.0, 20.0])],
                    vec![right_batch(&keys, &[110, 210], &[1.0, 2.0])],
                ],
                &[0, 0],
            )
            .await
            .unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert!(op.vnode_states[0].is_some());
        assert!(op.vnode_states[1].is_some());
        assert!(op.vnode_states[2..].iter().all(Option::is_none));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_admits_current_batch_before_advancing_its_watermark() {
        let (shuffle, _) = single_owner_shuffle(8).await;
        let mut op = IntervalJoinOperator::new(
            "current_batch_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let output = op
            .process(
                &[
                    vec![left_batch(&["A"], &[100], &[1.0])],
                    vec![right_batch(&["A"], &[110], &[2.0])],
                ],
                &[300, 300],
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);

        let error = op
            .process(
                &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
                &[300, 300],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn aligned_replay_reactivates_input_and_sweeps_resident_vnodes() {
        use laminar_sql::parser::join_parser::JoinType;

        let (shuffle, _) = single_owner_shuffle(2).await;
        let assignment_version = shuffle.registry.assignment_version();
        let resident_key = key_for_vnode(0, 2);
        let replay_key = key_for_vnode(1, 2);
        let left = left_batch(&[resident_key.as_str()], &[100], &[1.0]);
        let right = right_batch(&[replay_key.as_str()], &[250], &[2.0]);
        let mut config = test_config();
        config.join_type = JoinType::LeftAnti;
        let mut op = IntervalJoinOperator::new(
            "replay_sweep",
            config,
            None,
            laminar_sql::create_session_context(),
        );
        op.set_input_schemas(left.schema(), right.schema());
        op.attach_cluster_shuffle(shuffle);

        let initial = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
        assert!(initial.is_empty());
        assert!(op.vnode_states[0].is_some());
        op.applied_left_idle = true;
        op.applied_right_idle = true;
        op.aligned_replay.push_back((
            assignment_version,
            JoinInputSide::Right,
            300,
            crate::operator::RetainedBatch::local(right),
        ));

        let output = op.execute_aligned_replay().await.unwrap();

        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(op.applied_right_watermark, 300);
        assert!(op.applied_left_idle);
        assert!(!op.applied_right_idle);
        assert!(op.aligned_replay.is_empty());
        assert!(!op.output_frontier(unconstrained_frontier()).idle);
        assert_eq!(op.vnode_states[0].as_ref().unwrap().buffered_rows(), (0, 0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn outbound_admission_turns_later_local_halt_into_recovery() {
        let (shuffle, remote_receiver) = two_owner_shuffle().await;
        let local_key = key_for_vnode(0, 2);
        let remote_key = key_for_vnode(1, 2);
        let mut op = IntervalJoinOperator::new(
            "post_shuffle_failure",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);
        op.max_managed_state_bytes = 1;

        let error = op
            .process(
                &[
                    vec![left_batch(
                        &[local_key.as_str(), remote_key.as_str()],
                        &[100, 100],
                        &[1.0, 2.0],
                    )],
                    vec![],
                ],
                &[0, 0],
            )
            .await
            .unwrap_err();

        assert!(matches!(error, DbError::ShufflePartialSend(_)));
        assert!(error.requires_pipeline_recovery());
        let received = tokio::time::timeout(Duration::from_secs(2), remote_receiver.recv())
            .await
            .expect("remote frame was not delivered")
            .expect("shuffle receiver closed");
        assert!(matches!(
            received.message(),
            laminar_core::shuffle::ShuffleMessage::Data { .. }
        ));
    }

    #[tokio::test]
    async fn test_cross_cycle_matching() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        // Cycle 1: only left data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let result = op.process(&[vec![left], vec![]], &[0, 0]).await.unwrap();
        assert!(result.is_empty());

        // Cycle 2: right data arrives, should match the buffered left
        let right = right_batch(&["A"], &[150], &[1.0]);
        let result = op.process(&[vec![], vec![right]], &[0, 0]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
        assert!(op.vnode_states[0].is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn composite_keys_route_by_the_full_ordered_tuple() {
        let vnode_count = 64;
        let batch = composite_left_batch(&[1, 2]);
        let mut expected = laminar_core::shuffle::row_vnodes(&batch, &[0, 1], vnode_count).unwrap();
        expected.sort_unstable();
        expected.dedup();
        assert_eq!(expected.len(), 2, "test tuple hashes unexpectedly collided");

        let (shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut config = test_config();
        config.left_keys.push("region".into());
        config.right_keys.push("region".into());
        let mut op = IntervalJoinOperator::new(
            "composite_interval",
            config,
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);
        op.process(&[vec![batch], vec![]], &[0, 0]).await.unwrap();

        let actual = op
            .vnode_states
            .iter()
            .zip(0_u32..)
            .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn later_vnode_failure_requires_recovery_after_prior_admission() {
        let (shuffle, _) = single_owner_shuffle(2).await;
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let mut retained = IntervalJoinState::new();
        let mut output_budget = IntervalJoinOutputBudget::default();
        execute_interval_join_cycle(
            &mut retained,
            &[left_batch(&["seed"], &[100], &[1.0])],
            &[],
            &op.config,
            0,
            0,
            0,
            0,
            usize::MAX,
            &mut output_budget,
        )
        .unwrap();
        op.vnode_states[1] = Some(Box::new(retained));

        let mut routed = BTreeMap::new();
        routed.insert(0, [vec![left_batch(&["ok"], &[100], &[1.0])], vec![]]);
        routed.insert(1, [vec![incompatible_left_batch()], vec![]]);
        let error = op.execute_routed_shards(routed, 0, 0).unwrap_err();

        assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert_eq!(op.vnode_states[0].as_ref().unwrap().buffered_rows(), (1, 0));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_watermark_sweep_failure_requires_recovery() {
        let (shuffle, _) = single_owner_shuffle(2).await;
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        op.attach_cluster_shuffle(shuffle);

        let mut retained = IntervalJoinState::new();
        let mut output_budget = IntervalJoinOutputBudget::default();
        for timestamp in (1_000..1_400).step_by(10) {
            execute_interval_join_cycle(
                &mut retained,
                &[left_batch(&["seed"], &[timestamp], &[1.0])],
                &[],
                &op.config,
                0,
                0,
                0,
                0,
                usize::MAX,
                &mut output_budget,
            )
            .unwrap();
        }
        op.vnode_states[0] = Some(Box::new(retained));

        // Force compaction to fail after the sweep has already removed old index entries.
        op.config.left_keys = vec!["missing".to_string()];
        let error = op.process(&[], &[0, 1_300]).await.unwrap_err();

        assert!(matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert!(error.requires_pipeline_recovery());
        assert_eq!(
            op.vnode_states[0].as_ref().unwrap().buffered_rows(),
            (20, 0)
        );
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx.clone());

        // Buffer some data
        let left = left_batch(&["A"], &[100], &[10.0]);
        let right = right_batch(&["A"], &[110], &[1.0]);
        let _ = op
            .process(&[vec![left], vec![right]], &[50, 50])
            .await
            .unwrap();
        assert_eq!(
            op.output_frontier(unconstrained_frontier()).watermark,
            Some(-50)
        );

        let metadata = op
            .checkpoint()
            .unwrap()
            .expect("watermarks are checkpointed");
        let captured = op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .expect("interval join has vnode state");
        let state = captured
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("the first vnode capture is complete");
        assert!(op.checkpoint_vnodes(&[0], 1, u64::MAX).unwrap().unwrap()[0]
            .state
            .is_none());
        let state = materialize_capture(state).unwrap();

        let mut op2 = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        op2.restore(metadata).unwrap();
        op2.restore_vnode(0, 1, &state).unwrap();
        assert_eq!(
            op2.output_frontier(unconstrained_frontier()).watermark,
            Some(-50)
        );

        // New right data should match the restored left
        let right2 = right_batch(&["A"], &[120], &[2.0]);
        let result = op2
            .process(&[vec![], vec![right2]], &[50, 50])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn checkpoint_respects_ipc_and_archive_peak_budget() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new("test_interval", test_config(), None, ctx);
        let wide_key = "x".repeat(64 * 1024);
        op.process(
            &[
                vec![left_batch(&[wide_key.as_str()], &[100], &[1.0])],
                vec![],
            ],
            &[0, 0],
        )
        .await
        .unwrap();

        let ipc_bytes = op.vnode_states[0]
            .as_mut()
            .unwrap()
            .snapshot_checkpoint(&op.config, crate::config::DEFAULT_MAX_MANAGED_STATE_BYTES)
            .unwrap()
            .retained_ipc_bytes()
            .unwrap();
        let archive_bytes = IntervalJoinOperator::serialize_state(
            op.vnode_states[0].as_mut().unwrap(),
            &op.config,
            "interval join peak-budget sizing",
            usize::MAX,
        )
        .unwrap()
        .len();
        let limit = ipc_bytes.checked_add(archive_bytes).unwrap();
        assert!(op.accounted_state_bytes() <= limit);
        op.set_managed_state_budget(limit);

        let state = op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("the first vnode capture is complete");
        assert!(matches!(&state, StateFrameCapture::Deferred { .. }));
        let state = materialize_capture(state).unwrap();
        assert_eq!(state.len(), archive_bytes);
    }

    #[test]
    fn deferred_vnode_frames_share_the_operator_checkpoint_budget() {
        let make_operator = || {
            let mut operator = IntervalJoinOperator::new_with_key_groups(
                "test_interval",
                test_config(),
                None,
                laminar_sql::create_session_context(),
                KeyGroupCount::try_from(2_u16).unwrap(),
            );
            for state in &mut operator.vnode_states {
                *state = Some(Box::new(IntervalJoinState::new()));
            }
            operator
        };

        let mut sizing = make_operator();
        let capture = sizing
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .unwrap();
        let frame_bytes = materialize_capture(capture).unwrap().len();
        let limit = frame_bytes.checked_mul(2).unwrap() - 1;

        let mut peak_operator = make_operator();
        peak_operator.vnode_states[1] = None;
        peak_operator.set_managed_state_budget(frame_bytes);
        let peak_capture = peak_operator
            .checkpoint_vnodes(&[0], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .unwrap();
        assert!(materialize_capture(peak_capture).is_err());

        let mut operator = make_operator();
        operator.set_managed_state_budget(limit);
        let mut frames = operator
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|captured| captured.state.unwrap());
        materialize_capture(frames.next().unwrap()).unwrap();
        assert!(materialize_capture(frames.next().unwrap()).is_err());
    }

    #[tokio::test]
    async fn vnode_restore_preserves_sparse_state() {
        let key_group_count = KeyGroupCount::try_from(2_u16).unwrap();
        let key = key_for_vnode(1, 2);
        let mut donor = IntervalJoinOperator::new_with_key_groups(
            "sparse_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        donor
            .process(
                &[vec![left_batch(&[key.as_str()], &[100], &[1.0])], vec![]],
                &[0, 0],
            )
            .await
            .unwrap();
        let frames = donor
            .checkpoint_vnodes(&[0, 1], 2, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|frame| {
                (
                    frame.vnode,
                    materialize_capture(frame.state.unwrap()).unwrap(),
                )
            })
            .collect::<Vec<_>>();

        let mut restored = IntervalJoinOperator::new_with_key_groups(
            "sparse_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
            key_group_count,
        );
        for (vnode, state) in frames {
            restored.restore_vnode(vnode, 2, &state).unwrap();
        }

        assert!(restored.vnode_states[0].is_none() && restored.vnode_states[1].is_some());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn fresh_owner_stages_common_portable_cut_atomically() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::state::NodeId;

        use crate::operator_graph::{ManagedVnodeRestore, ManagedWholeRestore};

        let vnode_count = 2;
        let (scope, target_fence) = single_owner_shuffle(vnode_count).await;
        let mut target = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        target.attach_cluster_shuffle(scope.clone());
        let predecessor_version = target_fence.assignment_version - 1;
        let predecessor_owners = [2_u64, 3];
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &predecessor_owners,
            vec![
                CheckpointParticipant {
                    node_id: 2,
                    boot_incarnation: uuid::Uuid::from_u128(2),
                },
                CheckpointParticipant {
                    node_id: 3,
                    boot_incarnation: uuid::Uuid::from_u128(3),
                },
            ],
        )
        .unwrap();
        let predecessor_registry = VnodeRegistry::new_unassigned(vnode_count);
        predecessor_registry.set_assignment_and_version(
            Arc::from(predecessor_owners.map(NodeId)),
            predecessor_version,
        );
        target.local_assignment = predecessor_registry.versioned_snapshot();

        let mut empty = IntervalJoinState::new();
        let vnode0 = IntervalJoinOperator::serialize_state(
            &mut empty,
            &target.config,
            "test vnode 0",
            target.max_managed_state_bytes,
        )
        .unwrap();
        let vnode1 = IntervalJoinOperator::serialize_state(
            &mut IntervalJoinState::new(),
            &target.config,
            "test vnode 1",
            target.max_managed_state_bytes,
        )
        .unwrap();
        let restores = [
            ManagedVnodeRestore {
                participant_id: 2,
                vnode: 0,
                state: &vnode0,
            },
            ManagedVnodeRestore {
                participant_id: 3,
                vnode: 1,
                state: &vnode1,
            },
        ];
        let donor_config = target.config.clone();
        let encode_whole = |participant_id: u64, right_watermark: i64, left_idle: bool| {
            let checkpoint = IntervalJoinOperatorCheckpoint {
                version: OPERATOR_CHECKPOINT_VERSION,
                join_type: join_type_tag(donor_config.join_type),
                left_keys: donor_config.left_keys.clone(),
                right_keys: donor_config.right_keys.clone(),
                left_time_column: donor_config.left_time_column.clone(),
                right_time_column: donor_config.right_time_column.clone(),
                left_table: donor_config.left_table.clone(),
                right_table: donor_config.right_table.clone(),
                bound_ms: i64::try_from(donor_config.time_bound.as_millis()).unwrap(),
                aligned_replay: Vec::new(),
                applied_left_watermark: 300,
                applied_right_watermark: right_watermark,
                applied_left_idle: left_idle,
                applied_right_idle: false,
                assignment_version: Some(predecessor.assignment_version),
                owner_map_digest: Some(predecessor.assignment_digest),
                participant_id: Some(participant_id),
            };
            rkyv::to_bytes::<rkyv::rancor::Error>(&checkpoint)
                .unwrap()
                .to_vec()
        };
        let donor2 = encode_whole(2, 250, true);
        let disagreeing_donor3 = encode_whole(3, 251, false);
        let disagreeing = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &donor2,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &disagreeing_donor3,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &disagreeing,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert_eq!(target.applied_left_watermark, i64::MIN);
        assert!(target.vnode_states.iter().all(Option::is_none));

        let donor3 = encode_whole(3, 250, false);
        let whole_restores = [
            ManagedWholeRestore {
                participant_id: 2,
                state: &donor2,
            },
            ManagedWholeRestore {
                participant_id: 3,
                state: &donor3,
            },
        ];

        let mut stale = IntervalJoinState::new();
        let stale_key = key_for_vnode(0, vnode_count);
        execute_interval_join_cycle(
            &mut stale,
            &[left_batch(&[stale_key.as_str()], &[200], &[1.0])],
            &[],
            &target.config,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            i64::MIN,
            target.max_managed_state_bytes,
            &mut IntervalJoinOutputBudget::default(),
        )
        .unwrap();
        let stale_vnode0 = IntervalJoinOperator::serialize_state(
            &mut stale,
            &target.config,
            "stale vnode 0",
            target.max_managed_state_bytes,
        )
        .unwrap();
        let stale_restores = [
            ManagedVnodeRestore {
                participant_id: 2,
                vnode: 0,
                state: &stale_vnode0,
            },
            ManagedVnodeRestore {
                participant_id: 3,
                vnode: 1,
                state: &vnode1,
            },
        ];
        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &stale_restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap_err();
        assert!(target.vnode_states.iter().all(Option::is_none));

        target
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        assert_eq!(target.applied_left_watermark, i64::MIN);
        target.publish_vnode_transition();
        assert_eq!(target.applied_left_watermark, 300);
        assert_eq!(target.applied_right_watermark, 250);
        assert!(!target.applied_left_idle);
        assert!(!target.applied_right_idle);
        target.finish_vnode_transition();

        let predecessor_nodes = predecessor_owners.map(NodeId);
        let mut bootstrap = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        bootstrap.attach_cluster_shuffle(scope);
        bootstrap
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target_fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &whole_restores,
                mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
                    predecessor_owners: &predecessor_nodes,
                },
            })
            .unwrap();
        bootstrap.publish_vnode_transition();
        assert_eq!(bootstrap.applied_left_watermark, 300);
        assert_eq!(bootstrap.applied_right_watermark, 250);
        assert!(bootstrap.vnode_states.iter().all(Option::is_some));
        bootstrap.finish_vnode_transition();
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn zero_owner_topology_transition_is_not_an_acquisition() {
        use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
        use laminar_core::state::{NodeId, VnodeRegistry};

        let (scope, current) = single_owner_shuffle(1).await;
        let predecessor_version = current.assignment_version;
        let target_version = predecessor_version + 1;
        let predecessor = CheckpointAssignmentFence::from_owner_map(
            predecessor_version,
            &[2],
            vec![CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            }],
        )
        .unwrap();
        let target = CheckpointAssignmentFence::from_owner_map(
            target_version,
            &[3],
            vec![CheckpointParticipant {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(3),
            }],
        )
        .unwrap();
        scope
            .registry
            .set_assignment_and_version(Arc::from([NodeId(3)]), target_version);
        scope.sender.invalidate_assignment_fence();
        scope.receiver.invalidate_assignment_fence();

        let mut operator = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        operator.attach_cluster_shuffle(scope);
        let predecessor_registry = VnodeRegistry::new_unassigned(1);
        predecessor_registry
            .set_assignment_and_version(Arc::from([NodeId(2)]), predecessor_version);
        operator.local_assignment = predecessor_registry.versioned_snapshot();

        operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &target,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        operator.publish_vnode_transition();

        assert_eq!(operator.local_assignment.version(), target_version);
        assert_eq!(operator.local_assignment.owners(), &[NodeId(3)]);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_transition_preserves_checkpointed_admission_watermarks() {
        let vnode_count = 8;
        let (source_shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut source = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        source.attach_cluster_shuffle(source_shuffle);
        assert!(source
            .process(&[vec![], vec![]], &[300, 300])
            .await
            .unwrap()
            .is_empty());
        let checkpoint = source.checkpoint().unwrap().unwrap();

        let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
        let mut restored = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        restored.restore(checkpoint).unwrap();
        let predecessor = install_single_owner_predecessor(&mut restored, &fence);
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        restored.publish_vnode_transition();
        assert_eq!(
            restored.local_assignment.version(),
            fence.assignment_version
        );
        restored.finish_vnode_transition();

        let error = restored
            .process(
                &[vec![left_batch(&["late"], &[100], &[1.0])], vec![]],
                &[300, 300],
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("below closed cutoff 300"));
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_capture_restore_preserves_cross_cycle_match() {
        let vnode_count = 8;
        let key_batch = left_batch(&["hot"], &[100], &[10.0]);
        let vnode = laminar_core::shuffle::row_vnodes(&key_batch, &[0], vnode_count).unwrap()[0];

        let (donor_shuffle, _) = single_owner_shuffle(vnode_count).await;
        let mut donor = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        donor.attach_cluster_shuffle(donor_shuffle);
        assert!(donor
            .process(&[vec![key_batch], vec![]], &[0, 0])
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            donor
                .vnode_states
                .iter()
                .zip(0_u32..)
                .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
                .collect::<Vec<_>>(),
            vec![vnode]
        );

        let captured = donor
            .checkpoint_vnodes(&[vnode], vnode_count, u64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(captured[0].vnode, vnode);
        let capture = captured
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("the first vnode capture is complete");
        let state = materialize_capture(capture).unwrap();

        let (restored_shuffle, fence) = single_owner_shuffle(vnode_count).await;
        let mut restored = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            None,
            laminar_sql::create_session_context(),
        );
        restored.attach_cluster_shuffle(restored_shuffle);
        let restores = [crate::operator_graph::ManagedVnodeRestore {
            participant_id: 1,
            vnode,
            state: &state,
        }];
        let predecessor = install_single_owner_predecessor(&mut restored, &fence);
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        let prepared = restored.managed_state_accounting().unwrap();
        assert!(prepared.prepared > 0);
        assert_eq!(prepared.retired, 0);
        restored.abort_vnode_transition();
        let aborted = restored.managed_state_accounting().unwrap();
        assert!(aborted.prepared > 0);
        assert_eq!(aborted.retired, 0);
        assert_eq!(
            restored.local_assignment.version(),
            predecessor.assignment_version
        );
        restored.finish_vnode_transition();
        restored
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: &fence,
                revoked: &rustc_hash::FxHashSet::default(),
                restores: &restores,
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .unwrap();
        restored.publish_vnode_transition();
        assert_eq!(
            restored.local_assignment.version(),
            fence.assignment_version
        );
        assert_eq!(restored.applied_left_watermark, 0);
        assert_eq!(restored.applied_right_watermark, 0);
        restored.finish_vnode_transition();

        let output = restored
            .process(
                &[vec![], vec![right_batch(&["hot"], &[110], &[1.0])]],
                &[0, 0],
            )
            .await
            .unwrap();
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            restored
                .vnode_states
                .iter()
                .zip(0_u32..)
                .filter_map(|(state, vnode)| state.as_ref().map(|_| vnode))
                .collect::<Vec<_>>(),
            vec![vnode]
        );

        assert!(restored
            .process(&[vec![], vec![]], &[500, 500])
            .await
            .unwrap()
            .is_empty());
        let state = restored.vnode_states[vnode as usize].as_ref().unwrap();
        assert_eq!(state.buffered_rows(), (0, 0));
    }

    #[tokio::test]
    async fn post_projection_fault_requires_recovery_after_state_admission() {
        let ctx = laminar_sql::create_session_context();
        let mut op = IntervalJoinOperator::new(
            "test_interval",
            test_config(),
            Some(Arc::from("SELECT missing FROM __interval_tmp")),
            ctx,
        );

        let error = op
            .process(
                &[
                    vec![left_batch(&["A"], &[100], &[10.0])],
                    vec![right_batch(&["A"], &[110], &[1.0])],
                ],
                &[0, 0],
            )
            .await
            .unwrap_err();

        assert!(matches!(&error, DbError::StatefulOperatorPartialApply(_)));
        assert!(error.requires_pipeline_recovery());
        let capture = op
            .checkpoint_vnodes(&[0], 1, u64::MAX)
            .unwrap()
            .unwrap()
            .into_iter()
            .next()
            .and_then(|captured| captured.state)
            .expect("state admitted before projection failure remains checkpointable");
        let state = materialize_capture(capture).unwrap();
        assert_eq!(state.first(), Some(&PRESENT_VNODE));
        let decoded = rkyv::from_bytes::<JoinStateCheckpoint, rkyv::rancor::Error>(
            &state[VNODE_FRAME_HEADER_LEN..],
        )
        .unwrap();
        assert_eq!(decoded.left_buffer_rows, 1);
        assert_eq!(decoded.right_buffer_rows, 1);
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = IntervalJoinOperator::new("my_interval_join", test_config(), None, ctx);
        assert_eq!(&*op.projection.op_name, "my_interval_join");
    }
}
