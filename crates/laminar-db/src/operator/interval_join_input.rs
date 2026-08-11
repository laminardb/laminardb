//! Replay-safe, vnode-local input normalization for bounded interval joins.
//!
//! The startup planner admits this private contract only for certified direct source routes. It
//! converts ordered source batches into weighted differential rows, then publishes its logical
//! state only after the caller confirms that the downstream join kernel accepted the delta.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::row::{RowConverter, Rows, SortField};
use laminar_connectors::connector::{
    schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
    source_mutations_routed, source_row_positions, strip_source_mutations_routed,
    strip_source_row_positions, SourceMutation, SourceRowPositionRef,
};
use laminar_core::changelog::WEIGHT_COLUMN;
use rustc_hash::FxHashMap;
use sha2::{Digest, Sha256};

use crate::error::DbError;
use crate::interval_join::{
    logical_row_bytes, HEAP_ALLOCATION_CHARGE, MAX_CYCLE_OUTPUT_BYTES, MAX_CYCLE_OUTPUT_ROWS,
};
use crate::temporal_join_state::TimestampMillisView;

const BASE_STATE_CHARGE: usize = 512;
const HASH_CAPACITY_CHARGE: usize = 128;
const FRONTIER_ENTRY_CHARGE: usize = 192;
const SLOT_ENTRY_CHARGE: usize = 160;
const RETAINED_BATCH_CHARGE: usize = 256;
// Bounds two replay maps, one mutation map, encoded-row ownership, and the worst keyed output
// rosters per input row before any of those allocations are attempted.
const NORMALIZATION_ROW_SCRATCH_CHARGE: usize = 1_024;
// Bounds full-row and primary-key Rows plus their independently owned Arc identities/keys.
const NORMALIZATION_CELL_SCRATCH_CHARGE: usize = 128;

/// Source semantics normalized by one bounded-join input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BoundedJoinInputMode {
    /// Every accepted source row contributes `+1`.
    AppendOnly,
    /// The current image for each non-null primary key contributes `+1`.
    KeyedUpsert {
        /// Visible input columns comprising the complete primary key.
        primary_key_indices: Vec<usize>,
    },
    /// The sole trailing `__weight` column carries exact row multiplicity changes.
    FullChangelog,
}

/// Immutable construction contract for one vnode-local normalizer.
#[derive(Debug, Clone)]
pub(crate) struct BoundedJoinInputConfig {
    /// Owning vnode, used to make terminal errors attributable.
    pub(crate) vnode: u32,
    /// Event-time column in the visible row (excluding source metadata and `__weight`).
    pub(crate) event_time_index: usize,
    /// Source mutation semantics.
    pub(crate) mode: BoundedJoinInputMode,
    /// Hard conservative bound for retained state.
    pub(crate) max_retained_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplayOperation {
    Append,
    Put,
    Tombstone,
    Weight(i64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayIdentity {
    row: Arc<[u8]>,
    operation: ReplayOperation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayCursor {
    order: Arc<[u8]>,
    sub_offset: u32,
}

impl ReplayCursor {
    fn compare(&self, position: SourceRowPositionRef<'_>) -> Ordering {
        position
            .order_key
            .cmp(self.order.as_ref())
            .then_with(|| position.sub_offset.cmp(&self.sub_offset))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayFrontier {
    cursor: ReplayCursor,
    identity: ReplayIdentity,
}

type ReplayFrontierUpdate = (Arc<[u8]>, ReplayFrontier);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoredRow {
    batch_id: u64,
    row: u32,
    logical_bytes: usize,
}

#[derive(Clone)]
struct RetainedBatch {
    batch: Arc<RecordBatch>,
    references: usize,
}

type GcEntry = Reverse<(i64, Arc<[u8]>)>;

/// Exact cutoff min-heap. Identities and event times are immutable while a slot is retained, so
/// every slot has one entry and updates create no stale-index churn.
struct ExactGcIndex {
    heap: BinaryHeap<GcEntry>,
}

impl ExactGcIndex {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
        }
    }

    fn keys_before(&self, cutoff: i64, vnode: u32) -> Result<Vec<Arc<[u8]>>, DbError> {
        let count = self.count_before(0, cutoff, vnode)?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(count)
            .map_err(|_| terminal(vnode, "cutoff eviction roster cannot be reserved"))?;
        self.collect_before(0, cutoff, &mut keys);
        Ok(keys)
    }

    fn count_before(&self, index: usize, cutoff: i64, vnode: u32) -> Result<usize, DbError> {
        let Some(Reverse((event_time, _))) = self.heap.as_slice().get(index) else {
            return Ok(0);
        };
        if *event_time >= cutoff {
            return Ok(0);
        }
        let left = index
            .checked_mul(2)
            .and_then(|index| index.checked_add(1))
            .ok_or_else(|| terminal(vnode, "cutoff index overflow"))?;
        let right = left
            .checked_add(1)
            .ok_or_else(|| terminal(vnode, "cutoff index overflow"))?;
        let left_count = self.count_before(left, cutoff, vnode)?;
        let right_count = self.count_before(right, cutoff, vnode)?;
        1usize
            .checked_add(left_count)
            .and_then(|count| count.checked_add(right_count))
            .ok_or_else(|| terminal(vnode, "cutoff eviction count overflow"))
    }

    fn collect_before(&self, index: usize, cutoff: i64, keys: &mut Vec<Arc<[u8]>>) {
        let Some(Reverse((event_time, identity))) = self.heap.as_slice().get(index) else {
            return;
        };
        if *event_time >= cutoff {
            return;
        }
        keys.push(Arc::clone(identity));
        let left = index * 2 + 1;
        self.collect_before(left, cutoff, keys);
        self.collect_before(left + 1, cutoff, keys);
    }

    fn try_reserve(&mut self, additional: usize, vnode: u32) -> Result<(), DbError> {
        self.heap.try_reserve(additional).map_err(|_| {
            DbError::BackpressureFail(format!(
                "bounded join input vnode {vnode} cutoff heap capacity cannot be reserved"
            ))
        })
    }

    fn insert(&mut self, event_time: i64, identity: Arc<[u8]>) {
        self.heap.push(Reverse((event_time, identity)));
    }

    fn pop_before(&mut self, cutoff: i64) -> Option<(i64, Arc<[u8]>)> {
        let Reverse((event_time, _)) = self.heap.peek()?;
        if *event_time >= cutoff {
            return None;
        }
        self.heap.pop().map(|Reverse(entry)| entry)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyedSlot {
    partition: Arc<[u8]>,
    event_time: i64,
    row_identity: Option<Arc<[u8]>>,
    row: Option<StoredRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FullSlot {
    partition: Arc<[u8]>,
    event_time: i64,
    multiplicity: i64,
}

struct KeyedState {
    primary_key_codec: RowConverter,
    primary_key_indices: Vec<usize>,
    slots: FxHashMap<Arc<[u8]>, KeyedSlot>,
    gc: ExactGcIndex,
    retained_batches: FxHashMap<u64, RetainedBatch>,
    next_batch_id: u64,
}

struct FullState {
    slots: FxHashMap<Arc<[u8]>, FullSlot>,
    gc: ExactGcIndex,
}

enum ModeState {
    AppendOnly,
    Keyed(KeyedState),
    Full(FullState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlannedRow {
    Stored(StoredRow),
    Input(u32),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PlannedKeyedSlot {
    partition: Arc<[u8]>,
    event_time: i64,
    row_identity: Option<Arc<[u8]>>,
    row: Option<PlannedRow>,
}

impl From<&KeyedSlot> for PlannedKeyedSlot {
    fn from(slot: &KeyedSlot) -> Self {
        Self {
            partition: Arc::clone(&slot.partition),
            event_time: slot.event_time,
            row_identity: slot.row_identity.clone(),
            row: slot.row.map(PlannedRow::Stored),
        }
    }
}

#[derive(Clone)]
struct OutputRowRef {
    batch: Arc<RecordBatch>,
    row: u32,
    logical_bytes: usize,
}

struct KeyedCommit {
    evicted: Vec<Arc<[u8]>>,
    updates: Vec<(Arc<[u8]>, KeyedSlot)>,
    indexed_new: Vec<(i64, Arc<[u8]>)>,
    released_batches: Vec<u64>,
    new_batch: Option<(u64, Arc<RecordBatch>)>,
}

struct FullCommit {
    evicted: Vec<Arc<[u8]>>,
    updates: Vec<(Arc<[u8]>, FullSlot)>,
    indexed_new: Vec<(i64, Arc<[u8]>)>,
}

enum ModeCommit {
    AppendOnly,
    Keyed(KeyedCommit),
    Full(FullCommit),
}

struct InputCommit {
    cutoff: i64,
    frontier_updates: Vec<ReplayFrontierUpdate>,
    mode: ModeCommit,
    logical_bytes: usize,
}

const NORMALIZER_CHECKPOINT_VERSION: u8 = 1;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum CheckpointReplayOperation {
    Append,
    Put,
    Tombstone,
    Weight(i64),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointReplayFrontier {
    partition: Vec<u8>,
    order: Vec<u8>,
    sub_offset: u32,
    row: Vec<u8>,
    operation: CheckpointReplayOperation,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointKeyedSlot {
    key: Vec<u8>,
    partition: Vec<u8>,
    event_time: i64,
    compacted_row: Option<u32>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointFullSlot {
    row: Vec<u8>,
    partition: Vec<u8>,
    event_time: i64,
    multiplicity: i64,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
enum BoundedJoinInputModeCheckpoint {
    AppendOnly,
    Keyed {
        next_batch_id: u64,
        slots: Vec<CheckpointKeyedSlot>,
        compacted_rows_ipc: Vec<u8>,
    },
    Full {
        slots: Vec<CheckpointFullSlot>,
    },
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct BoundedJoinInputCheckpoint {
    version: u8,
    config_fingerprint: [u8; 32],
    closed_cutoff: i64,
    replay_frontiers: Vec<CheckpointReplayFrontier>,
    mode: BoundedJoinInputModeCheckpoint,
}

struct CapturedReplayFrontier {
    partition: Arc<[u8]>,
    frontier: ReplayFrontier,
}

struct CapturedKeyedSlot {
    key: Arc<[u8]>,
    slot: KeyedSlot,
}

struct CapturedFullSlot {
    row: Arc<[u8]>,
    slot: FullSlot,
}

enum CapturedInputMode {
    AppendOnly,
    Keyed {
        next_batch_id: u64,
        slots: Vec<CapturedKeyedSlot>,
        batches: Vec<(u64, Arc<RecordBatch>)>,
    },
    Full {
        slots: Vec<CapturedFullSlot>,
    },
}

pub(crate) struct BoundedJoinInputCheckpointCapture {
    config_fingerprint: [u8; 32],
    closed_cutoff: i64,
    replay_frontiers: Vec<CapturedReplayFrontier>,
    mode: CapturedInputMode,
    retained_bytes: usize,
}

/// A prepared differential batch whose logical state is not yet visible.
///
/// Dropping this value aborts the logical update. Capacity reserved during preparation stays in
/// the normalizer and remains included in its retained-memory accounting.
pub(crate) struct PreparedBoundedJoinInput<'a> {
    state: &'a mut BoundedJoinInputNormalizer,
    output: Vec<RecordBatch>,
    commit: InputCommit,
    projected_state_bytes: usize,
    transient_state_bytes: usize,
}

impl PreparedBoundedJoinInput<'_> {
    /// Weighted batches to pass to the private bounded-join kernel.
    pub(crate) fn output_batches(&self) -> &[RecordBatch] {
        &self.output
    }

    #[cfg(test)]
    fn output(&self) -> &RecordBatch {
        self.output
            .first()
            .expect("prepared nonempty input produces one output batch")
    }

    /// Exact retained-state charge after this prepared transaction commits.
    pub(crate) const fn projected_state_bytes(&self) -> usize {
        self.projected_state_bytes
    }

    /// Retained-state charge while the old logical image and preparation reservations coexist.
    pub(crate) const fn transient_state_bytes(&self) -> usize {
        self.transient_state_bytes
    }

    /// Publish the prepared replay frontiers, affinity slots, and current images.
    ///
    /// Preparation reserves every destination map. This method performs no fallible work and is
    /// intended to run synchronously immediately after downstream success.
    pub(crate) fn commit(self) {
        let Self {
            state,
            output: _,
            commit,
            projected_state_bytes: _,
            transient_state_bytes: _,
        } = self;
        state.apply_commit(commit);
    }
}

struct NormalizerEncodePreflight {
    persistent_metadata_bytes: usize,
    keyed_scratch_bytes: usize,
}

pub(super) struct NormalizerIpcRestorePreflight {
    pub(super) rows: usize,
    pub(super) body_bytes: usize,
}

fn checkpoint_allocation(bytes: usize) -> Result<usize, DbError> {
    bytes
        .checked_add(usize::from(bytes != 0) * HEAP_ALLOCATION_CHARGE)
        .ok_or_else(|| DbError::Checkpoint("normalizer checkpoint accounting overflow".into()))
}

fn checkpoint_roster<T>(len: usize) -> Result<usize, DbError> {
    checkpoint_allocation(len.checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
        DbError::Checkpoint("normalizer checkpoint roster accounting overflow".into())
    })?)
}

impl BoundedJoinInputCheckpointCapture {
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    fn encode_preflight(&self) -> Result<NormalizerEncodePreflight, DbError> {
        let mut persistent_metadata_bytes =
            checkpoint_roster::<CheckpointReplayFrontier>(self.replay_frontiers.len())?;
        for captured in &self.replay_frontiers {
            for bytes in [
                captured.partition.len(),
                captured.frontier.cursor.order.len(),
                captured.frontier.identity.row.len(),
            ] {
                persistent_metadata_bytes = persistent_metadata_bytes
                    .checked_add(checkpoint_allocation(bytes)?)
                    .ok_or_else(|| {
                        DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                    })?;
            }
        }

        let mut keyed_scratch_bytes = 0usize;
        match &self.mode {
            CapturedInputMode::AppendOnly => {}
            CapturedInputMode::Full { slots } => {
                persistent_metadata_bytes = persistent_metadata_bytes
                    .checked_add(checkpoint_roster::<CheckpointFullSlot>(slots.len())?)
                    .ok_or_else(|| {
                        DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                    })?;
                for captured in slots {
                    let partition_bytes = checkpoint_allocation(captured.slot.partition.len())?;
                    persistent_metadata_bytes = persistent_metadata_bytes
                        .checked_add(checkpoint_allocation(captured.row.len())?)
                        .and_then(|bytes| bytes.checked_add(partition_bytes))
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                        })?;
                }
            }
            CapturedInputMode::Keyed {
                next_batch_id,
                slots,
                batches,
            } => {
                if *next_batch_id == 0 || batches.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
                    return Err(DbError::Checkpoint(
                        "normalizer retained batch roster is non-canonical".into(),
                    ));
                }
                persistent_metadata_bytes = persistent_metadata_bytes
                    .checked_add(checkpoint_roster::<CheckpointKeyedSlot>(slots.len())?)
                    .ok_or_else(|| {
                        DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                    })?;
                let mut live_rows = 0usize;
                for captured in slots {
                    let partition_bytes = checkpoint_allocation(captured.slot.partition.len())?;
                    persistent_metadata_bytes = persistent_metadata_bytes
                        .checked_add(checkpoint_allocation(captured.key.len())?)
                        .and_then(|bytes| bytes.checked_add(partition_bytes))
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                        })?;
                    if let Some(row) = captured.slot.row {
                        let batch_index = batches
                            .binary_search_by_key(&row.batch_id, |(batch_id, _)| *batch_id)
                            .map_err(|_| {
                                DbError::Checkpoint(
                                    "normalizer slot references an uncaptured batch".into(),
                                )
                            })?;
                        if row.row as usize >= batches[batch_index].1.num_rows() {
                            return Err(DbError::Checkpoint(
                                "normalizer slot row is outside its retained batch".into(),
                            ));
                        }
                        live_rows = live_rows.checked_add(1).ok_or_else(|| {
                            DbError::Checkpoint("normalizer compacted row count overflow".into())
                        })?;
                    }
                }
                u32::try_from(live_rows).map_err(|_| {
                    DbError::Checkpoint("normalizer compacted row count exceeds u32".into())
                })?;
                if live_rows != 0 {
                    let schema = batches
                        .first()
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer live rows have no retained batch".into(),
                            )
                        })?
                        .1
                        .schema();
                    if batches
                        .iter()
                        .any(|(_, batch)| batch.schema().as_ref() != schema.as_ref())
                    {
                        return Err(DbError::Checkpoint(
                            "normalizer retained batches have mixed schemas".into(),
                        ));
                    }
                    let source_upper = batches.iter().try_fold(0usize, |bytes, (_, batch)| {
                        retained_batch_charge(batch)
                            .map_err(|error| DbError::Checkpoint(error.to_string()))
                            .and_then(|batch_bytes| {
                                bytes.checked_add(batch_bytes).ok_or_else(|| {
                                    DbError::Checkpoint(
                                        "normalizer compaction accounting overflow".into(),
                                    )
                                })
                            })
                    })?;
                    let column_bytes = checkpoint_roster::<ArrayRef>(schema.fields().len())?;
                    keyed_scratch_bytes = checkpoint_roster::<(usize, usize)>(live_rows)?
                        .checked_add(checkpoint_roster::<&dyn Array>(batches.len())?)
                        .and_then(|bytes| bytes.checked_add(column_bytes))
                        .and_then(|bytes| {
                            source_upper
                                .checked_mul(2)
                                .and_then(|upper| bytes.checked_add(upper))
                        })
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer compaction accounting overflow".into())
                        })?;
                }
            }
        }
        Ok(NormalizerEncodePreflight {
            persistent_metadata_bytes,
            keyed_scratch_bytes,
        })
    }

    pub(crate) fn encode(
        self,
        max_encoded_bytes: usize,
    ) -> Result<BoundedJoinInputCheckpoint, DbError> {
        let preflight = self.encode_preflight()?;
        let minimum = preflight
            .persistent_metadata_bytes
            .checked_add(preflight.keyed_scratch_bytes)
            .and_then(|bytes| {
                bytes.checked_add(
                    usize::from(preflight.keyed_scratch_bytes != 0) * HEAP_ALLOCATION_CHARGE,
                )
            })
            .ok_or_else(|| {
                DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
            })?;
        if minimum > max_encoded_bytes {
            return Err(DbError::Checkpoint(format!(
                "normalizer checkpoint materialization requires at least {minimum} bytes before IPC; limit is {max_encoded_bytes} bytes"
            )));
        }
        let mut replay_frontiers = Vec::new();
        replay_frontiers
            .try_reserve_exact(self.replay_frontiers.len())
            .map_err(|_| DbError::Checkpoint("normalizer replay checkpoint is too large".into()))?;
        for captured in self.replay_frontiers {
            let operation = match captured.frontier.identity.operation {
                ReplayOperation::Append => CheckpointReplayOperation::Append,
                ReplayOperation::Put => CheckpointReplayOperation::Put,
                ReplayOperation::Tombstone => CheckpointReplayOperation::Tombstone,
                ReplayOperation::Weight(weight) => CheckpointReplayOperation::Weight(weight),
            };
            replay_frontiers.push(CheckpointReplayFrontier {
                partition: captured.partition.as_ref().to_vec(),
                order: captured.frontier.cursor.order.as_ref().to_vec(),
                sub_offset: captured.frontier.cursor.sub_offset,
                row: captured.frontier.identity.row.as_ref().to_vec(),
                operation,
            });
        }

        let checkpoint = match self.mode {
            CapturedInputMode::AppendOnly => BoundedJoinInputCheckpoint {
                version: NORMALIZER_CHECKPOINT_VERSION,
                config_fingerprint: self.config_fingerprint,
                closed_cutoff: self.closed_cutoff,
                replay_frontiers,
                mode: BoundedJoinInputModeCheckpoint::AppendOnly,
            },
            CapturedInputMode::Keyed {
                next_batch_id,
                slots,
                batches,
            } => {
                let live_rows = slots
                    .iter()
                    .filter(|captured| captured.slot.row.is_some())
                    .count();
                let mut positions = Vec::new();
                positions.try_reserve_exact(live_rows).map_err(|_| {
                    DbError::Checkpoint("normalizer compacted-row roster is too large".into())
                })?;
                let mut encoded_slots = Vec::new();
                encoded_slots.try_reserve_exact(slots.len()).map_err(|_| {
                    DbError::Checkpoint("normalizer keyed checkpoint is too large".into())
                })?;
                for captured in slots {
                    let compacted_row = if let Some(row) = captured.slot.row {
                        let batch_index = batches
                            .binary_search_by_key(&row.batch_id, |(batch_id, _)| *batch_id)
                            .expect("capture preflight validated retained batch references");
                        let index = u32::try_from(positions.len()).map_err(|_| {
                            DbError::Checkpoint("normalizer compacted row count exceeds u32".into())
                        })?;
                        positions.push((batch_index, row.row as usize));
                        Some(index)
                    } else {
                        None
                    };
                    encoded_slots.push(CheckpointKeyedSlot {
                        key: captured.key.as_ref().to_vec(),
                        partition: captured.slot.partition.as_ref().to_vec(),
                        event_time: captured.slot.event_time,
                        compacted_row,
                    });
                }
                let mut checkpoint = BoundedJoinInputCheckpoint {
                    version: NORMALIZER_CHECKPOINT_VERSION,
                    config_fingerprint: self.config_fingerprint,
                    closed_cutoff: self.closed_cutoff,
                    replay_frontiers,
                    mode: BoundedJoinInputModeCheckpoint::Keyed {
                        next_batch_id,
                        slots: encoded_slots,
                        compacted_rows_ipc: Vec::new(),
                    },
                };
                if !positions.is_empty() {
                    let metadata_bytes = checkpoint.retained_bytes()?;
                    let serialization_limit = max_encoded_bytes
                        .checked_sub(metadata_bytes)
                        .and_then(|bytes| bytes.checked_sub(preflight.keyed_scratch_bytes))
                        .and_then(|bytes| bytes.checked_sub(HEAP_ALLOCATION_CHARGE))
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer compacted checkpoint exceeds its cumulative byte limit"
                                    .into(),
                            )
                        })?;
                    let schema = batches
                        .first()
                        .expect("capture preflight validated live retained batches")
                        .1
                        .schema();
                    let mut columns = Vec::new();
                    columns
                        .try_reserve_exact(schema.fields().len())
                        .map_err(|_| {
                            DbError::Checkpoint(
                                "normalizer compacted checkpoint is too wide".into(),
                            )
                        })?;
                    let mut arrays = Vec::<&dyn Array>::new();
                    arrays.try_reserve_exact(batches.len()).map_err(|_| {
                        DbError::Checkpoint(
                            "normalizer checkpoint batch-column roster is too large".into(),
                        )
                    })?;
                    for column in 0..schema.fields().len() {
                        arrays.clear();
                        arrays.extend(
                            batches
                                .iter()
                                .map(|(_, batch)| batch.column(column).as_ref()),
                        );
                        columns.push(arrow::compute::interleave(&arrays, &positions).map_err(
                            |error| {
                                DbError::query_pipeline_arrow(
                                    "normalizer checkpoint compaction",
                                    &error,
                                )
                            },
                        )?);
                    }
                    let compacted = RecordBatch::try_new(schema, columns).map_err(|error| {
                        DbError::query_pipeline_arrow(
                            "normalizer compacted checkpoint batch",
                            &error,
                        )
                    })?;
                    let compacted_rows_ipc =
                        laminar_core::serialization::serialize_batches_stream_bounded(
                            compacted.schema().as_ref(),
                            std::iter::once(&compacted),
                            serialization_limit,
                        )
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "normalizer compacted checkpoint serialization: {error}"
                            ))
                        })?;
                    let BoundedJoinInputModeCheckpoint::Keyed {
                        compacted_rows_ipc: destination,
                        ..
                    } = &mut checkpoint.mode
                    else {
                        unreachable!("constructed keyed checkpoint mode")
                    };
                    *destination = compacted_rows_ipc;
                }
                checkpoint
            }
            CapturedInputMode::Full { slots } => {
                let mut encoded_slots = Vec::new();
                encoded_slots.try_reserve_exact(slots.len()).map_err(|_| {
                    DbError::Checkpoint("normalizer full checkpoint is too large".into())
                })?;
                encoded_slots.extend(slots.into_iter().map(|captured| CheckpointFullSlot {
                    row: captured.row.as_ref().to_vec(),
                    partition: captured.slot.partition.as_ref().to_vec(),
                    event_time: captured.slot.event_time,
                    multiplicity: captured.slot.multiplicity,
                }));
                BoundedJoinInputCheckpoint {
                    version: NORMALIZER_CHECKPOINT_VERSION,
                    config_fingerprint: self.config_fingerprint,
                    closed_cutoff: self.closed_cutoff,
                    replay_frontiers,
                    mode: BoundedJoinInputModeCheckpoint::Full {
                        slots: encoded_slots,
                    },
                }
            }
        };
        let retained = checkpoint.retained_bytes()?;
        if retained > max_encoded_bytes {
            return Err(DbError::Checkpoint(format!(
                "normalizer checkpoint retains {retained} bytes; limit is {max_encoded_bytes} bytes"
            )));
        }
        Ok(checkpoint)
    }
}

impl BoundedJoinInputCheckpoint {
    pub(crate) fn retained_bytes(&self) -> Result<usize, DbError> {
        fn bytes_vec(bytes: &Vec<u8>) -> Result<usize, DbError> {
            checkpoint_allocation(bytes.capacity())
        }

        let mut bytes =
            checkpoint_roster::<CheckpointReplayFrontier>(self.replay_frontiers.capacity())?;
        for frontier in &self.replay_frontiers {
            let partition_bytes = bytes_vec(&frontier.partition)?;
            let order_bytes = bytes_vec(&frontier.order)?;
            let row_bytes = bytes_vec(&frontier.row)?;
            bytes = bytes
                .checked_add(partition_bytes)
                .and_then(|bytes| bytes.checked_add(order_bytes))
                .and_then(|bytes| bytes.checked_add(row_bytes))
                .ok_or_else(|| {
                    DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                })?;
        }
        bytes = match &self.mode {
            BoundedJoinInputModeCheckpoint::AppendOnly => bytes,
            BoundedJoinInputModeCheckpoint::Keyed {
                slots,
                compacted_rows_ipc,
                ..
            } => {
                let slot_roster = checkpoint_roster::<CheckpointKeyedSlot>(slots.capacity())?;
                let ipc_bytes = bytes_vec(compacted_rows_ipc)?;
                let mut bytes = bytes
                    .checked_add(slot_roster)
                    .and_then(|bytes| bytes.checked_add(ipc_bytes))
                    .ok_or_else(|| {
                        DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                    })?;
                for slot in slots {
                    let key_bytes = bytes_vec(&slot.key)?;
                    let partition_bytes = bytes_vec(&slot.partition)?;
                    bytes = bytes
                        .checked_add(key_bytes)
                        .and_then(|bytes| bytes.checked_add(partition_bytes))
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                        })?;
                }
                bytes
            }
            BoundedJoinInputModeCheckpoint::Full { slots } => {
                let slot_roster = checkpoint_roster::<CheckpointFullSlot>(slots.capacity())?;
                let mut bytes = bytes.checked_add(slot_roster).ok_or_else(|| {
                    DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                })?;
                for slot in slots {
                    let row_bytes = bytes_vec(&slot.row)?;
                    let partition_bytes = bytes_vec(&slot.partition)?;
                    bytes = bytes
                        .checked_add(row_bytes)
                        .and_then(|bytes| bytes.checked_add(partition_bytes))
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer checkpoint accounting overflow".into())
                        })?;
                }
                bytes
            }
        };
        Ok(bytes)
    }
}

/// Concrete replay and mutation state for one bounded-join input vnode.
pub(crate) struct BoundedJoinInputNormalizer {
    config: BoundedJoinInputConfig,
    input_schema: SchemaRef,
    visible_schema: SchemaRef,
    output_schema: SchemaRef,
    row_codec: RowConverter,
    replay_frontiers: FxHashMap<Arc<[u8]>, ReplayFrontier>,
    mode: ModeState,
    closed_cutoff: i64,
    base_bytes: usize,
    logical_bytes: usize,
    capacity_bytes: usize,
    charged_bytes: usize,
    config_fingerprint: [u8; 32],
}

impl BoundedJoinInputNormalizer {
    pub(crate) fn construction_preflight_bytes(
        input_schema: &Schema,
        event_time_index: usize,
        mode: &BoundedJoinInputMode,
    ) -> Result<usize, DbError> {
        validate_linear_row_encoding_schema(input_schema)?;
        validate_event_time(input_schema, event_time_index)?;
        let (primary_key_fields, primary_key_validation_scratch) = match mode {
            BoundedJoinInputMode::AppendOnly => {
                validate_no_weight(input_schema, "append-only")?;
                (0, 0)
            }
            BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices,
            } => {
                validate_no_weight(input_schema, "keyed-upsert")?;
                if primary_key_indices.is_empty()
                    || !primary_key_indices.contains(&event_time_index)
                    || primary_key_indices
                        .iter()
                        .any(|index| *index >= input_schema.fields().len())
                {
                    return Err(DbError::Config(
                        "bounded join keyed primary-key contract is invalid".into(),
                    ));
                }
                (
                    primary_key_indices.len(),
                    primary_key_validation_scratch_bytes(input_schema.fields().len())?,
                )
            }
            BoundedJoinInputMode::FullChangelog => {
                let weight_index = full_weight_index(input_schema)?;
                if event_time_index == weight_index {
                    return Err(DbError::Config(
                        "bounded join event time cannot be the weight column".into(),
                    ));
                }
                (0, 0)
            }
        };
        let schema_bytes = schema_charge(input_schema)?;
        let converter_fields = input_schema
            .fields()
            .len()
            .checked_add(primary_key_fields)
            .ok_or_else(|| DbError::Config("bounded join construction overflow".into()))?;
        BASE_STATE_CHARGE
            .checked_add(schema_bytes.checked_mul(8).ok_or_else(|| {
                DbError::Config("bounded join construction accounting overflow".into())
            })?)
            .and_then(|bytes| {
                converter_fields
                    .checked_mul(2_048)
                    .and_then(|converter| bytes.checked_add(converter))
            })
            .and_then(|bytes| bytes.checked_add(1_024))
            .and_then(|bytes| bytes.checked_add(primary_key_validation_scratch))
            .ok_or_else(|| DbError::Config("bounded join construction accounting overflow".into()))
    }

    /// Construct a private normalizer for a visible source schema.
    ///
    /// `input_schema` excludes hidden source metadata. It includes the sole trailing `__weight`
    /// only for [`BoundedJoinInputMode::FullChangelog`].
    pub(crate) fn try_new(
        input_schema: SchemaRef,
        config: BoundedJoinInputConfig,
    ) -> Result<Self, DbError> {
        let dynamic_remaining_budget = config.max_retained_bytes;
        Self::try_new_at_cutoff(input_schema, config, i64::MIN, dynamic_remaining_budget)
    }

    pub(crate) fn try_new_at_cutoff(
        input_schema: SchemaRef,
        config: BoundedJoinInputConfig,
        closed_cutoff: i64,
        dynamic_remaining_budget: usize,
    ) -> Result<Self, DbError> {
        let effective_limit = config.max_retained_bytes.min(dynamic_remaining_budget);
        let preflight = Self::construction_preflight_bytes(
            input_schema.as_ref(),
            config.event_time_index,
            &config.mode,
        )?;
        if preflight > effective_limit {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "bounded join input vnode {} construction preflight",
                    config.vnode
                ),
                accounted_bytes: preflight,
                limit_bytes: effective_limit,
            });
        }
        if let BoundedJoinInputMode::KeyedUpsert {
            primary_key_indices,
        } = &config.mode
        {
            let requested = primary_key_validation_scratch_bytes(input_schema.fields().len())?;
            let actual = validate_primary_key_indices_unique(
                primary_key_indices,
                input_schema.fields().len(),
            )?;
            let actual_preflight = preflight
                .checked_sub(requested)
                .and_then(|bytes| bytes.checked_add(actual))
                .ok_or_else(|| DbError::Config("bounded join construction overflow".into()))?;
            if actual_preflight > effective_limit {
                return Err(DbError::ManagedStateBudgetExceeded {
                    context: format!(
                        "bounded join input vnode {} primary-key validation",
                        config.vnode
                    ),
                    accounted_bytes: actual_preflight,
                    limit_bytes: effective_limit,
                });
            }
        }
        let (visible_schema, mode) = match &config.mode {
            BoundedJoinInputMode::AppendOnly => {
                validate_no_weight(&input_schema, "append-only")?;
                (Arc::clone(&input_schema), ModeState::AppendOnly)
            }
            BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices,
            } => {
                validate_no_weight(&input_schema, "keyed-upsert")?;
                if primary_key_indices.is_empty() {
                    return Err(DbError::Config(
                        "bounded join keyed-upsert input requires a primary key".into(),
                    ));
                }
                let mut fields = Vec::new();
                fields
                    .try_reserve_exact(primary_key_indices.len())
                    .map_err(|_| DbError::Config("bounded join primary key is too wide".into()))?;
                for &index in primary_key_indices {
                    let field = input_schema.fields().get(index).ok_or_else(|| {
                        DbError::Config(
                            "bounded join primary-key index is outside the visible schema".into(),
                        )
                    })?;
                    fields.push(SortField::new(field.data_type().clone()));
                }
                if !primary_key_indices.contains(&config.event_time_index) {
                    return Err(DbError::Config(
                        "bounded join keyed-upsert primary key must include event time".into(),
                    ));
                }
                let primary_key_codec = RowConverter::new(fields).map_err(|error| {
                    DbError::Config(format!(
                        "bounded join primary key cannot be deterministically encoded: {error}"
                    ))
                })?;
                (
                    Arc::clone(&input_schema),
                    ModeState::Keyed(KeyedState {
                        primary_key_codec,
                        primary_key_indices: primary_key_indices.clone(),
                        slots: FxHashMap::default(),
                        gc: ExactGcIndex::new(),
                        retained_batches: FxHashMap::default(),
                        next_batch_id: 1,
                    }),
                )
            }
            BoundedJoinInputMode::FullChangelog => {
                let visible = validate_and_strip_weight_schema(&input_schema)?;
                (
                    visible,
                    ModeState::Full(FullState {
                        slots: FxHashMap::default(),
                        gc: ExactGcIndex::new(),
                    }),
                )
            }
        };
        validate_event_time(&visible_schema, config.event_time_index)?;
        let row_codec = row_codec(&visible_schema)?;
        let output_schema = weighted_schema(&visible_schema);
        let config_fingerprint = normalizer_config_fingerprint(
            input_schema.as_ref(),
            config.event_time_index,
            &config.mode,
        );
        let base_bytes = construction_charge(
            &input_schema,
            &visible_schema,
            &output_schema,
            &row_codec,
            &mode,
            &config.mode,
        )?;
        if base_bytes > effective_limit {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("bounded join input vnode {} base state", config.vnode),
                accounted_bytes: base_bytes,
                limit_bytes: effective_limit,
            });
        }
        Ok(Self {
            config,
            input_schema,
            visible_schema,
            output_schema,
            row_codec,
            replay_frontiers: FxHashMap::default(),
            mode,
            closed_cutoff,
            base_bytes,
            logical_bytes: 0,
            capacity_bytes: 0,
            charged_bytes: base_bytes,
            config_fingerprint,
        })
    }

    /// Conservative retained-memory charge, including aborted preparation reservations.
    pub(crate) const fn accounted_state_bytes(&self) -> usize {
        self.charged_bytes
    }

    /// Current event-time floor. Rows below it are terminally late.
    pub(crate) const fn closed_cutoff(&self) -> i64 {
        self.closed_cutoff
    }

    /// Stable execution/checkpoint fingerprint excluding only vnode-local budget policy.
    #[cfg(test)]
    pub(crate) const fn config_fingerprint(&self) -> [u8; 32] {
        self.config_fingerprint
    }

    pub(crate) fn visible_schema(&self) -> &SchemaRef {
        &self.visible_schema
    }

    pub(crate) fn capture_checkpoint(
        &self,
        max_capture_bytes: usize,
    ) -> Result<BoundedJoinInputCheckpointCapture, DbError> {
        let roster_bytes =
            checkpoint_roster::<CapturedReplayFrontier>(self.replay_frontiers.len())?
                .checked_add(match &self.mode {
                    ModeState::AppendOnly => 0,
                    ModeState::Keyed(keyed) => {
                        checkpoint_roster::<CapturedKeyedSlot>(keyed.slots.len())?
                            .checked_add(checkpoint_roster::<(u64, Arc<RecordBatch>)>(
                                keyed.retained_batches.len(),
                            )?)
                            .ok_or_else(|| {
                                self.terminal("checkpoint capture accounting overflow")
                            })?
                    }
                    ModeState::Full(full) => {
                        checkpoint_roster::<CapturedFullSlot>(full.slots.len())?
                    }
                })
                .ok_or_else(|| self.terminal("checkpoint capture accounting overflow"))?;
        let projected_retained = self
            .accounted_state_bytes()
            .checked_add(roster_bytes)
            .ok_or_else(|| self.terminal("checkpoint capture accounting overflow"))?;
        if projected_retained > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "bounded join input vnode {} capture requires {projected_retained} bytes; remaining capture limit is {max_capture_bytes} bytes",
                self.config.vnode
            )));
        }
        let mut replay_frontiers = Vec::new();
        replay_frontiers
            .try_reserve_exact(self.replay_frontiers.len())
            .map_err(|_| self.terminal("checkpoint replay roster cannot be reserved"))?;
        replay_frontiers.extend(self.replay_frontiers.iter().map(|(partition, frontier)| {
            CapturedReplayFrontier {
                partition: Arc::clone(partition),
                frontier: frontier.clone(),
            }
        }));
        replay_frontiers.sort_unstable_by(|left, right| left.partition.cmp(&right.partition));

        let mode = match &self.mode {
            ModeState::AppendOnly => CapturedInputMode::AppendOnly,
            ModeState::Keyed(keyed) => {
                let mut slots = Vec::new();
                slots.try_reserve_exact(keyed.slots.len()).map_err(|_| {
                    self.terminal("checkpoint keyed-slot roster cannot be reserved")
                })?;
                slots.extend(keyed.slots.iter().map(|(key, slot)| CapturedKeyedSlot {
                    key: Arc::clone(key),
                    slot: slot.clone(),
                }));
                slots.sort_unstable_by(|left, right| left.key.cmp(&right.key));
                let mut batches = Vec::new();
                batches
                    .try_reserve_exact(keyed.retained_batches.len())
                    .map_err(|_| {
                        self.terminal("checkpoint retained-batch roster cannot be reserved")
                    })?;
                batches.extend(
                    keyed
                        .retained_batches
                        .iter()
                        .map(|(batch_id, retained)| (*batch_id, Arc::clone(&retained.batch))),
                );
                batches.sort_unstable_by_key(|(batch_id, _)| *batch_id);
                CapturedInputMode::Keyed {
                    next_batch_id: keyed.next_batch_id,
                    slots,
                    batches,
                }
            }
            ModeState::Full(full) => {
                let mut slots = Vec::new();
                slots
                    .try_reserve_exact(full.slots.len())
                    .map_err(|_| self.terminal("checkpoint full-slot roster cannot be reserved"))?;
                slots.extend(full.slots.iter().map(|(row, slot)| CapturedFullSlot {
                    row: Arc::clone(row),
                    slot: slot.clone(),
                }));
                slots.sort_unstable_by(|left, right| left.row.cmp(&right.row));
                CapturedInputMode::Full { slots }
            }
        };
        let roster_charge = |capacity: usize, element: usize| {
            capacity
                .checked_mul(element)
                .and_then(|bytes| {
                    bytes.checked_add(usize::from(capacity != 0) * HEAP_ALLOCATION_CHARGE)
                })
                .ok_or_else(|| self.terminal("checkpoint capture accounting overflow"))
        };
        let mut capture_roster_bytes = roster_charge(
            replay_frontiers.capacity(),
            std::mem::size_of::<CapturedReplayFrontier>(),
        )?;
        match &mode {
            CapturedInputMode::AppendOnly => {}
            CapturedInputMode::Keyed { slots, batches, .. } => {
                let slot_bytes =
                    roster_charge(slots.capacity(), std::mem::size_of::<CapturedKeyedSlot>())?;
                let batch_bytes = roster_charge(
                    batches.capacity(),
                    std::mem::size_of::<(u64, Arc<RecordBatch>)>(),
                )?;
                capture_roster_bytes = capture_roster_bytes
                    .checked_add(slot_bytes)
                    .and_then(|bytes| bytes.checked_add(batch_bytes))
                    .ok_or_else(|| self.terminal("checkpoint capture accounting overflow"))?;
            }
            CapturedInputMode::Full { slots } => {
                capture_roster_bytes = capture_roster_bytes
                    .checked_add(roster_charge(
                        slots.capacity(),
                        std::mem::size_of::<CapturedFullSlot>(),
                    )?)
                    .ok_or_else(|| self.terminal("checkpoint capture accounting overflow"))?;
            }
        }
        let retained_bytes = self
            .accounted_state_bytes()
            .checked_add(capture_roster_bytes)
            .ok_or_else(|| self.terminal("checkpoint capture accounting overflow"))?;
        if retained_bytes > max_capture_bytes {
            return Err(DbError::Checkpoint(format!(
                "bounded join input vnode {} capture retains {retained_bytes} bytes; remaining capture limit is {max_capture_bytes} bytes",
                self.config.vnode
            )));
        }
        Ok(BoundedJoinInputCheckpointCapture {
            config_fingerprint: self.config_fingerprint,
            closed_cutoff: self.closed_cutoff,
            replay_frontiers,
            mode,
            retained_bytes,
        })
    }

    fn checkpoint_restore_preflight_bytes(
        checkpoint: &BoundedJoinInputCheckpoint,
        input_schema: &Schema,
        config: &BoundedJoinInputConfig,
    ) -> Result<usize, DbError> {
        let mut logical_bytes = 0usize;
        let mut previous_partition: Option<&[u8]> = None;
        for captured in &checkpoint.replay_frontiers {
            if previous_partition.is_some_and(|previous| previous >= captured.partition.as_slice())
            {
                return Err(DbError::Checkpoint(
                    "normalizer replay checkpoint is not strictly sorted".into(),
                ));
            }
            let operation_matches = match (&config.mode, &captured.operation) {
                (BoundedJoinInputMode::AppendOnly, CheckpointReplayOperation::Append)
                | (
                    BoundedJoinInputMode::KeyedUpsert { .. },
                    CheckpointReplayOperation::Put | CheckpointReplayOperation::Tombstone,
                ) => true,
                (
                    BoundedJoinInputMode::FullChangelog,
                    CheckpointReplayOperation::Weight(weight),
                ) => *weight != 0,
                _ => false,
            };
            if !operation_matches {
                return Err(DbError::Checkpoint(
                    "normalizer replay operation disagrees with its mode".into(),
                ));
            }
            logical_bytes = logical_bytes
                .checked_add(FRONTIER_ENTRY_CHARGE)
                .and_then(|bytes| bytes.checked_add(captured.partition.len()))
                .and_then(|bytes| bytes.checked_add(captured.order.len()))
                .and_then(|bytes| bytes.checked_add(captured.row.len()))
                .ok_or_else(|| {
                    DbError::Checkpoint("normalizer restore logical accounting overflow".into())
                })?;
            previous_partition = Some(captured.partition.as_slice());
        }

        let frontier_capacity =
            predicted_capacity(0, 0, checkpoint.replay_frontiers.len(), config.vnode)?;
        let (slot_capacity, batch_capacity, gc_capacity, scratch_bytes) =
            match (&config.mode, &checkpoint.mode) {
                (BoundedJoinInputMode::AppendOnly, BoundedJoinInputModeCheckpoint::AppendOnly) => {
                    (0, 0, 0, 0)
                }
                (
                    BoundedJoinInputMode::KeyedUpsert { .. },
                    BoundedJoinInputModeCheckpoint::Keyed {
                        next_batch_id,
                        slots,
                        compacted_rows_ipc,
                    },
                ) => {
                    let live_rows = slots
                        .iter()
                        .filter(|slot| slot.compacted_row.is_some())
                        .count();
                    if (*next_batch_id == 0 || (*next_batch_id == 1 && live_rows != 0))
                        || (live_rows == 0) != compacted_rows_ipc.is_empty()
                    {
                        return Err(DbError::Checkpoint(
                            "normalizer keyed checkpoint batch metadata is invalid".into(),
                        ));
                    }
                    let mut previous_key: Option<&[u8]> = None;
                    let mut expected_row = 0usize;
                    for captured in slots {
                        if previous_key.is_some_and(|previous| previous >= captured.key.as_slice())
                            || captured.event_time < checkpoint.closed_cutoff
                        {
                            return Err(DbError::Checkpoint(
                                "normalizer keyed slots are non-canonical or below cutoff".into(),
                            ));
                        }
                        if let Some(compacted_row) = captured.compacted_row {
                            if compacted_row as usize != expected_row {
                                return Err(DbError::Checkpoint(
                                    "normalizer compacted row references are non-canonical".into(),
                                ));
                            }
                            expected_row = expected_row.checked_add(1).ok_or_else(|| {
                                DbError::Checkpoint(
                                    "normalizer compacted row count overflow".into(),
                                )
                            })?;
                        }
                        logical_bytes = logical_bytes
                            .checked_add(SLOT_ENTRY_CHARGE)
                            .and_then(|bytes| bytes.checked_add(captured.key.len()))
                            .and_then(|bytes| bytes.checked_add(captured.partition.len()))
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "normalizer keyed restore accounting overflow".into(),
                                )
                            })?;
                        previous_key = Some(captured.key.as_slice());
                    }
                    if expected_row != live_rows {
                        return Err(DbError::Checkpoint(
                            "normalizer compacted row count is inconsistent".into(),
                        ));
                    }
                    let ipc_preflight = if live_rows == 0 {
                        NormalizerIpcRestorePreflight {
                            rows: 0,
                            body_bytes: 0,
                        }
                    } else {
                        preflight_normalizer_ipc_restore(compacted_rows_ipc)?
                    };
                    if ipc_preflight.rows != live_rows
                        || ipc_preflight.body_bytes > compacted_rows_ipc.len()
                    {
                        return Err(DbError::Checkpoint(
                            "normalizer current-image IPC row or body accounting is invalid".into(),
                        ));
                    }
                    let decoded_payload_bound =
                        compacted_rows_ipc.len().checked_mul(4).ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer current-image decode accounting overflow".into(),
                            )
                        })?;
                    let cell_count = live_rows
                        .checked_mul(input_schema.fields().len())
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer current-image cell accounting overflow".into(),
                            )
                        })?;
                    let identity_bound = cell_count
                        .checked_mul(64)
                        .and_then(|bytes| bytes.checked_add(decoded_payload_bound))
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer current-image identity accounting overflow".into(),
                            )
                        })?;
                    logical_bytes = logical_bytes.checked_add(identity_bound).ok_or_else(|| {
                        DbError::Checkpoint(
                            "normalizer current-image identity accounting overflow".into(),
                        )
                    })?;
                    if live_rows != 0 {
                        logical_bytes = logical_bytes
                            .checked_add(RETAINED_BATCH_CHARGE)
                            .and_then(|bytes| bytes.checked_add(decoded_payload_bound))
                            .and_then(|bytes| {
                                input_schema
                                    .fields()
                                    .len()
                                    .checked_mul(std::mem::size_of::<ArrayRef>())
                                    .and_then(|columns| bytes.checked_add(columns))
                            })
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "normalizer current-image batch accounting overflow".into(),
                                )
                            })?;
                    }
                    let row_scratch = live_rows
                        .checked_mul(NORMALIZATION_ROW_SCRATCH_CHARGE)
                        .and_then(|bytes| {
                            cell_count
                                .checked_mul(NORMALIZATION_CELL_SCRATCH_CHARGE)
                                .and_then(|cells| bytes.checked_add(cells))
                        })
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer current-image row scratch overflow".into(),
                            )
                        })?;
                    let column_scratch = input_schema
                        .fields()
                        .len()
                        .checked_mul(std::mem::size_of::<ArrayRef>())
                        .and_then(|bytes| bytes.checked_add(HEAP_ALLOCATION_CHARGE))
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer current-image column scratch overflow".into(),
                            )
                        })?;
                    let scratch = decoded_payload_bound
                        .checked_add(row_scratch)
                        .and_then(|bytes| bytes.checked_add(column_scratch))
                        .ok_or_else(|| {
                            DbError::Checkpoint(
                                "normalizer current-image scratch accounting overflow".into(),
                            )
                        })?;
                    (
                        predicted_capacity(0, 0, slots.len(), config.vnode)?,
                        predicted_capacity(0, 0, usize::from(live_rows != 0), config.vnode)?,
                        predicted_vec_capacity(0, 0, slots.len(), config.vnode)?,
                        scratch,
                    )
                }
                (
                    BoundedJoinInputMode::FullChangelog,
                    BoundedJoinInputModeCheckpoint::Full { slots },
                ) => {
                    let mut previous_row: Option<&[u8]> = None;
                    for captured in slots {
                        if previous_row.is_some_and(|previous| previous >= captured.row.as_slice())
                            || captured.event_time < checkpoint.closed_cutoff
                            || captured.multiplicity < 0
                        {
                            return Err(DbError::Checkpoint(
                                "normalizer full slots are non-canonical or invalid".into(),
                            ));
                        }
                        logical_bytes = logical_bytes
                            .checked_add(SLOT_ENTRY_CHARGE)
                            .and_then(|bytes| bytes.checked_add(captured.row.len()))
                            .and_then(|bytes| bytes.checked_add(captured.partition.len()))
                            .ok_or_else(|| {
                                DbError::Checkpoint(
                                    "normalizer full restore accounting overflow".into(),
                                )
                            })?;
                        previous_row = Some(captured.row.as_slice());
                    }
                    (
                        predicted_capacity(0, 0, slots.len(), config.vnode)?,
                        0,
                        predicted_vec_capacity(0, 0, slots.len(), config.vnode)?,
                        0,
                    )
                }
                _ => {
                    return Err(DbError::Checkpoint(
                        "bounded join input checkpoint mode changed".into(),
                    ));
                }
            };
        let base_bytes = Self::construction_preflight_bytes(
            input_schema,
            config.event_time_index,
            &config.mode,
        )?;
        let capacity_bytes = capacity_charge(
            frontier_capacity,
            slot_capacity,
            batch_capacity,
            gc_capacity,
            config.vnode,
        )?;
        base_bytes
            .checked_add(logical_bytes)
            .and_then(|bytes| bytes.checked_add(capacity_bytes))
            .and_then(|bytes| bytes.checked_add(scratch_bytes))
            .ok_or_else(|| {
                DbError::Checkpoint("normalizer restore peak accounting overflow".into())
            })
    }

    pub(crate) fn from_checkpoint(
        checkpoint: &BoundedJoinInputCheckpoint,
        input_schema: SchemaRef,
        config: BoundedJoinInputConfig,
        dynamic_remaining_budget: usize,
    ) -> Result<Self, DbError> {
        if checkpoint.version != NORMALIZER_CHECKPOINT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "bounded join input checkpoint version {} is unsupported",
                checkpoint.version
            )));
        }
        let effective_limit = config.max_retained_bytes.min(dynamic_remaining_budget);
        let restore_peak =
            Self::checkpoint_restore_preflight_bytes(checkpoint, input_schema.as_ref(), &config)?;
        if restore_peak > effective_limit {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "bounded join input vnode {} checkpoint restore peak",
                    config.vnode
                ),
                accounted_bytes: restore_peak,
                limit_bytes: effective_limit,
            });
        }
        let expected_fingerprint = normalizer_config_fingerprint(
            input_schema.as_ref(),
            config.event_time_index,
            &config.mode,
        );
        if checkpoint.config_fingerprint != expected_fingerprint {
            return Err(DbError::Checkpoint(
                "bounded join input checkpoint configuration fingerprint changed".into(),
            ));
        }
        // The caller owns and accounts the decoded checkpoint separately from this component's
        // remaining restore headroom. Still validate its internal capacity arithmetic here.
        checkpoint.retained_bytes()?;
        let mut state = Self::try_new_at_cutoff(
            input_schema,
            config,
            checkpoint.closed_cutoff,
            effective_limit,
        )?;
        state
            .replay_frontiers
            .try_reserve(checkpoint.replay_frontiers.len())
            .map_err(|_| DbError::Checkpoint("normalizer replay state is too large".into()))?;
        let mut previous_partition: Option<&[u8]> = None;
        for captured in &checkpoint.replay_frontiers {
            if previous_partition.is_some_and(|previous| previous >= captured.partition.as_slice())
            {
                return Err(DbError::Checkpoint(
                    "normalizer replay checkpoint is not strictly sorted".into(),
                ));
            }
            let operation = match (&state.mode, &captured.operation) {
                (ModeState::AppendOnly, CheckpointReplayOperation::Append) => {
                    ReplayOperation::Append
                }
                (ModeState::Keyed(_), CheckpointReplayOperation::Put) => ReplayOperation::Put,
                (ModeState::Keyed(_), CheckpointReplayOperation::Tombstone) => {
                    ReplayOperation::Tombstone
                }
                (ModeState::Full(_), CheckpointReplayOperation::Weight(weight)) if *weight != 0 => {
                    ReplayOperation::Weight(*weight)
                }
                _ => {
                    return Err(DbError::Checkpoint(
                        "normalizer replay operation disagrees with its mode".into(),
                    ));
                }
            };
            let partition = Arc::<[u8]>::from(captured.partition.as_slice());
            state.replay_frontiers.insert(
                partition,
                ReplayFrontier {
                    cursor: ReplayCursor {
                        order: Arc::from(captured.order.as_slice()),
                        sub_offset: captured.sub_offset,
                    },
                    identity: ReplayIdentity {
                        row: Arc::from(captured.row.as_slice()),
                        operation,
                    },
                },
            );
            previous_partition = Some(captured.partition.as_slice());
        }

        match (&mut state.mode, &checkpoint.mode) {
            (ModeState::AppendOnly, BoundedJoinInputModeCheckpoint::AppendOnly) => {}
            (
                ModeState::Keyed(keyed),
                BoundedJoinInputModeCheckpoint::Keyed {
                    next_batch_id,
                    slots,
                    compacted_rows_ipc,
                },
            ) => {
                let live_rows = slots
                    .iter()
                    .filter(|slot| slot.compacted_row.is_some())
                    .count();
                if (*next_batch_id == 0 || (*next_batch_id == 1 && live_rows != 0))
                    || (live_rows == 0) != compacted_rows_ipc.is_empty()
                {
                    return Err(DbError::Checkpoint(
                        "normalizer keyed checkpoint batch metadata is invalid".into(),
                    ));
                }
                let compacted = if live_rows == 0 {
                    None
                } else {
                    let decoded =
                        laminar_core::serialization::deserialize_batch_stream(compacted_rows_ipc)
                            .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "normalizer current-image batch decode: {error}"
                            ))
                        })?;
                    if decoded.num_rows() != live_rows
                        || decoded.schema().as_ref() != state.visible_schema.as_ref()
                    {
                        return Err(DbError::Checkpoint(
                            "normalizer current-image batch shape or schema changed".into(),
                        ));
                    }
                    Some(Arc::new(canonicalize_batch_schema(
                        decoded,
                        &state.visible_schema,
                    )?))
                };
                let row_bytes = compacted
                    .as_ref()
                    .map(|batch| logical_row_bytes(batch))
                    .transpose()?;
                let encoded_rows = compacted
                    .as_ref()
                    .map(|batch| {
                        state
                            .row_codec
                            .convert_columns(batch.columns())
                            .map_err(|error| {
                                DbError::Checkpoint(format!(
                                    "normalizer current-image rows cannot be encoded: {error}"
                                ))
                            })
                    })
                    .transpose()?;
                let primary_keys = compacted
                    .as_ref()
                    .map(|batch| {
                        let key_columns = keyed
                            .primary_key_indices
                            .iter()
                            .map(|&index| batch.column(index))
                            .cloned()
                            .collect::<Vec<_>>();
                        keyed
                            .primary_key_codec
                            .convert_columns(&key_columns)
                            .map_err(|error| {
                                DbError::Checkpoint(format!(
                                    "normalizer current-image primary keys cannot be encoded: {error}"
                                ))
                            })
                    })
                    .transpose()?;
                let event_times = compacted
                    .as_ref()
                    .map(|batch| {
                        TimestampMillisView::try_new(
                            batch.column(state.config.event_time_index).as_ref(),
                            "bounded join input checkpoint",
                        )
                        .map_err(|error| {
                            DbError::Checkpoint(format!(
                                "normalizer current-image event-time decode: {error}"
                            ))
                        })
                    })
                    .transpose()?;

                keyed.slots.try_reserve(slots.len()).map_err(|_| {
                    DbError::Checkpoint("normalizer keyed state is too large".into())
                })?;
                keyed.gc.try_reserve(slots.len(), state.config.vnode)?;
                if live_rows != 0 {
                    keyed.retained_batches.try_reserve(1).map_err(|_| {
                        DbError::Checkpoint("normalizer retained batch cannot be reserved".into())
                    })?;
                }
                let restored_batch_id = next_batch_id.saturating_sub(1);
                let mut expected_row = 0usize;
                let mut previous_key: Option<&[u8]> = None;
                for captured in slots {
                    if previous_key.is_some_and(|previous| previous >= captured.key.as_slice())
                        || captured.event_time < checkpoint.closed_cutoff
                    {
                        return Err(DbError::Checkpoint(
                            "normalizer keyed slots are non-canonical or below cutoff".into(),
                        ));
                    }
                    let (row_identity, row) = if let Some(compacted_row) = captured.compacted_row {
                        if compacted_row as usize != expected_row {
                            return Err(DbError::Checkpoint(
                                "normalizer compacted row references are non-canonical".into(),
                            ));
                        }
                        let batch = compacted.as_ref().expect("live checkpoint batch decoded");
                        if keyed
                            .primary_key_indices
                            .iter()
                            .any(|&index| batch.column(index).is_null(expected_row))
                            || primary_keys
                                .as_ref()
                                .expect("live primary keys encoded")
                                .row(expected_row)
                                .data()
                                != captured.key.as_slice()
                            || event_times
                                .as_ref()
                                .expect("live event times decoded")
                                .value(expected_row, "bounded join input checkpoint")
                                .map_err(|error| {
                                    DbError::Checkpoint(format!(
                                        "normalizer current-image event-time value: {error}"
                                    ))
                                })?
                                != captured.event_time
                        {
                            return Err(DbError::Checkpoint(
                                "normalizer compacted row does not match its keyed slot".into(),
                            ));
                        }
                        let identity = Arc::from(
                            encoded_rows
                                .as_ref()
                                .expect("live rows encoded")
                                .row(expected_row)
                                .data(),
                        );
                        let row = StoredRow {
                            batch_id: restored_batch_id,
                            row: compacted_row,
                            logical_bytes: row_bytes.as_ref().expect("live row bytes")
                                [expected_row],
                        };
                        expected_row += 1;
                        (Some(identity), Some(row))
                    } else {
                        (None, None)
                    };
                    let key = Arc::<[u8]>::from(captured.key.as_slice());
                    keyed.gc.insert(captured.event_time, Arc::clone(&key));
                    keyed.slots.insert(
                        key,
                        KeyedSlot {
                            partition: Arc::from(captured.partition.as_slice()),
                            event_time: captured.event_time,
                            row_identity,
                            row,
                        },
                    );
                    previous_key = Some(captured.key.as_slice());
                }
                if expected_row != live_rows {
                    return Err(DbError::Checkpoint(
                        "normalizer compacted row count is inconsistent".into(),
                    ));
                }
                if let Some(batch) = compacted {
                    keyed.retained_batches.insert(
                        restored_batch_id,
                        RetainedBatch {
                            batch,
                            references: live_rows,
                        },
                    );
                }
                keyed.next_batch_id = *next_batch_id;
            }
            (ModeState::Full(full), BoundedJoinInputModeCheckpoint::Full { slots }) => {
                full.slots.try_reserve(slots.len()).map_err(|_| {
                    DbError::Checkpoint("normalizer full state is too large".into())
                })?;
                full.gc.try_reserve(slots.len(), state.config.vnode)?;
                let mut previous_row: Option<&[u8]> = None;
                for captured in slots {
                    if previous_row.is_some_and(|previous| previous >= captured.row.as_slice())
                        || captured.event_time < checkpoint.closed_cutoff
                        || captured.multiplicity < 0
                    {
                        return Err(DbError::Checkpoint(
                            "normalizer full slots are non-canonical or invalid".into(),
                        ));
                    }
                    let row = Arc::<[u8]>::from(captured.row.as_slice());
                    full.gc.insert(captured.event_time, Arc::clone(&row));
                    full.slots.insert(
                        row,
                        FullSlot {
                            partition: Arc::from(captured.partition.as_slice()),
                            event_time: captured.event_time,
                            multiplicity: captured.multiplicity,
                        },
                    );
                    previous_row = Some(captured.row.as_slice());
                }
            }
            _ => {
                return Err(DbError::Checkpoint(
                    "bounded join input checkpoint mode changed".into(),
                ));
            }
        }

        let mut logical_bytes = 0usize;
        for (partition, frontier) in &state.replay_frontiers {
            logical_bytes = logical_bytes
                .checked_add(frontier_charge(partition, frontier)?)
                .ok_or_else(|| {
                    DbError::Checkpoint("normalizer logical accounting overflow".into())
                })?;
        }
        match &state.mode {
            ModeState::AppendOnly => {}
            ModeState::Keyed(keyed) => {
                for (key, slot) in &keyed.slots {
                    logical_bytes = logical_bytes
                        .checked_add(keyed_slot_charge(key, slot)?)
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer logical accounting overflow".into())
                        })?;
                }
                for retained in keyed.retained_batches.values() {
                    logical_bytes = logical_bytes
                        .checked_add(retained_batch_charge(&retained.batch)?)
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer logical accounting overflow".into())
                        })?;
                }
            }
            ModeState::Full(full) => {
                for (row, slot) in &full.slots {
                    logical_bytes = logical_bytes
                        .checked_add(full_slot_charge(row, slot)?)
                        .ok_or_else(|| {
                            DbError::Checkpoint("normalizer logical accounting overflow".into())
                        })?;
                }
            }
        }
        state.logical_bytes = logical_bytes;
        state.refresh_capacity_charge()?;
        if state.accounted_state_bytes() > effective_limit {
            return Err(DbError::Checkpoint(format!(
                "bounded join input restored state accounts {} bytes; remaining limit is {effective_limit} bytes",
                state.accounted_state_bytes()
            )));
        }
        Ok(state)
    }

    /// Validate and prepare every routed batch for one source-side vnode cycle atomically.
    pub(crate) fn prepare_batches(
        &mut self,
        batches: &[RecordBatch],
        prior_own_cutoff: i64,
        post_cycle_own_close_cutoff: i64,
        dynamic_remaining_budget: usize,
    ) -> Result<PreparedBoundedJoinInput<'_>, DbError> {
        if prior_own_cutoff != self.closed_cutoff {
            return Err(self.terminal(format!(
                "caller prior cutoff {prior_own_cutoff} disagrees with retained cutoff {}",
                self.closed_cutoff
            )));
        }
        if post_cycle_own_close_cutoff < prior_own_cutoff {
            return Err(self.terminal(format!(
                "event-time cutoff regressed from {prior_own_cutoff} to {post_cycle_own_close_cutoff}"
            )));
        }
        let output_expansion = usize::from(matches!(&self.mode, ModeState::Keyed(_))) + 1;
        preflight_normalization_batches(batches, output_expansion, 0, self.config.vnode)?;
        if batches.is_empty() {
            return self.prepare_empty_cycle(post_cycle_own_close_cutoff, dynamic_remaining_budget);
        }
        let batch = if batches.len() == 1 {
            batches[0].clone()
        } else {
            self.concat_routed_batches(batches, output_expansion)?
        };
        self.prepare_combined_batch(
            &batch,
            prior_own_cutoff,
            post_cycle_own_close_cutoff,
            dynamic_remaining_budget,
        )
    }

    fn concat_routed_batches(
        &self,
        batches: &[RecordBatch],
        output_expansion: usize,
    ) -> Result<RecordBatch, DbError> {
        debug_assert!(batches.len() > 1);
        let routed_schema = batches[0].schema();
        if batches
            .iter()
            .skip(1)
            .all(|batch| batch.schema().as_ref() == routed_schema.as_ref())
        {
            return arrow::compute::concat_batches(&routed_schema, batches).map_err(|error| {
                DbError::query_pipeline_arrow("bounded join routed input concat", &error)
            });
        }
        if !matches!(self.mode, ModeState::Keyed(_)) {
            return Err(self.terminal("routed batches changed schema within one cycle"));
        }

        let mut saw_mutations = false;
        let mut saw_position_only = false;
        let mut promoted_rows = 0usize;
        let mut promoted_batches = 0usize;
        for batch in batches {
            source_row_positions(batch)
                .map_err(|error| self.terminal(format!("source positions are invalid: {error}")))?
                .ok_or_else(|| self.terminal("source positions are required"))?;
            if source_mutations_routed(batch)
                .map_err(|error| self.terminal(format!("source mutations are invalid: {error}")))?
                .is_some()
            {
                saw_mutations = true;
            } else {
                saw_position_only = true;
                promoted_rows = promoted_rows
                    .checked_add(batch.num_rows())
                    .ok_or_else(|| self.terminal("source mutation promotion row count overflow"))?;
                promoted_batches = promoted_batches.checked_add(1).ok_or_else(|| {
                    self.terminal("source mutation promotion batch count overflow")
                })?;
            }
        }
        if !saw_mutations || !saw_position_only {
            return Err(self.terminal("routed batches changed schema within one cycle"));
        }

        let promotion_scratch = keyed_promotion_scratch_bytes(
            self.input_schema.as_ref(),
            batches.len(),
            promoted_rows,
            promoted_batches,
            self.config.vnode,
        )?;
        preflight_normalization_batches(
            batches,
            output_expansion,
            promotion_scratch,
            self.config.vnode,
        )?;
        let positioned_schema =
            schema_with_source_row_positions(&self.input_schema).map_err(|error| {
                self.terminal(format!("positioned source schema is invalid: {error}"))
            })?;
        let mutation_schema = schema_with_source_mutations_and_row_positions(&self.input_schema)
            .map_err(|error| {
                self.terminal(format!("mutation source schema is invalid: {error}"))
            })?;
        let visible_columns = self.input_schema.fields().len();
        let mut canonical = Vec::new();
        canonical
            .try_reserve_exact(batches.len())
            .map_err(|_| self.terminal("canonical routed batch roster cannot be reserved"))?;
        for batch in batches {
            if batch.schema().as_ref() == mutation_schema.as_ref() {
                canonical.push(batch.clone());
                continue;
            }
            if batch.schema().as_ref() != positioned_schema.as_ref() {
                return Err(self.terminal("routed batches changed schema within one cycle"));
            }
            let mut columns = Vec::new();
            columns
                .try_reserve_exact(
                    batch
                        .num_columns()
                        .checked_add(1)
                        .ok_or_else(|| self.terminal("promoted source column count overflow"))?,
                )
                .map_err(|_| self.terminal("promoted source columns cannot be reserved"))?;
            columns.extend(batch.columns()[..visible_columns].iter().cloned());
            let mut puts = Vec::new();
            puts.try_reserve_exact(batch.num_rows())
                .map_err(|_| self.terminal("promoted source mutation values cannot be reserved"))?;
            puts.resize(batch.num_rows(), 0_u8);
            columns.push(Arc::new(UInt8Array::from(puts)) as ArrayRef);
            columns.extend(batch.columns()[visible_columns..].iter().cloned());
            canonical.push(
                RecordBatch::try_new(Arc::clone(&mutation_schema), columns).map_err(|error| {
                    DbError::query_pipeline_arrow("bounded join source mutation promotion", &error)
                })?,
            );
        }
        arrow::compute::concat_batches(&mutation_schema, &canonical).map_err(|error| {
            DbError::query_pipeline_arrow("bounded join routed input concat", &error)
        })
    }

    #[cfg(test)]
    fn prepare(
        &mut self,
        batch: RecordBatch,
        cutoff: i64,
    ) -> Result<PreparedBoundedJoinInput<'_>, DbError> {
        let prior = self.closed_cutoff;
        let limit = self.config.max_retained_bytes;
        self.prepare_batches(std::slice::from_ref(&batch), prior, cutoff, limit)
    }

    fn prepare_combined_batch(
        &mut self,
        batch: &RecordBatch,
        prior_cutoff: i64,
        post_cutoff: i64,
        dynamic_remaining_budget: usize,
    ) -> Result<PreparedBoundedJoinInput<'_>, DbError> {
        let positions = source_row_positions(batch)
            .map_err(|error| self.terminal(format!("source positions are invalid: {error}")))?
            .ok_or_else(|| self.terminal("source positions are required"))?;
        let mutations = source_mutations_routed(batch)
            .map_err(|error| self.terminal(format!("source mutations are invalid: {error}")))?;
        // Routed slices may retain an all-Put mutation array. Validate it with routed semantics,
        // borrow its operations above, then remove it before the strict position-only strip.
        let positioned_without_mutations = strip_source_mutations_routed(batch)
            .map_err(|error| self.terminal(format!("source mutations are invalid: {error}")))?;
        let visible_input = strip_source_row_positions(&positioned_without_mutations)
            .map_err(|error| self.terminal(format!("source metadata is invalid: {error}")))?;
        if visible_input.schema().as_ref() != self.input_schema.as_ref() {
            return Err(self.terminal("visible input schema changed while state was retained"));
        }
        let plain = match &self.mode {
            ModeState::AppendOnly => {
                if mutations.is_some() {
                    return Err(
                        self.terminal("append-only input must not carry source mutation metadata")
                    );
                }
                visible_input.clone()
            }
            ModeState::Keyed(_) => visible_input.clone(),
            ModeState::Full(_) => {
                if mutations.is_some() {
                    return Err(self.terminal(
                        "full-changelog input must use weights, not source mutation metadata",
                    ));
                }
                strip_weight(&visible_input)?
            }
        };
        if plain.schema().as_ref() != self.visible_schema.as_ref() {
            return Err(self.terminal("normalized visible schema changed while state was retained"));
        }
        if positions.len() != plain.num_rows() {
            return Err(self.terminal("source positions are not row-aligned"));
        }
        let plain = canonicalize_batch_schema(plain, &self.visible_schema)?;
        let input = Arc::new(plain);
        let input_row_bytes = logical_row_bytes(&input)?;
        let encoded_rows = self
            .row_codec
            .convert_columns(input.columns())
            .map_err(|error| self.terminal(format!("visible rows cannot be encoded: {error}")))?;
        let event_times = TimestampMillisView::try_new(
            input.column(self.config.event_time_index).as_ref(),
            "bounded join input",
        )
        .map_err(|error| self.terminal(error))?;

        let (output_rows, output_weights, commit) = match &self.mode {
            ModeState::AppendOnly => self.plan_append(
                &input,
                &input_row_bytes,
                &encoded_rows,
                positions,
                &event_times,
                prior_cutoff,
                post_cutoff,
            )?,
            ModeState::Keyed(keyed) => self.plan_keyed(
                keyed,
                &input,
                &input_row_bytes,
                &encoded_rows,
                positions,
                mutations,
                &event_times,
                prior_cutoff,
                post_cutoff,
            )?,
            ModeState::Full(full) => self.plan_full(
                full,
                &input,
                &input_row_bytes,
                &visible_input,
                &encoded_rows,
                positions,
                &event_times,
                prior_cutoff,
                post_cutoff,
            )?,
        };
        let output = build_output(
            &self.visible_schema,
            &self.output_schema,
            &output_rows,
            &output_weights,
        )?;

        let projected_state_bytes =
            self.preflight_and_reserve(&commit, dynamic_remaining_budget)?;
        let transient_state_bytes = self.accounted_state_bytes();
        Ok(PreparedBoundedJoinInput {
            state: self,
            output: vec![output],
            commit,
            projected_state_bytes,
            transient_state_bytes,
        })
    }

    fn prepare_empty_cycle(
        &mut self,
        post_cutoff: i64,
        dynamic_remaining_budget: usize,
    ) -> Result<PreparedBoundedJoinInput<'_>, DbError> {
        let frontier_updates = Vec::new();
        let (mode, logical_bytes) = match &self.mode {
            ModeState::AppendOnly => (ModeCommit::AppendOnly, self.logical_bytes),
            ModeState::Keyed(keyed) => {
                let evicted = keyed.gc.keys_before(post_cutoff, self.config.vnode)?;
                let (logical_bytes, released_batches) =
                    self.logical_after_keyed(keyed, &evicted, &[], None, &frontier_updates)?;
                (
                    ModeCommit::Keyed(KeyedCommit {
                        evicted,
                        updates: Vec::new(),
                        indexed_new: Vec::new(),
                        released_batches,
                        new_batch: None,
                    }),
                    logical_bytes,
                )
            }
            ModeState::Full(full) => {
                let evicted = full.gc.keys_before(post_cutoff, self.config.vnode)?;
                let logical_bytes =
                    self.logical_after_full(full, &evicted, &[], &frontier_updates)?;
                (
                    ModeCommit::Full(FullCommit {
                        evicted,
                        updates: Vec::new(),
                        indexed_new: Vec::new(),
                    }),
                    logical_bytes,
                )
            }
        };
        let commit = InputCommit {
            cutoff: post_cutoff,
            frontier_updates,
            mode,
            logical_bytes,
        };
        let projected_state_bytes =
            self.preflight_and_reserve(&commit, dynamic_remaining_budget)?;
        let transient_state_bytes = self.accounted_state_bytes();
        Ok(PreparedBoundedJoinInput {
            state: self,
            output: Vec::new(),
            commit,
            projected_state_bytes,
            transient_state_bytes,
        })
    }

    fn plan_append(
        &self,
        input: &Arc<RecordBatch>,
        input_row_bytes: &[usize],
        encoded_rows: &Rows,
        positions: laminar_connectors::connector::SourceRowPositionView<'_>,
        event_times: &TimestampMillisView<'_>,
        prior_cutoff: i64,
        post_cutoff: i64,
    ) -> Result<(Vec<OutputRowRef>, Vec<i64>, InputCommit), DbError> {
        let mut observed = FxHashMap::default();
        let mut staged_frontiers = FxHashMap::default();
        observed
            .try_reserve(input.num_rows())
            .map_err(|_| self.terminal("append replay roster cannot be reserved"))?;
        staged_frontiers
            .try_reserve(input.num_rows())
            .map_err(|_| self.terminal("append frontier roster cannot be reserved"))?;
        let mut output_rows = Vec::new();
        let mut output_weights = Vec::new();
        output_rows
            .try_reserve_exact(input.num_rows())
            .map_err(|_| self.terminal("append output row roster cannot be reserved"))?;
        output_weights
            .try_reserve_exact(input.num_rows())
            .map_err(|_| self.terminal("append output weight roster cannot be reserved"))?;
        for (row, logical_bytes) in input_row_bytes.iter().copied().enumerate() {
            let position = positions
                .get(row)
                .expect("validated source positions are row-aligned");
            let identity = ReplayIdentity {
                row: Arc::from(encoded_rows.row(row).data()),
                operation: ReplayOperation::Append,
            };
            if !admit_position(
                &self.replay_frontiers,
                &mut observed,
                &mut staged_frontiers,
                position,
                identity,
                self.config.vnode,
            )? {
                continue;
            }
            reject_late(
                event_time_value(event_times, row, self.config.vnode)?,
                prior_cutoff,
                self.config.vnode,
            )?;
            output_rows.push(OutputRowRef {
                batch: Arc::clone(input),
                row: row_index(row, self.config.vnode)?,
                logical_bytes,
            });
            output_weights.push(1);
        }
        let frontier_updates = staged_frontier_vec(staged_frontiers, self.config.vnode)?;
        let logical_bytes = self.logical_after_frontiers(&frontier_updates)?;
        Ok((
            output_rows,
            output_weights,
            InputCommit {
                cutoff: post_cutoff,
                frontier_updates,
                mode: ModeCommit::AppendOnly,
                logical_bytes,
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_keyed(
        &self,
        keyed: &KeyedState,
        input: &Arc<RecordBatch>,
        input_row_bytes: &[usize],
        encoded_rows: &Rows,
        positions: laminar_connectors::connector::SourceRowPositionView<'_>,
        mutations: Option<laminar_connectors::connector::SourceMutationView<'_>>,
        event_times: &TimestampMillisView<'_>,
        prior_cutoff: i64,
        post_cutoff: i64,
    ) -> Result<(Vec<OutputRowRef>, Vec<i64>, InputCommit), DbError> {
        let key_columns = keyed
            .primary_key_indices
            .iter()
            .map(|&index| input.column(index))
            .cloned()
            .collect::<Vec<ArrayRef>>();
        let primary_keys = keyed
            .primary_key_codec
            .convert_columns(&key_columns)
            .map_err(|error| self.terminal(format!("primary keys cannot be encoded: {error}")))?;
        let mut observed = FxHashMap::default();
        let mut staged_frontiers = FxHashMap::default();
        let mut staged_slots = FxHashMap::<Arc<[u8]>, PlannedKeyedSlot>::default();
        for (roster, what) in [
            (&mut observed, "keyed replay roster"),
            (&mut staged_frontiers, "keyed frontier roster"),
        ] {
            roster
                .try_reserve(input.num_rows())
                .map_err(|_| self.terminal(format!("{what} cannot be reserved")))?;
        }
        staged_slots
            .try_reserve(input.num_rows())
            .map_err(|_| self.terminal("keyed slot roster cannot be reserved"))?;
        let mut output_rows = Vec::new();
        let mut output_weights = Vec::new();
        let max_output = input
            .num_rows()
            .checked_mul(2)
            .ok_or_else(|| self.terminal("keyed-upsert output row count overflow"))?;
        output_rows
            .try_reserve_exact(max_output)
            .map_err(|_| self.terminal("keyed-upsert output row roster cannot be reserved"))?;
        output_weights
            .try_reserve_exact(max_output)
            .map_err(|_| self.terminal("keyed-upsert output weight roster cannot be reserved"))?;

        let evicted = keyed.gc.keys_before(post_cutoff, self.config.vnode)?;
        for row in 0..input.num_rows() {
            if key_columns.iter().any(|column| column.is_null(row)) {
                return Err(self.terminal(format!("primary key contains NULL at input row {row}")));
            }
            let position = positions
                .get(row)
                .expect("validated source positions are row-aligned");
            let operation = match mutations.and_then(|view| view.get(row)) {
                Some(SourceMutation::Tombstone) => ReplayOperation::Tombstone,
                Some(SourceMutation::Put) | None => ReplayOperation::Put,
            };
            let row_identity = Arc::<[u8]>::from(encoded_rows.row(row).data());
            let replay_identity = ReplayIdentity {
                row: Arc::clone(&row_identity),
                operation: operation.clone(),
            };
            if !admit_position(
                &self.replay_frontiers,
                &mut observed,
                &mut staged_frontiers,
                position,
                replay_identity,
                self.config.vnode,
            )? {
                continue;
            }
            let event_time = event_time_value(event_times, row, self.config.vnode)?;
            reject_late(event_time, prior_cutoff, self.config.vnode)?;
            let key = Arc::<[u8]>::from(primary_keys.row(row).data());
            let current = staged_slots.get(key.as_ref()).cloned().or_else(|| {
                keyed
                    .slots
                    .get(key.as_ref())
                    .filter(|slot| slot.event_time >= prior_cutoff)
                    .map(PlannedKeyedSlot::from)
            });
            if let Some(current) = &current {
                validate_affinity(
                    &current.partition,
                    position.partition,
                    "primary key",
                    self.config.vnode,
                )?;
                if current.event_time != event_time {
                    return Err(self.terminal(
                        "primary-key event time changed despite being part of the primary key",
                    ));
                }
            }
            let partition = current.as_ref().map_or_else(
                || Arc::from(position.partition),
                |slot| Arc::clone(&slot.partition),
            );
            match operation {
                ReplayOperation::Put => {
                    if current
                        .as_ref()
                        .and_then(|slot| slot.row_identity.as_deref())
                        != Some(row_identity.as_ref())
                    {
                        if let Some(old) = current.as_ref().and_then(|slot| slot.row) {
                            output_rows.push(resolve_output_row(
                                keyed,
                                input,
                                input_row_bytes,
                                old,
                            )?);
                            output_weights.push(-1);
                        }
                        output_rows.push(OutputRowRef {
                            batch: Arc::clone(input),
                            row: row_index(row, self.config.vnode)?,
                            logical_bytes: input_row_bytes[row],
                        });
                        output_weights.push(1);
                        staged_slots.insert(
                            key,
                            PlannedKeyedSlot {
                                partition,
                                event_time,
                                row_identity: Some(row_identity),
                                row: Some(PlannedRow::Input(row_index(row, self.config.vnode)?)),
                            },
                        );
                    } else if let Some(current) = current {
                        staged_slots.insert(key, current);
                    }
                }
                ReplayOperation::Tombstone => {
                    if let Some(old) = current.as_ref().and_then(|slot| slot.row) {
                        output_rows.push(resolve_output_row(keyed, input, input_row_bytes, old)?);
                        output_weights.push(-1);
                    }
                    staged_slots.insert(
                        key,
                        PlannedKeyedSlot {
                            partition,
                            event_time,
                            // The replay frontier owns exact last-row identity. A tombstone slot
                            // only fences partition affinity until its event-time cutoff.
                            row_identity: None,
                            row: None,
                        },
                    );
                }
                ReplayOperation::Append | ReplayOperation::Weight(_) => {
                    unreachable!("keyed operation is constructed above")
                }
            }
        }

        let frontier_updates = staged_frontier_vec(staged_frontiers, self.config.vnode)?;
        let mut planned_slots = Vec::new();
        planned_slots
            .try_reserve_exact(staged_slots.len())
            .map_err(|_| self.terminal("keyed commit roster cannot be reserved"))?;
        planned_slots.extend(
            staged_slots
                .into_iter()
                .filter(|(_, slot)| slot.event_time >= post_cutoff),
        );
        let (mode, logical_bytes) = self.finish_keyed_commit(
            keyed,
            input,
            input_row_bytes,
            evicted,
            planned_slots,
            &frontier_updates,
        )?;
        Ok((
            output_rows,
            output_weights,
            InputCommit {
                cutoff: post_cutoff,
                frontier_updates,
                mode,
                logical_bytes,
            },
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_full(
        &self,
        full: &FullState,
        input: &Arc<RecordBatch>,
        input_row_bytes: &[usize],
        weighted_input: &RecordBatch,
        encoded_rows: &Rows,
        positions: laminar_connectors::connector::SourceRowPositionView<'_>,
        event_times: &TimestampMillisView<'_>,
        prior_cutoff: i64,
        post_cutoff: i64,
    ) -> Result<(Vec<OutputRowRef>, Vec<i64>, InputCommit), DbError> {
        let weights = weight_values(weighted_input, self.config.vnode)?;
        let mut observed = FxHashMap::default();
        let mut staged_frontiers = FxHashMap::default();
        let mut staged_slots = FxHashMap::<Arc<[u8]>, FullSlot>::default();
        for (roster, what) in [
            (&mut observed, "full replay roster"),
            (&mut staged_frontiers, "full frontier roster"),
        ] {
            roster
                .try_reserve(input.num_rows())
                .map_err(|_| self.terminal(format!("{what} cannot be reserved")))?;
        }
        staged_slots
            .try_reserve(input.num_rows())
            .map_err(|_| self.terminal("full exact-row roster cannot be reserved"))?;
        let mut output_rows = Vec::new();
        let mut output_weights = Vec::new();
        output_rows
            .try_reserve_exact(input.num_rows())
            .map_err(|_| self.terminal("full-changelog output row roster cannot be reserved"))?;
        output_weights
            .try_reserve_exact(input.num_rows())
            .map_err(|_| self.terminal("full-changelog output weight roster cannot be reserved"))?;
        let evicted = full.gc.keys_before(post_cutoff, self.config.vnode)?;

        for (row, logical_bytes) in input_row_bytes.iter().copied().enumerate() {
            let weight = weights.value(row);
            if weight == 0 {
                return Err(self.terminal(format!("full-changelog row {row} has zero weight")));
            }
            let position = positions
                .get(row)
                .expect("validated source positions are row-aligned");
            let row_identity = Arc::<[u8]>::from(encoded_rows.row(row).data());
            if !admit_position(
                &self.replay_frontiers,
                &mut observed,
                &mut staged_frontiers,
                position,
                ReplayIdentity {
                    row: Arc::clone(&row_identity),
                    operation: ReplayOperation::Weight(weight),
                },
                self.config.vnode,
            )? {
                continue;
            }
            let event_time = event_time_value(event_times, row, self.config.vnode)?;
            reject_late(event_time, prior_cutoff, self.config.vnode)?;
            let current = staged_slots
                .get(row_identity.as_ref())
                .cloned()
                .or_else(|| {
                    full.slots
                        .get(row_identity.as_ref())
                        .filter(|slot| slot.event_time >= prior_cutoff)
                        .cloned()
                });
            if let Some(current) = &current {
                validate_affinity(
                    &current.partition,
                    position.partition,
                    "exact row",
                    self.config.vnode,
                )?;
                if current.event_time != event_time {
                    return Err(self.terminal("exact-row event time changed during normalization"));
                }
            }
            let previous = current.as_ref().map_or(0, |slot| slot.multiplicity);
            let next = previous.checked_add(weight).ok_or_else(|| {
                self.terminal(format!(
                    "full-changelog multiplicity overflow for input row {row}"
                ))
            })?;
            if next < 0 {
                return Err(self.terminal(format!(
                    "full-changelog multiplicity underflow for input row {row}"
                )));
            }
            let partition = current.as_ref().map_or_else(
                || Arc::from(position.partition),
                |slot| Arc::clone(&slot.partition),
            );
            staged_slots.insert(
                row_identity,
                FullSlot {
                    partition,
                    event_time,
                    multiplicity: next,
                },
            );
            output_rows.push(OutputRowRef {
                batch: Arc::clone(input),
                row: row_index(row, self.config.vnode)?,
                logical_bytes,
            });
            output_weights.push(weight);
        }

        let frontier_updates = staged_frontier_vec(staged_frontiers, self.config.vnode)?;
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(staged_slots.len())
            .map_err(|_| self.terminal("full commit roster cannot be reserved"))?;
        updates.extend(
            staged_slots
                .into_iter()
                .filter(|(_, slot)| slot.event_time >= post_cutoff),
        );
        let mut indexed_new = Vec::new();
        indexed_new
            .try_reserve_exact(updates.len())
            .map_err(|_| self.terminal("full cutoff-index roster cannot be reserved"))?;
        for (row, slot) in &updates {
            if !full.slots.contains_key(row.as_ref()) {
                indexed_new.push((slot.event_time, Arc::clone(row)));
            }
        }
        let logical_bytes = self.logical_after_full(full, &evicted, &updates, &frontier_updates)?;
        Ok((
            output_rows,
            output_weights,
            InputCommit {
                cutoff: post_cutoff,
                frontier_updates,
                mode: ModeCommit::Full(FullCommit {
                    evicted,
                    updates,
                    indexed_new,
                }),
                logical_bytes,
            },
        ))
    }

    fn finish_keyed_commit(
        &self,
        keyed: &KeyedState,
        input: &Arc<RecordBatch>,
        input_row_bytes: &[usize],
        evicted: Vec<Arc<[u8]>>,
        planned: Vec<(Arc<[u8]>, PlannedKeyedSlot)>,
        frontier_updates: &[(Arc<[u8]>, ReplayFrontier)],
    ) -> Result<(ModeCommit, usize), DbError> {
        let input_references = planned
            .iter()
            .filter(|(_, slot)| matches!(slot.row, Some(PlannedRow::Input(_))))
            .count();
        let new_batch = if input_references == 0 {
            None
        } else {
            let batch_id = keyed.next_batch_id;
            if batch_id == u64::MAX {
                return Err(self.terminal("retained batch identifier exhausted"));
            }
            Some((batch_id, Arc::clone(input)))
        };
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(planned.len())
            .map_err(|_| self.terminal("keyed-upsert commit roster cannot be reserved"))?;
        for (key, slot) in planned {
            let row = match slot.row {
                Some(PlannedRow::Stored(row)) => Some(row),
                Some(PlannedRow::Input(row)) => Some(StoredRow {
                    batch_id: new_batch
                        .as_ref()
                        .expect("input references require a retained batch")
                        .0,
                    row,
                    logical_bytes: input_row_bytes[row as usize],
                }),
                None => None,
            };
            updates.push((
                key,
                KeyedSlot {
                    partition: slot.partition,
                    event_time: slot.event_time,
                    row_identity: slot.row_identity,
                    row,
                },
            ));
        }
        let (logical_bytes, released_batches) = self.logical_after_keyed(
            keyed,
            &evicted,
            &updates,
            new_batch.as_ref(),
            frontier_updates,
        )?;
        let mut indexed_new = Vec::new();
        indexed_new
            .try_reserve_exact(updates.len())
            .map_err(|_| self.terminal("keyed cutoff-index roster cannot be reserved"))?;
        for (key, slot) in &updates {
            if !keyed.slots.contains_key(key.as_ref()) {
                indexed_new.push((slot.event_time, Arc::clone(key)));
            }
        }
        Ok((
            ModeCommit::Keyed(KeyedCommit {
                evicted,
                updates,
                indexed_new,
                released_batches,
                new_batch,
            }),
            logical_bytes,
        ))
    }

    fn logical_after_frontiers(
        &self,
        updates: &[(Arc<[u8]>, ReplayFrontier)],
    ) -> Result<usize, DbError> {
        let mut bytes = self.logical_bytes;
        for (partition, frontier) in updates {
            if let Some(previous) = self.replay_frontiers.get(partition.as_ref()) {
                bytes = checked_sub(
                    bytes,
                    frontier_charge(partition, previous)?,
                    self.config.vnode,
                )?;
            }
            bytes = checked_add(
                bytes,
                frontier_charge(partition, frontier)?,
                self.config.vnode,
            )?;
        }
        Ok(bytes)
    }

    fn logical_after_keyed(
        &self,
        keyed: &KeyedState,
        evicted: &[Arc<[u8]>],
        updates: &[(Arc<[u8]>, KeyedSlot)],
        new_batch: Option<&(u64, Arc<RecordBatch>)>,
        frontier_updates: &[(Arc<[u8]>, ReplayFrontier)],
    ) -> Result<(usize, Vec<u64>), DbError> {
        let mut bytes = self.logical_after_frontiers(frontier_updates)?;
        let mut batch_deltas = FxHashMap::<u64, isize>::default();
        let affected_rows = evicted
            .len()
            .checked_add(updates.len())
            .ok_or_else(|| self.terminal("retained batch delta roster overflow"))?;
        batch_deltas
            .try_reserve(affected_rows)
            .map_err(|_| self.terminal("retained batch delta roster cannot be reserved"))?;
        for key in evicted {
            if let Some(slot) = keyed.slots.get(key.as_ref()) {
                bytes = checked_sub(bytes, keyed_slot_charge(key, slot)?, self.config.vnode)?;
                add_row_delta(&mut batch_deltas, slot.row, -1, self.config.vnode)?;
            }
        }
        for (key, next) in updates {
            if let Some(previous) = keyed.slots.get(key.as_ref()) {
                bytes = checked_sub(bytes, keyed_slot_charge(key, previous)?, self.config.vnode)?;
                add_row_delta(&mut batch_deltas, previous.row, -1, self.config.vnode)?;
            }
            bytes = checked_add(bytes, keyed_slot_charge(key, next)?, self.config.vnode)?;
            add_row_delta(&mut batch_deltas, next.row, 1, self.config.vnode)?;
        }
        let mut released_batches = Vec::new();
        released_batches
            .try_reserve_exact(batch_deltas.len())
            .map_err(|_| self.terminal("retained batch release roster cannot be reserved"))?;
        for (batch_id, delta) in batch_deltas {
            if new_batch.is_some_and(|(new_id, _)| *new_id == batch_id) {
                continue;
            }
            let retained = keyed
                .retained_batches
                .get(&batch_id)
                .ok_or_else(|| self.terminal("keyed slot references an unknown retained batch"))?;
            let next = retained
                .references
                .checked_add_signed(delta)
                .ok_or_else(|| self.terminal("retained batch reference count underflow"))?;
            if next == 0 {
                bytes = checked_sub(
                    bytes,
                    retained_batch_charge(&retained.batch)?,
                    self.config.vnode,
                )?;
                released_batches.push(batch_id);
            }
        }
        if let Some((_, batch)) = new_batch {
            bytes = checked_add(bytes, retained_batch_charge(batch)?, self.config.vnode)?;
        }
        Ok((bytes, released_batches))
    }

    fn logical_after_full(
        &self,
        full: &FullState,
        evicted: &[Arc<[u8]>],
        updates: &[(Arc<[u8]>, FullSlot)],
        frontier_updates: &[(Arc<[u8]>, ReplayFrontier)],
    ) -> Result<usize, DbError> {
        let mut bytes = self.logical_after_frontiers(frontier_updates)?;
        for row in evicted {
            if let Some(slot) = full.slots.get(row.as_ref()) {
                bytes = checked_sub(bytes, full_slot_charge(row, slot)?, self.config.vnode)?;
            }
        }
        for (row, next) in updates {
            if let Some(previous) = full.slots.get(row.as_ref()) {
                bytes = checked_sub(bytes, full_slot_charge(row, previous)?, self.config.vnode)?;
            }
            bytes = checked_add(bytes, full_slot_charge(row, next)?, self.config.vnode)?;
        }
        Ok(bytes)
    }

    fn preflight_and_reserve(
        &mut self,
        commit: &InputCommit,
        dynamic_remaining_budget: usize,
    ) -> Result<usize, DbError> {
        let effective_limit = self.config.max_retained_bytes.min(dynamic_remaining_budget);
        let new_frontiers = commit
            .frontier_updates
            .iter()
            .filter(|(partition, _)| !self.replay_frontiers.contains_key(partition.as_ref()))
            .count();
        let predicted_frontier_capacity = predicted_capacity(
            self.replay_frontiers.capacity(),
            self.replay_frontiers.len(),
            new_frontiers,
            self.config.vnode,
        )?;
        let (predicted_slot_capacity, predicted_batch_capacity, predicted_gc_capacity) =
            match (&self.mode, &commit.mode) {
                (ModeState::AppendOnly, ModeCommit::AppendOnly) => (0, 0, 0),
                (ModeState::Keyed(state), ModeCommit::Keyed(commit)) => {
                    let net_new_slots = commit
                        .indexed_new
                        .len()
                        .saturating_sub(commit.evicted.len());
                    let net_new_batches = usize::from(commit.new_batch.is_some())
                        .saturating_sub(commit.released_batches.len());
                    (
                        predicted_capacity(
                            state.slots.capacity(),
                            state.slots.len(),
                            net_new_slots,
                            self.config.vnode,
                        )?,
                        predicted_capacity(
                            state.retained_batches.capacity(),
                            state.retained_batches.len(),
                            net_new_batches,
                            self.config.vnode,
                        )?,
                        predicted_vec_capacity(
                            state.gc.heap.capacity(),
                            state.gc.heap.len(),
                            net_new_slots,
                            self.config.vnode,
                        )?,
                    )
                }
                (ModeState::Full(state), ModeCommit::Full(commit)) => {
                    let net_new_slots = commit
                        .indexed_new
                        .len()
                        .saturating_sub(commit.evicted.len());
                    (
                        predicted_capacity(
                            state.slots.capacity(),
                            state.slots.len(),
                            net_new_slots,
                            self.config.vnode,
                        )?,
                        0,
                        predicted_vec_capacity(
                            state.gc.heap.capacity(),
                            state.gc.heap.len(),
                            net_new_slots,
                            self.config.vnode,
                        )?,
                    )
                }
                _ => return Err(self.terminal("prepared input mode disagrees with retained state")),
            };
        let predicted_capacity_bytes = capacity_charge(
            predicted_frontier_capacity,
            predicted_slot_capacity,
            predicted_batch_capacity,
            predicted_gc_capacity,
            self.config.vnode,
        )?;
        // A dropped Prepared value keeps reservations but publishes none of the removals that may
        // have lowered `commit.logical_bytes`. Budget the larger side of that atomic boundary.
        let reservation_logical_bytes = self.logical_bytes.max(commit.logical_bytes);
        let predicted_total = self
            .base_bytes
            .checked_add(reservation_logical_bytes)
            .and_then(|bytes| bytes.checked_add(predicted_capacity_bytes))
            .ok_or_else(|| self.terminal("retained-state accounting overflow"))?;
        if predicted_total > effective_limit {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "bounded join input vnode {} retained state",
                    self.config.vnode
                ),
                accounted_bytes: predicted_total,
                limit_bytes: effective_limit,
            });
        }

        let vnode = self.config.vnode;
        let reserve_result = (|| {
            self.replay_frontiers
                .try_reserve(new_frontiers)
                .map_err(|_| {
                    DbError::BackpressureFail(format!(
                        "bounded join input vnode {vnode} replay-frontier capacity cannot be reserved"
                    ))
                })?;
            match (&mut self.mode, &commit.mode) {
                (ModeState::AppendOnly, ModeCommit::AppendOnly) => {}
                (ModeState::Keyed(state), ModeCommit::Keyed(commit)) => {
                    let net_new_slots = commit
                        .indexed_new
                        .len()
                        .saturating_sub(commit.evicted.len());
                    let net_new_batches = usize::from(commit.new_batch.is_some())
                        .saturating_sub(commit.released_batches.len());
                    state
                        .slots
                        .try_reserve(net_new_slots)
                        .map_err(|_| DbError::BackpressureFail(format!(
                            "bounded join input vnode {vnode} keyed-slot capacity cannot be reserved"
                        )))?;
                    state
                        .retained_batches
                        .try_reserve(net_new_batches)
                        .map_err(|_| DbError::BackpressureFail(format!(
                            "bounded join input vnode {vnode} retained-batch capacity cannot be reserved"
                        )))?;
                    state.gc.try_reserve(net_new_slots, vnode)?;
                }
                (ModeState::Full(state), ModeCommit::Full(commit)) => {
                    let net_new_slots = commit
                        .indexed_new
                        .len()
                        .saturating_sub(commit.evicted.len());
                    state.slots.try_reserve(net_new_slots).map_err(|_| {
                        DbError::BackpressureFail(format!(
                            "bounded join input vnode {vnode} exact-row capacity cannot be reserved"
                        ))
                    })?;
                    state.gc.try_reserve(net_new_slots, vnode)?;
                }
                _ => {
                    return Err(terminal(
                        vnode,
                        "prepared input mode disagrees with retained state",
                    ));
                }
            }
            Ok(())
        })();
        self.refresh_capacity_charge()?;
        reserve_result?;
        debug_assert!(self.capacity_bytes <= predicted_capacity_bytes);
        let projected_state_bytes = self
            .base_bytes
            .checked_add(commit.logical_bytes)
            .and_then(|bytes| bytes.checked_add(self.capacity_bytes))
            .ok_or_else(|| self.terminal("projected retained-state accounting overflow"))?;
        if projected_state_bytes > effective_limit {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!(
                    "bounded join input vnode {} projected state",
                    self.config.vnode
                ),
                accounted_bytes: projected_state_bytes,
                limit_bytes: effective_limit,
            });
        }
        Ok(projected_state_bytes)
    }

    fn refresh_capacity_charge(&mut self) -> Result<(), DbError> {
        let (slot_capacity, batch_capacity, gc_capacity) = match &self.mode {
            ModeState::AppendOnly => (0, 0, 0),
            ModeState::Keyed(state) => (
                state.slots.capacity(),
                state.retained_batches.capacity(),
                state.gc.heap.capacity(),
            ),
            ModeState::Full(state) => (state.slots.capacity(), 0, state.gc.heap.capacity()),
        };
        self.capacity_bytes = capacity_charge(
            self.replay_frontiers.capacity(),
            slot_capacity,
            batch_capacity,
            gc_capacity,
            self.config.vnode,
        )?;
        self.charged_bytes = self
            .base_bytes
            .checked_add(self.logical_bytes)
            .and_then(|bytes| bytes.checked_add(self.capacity_bytes))
            .ok_or_else(|| self.terminal("retained-state accounting overflow"))?;
        if self.charged_bytes > self.config.max_retained_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("bounded join input vnode {} capacity", self.config.vnode),
                accounted_bytes: self.charged_bytes,
                limit_bytes: self.config.max_retained_bytes,
            });
        }
        Ok(())
    }

    fn apply_commit(&mut self, commit: InputCommit) {
        let cutoff = commit.cutoff;
        for (partition, frontier) in commit.frontier_updates {
            self.replay_frontiers.insert(partition, frontier);
        }
        match (&mut self.mode, commit.mode) {
            (ModeState::AppendOnly, ModeCommit::AppendOnly) => {}
            (ModeState::Keyed(state), ModeCommit::Keyed(commit)) => {
                let expected_evictions = commit.evicted.len();
                let mut removed = 0usize;
                while let Some((_, key)) = state.gc.pop_before(cutoff) {
                    remove_keyed_slot_without_batch_gc(state, key.as_ref());
                    removed += 1;
                }
                debug_assert_eq!(removed, expected_evictions);
                for (key, _) in &commit.updates {
                    remove_keyed_slot_without_batch_gc(state, key.as_ref());
                }
                // Release exact zero-ref batches first, so a one-for-one replacement cannot
                // transiently grow this map or require capacity beyond final net state.
                for batch_id in commit.released_batches {
                    let retained = state
                        .retained_batches
                        .remove(&batch_id)
                        .expect("prepared released batch exists");
                    debug_assert_eq!(retained.references, 0);
                }
                if let Some((batch_id, batch)) = commit.new_batch {
                    let replaced = state.retained_batches.insert(
                        batch_id,
                        RetainedBatch {
                            batch,
                            references: 0,
                        },
                    );
                    debug_assert!(replaced.is_none());
                    state.next_batch_id = batch_id + 1;
                }
                for (key, slot) in commit.updates {
                    if let Some(row) = slot.row {
                        let retained = state
                            .retained_batches
                            .get_mut(&row.batch_id)
                            .expect("prepared keyed row references a retained batch");
                        retained.references = retained
                            .references
                            .checked_add(1)
                            .expect("prepared retained reference count was validated");
                    }
                    let replaced = state.slots.insert(key, slot);
                    debug_assert!(replaced.is_none());
                }
                for (event_time, key) in commit.indexed_new {
                    state.gc.insert(event_time, key);
                }
            }
            (ModeState::Full(state), ModeCommit::Full(commit)) => {
                let expected_evictions = commit.evicted.len();
                let mut removed = 0usize;
                while let Some((_, row)) = state.gc.pop_before(cutoff) {
                    state.slots.remove(row.as_ref());
                    removed += 1;
                }
                debug_assert_eq!(removed, expected_evictions);
                for (row, slot) in commit.updates {
                    state.slots.insert(row, slot);
                }
                for (event_time, row) in commit.indexed_new {
                    state.gc.insert(event_time, row);
                }
            }
            _ => unreachable!("prepared mode was validated before reservation"),
        }
        self.closed_cutoff = cutoff;
        self.logical_bytes = commit.logical_bytes;
        self.charged_bytes = self
            .base_bytes
            .checked_add(self.logical_bytes)
            .and_then(|bytes| bytes.checked_add(self.capacity_bytes))
            .expect("prepared retained-state accounting was validated");
        debug_assert!(self.charged_bytes <= self.config.max_retained_bytes);
    }

    fn terminal(&self, detail: impl std::fmt::Display) -> DbError {
        DbError::PipelineTerminal(format!(
            "bounded join input vnode {}: {detail}",
            self.config.vnode
        ))
    }
}

fn admit_position(
    committed: &FxHashMap<Arc<[u8]>, ReplayFrontier>,
    observed: &mut FxHashMap<Arc<[u8]>, ReplayFrontier>,
    staged: &mut FxHashMap<Arc<[u8]>, ReplayFrontier>,
    position: SourceRowPositionRef<'_>,
    identity: ReplayIdentity,
    vnode: u32,
) -> Result<bool, DbError> {
    if let Some(previous) = observed.get(position.partition) {
        match previous.cursor.compare(position) {
            Ordering::Less => {
                return Err(terminal(
                    vnode,
                    "source positions regressed within one partition",
                ));
            }
            Ordering::Equal => {
                if previous.identity != identity {
                    return Err(terminal(
                        vnode,
                        "one source position carried divergent row bytes",
                    ));
                }
                return Ok(false);
            }
            Ordering::Greater => {}
        }
    }

    let partition = Arc::<[u8]>::from(position.partition);
    let frontier = ReplayFrontier {
        cursor: ReplayCursor {
            order: Arc::from(position.order_key),
            sub_offset: position.sub_offset,
        },
        identity,
    };
    observed.insert(Arc::clone(&partition), frontier.clone());
    if let Some(previous) = committed.get(position.partition) {
        match previous.cursor.compare(position) {
            Ordering::Less => return Ok(false),
            Ordering::Equal => {
                if previous.identity != frontier.identity {
                    return Err(terminal(
                        vnode,
                        "one source position was replayed with divergent row bytes",
                    ));
                }
                return Ok(false);
            }
            Ordering::Greater => {}
        }
    }
    staged.insert(partition, frontier);
    Ok(true)
}

fn staged_frontier_vec(
    staged: FxHashMap<Arc<[u8]>, ReplayFrontier>,
    vnode: u32,
) -> Result<Vec<ReplayFrontierUpdate>, DbError> {
    let mut frontiers = Vec::new();
    frontiers
        .try_reserve_exact(staged.len())
        .map_err(|_| terminal(vnode, "frontier commit roster cannot be reserved"))?;
    frontiers.extend(staged);
    Ok(frontiers)
}

fn resolve_output_row(
    keyed: &KeyedState,
    input: &Arc<RecordBatch>,
    input_row_bytes: &[usize],
    row: PlannedRow,
) -> Result<OutputRowRef, DbError> {
    match row {
        PlannedRow::Input(row) => Ok(OutputRowRef {
            batch: Arc::clone(input),
            row,
            logical_bytes: input_row_bytes[row as usize],
        }),
        PlannedRow::Stored(row) => {
            let retained = keyed.retained_batches.get(&row.batch_id).ok_or_else(|| {
                DbError::PipelineTerminal(
                    "bounded join keyed slot references an unknown retained batch".into(),
                )
            })?;
            if row.row as usize >= retained.batch.num_rows() {
                return Err(DbError::PipelineTerminal(
                    "bounded join retained row position is invalid".into(),
                ));
            }
            Ok(OutputRowRef {
                batch: Arc::clone(&retained.batch),
                row: row.row,
                logical_bytes: row.logical_bytes,
            })
        }
    }
}

fn remove_keyed_slot_without_batch_gc(state: &mut KeyedState, key: &[u8]) {
    let Some(slot) = state.slots.remove(key) else {
        return;
    };
    if let Some(row) = slot.row {
        let retained = state
            .retained_batches
            .get_mut(&row.batch_id)
            .expect("retained slot batch exists");
        retained.references = retained
            .references
            .checked_sub(1)
            .expect("retained slot reference count is positive");
    }
}

pub(crate) fn normalizer_config_fingerprint(
    input_schema: &Schema,
    event_time_index: usize,
    mode: &BoundedJoinInputMode,
) -> [u8; 32] {
    fn update_usize(hasher: &mut Sha256, value: usize) {
        hasher.update(u64::try_from(value).unwrap_or(u64::MAX).to_le_bytes());
    }

    let mut hasher = Sha256::new();
    hasher.update(b"laminar-bounded-join-input-v1");
    let encoded_schema = laminar_connectors::config::encode_arrow_schema_ipc(input_schema);
    update_usize(&mut hasher, encoded_schema.len());
    hasher.update(encoded_schema.as_bytes());
    update_usize(&mut hasher, event_time_index);
    match mode {
        BoundedJoinInputMode::AppendOnly => hasher.update([0]),
        BoundedJoinInputMode::KeyedUpsert {
            primary_key_indices,
        } => {
            hasher.update([1]);
            update_usize(&mut hasher, primary_key_indices.len());
            for &index in primary_key_indices {
                update_usize(&mut hasher, index);
            }
        }
        BoundedJoinInputMode::FullChangelog => hasher.update([2]),
    }
    hasher.finalize().into()
}

fn validate_no_weight(schema: &Schema, mode: &str) -> Result<(), DbError> {
    if schema
        .fields()
        .iter()
        .any(|field| field.name().eq_ignore_ascii_case(WEIGHT_COLUMN))
    {
        return Err(DbError::Config(format!(
            "bounded join {mode} input must not declare reserved {WEIGHT_COLUMN}"
        )));
    }
    Ok(())
}

fn validate_linear_row_encoding_schema(schema: &Schema) -> Result<(), DbError> {
    fn validate(data_type: &DataType) -> Result<(), DbError> {
        match data_type {
            // Mutable normalization is hard-bounded from the physical Arrow/IPC payload before
            // RowConverter runs. Aliasing/view encodings, nested child materialization, bit-packed
            // booleans, unions, and zero-width values do not have the required flat linear bound.
            DataType::Dictionary(_, _)
            | DataType::RunEndEncoded(_, _)
            | DataType::Utf8View
            | DataType::BinaryView
            | DataType::List(_)
            | DataType::LargeList(_)
            | DataType::ListView(_)
            | DataType::LargeListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::Map(_, _)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Boolean
            | DataType::Null => Err(DbError::Config(
                "bounded join mutable input rejects expansion-prone Arrow encodings".into(),
            )),
            DataType::FixedSizeBinary(width) if *width <= 0 => Err(DbError::Config(
                "bounded join mutable input rejects expansion-prone Arrow encodings".into(),
            )),
            _ => Ok(()),
        }
    }

    for field in schema.fields() {
        validate(field.data_type())?;
    }
    Ok(())
}

fn primary_key_validation_scratch_bytes(field_count: usize) -> Result<usize, DbError> {
    field_count
        .checked_add(usize::from(field_count != 0) * HEAP_ALLOCATION_CHARGE)
        .ok_or_else(|| DbError::Config("bounded join primary-key validation overflow".into()))
}

fn validate_primary_key_indices_unique(
    primary_key_indices: &[usize],
    field_count: usize,
) -> Result<usize, DbError> {
    let mut seen = Vec::<u8>::new();
    seen.try_reserve_exact(field_count)
        .map_err(|_| DbError::Config("bounded join primary key is too wide".into()))?;
    seen.resize(field_count, 0);
    for &index in primary_key_indices {
        let marker = seen.get_mut(index).ok_or_else(|| {
            DbError::Config("bounded join keyed primary-key contract is invalid".into())
        })?;
        if *marker != 0 {
            return Err(DbError::Config(
                "bounded join keyed primary-key contract is invalid".into(),
            ));
        }
        *marker = 1;
    }
    primary_key_validation_scratch_bytes(seen.capacity())
}

fn full_weight_index(schema: &Schema) -> Result<usize, DbError> {
    let mut index = None;
    for (candidate, field) in schema.fields().iter().enumerate() {
        if field.name().eq_ignore_ascii_case(WEIGHT_COLUMN) && index.replace(candidate).is_some() {
            return Err(DbError::Config(format!(
                "bounded join full-changelog input requires a sole trailing {WEIGHT_COLUMN}"
            )));
        }
    }
    let index = index.ok_or_else(|| {
        DbError::Config(format!(
            "bounded join full-changelog input requires a sole trailing {WEIGHT_COLUMN}"
        ))
    })?;
    if index + 1 != schema.fields().len() {
        return Err(DbError::Config(format!(
            "bounded join full-changelog input requires a sole trailing {WEIGHT_COLUMN}"
        )));
    }
    let field = schema.field(index);
    if field.name() != WEIGHT_COLUMN || field.data_type() != &DataType::Int64 || field.is_nullable()
    {
        return Err(DbError::Config(format!(
            "bounded join full-changelog {WEIGHT_COLUMN} must be exact non-null Int64"
        )));
    }
    Ok(index)
}

fn validate_and_strip_weight_schema(schema: &SchemaRef) -> Result<SchemaRef, DbError> {
    let index = full_weight_index(schema)?;
    Ok(Arc::new(Schema::new_with_metadata(
        schema.fields()[..index].to_vec(),
        schema.metadata().clone(),
    )))
}

fn validate_event_time(schema: &Schema, index: usize) -> Result<(), DbError> {
    let field = schema.fields().get(index).ok_or_else(|| {
        DbError::Config("bounded join event-time index is outside the visible schema".into())
    })?;
    if field.is_nullable() || !matches!(field.data_type(), DataType::Timestamp(_, _)) {
        return Err(DbError::Config(
            "bounded join event time must be a non-null timestamp".into(),
        ));
    }
    Ok(())
}

fn row_codec(schema: &Schema) -> Result<RowConverter, DbError> {
    RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(|error| {
        DbError::Config(format!(
            "bounded join input rows cannot be deterministically encoded: {error}"
        ))
    })
}

fn construction_charge(
    input_schema: &SchemaRef,
    visible_schema: &SchemaRef,
    output_schema: &SchemaRef,
    row_codec: &RowConverter,
    mode: &ModeState,
    config_mode: &BoundedJoinInputMode,
) -> Result<usize, DbError> {
    let mut bytes = BASE_STATE_CHARGE;
    // Charge each schema ownership independently. Shared Field Arcs are deliberately overcounted,
    // while cloned field rosters and arbitrary schema metadata remain bounded.
    for schema in [input_schema, visible_schema, output_schema] {
        bytes = bytes.checked_add(schema_charge(schema)?).ok_or_else(|| {
            DbError::Config("bounded join construction accounting overflow".into())
        })?;
    }
    bytes = bytes
        .checked_add(row_codec.size())
        .ok_or_else(|| DbError::Config("bounded join row-codec accounting overflow".into()))?;
    match (mode, config_mode) {
        (
            ModeState::Keyed(state),
            BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices,
            },
        ) => {
            let config_indices = primary_key_indices
                .capacity()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| DbError::Config("bounded join PK accounting overflow".into()))?;
            let state_indices = state
                .primary_key_indices
                .capacity()
                .checked_mul(std::mem::size_of::<usize>())
                .ok_or_else(|| DbError::Config("bounded join PK accounting overflow".into()))?;
            bytes = bytes
                .checked_add(config_indices)
                .and_then(|bytes| bytes.checked_add(state_indices))
                .and_then(|bytes| bytes.checked_add(state.primary_key_codec.size()))
                .ok_or_else(|| DbError::Config("bounded join PK accounting overflow".into()))?;
        }
        (ModeState::AppendOnly, BoundedJoinInputMode::AppendOnly)
        | (ModeState::Full(_), BoundedJoinInputMode::FullChangelog) => {}
        _ => {
            return Err(DbError::Config(
                "bounded join input mode construction disagrees with its config".into(),
            ));
        }
    }
    Ok(bytes)
}

fn schema_charge(schema: &Schema) -> Result<usize, DbError> {
    let fields = schema.fields().iter().try_fold(0usize, |bytes, field| {
        field
            .size()
            // Arrow's size includes String headers but not hash-table control storage in field
            // metadata (including nested fields). Doubling conservatively covers that gap.
            .checked_mul(2)
            .ok_or_else(|| DbError::Config("bounded join schema accounting overflow".into()))?
            .checked_add(std::mem::size_of::<Arc<Field>>())
            .and_then(|field| bytes.checked_add(field))
            .ok_or_else(|| DbError::Config("bounded join schema accounting overflow".into()))
    })?;
    let metadata = schema
        .metadata()
        .capacity()
        // Include hash buckets/control bytes, not only the two String headers.
        .checked_mul(HASH_CAPACITY_CHARGE)
        .and_then(|bytes| {
            schema
                .metadata()
                .iter()
                .try_fold(bytes, |bytes, (key, value)| {
                    bytes
                        .checked_add(key.capacity())
                        .and_then(|bytes| bytes.checked_add(value.capacity()))
                })
        })
        .ok_or_else(|| {
            DbError::Config("bounded join schema metadata accounting overflow".into())
        })?;
    std::mem::size_of::<Schema>()
        .checked_add(fields)
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or_else(|| DbError::Config("bounded join schema accounting overflow".into()))
}

fn weighted_schema(schema: &Schema) -> SchemaRef {
    let mut fields = schema.fields().to_vec();
    fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

fn strip_weight(batch: &RecordBatch) -> Result<RecordBatch, DbError> {
    let index = batch.num_columns().checked_sub(1).ok_or_else(|| {
        DbError::PipelineTerminal("bounded join full-changelog input has no weight column".into())
    })?;
    let schema = validate_and_strip_weight_schema(&batch.schema())?;
    RecordBatch::try_new(schema, batch.columns()[..index].to_vec())
        .map_err(|error| DbError::query_pipeline_arrow("bounded join input strip weight", &error))
}

fn weight_values(batch: &RecordBatch, vnode: u32) -> Result<&Int64Array, DbError> {
    let index = batch
        .num_columns()
        .checked_sub(1)
        .ok_or_else(|| terminal(vnode, "full-changelog input is missing its weight column"))?;
    let weights = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| terminal(vnode, "full-changelog weight array is not Int64"))?;
    if weights.null_count() != 0 {
        return Err(terminal(vnode, "full-changelog weight array contains NULL"));
    }
    Ok(weights)
}

#[cfg(test)]
fn preflight_normalization_scratch(
    batch: &RecordBatch,
    output_expansion: usize,
    vnode: u32,
) -> Result<(), DbError> {
    preflight_normalization_scratch_parts(
        batch.num_rows(),
        batch.num_columns(),
        batch.num_columns(),
        batch.get_array_memory_size(),
        output_expansion,
        vnode,
    )
}

fn preflight_normalization_batches(
    batches: &[RecordBatch],
    output_expansion: usize,
    additional_scratch: usize,
    vnode: u32,
) -> Result<(), DbError> {
    let (rows, columns, max_columns, routed_array_bytes) = batches.iter().try_fold(
        (0usize, 0usize, 0usize, 0usize),
        |(rows, columns, max_columns, bytes), batch| {
            rows.checked_add(batch.num_rows())
                .zip(columns.checked_add(batch.num_columns()))
                .map(|(rows, columns)| (rows, columns, max_columns.max(batch.num_columns())))
                .zip(bytes.checked_add(batch.get_array_memory_size()))
                .map(|((rows, columns, max_columns), bytes)| (rows, columns, max_columns, bytes))
                .ok_or_else(|| terminal(vnode, "routed batch scratch accounting overflow"))
        },
    )?;
    preflight_normalization_scratch_parts_with_extra(
        rows,
        columns,
        max_columns,
        routed_array_bytes,
        output_expansion,
        additional_scratch,
        vnode,
    )
}

#[cfg(test)]
fn preflight_normalization_scratch_parts(
    rows: usize,
    columns: usize,
    max_columns: usize,
    routed_array_bytes: usize,
    output_expansion: usize,
    vnode: u32,
) -> Result<(), DbError> {
    preflight_normalization_scratch_parts_with_extra(
        rows,
        columns,
        max_columns,
        routed_array_bytes,
        output_expansion,
        0,
        vnode,
    )
}

fn preflight_normalization_scratch_parts_with_extra(
    rows: usize,
    columns: usize,
    max_columns: usize,
    routed_array_bytes: usize,
    output_expansion: usize,
    additional_scratch: usize,
    vnode: u32,
) -> Result<(), DbError> {
    let projected_rows = rows
        .checked_mul(output_expansion)
        .ok_or_else(|| terminal(vnode, "normalized output row count overflow"))?;
    if projected_rows > MAX_CYCLE_OUTPUT_ROWS {
        return Err(DbError::BackpressureFail(format!(
            "bounded join input vnode {vnode} could expand beyond {MAX_CYCLE_OUTPUT_ROWS} rows"
        )));
    }
    let cell_scratch = rows
        .checked_mul(max_columns)
        .and_then(|cells| cells.checked_mul(NORMALIZATION_CELL_SCRATCH_CHARGE))
        .ok_or_else(|| terminal(vnode, "normalization cell scratch accounting overflow"))?;
    let row_scratch = rows
        .checked_mul(NORMALIZATION_ROW_SCRATCH_CHARGE)
        .and_then(|bytes| bytes.checked_add(cell_scratch))
        .ok_or_else(|| terminal(vnode, "normalization row scratch accounting overflow"))?;
    let payload_scratch = routed_array_bytes
        .checked_mul(4)
        .ok_or_else(|| terminal(vnode, "normalization payload accounting overflow"))?;
    let column_scratch = columns
        .checked_mul(std::mem::size_of::<ArrayRef>())
        .and_then(|bytes| bytes.checked_add(HEAP_ALLOCATION_CHARGE))
        .ok_or_else(|| terminal(vnode, "normalization column accounting overflow"))?;
    let scratch = row_scratch
        .checked_add(payload_scratch)
        .and_then(|bytes| bytes.checked_add(column_scratch))
        .and_then(|bytes| bytes.checked_add(additional_scratch))
        .ok_or_else(|| terminal(vnode, "normalization scratch accounting overflow"))?;
    if scratch > MAX_CYCLE_OUTPUT_BYTES {
        return Err(DbError::BackpressureFail(format!(
            "bounded join input vnode {vnode} normalization scratch would exceed {} MiB",
            MAX_CYCLE_OUTPUT_BYTES / (1024 * 1024)
        )));
    }
    Ok(())
}

fn keyed_promotion_scratch_bytes(
    input_schema: &Schema,
    batch_count: usize,
    promoted_rows: usize,
    promoted_batches: usize,
    vnode: u32,
) -> Result<usize, DbError> {
    let canonical_columns = input_schema
        .fields()
        .len()
        .checked_add(4)
        .ok_or_else(|| terminal(vnode, "source mutation promotion column count overflow"))?;
    let batch_roster = batch_count
        .checked_mul(std::mem::size_of::<RecordBatch>())
        .and_then(|bytes| bytes.checked_add(HEAP_ALLOCATION_CHARGE))
        .ok_or_else(|| terminal(vnode, "source mutation promotion roster overflow"))?;
    let column_allocation_charges = batch_count
        .checked_mul(HEAP_ALLOCATION_CHARGE)
        .ok_or_else(|| terminal(vnode, "source mutation promotion column roster overflow"))?;
    let column_rosters = batch_count
        .checked_mul(canonical_columns)
        .and_then(|columns| columns.checked_mul(std::mem::size_of::<ArrayRef>()))
        .and_then(|bytes| bytes.checked_add(column_allocation_charges))
        .ok_or_else(|| terminal(vnode, "source mutation promotion column roster overflow"))?;
    let mutation_allocation_charge = HEAP_ALLOCATION_CHARGE
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<UInt8Array>()))
        .ok_or_else(|| terminal(vnode, "source mutation promotion allocation overflow"))?;
    let mutation_arrays = promoted_rows
        .checked_add(
            promoted_batches
                .checked_mul(mutation_allocation_charge)
                .ok_or_else(|| terminal(vnode, "source mutation promotion allocation overflow"))?,
        )
        .ok_or_else(|| terminal(vnode, "source mutation promotion payload overflow"))?;
    let schema_scratch = schema_charge(input_schema)?
        .checked_mul(4)
        .ok_or_else(|| terminal(vnode, "source mutation promotion schema overflow"))?;
    batch_roster
        .checked_add(column_rosters)
        .and_then(|bytes| bytes.checked_add(mutation_arrays))
        .and_then(|bytes| bytes.checked_add(schema_scratch))
        .ok_or_else(|| terminal(vnode, "source mutation promotion scratch overflow"))
}

pub(super) fn preflight_normalizer_ipc_restore(
    bytes: &[u8],
) -> Result<NormalizerIpcRestorePreflight, DbError> {
    preflight_single_batch_ipc_restore(bytes, false, "normalizer current-image")
}

#[cfg(feature = "cluster")]
pub(super) fn preflight_queued_batch_ipc_restore(
    bytes: &[u8],
) -> Result<NormalizerIpcRestorePreflight, DbError> {
    preflight_single_batch_ipc_restore(bytes, true, "interval join queued-data")
}

fn preflight_single_batch_ipc_restore(
    bytes: &[u8],
    allow_dictionary_batches: bool,
    context: &'static str,
) -> Result<NormalizerIpcRestorePreflight, DbError> {
    const CONTINUATION: u32 = u32::MAX;

    let mut offset = 0usize;
    let mut saw_schema = false;
    let mut rows = None;
    let mut body_bytes = 0usize;
    loop {
        let prefix_end = offset
            .checked_add(4)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC framing overflow")))?;
        let prefix = bytes
            .get(offset..prefix_end)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC frame is truncated")))?;
        offset = prefix_end;
        let mut metadata_len = u32::from_le_bytes(prefix.try_into().expect("four-byte prefix"));
        if metadata_len == CONTINUATION {
            let length_end = offset
                .checked_add(4)
                .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC framing overflow")))?;
            let length = bytes.get(offset..length_end).ok_or_else(|| {
                DbError::Checkpoint(format!("{context} IPC continuation is truncated"))
            })?;
            offset = length_end;
            metadata_len = u32::from_le_bytes(length.try_into().expect("four-byte length"));
        }
        if metadata_len == 0 {
            if offset != bytes.len() || !saw_schema || rows.is_none() {
                return Err(DbError::Checkpoint(format!(
                    "{context} IPC stream is non-canonical"
                )));
            }
            break;
        }
        let metadata_len = usize::try_from(metadata_len).map_err(|_| {
            DbError::Checkpoint(format!("{context} IPC metadata length exceeds usize"))
        })?;
        let metadata_end = offset.checked_add(metadata_len).ok_or_else(|| {
            DbError::Checkpoint(format!("{context} IPC metadata framing overflow"))
        })?;
        let metadata = bytes
            .get(offset..metadata_end)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC metadata is truncated")))?;
        offset = metadata_end;
        let message = arrow_ipc::root_as_message(metadata).map_err(|error| {
            DbError::Checkpoint(format!("{context} IPC metadata is invalid: {error}"))
        })?;
        let body_len = usize::try_from(message.bodyLength()).map_err(|_| {
            DbError::Checkpoint(format!(
                "{context} IPC body length is negative or too large"
            ))
        })?;
        let body_end = offset
            .checked_add(body_len)
            .ok_or_else(|| DbError::Checkpoint(format!("{context} IPC body framing overflow")))?;
        if body_end > bytes.len() {
            return Err(DbError::Checkpoint(format!(
                "{context} IPC body is truncated"
            )));
        }
        body_bytes = body_bytes.checked_add(body_len).ok_or_else(|| {
            DbError::Checkpoint(format!("{context} IPC body accounting overflow"))
        })?;
        match message.header_type() {
            arrow_ipc::MessageHeader::Schema if !saw_schema && rows.is_none() => {
                saw_schema = true;
            }
            // Mutable normalizer construction rejects dictionary schemas. Legacy append-only
            // channel batches may contain dictionaries, but only in the canonical pre-record
            // position and without compression.
            arrow_ipc::MessageHeader::DictionaryBatch if saw_schema && rows.is_none() => {
                if !allow_dictionary_batches {
                    return Err(DbError::Checkpoint(format!(
                        "{context} IPC dictionary data is unsupported"
                    )));
                }
                let dictionary = message.header_as_dictionary_batch().ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC dictionary-batch header is missing"))
                })?;
                let data = dictionary.data().ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC dictionary-batch data is missing"))
                })?;
                usize::try_from(data.length()).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "{context} IPC dictionary-batch length is negative or too large"
                    ))
                })?;
                if data.compression().is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "{context} IPC compression is unsupported"
                    )));
                }
            }
            arrow_ipc::MessageHeader::RecordBatch if saw_schema && rows.is_none() => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    DbError::Checkpoint(format!("{context} IPC record-batch header is missing"))
                })?;
                if batch.compression().is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "{context} IPC compression is unsupported"
                    )));
                }
                rows = Some(usize::try_from(batch.length()).map_err(|_| {
                    DbError::Checkpoint(format!(
                        "{context} IPC record-batch length is negative or too large"
                    ))
                })?);
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "{context} IPC message order is non-canonical"
                )));
            }
        }
        offset = body_end;
    }
    Ok(NormalizerIpcRestorePreflight {
        rows: rows.expect("canonical stream has one record batch"),
        body_bytes,
    })
}

fn canonicalize_batch_schema(
    batch: RecordBatch,
    schema: &SchemaRef,
) -> Result<RecordBatch, DbError> {
    if Arc::ptr_eq(&batch.schema(), schema) {
        return Ok(batch);
    }
    batch.with_schema(Arc::clone(schema)).map_err(|error| {
        DbError::query_pipeline_arrow("bounded join input canonical schema", &error)
    })
}

fn build_output(
    visible_schema: &SchemaRef,
    output_schema: &SchemaRef,
    rows: &[OutputRowRef],
    weights: &[i64],
) -> Result<RecordBatch, DbError> {
    if rows.len() != weights.len() {
        return Err(DbError::PipelineTerminal(
            "bounded join normalized row and weight rosters disagree".into(),
        ));
    }
    if rows.len() > MAX_CYCLE_OUTPUT_ROWS {
        return Err(DbError::BackpressureFail(format!(
            "bounded join normalized input exceeds {MAX_CYCLE_OUTPUT_ROWS} rows"
        )));
    }
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
    }
    let payload_bytes = rows.iter().try_fold(0usize, |bytes, row| {
        bytes.checked_add(row.logical_bytes).ok_or_else(|| {
            DbError::BackpressureFail("bounded join output byte accounting overflow".into())
        })
    })?;
    let weight_bytes = rows
        .len()
        .checked_mul(std::mem::size_of::<i64>())
        .and_then(|bytes| bytes.checked_add(rows.len().saturating_add(7) / 8))
        .ok_or_else(|| {
            DbError::BackpressureFail("bounded join weight accounting overflow".into())
        })?;
    let logical_bytes = payload_bytes.checked_add(weight_bytes).ok_or_else(|| {
        DbError::BackpressureFail("bounded join output byte accounting overflow".into())
    })?;
    let allocation_charge = logical_bytes
        .checked_mul(2)
        .and_then(|bytes| {
            rows.len()
                .checked_mul(output_schema.fields().len())
                .and_then(|positions| positions.checked_mul(16))
                .and_then(|positions| bytes.checked_add(positions))
        })
        .ok_or_else(|| {
            DbError::BackpressureFail("bounded join output allocation accounting overflow".into())
        })?;
    if allocation_charge > MAX_CYCLE_OUTPUT_BYTES {
        return Err(DbError::BackpressureFail(format!(
            "bounded join normalized input would exceed {} MiB",
            MAX_CYCLE_OUTPUT_BYTES / (1024 * 1024)
        )));
    }
    let mut batches = Vec::<Arc<RecordBatch>>::new();
    let mut by_pointer = FxHashMap::<usize, usize>::default();
    let mut indices = Vec::new();
    batches.try_reserve(rows.len()).map_err(|_| {
        DbError::BackpressureFail("bounded join output batch roster cannot be reserved".into())
    })?;
    by_pointer.try_reserve(rows.len()).map_err(|_| {
        DbError::BackpressureFail("bounded join output batch index cannot be reserved".into())
    })?;
    indices.try_reserve_exact(rows.len()).map_err(|_| {
        DbError::BackpressureFail("bounded join output positions cannot be reserved".into())
    })?;
    for row in rows {
        if row.row as usize >= row.batch.num_rows() {
            return Err(DbError::PipelineTerminal(
                "bounded join normalized output position is invalid".into(),
            ));
        }
        let pointer = Arc::as_ptr(&row.batch) as usize;
        let batch_index = *by_pointer.entry(pointer).or_insert_with(|| {
            let index = batches.len();
            batches.push(Arc::clone(&row.batch));
            index
        });
        indices.push((batch_index, row.row as usize));
    }
    let mut columns = Vec::<ArrayRef>::new();
    columns
        .try_reserve_exact(output_schema.fields().len())
        .map_err(|_| DbError::BackpressureFail("bounded join output is too wide".into()))?;
    for column in 0..visible_schema.fields().len() {
        let arrays = batches
            .iter()
            .map(|batch| batch.column(column).as_ref())
            .collect::<Vec<&dyn Array>>();
        columns.push(
            arrow::compute::interleave(&arrays, &indices).map_err(|error| {
                DbError::query_pipeline_arrow("bounded join input interleave", &error)
            })?,
        );
    }
    columns.push(Arc::new(Int64Array::from_iter_values(
        weights.iter().copied(),
    )));
    let output = RecordBatch::try_new(Arc::clone(output_schema), columns)
        .map_err(|error| DbError::query_pipeline_arrow("bounded join normalized input", &error))?;
    if output.get_array_memory_size() > MAX_CYCLE_OUTPUT_BYTES {
        return Err(DbError::BackpressureFail(format!(
            "bounded join normalized input exceeded {} MiB",
            MAX_CYCLE_OUTPUT_BYTES / (1024 * 1024)
        )));
    }
    Ok(output)
}

fn validate_affinity(
    expected: &[u8],
    actual: &[u8],
    identity: &str,
    vnode: u32,
) -> Result<(), DbError> {
    if expected == actual {
        Ok(())
    } else {
        Err(terminal(
            vnode,
            format!("{identity} moved between source partitions before its cutoff"),
        ))
    }
}

fn reject_late(event_time: i64, cutoff: i64, vnode: u32) -> Result<(), DbError> {
    if event_time < cutoff {
        Err(terminal(
            vnode,
            format!("received event time {event_time} below closed cutoff {cutoff}"),
        ))
    } else {
        Ok(())
    }
}

fn event_time_value(
    event_times: &TimestampMillisView<'_>,
    row: usize,
    vnode: u32,
) -> Result<i64, DbError> {
    event_times
        .value(row, "bounded join input")
        .map_err(|error| terminal(vnode, error))
}

fn row_index(row: usize, vnode: u32) -> Result<u32, DbError> {
    u32::try_from(row).map_err(|_| terminal(vnode, "input batch exceeds the supported row count"))
}

fn terminal(vnode: u32, detail: impl std::fmt::Display) -> DbError {
    DbError::PipelineTerminal(format!("bounded join input vnode {vnode}: {detail}"))
}

fn add_row_delta(
    deltas: &mut FxHashMap<u64, isize>,
    row: Option<StoredRow>,
    delta: isize,
    vnode: u32,
) -> Result<(), DbError> {
    let Some(row) = row else {
        return Ok(());
    };
    let current = deltas.entry(row.batch_id).or_default();
    *current = current
        .checked_add(delta)
        .ok_or_else(|| terminal(vnode, "retained batch reference delta overflow"))?;
    Ok(())
}

fn frontier_charge(partition: &[u8], frontier: &ReplayFrontier) -> Result<usize, DbError> {
    FRONTIER_ENTRY_CHARGE
        .checked_add(partition.len())
        .and_then(|bytes| bytes.checked_add(frontier.cursor.order.len()))
        .and_then(|bytes| bytes.checked_add(frontier.identity.row.len()))
        .ok_or_else(|| DbError::PipelineTerminal("replay-frontier accounting overflow".into()))
}

fn keyed_slot_charge(key: &[u8], slot: &KeyedSlot) -> Result<usize, DbError> {
    SLOT_ENTRY_CHARGE
        .checked_add(key.len())
        .and_then(|bytes| bytes.checked_add(slot.partition.len()))
        .and_then(|bytes| bytes.checked_add(slot.row_identity.as_ref().map_or(0, |row| row.len())))
        .ok_or_else(|| DbError::PipelineTerminal("keyed-slot accounting overflow".into()))
}

fn full_slot_charge(row: &[u8], slot: &FullSlot) -> Result<usize, DbError> {
    SLOT_ENTRY_CHARGE
        .checked_add(row.len())
        .and_then(|bytes| bytes.checked_add(slot.partition.len()))
        .ok_or_else(|| DbError::PipelineTerminal("exact-row slot accounting overflow".into()))
}

fn retained_batch_charge(batch: &RecordBatch) -> Result<usize, DbError> {
    RETAINED_BATCH_CHARGE
        .checked_add(batch.get_array_memory_size())
        .and_then(|bytes| {
            batch
                .num_columns()
                .checked_mul(std::mem::size_of::<ArrayRef>())
                .and_then(|columns| bytes.checked_add(columns))
        })
        .ok_or_else(|| DbError::PipelineTerminal("retained batch accounting overflow".into()))
}

fn checked_add(current: usize, charge: usize, vnode: u32) -> Result<usize, DbError> {
    current
        .checked_add(charge)
        .ok_or_else(|| terminal(vnode, "retained-state accounting overflow"))
}

fn checked_sub(current: usize, charge: usize, vnode: u32) -> Result<usize, DbError> {
    current
        .checked_sub(charge)
        .ok_or_else(|| terminal(vnode, "retained-state accounting underflow"))
}

fn predicted_capacity(
    current_capacity: usize,
    len: usize,
    additional: usize,
    vnode: u32,
) -> Result<usize, DbError> {
    let required = len
        .checked_add(additional)
        .ok_or_else(|| terminal(vnode, "hash-map capacity overflow"))?;
    if required <= current_capacity {
        return Ok(current_capacity);
    }
    required
        .checked_next_power_of_two()
        .and_then(|capacity| capacity.checked_mul(4))
        .ok_or_else(|| terminal(vnode, "hash-map capacity accounting overflow"))
}

fn predicted_vec_capacity(
    current_capacity: usize,
    len: usize,
    additional: usize,
    vnode: u32,
) -> Result<usize, DbError> {
    let required = len
        .checked_add(additional)
        .ok_or_else(|| terminal(vnode, "vector capacity overflow"))?;
    if required <= current_capacity {
        return Ok(current_capacity);
    }
    required
        .checked_next_power_of_two()
        .and_then(|capacity| capacity.checked_mul(4))
        .ok_or_else(|| terminal(vnode, "vector capacity accounting overflow"))
}

fn capacity_charge(
    frontier_capacity: usize,
    slot_capacity: usize,
    batch_capacity: usize,
    gc_capacity: usize,
    vnode: u32,
) -> Result<usize, DbError> {
    frontier_capacity
        .checked_add(slot_capacity)
        .and_then(|capacity| capacity.checked_add(batch_capacity))
        .and_then(|capacity| capacity.checked_mul(HASH_CAPACITY_CHARGE))
        .and_then(|bytes| {
            gc_capacity
                .checked_mul(std::mem::size_of::<GcEntry>())
                .and_then(|heap| bytes.checked_add(heap))
        })
        .ok_or_else(|| terminal(vnode, "retained capacity accounting overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryArray, StringArray, TimestampMillisecondArray, UInt32Array};
    use arrow::datatypes::{TimeUnit, UnionFields, UnionMode};
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceBatch, SourceRowPositionCapability, SourceRowPositions,
    };

    fn plain_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn plain_batch(ids: &[Option<&str>], times: &[i64], values: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            plain_schema(),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn expansion_prone_schemas_are_rejected_before_state_or_restore() {
        let item = Arc::new(Field::new("item", DataType::Int64, true));
        let rejected = [
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::clone(&item),
            ),
            DataType::Utf8View,
            DataType::BinaryView,
            DataType::List(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
            DataType::ListView(Arc::clone(&item)),
            DataType::LargeListView(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 2),
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int64, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
            DataType::Union(
                UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
                UnionMode::Sparse,
            ),
            DataType::Union(
                UnionFields::try_new([0], [Field::new("item", DataType::Int64, true)]).unwrap(),
                UnionMode::Dense,
            ),
            DataType::Boolean,
            DataType::Null,
            DataType::FixedSizeBinary(-1),
            DataType::FixedSizeBinary(0),
        ];
        let mode = BoundedJoinInputMode::KeyedUpsert {
            primary_key_indices: vec![0, 1],
        };
        for data_type in rejected {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", data_type, false),
                Field::new(
                    "event_time",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
                Field::new("value", DataType::Int64, false),
            ]));
            let config = BoundedJoinInputConfig {
                vnode: 0,
                event_time_index: 1,
                mode: mode.clone(),
                max_retained_bytes: usize::MAX,
            };
            let error = match BoundedJoinInputNormalizer::try_new(schema, config) {
                Ok(_) => panic!("expansion-prone schema must be rejected before construction"),
                Err(error) => error,
            };
            assert!(error.to_string().contains("expansion-prone"));
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "id",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
        let config = BoundedJoinInputConfig {
            vnode: 0,
            event_time_index: 1,
            mode: mode.clone(),
            max_retained_bytes: usize::MAX,
        };
        let checkpoint = BoundedJoinInputCheckpoint {
            version: NORMALIZER_CHECKPOINT_VERSION,
            config_fingerprint: normalizer_config_fingerprint(schema.as_ref(), 1, &mode),
            closed_cutoff: i64::MIN,
            replay_frontiers: Vec::new(),
            mode: BoundedJoinInputModeCheckpoint::Keyed {
                next_batch_id: 1,
                slots: Vec::new(),
                compacted_rows_ipc: Vec::new(),
            },
        };
        let error = match BoundedJoinInputNormalizer::from_checkpoint(
            &checkpoint,
            schema,
            config,
            usize::MAX,
        ) {
            Ok(_) => panic!("dictionary schema must be rejected before checkpoint restore"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("expansion-prone"));
    }

    #[test]
    fn duplicate_primary_key_indices_are_rejected_before_row_converter_construction() {
        let error = match BoundedJoinInputNormalizer::try_new(
            plain_schema(),
            BoundedJoinInputConfig {
                vnode: 0,
                event_time_index: 1,
                mode: BoundedJoinInputMode::KeyedUpsert {
                    primary_key_indices: vec![0, 1, 1],
                },
                max_retained_bytes: usize::MAX,
            },
        ) {
            Ok(_) => panic!("duplicate primary-key columns must be rejected before construction"),
            Err(error) => error,
        };
        assert!(error
            .to_string()
            .contains("primary-key contract is invalid"));

        BoundedJoinInputNormalizer::try_new(
            plain_schema(),
            BoundedJoinInputConfig {
                vnode: 0,
                event_time_index: 1,
                mode: BoundedJoinInputMode::KeyedUpsert {
                    primary_key_indices: vec![1, 0],
                },
                max_retained_bytes: usize::MAX,
            },
        )
        .expect("declared primary-key order is semantic and need not be sorted");
    }

    fn weighted_batch(
        ids: &[Option<&str>],
        times: &[i64],
        values: &[i64],
        weights: &[i64],
    ) -> RecordBatch {
        let mut fields = plain_schema().fields().to_vec();
        fields.push(Arc::new(Field::new(WEIGHT_COLUMN, DataType::Int64, false)));
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(times.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
                Arc::new(Int64Array::from(weights.to_vec())),
            ],
        )
        .unwrap()
    }

    fn positioned(
        batch: RecordBatch,
        partitions: &[&[u8]],
        orders: &[u64],
        mutations: Option<Vec<SourceMutation>>,
    ) -> RecordBatch {
        assert_eq!(partitions.len(), batch.num_rows());
        assert_eq!(orders.len(), batch.num_rows());
        let order_bytes = orders
            .iter()
            .map(|order| order.to_be_bytes())
            .collect::<Vec<_>>();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from_iter_values(partitions.iter().copied()),
            BinaryArray::from_iter_values(order_bytes.iter()),
            UInt32Array::from_iter_values(std::iter::repeat_n(0, batch.num_rows())),
        )
        .unwrap();
        let visible_schema = batch.schema();
        let positioned_schema = schema_with_source_row_positions(&visible_schema).unwrap();
        let mutation_schema =
            schema_with_source_mutations_and_row_positions(&visible_schema).unwrap();
        let source = SourceBatch::positioned(batch, positions).unwrap();
        let source = if let Some(mutations) = mutations {
            source.with_mutations(mutations).unwrap()
        } else {
            source
        };
        source
            .into_records_with_metadata(
                SourceRowPositionCapability::OrderedDeterministic,
                &positioned_schema,
                &mutation_schema,
            )
            .unwrap()
    }

    fn keyed_normalizer() -> BoundedJoinInputNormalizer {
        BoundedJoinInputNormalizer::try_new(
            plain_schema(),
            BoundedJoinInputConfig {
                vnode: 7,
                event_time_index: 1,
                mode: BoundedJoinInputMode::KeyedUpsert {
                    primary_key_indices: vec![0, 1],
                },
                max_retained_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    fn full_normalizer() -> BoundedJoinInputNormalizer {
        BoundedJoinInputNormalizer::try_new(
            weighted_batch(&[], &[], &[], &[]).schema(),
            BoundedJoinInputConfig {
                vnode: 9,
                event_time_index: 1,
                mode: BoundedJoinInputMode::FullChangelog,
                max_retained_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    fn append_normalizer() -> BoundedJoinInputNormalizer {
        BoundedJoinInputNormalizer::try_new(
            plain_schema(),
            BoundedJoinInputConfig {
                vnode: 5,
                event_time_index: 1,
                mode: BoundedJoinInputMode::AppendOnly,
                max_retained_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap()
    }

    fn output_weights(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column_by_name(WEIGHT_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn output_values(batch: &RecordBatch) -> Vec<i64> {
        batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn assert_gc_consistent(state: &BoundedJoinInputNormalizer) {
        match &state.mode {
            ModeState::AppendOnly => return,
            ModeState::Keyed(keyed) => assert_gc_index(&keyed.gc, keyed.slots.len(), |key| {
                keyed.slots.contains_key(key)
            }),
            ModeState::Full(full) => assert_gc_index(&full.gc, full.slots.len(), |row| {
                full.slots.contains_key(row)
            }),
        }
    }

    fn assert_gc_index(gc: &ExactGcIndex, slot_count: usize, contains: impl Fn(&[u8]) -> bool) {
        assert_eq!(gc.heap.len(), slot_count);
        for Reverse((_, identity)) in &gc.heap {
            assert!(contains(identity.as_ref()));
        }
    }

    #[test]
    fn construction_accounts_dynamic_schema_metadata_and_normalization_preflights_rows() {
        let baseline = append_normalizer().accounted_state_bytes();
        let metadata_value = "x".repeat(16 * 1024);
        let schema = Arc::new(Schema::new_with_metadata(
            plain_schema().fields().to_vec(),
            std::collections::HashMap::from([("large".into(), metadata_value.clone())]),
        ));
        let state = BoundedJoinInputNormalizer::try_new(
            schema,
            BoundedJoinInputConfig {
                vnode: 11,
                event_time_index: 1,
                mode: BoundedJoinInputMode::AppendOnly,
                max_retained_bytes: 16 * 1024 * 1024,
            },
        )
        .unwrap();
        assert!(state.accounted_state_bytes() >= baseline + metadata_value.len());

        let rows = MAX_CYCLE_OUTPUT_ROWS / 2 + 1;
        let oversized = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::new_null(rows))],
        )
        .unwrap();
        assert!(preflight_normalization_scratch(&oversized, 2, 11)
            .unwrap_err()
            .to_string()
            .contains("could expand beyond"));
    }

    #[test]
    fn normalization_preflight_counts_hidden_source_coordinate_buffers() {
        preflight_normalization_scratch_parts(1, 3, 3, 0, 1, 11).unwrap();
        let oversized_coordinates = MAX_CYCLE_OUTPUT_BYTES / 4 + 1;
        let error = preflight_normalization_scratch_parts(1, 6, 6, oversized_coordinates, 1, 11)
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("normalization scratch would exceed"),
            "unexpected error: {error}"
        );

        let wide_flat =
            preflight_normalization_scratch_parts(16 * 1024, 512, 512, 8 * 1024 * 1024, 1, 11)
                .unwrap_err()
                .to_string();
        assert!(
            wide_flat.contains("normalization scratch would exceed"),
            "unexpected error: {wide_flat}"
        );
    }

    #[test]
    fn output_payload_is_preflighted_before_roster_allocation_or_row_access() {
        let visible_schema = plain_schema();
        let output_schema = weighted_schema(visible_schema.as_ref());
        let batch = Arc::new(plain_batch(&[Some("A")], &[100], &[10]));
        let rows = [OutputRowRef {
            batch,
            row: u32::MAX,
            logical_bytes: MAX_CYCLE_OUTPUT_BYTES,
        }];
        let error = build_output(&visible_schema, &output_schema, &rows, &[1])
            .unwrap_err()
            .to_string();
        assert!(error.contains("would exceed"), "unexpected error: {error}");
    }

    #[test]
    fn keyed_accepts_routed_all_put_metadata() {
        let mut state = keyed_normalizer();
        let mixed = positioned(
            plain_batch(&[Some("A"), Some("B")], &[100, 100], &[10, 20]),
            &[b"partition-0", b"partition-0"],
            &[1, 2],
            Some(vec![SourceMutation::Put, SourceMutation::Tombstone]),
        );
        let routed_put = mixed.slice(0, 1);
        let prepared = state.prepare(routed_put, i64::MIN).unwrap();
        assert_eq!(output_values(prepared.output()), vec![10]);
        assert_eq!(output_weights(prepared.output()), vec![1]);
        prepared.commit();
    }

    #[test]
    fn keyed_prefix_and_retained_batch_references_are_exact() {
        let mut state = keyed_normalizer();
        let initial = positioned(
            plain_batch(
                &[Some("A"), Some("B"), Some("A")],
                &[100, 100, 100],
                &[10, 20, 11],
            ),
            &[b"partition-0", b"partition-0", b"partition-0"],
            &[1, 2, 3],
            None,
        );
        let prepared = state.prepare(initial, i64::MIN).unwrap();
        assert_eq!(output_values(prepared.output()), vec![10, 20, 10, 11]);
        assert_eq!(output_weights(prepared.output()), vec![1, 1, -1, 1]);
        prepared.commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.retained_batches.len(), 1);
        assert_eq!(
            keyed.retained_batches.values().next().unwrap().references,
            2
        );
        assert!(keyed
            .slots
            .values()
            .filter_map(|slot| slot.row)
            .all(|row| row.logical_bytes != 0));
        assert!(Arc::ptr_eq(
            &keyed
                .retained_batches
                .values()
                .next()
                .unwrap()
                .batch
                .schema(),
            &state.visible_schema
        ));

        state
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[12]),
                    &[b"partition-0"],
                    &[4],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.retained_batches.len(), 2);
        assert!(keyed
            .retained_batches
            .values()
            .all(|retained| retained.references == 1));

        state
            .prepare(
                positioned(
                    plain_batch(&[Some("B")], &[100], &[999]),
                    &[b"partition-0"],
                    &[5],
                    Some(vec![SourceMutation::Tombstone]),
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.retained_batches.len(), 1);
        assert_eq!(
            keyed.retained_batches.values().next().unwrap().references,
            1
        );
        assert_gc_consistent(&state);
    }

    #[test]
    fn full_changelog_applies_same_batch_prefix_arithmetic() {
        let mut state = full_normalizer();
        let prepared = state
            .prepare(
                positioned(
                    weighted_batch(&[Some("A"), Some("A")], &[100, 100], &[10, 10], &[2, -1]),
                    &[b"partition-0", b"partition-0"],
                    &[1, 2],
                    None,
                ),
                i64::MIN,
            )
            .unwrap();
        assert_eq!(output_weights(prepared.output()), vec![2, -1]);
        prepared.commit();
        let ModeState::Full(full) = &state.mode else {
            unreachable!()
        };
        assert_eq!(full.slots.values().next().unwrap().multiplicity, 1);
        assert_gc_consistent(&state);
    }

    #[test]
    fn keyed_replay_differential_and_partition_affinity_are_exact() {
        let mut state = keyed_normalizer();
        let first = positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[1],
            None,
        );
        let prepared = state.prepare(first.clone(), i64::MIN).unwrap();
        assert_eq!(output_weights(prepared.output()), vec![1]);
        prepared.commit();

        let replay = state.prepare(first, i64::MIN).unwrap();
        assert_eq!(replay.output().num_rows(), 0);
        replay.commit();

        let divergent = positioned(
            plain_batch(&[Some("A")], &[100], &[11]),
            &[b"partition-0"],
            &[1],
            None,
        );
        assert!(state
            .prepare(divergent, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("divergent row bytes"));

        let update = positioned(
            plain_batch(&[Some("A")], &[100], &[20]),
            &[b"partition-0"],
            &[2],
            None,
        );
        let prepared = state.prepare(update, i64::MIN).unwrap();
        assert_eq!(output_values(prepared.output()), vec![10, 20]);
        assert_eq!(output_weights(prepared.output()), vec![-1, 1]);
        prepared.commit();
        assert_gc_consistent(&state);

        let older = positioned(
            plain_batch(&[Some("A")], &[100], &[777]),
            &[b"partition-0"],
            &[1],
            None,
        );
        let prepared = state.prepare(older, i64::MIN).unwrap();
        assert_eq!(prepared.output().num_rows(), 0);
        prepared.commit();

        let moved = positioned(
            plain_batch(&[Some("A")], &[100], &[30]),
            &[b"partition-1"],
            &[1],
            None,
        );
        assert!(state
            .prepare(moved, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("moved between source partitions"));
    }

    #[test]
    fn keyed_multi_batch_overlay_rejects_cross_batch_regression_and_affinity_atomically() {
        let mut state = keyed_normalizer();
        let regression = [
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[10],
                None,
            ),
            positioned(
                plain_batch(&[Some("A")], &[100], &[9]),
                &[b"partition-0"],
                &[9],
                None,
            ),
        ];
        let error = state
            .prepare_batches(&regression, i64::MIN, i64::MIN, usize::MAX)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("regressed within one partition"));
        assert!(state.replay_frontiers.is_empty());
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert!(keyed.slots.is_empty());

        let moved = [
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            positioned(
                plain_batch(&[Some("A")], &[100], &[20]),
                &[b"partition-1"],
                &[2],
                None,
            ),
        ];
        let error = state
            .prepare_batches(&moved, i64::MIN, i64::MIN, usize::MAX)
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("moved between source partitions"));
        assert!(state.replay_frontiers.is_empty());
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert!(keyed.slots.is_empty());

        let valid = [
            positioned(
                plain_batch(&[Some("A")], &[100], &[10]),
                &[b"partition-0"],
                &[1],
                None,
            ),
            positioned(
                plain_batch(&[Some("A")], &[100], &[20]),
                &[b"partition-0"],
                &[2],
                Some(vec![SourceMutation::Tombstone]),
            ),
        ];
        let prepared = state
            .prepare_batches(&valid, i64::MIN, i64::MIN, usize::MAX)
            .unwrap();
        assert_eq!(output_values(prepared.output()), vec![10, 10]);
        assert_eq!(output_weights(prepared.output()), vec![1, -1]);
        prepared.commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.slots.len(), 1);
        assert!(keyed.slots.values().all(|slot| slot.row.is_none()));
        assert!(keyed.retained_batches.is_empty());
    }

    #[test]
    fn keyed_split_cutoff_emits_old_image_before_post_cycle_gc() {
        let mut state = keyed_normalizer();
        state
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[10]),
                    &[b"partition-0"],
                    &[1],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let update = [positioned(
            plain_batch(&[Some("A")], &[100], &[20]),
            &[b"partition-0"],
            &[2],
            None,
        )];
        let prepared = state
            .prepare_batches(&update, i64::MIN, 101, usize::MAX)
            .unwrap();
        assert_eq!(output_values(prepared.output()), vec![10, 20]);
        assert_eq!(output_weights(prepared.output()), vec![-1, 1]);
        prepared.commit();
        assert_eq!(state.closed_cutoff(), 101);
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert!(keyed.slots.is_empty());
        assert!(keyed.retained_batches.is_empty());
        assert!(keyed.gc.heap.is_empty());
        assert_eq!(state.replay_frontiers.len(), 1);
    }

    #[test]
    fn replay_regression_is_terminal_even_when_every_row_is_older() {
        let mut state = keyed_normalizer();
        state
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[10]),
                    &[b"partition-0"],
                    &[10],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let regressed = positioned(
            plain_batch(&[Some("A"), Some("A")], &[100, 100], &[8, 7]),
            &[b"partition-0", b"partition-0"],
            &[8, 7],
            None,
        );
        assert!(state
            .prepare(regressed, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("regressed within one partition"));
    }

    #[test]
    fn keyed_tombstones_keep_zero_affinity_without_arrow_payload() {
        let mut state = keyed_normalizer();
        state
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[10]),
                    &[b"partition-0"],
                    &[1],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let tombstone = positioned(
            plain_batch(&[Some("A")], &[100], &[999]),
            &[b"partition-0"],
            &[2],
            Some(vec![SourceMutation::Tombstone]),
        );
        let prepared = state.prepare(tombstone, i64::MIN).unwrap();
        assert_eq!(output_values(prepared.output()), vec![10]);
        assert_eq!(output_weights(prepared.output()), vec![-1]);
        prepared.commit();

        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.slots.len(), 1);
        assert!(keyed.slots.values().all(|slot| slot.row.is_none()));
        assert!(keyed.retained_batches.is_empty());
        assert_eq!(keyed.gc.heap.len(), 1);
        assert_gc_consistent(&state);

        let absent = positioned(
            plain_batch(&[Some("B")], &[100], &[1]),
            &[b"partition-1"],
            &[1],
            Some(vec![SourceMutation::Tombstone]),
        );
        let prepared = state.prepare(absent, i64::MIN).unwrap();
        assert_eq!(prepared.output().num_rows(), 0);
        prepared.commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.slots.len(), 2);
        assert!(keyed.retained_batches.is_empty());
        assert_eq!(keyed.gc.heap.len(), 2);
        assert_gc_consistent(&state);
    }

    #[test]
    fn keyed_primary_keys_are_runtime_non_null() {
        let mut state = keyed_normalizer();
        let null_key = positioned(
            plain_batch(&[None], &[100], &[10]),
            &[b"partition-0"],
            &[1],
            None,
        );
        assert!(state
            .prepare(null_key, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("primary key contains NULL"));
    }

    #[test]
    fn full_changelog_checks_multiplicity_and_partition_affinity() {
        let mut state = full_normalizer();
        state
            .prepare(
                positioned(
                    weighted_batch(&[Some("A")], &[100], &[10], &[2]),
                    &[b"partition-0"],
                    &[1],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let divergent_weight = positioned(
            weighted_batch(&[Some("A")], &[100], &[10], &[3]),
            &[b"partition-0"],
            &[1],
            None,
        );
        assert!(state
            .prepare(divergent_weight, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("divergent row bytes"));
        let to_zero = positioned(
            weighted_batch(&[Some("A")], &[100], &[10], &[-2]),
            &[b"partition-0"],
            &[2],
            None,
        );
        let prepared = state.prepare(to_zero, i64::MIN).unwrap();
        assert_eq!(output_weights(prepared.output()), vec![-2]);
        prepared.commit();
        let ModeState::Full(full) = &state.mode else {
            unreachable!()
        };
        assert_eq!(full.slots.values().next().unwrap().multiplicity, 0);
        assert_eq!(full.gc.heap.len(), 1);
        assert_gc_consistent(&state);

        let moved = positioned(
            weighted_batch(&[Some("A")], &[100], &[10], &[1]),
            &[b"partition-1"],
            &[1],
            None,
        );
        assert!(state
            .prepare(moved, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("moved between source partitions"));

        let underflow = positioned(
            weighted_batch(&[Some("A")], &[100], &[10], &[-1]),
            &[b"partition-0"],
            &[3],
            None,
        );
        assert!(state
            .prepare(underflow, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("multiplicity underflow"));
    }

    #[test]
    fn full_changelog_overflow_and_zero_weight_are_terminal() {
        let mut overflow = full_normalizer();
        overflow
            .prepare(
                positioned(
                    weighted_batch(&[Some("A")], &[100], &[10], &[i64::MAX]),
                    &[b"partition-0"],
                    &[1],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let plus_one = positioned(
            weighted_batch(&[Some("A")], &[100], &[10], &[1]),
            &[b"partition-0"],
            &[2],
            None,
        );
        assert!(overflow
            .prepare(plus_one, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("multiplicity overflow"));

        let mut zero = full_normalizer();
        let zero_weight = positioned(
            weighted_batch(&[Some("A")], &[100], &[10], &[0]),
            &[b"partition-0"],
            &[1],
            None,
        );
        assert!(zero
            .prepare(zero_weight, i64::MIN)
            .err()
            .unwrap()
            .to_string()
            .contains("zero weight"));
    }

    #[test]
    fn cutoff_evicts_slots_but_keeps_replay_frontiers() {
        let mut state = keyed_normalizer();
        state
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[10]),
                    &[b"partition-0"],
                    &[1],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        state
            .prepare(
                positioned(
                    plain_batch(&[Some("Z")], &[300], &[30]),
                    &[b"partition-0"],
                    &[2],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        let capacities = (
            keyed.slots.capacity(),
            keyed.retained_batches.capacity(),
            keyed.gc.heap.capacity(),
        );

        let replacement = positioned(
            plain_batch(&[Some("B")], &[200], &[20]),
            &[b"partition-0"],
            &[3],
            None,
        );
        let prepared = state.prepare(replacement, 101).unwrap();
        assert_eq!(output_values(prepared.output()), vec![20]);
        assert_eq!(output_weights(prepared.output()), vec![1]);
        prepared.commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.slots.len(), 2);
        assert_eq!(keyed.retained_batches.len(), 2);
        assert!(keyed
            .retained_batches
            .values()
            .all(|retained| retained.references == 1));
        assert_eq!(keyed.gc.heap.len(), 2);
        assert_eq!(
            (
                keyed.slots.capacity(),
                keyed.retained_batches.capacity(),
                keyed.gc.heap.capacity(),
            ),
            capacities,
            "one cutoff eviction plus one insertion must reserve only final net growth"
        );
        assert_gc_consistent(&state);
        assert_eq!(state.replay_frontiers.len(), 1);

        let replay = state
            .prepare(
                positioned(
                    plain_batch(&[Some("A"), Some("Z")], &[100, 300], &[10, 30]),
                    &[b"partition-0", b"partition-0"],
                    &[1, 2],
                    None,
                ),
                101,
            )
            .unwrap();
        assert_eq!(replay.output().num_rows(), 0);
        replay.commit();
        let late = positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[4],
            None,
        );
        assert!(state
            .prepare(late, 101)
            .err()
            .unwrap()
            .to_string()
            .contains("below closed cutoff"));

        let empty = positioned(plain_batch(&[], &[], &[]), &[], &[], None);
        state.prepare(empty, 301).unwrap().commit();
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert!(keyed.slots.is_empty());
        assert!(keyed.retained_batches.is_empty());
        assert!(keyed.gc.heap.is_empty());
        assert_gc_consistent(&state);
    }

    #[test]
    fn dropping_prepared_input_is_logically_atomic_and_keeps_capacity_charged() {
        let mut state = keyed_normalizer();
        let update = positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[1],
            None,
        );
        let before = state.accounted_state_bytes();
        {
            let prepared = state.prepare(update.clone(), i64::MIN).unwrap();
            assert_eq!(output_weights(prepared.output()), vec![1]);
        }
        assert!(state.accounted_state_bytes() >= before);
        assert!(state.replay_frontiers.is_empty());
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert!(keyed.slots.is_empty());
        assert!(keyed.gc.heap.is_empty());
        assert!(keyed.gc.heap.capacity() > 0);

        let prepared = state.prepare(update, i64::MIN).unwrap();
        assert_eq!(output_weights(prepared.output()), vec![1]);
        prepared.commit();
        assert_eq!(state.replay_frontiers.len(), 1);

        let empty = positioned(plain_batch(&[], &[], &[]), &[], &[], None);
        {
            let prepared = state.prepare(empty, 101).unwrap();
            assert_eq!(prepared.output().num_rows(), 0);
        }
        assert_eq!(state.closed_cutoff, i64::MIN);
        let ModeState::Keyed(keyed) = &state.mode else {
            unreachable!()
        };
        assert_eq!(keyed.slots.len(), 1);
        assert_eq!(keyed.retained_batches.len(), 1);
        assert_eq!(
            keyed.retained_batches.values().next().unwrap().references,
            1
        );
        assert_eq!(keyed.gc.heap.len(), 1);
        assert_gc_consistent(&state);
    }

    #[test]
    fn replay_only_and_zero_affinity_checkpoint_round_trip() {
        let mut append = append_normalizer();
        let replay = positioned(
            plain_batch(&[Some("A")], &[100], &[10]),
            &[b"partition-0"],
            &[1],
            None,
        );
        append.prepare(replay.clone(), i64::MIN).unwrap().commit();
        append
            .prepare_batches(&[], i64::MIN, 101, usize::MAX)
            .unwrap()
            .commit();
        let checkpoint = append
            .capture_checkpoint(usize::MAX)
            .unwrap()
            .encode(usize::MAX)
            .unwrap();
        let mut append = BoundedJoinInputNormalizer::from_checkpoint(
            &checkpoint,
            plain_schema(),
            BoundedJoinInputConfig {
                vnode: 5,
                event_time_index: 1,
                mode: BoundedJoinInputMode::AppendOnly,
                max_retained_bytes: 16 * 1024 * 1024,
            },
            usize::MAX,
        )
        .unwrap();
        assert_eq!(append.closed_cutoff(), 101);
        assert_eq!(append.replay_frontiers.len(), 1);
        let prepared = append
            .prepare_batches(&[replay], 101, 101, usize::MAX)
            .unwrap();
        assert_eq!(prepared.output().num_rows(), 0);
        prepared.commit();

        let mut keyed = keyed_normalizer();
        keyed
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[999]),
                    &[b"partition-0"],
                    &[1],
                    Some(vec![SourceMutation::Tombstone]),
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let checkpoint = keyed
            .capture_checkpoint(usize::MAX)
            .unwrap()
            .encode(usize::MAX)
            .unwrap();
        let mut keyed = BoundedJoinInputNormalizer::from_checkpoint(
            &checkpoint,
            plain_schema(),
            BoundedJoinInputConfig {
                vnode: 7,
                event_time_index: 1,
                mode: BoundedJoinInputMode::KeyedUpsert {
                    primary_key_indices: vec![0, 1],
                },
                max_retained_bytes: 16 * 1024 * 1024,
            },
            usize::MAX,
        )
        .unwrap();
        let ModeState::Keyed(restored) = &keyed.mode else {
            unreachable!()
        };
        assert_eq!(restored.slots.len(), 1);
        assert!(restored.slots.values().all(|slot| slot.row.is_none()));
        assert!(restored.retained_batches.is_empty());
        assert_gc_consistent(&keyed);
        let error = keyed
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[10]),
                    &[b"partition-1"],
                    &[2],
                    None,
                ),
                i64::MIN,
            )
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("moved between source partitions"));
    }

    #[test]
    fn keyed_live_checkpoint_rejects_tight_budget_before_ipc_materialization() {
        let mut keyed = keyed_normalizer();
        keyed
            .prepare(
                positioned(
                    plain_batch(&[Some("A")], &[100], &[10]),
                    &[b"partition-0"],
                    &[1],
                    None,
                ),
                i64::MIN,
            )
            .unwrap()
            .commit();
        let checkpoint = keyed
            .capture_checkpoint(usize::MAX)
            .unwrap()
            .encode(usize::MAX)
            .unwrap();
        let config = BoundedJoinInputConfig {
            vnode: 7,
            event_time_index: 1,
            mode: BoundedJoinInputMode::KeyedUpsert {
                primary_key_indices: vec![0, 1],
            },
            max_retained_bytes: usize::MAX,
        };
        let restore_peak = BoundedJoinInputNormalizer::checkpoint_restore_preflight_bytes(
            &checkpoint,
            plain_schema().as_ref(),
            &config,
        )
        .unwrap();
        let error = match BoundedJoinInputNormalizer::from_checkpoint(
            &checkpoint,
            plain_schema(),
            config.clone(),
            restore_peak - 1,
        ) {
            Ok(_) => panic!("tight restore headroom must fail before decoding compacted IPC"),
            Err(error) => error,
        };
        assert!(matches!(error, DbError::ManagedStateBudgetExceeded { .. }));

        let restored = BoundedJoinInputNormalizer::from_checkpoint(
            &checkpoint,
            plain_schema(),
            config,
            restore_peak,
        )
        .unwrap();
        let ModeState::Keyed(restored_keyed) = &restored.mode else {
            unreachable!()
        };
        assert_eq!(restored_keyed.slots.len(), 1);
        assert_eq!(restored_keyed.retained_batches.len(), 1);
        assert!(restored_keyed
            .slots
            .values()
            .all(|slot| slot.row.is_some() && slot.row_identity.is_some()));
        assert_gc_consistent(&restored);
    }

    #[test]
    fn append_only_is_replay_safe_and_emits_explicit_unit_weights() {
        let mut state = append_normalizer();
        let input = positioned(
            plain_batch(&[Some("A"), Some("B")], &[100, 101], &[10, 20]),
            &[b"partition-0", b"partition-1"],
            &[1, 1],
            None,
        );
        let prepared = state.prepare(input.clone(), i64::MIN).unwrap();
        assert_eq!(output_weights(prepared.output()), vec![1, 1]);
        prepared.commit();
        let replay = state.prepare(input, i64::MIN).unwrap();
        assert_eq!(replay.output().num_rows(), 0);
        replay.commit();
    }
}
