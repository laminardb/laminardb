//! Vnode-local state for event-time temporal joins.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, Int64Array, RecordBatch,
    TimestampMillisecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::row::{RowConverter, Rows, SortField};
use laminar_connectors::connector::{
    SourceMutation, SourceMutationView, SOURCE_ORDER_KEY_COLUMN as SOURCE_ORDER_COLUMN,
    SOURCE_PARTITION_COLUMN, SOURCE_SUB_OFFSET_COLUMN,
};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batches_stream_bounded};
use laminar_core::state::PartitionKeyCodecV1;
use laminar_sql::temporal::{TemporalJoinKind, TemporalProbeSchedule};
use rustc_hash::{FxHashMap, FxHashSet};
use xxhash_rust::xxh3::xxh3_128;

use crate::error::DbError;

const FORMAT_VERSION: u8 = 3;
const CHECKPOINT_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
const MAP_ENTRY_CHARGE: usize = 128;
const VERSION_ENTRY_CHARGE: usize = 256;
const TIMER_ENTRY_CHARGE: usize = 96;
const BATCH_CHARGE: usize = 256;
const BASE_STATE_CHARGE: usize = 512;
const HISTORY_KEY_ROSTER_CHARGE: usize = 32;
const POSITION_COLUMN_COUNT: usize = 3;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub(crate) struct TemporalSourcePosition {
    partition: Vec<u8>,
    order: Vec<u8>,
    sub_offset: u32,
}

impl TemporalSourcePosition {
    fn heap_bytes(&self) -> usize {
        self.partition.len().saturating_add(self.order.len())
    }
}

#[derive(Debug, Clone, Copy)]
#[allow(clippy::struct_field_names)]
pub(crate) struct TemporalStateLimits {
    pub(crate) max_retained_bytes: usize,
    pub(crate) max_pending_probes: usize,
    pub(crate) max_offsets_per_row: usize,
    pub(crate) max_horizon_ms: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct TemporalJoinStateConfig {
    pub(crate) vnode: u32,
    pub(crate) vnode_count: NonZeroU32,
    pub(crate) left_key_indices: Vec<usize>,
    pub(crate) right_key_indices: Vec<usize>,
    pub(crate) key_codec: Arc<PartitionKeyCodecV1>,
    pub(crate) left_time_index: usize,
    pub(crate) right_time_index: usize,
    pub(crate) left_name: String,
    pub(crate) right_name: String,
    pub(crate) operator_name: String,
    pub(crate) join_kind: TemporalJoinKind,
    pub(crate) schedule: TemporalProbeSchedule,
    pub(crate) emit_probe_metadata: bool,
    pub(crate) left_allowed_lateness_ms: i64,
    pub(crate) right_allowed_lateness_ms: i64,
    pub(crate) history_retention_ms: i64,
    pub(crate) limits: TemporalStateLimits,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TemporalRightApplyStats {
    pub(crate) inserted: usize,
    pub(crate) duplicates: usize,
    pub(crate) ignored_nulls: usize,
}

pub(crate) struct TemporalReadyDrain {
    pub(crate) output: RecordBatch,
    pub(crate) drained_probes: usize,
    pub(crate) has_more: bool,
}

pub(crate) struct TemporalHistoryGcDrain {
    pub(crate) steps: usize,
    pub(crate) removed_versions: usize,
    pub(crate) has_more: bool,
}

#[derive(Clone)]
struct RowRef {
    batch: Arc<RecordBatch>,
    row: u32,
}

struct RetainedBatch {
    batch: Arc<RecordBatch>,
    references: usize,
}

struct Version {
    row: Option<(u64, u32)>,
}

#[derive(Clone, PartialEq, Eq)]
struct MutationIdentity {
    key: Option<Box<[u8]>>,
    event_time: Option<i64>,
    tombstone: bool,
    payload_fingerprint: [u8; 16],
}

#[derive(Clone, PartialEq, Eq)]
struct LeftRowIdentity {
    key: Option<Box<[u8]>>,
    event_time: Option<i64>,
    payload_fingerprint: [u8; 16],
}

struct ReplayCursor {
    order: Box<[u8]>,
    sub_offset: u32,
}

impl ReplayCursor {
    fn from_source(source: &TemporalSourcePosition) -> Self {
        Self {
            order: source.order.clone().into_boxed_slice(),
            sub_offset: source.sub_offset,
        }
    }

    fn compare(&self, source: &TemporalSourcePosition) -> Ordering {
        source
            .order
            .as_slice()
            .cmp(self.order.as_ref())
            .then_with(|| source.sub_offset.cmp(&self.sub_offset))
    }
}

struct RightReplayFrontier {
    cursor: ReplayCursor,
    identity: MutationIdentity,
}

struct LeftReplayFrontier {
    cursor: ReplayCursor,
    identity: LeftRowIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ProbeIdentity {
    source: TemporalSourcePosition,
    offset_ms: i64,
}

struct PendingProbe {
    left_batch: u64,
    left_row: u32,
    key: Box<[u8]>,
    left_event_time: i64,
    probe_time: i64,
    deadline: i64,
    payload_fingerprint: [u8; 16],
}

struct OutputRow {
    left: RowRef,
    right: Option<RowRef>,
    offset_ms: i64,
    probe_time: Option<i64>,
}

struct HistoryGcRemoval {
    roster_index: usize,
    order: (i64, TemporalSourcePosition),
    batch_id: Option<u64>,
}

type VersionChain = BTreeMap<(i64, TemporalSourcePosition), Version>;
type VnodeHistory = FxHashMap<Box<[u8]>, VersionChain>;

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointConfig {
    vnode: u32,
    vnode_count: u32,
    left_key_indices: Vec<u32>,
    right_key_indices: Vec<u32>,
    left_time_index: u32,
    right_time_index: u32,
    left_name: String,
    right_name: String,
    operator_name: String,
    join_kind: u8,
    offsets: Vec<i64>,
    emit_probe_metadata: bool,
    left_allowed_lateness_ms: i64,
    right_allowed_lateness_ms: i64,
    history_retention_ms: i64,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointRightReplayFrontier {
    partition: Vec<u8>,
    order: Vec<u8>,
    sub_offset: u32,
    key: Option<Vec<u8>>,
    event_time: Option<i64>,
    tombstone: bool,
    payload_fingerprint: [u8; 16],
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointLeftReplayFrontier {
    partition: Vec<u8>,
    order: Vec<u8>,
    sub_offset: u32,
    key: Option<Vec<u8>>,
    event_time: Option<i64>,
    payload_fingerprint: [u8; 16],
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointVersion {
    key: Vec<u8>,
    event_time: i64,
    source: TemporalSourcePosition,
    tombstone: bool,
    right_row: Option<u32>,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointProbe {
    source: TemporalSourcePosition,
    offset_ms: i64,
    left_row: u32,
    key: Vec<u8>,
    left_event_time: i64,
    probe_time: i64,
    deadline: i64,
    payload_fingerprint: [u8; 16],
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct TemporalJoinCheckpoint {
    format_version: u8,
    config: CheckpointConfig,
    left_frontier: Option<i64>,
    left_idle: bool,
    right_frontier: Option<i64>,
    right_idle: bool,
    history_evicted_before: Option<i64>,
    history_key_roster: Vec<Vec<u8>>,
    history_gc_cursor: u64,
    history_gc_sweep_end: u64,
    history_gc_active_cutoff: Option<i64>,
    history_gc_completed_cutoff: Option<i64>,
    right_replay_frontiers: Vec<CheckpointRightReplayFrontier>,
    left_replay_frontiers: Vec<CheckpointLeftReplayFrontier>,
    versions: Vec<CheckpointVersion>,
    pending: Vec<CheckpointProbe>,
    right_rows_ipc: Vec<u8>,
    left_rows_ipc: Vec<u8>,
}

pub(crate) struct TemporalJoinVnodeState {
    config: TemporalJoinStateConfig,
    offsets: Vec<i64>,
    minimum_offset: i64,
    maximum_offset: i64,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    left_row_codec: RowConverter,
    right_row_codec: RowConverter,
    history: VnodeHistory,
    history_key_roster: Vec<Box<[u8]>>,
    right_replay_frontiers: FxHashMap<Box<[u8]>, RightReplayFrontier>,
    right_batches: FxHashMap<u64, RetainedBatch>,
    pending: FxHashMap<ProbeIdentity, PendingProbe>,
    timers: BTreeMap<i64, BTreeSet<ProbeIdentity>>,
    left_batches: FxHashMap<u64, RetainedBatch>,
    left_replay_frontiers: FxHashMap<Box<[u8]>, LeftReplayFrontier>,
    next_batch_id: u64,
    left_frontier: Option<i64>,
    left_idle: bool,
    right_frontier: Option<i64>,
    right_idle: bool,
    history_evicted_before: Option<i64>,
    history_gc_cursor: usize,
    history_gc_sweep_end: usize,
    history_gc_active_cutoff: Option<i64>,
    history_gc_completed_cutoff: Option<i64>,
    charged_bytes: usize,
}

impl TemporalJoinVnodeState {
    pub(crate) fn try_new(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        config: TemporalJoinStateConfig,
    ) -> Result<Self, DbError> {
        validate_config(&left_schema, &right_schema, &config)?;
        let offsets = expand_offsets(&config.schedule, config.limits)?;
        let minimum_offset = offsets
            .iter()
            .copied()
            .min()
            .expect("validated temporal schedule is non-empty");
        let maximum_offset = offsets
            .iter()
            .copied()
            .max()
            .expect("validated temporal schedule is non-empty");
        if config.schedule.is_multi_horizon() && !config.emit_probe_metadata {
            return Err(DbError::Config(
                "multi-horizon temporal probes must emit offset_ms and probe_time".into(),
            ));
        }
        validate_output_names(&left_schema, &right_schema, &config)?;
        let left_row_codec = row_codec(&left_schema, "left")?;
        let right_row_codec = row_codec(&right_schema, "right")?;
        let output_schema = temporal_join_output_schema(
            &left_schema,
            &right_schema,
            &config.right_name,
            config.join_kind,
            config.emit_probe_metadata,
        )?;
        if BASE_STATE_CHARGE > config.limits.max_retained_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join vnode {} base state", config.vnode),
                accounted_bytes: BASE_STATE_CHARGE,
                limit_bytes: config.limits.max_retained_bytes,
            });
        }
        Ok(Self {
            config,
            offsets,
            minimum_offset,
            maximum_offset,
            left_schema,
            right_schema,
            output_schema,
            left_row_codec,
            right_row_codec,
            history: FxHashMap::default(),
            history_key_roster: Vec::new(),
            right_replay_frontiers: FxHashMap::default(),
            right_batches: FxHashMap::default(),
            pending: FxHashMap::default(),
            timers: BTreeMap::new(),
            left_batches: FxHashMap::default(),
            left_replay_frontiers: FxHashMap::default(),
            next_batch_id: 1,
            left_frontier: None,
            left_idle: false,
            right_frontier: None,
            right_idle: false,
            history_evicted_before: None,
            history_gc_cursor: 0,
            history_gc_sweep_end: 0,
            history_gc_active_cutoff: None,
            history_gc_completed_cutoff: None,
            charged_bytes: BASE_STATE_CHARGE,
        })
    }

    pub(crate) fn retained_versions(&self) -> usize {
        self.history.values().map(BTreeMap::len).sum()
    }

    pub(crate) fn pending_probes(&self) -> usize {
        self.pending.len()
    }

    pub(crate) const fn accounted_state_bytes(&self) -> usize {
        self.charged_bytes
    }

    pub(crate) fn set_retained_byte_limit(&mut self, bytes: usize) -> Result<(), DbError> {
        if bytes < self.charged_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join vnode {} retained state", self.config.vnode),
                accounted_bytes: self.charged_bytes,
                limit_bytes: bytes,
            });
        }
        self.config.limits.max_retained_bytes = bytes;
        Ok(())
    }

    pub(crate) const fn frontier_snapshot(&self) -> (Option<i64>, bool, Option<i64>, bool) {
        (
            self.left_frontier,
            self.left_idle,
            self.right_frontier,
            self.right_idle,
        )
    }

    pub(crate) fn pending_watermark_hold(&self) -> Option<i64> {
        let (&deadline, _) = self.timers.first_key_value()?;
        let earliest_probe = deadline - self.config.right_allowed_lateness_ms;
        Some(earliest_probe.min(earliest_probe.saturating_sub(self.maximum_offset)))
    }

    #[cfg(test)]
    pub(crate) fn apply_right_batch(
        &mut self,
        batch: &RecordBatch,
        operations: Option<SourceMutationView<'_>>,
    ) -> Result<TemporalRightApplyStats, DbError> {
        self.validate_batch_schema(batch, false)?;
        let keys = self.encode_keys(batch, false)?;
        self.apply_right_batch_with_keys(batch, operations, &keys, None)
    }

    pub(crate) fn apply_right_batch_routed(
        &mut self,
        batch: &RecordBatch,
        operations: Option<SourceMutationView<'_>>,
        keys: &Rows,
        source_rows: &[u32],
    ) -> Result<TemporalRightApplyStats, DbError> {
        self.validate_batch_schema(batch, false)?;
        self.validate_routed_keys(batch, keys, source_rows)?;
        self.apply_right_batch_with_keys(batch, operations, keys, Some(source_rows))
    }

    fn apply_right_batch_with_keys(
        &mut self,
        batch: &RecordBatch,
        operations: Option<SourceMutationView<'_>>,
        keys: &Rows,
        source_rows: Option<&[u32]>,
    ) -> Result<TemporalRightApplyStats, DbError> {
        if operations.is_some_and(|operations| operations.len() != batch.num_rows()) {
            return Err(self.pipeline_error("right CDC operation count does not match row count"));
        }
        let positions = extract_source_positions(batch)?;
        if positions.iter().all(|source| {
            self.right_replay_frontiers
                .get(source.partition.as_slice())
                .is_some_and(|frontier| frontier.cursor.compare(source) == Ordering::Less)
        }) {
            return Ok(TemporalRightApplyStats {
                duplicates: positions.len(),
                ..TemporalRightApplyStats::default()
            });
        }
        let times = extract_times(batch, self.config.right_time_index, "right")?;
        let fingerprints = fingerprint_rows(&self.right_row_codec, batch, "right")?;
        let key_columns = self.key_columns(batch, false);
        let mut stats = TemporalRightApplyStats::default();
        let mut candidates = Vec::new();
        let mut staged_frontiers: FxHashMap<Box<[u8]>, RightReplayFrontier> = FxHashMap::default();

        for (row, source_position) in positions.iter().enumerate() {
            let null_key = key_columns.iter().any(|column| column.is_null(row));
            let key_row = source_rows.map_or(row, |rows| rows[row] as usize);
            let key = (!null_key).then(|| Box::<[u8]>::from(keys.row(key_row).data()));
            let event_time = (!times.is_null(row)).then(|| times.value(row));
            if source_rows.is_none() {
                if let Some(key) = key.as_deref() {
                    self.validate_vnode(key)?;
                }
            }
            let tombstone = operations.and_then(|operations| operations.get(row))
                == Some(SourceMutation::Tombstone);
            let identity = MutationIdentity {
                key: key.clone(),
                event_time,
                tombstone,
                payload_fingerprint: fingerprints[row],
            };
            if let Some(current) = staged_frontiers.get(source_position.partition.as_slice()) {
                match current.cursor.compare(source_position) {
                    Ordering::Less => {
                        return Err(self.pipeline_error(
                            "right source positions regressed within one input batch",
                        ));
                    }
                    Ordering::Equal => {
                        if current.identity != identity {
                            return Err(self.pipeline_error(
                                "a right source position was replayed with different temporal data",
                            ));
                        }
                        stats.duplicates += 1;
                        continue;
                    }
                    Ordering::Greater => {}
                }
            } else if let Some(current) = self
                .right_replay_frontiers
                .get(source_position.partition.as_slice())
            {
                match current.cursor.compare(source_position) {
                    Ordering::Less => {
                        stats.duplicates += 1;
                        continue;
                    }
                    Ordering::Equal => {
                        if current.identity != identity {
                            return Err(self.pipeline_error(
                                "a right source position was replayed with different temporal data",
                            ));
                        }
                        stats.duplicates += 1;
                        continue;
                    }
                    Ordering::Greater => {}
                }
            }
            staged_frontiers.insert(
                source_position.partition.clone().into_boxed_slice(),
                RightReplayFrontier {
                    cursor: ReplayCursor::from_source(source_position),
                    identity,
                },
            );
            let source = source_position.clone();
            let row = u32::try_from(row)
                .map_err(|_| self.pipeline_error("right batch exceeds the supported row count"))?;
            let (Some(key), Some(event_time)) = (key, event_time) else {
                stats.ignored_nulls += 1;
                candidates.push((None, source, row));
                continue;
            };
            self.reject_late_input(event_time, "right")?;
            candidates.push((Some((key, event_time, tombstone)), source, row));
        }

        if candidates.is_empty() {
            return Ok(stats);
        }
        let live_count = candidates
            .iter()
            .filter(|entry| entry.0.as_ref().is_some_and(|entry| !entry.2))
            .count();
        let mut growth = 0usize;
        let mut released = 0usize;
        for (partition, frontier) in &staged_frontiers {
            growth = growth
                .checked_add(self.right_replay_frontier_charge(partition, frontier)?)
                .ok_or_else(|| self.pipeline_error("right retained-state accounting overflow"))?;
            if let Some(previous) = self.right_replay_frontiers.get(partition.as_ref()) {
                released = released
                    .checked_add(self.right_replay_frontier_charge(partition, previous)?)
                    .ok_or_else(|| {
                        self.pipeline_error("right retained-state accounting overflow")
                    })?;
            }
        }
        let mut new_history_keys: FxHashSet<&[u8]> = FxHashSet::default();
        for (version, source, _) in &candidates {
            if let Some((key, _, _)) = version {
                let version_charge = VERSION_ENTRY_CHARGE
                    .checked_add(source.heap_bytes())
                    .ok_or_else(|| self.pipeline_error("right version accounting overflow"))?;
                growth = growth.checked_add(version_charge).ok_or_else(|| {
                    self.pipeline_error("right retained-state accounting overflow")
                })?;
                if !self.history.contains_key(key.as_ref()) && new_history_keys.insert(key.as_ref())
                {
                    let key_charge = MAP_ENTRY_CHARGE
                        .checked_add(key.len())
                        .and_then(|value| value.checked_add(HISTORY_KEY_ROSTER_CHARGE))
                        .and_then(|value| value.checked_add(key.len()))
                        .ok_or_else(|| {
                            self.pipeline_error("right history-key accounting overflow")
                        })?;
                    growth = growth.checked_add(key_charge).ok_or_else(|| {
                        self.pipeline_error("right retained-state accounting overflow")
                    })?;
                }
            }
        }
        if live_count != 0 {
            growth = growth
                .checked_add(batch_charge(batch).ok_or_else(|| {
                    self.pipeline_error("right retained-batch accounting overflow")
                })?)
                .ok_or_else(|| self.pipeline_error("right retained-state accounting overflow"))?;
        }
        let admitted_charge =
            self.admitted_replacement_charge(growth, released, "right version admission")?;
        let batch_id = if live_count == 0 {
            None
        } else {
            Some(self.allocate_batch_id()?)
        };
        if let Some(batch_id) = batch_id {
            self.right_batches.insert(
                batch_id,
                RetainedBatch {
                    batch: Arc::new(batch.clone()),
                    references: live_count,
                },
            );
        }
        for (version, source, row) in candidates {
            if let Some((key, event_time, tombstone)) = version {
                let order = (event_time, source.clone());
                let version = Version {
                    row: (!tombstone).then(|| (batch_id.expect("live batch exists"), row)),
                };
                let replaced = match self.history.entry(key) {
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        entry.get_mut().insert(order, version)
                    }
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        self.history_key_roster.push(entry.key().clone());
                        entry.insert(BTreeMap::new()).insert(order, version)
                    }
                };
                debug_assert!(replaced.is_none());
                stats.inserted += 1;
            }
        }
        for (partition, frontier) in staged_frontiers {
            self.right_replay_frontiers.insert(partition, frontier);
        }
        self.charged_bytes = admitted_charge;
        Ok(stats)
    }

    #[cfg(test)]
    pub(crate) fn probe_left_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DbError> {
        self.validate_batch_schema(batch, true)?;
        let keys = self.encode_keys(batch, true)?;
        self.probe_left_batch_with_keys(batch, &keys, None)
    }

    pub(crate) fn probe_left_batch_routed(
        &mut self,
        batch: &RecordBatch,
        keys: &Rows,
        source_rows: &[u32],
    ) -> Result<RecordBatch, DbError> {
        self.validate_batch_schema(batch, true)?;
        self.validate_routed_keys(batch, keys, source_rows)?;
        self.probe_left_batch_with_keys(batch, keys, Some(source_rows))
    }

    fn probe_left_batch_with_keys(
        &mut self,
        batch: &RecordBatch,
        keys: &Rows,
        source_rows: Option<&[u32]>,
    ) -> Result<RecordBatch, DbError> {
        let expanded_rows = batch
            .num_rows()
            .checked_mul(self.offsets.len())
            .ok_or_else(|| self.pipeline_error("temporal probe expansion overflowed"))?;
        u32::try_from(expanded_rows)
            .map_err(|_| self.pipeline_error("temporal probe expansion exceeds the row limit"))?;
        let positions = extract_source_positions(batch)?;
        if positions.iter().all(|source| {
            self.left_replay_frontiers
                .get(source.partition.as_slice())
                .is_some_and(|frontier| frontier.cursor.compare(source) == Ordering::Less)
                && !self.has_pending_left_source(source)
        }) {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        let times = extract_times(batch, self.config.left_time_index, "left")?;
        let fingerprints = fingerprint_rows(&self.left_row_codec, batch, "left")?;
        let key_columns = self.key_columns(batch, true);
        let input = Arc::new(batch.clone());
        let mut outputs = Vec::new();
        let mut planned = Vec::new();
        let mut staged_frontiers: FxHashMap<Box<[u8]>, LeftReplayFrontier> = FxHashMap::default();

        for (row, source_position) in positions.iter().enumerate() {
            outputs
                .try_reserve(self.offsets.len())
                .map_err(|_| self.pipeline_error("temporal output expansion is too large"))?;
            planned
                .try_reserve(self.offsets.len())
                .map_err(|_| self.pipeline_error("temporal probe expansion is too large"))?;
            let null_key = key_columns.iter().any(|column| column.is_null(row));
            let event_time = (!times.is_null(row)).then(|| times.value(row));
            let key_row = source_rows.map_or(row, |rows| rows[row] as usize);
            let key = (!null_key).then(|| Box::<[u8]>::from(keys.row(key_row).data()));
            if source_rows.is_none() {
                if let Some(key) = key.as_deref() {
                    self.validate_vnode(key)?;
                }
            }
            let row = u32::try_from(row)
                .map_err(|_| self.pipeline_error("left batch exceeds the supported row count"))?;
            let row_identity = LeftRowIdentity {
                key: key.clone(),
                event_time,
                payload_fingerprint: fingerprints[row as usize],
            };
            if let Some(current) = staged_frontiers.get(source_position.partition.as_slice()) {
                match current.cursor.compare(source_position) {
                    Ordering::Less => {
                        return Err(self.pipeline_error(
                            "left source positions regressed within one input batch",
                        ));
                    }
                    Ordering::Equal => {
                        if current.identity != row_identity {
                            return Err(self.pipeline_error(
                                "a left source position was replayed with different temporal data",
                            ));
                        }
                        continue;
                    }
                    Ordering::Greater => {}
                }
            } else if let Some(current) = self
                .left_replay_frontiers
                .get(source_position.partition.as_slice())
            {
                match current.cursor.compare(source_position) {
                    Ordering::Less => {
                        self.validate_replayed_pending_left(source_position, &row_identity)?;
                        continue;
                    }
                    Ordering::Equal => {
                        if current.identity != row_identity {
                            return Err(self.pipeline_error(
                                "a left source position was replayed with different temporal data",
                            ));
                        }
                        continue;
                    }
                    Ordering::Greater => {}
                }
            }
            if let Some(event_time) = event_time {
                self.reject_late_input(event_time, "left")?;
            }
            for &offset_ms in &self.offsets {
                let identity = ProbeIdentity {
                    source: source_position.clone(),
                    offset_ms,
                };
                if self.pending.contains_key(&identity) {
                    return Err(
                        self.pipeline_error("a pending left probe is ahead of its replay frontier")
                    );
                }
                let left = RowRef {
                    batch: Arc::clone(&input),
                    row,
                };
                let probe_time = event_time
                    .map(|event_time| {
                        event_time.checked_add(offset_ms).ok_or_else(|| {
                            self.pipeline_error(
                                "left event time plus temporal probe offset overflowed",
                            )
                        })
                    })
                    .transpose()?;
                let (Some(key), Some(event_time), Some(probe_time)) =
                    (key.clone(), event_time, probe_time)
                else {
                    self.push_final_output(&mut outputs, left, None, offset_ms, probe_time);
                    continue;
                };
                self.reject_evicted_probe(probe_time)?;
                let deadline = probe_time
                    .checked_add(self.config.right_allowed_lateness_ms)
                    .ok_or_else(|| self.pipeline_error("temporal probe deadline overflowed"))?;
                if deadline == i64::MAX {
                    return Err(self.pipeline_error(
                        "temporal probe deadline cannot be passed by a finite frontier",
                    ));
                }
                if self
                    .right_frontier
                    .is_some_and(|frontier| frontier > deadline)
                {
                    let right = self.lookup(&key, probe_time)?;
                    self.push_final_output(&mut outputs, left, right, offset_ms, Some(probe_time));
                } else {
                    planned.push((
                        identity,
                        key,
                        event_time,
                        probe_time,
                        deadline,
                        row,
                        row_identity.payload_fingerprint,
                    ));
                }
            }
            staged_frontiers.insert(
                source_position.partition.clone().into_boxed_slice(),
                LeftReplayFrontier {
                    cursor: ReplayCursor::from_source(source_position),
                    identity: row_identity,
                },
            );
        }

        let new_pending = planned.len();
        let pending_total = self
            .pending
            .len()
            .checked_add(new_pending)
            .ok_or_else(|| self.pipeline_error("pending temporal probe count overflowed"))?;
        if pending_total > self.config.limits.max_pending_probes {
            return Err(DbError::BackpressureFail(format!(
                "temporal join vnode {} would retain {pending_total} pending probes; limit is {}",
                self.config.vnode, self.config.limits.max_pending_probes
            )));
        }
        let mut growth = 0usize;
        let mut released = 0usize;
        for (partition, frontier) in &staged_frontiers {
            growth = growth
                .checked_add(self.left_replay_frontier_charge(partition, frontier)?)
                .ok_or_else(|| self.pipeline_error("left temporal state accounting overflow"))?;
            if let Some(previous) = self.left_replay_frontiers.get(partition.as_ref()) {
                released = released
                    .checked_add(self.left_replay_frontier_charge(partition, previous)?)
                    .ok_or_else(|| {
                        self.pipeline_error("left temporal state accounting overflow")
                    })?;
            }
        }
        for (identity, key, _, _, _, _, _) in &planned {
            let entry_charge = VERSION_ENTRY_CHARGE
                .checked_add(TIMER_ENTRY_CHARGE)
                .and_then(|value| value.checked_add(key.len()))
                .and_then(|value| value.checked_add(identity.source.heap_bytes()))
                .and_then(|value| value.checked_add(identity.source.heap_bytes()))
                .ok_or_else(|| self.pipeline_error("left temporal state accounting overflow"))?;
            growth = growth
                .checked_add(entry_charge)
                .ok_or_else(|| self.pipeline_error("left temporal state accounting overflow"))?;
        }
        if new_pending != 0 {
            growth = growth
                .checked_add(batch_charge(batch).ok_or_else(|| {
                    self.pipeline_error("left retained-batch accounting overflow")
                })?)
                .ok_or_else(|| self.pipeline_error("left temporal state accounting overflow"))?;
        }
        let admitted_charge =
            self.admitted_replacement_charge(growth, released, "left probe admission")?;
        let output = self.build_output(&outputs)?;
        let left_batch_id = if new_pending == 0 {
            None
        } else {
            Some(self.allocate_batch_id()?)
        };
        if let Some(batch_id) = left_batch_id {
            self.left_batches.insert(
                batch_id,
                RetainedBatch {
                    batch: Arc::clone(&input),
                    references: new_pending,
                },
            );
        }
        for (identity, key, left_event_time, probe_time, deadline, row, payload_fingerprint) in
            planned
        {
            let inserted = self
                .timers
                .entry(deadline)
                .or_default()
                .insert(identity.clone());
            debug_assert!(inserted);
            self.pending.insert(
                identity,
                PendingProbe {
                    left_batch: left_batch_id.expect("pending batch exists"),
                    left_row: row,
                    key,
                    left_event_time,
                    probe_time,
                    deadline,
                    payload_fingerprint,
                },
            );
        }
        for (partition, frontier) in staged_frontiers {
            self.left_replay_frontiers.insert(partition, frontier);
        }
        self.charged_bytes = admitted_charge;
        Ok(output)
    }

    pub(crate) fn advance_left_frontier(
        &mut self,
        frontier: Option<i64>,
        idle: bool,
    ) -> Result<(), DbError> {
        validate_frontier(self.left_frontier, frontier, "left")?;
        if let Some(frontier) = frontier {
            self.left_frontier = Some(frontier);
        }
        self.left_idle = idle;
        self.schedule_history_gc(self.left_frontier, self.right_frontier);
        Ok(())
    }

    pub(crate) fn advance_right_frontier(
        &mut self,
        frontier: Option<i64>,
        idle: bool,
    ) -> Result<(), DbError> {
        validate_frontier(self.right_frontier, frontier, "right")?;
        if let Some(frontier) = frontier {
            self.right_frontier = Some(frontier);
        }
        self.right_idle = idle;
        self.schedule_history_gc(self.left_frontier, self.right_frontier);
        Ok(())
    }

    pub(crate) fn has_ready_probes(&self) -> bool {
        self.right_frontier.is_some_and(|frontier| {
            self.timers
                .first_key_value()
                .is_some_and(|(deadline, _)| *deadline < frontier)
        })
    }

    pub(crate) fn drain_ready_probes(
        &mut self,
        max_probes: NonZeroUsize,
    ) -> Result<TemporalReadyDrain, DbError> {
        let Some(frontier) = self.right_frontier else {
            return Ok(TemporalReadyDrain {
                output: RecordBatch::new_empty(Arc::clone(&self.output_schema)),
                drained_probes: 0,
                has_more: false,
            });
        };
        let mut ready = Vec::new();
        ready
            .try_reserve(max_probes.get().min(self.pending.len()))
            .map_err(|_| self.pipeline_error("ready temporal probe budget is too large"))?;
        for (&deadline, identities) in self.timers.range(..frontier) {
            let remaining = max_probes.get() - ready.len();
            ready.extend(
                identities
                    .iter()
                    .take(remaining)
                    .cloned()
                    .map(|identity| (deadline, identity)),
            );
            if ready.len() == max_probes.get() {
                break;
            }
        }
        if ready.is_empty() {
            return Ok(TemporalReadyDrain {
                output: RecordBatch::new_empty(Arc::clone(&self.output_schema)),
                drained_probes: 0,
                has_more: false,
            });
        }

        let mut outputs = Vec::new();
        let mut seen = FxHashSet::default();
        let mut batch_releases: FxHashMap<u64, usize> = FxHashMap::default();
        let mut removed_charge = 0usize;
        for (deadline, identity) in &ready {
            if !seen.insert(identity.clone()) {
                return Err(self.pipeline_error("temporal timer contains a duplicate probe"));
            }
            let Some(probe) = self.pending.get(identity) else {
                return Err(self.pipeline_error("temporal timer referenced a missing probe"));
            };
            if probe.deadline != *deadline {
                return Err(self.pipeline_error("temporal timer deadline disagrees with probe"));
            }
            let left = self.left_row(probe.left_batch, probe.left_row)?;
            let right = self.lookup(&probe.key, probe.probe_time)?;
            self.push_final_output(
                &mut outputs,
                left,
                right,
                identity.offset_ms,
                Some(probe.probe_time),
            );
            removed_charge = removed_charge
                .checked_add(self.pending_probe_charge(identity, probe)?)
                .ok_or_else(|| self.pipeline_error("ready-probe accounting overflow"))?;
            let releases = batch_releases.entry(probe.left_batch).or_default();
            *releases = releases
                .checked_add(1)
                .ok_or_else(|| self.pipeline_error("ready-probe reference overflow"))?;
        }
        for (&batch_id, &release_count) in &batch_releases {
            let retained = self.left_batches.get(&batch_id).ok_or_else(|| {
                self.pipeline_error("pending temporal probe referenced a missing left batch")
            })?;
            if release_count > retained.references {
                return Err(
                    self.pipeline_error("temporal left batch reference count would underflow")
                );
            }
            if release_count == retained.references {
                removed_charge = removed_charge
                    .checked_add(batch_charge(&retained.batch).ok_or_else(|| {
                        self.pipeline_error("left retained-batch accounting overflow")
                    })?)
                    .ok_or_else(|| self.pipeline_error("ready-probe accounting overflow"))?;
            }
        }
        let next_charge = self
            .charged_bytes
            .checked_sub(removed_charge)
            .ok_or_else(|| self.pipeline_error("ready-probe accounting underflowed"))?;
        let output = self.build_output(&outputs)?;

        for (deadline, identity) in &ready {
            let remove_deadline = {
                let identities = self
                    .timers
                    .get_mut(deadline)
                    .expect("validated timer exists");
                let removed = identities.remove(identity);
                debug_assert!(removed);
                identities.is_empty()
            };
            if remove_deadline {
                self.timers.remove(deadline);
            }
            let removed = self.pending.remove(identity);
            debug_assert!(removed.is_some());
        }
        for (batch_id, release_count) in batch_releases {
            let remove_batch = {
                let retained = self
                    .left_batches
                    .get_mut(&batch_id)
                    .expect("validated batch exists");
                retained.references -= release_count;
                retained.references == 0
            };
            if remove_batch {
                self.left_batches.remove(&batch_id);
            }
        }
        self.charged_bytes = next_charge;
        self.schedule_history_gc(self.left_frontier, self.right_frontier);
        let has_more = self.has_ready_probes();
        Ok(TemporalReadyDrain {
            output,
            drained_probes: ready.len(),
            has_more,
        })
    }

    pub(crate) fn checkpoint(&self, max_encoded_bytes: usize) -> Result<Vec<u8>, DbError> {
        let (versions, right_rows) = self.checkpoint_versions()?;
        let (pending, left_rows) = self.checkpoint_pending()?;
        let right_rows_ipc = serialize_batches_stream_bounded(
            self.right_schema.as_ref(),
            std::iter::once(&right_rows),
            max_encoded_bytes,
        )
        .map_err(|error| DbError::Checkpoint(format!("temporal right IPC: {error}")))?;
        let left_rows_ipc = serialize_batches_stream_bounded(
            self.left_schema.as_ref(),
            std::iter::once(&left_rows),
            max_encoded_bytes,
        )
        .map_err(|error| DbError::Checkpoint(format!("temporal left IPC: {error}")))?;
        let mut right_replay_frontiers: Vec<_> = self.right_replay_frontiers.iter().collect();
        right_replay_frontiers
            .sort_unstable_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
        let right_replay_frontiers = right_replay_frontiers
            .into_iter()
            .map(|(partition, frontier)| CheckpointRightReplayFrontier {
                partition: partition.to_vec(),
                order: frontier.cursor.order.to_vec(),
                sub_offset: frontier.cursor.sub_offset,
                key: frontier.identity.key.as_deref().map(<[u8]>::to_vec),
                event_time: frontier.identity.event_time,
                tombstone: frontier.identity.tombstone,
                payload_fingerprint: frontier.identity.payload_fingerprint,
            })
            .collect();
        let mut left_replay_frontiers: Vec<_> = self.left_replay_frontiers.iter().collect();
        left_replay_frontiers
            .sort_unstable_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));
        let left_replay_frontiers = left_replay_frontiers
            .into_iter()
            .map(|(partition, frontier)| CheckpointLeftReplayFrontier {
                partition: partition.to_vec(),
                order: frontier.cursor.order.to_vec(),
                sub_offset: frontier.cursor.sub_offset,
                key: frontier.identity.key.as_deref().map(<[u8]>::to_vec),
                event_time: frontier.identity.event_time,
                payload_fingerprint: frontier.identity.payload_fingerprint,
            })
            .collect();
        let history_gc_cursor = u64::try_from(self.history_gc_cursor)
            .map_err(|_| DbError::Checkpoint("temporal history GC cursor exceeds u64".into()))?;
        let history_gc_sweep_end = u64::try_from(self.history_gc_sweep_end)
            .map_err(|_| DbError::Checkpoint("temporal history GC roster exceeds u64".into()))?;
        let checkpoint = TemporalJoinCheckpoint {
            format_version: FORMAT_VERSION,
            config: self.checkpoint_config()?,
            left_frontier: self.left_frontier,
            left_idle: self.left_idle,
            right_frontier: self.right_frontier,
            right_idle: self.right_idle,
            history_evicted_before: self.history_evicted_before,
            history_key_roster: self
                .history_key_roster
                .iter()
                .map(|key| key.to_vec())
                .collect(),
            history_gc_cursor,
            history_gc_sweep_end,
            history_gc_active_cutoff: self.history_gc_active_cutoff,
            history_gc_completed_cutoff: self.history_gc_completed_cutoff,
            right_replay_frontiers,
            left_replay_frontiers,
            versions,
            pending,
            right_rows_ipc,
            left_rows_ipc,
        };
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(max_encoded_bytes),
        );
        rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&checkpoint, writer)
            .map(|bytes| bytes.into_inner().into_vec())
            .map_err(|error| DbError::Checkpoint(format!("temporal checkpoint: {error}")))
    }

    pub(crate) fn restore(
        left_schema: SchemaRef,
        right_schema: SchemaRef,
        config: TemporalJoinStateConfig,
        bytes: &[u8],
    ) -> Result<Self, DbError> {
        let aligned;
        let bytes = if bytes.as_ptr().align_offset(CHECKPOINT_ALIGNMENT) == 0 {
            bytes
        } else {
            let mut copy = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
            copy.extend_from_slice(bytes);
            aligned = copy;
            &aligned
        };
        let checkpoint = rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("temporal checkpoint: {error}")))?;
        let mut state = Self::try_new(left_schema, right_schema, config)?;
        if checkpoint.format_version != FORMAT_VERSION {
            return Err(DbError::Checkpoint(format!(
                "unsupported temporal checkpoint version {}",
                checkpoint.format_version
            )));
        }
        state.validate_checkpoint_config(&checkpoint.config)?;
        state.validate_checkpoint_shape(&checkpoint)?;
        state.left_frontier = checkpoint.left_frontier;
        state.left_idle = checkpoint.left_idle;
        state.right_frontier = checkpoint.right_frontier;
        state.right_idle = checkpoint.right_idle;
        state.history_evicted_before = checkpoint.history_evicted_before;
        state.history_gc_cursor = usize::try_from(checkpoint.history_gc_cursor)
            .map_err(|_| DbError::Checkpoint("temporal history GC cursor exceeds usize".into()))?;
        state.history_gc_sweep_end = usize::try_from(checkpoint.history_gc_sweep_end)
            .map_err(|_| DbError::Checkpoint("temporal history GC roster exceeds usize".into()))?;
        state.history_gc_active_cutoff = checkpoint.history_gc_active_cutoff;
        state.history_gc_completed_cutoff = checkpoint.history_gc_completed_cutoff;
        let history_key_roster = checkpoint.history_key_roster;
        let right_rows = deserialize_batch_stream(&checkpoint.right_rows_ipc)
            .map_err(|error| DbError::Checkpoint(format!("temporal right IPC: {error}")))?;
        let left_rows = deserialize_batch_stream(&checkpoint.left_rows_ipc)
            .map_err(|error| DbError::Checkpoint(format!("temporal left IPC: {error}")))?;
        state.restore_right_replay_frontiers(checkpoint.right_replay_frontiers)?;
        state.restore_left_replay_frontiers(checkpoint.left_replay_frontiers)?;
        state.restore_versions(checkpoint.versions, right_rows)?;
        state.restore_history_gc_roster(history_key_roster)?;
        state.restore_pending(checkpoint.pending, left_rows)?;
        state.validate_restored_probe_consistency()?;
        state.restore_charge()?;
        Ok(state)
    }

    fn validate_batch_schema(&self, batch: &RecordBatch, left: bool) -> Result<(), DbError> {
        let expected = if left {
            &self.left_schema
        } else {
            &self.right_schema
        };
        if batch.schema().as_ref() != expected.as_ref() {
            return Err(DbError::SchemaMismatch(format!(
                "temporal {} input schema changed while state was retained",
                if left { "left" } else { "right" }
            )));
        }
        Ok(())
    }

    fn key_columns<'a>(&self, batch: &'a RecordBatch, left: bool) -> Vec<&'a ArrayRef> {
        let indices = if left {
            &self.config.left_key_indices
        } else {
            &self.config.right_key_indices
        };
        indices.iter().map(|&index| batch.column(index)).collect()
    }

    fn encode_keys(&self, batch: &RecordBatch, left: bool) -> Result<arrow::row::Rows, DbError> {
        let columns: Vec<ArrayRef> = self.key_columns(batch, left).into_iter().cloned().collect();
        self.config
            .key_codec
            .encode_columns(&columns)
            .map_err(|error| {
                self.pipeline_error(&format!("could not encode temporal join key: {error}"))
            })
    }

    fn validate_routed_keys(
        &self,
        batch: &RecordBatch,
        keys: &Rows,
        source_rows: &[u32],
    ) -> Result<(), DbError> {
        if source_rows.len() != batch.num_rows()
            || source_rows
                .iter()
                .any(|row| *row as usize >= keys.num_rows())
        {
            return Err(self.pipeline_error("routed temporal keys do not cover the routed batch"));
        }
        Ok(())
    }

    fn validate_vnode(&self, key: &[u8]) -> Result<(), DbError> {
        let actual = PartitionKeyCodecV1::vnode_for_encoded(key, self.config.vnode_count);
        if actual != self.config.vnode {
            return Err(self.pipeline_error(&format!(
                "key routed to vnode {actual}, not owned vnode {}",
                self.config.vnode
            )));
        }
        Ok(())
    }

    fn reject_late_input(&self, event_time: i64, side: &str) -> Result<(), DbError> {
        let (frontier, allowed_lateness_ms) = if side == "right" {
            (self.right_frontier, self.config.right_allowed_lateness_ms)
        } else {
            (self.left_frontier, self.config.left_allowed_lateness_ms)
        };
        if let Some(frontier) = frontier {
            let deadline = event_time.checked_add(allowed_lateness_ms).ok_or_else(|| {
                self.pipeline_error(&format!("{side} lateness deadline overflowed"))
            })?;
            if deadline < frontier {
                return Err(self.pipeline_error(&format!(
                    "{side} event at {event_time} arrived behind frontier {frontier} and allowed lateness"
                )));
            }
        }
        Ok(())
    }

    fn reject_evicted_probe(&self, probe_time: i64) -> Result<(), DbError> {
        if self
            .history_evicted_before
            .is_some_and(|floor| probe_time < floor)
        {
            return Err(self.pipeline_error(&format!(
                "probe time {probe_time} is older than retained history"
            )));
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8], probe_time: i64) -> Result<Option<RowRef>, DbError> {
        self.reject_evicted_probe(probe_time)?;
        let Some(versions) = self.history.get(key) else {
            return Ok(None);
        };
        let match_entry = if probe_time == i64::MAX {
            versions.last_key_value()
        } else {
            let next_time = probe_time + 1;
            let minimum_position = TemporalSourcePosition {
                partition: Vec::new(),
                order: Vec::new(),
                sub_offset: 0,
            };
            versions.range(..(next_time, minimum_position)).next_back()
        };
        let Some((_, version)) = match_entry else {
            return Ok(None);
        };
        let Some((batch_id, row)) = version.row else {
            return Ok(None);
        };
        self.right_row(batch_id, row).map(Some)
    }

    fn right_row(&self, batch_id: u64, row: u32) -> Result<RowRef, DbError> {
        let retained = self.right_batches.get(&batch_id).ok_or_else(|| {
            self.pipeline_error("temporal version referenced a missing right batch")
        })?;
        if row as usize >= retained.batch.num_rows() {
            return Err(self.pipeline_error("temporal version row is out of bounds"));
        }
        Ok(RowRef {
            batch: Arc::clone(&retained.batch),
            row,
        })
    }

    fn left_row(&self, batch_id: u64, row: u32) -> Result<RowRef, DbError> {
        let retained = self.left_batches.get(&batch_id).ok_or_else(|| {
            self.pipeline_error("pending temporal probe referenced a missing left batch")
        })?;
        if row as usize >= retained.batch.num_rows() {
            return Err(self.pipeline_error("pending temporal probe row is out of bounds"));
        }
        Ok(RowRef {
            batch: Arc::clone(&retained.batch),
            row,
        })
    }

    fn push_final_output(
        &self,
        output: &mut Vec<OutputRow>,
        left: RowRef,
        right: Option<RowRef>,
        offset_ms: i64,
        probe_time: Option<i64>,
    ) {
        if right.is_some() || self.config.join_kind == TemporalJoinKind::Left {
            output.push(OutputRow {
                left,
                right,
                offset_ms,
                probe_time,
            });
        }
    }

    fn build_output(&self, rows: &[OutputRow]) -> Result<RecordBatch, DbError> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }
        let left: Vec<Option<RowRef>> = rows.iter().map(|row| Some(row.left.clone())).collect();
        let right: Vec<Option<RowRef>> = rows.iter().map(|row| row.right.clone()).collect();
        let left_visible = self.left_schema.fields().len() - POSITION_COLUMN_COUNT;
        let mut columns = interleave_rows(&self.left_schema, &left, left_visible, false, "left")?;
        let right_columns = interleave_rows(
            &self.right_schema,
            &right,
            self.right_schema.fields().len() - POSITION_COLUMN_COUNT,
            self.config.join_kind == TemporalJoinKind::Left,
            "right",
        )?;
        columns.extend(right_columns);
        if self.config.emit_probe_metadata {
            columns.push(Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row.offset_ms),
            )));
            columns.push(Arc::new(TimestampMillisecondArray::from(
                rows.iter().map(|row| row.probe_time).collect::<Vec<_>>(),
            )));
        }
        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            self.pipeline_error(&format!("could not build temporal output: {error}"))
        })
    }

    fn schedule_history_gc(&mut self, left_frontier: Option<i64>, right_frontier: Option<i64>) {
        let Some(right_frontier) = right_frontier else {
            return;
        };
        let retention_cutoff = right_frontier
            .checked_sub(self.config.history_retention_ms)
            .unwrap_or(i64::MIN);
        let future_probe_cutoff = if self.left_idle {
            retention_cutoff
        } else {
            let Some(left_frontier) = left_frontier else {
                return;
            };
            let Some(earliest_future_event) =
                left_frontier.checked_sub(self.config.left_allowed_lateness_ms)
            else {
                return;
            };
            let Some(earliest_future_probe) =
                earliest_future_event.checked_add(self.minimum_offset)
            else {
                return;
            };
            retention_cutoff.min(earliest_future_probe)
        };
        let oldest_pending = self.timers.first_key_value().map(|(deadline, _)| {
            deadline
                .checked_sub(self.config.right_allowed_lateness_ms)
                .expect("validated temporal timer deadline cannot underflow")
        });
        let cutoff = oldest_pending.map_or(future_probe_cutoff, |pending| {
            future_probe_cutoff.min(pending)
        });
        if cutoff == i64::MIN {
            return;
        }
        self.history_evicted_before = Some(
            self.history_evicted_before
                .map_or(cutoff, |previous| previous.max(cutoff)),
        );
        if self.history_key_roster.is_empty() {
            self.history_gc_completed_cutoff = self.history_evicted_before;
            self.history_gc_active_cutoff = None;
            self.history_gc_cursor = 0;
            self.history_gc_sweep_end = 0;
        } else if self.history_gc_active_cutoff.is_none()
            && cutoff_is_newer(
                self.history_evicted_before,
                self.history_gc_completed_cutoff,
            )
        {
            self.history_gc_active_cutoff = self.history_evicted_before;
            self.history_gc_cursor = 0;
            self.history_gc_sweep_end = self.history_key_roster.len();
        }
    }

    pub(crate) fn has_history_gc_work(&self) -> bool {
        self.history_gc_active_cutoff.is_some()
            || cutoff_is_newer(
                self.history_evicted_before,
                self.history_gc_completed_cutoff,
            )
    }

    pub(crate) fn drain_history_gc(
        &mut self,
        max_steps: NonZeroUsize,
    ) -> Result<TemporalHistoryGcDrain, DbError> {
        if !self.has_history_gc_work() {
            return Ok(TemporalHistoryGcDrain {
                steps: 0,
                removed_versions: 0,
                has_more: false,
            });
        }
        if self.history_key_roster.is_empty() {
            self.history_gc_completed_cutoff = self.history_evicted_before;
            self.history_gc_active_cutoff = None;
            self.history_gc_cursor = 0;
            self.history_gc_sweep_end = 0;
            return Ok(TemporalHistoryGcDrain {
                steps: 0,
                removed_versions: 0,
                has_more: false,
            });
        }

        let active_cutoff = self
            .history_gc_active_cutoff
            .or(self.history_evicted_before)
            .ok_or_else(|| self.pipeline_error("history GC work has no requested cutoff"))?;
        let mut cursor = if self.history_gc_active_cutoff.is_some() {
            self.history_gc_cursor
        } else {
            0
        };
        let sweep_end = if self.history_gc_active_cutoff.is_some() {
            self.history_gc_sweep_end
        } else {
            self.history_key_roster.len()
        };
        if sweep_end == 0 || sweep_end > self.history_key_roster.len() || cursor >= sweep_end {
            return Err(self.pipeline_error("history GC cursor is outside its active key roster"));
        }

        let mut steps = 0usize;
        let mut removals = Vec::new();
        let mut last_planned_order: Option<(i64, TemporalSourcePosition)> = None;
        while steps < max_steps.get() && cursor < sweep_end {
            let key = self.history_key_roster[cursor].as_ref();
            let versions = self
                .history
                .get(key)
                .ok_or_else(|| self.pipeline_error("history GC roster referenced a missing key"))?;
            let mut entries = if let Some(order) = last_planned_order.as_ref() {
                versions.range((
                    std::ops::Bound::Excluded(order.clone()),
                    std::ops::Bound::Unbounded,
                ))
            } else {
                versions.range(..)
            };
            let (oldest_order, oldest_version) = entries.next().ok_or_else(|| {
                self.pipeline_error("history GC encountered an empty version chain")
            })?;
            let successor_is_below_cutoff = entries
                .next()
                .is_some_and(|((event_time, _), _)| *event_time < active_cutoff);
            if successor_is_below_cutoff {
                let order = oldest_order.clone();
                removals.push(HistoryGcRemoval {
                    roster_index: cursor,
                    order: order.clone(),
                    batch_id: oldest_version.row.map(|(batch_id, _)| batch_id),
                });
                last_planned_order = Some(order);
            } else {
                cursor += 1;
                last_planned_order = None;
            }
            steps += 1;
        }

        let mut batch_releases: FxHashMap<u64, usize> = FxHashMap::default();
        let mut removed_charge = 0usize;
        for removal in &removals {
            removed_charge = removed_charge
                .checked_add(
                    VERSION_ENTRY_CHARGE
                        .checked_add(removal.order.1.heap_bytes())
                        .ok_or_else(|| self.pipeline_error("history GC accounting overflow"))?,
                )
                .ok_or_else(|| self.pipeline_error("history GC accounting overflow"))?;
            if let Some(batch_id) = removal.batch_id {
                let releases = batch_releases.entry(batch_id).or_default();
                *releases = releases
                    .checked_add(1)
                    .ok_or_else(|| self.pipeline_error("history GC reference overflow"))?;
            }
        }
        for (&batch_id, &release_count) in &batch_releases {
            let retained = self.right_batches.get(&batch_id).ok_or_else(|| {
                self.pipeline_error("temporal version referenced a missing right batch")
            })?;
            if release_count > retained.references {
                return Err(
                    self.pipeline_error("temporal right batch reference count would underflow")
                );
            }
            if release_count == retained.references {
                removed_charge = removed_charge
                    .checked_add(batch_charge(&retained.batch).ok_or_else(|| {
                        self.pipeline_error("right retained-batch accounting overflow")
                    })?)
                    .ok_or_else(|| self.pipeline_error("history GC accounting overflow"))?;
            }
        }
        let next_charge = self
            .charged_bytes
            .checked_sub(removed_charge)
            .ok_or_else(|| self.pipeline_error("history GC accounting underflowed"))?;

        for removal in &removals {
            let key = self.history_key_roster[removal.roster_index].as_ref();
            let removed = self
                .history
                .get_mut(key)
                .expect("validated history GC key exists")
                .remove(&removal.order);
            assert!(removed.is_some(), "validated history GC version exists");
        }
        for (batch_id, release_count) in batch_releases {
            let remove_batch = {
                let retained = self
                    .right_batches
                    .get_mut(&batch_id)
                    .expect("validated history GC batch exists");
                retained.references -= release_count;
                retained.references == 0
            };
            if remove_batch {
                self.right_batches.remove(&batch_id);
            }
        }
        self.charged_bytes = next_charge;
        if cursor == sweep_end {
            self.history_gc_completed_cutoff = Some(
                self.history_gc_completed_cutoff
                    .map_or(active_cutoff, |completed| completed.max(active_cutoff)),
            );
            self.history_gc_active_cutoff = None;
            self.history_gc_cursor = 0;
            self.history_gc_sweep_end = 0;
        } else {
            self.history_gc_active_cutoff = Some(active_cutoff);
            self.history_gc_cursor = cursor;
            self.history_gc_sweep_end = sweep_end;
        }
        Ok(TemporalHistoryGcDrain {
            steps,
            removed_versions: removals.len(),
            has_more: self.has_history_gc_work(),
        })
    }

    fn allocate_batch_id(&mut self) -> Result<u64, DbError> {
        let id = self.next_batch_id;
        self.next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .ok_or_else(|| self.pipeline_error("temporal batch identity overflowed"))?;
        Ok(id)
    }

    fn admitted_replacement_charge(
        &self,
        growth: usize,
        released: usize,
        context: &str,
    ) -> Result<usize, DbError> {
        let accounted = self
            .charged_bytes
            .checked_sub(released)
            .and_then(|value| value.checked_add(growth))
            .ok_or_else(|| self.pipeline_error("temporal retained-state accounting overflow"))?;
        if accounted > self.config.limits.max_retained_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join vnode {} {context}", self.config.vnode),
                accounted_bytes: accounted,
                limit_bytes: self.config.limits.max_retained_bytes,
            });
        }
        Ok(accounted)
    }

    fn right_replay_frontier_charge(
        &self,
        partition: &[u8],
        frontier: &RightReplayFrontier,
    ) -> Result<usize, DbError> {
        MAP_ENTRY_CHARGE
            .checked_add(partition.len())
            .and_then(|value| value.checked_add(frontier.cursor.order.len()))
            .and_then(|value| {
                value.checked_add(frontier.identity.key.as_deref().map_or(0, <[u8]>::len))
            })
            .ok_or_else(|| self.pipeline_error("right replay-frontier accounting overflow"))
    }

    fn left_replay_frontier_charge(
        &self,
        partition: &[u8],
        frontier: &LeftReplayFrontier,
    ) -> Result<usize, DbError> {
        MAP_ENTRY_CHARGE
            .checked_add(partition.len())
            .and_then(|value| value.checked_add(frontier.cursor.order.len()))
            .and_then(|value| {
                value.checked_add(frontier.identity.key.as_deref().map_or(0, <[u8]>::len))
            })
            .ok_or_else(|| self.pipeline_error("left replay-frontier accounting overflow"))
    }

    fn validate_replayed_pending_left(
        &self,
        source: &TemporalSourcePosition,
        identity: &LeftRowIdentity,
    ) -> Result<(), DbError> {
        for &offset_ms in &self.offsets {
            let probe_identity = ProbeIdentity {
                source: source.clone(),
                offset_ms,
            };
            let Some(probe) = self.pending.get(&probe_identity) else {
                continue;
            };
            if identity.key.as_deref() != Some(probe.key.as_ref())
                || identity.event_time != Some(probe.left_event_time)
                || identity.payload_fingerprint != probe.payload_fingerprint
            {
                return Err(self.pipeline_error(
                    "a replayed left source position disagrees with its pending temporal probes",
                ));
            }
        }
        Ok(())
    }

    fn has_pending_left_source(&self, source: &TemporalSourcePosition) -> bool {
        let mut identity = ProbeIdentity {
            source: source.clone(),
            offset_ms: 0,
        };
        self.offsets.iter().any(|&offset_ms| {
            identity.offset_ms = offset_ms;
            self.pending.contains_key(&identity)
        })
    }

    fn restore_charge(&mut self) -> Result<(), DbError> {
        self.charged_bytes = calculate_charge(self)?;
        if self.charged_bytes > self.config.limits.max_retained_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join vnode {} retained state", self.config.vnode),
                accounted_bytes: self.charged_bytes,
                limit_bytes: self.config.limits.max_retained_bytes,
            });
        }
        Ok(())
    }

    fn pending_probe_charge(
        &self,
        identity: &ProbeIdentity,
        probe: &PendingProbe,
    ) -> Result<usize, DbError> {
        VERSION_ENTRY_CHARGE
            .checked_add(TIMER_ENTRY_CHARGE)
            .and_then(|value| value.checked_add(probe.key.len()))
            .and_then(|value| value.checked_add(identity.source.heap_bytes()))
            .and_then(|value| value.checked_add(identity.source.heap_bytes()))
            .ok_or_else(|| self.pipeline_error("pending-probe accounting overflow"))
    }

    fn pipeline_error(&self, message: &str) -> DbError {
        DbError::Pipeline(format!(
            "temporal join vnode {}: {message}",
            self.config.vnode
        ))
    }

    fn checkpoint_config(&self) -> Result<CheckpointConfig, DbError> {
        let to_u32 = |index: &usize| {
            u32::try_from(*index)
                .map_err(|_| DbError::Checkpoint("temporal column index does not fit u32".into()))
        };
        Ok(CheckpointConfig {
            vnode: self.config.vnode,
            vnode_count: self.config.vnode_count.get(),
            left_key_indices: self
                .config
                .left_key_indices
                .iter()
                .map(to_u32)
                .collect::<Result<_, _>>()?,
            right_key_indices: self
                .config
                .right_key_indices
                .iter()
                .map(to_u32)
                .collect::<Result<_, _>>()?,
            left_time_index: to_u32(&self.config.left_time_index)?,
            right_time_index: to_u32(&self.config.right_time_index)?,
            left_name: self.config.left_name.clone(),
            right_name: self.config.right_name.clone(),
            operator_name: self.config.operator_name.clone(),
            join_kind: match self.config.join_kind {
                TemporalJoinKind::Inner => 0,
                TemporalJoinKind::Left => 1,
            },
            offsets: self.offsets.clone(),
            emit_probe_metadata: self.config.emit_probe_metadata,
            left_allowed_lateness_ms: self.config.left_allowed_lateness_ms,
            right_allowed_lateness_ms: self.config.right_allowed_lateness_ms,
            history_retention_ms: self.config.history_retention_ms,
        })
    }

    fn validate_checkpoint_config(&self, archived: &CheckpointConfig) -> Result<(), DbError> {
        let expected = self.checkpoint_config()?;
        let matches = archived.vnode == expected.vnode
            && archived.vnode_count == expected.vnode_count
            && archived.left_key_indices == expected.left_key_indices
            && archived.right_key_indices == expected.right_key_indices
            && archived.left_time_index == expected.left_time_index
            && archived.right_time_index == expected.right_time_index
            && archived.left_name == expected.left_name
            && archived.right_name == expected.right_name
            && archived.operator_name == expected.operator_name
            && archived.join_kind == expected.join_kind
            && archived.offsets == expected.offsets
            && archived.emit_probe_metadata == expected.emit_probe_metadata
            && archived.left_allowed_lateness_ms == expected.left_allowed_lateness_ms
            && archived.right_allowed_lateness_ms == expected.right_allowed_lateness_ms
            && archived.history_retention_ms == expected.history_retention_ms;
        if !matches {
            return Err(DbError::Checkpoint(
                "temporal checkpoint does not match the planned operator".into(),
            ));
        }
        Ok(())
    }

    fn validate_checkpoint_shape(
        &self,
        checkpoint: &TemporalJoinCheckpoint,
    ) -> Result<(), DbError> {
        let entry_limit = self
            .config
            .limits
            .max_retained_bytes
            .checked_div(MAP_ENTRY_CHARGE)
            .unwrap_or(0)
            .saturating_add(1);
        if checkpoint.pending.len() > self.config.limits.max_pending_probes
            || checkpoint.versions.len() > entry_limit
            || checkpoint.right_replay_frontiers.len() > entry_limit
            || checkpoint.left_replay_frontiers.len() > entry_limit
            || checkpoint.history_key_roster.len() > entry_limit
        {
            return Err(DbError::Checkpoint(
                "temporal checkpoint exceeds configured state limits".into(),
            ));
        }
        if checkpoint.history_evicted_before.is_some_and(|floor| {
            checkpoint
                .right_frontier
                .is_none_or(|frontier| floor > frontier)
        }) {
            return Err(DbError::Checkpoint(
                "temporal checkpoint history floor is ahead of its right frontier".into(),
            ));
        }
        let roster_len = u64::try_from(checkpoint.history_key_roster.len())
            .map_err(|_| DbError::Checkpoint("temporal history GC roster exceeds u64".into()))?;
        if checkpoint
            .history_gc_completed_cutoff
            .is_some_and(|completed| {
                checkpoint
                    .history_evicted_before
                    .is_none_or(|requested| completed > requested)
            })
        {
            return Err(DbError::Checkpoint(
                "temporal completed history GC cutoff is ahead of its request".into(),
            ));
        }
        match checkpoint.history_gc_active_cutoff {
            Some(active) => {
                if checkpoint.history_gc_sweep_end == 0
                    || checkpoint.history_gc_sweep_end > roster_len
                    || checkpoint.history_gc_cursor >= checkpoint.history_gc_sweep_end
                    || checkpoint
                        .history_evicted_before
                        .is_none_or(|requested| active > requested)
                    || checkpoint
                        .history_gc_completed_cutoff
                        .is_some_and(|completed| active <= completed)
                {
                    return Err(DbError::Checkpoint(
                        "temporal active history GC cursor or cutoff is invalid".into(),
                    ));
                }
            }
            None => {
                if checkpoint.history_gc_cursor != 0 || checkpoint.history_gc_sweep_end != 0 {
                    return Err(DbError::Checkpoint(
                        "temporal inactive history GC retains a cursor".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn restore_right_replay_frontiers(
        &mut self,
        frontiers: Vec<CheckpointRightReplayFrontier>,
    ) -> Result<(), DbError> {
        for frontier in frontiers {
            if let Some(key) = frontier.key.as_deref() {
                self.validate_vnode(key)
                    .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            }
            let value = RightReplayFrontier {
                cursor: ReplayCursor {
                    order: frontier.order.into_boxed_slice(),
                    sub_offset: frontier.sub_offset,
                },
                identity: MutationIdentity {
                    key: frontier.key.map(Vec::into_boxed_slice),
                    event_time: frontier.event_time,
                    tombstone: frontier.tombstone,
                    payload_fingerprint: frontier.payload_fingerprint,
                },
            };
            if self
                .right_replay_frontiers
                .insert(frontier.partition.into_boxed_slice(), value)
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "duplicate right replay partition in temporal checkpoint".into(),
                ));
            }
        }
        Ok(())
    }

    fn restore_left_replay_frontiers(
        &mut self,
        frontiers: Vec<CheckpointLeftReplayFrontier>,
    ) -> Result<(), DbError> {
        for frontier in frontiers {
            if let Some(key) = frontier.key.as_deref() {
                self.validate_vnode(key)
                    .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            }
            let value = LeftReplayFrontier {
                cursor: ReplayCursor {
                    order: frontier.order.into_boxed_slice(),
                    sub_offset: frontier.sub_offset,
                },
                identity: LeftRowIdentity {
                    key: frontier.key.map(Vec::into_boxed_slice),
                    event_time: frontier.event_time,
                    payload_fingerprint: frontier.payload_fingerprint,
                },
            };
            if self
                .left_replay_frontiers
                .insert(frontier.partition.into_boxed_slice(), value)
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "duplicate left replay partition in temporal checkpoint".into(),
                ));
            }
        }
        Ok(())
    }

    fn checkpoint_versions(&self) -> Result<(Vec<CheckpointVersion>, RecordBatch), DbError> {
        let mut keys: Vec<&Box<[u8]>> = self.history.keys().collect();
        keys.sort_unstable_by(|left, right| left.as_ref().cmp(right.as_ref()));
        let mut rows = Vec::new();
        let version_count = self.history.values().map(BTreeMap::len).sum();
        let mut versions_out = Vec::with_capacity(version_count);
        for key in keys {
            for ((event_time, source), version) in &self.history[key] {
                let right_row = if let Some((batch, row)) = version.row {
                    let index = u32::try_from(rows.len()).map_err(|_| {
                        DbError::Checkpoint("temporal checkpoint has too many right rows".into())
                    })?;
                    rows.push(self.right_row(batch, row)?);
                    Some(index)
                } else {
                    None
                };
                versions_out.push(CheckpointVersion {
                    key: key.to_vec(),
                    event_time: *event_time,
                    source: source.clone(),
                    tombstone: version.row.is_none(),
                    right_row,
                });
            }
        }
        Ok((
            versions_out,
            compact_rows(&self.right_schema, &rows, "right")?,
        ))
    }

    fn checkpoint_pending(&self) -> Result<(Vec<CheckpointProbe>, RecordBatch), DbError> {
        let mut entries: Vec<_> = self.pending.iter().collect();
        entries.sort_unstable_by(|(left_id, left), (right_id, right)| {
            left.deadline
                .cmp(&right.deadline)
                .then_with(|| left_id.cmp(right_id))
        });
        let mut rows = Vec::with_capacity(entries.len());
        let mut pending_out = Vec::with_capacity(entries.len());
        for (identity, probe) in entries {
            let row = u32::try_from(rows.len()).map_err(|_| {
                DbError::Checkpoint("temporal checkpoint has too many pending rows".into())
            })?;
            rows.push(self.left_row(probe.left_batch, probe.left_row)?);
            pending_out.push(CheckpointProbe {
                source: identity.source.clone(),
                offset_ms: identity.offset_ms,
                left_row: row,
                key: probe.key.to_vec(),
                left_event_time: probe.left_event_time,
                probe_time: probe.probe_time,
                deadline: probe.deadline,
                payload_fingerprint: probe.payload_fingerprint,
            });
        }
        Ok((pending_out, compact_rows(&self.left_schema, &rows, "left")?))
    }

    fn restore_versions(
        &mut self,
        versions: Vec<CheckpointVersion>,
        rows: RecordBatch,
    ) -> Result<(), DbError> {
        if rows.schema().as_ref() != self.right_schema.as_ref() {
            return Err(DbError::Checkpoint(
                "temporal right checkpoint schema changed".into(),
            ));
        }
        let live = versions.iter().filter(|version| !version.tombstone).count();
        if rows.num_rows() != live {
            return Err(DbError::Checkpoint(
                "temporal right checkpoint row count disagrees with metadata".into(),
            ));
        }
        let rows = Arc::new(rows);
        let right_row_count = rows.num_rows();
        let row_positions = extract_source_positions(&rows)?;
        let row_keys = self.encode_keys(&rows, false)?;
        let row_times = extract_times(&rows, self.config.right_time_index, "right")?;
        let row_fingerprints = fingerprint_rows(&self.right_row_codec, &rows, "right")?;
        let key_columns = self.key_columns(&rows, false);
        let mut used_rows = FxHashSet::default();
        let mut retained_sources = FxHashSet::default();
        let batch_id = if live == 0 {
            None
        } else {
            Some(self.allocate_batch_id()?)
        };
        if let Some(batch_id) = batch_id {
            self.right_batches.insert(
                batch_id,
                RetainedBatch {
                    batch: Arc::clone(&rows),
                    references: live,
                },
            );
        }
        for version in versions {
            if !retained_sources.insert(version.source.clone()) {
                return Err(DbError::Checkpoint(
                    "temporal checkpoint retains one right source position more than once".into(),
                ));
            }
            self.validate_vnode(&version.key)
                .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            if version.tombstone != version.right_row.is_none() {
                return Err(DbError::Checkpoint(
                    "temporal tombstone checkpoint row is invalid".into(),
                ));
            }
            if version
                .right_row
                .is_some_and(|row| row as usize >= right_row_count)
            {
                return Err(DbError::Checkpoint(
                    "temporal right checkpoint row is out of bounds".into(),
                ));
            }
            let frontier = self
                .right_replay_frontiers
                .get(version.source.partition.as_slice())
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "temporal version is missing its right replay frontier".into(),
                    )
                })?;
            let frontier_order = frontier.cursor.compare(&version.source);
            if frontier_order == Ordering::Greater {
                return Err(DbError::Checkpoint(
                    "temporal version is ahead of its right replay frontier".into(),
                ));
            }
            if frontier_order == Ordering::Equal
                && (frontier.identity.key.as_deref() != Some(version.key.as_slice())
                    || frontier.identity.event_time != Some(version.event_time)
                    || frontier.identity.tombstone != version.tombstone)
            {
                return Err(DbError::Checkpoint(
                    "temporal version disagrees with its right replay frontier".into(),
                ));
            }
            if let Some(row) = version.right_row {
                if !used_rows.insert(row) {
                    return Err(DbError::Checkpoint(
                        "temporal right checkpoint row is referenced more than once".into(),
                    ));
                }
                let row = row as usize;
                if row_positions[row] != version.source
                    || key_columns.iter().any(|column| column.is_null(row))
                    || row_times.is_null(row)
                    || row_keys.row(row).as_ref() != version.key
                    || row_times.value(row) != version.event_time
                    || (frontier_order == Ordering::Equal
                        && row_fingerprints[row] != frontier.identity.payload_fingerprint)
                {
                    return Err(DbError::Checkpoint(
                        "temporal right checkpoint row disagrees with version metadata".into(),
                    ));
                }
            }
            let row = version
                .right_row
                .map(|row| (batch_id.expect("live batch exists"), row));
            let replaced = self
                .history
                .entry(version.key.into_boxed_slice())
                .or_default()
                .insert((version.event_time, version.source), Version { row });
            if replaced.is_some() {
                return Err(DbError::Checkpoint(
                    "duplicate temporal version in checkpoint".into(),
                ));
            }
        }
        if used_rows.len() != right_row_count {
            return Err(DbError::Checkpoint(
                "temporal right checkpoint contains an unreferenced row".into(),
            ));
        }
        Ok(())
    }

    fn restore_history_gc_roster(&mut self, roster: Vec<Vec<u8>>) -> Result<(), DbError> {
        if roster.len() != self.history.len() {
            return Err(DbError::Checkpoint(
                "temporal history GC roster does not cover every history key".into(),
            ));
        }
        let mut seen = FxHashSet::default();
        self.history_key_roster
            .try_reserve(roster.len())
            .map_err(|_| DbError::Checkpoint("temporal history GC roster is too large".into()))?;
        for key in roster {
            if !seen.insert(key.clone()) || !self.history.contains_key(key.as_slice()) {
                return Err(DbError::Checkpoint(
                    "temporal history GC roster contains a duplicate or unknown key".into(),
                ));
            }
            self.history_key_roster.push(key.into_boxed_slice());
        }
        self.validate_restored_history_gc_progress()
    }

    fn validate_restored_history_gc_progress(&self) -> Result<(), DbError> {
        if let Some(completed) = self.history_gc_completed_cutoff {
            for versions in self.history.values() {
                if versions
                    .keys()
                    .filter(|(event_time, _)| *event_time < completed)
                    .count()
                    > 1
                {
                    return Err(DbError::Checkpoint(
                        "temporal history retains unswept versions below its completed GC cutoff"
                            .into(),
                    ));
                }
            }
        }
        if let Some(active) = self.history_gc_active_cutoff {
            for key in &self.history_key_roster[..self.history_gc_cursor] {
                let versions = &self.history[key.as_ref()];
                if versions
                    .keys()
                    .filter(|(event_time, _)| *event_time < active)
                    .count()
                    > 1
                {
                    return Err(DbError::Checkpoint(
                        "temporal history GC cursor skips an unswept key".into(),
                    ));
                }
            }
            for key in &self.history_key_roster[self.history_gc_sweep_end..] {
                let versions = &self.history[key.as_ref()];
                if versions
                    .keys()
                    .filter(|(event_time, _)| *event_time < active)
                    .count()
                    > 1
                {
                    return Err(DbError::Checkpoint(
                        "temporal history appended after the active GC snapshot retains multiple pre-cutoff versions"
                            .into(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn restore_pending(
        &mut self,
        pending: Vec<CheckpointProbe>,
        rows: RecordBatch,
    ) -> Result<(), DbError> {
        if rows.schema().as_ref() != self.left_schema.as_ref() || rows.num_rows() != pending.len() {
            return Err(DbError::Checkpoint(
                "temporal pending checkpoint rows are invalid".into(),
            ));
        }
        if pending.len() > self.config.limits.max_pending_probes {
            return Err(DbError::Checkpoint(
                "temporal checkpoint exceeds pending-probe limit".into(),
            ));
        }
        let rows = Arc::new(rows);
        let left_row_count = rows.num_rows();
        let row_positions = extract_source_positions(&rows)?;
        let row_keys = self.encode_keys(&rows, true)?;
        let row_times = extract_times(&rows, self.config.left_time_index, "left")?;
        let row_fingerprints = fingerprint_rows(&self.left_row_codec, &rows, "left")?;
        let key_columns = self.key_columns(&rows, true);
        let mut used_rows = FxHashSet::default();
        let batch_id = if pending.is_empty() {
            None
        } else {
            Some(self.allocate_batch_id()?)
        };
        if let Some(batch_id) = batch_id {
            self.left_batches.insert(
                batch_id,
                RetainedBatch {
                    batch: Arc::clone(&rows),
                    references: pending.len(),
                },
            );
        }
        for probe in pending {
            self.validate_vnode(&probe.key)
                .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            if !self.offsets.contains(&probe.offset_ms) {
                return Err(DbError::Checkpoint(
                    "temporal pending probe uses an unplanned offset".into(),
                ));
            }
            if probe.left_event_time.checked_add(probe.offset_ms) != Some(probe.probe_time)
                || probe
                    .probe_time
                    .checked_add(self.config.right_allowed_lateness_ms)
                    != Some(probe.deadline)
            {
                return Err(DbError::Checkpoint(
                    "temporal pending-probe timing is invalid".into(),
                ));
            }
            if probe.left_row as usize >= left_row_count {
                return Err(DbError::Checkpoint(
                    "temporal pending checkpoint row is out of bounds".into(),
                ));
            }
            self.reject_evicted_probe(probe.probe_time)
                .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            if !used_rows.insert(probe.left_row) {
                return Err(DbError::Checkpoint(
                    "temporal pending checkpoint row is referenced more than once".into(),
                ));
            }
            let row = probe.left_row as usize;
            if row_positions[row] != probe.source
                || key_columns.iter().any(|column| column.is_null(row))
                || row_times.is_null(row)
                || row_keys.row(row).as_ref() != probe.key
                || row_times.value(row) != probe.left_event_time
                || row_fingerprints[row] != probe.payload_fingerprint
            {
                return Err(DbError::Checkpoint(
                    "temporal pending checkpoint row disagrees with probe metadata".into(),
                ));
            }
            let frontier = self
                .left_replay_frontiers
                .get(probe.source.partition.as_slice())
                .ok_or_else(|| {
                    DbError::Checkpoint(
                        "temporal pending probe is missing its left replay frontier".into(),
                    )
                })?;
            match frontier.cursor.compare(&probe.source) {
                Ordering::Greater => {
                    return Err(DbError::Checkpoint(
                        "temporal pending probe is ahead of its left replay frontier".into(),
                    ));
                }
                Ordering::Equal
                    if frontier.identity.key.as_deref() != Some(probe.key.as_slice())
                        || frontier.identity.event_time != Some(probe.left_event_time)
                        || frontier.identity.payload_fingerprint != probe.payload_fingerprint =>
                {
                    return Err(DbError::Checkpoint(
                        "temporal pending probe disagrees with its left replay frontier".into(),
                    ));
                }
                Ordering::Less | Ordering::Equal => {}
            }
            let identity = ProbeIdentity {
                source: probe.source,
                offset_ms: probe.offset_ms,
            };
            let inserted_timer = self
                .timers
                .entry(probe.deadline)
                .or_default()
                .insert(identity.clone());
            if !inserted_timer {
                return Err(DbError::Checkpoint(
                    "duplicate temporal timer in checkpoint".into(),
                ));
            }
            if self
                .pending
                .insert(
                    identity,
                    PendingProbe {
                        left_batch: batch_id.expect("pending batch exists"),
                        left_row: probe.left_row,
                        key: probe.key.into_boxed_slice(),
                        left_event_time: probe.left_event_time,
                        probe_time: probe.probe_time,
                        deadline: probe.deadline,
                        payload_fingerprint: probe.payload_fingerprint,
                    },
                )
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "duplicate pending probe in temporal checkpoint".into(),
                ));
            }
        }
        if used_rows.len() != left_row_count {
            return Err(DbError::Checkpoint(
                "temporal pending checkpoint contains an unreferenced row".into(),
            ));
        }
        Ok(())
    }

    fn validate_restored_probe_consistency(&self) -> Result<(), DbError> {
        let mut by_source: FxHashMap<TemporalSourcePosition, LeftRowIdentity> =
            FxHashMap::default();
        for (identity, probe) in &self.pending {
            let row = LeftRowIdentity {
                key: Some(probe.key.clone()),
                event_time: Some(probe.left_event_time),
                payload_fingerprint: probe.payload_fingerprint,
            };
            match by_source.entry(identity.source.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) if entry.get() != &row => {
                    return Err(DbError::Checkpoint(
                        "temporal pending horizons disagree on their left source row".into(),
                    ));
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(row);
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
        Ok(())
    }
}

fn validate_config(
    left: &Schema,
    right: &Schema,
    config: &TemporalJoinStateConfig,
) -> Result<(), DbError> {
    if config.vnode >= config.vnode_count.get() {
        return Err(DbError::Config(
            "temporal vnode is outside vnode count".into(),
        ));
    }
    if config.left_key_indices.is_empty()
        || config.left_key_indices.len() != config.right_key_indices.len()
    {
        return Err(DbError::Config(
            "temporal join requires paired equality keys".into(),
        ));
    }
    for (&left_index, &right_index) in config
        .left_key_indices
        .iter()
        .zip(&config.right_key_indices)
    {
        let left_field = left
            .fields()
            .get(left_index)
            .ok_or_else(|| DbError::Config("temporal left key index is out of bounds".into()))?;
        let right_field = right
            .fields()
            .get(right_index)
            .ok_or_else(|| DbError::Config("temporal right key index is out of bounds".into()))?;
        if left_field.data_type() != right_field.data_type() {
            return Err(DbError::Config(
                "temporal join key types must match exactly".into(),
            ));
        }
    }
    for (schema, index, side) in [
        (left, config.left_time_index, "left"),
        (right, config.right_time_index, "right"),
    ] {
        let field = schema.fields().get(index).ok_or_else(|| {
            DbError::Config(format!("temporal {side} time index is out of bounds"))
        })?;
        if !matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Millisecond, _)
        ) {
            return Err(DbError::Config(format!(
                "temporal {side} time must be Timestamp(Millisecond)"
            )));
        }
    }
    validate_position_schema(left, "left")?;
    validate_position_schema(right, "right")?;
    if config.left_name.is_empty()
        || config.right_name.is_empty()
        || config.operator_name.is_empty()
    {
        return Err(DbError::Config(
            "temporal operator and input names must not be empty".into(),
        ));
    }
    if config.left_allowed_lateness_ms < 0
        || config.right_allowed_lateness_ms < 0
        || config.history_retention_ms <= 0
    {
        return Err(DbError::Config(
            "temporal lateness must be nonnegative and retention must be positive".into(),
        ));
    }
    if config.history_retention_ms < config.right_allowed_lateness_ms {
        return Err(DbError::Config(
            "temporal history retention must cover right-side allowed lateness".into(),
        ));
    }
    if config.limits.max_retained_bytes == 0
        || config.limits.max_pending_probes == 0
        || config.limits.max_offsets_per_row == 0
        || config.limits.max_horizon_ms < 0
    {
        return Err(DbError::Config(
            "temporal state limits must be finite and positive".into(),
        ));
    }
    Ok(())
}

fn validate_position_schema(schema: &Schema, side: &str) -> Result<(), DbError> {
    let visible = schema
        .fields()
        .len()
        .checked_sub(POSITION_COLUMN_COUNT)
        .ok_or_else(|| {
            DbError::Config(format!(
                "temporal {side} input is missing trailing source positions"
            ))
        })?;
    let expected = [
        (SOURCE_PARTITION_COLUMN, DataType::Binary),
        (SOURCE_ORDER_COLUMN, DataType::Binary),
        (SOURCE_SUB_OFFSET_COLUMN, DataType::UInt32),
    ];
    if schema.fields()[..visible].iter().any(|field| {
        expected
            .iter()
            .any(|(name, _)| field.name().eq_ignore_ascii_case(name))
    }) {
        return Err(DbError::Config(format!(
            "temporal {side} input uses a reserved source-position name"
        )));
    }
    for (field, (name, data_type)) in schema.fields()[visible..].iter().zip(expected) {
        if field.name() != name || field.data_type() != &data_type || field.is_nullable() {
            return Err(DbError::Config(format!(
                "temporal {side} source positions must be the exact trailing typed fields"
            )));
        }
    }
    Ok(())
}

fn validate_output_names(
    left: &Schema,
    right: &Schema,
    config: &TemporalJoinStateConfig,
) -> Result<(), DbError> {
    let left_visible = left.fields().len() - POSITION_COLUMN_COUNT;
    let right_visible = right.fields().len() - POSITION_COLUMN_COUNT;
    let mut names = FxHashSet::default();
    for field in &left.fields()[..left_visible] {
        if !names.insert(field.name().to_ascii_lowercase()) {
            return Err(DbError::Config(format!(
                "temporal output column name collision: {}",
                field.name()
            )));
        }
    }
    for field in &right.fields()[..right_visible] {
        let name = format!("{}_{}", field.name(), config.right_name);
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(DbError::Config(format!(
                "temporal output column name collision: {name}"
            )));
        }
    }
    if config.emit_probe_metadata {
        for name in ["offset_ms", "probe_time"] {
            if !names.insert(name.to_owned()) {
                return Err(DbError::Config(format!(
                    "temporal output column name collision: {name}"
                )));
            }
        }
    }
    Ok(())
}

fn expand_offsets(
    schedule: &TemporalProbeSchedule,
    limits: TemporalStateLimits,
) -> Result<Vec<i64>, DbError> {
    let offsets = schedule.offsets_ms().to_vec();
    if offsets.is_empty() || offsets.len() > limits.max_offsets_per_row {
        return Err(DbError::Config(format!(
            "temporal schedule must contain 1..={} offsets",
            limits.max_offsets_per_row
        )));
    }
    for offset in &offsets {
        let magnitude = offset
            .checked_abs()
            .ok_or_else(|| DbError::Config("temporal offset magnitude overflowed".into()))?;
        if magnitude > limits.max_horizon_ms {
            return Err(DbError::Config(format!(
                "temporal offset {offset} exceeds maximum horizon"
            )));
        }
    }
    let unique: FxHashSet<i64> = offsets.iter().copied().collect();
    if unique.len() != offsets.len() {
        return Err(DbError::Config("temporal offsets must be unique".into()));
    }
    Ok(offsets)
}

pub(crate) fn temporal_join_output_schema(
    left: &Schema,
    right: &Schema,
    right_name: &str,
    join_kind: TemporalJoinKind,
    emit_probe_metadata: bool,
) -> Result<SchemaRef, DbError> {
    validate_position_schema(left, "left")?;
    validate_position_schema(right, "right")?;
    let left_visible = left.fields().len() - POSITION_COLUMN_COUNT;
    let right_visible = right.fields().len() - POSITION_COLUMN_COUNT;
    let mut fields = left.fields()[..left_visible].to_vec();
    fields.extend(right.fields()[..right_visible].iter().map(|field| {
        let renamed = field
            .as_ref()
            .clone()
            .with_name(format!("{}_{}", field.name(), right_name));
        Arc::new(if join_kind == TemporalJoinKind::Left {
            renamed.with_nullable(true)
        } else {
            renamed
        })
    }));
    if emit_probe_metadata {
        fields.push(Arc::new(Field::new("offset_ms", DataType::Int64, false)));
        fields.push(Arc::new(Field::new(
            "probe_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn extract_source_positions(batch: &RecordBatch) -> Result<Vec<TemporalSourcePosition>, DbError> {
    let partition = binary_column(batch, SOURCE_PARTITION_COLUMN)?;
    let order = binary_column(batch, SOURCE_ORDER_COLUMN)?;
    let sub_offset = batch
        .column_by_name(SOURCE_SUB_OFFSET_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| {
            DbError::Pipeline("temporal source sub-offset metadata is invalid".into())
        })?;
    if partition.null_count() != 0 || order.null_count() != 0 || sub_offset.null_count() != 0 {
        return Err(DbError::Pipeline(
            "temporal source positions must not contain nulls".into(),
        ));
    }
    Ok((0..batch.num_rows())
        .map(|row| TemporalSourcePosition {
            partition: partition.value(row).to_vec(),
            order: order.value(row).to_vec(),
            sub_offset: sub_offset.value(row),
        })
        .collect())
}

fn binary_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a BinaryArray, DbError> {
    batch
        .column_by_name(name)
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| {
            DbError::Pipeline(format!("temporal source position column {name} is invalid"))
        })
}

fn extract_times<'a>(
    batch: &'a RecordBatch,
    index: usize,
    side: &str,
) -> Result<&'a TimestampMillisecondArray, DbError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .ok_or_else(|| {
            DbError::Pipeline(format!(
                "temporal {side} time is not Timestamp(Millisecond)"
            ))
        })
}

fn row_codec(schema: &Schema, side: &str) -> Result<RowConverter, DbError> {
    RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )
    .map_err(|error| {
        DbError::Config(format!(
            "temporal {side} rows cannot be deterministically encoded: {error}"
        ))
    })
}

fn fingerprint_rows(
    codec: &RowConverter,
    batch: &RecordBatch,
    side: &str,
) -> Result<Vec<[u8; 16]>, DbError> {
    let rows = codec.convert_columns(batch.columns()).map_err(|error| {
        DbError::Pipeline(format!(
            "temporal {side} rows cannot be deterministically encoded: {error}"
        ))
    })?;
    Ok((0..batch.num_rows())
        .map(|row| xxh3_128(rows.row(row).as_ref()).to_le_bytes())
        .collect())
}

fn validate_frontier(previous: Option<i64>, next: Option<i64>, side: &str) -> Result<(), DbError> {
    if let (Some(previous), Some(next)) = (previous, next) {
        if next < previous {
            return Err(DbError::Pipeline(format!(
                "temporal {side} frontier regressed from {previous} to {next}"
            )));
        }
    }
    if previous.is_some() && next.is_none() {
        return Err(DbError::Pipeline(format!(
            "temporal {side} frontier cannot become uninitialized"
        )));
    }
    Ok(())
}

fn interleave_rows(
    schema: &Schema,
    rows: &[Option<RowRef>],
    column_count: usize,
    nullable: bool,
    side: &str,
) -> Result<Vec<ArrayRef>, DbError> {
    let mut batches = Vec::<Arc<RecordBatch>>::new();
    let mut by_pointer = FxHashMap::<usize, usize>::default();
    let mut positions = Vec::with_capacity(rows.len());
    let needs_null = rows.iter().any(Option::is_none);
    for row in rows {
        if let Some(row) = row {
            let pointer = Arc::as_ptr(&row.batch) as usize;
            let batch_index = *by_pointer.entry(pointer).or_insert_with(|| {
                let index = batches.len();
                batches.push(Arc::clone(&row.batch));
                index
            });
            positions.push((batch_index, row.row as usize));
        } else {
            positions.push((usize::MAX, 0));
        }
    }
    let null_index = batches.len();
    let resolved: Vec<(usize, usize)> = positions
        .iter()
        .map(|&(batch, row)| {
            if batch == usize::MAX {
                (null_index, 0)
            } else {
                (batch, row)
            }
        })
        .collect();
    let mut columns = Vec::with_capacity(column_count);
    for (column_index, field) in schema.fields().iter().take(column_count).enumerate() {
        let null = needs_null.then(|| new_null_array(field.data_type(), 1));
        let mut arrays: Vec<&dyn Array> = batches
            .iter()
            .map(|batch| batch.column(column_index).as_ref())
            .collect();
        if let Some(null) = &null {
            arrays.push(null.as_ref());
        }
        columns.push(
            arrow::compute::interleave(&arrays, &resolved).map_err(|error| {
                DbError::query_pipeline_arrow(format!("temporal join {side} output"), &error)
            })?,
        );
    }
    debug_assert!(nullable || !needs_null);
    Ok(columns)
}

fn compact_rows(schema: &Schema, rows: &[RowRef], side: &str) -> Result<RecordBatch, DbError> {
    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(schema.clone())));
    }
    let optional: Vec<Option<RowRef>> = rows.iter().cloned().map(Some).collect();
    let columns = interleave_rows(schema, &optional, schema.fields().len(), false, side)?;
    RecordBatch::try_new(Arc::new(schema.clone()), columns)
        .map_err(|error| DbError::Checkpoint(format!("temporal {side} compaction: {error}")))
}

fn batch_charge(batch: &RecordBatch) -> Option<usize> {
    batch.get_array_memory_size().checked_add(BATCH_CHARGE)
}

fn cutoff_is_newer(candidate: Option<i64>, completed: Option<i64>) -> bool {
    candidate.is_some_and(|candidate| completed.is_none_or(|completed| candidate > completed))
}

fn calculate_charge(state: &TemporalJoinVnodeState) -> Result<usize, DbError> {
    let mut bytes = BASE_STATE_CHARGE;
    for (key, versions) in &state.history {
        bytes = bytes
            .checked_add(
                MAP_ENTRY_CHARGE
                    .checked_add(key.len())
                    .ok_or_else(|| state.pipeline_error("history accounting overflow"))?,
            )
            .ok_or_else(|| state.pipeline_error("history accounting overflow"))?;
        for (_, source) in versions.keys() {
            bytes = bytes
                .checked_add(
                    VERSION_ENTRY_CHARGE
                        .checked_add(source.heap_bytes())
                        .ok_or_else(|| state.pipeline_error("history accounting overflow"))?,
                )
                .ok_or_else(|| state.pipeline_error("history accounting overflow"))?;
        }
    }
    for (partition, frontier) in &state.right_replay_frontiers {
        bytes = bytes
            .checked_add(state.right_replay_frontier_charge(partition, frontier)?)
            .ok_or_else(|| state.pipeline_error("right replay-frontier accounting overflow"))?;
    }
    for (partition, frontier) in &state.left_replay_frontiers {
        bytes = bytes
            .checked_add(state.left_replay_frontier_charge(partition, frontier)?)
            .ok_or_else(|| state.pipeline_error("left replay-frontier accounting overflow"))?;
    }
    for key in &state.history_key_roster {
        bytes = bytes
            .checked_add(
                HISTORY_KEY_ROSTER_CHARGE
                    .checked_add(key.len())
                    .ok_or_else(|| state.pipeline_error("history roster accounting overflow"))?,
            )
            .ok_or_else(|| state.pipeline_error("history roster accounting overflow"))?;
    }
    for retained in state
        .right_batches
        .values()
        .chain(state.left_batches.values())
    {
        bytes = bytes
            .checked_add(
                batch_charge(&retained.batch)
                    .ok_or_else(|| state.pipeline_error("batch accounting overflow"))?,
            )
            .ok_or_else(|| state.pipeline_error("batch accounting overflow"))?;
    }
    for (identity, probe) in &state.pending {
        bytes = bytes
            .checked_add(state.pending_probe_charge(identity, probe)?)
            .ok_or_else(|| state.pipeline_error("pending-probe accounting overflow"))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, TimestampMillisecondArray, UInt8Array};
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, source_mutations,
    };

    fn schema(prefix: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new(format!("{prefix}_value"), DataType::Int64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new(SOURCE_PARTITION_COLUMN, DataType::Binary, false),
            Field::new(SOURCE_ORDER_COLUMN, DataType::Binary, false),
            Field::new(SOURCE_SUB_OFFSET_COLUMN, DataType::UInt32, false),
        ]))
    }

    fn batch(
        schema: SchemaRef,
        keys: Vec<Option<&str>>,
        values: Vec<Option<i64>>,
        times: Vec<Option<i64>>,
        orders: Vec<u8>,
    ) -> RecordBatch {
        let rows = keys.len();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(values)),
                Arc::new(TimestampMillisecondArray::from(times)),
                Arc::new(BinaryArray::from_iter_values(std::iter::repeat_n(
                    b"p0".as_slice(),
                    rows,
                ))),
                Arc::new(BinaryArray::from_iter_values(
                    orders.into_iter().map(|order| vec![order]),
                )),
                Arc::new(UInt32Array::from_iter_values(0..rows as u32)),
            ],
        )
        .unwrap()
    }

    fn mutation_metadata(batch: &RecordBatch, operations: &[SourceMutation]) -> RecordBatch {
        let visible_columns = batch.num_columns() - POSITION_COLUMN_COUNT;
        let visible_schema = Arc::new(Schema::new_with_metadata(
            batch.schema().fields()[..visible_columns].to_vec(),
            batch.schema().metadata().clone(),
        ));
        let schema = schema_with_source_mutations_and_row_positions(&visible_schema).unwrap();
        let mut columns = batch.columns()[..visible_columns].to_vec();
        columns.push(Arc::new(UInt8Array::from_iter_values(
            operations.iter().map(|operation| match operation {
                SourceMutation::Put => 0,
                SourceMutation::Tombstone => 1,
            }),
        )));
        columns.extend_from_slice(&batch.columns()[visible_columns..]);
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn with_values(batch: &RecordBatch, values: Vec<Option<i64>>) -> RecordBatch {
        let mut columns = batch.columns().to_vec();
        columns[1] = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(batch.schema(), columns).unwrap()
    }

    fn config(kind: TemporalJoinKind, schedule: TemporalProbeSchedule) -> TemporalJoinStateConfig {
        TemporalJoinStateConfig {
            vnode: 0,
            vnode_count: NonZeroU32::new(1).unwrap(),
            left_key_indices: vec![0],
            right_key_indices: vec![0],
            key_codec: Arc::new(PartitionKeyCodecV1::try_new([DataType::Utf8]).unwrap()),
            left_time_index: 2,
            right_time_index: 2,
            left_name: "trades".into(),
            right_name: "quotes".into(),
            operator_name: "trade_quote_asof".into(),
            join_kind: kind,
            emit_probe_metadata: schedule.is_multi_horizon(),
            schedule,
            left_allowed_lateness_ms: 0,
            right_allowed_lateness_ms: 0,
            history_retention_ms: 10_000,
            limits: TemporalStateLimits {
                max_retained_bytes: 4 * 1024 * 1024,
                max_pending_probes: 100,
                max_offsets_per_row: 16,
                max_horizon_ms: 60_000,
            },
        }
    }

    fn state(kind: TemporalJoinKind, schedule: TemporalProbeSchedule) -> TemporalJoinVnodeState {
        TemporalJoinVnodeState::try_new(schema("left"), schema("right"), config(kind, schedule))
            .unwrap()
    }

    fn prices(output: &RecordBatch) -> Vec<Option<i64>> {
        let index = output.schema().index_of("right_value_quotes").unwrap();
        output
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn predecessor_equal_time_position_and_tombstone_are_deterministic() {
        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let right = batch(
            schema("right"),
            vec![Some("A"), Some("A"), Some("A")],
            vec![Some(10), Some(11), None],
            vec![Some(100), Some(100), Some(200)],
            vec![1, 2, 3],
        );
        let operations = [
            SourceMutation::Put,
            SourceMutation::Put,
            SourceMutation::Tombstone,
        ];
        let metadata = mutation_metadata(&right, &operations);
        let operations = source_mutations(&metadata).unwrap();
        assert_eq!(
            state
                .apply_right_batch(&right, operations)
                .unwrap()
                .inserted,
            3
        );
        assert_eq!(
            state
                .apply_right_batch(&right, operations)
                .unwrap()
                .duplicates,
            3
        );
        state.advance_right_frontier(Some(1_000), false).unwrap();
        assert_eq!(
            state
                .apply_right_batch(&right, operations)
                .unwrap()
                .duplicates,
            3
        );
        let changed = batch(
            schema("right"),
            vec![Some("A"), Some("A"), Some("A")],
            vec![Some(10), Some(11), Some(99)],
            vec![Some(100), Some(100), Some(200)],
            vec![1, 2, 3],
        );
        assert!(state
            .apply_right_batch(&changed, operations)
            .unwrap_err()
            .to_string()
            .contains("replayed with different temporal data"));
        let left = batch(
            schema("left"),
            vec![Some("A"), Some("A"), None, Some("A")],
            vec![Some(1); 4],
            vec![Some(100), Some(150), Some(150), Some(250)],
            vec![10, 11, 12, 13],
        );
        let output = state.probe_left_batch(&left).unwrap();
        assert_eq!(prices(&output), vec![Some(11), Some(11), None, None]);
    }

    #[test]
    fn compact_replay_frontiers_plateau_with_lifetime_records() {
        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let mut plateau = None;
        for order in 1..=64_u8 {
            let right = batch(
                schema("right"),
                vec![None],
                vec![Some(1)],
                vec![None],
                vec![order],
            );
            assert_eq!(
                state.apply_right_batch(&right, None).unwrap().ignored_nulls,
                1
            );
            let left = batch(
                schema("left"),
                vec![None],
                vec![Some(1)],
                vec![None],
                vec![order],
            );
            assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 1);
            let charged = state.accounted_state_bytes();
            if let Some(expected) = plateau {
                assert_eq!(charged, expected);
            } else {
                plateau = Some(charged);
            }
        }
        assert_eq!(state.right_replay_frontiers.len(), 1);
        assert_eq!(state.left_replay_frontiers.len(), 1);
        assert_eq!(state.retained_versions(), 0);
        assert_eq!(state.pending_probes(), 0);
        assert_eq!(
            state.accounted_state_bytes(),
            calculate_charge(&state).unwrap()
        );
    }

    #[test]
    fn replay_frontier_validates_current_and_skips_older_cursor() {
        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let right = batch(
            schema("right"),
            vec![Some("A"), Some("A")],
            vec![Some(1), Some(2)],
            vec![Some(10), Some(20)],
            vec![1, 2],
        );
        state.apply_right_batch(&right, None).unwrap();
        let older_changed = with_values(&right.slice(0, 1), vec![Some(99)]);
        assert_eq!(
            state
                .apply_right_batch(&older_changed, None)
                .unwrap()
                .duplicates,
            1
        );
        let current = right.slice(1, 1);
        assert_eq!(
            state.apply_right_batch(&current, None).unwrap().duplicates,
            1
        );
        let current_changed = with_values(&current, vec![Some(99)]);
        assert!(state
            .apply_right_batch(&current_changed, None)
            .unwrap_err()
            .to_string()
            .contains("replayed with different temporal data"));

        state.advance_right_frontier(Some(1_000), false).unwrap();
        let left = batch(
            schema("left"),
            vec![Some("A"), Some("A")],
            vec![Some(1), Some(2)],
            vec![Some(10), Some(20)],
            vec![3, 4],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 2);
        assert_eq!(
            state
                .probe_left_batch(&with_values(&left.slice(0, 1), vec![Some(99)]))
                .unwrap()
                .num_rows(),
            0
        );
        let current = left.slice(1, 1);
        assert_eq!(state.probe_left_batch(&current).unwrap().num_rows(), 0);
        assert!(state
            .probe_left_batch(&with_values(&current, vec![Some(99)]))
            .unwrap_err()
            .to_string()
            .contains("replayed with different temporal data"));
    }

    #[test]
    fn older_left_replay_validates_retained_pending_horizons() {
        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let left = batch(
            schema("left"),
            vec![Some("A"), Some("A")],
            vec![Some(1), Some(2)],
            vec![Some(10), Some(20)],
            vec![1, 2],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
        assert_eq!(state.pending_probes(), 2);
        assert_eq!(
            state
                .probe_left_batch(&left.slice(0, 1))
                .unwrap()
                .num_rows(),
            0
        );
        assert!(state
            .probe_left_batch(&with_values(&left.slice(0, 1), vec![Some(99)]))
            .unwrap_err()
            .to_string()
            .contains("disagrees with its pending temporal probes"));
        assert_eq!(state.pending_probes(), 2);
    }

    #[test]
    fn source_order_regression_within_batch_is_atomic() {
        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let right = batch(
            schema("right"),
            vec![Some("A"), Some("A")],
            vec![Some(1), Some(2)],
            vec![Some(10), Some(20)],
            vec![2, 1],
        );
        assert!(state
            .apply_right_batch(&right, None)
            .unwrap_err()
            .to_string()
            .contains("regressed within one input batch"));
        assert!(state.right_replay_frontiers.is_empty());
        assert_eq!(state.retained_versions(), 0);
        assert_eq!(state.accounted_state_bytes(), BASE_STATE_CHARGE);
    }

    #[test]
    fn idle_left_uses_finite_retention_and_rejects_old_revival_probe() {
        let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        cfg.history_retention_ms = 50;
        let mut state =
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg).unwrap();
        let right = batch(
            schema("right"),
            vec![Some("A"), Some("A"), Some("A")],
            vec![Some(1), Some(2), Some(3)],
            vec![Some(10), Some(20), Some(100)],
            vec![1, 2, 3],
        );
        state.apply_right_batch(&right, None).unwrap();
        state.advance_left_frontier(Some(20), true).unwrap();
        state.advance_right_frontier(Some(200), false).unwrap();
        assert_eq!(state.history_evicted_before, Some(150));
        while state.has_history_gc_work() {
            state
                .drain_history_gc(NonZeroUsize::new(16).unwrap())
                .unwrap();
        }
        assert_eq!(state.retained_versions(), 1);

        let revival = batch(
            schema("left"),
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(140)],
            vec![10],
        );
        assert!(state
            .probe_left_batch(&revival)
            .unwrap_err()
            .to_string()
            .contains("older than retained history"));
        assert!(state.left_replay_frontiers.is_empty());
    }

    #[test]
    fn inner_join_omits_nulls_and_missing_versions() {
        let mut state = state(TemporalJoinKind::Inner, TemporalProbeSchedule::as_of());
        state.advance_right_frontier(Some(1_000), false).unwrap();
        let left = batch(
            schema("left"),
            vec![Some("missing"), None],
            vec![Some(1), Some(2)],
            vec![Some(100), None],
            vec![1, 2],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
    }

    #[test]
    fn multi_horizon_null_key_keeps_probe_time_without_hidden_positions() {
        let mut state = state(
            TemporalJoinKind::Left,
            TemporalProbeSchedule::list(vec![0, 5]).unwrap(),
        );
        let left = batch(
            schema("left"),
            vec![None],
            vec![Some(1)],
            vec![Some(100)],
            vec![1],
        );
        let output = state.probe_left_batch(&left).unwrap();
        let probe_times = output
            .column_by_name("probe_time")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        assert_eq!(probe_times, vec![Some(100), Some(105)]);
        assert!(output.column_by_name(SOURCE_PARTITION_COLUMN).is_none());
        assert!(output.column_by_name(SOURCE_ORDER_COLUMN).is_none());
        assert!(output.column_by_name(SOURCE_SUB_OFFSET_COLUMN).is_none());
    }

    #[test]
    fn list_and_range_share_state_and_timestamp_addition_is_checked() {
        let limits = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of()).limits;
        assert_eq!(
            expand_offsets(
                &TemporalProbeSchedule::list(vec![5, 15, -5]).unwrap(),
                limits
            )
            .unwrap(),
            vec![5, 15, -5]
        );
        assert_eq!(
            expand_offsets(&TemporalProbeSchedule::range(-5, 5, 5).unwrap(), limits).unwrap(),
            vec![-5, 0, 5]
        );
        let mut overflow_state = state(
            TemporalJoinKind::Left,
            TemporalProbeSchedule::list(vec![1]).unwrap(),
        );
        let left = batch(
            schema("left"),
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(i64::MAX)],
            vec![1],
        );
        assert!(overflow_state
            .probe_left_batch(&left)
            .unwrap_err()
            .to_string()
            .contains("overflowed"));

        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let left = batch(
            schema("left"),
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(i64::MAX)],
            vec![2],
        );
        assert!(state
            .probe_left_batch(&left)
            .unwrap_err()
            .to_string()
            .contains("finite frontier"));

        let mut fields = schema("left").fields().to_vec();
        fields[2] = Arc::new(Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ));
        assert!(TemporalJoinVnodeState::try_new(
            Arc::new(Schema::new(fields)),
            schema("right"),
            config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of()),
        )
        .err()
        .unwrap()
        .to_string()
        .contains("Timestamp(Millisecond)"));

        let mut invalid = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        invalid.right_allowed_lateness_ms = 101;
        invalid.history_retention_ms = 100;
        assert!(
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), invalid)
                .err()
                .unwrap()
                .to_string()
                .contains("retention must cover")
        );

        let schedule = TemporalProbeSchedule::list(vec![0, 1]).unwrap();
        let mut fields = schema("left").fields().to_vec();
        fields[0] = Arc::new(Field::new("OFFSET_MS", DataType::Utf8, true));
        assert!(TemporalJoinVnodeState::try_new(
            Arc::new(Schema::new(fields)),
            schema("right"),
            config(TemporalJoinKind::Left, schedule),
        )
        .err()
        .unwrap()
        .to_string()
        .contains("output column name collision"));

        let mut below_base = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        below_base.limits.max_retained_bytes = BASE_STATE_CHARGE - 1;
        assert!(matches!(
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), below_base),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
    }

    #[test]
    fn right_frontier_finalizes_buffered_probe() {
        let mut state = state(
            TemporalJoinKind::Left,
            TemporalProbeSchedule::list(vec![50]).unwrap(),
        );
        let right = batch(
            schema("right"),
            vec![Some("A")],
            vec![Some(7)],
            vec![Some(120)],
            vec![1],
        );
        state.apply_right_batch(&right, None).unwrap();
        let left = batch(
            schema("left"),
            vec![Some("A"), Some("A")],
            vec![Some(1), Some(2)],
            vec![Some(100), Some(100)],
            vec![2, 3],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
        assert_eq!(state.pending_probes(), 2);
        assert_eq!(state.pending_watermark_hold(), Some(100));
        state.advance_right_frontier(Some(149), false).unwrap();
        assert!(!state.has_ready_probes());
        state.advance_right_frontier(Some(150), false).unwrap();
        assert!(!state.has_ready_probes());
        state.advance_right_frontier(Some(151), false).unwrap();
        assert!(state.has_ready_probes());
        let drained = state
            .drain_ready_probes(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(prices(&drained.output), vec![Some(7)]);
        assert_eq!(drained.drained_probes, 1);
        assert!(drained.has_more);
        assert_eq!(
            state.accounted_state_bytes(),
            calculate_charge(&state).unwrap()
        );
        let drained = state
            .drain_ready_probes(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(prices(&drained.output), vec![Some(7)]);
        assert_eq!(drained.drained_probes, 1);
        assert!(!drained.has_more);
        assert_eq!(state.pending_probes(), 0);
        assert_eq!(state.pending_watermark_hold(), None);
        assert_eq!(
            state.accounted_state_bytes(),
            calculate_charge(&state).unwrap()
        );
    }

    #[test]
    fn retention_preserves_one_predecessor_anchor() {
        let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        cfg.history_retention_ms = 50;
        let mut state =
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
        let right = batch(
            schema("right"),
            vec![Some("A"), Some("A"), Some("A"), Some("B"), Some("B")],
            vec![Some(1), Some(2), Some(3), Some(4), Some(5)],
            vec![Some(10), Some(20), Some(100), Some(10), Some(20)],
            vec![1, 2, 3, 4, 5],
        );
        state.apply_right_batch(&right, None).unwrap();
        state.advance_right_frontier(Some(120), false).unwrap();
        assert_eq!(state.retained_versions(), 5);
        state.advance_left_frontier(Some(120), false).unwrap();
        assert_eq!(state.retained_versions(), 5);
        assert!(state.has_history_gc_work());
        let drained = state
            .drain_history_gc(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(drained.steps, 1);
        assert_eq!(drained.removed_versions, 1);
        assert!(drained.has_more);
        assert_eq!(state.retained_versions(), 4);
        assert_eq!(
            state.accounted_state_bytes(),
            calculate_charge(&state).unwrap()
        );

        let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
        let mut corrupted =
            rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(&checkpoint).unwrap();
        corrupted.history_gc_sweep_end = 1;
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(4 * 1024 * 1024),
        );
        let corrupted = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&corrupted, writer)
            .unwrap()
            .into_inner()
            .into_vec();
        assert!(TemporalJoinVnodeState::restore(
            schema("left"),
            schema("right"),
            cfg.clone(),
            &corrupted,
        )
        .err()
        .unwrap()
        .to_string()
        .contains("appended after the active GC snapshot"));

        let mut restored =
            TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint)
                .unwrap();
        assert!(restored.has_history_gc_work());
        let drained = restored
            .drain_history_gc(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(drained.steps, 1);
        assert_eq!(drained.removed_versions, 0);
        assert!(drained.has_more);
        let drained = restored
            .drain_history_gc(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(drained.removed_versions, 1);
        assert!(drained.has_more);
        let drained = restored
            .drain_history_gc(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(drained.removed_versions, 0);
        assert!(!drained.has_more);
        assert_eq!(restored.retained_versions(), 3);
    }

    #[test]
    fn checkpoint_restores_compact_replay_frontier_across_history_gc() {
        let mut cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        cfg.history_retention_ms = 50;
        let mut state =
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
        let right = batch(
            schema("right"),
            vec![Some("A"), Some("A")],
            vec![Some(1), Some(2)],
            vec![Some(10), Some(100)],
            vec![1, 2],
        );
        state.apply_right_batch(&right, None).unwrap();
        state.advance_right_frontier(Some(200), false).unwrap();
        let left = batch(
            schema("left"),
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(100)],
            vec![3],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 1);
        state.advance_left_frontier(Some(200), false).unwrap();
        assert_eq!(state.retained_versions(), 2);
        assert!(state.has_history_gc_work());

        let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
        let mut restored =
            TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint)
                .unwrap();
        assert_eq!(restored.probe_left_batch(&left).unwrap().num_rows(), 0);
        assert_eq!(restored.left_replay_frontiers.len(), 1);
        assert_eq!(restored.retained_versions(), 2);
        assert!(restored.has_history_gc_work());
        let first = restored
            .drain_history_gc(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(first.removed_versions, 1);
        assert!(first.has_more);
        let second = restored
            .drain_history_gc(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(second.removed_versions, 0);
        assert!(!second.has_more);
        assert_eq!(restored.retained_versions(), 1);
    }

    #[test]
    fn pending_key_bytes_are_rejected_before_state_mutation() {
        let mut state = state(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let left = batch(
            schema("left"),
            vec![Some(&"x".repeat(4_096))],
            vec![Some(1)],
            vec![Some(100)],
            vec![1],
        );
        let encoded = state.encode_keys(&left, true).unwrap();
        let old_growth = VERSION_ENTRY_CHARGE
            + TIMER_ENTRY_CHARGE
            + left.get_array_memory_size()
            + BATCH_CHARGE
            + 6;
        state.config.limits.max_retained_bytes = state.accounted_state_bytes() + old_growth;
        let before = state.accounted_state_bytes();

        assert!(matches!(
            state.probe_left_batch(&left),
            Err(DbError::ManagedStateBudgetExceeded { .. })
        ));
        assert!(!encoded.row(0).as_ref().is_empty());
        assert_eq!(state.pending_probes(), 0);
        assert!(state.left_replay_frontiers.is_empty());
        assert!(state.timers.is_empty());
        assert_eq!(state.accounted_state_bytes(), before);
    }

    #[test]
    fn restore_rejects_pending_probe_ahead_of_left_replay_frontier() {
        let cfg = config(TemporalJoinKind::Left, TemporalProbeSchedule::as_of());
        let mut state =
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
        let left = batch(
            schema("left"),
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(100)],
            vec![1],
        );
        state.probe_left_batch(&left).unwrap();
        let bytes = state.checkpoint(4 * 1024 * 1024).unwrap();
        let mut decoded =
            rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(&bytes).unwrap();
        decoded.left_replay_frontiers[0].order.clear();
        let writer = rkyv::ser::writer::IoWriter::new(
            laminar_core::serialization::BoundedBytesWriter::new(4 * 1024 * 1024),
        );
        let corrupted = rkyv::api::high::to_bytes_in::<_, rkyv::rancor::Error>(&decoded, writer)
            .unwrap()
            .into_inner()
            .into_vec();

        assert!(
            TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &corrupted,)
                .err()
                .unwrap()
                .to_string()
                .contains("pending probe is ahead of its left replay frontier")
        );
    }

    #[test]
    fn checkpoint_roundtrip_restores_history_pending_timer_and_frontiers() {
        let cfg = config(
            TemporalJoinKind::Left,
            TemporalProbeSchedule::list(vec![50]).unwrap(),
        );
        let mut state =
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
        let right = batch(
            schema("right"),
            vec![Some("A")],
            vec![Some(9)],
            vec![Some(120)],
            vec![1],
        );
        state.apply_right_batch(&right, None).unwrap();
        let left = batch(
            schema("left"),
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(100)],
            vec![2],
        );
        state.probe_left_batch(&left).unwrap();
        state.advance_left_frontier(Some(90), true).unwrap();
        state.advance_right_frontier(Some(151), false).unwrap();
        assert!(state.has_ready_probes());
        let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
        let mut restored =
            TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint)
                .unwrap();
        assert_eq!(restored.retained_versions(), 1);
        assert_eq!(restored.right_replay_frontiers.len(), 1);
        assert_eq!(restored.left_replay_frontiers.len(), 1);
        assert_eq!(restored.pending_probes(), 1);
        assert_eq!(restored.pending_watermark_hold(), Some(100));
        assert!(restored.has_ready_probes());
        assert_eq!(
            restored.apply_right_batch(&right, None).unwrap().duplicates,
            1
        );
        assert_eq!(restored.probe_left_batch(&left).unwrap().num_rows(), 0);
        assert_eq!(restored.pending_probes(), 1);
        let drained = restored
            .drain_ready_probes(NonZeroUsize::new(1).unwrap())
            .unwrap();
        assert_eq!(prices(&drained.output), vec![Some(9)]);
    }
}
