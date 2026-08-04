//! Vnode-local state for event-time temporal joins.

use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, Int64Array, RecordBatch,
    TimestampMillisecondArray, UInt32Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::row::{RowConverter, SortField};
use laminar_connectors::connector::{
    SourceMutation, SOURCE_ORDER_KEY_COLUMN as SOURCE_ORDER_COLUMN, SOURCE_PARTITION_COLUMN,
    SOURCE_SUB_OFFSET_COLUMN,
};
use laminar_core::serialization::{deserialize_batch_stream, serialize_batches_stream_bounded};
use laminar_core::state::PartitionKeyCodecV1;
use laminar_sql::temporal::{TemporalJoinKind, TemporalProbeSchedule};
use rustc_hash::{FxHashMap, FxHashSet};
use sha2::{Digest, Sha256};

use crate::error::DbError;

const FORMAT_VERSION: u8 = 1;
const MAP_ENTRY_CHARGE: usize = 128;
const VERSION_ENTRY_CHARGE: usize = 256;
const TIMER_ENTRY_CHARGE: usize = 96;
const BATCH_CHARGE: usize = 256;
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
    payload_fingerprint: [u8; 32],
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
    payload_fingerprint: [u8; 32],
}

#[derive(Clone, PartialEq, Eq)]
struct FinalizedProbe {
    key: Option<Box<[u8]>>,
    left_event_time: Option<i64>,
    payload_fingerprint: [u8; 32],
}

struct OutputRow {
    left: RowRef,
    right: Option<RowRef>,
    offset_ms: i64,
    probe_time: Option<i64>,
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
struct CheckpointMutationIdentity {
    source: TemporalSourcePosition,
    key: Option<Vec<u8>>,
    event_time: Option<i64>,
    tombstone: bool,
    payload_fingerprint: [u8; 32],
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
    payload_fingerprint: [u8; 32],
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct CheckpointFinalizedProbe {
    source: TemporalSourcePosition,
    offset_ms: i64,
    key: Option<Vec<u8>>,
    left_event_time: Option<i64>,
    payload_fingerprint: [u8; 32],
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
    applied_right: Vec<CheckpointMutationIdentity>,
    versions: Vec<CheckpointVersion>,
    pending: Vec<CheckpointProbe>,
    finalized: Vec<CheckpointFinalizedProbe>,
    right_rows_ipc: Vec<u8>,
    left_rows_ipc: Vec<u8>,
}

pub(crate) struct TemporalJoinVnodeState {
    config: TemporalJoinStateConfig,
    offsets: Vec<i64>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    output_schema: SchemaRef,
    key_codec: PartitionKeyCodecV1,
    left_row_codec: RowConverter,
    right_row_codec: RowConverter,
    history: VnodeHistory,
    applied_right: FxHashMap<TemporalSourcePosition, MutationIdentity>,
    right_batches: FxHashMap<u64, RetainedBatch>,
    pending: FxHashMap<ProbeIdentity, PendingProbe>,
    timers: BTreeMap<i64, Vec<ProbeIdentity>>,
    left_batches: FxHashMap<u64, RetainedBatch>,
    finalized: FxHashMap<ProbeIdentity, FinalizedProbe>,
    next_batch_id: u64,
    left_frontier: Option<i64>,
    left_idle: bool,
    right_frontier: Option<i64>,
    right_idle: bool,
    history_evicted_before: Option<i64>,
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
        if config.schedule.is_multi_horizon() && !config.emit_probe_metadata {
            return Err(DbError::Config(
                "multi-horizon temporal probes must emit offset_ms and probe_time".into(),
            ));
        }
        let key_types = config
            .left_key_indices
            .iter()
            .map(|&index| left_schema.field(index).data_type().clone());
        let key_codec = PartitionKeyCodecV1::try_new(key_types).map_err(|error| {
            DbError::Config(format!("temporal join key is not partitionable: {error}"))
        })?;
        let left_row_codec = row_codec(&left_schema, "left")?;
        let right_row_codec = row_codec(&right_schema, "right")?;
        let output_schema = output_schema(&left_schema, &right_schema, &config);
        let mut state = Self {
            config,
            offsets,
            left_schema,
            right_schema,
            output_schema,
            key_codec,
            left_row_codec,
            right_row_codec,
            history: FxHashMap::default(),
            applied_right: FxHashMap::default(),
            right_batches: FxHashMap::default(),
            pending: FxHashMap::default(),
            timers: BTreeMap::new(),
            left_batches: FxHashMap::default(),
            finalized: FxHashMap::default(),
            next_batch_id: 1,
            left_frontier: None,
            left_idle: false,
            right_frontier: None,
            right_idle: false,
            history_evicted_before: None,
            charged_bytes: 0,
        };
        state.refresh_charge()?;
        Ok(state)
    }

    pub(crate) fn output_schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
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

    pub(crate) fn apply_right_batch(
        &mut self,
        batch: &RecordBatch,
        operations: Option<&[SourceMutation]>,
    ) -> Result<TemporalRightApplyStats, DbError> {
        self.validate_batch_schema(batch, false)?;
        if operations.is_some_and(|operations| operations.len() != batch.num_rows()) {
            return Err(self.pipeline_error("right CDC operation count does not match row count"));
        }
        let positions = extract_source_positions(batch)?;
        let keys = self.encode_keys(batch, false)?;
        let times = extract_times(batch, self.config.right_time_index, "right")?;
        let fingerprints = fingerprint_rows(&self.right_row_codec, batch, "right")?;
        let key_columns = self.key_columns(batch, false);
        let mut stats = TemporalRightApplyStats::default();
        let mut candidates = Vec::new();
        let mut batch_positions: FxHashMap<TemporalSourcePosition, MutationIdentity> =
            FxHashMap::default();

        for (row, source_position) in positions.iter().enumerate() {
            let null_key = key_columns.iter().any(|column| column.is_null(row));
            let key = (!null_key).then(|| Box::<[u8]>::from(keys.row(row).as_ref()));
            let event_time = (!times.is_null(row)).then(|| times.value(row));
            if let Some(key) = key.as_deref() {
                self.validate_vnode(key)?;
            }
            let tombstone =
                operations.is_some_and(|operations| operations[row] == SourceMutation::Tombstone);
            let identity = MutationIdentity {
                key: key.clone(),
                event_time,
                tombstone,
                payload_fingerprint: fingerprints[row],
            };
            let source = source_position.clone();
            if let Some(previous) = self.applied_right.get(&source) {
                if previous != &identity {
                    return Err(self.pipeline_error(
                        "a right source position was replayed with different temporal data",
                    ));
                }
                stats.duplicates += 1;
                continue;
            }
            if let Some(previous) = batch_positions.get(&source) {
                if previous != &identity {
                    return Err(self.pipeline_error(
                        "a right batch reused one source position for different temporal data",
                    ));
                }
                stats.duplicates += 1;
                continue;
            }
            batch_positions.insert(source.clone(), identity.clone());
            let row = u32::try_from(row)
                .map_err(|_| self.pipeline_error("right batch exceeds the supported row count"))?;
            let (Some(key), Some(event_time)) = (key, event_time) else {
                stats.ignored_nulls += 1;
                candidates.push((None, source, identity, row));
                continue;
            };
            self.reject_late_input(event_time, "right")?;
            candidates.push((Some((key, event_time, tombstone)), source, identity, row));
        }

        if candidates.is_empty() {
            return Ok(stats);
        }
        let live_count = candidates
            .iter()
            .filter(|entry| entry.0.as_ref().is_some_and(|entry| !entry.2))
            .count();
        let growth = candidates
            .iter()
            .try_fold(0usize, |total, entry| {
                let history = entry.0.as_ref().map_or(0, |(key, _, _)| {
                    VERSION_ENTRY_CHARGE
                        .saturating_add(MAP_ENTRY_CHARGE)
                        .saturating_add(key.len())
                        .saturating_add(entry.1.heap_bytes())
                });
                total
                    .checked_add(MAP_ENTRY_CHARGE.saturating_add(history))
                    .and_then(|value| {
                        value.checked_add(
                            entry
                                .2
                                .key
                                .as_deref()
                                .map_or(0, <[u8]>::len)
                                .saturating_add(entry.1.heap_bytes()),
                        )
                    })
            })
            .and_then(|value| {
                if live_count == 0 {
                    Some(value)
                } else {
                    value.checked_add(batch.get_array_memory_size().saturating_add(BATCH_CHARGE))
                }
            })
            .ok_or_else(|| self.pipeline_error("right retained-state accounting overflow"))?;
        self.ensure_state_growth(growth, "right version admission")?;
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
        for (version, source, identity, row) in candidates {
            if let Some((key, event_time, tombstone)) = version {
                let order = (event_time, source.clone());
                let version = Version {
                    row: (!tombstone).then_some((batch_id.expect("live batch exists"), row)),
                };
                let replaced = self.history.entry(key).or_default().insert(order, version);
                debug_assert!(replaced.is_none());
                stats.inserted += 1;
            }
            self.applied_right.insert(source, identity);
        }
        self.refresh_charge()?;
        Ok(stats)
    }

    pub(crate) fn probe_left_batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, DbError> {
        self.validate_batch_schema(batch, true)?;
        let expanded_rows = batch
            .num_rows()
            .checked_mul(self.offsets.len())
            .ok_or_else(|| self.pipeline_error("temporal probe expansion overflowed"))?;
        u32::try_from(expanded_rows)
            .map_err(|_| self.pipeline_error("temporal probe expansion exceeds the row limit"))?;
        let positions = extract_source_positions(batch)?;
        let keys = self.encode_keys(batch, true)?;
        let times = extract_times(batch, self.config.left_time_index, "left")?;
        let fingerprints = fingerprint_rows(&self.left_row_codec, batch, "left")?;
        let key_columns = self.key_columns(batch, true);
        let input = Arc::new(batch.clone());
        let mut outputs = Vec::new();
        let mut planned = Vec::new();
        let mut seen: FxHashMap<ProbeIdentity, FinalizedProbe> = FxHashMap::default();

        for (row, source_position) in positions.iter().enumerate() {
            outputs
                .try_reserve(self.offsets.len())
                .map_err(|_| self.pipeline_error("temporal output expansion is too large"))?;
            planned
                .try_reserve(self.offsets.len())
                .map_err(|_| self.pipeline_error("temporal probe expansion is too large"))?;
            let null_key = key_columns.iter().any(|column| column.is_null(row));
            let event_time = (!times.is_null(row)).then(|| times.value(row));
            let key = (!null_key).then(|| Box::<[u8]>::from(keys.row(row).as_ref()));
            if let Some(key) = key.as_deref() {
                self.validate_vnode(key)?;
            }
            let row = u32::try_from(row)
                .map_err(|_| self.pipeline_error("left batch exceeds the supported row count"))?;
            let fingerprint = FinalizedProbe {
                key: key.clone(),
                left_event_time: event_time,
                payload_fingerprint: fingerprints[row as usize],
            };
            let mut lateness_checked = false;
            for &offset_ms in &self.offsets {
                let identity = ProbeIdentity {
                    source: source_position.clone(),
                    offset_ms,
                };
                if let Some(previous) = seen.get(&identity) {
                    if previous != &fingerprint {
                        return Err(self.pipeline_error(
                            "a left batch reused one source position for different temporal data",
                        ));
                    }
                    continue;
                }
                seen.insert(identity.clone(), fingerprint.clone());
                if let Some(previous) = self.finalized.get(&identity) {
                    if previous != &fingerprint {
                        return Err(self.pipeline_error(
                            "a left source position was replayed with different temporal data",
                        ));
                    }
                    continue;
                }
                if let Some(previous) = self.pending.get(&identity) {
                    if previous.left_event_time != event_time.unwrap_or(i64::MIN)
                        || Some(previous.key.as_ref()) != key.as_deref()
                        || previous.payload_fingerprint != fingerprint.payload_fingerprint
                    {
                        return Err(self.pipeline_error(
                            "a pending left source position was replayed with different temporal data",
                        ));
                    }
                    continue;
                }
                if !lateness_checked {
                    if let Some(event_time) = event_time {
                        self.reject_late_input(event_time, "left")?;
                    }
                    lateness_checked = true;
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
                    planned.push((identity, fingerprint.clone(), None));
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
                    planned.push((identity, fingerprint.clone(), None));
                } else {
                    planned.push((
                        identity,
                        fingerprint.clone(),
                        Some((key, event_time, probe_time, deadline, row)),
                    ));
                }
            }
        }

        let new_pending = planned.iter().filter(|entry| entry.2.is_some()).count();
        let new_finalized = planned.len().saturating_sub(new_pending);
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
        let pending_growth = new_pending
            .checked_mul(VERSION_ENTRY_CHARGE.saturating_add(TIMER_ENTRY_CHARGE))
            .and_then(|value| {
                if new_pending == 0 {
                    Some(value)
                } else {
                    value.checked_add(batch.get_array_memory_size().saturating_add(BATCH_CHARGE))
                }
            })
            .ok_or_else(|| self.pipeline_error("pending temporal state accounting overflow"))?;
        let pending_position_growth = planned
            .iter()
            .filter(|entry| entry.2.is_some())
            .try_fold(0usize, |total, entry| {
                let key_bytes = entry.2.as_ref().map_or(0, |(key, _, _, _, _)| key.len());
                total.checked_add(
                    entry
                        .0
                        .source
                        .heap_bytes()
                        .saturating_mul(2)
                        .saturating_add(key_bytes),
                )
            })
            .ok_or_else(|| self.pipeline_error("pending source-position accounting overflow"))?;
        let finalized_growth = new_finalized
            .checked_mul(MAP_ENTRY_CHARGE)
            .and_then(|base| {
                planned
                    .iter()
                    .filter(|entry| entry.2.is_none())
                    .try_fold(base, |total, entry| {
                        total
                            .checked_add(entry.1.key.as_deref().map_or(0, <[u8]>::len))
                            .and_then(|value| value.checked_add(entry.0.source.heap_bytes()))
                    })
            })
            .ok_or_else(|| self.pipeline_error("finalized temporal state accounting overflow"))?;
        let growth = pending_growth
            .checked_add(pending_position_growth)
            .and_then(|value| value.checked_add(finalized_growth))
            .ok_or_else(|| self.pipeline_error("left temporal state accounting overflow"))?;
        self.ensure_state_growth(growth, "left probe admission")?;
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
        for (identity, fingerprint, pending) in planned {
            if let Some((key, left_event_time, probe_time, deadline, row)) = pending {
                self.timers
                    .entry(deadline)
                    .or_default()
                    .push(identity.clone());
                self.pending.insert(
                    identity,
                    PendingProbe {
                        left_batch: left_batch_id.expect("pending batch exists"),
                        left_row: row,
                        key,
                        left_event_time,
                        probe_time,
                        deadline,
                        payload_fingerprint: fingerprint.payload_fingerprint,
                    },
                );
            } else {
                self.finalized.insert(identity, fingerprint);
            }
        }
        self.refresh_charge()?;
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
            self.gc_history()?;
        }
        self.left_idle = idle;
        self.refresh_charge()
    }

    pub(crate) fn advance_right_frontier(
        &mut self,
        frontier: Option<i64>,
        idle: bool,
    ) -> Result<RecordBatch, DbError> {
        validate_frontier(self.right_frontier, frontier, "right")?;
        let Some(frontier) = frontier else {
            self.right_idle = idle;
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        };
        let ready_deadlines: Vec<i64> = self
            .timers
            .range(..frontier)
            .map(|(&time, _)| time)
            .collect();
        let mut outputs = Vec::new();
        let mut ready = Vec::new();
        let mut seen = FxHashSet::default();
        for &deadline in &ready_deadlines {
            let mut identities = self.timers.get(&deadline).cloned().unwrap_or_default();
            identities.sort_unstable();
            for identity in identities {
                if !seen.insert(identity.clone()) {
                    return Err(self.pipeline_error("temporal timer contains a duplicate probe"));
                }
                let Some(probe) = self.pending.get(&identity) else {
                    return Err(self.pipeline_error("temporal timer referenced a missing probe"));
                };
                if probe.deadline != deadline {
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
                ready.push((deadline, identity));
            }
        }
        let output = self.build_output(&outputs)?;

        self.right_frontier = Some(frontier);
        self.right_idle = idle;
        for deadline in ready_deadlines {
            self.timers.remove(&deadline);
        }
        for (deadline, identity) in ready {
            let probe = self.pending.remove(&identity).ok_or_else(|| {
                self.pipeline_error("temporal timer referenced a missing probe during commit")
            })?;
            if probe.deadline != deadline {
                return Err(
                    self.pipeline_error("temporal timer deadline changed while committing a probe")
                );
            }
            self.finalized.insert(
                identity,
                FinalizedProbe {
                    key: Some(probe.key),
                    left_event_time: Some(probe.left_event_time),
                    payload_fingerprint: probe.payload_fingerprint,
                },
            );
            self.release_left_batch(probe.left_batch)?;
        }
        self.gc_history()?;
        self.refresh_charge()?;
        Ok(output)
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
        let finalized = self
            .sorted_finalized()
            .into_iter()
            .map(|(identity, value)| CheckpointFinalizedProbe {
                source: identity.source.clone(),
                offset_ms: identity.offset_ms,
                key: value.key.as_deref().map(<[u8]>::to_vec),
                left_event_time: value.left_event_time,
                payload_fingerprint: value.payload_fingerprint,
            })
            .collect();
        let mut applied_right: Vec<_> = self.applied_right.iter().collect();
        applied_right.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        let applied_right = applied_right
            .into_iter()
            .map(|(source, identity)| CheckpointMutationIdentity {
                source: source.clone(),
                key: identity.key.as_deref().map(<[u8]>::to_vec),
                event_time: identity.event_time,
                tombstone: identity.tombstone,
                payload_fingerprint: identity.payload_fingerprint,
            })
            .collect();
        let checkpoint = TemporalJoinCheckpoint {
            format_version: FORMAT_VERSION,
            config: self.checkpoint_config()?,
            left_frontier: self.left_frontier,
            left_idle: self.left_idle,
            right_frontier: self.right_frontier,
            right_idle: self.right_idle,
            history_evicted_before: self.history_evicted_before,
            applied_right,
            versions,
            pending,
            finalized,
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
        let right_rows = deserialize_batch_stream(&checkpoint.right_rows_ipc)
            .map_err(|error| DbError::Checkpoint(format!("temporal right IPC: {error}")))?;
        let left_rows = deserialize_batch_stream(&checkpoint.left_rows_ipc)
            .map_err(|error| DbError::Checkpoint(format!("temporal left IPC: {error}")))?;
        state.restore_applied_right(checkpoint.applied_right)?;
        state.restore_versions(checkpoint.versions, right_rows)?;
        state.restore_pending(checkpoint.pending, left_rows)?;
        state.restore_finalized(checkpoint.finalized)?;
        state.validate_restored_history_anchor()?;
        state.validate_restored_probe_consistency()?;
        if let Some(frontier) = state.right_frontier {
            if state
                .timers
                .keys()
                .next()
                .is_some_and(|deadline| *deadline < frontier)
            {
                return Err(DbError::Checkpoint(
                    "temporal checkpoint contains an already-final probe".into(),
                ));
            }
        }
        state.refresh_charge()?;
        if state.charged_bytes > state.config.limits.max_retained_bytes {
            return Err(DbError::Checkpoint(format!(
                "restored temporal state uses {} bytes; limit is {}",
                state.charged_bytes, state.config.limits.max_retained_bytes
            )));
        }
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
        self.key_codec.encode_columns(&columns).map_err(|error| {
            self.pipeline_error(&format!("could not encode temporal join key: {error}"))
        })
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
        let mut columns = interleave_rows(
            &self.left_schema,
            &left,
            self.left_schema.fields().len(),
            false,
            "left",
        )?;
        let mut positions =
            columns.split_off(self.left_schema.fields().len() - POSITION_COLUMN_COUNT);
        let orders = rows
            .iter()
            .map(|row| derived_probe_order(&row.left, &self.config.operator_name, row.offset_ms))
            .collect::<Result<Vec<_>, _>>()?;
        positions[1] = Arc::new(BinaryArray::from_iter_values(orders));
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
        columns.extend(positions);
        RecordBatch::try_new(Arc::clone(&self.output_schema), columns).map_err(|error| {
            self.pipeline_error(&format!("could not build temporal output: {error}"))
        })
    }

    fn gc_history(&mut self) -> Result<(), DbError> {
        let (Some(right_frontier), Some(left_frontier)) = (self.right_frontier, self.left_frontier)
        else {
            return Ok(());
        };
        let retention_cutoff = right_frontier
            .checked_sub(self.config.history_retention_ms)
            .unwrap_or(i64::MIN);
        let Some(earliest_future_event) =
            left_frontier.checked_sub(self.config.left_allowed_lateness_ms)
        else {
            return Ok(());
        };
        let Some(minimum_offset) = self.offsets.iter().copied().min() else {
            return Ok(());
        };
        let Some(earliest_future_probe) = earliest_future_event.checked_add(minimum_offset) else {
            return Ok(());
        };
        let oldest_pending = self.pending.values().map(|probe| probe.probe_time).min();
        let cutoff = oldest_pending
            .map_or(retention_cutoff.min(earliest_future_probe), |pending| {
                retention_cutoff.min(earliest_future_probe).min(pending)
            });
        if cutoff == i64::MIN {
            return Ok(());
        }
        let minimum_position = TemporalSourcePosition {
            partition: Vec::new(),
            order: Vec::new(),
            sub_offset: 0,
        };
        let boundary = (cutoff, minimum_position);
        let mut removed = Vec::new();
        for versions in self.history.values_mut() {
            let mut recent = versions.split_off(&boundary);
            let anchor = versions.pop_last();
            removed.extend(std::mem::take(versions));
            if let Some((order, version)) = anchor {
                versions.insert(order, version);
            }
            versions.append(&mut recent);
        }
        let removed_any = !removed.is_empty();
        for ((_, _source), version) in removed {
            if let Some((batch, _)) = version.row {
                self.release_right_batch(batch)?;
            }
        }
        self.history.retain(|_, versions| !versions.is_empty());
        if removed_any {
            self.history_evicted_before = Some(
                self.history_evicted_before
                    .map_or(cutoff, |previous| previous.max(cutoff)),
            );
        }
        Ok(())
    }

    fn release_right_batch(&mut self, batch_id: u64) -> Result<(), DbError> {
        release_batch(&mut self.right_batches, batch_id, "right")
    }

    fn release_left_batch(&mut self, batch_id: u64) -> Result<(), DbError> {
        release_batch(&mut self.left_batches, batch_id, "left")
    }

    fn allocate_batch_id(&mut self) -> Result<u64, DbError> {
        let id = self.next_batch_id;
        self.next_batch_id = self
            .next_batch_id
            .checked_add(1)
            .ok_or_else(|| self.pipeline_error("temporal batch identity overflowed"))?;
        Ok(id)
    }

    fn ensure_state_growth(&self, growth: usize, context: &str) -> Result<(), DbError> {
        let accounted = self
            .charged_bytes
            .checked_add(growth)
            .ok_or_else(|| self.pipeline_error("temporal retained-state accounting overflow"))?;
        if accounted > self.config.limits.max_retained_bytes {
            return Err(DbError::ManagedStateBudgetExceeded {
                context: format!("temporal join vnode {} {context}", self.config.vnode),
                accounted_bytes: accounted,
                limit_bytes: self.config.limits.max_retained_bytes,
            });
        }
        Ok(())
    }

    fn refresh_charge(&mut self) -> Result<(), DbError> {
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
            || checkpoint.applied_right.len() > entry_limit
            || checkpoint.finalized.len() > entry_limit
        {
            return Err(DbError::Checkpoint(
                "temporal checkpoint exceeds configured state limits".into(),
            ));
        }
        if checkpoint.versions.len() > checkpoint.applied_right.len() {
            return Err(DbError::Checkpoint(
                "temporal checkpoint versions exceed its right dedup entries".into(),
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
        Ok(())
    }

    fn restore_applied_right(
        &mut self,
        applied: Vec<CheckpointMutationIdentity>,
    ) -> Result<(), DbError> {
        for mutation in applied {
            if let Some(key) = mutation.key.as_deref() {
                self.validate_vnode(key)
                    .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            }
            let identity = MutationIdentity {
                key: mutation.key.map(Vec::into_boxed_slice),
                event_time: mutation.event_time,
                tombstone: mutation.tombstone,
                payload_fingerprint: mutation.payload_fingerprint,
            };
            if self
                .applied_right
                .insert(mutation.source, identity)
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "duplicate right source position in temporal checkpoint".into(),
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

    fn sorted_finalized(&self) -> Vec<(&ProbeIdentity, &FinalizedProbe)> {
        let mut values: Vec<_> = self.finalized.iter().collect();
        #[allow(clippy::unnecessary_sort_by)]
        values.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
        values
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
            let identity = self.applied_right.get(&version.source).ok_or_else(|| {
                DbError::Checkpoint("temporal version is missing its right dedup identity".into())
            })?;
            if identity.key.as_deref() != Some(version.key.as_slice())
                || identity.event_time != Some(version.event_time)
                || identity.tombstone != version.tombstone
            {
                return Err(DbError::Checkpoint(
                    "temporal version disagrees with its right dedup identity".into(),
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
                    || row_fingerprints[row] != identity.payload_fingerprint
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
        for (source, identity) in &self.applied_right {
            if let (Some(_), Some(event_time)) = (&identity.key, identity.event_time) {
                if !retained_sources.contains(source)
                    && self
                        .history_evicted_before
                        .is_none_or(|floor| event_time >= floor)
                {
                    return Err(DbError::Checkpoint(
                        "temporal right dedup entry is missing a retained version".into(),
                    ));
                }
            }
        }
        if let Some(floor) = self.history_evicted_before {
            for versions in self.history.values() {
                if versions
                    .keys()
                    .filter(|(event_time, _)| *event_time < floor)
                    .count()
                    > 1
                {
                    return Err(DbError::Checkpoint(
                        "temporal checkpoint retained multiple versions below its history floor"
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
            if self
                .right_frontier
                .is_some_and(|frontier| probe.deadline < frontier)
            {
                return Err(DbError::Checkpoint(
                    "temporal checkpoint contains an already-final probe".into(),
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
            let identity = ProbeIdentity {
                source: probe.source,
                offset_ms: probe.offset_ms,
            };
            self.timers
                .entry(probe.deadline)
                .or_default()
                .push(identity.clone());
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

    fn restore_finalized(
        &mut self,
        finalized: Vec<CheckpointFinalizedProbe>,
    ) -> Result<(), DbError> {
        for probe in finalized {
            if !self.offsets.contains(&probe.offset_ms) {
                return Err(DbError::Checkpoint(
                    "temporal finalized probe uses an unplanned offset".into(),
                ));
            }
            if let Some(key) = probe.key.as_deref() {
                self.validate_vnode(key)
                    .map_err(|error| DbError::Checkpoint(error.to_string()))?;
            }
            if let Some(event_time) = probe.left_event_time {
                event_time.checked_add(probe.offset_ms).ok_or_else(|| {
                    DbError::Checkpoint("temporal finalized-probe timing overflowed".into())
                })?;
            }
            let identity = ProbeIdentity {
                source: probe.source,
                offset_ms: probe.offset_ms,
            };
            if self.pending.contains_key(&identity) {
                return Err(DbError::Checkpoint(
                    "temporal probe is both pending and finalized in checkpoint".into(),
                ));
            }
            if self
                .finalized
                .insert(
                    identity,
                    FinalizedProbe {
                        key: probe.key.map(Vec::into_boxed_slice),
                        left_event_time: probe.left_event_time,
                        payload_fingerprint: probe.payload_fingerprint,
                    },
                )
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "duplicate finalized probe in temporal checkpoint".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_restored_history_anchor(&self) -> Result<(), DbError> {
        let Some(floor) = self.history_evicted_before else {
            return Ok(());
        };
        let mut expected = FxHashMap::<Box<[u8]>, (i64, TemporalSourcePosition)>::default();
        for (source, identity) in &self.applied_right {
            let (Some(key), Some(event_time)) = (&identity.key, identity.event_time) else {
                continue;
            };
            if event_time >= floor {
                continue;
            }
            let order = (event_time, source.clone());
            match expected.entry(key.clone()) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if order > *entry.get() {
                        entry.insert(order);
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(order);
                }
            }
        }
        for (key, order) in expected {
            if self
                .history
                .get(key.as_ref())
                .is_none_or(|versions| !versions.contains_key(&order))
            {
                return Err(DbError::Checkpoint(
                    "temporal checkpoint is missing a predecessor history anchor".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_restored_probe_consistency(&self) -> Result<(), DbError> {
        let mut by_source: FxHashMap<TemporalSourcePosition, (FinalizedProbe, FxHashSet<i64>)> =
            FxHashMap::default();
        for (identity, probe) in &self.pending {
            let fingerprint = FinalizedProbe {
                key: Some(probe.key.clone()),
                left_event_time: Some(probe.left_event_time),
                payload_fingerprint: probe.payload_fingerprint,
            };
            let entry = by_source
                .entry(identity.source.clone())
                .or_insert_with(|| (fingerprint.clone(), FxHashSet::default()));
            if entry.0 != fingerprint || !entry.1.insert(identity.offset_ms) {
                return Err(DbError::Checkpoint(
                    "temporal pending horizons disagree on their left source row".into(),
                ));
            }
        }
        for (identity, probe) in &self.finalized {
            let entry = by_source
                .entry(identity.source.clone())
                .or_insert_with(|| (probe.clone(), FxHashSet::default()));
            if entry.0 != *probe || !entry.1.insert(identity.offset_ms) {
                return Err(DbError::Checkpoint(
                    "temporal probe horizons disagree on their left source row".into(),
                ));
            }
        }
        for (_, horizons) in by_source.values() {
            if horizons.len() != self.offsets.len()
                || self.offsets.iter().any(|offset| !horizons.contains(offset))
            {
                return Err(DbError::Checkpoint(
                    "temporal checkpoint is missing a planned probe horizon".into(),
                ));
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

fn output_schema(left: &Schema, right: &Schema, config: &TemporalJoinStateConfig) -> SchemaRef {
    let left_visible = left.fields().len() - POSITION_COLUMN_COUNT;
    let right_visible = right.fields().len() - POSITION_COLUMN_COUNT;
    let mut fields = left.fields()[..left_visible].to_vec();
    fields.extend(right.fields()[..right_visible].iter().map(|field| {
        let renamed =
            field
                .as_ref()
                .clone()
                .with_name(format!("{}_{}", field.name(), config.right_name));
        Arc::new(if config.join_kind == TemporalJoinKind::Left {
            renamed.with_nullable(true)
        } else {
            renamed
        })
    }));
    if config.emit_probe_metadata {
        fields.push(Arc::new(Field::new("offset_ms", DataType::Int64, false)));
        fields.push(Arc::new(Field::new(
            "probe_time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )));
    }
    fields.extend(left.fields()[left_visible..].iter().cloned());
    Arc::new(Schema::new(fields))
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
) -> Result<Vec<[u8; 32]>, DbError> {
    let rows = codec.convert_columns(batch.columns()).map_err(|error| {
        DbError::Pipeline(format!(
            "temporal {side} rows cannot be deterministically encoded: {error}"
        ))
    })?;
    Ok((0..batch.num_rows())
        .map(|row| {
            let mut hasher = Sha256::new();
            hasher.update(b"laminardb/temporal-row/v1");
            hasher.update(rows.row(row).as_ref());
            hasher.finalize().into()
        })
        .collect())
}

fn derived_probe_order(
    left: &RowRef,
    operator_name: &str,
    offset_ms: i64,
) -> Result<Vec<u8>, DbError> {
    let order = left
        .batch
        .column_by_name(SOURCE_ORDER_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
        .ok_or_else(|| DbError::Pipeline("temporal left source-order column is invalid".into()))?;
    let order = order.value(left.row as usize);
    let length = u32::try_from(order.len()).map_err(|_| {
        DbError::Pipeline("temporal source-order value exceeds the encoding limit".into())
    })?;
    let operator_length = u32::try_from(operator_name.len()).map_err(|_| {
        DbError::Pipeline("temporal operator name exceeds the identity encoding limit".into())
    })?;
    let capacity = order
        .len()
        .checked_add(operator_name.len())
        .and_then(|size| size.checked_add(17))
        .ok_or_else(|| DbError::Pipeline("temporal probe identity is too large".into()))?;
    let mut derived = Vec::with_capacity(capacity);
    derived.push(1);
    derived.extend_from_slice(&operator_length.to_be_bytes());
    derived.extend_from_slice(operator_name.as_bytes());
    derived.extend_from_slice(&length.to_be_bytes());
    derived.extend_from_slice(order);
    derived.extend_from_slice(&offset_ms.to_be_bytes());
    Ok(derived)
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

fn release_batch(
    batches: &mut FxHashMap<u64, RetainedBatch>,
    batch_id: u64,
    side: &str,
) -> Result<(), DbError> {
    let retained = batches.get_mut(&batch_id).ok_or_else(|| {
        DbError::Pipeline(format!("temporal {side} row referenced a missing batch"))
    })?;
    retained.references = retained.references.checked_sub(1).ok_or_else(|| {
        DbError::Pipeline(format!("temporal {side} batch reference count underflowed"))
    })?;
    if retained.references == 0 {
        batches.remove(&batch_id);
    }
    Ok(())
}

fn calculate_charge(state: &TemporalJoinVnodeState) -> Result<usize, DbError> {
    let mut bytes = 512usize;
    for (key, versions) in &state.history {
        bytes = bytes
            .checked_add(MAP_ENTRY_CHARGE + key.len())
            .and_then(|value| {
                value.checked_add(versions.len().saturating_mul(VERSION_ENTRY_CHARGE))
            })
            .and_then(|value| {
                versions.keys().try_fold(value, |total, (_, source)| {
                    total.checked_add(source.heap_bytes())
                })
            })
            .ok_or_else(|| state.pipeline_error("history accounting overflow"))?;
    }
    for (source, identity) in &state.applied_right {
        bytes = bytes
            .checked_add(
                MAP_ENTRY_CHARGE
                    + identity.key.as_deref().map_or(0, <[u8]>::len)
                    + source.heap_bytes(),
            )
            .ok_or_else(|| state.pipeline_error("right dedup accounting overflow"))?;
    }
    for retained in state
        .right_batches
        .values()
        .chain(state.left_batches.values())
    {
        bytes = bytes
            .checked_add(
                retained
                    .batch
                    .get_array_memory_size()
                    .saturating_add(BATCH_CHARGE),
            )
            .ok_or_else(|| state.pipeline_error("batch accounting overflow"))?;
    }
    for (identity, probe) in &state.pending {
        bytes = bytes
            .checked_add(
                VERSION_ENTRY_CHARGE
                    + TIMER_ENTRY_CHARGE
                    + probe.key.len()
                    + identity.source.heap_bytes().saturating_mul(2),
            )
            .ok_or_else(|| state.pipeline_error("pending-probe accounting overflow"))?;
    }
    for (identity, probe) in &state.finalized {
        bytes = bytes
            .checked_add(
                MAP_ENTRY_CHARGE
                    + probe.key.as_deref().map_or(0, <[u8]>::len)
                    + identity.source.heap_bytes(),
            )
            .ok_or_else(|| state.pipeline_error("probe dedup accounting overflow"))?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, TimestampMillisecondArray};

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

    fn config(kind: TemporalJoinKind, schedule: TemporalProbeSchedule) -> TemporalJoinStateConfig {
        TemporalJoinStateConfig {
            vnode: 0,
            vnode_count: NonZeroU32::new(1).unwrap(),
            left_key_indices: vec![0],
            right_key_indices: vec![0],
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
        assert_eq!(
            state
                .apply_right_batch(&right, Some(&operations))
                .unwrap()
                .inserted,
            3
        );
        assert_eq!(
            state
                .apply_right_batch(&right, Some(&operations))
                .unwrap()
                .duplicates,
            3
        );
        state.advance_right_frontier(Some(1_000), false).unwrap();
        assert_eq!(
            state
                .apply_right_batch(&right, Some(&operations))
                .unwrap()
                .duplicates,
            3
        );
        let changed = batch(
            schema("right"),
            vec![Some("A"), Some("A"), Some("A")],
            vec![Some(99), Some(11), None],
            vec![Some(100), Some(100), Some(200)],
            vec![1, 2, 3],
        );
        assert!(state
            .apply_right_batch(&changed, Some(&operations))
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
    fn multi_horizon_null_key_keeps_probe_time_and_unique_position() {
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
        let orders = output
            .column_by_name(SOURCE_ORDER_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_ne!(orders.value(0), orders.value(1));
        let visible = laminar_connectors::connector::strip_source_row_positions(&output).unwrap();
        assert!(visible.column_by_name(SOURCE_PARTITION_COLUMN).is_none());
        assert_eq!(
            visible.num_columns() + POSITION_COLUMN_COUNT,
            output.num_columns()
        );
        let row = RowRef {
            batch: Arc::new(left),
            row: 0,
        };
        assert_ne!(
            derived_probe_order(&row, "operator-a", 0).unwrap(),
            derived_probe_order(&row, "operator-b", 0).unwrap()
        );
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
            vec![Some("A")],
            vec![Some(1)],
            vec![Some(100)],
            vec![2],
        );
        assert_eq!(state.probe_left_batch(&left).unwrap().num_rows(), 0);
        assert_eq!(state.pending_probes(), 1);
        assert_eq!(
            state
                .advance_right_frontier(Some(149), false)
                .unwrap()
                .num_rows(),
            0
        );
        assert_eq!(
            state
                .advance_right_frontier(Some(150), false)
                .unwrap()
                .num_rows(),
            0
        );
        let output = state.advance_right_frontier(Some(151), false).unwrap();
        assert_eq!(prices(&output), vec![Some(7)]);
        assert_eq!(state.pending_probes(), 0);
    }

    #[test]
    fn retention_preserves_one_predecessor_anchor() {
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
        state.advance_right_frontier(Some(120), false).unwrap();
        assert_eq!(state.retained_versions(), 3);
        state.advance_left_frontier(Some(120), false).unwrap();
        assert_eq!(state.retained_versions(), 2);
    }

    #[test]
    fn checkpoint_restores_finalized_probe_older_than_history_floor() {
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

        let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
        let restored =
            TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint)
                .unwrap();
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
        assert!(state.finalized.is_empty());
        assert!(state.timers.is_empty());
        assert_eq!(state.accounted_state_bytes(), before);
    }

    #[test]
    fn restore_rejects_missing_probe_horizon() {
        let cfg = config(
            TemporalJoinKind::Left,
            TemporalProbeSchedule::list(vec![0, 5]).unwrap(),
        );
        let mut state =
            TemporalJoinVnodeState::try_new(schema("left"), schema("right"), cfg.clone()).unwrap();
        let left = batch(
            schema("left"),
            vec![None],
            vec![Some(1)],
            vec![Some(100)],
            vec![1],
        );
        state.probe_left_batch(&left).unwrap();
        let bytes = state.checkpoint(4 * 1024 * 1024).unwrap();
        let mut decoded =
            rkyv::from_bytes::<TemporalJoinCheckpoint, rkyv::rancor::Error>(&bytes).unwrap();
        decoded.finalized.pop();
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
                .contains("missing a planned probe horizon")
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
        let checkpoint = state.checkpoint(4 * 1024 * 1024).unwrap();
        let mut restored =
            TemporalJoinVnodeState::restore(schema("left"), schema("right"), cfg, &checkpoint)
                .unwrap();
        assert_eq!(restored.retained_versions(), 1);
        assert_eq!(restored.pending_probes(), 1);
        assert_eq!(
            restored
                .advance_right_frontier(Some(150), false)
                .unwrap()
                .num_rows(),
            0
        );
        let output = restored.advance_right_frontier(Some(151), false).unwrap();
        assert_eq!(prices(&output), vec![Some(9)]);
    }
}
