//! Aggregate output construction and publication bookkeeping.

#[cfg(feature = "cluster")]
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::ScalarValue;
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{
    OutputFrameId, OutputPartitionId, PartitionSequence, StreamGeneration,
};
use rustc_hash::FxHashSet;

#[cfg(feature = "cluster")]
use super::apply_compiled_having;
#[cfg(feature = "cluster")]
use super::vnode_state::AggregateVnodeState;
use super::{build_weighted_batch, ConcreteAggregateState, DbError, IncrementalAggState};
#[cfg(feature = "cluster")]
use crate::subscription::PartitionedOutputBatch;

/// Deterministic row bound for a logical partition frame.
#[cfg(feature = "cluster")]
pub(crate) const MAX_PARTITION_OUTPUT_FRAME_ROWS: usize = 1_024;

/// Bounded frame roster retained between compute and publication.
#[cfg(feature = "cluster")]
pub(crate) const MAX_PREPARED_PARTITION_FRAMES: usize = 65_536;

#[cfg(feature = "cluster")]
struct PendingChange {
    key: arrow::row::OwnedRow,
    values: Vec<ScalarValue>,
    weight: i64,
    commit_insert: bool,
}

#[cfg(feature = "cluster")]
struct EvaluatedVnodeOutput {
    changes: Vec<PendingChange>,
    frame_ranges: Vec<Range<usize>>,
    deletions: Vec<arrow::row::OwnedRow>,
}

#[cfg(feature = "cluster")]
struct PreparedVnodeEmission {
    vnode: u32,
    dirty: Option<FxHashSet<arrow::row::OwnedRow>>,
    insertions: Vec<(arrow::row::OwnedRow, Vec<ScalarValue>)>,
    deletions: Vec<arrow::row::OwnedRow>,
    next_output_sequence: u64,
    sequence_advanced: bool,
}

/// Output and infallible publication changes prepared from one aggregate task turn.
#[cfg(feature = "cluster")]
pub(crate) struct PreparedAggregateEmission {
    frames: Vec<PartitionedOutputBatch>,
    commits: Vec<PreparedVnodeEmission>,
}

#[cfg(feature = "cluster")]
impl PreparedAggregateEmission {
    pub(crate) fn result_batches(&self) -> Vec<RecordBatch> {
        self.frames
            .iter()
            .map(|frame| frame.batch.clone())
            .collect()
    }

    pub(crate) fn take_frames(&mut self) -> Vec<PartitionedOutputBatch> {
        std::mem::take(&mut self.frames)
    }

    pub(crate) fn retained_bookkeeping_bytes(&self) -> usize {
        self.commits.iter().fold(0usize, |total, commit| {
            let insert_roster = commit
                .insertions
                .capacity()
                .saturating_mul(std::mem::size_of::<(arrow::row::OwnedRow, Vec<ScalarValue>)>());
            let delete_roster = commit
                .deletions
                .capacity()
                .saturating_mul(std::mem::size_of::<arrow::row::OwnedRow>());
            let deletion_payload_bytes = commit.deletions.iter().fold(0usize, |bytes, key| {
                bytes.saturating_add(key.as_ref().len())
            });
            let scalar_bytes = commit
                .insertions
                .iter()
                .fold(0usize, |bytes, (key, values)| {
                    values.iter().fold(
                        bytes.saturating_add(key.as_ref().len()).saturating_add(
                            values
                                .capacity()
                                .saturating_mul(std::mem::size_of::<ScalarValue>()),
                        ),
                        |bytes, value| bytes.saturating_add(value.size()),
                    )
                });
            total
                .saturating_add(insert_roster)
                .saturating_add(delete_roster)
                .saturating_add(deletion_payload_bytes)
                .saturating_add(scalar_bytes)
        })
    }
}

fn scalar_vectors_equal(left: &[ScalarValue], right: &[ScalarValue]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| match (left, right) {
                (ScalarValue::Float64(Some(left)), ScalarValue::Float64(Some(right)))
                    if left.is_nan() && right.is_nan() =>
                {
                    true
                }
                (ScalarValue::Float32(Some(left)), ScalarValue::Float32(Some(right)))
                    if left.is_nan() && right.is_nan() =>
                {
                    true
                }
                _ => left == right,
            })
}

#[cfg(feature = "cluster")]
fn record_key_range(
    previous_len: usize,
    current_len: usize,
    chunk_start: &mut usize,
    ranges: &mut Vec<Range<usize>>,
) {
    let key_rows = current_len - previous_len;
    if previous_len > *chunk_start
        && previous_len - *chunk_start + key_rows > MAX_PARTITION_OUTPUT_FRAME_ROWS
    {
        ranges.push(*chunk_start..previous_len);
        *chunk_start = previous_len;
    }
}

#[cfg(feature = "cluster")]
fn finish_ranges(row_count: usize, chunk_start: usize, ranges: &mut Vec<Range<usize>>) {
    if row_count > chunk_start {
        ranges.push(chunk_start..row_count);
    }
}

#[cfg(feature = "cluster")]
fn evaluate_changelog_vnode(
    state: &mut AggregateVnodeState,
    dirty: &FxHashSet<arrow::row::OwnedRow>,
    grouped: bool,
) -> Result<EvaluatedVnodeOutput, DbError> {
    let mut keys = dirty.iter().collect::<Vec<_>>();
    keys.sort_unstable_by(|left, right| left.as_ref().cmp(right.as_ref()));
    let mut changes = Vec::new();
    let mut ranges = Vec::new();
    let mut deletions = Vec::new();
    let mut chunk_start = 0usize;

    for key in keys {
        let Some(entry) = state.groups.get_mut(key) else {
            continue;
        };
        let previous_len = changes.len();
        if grouped && entry.input_weight == 0 {
            if let Some(old) = state.last_emitted.get(key) {
                changes.push(PendingChange {
                    key: key.clone(),
                    values: old.clone(),
                    weight: -1,
                    commit_insert: false,
                });
            }
            deletions.push(key.clone());
        } else {
            let current = entry
                .accs
                .iter_mut()
                .map(ConcreteAggregateState::evaluate)
                .collect::<Result<Vec<_>, _>>()?;
            match state.last_emitted.get(key) {
                Some(old) if scalar_vectors_equal(old, &current) => {}
                Some(old) => {
                    changes.push(PendingChange {
                        key: key.clone(),
                        values: old.clone(),
                        weight: -1,
                        commit_insert: false,
                    });
                    changes.push(PendingChange {
                        key: key.clone(),
                        values: current,
                        weight: 1,
                        commit_insert: true,
                    });
                }
                None => changes.push(PendingChange {
                    key: key.clone(),
                    values: current,
                    weight: 1,
                    commit_insert: true,
                }),
            }
        }
        record_key_range(previous_len, changes.len(), &mut chunk_start, &mut ranges);
    }
    finish_ranges(changes.len(), chunk_start, &mut ranges);
    Ok(EvaluatedVnodeOutput {
        changes,
        frame_ranges: ranges,
        deletions,
    })
}

#[cfg(feature = "cluster")]
fn build_weighted_change_batch(
    state: &IncrementalAggState,
    changes: &[PendingChange],
) -> Result<RecordBatch, DbError> {
    let keys = changes
        .iter()
        .map(|change| change.key.clone())
        .collect::<Vec<_>>();
    let values = changes
        .iter()
        .map(|change| change.values.clone())
        .collect::<Vec<_>>();
    let weights = changes
        .iter()
        .map(|change| change.weight)
        .collect::<Vec<_>>();
    build_weighted_batch(
        &keys,
        &values,
        &weights,
        &state.row_converter,
        state.num_group_cols,
        &state.agg_specs,
        &state.output_schema,
    )
}

#[cfg(feature = "cluster")]
fn filtered_batch(
    state: &IncrementalAggState,
    batch: RecordBatch,
) -> Result<Option<RecordBatch>, DbError> {
    let Some(filter) = state.having_filter.as_ref() else {
        return Ok(Some(batch));
    };
    Ok(apply_compiled_having(std::slice::from_ref(&batch), filter)?
        .into_iter()
        .find(|filtered| filtered.num_rows() != 0))
}

#[cfg(feature = "cluster")]
fn next_frame(
    stream_generation: StreamGeneration,
    partition: OutputPartitionId,
    next_sequence: &mut PartitionSequence,
    batch: RecordBatch,
) -> Result<PartitionedOutputBatch, DbError> {
    let sequence = *next_sequence;
    *next_sequence = sequence.checked_next().map_err(|_| {
        DbError::PipelineTerminal(format!(
            "subscription output partition {} sequence overflow",
            partition.get()
        ))
    })?;
    Ok(PartitionedOutputBatch {
        id: OutputFrameId {
            stream_generation,
            partition,
            sequence,
        },
        batch,
    })
}

impl IncrementalAggState {
    /// Emit current aggregate state; accumulators keep running (no reset).
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.emit_changelog {
            return self.emit_changelog_delta();
        }
        self.emit_running_state()
    }

    /// Prepare deterministic vnode-local frames without advancing changelog or sequence state.
    #[cfg(feature = "cluster")]
    pub(crate) fn prepare_partitioned_emit(
        &mut self,
        stream_generation: StreamGeneration,
    ) -> Result<PreparedAggregateEmission, DbError> {
        if self.emit_changelog {
            self.prepare_partitioned_changelog(stream_generation)
        } else {
            self.prepare_partitioned_running_state(stream_generation)
        }
    }

    fn take_emit_dirty_sets(&mut self) -> Vec<(u32, FxHashSet<arrow::row::OwnedRow>)> {
        self.vnode_states
            .iter_mut()
            .filter_map(|(vnode, state)| {
                (!state.emit_dirty_keys.is_empty())
                    .then(|| (vnode, std::mem::take(&mut state.emit_dirty_keys)))
            })
            .collect()
    }

    fn finish_emit_dirty_sets(
        &mut self,
        dirty_by_vnode: Vec<(u32, FxHashSet<arrow::row::OwnedRow>)>,
        emission_succeeded: bool,
    ) {
        for (vnode, dirty) in dirty_by_vnode {
            self.vnode_states
                .get_mut(vnode)
                .expect("emitting aggregate vnode must remain resident")
                .replace_emit_dirty_keys_after_attempt(dirty, emission_succeeded);
        }
    }

    #[cfg(feature = "cluster")]
    fn prepare_changelog_vnode(
        &mut self,
        stream_generation: StreamGeneration,
        vnode: u32,
        dirty: FxHashSet<arrow::row::OwnedRow>,
    ) -> Result<(Vec<PartitionedOutputBatch>, PreparedVnodeEmission), DbError> {
        let prepared = (|| {
            let evaluated = evaluate_changelog_vnode(
                self.vnode_states
                    .get_mut(vnode)
                    .expect("dirty aggregate vnode must remain resident during emission"),
                &dirty,
                self.num_group_cols != 0,
            )?;
            let insert_count = evaluated
                .changes
                .iter()
                .filter(|change| change.commit_insert)
                .count();
            let state = self
                .vnode_states
                .get_mut(vnode)
                .expect("evaluated aggregate vnode must remain resident");
            let previous_spare = state.collection_spare_usage();
            state
                .last_emitted
                .try_reserve(insert_count)
                .map_err(|error| {
                    DbError::Pipeline(format!(
                        "aggregate vnode {vnode} changelog commit reservation failed: {error}"
                    ))
                })?;
            state.reconcile_collection_spare_usage(previous_spare);
            let partition = OutputPartitionId::new(u16::try_from(vnode).map_err(|_| {
                DbError::Pipeline(format!(
                    "aggregate vnode {vnode} is not a valid output partition"
                ))
            })?);
            let initial_sequence = self
                .vnode_states
                .get(vnode)
                .expect("evaluated aggregate vnode must remain resident")
                .next_output_sequence;
            let mut next_sequence = PartitionSequence::new(initial_sequence);
            let mut frames = Vec::with_capacity(evaluated.frame_ranges.len());
            for range in &evaluated.frame_ranges {
                let batch = build_weighted_change_batch(self, &evaluated.changes[range.clone()])?;
                if let Some(batch) = filtered_batch(self, batch)? {
                    frames.push(next_frame(
                        stream_generation,
                        partition,
                        &mut next_sequence,
                        batch,
                    )?);
                }
            }
            Ok::<_, DbError>((evaluated, frames, initial_sequence, next_sequence))
        })();
        let (evaluated, frames, initial_sequence, next_sequence) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                self.finish_emit_dirty_sets(vec![(vnode, dirty)], false);
                return Err(error);
            }
        };
        let insertions = evaluated
            .changes
            .into_iter()
            .filter(|change| change.commit_insert)
            .map(|change| (change.key, change.values))
            .collect();
        Ok((
            frames,
            PreparedVnodeEmission {
                vnode,
                dirty: Some(dirty),
                insertions,
                deletions: evaluated.deletions,
                next_output_sequence: next_sequence.get(),
                sequence_advanced: next_sequence.get() != initial_sequence,
            },
        ))
    }

    #[cfg(feature = "cluster")]
    fn prepare_partitioned_changelog(
        &mut self,
        stream_generation: StreamGeneration,
    ) -> Result<PreparedAggregateEmission, DbError> {
        let dirty_by_vnode = self.take_emit_dirty_sets();
        let mut frames = Vec::new();
        let mut commits = Vec::with_capacity(dirty_by_vnode.len());
        let mut remaining_dirty = dirty_by_vnode.into_iter();
        while let Some((vnode, dirty)) = remaining_dirty.next() {
            match self.prepare_changelog_vnode(stream_generation, vnode, dirty) {
                Ok((mut vnode_frames, commit)) => {
                    if frames.len().saturating_add(vnode_frames.len())
                        > MAX_PREPARED_PARTITION_FRAMES
                    {
                        let mut aborted = commits;
                        aborted.push(commit);
                        aborted.extend(remaining_dirty.map(|(vnode, dirty)| {
                            PreparedVnodeEmission {
                                vnode,
                                dirty: Some(dirty),
                                insertions: Vec::new(),
                                deletions: Vec::new(),
                                next_output_sequence: 0,
                                sequence_advanced: false,
                            }
                        }));
                        self.abort_partitioned_emit(PreparedAggregateEmission {
                            frames: Vec::new(),
                            commits: aborted,
                        });
                        return Err(DbError::Pipeline(format!(
                            "aggregate subscription output exceeded {MAX_PREPARED_PARTITION_FRAMES} prepared frames"
                        )));
                    }
                    frames.append(&mut vnode_frames);
                    commits.push(commit);
                }
                Err(error) => {
                    let mut dirty = commits
                        .into_iter()
                        .filter_map(|commit| commit.dirty.map(|dirty| (commit.vnode, dirty)))
                        .collect::<Vec<_>>();
                    dirty.extend(remaining_dirty);
                    self.finish_emit_dirty_sets(dirty, false);
                    return Err(error);
                }
            }
        }
        Ok(PreparedAggregateEmission { frames, commits })
    }

    #[cfg(feature = "cluster")]
    fn evaluate_running_vnode(&mut self, vnode: u32) -> Result<EvaluatedVnodeOutput, DbError> {
        let state = self
            .vnode_states
            .get_mut(vnode)
            .expect("active aggregate vnode must remain resident");
        let mut keys = state.groups.keys().cloned().collect::<Vec<_>>();
        keys.sort_unstable_by(|left, right| left.as_ref().cmp(right.as_ref()));
        let mut changes = Vec::new();
        let mut ranges = Vec::new();
        let mut deletions = Vec::new();
        let mut chunk_start = 0usize;
        for key in keys {
            let entry = state.groups.get_mut(&key).expect("sorted aggregate key");
            if self.num_group_cols != 0 && entry.input_weight == 0 {
                deletions.push(key);
                continue;
            }
            let previous_len = changes.len();
            let values = entry
                .accs
                .iter_mut()
                .map(ConcreteAggregateState::evaluate)
                .collect::<Result<Vec<_>, _>>()?;
            changes.push(PendingChange {
                key,
                values,
                weight: 0,
                commit_insert: false,
            });
            record_key_range(previous_len, changes.len(), &mut chunk_start, &mut ranges);
        }
        finish_ranges(changes.len(), chunk_start, &mut ranges);
        Ok(EvaluatedVnodeOutput {
            changes,
            frame_ranges: ranges,
            deletions,
        })
    }

    #[cfg(feature = "cluster")]
    fn build_running_batch(&self, rows: &[PendingChange]) -> Result<RecordBatch, DbError> {
        let group_arrays = if self.num_group_cols == 0 {
            Vec::new()
        } else {
            self.row_converter
                .convert_rows(rows.iter().map(|row| row.key.row()))
                .map_err(|error| DbError::Pipeline(format!("group key array build: {error}")))?
        };
        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (index, spec) in self.agg_specs.iter().enumerate() {
            let values = rows.iter().map(|row| row.values[index].clone());
            let array = ScalarValue::iter_to_array(values)
                .map_err(|error| DbError::Pipeline(format!("agg result array build: {error}")))?;
            let array = if array.data_type() == &spec.return_type {
                array
            } else {
                arrow::compute::cast(&array, &spec.return_type).map_err(|error| {
                    DbError::Pipeline(format!(
                        "aggregate '{}' result cast failed: {error}",
                        spec.output_name
                    ))
                })?
            };
            agg_arrays.push(array);
        }
        let mut arrays = group_arrays;
        arrays.extend(agg_arrays);
        RecordBatch::try_new(Arc::clone(&self.output_schema), arrays)
            .map_err(|error| DbError::Pipeline(format!("result batch build: {error}")))
    }

    #[cfg(feature = "cluster")]
    fn prepare_running_vnode(
        &mut self,
        stream_generation: StreamGeneration,
        vnode: u32,
    ) -> Result<(Vec<PartitionedOutputBatch>, PreparedVnodeEmission), DbError> {
        let evaluated = self.evaluate_running_vnode(vnode)?;
        let partition = OutputPartitionId::new(u16::try_from(vnode).map_err(|_| {
            DbError::Pipeline(format!(
                "aggregate vnode {vnode} is not a valid output partition"
            ))
        })?);
        let initial_sequence = self
            .vnode_states
            .get(vnode)
            .expect("evaluated aggregate vnode must remain resident")
            .next_output_sequence;
        let mut next_sequence = PartitionSequence::new(initial_sequence);
        let mut frames = Vec::with_capacity(evaluated.frame_ranges.len());
        for range in &evaluated.frame_ranges {
            let batch = self.build_running_batch(&evaluated.changes[range.clone()])?;
            if let Some(batch) = filtered_batch(self, batch)? {
                frames.push(next_frame(
                    stream_generation,
                    partition,
                    &mut next_sequence,
                    batch,
                )?);
            }
        }
        Ok((
            frames,
            PreparedVnodeEmission {
                vnode,
                dirty: None,
                insertions: Vec::new(),
                deletions: evaluated.deletions,
                next_output_sequence: next_sequence.get(),
                sequence_advanced: next_sequence.get() != initial_sequence,
            },
        ))
    }

    #[cfg(feature = "cluster")]
    fn prepare_partitioned_running_state(
        &mut self,
        stream_generation: StreamGeneration,
    ) -> Result<PreparedAggregateEmission, DbError> {
        let vnodes = self.vnode_states.active_vnodes().to_vec();
        let mut frames = Vec::new();
        let mut commits = Vec::with_capacity(vnodes.len());
        for vnode in vnodes {
            let (mut vnode_frames, commit) =
                self.prepare_running_vnode(stream_generation, vnode)?;
            if frames.len().saturating_add(vnode_frames.len()) > MAX_PREPARED_PARTITION_FRAMES {
                return Err(DbError::Pipeline(format!(
                    "aggregate subscription output exceeded {MAX_PREPARED_PARTITION_FRAMES} prepared frames"
                )));
            }
            frames.append(&mut vnode_frames);
            commits.push(commit);
        }
        Ok(PreparedAggregateEmission { frames, commits })
    }

    /// Publish an output preparation after every external append and ordinary sink succeeds.
    #[cfg(feature = "cluster")]
    pub(crate) fn commit_partitioned_emit(&mut self, prepared: PreparedAggregateEmission) {
        for commit in prepared.commits {
            let changed = commit.sequence_advanced || !commit.insertions.is_empty();
            let state = self
                .vnode_states
                .get_mut(commit.vnode)
                .expect("prepared aggregate vnode must remain resident until publication");
            for (key, values) in commit.insertions {
                state.insert_last_emitted(key, values);
            }
            state.next_output_sequence = commit.next_output_sequence;
            if let Some(dirty) = commit.dirty {
                state.replace_emit_dirty_keys_after_attempt(dirty, true);
            }
            for key in commit.deletions {
                self.commit_deleted_group(commit.vnode, &key);
            }
            if changed {
                self.mark_checkpoint_vnode_dirty(commit.vnode);
            }
        }
        debug_assert!(self.vnode_states.iter().all(|(_, state)| state
            .last_emitted
            .keys()
            .all(|key| state.groups.contains_key(key))));
    }

    /// Restore dirty-key ownership when publication fails before bookkeeping commit.
    #[cfg(feature = "cluster")]
    pub(crate) fn abort_partitioned_emit(&mut self, prepared: PreparedAggregateEmission) {
        let dirty = prepared
            .commits
            .into_iter()
            .filter_map(|commit| commit.dirty.map(|dirty| (commit.vnode, dirty)))
            .collect();
        self.finish_emit_dirty_sets(dirty, false);
    }

    fn emit_running_state(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.num_group_cols > 0 {
            let tombstones = self
                .vnode_states
                .iter()
                .flat_map(|(vnode, state)| {
                    state
                        .groups
                        .iter()
                        .filter(|(_, entry)| entry.input_weight == 0)
                        .map(move |(key, _)| (vnode, key.clone()))
                })
                .collect::<Vec<_>>();
            for (vnode, key) in tombstones {
                self.commit_deleted_group(vnode, &key);
            }
        }
        let num_rows = self.vnode_states.resident_group_count();
        if num_rows == 0 {
            return Ok(Vec::new());
        }

        let group_arrays = if self.num_group_cols > 0 {
            self.row_converter
                .convert_rows(
                    self.vnode_states
                        .iter()
                        .flat_map(|(_, state)| state.groups.keys())
                        .map(arrow::row::OwnedRow::row),
                )
                .map_err(|error| DbError::Pipeline(format!("group key array build: {error}")))?
        } else {
            Vec::new()
        };
        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (index, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars = Vec::with_capacity(num_rows);
            for (_, state) in self.vnode_states.iter_mut() {
                for entry in state.groups.values_mut() {
                    scalars.push(entry.accs[index].evaluate()?);
                }
            }
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|error| DbError::Pipeline(format!("agg result array build: {error}")))?;
            let array = if array.data_type() == &spec.return_type {
                array
            } else {
                arrow::compute::cast(&array, &spec.return_type).map_err(|error| {
                    DbError::Pipeline(format!(
                        "aggregate '{}' result cast failed: {error}",
                        spec.output_name
                    ))
                })?
            };
            agg_arrays.push(array);
        }
        let mut arrays = group_arrays;
        arrays.extend(agg_arrays);
        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), arrays)
            .map_err(|error| DbError::Pipeline(format!("result batch build: {error}")))?;
        Ok(vec![batch])
    }

    fn emit_changelog_delta(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let dirty_by_vnode = self.take_emit_dirty_sets();
        let mut retract_keys = Vec::new();
        let mut retract_vals = Vec::new();
        let mut insert_keys = Vec::new();
        let mut insert_vals = Vec::new();
        let mut insert_vnodes = Vec::new();
        let mut deleted_groups = Vec::new();

        let evaluation = (|| {
            for (vnode, dirty) in &dirty_by_vnode {
                let state = self
                    .vnode_states
                    .get_mut(*vnode)
                    .expect("dirty aggregate vnode must remain resident during emission");
                for key in dirty {
                    let Some(entry) = state.groups.get_mut(key) else {
                        continue;
                    };
                    if entry.input_weight == 0 && self.num_group_cols > 0 {
                        if let Some(old) = state.last_emitted.get(key) {
                            retract_keys.push(key.clone());
                            retract_vals.push(old.clone());
                        }
                        deleted_groups.push((*vnode, key.clone()));
                        continue;
                    }
                    let current = entry
                        .accs
                        .iter_mut()
                        .map(ConcreteAggregateState::evaluate)
                        .collect::<Result<Vec<_>, _>>()?;
                    match state.last_emitted.get(key) {
                        Some(old) if scalar_vectors_equal(old, &current) => {}
                        Some(old) => {
                            retract_keys.push(key.clone());
                            retract_vals.push(old.clone());
                            insert_keys.push(key.clone());
                            insert_vals.push(current);
                            insert_vnodes.push(*vnode);
                        }
                        None => {
                            insert_keys.push(key.clone());
                            insert_vals.push(current);
                            insert_vnodes.push(*vnode);
                        }
                    }
                }
            }
            Ok::<(), DbError>(())
        })();
        if let Err(error) = evaluation {
            self.finish_emit_dirty_sets(dirty_by_vnode, false);
            return Err(error);
        }
        self.finish_legacy_changelog(
            dirty_by_vnode,
            retract_keys,
            retract_vals,
            insert_keys,
            insert_vals,
            insert_vnodes,
            deleted_groups,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn finish_legacy_changelog(
        &mut self,
        dirty_by_vnode: Vec<(u32, FxHashSet<arrow::row::OwnedRow>)>,
        retract_keys: Vec<arrow::row::OwnedRow>,
        retract_vals: Vec<Vec<ScalarValue>>,
        insert_keys: Vec<arrow::row::OwnedRow>,
        insert_vals: Vec<Vec<ScalarValue>>,
        insert_vnodes: Vec<u32>,
        deleted_groups: Vec<(u32, arrow::row::OwnedRow)>,
    ) -> Result<Vec<RecordBatch>, DbError> {
        let retract_count = retract_keys.len();
        let total = retract_count + insert_keys.len();
        if total == 0 {
            for (vnode, key) in deleted_groups {
                self.commit_deleted_group(vnode, &key);
            }
            self.finish_emit_dirty_sets(dirty_by_vnode, true);
            return Ok(Vec::new());
        }
        let mut keys = Vec::with_capacity(total);
        let mut values = Vec::with_capacity(total);
        let mut weights = Vec::with_capacity(total);
        for (key, value) in retract_keys.into_iter().zip(retract_vals) {
            keys.push(key);
            values.push(value);
            weights.push(-1);
        }
        for (key, value) in insert_keys.into_iter().zip(insert_vals) {
            keys.push(key);
            values.push(value);
            weights.push(1);
        }
        let batch = match build_weighted_batch(
            &keys,
            &values,
            &weights,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        ) {
            Ok(batch) => batch,
            Err(error) => {
                self.finish_emit_dirty_sets(dirty_by_vnode, false);
                return Err(error);
            }
        };
        for ((key, current), vnode) in keys
            .into_iter()
            .zip(values)
            .skip(retract_count)
            .zip(insert_vnodes)
        {
            self.commit_last_emitted(vnode, key, current);
        }
        for (vnode, key) in deleted_groups {
            self.commit_deleted_group(vnode, &key);
        }
        self.finish_emit_dirty_sets(dirty_by_vnode, true);
        Ok(vec![batch])
    }
}
