//! Columnar vnode checkpoint frames: layout, encode, archive preflight, and restore decode.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use arrow::record_batch::RecordBatch;
use rustc_hash::FxHashMap;

use super::{
    CoreWindowAssigner, CoreWindowState, CoreWindowVnodeCheckpointCapture, CoreWindowVnodeState,
    PreflightedCoreWindowVnodeArchive, PreparedCoreWindowRestore, SessionAccState, SessionDeadline,
    SessionGroupState,
};
use crate::aggregate_state::{
    encode_accumulator_states, encode_group_keys, global_aggregate_key, EncodeBudget,
};
use crate::error::DbError;

/// One vnode's window state in columnar form.
///
/// Fixed windows ascend by start; window `i` holds the group rows from `window_group_ends[i - 1]`
/// (0 for the first) to `window_group_ends[i]`. Session group `i` holds the interval rows from
/// `group_session_ends[i - 1]` to `group_session_ends[i]`, ascending by start. `keys_ipc` holds one
/// row per group (empty without GROUP BY), and each `acc_state_ipc` batch one row per fixed-window
/// group or session interval.
#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct CoreWindowVnodeCheckpoint {
    pub fingerprint: u64,
    pub vnode: u32,
    pub window_type: u8,
    pub frontier_floor_ms: i64,
    pub window_starts: Vec<i64>,
    pub window_group_ends: Vec<u32>,
    pub group_session_ends: Vec<u32>,
    pub session_starts: Vec<i64>,
    pub session_ends: Vec<i64>,
    pub keys_ipc: Vec<u8>,
    pub acc_state_ipc: Vec<Vec<u8>>,
}

/// Group and state row counts declared by a frame's rosters.
#[derive(Clone, Copy)]
struct FrameRows {
    groups: usize,
    states: usize,
}

/// A frame's decoded key and accumulator-state columns.
struct FrameColumns {
    keys: Option<arrow::row::Rows>,
    states: Vec<RecordBatch>,
}

impl CoreWindowVnodeCheckpoint {
    pub(crate) fn retained_serialization_bytes(&self) -> Result<usize, DbError> {
        fn roster<T>(values: &Vec<T>) -> Option<usize> {
            values.capacity().checked_mul(std::mem::size_of::<T>())
        }

        [
            roster(&self.window_starts),
            roster(&self.window_group_ends),
            roster(&self.group_session_ends),
            roster(&self.session_starts),
            roster(&self.session_ends),
            roster(&self.acc_state_ipc),
            Some(self.keys_ipc.capacity()),
        ]
        .into_iter()
        .chain(
            self.acc_state_ipc
                .iter()
                .map(|state| Some(state.capacity())),
        )
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes?))
        .ok_or_else(|| {
            DbError::Checkpoint("Core window checkpoint byte accounting overflow".into())
        })
    }
}

/// Cumulative row ends of consecutive runs of `lens` rows.
fn prefix_ends(lens: impl ExactSizeIterator<Item = usize>) -> Result<Vec<u32>, DbError> {
    let mut ends = Vec::with_capacity(lens.len());
    let mut end = 0_usize;
    for len in lens {
        end = end
            .checked_add(len)
            .ok_or_else(|| DbError::Checkpoint("Core window checkpoint row overflow".into()))?;
        ends.push(u32::try_from(end).map_err(|_| {
            DbError::Checkpoint(format!("Core window checkpoint row {end} exceeds u32"))
        })?);
    }
    Ok(ends)
}

impl CoreWindowVnodeCheckpointCapture {
    #[must_use]
    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub(crate) fn encode(
        self,
        max_working_bytes: usize,
    ) -> Result<CoreWindowVnodeCheckpoint, DbError> {
        let mut budget = EncodeBudget::new(max_working_bytes, "Core window");
        budget.retain_roster::<i64>(self.windows.len(), "window start roster")?;
        budget.retain_roster::<u32>(self.windows.len(), "window group roster")?;
        budget.retain_roster::<u32>(self.session_state.len(), "session group roster")?;
        let window_starts = self
            .windows
            .iter()
            .map(|window| window.window_start)
            .collect();
        let window_group_ends = prefix_ends(self.windows.iter().map(|window| window.groups.len()))?;
        let group_session_ends =
            prefix_ends(self.session_state.iter().map(|group| group.sessions.len()))?;
        let sessions = || {
            self.session_state
                .iter()
                .flat_map(|group| group.sessions.iter())
        };
        let session_count = sessions().count();
        budget.retain_roster::<i64>(session_count, "session start roster")?;
        budget.retain_roster::<i64>(session_count, "session end roster")?;
        let session_starts = sessions().map(|session| session.start).collect();
        let session_ends = sessions().map(|session| session.end).collect();

        let window_groups = || self.windows.iter().flat_map(|window| window.groups.iter());
        let keys_ipc = encode_group_keys(
            &self.row_converter,
            self.group_types.len(),
            window_groups()
                .map(|group| &group.key)
                .chain(self.session_state.iter().map(|group| group.key.as_ref())),
            &mut budget,
        )?;
        let acc_state_ipc = encode_accumulator_states(
            &window_groups()
                .map(|group| group.accumulator_states.as_slice())
                .chain(sessions().map(|session| session.accumulator_states.as_slice())),
            &mut budget,
        )?;
        let checkpoint = CoreWindowVnodeCheckpoint {
            fingerprint: self.fingerprint,
            vnode: self.vnode,
            window_type: self.window_type,
            frontier_floor_ms: self.frontier_floor_ms,
            window_starts,
            window_group_ends,
            group_session_ends,
            session_starts,
            session_ends,
            keys_ipc,
            acc_state_ipc,
        };
        debug_assert!(checkpoint
            .retained_serialization_bytes()
            .is_ok_and(|bytes| bytes <= max_working_bytes));
        Ok(checkpoint)
    }
}

impl CoreWindowState {
    pub(super) fn check_frame_identity(
        &self,
        vnode: u32,
        frame_vnode: u32,
        fingerprint: u64,
        window_type: u8,
    ) -> Result<(), DbError> {
        if frame_vnode != vnode {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode frame identity mismatch: frame={frame_vnode}, expected={vnode}"
            )));
        }
        if fingerprint != self.query_fingerprint() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint fingerprint mismatch"
            )));
        }
        if window_type != self.window_type_tag() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint type mismatch"
            )));
        }
        Ok(())
    }

    /// Checks the rosters are canonical for this query: only the assigner's own rosters are
    /// present, starts and row ends strictly ascend, and no window or session group is empty.
    fn check_frame_rosters(
        &self,
        vnode: u32,
        window_starts: impl ExactSizeIterator<Item = i64>,
        window_group_ends: impl ExactSizeIterator<Item = u32>,
        group_session_ends: impl ExactSizeIterator<Item = u32>,
        session_rows: (usize, usize),
    ) -> Result<FrameRows, DbError> {
        let non_canonical = |detail: &str| {
            DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint has a non-canonical {detail}"
            ))
        };
        if matches!(self.assigner, CoreWindowAssigner::Session { .. }) {
            if window_starts.len() != 0 || window_group_ends.len() != 0 {
                return Err(non_canonical("fixed-window roster"));
            }
            let groups = group_session_ends.len();
            if groups > self.max_groups_per_window {
                return Err(DbError::Checkpoint(format!(
                    "Core window session group cardinality exceeds limit {}",
                    self.max_groups_per_window
                )));
            }
            let states = Self::run_rows(group_session_ends, usize::MAX)
                .ok_or_else(|| non_canonical("session group roster"))?;
            if session_rows != (states, states) {
                return Err(non_canonical("session interval roster"));
            }
            return Ok(FrameRows { groups, states });
        }
        if group_session_ends.len() != 0 || session_rows != (0, 0) {
            return Err(non_canonical("session roster"));
        }
        if window_starts.len() != window_group_ends.len() {
            return Err(non_canonical("window roster"));
        }
        let mut previous = None;
        for start in window_starts {
            if previous.is_some_and(|previous| previous >= start) {
                return Err(non_canonical("window start roster"));
            }
            previous = Some(start);
        }
        let groups = Self::run_rows(window_group_ends, self.max_groups_per_window).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "Core window vnode {vnode} window group roster is non-canonical or exceeds limit {}",
                self.max_groups_per_window
            ))
        })?;
        Ok(FrameRows {
            groups,
            states: groups,
        })
    }

    /// The last of strictly ascending row ends whose runs are each 1..=`limit` rows long.
    fn run_rows(ends: impl Iterator<Item = u32>, limit: usize) -> Option<usize> {
        let mut last = 0_u32;
        for end in ends {
            let rows = end.checked_sub(last)?;
            if rows == 0 || rows as usize > limit {
                return None;
            }
            last = end;
        }
        Some(last as usize)
    }

    /// Checks keys are present exactly when the query groups and rows exist, and that every
    /// accumulator has a non-empty state batch exactly when state rows exist.
    fn check_frame_payloads(
        &self,
        vnode: u32,
        rows: FrameRows,
        keys_empty: bool,
        accumulator_batches: usize,
        empty_accumulator_batch: bool,
    ) -> Result<(), DbError> {
        let keys_expected = self.num_group_cols != 0 && rows.groups != 0;
        let accumulators_expected = if rows.states == 0 {
            0
        } else {
            self.agg_specs.len()
        };
        if keys_empty == keys_expected
            || accumulator_batches != accumulators_expected
            || empty_accumulator_batch
        {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint has non-canonical state payloads"
            )));
        }
        Ok(())
    }

    fn check_frame(
        &self,
        vnode: u32,
        checkpoint: &CoreWindowVnodeCheckpoint,
    ) -> Result<FrameRows, DbError> {
        let rows = self.check_frame_rosters(
            vnode,
            checkpoint.window_starts.iter().copied(),
            checkpoint.window_group_ends.iter().copied(),
            checkpoint.group_session_ends.iter().copied(),
            (
                checkpoint.session_starts.len(),
                checkpoint.session_ends.len(),
            ),
        )?;
        self.check_frame_payloads(
            vnode,
            rows,
            checkpoint.keys_ipc.is_empty(),
            checkpoint.acc_state_ipc.len(),
            checkpoint.acc_state_ipc.iter().any(Vec::is_empty),
        )?;
        Ok(rows)
    }

    pub(crate) fn preflight_vnode_bytes<'a>(
        &self,
        vnode: u32,
        vnode_count: u32,
        bytes: &'a [u8],
    ) -> Result<PreflightedCoreWindowVnodeArchive<'a>, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        if vnode >= vnode_count.get() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} is outside vnode_count {}",
                vnode_count.get()
            )));
        }
        let checkpoint = rkyv::access::<ArchivedCoreWindowVnodeCheckpoint, rkyv::rancor::Error>(
            bytes,
        )
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint validation failed: {error}"
            ))
        })?;
        self.check_frame_identity(
            vnode,
            checkpoint.vnode.to_native(),
            checkpoint.fingerprint.to_native(),
            checkpoint.window_type,
        )?;
        let rows = self.check_frame_rosters(
            vnode,
            checkpoint
                .window_starts
                .iter()
                .map(|start| start.to_native()),
            checkpoint
                .window_group_ends
                .iter()
                .map(|end| end.to_native()),
            checkpoint
                .group_session_ends
                .iter()
                .map(|end| end.to_native()),
            (
                checkpoint.session_starts.len(),
                checkpoint.session_ends.len(),
            ),
        )?;
        self.check_frame_payloads(
            vnode,
            rows,
            checkpoint.keys_ipc.is_empty(),
            checkpoint.acc_state_ipc.len(),
            checkpoint
                .acc_state_ipc
                .iter()
                .any(rkyv::vec::ArchivedVec::is_empty),
        )?;
        // Every row costs at least one byte in its IPC batch.
        if rows.groups.max(rows.states) > bytes.len() {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint declares an impossible state roster"
            )));
        }
        Ok(PreflightedCoreWindowVnodeArchive { checkpoint })
    }

    /// Decodes the key and state batches, checking their row counts against the rosters and each
    /// state batch's columns against a fresh accumulator's state.
    fn decode_frame_columns(
        &self,
        checkpoint: &CoreWindowVnodeCheckpoint,
        rows: FrameRows,
    ) -> Result<FrameColumns, DbError> {
        let decode = |bytes: &[u8], expected_rows: usize, component: &str| {
            let batch =
                laminar_core::serialization::deserialize_batch_stream(bytes).map_err(|error| {
                    DbError::Checkpoint(format!("Core window {component} decode: {error}"))
                })?;
            if batch.num_rows() != expected_rows {
                return Err(DbError::Checkpoint(format!(
                    "Core window {component} batch has {} rows; expected {expected_rows}",
                    batch.num_rows()
                )));
            }
            Ok(batch)
        };
        let keys = if checkpoint.keys_ipc.is_empty() {
            None
        } else {
            let batch = decode(&checkpoint.keys_ipc, rows.groups, "group key")?;
            let keys = self
                .row_converter
                .convert_columns(batch.columns())
                .map_err(|error| {
                    DbError::Checkpoint(format!("Core window group key rows: {error}"))
                })?;
            Some(keys)
        };
        let mut states = Vec::with_capacity(checkpoint.acc_state_ipc.len());
        for (spec, bytes) in self.agg_specs.iter().zip(&checkpoint.acc_state_ipc) {
            let batch = decode(bytes, rows.states, "accumulator state")?;
            let expected = spec
                .create_accumulator()
                .and_then(|mut accumulator| {
                    accumulator
                        .state()
                        .map_err(|error| DbError::Checkpoint(error.to_string()))
                })
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "Core window accumulator schema inspection failed: {error}"
                    ))
                })?;
            if batch.num_columns() != expected.len()
                || batch
                    .columns()
                    .iter()
                    .zip(&expected)
                    .any(|(column, state)| column.data_type() != &state.data_type())
            {
                return Err(DbError::Checkpoint(
                    "Core window checkpoint accumulator schema does not match the query".into(),
                ));
            }
            states.push(batch);
        }
        Ok(FrameColumns { keys, states })
    }

    fn frame_group_key(
        &self,
        columns: &FrameColumns,
        group: usize,
        vnode: u32,
    ) -> Result<arrow::row::OwnedRow, DbError> {
        let key = columns
            .keys
            .as_ref()
            .map_or_else(global_aggregate_key, |keys| keys.row(group).owned());
        if self.vnode_for_group_key(&key) != vnode {
            return Err(DbError::Checkpoint(format!(
                "Core window vnode {vnode} checkpoint contains a key for another vnode"
            )));
        }
        Ok(key)
    }

    fn frame_accumulators(
        &self,
        columns: &FrameColumns,
        row: usize,
    ) -> Result<Vec<Box<dyn datafusion_expr::Accumulator>>, DbError> {
        let mut accumulators = Vec::new();
        accumulators
            .try_reserve_exact(self.agg_specs.len())
            .map_err(|error| {
                DbError::Checkpoint(format!("Core window accumulator reserve failed: {error}"))
            })?;
        for (spec, batch) in self.agg_specs.iter().zip(&columns.states) {
            let mut accumulator = spec.create_accumulator().map_err(|error| {
                DbError::Checkpoint(format!("Core window accumulator creation failed: {error}"))
            })?;
            let state: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|column| column.slice(row, 1))
                .collect();
            accumulator.merge_batch(&state).map_err(|error| {
                DbError::Checkpoint(format!("Core window accumulator merge failed: {error}"))
            })?;
            accumulators.push(accumulator);
        }
        Ok(accumulators)
    }

    fn check_fixed_window_start(
        &self,
        window_start: i64,
        frontier_floor_ms: i64,
        (size_ms, alignment_ms, offset_ms): (i64, i64, i64),
    ) -> Result<(), DbError> {
        if (i128::from(window_start) - i128::from(offset_ms)).rem_euclid(i128::from(alignment_ms))
            != 0
        {
            return Err(DbError::Checkpoint(format!(
                "fixed-window checkpoint start {window_start} is not aligned to the configured assigner"
            )));
        }
        let window_end = window_start.checked_add(size_ms).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "fixed-window checkpoint start {window_start} cannot fit its {size_ms}ms size"
            ))
        })?;
        if frontier_floor_ms != i64::MIN
            && window_end.saturating_add(self.allowed_lateness_ms) <= frontier_floor_ms
        {
            return Err(DbError::Checkpoint(format!(
                "fixed-window checkpoint retains already-closed window {window_start}"
            )));
        }
        Ok(())
    }

    pub(super) fn prepare_fixed_window_restore(
        &self,
        checkpoint: &CoreWindowVnodeCheckpoint,
        vnode: u32,
        assigner: (i64, i64, i64),
    ) -> Result<PreparedCoreWindowRestore, DbError> {
        let rows = self.check_frame(vnode, checkpoint)?;
        let columns = self.decode_frame_columns(checkpoint, rows)?;
        let mut windows = BTreeMap::new();
        let mut first = 0_usize;
        for (&window_start, &end) in checkpoint
            .window_starts
            .iter()
            .zip(&checkpoint.window_group_ends)
        {
            self.check_fixed_window_start(window_start, checkpoint.frontier_floor_ms, assigner)?;
            let end = end as usize;
            let mut groups = FxHashMap::default();
            groups.try_reserve(end - first).map_err(|error| {
                DbError::Checkpoint(format!(
                    "fixed-window checkpoint group reserve failed: {error}"
                ))
            })?;
            for group in first..end {
                let key = self.frame_group_key(&columns, group, vnode)?;
                let accumulators = self.frame_accumulators(&columns, group)?;
                if groups.insert(key, accumulators).is_some() {
                    return Err(DbError::Checkpoint(format!(
                        "fixed-window checkpoint window {window_start} contains a duplicate group key"
                    )));
                }
            }
            windows.insert(window_start, groups);
            first = end;
        }

        let state = (!windows.is_empty()).then(|| {
            let accounted_state_bytes = Self::fixed_windows_bytes(&windows);
            Box::new(CoreWindowVnodeState {
                windows,
                session_groups: FxHashMap::default(),
                session_deadlines: BTreeSet::new(),
                accounted_state_bytes,
            })
        });
        Ok(PreparedCoreWindowRestore {
            state,
            frontier_floor_ms: checkpoint.frontier_floor_ms,
        })
    }

    fn restore_session(
        &self,
        checkpoint: &CoreWindowVnodeCheckpoint,
        row: usize,
        gap_ms: i64,
        previous_end: Option<i64>,
    ) -> Result<(i64, i64), DbError> {
        let (start, end) = (checkpoint.session_starts[row], checkpoint.session_ends[row]);
        let minimum_end = start.checked_add(gap_ms).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "session checkpoint interval starting at {start} cannot fit its {gap_ms}ms gap"
            ))
        })?;
        if end < minimum_end {
            return Err(DbError::Checkpoint(format!(
                "session checkpoint contains invalid interval [{start}, {end})"
            )));
        }
        if previous_end.is_some_and(|previous| previous >= start) {
            return Err(DbError::Checkpoint(
                "session checkpoint intervals are not strictly ordered and disjoint".into(),
            ));
        }
        if checkpoint.frontier_floor_ms != i64::MIN
            && end.saturating_add(self.allowed_lateness_ms) <= checkpoint.frontier_floor_ms
        {
            return Err(DbError::Checkpoint(format!(
                "session checkpoint retains already-closed interval [{start}, {end})"
            )));
        }
        Ok((start, end))
    }

    pub(super) fn prepare_session_window_restore(
        &self,
        checkpoint: &CoreWindowVnodeCheckpoint,
        vnode: u32,
        gap_ms: i64,
    ) -> Result<PreparedCoreWindowRestore, DbError> {
        let rows = self.check_frame(vnode, checkpoint)?;
        let columns = self.decode_frame_columns(checkpoint, rows)?;
        let mut session_groups = FxHashMap::default();
        let mut session_deadlines = BTreeSet::new();
        session_groups.try_reserve(rows.groups).map_err(|error| {
            DbError::Checkpoint(format!("session checkpoint group reserve failed: {error}"))
        })?;
        let mut first = 0_usize;
        for (group, &group_end) in checkpoint.group_session_ends.iter().enumerate() {
            let key = Arc::new(self.frame_group_key(&columns, group, vnode)?);
            let mut sessions = BTreeMap::new();
            let mut previous_end = None;
            for row in first..group_end as usize {
                let (start, end) = self.restore_session(checkpoint, row, gap_ms, previous_end)?;
                if !session_deadlines.insert(SessionDeadline::new(
                    Arc::clone(&key),
                    start,
                    end,
                    self.allowed_lateness_ms,
                )) {
                    return Err(DbError::Checkpoint(
                        "session checkpoint contains a duplicate deadline".into(),
                    ));
                }
                let accs = self.frame_accumulators(&columns, row)?;
                sessions.insert(start, SessionAccState { start, end, accs });
                previous_end = Some(end);
            }
            if session_groups
                .insert(key, SessionGroupState { sessions })
                .is_some()
            {
                return Err(DbError::Checkpoint(
                    "session checkpoint contains a duplicate group key".into(),
                ));
            }
            first = group_end as usize;
        }

        let state = (!session_groups.is_empty()).then(|| {
            let accounted_state_bytes =
                Self::session_groups_bytes(&session_groups, &session_deadlines);
            Box::new(CoreWindowVnodeState {
                windows: BTreeMap::new(),
                session_groups,
                session_deadlines,
                accounted_state_bytes,
            })
        });
        Ok(PreparedCoreWindowRestore {
            state,
            frontier_floor_ms: checkpoint.frontier_floor_ms,
        })
    }
}
