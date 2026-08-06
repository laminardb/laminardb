//! Incremental aggregation state for streaming GROUP BY queries.
//!
//! One `IncrementalAggState` per operator; grouped accumulators are partitioned by stable vnodes.

use std::num::NonZeroU32;
use std::sync::Arc;

use rustc_hash::{FxBuildHasher, FxHashMap, FxHashSet};

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::AggregateUDF;
use laminar_core::state::{KeyGroupCount, PartitionKeyCodecV1};

use crate::db::exact_table_reference;
use crate::error::DbError;

mod accounting;
mod checkpoints;
mod compile;
mod keys;
mod scalar_ipc;
mod vnode_state;
pub(crate) use checkpoints::AggStateArchiveRestoreProfile;
pub(crate) use checkpoints::{
    query_fingerprint, query_fingerprint_with_config, AggStateCheckpoint, EmittedCheckpoint,
    GroupCheckpoint, WindowCheckpoint,
};
#[cfg(test)]
pub(crate) use compile::expr_to_sql;
pub(crate) use compile::{
    apply_compiled_having, compile_having_filter, extract_clauses, find_aggregate,
    CompiledProjection, PreAggBuilder,
};
pub(crate) use keys::{
    global_aggregate_key, row_to_scalar_key_with_types, scalar_key_to_owned_row,
};
pub(crate) use scalar_ipc::{ipc_to_scalars, scalars_to_ipc, scalars_to_ipc_bounded};
use vnode_state::{AggregateVnodeSlots, AggregateVnodeState};

/// Builds the per-window result batch for one closed window.
/// Output schema: `[group_cols..., agg_outputs...]`.
pub(crate) fn emit_window_batch(
    groups: FxHashMap<arrow::row::OwnedRow, Vec<Box<dyn datafusion_expr::Accumulator>>>,
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    agg_specs: &[AggFuncSpec],
    output_schema: &SchemaRef,
) -> Result<Option<RecordBatch>, DbError> {
    let num_rows = groups.len();
    if num_rows == 0 {
        return Ok(None);
    }

    // Collect keys and evaluate accumulators in a single drain pass.
    let mut row_keys: Vec<arrow::row::OwnedRow> = Vec::with_capacity(num_rows);
    let mut agg_scalars: Vec<Vec<ScalarValue>> = (0..agg_specs.len())
        .map(|_| Vec::with_capacity(num_rows))
        .collect();

    for (key, mut accs) in groups {
        row_keys.push(key);
        for (i, acc) in accs.iter_mut().enumerate() {
            let sv = acc
                .evaluate()
                .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;
            agg_scalars[i].push(sv);
        }
    }

    let group_arrays = if num_group_cols == 0 {
        Vec::new()
    } else {
        row_converter
            .convert_rows(row_keys.iter().map(arrow::row::OwnedRow::row))
            .map_err(|e| DbError::Pipeline(format!("group key array: {e}")))?
    };

    let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(agg_specs.len());
    for (agg_idx, scalars) in agg_scalars.into_iter().enumerate() {
        let spec = &agg_specs[agg_idx];
        let array = ScalarValue::iter_to_array(scalars)
            .map_err(|e| DbError::Pipeline(format!("agg result array: {e}")))?;
        if array.data_type() == &spec.return_type {
            agg_arrays.push(array);
        } else {
            let casted = arrow::compute::cast(&array, &spec.return_type).unwrap_or(array);
            agg_arrays.push(casted);
        }
    }

    let mut all_arrays = Vec::with_capacity(group_arrays.len() + agg_arrays.len());
    all_arrays.extend(group_arrays);
    all_arrays.extend(agg_arrays);

    let batch = RecordBatch::try_new(Arc::clone(output_schema), all_arrays)
        .map_err(|e| DbError::Pipeline(format!("result batch build: {e}")))?;

    Ok(Some(batch))
}

pub(crate) struct AggFuncSpec {
    pub(crate) udf: Arc<AggregateUDF>,
    pub(crate) input_types: Vec<DataType>,
    pub(crate) input_col_indices: Vec<usize>,
    pub(crate) output_name: String,
    pub(crate) return_type: DataType,
    pub(crate) distinct: bool,
    pub(crate) is_count_star: bool,
    pub(crate) filter_col_index: Option<usize>,
}

impl AggFuncSpec {
    pub(crate) fn create_accumulator(
        &self,
    ) -> Result<Box<dyn datafusion_expr::Accumulator>, DbError> {
        let return_field = Arc::new(Field::new(
            &self.output_name,
            self.return_type.clone(),
            true,
        ));
        let schema = Schema::new(
            self.input_types
                .iter()
                .enumerate()
                .map(|(i, dt)| Field::new(format!("col_{i}"), dt.clone(), true))
                .collect::<Vec<_>>(),
        );
        let expr_fields: Vec<Arc<Field>> = self
            .input_types
            .iter()
            .enumerate()
            .map(|(i, dt)| Arc::new(Field::new(format!("col_{i}"), dt.clone(), true)))
            .collect();
        let args = AccumulatorArgs {
            return_field,
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: self.udf.name(),
            is_distinct: self.distinct,
            exprs: &[],
            expr_fields: &expr_fields,
        };
        self.udf.accumulator(args).map_err(|e| {
            DbError::Pipeline(format!(
                "accumulator creation failed for '{}': {e}",
                self.udf.name()
            ))
        })
    }

    /// Create a retractable accumulator for changelog streams (`__weight` as last input).
    pub(crate) fn create_retractable_accumulator(
        &self,
    ) -> Result<Box<dyn datafusion_expr::Accumulator>, DbError> {
        crate::retractable_accumulator::create_retractable(
            &self.udf.name().to_lowercase(),
            &self.return_type,
            self.is_count_star,
        )
    }
}

/// Snapshot an accumulator's state for a checkpoint and rebuild it in place,
/// returning the raw `Vec<ScalarValue>` (no IPC framing — the columnar agg path
/// batches across groups instead).
///
/// `Accumulator::state()` may drain internal state (e.g. DISTINCT hash sets); we
/// rebuild the accumulator from the snapshot so it keeps running correctly. The
/// rebuild happens before returning, so a later encode failure can never leave
/// the accumulator empty. `retractable` must match how the accumulator was created.
pub(crate) fn snapshot_state_scalars(
    acc: &mut Box<dyn datafusion_expr::Accumulator>,
    spec: &AggFuncSpec,
    retractable: bool,
) -> Result<Vec<ScalarValue>, DbError> {
    let state = acc
        .state()
        .map_err(|e| DbError::Pipeline(format!("accumulator state: {e}")))?;
    let arrays: Vec<ArrayRef> = state
        .iter()
        .map(|sv| {
            sv.to_array()
                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
        })
        .collect::<Result<_, _>>()?;
    let mut rebuilt = if retractable {
        spec.create_retractable_accumulator()?
    } else {
        spec.create_accumulator()?
    };
    rebuilt
        .merge_batch(&arrays)
        .map_err(|e| DbError::Pipeline(format!("accumulator rebuild: {e}")))?;
    *acc = rebuilt;
    Ok(state)
}

/// IPC-bytes variant of [`snapshot_state_scalars`], kept for EOWC's per-group
/// [`GroupCheckpoint`] shape.
pub(crate) fn snapshot_and_rebuild(
    acc: &mut Box<dyn datafusion_expr::Accumulator>,
    spec: &AggFuncSpec,
    retractable: bool,
) -> Result<Vec<u8>, DbError> {
    let state = snapshot_state_scalars(acc, spec, retractable)?;
    scalars_to_ipc(&state)
}

fn arrays_to_ipc_bounded(arrays: &[ArrayRef], max_bytes: usize) -> Result<Vec<u8>, DbError> {
    if arrays.is_empty() {
        return Ok(Vec::new());
    }
    let fields: Vec<Arc<Field>> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Arc::new(Field::new(format!("c{i}"), a.data_type().clone(), true)))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays.to_vec())
        .map_err(|e| DbError::Pipeline(format!("columnar batch build: {e}")))?;
    laminar_core::serialization::serialize_batches_stream_bounded(
        batch.schema().as_ref(),
        std::iter::once(&batch),
        max_bytes,
    )
    .map_err(|e| DbError::Pipeline(format!("columnar IPC encode: {e}")))
}

struct CapturedAggregateGroup {
    key: arrow::row::OwnedRow,
    accumulator_states: Vec<Vec<ScalarValue>>,
    last_updated_ms: i64,
}

pub(crate) struct AggregateVnodeCheckpointCapture {
    fingerprint: u64,
    group_types: Arc<[DataType]>,
    row_converter: Arc<arrow::row::RowConverter>,
    groups: Vec<CapturedAggregateGroup>,
    last_emitted: Vec<(arrow::row::OwnedRow, Vec<ScalarValue>)>,
    retained_bytes: u64,
}

impl AggregateVnodeCheckpointCapture {
    pub(crate) const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.groups.is_empty() && self.last_emitted.is_empty()
    }

    fn calculate_retained_bytes(&self) -> u64 {
        fn add(total: &mut u64, bytes: usize) {
            *total = total.saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        }

        let mut retained = u64::try_from(std::mem::size_of::<Self>()).unwrap_or(u64::MAX);
        add(
            &mut retained,
            self.group_types
                .len()
                .saturating_mul(std::mem::size_of::<DataType>()),
        );
        add(
            &mut retained,
            self.groups
                .capacity()
                .saturating_mul(std::mem::size_of::<CapturedAggregateGroup>()),
        );
        for group in &self.groups {
            add(&mut retained, group.key.as_ref().len());
            add(
                &mut retained,
                group
                    .accumulator_states
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Vec<ScalarValue>>()),
            );
            for state in &group.accumulator_states {
                for value in state {
                    add(&mut retained, value.size());
                }
                add(
                    &mut retained,
                    state
                        .capacity()
                        .saturating_sub(state.len())
                        .saturating_mul(std::mem::size_of::<ScalarValue>()),
                );
            }
        }
        add(
            &mut retained,
            self.last_emitted
                .capacity()
                .saturating_mul(std::mem::size_of::<(arrow::row::OwnedRow, Vec<ScalarValue>)>()),
        );
        for (key, values) in &self.last_emitted {
            add(&mut retained, key.as_ref().len());
            for value in values {
                add(&mut retained, value.size());
            }
            add(
                &mut retained,
                values
                    .capacity()
                    .saturating_sub(values.len())
                    .saturating_mul(std::mem::size_of::<ScalarValue>()),
            );
        }
        retained
    }

    pub(crate) fn encode(self, max_working_bytes: usize) -> Result<AggStateCheckpoint, DbError> {
        fn checked_product(left: usize, right: usize, component: &str) -> Result<usize, DbError> {
            left.checked_mul(right).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate {component} checkpoint accounting overflow"
                ))
            })
        }

        fn checked_sum(
            values: impl IntoIterator<Item = usize>,
            component: &str,
        ) -> Result<usize, DbError> {
            values.into_iter().try_fold(0usize, |total, value| {
                total.checked_add(value).ok_or_else(|| {
                    DbError::Checkpoint(format!(
                        "aggregate {component} checkpoint accounting overflow"
                    ))
                })
            })
        }

        fn array_scratch_bytes<'a>(
            values: impl IntoIterator<Item = &'a ScalarValue>,
            rows: usize,
            columns: usize,
            component: &str,
        ) -> Result<usize, DbError> {
            let payload = checked_sum(values.into_iter().map(ScalarValue::size), component)?;
            let copied_payload = checked_product(payload, 2, component)?;
            let cell_overhead = checked_product(rows, columns, component)
                .and_then(|cells| checked_product(cells, 32, component))?;
            checked_sum(
                [
                    copied_payload,
                    cell_overhead,
                    checked_product(columns, std::mem::size_of::<ArrayRef>(), component)?,
                ],
                component,
            )
        }

        fn row_scratch_bytes(
            payload_bytes: usize,
            rows: usize,
            columns: usize,
            component: &str,
        ) -> Result<usize, DbError> {
            checked_sum(
                [
                    checked_product(payload_bytes, 2, component)?,
                    checked_product(rows, columns, component)
                        .and_then(|cells| checked_product(cells, 32, component))?,
                    checked_product(columns, std::mem::size_of::<ArrayRef>(), component)?,
                ],
                component,
            )
        }

        fn transient_limit(
            remaining: usize,
            scratch: usize,
            component: &str,
        ) -> Result<usize, DbError> {
            remaining.checked_sub(scratch).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate {component} scratch space exceeded its cumulative checkpoint byte limit"
                ))
            })
        }

        fn retain_roster<T>(
            remaining: &mut usize,
            capacity: usize,
            component: &str,
        ) -> Result<(), DbError> {
            let bytes = checked_product(capacity, std::mem::size_of::<T>(), component)?;
            *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate {component} exceeded its cumulative checkpoint byte limit"
                ))
            })?;
            Ok(())
        }

        fn retain_encoded(
            remaining: &mut usize,
            encoded: Vec<u8>,
            component: &str,
        ) -> Result<Vec<u8>, DbError> {
            *remaining = remaining.checked_sub(encoded.capacity()).ok_or_else(|| {
                DbError::Checkpoint(format!(
                    "aggregate {component} exceeded its cumulative checkpoint byte limit"
                ))
            })?;
            Ok(encoded)
        }

        let mut remaining = max_working_bytes;
        let keys_ipc = if self.groups.is_empty() || self.group_types.is_empty() {
            Vec::new()
        } else {
            let key_payload = checked_sum(
                self.groups.iter().map(|group| group.key.as_ref().len()),
                "group keys",
            )?;
            let scratch = row_scratch_bytes(
                key_payload,
                self.groups.len(),
                self.group_types.len(),
                "group keys",
            )?;
            let encode_limit = transient_limit(remaining, scratch, "group keys")?;
            let arrays = self
                .row_converter
                .convert_rows(self.groups.iter().map(|group| group.key.row()))
                .map_err(|error| {
                    DbError::Checkpoint(format!("aggregate group key array build: {error}"))
                })?;
            let encoded = arrays_to_ipc_bounded(&arrays, encode_limit)?;
            retain_encoded(&mut remaining, encoded, "group keys")?
        };

        let accumulator_count = self
            .groups
            .first()
            .map_or(0, |group| group.accumulator_states.len());
        if self
            .groups
            .iter()
            .any(|group| group.accumulator_states.len() != accumulator_count)
        {
            return Err(DbError::Checkpoint(
                "aggregate checkpoint capture has inconsistent accumulator arity".into(),
            ));
        }

        retain_roster::<Vec<u8>>(
            &mut remaining,
            accumulator_count,
            "accumulator state roster",
        )?;
        let mut acc_state_ipc = Vec::with_capacity(accumulator_count);
        for accumulator_index in 0..accumulator_count {
            let arity = self
                .groups
                .first()
                .map_or(0, |group| group.accumulator_states[accumulator_index].len());
            if self
                .groups
                .iter()
                .any(|group| group.accumulator_states[accumulator_index].len() != arity)
            {
                return Err(DbError::Checkpoint(
                    "aggregate checkpoint capture has inconsistent state arity".into(),
                ));
            }
            let scratch = array_scratch_bytes(
                self.groups
                    .iter()
                    .flat_map(|group| group.accumulator_states[accumulator_index].iter()),
                self.groups.len(),
                arity,
                "accumulator state",
            )?;
            let encode_limit = transient_limit(remaining, scratch, "accumulator state")?;
            let mut columns = Vec::with_capacity(arity);
            for column in 0..arity {
                columns.push(
                    ScalarValue::iter_to_array(
                        self.groups.iter().map(|group| {
                            group.accumulator_states[accumulator_index][column].clone()
                        }),
                    )
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate accumulator state array build: {error}"
                        ))
                    })?,
                );
            }
            let encoded = arrays_to_ipc_bounded(&columns, encode_limit)?;
            acc_state_ipc.push(retain_encoded(
                &mut remaining,
                encoded,
                "accumulator state",
            )?);
        }

        retain_roster::<i64>(&mut remaining, self.groups.len(), "last-updated roster")?;
        let last_updated_ms = self
            .groups
            .iter()
            .map(|group| group.last_updated_ms)
            .collect::<Vec<_>>();

        retain_roster::<EmittedCheckpoint>(
            &mut remaining,
            self.last_emitted.len(),
            "changelog roster",
        )?;
        let mut last_emitted = Vec::with_capacity(self.last_emitted.len());
        for (key, values) in self.last_emitted {
            let key_scratch = row_scratch_bytes(
                key.as_ref().len(),
                1,
                self.group_types.len(),
                "changelog key",
            )?;
            let key_limit = transient_limit(remaining, key_scratch, "changelog key")?;
            let key = row_to_scalar_key_with_types(&self.row_converter, &key, &self.group_types)?;
            let key = scalars_to_ipc_bounded(&key, key_limit)?;
            let key = retain_encoded(&mut remaining, key, "changelog key")?;
            let value_scratch =
                array_scratch_bytes(values.iter(), 1, values.len(), "changelog values")?;
            let value_limit = transient_limit(remaining, value_scratch, "changelog values")?;
            let values = scalars_to_ipc_bounded(&values, value_limit)?;
            let values = retain_encoded(&mut remaining, values, "changelog values")?;
            last_emitted.push(EmittedCheckpoint { key, values });
        }

        let checkpoint = AggStateCheckpoint {
            fingerprint: self.fingerprint,
            keys_ipc,
            acc_state_ipc,
            last_updated_ms,
            last_emitted,
        };
        debug_assert!(checkpoint
            .retained_serialization_bytes()
            .is_ok_and(|bytes| bytes <= max_working_bytes));
        Ok(checkpoint)
    }
}

/// Reject retractable MIN/MAX checkpoint work before `Accumulator::state()` materializes the
/// counted multisets as Arrow lists. The charge is the cached accumulator-reported working bytes
/// for every selected group. It is deliberately conservative when the same group also contains a
/// fixed-size SUM/COUNT/AVG accumulator, and is not an estimate of the final IPC wire size.
fn ensure_retractable_extremum_checkpoint_budget<'a>(
    agg_specs: &[AggFuncSpec],
    retractable: bool,
    entries: impl IntoIterator<Item = &'a GroupEntry>,
    limit_bytes: usize,
    context: &str,
) -> Result<(), DbError> {
    if !retractable
        || !agg_specs
            .iter()
            .any(|spec| matches!(spec.udf.name().to_ascii_lowercase().as_str(), "min" | "max"))
    {
        return Ok(());
    }

    let mut charged_bytes = 0_usize;
    for entry in entries {
        let Some(next) = charged_bytes.checked_add(entry.accumulator_reported_bytes) else {
            return Err(DbError::RetractableExtremumCheckpointBudgetExceeded {
                context: context.to_string(),
                charged_bytes: usize::MAX,
                limit_bytes,
            });
        };
        charged_bytes = next;
        if charged_bytes > limit_bytes {
            return Err(DbError::RetractableExtremumCheckpointBudgetExceeded {
                context: context.to_string(),
                charged_bytes,
                limit_bytes,
            });
        }
    }
    Ok(())
}

/// Per-group decoded state: `(key, last_updated_ms, per-accumulator state arrays)`.
type DecodedGroupState = (arrow::row::OwnedRow, i64, Vec<Vec<ArrayRef>>);

struct DecodedAggMutation {
    groups: Vec<DecodedGroupState>,
    last_emitted: FxHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
}

/// A decoded vnode image built off-side before publication.
struct StagedAggMutation {
    groups: FxHashMap<arrow::row::OwnedRow, GroupEntry>,
    last_emitted: FxHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
}

fn validate_unique_decoded_group_keys(groups: &[DecodedGroupState]) -> Result<(), DbError> {
    let mut keys: FxHashSet<&[u8]> =
        FxHashSet::with_capacity_and_hasher(groups.len(), FxBuildHasher);
    if groups.iter().any(|(key, _, _)| !keys.insert(key.as_ref())) {
        return Err(DbError::Pipeline(
            "aggregate checkpoint contains a duplicate group key".into(),
        ));
    }
    Ok(())
}

/// Decode a columnar checkpoint to per-group state arrays. Keys round-trip through
/// the row converter's own column types, so no per-type coercion is needed.
fn decode_columnar_state_arrays(
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    keys_ipc: &[u8],
    acc_state_ipc: &[Vec<u8>],
    last_updated_ms: &[i64],
) -> Result<Vec<DecodedGroupState>, DbError> {
    let n = last_updated_ms.len();
    if n == 0 {
        return Ok(Vec::new());
    }
    // A grouped aggregate always encodes keys; empty key bytes with groups present
    // means a truncated/corrupt checkpoint, not the global (no-GROUP-BY) key.
    if num_group_cols > 0 && keys_ipc.is_empty() {
        return Err(DbError::Pipeline(format!(
            "columnar checkpoint shape: {n} groups but no key bytes"
        )));
    }
    let key_rows = if keys_ipc.is_empty() {
        None
    } else {
        let batch = laminar_core::serialization::deserialize_batch_stream(keys_ipc)
            .map_err(|e| DbError::Pipeline(format!("keys IPC decode: {e}")))?;
        let rows = row_converter
            .convert_columns(batch.columns())
            .map_err(|e| DbError::Pipeline(format!("keys row convert: {e}")))?;
        if rows.num_rows() != n {
            return Err(DbError::Pipeline(format!(
                "columnar checkpoint shape: {} key rows vs {n} groups",
                rows.num_rows()
            )));
        }
        Some(rows)
    };
    let acc_batches: Vec<Option<RecordBatch>> = acc_state_ipc
        .iter()
        .map(|bytes| {
            if bytes.is_empty() {
                return Ok(None);
            }
            let batch = laminar_core::serialization::deserialize_batch_stream(bytes)
                .map_err(|e| DbError::Pipeline(format!("acc state IPC decode: {e}")))?;
            if batch.num_rows() != n {
                return Err(DbError::Pipeline(format!(
                    "columnar checkpoint shape: acc batch {} rows vs {n} groups",
                    batch.num_rows()
                )));
            }
            Ok(Some(batch))
        })
        .collect::<Result<_, _>>()?;

    let mut out = Vec::with_capacity(n);
    for (j, &updated_ms) in last_updated_ms.iter().enumerate() {
        let row_key = match &key_rows {
            Some(rows) => rows.row(j).owned(),
            None => global_aggregate_key(),
        };
        let mut state_arrays: Vec<Vec<ArrayRef>> = Vec::with_capacity(acc_batches.len());
        for batch in &acc_batches {
            let arrays = match batch {
                Some(b) => (0..b.num_columns())
                    .map(|c| {
                        let sv = ScalarValue::try_from_array(b.column(c), j)
                            .map_err(|e| DbError::Pipeline(format!("acc state scalar: {e}")))?;
                        sv.to_array()
                            .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            };
            state_arrays.push(arrays);
        }
        out.push((row_key, updated_ms, state_arrays));
    }
    Ok(out)
}

/// Build fresh accumulators, merging each decoded state-array set into its own.
fn build_accumulators_from_state(
    agg_specs: &[AggFuncSpec],
    retractable: bool,
    state_arrays: &[Vec<ArrayRef>],
) -> Result<Vec<Box<dyn datafusion_expr::Accumulator>>, DbError> {
    let mut accs = Vec::with_capacity(agg_specs.len());
    for (i, spec) in agg_specs.iter().enumerate() {
        let mut acc = if retractable {
            spec.create_retractable_accumulator()?
        } else {
            spec.create_accumulator()?
        };
        if let Some(arrays) = state_arrays.get(i) {
            if !arrays.is_empty() {
                acc.merge_batch(arrays)
                    .map_err(|e| DbError::Pipeline(format!("accumulator merge: {e}")))?;
            }
        }
        accs.push(acc);
    }
    Ok(accs)
}

/// Reject a checkpoint whose per-accumulator state-batch count doesn't match the
/// query's aggregates — a truncated/corrupt slice the fingerprint can't catch.
/// An empty checkpoint carries no acc batches by construction, so skip it.
fn validate_columnar_acc_shape(
    checkpoint: &AggStateCheckpoint,
    agg_specs: &[AggFuncSpec],
) -> Result<(), DbError> {
    if checkpoint.last_updated_ms.is_empty() {
        if !checkpoint.keys_ipc.is_empty()
            || !checkpoint.acc_state_ipc.is_empty()
            || !checkpoint.last_emitted.is_empty()
        {
            return Err(DbError::Pipeline(
                "non-canonical empty aggregate checkpoint contains state payloads".into(),
            ));
        }
        return Ok(());
    }
    if checkpoint.acc_state_ipc.len() != agg_specs.len() {
        return Err(DbError::Pipeline(format!(
            "columnar checkpoint shape: {} accumulator states vs {} aggregates",
            checkpoint.acc_state_ipc.len(),
            agg_specs.len()
        )));
    }
    Ok(())
}

/// Row-concatenate two IPC stream batches sharing a schema; an empty side passes
/// the other through unchanged.
pub(crate) struct IncrementalAggState {
    query_sql: String,
    #[cfg(test)]
    pre_agg_sql: String,
    num_group_cols: usize,
    key_group_count: KeyGroupCount,
    group_types: Vec<DataType>,
    agg_specs: Vec<AggFuncSpec>,
    vnode_states: AggregateVnodeSlots,
    row_converter: Arc<arrow::row::RowConverter>,
    output_schema: SchemaRef,
    compiled_projection: Option<CompiledProjection>,
    cached_pre_agg_physical: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    max_groups: usize,
    max_retractable_extremum_checkpoint_bytes: usize,
    emit_changelog: bool,
    weight_col_idx: Option<usize>,
    checkpointed_vnodes: Box<[bool]>,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq)]
/// Bookkeeping-only test snapshot; accumulator values are evaluated separately when relevant.
pub(crate) struct AggregateWorkingSetSnapshot {
    pub(crate) group_timestamps: std::collections::BTreeMap<Vec<u8>, i64>,
    pub(crate) last_emitted: std::collections::BTreeMap<Vec<u8>, Vec<ScalarValue>>,
    pub(crate) emit_dirty_keys: std::collections::BTreeSet<Vec<u8>>,
    pub(crate) checkpoint_dirty_keys:
        std::collections::BTreeMap<u32, std::collections::BTreeSet<Vec<u8>>>,
    pub(crate) last_emitted_dirty_keys:
        std::collections::BTreeMap<u32, std::collections::BTreeSet<Vec<u8>>>,
}

impl IncrementalAggState {
    /// Number of leading GROUP BY columns; used by the shuffle path for hashing.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub(crate) fn num_group_cols(&self) -> usize {
        self.num_group_cols
    }

    pub(crate) fn cluster_state_rejection(&self, reads_changelog: bool) -> Option<String> {
        for spec in &self.agg_specs {
            let name = spec.udf.name().to_ascii_lowercase();
            if spec.distinct {
                return Some(format!(
                    "DISTINCT aggregate '{name}' has unbounded per-key state and no spillable vnode lifecycle"
                ));
            }
            if reads_changelog && matches!(name.as_str(), "min" | "max") {
                return Some(format!(
                    "aggregate '{name}' over a changelog uses an unbounded counted multiset and has no spillable vnode lifecycle"
                ));
            }
            if !matches!(name.as_str(), "count" | "sum" | "avg" | "min" | "max") {
                return Some(format!(
                    "aggregate '{name}' has unbounded or unclassified per-key state and no spillable vnode lifecycle"
                ));
            }
        }
        None
    }

    #[cfg(test)]
    pub(crate) fn set_max_groups_for_test(&mut self, max_groups: usize) {
        self.max_groups = max_groups;
    }

    pub(crate) fn set_max_retractable_extremum_checkpoint_bytes(&mut self, bytes: usize) {
        assert!(
            bytes > 0,
            "retractable-extremum checkpoint budget must be nonzero"
        );
        self.max_retractable_extremum_checkpoint_bytes = bytes;
    }

    /// Freeze the query-plan limits used by the complete archive preflight pass.
    pub(crate) fn vnode_archive_restore_profile(&self) -> AggStateArchiveRestoreProfile {
        checkpoints::AggStateArchiveRestoreProfile::new(
            self.query_fingerprint(),
            self.agg_specs.len(),
            self.max_groups,
            self.num_group_cols > 0,
            self.emit_changelog,
        )
    }

    /// Reject an impossible final transition cardinality before decoding Arrow payloads.
    pub(crate) fn preflight_vnode_transition_cardinality(
        &self,
        vnode_count: u32,
        restored_lower_bounds: &[(u32, usize)],
        revoked: &rustc_hash::FxHashSet<u32>,
    ) -> Result<(), DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        let mut transitioned_group_count = 0_usize;
        for vnode in revoked {
            if *vnode >= vnode_count.get() {
                return Err(DbError::Pipeline(format!(
                    "revoked vnode {vnode} is outside vnode_count {}",
                    vnode_count.get()
                )));
            }
            transitioned_group_count = checked_vnode_transition_capacity(
                transitioned_group_count,
                self.vnode_states
                    .get(*vnode)
                    .map_or(0, |state| state.groups.len()),
            )?;
        }

        let mut restored = rustc_hash::FxHashSet::default();
        restored
            .try_reserve(restored_lower_bounds.len())
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "aggregate vnode transition could not reserve restored preflight roster: {error}"
                ))
            })?;
        let mut replacement_lower_bound = 0_usize;
        for &(vnode, vnode_lower_bound) in restored_lower_bounds {
            if self.num_group_cols == 0 && vnode != 0 {
                return Err(DbError::Pipeline(format!(
                    "global aggregate restore targeted vnode {vnode}; only vnode 0 is valid"
                )));
            }
            if vnode >= vnode_count.get() {
                return Err(DbError::Pipeline(format!(
                    "restored vnode {vnode} is outside vnode_count {}",
                    vnode_count.get()
                )));
            }
            if !restored.insert(vnode) {
                return Err(DbError::Pipeline(format!(
                    "aggregate vnode transition repeats restored vnode {vnode}"
                )));
            }
            if !revoked.contains(&vnode) {
                transitioned_group_count = checked_vnode_transition_capacity(
                    transitioned_group_count,
                    self.vnode_states
                        .get(vnode)
                        .map_or(0, |state| state.groups.len()),
                )?;
            }
            replacement_lower_bound =
                checked_vnode_transition_capacity(replacement_lower_bound, vnode_lower_bound)?;
        }
        let retained_group_count = checked_vnode_transition_final_count(
            self.vnode_states.resident_group_count(),
            transitioned_group_count,
            0,
        )?;
        let final_lower_bound =
            checked_vnode_transition_capacity(retained_group_count, replacement_lower_bound)?;
        if final_lower_bound > self.max_groups {
            return Err(DbError::Pipeline(format!(
                "aggregate group limit exceeded during vnode transition preflight: retained={retained_group_count}, replacement_lower_bound={replacement_lower_bound}, limit={}",
                self.max_groups
            )));
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn logical_group_count_for_test(&self) -> usize {
        self.vnode_states.resident_group_count()
    }

    #[cfg(test)]
    pub(crate) fn active_vnodes_for_test(&self) -> &[u32] {
        self.vnode_states.active_vnodes_for_test()
    }

    #[cfg(test)]
    pub(crate) fn working_set_snapshot_for_test(&self) -> AggregateWorkingSetSnapshot {
        let mut snapshot = AggregateWorkingSetSnapshot {
            group_timestamps: std::collections::BTreeMap::new(),
            last_emitted: std::collections::BTreeMap::new(),
            emit_dirty_keys: std::collections::BTreeSet::new(),
            checkpoint_dirty_keys: std::collections::BTreeMap::new(),
            last_emitted_dirty_keys: std::collections::BTreeMap::new(),
        };
        for (vnode, state) in self.vnode_states.iter() {
            snapshot.group_timestamps.extend(
                state
                    .groups
                    .iter()
                    .map(|(key, entry)| (key.as_ref().to_vec(), entry.last_updated_ms)),
            );
            snapshot.last_emitted.extend(
                state
                    .last_emitted
                    .iter()
                    .map(|(key, values)| (key.as_ref().to_vec(), values.clone())),
            );
            snapshot.emit_dirty_keys.extend(
                state
                    .emit_dirty_keys
                    .iter()
                    .map(|key| key.as_ref().to_vec()),
            );
            if !state.checkpoint_dirty_keys.is_empty() {
                snapshot.checkpoint_dirty_keys.insert(
                    vnode,
                    state
                        .checkpoint_dirty_keys
                        .iter()
                        .map(|key| key.as_ref().to_vec())
                        .collect(),
                );
            }
            if !state.last_emitted_dirty_keys.is_empty() {
                snapshot.last_emitted_dirty_keys.insert(
                    vnode,
                    state
                        .last_emitted_dirty_keys
                        .iter()
                        .map(|key| key.as_ref().to_vec())
                        .collect(),
                );
            }
        }
        snapshot
    }

    #[cfg(test)]
    pub(crate) fn evaluated_groups_for_test(
        &mut self,
    ) -> Result<std::collections::BTreeMap<Vec<u8>, Vec<ScalarValue>>, DbError> {
        let mut evaluated = std::collections::BTreeMap::new();
        for (_, state) in self.vnode_states.iter_mut() {
            for (key, entry) in &mut state.groups {
                let values = entry
                    .accs
                    .iter_mut()
                    .map(|accumulator| {
                        accumulator.evaluate().map_err(|error| {
                            DbError::Pipeline(format!(
                                "test aggregate accumulator evaluation: {error}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if evaluated.insert(key.as_ref().to_vec(), values).is_some() {
                    return Err(DbError::Pipeline(
                        "test aggregate snapshot contains a duplicate group key".into(),
                    ));
                }
            }
        }
        Ok(evaluated)
    }
}

/// Z-set weight column name shared between the MV producer and upsert-sink consumers.
pub(crate) use laminar_core::changelog::WEIGHT_COLUMN;

/// Build a weighted `RecordBatch` from collected keys, values, and weights.
fn build_weighted_batch(
    keys: &[arrow::row::OwnedRow],
    vals: &[Vec<ScalarValue>],
    weights: &[i64],
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    agg_specs: &[AggFuncSpec],
    output_schema: &SchemaRef,
) -> Result<RecordBatch, DbError> {
    let group_arrays = if num_group_cols > 0 {
        row_converter
            .convert_rows(keys.iter().map(arrow::row::OwnedRow::row))
            .map_err(|e| DbError::Pipeline(format!("group key array: {e}")))?
    } else {
        Vec::new()
    };

    let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(agg_specs.len());
    for (agg_idx, spec) in agg_specs.iter().enumerate() {
        let scalars: Vec<ScalarValue> = vals.iter().map(|v| v[agg_idx].clone()).collect();
        let array = ScalarValue::iter_to_array(scalars)
            .map_err(|e| DbError::Pipeline(format!("agg array: {e}")))?;
        if array.data_type() == &spec.return_type {
            agg_arrays.push(array);
        } else {
            let casted = arrow::compute::cast(&array, &spec.return_type).unwrap_or(array);
            agg_arrays.push(casted);
        }
    }

    let weight_array: ArrayRef = Arc::new(arrow::array::Int64Array::from_iter_values(
        weights.iter().copied(),
    ));

    let mut all_arrays = group_arrays;
    all_arrays.extend(agg_arrays);
    all_arrays.push(weight_array);

    RecordBatch::try_new(Arc::clone(output_schema), all_arrays)
        .map_err(|e| DbError::Pipeline(format!("weighted batch: {e}")))
}

pub(crate) struct GroupEntry {
    pub(crate) accs: Vec<Box<dyn datafusion_expr::Accumulator>>,
    pub(crate) last_updated_ms: i64,
    accumulator_reported_bytes: usize,
}

impl GroupEntry {
    fn new(accs: Vec<Box<dyn datafusion_expr::Accumulator>>, last_updated_ms: i64) -> Self {
        Self {
            accs,
            last_updated_ms,
            // Generic `Accumulator::size()` is not guaranteed O(1). Construction starts at zero;
            // the first record mutation or lifecycle reconciliation installs the current charge.
            accumulator_reported_bytes: 0,
        }
    }

    fn accounted_accumulator_usage(&self) -> accounting::AggregateStateUsage {
        accounting::logical_collection_element_usage::<Box<dyn datafusion_expr::Accumulator>>(
            self.accs.capacity(),
        )
        .saturating_add(accounting::AggregateStateUsage::from_parts(
            0,
            0,
            0,
            0,
            self.accumulator_reported_bytes,
        ))
    }

    fn refresh_accumulator_usage(&mut self) -> (usize, usize) {
        let previous = self.accumulator_reported_bytes;
        let current = accounting::accumulator_usage(&self.accs).accumulator_reported_bytes();
        self.accumulator_reported_bytes = current;
        (previous, current)
    }
}

/// One owned vnode image yielded only when aggregate preparation reaches that vnode.
pub(crate) struct OwnedAggVnodeRestore {
    pub(crate) vnode: u32,
    pub(crate) state: AggStateCheckpoint,
}

/// Fully decoded aggregate transition containing one replacement decision per transitioned vnode.
pub(crate) struct PreparedAggVnodeTransition {
    replacements: Vec<(u32, Option<Box<AggregateVnodeState>>)>,
    final_active_vnodes: Vec<u32>,
    final_group_count: usize,
}

/// State and staging allocations displaced by one published aggregate transition.
///
/// Destruction can release accumulator- and checkpoint-owned allocations, so publication returns
/// this opaque owner and the graph drops it only after leaving the publication section.
pub(crate) struct RetiredAggVnodeTransition {
    retired_state: PreparedAggVnodeTransition,
}

impl PreparedAggVnodeTransition {
    fn accounted_usage(&self) -> accounting::AggregateStateUsage {
        let usage = accounting::topology_element_usage::<Self>(1)
            .saturating_add(accounting::topology_element_usage::<(
                u32,
                Option<Box<AggregateVnodeState>>,
            )>(self.replacements.capacity()))
            .saturating_add(accounting::topology_element_usage::<u32>(
                self.final_active_vnodes.capacity(),
            ));
        self.replacements
            .iter()
            .filter_map(|(_, state)| state.as_deref())
            .fold(usage, |usage, state| usage.saturating_add(state.usage()))
    }

    /// Retained aggregate bytes owned off-side while a vnode transition is prepared.
    /// Saturated accounting reports `usize::MAX`; observability never faults the data plane.
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        let usage = self.accounted_usage();
        if usage.is_saturated() {
            usize::MAX
        } else {
            usage.total_bytes()
        }
    }
}

impl RetiredAggVnodeTransition {
    /// Retained aggregate bytes displaced by publication and awaiting post-fence cleanup.
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        self.retired_state.accounted_state_bytes()
    }
}

fn checked_vnode_transition_capacity(left: usize, right: usize) -> Result<usize, DbError> {
    left.checked_add(right)
        .ok_or_else(|| DbError::Pipeline("aggregate vnode transition capacity overflow".into()))
}

fn checked_vnode_transition_final_count(
    current: usize,
    removed: usize,
    replacement: usize,
) -> Result<usize, DbError> {
    let retained = current
        .checked_sub(removed)
        .ok_or_else(|| {
            DbError::Pipeline(format!(
                "aggregate vnode transition group-count invariant failed: current={current}, removed={removed}"
            ))
        })?;
    checked_vnode_transition_capacity(retained, replacement)
}

impl IncrementalAggState {
    /// Attempt to build an `IncrementalAggState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query does not
    /// contain an `Aggregate` node (not an aggregation query). `key_group_count`
    /// becomes the state's immutable routing identity; a global aggregate still
    /// retains the complete topology while mapping its only key to vnode zero.
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        emit_changelog: bool,
        key_group_count: KeyGroupCount,
    ) -> Result<Option<Self>, DbError> {
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| DbError::Pipeline(format!("plan error: {e}")))?;

        let plan = df.logical_plan();

        // Top-level schema preserves user aliases (e.g. `SUM(x) AS total`).
        let top_schema = Arc::new(plan.schema().as_arrow().clone());

        let Some(agg_info) = find_aggregate(plan) else {
            return Ok(None);
        };

        let group_exprs = agg_info.group_exprs;
        let aggr_exprs = agg_info.aggr_exprs;
        let agg_schema = agg_info.schema;
        let agg_df_schema = agg_info.df_schema;
        let input_schema = agg_info.input_schema;
        let having_predicate = agg_info.having_predicate;

        if aggr_exprs.is_empty() {
            return Ok(None);
        }

        // Bail if there's a non-trivial projection above the Aggregate
        // (e.g. SUM(a)/SUM(b) AS ratio). Check field count AND types: a
        // coincidental count match can still hide a remapping projection.
        if top_schema.fields().len() != agg_schema.fields().len() {
            return Ok(None);
        }
        for (top_f, agg_f) in top_schema.fields().iter().zip(agg_schema.fields()) {
            if top_f.data_type() != agg_f.data_type() {
                return Ok(None);
            }
        }

        let num_group_cols = group_exprs.len();

        let mut group_col_names = Vec::new();
        let mut group_types = Vec::new();
        for i in 0..num_group_cols {
            let top_field = top_schema.field(i);
            let agg_field = agg_schema.field(i);
            group_col_names.push(top_field.name().clone());
            group_types.push(agg_field.data_type().clone());
        }

        // single_source_table rejects self-joins where compilation is unsafe.
        let compile_source = crate::sql_analysis::single_source_table(sql);
        let state = ctx.state();
        let props = state.execution_props();
        let input_df_schema = &agg_info.input_df_schema;
        let compile =
            |e: &datafusion_expr::Expr| create_physical_expr(e, input_df_schema, props).ok();
        let mut builder =
            PreAggBuilder::new(&input_schema, num_group_cols, compile_source.is_some());

        for (i, group_expr) in group_exprs.iter().enumerate() {
            builder.push_group_expr(i, group_expr, &compile);
        }

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            // Top-level schema carries the user alias (e.g. `SUM(x) AS total`).
            let output_name = if agg_schema_idx < top_schema.fields().len() {
                top_schema.field(agg_schema_idx).name().clone()
            } else {
                agg_field.name().clone()
            };
            if !builder.push_aggregate(expr, output_name, agg_field, &compile) {
                return Ok(None);
            }
        }

        let mut compile_ok = builder.compile_ok;
        let next_col_idx = builder.next_col_idx;
        let mut pre_agg_select_items = builder.pre_agg_select_items;
        let agg_specs = builder.agg_specs;
        let compiled_exprs = builder.compiled_exprs;
        let proj_fields = builder.proj_fields;

        let clauses = extract_clauses(sql);

        // Check the registered schema, not the pruned plan schema, to detect __weight.
        let source_has_weight = if let Ok(tp) = ctx
            .table_provider(exact_table_reference(clauses.from_clause.trim_matches('"')))
            .await
        {
            tp.schema().column_with_name(WEIGHT_COLUMN).is_some()
        } else {
            false
        };

        let weight_col_idx = if source_has_weight {
            for spec in &agg_specs {
                if spec.distinct {
                    return Err(DbError::Pipeline(format!(
                        "DISTINCT aggregates are not supported over changelog streams \
                         ({}(DISTINCT ...) requires per-value tracking not yet implemented).",
                        spec.udf.name()
                    )));
                }
                let name = spec.udf.name().to_lowercase();
                if !matches!(name.as_str(), "sum" | "count" | "avg" | "min" | "max") {
                    return Err(DbError::Pipeline(format!(
                        "Cannot compute {}() over a changelog stream. \
                         Supported: SUM, COUNT, AVG, MIN, MAX.",
                        spec.udf.name()
                    )));
                }
            }
            let idx = next_col_idx;
            pre_agg_select_items.push(format!("\"{WEIGHT_COLUMN}\""));
            Some(idx)
        } else {
            None
        };

        let pre_agg_sql = format!(
            "SELECT {} FROM {}{}",
            pre_agg_select_items.join(", "),
            clauses.from_clause,
            clauses.where_clause,
        );

        // The compiled pre-agg evaluates on the operator's routed input batch. For a changelog
        // source it must carry the `__weight` column through (as the last column, at
        // `weight_col_idx`); otherwise a chained aggregate would have to re-scan the source's
        // per-cycle live provider, which desyncs from the routed changelog and mis-nets
        // retractions. The unoptimized plan's input schema keeps every source column, so the
        // compiled exprs and the appended `__weight` index the routed batch correctly.
        let compiled_projection = if !compile_ok {
            None
        } else if weight_col_idx.is_none() {
            let filter = if let Some(where_pred) = &agg_info.where_predicate {
                if let Ok(phys) = create_physical_expr(where_pred, input_df_schema, props) {
                    Some(phys)
                } else {
                    compile_ok = false;
                    None
                }
            } else {
                None
            };
            if compile_ok {
                Some(CompiledProjection {
                    exprs: compiled_exprs,
                    filter,
                    output_schema: Arc::new(Schema::new(proj_fields)),
                })
            } else {
                None
            }
        } else if agg_info.where_predicate.is_none() {
            // Changelog source, no WHERE: append the `__weight` passthrough. A WHERE over a
            // changelog needs retraction-aware filtering — fall back to the cached plan.
            match create_physical_expr(&datafusion_expr::col(WEIGHT_COLUMN), input_df_schema, props)
            {
                Ok(weight_expr) => {
                    let mut exprs = compiled_exprs;
                    let mut fields = proj_fields;
                    exprs.push(weight_expr);
                    fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
                    Some(CompiledProjection {
                        exprs,
                        filter: None,
                        output_schema: Arc::new(Schema::new(fields)),
                    })
                }
                Err(_) => None,
            }
        } else {
            None
        };

        let mut output_fields: Vec<Field> = Vec::new();
        for (name, dt) in group_col_names.iter().zip(group_types.iter()) {
            output_fields.push(Field::new(name, dt.clone(), true));
        }
        for spec in &agg_specs {
            output_fields.push(Field::new(
                &spec.output_name,
                spec.return_type.clone(),
                true,
            ));
        }
        if emit_changelog {
            output_fields.push(Field::new(WEIGHT_COLUMN, DataType::Int64, false));
        }
        let output_schema = Arc::new(Schema::new(output_fields));

        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &agg_df_schema)?;

        // Plan once at init; LiveSourceProvider leaves carry fresh data per execute.
        let cached_pre_agg_physical =
            if compiled_projection.is_none() {
                let logical = ctx.sql(&pre_agg_sql).await.map_err(|e| {
                    DbError::Pipeline(format!("pre-agg SQL planning failed for aggregate: {e}"))
                })?;
                let plan = logical.logical_plan().clone();
                Some(ctx.state().create_physical_plan(&plan).await.map_err(|e| {
                    DbError::Pipeline(format!("pre-agg physical planning failed: {e}"))
                })?)
            } else {
                None
            };

        let sort_fields: Vec<arrow::row::SortField> = group_types
            .iter()
            .map(|dt| arrow::row::SortField::new(dt.clone()))
            .collect();
        let row_converter = Arc::new(
            arrow::row::RowConverter::new(sort_fields)
                .map_err(|e| DbError::Pipeline(format!("row converter init: {e}")))?,
        );
        let vnode_states = AggregateVnodeSlots::try_new(key_group_count)?;
        let checkpointed_vnodes =
            vec![false; usize::from(key_group_count.get())].into_boxed_slice();

        Ok(Some(Self {
            query_sql: sql.to_string(),
            #[cfg(test)]
            pre_agg_sql,
            num_group_cols,
            key_group_count,
            group_types,
            agg_specs,
            vnode_states,
            row_converter,
            output_schema,
            compiled_projection,
            cached_pre_agg_physical,
            having_filter,
            max_groups: 1_000_000,
            max_retractable_extremum_checkpoint_bytes:
                crate::config::DEFAULT_MAX_RETRACTABLE_EXTREMUM_CHECKPOINT_BYTES,
            emit_changelog,
            weight_col_idx,
            checkpointed_vnodes,
        }))
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn key_group_count(&self) -> KeyGroupCount {
        self.key_group_count
    }

    fn routing_vnode_count(&self) -> NonZeroU32 {
        NonZeroU32::from(self.key_group_count.into_non_zero())
    }

    /// Map an already Arrow-row-encoded aggregate key through partitioning ABI v1.
    ///
    /// The aggregate hot path already owns these bytes, so this deliberately uses the
    /// codec's static mapping rather than constructing another codec or re-encoding the key.
    /// Global aggregate state is pinned to vnode zero.
    fn vnode_for_group_key(
        num_group_cols: usize,
        key: &arrow::row::OwnedRow,
        vnode_count: NonZeroU32,
    ) -> u32 {
        if num_group_cols == 0 || vnode_count.get() == 1 {
            0
        } else {
            PartitionKeyCodecV1::vnode_for_encoded(key.as_ref(), vnode_count)
        }
    }

    /// Require lifecycle callers to use the immutable routing topology selected at construction.
    /// A future key-group-count change requires a new state identity and explicit repartition;
    /// capture or restore must never reinterpret live keys in place.
    pub(crate) fn validate_vnode_count(&self, requested: u32) -> Result<NonZeroU32, DbError> {
        let requested = KeyGroupCount::try_from(requested).map_err(|error| {
            DbError::Pipeline(format!("aggregate vnode_count is invalid: {error}"))
        })?;
        if requested != self.key_group_count {
            return Err(DbError::Pipeline(format!(
                "aggregate key-group count mismatch: state={}, requested={requested}",
                self.key_group_count
            )));
        }
        Ok(self.routing_vnode_count())
    }

    /// Commit a successfully built changelog insertion to the dedup map.
    fn commit_last_emitted(
        &mut self,
        vnode: u32,
        key: arrow::row::OwnedRow,
        values: Vec<ScalarValue>,
    ) {
        let state = self
            .vnode_states
            .get_mut(vnode)
            .expect("emitted aggregate group must remain in its vnode slot");
        state.insert_last_emitted_dirty_key(key.clone());
        state.insert_last_emitted(key, values);
    }

    #[cfg(any(not(feature = "cluster"), test))]
    pub fn process_batch(&mut self, batch: &RecordBatch, watermark_ms: i64) -> Result<(), DbError> {
        self.process_batch_for_vnode(batch, watermark_ms, None)
    }

    /// Apply one pre-aggregate batch. `uniform_vnode` is present only when trusted internal
    /// shuffle metadata declares one vnode for the complete batch; owner-coalesced mixed batches
    /// derive the vnode once per unique encoded group below. The sender's route construction, not
    /// a receiver-side rehash, binds row keys to that metadata.
    pub(crate) fn process_batch_for_vnode(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
        uniform_vnode: Option<u32>,
    ) -> Result<(), DbError> {
        let vnode_count = self.routing_vnode_count();
        if let Some(vnode) = uniform_vnode {
            if vnode >= vnode_count.get() {
                return Err(DbError::Pipeline(format!(
                    "aggregate routed vnode {vnode} is outside key-group count {}",
                    vnode_count.get()
                )));
            }
            if self.num_group_cols == 0 && vnode != 0 {
                return Err(DbError::Pipeline(format!(
                    "global aggregate received routed vnode {vnode}; expected vnode 0"
                )));
            }
        }

        if batch.num_rows() == 0 {
            return Ok(());
        }

        if self.num_group_cols == 0 {
            return self.process_batch_no_groups(batch, watermark_ms);
        }

        let group_cols: Vec<ArrayRef> = (0..self.num_group_cols)
            .map(|i| Arc::clone(batch.column(i)))
            .collect();

        let rows = self
            .row_converter
            .convert_columns(&group_cols)
            .map_err(|e| DbError::Pipeline(format!("row conversion: {e}")))?;

        // One OwnedRow alloc per unique group, not per row.
        let estimated_groups = (batch.num_rows() / 4).max(16);
        let mut group_indices: FxHashMap<arrow::row::Row<'_>, Vec<u32>> =
            FxHashMap::with_capacity_and_hasher(estimated_groups, rustc_hash::FxBuildHasher);
        for row_idx in 0..batch.num_rows() {
            #[allow(clippy::cast_possible_truncation)]
            group_indices
                .entry(rows.row(row_idx))
                .or_default()
                .push(row_idx as u32);
        }

        // Allocate each unique key once, then reject the whole batch before touching an
        // accumulator if its new keys would exceed the cardinality bound. Partial application is
        // not retry-safe: an existing group updated before the error would be counted twice.
        let mut grouped_rows = Vec::with_capacity(group_indices.len());
        let mut incoming_new_groups = 0usize;
        for (row_ref, indices) in group_indices {
            let key = row_ref.owned();
            let vnode = uniform_vnode.unwrap_or_else(|| {
                Self::vnode_for_group_key(self.num_group_cols, &key, vnode_count)
            });
            if !self
                .vnode_states
                .get(vnode)
                .is_some_and(|state| state.groups.contains_key(&key))
            {
                incoming_new_groups = incoming_new_groups.saturating_add(1);
            }
            grouped_rows.push((vnode, key, indices));
        }
        let current_groups = self.vnode_states.resident_group_count();
        let required_groups = current_groups.saturating_add(incoming_new_groups);
        if required_groups > self.max_groups {
            return Err(DbError::Pipeline(format!(
                "aggregate group limit exceeded: current={}, incoming_new={}, limit={}",
                current_groups, incoming_new_groups, self.max_groups
            )));
        }

        let agg_specs = &self.agg_specs;
        let weight_col_idx = self.weight_col_idx;
        let emit_changelog = self.emit_changelog;
        for (vnode, owned_key, indices) in grouped_rows {
            let key;
            let (inserted, update_result) = {
                let vnode_state = self.vnode_states.get_or_insert(vnode);
                let previous_spare = vnode_state.collection_spare_usage();
                let (entry, inserted) = match vnode_state.groups.entry(owned_key) {
                    std::collections::hash_map::Entry::Occupied(e) => {
                        key = e.key().clone();
                        (e.into_mut(), false)
                    }
                    std::collections::hash_map::Entry::Vacant(e) => {
                        key = e.key().clone();
                        let mut accs = Vec::with_capacity(agg_specs.len());
                        for spec in agg_specs {
                            let acc = if weight_col_idx.is_some() {
                                spec.create_retractable_accumulator()?
                            } else {
                                spec.create_accumulator()?
                            };
                            accs.push(acc);
                        }
                        (e.insert(GroupEntry::new(accs, watermark_ms)), true)
                    }
                };
                let update_result = Self::update_group_accumulators(
                    &mut entry.accs,
                    batch,
                    &indices,
                    agg_specs,
                    weight_col_idx,
                );
                entry.last_updated_ms = watermark_ms;
                let accumulator_reconciliation = if inserted {
                    entry.refresh_accumulator_usage();
                    None
                } else {
                    Some(entry.refresh_accumulator_usage())
                };
                let inserted_usage =
                    inserted.then(|| AggregateVnodeState::group_usage(&key, entry));
                if let Some(usage) = inserted_usage {
                    vnode_state.add_usage(usage);
                }
                if let Some((previous, current)) = accumulator_reconciliation {
                    vnode_state.reconcile_accumulator_usage(
                        accounting::AggregateStateUsage::from_parts(0, 0, 0, 0, previous),
                        accounting::AggregateStateUsage::from_parts(0, 0, 0, 0, current),
                    );
                }
                vnode_state.reconcile_collection_spare_usage(previous_spare);
                if update_result.is_ok() {
                    if emit_changelog {
                        vnode_state.insert_emit_dirty_key(key.clone());
                    }
                    vnode_state.insert_checkpoint_dirty_key(key.clone());
                }
                (inserted, update_result)
            };
            if inserted {
                self.vnode_states.increment_resident_groups();
            }
            update_result?;
        }
        Ok(())
    }

    /// Fast path for global aggregates (no GROUP BY).
    fn process_batch_no_groups(
        &mut self,
        batch: &RecordBatch,
        watermark_ms: i64,
    ) -> Result<(), DbError> {
        let empty_key = global_aggregate_key();
        let inserted = !self
            .vnode_states
            .get(0)
            .is_some_and(|state| state.groups.contains_key(&empty_key));
        if inserted {
            let mut accs = Vec::with_capacity(self.agg_specs.len());
            for spec in &self.agg_specs {
                let acc = if self.weight_col_idx.is_some() {
                    spec.create_retractable_accumulator()?
                } else {
                    spec.create_accumulator()?
                };
                accs.push(acc);
            }
            let entry = GroupEntry::new(accs, watermark_ms);
            let usage = AggregateVnodeState::group_usage(&empty_key, &entry);
            let vnode_state = self.vnode_states.get_or_insert(0);
            let previous_spare = vnode_state.collection_spare_usage();
            vnode_state.groups.insert(empty_key.clone(), entry);
            vnode_state.add_usage(usage);
            vnode_state.reconcile_collection_spare_usage(previous_spare);
            self.vnode_states.increment_resident_groups();
        }
        #[allow(clippy::cast_possible_truncation)]
        let all_indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
        let vnode_state = self.vnode_states.get_mut(0).unwrap();
        let (res, previous_accumulator_usage, current_accumulator_usage) = {
            let entry = vnode_state.groups.get_mut(&empty_key).unwrap();
            entry.last_updated_ms = watermark_ms;
            let res = Self::update_group_accumulators(
                &mut entry.accs,
                batch,
                &all_indices,
                &self.agg_specs,
                self.weight_col_idx,
            );
            let (previous, current) = entry.refresh_accumulator_usage();
            (res, previous, current)
        };
        vnode_state.reconcile_accumulator_usage(
            accounting::AggregateStateUsage::from_parts(0, 0, 0, 0, previous_accumulator_usage),
            accounting::AggregateStateUsage::from_parts(0, 0, 0, 0, current_accumulator_usage),
        );
        vnode_state.insert_checkpoint_dirty_key(empty_key.clone());
        if self.emit_changelog {
            vnode_state.insert_emit_dirty_key(empty_key);
        }
        res
    }

    /// Update accumulators for a group: one `take()` per column per accumulator, no per-row allocation.
    pub(crate) fn update_group_accumulators(
        accs: &mut [Box<dyn datafusion_expr::Accumulator>],
        batch: &RecordBatch,
        indices: &[u32],
        agg_specs: &[AggFuncSpec],
        weight_col_idx: Option<usize>,
    ) -> Result<(), DbError> {
        let index_array = arrow::array::UInt32Array::from(indices.to_vec());

        let weight_arr = if let Some(w_idx) = weight_col_idx {
            Some(
                compute::take(batch.column(w_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("weight take: {e}")))?,
            )
        } else {
            None
        };

        for (i, spec) in agg_specs.iter().enumerate() {
            let mut input_arrays: Vec<ArrayRef> = Vec::with_capacity(spec.input_col_indices.len());
            for &col_idx in &spec.input_col_indices {
                let arr = compute::take(batch.column(col_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("array take failed: {e}")))?;
                input_arrays.push(arr);
            }

            let filtered_weight = if let Some(filter_idx) = spec.filter_col_index {
                let filter_arr = compute::take(batch.column(filter_idx), &index_array, None)
                    .map_err(|e| DbError::Pipeline(format!("filter take: {e}")))?;
                if let Some(mask) = filter_arr
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                {
                    let mut filtered = Vec::with_capacity(input_arrays.len());
                    for arr in &input_arrays {
                        filtered.push(
                            compute::filter(arr, mask)
                                .map_err(|e| DbError::Pipeline(format!("filter apply: {e}")))?,
                        );
                    }
                    input_arrays = filtered;
                    weight_arr
                        .as_ref()
                        .map(|w| {
                            compute::filter(w, mask)
                                .map_err(|e| DbError::Pipeline(format!("weight filter: {e}")))
                        })
                        .transpose()?
                } else {
                    weight_arr.clone()
                }
            } else {
                weight_arr.clone()
            };

            if let Some(w) = &filtered_weight {
                input_arrays.push(Arc::clone(w));
            }

            accs[i]
                .update_batch(&input_arrays)
                .map_err(|e| DbError::Pipeline(format!("accumulator update: {e}")))?;
        }
        Ok(())
    }

    /// Emit current aggregate state; accumulators keep running (no reset).
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.emit_changelog {
            return self.emit_changelog_delta();
        }
        self.emit_running_state()
    }

    fn emit_running_state(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.vnode_states.resident_group_count() == 0 {
            return Ok(Vec::new());
        }

        let num_rows = self.vnode_states.resident_group_count();

        let group_arrays = if self.num_group_cols > 0 {
            self.row_converter
                .convert_rows(
                    self.vnode_states
                        .iter()
                        .flat_map(|(_, state)| state.groups.keys())
                        .map(arrow::row::OwnedRow::row),
                )
                .map_err(|e| DbError::Pipeline(format!("group key array build: {e}")))?
        } else {
            Vec::new()
        };

        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars = Vec::with_capacity(num_rows);
            for (_, state) in self.vnode_states.iter_mut() {
                for entry in state.groups.values_mut() {
                    let scalar = entry.accs[agg_idx].evaluate().map_err(|error| {
                        DbError::Pipeline(format!("accumulator evaluate: {error}"))
                    })?;
                    scalars.push(scalar);
                }
            }
            let array = ScalarValue::iter_to_array(scalars)
                .map_err(|e| DbError::Pipeline(format!("agg result array build: {e}")))?;
            if array.data_type() == &spec.return_type {
                agg_arrays.push(array);
            } else {
                let casted = arrow::compute::cast(&array, &spec.return_type).unwrap_or(array);
                agg_arrays.push(casted);
            }
        }

        let mut all_arrays = group_arrays;
        all_arrays.extend(agg_arrays);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), all_arrays)
            .map_err(|e| DbError::Pipeline(format!("result batch build: {e}")))?;

        Ok(vec![batch])
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

    fn emit_changelog_delta(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let mut dirty_by_vnode = Vec::new();
        for (vnode, state) in self.vnode_states.iter_mut() {
            if !state.emit_dirty_keys.is_empty() {
                dirty_by_vnode.push((vnode, std::mem::take(&mut state.emit_dirty_keys)));
            }
        }

        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();
        let mut insert_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut insert_vals: Vec<Vec<ScalarValue>> = Vec::new();
        let mut insert_vnodes = Vec::new();

        let mut eval_err: Option<DbError> = None;
        'vnodes: for (vnode, dirty) in &dirty_by_vnode {
            let state = self
                .vnode_states
                .get_mut(*vnode)
                .expect("dirty aggregate vnode must remain resident during emission");
            for key in dirty {
                let Some(entry) = state.groups.get_mut(key) else {
                    continue;
                };
                let evaluated = entry
                    .accs
                    .iter_mut()
                    .map(|a| a.evaluate())
                    .collect::<Result<Vec<_>, _>>();
                let current: Vec<ScalarValue> = match evaluated {
                    Ok(current) => current,
                    Err(e) => {
                        eval_err = Some(DbError::Pipeline(format!("accumulator evaluate: {e}")));
                        break 'vnodes;
                    }
                };

                if let Some(old) = state.last_emitted.get(key) {
                    // ScalarValue::eq treats NaN != NaN; short-circuit to avoid an infinite
                    // retract+insert loop on float aggregates.
                    let changed = old.iter().zip(current.iter()).any(|(a, b)| match (a, b) {
                        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y)))
                            if x.is_nan() && y.is_nan() =>
                        {
                            false
                        }
                        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y)))
                            if x.is_nan() && y.is_nan() =>
                        {
                            false
                        }
                        _ => a != b,
                    });
                    if changed {
                        retract_keys.push(key.clone());
                        retract_vals.push(old.clone());
                        insert_keys.push(key.clone());
                        insert_vals.push(current);
                        insert_vnodes.push(*vnode);
                    }
                } else {
                    insert_keys.push(key.clone());
                    insert_vals.push(current);
                    insert_vnodes.push(*vnode);
                }
            }
        }
        if let Some(e) = eval_err {
            self.finish_emit_dirty_sets(dirty_by_vnode, false);
            return Err(e);
        }

        let retract_count = retract_keys.len();
        let total = retract_count + insert_keys.len();
        if total == 0 {
            self.finish_emit_dirty_sets(dirty_by_vnode, true);
            return Ok(Vec::new());
        }

        let mut all_keys = Vec::with_capacity(total);
        let mut all_vals = Vec::with_capacity(total);
        let mut weights = Vec::with_capacity(total);

        for (k, v) in retract_keys.into_iter().zip(retract_vals) {
            all_keys.push(k);
            all_vals.push(v);
            weights.push(-1i64);
        }
        for (k, v) in insert_keys.into_iter().zip(insert_vals) {
            all_keys.push(k);
            all_vals.push(v);
            weights.push(1i64);
        }

        let batch = match build_weighted_batch(
            &all_keys,
            &all_vals,
            &weights,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        ) {
            Ok(batch) => batch,
            Err(e) => {
                self.finish_emit_dirty_sets(dirty_by_vnode, false);
                return Err(e);
            }
        };

        // Commit only after the complete combined output batch exists. The insert half of
        // `all_keys`/`all_vals` is aligned with `insert_vnodes`.
        for ((key, current), vnode) in all_keys
            .into_iter()
            .zip(all_vals)
            .skip(retract_count)
            .zip(insert_vnodes)
        {
            self.commit_last_emitted(vnode, key, current);
        }
        self.finish_emit_dirty_sets(dirty_by_vnode, true);

        debug_assert!(
            self.vnode_states.iter().all(|(_, state)| state
                .last_emitted
                .keys()
                .all(|key| state.groups.contains_key(key))),
            "last_emitted must be a subset of groups"
        );

        Ok(vec![batch])
    }

    pub fn having_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.having_filter.as_ref()
    }

    pub fn compiled_projection(&self) -> Option<&CompiledProjection> {
        self.compiled_projection.as_ref()
    }

    pub fn cached_pre_agg_physical(
        &self,
    ) -> Option<&Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
        self.cached_pre_agg_physical.as_ref()
    }

    pub(crate) fn query_fingerprint(&self) -> u64 {
        query_fingerprint(&self.query_sql, &self.output_schema)
    }

    /// Deterministic lower-bound charge for the live aggregate working set.
    ///
    /// This walks only the bounded active-vnode roster over cached per-vnode counters; it does not
    /// scan groups. Touched groups reconcile dynamic accumulator bytes during record processing.
    /// Hash collection capacity and nested retained changelog scalars are charged, while allocator
    /// overhead, transient scratch, and RSS remain outside this deterministic envelope. Overflow is
    /// clamped because metrics publication must not fault processing.
    pub(crate) fn accounted_state_bytes(&self) -> usize {
        let usage = self.vnode_states.accounted_usage();
        if usage.is_saturated() {
            usize::MAX
        } else {
            usage.total_bytes()
        }
    }

    #[cfg(test)]
    fn cached_usage_matches_structural_recompute(&self) -> bool {
        self.vnode_states
            .cached_usage_matches_structural_recompute()
    }

    /// Decode the per-entry changelog `last_emitted` checkpoint into the live map.
    fn decode_last_emitted(
        &self,
        entries: &[EmittedCheckpoint],
    ) -> Result<FxHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>, DbError> {
        let mut out = FxHashMap::with_capacity_and_hasher(entries.len(), FxBuildHasher);
        for ec in entries {
            let sv_key = ipc_to_scalars(&ec.key)?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;
            let vals = ipc_to_scalars(&ec.values)?;
            if vals.len() != self.agg_specs.len() {
                return Err(DbError::Pipeline(format!(
                    "aggregate checkpoint changelog arity mismatch: saved={}, expected={}",
                    vals.len(),
                    self.agg_specs.len()
                )));
            }
            for (index, (value, spec)) in vals.iter().zip(&self.agg_specs).enumerate() {
                let saved_type = value.data_type();
                if saved_type != spec.return_type {
                    return Err(DbError::Pipeline(format!(
                        "aggregate checkpoint changelog type mismatch at {index}: saved={saved_type}, expected={}",
                        spec.return_type
                    )));
                }
            }
            if out.insert(row_key, vals).is_some() {
                return Err(DbError::Pipeline(
                    "aggregate checkpoint contains a duplicate changelog key".into(),
                ));
            }
        }
        Ok(out)
    }

    fn capture_full_vnode(
        &mut self,
        vnode: u32,
        fingerprint: u64,
        retractable: bool,
        group_types: Arc<[DataType]>,
        row_converter: Arc<arrow::row::RowConverter>,
    ) -> Result<AggregateVnodeCheckpointCapture, DbError> {
        let mut groups = Vec::new();
        let mut last_emitted = Vec::new();
        if let Some(state) = self.vnode_states.get_mut(vnode) {
            groups
                .try_reserve_exact(state.groups.len())
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "aggregate vnode {vnode} checkpoint group roster: {error}"
                    ))
                })?;
            for (key, entry) in &mut state.groups {
                let mut accumulator_states = Vec::with_capacity(entry.accs.len());
                for (index, accumulator) in entry.accs.iter_mut().enumerate() {
                    accumulator_states.push(snapshot_state_scalars(
                        accumulator,
                        &self.agg_specs[index],
                        retractable,
                    )?);
                }
                groups.push(CapturedAggregateGroup {
                    key: key.clone(),
                    accumulator_states,
                    last_updated_ms: entry.last_updated_ms,
                });
            }
            if self.emit_changelog {
                last_emitted
                    .try_reserve_exact(state.last_emitted.len())
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "aggregate vnode {vnode} checkpoint changelog roster: {error}"
                        ))
                    })?;
                last_emitted.extend(
                    state
                        .last_emitted
                        .iter()
                        .map(|(key, values)| (key.clone(), values.clone())),
                );
            }
            state.refresh_usage();
        }
        let mut capture = AggregateVnodeCheckpointCapture {
            fingerprint,
            group_types,
            row_converter,
            groups,
            last_emitted,
            retained_bytes: 0,
        };
        capture.retained_bytes = capture.calculate_retained_bytes();
        Ok(capture)
    }

    /// Capture full images for dirty vnodes in the exact requested ownership order.
    /// `None` means the prior committed frame remains authoritative.
    pub(crate) fn capture_checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Vec<Option<AggregateVnodeCheckpointCapture>>, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        if required_vnodes.windows(2).any(|pair| pair[0] >= pair[1])
            || required_vnodes
                .iter()
                .any(|vnode| *vnode >= vnode_count.get())
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate received a non-canonical vnode roster {required_vnodes:?} for vnode_count {}",
                vnode_count.get()
            )));
        }
        if let Some(unowned) = self
            .vnode_states
            .active_vnodes()
            .iter()
            .find(|vnode| required_vnodes.binary_search(vnode).is_err())
        {
            return Err(DbError::Checkpoint(format!(
                "aggregate retained state for unowned vnode {unowned}"
            )));
        }

        let retractable = self.weight_col_idx.is_some();
        let fingerprint = self.query_fingerprint();
        let capture_plan = required_vnodes
            .iter()
            .map(|&vnode| {
                !self.checkpointed_vnodes[vnode as usize]
                    || self.vnode_states.get(vnode).is_some_and(|state| {
                        !state.checkpoint_dirty_keys.is_empty()
                            || !state.last_emitted_dirty_keys.is_empty()
                    })
            })
            .collect::<Vec<_>>();
        let selected_entries = capture_plan
            .iter()
            .zip(required_vnodes)
            .filter(|(capture, _)| **capture)
            .filter_map(|(_, vnode)| self.vnode_states.get(*vnode))
            .flat_map(|state| state.groups.values());
        ensure_retractable_extremum_checkpoint_budget(
            &self.agg_specs,
            retractable,
            selected_entries,
            self.max_retractable_extremum_checkpoint_bytes,
            "per-vnode aggregate checkpoint capture",
        )?;

        let mut out = Vec::with_capacity(required_vnodes.len());
        let mut remaining_capture_bytes = max_capture_bytes;
        let group_types = Arc::<[DataType]>::from(self.group_types.clone());
        let row_converter = Arc::clone(&self.row_converter);
        for (&vnode, capture) in required_vnodes.iter().zip(&capture_plan) {
            let captured = if *capture {
                if let Some(state) = self.vnode_states.get(vnode) {
                    let usage = state.usage();
                    let estimated_bytes = if usage.is_saturated() {
                        u64::MAX
                    } else {
                        u64::try_from(usage.total_bytes()).unwrap_or(u64::MAX)
                    };
                    if estimated_bytes > remaining_capture_bytes {
                        return Err(DbError::Checkpoint(format!(
                            "aggregate vnode {vnode} state exceeds the remaining capture budget"
                        )));
                    }
                }
                let captured = self.capture_full_vnode(
                    vnode,
                    fingerprint,
                    retractable,
                    Arc::clone(&group_types),
                    Arc::clone(&row_converter),
                )?;
                remaining_capture_bytes = remaining_capture_bytes
                    .checked_sub(captured.retained_bytes())
                    .ok_or_else(|| {
                        DbError::Checkpoint(format!(
                            "aggregate vnode {vnode} capture exceeded the remaining capture budget"
                        ))
                    })?;
                Some(captured)
            } else {
                None
            };
            out.push(captured);
        }

        for (vnode, checkpointed) in self.checkpointed_vnodes.iter_mut().enumerate() {
            let vnode = u32::try_from(vnode).map_err(|_| {
                DbError::Checkpoint("aggregate vnode index exceeds the u32 domain".into())
            })?;
            if required_vnodes.binary_search(&vnode).is_err() {
                *checkpointed = false;
            }
        }
        for (&vnode, capture) in required_vnodes.iter().zip(capture_plan) {
            self.checkpointed_vnodes[vnode as usize] = true;
            if capture {
                if let Some(state) = self.vnode_states.get_mut(vnode) {
                    state.clear_checkpoint_dirty_keys();
                    state.clear_last_emitted_dirty_keys();
                }
            }
        }
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
    ) -> Result<Vec<Option<AggStateCheckpoint>>, DbError> {
        let captures = self.capture_checkpoint_vnodes(required_vnodes, vnode_count, u64::MAX)?;
        let encoded = captures
            .into_iter()
            .map(|capture| {
                capture
                    .map(|capture| capture.encode(usize::MAX))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>();
        if encoded.is_err() {
            self.force_full_vnode_capture();
        }
        encoded
    }

    pub(crate) fn force_full_vnode_capture(&mut self) {
        self.checkpointed_vnodes.fill(false);
    }

    fn decode_recovery_mutation(
        &self,
        checkpoint: &AggStateCheckpoint,
        context: &str,
        current_fp: u64,
    ) -> Result<DecodedAggMutation, DbError> {
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "{context} fingerprint mismatch: saved={}, current={current_fp}",
                checkpoint.fingerprint
            )));
        }
        validate_columnar_acc_shape(checkpoint, &self.agg_specs)?;
        let last_emitted = self.decode_last_emitted(&checkpoint.last_emitted)?;
        let groups = decode_columnar_state_arrays(
            &self.row_converter,
            self.num_group_cols,
            &checkpoint.keys_ipc,
            &checkpoint.acc_state_ipc,
            &checkpoint.last_updated_ms,
        )?;
        validate_unique_decoded_group_keys(&groups)?;
        Ok(DecodedAggMutation {
            groups,
            last_emitted,
        })
    }

    fn stage_recovery_base(
        &self,
        staged: &mut StagedAggMutation,
        mutation: DecodedAggMutation,
    ) -> Result<(), DbError> {
        let retractable = self.weight_col_idx.is_some();
        for (row_key, last_updated_ms, state_arrays) in mutation.groups {
            let entry = GroupEntry::new(
                build_accumulators_from_state(&self.agg_specs, retractable, &state_arrays)?,
                last_updated_ms,
            );
            if staged.groups.insert(row_key, entry).is_some() {
                return Err(DbError::Pipeline(
                    "aggregate checkpoint contains a duplicate group key".into(),
                ));
            }
        }
        for (row_key, values) in mutation.last_emitted {
            if !staged.groups.contains_key(&row_key) {
                return Err(DbError::Pipeline(
                    "aggregate checkpoint contains changelog state for a missing group".into(),
                ));
            }
            if staged.last_emitted.insert(row_key, values).is_some() {
                return Err(DbError::Pipeline(
                    "aggregate checkpoint contains a duplicate changelog key".into(),
                ));
            }
        }
        Ok(())
    }

    /// Decode and build complete replacement vnode slots without changing the live working set.
    ///
    /// The caller has already validated the complete borrowed archive and cardinality roster. The
    /// iterator deliberately yields one owned vnode image at a time; its rkyv and Arrow expansion
    /// is consumed into private replacement state before the next vnode is requested. Publication
    /// remains one all-vnode operation after the complete iterator succeeds.
    pub(crate) fn prepare_owned_vnode_transition(
        &self,
        vnode_count: u32,
        restores: impl ExactSizeIterator<Item = Result<OwnedAggVnodeRestore, DbError>>,
        revoked: &rustc_hash::FxHashSet<u32>,
    ) -> Result<PreparedAggVnodeTransition, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        let reserve_error = |component: &str, error: std::collections::TryReserveError| {
            DbError::Pipeline(format!(
                "aggregate vnode transition could not reserve {component}: {error}"
            ))
        };

        let restore_count = restores.len();
        let transitioned_capacity =
            checked_vnode_transition_capacity(restore_count, revoked.len())?;
        let mut transitioned = rustc_hash::FxHashSet::default();
        transitioned
            .try_reserve(transitioned_capacity)
            .map_err(|error| reserve_error("vnode roster", error))?;
        for vnode in revoked {
            if *vnode >= vnode_count.get() {
                return Err(DbError::Pipeline(format!(
                    "revoked vnode {vnode} is outside vnode_count {}",
                    vnode_count.get()
                )));
            }
            transitioned.insert(*vnode);
        }

        let mut restored_vnodes = rustc_hash::FxHashSet::default();
        restored_vnodes
            .try_reserve(restore_count)
            .map_err(|error| reserve_error("restored vnode roster", error))?;
        let mut replacement_by_vnode = FxHashMap::default();
        replacement_by_vnode
            .try_reserve(restore_count)
            .map_err(|error| reserve_error("replacement vnode roster", error))?;
        let mut replacement_group_count = 0_usize;
        let current_fp = self.query_fingerprint();
        for restore in restores {
            let OwnedAggVnodeRestore { vnode, state } = restore?;
            if vnode >= vnode_count.get() {
                return Err(DbError::Pipeline(format!(
                    "restored vnode {} is outside vnode_count {}",
                    vnode,
                    vnode_count.get()
                )));
            }
            if !restored_vnodes.insert(vnode) {
                return Err(DbError::Pipeline(format!(
                    "aggregate vnode transition repeats restored vnode {vnode}"
                )));
            }
            transitioned.insert(vnode);

            let group_capacity = state.last_updated_ms.len().min(self.max_groups);
            let emitted_capacity = state.last_emitted.len().min(self.max_groups);
            let mut staged_groups = FxHashMap::default();
            staged_groups
                .try_reserve(group_capacity)
                .map_err(|error| reserve_error("staged groups", error))?;
            let mut staged_last_emitted = FxHashMap::default();
            staged_last_emitted
                .try_reserve(emitted_capacity)
                .map_err(|error| reserve_error("staged changelog state", error))?;
            let mut staged = StagedAggMutation {
                groups: staged_groups,
                last_emitted: staged_last_emitted,
            };
            let belongs_to_vnode = |key: &arrow::row::OwnedRow| {
                Self::vnode_for_group_key(self.num_group_cols, key, vnode_count) == vnode
            };

            let decoded = self.decode_recovery_mutation(&state, "vnode transition", current_fp)?;
            drop(state);
            if decoded
                .groups
                .iter()
                .any(|(key, _, _)| !belongs_to_vnode(key))
                || decoded
                    .last_emitted
                    .keys()
                    .any(|key| !belongs_to_vnode(key))
            {
                return Err(DbError::Pipeline(format!(
                    "authoritative vnode {vnode} checkpoint contains a key for another vnode"
                )));
            }
            self.stage_recovery_base(&mut staged, decoded)?;
            if staged
                .last_emitted
                .keys()
                .any(|key| !staged.groups.contains_key(key))
            {
                return Err(DbError::Pipeline(
                    "aggregate vnode transition contains changelog state for a missing group"
                        .into(),
                ));
            }

            replacement_group_count =
                checked_vnode_transition_capacity(replacement_group_count, staged.groups.len())?;
            if replacement_group_count > self.max_groups {
                return Err(DbError::Pipeline(format!(
                    "aggregate replacement group limit exceeded during vnode transition: replacement={}, limit={}",
                    replacement_group_count, self.max_groups
                )));
            }
            let replacement = AggregateVnodeState::try_from_recovered(
                staged.groups,
                staged.last_emitted,
                self.emit_changelog,
            )
            .map_err(|(component, error)| reserve_error(component, error))?;
            // Ownership lives in the graph, not in this sparse working set. An authoritative EMPTY
            // image therefore clears any old slot without adding an inert active-roster entry.
            if !replacement.groups.is_empty()
                && replacement_by_vnode
                    .insert(vnode, Box::new(replacement))
                    .is_some()
            {
                return Err(DbError::Pipeline(format!(
                    "aggregate vnode transition repeats restored vnode {vnode}"
                )));
            }
        }

        let transitioned_group_count = transitioned.iter().try_fold(0_usize, |total, vnode| {
            checked_vnode_transition_capacity(
                total,
                self.vnode_states
                    .get(*vnode)
                    .map_or(0, |state| state.groups.len()),
            )
        })?;
        let retained_group_count = checked_vnode_transition_final_count(
            self.vnode_states.resident_group_count(),
            transitioned_group_count,
            0,
        )?;
        let final_group_count =
            checked_vnode_transition_capacity(retained_group_count, replacement_group_count)?;
        if final_group_count > self.max_groups {
            return Err(DbError::Pipeline(format!(
                "aggregate group limit exceeded during vnode transition: retained={retained_group_count}, replacement={}, limit={}",
                replacement_group_count,
                self.max_groups
            )));
        }

        let mut transitioned_vnodes = transitioned.into_iter().collect::<Vec<_>>();
        transitioned_vnodes.sort_unstable();
        let mut replacements = Vec::new();
        replacements
            .try_reserve_exact(transitioned_vnodes.len())
            .map_err(|error| reserve_error("replacement slot roster", error))?;
        for &vnode in &transitioned_vnodes {
            replacements.push((vnode, replacement_by_vnode.remove(&vnode)));
        }
        debug_assert!(replacement_by_vnode.is_empty());
        let mut final_active_vnodes = Vec::new();
        let slot_count = usize::from(self.key_group_count.get());
        // Reserve the immutable topology bound during preparation so publication can swap the
        // complete roster without allocating under the graph's transition fence.
        final_active_vnodes
            .try_reserve_exact(slot_count)
            .map_err(|error| reserve_error("final active vnode roster", error))?;
        final_active_vnodes.extend(
            self.vnode_states
                .active_vnodes()
                .iter()
                .copied()
                .filter(|vnode| transitioned_vnodes.binary_search(vnode).is_err()),
        );
        final_active_vnodes.extend(
            replacements
                .iter()
                .filter_map(|(vnode, replacement)| replacement.is_some().then_some(*vnode)),
        );
        final_active_vnodes.sort_unstable();
        debug_assert!(final_active_vnodes.windows(2).all(|pair| pair[0] < pair[1]));
        Ok(PreparedAggVnodeTransition {
            replacements,
            final_active_vnodes,
            final_group_count,
        })
    }

    /// Publish an already prepared aggregate transition with one allocation-free pointer swap per
    /// transitioned vnode. Unchanged vnode boxes retain pointer identity.
    pub(crate) fn publish_prepared_vnode_transition(
        &mut self,
        mut prepared: PreparedAggVnodeTransition,
    ) -> RetiredAggVnodeTransition {
        for (vnode, replacement) in &mut prepared.replacements {
            let retired = self
                .vnode_states
                .replace_for_publication(*vnode, replacement.take());
            // Reuse the prepared replacement cell to retain the displaced box. The retired
            // transition then owns every old slot until post-fence cleanup without allocating or
            // deallocating during publication.
            *replacement = retired;
            self.checkpointed_vnodes[*vnode as usize] = false;
        }
        self.vnode_states
            .swap_active_vnodes(&mut prepared.final_active_vnodes);
        self.vnode_states
            .set_resident_group_count(prepared.final_group_count);

        RetiredAggVnodeTransition {
            retired_state: prepared,
        }
    }

    /// Release state retired by [`Self::publish_prepared_vnode_transition`] after the graph leaves
    /// its publication fence.
    pub(crate) fn finish_vnode_transition(retired: RetiredAggVnodeTransition) {
        drop(retired);
    }

    pub(crate) fn restore_vnode(
        &mut self,
        vnode: u32,
        vnode_count: u32,
        state: AggStateCheckpoint,
    ) -> Result<(), DbError> {
        let prepared = self.prepare_owned_vnode_transition(
            vnode_count,
            std::iter::once(Ok(OwnedAggVnodeRestore { vnode, state })),
            &FxHashSet::default(),
        )?;
        let retired = self.publish_prepared_vnode_transition(prepared);
        Self::finish_vnode_transition(retired);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod vnode_partition_tests;
