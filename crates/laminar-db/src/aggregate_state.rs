//! Incremental aggregation state for streaming GROUP BY queries.
//!
//! One `IncrementalAggState` per pipeline; one `DataFusion` `Accumulator` per
//! aggregate per group. Cross-vnode partial merges live in
//! `laminar_core::state::partial_aggregate` and are a separate concern.

use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use rustc_hash::FxHashMap;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::AggregateUDF;

use crate::db::exact_table_reference;
use crate::error::DbError;

mod checkpoints;
mod compile;
mod keys;
mod scalar_ipc;
pub(crate) use checkpoints::{
    query_fingerprint, query_fingerprint_with_config, AggStateCheckpoint, EmittedCheckpoint,
    EowcStateCheckpoint, GroupCheckpoint, JoinStateCheckpoint, WindowCheckpoint,
};
pub(crate) use compile::{
    apply_compiled_having, compile_having_filter, expr_to_sql, extract_clauses, find_aggregate,
    CompiledProjection, PreAggBuilder,
};
pub(crate) use keys::{
    global_aggregate_key, row_to_scalar_key_with_types, scalar_key_to_owned_row,
};
pub(crate) use scalar_ipc::{ipc_to_scalars, scalars_to_ipc};

/// Builds the per-window result batch for one closed window.
/// Output schema: `[group_cols..., agg_outputs...]`.
pub(crate) fn emit_window_batch(
    groups: ahash::AHashMap<arrow::row::OwnedRow, Vec<Box<dyn datafusion_expr::Accumulator>>>,
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

/// Build an IPC stream from arrays as the columns of a single batch; empty input
/// (no columns) → empty `Vec`.
fn arrays_to_ipc(arrays: &[ArrayRef]) -> Result<Vec<u8>, DbError> {
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
    laminar_core::serialization::serialize_batch_stream(&batch)
        .map_err(|e| DbError::Pipeline(format!("columnar IPC encode: {e}")))
}

/// Columnar encode result: `(keys IPC, per-accumulator state IPC, last_updated_ms)`.
type ColumnarEncoding = (Vec<u8>, Vec<Vec<u8>>, Vec<i64>);

/// Encode groups columnar: keys in one IPC batch, each accumulator's state across
/// all groups in one IPC batch. Row `j` of every batch refers to `entries[j]`.
fn encode_groups_columnar(
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    agg_specs: &[AggFuncSpec],
    retractable: bool,
    entries: &mut [(arrow::row::OwnedRow, &mut GroupEntry)],
) -> Result<ColumnarEncoding, DbError> {
    if entries.is_empty() {
        return Ok((Vec::new(), Vec::new(), Vec::new()));
    }
    let n = entries.len();

    let keys_ipc = if num_group_cols == 0 {
        Vec::new()
    } else {
        let key_arrays = row_converter
            .convert_rows(entries.iter().map(|(k, _)| k.row()))
            .map_err(|e| DbError::Pipeline(format!("group key array build: {e}")))?;
        arrays_to_ipc(&key_arrays)?
    };

    // Per accumulator, per group: collect state scalars (also rebuilds the acc).
    let num_accs = agg_specs.len();
    let mut acc_rows: Vec<Vec<Vec<ScalarValue>>> =
        (0..num_accs).map(|_| Vec::with_capacity(n)).collect();
    let mut last_updated_ms = Vec::with_capacity(n);
    for (_, entry) in entries.iter_mut() {
        last_updated_ms.push(entry.last_updated_ms);
        for (i, acc) in entry.accs.iter_mut().enumerate() {
            acc_rows[i].push(snapshot_state_scalars(acc, &agg_specs[i], retractable)?);
        }
    }

    let mut acc_state_ipc = Vec::with_capacity(num_accs);
    for rows in acc_rows {
        let arity = rows.first().map_or(0, Vec::len);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(arity);
        for c in 0..arity {
            let col = ScalarValue::iter_to_array(rows.iter().map(|s| s[c].clone()))
                .map_err(|e| DbError::Pipeline(format!("acc state column build: {e}")))?;
            columns.push(col);
        }
        acc_state_ipc.push(arrays_to_ipc(&columns)?);
    }

    Ok((keys_ipc, acc_state_ipc, last_updated_ms))
}

struct DecodedGroup {
    row_key: arrow::row::OwnedRow,
    accs: Vec<Box<dyn datafusion_expr::Accumulator>>,
    last_updated_ms: i64,
}

/// Per-group decoded state: `(key, last_updated_ms, per-accumulator state arrays)`.
type DecodedGroupState = (arrow::row::OwnedRow, i64, Vec<Vec<ArrayRef>>);

#[cfg(feature = "cluster")]
struct DecodedAggMutation {
    groups: Vec<DecodedGroupState>,
    last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
}

/// A transaction image containing only keys touched by one recovered vnode chain. Delta-only
/// recovery starts from rebuilt live state; a FULL base starts empty because it is authoritative.
/// The live maps change only after the complete base + delta sequence succeeds.
#[cfg(feature = "cluster")]
struct StagedAggMutation {
    groups: AHashMap<arrow::row::OwnedRow, GroupEntry>,
    last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    affected: AHashSet<arrow::row::OwnedRow>,
}

#[cfg(feature = "cluster")]
fn validate_unique_decoded_group_keys(groups: &[DecodedGroupState]) -> Result<(), DbError> {
    let mut keys: AHashSet<&[u8]> = AHashSet::with_capacity(groups.len());
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

/// Inverse of [`encode_groups_columnar`]: rebuild one [`DecodedGroup`] per row.
fn decode_groups_columnar(
    row_converter: &arrow::row::RowConverter,
    num_group_cols: usize,
    agg_specs: &[AggFuncSpec],
    retractable: bool,
    keys_ipc: &[u8],
    acc_state_ipc: &[Vec<u8>],
    last_updated_ms: &[i64],
) -> Result<Vec<DecodedGroup>, DbError> {
    decode_columnar_state_arrays(
        row_converter,
        num_group_cols,
        keys_ipc,
        acc_state_ipc,
        last_updated_ms,
    )?
    .into_iter()
    .map(|(row_key, last_updated_ms, state_arrays)| {
        Ok(DecodedGroup {
            row_key,
            accs: build_accumulators_from_state(agg_specs, retractable, &state_arrays)?,
            last_updated_ms,
        })
    })
    .collect()
}

/// Row-concatenate two IPC stream batches sharing a schema; an empty side passes
/// the other through unchanged.
#[cfg(all(test, feature = "cluster"))]
fn concat_columnar_ipc(a: &[u8], b: &[u8]) -> Result<Vec<u8>, DbError> {
    if a.is_empty() {
        return Ok(b.to_vec());
    }
    if b.is_empty() {
        return Ok(a.to_vec());
    }
    let ba = laminar_core::serialization::deserialize_batch_stream(a)
        .map_err(|e| DbError::Pipeline(format!("columnar concat decode: {e}")))?;
    let bb = laminar_core::serialization::deserialize_batch_stream(b)
        .map_err(|e| DbError::Pipeline(format!("columnar concat decode: {e}")))?;
    let merged = arrow::compute::concat_batches(&ba.schema(), [&ba, &bb])
        .map_err(|e| DbError::Pipeline(format!("columnar concat: {e}")))?;
    laminar_core::serialization::serialize_batch_stream(&merged)
        .map_err(|e| DbError::Pipeline(format!("columnar concat encode: {e}")))
}

#[cfg(feature = "cluster")]
fn validate_checkpoint_layout_and_keys(
    checkpoint: &AggStateCheckpoint,
    operation: &str,
) -> Result<Vec<Vec<u8>>, DbError> {
    let group_count = checkpoint.last_updated_ms.len();
    if group_count == 0 {
        if !checkpoint.keys_ipc.is_empty()
            || !checkpoint.acc_state_ipc.is_empty()
            || !checkpoint.last_emitted.is_empty()
        {
            return Err(DbError::Pipeline(format!(
                "{operation}: non-canonical empty aggregate checkpoint contains state payloads"
            )));
        }
        return Ok(Vec::new());
    }
    if checkpoint.acc_state_ipc.is_empty() {
        return Err(DbError::Pipeline(format!(
            "{operation}: non-empty aggregate checkpoint has no accumulator state columns"
        )));
    }

    let keys = if checkpoint.keys_ipc.is_empty() {
        if group_count != 1 {
            return Err(DbError::Pipeline(format!(
                "{operation}: global aggregate checkpoint contains {group_count} groups"
            )));
        }
        // Global aggregates deliberately carry no key IPC. Keep their singleton identity distinct
        // from every encoded GROUP BY row while checking disjointness.
        vec![vec![0]]
    } else {
        let batch = laminar_core::serialization::deserialize_batch_stream(&checkpoint.keys_ipc)
            .map_err(|error| DbError::Pipeline(format!("{operation}: keys decode: {error}")))?;
        if batch.num_rows() != group_count {
            return Err(DbError::Pipeline(format!(
                "{operation}: key/timestamp row mismatch ({} vs {group_count})",
                batch.num_rows()
            )));
        }
        if batch.num_columns() == 0 {
            return Err(DbError::Pipeline(format!(
                "{operation}: keyed aggregate checkpoint has an empty key schema"
            )));
        }
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| arrow::row::SortField::new(field.data_type().clone()))
            .collect();
        let converter = arrow::row::RowConverter::new(fields)
            .map_err(|error| DbError::Pipeline(format!("{operation}: key layout: {error}")))?;
        let rows = converter
            .convert_columns(batch.columns())
            .map_err(|error| DbError::Pipeline(format!("{operation}: key rows: {error}")))?;
        rows.iter()
            .map(|row| {
                let mut identity = Vec::with_capacity(row.as_ref().len().saturating_add(1));
                identity.push(1);
                identity.extend_from_slice(row.as_ref());
                identity
            })
            .collect()
    };

    for state in &checkpoint.acc_state_ipc {
        if state.is_empty() {
            continue;
        }
        let batch =
            laminar_core::serialization::deserialize_batch_stream(state).map_err(|error| {
                DbError::Pipeline(format!("{operation}: accumulator decode: {error}"))
            })?;
        if batch.num_rows() != group_count {
            return Err(DbError::Pipeline(format!(
                "{operation}: accumulator/timestamp row mismatch ({} vs {group_count})",
                batch.num_rows()
            )));
        }
    }

    let mut unique = AHashSet::with_capacity(keys.len());
    if keys.iter().any(|key| !unique.insert(key.as_slice())) {
        return Err(DbError::Pipeline(format!(
            "{operation}: aggregate checkpoint contains a duplicate group key"
        )));
    }
    let mut emitted = AHashSet::with_capacity(checkpoint.last_emitted.len());
    if checkpoint
        .last_emitted
        .iter()
        .any(|entry| !emitted.insert(entry.key.as_slice()))
    {
        return Err(DbError::Pipeline(format!(
            "{operation}: aggregate checkpoint contains a duplicate changelog key"
        )));
    }
    if checkpoint.last_emitted.len() > group_count {
        return Err(DbError::Pipeline(format!(
            "{operation}: aggregate checkpoint contains more changelog keys than groups"
        )));
    }
    Ok(keys)
}

#[cfg(feature = "cluster")]
pub(crate) fn validate_agg_checkpoint_slice(
    checkpoint: &AggStateCheckpoint,
) -> Result<(), DbError> {
    validate_checkpoint_layout_and_keys(checkpoint, "vnode restore").map(|_| ())
}

/// Merge serialized aggregate slices over disjoint keys into one checkpoint.
#[cfg(feature = "cluster")]
#[allow(clippy::too_many_lines)]
pub(crate) fn merge_serialized_agg_cps(slices: &[bytes::Bytes]) -> Result<Vec<u8>, DbError> {
    let checkpoints = slices
        .iter()
        .map(|slice| {
            rkyv::from_bytes::<AggStateCheckpoint, rkyv::rancor::Error>(slice)
                .map_err(|e| DbError::Pipeline(format!("merge agg slices: decode: {e}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let first = checkpoints
        .first()
        .ok_or_else(|| DbError::Pipeline("merge agg slices: empty".into()))?;
    let fingerprint = first.fingerprint;
    if checkpoints
        .iter()
        .any(|cp| cp.fingerprint != first.fingerprint)
    {
        return Err(DbError::Pipeline(
            "merge agg slices: query fingerprint mismatch".into(),
        ));
    }
    let layouts = checkpoints
        .iter()
        .map(|checkpoint| validate_checkpoint_layout_and_keys(checkpoint, "merge agg slices"))
        .collect::<Result<Vec<_>, _>>()?;
    let logical_rows = layouts.iter().try_fold(0_usize, |total, keys| {
        total
            .checked_add(keys.len())
            .ok_or_else(|| DbError::Pipeline("merge agg row count overflow".into()))
    })?;
    let mut unique = AHashSet::with_capacity(logical_rows);
    if layouts
        .iter()
        .flatten()
        .any(|key| !unique.insert(key.as_slice()))
    {
        return Err(DbError::Pipeline(
            "merge agg slices: aggregate checkpoint keys are not disjoint".into(),
        ));
    }

    let (keys_ipc, encoded_key_rows) =
        merge_columnar_streams(checkpoints.iter().map(|cp| cp.keys_ipc.as_slice()), "keys")?;
    let has_encoded_keys = checkpoints
        .iter()
        .any(|checkpoint| !checkpoint.keys_ipc.is_empty());
    if has_encoded_keys && encoded_key_rows != logical_rows {
        return Err(DbError::Pipeline(format!(
            "merge agg slices: merged key row mismatch ({encoded_key_rows} vs {logical_rows})"
        )));
    }

    let accumulator_columns = checkpoints
        .iter()
        .zip(&layouts)
        .find(|(_, keys)| !keys.is_empty())
        .map_or(0, |(checkpoint, _)| checkpoint.acc_state_ipc.len());
    if checkpoints.iter().zip(&layouts).any(|(checkpoint, keys)| {
        !keys.is_empty() && checkpoint.acc_state_ipc.len() != accumulator_columns
    }) {
        return Err(DbError::Pipeline(
            "merge agg slices: accumulator column count mismatch".into(),
        ));
    }
    let mut acc_state_ipc = Vec::with_capacity(accumulator_columns);
    for column in 0..accumulator_columns {
        let (merged, acc_rows) = merge_columnar_streams(
            checkpoints
                .iter()
                .map(|cp| cp.acc_state_ipc.get(column).map_or(&[][..], Vec::as_slice)),
            "accumulator",
        )?;
        // An accumulator with zero serialized state columns is represented by an empty byte vector
        // for every group and is rebuilt from its default state during restore.
        if !merged.is_empty() && acc_rows != logical_rows {
            return Err(DbError::Pipeline(format!(
                "merge agg slices: group/accumulator row mismatch ({logical_rows} vs {acc_rows})"
            )));
        }
        acc_state_ipc.push(merged);
    }

    let timestamp_capacity = checkpoints.iter().try_fold(0_usize, |total, cp| {
        total
            .checked_add(cp.last_updated_ms.len())
            .ok_or_else(|| DbError::Pipeline("merge agg timestamp count overflow".into()))
    })?;
    if timestamp_capacity != logical_rows {
        return Err(DbError::Pipeline(format!(
            "merge agg slices: merged group/timestamp row mismatch ({logical_rows} vs {timestamp_capacity})"
        )));
    }
    let emitted_capacity = checkpoints.iter().try_fold(0_usize, |total, cp| {
        total
            .checked_add(cp.last_emitted.len())
            .ok_or_else(|| DbError::Pipeline("merge agg emitted count overflow".into()))
    })?;
    let mut last_updated_ms = Vec::with_capacity(timestamp_capacity);
    let mut last_emitted = Vec::with_capacity(emitted_capacity);
    for checkpoint in checkpoints {
        last_updated_ms.extend(checkpoint.last_updated_ms);
        last_emitted.extend(checkpoint.last_emitted);
    }
    let merged = AggStateCheckpoint {
        fingerprint,
        keys_ipc,
        acc_state_ipc,
        last_updated_ms,
        last_emitted,
    };
    validate_checkpoint_layout_and_keys(&merged, "merge agg slices")?;
    rkyv::to_bytes::<rkyv::rancor::Error>(&merged)
        .map(|v| v.to_vec())
        .map_err(|e| DbError::Pipeline(format!("merge agg slices: encode: {e}")))
}

#[cfg(feature = "cluster")]
fn decode_columnar_stream(bytes: &[u8], label: &str) -> Result<(RecordBatch, usize), DbError> {
    let batch = laminar_core::serialization::deserialize_batch_stream(bytes)
        .map_err(|e| DbError::Pipeline(format!("merge agg {label} decode: {e}")))?;
    let rows = batch.num_rows();
    Ok((batch, rows))
}

#[cfg(feature = "cluster")]
fn merge_columnar_streams<'a>(
    streams: impl Iterator<Item = &'a [u8]>,
    label: &str,
) -> Result<(Vec<u8>, usize), DbError> {
    let batches = streams
        .filter(|bytes| !bytes.is_empty())
        .map(|bytes| decode_columnar_stream(bytes, label).map(|(batch, _)| batch))
        .collect::<Result<Vec<_>, _>>()?;
    let Some(first) = batches.first() else {
        return Ok((Vec::new(), 0));
    };
    let rows = batches.iter().try_fold(0_usize, |total, batch| {
        total
            .checked_add(batch.num_rows())
            .ok_or_else(|| DbError::Pipeline(format!("merge agg {label} row count overflow")))
    })?;
    let merged = arrow::compute::concat_batches(&first.schema(), batches.iter())
        .map_err(|e| DbError::Pipeline(format!("merge agg {label} concat: {e}")))?;
    let encoded = laminar_core::serialization::serialize_batch_stream(&merged)
        .map_err(|e| DbError::Pipeline(format!("merge agg {label} encode: {e}")))?;
    Ok((encoded, rows))
}

fn insert_vnode_tracking_key(
    map: &mut AHashMap<u32, AHashSet<arrow::row::OwnedRow>>,
    vnode: u32,
    key: arrow::row::OwnedRow,
) {
    map.entry(vnode).or_default().insert(key);
}

pub(crate) struct IncrementalAggState {
    query_sql: String,
    #[cfg(test)]
    pre_agg_sql: String,
    num_group_cols: usize,
    group_types: Vec<DataType>,
    agg_specs: Vec<AggFuncSpec>,
    groups: AHashMap<arrow::row::OwnedRow, GroupEntry>,
    row_converter: arrow::row::RowConverter,
    output_schema: SchemaRef,
    compiled_projection: Option<CompiledProjection>,
    cached_pre_agg_physical: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    having_sql: Option<String>,
    max_groups: usize,
    emit_changelog: bool,
    last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    // Group keys touched since the last emit. The changelog path re-evaluates only
    // these instead of scanning every group. Transient (cleared each emit),
    // populated only when emit_changelog, so not checkpointed.
    dirty_keys: AHashSet<arrow::row::OwnedRow>,
    weight_col_idx: Option<usize>,
    // Delta-state tracking: keys mutated since the last per-vnode capture, bucketed by vnode.
    // Populated only while `delta_vnode_count` is set (off by default → zero cost).
    #[cfg(feature = "cluster")]
    delta_enabled: bool,
    delta_vnode_count: Option<u32>,
    dirty_keys_by_vnode: AHashMap<u32, AHashSet<arrow::row::OwnedRow>>,
    // Changelog emission keys whose `last_emitted` changed since the last per-vnode
    // capture (set in `emit_changelog_delta`). Populated only while `delta_vnode_count`
    // is set; lets changelog aggregates take the delta path.
    last_emitted_dirty_by_vnode: AHashMap<u32, AHashSet<arrow::row::OwnedRow>>,
    // Deltas emitted since the last full capture, per vnode — bounds the chain so the full
    // base never ages out of the prune window; cleared to a full re-base on restore/acquire.
    #[cfg(feature = "cluster")]
    delta_chain_len: AHashMap<u32, u32>,
    // Vnodes a failed epoch must re-base FULL next capture; re-armed into `touched` so an emptied
    // vnode (which otherwise drops out of it) doesn't resurrect its dropped groups on recovery.
    #[cfg(feature = "cluster")]
    force_rebase_vnodes: rustc_hash::FxHashSet<u32>,
}

impl IncrementalAggState {
    /// Number of leading GROUP BY columns; used by the shuffle path for hashing.
    #[cfg(feature = "cluster")]
    #[must_use]
    pub(crate) fn num_group_cols(&self) -> usize {
        self.num_group_cols
    }

    pub(crate) fn cluster_state_rejection(&self, reads_changelog: bool) -> Option<String> {
        if self.num_group_cols != 0 {
            return Some(
                "keyed aggregates retain operator-owned map state without a live-state byte budget"
                    .into(),
            );
        }
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
}

/// A per-vnode state delta: the groups changed since the chain base, columnar with the same shape
/// as a `FULL` slice.
#[cfg(feature = "cluster")]
pub(crate) struct AggVnodeDelta {
    pub(crate) changed: AggStateCheckpoint,
}

/// What a per-vnode capture emits for one vnode under delta-enabled checkpointing.
#[cfg(feature = "cluster")]
pub(crate) enum VnodeCapture {
    /// Full columnar slice — the chain base (re-base).
    Full(AggStateCheckpoint),
    /// Incremental delta against the previous epoch's partial for this vnode.
    Delta(AggVnodeDelta),
}

impl IncrementalAggState {
    /// Attempt to build an `IncrementalAggState` by introspecting the logical
    /// plan of the given SQL query. Returns `None` if the query does not
    /// contain an `Aggregate` node (not an aggregation query).
    #[allow(clippy::too_many_lines)]
    pub async fn try_from_sql(
        ctx: &SessionContext,
        sql: &str,
        emit_changelog: bool,
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

        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &output_schema);
        let having_sql = if having_filter.is_none() {
            having_predicate.as_ref().map(expr_to_sql)
        } else {
            None
        };

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
        let row_converter = arrow::row::RowConverter::new(sort_fields)
            .map_err(|e| DbError::Pipeline(format!("row converter init: {e}")))?;

        Ok(Some(Self {
            query_sql: sql.to_string(),
            #[cfg(test)]
            pre_agg_sql,
            num_group_cols,
            group_types,
            agg_specs,
            groups: AHashMap::new(),
            row_converter,
            output_schema,
            compiled_projection,
            cached_pre_agg_physical,
            having_filter,
            having_sql,
            max_groups: 1_000_000,
            emit_changelog,
            last_emitted: AHashMap::new(),
            dirty_keys: AHashSet::new(),
            weight_col_idx,
            #[cfg(feature = "cluster")]
            delta_enabled: false,
            delta_vnode_count: None,
            dirty_keys_by_vnode: AHashMap::new(),
            last_emitted_dirty_by_vnode: AHashMap::new(),
            #[cfg(feature = "cluster")]
            delta_chain_len: AHashMap::new(),
            #[cfg(feature = "cluster")]
            force_rebase_vnodes: rustc_hash::FxHashSet::default(),
        }))
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn delta_enabled(&self) -> bool {
        self.delta_enabled
    }

    #[cfg(feature = "cluster")]
    pub(crate) fn set_delta_enabled(&mut self, enabled: bool) {
        self.delta_enabled = enabled;
    }

    /// Vnode for a group key under delta bucketing — same hash as the capture path.
    fn delta_vnode_of(&self, key_bytes: &[u8], count: u32) -> u32 {
        if self.num_group_cols == 0 {
            0
        } else {
            #[allow(clippy::cast_possible_truncation)]
            {
                (laminar_core::state::key_hash(key_bytes) % u64::from(count)) as u32
            }
        }
    }

    /// Mark a changelog emission key dirty for the delta path. No-op unless delta
    /// capture is enabled (`delta_vnode_count` set), so it costs nothing by default.
    fn mark_last_emitted_dirty(&mut self, key: &arrow::row::OwnedRow) {
        if let Some(count) = self.delta_vnode_count {
            let v = self.delta_vnode_of(key.as_ref(), count);
            self.last_emitted_dirty_by_vnode
                .entry(v)
                .or_default()
                .insert(key.clone());
        }
    }

    fn mark_emit_dirty(&mut self, key: arrow::row::OwnedRow) {
        self.dirty_keys.insert(key);
    }

    fn insert_last_emitted(&mut self, key: arrow::row::OwnedRow, values: Vec<ScalarValue>) {
        self.last_emitted.insert(key, values);
    }

    #[cfg(feature = "cluster")]
    fn remove_last_emitted(&mut self, key: &arrow::row::OwnedRow) -> Option<Vec<ScalarValue>> {
        self.last_emitted.remove(key)
    }

    pub fn process_batch(&mut self, batch: &RecordBatch, watermark_ms: i64) -> Result<(), DbError> {
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
            if !self.groups.contains_key(&key) {
                incoming_new_groups = incoming_new_groups.saturating_add(1);
            }
            grouped_rows.push((key, indices));
        }
        let required_groups = self.groups.len().saturating_add(incoming_new_groups);
        if required_groups > self.max_groups {
            return Err(DbError::Pipeline(format!(
                "aggregate group limit exceeded: current={}, incoming_new={}, limit={}",
                self.groups.len(),
                incoming_new_groups,
                self.max_groups
            )));
        }

        for (owned_key, indices) in grouped_rows {
            let key;
            let entry = match self.groups.entry(owned_key) {
                std::collections::hash_map::Entry::Occupied(e) => {
                    key = e.key().clone();
                    e.into_mut()
                }
                std::collections::hash_map::Entry::Vacant(e) => {
                    key = e.key().clone();
                    let mut accs = Vec::with_capacity(self.agg_specs.len());
                    for spec in &self.agg_specs {
                        let acc = if self.weight_col_idx.is_some() {
                            spec.create_retractable_accumulator()?
                        } else {
                            spec.create_accumulator()?
                        };
                        accs.push(acc);
                    }
                    e.insert(GroupEntry {
                        accs,
                        last_updated_ms: watermark_ms,
                    })
                }
            };
            let update_result = Self::update_group_accumulators(
                &mut entry.accs,
                batch,
                &indices,
                &self.agg_specs,
                self.weight_col_idx,
            );
            entry.last_updated_ms = watermark_ms;
            update_result?;
            if self.emit_changelog {
                self.mark_emit_dirty(key.clone());
            }
            if let Some(count) = self.delta_vnode_count {
                let v = self.delta_vnode_of(key.as_ref(), count);
                insert_vnode_tracking_key(&mut self.dirty_keys_by_vnode, v, key);
            }
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
        if !self.groups.contains_key(&empty_key) {
            let mut accs = Vec::with_capacity(self.agg_specs.len());
            for spec in &self.agg_specs {
                let acc = if self.weight_col_idx.is_some() {
                    spec.create_retractable_accumulator()?
                } else {
                    spec.create_accumulator()?
                };
                accs.push(acc);
            }
            self.groups.insert(
                empty_key.clone(),
                GroupEntry {
                    accs,
                    last_updated_ms: watermark_ms,
                },
            );
        }
        let entry = self.groups.get_mut(&empty_key).unwrap();
        entry.last_updated_ms = watermark_ms;
        #[allow(clippy::cast_possible_truncation)]
        let all_indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
        let res = Self::update_group_accumulators(
            &mut entry.accs,
            batch,
            &all_indices,
            &self.agg_specs,
            self.weight_col_idx,
        );
        if self.delta_vnode_count.is_some() {
            let key = global_aggregate_key();
            insert_vnode_tracking_key(&mut self.dirty_keys_by_vnode, 0, key);
        }
        if self.emit_changelog {
            self.mark_emit_dirty(empty_key);
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
        if self.groups.is_empty() {
            return Ok(Vec::new());
        }

        let num_rows = self.groups.len();

        let group_arrays = if self.num_group_cols > 0 {
            self.row_converter
                .convert_rows(self.groups.keys().map(arrow::row::OwnedRow::row))
                .map_err(|e| DbError::Pipeline(format!("group key array build: {e}")))?
        } else {
            Vec::new()
        };

        let mut agg_arrays: Vec<ArrayRef> = Vec::with_capacity(self.agg_specs.len());
        for (agg_idx, spec) in self.agg_specs.iter().enumerate() {
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(num_rows);
            for entry in self.groups.values_mut() {
                let sv = entry.accs[agg_idx]
                    .evaluate()
                    .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;
                scalars.push(sv);
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

    #[allow(clippy::too_many_lines)]
    fn emit_changelog_delta(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();
        let mut insert_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut insert_vals: Vec<Vec<ScalarValue>> = Vec::new();

        // Only touched groups can differ from `last_emitted`. Take the set so the loop can
        // borrow `groups`/`last_emitted`. On any error before the output batch is built, the
        // whole set is restored and `last_emitted` is left untouched — its mutations are
        // deferred to the commit step below — so a mid-emit failure neither silently drops
        // pending groups nor drifts downstream from a partially-applied emit (EX-3).
        let mut dirty = std::mem::take(&mut self.dirty_keys);
        let mut eval_err: Option<DbError> = None;
        for key in &dirty {
            let Some(entry) = self.groups.get_mut(key) else {
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
                    break;
                }
            };

            if let Some(old) = self.last_emitted.get(key) {
                // ScalarValue::eq treats NaN != NaN; short-circuit to avoid
                // an infinite retract+insert loop on float aggregates.
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
                }
            } else {
                insert_keys.push(key.clone());
                insert_vals.push(current);
            }
        }
        if let Some(e) = eval_err {
            self.dirty_keys = dirty;
            return Err(e);
        }

        let retract_count = retract_keys.len();
        let total = retract_count + insert_keys.len();
        if total == 0 {
            dirty.clear();
            self.dirty_keys = dirty;
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
                self.dirty_keys = dirty;
                return Err(e);
            }
        };

        // Commit: advance `last_emitted` only now that the output batch is built. The insert half
        // of `all_keys`/`all_vals` (after the retracts) is exactly the changed/new groups' new
        // values, so derive the update from it rather than keeping a parallel Vec.
        for (key, current) in all_keys.into_iter().zip(all_vals).skip(retract_count) {
            self.mark_last_emitted_dirty(&key);
            self.insert_last_emitted(key, current);
        }
        dirty.clear();
        self.dirty_keys = dirty;

        // Chain-apply paths only admit an entry whose group is resident, so the invariant holds.
        debug_assert!(
            self.last_emitted
                .keys()
                .all(|k| self.groups.contains_key(k)),
            "last_emitted must be a subset of groups"
        );

        Ok(vec![batch])
    }

    pub fn having_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.having_filter.as_ref()
    }

    pub fn having_sql(&self) -> Option<&str> {
        self.having_sql.as_deref()
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

    pub(crate) fn checkpoint_groups(&mut self) -> Result<AggStateCheckpoint, DbError> {
        let fingerprint = self.query_fingerprint();
        let retractable = self.weight_col_idx.is_some();
        let mut entries: Vec<(arrow::row::OwnedRow, &mut GroupEntry)> = self
            .groups
            .iter_mut()
            .map(|(k, v)| (k.clone(), v))
            .collect();
        let encoded = encode_groups_columnar(
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            retractable,
            &mut entries,
        );
        drop(entries);
        let (keys_ipc, acc_state_ipc, last_updated_ms) = encoded?;

        let last_emitted = self.checkpoint_last_emitted()?;
        let checkpoint = AggStateCheckpoint {
            fingerprint,
            keys_ipc,
            acc_state_ipc,
            last_updated_ms,
            last_emitted,
        };
        Ok(checkpoint)
    }

    /// Encode the changelog `last_emitted` map per-entry (still keyed individually;
    /// columnarizing it is out of scope). Empty unless `emit_changelog`.
    fn checkpoint_last_emitted(&self) -> Result<Vec<EmittedCheckpoint>, DbError> {
        if !self.emit_changelog {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(self.last_emitted.len());
        for (row_key, vals) in &self.last_emitted {
            let sv_key =
                row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
            out.push(EmittedCheckpoint {
                key: scalars_to_ipc(&sv_key)?,
                values: scalars_to_ipc(vals)?,
            });
        }
        Ok(out)
    }

    pub(crate) fn restore_groups(
        &mut self,
        checkpoint: &AggStateCheckpoint,
    ) -> Result<usize, DbError> {
        let current_fp = self.query_fingerprint();
        if checkpoint.fingerprint != current_fp {
            return Err(DbError::Pipeline(format!(
                "checkpoint fingerprint mismatch: saved={}, current={}",
                checkpoint.fingerprint, current_fp
            )));
        }
        validate_columnar_acc_shape(checkpoint, &self.agg_specs)?;
        // Build locally then swap so a mid-list decode error can't leave
        // last_emitted partially populated.
        let new_last_emitted = self.decode_last_emitted(&checkpoint.last_emitted)?;
        let retractable = self.weight_col_idx.is_some();
        let decoded = decode_groups_columnar(
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            retractable,
            &checkpoint.keys_ipc,
            &checkpoint.acc_state_ipc,
            &checkpoint.last_updated_ms,
        )?;
        let restored = decoded.len();
        let mut new_groups: AHashMap<arrow::row::OwnedRow, GroupEntry> =
            AHashMap::with_capacity(restored);
        for g in decoded {
            match new_groups.entry(g.row_key) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(GroupEntry {
                        accs: g.accs,
                        last_updated_ms: g.last_updated_ms,
                    });
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(DbError::Pipeline(
                        "aggregate checkpoint contains a duplicate group key".into(),
                    ));
                }
            }
        }

        if new_last_emitted
            .keys()
            .any(|row_key| !new_groups.contains_key(row_key))
        {
            return Err(DbError::Pipeline(
                "aggregate checkpoint contains changelog state for a missing group".into(),
            ));
        }

        self.groups = new_groups;
        self.last_emitted = new_last_emitted;
        // Restored state is internally consistent (groups == last_emitted), so
        // nothing is pending for the changelog path.
        self.dirty_keys.clear();
        // The restored state is the new baseline — no pending delta entries.
        self.dirty_keys_by_vnode.clear();
        self.last_emitted_dirty_by_vnode.clear();
        #[cfg(feature = "cluster")]
        self.delta_chain_len.clear();
        Ok(restored)
    }

    /// Decode the per-entry changelog `last_emitted` checkpoint into the live map.
    fn decode_last_emitted(
        &self,
        entries: &[EmittedCheckpoint],
    ) -> Result<AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>, DbError> {
        let mut out: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>> =
            AHashMap::with_capacity(entries.len());
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

    /// Partition state into one [`AggStateCheckpoint`] per vnode using the same hash as the shuffle.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // checkpoint path; vnode-keyed map
    pub(crate) fn checkpoint_groups_by_vnode(
        &mut self,
        vnode_count: u32,
    ) -> Result<std::collections::HashMap<u32, AggStateCheckpoint>, DbError> {
        if vnode_count == 0 {
            return Err(DbError::Pipeline("vnode_count must be > 0".to_string()));
        }
        let fingerprint = self.query_fingerprint();
        let global = self.num_group_cols == 0;
        let retractable = self.weight_col_idx.is_some();
        let vnode_of = |row_key: &arrow::row::OwnedRow| -> u32 {
            if global {
                0
            } else {
                #[allow(clippy::cast_possible_truncation)]
                let v = (laminar_core::state::key_hash(row_key.as_ref()) % u64::from(vnode_count))
                    as u32;
                v
            }
        };

        // Bucket the live groups by vnode first, then columnar-encode each subset.
        let mut by_vnode: std::collections::HashMap<
            u32,
            Vec<(arrow::row::OwnedRow, &mut GroupEntry)>,
        > = std::collections::HashMap::new();
        for (row_key, entry) in &mut self.groups {
            let vnode = vnode_of(row_key);
            by_vnode
                .entry(vnode)
                .or_default()
                .push((row_key.clone(), entry));
        }

        let mut buckets: std::collections::HashMap<u32, AggStateCheckpoint> =
            std::collections::HashMap::with_capacity(by_vnode.len());
        for (vnode, mut entries) in by_vnode {
            let encoded = encode_groups_columnar(
                &self.row_converter,
                self.num_group_cols,
                &self.agg_specs,
                retractable,
                &mut entries,
            );
            drop(entries);
            let (keys_ipc, acc_state_ipc, last_updated_ms) = encoded?;
            buckets.insert(
                vnode,
                AggStateCheckpoint {
                    fingerprint,
                    keys_ipc,
                    acc_state_ipc,
                    last_updated_ms,
                    last_emitted: Vec::new(),
                },
            );
        }

        if self.emit_changelog {
            for (row_key, vals) in &self.last_emitted {
                let vnode = vnode_of(row_key);
                let sv_key =
                    row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
                buckets
                    .entry(vnode)
                    .or_insert_with(|| AggStateCheckpoint {
                        fingerprint,
                        keys_ipc: Vec::new(),
                        acc_state_ipc: Vec::new(),
                        last_updated_ms: Vec::new(),
                        last_emitted: Vec::new(),
                    })
                    .last_emitted
                    .push(EmittedCheckpoint {
                        key: scalars_to_ipc(&sv_key)?,
                        values: scalars_to_ipc(vals)?,
                    });
            }
        }

        // Delta tracking re-bases on this capture: the dirty sets reset, so the next
        // checkpoint's delta is measured against the state staged here.
        if self.delta_enabled {
            self.delta_vnode_count = Some(vnode_count);
            self.dirty_keys_by_vnode.clear();
            self.last_emitted_dirty_by_vnode.clear();
        }

        Ok(buckets)
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_full_vnode(
        &mut self,
        vnode: u32,
        vnode_count: u32,
        fingerprint: u64,
        retractable: bool,
    ) -> Result<VnodeCapture, DbError> {
        let global = self.num_group_cols == 0;
        let vnode_of = |key: &arrow::row::OwnedRow| -> u32 {
            if global {
                0
            } else {
                #[allow(clippy::cast_possible_truncation)]
                {
                    (laminar_core::state::key_hash(key.as_ref()) % u64::from(vnode_count)) as u32
                }
            }
        };
        let mut entries: Vec<(arrow::row::OwnedRow, &mut GroupEntry)> = self
            .groups
            .iter_mut()
            .filter(|(key, _)| vnode_of(key) == vnode)
            .map(|(key, entry)| (key.clone(), entry))
            .collect();
        let encoded = encode_groups_columnar(
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            retractable,
            &mut entries,
        );
        drop(entries);
        let (keys_ipc, acc_state_ipc, last_updated_ms) = encoded?;
        let last_emitted = self.last_emitted_for_vnode(vnode, vnode_count, None)?;
        let full = AggStateCheckpoint {
            fingerprint,
            keys_ipc,
            acc_state_ipc,
            last_updated_ms,
            last_emitted,
        };
        Ok(VnodeCapture::Full(full))
    }

    /// Per-vnode capture under delta checkpointing: each touched vnode emits a FULL re-base or an
    /// incremental DELTA. Re-bases FULL when the vnode has no chain base (fresh / just-acquired) or
    /// the chain reached `chain_bound`. Changelog aggregates delta-encode `last_emitted` alongside the
    /// groups, so the dedup map survives chain replay. Clears the per-vnode dirty sets; the next
    /// delta measures against the state captured here.
    #[cfg(feature = "cluster")]
    #[allow(clippy::disallowed_types)] // checkpoint path; vnode-keyed map
    pub(crate) fn checkpoint_delta_by_vnode(
        &mut self,
        vnode_count: u32,
        chain_bound: u32,
    ) -> Result<std::collections::HashMap<u32, VnodeCapture>, DbError> {
        if vnode_count == 0 {
            return Err(DbError::Pipeline("vnode_count must be > 0".to_string()));
        }

        let retractable = self.weight_col_idx.is_some();
        let fingerprint = self.query_fingerprint();
        let global = self.num_group_cols == 0;
        let vnode_of = |row_key: &arrow::row::OwnedRow| -> u32 {
            if global {
                0
            } else {
                #[allow(clippy::cast_possible_truncation)]
                {
                    (laminar_core::state::key_hash(row_key.as_ref()) % u64::from(vnode_count))
                        as u32
                }
            }
        };

        // Vnodes holding groups remain in the chain even when this epoch has no changes.
        let mut touched: std::collections::HashSet<u32> = std::collections::HashSet::new();
        for row_key in self.groups.keys() {
            touched.insert(vnode_of(row_key));
        }
        // Re-visit vnodes a failed epoch must re-base — even emptied ones that fell out of the sets
        // above; `force_full_rebase` dropped their chain len, so each re-bases FULL below.
        for v in self.force_rebase_vnodes.drain() {
            touched.insert(v);
        }

        let mut out: std::collections::HashMap<u32, VnodeCapture> =
            std::collections::HashMap::with_capacity(touched.len());
        for v in touched {
            let force_full = match self.delta_chain_len.get(&v).copied() {
                None => true, // no base yet (fresh / just-acquired)
                Some(n) => n >= chain_bound,
            };
            if force_full {
                let cap = self.checkpoint_full_vnode(v, vnode_count, fingerprint, retractable)?;
                out.insert(v, cap);
                self.delta_chain_len.insert(v, 0);
            } else {
                let delta = self.encode_delta_for_vnode(v)?;
                out.insert(v, VnodeCapture::Delta(delta));
                *self.delta_chain_len.entry(v).or_insert(0) += 1;
            }
            self.dirty_keys_by_vnode.remove(&v);
            self.last_emitted_dirty_by_vnode.remove(&v);
        }

        self.delta_vnode_count = Some(vnode_count);
        Ok(out)
    }

    /// Build changelog `last_emitted` entries for one vnode (empty for non-changelog
    /// aggs). `only` restricts to specific keys (the delta's dirty emission set);
    /// `None` captures every entry for the vnode (a FULL re-base). Lets a recovered
    /// chain reproduce the dedup map so the first post-recovery emit is exact.
    #[cfg(feature = "cluster")]
    fn last_emitted_for_vnode(
        &self,
        vnode: u32,
        vnode_count: u32,
        only: Option<&AHashSet<arrow::row::OwnedRow>>,
    ) -> Result<Vec<EmittedCheckpoint>, DbError> {
        if !self.emit_changelog {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for (row_key, vals) in &self.last_emitted {
            if self.delta_vnode_of(row_key.as_ref(), vnode_count) != vnode {
                continue;
            }
            if only.is_some_and(|keys| !keys.contains(row_key)) {
                continue;
            }
            let sv_key =
                row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
            out.push(EmittedCheckpoint {
                key: scalars_to_ipc(&sv_key)?,
                values: scalars_to_ipc(vals)?,
            });
        }
        Ok(out)
    }

    /// Encode changed groups for `vnode` via the columnar FULL encoding over the dirty subset.
    #[cfg(feature = "cluster")]
    pub(crate) fn encode_delta_for_vnode(&mut self, vnode: u32) -> Result<AggVnodeDelta, DbError> {
        let fingerprint = self.query_fingerprint();
        let retractable = self.weight_col_idx.is_some();

        let changed = self
            .dirty_keys_by_vnode
            .get(&vnode)
            .cloned()
            .unwrap_or_default();
        let mut entries: Vec<(arrow::row::OwnedRow, &mut GroupEntry)> = self
            .groups
            .iter_mut()
            .filter(|(k, _)| changed.contains(*k))
            .map(|(k, v)| (k.clone(), v))
            .collect();
        let encoded = encode_groups_columnar(
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            retractable,
            &mut entries,
        );
        drop(entries); // release the &mut self.groups borrow before reading last_emitted
        let (keys_ipc, acc_state_ipc, last_updated_ms) = encoded?;

        // Changed emission entries ride in `changed.last_emitted`.
        let vnode_count = self.delta_vnode_count.unwrap_or(1);
        let last_emitted = self.last_emitted_for_vnode(
            vnode,
            vnode_count,
            self.last_emitted_dirty_by_vnode.get(&vnode),
        )?;

        Ok(AggVnodeDelta {
            changed: AggStateCheckpoint {
                fingerprint,
                keys_ipc,
                acc_state_ipc,
                last_updated_ms,
                last_emitted,
            },
        })
    }

    #[cfg(feature = "cluster")]
    fn decode_recovery_mutation(
        &self,
        checkpoint: &AggStateCheckpoint,
        context: &str,
    ) -> Result<DecodedAggMutation, DbError> {
        let current_fp = self.query_fingerprint();
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

    #[cfg(feature = "cluster")]
    fn decode_recovery_delta(&self, delta: &AggVnodeDelta) -> Result<DecodedAggMutation, DbError> {
        self.decode_recovery_mutation(&delta.changed, "delta")
    }

    /// Rebuild one live group for an off-side recovery transaction.
    #[cfg(feature = "cluster")]
    fn clone_group_for_recovery(
        &mut self,
        key: &arrow::row::OwnedRow,
    ) -> Result<Option<GroupEntry>, DbError> {
        let Some(entry) = self.groups.get_mut(key) else {
            return Ok(None);
        };
        if entry.accs.len() != self.agg_specs.len() {
            return Err(DbError::Pipeline(
                "live aggregate accumulator shape is inconsistent".into(),
            ));
        }
        let retractable = self.weight_col_idx.is_some();
        let staged = (|| {
            let mut state_arrays = Vec::with_capacity(entry.accs.len());
            for (accumulator, spec) in entry.accs.iter_mut().zip(&self.agg_specs) {
                let state = snapshot_state_scalars(accumulator, spec, retractable)?;
                let arrays = state
                    .iter()
                    .map(|value| {
                        value
                            .to_array()
                            .map_err(|error| DbError::Pipeline(format!("scalar to array: {error}")))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                state_arrays.push(arrays);
            }
            Ok::<_, DbError>(GroupEntry {
                accs: build_accumulators_from_state(&self.agg_specs, retractable, &state_arrays)?,
                last_updated_ms: entry.last_updated_ms,
            })
        })();
        staged.map(Some)
    }

    #[cfg(feature = "cluster")]
    fn build_delta_recovery_image(
        &mut self,
        affected: AHashSet<arrow::row::OwnedRow>,
    ) -> Result<StagedAggMutation, DbError> {
        let mut groups = AHashMap::with_capacity(affected.len());
        let mut last_emitted = AHashMap::with_capacity(affected.len());
        for key in &affected {
            if let Some(entry) = self.clone_group_for_recovery(key)? {
                groups.insert(key.clone(), entry);
            }
            if let Some(values) = self.last_emitted.get(key) {
                if !self.groups.contains_key(key) {
                    return Err(DbError::Pipeline(
                        "live aggregate contains changelog state for a missing group".into(),
                    ));
                }
                last_emitted.insert(key.clone(), values.clone());
            }
        }
        Ok(StagedAggMutation {
            groups,
            last_emitted,
            affected,
        })
    }

    #[cfg(feature = "cluster")]
    fn install_recovery_base(
        &self,
        staged: &mut StagedAggMutation,
        mutation: DecodedAggMutation,
    ) -> Result<Vec<arrow::row::OwnedRow>, DbError> {
        let retractable = self.weight_col_idx.is_some();
        let mut merged_keys = Vec::with_capacity(mutation.groups.len());
        for (row_key, last_updated_ms, state_arrays) in mutation.groups {
            merged_keys.push(row_key.clone());
            let entry = GroupEntry {
                accs: build_accumulators_from_state(&self.agg_specs, retractable, &state_arrays)?,
                last_updated_ms,
            };
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
        Ok(merged_keys)
    }

    #[cfg(feature = "cluster")]
    fn apply_recovery_delta_to_image(
        &self,
        staged: &mut StagedAggMutation,
        delta: DecodedAggMutation,
    ) -> Result<(), DbError> {
        let retractable = self.weight_col_idx.is_some();
        for (row_key, last_updated_ms, state_arrays) in delta.groups {
            staged.groups.insert(
                row_key,
                GroupEntry {
                    accs: build_accumulators_from_state(
                        &self.agg_specs,
                        retractable,
                        &state_arrays,
                    )?,
                    last_updated_ms,
                },
            );
        }
        for (row_key, values) in delta.last_emitted {
            if !staged.groups.contains_key(&row_key) {
                return Err(DbError::Pipeline(
                    "delta checkpoint contains changelog state for a missing group".into(),
                ));
            }
            staged.last_emitted.insert(row_key, values);
        }
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn commit_recovery_image(
        &mut self,
        mut staged: StagedAggMutation,
        merged_keys: &[arrow::row::OwnedRow],
    ) {
        for key in staged.affected {
            self.groups.remove(&key);
            if let Some(entry) = staged.groups.remove(&key) {
                self.groups.insert(key.clone(), entry);
            }
            self.remove_last_emitted(&key);
            if let Some(values) = staged.last_emitted.remove(&key) {
                self.insert_last_emitted(key, values);
            }
        }

        // Preserve the former merge bookkeeping, but publish it only with the state transaction.
        for row_key in merged_keys {
            if self.emit_changelog {
                self.mark_emit_dirty(row_key.clone());
            }
            if let Some(count) = self.delta_vnode_count {
                let vnode = self.delta_vnode_of(row_key.as_ref(), count);
                insert_vnode_tracking_key(&mut self.dirty_keys_by_vnode, vnode, row_key.clone());
            }
        }
    }

    #[cfg(feature = "cluster")]
    fn apply_recovery_transaction(
        &mut self,
        base: Option<&AggStateCheckpoint>,
        deltas: &[AggVnodeDelta],
    ) -> Result<usize, DbError> {
        // Decode every payload before touching even the live accumulator snapshots. A malformed late
        // delta therefore cannot consume or partially merge the earlier base.
        let decoded_base = base
            .map(|checkpoint| self.decode_recovery_mutation(checkpoint, "merge"))
            .transpose()?;
        let decoded_deltas = deltas
            .iter()
            .map(|delta| self.decode_recovery_delta(delta))
            .collect::<Result<Vec<_>, _>>()?;

        let mut affected = AHashSet::new();
        if let Some(mutation) = &decoded_base {
            affected.extend(mutation.groups.iter().map(|(key, _, _)| key.clone()));
            affected.extend(mutation.last_emitted.keys().cloned());
        }
        for delta in &decoded_deltas {
            affected.extend(delta.groups.iter().map(|(key, _, _)| key.clone()));
            affected.extend(delta.last_emitted.keys().cloned());
        }

        let mut staged = if decoded_base.is_some() {
            StagedAggMutation {
                groups: AHashMap::with_capacity(affected.len()),
                last_emitted: AHashMap::with_capacity(affected.len()),
                affected,
            }
        } else {
            self.build_delta_recovery_image(affected)?
        };
        let merged_keys = match decoded_base {
            Some(mutation) => self.install_recovery_base(&mut staged, mutation)?,
            None => Vec::new(),
        };
        for delta in decoded_deltas {
            self.apply_recovery_delta_to_image(&mut staged, delta)?;
        }
        let merged = merged_keys.len();
        self.commit_recovery_image(staged, &merged_keys);
        Ok(merged)
    }

    /// Apply a delta atomically: changed groups replace existing state per key.
    #[cfg(feature = "cluster")]
    pub(crate) fn apply_delta(&mut self, delta: &AggVnodeDelta) -> Result<(), DbError> {
        self.apply_recovery_transaction(None, std::slice::from_ref(delta))
            .map(|_| ())
    }

    /// Replay a recovered chain into an off-side image and publish it once after every delta has
    /// succeeded. A failed or retried chain cannot double-apply its base to live state.
    #[cfg(feature = "cluster")]
    pub(crate) fn apply_vnode_chain(
        &mut self,
        base: &AggStateCheckpoint,
        deltas: &[AggVnodeDelta],
    ) -> Result<usize, DbError> {
        self.apply_recovery_transaction(Some(base), deltas)
    }

    /// Apply an authoritative FULL checkpoint transactionally. Disjoint vnode keys are inserted;
    /// overlapping keys are replaced so replay is idempotent.
    #[cfg(feature = "cluster")]
    pub(crate) fn merge_groups(
        &mut self,
        checkpoint: &AggStateCheckpoint,
    ) -> Result<usize, DbError> {
        self.apply_recovery_transaction(Some(checkpoint), &[])
    }

    /// Re-base delta tracking for vnodes this node just ACQUIRED (owned-set grew): a just-acquired
    /// vnode has no parent epoch to chain a delta onto, so its next capture must re-upload FULL.
    /// This is the only place the chain is reset on ownership change. The reset reaches a
    /// re-acquired vnode even when it has no durable state to rehydrate, which would otherwise emit
    /// a parentless delta (LDB-6025).
    ///
    /// Complements (does not duplicate) `drop_vnodes`: that handles the REVOCATION transition
    /// (clearing all state for a lost vnode); this handles the ACQUISITION transition, whose
    /// load-bearing work is the `delta_chain_len` re-base — after a restart `prev_owned` is empty, so
    /// every owned vnode reads as newly-acquired and must re-base FULL (no parent epoch here).
    #[cfg(feature = "cluster")]
    pub(crate) fn reset_acquired_vnodes(&mut self, acquired: &rustc_hash::FxHashSet<u32>) {
        if acquired.is_empty() {
            return;
        }
        for v in acquired {
            self.delta_chain_len.remove(v);
        }
    }

    /// Force every chained vnode's next delta capture to re-base FULL after a failed epoch, whose
    /// destructive capture cleared the dirty sets before durability. Re-arms emptied vnodes via
    /// `force_rebase_vnodes`; no-op when delta is off.
    #[cfg(feature = "cluster")]
    pub(crate) fn force_full_rebase(&mut self) {
        self.force_rebase_vnodes
            .extend(self.delta_chain_len.keys().copied());
        self.delta_chain_len.clear();
    }

    /// Drop in-memory state for revoked (lost) vnodes. Ownership movement is a physical state
    /// transition, not a logical relation change, so it must not emit changelog rows. Purging
    /// prevents stale keys absent from a later FULL image from surviving a re-acquire.
    #[cfg(feature = "cluster")]
    pub(crate) fn drop_vnodes(&mut self, revoked: &rustc_hash::FxHashSet<u32>, vnode_count: u32) {
        if revoked.is_empty() {
            return;
        }
        let global = self.num_group_cols == 0;
        let vnode_of = |k: &arrow::row::OwnedRow| -> u32 {
            if global {
                0
            } else {
                #[allow(clippy::cast_possible_truncation)]
                {
                    (laminar_core::state::key_hash(k.as_ref()) % u64::from(vnode_count)) as u32
                }
            }
        };
        let in_revoked = |k: &arrow::row::OwnedRow| -> bool { revoked.contains(&vnode_of(k)) };

        self.groups.retain(|k, _| !in_revoked(k));
        self.last_emitted.retain(|k, _| !in_revoked(k));
        self.dirty_keys.retain(|k| !in_revoked(k));
        for v in revoked {
            self.dirty_keys_by_vnode.remove(v);
            self.last_emitted_dirty_by_vnode.remove(v);
            self.delta_chain_len.remove(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Profiler: isolates the per-cycle cost of a non-windowed running-state aggregate at high
    // group cardinality — #1 full re-emit (`emit_running_state`) vs #2 checkpoint capture
    // (`checkpoint_groups`) vs the incremental baseline (folding ONE changed row). Run release:
    //   cargo test -p laminar-db --lib --release profile_agg_emit_vs_capture -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "profiler — run with --release --ignored --nocapture"]
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    async fn profile_agg_emit_vs_capture() {
        use std::time::Instant;

        fn pre_agg_batch(n: usize) -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ]));
            let names: Vec<String> = (0..n).map(|i| format!("g{i}")).collect();
            let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
            let vals: Vec<f64> = (0..n).map(|i| i as f64).collect();
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(arrow::array::StringArray::from(name_refs)),
                    Arc::new(arrow::array::Float64Array::from(vals)),
                ],
            )
            .unwrap()
        }

        println!("\n--- non-windowed running-state aggregate, per-cycle cost ---");
        for &n in &[10_000usize, 100_000, 1_000_000] {
            let ctx = laminar_sql::create_session_context();
            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("value", DataType::Float64, true),
            ]));
            let dummy = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec!["x"])),
                    Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                ],
            )
            .unwrap();
            let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![dummy]]).unwrap();
            ctx.register_table("events", Arc::new(mem)).unwrap();
            let mut state = IncrementalAggState::try_from_sql(
                &ctx,
                "SELECT name, SUM(value) AS total FROM events GROUP BY name",
                false, // non-windowed running-state → the full re-emit path
            )
            .await
            .unwrap()
            .expect("agg state");

            state.process_batch(&pre_agg_batch(n), i64::MIN).unwrap();
            assert_eq!(state.groups.len(), n);

            // #1 — full re-emit of all N groups (what a non-windowed running-state MV does
            // every cycle, regardless of how many groups actually changed).
            let t = Instant::now();
            let out = state.emit_running_state().unwrap();
            let emit_us = t.elapsed().as_micros();
            let emitted: usize = out.iter().map(arrow_array::RecordBatch::num_rows).sum();

            // #2 — O(groups) checkpoint capture (inline on the pipeline task).
            let t = Instant::now();
            let _cp = state.checkpoint_groups().unwrap();
            let capture_us = t.elapsed().as_micros();

            // Baseline — the real incremental work for a cycle touching ONE group.
            let t = Instant::now();
            state.process_batch(&pre_agg_batch(1), i64::MIN).unwrap();
            let process_one_us = t.elapsed().as_micros().max(1);

            println!(
                "N={n:>9}  emit={emit_us:>8}us ({:>4}ns/grp, {emitted} rows)  \
                 capture={capture_us:>8}us ({:>4}ns/grp)  process_1row={process_one_us:>4}us  \
                 emit/process1={:>6.0}x",
                (emit_us * 1000) / n as u128,
                (capture_us * 1000) / n as u128,
                emit_us as f64 / process_one_us as f64,
            );
        }
    }

    #[tokio::test]
    async fn test_try_from_sql_rejects_post_aggregate_projection() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                Arc::new(arrow::array::Float64Array::from(vec![2.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        // SUM(a)/SUM(b) collapses 2 aggregates into 1 derived column →
        // top_schema fields != agg_schema fields → should return None.
        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(a) / SUM(b) AS ratio FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap();
        assert!(
            result.is_none(),
            "Post-aggregate projection should return None"
        );
    }

    #[test]
    fn test_extract_clauses_simple() {
        let c = extract_clauses("SELECT a, SUM(b) FROM trades GROUP BY a");
        assert_eq!(c.from_clause, "trades");
        assert!(c.where_clause.is_empty());
    }

    #[test]
    fn test_extract_clauses_with_where() {
        let c = extract_clauses("SELECT * FROM events WHERE x > 1 GROUP BY y");
        assert_eq!(c.from_clause, "events");
        assert!(
            c.where_clause.contains("WHERE"),
            "should contain WHERE: {}",
            c.where_clause
        );
        assert!(
            c.where_clause.contains("x > 1"),
            "should contain predicate: {}",
            c.where_clause
        );
    }

    #[test]
    fn test_extract_clauses_with_join() {
        let c = extract_clauses("SELECT * FROM events e JOIN dim d ON e.id = d.id");
        // AST preserves join structure
        assert!(
            c.from_clause.contains("events"),
            "should contain events: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("JOIN"),
            "should contain JOIN: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("dim"),
            "should contain dim: {}",
            c.from_clause
        );
    }

    #[test]
    fn test_extract_clauses_keyword_in_string_literal() {
        // This would break heuristic extraction but works with AST
        let c =
            extract_clauses("SELECT * FROM logs WHERE msg = 'joined GROUP chat' GROUP BY user_id");
        assert_eq!(c.from_clause, "logs");
        // WHERE should include the full predicate including the string
        assert!(
            c.where_clause.contains("GROUP chat"),
            "string literal should be preserved: {}",
            c.where_clause
        );
    }

    #[test]
    fn test_extract_clauses_no_where() {
        let c = extract_clauses("SELECT * FROM events GROUP BY y");
        assert_eq!(c.from_clause, "events");
        assert!(c.where_clause.is_empty());
    }

    #[tokio::test]
    async fn test_try_from_sql_non_aggregate() {
        let ctx = laminar_sql::create_session_context();
        // Register a dummy table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let result = IncrementalAggState::try_from_sql(&ctx, "SELECT * FROM events", false)
            .await
            .unwrap();
        assert!(result.is_none(), "Non-aggregate query should return None");
    }

    #[tokio::test]
    async fn test_try_from_sql_with_group_by() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap();
        assert!(result.is_some(), "Aggregate query should return Some");
        let state = result.unwrap();
        assert_eq!(state.num_group_cols, 1);
        assert_eq!(state.agg_specs.len(), 1);
    }

    #[tokio::test]
    async fn test_incremental_aggregation_across_batches() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        // Register table for plan creation
        let dummy_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy_batch]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Simulate pre-agg output: batch 1
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch1, i64::MIN).unwrap();

        let result1 = state.emit().unwrap();
        assert_eq!(result1.len(), 1);
        assert_eq!(result1[0].num_rows(), 2); // two groups: a, b

        // Batch 2: more data for existing groups
        let batch2 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "c"])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 15.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch2, i64::MIN).unwrap();

        let result2 = state.emit().unwrap();
        assert_eq!(result2.len(), 1);
        assert_eq!(result2[0].num_rows(), 3); // three groups: a, b, c

        // Verify running totals: group "a" should have 10+30+5 = 45
        let names = result2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let totals = result2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();

        for i in 0..result2[0].num_rows() {
            match names.value(i) {
                "a" => assert!(
                    (totals.value(i) - 45.0).abs() < f64::EPSILON,
                    "Expected 45.0 for group 'a', got {}",
                    totals.value(i)
                ),
                "b" => assert!(
                    (totals.value(i) - 20.0).abs() < f64::EPSILON,
                    "Expected 20.0 for group 'b', got {}",
                    totals.value(i)
                ),
                "c" => assert!(
                    (totals.value(i) - 15.0).abs() < f64::EPSILON,
                    "Expected 15.0 for group 'c', got {}",
                    totals.value(i)
                ),
                other => panic!("Unexpected group: {other}"),
            }
        }
    }

    /// Helper: register a table and build an `IncrementalAggState` from SQL.
    async fn setup_agg_state(sql: &str) -> (SessionContext, IncrementalAggState) {
        setup_agg_state_with_changelog(sql, false).await
    }

    async fn setup_agg_state_with_changelog(
        sql: &str,
        emit_changelog: bool,
    ) -> (SessionContext, IncrementalAggState) {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();
        let state = IncrementalAggState::try_from_sql(&ctx, sql, emit_changelog)
            .await
            .unwrap()
            .expect("expected aggregate state");
        (ctx, state)
    }

    fn sum_pre_agg_batch(names: &[&str], values: &[f64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(names.to_vec())),
                Arc::new(arrow::array::Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn checkpoint_bytes(state: &mut IncrementalAggState) -> Vec<u8> {
        rkyv::to_bytes::<rkyv::rancor::Error>(&state.checkpoint_groups().unwrap())
            .unwrap()
            .to_vec()
    }

    #[tokio::test]
    async fn test_distinct_flag_extracted() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(state.agg_specs[0].distinct, "DISTINCT flag should be set");
    }

    #[tokio::test]
    async fn test_distinct_count_produces_correct_result() {
        let (_, mut state) =
            setup_agg_state("SELECT name, COUNT(DISTINCT value) as cnt FROM events GROUP BY name")
                .await;

        // Pre-agg schema: name, __agg_input_1
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Feed duplicates: value 10 appears 3 times for group "a"
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 10.0, 10.0, 20.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result.len(), 1);
        let count_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count should be Int64");
        // DISTINCT count: {10.0, 20.0} = 2
        assert_eq!(count_col.value(0), 2, "COUNT(DISTINCT) should be 2");
    }

    #[tokio::test]
    async fn test_distinct_sum_produces_correct_result() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(DISTINCT value) as total FROM events GROUP BY name")
                .await;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Feed duplicates: 10 appears twice
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 10.0, 20.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        let total_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("sum should be Float64");
        // DISTINCT sum: 10 + 20 = 30 (not 10+10+20=40)
        assert!(
            (total_col.value(0) - 30.0).abs() < f64::EPSILON,
            "SUM(DISTINCT) should be 30, got {}",
            total_col.value(0)
        );
    }

    #[tokio::test]
    async fn test_filter_clause_extracted() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(
            state.agg_specs[0].filter_col_index.is_some(),
            "FILTER clause should set filter_col_index"
        );
    }

    #[tokio::test]
    async fn test_filter_clause_applied() {
        let (_, mut state) = setup_agg_state(
            "SELECT name, SUM(value) FILTER (WHERE value > 0) as pos_sum FROM events GROUP BY name",
        )
        .await;

        // The pre-agg SQL wraps the input with CASE WHEN and adds a
        // filter boolean column. Build a batch matching that schema.
        let filter_col_idx = state.agg_specs[0]
            .filter_col_index
            .expect("filter_col_index should be set");
        let num_cols = state.num_group_cols
            + state
                .agg_specs
                .iter()
                .map(|s| s.input_col_indices.len())
                .sum::<usize>()
            + state
                .agg_specs
                .iter()
                .filter(|s| s.filter_col_index.is_some())
                .count();
        assert!(
            filter_col_idx < num_cols,
            "filter col index should be in range"
        );

        // Build pre-agg batch manually: name, CASE value, CASE filter
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
            Field::new("__agg_filter_2", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a", "a"])),
                // value > 0 wrapped: -5 becomes NULL, 10 stays, 20 stays
                Arc::new(arrow::array::Float64Array::from(vec![-5.0, 10.0, 20.0])),
                // filter mask: false, true, true
                Arc::new(arrow::array::BooleanArray::from(vec![false, true, true])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        let total_col = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("sum should be Float64");
        // Only 10 + 20 = 30 (the -5 row is filtered out)
        assert!(
            (total_col.value(0) - 30.0).abs() < f64::EPSILON,
            "SUM with FILTER should be 30, got {}",
            total_col.value(0)
        );
    }

    #[tokio::test]
    async fn test_having_clause_detected() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(value) as total FROM events GROUP BY name HAVING SUM(value) > 100",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");
        assert!(
            state.having_sql.is_some(),
            "HAVING predicate should be extracted"
        );
    }

    #[tokio::test]
    async fn test_create_accumulator_error_propagated() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Verify create_accumulator returns Ok (not panic)
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        // This should succeed without panicking
        assert!(state.process_batch(&batch, i64::MIN).is_ok());
    }

    #[tokio::test]
    async fn test_sum_int32_input_is_coerced_before_accumulator_update() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Int32, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Int32Array::from(vec![0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("orders", Arc::new(mem_table)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(amount) as total FROM orders GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        assert_eq!(
            state.agg_specs[0].input_types[0],
            DataType::Int64,
            "SUM(Int32) must feed the DataFusion Int64 accumulator"
        );
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x", "x"])),
                Arc::new(arrow::array::Int32Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let pre_agg = state
            .compiled_projection()
            .expect("single-source aggregate compiles")
            .evaluate(&input)
            .unwrap();
        assert_eq!(pre_agg.column(1).data_type(), &DataType::Int64);
        state.process_batch(&pre_agg, i64::MIN).unwrap();
        let output = state.emit().unwrap().pop().unwrap();
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .value(0),
            30,
            "coerced SUM must execute without an accumulator downcast panic"
        );
    }

    #[tokio::test]
    async fn test_avg_float32_input_is_coerced_before_accumulator_update() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("price", DataType::Float32, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float32Array::from(vec![0.0f32])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("products", Arc::new(mem_table)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, AVG(price) as avg_price FROM products GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        assert_eq!(
            state.agg_specs[0].input_types[0],
            DataType::Float64,
            "AVG(Float32) must feed the DataFusion Float64 accumulator"
        );
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x", "x"])),
                Arc::new(arrow::array::Float32Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        let pre_agg = state
            .compiled_projection()
            .expect("single-source aggregate compiles")
            .evaluate(&input)
            .unwrap();
        assert_eq!(pre_agg.column(1).data_type(), &DataType::Float64);
        state.process_batch(&pre_agg, i64::MIN).unwrap();
        let output = state.emit().unwrap().pop().unwrap();
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .unwrap()
                .value(0),
            15.0,
            "coerced AVG must execute without an accumulator downcast panic"
        );
    }

    #[tokio::test]
    async fn test_type_inference_literal_expr() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Int64Array::from(vec![0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, MIN(value) as min_val FROM events GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // Int64 in, Int64 out — should still be Int64
        assert_eq!(state.agg_specs[0].input_types[0], DataType::Int64,);
    }

    #[test]
    fn test_extract_clauses_subquery_in_where() {
        // Subquery with its own WHERE — AST handles nesting
        let c = extract_clauses(
            "SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders WHERE status = 'active') GROUP BY name",
        );
        assert_eq!(c.from_clause, "orders");
        assert!(
            c.where_clause.contains("AVG"),
            "subquery should be preserved: {}",
            c.where_clause
        );
    }

    #[test]
    fn test_expr_to_sql_column() {
        use datafusion_expr::col;
        assert_eq!(expr_to_sql(&col("price")), "\"price\"");
    }

    #[test]
    fn test_expr_to_sql_string_literal() {
        let e = datafusion_expr::Expr::Literal(ScalarValue::Utf8(Some("it's".to_string())), None);
        assert_eq!(expr_to_sql(&e), "'it''s'");
    }

    #[test]
    fn test_expr_to_sql_null_literal() {
        let e = datafusion_expr::Expr::Literal(ScalarValue::Null, None);
        assert_eq!(expr_to_sql(&e), "NULL");
    }

    #[test]
    fn test_expr_to_sql_boolean_literal() {
        let t = datafusion_expr::Expr::Literal(ScalarValue::Boolean(Some(true)), None);
        assert_eq!(expr_to_sql(&t), "TRUE");
        let f = datafusion_expr::Expr::Literal(ScalarValue::Boolean(Some(false)), None);
        assert_eq!(expr_to_sql(&f), "FALSE");
    }

    #[test]
    fn test_expr_to_sql_binary_expr() {
        use datafusion_expr::{col, lit};
        let e = col("x").gt(lit(10));
        let sql = expr_to_sql(&e);
        assert!(sql.contains("\"x\""), "should contain column: {sql}");
        assert!(sql.contains('>'), "should contain >: {sql}");
        assert!(sql.contains("10"), "should contain 10: {sql}");
    }

    #[test]
    fn test_expr_to_sql_cast() {
        use datafusion_expr::Expr;
        let e = Expr::Cast(datafusion_expr::expr::Cast {
            expr: Box::new(datafusion_expr::col("x")),
            data_type: DataType::Float64,
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("CAST"), "should contain CAST: {sql}");
        assert!(sql.contains("Float64"), "should contain target type: {sql}");
    }

    #[test]
    fn test_expr_to_sql_scalar_function() {
        use datafusion_expr::Expr;
        // Build a scalar function expr via DataFusion
        let func = datafusion::functions::string::upper();
        let e = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction {
            func,
            args: vec![datafusion_expr::col("name")],
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("upper"), "should contain function name: {sql}");
        assert!(sql.contains("\"name\""), "should contain arg: {sql}");
    }

    #[test]
    fn test_expr_to_sql_case() {
        use datafusion_expr::{col, lit};
        let e = datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
            expr: None,
            when_then_expr: vec![(Box::new(col("x").gt(lit(0))), Box::new(lit(1)))],
            else_expr: Some(Box::new(lit(0))),
        });
        let sql = expr_to_sql(&e);
        assert!(sql.starts_with("CASE"), "should start with CASE: {sql}");
        assert!(sql.contains("WHEN"), "should contain WHEN: {sql}");
        assert!(sql.contains("THEN"), "should contain THEN: {sql}");
        assert!(sql.contains("ELSE"), "should contain ELSE: {sql}");
        assert!(sql.ends_with("END"), "should end with END: {sql}");
    }

    #[test]
    fn test_expr_to_sql_not() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::Not(Box::new(col("active")));
        assert_eq!(expr_to_sql(&e), "(NOT \"active\")");
    }

    #[test]
    fn test_expr_to_sql_negative() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::Negative(Box::new(col("x")));
        assert_eq!(expr_to_sql(&e), "(-\"x\")");
    }

    #[test]
    fn test_expr_to_sql_is_null() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::IsNull(Box::new(col("x")));
        assert_eq!(expr_to_sql(&e), "(\"x\" IS NULL)");
    }

    #[test]
    fn test_expr_to_sql_is_not_null() {
        use datafusion_expr::col;
        let e = datafusion_expr::Expr::IsNotNull(Box::new(col("x")));
        assert_eq!(expr_to_sql(&e), "(\"x\" IS NOT NULL)");
    }

    #[test]
    fn test_expr_to_sql_between() {
        use datafusion_expr::{col, lit};
        let e = col("x").between(lit(1), lit(10));
        let sql = expr_to_sql(&e);
        assert!(sql.contains("BETWEEN"), "should contain BETWEEN: {sql}");
        assert!(sql.contains("AND"), "should contain AND: {sql}");
    }

    #[test]
    fn test_expr_to_sql_in_list() {
        use datafusion_expr::{col, lit};
        let e = col("status").in_list(vec![lit("a"), lit("b")], false);
        let sql = expr_to_sql(&e);
        assert!(sql.contains("IN"), "should contain IN: {sql}");
        assert!(sql.contains("'a'"), "should contain 'a': {sql}");
        assert!(sql.contains("'b'"), "should contain 'b': {sql}");
    }

    #[test]
    fn test_expr_to_sql_like() {
        use datafusion_expr::col;
        let e = col("name").like(datafusion_expr::lit("foo%"));
        let sql = expr_to_sql(&e);
        assert!(sql.contains("LIKE"), "should contain LIKE: {sql}");
        assert!(sql.contains("'foo%'"), "should contain pattern: {sql}");
    }

    #[test]
    fn test_expr_to_sql_aggregate_function() {
        // AggregateFunction in expr_to_sql is used for HAVING
        use datafusion_expr::Expr;
        let sum_udf = datafusion::functions_aggregate::sum::sum_udaf();
        let e = Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
            func: sum_udf,
            params: datafusion_expr::expr::AggregateFunctionParams {
                args: vec![datafusion_expr::col("x")],
                distinct: false,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("sum"), "should contain sum: {sql}");
        assert!(sql.contains("\"x\""), "should contain arg: {sql}");
    }

    #[test]
    fn test_expr_to_sql_aggregate_distinct() {
        use datafusion_expr::Expr;
        let count_udf = datafusion::functions_aggregate::count::count_udaf();
        let e = Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction {
            func: count_udf,
            params: datafusion_expr::expr::AggregateFunctionParams {
                args: vec![datafusion_expr::col("id")],
                distinct: true,
                filter: None,
                order_by: vec![],
                null_treatment: None,
            },
        });
        let sql = expr_to_sql(&e);
        assert!(sql.contains("DISTINCT"), "should contain DISTINCT: {sql}");
    }

    #[tokio::test]
    async fn test_group_by_expression_scalar_function() {
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["hello"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let mem_table =
            datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                .unwrap();
        ctx.register_table("events", Arc::new(mem_table)).unwrap();

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT upper(name), SUM(value) as total FROM events GROUP BY upper(name)",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // The pre-agg SQL should contain the expression, not a
        // quoted identifier
        assert!(
            state.pre_agg_sql.contains("upper("),
            "pre-agg SQL should contain expression: {}",
            state.pre_agg_sql
        );
        assert!(
            !state.pre_agg_sql.contains("\"upper("),
            "should NOT quote expression as identifier: {}",
            state.pre_agg_sql
        );
    }

    #[tokio::test]
    async fn test_group_by_simple_column_still_works() {
        let (_, state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        // Simple column ref should be a quoted identifier
        assert!(
            state.pre_agg_sql.contains("\"name\""),
            "simple column should be quoted: {}",
            state.pre_agg_sql
        );
    }

    #[tokio::test]
    async fn group_cardinality_limit_rejects_the_whole_batch_and_retry() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        state.max_groups = 2;
        state
            .process_batch(&sum_pre_agg_batch(&["a", "b"], &[10.0, 20.0]), 1)
            .unwrap();
        let before = checkpoint_bytes(&mut state);

        // Updating a while introducing c must not partially apply a before rejecting c.
        let over_limit = sum_pre_agg_batch(&["a", "c"], &[5.0, 100.0]);
        for _ in 0..2 {
            let error = state.process_batch(&over_limit, 2).unwrap_err();
            assert!(error.to_string().contains("group limit exceeded"));
            assert_eq!(checkpoint_bytes(&mut state), before);
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn replay_rejects_changelog_state_without_its_group() {
        async fn changelog_sum_state(ctx: &SessionContext) -> IncrementalAggState {
            IncrementalAggState::try_from_sql(
                ctx,
                "SELECT name, SUM(value) as total FROM events GROUP BY name",
                true,
            )
            .await
            .unwrap()
            .unwrap()
        }

        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![dummy]]).unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let input = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        let mut source = changelog_sum_state(&ctx).await;
        source.process_batch(&input, 1_000).unwrap();
        source.emit().unwrap();
        let emitted = source.checkpoint_groups().unwrap().last_emitted;
        assert!(!emitted.is_empty());

        let mut empty_checkpoint = changelog_sum_state(&ctx).await.checkpoint_groups().unwrap();
        empty_checkpoint.last_emitted = emitted;

        let mut restored = changelog_sum_state(&ctx).await;
        let error = restored
            .restore_groups(&empty_checkpoint)
            .expect_err("whole-state restore must reject orphaned changelog state");
        assert!(error.to_string().contains("non-canonical empty"));
        assert!(restored.groups.is_empty());
        assert!(restored.last_emitted.is_empty());

        let mut merged = changelog_sum_state(&ctx).await;
        let error = merged.merge_groups(&empty_checkpoint).unwrap_err();
        assert!(error.to_string().contains("non-canonical empty"));
        assert!(merged.groups.is_empty());

        let delta = AggVnodeDelta {
            changed: empty_checkpoint,
        };
        let mut applied = changelog_sum_state(&ctx).await;
        let error = applied.apply_delta(&delta).unwrap_err();
        assert!(error.to_string().contains("non-canonical empty"));
        assert!(applied.groups.is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn delta_tracking_records_dirty_keys_per_vnode_and_resets_on_capture() {
        const VNODES: u32 = 4;
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        state.set_delta_enabled(true);

        // First per-vnode capture establishes the delta baseline and starts a window.
        state.checkpoint_groups_by_vnode(VNODES).unwrap();
        assert!(state.dirty_keys_by_vnode.is_empty());

        let pre_agg = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            pre_agg,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, 1000).unwrap();

        // Every mutated key is recorded, bucketed by vnode.
        let tracked: usize = state.dirty_keys_by_vnode.values().map(|s| s.len()).sum();
        assert_eq!(tracked, 3, "all mutated keys tracked in the delta window");

        // The next capture resets the window.
        state.checkpoint_groups_by_vnode(VNODES).unwrap();
        assert!(
            state.dirty_keys_by_vnode.is_empty(),
            "capture resets the per-vnode dirty set",
        );
    }

    /// FULL base + an ordered chain of deltas, replayed via `apply_vnode_chain`, reproduces the
    /// producer exactly — and a chain re-bases to FULL once it reaches `chain_bound`.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn delta_chain_replay_reproduces_full_baseline() {
        use std::collections::BTreeMap;
        const V: u32 = 1; // single vnode → every key lands in vnode 0

        fn pre_agg_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ]))
        }
        fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
            let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
            let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
            let batch = RecordBatch::try_new(
                pre_agg_schema(),
                vec![
                    Arc::new(arrow::array::StringArray::from(names)),
                    Arc::new(arrow::array::Float64Array::from(vals)),
                ],
            )
            .unwrap();
            state.process_batch(&batch, ts).unwrap();
        }
        fn group_vals(state: &mut IncrementalAggState) -> BTreeMap<Vec<u8>, String> {
            state
                .groups
                .iter_mut()
                .map(|(k, v)| {
                    (
                        k.as_ref().to_vec(),
                        format!("{:?}", v.accs[0].evaluate().unwrap()),
                    )
                })
                .collect()
        }
        // Non-changelog agg: `checkpoint_delta_by_vnode` emits deltas (a changelog agg re-bases FULL).
        async fn agg(ctx: &SessionContext) -> IncrementalAggState {
            IncrementalAggState::try_from_sql(
                ctx,
                "SELECT name, SUM(value) as total FROM events GROUP BY name",
                false,
            )
            .await
            .unwrap()
            .unwrap()
        }
        #[allow(clippy::disallowed_types)] // matches checkpoint_delta_by_vnode's return type
        fn delta_for_vnode0(cap: std::collections::HashMap<u32, VnodeCapture>) -> AggVnodeDelta {
            match cap.into_iter().find(|(v, _)| *v == 0).map(|(_, c)| c) {
                Some(VnodeCapture::Delta(d)) => d,
                _ => panic!("expected a DELTA for vnode 0"),
            }
        }

        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let mut producer = agg(&ctx).await;
        producer.set_delta_enabled(true);

        // Epoch 0: seed a,b,c — first capture re-bases FULL and opens the delta window.
        feed(&mut producer, &[("a", 1.0), ("b", 2.0), ("c", 3.0)], 1000);
        let cap0 = producer.checkpoint_delta_by_vnode(V, 8).unwrap();
        let Some(VnodeCapture::Full(base)) =
            cap0.into_iter().find(|(v, _)| *v == 0).map(|(_, c)| c)
        else {
            panic!("first capture must be FULL");
        };

        // Epoch 1: change a → DELTA. Epoch 2: change b + add e → DELTA.
        feed(&mut producer, &[("a", 10.0)], 2000);
        let d1 = delta_for_vnode0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());
        feed(&mut producer, &[("b", 20.0), ("e", 5.0)], 3000);
        let d2 = delta_for_vnode0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());

        // Replay FULL base + ordered deltas into a fresh consumer.
        let mut consumer = agg(&ctx).await;
        consumer.apply_vnode_chain(&base, &[d1, d2]).unwrap();
        assert_eq!(
            group_vals(&mut consumer),
            group_vals(&mut producer),
            "FULL base + ordered delta chain must reproduce the producer state",
        );

        // chain_bound = 1: the chain re-bases to FULL on the next capture.
        feed(&mut producer, &[("a", 11.0)], 4000);
        let rebased = producer.checkpoint_delta_by_vnode(V, 1).unwrap();
        assert!(
            matches!(rebased.get(&0), Some(VnodeCapture::Full(_))),
            "a chain at the bound must re-base to FULL",
        );
    }

    /// `force_full_rebase` makes the next capture re-base FULL even below `chain_bound`, so a failed
    /// epoch's dirty-set clear can't silently drop its changes.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn force_full_rebase_recaptures_full_after_failed_epoch() {
        const V: u32 = 1; // single vnode → every key lands in vnode 0
        fn pre_agg_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ]))
        }
        fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
            let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
            let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
            let batch = RecordBatch::try_new(
                pre_agg_schema(),
                vec![
                    Arc::new(arrow::array::StringArray::from(names)),
                    Arc::new(arrow::array::Float64Array::from(vals)),
                ],
            )
            .unwrap();
            state.process_batch(&batch, ts).unwrap();
        }
        async fn agg(ctx: &SessionContext) -> IncrementalAggState {
            IncrementalAggState::try_from_sql(
                ctx,
                "SELECT name, SUM(value) as total FROM events GROUP BY name",
                false,
            )
            .await
            .unwrap()
            .unwrap()
        }
        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let mut producer = agg(&ctx).await;
        producer.set_delta_enabled(true);

        // Epoch 0 → FULL base; epoch 1 (well below chain_bound=8) → DELTA.
        feed(&mut producer, &[("a", 1.0), ("b", 2.0)], 1000);
        assert!(matches!(
            producer.checkpoint_delta_by_vnode(V, 8).unwrap().get(&0),
            Some(VnodeCapture::Full(_))
        ));
        feed(&mut producer, &[("a", 10.0)], 2000);
        assert!(
            matches!(
                producer.checkpoint_delta_by_vnode(V, 8).unwrap().get(&0),
                Some(VnodeCapture::Delta(_))
            ),
            "below the chain bound, a normal capture is a DELTA",
        );

        // Simulate the failed epoch's recovery hook: the next capture must re-base FULL.
        producer.force_full_rebase();
        feed(&mut producer, &[("b", 20.0)], 3000);
        assert!(
            matches!(
                producer.checkpoint_delta_by_vnode(V, 8).unwrap().get(&0),
                Some(VnodeCapture::Full(_))
            ),
            "force_full_rebase must re-base the next capture FULL, not chain a gapped delta",
        );
    }

    /// A changelog aggregate's delta chain must reproduce BOTH the group state and the
    /// `last_emitted` dedup map, so the first post-recovery emit re-emits nothing and a
    /// later change emits identically.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // one coherent replay scenario with local scaffolding
    async fn delta_chain_replay_reproduces_changelog_last_emitted() {
        use std::collections::BTreeMap;
        const V: u32 = 1; // single vnode → every key lands in vnode 0

        fn pre_agg_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ]))
        }
        fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
            let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
            let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
            let batch = RecordBatch::try_new(
                pre_agg_schema(),
                vec![
                    Arc::new(arrow::array::StringArray::from(names)),
                    Arc::new(arrow::array::Float64Array::from(vals)),
                ],
            )
            .unwrap();
            state.process_batch(&batch, ts).unwrap();
        }
        // (groups, last_emitted) as comparable string maps.
        fn snapshot(
            state: &mut IncrementalAggState,
        ) -> (BTreeMap<Vec<u8>, String>, BTreeMap<Vec<u8>, String>) {
            let groups = state
                .groups
                .iter_mut()
                .map(|(k, v)| {
                    (
                        k.as_ref().to_vec(),
                        format!("{:?}", v.accs[0].evaluate().unwrap()),
                    )
                })
                .collect();
            let emitted = state
                .last_emitted
                .iter()
                .map(|(k, v)| (k.as_ref().to_vec(), format!("{v:?}")))
                .collect();
            (groups, emitted)
        }
        async fn agg(ctx: &SessionContext) -> IncrementalAggState {
            IncrementalAggState::try_from_sql(
                ctx,
                "SELECT name, SUM(value) as total FROM events GROUP BY name",
                true, // emit_changelog
            )
            .await
            .unwrap()
            .unwrap()
        }
        #[allow(clippy::disallowed_types)]
        fn delta0(cap: std::collections::HashMap<u32, VnodeCapture>) -> AggVnodeDelta {
            match cap.into_iter().find(|(v, _)| *v == 0).map(|(_, c)| c) {
                Some(VnodeCapture::Delta(d)) => d,
                _ => panic!("expected a DELTA for vnode 0"),
            }
        }

        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let mut producer = agg(&ctx).await;
        producer.set_delta_enabled(true);

        // Epoch 0: seed + emit a,b,c, then FULL re-base (must carry last_emitted).
        feed(&mut producer, &[("a", 1.0), ("b", 2.0), ("c", 3.0)], 1000);
        producer.emit().unwrap();
        let Some(VnodeCapture::Full(base)) = producer
            .checkpoint_delta_by_vnode(V, 8)
            .unwrap()
            .into_iter()
            .find(|(v, _)| *v == 0)
            .map(|(_, c)| c)
        else {
            panic!("first capture must be FULL");
        };
        assert!(
            !base.last_emitted.is_empty(),
            "a changelog FULL re-base must carry the dedup map",
        );

        // Epoch 1: change a, emit → DELTA carries a's updated last_emitted.
        feed(&mut producer, &[("a", 10.0)], 2000);
        producer.emit().unwrap();
        let d1 = delta0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());

        // Epoch 2: change b + add d, emit → DELTA.
        feed(&mut producer, &[("b", 20.0), ("d", 4.0)], 3000);
        producer.emit().unwrap();
        let d2 = delta0(producer.checkpoint_delta_by_vnode(V, 8).unwrap());

        // Replay FULL base + ordered deltas into a fresh consumer.
        let mut consumer = agg(&ctx).await;
        consumer.set_delta_enabled(true);
        consumer.apply_vnode_chain(&base, &[d1, d2]).unwrap();

        let (pg, pe) = snapshot(&mut producer);
        let (cg, ce) = snapshot(&mut consumer);
        assert_eq!(cg, pg, "groups must match after chain replay");
        assert_eq!(
            ce, pe,
            "last_emitted dedup map must match after chain replay"
        );

        // No new input → the recovered dedup map must re-emit NOTHING (no duplicates).
        let drained: usize = consumer
            .emit()
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(
            drained, 0,
            "recovered changelog state must not re-emit unchanged groups"
        );

        // A genuine change emits identically on both.
        feed(&mut producer, &[("a", 100.0)], 4000);
        feed(&mut consumer, &[("a", 100.0)], 4000);
        let pr: usize = producer
            .emit()
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        let cr: usize = consumer
            .emit()
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(
            cr, pr,
            "post-recovery emit must produce identical changelog output"
        );
    }

    /// A global (no-GROUP-BY) changelog aggregate with delta checkpoints must capture without
    /// panicking on the empty group key (`row_to_scalar_key_with_types` on the global sentinel),
    /// and the captured slice must restore to the same value.
    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn global_changelog_delta_checkpoint_roundtrips() {
        async fn agg(ctx: &SessionContext) -> IncrementalAggState {
            IncrementalAggState::try_from_sql(ctx, "SELECT SUM(value) as total FROM events", true)
                .await
                .unwrap()
                .unwrap()
        }

        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Float64Array::from(vec![0.0]))],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let pre = Arc::new(Schema::new(vec![Field::new(
            "__agg_input_1",
            DataType::Float64,
            true,
        )]));
        let feed = |state: &mut IncrementalAggState, vals: Vec<f64>, ts: i64| {
            let batch = RecordBatch::try_new(
                Arc::clone(&pre),
                vec![Arc::new(arrow::array::Float64Array::from(vals))],
            )
            .unwrap();
            state.process_batch(&batch, ts).unwrap();
        };

        let mut state = agg(&ctx).await;
        state.set_delta_enabled(true);
        feed(&mut state, vec![1.0, 2.0, 3.0], 1000);
        state.emit().unwrap();

        // Before the fix this panicked: the empty global key hit convert_rows on a 0-field converter.
        let caps = state.checkpoint_delta_by_vnode(1, 8).unwrap();
        assert!(
            caps.contains_key(&0),
            "the global group is captured under vnode 0"
        );

        // Restore into a fresh aggregate; the single global group must total 6.0.
        let mut restored = agg(&ctx).await;
        match caps.get(&0).expect("vnode-0 capture") {
            VnodeCapture::Full(cp) => {
                restored.merge_groups(cp).unwrap();
            }
            VnodeCapture::Delta(d) => {
                restored.apply_delta(d).unwrap();
            }
        }
        let value = restored
            .groups
            .get_mut(&global_aggregate_key())
            .expect("global group restored")
            .accs[0]
            .evaluate()
            .unwrap();
        assert_eq!(value, ScalarValue::Float64(Some(6.0)));
    }

    /// `drop_vnodes` purges ALL state for a revoked vnode — resident groups, `last_emitted`, the
    /// per-vnode delta maps, and the chain length — while a sibling vnode is untouched and
    /// `last_emitted ⊆ groups` still holds. This prevents stale keys from surviving rehydration.
    #[cfg(feature = "cluster")]
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn drop_vnodes_purges_revoked_keeps_sibling() {
        use arrow::array::ArrayRef;
        const VC: u32 = 8;

        fn pre_agg_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ]))
        }
        fn feed(state: &mut IncrementalAggState, rows: &[(&str, f64)], ts: i64) {
            let names: Vec<&str> = rows.iter().map(|(n, _)| *n).collect();
            let vals: Vec<f64> = rows.iter().map(|(_, v)| *v).collect();
            let batch = RecordBatch::try_new(
                pre_agg_schema(),
                vec![
                    Arc::new(arrow::array::StringArray::from(names)),
                    Arc::new(arrow::array::Float64Array::from(vals)),
                ],
            )
            .unwrap();
            state.process_batch(&batch, ts).unwrap();
        }
        async fn agg(ctx: &SessionContext) -> IncrementalAggState {
            IncrementalAggState::try_from_sql(
                ctx,
                "SELECT name, SUM(value) as total FROM events GROUP BY name",
                true,
            )
            .await
            .unwrap()
            .unwrap()
        }

        let ctx = laminar_sql::create_session_context();
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let dummy = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["x"])),
                Arc::new(arrow::array::Float64Array::from(vec![0.0])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
            .unwrap();
        ctx.register_table("events", Arc::new(mem)).unwrap();

        let mut state = agg(&ctx).await;
        state.set_delta_enabled(true);

        let row_of = |state: &IncrementalAggState, key: &str| -> arrow::row::OwnedRow {
            let cols: Vec<ArrayRef> = vec![Arc::new(arrow::array::StringArray::from(vec![key]))];
            state
                .row_converter
                .convert_columns(&cols)
                .unwrap()
                .row(0)
                .owned()
        };
        let vnode_of = |state: &IncrementalAggState, key: &str| {
            state.delta_vnode_of(row_of(state, key).as_ref(), VC)
        };

        // A vnode `y` with two keys and a distinct vnode `x`.
        let cands: Vec<String> = (0..64).map(|i| format!("k{i}")).collect();
        let mut by_v: std::collections::BTreeMap<u32, Vec<String>> =
            std::collections::BTreeMap::new();
        for c in &cands {
            by_v.entry(vnode_of(&state, c)).or_default().push(c.clone());
        }
        let vy = *by_v
            .iter()
            .find(|(_, ks)| ks.len() >= 2)
            .map(|(v, _)| v)
            .expect("a vnode with two keys");
        let vx = *by_v
            .keys()
            .find(|v| **v != vy)
            .expect("a second distinct vnode");
        let (y_first, y_second) = (by_v[&vy][0].clone(), by_v[&vy][1].clone());
        let x_key = by_v[&vx][0].clone();

        feed(
            &mut state,
            &[(&y_first, 1.0), (&y_second, 2.0), (&x_key, 3.0)],
            1000,
        );
        state.emit().unwrap();
        let _ = state.checkpoint_delta_by_vnode(VC, 8).unwrap(); // chain_len[vx]=[vy]=0

        let y_second_row = row_of(&state, &y_second);

        // Re-dirty both vnodes so the per-vnode delta maps are populated at drop time.
        feed(&mut state, &[(&y_first, 5.0), (&x_key, 7.0)], 2000);

        let y_first_row = row_of(&state, &y_first);
        let x_row = row_of(&state, &x_key);
        assert!(
            state.groups.contains_key(&y_first_row),
            "precondition: first y group present"
        );
        assert!(
            state.groups.contains_key(&x_row),
            "precondition: x resident"
        );

        // Revoke vy.
        let revoked: rustc_hash::FxHashSet<u32> = [vy].into_iter().collect();
        state.drop_vnodes(&revoked, VC);

        // Every vy entry is gone.
        assert!(
            !state.groups.contains_key(&y_first_row),
            "revoked first group dropped"
        );
        assert!(
            !state.groups.contains_key(&y_second_row),
            "revoked second group dropped"
        );
        assert!(
            !state.last_emitted.contains_key(&y_first_row),
            "revoked last_emitted dropped"
        );
        assert!(!state.dirty_keys_by_vnode.contains_key(&vy));
        assert!(!state.last_emitted_dirty_by_vnode.contains_key(&vy));
        assert!(!state.delta_chain_len.contains_key(&vy));

        // The sibling vnode is untouched.
        assert!(
            state.groups.contains_key(&x_row),
            "sibling resident group kept"
        );
        assert!(
            state.delta_chain_len.contains_key(&vx),
            "sibling chain kept"
        );

        // Invariant preserved: the dedup map stays a subset of resident groups.
        for k in state.last_emitted.keys() {
            assert!(
                state.groups.contains_key(k),
                "last_emitted must remain a subset of groups",
            );
        }
    }

    #[tokio::test]
    async fn empty_restore_rejects_every_noncanonical_payload() {
        let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
        let (_, mut state) = setup_agg_state(sql).await;
        let empty = state.checkpoint_groups().unwrap();
        assert!(empty.last_updated_ms.is_empty());

        let mut keys_only = empty.clone();
        keys_only.keys_ipc = vec![1];
        let mut accumulators_only = empty.clone();
        accumulators_only.acc_state_ipc = vec![vec![1]];
        let mut changelog_only = empty;
        changelog_only.last_emitted = vec![EmittedCheckpoint {
            key: vec![1],
            values: vec![1],
        }];

        for checkpoint in [keys_only, accumulators_only, changelog_only] {
            let error = state.restore_groups(&checkpoint).unwrap_err();
            assert!(error.to_string().contains("non-canonical empty"));
            assert!(state.groups.is_empty());
            assert!(state.last_emitted.is_empty());
        }
    }

    #[tokio::test]
    async fn restore_rejects_malformed_changelog_values_before_mutation() {
        let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
        let (_, mut donor) = setup_agg_state_with_changelog(sql, true).await;
        donor
            .process_batch(&sum_pre_agg_batch(&["a"], &[10.0]), 1)
            .unwrap();
        donor.emit().unwrap();
        let valid = donor.checkpoint_groups().unwrap();
        assert_eq!(valid.last_emitted.len(), 1);

        let corruptions = [
            (Vec::new(), "arity mismatch"),
            (
                scalars_to_ipc(&[ScalarValue::Utf8(Some("wrong".into()))]).unwrap(),
                "type mismatch",
            ),
            (
                scalars_to_ipc(&[
                    ScalarValue::Float64(Some(10.0)),
                    ScalarValue::Float64(Some(20.0)),
                ])
                .unwrap(),
                "arity mismatch",
            ),
        ];

        for (values, expected) in corruptions {
            let (_, mut target) = setup_agg_state_with_changelog(sql, true).await;
            target
                .process_batch(&sum_pre_agg_batch(&["z"], &[9.0]), 1)
                .unwrap();
            target.emit().unwrap();
            let before = checkpoint_bytes(&mut target);

            let mut malformed = valid.clone();
            malformed.last_emitted[0].values = values;
            let error = target.restore_groups(&malformed).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
            assert_eq!(checkpoint_bytes(&mut target), before);
        }
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_replay_rejects_malformed_changelog_values_before_mutation() {
        let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
        let (_, mut donor) = setup_agg_state_with_changelog(sql, true).await;
        donor
            .process_batch(&sum_pre_agg_batch(&["a"], &[10.0]), 1)
            .unwrap();
        donor.emit().unwrap();
        let valid = donor.checkpoint_groups().unwrap();

        let (_, mut target) = setup_agg_state_with_changelog(sql, true).await;
        target
            .process_batch(&sum_pre_agg_batch(&["z"], &[9.0]), 1)
            .unwrap();
        target.emit().unwrap();
        let before = checkpoint_bytes(&mut target);

        let mut wrong_type = valid.clone();
        wrong_type.last_emitted[0].values =
            scalars_to_ipc(&[ScalarValue::Utf8(Some("wrong".into()))]).unwrap();
        let error = target.merge_groups(&wrong_type).unwrap_err();
        assert!(error.to_string().contains("type mismatch"), "{error}");
        assert_eq!(checkpoint_bytes(&mut target), before);

        let mut wrong_arity = valid;
        wrong_arity.last_emitted[0].values = scalars_to_ipc(&[
            ScalarValue::Float64(Some(10.0)),
            ScalarValue::Float64(Some(20.0)),
        ])
        .unwrap();
        let error = target
            .apply_delta(&AggVnodeDelta {
                changed: wrong_arity,
            })
            .unwrap_err();
        assert!(error.to_string().contains("arity mismatch"), "{error}");
        assert_eq!(checkpoint_bytes(&mut target), before);
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn bulk_merge_and_restore_reject_duplicate_group_keys() {
        let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
        let (_, mut donor) = setup_agg_state(sql).await;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        donor.process_batch(&batch, i64::MIN).unwrap();
        let one_group = donor.checkpoint_groups().unwrap();
        let encoded = bytes::Bytes::from(
            rkyv::to_bytes::<rkyv::rancor::Error>(&one_group)
                .unwrap()
                .to_vec(),
        );
        let error = merge_serialized_agg_cps(&[encoded.clone(), encoded]).unwrap_err();
        assert!(error.to_string().contains("not disjoint"));

        // Construct a corrupt duplicate image directly so every live restore/apply entry point is
        // still covered independently of the fail-closed bulk merge helper.
        let mut duplicated = one_group.clone();
        duplicated.keys_ipc =
            concat_columnar_ipc(&duplicated.keys_ipc, &one_group.keys_ipc).unwrap();
        for (dst, src) in duplicated
            .acc_state_ipc
            .iter_mut()
            .zip(&one_group.acc_state_ipc)
        {
            *dst = concat_columnar_ipc(dst, src).unwrap();
        }
        duplicated.last_updated_ms.extend(one_group.last_updated_ms);
        duplicated.last_emitted.extend(one_group.last_emitted);

        let (_, mut restored) = setup_agg_state(sql).await;
        let error = restored.restore_groups(&duplicated).unwrap_err();
        assert!(error.to_string().contains("duplicate group key"));
        assert!(restored.groups.is_empty());

        let (_, mut merged) = setup_agg_state(sql).await;
        let error = merged.merge_groups(&duplicated).unwrap_err();
        assert!(error.to_string().contains("duplicate group key"));
        assert!(merged.groups.is_empty());

        let (_, mut applied) = setup_agg_state(sql).await;
        let error = applied
            .apply_delta(&AggVnodeDelta {
                changed: duplicated,
            })
            .unwrap_err();
        assert!(error.to_string().contains("duplicate group key"));
        assert!(applied.groups.is_empty());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn vnode_chain_failure_is_atomic_and_successful_retry_is_idempotent() {
        let sql = "SELECT name, SUM(value) as total FROM events GROUP BY name";
        let (ctx, mut live) = setup_agg_state(sql).await;
        let batch = |name: &str, value: f64| {
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("__agg_input_1", DataType::Float64, true),
                ])),
                vec![
                    Arc::new(arrow::array::StringArray::from(vec![name])),
                    Arc::new(arrow::array::Float64Array::from(vec![value])),
                ],
            )
            .unwrap()
        };
        live.process_batch(&batch("a", 10.0), 1).unwrap();

        let mut base_donor = IncrementalAggState::try_from_sql(&ctx, sql, false)
            .await
            .unwrap()
            .unwrap();
        base_donor.process_batch(&batch("a", 1.0), 2).unwrap();
        let base = base_donor.checkpoint_groups().unwrap();

        let mut delta_donor = IncrementalAggState::try_from_sql(&ctx, sql, false)
            .await
            .unwrap()
            .unwrap();
        delta_donor.process_batch(&batch("b", 5.0), 3).unwrap();
        let valid_changed = delta_donor.checkpoint_groups().unwrap();
        let mut invalid_changed = valid_changed.clone();
        invalid_changed.last_emitted.push(EmittedCheckpoint {
            key: scalars_to_ipc(&[ScalarValue::Utf8(Some("missing".into()))]).unwrap(),
            values: scalars_to_ipc(&[ScalarValue::Float64(Some(99.0))]).unwrap(),
        });
        let invalid_late_delta = AggVnodeDelta {
            changed: invalid_changed,
        };

        let before = rkyv::to_bytes::<rkyv::rancor::Error>(&live.checkpoint_groups().unwrap())
            .unwrap()
            .to_vec();
        let error = live
            .apply_vnode_chain(&base, &[invalid_late_delta])
            .unwrap_err();
        assert!(error.to_string().contains("missing group"));
        let after = rkyv::to_bytes::<rkyv::rancor::Error>(&live.checkpoint_groups().unwrap())
            .unwrap()
            .to_vec();
        assert_eq!(
            after, before,
            "failed chain changed the live checkpoint image"
        );

        let valid_delta = AggVnodeDelta {
            changed: valid_changed,
        };
        live.apply_vnode_chain(&base, std::slice::from_ref(&valid_delta))
            .unwrap();
        let mut first_values: Vec<f64> = live
            .groups
            .values_mut()
            .map(|entry| match entry.accs[0].evaluate().unwrap() {
                ScalarValue::Float64(Some(value)) => value,
                other => panic!("unexpected aggregate value {other:?}"),
            })
            .collect();
        first_values.sort_by(f64::total_cmp);
        assert_eq!(first_values, vec![1.0, 5.0]);

        live.apply_vnode_chain(&base, std::slice::from_ref(&valid_delta))
            .unwrap();
        let mut retry_values: Vec<f64> = live
            .groups
            .values_mut()
            .map(|entry| match entry.accs[0].evaluate().unwrap() {
                ScalarValue::Float64(Some(value)) => value,
                other => panic!("unexpected aggregate value {other:?}"),
            })
            .collect();
        retry_values.sort_by(f64::total_cmp);
        assert_eq!(retry_values, first_values);
    }

    #[tokio::test]
    async fn group_cardinality_existing_groups_update_at_the_limit() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        state.max_groups = 2;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));

        // Batch 1: create 2 groups (at limit)
        let batch1 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch1, i64::MIN).unwrap();

        // Existing keys remain writable at the limit.
        let batch2 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 7.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch2, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result[0].num_rows(), 2, "still only 2 groups");

        let names = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let totals = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        for i in 0..2 {
            match names.value(i) {
                "a" => assert_eq!(totals.value(i), 15.0),
                "b" => assert_eq!(totals.value(i), 27.0),
                other => panic!("unexpected group {other}"),
            }
        }
    }

    #[test]
    fn test_extract_clauses_multiple_joins() {
        let c = extract_clauses(
            "SELECT * FROM orders o JOIN customers c ON o.cust_id = c.id JOIN products p ON o.prod_id = p.id WHERE o.amount > 100 GROUP BY c.name",
        );
        assert!(
            c.from_clause.contains("orders"),
            "should contain orders: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("customers"),
            "should contain customers: {}",
            c.from_clause
        );
        assert!(
            c.from_clause.contains("products"),
            "should contain products: {}",
            c.from_clause
        );
        assert!(
            c.where_clause.contains("100"),
            "WHERE should contain predicate: {}",
            c.where_clause
        );
    }

    #[tokio::test]
    async fn test_agg_checkpoint_roundtrip_single_group() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Feed data
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        // Checkpoint
        let cp = state.checkpoint_groups().unwrap();
        assert_eq!(cp.last_updated_ms.len(), 1);

        // Create a fresh state and restore
        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let restored = state2.restore_groups(&cp).unwrap();
        assert_eq!(restored, 1);

        // Emit and verify value matches
        let result = state2.emit().unwrap();
        assert_eq!(result.len(), 1);
        let total = result[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(
            (total.value(0) - 30.0).abs() < f64::EPSILON,
            "Restored SUM should be 30, got {}",
            total.value(0)
        );
    }

    #[tokio::test]
    async fn test_agg_checkpoint_roundtrip_multi_group() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "a", "b", "c",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 20.0, 30.0, 40.0, 50.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let cp = state.checkpoint_groups().unwrap();
        assert_eq!(cp.last_updated_ms.len(), 3);

        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let restored = state2.restore_groups(&cp).unwrap();
        assert_eq!(restored, 3);

        let result = state2.emit().unwrap();
        assert_eq!(result[0].num_rows(), 3);
    }

    /// Columnar checkpoint round-trip with a mix of accumulator shapes
    /// (SUM, COUNT(*), MAX) across several groups: restored emit must equal
    /// the original emit row-for-row.
    #[tokio::test]
    async fn test_agg_checkpoint_roundtrip_mixed_accumulators() {
        let sql = "SELECT name, SUM(value) AS s, COUNT(*) AS c, MAX(value) AS m \
                   FROM events GROUP BY name";
        let (_, mut state) = setup_agg_state(sql).await;

        // Pre-agg layout for [name] + SUM(value), COUNT(*), MAX(value):
        // group col, then __agg_input_1 (SUM), __agg_input_2 (COUNT* dummy bool), __agg_input_3 (MAX).
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
            Field::new("__agg_input_2", DataType::Boolean, true),
            Field::new("__agg_input_3", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "a", "b", "c", "a",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 20.0, 30.0, 40.0, 50.0, 5.0,
                ])),
                Arc::new(arrow::array::BooleanArray::from(vec![true; 6])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    10.0, 20.0, 30.0, 40.0, 50.0, 5.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();
        let original = state.emit().unwrap();

        let cp = state.checkpoint_groups().unwrap();
        assert_eq!(cp.last_updated_ms.len(), 3);

        let (_, mut state2) = setup_agg_state(sql).await;
        assert_eq!(state2.restore_groups(&cp).unwrap(), 3);
        let restored = state2.emit().unwrap();

        // Compare as (name -> (s, c, m)) maps so HashMap iteration order is irrelevant.
        let collect = |batches: &[RecordBatch]| {
            let mut out: std::collections::BTreeMap<String, (f64, i64, f64)> =
                std::collections::BTreeMap::new();
            for b in batches {
                let names = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let s = b
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                let c = b
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                let m = b
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::Float64Array>()
                    .unwrap();
                for i in 0..b.num_rows() {
                    out.insert(
                        names.value(i).to_string(),
                        (s.value(i), c.value(i), m.value(i)),
                    );
                }
            }
            out
        };
        assert_eq!(collect(&original), collect(&restored));
    }

    #[tokio::test]
    async fn test_restore_fingerprint_mismatch_errors() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Feed data and checkpoint
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a"])),
                Arc::new(arrow::array::Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();
        let mut cp = state.checkpoint_groups().unwrap();

        // Tamper with fingerprint
        cp.fingerprint = 999_999;

        // Restore should fail
        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let result = state2.restore_groups(&cp);
        assert!(result.is_err(), "Fingerprint mismatch should error");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("fingerprint mismatch"),
            "Error should mention fingerprint: {err}"
        );
    }

    #[tokio::test]
    async fn test_changelog_delta_emit() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(price) AS total FROM t GROUP BY symbol",
            true, // changelog mode
        )
        .await
        .unwrap()
        .unwrap();

        // Output schema should include __weight.
        assert_eq!(
            state
                .output_schema
                .field(state.output_schema.fields().len() - 1)
                .name(),
            WEIGHT_COLUMN
        );

        // Cycle 1: new data → all groups are +1 inserts.
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1.len(), 1);
        let batch1 = &r1[0];
        assert_eq!(batch1.num_rows(), 2); // AAPL +1, GOOG +1
        let w1 = batch1
            .column(batch1.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert!(w1.iter().all(|w| w == Some(1))); // all inserts

        // Cycle 2: AAPL changes, GOOG unchanged → -1 old AAPL, +1 new AAPL, GOOG skipped.
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
                Arc::new(arrow::array::Int64Array::from(vec![50])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        assert_eq!(r2.len(), 1);
        let batch2 = &r2[0];
        // Should be 2 rows: -1 (AAPL old), +1 (AAPL new). GOOG is unchanged → skipped.
        assert_eq!(batch2.num_rows(), 2);
        let w2 = batch2
            .column(batch2.num_columns() - 1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(w2.value(0), -1); // retraction
        assert_eq!(w2.value(1), 1); // insert

        // Cycle 3: no new data, nothing changed → empty output.
        let r3 = state.emit().unwrap();
        assert!(r3.is_empty() || r3.iter().all(|b| b.num_rows() == 0));
    }

    #[tokio::test]
    async fn changelog_restore_emits_no_duplicates_then_resumes() {
        // After recovery, restored groups are already reflected downstream (last_emitted
        // is restored in lockstep with groups), so the first post-restore emit must be
        // empty — re-emitting would duplicate. A later change must still emit normally.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        let sql = "SELECT symbol, SUM(price) AS total FROM t GROUP BY symbol";
        let mut state = IncrementalAggState::try_from_sql(&ctx, sql, true)
            .await
            .unwrap()
            .unwrap();

        let pre_agg = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
        ]));
        let b1 = RecordBatch::try_new(
            Arc::clone(&pre_agg),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        assert_eq!(
            state
                .emit()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        ); // AAPL +1, GOOG +1

        // Recover into a fresh state from the post-emit checkpoint.
        let cp = state.checkpoint_groups().unwrap();
        let mut restored = IncrementalAggState::try_from_sql(&ctx, sql, true)
            .await
            .unwrap()
            .unwrap();
        restored.restore_groups(&cp).unwrap();

        // First emit after restore: nothing new → empty (no duplicate inserts).
        let r0 = restored.emit().unwrap();
        assert!(
            r0.is_empty() || r0.iter().all(|b| b.num_rows() == 0),
            "restored groups must not be re-emitted"
        );

        // A real change resumes normally: AAPL 100 -> 150 emits retract + insert.
        let b2 = RecordBatch::try_new(
            pre_agg,
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL"])),
                Arc::new(arrow::array::Int64Array::from(vec![50])),
            ],
        )
        .unwrap();
        restored.process_batch(&b2, 2000).unwrap();
        assert_eq!(
            restored
                .emit()
                .unwrap()
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2,
            "post-restore change must emit retract+insert"
        );
    }

    #[tokio::test]
    async fn test_cascaded_agg_retract_batch() {
        // Simulate a downstream aggregate consuming upstream changelog output
        // with a __weight column. Negative weights should trigger retract_batch.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // weight_col_idx should be detected from upstream schema.
        assert!(state.weight_col_idx.is_some());

        // Cycle 1: insert AAPL=100 (+1), GOOG=200 (+1).
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("total", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 200])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1[0].num_rows(), 2);

        // Cycle 2: retract AAPL=100 (-1), insert AAPL=150 (+1). GOOG unchanged.
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("symbol", DataType::Utf8, true),
                Field::new("total", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["AAPL", "AAPL"])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 150])),
                Arc::new(arrow::array::Int64Array::from(vec![-1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        // AAPL: was 100, retracted 100, added 150 → SUM=150. GOOG: still 200.
        assert_eq!(r2[0].num_rows(), 2);
        let totals = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let symbols = r2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..r2[0].num_rows() {
            match symbols.value(i) {
                "AAPL" => assert_eq!(totals.value(i), 150),
                "GOOG" => assert_eq!(totals.value(i), 200),
                other => panic!("unexpected symbol: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn test_min_accepted_over_changelog_upstream() {
        // MIN is now supported over changelog streams via retractable accumulators.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, MIN(price) AS low FROM upstream GROUP BY symbol",
            false,
        )
        .await;
        assert!(result.is_ok(), "MIN should be accepted over changelog");
    }

    #[tokio::test]
    async fn test_unsupported_agg_rejected_over_changelog() {
        // STDDEV is NOT supported over changelog streams.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Float64Array::from(vec![1.0])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, STDDEV(price) AS sd FROM upstream GROUP BY symbol",
            false,
        )
        .await;
        match result {
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("Cannot compute"), "got: {msg}");
            }
            Ok(_) => panic!("expected error for STDDEV over changelog upstream"),
        }
    }

    #[tokio::test]
    async fn test_cascaded_count_star_over_changelog() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(state.weight_col_idx.is_some());

        // Cycle 1: insert 3 rows.
        // Pre-agg schema for COUNT(*): [region, TRUE (dummy bool), __weight].
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Boolean, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "EU"])),
                Arc::new(arrow::array::BooleanArray::from(vec![true, true, true])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1[0].num_rows(), 2);

        // Cycle 2: retract one US row
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Boolean, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::BooleanArray::from(vec![true])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let counts = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let regions = r2[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        for i in 0..r2[0].num_rows() {
            match regions.value(i) {
                "US" => assert_eq!(counts.value(i), 1, "US count should be 1 after retraction"),
                "EU" => assert_eq!(counts.value(i), 1, "EU count should remain 1"),
                other => panic!("unexpected region: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn changelog_retractable_survives_checkpoint() {
        // checkpoint_groups() rebuilds each live accumulator from its snapshot.
        // For a changelog (`__weight`) aggregate the live accumulator is the
        // retractable variant; rebuilding it as a plain one would silently drop
        // retraction. Prove a retract still works *after* a mid-stream checkpoint.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, COUNT(*) AS cnt FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(state.weight_col_idx.is_some());

        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Boolean, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let mk = |regions: Vec<&str>, weights: Vec<i64>| {
            let n = regions.len();
            RecordBatch::try_new(
                Arc::clone(&pre_agg_schema),
                vec![
                    Arc::new(arrow::array::StringArray::from(regions)),
                    Arc::new(arrow::array::BooleanArray::from(vec![true; n])),
                    Arc::new(arrow::array::Int64Array::from(weights)),
                ],
            )
            .unwrap()
        };

        state
            .process_batch(&mk(vec!["US", "US", "EU"], vec![1, 1, 1]), 1000)
            .unwrap();
        let _ = state.emit().unwrap();

        // Mid-stream checkpoint — must keep the live accumulators retractable.
        let _ = state.checkpoint_groups().unwrap();

        // Retract one US row; a downgraded plain accumulator could not.
        state
            .process_batch(&mk(vec!["US"], vec![-1]), 2000)
            .unwrap();
        let r = state.emit().unwrap();
        let regions = r[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let counts = r[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for i in 0..r[0].num_rows() {
            match regions.value(i) {
                "US" => assert_eq!(counts.value(i), 1, "US count must be 1 after retract"),
                "EU" => assert_eq!(counts.value(i), 1),
                other => panic!("unexpected region: {other}"),
            }
        }
    }

    #[tokio::test]
    async fn test_cascaded_avg_over_changelog() {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, AVG(price) AS avg_price FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Insert: 10, 20, 30 for "US" -> avg = 20
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        let avg = r1[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((avg.value(0) - 20.0).abs() < 0.001, "avg should be 20.0");

        // Retract 10 -> {20, 30} -> avg = 25
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let avg2 = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!(
            (avg2.value(0) - 25.0).abs() < 0.001,
            "avg should be 25.0 after retraction"
        );
    }

    #[tokio::test]
    async fn test_cascaded_min_over_changelog() {
        // Single MIN aggregate — pre-agg schema: [region, price, __weight]
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, MIN(price) AS lo FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Insert 10, 20, 30
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        let mins = r1[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(mins.value(0), 10);

        // Retract current min (10) -> new min = 20
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let mins2 = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(mins2.value(0), 20, "min should be 20 after retracting 10");

        // Retract 20, retract 30 -> empty -> NULL
        let b3 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![-1, -1])),
            ],
        )
        .unwrap();
        state.process_batch(&b3, 3000).unwrap();
        let r3 = state.emit().unwrap();
        assert!(
            r3[0].column(1).is_null(0),
            "min should be NULL after all values retracted"
        );
    }

    #[tokio::test]
    async fn test_cascaded_max_retract_over_changelog() {
        // Single MAX aggregate — pre-agg schema: [region, price, __weight]
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let mut state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, MAX(price) AS hi FROM upstream GROUP BY region",
            false,
        )
        .await
        .unwrap()
        .unwrap();

        // Insert 10, 20, 30
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])),
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        let maxs = r1[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(maxs.value(0), 30);

        // Retract current max (30) -> new max = 20
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("price", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US"])),
                Arc::new(arrow::array::Int64Array::from(vec![30])),
                Arc::new(arrow::array::Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        let maxs2 = r2[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(maxs2.value(0), 20, "max should be 20 after retracting 30");
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_cascaded_mixed_aggregates_over_changelog() {
        // Mixed: SUM + COUNT(*) + AVG + MIN + MAX on same column.
        // Pre-agg schema: [region, amount(SUM), TRUE(COUNT), amount(AVG),
        //                   amount(MIN), amount(MAX), __weight] = 7 columns.
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["X"])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();

        let result = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT region, SUM(amount) AS total, COUNT(*) AS cnt, \
             AVG(amount) AS avg_amt, MIN(amount) AS lo, MAX(amount) AS hi \
             FROM upstream GROUP BY region",
            false,
        )
        .await;
        assert!(result.is_ok(), "mixed aggregates should be accepted");
        let mut state = result.unwrap().unwrap();

        // Pre-agg has 7 cols: [region, amt, TRUE, amt, amt, amt, __weight].
        // Build matching batch.
        let b1 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Int64, true),
                Field::new("__agg_input_2", DataType::Boolean, true),
                Field::new("__agg_input_3", DataType::Int64, true),
                Field::new("__agg_input_4", DataType::Int64, true),
                Field::new("__agg_input_5", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // SUM input
                Arc::new(arrow::array::BooleanArray::from(vec![true, true, true])), // COUNT(*)
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // AVG input
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // MIN input
                Arc::new(arrow::array::Int64Array::from(vec![10, 20, 30])), // MAX input
                Arc::new(arrow::array::Int64Array::from(vec![1, 1, 1])),    // weight
            ],
        )
        .unwrap();
        state.process_batch(&b1, 1000).unwrap();
        let r1 = state.emit().unwrap();
        assert_eq!(r1[0].num_rows(), 1);

        // Retract 10, insert 40.
        let b2 = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("region", DataType::Utf8, true),
                Field::new("__agg_input_1", DataType::Int64, true),
                Field::new("__agg_input_2", DataType::Boolean, true),
                Field::new("__agg_input_3", DataType::Int64, true),
                Field::new("__agg_input_4", DataType::Int64, true),
                Field::new("__agg_input_5", DataType::Int64, true),
                Field::new(WEIGHT_COLUMN, DataType::Int64, false),
            ])),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["US", "US"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::BooleanArray::from(vec![true, true])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 40])),
                Arc::new(arrow::array::Int64Array::from(vec![-1, 1])),
            ],
        )
        .unwrap();
        state.process_batch(&b2, 2000).unwrap();
        let r2 = state.emit().unwrap();
        // {20, 30, 40}: SUM=90, COUNT=3, AVG=30, MIN=20, MAX=40
        let b = &r2[0];
        let sum_col = b
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let cnt_col = b
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let avg_col = b
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        let min_col = b
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let max_col = b
            .column(5)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(sum_col.value(0), 90, "SUM should be 90");
        assert_eq!(cnt_col.value(0), 3, "COUNT should be 3");
        assert!((avg_col.value(0) - 30.0).abs() < 0.001, "AVG should be 30");
        assert_eq!(min_col.value(0), 20, "MIN should be 20");
        assert_eq!(max_col.value(0), 40, "MAX should be 40");
    }

    fn round_trip(sv: &ScalarValue) -> ScalarValue {
        let bytes = scalars_to_ipc(std::slice::from_ref(sv)).unwrap();
        let back = ipc_to_scalars(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        back.into_iter().next().unwrap()
    }

    #[test]
    fn scalar_ipc_round_trip() {
        // Arrow IPC preserves exact type — no widening, unlike the old JSON path.
        assert_eq!(round_trip(&ScalarValue::Null), ScalarValue::Null);
        assert_eq!(
            round_trip(&ScalarValue::Boolean(Some(true))),
            ScalarValue::Boolean(Some(true)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Int64(Some(-42))),
            ScalarValue::Int64(Some(-42)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Float64(Some(2.72))),
            ScalarValue::Float64(Some(2.72)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Utf8(Some("hello".into()))),
            ScalarValue::Utf8(Some("hello".into())),
        );
        let tz: Option<Arc<str>> = Some(Arc::from("UTC"));
        assert_eq!(
            round_trip(&ScalarValue::TimestampNanosecond(
                Some(1_000_000),
                tz.clone()
            )),
            ScalarValue::TimestampNanosecond(Some(1_000_000), tz),
        );
        assert_eq!(
            round_trip(&ScalarValue::Date32(Some(19000))),
            ScalarValue::Date32(Some(19000)),
        );
        assert_eq!(
            round_trip(&ScalarValue::Date64(Some(1_700_000_000_000))),
            ScalarValue::Date64(Some(1_700_000_000_000)),
        );
    }

    #[test]
    fn binary_scalar_roundtrips_exactly() {
        // Under the old serde_json path, Binary was string-coerced via the
        // "STR" fallback. Arrow IPC preserves Binary natively.
        let sv = ScalarValue::Binary(Some(vec![1, 2, 3]));
        assert_eq!(round_trip(&sv), sv);
    }

    /// Profiling (not a correctness test): measures the on-task whole-node
    /// `checkpoint_groups` capture cost vs group count — the cost an incremental
    /// (dirty-only) capture would shrink. Reports total time, ns/group,
    /// and serialized size, so the incremental win for a given dirty ratio is
    /// `ns/group * dirty_count`. `#[ignore]`d; run in release:
    /// `cargo test -p laminar-db --release profile_checkpoint_capture -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "profiling; run with --release --ignored --nocapture"]
    async fn profile_checkpoint_capture_cost() {
        for &n in &[10_000usize, 100_000, 1_000_000] {
            let ctx = SessionContext::new();
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Float64, false),
            ]));
            let dummy = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(arrow::array::Int64Array::from(vec![0i64])),
                    Arc::new(arrow::array::Float64Array::from(vec![0.0])),
                ],
            )
            .unwrap();
            let mem =
                datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![dummy]])
                    .unwrap();
            ctx.register_table("events", Arc::new(mem)).unwrap();
            let mut state = IncrementalAggState::try_from_sql(
                &ctx,
                "SELECT id, SUM(value) AS total FROM events GROUP BY id",
                false,
            )
            .await
            .unwrap()
            .unwrap();

            let pre = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("__agg_input_1", DataType::Float64, true),
            ]));
            #[allow(clippy::cast_precision_loss, clippy::cast_possible_wrap)]
            let batch = RecordBatch::try_new(
                pre,
                vec![
                    Arc::new(arrow::array::Int64Array::from(
                        (0..n as i64).collect::<Vec<_>>(),
                    )),
                    Arc::new(arrow::array::Float64Array::from(
                        (0..n).map(|i| i as f64).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            state.process_batch(&batch, 0).unwrap();

            let t0 = std::time::Instant::now();
            let cp = state.checkpoint_groups().unwrap();
            let elapsed = t0.elapsed();

            let bytes: usize = cp.keys_ipc.len()
                + cp.acc_state_ipc.iter().map(Vec::len).sum::<usize>()
                + cp.last_updated_ms.len() * 8;
            #[allow(clippy::cast_precision_loss)]
            let ns_per_group = elapsed.as_nanos() as f64 / n as f64;
            println!(
                "checkpoint_groups: {n:>9} groups -> {elapsed:>11.2?}  ({ns_per_group:6.0} ns/group)  ~{} KiB",
                bytes / 1024
            );
            assert_eq!(cp.last_updated_ms.len(), n);
        }
    }
}

/// Per-vnode checkpoint partitioning + merge-apply (the cross-node vnode
/// rehydration round-trip). Gated to cluster builds since that's where the
/// new methods compile.
#[cfg(all(test, feature = "cluster"))]
mod vnode_partition_tests {
    use super::*;

    const VNODES: u32 = 16;

    fn pre_agg_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new("total", DataType::Int64, true),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]))
    }

    async fn fresh_state() -> IncrementalAggState {
        let ctx = laminar_sql::create_session_context();
        // The seed row is for schema inference only — `try_from_sql` plans the
        // query, it does not fold table rows into the accumulators.
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
            Field::new(WEIGHT_COLUMN, DataType::Int64, false),
        ]));
        let seed = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["seed"])),
                Arc::new(arrow::array::Int64Array::from(vec![0])),
                Arc::new(arrow::array::Int64Array::from(vec![1])),
            ],
        )
        .unwrap();
        let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![seed]]).unwrap();
        ctx.register_table("upstream", Arc::new(mem)).unwrap();
        IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT symbol, SUM(total) AS grand_total FROM upstream GROUP BY symbol",
            false,
        )
        .await
        .unwrap()
        .unwrap()
    }

    fn feed(state: &mut IncrementalAggState, rows: &[(&str, i64)]) {
        let syms: Vec<&str> = rows.iter().map(|(s, _)| *s).collect();
        let tots: Vec<i64> = rows.iter().map(|(_, t)| *t).collect();
        let n = rows.len();
        let batch = RecordBatch::try_new(
            pre_agg_schema(),
            vec![
                Arc::new(arrow::array::StringArray::from(syms)),
                Arc::new(arrow::array::Int64Array::from(tots)),
                Arc::new(arrow::array::Int64Array::from(vec![1i64; n])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, 1000).unwrap();
    }

    fn totals(state: &mut IncrementalAggState) -> std::collections::BTreeMap<String, i64> {
        let mut out = std::collections::BTreeMap::new();
        for b in state.emit().unwrap() {
            let syms = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let tots = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                out.insert(syms.value(i).to_string(), tots.value(i));
            }
        }
        out
    }

    #[tokio::test]
    async fn per_vnode_checkpoint_merge_round_trips() {
        let mut a = fresh_state().await;
        feed(
            &mut a,
            &[
                ("AAPL", 100),
                ("GOOG", 200),
                ("MSFT", 50),
                ("AMZN", 75),
                ("META", 25),
                ("NVDA", 10),
            ],
        );

        // Partition by vnode, and the full single-blob checkpoint as a baseline.
        let by_vnode = a.checkpoint_groups_by_vnode(VNODES).unwrap();
        let full = a.checkpoint_groups().unwrap();

        // Every group lands in exactly one vnode slice — union == the whole.
        let partitioned: usize = by_vnode.values().map(|cp| cp.last_updated_ms.len()).sum();
        assert_eq!(
            partitioned,
            full.last_updated_ms.len(),
            "per-vnode slices must cover every group exactly once",
        );

        // Reassemble on a fresh node by merging each vnode's slice; the
        // aggregated output must match the original.
        let mut b = fresh_state().await;
        for slice in by_vnode.values() {
            b.merge_groups(slice).unwrap();
        }
        assert_eq!(
            totals(&mut b),
            totals(&mut a),
            "merging the per-vnode slices reproduces the original aggregate",
        );
    }

    #[tokio::test]
    async fn full_replay_replaces_preexisting_rows_and_is_idempotent() {
        // Rehydration is fenced ahead of new owner processing. The committed FULL image is
        // authoritative, including when the same chain is delivered again after a lost ack.
        let mut donor = fresh_state().await;
        feed(&mut donor, &[("AAPL", 100), ("GOOG", 200)]);
        let by_vnode = donor.checkpoint_groups_by_vnode(VNODES).unwrap();

        let mut acquirer = fresh_state().await;
        feed(&mut acquirer, &[("AAPL", 5), ("GOOG", 5)]);
        for slice in by_vnode.values() {
            acquirer.merge_groups(slice).unwrap();
        }

        let first = totals(&mut acquirer);
        assert_eq!(first.get("AAPL"), Some(&100));
        assert_eq!(first.get("GOOG"), Some(&200));

        for slice in by_vnode.values() {
            acquirer.merge_groups(slice).unwrap();
        }
        assert_eq!(totals(&mut acquirer), first);
    }
}
