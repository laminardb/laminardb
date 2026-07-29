//! Incremental aggregation state for streaming GROUP BY queries.
//!
//! One `IncrementalAggState` per pipeline; one `DataFusion` `Accumulator` per
//! aggregate per group. Cross-vnode partial merges live in
//! `laminar_core::state::partial_aggregate` and are a separate concern.

use std::num::NonZeroU32;
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
use laminar_core::state::{KeyGroupCount, PartitionKeyCodecV1};

use crate::db::exact_table_reference;
use crate::error::DbError;

mod artifact_v1;
mod checkpoints;
mod compile;
mod keys;
mod managed_v1;
mod scalar_ipc;
mod vnode_state;
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
use vnode_state::AggregateVnodeSlots;
#[cfg(feature = "cluster")]
use vnode_state::AggregateVnodeState;

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
    #[cfg(test)]
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

#[cfg(all(feature = "cluster", test))]
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

#[cfg(all(feature = "cluster", test))]
pub(crate) fn validate_agg_checkpoint_slice(
    checkpoint: &AggStateCheckpoint,
) -> Result<(), DbError> {
    validate_checkpoint_layout_and_keys(checkpoint, "vnode restore").map(|_| ())
}

/// Merge serialized aggregate slices over disjoint keys into one checkpoint.
#[cfg(all(feature = "cluster", test))]
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

#[cfg(all(feature = "cluster", test))]
fn decode_columnar_stream(bytes: &[u8], label: &str) -> Result<(RecordBatch, usize), DbError> {
    let batch = laminar_core::serialization::deserialize_batch_stream(bytes)
        .map_err(|e| DbError::Pipeline(format!("merge agg {label} decode: {e}")))?;
    let rows = batch.num_rows();
    Ok((batch, rows))
}

#[cfg(all(feature = "cluster", test))]
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

pub(crate) struct IncrementalAggState {
    query_sql: String,
    #[cfg(test)]
    pre_agg_sql: String,
    num_group_cols: usize,
    key_group_count: KeyGroupCount,
    group_types: Vec<DataType>,
    agg_specs: Vec<AggFuncSpec>,
    vnode_states: AggregateVnodeSlots,
    row_converter: arrow::row::RowConverter,
    output_schema: SchemaRef,
    compiled_projection: Option<CompiledProjection>,
    cached_pre_agg_physical: Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>>,
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    having_sql: Option<String>,
    max_groups: usize,
    emit_changelog: bool,
    weight_col_idx: Option<usize>,
    #[cfg(feature = "cluster")]
    delta_enabled: bool,
    delta_tracking_active: bool,
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
    #[cfg(feature = "cluster")]
    pub(crate) delta_chain_len: std::collections::BTreeMap<u32, u32>,
    #[cfg(feature = "cluster")]
    pub(crate) force_full_rebase_vnodes: std::collections::BTreeSet<u32>,
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

    #[cfg(test)]
    pub(crate) fn set_max_groups_for_test(&mut self, max_groups: usize) {
        self.max_groups = max_groups;
    }

    #[cfg(test)]
    pub(crate) fn logical_group_count_for_test(&self) -> usize {
        self.vnode_states.resident_group_count()
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn contains_group_for_test(&self, key: &arrow::row::OwnedRow) -> bool {
        let vnode = Self::vnode_for_group_key(self.num_group_cols, key, self.routing_vnode_count());
        self.vnode_states
            .get(vnode)
            .is_some_and(|state| state.groups.contains_key(key))
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn group_keys_for_test(&self) -> Vec<arrow::row::OwnedRow> {
        self.vnode_states
            .iter()
            .flat_map(|(_, state)| state.groups.keys().cloned())
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn vnode_slot_identity_for_test(&self, vnode: u32) -> Option<*const ()> {
        self.vnode_states
            .get(vnode)
            .map(|state| std::ptr::from_ref(state).cast())
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
            #[cfg(feature = "cluster")]
            delta_chain_len: std::collections::BTreeMap::new(),
            #[cfg(feature = "cluster")]
            force_full_rebase_vnodes: std::collections::BTreeSet::new(),
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
            #[cfg(feature = "cluster")]
            if let Some(chain_len) = state.delta_chain_len {
                snapshot.delta_chain_len.insert(vnode, chain_len);
            }
            #[cfg(feature = "cluster")]
            if state.force_full_rebase {
                snapshot.force_full_rebase_vnodes.insert(vnode);
            }
        }
        snapshot
    }

    #[cfg(all(test, feature = "cluster"))]
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
}

/// A per-vnode state delta: the groups changed since the chain base, columnar with the same shape
/// as a `FULL` slice.
#[cfg(feature = "cluster")]
pub(crate) struct AggVnodeDelta {
    pub(crate) changed: AggStateCheckpoint,
}

/// One decoded authoritative vnode chain supplied to an aggregate transition preparation.
///
/// The graph owns the serialized recovery artifacts. `SqlQuery` decodes them once, then lends the
/// complete operator roster here so every vnode is validated and staged before publication.
#[cfg(feature = "cluster")]
pub(crate) struct AggVnodeRestore<'a> {
    pub(crate) vnode: u32,
    pub(crate) base: &'a AggStateCheckpoint,
    pub(crate) deltas: &'a [AggVnodeDelta],
}

/// Fully decoded aggregate transition containing one replacement decision per transitioned vnode.
#[cfg(feature = "cluster")]
pub(crate) struct PreparedAggVnodeTransition {
    replacements: Vec<(u32, Option<Box<AggregateVnodeState>>)>,
    final_active_vnodes: Vec<u32>,
    final_group_count: usize,
}

/// State and staging allocations displaced by one published aggregate transition.
///
/// Destruction can release accumulator- and checkpoint-owned allocations, so publication returns
/// this opaque owner and the graph drops it only after leaving the publication section.
#[cfg(feature = "cluster")]
pub(crate) struct RetiredAggVnodeTransition {
    _retired_state: PreparedAggVnodeTransition,
}

#[cfg(feature = "cluster")]
fn checked_vnode_transition_capacity(left: usize, right: usize) -> Result<usize, DbError> {
    left.checked_add(right)
        .ok_or_else(|| DbError::Pipeline("aggregate vnode transition capacity overflow".into()))
}

#[cfg(feature = "cluster")]
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
        let vnode_states = AggregateVnodeSlots::try_new(key_group_count)?;

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
            having_sql,
            max_groups: 1_000_000,
            emit_changelog,
            weight_col_idx,
            #[cfg(feature = "cluster")]
            delta_enabled: false,
            delta_tracking_active: false,
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
    #[cfg(feature = "cluster")]
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

    /// Commit a successfully built changelog insertion to the dedup map and, once delta tracking
    /// has a baseline, mark that entry for checkpointing.
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
        if self.delta_tracking_active {
            state.last_emitted_dirty_keys.insert(key.clone());
        }
        state.last_emitted.insert(key, values);
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
        let delta_tracking_active = self.delta_tracking_active;
        for (vnode, owned_key, indices) in grouped_rows {
            let key;
            let (inserted, update_result) = {
                let vnode_state = self.vnode_states.get_or_insert(vnode);
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
                        (
                            e.insert(GroupEntry {
                                accs,
                                last_updated_ms: watermark_ms,
                            }),
                            true,
                        )
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
                if update_result.is_ok() {
                    if emit_changelog {
                        vnode_state.emit_dirty_keys.insert(key.clone());
                    }
                    if delta_tracking_active {
                        vnode_state.checkpoint_dirty_keys.insert(key.clone());
                    }
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
            self.vnode_states.get_or_insert(0).groups.insert(
                empty_key.clone(),
                GroupEntry {
                    accs,
                    last_updated_ms: watermark_ms,
                },
            );
            self.vnode_states.increment_resident_groups();
        }
        #[allow(clippy::cast_possible_truncation)]
        let all_indices: Vec<u32> = (0..batch.num_rows() as u32).collect();
        let vnode_state = self.vnode_states.get_mut(0).unwrap();
        let entry = vnode_state.groups.get_mut(&empty_key).unwrap();
        entry.last_updated_ms = watermark_ms;
        let res = Self::update_group_accumulators(
            &mut entry.accs,
            batch,
            &all_indices,
            &self.agg_specs,
            self.weight_col_idx,
        );
        if self.delta_tracking_active {
            vnode_state.checkpoint_dirty_keys.insert(empty_key.clone());
        }
        if self.emit_changelog {
            vnode_state.emit_dirty_keys.insert(empty_key);
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
            let mut scalars: Vec<ScalarValue> = Vec::with_capacity(num_rows);
            for (_, state) in self.vnode_states.iter_mut() {
                for entry in state.groups.values_mut() {
                    let sv = entry.accs[agg_idx]
                        .evaluate()
                        .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;
                    scalars.push(sv);
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
        dirty_by_vnode: Vec<(u32, AHashSet<arrow::row::OwnedRow>)>,
        emission_succeeded: bool,
    ) {
        for (vnode, mut dirty) in dirty_by_vnode {
            // A successful emission clears the retained set while preserving its allocation.
            if emission_succeeded {
                dirty.clear();
            }
            self.vnode_states
                .get_mut(vnode)
                .expect("emitting aggregate vnode must remain resident")
                .emit_dirty_keys = dirty;
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

    /// Canonical semantic EMPTY image for this exact aggregate plan.
    #[cfg(feature = "cluster")]
    pub(crate) fn empty_checkpoint(&self) -> AggStateCheckpoint {
        AggStateCheckpoint {
            fingerprint: self.query_fingerprint(),
            keys_ipc: Vec::new(),
            acc_state_ipc: Vec::new(),
            last_updated_ms: Vec::new(),
            last_emitted: Vec::new(),
        }
    }

    pub(crate) fn checkpoint_groups(&mut self) -> Result<AggStateCheckpoint, DbError> {
        let fingerprint = self.query_fingerprint();
        let retractable = self.weight_col_idx.is_some();
        let mut entries: Vec<(arrow::row::OwnedRow, &mut GroupEntry)> = self
            .vnode_states
            .iter_mut()
            .flat_map(|(_, state)| state.groups.iter_mut())
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
        let emitted_count = self
            .vnode_states
            .iter()
            .map(|(_, state)| state.last_emitted.len())
            .sum();
        let mut out = Vec::with_capacity(emitted_count);
        for (_, state) in self.vnode_states.iter() {
            for (row_key, vals) in &state.last_emitted {
                let sv_key =
                    row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
                out.push(EmittedCheckpoint {
                    key: scalars_to_ipc(&sv_key)?,
                    values: scalars_to_ipc(vals)?,
                });
            }
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
        let vnode_count = self.routing_vnode_count();
        let mut new_vnode_states = AggregateVnodeSlots::try_new(self.key_group_count)?;
        for g in decoded {
            let vnode = Self::vnode_for_group_key(self.num_group_cols, &g.row_key, vnode_count);
            match new_vnode_states
                .get_or_insert(vnode)
                .groups
                .entry(g.row_key)
            {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(GroupEntry {
                        accs: g.accs,
                        last_updated_ms: g.last_updated_ms,
                    });
                    new_vnode_states.increment_resident_groups();
                }
                std::collections::hash_map::Entry::Occupied(_) => {
                    return Err(DbError::Pipeline(
                        "aggregate checkpoint contains a duplicate group key".into(),
                    ));
                }
            }
        }

        for (row_key, values) in new_last_emitted {
            let vnode = Self::vnode_for_group_key(self.num_group_cols, &row_key, vnode_count);
            let state = new_vnode_states.get(vnode).ok_or_else(|| {
                DbError::Pipeline(
                    "aggregate checkpoint contains changelog state for a missing group".into(),
                )
            })?;
            if !state.groups.contains_key(&row_key) {
                return Err(DbError::Pipeline(
                    "aggregate checkpoint contains changelog state for a missing group".into(),
                ));
            }
            new_vnode_states
                .get_mut(vnode)
                .expect("validated aggregate vnode must remain present")
                .last_emitted
                .insert(row_key, values);
        }

        self.vnode_states = new_vnode_states;
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
    pub(crate) fn checkpoint_groups_by_vnode(
        &mut self,
        vnode_count: u32,
    ) -> Result<std::collections::HashMap<u32, AggStateCheckpoint>, DbError> {
        self.validate_vnode_count(vnode_count)?;
        let fingerprint = self.query_fingerprint();
        let retractable = self.weight_col_idx.is_some();

        let mut buckets: std::collections::HashMap<u32, AggStateCheckpoint> =
            std::collections::HashMap::new();
        for (vnode, state) in self.vnode_states.iter_mut() {
            if state.groups.is_empty() {
                continue;
            }
            let mut entries = state
                .groups
                .iter_mut()
                .map(|(key, entry)| (key.clone(), entry))
                .collect::<Vec<_>>();
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
            for (vnode, state) in self.vnode_states.iter() {
                for (row_key, vals) in &state.last_emitted {
                    let sv_key = row_to_scalar_key_with_types(
                        &self.row_converter,
                        row_key,
                        &self.group_types,
                    )?;
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
        }

        // Delta tracking re-bases on this capture: the dirty sets reset, so the next
        // checkpoint's delta is measured against the state staged here.
        if self.delta_enabled {
            self.delta_tracking_active = true;
            for (_, state) in self.vnode_states.iter_mut() {
                state.checkpoint_dirty_keys.clear();
                state.last_emitted_dirty_keys.clear();
            }
        }

        Ok(buckets)
    }

    #[cfg(feature = "cluster")]
    fn checkpoint_full_vnode(
        &mut self,
        vnode: u32,
        vnode_count: NonZeroU32,
        fingerprint: u64,
        retractable: bool,
    ) -> Result<VnodeCapture, DbError> {
        debug_assert!(vnode < vnode_count.get());
        let mut entries: Vec<(arrow::row::OwnedRow, &mut GroupEntry)> = self
            .vnode_states
            .get_mut(vnode)
            .map(|state| {
                state
                    .groups
                    .iter_mut()
                    .map(|(key, entry)| (key.clone(), entry))
                    .collect()
            })
            .unwrap_or_default();
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
    pub(crate) fn checkpoint_delta_by_vnode(
        &mut self,
        vnode_count: u32,
        chain_bound: u32,
    ) -> Result<std::collections::HashMap<u32, VnodeCapture>, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;

        let retractable = self.weight_col_idx.is_some();
        let fingerprint = self.query_fingerprint();
        // Resident vnodes remain in their chain even when this epoch has no changes. A forced
        // metadata-only shard keeps an emptied vnode visible for its required FULL re-base.
        let touched = self
            .vnode_states
            .iter()
            .filter_map(|(vnode, state)| {
                (!state.groups.is_empty() || state.force_full_rebase).then_some(vnode)
            })
            .collect::<Vec<_>>();

        let mut out: std::collections::HashMap<u32, VnodeCapture> =
            std::collections::HashMap::with_capacity(touched.len());
        for v in touched {
            let force_full = self.vnode_states.get(v).is_some_and(|state| {
                state.force_full_rebase
                    || state
                        .delta_chain_len
                        .is_none_or(|chain_len| chain_len >= chain_bound)
            });
            if force_full {
                let cap = self.checkpoint_full_vnode(v, vnode_count, fingerprint, retractable)?;
                out.insert(v, cap);
                let state = self.vnode_states.get_or_insert(v);
                state.delta_chain_len = Some(0);
                state.force_full_rebase = false;
            } else {
                let delta = self.encode_delta_for_vnode(v)?;
                out.insert(v, VnodeCapture::Delta(delta));
                let state = self.vnode_states.get_or_insert(v);
                state.delta_chain_len =
                    Some(state.delta_chain_len.unwrap_or_default().saturating_add(1));
            }
            let state = self.vnode_states.get_or_insert(v);
            state.checkpoint_dirty_keys.clear();
            state.last_emitted_dirty_keys.clear();
        }

        self.delta_tracking_active = true;
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
        vnode_count: NonZeroU32,
        only: Option<&AHashSet<arrow::row::OwnedRow>>,
    ) -> Result<Vec<EmittedCheckpoint>, DbError> {
        if !self.emit_changelog {
            return Ok(Vec::new());
        }
        debug_assert!(vnode < vnode_count.get());
        let Some(state) = self.vnode_states.get(vnode) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for (row_key, vals) in &state.last_emitted {
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
            .vnode_states
            .get(vnode)
            .map(|state| state.checkpoint_dirty_keys.clone())
            .unwrap_or_default();
        let emitted_changed = self
            .vnode_states
            .get(vnode)
            .map(|state| state.last_emitted_dirty_keys.clone())
            .unwrap_or_default();
        let mut entries: Vec<(arrow::row::OwnedRow, &mut GroupEntry)> = self
            .vnode_states
            .get_mut(vnode)
            .map(|state| {
                state
                    .groups
                    .iter_mut()
                    .filter(|(key, _)| changed.contains(*key))
                    .map(|(key, entry)| (key.clone(), entry))
                    .collect()
            })
            .unwrap_or_default();
        let encoded = encode_groups_columnar(
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            retractable,
            &mut entries,
        );
        drop(entries); // release the mutable vnode-state borrow before reading last_emitted
        let (keys_ipc, acc_state_ipc, last_updated_ms) = encoded?;

        // Changed emission entries ride in `changed.last_emitted`.
        if !self.delta_tracking_active {
            return Err(DbError::Pipeline(
                "aggregate delta encoding requires an established baseline".to_string(),
            ));
        }
        let vnode_count = self.routing_vnode_count();
        let last_emitted =
            self.last_emitted_for_vnode(vnode, vnode_count, Some(&emitted_changed))?;

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
    #[cfg(all(feature = "cluster", test))]
    fn clone_group_for_recovery(
        &mut self,
        key: &arrow::row::OwnedRow,
    ) -> Result<Option<GroupEntry>, DbError> {
        let vnode = Self::vnode_for_group_key(self.num_group_cols, key, self.routing_vnode_count());
        let Some(entry) = self
            .vnode_states
            .get_mut(vnode)
            .and_then(|state| state.groups.get_mut(key))
        else {
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

    #[cfg(all(feature = "cluster", test))]
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
            let vnode =
                Self::vnode_for_group_key(self.num_group_cols, key, self.routing_vnode_count());
            if let Some(values) = self
                .vnode_states
                .get(vnode)
                .and_then(|state| state.last_emitted.get(key))
            {
                if !self
                    .vnode_states
                    .get(vnode)
                    .is_some_and(|state| state.groups.contains_key(key))
                {
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
    fn stage_recovery_base(
        &self,
        staged: &mut StagedAggMutation,
        mutation: DecodedAggMutation,
    ) -> Result<(), DbError> {
        let retractable = self.weight_col_idx.is_some();
        for (row_key, last_updated_ms, state_arrays) in mutation.groups {
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
        Ok(())
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

    #[cfg(all(feature = "cluster", test))]
    fn commit_recovery_image(
        &mut self,
        mut staged: StagedAggMutation,
        merged_keys: &[arrow::row::OwnedRow],
        replaced_vnode: Option<(u32, NonZeroU32)>,
    ) {
        if let Some((vnode, vnode_count)) = replaced_vnode {
            debug_assert_eq!(vnode_count, self.routing_vnode_count());
            if let Some(state) = self.vnode_states.get_mut(vnode) {
                state.emit_dirty_keys.clear();
                state.checkpoint_dirty_keys.clear();
                state.last_emitted_dirty_keys.clear();
                state.delta_chain_len = None;
                state.force_full_rebase = false;
            }
        }
        let vnode_count = self.routing_vnode_count();
        let mut resident_group_count = self.vnode_states.resident_group_count();
        for key in staged.affected {
            let vnode = Self::vnode_for_group_key(self.num_group_cols, &key, vnode_count);
            let state = self.vnode_states.get_or_insert(vnode);
            if state.groups.remove(&key).is_some() {
                resident_group_count = resident_group_count
                    .checked_sub(1)
                    .expect("aggregate resident count must cover every removed group");
            }
            if let Some(entry) = staged.groups.remove(&key) {
                if state.groups.insert(key.clone(), entry).is_none() {
                    resident_group_count = resident_group_count
                        .checked_add(1)
                        .expect("aggregate resident group count overflow");
                }
            }
            state.last_emitted.remove(&key);
            if let Some(values) = staged.last_emitted.remove(&key) {
                state.last_emitted.insert(key, values);
            }
        }
        self.vnode_states
            .set_resident_group_count(resident_group_count);

        // Preserve the former merge bookkeeping, but publish it only with the state transaction.
        for row_key in merged_keys {
            let vnode = Self::vnode_for_group_key(self.num_group_cols, row_key, vnode_count);
            let state = self.vnode_states.get_or_insert(vnode);
            if self.emit_changelog {
                state.emit_dirty_keys.insert(row_key.clone());
            }
            if self.delta_tracking_active {
                state.checkpoint_dirty_keys.insert(row_key.clone());
            }
        }
    }

    /// Decode and build complete replacement vnode slots without changing the live working set.
    /// Every restored vnode is authoritative; a canonical empty restore and an explicitly revoked
    /// vnode without a restore both clear the physical slot. Once this returns, the graph must keep
    /// the operator quiescent until it aborts or publishes the prepared value.
    #[cfg(feature = "cluster")]
    pub(crate) fn prepare_vnode_transition(
        &self,
        vnode_count: u32,
        restores: &[AggVnodeRestore<'_>],
        revoked: &rustc_hash::FxHashSet<u32>,
    ) -> Result<PreparedAggVnodeTransition, DbError> {
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        let reserve_error = |component: &str, error: std::collections::TryReserveError| {
            DbError::Pipeline(format!(
                "aggregate vnode transition could not reserve {component}: {error}"
            ))
        };

        let transitioned_capacity =
            checked_vnode_transition_capacity(restores.len(), revoked.len())?;
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

        // Decode and validate the complete roster before constructing any replacement
        // accumulator. A corrupt late chain cannot leave an earlier chain partly published.
        let mut decoded = Vec::new();
        decoded
            .try_reserve_exact(restores.len())
            .map_err(|error| reserve_error("decoded restore roster", error))?;
        let mut restored_vnodes = rustc_hash::FxHashSet::default();
        restored_vnodes
            .try_reserve(restores.len())
            .map_err(|error| reserve_error("restored vnode roster", error))?;
        for restore in restores {
            if restore.vnode >= vnode_count.get() {
                return Err(DbError::Pipeline(format!(
                    "restored vnode {} is outside vnode_count {}",
                    restore.vnode,
                    vnode_count.get()
                )));
            }
            if !restored_vnodes.insert(restore.vnode) {
                return Err(DbError::Pipeline(format!(
                    "aggregate vnode transition repeats restored vnode {}",
                    restore.vnode
                )));
            }
            transitioned.insert(restore.vnode);

            let base = self.decode_recovery_mutation(restore.base, "vnode transition base")?;
            let deltas = restore
                .deltas
                .iter()
                .map(|delta| self.decode_recovery_delta(delta))
                .collect::<Result<Vec<_>, _>>()?;
            let belongs_to_vnode = |key: &arrow::row::OwnedRow| {
                Self::vnode_for_group_key(self.num_group_cols, key, vnode_count) == restore.vnode
            };
            let base_has_foreign_key = base.groups.iter().any(|(key, _, _)| !belongs_to_vnode(key))
                || base.last_emitted.keys().any(|key| !belongs_to_vnode(key));
            let delta_has_foreign_key = deltas.iter().any(|delta| {
                delta
                    .groups
                    .iter()
                    .any(|(key, _, _)| !belongs_to_vnode(key))
                    || delta.last_emitted.keys().any(|key| !belongs_to_vnode(key))
            });
            if base_has_foreign_key || delta_has_foreign_key {
                return Err(DbError::Pipeline(format!(
                    "authoritative vnode {} recovery chain contains a key for another vnode",
                    restore.vnode
                )));
            }
            decoded.push((restore.vnode, base, deltas));
        }

        let mut replacement_by_vnode = AHashMap::new();
        replacement_by_vnode
            .try_reserve(decoded.len())
            .map_err(|error| reserve_error("replacement vnode roster", error))?;
        let mut replacement_group_count = 0_usize;
        for (vnode, base, deltas) in decoded {
            let group_capacity = deltas.iter().try_fold(base.groups.len(), |total, delta| {
                checked_vnode_transition_capacity(total, delta.groups.len())
            })?;
            let emitted_capacity = deltas
                .iter()
                .try_fold(base.last_emitted.len(), |total, delta| {
                    checked_vnode_transition_capacity(total, delta.last_emitted.len())
                })?;
            let mut staged_groups = AHashMap::new();
            staged_groups
                .try_reserve(group_capacity)
                .map_err(|error| reserve_error("staged groups", error))?;
            let mut staged_last_emitted = AHashMap::new();
            staged_last_emitted
                .try_reserve(emitted_capacity)
                .map_err(|error| reserve_error("staged changelog state", error))?;
            let mut staged = StagedAggMutation {
                groups: staged_groups,
                last_emitted: staged_last_emitted,
                #[cfg(test)]
                affected: AHashSet::new(),
            };
            self.stage_recovery_base(&mut staged, base)?;
            for delta in deltas {
                self.apply_recovery_delta_to_image(&mut staged, delta)?;
            }
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
            let mut replacement = AggregateVnodeState {
                groups: staged.groups,
                last_emitted: staged.last_emitted,
                ..AggregateVnodeState::default()
            };
            if self.emit_changelog {
                replacement
                    .emit_dirty_keys
                    .try_reserve(replacement.groups.len())
                    .map_err(|error| reserve_error("changelog dirty keys", error))?;
                replacement
                    .emit_dirty_keys
                    .extend(replacement.groups.keys().cloned());
            }
            if self.delta_tracking_active {
                replacement
                    .checkpoint_dirty_keys
                    .try_reserve(replacement.groups.len())
                    .map_err(|error| reserve_error("checkpoint dirty keys", error))?;
                replacement
                    .checkpoint_dirty_keys
                    .extend(replacement.groups.keys().cloned());
            }
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
    #[cfg(feature = "cluster")]
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
        }
        self.vnode_states
            .swap_active_vnodes(&mut prepared.final_active_vnodes);
        self.vnode_states
            .set_resident_group_count(prepared.final_group_count);

        RetiredAggVnodeTransition {
            _retired_state: prepared,
        }
    }

    /// Release state retired by [`Self::publish_prepared_vnode_transition`] after the graph leaves
    /// its publication fence.
    #[cfg(feature = "cluster")]
    pub(crate) fn finish_vnode_transition(retired: RetiredAggVnodeTransition) {
        drop(retired);
    }

    #[cfg(all(feature = "cluster", test))]
    fn apply_recovery_transaction(
        &mut self,
        base: Option<&AggStateCheckpoint>,
        deltas: &[AggVnodeDelta],
        replacement: Option<(u32, u32)>,
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

        let replacement_scope = replacement
            .map(|(vnode, vnode_count)| {
                let vnode_count = self.validate_vnode_count(vnode_count)?;
                if vnode >= vnode_count.get() {
                    return Err(DbError::Pipeline(format!(
                        "vnode {vnode} is outside vnode_count {}",
                        vnode_count.get()
                    )));
                }
                let belongs_to_replaced_vnode = |key: &arrow::row::OwnedRow| {
                    Self::vnode_for_group_key(self.num_group_cols, key, vnode_count) == vnode
                };
                if decoded_base.as_ref().is_some_and(|mutation| {
                    mutation
                        .groups
                        .iter()
                        .any(|(key, _, _)| !belongs_to_replaced_vnode(key))
                        || mutation
                            .last_emitted
                            .keys()
                            .any(|key| !belongs_to_replaced_vnode(key))
                }) || decoded_deltas.iter().any(|mutation| {
                    mutation
                        .groups
                        .iter()
                        .any(|(key, _, _)| !belongs_to_replaced_vnode(key))
                        || mutation
                            .last_emitted
                            .keys()
                            .any(|key| !belongs_to_replaced_vnode(key))
                }) {
                    return Err(DbError::Pipeline(format!(
                        "authoritative vnode {vnode} recovery chain contains a key for another vnode"
                    )));
                }
                Ok((vnode, vnode_count))
            })
            .transpose()?;

        let mut affected = AHashSet::new();
        if let Some(mutation) = &decoded_base {
            affected.extend(mutation.groups.iter().map(|(key, _, _)| key.clone()));
            affected.extend(mutation.last_emitted.keys().cloned());
        }
        for delta in &decoded_deltas {
            affected.extend(delta.groups.iter().map(|(key, _, _)| key.clone()));
            affected.extend(delta.last_emitted.keys().cloned());
        }
        if let Some((vnode, vnode_count)) = replacement_scope {
            debug_assert_eq!(vnode_count, self.routing_vnode_count());
            if let Some(state) = self.vnode_states.get(vnode) {
                affected.extend(state.groups.keys().cloned());
                affected.extend(state.last_emitted.keys().cloned());
            }
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
            Some(mutation) => {
                let merged_keys = mutation
                    .groups
                    .iter()
                    .map(|(key, _, _)| key.clone())
                    .collect::<Vec<_>>();
                self.stage_recovery_base(&mut staged, mutation)?;
                merged_keys
            }
            None => Vec::new(),
        };
        for delta in decoded_deltas {
            self.apply_recovery_delta_to_image(&mut staged, delta)?;
        }
        let merged = merged_keys.len();
        self.commit_recovery_image(staged, &merged_keys, replacement_scope);
        Ok(merged)
    }

    /// Apply a delta atomically: changed groups replace existing state per key.
    #[cfg(all(feature = "cluster", test))]
    pub(crate) fn apply_delta(&mut self, delta: &AggVnodeDelta) -> Result<(), DbError> {
        self.apply_recovery_transaction(None, std::slice::from_ref(delta), None)
            .map(|_| ())
    }

    /// Merge a recovered chain transactionally. Test-only: production vnode restore must replace
    /// keys absent from the authoritative image through [`Self::replace_vnode_chain`].
    #[cfg(all(feature = "cluster", test))]
    pub(crate) fn apply_vnode_chain(
        &mut self,
        base: &AggStateCheckpoint,
        deltas: &[AggVnodeDelta],
    ) -> Result<usize, DbError> {
        self.apply_recovery_transaction(Some(base), deltas, None)
    }

    /// Replace one vnode from a recovered chain and publish it once after every delta succeeds.
    /// Keys absent from the authoritative image are removed, so retry cannot retain post-cut state.
    #[cfg(all(feature = "cluster", test))]
    pub(crate) fn replace_vnode_chain(
        &mut self,
        vnode: u32,
        vnode_count: u32,
        base: &AggStateCheckpoint,
        deltas: &[AggVnodeDelta],
    ) -> Result<usize, DbError> {
        self.apply_recovery_transaction(Some(base), deltas, Some((vnode, vnode_count)))
    }

    /// Apply an authoritative FULL checkpoint transactionally. Disjoint vnode keys are inserted;
    /// overlapping keys are replaced so replay is idempotent.
    #[cfg(all(feature = "cluster", test))]
    pub(crate) fn merge_groups(
        &mut self,
        checkpoint: &AggStateCheckpoint,
    ) -> Result<usize, DbError> {
        self.apply_recovery_transaction(Some(checkpoint), &[], None)
    }

    /// Re-base delta tracking for vnodes this node just ACQUIRED (owned-set grew): a just-acquired
    /// vnode has no parent epoch to chain a delta onto, so its next capture must re-upload FULL.
    /// This is the only place a resident chain is reset on ownership change. An acquired vnode
    /// without a resident slot is absent from delta capture; `SqlQueryOperator::checkpoint_by_vnode`
    /// supplies its required canonical EMPTY full image instead, so it cannot emit a parentless
    /// delta (LDB-6025).
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
            if let Some(state) = self.vnode_states.get_mut(*v) {
                state.delta_chain_len = None;
            }
        }
    }

    /// Force every chained vnode's next delta capture to re-base FULL after a failed epoch, whose
    /// destructive capture cleared the dirty sets before durability. Re-arms emptied vnode slots
    /// through their `force_full_rebase` flag; no-op when delta is off.
    #[cfg(feature = "cluster")]
    pub(crate) fn force_full_rebase(&mut self) {
        for (_, state) in self.vnode_states.iter_mut() {
            if state.delta_chain_len.take().is_some() {
                state.force_full_rebase = true;
            }
        }
    }

    /// Drop in-memory state for revoked (lost) vnodes. Ownership movement is a physical state
    /// transition, not a logical relation change, so it must not emit changelog rows. Purging
    /// prevents stale keys absent from a later FULL image from surviving a re-acquire.
    #[cfg(all(feature = "cluster", test))]
    pub(crate) fn drop_vnodes(
        &mut self,
        revoked: &rustc_hash::FxHashSet<u32>,
        vnode_count: u32,
    ) -> Result<(), DbError> {
        if revoked.is_empty() {
            return Ok(());
        }
        let vnode_count = self.validate_vnode_count(vnode_count)?;
        if let Some(vnode) = revoked.iter().find(|vnode| **vnode >= vnode_count.get()) {
            return Err(DbError::Pipeline(format!(
                "revoked vnode {vnode} is outside vnode_count {}",
                vnode_count.get()
            )));
        }
        let mut resident_group_count = self.vnode_states.resident_group_count();
        for v in revoked {
            if let Some(retired) = self.vnode_states.remove(*v) {
                resident_group_count = resident_group_count
                    .checked_sub(retired.groups.len())
                    .expect("aggregate resident count must cover every revoked group");
            }
        }
        self.vnode_states
            .set_resident_group_count(resident_group_count);
        Ok(())
    }
}

#[cfg(test)]
mod tests;

/// Per-vnode checkpoint partitioning + merge-apply (the cross-node vnode
/// rehydration round-trip). Gated to cluster builds since that's where the
/// new methods compile.
#[cfg(all(test, feature = "cluster"))]
mod vnode_partition_tests;
