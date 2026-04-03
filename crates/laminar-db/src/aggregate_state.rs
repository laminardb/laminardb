//! Incremental aggregate state for streaming GROUP BY queries.
//!
//! Maintains per-group accumulator state across micro-batch cycles so that
//! streaming aggregations (SUM, COUNT, AVG, etc.) produce correct running
//! totals instead of per-batch results.
//!
//! # Architecture
//!
//! ```text
//! Source batch → group-by partition → per-group accumulators → emit RecordBatch
//!                                          ↕ (persists across cycles)
//! ```
//!
//! Uses `DataFusion`'s `Accumulator` trait directly for correct semantics
//! across all 50+ built-in aggregates (including AVG, STDDEV, etc. that
//! require multi-field state).

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ahash::AHashMap;
use rustc_hash::FxHashMap;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::{AggregateUDF, LogicalPlan};
use serde_json::json;

use crate::error::DbError;

// ── Shared group-key helpers ─────────────────────────────────────────

/// Convert an `OwnedRow` back to a `Vec<ScalarValue>` group key,
/// casting types to match `group_types` when needed.
///
/// Used by `IncrementalAggState`, `CoreWindowState`, and `IncrementalEowcState`
/// to convert from the compact row format back to the `Vec<ScalarValue>` keys
/// used in the per-group accumulator maps.
pub(crate) fn row_to_scalar_key_with_types(
    converter: &arrow::row::RowConverter,
    row_key: &arrow::row::OwnedRow,
    group_types: &[DataType],
) -> Result<Vec<ScalarValue>, DbError> {
    let row_as_cols = converter
        .convert_rows(std::iter::once(row_key.row()))
        .map_err(|e| DbError::Pipeline(format!("row→key: {e}")))?;

    let mut sv_key = Vec::with_capacity(group_types.len());
    for (col_idx, arr) in row_as_cols.iter().enumerate() {
        let sv = ScalarValue::try_from_array(arr, 0)
            .map_err(|e| DbError::Pipeline(format!("group key decode: {e}")))?;
        if sv.data_type() == group_types[col_idx] {
            sv_key.push(sv);
        } else {
            sv_key.push(sv.cast_to(&group_types[col_idx]).unwrap_or(sv));
        }
    }
    Ok(sv_key)
}

/// Sentinel `OwnedRow` for global aggregates (no GROUP BY).
///
/// Cached via `OnceLock` — allocated once, reused forever.
pub(crate) fn global_aggregate_key() -> arrow::row::OwnedRow {
    use std::sync::OnceLock;
    static SENTINEL: OnceLock<arrow::row::OwnedRow> = OnceLock::new();
    SENTINEL
        .get_or_init(|| {
            let conv =
                arrow::row::RowConverter::new(vec![arrow::row::SortField::new(DataType::Boolean)])
                    .unwrap();
            let col: ArrayRef = Arc::new(arrow::array::BooleanArray::from(vec![false]));
            conv.convert_columns(&[col]).unwrap().row(0).owned()
        })
        .clone()
}

/// Convert a `Vec<ScalarValue>` key to an `OwnedRow`, casting to
/// `target_types` when `json_to_scalar` has widened the types.
pub(crate) fn scalar_key_to_owned_row(
    row_converter: &arrow::row::RowConverter,
    sv_key: &[ScalarValue],
    target_types: &[DataType],
) -> Result<arrow::row::OwnedRow, DbError> {
    if sv_key.is_empty() {
        return Ok(global_aggregate_key());
    }
    let arrays: Vec<ArrayRef> = sv_key
        .iter()
        .enumerate()
        .map(|(i, sv)| {
            let arr = sv
                .to_array()
                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))?;
            if i < target_types.len() && arr.data_type() != &target_types[i] {
                arrow::compute::cast(&arr, &target_types[i])
                    .map_err(|e| DbError::Pipeline(format!("key type cast: {e}")))
            } else {
                Ok(arr)
            }
        })
        .collect::<Result<_, _>>()?;
    let rows = row_converter
        .convert_columns(&arrays)
        .map_err(|e| DbError::Pipeline(format!("key to row: {e}")))?;
    Ok(rows.row(0).owned())
}

/// Emit a single window's accumulated state as a `RecordBatch`.
///
/// Shared by `CoreWindowState` and `IncrementalEowcState`. Builds columns
/// for `window_start`, `window_end`, group keys, and aggregate results.
pub(crate) fn emit_window_batch(
    window_start: i64,
    window_end: i64,
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

    let win_start_array: ArrayRef =
        Arc::new(arrow::array::Int64Array::from(vec![window_start; num_rows]));
    let win_end_array: ArrayRef =
        Arc::new(arrow::array::Int64Array::from(vec![window_end; num_rows]));

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

    let mut all_arrays = vec![win_start_array, win_end_array];
    all_arrays.extend(group_arrays);
    all_arrays.extend(agg_arrays);

    let batch = RecordBatch::try_new(Arc::clone(output_schema), all_arrays)
        .map_err(|e| DbError::Pipeline(format!("result batch build: {e}")))?;

    Ok(Some(batch))
}

// ── ScalarValue JSON encoding ────────────────────────────────────────

/// Encode a `ScalarValue` to a JSON value for checkpoint serialization.
///
/// Supports the common types returned by `Accumulator::state()`:
/// Null, Boolean, Int8–64, UInt8–64, Float32/64, Utf8, and List.
/// Integer types are widened to i64/u64 for compact encoding.
pub(crate) fn scalar_to_json(sv: &ScalarValue) -> serde_json::Value {
    match sv {
        ScalarValue::Null => json!({"t": "N"}),
        ScalarValue::Boolean(None) => json!({"t": "B", "v": null}),
        ScalarValue::Boolean(Some(b)) => json!({"t": "B", "v": b}),
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None) => json!({"t": "I64", "v": null}),
        ScalarValue::Int8(Some(n)) => json!({"t": "I64", "v": i64::from(*n)}),
        ScalarValue::Int16(Some(n)) => json!({"t": "I64", "v": i64::from(*n)}),
        ScalarValue::Int32(Some(n)) => json!({"t": "I64", "v": i64::from(*n)}),
        ScalarValue::Int64(Some(n)) => json!({"t": "I64", "v": n}),
        ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None) => json!({"t": "U64", "v": null}),
        ScalarValue::UInt8(Some(n)) => json!({"t": "U64", "v": u64::from(*n)}),
        ScalarValue::UInt16(Some(n)) => json!({"t": "U64", "v": u64::from(*n)}),
        ScalarValue::UInt32(Some(n)) => json!({"t": "U64", "v": u64::from(*n)}),
        ScalarValue::UInt64(Some(n)) => json!({"t": "U64", "v": n}),
        ScalarValue::Float32(None) | ScalarValue::Float64(None) => {
            json!({"t": "F64", "v": null})
        }
        ScalarValue::Float32(Some(f)) => json!({"t": "F64", "v": f64::from(*f)}),
        ScalarValue::Float64(Some(f)) => json!({"t": "F64", "v": f}),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            json!({"t": "S", "v": null})
        }
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => json!({"t": "S", "v": s}),
        ScalarValue::List(arr) => {
            use arrow::array::Array;
            let list_arr: Option<&arrow::array::ListArray> = arr.as_any().downcast_ref();
            match list_arr {
                Some(list) if !list.is_empty() => {
                    let values = list.value(0);
                    let mut items = Vec::with_capacity(values.len());
                    for i in 0..values.len() {
                        let sv =
                            ScalarValue::try_from_array(&values, i).unwrap_or(ScalarValue::Null);
                        items.push(scalar_to_json(&sv));
                    }
                    json!({"t": "L", "v": items})
                }
                _ => json!({"t": "L", "v": []}),
            }
        }
        // Fallback: encode as string representation with type tag
        other => json!({"t": "STR", "v": other.to_string()}),
    }
}

/// Decode a JSON value back to a `ScalarValue`.
pub(crate) fn json_to_scalar(v: &serde_json::Value) -> Result<ScalarValue, DbError> {
    let t = v
        .get("t")
        .and_then(|t| t.as_str())
        .ok_or_else(|| DbError::Pipeline("missing type tag in scalar JSON".to_string()))?;
    let val = v.get("v");
    match t {
        "N" => Ok(ScalarValue::Null),
        "B" => match val.and_then(serde_json::Value::as_bool) {
            Some(b) => Ok(ScalarValue::Boolean(Some(b))),
            None => Ok(ScalarValue::Boolean(None)),
        },
        "I64" => match val {
            Some(serde_json::Value::Number(n)) => Ok(ScalarValue::Int64(n.as_i64())),
            _ => Ok(ScalarValue::Int64(None)),
        },
        "U64" => match val {
            Some(serde_json::Value::Number(n)) => Ok(ScalarValue::UInt64(n.as_u64())),
            _ => Ok(ScalarValue::UInt64(None)),
        },
        "F64" => match val {
            Some(serde_json::Value::Number(n)) => Ok(ScalarValue::Float64(n.as_f64())),
            _ => Ok(ScalarValue::Float64(None)),
        },
        "S" => match val.and_then(|v| v.as_str()) {
            Some(s) => Ok(ScalarValue::Utf8(Some(s.to_string()))),
            None => Ok(ScalarValue::Utf8(None)),
        },
        "L" => {
            let items = val
                .and_then(|v| v.as_array())
                .ok_or_else(|| DbError::Pipeline("expected array for List scalar".to_string()))?;
            let scalars: Result<Vec<ScalarValue>, _> = items.iter().map(json_to_scalar).collect();
            let scalars = scalars?;
            if scalars.is_empty() {
                Ok(ScalarValue::List(Arc::new(
                    arrow::array::GenericListArray::new_null(
                        Arc::new(Field::new("item", DataType::Null, true)),
                        1,
                    ),
                )))
            } else {
                let arr = ScalarValue::new_list(&scalars, &scalars[0].data_type(), true);
                Ok(ScalarValue::List(arr))
            }
        }
        other => Err(DbError::Pipeline(format!(
            "unsupported scalar type tag in checkpoint: {other}"
        ))),
    }
}

/// Compute a u64 fingerprint for a query based on its SQL and output schema.
///
/// Used to detect schema evolution between checkpoint save and restore.
pub(crate) fn query_fingerprint(pre_agg_sql: &str, output_schema: &Schema) -> u64 {
    let mut hasher = std::hash::DefaultHasher::new();
    pre_agg_sql.hash(&mut hasher);
    for field in output_schema.fields() {
        field.name().hash(&mut hasher);
        field.data_type().to_string().hash(&mut hasher);
    }
    hasher.finish()
}

/// Serializable checkpoint for a single group's state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct GroupCheckpoint {
    /// Serialized group key.
    pub key: Vec<serde_json::Value>,
    /// Per-accumulator state vectors.
    pub acc_states: Vec<Vec<serde_json::Value>>,
    /// Watermark at which this group was last updated (for TTL).
    #[serde(default = "default_last_updated")]
    pub last_updated_ms: i64,
}

fn default_last_updated() -> i64 {
    i64::MIN
}

/// Serializable checkpoint for all groups in an agg state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct AggStateCheckpoint {
    /// Query fingerprint for schema validation.
    pub fingerprint: u64,
    /// Per-group checkpoint data.
    pub groups: Vec<GroupCheckpoint>,
    /// Last emitted values per group (changelog mode only).
    #[serde(default)]
    pub last_emitted: Vec<EmittedCheckpoint>,
}

/// Serializable checkpoint for a single group's last emitted value.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct EmittedCheckpoint {
    /// Serialized group key.
    pub key: Vec<serde_json::Value>,
    /// Last emitted aggregate values.
    pub values: Vec<serde_json::Value>,
}

/// Serializable checkpoint for a single window's groups.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct WindowCheckpoint {
    /// Window start timestamp (ms since epoch).
    pub window_start: i64,
    /// Groups within this window.
    pub groups: Vec<GroupCheckpoint>,
}

/// Serializable checkpoint for all windows in an EOWC state.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct EowcStateCheckpoint {
    /// Query fingerprint for schema validation.
    pub fingerprint: u64,
    /// Per-window checkpoint data.
    pub windows: Vec<WindowCheckpoint>,
}

/// Serializable checkpoint for join state (interval joins).
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct JoinStateCheckpoint {
    /// Number of buffered left-side rows.
    #[serde(default)]
    pub left_buffer_rows: u64,
    /// Number of buffered right-side rows.
    #[serde(default)]
    pub right_buffer_rows: u64,
    /// Serialized left-side batches (Arrow IPC).
    #[serde(default)]
    pub left_batches: Vec<Vec<u8>>,
    /// Serialized right-side batches (Arrow IPC).
    #[serde(default)]
    pub right_batches: Vec<Vec<u8>>,
    /// Last cutoff used for left-side eviction.
    #[serde(default = "default_evicted_watermark")]
    pub last_evicted_watermark: i64,
    /// Last cutoff used for right-side eviction.
    #[serde(default = "default_evicted_watermark")]
    pub last_evicted_watermark_right: i64,
}

fn default_evicted_watermark() -> i64 {
    i64::MIN
}

/// Top-level checkpoint for the entire `StreamExecutor`.
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct StreamExecutorCheckpoint {
    /// Checkpoint format version.
    pub version: u32,
    /// Virtual partition count at checkpoint time.
    #[serde(default)]
    pub vnode_count: u16,
    /// Non-EOWC aggregate states, keyed by query name.
    #[serde(default)]
    pub agg_states: FxHashMap<String, AggStateCheckpoint>,
    /// EOWC aggregate states, keyed by query name.
    #[serde(default)]
    pub eowc_states: FxHashMap<String, EowcStateCheckpoint>,
    /// Core window pipeline states, keyed by query name.
    #[serde(default)]
    pub core_window_states: FxHashMap<String, crate::core_window_state::CoreWindowCheckpoint>,
    /// Join states, keyed by query name.
    ///
    /// Currently empty for ASOF/temporal joins (stateless per-cycle).
    /// Populated by future stateful joins (interval joins, etc.).
    #[serde(default)]
    pub join_states: FxHashMap<String, JoinStateCheckpoint>,
    /// Raw-batch EOWC states, keyed by query name.
    ///
    /// These are non-aggregate EOWC queries that accumulate raw
    /// `RecordBatch` data until the watermark closes their windows.
    /// Each batch is serialized as Arrow IPC bytes.
    #[serde(default)]
    pub raw_eowc_states: FxHashMap<String, RawEowcCheckpoint>,
}

// Compile-time assertion: StreamExecutorCheckpoint must be Send
// so it can be offloaded to spawn_blocking for serialization.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<StreamExecutorCheckpoint>();
};

/// Checkpoint for raw-batch EOWC state (non-aggregate EOWC queries).
///
/// Stores accumulated `RecordBatch` data as Arrow IPC bytes plus
/// the watermark boundary used for window-close tracking.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RawEowcCheckpoint {
    /// The last closed-window boundary that was emitted.
    pub last_closed_boundary: i64,
    /// Total accumulated rows (for diagnostics).
    pub accumulated_rows: usize,
    /// Accumulated source batches, keyed by source table name.
    /// Each inner `Vec<u8>` is a single `RecordBatch` serialized as Arrow IPC.
    pub sources: FxHashMap<String, Vec<Vec<u8>>>,
}

/// Specification for one aggregate function in a streaming query.
pub(crate) struct AggFuncSpec {
    /// The `DataFusion` aggregate UDF.
    pub(crate) udf: Arc<AggregateUDF>,
    /// Input data types for this aggregate.
    pub(crate) input_types: Vec<DataType>,
    /// Column indices in the pre-aggregation output that feed this aggregate.
    /// These index into the pre-agg schema (group cols first, then agg inputs).
    pub(crate) input_col_indices: Vec<usize>,
    /// Output column name (alias from the query).
    pub(crate) output_name: String,
    /// Output data type.
    pub(crate) return_type: DataType,
    /// Whether this is a DISTINCT aggregate (e.g., `COUNT(DISTINCT x)`).
    pub(crate) distinct: bool,
    /// Whether this is COUNT(*) — uses a dummy boolean input, no real column.
    pub(crate) is_count_star: bool,
    /// Column index of the FILTER boolean column in pre-agg output, if any.
    pub(crate) filter_col_index: Option<usize>,
}

impl AggFuncSpec {
    /// Create a fresh `DataFusion` accumulator for this function.
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

    /// Create a weight-aware retractable accumulator for changelog streams.
    ///
    /// These accumulators receive the `__weight` column as the last input
    /// in `update_batch` and handle retraction internally.
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

/// Incremental aggregation state for a streaming GROUP BY query.
///
/// Persists per-group accumulator state across micro-batch cycles so that
/// running aggregations produce correct results.
pub(crate) struct IncrementalAggState {
    /// SQL that computes pre-aggregation expressions (group keys + agg inputs).
    /// Kept for `query_fingerprint()` and as fallback for multi-source queries.
    pre_agg_sql: String,
    /// Number of group-by columns in the pre-agg output (first N columns).
    num_group_cols: usize,
    /// Group-by column data types.
    group_types: Vec<DataType>,
    /// Aggregate function specifications.
    agg_specs: Vec<AggFuncSpec>,
    /// Per-group accumulator state keyed by binary-encoded group key.
    groups: AHashMap<arrow::row::OwnedRow, GroupEntry>,
    /// Persistent row converter for encoding/decoding group keys.
    row_converter: arrow::row::RowConverter,
    /// Output schema (group columns + aggregate results).
    output_schema: SchemaRef,
    /// Compiled pre-agg projection (single-source queries only).
    compiled_projection: Option<CompiledProjection>,
    /// Cached optimized logical plan for the pre-agg SQL (multi-source queries).
    /// Skips SQL parsing and logical optimization on subsequent cycles.
    cached_pre_agg_plan: Option<LogicalPlan>,
    /// Compiled HAVING predicate.
    having_filter: Option<Arc<dyn PhysicalExpr>>,
    /// HAVING predicate SQL fallback (when compiled filter is not available).
    having_sql: Option<String>,
    /// Maximum number of distinct groups before new groups are dropped.
    max_groups: usize,
    /// When true, `emit()` produces Z-set delta records with a `__weight`
    /// column (+1 insert, -1 retract). Only changed groups are emitted.
    emit_changelog: bool,
    /// Previous cycle's emitted values per group. Used to detect changes
    /// and produce retraction pairs. Only populated when `emit_changelog`.
    last_emitted: AHashMap<arrow::row::OwnedRow, Vec<ScalarValue>>,
    /// Idle group TTL in milliseconds. Only usable with `emit_changelog`
    /// (eviction emits -1 retractions before removing groups).
    pub(crate) idle_ttl_ms: Option<u64>,
    /// Column index of `__weight` in the pre-agg batch when the upstream
    /// source is a changelog stream. `None` for non-changelog sources.
    weight_col_idx: Option<usize>,
}

/// Name of the Z-set weight column appended to changelog output.
pub(crate) const WEIGHT_COLUMN: &str = "__weight";

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

    let weight_array: ArrayRef = Arc::new(arrow::array::Int64Array::from(weights.to_vec()));

    let mut all_arrays = group_arrays;
    all_arrays.extend(agg_arrays);
    all_arrays.push(weight_array);

    RecordBatch::try_new(Arc::clone(output_schema), all_arrays)
        .map_err(|e| DbError::Pipeline(format!("weighted batch: {e}")))
}

/// Per-group accumulator state with TTL metadata.
pub(crate) struct GroupEntry {
    pub(crate) accs: Vec<Box<dyn datafusion_expr::Accumulator>>,
    /// Watermark (ms) at which this group last received data.
    pub(crate) last_updated_ms: i64,
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

        // Use the top-level plan's output schema for column names — this
        // includes user-specified aliases (e.g., `SUM(x) AS total`).
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

        // Bail out if the top-level plan has a non-trivial projection above
        // the Aggregate (e.g., SUM(a)/SUM(b) AS ratio, CASE WHEN ... END).
        // The incremental accumulator emits raw aggregate outputs and cannot
        // apply post-aggregate projections.  Fall through to EOWC raw-batch.
        //
        // Check both field count AND types — a coincidental count match (e.g.,
        // 2 group keys + 3 aggregates == 5 SELECT items after projection drops
        // a group key and adds a computed column) can mask a projection that
        // remaps group columns to aggregate aliases.
        if top_schema.fields().len() != agg_schema.fields().len() {
            return Ok(None);
        }
        for (top_f, agg_f) in top_schema.fields().iter().zip(agg_schema.fields()) {
            if top_f.data_type() != agg_f.data_type() {
                return Ok(None);
            }
        }

        let num_group_cols = group_exprs.len();

        // Resolve group-by column names and types.
        // Use top-level schema for names (preserves aliases) and agg schema
        // for types (accurate data types from the aggregate node).
        let mut group_col_names = Vec::new();
        let mut group_types = Vec::new();
        for i in 0..num_group_cols {
            let top_field = top_schema.field(i);
            let agg_field = agg_schema.field(i);
            group_col_names.push(top_field.name().clone());
            group_types.push(agg_field.data_type().clone());
        }

        // Determine if we should attempt to compile pre-agg expressions.
        // Use single_source_table (counts occurrences) to reject self-joins.
        let compile_source = crate::sql_analysis::single_source_table(sql);
        let state = ctx.state();
        let props = state.execution_props();
        let input_df_schema = &agg_info.input_df_schema;
        let mut compiled_exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let mut proj_fields: Vec<Field> = Vec::new();
        let mut compile_ok = compile_source.is_some();

        let mut agg_specs = Vec::new();
        let mut pre_agg_select_items: Vec<String> = Vec::new();

        // For simple column refs, quote as identifier. For complex
        // expressions (EXTRACT, CASE WHEN, etc.), generate the SQL
        // expression with an alias so the source query evaluates it.
        for (i, group_expr) in group_exprs.iter().enumerate() {
            if let datafusion_expr::Expr::Column(col) = group_expr {
                pre_agg_select_items.push(format!("\"{}\"", col.name));
            } else {
                let group_sql = expr_to_sql(group_expr);
                pre_agg_select_items.push(format!("{group_sql} AS \"__group_{i}\""));
            }

            // Compile group expression
            if compile_ok {
                match create_physical_expr(group_expr, input_df_schema, props) {
                    Ok(phys) => {
                        let dt = phys
                            .data_type(input_df_schema.as_arrow())
                            .unwrap_or(DataType::Utf8);
                        let name = match group_expr {
                            datafusion_expr::Expr::Column(col) => col.name.clone(),
                            _ => format!("__group_{i}"),
                        };
                        proj_fields.push(Field::new(name, dt, true));
                        compiled_exprs.push(phys);
                    }
                    Err(_) => compile_ok = false,
                }
            }
        }

        // Column index tracker for pre-agg output
        let mut next_col_idx = num_group_cols;

        for (i, expr) in aggr_exprs.iter().enumerate() {
            let agg_schema_idx = num_group_cols + i;
            let agg_field = agg_schema.field(agg_schema_idx);
            // Use the top-level schema for the output name (has user alias)
            let output_name = if agg_schema_idx < top_schema.fields().len() {
                top_schema.field(agg_schema_idx).name().clone()
            } else {
                agg_field.name().clone()
            };

            if let datafusion_expr::Expr::AggregateFunction(agg_func) = expr {
                let udf = Arc::clone(&agg_func.func);
                let is_distinct = agg_func.params.distinct;

                // Collect input column indices and add input expressions
                // to the pre-agg SELECT
                let mut input_col_indices = Vec::new();
                let mut input_types = Vec::new();

                if agg_func.params.args.is_empty() {
                    // COUNT(*) — no input columns needed, pass a dummy boolean
                    let col_idx = next_col_idx;
                    next_col_idx += 1;
                    pre_agg_select_items.push(format!("TRUE AS \"__agg_input_{col_idx}\""));
                    input_col_indices.push(col_idx);
                    input_types.push(DataType::Boolean);

                    // Compile literal TRUE
                    if compile_ok {
                        match create_physical_expr(
                            &datafusion_expr::lit(true),
                            input_df_schema,
                            props,
                        ) {
                            Ok(phys) => {
                                proj_fields.push(Field::new(
                                    format!("__agg_input_{col_idx}"),
                                    DataType::Boolean,
                                    true,
                                ));
                                compiled_exprs.push(phys);
                            }
                            Err(_) => compile_ok = false,
                        }
                    }
                } else {
                    for arg_expr in &agg_func.params.args {
                        let col_idx = next_col_idx;
                        next_col_idx += 1;
                        let expr_sql = expr_to_sql(arg_expr);

                        // If FILTER clause present, wrap input with
                        // CASE WHEN so filtered rows become NULL (ignored
                        // by accumulators)
                        if let Some(filter_expr) = &agg_func.params.filter {
                            let filter_sql = expr_to_sql(filter_expr);
                            pre_agg_select_items.push(format!(
                                "CASE WHEN {filter_sql} THEN {expr_sql} ELSE NULL END AS \"__agg_input_{col_idx}\""
                            ));

                            // Compile: CASE WHEN filter THEN arg ELSE NULL END
                            if compile_ok {
                                let case_expr =
                                    datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
                                        expr: None,
                                        when_then_expr: vec![(
                                            Box::new(filter_expr.as_ref().clone()),
                                            Box::new(arg_expr.clone()),
                                        )],
                                        else_expr: Some(Box::new(datafusion_expr::lit(
                                            ScalarValue::Null,
                                        ))),
                                    });
                                match create_physical_expr(&case_expr, input_df_schema, props) {
                                    Ok(phys) => {
                                        let dt = resolve_expr_type(
                                            arg_expr,
                                            &input_schema,
                                            agg_field.data_type(),
                                        );
                                        proj_fields.push(Field::new(
                                            format!("__agg_input_{col_idx}"),
                                            dt,
                                            true,
                                        ));
                                        compiled_exprs.push(phys);
                                    }
                                    Err(_) => compile_ok = false,
                                }
                            }
                        } else {
                            pre_agg_select_items
                                .push(format!("{expr_sql} AS \"__agg_input_{col_idx}\""));

                            // Compile the arg expression directly
                            if compile_ok {
                                match create_physical_expr(arg_expr, input_df_schema, props) {
                                    Ok(phys) => {
                                        let dt = resolve_expr_type(
                                            arg_expr,
                                            &input_schema,
                                            agg_field.data_type(),
                                        );
                                        proj_fields.push(Field::new(
                                            format!("__agg_input_{col_idx}"),
                                            dt,
                                            true,
                                        ));
                                        compiled_exprs.push(phys);
                                    }
                                    Err(_) => compile_ok = false,
                                }
                            }
                        }

                        input_col_indices.push(col_idx);
                        let dt = resolve_expr_type(arg_expr, &input_schema, agg_field.data_type());
                        input_types.push(dt);
                    }
                }

                // Add a boolean filter column for masking rows
                // in process_batch when FILTER clause is present
                let filter_col_index = if let Some(filter_expr) = &agg_func.params.filter {
                    let col_idx = next_col_idx;
                    next_col_idx += 1;
                    let filter_sql = expr_to_sql(filter_expr);
                    pre_agg_select_items.push(format!(
                            "CASE WHEN {filter_sql} THEN TRUE ELSE FALSE END AS \"__agg_filter_{col_idx}\""
                        ));

                    // Compile: CASE WHEN filter THEN TRUE ELSE FALSE END
                    if compile_ok {
                        let case_expr = datafusion_expr::Expr::Case(datafusion_expr::expr::Case {
                            expr: None,
                            when_then_expr: vec![(
                                Box::new(filter_expr.as_ref().clone()),
                                Box::new(datafusion_expr::lit(true)),
                            )],
                            else_expr: Some(Box::new(datafusion_expr::lit(false))),
                        });
                        match create_physical_expr(&case_expr, input_df_schema, props) {
                            Ok(phys) => {
                                proj_fields.push(Field::new(
                                    format!("__agg_filter_{col_idx}"),
                                    DataType::Boolean,
                                    true,
                                ));
                                compiled_exprs.push(phys);
                            }
                            Err(_) => compile_ok = false,
                        }
                    }

                    Some(col_idx)
                } else {
                    None
                };

                let return_type = udf
                    .return_type(&input_types)
                    .unwrap_or_else(|_| agg_field.data_type().clone());

                agg_specs.push(AggFuncSpec {
                    udf,
                    input_types,
                    input_col_indices,
                    output_name,
                    return_type,
                    distinct: is_distinct,
                    is_count_star: agg_func.params.args.is_empty(),
                    filter_col_index,
                });
            } else {
                // Non-aggregate expression in the aggregate output —
                // this shouldn't happen for well-formed plans.
                return Ok(None);
            }
        }

        let clauses = extract_clauses(sql);

        // Detect __weight in upstream source for cascaded changelog aggregation.
        // Check the registered table schema (not the pruned plan schema, which
        // may have dropped unreferenced columns).
        let source_has_weight = if let Ok(tp) = ctx
            .table_provider(clauses.from_clause.trim_matches('"'))
            .await
        {
            tp.schema().column_with_name(WEIGHT_COLUMN).is_some()
        } else {
            false
        };

        let weight_col_idx = if source_has_weight {
            // Validate that all aggregates support retractable changelog
            // semantics. Retractable accumulators handle the weight column
            // internally (SUM, COUNT, AVG, MIN, MAX).
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

        // Build compiled projection for single-source queries.
        // Skip compilation when upstream has __weight — the compiled path
        // doesn't include the weight column, so process_batch would read
        // the wrong index. The cached plan path handles it via pre_agg_sql.
        let compiled_projection = if compile_ok && weight_col_idx.is_none() {
            let source_table = compile_source.unwrap();
            // Compile WHERE predicate
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
                    source_table,
                    exprs: compiled_exprs,
                    filter,
                    output_schema: Arc::new(Schema::new(proj_fields)),
                })
            } else {
                None
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

        // Compile HAVING filter.
        let having_filter = compile_having_filter(ctx, having_predicate.as_ref(), &output_schema);
        let having_sql = if having_filter.is_none() {
            having_predicate.as_ref().map(expr_to_sql)
        } else {
            None
        };

        // ONE-TIME setup: cache the optimized logical plan for multi-source
        // pre-agg queries (when compiled projection is not available). This
        // ctx.sql() call runs ONLY at first-cycle initialization, never
        // per-cycle. Subsequent cycles use the cached plan directly.
        // Fail fast if the pre-agg SQL is invalid — it would fail every cycle.
        let cached_pre_agg_plan = if compiled_projection.is_none() {
            match ctx.sql(&pre_agg_sql).await {
                Ok(df) => Some(df.logical_plan().clone()),
                Err(e) => {
                    return Err(DbError::Pipeline(format!(
                        "pre-agg SQL planning failed for aggregate: {e}"
                    )));
                }
            }
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
            pre_agg_sql,
            num_group_cols,
            group_types,
            agg_specs,
            groups: AHashMap::new(),
            row_converter,
            output_schema,
            compiled_projection,
            cached_pre_agg_plan,
            having_filter,
            having_sql,
            max_groups: 1_000_000,
            emit_changelog,
            last_emitted: AHashMap::new(),
            idle_ttl_ms: None,
            weight_col_idx,
        }))
    }

    /// Evict idle groups and return retraction records. Only produces output
    /// when both `emit_changelog` and `idle_ttl_ms` are set.
    pub fn evict_idle(&mut self, watermark: i64) -> Result<Vec<RecordBatch>, DbError> {
        let Some(ttl) = self.idle_ttl_ms else {
            return Ok(Vec::new());
        };
        if !self.emit_changelog {
            return Ok(Vec::new());
        }

        #[allow(clippy::cast_possible_wrap)]
        let cutoff = watermark.saturating_sub(ttl as i64);

        let idle_keys: Vec<arrow::row::OwnedRow> = self
            .groups
            .iter()
            .filter(|(_, e)| e.last_updated_ms < cutoff)
            .map(|(k, _)| k.clone())
            .collect();

        if idle_keys.is_empty() {
            return Ok(Vec::new());
        }

        // Collect retraction data from last_emitted before removing.
        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();

        for key in &idle_keys {
            if let Some(old) = self.last_emitted.remove(key) {
                retract_keys.push(key.clone());
                retract_vals.push(old);
            }
            self.groups.remove(key);
        }

        if retract_keys.is_empty() {
            return Ok(Vec::new());
        }

        let weights = vec![-1i64; retract_keys.len()];
        let batch = build_weighted_batch(
            &retract_keys,
            &retract_vals,
            &weights,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        )?;
        Ok(vec![batch])
    }

    /// Process a pre-aggregation batch.
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

        // Group row indices by key for batch accumulator updates.
        let estimated_groups = (batch.num_rows() / 4).max(16);
        let mut group_indices: FxHashMap<arrow::row::OwnedRow, Vec<u32>> =
            FxHashMap::with_capacity_and_hasher(estimated_groups, rustc_hash::FxBuildHasher);
        for row_idx in 0..batch.num_rows() {
            #[allow(clippy::cast_possible_truncation)]
            group_indices
                .entry(rows.row(row_idx).owned())
                .or_default()
                .push(row_idx as u32);
        }

        for (row_key, indices) in &group_indices {
            if !self.groups.contains_key(row_key) {
                if self.groups.len() >= self.max_groups {
                    tracing::warn!(
                        max_groups = self.max_groups,
                        current_groups = self.groups.len(),
                        "group cardinality limit reached, dropping new group"
                    );
                    continue;
                }
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
                    row_key.clone(),
                    GroupEntry {
                        accs,
                        last_updated_ms: watermark_ms,
                    },
                );
            }
            let Some(entry) = self.groups.get_mut(row_key) else {
                continue;
            };
            Self::update_group_accumulators(
                &mut entry.accs,
                batch,
                indices,
                &self.agg_specs,
                self.weight_col_idx,
            )?;
            entry.last_updated_ms = watermark_ms;
        }

        Ok(())
    }

    /// Fast path for global aggregates (COUNT(*), SUM(x), etc. with no GROUP BY).
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
        Self::update_group_accumulators(
            &mut entry.accs,
            batch,
            &all_indices,
            &self.agg_specs,
            self.weight_col_idx,
        )
    }

    /// Update accumulators for a single group given the row indices.
    ///
    /// Takes a batch and a slice of row indices, does a single `take()` per
    /// column per accumulator, and feeds the resulting sub-arrays to
    /// `Accumulator::update_batch`. This avoids per-row allocation.
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

            // Apply per-accumulator FILTER mask.
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
                    // Also filter weight array through the same mask.
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

            // Retractable accumulators receive weight as the last input.
            if let Some(w) = &filtered_weight {
                input_arrays.push(Arc::clone(w));
            }

            accs[i]
                .update_batch(&input_arrays)
                .map_err(|e| DbError::Pipeline(format!("accumulator update: {e}")))?;
        }
        Ok(())
    }

    /// Emit the current aggregate state as a `RecordBatch`.
    ///
    /// Does NOT reset the accumulators — they continue accumulating for the
    /// next cycle (running totals).
    pub fn emit(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        if self.emit_changelog {
            return self.emit_changelog_delta();
        }
        self.emit_running_state()
    }

    /// Emit full running state (all groups, every cycle). No __weight column.
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

    /// Emit Z-set delta: only changed/new/deleted groups with __weight column.
    fn emit_changelog_delta(&mut self) -> Result<Vec<RecordBatch>, DbError> {
        // Collect retractions and inserts.
        let mut retract_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut retract_vals: Vec<Vec<ScalarValue>> = Vec::new();
        let mut insert_keys: Vec<arrow::row::OwnedRow> = Vec::new();
        let mut insert_vals: Vec<Vec<ScalarValue>> = Vec::new();

        for (key, entry) in &mut self.groups {
            let current: Vec<ScalarValue> = entry
                .accs
                .iter_mut()
                .map(|a| a.evaluate())
                .collect::<Result<_, _>>()
                .map_err(|e| DbError::Pipeline(format!("accumulator evaluate: {e}")))?;

            if let Some(old) = self.last_emitted.get(key) {
                if old != &current {
                    retract_keys.push(key.clone());
                    retract_vals.push(old.clone());
                    insert_keys.push(key.clone());
                    insert_vals.push(current.clone());
                    self.last_emitted.insert(key.clone(), current);
                }
                // Unchanged: skip (true delta semantics).
            } else {
                insert_keys.push(key.clone());
                insert_vals.push(current.clone());
                self.last_emitted.insert(key.clone(), current);
            }
        }

        // Detect deleted groups (in last_emitted but not in groups).
        let deleted: Vec<arrow::row::OwnedRow> = self
            .last_emitted
            .keys()
            .filter(|k| !self.groups.contains_key(*k))
            .cloned()
            .collect();
        for key in &deleted {
            let old = self.last_emitted.remove(key).unwrap();
            retract_keys.push(key.clone());
            retract_vals.push(old);
        }

        let total = retract_keys.len() + insert_keys.len();
        if total == 0 {
            return Ok(Vec::new());
        }

        // Build batch: retracts first (-1), then inserts (+1).
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

        let batch = build_weighted_batch(
            &all_keys,
            &all_vals,
            &weights,
            &self.row_converter,
            self.num_group_cols,
            &self.agg_specs,
            &self.output_schema,
        )?;
        Ok(vec![batch])
    }

    /// Pre-aggregation SQL.
    #[allow(dead_code)] // Accessed in tests and available for diagnostics.
    pub fn pre_agg_sql(&self) -> &str {
        &self.pre_agg_sql
    }

    /// Compiled HAVING filter, if any.
    pub fn having_filter(&self) -> Option<&Arc<dyn PhysicalExpr>> {
        self.having_filter.as_ref()
    }

    /// HAVING predicate SQL fallback (when compiled filter is not available).
    pub fn having_sql(&self) -> Option<&str> {
        self.having_sql.as_deref()
    }

    /// Compiled pre-agg projection (single-source queries only).
    pub fn compiled_projection(&self) -> Option<&CompiledProjection> {
        self.compiled_projection.as_ref()
    }

    /// Cached optimized logical plan for the pre-agg SQL.
    ///
    /// Present only for multi-source queries (where `compiled_projection` is
    /// `None`). Allows skipping SQL parsing and logical optimization per cycle.
    pub fn cached_pre_agg_plan(&self) -> Option<&LogicalPlan> {
        self.cached_pre_agg_plan.as_ref()
    }

    /// Compute a fingerprint for this query (SQL + schema).
    pub(crate) fn query_fingerprint(&self) -> u64 {
        query_fingerprint(&self.pre_agg_sql, &self.output_schema)
    }

    /// Estimated memory usage in bytes across all groups.
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let mut total = 0;
        for (key, entry) in &self.groups {
            total += key.as_ref().len();
            for acc in &entry.accs {
                total += acc.size();
            }
        }
        total
    }

    /// Number of distinct groups currently tracked.
    #[allow(dead_code)]
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Checkpoint all group states into a serializable struct.
    pub(crate) fn checkpoint_groups(&mut self) -> Result<AggStateCheckpoint, DbError> {
        let fingerprint = self.query_fingerprint();
        let mut groups = Vec::with_capacity(self.groups.len());
        for (row_key, entry) in &mut self.groups {
            let sv_key =
                row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
            let key_json: Vec<serde_json::Value> = sv_key.iter().map(scalar_to_json).collect();
            let mut acc_states = Vec::with_capacity(entry.accs.len());
            for acc in &mut entry.accs {
                let state = acc
                    .state()
                    .map_err(|e| DbError::Pipeline(format!("accumulator state: {e}")))?;
                acc_states.push(state.iter().map(scalar_to_json).collect());
            }
            groups.push(GroupCheckpoint {
                key: key_json,
                acc_states,
                last_updated_ms: entry.last_updated_ms,
            });
        }
        // Serialize last_emitted for changelog recovery.
        let mut last_emitted_cp = Vec::new();
        if self.emit_changelog {
            for (row_key, vals) in &self.last_emitted {
                let sv_key =
                    row_to_scalar_key_with_types(&self.row_converter, row_key, &self.group_types)?;
                last_emitted_cp.push(EmittedCheckpoint {
                    key: sv_key.iter().map(scalar_to_json).collect(),
                    values: vals.iter().map(scalar_to_json).collect(),
                });
            }
        }

        Ok(AggStateCheckpoint {
            fingerprint,
            groups,
            last_emitted: last_emitted_cp,
        })
    }

    /// Restore group states from a checkpoint.
    ///
    /// Creates fresh accumulators and restores their state via
    /// `merge_batch()` with single-element arrays derived from the
    /// checkpointed `ScalarValue`s.
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
        self.groups.clear();
        for gc in &checkpoint.groups {
            // Decode JSON → ScalarValue → Array → OwnedRow.
            let sv_key: Result<Vec<ScalarValue>, _> = gc.key.iter().map(json_to_scalar).collect();
            let sv_key = sv_key?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;

            let mut accs = Vec::with_capacity(self.agg_specs.len());
            for (i, spec) in self.agg_specs.iter().enumerate() {
                let mut acc = if self.weight_col_idx.is_some() {
                    spec.create_retractable_accumulator()?
                } else {
                    spec.create_accumulator()?
                };
                if i < gc.acc_states.len() {
                    let state_scalars: Result<Vec<ScalarValue>, _> =
                        gc.acc_states[i].iter().map(json_to_scalar).collect();
                    let state_scalars = state_scalars?;
                    let arrays: Vec<ArrayRef> = state_scalars
                        .iter()
                        .map(|sv| {
                            sv.to_array()
                                .map_err(|e| DbError::Pipeline(format!("scalar to array: {e}")))
                        })
                        .collect::<Result<_, _>>()?;
                    acc.merge_batch(&arrays)
                        .map_err(|e| DbError::Pipeline(format!("accumulator merge: {e}")))?;
                }
                accs.push(acc);
            }
            self.groups.insert(
                row_key,
                GroupEntry {
                    accs,
                    last_updated_ms: gc.last_updated_ms,
                },
            );
        }

        // Restore last_emitted for changelog recovery.
        self.last_emitted.clear();
        for ec in &checkpoint.last_emitted {
            let sv_key: Result<Vec<ScalarValue>, _> = ec.key.iter().map(json_to_scalar).collect();
            let sv_key = sv_key?;
            let row_key = scalar_key_to_owned_row(&self.row_converter, &sv_key, &self.group_types)?;
            let vals: Result<Vec<ScalarValue>, _> = ec.values.iter().map(json_to_scalar).collect();
            self.last_emitted.insert(row_key, vals?);
        }

        Ok(checkpoint.groups.len())
    }
}

// ── Plan introspection helpers ─────────────────────────────────────────

/// Result of finding an aggregate node in a logical plan.
pub(crate) struct AggregateInfo {
    pub(crate) group_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) aggr_exprs: Vec<datafusion_expr::Expr>,
    pub(crate) schema: Arc<Schema>,
    /// Input schema (pre-widening) for resolving aggregate argument types.
    pub(crate) input_schema: Arc<Schema>,
    /// HAVING predicate (from a Filter node directly above the Aggregate).
    pub(crate) having_predicate: Option<datafusion_expr::Expr>,
    /// `DFSchema` of the Aggregate's input (for compiling pre-agg expressions).
    pub(crate) input_df_schema: Arc<DFSchema>,
    /// WHERE predicate from a Filter node below the Aggregate, if any.
    pub(crate) where_predicate: Option<datafusion_expr::Expr>,
}

/// Walk a `DataFusion` `LogicalPlan` tree to find the first `Aggregate` node.
pub(crate) fn find_aggregate(plan: &LogicalPlan) -> Option<AggregateInfo> {
    find_aggregate_inner(plan, None)
}

fn find_aggregate_inner(
    plan: &LogicalPlan,
    parent_filter: Option<&datafusion_expr::Expr>,
) -> Option<AggregateInfo> {
    match plan {
        LogicalPlan::Aggregate(agg) => {
            let schema = Arc::new(agg.schema.as_arrow().clone());
            let input_schema = Arc::new(agg.input.schema().as_arrow().clone());
            let input_df_schema = Arc::clone(agg.input.schema());
            let where_predicate = extract_where_predicate(&agg.input);
            Some(AggregateInfo {
                group_exprs: agg.group_expr.clone(),
                aggr_exprs: agg.aggr_expr.clone(),
                schema,
                input_schema,
                having_predicate: parent_filter.cloned(),
                input_df_schema,
                where_predicate,
            })
        }
        // A Filter directly above an Aggregate is a HAVING clause
        LogicalPlan::Filter(filter) => {
            if matches!(&*filter.input, LogicalPlan::Aggregate(_)) {
                find_aggregate_inner(&filter.input, Some(&filter.predicate))
            } else {
                find_aggregate_inner(&filter.input, None)
            }
        }
        // Walk through wrappers that don't change aggregation semantics
        LogicalPlan::Projection(proj) => find_aggregate_inner(&proj.input, None),
        LogicalPlan::Sort(sort) => find_aggregate_inner(&sort.input, None),
        LogicalPlan::Limit(limit) => find_aggregate_inner(&limit.input, None),
        LogicalPlan::SubqueryAlias(alias) => find_aggregate_inner(&alias.input, None),
        _ => {
            for input in plan.inputs() {
                if let Some(result) = find_aggregate_inner(input, None) {
                    return Some(result);
                }
            }
            None
        }
    }
}

/// Extract the WHERE predicate from a plan tree below the Aggregate.
///
/// Walks through common unary wrapper nodes (Projection, Sort, Limit,
/// `SubqueryAlias`) to find a `Filter` that the optimizer may have placed
/// below intermediate nodes.
fn extract_where_predicate(plan: &LogicalPlan) -> Option<datafusion_expr::Expr> {
    match plan {
        LogicalPlan::Filter(f) => Some(f.predicate.clone()),
        LogicalPlan::Projection(p) => extract_where_predicate(&p.input),
        LogicalPlan::Sort(s) => extract_where_predicate(&s.input),
        LogicalPlan::Limit(l) => extract_where_predicate(&l.input),
        LogicalPlan::SubqueryAlias(a) => extract_where_predicate(&a.input),
        _ => None,
    }
}

/// Convert a `ScalarValue` to a SQL-safe literal string.
fn scalar_value_to_sql(sv: &ScalarValue) -> String {
    match sv {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Utf8View(None)
        | ScalarValue::Null
        | ScalarValue::Boolean(None) => "NULL".to_string(),
        ScalarValue::Boolean(Some(b)) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        ScalarValue::IntervalDayTime(Some(v)) => {
            let mut parts = Vec::new();
            if v.days != 0 {
                parts.push(format!("{} days", v.days));
            }
            if v.milliseconds != 0 || parts.is_empty() {
                let abs_ms = v.milliseconds.unsigned_abs();
                let secs = abs_ms / 1000;
                let frac = abs_ms % 1000;
                let sign = if v.milliseconds < 0 { "-" } else { "" };
                if frac == 0 {
                    parts.push(format!("{sign}{secs} seconds"));
                } else {
                    parts.push(format!("{sign}{secs}.{frac:03} seconds"));
                }
            }
            format!("INTERVAL '{}'", parts.join(" "))
        }
        ScalarValue::IntervalYearMonth(Some(v)) => {
            let years = v / 12;
            let months = v % 12;
            let mut parts = Vec::new();
            if years != 0 {
                parts.push(format!("{years} years"));
            }
            if months != 0 || parts.is_empty() {
                parts.push(format!("{months} months"));
            }
            format!("INTERVAL '{}'", parts.join(" "))
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            let mut parts = Vec::new();
            if v.months != 0 {
                parts.push(format!("{} months", v.months));
            }
            if v.days != 0 {
                parts.push(format!("{} days", v.days));
            }
            let nanos = v.nanoseconds;
            if nanos != 0 || parts.is_empty() {
                let abs_ns = nanos.unsigned_abs();
                let secs = abs_ns / 1_000_000_000;
                let remainder_ns = abs_ns % 1_000_000_000;
                let sign = if nanos < 0 { "-" } else { "" };
                if remainder_ns == 0 {
                    parts.push(format!("{sign}{secs} seconds"));
                } else {
                    let millis = remainder_ns / 1_000_000;
                    parts.push(format!("{sign}{secs}.{millis:03} seconds"));
                }
            }
            format!("INTERVAL '{}'", parts.join(" "))
        }
        _ => sv.to_string(),
    }
}

/// Convert a `DataFusion` CASE expression to SQL.
fn case_to_sql(case: &datafusion_expr::expr::Case) -> String {
    use std::fmt::Write;
    let mut sql = String::from("CASE");
    if let Some(operand) = &case.expr {
        let _ = write!(sql, " {}", expr_to_sql(operand));
    }
    for (when_expr, then_expr) in &case.when_then_expr {
        let _ = write!(
            sql,
            " WHEN {} THEN {}",
            expr_to_sql(when_expr),
            expr_to_sql(then_expr)
        );
    }
    if let Some(else_expr) = &case.else_expr {
        let _ = write!(sql, " ELSE {}", expr_to_sql(else_expr));
    }
    sql.push_str(" END");
    sql
}

/// Convert a `DataFusion` `Expr` to a SQL string for use in pre-aggregation
/// queries.
pub(crate) fn expr_to_sql(expr: &datafusion_expr::Expr) -> String {
    use datafusion_expr::Expr;
    match expr {
        Expr::Column(col) => format!("\"{}\"", col.name),
        Expr::Literal(sv, _) => scalar_value_to_sql(sv),
        Expr::Alias(alias) => expr_to_sql(&alias.expr),
        Expr::BinaryExpr(bin) => {
            let left = expr_to_sql(&bin.left);
            let right = expr_to_sql(&bin.right);
            format!("({left} {op} {right})", op = bin.op)
        }
        Expr::Cast(cast) => {
            let inner = expr_to_sql(&cast.expr);
            format!("CAST({inner} AS {})", cast.data_type)
        }
        Expr::TryCast(cast) => {
            let inner = expr_to_sql(&cast.expr);
            format!("TRY_CAST({inner} AS {})", cast.data_type)
        }
        Expr::ScalarFunction(func) => {
            let args: Vec<String> = func.args.iter().map(expr_to_sql).collect();
            format!("{}({})", func.func.name(), args.join(", "))
        }
        Expr::AggregateFunction(agg) => {
            let name = agg.func.name();
            let args: Vec<String> = agg.params.args.iter().map(expr_to_sql).collect();
            if agg.params.distinct {
                format!("{name}(DISTINCT {})", args.join(", "))
            } else {
                format!("{name}({})", args.join(", "))
            }
        }
        Expr::Case(case) => case_to_sql(case),
        Expr::Not(inner) => format!("(NOT {})", expr_to_sql(inner)),
        Expr::Negative(inner) => format!("(-{})", expr_to_sql(inner)),
        Expr::IsNull(inner) => {
            format!("({} IS NULL)", expr_to_sql(inner))
        }
        Expr::IsNotNull(inner) => {
            format!("({} IS NOT NULL)", expr_to_sql(inner))
        }
        Expr::IsTrue(inner) => {
            format!("({} IS TRUE)", expr_to_sql(inner))
        }
        Expr::IsFalse(inner) => {
            format!("({} IS FALSE)", expr_to_sql(inner))
        }
        Expr::IsNotTrue(inner) => {
            format!("({} IS NOT TRUE)", expr_to_sql(inner))
        }
        Expr::IsNotFalse(inner) => {
            format!("({} IS NOT FALSE)", expr_to_sql(inner))
        }
        Expr::Between(between) => {
            let e = expr_to_sql(&between.expr);
            let low = expr_to_sql(&between.low);
            let high = expr_to_sql(&between.high);
            let not = if between.negated { " NOT" } else { "" };
            format!("({e}{not} BETWEEN {low} AND {high})")
        }
        Expr::InList(in_list) => {
            let e = expr_to_sql(&in_list.expr);
            let items: Vec<String> = in_list.list.iter().map(expr_to_sql).collect();
            let not = if in_list.negated { " NOT" } else { "" };
            format!("({e}{not} IN ({}))", items.join(", "))
        }
        Expr::Like(like) => {
            let e = expr_to_sql(&like.expr);
            let pat = expr_to_sql(&like.pattern);
            let kw = if like.case_insensitive {
                "ILIKE"
            } else {
                "LIKE"
            };
            let not = if like.negated { " NOT" } else { "" };
            if let Some(esc) = &like.escape_char {
                format!("({e}{not} {kw} {pat} ESCAPE '{esc}')")
            } else {
                format!("({e}{not} {kw} {pat})")
            }
        }
        #[allow(deprecated)]
        Expr::Wildcard { .. } => "TRUE".to_string(),
        other => other.to_string(),
    }
}

/// Pre-compiled projection for evaluating pre-agg expressions without SQL.
///
/// Replaces the `ctx.sql(pre_agg_sql).await.collect().await` hot-path call
/// with direct `PhysicalExpr::evaluate()` on the source batch, eliminating
/// SQL parsing, logical planning, physical planning, and `MemTable` overhead.
#[allow(dead_code)] // source_table used by legacy evaluate_compiled_projection path
pub(crate) struct CompiledProjection {
    /// Source table name (for looking up raw batches instead of querying `MemTable`).
    pub(crate) source_table: String,
    /// One `PhysicalExpr` per column in the output (group keys, agg inputs, filter bools).
    pub(crate) exprs: Vec<Arc<dyn PhysicalExpr>>,
    /// WHERE predicate (applied before projection), if any.
    pub(crate) filter: Option<Arc<dyn PhysicalExpr>>,
    /// Output schema matching what `process_batch` / `update_batch` expects.
    pub(crate) output_schema: SchemaRef,
}

impl CompiledProjection {
    /// Source table name.
    pub(crate) fn source_table(&self) -> &str {
        &self.source_table
    }

    /// Evaluate the projection against a source batch.
    ///
    /// Applies the WHERE filter (if any), then evaluates each expression
    /// to produce the projected output batch.
    pub(crate) fn evaluate(&self, batch: &RecordBatch) -> Result<RecordBatch, DbError> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        // Apply WHERE filter first
        let filtered = if let Some(ref filter) = self.filter {
            let result = filter
                .evaluate(batch)
                .map_err(|e| DbError::Pipeline(format!("WHERE filter evaluate: {e}")))?;
            let mask = result
                .into_array(batch.num_rows())
                .map_err(|e| DbError::Pipeline(format!("WHERE filter to array: {e}")))?;
            let bool_arr = mask
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| DbError::Pipeline("WHERE filter not boolean".into()))?;
            arrow::compute::filter_record_batch(batch, bool_arr)
                .map_err(|e| DbError::Pipeline(format!("WHERE filter: {e}")))?
        } else {
            batch.clone()
        };

        if filtered.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.output_schema)));
        }

        // Evaluate each projection expression
        let mut arrays = Vec::with_capacity(self.exprs.len());
        for expr in &self.exprs {
            let result = expr
                .evaluate(&filtered)
                .map_err(|e| DbError::Pipeline(format!("projection evaluate: {e}")))?;
            let arr = result
                .into_array(filtered.num_rows())
                .map_err(|e| DbError::Pipeline(format!("projection to array: {e}")))?;
            arrays.push(arr);
        }

        RecordBatch::try_new(Arc::clone(&self.output_schema), arrays)
            .map_err(|e| DbError::Pipeline(format!("projection batch build: {e}")))
    }
}

/// Apply a compiled HAVING filter to emitted aggregate batches.
///
/// Evaluates the `PhysicalExpr` predicate against each batch and filters rows.
pub(crate) fn apply_compiled_having(
    batches: &[RecordBatch],
    having_filter: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<RecordBatch>, DbError> {
    let mut result = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let mask_result = having_filter
            .evaluate(batch)
            .map_err(|e| DbError::Pipeline(format!("HAVING evaluate: {e}")))?;
        let mask = mask_result
            .into_array(batch.num_rows())
            .map_err(|e| DbError::Pipeline(format!("HAVING to array: {e}")))?;
        let bool_arr = mask
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| DbError::Pipeline("HAVING filter not boolean".into()))?;
        let filtered = arrow::compute::filter_record_batch(batch, bool_arr)
            .map_err(|e| DbError::Pipeline(format!("HAVING filter: {e}")))?;
        if filtered.num_rows() > 0 {
            result.push(filtered);
        }
    }
    Ok(result)
}

/// Compile a HAVING predicate to a `PhysicalExpr`.
///
/// Returns `None` if compilation fails (caller should fall back to SQL).
pub(crate) fn compile_having_filter(
    ctx: &SessionContext,
    having_predicate: Option<&datafusion_expr::Expr>,
    output_schema: &SchemaRef,
) -> Option<Arc<dyn PhysicalExpr>> {
    let having_pred = having_predicate?;
    let df_schema = DFSchema::try_from(output_schema.as_ref().clone()).ok()?;
    let state = ctx.state();
    let props = state.execution_props();
    create_physical_expr(having_pred, &df_schema, props).ok()
}

/// Extracted FROM and WHERE clauses from a SQL query.
pub(crate) struct SqlClauses {
    /// The FROM clause (table references, joins, etc.)
    pub(crate) from_clause: String,
    /// The WHERE clause including the `WHERE` keyword, or empty string.
    pub(crate) where_clause: String,
}

/// Extract FROM and WHERE clauses from a SQL string using the `sqlparser`
/// AST. Falls back to heuristic string extraction if parsing fails.
pub(crate) fn extract_clauses(sql: &str) -> SqlClauses {
    if let Ok(clauses) = extract_clauses_ast(sql) {
        return clauses;
    }
    // Fallback: heuristic extraction for non-standard SQL
    SqlClauses {
        from_clause: extract_from_clause_heuristic(sql),
        where_clause: extract_where_clause_heuristic(sql),
    }
}

/// AST-based clause extraction using `sqlparser`.
fn extract_clauses_ast(sql: &str) -> Result<SqlClauses, DbError> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    let dialect = GenericDialect {};
    let stmts = Parser::parse_sql(&dialect, sql)
        .map_err(|e| DbError::Pipeline(format!("SQL parse error: {e}")))?;

    let stmt = stmts
        .into_iter()
        .next()
        .ok_or_else(|| DbError::Pipeline("empty SQL statement".to_string()))?;

    let sqlparser::ast::Statement::Query(query) = stmt else {
        return Err(DbError::Pipeline("expected SELECT statement".to_string()));
    };

    let sqlparser::ast::SetExpr::Select(select) = *query.body else {
        return Err(DbError::Pipeline("expected simple SELECT".to_string()));
    };

    let from_clause = select
        .from
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");

    let where_clause = select
        .selection
        .as_ref()
        .map(|expr| format!(" WHERE {expr}"))
        .unwrap_or_default();

    Ok(SqlClauses {
        from_clause,
        where_clause,
    })
}

/// Heuristic FROM clause extraction (fallback).
fn extract_from_clause_heuristic(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let from_pos = upper.find(" FROM ").map(|p| p + 6);
    let Some(start) = from_pos else {
        return String::new();
    };
    let rest = &sql[start..];
    let end_keywords = [" WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "];
    let end = end_keywords
        .iter()
        .filter_map(|kw| rest.to_uppercase().find(kw))
        .min()
        .unwrap_or(rest.len());
    rest[..end].trim().to_string()
}

/// Heuristic WHERE clause extraction (fallback).
fn extract_where_clause_heuristic(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let where_pos = upper.find(" WHERE ");
    let Some(start) = where_pos else {
        return String::new();
    };
    let rest = &sql[start..];
    let end_keywords = [" GROUP ", " ORDER ", " LIMIT ", " HAVING "];
    let end = end_keywords
        .iter()
        .filter_map(|kw| rest[7..].to_uppercase().find(kw).map(|p| p + 7))
        .min()
        .unwrap_or(rest.len());
    format!(" {}", rest[..end].trim())
}

/// Resolve the data type of a `DataFusion` expression against the
/// aggregate node's input schema. This gives the true pre-widening
/// type (e.g., `Int32` for a column that SUM widens to `Int64`).
pub(crate) fn resolve_expr_type(
    expr: &datafusion_expr::Expr,
    input_schema: &Schema,
    fallback_type: &DataType,
) -> DataType {
    match expr {
        datafusion_expr::Expr::Column(col) => input_schema
            .field_with_name(&col.name)
            .map_or_else(|_| fallback_type.clone(), |f| f.data_type().clone()),
        datafusion_expr::Expr::Literal(sv, _) => sv.data_type(),
        datafusion_expr::Expr::Cast(cast) => cast.data_type.clone(),
        datafusion_expr::Expr::TryCast(cast) => cast.data_type.clone(),
        datafusion_expr::Expr::BinaryExpr(bin) => {
            // For arithmetic, the result type is typically the wider
            // of the two operands. Resolve the left side as a
            // reasonable approximation.
            resolve_expr_type(&bin.left, input_schema, fallback_type)
        }
        datafusion_expr::Expr::ScalarFunction(func) => {
            // Try to get return type from input types
            let arg_types: Vec<DataType> = func
                .args
                .iter()
                .map(|a| resolve_expr_type(a, input_schema, fallback_type))
                .collect();
            func.func
                .return_type(&arg_types)
                .unwrap_or_else(|_| fallback_type.clone())
        }
        #[allow(deprecated)]
        datafusion_expr::Expr::Wildcard { .. } => DataType::Boolean,
        _ => fallback_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_day_time_seconds_only() {
        use arrow::datatypes::IntervalDayTime;
        let sv = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(0, 10_000)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '10 seconds'");
    }

    #[test]
    fn test_interval_day_time_days_only() {
        use arrow::datatypes::IntervalDayTime;
        let sv = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(3, 0)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '3 days'");
    }

    #[test]
    fn test_interval_day_time_mixed() {
        use arrow::datatypes::IntervalDayTime;
        let sv = ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(1, 5_500)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '1 days 5.500 seconds'");
    }

    #[test]
    fn test_interval_year_month() {
        let sv = ScalarValue::IntervalYearMonth(Some(15));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '1 years 3 months'");
    }

    #[test]
    fn test_interval_month_day_nano() {
        use arrow::datatypes::IntervalMonthDayNano;
        let sv =
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(2, 1, 3_000_000_000)));
        let sql = scalar_value_to_sql(&sv);
        assert_eq!(sql, "INTERVAL '2 months 1 days 3 seconds'");
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
        let state = IncrementalAggState::try_from_sql(&ctx, sql, false)
            .await
            .unwrap()
            .expect("expected aggregate state");
        (ctx, state)
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

    // ── Type inference tests ────────────────────────────────────────────

    #[tokio::test]
    async fn test_type_inference_preserves_source_int32() {
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

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, SUM(amount) as total FROM orders GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // Input type should be Int32 (source type), NOT Int64 (widened)
        assert_eq!(
            state.agg_specs[0].input_types[0],
            DataType::Int32,
            "SUM(int32_col) input type should be Int32, got {:?}",
            state.agg_specs[0].input_types[0]
        );
    }

    #[tokio::test]
    async fn test_type_inference_preserves_source_float32() {
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

        let state = IncrementalAggState::try_from_sql(
            &ctx,
            "SELECT name, AVG(price) as avg_price FROM products GROUP BY name",
            false,
        )
        .await
        .unwrap()
        .expect("expected aggregate state");

        // AVG input should be Float32 (source), not Float64 (widened)
        assert_eq!(
            state.agg_specs[0].input_types[0],
            DataType::Float32,
            "AVG(float32_col) input type should be Float32, got {:?}",
            state.agg_specs[0].input_types[0]
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

    // ── AST extraction tests ────────────────────────────────────────────

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

    // ── expr_to_sql coverage ────────────────────────────────────────

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

    // ── GROUP BY expression tests ───────────────────────────────────

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

    // ── Group cardinality limit ────────────────────────────────────

    #[tokio::test]
    async fn test_group_cardinality_limit_enforced() {
        let (_, mut state) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;

        // Set a very small limit for testing
        state.max_groups = 3;

        // Feed 5 unique groups
        let pre_agg_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__agg_input_1", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    "a", "b", "c", "d", "e",
                ])),
                Arc::new(arrow::array::Float64Array::from(vec![
                    1.0, 2.0, 3.0, 4.0, 5.0,
                ])),
            ],
        )
        .unwrap();
        state.process_batch(&batch, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            result[0].num_rows() <= 3,
            "should have at most 3 groups, got {}",
            result[0].num_rows()
        );
    }

    #[tokio::test]
    async fn test_group_cardinality_existing_groups_still_updated() {
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

        // Batch 2: update existing groups + attempt new group "c"
        let batch2 = RecordBatch::try_new(
            Arc::clone(&pre_agg_schema),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "c"])),
                Arc::new(arrow::array::Float64Array::from(vec![5.0, 100.0])),
            ],
        )
        .unwrap();
        state.process_batch(&batch2, i64::MIN).unwrap();

        let result = state.emit().unwrap();
        assert_eq!(result[0].num_rows(), 2, "still only 2 groups");

        // Group "a" should have 10+5=15
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
            if names.value(i) == "a" {
                assert!(
                    (totals.value(i) - 15.0).abs() < f64::EPSILON,
                    "group 'a' should be 15, got {}",
                    totals.value(i)
                );
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

    // ── Checkpoint/restore tests ─────────────────────────────────────

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_scalar_to_json_roundtrip() {
        let cases: Vec<ScalarValue> = vec![
            ScalarValue::Null,
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(None),
            ScalarValue::Int8(Some(42)),
            ScalarValue::Int16(Some(-100)),
            ScalarValue::Int32(Some(999)),
            ScalarValue::Int64(Some(123_456_789)),
            ScalarValue::Int64(None),
            ScalarValue::UInt8(Some(255)),
            ScalarValue::UInt16(Some(65535)),
            ScalarValue::UInt32(Some(1_000_000)),
            ScalarValue::UInt64(Some(9_999_999_999)),
            ScalarValue::UInt64(None),
            ScalarValue::Float32(Some(1.5)),
            ScalarValue::Float64(Some(9.876_54)),
            ScalarValue::Float64(None),
            ScalarValue::Utf8(Some("hello world".to_string())),
            ScalarValue::Utf8(None),
        ];

        for original in &cases {
            let json_val = scalar_to_json(original);
            let restored = json_to_scalar(&json_val).unwrap();
            // Compare by evaluate — widened types match on value
            let orig_str = format!("{original:?}");
            let rest_str = format!("{restored:?}");
            match original {
                ScalarValue::Null => {
                    assert!(
                        matches!(restored, ScalarValue::Null),
                        "{orig_str} != {rest_str}"
                    );
                }
                ScalarValue::Boolean(v) => {
                    assert_eq!(
                        *v,
                        match restored {
                            ScalarValue::Boolean(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::Int8(Some(n)) => {
                    assert_eq!(
                        Some(i64::from(*n)),
                        match restored {
                            ScalarValue::Int64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::Int16(Some(n)) => {
                    assert_eq!(
                        Some(i64::from(*n)),
                        match restored {
                            ScalarValue::Int64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::Int32(Some(n)) => {
                    assert_eq!(
                        Some(i64::from(*n)),
                        match restored {
                            ScalarValue::Int64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::Int64(v) => {
                    assert_eq!(
                        *v,
                        match restored {
                            ScalarValue::Int64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::UInt8(Some(n)) => {
                    assert_eq!(
                        Some(u64::from(*n)),
                        match restored {
                            ScalarValue::UInt64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::UInt16(Some(n)) => {
                    assert_eq!(
                        Some(u64::from(*n)),
                        match restored {
                            ScalarValue::UInt64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::UInt32(Some(n)) => {
                    assert_eq!(
                        Some(u64::from(*n)),
                        match restored {
                            ScalarValue::UInt64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::UInt64(v) => {
                    assert_eq!(
                        *v,
                        match restored {
                            ScalarValue::UInt64(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                ScalarValue::Float32(Some(f)) => {
                    let ScalarValue::Float64(restored_f) = restored else {
                        panic!("type mismatch: {rest_str}")
                    };
                    assert!(
                        (f64::from(*f) - restored_f.unwrap()).abs() < 1e-6,
                        "{orig_str} != {rest_str}"
                    );
                }
                ScalarValue::Float64(v) => {
                    let ScalarValue::Float64(restored_f) = restored else {
                        panic!("type mismatch: {rest_str}")
                    };
                    assert_eq!(*v, restored_f, "{orig_str} != {rest_str}");
                }
                ScalarValue::Utf8(v) => {
                    assert_eq!(
                        *v,
                        match restored {
                            ScalarValue::Utf8(r) => r,
                            _ => panic!("type mismatch: {rest_str}"),
                        }
                    );
                }
                _ => {}
            }
        }
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
        assert_eq!(cp.groups.len(), 1);

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
        assert_eq!(cp.groups.len(), 3);

        let (_, mut state2) =
            setup_agg_state("SELECT name, SUM(value) as total FROM events GROUP BY name").await;
        let restored = state2.restore_groups(&cp).unwrap();
        assert_eq!(restored, 3);

        let result = state2.emit().unwrap();
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_checkpoint_empty_state_returns_none() {
        use crate::stream_executor::StreamExecutor;
        let ctx = laminar_sql::create_session_context();
        laminar_sql::register_streaming_functions(&ctx);
        let mut executor = StreamExecutor::new(ctx);
        // No queries registered = no state
        let result = executor.snapshot_state().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_restore_corrupt_bytes_returns_error() {
        use crate::stream_executor::StreamExecutor;
        let ctx = laminar_sql::create_session_context();
        let mut executor = StreamExecutor::new(ctx);
        let result = executor.restore_state(b"not valid json");
        assert!(result.is_err());
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

    #[test]
    fn test_checkpoint_backward_compat_without_join_states() {
        // Verify that checkpoints serialized before join_states was added
        // still deserialize correctly (serde(default) handles it).
        let json = r#"{
            "version": 1,
            "agg_states": {},
            "eowc_states": {},
            "core_window_states": {}
        }"#;
        let cp: StreamExecutorCheckpoint = serde_json::from_str(json).unwrap();
        assert_eq!(cp.version, 1);
        assert!(cp.join_states.is_empty());
    }

    #[test]
    fn test_checkpoint_with_join_states_round_trip() {
        let mut join_states = FxHashMap::default();
        join_states.insert(
            "enriched".to_string(),
            JoinStateCheckpoint {
                left_buffer_rows: 100,
                right_buffer_rows: 50,
                left_batches: vec![vec![1, 2, 3]],
                right_batches: vec![vec![4, 5, 6]],
                last_evicted_watermark: 42,
                last_evicted_watermark_right: 42,
            },
        );

        let cp = StreamExecutorCheckpoint {
            version: 2,
            vnode_count: laminar_storage::checkpoint_manifest::VNODE_COUNT,
            agg_states: FxHashMap::default(),
            eowc_states: FxHashMap::default(),
            core_window_states: FxHashMap::default(),
            join_states,
            raw_eowc_states: FxHashMap::default(),
        };

        let bytes = serde_json::to_vec(&cp).unwrap();
        let restored: StreamExecutorCheckpoint = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(restored.join_states.len(), 1);
        let js = restored.join_states.get("enriched").unwrap();
        assert_eq!(js.left_buffer_rows, 100);
        assert_eq!(js.right_buffer_rows, 50);
        assert_eq!(js.left_batches, vec![vec![1, 2, 3]]);
        assert_eq!(js.right_batches, vec![vec![4, 5, 6]]);
        assert_eq!(js.last_evicted_watermark, 42);
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
}
