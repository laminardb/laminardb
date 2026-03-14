//! Physical execution plan for lookup joins.
//!
//! Bridges `LookupJoinNode` (logical) to a hash-probe executor that
//! joins streaming input against a pre-indexed lookup table snapshot.
//!
//! ## Data flow
//!
//! ```text
//! Stream input ──► LookupJoinExec ──► Output (stream + lookup columns)
//!                       │
//!                  HashIndex probe
//!                       │
//!                  LookupSnapshot (pre-indexed RecordBatch)
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use parking_lot::RwLock;

use std::collections::BTreeMap;

use arrow::compute::take;
use arrow::row::{RowConverter, SortField};
use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Expr;
use futures::StreamExt;
use laminar_core::lookup::foyer_cache::FoyerMemoryCache;
use laminar_core::lookup::source::LookupSourceDyn;
use tokio::sync::Semaphore;

use super::lookup_join::{LookupJoinNode, LookupJoinType};

// ── Registry ─────────────────────────────────────────────────────

/// Thread-safe registry of lookup table entries (snapshot or partial).
///
/// The db layer populates this when `CREATE LOOKUP TABLE` executes;
/// the [`LookupJoinExtensionPlanner`] reads it at physical plan time.
#[derive(Default)]
pub struct LookupTableRegistry {
    tables: RwLock<HashMap<String, RegisteredLookup>>,
}

/// A registered lookup table entry — snapshot, partial (on-demand), or
/// versioned (temporal join with version history).
pub enum RegisteredLookup {
    /// Full snapshot: all rows pre-loaded in a single batch.
    Snapshot(Arc<LookupSnapshot>),
    /// Partial (on-demand): bounded foyer cache with S3-FIFO eviction.
    Partial(Arc<PartialLookupState>),
    /// Versioned: all versions of all keys for temporal joins.
    Versioned(Arc<VersionedLookupState>),
}

/// Point-in-time snapshot of a lookup table for join execution.
pub struct LookupSnapshot {
    /// All rows concatenated into a single batch.
    pub batch: RecordBatch,
    /// Primary key column names used to build the hash index.
    pub key_columns: Vec<String>,
}

/// State for a versioned (temporal) lookup table.
///
/// Holds all versions of all keys in a single `RecordBatch`, plus a
/// pre-built `VersionedIndex` for efficient point-in-time lookups.
/// The index is built once at registration time and rebuilt only when
/// the table is updated via CDC.
pub struct VersionedLookupState {
    /// All rows (all versions) concatenated into a single batch.
    pub batch: RecordBatch,
    /// Pre-built versioned index (built at registration time, not per-cycle).
    pub index: Arc<VersionedIndex>,
    /// Primary key column names for the equi-join.
    pub key_columns: Vec<String>,
    /// Column containing the version timestamp in the table.
    pub version_column: String,
    /// Stream-side column name for event time (the AS OF column).
    pub stream_time_column: String,
}

/// State for a partial (on-demand) lookup table.
pub struct PartialLookupState {
    /// Bounded foyer memory cache with S3-FIFO eviction.
    pub foyer_cache: Arc<FoyerMemoryCache>,
    /// Schema of the lookup table.
    pub schema: SchemaRef,
    /// Key column names for row encoding.
    pub key_columns: Vec<String>,
    /// `SortField` descriptors for key encoding via `RowConverter`.
    pub key_sort_fields: Vec<SortField>,
    /// Async source for cache miss fallback (None = cache-only mode).
    pub source: Option<Arc<dyn LookupSourceDyn>>,
    /// Limits concurrent source queries to avoid overloading the source.
    pub fetch_semaphore: Arc<Semaphore>,
}

impl LookupTableRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers or replaces a lookup table snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn register(&self, name: &str, snapshot: LookupSnapshot) {
        self.tables.write().insert(
            name.to_lowercase(),
            RegisteredLookup::Snapshot(Arc::new(snapshot)),
        );
    }

    /// Registers or replaces a partial (on-demand) lookup table.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn register_partial(&self, name: &str, state: PartialLookupState) {
        self.tables.write().insert(
            name.to_lowercase(),
            RegisteredLookup::Partial(Arc::new(state)),
        );
    }

    /// Registers or replaces a versioned (temporal) lookup table.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn register_versioned(&self, name: &str, state: VersionedLookupState) {
        self.tables.write().insert(
            name.to_lowercase(),
            RegisteredLookup::Versioned(Arc::new(state)),
        );
    }

    /// Removes a lookup table from the registry.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn unregister(&self, name: &str) {
        self.tables.write().remove(&name.to_lowercase());
    }

    /// Returns the current snapshot for a table, if registered as a snapshot.
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<Arc<LookupSnapshot>> {
        let tables = self.tables.read();
        match tables.get(&name.to_lowercase())? {
            RegisteredLookup::Snapshot(s) => Some(Arc::clone(s)),
            RegisteredLookup::Partial(_) | RegisteredLookup::Versioned(_) => None,
        }
    }

    /// Returns the registered lookup entry (snapshot, partial, or versioned).
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn get_entry(&self, name: &str) -> Option<RegisteredLookup> {
        let tables = self.tables.read();
        tables.get(&name.to_lowercase()).map(|e| match e {
            RegisteredLookup::Snapshot(s) => RegisteredLookup::Snapshot(Arc::clone(s)),
            RegisteredLookup::Partial(p) => RegisteredLookup::Partial(Arc::clone(p)),
            RegisteredLookup::Versioned(v) => RegisteredLookup::Versioned(Arc::clone(v)),
        })
    }
}

// ── Hash Index ───────────────────────────────────────────────────

/// Pre-built hash index mapping encoded key bytes to row indices.
struct HashIndex {
    map: HashMap<Box<[u8]>, Vec<u32>>,
}

impl HashIndex {
    /// Builds an index over `key_indices` columns in `batch`.
    ///
    /// Uses Arrow's `RowConverter` for binary-comparable key encoding
    /// so any Arrow data type is handled without manual serialization.
    fn build(batch: &RecordBatch, key_indices: &[usize]) -> Result<Self> {
        if batch.num_rows() == 0 {
            return Ok(Self {
                map: HashMap::new(),
            });
        }

        let sort_fields: Vec<SortField> = key_indices
            .iter()
            .map(|&i| SortField::new(batch.schema().field(i).data_type().clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)?;

        let key_cols: Vec<_> = key_indices
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        let rows = converter.convert_columns(&key_cols)?;

        let num_rows = batch.num_rows();
        let mut map: HashMap<Box<[u8]>, Vec<u32>> = HashMap::with_capacity(num_rows);
        #[allow(clippy::cast_possible_truncation)] // batch row count fits u32
        for i in 0..num_rows {
            map.entry(Box::from(rows.row(i).as_ref()))
                .or_default()
                .push(i as u32);
        }

        Ok(Self { map })
    }

    fn probe(&self, key: &[u8]) -> Option<&[u32]> {
        self.map.get(key).map(Vec::as_slice)
    }
}

// ── Versioned Index ──────────────────────────────────────────────

/// Pre-built versioned index mapping encoded key bytes to a BTreeMap
/// of version timestamps to row indices. Supports point-in-time lookups
/// via `probe_at_time` for temporal joins.
#[derive(Default)]
pub struct VersionedIndex {
    map: HashMap<Box<[u8]>, BTreeMap<i64, Vec<u32>>>,
}

impl VersionedIndex {
    /// Builds a versioned index over `key_indices` and `version_col_idx`
    /// columns in `batch`.
    ///
    /// Uses Arrow's `RowConverter` for binary-comparable key encoding.
    /// Null keys and null version timestamps are skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if key encoding or timestamp extraction fails.
    pub fn build(
        batch: &RecordBatch,
        key_indices: &[usize],
        version_col_idx: usize,
    ) -> Result<Self> {
        if batch.num_rows() == 0 {
            return Ok(Self {
                map: HashMap::new(),
            });
        }

        let sort_fields: Vec<SortField> = key_indices
            .iter()
            .map(|&i| SortField::new(batch.schema().field(i).data_type().clone()))
            .collect();
        let converter = RowConverter::new(sort_fields)?;

        let key_cols: Vec<_> = key_indices
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        let rows = converter.convert_columns(&key_cols)?;

        let timestamps = extract_i64_timestamps(batch.column(version_col_idx))?;

        let num_rows = batch.num_rows();
        let mut map: HashMap<Box<[u8]>, BTreeMap<i64, Vec<u32>>> = HashMap::with_capacity(num_rows);
        #[allow(clippy::cast_possible_truncation)]
        for (i, ts_opt) in timestamps.iter().enumerate() {
            // Skip rows with null keys or null version timestamps.
            let Some(version_ts) = ts_opt else { continue };
            if key_cols.iter().any(|c| c.is_null(i)) {
                continue;
            }
            let key = Box::from(rows.row(i).as_ref());
            map.entry(key)
                .or_default()
                .entry(*version_ts)
                .or_default()
                .push(i as u32);
        }

        Ok(Self { map })
    }

    /// Finds the row index for the latest version `<= event_ts` for the
    /// given key. Returns the last row index at that version.
    fn probe_at_time(&self, key: &[u8], event_ts: i64) -> Option<u32> {
        let versions = self.map.get(key)?;
        let (_, indices) = versions.range(..=event_ts).next_back()?;
        indices.last().copied()
    }
}

/// Extracts `Option<i64>` timestamp values from an Arrow array column.
///
/// Returns `None` for null entries (callers must handle nulls explicitly).
/// Supports `Int64`, all `Timestamp` variants (scaled to milliseconds),
/// and `Float64` (truncated to `i64`).
fn extract_i64_timestamps(col: &dyn arrow_array::Array) -> Result<Vec<Option<i64>>> {
    use arrow_array::{
        Float64Array, Int64Array, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow_schema::{DataType, TimeUnit};

    let n = col.len();
    let mut out = Vec::with_capacity(n);
    macro_rules! extract_typed {
        ($arr_type:ty, $scale:expr) => {{
            let arr = col.as_any().downcast_ref::<$arr_type>().ok_or_else(|| {
                DataFusionError::Internal(concat!("expected ", stringify!($arr_type)).into())
            })?;
            for i in 0..n {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) * $scale)
                });
            }
        }};
    }

    match col.data_type() {
        DataType::Int64 => extract_typed!(Int64Array, 1),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            extract_typed!(TimestampMillisecondArray, 1);
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("expected TimestampMicrosecondArray".into())
                })?;
            for i in 0..n {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) / 1000)
                });
            }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            extract_typed!(TimestampSecondArray, 1000);
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("expected TimestampNanosecondArray".into())
                })?;
            for i in 0..n {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) / 1_000_000)
                });
            }
        }
        DataType::Float64 => {
            let arr = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("expected Float64Array".into()))?;
            #[allow(clippy::cast_possible_truncation)]
            for i in 0..n {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) as i64)
                });
            }
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "unsupported timestamp type for temporal join: {other:?}"
            )));
        }
    }

    Ok(out)
}

// ── Physical Execution Plan ──────────────────────────────────────

/// Physical plan that hash-probes a pre-indexed lookup table for
/// each batch from the streaming input.
pub struct LookupJoinExec {
    input: Arc<dyn ExecutionPlan>,
    index: Arc<HashIndex>,
    lookup_batch: Arc<RecordBatch>,
    stream_key_indices: Vec<usize>,
    join_type: LookupJoinType,
    schema: SchemaRef,
    properties: PlanProperties,
    /// `RowConverter` config for encoding probe keys identically to the index.
    key_sort_fields: Vec<SortField>,
    stream_field_count: usize,
}

impl LookupJoinExec {
    /// Creates a new lookup join executor.
    ///
    /// `stream_key_indices` and `lookup_key_indices` must be the same
    /// length and correspond pairwise (stream key 0 matches lookup key 0).
    ///
    /// # Errors
    ///
    /// Returns an error if the hash index cannot be built (e.g., unsupported key type).
    #[allow(clippy::needless_pass_by_value)] // lookup_batch is moved into Arc
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        lookup_batch: RecordBatch,
        stream_key_indices: Vec<usize>,
        lookup_key_indices: Vec<usize>,
        join_type: LookupJoinType,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        let index = HashIndex::build(&lookup_batch, &lookup_key_indices)?;

        let key_sort_fields: Vec<SortField> = lookup_key_indices
            .iter()
            .map(|&i| SortField::new(lookup_batch.schema().field(i).data_type().clone()))
            .collect();

        // Left outer joins produce NULLs for non-matching lookup rows,
        // so force all lookup columns nullable in the output schema.
        let output_schema = if join_type == LookupJoinType::LeftOuter {
            let stream_count = input.schema().fields().len();
            let mut fields = output_schema.fields().to_vec();
            for f in &mut fields[stream_count..] {
                if !f.is_nullable() {
                    *f = Arc::new(f.as_ref().clone().with_nullable(true));
                }
            }
            Arc::new(Schema::new_with_metadata(
                fields,
                output_schema.metadata().clone(),
            ))
        } else {
            output_schema
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        let stream_field_count = input.schema().fields().len();

        Ok(Self {
            input,
            index: Arc::new(index),
            lookup_batch: Arc::new(lookup_batch),
            stream_key_indices,
            join_type,
            schema: output_schema,
            properties,
            key_sort_fields,
            stream_field_count,
        })
    }
}

impl Debug for LookupJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LookupJoinExec")
            .field("join_type", &self.join_type)
            .field("stream_keys", &self.stream_key_indices)
            .field("lookup_rows", &self.lookup_batch.num_rows())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for LookupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LookupJoinExec: type={}, stream_keys={:?}, lookup_rows={}",
                    self.join_type,
                    self.stream_key_indices,
                    self.lookup_batch.num_rows(),
                )
            }
            DisplayFormatType::TreeRender => write!(f, "LookupJoinExec"),
        }
    }
}

impl ExecutionPlan for LookupJoinExec {
    fn name(&self) -> &'static str {
        "LookupJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "LookupJoinExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            index: Arc::clone(&self.index),
            lookup_batch: Arc::clone(&self.lookup_batch),
            stream_key_indices: self.stream_key_indices.clone(),
            join_type: self.join_type,
            schema: Arc::clone(&self.schema),
            properties: self.properties.clone(),
            key_sort_fields: self.key_sort_fields.clone(),
            stream_field_count: self.stream_field_count,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let converter = RowConverter::new(self.key_sort_fields.clone())?;
        let index = Arc::clone(&self.index);
        let lookup_batch = Arc::clone(&self.lookup_batch);
        let stream_key_indices = self.stream_key_indices.clone();
        let join_type = self.join_type;
        let schema = self.schema();
        let stream_field_count = self.stream_field_count;

        let output = input_stream.map(move |result| {
            let batch = result?;
            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
            }
            probe_batch(
                &batch,
                &converter,
                &index,
                &lookup_batch,
                &stream_key_indices,
                join_type,
                &schema,
                stream_field_count,
            )
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

impl datafusion::physical_plan::ExecutionPlanProperties for LookupJoinExec {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties.output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

// ── Probe Logic ──────────────────────────────────────────────────

/// Probes the hash index for each row in `stream_batch` and builds
/// the joined output batch.
#[allow(clippy::too_many_arguments)]
fn probe_batch(
    stream_batch: &RecordBatch,
    converter: &RowConverter,
    index: &HashIndex,
    lookup_batch: &RecordBatch,
    stream_key_indices: &[usize],
    join_type: LookupJoinType,
    output_schema: &SchemaRef,
    stream_field_count: usize,
) -> Result<RecordBatch> {
    let key_cols: Vec<_> = stream_key_indices
        .iter()
        .map(|&i| stream_batch.column(i).clone())
        .collect();
    let rows = converter.convert_columns(&key_cols)?;

    let num_rows = stream_batch.num_rows();
    let mut stream_indices: Vec<u32> = Vec::with_capacity(num_rows);
    let mut lookup_indices: Vec<Option<u32>> = Vec::with_capacity(num_rows);

    #[allow(clippy::cast_possible_truncation)] // batch row count fits u32
    for row in 0..num_rows {
        // SQL semantics: NULL != NULL, so rows with any null key never match.
        if key_cols.iter().any(|c| c.is_null(row)) {
            if join_type == LookupJoinType::LeftOuter {
                stream_indices.push(row as u32);
                lookup_indices.push(None);
            }
            continue;
        }

        let key = rows.row(row);
        match index.probe(key.as_ref()) {
            Some(matches) => {
                for &lookup_row in matches {
                    stream_indices.push(row as u32);
                    lookup_indices.push(Some(lookup_row));
                }
            }
            None if join_type == LookupJoinType::LeftOuter => {
                stream_indices.push(row as u32);
                lookup_indices.push(None);
            }
            None => {}
        }
    }

    if stream_indices.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
    }

    // Gather stream-side columns
    let take_stream = UInt32Array::from(stream_indices);
    let mut columns = Vec::with_capacity(output_schema.fields().len());

    for col in stream_batch.columns() {
        columns.push(take(col.as_ref(), &take_stream, None)?);
    }

    // Gather lookup-side columns (None → null in output)
    let take_lookup: UInt32Array = lookup_indices.into_iter().collect();
    for col in lookup_batch.columns() {
        columns.push(take(col.as_ref(), &take_lookup, None)?);
    }

    debug_assert_eq!(
        columns.len(),
        stream_field_count + lookup_batch.num_columns(),
        "output column count mismatch"
    );

    Ok(RecordBatch::try_new(Arc::clone(output_schema), columns)?)
}

// ── Versioned Lookup Join Exec ────────────────────────────────────

/// Physical plan that probes a versioned (temporal) index for each
/// batch from the streaming input. For each stream row, finds the
/// table row with the latest version timestamp `<= event_ts`.
pub struct VersionedLookupJoinExec {
    input: Arc<dyn ExecutionPlan>,
    index: Arc<VersionedIndex>,
    table_batch: Arc<RecordBatch>,
    stream_key_indices: Vec<usize>,
    stream_time_col_idx: usize,
    join_type: LookupJoinType,
    schema: SchemaRef,
    properties: PlanProperties,
    key_sort_fields: Vec<SortField>,
    stream_field_count: usize,
}

impl VersionedLookupJoinExec {
    /// Creates a new versioned lookup join executor.
    ///
    /// The `index` should be pre-built via `VersionedIndex::build()` and
    /// cached in `VersionedLookupState`. The index is only rebuilt when
    /// the table data changes (CDC update), not per execution cycle.
    ///
    /// # Errors
    ///
    /// Returns an error if the output schema cannot be constructed.
    #[allow(clippy::too_many_arguments, clippy::needless_pass_by_value)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        table_batch: RecordBatch,
        index: Arc<VersionedIndex>,
        stream_key_indices: Vec<usize>,
        stream_time_col_idx: usize,
        join_type: LookupJoinType,
        output_schema: SchemaRef,
        key_sort_fields: Vec<SortField>,
    ) -> Result<Self> {
        let output_schema = if join_type == LookupJoinType::LeftOuter {
            let stream_count = input.schema().fields().len();
            let mut fields = output_schema.fields().to_vec();
            for f in &mut fields[stream_count..] {
                if !f.is_nullable() {
                    *f = Arc::new(f.as_ref().clone().with_nullable(true));
                }
            }
            Arc::new(Schema::new_with_metadata(
                fields,
                output_schema.metadata().clone(),
            ))
        } else {
            output_schema
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        let stream_field_count = input.schema().fields().len();

        Ok(Self {
            input,
            index,
            table_batch: Arc::new(table_batch),
            stream_key_indices,
            stream_time_col_idx,
            join_type,
            schema: output_schema,
            properties,
            key_sort_fields,
            stream_field_count,
        })
    }
}

impl Debug for VersionedLookupJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("VersionedLookupJoinExec")
            .field("join_type", &self.join_type)
            .field("stream_keys", &self.stream_key_indices)
            .field("table_rows", &self.table_batch.num_rows())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for VersionedLookupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VersionedLookupJoinExec: type={}, stream_keys={:?}, table_rows={}",
                    self.join_type,
                    self.stream_key_indices,
                    self.table_batch.num_rows(),
                )
            }
            DisplayFormatType::TreeRender => write!(f, "VersionedLookupJoinExec"),
        }
    }
}

impl ExecutionPlan for VersionedLookupJoinExec {
    fn name(&self) -> &'static str {
        "VersionedLookupJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "VersionedLookupJoinExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            index: Arc::clone(&self.index),
            table_batch: Arc::clone(&self.table_batch),
            stream_key_indices: self.stream_key_indices.clone(),
            stream_time_col_idx: self.stream_time_col_idx,
            join_type: self.join_type,
            schema: Arc::clone(&self.schema),
            properties: self.properties.clone(),
            key_sort_fields: self.key_sort_fields.clone(),
            stream_field_count: self.stream_field_count,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let converter = RowConverter::new(self.key_sort_fields.clone())?;
        let index = Arc::clone(&self.index);
        let table_batch = Arc::clone(&self.table_batch);
        let stream_key_indices = self.stream_key_indices.clone();
        let stream_time_col_idx = self.stream_time_col_idx;
        let join_type = self.join_type;
        let schema = self.schema();
        let stream_field_count = self.stream_field_count;

        let output = input_stream.map(move |result| {
            let batch = result?;
            if batch.num_rows() == 0 {
                return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
            }
            probe_versioned_batch(
                &batch,
                &converter,
                &index,
                &table_batch,
                &stream_key_indices,
                stream_time_col_idx,
                join_type,
                &schema,
                stream_field_count,
            )
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

impl datafusion::physical_plan::ExecutionPlanProperties for VersionedLookupJoinExec {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties.output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

/// Probes the versioned index for each row in `stream_batch`, finding
/// the table row with the latest version `<= event_ts`.
#[allow(clippy::too_many_arguments)]
fn probe_versioned_batch(
    stream_batch: &RecordBatch,
    converter: &RowConverter,
    index: &VersionedIndex,
    table_batch: &RecordBatch,
    stream_key_indices: &[usize],
    stream_time_col_idx: usize,
    join_type: LookupJoinType,
    output_schema: &SchemaRef,
    stream_field_count: usize,
) -> Result<RecordBatch> {
    let key_cols: Vec<_> = stream_key_indices
        .iter()
        .map(|&i| stream_batch.column(i).clone())
        .collect();
    let rows = converter.convert_columns(&key_cols)?;
    let event_timestamps =
        extract_i64_timestamps(stream_batch.column(stream_time_col_idx).as_ref())?;

    let num_rows = stream_batch.num_rows();
    let mut stream_indices: Vec<u32> = Vec::with_capacity(num_rows);
    let mut lookup_indices: Vec<Option<u32>> = Vec::with_capacity(num_rows);

    #[allow(clippy::cast_possible_truncation)]
    for (row, event_ts_opt) in event_timestamps.iter().enumerate() {
        // Null keys or null event timestamps cannot match.
        if key_cols.iter().any(|c| c.is_null(row)) || event_ts_opt.is_none() {
            if join_type == LookupJoinType::LeftOuter {
                stream_indices.push(row as u32);
                lookup_indices.push(None);
            }
            continue;
        }

        let key = rows.row(row);
        let event_ts = event_ts_opt.unwrap();
        match index.probe_at_time(key.as_ref(), event_ts) {
            Some(table_row_idx) => {
                stream_indices.push(row as u32);
                lookup_indices.push(Some(table_row_idx));
            }
            None if join_type == LookupJoinType::LeftOuter => {
                stream_indices.push(row as u32);
                lookup_indices.push(None);
            }
            None => {}
        }
    }

    if stream_indices.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
    }

    let take_stream = UInt32Array::from(stream_indices);
    let mut columns = Vec::with_capacity(output_schema.fields().len());

    for col in stream_batch.columns() {
        columns.push(take(col.as_ref(), &take_stream, None)?);
    }

    let take_lookup: UInt32Array = lookup_indices.into_iter().collect();
    for col in table_batch.columns() {
        columns.push(take(col.as_ref(), &take_lookup, None)?);
    }

    debug_assert_eq!(
        columns.len(),
        stream_field_count + table_batch.num_columns(),
        "output column count mismatch"
    );

    Ok(RecordBatch::try_new(Arc::clone(output_schema), columns)?)
}

// ── Partial Lookup Join Exec ──────────────────────────────────────

/// Physical plan that probes a bounded foyer cache per key for each
/// batch from the streaming input. Used for on-demand/partial tables
/// where the full dataset does not fit in memory.
pub struct PartialLookupJoinExec {
    input: Arc<dyn ExecutionPlan>,
    foyer_cache: Arc<FoyerMemoryCache>,
    stream_key_indices: Vec<usize>,
    join_type: LookupJoinType,
    schema: SchemaRef,
    properties: PlanProperties,
    key_sort_fields: Vec<SortField>,
    stream_field_count: usize,
    lookup_schema: SchemaRef,
    source: Option<Arc<dyn LookupSourceDyn>>,
    fetch_semaphore: Arc<Semaphore>,
}

impl PartialLookupJoinExec {
    /// Creates a new partial lookup join executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the output schema cannot be constructed.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        foyer_cache: Arc<FoyerMemoryCache>,
        stream_key_indices: Vec<usize>,
        key_sort_fields: Vec<SortField>,
        join_type: LookupJoinType,
        lookup_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        Self::try_new_with_source(
            input,
            foyer_cache,
            stream_key_indices,
            key_sort_fields,
            join_type,
            lookup_schema,
            output_schema,
            None,
            Arc::new(Semaphore::new(64)),
        )
    }

    /// Creates a new partial lookup join executor with optional source fallback.
    ///
    /// # Errors
    ///
    /// Returns an error if the output schema cannot be constructed.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_source(
        input: Arc<dyn ExecutionPlan>,
        foyer_cache: Arc<FoyerMemoryCache>,
        stream_key_indices: Vec<usize>,
        key_sort_fields: Vec<SortField>,
        join_type: LookupJoinType,
        lookup_schema: SchemaRef,
        output_schema: SchemaRef,
        source: Option<Arc<dyn LookupSourceDyn>>,
        fetch_semaphore: Arc<Semaphore>,
    ) -> Result<Self> {
        let output_schema = if join_type == LookupJoinType::LeftOuter {
            let stream_count = input.schema().fields().len();
            let mut fields = output_schema.fields().to_vec();
            for f in &mut fields[stream_count..] {
                if !f.is_nullable() {
                    *f = Arc::new(f.as_ref().clone().with_nullable(true));
                }
            }
            Arc::new(Schema::new_with_metadata(
                fields,
                output_schema.metadata().clone(),
            ))
        } else {
            output_schema
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        let stream_field_count = input.schema().fields().len();

        Ok(Self {
            input,
            foyer_cache,
            stream_key_indices,
            join_type,
            schema: output_schema,
            properties,
            key_sort_fields,
            stream_field_count,
            lookup_schema,
            source,
            fetch_semaphore,
        })
    }
}

impl Debug for PartialLookupJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialLookupJoinExec")
            .field("join_type", &self.join_type)
            .field("stream_keys", &self.stream_key_indices)
            .field("cache_table_id", &self.foyer_cache.table_id())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for PartialLookupJoinExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "PartialLookupJoinExec: type={}, stream_keys={:?}, cache_entries={}",
                    self.join_type,
                    self.stream_key_indices,
                    self.foyer_cache.len(),
                )
            }
            DisplayFormatType::TreeRender => write!(f, "PartialLookupJoinExec"),
        }
    }
}

impl ExecutionPlan for PartialLookupJoinExec {
    fn name(&self) -> &'static str {
        "PartialLookupJoinExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "PartialLookupJoinExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            foyer_cache: Arc::clone(&self.foyer_cache),
            stream_key_indices: self.stream_key_indices.clone(),
            join_type: self.join_type,
            schema: Arc::clone(&self.schema),
            properties: self.properties.clone(),
            key_sort_fields: self.key_sort_fields.clone(),
            stream_field_count: self.stream_field_count,
            lookup_schema: Arc::clone(&self.lookup_schema),
            source: self.source.clone(),
            fetch_semaphore: Arc::clone(&self.fetch_semaphore),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let converter = Arc::new(RowConverter::new(self.key_sort_fields.clone())?);
        let foyer_cache = Arc::clone(&self.foyer_cache);
        let stream_key_indices = self.stream_key_indices.clone();
        let join_type = self.join_type;
        let schema = self.schema();
        let stream_field_count = self.stream_field_count;
        let lookup_schema = Arc::clone(&self.lookup_schema);
        let source = self.source.clone();
        let fetch_semaphore = Arc::clone(&self.fetch_semaphore);

        let output = input_stream.then(move |result| {
            let foyer_cache = Arc::clone(&foyer_cache);
            let converter = Arc::clone(&converter);
            let stream_key_indices = stream_key_indices.clone();
            let schema = Arc::clone(&schema);
            let lookup_schema = Arc::clone(&lookup_schema);
            let source = source.clone();
            let fetch_semaphore = Arc::clone(&fetch_semaphore);
            async move {
                let batch = result?;
                if batch.num_rows() == 0 {
                    return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
                }
                probe_partial_batch_with_fallback(
                    &batch,
                    &converter,
                    &foyer_cache,
                    &stream_key_indices,
                    join_type,
                    &schema,
                    stream_field_count,
                    &lookup_schema,
                    source.as_deref(),
                    &fetch_semaphore,
                )
                .await
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            output,
        )))
    }
}

impl datafusion::physical_plan::ExecutionPlanProperties for PartialLookupJoinExec {
    fn output_partitioning(&self) -> &Partitioning {
        self.properties.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        self.properties.output_ordering()
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Unbounded {
            requires_infinite_memory: false,
        }
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

/// Probes the foyer cache for each row in `stream_batch`, falling back
/// to the async source for cache misses. Inserts source results into
/// the cache before building the output.
#[allow(clippy::too_many_arguments)]
async fn probe_partial_batch_with_fallback(
    stream_batch: &RecordBatch,
    converter: &RowConverter,
    foyer_cache: &FoyerMemoryCache,
    stream_key_indices: &[usize],
    join_type: LookupJoinType,
    output_schema: &SchemaRef,
    stream_field_count: usize,
    lookup_schema: &SchemaRef,
    source: Option<&dyn LookupSourceDyn>,
    fetch_semaphore: &Semaphore,
) -> Result<RecordBatch> {
    let key_cols: Vec<_> = stream_key_indices
        .iter()
        .map(|&i| stream_batch.column(i).clone())
        .collect();
    let rows = converter.convert_columns(&key_cols)?;

    let num_rows = stream_batch.num_rows();
    let mut stream_indices: Vec<u32> = Vec::with_capacity(num_rows);
    let mut lookup_batches: Vec<Option<RecordBatch>> = Vec::with_capacity(num_rows);
    let mut miss_keys: Vec<(usize, Vec<u8>)> = Vec::new();

    #[allow(clippy::cast_possible_truncation)]
    for row in 0..num_rows {
        // SQL semantics: NULL != NULL, so rows with any null key never match.
        if key_cols.iter().any(|c| c.is_null(row)) {
            if join_type == LookupJoinType::LeftOuter {
                stream_indices.push(row as u32);
                lookup_batches.push(None);
            }
            continue;
        }

        let key = rows.row(row);
        let result = foyer_cache.get_cached(key.as_ref());
        if let Some(batch) = result.into_batch() {
            stream_indices.push(row as u32);
            lookup_batches.push(Some(batch));
        } else {
            let idx = stream_indices.len();
            stream_indices.push(row as u32);
            lookup_batches.push(None);
            miss_keys.push((idx, key.as_ref().to_vec()));
        }
    }

    // Fetch missed keys from the source in a single batch query
    if let Some(source) = source {
        if !miss_keys.is_empty() {
            let _permit = fetch_semaphore
                .acquire()
                .await
                .map_err(|_| DataFusionError::Internal("fetch semaphore closed".into()))?;

            let key_refs: Vec<&[u8]> = miss_keys.iter().map(|(_, k)| k.as_slice()).collect();
            let source_results = source.query_batch(&key_refs, &[], &[]).await;

            match source_results {
                Ok(results) => {
                    for ((idx, key_bytes), maybe_batch) in miss_keys.iter().zip(results.into_iter())
                    {
                        if let Some(batch) = maybe_batch {
                            foyer_cache.insert(key_bytes, batch.clone());
                            lookup_batches[*idx] = Some(batch);
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "source fallback failed, serving cache-only results");
                }
            }
        }
    }

    // For inner joins, remove rows that still have no match
    if join_type == LookupJoinType::Inner {
        let mut write = 0;
        for read in 0..stream_indices.len() {
            if lookup_batches[read].is_some() {
                stream_indices[write] = stream_indices[read];
                lookup_batches.swap(write, read);
                write += 1;
            }
        }
        stream_indices.truncate(write);
        lookup_batches.truncate(write);
    }

    if stream_indices.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(output_schema)));
    }

    let take_indices = UInt32Array::from(stream_indices);
    let mut columns = Vec::with_capacity(output_schema.fields().len());

    for col in stream_batch.columns() {
        columns.push(take(col.as_ref(), &take_indices, None)?);
    }

    let lookup_col_count = lookup_schema.fields().len();
    for col_idx in 0..lookup_col_count {
        let arrays: Vec<_> = lookup_batches
            .iter()
            .map(|opt| match opt {
                Some(b) => b.column(col_idx).clone(),
                None => arrow_array::new_null_array(lookup_schema.field(col_idx).data_type(), 1),
            })
            .collect();
        let refs: Vec<&dyn arrow_array::Array> = arrays.iter().map(AsRef::as_ref).collect();
        columns.push(arrow::compute::concat(&refs)?);
    }

    debug_assert_eq!(
        columns.len(),
        stream_field_count + lookup_col_count,
        "output column count mismatch"
    );

    Ok(RecordBatch::try_new(Arc::clone(output_schema), columns)?)
}

// ── Extension Planner ────────────────────────────────────────────

/// Converts `LookupJoinNode` logical plans to [`LookupJoinExec`]
/// or [`PartialLookupJoinExec`] physical plans by resolving table
/// data from the registry.
pub struct LookupJoinExtensionPlanner {
    registry: Arc<LookupTableRegistry>,
}

impl LookupJoinExtensionPlanner {
    /// Creates a planner backed by the given registry.
    pub fn new(registry: Arc<LookupTableRegistry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl ExtensionPlanner for LookupJoinExtensionPlanner {
    #[allow(clippy::too_many_lines)]
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(lookup_node) = node.as_any().downcast_ref::<LookupJoinNode>() else {
            return Ok(None);
        };

        let entry = self
            .registry
            .get_entry(lookup_node.lookup_table_name())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "lookup table '{}' not registered",
                    lookup_node.lookup_table_name()
                ))
            })?;

        let input = Arc::clone(&physical_inputs[0]);
        let stream_schema = input.schema();

        match entry {
            RegisteredLookup::Partial(partial_state) => {
                let stream_key_indices = resolve_stream_keys(lookup_node, &stream_schema)?;

                let mut output_fields = stream_schema.fields().to_vec();
                output_fields.extend(partial_state.schema.fields().iter().cloned());
                let output_schema = Arc::new(Schema::new(output_fields));

                let exec = PartialLookupJoinExec::try_new_with_source(
                    input,
                    Arc::clone(&partial_state.foyer_cache),
                    stream_key_indices,
                    partial_state.key_sort_fields.clone(),
                    lookup_node.join_type(),
                    Arc::clone(&partial_state.schema),
                    output_schema,
                    partial_state.source.clone(),
                    Arc::clone(&partial_state.fetch_semaphore),
                )?;
                Ok(Some(Arc::new(exec)))
            }
            RegisteredLookup::Snapshot(snapshot) => {
                let lookup_schema = snapshot.batch.schema();
                let lookup_key_indices = resolve_lookup_keys(lookup_node, &lookup_schema)?;

                let lookup_batch = if lookup_node.pushdown_predicates().is_empty()
                    || snapshot.batch.num_rows() == 0
                {
                    snapshot.batch.clone()
                } else {
                    apply_pushdown_predicates(
                        &snapshot.batch,
                        lookup_node.pushdown_predicates(),
                        session_state,
                    )?
                };

                let stream_key_indices = resolve_stream_keys(lookup_node, &stream_schema)?;

                // Validate join key types are compatible
                for (si, li) in stream_key_indices.iter().zip(&lookup_key_indices) {
                    let st = stream_schema.field(*si).data_type();
                    let lt = lookup_schema.field(*li).data_type();
                    if st != lt {
                        return Err(DataFusionError::Plan(format!(
                            "Lookup join key type mismatch: stream '{}' is {st:?} \
                             but lookup '{}' is {lt:?}",
                            stream_schema.field(*si).name(),
                            lookup_schema.field(*li).name(),
                        )));
                    }
                }

                let mut output_fields = stream_schema.fields().to_vec();
                output_fields.extend(lookup_batch.schema().fields().iter().cloned());
                let output_schema = Arc::new(Schema::new(output_fields));

                let exec = LookupJoinExec::try_new(
                    input,
                    lookup_batch,
                    stream_key_indices,
                    lookup_key_indices,
                    lookup_node.join_type(),
                    output_schema,
                )?;

                Ok(Some(Arc::new(exec)))
            }
            RegisteredLookup::Versioned(versioned_state) => {
                let table_schema = versioned_state.batch.schema();
                let lookup_key_indices = resolve_lookup_keys(lookup_node, &table_schema)?;
                let stream_key_indices = resolve_stream_keys(lookup_node, &stream_schema)?;

                // Validate key type compatibility.
                for (si, li) in stream_key_indices.iter().zip(&lookup_key_indices) {
                    let st = stream_schema.field(*si).data_type();
                    let lt = table_schema.field(*li).data_type();
                    if st != lt {
                        return Err(DataFusionError::Plan(format!(
                            "Temporal join key type mismatch: stream '{}' is {st:?} \
                             but table '{}' is {lt:?}",
                            stream_schema.field(*si).name(),
                            table_schema.field(*li).name(),
                        )));
                    }
                }

                let stream_time_col_idx = stream_schema
                    .index_of(&versioned_state.stream_time_column)
                    .map_err(|_| {
                        DataFusionError::Plan(format!(
                            "stream time column '{}' not found in stream schema",
                            versioned_state.stream_time_column
                        ))
                    })?;

                let key_sort_fields: Vec<SortField> = lookup_key_indices
                    .iter()
                    .map(|&i| SortField::new(table_schema.field(i).data_type().clone()))
                    .collect();

                let mut output_fields = stream_schema.fields().to_vec();
                output_fields.extend(table_schema.fields().iter().cloned());
                let output_schema = Arc::new(Schema::new(output_fields));

                let exec = VersionedLookupJoinExec::try_new(
                    input,
                    versioned_state.batch.clone(),
                    Arc::clone(&versioned_state.index),
                    stream_key_indices,
                    stream_time_col_idx,
                    lookup_node.join_type(),
                    output_schema,
                    key_sort_fields,
                )?;

                Ok(Some(Arc::new(exec)))
            }
        }
    }
}

/// Evaluates pushdown predicates against the lookup snapshot, returning
/// only the rows that pass all predicates. This shrinks the hash index.
fn apply_pushdown_predicates(
    batch: &RecordBatch,
    predicates: &[Expr],
    session_state: &SessionState,
) -> Result<RecordBatch> {
    use arrow::compute::filter_record_batch;
    use datafusion::physical_expr::create_physical_expr;

    let schema = batch.schema();
    let df_schema = datafusion::common::DFSchema::try_from(schema.as_ref().clone())?;

    let mut mask = None::<arrow_array::BooleanArray>;
    for pred in predicates {
        let phys_expr = create_physical_expr(pred, &df_schema, session_state.execution_props())?;
        let result = phys_expr.evaluate(batch)?;
        let bool_arr = result
            .into_array(batch.num_rows())?
            .as_any()
            .downcast_ref::<arrow_array::BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("pushdown predicate did not evaluate to boolean".into())
            })?
            .clone();
        mask = Some(match mask {
            Some(existing) => arrow::compute::and(&existing, &bool_arr)?,
            None => bool_arr,
        });
    }

    match mask {
        Some(m) => Ok(filter_record_batch(batch, &m)?),
        None => Ok(batch.clone()),
    }
}

fn resolve_stream_keys(node: &LookupJoinNode, schema: &SchemaRef) -> Result<Vec<usize>> {
    node.join_keys()
        .iter()
        .map(|pair| match &pair.stream_expr {
            Expr::Column(col) => schema.index_of(&col.name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "stream key column '{}' not found in physical schema",
                    col.name
                ))
            }),
            other => Err(DataFusionError::NotImplemented(format!(
                "lookup join requires column references as stream keys, got: {other}"
            ))),
        })
        .collect()
}

fn resolve_lookup_keys(node: &LookupJoinNode, schema: &SchemaRef) -> Result<Vec<usize>> {
    node.join_keys()
        .iter()
        .map(|pair| {
            schema.index_of(&pair.lookup_column).map_err(|_| {
                DataFusionError::Plan(format!(
                    "lookup key column '{}' not found in lookup table schema",
                    pair.lookup_column
                ))
            })
        })
        .collect()
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter as TestStreamAdapter;
    use futures::TryStreamExt;

    /// Creates a bounded `ExecutionPlan` from a single `RecordBatch`.
    fn batch_exec(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        let schema = batch.schema();
        let batches = vec![batch];
        let stream_schema = Arc::clone(&schema);
        Arc::new(StreamExecStub {
            schema,
            batches: std::sync::Mutex::new(Some(batches)),
            stream_schema,
        })
    }

    /// Minimal bounded exec for tests — produces one partition of batches.
    struct StreamExecStub {
        schema: SchemaRef,
        batches: std::sync::Mutex<Option<Vec<RecordBatch>>>,
        stream_schema: SchemaRef,
    }

    impl Debug for StreamExecStub {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "StreamExecStub")
        }
    }

    impl DisplayAs for StreamExecStub {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "StreamExecStub")
        }
    }

    impl ExecutionPlan for StreamExecStub {
        fn name(&self) -> &'static str {
            "StreamExecStub"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn properties(&self) -> &PlanProperties {
            // Leak a static PlanProperties for test simplicity
            Box::leak(Box::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&self.schema)),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )))
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
            let batches = self.batches.lock().unwrap().take().unwrap_or_default();
            let schema = Arc::clone(&self.stream_schema);
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(TestStreamAdapter::new(schema, stream)))
        }
    }

    impl datafusion::physical_plan::ExecutionPlanProperties for StreamExecStub {
        fn output_partitioning(&self) -> &Partitioning {
            self.properties().output_partitioning()
        }
        fn output_ordering(&self) -> Option<&LexOrdering> {
            None
        }
        fn boundedness(&self) -> Boundedness {
            Boundedness::Bounded
        }
        fn pipeline_behavior(&self) -> EmissionType {
            EmissionType::Final
        }
        fn equivalence_properties(&self) -> &EquivalenceProperties {
            self.properties().equivalence_properties()
        }
    }

    fn orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn customers_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn customers_batch() -> RecordBatch {
        RecordBatch::try_new(
            customers_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    fn orders_batch() -> RecordBatch {
        RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
                Arc::new(Int64Array::from(vec![1, 2, 99, 3])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0, 40.0])),
            ],
        )
        .unwrap()
    }

    fn make_exec(join_type: LookupJoinType) -> LookupJoinExec {
        let input = batch_exec(orders_batch());
        LookupJoinExec::try_new(
            input,
            customers_batch(),
            vec![1], // customer_id
            vec![0], // id
            join_type,
            output_schema(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn inner_join_filters_non_matches() {
        let exec = make_exec(LookupJoinType::Inner);
        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 3, "customer_id=99 has no match, filtered by inner");

        let names = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Charlie");
    }

    #[tokio::test]
    async fn left_outer_preserves_non_matches() {
        let exec = make_exec(LookupJoinType::LeftOuter);
        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "all 4 stream rows preserved in left outer");

        let names = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Row 2 (customer_id=99) should have null name
        assert!(names.is_null(2));
    }

    #[tokio::test]
    async fn empty_lookup_inner_produces_no_rows() {
        let empty = RecordBatch::new_empty(customers_schema());
        let input = batch_exec(orders_batch());
        let exec = LookupJoinExec::try_new(
            input,
            empty,
            vec![1],
            vec![0],
            LookupJoinType::Inner,
            output_schema(),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn empty_lookup_left_outer_preserves_all_stream_rows() {
        let empty = RecordBatch::new_empty(customers_schema());
        let input = batch_exec(orders_batch());
        let exec = LookupJoinExec::try_new(
            input,
            empty,
            vec![1],
            vec![0],
            LookupJoinType::LeftOuter,
            output_schema(),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4);
    }

    #[tokio::test]
    async fn duplicate_keys_produce_multiple_rows() {
        let lookup = RecordBatch::try_new(
            customers_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["Alice-A", "Alice-B"])),
            ],
        )
        .unwrap();

        let stream = RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Float64Array::from(vec![10.0])),
            ],
        )
        .unwrap();

        let input = batch_exec(stream);
        let exec = LookupJoinExec::try_new(
            input,
            lookup,
            vec![1],
            vec![0],
            LookupJoinType::Inner,
            output_schema(),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 2, "one stream row matched two lookup rows");
    }

    #[test]
    fn with_new_children_preserves_state() {
        let exec = Arc::new(make_exec(LookupJoinType::Inner));
        let expected_schema = exec.schema();
        let children = exec.children().into_iter().cloned().collect();
        let rebuilt = exec.with_new_children(children).unwrap();
        assert_eq!(rebuilt.schema(), expected_schema);
        assert_eq!(rebuilt.name(), "LookupJoinExec");
    }

    #[test]
    fn display_format() {
        let exec = make_exec(LookupJoinType::Inner);
        let s = format!("{exec:?}");
        assert!(s.contains("LookupJoinExec"));
        assert!(s.contains("lookup_rows: 3"));
    }

    #[test]
    fn registry_crud() {
        let reg = LookupTableRegistry::new();
        assert!(reg.get("customers").is_none());

        reg.register(
            "customers",
            LookupSnapshot {
                batch: customers_batch(),
                key_columns: vec!["id".into()],
            },
        );
        assert!(reg.get("customers").is_some());
        assert!(reg.get("CUSTOMERS").is_some(), "case-insensitive");

        reg.unregister("customers");
        assert!(reg.get("customers").is_none());
    }

    #[test]
    fn registry_update_replaces() {
        let reg = LookupTableRegistry::new();
        reg.register(
            "t",
            LookupSnapshot {
                batch: RecordBatch::new_empty(customers_schema()),
                key_columns: vec![],
            },
        );
        assert_eq!(reg.get("t").unwrap().batch.num_rows(), 0);

        reg.register(
            "t",
            LookupSnapshot {
                batch: customers_batch(),
                key_columns: vec![],
            },
        );
        assert_eq!(reg.get("t").unwrap().batch.num_rows(), 3);
    }

    #[test]
    fn pushdown_predicates_filter_snapshot() {
        use datafusion::logical_expr::{col, lit};

        let batch = customers_batch(); // id=[1,2,3], name=[Alice,Bob,Charlie]
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();

        // Filter: id > 1 (should keep rows 2 and 3)
        let predicates = vec![col("id").gt(lit(1i64))];
        let filtered = apply_pushdown_predicates(&batch, &predicates, &state).unwrap();
        assert_eq!(filtered.num_rows(), 2);

        let ids = filtered
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 3);
    }

    #[test]
    fn pushdown_predicates_empty_passes_all() {
        let batch = customers_batch();
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();

        let filtered = apply_pushdown_predicates(&batch, &[], &state).unwrap();
        assert_eq!(filtered.num_rows(), 3);
    }

    #[test]
    fn pushdown_predicates_multiple_and() {
        use datafusion::logical_expr::{col, lit};

        let batch = customers_batch(); // id=[1,2,3]
        let ctx = datafusion::prelude::SessionContext::new();
        let state = ctx.state();

        // id >= 2 AND id < 3 → only row with id=2
        let predicates = vec![col("id").gt_eq(lit(2i64)), col("id").lt(lit(3i64))];
        let filtered = apply_pushdown_predicates(&batch, &predicates, &state).unwrap();
        assert_eq!(filtered.num_rows(), 1);
    }

    // ── PartialLookupJoinExec Tests ──────────────────────────────

    use laminar_core::lookup::foyer_cache::FoyerMemoryCacheConfig;

    fn make_foyer_cache() -> Arc<FoyerMemoryCache> {
        Arc::new(FoyerMemoryCache::new(
            1,
            FoyerMemoryCacheConfig {
                capacity: 64,
                shards: 4,
            },
        ))
    }

    fn customer_row(id: i64, name: &str) -> RecordBatch {
        RecordBatch::try_new(
            customers_schema(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![name])),
            ],
        )
        .unwrap()
    }

    fn warm_cache(cache: &FoyerMemoryCache) {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();

        for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
            let key_col = Int64Array::from(vec![id]);
            let rows = converter.convert_columns(&[Arc::new(key_col)]).unwrap();
            let key = rows.row(0);
            cache.insert(key.as_ref(), customer_row(id, name));
        }
    }

    fn make_partial_exec(join_type: LookupJoinType) -> PartialLookupJoinExec {
        let cache = make_foyer_cache();
        warm_cache(&cache);

        let input = batch_exec(orders_batch());
        let key_sort_fields = vec![SortField::new(DataType::Int64)];

        PartialLookupJoinExec::try_new(
            input,
            cache,
            vec![1], // customer_id
            key_sort_fields,
            join_type,
            customers_schema(),
            output_schema(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn partial_inner_join_filters_non_matches() {
        let exec = make_partial_exec(LookupJoinType::Inner);
        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 3, "customer_id=99 has no match, filtered by inner");

        let names = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert_eq!(names.value(2), "Charlie");
    }

    #[tokio::test]
    async fn partial_left_outer_preserves_non_matches() {
        let exec = make_partial_exec(LookupJoinType::LeftOuter);
        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4, "all 4 stream rows preserved in left outer");

        let names = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(names.is_null(2), "customer_id=99 should have null name");
    }

    #[tokio::test]
    async fn partial_empty_cache_inner_produces_no_rows() {
        let cache = make_foyer_cache();
        let input = batch_exec(orders_batch());
        let key_sort_fields = vec![SortField::new(DataType::Int64)];

        let exec = PartialLookupJoinExec::try_new(
            input,
            cache,
            vec![1],
            key_sort_fields,
            LookupJoinType::Inner,
            customers_schema(),
            output_schema(),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn partial_empty_cache_left_outer_preserves_all() {
        let cache = make_foyer_cache();
        let input = batch_exec(orders_batch());
        let key_sort_fields = vec![SortField::new(DataType::Int64)];

        let exec = PartialLookupJoinExec::try_new(
            input,
            cache,
            vec![1],
            key_sort_fields,
            LookupJoinType::LeftOuter,
            customers_schema(),
            output_schema(),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn partial_with_new_children_preserves_state() {
        let exec = Arc::new(make_partial_exec(LookupJoinType::Inner));
        let expected_schema = exec.schema();
        let children = exec.children().into_iter().cloned().collect();
        let rebuilt = exec.with_new_children(children).unwrap();
        assert_eq!(rebuilt.schema(), expected_schema);
        assert_eq!(rebuilt.name(), "PartialLookupJoinExec");
    }

    #[test]
    fn partial_display_format() {
        let exec = make_partial_exec(LookupJoinType::Inner);
        let s = format!("{exec:?}");
        assert!(s.contains("PartialLookupJoinExec"));
        assert!(s.contains("cache_table_id: 1"));
    }

    #[test]
    fn registry_partial_entry() {
        let reg = LookupTableRegistry::new();
        let cache = make_foyer_cache();
        let key_sort_fields = vec![SortField::new(DataType::Int64)];

        reg.register_partial(
            "customers",
            PartialLookupState {
                foyer_cache: cache,
                schema: customers_schema(),
                key_columns: vec!["id".into()],
                key_sort_fields,
                source: None,
                fetch_semaphore: Arc::new(Semaphore::new(64)),
            },
        );

        assert!(reg.get("customers").is_none());

        let entry = reg.get_entry("customers");
        assert!(entry.is_some());
        assert!(matches!(entry.unwrap(), RegisteredLookup::Partial(_)));
    }

    #[tokio::test]
    async fn partial_source_fallback_on_miss() {
        use laminar_core::lookup::source::LookupError;
        use laminar_core::lookup::source::LookupSourceDyn;

        struct TestSource;

        #[async_trait]
        impl LookupSourceDyn for TestSource {
            async fn query_batch(
                &self,
                keys: &[&[u8]],
                _predicates: &[laminar_core::lookup::predicate::Predicate],
                _projection: &[laminar_core::lookup::source::ColumnId],
            ) -> std::result::Result<Vec<Option<RecordBatch>>, LookupError> {
                Ok(keys
                    .iter()
                    .map(|_| Some(customer_row(99, "FromSource")))
                    .collect())
            }

            fn schema(&self) -> SchemaRef {
                customers_schema()
            }
        }

        let cache = make_foyer_cache();
        // Only warm id=1 in cache, id=99 will miss and go to source
        warm_cache(&cache);

        let orders = RecordBatch::try_new(
            orders_schema(),
            vec![
                Arc::new(Int64Array::from(vec![200])),
                Arc::new(Int64Array::from(vec![99])), // not in cache
                Arc::new(Float64Array::from(vec![50.0])),
            ],
        )
        .unwrap();

        let input = batch_exec(orders);
        let key_sort_fields = vec![SortField::new(DataType::Int64)];
        let source: Arc<dyn LookupSourceDyn> = Arc::new(TestSource);

        let exec = PartialLookupJoinExec::try_new_with_source(
            input,
            cache,
            vec![1],
            key_sort_fields,
            LookupJoinType::Inner,
            customers_schema(),
            output_schema(),
            Some(source),
            Arc::new(Semaphore::new(64)),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total, 1, "source fallback should produce 1 row");

        let names = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "FromSource");
    }

    #[tokio::test]
    async fn partial_source_error_graceful_degradation() {
        use laminar_core::lookup::source::LookupError;
        use laminar_core::lookup::source::LookupSourceDyn;

        struct FailingSource;

        #[async_trait]
        impl LookupSourceDyn for FailingSource {
            async fn query_batch(
                &self,
                _keys: &[&[u8]],
                _predicates: &[laminar_core::lookup::predicate::Predicate],
                _projection: &[laminar_core::lookup::source::ColumnId],
            ) -> std::result::Result<Vec<Option<RecordBatch>>, LookupError> {
                Err(LookupError::Internal("source unavailable".into()))
            }

            fn schema(&self) -> SchemaRef {
                customers_schema()
            }
        }

        let cache = make_foyer_cache();
        let input = batch_exec(orders_batch());
        let key_sort_fields = vec![SortField::new(DataType::Int64)];
        let source: Arc<dyn LookupSourceDyn> = Arc::new(FailingSource);

        let exec = PartialLookupJoinExec::try_new_with_source(
            input,
            cache,
            vec![1],
            key_sort_fields,
            LookupJoinType::LeftOuter,
            customers_schema(),
            output_schema(),
            Some(source),
            Arc::new(Semaphore::new(64)),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();
        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // All rows preserved in left outer, but all lookup columns null
        assert_eq!(total, 4);
    }

    #[test]
    fn registry_snapshot_entry_via_get_entry() {
        let reg = LookupTableRegistry::new();
        reg.register(
            "t",
            LookupSnapshot {
                batch: customers_batch(),
                key_columns: vec!["id".into()],
            },
        );

        let entry = reg.get_entry("t");
        assert!(matches!(entry.unwrap(), RegisteredLookup::Snapshot(_)));
        assert!(reg.get("t").is_some());
    }

    // ── NULL key tests ────────────────────────────────────────────────

    fn nullable_orders_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true), // nullable key
            Field::new("amount", DataType::Float64, false),
        ]))
    }

    fn nullable_output_schema(join_type: LookupJoinType) -> SchemaRef {
        let lookup_nullable = join_type == LookupJoinType::LeftOuter;
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, true),
            Field::new("amount", DataType::Float64, false),
            Field::new("id", DataType::Int64, lookup_nullable),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[tokio::test]
    async fn null_key_inner_join_no_match() {
        // Stream: customer_id = [1, NULL, 2]
        let stream_batch = RecordBatch::try_new(
            nullable_orders_schema(),
            vec![
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();

        let input = batch_exec(stream_batch);
        let exec = LookupJoinExec::try_new(
            input,
            customers_batch(),
            vec![1],
            vec![0],
            LookupJoinType::Inner,
            nullable_output_schema(LookupJoinType::Inner),
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // Only customer_id=1 and customer_id=2 match; NULL is skipped
        assert_eq!(total, 2, "NULL key row should not match in inner join");
    }

    #[tokio::test]
    async fn null_key_left_outer_produces_nulls() {
        // Stream: customer_id = [1, NULL, 2]
        let stream_batch = RecordBatch::try_new(
            nullable_orders_schema(),
            vec![
                Arc::new(Int64Array::from(vec![100, 101, 102])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
                Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])),
            ],
        )
        .unwrap();

        let input = batch_exec(stream_batch);
        let out_schema = nullable_output_schema(LookupJoinType::LeftOuter);
        let exec = LookupJoinExec::try_new(
            input,
            customers_batch(),
            vec![1],
            vec![0],
            LookupJoinType::LeftOuter,
            out_schema,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
        // All 3 rows preserved; NULL key row has null lookup columns
        assert_eq!(total, 3, "all rows preserved in left outer");

        let names = batches[0]
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "Alice");
        assert!(
            names.is_null(1),
            "NULL key row should have null lookup name"
        );
        assert_eq!(names.value(2), "Bob");
    }

    // ── Versioned Lookup Join Tests ────────────────────────────────

    fn versioned_table_batch() -> RecordBatch {
        // Table with key=currency, version_ts=valid_from, rate=value
        // Two currencies with multiple versions each
        let schema = Arc::new(Schema::new(vec![
            Field::new("currency", DataType::Utf8, false),
            Field::new("valid_from", DataType::Int64, false),
            Field::new("rate", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["USD", "USD", "EUR", "EUR", "EUR"])),
                Arc::new(Int64Array::from(vec![100, 200, 100, 150, 300])),
                Arc::new(Float64Array::from(vec![1.0, 1.1, 0.85, 0.90, 0.88])),
            ],
        )
        .unwrap()
    }

    fn stream_batch_with_time() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("currency", DataType::Utf8, false),
            Field::new("event_ts", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["USD", "EUR", "USD", "EUR"])),
                Arc::new(Int64Array::from(vec![150, 160, 250, 50])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_versioned_index_build_and_probe() {
        let batch = versioned_table_batch();
        let index = VersionedIndex::build(&batch, &[0], 1).unwrap();

        // USD has versions at 100 and 200
        // Probe at 150 → should find version 100 (latest <= 150)
        let key_sf = vec![SortField::new(DataType::Utf8)];
        let converter = RowConverter::new(key_sf).unwrap();
        let usd_col = Arc::new(StringArray::from(vec!["USD"]));
        let usd_rows = converter.convert_columns(&[usd_col]).unwrap();
        let usd_key = usd_rows.row(0);

        let result = index.probe_at_time(usd_key.as_ref(), 150);
        assert!(result.is_some());
        // Row 0 is USD@100, Row 1 is USD@200. At time 150, should get row 0.
        assert_eq!(result.unwrap(), 0);

        // Probe at 250 → should find version 200 (row 1)
        let result = index.probe_at_time(usd_key.as_ref(), 250);
        assert_eq!(result.unwrap(), 1);
    }

    #[test]
    fn test_versioned_index_no_version_before_ts() {
        let batch = versioned_table_batch();
        let index = VersionedIndex::build(&batch, &[0], 1).unwrap();

        let key_sf = vec![SortField::new(DataType::Utf8)];
        let converter = RowConverter::new(key_sf).unwrap();
        let eur_col = Arc::new(StringArray::from(vec!["EUR"]));
        let eur_rows = converter.convert_columns(&[eur_col]).unwrap();
        let eur_key = eur_rows.row(0);

        // EUR versions start at 100. Probe at 50 → None
        let result = index.probe_at_time(eur_key.as_ref(), 50);
        assert!(result.is_none());
    }

    /// Helper to build a VersionedLookupJoinExec for tests.
    fn build_versioned_exec(
        table: RecordBatch,
        stream: &RecordBatch,
        join_type: LookupJoinType,
    ) -> VersionedLookupJoinExec {
        let input = batch_exec(stream.clone());
        let index = Arc::new(VersionedIndex::build(&table, &[0], 1).unwrap());
        let key_sort_fields = vec![SortField::new(DataType::Utf8)];
        let mut output_fields = stream.schema().fields().to_vec();
        output_fields.extend(table.schema().fields().iter().cloned());
        let output_schema = Arc::new(Schema::new(output_fields));
        VersionedLookupJoinExec::try_new(
            input,
            table,
            index,
            vec![1], // stream key col: currency
            2,       // stream time col: event_ts
            join_type,
            output_schema,
            key_sort_fields,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_versioned_join_exec_inner() {
        let table = versioned_table_batch();
        let stream = stream_batch_with_time();
        let exec = build_versioned_exec(table, &stream, LookupJoinType::Inner);

        let ctx = Arc::new(TaskContext::default());
        let stream_out = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream_out.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // Row 1: order_id=1, USD, ts=150 → USD@100 (rate=1.0)
        // Row 2: order_id=2, EUR, ts=160 → EUR@150 (rate=0.90)
        // Row 3: order_id=3, USD, ts=250 → USD@200 (rate=1.1)
        // Row 4: order_id=4, EUR, ts=50 → no EUR version <= 50 → SKIP (inner)
        assert_eq!(batch.num_rows(), 3);

        let rates = batch
            .column(5) // rate is 6th column (3 stream + 3 table, rate is table col 2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((rates.value(0) - 1.0).abs() < f64::EPSILON); // USD@100
        assert!((rates.value(1) - 0.90).abs() < f64::EPSILON); // EUR@150
        assert!((rates.value(2) - 1.1).abs() < f64::EPSILON); // USD@200
    }

    #[tokio::test]
    async fn test_versioned_join_exec_left_outer() {
        let table = versioned_table_batch();
        let stream = stream_batch_with_time();
        let exec = build_versioned_exec(table, &stream, LookupJoinType::LeftOuter);

        let ctx = Arc::new(TaskContext::default());
        let stream_out = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream_out.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // All 4 rows present (left outer)
        assert_eq!(batch.num_rows(), 4);

        // Row 4 (EUR@50): no version → null rate
        let rates = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(rates.is_null(3), "EUR@50 should have null rate");
    }

    #[test]
    fn test_versioned_index_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let index = VersionedIndex::build(&batch, &[0], 1).unwrap();
        assert!(index.map.is_empty());
    }

    #[test]
    fn test_versioned_lookup_registry() {
        let registry = LookupTableRegistry::new();
        let table = versioned_table_batch();
        let index = Arc::new(VersionedIndex::build(&table, &[0], 1).unwrap());

        registry.register_versioned(
            "rates",
            VersionedLookupState {
                batch: table,
                index,
                key_columns: vec!["currency".to_string()],
                version_column: "valid_from".to_string(),
                stream_time_column: "event_ts".to_string(),
            },
        );

        let entry = registry.get_entry("rates");
        assert!(entry.is_some());
        assert!(matches!(entry.unwrap(), RegisteredLookup::Versioned(_)));

        // get() should return None for versioned entries (snapshot-only)
        assert!(registry.get("rates").is_none());
    }
}
