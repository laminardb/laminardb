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

use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use parking_lot::RwLock;

use arrow::compute::take;
use arrow::row::{RowConverter, SortField};
use arrow_array::{Array, RecordBatch, UInt32Array};
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
use laminar_core::lookup::lookup_cache::LookupMemoryCache;
use laminar_core::lookup::source::{ColumnId, LookupSourceDyn};
use tokio::sync::Semaphore;

use super::lookup_join::{LookupJoinNode, LookupJoinType};

mod partial;

pub use partial::PartialLookupJoinExec;

/// Deadline for a cache-miss source fetch. The fetch is awaited inline on the
/// single compute thread, so without a bound a hung source wedges the pipeline.
const LOOKUP_SOURCE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// ── Registry ─────────────────────────────────────────────────────

/// Thread-safe registry of lookup table entries (snapshot or partial).
///
/// The db layer populates this when `CREATE LOOKUP TABLE` executes;
/// the [`LookupJoinExtensionPlanner`] reads it at physical plan time.
#[derive(Default)]
pub struct LookupTableRegistry {
    tables: RwLock<HashMap<String, RegisteredLookup>>,
}

/// A registered lookup table entry — snapshot or partial (on-demand).
pub enum RegisteredLookup {
    /// Full snapshot: all rows pre-loaded in a single batch.
    Snapshot(Arc<LookupSnapshot>),
    /// Partial (on-demand): bounded lookup cache with S3-FIFO eviction.
    Partial(Arc<PartialLookupState>),
}

/// Point-in-time snapshot of a lookup table for join execution.
pub struct LookupSnapshot {
    /// All rows concatenated into a single batch.
    pub batch: RecordBatch,
}

/// State for a partial (on-demand) lookup table.
pub struct PartialLookupState {
    /// Bounded in-memory cache with S3-FIFO eviction.
    pub lookup_cache: Arc<LookupMemoryCache>,
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
    /// Column indices (into `schema`) to fetch from the source — the union of
    /// every column any query references, plus the key. Empty = fetch all.
    /// Per-table (the cache is shared across queries), so it must be a superset.
    pub projection: Vec<ColumnId>,
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
            RegisteredLookup::Partial(_) => None,
        }
    }

    /// Returns the registered lookup entry (snapshot or partial).
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn get_entry(&self, name: &str) -> Option<RegisteredLookup> {
        let tables = self.tables.read();
        tables.get(&name.to_lowercase()).map(|e| match e {
            RegisteredLookup::Snapshot(s) => RegisteredLookup::Snapshot(Arc::clone(s)),
            RegisteredLookup::Partial(p) => RegisteredLookup::Partial(Arc::clone(p)),
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
    properties: Arc<PlanProperties>,
    /// Prebuilt `RowConverter` for encoding probe keys. Shared across
    /// every `execute()` call so we don't rebuild per-type encoders on
    /// every cycle of a cached physical plan.
    converter: Arc<RowConverter>,
    stream_field_count: usize,
    projection: Vec<usize>,
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
        let converter = Arc::new(RowConverter::new(key_sort_fields)?);

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

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));

        let stream_field_count = input.schema().fields().len();
        let projection = (0..(stream_field_count + lookup_batch.num_columns())).collect();

        Ok(Self {
            input,
            index: Arc::new(index),
            lookup_batch: Arc::new(lookup_batch),
            stream_key_indices,
            join_type,
            schema: output_schema,
            properties,
            converter,
            stream_field_count,
            projection,
        })
    }

    /// Sets the projection list for output columns.
    #[must_use]
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = projection;
        self
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

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
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
            converter: Arc::clone(&self.converter),
            stream_field_count: self.stream_field_count,
            projection: self.projection.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let converter = Arc::clone(&self.converter);
        let index = Arc::clone(&self.index);
        let lookup_batch = Arc::clone(&self.lookup_batch);
        let stream_key_indices = self.stream_key_indices.clone();
        let join_type = self.join_type;
        let schema = self.schema();
        let stream_field_count = self.stream_field_count;
        let projection = self.projection.clone();

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
                &projection,
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
    projection: &[usize],
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
    let mut columns = Vec::with_capacity(stream_field_count + lookup_batch.num_columns());

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

    let projected_columns: Vec<_> = projection.iter().map(|&idx| columns[idx].clone()).collect();

    debug_assert_eq!(
        projected_columns.len(),
        output_schema.fields().len(),
        "projected column count mismatch"
    );

    Ok(RecordBatch::try_new(
        Arc::clone(output_schema),
        projected_columns,
    )?)
}

fn resolve_physical_projection(
    logical_schema: &datafusion::common::DFSchema,
    stream_schema: &arrow::datatypes::Schema,
    lookup_schema: &arrow::datatypes::Schema,
    lookup_table: &str,
    lookup_alias: Option<&str>,
) -> Result<Vec<usize>> {
    let mut projection = Vec::with_capacity(logical_schema.fields().len());
    for idx in 0..logical_schema.fields().len() {
        let (qualifier, field) = logical_schema.qualified_field(idx);
        let name = field.name();
        let is_lookup = if let Some(relation) = qualifier {
            let table = relation.table();
            table == lookup_table || Some(table) == lookup_alias
        } else {
            // No qualifier: fallback to name-based detection.
            if stream_schema.index_of(name).is_ok() && lookup_schema.index_of(name).is_err() {
                false
            } else if lookup_schema.index_of(name).is_ok() && stream_schema.index_of(name).is_err()
            {
                true
            } else {
                stream_schema.index_of(name).is_ok()
            }
        };

        if is_lookup {
            let lookup_idx = lookup_schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "lookup join projection: output field '{name}' not found in lookup \
                     schema {lookup_schema:?}"
                ))
            })?;
            projection.push(stream_schema.fields().len() + lookup_idx);
        } else {
            let stream_idx = stream_schema.index_of(name).map_err(|_| {
                DataFusionError::Plan(format!(
                    "lookup join projection: output field '{name}' not found in stream \
                     schema {stream_schema:?}"
                ))
            })?;
            projection.push(stream_idx);
        }
    }
    Ok(projection)
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
                    Arc::clone(&partial_state.lookup_cache),
                    stream_key_indices,
                    partial_state.key_sort_fields.clone(),
                    lookup_node.join_type(),
                    Arc::clone(&partial_state.schema),
                    output_schema,
                    partial_state.source.clone(),
                    Arc::clone(&partial_state.fetch_semaphore),
                    partial_state.projection.clone(),
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

                let logical_schema = lookup_node.schema();
                let physical_projection = resolve_physical_projection(
                    logical_schema,
                    &stream_schema,
                    &lookup_schema,
                    lookup_node.lookup_table_name(),
                    lookup_node.lookup_alias(),
                )?;

                let output_schema = Arc::new(Schema::new(logical_schema.fields().to_vec()));

                let exec = LookupJoinExec::try_new(
                    input,
                    lookup_batch,
                    stream_key_indices,
                    lookup_key_indices,
                    lookup_node.join_type(),
                    output_schema,
                )?
                .with_projection(physical_projection);

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
mod tests;
