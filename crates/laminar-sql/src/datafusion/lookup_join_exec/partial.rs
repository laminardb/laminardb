//! Partial lookup execution with cache hits and bounded source fallback.
//!
//! The operator owns miss collection, one fallback lookup per batch, and deterministic reassembly
//! into original stream-row order.

use super::{
    fmt, take, Arc, Array, Boundedness, ColumnId, DataFusionError, Debug, DisplayAs,
    DisplayFormatType, EmissionType, EquivalenceProperties, ExecutionPlan, Formatter, LexOrdering,
    LookupJoinType, LookupMemoryCache, LookupSourceDyn, Partitioning, PlanProperties, RecordBatch,
    RecordBatchStreamAdapter, Result, RowConverter, Schema, SchemaRef, Semaphore,
    SendableRecordBatchStream, SortField, StreamExt, TaskContext, UInt32Array,
    LOOKUP_SOURCE_TIMEOUT,
};

/// Physical plan that probes a bounded lookup cache per key for each input batch.
///
/// Used for on-demand tables whose full dataset does not fit in memory.
pub struct PartialLookupJoinExec {
    input: Arc<dyn ExecutionPlan>,
    lookup_cache: Arc<LookupMemoryCache>,
    stream_key_indices: Vec<usize>,
    join_type: LookupJoinType,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    /// Prebuilt `RowConverter` — built once at planning time, reused on
    /// every `execute()`. Previously rebuilt per-cycle.
    converter: Arc<RowConverter>,
    stream_field_count: usize,
    lookup_schema: SchemaRef,
    source: Option<Arc<dyn LookupSourceDyn>>,
    fetch_semaphore: Arc<Semaphore>,
    projection: Vec<ColumnId>,
}

impl PartialLookupJoinExec {
    /// Creates a new partial lookup join executor.
    ///
    /// # Errors
    ///
    /// Returns an error if the output schema cannot be constructed.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        lookup_cache: Arc<LookupMemoryCache>,
        stream_key_indices: Vec<usize>,
        key_sort_fields: Vec<SortField>,
        join_type: LookupJoinType,
        lookup_schema: SchemaRef,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        Self::try_new_with_source(
            input,
            lookup_cache,
            stream_key_indices,
            key_sort_fields,
            join_type,
            lookup_schema,
            output_schema,
            None,
            Arc::new(Semaphore::new(64)),
            vec![],
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
        lookup_cache: Arc<LookupMemoryCache>,
        stream_key_indices: Vec<usize>,
        key_sort_fields: Vec<SortField>,
        join_type: LookupJoinType,
        lookup_schema: SchemaRef,
        output_schema: SchemaRef,
        source: Option<Arc<dyn LookupSourceDyn>>,
        fetch_semaphore: Arc<Semaphore>,
        projection: Vec<ColumnId>,
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

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        ));

        let stream_field_count = input.schema().fields().len();
        let converter = Arc::new(RowConverter::new(key_sort_fields)?);

        Ok(Self {
            input,
            lookup_cache,
            stream_key_indices,
            join_type,
            schema: output_schema,
            properties,
            converter,
            stream_field_count,
            lookup_schema,
            source,
            fetch_semaphore,
            projection,
        })
    }
}

impl Debug for PartialLookupJoinExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialLookupJoinExec")
            .field("join_type", &self.join_type)
            .field("stream_keys", &self.stream_key_indices)
            .field("cache_table_id", &self.lookup_cache.table_id())
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
                    self.lookup_cache.len(),
                )
            }
            DisplayFormatType::TreeRender => write!(f, "PartialLookupJoinExec"),
        }
    }
}

impl ExecutionPlan for PartialLookupJoinExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "PartialLookupJoinExec"
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
                "PartialLookupJoinExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self {
            input: children.swap_remove(0),
            lookup_cache: Arc::clone(&self.lookup_cache),
            stream_key_indices: self.stream_key_indices.clone(),
            join_type: self.join_type,
            schema: Arc::clone(&self.schema),
            properties: self.properties.clone(),
            converter: Arc::clone(&self.converter),
            stream_field_count: self.stream_field_count,
            lookup_schema: Arc::clone(&self.lookup_schema),
            source: self.source.clone(),
            fetch_semaphore: Arc::clone(&self.fetch_semaphore),
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
        let lookup_cache = Arc::clone(&self.lookup_cache);
        let stream_key_indices = self.stream_key_indices.clone();
        let join_type = self.join_type;
        let schema = self.schema();
        let stream_field_count = self.stream_field_count;
        let lookup_schema = Arc::clone(&self.lookup_schema);
        let source = self.source.clone();
        let fetch_semaphore = Arc::clone(&self.fetch_semaphore);
        let projection = self.projection.clone();

        let output = input_stream.then(move |result| {
            let lookup_cache = Arc::clone(&lookup_cache);
            let converter = Arc::clone(&converter);
            let stream_key_indices = stream_key_indices.clone();
            let schema = Arc::clone(&schema);
            let lookup_schema = Arc::clone(&lookup_schema);
            let source = source.clone();
            let fetch_semaphore = Arc::clone(&fetch_semaphore);
            let projection = projection.clone();
            async move {
                let batch = result?;
                if batch.num_rows() == 0 {
                    return Ok(RecordBatch::new_empty(Arc::clone(&schema)));
                }
                probe_partial_batch_with_fallback(
                    &batch,
                    &converter,
                    &lookup_cache,
                    &stream_key_indices,
                    join_type,
                    &schema,
                    stream_field_count,
                    &lookup_schema,
                    source.as_deref(),
                    &fetch_semaphore,
                    &projection,
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

/// Probes the lookup cache for each row in `stream_batch`, falling back
/// to the async source for cache misses. Inserts source results into
/// the cache before building the output.
// PERF: This batch probe keeps cache lookup, fallback application, and output assembly together so
// row indices and optional matches stay in one allocation domain; extracting a generic context
// would add hot-path indirection without making the apply order easier to audit.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn probe_partial_batch_with_fallback(
    stream_batch: &RecordBatch,
    converter: &RowConverter,
    lookup_cache: &LookupMemoryCache,
    stream_key_indices: &[usize],
    join_type: LookupJoinType,
    output_schema: &SchemaRef,
    stream_field_count: usize,
    lookup_schema: &SchemaRef,
    source: Option<&dyn LookupSourceDyn>,
    fetch_semaphore: &Semaphore,
    projection: &[ColumnId],
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
        let result = lookup_cache.get_cached(key.as_ref());
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

            // Propagate fetch failures instead of silently serving cache-only
            // results: returning Err replays the cycle rather than dropping
            // inner-join matches or NULL-filling left-join rows.
            let results = match tokio::time::timeout(
                LOOKUP_SOURCE_TIMEOUT,
                source.query_batch(&key_refs, &[], projection),
            )
            .await
            {
                Ok(Ok(results)) => results,
                Ok(Err(e)) => {
                    return Err(DataFusionError::Execution(format!(
                        "lookup source query failed ({} keys): {e}",
                        miss_keys.len()
                    )));
                }
                Err(_elapsed) => {
                    return Err(DataFusionError::Execution(format!(
                        "lookup source query timed out after {LOOKUP_SOURCE_TIMEOUT:?} \
                         ({} keys)",
                        miss_keys.len()
                    )));
                }
            };

            if results.len() != miss_keys.len() {
                return Err(DataFusionError::Execution(format!(
                    "Lookup source returned mismatched results cardinality. Expected {} results, but got {} results. Miss keys: {:?}",
                    miss_keys.len(),
                    results.len(),
                    miss_keys
                )));
            }

            for ((idx, key_bytes), maybe_batch) in miss_keys.iter().zip(results) {
                if let Some(batch) = maybe_batch {
                    lookup_cache.insert(key_bytes, batch.clone());
                    lookup_batches[*idx] = Some(batch);
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
