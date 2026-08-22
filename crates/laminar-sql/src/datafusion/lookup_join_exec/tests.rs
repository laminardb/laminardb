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
    let properties = Arc::new(PlanProperties::new(
        EquivalenceProperties::new(Arc::clone(&schema)),
        Partitioning::UnknownPartitioning(1),
        EmissionType::Final,
        Boundedness::Bounded,
    ));
    Arc::new(StreamExecStub {
        schema,
        batches: std::sync::Mutex::new(Some(batches)),
        stream_schema,
        properties,
    })
}

/// Minimal bounded exec for tests — produces one partition of batches.
struct StreamExecStub {
    schema: SchemaRef,
    batches: std::sync::Mutex<Option<Vec<RecordBatch>>>,
    stream_schema: SchemaRef,
    properties: Arc<PlanProperties>,
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &'static str {
        "StreamExecStub"
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
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
        },
    );
    assert_eq!(reg.get("t").unwrap().batch.num_rows(), 0);

    reg.register(
        "t",
        LookupSnapshot {
            batch: customers_batch(),
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

use laminar_core::lookup::lookup_cache::LookupMemoryCacheConfig;

fn make_lookup_cache() -> Arc<LookupMemoryCache> {
    Arc::new(LookupMemoryCache::new(
        1,
        LookupMemoryCacheConfig {
            capacity_bytes: 64 * 1024,
            ttl: None,
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

fn warm_cache(cache: &LookupMemoryCache) {
    let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();

    for (id, name) in [(1, "Alice"), (2, "Bob"), (3, "Charlie")] {
        let key_col = Int64Array::from(vec![id]);
        let rows = converter.convert_columns(&[Arc::new(key_col)]).unwrap();
        let key = rows.row(0);
        cache.insert(key.as_ref(), customer_row(id, name));
    }
}

fn make_partial_exec(join_type: LookupJoinType) -> PartialLookupJoinExec {
    let cache = make_lookup_cache();
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
    let cache = make_lookup_cache();
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
    let cache = make_lookup_cache();
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
    let cache = make_lookup_cache();
    let key_sort_fields = vec![SortField::new(DataType::Int64)];

    reg.register_partial(
        "customers",
        PartialLookupState {
            lookup_cache: cache,
            schema: customers_schema(),
            key_columns: vec!["id".into()],
            key_sort_fields,
            source: None,
            fetch_semaphore: Arc::new(Semaphore::new(64)),
            projection: Vec::new(),
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

    let cache = make_lookup_cache();
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
        vec![],
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
async fn partial_source_error_propagates() {
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

    let cache = make_lookup_cache();
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
        vec![],
    )
    .unwrap();

    // A source error must propagate (fail the cycle → replay), NOT silently
    // serve cache-only results that drop/NULL-fill the missed rows.
    let ctx = Arc::new(TaskContext::default());
    let result: std::result::Result<Vec<RecordBatch>, _> =
        exec.execute(0, ctx).unwrap().try_collect().await;
    let err = result.expect_err("source error must propagate, not degrade silently");
    assert!(
        err.to_string().contains("lookup source query failed"),
        "unexpected error: {err}"
    );
}

#[test]
fn registry_snapshot_entry_via_get_entry() {
    let reg = LookupTableRegistry::new();
    reg.register(
        "t",
        LookupSnapshot {
            batch: customers_batch(),
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
