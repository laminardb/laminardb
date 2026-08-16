use super::*;
#[cfg(feature = "cluster")]
use crate::operator_graph::{ManagedVnodeRestore, ManagedVnodeTransitionMode};
use arrow::array::{DictionaryArray, Int64Array, Int8Array, StringArray};
use arrow::datatypes::{Field, Int8Type, Schema};

#[test]
fn sql_capability_classification_is_shape_aware_and_fail_closed() {
    use crate::operator::capability::{
        ClusterExecutionStatus, ManagedStateContract, OperatorStateClass,
    };

    let context = laminar_sql::create_session_context();
    let classify = |sql| classify_sql_capability(sql, &context);

    let stateless = classify("SELECT key, value * 2 FROM events");
    assert_eq!(stateless.state_class, OperatorStateClass::Stateless);
    assert_eq!(stateless.cluster_status, ClusterExecutionStatus::DdlGuarded);

    let scalar = classify("SELECT UPPER(key) FROM events");
    assert_eq!(scalar.state_class, OperatorStateClass::Stateless);
    assert_eq!(scalar.cluster_status, ClusterExecutionStatus::DdlGuarded);

    let global = classify("SELECT COUNT(*) AS n FROM events");
    assert_eq!(global.state_class, OperatorStateClass::GlobalSingleton);
    assert_eq!(global.cluster_status, ClusterExecutionStatus::DdlGuarded);
    assert_eq!(
        global.managed_state,
        Some(ManagedStateContract::SqlAggregateV1)
    );

    let keyed = classify("SELECT key, SUM(value) AS total FROM events GROUP BY key");
    assert_eq!(keyed.state_class, OperatorStateClass::VnodeKeyed);
    assert_eq!(keyed.cluster_status, ClusterExecutionStatus::DdlGuarded);
    assert_eq!(
        keyed.managed_state,
        Some(ManagedStateContract::SqlAggregateV1)
    );

    let window_keyed = classify(
        "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), SUM(value) FROM events \
             GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
    );
    assert_eq!(window_keyed.state_class, OperatorStateClass::VnodeKeyed);
    let ClusterExecutionStatus::Rejected { reason } = window_keyed.cluster_status else {
        panic!("windowed aggregate must remain rejected")
    };
    assert!(reason.contains("timer"), "{reason}");
    assert!(reason.contains("watermark"), "{reason}");
    assert_eq!(window_keyed.managed_state, None);

    let analytic = classify("SELECT SUM(value) OVER (PARTITION BY key) AS running FROM events");
    assert_eq!(analytic.state_class, OperatorStateClass::LocalOnly);
    assert!(matches!(
        analytic.cluster_status,
        ClusterExecutionStatus::Rejected { .. }
    ));

    for ambiguous_sql in [
        "SELECT mystery(value) FROM events",
        "SELECT DISTINCT key FROM events",
        "SELECT key FROM events GROUP BY key",
        "SELECT key FROM (SELECT key FROM events) nested",
        "SELECT a.key FROM events a JOIN other b ON a.key = b.key",
        "SELECT COUNT(*) FROM (SELECT * FROM events) nested",
        "WITH nested AS (SELECT * FROM events) SELECT COUNT(*) FROM nested",
        "SELECT COUNT(*) FROM events a JOIN other b ON a.key = b.key",
        "SELECT DISTINCT COUNT(*) FROM events",
        "SELECT key FROM events ORDER BY key",
        "SELECT COUNT(*) AS n FROM events LIMIT 1",
        "SELECT key FROM events; SELECT key FROM events",
    ] {
        let ambiguous = classify(ambiguous_sql);
        assert_eq!(
            ambiguous.state_class,
            OperatorStateClass::LocalOnly,
            "{ambiguous_sql}"
        );
        assert!(
            matches!(
                ambiguous.cluster_status,
                ClusterExecutionStatus::Rejected { .. }
            ),
            "{ambiguous_sql}"
        );
    }

    let malformed = classify("not sql");
    assert_eq!(malformed.state_class, OperatorStateClass::LocalOnly);
    assert!(matches!(
        malformed.cluster_status,
        ClusterExecutionStatus::Rejected { .. }
    ));
}

#[tokio::test]
async fn managed_aggregate_initializes_before_receiving_input() {
    let (context, batch) = context_and_batch();
    let key_group_count = KeyGroupCount::try_from(8_u16).unwrap();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        key_group_count,
    );

    operator.initialize_managed_state().await.unwrap();

    let QueryState::Agg(ref aggregate) = operator.state else {
        panic!("expected initialized aggregate state");
    };
    assert_eq!(aggregate.key_group_count(), key_group_count);
    let empty_accounting = operator
        .managed_state_accounting()
        .expect("initialized aggregate must report managed state");
    assert!(empty_accounting.live > 0);
    assert_eq!(empty_accounting.prepared, 0);
    assert_eq!(empty_accounting.retired, 0);

    operator.process(&[vec![batch]], &[i64::MIN]).await.unwrap();
    let populated_accounting = operator.managed_state_accounting().unwrap();
    assert!(populated_accounting.live > empty_accounting.live);
    assert_eq!(populated_accounting.prepared, 0);
    assert_eq!(populated_accounting.retired, 0);
    assert!(operator.checkpoint().unwrap().is_none());
    let captured = operator
        .checkpoint_vnodes(&(0..8).collect::<Vec<_>>(), 8, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(captured.len(), 8);
    assert!(captured.iter().all(|frame| frame.state.is_some()));
}

#[tokio::test]
async fn managed_aggregate_rejects_nested_checkpoint_types_at_initialization() {
    let cases = [
            (
                "nested-group",
                "SELECT make_array(key) AS keys, COUNT(*) AS count FROM events GROUP BY make_array(key)",
                "group key",
            ),
            (
                "nested-result",
                "SELECT MIN(struct(key, value)) AS min_pair FROM events",
                "emitted result",
            ),
        ];

    for (name, sql, component) in cases {
        let (context, _) = context_and_batch();
        let mut operator = SqlQueryOperator::new(name, sql, context, None, false);
        let error = operator
            .initialize_managed_state()
            .await
            .expect_err("nested aggregate checkpoint types must fail during initialization");
        assert!(matches!(&error, DbError::Unsupported(_)), "{error}");
        assert!(
            error
                .to_string()
                .contains(laminar_core::error_codes::SQL_UNSUPPORTED),
            "{error}"
        );
        assert!(error.to_string().contains(component), "{error}");
        assert!(matches!(operator.state, QueryState::Uninit));
    }
}

#[tokio::test]
async fn local_full_state_aggregate_reuses_budgeted_final_output_on_empty_cycles() {
    let (context, batch) = context_and_batch();
    let mut operator = SqlQueryOperator::new(
        "cached-counts",
        "SELECT key, COUNT(*) AS count FROM events GROUP BY key",
        context,
        None,
        false,
    );

    let first = operator
        .process(&[vec![batch.clone()]], &[i64::MIN])
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    let cached_bytes = operator
        .cached_local_aggregate_output
        .as_ref()
        .expect("successful final post-HAVING output must be cached")
        .retained_bytes;
    assert!(
        cached_bytes
            >= first
                .iter()
                .map(RecordBatch::get_array_memory_size)
                .sum::<usize>(),
        "the cache charge must cover retained Arrow backing allocations"
    );
    let QueryState::Agg(aggregate) = &operator.state else {
        panic!("count query must use aggregate state");
    };
    assert_eq!(
        operator.managed_state_accounting().unwrap().live,
        aggregate
            .accounted_state_bytes()
            .checked_add(cached_bytes)
            .unwrap(),
        "the retained output is part of managed live state"
    );

    let empty = operator.process(&[Vec::new()], &[123]).await.unwrap();
    assert_eq!(empty.len(), first.len());
    for (original, reused) in first.iter().zip(&empty) {
        assert_eq!(original.schema(), reused.schema());
        for (original, reused) in original.columns().iter().zip(reused.columns()) {
            assert!(
                Arc::ptr_eq(original, reused),
                "an empty cycle must shallow-clone the cached Arrow output"
            );
        }
    }

    let updated = operator
        .process(&[vec![batch.slice(0, 1)]], &[456])
        .await
        .unwrap();
    assert_eq!(updated.len(), 1);
    assert!(
        first[0]
            .columns()
            .iter()
            .zip(updated[0].columns())
            .any(|(before, after)| !Arc::ptr_eq(before, after)),
        "a state mutation must replace the cached emission"
    );
    let updated_empty = operator.process(&[Vec::new()], &[789]).await.unwrap();
    assert!(updated[0]
        .columns()
        .iter()
        .zip(updated_empty[0].columns())
        .all(|(original, reused)| Arc::ptr_eq(original, reused)));

    let QueryState::Agg(aggregate) = &operator.state else {
        panic!("count query must retain aggregate state");
    };
    let aggregate_bytes = aggregate.accounted_state_bytes();
    let updated_cache_bytes = operator
        .cached_local_aggregate_output
        .as_ref()
        .expect("updated full-state output must replace the cache")
        .retained_bytes;
    let tight_budget = aggregate_bytes
        .checked_add(updated_cache_bytes)
        .and_then(|bytes| bytes.checked_sub(1))
        .unwrap();
    operator.set_managed_state_budget(tight_budget);
    assert!(operator.cached_local_aggregate_output.is_none());

    let tight_output = operator
        .process(&[vec![batch.slice(0, 1)]], &[1_000])
        .await
        .expect("declining the optional cache must not fail aggregate processing");
    assert_eq!(tight_output.len(), 1);
    assert!(
        operator.cached_local_aggregate_output.is_none(),
        "live aggregate plus output above the operator budget must not be cached"
    );
    let uncached_empty = operator
        .process(&[Vec::new()], &[1_001])
        .await
        .expect("an uncached empty cycle must retain explicit full-state semantics");
    assert_eq!(uncached_empty.len(), 1);
    assert!(operator.cached_local_aggregate_output.is_none());
    assert!(tight_output[0]
        .columns()
        .iter()
        .zip(uncached_empty[0].columns())
        .any(|(before, after)| !Arc::ptr_eq(before, after)));
}

#[tokio::test]
async fn changelog_aggregate_having_is_rejected_at_state_startup() {
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new(
        "qualified-sums",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key HAVING SUM(value) > 0",
        context,
        None,
        true,
    );

    let error = operator
        .initialize_managed_state()
        .await
        .expect_err("changelog HAVING must fail before state becomes executable");
    assert!(
        error.to_string().contains("transition-aware HAVING"),
        "{error}"
    );
    assert!(matches!(operator.state, QueryState::Uninit));
}

#[tokio::test]
async fn weighted_projection_compiled_and_cached_paths_share_one_sql_envelope() {
    let context = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new(
            laminar_core::changelog::WEIGHT_COLUMN,
            DataType::Int64,
            false,
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![1, -1, 2])),
        ],
    )
    .unwrap();
    let provider = Arc::new(laminar_sql::datafusion::LiveSourceProvider::new(schema));
    let handle = provider.handle();
    context.register_table("changes", provider).unwrap();
    handle.swap(vec![batch.clone()]);
    let sql = "SELECT value + 1 AS adjusted FROM changes WHERE id >= 2";

    let mut compiled =
        SqlQueryOperator::new("weighted-compiled", sql, context.clone(), None, false);
    compiled.lazy_init().await.unwrap();
    assert!(matches!(compiled.state, QueryState::Compiled(_)));
    let compiled_output = compiled
        .process(&[vec![batch.clone()]], &[i64::MIN])
        .await
        .unwrap();

    let mut cached = SqlQueryOperator::new("weighted-cached", sql, context.clone(), None, false);
    cached.lazy_init().await.unwrap();
    assert_eq!(cached.sql.matches("__weight").count(), 2);
    cached.build_and_cache_physical_plan().await.unwrap();
    assert!(matches!(cached.state, QueryState::CachedPlan(_)));
    let cached_output = cached
        .process(&[vec![batch.clone()]], &[i64::MIN])
        .await
        .unwrap();

    assert_eq!(compiled_output.len(), 1);
    assert_eq!(cached_output.len(), 1);
    assert_eq!(compiled_output[0].schema(), cached_output[0].schema());
    let weight = laminar_core::changelog::WEIGHT_COLUMN;
    let weight_field = compiled_output[0].schema().field(1).clone();
    assert_eq!(weight_field.name(), weight);
    assert!(!weight_field.is_nullable());
    for output in [&compiled_output, &cached_output] {
        let adjusted = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let weights = output[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(adjusted.values(), &[21, 31]);
        assert_eq!(weights.values(), &[-1, 2]);
    }

    let wildcard_sql = "SELECT id AS copy, * FROM changes WHERE id >= 2";
    let mut wildcard_compiled = SqlQueryOperator::new(
        "weighted-wildcard-compiled",
        wildcard_sql,
        context.clone(),
        None,
        false,
    );
    wildcard_compiled.lazy_init().await.unwrap();
    assert!(matches!(wildcard_compiled.state, QueryState::Compiled(_)));
    let wildcard_compiled_output = wildcard_compiled
        .process(&[vec![batch.clone()]], &[i64::MIN])
        .await
        .unwrap();

    let mut wildcard_cached = SqlQueryOperator::new(
        "weighted-wildcard-cached",
        wildcard_sql,
        context,
        None,
        false,
    );
    wildcard_cached.lazy_init().await.unwrap();
    wildcard_cached
        .build_and_cache_physical_plan()
        .await
        .unwrap();
    assert!(matches!(wildcard_cached.state, QueryState::CachedPlan(_)));
    let wildcard_cached_output = wildcard_cached
        .process(&[vec![batch]], &[i64::MIN])
        .await
        .unwrap();

    assert_eq!(wildcard_compiled_output.len(), 1);
    assert_eq!(wildcard_cached_output.len(), 1);
    assert_eq!(
        wildcard_compiled_output[0].schema(),
        wildcard_cached_output[0].schema()
    );
    assert_eq!(
        wildcard_compiled_output[0]
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        ["copy", "id", "value", weight]
    );
    for output in [&wildcard_compiled_output, &wildcard_cached_output] {
        let copy = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let weights = output[0]
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(copy.values(), &[2, 3]);
        assert_eq!(weights.values(), &[-1, 2]);
    }
}

#[tokio::test]
async fn local_aggregate_coalescing_preserves_append_rows_and_state() {
    let context = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let seed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["seed"])),
            Arc::new(Int64Array::from(vec![0])),
        ],
    )
    .unwrap();
    let table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]]).unwrap();
    context.register_table("changes", Arc::new(table)).unwrap();

    let mut tiny_inputs = Vec::new();
    let mut expected_projected_rows = Vec::new();
    for index in 0..2_050usize {
        let key = format!("k{}", index % 7);
        let value = i64::try_from(index).unwrap() + 1;
        tiny_inputs.push(
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(vec![key.as_str()])),
                    Arc::new(Int64Array::from(vec![value])),
                ],
            )
            .unwrap(),
        );
        expected_projected_rows.push((key, value));
    }
    let combined_input = arrow::compute::concat_batches(&schema, tiny_inputs.as_slice()).unwrap();
    let sql = "SELECT key, COUNT(*) AS match_count, MAX(value) AS max_value \
                   FROM changes GROUP BY key";

    let mut coalesced_operator =
        SqlQueryOperator::new("coalesced", sql, context.clone(), None, false);
    coalesced_operator.lazy_init().await.unwrap();
    let projected = coalesced_operator
        .pre_aggregate(&tiny_inputs)
        .await
        .unwrap();
    let QueryState::Agg(aggregate) = &coalesced_operator.state else {
        panic!("coalesced operator must be initialized");
    };
    assert!(aggregate.certifies_local_input_coalescing());
    let projected_schema = projected[0].schema();
    let coalesced = coalesced_operator
        .prepare_local_aggregate_batches(projected)
        .unwrap();
    assert_eq!(
        coalesced
            .iter()
            .map(RecordBatch::num_rows)
            .collect::<Vec<_>>(),
        [1_024, 1_024, 2],
        "the actual aggregate apply input is bounded by the local row target"
    );
    assert!(coalesced.iter().all(|batch| {
        batch.schema().as_ref() == projected_schema.as_ref()
            && batch.num_rows() <= LOCAL_AGG_COALESCE_MAX_BATCH_ROWS
            && laminar_core::shuffle::logical_batch_bytes(batch).unwrap()
                <= LOCAL_AGG_COALESCE_TARGET_BATCH_BYTES
    }));
    let actual_projected_rows = coalesced
        .iter()
        .flat_map(|batch| {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows())
                .map(|row| (keys.value(row).to_owned(), values.value(row)))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(actual_projected_rows, expected_projected_rows);

    let tiny_ports = vec![tiny_inputs];
    let coalesced_output = coalesced_operator
        .process(&tiny_ports, &[777])
        .await
        .unwrap();
    let mut single_batch_operator =
        SqlQueryOperator::new("single-batch", sql, context, None, false);
    let single_batch_ports = vec![vec![combined_input]];
    let single_batch_output = single_batch_operator
        .process(&single_batch_ports, &[777])
        .await
        .unwrap();

    let output_rows = |batches: &[RecordBatch]| {
        let mut rows = std::collections::BTreeMap::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let maxima = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.insert(
                    keys.value(row).to_owned(),
                    (counts.value(row), maxima.value(row)),
                );
            }
        }
        rows
    };
    assert_eq!(
        coalesced_output[0].schema(),
        single_batch_output[0].schema()
    );
    assert_eq!(
        output_rows(&coalesced_output),
        output_rows(&single_batch_output)
    );

    let QueryState::Agg(coalesced_state) = &coalesced_operator.state else {
        panic!("coalesced operator must retain aggregate state");
    };
    let QueryState::Agg(single_batch_state) = &single_batch_operator.state else {
        panic!("single-batch operator must retain aggregate state");
    };
    assert_eq!(coalesced_state.logical_group_count_for_test(), 7);
    assert_eq!(
        coalesced_state.working_set_snapshot_for_test(),
        single_batch_state.working_set_snapshot_for_test()
    );
}

#[test]
fn local_aggregate_coalescing_preserves_dictionary_batch_boundaries() {
    let batches = (0..130)
        .map(|index| {
            let values: arrow::array::ArrayRef =
                Arc::new(StringArray::from(vec![format!("dictionary-{index}")]));
            let dictionary =
                DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![0]), values).unwrap();
            RecordBatch::try_from_iter(vec![(
                "dictionary_value",
                Arc::new(dictionary) as arrow::array::ArrayRef,
            )])
            .unwrap()
        })
        .collect::<Vec<_>>();

    let preserved = coalesce_local_aggregate_batches("dictionary", batches).unwrap();
    assert_eq!(preserved.len(), 130);
    for (index, batch) in preserved.iter().enumerate() {
        let dictionary = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int8Type>>()
            .unwrap();
        let values = dictionary
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), format!("dictionary-{index}"));
    }
}

#[tokio::test]
async fn local_aggregate_coalescing_preserves_weighted_prefix_rejection() {
    let context = laminar_sql::create_session_context();
    let weight = laminar_core::changelog::WEIGHT_COLUMN;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new(weight, DataType::Int64, false),
    ]));
    let seed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["seed"])),
            Arc::new(Int64Array::from(vec![0])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]]).unwrap();
    context.register_table("changes", Arc::new(table)).unwrap();

    let weighted_batch = |row_weight| {
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(vec!["absent"])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![row_weight])),
            ],
        )
        .unwrap()
    };
    let mut operator = SqlQueryOperator::new(
        "weighted-prefix",
        "SELECT key, COUNT(*) AS count FROM changes GROUP BY key",
        context,
        None,
        false,
    );
    operator.lazy_init().await.unwrap();
    let QueryState::Agg(aggregate) = &operator.state else {
        panic!("weighted operator must be initialized");
    };
    assert!(!aggregate.certifies_local_input_coalescing());

    let weighted_inputs = vec![weighted_batch(-1), weighted_batch(1)];
    let projected = operator.pre_aggregate(&weighted_inputs).await.unwrap();
    let preserved = operator.prepare_local_aggregate_batches(projected).unwrap();
    assert_eq!(preserved.len(), 2);
    assert_eq!(
        preserved
            .iter()
            .map(|batch| {
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0)
            })
            .collect::<Vec<_>>(),
        [-1, 1]
    );

    let inputs = vec![weighted_inputs];
    let error = operator.process(&inputs, &[777]).await.unwrap_err();
    assert!(
        error.to_string().contains("input weight became negative"),
        "weighted batches must retain their prefix validation boundary: {error}"
    );
}

#[tokio::test]
async fn local_aggregate_coalescing_does_not_resegment_sum() {
    let (context, batch) = context_and_batch();
    let mut operator = SqlQueryOperator::new(
        "sum-boundaries",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
    );
    operator.lazy_init().await.unwrap();
    let QueryState::Agg(aggregate) = &operator.state else {
        panic!("SUM operator must be initialized");
    };
    assert!(!aggregate.certifies_local_input_coalescing());

    let inputs = vec![batch.slice(0, 1), batch.slice(1, 1)];
    let projected = operator.pre_aggregate(&inputs).await.unwrap();
    assert_eq!(projected.len(), 2);
    let preserved = operator.prepare_local_aggregate_batches(projected).unwrap();
    assert_eq!(
        preserved.len(),
        2,
        "SUM keeps its original overflow and floating-point reduction boundaries"
    );
}

pub(super) fn context_and_batch() -> (SessionContext, RecordBatch) {
    let context = laminar_sql::create_session_context();
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let seed = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["seed"])),
            Arc::new(Int64Array::from(vec![0])),
        ],
    )
    .unwrap();
    let table =
        datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![vec![seed]]).unwrap();
    context.register_table("events", Arc::new(table)).unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    (context, batch)
}

fn unaligned_aggregate_archive_transport(bytes: &[u8]) -> bytes::Bytes {
    let mut transport = vec![0_u8; bytes.len() + AGG_CHECKPOINT_ARCHIVE_ALIGNMENT];
    let base = transport.as_ptr() as usize;
    let offset = (0..AGG_CHECKPOINT_ARCHIVE_ALIGNMENT)
        .find(|offset| !(base + offset).is_multiple_of(AGG_CHECKPOINT_ARCHIVE_ALIGNMENT))
        .expect("an aggregate archive transport offset must be unaligned");
    transport[offset..offset + bytes.len()].copy_from_slice(bytes);
    let bytes = bytes::Bytes::from(transport).slice(offset..offset + bytes.len());
    assert_ne!(
        bytes
            .as_ptr()
            .align_offset(AGG_CHECKPOINT_ARCHIVE_ALIGNMENT),
        0
    );
    bytes
}

#[cfg(feature = "cluster")]
async fn cluster_scope(owners: [u64; 8]) -> ClusterShuffleConfig {
    use std::time::Duration;

    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::cluster::control::LeaseDeadline;
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::VnodeRegistry;

    let registry = Arc::new(VnodeRegistry::new(8));
    registry.set_assignment(Arc::from(owners.map(NodeId)));
    let incarnation = uuid::Uuid::from_u128(1);
    let receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), incarnation)
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1, incarnation));
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    receiver
        .install_process_lease_deadline(Arc::clone(&deadline))
        .unwrap();
    sender.install_process_lease_deadline(deadline).unwrap();
    let participants = owners
        .iter()
        .copied()
        .filter(|node| *node != 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect();
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &owners,
        participants,
    )
    .unwrap();
    sender.install_assignment_fence(&fence, &owners).unwrap();
    receiver.install_assignment_fence(&fence, &owners).unwrap();
    ClusterShuffleConfig {
        registry,
        sender,
        receiver,
        self_id: NodeId(1),
    }
}

#[cfg(feature = "cluster")]
fn portable_aggregate_whole_frame(
    predecessor: &laminar_core::checkpoint::CheckpointAssignmentFence,
    donor: u64,
    frontier: InputFrontier,
) -> Vec<u8> {
    let channels = predecessor
        .participant_ids()
        .into_iter()
        .filter(|peer| *peer != donor)
        .map(|peer| AggCheckpointChannel {
            peer,
            applied: frontier.into(),
            events: Vec::new(),
        })
        .collect();
    rkyv::to_bytes::<rkyv::rancor::Error>(&AggOpCheckpoint {
        version: AGG_OP_CHECKPOINT_VERSION,
        assignment_version: predecessor.assignment_version,
        owner_map_digest: predecessor.assignment_digest,
        self_id: donor,
        recovery_gen: 0,
        local_frontier: frontier.into(),
        effective_frontier: frontier.into(),
        remote_peer_cursor: None,
        channels,
    })
    .unwrap()
    .to_vec()
}

#[cfg(feature = "cluster")]
fn projected_batch_for_vnode(
    operator: &SqlQueryOperator,
    vnode: u32,
    value: i64,
) -> (String, RecordBatch) {
    let QueryState::Agg(aggregate) = &operator.state else {
        panic!("aggregate must be initialized");
    };
    let projection = aggregate.compiled_projection().unwrap();
    for index in 0..1_000 {
        let key = format!("K{index}");
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![key.as_str()])),
                Arc::new(Int64Array::from(vec![value])),
            ],
        )
        .unwrap();
        let projected = projection.evaluate(&input).unwrap();
        let routed = hash_rows_to_vnodes(
            &projected,
            aggregate.num_group_cols(),
            u32::from(operator.key_group_count),
        )
        .unwrap();
        if routed == [vnode] {
            return (key, projected);
        }
    }
    panic!("no test key hashes to vnode {vnode}");
}

#[cfg(feature = "cluster")]
fn projected_batch_for_key(operator: &SqlQueryOperator, key: &str, value: i64) -> RecordBatch {
    let QueryState::Agg(aggregate) = &operator.state else {
        panic!("aggregate must be initialized");
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let input = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![key])),
            Arc::new(Int64Array::from(vec![value])),
        ],
    )
    .unwrap();
    aggregate
        .compiled_projection()
        .unwrap()
        .evaluate(&input)
        .unwrap()
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpointed_remote_frontiers_compare_in_receiver_domain() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_shuffle(scope.clone());
    operator.effective_frontier = InputFrontier {
        watermark: Some(500),
        idle: false,
    };
    let idle = InputFrontier {
        watermark: Some(100),
        idle: true,
    };
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = idle;
    channel.accepted = idle;
    let assignment = scope.registry.assignment_version();
    let recovery = scope.receiver.recovery_gen();
    let active = |watermark| InputFrontier {
        watermark: Some(watermark),
        idle: false,
    };

    operator
        .stage_checkpointed_shuffle_frontier("sum", 2, active(100), assignment, recovery)
        .unwrap();
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
    operator
        .stage_checkpointed_shuffle_frontier("sum", 2, active(150), assignment, recovery)
        .unwrap();
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(500));
    operator
        .stage_checkpointed_shuffle_frontier("sum", 2, active(550), assignment, recovery)
        .unwrap();
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
    assert!(operator
        .stage_checkpointed_shuffle_frontier(
            "sum",
            2,
            InputFrontier {
                watermark: None,
                idle: false,
            },
            assignment,
            recovery,
        )
        .is_err());
    assert!(operator
        .stage_checkpointed_shuffle_frontier("sum", 2, active(525), assignment, recovery,)
        .is_err());
    assert_eq!(operator.peer_channels[&2].accepted.watermark, Some(550));
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restored_frontier_bootstrap_precedes_live_source_frontier() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    operator.initialize_managed_state().await.unwrap();
    let (key, local) = projected_batch_for_vnode(&operator, 0, 42);
    let buffered = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ])),
        vec![
            Arc::new(StringArray::from(vec![key.as_str()])),
            Arc::new(Int64Array::from(vec![42])),
        ],
    )
    .unwrap();
    operator.attach_cluster_shuffle(scope.clone());
    let restored = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let live = InputFrontier {
        watermark: Some(1_000),
        idle: false,
    };
    operator.local_frontier = restored;
    operator.effective_frontier = restored;
    operator.last_broadcast = InputFrontier::default();
    let channel = operator.peer_channels.get_mut(&2).unwrap();
    channel.applied = restored;
    channel.accepted = restored;

    assert!(!operator.wants_input());
    let assignment = scope.registry.versioned_snapshot();
    let bootstrap = operator.cluster_cycle_local_frontier(live, false).unwrap();
    assert_eq!(bootstrap, restored);
    let plan = operator
        .plan_cluster_batches(Vec::new(), bootstrap, &scope, &assignment, &[2])
        .unwrap();
    assert!(matches!(
        plan.outbound.as_slice(),
        [(
            2,
            ShuffleMessage::Frontier {
                watermark: Some(100),
                idle: false,
                ..
            }
        )]
    ));
    operator.process_cluster(&[Vec::new()], live).await.unwrap();
    let mut pending = operator.pending_cluster_input.take().unwrap();
    assert_eq!(pending.local_frontier, restored);
    assert!(pending.local_batches.is_empty());
    pending.send.take().unwrap().abort();

    // Simulate completion of the bootstrap send. The graph may now release its retained row,
    // and the ordinary node-local frontier is used without being globally frozen.
    operator.last_broadcast = restored;
    assert!(operator.wants_input());
    let admitted = operator.cluster_cycle_local_frontier(live, true).unwrap();
    assert_eq!(admitted, live);
    let plan = operator
        .plan_cluster_batches(vec![local], admitted, &scope, &assignment, &[2])
        .unwrap();
    assert_eq!(plan.local_batches.len(), 1);
    assert_eq!(plan.local_frontier, live);
    operator
        .process_cluster(&[vec![buffered]], live)
        .await
        .unwrap();
    let mut pending = operator.pending_cluster_input.take().unwrap();
    assert_eq!(pending.local_frontier, live);
    assert_eq!(pending.local_batches.len(), 1);
    pending.send.take().unwrap().abort();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn pending_send_drains_remote_sum_before_publishing_local_cut() {
    use std::time::Duration;

    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    operator.initialize_managed_state().await.unwrap();
    let (key, local) = projected_batch_for_vnode(&operator, 0, 8);
    let remote = projected_batch_for_key(&operator, &key, 34);
    let (_, outbound_batch) = projected_batch_for_vnode(&operator, 1, 1);
    operator.attach_cluster_shuffle(scope.clone());
    let frontier = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let assignment = scope.registry.versioned_snapshot();
    let plan = operator
        .plan_cluster_batches(
            vec![local, outbound_batch],
            frontier,
            &scope,
            &assignment,
            &[2],
        )
        .unwrap();
    assert_eq!(plan.local_batches.len(), 1);
    let accounted_bytes = operator.cluster_input_plan_bytes(&plan).unwrap();
    let AggClusterInputPlan {
        local_batches,
        outbound,
        local_frontier,
        effective_frontier: _,
    } = plan;
    let (release, held) = tokio::sync::oneshot::channel();
    let (completion_tx, completion) = tokio::sync::oneshot::channel();
    let send = tokio::spawn(async move {
        let _ = held.await;
        drop(outbound);
        let _ = completion_tx.send((Ok(()), None));
    });
    operator.pending_cluster_input = Some(PendingAggClusterInput {
        local_batches,
        outbound: None,
        local_frontier,
        send: Some(send),
        completion: Some(completion),
        accounted_bytes,
    });
    let version = assignment.version();
    let recovery = scope.receiver.recovery_gen();
    operator
        .stage_checkpointed_shuffle(
            "sum",
            crate::operator::RetainedBatch::restored_channel(
                remote,
                2,
                version,
                recovery,
                Arc::from([0_u32]),
            ),
            i64::MIN,
        )
        .unwrap();
    operator
        .stage_checkpointed_shuffle_frontier("sum", 2, frontier, version, recovery)
        .unwrap();

    let remote_output = tokio::time::timeout(
        Duration::from_millis(50),
        operator.process_cluster(&[Vec::new()], InputFrontier::default()),
    )
    .await
    .expect("held send blocked remote replay")
    .unwrap();
    assert_eq!(
        remote_output
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert_eq!(operator.queued_remote_events, 1);
    assert_eq!(operator.local_frontier, InputFrontier::default());
    assert!(!operator.wants_input());
    assert!(operator.checkpoint_drain_pending());
    assert!(operator.checkpoint_vnodes(&[0], 8, u64::MAX).is_err());

    release.send(()).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let output = operator
                .process_cluster(&[Vec::new()], InputFrontier::default())
                .await
                .unwrap();
            if operator.pending_cluster_input.is_none() {
                break output;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let total = output[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(total.value(0), 42);
    assert!(operator.pending_cluster_input.is_none());
    assert_eq!(operator.queued_remote_events, 0);
    assert_eq!(operator.local_frontier, frontier);
    assert_eq!(operator.effective_frontier, frontier);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn zero_admission_retry_has_no_runnable_spin() {
    let scope = cluster_scope([1, 2, 1, 1, 1, 1, 1, 1]).await;
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_shuffle(scope);
    let retry_plan = vec![(
        2,
        ShuffleMessage::Frontier {
            stage: "sum".to_string(),
            watermark: None,
            idle: false,
        },
    )];
    let (completion_tx, completion) = tokio::sync::oneshot::channel();
    assert!(completion_tx
        .send((
            Err(DbError::ShuffleNotReady("injected zero admission".into())),
            Some(retry_plan),
        ))
        .is_ok());
    let send = tokio::spawn(async {});
    operator.pending_cluster_input = Some(PendingAggClusterInput {
        local_batches: Vec::new(),
        outbound: None,
        local_frontier: InputFrontier::default(),
        send: Some(send),
        completion: Some(completion),
        accounted_bytes: 0,
    });
    assert!(!operator.deferred_work_is_runnable());
    operator
        .process_cluster(&[Vec::new()], InputFrontier::default())
        .await
        .unwrap();
    let pending = operator.pending_cluster_input.as_ref().unwrap();
    assert!(pending.send.is_some());
    assert!(pending.outbound.is_none());
    assert!(!operator.deferred_work_is_runnable());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn ordered_channel_checkpoint_roundtrips_under_budget() {
    let scope = cluster_scope([1, 2, 2, 2, 2, 2, 2, 2]).await;
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    operator.initialize_managed_state().await.unwrap();
    let (_, remote) = projected_batch_for_vnode(&operator, 0, 42);
    let remote_schema = remote.schema();
    let remote = arrow::compute::concat_batches(&remote_schema, &vec![remote; 512]).unwrap();
    operator.attach_cluster_shuffle(scope.clone());
    let version = scope.registry.assignment_version();
    let recovery = scope.receiver.recovery_gen();
    operator
        .stage_checkpointed_shuffle(
            "sum",
            crate::operator::RetainedBatch::restored_channel(
                remote,
                2,
                version,
                recovery,
                Arc::from([0_u32]),
            ),
            i64::MIN,
        )
        .unwrap();
    let frontier = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    operator
        .stage_checkpointed_shuffle_frontier("sum", 2, frontier, version, recovery)
        .unwrap();
    assert!(operator.checkpoint_capture(0).is_err());
    let capture = operator
        .checkpoint_capture(1 << 20)
        .unwrap()
        .expect("cluster aggregate always captures its channel cut");
    let mut staged = capture.retained_bytes();
    let encoded = capture.materialize(&mut staged, 1 << 20).unwrap();

    let assert_pristine = |operator: &SqlQueryOperator| {
        assert!(!operator.whole_restore_applied);
        assert_eq!(operator.local_frontier, InputFrontier::default());
        assert_eq!(operator.last_broadcast, InputFrontier::default());
        assert_eq!(operator.effective_frontier, InputFrontier::default());
        assert!(operator.remote_peer_cursor.is_none());
        assert_eq!(operator.queued_payload_bytes, 0);
        assert_eq!(operator.queued_event_capacity_bytes, 0);
        assert_eq!(operator.queued_remote_events, 0);
        assert!(operator.peer_channels.values().all(|channel| {
            channel.applied == InputFrontier::default()
                && channel.accepted == InputFrontier::default()
                && channel.events.is_empty()
        }));
    };

    let published = InputFrontier {
        watermark: Some(100),
        idle: false,
    };
    let revival = InputFrontier {
        watermark: Some(50),
        idle: false,
    };
    let mut malformed =
        rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(encoded.as_ref()).unwrap();
    malformed.local_frontier = published.into();
    malformed.effective_frontier = published.into();
    let malformed_channel = malformed.channels.first_mut().unwrap();
    malformed_channel.applied = AggCheckpointFrontier {
        watermark: Some(0),
        idle: true,
    };
    malformed_channel.events.swap(0, 1);
    let AggCheckpointEvent::Frontier {
        frontier: malformed_revival,
        ..
    } = &mut malformed_channel.events[0]
    else {
        panic!("expected queued frontier before malformed data");
    };
    *malformed_revival = revival.into();
    let malformed = rkyv::to_bytes::<rkyv::rancor::Error>(&malformed).unwrap();
    let (malformed_context, _) = context_and_batch();
    let mut malformed_restore = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        malformed_context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    malformed_restore.initialize_managed_state().await.unwrap();
    malformed_restore.attach_cluster_shuffle(scope.clone());
    let pristine_bytes = malformed_restore.checked_live_state_bytes().unwrap();
    assert!(matches!(
        malformed_restore.restore(OperatorCheckpoint {
            data: malformed.to_vec()
        }),
        Err(DbError::Checkpoint(_))
    ));
    assert_pristine(&malformed_restore);
    assert_eq!(
        malformed_restore.checked_live_state_bytes().unwrap(),
        pristine_bytes
    );

    let (restored_context, _) = context_and_batch();
    let mut restored = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        restored_context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    restored.initialize_managed_state().await.unwrap();
    restored.attach_cluster_shuffle(scope.clone());
    restored
        .restore(OperatorCheckpoint {
            data: encoded.to_vec(),
        })
        .unwrap();
    assert_eq!(restored.queued_remote_events, 2);
    assert!(restored.checkpoint_vnodes(&[0], 8, u64::MAX).is_ok());

    let decoded_accounted = restored.checked_live_state_bytes().unwrap();
    assert!(decoded_accounted > encoded.len());
    let decoded_budget = decoded_accounted - 1;
    assert!(decoded_budget >= encoded.len());
    let (limited_context, _) = context_and_batch();
    let mut limited = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        limited_context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    limited.initialize_managed_state().await.unwrap();
    limited.attach_cluster_shuffle(scope);
    let pristine_bytes = limited.checked_live_state_bytes().unwrap();
    limited.set_managed_state_budget(decoded_budget);
    assert!(matches!(
        limited.restore(OperatorCheckpoint {
            data: encoded.to_vec()
        }),
        Err(DbError::ManagedStateBudgetExceeded { .. })
    ));
    assert_pristine(&limited);
    assert_eq!(limited.checked_live_state_bytes().unwrap(), pristine_bytes);

    let output = restored
        .process_cluster(&[Vec::new()], InputFrontier::default())
        .await
        .unwrap();
    assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    restored
        .process_cluster(&[Vec::new()], InputFrontier::default())
        .await
        .unwrap();
    assert_eq!(restored.peer_channels[&2].applied, frontier);
    assert_eq!(restored.queued_remote_events, 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn aggregate_topology_transition_is_atomic_and_accounts_retired_channels() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let predecessor_owners = [1, 2, 1, 1, 1, 1, 1, 1];
    let target_owners = [1, 3, 1, 1, 1, 1, 1, 1];
    let scope = cluster_scope(predecessor_owners).await;
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        KeyGroupCount::try_from(8_u16).unwrap(),
    );
    operator.initialize_managed_state().await.unwrap();
    operator.attach_cluster_shuffle(scope.clone());
    let predecessor_version = scope.registry.assignment_version();
    let participant = |node_id| CheckpointParticipant {
        node_id,
        boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
    };
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &predecessor_owners,
        vec![participant(1), participant(2)],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        predecessor_version + 1,
        &target_owners,
        vec![participant(1), participant(3)],
    )
    .unwrap();

    let old_channel = operator.peer_channels.get_mut(&2).unwrap();
    old_channel.events.reserve(8);
    let retained_event_capacity = old_channel.events.capacity() * AGG_REMOTE_EVENT_CHARGE;
    operator.queued_event_capacity_bytes = retained_event_capacity;
    let pristine = operator.managed_state_accounting().unwrap();

    scope.registry.set_assignment_and_version(
        Arc::from(target_owners.map(NodeId)),
        target.assignment_version,
    );
    scope
        .sender
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    scope
        .receiver
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    let revoked = rustc_hash::FxHashSet::default();

    let mut wrong_digest = target.clone();
    wrong_digest.assignment_digest[0] ^= 1;
    let mut wrong_incarnation = target.clone();
    wrong_incarnation
        .participants
        .iter_mut()
        .find(|participant| participant.node_id == 1)
        .unwrap()
        .boot_incarnation = uuid::Uuid::from_u128(11);
    for invalid in [&wrong_digest, &wrong_incarnation] {
        assert!(operator
            .prepare_vnode_transition(ManagedVnodeTransition {
                predecessor: &predecessor,
                target: invalid,
                revoked: &revoked,
                restores: &[],
                whole_restores: &[],
                mode: ManagedVnodeTransitionMode::Live,
            })
            .is_err());
        assert!(operator.prepared_vnode_transition.is_none());
        assert!(operator.vnode_transition_cleanup.is_none());
        assert_eq!(
            operator.cluster_assignment.as_ref().unwrap().version(),
            predecessor_version
        );
        assert_eq!(operator.cluster_peers.as_ref(), &[2]);
        assert_eq!(
            operator.peer_channels[&2].events.capacity() * AGG_REMOTE_EVENT_CHARGE,
            retained_event_capacity
        );
        assert_eq!(operator.managed_state_accounting().unwrap(), pristine);
    }

    operator
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target,
            revoked: &revoked,
            restores: &[],
            whole_restores: &[],
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    operator.publish_vnode_transition();
    assert_eq!(
        operator.cluster_assignment.as_ref().unwrap().version(),
        target.assignment_version
    );
    assert_eq!(
        operator.cluster_assignment_digest,
        Some(target.assignment_digest)
    );
    assert_eq!(operator.cluster_peers.as_ref(), &[3]);
    assert!(operator.peer_channels.contains_key(&3));
    assert!(!operator.peer_channels.contains_key(&2));
    assert_eq!(operator.queued_event_capacity_bytes, 0);

    let SqlVnodeTransitionCleanup::Published {
        aggregate,
        topology,
    } = operator.vnode_transition_cleanup.as_ref().unwrap()
    else {
        panic!("aggregate transition must retain its displaced topology");
    };
    let topology_base = topology
        .assignment
        .owners()
        .len()
        .saturating_mul(std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>())
        .saturating_add(
            topology
                .peers
                .len()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(topology.channels.len().saturating_mul(
            std::mem::size_of::<(u64, AggPeerChannel)>() + AGG_PEER_CHANNEL_ENTRY_CHARGE,
        ));
    assert_eq!(
        topology.accounted_state_bytes(),
        topology_base + retained_event_capacity
    );
    assert_eq!(
        operator.managed_state_accounting().unwrap().retired,
        aggregate.accounted_state_bytes() + topology.accounted_state_bytes()
    );
    operator.finish_vnode_transition();
    assert!(operator.vnode_transition_cleanup.is_none());
    assert_eq!(operator.managed_state_accounting().unwrap().retired, 0);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn aggregate_checkpoint_bootstrap_requires_every_whole_donor_and_installs_common_cut() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let predecessor_owners = [2, 3, 2, 3, 2, 3, 2, 3];
    let target_owners = [1; 8];
    // The bootstrap process was not a predecessor owner, so attach its transport directly to
    // the recovery target while retaining the independent predecessor fence below.
    let scope = cluster_scope(target_owners).await;
    let (context, batch) = context_and_batch();
    let key_groups = KeyGroupCount::try_from(8_u16).unwrap();
    let participant = |node_id| CheckpointParticipant {
        node_id,
        boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
    };
    let predecessor_version = scope.registry.assignment_version();
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &predecessor_owners,
        vec![participant(2), participant(3)],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        predecessor_version + 1,
        &target_owners,
        vec![participant(1)],
    )
    .unwrap();
    scope.registry.set_assignment_and_version(
        Arc::from(target_owners.map(NodeId)),
        target.assignment_version,
    );
    scope
        .sender
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    scope
        .receiver
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    assert_eq!(scope.receiver.recovery_gen(), 0);

    let mut donors = Vec::new();
    let frontier = InputFrontier {
        watermark: Some(777),
        idle: false,
    };
    for donor in [2, 3] {
        let mut frame = rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(
            &portable_aggregate_whole_frame(&predecessor, donor, frontier),
        )
        .unwrap();
        // A fresh acquirer can legitimately have missed owner-only recovery rounds. The
        // donor's portable whole frame imports an empty, predecessor-bound channel cut, not
        // live transport-generation state, so a newer donor generation remains admissible.
        frame.recovery_gen = 7;
        donors.push((
            donor,
            rkyv::to_bytes::<rkyv::rancor::Error>(&frame)
                .unwrap()
                .to_vec(),
        ));
    }
    let whole_restores = donors
        .iter()
        .map(
            |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                participant_id: *participant_id,
                state,
            },
        )
        .collect::<Vec<_>>();

    let mut donor_state = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT SUM(value) AS total FROM events",
        context.clone(),
        None,
        false,
        key_groups,
    );
    donor_state.initialize_managed_state().await.unwrap();
    donor_state.process(&[vec![batch]], &[100]).await.unwrap();
    let vnode_frames = donor_state
        .checkpoint_vnodes(&[0], 8, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .map(|frame| {
            let capture = frame.state.unwrap();
            let mut staged = capture.retained_bytes();
            let state = capture.materialize(&mut staged, u64::MAX).unwrap();
            (frame.vnode, state)
        })
        .collect::<Vec<_>>();
    let restores = vnode_frames
        .iter()
        .map(|(vnode, state)| ManagedVnodeRestore {
            participant_id: predecessor_owners[*vnode as usize],
            vnode: *vnode,
            state,
        })
        .collect::<Vec<_>>();

    let mut target_operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT SUM(value) AS total FROM events",
        context,
        None,
        false,
        key_groups,
    );
    target_operator.initialize_managed_state().await.unwrap();
    target_operator.attach_cluster_shuffle(scope);
    let revoked = rustc_hash::FxHashSet::default();
    let predecessor_owner_nodes = predecessor_owners.map(NodeId);
    let transition = || ManagedVnodeTransition {
        predecessor: &predecessor,
        target: &target,
        revoked: &revoked,
        restores: &restores,
        whole_restores: &whole_restores,
        mode: ManagedVnodeTransitionMode::CheckpointBootstrap {
            predecessor_owners: &predecessor_owner_nodes,
        },
    };

    let missing = ManagedVnodeTransition {
        whole_restores: &whole_restores[..1],
        ..transition()
    };
    assert!(target_operator.prepare_vnode_transition(missing).is_err());
    assert!(target_operator.prepared_vnode_transition.is_none());

    let mut queued_donors = donors.clone();
    let mut queued =
        rkyv::from_bytes::<AggOpCheckpoint, rkyv::rancor::Error>(&queued_donors[0].1).unwrap();
    queued.channels[0]
        .events
        .push(AggCheckpointEvent::Frontier {
            recovery_gen: 0,
            frontier: frontier.into(),
        });
    queued_donors[0].1 = rkyv::to_bytes::<rkyv::rancor::Error>(&queued)
        .unwrap()
        .to_vec();
    let queued_whole = queued_donors
        .iter()
        .map(
            |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                participant_id: *participant_id,
                state,
            },
        )
        .collect::<Vec<_>>();
    let queued_transition = ManagedVnodeTransition {
        whole_restores: &queued_whole,
        ..transition()
    };
    assert!(target_operator
        .prepare_vnode_transition(queued_transition)
        .is_err());
    assert!(target_operator.prepared_vnode_transition.is_none());

    let disagreeing_donors = [
        (2, portable_aggregate_whole_frame(&predecessor, 2, frontier)),
        (
            3,
            portable_aggregate_whole_frame(
                &predecessor,
                3,
                InputFrontier {
                    watermark: Some(778),
                    idle: false,
                },
            ),
        ),
    ];
    let disagreeing_whole = disagreeing_donors
        .iter()
        .map(
            |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                participant_id: *participant_id,
                state,
            },
        )
        .collect::<Vec<_>>();
    let disagreeing_transition = ManagedVnodeTransition {
        whole_restores: &disagreeing_whole,
        ..transition()
    };
    assert!(target_operator
        .prepare_vnode_transition(disagreeing_transition)
        .is_err());
    assert!(target_operator.prepared_vnode_transition.is_none());

    let unaligned_donors = donors
        .iter()
        .map(|(participant_id, state)| {
            (
                *participant_id,
                unaligned_aggregate_archive_transport(state),
            )
        })
        .collect::<Vec<_>>();
    let unaligned_whole = unaligned_donors
        .iter()
        .map(
            |(participant_id, state)| crate::operator_graph::ManagedWholeRestore {
                participant_id: *participant_id,
                state,
            },
        )
        .collect::<Vec<_>>();
    let raw_payload = restores
        .iter()
        .map(|restore| restore.state.len())
        .chain(unaligned_whole.iter().map(|restore| restore.state.len()))
        .sum::<usize>();
    let alignment_copy = restores
        .iter()
        .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state))
        .chain(
            unaligned_whole
                .iter()
                .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state)),
        )
        .max()
        .unwrap();
    let roster_scratch = aggregate_transition_roster_scratch_bytes(
        predecessor.participants.len(),
        restores.len(),
        restores.len(),
    )
    .unwrap();
    let payload_peak = raw_payload + alignment_copy + roster_scratch;
    target_operator.set_managed_state_budget(payload_peak - 1);
    let unaligned_transition = ManagedVnodeTransition {
        whole_restores: &unaligned_whole,
        ..transition()
    };
    assert!(matches!(
        target_operator.prepare_vnode_transition(unaligned_transition),
        Err(DbError::ManagedStateBudgetExceeded {
            accounted_bytes,
            limit_bytes,
            ..
        }) if accounted_bytes == payload_peak && limit_bytes == payload_peak - 1
    ));
    assert!(target_operator.prepared_vnode_transition.is_none());

    let QueryState::Agg(aggregate) = &target_operator.state else {
        panic!("expected aggregate transition target");
    };
    let profile = aggregate.vnode_archive_restore_profile();
    let restore_preflights = restores
        .iter()
        .map(|restore| {
            with_aligned_aggregate_checkpoint_bytes(restore.state, |state| {
                profile
                    .preflight(state, format_args!("transition decode-bound test"))
                    .map(|archive| archive.restore_preflight())
            })
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(restore_preflights
        .iter()
        .any(|preflight| preflight.group_count() != 0));
    let replacement_upper = restore_preflights
        .iter()
        .map(|preflight| preflight.final_state_upper_bytes())
        .sum::<usize>();
    let sequential_peak = restore_preflights
        .iter()
        .map(|preflight| preflight.sequential_decode_bytes().unwrap())
        .max()
        .unwrap_or(0);
    let raw_payload = restores
        .iter()
        .map(|restore| restore.state.len())
        .chain(whole_restores.iter().map(|restore| restore.state.len()))
        .sum::<usize>();
    let alignment_copy = restores
        .iter()
        .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state))
        .chain(
            whole_restores
                .iter()
                .map(|restore| aggregate_checkpoint_alignment_copy_bytes(restore.state)),
        )
        .max()
        .unwrap_or(0);
    let payload_phase = raw_payload
        + alignment_copy
        + aggregate_transition_roster_scratch_bytes(
            predecessor.participants.len(),
            restores.len(),
            restores.len(),
        )
        .unwrap();
    let topology_upper =
        target_owners.len() * (std::mem::size_of::<NodeId>() + std::mem::size_of::<u64>());
    let decode_peak = target_operator.checked_live_state_bytes().unwrap()
        + payload_phase
        + aggregate
            .vnode_transition_restore_roster_bytes(restores.len(), revoked.len())
            .unwrap()
        + topology_upper
        + replacement_upper
        + sequential_peak;
    target_operator.set_managed_state_budget(decode_peak - 1);
    assert!(matches!(
        target_operator.prepare_vnode_transition(transition()),
        Err(DbError::ManagedStateBudgetExceeded {
            accounted_bytes,
            limit_bytes,
            ..
        }) if accounted_bytes == decode_peak && limit_bytes == decode_peak - 1
    ));
    assert!(target_operator.prepared_vnode_transition.is_none());

    target_operator.set_managed_state_budget(usize::MAX);
    target_operator
        .prepare_vnode_transition(transition())
        .unwrap();
    target_operator.publish_vnode_transition();
    assert_eq!(target_operator.local_frontier, frontier);
    assert_eq!(target_operator.effective_frontier, frontier);
    assert!(target_operator.cluster_peers.is_empty());
    assert!(target_operator.peer_channels.is_empty());
    target_operator.finish_vnode_transition();
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn aggregate_transition_restores_unaligned_vnode_archive_with_bounded_copy() {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};

    let predecessor_owners = [2, 1, 1, 1, 1, 1, 1, 1];
    let target_owners = [1, 1, 1, 1, 1, 1, 1, 1];
    let scope = cluster_scope(predecessor_owners).await;
    let (context, _) = context_and_batch();
    let key_groups = KeyGroupCount::try_from(8_u16).unwrap();

    let mut donor = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context.clone(),
        None,
        false,
        key_groups,
    );
    donor.initialize_managed_state().await.unwrap();
    let required = (0..8).collect::<Vec<_>>();
    let donor_vnode = donor
        .checkpoint_vnodes(&required, 8, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .find(|frame| frame.vnode == 0)
        .unwrap();
    let capture = donor_vnode.state.unwrap();
    let mut staged_bytes = capture.retained_bytes();
    let donor_vnode = capture.materialize(&mut staged_bytes, u64::MAX).unwrap();
    let donor_vnode = unaligned_aggregate_archive_transport(&donor_vnode);

    let mut target_operator = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        key_groups,
    );
    target_operator.initialize_managed_state().await.unwrap();
    target_operator.attach_cluster_shuffle(scope.clone());

    let participant = |node_id| CheckpointParticipant {
        node_id,
        boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
    };
    let predecessor_version = scope.registry.assignment_version();
    let predecessor = CheckpointAssignmentFence::from_owner_map(
        predecessor_version,
        &predecessor_owners,
        vec![participant(1), participant(2)],
    )
    .unwrap();
    let target = CheckpointAssignmentFence::from_owner_map(
        predecessor_version + 1,
        &target_owners,
        vec![participant(1)],
    )
    .unwrap();
    scope.registry.set_assignment_and_version(
        Arc::from(target_owners.map(NodeId)),
        target.assignment_version,
    );
    scope
        .sender
        .install_assignment_fence(&target, &target_owners)
        .unwrap();
    scope
        .receiver
        .install_assignment_fence(&target, &target_owners)
        .unwrap();

    let restores = [ManagedVnodeRestore {
        participant_id: 2,
        vnode: 0,
        state: &donor_vnode,
    }];
    let donor_whole = portable_aggregate_whole_frame(&predecessor, 2, InputFrontier::default());
    let whole_restores = [crate::operator_graph::ManagedWholeRestore {
        participant_id: 2,
        state: &donor_whole,
    }];
    let revoked = rustc_hash::FxHashSet::default();
    let payload_phase_bytes = donor_vnode
        .len()
        .checked_add(donor_whole.len())
        .and_then(|bytes| {
            bytes.checked_add(
                aggregate_checkpoint_alignment_copy_bytes(&donor_vnode)
                    .max(aggregate_checkpoint_alignment_copy_bytes(&donor_whole)),
            )
        })
        .and_then(|bytes| {
            bytes.checked_add(
                aggregate_transition_roster_scratch_bytes(
                    predecessor.participants.len(),
                    restores.len(),
                    restores.len(),
                )
                .unwrap(),
            )
        })
        .unwrap();
    let payload_limit = payload_phase_bytes - 1;
    target_operator.set_managed_state_budget(payload_limit);
    let error = target_operator
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target,
            revoked: &revoked,
            restores: &restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap_err();
    match error {
        DbError::ManagedStateBudgetExceeded {
            accounted_bytes,
            limit_bytes,
            ..
        } => {
            assert_eq!(accounted_bytes, payload_phase_bytes);
            assert_eq!(limit_bytes, payload_limit);
        }
        other => panic!("unaligned aggregate transition returned the wrong error: {other}"),
    }
    assert!(target_operator.prepared_vnode_transition.is_none());

    target_operator.set_managed_state_budget(usize::MAX);
    target_operator
        .prepare_vnode_transition(ManagedVnodeTransition {
            predecessor: &predecessor,
            target: &target,
            revoked: &revoked,
            restores: &restores,
            whole_restores: &whole_restores,
            mode: ManagedVnodeTransitionMode::Live,
        })
        .unwrap();
    assert!(target_operator.managed_state_accounting().unwrap().prepared > 0);
    target_operator.publish_vnode_transition();
    assert_eq!(
        target_operator
            .cluster_assignment
            .as_ref()
            .unwrap()
            .version(),
        target.assignment_version
    );
    target_operator.finish_vnode_transition();
}

#[tokio::test]
async fn derived_aggregate_requires_incremental_execution() {
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new(
        "ratio",
        "SELECT SUM(value) / COUNT(value) AS ratio FROM events",
        context,
        None,
        false,
    );

    let error = operator.initialize_managed_state().await.unwrap_err();
    assert!(matches!(error, DbError::Unsupported(_)));
    assert!(format!("{error}").contains(laminar_core::error_codes::SQL_UNSUPPORTED));
}

#[test]
fn stateful_apply_classification_preserves_stronger_dispositions() {
    let ordinary = stateful_apply_outcome_unknown(
        "totals",
        "state update",
        DbError::Pipeline("injected update failure".into()),
    );
    assert!(matches!(ordinary, DbError::StatefulOperatorPartialApply(_)));
    assert!(ordinary.requires_pipeline_recovery());

    let recovery = stateful_apply_outcome_unknown(
        "totals",
        "state update",
        DbError::Checkpoint("injected recovery".into()),
    );
    assert!(matches!(recovery, DbError::Checkpoint(_)));

    let halt = stateful_apply_outcome_unknown(
        "totals",
        "state update",
        DbError::BackpressureFail("injected halt".into()),
    );
    assert!(matches!(halt, DbError::BackpressureFail(_)));
}

#[cfg(feature = "cluster")]
#[test]
fn aggregate_shuffle_wrappers_preserve_terminal_disposition() {
    fn assert_terminal(error: DbError, expected: &str) {
        let DbError::ShuffleTerminal(reason) = error else {
            panic!("expected permanent shuffle halt, got {error}");
        };
        assert_eq!(reason, expected);
    }

    let operator = SqlQueryOperator::new(
        "totals",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        laminar_sql::create_session_context(),
        None,
        false,
    );
    assert_terminal(
        operator.remote_replay_error(DbError::ShuffleTerminal("remote replay".into())),
        "remote replay",
    );
    assert_terminal(
        operator.outbound_finalize_error(DbError::ShuffleTerminal("outbound".into())),
        "outbound",
    );
}

#[tokio::test]
async fn later_aggregate_batch_failure_requires_recovery_after_prior_mutation() {
    let (context, seed) = context_and_batch();
    let schema = seed.schema();
    let first = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![1])),
        ],
    )
    .unwrap();
    let later = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["b", "c"])),
            Arc::new(Int64Array::from(vec![2, 3])),
        ],
    )
    .unwrap();
    let mut operator = SqlQueryOperator::new(
        "totals",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
    );
    operator.lazy_init().await.unwrap();
    let QueryState::Agg(ref mut aggregate) = operator.state else {
        panic!("expected incremental aggregate state");
    };
    aggregate.set_max_groups_for_test(2);

    let error = operator
        .process(&[vec![first, later]], &[i64::MIN])
        .await
        .expect_err("the later batch must exceed the aggregate group limit");

    assert!(matches!(
        &error,
        DbError::StatefulOperatorPartialApply(message)
            if message.contains("state update") && message.contains("outcome is unknown")
    ));
    assert!(error.requires_pipeline_recovery());
}

#[test]
fn corrupt_aggregate_checkpoint_is_a_recovery_fault() {
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
    );
    let error = operator
        .restore(OperatorCheckpoint {
            data: b"not-rkyv".to_vec(),
        })
        .unwrap_err();
    assert!(matches!(error, DbError::Checkpoint(_)));
    assert!(error.requires_pipeline_recovery());
}

#[tokio::test]
async fn vnode_capture_is_incremental_and_restores_unaligned_without_whole_state() {
    let (context, batch) = context_and_batch();
    let key_groups = KeyGroupCount::try_from(8_u16).unwrap();
    let mut donor = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context.clone(),
        None,
        false,
        key_groups,
    );
    donor.initialize_managed_state().await.unwrap();
    donor.process(&[vec![batch]], &[100]).await.unwrap();
    let owned = (0..8).collect::<Vec<_>>();
    let baseline = donor
        .checkpoint_vnodes(&owned, 8, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(baseline.len(), owned.len());
    assert!(baseline.iter().all(|frame| frame.state.is_some()));
    assert!(donor
        .checkpoint_vnodes(&owned, 8, u64::MAX)
        .unwrap()
        .unwrap()
        .is_empty());

    let frames = baseline
        .into_iter()
        .map(|frame| {
            let capture = frame.state.unwrap();
            let mut staged_bytes = capture.retained_bytes();
            let state = capture.materialize(&mut staged_bytes, u64::MAX).unwrap();
            (frame.vnode, unaligned_aggregate_archive_transport(&state))
        })
        .collect::<Vec<_>>();
    assert!(frames.iter().all(|(_, state)| {
        state
            .as_ptr()
            .align_offset(AGG_CHECKPOINT_ARCHIVE_ALIGNMENT)
            != 0
    }));

    let mut limited = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context.clone(),
        None,
        false,
        key_groups,
    );
    limited.initialize_managed_state().await.unwrap();
    let (limited_vnode, limited_state) = &frames[0];
    let alignment_accounted = limited_state.len().checked_mul(2).unwrap();
    let limit = alignment_accounted - 1;
    limited.set_managed_state_budget(limit);
    let error = limited
        .restore_vnode(*limited_vnode, 8, limited_state)
        .unwrap_err();
    match error {
        DbError::ManagedStateBudgetExceeded {
            accounted_bytes,
            limit_bytes,
            ..
        } => {
            assert_eq!(accounted_bytes, alignment_accounted);
            assert_eq!(limit_bytes, limit);
        }
        other => panic!("unaligned aggregate restore returned the wrong error: {other}"),
    }
    let QueryState::Agg(limited_state) = &limited.state else {
        panic!("expected limited aggregate state");
    };
    assert_eq!(limited_state.logical_group_count_for_test(), 0);

    let (decode_vnode, decode_state) = frames
        .iter()
        .find(|(_, state)| {
            let QueryState::Agg(aggregate) = &limited.state else {
                return false;
            };
            let profile = aggregate.vnode_archive_restore_profile();
            with_aligned_aggregate_checkpoint_bytes(state, |state| {
                profile
                    .preflight(state, format_args!("decode-bound test"))
                    .map(|archive| archive.group_count() != 0)
            })
            .unwrap()
        })
        .expect("captured aggregate has a nonempty vnode");
    let mut decode_limited = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context.clone(),
        None,
        false,
        key_groups,
    );
    decode_limited.initialize_managed_state().await.unwrap();
    let QueryState::Agg(aggregate) = &decode_limited.state else {
        panic!("expected decode-limited aggregate state");
    };
    let profile = aggregate.vnode_archive_restore_profile();
    let restore_preflight = with_aligned_aggregate_checkpoint_bytes(decode_state, |state| {
        profile
            .preflight(state, format_args!("decode-bound test"))
            .map(|archive| archive.restore_preflight())
    })
    .unwrap();
    let decode_peak = aggregate
        .accounted_state_bytes()
        .checked_add(decode_state.len())
        .and_then(|bytes| {
            bytes.checked_add(aggregate_checkpoint_alignment_copy_bytes(decode_state))
        })
        .and_then(|bytes| {
            aggregate
                .vnode_transition_restore_roster_bytes(1, 0)
                .ok()
                .and_then(|roster| bytes.checked_add(roster))
        })
        .and_then(|bytes| {
            restore_preflight
                .sequential_decode_bytes()
                .and_then(|decode| bytes.checked_add(decode))
        })
        .and_then(|bytes| bytes.checked_add(restore_preflight.final_state_upper_bytes()))
        .unwrap();
    decode_limited.set_managed_state_budget(decode_peak - 1);
    let error = decode_limited
        .restore_vnode(*decode_vnode, 8, decode_state)
        .unwrap_err();
    assert!(matches!(
        error,
        DbError::ManagedStateBudgetExceeded {
            accounted_bytes,
            limit_bytes,
            ..
        } if accounted_bytes == decode_peak && limit_bytes == decode_peak - 1
    ));
    let QueryState::Agg(aggregate) = &decode_limited.state else {
        panic!("expected decode-limited aggregate state");
    };
    assert_eq!(aggregate.logical_group_count_for_test(), 0);

    let mut restored = SqlQueryOperator::new_with_key_groups(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
        key_groups,
    );
    restored.initialize_managed_state().await.unwrap();
    for (vnode, state) in &frames {
        restored.restore_vnode(*vnode, 8, state).unwrap();
    }
    let QueryState::Agg(aggregate) = &restored.state else {
        panic!("expected restored aggregate state");
    };
    assert_eq!(aggregate.logical_group_count_for_test(), 2);
    let expected = donor.process(&[Vec::new()], &[200]).await.unwrap();
    let actual = restored.process(&[Vec::new()], &[200]).await.unwrap();
    assert_eq!(actual, expected);

    donor.force_full_vnode_capture();
    let forced = donor
        .checkpoint_vnodes(&owned, 8, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(forced.len(), owned.len());
    assert!(forced.iter().all(|frame| frame.state.is_some()));
}

#[cfg(not(feature = "cluster"))]
#[test]
fn cluster_shuffle_checkpoint_is_rejected_without_support() {
    let (context, _) = context_and_batch();
    let mut operator = SqlQueryOperator::new(
        "sum",
        "SELECT key, SUM(value) AS total FROM events GROUP BY key",
        context,
        None,
        false,
    );
    let error = operator
        .restore(OperatorCheckpoint { data: Vec::new() })
        .unwrap_err();
    assert!(matches!(error, DbError::Checkpoint(_)));
    assert!(error.to_string().contains("cluster support"));
}
