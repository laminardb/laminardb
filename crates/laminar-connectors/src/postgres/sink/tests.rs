use super::*;
use std::sync::Arc;

use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

#[test]
fn dispatched_write_without_server_response_has_unknown_outcome() {
    let unknown = classify_postgres_write_failure("UNNEST execute", &"connection lost", false);
    assert!(unknown.is_outcome_unknown());
    assert!(unknown.to_string().contains("may have committed"));

    let rejected = classify_postgres_write_failure("UNNEST execute", &"unique violation", true);
    assert!(matches!(rejected, ConnectorError::WriteError(_)));
    assert!(!rejected.is_outcome_unknown());
}

#[test]
fn uncommitted_transaction_resolves_an_ambiguous_statement_outcome() {
    let retryable = resolve_uncommitted_transaction_error(ConnectorError::outcome_unknown(
        "connection lost after UNNEST",
        true,
    ));
    assert!(matches!(retryable, ConnectorError::ConnectionFailed(_)));
    assert!(retryable.is_transient());
    assert!(!retryable.is_outcome_unknown());

    let terminal = resolve_uncommitted_transaction_error(ConnectorError::outcome_unknown(
        "protocol state invalid",
        false,
    ));
    assert!(matches!(terminal, ConnectorError::TransactionError(_)));
    assert!(!terminal.is_transient());
    assert!(!terminal.is_outcome_unknown());
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn composite_key_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]))
}

fn test_config() -> PostgresSinkConfig {
    PostgresSinkConfig::new("localhost", "mydb", "events")
}

fn upsert_config() -> PostgresSinkConfig {
    let mut cfg = test_config();
    cfg.write_mode = WriteMode::Upsert;
    cfg.primary_key_columns = vec!["id".to_string()];
    cfg
}

fn test_batch(n: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<&str> = (0..n).map(|_| "test").collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(arrow_array::Float64Array::from(values)),
        ],
    )
    .expect("test batch creation")
}

fn variable_width_batch(value: &str) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![value])),
            Arc::new(arrow_array::Float64Array::from(vec![1.0])),
        ],
    )
    .expect("variable-width test batch")
}

// ── Constructor tests ──

#[test]
fn test_new_defaults() {
    let sink = PostgresSink::new(test_schema(), test_config(), None);
    assert_eq!(sink.state(), ConnectorState::Created);
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
    assert!(sink.upsert_sql.is_none());
    assert!(sink.copy_sql.is_none());
}

#[test]
fn constructor_uses_small_fixed_buffer_preallocation() {
    let sink = PostgresSink::new(test_schema(), test_config(), None);
    assert_eq!(sink.buffer.capacity(), 4);
}

#[test]
fn test_user_schema_strips_metadata() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_private_value", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
    ]));
    let sink = PostgresSink::new(schema, test_config(), None);
    assert_eq!(sink.user_schema.fields().len(), 3);
    assert_eq!(sink.user_schema.field(0).name(), "id");
    assert_eq!(sink.user_schema.field(1).name(), "_private_value");
    assert_eq!(sink.user_schema.field(2).name(), "value");
}

#[test]
fn test_schema_returned() {
    let schema = test_schema();
    let sink = PostgresSink::new(schema.clone(), test_config(), None);
    assert_eq!(sink.schema(), schema);
}

#[test]
fn engine_schema_replaces_placeholder_and_drives_write_sql() {
    let engine_schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, false),
        Field::new("sequence", DataType::Int64, false),
        Field::new("enabled", DataType::Boolean, true),
        Field::new("_op", DataType::Utf8, false),
    ]));
    let placeholder = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        false,
    )]));
    let mut config = ConnectorConfig::new("postgres-sink");
    config.set("hostname", "localhost");
    config.set("database", "analytics");
    config.set("username", "writer");
    config.set("table.name", "events");
    config.set("auto.create.table", "true");
    config.set("write.mode", "upsert");
    config.set("primary.key", "tenant");
    config.set("changelog.mode", "true");
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(engine_schema.as_ref()),
    );

    let mut sink = PostgresSink::new(placeholder, test_config(), None);
    sink.apply_connector_config(&config).unwrap();
    sink.prepare_statements().unwrap();

    assert_eq!(sink.schema, engine_schema);
    assert_eq!(
        sink.user_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect::<Vec<_>>(),
        vec!["tenant", "sequence", "enabled"]
    );
    let upsert = sink.upsert_sql.as_deref().unwrap();
    assert!(upsert.contains("\"tenant\"") && upsert.contains("\"sequence\""));
    let ddl = sink.create_table_sql.as_deref().unwrap();
    assert!(ddl.contains("\"tenant\" TEXT NOT NULL"), "{ddl}");
    assert!(ddl.contains("\"sequence\" BIGINT NOT NULL"), "{ddl}");
    assert!(ddl.contains("\"enabled\" BOOLEAN"), "{ddl}");
    assert!(!ddl.contains("_op"), "{ddl}");
}

// ── SQL generation tests ──

#[test]
fn test_build_copy_sql() {
    let schema = test_schema();
    let config = test_config();
    let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
    assert_eq!(
        sql,
        "COPY \"public\".\"events\" (\"id\", \"name\", \"value\") FROM STDIN BINARY"
    );
}

#[test]
fn test_build_copy_sql_custom_schema() {
    let schema = test_schema();
    let mut config = test_config();
    config.schema_name = "analytics".to_string();
    let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
    assert!(sql.starts_with("COPY \"analytics\".\"events\""));
}

#[test]
fn sql_generation_quotes_reserved_mixed_case_and_embedded_quote_identifiers() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("select", DataType::Int64, false),
        Field::new("MixedCase", DataType::Utf8, true),
        Field::new("a\"b", DataType::Boolean, true),
    ]));
    let mut config = test_config();
    config.schema_name = "Tenant.Schema".into();
    config.table_name = "Order".into();

    let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
    assert_eq!(
            sql,
            "COPY \"Tenant.Schema\".\"Order\" (\"select\", \"MixedCase\", \"a\"\"b\") FROM STDIN BINARY"
        );
}

#[test]
fn schema_admission_rejects_unsupported_types_and_nullable_keys() {
    let unsupported = Arc::new(Schema::new(vec![Field::new(
        "nested",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]));
    assert!(PostgresSink::build_copy_sql(&unsupported, &test_config()).is_err());

    let nullable_key = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    assert!(PostgresSink::build_upsert_sql(&nullable_key, &upsert_config()).is_err());

    let nul_name = Arc::new(Schema::new(vec![Field::new(
        "bad\0column",
        DataType::Int64,
        false,
    )]));
    assert!(PostgresSink::build_copy_sql(&nul_name, &test_config()).is_err());
}

#[test]
fn test_build_copy_sql_excludes_metadata_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let mut config = upsert_config();
    config.changelog_mode = true;
    let sql = PostgresSink::build_copy_sql(&schema, &config).unwrap();
    assert_eq!(
        sql,
        "COPY \"public\".\"events\" (\"id\", \"value\") FROM STDIN BINARY"
    );
}

#[test]
fn timestamp_metadata_is_never_silently_dropped_outside_changelog_mode() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_ts_ms", DataType::Int64, false),
    ]));
    let error = PostgresSink::build_copy_sql(&schema, &test_config()).unwrap_err();
    assert!(error.to_string().contains("changelog.mode=true"));

    let user_underscore = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_private", DataType::Utf8, true),
    ]));
    let sql = PostgresSink::build_copy_sql(&user_underscore, &test_config()).unwrap();
    assert!(sql.contains("\"_private\""), "{sql}");
}

#[test]
fn test_build_upsert_sql() {
    let schema = test_schema();
    let config = upsert_config();
    let sql = PostgresSink::build_upsert_sql(&schema, &config).unwrap();

    assert!(sql.starts_with("INSERT INTO \"public\".\"events\""));
    assert!(sql.contains("SELECT * FROM UNNEST"));
    assert!(sql.contains("$1::int8[]"));
    assert!(sql.contains("$2::text[]"));
    assert!(sql.contains("$3::float8[]"));
    assert!(sql.contains("ON CONFLICT (\"id\")"));
    assert!(sql.contains("DO UPDATE SET"));
    assert!(sql.contains("\"name\" = EXCLUDED.\"name\""));
    assert!(sql.contains("\"value\" = EXCLUDED.\"value\""));
    assert!(!sql.contains("\"id\" = EXCLUDED.\"id\""));
}

#[test]
fn test_build_upsert_sql_composite_key() {
    let schema = composite_key_schema();
    let mut config = upsert_config();
    config.primary_key_columns = vec!["id".to_string(), "name".to_string()];
    let sql = PostgresSink::build_upsert_sql(&schema, &config).unwrap();

    assert!(sql.contains("ON CONFLICT (\"id\", \"name\")"));
    assert!(sql.contains("\"value\" = EXCLUDED.\"value\""));
    assert!(!sql.contains("\"id\" = EXCLUDED.\"id\""));
    assert!(!sql.contains("\"name\" = EXCLUDED.\"name\""));
}

#[test]
fn test_build_upsert_sql_key_only_table() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let mut config = test_config();
    config.write_mode = WriteMode::Upsert;
    config.primary_key_columns = vec!["id".to_string()];

    let sql = PostgresSink::build_upsert_sql(&schema, &config).unwrap();
    assert!(sql.contains("DO NOTHING"), "sql: {sql}");
}

#[test]
fn test_build_delete_sql() {
    let schema = test_schema();
    let config = upsert_config();
    let sql = PostgresSink::build_delete_sql(&schema, &config).unwrap();

    assert_eq!(
        sql,
        "DELETE FROM \"public\".\"events\" WHERE \"id\" = ANY($1::int8[])"
    );
}

#[test]
fn test_build_delete_sql_composite_key() {
    let schema = composite_key_schema();
    let mut config = upsert_config();
    config.primary_key_columns = vec!["id".to_string(), "name".to_string()];
    let sql = PostgresSink::build_delete_sql(&schema, &config).unwrap();

    // Composite PK must match tuple-wise (UNNEST zips $1/$2 positionally), NOT the
    // cross-product `id = ANY($1) AND name = ANY($2)` which over-deletes (CN-2).
    assert_eq!(
        sql,
        "DELETE FROM \"public\".\"events\" AS \"target\" USING UNNEST($1::int8[], \
             $2::text[]) AS \"keys\"(\"id\", \"name\") WHERE \"target\".\"id\" = \
             \"keys\".\"id\" AND \"target\".\"name\" = \"keys\".\"name\""
    );
    assert!(!sql.contains("ANY($1::int8[]) AND"));
}

#[test]
fn test_build_create_table_sql() {
    let schema = test_schema();
    let config = upsert_config();
    let sql = PostgresSink::build_create_table_sql(&schema, &config).unwrap();

    assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS \"public\".\"events\""));
    assert!(sql.contains("\"id\" BIGINT NOT NULL"));
    assert!(sql.contains("\"name\" TEXT"));
    assert!(sql.contains("\"value\" DOUBLE PRECISION"));
    assert!(sql.contains("PRIMARY KEY (\"id\")"));
}

#[test]
fn test_build_create_table_sql_no_pk() {
    let schema = test_schema();
    let config = test_config();
    let sql = PostgresSink::build_create_table_sql(&schema, &config).unwrap();

    assert!(sql.starts_with("CREATE TABLE IF NOT EXISTS"));
    assert!(!sql.contains("PRIMARY KEY"));
}

#[test]
fn ordinary_upsert_collapse_keeps_last_row_per_primary_key() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 1])),
            Arc::new(StringArray::from(vec!["old", "other", "new"])),
        ],
    )
    .unwrap();

    let collapsed = collapse_upsert_batch(&batch, &["id".to_string()]).unwrap();

    assert_eq!(collapsed.num_rows(), 2);
    assert!(collapsed.schema().index_of("_op").is_err());
    let ids = collapsed
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let values = collapsed
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let id_one = (0..collapsed.num_rows())
        .find(|&row| ids.value(row) == 1)
        .unwrap();
    assert_eq!(values.value(id_one), "new");
}

// ── Changelog splitting tests ──

fn changelog_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("_op", DataType::Utf8, false),
        Field::new("_ts_ms", DataType::Int64, false),
    ]))
}

fn changelog_batch() -> RecordBatch {
    RecordBatch::try_new(
        changelog_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            Arc::new(StringArray::from(vec!["I", "U", "D", "I", "D"])),
            Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500])),
        ],
    )
    .expect("changelog batch creation")
}

#[test]
fn test_split_changelog_batch() {
    let batch = changelog_batch();
    let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");

    assert_eq!(inserts.num_rows(), 3);
    assert_eq!(deletes.num_rows(), 2);
    assert_eq!(inserts.num_columns(), 2);
    assert_eq!(deletes.num_columns(), 2);

    let insert_ids = inserts
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 array");
    assert_eq!(insert_ids.value(0), 1);
    assert_eq!(insert_ids.value(1), 2);
    assert_eq!(insert_ids.value(2), 4);

    let delete_ids = deletes
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64 array");
    assert_eq!(delete_ids.value(0), 3);
    assert_eq!(delete_ids.value(1), 5);
}

#[test]
fn test_split_changelog_all_inserts() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["I", "I"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .expect("batch");

    let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");
    assert_eq!(inserts.num_rows(), 2);
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn test_split_changelog_all_deletes() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(StringArray::from(vec!["D", "D"])),
            Arc::new(Int64Array::from(vec![100, 200])),
        ],
    )
    .expect("batch");

    let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");
    assert_eq!(inserts.num_rows(), 0);
    assert_eq!(deletes.num_rows(), 2);
}

#[test]
fn test_split_changelog_missing_op_column() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1]))]).expect("batch");

    let result = PostgresSink::split_changelog_batch(&batch);
    assert!(result.is_err());
}

#[test]
fn test_split_changelog_snapshot_read() {
    let schema = changelog_schema();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(StringArray::from(vec!["r"])),
            Arc::new(Int64Array::from(vec![100])),
        ],
    )
    .expect("batch");

    let (inserts, deletes) = PostgresSink::split_changelog_batch(&batch).expect("split");
    assert_eq!(inserts.num_rows(), 1);
    assert_eq!(deletes.num_rows(), 0);
}

#[test]
fn changelog_null_and_unknown_operations_fail_closed() {
    for operations in [
        StringArray::from(vec![Some("I"), None]),
        StringArray::from(vec![Some("I"), Some("future")]),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("_op", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2])), Arc::new(operations)],
        )
        .unwrap();
        assert!(PostgresSink::split_changelog_batch(&batch).is_err());
    }
}

#[cfg(feature = "postgres-sink")]
#[tokio::test]
async fn invalid_changelog_operation_is_rejected_before_buffer_mutation() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_op", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["future"])),
        ],
    )
    .unwrap();
    let mut config = upsert_config();
    config.changelog_mode = true;
    let mut sink = PostgresSink::new(schema, config, None);
    sink.state = ConnectorState::Running;

    let error = sink.write_batch(&batch).await.unwrap_err();
    assert!(!error.is_transient());
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
}

// ── Buffering tests ──

#[tokio::test]
async fn test_write_batch_buffering() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(10);
    let result = sink.write_batch(&batch).await.expect("write");

    assert_eq!(result.records_written, 0);
    assert_eq!(sink.buffered_rows(), 10);
    assert_eq!(sink.buffered_retained_bytes, retained_batch_bytes(&batch));
}

#[tokio::test]
async fn test_write_batch_empty() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;

    let batch = test_batch(0);
    let result = sink.write_batch(&batch).await.expect("write");
    assert_eq!(result.records_written, 0);
    assert_eq!(sink.buffered_rows(), 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
}

#[tokio::test]
async fn test_write_batch_not_running() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);

    let batch = test_batch(10);
    let result = sink.write_batch(&batch).await;
    assert!(result.is_err());
}

#[test]
fn retained_limit_uses_variable_width_memory_and_allows_exact_boundary() {
    let narrow = variable_width_batch("x");
    let wide_value = "x".repeat(4096);
    let wide = variable_width_batch(&wide_value);
    let narrow_bytes = retained_batch_bytes(&narrow);
    let wide_bytes = retained_batch_bytes(&wide);
    assert!(wide_bytes > narrow_bytes);

    let exact_limit = narrow_bytes + wide_bytes;
    assert!(!requires_preflush(narrow_bytes, wide_bytes, exact_limit).unwrap());
    assert!(requires_preflush(narrow_bytes, wide_bytes, exact_limit - 1).unwrap());
    assert!(requires_preflush(usize::MAX, 1, usize::MAX).unwrap());
}

#[cfg(feature = "postgres-sink")]
#[tokio::test]
async fn oversized_batch_rejection_does_not_mutate_existing_buffer() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;

    let existing = variable_width_batch("retained");
    sink.write_batch_with_retained_limit(&existing, usize::MAX)
        .await
        .unwrap();
    let rows_before = sink.buffered_rows;
    let bytes_before = sink.buffered_retained_bytes;
    let batches_before = sink.buffer.len();

    let incoming_value = "x".repeat(4096);
    let incoming = variable_width_batch(&incoming_value);
    let incoming_bytes = retained_batch_bytes(&incoming);
    let error = sink
        .write_batch_with_retained_limit(&incoming, incoming_bytes - 1)
        .await
        .expect_err("single oversized batch must fail before admission");

    assert!(error.to_string().contains("split the batch upstream"));
    assert!(!error.is_transient());
    assert_eq!(sink.buffered_rows, rows_before);
    assert_eq!(sink.buffered_retained_bytes, bytes_before);
    assert_eq!(sink.buffer.len(), batches_before);
    assert_eq!(sink.buffer[0].num_rows(), existing.num_rows());
}

#[cfg(feature = "postgres-sink")]
#[tokio::test]
async fn explicit_flush_error_clears_buffer_accounting() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;

    sink.write_batch_with_retained_limit(&variable_width_batch("pending"), usize::MAX)
        .await
        .unwrap();
    let error = sink
        .flush()
        .await
        .expect_err("missing pool must fail flush");

    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
}

#[cfg(feature = "postgres-sink")]
#[tokio::test]
async fn crossing_batch_flushes_existing_before_admission() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;

    let existing = variable_width_batch("existing");
    sink.write_batch_with_retained_limit(&existing, usize::MAX)
        .await
        .unwrap();
    let incoming = variable_width_batch("incoming");
    let crossing_limit =
        retained_batch_bytes(&existing).saturating_add(retained_batch_bytes(&incoming)) - 1;

    let error = sink
        .write_batch_with_retained_limit(&incoming, crossing_limit)
        .await
        .expect_err("crossing admission must flush the existing buffer first");

    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
}

#[cfg(feature = "postgres-sink")]
#[tokio::test]
async fn close_reports_flush_failure_but_releases_state() {
    let mut sink = PostgresSink::new(test_schema(), test_config(), None);
    sink.state = ConnectorState::Running;
    sink.write_batch_with_retained_limit(&variable_width_batch("pending"), usize::MAX)
        .await
        .unwrap();

    let error = sink
        .close()
        .await
        .expect_err("missing pool must fail flush");

    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert!(sink.buffer.is_empty());
    assert_eq!(sink.buffered_rows, 0);
    assert_eq!(sink.buffered_retained_bytes, 0);
    assert_eq!(sink.state, ConnectorState::Closed);
}

// ── Contract tests ──

#[cfg(feature = "postgres-sink")]
#[test]
fn contract_append_is_multi_writer_durable_at_least_once() {
    let sink = PostgresSink::new(test_schema(), test_config(), None);
    let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::MultiWriter);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert_eq!(
        sink.suggested_write_timeout(),
        sink.config.statement_timeout + Duration::from_secs(5)
    );
    assert_eq!(sink.flush_interval(), Duration::from_millis(250));
}

#[cfg(feature = "postgres-sink")]
#[test]
fn contract_upsert_requires_keyed_input() {
    let sink = PostgresSink::new(test_schema(), upsert_config(), None);
    let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
    assert_eq!(contract.input_mode, SinkInputMode::KeyedUpsert);
    assert_eq!(contract.topology, SinkTopology::Singleton);
}

#[cfg(feature = "postgres-sink")]
#[test]
fn contract_changelog_accepts_full_changelog() {
    let mut config = upsert_config();
    config.changelog_mode = true;
    let sink = PostgresSink::new(changelog_schema(), config, None);
    let contract = sink.contract(&ConnectorConfig::new("postgres")).unwrap();
    assert_eq!(contract.input_mode, SinkInputMode::FullChangelog);
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert!(contract.accepts_full_changelog());
    assert_eq!(
        sink.suggested_write_timeout(),
        sink.config.statement_timeout.saturating_mul(2) + Duration::from_secs(5)
    );
}

#[cfg(feature = "postgres-sink")]
#[tokio::test]
async fn open_rejects_invalid_ca_before_network_io() {
    let directory = tempfile::tempdir().unwrap();
    let ca_path = directory.path().join("missing.pem");
    let mut config = test_config();
    config.ssl_ca_cert_path = Some(ca_path.clone());
    let mut sink = PostgresSink::new(test_schema(), config, None);

    let error = sink
        .open(&ConnectorConfig::new("postgres-sink"))
        .await
        .expect_err("invalid custom CA must fail before pool I/O");

    let message = error.to_string();
    assert!(
        message.contains(&ca_path.display().to_string()),
        "{message}"
    );
    assert_eq!(sink.state(), ConnectorState::Created);
}

#[cfg(not(feature = "postgres-sink"))]
#[test]
fn missing_feature_fails_contract_before_io() {
    let sink = PostgresSink::new(test_schema(), test_config(), None);
    let error = sink
        .contract(&ConnectorConfig::new("postgres"))
        .expect_err("disabled PostgreSQL sink must fail admission");
    assert!(error.to_string().contains("postgres-sink"));
}

// ── Debug output test ──

#[test]
fn test_debug_output() {
    let sink = PostgresSink::new(test_schema(), test_config(), None);
    let debug = format!("{sink:?}");
    assert!(debug.contains("PostgresSink"));
    assert!(debug.contains("public") && debug.contains("events"));
}

// ── Helper function tests ──

#[test]
fn test_build_user_schema() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("_op", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
        Field::new("_ts_ms", DataType::Int64, false),
    ]));
    let user = build_user_schema(&schema);
    assert_eq!(user.fields().len(), 2);
    assert_eq!(user.field(0).name(), "id");
    assert_eq!(user.field(1).name(), "value");
}

#[test]
fn test_build_user_schema_no_metadata() {
    let schema = test_schema();
    let user = build_user_schema(&schema);
    assert_eq!(user.fields().len(), 3);
}
