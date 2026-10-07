//! Execute the published schema examples through the ordinary SQL backend.
#[cfg(any(
    feature = "kafka",
    feature = "postgres-sink",
    feature = "files",
    feature = "websocket"
))]
use arrow::array::StringArray;
#[cfg(any(
    feature = "kafka",
    feature = "postgres-sink",
    feature = "files",
    feature = "websocket"
))]
use laminar_db::{ExecuteResult, LaminarDB};

const GUIDE: &str = include_str!("../../../docs/SCHEMA_RESOLUTION.md");

fn example(index: usize) -> &'static str {
    GUIDE
        .split("## Examples")
        .nth(1)
        .unwrap()
        .split("```sql")
        .nth(index + 1)
        .unwrap()
        .split("```")
        .next()
        .unwrap()
}

#[cfg(any(feature = "kafka", feature = "postgres-sink", feature = "files"))]
async fn execute_example(db: &LaminarDB, sql: &str) {
    for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
        db.execute(statement).await.unwrap();
    }
}

#[cfg(any(
    feature = "kafka",
    feature = "postgres-sink",
    feature = "files",
    feature = "websocket"
))]
async fn fields(db: &LaminarDB, name: &str) -> Vec<String> {
    let ExecuteResult::Metadata(rows) = db.execute(&format!("DESCRIBE {name}")).await.unwrap()
    else {
        panic!("DESCRIBE must return metadata");
    };
    let names = rows
        .column_by_name("column_name")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    names.iter().map(|name| name.unwrap().to_owned()).collect()
}

#[test]
fn every_published_schema_example_uses_the_installed_sql_parser() {
    for index in 0..4 {
        for statement in example(index)
            .split(';')
            .map(str::trim)
            .filter(|sql| !sql.is_empty())
        {
            let parsed = laminar_sql::parser::StreamingParser::parse_sql(statement).unwrap();
            assert!(!parsed.is_empty());
        }
    }
}

#[cfg(feature = "websocket")]
#[tokio::test]
async fn websocket_binary_format_reaches_native_reader_validation_through_sql() {
    let db = LaminarDB::open().unwrap();
    db.execute("CREATE SOURCE bytes_in (payload BYTEA NOT NULL) FROM WEBSOCKET ('url' = 'ws://127.0.0.1:1') FORMAT BINARY")
        .await.unwrap();
    assert_eq!(fields(&db, "bytes_in").await, vec!["payload"]);
    let error = db.execute("CREATE SOURCE wrong (payload BIGINT) FROM WEBSOCKET ('url' = 'ws://127.0.0.1:1') FORMAT BINARY")
        .await.unwrap_err();
    assert!(error.to_string().contains("Binary"), "{error}");
}

#[cfg(all(feature = "files", feature = "delta-lake"))]
#[tokio::test]
async fn published_generator_file_and_delta_flow_resolves_without_redundant_columns() {
    let directory = tempfile::tempdir().unwrap();
    let files = directory
        .path()
        .join("output")
        .to_string_lossy()
        .replace('\\', "/");
    let delta = directory
        .path()
        .join("delta")
        .to_string_lossy()
        .replace('\\', "/");
    let sql = example(2)
        .replace("./output", &files)
        .replace("./delta-output", &delta);
    let db = LaminarDB::open().unwrap();
    execute_example(&db, &sql).await;
    assert_eq!(
        fields(&db, "generated").await,
        vec!["seq", "ts_ms", "value"]
    );
    assert_eq!(fields(&db, "files_out").await, vec!["seq", "value"]);
    assert_eq!(fields(&db, "delta_out").await, vec!["seq", "value"]);
    assert!(
        !std::path::Path::new(&files).exists(),
        "file resolution must not publish output"
    );
    assert!(
        std::path::Path::new(&delta).join("_delta_log").exists(),
        "Delta preparation is explicitly authorized by auto.create"
    );
}

#[cfg(feature = "kafka")]
#[tokio::test]
#[ignore = "requires LAMINAR_SCHEMA_TEST_KAFKA"]
async fn published_kafka_flow_replays_saved_reader_and_writer_without_selecting_latest() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let brokers = std::env::var("LAMINAR_SCHEMA_TEST_KAFKA").unwrap();
    let server = MockServer::start().await;
    let native = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"label","type":"string"}]}"#;
    for (subject, id) in [("events-value", 101), ("archive-value", 202)] {
        Mock::given(method("GET"))
            .and(path(format!("/subjects/{subject}/versions/latest")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id":id,"version":1,"schema":native,"schemaType":"AVRO"
            })))
            .expect(1)
            .mount(&server)
            .await;
    }
    let sql = example(0)
        .replace("localhost:19092", &brokers)
        .replace("http://localhost:8081", &server.uri());
    let directory = tempfile::tempdir().unwrap();
    let config = laminar_db::LaminarConfig {
        checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
            data_dir: Some(directory.path().into()),
            ..Default::default()
        }),
        ..Default::default()
    };
    let db = LaminarDB::open_with_config(config.clone()).unwrap();
    execute_example(&db, &sql).await;
    assert_eq!(fields(&db, "events").await, vec!["id", "label"]);
    assert_eq!(fields(&db, "archive").await, vec!["label", "id"]);
    drop(db);
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    server.reset().await;
    let db = LaminarDB::open_with_config(config).unwrap();
    execute_example(&db, &sql).await;
    assert_eq!(fields(&db, "events").await, vec!["id", "label"]);
    assert_eq!(fields(&db, "archive").await, vec!["label", "id"]);
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[cfg(all(feature = "postgres-cdc", feature = "postgres-sink"))]
#[tokio::test]
#[ignore = "requires provisioned dimensions tables and SCHEMA_PG_CONNECTION/PASSWORD/PORT"]
async fn published_postgres_reference_join_and_named_sink_resolve_from_metadata() {
    let mut sql = example(1).replace("'example'", "'laminar_schema'");
    if let Ok(database) = std::env::var("LAMINAR_SCHEMA_TEST_PG_DATABASE") {
        sql = sql.replace("'laminar_schema'", &format!("'{database}'"));
    }
    let db = LaminarDB::open().unwrap();
    execute_example(&db, &sql).await;
    assert_eq!(fields(&db, "dimension_snapshot").await, vec!["id", "label"]);
    assert_eq!(fields(&db, "dimensions").await, vec!["id", "label"]);
    assert_eq!(fields(&db, "dimensions_copy").await, vec!["label", "id"]);
}

#[cfg(feature = "files")]
#[tokio::test]
async fn published_parquet_and_bounded_sample_sources_resolve_without_consuming_files() {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_connectors::schema::traits::FormatEncoder;
    use laminar_connectors::schema::ParquetEncoder;
    use std::sync::Arc;

    let directory = tempfile::tempdir().unwrap();
    let input = directory.path().join("input");
    let json = directory.path().join("json");
    std::fs::create_dir(&input).unwrap();
    std::fs::create_dir(&json).unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))]).unwrap();
    let bytes = ParquetEncoder::new(schema)
        .encode_batch(&batch)
        .unwrap()
        .remove(0);
    let parquet_path = input.join("events.parquet");
    std::fs::write(&parquet_path, &bytes).unwrap();
    let json_bytes = b"{\"id\":1}\n{\"id\":2}\n";
    let json_path = json.join("events.json");
    std::fs::write(&json_path, json_bytes).unwrap();
    let input = input.to_string_lossy().replace('\\', "/");
    let json = json.to_string_lossy().replace('\\', "/");
    let sql = example(3)
        .replace("./input", &input)
        .replace("./json-input", &json);
    let db = LaminarDB::open().unwrap();
    execute_example(&db, &sql).await;
    assert_eq!(fields(&db, "parquet_events").await, vec!["id"]);
    assert_eq!(fields(&db, "json_events").await, vec!["id"]);
    assert_eq!(std::fs::read(&parquet_path).unwrap(), bytes);
    assert_eq!(std::fs::read(&json_path).unwrap(), json_bytes);
}
