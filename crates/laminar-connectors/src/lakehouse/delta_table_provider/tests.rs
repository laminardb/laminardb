use super::*;
use arrow_array::{Float64Array, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use tempfile::TempDir;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]))
}

#[allow(clippy::cast_precision_loss)]
fn test_batch(n: usize) -> arrow_array::RecordBatch {
    let ids: Vec<i64> = (0..n as i64).collect();
    let names: Vec<&str> = (0..n).map(|_| "test").collect();
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

    arrow_array::RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn test_register_and_query_delta_table() {
    use super::super::delta_io;
    use deltalake::protocol::SaveMode;

    let temp_dir = TempDir::new().unwrap();
    let table_path = temp_dir.path().to_str().unwrap();

    // Create a Delta table with some data.
    let schema = test_schema();
    let table = delta_io::open_or_create_table(table_path, HashMap::new(), Some(&schema))
        .await
        .unwrap();

    let batch = test_batch(10);
    let (_table, version) = delta_io::write_batches(
        table,
        vec![batch],
        SaveMode::Append,
        None,
        false,
        None,
        None,
    )
    .await
    .unwrap();
    assert_eq!(version, 1);

    // Register as TableProvider and query.
    let ctx = SessionContext::new();
    register_delta_table(&ctx, "MixedDelta", table_path, HashMap::new())
        .await
        .unwrap();

    let df = ctx
        .sql("SELECT COUNT(*) AS cnt FROM \"MixedDelta\"")
        .await
        .unwrap();
    let results = df.collect().await.unwrap();

    assert_eq!(results.len(), 1);
    let count = results[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(count, 10);
}
