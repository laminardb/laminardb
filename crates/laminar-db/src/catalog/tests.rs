use super::*;
use arrow::datatypes::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

#[tokio::test]
async fn test_register_source() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    let result = catalog.register_source("test", test_schema(), vec![], None, None, None, None);
    assert!(result.is_ok());
    assert!(catalog.get_source("test").is_some());
}

#[test]
fn source_ingress_rejects_null_primary_key() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![None::<i64>]))],
    )
    .unwrap();

    let error = validate_source_batch("keyed", &schema, &["id".into()], &[0], &batch).unwrap_err();
    assert!(error
        .to_string()
        .contains("primary-key column 'id' contains 1 null"));
}

#[test]
fn register_source_rejects_nullable_primary_key() {
    let catalog = SourceCatalog::new(8, BackpressureStrategy::Block);
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let error = catalog
        .register_source("keyed", schema, vec!["id".into()], None, None, None, None)
        .err()
        .expect("nullable primary key must be rejected");
    assert!(error
        .to_string()
        .contains("primary-key column 'id' must be non-nullable"));
}

#[test]
fn register_source_rejects_repeated_primary_key_column() {
    let catalog = SourceCatalog::new(8, BackpressureStrategy::Block);
    let error = catalog
        .register_source(
            "keyed",
            test_schema(),
            vec!["id".into(), "id".into()],
            None,
            None,
            None,
            None,
        )
        .err()
        .expect("repeated primary key must be rejected");
    assert!(error
        .to_string()
        .contains("primary key repeats column 'id'"));
}

#[tokio::test]
async fn test_register_duplicate_source() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    catalog
        .register_source("test", test_schema(), vec![], None, None, None, None)
        .unwrap();
    let result = catalog.register_source("test", test_schema(), vec![], None, None, None, None);
    assert!(matches!(
        result,
        Err(crate::DbError::SourceAlreadyExists(_))
    ));
}

#[tokio::test]
async fn test_drop_source() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    catalog
        .register_source("test", test_schema(), vec![], None, None, None, None)
        .unwrap();
    assert!(catalog.drop_source("test"));
    assert!(catalog.get_source("test").is_none());
}

#[tokio::test]
async fn test_list_sources() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    catalog
        .register_source("a", test_schema(), vec![], None, None, None, None)
        .unwrap();
    catalog
        .register_source("b", test_schema(), vec![], None, None, None, None)
        .unwrap();
    let mut names = catalog.list_sources();
    names.sort();
    assert_eq!(names, vec!["a", "b"]);
}

#[tokio::test]
async fn test_register_sink() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    assert!(catalog.register_sink("output", "events").is_ok());
    assert_eq!(catalog.list_sinks(), vec!["output"]);
}

#[tokio::test]
async fn test_register_query() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    let id = catalog.register_query("SELECT * FROM events");
    assert_eq!(id, 1);
    let queries = catalog.list_queries();
    assert_eq!(queries.len(), 1);
    assert!(queries[0].2); // active
}

#[tokio::test]
async fn test_deactivate_query() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    let id = catalog.register_query("SELECT * FROM events");
    catalog.deactivate_query(id);
    let queries = catalog.list_queries();
    assert!(!queries[0].2); // inactive
}

#[tokio::test]
async fn test_deactivate_query_limit() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    let mut ids = Vec::new();
    for i in 0..105 {
        let sql = format!("SELECT * FROM events_{i}");
        ids.push(catalog.register_query(&sql));
    }
    for id in &ids {
        catalog.deactivate_query(*id);
    }
    let queries = catalog.list_queries();
    assert_eq!(queries.len(), 100);
    let remaining_ids: std::collections::HashSet<u64> = queries.iter().map(|q| q.0).collect();
    for id in 1..=5 {
        assert!(
            !remaining_ids.contains(&id),
            "Query {id} should have been evicted"
        );
    }
    for id in 6..=105 {
        assert!(
            remaining_ids.contains(&id),
            "Query {id} should be remaining"
        );
    }
}

#[tokio::test]
async fn test_describe_source() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    let schema = test_schema();
    catalog
        .register_source("test", schema.clone(), vec![], None, None, None, None)
        .unwrap();
    let result = catalog.describe_source("test");
    assert!(result.is_some());
    assert_eq!(result.unwrap().fields().len(), 2);
}

#[tokio::test]
async fn test_or_replace() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    catalog
        .register_source("test", test_schema(), vec![], None, None, None, None)
        .unwrap();
    let entry = catalog.register_source_or_replace(
        "test",
        test_schema(),
        vec![],
        Some("ts".into()),
        None,
        None,
        None,
    );
    assert_eq!(entry.watermark_column, Some("ts".to_string()));
}

#[tokio::test]
async fn test_push_and_buffer_snapshot() {
    let catalog = SourceCatalog::new(1024, BackpressureStrategy::Block);
    let schema = test_schema();
    let entry = catalog
        .register_source("test", schema.clone(), vec![], None, None, None, None)
        .unwrap();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1])),
            Arc::new(arrow::array::Float64Array::from(vec![1.5])),
        ],
    )
    .unwrap();

    entry.push_and_buffer(batch).unwrap();
    let snap = entry.snapshot();
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].num_rows(), 1);
}

#[tokio::test]
async fn test_buffer_capacity_drops_oldest() {
    // SnapshotRing capacity=2; channel gets a larger buffer so pushes don't block.
    let catalog = SourceCatalog::new(2, BackpressureStrategy::DropOldest);
    let schema = test_schema();
    let entry = catalog
        .register_source("test", schema.clone(), vec![], None, None, None, None)
        .unwrap();

    let values: [(i64, f64); 3] = [(0, 1.0), (1, 2.0), (2, 3.0)];
    for (id, val) in values {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![id])),
                Arc::new(arrow::array::Float64Array::from(vec![val])),
            ],
        )
        .unwrap();
        entry.push_and_buffer(batch).unwrap();
    }

    let snap = entry.snapshot();
    // SnapshotRing capacity=2, so only the last 2 batches remain
    assert_eq!(snap.len(), 2);
    let col = snap[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(col.value(0), 1); // batch 0 was dropped
}
