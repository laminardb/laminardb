use super::*;
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

// Test record type
#[derive(Clone, Debug)]
struct TestEvent {
    id: i64,
    value: f64,
    timestamp: i64,
}

impl Record for TestEvent {
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("timestamp", DataType::Int64, false),
        ]))
    }

    fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Self::schema(),
            vec![
                Arc::new(Int64Array::from(vec![self.id])),
                Arc::new(Float64Array::from(vec![self.value])),
                Arc::new(Int64Array::from(vec![self.timestamp])),
            ],
        )
        .unwrap()
    }

    fn event_time(&self) -> Option<i64> {
        Some(self.timestamp)
    }
}

#[tokio::test]
async fn test_create_source_sink() {
    let (source, _sink) = create::<TestEvent>(1024);

    assert!(!source.is_closed());
    assert_eq!(source.pending(), 0);
}

#[tokio::test]
async fn test_push_single() {
    let (source, _sink) = create::<TestEvent>(16);

    let event = TestEvent {
        id: 1,
        value: 42.0,
        timestamp: 1000,
    };

    assert!(source.push(event).is_ok());
    assert_eq!(source.pending(), 1);
}

#[tokio::test]
async fn test_try_push() {
    let (source, _sink) = create::<TestEvent>(16);

    let event = TestEvent {
        id: 1,
        value: 42.0,
        timestamp: 1000,
    };

    assert!(source.try_push(event).is_ok());
}

#[tokio::test]
async fn test_push_batch() {
    let (source, _sink) = create::<TestEvent>(16);

    let events = vec![
        TestEvent {
            id: 1,
            value: 1.0,
            timestamp: 1000,
        },
        TestEvent {
            id: 2,
            value: 2.0,
            timestamp: 2000,
        },
        TestEvent {
            id: 3,
            value: 3.0,
            timestamp: 3000,
        },
    ];

    let count = source.push_batch(&events);
    assert_eq!(count, 3);
    assert_eq!(source.pending(), 3);
}

#[tokio::test]
async fn test_push_arrow() {
    let (source, _sink) = create::<TestEvent>(16);

    let batch = RecordBatch::try_new(
        TestEvent::schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            Arc::new(Int64Array::from(vec![1000, 2000, 3000])),
        ],
    )
    .unwrap();

    assert!(source.push_arrow(batch).is_ok());
}

#[tokio::test]
async fn test_push_arrow_schema_mismatch() {
    let (source, _sink) = create::<TestEvent>(16);

    // Create batch with different schema
    let wrong_schema = Arc::new(Schema::new(vec![Field::new(
        "wrong",
        DataType::Utf8,
        false,
    )]));

    let batch = RecordBatch::try_new(
        wrong_schema,
        vec![Arc::new(StringArray::from(vec!["test"]))],
    )
    .unwrap();

    let result = source.push_arrow(batch);
    assert!(matches!(result, Err(StreamingError::SchemaMismatch { .. })));
}

#[tokio::test]
async fn test_watermark() {
    let (source, _sink) = create::<TestEvent>(16);

    assert_eq!(source.current_watermark(), i64::MIN);

    source.watermark(1000);
    assert_eq!(source.current_watermark(), 1000);

    source.watermark(2000);
    assert_eq!(source.current_watermark(), 2000);

    // Watermark should not go backwards
    source.watermark(1500);
    assert_eq!(source.current_watermark(), 2000);
}

#[tokio::test]
async fn recovery_restore_lowers_shared_watermark_then_runtime_advances() {
    let (source, _sink) = create::<TestEvent>(16);
    let clone = source.clone();
    source.watermark(2_000);

    source.restore_watermark_for_recovery(500);
    assert_eq!(source.current_watermark(), 500);
    assert_eq!(clone.current_watermark(), 500);

    clone.watermark(600);
    assert_eq!(source.current_watermark(), 600);
    clone.watermark(550);
    assert_eq!(source.current_watermark(), 600);
}

#[tokio::test]
async fn test_watermark_from_event_time() {
    let (source, _sink) = create::<TestEvent>(16);

    let event = TestEvent {
        id: 1,
        value: 42.0,
        timestamp: 5000,
    };

    source.push(event).unwrap();

    // Watermark should be updated from event time
    assert_eq!(source.current_watermark(), 5000);
}

#[tokio::test]
async fn test_clone_multi_producer() {
    let (source, sink) = create::<TestEvent>(16);
    let source2 = source.clone();
    let mut sub = sink.subscribe(); // subscribe before push

    source
        .push(TestEvent {
            id: 1,
            value: 1.0,
            timestamp: 1000,
        })
        .unwrap();
    source2
        .push(TestEvent {
            id: 2,
            value: 2.0,
            timestamp: 2000,
        })
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(sub.poll().is_some());
    assert!(sub.poll().is_some());
}

#[tokio::test]
async fn test_schema() {
    let (source, _sink) = create::<TestEvent>(16);

    let schema = source.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "value");
    assert_eq!(schema.field(2).name(), "timestamp");
}

#[tokio::test]
async fn test_named_source() {
    let config = SourceConfig::named("my_source");
    let (source, _sink) = create_with_config::<TestEvent>(config);

    assert_eq!(source.name(), Some("my_source"));
}

#[tokio::test]
async fn test_debug_format() {
    let (source, _sink) = create::<TestEvent>(16);

    let debug = format!("{source:?}");
    assert!(debug.contains("Source"));
}

#[tokio::test]
async fn test_set_event_time_column() {
    let (source, _sink) = create::<TestEvent>(16);

    assert!(source.event_time_column().is_none());

    source.set_event_time_column("timestamp");
    assert_eq!(source.event_time_column(), Some("timestamp".to_string()));
}

#[tokio::test]
async fn test_event_time_column_preserved_on_clone() {
    let (source, _sink) = create::<TestEvent>(16);
    source.set_event_time_column("ts");

    let source2 = source.clone();
    assert_eq!(source2.event_time_column(), Some("ts".to_string()));
}
