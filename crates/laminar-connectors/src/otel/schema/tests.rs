use super::*;

#[test]
fn test_traces_schema_field_count() {
    let schema = traces_schema();
    assert_eq!(schema.fields().len(), 20);
}

#[test]
fn test_traces_schema_trace_id_type() {
    let schema = traces_schema();
    let field = schema.field_with_name("trace_id").unwrap();
    assert_eq!(*field.data_type(), DataType::FixedSizeBinary(16));
    assert!(!field.is_nullable());
}

#[test]
fn test_traces_schema_span_id_type() {
    let schema = traces_schema();
    let field = schema.field_with_name("span_id").unwrap();
    assert_eq!(*field.data_type(), DataType::FixedSizeBinary(8));
    assert!(!field.is_nullable());
}

#[test]
fn test_traces_schema_parent_nullable() {
    let schema = traces_schema();
    let field = schema.field_with_name("parent_span_id").unwrap();
    assert!(field.is_nullable());
}

#[test]
fn test_traces_schema_timestamps() {
    let schema = traces_schema();
    for col in ["start_time_unix_nano", "end_time_unix_nano", "duration_ns"] {
        let field = schema.field_with_name(col).unwrap();
        assert_eq!(*field.data_type(), DataType::Int64, "column {col}");
    }
    let received_at = schema.field_with_name("_laminar_received_at").unwrap();
    assert_eq!(
        *received_at.data_type(),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
}

#[test]
fn test_metrics_schema_field_count() {
    let schema = metrics_schema();
    assert_eq!(schema.fields().len(), 14);
}

#[test]
fn test_logs_schema_field_count() {
    let schema = logs_schema();
    assert_eq!(schema.fields().len(), 12);
}

#[test]
fn test_logs_schema_trace_correlation() {
    let schema = logs_schema();
    let trace_id = schema.field_with_name("trace_id").unwrap();
    assert_eq!(*trace_id.data_type(), DataType::FixedSizeBinary(16));
    assert!(trace_id.is_nullable());
    let span_id = schema.field_with_name("span_id").unwrap();
    assert_eq!(*span_id.data_type(), DataType::FixedSizeBinary(8));
    assert!(span_id.is_nullable());
}
