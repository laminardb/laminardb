//! Arrow schemas for OTel signal types.
//!
//! Each signal type (traces, metrics, logs) has a fixed Arrow schema
//! that the protobuf data is flattened into.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

/// Arrow schema for trace spans.
///
/// Flattens the nested `ResourceSpans -> ScopeSpans -> Span` protobuf
/// hierarchy into a flat columnar layout suitable for streaming SQL.
#[must_use]
pub fn traces_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Int32, false),
        Field::new("start_time_unix_nano", DataType::Int64, false),
        Field::new("end_time_unix_nano", DataType::Int64, false),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("status_code", DataType::Int32, false),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("resource_service_name", DataType::Utf8, true),
        Field::new("resource_service_version", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("events_count", DataType::Int32, false),
        Field::new("links_count", DataType::Int32, false),
        Field::new("_laminar_received_at", DataType::Int64, false),
    ]))
}

/// Arrow schema for metric data points.
#[must_use]
pub fn metrics_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("metric_type", DataType::Int32, false),
        Field::new("timestamp_unix_nano", DataType::Int64, false),
        Field::new("value_double", DataType::Float64, true),
        Field::new("value_int", DataType::Int64, true),
        Field::new("histogram_count", DataType::UInt64, true),
        Field::new("histogram_sum", DataType::Float64, true),
        Field::new("resource_service_name", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("_laminar_received_at", DataType::Int64, false),
    ]))
}

/// Arrow schema for log records.
#[must_use]
pub fn logs_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_unix_nano", DataType::Int64, false),
        Field::new("observed_timestamp_unix_nano", DataType::Int64, true),
        Field::new("severity_number", DataType::Int32, false),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body_string", DataType::Utf8, true),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("resource_service_name", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("_laminar_received_at", DataType::Int64, false),
    ]))
}

#[cfg(test)]
mod tests {
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
        for col in [
            "start_time_unix_nano",
            "end_time_unix_nano",
            "duration_ns",
            "_laminar_received_at",
        ] {
            let field = schema.field_with_name(col).unwrap();
            assert_eq!(*field.data_type(), DataType::Int64, "column {col}");
        }
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
}
