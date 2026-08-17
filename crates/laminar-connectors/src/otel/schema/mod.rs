//! Arrow schemas for OTel signal types.
//!
//! Each schema flattens the nested OTLP protobuf hierarchy
//! (Resource → Scope → Span/DataPoint/LogRecord) into flat columns.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

/// Trace span schema.
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
        Field::new(
            "_laminar_received_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

/// Metric data point schema.
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
        Field::new(
            "_laminar_received_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

/// Log record schema.
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
        Field::new(
            "_laminar_received_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]))
}

#[cfg(test)]
mod tests;
