//! Protobuf-to-Arrow conversion for OTel traces, metrics, and logs.
//!
//! Flattens the nested protobuf hierarchies into flat Arrow `RecordBatch` rows.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    UInt64Builder,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{metric, number_data_point};
use opentelemetry_proto::tonic::resource::v1::Resource;

/// Pad or truncate a byte slice to exactly `len` bytes.
/// Returns the input directly if it's already the right size.
fn fixed_bytes(src: &[u8], len: usize) -> std::borrow::Cow<'_, [u8]> {
    if src.len() == len {
        std::borrow::Cow::Borrowed(src)
    } else {
        let mut out = vec![0u8; len];
        let copy_len = src.len().min(len);
        out[..copy_len].copy_from_slice(&src[..copy_len]);
        std::borrow::Cow::Owned(out)
    }
}

/// Returns `true` if every byte in the slice is zero.
fn is_all_zeros(bytes: &[u8]) -> bool {
    bytes.iter().all(|&b| b == 0)
}

/// Extract a string attribute value from resource attributes by key.
fn extract_resource_attr(resource: Option<&Resource>, key: &str) -> Option<String> {
    resource.and_then(|r| {
        r.attributes.iter().find_map(|kv| {
            if kv.key == key {
                any_value_to_string(kv.value.as_ref())
            } else {
                None
            }
        })
    })
}

/// Serialize resource attributes to JSON, excluding promoted fields.
fn resource_attributes_json(resource: Option<&Resource>) -> Option<String> {
    let attrs: Vec<&KeyValue> = resource
        .map(|r| {
            r.attributes
                .iter()
                .filter(|kv| kv.key != "service.name" && kv.key != "service.version")
                .collect()
        })
        .unwrap_or_default();

    if attrs.is_empty() {
        return None;
    }

    Some(key_values_to_json(&attrs))
}

/// Serialize a `KeyValue` slice to a JSON object string, or `None` if empty.
fn kv_to_json(attrs: &[KeyValue]) -> Option<String> {
    if attrs.is_empty() {
        return None;
    }
    let refs: Vec<&KeyValue> = attrs.iter().collect();
    Some(key_values_to_json(&refs))
}

/// Convert a list of `KeyValue` to a JSON object string.
fn key_values_to_json(kvs: &[&KeyValue]) -> String {
    let mut buf = String::with_capacity(kvs.len() * 32);
    buf.push('{');
    for (i, kv) in kvs.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        // Key is always a JSON string
        write_json_string(&mut buf, &kv.key);
        buf.push(':');
        // Value
        if let Some(v) = &kv.value {
            write_any_value_json(&mut buf, v);
        } else {
            buf.push_str("null");
        }
    }
    buf.push('}');
    buf
}

/// Write a JSON-escaped string.
fn write_json_string(buf: &mut String, s: &str) {
    buf.push('"');
    for c in s.chars() {
        match c {
            '"' => buf.push_str("\\\""),
            '\\' => buf.push_str("\\\\"),
            '\n' => buf.push_str("\\n"),
            '\r' => buf.push_str("\\r"),
            '\t' => buf.push_str("\\t"),
            c if c.is_control() => {
                use std::fmt::Write;
                let _ = write!(buf, "\\u{:04x}", c as u32);
            }
            c => buf.push(c),
        }
    }
    buf.push('"');
}

/// Write an `AnyValue` as JSON.
fn write_any_value_json(buf: &mut String, v: &AnyValue) {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match &v.value {
        Some(Value::StringValue(s)) => write_json_string(buf, s),
        Some(Value::BoolValue(b)) => buf.push_str(if *b { "true" } else { "false" }),
        Some(Value::IntValue(i)) => {
            use std::fmt::Write;
            let _ = write!(buf, "{i}");
        }
        Some(Value::DoubleValue(d)) => {
            if d.is_finite() {
                use std::fmt::Write;
                let _ = write!(buf, "{d}");
            } else {
                buf.push_str("null");
            }
        }
        Some(Value::ArrayValue(arr)) => {
            buf.push('[');
            for (i, val) in arr.values.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_any_value_json(buf, val);
            }
            buf.push(']');
        }
        Some(Value::KvlistValue(kvl)) => {
            buf.push('{');
            for (i, kv) in kvl.values.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_json_string(buf, &kv.key);
                buf.push(':');
                if let Some(val) = &kv.value {
                    write_any_value_json(buf, val);
                } else {
                    buf.push_str("null");
                }
            }
            buf.push('}');
        }
        Some(Value::BytesValue(b)) => {
            use base64::Engine;
            buf.push('"');
            buf.push_str(&base64::engine::general_purpose::STANDARD.encode(b));
            buf.push('"');
        }
        None => buf.push_str("null"),
    }
}

/// Convert an `AnyValue` to a `String` (for promoted fields like service.name).
fn any_value_to_string(v: Option<&AnyValue>) -> Option<String> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    v.and_then(|av| match &av.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        _ => None,
    })
}

/// Convert an `ExportTraceServiceRequest` into an Arrow `RecordBatch`.
///
/// Flattens the nested protobuf hierarchy:
/// `ResourceSpans` -> `ScopeSpans` -> `Span` into flat rows.
///
/// Returns `Ok(None)` if the request contains zero spans.
///
/// # Errors
///
/// Returns `ArrowError` if column construction fails (e.g., type mismatch).
#[allow(clippy::too_many_lines)]
pub fn trace_request_to_batch(
    req: &ExportTraceServiceRequest,
    schema: &SchemaRef,
    received_at_nanos: i64,
) -> Result<Option<RecordBatch>, arrow_schema::ArrowError> {
    // Count total spans for capacity hints
    let total_spans: usize = req
        .resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum();

    if total_spans == 0 {
        return Ok(None);
    }

    // Column builders
    let mut trace_ids = FixedSizeBinaryBuilder::with_capacity(total_spans, 16);
    let mut span_ids = FixedSizeBinaryBuilder::with_capacity(total_spans, 8);
    let mut parent_span_ids = FixedSizeBinaryBuilder::with_capacity(total_spans, 8);
    let mut trace_states = StringBuilder::with_capacity(total_spans, total_spans * 8);
    let mut names = StringBuilder::with_capacity(total_spans, total_spans * 32);
    let mut kinds = Int32Builder::with_capacity(total_spans);
    let mut start_times = Int64Builder::with_capacity(total_spans);
    let mut end_times = Int64Builder::with_capacity(total_spans);
    let mut durations = Int64Builder::with_capacity(total_spans);
    let mut status_codes = Int32Builder::with_capacity(total_spans);
    let mut status_messages = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut res_service_names = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut res_service_versions = StringBuilder::with_capacity(total_spans, total_spans * 8);
    let mut res_attributes = StringBuilder::with_capacity(total_spans, total_spans * 64);
    let mut scope_names = StringBuilder::with_capacity(total_spans, total_spans * 16);
    let mut scope_versions = StringBuilder::with_capacity(total_spans, total_spans * 8);
    let mut span_attrs = StringBuilder::with_capacity(total_spans, total_spans * 64);
    let mut events_counts = Int32Builder::with_capacity(total_spans);
    let mut links_counts = Int32Builder::with_capacity(total_spans);
    let mut received_at = Int64Builder::with_capacity(total_spans);

    for resource_spans in &req.resource_spans {
        let resource = resource_spans.resource.as_ref();
        let svc_name = extract_resource_attr(resource, "service.name");
        let svc_version = extract_resource_attr(resource, "service.version");
        let res_attrs_json = resource_attributes_json(resource);

        for scope_spans in &resource_spans.scope_spans {
            let scope = scope_spans.scope.as_ref();
            let scope_n = scope.map(|s| s.name.as_str());
            let scope_v = scope.map(|s| s.version.as_str());

            for span in &scope_spans.spans {
                // trace_id: ensure exactly 16 bytes
                let tid = fixed_bytes(&span.trace_id, 16);
                trace_ids.append_value(&tid)?;

                // span_id: ensure exactly 8 bytes
                let sid = fixed_bytes(&span.span_id, 8);
                span_ids.append_value(&sid)?;

                // parent_span_id: null if all zeros or empty
                let psid = fixed_bytes(&span.parent_span_id, 8);
                if span.parent_span_id.is_empty() || is_all_zeros(&psid) {
                    parent_span_ids.append_null();
                } else {
                    parent_span_ids.append_value(&psid)?;
                }

                // trace_state
                if span.trace_state.is_empty() {
                    trace_states.append_null();
                } else {
                    trace_states.append_value(&span.trace_state);
                }

                // name
                names.append_value(&span.name);

                // kind (SpanKind enum as i32)
                kinds.append_value(span.kind);

                // timestamps
                #[allow(clippy::cast_possible_wrap)]
                let start_ns = span.start_time_unix_nano as i64;
                #[allow(clippy::cast_possible_wrap)]
                let end_ns = span.end_time_unix_nano as i64;
                start_times.append_value(start_ns);
                end_times.append_value(end_ns);
                durations.append_value(end_ns.saturating_sub(start_ns));

                // status
                if let Some(status) = &span.status {
                    status_codes.append_value(status.code);
                    if status.message.is_empty() {
                        status_messages.append_null();
                    } else {
                        status_messages.append_value(&status.message);
                    }
                } else {
                    status_codes.append_value(0); // STATUS_CODE_UNSET
                    status_messages.append_null();
                }

                // resource/scope/attributes (shared helper)
                append_context_fields(
                    svc_name.as_deref(),
                    res_attrs_json.as_deref(),
                    scope_n,
                    &span.attributes,
                    received_at_nanos,
                    &mut res_service_names,
                    &mut res_attributes,
                    &mut scope_names,
                    &mut span_attrs,
                    &mut received_at,
                );

                // trace-specific extra columns: service_version, scope_version
                append_nullable_opt(&mut res_service_versions, svc_version.as_deref());
                append_nullable_opt(&mut scope_versions, scope_v);

                // counts
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let ec = span.events.len() as i32;
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                let lc = span.links.len() as i32;
                events_counts.append_value(ec);
                links_counts.append_value(lc);
            }
        }
    }

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(trace_ids.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(parent_span_ids.finish()),
            Arc::new(trace_states.finish()),
            Arc::new(names.finish()),
            Arc::new(kinds.finish()),
            Arc::new(start_times.finish()),
            Arc::new(end_times.finish()),
            Arc::new(durations.finish()),
            Arc::new(status_codes.finish()),
            Arc::new(status_messages.finish()),
            Arc::new(res_service_names.finish()),
            Arc::new(res_service_versions.finish()),
            Arc::new(res_attributes.finish()),
            Arc::new(scope_names.finish()),
            Arc::new(scope_versions.finish()),
            Arc::new(span_attrs.finish()),
            Arc::new(events_counts.finish()),
            Arc::new(links_counts.finish()),
            Arc::new(received_at.finish()),
        ],
    )?;

    Ok(Some(batch))
}

/// Convert an `AnyValue` to a string for log body. Unlike `any_value_to_string`,
/// this handles complex types (Array, `KvList`, Bytes) by falling back to JSON.
fn any_value_to_body_string(v: Option<&AnyValue>) -> Option<String> {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    v.and_then(|av| match &av.value {
        Some(Value::StringValue(s)) => Some(s.clone()),
        Some(Value::IntValue(i)) => Some(i.to_string()),
        Some(Value::BoolValue(b)) => Some(b.to_string()),
        Some(Value::DoubleValue(d)) => Some(d.to_string()),
        Some(_) => {
            let mut buf = String::new();
            write_any_value_json(&mut buf, av);
            Some(buf)
        }
        None => None,
    })
}

// ── Metrics conversion ──

/// Convert an `ExportMetricsServiceRequest` into an Arrow `RecordBatch`.
///
/// Flattens `ResourceMetrics` -> `ScopeMetrics` -> `Metric` -> data points.
/// Each data point becomes one row.
///
/// # Errors
///
/// Returns `ArrowError` if column construction fails.
#[allow(clippy::too_many_lines)]
pub fn metrics_request_to_batch(
    req: &ExportMetricsServiceRequest,
    schema: &SchemaRef,
    received_at_nanos: i64,
) -> Result<Option<RecordBatch>, arrow_schema::ArrowError> {
    // Count total data points for capacity hints
    let total_points: usize = req
        .resource_metrics
        .iter()
        .flat_map(|rm| &rm.scope_metrics)
        .flat_map(|sm| &sm.metrics)
        .map(count_metric_points)
        .sum();

    if total_points == 0 {
        return Ok(None);
    }

    let mut metric_names = StringBuilder::with_capacity(total_points, total_points * 32);
    let mut metric_descs = StringBuilder::with_capacity(total_points, total_points * 32);
    let mut metric_units = StringBuilder::with_capacity(total_points, total_points * 8);
    let mut metric_types = Int32Builder::with_capacity(total_points);
    let mut timestamps = Int64Builder::with_capacity(total_points);
    let mut value_doubles = Float64Builder::with_capacity(total_points);
    let mut value_ints = Int64Builder::with_capacity(total_points);
    let mut hist_counts = UInt64Builder::with_capacity(total_points);
    let mut hist_sums = Float64Builder::with_capacity(total_points);
    let mut res_svc_names = StringBuilder::with_capacity(total_points, total_points * 16);
    let mut res_attrs = StringBuilder::with_capacity(total_points, total_points * 64);
    let mut scope_names_col = StringBuilder::with_capacity(total_points, total_points * 16);
    let mut attrs_col = StringBuilder::with_capacity(total_points, total_points * 64);
    let mut received_at = Int64Builder::with_capacity(total_points);

    for rm in &req.resource_metrics {
        let resource = rm.resource.as_ref();
        let svc_name = extract_resource_attr(resource, "service.name");
        let res_json = resource_attributes_json(resource);

        for sm in &rm.scope_metrics {
            let scope_name = sm.scope.as_ref().map(|s| s.name.as_str());

            for metric in &sm.metrics {
                let name = &metric.name;
                let desc = &metric.description;
                let unit = &metric.unit;

                let Some(data) = &metric.data else {
                    continue;
                };

                match data {
                    metric::Data::Gauge(g) => {
                        for dp in &g.data_points {
                            append_metric_row(
                                name,
                                desc,
                                unit,
                                0,
                                dp.time_unix_nano,
                                dp.value.as_ref(),
                                &dp.attributes,
                                svc_name.as_deref(),
                                res_json.as_deref(),
                                scope_name,
                                received_at_nanos,
                                &mut metric_names,
                                &mut metric_descs,
                                &mut metric_units,
                                &mut metric_types,
                                &mut timestamps,
                                &mut value_doubles,
                                &mut value_ints,
                                &mut hist_counts,
                                &mut hist_sums,
                                &mut res_svc_names,
                                &mut res_attrs,
                                &mut scope_names_col,
                                &mut attrs_col,
                                &mut received_at,
                            );
                        }
                    }
                    metric::Data::Sum(s) => {
                        for dp in &s.data_points {
                            append_metric_row(
                                name,
                                desc,
                                unit,
                                1,
                                dp.time_unix_nano,
                                dp.value.as_ref(),
                                &dp.attributes,
                                svc_name.as_deref(),
                                res_json.as_deref(),
                                scope_name,
                                received_at_nanos,
                                &mut metric_names,
                                &mut metric_descs,
                                &mut metric_units,
                                &mut metric_types,
                                &mut timestamps,
                                &mut value_doubles,
                                &mut value_ints,
                                &mut hist_counts,
                                &mut hist_sums,
                                &mut res_svc_names,
                                &mut res_attrs,
                                &mut scope_names_col,
                                &mut attrs_col,
                                &mut received_at,
                            );
                        }
                    }
                    metric::Data::Histogram(h) => {
                        for dp in &h.data_points {
                            append_histogram_row(
                                name,
                                desc,
                                unit,
                                2,
                                dp.time_unix_nano,
                                dp.count,
                                dp.sum,
                                &dp.attributes,
                                svc_name.as_deref(),
                                res_json.as_deref(),
                                scope_name,
                                received_at_nanos,
                                &mut metric_names,
                                &mut metric_descs,
                                &mut metric_units,
                                &mut metric_types,
                                &mut timestamps,
                                &mut value_doubles,
                                &mut value_ints,
                                &mut hist_counts,
                                &mut hist_sums,
                                &mut res_svc_names,
                                &mut res_attrs,
                                &mut scope_names_col,
                                &mut attrs_col,
                                &mut received_at,
                            );
                        }
                    }
                    metric::Data::ExponentialHistogram(eh) => {
                        for dp in &eh.data_points {
                            append_histogram_row(
                                name,
                                desc,
                                unit,
                                3,
                                dp.time_unix_nano,
                                dp.count,
                                dp.sum,
                                &dp.attributes,
                                svc_name.as_deref(),
                                res_json.as_deref(),
                                scope_name,
                                received_at_nanos,
                                &mut metric_names,
                                &mut metric_descs,
                                &mut metric_units,
                                &mut metric_types,
                                &mut timestamps,
                                &mut value_doubles,
                                &mut value_ints,
                                &mut hist_counts,
                                &mut hist_sums,
                                &mut res_svc_names,
                                &mut res_attrs,
                                &mut scope_names_col,
                                &mut attrs_col,
                                &mut received_at,
                            );
                        }
                    }
                    metric::Data::Summary(s) => {
                        for dp in &s.data_points {
                            append_histogram_row(
                                name,
                                desc,
                                unit,
                                4,
                                dp.time_unix_nano,
                                dp.count,
                                Some(dp.sum),
                                &dp.attributes,
                                svc_name.as_deref(),
                                res_json.as_deref(),
                                scope_name,
                                received_at_nanos,
                                &mut metric_names,
                                &mut metric_descs,
                                &mut metric_units,
                                &mut metric_types,
                                &mut timestamps,
                                &mut value_doubles,
                                &mut value_ints,
                                &mut hist_counts,
                                &mut hist_sums,
                                &mut res_svc_names,
                                &mut res_attrs,
                                &mut scope_names_col,
                                &mut attrs_col,
                                &mut received_at,
                            );
                        }
                    }
                }
            }
        }
    }

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(metric_names.finish()),
            Arc::new(metric_descs.finish()),
            Arc::new(metric_units.finish()),
            Arc::new(metric_types.finish()),
            Arc::new(timestamps.finish()),
            Arc::new(value_doubles.finish()),
            Arc::new(value_ints.finish()),
            Arc::new(hist_counts.finish()),
            Arc::new(hist_sums.finish()),
            Arc::new(res_svc_names.finish()),
            Arc::new(res_attrs.finish()),
            Arc::new(scope_names_col.finish()),
            Arc::new(attrs_col.finish()),
            Arc::new(received_at.finish()),
        ],
    )?;

    Ok(Some(batch))
}

/// Count data points in a metric across all data variants.
fn count_metric_points(m: &opentelemetry_proto::tonic::metrics::v1::Metric) -> usize {
    match &m.data {
        Some(metric::Data::Gauge(g)) => g.data_points.len(),
        Some(metric::Data::Sum(s)) => s.data_points.len(),
        Some(metric::Data::Histogram(h)) => h.data_points.len(),
        Some(metric::Data::ExponentialHistogram(eh)) => eh.data_points.len(),
        Some(metric::Data::Summary(s)) => s.data_points.len(),
        None => 0,
    }
}

/// Append one row for a Gauge/Sum `NumberDataPoint`.
#[allow(clippy::too_many_arguments)]
fn append_metric_row(
    name: &str,
    desc: &str,
    unit: &str,
    metric_type: i32,
    time_unix_nano: u64,
    value: Option<&number_data_point::Value>,
    dp_attrs: &[KeyValue],
    svc_name: Option<&str>,
    res_json: Option<&str>,
    scope_name: Option<&str>,
    received_at_nanos: i64,
    metric_names: &mut StringBuilder,
    metric_descs: &mut StringBuilder,
    metric_units: &mut StringBuilder,
    metric_types: &mut Int32Builder,
    timestamps: &mut Int64Builder,
    value_doubles: &mut Float64Builder,
    value_ints: &mut Int64Builder,
    hist_counts: &mut UInt64Builder,
    hist_sums: &mut Float64Builder,
    res_svc_names: &mut StringBuilder,
    res_attrs: &mut StringBuilder,
    scope_names_col: &mut StringBuilder,
    attrs_col: &mut StringBuilder,
    received_at: &mut Int64Builder,
) {
    metric_names.append_value(name);
    append_nullable_str(metric_descs, desc);
    append_nullable_str(metric_units, unit);
    metric_types.append_value(metric_type);
    #[allow(clippy::cast_possible_wrap)]
    timestamps.append_value(time_unix_nano as i64);

    match value {
        Some(number_data_point::Value::AsDouble(d)) => {
            value_doubles.append_value(*d);
            value_ints.append_null();
        }
        Some(number_data_point::Value::AsInt(i)) => {
            value_doubles.append_null();
            value_ints.append_value(*i);
        }
        None => {
            value_doubles.append_null();
            value_ints.append_null();
        }
    }
    hist_counts.append_null();
    hist_sums.append_null();

    append_context_fields(
        svc_name,
        res_json,
        scope_name,
        dp_attrs,
        received_at_nanos,
        res_svc_names,
        res_attrs,
        scope_names_col,
        attrs_col,
        received_at,
    );
}

/// Append one row for a Histogram/`ExponentialHistogram`/Summary data point.
#[allow(clippy::too_many_arguments)]
fn append_histogram_row(
    name: &str,
    desc: &str,
    unit: &str,
    metric_type: i32,
    time_unix_nano: u64,
    count: u64,
    sum: Option<f64>,
    dp_attrs: &[KeyValue],
    svc_name: Option<&str>,
    res_json: Option<&str>,
    scope_name: Option<&str>,
    received_at_nanos: i64,
    metric_names: &mut StringBuilder,
    metric_descs: &mut StringBuilder,
    metric_units: &mut StringBuilder,
    metric_types: &mut Int32Builder,
    timestamps: &mut Int64Builder,
    value_doubles: &mut Float64Builder,
    value_ints: &mut Int64Builder,
    hist_counts: &mut UInt64Builder,
    hist_sums: &mut Float64Builder,
    res_svc_names: &mut StringBuilder,
    res_attrs: &mut StringBuilder,
    scope_names_col: &mut StringBuilder,
    attrs_col: &mut StringBuilder,
    received_at: &mut Int64Builder,
) {
    metric_names.append_value(name);
    append_nullable_str(metric_descs, desc);
    append_nullable_str(metric_units, unit);
    metric_types.append_value(metric_type);
    #[allow(clippy::cast_possible_wrap)]
    timestamps.append_value(time_unix_nano as i64);

    value_doubles.append_null();
    value_ints.append_null();
    hist_counts.append_value(count);
    match sum {
        Some(s) => hist_sums.append_value(s),
        None => hist_sums.append_null(),
    }

    append_context_fields(
        svc_name,
        res_json,
        scope_name,
        dp_attrs,
        received_at_nanos,
        res_svc_names,
        res_attrs,
        scope_names_col,
        attrs_col,
        received_at,
    );
}

/// Append resource/scope/attribute/`received_at` fields (shared across all metric types).
#[allow(clippy::too_many_arguments)]
fn append_context_fields(
    svc_name: Option<&str>,
    res_json: Option<&str>,
    scope_name: Option<&str>,
    attrs: &[KeyValue],
    received_at_nanos: i64,
    res_svc_names: &mut StringBuilder,
    res_attrs: &mut StringBuilder,
    scope_names_col: &mut StringBuilder,
    attrs_col: &mut StringBuilder,
    received_at: &mut Int64Builder,
) {
    match svc_name {
        Some(s) => res_svc_names.append_value(s),
        None => res_svc_names.append_null(),
    }
    match res_json {
        Some(s) => res_attrs.append_value(s),
        None => res_attrs.append_null(),
    }
    match scope_name {
        Some(s) if !s.is_empty() => scope_names_col.append_value(s),
        _ => scope_names_col.append_null(),
    }
    match kv_to_json(attrs) {
        Some(s) => attrs_col.append_value(&s),
        None => attrs_col.append_null(),
    }
    received_at.append_value(received_at_nanos);
}

/// Append a string value as non-null if non-empty, null otherwise.
fn append_nullable_str(builder: &mut StringBuilder, s: &str) {
    if s.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(s);
    }
}

/// Append an `Option<&str>` — `Some(non-empty)` → value, else null.
fn append_nullable_opt(builder: &mut StringBuilder, s: Option<&str>) {
    match s {
        Some(v) if !v.is_empty() => builder.append_value(v),
        _ => builder.append_null(),
    }
}

// ── Logs conversion ──

/// Convert an `ExportLogsServiceRequest` into an Arrow `RecordBatch`.
///
/// Flattens `ResourceLogs` -> `ScopeLogs` -> `LogRecord`. Each record = one row.
///
/// # Errors
///
/// Returns `ArrowError` if column construction fails.
#[allow(clippy::too_many_lines)]
pub fn logs_request_to_batch(
    req: &ExportLogsServiceRequest,
    schema: &SchemaRef,
    received_at_nanos: i64,
) -> Result<Option<RecordBatch>, arrow_schema::ArrowError> {
    let total_records: usize = req
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();

    if total_records == 0 {
        return Ok(None);
    }

    let mut ts = Int64Builder::with_capacity(total_records);
    let mut observed_ts = Int64Builder::with_capacity(total_records);
    let mut sev_nums = Int32Builder::with_capacity(total_records);
    let mut sev_texts = StringBuilder::with_capacity(total_records, total_records * 8);
    let mut bodies = StringBuilder::with_capacity(total_records, total_records * 64);
    let mut trace_ids = FixedSizeBinaryBuilder::with_capacity(total_records, 16);
    let mut span_ids = FixedSizeBinaryBuilder::with_capacity(total_records, 8);
    let mut res_svc_names = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut res_attrs = StringBuilder::with_capacity(total_records, total_records * 64);
    let mut scope_names_col = StringBuilder::with_capacity(total_records, total_records * 16);
    let mut attrs_col = StringBuilder::with_capacity(total_records, total_records * 64);
    let mut received_at = Int64Builder::with_capacity(total_records);

    for rl in &req.resource_logs {
        let resource = rl.resource.as_ref();
        let svc_name = extract_resource_attr(resource, "service.name");
        let res_json = resource_attributes_json(resource);

        for sl in &rl.scope_logs {
            let scope_name = sl.scope.as_ref().map(|s| s.name.as_str());

            for log in &sl.log_records {
                #[allow(clippy::cast_possible_wrap)]
                ts.append_value(log.time_unix_nano as i64);

                if log.observed_time_unix_nano == 0 {
                    observed_ts.append_null();
                } else {
                    #[allow(clippy::cast_possible_wrap)]
                    observed_ts.append_value(log.observed_time_unix_nano as i64);
                }

                sev_nums.append_value(log.severity_number);

                if log.severity_text.is_empty() {
                    sev_texts.append_null();
                } else {
                    sev_texts.append_value(&log.severity_text);
                }

                match any_value_to_body_string(log.body.as_ref()) {
                    Some(s) => bodies.append_value(&s),
                    None => bodies.append_null(),
                }

                // trace_id: null if empty or all-zeros
                let tid = fixed_bytes(&log.trace_id, 16);
                if log.trace_id.is_empty() || is_all_zeros(&tid) {
                    trace_ids.append_null();
                } else {
                    trace_ids.append_value(&tid)?;
                }

                // span_id: null if empty or all-zeros
                let sid = fixed_bytes(&log.span_id, 8);
                if log.span_id.is_empty() || is_all_zeros(&sid) {
                    span_ids.append_null();
                } else {
                    span_ids.append_value(&sid)?;
                }

                // Resource/scope/attributes
                match &svc_name {
                    Some(s) => res_svc_names.append_value(s),
                    None => res_svc_names.append_null(),
                }
                match &res_json {
                    Some(s) => res_attrs.append_value(s),
                    None => res_attrs.append_null(),
                }
                match scope_name {
                    Some(s) if !s.is_empty() => scope_names_col.append_value(s),
                    _ => scope_names_col.append_null(),
                }
                match kv_to_json(&log.attributes) {
                    Some(s) => attrs_col.append_value(&s),
                    None => attrs_col.append_null(),
                }

                received_at.append_value(received_at_nanos);
            }
        }
    }

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(ts.finish()),
            Arc::new(observed_ts.finish()),
            Arc::new(sev_nums.finish()),
            Arc::new(sev_texts.finish()),
            Arc::new(bodies.finish()),
            Arc::new(trace_ids.finish()),
            Arc::new(span_ids.finish()),
            Arc::new(res_svc_names.finish()),
            Arc::new(res_attrs.finish()),
            Arc::new(scope_names_col.finish()),
            Arc::new(attrs_col.finish()),
            Arc::new(received_at.finish()),
        ],
    )?;

    Ok(Some(batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otel::schema::traces_schema;
    use arrow_array::Array;
    use arrow_schema::SchemaRef;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue as ProtoAnyValue, InstrumentationScope,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource as ProtoResource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    fn schema() -> SchemaRef {
        traces_schema()
    }

    fn make_kv(key: &str, val: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(ProtoAnyValue {
                value: Some(any_value::Value::StringValue(val.to_string())),
            }),
        }
    }

    fn make_kv_int(key: &str, val: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(ProtoAnyValue {
                value: Some(any_value::Value::IntValue(val)),
            }),
        }
    }

    fn make_test_request(num_spans: usize) -> ExportTraceServiceRequest {
        let spans: Vec<Span> = (0..num_spans)
            .map(|i| {
                let mut trace_id = vec![0u8; 16];
                trace_id[15] = (i + 1) as u8;
                let mut span_id = vec![0u8; 8];
                span_id[7] = (i + 1) as u8;

                Span {
                    trace_id,
                    span_id,
                    parent_span_id: vec![],
                    trace_state: String::new(),
                    name: format!("span-{i}"),
                    kind: 1, // INTERNAL
                    start_time_unix_nano: 1_000_000_000 * (i as u64 + 1),
                    end_time_unix_nano: 1_000_000_000 * (i as u64 + 1) + 500_000,
                    attributes: vec![make_kv("http.method", "GET")],
                    dropped_attributes_count: 0,
                    events: vec![],
                    dropped_events_count: 0,
                    links: vec![],
                    dropped_links_count: 0,
                    status: Some(Status {
                        message: String::new(),
                        code: 0,
                    }),
                    flags: 0,
                }
            })
            .collect();

        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(ProtoResource {
                    attributes: vec![
                        make_kv("service.name", "test-svc"),
                        make_kv("service.version", "1.0.0"),
                        make_kv("host.name", "test-host"),
                    ],
                    dropped_attributes_count: 0,
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "my-lib".to_string(),
                        version: "0.1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn test_empty_request() {
        let req = ExportTraceServiceRequest {
            resource_spans: vec![],
        };
        let result = trace_request_to_batch(&req, &schema(), 0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_single_span() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 999)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 20);
    }

    #[test]
    fn test_multi_span() {
        let req = make_test_request(10);
        let batch = trace_request_to_batch(&req, &schema(), 999)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 10);
    }

    #[test]
    fn test_large_batch() {
        let req = make_test_request(10_000);
        let batch = trace_request_to_batch(&req, &schema(), 999)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 10_000);
    }

    #[test]
    fn test_schema_matches() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 999)
            .unwrap()
            .unwrap();
        assert_eq!(batch.schema(), traces_schema());
    }

    #[test]
    fn test_trace_id_encoding() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        let tid = col.value(0);
        // First span: trace_id[15] = 1
        assert_eq!(tid.len(), 16);
        assert_eq!(tid[15], 1);
    }

    #[test]
    fn test_parent_span_id_null() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert!(col.is_null(0)); // empty parent => null
    }

    #[test]
    fn test_duration_computed() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let dur_col = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(dur_col.value(0), 500_000); // end - start
    }

    #[test]
    fn test_service_name_promoted() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let col = batch
            .column(11)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "test-svc");
    }

    #[test]
    fn test_resource_attrs_exclude_promoted() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let col = batch
            .column(13)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let json = col.value(0);
        // Should contain host.name but NOT service.name/service.version
        assert!(json.contains("host.name"));
        assert!(!json.contains("service.name"));
        assert!(!json.contains("service.version"));
    }

    #[test]
    fn test_attributes_json() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let col = batch
            .column(16)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        let json = col.value(0);
        assert!(json.contains("http.method"));
        assert!(json.contains("GET"));
    }

    #[test]
    fn test_scope_fields() {
        let req = make_test_request(1);
        let batch = trace_request_to_batch(&req, &schema(), 0).unwrap().unwrap();

        let name_col = batch
            .column(14)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "my-lib");

        let ver_col = batch
            .column(15)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(ver_col.value(0), "0.1.0");
    }

    #[test]
    fn test_received_at() {
        let req = make_test_request(3);
        let batch = trace_request_to_batch(&req, &schema(), 42)
            .unwrap()
            .unwrap();

        let col = batch
            .column(19)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        for i in 0..3 {
            assert_eq!(col.value(i), 42);
        }
    }

    #[test]
    fn test_int_attribute_to_string() {
        let kv = make_kv_int("retries", 3);
        let s = any_value_to_string(kv.value.as_ref());
        assert_eq!(s, Some("3".to_string()));
    }

    #[test]
    fn test_json_escaping() {
        let mut buf = String::new();
        write_json_string(&mut buf, "hello \"world\"\nline2");
        assert_eq!(buf, r#""hello \"world\"\nline2""#);
    }

    // ── Metrics tests ──

    use crate::otel::schema::metrics_schema;
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };

    fn metrics_schema_ref() -> SchemaRef {
        metrics_schema()
    }

    fn make_gauge_metric(name: &str, value: f64, ts: u64) -> Metric {
        Metric {
            name: name.to_string(),
            description: "test gauge".to_string(),
            unit: "ms".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: vec![make_kv("host", "web-1")],
                    start_time_unix_nano: 0,
                    time_unix_nano: ts,
                    exemplars: vec![],
                    flags: 0,
                    value: Some(number_data_point::Value::AsDouble(value)),
                }],
            })),
        }
    }

    fn make_metrics_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(ProtoResource {
                    attributes: vec![make_kv("service.name", "metrics-svc")],
                    dropped_attributes_count: 0,
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "meter".to_string(),
                        version: "1.0".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn test_metrics_empty_request() {
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![],
        };
        assert!(metrics_request_to_batch(&req, &metrics_schema_ref(), 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_metrics_gauge_double() {
        let req = make_metrics_request(vec![make_gauge_metric("cpu.usage", 42.5, 1000)]);
        let batch = metrics_request_to_batch(&req, &metrics_schema_ref(), 99)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);

        // metric_name
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "cpu.usage");

        // metric_type = 0 (gauge)
        let types = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(types.value(0), 0);

        // value_double
        let vd = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((vd.value(0) - 42.5).abs() < f64::EPSILON);

        // value_int is null
        let vi = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert!(vi.is_null(0));

        // histogram fields null
        assert!(batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn test_metrics_sum_int() {
        let metric = Metric {
            name: "requests".to_string(),
            description: String::new(),
            unit: "1".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 2000,
                    exemplars: vec![],
                    flags: 0,
                    value: Some(number_data_point::Value::AsInt(100)),
                }],
                aggregation_temporality: 2,
                is_monotonic: true,
            })),
        };
        let req = make_metrics_request(vec![metric]);
        let batch = metrics_request_to_batch(&req, &metrics_schema_ref(), 0)
            .unwrap()
            .unwrap();

        // metric_type = 1 (sum)
        let types = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(types.value(0), 1);

        // value_int = 100
        let vi = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(vi.value(0), 100);

        // value_double is null
        assert!(batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .is_null(0));
    }

    #[test]
    fn test_metrics_histogram() {
        let metric = Metric {
            name: "latency".to_string(),
            description: String::new(),
            unit: "ms".to_string(),
            metadata: vec![],
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![],
                    start_time_unix_nano: 0,
                    time_unix_nano: 3000,
                    count: 42,
                    sum: Some(1234.5),
                    bucket_counts: vec![10, 20, 12],
                    explicit_bounds: vec![10.0, 50.0],
                    exemplars: vec![],
                    flags: 0,
                    min: None,
                    max: None,
                }],
                aggregation_temporality: 1,
            })),
        };
        let req = make_metrics_request(vec![metric]);
        let batch = metrics_request_to_batch(&req, &metrics_schema_ref(), 0)
            .unwrap()
            .unwrap();

        // metric_type = 2 (histogram)
        let types = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(types.value(0), 2);

        // histogram_count = 42
        let hc = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(hc.value(0), 42);

        // histogram_sum = 1234.5
        let hs = batch
            .column(8)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap();
        assert!((hs.value(0) - 1234.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_metrics_schema_matches() {
        let req = make_metrics_request(vec![make_gauge_metric("x", 1.0, 0)]);
        let batch = metrics_request_to_batch(&req, &metrics_schema_ref(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(batch.schema(), metrics_schema());
    }

    #[test]
    fn test_metrics_service_name() {
        let req = make_metrics_request(vec![make_gauge_metric("x", 1.0, 0)]);
        let batch = metrics_request_to_batch(&req, &metrics_schema_ref(), 0)
            .unwrap()
            .unwrap();
        let col = batch
            .column(9)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "metrics-svc");
    }

    // ── Logs tests ──

    use crate::otel::schema::logs_schema;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};

    fn logs_schema_ref() -> SchemaRef {
        logs_schema()
    }

    fn make_log_record(body: &str, severity: i32, ts: u64) -> LogRecord {
        LogRecord {
            time_unix_nano: ts,
            observed_time_unix_nano: ts + 1000,
            severity_number: severity,
            severity_text: "INFO".to_string(),
            body: Some(ProtoAnyValue {
                value: Some(any_value::Value::StringValue(body.to_string())),
            }),
            attributes: vec![make_kv("env", "prod")],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
        }
    }

    fn make_logs_request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(ProtoResource {
                    attributes: vec![make_kv("service.name", "log-svc")],
                    dropped_attributes_count: 0,
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope {
                        name: "logger".to_string(),
                        version: "0.1".to_string(),
                        attributes: vec![],
                        dropped_attributes_count: 0,
                    }),
                    log_records: records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    #[test]
    fn test_logs_empty_request() {
        let req = ExportLogsServiceRequest {
            resource_logs: vec![],
        };
        assert!(logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .is_none());
    }

    #[test]
    fn test_logs_single_record() {
        let req = make_logs_request(vec![make_log_record("hello world", 9, 5000)]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 77)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 12);
    }

    #[test]
    fn test_logs_body_string() {
        let req = make_logs_request(vec![make_log_record("test message", 9, 0)]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();
        let col = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "test message");
    }

    #[test]
    fn test_logs_severity() {
        let req = make_logs_request(vec![make_log_record("err", 17, 0)]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();
        let sev = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(sev.value(0), 17); // ERROR
        let text = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "INFO");
    }

    #[test]
    fn test_logs_trace_correlation() {
        let mut record = make_log_record("with trace", 9, 0);
        record.trace_id = vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        record.span_id = vec![0, 0, 0, 0, 0, 0, 0, 2];
        let req = make_logs_request(vec![record]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();

        let tid = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert!(!tid.is_null(0));
        assert_eq!(tid.value(0)[15], 1);

        let sid = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert!(!sid.is_null(0));
        assert_eq!(sid.value(0)[7], 2);
    }

    #[test]
    fn test_logs_null_trace_ids() {
        let req = make_logs_request(vec![make_log_record("no trace", 9, 0)]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();
        let tid = batch
            .column(5)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert!(tid.is_null(0));
        let sid = batch
            .column(6)
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert!(sid.is_null(0));
    }

    #[test]
    fn test_logs_observed_timestamp() {
        let mut record = make_log_record("ts test", 9, 5000);
        record.observed_time_unix_nano = 0;
        let req = make_logs_request(vec![record]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();
        let obs = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert!(obs.is_null(0));
    }

    #[test]
    fn test_logs_schema_matches() {
        let req = make_logs_request(vec![make_log_record("x", 9, 0)]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(batch.schema(), logs_schema());
    }

    #[test]
    fn test_logs_service_name() {
        let req = make_logs_request(vec![make_log_record("x", 9, 0)]);
        let batch = logs_request_to_batch(&req, &logs_schema_ref(), 0)
            .unwrap()
            .unwrap();
        let col = batch
            .column(7)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(col.value(0), "log-svc");
    }
}
