//! Protobuf-to-Arrow conversion for OTel traces, metrics, and logs.
//!
//! Flattens the nested protobuf hierarchies into flat Arrow `RecordBatch` rows.

use std::sync::Arc;

use arrow_array::builder::{
    FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
    TimestampNanosecondBuilder, UInt64Builder,
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

/// Maximum nesting depth for `AnyValue` JSON serialization.
/// Protects against stack overflow from malicious OTLP payloads.
const MAX_JSON_DEPTH: usize = 32;

/// Write an `AnyValue` as JSON with bounded recursion depth.
fn write_any_value_json(buf: &mut String, v: &AnyValue) {
    write_any_value_json_depth(buf, v, 0);
}

fn write_any_value_json_depth(buf: &mut String, v: &AnyValue, depth: usize) {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    if depth >= MAX_JSON_DEPTH {
        buf.push_str("null");
        return;
    }
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
                write_any_value_json_depth(buf, val, depth + 1);
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
                    write_any_value_json_depth(buf, val, depth + 1);
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

/// Convert an `ExportTraceServiceRequest` into a `RecordBatch`, or `None` if empty.
///
/// # Errors
///
/// Returns `ArrowError` if column construction fails.
#[allow(clippy::too_many_lines)] // 20 columns = inherently long
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
    let mut received_at = TimestampNanosecondBuilder::with_capacity(total_spans);

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

struct MetricBuilders {
    names: StringBuilder,
    descs: StringBuilder,
    units: StringBuilder,
    types: Int32Builder,
    timestamps: Int64Builder,
    value_doubles: Float64Builder,
    value_ints: Int64Builder,
    hist_counts: UInt64Builder,
    hist_sums: Float64Builder,
    res_svc_names: StringBuilder,
    res_attrs: StringBuilder,
    scope_names: StringBuilder,
    attrs: StringBuilder,
    received_at: TimestampNanosecondBuilder,
}

struct MetricContext<'a> {
    name: &'a str,
    desc: &'a str,
    unit: &'a str,
    metric_type: i32,
    svc_name: Option<&'a str>,
    res_json: Option<&'a str>,
    scope_name: Option<&'a str>,
    received_at_nanos: i64,
}

impl MetricBuilders {
    fn with_capacity(n: usize) -> Self {
        Self {
            names: StringBuilder::with_capacity(n, n * 32),
            descs: StringBuilder::with_capacity(n, n * 32),
            units: StringBuilder::with_capacity(n, n * 8),
            types: Int32Builder::with_capacity(n),
            timestamps: Int64Builder::with_capacity(n),
            value_doubles: Float64Builder::with_capacity(n),
            value_ints: Int64Builder::with_capacity(n),
            hist_counts: UInt64Builder::with_capacity(n),
            hist_sums: Float64Builder::with_capacity(n),
            res_svc_names: StringBuilder::with_capacity(n, n * 16),
            res_attrs: StringBuilder::with_capacity(n, n * 64),
            scope_names: StringBuilder::with_capacity(n, n * 16),
            attrs: StringBuilder::with_capacity(n, n * 64),
            received_at: TimestampNanosecondBuilder::with_capacity(n),
        }
    }

    fn append_common(&mut self, ctx: &MetricContext, time_unix_nano: u64, dp_attrs: &[KeyValue]) {
        self.names.append_value(ctx.name);
        append_nullable_str(&mut self.descs, ctx.desc);
        append_nullable_str(&mut self.units, ctx.unit);
        self.types.append_value(ctx.metric_type);
        #[allow(clippy::cast_possible_wrap)]
        self.timestamps.append_value(time_unix_nano as i64);

        append_context_fields(
            ctx.svc_name,
            ctx.res_json,
            ctx.scope_name,
            dp_attrs,
            ctx.received_at_nanos,
            &mut self.res_svc_names,
            &mut self.res_attrs,
            &mut self.scope_names,
            &mut self.attrs,
            &mut self.received_at,
        );
    }

    fn append_number_dp(
        &mut self,
        ctx: &MetricContext,
        time_unix_nano: u64,
        value: Option<&number_data_point::Value>,
        dp_attrs: &[KeyValue],
    ) {
        self.append_common(ctx, time_unix_nano, dp_attrs);
        match value {
            Some(number_data_point::Value::AsDouble(d)) => {
                self.value_doubles.append_value(*d);
                self.value_ints.append_null();
            }
            Some(number_data_point::Value::AsInt(i)) => {
                self.value_doubles.append_null();
                self.value_ints.append_value(*i);
            }
            None => {
                self.value_doubles.append_null();
                self.value_ints.append_null();
            }
        }
        self.hist_counts.append_null();
        self.hist_sums.append_null();
    }

    fn append_histogram_dp(
        &mut self,
        ctx: &MetricContext,
        time_unix_nano: u64,
        count: u64,
        sum: Option<f64>,
        dp_attrs: &[KeyValue],
    ) {
        self.append_common(ctx, time_unix_nano, dp_attrs);
        self.value_doubles.append_null();
        self.value_ints.append_null();
        self.hist_counts.append_value(count);
        match sum {
            Some(s) => self.hist_sums.append_value(s),
            None => self.hist_sums.append_null(),
        }
    }

    fn finish(mut self, schema: &SchemaRef) -> Result<RecordBatch, arrow_schema::ArrowError> {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(self.names.finish()),
                Arc::new(self.descs.finish()),
                Arc::new(self.units.finish()),
                Arc::new(self.types.finish()),
                Arc::new(self.timestamps.finish()),
                Arc::new(self.value_doubles.finish()),
                Arc::new(self.value_ints.finish()),
                Arc::new(self.hist_counts.finish()),
                Arc::new(self.hist_sums.finish()),
                Arc::new(self.res_svc_names.finish()),
                Arc::new(self.res_attrs.finish()),
                Arc::new(self.scope_names.finish()),
                Arc::new(self.attrs.finish()),
                Arc::new(self.received_at.finish()),
            ],
        )
    }
}

/// Convert an `ExportMetricsServiceRequest` into an Arrow `RecordBatch`.
///
/// # Errors
///
/// Returns `ArrowError` if column construction fails.
#[allow(clippy::too_many_lines)] // 5 metric types × dispatch loop; builders already extracted
pub fn metrics_request_to_batch(
    req: &ExportMetricsServiceRequest,
    schema: &SchemaRef,
    received_at_nanos: i64,
) -> Result<Option<RecordBatch>, arrow_schema::ArrowError> {
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

    let mut b = MetricBuilders::with_capacity(total_points);

    for rm in &req.resource_metrics {
        let resource = rm.resource.as_ref();
        let svc_name = extract_resource_attr(resource, "service.name");
        let res_json = resource_attributes_json(resource);

        for sm in &rm.scope_metrics {
            let scope_name = sm.scope.as_ref().map(|s| s.name.as_str());

            for metric in &sm.metrics {
                let Some(data) = &metric.data else { continue };
                let base = MetricContext {
                    name: &metric.name,
                    desc: &metric.description,
                    unit: &metric.unit,
                    metric_type: 0,
                    svc_name: svc_name.as_deref(),
                    res_json: res_json.as_deref(),
                    scope_name,
                    received_at_nanos,
                };

                match data {
                    metric::Data::Gauge(g) => {
                        for dp in &g.data_points {
                            b.append_number_dp(
                                &base,
                                dp.time_unix_nano,
                                dp.value.as_ref(),
                                &dp.attributes,
                            );
                        }
                    }
                    metric::Data::Sum(s) => {
                        for dp in &s.data_points {
                            let ctx = MetricContext {
                                metric_type: 1,
                                ..base
                            };
                            b.append_number_dp(
                                &ctx,
                                dp.time_unix_nano,
                                dp.value.as_ref(),
                                &dp.attributes,
                            );
                        }
                    }
                    metric::Data::Histogram(h) => {
                        for dp in &h.data_points {
                            let ctx = MetricContext {
                                metric_type: 2,
                                ..base
                            };
                            b.append_histogram_dp(
                                &ctx,
                                dp.time_unix_nano,
                                dp.count,
                                dp.sum,
                                &dp.attributes,
                            );
                        }
                    }
                    metric::Data::ExponentialHistogram(eh) => {
                        for dp in &eh.data_points {
                            let ctx = MetricContext {
                                metric_type: 3,
                                ..base
                            };
                            b.append_histogram_dp(
                                &ctx,
                                dp.time_unix_nano,
                                dp.count,
                                dp.sum,
                                &dp.attributes,
                            );
                        }
                    }
                    metric::Data::Summary(s) => {
                        for dp in &s.data_points {
                            let ctx = MetricContext {
                                metric_type: 4,
                                ..base
                            };
                            b.append_histogram_dp(
                                &ctx,
                                dp.time_unix_nano,
                                dp.count,
                                Some(dp.sum),
                                &dp.attributes,
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(Some(b.finish(schema)?))
}

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
    received_at: &mut TimestampNanosecondBuilder,
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
/// # Errors
///
/// Returns `ArrowError` if column construction fails.
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
    let mut received_at = TimestampNanosecondBuilder::with_capacity(total_records);

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

                append_context_fields(
                    svc_name.as_deref(),
                    res_json.as_deref(),
                    scope_name,
                    &log.attributes,
                    received_at_nanos,
                    &mut res_svc_names,
                    &mut res_attrs,
                    &mut scope_names_col,
                    &mut attrs_col,
                    &mut received_at,
                );
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
mod tests;
