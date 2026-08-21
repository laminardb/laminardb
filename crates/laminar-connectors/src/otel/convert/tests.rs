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
                entity_refs: vec![],
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
        .downcast_ref::<arrow_array::TimestampNanosecondArray>()
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
    Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    Sum,
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
                entity_refs: vec![],
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
        event_name: String::new(),
    }
}

fn make_logs_request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(ProtoResource {
                attributes: vec![make_kv("service.name", "log-svc")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
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
