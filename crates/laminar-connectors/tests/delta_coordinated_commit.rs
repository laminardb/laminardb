//! Delta exactly-once provider-admission integration tests.

#![cfg(feature = "delta-lake-s3")]

use std::time::Duration;

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{DeliveryGuarantee, SinkConnector};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig};

#[tokio::test]
async fn custom_s3_endpoint_passes_admission_before_table_io() {
    let mut config = DeltaLakeSinkConfig::new("s3://warehouse/events");
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    config.write_timeout = Duration::from_secs(5);
    config
        .storage_options
        .insert("aws_endpoint".into(), "http://127.0.0.1:9".into());
    config
        .storage_options
        .insert("aws_conditional_put".into(), "etag".into());

    let mut sink = DeltaLakeSink::new(config, None);
    let error = sink
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .expect_err("unreachable endpoint must fail during table I/O");
    assert!(
        matches!(
            error,
            ConnectorError::ConnectionFailed(_) | ConnectorError::TransactionError(_)
        ),
        "expected table I/O failure after admission, got: {error}"
    );
    assert!(
        error.to_string().contains("table open")
            || error.to_string().contains("failed to open Delta table"),
        "expected table I/O failure after admission, got: {error}"
    );
}
