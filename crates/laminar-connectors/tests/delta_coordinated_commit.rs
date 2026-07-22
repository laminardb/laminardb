//! Delta exactly-once provider-admission integration tests.

#![cfg(feature = "delta-lake-s3")]

use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{DeliveryGuarantee, SinkConnector};
use laminar_connectors::lakehouse::{DeltaLakeSink, DeltaLakeSinkConfig};

#[tokio::test]
async fn custom_s3_endpoint_fails_before_table_io() {
    let mut config = DeltaLakeSinkConfig::new("s3://warehouse/events");
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    config
        .storage_options
        .insert("aws_endpoint".into(), "http://127.0.0.1:9".into());

    let mut sink = DeltaLakeSink::new(config, None);
    let error = sink
        .open(&ConnectorConfig::new("delta-lake"))
        .await
        .expect_err("uncertified custom endpoint must fail before table I/O");
    assert!(
        error.to_string().contains("custom S3 endpoints"),
        "got: {error}"
    );
}
