use super::*;

#[test]
fn test_mock_batch() {
    let batch = mock_batch(10);
    assert_eq!(batch.num_rows(), 10);
    assert_eq!(batch.num_columns(), 2);
}

#[tokio::test]
async fn test_mock_source_connector() {
    let mut source = MockSourceConnector::with_batches(3, 5);
    source
        .start(
            crate::connector::SourceStart::new(
                ConnectorConfig::new("mock"),
                crate::connector::SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let b1 = source.poll_batch(100).await.unwrap();
    assert!(b1.is_some());
    assert_eq!(b1.unwrap().num_rows(), 5);

    let b2 = source.poll_batch(100).await.unwrap();
    assert!(b2.is_some());

    let b3 = source.poll_batch(100).await.unwrap();
    assert!(b3.is_some());

    let b4 = source.poll_batch(100).await.unwrap();
    assert!(b4.is_none());

    assert_eq!(source.records_produced(), 15);

    let cp = source.checkpoint();
    assert_eq!(cp.get_offset("records"), Some("15"));

    source.close().await.unwrap();
}

#[tokio::test]
async fn test_mock_sink_connector() {
    let mut sink = MockSinkConnector::new();
    sink.open(&ConnectorConfig::new("mock")).await.unwrap();

    let batch = mock_batch(10);
    let result = sink.write_batch(&batch).await.unwrap();
    assert_eq!(result.records_written, 10);

    assert_eq!(sink.batch_count(), 1);
    assert_eq!(sink.records_written(), 10);

    sink.write_batch(&mock_batch(5)).await.unwrap();

    assert_eq!(sink.records_written(), 15);
    assert_eq!(sink.batch_count(), 2);

    let contract = sink.contract(&ConnectorConfig::new("mock")).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
    assert_eq!(contract.topology, SinkTopology::NodeLocalEgress);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(60));

    sink.close().await.unwrap();
}

#[tokio::test]
async fn test_mock_source_resume() {
    let mut source = MockSourceConnector::new();
    let mut checkpoint = SourceCheckpoint::new();
    checkpoint.set_offset("records", "10");
    source
        .start(
            crate::connector::SourceStart::new(
                ConnectorConfig::new("mock"),
                crate::connector::SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::new(5, 5),
                    checkpoint,
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(source.records_produced(), 10);
}

#[test]
fn test_register_helpers() {
    let registry = ConnectorRegistry::new();
    register_mock_source(&registry).unwrap();
    register_mock_sink(&registry).unwrap();

    assert!(registry.source_info("mock").is_some());
    assert!(registry.sink_info("mock").is_some());
}
