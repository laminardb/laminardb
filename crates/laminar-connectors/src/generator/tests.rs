use super::*;
use crate::connector::{DeliveryGuarantee, SourcePosition, SourceStart};
use laminar_core::checkpoint::CheckpointAttempt;

fn start_request(config: ConnectorConfig, position: SourcePosition) -> SourceStart {
    SourceStart::new(config, position, DeliveryGuarantee::AtLeastOnce).unwrap()
}

#[test]
fn source_contract_is_replayable_singleton() {
    let source = GeneratorSource::default();
    let contract = source
        .contract(&ConnectorConfig::new("generator"))
        .expect("static generator contract");
    assert_eq!(contract.consistency, SourceConsistency::Replayable);
    assert_eq!(contract.topology, SourceTopology::Singleton);
    assert_eq!(
        contract.row_positions,
        SourceRowPositionCapability::OrderedDeterministic
    );
    assert!(contract.is_exact_delivery_certified());
}

#[test]
fn row_positions_bind_source_partition_and_sequence() {
    let mut source = GeneratorSource::default();
    source.input_channel = generator_input_channel("prices").unwrap();
    let positions = source.build_row_positions(7, 2).unwrap();
    let checkpoint = source.checkpoint();
    let expected_channel = positions.partition().value(0).to_vec();

    assert_eq!(&positions.partition().value(0)[4..10], b"prices");
    assert_eq!(
        positions.partition().value(0),
        positions.partition().value(1)
    );
    assert_eq!(positions.order_key().value(0), 7_u64.to_be_bytes());
    assert_eq!(positions.order_key().value(1), 8_u64.to_be_bytes());
    assert_eq!(positions.sub_offset().values(), &[0, 0]);
    assert_eq!(
        checkpoint.input_channels(),
        Some(std::slice::from_ref(&expected_channel))
    );
}

#[tokio::test]
async fn deterministic_and_replayable_across_resume() {
    let mut config = ConnectorConfig::new("generator");
    config.set("rows.per.second", "1000000");
    let mut a = GeneratorSource::default();
    a.start(start_request(config.clone(), SourcePosition::Initial))
        .await
        .unwrap();
    let _ = a.poll_batch(8).await.unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let first = a.poll_batch(8).await.unwrap().expect("rows");
    assert_eq!(first.num_rows(), 8);

    let cp = a.checkpoint();
    assert_eq!(
        cp.get_metadata("connector"),
        Some(GENERATOR_CHECKPOINT_CONNECTOR)
    );
    assert_eq!(
        cp.get_metadata(CHECKPOINT_VERSION_METADATA),
        Some(GENERATOR_CHECKPOINT_VERSION)
    );
    let mut b = GeneratorSource::default();
    b.start(start_request(
        config,
        SourcePosition::Resume {
            attempt: CheckpointAttempt::new(1, 1),
            checkpoint: cp,
        },
    ))
    .await
    .unwrap();
    let _ = b.poll_batch(4).await.unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    let from_a = a.poll_batch(4).await.unwrap().expect("rows").records;
    let from_b = b.poll_batch(4).await.unwrap().expect("rows").records;
    assert_eq!(from_a, from_b, "replay from offset must be byte-identical");
}

#[tokio::test]
async fn rate_limit_and_max_rows_bound_emission() {
    let mut config = ConnectorConfig::new("generator");
    config.set("rows.per.second", "1000");
    config.set("max.rows", "3");
    let mut g = GeneratorSource::default();
    g.start(start_request(config, SourcePosition::Initial))
        .await
        .unwrap();
    let _ = g.poll_batch(100).await.unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    let batch = g.poll_batch(100).await.unwrap().expect("rows");
    assert_eq!(batch.num_rows(), 3, "max.rows caps emission");
    assert!(g.poll_batch(100).await.unwrap().is_none(), "exhausted");
}

#[tokio::test]
async fn malformed_resume_fails_before_rate_anchor() {
    let mut checkpoint = GeneratorSource::default().checkpoint();
    checkpoint.set_offset("seq", "not-a-sequence");
    let mut source = GeneratorSource::default();
    let error = source
        .start(start_request(
            ConnectorConfig::new("generator"),
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(1, 1),
                checkpoint,
            },
        ))
        .await
        .expect_err("malformed durable cursor must fail closed");
    assert!(error.to_string().contains("bad generator offset"));
    assert!(source.anchor.is_none());
}

#[tokio::test]
async fn resume_rejects_wrong_checkpoint_identity_or_version() {
    for (connector, version, expected) in [
        (
            "files",
            GENERATOR_CHECKPOINT_VERSION,
            "belongs to connector 'files'",
        ),
        (
            GENERATOR_CHECKPOINT_CONNECTOR,
            "1",
            "requires checkpoint.version=2",
        ),
    ] {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("seq", "8");
        checkpoint.set_metadata("connector", connector);
        checkpoint.set_metadata(CHECKPOINT_VERSION_METADATA, version);

        let mut source = GeneratorSource::default();
        let error = source
            .start(start_request(
                ConnectorConfig::new("generator"),
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::canonical(7),
                    checkpoint,
                },
            ))
            .await
            .expect_err("non-current generator checkpoint must be rejected");
        assert!(error.to_string().contains(expected), "{error}");
        assert!(source.anchor.is_none());
    }
}

#[tokio::test]
async fn resume_at_finite_end_remains_exhausted() {
    let mut config = ConnectorConfig::new("generator");
    config.set("rows.per.second", "1000000");
    config.set("max.rows", "8");
    let mut checkpoint = GeneratorSource::default().checkpoint();
    checkpoint.set_offset("seq", "8");
    let mut source = GeneratorSource::default();
    source
        .start(start_request(
            config,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(7, 7),
                checkpoint,
            },
        ))
        .await
        .unwrap();

    assert!(source.poll_batch(8).await.unwrap().is_none());
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert!(source.poll_batch(8).await.unwrap().is_none());
    assert_eq!(source.checkpoint().get_offset("seq"), Some("8"));
}
