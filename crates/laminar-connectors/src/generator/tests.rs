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
    assert!(!contract.is_cluster_exact_delivery_certified());
}

#[test]
fn row_positions_bind_source_partition_and_sequence() {
    let mut source = GeneratorSource::default();
    source.input_channel = generator_input_channel("prices", 0).unwrap();
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

mod splits {
    use std::num::{NonZeroU32, NonZeroU64};

    use arrow_array::{Array as _, Int64Array};
    use laminar_core::checkpoint::AssignmentDrainId;
    use laminar_core::state::{NodeId, VnodeRegistry};

    use super::*;
    use crate::connector::{SourceBatchCursor, SourceDrainRequest, SourceDrainSettlement};

    const VNODES: u32 = 4;

    fn config(splits: u32, max_rows: u64) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("generator");
        config.set("rows.per.second", "1000000");
        config.set("max.rows", max_rows.to_string());
        config.set("splits", splits.to_string());
        config.set("laminar.source.name", "gen");
        config
    }

    fn resume(checkpoint: SourceCheckpoint) -> SourcePosition {
        SourcePosition::Resume {
            attempt: CheckpointAttempt::new(1, 1),
            checkpoint,
        }
    }

    fn publish(registry: &VnodeRegistry, owners: [u64; VNODES as usize], version: u64) {
        registry.set_assignment_and_version(owners.map(NodeId).to_vec().into(), version);
    }

    fn channels_owned_by(owners: [u64; VNODES as usize], node: u64) -> Vec<u32> {
        let vnodes = NonZeroU32::new(VNODES).unwrap();
        (0..VNODES)
            .filter(|channel| owners[split::ROUTE.vnode("gen", *channel, vnodes) as usize] == node)
            .collect()
    }

    fn channels(checkpoint: &SourceCheckpoint) -> Vec<u32> {
        checkpoint
            .input_channels()
            .unwrap()
            .iter()
            .map(|channel| u32::from_be_bytes(channel[channel.len() - 4..].try_into().unwrap()))
            .collect()
    }

    async fn cluster_source(
        registry: &Arc<VnodeRegistry>,
        node: u64,
        config: ConnectorConfig,
        position: SourcePosition,
    ) -> GeneratorSource {
        let mut source = GeneratorSource::default();
        source
            .set_vnode_assignment("gen", Arc::clone(registry), NodeId(node))
            .unwrap();
        source.start(start_request(config, position)).await.unwrap();
        source.drive_control_plane();
        source
    }

    /// Read up to `rows` rows as `(seq, channel)`, stopping once the source stays empty.
    async fn read(source: &mut GeneratorSource, rows: usize) -> Vec<(u64, u32)> {
        let mut read = Vec::new();
        let mut idle = 0;
        while read.len() < rows && idle < 20 {
            let Some(batch) = source.poll_batch(rows - read.len()).await.unwrap() else {
                idle += 1;
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                continue;
            };
            idle = 0;
            let seqs = batch
                .records
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let positions = batch.row_positions().unwrap();
            for row in 0..seqs.len() {
                let channel = positions.partition().value(row);
                read.push((
                    u64::try_from(seqs.value(row)).unwrap(),
                    u32::from_be_bytes(channel[channel.len() - 4..].try_into().unwrap()),
                ));
            }
        }
        read
    }

    /// The cursor every participant merges into a cluster checkpoint.
    fn merge(parts: &[SourceCheckpoint], version: u64) -> Arc<SourceCheckpoint> {
        let mut merged = SourceCheckpoint::new();
        let mut channels = Vec::new();
        for part in parts {
            for (key, value) in part.offsets() {
                merged.set_offset(key.clone(), value.clone());
            }
            channels.extend(part.input_channels().unwrap().iter().cloned());
        }
        for (key, value) in parts[0].metadata() {
            merged.set_metadata(key.clone(), value.clone());
        }
        channels.sort_unstable();
        merged.set_input_channels(channels).unwrap();
        merged.bind_assignment_version(NonZeroU64::new(version).unwrap());
        Arc::new(merged)
    }

    fn round(predecessor: u64) -> AssignmentDrainId {
        AssignmentDrainId {
            predecessor_version: predecessor,
            target_version: predecessor + 1,
            digest: [7; 32],
        }
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(5)
    }

    async fn begin(sources: &mut [&mut GeneratorSource], round: AssignmentDrainId) {
        let request = SourceDrainRequest::new(round).unwrap();
        for source in sources {
            source.begin_drain(&request, deadline()).unwrap();
            assert!(source.poll_drain_ready(round).unwrap());
            assert!(source.poll_batch(64).await.unwrap().is_none());
        }
    }

    fn sorted(rows: &[(u64, u32)]) -> Vec<u64> {
        let mut seqs: Vec<u64> = rows.iter().map(|(seq, _)| *seq).collect();
        seqs.sort_unstable();
        seqs
    }

    #[test]
    fn split_contract_is_placed_and_certified_for_exactly_once() {
        let contract = GeneratorSource::default().contract(&config(4, 10)).unwrap();
        assert_eq!(contract.topology, SourceTopology::Splittable);
        assert_eq!(
            contract.placement,
            Some(SourceChannelPlacement {
                channels: NonZeroU32::new(4).unwrap(),
                route: laminar_core::state::SourceChannelRoute::V1,
            })
        );
        assert!(contract.is_exact_delivery_certified());
        assert!(contract.is_cluster_exact_delivery_certified());

        let unsplit = GeneratorSource::default()
            .contract(&ConnectorConfig::new("generator"))
            .unwrap();
        assert_eq!(unsplit.placement, None);

        for (splits, expected) in [(0, "splits must be > 0"), (65_536, "at most 65535")] {
            let error = GeneratorSource::default()
                .contract(&config(splits, 10))
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn split_channels_emit_the_sequence_once_across_resume() {
        let mut first = GeneratorSource::default();
        first
            .start(start_request(config(3, 30), SourcePosition::Initial))
            .await
            .unwrap();
        let mut rows = read(&mut first, 12).await;
        let checkpoint = first.checkpoint();
        assert_eq!(channels(&checkpoint), [0, 1, 2]);
        assert_eq!(checkpoint.assignment_version(), None);

        let mut resumed = GeneratorSource::default();
        resumed
            .start(start_request(config(3, 30), resume(checkpoint)))
            .await
            .unwrap();
        rows.extend(read(&mut resumed, 100).await);

        assert!(rows
            .iter()
            .all(|(seq, channel)| seq % 3 == u64::from(*channel)));
        assert_eq!(sorted(&rows), (0..30).collect::<Vec<_>>());
        let end = resumed.checkpoint();
        assert_eq!(
            [
                end.get_offset("seq.0"),
                end.get_offset("seq.1"),
                end.get_offset("seq.2")
            ],
            [Some("30"), Some("31"), Some("32")]
        );
    }

    #[tokio::test]
    async fn resume_rejects_another_split_count_or_an_unsplit_cursor() {
        let mut three = GeneratorSource::default();
        three
            .start(start_request(config(3, 30), SourcePosition::Initial))
            .await
            .unwrap();
        for (checkpoint, expected) in [
            (
                three.checkpoint(),
                "has 3 splits, the source is configured with 4",
            ),
            (
                GeneratorSource::default().checkpoint(),
                "requires checkpoint.version=3",
            ),
        ] {
            let error = GeneratorSource::default()
                .start(start_request(config(4, 30), resume(checkpoint)))
                .await
                .unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn cluster_source_reads_its_routed_channels_once_published() {
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        let mut source =
            cluster_source(&registry, 1, config(4, 400), SourcePosition::Initial).await;
        assert!(!source.checkpoint_ready().unwrap());
        assert!(source.try_checkpoint().unwrap().is_none());
        assert!(source.poll_batch(64).await.unwrap().is_none());

        let owners = [1, 2, 1, 2];
        publish(&registry, owners, 1);
        source.drive_control_plane();
        assert!(source.checkpoint_ready().unwrap());
        let checkpoint = source.try_checkpoint().unwrap().unwrap();
        assert_eq!(
            checkpoint.assignment_version().map(NonZeroU64::get),
            Some(1)
        );
        let expected = channels_owned_by(owners, 1);
        assert_eq!(channels(&checkpoint), expected);

        // The first poll anchors the rate limit.
        assert!(source.poll_batch(8).await.unwrap().is_none());
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let mut batch = source.poll_batch(8).await.unwrap().expect("rows");
        match batch.take_cursor() {
            Some(SourceBatchCursor::Complete(cursor)) => {
                assert_eq!(cursor.assignment_version().map(NonZeroU64::get), Some(1));
            }
            other => panic!("cluster batch must carry a complete cursor, got {other:?}"),
        }
        let rows = read(&mut source, 400).await;
        assert!(rows.iter().all(|(_, channel)| expected.contains(channel)));
    }

    #[tokio::test]
    async fn committed_drains_move_channels_at_the_handoff_cursor_and_back() {
        let config = config(4, 400);
        let (left, right) = (
            Arc::new(VnodeRegistry::new_unassigned(VNODES)),
            Arc::new(VnodeRegistry::new_unassigned(VNODES)),
        );
        publish(&left, [1; 4], 1);
        publish(&right, [1; 4], 1);
        let mut a = cluster_source(&left, 1, config.clone(), SourcePosition::Initial).await;
        let mut b = cluster_source(&right, 2, config, SourcePosition::Initial).await;
        let mut rows = read(&mut a, 100).await;
        assert!(read(&mut b, 1).await.is_empty());

        // Every channel moves to node 2.
        begin(&mut [&mut a, &mut b], round(1)).await;
        let handoff = merge(&[a.checkpoint(), b.checkpoint()], 1);
        publish(&left, [2; 4], 2);
        publish(&right, [2; 4], 2);
        for source in [&mut a, &mut b] {
            source
                .finish_drain(
                    SourceDrainSettlement::commit(round(1), Arc::clone(&handoff)),
                    deadline(),
                )
                .await
                .unwrap();
        }
        assert!(channels(&a.checkpoint()).is_empty());
        assert_eq!(b.checkpoint().offsets(), handoff.offsets());
        rows.extend(read(&mut b, 150).await);

        // And back to node 1, which must not resume from what it read before.
        begin(&mut [&mut a, &mut b], round(2)).await;
        let handoff = merge(&[a.checkpoint(), b.checkpoint()], 2);
        publish(&left, [1; 4], 3);
        publish(&right, [1; 4], 3);
        for source in [&mut a, &mut b] {
            source
                .finish_drain(
                    SourceDrainSettlement::commit(round(2), Arc::clone(&handoff)),
                    deadline(),
                )
                .await
                .unwrap();
        }
        assert_eq!(a.checkpoint().offsets(), handoff.offsets());
        assert_eq!(
            a.checkpoint().assignment_version().map(NonZeroU64::get),
            Some(3)
        );
        rows.extend(read(&mut a, 400).await);

        assert_eq!(sorted(&rows), (0..400).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn a_commit_before_the_target_publication_waits_for_it() {
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, [1; 4], 1);
        let mut a = cluster_source(&registry, 1, config(4, 400), SourcePosition::Initial).await;
        let _ = read(&mut a, 40).await;
        begin(&mut [&mut a], round(1)).await;
        let handoff = merge(&[a.checkpoint()], 1);
        a.finish_drain(SourceDrainSettlement::commit(round(1), handoff), deadline())
            .await
            .unwrap();
        assert!(a.checkpoint_ready().unwrap(), "the predecessor still holds");

        publish(&registry, [1, 1, 2, 2], 2);
        assert!(!a.checkpoint_ready().unwrap());
        a.drive_control_plane();
        assert!(a.checkpoint_ready().unwrap());
        assert_eq!(
            channels(&a.checkpoint()),
            channels_owned_by([1, 1, 2, 2], 1)
        );
    }

    #[tokio::test]
    async fn an_aborted_drain_keeps_reading_the_same_channels() {
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, [1; 4], 1);
        let mut a = cluster_source(&registry, 1, config(4, 400), SourcePosition::Initial).await;
        let mut rows = read(&mut a, 40).await;
        begin(&mut [&mut a], round(1)).await;
        let before = a.checkpoint();
        a.finish_drain(SourceDrainSettlement::abort(round(1)), deadline())
            .await
            .unwrap();
        assert_eq!(a.checkpoint(), before);
        rows.extend(read(&mut a, 400).await);
        assert_eq!(sorted(&rows), (0..400).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn outside_a_drain_only_a_channel_new_to_the_source_starts_from_its_start_cursor() {
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, [2; 4], 1);
        let start = {
            let mut cursor = SourceCheckpoint::new();
            for channel in 0..VNODES {
                cursor.set_offset(
                    format!("seq.{channel}"),
                    (u64::from(channel) + 40).to_string(),
                );
            }
            cursor.set_metadata("connector", GENERATOR_CHECKPOINT_CONNECTOR);
            cursor.set_metadata(CHECKPOINT_VERSION_METADATA, split::SPLIT_CHECKPOINT_VERSION);
            cursor.set_metadata(split::SPLITS_METADATA, VNODES.to_string());
            cursor
                .set_input_channels(
                    (0..VNODES)
                        .map(|channel| generator_input_channel("gen", channel).unwrap())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            cursor
        };
        // A source restarted by recovery takes the target assignment from its resume cut.
        let mut a = cluster_source(&registry, 1, config(4, 400), resume(start.clone())).await;
        assert!(channels(&a.checkpoint()).is_empty());
        publish(&registry, [1; 4], 2);
        a.drive_control_plane();
        assert_eq!(a.checkpoint().offsets(), start.offsets());

        // A channel it gave up comes back only through a committed drain.
        let _ = read(&mut a, 40).await;
        publish(&registry, [2; 4], 3);
        a.drive_control_plane();
        assert!(a.checkpoint_ready().unwrap());
        publish(&registry, [1; 4], 4);
        a.drive_control_plane();
        let error = a.checkpoint_ready().unwrap_err();
        assert!(error.to_string().contains("outside a drain"), "{error}");
    }

    /// A recovery restart starts the same source again from the recovery cut, so a channel the
    /// source gave up before the fault comes back from that cut rather than being refused.
    #[tokio::test]
    async fn a_restart_takes_back_a_channel_it_gave_up_from_the_resume_cut() {
        let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
        publish(&registry, [1; 4], 1);
        let mut a = cluster_source(&registry, 1, config(4, 400), SourcePosition::Initial).await;
        let mut rows = read(&mut a, 40).await;
        let cut = a.checkpoint();
        publish(&registry, [2; 4], 2);
        a.drive_control_plane();
        assert!(channels(&a.checkpoint()).is_empty());

        a.start(start_request(config(4, 400), resume(cut.clone())))
            .await
            .unwrap();
        publish(&registry, [1; 4], 3);
        a.drive_control_plane();
        assert!(a.checkpoint_ready().unwrap());
        assert_eq!(a.checkpoint().offsets(), cut.offsets());
        rows.extend(read(&mut a, 400).await);
        assert_eq!(sorted(&rows), (0..400).collect::<Vec<_>>());
    }
}
