#![allow(clippy::disallowed_types)]
//! Exact-manifest and range-read checkpoint benchmarks.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_core::checkpoint::{
    checkpoint_sha256, merge_node_subscription_manifests, ByteRange, ChangelogMode,
    ChannelProgress, CheckpointAssignmentFence, CheckpointManifest, CheckpointParticipant,
    CheckpointStore, ConnectorCheckpoint, NodePartitionRange, NodeSubscriptionManifest,
    NodeSubscriptionStreamManifest, ObjectStoreCheckpointStore, OutputDistribution,
    OutputDistributionCertificate, OutputPartitionId, PartitionSequence, PipelineIdentity,
    StateFrame, StateFrameKey, StreamGeneration, SubscriptionDigest, SubscriptionProtocolVersion,
    OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
use laminar_core::serialization::{
    serialize_batches_stream_bounded, serialize_batches_stream_lz4_bounded,
};
use laminar_core::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};

fn synthetic_state(size: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let mut value = 0xDEAD_BEEF_CAFE_BABE_u64;
    while data.len() < size {
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        data.extend_from_slice(&value.to_le_bytes());
    }
    data.truncate(size);
    data
}

fn checkpoint_payload(state_bytes: usize) -> Bytes {
    Bytes::from(synthetic_state(state_bytes))
}

fn checkpoint_manifest(id: u64, sources: usize, payload: &[u8]) -> CheckpointManifest {
    let key_groups = KeyGroupCount::try_from(1_u16).unwrap();
    let mut manifest = CheckpointManifest::new_with_key_group_count(id, id, key_groups);
    manifest.deployment_id = uuid::Uuid::from_u128(1).to_string();

    for source in 0..sources {
        let name = format!("source_{source:03}");
        manifest.source_names.push(name.clone());
        manifest.source_offsets.insert(
            name.clone(),
            ConnectorCheckpoint::with_offsets(HashMap::from([
                ("events:0".into(), (1_000 * id).to_string()),
                ("events:1".into(), (2_000 * id).to_string()),
            ])),
        );
        manifest.channel_progress.push(ChannelProgress {
            participant_id: manifest.participant_id,
            source_name: name,
            input_channel: vec![0],
            watermark: Some(500_000 + source as i64),
            idle: false,
        });
    }
    manifest.checkpoint_watermark = (!manifest.channel_progress.is_empty()).then_some(500_000);

    manifest.node_data.object_length = payload.len() as u64;
    manifest.node_data.sha256 = checkpoint_sha256(payload);
    manifest.state_frames.push(StateFrame {
        key: StateFrameKey::OperatorWhole {
            operator_id: "aggregate".into(),
        },
        chunk: manifest.node_data.chunk,
        range: ByteRange {
            offset: 0,
            length: payload.len() as u64,
        },
        sha256: checkpoint_sha256(payload),
    });
    manifest
}

fn store(directory: &std::path::Path) -> ObjectStoreCheckpointStore {
    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new_with_prefix(directory).unwrap());
    ObjectStoreCheckpointStore::new(backing, "")
        .with_key_group_count(KeyGroupCount::try_from(1_u16).unwrap())
}

fn bench_exact_manifest_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_exact_manifest_load");
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for &(sources, state_bytes) in &[(2, 4_096), (10, 4_096), (50, 4_096)] {
        let directory = tempfile::tempdir().unwrap();
        let store = store(directory.path());
        let payload = checkpoint_payload(state_bytes);
        let manifest = checkpoint_manifest(1, sources, &payload);
        runtime
            .block_on(store.save_checkpoint(&manifest, std::slice::from_ref(&payload)))
            .unwrap();

        group.bench_function(BenchmarkId::new("load", sources), |bencher| {
            bencher.iter(|| black_box(runtime.block_on(store.load_manifest(1)).unwrap().unwrap()))
        });
    }
    group.finish();
}

fn bench_verified_state_range_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_verified_state_range_read");
    group.sample_size(10);
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for state_bytes in [1_024, 1_048_576, 10_485_760] {
        let directory = tempfile::tempdir().unwrap();
        let store = store(directory.path());
        let payload = checkpoint_payload(state_bytes);
        let manifest = checkpoint_manifest(1, 5, &payload);
        runtime
            .block_on(store.save_checkpoint(&manifest, std::slice::from_ref(&payload)))
            .unwrap();
        let frame = manifest.state_frames[0].clone();
        let object_length = manifest.node_data.object_length;

        group.throughput(Throughput::Bytes(state_bytes as u64));
        group.bench_function(
            BenchmarkId::new("load", humanize_bytes(state_bytes)),
            |bencher| {
                bencher.iter(|| {
                    let mut payloads = runtime
                        .block_on(store.load_node_data_ranges(
                            frame.chunk,
                            object_length,
                            &[frame.range],
                        ))
                        .unwrap()
                        .unwrap();
                    let bytes = payloads.pop().unwrap();
                    assert_eq!(checkpoint_sha256(&bytes), frame.sha256);
                    black_box(bytes)
                })
            },
        );
    }
    group.finish();
}

fn bench_checkpoint_save(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_one_node_object_save");
    group.sample_size(10);
    let runtime = tokio::runtime::Runtime::new().unwrap();

    for state_bytes in [1_024, 1_048_576, 10_485_760] {
        let directory = tempfile::tempdir().unwrap();
        let store = store(directory.path());
        let payload = checkpoint_payload(state_bytes);
        let mut id = 1_u64;

        group.throughput(Throughput::Bytes(state_bytes as u64));
        group.bench_function(
            BenchmarkId::new("save", humanize_bytes(state_bytes)),
            |bencher| {
                bencher.iter(|| {
                    let manifest = checkpoint_manifest(id, 5, &payload);
                    runtime
                        .block_on(store.save_checkpoint(&manifest, std::slice::from_ref(&payload)))
                        .unwrap();
                    id += 1;
                })
            },
        );
    }
    group.finish();
}

fn subscription_output_batches(rows: usize) -> Vec<RecordBatch> {
    const FRAME_ROWS: usize = 1_024;
    let schema = Arc::new(Schema::new(vec![
        Field::new("account_id", DataType::Int64, false),
        Field::new("aggregate", DataType::Int64, false),
        Field::new("__weight", DataType::Int64, false),
    ]));
    (0..rows)
        .step_by(FRAME_ROWS)
        .map(|start| {
            let end = (start + FRAME_ROWS).min(rows);
            let keys = (start..end).map(|row| (row % 64) as i64);
            let values = (start..end).map(|row| (row / 64) as i64);
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from_iter_values(keys)),
                    Arc::new(Int64Array::from_iter_values(values)),
                    Arc::new(Int64Array::from_value(1, end - start)),
                ],
            )
            .unwrap()
        })
        .collect()
}

fn bench_subscription_output_ipc_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_output_ipc_encode");
    group.sample_size(20);
    let batches = subscription_output_batches(65_536);
    let schema = batches[0].schema();
    let input_bytes = batches
        .iter()
        .map(RecordBatch::get_array_memory_size)
        .sum::<usize>();
    group.throughput(Throughput::Bytes(input_bytes as u64));

    group.bench_function("uncompressed", |bencher| {
        bencher.iter(|| {
            black_box(
                serialize_batches_stream_bounded(schema.as_ref(), &batches, usize::MAX).unwrap(),
            )
        });
    });
    group.bench_function("lz4", |bencher| {
        bencher.iter(|| {
            black_box(
                serialize_batches_stream_lz4_bounded(schema.as_ref(), &batches, usize::MAX)
                    .unwrap(),
            )
        });
    });
    group.finish();
}

fn bench_subscription_output_ipc_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_output_ipc_decode");
    group.sample_size(20);
    let batches = subscription_output_batches(65_536);
    let encoded =
        serialize_batches_stream_lz4_bounded(batches[0].schema().as_ref(), &batches, usize::MAX)
            .unwrap();
    group.throughput(Throughput::Bytes(encoded.len() as u64));

    group.bench_function("lz4", |bencher| {
        bencher.iter(|| {
            let reader =
                StreamReader::try_new(std::io::Cursor::new(encoded.as_slice()), None).unwrap();
            let rows = reader.map(|batch| batch.unwrap().num_rows()).sum::<usize>();
            black_box(rows)
        });
    });
    group.finish();
}

fn bench_partition_sequence_assignment(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_partition_sequence_assignment");
    for assignments in [64_u64, 4_096] {
        group.throughput(Throughput::Elements(assignments));
        group.bench_function(BenchmarkId::from_parameter(assignments), |bencher| {
            bencher.iter(|| {
                let mut sequence = PartitionSequence::FIRST;
                for _ in 0..assignments {
                    sequence = black_box(sequence.checked_next().unwrap());
                }
                black_box(sequence)
            });
        });
    }
    group.finish();
}

fn subscription_manifest_fixture(
    vnode_count: u16,
) -> (CheckpointAssignmentFence, Vec<NodeSubscriptionManifest>) {
    const PARTICIPANT_COUNT: u64 = 3;
    let participants = (1..=PARTICIPANT_COUNT)
        .map(|node_id| CheckpointParticipant {
            node_id,
            boot_incarnation: uuid::Uuid::from_u128(u128::from(node_id)),
        })
        .collect::<Vec<_>>();
    let owners = (0..vnode_count)
        .map(|partition| u64::from(partition) % PARTICIPANT_COUNT + 1)
        .collect::<Vec<_>>();
    let assignment = CheckpointAssignmentFence::from_owner_map(7, &owners, participants).unwrap();
    let certificate = OutputDistributionCertificate {
        version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        stream_id: "positions".into(),
        catalog_generation: 1,
        stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes([1; 32])),
        final_operator_id: "stream:positions".into(),
        distribution: OutputDistribution::VnodePartitioned {
            key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
            partition_abi: PARTITIONING_ABI_VERSION,
            vnode_count,
        },
        schema_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
        changelog_mode: ChangelogMode::WeightedRetractInsert,
        history_retention_bytes: 64 * 1024 * 1024,
        query_fingerprint: SubscriptionDigest::from_bytes([4; 32]),
        pipeline_identity: PipelineIdentity::empty(),
    };
    let manifests = (1..=PARTICIPANT_COUNT)
        .map(|participant_id| {
            let owned_vnodes = owners
                .iter()
                .enumerate()
                .filter(|(_, owner)| **owner == participant_id)
                .map(|(partition, _)| u16::try_from(partition).unwrap())
                .collect::<Vec<_>>();
            let ranges = owned_vnodes
                .iter()
                .map(|partition| NodePartitionRange {
                    partition: OutputPartitionId::new(*partition),
                    first_sequence: PartitionSequence::new(42),
                    through_sequence: PartitionSequence::new(42),
                })
                .collect();
            let mut manifest = NodeSubscriptionManifest {
                protocol_version: SubscriptionProtocolVersion::CURRENT,
                epoch: 10,
                checkpoint_id: 10,
                participant_id,
                assignment_certificate: assignment.clone(),
                streams: vec![NodeSubscriptionStreamManifest {
                    distribution_certificate: certificate.clone(),
                    ranges,
                    segments: Vec::new(),
                }],
                manifest_digest: SubscriptionDigest::from_bytes([0; 32]),
            };
            manifest.seal(&owned_vnodes).unwrap();
            manifest
        })
        .collect();
    (assignment, manifests)
}

fn bench_subscription_manifest_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_manifest_construction");
    group.sample_size(20);
    for vnode_count in [64_u16, 1_024] {
        let (assignment, manifests) = subscription_manifest_fixture(vnode_count);
        let manifests = manifests.iter().collect::<Vec<_>>();
        group.throughput(Throughput::Elements(u64::from(vnode_count)));
        group.bench_function(BenchmarkId::from_parameter(vnode_count), |bencher| {
            bencher.iter(|| {
                black_box(
                    merge_node_subscription_manifests(10, 10, &assignment, &manifests).unwrap(),
                )
            });
        });
    }
    group.finish();
}

fn humanize_bytes(bytes: usize) -> String {
    if bytes >= 1_048_576 {
        format!("{}MB", bytes / 1_048_576)
    } else if bytes >= 1_024 {
        format!("{}KB", bytes / 1_024)
    } else {
        format!("{bytes}B")
    }
}

criterion_group!(
    benches,
    bench_exact_manifest_load,
    bench_verified_state_range_read,
    bench_checkpoint_save,
    bench_subscription_output_ipc_encode,
    bench_subscription_output_ipc_decode,
    bench_partition_sequence_assignment,
    bench_subscription_manifest_construction,
);
criterion_main!(benches);
