use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use laminar_core::checkpoint::{
    ChangelogMode, CheckpointAttempt, CheckpointParticipant, OutputDistribution,
    OutputDistributionCertificate, OutputFrameId, OutputPartitionId, PartitionFrontier,
    PartitionSequence, PipelineIdentity, StreamGeneration, SubscriptionDigest,
    SubscriptionProtocolVersion, OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
use laminar_core::state::PARTITIONING_ABI_VERSION;

use super::super::{ClusterSubscriptionOutputState, OutputWriterAuthority};
use crate::error::DbError;
use crate::subscription::{
    CertifiedSubscriptionFrontiers, ClusterSubscriptionError, PartitionedOutputBatch,
    PreparedSubscriptionOutput,
};

fn batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
}

fn certificate(schema: &Schema) -> Arc<OutputDistributionCertificate> {
    Arc::new(OutputDistributionCertificate {
        version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        stream_id: "positions".into(),
        catalog_generation: 1,
        stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes([1; 32])),
        final_operator_id: "stream:positions".into(),
        distribution: OutputDistribution::VnodePartitioned {
            key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
            partition_abi: PARTITIONING_ABI_VERSION,
            vnode_count: 4,
        },
        schema_fingerprint: crate::pipeline_identity::subscription_schema_fingerprint(schema)
            .unwrap(),
        changelog_mode: ChangelogMode::WeightedRetractInsert,
        history_retention_bytes: 0,
        query_fingerprint: SubscriptionDigest::from_bytes([3; 32]),
        pipeline_identity: PipelineIdentity::empty(),
    })
}

fn authority() -> OutputWriterAuthority {
    OutputWriterAuthority {
        participant: CheckpointParticipant {
            node_id: 1,
            boot_incarnation: uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111")
                .unwrap(),
        },
        process_term: 2,
        assignment_version: 3,
        assignment_digest: [4; 32],
    }
}

fn output(
    certificate: &Arc<OutputDistributionCertificate>,
    sequence: u64,
    values: Vec<i64>,
) -> PreparedSubscriptionOutput {
    PreparedSubscriptionOutput {
        certificate: Arc::clone(certificate),
        frames: vec![PartitionedOutputBatch {
            id: OutputFrameId {
                stream_generation: certificate.stream_generation,
                partition: OutputPartitionId::new(0),
                sequence: PartitionSequence::new(sequence),
            },
            batch: batch(values),
        }],
    }
}

fn capture(
    certificate: &Arc<OutputDistributionCertificate>,
    through_sequence: u64,
) -> Vec<CertifiedSubscriptionFrontiers> {
    vec![CertifiedSubscriptionFrontiers {
        certificate: Arc::clone(certificate),
        frontiers: vec![PartitionFrontier {
            partition: OutputPartitionId::new(0),
            through_sequence: PartitionSequence::new(through_sequence),
        }],
    }]
}

fn capture_partitions(
    certificate: &Arc<OutputDistributionCertificate>,
    partitions: u16,
    through_sequence: u64,
) -> Vec<CertifiedSubscriptionFrontiers> {
    vec![CertifiedSubscriptionFrontiers {
        certificate: Arc::clone(certificate),
        frontiers: (0..partitions)
            .map(|partition| PartitionFrontier {
                partition: OutputPartitionId::new(partition),
                through_sequence: PartitionSequence::new(through_sequence),
            })
            .collect(),
    }]
}

fn partition_burst(
    certificate: &Arc<OutputDistributionCertificate>,
    sequence: u64,
    partitions: u16,
    batch: &RecordBatch,
) -> PreparedSubscriptionOutput {
    let frames = (0..partitions)
        .map(|partition| PartitionedOutputBatch {
            id: OutputFrameId {
                stream_generation: certificate.stream_generation,
                partition: OutputPartitionId::new(partition),
                sequence: PartitionSequence::new(sequence),
            },
            batch: batch.clone(),
        })
        .collect();
    PreparedSubscriptionOutput {
        certificate: Arc::clone(certificate),
        frames,
    }
}

fn sequential_partition_frames(
    certificate: &Arc<OutputDistributionCertificate>,
    first_sequence: u64,
    frame_count: u64,
    batch: &RecordBatch,
) -> PreparedSubscriptionOutput {
    PreparedSubscriptionOutput {
        certificate: Arc::clone(certificate),
        frames: (first_sequence..first_sequence + frame_count)
            .map(|sequence| PartitionedOutputBatch {
                id: OutputFrameId {
                    stream_generation: certificate.stream_generation,
                    partition: OutputPartitionId::new(0),
                    sequence: PartitionSequence::new(sequence),
                },
                batch: batch.clone(),
            })
            .collect(),
    }
}

#[test]
fn cycle_abort_does_not_suppress_the_same_frame() {
    let sample = batch(vec![1]);
    let certificate = certificate(sample.schema().as_ref());
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();

    state
        .stage_cycle(vec![output(&certificate, 0, vec![1])], authority())
        .unwrap();
    state.abort_cycle();
    state
        .stage_cycle(vec![output(&certificate, 0, vec![1])], authority())
        .unwrap();
    state.commit_cycle();

    let attempt = CheckpointAttempt::canonical(1);
    state.reserve_checkpoint(attempt).unwrap();
    let prepared = state
        .prepare_checkpoint(attempt, capture(&certificate, 1))
        .unwrap()
        .unwrap();
    assert_eq!(prepared.streams[0].partitions[0].frames.len(), 1);
    assert_eq!(
        prepared.streams[0].partitions[0].range,
        laminar_core::checkpoint::NodePartitionRange {
            partition: OutputPartitionId::new(0),
            first_sequence: PartitionSequence::FIRST,
            through_sequence: PartitionSequence::new(1),
        }
    );
}

#[test]
fn duplicate_frame_is_idempotent_only_when_the_batch_matches() {
    let sample = batch(vec![1]);
    let certificate = certificate(sample.schema().as_ref());
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();
    state
        .stage_cycle(vec![output(&certificate, 0, vec![1])], authority())
        .unwrap();
    state.commit_cycle();

    state
        .stage_cycle(vec![output(&certificate, 0, vec![1])], authority())
        .unwrap();
    state.commit_cycle();
    let mut same_cycle_duplicate = output(&certificate, 1, vec![2]);
    same_cycle_duplicate.frames.push(PartitionedOutputBatch {
        id: same_cycle_duplicate.frames[0].id,
        batch: batch(vec![2]),
    });
    state
        .stage_cycle(vec![same_cycle_duplicate], authority())
        .unwrap();
    state.commit_cycle();
    assert!(matches!(
        state.stage_cycle(vec![output(&certificate, 0, vec![3])], authority()),
        Err(DbError::Subscription(
            ClusterSubscriptionError::ConflictingDuplicateSequence
        ))
    ));

    let attempt = CheckpointAttempt::canonical(1);
    state.reserve_checkpoint(attempt).unwrap();
    let prepared = state
        .prepare_checkpoint(attempt, capture(&certificate, 2))
        .unwrap()
        .unwrap();
    assert_eq!(prepared.streams[0].partitions[0].frames.len(), 2);
}

#[test]
fn sequence_gap_is_rejected_without_mutating_pending_state() {
    let sample = batch(vec![1]);
    let certificate = certificate(sample.schema().as_ref());
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();
    state
        .stage_cycle(vec![output(&certificate, 0, vec![1])], authority())
        .unwrap();
    state.commit_cycle();
    assert!(matches!(
        state.stage_cycle(vec![output(&certificate, 2, vec![2])], authority()),
        Err(DbError::Subscription(
            ClusterSubscriptionError::PartitionSequenceGap { .. }
        ))
    ));
    state
        .stage_cycle(vec![output(&certificate, 1, vec![2])], authority())
        .unwrap();
}

#[test]
fn checkpoint_abort_restores_pre_cut_frames_before_post_cut_output() {
    let sample = batch(vec![1]);
    let certificate = certificate(sample.schema().as_ref());
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();
    state
        .stage_cycle(vec![output(&certificate, 0, vec![1])], authority())
        .unwrap();
    state.commit_cycle();

    let first_attempt = CheckpointAttempt::canonical(1);
    state.reserve_checkpoint(first_attempt).unwrap();
    let _prepared = state
        .prepare_checkpoint(first_attempt, capture(&certificate, 1))
        .unwrap()
        .unwrap();
    state
        .stage_cycle(vec![output(&certificate, 1, vec![2])], authority())
        .unwrap();
    state.commit_cycle();
    state.abort_checkpoint(first_attempt).unwrap();

    let second_attempt = CheckpointAttempt::canonical(2);
    state.reserve_checkpoint(second_attempt).unwrap();
    let prepared = state
        .prepare_checkpoint(second_attempt, capture(&certificate, 2))
        .unwrap()
        .unwrap();
    let frames = &prepared.streams[0].partitions[0].frames;
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.id.sequence.get())
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn one_stream_can_use_but_not_exceed_the_process_pending_budget() {
    const MIB: usize = 1024 * 1024;
    const PARTITIONS: u16 = 48;
    let shared = batch(vec![7; 3 * MIB / size_of::<i64>()]);
    let mut certificate = (*certificate(shared.schema().as_ref())).clone();
    certificate.distribution = OutputDistribution::VnodePartitioned {
        key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
        partition_abi: PARTITIONING_ABI_VERSION,
        vnode_count: PARTITIONS,
    };
    let certificate = Arc::new(certificate);
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();

    state
        .stage_cycle(
            vec![partition_burst(&certificate, 0, PARTITIONS, &shared)],
            authority(),
        )
        .unwrap();
    state.commit_cycle();
    assert!(state.retained_bytes() > 128 * MIB);
    assert!(state.retained_bytes() < 256 * MIB);
    assert!(state.output_pressure().checkpoint_due());
    assert!(!state.output_pressure().commit_backpressured());

    let error = state
        .stage_cycle(
            vec![partition_burst(&certificate, 1, PARTITIONS, &shared)],
            authority(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("total pending output"));
}

#[test]
fn prepared_cut_applies_backpressure_until_commit_releases_it() {
    const MIB: usize = 1024 * 1024;
    const PARTITIONS: u16 = 48;
    let pre_cut = batch(vec![7; 3 * MIB / size_of::<i64>()]);
    let post_cut = batch(vec![8; 2 * MIB / size_of::<i64>()]);
    let mut certificate = (*certificate(pre_cut.schema().as_ref())).clone();
    certificate.distribution = OutputDistribution::VnodePartitioned {
        key_expressions_fingerprint: SubscriptionDigest::from_bytes([2; 32]),
        partition_abi: PARTITIONING_ABI_VERSION,
        vnode_count: PARTITIONS,
    };
    let certificate = Arc::new(certificate);
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();

    state
        .stage_cycle(
            vec![partition_burst(&certificate, 0, PARTITIONS, &pre_cut)],
            authority(),
        )
        .unwrap();
    state.commit_cycle();
    assert!(state.output_pressure().checkpoint_due());
    let attempt = CheckpointAttempt::canonical(1);
    state.reserve_checkpoint(attempt).unwrap();
    state
        .prepare_checkpoint(attempt, capture_partitions(&certificate, PARTITIONS, 1))
        .unwrap();
    assert!(!state.output_pressure().checkpoint_due());
    assert!(!state.output_pressure().commit_backpressured());

    state
        .stage_cycle(
            vec![partition_burst(&certificate, 1, PARTITIONS, &post_cut)],
            authority(),
        )
        .unwrap();
    state.commit_cycle();
    assert!(!state.output_pressure().checkpoint_due());
    assert!(state.output_pressure().commit_backpressured());

    state.commit_checkpoint(attempt).unwrap();
    assert!(!state.output_pressure().checkpoint_due());
    assert!(!state.output_pressure().commit_backpressured());
    assert!(state.retained_bytes() < 128 * MIB);
}

#[test]
fn prepared_and_open_bytes_share_the_partition_bound() {
    const MIB: usize = 1024 * 1024;
    let shared = batch(vec![7; 3 * MIB / size_of::<i64>()]);
    let certificate = certificate(shared.schema().as_ref());
    let mut state =
        ClusterSubscriptionOutputState::new(vec![Arc::clone(&certificate)], None).unwrap();

    state
        .stage_cycle(
            vec![sequential_partition_frames(&certificate, 0, 8, &shared)],
            authority(),
        )
        .unwrap();
    state.commit_cycle();
    assert!(state.output_pressure().checkpoint_due());
    let attempt = CheckpointAttempt::canonical(1);
    state.reserve_checkpoint(attempt).unwrap();
    state
        .prepare_checkpoint(attempt, capture(&certificate, 8))
        .unwrap();
    assert!(state.output_pressure().commit_backpressured());

    let error = state
        .stage_cycle(
            vec![sequential_partition_frames(&certificate, 8, 3, &shared)],
            authority(),
        )
        .unwrap_err();
    assert!(error.to_string().contains("pending partition output"));

    state.abort_checkpoint(attempt).unwrap();
    assert!(state.output_pressure().checkpoint_due());
    assert!(!state.output_pressure().commit_backpressured());
}
