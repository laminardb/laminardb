use super::*;
use arrow_array::{ArrayRef, BinaryArray, Int64Array, RecordBatch, UInt32Array, UInt8Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use std::sync::Arc;

use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn test_batch(n: usize) -> RecordBatch {
    #[allow(clippy::cast_possible_wrap)]
    let ids: Vec<i64> = (0..n as i64).collect();
    RecordBatch::try_new(test_schema(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
}

#[test]
fn connector_task_generation_terminates_after_owner_and_every_guard() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let first = owner.track().expect("live generation");
    let second = owner.track().expect("live generation");

    assert!(!tracker.is_terminated());
    drop(owner);
    assert!(!tracker.is_terminated());
    drop(first);
    assert!(!tracker.is_terminated());
    drop(second);
    assert!(tracker.is_terminated());
}

#[test]
fn connector_task_admission_is_sealed_by_owner_drop() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let admission = owner.admission();
    let admission_clone = admission.clone();
    let admitted = admission.track().expect("live generation");

    drop(owner);

    assert!(admission.track().is_none());
    assert!(admission_clone.track().is_none());
    assert!(!tracker.is_terminated());
    drop(admitted);
    assert!(tracker.is_terminated());
}

#[test]
fn connector_task_admission_does_not_retain_generation_state() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let admission = owner.admission();

    drop(owner);
    drop(tracker);

    assert!(admission.inner.upgrade().is_none());
    assert!(admission.track().is_none());
}

#[tokio::test]
async fn connector_task_wait_wakes_every_tracker_clone() {
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().expect("live generation");
    let first = tokio::spawn({
        let tracker = tracker.clone();
        async move { tracker.wait_terminated().await }
    });
    let second = tokio::spawn({
        let tracker = tracker.clone();
        async move { tracker.wait_terminated().await }
    });

    drop(owner);
    assert!(!tracker.is_terminated());
    drop(guard);

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        first.await.expect("first waiter task");
        second.await.expect("second waiter task");
    })
    .await
    .expect("tracker waiters must wake");
    tracker.wait_terminated().await;
}

#[test]
fn test_source_batch() {
    let batch = SourceBatch::new(test_batch(10));
    assert_eq!(batch.num_rows(), 10);
    assert!(batch.row_positions().is_none());
    assert!(batch.mutations().is_none());
}

#[test]
fn source_row_positions_reject_nulls_and_misalignment() {
    let null_partition = BinaryArray::from(vec![Some(&b"p0"[..]), None]);
    let order = BinaryArray::from(vec![&b"0"[..], &b"1"[..]]);
    let sub_offset = UInt32Array::from(vec![0, 0]);
    assert!(
        SourceRowPositions::try_new(null_partition, order.clone(), sub_offset.clone()).is_err()
    );

    let positions = SourceRowPositions::try_new(
        BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
        order,
        sub_offset,
    )
    .unwrap();
    assert!(SourceBatch::positioned(test_batch(1), positions).is_err());
}

#[test]
fn source_batch_validates_and_canonicalizes_mutations() {
    let mixed = SourceBatch::new(test_batch(2))
        .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
        .unwrap();
    assert_eq!(
        mixed.mutations(),
        Some(&[SourceMutation::Put, SourceMutation::Tombstone][..])
    );

    let puts = SourceBatch::new(test_batch(2))
        .with_mutations(vec![SourceMutation::Put; 2])
        .unwrap();
    assert!(puts.mutations().is_none());
    assert!(SourceBatch::new(test_batch(2))
        .with_mutations(vec![SourceMutation::Tombstone])
        .is_err());
}

#[test]
fn source_metadata_round_trip_is_sparse_and_zero_copy() {
    let records = test_batch(2);
    assert!(source_row_positions(&records).unwrap().is_none());
    let positioned_schema = schema_with_source_row_positions(&records.schema()).unwrap();
    let mutation_schema =
        schema_with_source_mutations_and_row_positions(&records.schema()).unwrap();
    let positions = SourceRowPositions::try_new(
        BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
        BinaryArray::from(vec![&b"0"[..], &b"1"[..]]),
        UInt32Array::from(vec![0, 0]),
    )
    .unwrap();
    let encoded = SourceBatch::positioned(records.clone(), positions.clone())
        .unwrap()
        .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned_schema,
            &mutation_schema,
        )
        .unwrap();
    let mutations = source_mutations(&encoded).unwrap().unwrap();
    assert_eq!(mutations.len(), 2);
    assert!(!mutations.is_empty());
    assert_eq!(mutations.get(0), Some(SourceMutation::Put));
    assert_eq!(mutations.get(1), Some(SourceMutation::Tombstone));
    let row_positions = source_row_positions(&encoded).unwrap().unwrap();
    assert_eq!(row_positions.len(), 2);
    assert!(!row_positions.is_empty());
    assert_eq!(
        row_positions.get(1),
        Some(SourceRowPositionRef {
            partition: b"p0",
            order_key: b"1",
            sub_offset: 0,
        })
    );
    assert_eq!(row_positions.get(2), None);
    assert_eq!(
        encoded.schema().field(records.num_columns()).name(),
        SOURCE_MUTATION_COLUMN
    );

    let positioned = strip_source_mutations(&encoded).unwrap();
    assert_eq!(positioned.schema(), positioned_schema);
    assert!(Arc::ptr_eq(positioned.column(0), records.column(0)));

    let routed_put = encoded.slice(0, 1);
    assert!(source_mutations(&routed_put).is_err());
    assert_eq!(
        source_mutations_routed(&routed_put)
            .unwrap()
            .unwrap()
            .get(0),
        Some(SourceMutation::Put)
    );
    let routed_visible = Arc::clone(routed_put.column(0));
    let routed_positioned = strip_source_mutations_routed(&routed_put).unwrap();
    assert!(Arc::ptr_eq(&routed_visible, routed_positioned.column(0)));

    let stripped = strip_source_row_positions(&encoded).unwrap();
    assert_eq!(stripped.schema(), records.schema());
    assert_eq!(stripped.num_rows(), records.num_rows());
    assert!(Arc::ptr_eq(stripped.column(0), records.column(0)));

    let puts = SourceBatch::positioned(records.clone(), positions)
        .unwrap()
        .with_mutations(vec![SourceMutation::Put; 2])
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned_schema,
            &mutation_schema,
        )
        .unwrap();
    assert!(Arc::ptr_eq(&puts.schema(), &positioned_schema));
    assert!(puts.column_by_name(SOURCE_MUTATION_COLUMN).is_none());
    assert!(source_mutations(&puts).unwrap().is_none());
}

#[test]
fn source_metadata_rejects_collisions_and_malformed_batches() {
    let collision = Arc::new(Schema::new(vec![Field::new(
        "__SOURCE_MUTATION",
        DataType::UInt8,
        false,
    )]));
    assert!(schema_with_source_row_positions(&collision).is_err());
    assert!(schema_with_source_mutations_and_row_positions(&collision).is_err());

    let records = test_batch(2);
    let positioned_schema = schema_with_source_row_positions(&records.schema()).unwrap();
    let mutation_schema =
        schema_with_source_mutations_and_row_positions(&records.schema()).unwrap();
    let positions = SourceRowPositions::try_new(
        BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
        BinaryArray::from(vec![&b"0"[..], &b"1"[..]]),
        UInt32Array::from(vec![0, 0]),
    )
    .unwrap();
    let encoded = SourceBatch::positioned(records.clone(), positions)
        .unwrap()
        .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &positioned_schema,
            &mutation_schema,
        )
        .unwrap();
    let mutation_index = records.num_columns();

    let malformed = |field: Field, array: ArrayRef| {
        let mut fields = encoded.schema().fields().to_vec();
        fields[mutation_index] = Arc::new(field);
        let mut columns = encoded.columns().to_vec();
        columns[mutation_index] = array;
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    };
    let wrong_type = malformed(
        Field::new(SOURCE_MUTATION_COLUMN, DataType::Int64, false),
        Arc::new(Int64Array::from(vec![0, 1])),
    );
    assert!(source_mutations(&wrong_type).is_err());

    let null = malformed(
        Field::new(SOURCE_MUTATION_COLUMN, DataType::UInt8, true),
        Arc::new(UInt8Array::from(vec![Some(0), None])),
    );
    assert!(strip_source_mutations(&null).is_err());

    let unknown = malformed(
        Field::new(SOURCE_MUTATION_COLUMN, DataType::UInt8, false),
        Arc::new(UInt8Array::from(vec![0, 2])),
    );
    assert!(source_mutations(&unknown).is_err());
    assert!(strip_source_mutations(&unknown).is_err());

    let all_put = malformed(
        Field::new(SOURCE_MUTATION_COLUMN, DataType::UInt8, false),
        Arc::new(UInt8Array::from(vec![0, 0])),
    );
    assert!(strip_source_mutations(&all_put).is_err());

    let mut fields = encoded.schema().fields().to_vec();
    let mutation_field = fields.remove(mutation_index);
    fields.push(mutation_field);
    let mut columns = encoded.columns().to_vec();
    let mutation_column = columns.remove(mutation_index);
    columns.push(mutation_column);
    let misplaced = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    assert!(source_mutations(&misplaced).is_err());
}

#[test]
fn test_write_result() {
    let result = WriteResult::new(100, 5000);
    assert_eq!(result.records_written, 100);
    assert_eq!(result.bytes_written, 5000);
}

#[test]
fn source_drain_request_requires_canonical_round() {
    let round = laminar_core::checkpoint::AssignmentDrainId {
        predecessor_version: 7,
        target_version: 8,
        digest: [9; 32],
    };
    assert_eq!(SourceDrainRequest::new(round).unwrap().round, round);
    assert!(
        SourceDrainRequest::new(laminar_core::checkpoint::AssignmentDrainId {
            predecessor_version: 8,
            target_version: 8,
            digest: [9; 32],
        })
        .is_err()
    );
}

#[test]
fn source_contract_defaults_fail_closed() {
    let contract = SourceContract::default();
    assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
    assert_eq!(contract.topology, SourceTopology::Singleton);
    assert_eq!(contract.input_mode, SourceInputMode::AppendOnly);
    assert_eq!(
        contract.row_positions,
        SourceRowPositionCapability::Unavailable
    );
    assert!(!contract.supports_replay());
    assert!(!contract.requires_checkpointing());
    assert!(!contract.is_exact_delivery_certified());
}

#[test]
fn commit_coupled_sources_are_replayable_and_require_checkpoints() {
    let contract = SourceContract::new(
        SourceConsistency::CommitCoupled,
        SourceTopology::NodeLocalIngress,
        SourceInputMode::FullChangelog,
    );
    assert!(contract.supports_replay());
    assert!(contract.requires_checkpointing());
    assert_eq!(contract.input_mode, SourceInputMode::FullChangelog);
}

#[test]
fn source_start_rejects_split_and_zero_resume_before_connector_start() {
    use laminar_core::checkpoint::CheckpointAttempt;

    for attempt in [CheckpointAttempt::new(7, 8), CheckpointAttempt::new(0, 0)] {
        let error = SourceStart::new(
            ConnectorConfig::new("test"),
            SourcePosition::Resume {
                attempt,
                checkpoint: SourceCheckpoint::new(),
            },
            DeliveryGuarantee::AtLeastOnce,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ConnectorError::ConfigurationError(message)
                if message.contains("one nonzero canonical checkpoint ID")
        ));
    }
}

#[test]
fn source_start_accepts_initial_and_exposes_validated_parts() {
    let mut config = ConnectorConfig::new("test");
    config.set("endpoint", "local");
    let request = SourceStart::new(
        config,
        SourcePosition::Initial,
        DeliveryGuarantee::BestEffort,
    )
    .unwrap();

    let (config, position, delivery) = request.into_parts();
    assert_eq!(config.get("endpoint"), Some("local"));
    assert!(matches!(position, SourcePosition::Initial));
    assert_eq!(delivery, DeliveryGuarantee::BestEffort);
}

#[test]
fn sink_contract_defaults_fail_closed() {
    let contract = SinkContract::default();
    assert_eq!(contract.consistency, SinkConsistency::Ephemeral);
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert!(!contract.input_mode.accepts_full_changelog());
}

#[test]
fn coordinated_namespace_is_bounded_stable_and_sink_scoped() {
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    const DEPLOYMENT: &str = "018f0000-0000-7000-8000-000000000001";

    let first =
        CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "orders")
            .unwrap();
    let same = CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "orders")
        .unwrap();
    let other = CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "audit")
        .unwrap();
    let other_deployment = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000002",
        "orders",
    )
    .unwrap();

    assert_eq!(first.external_key(), same.external_key());
    assert_ne!(first.external_key(), other.external_key());
    assert_ne!(first.external_key(), other_deployment.external_key());
    assert_eq!(first.external_key().len(), "ldb-c3-".len() + 64);
    assert!(first
        .external_key()
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
}

#[test]
fn coordinated_namespace_rejects_ambiguous_identity() {
    const DEPLOYMENT: &str = "018f0000-0000-7000-8000-000000000001";

    use laminar_core::checkpoint::checkpoint_manifest::{
        PipelineIdentity, PIPELINE_IDENTITY_VERSION,
    };
    let malformed = PipelineIdentity {
        canonical_version: PIPELINE_IDENTITY_VERSION,
        sha256: "NOT-A-DIGEST".into(),
    };
    assert!(CoordinatedCommitNamespace::try_new(malformed, DEPLOYMENT, "orders").is_err());
    assert!(
        CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT, "").is_err()
    );
    assert!(
        CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), "not-a-uuid", "orders")
            .is_err()
    );
}

#[test]
fn coordinated_batch_fingerprint_covers_the_exact_ordered_cut() {
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "orders",
    )
    .unwrap();
    let attempt = CheckpointAttempt::new(8, 108);
    let batch = CoordinatedCommitBatch {
        namespace,
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 107,
            fencing_token: 3,
        },
        fencing_token: 4,
        target: attempt,
        entries: vec![CoordinatedCommitPayload {
            attempt,
            participant_id: 7,
            payload: None,
        }],
    };
    let expected = batch.exact_fingerprint();
    assert_eq!(expected, batch.clone().exact_fingerprint());

    let mut variants = Vec::new();
    let mut variant = batch.clone();
    variant.namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "audit",
    )
    .unwrap();
    variants.push(variant);
    let mut variant = batch.clone();
    variant.expected_predecessor.checkpoint_id -= 1;
    variants.push(variant);
    let mut variant = batch.clone();
    variant.fencing_token += 1;
    variants.push(variant);
    let mut variant = batch.clone();
    variant.target.epoch += 1;
    variants.push(variant);
    let mut variant = batch.clone();
    variant.entries[0].attempt.checkpoint_id += 1;
    variants.push(variant);
    let mut variant = batch.clone();
    variant.entries[0].participant_id += 1;
    variants.push(variant);
    let mut variant = batch;
    variant.entries[0].payload = Some(Vec::new());
    variants.push(variant);

    assert!(variants
        .into_iter()
        .all(|variant| variant.exact_fingerprint() != expected));
}

fn valid_coordinated_batch() -> CoordinatedCommitBatch {
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let target = CheckpointAttempt::canonical(102);
    CoordinatedCommitBatch {
        namespace: CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 101,
            fencing_token: 1,
        },
        fencing_token: 2,
        target,
        entries: vec![CoordinatedCommitPayload {
            attempt: target,
            participant_id: 1,
            payload: None,
        }],
    }
}

#[test]
fn coordinated_batch_rejects_noncanonical_target_before_other_shape_checks() {
    use laminar_core::checkpoint::CheckpointAttempt;

    for target in [
        CheckpointAttempt::new(102, 103),
        CheckpointAttempt::new(0, 0),
    ] {
        let mut batch = valid_coordinated_batch();
        batch.target = target;
        let error = batch.validate_shape().unwrap_err();
        assert!(
            error.contains("target must use one nonzero canonical checkpoint ID"),
            "unexpected validation error: {error}"
        );
    }
}

#[test]
fn coordinated_batch_rejects_noncanonical_entry_before_other_shape_checks() {
    use laminar_core::checkpoint::CheckpointAttempt;

    for attempt in [
        CheckpointAttempt::new(101, 102),
        CheckpointAttempt::new(0, 0),
    ] {
        let mut batch = valid_coordinated_batch();
        batch.entries[0].attempt = attempt;
        let error = batch.validate_shape().unwrap_err();
        assert!(
            error.contains("canonical checkpoint ID"),
            "unexpected validation error: {error}"
        );
    }
    let mut batch = valid_coordinated_batch();
    batch.entries[0].participant_id = 0;
    assert!(batch
        .validate_shape()
        .unwrap_err()
        .contains("nonzero participant"));
}

#[test]
fn coordinated_batch_rejects_cursor_rollback_and_unproven_overlap() {
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let first = CheckpointAttempt::canonical(108);
    let target = CheckpointAttempt::canonical(110);
    let batch = CoordinatedCommitBatch {
        namespace: CoordinatedCommitNamespace::try_new(
            PipelineIdentity::empty(),
            "018f0000-0000-7000-8000-000000000001",
            "orders",
        )
        .unwrap(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 107,
            fencing_token: 3,
        },
        fencing_token: 4,
        target,
        entries: vec![
            CoordinatedCommitPayload {
                attempt: first,
                participant_id: 1,
                payload: None,
            },
            CoordinatedCommitPayload {
                attempt: target,
                participant_id: 1,
                payload: None,
            },
        ],
    };

    let cursor = |checkpoint_id, fencing_token| {
        Some(CoordinatedCommitCursor {
            checkpoint_id,
            fencing_token,
        })
    };
    assert!(batch.validate_observed_cursor(cursor(106, 3)).is_err());
    assert!(batch.validate_observed_cursor(cursor(109, 3)).is_err());
    assert!(batch.validate_observed_cursor(cursor(107, 2)).is_err());
    assert!(batch.validate_observed_cursor(cursor(107, 3)).is_ok());
    assert!(batch.validate_observed_cursor(cursor(108, 3)).is_ok());
    assert!(batch.validate_observed_cursor(cursor(110, 4)).is_ok());
    assert!(batch.validate_observed_cursor(cursor(110, 3)).is_err());
    assert!(batch.validate_observed_cursor(cursor(108, 5)).is_err());
}

#[test]
fn coordinated_batch_requires_unique_canonical_attempt_participants() {
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "orders",
    )
    .unwrap();
    let target = CheckpointAttempt::canonical(102);
    let batch = |entries| CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 100,
            fencing_token: 1,
        },
        fencing_token: 2,
        target,
        entries,
    };
    let payload = |attempt, participant_id| CoordinatedCommitPayload {
        attempt,
        participant_id,
        payload: None,
    };

    let duplicate = batch(vec![payload(target, 1), payload(target, 1)]);
    assert!(duplicate
        .validate_shape()
        .unwrap_err()
        .contains("duplicate"));

    let out_of_order = batch(vec![payload(target, 2), payload(target, 1)]);
    assert!(out_of_order
        .validate_shape()
        .unwrap_err()
        .contains("out-of-order"));

    let noncanonical = batch(vec![
        payload(CheckpointAttempt::new(3, 101), 1),
        payload(target, 2),
    ]);
    assert!(noncanonical
        .validate_shape()
        .unwrap_err()
        .contains("canonical checkpoint ID"));
}

#[test]
fn coordinated_batch_entry_limit_accepts_max_and_rejects_max_plus_one() {
    use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
    use laminar_core::checkpoint::CheckpointAttempt;

    let namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        "018f0000-0000-7000-8000-000000000001",
        "orders",
    )
    .unwrap();
    let target = CheckpointAttempt::canonical(101);
    let make_batch = |count: usize| CoordinatedCommitBatch {
        namespace: namespace.clone(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 1,
        target,
        entries: (1..=count)
            .map(|participant_id| CoordinatedCommitPayload {
                attempt: target,
                participant_id: participant_id as u64,
                payload: None,
            })
            .collect(),
    };

    assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES - 1)
        .validate_shape()
        .is_ok());
    assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES)
        .validate_shape()
        .is_ok());
    assert!(make_batch(MAX_COORDINATED_COMMIT_BATCH_ENTRIES + 1)
        .validate_shape()
        .is_err());
}

struct DefaultPreCommitSink {
    coordinated: bool,
}

#[async_trait]
impl SinkConnector for DefaultPreCommitSink {
    async fn open(&mut self, _config: &ConnectorConfig) -> Result<(), ConnectorError> {
        Ok(())
    }
    async fn write_batch(&mut self, _batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        Ok(WriteResult::new(0, 0))
    }
    fn schema(&self) -> SchemaRef {
        test_schema()
    }
    fn suggested_write_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }
    fn as_coordinated_committer(&self) -> Option<&dyn CoordinatedCommitter> {
        self.coordinated
            .then_some(self as &dyn CoordinatedCommitter)
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

#[async_trait]
impl CoordinatedCommitter for DefaultPreCommitSink {
    async fn commit_aggregated(
        &self,
        _batch: CoordinatedCommitBatch,
        _context: CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    async fn committed_cursor(
        &self,
        _namespace: &CoordinatedCommitNamespace,
    ) -> Result<Option<CoordinatedCommitCursor>, ConnectorError> {
        Ok(None)
    }
}

#[tokio::test]
async fn default_pre_commit_rejects_coordinated_sink() {
    let mut sink = DefaultPreCommitSink { coordinated: true };
    assert!(matches!(
        sink.pre_commit(1).await,
        Err(ConnectorError::ConfigurationError(_))
    ));
}

#[tokio::test]
async fn default_pre_commit_ok_for_non_coordinated_sink() {
    let mut sink = DefaultPreCommitSink { coordinated: false };
    assert!(matches!(sink.pre_commit(1).await, Ok(None)));
}
