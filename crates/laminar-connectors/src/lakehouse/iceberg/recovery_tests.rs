use std::sync::Arc;
use std::time::Duration;

use iceberg::spec::{FormatVersion, PrimitiveType, Type};
use iceberg::transaction::{AddColumn, ApplyTransactionAction, Transaction};
use iceberg::TableCreation;
use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
use laminar_core::checkpoint::CheckpointAttempt;
use tokio_stream::StreamExt;

use crate::config::ConnectorState;
use crate::connector::{
    CoordinatedAbortBatch, CoordinatedAbortDescriptor, CoordinatedAbortEntry,
    CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, CoordinatedCommitPayload, CoordinatedCommitter, DeliveryGuarantee,
    SinkConnector, SinkRuntimeContext,
};
use crate::error::ConnectorError;

use super::fault_injection::{scope, IcebergFault, IcebergFaultPoint};
use super::test_support::{
    append_rows, batch as record_batch, create_test_table, table_ident, TestTable,
};
use super::IcebergSink;

const DEPLOYMENT_ID: &str = "018f0000-0000-7000-8000-000000000001";
const SINK_ID: &str = "orders";

fn namespace() -> CoordinatedCommitNamespace {
    CoordinatedCommitNamespace::try_new(PipelineIdentity::empty(), DEPLOYMENT_ID, SINK_ID).unwrap()
}

fn commit_batch(checkpoint_id: u64, descriptor: Vec<u8>) -> CoordinatedCommitBatch {
    let target = CheckpointAttempt::canonical(checkpoint_id);
    CoordinatedCommitBatch {
        namespace: namespace(),
        expected_predecessor: CoordinatedCommitCursor {
            checkpoint_id: 0,
            fencing_token: 0,
        },
        fencing_token: 7,
        target,
        entries: vec![CoordinatedCommitPayload {
            attempt: target,
            participant_id: 1,
            payload: Some(descriptor),
        }],
    }
}

fn abort_batch(checkpoint_id: u64, descriptor: Vec<u8>) -> CoordinatedAbortBatch {
    let target = CheckpointAttempt::canonical(checkpoint_id);
    CoordinatedAbortBatch {
        namespace: namespace(),
        fencing_token: 7,
        target,
        entries: vec![CoordinatedAbortEntry {
            attempt: target,
            participant_id: 1,
            descriptor: CoordinatedAbortDescriptor::Prepared(Some(descriptor)),
            artifact_intent: None,
        }],
    }
}

fn open_abort_batch(checkpoint_id: u64, artifact_intent: Option<Vec<u8>>) -> CoordinatedAbortBatch {
    let target = CheckpointAttempt::canonical(checkpoint_id);
    CoordinatedAbortBatch {
        namespace: namespace(),
        fencing_token: 7,
        target,
        entries: vec![CoordinatedAbortEntry {
            attempt: target,
            participant_id: 1,
            descriptor: CoordinatedAbortDescriptor::Open,
            artifact_intent,
        }],
    }
}

fn configured_sink(
    fixture: &TestTable,
    table: iceberg::table::Table,
    delivery_guarantee: DeliveryGuarantee,
) -> IcebergSink {
    let mut config = fixture.config.clone();
    config.delivery_guarantee = delivery_guarantee;
    let mut sink = IcebergSink::new(config, None);
    if delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
        sink.bind_runtime_context(SinkRuntimeContext {
            deployment_id: DEPLOYMENT_ID.into(),
            sink_id: SINK_ID.into(),
            participant_id: 1,
        })
        .unwrap();
    }
    let schema =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&table.current_schema_ref()).unwrap());
    sink.schema = Some(Arc::clone(&schema));
    sink.alignment_plan = Some(
        super::schema_alignment::SchemaAlignmentPlan::new(
            table.metadata().current_schema_id(),
            Arc::clone(&schema),
            Arc::clone(&schema),
        )
        .unwrap(),
    );
    sink.iceberg_arrow_schema = Some(schema);
    sink.catalog = Some(Arc::clone(&fixture.catalog));
    sink.table = Some(table);
    sink.state = ConnectorState::Running;
    sink
}

fn coordinated_sink(fixture: &TestTable, table: iceberg::table::Table) -> IcebergSink {
    configured_sink(fixture, table, DeliveryGuarantee::ExactlyOnce)
}

async fn admit_epoch(sink: &mut IcebergSink, epoch: u64) -> Vec<u8> {
    let intent = sink
        .checkpoint_artifact_intent(epoch)
        .await
        .unwrap()
        .expect("coordinated Iceberg epochs require cleanup intent");
    sink.begin_epoch(epoch).await.unwrap();
    intent
}

async fn descriptor_with_intent(
    sink: &mut IcebergSink,
    fixture: &TestTable,
    epoch: u64,
) -> (Vec<u8>, Vec<u8>) {
    let intent = admit_epoch(sink, epoch).await;
    sink.write_batch(&record_batch(
        &fixture.table,
        &[(1, Some("a")), (2, Some("b"))],
    ))
    .await
    .unwrap();
    let descriptor = sink
        .pre_commit(epoch)
        .await
        .unwrap()
        .expect("non-empty epoch must produce a descriptor");
    (intent, descriptor)
}

async fn descriptor(sink: &mut IcebergSink, fixture: &TestTable, epoch: u64) -> Vec<u8> {
    descriptor_with_intent(sink, fixture, epoch).await.1
}

async fn table_row_count(table: &iceberg::table::Table) -> usize {
    let stream = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap();
    let mut stream = std::pin::pin!(stream);
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    rows
}

async fn add_optional_column(table: &iceberg::table::Table, fixture: &TestTable) {
    let transaction = Transaction::new(table);
    let transaction = transaction
        .update_schema()
        .add_column(AddColumn::optional(
            "later",
            Type::Primitive(PrimitiveType::String),
        ))
        .apply(transaction)
        .unwrap();
    transaction.commit(fixture.catalog.as_ref()).await.unwrap();
}

fn commit_context() -> CoordinatedCommitContext {
    CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10))
}

async fn cleanup_aborted(
    sink: &IcebergSink,
    batch: CoordinatedAbortBatch,
) -> Result<(), ConnectorError> {
    sink.coordinated_abort_cleaner()
        .expect("coordinated Iceberg sink must expose its detached abort cleaner")
        .cleanup_aborted(batch, commit_context())
        .await
}

#[tokio::test]
async fn epoch_artifact_intent_is_deterministic_and_debug_redacted() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());

    let first = sink.checkpoint_artifact_intent(1).await.unwrap().unwrap();
    let replay = sink.checkpoint_artifact_intent(1).await.unwrap().unwrap();

    assert_eq!(first, replay);
    let intent = super::epoch_intent::IcebergEpochIntentV1::decode(&first).unwrap();
    let debug = format!("{intent:?}");
    assert!(debug.contains("epoch_id"));
    assert!(!debug.contains(fixture.table.metadata().location()));
}

#[tokio::test]
async fn empty_coordinated_epoch_does_not_open_a_writer() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());

    let _ = admit_epoch(&mut sink, 1).await;
    assert_eq!(sink.active_epoch_id, Some(1));
    assert!(sink.active_epoch.get_mut().is_none());
    assert_eq!(sink.pre_commit(1).await.unwrap(), None);
    assert!(sink.active_epoch_id.is_none());
    assert!(sink.active_epoch.get_mut().is_none());
}

#[tokio::test]
async fn durable_abort_deletes_only_the_prepared_epochs_exact_artifacts() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let _ = descriptor(&mut sink, &fixture, 1).await;
    let artifacts = sink
        .prepared_epoch
        .as_ref()
        .expect("non-empty pre-commit must retain exact artifact ownership")
        .artifacts();
    assert_eq!(artifacts.generated_paths().len(), 1);
    assert_eq!(artifacts.created_final_paths().len(), 1);
    assert_eq!(
        sink.metrics.pending_artifact_paths.get(),
        i64::try_from(artifacts.path_count()).unwrap()
    );
    for path in artifacts.created_final_paths() {
        assert!(fixture.table.file_io().exists(path).await.unwrap());
    }

    sink.rollback_epoch(1).await.unwrap();

    assert!(sink.prepared_epoch.is_none());
    assert_eq!(sink.metrics.pending_artifact_paths.get(), 0);
    for path in artifacts
        .generated_paths()
        .iter()
        .chain(artifacts.created_final_paths())
    {
        assert!(!fixture.table.file_io().exists(path).await.unwrap());
    }
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 0);
}

#[tokio::test]
async fn restarted_abort_cleanup_uses_the_durable_descriptor_and_is_idempotent() {
    let fixture = create_test_table(false).await;
    let mut writer = coordinated_sink(&fixture, fixture.table.clone());
    let payload = descriptor(&mut writer, &fixture, 1).await;
    let final_path = super::descriptor::IcebergCommitDescriptorV1::decode(&payload)
        .unwrap()
        .files[0]
        .path
        .clone();
    writer.close().await.unwrap();
    assert!(fixture.table.file_io().exists(&final_path).await.unwrap());

    let current = fixture.catalog.load_table(&table_ident()).await.unwrap();
    let restarted = coordinated_sink(&fixture, current);
    cleanup_aborted(&restarted, abort_batch(1, payload.clone()))
        .await
        .unwrap();
    cleanup_aborted(&restarted, abort_batch(1, payload))
        .await
        .unwrap();

    assert!(!fixture.table.file_io().exists(&final_path).await.unwrap());
    assert_eq!(restarted.metrics.artifact_delete_successes.get(), 1);
    assert_eq!(restarted.metrics.artifact_cleanup_failures.get(), 0);
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 0);
}

#[tokio::test]
async fn abort_cleanup_never_deletes_a_file_with_published_checkpoint_evidence() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let (intent, payload) = descriptor_with_intent(&mut sink, &fixture, 1).await;
    let final_path = super::descriptor::IcebergCommitDescriptorV1::decode(&payload)
        .unwrap()
        .files[0]
        .path
        .clone();
    sink.commit_aggregated(commit_batch(1, payload.clone()), commit_context())
        .await
        .unwrap();

    let error = cleanup_aborted(&sink, open_abort_batch(1, Some(intent)))
        .await
        .expect_err("published checkpoint evidence must fence cleanup");

    assert!(error.is_outcome_unknown());
    assert!(error
        .to_string()
        .contains("LDB-ICEBERG-ABORT-CLEANUP-PUBLISHED"));
    assert!(fixture.table.file_io().exists(&final_path).await.unwrap());
    assert_eq!(sink.metrics.artifact_delete_successes.get(), 0);
    assert_eq!(sink.metrics.unknown_outcomes.get(), 1);
}

#[tokio::test]
async fn successor_epoch_cleans_staging_but_retains_committed_data_files() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let payload = descriptor(&mut sink, &fixture, 1).await;
    let artifacts = sink.prepared_epoch.as_ref().unwrap().artifacts();
    let staging_path = artifacts.generated_paths()[0].clone();
    let final_path = artifacts.created_final_paths()[0].clone();
    fixture
        .table
        .file_io()
        .new_output(&staging_path)
        .unwrap()
        .write(bytes::Bytes::from_static(b"unreferenced-staging-retry"))
        .await
        .unwrap();
    sink.commit_aggregated(commit_batch(1, payload), commit_context())
        .await
        .unwrap();
    assert!(fixture.table.file_io().exists(&staging_path).await.unwrap());
    assert!(fixture.table.file_io().exists(&final_path).await.unwrap());

    let _ = admit_epoch(&mut sink, 2).await;

    assert!(sink.prepared_epoch.is_none());
    assert!(!fixture.table.file_io().exists(&staging_path).await.unwrap());
    assert!(fixture.table.file_io().exists(&final_path).await.unwrap());
    assert_eq!(sink.metrics.pending_artifact_paths.get(), 0);
    sink.rollback_epoch(2).await.unwrap();
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(table_row_count(&table).await, 2);
}

#[tokio::test]
async fn close_deletes_final_files_when_precommit_never_issued_a_descriptor() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let _ = admit_epoch(&mut sink, 1).await;
    sink.write_batch(&record_batch(&fixture.table, &[(1, Some("a"))]))
        .await
        .unwrap();
    let error = scope(
        [IcebergFault::first(IcebergFaultPoint::AfterFileClose)],
        sink.pre_commit(1),
    )
    .await
    .expect_err("the descriptor must not escape the injected boundary");
    assert!(error.to_string().contains("LDB-ICEBERG-FAULT-INJECTION"));
    let artifacts = sink.prepared_epoch.as_ref().unwrap().artifacts();
    assert_eq!(artifacts.created_final_paths().len(), 1);
    assert!(fixture
        .table
        .file_io()
        .exists(&artifacts.created_final_paths()[0])
        .await
        .unwrap());

    sink.close().await.unwrap();

    for path in artifacts
        .generated_paths()
        .iter()
        .chain(artifacts.created_final_paths())
    {
        assert!(!fixture.table.file_io().exists(path).await.unwrap());
    }
}

#[tokio::test]
async fn close_retains_final_files_after_descriptor_issuance() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let _ = descriptor(&mut sink, &fixture, 1).await;
    let artifacts = sink.prepared_epoch.as_ref().unwrap().artifacts();
    assert_eq!(artifacts.created_final_paths().len(), 1);

    sink.close().await.unwrap();

    assert!(fixture
        .table
        .file_io()
        .exists(&artifacts.created_final_paths()[0])
        .await
        .unwrap());
    for path in artifacts.generated_paths() {
        assert!(!fixture.table.file_io().exists(path).await.unwrap());
    }
}

#[tokio::test]
async fn first_write_refreshes_the_descriptor_base_snapshot() {
    let fixture = create_test_table(false).await;
    let stale = fixture.table.clone();
    let (current, _) = append_rows(&fixture, &stale, 90, &[(90, Some("external"))]).await;
    let mut sink = coordinated_sink(&fixture, stale);

    let payload = descriptor(&mut sink, &fixture, 1).await;
    let descriptor = super::descriptor::IcebergCommitDescriptorV1::decode(&payload).unwrap();
    assert_eq!(
        descriptor.table.base_snapshot_id,
        current.metadata().current_snapshot_id()
    );
    assert_eq!(
        descriptor.table.metadata_location,
        current.metadata_location().unwrap()
    );
}

#[tokio::test]
async fn table_replacement_fails_before_epoch_writer_creation() {
    let fixture = create_test_table(false).await;
    let stale = fixture.table.clone();
    let stale_uuid = stale.metadata().uuid();
    let mut sink = coordinated_sink(&fixture, stale.clone());
    let ident = table_ident();
    fixture.catalog.drop_table(&ident).await.unwrap();
    let replacement = fixture
        .catalog
        .create_table(
            &ident.namespace,
            TableCreation::builder()
                .name(ident.name)
                .schema(stale.current_schema_ref().as_ref().clone())
                .format_version(FormatVersion::V2)
                .build(),
        )
        .await
        .unwrap();
    assert_ne!(replacement.metadata().uuid(), stale_uuid);

    let _ = admit_epoch(&mut sink, 1).await;
    let error = sink
        .write_batch(&record_batch(&stale, &[(1, Some("a"))]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("LDB-ICEBERG-TABLE-REPLACED"));
    assert!(sink.active_epoch.get_mut().is_none());
    assert_eq!(replacement.metadata().snapshots().count(), 0);
    sink.rollback_epoch(1).await.unwrap();
}

#[tokio::test]
async fn strict_schema_change_fails_before_epoch_writer_creation() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    add_optional_column(&fixture.table, &fixture).await;

    let _ = admit_epoch(&mut sink, 1).await;
    let error = sink
        .write_batch(&record_batch(&fixture.table, &[(1, Some("a"))]))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("LDB-ICEBERG-SCHEMA-CHANGED"));
    assert!(sink.active_epoch.get_mut().is_none());
    let current = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(current.metadata().snapshots().count(), 0);
    sink.rollback_epoch(1).await.unwrap();
}

#[tokio::test]
async fn direct_flush_rejects_schema_change_without_creating_a_snapshot() {
    let fixture = create_test_table(false).await;
    let mut sink = configured_sink(
        &fixture,
        fixture.table.clone(),
        DeliveryGuarantee::AtLeastOnce,
    );
    sink.write_batch(&record_batch(&fixture.table, &[(1, Some("a"))]))
        .await
        .unwrap();
    let artifacts = sink
        .active_epoch
        .get_mut()
        .as_ref()
        .unwrap()
        .artifact_tracker();
    add_optional_column(&fixture.table, &fixture).await;

    let error = sink.flush().await.unwrap_err();
    assert!(error.to_string().contains("LDB-ICEBERG-SCHEMA-CHANGED"));
    assert_eq!(sink.state, ConnectorState::Failed);
    assert!(sink.active_epoch.get_mut().is_none());
    assert!(sink.active_epoch_id.is_none());
    assert!(matches!(
        sink.write_batch(&record_batch(&fixture.table, &[(2, Some("b"))]))
            .await,
        Err(ConnectorError::InvalidState { .. })
    ));
    let current = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(current.metadata().snapshots().count(), 0);
    for path in artifacts.snapshot().generated_paths() {
        assert!(!fixture.table.file_io().exists(path).await.unwrap());
    }
}

#[tokio::test]
async fn direct_flush_rebases_over_a_compatible_external_append() {
    let fixture = create_test_table(false).await;
    let mut sink = configured_sink(
        &fixture,
        fixture.table.clone(),
        DeliveryGuarantee::AtLeastOnce,
    );
    sink.write_batch(&record_batch(&fixture.table, &[(1, Some("sink"))]))
        .await
        .unwrap();
    let artifacts = sink
        .active_epoch
        .get_mut()
        .as_ref()
        .unwrap()
        .artifact_tracker();
    let _ = append_rows(&fixture, &fixture.table, 90, &[(2, Some("external"))]).await;

    sink.flush().await.unwrap();
    let current = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(current.metadata().snapshots().count(), 2);
    assert_eq!(table_row_count(&current).await, 2);
    for path in artifacts.snapshot().generated_paths() {
        assert!(fixture.table.file_io().exists(path).await.unwrap());
    }
}

#[tokio::test]
async fn direct_flush_retries_a_definite_catalog_conflict() {
    let fixture = create_test_table(false).await;
    let mut sink = configured_sink(
        &fixture,
        fixture.table.clone(),
        DeliveryGuarantee::AtLeastOnce,
    );
    sink.write_batch(&record_batch(&fixture.table, &[(1, Some("sink"))]))
        .await
        .unwrap();

    scope(
        [IcebergFault::first(
            IcebergFaultPoint::CatalogCommitConflict,
        )],
        sink.flush(),
    )
    .await
    .unwrap();

    assert_eq!(sink.state, ConnectorState::Running);
    assert_eq!(sink.metrics.commit_conflicts.get(), 1);
    assert_eq!(sink.metrics.commit_retries.get(), 1);
    assert_eq!(sink.metrics.unknown_outcomes.get(), 0);
    let current = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(current.metadata().snapshots().count(), 1);
    assert_eq!(table_row_count(&current).await, 1);
}

#[tokio::test]
async fn direct_response_loss_is_not_retried_or_allowed_to_advance() {
    let fixture = create_test_table(false).await;
    let mut sink = configured_sink(
        &fixture,
        fixture.table.clone(),
        DeliveryGuarantee::AtLeastOnce,
    );
    sink.write_batch(&record_batch(&fixture.table, &[(1, Some("sink"))]))
        .await
        .unwrap();
    let artifacts = sink
        .active_epoch
        .get_mut()
        .as_ref()
        .unwrap()
        .artifact_tracker();

    let error = scope(
        [IcebergFault::first(IcebergFaultPoint::AfterCatalogCommit)],
        sink.flush(),
    )
    .await
    .expect_err("response loss after dispatch must retain an unknown outcome");
    assert!(error.is_outcome_unknown());
    assert_eq!(sink.state, ConnectorState::Failed);
    assert_eq!(sink.metrics.commit_retries.get(), 0);
    assert_eq!(sink.metrics.unknown_outcomes.get(), 1);
    assert!(matches!(
        sink.flush().await,
        Err(ConnectorError::InvalidState { .. })
    ));

    let current = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(current.metadata().snapshots().count(), 1);
    assert_eq!(table_row_count(&current).await, 1);
    for path in artifacts.snapshot().generated_paths() {
        assert!(fixture.table.file_io().exists(path).await.unwrap());
    }
}

#[tokio::test]
async fn precommit_fault_boundaries_leave_no_snapshot_and_restart_replays_safely() {
    for point in [
        IcebergFaultPoint::BeforeFileClose,
        IcebergFaultPoint::AfterFileClose,
        IcebergFaultPoint::AfterDescriptor,
    ] {
        let fixture = create_test_table(false).await;
        let mut failed = coordinated_sink(&fixture, fixture.table.clone());
        let _ = admit_epoch(&mut failed, 1).await;
        failed
            .write_batch(&record_batch(
                &fixture.table,
                &[(1, Some("a")), (2, Some("b"))],
            ))
            .await
            .unwrap();
        let error = scope([IcebergFault::first(point)], failed.pre_commit(1))
            .await
            .expect_err("fault must interrupt pre-commit");
        assert!(error.to_string().contains("LDB-ICEBERG-FAULT-INJECTION"));
        assert!(failed.prepared_epoch.is_some());
        failed.rollback_epoch(1).await.unwrap();
        assert!(failed.prepared_epoch.is_none());
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0, "point={point:?}");

        let mut restarted = coordinated_sink(&fixture, table);
        let descriptor = descriptor(&mut restarted, &fixture, 1).await;
        restarted
            .commit_aggregated(commit_batch(1, descriptor), commit_context())
            .await
            .unwrap();
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 1, "point={point:?}");
        assert_eq!(table_row_count(&table).await, 2, "point={point:?}");
    }
}

#[tokio::test]
async fn durable_intent_cleans_pre_descriptor_files_after_process_loss() {
    for point in [
        IcebergFaultPoint::BeforeFileClose,
        IcebergFaultPoint::AfterFileClose,
        IcebergFaultPoint::AfterDescriptor,
    ] {
        let fixture = create_test_table(false).await;
        let mut failed = coordinated_sink(&fixture, fixture.table.clone());
        let intent = admit_epoch(&mut failed, 1).await;
        failed
            .write_batch(&record_batch(
                &fixture.table,
                &[(1, Some("a")), (2, Some("b"))],
            ))
            .await
            .unwrap();
        scope([IcebergFault::first(point)], failed.pre_commit(1))
            .await
            .expect_err("fault must prevent descriptor durability");
        let artifacts = failed.prepared_epoch.as_ref().unwrap().artifacts();
        let paths = artifacts
            .generated_paths()
            .iter()
            .chain(artifacts.created_final_paths())
            .cloned()
            .collect::<Vec<_>>();
        let admitted = super::epoch_intent::IcebergEpochIntentV1::decode(&intent).unwrap();
        assert!(!paths.is_empty());
        assert!(paths.iter().all(|path| {
            path.strip_prefix(admitted.attempt_root())
                .is_some_and(|suffix| suffix.starts_with('/'))
        }));
        let mut created = false;
        for path in &paths {
            created |= fixture.table.file_io().exists(path).await.unwrap();
        }
        if point != IcebergFaultPoint::BeforeFileClose {
            assert!(
                created,
                "fault boundary {point:?} created no recoverable file"
            );
        }
        drop(failed);

        let restarted = coordinated_sink(&fixture, fixture.table.clone());
        cleanup_aborted(&restarted, open_abort_batch(1, Some(intent)))
            .await
            .unwrap();
        for path in &paths {
            assert!(!fixture.table.file_io().exists(path).await.unwrap());
        }
        let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
        assert_eq!(table.metadata().snapshots().count(), 0);
    }
}

#[tokio::test]
async fn open_abort_without_durable_intent_fails_before_file_deletion() {
    let fixture = create_test_table(false).await;
    let mut failed = coordinated_sink(&fixture, fixture.table.clone());
    let intent = admit_epoch(&mut failed, 1).await;
    failed
        .write_batch(&record_batch(&fixture.table, &[(1, Some("a"))]))
        .await
        .unwrap();
    scope(
        [IcebergFault::first(IcebergFaultPoint::AfterFileClose)],
        failed.pre_commit(1),
    )
    .await
    .expect_err("fault must prevent descriptor durability");
    let paths = failed
        .prepared_epoch
        .as_ref()
        .unwrap()
        .artifacts()
        .created_final_paths()
        .to_vec();
    assert_eq!(paths.len(), 1);
    drop(failed);

    let restarted = coordinated_sink(&fixture, fixture.table.clone());
    let error = cleanup_aborted(&restarted, open_abort_batch(1, None))
        .await
        .expect_err("open cleanup without durable intent must fail closed");
    assert!(error
        .to_string()
        .contains("LDB-ICEBERG-EPOCH-INTENT-MISSING"));
    assert!(fixture.table.file_io().exists(&paths[0]).await.unwrap());

    cleanup_aborted(&restarted, open_abort_batch(1, Some(intent)))
        .await
        .unwrap();
    assert!(!fixture.table.file_io().exists(&paths[0]).await.unwrap());
}

#[tokio::test]
async fn partial_partition_write_cannot_be_published_by_direct_flush() {
    let fixture = create_test_table(true).await;
    let mut sink = configured_sink(
        &fixture,
        fixture.table.clone(),
        DeliveryGuarantee::AtLeastOnce,
    );
    let error = scope(
        [IcebergFault::on_occurrence(
            IcebergFaultPoint::BeforePartitionWrite,
            2,
        )],
        sink.write_batch(&record_batch(
            &fixture.table,
            &[(1, Some("a")), (2, Some("b"))],
        )),
    )
    .await
    .expect_err("the second partition write must fail after the first mutates its writer");
    assert!(error.to_string().contains("LDB-ICEBERG-FAULT-INJECTION"));

    let retry = sink
        .write_batch(&record_batch(&fixture.table, &[(3, Some("c"))]))
        .await
        .expect_err("a partially written epoch must remain poisoned");
    assert!(retry.to_string().contains("LDB-ICEBERG-EPOCH-POISONED"));
    let flush = sink
        .flush()
        .await
        .expect_err("direct flush must not publish a partially written epoch");
    assert!(flush.to_string().contains("LDB-ICEBERG-EPOCH-POISONED"));

    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 0);
}

#[tokio::test]
async fn failure_before_catalog_dispatch_is_not_published_and_can_retry() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let batch = commit_batch(1, descriptor(&mut sink, &fixture, 1).await);
    let error = scope(
        [IcebergFault::first(IcebergFaultPoint::BeforeCatalogCommit)],
        sink.commit_aggregated(batch.clone(), commit_context()),
    )
    .await
    .expect_err("pre-dispatch fault must fail the publication");
    assert!(!error.is_outcome_unknown());
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 0);

    sink.commit_aggregated(batch.clone(), commit_context())
        .await
        .unwrap();
    sink.commit_aggregated(batch, commit_context())
        .await
        .unwrap();
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(table_row_count(&table).await, 2);
}

#[tokio::test]
async fn unknown_refresh_and_cursor_faults_block_until_exact_reconciliation() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let batch = commit_batch(1, descriptor(&mut sink, &fixture, 1).await);
    let error = scope(
        [
            IcebergFault::first(IcebergFaultPoint::AfterCatalogCommit),
            IcebergFault::on_occurrence(IcebergFaultPoint::DuringMetadataRefresh, 2),
        ],
        sink.commit_aggregated(batch.clone(), commit_context()),
    )
    .await
    .expect_err("failed refresh must preserve the unknown outcome");
    assert!(error.is_outcome_unknown());
    assert!(sink.unresolved_publication.lock().is_some());
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);

    let error = scope(
        [IcebergFault::first(
            IcebergFaultPoint::DuringCommittedCursor,
        )],
        sink.committed_cursor(&namespace()),
    )
    .await
    .expect_err("cursor inspection fault must retain the unknown outcome");
    assert!(error.is_outcome_unknown());
    assert!(sink.unresolved_publication.lock().is_some());

    assert_eq!(
        sink.committed_cursor(&namespace()).await.unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 1,
            fencing_token: 7,
        })
    );
    assert!(sink.unresolved_publication.lock().is_none());

    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    let restarted = coordinated_sink(&fixture, table);
    restarted
        .commit_aggregated(batch, commit_context())
        .await
        .unwrap();
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(table_row_count(&table).await, 2);
}

#[tokio::test]
async fn unknown_publication_blocks_artifact_deletion_until_reconciled() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let batch = commit_batch(1, descriptor(&mut sink, &fixture, 1).await);
    let final_paths = sink
        .prepared_epoch
        .as_ref()
        .unwrap()
        .artifacts()
        .created_final_paths()
        .to_vec();
    let error = scope(
        [
            IcebergFault::first(IcebergFaultPoint::AfterCatalogCommit),
            IcebergFault::on_occurrence(IcebergFaultPoint::DuringMetadataRefresh, 2),
        ],
        sink.commit_aggregated(batch, commit_context()),
    )
    .await
    .expect_err("lost commit response must remain ambiguous");
    assert!(error.is_outcome_unknown());

    let rollback = sink
        .rollback_epoch(1)
        .await
        .expect_err("ambiguous publication must fence artifact deletion");
    assert!(rollback.to_string().contains("remains unresolved"));
    assert!(sink.prepared_epoch.is_some());
    for path in &final_paths {
        assert!(fixture.table.file_io().exists(path).await.unwrap());
    }

    assert_eq!(
        sink.committed_cursor(&namespace()).await.unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 1,
            fencing_token: 7,
        })
    );
    let _ = admit_epoch(&mut sink, 2).await;
    for path in &final_paths {
        assert!(fixture.table.file_io().exists(path).await.unwrap());
    }
    sink.rollback_epoch(2).await.unwrap();
}

#[tokio::test]
async fn unresolved_publication_rejects_a_foreign_namespace_before_catalog_io() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let batch = commit_batch(1, descriptor(&mut sink, &fixture, 1).await);
    let pending = super::publication::unresolved_publication(&sink.config, &batch).unwrap();
    *sink.unresolved_publication.lock() = Some(pending);
    let foreign_namespace = CoordinatedCommitNamespace::try_new(
        PipelineIdentity::empty(),
        DEPLOYMENT_ID,
        "foreign-sink",
    )
    .unwrap();

    scope(
        [IcebergFault::first(
            IcebergFaultPoint::DuringMetadataRefresh,
        )],
        async {
            let error = sink
                .committed_cursor(&foreign_namespace)
                .await
                .expect_err("a foreign namespace must not inspect the unresolved publication");
            assert!(error.is_outcome_unknown());
            assert!(error
                .to_string()
                .contains("LDB-ICEBERG-UNRESOLVED-NAMESPACE"));

            let error = sink
                .committed_cursor(&namespace())
                .await
                .expect_err("the unconsumed catalog fault proves the first call did no I/O");
            assert!(error.to_string().contains("metadata refresh failure"));
        },
    )
    .await;
    assert!(sink.unresolved_publication.lock().is_some());
}

#[tokio::test]
async fn failed_post_commit_file_set_verification_retains_the_recovery_fence() {
    let fixture = create_test_table(false).await;
    let mut sink = coordinated_sink(&fixture, fixture.table.clone());
    let batch = commit_batch(1, descriptor(&mut sink, &fixture, 1).await);
    let error = scope(
        [IcebergFault::first(
            IcebergFaultPoint::DuringManifestReconciliation,
        )],
        sink.commit_aggregated(batch.clone(), commit_context()),
    )
    .await
    .expect_err("failed exact verification after commit must remain ambiguous");
    assert!(error.is_outcome_unknown());
    assert!(sink.unresolved_publication.lock().is_some());

    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);
    assert_eq!(table_row_count(&table).await, 2);
    assert_eq!(
        sink.committed_cursor(&namespace()).await.unwrap(),
        Some(CoordinatedCommitCursor {
            checkpoint_id: 1,
            fencing_token: 7,
        })
    );
    assert!(sink.unresolved_publication.lock().is_none());

    sink.commit_aggregated(batch, commit_context())
        .await
        .unwrap();
    let table = fixture.catalog.load_table(&table_ident()).await.unwrap();
    assert_eq!(table.metadata().snapshots().count(), 1);
}
