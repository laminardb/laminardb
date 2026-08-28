use std::sync::Arc;
use std::time::Duration;

use laminar_core::checkpoint::checkpoint_manifest::PipelineIdentity;
use laminar_core::checkpoint::CheckpointAttempt;
use tokio_stream::StreamExt;

use crate::config::ConnectorState;
use crate::connector::{
    CoordinatedCommitBatch, CoordinatedCommitContext, CoordinatedCommitCursor,
    CoordinatedCommitNamespace, CoordinatedCommitPayload, CoordinatedCommitter, DeliveryGuarantee,
    SinkConnector, SinkRuntimeContext,
};

use super::fault_injection::{scope, IcebergFault, IcebergFaultPoint};
use super::test_support::{batch as record_batch, create_test_table, table_ident, TestTable};
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

fn coordinated_sink(fixture: &TestTable, table: iceberg::table::Table) -> IcebergSink {
    let mut config = fixture.config.clone();
    config.delivery_guarantee = DeliveryGuarantee::ExactlyOnce;
    let mut sink = IcebergSink::new(config, None);
    sink.bind_runtime_context(SinkRuntimeContext {
        deployment_id: DEPLOYMENT_ID.into(),
        sink_id: SINK_ID.into(),
        participant_id: 1,
    })
    .unwrap();
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

async fn descriptor(sink: &mut IcebergSink, fixture: &TestTable, epoch: u64) -> Vec<u8> {
    sink.begin_epoch(epoch).await.unwrap();
    sink.write_batch(&record_batch(
        &fixture.table,
        &[(1, Some("a")), (2, Some("b"))],
    ))
    .await
    .unwrap();
    sink.pre_commit(epoch)
        .await
        .unwrap()
        .expect("non-empty epoch must produce a descriptor")
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

fn commit_context() -> CoordinatedCommitContext {
    CoordinatedCommitContext::new(tokio::time::Instant::now() + Duration::from_secs(10))
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
        failed.begin_epoch(1).await.unwrap();
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
