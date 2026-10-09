use std::sync::atomic::{AtomicBool, Ordering};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int32Type, Int64Type};
use arrow_array::Array;
use arrow_schema::{DataType, Field};

use super::checkpoint::parse_resumable;
use super::reader::{
    publish_terminal_wal_error, retained_wal_payload_bytes, send_wal_or_shutdown, WalPayloadTx,
};
use super::*;
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    source_mutations, source_row_positions, SourceBatch, SourceCheckpointUnavailablePolicy,
    SourceConnector, SourceConsistency, SourceInputMode, SourceRowPositionCapability,
    SourceTopology,
};
use crate::postgres::cdc::config::{OutputMode, TableName};
use crate::postgres::cdc::postgres_io::{source_config_digest, CaptureTable};
use crate::postgres::cdc::schema_resolution::bind_layout;
use crate::postgres::cdc::types::{PgColumn, INT4_OID, INT8_OID, TEXT_OID};

const OID: u32 = 7;

/// One pgoutput tuple value.
#[derive(Clone, Copy)]
enum V<'a> {
    T(&'a str),
    Null,
    Unchanged,
}

fn capture_table() -> CaptureTable {
    CaptureTable {
        relation: RelationInfo {
            relation_id: OID,
            namespace: "public".into(),
            name: "orders".into(),
            replica_identity: 'f',
            columns: vec![
                PgColumn::new("id".into(), INT8_OID, -1, true),
                PgColumn::new("status".into(), TEXT_OID, -1, true),
                PgColumn::new("qty".into(), INT4_OID, -1, true),
                PgColumn::new("note".into(), TEXT_OID, -1, true),
            ],
        },
        not_null: vec![true, false, false, false],
        primary_key: vec!["id".into()],
    }
}

fn source_config(output_mode: OutputMode) -> PostgresCdcConfig {
    PostgresCdcConfig {
        ssl_mode: crate::postgres::SslMode::Disable,
        table: TableName::parse("public.orders").unwrap(),
        output_mode,
        ..PostgresCdcConfig::default()
    }
}

fn declared(output_mode: OutputMode) -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("status", DataType::Utf8, true),
        Field::new("qty", DataType::Int32, true),
        Field::new("note", DataType::Utf8, true),
    ];
    if output_mode == OutputMode::Changelog {
        fields.push(Field::new("__weight", DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}

fn test_binding(config: &PostgresCdcConfig) -> PostgresCheckpointBinding {
    PostgresCheckpointBinding {
        system_identifier: 7,
        timeline_id: 1,
        database_oid: 5,
        publication_oid: 16_384,
        publication_definition_sha256: "11".repeat(32),
        source_config_sha256: source_config_digest(config),
        slot_plugin: "pgoutput".into(),
        slot_two_phase: false,
        slot_failover: false,
    }
}

fn streaming_source(output_mode: OutputMode) -> PostgresCdcSource {
    let config = source_config(output_mode);
    let schema = declared(output_mode);
    let layout = bind_layout(&config, &schema, &["id".into()], &capture_table()).unwrap();
    let mut source = PostgresCdcSource::new(config, None);
    source.checkpoint_binding = Some(test_binding(&source.config));
    source.open_rows = Some(RowBuilder::new(&layout));
    source.layout = Some(layout);
    source.relation = Some(capture_table().relation);
    source.schema = schema;
    source.state = ConnectorState::Running;
    source.phase = Phase::Streaming;
    source.applied_lsn = Some(pgwire_replication::AppliedLsnHandle::new(
        pgwire_replication::Lsn::ZERO,
    ));
    // Live contract revalidation needs a server; integration tests cover it.
    source.next_contract_check =
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(3_600));
    source
}

fn relation_message() -> Vec<u8> {
    let mut buf = vec![b'R'];
    buf.extend_from_slice(&OID.to_be_bytes());
    buf.extend_from_slice(b"public\0orders\0f");
    let columns = capture_table().relation.columns;
    buf.extend_from_slice(&i16::try_from(columns.len()).unwrap().to_be_bytes());
    for column in columns {
        buf.push(1);
        buf.extend_from_slice(column.name.as_bytes());
        buf.push(0);
        buf.extend_from_slice(&column.type_oid.to_be_bytes());
        buf.extend_from_slice(&column.type_modifier.to_be_bytes());
    }
    buf
}

fn tuple(buf: &mut Vec<u8>, tag: u8, values: &[V<'_>]) {
    buf.push(tag);
    buf.extend_from_slice(&i16::try_from(values.len()).unwrap().to_be_bytes());
    for value in values {
        match value {
            V::T(text) => {
                buf.push(b't');
                buf.extend_from_slice(&i32::try_from(text.len()).unwrap().to_be_bytes());
                buf.extend_from_slice(text.as_bytes());
            }
            V::Null => buf.push(b'n'),
            V::Unchanged => buf.push(b'u'),
        }
    }
}

fn begin(final_lsn: u64) -> Vec<u8> {
    let mut buf = vec![b'B'];
    buf.extend_from_slice(&final_lsn.to_be_bytes());
    buf.extend_from_slice(&0_i64.to_be_bytes());
    buf.extend_from_slice(&1_u32.to_be_bytes());
    buf
}

fn commit(commit_lsn: u64, end_lsn: u64) -> Vec<u8> {
    let mut buf = vec![b'C', 0];
    buf.extend_from_slice(&commit_lsn.to_be_bytes());
    buf.extend_from_slice(&end_lsn.to_be_bytes());
    buf.extend_from_slice(&0_i64.to_be_bytes());
    buf
}

fn insert(values: &[V<'_>]) -> Vec<u8> {
    let mut buf = vec![b'I'];
    buf.extend_from_slice(&OID.to_be_bytes());
    tuple(&mut buf, b'N', values);
    buf
}

fn update(old_tag: Option<u8>, old: &[V<'_>], new: &[V<'_>]) -> Vec<u8> {
    let mut buf = vec![b'U'];
    buf.extend_from_slice(&OID.to_be_bytes());
    if let Some(tag) = old_tag {
        tuple(&mut buf, tag, old);
    }
    tuple(&mut buf, b'N', new);
    buf
}

fn delete(old_tag: u8, old: &[V<'_>]) -> Vec<u8> {
    let mut buf = vec![b'D'];
    buf.extend_from_slice(&OID.to_be_bytes());
    tuple(&mut buf, old_tag, old);
    buf
}

fn row<'a>(id: &'a str, status: &'a str, qty: &'a str) -> [V<'a>; 4] {
    [V::T(id), V::T(status), V::T(qty), V::Null]
}

fn transaction(source: &mut PostgresCdcSource, final_lsn: u64, changes: Vec<Vec<u8>>) {
    source.enqueue_wal_data(begin(final_lsn));
    for change in changes {
        source.enqueue_wal_data(change);
    }
    source.enqueue_wal_data(commit(final_lsn, final_lsn + 0x10));
}

async fn next(source: &mut PostgresCdcSource, max: usize) -> SourceBatch {
    source.poll_batch(max).await.unwrap().expect("a batch")
}

fn ids(batch: &SourceBatch) -> Vec<i64> {
    batch
        .records
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

fn statuses(batch: &SourceBatch) -> Vec<Option<String>> {
    let column = batch.records.column(1).as_string::<i32>();
    (0..column.len())
        .map(|row| column.is_valid(row).then(|| column.value(row).to_string()))
        .collect()
}

fn encoded(source: &PostgresCdcSource, batch: SourceBatch) -> arrow_array::RecordBatch {
    use crate::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
    };
    batch
        .into_records_with_metadata(
            SourceRowPositionCapability::OrderedDeterministic,
            &schema_with_source_row_positions(&source.schema).unwrap(),
            &schema_with_source_mutations_and_row_positions(&source.schema).unwrap(),
        )
        .unwrap()
}

// ── Contract ──

#[test]
fn contract_is_commit_coupled_singleton_with_ordered_positions() {
    let source = streaming_source(OutputMode::Upsert);
    let empty = ConnectorConfig::new("postgres-cdc");
    let contract = source.contract(&empty).unwrap();
    assert_eq!(contract.consistency, SourceConsistency::CommitCoupled);
    assert_eq!(contract.topology, SourceTopology::Singleton);
    assert_eq!(contract.input_mode, SourceInputMode::KeyedUpsert);
    assert_eq!(
        contract.row_positions,
        SourceRowPositionCapability::OrderedDeterministic
    );
    assert!(!contract.is_exact_delivery_certified());
    let changelog = streaming_source(OutputMode::Changelog);
    assert_eq!(
        changelog.contract(&empty).unwrap().input_mode,
        SourceInputMode::FullChangelog
    );
    assert_eq!(
        source.checkpoint_unavailable_policy(),
        SourceCheckpointUnavailablePolicy::PollToReplayBoundary
    );
}

// ── Upsert row semantics ──

#[tokio::test]
async fn upsert_emits_full_puts_and_key_only_tombstones() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    transaction(
        &mut source,
        0x100,
        vec![
            insert(&row("1", "OPEN", "100")),
            update(
                Some(b'O'),
                &row("1", "OPEN", "100"),
                &row("1", "OPEN", "120"),
            ),
            delete(b'O', &row("1", "OPEN", "120")),
        ],
    );
    let batch = next(&mut source, 100).await;
    assert_eq!(ids(&batch), [1, 1, 1]);
    assert_eq!(
        batch.mutations().unwrap(),
        [
            SourceMutation::Put,
            SourceMutation::Put,
            SourceMutation::Tombstone
        ]
    );
    let qty = batch.records.column(2).as_primitive::<Int32Type>();
    assert_eq!((qty.value(0), qty.value(1)), (100, 120));
    assert!(qty.is_null(2), "a delete carries only the key");
    assert_eq!(statuses(&batch)[2], None);
}

#[tokio::test]
async fn primary_key_change_retracts_the_old_key_before_the_new_row() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    transaction(
        &mut source,
        0x100,
        vec![update(
            Some(b'O'),
            &row("1", "OPEN", "5"),
            &row("2", "OPEN", "5"),
        )],
    );
    let batch = next(&mut source, 100).await;
    assert_eq!(ids(&batch), [1, 2]);
    assert_eq!(
        batch.mutations().unwrap(),
        [SourceMutation::Tombstone, SourceMutation::Put]
    );
}

#[tokio::test]
async fn unchanged_toast_reads_the_old_image_and_null_stays_null() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    let big = "x".repeat(4096);
    transaction(
        &mut source,
        0x100,
        vec![update(
            Some(b'O'),
            &[V::T("1"), V::Null, V::T("1"), V::T(&big)],
            &[V::T("1"), V::Null, V::T("2"), V::Unchanged],
        )],
    );
    let batch = next(&mut source, 100).await;
    let notes = batch.records.column(3).as_string::<i32>();
    assert_eq!(
        notes.value(0),
        big,
        "unchanged TOAST is restored, never NULL"
    );
    assert!(batch.records.column(1).is_null(0), "SQL NULL stays NULL");
}

#[tokio::test]
async fn missing_full_old_image_fails_closed() {
    for change in [
        update(None, &[], &row("1", "OPEN", "1")),
        update(Some(b'K'), &row("1", "OPEN", "1"), &row("1", "OPEN", "2")),
        delete(b'K', &row("1", "OPEN", "1")),
    ] {
        let mut source = streaming_source(OutputMode::Upsert);
        source.enqueue_wal_data(relation_message());
        transaction(&mut source, 0x100, vec![change]);
        let error = source.poll_batch(100).await.unwrap_err();
        assert!(
            error.to_string().contains("REPLICA IDENTITY FULL"),
            "{error}"
        );
        assert_eq!(source.state, ConnectorState::Failed);
    }
}

#[tokio::test]
async fn unchanged_toast_without_a_full_old_value_is_an_error_not_null() {
    let mut source = streaming_source(OutputMode::Changelog);
    source.enqueue_wal_data(relation_message());
    transaction(
        &mut source,
        0x100,
        vec![update(
            Some(b'O'),
            &[V::T("1"), V::Null, V::T("1"), V::Unchanged],
            &[V::T("1"), V::Null, V::T("2"), V::Unchanged],
        )],
    );
    let error = source.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("unchanged TOAST"), "{error}");
}

#[tokio::test]
async fn repeated_changes_to_one_key_keep_transaction_order() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    transaction(
        &mut source,
        0x100,
        vec![
            insert(&row("1", "A", "1")),
            delete(b'O', &row("1", "A", "1")),
            insert(&row("1", "B", "2")),
            update(Some(b'O'), &row("1", "B", "2"), &row("1", "C", "3")),
        ],
    );
    let batch = next(&mut source, 100).await;
    assert_eq!(
        statuses(&batch),
        [Some("A".into()), None, Some("B".into()), Some("C".into())]
    );
    assert_eq!(
        batch.mutations().unwrap(),
        [
            SourceMutation::Put,
            SourceMutation::Tombstone,
            SourceMutation::Put,
            SourceMutation::Put
        ]
    );
}

// ── Changelog row semantics ──

#[tokio::test]
async fn changelog_weights_before_and_after_images() {
    let mut source = streaming_source(OutputMode::Changelog);
    source.enqueue_wal_data(relation_message());
    transaction(
        &mut source,
        0x100,
        vec![
            insert(&row("1", "OPEN", "100")),
            update(
                Some(b'O'),
                &row("1", "OPEN", "100"),
                &row("1", "CLOSED", "120"),
            ),
            update(
                Some(b'O'),
                &row("1", "CLOSED", "120"),
                &row("2", "CLOSED", "120"),
            ),
            delete(b'O', &row("2", "CLOSED", "120")),
        ],
    );
    let batch = next(&mut source, 100).await;
    assert!(batch.mutations().is_none(), "changelog rows carry weights");
    let weights = batch.records.column(4).as_primitive::<Int64Type>();
    assert_eq!(weights.values().as_ref(), [1, -1, 1, -1, 1, -1]);
    assert_eq!(ids(&batch), [1, 1, 1, 1, 2, 2]);
    let qty = batch.records.column(2).as_primitive::<Int32Type>();
    assert_eq!(qty.values().as_ref(), [100, 100, 120, 120, 120, 120]);
    assert_eq!(
        statuses(&batch),
        ["OPEN", "OPEN", "CLOSED", "CLOSED", "CLOSED", "CLOSED"].map(|s| Some(s.to_string()))
    );
}

// ── Relation and protocol contract ──

#[tokio::test]
async fn relation_layout_drift_and_unbound_relations_fail_closed() {
    let mut drifted = relation_message();
    // The last column's type OID sits immediately before its four-byte type modifier.
    let oid_end = drifted.len() - 4;
    drifted[oid_end - 4..oid_end].copy_from_slice(&INT8_OID.to_be_bytes());
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(drifted);
    let error = source.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("fresh snapshot"), "{error}");

    let mut source = streaming_source(OutputMode::Upsert);
    transaction(&mut source, 0x100, vec![insert(&row("1", "A", "1"))]);
    let error = source.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("announced"), "{error}");
}

#[tokio::test]
async fn truncate_stops_intake_with_reset_guidance() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    let mut truncate = vec![b'T'];
    truncate.extend_from_slice(&1_i32.to_be_bytes());
    truncate.push(0);
    truncate.extend_from_slice(&OID.to_be_bytes());
    transaction(&mut source, 0x100, vec![truncate]);
    let error = source.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("TRUNCATE"), "{error}");
    assert!(error.to_string().contains("drop slot"), "{error}");
}

#[tokio::test]
async fn open_transaction_rows_stay_invisible_and_behind_the_cursor() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    source.enqueue_wal_data(begin(0x100));
    source.enqueue_wal_data(insert(&row("1", "A", "1")));
    assert!(source.poll_batch(100).await.unwrap().is_none());
    assert_eq!(source.buffered_rows(), 1);
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/0"));
    source.enqueue_wal_data(commit(0x100, 0x110));
    let batch = next(&mut source, 100).await;
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/110"));
}

// ── Positions, batching, and cursors ──

#[tokio::test]
async fn positions_are_deterministic_wal_order_with_bound_cursors() {
    let changes = || {
        vec![
            insert(&row("1", "A", "1")),
            update(Some(b'O'), &row("1", "A", "1"), &row("2", "A", "1")),
        ]
    };
    let mut encoded_runs = Vec::new();
    for _ in 0..2 {
        let mut source = streaming_source(OutputMode::Upsert);
        source.enqueue_wal_data(relation_message());
        transaction(&mut source, 0x100, changes());
        transaction(&mut source, 0x200, vec![insert(&row("3", "B", "2"))]);
        let mut batch = next(&mut source, 100).await;
        let cursor = batch.take_cursor().expect("every batch binds its cursor");
        let crate::connector::SourceBatchCursor::Complete(cursor) = cursor else {
            panic!("complete cursor expected");
        };
        assert_eq!(cursor.get_offset("lsn"), Some("0/210"));
        encoded_runs.push(encoded(&source, batch));
    }
    assert_eq!(
        encoded_runs[0], encoded_runs[1],
        "replay reproduces rows and positions"
    );
    let positions = source_row_positions(&encoded_runs[0]).unwrap().unwrap();
    let mut previous = None;
    for row in 0..positions.len() {
        let position = positions.get(row).unwrap();
        assert_eq!(position.partition, b"laminar_slot");
        let key = (position.order_key.to_vec(), position.sub_offset);
        assert!(previous.as_ref().is_none_or(|previous| previous < &key));
        previous = Some(key);
    }
    let first = positions.get(0).unwrap();
    assert_eq!(first.order_key[0], 1);
    assert_eq!(&first.order_key[1..], &0x110_u64.to_be_bytes());
    assert_eq!(
        (0..3)
            .map(|row| positions.get(row).unwrap().sub_offset)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    assert!(source_mutations(&encoded_runs[0]).unwrap().is_some());
}

#[tokio::test]
async fn batch_target_never_splits_a_transaction() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.enqueue_wal_data(relation_message());
    transaction(
        &mut source,
        0x100,
        vec![
            insert(&row("1", "A", "1")),
            insert(&row("2", "A", "1")),
            insert(&row("3", "A", "1")),
        ],
    );
    transaction(&mut source, 0x200, vec![insert(&row("4", "A", "1"))]);
    assert_eq!(next(&mut source, 2).await.num_rows(), 3);
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/110"));
    assert_eq!(next(&mut source, 2).await.num_rows(), 1);
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/210"));
}

#[test]
fn snapshot_cursor_is_unavailable_and_never_resumable() {
    let mut source = streaming_source(OutputMode::Upsert);
    let streaming = source.try_checkpoint().unwrap().unwrap();
    let (lsn, binding) = parse_resumable(&streaming, &source.config, "test").unwrap();
    assert_eq!(lsn, Lsn::ZERO);
    assert_eq!(&binding, source.checkpoint_binding.as_ref().unwrap());

    let snapshot = super::checkpoint::write_cursor(
        &source.config,
        source.checkpoint_binding.as_ref(),
        super::checkpoint::CursorPhase::Snapshot,
    );
    assert!(snapshot.get_offset("lsn").is_none());
    let error = parse_resumable(&snapshot, &source.config, "test").unwrap_err();
    assert!(error.to_string().contains("cannot be resumed"), "{error}");

    let mut other_table = source.config.clone();
    other_table.table = TableName::parse("public.other").unwrap();
    assert!(parse_resumable(&streaming, &other_table, "test").is_err());
    source.config.output_mode = OutputMode::Changelog;
    let error = parse_resumable(&streaming, &source.config, "test").unwrap_err();
    assert!(error.to_string().contains("drifted"), "{error}");
}

// ── Bounded buffering ──

async fn queue(source: &mut PostgresCdcSource, payloads: Vec<WalPayload>) -> WalPayloadTx {
    let budget = Arc::new(Semaphore::new(64 * 1024 * 1024));
    let (tx, rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(payloads.len().max(1));
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    for payload in payloads {
        assert!(
            send_wal_or_shutdown(&tx, payload, &budget, 64 * 1024 * 1024, &mut shutdown_rx)
                .await
                .unwrap()
        );
    }
    source.wal_rx = Some(rx);
    source.wal_byte_budget = Some(budget);
    tx
}

fn xlog(data: Vec<u8>) -> WalPayload {
    WalPayload::XLogData {
        wal_end: 0,
        data: Bytes::from(data),
    }
}

fn wire_transaction(final_lsn: u64, rows: usize, note: &str) -> Vec<WalPayload> {
    let mut payloads = vec![WalPayload::Begin {
        final_lsn,
        commit_ts_us: 0,
    }];
    for id in 0..rows {
        let id = id.to_string();
        payloads.push(xlog(insert(&[V::T(&id), V::Null, V::Null, V::T(note)])));
    }
    payloads.push(WalPayload::Commit {
        end_lsn: final_lsn + 0x10,
        commit_ts_us: 0,
        lsn: final_lsn,
    });
    payloads
}

#[tokio::test]
async fn independently_fitting_transactions_progress_under_a_small_budget() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.config.max_buffered_bytes = 1024 * 1024;
    let note = "v".repeat(20 * 1024);
    let mut payloads = vec![xlog(relation_message())];
    for transaction in 0..4 {
        payloads.extend(wire_transaction(0x100 * (transaction + 1), 3, &note));
    }
    let _tx = queue(&mut source, payloads).await;
    let mut rows = 0;
    for _ in 0..20 {
        if let Some(batch) = source.poll_batch(1_000).await.unwrap() {
            rows += batch.num_rows();
            assert!(
                source.drainable_bytes() <= source.config.decoded_event_bytes(),
                "retained decoded rows stay within the stage budget"
            );
        }
    }
    assert_eq!(
        rows, 12,
        "every transaction is emitted, none fails the source"
    );
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/410"));
}

#[tokio::test]
async fn a_single_transaction_larger_than_the_budget_fails_with_diagnostics() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.config.max_buffered_bytes = 1024 * 1024;
    let note = "v".repeat(64 * 1024);
    let mut payloads = vec![xlog(relation_message())];
    payloads.extend(wire_transaction(0x100, 8, &note));
    let _tx = queue(&mut source, payloads).await;
    let error = source.poll_batch(1_000).await.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("exceeds the decoded-stage budget on its own"),
        "{message}"
    );
    assert!(message.contains("max.buffered.bytes"), "{message}");
    assert_eq!(source.state, ConnectorState::Failed);
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/0"));
}

#[tokio::test]
async fn relation_metadata_cannot_pin_intake_above_the_watermark() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.config.max_buffered_bytes = 1024 * 1024;
    let mut relation = capture_table().relation;
    relation
        .name
        .reserve(source.config.relation_metadata_bytes());
    source.relation = Some(relation);
    assert!(source.event_high_watermark() > source.config.decoded_event_bytes() / 2);
    let mut payloads = vec![xlog(relation_message())];
    payloads.extend(wire_transaction(0x100, 1, "small"));
    let _tx = queue(&mut source, payloads).await;
    let batch = source
        .poll_batch(100)
        .await
        .unwrap()
        .expect("a small transaction drains");
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn deferred_payloads_are_kept_and_decoded_after_the_drain() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.config.max_buffered_bytes = 1024 * 1024;
    let note = "v".repeat(20 * 1024);
    let mut payloads = vec![xlog(relation_message())];
    payloads.extend(wire_transaction(0x100, 6, &note));
    payloads.extend(wire_transaction(0x200, 6, &note));
    payloads.extend(wire_transaction(0x300, 1, "tail"));
    let _tx = queue(&mut source, payloads).await;
    let first = next(&mut source, 1_000).await;
    assert_eq!(first.num_rows(), 6);
    assert!(
        !source.pending_payloads.is_empty(),
        "the payload that did not fit waits; it is not lost"
    );
    let mut rows = first.num_rows();
    for _ in 0..5 {
        if let Some(batch) = source.poll_batch(1_000).await.unwrap() {
            rows += batch.num_rows();
        }
    }
    assert_eq!(rows, 13);
    assert_eq!(source.checkpoint().get_offset("lsn"), Some("0/310"));
}

// ── Durable feedback ──

fn committed_cursor(source: &PostgresCdcSource, lsn: u64) -> SourceCheckpoint {
    super::checkpoint::write_cursor(
        &source.config,
        source.checkpoint_binding.as_ref(),
        super::checkpoint::CursorPhase::Streaming(Lsn::new(lsn)),
    )
}

#[tokio::test]
async fn durable_feedback_reaches_the_worker_while_the_wal_queue_is_full() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.polled_lsn = Lsn::new(0x500);
    let (tx, rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
    let budget = Arc::new(Semaphore::new(1024));
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(send_wal_or_shutdown(
        &tx,
        WalPayload::KeepAlive { wal_end: 9 },
        &budget,
        1024,
        &mut shutdown_rx
    )
    .await
    .unwrap());
    source.wal_rx = Some(rx);
    let handle = source.applied_lsn.clone().unwrap();

    source
        .notify_epoch_committed(3, &committed_cursor(&source, 0x400))
        .await
        .unwrap();
    assert_eq!(handle.get().as_u64(), 0x400);
    assert_eq!(source.confirmed_flush_lsn(), Lsn::new(0x400));

    source
        .notify_epoch_committed(2, &committed_cursor(&source, 0x300))
        .await
        .unwrap();
    assert_eq!(
        handle.get().as_u64(),
        0x400,
        "stale commits never regress feedback"
    );
}

#[tokio::test]
async fn feedback_never_passes_the_polled_cursor_or_a_drifted_binding() {
    let mut source = streaming_source(OutputMode::Upsert);
    source.polled_lsn = Lsn::new(0x100);
    let handle = source.applied_lsn.clone().unwrap();
    let error = source
        .notify_epoch_committed(1, &committed_cursor(&source, 0x200))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ahead"), "{error}");

    let mut drifted = committed_cursor(&source, 0x100);
    drifted.set_metadata("publication_oid", "1");
    let error = source
        .notify_epoch_committed(1, &drifted)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("drifted"), "{error}");

    let snapshot = super::checkpoint::write_cursor(
        &source.config,
        source.checkpoint_binding.as_ref(),
        super::checkpoint::CursorPhase::Snapshot,
    );
    source.notify_epoch_committed(1, &snapshot).await.unwrap();
    assert_eq!(
        handle.get().as_u64(),
        0,
        "no feedback is ever sent for snapshot rows"
    );
}

// ── Reader ownership ──

#[tokio::test]
async fn close_interrupts_reader_blocked_on_full_wal_queue() {
    let mut source = streaming_source(OutputMode::Upsert);
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
    let payload_bytes = retained_wal_payload_bytes(&WalPayload::KeepAlive { wal_end: 1 });
    let byte_budget = Arc::new(Semaphore::new(payload_bytes * 2));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(send_wal_or_shutdown(
        &wal_tx,
        WalPayload::KeepAlive { wal_end: 1 },
        &byte_budget,
        payload_bytes * 2,
        &mut shutdown_rx,
    )
    .await
    .unwrap());
    let stopped = Arc::new(AtomicBool::new(false));
    let stopped_in_task = Arc::clone(&stopped);
    let task_budget = Arc::clone(&byte_budget);
    let reader_handle = tokio::spawn(async move {
        let sent = send_wal_or_shutdown(
            &wal_tx,
            WalPayload::KeepAlive { wal_end: 2 },
            &task_budget,
            payload_bytes * 2,
            &mut shutdown_rx,
        )
        .await;
        stopped_in_task.store(matches!(sent, Ok(false)), Ordering::Release);
    });
    source.wal_rx = Some(wal_rx);
    source.wal_byte_budget = Some(byte_budget);
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(reader_handle);

    tokio::time::timeout(std::time::Duration::from_millis(250), source.close())
        .await
        .expect("close must not wait for WAL queue capacity")
        .unwrap();
    assert!(stopped.load(Ordering::Acquire));
    assert_eq!(source.state, ConnectorState::Closed);
    assert!(source.applied_lsn.is_none());
}

#[tokio::test]
async fn reader_terminal_error_fails_the_source() {
    let terminal: WalTerminalError = Arc::new(std::sync::Mutex::new(None));
    let data_ready = Notify::new();
    publish_terminal_wal_error(&terminal, "stream failed".into(), &data_ready);
    let mut source = streaming_source(OutputMode::Upsert);
    source.wal_terminal_error = Some(terminal);
    let error = source.poll_batch(10).await.unwrap_err();
    assert!(error.to_string().contains("stream failed"));
    assert_eq!(source.state, ConnectorState::Failed);
}
