use super::*;
use crate::postgres::cdc::types::{INT4_OID, INT8_OID, TEXT_OID};
use arrow_array::cast::AsArray;

struct ReaderDropSignal(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for ReaderDropSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

fn default_source() -> PostgresCdcSource {
    let mut config = PostgresCdcConfig::default();
    config.ssl_mode = crate::postgres::SslMode::Disable;
    PostgresCdcSource::new(config, None)
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
        slot_failover: true,
    }
}

fn running_source() -> PostgresCdcSource {
    let mut src = default_source();
    src.state = ConnectorState::Running;
    src.checkpoint_binding = Some(test_binding(&src.config));
    src
}

fn recovery_identity_config() -> ConnectorConfig {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "db-a.internal");
    config.set("database", "orders");
    config.set("username", "replicator");
    config.set("password", "secret-a");
    config.set("slot.name", "orders_slot");
    config.set("publication", "orders_pub");
    config.set("table.include", "public.z, public.a");
    config
}

// ── Construction ──

#[test]
fn test_new_source() {
    let src = default_source();
    assert_eq!(src.state, ConnectorState::Created);
    assert!(src.confirmed_flush_lsn.is_zero());
    assert_eq!(src.buffered_events(), 0);
    assert_eq!(src.schema().fields().len(), 6);
}

#[test]
fn source_contract_fails_closed_for_raw_json_envelope() {
    let error = default_source()
        .contract(&ConnectorConfig::new("postgres-cdc"))
        .unwrap_err();
    assert!(error.to_string().contains("raw JSON change envelope"));
}

#[test]
fn source_lifecycle_cancellation_retires_the_generation() {
    assert_eq!(
        default_source().cancellation_policy(),
        crate::connector::ConnectorCancellationPolicy::RetireConnector
    );
}

#[test]
fn recovery_identity_ignores_operational_connection_tuning() {
    let left = recovery_identity_config();
    let source = PostgresCdcSource::from_config(&left).unwrap();
    let mut right = recovery_identity_config();
    right.set("host", "db-b.internal");
    right.set("port", "6432");
    right.set("username", "rotated-user");
    right.set("password", "rotated-secret");
    right.set("ssl.mode", "disable");
    right.set("max.buffered.bytes", "134217728");

    let stored = source.recovery_identity_options(&left).unwrap();
    assert_eq!(
        stored,
        source.recovery_identity_options(&right).unwrap(),
        "connection and memory tuning must not fence durable recovery"
    );
    assert_eq!(
        stored,
        source
            .recovery_identity_options(&ConnectorConfig::new("postgres-cdc"))
            .unwrap(),
        "an empty runtime config must use the validated provider config"
    );
}

#[test]
fn recovery_identity_normalizes_filters_and_fences_slot_semantics() {
    let left = recovery_identity_config();
    let source = PostgresCdcSource::from_config(&left).unwrap();
    let mut reordered = recovery_identity_config();
    reordered.set("table.include", "public.a,public.z,public.a");
    assert_eq!(
        source.recovery_identity_options(&left).unwrap(),
        source.recovery_identity_options(&reordered).unwrap(),
        "equivalent filters must have one canonical identity"
    );

    let mut different_slot = recovery_identity_config();
    different_slot.set("slot.name", "other_slot");
    assert_ne!(
        source.recovery_identity_options(&left).unwrap(),
        source.recovery_identity_options(&different_slot).unwrap(),
        "a different replication history must fence recovery"
    );
}

#[test]
fn test_from_config() {
    let mut config = ConnectorConfig::new("postgres-cdc");
    config.set("host", "pg.local");
    config.set("database", "testdb");
    config.set("slot.name", "my_slot");
    config.set("publication", "my_pub");
    config.set("ssl.mode", "disable");

    let src = PostgresCdcSource::from_config(&config).unwrap();
    assert_eq!(src.config().host, "pg.local");
    assert_eq!(src.config().database, "testdb");
}

#[test]
fn test_from_config_invalid() {
    let config = ConnectorConfig::new("postgres-cdc");
    assert!(PostgresCdcSource::from_config(&config).is_err());
}

// ── Lifecycle ──

#[tokio::test]
async fn initial_start_fails_closed_before_external_io() {
    let mut src = default_source();
    let error = src
        .start(
            SourceStart::new(
                ConnectorConfig::new("postgres-cdc"),
                SourcePosition::Initial,
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("initial startup must wait for certified snapshot/WAL bootstrap");
    assert!(error.to_string().contains("[LDB-5060]"), "{error}");
    assert_eq!(src.state, ConnectorState::Created);
    assert!(src.reader_handle.is_none());
    assert!(src.wal_rx.is_none());
}

#[tokio::test]
async fn start_normalizes_a_programmatic_filter_before_checkpoint_identity() {
    let mut src = default_source();
    src.config.table_include = vec![
        " public.users ".into(),
        String::new(),
        "public.orders".into(),
        "public.users".into(),
    ];
    let mut expected_config = src.config.clone();
    expected_config.normalize_table_filters();
    let mut checkpoint = src.checkpoint();
    checkpoint.set_offset("lsn", "1/10");
    write_checkpoint_binding(&mut checkpoint, &test_binding(&expected_config));

    src.start(
        SourceStart::new(
            ConnectorConfig::new("postgres-cdc"),
            SourcePosition::Resume {
                attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                checkpoint,
            },
            crate::connector::DeliveryGuarantee::AtLeastOnce,
        )
        .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(
        src.config.table_include,
        vec!["public.orders", "public.users"]
    );
}

#[tokio::test]
async fn test_close() {
    let mut src = running_source();
    src.inject_event(ChangeEvent {
        table: "t".to_string(),
        op: CdcOperation::Insert,
        lsn: Lsn::ZERO,
        ts_ms: 0,
        before: None,
        after: Some("{}".to_string()),
    });

    src.close().await.unwrap();
    assert_eq!(src.state, ConnectorState::Closed);
    assert_eq!(src.buffered_events(), 0);
}

#[tokio::test]
async fn normal_close_joins_the_owned_reader() {
    let mut src = running_source();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    src.reader_shutdown = Some(shutdown_tx);
    src.reader_handle = Some(tokio::spawn(async move {
        let _drop_signal = ReaderDropSignal(Some(dropped_tx));
        let _ = started_tx.send(());
        let _ = shutdown_rx.changed().await;
    }));
    started_rx.await.expect("reader task started");

    tokio::time::timeout(std::time::Duration::from_secs(1), src.close())
        .await
        .expect("normal close must join the reader")
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
        .await
        .expect("reader was not joined")
        .expect("reader drop signal closed");
    assert!(src.reader_handle.is_none());
    assert!(src.reader_shutdown.is_none());
}

#[tokio::test]
async fn cancelling_close_preserves_reader_ownership_for_retry() {
    let mut src = running_source();
    let terminal = src.terminal_task_tracker().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let release = Arc::new(Notify::new());
    let task_release = Arc::clone(&release);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader_guard = src
        .task_owner
        .track()
        .expect("live test source must admit its reader");
    src.reader_shutdown = Some(shutdown_tx);
    src.reader_handle = Some(tokio::spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = ReaderDropSignal(Some(dropped_tx));
        let _ = started_tx.send(());
        task_release.notified().await;
    }));
    started_rx.await.expect("reader task started");

    let mut close = Box::pin(src.close());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut close)
            .await
            .is_err(),
        "reader should keep the first close pending"
    );
    drop(close);

    assert!(src.reader_handle.is_some());
    assert!(src.reader_shutdown.is_some());
    assert!(*shutdown_rx.borrow());

    release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), src.close())
        .await
        .expect("retry close must join the retained reader")
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
        .await
        .expect("retained reader was not joined")
        .expect("reader drop signal closed");
    drop(src);
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("source tracker must resolve after retry close joins the reader");
}

#[tokio::test]
async fn dropping_source_signals_and_tracks_the_reader_to_completion() {
    let mut src = running_source();
    let terminal = src.terminal_task_tracker().unwrap();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut task_shutdown = shutdown_rx.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader_guard = src
        .task_owner
        .track()
        .expect("live test source must admit its reader");
    src.reader_shutdown = Some(shutdown_tx);
    src.reader_handle = Some(tokio::spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = ReaderDropSignal(Some(dropped_tx));
        let _ = started_tx.send(());
        let _ = task_shutdown.changed().await;
    }));
    started_rx.await.expect("reader task started");

    drop(src);

    assert!(*shutdown_rx.borrow());
    tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
        .await
        .expect("reader must stop when the source is dropped")
        .expect("reader drop signal closed");
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("source tracker outlived the completed WAL reader");
}

#[test]
fn tracker_covers_a_reader_destroyed_before_first_poll_on_another_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut source = running_source();
    let terminal = source.terminal_task_tracker().unwrap();
    let reader_guard = source
        .task_owner
        .track()
        .expect("live test source must admit its reader");
    let (shutdown_tx, _shutdown_rx) = tokio::sync::watch::channel(false);
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let drop_signal = ReaderDropSignal(Some(dropped_tx));
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(runtime.spawn(async move {
        let _reader_guard = reader_guard;
        let _drop_signal = drop_signal;
        std::future::pending::<()>().await;
    }));

    // The source is retired outside the reader's executor, before that
    // executor has polled the task even once.
    drop(source);
    assert!(!terminal.is_terminated());
    drop(runtime);

    let observer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    observer.block_on(async {
        tokio::time::timeout(std::time::Duration::from_secs(1), dropped_rx)
            .await
            .expect("runtime destruction must drop the unpolled reader promptly")
            .expect("unpolled reader drop signal was lost");
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            terminal.wait_terminated(),
        )
        .await
        .expect("tracker must resolve across runtimes after actual task destruction");
    });
}

#[tokio::test]
async fn close_interrupts_reader_blocked_on_full_wal_queue() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut src = running_source();
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
    let task_byte_budget = Arc::clone(&byte_budget);
    let reader_handle = tokio::spawn(async move {
        let sent = send_wal_or_shutdown(
            &wal_tx,
            WalPayload::KeepAlive { wal_end: 2 },
            &task_byte_budget,
            payload_bytes * 2,
            &mut shutdown_rx,
        )
        .await;
        stopped_in_task.store(matches!(sent, Ok(false)), Ordering::Release);
    });

    src.wal_rx = Some(wal_rx);
    src.wal_byte_budget = Some(byte_budget);
    src.reader_shutdown = Some(shutdown_tx);
    src.reader_handle = Some(reader_handle);

    tokio::time::timeout(std::time::Duration::from_millis(250), src.close())
        .await
        .expect("close must not wait for WAL queue capacity")
        .unwrap();
    assert!(stopped.load(Ordering::Acquire));
    assert_eq!(src.state, ConnectorState::Closed);
}

#[tokio::test]
async fn oversized_raw_wal_payload_reports_terminal_error_without_waiting_for_capacity() {
    let max_payload_bytes = 128;
    let byte_budget = Arc::new(Semaphore::new(max_payload_bytes));
    let _all_capacity = Arc::clone(&byte_budget)
        .acquire_many_owned(u32::try_from(max_payload_bytes).unwrap())
        .await
        .unwrap();
    let (wal_tx, _wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let oversized = WalPayload::XLogData {
        wal_end: 1,
        data: Bytes::from(vec![0; max_payload_bytes]),
    };

    let message = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        send_wal_or_shutdown(
            &wal_tx,
            oversized,
            &byte_budget,
            max_payload_bytes,
            &mut shutdown_rx,
        ),
    )
    .await
    .expect("oversized payload must fail before waiting for byte permits")
    .expect_err("payload and envelope exceed the byte budget");
    assert!(message.contains("hard raw buffer limit"), "{message}");

    let terminal_error: WalTerminalError = Arc::new(std::sync::Mutex::new(None));
    let data_ready = Notify::new();
    publish_terminal_wal_error(&terminal_error, message.clone(), &data_ready);
    let mut source = running_source();
    source.wal_terminal_error = Some(terminal_error);
    let error = source
        .fail_on_terminal_wal_error()
        .expect_err("reader terminal error must fail the source");
    assert!(error.to_string().contains(message.as_str()));
    assert_eq!(source.state, ConnectorState::Failed);
}

#[tokio::test]
async fn raw_wal_budget_backpressures_aggregate_payload_bytes() {
    let first = WalPayload::XLogData {
        wal_end: 1,
        data: Bytes::from_static(&[1; 32]),
    };
    let payload_bytes = retained_wal_payload_bytes(&first);
    let byte_limit = payload_bytes * 2 - 1;
    let byte_budget = Arc::new(Semaphore::new(byte_limit));
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(
        send_wal_or_shutdown(&wal_tx, first, &byte_budget, byte_limit, &mut shutdown_rx,)
            .await
            .unwrap()
    );

    let task_budget = Arc::clone(&byte_budget);
    let mut handle = tokio::spawn(async move {
        send_wal_or_shutdown(
            &wal_tx,
            WalPayload::XLogData {
                wal_end: 2,
                data: Bytes::from_static(&[2; 32]),
            },
            &task_budget,
            byte_limit,
            &mut shutdown_rx,
        )
        .await
    });

    let first_owned = wal_rx.recv().await.unwrap();
    assert_eq!(byte_budget.available_permits(), payload_bytes - 1);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), &mut handle)
            .await
            .is_err(),
        "receiving must not release byte ownership before processing"
    );
    drop(first_owned);
    assert!(handle.await.unwrap().unwrap());
    let second_owned = wal_rx.recv().await.unwrap();
    drop(second_owned);
    assert_eq!(byte_budget.available_permits(), byte_limit);
}

#[tokio::test]
async fn pending_wal_payload_keeps_its_byte_reservation() {
    let payload = WalPayload::KeepAlive { wal_end: 7 };
    let payload_bytes = retained_wal_payload_bytes(&payload);
    let byte_budget = Arc::new(Semaphore::new(payload_bytes));
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(send_wal_or_shutdown(
        &wal_tx,
        payload,
        &byte_budget,
        payload_bytes,
        &mut shutdown_rx,
    )
    .await
    .unwrap());

    let mut source = running_source();
    source
        .pending_payloads
        .push_back(wal_rx.recv().await.unwrap());
    assert_eq!(byte_budget.available_permits(), 0);
    let pending = source.pending_payloads.pop_front().unwrap();
    source.process_owned_wal_payload(pending).unwrap();
    assert_eq!(source.write_lsn, Lsn::new(7));
    assert_eq!(byte_budget.available_permits(), payload_bytes);
}

#[tokio::test]
async fn owned_wal_path_records_boundary_bytes_once() {
    let begin = WalPayload::Begin {
        final_lsn: 0x100,
        commit_ts_us: 0,
        xid: 1,
    };
    let commit = WalPayload::Commit {
        end_lsn: 0x200,
        commit_ts_us: 0,
        lsn: 0x100,
    };
    let expected_bytes = logical_wal_payload_bytes(&begin)
        .checked_add(logical_wal_payload_bytes(&commit))
        .unwrap();
    let byte_limit = retained_wal_payload_bytes(&begin)
        .checked_add(retained_wal_payload_bytes(&commit))
        .unwrap();
    let byte_budget = Arc::new(Semaphore::new(byte_limit));
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(
        send_wal_or_shutdown(&wal_tx, begin, &byte_budget, byte_limit, &mut shutdown_rx,)
            .await
            .unwrap()
    );
    assert!(
        send_wal_or_shutdown(&wal_tx, commit, &byte_budget, byte_limit, &mut shutdown_rx,)
            .await
            .unwrap()
    );

    let mut source = running_source();
    source
        .process_owned_wal_payload(wal_rx.recv().await.unwrap())
        .unwrap();
    source
        .process_owned_wal_payload(wal_rx.recv().await.unwrap())
        .unwrap();

    assert_eq!(
        source.metrics.bytes_received.get(),
        u64::try_from(expected_bytes).unwrap()
    );
    assert_eq!(byte_budget.available_permits(), byte_limit);
}

#[tokio::test]
async fn decoded_byte_high_watermark_stops_raw_lookahead() {
    let mut source = running_source();
    source.config.max_buffered_bytes = 1024 * 1024;
    let relation_name = "x".repeat(source.config.decoded_high_watermark_bytes());
    let relation = WalPayload::XLogData {
        wal_end: 1,
        data: Bytes::from(PostgresCdcSource::build_relation_message(
            1,
            "public",
            &relation_name,
            &[],
        )),
    };
    let keepalive = WalPayload::KeepAlive { wal_end: 2 };
    let byte_limit = source.config.raw_wal_bytes();
    let byte_budget = Arc::new(Semaphore::new(byte_limit));
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(send_wal_or_shutdown(
        &wal_tx,
        relation,
        &byte_budget,
        byte_limit,
        &mut shutdown_rx,
    )
    .await
    .unwrap());
    assert!(send_wal_or_shutdown(
        &wal_tx,
        keepalive,
        &byte_budget,
        byte_limit,
        &mut shutdown_rx,
    )
    .await
    .unwrap());
    source.wal_rx = Some(wal_rx);
    source.wal_byte_budget = Some(byte_budget);

    assert!(source.poll_batch(1).await.unwrap().is_none());
    assert_eq!(source.relation_cache.len(), 1);
    assert!(
        source.decoded_retained_bytes().unwrap() >= source.config.decoded_high_watermark_bytes()
    );
    assert!(source.pending_payloads.is_empty());
    assert_eq!(source.write_lsn, Lsn::new(1));
}

#[tokio::test]
async fn bounded_poll_self_notifies_when_an_open_transaction_has_queued_work() {
    let begin = WalPayload::Begin {
        final_lsn: 0x100,
        commit_ts_us: 0,
        xid: 1,
    };
    let commit = WalPayload::Commit {
        end_lsn: 0x200,
        commit_ts_us: 0,
        lsn: 0x100,
    };
    let byte_limit =
        retained_wal_payload_bytes(&begin).saturating_add(retained_wal_payload_bytes(&commit));
    let byte_budget = Arc::new(Semaphore::new(byte_limit));
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(2);
    let (_shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    assert!(
        send_wal_or_shutdown(&wal_tx, begin, &byte_budget, byte_limit, &mut shutdown_rx,)
            .await
            .unwrap()
    );
    assert!(
        send_wal_or_shutdown(&wal_tx, commit, &byte_budget, byte_limit, &mut shutdown_rx,)
            .await
            .unwrap()
    );

    let mut source = running_source();
    source.wal_rx = Some(wal_rx);
    source.wal_byte_budget = Some(byte_budget);
    assert!(source.poll_batch(1).await.unwrap().is_none());
    assert!(source.current_txn.is_some());
    assert_eq!(source.pending_payloads.len(), 1);
    tokio::time::timeout(
        std::time::Duration::from_millis(25),
        source.data_ready.notified(),
    )
    .await
    .expect("queued protocol work must leave a readiness permit");
    assert!(source.poll_batch(1).await.unwrap().is_none());
    assert!(source.current_txn.is_none());
    assert!(source.pending_payloads.is_empty());
}

#[tokio::test]
async fn close_interrupts_reader_waiting_for_raw_wal_byte_budget() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let mut source = running_source();
    let payload = WalPayload::KeepAlive { wal_end: 9 };
    let payload_bytes = retained_wal_payload_bytes(&payload);
    let byte_budget = Arc::new(Semaphore::new(payload_bytes));
    let held_capacity = Arc::clone(&byte_budget)
        .acquire_many_owned(u32::try_from(payload_bytes).unwrap())
        .await
        .unwrap();
    let (wal_tx, wal_rx) = crossfire::mpsc::bounded_async::<OwnedWalPayload>(1);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let task_budget = Arc::clone(&byte_budget);
    let stopped = Arc::new(AtomicBool::new(false));
    let task_stopped = Arc::clone(&stopped);
    let reader_handle = tokio::spawn(async move {
        let sent = send_wal_or_shutdown(
            &wal_tx,
            payload,
            &task_budget,
            payload_bytes,
            &mut shutdown_rx,
        )
        .await;
        task_stopped.store(matches!(sent, Ok(false)), Ordering::Release);
    });

    source.wal_rx = Some(wal_rx);
    source.wal_byte_budget = Some(byte_budget);
    source.reader_shutdown = Some(shutdown_tx);
    source.reader_handle = Some(reader_handle);
    tokio::time::timeout(std::time::Duration::from_millis(250), source.close())
        .await
        .expect("close must interrupt a WAL byte-permit wait")
        .unwrap();
    drop(held_capacity);
    assert!(stopped.load(Ordering::Acquire));
    assert_eq!(source.state, ConnectorState::Closed);
}

// ── Checkpoint / Restore ──

#[test]
fn test_checkpoint() {
    let mut src = running_source();
    src.confirmed_flush_lsn = "1/ABCD".parse().unwrap();
    src.polled_lsn = "1/ABCD".parse().unwrap();
    src.write_lsn = "1/ABCE".parse().unwrap();

    let cp = src.checkpoint();
    assert_eq!(cp.get_offset("lsn"), Some("1/ABCD"));
    assert_eq!(cp.get_offset("write_lsn"), None);
    assert_eq!(cp.get_metadata("slot_name"), Some("laminar_slot"));
    assert_eq!(cp.get_metadata("checkpoint_version"), Some("3"));
    assert_eq!(cp.get_metadata(SYSTEM_IDENTIFIER_METADATA), Some("7"));
    assert_eq!(cp.get_metadata(TIMELINE_ID_METADATA), Some("1"));
    assert_eq!(cp.get_metadata(SLOT_PLUGIN_METADATA), Some("pgoutput"));
}

fn committed_lsn_checkpoint(lsn: &str) -> SourceCheckpoint {
    let source = default_source();
    let mut checkpoint = source.checkpoint();
    checkpoint.set_offset("lsn", lsn);
    write_checkpoint_binding(&mut checkpoint, &test_binding(&source.config));
    checkpoint
}

#[tokio::test]
async fn committed_epoch_rejects_malformed_durable_lsn() {
    let mut source = default_source();
    let error = source
        .notify_epoch_committed(7, &committed_lsn_checkpoint("not-an-lsn"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid LSN"), "{error}");
    assert!(source.confirmed_flush_lsn.is_zero());
}

#[tokio::test]
async fn committed_epoch_rejects_missing_feedback_channel() {
    let mut source = running_source();
    source.polled_lsn = "1/10".parse().unwrap();
    let error = source
        .notify_epoch_committed(7, &committed_lsn_checkpoint("1/10"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("feedback channel is missing"),
        "{error}"
    );
    assert!(source.confirmed_flush_lsn.is_zero());
}

#[tokio::test]
async fn committed_epoch_rejects_closed_feedback_without_advancing_local_lsn() {
    let mut source = running_source();
    source.polled_lsn = "1/10".parse().unwrap();
    let (feedback_tx, feedback_rx) = tokio::sync::watch::channel(0);
    drop(feedback_rx);
    source.confirmed_lsn_tx = Some(feedback_tx);

    let error = source
        .notify_epoch_committed(7, &committed_lsn_checkpoint("1/10"))
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("feedback channel is closed"),
        "{error}"
    );
    assert!(source.confirmed_flush_lsn.is_zero());
}

#[tokio::test]
async fn committed_epoch_advances_local_lsn_only_after_feedback_handoff() {
    let mut source = running_source();
    source.polled_lsn = "1/10".parse().unwrap();
    let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0);
    source.confirmed_lsn_tx = Some(feedback_tx);

    source
        .notify_epoch_committed(7, &committed_lsn_checkpoint("1/10"))
        .await
        .unwrap();
    let expected = "1/10".parse::<Lsn>().unwrap();
    assert_eq!(source.confirmed_flush_lsn, expected);
    assert_eq!(*feedback_rx.borrow_and_update(), expected.as_u64());
}

#[tokio::test]
async fn committed_epoch_ahead_of_polled_lsn_leaves_feedback_and_cursor_unchanged() {
    let mut source = running_source();
    source.confirmed_flush_lsn = "1/8".parse().unwrap();
    source.polled_lsn = "1/10".parse().unwrap();
    let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0x1008);
    source.confirmed_lsn_tx = Some(feedback_tx);

    let error = source
        .notify_epoch_committed(7, &committed_lsn_checkpoint("1/11"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("ahead of"), "{error}");
    assert_eq!(source.confirmed_flush_lsn, "1/8".parse().unwrap());
    assert_eq!(*feedback_rx.borrow_and_update(), 0x1008);
}

#[tokio::test]
async fn committed_epoch_rejects_binding_drift_before_feedback() {
    let mut source = running_source();
    source.polled_lsn = "1/10".parse().unwrap();
    let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0);
    source.confirmed_lsn_tx = Some(feedback_tx);
    let mut checkpoint = committed_lsn_checkpoint("1/10");
    checkpoint.set_metadata(PUBLICATION_OID_METADATA, "16385");

    let error = source
        .notify_epoch_committed(7, &checkpoint)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identity drifted"), "{error}");
    assert!(source.confirmed_flush_lsn.is_zero());
    assert_eq!(*feedback_rx.borrow_and_update(), 0);
}

#[tokio::test]
async fn confirmed_lsn_watch_wakes_without_a_replication_event() {
    let (feedback_tx, mut feedback_rx) = tokio::sync::watch::channel(0);
    feedback_tx.send(0x1234).unwrap();

    tokio::time::timeout(std::time::Duration::from_millis(25), feedback_rx.changed())
        .await
        .expect("confirmed LSN notification must wake the reader select")
        .unwrap();
    assert_eq!(
        take_confirmed_lsn(&mut feedback_rx).unwrap().as_u64(),
        0x1234
    );
}

#[tokio::test]
async fn committed_epoch_never_regresses_confirmed_lsn() {
    let mut source = running_source();
    source.confirmed_flush_lsn = "2/20".parse().unwrap();
    source.polled_lsn = "2/20".parse().unwrap();

    source
        .notify_epoch_committed(6, &committed_lsn_checkpoint("1/10"))
        .await
        .unwrap();
    assert_eq!(source.confirmed_flush_lsn, "2/20".parse().unwrap());
}

#[tokio::test]
async fn test_resume_installs_exact_engine_lsn() {
    let mut src = default_source();
    let cp = committed_lsn_checkpoint("2/FF00");

    let result = src
        .start(
            SourceStart::new(
                ConnectorConfig::new("postgres-cdc"),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: cp,
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await;
    result.unwrap();
    assert_eq!(src.confirmed_flush_lsn.as_u64(), 0x2_0000_FF00);
    assert_eq!(src.polled_lsn.as_u64(), 0x2_0000_FF00);
    assert_eq!(
        src.write_lsn.as_u64(),
        0x2_0000_FF00,
        "diagnostic write_lsn starts at the durable recovery cursor"
    );
}

#[tokio::test]
async fn test_resume_invalid_lsn_fails_before_replication() {
    let mut src = default_source();
    let cp = committed_lsn_checkpoint("not_an_lsn");

    let error = src
        .start(
            SourceStart::new(
                ConnectorConfig::new("postgres-cdc"),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: cp,
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("invalid durable LSN must fail closed");
    assert!(error.to_string().contains("invalid LSN"));
    assert_eq!(src.state, ConnectorState::Created);
}

#[tokio::test]
async fn old_checkpoint_version_fails_without_installing_runtime_state() {
    let mut src = default_source();
    let mut checkpoint = committed_lsn_checkpoint("1/10");
    checkpoint.set_metadata("checkpoint_version", "2");

    let error = src
        .start(
            SourceStart::new(
                ConnectorConfig::new("postgres-cdc"),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint,
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("expected '3'"), "{error}");
    assert_eq!(src.state, ConnectorState::Created);
    assert!(src.checkpoint_binding.is_none());
    assert!(src.reader_handle.is_none());
    assert!(src.wal_rx.is_none());
    assert!(src.confirmed_lsn_tx.is_none());
}

// ── Poll (empty) ──

#[tokio::test]
async fn test_poll_empty() {
    let mut src = running_source();
    let result = src.poll_batch(100).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_poll_not_running() {
    let mut src = default_source();
    assert!(src.poll_batch(100).await.is_err());
}

// ── WAL message processing: full transaction ──

#[tokio::test]
async fn test_process_insert_transaction() {
    let mut src = running_source();

    let rel_msg = PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
    );
    let begin_msg = PostgresCdcSource::build_begin_message(0x100, 0, 1);
    let insert_msg = PostgresCdcSource::build_insert_message(16384, &[Some("42"), Some("Alice")]);
    let commit_msg = PostgresCdcSource::build_commit_message(0x100, 0x200, 0);

    src.enqueue_wal_data(rel_msg);
    src.enqueue_wal_data(begin_msg);
    src.enqueue_wal_data(insert_msg);
    src.enqueue_wal_data(commit_msg);

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let records = &batch.records;
    let table_col = records.column(0).as_string::<i32>();
    assert_eq!(table_col.value(0), "public.users");

    let op_col = records.column(1).as_string::<i32>();
    assert_eq!(op_col.value(0), "I");

    let after_col = records.column(5).as_string::<i32>();
    let after_json: serde_json::Value = serde_json::from_str(after_col.value(0)).unwrap();
    assert_eq!(after_json["id"], "42");
    assert_eq!(after_json["name"], "Alice");

    // before should be null for INSERT
    assert!(records.column(4).is_null(0));
}

// ── Multiple events in one transaction ──

#[tokio::test]
async fn test_multi_event_transaction() {
    let mut src = running_source();

    // Register relation
    let rel_msg = PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
    );
    src.enqueue_wal_data(rel_msg);

    // Transaction with 3 events
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x300, 0, 2));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
        16384,
        &[Some("1"), Some("Alice")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
        16384,
        &[Some("2"), Some("Bob")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
        16384,
        &[Some("3"), Some("Charlie")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x300, 0x400, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
}

// ── Events buffered until commit ──

#[tokio::test]
async fn test_events_buffered_until_commit() {
    let mut src = running_source();

    let rel_msg = PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1)],
    );
    src.enqueue_wal_data(rel_msg);

    // Begin + Insert but NO commit
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(16384, &[Some("1")]));

    // Poll should return nothing (events in txn buffer)
    let result = src.poll_batch(100).await.unwrap();
    assert!(result.is_none());

    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn decoded_container_growth_is_charged_before_the_rejected_event() {
    let mut src = running_source();
    src.config.max_buffered_bytes = 1024 * 1024;
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "id", INT4_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x400, 0, 1));
    for _ in 0..10_000 {
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
    }

    let error = src.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("decoded-stage buffer limit"));
    assert_eq!(src.state, ConnectorState::Failed);
    assert!(src.buffered_event_count > 0);
    assert_eq!(
        src.current_txn.as_ref().unwrap().events.len(),
        src.buffered_event_count
    );
    assert!(src.decoded_retained_bytes().unwrap() <= src.config.decoded_event_bytes());
    assert!(src.committed_transactions.is_empty());
    assert!(src.write_lsn.is_zero());
    assert!(src.polled_lsn.is_zero());
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/0"));
}

#[tokio::test]
async fn relation_cache_growth_is_bounded_by_the_decoded_stage() {
    let mut src = running_source();
    src.config.max_buffered_bytes = 1024 * 1024;
    for relation_id in 1..=5_000 {
        let name = format!("t{relation_id}");
        src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
            relation_id,
            "public",
            &name,
            &[(1, "id", INT4_OID, -1)],
        ));
    }

    let error = src.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("relation-cache"), "{error}");
    assert_eq!(src.state, ConnectorState::Failed);
    assert!(!src.relation_cache.is_empty());
    assert!(src.decoded_retained_bytes().unwrap() <= src.config.decoded_event_bytes());
}

#[test]
fn relation_replacement_charges_only_retained_growth() {
    let mut src = running_source();
    let relation = RelationInfo {
        relation_id: 1,
        namespace: "public".to_string(),
        name: "orders".to_string(),
        replica_identity: 'd',
        columns: Vec::new(),
    };
    src.admit_relation(relation.clone()).unwrap();
    let retained = src.decoded_retained_bytes().unwrap();
    src.config.max_buffered_bytes = retained.checked_mul(3).unwrap();

    src.admit_relation(relation).unwrap();

    assert_eq!(src.relation_cache.len(), 1);
    assert_eq!(src.decoded_retained_bytes().unwrap(), retained);
}

#[tokio::test]
async fn json_escape_expansion_is_rejected_by_the_total_byte_limit() {
    let mut src = running_source();
    src.config.max_buffered_bytes = 1024 * 1024;
    let oversized_value = "\n".repeat(src.config.decoded_event_bytes() / 2 + 128);
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "payload", TEXT_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x400, 0, 1));
    let insert = PostgresCdcSource::build_insert_message(100, &[Some(&oversized_value)]);
    assert!(insert.len() <= src.config.raw_wal_bytes());
    src.enqueue_wal_data(insert);

    let error = src.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("retained bytes"));
    assert_eq!(src.state, ConnectorState::Failed);
    assert_eq!(src.buffered_event_count, 0);
    assert_eq!(src.buffered_event_bytes, 0);
    assert!(src.current_txn.as_ref().unwrap().events.is_empty());
    assert!(src.write_lsn.is_zero());
    assert!(src.polled_lsn.is_zero());
}

#[tokio::test]
async fn row_change_outside_transaction_fails_closed() {
    let mut src = running_source();
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "id", INT4_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));

    let error = src.poll_batch(100).await.unwrap_err();
    assert!(error.to_string().contains("outside a transaction"));
    assert_eq!(src.buffered_events(), 0);
}

#[test]
fn corrupt_commit_boundaries_fail_closed() {
    let cases = [
        super::super::decoder::CommitMessage {
            flags: 0,
            commit_lsn: Lsn::new(0x101),
            end_lsn: Lsn::new(0x200),
            commit_ts_ms: 7,
        },
        super::super::decoder::CommitMessage {
            flags: 0,
            commit_lsn: Lsn::new(0x100),
            end_lsn: Lsn::new(0x0ff),
            commit_ts_ms: 7,
        },
        super::super::decoder::CommitMessage {
            flags: 0,
            commit_lsn: Lsn::new(0x100),
            end_lsn: Lsn::new(0x200),
            commit_ts_ms: 8,
        },
    ];

    for commit in cases {
        let mut src = running_source();
        src.process_wal_message(WalMessage::Begin(super::super::decoder::BeginMessage {
            final_lsn: Lsn::new(0x100),
            commit_ts_ms: 7,
            xid: 1,
        }))
        .unwrap();
        let error = src
            .process_wal_message(WalMessage::Commit(commit))
            .unwrap_err();
        assert_eq!(src.state, ConnectorState::Failed, "{error}");
        assert!(src.committed_transactions.is_empty());
        assert!(src.current_txn.is_some());
    }
}

#[test]
fn commit_end_lsn_cannot_move_behind_a_queued_transaction() {
    let mut src = running_source();
    for (final_lsn, end_lsn) in [(0x100, 0x300), (0x200, 0x250)] {
        src.process_wal_message(WalMessage::Begin(super::super::decoder::BeginMessage {
            final_lsn: Lsn::new(final_lsn),
            commit_ts_ms: 0,
            xid: 1,
        }))
        .unwrap();
        let result =
            src.process_wal_message(WalMessage::Commit(super::super::decoder::CommitMessage {
                flags: 0,
                commit_lsn: Lsn::new(final_lsn),
                end_lsn: Lsn::new(end_lsn),
                commit_ts_ms: 0,
            }));
        if end_lsn == 0x250 {
            let error = result.unwrap_err();
            assert!(error.to_string().contains("behind"), "{error}");
        } else {
            result.unwrap();
        }
    }
    assert_eq!(src.state, ConnectorState::Failed);
    assert_eq!(src.committed_transactions.len(), 1);
    assert!(src.current_txn.is_some());
}

#[test]
fn vendor_timestamp_overflow_is_rejected() {
    let mut src = running_source();
    let error = src
        .process_wal_payload(WalPayload::Begin {
            final_lsn: 0x100,
            commit_ts_us: i64::MAX,
            xid: 1,
        })
        .unwrap_err();
    assert!(error.to_string().contains("timestamp"), "{error}");
    assert!(src.current_txn.is_none());
}

#[test]
fn malformed_raw_boundary_is_not_silently_skipped() {
    let mut src = running_source();
    let mut data = PostgresCdcSource::build_begin_message(0x100, 0, 1);
    data.push(0xff);
    let error = src
        .process_wal_payload(WalPayload::XLogData {
            wal_end: 0x100,
            data: Bytes::from(data),
        })
        .unwrap_err();
    assert!(error.to_string().contains("trailing bytes"), "{error}");
    assert!(src.current_txn.is_none());
    assert!(src.write_lsn.is_zero());
}

#[tokio::test]
async fn checkpoint_stays_before_open_transaction_when_write_lsn_advances() {
    let mut src = running_source();
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "id", INT4_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x300, 0, 2));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("2")]));

    src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
    assert!(src.current_txn.is_some());

    src.process_wal_payload(WalPayload::KeepAlive { wal_end: 0x500 })
        .unwrap();
    assert_eq!(src.write_lsn.as_u64(), 0x500);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
}

#[tokio::test]
async fn batch_target_never_splits_a_committed_transaction() {
    let mut src = running_source();
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "id", INT4_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x400, 0, 1));
    for id in ["1", "2", "3"] {
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some(id)]));
    }
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x400, 0x500, 0));

    let first = src.poll_batch(2).await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 3);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/500"));
    assert!(src.poll_batch(2).await.unwrap().is_none());
}

#[tokio::test]
async fn batch_target_stops_before_the_next_whole_transaction() {
    let mut src = running_source();
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "id", INT4_OID, -1)],
    ));
    for (xid, ids, final_lsn, end_lsn) in
        [(1, ["1", "2"], 0x100, 0x200), (2, ["3", "4"], 0x300, 0x400)]
    {
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(final_lsn, 0, xid));
        for id in ids {
            src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some(id)]));
        }
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(
            final_lsn, end_lsn, 0,
        ));
    }

    let first = src.poll_batch(3).await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
    let second = src.poll_batch(3).await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 2);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/400"));
}

#[tokio::test]
async fn buffered_whole_transaction_wakes_an_event_driven_next_poll() {
    let mut src = running_source();
    src.inject_event(ChangeEvent {
        table: "public.orders".into(),
        op: CdcOperation::Insert,
        lsn: Lsn::new(0x100),
        ts_ms: 0,
        before: None,
        after: Some("{\"id\":\"1\"}".into()),
    });
    src.inject_event(ChangeEvent {
        table: "public.orders".into(),
        op: CdcOperation::Insert,
        lsn: Lsn::new(0x200),
        ts_ms: 0,
        before: None,
        after: Some("{\"id\":\"2\"}".into()),
    });
    let ready = src.data_ready_notify().unwrap();

    let first = src.poll_batch(1).await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 1);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/100"));
    tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
        .await
        .expect("a buffered committed transaction must retain a readiness permit");
    let second = src.poll_batch(1).await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 1);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
}

#[tokio::test]
async fn zero_capacity_poll_does_not_self_wake() {
    let mut src = running_source();
    src.inject_event(ChangeEvent {
        table: "public.orders".into(),
        op: CdcOperation::Insert,
        lsn: Lsn::new(0x100),
        ts_ms: 0,
        before: None,
        after: Some("{\"id\":\"1\"}".into()),
    });
    let ready = src.data_ready_notify().unwrap();

    assert!(src.poll_batch(0).await.unwrap().is_none());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
            .await
            .is_err(),
        "a zero-capacity poll must not create a readiness busy loop"
    );
}

#[tokio::test]
async fn final_batch_rearms_raw_polling_once() {
    let mut src = running_source();
    src.inject_event(ChangeEvent {
        table: "public.orders".into(),
        op: CdcOperation::Insert,
        lsn: Lsn::new(0x100),
        ts_ms: 0,
        before: None,
        after: Some("{\"id\":\"1\"}".into()),
    });
    let ready = src.data_ready_notify().unwrap();

    src.poll_batch(1).await.unwrap().unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
        .await
        .expect("the final batch must re-arm raw WAL polling");
    assert!(src.poll_batch(1).await.unwrap().is_none());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), ready.notified())
            .await
            .is_err(),
        "an empty follow-up poll must quiesce"
    );
}

#[tokio::test]
async fn empty_filtered_transaction_advances_only_in_wal_order() {
    let mut config = PostgresCdcConfig::default();
    config.ssl_mode = crate::postgres::SslMode::Disable;
    config.table_exclude = vec!["public.users".to_string()];
    let mut src = PostgresCdcSource::new(config, None);
    src.state = ConnectorState::Running;
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "orders",
        &[(1, "id", INT4_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        101,
        "public",
        "users",
        &[(1, "id", INT4_OID, -1)],
    ));

    for (xid, relation, id, commit_lsn) in [
        (1, 100, "1", 0x100),
        (2, 101, "2", 0x200),
        (3, 100, "3", 0x300),
    ] {
        src.enqueue_wal_data(PostgresCdcSource::build_begin_message(
            commit_lsn - 1,
            0,
            xid,
        ));
        src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
            relation,
            &[Some(id)],
        ));
        if xid == 1 {
            src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
                relation,
                &[Some("11")],
            ));
        }
        src.enqueue_wal_data(PostgresCdcSource::build_commit_message(
            commit_lsn - 1,
            commit_lsn,
            0,
        ));
    }

    let first = src.poll_batch(1).await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
    let second = src.poll_batch(1).await.unwrap().unwrap();
    assert_eq!(second.num_rows(), 1);
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/300"));
}

// ── Update with old tuple ──

#[tokio::test]
async fn test_process_update() {
    let mut src = running_source();

    let rel_msg = PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
    );
    src.enqueue_wal_data(rel_msg);

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_update_message(
        16384,
        b'O',
        &[Some("42"), Some("Alice")],
        &[Some("42"), Some("Bob")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);

    let op_col = batch.records.column(1).as_string::<i32>();
    assert_eq!(op_col.value(0), "U");

    let before = batch.records.column(4).as_string::<i32>();
    let before: serde_json::Value = serde_json::from_str(before.value(0)).unwrap();
    assert_eq!(before["id"], "42");
    assert_eq!(before["name"], "Alice");

    let after = batch.records.column(5).as_string::<i32>();
    let after: serde_json::Value = serde_json::from_str(after.value(0)).unwrap();
    assert_eq!(after["id"], "42");
    assert_eq!(after["name"], "Bob");
}

#[tokio::test]
async fn key_update_before_image_omits_non_identity_fields() {
    let mut src = running_source();
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_update_message(
        16384,
        b'K',
        &[Some("41"), None],
        &[Some("42"), Some("Alice")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    let before = batch.records.column(4).as_string::<i32>();
    let before: serde_json::Value = serde_json::from_str(before.value(0)).unwrap();
    assert_eq!(before["id"], "41");
    assert!(before.get("name").is_none());
}

// ── Delete ──

#[tokio::test]
async fn test_process_delete() {
    let mut src = running_source();

    let rel_msg = PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1), (0, "name", TEXT_OID, -1)],
    );
    src.enqueue_wal_data(rel_msg);

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_delete_message(
        16384,
        &[Some("42"), None],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    let op_col = batch.records.column(1).as_string::<i32>();
    assert_eq!(op_col.value(0), "D");

    let before = batch.records.column(4).as_string::<i32>();
    let before: serde_json::Value = serde_json::from_str(before.value(0)).unwrap();
    assert_eq!(before["id"], "42");
    assert!(before.get("name").is_none());
    assert!(batch.records.column(5).is_null(0));
}

// ── Table filtering ──

#[tokio::test]
async fn test_table_exclude_filter() {
    let mut config = PostgresCdcConfig::default();
    config.table_exclude = vec!["public.users".to_string()];
    let mut src = PostgresCdcSource::new(config, None);
    src.state = ConnectorState::Running;

    let rel_msg = PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1)],
    );
    src.enqueue_wal_data(rel_msg);

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(16384, &[Some("1")]));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let result = src.poll_batch(100).await.unwrap();
    assert!(result.is_none()); // filtered out
    assert_eq!(src.checkpoint().get_offset("lsn"), Some("0/200"));
}

#[tokio::test]
async fn public_qualified_include_matches_runtime_table_name() {
    let mut config = PostgresCdcConfig::default();
    config.table_include = vec!["public.users".to_string()];
    let mut src = PostgresCdcSource::new(config, None);
    src.state = ConnectorState::Running;
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        16_384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
        16_384,
        &[Some("1")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    let table = batch.records.column(0).as_string::<i32>();
    assert_eq!(table.value(0), "public.users");
}

// ── Max poll records batching ──

#[tokio::test]
async fn test_poll_batch_honors_engine_limit() {
    let mut src = running_source();

    // Inject 5 events directly
    for i in 0..5 {
        src.inject_event(ChangeEvent {
            table: "t".to_string(),
            op: CdcOperation::Insert,
            lsn: Lsn::new(i as u64),
            ts_ms: 0,
            before: None,
            after: Some(format!("{{\"id\":\"{i}\"}}")),
        });
    }

    // Poll only 2
    let batch = src.poll_batch(2).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(src.buffered_events(), 3);

    // Poll remaining
    let batch = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(src.buffered_events(), 0);
}

// ── Replication lag ──

#[test]
fn test_replication_lag() {
    let mut src = default_source();
    src.write_lsn = Lsn::new(1000);
    src.confirmed_flush_lsn = Lsn::new(500);
    assert_eq!(src.replication_lag_bytes(), 500);
}

// ── Unknown relation ID ──

#[tokio::test]
async fn test_unknown_relation_error() {
    let mut src = running_source();

    // Insert without prior Relation message
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(99999, &[Some("1")]));

    let result = src.poll_batch(100).await;
    assert!(result.is_err());
}

// ── Multi-table in one transaction ──

#[tokio::test]
async fn test_multi_table_transaction() {
    let mut src = running_source();

    // Two relations
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "users",
        &[(1, "id", INT4_OID, -1)],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        200,
        "public",
        "orders",
        &[(1, "order_id", INT4_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x500, 0, 5));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
        200,
        &[Some("1001")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x500, 0x600, 0));

    let batch = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let table_col = batch.records.column(0).as_string::<i32>();
    assert_eq!(table_col.value(0), "public.users");
    assert_eq!(table_col.value(1), "public.orders");
}

// ── Relation cache update (schema change) ──

#[tokio::test]
async fn test_schema_change_mid_stream() {
    let mut src = running_source();

    // Initial schema: 1 column
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "users",
        &[(1, "id", INT4_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x200, 0));

    let batch1 = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch1.num_rows(), 1);

    // Schema changes: add a column
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "users",
        &[(1, "id", INT4_OID, -1), (0, "email", TEXT_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x200, 0, 2));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(
        100,
        &[Some("2"), Some("alice@example.com")],
    ));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x200, 0x300, 0));

    let batch2 = src.poll_batch(100).await.unwrap().unwrap();
    assert_eq!(batch2.num_rows(), 1);

    // Verify the new column appears in JSON
    let after_col = batch2.records.column(5).as_string::<i32>();
    let json: serde_json::Value = serde_json::from_str(after_col.value(0)).unwrap();
    assert_eq!(json["email"], "alice@example.com");
}

// ── Write LSN advances on commit ──

#[tokio::test]
async fn test_write_lsn_advances() {
    let mut src = running_source();

    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "t",
        &[(1, "id", INT4_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x500, 0));

    let _ = src.poll_batch(100).await;
    assert_eq!(src.write_lsn().as_u64(), 0x500);
}

// ── TRUNCATE returns error ──

#[tokio::test]
async fn test_truncate_returns_error() {
    let mut src = running_source();

    // Register relation so the error message includes the table name.
    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        16384,
        "public",
        "users",
        &[(1, "id", INT8_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_truncate_message(&[16384], 0));

    let result = src.poll_batch(100).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("TRUNCATE"),
        "error should mention TRUNCATE: {err}"
    );
    assert!(
        err.contains("users"),
        "error should mention table name: {err}"
    );
}

#[tokio::test]
async fn test_truncate_unknown_relation_uses_oid() {
    let mut src = running_source();

    // No relation registered for ID 99999
    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_truncate_message(&[99999], 0));

    let result = src.poll_batch(100).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("oid:99999"), "error should mention oid: {err}");
}

// ── confirmed_flush_lsn not advanced until checkpoint ──

#[tokio::test]
async fn test_confirmed_lsn_not_advanced_until_checkpoint() {
    let mut src = running_source();

    src.enqueue_wal_data(PostgresCdcSource::build_relation_message(
        100,
        "public",
        "t",
        &[(1, "id", INT4_OID, -1)],
    ));

    src.enqueue_wal_data(PostgresCdcSource::build_begin_message(0x100, 0, 1));
    src.enqueue_wal_data(PostgresCdcSource::build_insert_message(100, &[Some("1")]));
    src.enqueue_wal_data(PostgresCdcSource::build_commit_message(0x100, 0x500, 0));

    // Before poll: confirmed_flush_lsn is ZERO.
    assert!(src.confirmed_flush_lsn().is_zero());

    // After poll: confirmed_flush_lsn must NOT have advanced.
    let _ = src.poll_batch(100).await.unwrap().unwrap();
    assert!(
        src.confirmed_flush_lsn().is_zero(),
        "confirmed_flush_lsn should not advance on poll, got {}",
        src.confirmed_flush_lsn()
    );

    // polled_lsn should have advanced.
    assert_eq!(src.polled_lsn.as_u64(), 0x500);

    // After checkpoint: the checkpoint offset should use polled_lsn.
    let cp = src.checkpoint();
    assert_eq!(cp.get_offset("lsn"), Some("0/500"));
}

// ── Resume identity validation ──

#[tokio::test]
async fn test_resume_rejects_slot_identity_mismatch() {
    let mut src = default_source();
    let mut cp = committed_lsn_checkpoint("2/FF00");
    cp.set_metadata("slot_name", "different_slot");

    let error = src
        .start(
            SourceStart::new(
                ConnectorConfig::new("postgres-cdc"),
                SourcePosition::Resume {
                    attempt: laminar_core::state::CheckpointAttempt::new(1, 1),
                    checkpoint: cp,
                },
                crate::connector::DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .expect_err("checkpoint for another slot must fail closed");
    assert!(error.to_string().contains("different_slot"));
    assert_eq!(src.state, ConnectorState::Created);
}

// ── Backpressure (no event dropping) ──

#[tokio::test]
async fn test_backpressure_does_not_drop_buffered_events() {
    let mut src = running_source();

    // Inject 200 events directly into the event buffer.
    // With backpressure, existing buffered events are never dropped —
    // only channel draining is paused when the buffer exceeds the
    // high watermark. Direct-injected events are already in the buffer.
    for i in 0..200u64 {
        src.inject_event(ChangeEvent {
            table: "public.t".to_string(),
            op: CdcOperation::Insert,
            before: None,
            after: Some(format!("{{\"id\": {i}}}")),
            ts_ms: i as i64,
            lsn: Lsn::new(i),
        });
    }
    assert_eq!(src.buffered_events(), 200);

    // poll_batch drains events from the buffer — no dropping.
    let batch = src.poll_batch(50).await.unwrap().unwrap();
    assert_eq!(batch.records.num_rows(), 50);
    // 200 - 50 drained = 150 remaining. No events dropped.
    assert_eq!(src.buffered_events(), 150);
}
