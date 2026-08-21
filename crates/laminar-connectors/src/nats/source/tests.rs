use super::*;
use arrow_schema::Schema;

struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

async fn pending_task(
    started: tokio::sync::oneshot::Sender<()>,
    dropped: tokio::sync::oneshot::Sender<()>,
    release: tokio::sync::oneshot::Receiver<()>,
) {
    let _drop_signal = DropSignal(Some(dropped));
    let _ = started.send(());
    let _ = release.await;
}

#[test]
fn source_contract_is_ephemeral_even_for_jetstream_config() {
    let source = NatsSource::new(Arc::new(Schema::empty()), None);
    let mut config = ConnectorConfig::new("nats");
    config.set("mode", "jetstream");
    let contract = source.contract(&config).expect("static NATS contract");
    assert_eq!(contract.consistency, SourceConsistency::Ephemeral);
    assert_eq!(contract.topology, SourceTopology::Singleton);

    config.set("format", "debezium");
    assert_eq!(
        source.contract(&config).unwrap().input_mode,
        SourceInputMode::KeyedUpsert
    );
}

#[test]
fn ephemeral_source_checkpoint_has_no_protocol_state() {
    let src = NatsSource::new(Arc::new(Schema::empty()), None);
    assert!(src.checkpoint().is_empty());
    assert_eq!(
        src.cancellation_policy(),
        crate::connector::ConnectorCancellationPolicy::RetireConnector
    );
}

#[test]
fn task_tracker_notifies_waiters_on_another_runtime() {
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (tracker_tx, tracker_rx) = std::sync::mpsc::sync_channel(1);
    let owner_thread = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let mut source = NatsSource::new(Arc::new(Schema::empty()), None);
                let terminal = source.terminal_task_tracker().unwrap();
                let owner_waiter = terminal.clone();
                let (_, rx) = mpsc::bounded_async::<Incoming>(1);
                let (shutdown, _) = watch::channel(false);
                let reader =
                    TrackedTask::spawn(&source.task_owner, "cross-runtime-reader", async move {
                        let _ = release_rx.await;
                    })
                    .unwrap();
                source.running = Some(Running {
                    deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
                    rx: Some(rx),
                    shutdown,
                    reader,
                    ack_runtime: None,
                });
                tracker_tx.send(terminal).unwrap();
                drop(source);
                owner_waiter.wait_terminated().await;
            });
    });

    let terminal = tracker_rx.recv().unwrap();
    assert!(!terminal.is_terminated());
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            release_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
                .await
                .expect("cross-runtime tracker waiter was not notified");
        });
    owner_thread.join().unwrap();
}

#[tokio::test]
async fn disconnected_reader_drains_queued_payload_before_terminal_error() {
    let mut src = NatsSource::new(Arc::new(Schema::empty()), None);
    let (tx, rx) = mpsc::bounded_async::<Incoming>(2);
    assert!(tx
        .try_send(Incoming {
            payload: Bytes::from_static(b"one"),
            ack: None,
        })
        .is_ok());
    drop(tx);
    let (shutdown, _) = watch::channel(false);
    let reader = TrackedTask::spawn(&src.task_owner, "test-reader", async {}).unwrap();
    src.running = Some(Running {
        deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
        rx: Some(rx),
        shutdown,
        reader,
        ack_runtime: None,
    });

    let final_batch = src.poll_batch(10).await.unwrap().unwrap();
    assert_eq!(final_batch.records.num_rows(), 1);

    let error = src.poll_batch(10).await.unwrap_err();
    assert!(matches!(error, ConnectorError::ReadError(_)));
    assert!(error.to_string().contains("reader task terminated"));
}

#[tokio::test]
async fn dropping_source_signals_and_reaps_the_owned_reader() {
    let mut source = NatsSource::new(Arc::new(Schema::empty()), None);
    let terminal = source.terminal_task_tracker().unwrap();
    let (_, rx) = mpsc::bounded_async::<Incoming>(1);
    let (shutdown, mut task_shutdown) = watch::channel(false);
    let shutdown_observer = task_shutdown.clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
    let reader = TrackedTask::spawn(&source.task_owner, "test-reader", async move {
        let _drop_signal = DropSignal(Some(dropped_tx));
        let _ = started_tx.send(());
        let _ = task_shutdown.changed().await;
    })
    .unwrap();
    source.running = Some(Running {
        deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
        rx: Some(rx),
        shutdown,
        reader,
        ack_runtime: None,
    });
    started_rx.await.expect("reader task started");

    drop(source);

    assert!(*shutdown_observer.borrow(), "drop must publish shutdown");
    tokio::time::timeout(Duration::from_secs(1), dropped_rx)
        .await
        .expect("reader must observe shutdown on drop")
        .expect("reader drop signal");
    tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
        .await
        .expect("reader and its tracked reaper must terminate");
}

#[tokio::test]
async fn normal_close_joins_reader_and_ack_tasks() {
    let mut source = NatsSource::new(Arc::new(Schema::empty()), None);
    let (_, rx) = mpsc::bounded_async::<Incoming>(1);
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let (reader_started_tx, reader_started_rx) = tokio::sync::oneshot::channel();
    let (reader_dropped_tx, reader_dropped_rx) = tokio::sync::oneshot::channel();
    let reader = TrackedTask::spawn(&source.task_owner, "test-reader", async move {
        let _drop_signal = DropSignal(Some(reader_dropped_tx));
        let _ = reader_started_tx.send(());
        let _ = shutdown_rx.changed().await;
    })
    .unwrap();

    let (ack_tx, mut ack_rx) = tokio_mpsc::channel::<jetstream::Message>(1);
    let (ack_shutdown, _) = watch::channel(false);
    let (ack_started_tx, ack_started_rx) = tokio::sync::oneshot::channel();
    let (ack_dropped_tx, ack_dropped_rx) = tokio::sync::oneshot::channel();
    let ack_task = TrackedTask::spawn(&source.task_owner, "test-ack", async move {
        let _drop_signal = DropSignal(Some(ack_dropped_tx));
        let _ = ack_started_tx.send(());
        while ack_rx.recv().await.is_some() {}
    })
    .unwrap();

    source.running = Some(Running {
        deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
        rx: Some(rx),
        shutdown,
        reader,
        ack_runtime: Some(AckRuntime {
            tx: Some(ack_tx),
            shutdown: ack_shutdown,
            task: ack_task,
        }),
    });
    reader_started_rx.await.expect("reader task started");
    ack_started_rx.await.expect("ack task started");

    tokio::time::timeout(Duration::from_secs(1), source.close())
        .await
        .expect("normal close must join owned tasks")
        .unwrap();

    for (name, dropped) in [("reader", reader_dropped_rx), ("ack", ack_dropped_rx)] {
        tokio::time::timeout(Duration::from_secs(1), dropped)
            .await
            .unwrap_or_else(|_| panic!("{name} task was not joined"))
            .unwrap_or_else(|_| panic!("{name} drop signal closed"));
    }
    assert!(source.running.is_none());
}

#[tokio::test]
async fn cancelling_close_does_not_detach_reader_or_ack_tasks() {
    let mut source = NatsSource::new(Arc::new(Schema::empty()), None);
    let terminal = source.terminal_task_tracker().unwrap();
    let (_, rx) = mpsc::bounded_async::<Incoming>(1);
    let (shutdown, shutdown_rx) = watch::channel(false);
    let (reader_started_tx, reader_started_rx) = tokio::sync::oneshot::channel();
    let (reader_dropped_tx, reader_dropped_rx) = tokio::sync::oneshot::channel();
    let (reader_release_tx, reader_release_rx) = tokio::sync::oneshot::channel();
    let reader = TrackedTask::spawn(
        &source.task_owner,
        "test-reader",
        pending_task(reader_started_tx, reader_dropped_tx, reader_release_rx),
    )
    .unwrap();

    let (ack_tx, ack_rx) = tokio_mpsc::channel::<jetstream::Message>(1);
    let (ack_shutdown, _) = watch::channel(false);
    let (ack_started_tx, ack_started_rx) = tokio::sync::oneshot::channel();
    let (ack_dropped_tx, ack_dropped_rx) = tokio::sync::oneshot::channel();
    let (ack_release_tx, ack_release_rx) = tokio::sync::oneshot::channel();
    let ack_task = TrackedTask::spawn(&source.task_owner, "test-ack", async move {
        let _ack_rx = ack_rx;
        pending_task(ack_started_tx, ack_dropped_tx, ack_release_rx).await;
    })
    .unwrap();
    source.running = Some(Running {
        deserializer: serde::create_deserializer(serde::Format::Raw).unwrap(),
        rx: Some(rx),
        shutdown,
        reader,
        ack_runtime: Some(AckRuntime {
            tx: Some(ack_tx),
            shutdown: ack_shutdown,
            task: ack_task,
        }),
    });
    reader_started_rx.await.expect("reader task started");
    ack_started_rx.await.expect("ack task started");

    let close = tokio::spawn(async move { source.close().await });
    tokio::task::yield_now().await;
    assert!(!close.is_finished(), "close must be waiting for the reader");
    close.abort();
    assert!(close
        .await
        .expect_err("close waiter cancelled")
        .is_cancelled());

    assert!(
        *shutdown_rx.borrow(),
        "cancelling close must publish shutdown"
    );
    assert!(
        !terminal.is_terminated(),
        "task guards must keep a cancelled generation non-terminal"
    );
    reader_release_tx.send(()).expect("release reader");
    ack_release_tx.send(()).expect("release ack worker");

    for (name, dropped) in [("reader", reader_dropped_rx), ("ack", ack_dropped_rx)] {
        tokio::time::timeout(Duration::from_secs(1), dropped)
            .await
            .unwrap_or_else(|_| panic!("{name} task remained detached"))
            .unwrap_or_else(|_| panic!("{name} drop signal closed"));
    }
    tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
        .await
        .expect("generation must become terminal after every owned task exits");
}

#[tokio::test]
async fn ack_shutdown_discards_queued_but_unstarted_work() {
    let (tx, rx) = tokio_mpsc::channel(8);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (admitted_tx, mut admitted_rx) = tokio_mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let worker_release = Arc::clone(&release);
    let worker = tokio::spawn(run_bounded_queue(rx, shutdown_rx, 1, move |message| {
        let admitted_tx = admitted_tx.clone();
        let release = Arc::clone(&worker_release);
        async move {
            admitted_tx.send(message).unwrap();
            release.notified().await;
        }
    }));

    for message in 1..=3 {
        tx.send(message).await.unwrap();
    }
    assert_eq!(admitted_rx.recv().await, Some(1));

    shutdown_tx.send_replace(true);
    drop(tx);
    tokio::task::yield_now().await;
    release.notify_one();

    let abandoned = tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("worker shutdown must be bounded by admitted work")
        .unwrap();
    assert_eq!(abandoned, 2);
    assert!(admitted_rx.try_recv().is_err());
}

#[test]
fn backoff_base_grows_then_caps_at_5s() {
    assert_eq!(fetch_backoff_base(1), Duration::from_millis(500));
    assert_eq!(fetch_backoff_base(2), Duration::from_millis(1000));
    assert_eq!(fetch_backoff_base(3), Duration::from_millis(2000));
    assert_eq!(fetch_backoff_base(4), Duration::from_millis(4000));
    assert_eq!(fetch_backoff_base(5), Duration::from_millis(5000));
    assert_eq!(fetch_backoff_base(100), Duration::from_millis(5000));
}

#[test]
fn jitter_stays_within_plus_minus_20_percent() {
    let base = Duration::from_millis(1000);
    for entropy in [0u64, 1, 99, 12345, u64::MAX] {
        let j = with_jitter(base, entropy);
        assert!(
            j >= Duration::from_millis(800) && j <= Duration::from_millis(1200),
            "entropy {entropy}: jittered = {j:?} outside ±20% of {base:?}"
        );
    }
}
