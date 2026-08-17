use super::*;
use crate::connector::DeliveryGuarantee;

fn start_request() -> SourceStart {
    let mut config = ConnectorConfig::new("otel");
    config.set("bind.address", "127.0.0.1");
    config.set("port", "0");
    SourceStart::new(
        config,
        SourcePosition::Initial,
        DeliveryGuarantee::BestEffort,
    )
    .unwrap()
}

#[tokio::test]
async fn clean_close_can_restart_the_same_tracked_generation() {
    let mut source = OtelSource::new(traces_schema(), None);
    let terminal = source.terminal_task_tracker().unwrap();

    source.start(start_request()).await.unwrap();
    assert!(source.server_task.is_some());
    source.close().await.unwrap();
    assert!(source.server_task.is_none());
    assert!(!terminal.is_terminated(), "the generation owner is live");

    source.start(start_request()).await.unwrap();
    assert!(source.server_task.is_some());
    drop(source);

    tokio::time::timeout(Duration::from_secs(2), terminal.wait_terminated())
        .await
        .expect("restarted OTel server generation did not terminate");
}

#[tokio::test]
async fn aborted_close_waiter_aborts_the_owned_server_task() {
    let mut source = OtelSource::new(traces_schema(), None);
    let terminal = source.terminal_task_tracker().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    source.server_task = Some(
        TrackedServerTask::spawn(
            &source.task_owner,
            Arc::clone(&source.data_ready),
            async move {
                let _ = started_tx.send(());
                let _ = release_rx.await;
                Ok(())
            },
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    source.shutdown_tx = Some(shutdown_tx);
    source.state = ConnectorState::Running;
    started_rx.await.expect("test server task started");

    let close = tokio::spawn(async move { source.close().await });
    tokio::task::yield_now().await;
    assert!(!close.is_finished(), "close must be joining the server");
    close.abort();
    assert!(
        close
            .await
            .expect_err("close waiter cancelled")
            .is_cancelled(),
        "close waiter must be cancelled"
    );

    assert!(*shutdown_rx.borrow(), "close must publish shutdown");
    tokio::time::timeout(Duration::from_secs(2), terminal.wait_terminated())
        .await
        .expect("cancelled close left the server generation live");
    assert!(
        release_tx.send(()).is_err(),
        "the aborted server must drop its test receiver"
    );
}

#[tokio::test(start_paused = true)]
async fn close_timeout_aborts_the_server_task() {
    let mut source = OtelSource::new(traces_schema(), None);
    let terminal = source.terminal_task_tracker().unwrap();
    source.server_task = Some(
        TrackedServerTask::spawn(
            &source.task_owner,
            Arc::clone(&source.data_ready),
            std::future::pending::<Result<(), ConnectorError>>(),
        )
        .unwrap(),
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    source.shutdown_tx = Some(shutdown_tx);
    source.state = ConnectorState::Running;

    let error = source.close().await.unwrap_err();

    assert!(*shutdown_rx.borrow(), "close must publish shutdown");
    assert!(error.to_string().contains("close deadline"), "{error}");
    assert_eq!(source.state, ConnectorState::Failed);
    assert!(
        source.server_task.is_none(),
        "the aborted task must not block restart"
    );
    assert!(matches!(
        source.start(start_request()).await,
        Err(ConnectorError::InvalidState { .. })
    ));
    drop(source);
    tokio::time::timeout(Duration::from_secs(1), terminal.wait_terminated())
        .await
        .expect("aborted OTel server generation did not terminate");
}

#[tokio::test]
async fn late_server_completion_is_a_close_timeout() {
    let owner = ConnectorTaskOwner::new().0;
    let mut task = TrackedServerTask::spawn(&owner, Arc::new(Notify::new()), async {
        std::thread::sleep(Duration::from_millis(25));
        Ok(())
    })
    .unwrap();

    let result = task
        .wait_until(tokio::time::Instant::now() + Duration::from_millis(5))
        .await;

    assert!(matches!(result, ServerWait::TimedOut));
    assert!(task.handle.is_none());
}

#[tokio::test]
async fn unexpected_server_exit_fails_live_polling() {
    let mut source = OtelSource::new(traces_schema(), None);
    let (_batch_tx, batch_rx) = mpsc::bounded_async::<RecordBatch>(1);
    source.batch_rx = Some(batch_rx);
    source.server_task = Some(
        TrackedServerTask::spawn(&source.task_owner, Arc::clone(&source.data_ready), async {
            Err(ConnectorError::ConnectionFailed(
                "accept loop failed".into(),
            ))
        })
        .unwrap(),
    );
    source.state = ConnectorState::Running;
    tokio::time::timeout(Duration::from_secs(1), async {
        while !source
            .server_task
            .as_ref()
            .and_then(|task| task.handle.as_ref())
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("test server failure did not become observable");
    tokio::time::timeout(Duration::from_secs(1), source.data_ready.notified())
        .await
        .expect("terminal server exit did not wake source polling");

    let error = source.poll_batch(1).await.unwrap_err();

    assert!(error.to_string().contains("accept loop failed"), "{error}");
    assert!(
        !error.is_transient(),
        "terminal generation errors must not retry"
    );
    assert_eq!(source.state, ConnectorState::Failed);
    assert!(source.server_task.is_none());
}
