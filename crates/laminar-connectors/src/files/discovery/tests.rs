use super::*;
use crate::connector::ConnectorTaskOwner;
use crate::files::manifest::FileIngestionManifest;

#[test]
fn test_split_dir_and_glob() {
    let (dir, glob) = split_dir_and_glob("/data/logs/*.csv");
    assert_eq!(dir, "/data/logs");
    assert_eq!(glob.as_deref(), Some("*.csv"));

    let (dir, glob) = split_dir_and_glob("/data/logs");
    assert_eq!(dir, "/data/logs");
    assert!(glob.is_none());

    let (dir, glob) = split_dir_and_glob("/data/logs/events_*.json");
    assert_eq!(dir, "/data/logs");
    assert_eq!(glob.as_deref(), Some("events_*.json"));
}

#[test]
fn test_should_use_poll_on_local() {
    // On non-Linux or local FS, should return false.
    assert!(!should_use_poll_watcher("/tmp"));
}

#[tokio::test]
async fn terminal_discovery_failure_is_observable() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    let config = DiscoveryConfig {
        path: missing.to_string_lossy().into_owned(),
        poll_interval: Duration::from_secs(60),
        stabilisation_delay: Duration::from_secs(1),
        glob_pattern: None,
    };
    let known = Arc::new(FileIngestionManifest::new().snapshot_for_dedup());
    let (owner, _tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().unwrap();
    let mut engine = FileDiscoveryEngine::start(config, known, guard, owner.track().unwrap());
    let error = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match engine.drain(10).await {
                Ok(files) => {
                    assert!(files.is_empty());
                    tokio::task::yield_now().await;
                }
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("discovery failure was not published");
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("is not a directory"));
}

#[tokio::test]
async fn generation_stays_live_until_discovery_task_exits() {
    let directory = tempfile::tempdir().unwrap();
    let config = DiscoveryConfig {
        path: directory.path().to_string_lossy().into_owned(),
        poll_interval: Duration::from_secs(60),
        stabilisation_delay: Duration::from_secs(60),
        glob_pattern: None,
    };
    let known = Arc::new(FileIngestionManifest::new().snapshot_for_dedup());
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().unwrap();
    let mut engine = FileDiscoveryEngine::start(config, known, guard, owner.track().unwrap());

    tokio::task::yield_now().await;
    drop(owner);
    assert!(
        !tracker.is_terminated(),
        "the live discovery child must retain its generation"
    );

    engine
        .abort_and_join_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("discovery task did not release its generation guard");
}

#[tokio::test]
async fn initial_scan_larger_than_event_channel_does_not_deadlock() {
    const FILE_COUNT: usize = 600;

    let directory = tempfile::tempdir().unwrap();
    for index in 0..FILE_COUNT {
        std::fs::write(directory.path().join(format!("{index:04}.txt")), b"x").unwrap();
    }
    let config = DiscoveryConfig {
        path: directory.path().to_string_lossy().into_owned(),
        poll_interval: Duration::from_secs(60),
        stabilisation_delay: Duration::ZERO,
        glob_pattern: Some("*.txt".into()),
    };
    let known = Arc::new(FileIngestionManifest::new().snapshot_for_dedup());
    let (owner, _tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().unwrap();
    let mut engine = FileDiscoveryEngine::start(config, known, guard, owner.track().unwrap());

    let discovered = tokio::time::timeout(Duration::from_secs(5), async {
        let mut paths = std::collections::BTreeSet::new();
        while paths.len() < FILE_COUNT {
            for file in engine.drain(128).await.unwrap() {
                paths.insert(file.path);
            }
            tokio::task::yield_now().await;
        }
        paths
    })
    .await
    .expect("initial scan stalled above the former 512-entry channel bound");
    assert_eq!(discovered.len(), FILE_COUNT);
    engine
        .abort_and_join_until(tokio::time::Instant::now() + Duration::from_secs(1))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_join_retains_handle_and_generation_guard() {
    let (_tx, rx) = mpsc::bounded_async::<DiscoveredFile>(1);
    let (owner, tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let _guard = guard;
        let _ = started_tx.send(());
        std::thread::sleep(Duration::from_millis(100));
        Ok(())
    });
    let mut engine = FileDiscoveryEngine {
        rx,
        handle: Some(handle),
        initial_scan_handle: None,
        terminal_error: None,
    };
    started_rx.await.unwrap();
    drop(owner);

    let error = engine
        .abort_and_join_until(tokio::time::Instant::now() + Duration::from_millis(5))
        .await
        .expect_err("blocked task must exceed the join deadline");
    assert!(matches!(error, ConnectorError::Timeout(_)));
    assert!(
        engine.handle.is_some(),
        "timed-out join handle was detached"
    );
    assert!(!tracker.is_terminated(), "active task guard was lost");

    drop(engine);
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("blocked discovery task did not release its generation guard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_initial_scan_retains_handle_and_generation_guard() {
    let (_tx, rx) = mpsc::bounded_async::<DiscoveredFile>(1);
    let (owner, tracker) = ConnectorTaskOwner::new();
    let scan_guard = owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let initial_scan_handle = tokio::task::spawn_blocking(move || {
        let _scan_guard = scan_guard;
        let _ = started_tx.send(());
        std::thread::sleep(Duration::from_millis(100));
    });
    let handle = tokio::spawn(async {
        std::future::pending::<()>().await;
        Ok(())
    });
    let mut engine = FileDiscoveryEngine {
        rx,
        handle: Some(handle),
        initial_scan_handle: Some(initial_scan_handle),
        terminal_error: None,
    };
    started_rx.await.unwrap();
    drop(owner);

    let error = engine
        .abort_and_join_until(tokio::time::Instant::now() + Duration::from_millis(5))
        .await
        .expect_err("blocked initial scan must exceed the join deadline");
    assert!(matches!(error, ConnectorError::Timeout(_)));
    assert!(
        engine.initial_scan_handle.is_some(),
        "timed-out initial scan handle was detached"
    );
    assert!(!tracker.is_terminated(), "initial scan guard was lost");

    drop(engine);
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("blocked initial scan did not release its generation guard");
}

#[tokio::test(flavor = "current_thread")]
async fn late_blocking_completion_exceeds_the_absolute_close_deadline() {
    let (_tx, rx) = mpsc::bounded_async::<DiscoveredFile>(1);
    let (owner, _tracker) = ConnectorTaskOwner::new();
    let guard = owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
    let handle = tokio::spawn(async move {
        let _guard = guard;
        let _ = started_tx.send(());
        std::thread::sleep(Duration::from_millis(25));
        Ok(())
    });
    let mut engine = FileDiscoveryEngine {
        rx,
        handle: Some(handle),
        initial_scan_handle: None,
        terminal_error: None,
    };
    started_rx.await.unwrap();
    assert!(tokio::time::Instant::now() >= deadline);

    let error = engine
        .abort_and_join_until(deadline)
        .await
        .expect_err("late discovery completion must miss its absolute deadline");
    assert!(matches!(error, ConnectorError::Timeout(_)));
}
