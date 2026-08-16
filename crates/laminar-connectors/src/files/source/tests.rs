use super::*;
use crate::connector::{DeliveryGuarantee, SourcePosition, SourceStart};
use laminar_core::checkpoint::CheckpointAttempt;
use std::collections::BTreeMap;
use tokio::sync::Notify;

struct TestFileReader {
    files: BTreeMap<String, Vec<u8>>,
    blocked_path: Option<String>,
    read_started: Notify,
    release_read: Notify,
}

struct BlockingChildFileReader {
    bytes: Vec<u8>,
    started: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

#[async_trait]
impl FileReader for TestFileReader {
    async fn read(
        &self,
        path: &str,
        _task_guard: ConnectorTaskGuard,
    ) -> Result<Vec<u8>, ConnectorError> {
        if self.blocked_path.as_deref() == Some(path) {
            self.read_started.notify_one();
            self.release_read.notified().await;
        }
        self.files
            .get(path)
            .cloned()
            .ok_or_else(|| ConnectorError::ReadError(format!("missing test file '{path}'")))
    }
}

#[async_trait]
impl FileReader for BlockingChildFileReader {
    async fn read(
        &self,
        _path: &str,
        task_guard: ConnectorTaskGuard,
    ) -> Result<Vec<u8>, ConnectorError> {
        let started = self.started.lock().unwrap().take().unwrap();
        let release = self.release.lock().unwrap().take().unwrap();
        let bytes = self.bytes.clone();
        tokio::task::spawn_blocking(move || {
            let _task_guard = task_guard;
            let _ = started.send(());
            let _ = release.recv();
            bytes
        })
        .await
        .map_err(|e| ConnectorError::ReadError(format!("test file worker failed: {e}")))
    }
}

fn start_request(config: ConnectorConfig, position: SourcePosition) -> SourceStart {
    SourceStart::new(config, position, DeliveryGuarantee::AtLeastOnce).unwrap()
}

async fn started_text_source(directory: &std::path::Path, position: SourcePosition) -> FileSource {
    let config = text_source_config(directory);
    let mut source = FileSource::new();
    source.start(start_request(config, position)).await.unwrap();
    source
}

fn text_source_config(directory: &std::path::Path) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("files");
    config.set("path", directory.to_string_lossy().to_string());
    config.set("format", "text");
    config.set("stabilisation_delay", "60s");
    config
}

async fn install_blocked_discovery(source: &mut FileSource, blocked_for: std::time::Duration) {
    source
        .discovery
        .as_mut()
        .unwrap()
        .abort_and_join_until(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await
        .unwrap();
    let guard = source.task_owner.track().unwrap();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let _guard = guard;
        let _ = started_tx.send(());
        std::thread::sleep(blocked_for);
        Ok(())
    });
    source
        .discovery
        .as_mut()
        .unwrap()
        .install_task_for_test(handle);
    started_rx.await.unwrap();
}

fn staged(path: &str, bytes: &[u8]) -> PendingFile {
    PendingFile {
        discovered: DiscoveredFile {
            path: path.into(),
            size: u64::try_from(bytes.len()).unwrap(),
            modified_ms: 1234,
        },
        resume: None,
    }
}

#[test]
fn test_file_source_default() {
    let source = FileSource::new();
    assert!(!source.is_open);
    assert_eq!(source.manifest.processed_count(), 0);
}

#[tokio::test]
async fn test_open_missing_path() {
    let mut source = FileSource::new();
    let config = ConnectorConfig::new("files");
    let result = source
        .start(start_request(config, SourcePosition::Initial))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_open_with_text_format() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = FileSource::new();
    let mut config = ConnectorConfig::new("files");
    config.set("path", directory.path().to_string_lossy().to_string());
    config.set("format", "text");
    let result = source
        .start(start_request(config, SourcePosition::Initial))
        .await;
    assert!(result.is_ok());
    assert!(source.is_open);
    assert_eq!(source.schema().field(0).name(), "line");
    source.close().await.unwrap();
}

#[tokio::test]
async fn start_while_open_is_rejected_before_request_validation() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = started_text_source(directory.path(), SourcePosition::Initial).await;
    let original_path = source.config.as_ref().unwrap().path.clone();

    let error = source
        .start(start_request(
            ConnectorConfig::new("files"),
            SourcePosition::Initial,
        ))
        .await
        .expect_err("an open source must reject a second start");
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert_eq!(source.config.as_ref().unwrap().path, original_path);
    assert!(source.is_open);
    source.close().await.unwrap();
}

#[tokio::test]
async fn discovery_failure_reaches_poll_and_retires_the_instance() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    let mut source = FileSource::new();
    source
        .start(start_request(
            text_source_config(&missing),
            SourcePosition::Initial,
        ))
        .await
        .unwrap();

    let error = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            match source.poll_batch(10).await {
                Ok(None) => tokio::task::yield_now().await,
                Ok(Some(batch)) => panic!("unexpected batch with {} rows", batch.num_rows()),
                Err(error) => break error,
            }
        }
    })
    .await
    .expect("discovery failure remained silent");
    assert!(matches!(error, ConnectorError::InvalidState { .. }));
    assert!(!error.is_transient());
    assert!(error.to_string().contains("is not a directory"));
    assert!(source.restart_forbidden);

    let retry_directory = tempfile::tempdir().unwrap();
    let retry_error = source
        .start(start_request(
            text_source_config(retry_directory.path()),
            SourcePosition::Initial,
        ))
        .await
        .expect_err("failed source instance must not restart");
    assert!(matches!(retry_error, ConnectorError::InvalidState { .. }));
}

#[tokio::test]
async fn clean_close_allows_same_instance_restart() {
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let mut source = started_text_source(first.path(), SourcePosition::Initial).await;
    source.close().await.unwrap();

    source
        .start(start_request(
            text_source_config(second.path()),
            SourcePosition::Initial,
        ))
        .await
        .unwrap();
    assert!(source.is_open);
    assert_eq!(
        source.config.as_ref().unwrap().path.as_str(),
        second.path().to_string_lossy().as_ref()
    );
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_close_retains_task_and_forbids_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = started_text_source(directory.path(), SourcePosition::Initial).await;
    install_blocked_discovery(&mut source, std::time::Duration::from_millis(250)).await;
    let terminal = source.terminal_task_tracker().unwrap();

    let error = source
        .close_until(tokio::time::Instant::now() + std::time::Duration::from_millis(5))
        .await
        .expect_err("blocked discovery must exceed the close deadline");
    assert!(matches!(error, ConnectorError::Timeout(_)));
    assert!(source.discovery.is_some());
    assert!(source.restart_forbidden);

    let restart = source
        .start(start_request(
            text_source_config(directory.path()),
            SourcePosition::Initial,
        ))
        .await;
    assert!(matches!(restart, Err(ConnectorError::InvalidState { .. })));
    drop(source);
    assert!(
        !terminal.is_terminated(),
        "timed-out close detached an active discovery task"
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("discovery task did not release its generation guard");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_close_retains_task_and_forbids_restart() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = started_text_source(directory.path(), SourcePosition::Initial).await;
    install_blocked_discovery(&mut source, std::time::Duration::from_millis(250)).await;
    let terminal = source.terminal_task_tracker().unwrap();

    {
        let close =
            source.close_until(tokio::time::Instant::now() + std::time::Duration::from_secs(1));
        tokio::pin!(close);
        tokio::select! {
            result = &mut close => panic!("blocked close unexpectedly completed: {result:?}"),
            () = tokio::time::sleep(std::time::Duration::from_millis(5)) => {}
        }
    }

    assert!(source.discovery.is_some());
    assert!(source.restart_forbidden);
    let restart = source
        .start(start_request(
            text_source_config(directory.path()),
            SourcePosition::Initial,
        ))
        .await;
    assert!(matches!(restart, Err(ConnectorError::InvalidState { .. })));
    drop(source);
    assert!(
        !terminal.is_terminated(),
        "cancelled close detached an active discovery task"
    );
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("discovery task did not release its generation guard");
}

#[tokio::test]
async fn test_poll_batch_when_not_started() {
    let mut source = FileSource::new();
    let result = source.poll_batch(100).await;
    assert!(result.is_err());
}

#[test]
fn test_checkpoint_roundtrip() {
    let mut source = FileSource::new();
    source.manifest.insert("test.csv".into());
    let cp = source.checkpoint();
    assert!(cp.get_offset("manifest").is_some());
    assert_eq!(
        cp.get_metadata("connector"),
        Some(FILE_CHECKPOINT_CONNECTOR)
    );
    assert_eq!(
        cp.get_metadata(CHECKPOINT_VERSION_METADATA),
        Some(FILE_CHECKPOINT_VERSION)
    );
}

#[tokio::test]
async fn test_resume_from_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = FileSource::new();

    // Build a checkpoint with manifest data.
    let mut cp = FileSource::new().checkpoint();
    cp.set_offset("manifest", r#"["a.csv"]"#);

    let mut config = ConnectorConfig::new("files");
    config.set("path", directory.path().to_string_lossy().to_string());
    config.set("format", "text");
    source
        .start(start_request(
            config,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(1, 1),
                checkpoint: cp,
            },
        ))
        .await
        .unwrap();
    assert_eq!(source.manifest.processed_count(), 1);
    assert!(source.manifest.contains("a.csv"));
    source.close().await.unwrap();
}

#[tokio::test]
async fn corrupt_resume_manifest_fails_before_discovery_starts() {
    let mut cp = FileSource::new().checkpoint();
    cp.set_offset("manifest", "{not-json");
    let mut config = ConnectorConfig::new("files");
    config.set("path", "/tmp");
    config.set("format", "text");
    let mut source = FileSource::new();
    let error = source
        .start(start_request(
            config,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::new(1, 1),
                checkpoint: cp,
            },
        ))
        .await
        .expect_err("corrupt durable manifest must fail closed");
    assert!(error.to_string().contains("invalid file manifest"));
    assert!(source.discovery.is_none());
    assert!(!source.is_open);
}

#[tokio::test]
async fn resume_rejects_wrong_checkpoint_identity_or_version() {
    let directory = tempfile::tempdir().unwrap();
    for (connector, version, expected) in [
        (
            "generator",
            FILE_CHECKPOINT_VERSION,
            "belongs to connector 'generator'",
        ),
        (
            FILE_CHECKPOINT_CONNECTOR,
            "0",
            "requires checkpoint.version=1",
        ),
    ] {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("manifest", "[]");
        checkpoint.set_metadata("connector", connector);
        checkpoint.set_metadata(CHECKPOINT_VERSION_METADATA, version);

        let mut source = FileSource::new();
        let error = source
            .start(start_request(
                text_source_config(directory.path()),
                SourcePosition::Resume {
                    attempt: CheckpointAttempt::canonical(7),
                    checkpoint,
                },
            ))
            .await
            .expect_err("non-current file checkpoint must be rejected");
        assert!(error.to_string().contains(expected), "{error}");
        assert!(source.discovery.is_none());
        assert!(!source.is_open);
    }
}

#[tokio::test]
async fn resume_rejects_unknown_file_progress_fields() {
    let directory = tempfile::tempdir().unwrap();
    let mut checkpoint = FileSource::new().checkpoint();
    checkpoint.set_offset(
            "file_progress",
            r#"{"path":"a.txt","size":1,"modified_ms":1,"content_sha256":"00","next_row":1,"legacy_cursor":true}"#,
        );
    let mut source = FileSource::new();
    let error = source
        .start(start_request(
            text_source_config(directory.path()),
            SourcePosition::Resume {
                attempt: CheckpointAttempt::canonical(8),
                checkpoint,
            },
        ))
        .await
        .expect_err("same-version unknown progress fields must fail closed");
    assert!(error.to_string().contains("unknown field"), "{error}");
    assert!(source.discovery.is_none());
    assert!(!source.is_open);
}

#[tokio::test]
async fn cancelled_poll_retains_unpublished_file_progress() {
    let directory = tempfile::tempdir().unwrap();
    let mut source = started_text_source(directory.path(), SourcePosition::Initial).await;
    let first_path = "staged-first.txt";
    let second_path = "staged-second.txt";
    let first = b"first\n".to_vec();
    let second = b"second\n".to_vec();
    let reader = Arc::new(TestFileReader {
        files: BTreeMap::from([
            (first_path.into(), first.clone()),
            (second_path.into(), second.clone()),
        ]),
        blocked_path: Some(second_path.into()),
        read_started: Notify::new(),
        release_read: Notify::new(),
    });
    source.reader = reader.clone();
    source.pending_files.push_back(staged(first_path, &first));
    source.pending_files.push_back(staged(second_path, &second));

    let batch = source.poll_batch(10).await.unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(source.manifest.contains(first_path));

    {
        let poll = source.poll_batch(10);
        tokio::pin!(poll);
        let read_started = reader.read_started.notified();
        tokio::pin!(read_started);
        tokio::select! {
            biased;
            result = &mut poll => panic!("blocked read unexpectedly completed: {result:?}"),
            () = &mut read_started => {}
        }
        // Dropping `poll` here models the runtime cancelling an in-flight
        // source poll to service shutdown/control work.
    }

    assert!(source.manifest.contains(first_path));
    assert!(!source.manifest.contains(second_path));
    assert_eq!(
        source
            .pending_files
            .front()
            .map(|pending| pending.discovered.path.as_str()),
        Some(second_path)
    );
    source.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_source_waits_for_inflight_file_read() {
    let directory = tempfile::tempdir().unwrap();
    let path = "blocked.txt";
    let bytes = b"blocked\n".to_vec();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let reader = Arc::new(BlockingChildFileReader {
        bytes: bytes.clone(),
        started: std::sync::Mutex::new(Some(started_tx)),
        release: std::sync::Mutex::new(Some(release_rx)),
    });

    let mut source = started_text_source(directory.path(), SourcePosition::Initial).await;
    let terminal = source
        .terminal_task_tracker()
        .expect("file source owns discovery and read tasks");
    source.reader = reader;
    source.pending_files.push_back(staged(path, &bytes));

    let caller = tokio::spawn(async move { source.poll_batch(10).await });
    started_rx.await.unwrap();
    caller.abort();
    let _ = caller.await;
    assert!(
        !terminal.is_terminated(),
        "retirement must retain the started filesystem child"
    );

    release_tx.send(()).unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        terminal.wait_terminated(),
    )
    .await
    .expect("file source generation did not reach terminal completion");
}

#[tokio::test]
async fn max_records_cursor_resumes_exactly_within_a_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = "bounded.txt";
    let bytes = b"one\ntwo\nthree\n".to_vec();
    let reader = Arc::new(TestFileReader {
        files: BTreeMap::from([(path.into(), bytes.clone())]),
        blocked_path: None,
        read_started: Notify::new(),
        release_read: Notify::new(),
    });

    let mut source = started_text_source(directory.path(), SourcePosition::Initial).await;
    source.reader = reader.clone();
    source.pending_files.push_back(staged(path, &bytes));
    let first = source.poll_batch(2).await.unwrap().unwrap();
    assert_eq!(first.num_rows(), 2);
    assert!(!source.manifest.contains(path));
    let checkpoint = source.checkpoint();
    let progress: FileProgress = serde_json::from_str(
        checkpoint
            .get_offset("file_progress")
            .expect("partial file cursor must be checkpointed"),
    )
    .unwrap();
    assert_eq!(progress.next_row, 2);
    source.close().await.unwrap();

    let mut resumed = started_text_source(
        directory.path(),
        SourcePosition::Resume {
            attempt: CheckpointAttempt::canonical(11),
            checkpoint,
        },
    )
    .await;
    resumed.reader = reader;
    let remaining = resumed.poll_batch(2).await.unwrap().unwrap();
    assert_eq!(remaining.num_rows(), 1);
    assert!(resumed.manifest.contains(path));
    assert!(resumed.checkpoint().get_offset("file_progress").is_none());
    resumed.close().await.unwrap();
}

#[test]
fn test_source_contract() {
    let source = FileSource::new();
    let contract = source
        .contract(&ConnectorConfig::new("files"))
        .expect("static file contract");
    assert_eq!(contract.consistency, SourceConsistency::Replayable);
    assert_eq!(contract.topology, SourceTopology::Singleton);
}
