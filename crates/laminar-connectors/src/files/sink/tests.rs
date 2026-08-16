use super::*;
use arrow_array::{Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema};

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn test_batch(schema: &SchemaRef) -> RecordBatch {
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

#[test]
fn row_encoder_cardinality_must_match_the_input() {
    validate_encoded_row_count(3, 3).unwrap();
    let short = validate_encoded_row_count(3, 2).unwrap_err();
    let long = validate_encoded_row_count(3, 4).unwrap_err();
    assert!(matches!(
        short,
        ConnectorError::Serde(crate::error::SerdeError::RecordCountMismatch {
            expected: 3,
            got: 2
        })
    ));
    assert!(matches!(
        long,
        ConnectorError::Serde(crate::error::SerdeError::RecordCountMismatch {
            expected: 3,
            got: 4
        })
    ));
}

fn test_config(out_path: &Path, format: &str) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("files");
    config.set("path", out_path.to_str().unwrap());
    config.set("format", format);
    config.set(
        "_arrow_schema",
        crate::config::encode_arrow_schema_ipc(test_schema().as_ref()),
    );
    config
}

fn final_files(out_path: &Path) -> Vec<PathBuf> {
    let mut files = std::fs::read_dir(out_path)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| !path.to_string_lossy().ends_with(".tmp"))
        .collect::<Vec<_>>();
    files.sort();
    files
}

#[test]
fn test_sink_default() {
    let sink = FileSink::new();
    assert!(!sink.is_open);
}

#[tokio::test]
async fn retired_generation_owns_uncooperative_blocking_child_until_exit() {
    let sink = Arc::new(FileSink::new());
    let tracker = sink
        .terminal_task_tracker()
        .expect("file sink owns blocking tasks");
    let child_sink = Arc::clone(&sink);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let caller = tokio::spawn(async move {
        child_sink
            .run_blocking(move || {
                let _ = started_tx.send(());
                let _ = release_rx.recv();
            })
            .await
    });
    started_rx.await.unwrap();
    assert!(!tracker.is_terminated());

    caller.abort();
    let _ = caller.await;
    drop(sink);
    tokio::task::yield_now().await;
    assert!(
        !tracker.is_terminated(),
        "retirement must retain a started blocking child"
    );

    release_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), tracker.wait_terminated())
        .await
        .expect("blocking generation did not reach terminal completion");
}

#[tokio::test]
async fn test_sink_open_creates_dir() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");

    let mut sink = FileSink::new();
    let config = test_config(&out_path, "json");

    sink.open(&config).await.unwrap();
    assert!(sink.is_open);
    assert!(out_path.exists());
    sink.close().await.unwrap();
}

#[tokio::test]
async fn open_preserves_orphan_tmp_and_advances_past_its_generation() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");
    std::fs::create_dir_all(&out_path).unwrap();

    // A prior crash may leave an unpublished file. Startup must not delete
    // it because another process could still own that path.
    let orphan = out_path.join("part_000007_000.jsonl.tmp");
    std::fs::write(&orphan, b"orphan").unwrap();

    let mut sink = FileSink::new();
    let config = test_config(&out_path, "json");

    sink.open(&config).await.unwrap();

    assert!(orphan.exists());
    assert_eq!(sink.next_generation, 8);

    sink.close().await.unwrap();
}

#[test]
fn contract_is_singleton_durable_at_least_once() {
    let dir = tempfile::tempdir().unwrap();
    let config = test_config(dir.path(), "json");
    let sink = FileSink::new();
    let contract = sink.contract(&config).unwrap();
    assert_eq!(contract.consistency, SinkConsistency::DurableAtLeastOnce);
    assert_eq!(contract.topology, SinkTopology::Singleton);
    assert_eq!(contract.input_mode, SinkInputMode::AppendOnly);
    assert_eq!(sink.suggested_write_timeout(), Duration::from_secs(30));
}

#[tokio::test]
async fn periodic_flush_publishes_pending_rows() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");
    let config = test_config(&out_path, "json");
    let schema = test_schema();

    let mut sink = FileSink::new();
    sink.open(&config).await.unwrap();
    sink.write_batch(&test_batch(&schema)).await.unwrap();
    assert_eq!(final_files(&out_path).len(), 0);

    sink.flush().await.unwrap();

    let files = final_files(&out_path);
    assert_eq!(files.len(), 1);
    assert!(files[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .contains("_000001_"));
    assert!(sink.active_tmp_files.is_empty());
    assert!(sink.writer.is_none());
    sink.close().await.unwrap();
}

#[tokio::test]
async fn close_publishes_pending_rows() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");
    let config = test_config(&out_path, "json");
    let schema = test_schema();

    let mut sink = FileSink::new();
    sink.open(&config).await.unwrap();
    sink.write_batch(&test_batch(&schema)).await.unwrap();
    sink.close().await.unwrap();

    assert_eq!(final_files(&out_path).len(), 1);
    assert!(!std::fs::read_dir(&out_path)
        .unwrap()
        .flatten()
        .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp")));
}

#[tokio::test]
async fn restart_uses_a_strictly_higher_generation() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");
    let config = test_config(&out_path, "json");
    let schema = test_schema();

    let mut first = FileSink::new();
    first.open(&config).await.unwrap();
    first.write_batch(&test_batch(&schema)).await.unwrap();
    first.flush().await.unwrap();
    first.close().await.unwrap();
    let first_path = final_files(&out_path).pop().unwrap();
    let first_contents = std::fs::read(&first_path).unwrap();

    let mut restarted = FileSink::new();
    restarted.open(&config).await.unwrap();
    assert_eq!(restarted.next_generation, 2);
    restarted.write_batch(&test_batch(&schema)).await.unwrap();
    restarted.flush().await.unwrap();
    restarted.close().await.unwrap();

    let files = final_files(&out_path);
    assert_eq!(files.len(), 2);
    assert!(files[0]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .contains("_000001_"));
    assert!(files[1]
        .file_name()
        .unwrap()
        .to_string_lossy()
        .contains("_000002_"));
    assert_eq!(std::fs::read(first_path).unwrap(), first_contents);
}

#[tokio::test]
async fn periodic_flush_materializes_bulk_batches() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");
    let config = test_config(&out_path, "arrow");
    let schema = test_schema();

    let mut sink = FileSink::new();
    sink.open(&config).await.unwrap();
    sink.write_batch(&test_batch(&schema)).await.unwrap();
    assert_eq!(sink.buffered_batches.len(), 1);

    sink.flush().await.unwrap();

    let files = final_files(&out_path);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].extension().unwrap(), "arrow");
    assert!(sink.buffered_batches.is_empty());
    sink.close().await.unwrap();
}

#[tokio::test]
async fn partial_publication_is_outcome_unknown_and_recovery_uses_new_generation() {
    let dir = tempfile::tempdir().unwrap();
    let out_path = dir.path().join("output");
    let mut config = test_config(&out_path, "json");
    config.set("max_file_size", "1");
    let schema = test_schema();

    let mut sink = FileSink::new();
    sink.open(&config).await.unwrap();
    sink.write_batch(&test_batch(&schema)).await.unwrap();
    sink.write_batch(&test_batch(&schema)).await.unwrap();
    sink.prepare_pending_files().await.unwrap();
    assert_eq!(sink.active_tmp_files.len(), 2);

    // Model a filesystem failure after preparation but before the second
    // rename. The first segment publishes; the missing second one fails.
    std::fs::remove_file(&sink.active_tmp_files[1]).unwrap();
    let error = sink.publish_pending_files().await.unwrap_err();
    assert!(error.to_string().contains("cannot publish"));
    assert!(error.is_outcome_unknown());
    assert_eq!(final_files(&out_path).len(), 1);
    drop(sink);

    // Outcome-unknown retires the old connector. Recovery scans the partial
    // final output and replays into a strictly newer immutable generation.
    let mut recovered = FileSink::new();
    recovered.open(&config).await.unwrap();
    assert_eq!(recovered.next_generation, 2);
    recovered.write_batch(&test_batch(&schema)).await.unwrap();
    recovered.flush().await.unwrap();
    assert_eq!(final_files(&out_path).len(), 2);
    recovered.close().await.unwrap();
}
