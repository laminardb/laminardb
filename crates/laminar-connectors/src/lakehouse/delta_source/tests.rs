use super::*;
use crate::connector::DeliveryGuarantee;

fn test_config() -> DeltaSourceConfig {
    DeltaSourceConfig::new("/tmp/delta_source_test")
}

#[test]
fn contract_is_a_replayable_positioned_full_changelog() {
    let source = DeltaSource::new(test_config(), None);
    let contract = source
        .contract(&ConnectorConfig::new("delta-lake"))
        .unwrap();
    assert_eq!(contract.consistency, SourceConsistency::Replayable);
    assert_eq!(contract.topology, SourceTopology::Singleton);
    assert_eq!(contract.input_mode, SourceInputMode::FullChangelog);
    assert_eq!(
        contract.row_positions,
        SourceRowPositionCapability::OrderedDeterministic
    );
    assert!(!contract.is_exact_delivery_certified());
    assert_eq!(
        source.checkpoint_unavailable_policy(),
        SourceCheckpointUnavailablePolicy::PollToReplayBoundary
    );

    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", "/tmp/delta_source_test");
    config.set("read.mode", "incremental");
    assert!(source.contract(&config).is_err());
}

#[tokio::test]
async fn start_rejects_removed_option_before_opening_table() {
    let mut source = DeltaSource::new(test_config(), None);
    let mut config = ConnectorConfig::new("delta-lake");
    config.set("table.path", "/tmp/delta_source_test");
    config.set("cdf.enabled", "true");
    let error = source
        .start(
            SourceStart::new(
                config,
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cdf.enabled"));
    assert_eq!(source.state(), ConnectorState::Created);
}

#[tokio::test]
async fn resume_rejects_a_legacy_or_foreign_cursor_before_io() {
    let mut legacy = SourceCheckpoint::new();
    legacy.set_offset("delta_version", "42");
    let mut source = DeltaSource::new(test_config(), None);
    let error = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("delta-lake"),
                SourcePosition::Resume {
                    attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(11),
                    checkpoint: legacy,
                },
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap(),
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("not a valid cursor"), "{error}");
    assert_eq!(source.state(), ConnectorState::Created);
}

#[test]
fn test_debug_output() {
    let source = DeltaSource::new(test_config(), None);
    let debug = format!("{source:?}");
    assert!(debug.contains("DeltaSource"));
    assert!(debug.contains("table_path: \"<configured>\""));
    assert!(!debug.contains("/tmp/delta_source_test"));
}

#[tokio::test]
async fn test_poll_not_running() {
    let mut source = DeltaSource::new(test_config(), None);
    assert!(source.poll_batch(100).await.is_err());
}

/// D020: Source `start()` must error without delta-lake feature.
#[cfg(not(feature = "delta-lake"))]
#[tokio::test]
async fn test_open_requires_feature() {
    let mut source = DeltaSource::new(test_config(), None);
    let result = source
        .start(
            SourceStart::new(
                ConnectorConfig::new("delta-lake"),
                SourcePosition::Initial,
                DeliveryGuarantee::BestEffort,
            )
            .unwrap(),
        )
        .await;
    assert!(result.unwrap_err().to_string().contains("delta-lake"));
}

#[cfg(feature = "delta-lake")]
mod table {
    use std::collections::HashMap;

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;
    use deltalake::{DeltaTable, TableProperty};

    use super::*;

    /// `(id, label, weight)` of one emitted changelog row.
    pub(super) type Change = (i64, String, i64);

    pub(super) fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]))
    }

    pub(super) fn rows(ids: &[i64], label: &str) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vec![label; ids.len()])),
            ],
        )
        .unwrap()
    }

    pub(super) async fn create(path: &str, properties: Vec<(TableProperty, &str)>) -> DeltaTable {
        let columns: deltalake::kernel::StructType = schema().as_ref().try_into_kernel().unwrap();
        let mut builder =
            super::super::super::delta_io::open_or_create_table(path, HashMap::new(), None)
                .await
                .unwrap()
                .create()
                .with_columns(columns.fields().cloned());
        for (property, value) in properties {
            builder = builder.with_configuration_property(property, Some(value));
        }
        builder.await.unwrap()
    }

    pub(super) async fn cdf_table(path: &str) -> DeltaTable {
        create(path, vec![(TableProperty::EnableChangeDataFeed, "true")]).await
    }

    pub(super) async fn open(path: &str) -> DeltaTable {
        super::super::super::delta_io::open_or_create_table(path, HashMap::new(), None)
            .await
            .unwrap()
    }

    pub(super) fn connector_config(path: &str, extra: &[(&str, &str)]) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("delta-lake");
        config.set("table.path", path);
        config.set("poll.interval.ms", "1");
        for (key, value) in extra {
            config.set(*key, *value);
        }
        config
    }

    pub(super) async fn start(
        path: &str,
        extra: &[(&str, &str)],
        position: SourcePosition,
    ) -> Result<DeltaSource, ConnectorError> {
        let mut source = DeltaSource::new(DeltaSourceConfig::default(), None);
        source
            .start(
                SourceStart::new(
                    connector_config(path, extra),
                    position,
                    DeliveryGuarantee::AtLeastOnce,
                )
                .unwrap(),
            )
            .await?;
        Ok(source)
    }

    pub(super) fn resume(checkpoint: SourceCheckpoint) -> SourcePosition {
        SourcePosition::Resume {
            attempt: laminar_core::checkpoint::CheckpointAttempt::canonical(7),
            checkpoint,
        }
    }

    pub(super) fn changes(batch: &SourceBatch) -> Vec<Change> {
        let records = &batch.records;
        let ids = records
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let labels = records
            .column_by_name("label")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let weights = records
            .column_by_name("__weight")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        (0..records.num_rows())
            .map(|row| {
                (
                    ids.value(row),
                    labels.value(row).to_string(),
                    weights.value(row),
                )
            })
            .collect()
    }

    /// Everything the source emits until it reports no more data, with every checkpoint it
    /// offers between polls and the number of rows emitted before it.
    pub(super) async fn drain(
        source: &mut DeltaSource,
        max_records: usize,
    ) -> (Vec<Change>, Vec<(SourceCheckpoint, usize)>) {
        let mut emitted = Vec::new();
        let mut boundaries = Vec::new();
        if let Some(checkpoint) = source.try_checkpoint().unwrap() {
            boundaries.push((checkpoint, 0));
        }
        while let Some(batch) = source.poll_batch(max_records).await.unwrap() {
            assert!(batch.num_rows() <= max_records);
            assert!(batch.row_positions().is_some(), "every row is positioned");
            emitted.extend(changes(&batch));
            if let Some(checkpoint) = source.try_checkpoint().unwrap() {
                boundaries.push((checkpoint, emitted.len()));
            }
        }
        (emitted, boundaries)
    }

    pub(super) fn sorted(mut changes: Vec<Change>) -> Vec<Change> {
        changes.sort();
        changes
    }

    /// Net multiset (row -> weight) of a changelog, without zero entries.
    pub(super) fn net(changes: &[Change]) -> Vec<(i64, String, i64)> {
        let mut net = std::collections::BTreeMap::<(i64, String), i64>::new();
        for (id, label, weight) in changes {
            *net.entry((*id, label.clone())).or_default() += weight;
        }
        net.into_iter()
            .filter(|(_, weight)| *weight != 0)
            .map(|((id, label), weight)| (id, label, weight))
            .collect()
    }
}

#[cfg(feature = "delta-lake")]
use table::{cdf_table, drain, net, resume, rows, sorted, start};

#[cfg(feature = "delta-lake")]
async fn drain_until(source: &mut DeltaSource, rows: usize) -> Vec<table::Change> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut emitted = Vec::new();
    while emitted.len() < rows {
        assert!(std::time::Instant::now() < deadline, "only {emitted:?}");
        match source.poll_batch(1024).await.unwrap() {
            Some(batch) => emitted.extend(table::changes(&batch)),
            None => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
        }
    }
    emitted
}

/// A default source pins the version current at first start, emits that snapshot as inserts,
/// and then each later commit: update pre/post images, deletes, and inserts.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn snapshot_then_changes_emits_bootstrap_and_every_later_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    cdf_table(&path)
        .await
        .write(vec![rows(&[1, 2, 3], "a")])
        .await
        .unwrap();

    let mut source = start(&path, &[], SourcePosition::Initial).await.unwrap();
    // A commit after the pin belongs to the change feed, not the snapshot.
    table::open(&path)
        .await
        .write(vec![rows(&[4], "b")])
        .await
        .unwrap();
    let (emitted, boundaries) = drain(&mut source, 2).await;
    assert_eq!(
        sorted(emitted),
        vec![
            (1, "a".into(), 1),
            (2, "a".into(), 1),
            (3, "a".into(), 1),
            (4, "b".into(), 1)
        ]
    );
    // The bootstrap ends at the pinned version and hands off to the next commit.
    let handoff = boundaries
        .iter()
        .map(|(checkpoint, _)| super::cursor::DeltaCursor::from_checkpoint(checkpoint).unwrap())
        .find(|cursor| matches!(cursor.phase, super::cursor::DeltaPhase::Changes { .. }))
        .unwrap();
    assert_eq!(
        handoff.phase,
        super::cursor::DeltaPhase::Changes { next_version: 2 }
    );

    let table = table::open(&path).await;
    let table = table
        .update()
        .with_predicate("id = 2")
        .with_update("label", "'z'")
        .await
        .unwrap()
        .0;
    table.delete().with_predicate("id = 3").await.unwrap();
    let emitted = drain_until(&mut source, 3).await;
    assert_eq!(
        sorted(emitted),
        vec![(2, "a".into(), -1), (2, "z".into(), 1), (3, "a".into(), -1)]
    );
}

/// Resuming from any checkpoint the source offered reproduces exactly the rows emitted after
/// it: no unit is split across a checkpoint, and maintenance rewrites emit nothing.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn resume_from_every_offered_checkpoint_reproduces_the_remaining_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let mut table = cdf_table(&path).await;
    for (ids, label) in [
        (&[1_i64, 2, 3][..], "a"),
        (&[4, 5][..], "b"),
        (&[6][..], "c"),
    ] {
        table = table.write(vec![rows(ids, label)]).await.unwrap();
    }
    let pinned = table.version().unwrap();
    let mut first = start(&path, &[], SourcePosition::Initial).await.unwrap();
    let mut table = table
        .update()
        .with_predicate("id = 1")
        .with_update("label", "'x'")
        .await
        .unwrap()
        .0;
    table = table.write(vec![rows(&[7, 8, 9], "d")]).await.unwrap();
    table = table.delete().with_predicate("id >= 5").await.unwrap().0;
    let (table, metrics) = table.optimize().await.unwrap();
    assert!(
        metrics.num_files_added > 0,
        "compaction commits a dataChange=false rewrite"
    );
    let latest = table.version().unwrap();
    assert!(latest > pinned + 3);

    let (emitted, boundaries) = drain(&mut first, 2).await;
    assert!(boundaries.len() > 4, "{boundaries:?}");
    let cursor = super::cursor::DeltaCursor::from_checkpoint(&first.checkpoint()).unwrap();
    assert_eq!(
        cursor.phase,
        super::cursor::DeltaPhase::Changes {
            next_version: latest + 1
        }
    );
    // The changelog nets to the table's current contents.
    assert_eq!(
        net(&emitted),
        vec![
            (1, "x".into(), 1),
            (2, "a".into(), 1),
            (3, "a".into(), 1),
            (4, "b".into(), 1)
        ]
    );

    for (checkpoint, offset) in boundaries {
        let mut resumed = start(&path, &[], resume(checkpoint.clone())).await.unwrap();
        let (replayed, _) = drain(&mut resumed, 3).await;
        assert_eq!(
            sorted(replayed),
            sorted(emitted[offset..].to_vec()),
            "resume from {checkpoint:?}"
        );
    }
}

/// A checkpoint is offered only between units: while a commit is partly emitted, the barrier
/// must wait for its final rows, which arrive in `max_records` slices.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn checkpoints_wait_for_the_end_of_a_partly_emitted_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let table = cdf_table(&path).await;
    let mut source = start(
        &path,
        &[("starting.version", "latest")],
        SourcePosition::Initial,
    )
    .await
    .unwrap();
    let before = source.try_checkpoint().unwrap().unwrap();
    let ids: Vec<i64> = (0..10).collect();
    table.write(vec![rows(&ids, "w")]).await.unwrap();

    let mut sizes = Vec::new();
    loop {
        let Some(batch) = source.poll_batch(3).await.unwrap() else {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            continue;
        };
        sizes.push(batch.num_rows());
        let positions = batch.row_positions().unwrap();
        assert_eq!(
            positions.sub_offset().values().first().copied(),
            u32::try_from(sizes.iter().sum::<usize>() - batch.num_rows()).ok()
        );
        match source.try_checkpoint().unwrap() {
            None => assert!(sizes.iter().sum::<usize>() < 10),
            Some(after) => {
                assert_eq!(sizes.iter().sum::<usize>(), 10);
                assert_ne!(after, before);
                break;
            }
        }
    }
    assert_eq!(sizes, vec![3, 3, 3, 1]);
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn explicit_starting_version_is_inclusive_and_latest_skips_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let table = cdf_table(&path).await;
    let table = table.write(vec![rows(&[1], "v1")]).await.unwrap();
    table.write(vec![rows(&[2], "v2")]).await.unwrap();

    let mut from_one = start(&path, &[("starting.version", "1")], SourcePosition::Initial)
        .await
        .unwrap();
    assert_eq!(
        sorted(drain(&mut from_one, 10).await.0),
        vec![(1, "v1".into(), 1), (2, "v2".into(), 1)]
    );
    let mut latest = start(
        &path,
        &[("starting.version", "latest")],
        SourcePosition::Initial,
    )
    .await
    .unwrap();
    assert!(drain(&mut latest, 10).await.0.is_empty());
    let error = start(&path, &[("starting.version", "9")], SourcePosition::Initial)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("beyond"), "{error}");
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn a_known_backlog_is_read_without_waiting_for_the_poll_interval() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let mut table = cdf_table(&path).await;
    let commits = i64::try_from(super::MAX_COMMITS_PER_SCAN).unwrap() + 6;
    for id in 1..=commits {
        table = table.write(vec![rows(&[id], "v")]).await.unwrap();
    }

    let mut source = start(
        &path,
        &[("starting.version", "1"), ("poll.interval.ms", "600000")],
        SourcePosition::Initial,
    )
    .await
    .unwrap();
    let emitted = drain_until(&mut source, usize::try_from(commits).unwrap()).await;
    let expected: Vec<_> = (1..=commits).map(|id| (id, "v".into(), 1)).collect();
    assert_eq!(sorted(emitted), expected);
}

#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn snapshot_mode_completes_and_reads_tables_without_change_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    table::create(&path, Vec::new())
        .await
        .write(vec![rows(&[1, 2], "s")])
        .await
        .unwrap();

    let error = start(&path, &[], SourcePosition::Initial)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("enableChangeDataFeed"),
        "{error}"
    );

    let mut source = start(&path, &[("read.mode", "snapshot")], SourcePosition::Initial)
        .await
        .unwrap();
    let (emitted, _) = drain(&mut source, 10).await;
    assert_eq!(
        sorted(emitted),
        vec![(1, "s".into(), 1), (2, "s".into(), 1)]
    );
    let complete = source.checkpoint();
    table::open(&path)
        .await
        .write(vec![rows(&[3], "later")])
        .await
        .unwrap();
    assert!(source.poll_batch(10).await.unwrap().is_none());

    let mut resumed = start(
        &path,
        &[("read.mode", "snapshot")],
        resume(complete.clone()),
    )
    .await
    .unwrap();
    assert!(drain(&mut resumed, 10).await.0.is_empty());
    let error = start(&path, &[], resume(complete)).await.unwrap_err();
    assert!(error.to_string().contains("read.mode"), "{error}");
}

/// A table recreated at the same location is a different incarnation: neither a checkpoint nor
/// a running source may continue against it.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn a_replaced_table_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    cdf_table(&path)
        .await
        .write(vec![rows(&[1], "a")])
        .await
        .unwrap();
    let mut source = start(&path, &[], SourcePosition::Initial).await.unwrap();
    drain(&mut source, 10).await;
    let checkpoint = source.checkpoint();

    std::fs::remove_dir_all(&path).unwrap();
    std::fs::create_dir_all(&path).unwrap();
    let table = cdf_table(&path).await;
    let table = table.write(vec![rows(&[2], "b")]).await.unwrap();
    table.write(vec![rows(&[3], "c")]).await.unwrap();

    let error = loop {
        match source.poll_batch(10).await {
            Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            Ok(Some(batch)) => panic!("read {:?} from the replacement", table::changes(&batch)),
            Err(error) => break error,
        }
    };
    assert!(error.to_string().contains("replaced"), "{error}");
    let error = start(&path, &[], resume(checkpoint)).await.unwrap_err();
    assert!(error.to_string().contains("replaced"), "{error}");
}

/// Expired change history fails the read instead of skipping to a later version.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn missing_change_history_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let table = cdf_table(&path).await;
    let table = table.write(vec![rows(&[1], "a")]).await.unwrap();
    let table = table.write(vec![rows(&[2], "b")]).await.unwrap();
    // Log cleanup after a checkpoint: the table still opens, but commits 0 and 1 are gone.
    deltalake::checkpoints::create_checkpoint(&table, None)
        .await
        .unwrap();
    for version in [0, 1] {
        std::fs::remove_file(
            dir.path()
                .join("_delta_log")
                .join(format!("{version:020}.json")),
        )
        .unwrap();
    }

    let mut source = start(&path, &[("starting.version", "1")], SourcePosition::Initial)
        .await
        .unwrap();
    let error = source.poll_batch(10).await.unwrap_err();
    assert!(!error.is_transient(), "{error}");
    let cursor = super::cursor::DeltaCursor::from_checkpoint(&source.checkpoint()).unwrap();
    assert_eq!(
        cursor.phase,
        super::cursor::DeltaPhase::Changes { next_version: 1 }
    );
}

/// The source binding is fixed: a schema change fails closed before any row of it is read.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn a_schema_change_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    cdf_table(&path)
        .await
        .write(vec![rows(&[1], "a")])
        .await
        .unwrap();
    let mut source = start(&path, &[], SourcePosition::Initial).await.unwrap();
    drain(&mut source, 10).await;

    table::open(&path)
        .await
        .add_columns()
        .with_fields([deltalake::kernel::StructField::nullable(
            "extra",
            deltalake::kernel::DataType::LONG,
        )])
        .await
        .unwrap();
    let error = loop {
        match source.poll_batch(10).await {
            Ok(None) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
            Ok(Some(batch)) => panic!("read {:?} after the schema change", table::changes(&batch)),
            Err(error) => break error,
        }
    };
    assert!(error.to_string().contains("schema changed"), "{error}");
}

/// Commit a deletion-vector delete of `deleted` row indexes of the table's single data file, in
/// the shape a DV-enabled writer produces without `cdc` files: `remove` + `add` of the same path,
/// the new `add` carrying the vector.
#[cfg(feature = "delta-lake")]
fn commit_deletion_vector(dir: &std::path::Path, version: u64, deleted: &[u64]) {
    use delta_kernel::actions::deletion_vector_writer::{
        KernelDeletionVector, StreamingDeletionVectorWriter,
    };

    let log = dir.join("_delta_log");
    let previous = std::fs::read_to_string(log.join(format!("{:020}.json", version - 1))).unwrap();
    let add = previous
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find_map(|action| action.get("add").cloned())
        .expect("the previous commit added the data file");

    let mut bytes = Vec::new();
    let mut writer = StreamingDeletionVectorWriter::new(&mut bytes);
    let mut vector = KernelDeletionVector::new();
    vector.add_deleted_row_indexes(deleted.iter().copied());
    let written = writer.write_deletion_vector(vector).unwrap();
    writer.finalize().unwrap();
    let dv_path = dir.join(format!("deletion_vector_{version}.bin"));
    std::fs::write(&dv_path, bytes).unwrap();
    let dv_url = url::Url::from_file_path(&dv_path).unwrap();

    let now = 1_700_000_000_000_i64 + i64::try_from(version).unwrap();
    let remove = serde_json::json!({"remove": {
        "path": add["path"], "deletionTimestamp": now, "dataChange": true,
        "extendedFileMetadata": true, "partitionValues": add["partitionValues"],
        "size": add["size"]
    }});
    let mut updated = add.clone();
    updated["dataChange"] = serde_json::json!(true);
    updated["modificationTime"] = serde_json::json!(now);
    updated["deletionVector"] = serde_json::json!({
        "storageType": "p", "pathOrInlineDv": dv_url.as_str(),
        "offset": written.offset, "sizeInBytes": written.size_in_bytes,
        "cardinality": written.cardinality
    });
    let commit = [
        serde_json::json!({"commitInfo": {"timestamp": now, "operation": "DELETE"}}),
        remove,
        serde_json::json!({ "add": updated }),
    ]
    .iter()
    .map(serde_json::Value::to_string)
    .collect::<Vec<_>>()
    .join("\n");
    std::fs::write(log.join(format!("{version:020}.json")), commit).unwrap();
}

/// Rows removed by a deletion vector leave the change feed as deletes of exactly those rows
/// (not as a delete and re-insert of the whole file), and snapshots skip them.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn deletion_vector_deletes_are_emitted_once_and_hidden_from_snapshots() {
    use deltalake::TableProperty;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    table::create(
        &path,
        vec![
            (TableProperty::EnableChangeDataFeed, "true"),
            (TableProperty::EnableDeletionVectors, "true"),
        ],
    )
    .await
    .write(vec![rows(&[1, 2, 3, 4], "a")])
    .await
    .unwrap();
    let mut source = start(&path, &[], SourcePosition::Initial).await.unwrap();
    commit_deletion_vector(dir.path(), 2, &[1, 2]);

    let emitted = drain_until(&mut source, 6).await;
    assert_eq!(
        sorted(emitted),
        vec![
            (1, "a".into(), 1),
            (2, "a".into(), -1),
            (2, "a".into(), 1),
            (3, "a".into(), -1),
            (3, "a".into(), 1),
            (4, "a".into(), 1)
        ]
    );
    let mut snapshot = start(&path, &[("read.mode", "snapshot")], SourcePosition::Initial)
        .await
        .unwrap();
    assert_eq!(
        sorted(drain(&mut snapshot, 10).await.0),
        vec![(1, "a".into(), 1), (4, "a".into(), 1)]
    );
}

/// A commit far larger than the read budget streams through in bounded slices: the reader
/// never queues more than its byte budget and the cursor moves only once the commit is done.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn a_large_commit_streams_under_the_byte_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let table = cdf_table(&path).await;
    let mut source = start(
        &path,
        &[("starting.version", "latest")],
        SourcePosition::Initial,
    )
    .await
    .unwrap();
    let ids: Vec<i64> = (0..200_000).collect();
    table.write(vec![rows(&ids, "large")]).await.unwrap();

    let mut emitted = 0;
    let mut checkpoints = 0;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while emitted < ids.len() {
        assert!(std::time::Instant::now() < deadline);
        match source.poll_batch(4096).await.unwrap() {
            Some(batch) => {
                emitted += batch.num_rows();
                checkpoints += usize::from(source.try_checkpoint().unwrap().is_some());
            }
            None => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
        }
    }
    assert_eq!(emitted, ids.len());
    assert_eq!(
        checkpoints, 1,
        "only the commit's final slice ends the unit"
    );
    assert_eq!(
        source_reader(&source).context.budget.available_permits(),
        reader::MAX_QUEUED_BYTES as usize,
        "every emitted batch returned its bytes to the read budget"
    );
}

#[cfg(feature = "delta-lake")]
fn source_reader(source: &DeltaSource) -> &Reader {
    source.reader.as_ref().unwrap()
}

/// Kernel reads produce exactly the bound Arrow types for partition columns, timestamps (with
/// and without zone), decimals, and nested columns, in both snapshot and change-feed units.
#[cfg(feature = "delta-lake")]
#[tokio::test]
async fn typed_partitioned_rows_keep_their_bound_types() {
    use arrow_array::{
        builder::{ListBuilder, StringBuilder},
        Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use deltalake::kernel::engine::arrow_conversion::TryIntoKernel as _;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("region", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "local",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("amount", DataType::Decimal128(12, 3), true),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("element", DataType::Utf8, true))),
            true,
        ),
    ]));
    let batch = |ids: &[i64], region: &str| {
        let mut tags = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
            "element",
            DataType::Utf8,
            true,
        )));
        for id in ids {
            tags.values().append_value(format!("t{id}"));
            tags.append(true);
        }
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(vec![region; ids.len()])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![1_700_000_000_123_456; ids.len()])
                        .with_timezone("UTC"),
                ),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_700_000_000_000_001;
                    ids.len()
                ])),
                Arc::new(
                    Decimal128Array::from(vec![12_345_i128; ids.len()])
                        .with_precision_and_scale(12, 3)
                        .unwrap(),
                ),
                Arc::new(tags.finish()),
            ],
        )
        .unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let columns: deltalake::kernel::StructType = schema.as_ref().try_into_kernel().unwrap();
    let table =
        super::super::delta_io::open_or_create_table(&path, std::collections::HashMap::new(), None)
            .await
            .unwrap()
            .create()
            .with_columns(columns.fields().cloned())
            .with_partition_columns(["region"])
            .with_configuration_property(
                deltalake::TableProperty::EnableChangeDataFeed,
                Some("true"),
            )
            .await
            .unwrap();
    table.write(vec![batch(&[1, 2], "eu")]).await.unwrap();

    let mut source = start(&path, &[], SourcePosition::Initial).await.unwrap();
    table::open(&path)
        .await
        .write(vec![batch(&[3], "us")])
        .await
        .unwrap()
        .delete()
        .with_predicate("id = 1")
        .await
        .unwrap();

    let expected = source.schema();
    assert_eq!(
        expected.field_with_name("region").unwrap().data_type(),
        &DataType::Utf8
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    let mut rows = 0;
    while rows < 4 {
        assert!(std::time::Instant::now() < deadline);
        let Some(batch) = source.poll_batch(100).await.unwrap() else {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            continue;
        };
        assert_eq!(batch.records.schema(), expected);
        let at = batch
            .records
            .column_by_name("at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(at
            .values()
            .iter()
            .all(|value| *value == 1_700_000_000_123_456));
        rows += batch.num_rows();
    }
    assert_eq!(rows, 4, "two snapshot rows, one insert, one delete");
}

/// A Unity Catalog source reads through vended credentials and renews them, outside any open
/// replay unit, before they expire.
#[cfg(feature = "delta-lake-unity")]
#[tokio::test]
async fn unity_source_reads_with_vended_credentials_and_renews_them() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let dir = tempfile::tempdir().unwrap();
    let path_str = dir.path().to_string_lossy().to_string();
    cdf_table(&path_str)
        .await
        .write(vec![rows(&[1, 2], "a")])
        .await
        .unwrap()
        .write(vec![rows(&[10], "c")])
        .await
        .unwrap();

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables/main.lake.events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "events", "catalog_name": "main", "schema_name": "lake",
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "columns": [],
            "storage_location": url::Url::from_directory_path(dir.path()).unwrap().as_str(),
            "properties": {}, "created_at": 0, "updated_at": 0, "table_id": "uc-1"
        })))
        .mount(&server)
        .await;
    // Credentials that are already inside the renewal margin force a renewal before each read.
    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/temporary-table-credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "aws_temp_credentials": {
                "access_key_id": "ASIAVENDED", "secret_access_key": "vended-secret"
            },
            "expiration_time": (chrono::Utc::now() + chrono::Duration::minutes(2))
                .timestamp_millis(),
            "url": "s3://bucket/lake/events"
        })))
        .mount(&server)
        .await;

    let workspace = server.uri();
    let unity = [
        ("catalog.type", "unity"),
        ("catalog.workspace_url", workspace.as_str()),
        ("catalog.access_token", "dapi-test"),
        ("catalog.name", "main"),
        ("catalog.schema", "lake"),
    ];
    let mut source = start("uc://main.lake.events", &unity, SourcePosition::Initial)
        .await
        .unwrap();
    let vend_requests = || async {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|request| request.url.path().ends_with("temporary-table-credentials"))
            .count()
    };
    // Two snapshot files: the read stops after the first to renew before the second.
    let (snapshot, _) = drain(&mut source, 10).await;
    assert_eq!(
        sorted(snapshot),
        vec![(1, "a".into(), 1), (2, "a".into(), 1), (10, "c".into(), 1)]
    );
    assert!(
        vend_requests().await >= 3,
        "open plus a renewal per snapshot file"
    );
    table::open(&path_str)
        .await
        .write(vec![rows(&[3], "b")])
        .await
        .unwrap();
    assert_eq!(drain_until(&mut source, 1).await, vec![(3, "b".into(), 1)]);

    assert!(
        vend_requests().await >= 4,
        "and a renewal before the change read"
    );

    // Revoked access: the next renewal is refused and the source fails instead of reading on
    // with expiring credentials or falling back to another storage identity.
    server.reset().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "name": "events", "catalog_name": "main", "schema_name": "lake",
            "table_type": "EXTERNAL", "data_source_format": "DELTA", "columns": [],
            "storage_location": url::Url::from_directory_path(dir.path()).unwrap().as_str(),
            "properties": {}, "created_at": 0, "updated_at": 0, "table_id": "uc-1"
        })))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
            "error_code": "PERMISSION_DENIED", "message": "revoked"
        })))
        .mount(&server)
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let error = source.poll_batch(10).await.unwrap_err();
    assert!(error.to_string().contains("PERMISSION_DENIED"), "{error}");
    assert!(!error.is_transient(), "{error}");
}

/// Latency from a commit becoming visible in the log to the source emitting its rows, and read
/// throughput for a large snapshot and a large commit. Run with
/// `cargo test -p laminar-connectors --features delta-lake --release --lib delta_source_measurements -- --ignored --nocapture`.
#[cfg(feature = "delta-lake")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "measurement harness; run explicitly"]
async fn delta_source_measurements() {
    fn percentile(samples: &mut [std::time::Duration], p: f64) -> std::time::Duration {
        samples.sort_unstable();
        let rank = ((samples.len() - 1) as f64 * p).round() as usize;
        samples[rank]
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let mut table = cdf_table(&path).await;
    let rows_per_write = 1_000_000_i64;
    let ids: Vec<i64> = (0..rows_per_write).collect();
    table = table.write(vec![rows(&ids, "snapshot")]).await.unwrap();

    let started = std::time::Instant::now();
    let mut source = start(
        &path,
        &[("poll.interval.ms", "10")],
        SourcePosition::Initial,
    )
    .await
    .unwrap();
    let mut emitted = 0_usize;
    while emitted < ids.len() {
        if let Some(batch) = source.poll_batch(8192).await.unwrap() {
            emitted += batch.num_rows();
        }
    }
    let snapshot = started.elapsed();
    println!(
        "snapshot: {emitted} rows in {snapshot:?} = {:.0} rows/s",
        emitted as f64 / snapshot.as_secs_f64()
    );

    table = table.write(vec![rows(&ids, "commit")]).await.unwrap();
    let started = std::time::Instant::now();
    let mut emitted = 0_usize;
    while emitted < ids.len() {
        match source.poll_batch(8192).await.unwrap() {
            Some(batch) => emitted += batch.num_rows(),
            None => tokio::time::sleep(std::time::Duration::from_millis(1)).await,
        }
    }
    let commit = started.elapsed();
    println!(
        "one commit: {emitted} rows in {commit:?} = {:.0} rows/s (includes up to one poll interval)",
        emitted as f64 / commit.as_secs_f64()
    );

    let mut latencies = Vec::new();
    for round in 0..50_i64 {
        table = table
            .write(vec![rows(&[rows_per_write + round], "probe")])
            .await
            .unwrap();
        let committed = std::time::Instant::now();
        loop {
            if source.poll_batch(8192).await.unwrap().is_some() {
                latencies.push(committed.elapsed());
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
    println!(
        "commit -> emission (poll.interval.ms=10): p50 {:?} p95 {:?} max {:?}",
        percentile(&mut latencies, 0.5),
        percentile(&mut latencies, 0.95),
        percentile(&mut latencies, 1.0)
    );
}
