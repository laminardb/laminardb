use std::collections::HashSet;

use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus, Operation, SnapshotRef};
use iceberg::table::Table;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::IcebergSourceConfig;
use crate::lakehouse::iceberg_scan::{load_manifest, load_manifest_list, ManifestReadLimits};

use super::cursor::IcebergSourceCursorV1;

#[derive(Debug)]
pub(super) struct AppendSnapshotPlan {
    pub snapshot: SnapshotRef,
    pub added_file_paths: HashSet<String>,
    pub manifest_count: usize,
}

pub(super) async fn plan_appends(
    table: &Table,
    cursor: &IcebergSourceCursorV1,
    config: &IcebergSourceConfig,
    deadline: tokio::time::Instant,
) -> Result<Vec<AppendSnapshotPlan>, ConnectorError> {
    let snapshots = lineage_after(table, cursor, config.max_snapshots_per_poll)?;
    let limits = ManifestReadLimits::from_source(config);
    let mut remaining_files = config.max_planned_files;
    let mut plans = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        let (added_file_paths, manifest_count) =
            added_files_for_snapshot(table, &snapshot, remaining_files, limits, deadline).await?;
        remaining_files = remaining_files
            .checked_sub(added_file_paths.len())
            .ok_or_else(file_limit_error)?;
        plans.push(AppendSnapshotPlan {
            snapshot,
            added_file_paths,
            manifest_count,
        });
    }
    Ok(plans)
}

pub(super) fn validate_cursor_lineage(
    table: &Table,
    cursor: &IcebergSourceCursorV1,
    max_snapshots: usize,
) -> Result<(), ConnectorError> {
    lineage_after(table, cursor, max_snapshots).map(|_| ())
}

fn lineage_after(
    table: &Table,
    cursor: &IcebergSourceCursorV1,
    max_snapshots: usize,
) -> Result<Vec<SnapshotRef>, ConnectorError> {
    let metadata = table.metadata();
    if metadata.snapshot_by_id(cursor.snapshot_id).is_none() {
        return Err(ConnectorError::ConfigurationError(
            "[LDB-ICEBERG-CURSOR-EXPIRED] checkpoint snapshot is absent from retained table history"
                .into(),
        ));
    }
    let mut snapshot = metadata
        .snapshot_for_ref(&cursor.table_ref)
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "[LDB-ICEBERG-CURSOR-REF-MISSING] table ref '{}' no longer exists",
                cursor.table_ref
            ))
        })?
        .clone();
    let mut reverse_lineage = Vec::new();
    let mut visited = HashSet::new();
    while snapshot.snapshot_id() != cursor.snapshot_id {
        if reverse_lineage.len() == max_snapshots {
            return Err(ConnectorError::ConfigurationError(
                "[LDB-ICEBERG-APPEND-SNAPSHOT-LIMIT] lineage exceeded read.max.snapshots.per.poll"
                    .into(),
            ));
        }
        if !visited.insert(snapshot.snapshot_id()) {
            return Err(ConnectorError::TransactionError(
                "[LDB-ICEBERG-APPEND-LINEAGE-CYCLE] snapshot ancestry contains a cycle".into(),
            ));
        }
        reverse_lineage.push(snapshot.clone());
        let parent_id = snapshot.parent_snapshot_id().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "[LDB-ICEBERG-CURSOR-DIVERGED] checkpoint snapshot is not an ancestor of the configured ref"
                    .into(),
            )
        })?;
        snapshot = metadata.snapshot_by_id(parent_id).cloned().ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "[LDB-ICEBERG-CURSOR-EXPIRED] snapshot history needed for resume has expired"
                    .into(),
            )
        })?;
    }
    reverse_lineage.reverse();
    Ok(reverse_lineage)
}

async fn added_files_for_snapshot(
    table: &Table,
    snapshot: &SnapshotRef,
    max_files: usize,
    limits: ManifestReadLimits,
    deadline: tokio::time::Instant,
) -> Result<(HashSet<String>, usize), ConnectorError> {
    if snapshot.summary().operation != Operation::Append {
        return Err(non_append_error(
            snapshot.snapshot_id(),
            snapshot.summary().operation.as_str(),
        ));
    }

    let manifest_list = load_manifest_list(table, snapshot, limits, deadline).await?;
    let mut paths = HashSet::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.added_snapshot_id != snapshot.snapshot_id()
            && !manifest_file.has_deleted_files()
        {
            continue;
        }
        if manifest_file.content == ManifestContentType::Deletes
            && manifest_file.added_snapshot_id == snapshot.snapshot_id()
            && manifest_file.has_added_files()
        {
            return Err(non_append_error(
                snapshot.snapshot_id(),
                "delete-file addition",
            ));
        }
        let manifest = load_manifest(table, manifest_file, limits, deadline).await?;
        for entry in manifest.entries() {
            if entry.snapshot_id() != Some(snapshot.snapshot_id()) {
                continue;
            }
            match entry.status() {
                ManifestStatus::Added if entry.content_type() == DataContentType::Data => {
                    paths.insert(entry.file_path().to_string());
                    if paths.len() > max_files {
                        return Err(file_limit_error());
                    }
                }
                ManifestStatus::Added => {
                    return Err(non_append_error(
                        snapshot.snapshot_id(),
                        "delete-file addition",
                    ));
                }
                ManifestStatus::Deleted => {
                    return Err(non_append_error(
                        snapshot.snapshot_id(),
                        "data-file removal",
                    ));
                }
                ManifestStatus::Existing => {}
            }
        }
    }
    Ok((paths, manifest_list.entries().len()))
}

fn non_append_error(snapshot_id: i64, operation: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(format!(
        "[LDB-ICEBERG-APPEND-NON-APPEND] snapshot {snapshot_id} contains {operation}; append mode cannot represent it"
    ))
}

fn file_limit_error() -> ConnectorError {
    ConnectorError::ConfigurationError(
        "[LDB-ICEBERG-APPEND-FILE-LIMIT] append planning exceeded read.max.planned.files".into(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary};

    use crate::config::ConnectorConfig;
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};
    use crate::lakehouse::iceberg_config::{IcebergReadMode, IcebergSourceConfig};

    use super::*;

    fn source_config(
        fixture: &crate::lakehouse::iceberg::test_support::TestTable,
    ) -> IcebergSourceConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", fixture.config.catalog.catalog_uri.clone());
        config.set(
            "catalog.warehouse",
            fixture.config.catalog.warehouse.clone(),
        );
        config.set("namespace", "test");
        config.set("table.name", "events");
        config.set("storage.type", "fs");
        config.set("read.mode", "append");
        IcebergSourceConfig::from_config(&config).unwrap()
    }

    fn table_with_metadata(table: &Table, metadata: iceberg::spec::TableMetadata) -> Table {
        let mut builder = Table::builder()
            .identifier(table.identifier().clone())
            .metadata(metadata)
            .file_io(table.file_io().clone())
            .runtime(iceberg::Runtime::try_current().unwrap());
        if let Some(location) = table.metadata_location() {
            builder = builder.metadata_location(location);
        }
        builder.build().unwrap()
    }

    fn branch(snapshot_id: i64) -> SnapshotReference {
        SnapshotReference {
            snapshot_id,
            retention: SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        }
    }

    async fn plan(
        table: &Table,
        cursor: &IcebergSourceCursorV1,
        config: &IcebergSourceConfig,
    ) -> Result<Vec<AppendSnapshotPlan>, ConnectorError> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        plan_appends(table, cursor, config, deadline).await
    }

    #[tokio::test]
    async fn traverses_every_append_in_lineage_order_without_bootstrap_files() {
        let fixture = create_test_table(false).await;
        let (first, first_paths) =
            append_rows(&fixture, &fixture.table, 1, &[(1, Some("a"))]).await;
        let config = source_config(&fixture);
        assert_eq!(config.read_mode, IcebergReadMode::Append);
        let first_snapshot = first.metadata().current_snapshot().unwrap().clone();
        let cursor = IcebergSourceCursorV1::from_snapshot(
            &config,
            &first,
            &first_snapshot,
            first.metadata().current_schema_id(),
        );
        let (second, second_paths) = append_rows(&fixture, &first, 2, &[(2, Some("b"))]).await;
        let second_id = second.metadata().current_snapshot_id().unwrap();
        let (third, third_paths) = append_rows(&fixture, &second, 3, &[(3, Some("c"))]).await;
        let third_id = third.metadata().current_snapshot_id().unwrap();

        let plans = plan(&third, &cursor, &config).await.unwrap();
        assert_eq!(
            plans
                .iter()
                .map(|plan| plan.snapshot.snapshot_id())
                .collect::<Vec<_>>(),
            vec![second_id, third_id]
        );
        assert_eq!(
            plans[0].added_file_paths,
            second_paths.into_iter().collect::<HashSet<_>>()
        );
        assert_eq!(
            plans[1].added_file_paths,
            third_paths.into_iter().collect::<HashSet<_>>()
        );
        assert!(plans.iter().all(|plan| plan.manifest_count > 0));
        assert!(plans.iter().all(|plan| first_paths
            .iter()
            .all(|path| !plan.added_file_paths.contains(path))));
        let current = IcebergSourceCursorV1::from_snapshot(
            &config,
            &third,
            third.metadata().current_snapshot().unwrap(),
            third.metadata().current_schema_id(),
        );
        assert!(plan(&third, &current, &config).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn expired_divergent_and_overwrite_cursors_fail_closed() {
        let fixture = create_test_table(false).await;
        let (first, _) = append_rows(&fixture, &fixture.table, 1, &[(1, None)]).await;
        let (second, _) = append_rows(&fixture, &first, 2, &[(2, None)]).await;
        let config = source_config(&fixture);
        let first_snapshot = first.metadata().current_snapshot().unwrap().clone();
        let second_snapshot = second.metadata().current_snapshot().unwrap().clone();
        let first_cursor = IcebergSourceCursorV1::from_snapshot(
            &config,
            &second,
            &first_snapshot,
            second.metadata().current_schema_id(),
        );
        let second_cursor = IcebergSourceCursorV1::from_snapshot(
            &config,
            &second,
            &second_snapshot,
            second.metadata().current_schema_id(),
        );

        let mut expired = first_cursor.clone();
        expired.snapshot_id = i64::MAX;
        let error = validate_cursor_lineage(&second, &expired, 10).unwrap_err();
        assert!(error.to_string().contains("CURSOR-EXPIRED"));
        assert!(!error.is_transient());

        let metadata = second
            .metadata()
            .clone()
            .into_builder(None)
            .set_ref("main", branch(first_snapshot.snapshot_id()))
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let divergent = table_with_metadata(&second, metadata);
        let error = validate_cursor_lineage(&divergent, &second_cursor, 10).unwrap_err();
        assert!(error.to_string().contains("CURSOR-DIVERGED"));
        assert!(!error.is_transient());

        let overwrite = Snapshot::builder()
            .with_snapshot_id(second_snapshot.snapshot_id() + 1)
            .with_parent_snapshot_id(Some(second_snapshot.snapshot_id()))
            .with_sequence_number(second_snapshot.sequence_number() + 1)
            .with_timestamp_ms(second_snapshot.timestamp_ms() + 1)
            .with_manifest_list("memory:///unused-overwrite-manifest-list.avro")
            .with_summary(Summary {
                operation: Operation::Overwrite,
                additional_properties: HashMap::new(),
            })
            .with_schema_id(second.metadata().current_schema_id())
            .build();
        let metadata = second
            .metadata()
            .clone()
            .into_builder(None)
            .set_branch_snapshot(overwrite, "main")
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        let overwrite_table = table_with_metadata(&second, metadata);
        let error = plan(&overwrite_table, &second_cursor, &config)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("APPEND-NON-APPEND"));
        assert!(!error.is_transient());
    }

    #[tokio::test]
    async fn lineage_and_file_limits_are_stable_errors() {
        let fixture = create_test_table(false).await;
        let (first, _) = append_rows(&fixture, &fixture.table, 1, &[(1, None)]).await;
        let config = source_config(&fixture);
        let cursor = IcebergSourceCursorV1::from_snapshot(
            &config,
            &first,
            first.metadata().current_snapshot().unwrap(),
            first.metadata().current_schema_id(),
        );
        let (second, _) = append_rows(&fixture, &first, 2, &[(2, None)]).await;
        let (third, _) = append_rows(&fixture, &second, 3, &[(3, None)]).await;
        let mut snapshot_limited = config.clone();
        snapshot_limited.max_snapshots_per_poll = 1;
        let error = plan(&third, &cursor, &snapshot_limited).await.unwrap_err();
        assert!(error.to_string().contains("SNAPSHOT-LIMIT"));
        assert!(!error.is_transient());
        let mut file_limited = config;
        file_limited.max_planned_files = 1;
        let error = plan(&third, &cursor, &file_limited).await.unwrap_err();
        assert!(error.to_string().contains("FILE-LIMIT"));
        assert!(!error.is_transient());
    }
}
