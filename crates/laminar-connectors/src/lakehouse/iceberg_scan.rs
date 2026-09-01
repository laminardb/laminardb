use futures_util::StreamExt;
use iceberg::expr::{Bind, BoundPredicate, Predicate};
use iceberg::scan::{FileScanTaskStream, TableScan};
use iceberg::spec::{Manifest, ManifestFile, ManifestList, SchemaRef, SnapshotRef};
use iceberg::table::Table;
use iceberg::{Error, ErrorKind};

use crate::error::ConnectorError;

use super::iceberg_config::IcebergSourceConfig;

const DEFAULT_MAX_MANIFEST_LIST_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_MANIFESTS_PER_SNAPSHOT: usize = 65_536;
pub(crate) const DEFAULT_MAX_PLANNED_FILES: usize = 65_536;

pub(crate) fn parse_and_bind_filter(
    encoded: Option<&str>,
    schema: SchemaRef,
) -> Result<Option<Predicate>, ConnectorError> {
    let predicate = encoded
        .map(serde_json::from_str::<Predicate>)
        .transpose()
        .map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "[LDB-ICEBERG-FILTER-SYNTAX] invalid Iceberg filter predicate JSON at line {} column {}",
                error.line(),
                error.column()
            ))
        })?;
    bind_filter(predicate.as_ref(), schema)?;
    Ok(predicate)
}

pub(crate) fn bind_filter(
    predicate: Option<&Predicate>,
    schema: SchemaRef,
) -> Result<Option<BoundPredicate>, ConnectorError> {
    predicate
        .map(|predicate| {
            predicate.clone().rewrite_not().bind(schema, true).map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "[LDB-ICEBERG-FILTER-BIND] Iceberg filter predicate is incompatible with the selected snapshot schema ({})",
                    error.kind()
                ))
            })
        })
        .transpose()
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ManifestReadLimits {
    manifest_list_bytes: u64,
    manifest_bytes: u64,
    manifests_per_snapshot: usize,
    metadata_concurrency: usize,
}

impl ManifestReadLimits {
    pub(crate) fn from_source(config: &IcebergSourceConfig) -> Self {
        Self {
            manifest_list_bytes: usize_to_u64(config.max_manifest_list_bytes),
            manifest_bytes: usize_to_u64(config.max_manifest_bytes),
            manifests_per_snapshot: config.max_manifests_per_snapshot,
            metadata_concurrency: config.scan_concurrency,
        }
    }

    pub(crate) const fn fixed() -> Self {
        Self {
            manifest_list_bytes: DEFAULT_MAX_MANIFEST_LIST_BYTES,
            manifest_bytes: DEFAULT_MAX_MANIFEST_BYTES,
            manifests_per_snapshot: DEFAULT_MAX_MANIFESTS_PER_SNAPSHOT,
            metadata_concurrency: 4,
        }
    }
}

pub(crate) async fn preflight_current_snapshot_manifest_list(
    table: &Table,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let Some(snapshot) = table.metadata().current_snapshot() else {
        return Ok(());
    };
    load_manifest_list(table, snapshot, ManifestReadLimits::fixed(), deadline).await?;
    Ok(())
}

pub(crate) async fn preflight_snapshot(
    table: &Table,
    snapshot: &SnapshotRef,
    limits: ManifestReadLimits,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let list = load_manifest_list(table, snapshot, limits, deadline).await?;
    let checks = futures_util::stream::iter(list.entries().iter().cloned())
        .map(|manifest| async move {
            validate_manifest_object_size(table, &manifest, limits, deadline).await
        })
        .buffer_unordered(limits.metadata_concurrency);
    futures_util::pin_mut!(checks);
    while let Some(result) = checks.next().await {
        result?;
    }
    Ok(())
}

pub(crate) async fn load_manifest_list(
    table: &Table,
    snapshot: &SnapshotRef,
    limits: ManifestReadLimits,
    deadline: tokio::time::Instant,
) -> Result<ManifestList, ConnectorError> {
    let input = table
        .file_io()
        .new_input(snapshot.manifest_list())
        .map_err(|error| metadata_read_error("manifest list metadata", &error))?;
    let metadata = tokio::time::timeout_at(deadline, input.metadata())
        .await
        .map_err(|_| metadata_timeout("manifest list metadata"))?
        .map_err(|error| metadata_read_error("manifest list metadata", &error))?;
    if metadata.size > limits.manifest_list_bytes {
        return Err(ConnectorError::ConfigurationError(format!(
            "[LDB-ICEBERG-MANIFEST-LIST-BYTE-LIMIT] manifest list has {} bytes; limit is {}",
            metadata.size, limits.manifest_list_bytes
        )));
    }

    let list = tokio::time::timeout_at(deadline, table.manifest_list_reader(snapshot).load())
        .await
        .map_err(|_| metadata_timeout("manifest list read"))?
        .map_err(|error| metadata_read_error("manifest list read", &error))?;
    validate_manifest_list(&list, limits)?;
    Ok(list)
}

pub(crate) async fn load_manifest(
    table: &Table,
    manifest_file: &ManifestFile,
    limits: ManifestReadLimits,
    deadline: tokio::time::Instant,
) -> Result<Manifest, ConnectorError> {
    validate_manifest_object_size(table, manifest_file, limits, deadline).await?;
    tokio::time::timeout_at(deadline, manifest_file.load_manifest(table.file_io()))
        .await
        .map_err(|_| metadata_timeout("manifest read"))?
        .map_err(|error| metadata_read_error("manifest read", &error))
}

async fn validate_manifest_object_size(
    table: &Table,
    manifest_file: &ManifestFile,
    limits: ManifestReadLimits,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    validate_manifest_size(manifest_file.manifest_length, limits.manifest_bytes)?;
    let input = table
        .file_io()
        .new_input(&manifest_file.manifest_path)
        .map_err(|error| metadata_read_error("manifest metadata", &error))?;
    let metadata = tokio::time::timeout_at(deadline, input.metadata())
        .await
        .map_err(|_| metadata_timeout("manifest metadata"))?
        .map_err(|error| metadata_read_error("manifest metadata", &error))?;
    if metadata.size > limits.manifest_bytes {
        return Err(manifest_byte_limit_error(
            metadata.size,
            limits.manifest_bytes,
        ));
    }
    Ok(())
}

pub(crate) async fn plan_files(
    scan: &TableScan,
    max_files: usize,
    deadline: tokio::time::Instant,
) -> Result<FileScanTaskStream, ConnectorError> {
    let tasks = tokio::time::timeout_at(deadline, scan.plan_files())
        .await
        .map_err(|_| metadata_timeout("scan planning"))?
        .map_err(|error| connector_scan_error("Iceberg scan planning failed", &error))?;
    Ok(bound_file_tasks(tasks, max_files))
}

fn bound_file_tasks(tasks: FileScanTaskStream, max_files: usize) -> FileScanTaskStream {
    let mut planned_files = 0_usize;
    Box::pin(tasks.map(move |result| {
        let task = result.map_err(sanitize_scan_error)?;
        if planned_files == max_files {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                "[LDB-ICEBERG-SCAN-FILE-LIMIT] scan planning exceeded read.max.planned.files",
            ));
        }
        planned_files += 1;
        Ok(task)
    }))
}

pub(crate) fn connector_scan_error(context: &str, error: &Error) -> ConnectorError {
    if is_scan_file_limit(error) {
        return ConnectorError::ConfigurationError(
            "[LDB-ICEBERG-SCAN-FILE-LIMIT] scan planning exceeded read.max.planned.files".into(),
        );
    }
    ConnectorError::ReadError(format!("{context} ({})", error.kind()))
}

fn validate_manifest_list(
    list: &ManifestList,
    limits: ManifestReadLimits,
) -> Result<(), ConnectorError> {
    if list.entries().len() > limits.manifests_per_snapshot {
        return Err(ConnectorError::ConfigurationError(format!(
            "[LDB-ICEBERG-MANIFEST-COUNT-LIMIT] snapshot references {} manifests; limit is {}",
            list.entries().len(),
            limits.manifests_per_snapshot
        )));
    }
    for manifest in list.entries() {
        validate_manifest_size(manifest.manifest_length, limits.manifest_bytes)?;
    }
    Ok(())
}

fn validate_manifest_size(declared: i64, limit: u64) -> Result<(), ConnectorError> {
    let size = u64::try_from(declared).map_err(|_| {
        ConnectorError::TransactionError(
            "[LDB-ICEBERG-MANIFEST-SIZE-INVALID] manifest length is negative".into(),
        )
    })?;
    if size > limit {
        return Err(manifest_byte_limit_error(size, limit));
    }
    Ok(())
}

fn manifest_byte_limit_error(size: u64, limit: u64) -> ConnectorError {
    ConnectorError::ConfigurationError(format!(
        "[LDB-ICEBERG-MANIFEST-BYTE-LIMIT] manifest has {size} bytes; limit is {limit}"
    ))
}

fn metadata_timeout(operation: &str) -> ConnectorError {
    ConnectorError::ReadError(format!(
        "[LDB-ICEBERG-METADATA-TIMEOUT] {operation} exceeded its deadline"
    ))
}

fn metadata_read_error(operation: &str, error: &Error) -> ConnectorError {
    ConnectorError::ReadError(format!(
        "[LDB-ICEBERG-METADATA-READ] {operation} failed ({})",
        error.kind()
    ))
}

fn sanitize_scan_error(error: Error) -> Error {
    if is_scan_file_limit(&error) {
        return error;
    }
    Error::new(
        error.kind(),
        format!("[LDB-ICEBERG-SCAN] scan task failed ({})", error.kind()),
    )
    .with_retryable(error.retryable())
}

fn is_scan_file_limit(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    for _ in 0..16 {
        let Some(candidate) = current else {
            return false;
        };
        if candidate
            .to_string()
            .contains("[LDB-ICEBERG-SCAN-FILE-LIMIT]")
        {
            return true;
        }
        current = candidate.source();
    }
    false
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use crate::lakehouse::iceberg::test_support::{append_rows, create_test_table};

    use super::*;

    #[test]
    fn scan_errors_do_not_expose_upstream_messages() {
        let error = Error::new(
            ErrorKind::Unexpected,
            "request failed with bearer do-not-expose",
        );
        let rendered = connector_scan_error("Iceberg read failed", &error).to_string();
        assert!(rendered.contains("Unexpected"));
        assert!(!rendered.contains("do-not-expose"));
    }

    #[tokio::test]
    async fn manifest_list_and_manifest_limits_fail_before_manifest_loading() {
        let fixture = create_test_table(false).await;
        let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, None)]).await;
        let snapshot = table.metadata().current_snapshot().unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);

        let list_error = load_manifest_list(
            &table,
            snapshot,
            ManifestReadLimits {
                manifest_list_bytes: 1,
                ..ManifestReadLimits::fixed()
            },
            deadline,
        )
        .await
        .unwrap_err();
        assert!(list_error.to_string().contains("MANIFEST-LIST-BYTE-LIMIT"));
        assert!(!list_error.is_transient());

        let manifest_error = load_manifest_list(
            &table,
            snapshot,
            ManifestReadLimits {
                manifest_bytes: 1,
                ..ManifestReadLimits::fixed()
            },
            deadline,
        )
        .await
        .unwrap_err();
        assert!(manifest_error.to_string().contains("MANIFEST-BYTE-LIMIT"));
        assert!(!manifest_error.is_transient());
    }

    #[tokio::test]
    async fn manifest_count_limit_is_stable() {
        let fixture = create_test_table(false).await;
        let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, None)]).await;
        let snapshot = table.metadata().current_snapshot().unwrap();
        let error = load_manifest_list(
            &table,
            snapshot,
            ManifestReadLimits {
                manifests_per_snapshot: 0,
                ..ManifestReadLimits::fixed()
            },
            tokio::time::Instant::now() + std::time::Duration::from_secs(5),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("MANIFEST-COUNT-LIMIT"));
        assert!(!error.is_transient());
    }

    #[tokio::test]
    async fn actual_manifest_object_size_cannot_bypass_the_declared_limit() {
        let fixture = create_test_table(false).await;
        let (table, _) = append_rows(&fixture, &fixture.table, 1, &[(1, None)]).await;
        let snapshot = table.metadata().current_snapshot().unwrap();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        let list = load_manifest_list(&table, snapshot, ManifestReadLimits::fixed(), deadline)
            .await
            .unwrap();
        let mut understated = list.entries()[0].clone();
        understated.manifest_length = 1;

        let error = validate_manifest_object_size(
            &table,
            &understated,
            ManifestReadLimits {
                manifest_bytes: 1,
                ..ManifestReadLimits::fixed()
            },
            deadline,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("MANIFEST-BYTE-LIMIT"));
        assert!(!error.is_transient());
    }
}
