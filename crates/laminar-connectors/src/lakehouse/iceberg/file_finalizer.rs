//! Replay-safe promotion of completed coordinated data files.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use iceberg::io::FileIO;
use iceberg::spec::{DataFile, DataFileBuilder};
use iceberg::writer::file_writer::location_generator::FileNameGenerator;
use sha2::{Digest, Sha256};

use crate::error::ConnectorError;

use super::metrics::IcebergMetrics;

const MAX_COPY_CHUNK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(super) struct ReplaySafeFileNameGenerator {
    prefix: String,
    staging_id: Option<String>,
    ordinal: Arc<AtomicU64>,
}

impl ReplaySafeFileNameGenerator {
    pub(super) fn new(
        deployment_id: &str,
        sink_id: &str,
        participant_id: u64,
        epoch: u64,
        coordinated: bool,
    ) -> Self {
        let mut digest = Sha256::new();
        digest.update(b"laminardb-iceberg-data-v2\0");
        digest.update(deployment_id.as_bytes());
        digest.update([0]);
        digest.update(sink_id.as_bytes());
        digest.update(participant_id.to_be_bytes());
        digest.update(epoch.to_be_bytes());
        Self {
            prefix: format!("ldb-{:x}", digest.finalize()),
            staging_id: coordinated.then(|| uuid::Uuid::now_v7().simple().to_string()),
            ordinal: Arc::new(AtomicU64::new(0)),
        }
    }

    fn final_path(&self, staging_path: &str, content_hash: &str) -> Result<String, ConnectorError> {
        let staging_id = self.staging_id.as_deref().ok_or_else(|| {
            ConnectorError::Internal("direct Iceberg file cannot be promoted".into())
        })?;
        let (parent, name) = staging_path
            .rsplit_once('/')
            .map_or(("", staging_path), |(parent, name)| (parent, name));
        let marker = format!("{}-stage-{staging_id}-", self.prefix);
        let ordinal = name
            .strip_prefix(&marker)
            .and_then(|name| name.strip_suffix(".parquet"))
            .ok_or_else(|| {
                ConnectorError::Internal(
                    "Iceberg staging path does not match its epoch namespace".into(),
                )
            })?;
        let name = format!("{}-{ordinal}-{content_hash}.parquet", self.prefix);
        Ok(if parent.is_empty() {
            name
        } else {
            format!("{parent}/{name}")
        })
    }
}

impl FileNameGenerator for ReplaySafeFileNameGenerator {
    fn generate_file_name(&self) -> String {
        let ordinal = self.ordinal.fetch_add(1, Ordering::Relaxed);
        match &self.staging_id {
            Some(staging_id) => format!("{}-stage-{staging_id}-{ordinal:08}.parquet", self.prefix),
            None => format!("{}-{ordinal:08}.parquet", self.prefix),
        }
    }
}

pub(super) async fn finalize_coordinated_files(
    file_io: &FileIO,
    names: &ReplaySafeFileNameGenerator,
    partition_spec_id: i32,
    max_buffer_bytes: usize,
    metrics: &IcebergMetrics,
    files: Vec<DataFile>,
) -> Result<Vec<DataFile>, ConnectorError> {
    if max_buffer_bytes == 0 {
        return Err(ConnectorError::Internal(
            "Iceberg finalization buffer limit must be nonzero".into(),
        ));
    }
    let chunk_bytes = u64::try_from(max_buffer_bytes.min(MAX_COPY_CHUNK_BYTES)).map_err(|_| {
        ConnectorError::Internal("Iceberg finalization buffer limit exceeds u64".into())
    })?;
    let mut finalized = Vec::with_capacity(files.len());
    for file in files {
        finalized.push(
            finalize_file(
                file_io,
                names,
                partition_spec_id,
                chunk_bytes,
                metrics,
                file,
            )
            .await?,
        );
    }
    Ok(finalized)
}

async fn finalize_file(
    file_io: &FileIO,
    names: &ReplaySafeFileNameGenerator,
    partition_spec_id: i32,
    chunk_bytes: u64,
    metrics: &IcebergMetrics,
    file: DataFile,
) -> Result<DataFile, ConnectorError> {
    let staging_path = file.file_path();
    let expected_bytes = file.file_size_in_bytes();
    let content_hash =
        hash_file(file_io, staging_path, expected_bytes, chunk_bytes, metrics).await?;
    let final_path = names.final_path(staging_path, &content_hash)?;
    let final_input = file_io
        .new_input(&final_path)
        .map_err(|error| file_error("open final data file", &error))?;
    let final_exists = final_input
        .exists()
        .await
        .map_err(|error| file_error("inspect final data file", &error))?;
    let final_matches = if final_exists {
        file_matches(
            file_io,
            &final_path,
            expected_bytes,
            chunk_bytes,
            metrics,
            &content_hash,
        )
        .await?
    } else {
        false
    };
    if !final_matches {
        copy_file(
            file_io,
            staging_path,
            &final_path,
            expected_bytes,
            chunk_bytes,
            metrics,
        )
        .await?;
        if !file_matches(
            file_io,
            &final_path,
            expected_bytes,
            chunk_bytes,
            metrics,
            &content_hash,
        )
        .await?
        {
            return Err(ConnectorError::WriteError(
                "Iceberg promoted data file failed content verification".into(),
            ));
        }
    }
    if file_io.delete(staging_path).await.is_err() {
        tracing::warn!("Iceberg retained an unreferenced staging file after promotion");
    }
    rebuild_data_file(&file, final_path, partition_spec_id)
}

async fn file_matches(
    file_io: &FileIO,
    path: &str,
    expected_bytes: u64,
    chunk_bytes: u64,
    metrics: &IcebergMetrics,
    expected_hash: &str,
) -> Result<bool, ConnectorError> {
    let input = file_io
        .new_input(path)
        .map_err(|error| file_error("open final data file", &error))?;
    let size = input
        .metadata()
        .await
        .map_err(|error| file_error("read final data-file metadata", &error))?
        .size;
    if size != expected_bytes {
        return Ok(false);
    }
    Ok(hash_file(file_io, path, expected_bytes, chunk_bytes, metrics).await? == expected_hash)
}

async fn hash_file(
    file_io: &FileIO,
    path: &str,
    expected_bytes: u64,
    chunk_bytes: u64,
    metrics: &IcebergMetrics,
) -> Result<String, ConnectorError> {
    let input = file_io
        .new_input(path)
        .map_err(|error| file_error("open data file for verification", &error))?;
    let metadata = input
        .metadata()
        .await
        .map_err(|error| file_error("read data-file metadata", &error))?;
    if metadata.size != expected_bytes {
        return Err(ConnectorError::WriteError(format!(
            "Iceberg data file size is {}, expected {expected_bytes}",
            metadata.size
        )));
    }
    let reader = input
        .reader()
        .await
        .map_err(|error| file_error("open bounded data-file reader", &error))?;
    let mut digest = Sha256::new();
    let mut offset = 0_u64;
    while offset < expected_bytes {
        let end = offset.saturating_add(chunk_bytes).min(expected_bytes);
        let bytes = reader
            .read(offset..end)
            .await
            .map_err(|error| file_error("read bounded data-file chunk", &error))?;
        metrics.set_buffer(0, bytes.len());
        let expected = usize::try_from(end - offset)
            .map_err(|_| ConnectorError::Internal("Iceberg copy chunk exceeds usize".into()))?;
        if bytes.len() != expected {
            metrics.set_buffer(0, 0);
            return Err(ConnectorError::WriteError(
                "Iceberg data-file verification returned a short read".into(),
            ));
        }
        digest.update(&bytes);
        metrics.set_buffer(0, 0);
        offset = end;
    }
    Ok(format!("{:x}", digest.finalize()))
}

async fn copy_file(
    file_io: &FileIO,
    source_path: &str,
    destination_path: &str,
    expected_bytes: u64,
    chunk_bytes: u64,
    metrics: &IcebergMetrics,
) -> Result<(), ConnectorError> {
    let reader = file_io
        .new_input(source_path)
        .map_err(|error| file_error("open staged data file", &error))?
        .reader()
        .await
        .map_err(|error| file_error("open staged data-file reader", &error))?;
    let output = file_io
        .new_output(destination_path)
        .map_err(|error| file_error("open final data file", &error))?;
    let mut writer = output
        .writer()
        .await
        .map_err(|error| file_error("open final data-file writer", &error))?;
    let mut offset = 0_u64;
    while offset < expected_bytes {
        let end = offset.saturating_add(chunk_bytes).min(expected_bytes);
        let bytes = reader
            .read(offset..end)
            .await
            .map_err(|error| file_error("read staged data-file chunk", &error))?;
        metrics.set_buffer(0, bytes.len());
        let expected = usize::try_from(end - offset)
            .map_err(|_| ConnectorError::Internal("Iceberg copy chunk exceeds usize".into()))?;
        if bytes.len() != expected {
            metrics.set_buffer(0, 0);
            return Err(ConnectorError::WriteError(
                "Iceberg staged data-file copy returned a short read".into(),
            ));
        }
        let write = writer.write(bytes).await;
        metrics.set_buffer(0, 0);
        write.map_err(|error| file_error("write final data-file chunk", &error))?;
        offset = end;
    }
    writer
        .close()
        .await
        .map_err(|error| file_error("close final data file", &error))?;
    let final_size = output
        .to_input_file()
        .metadata()
        .await
        .map_err(|error| file_error("verify final data-file size", &error))?
        .size;
    if final_size != expected_bytes {
        return Err(ConnectorError::WriteError(format!(
            "Iceberg promoted data file size is {final_size}, expected {expected_bytes}"
        )));
    }
    Ok(())
}

fn rebuild_data_file(
    file: &DataFile,
    final_path: String,
    partition_spec_id: i32,
) -> Result<DataFile, ConnectorError> {
    let mut builder = DataFileBuilder::default();
    builder
        .content(file.content_type())
        .file_path(final_path)
        .file_format(file.file_format())
        .partition(file.partition().clone())
        .record_count(file.record_count())
        .file_size_in_bytes(file.file_size_in_bytes())
        .column_sizes(file.column_sizes().clone())
        .value_counts(file.value_counts().clone())
        .null_value_counts(file.null_value_counts().clone())
        .nan_value_counts(file.nan_value_counts().clone())
        .lower_bounds(file.lower_bounds().clone())
        .upper_bounds(file.upper_bounds().clone())
        .key_metadata(file.key_metadata().map(<[u8]>::to_vec))
        .split_offsets(file.split_offsets().map(<[i64]>::to_vec))
        .equality_ids(file.equality_ids())
        .first_row_id(file.first_row_id())
        .partition_spec_id(partition_spec_id)
        .referenced_data_file(file.referenced_data_file())
        .content_offset(file.content_offset())
        .content_size_in_bytes(file.content_size_in_bytes());
    if let Some(sort_order_id) = file.sort_order_id() {
        builder.sort_order_id(sort_order_id);
    }
    builder
        .build()
        .map_err(|error| ConnectorError::Internal(format!("rebuild Iceberg data file: {error}")))
}

fn file_error(operation: &str, error: &iceberg::Error) -> ConnectorError {
    ConnectorError::WriteError(format!(
        "Iceberg {operation} ({})",
        crate::lakehouse::iceberg_io::external_error_summary(error)
    ))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use iceberg::spec::{DataContentType, DataFileFormat, Struct};

    use super::*;

    fn names() -> ReplaySafeFileNameGenerator {
        ReplaySafeFileNameGenerator::new(
            "018f0000-0000-7000-8000-000000000001",
            "orders",
            7,
            19,
            true,
        )
    }

    async fn staged_file(
        file_io: &FileIO,
        names: &ReplaySafeFileNameGenerator,
        contents: &'static [u8],
    ) -> DataFile {
        let path = format!("memory:///table/data/{}", names.generate_file_name());
        file_io
            .new_output(&path)
            .unwrap()
            .write(Bytes::from_static(contents))
            .await
            .unwrap();
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path)
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(1)
            .file_size_in_bytes(contents.len() as u64)
            .partition_spec_id(0)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn replay_staging_ids_promote_identical_bytes_to_one_final_path() {
        let file_io = FileIO::new_with_memory();
        let first_names = names();
        let first_staged = staged_file(&file_io, &first_names, b"complete-parquet").await;
        let first_staging_path = first_staged.file_path().to_string();
        let metrics = IcebergMetrics::new(None);
        let first = finalize_coordinated_files(
            &file_io,
            &first_names,
            0,
            1024,
            &metrics,
            vec![first_staged],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert!(!file_io.exists(first_staging_path).await.unwrap());
        assert!(file_io.exists(first.file_path()).await.unwrap());
        assert!(!first.file_path().contains("-stage-"));
        file_io
            .new_output(first.file_path())
            .unwrap()
            .write(Bytes::from(vec![
                b'x';
                usize::try_from(first.file_size_in_bytes())
                    .unwrap()
            ]))
            .await
            .unwrap();

        let replay_names = names();
        let replay_staged = staged_file(&file_io, &replay_names, b"complete-parquet").await;
        let replay_staging_path = replay_staged.file_path().to_string();
        let replay = finalize_coordinated_files(
            &file_io,
            &replay_names,
            0,
            1024,
            &metrics,
            vec![replay_staged],
        )
        .await
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(first, replay);
        assert!(!file_io.exists(replay_staging_path).await.unwrap());
        assert_eq!(
            file_io
                .new_input(replay.file_path())
                .unwrap()
                .read()
                .await
                .unwrap(),
            Bytes::from_static(b"complete-parquet")
        );
    }

    #[tokio::test]
    async fn different_file_contents_cannot_overwrite_the_same_final_path() {
        let file_io = FileIO::new_with_memory();
        let metrics = IcebergMetrics::new(None);
        let first_names = names();
        let first = staged_file(&file_io, &first_names, b"first-layout").await;
        let first =
            finalize_coordinated_files(&file_io, &first_names, 0, 1024, &metrics, vec![first])
                .await
                .unwrap()
                .pop()
                .unwrap();

        let replay_names = names();
        let replay = staged_file(&file_io, &replay_names, b"second-layout").await;
        let replay =
            finalize_coordinated_files(&file_io, &replay_names, 0, 1024, &metrics, vec![replay])
                .await
                .unwrap()
                .pop()
                .unwrap();
        assert_ne!(first.file_path(), replay.file_path());
        assert!(file_io.exists(first.file_path()).await.unwrap());
        assert!(file_io.exists(replay.file_path()).await.unwrap());
    }
}
