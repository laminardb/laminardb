use std::ops::Range;

use bytes::Bytes;
use iceberg::io::FileRead;
use iceberg::spec::DataFile;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_io::external_error_summary;
use crate::lakehouse::iceberg_scan::preflight_current_snapshot_manifest_list;

const DATA_FILE_PREFLIGHT_CONCURRENCY: usize = 16;
const PARQUET_HEADER_BYTES: u64 = 4;
const PARQUET_TRAILER_BYTES: u64 = 8;
const PARQUET_MIN_BYTES: u64 = PARQUET_HEADER_BYTES + PARQUET_TRAILER_BYTES;
const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

pub(super) async fn validate_data_file_objects(
    table: &iceberg::table::Table,
    data_files: &[DataFile],
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    if deadline <= tokio::time::Instant::now() {
        return Err(ConnectorError::TransactionError(
            "Iceberg coordinated publication deadline elapsed during data-file preflight".into(),
        ));
    }
    preflight_current_snapshot_manifest_list(table, deadline).await?;
    for files in data_files.chunks(DATA_FILE_PREFLIGHT_CONCURRENCY) {
        futures_util::future::try_join_all(
            files
                .iter()
                .map(|file| validate_data_file_object(table, file, deadline)),
        )
        .await?;
    }
    Ok(())
}

async fn validate_data_file_object(
    table: &iceberg::table::Table,
    file: &DataFile,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let input = table
        .file_io()
        .new_input(file.file_path())
        .map_err(|error| data_file_metadata_error(&error))?;
    let metadata = tokio::time::timeout_at(deadline, input.metadata())
        .await
        .map_err(|_| data_file_preflight_timeout())?
        .map_err(|error| data_file_metadata_error(&error))?;
    if metadata.size != file.file_size_in_bytes() {
        return Err(ConnectorError::TransactionError(format!(
            "[LDB-ICEBERG-DATA-FILE-SIZE] coordinated data file has {} bytes; descriptor expects {}",
            metadata.size,
            file.file_size_in_bytes()
        )));
    }
    validate_parquet_envelope(&input, metadata.size, deadline).await?;
    Ok(())
}

async fn validate_parquet_envelope(
    input: &iceberg::io::InputFile,
    file_size: u64,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    if file_size < PARQUET_MIN_BYTES {
        return Err(incomplete_parquet_error());
    }
    let reader = tokio::time::timeout_at(deadline, input.reader())
        .await
        .map_err(|_| data_file_preflight_timeout())?
        .map_err(|error| data_file_metadata_error(&error))?;
    let trailer_start = file_size - PARQUET_TRAILER_BYTES;
    let (header, trailer) = tokio::try_join!(
        read_exact_range(reader.as_ref(), 0..PARQUET_HEADER_BYTES, deadline),
        read_exact_range(reader.as_ref(), trailer_start..file_size, deadline),
    )?;
    if header.as_ref() != PARQUET_MAGIC || &trailer[4..] != PARQUET_MAGIC {
        return Err(incomplete_parquet_error());
    }
    let mut encoded_footer_len = [0_u8; 4];
    encoded_footer_len.copy_from_slice(&trailer[..4]);
    let footer_len = u64::from(u32::from_le_bytes(encoded_footer_len));
    if footer_len == 0 || footer_len > file_size - PARQUET_MIN_BYTES {
        return Err(incomplete_parquet_error());
    }
    Ok(())
}

async fn read_exact_range(
    reader: &dyn FileRead,
    range: Range<u64>,
    deadline: tokio::time::Instant,
) -> Result<Bytes, ConnectorError> {
    let expected = usize::try_from(range.end - range.start)
        .map_err(|_| ConnectorError::Internal("Iceberg preflight range exceeds usize".into()))?;
    let bytes = tokio::time::timeout_at(deadline, reader.read(range))
        .await
        .map_err(|_| data_file_preflight_timeout())?
        .map_err(|error| data_file_metadata_error(&error))?;
    if bytes.len() != expected {
        return Err(incomplete_parquet_error());
    }
    Ok(bytes)
}

fn data_file_preflight_timeout() -> ConnectorError {
    ConnectorError::WriteError(
        "[LDB-ICEBERG-DATA-FILE-PREFLIGHT-TIMEOUT] coordinated data-file validation exceeded its deadline"
            .into(),
    )
}

fn incomplete_parquet_error() -> ConnectorError {
    ConnectorError::TransactionError(
        "[LDB-ICEBERG-DATA-FILE-PARQUET] coordinated data file is not a complete Parquet object"
            .into(),
    )
}

fn data_file_metadata_error(error: &iceberg::Error) -> ConnectorError {
    ConnectorError::WriteError(format!(
        "[LDB-ICEBERG-DATA-FILE-PREFLIGHT] coordinated data-file metadata validation failed ({})",
        external_error_summary(error)
    ))
}
