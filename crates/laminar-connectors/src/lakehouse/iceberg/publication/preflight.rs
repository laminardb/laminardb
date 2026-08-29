use iceberg::spec::DataFile;

use crate::error::ConnectorError;
use crate::lakehouse::iceberg_io::external_error_summary;

const DATA_FILE_METADATA_CONCURRENCY: usize = 16;

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
    for files in data_files.chunks(DATA_FILE_METADATA_CONCURRENCY) {
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
        .map_err(|_| {
            ConnectorError::WriteError(
                "[LDB-ICEBERG-DATA-FILE-PREFLIGHT-TIMEOUT] coordinated data-file metadata validation exceeded its deadline"
                    .into(),
            )
        })?
        .map_err(|error| data_file_metadata_error(&error))?;
    if metadata.size != file.file_size_in_bytes() {
        return Err(ConnectorError::TransactionError(format!(
            "[LDB-ICEBERG-DATA-FILE-SIZE] coordinated data file has {} bytes; descriptor expects {}",
            metadata.size,
            file.file_size_in_bytes()
        )));
    }
    Ok(())
}

fn data_file_metadata_error(error: &iceberg::Error) -> ConnectorError {
    ConnectorError::WriteError(format!(
        "[LDB-ICEBERG-DATA-FILE-PREFLIGHT] coordinated data-file metadata validation failed ({})",
        external_error_summary(error)
    ))
}
