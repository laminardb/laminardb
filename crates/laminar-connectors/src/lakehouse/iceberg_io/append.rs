use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::Catalog;

use crate::error::ConnectorError;

use super::external_error_summary;

pub(crate) enum GeneratedAppendError {
    Preflight(ConnectorError),
    Commit(iceberg::Error),
}

#[derive(Clone, Copy)]
enum AppendPathProvenance {
    Arbitrary,
    WriterGenerated,
}

impl AppendPathProvenance {
    const fn check_duplicates(self) -> bool {
        matches!(self, Self::Arbitrary)
    }
}

/// Append `data_files` in one Iceberg transaction.
///
/// # Errors
/// Returns [`ConnectorError::OutcomeUnknown`] when the catalog does not acknowledge the commit.
pub async fn commit_data_files_append(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<iceberg::spec::DataFile>,
) -> Result<Table, ConnectorError> {
    commit_append(table, catalog, data_files, AppendPathProvenance::Arbitrary)
        .await
        .map_err(|error| iceberg_commit_error(&error))
}

pub(crate) async fn commit_generated_data_files_append(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<iceberg::spec::DataFile>,
    deadline: tokio::time::Instant,
) -> Result<Table, GeneratedAppendError> {
    // INVARIANT: callers use writer-generated paths in a unique deployment/epoch namespace.
    super::super::iceberg_scan::preflight_current_snapshot_manifest_list(table, deadline)
        .await
        .map_err(GeneratedAppendError::Preflight)?;
    commit_append(
        table,
        catalog,
        data_files,
        AppendPathProvenance::WriterGenerated,
    )
    .await
    .map_err(GeneratedAppendError::Commit)
}

async fn commit_append(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<iceberg::spec::DataFile>,
    path_provenance: AppendPathProvenance,
) -> iceberg::Result<Table> {
    let tx = Transaction::new(table);
    let tx = if data_files.is_empty() {
        tx
    } else {
        tx.fast_append()
            .with_check_duplicate(path_provenance.check_duplicates())
            .add_data_files(data_files)
            .apply(tx)?
    };
    tx.commit(catalog).await
}

pub(crate) fn iceberg_commit_error(error: &iceberg::Error) -> ConnectorError {
    use iceberg::ErrorKind;

    let kind = error.kind();
    let retryable = error.retryable();
    let summary = external_error_summary(error);
    match kind {
        ErrorKind::CatalogCommitConflicts => {
            ConnectorError::WriteError(format!("Iceberg catalog commit conflict ({summary})"))
        }
        ErrorKind::PreconditionFailed
        | ErrorKind::DataInvalid
        | ErrorKind::NamespaceAlreadyExists
        | ErrorKind::TableAlreadyExists
        | ErrorKind::NamespaceNotFound
        | ErrorKind::TableNotFound
        | ErrorKind::FeatureUnsupported => {
            ConnectorError::TransactionError(format!("Iceberg catalog rejected commit ({summary})"))
        }
        ErrorKind::Unexpected => ConnectorError::outcome_unknown(
            format!("Iceberg catalog commit may have applied ({summary})"),
            retryable,
        ),
        _ => ConnectorError::outcome_unknown(
            format!("Iceberg catalog returned an unclassified commit failure ({summary})"),
            retryable,
        ),
    }
}
