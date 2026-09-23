//! Delta Lake table provider integration with `DataFusion`.
//!
//! This module provides a thin helper to open a Delta Lake table and
//! register it as a `TableProvider` in a `SessionContext`.
//!
//! # Usage
//!
//! ```rust,ignore
//! use laminar_connectors::lakehouse::delta_table_provider::register_delta_table;
//! use datafusion::prelude::SessionContext;
//! use std::collections::HashMap;
//!
//! let ctx = SessionContext::new();
//! register_delta_table(&ctx, "my_table", "/path/to/delta/table", HashMap::new()).await?;
//!
//! // Now query it:
//! let df = ctx.sql("SELECT * FROM my_table").await?;
//! ```

#[cfg(feature = "delta-lake")]
use std::collections::HashMap;

#[cfg(feature = "delta-lake")]
use std::sync::Arc;

#[cfg(feature = "delta-lake")]
use datafusion::prelude::SessionContext;

#[cfg(feature = "delta-lake")]
use tracing::info;

#[cfg(feature = "delta-lake")]
use crate::error::ConnectorError;

/// Opens a Delta Lake table and registers it as a table provider in the
/// given `DataFusion` `SessionContext`.
///
/// # Arguments
///
/// * `ctx` - The `DataFusion` session context to register in
/// * `name` - The SQL table name (e.g., `"trades"`)
/// * `table_uri` - Path to the Delta Lake table (local, `s3://`, `az://`, `gs://`)
/// * `storage_options` - Storage credentials and configuration
///
/// # Errors
///
/// Returns `ConnectorError::ConnectionFailed` if the table cannot be opened,
/// or `ConnectorError::Internal` if registration fails.
#[cfg(feature = "delta-lake")]
#[allow(clippy::implicit_hasher)]
pub async fn register_delta_table(
    ctx: &SessionContext,
    name: &str,
    table_uri: &str,
    storage_options: HashMap<String, String>,
) -> Result<(), ConnectorError> {
    use super::delta_io;

    // The URI is deliberately excluded: validation below rejects signed queries, but this log
    // occurs before validation and must never publish one.
    info!(name, "registering Delta Lake table as TableProvider");

    // Open the existing table.
    let table = delta_io::open_or_create_table(table_uri, storage_options, None).await?;

    // Register the table's object store with the session so scans can resolve
    // non-local URLs (s3://, az://, gs://); without it, reading an
    // object-store-backed table fails with "No suitable object store found".
    // (Local-filesystem tables use DataFusion's built-in store.)
    table
        .update_datafusion_session(&ctx.state())
        .map_err(|e| ConnectorError::Internal(format!("register Delta object store: {e}")))?;

    // Build a DeltaTableProvider (which implements TableProvider) from the table.
    let provider =
        table.table_provider().build().await.map_err(|e| {
            ConnectorError::Internal(format!("failed to build table provider: {e}"))
        })?;

    ctx.register_table(
        datafusion::common::TableReference::bare(name),
        Arc::new(provider),
    )
    .map_err(|e| {
        ConnectorError::Internal(format!("failed to register Delta table '{name}': {e}"))
    })?;

    info!(name, "Delta Lake table registered successfully");

    Ok(())
}

#[cfg(all(test, feature = "delta-lake"))]
mod tests;
