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

    info!(name, table_uri, "registering Delta Lake table as TableProvider");

    // Open the existing table.
    let table = delta_io::open_or_create_table(table_uri, storage_options, None).await?;

    // Build a DeltaTableProvider (which implements TableProvider) from the table.
    let provider = table
        .table_provider()
        .build()
        .await
        .map_err(|e| ConnectorError::Internal(format!("failed to build table provider: {e}")))?;

    ctx.register_table(name, Arc::new(provider)).map_err(|e| {
        ConnectorError::Internal(format!("failed to register Delta table '{name}': {e}"))
    })?;

    info!(name, table_uri, "Delta Lake table registered successfully");

    Ok(())
}

#[cfg(all(test, feature = "delta-lake"))]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use tempfile::TempDir;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[allow(clippy::cast_precision_loss)]
    fn test_batch(n: usize) -> arrow_array::RecordBatch {
        let ids: Vec<i64> = (0..n as i64).collect();
        let names: Vec<&str> = (0..n).map(|_| "test").collect();
        let values: Vec<f64> = (0..n).map(|i| i as f64 * 1.5).collect();

        arrow_array::RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_register_and_query_delta_table() {
        use super::super::delta_io;
        use deltalake::protocol::SaveMode;

        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_str().unwrap();

        // Create a Delta table with some data.
        let schema = test_schema();
        let table = delta_io::open_or_create_table(table_path, HashMap::new(), Some(&schema))
            .await
            .unwrap();

        let batch = test_batch(10);
        let (_table, version) = delta_io::write_batches(
            table,
            vec![batch],
            "test-writer",
            1,
            SaveMode::Append,
            None,
        )
        .await
        .unwrap();
        assert_eq!(version, 1);

        // Register as TableProvider and query.
        let ctx = SessionContext::new();
        register_delta_table(&ctx, "test_delta", table_path, HashMap::new())
            .await
            .unwrap();

        let df = ctx.sql("SELECT COUNT(*) AS cnt FROM test_delta").await.unwrap();
        let results = df.collect().await.unwrap();

        assert_eq!(results.len(), 1);
        let count = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 10);
    }
}
