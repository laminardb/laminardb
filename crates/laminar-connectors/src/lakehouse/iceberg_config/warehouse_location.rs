//! Iceberg warehouse location validation and consumer-specific adaptation.

use crate::error::ConnectorError;
use crate::storage::{StorageConsumer, StorageLocation};

pub(super) fn canonicalize(warehouse: &str) -> Result<String, ConnectorError> {
    let test_memory_warehouse = cfg!(test)
        && warehouse
            .get(..9)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("memory://"));
    if !warehouse.contains("://") || test_memory_warehouse {
        return Ok(warehouse.to_string());
    }

    StorageLocation::parse(warehouse)
        .and_then(|location| location.adapt(StorageConsumer::Iceberg))
        .map(|adapted| adapted.url)
        .map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "invalid catalog.warehouse storage location: {error}"
            ))
        })
}
