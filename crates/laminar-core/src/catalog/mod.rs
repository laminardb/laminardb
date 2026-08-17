//! Feature-neutral catalog identity types.

use serde::{Deserialize, Serialize};

/// Exclusive owner of a user catalog identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogObjectKind {
    /// Ingest source.
    Source,
    /// Output sink.
    Sink,
    /// Reference table.
    Table,
    /// Lookup table.
    LookupTable,
    /// Streaming query output.
    Stream,
    /// Materialized streaming view.
    MaterializedView,
}

impl std::fmt::Display for CatalogObjectKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Source => "source",
            Self::Sink => "sink",
            Self::Table => "table",
            Self::LookupTable => "lookup table",
            Self::Stream => "stream",
            Self::MaterializedView => "materialized view",
        })
    }
}
