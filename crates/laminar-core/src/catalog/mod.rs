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

/// Marker between an owner's name and a stage index in an internal stage's name.
pub const STAGE_MARKER: &str = "$stage";

/// The catalog object that owns `name` when `name` is an internal stage `<owner>$stage<n>`: a
/// hidden stream the planner splits out of the owner's query, whose state belongs to the owner.
#[must_use]
pub fn stage_owner(name: &str) -> Option<&str> {
    let (owner, index) = name.rsplit_once(STAGE_MARKER)?;
    (!owner.is_empty() && !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit()))
        .then_some(owner)
}

#[cfg(test)]
mod tests {
    use super::stage_owner;

    #[test]
    fn stage_names_round_trip_to_their_owner() {
        assert_eq!(stage_owner("out$stage1"), Some("out"));
        assert_eq!(stage_owner("a$stage2$stage12"), Some("a$stage2"));
        for name in ["out", "$stage1", "out$stage", "out$stagex", "out$stage1a"] {
            assert_eq!(stage_owner(name), None, "{name}");
        }
    }
}
