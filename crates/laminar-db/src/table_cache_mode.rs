//! Cache mode for reference tables: Full (all rows), Partial (LRU), or None.

use crate::error::DbError;

/// How a reference table's rows are cached in memory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TableCacheMode {
    /// All rows in memory (default).
    #[default]
    Full,
    /// LRU cache; cold keys fall through to the backing store.
    Partial { max_entries: usize },
    /// No caching.
    None,
}

/// Default maximum entries for partial cache mode.
const DEFAULT_PARTIAL_MAX_ENTRIES: usize = 500_000;

impl TableCacheMode {
    #[allow(dead_code)] // test-only
    pub fn max_entries(&self) -> Option<usize> {
        match self {
            Self::Partial { max_entries } => Some(*max_entries),
            _ => None,
        }
    }

    pub fn partial_default() -> Self {
        Self::Partial {
            max_entries: DEFAULT_PARTIAL_MAX_ENTRIES,
        }
    }
}

/// Parse a `cache_mode` DDL option; accepts `"full"`, `"partial"`, `"none"`.
pub(crate) fn parse_cache_mode(s: &str) -> Result<TableCacheMode, DbError> {
    match s.to_lowercase().as_str() {
        "full" => Ok(TableCacheMode::Full),
        "partial" => Ok(TableCacheMode::partial_default()),
        "none" => Ok(TableCacheMode::None),
        _ => Err(DbError::InvalidOperation(format!(
            "Unknown cache_mode '{s}': expected full, partial, or none"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cache_mode_full() {
        assert_eq!(parse_cache_mode("full").unwrap(), TableCacheMode::Full);
        assert_eq!(parse_cache_mode("FULL").unwrap(), TableCacheMode::Full);
    }

    #[test]
    fn test_parse_cache_mode_partial() {
        let mode = parse_cache_mode("partial").unwrap();
        assert_eq!(
            mode,
            TableCacheMode::Partial {
                max_entries: 500_000
            }
        );
    }

    #[test]
    fn test_parse_cache_mode_none() {
        assert_eq!(parse_cache_mode("none").unwrap(), TableCacheMode::None);
        assert_eq!(parse_cache_mode("NONE").unwrap(), TableCacheMode::None);
    }

    #[test]
    fn test_parse_cache_mode_invalid() {
        let err = parse_cache_mode("bogus").unwrap_err();
        assert!(err.to_string().contains("Unknown cache_mode"));
    }

    #[test]
    fn test_default_is_full() {
        assert_eq!(TableCacheMode::default(), TableCacheMode::Full);
    }

    #[test]
    fn test_max_entries() {
        assert_eq!(TableCacheMode::Full.max_entries(), None);
        assert_eq!(TableCacheMode::None.max_entries(), None);
        assert_eq!(
            TableCacheMode::Partial { max_entries: 1000 }.max_entries(),
            Some(1000)
        );
    }
}
