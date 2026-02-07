//! Cache mode configuration for reference tables.
//!
//! Controls how dimension table rows are cached in memory:
//! - `Full`: all rows in memory (default, suitable for small tables)
//! - `Partial`: LRU cache of hot keys with xor filter for negative lookups
//! - `None`: no caching (direct backing store access)

use crate::error::DbError;

/// How a reference table's rows are cached in memory.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TableCacheMode {
    /// All rows kept in memory (default). Best for tables < 10M rows.
    #[default]
    Full,
    /// LRU cache of hot keys. Cold keys fall through to the backing store.
    Partial {
        /// Maximum number of entries in the LRU cache.
        max_entries: usize,
    },
    /// No caching — every lookup goes to the backing store.
    None,
}

/// Default maximum entries for partial cache mode.
const DEFAULT_PARTIAL_MAX_ENTRIES: usize = 500_000;

impl TableCacheMode {
    /// Return the max entries for partial mode, or `None` for other modes.
    #[allow(dead_code)]
    pub fn max_entries(&self) -> Option<usize> {
        match self {
            Self::Partial { max_entries } => Some(*max_entries),
            _ => None,
        }
    }

    /// Create a `Partial` mode with the default max entries.
    pub fn partial_default() -> Self {
        Self::Partial {
            max_entries: DEFAULT_PARTIAL_MAX_ENTRIES,
        }
    }
}

/// Parse a cache mode string from DDL `WITH (cache_mode = '...')`.
///
/// Supported values:
/// - `"full"` → `TableCacheMode::Full`
/// - `"partial"` → `TableCacheMode::Partial { max_entries: 500_000 }`
/// - `"none"` → `TableCacheMode::None`
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
