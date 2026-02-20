//! Custom SQL dialect for LaminarDB streaming extensions.
//!
//! Extends `GenericDialect` to support streaming-specific features
//! like FILTER clauses during aggregation.

use sqlparser::dialect::{Dialect, GenericDialect};

/// SQL dialect for LaminarDB streaming SQL.
///
/// Delegates to [`GenericDialect`] for most behavior but enables
/// streaming-specific features:
/// - `FILTER` clauses during aggregation
/// - Backtick-delimited identifiers
#[derive(Debug, Default)]
pub struct LaminarDialect {
    generic: GenericDialect,
}

impl Dialect for LaminarDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.generic.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.generic.is_identifier_part(ch)
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        self.generic.is_delimited_identifier_start(ch) || ch == '`'
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_timestamp_versioning(&self) -> bool {
        true
    }
}
