//! Shared sqlparser parsing and identifier predicates used by the analysis products.
//!
//! INVARIANT: parse failures are meaningful to callers — `parse_standard_query` returns `None`
//! and each caller decides its fail-open/fail-closed response; this module never guesses.

use sqlparser::ast::{
    Ident, ObjectName, ObjectNamePart, Query, Statement, TableFactor, WildcardAdditionalOptions,
};

/// Parse exactly one standard (non-streaming) SELECT statement from `sql`.
pub(super) fn parse_standard_query(sql: &str) -> Option<Box<Query>> {
    let mut statements = laminar_sql::parse_streaming_sql(sql).ok()?.into_iter();
    let laminar_sql::parser::StreamingStatement::Standard(statement) = statements.next()? else {
        return None;
    };
    if statements.next().is_some() {
        return None;
    }
    let Statement::Query(query) = *statement else {
        return None;
    };
    Some(query)
}

pub(super) fn single_function_ident(name: &ObjectName) -> Option<&Ident> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => Some(ident),
        _ => None,
    }
}

pub(super) fn is_inline_unnest_factor(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::UNNEST { .. } => true,
        TableFactor::Table {
            name,
            args: Some(_),
            ..
        }
        | TableFactor::Function { name, .. } => {
            name.0.len() == 1 && name.to_string().eq_ignore_ascii_case("unnest")
        }
        _ => false,
    }
}

pub(super) fn wildcard_has_options(options: &WildcardAdditionalOptions) -> bool {
    options.opt_ilike.is_some()
        || options.opt_exclude.is_some()
        || options.opt_except.is_some()
        || options.opt_replace.is_some()
        || options.opt_rename.is_some()
}

/// Identifier equality for unquoted (case-insensitive) comparisons across join rewrites.
pub(super) fn unquoted_identifier_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

pub(super) fn ident_is_wallclock(name: &str) -> bool {
    name.eq_ignore_ascii_case("now") || name.eq_ignore_ascii_case("current_timestamp")
}
