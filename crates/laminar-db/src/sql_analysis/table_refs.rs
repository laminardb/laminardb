//! Source-table discovery: table-reference extraction and single-source certification.
//!
//! Traversal resolves window TVFs (`FROM TUMBLE(t, ts, …)`) to their inner source and
//! skips CTE names, so discovered names are the physical relations a pipeline must bind.

use std::ops::ControlFlow;

use rustc_hash::FxHashSet;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectNamePart, SetExpr, Statement, TableFactor,
    Visit, Visitor,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use super::ast::{is_inline_unnest_factor, parse_standard_query};
use super::hazards::QueryHazardVisitor;

/// Returns the deduplicated set of table names from FROM/JOIN clauses.
///
/// For self-join detection use [`single_source_table`] (which counts occurrences).
pub(crate) fn extract_table_references(sql: &str) -> FxHashSet<String> {
    let mut tables = FxHashSet::default();
    let dialect = GenericDialect {};
    if let Ok(statements) = Parser::parse_sql(&dialect, sql) {
        for stmt in &statements {
            if let Statement::Query(query) = stmt {
                let _ = query.visit(&mut TableReferenceVisitor::new(&mut tables));
            }
        }
    }
    tables
}

struct TableReferenceVisitor<'a> {
    tables: &'a mut FxHashSet<String>,
    cte_scopes: Vec<Vec<Ident>>,
}

impl<'a> TableReferenceVisitor<'a> {
    fn new(tables: &'a mut FxHashSet<String>) -> Self {
        Self {
            tables,
            cte_scopes: Vec::new(),
        }
    }

    fn is_cte(&self, reference: &str) -> bool {
        self.cte_scopes.iter().rev().flatten().any(|alias| {
            if alias.quote_style.is_some() {
                alias.value == reference
            } else {
                alias.value.eq_ignore_ascii_case(reference)
            }
        })
    }
}

impl Visitor for TableReferenceVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.cte_scopes.push(
            query
                .with
                .as_ref()
                .map(|with| {
                    with.cte_tables
                        .iter()
                        .map(|cte| cte.alias.name.clone())
                        .collect()
                })
                .unwrap_or_default(),
        );
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.cte_scopes.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        match factor {
            TableFactor::Table { .. } if is_inline_unnest_factor(factor) => {}
            TableFactor::Table { name, args, .. } => {
                let source = resolve_tvf_source(name, args.as_ref());
                if !self.is_cte(&source) {
                    self.tables.insert(source);
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

/// Returns the single source table name only if there is exactly one FROM/JOIN occurrence.
///
/// A self-join (`events e1 JOIN events e2`) returns `None` even though the base name repeats.
pub(crate) fn single_source_table(sql: &str) -> Option<String> {
    let query = parse_standard_query(sql)?;
    let mut visitor = QueryHazardVisitor::new(None);
    let _ = query.visit(&mut visitor);
    if visitor.hazards.unnest || visitor.hazards.nested_query {
        return None;
    }
    let mut tables = Vec::new();
    collect_tables_counting(query.body.as_ref(), &mut tables);
    if tables.len() == 1 {
        tables.into_iter().next()
    } else {
        None
    }
}

fn collect_tables_counting(set_expr: &SetExpr, tables: &mut Vec<String>) {
    match set_expr {
        SetExpr::Select(select) => {
            for table_with_joins in &select.from {
                collect_factor_counting(&table_with_joins.relation, tables);
                for join in &table_with_joins.joins {
                    collect_factor_counting(&join.relation, tables);
                }
            }
        }
        SetExpr::SetOperation { left, right, .. } => {
            collect_tables_counting(left.as_ref(), tables);
            collect_tables_counting(right.as_ref(), tables);
        }
        SetExpr::Query(query) => {
            collect_tables_counting(query.body.as_ref(), tables);
        }
        _ => {}
    }
}

fn collect_factor_counting(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, args, .. } => {
            tables.push(resolve_tvf_source(name, args.as_ref()));
        }
        TableFactor::Derived { subquery, .. } => {
            collect_tables_counting(subquery.body.as_ref(), tables);
        }
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_factor_counting(&table_with_joins.relation, tables);
            for join in &table_with_joins.joins {
                collect_factor_counting(&join.relation, tables);
            }
        }
        // Lateral UNNEST, TVFs, etc. block the single-source path.
        _ => tables.push("\u{0}non_table_factor".to_string()),
    }
}

/// Resolve the real source table from a `TableFactor::Table`.
///
/// sqlparser parses `FROM TUMBLE(events, ts, ...)` as a table named `TUMBLE` with args;
/// for window TVFs the first arg is the actual source.
pub(super) fn resolve_tvf_source(
    name: &sqlparser::ast::ObjectName,
    args: Option<&sqlparser::ast::TableFunctionArgs>,
) -> String {
    let name_str = match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)] => normalize_ident(ident),
        _ => name.to_string(),
    };
    let base_name = name_str.rsplit('.').next().unwrap_or(&name_str);
    if let Some(tfa) = args {
        if is_window_tvf(base_name) {
            if let Some(source) = first_ident_arg(&tfa.args) {
                return source;
            }
        }
    }
    name_str
}

pub(super) fn is_window_tvf(name: &str) -> bool {
    name.eq_ignore_ascii_case("TUMBLE")
        || name.eq_ignore_ascii_case("HOP")
        || name.eq_ignore_ascii_case("SESSION")
        || name.eq_ignore_ascii_case("SLIDE")
}

fn first_ident_arg(args: &[FunctionArg]) -> Option<String> {
    match args.first()? {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(id))) => {
            Some(normalize_ident(id))
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
            let mut buf = String::new();
            for (i, part) in parts.iter().enumerate() {
                if i > 0 {
                    buf.push('.');
                }
                buf.push_str(&normalize_ident(part));
            }
            Some(buf)
        }
        _ => None,
    }
}

fn normalize_ident(ident: &Ident) -> String {
    ident.value.clone()
}
