//! Cluster query-hazard scanning: runtime/AI functions, unnest, nested queries, and
//! managed window-source certification for cluster admission.
//!
//! INVARIANT: every check is fail-closed — a query shape that cannot be proven safe for
//! cluster execution reports a hazard rather than being admitted.

use std::ops::ControlFlow;

use laminar_sql::parser::WindowRewriter;
use laminar_sql::translator::WindowOperatorConfig;
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, ObjectName,
    SelectFlavor, SetExpr, TableFactor, Visit, Visitor,
};

use super::ai::is_ai_function_name;
use super::ast::{ident_is_wallclock, parse_standard_query, single_function_ident};
use super::table_refs::resolve_tvf_source;

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
pub(crate) struct ClusterQueryHazards {
    pub(crate) runtime_function: bool,
    pub(crate) ai_function: bool,
    pub(crate) unnest: bool,
    pub(crate) nested_query: bool,
}

fn is_cluster_runtime_function(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "now"
            | "current_timestamp"
            | "current_date"
            | "today"
            | "current_time"
            | "proctime"
            | "watermark"
    )
}

fn supported_window_marker(name: &str) -> Option<(&'static str, bool)> {
    match name.to_ascii_lowercase().as_str() {
        "tumble" => Some(("tumble", true)),
        "tumble_end" => Some(("tumble", false)),
        "hop" => Some(("hop", true)),
        "hop_end" => Some(("hop", false)),
        "session" => Some(("session", true)),
        _ => None,
    }
}

fn is_unsupported_window_marker(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "cumulate" | "cumulate_end" | "slide"
    )
}

fn window_time_arg_matches(function: &sqlparser::ast::Function, expected: &str) -> bool {
    let FunctionArguments::List(arguments) = &function.args else {
        return false;
    };
    matches!(
        arguments.args.first(),
        Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))))
            if ident.value == expected
    )
}

fn window_marker_matches(
    function: &sqlparser::ast::Function,
    base_name: &str,
    expected: &WindowOperatorConfig,
) -> bool {
    let FunctionArguments::List(arguments) = &function.args else {
        return false;
    };
    if function.uses_odbc_syntax
        || !matches!(function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
        || arguments.duplicate_treatment.is_some()
        || !arguments.clauses.is_empty()
        || !window_time_arg_matches(function, &expected.time_column)
    {
        return false;
    }

    let mut canonical = function.clone();
    canonical.name = ObjectName::from(vec![Ident::new(base_name)]);
    let Ok(Some(window)) = WindowRewriter::extract_window_function(&Expr::Function(canonical))
    else {
        return false;
    };
    let Ok(actual) = WindowOperatorConfig::from_window_function(&window) else {
        return false;
    };
    actual.window_type == expected.window_type
        && actual.time_column == expected.time_column
        && actual.size == expected.size
        && actual.slide == expected.slide
        && actual.gap == expected.gap
        && actual.offset_ms == expected.offset_ms
}

pub(super) struct QueryHazardVisitor<'a> {
    pub(super) hazards: ClusterQueryHazards,
    query_count: usize,
    expected_window: Option<&'a WindowOperatorConfig>,
    saw_window_start: bool,
    invalid_window: bool,
}

impl<'a> QueryHazardVisitor<'a> {
    pub(super) fn new(expected_window: Option<&'a WindowOperatorConfig>) -> Self {
        Self {
            hazards: ClusterQueryHazards::default(),
            query_count: 0,
            expected_window,
            saw_window_start: false,
            invalid_window: false,
        }
    }
}

impl Visitor for QueryHazardVisitor<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.query_count += 1;
        self.hazards.nested_query |= self.query_count > 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<Self::Break> {
        self.hazards.unnest |= match factor {
            TableFactor::UNNEST { .. } => true,
            TableFactor::Table {
                name,
                args: Some(_),
                ..
            }
            | TableFactor::Function { name, .. } => single_function_ident(name)
                .is_some_and(|ident| ident.value.eq_ignore_ascii_case("unnest")),
            _ => false,
        };
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if let Expr::Identifier(ident) = expr {
            self.hazards.runtime_function |=
                ident.quote_style.is_none() && ident_is_wallclock(&ident.value);
            return ControlFlow::Continue(());
        }
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };
        let Some(ident) = single_function_ident(&function.name) else {
            return ControlFlow::Continue(());
        };
        let name = ident.value.as_str();
        self.hazards.runtime_function |= is_cluster_runtime_function(name);
        self.hazards.ai_function |= is_ai_function_name(name);
        self.hazards.unnest |= name.eq_ignore_ascii_case("unnest");
        if let Some((base_name, is_start)) = supported_window_marker(name) {
            self.saw_window_start |= is_start;
            if let Some(expected) = self.expected_window {
                self.invalid_window |= !window_marker_matches(function, base_name, expected);
            }
        } else if is_unsupported_window_marker(name) {
            self.invalid_window = true;
        }
        ControlFlow::Continue(())
    }
}

pub(crate) fn cluster_query_hazards(sql: &str) -> Option<ClusterQueryHazards> {
    let query = parse_standard_query(sql)?;
    let mut visitor = QueryHazardVisitor::new(None);
    let _ = query.visit(&mut visitor);
    Some(visitor.hazards)
}

/// The single managed source relation of a certified managed window query, if the query
/// shape is exactly the managed `TUMBLE`/`HOP`/`SESSION` aggregate form.
pub(crate) fn managed_core_window_source(
    sql: &str,
    window: &WindowOperatorConfig,
) -> Option<String> {
    let query = parse_standard_query(sql)?;
    let mut visitor = QueryHazardVisitor::new(Some(window));
    let _ = query.visit(&mut visitor);
    if visitor.hazards.runtime_function
        || visitor.hazards.ai_function
        || visitor.hazards.unnest
        || visitor.hazards.nested_query
        || !visitor.saw_window_start
        || visitor.invalid_window
    {
        return None;
    }
    if query.with.is_some()
        || query.order_by.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let GroupByExpr::Expressions(group_exprs, group_modifiers) = &select.group_by else {
        return None;
    };
    if !group_modifiers.is_empty()
        || !group_exprs.iter().any(|expression| {
            matches!(expression, Expr::Function(function)
            if single_function_ident(&function.name).is_some_and(|ident| {
                supported_window_marker(&ident.value).is_some_and(|(_, is_start)| is_start)
            }))
        })
    {
        return None;
    }
    if select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !matches!(select.flavor, SelectFlavor::Standard)
    {
        return None;
    }
    let [from] = select.from.as_slice() else {
        return None;
    };
    if !from.joins.is_empty() {
        return None;
    }
    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_hints,
        ..
    } = &from.relation
    else {
        return None;
    };
    if alias
        .as_ref()
        .is_some_and(|alias| !alias.columns.is_empty())
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || !index_hints.is_empty()
    {
        return None;
    }
    Some(resolve_tvf_source(name, None))
}

#[cfg(test)]
pub(crate) fn query_uses_runtime_clock(sql: &str) -> bool {
    cluster_query_hazards(sql).is_none_or(|hazards| hazards.runtime_function)
}
