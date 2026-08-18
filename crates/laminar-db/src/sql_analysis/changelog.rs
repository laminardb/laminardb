//! Changelog-weight admission analysis and changelog⋈static enrichment detection.
//!
//! The engine owns the differential `__weight` column. These checks decide whether a query
//! provably preserves it (fail-closed: parse failures count as references/unsafe modifiers),
//! and whether a join is the single changelog-left/static-right equi-join the dedicated
//! enrichment operator executes.

use std::ops::ControlFlow;

use rustc_hash::FxHashSet;
use sqlparser::ast::{
    Expr, GroupByExpr, Ident, ObjectNamePart, SelectFlavor, SelectItem, SetExpr, Statement,
    TableFactor, Visit, Visitor,
};

use super::ast::{parse_standard_query, unquoted_identifier_eq, wildcard_has_options};

/// Render one single-source projection/filter so every execution path selects the engine-owned
/// changelog weight inside the SQL AST. A plain wildcard already selects the input weight.
/// Reserved output aliases and wildcard modifiers are rejected rather than risking a spoofed or
/// path-dependent weight.
pub(crate) fn projection_sql_preserving_weight(sql: &str) -> Option<String> {
    if query_references_weight(sql) {
        return None;
    }
    let mut statements = laminar_sql::parse_streaming_sql(sql).ok()?.into_iter();
    let laminar_sql::parser::StreamingStatement::Standard(statement) = statements.next()? else {
        return None;
    };
    if statements.next().is_some() {
        return None;
    }
    let mut statement = *statement;
    let Statement::Query(query) = &mut statement else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_mut() else {
        return None;
    };
    let weight = laminar_core::changelog::WEIGHT_COLUMN;
    if select.projection.iter().any(|item| match item {
        SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options) => {
            wildcard_has_options(options)
        }
        _ => false,
    }) {
        return None;
    }
    if select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..)
        )
    }) {
        let mut source_wildcard = None;
        for (index, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(_)
                | SelectItem::QualifiedWildcard(
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(_),
                    _,
                ) => {
                    if source_wildcard.replace(index).is_some() {
                        return None;
                    }
                }
                SelectItem::QualifiedWildcard(
                    sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(_),
                    _,
                ) => return None,
                _ => {}
            }
        }
        return source_wildcard
            .is_some_and(|index| index + 1 == select.projection.len())
            .then(|| sql.to_string());
    }

    select.projection.push(SelectItem::ExprWithAlias {
        expr: Expr::Identifier(Ident::new(weight)),
        alias: Ident::new(weight),
    });
    Some(statement.to_string())
}

/// Whether a query explicitly names or produces the engine-owned changelog weight. Parse failures
/// are treated as references so admission cannot mistake an uninspected query for a safe one.
pub(crate) fn query_references_weight(sql: &str) -> bool {
    struct WeightReferenceVisitor {
        found: bool,
    }

    impl Visitor for WeightReferenceVisitor {
        type Break = ();

        fn pre_visit_query(&mut self, query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
            let weight = laminar_core::changelog::WEIGHT_COLUMN;
            if let SetExpr::Select(select) = query.body.as_ref() {
                self.found |= select.projection.iter().any(|item| {
                    matches!(
                        item,
                        SelectItem::ExprWithAlias { alias, .. }
                            if alias.value.eq_ignore_ascii_case(weight)
                    )
                });
            }
            if self.found {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }

        fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
            let identifier = match expression {
                Expr::Identifier(identifier) => Some(identifier),
                Expr::CompoundIdentifier(identifiers) => identifiers.last(),
                _ => None,
            };
            self.found |= identifier.is_some_and(|identifier| {
                identifier
                    .value
                    .eq_ignore_ascii_case(laminar_core::changelog::WEIGHT_COLUMN)
            });
            if self.found {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let Some(query) = parse_standard_query(sql) else {
        return true;
    };
    let mut visitor = WeightReferenceVisitor { found: false };
    let _ = query.visit(&mut visitor);
    visitor.found
}

/// Whether a SQL predicate explicitly reads the engine-owned changelog weight. Parse failures are
/// treated as references so a sink filter cannot escape the fail-closed path.
pub(crate) fn predicate_references_weight(predicate: &str) -> bool {
    query_references_weight(&format!(
        "SELECT * FROM __sink_filter_input WHERE {predicate}"
    ))
}

/// Whether a query requests row-set, ordering, or analytic semantics that cannot be applied
/// independently to a stream of weighted differential rows. Parse failures are unsafe.
#[derive(Default)]
struct MutableChangelogModifierVisitor {
    query_count: usize,
    nested_query: bool,
    analytic_expression: bool,
}

impl Visitor for MutableChangelogModifierVisitor {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &sqlparser::ast::Query) -> ControlFlow<Self::Break> {
        self.query_count += 1;
        self.nested_query |= self.query_count > 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        self.analytic_expression |=
            matches!(expression, Expr::Function(function) if function.over.is_some());
        ControlFlow::Continue(())
    }
}

fn query_ast_has_order_or_row_limit(query: &sqlparser::ast::Query) -> bool {
    if query.order_by.is_some() || query.limit_clause.is_some() || query.fetch.is_some() {
        return true;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return true;
    };
    select.top.is_some()
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
}

/// Whether a query orders or truncates a differential row set. Parse failures are unsafe.
pub(crate) fn query_has_order_or_row_limit(sql: &str) -> bool {
    parse_standard_query(sql).is_none_or(|query| query_ast_has_order_or_row_limit(&query))
}

pub(crate) fn mutable_changelog_has_unsafe_modifiers(sql: &str) -> bool {
    let Some(query) = parse_standard_query(sql) else {
        return true;
    };
    let mut visitor = MutableChangelogModifierVisitor::default();
    let _ = query.visit(&mut visitor);
    if query.with.is_some()
        || query_ast_has_order_or_row_limit(&query)
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
        || visitor.analytic_expression
        || visitor.nested_query
    {
        return true;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return true;
    };
    let empty_group_by = matches!(
        &select.group_by,
        GroupByExpr::Expressions(expressions, modifiers)
            if expressions.is_empty() && modifiers.is_empty()
    );
    select.flavor != SelectFlavor::Standard
        || select.distinct.is_some()
        || select.top.is_some()
        || select.exclude.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !empty_group_by
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
}

/// A flattened bounded-join projection has only the two outer join inputs in scope. Rewriting
/// qualified leaves through a nested query would require scope-aware name resolution and could
/// otherwise capture an inner alias, so the bounded path rejects that shape before graph mutation.
pub(crate) fn interval_output_has_nested_query(sql: &str) -> bool {
    let Some(query) = parse_standard_query(sql) else {
        return true;
    };
    let mut visitor = MutableChangelogModifierVisitor::default();
    let _ = query.visit(&mut visitor);
    visitor.nested_query
}

/// Temp table name the changelog batch is registered under for the enrich-join SQL.
pub(crate) const CHANGELOG_ENRICH_TMP: &str = "__changelog_enrich_tmp";

/// A `<changelog> JOIN <static table>` dimension enrichment.
pub(crate) struct ChangelogEnrichConfig {
    /// The left changelog table the operator consumes from `input_bufs`.
    pub changelog_table: String,
    /// Static dimension relation on the right side.
    pub static_table: String,
    /// Ordered left equi-join keys certified by detection.
    pub left_keys: Vec<String>,
    /// Ordered right equi-join keys certified by detection.
    pub right_keys: Vec<String>,
    /// Whether this is a LEFT rather than INNER join.
    pub left_outer: bool,
    /// Temp-rewritten join SQL (over [`CHANGELOG_ENRICH_TMP`]) that preserves `__weight`.
    pub projection_sql: String,
}

/// The admitted single equi-join with its resolved unquoted join qualifiers.
struct CertifiedEnrichJoin<'a> {
    select: &'a sqlparser::ast::Select,
    analysis: laminar_sql::parser::join_parser::JoinAnalysis,
    join_kw: &'static str,
    left_qualifier: String,
    right_qualifier: String,
}

/// Detect a single equi-join of a changelog left and a static table right; returns
/// the changelog table and a `__weight`-preserving temp-rewritten join SQL, else `None`.
pub(crate) fn detect_changelog_enrich_query(
    sql: &str,
    changelog_tables: &FxHashSet<String>,
    static_tables: &FxHashSet<String>,
) -> Option<ChangelogEnrichConfig> {
    use laminar_sql::parser::join_parser::JoinType;

    if changelog_tables.is_empty()
        || static_tables.is_empty()
        || query_references_weight(sql)
        || mutable_changelog_has_unsafe_modifiers(sql)
    {
        return None;
    }
    let statements = laminar_sql::parse_streaming_sql(sql).ok()?;
    let laminar_sql::parser::StreamingStatement::Standard(stmt) = statements.first()? else {
        return None;
    };
    let Statement::Query(query) = stmt.as_ref() else {
        return None;
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    let [from] = select.from.as_slice() else {
        return None;
    };
    let [join] = from.joins.as_slice() else {
        return None;
    };
    let join = certify_changelog_join(select, from, join, changelog_tables, static_tables)?;
    let items = changelog_projection_items(join.select, &join.left_qualifier)?;
    let projection_sql = build_changelog_projection_sql(&join, &items);
    let mut left_keys = vec![join.analysis.left_key_column.clone()];
    let mut right_keys = vec![join.analysis.right_key_column.clone()];
    left_keys.extend(
        join.analysis
            .additional_key_columns
            .iter()
            .map(|(left, _)| left.clone()),
    );
    right_keys.extend(
        join.analysis
            .additional_key_columns
            .iter()
            .map(|(_, right)| right.clone()),
    );
    Some(ChangelogEnrichConfig {
        changelog_table: join.analysis.left_table.clone(),
        static_table: join.analysis.right_table.clone(),
        left_keys,
        right_keys,
        left_outer: join.analysis.join_type == JoinType::Left,
        projection_sql,
    })
}

/// Admit the join shape: exactly one changelog-left/static-right equi-join with unquoted
/// identifiers, distinct qualifiers, and an alias where the reconstructed SQL needs one.
fn certify_changelog_join<'a>(
    select: &'a sqlparser::ast::Select,
    from: &sqlparser::ast::TableWithJoins,
    join: &sqlparser::ast::Join,
    changelog_tables: &FxHashSet<String>,
    static_tables: &FxHashSet<String>,
) -> Option<CertifiedEnrichJoin<'a>> {
    use laminar_sql::parser::join_parser::JoinType;

    // Changelog enrichment rebuilds the join over an internal left relation. The join parser's
    // string analysis intentionally discards identifier quote style, so accepting quoted relation
    // names or aliases here could change identifier equality (or produce invalid reconstructed
    // SQL) after intake. Keep this specialized rewrite on unquoted identifiers only.
    if table_factor_uses_quoted_identifier(&from.relation)
        || table_factor_uses_quoted_identifier(&join.relation)
    {
        return None;
    }
    let multi = laminar_sql::parser::join_parser::analyze_joins(select).ok()??;
    if multi.joins.len() != 1 {
        return None;
    }
    let j = &multi.joins[0];
    if j.is_temporal_join() || j.time_bound.is_some() {
        return None;
    }
    // Only changelog-left to static-right enrichment is supported. Every other changelog join
    // shape is rejected by DDL and graph admission.
    if !changelog_tables.contains(&j.left_table) || !static_tables.contains(&j.right_table) {
        return None;
    }
    let join_kw = match j.join_type {
        JoinType::Inner => "JOIN",
        JoinType::Left => "LEFT JOIN",
        _ => return None,
    };

    // The ON clause is reconstructed from the extracted equi-keys only, so a non-equi residual
    // (e.g. `AND a.x > b.y`) would be silently dropped and widen the join — reject it so general
    // execution honors the residual instead.
    if !single_join_on_is_pure_equi(select) {
        return None;
    }
    // An aliasless left table is emitted as `... AS {name}`; a compound (schema-qualified) name
    // would produce invalid SQL (`AS schema.tbl`). Reject so the user adds an explicit alias. Use
    // the identifier part-count, not a `.` scan — a quoted `"a.b"` is a single legal identifier.
    if j.left_alias.is_none()
        && matches!(&from.relation, TableFactor::Table { name, .. } if name.0.len() > 1)
    {
        return None;
    }

    let lalias = j.left_alias.as_deref().unwrap_or(&j.left_table);
    let ralias = j.right_alias.as_deref().unwrap_or(&j.right_table);
    if unquoted_identifier_eq(lalias, ralias) {
        return None;
    }

    Some(CertifiedEnrichJoin {
        select,
        analysis: j.clone(),
        join_kw,
        left_qualifier: lalias.to_string(),
        right_qualifier: ralias.to_string(),
    })
}

/// Projection items for the rewritten SQL, appending the engine weight unless a trailing
/// left-qualified wildcard already selects it.
fn changelog_projection_items(
    select: &sqlparser::ast::Select,
    lalias: &str,
) -> Option<Vec<String>> {
    let weight = laminar_core::changelog::WEIGHT_COLUMN;
    if select.projection.iter().any(|item| match item {
        SelectItem::Wildcard(options) | SelectItem::QualifiedWildcard(_, options) => {
            wildcard_has_options(options)
        }
        _ => false,
    }) || select.projection.iter().any(|item| match item {
        SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name),
            _,
        ) => name.0.iter().any(|part| {
            part.as_ident()
                .is_none_or(|identifier| identifier.quote_style.is_some())
        }),
        SelectItem::QualifiedWildcard(
            sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(_),
            _,
        ) => true,
        _ => false,
    }) || select
        .projection
        .iter()
        .any(|item| matches!(item, SelectItem::Wildcard(_)))
    {
        return None;
    }
    let mut items: Vec<String> = select.projection.iter().map(ToString::to_string).collect();
    let mut left_wildcard = None;
    for (index, item) in select.projection.iter().enumerate() {
        if matches!(
            item,
            SelectItem::QualifiedWildcard(
                sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name),
                _,
            ) if name
                .0
                .last()
                .and_then(ObjectNamePart::as_ident)
                .is_some_and(|identifier| {
                    identifier.quote_style.is_none()
                        && unquoted_identifier_eq(&identifier.value, lalias)
                })
        ) && left_wildcard.replace(index).is_some()
        {
            return None;
        }
    }
    if left_wildcard.is_some_and(|index| index + 1 != select.projection.len()) {
        return None;
    }
    let wildcard_preserves_weight = left_wildcard.is_some();
    if !wildcard_preserves_weight {
        items.push(format!("{lalias}.\"{weight}\""));
    }
    Some(items)
}

/// Rebuild the join SQL over [`CHANGELOG_ENRICH_TMP`] from the certified equi-keys.
fn build_changelog_projection_sql(join: &CertifiedEnrichJoin<'_>, items: &[String]) -> String {
    let mut on_clauses = vec![format!(
        "{}.\"{}\" = {}.\"{}\"",
        join.left_qualifier,
        join.analysis.left_key_column,
        join.right_qualifier,
        join.analysis.right_key_column
    )];
    for (lk, rk) in &join.analysis.additional_key_columns {
        on_clauses.push(format!(
            "{}.\"{lk}\" = {}.\"{rk}\"",
            join.left_qualifier, join.right_qualifier
        ));
    }
    let on = on_clauses.join(" AND ");
    let right_from = match &join.analysis.right_alias {
        Some(a) => format!("{} AS {a}", join.analysis.right_table),
        None => join.analysis.right_table.clone(),
    };
    let where_clause = join
        .select
        .selection
        .as_ref()
        .map_or(String::new(), |e| format!(" WHERE {e}"));
    format!(
        "SELECT {} FROM {CHANGELOG_ENRICH_TMP} AS {} {} {right_from} ON {on}{where_clause}",
        items.join(", "),
        join.left_qualifier,
        join.join_kw
    )
}

fn table_factor_uses_quoted_identifier(factor: &TableFactor) -> bool {
    let TableFactor::Table { name, alias, .. } = factor else {
        return true;
    };
    name.0.iter().any(|part| {
        part.as_ident()
            .is_none_or(|identifier| identifier.quote_style.is_some())
    }) || alias
        .as_ref()
        .is_some_and(|alias| alias.name.quote_style.is_some() || !alias.columns.is_empty())
}

/// `true` if the single join's ON clause is a pure conjunction of `col = col` equalities (or a
/// `USING` list). Anything else (`>`, function, residual predicate) ⇒ the equi-key extractor would
/// silently drop it, so the IVM join must reject the query.
fn single_join_on_is_pure_equi(select: &sqlparser::ast::Select) -> bool {
    use sqlparser::ast::{JoinConstraint, JoinOperator};
    if select.from.len() != 1 {
        return false;
    }
    let twj = &select.from[0];
    if twj.joins.len() != 1 {
        return false;
    }
    let (JoinOperator::Inner(constraint)
    | JoinOperator::Join(constraint)
    | JoinOperator::Left(constraint)
    | JoinOperator::LeftOuter(constraint)) = &twj.joins[0].join_operator
    else {
        return false;
    };
    match constraint {
        JoinConstraint::On(expr) => on_expr_is_pure_equi(expr),
        JoinConstraint::Using(_) => true,
        _ => false,
    }
}

fn on_expr_is_pure_equi(expr: &Expr) -> bool {
    use sqlparser::ast::BinaryOperator;
    let is_col = |e: &Expr| matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_));
    match expr {
        Expr::Nested(inner) => on_expr_is_pure_equi(inner),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => on_expr_is_pure_equi(left) && on_expr_is_pure_equi(right),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => is_col(left) && is_col(right),
        _ => false,
    }
}
