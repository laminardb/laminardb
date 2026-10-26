//! Statement splitting: a stream query whose shape no single managed operator executes — an
//! aggregate over a join, or an aggregate over an aggregating or joining subquery — is rewritten
//! into internal named stages that each admit and execute as an ordinary stream.
//!
//! INVARIANT: stage names are `<owner>$stage<n>` in creation order. Stage `n` reads only catalog
//! objects and stages below `n`; the owner's own query reads only its stages.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, Distinct, Expr, GroupByExpr, Ident, ObjectName,
    ObjectNamePart, Query, Select, SelectItem, SetExpr, TableAlias, TableFactor, TableWithJoins,
};

use super::ast::parse_standard_query;
use crate::error::DbError;

pub(crate) use laminar_core::catalog::stage_owner;
use laminar_core::catalog::STAGE_MARKER;

/// Each stage is a stream operator; deeper nesting is a query to write as named streams.
const MAX_STAGES: usize = 8;

/// One internal stage, in creation order.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StageQuery {
    pub(crate) name: String,
    pub(crate) sql: String,
    /// The stage aggregates, so its consumer needs its changes rather than running snapshots.
    pub(crate) changelog: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SplitQuery {
    pub(crate) stages: Vec<StageQuery>,
    /// The owner's query over its stages.
    pub(crate) query_sql: String,
}

/// `owner`'s stages among `names`, in creation order.
pub(crate) fn owned_stages<'a>(
    owner: &str,
    names: impl IntoIterator<Item = &'a String>,
) -> Vec<String> {
    let mut stages: Vec<String> = names
        .into_iter()
        .filter(|name| stage_owner(name) == Some(owner))
        .cloned()
        .collect();
    // Names share the owner's prefix, so length then text orders them by stage number.
    stages.sort_unstable_by(|left, right| (left.len(), left).cmp(&(right.len(), right)));
    stages
}

/// Rewrites `sql` into stages, or returns `None` when one operator executes it as written.
/// `is_aggregate` reports whether a function name resolves to an aggregate.
pub(crate) fn split_stream_query(
    owner: &str,
    sql: &str,
    is_aggregate: &dyn Fn(&str) -> bool,
) -> Result<Option<SplitQuery>, DbError> {
    let Some(query) = parse_standard_query(sql) else {
        return Ok(None);
    };
    let mut splitter = Splitter {
        owner,
        is_aggregate,
        stages: Vec::new(),
    };
    let Some(query) = splitter.split(*query)? else {
        return Ok(None);
    };
    Ok(Some(SplitQuery {
        stages: splitter.stages,
        query_sql: query.to_string(),
    }))
}

struct Splitter<'a> {
    owner: &'a str,
    is_aggregate: &'a dyn Fn(&str) -> bool,
    stages: Vec<StageQuery>,
}

impl Splitter<'_> {
    fn split(&mut self, mut query: Query) -> Result<Option<Query>, DbError> {
        if query.with.is_some() || !query.pipe_operators.is_empty() {
            return Ok(None);
        }
        let SetExpr::Select(select) = query.body.as_mut() else {
            return Ok(None);
        };
        if !self.aggregates(select) {
            return Ok(None);
        }
        let split = match select.from.as_slice() {
            [from] if from.joins.is_empty() => self.split_subquery(select)?,
            [from]
                if from.joins.len() == 1
                    && is_named_table(&from.relation)
                    && is_named_table(&from.joins[0].relation) =>
            {
                self.split_join(select, &mut query.order_by)?
            }
            _ => false,
        };
        Ok(split.then_some(query))
    }

    /// `SELECT .. FROM (<aggregate or join>) [AS t] ..` reads the subquery as a stage.
    fn split_subquery(&mut self, select: &mut Select) -> Result<bool, DbError> {
        let TableFactor::Derived {
            lateral: false,
            subquery,
            alias,
            sample: None,
        } = &select.from[0].relation
        else {
            return Ok(false);
        };
        if alias
            .as_ref()
            .is_some_and(|alias| !alias.columns.is_empty())
        {
            return Ok(false);
        }
        let SetExpr::Select(inner) = subquery.body.as_ref() else {
            return Ok(false);
        };
        let changelog = self.aggregates(inner);
        let joins = inner.from.iter().any(|from| !from.joins.is_empty());
        if !changelog && !joins {
            return Ok(false);
        }
        let alias = alias.clone();
        let inner = (**subquery).clone();
        let inner = self.split(inner.clone())?.unwrap_or(inner);
        let stage = self.push(inner.to_string(), changelog)?;
        select.from[0].relation = stage_relation(&stage, alias);
        Ok(true)
    }

    /// `SELECT <aggregates> FROM a JOIN b ON .. [WHERE ..] ..` joins in a stage that projects
    /// every input column the aggregate reads as `<qualifier>_<column>`.
    fn split_join(
        &mut self,
        select: &mut Select,
        order_by: &mut Option<sqlparser::ast::OrderBy>,
    ) -> Result<bool, DbError> {
        let from = std::mem::take(&mut select.from);
        let selection = select.selection.take();
        let mut columns = JoinColumns::new(self.owner, select)?;
        columns.collect(&*select)?;
        columns.collect(&*order_by)?;
        if columns.aliases.is_empty() {
            columns.collect_first(&from[0].joins[0])?;
        }
        columns.reject_shadowed_outputs()?;
        let projection = columns
            .aliases
            .iter()
            .map(|(alias, column)| format!("{column} AS {}", quoted(alias)))
            .collect::<Vec<_>>()
            .join(", ");
        let filter = selection.map_or_else(String::new, |filter| format!(" WHERE {filter}"));
        let stage = self.push(
            format!("SELECT {projection} FROM {}{filter}", from[0]),
            false,
        )?;
        columns.rewrite(&mut *select);
        columns.rewrite(&mut *order_by);
        select.from = vec![TableWithJoins {
            relation: stage_relation(&stage, None),
            joins: Vec::new(),
        }];
        Ok(true)
    }

    fn push(&mut self, sql: String, changelog: bool) -> Result<String, DbError> {
        if self.stages.len() == MAX_STAGES {
            return Err(DbError::Unsupported(format!(
                "stream '{}' needs more than {MAX_STAGES} stages; write it as named streams",
                self.owner
            )));
        }
        let name = format!("{}{STAGE_MARKER}{}", self.owner, self.stages.len() + 1);
        self.stages.push(StageQuery {
            name: name.clone(),
            sql,
            changelog,
        });
        Ok(name)
    }

    fn aggregates(&self, select: &Select) -> bool {
        let grouped = match &select.group_by {
            GroupByExpr::All(_) => true,
            GroupByExpr::Expressions(expressions, modifiers) => {
                !expressions.is_empty() || !modifiers.is_empty()
            }
        };
        grouped
            || select.having.is_some()
            || matches!(select.distinct, Some(Distinct::Distinct | Distinct::On(_)))
            || visit_expressions(&select.projection, |expr| match expr {
                Expr::Function(function) if function.over.is_none() => {
                    match function.name.0.as_slice() {
                        [ObjectNamePart::Identifier(name)] if self.is_aggregate_name(name) => {
                            ControlFlow::Break(())
                        }
                        _ => ControlFlow::Continue(()),
                    }
                }
                _ => ControlFlow::Continue(()),
            })
            .is_break()
    }

    fn is_aggregate_name(&self, name: &Ident) -> bool {
        match name.quote_style {
            Some(_) => (self.is_aggregate)(&name.value),
            None => (self.is_aggregate)(&name.value.to_ascii_lowercase()),
        }
    }
}

/// The join inputs an aggregate reads, keyed by their stage column name.
struct JoinColumns<'a> {
    owner: &'a str,
    /// Bare identifiers that name an output column rather than an input column.
    output_names: Vec<String>,
    /// Output names the query references bare.
    referenced_outputs: Vec<String>,
    aliases: BTreeMap<String, String>,
    columns: BTreeMap<(String, String), String>,
}

impl<'a> JoinColumns<'a> {
    /// Names each output explicitly, because the stage renames the input columns that unaliased
    /// expressions would otherwise be named after.
    fn new(owner: &'a str, select: &mut Select) -> Result<Self, DbError> {
        let mut columns = Self {
            owner,
            output_names: Vec::new(),
            referenced_outputs: Vec::new(),
            aliases: BTreeMap::new(),
            columns: BTreeMap::new(),
        };
        for item in &mut select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) if parts.len() == 2 => {
                    let alias = parts[1].clone();
                    let expr = Expr::CompoundIdentifier(std::mem::take(parts));
                    *item = SelectItem::ExprWithAlias { expr, alias };
                }
                SelectItem::UnnamedExpr(expr) if reads_qualified_column(expr) => {
                    return Err(columns.unsupported(&format!("alias the expression '{expr}'")));
                }
                SelectItem::UnnamedExpr(_) | SelectItem::ExprWithAlias { .. } => {}
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(..) => {
                    return Err(columns.unsupported("list the output columns instead of '*'"));
                }
            }
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                columns.output_names.push(alias.value.clone());
            }
        }
        Ok(columns)
    }

    fn collect<V: sqlparser::ast::Visit>(&mut self, node: &V) -> Result<(), DbError> {
        let mut result = Ok(());
        let _ = visit_expressions(node, |expr| {
            result = self.reference(expr);
            if result.is_err() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        result
    }

    /// An aggregate that reads no input column still needs one row per join match.
    fn collect_first(&mut self, join: &sqlparser::ast::Join) -> Result<(), DbError> {
        let mut first = None;
        let _ = visit_expressions(join, |expr| match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                first = Some(expr.clone());
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        });
        first.map_or(Ok(()), |expr| self.reference(&expr))
    }

    fn reference(&mut self, expr: &Expr) -> Result<(), DbError> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                let [qualifier, column] = parts.as_slice() else {
                    return Err(
                        self.unsupported(&format!("reference '{expr}' must name one input column"))
                    );
                };
                let key = (qualifier.value.clone(), column.value.clone());
                if self.columns.contains_key(&key) {
                    return Ok(());
                }
                let alias = format!("{}_{}", qualifier.value, column.value);
                if self.aliases.contains_key(&alias) {
                    return Err(self.unsupported(&format!(
                        "stage column '{alias}' for '{expr}' is ambiguous"
                    )));
                }
                self.aliases.insert(alias.clone(), expr.to_string());
                self.columns.insert(key, alias);
                Ok(())
            }
            Expr::Identifier(ident) if self.output_names.contains(&ident.value) => {
                self.referenced_outputs.push(ident.value.clone());
                Ok(())
            }
            Expr::Identifier(ident) => Err(self.unsupported(&format!(
                "qualify column '{ident}' with its input name or alias"
            ))),
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                Err(self.unsupported("subquery expressions are unsupported"))
            }
            _ => Ok(()),
        }
    }

    /// A bare output name that equals a stage column would read the column instead.
    fn reject_shadowed_outputs(&self) -> Result<(), DbError> {
        match self
            .referenced_outputs
            .iter()
            .find(|name| self.aliases.contains_key(*name))
        {
            Some(name) => Err(self.unsupported(&format!(
                "output '{name}' has the name of a stage column; rename the output"
            ))),
            None => Ok(()),
        }
    }

    fn rewrite<V: sqlparser::ast::VisitMut>(&self, node: &mut V) {
        let _ = visit_expressions_mut(node, |expr| {
            if let Expr::CompoundIdentifier(parts) = expr {
                if let [qualifier, column] = parts.as_slice() {
                    if let Some(alias) = self
                        .columns
                        .get(&(qualifier.value.clone(), column.value.clone()))
                    {
                        *expr = Expr::Identifier(Ident::with_quote('"', alias));
                    }
                }
            }
            ControlFlow::<()>::Continue(())
        });
    }

    fn unsupported(&self, reason: &str) -> DbError {
        DbError::Unsupported(format!(
            "stream '{}' aggregates over a join, which runs as a join stage and an aggregate \
             stage: {reason}",
            self.owner
        ))
    }
}

fn reads_qualified_column(expr: &Expr) -> bool {
    visit_expressions(expr, |nested| match nested {
        Expr::CompoundIdentifier(_) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

fn is_named_table(factor: &TableFactor) -> bool {
    matches!(factor, TableFactor::Table { args: None, .. })
}

fn quoted(name: &str) -> Ident {
    Ident::with_quote('"', name)
}

fn stage_relation(name: &str, alias: Option<TableAlias>) -> TableFactor {
    TableFactor::Table {
        name: ObjectName::from(vec![quoted(name)]),
        alias,
        args: None,
        with_hints: Vec::new(),
        version: None,
        with_ordinality: false,
        partitions: Vec::new(),
        json_path: None,
        sample: None,
        index_hints: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(sql: &str) -> Result<Option<SplitQuery>, DbError> {
        split_stream_query("out", sql, &|name| {
            matches!(name, "count" | "sum" | "avg" | "min" | "max")
        })
    }

    #[test]
    fn owned_stages_are_in_creation_order() {
        let names: Vec<String> = ["a$stage10", "out$stage2", "a$stage9", "a", "a$stage1"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(
            owned_stages("a", &names),
            ["a$stage1", "a$stage9", "a$stage10"]
        );
    }

    #[test]
    fn single_operator_queries_are_not_split() {
        for sql in [
            "SELECT k, COUNT(*) AS n FROM events GROUP BY k",
            "SELECT k, v FROM (SELECT k, v FROM events WHERE v > 0) t",
            "SELECT k, n FROM (SELECT k, COUNT(*) AS n FROM events GROUP BY k) t",
            "SELECT o.k AS k, p.v AS v FROM orders o JOIN payments p ON o.k = p.k",
            "WITH t AS (SELECT k, COUNT(*) AS n FROM events GROUP BY k) \
             SELECT n, COUNT(*) AS c FROM t GROUP BY n",
        ] {
            assert_eq!(split(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn nested_aggregate_reads_its_inner_aggregate_as_a_changelog_stage() {
        let split = split(
            "SELECT n, COUNT(*) AS keys FROM \
             (SELECT k, COUNT(*) AS n FROM events GROUP BY k) AS t GROUP BY n",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            split.stages,
            vec![StageQuery {
                name: "out$stage1".into(),
                sql: "SELECT k, COUNT(*) AS n FROM events GROUP BY k".into(),
                changelog: true,
            }]
        );
        assert_eq!(
            split.query_sql,
            "SELECT n, COUNT(*) AS keys FROM \"out$stage1\" AS t GROUP BY n"
        );
    }

    #[test]
    fn join_aggregate_projects_each_qualified_input_column_once() {
        let split = split(
            "SELECT o.k AS k, COUNT(*) AS n, SUM(p.v) AS s FROM orders o \
             JOIN payments p ON o.k = p.k \
             AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
             WHERE p.v > 0 GROUP BY o.k HAVING SUM(p.v) > 10",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            split.stages,
            vec![StageQuery {
                name: "out$stage1".into(),
                sql: "SELECT o.k AS \"o_k\", p.v AS \"p_v\" FROM orders o \
                      JOIN payments p ON o.k = p.k \
                      AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
                      WHERE p.v > 0"
                    .into(),
                changelog: false,
            }]
        );
        assert_eq!(
            split.query_sql,
            "SELECT \"o_k\" AS k, COUNT(*) AS n, SUM(\"p_v\") AS s FROM \"out$stage1\" \
             GROUP BY \"o_k\" HAVING SUM(\"p_v\") > 10"
        );
    }

    #[test]
    fn join_aggregate_without_column_reads_still_counts_matches() {
        let split = split("SELECT COUNT(*) AS n FROM orders o JOIN payments p ON o.k = p.k")
            .unwrap()
            .unwrap();
        assert_eq!(
            split.stages[0].sql,
            "SELECT o.k AS \"o_k\" FROM orders o JOIN payments p ON o.k = p.k"
        );
        assert_eq!(split.query_sql, "SELECT COUNT(*) AS n FROM \"out$stage1\"");
    }

    #[test]
    fn nested_join_aggregate_splits_inside_out() {
        let split = split(
            "SELECT n, COUNT(*) AS keys FROM (SELECT o.k AS k, COUNT(*) AS n FROM orders o \
             JOIN payments p ON o.k = p.k GROUP BY o.k) GROUP BY n",
        )
        .unwrap()
        .unwrap();
        let names: Vec<_> = split
            .stages
            .iter()
            .map(|stage| stage.name.as_str())
            .collect();
        assert_eq!(names, ["out$stage1", "out$stage2"]);
        assert!(!split.stages[0].changelog);
        assert_eq!(
            split.stages[1].sql,
            "SELECT \"o_k\" AS k, COUNT(*) AS n FROM \"out$stage1\" GROUP BY \"o_k\""
        );
        assert!(split.stages[1].changelog);
        assert_eq!(
            split.query_sql,
            "SELECT n, COUNT(*) AS keys FROM \"out$stage2\" GROUP BY n"
        );
    }

    #[test]
    fn join_aggregate_rejects_references_it_cannot_place() {
        for (sql, reason) in [
            (
                "SELECT k, COUNT(*) AS n FROM orders o JOIN payments p ON o.k = p.k GROUP BY k",
                "qualify column 'k'",
            ),
            (
                "SELECT o.k AS p_v, SUM(p.v) AS s FROM orders o JOIN payments p ON o.k = p.k \
                 GROUP BY p_v",
                "output 'p_v' has the name of a stage column",
            ),
            (
                "SELECT o.k AS k, SUM(p.v) FROM orders o JOIN payments p ON o.k = p.k \
                 GROUP BY o.k",
                "alias the expression 'SUM(p.v)'",
            ),
            (
                "SELECT * FROM orders o JOIN payments p ON o.k = p.k GROUP BY o.k",
                "instead of '*'",
            ),
            (
                "SELECT a_b.c AS x, SUM(a.b_c) AS s FROM a_b JOIN a ON a_b.k = a.k GROUP BY a_b.c",
                "stage column 'a_b_c'",
            ),
        ] {
            let error = split(sql).unwrap_err().to_string();
            assert!(error.contains(reason), "{sql}: {error}");
        }
    }

    #[test]
    fn join_aggregate_keeps_its_output_names() {
        let split = split(
            "SELECT o.k, p.v AS o_k, COUNT(*) FROM orders o JOIN payments p ON o.k = p.k \
             GROUP BY o.k, p.v",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            split.query_sql,
            "SELECT \"o_k\" AS k, \"p_v\" AS o_k, COUNT(*) FROM \"out$stage1\" \
             GROUP BY \"o_k\", \"p_v\""
        );
    }

    #[test]
    fn output_aliases_stay_bare_in_the_aggregate_stage() {
        let split = split(
            "SELECT o.k AS key, COUNT(*) AS n FROM orders o JOIN payments p ON o.k = p.k \
             GROUP BY key ORDER BY n",
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            split.query_sql,
            "SELECT \"o_k\" AS key, COUNT(*) AS n FROM \"out$stage1\" GROUP BY key ORDER BY n"
        );
    }

    #[test]
    fn stage_count_is_bounded() {
        let mut sql = "SELECT k, COUNT(*) AS n FROM events GROUP BY k".to_string();
        for _ in 0..=MAX_STAGES {
            sql = format!("SELECT n AS k, COUNT(*) AS n FROM ({sql}) GROUP BY n");
        }
        assert!(split(&sql)
            .unwrap_err()
            .to_string()
            .contains("more than 8 stages"));
    }
}
