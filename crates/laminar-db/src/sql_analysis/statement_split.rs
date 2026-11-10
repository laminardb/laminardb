//! Statement splitting: a stream or view query whose shape no single managed operator executes —
//! a chain of interval joins over more than two inputs, an aggregate over a join, or an aggregate
//! over an aggregating or joining subquery — is rewritten into internal named stages that each
//! admit and execute as an ordinary stream.
//!
//! INVARIANT: stage names are `<owner>$stage<n>` in creation order. Stage `n` reads only catalog
//! objects and stages below `n`; the owner's own query reads its stages and, when it runs the
//! last join of a chain, that join's catalog input.

use std::collections::BTreeMap;
use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions, visit_expressions_mut, Distinct, Expr, GroupByExpr, Ident, Join,
    JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, OrderBy, Query, Select, SelectItem,
    SetExpr, TableAlias, TableFactor, TableWithJoins,
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
        let aggregates = self.aggregates(select);
        let split = match select.from.as_slice() {
            [from] if is_interval_join_chain(from) => {
                self.split_joins(select, &mut query.order_by, aggregates)?
            }
            [from] if aggregates && from.joins.is_empty() => self.split_subquery(select)?,
            [from]
                if aggregates
                    && from.joins.len() == 1
                    && is_named_table(&from.relation)
                    && is_named_table(&from.joins[0].relation) =>
            {
                self.split_joins(select, &mut query.order_by, true)?
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

    /// `SELECT .. FROM r0 JOIN r1 ON .. [JOIN r2 ON ..] ..` runs the first join over `r0` and
    /// `r1` in stage 1, and join `j` over stage `j - 1` and `r<j>`. Each stage projects every
    /// input column read after it as `<qualifier>_<column>`. An aggregate reads the last join's
    /// stage; otherwise the owner runs the last join.
    fn split_joins(
        &mut self,
        select: &mut Select,
        order_by: &mut Option<OrderBy>,
        aggregates: bool,
    ) -> Result<bool, DbError> {
        let TableWithJoins {
            relation,
            mut joins,
        } = select.from.remove(0);
        let mut selection = select.selection.take();
        let last = joins.len();
        let owner_reads_at = if aggregates { last + 1 } else { last };
        let mut columns = JoinColumns::new(self.owner, select, &relation, &joins)?;
        columns.collect(&*select, owner_reads_at)?;
        columns.collect(&*order_by, owner_reads_at)?;
        if last > 1 {
            columns.collect(&selection, last)?;
        }
        for (index, join) in joins.iter().enumerate().skip(1) {
            columns.collect(join, index + 1)?;
        }
        if aggregates && !columns.projects(last) {
            columns.collect_first(&joins[last - 1], owner_reads_at)?;
        }
        columns.reject_shadowed_outputs()?;

        let owner_join = if aggregates { None } else { joins.pop() };
        let mut left = relation;
        let mut previous: Option<String> = None;
        for (index, mut join) in joins.into_iter().enumerate() {
            let position = index + 1;
            if let Some(stage) = &previous {
                columns.rewrite_from_stage(&mut join, position, stage);
            }
            let mut filter = String::new();
            if position == last {
                if let (Some(stage), Some(selection)) = (&previous, &mut selection) {
                    columns.rewrite_from_stage(selection, position, stage);
                }
                filter = selection
                    .take()
                    .map_or_else(String::new, |filter| format!(" WHERE {filter}"));
            }
            let projection = columns.projection(position, previous.as_deref());
            if projection.is_empty() {
                return Err(columns.unsupported(&format!(
                    "the join after stage {position} reads none of its columns"
                )));
            }
            let input = TableWithJoins {
                relation: left,
                joins: vec![join],
            };
            let stage = self.push(
                format!("SELECT {} FROM {input}{filter}", projection.join(", ")),
                false,
            )?;
            left = stage_relation(&stage, None);
            previous = Some(stage);
        }
        let Some(mut join) = owner_join else {
            columns.rewrite(&mut *select);
            columns.rewrite(&mut *order_by);
            select.from = vec![TableWithJoins {
                relation: left,
                joins: Vec::new(),
            }];
            return Ok(true);
        };
        if let Some(stage) = &previous {
            columns.rewrite_from_stage(&mut join, last, stage);
            columns.rewrite_from_stage(&mut *select, last, stage);
            columns.rewrite_from_stage(&mut *order_by, last, stage);
            columns.rewrite_from_stage(&mut selection, last, stage);
        }
        select.from = vec![TableWithJoins {
            relation: left,
            joins: vec![join],
        }];
        select.selection = selection;
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

/// An input column that a stage projects for the stages and owner after it.
struct StageColumn {
    /// The column as the statement names it.
    input: String,
    /// The first stage that reads the column's input.
    stage: usize,
    /// The position of its last reader: a stage, or the owner.
    read_until: usize,
}

/// The join inputs read after the stage that joins them, keyed by their stage column name.
/// Position `j` is join `j`; an aggregate over the joins reads at the position after the last.
struct JoinColumns<'a> {
    owner: &'a str,
    /// Each input's qualifier, in join order.
    qualifiers: Vec<Option<String>>,
    /// Bare identifiers that name an output column rather than an input column.
    output_names: Vec<String>,
    /// Output names the query references bare.
    referenced_outputs: Vec<String>,
    aliases: BTreeMap<String, StageColumn>,
    columns: BTreeMap<(String, String), String>,
}

impl<'a> JoinColumns<'a> {
    /// Names each output explicitly, because the stage renames the input columns that unaliased
    /// expressions would otherwise be named after.
    fn new(
        owner: &'a str,
        select: &mut Select,
        relation: &TableFactor,
        joins: &[Join],
    ) -> Result<Self, DbError> {
        let mut columns = Self {
            owner,
            qualifiers: std::iter::once(relation)
                .chain(joins.iter().map(|join| &join.relation))
                .map(relation_qualifier)
                .collect(),
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
                SelectItem::ExprWithAliases { .. } => {
                    return Err(columns.unsupported("use one alias per output column"));
                }
            }
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                columns.output_names.push(alias.value.clone());
            }
        }
        Ok(columns)
    }

    fn last_join(&self) -> usize {
        self.qualifiers.len() - 1
    }

    /// The first stage that reads `qualifier`'s input; an unknown qualifier is left to the last
    /// join, whose planning names it.
    fn stage_of(&self, qualifier: &str) -> usize {
        self.qualifiers
            .iter()
            .position(|name| name.as_deref() == Some(qualifier))
            .map_or(self.last_join(), |index| index.max(1))
    }

    /// Records the input columns `node` reads at position `read_at`.
    fn collect<V: sqlparser::ast::Visit>(
        &mut self,
        node: &V,
        read_at: usize,
    ) -> Result<(), DbError> {
        let mut result = Ok(());
        let _ = visit_expressions(node, |expr| {
            result = self.reference(expr, read_at);
            if result.is_err() {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        result
    }

    /// An aggregate that reads no input column still needs one row per join match.
    fn collect_first(&mut self, join: &Join, read_at: usize) -> Result<(), DbError> {
        let mut first = None;
        let _ = visit_expressions(join, |expr| match expr {
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                first = Some(expr.clone());
                ControlFlow::Break(())
            }
            _ => ControlFlow::Continue(()),
        });
        first.map_or(Ok(()), |expr| self.reference(&expr, read_at))
    }

    fn reference(&mut self, expr: &Expr, read_at: usize) -> Result<(), DbError> {
        match expr {
            Expr::CompoundIdentifier(parts) => {
                let [qualifier, column] = parts.as_slice() else {
                    return Err(
                        self.unsupported(&format!("reference '{expr}' must name one input column"))
                    );
                };
                let stage = self.stage_of(&qualifier.value);
                if stage >= read_at {
                    return Ok(());
                }
                let key = (qualifier.value.clone(), column.value.clone());
                if let Some(alias) = self.columns.get(&key) {
                    if let Some(stage_column) = self.aliases.get_mut(alias) {
                        stage_column.read_until = stage_column.read_until.max(read_at);
                    }
                    return Ok(());
                }
                let alias = format!("{}_{}", qualifier.value, column.value);
                if self.aliases.contains_key(&alias) {
                    return Err(self.unsupported(&format!(
                        "stage column '{alias}' for '{expr}' is ambiguous"
                    )));
                }
                let stage_column = StageColumn {
                    input: expr.to_string(),
                    stage,
                    read_until: read_at,
                };
                self.aliases.insert(alias.clone(), stage_column);
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

    fn projects(&self, stage: usize) -> bool {
        self.aliases
            .values()
            .any(|column| column.stage <= stage && stage < column.read_until)
    }

    /// Stage `stage`'s select list: its own inputs' columns as the statement names them, and the
    /// columns it carries on from `previous`.
    fn projection(&self, stage: usize, previous: Option<&str>) -> Vec<String> {
        self.aliases
            .iter()
            .filter(|(_, column)| column.stage <= stage && stage < column.read_until)
            .map(|(alias, column)| match previous {
                Some(previous) if column.stage < stage => {
                    format!(
                        "{}.{} AS {}",
                        quoted(previous),
                        quoted(alias),
                        quoted(alias)
                    )
                }
                _ => format!("{} AS {}", column.input, quoted(alias)),
            })
            .collect()
    }

    /// Reads each column that an earlier stage carries from the previous stage, `stage`.
    fn rewrite_from_stage<V: sqlparser::ast::VisitMut>(
        &self,
        node: &mut V,
        position: usize,
        stage: &str,
    ) {
        let _ = visit_expressions_mut(node, |expr| {
            if let Some(alias) = self.stage_alias(expr, |column| column.stage < position) {
                *expr = Expr::CompoundIdentifier(vec![quoted(stage), quoted(alias)]);
            }
            ControlFlow::<()>::Continue(())
        });
    }

    /// Reads every collected column bare, from the one stage the owner aggregates over.
    fn rewrite<V: sqlparser::ast::VisitMut>(&self, node: &mut V) {
        let _ = visit_expressions_mut(node, |expr| {
            if let Some(alias) = self.stage_alias(expr, |_| true) {
                *expr = Expr::Identifier(quoted(alias));
            }
            ControlFlow::<()>::Continue(())
        });
    }

    fn stage_alias(&self, expr: &Expr, carried: impl Fn(&StageColumn) -> bool) -> Option<&str> {
        let Expr::CompoundIdentifier(parts) = expr else {
            return None;
        };
        let [qualifier, column] = parts.as_slice() else {
            return None;
        };
        let alias = self
            .columns
            .get(&(qualifier.value.clone(), column.value.clone()))?;
        self.aliases
            .get(alias)
            .is_some_and(carried)
            .then_some(alias.as_str())
    }

    fn unsupported(&self, reason: &str) -> DbError {
        let shape = match self.qualifiers.len() {
            2 => "aggregates over a join, which runs as a join stage and an aggregate stage"
                .to_string(),
            inputs => format!("joins {inputs} inputs, which runs as one stage per join"),
        };
        DbError::Unsupported(format!("stream '{}' {shape}: {reason}", self.owner))
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

/// Two or more joins, each an interval join (an `ON` condition with a `BETWEEN` bound) of a
/// single-part catalog name onto the inputs before it.
fn is_interval_join_chain(from: &TableWithJoins) -> bool {
    let catalog_input = |factor: &TableFactor| {
        matches!(
            factor,
            TableFactor::Table { name, alias, args: None, version: None, .. }
                if name.0.len() == 1 && alias.as_ref().is_none_or(|alias| alias.columns.is_empty())
        )
    };
    from.joins.len() > 1
        && catalog_input(&from.relation)
        && from.joins.iter().all(|join| {
            catalog_input(&join.relation)
                && join_condition(&join.join_operator).is_some_and(|condition| {
                    visit_expressions(condition, |expr| match expr {
                        Expr::Between { negated: false, .. } => ControlFlow::Break(()),
                        _ => ControlFlow::Continue(()),
                    })
                    .is_break()
                })
        })
}

fn join_condition(operator: &JoinOperator) -> Option<&Expr> {
    let (JoinOperator::Join(constraint)
    | JoinOperator::Inner(constraint)
    | JoinOperator::Left(constraint)
    | JoinOperator::LeftOuter(constraint)
    | JoinOperator::Right(constraint)
    | JoinOperator::RightOuter(constraint)
    | JoinOperator::FullOuter(constraint)
    | JoinOperator::Semi(constraint)
    | JoinOperator::LeftSemi(constraint)
    | JoinOperator::RightSemi(constraint)
    | JoinOperator::Anti(constraint)
    | JoinOperator::LeftAnti(constraint)
    | JoinOperator::RightAnti(constraint)) = operator
    else {
        return None;
    };
    match constraint {
        JoinConstraint::On(condition) => Some(condition),
        _ => None,
    }
}

/// The name a query qualifies an input's columns with: its alias, else its last name part.
fn relation_qualifier(factor: &TableFactor) -> Option<String> {
    let TableFactor::Table { name, alias, .. } = factor else {
        return None;
    };
    match alias {
        Some(alias) => Some(alias.name.value.clone()),
        None => match name.0.last()? {
            ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
            ObjectNamePart::Function(_) => None,
        },
    }
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

    const THREE_WAY: &str = "FROM orders o \
         JOIN payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND \
         JOIN refunds r ON p.v = r.v AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '30' MILLISECOND";

    #[test]
    fn multi_way_join_runs_its_last_join_in_the_owner() {
        let split = split(&format!(
            "SELECT o.k, p.v AS pv, r.v AS rv {THREE_WAY} WHERE o.v > r.v"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(
            split.stages,
            vec![StageQuery {
                name: "out$stage1".into(),
                sql: "SELECT o.k AS \"o_k\", o.ts AS \"o_ts\", o.v AS \"o_v\", \
                      p.v AS \"p_v\" FROM orders o \
                      JOIN payments p ON o.k = p.k \
                      AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                    .into(),
                changelog: false,
            }]
        );
        assert_eq!(
            split.query_sql,
            "SELECT \"out$stage1\".\"o_k\" AS k, \"out$stage1\".\"p_v\" AS pv, r.v AS rv \
             FROM \"out$stage1\" JOIN refunds r ON \"out$stage1\".\"p_v\" = r.v \
             AND r.ts BETWEEN \"out$stage1\".\"o_ts\" \
             AND \"out$stage1\".\"o_ts\" + INTERVAL '30' MILLISECOND \
             WHERE \"out$stage1\".\"o_v\" > r.v"
        );
    }

    #[test]
    fn multi_way_aggregate_carries_only_the_columns_read_after_each_join() {
        let split = split(&format!(
            "SELECT r.k AS k, COUNT(*) AS n {THREE_WAY} WHERE o.v > 0 GROUP BY r.k"
        ))
        .unwrap()
        .unwrap();
        let sql: Vec<_> = split
            .stages
            .iter()
            .map(|stage| stage.sql.as_str())
            .collect();
        assert_eq!(
            sql,
            [
                "SELECT o.ts AS \"o_ts\", o.v AS \"o_v\", p.v AS \"p_v\" FROM orders o \
                 JOIN payments p ON o.k = p.k \
                 AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND",
                "SELECT r.k AS \"r_k\" FROM \"out$stage1\" \
                 JOIN refunds r ON \"out$stage1\".\"p_v\" = r.v \
                 AND r.ts BETWEEN \"out$stage1\".\"o_ts\" \
                 AND \"out$stage1\".\"o_ts\" + INTERVAL '30' MILLISECOND \
                 WHERE \"out$stage1\".\"o_v\" > 0",
            ]
        );
        assert!(split.stages.iter().all(|stage| !stage.changelog));
        assert_eq!(
            split.query_sql,
            "SELECT \"r_k\" AS k, COUNT(*) AS n FROM \"out$stage2\" GROUP BY \"r_k\""
        );
    }

    #[test]
    fn four_way_join_chains_three_stages() {
        let split = split(&format!(
            "SELECT s.v AS v {THREE_WAY} \
             JOIN shipments s ON r.k = s.k AND s.ts BETWEEN o.ts AND o.ts + INTERVAL '10' MILLISECOND"
        ))
        .unwrap()
        .unwrap();
        assert_eq!(split.stages.len(), 2);
        assert_eq!(
            split.stages[1].sql,
            "SELECT \"out$stage1\".\"o_ts\" AS \"o_ts\", r.k AS \"r_k\" FROM \"out$stage1\" \
             JOIN refunds r ON \"out$stage1\".\"p_v\" = r.v \
             AND r.ts BETWEEN \"out$stage1\".\"o_ts\" \
             AND \"out$stage1\".\"o_ts\" + INTERVAL '30' MILLISECOND"
        );
        assert_eq!(
            split.query_sql,
            "SELECT s.v AS v FROM \"out$stage2\" JOIN shipments s \
             ON \"out$stage2\".\"r_k\" = s.k AND s.ts BETWEEN \"out$stage2\".\"o_ts\" \
             AND \"out$stage2\".\"o_ts\" + INTERVAL '10' MILLISECOND"
        );
    }

    #[test]
    fn multi_way_joins_without_an_interval_on_every_join_are_not_split() {
        for sql in [
            "SELECT o.k AS k FROM orders o \
             JOIN payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND \
             JOIN customers c ON p.c = c.id",
            "SELECT o.k AS k FROM orders o \
             JOIN payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND \
             JOIN refunds r USING (k)",
            "SELECT o.k AS k FROM orders o \
             JOIN payments p ON o.k = p.k AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND \
             JOIN (SELECT k, ts FROM refunds) r \
             ON o.k = r.k AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND",
        ] {
            assert_eq!(split(sql).unwrap(), None, "{sql}");
        }
    }

    #[test]
    fn multi_way_join_requires_qualified_columns() {
        let error = split(&format!("SELECT o.k AS k {THREE_WAY} WHERE v > 0"))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("joins 3 inputs, which runs as one stage per join: qualify column 'v'"),
            "{error}"
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
