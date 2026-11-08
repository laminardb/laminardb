//! Cluster-state classification of a `UNION ALL` query.

use datafusion::prelude::SessionContext;
use sqlparser::ast::{Query, SetExpr, SetOperator, SetQuantifier};

use super::classify_sql_capability;
use crate::operator::capability::OperatorCapability;

/// A `UNION ALL` is stateless when every branch, under the union's own clauses, is a stateless
/// projection or filter over one table: each node then unions the rows it holds.
pub(super) fn classify_union_all(query: &Query, ctx: &SessionContext) -> OperatorCapability {
    let mut branches = Vec::new();
    let stateless = union_all_branches(&query.body, &mut branches)
        && branches.into_iter().all(|branch| {
            let branch = Query {
                body: Box::new(branch.clone()),
                ..query.clone()
            };
            classify_sql_capability(&branch.to_string(), ctx)
                == OperatorCapability::stateless_sql_query()
        });
    if stateless {
        OperatorCapability::stateless_sql_query()
    } else {
        OperatorCapability::unclassified_sql_query()
    }
}

/// The `SELECT` branches of a body built only from `UNION ALL`.
fn union_all_branches<'a>(body: &'a SetExpr, branches: &mut Vec<&'a SetExpr>) -> bool {
    match body {
        SetExpr::Select(_) => {
            branches.push(body);
            true
        }
        SetExpr::SetOperation {
            op: SetOperator::Union,
            set_quantifier: SetQuantifier::All,
            left,
            right,
        } => union_all_branches(left, branches) && union_all_branches(right, branches),
        _ => false,
    }
}
