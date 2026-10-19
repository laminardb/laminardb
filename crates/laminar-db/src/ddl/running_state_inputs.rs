//! Stateful consumers must read a changelog, never a running-state aggregate stream.

use laminar_sql::parser::EmitClause;
use rustc_hash::FxHashSet;

use crate::db::LaminarDB;
use crate::error::DbError;

use super::PlannedStreamingQuery;

/// What an upstream stream emits, as far as a stateful consumer is concerned.
enum UpstreamEmission {
    /// A `__weight` changelog or append-only output; safe to aggregate.
    Consumable,
    /// A running-state aggregate: each cycle re-emits touched groups without retracting the
    /// value they replace.
    RunningState,
    /// A stateless stream that forwards whatever its own inputs emit.
    Forwarding(String),
}

impl LaminarDB {
    /// Reject a window, join or aggregate whose input is, directly or through stateless streams,
    /// a running-state aggregate. It would count every re-emitted snapshot as new input.
    pub(super) async fn reject_stateful_consumer_of_running_state(
        &self,
        kind: &str,
        name: &str,
        query_sql: &str,
        planned: &PlannedStreamingQuery,
    ) -> Result<(), DbError> {
        let stateful = planned.window_config.is_some()
            || planned.join_config.is_some()
            || self.query_aggregates(query_sql).await;
        if !stateful {
            return Ok(());
        }
        let mut pending: Vec<String> = crate::sql_analysis::extract_table_references(query_sql)
            .into_iter()
            .collect();
        let mut visited = FxHashSet::default();
        while let Some(input) = pending.pop() {
            if !visited.insert(input.clone()) {
                continue;
            }
            match self.upstream_emission(&input).await {
                UpstreamEmission::Consumable => {}
                UpstreamEmission::Forwarding(upstream_sql) => {
                    pending.extend(crate::sql_analysis::extract_table_references(&upstream_sql));
                }
                UpstreamEmission::RunningState => {
                    return Err(DbError::InvalidOperation(format!(
                        "{kind} '{name}' is stateful over '{input}', a running-state aggregate \
                         that re-emits groups without retracting earlier values; declare \
                         '{input}' with EMIT CHANGES so it emits a changelog"
                    )));
                }
            }
        }
        Ok(())
    }

    async fn upstream_emission(&self, input: &str) -> UpstreamEmission {
        let registration = self
            .connector_manager
            .lock()
            .streams()
            .get(input)
            .map(|stream| {
                (
                    stream.incremental,
                    stream.emit_clause.clone(),
                    stream.query_sql.clone(),
                )
            });
        let Some((incremental, emit_clause, query_sql)) = registration else {
            return UpstreamEmission::Consumable;
        };
        let emits_changelog_or_closed_windows = incremental
            || matches!(
                emit_clause,
                Some(EmitClause::Changes | EmitClause::OnWindowClose | EmitClause::Final)
            );
        if emits_changelog_or_closed_windows {
            return UpstreamEmission::Consumable;
        }
        if self.query_aggregates(&query_sql).await {
            UpstreamEmission::RunningState
        } else {
            UpstreamEmission::Forwarding(query_sql)
        }
    }

    async fn query_aggregates(&self, query_sql: &str) -> bool {
        self.ctx
            .sql(query_sql)
            .await
            .is_ok_and(|df| crate::aggregate_state::find_aggregate(df.logical_plan()).is_some())
    }
}

#[cfg(test)]
mod tests {
    use crate::LaminarDB;

    async fn db_with_running_state_upstream() -> std::sync::Arc<LaminarDB> {
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE events (k BIGINT NOT NULL, v BIGINT NOT NULL)")
            .await
            .unwrap();
        db.execute("CREATE STREAM per_key AS SELECT k, COUNT(*) AS n FROM events GROUP BY k")
            .await
            .unwrap();
        db.execute(
            "CREATE STREAM changes AS SELECT k, COUNT(*) AS n FROM events GROUP BY k EMIT CHANGES",
        )
        .await
        .unwrap();
        db.execute("CREATE STREAM forwarded AS SELECT k, n FROM per_key WHERE n > 1")
            .await
            .unwrap();
        db
    }

    #[tokio::test]
    async fn aggregate_over_running_state_is_rejected_directly_and_through_projection() {
        let db = db_with_running_state_upstream().await;
        for (name, input) in [("direct", "per_key"), ("through", "forwarded")] {
            let error = db
                .execute(&format!(
                    "CREATE STREAM {name} AS SELECT n, COUNT(*) AS keys FROM {input} GROUP BY n"
                ))
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("'per_key', a running-state aggregate"),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn changelog_and_stateless_consumers_are_admitted() {
        let db = db_with_running_state_upstream().await;
        db.execute("CREATE STREAM histogram AS SELECT n, COUNT(*) AS keys FROM changes GROUP BY n")
            .await
            .unwrap();
        db.execute("CREATE STREAM projected AS SELECT k, n * 2 AS twice FROM per_key")
            .await
            .unwrap();
    }
}
