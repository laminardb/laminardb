//! Split streams in the catalog: an owner and its internal stages are created and dropped as one
//! object. Only the owner stores DDL, so replay re-splits the owner's statement.
//!
//! INVARIANT: an owner exists only after all of its stages; a stage without its owner is left by a
//! cancelled create and is dropped before the owner's name is created again.

use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::{EmitClause, StreamingStatement};
use sqlparser::ast::Ident;

use crate::db::LaminarDB;
use crate::error::DbError;
use crate::sql_analysis::SplitQuery;

use super::catalog::CatalogNameReservation;
use super::drop::CatalogDropTarget;
use super::stream::StreamDefinition;

impl LaminarDB {
    pub(crate) fn split_stream_query(
        &self,
        owner: &str,
        query_sql: &str,
    ) -> Result<Option<SplitQuery>, DbError> {
        crate::sql_analysis::split_stream_query(owner, query_sql, &|function| {
            datafusion_expr::registry::FunctionRegistry::udaf(&self.ctx, function).is_ok()
        })
    }

    /// Creates each stage and then the owner over them; on failure the created stages are dropped.
    pub(super) async fn create_split_stream(
        &self,
        owner: StreamDefinition<'_>,
        split: &SplitQuery,
        reservation: CatalogNameReservation<'_>,
    ) -> Result<(), DbError> {
        let owner_name = crate::db::canonical_object_name(owner.name)?;
        self.drop_stream_stages(&owner_name).await?;
        let Err(error) = self
            .create_stages_then_owner(owner, split, reservation)
            .await
        else {
            return Ok(());
        };
        match self.drop_stream_stages(&owner_name).await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(DbError::Pipeline(format!(
                "{error}; dropping the stages of '{owner_name}' also failed: {cleanup}"
            ))),
        }
    }

    async fn create_stages_then_owner(
        &self,
        owner: StreamDefinition<'_>,
        split: &SplitQuery,
        reservation: CatalogNameReservation<'_>,
    ) -> Result<(), DbError> {
        for stage in &split.stages {
            let emit = stage.changelog.then_some(EmitClause::Changes);
            let statement = parse_stage(&stage.name, &stage.sql, emit.as_ref())?;
            let StreamingStatement::CreateStream {
                name,
                query,
                emit_clause,
                query_sql,
                ..
            } = &statement
            else {
                return Err(stage_parse_error(&stage.name));
            };
            self.validate_stream_query_text(&stage.name, query_sql)?;
            let reservation = self.reserve_stage_name(&stage.name)?;
            let definition = StreamDefinition {
                ddl: None,
                name,
                query,
                emit_clause: emit_clause.as_ref(),
                query_sql,
                retention_bytes: None,
            };
            self.create_stream_object(definition, reservation).await?;
        }
        let owner_name = crate::db::canonical_object_name(owner.name)?;
        let statement = parse_stage(&owner_name, &split.query_sql, owner.emit_clause)?;
        let StreamingStatement::CreateStream {
            query, query_sql, ..
        } = &statement
        else {
            return Err(stage_parse_error(&owner_name));
        };
        self.validate_stream_query_text(&owner_name, query_sql)?;
        let definition = StreamDefinition {
            query,
            query_sql,
            ..owner
        };
        self.create_stream_object(definition, reservation).await
    }

    async fn drop_stream_stages(&self, owner: &str) -> Result<(), DbError> {
        let targets = self.stream_stage_targets(owner);
        if targets.is_empty() {
            return Ok(());
        }
        self.drop_stream_targets("DROP STREAM stages", targets)
            .await
    }

    /// `owner`'s stages, latest first so each is dropped before the stage it reads.
    pub(super) fn stream_stage_targets(&self, owner: &str) -> Vec<CatalogDropTarget> {
        let namespace = self.catalog_namespace.lock();
        let streams = namespace
            .iter()
            .filter(|(_, kind)| **kind == CatalogObjectKind::Stream)
            .map(|(name, _)| name);
        crate::sql_analysis::owned_stages(owner, streams)
            .into_iter()
            .rev()
            .map(|name| CatalogDropTarget {
                name,
                kind: CatalogObjectKind::Stream,
            })
            .collect()
    }
}

fn parse_stage(
    name: &str,
    sql: &str,
    emit: Option<&EmitClause>,
) -> Result<StreamingStatement, DbError> {
    let emit = emit.map_or_else(String::new, |emit| format!(" {emit}"));
    let ddl = format!(
        "CREATE STREAM {} AS {sql}{emit}",
        Ident::with_quote('"', name)
    );
    let mut statements = laminar_sql::parse_streaming_sql(&ddl)?;
    match (statements.pop(), statements.is_empty()) {
        (Some(statement @ StreamingStatement::CreateStream { .. }), true) => Ok(statement),
        _ => Err(stage_parse_error(name)),
    }
}

fn stage_parse_error(name: &str) -> DbError {
    DbError::InvalidOperation(format!("stage '{name}' did not parse as one CREATE STREAM"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::AsArray;

    use crate::db::exact_table_reference;
    use crate::handle::ExecuteResult;
    use crate::LaminarDB;

    const SOURCES: [&str; 2] = [
        "CREATE SOURCE orders (k BIGINT NOT NULL, v BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
        "CREATE SOURCE payments (k BIGINT NOT NULL, v BIGINT NOT NULL, ts TIMESTAMP NOT NULL, \
         WATERMARK FOR ts AS ts - INTERVAL '1' MILLISECOND)",
    ];
    const FUSED: &str = "CREATE STREAM out AS SELECT o.k AS k, COUNT(*) AS n FROM orders o \
         JOIN payments p ON o.k = p.k \
         AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND GROUP BY o.k EMIT CHANGES";

    async fn db(ddl: &[&str]) -> Arc<LaminarDB> {
        let db = LaminarDB::open().unwrap();
        for statement in SOURCES.iter().chain(ddl) {
            db.execute(statement).await.unwrap();
        }
        db
    }

    fn metadata(result: ExecuteResult) -> Vec<(String, String)> {
        let ExecuteResult::Metadata(batch) = result else {
            panic!("expected metadata");
        };
        let keys = batch.column(0).as_string::<i32>();
        let values = (batch.num_columns() > 1).then(|| batch.column(1).as_string::<i32>());
        (0..batch.num_rows())
            .map(|row| {
                let value = values.map_or("", |values| values.value(row));
                (keys.value(row).to_string(), value.to_string())
            })
            .collect()
    }

    fn catalog_names(db: &LaminarDB) -> Vec<String> {
        let mut names: Vec<_> = db.catalog_namespace.lock().keys().cloned().collect();
        names.sort();
        names
    }

    async fn error(db: &LaminarDB, sql: &str) -> String {
        db.execute(sql).await.unwrap_err().to_string()
    }

    #[tokio::test]
    async fn split_stream_is_one_catalog_object() {
        let db = db(&[FUSED]).await;
        assert_eq!(
            catalog_names(&db),
            ["orders", "out", "out$stage1", "payments"]
        );
        let shown = metadata(db.execute("SHOW STREAMS").await.unwrap());
        assert_eq!(
            shown.iter().map(|(name, _)| name).collect::<Vec<_>>(),
            ["out"]
        );
        assert_eq!(
            db.streams().into_iter().map(|s| s.name).collect::<Vec<_>>(),
            ["out"]
        );
        assert_eq!(
            db.connector_manager.lock().ordered_ddl(),
            [
                ("orders".into(), SOURCES[0].into(), 1),
                ("payments".into(), SOURCES[1].into(), 1),
                ("out".into(), FUSED.into(), 1),
            ]
        );
        let resolved = db.resolve_catalog_stream_outputs(None).await.unwrap();
        let plan = resolved.exchange_plan.stream("out").unwrap();
        assert_eq!(
            plan.inputs
                .values()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["colocated on (o_k Int64)"]
        );
        assert!(db.lookup_subscription_schema("out$stage1").is_none());
    }

    #[tokio::test]
    async fn stages_are_reserved_for_their_owner() {
        let db = db(&[FUSED]).await;
        for (sql, expected) in [
            ("DROP STREAM \"out$stage1\"", "drop 'out' instead"),
            (
                "CREATE STREAM \"x$stage1\" AS SELECT k FROM orders",
                "reserved for an internal stage of stream 'x'",
            ),
            (
                "CREATE STREAM y AS SELECT \"o_k\" FROM \"out$stage1\"",
                "read 'out' instead",
            ),
            ("CREATE SINK s FROM \"out$stage1\"", "read 'out' instead"),
            ("DROP SOURCE orders", "depended on by out; use CASCADE"),
        ] {
            let error = error(&db, sql).await;
            assert!(error.contains(expected), "{sql}: {error}");
        }
    }

    #[tokio::test]
    async fn dropping_the_owner_or_its_source_removes_every_stage() {
        let db = db(&[FUSED]).await;
        db.execute("DROP STREAM out").await.unwrap();
        assert_eq!(catalog_names(&db), ["orders", "payments"]);
        assert!(!db
            .ctx
            .table_exist(exact_table_reference("out$stage1"))
            .unwrap());
        assert!(db.connector_manager.lock().streams().is_empty());

        db.execute(FUSED).await.unwrap();
        db.execute("DROP SOURCE payments CASCADE").await.unwrap();
        assert_eq!(catalog_names(&db), ["orders"]);
        assert!(db.connector_manager.lock().streams().is_empty());
    }

    #[tokio::test]
    async fn a_rejected_owner_drops_the_stages_it_created() {
        let db = db(&[]).await;
        // The inner stage admits; MIN cannot consume its weighted changelog.
        let error = error(
            &db,
            "CREATE STREAM out AS SELECT MIN(n) AS lo FROM \
             (SELECT k, COUNT(*) AS n FROM orders GROUP BY k) EMIT CHANGES",
        )
        .await;
        assert!(
            error.contains("MIN/MAX require append-only input"),
            "{error}"
        );
        assert_eq!(catalog_names(&db), ["orders", "payments"]);
        assert!(db.connector_manager.lock().streams().is_empty());
        assert!(!db
            .ctx
            .table_exist(exact_table_reference("out$stage1"))
            .unwrap());
    }

    /// Consolidated `(n, keys)` changes until they equal `expected` or the deadline passes.
    async fn consolidated_changes(
        portal: &mut crate::subscription::SubscriptionPortal,
        expected: &std::collections::BTreeMap<(i64, i64), i64>,
    ) -> std::collections::BTreeMap<(i64, i64), i64> {
        let mut changes = std::collections::BTreeMap::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while &changes != expected {
            let Ok(Some(envelope)) =
                tokio::time::timeout_at(deadline, portal.next_envelope()).await
            else {
                break;
            };
            let crate::subscription::PortalFrame::Batch { batch, .. } = envelope.frame else {
                continue;
            };
            let column = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .clone()
            };
            let (n, keys) = (column("n"), column("keys"));
            let weight = column(laminar_core::changelog::WEIGHT_COLUMN);
            for row in 0..batch.num_rows() {
                *changes.entry((n.value(row), keys.value(row))).or_default() += weight.value(row);
            }
            changes.retain(|_, weight| *weight != 0);
        }
        changes
    }

    #[tokio::test]
    async fn stage_state_survives_a_checkpoint_restart() {
        let storage = tempfile::tempdir().unwrap();
        let db = LaminarDB::open_with_config(crate::LaminarConfig {
            storage_dir: Some(storage.path().to_path_buf()),
            checkpoint: Some(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            }),
            ..Default::default()
        })
        .unwrap();
        db.execute("CREATE SOURCE events (k BIGINT, v BIGINT)")
            .await
            .unwrap();
        db.execute(
            "CREATE STREAM out AS SELECT n, COUNT(*) AS keys FROM \
             (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n EMIT CHANGES",
        )
        .await
        .unwrap();
        let push = |keys: &[i64]| {
            let column = Arc::new(arrow::array::Int64Array::from(keys.to_vec())) as _;
            let batch = arrow::array::RecordBatch::try_from_iter_with_nullable(vec![
                ("k", Arc::clone(&column), true),
                ("v", column, true),
            ])
            .unwrap();
            db.source_untyped("events")
                .unwrap()
                .push_arrow(batch)
                .unwrap();
        };
        db.start().await.unwrap();
        let mut portal = db
            .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
            .await
            .unwrap();
        push(&[1, 2, 3]);
        let expected = std::collections::BTreeMap::from([((1, 3), 1)]);
        assert_eq!(consolidated_changes(&mut portal, &expected).await, expected);
        assert!(db.checkpoint().await.unwrap().success);
        drop(portal);
        db.stop_pipeline().await.unwrap();

        db.start().await.unwrap();
        let mut portal = db
            .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
            .await
            .unwrap();
        // Key 1 moves from one to two rows only if its stage count was restored.
        push(&[1]);
        let expected = std::collections::BTreeMap::from([((1, 2), 1), ((1, 3), -1), ((2, 1), 1)]);
        assert_eq!(consolidated_changes(&mut portal, &expected).await, expected);
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explain_lists_each_stage() {
        let db = db(&[]).await;
        let explained = metadata(db.execute(&format!("EXPLAIN {FUSED}")).await.unwrap());
        let stages: Vec<_> = explained
            .iter()
            .filter(|(key, _)| key.starts_with("stage."))
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        assert_eq!(
            stages,
            [
                (
                    "stage.out$stage1",
                    "SELECT o.k AS \"o_k\" FROM orders o JOIN payments p ON o.k = p.k \
                     AND p.ts BETWEEN o.ts AND o.ts + INTERVAL '50' MILLISECOND"
                ),
                (
                    "stage.out",
                    "SELECT \"o_k\" AS k, COUNT(*) AS n FROM \"out$stage1\" GROUP BY \"o_k\""
                ),
            ]
        );
    }
}
