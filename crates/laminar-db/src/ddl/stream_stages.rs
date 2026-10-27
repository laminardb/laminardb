//! Split statements in the catalog: an owner stream or materialized view and its internal stages
//! are created and dropped as one object. Only the owner stores DDL, so replay re-splits the
//! owner's statement.
//!
//! INVARIANT: an owner exists only after all of its stages; a stage without its owner is left by a
//! cancelled create and is dropped before the owner's name is created again.

use laminar_core::catalog::CatalogObjectKind;
use laminar_sql::parser::{EmitClause, StreamingStatement};
use sqlparser::ast::Ident;

use crate::connector_manager::StreamRegistration;
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
        let created = match self.create_stages(&owner_name, split).await {
            Ok(()) => self.create_split_owner(owner, split, reservation).await,
            Err(error) => Err(error),
        };
        self.drop_stages_on_failure(&owner_name, created).await
    }

    async fn create_split_owner(
        &self,
        owner: StreamDefinition<'_>,
        split: &SplitQuery,
        reservation: CatalogNameReservation<'_>,
    ) -> Result<(), DbError> {
        let owner_name = crate::db::canonical_object_name(owner.name)?;
        let (query, query_sql) = parse_split_owner(&owner_name, split, owner.emit_clause)?;
        self.validate_stream_query_text(&owner_name, &query_sql)?;
        let definition = StreamDefinition {
            query: &query,
            query_sql: &query_sql,
            ..owner
        };
        self.create_stream_object(definition, reservation).await
    }

    /// Creates `owner`'s stages in order, after dropping any that a cancelled create left.
    pub(super) async fn create_stages(
        &self,
        owner: &str,
        split: &SplitQuery,
    ) -> Result<(), DbError> {
        self.drop_stream_stages(owner).await?;
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
        Ok(())
    }

    /// `created`, after dropping `owner`'s stages when it failed.
    pub(super) async fn drop_stages_on_failure(
        &self,
        owner: &str,
        created: Result<(), DbError>,
    ) -> Result<(), DbError> {
        let Err(error) = created else {
            return Ok(());
        };
        match self.drop_stream_stages(owner).await {
            Ok(()) => Err(error),
            Err(cleanup) => Err(DbError::Pipeline(format!(
                "{error}; dropping the stages of '{owner}' also failed: {cleanup}"
            ))),
        }
    }

    /// The registrations CREATE would give each stage and then the owner, for planning a split
    /// statement without creating it.
    pub(crate) async fn split_stream_candidates(
        &self,
        owner: &str,
        split: &SplitQuery,
        owner_emit: Option<&EmitClause>,
    ) -> Result<Vec<StreamRegistration>, DbError> {
        let stages = split.stages.iter().map(|stage| {
            let emit = stage.changelog.then_some(EmitClause::Changes);
            (stage.name.as_str(), stage.sql.as_str(), emit)
        });
        let owner = (owner, split.query_sql.as_str(), owner_emit.cloned());
        let mut candidates = Vec::with_capacity(split.stages.len() + 1);
        for (name, sql, emit) in stages.chain(std::iter::once(owner)) {
            let statement = parse_stage(name, sql, emit.as_ref())?;
            let StreamingStatement::CreateStream {
                name: object,
                query,
                emit_clause,
                query_sql,
                ..
            } = &statement
            else {
                return Err(stage_parse_error(name));
            };
            let planned = self
                .plan_streaming_query(object, query, emit_clause.clone(), query_sql)
                .await?;
            candidates.push(planned.candidate_registration(name, query_sql));
        }
        Ok(candidates)
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

/// The owner's query over its stages and that query's text, parsed as CREATE parses a query.
pub(super) fn parse_split_owner(
    owner: &str,
    split: &SplitQuery,
    emit: Option<&EmitClause>,
) -> Result<(StreamingStatement, String), DbError> {
    match parse_stage(owner, &split.query_sql, emit)? {
        StreamingStatement::CreateStream {
            query, query_sql, ..
        } => Ok((*query, query_sql)),
        _ => Err(stage_parse_error(owner)),
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
                "reserved for an internal stage of 'x'",
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
    async fn split_view_is_one_catalog_object() {
        const VIEW: &str = "CREATE MATERIALIZED VIEW hist AS SELECT n, COUNT(*) AS keys FROM \
             (SELECT k, COUNT(*) AS n FROM orders GROUP BY k) GROUP BY n";
        let db = db(&[VIEW]).await;
        assert_eq!(
            catalog_names(&db),
            ["hist", "hist$stage1", "orders", "payments"]
        );
        assert_eq!(
            db.connector_manager.lock().ordered_ddl()[2],
            ("hist".into(), VIEW.into(), 1)
        );
        assert_eq!(
            db.mv_registry.lock().get("hist").unwrap().sources,
            ["orders"]
        );
        for (sql, expected) in [
            ("DROP STREAM \"hist$stage1\"", "drop 'hist' instead"),
            ("DROP SOURCE orders", "depended on by hist; use CASCADE"),
        ] {
            let error = error(&db, sql).await;
            assert!(error.contains(expected), "{sql}: {error}");
        }

        db.execute("DROP MATERIALIZED VIEW hist").await.unwrap();
        assert_eq!(catalog_names(&db), ["orders", "payments"]);
        assert!(db.connector_manager.lock().streams().is_empty());
        assert!(!db
            .ctx
            .table_exist(exact_table_reference("hist$stage1"))
            .unwrap());

        db.execute(VIEW).await.unwrap();
        db.execute("DROP SOURCE orders CASCADE").await.unwrap();
        assert_eq!(catalog_names(&db), ["payments"]);
        assert!(db.connector_manager.lock().streams().is_empty());
        assert!(db.mv_registry.lock().get("hist").is_none());
    }

    #[tokio::test]
    async fn a_rejected_owner_drops_the_stages_it_created() {
        let db = db(&[]).await;
        // The inner stage admits; MIN cannot consume its weighted changelog.
        for sql in [
            "CREATE STREAM out AS SELECT MIN(n) AS lo FROM \
             (SELECT k, COUNT(*) AS n FROM orders GROUP BY k) EMIT CHANGES",
            "CREATE MATERIALIZED VIEW out AS SELECT MIN(n) AS lo FROM \
             (SELECT k, COUNT(*) AS n FROM orders GROUP BY k)",
        ] {
            let error = error(&db, sql).await;
            assert!(
                error.contains("MIN/MAX require append-only input"),
                "{sql}: {error}"
            );
            assert_eq!(catalog_names(&db), ["orders", "payments"]);
            assert!(db.connector_manager.lock().streams().is_empty());
            assert!(!db
                .ctx
                .table_exist(exact_table_reference("out$stage1"))
                .unwrap());
        }
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

    const HISTOGRAM: &str = "SELECT n, COUNT(*) AS keys FROM \
         (SELECT k, COUNT(*) AS n FROM events GROUP BY k) GROUP BY n";

    async fn checkpointed_db(storage: &std::path::Path, ddl: &str) -> Arc<LaminarDB> {
        let db = LaminarDB::open_with_config(crate::LaminarConfig {
            storage_dir: Some(storage.to_path_buf()),
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
        db.execute(ddl).await.unwrap();
        db
    }

    fn push(db: &LaminarDB, keys: &[i64]) {
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
    }

    #[tokio::test]
    async fn stage_state_survives_a_checkpoint_restart() {
        let storage = tempfile::tempdir().unwrap();
        let db = checkpointed_db(
            storage.path(),
            &format!("CREATE STREAM out AS {HISTOGRAM} EMIT CHANGES"),
        )
        .await;
        db.start().await.unwrap();
        let mut portal = db
            .open_subscription("out", None, crate::subscription::SubscribeStart::Tail)
            .await
            .unwrap();
        push(&db, &[1, 2, 3]);
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
        push(&db, &[1]);
        let expected = std::collections::BTreeMap::from([((1, 2), 1), ((1, 3), -1), ((2, 1), 1)]);
        assert_eq!(consolidated_changes(&mut portal, &expected).await, expected);
        db.shutdown().await.unwrap();
    }

    /// View `hist`'s `(n, keys)` rows once they equal `expected` or the deadline passes.
    async fn view_rows(
        db: &LaminarDB,
        expected: &std::collections::BTreeMap<i64, i64>,
    ) -> std::collections::BTreeMap<i64, i64> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let batch = crate::mv_store::read_views(&db.mv_store, &["hist".to_owned()])
                .await
                .unwrap()
                .remove("hist")
                .unwrap();
            let column = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_primitive::<arrow::datatypes::Int64Type>()
                    .clone()
            };
            let (n, keys) = (column("n"), column("keys"));
            let rows = (0..batch.num_rows())
                .map(|row| (n.value(row), keys.value(row)))
                .collect();
            if &rows == expected || tokio::time::Instant::now() >= deadline {
                return rows;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn split_view_state_survives_a_checkpoint_restart() {
        let storage = tempfile::tempdir().unwrap();
        let db = checkpointed_db(
            storage.path(),
            &format!("CREATE MATERIALIZED VIEW hist AS {HISTOGRAM}"),
        )
        .await;
        db.start().await.unwrap();
        push(&db, &[1, 2, 3]);
        let expected = std::collections::BTreeMap::from([(1, 3)]);
        assert_eq!(view_rows(&db, &expected).await, expected);
        assert!(db.checkpoint().await.unwrap().success);
        db.stop_pipeline().await.unwrap();

        db.start().await.unwrap();
        // Key 1 moves from one to two rows only if its stage count and the view were restored.
        push(&db, &[1]);
        let expected = std::collections::BTreeMap::from([(1, 2), (2, 1)]);
        assert_eq!(view_rows(&db, &expected).await, expected);
        db.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn explain_lists_each_stage_with_its_exchanges() {
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
                    "stage.out$stage1.exchange.input.left:orders",
                    "repartition on (k Int64) from arbitrary, append-only, event time ts"
                ),
                (
                    "stage.out$stage1.exchange.input.right:payments",
                    "repartition on (k Int64) from arbitrary, append-only, event time ts"
                ),
                (
                    "stage.out$stage1.exchange.output",
                    "hash(o_k Int64), append-only"
                ),
                (
                    "stage.out",
                    "SELECT \"o_k\" AS k, COUNT(*) AS n FROM \"out$stage1\" GROUP BY \"o_k\""
                ),
                (
                    "stage.out.exchange.input.out$stage1",
                    "colocated on (o_k Int64) from hash(o_k Int64), append-only"
                ),
                (
                    "stage.out.exchange.output",
                    "hash(k Int64), weighted changelog"
                ),
            ]
        );
        assert!(!db
            .ctx
            .table_exist(exact_table_reference("out$stage1"))
            .unwrap());
    }
}
