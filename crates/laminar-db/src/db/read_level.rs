//! Ad-hoc queries that read materialized views at a level the session chose: after the latest
//! completed cycle, or at one committed checkpoint, possibly the first to hold what the views'
//! sources had upstream when the read arrived.

use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

use arrow::datatypes::SchemaRef;
use laminar_connectors::config::ConnectorConfig;
use laminar_sql::parser::{parse_streaming_sql, StreamingStatement};

use super::LaminarDB;
use crate::error::DbError;
use crate::mv_store::{
    CommittedRead, CommittedReadError, CommittedViews, PinnedCut, ReadCut, RecencyTargets,
    VIEW_READ_DEADLINE,
};
use crate::query_rows::{execution_error, QueryRows};

/// How an ad-hoc query reads the materialized views it scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadLevel {
    /// Every view after the latest completed cycle, read under one guard. Recovery may roll the
    /// result back. Embedded and single-node only: a cluster node holds only its part of each
    /// view.
    Latest,
    /// Every view at the one committed checkpoint the argument resolves to; see
    /// [`LaminarDB::query_committed`].
    Committed(CommittedRead),
}

/// The rows of an ad-hoc read and the committed cut it observed.
#[derive(Debug)]
pub struct ReadResult {
    /// The cut every view was read at; `None` for a [`ReadLevel::Latest`] read.
    pub cut: Option<ReadCut>,
    /// The result rows.
    pub rows: QueryRows,
}

impl LaminarDB {
    /// The level a read uses when its session chose none: [`ReadLevel::Latest`] in embedded and
    /// single-node mode, the newest committed checkpoint on a cluster node.
    #[must_use]
    pub fn default_read_level(&self) -> ReadLevel {
        if self.is_cluster_runtime() {
            ReadLevel::Committed(CommittedRead::Latest)
        } else {
            ReadLevel::Latest
        }
    }

    /// Run the query `sql` with every materialized view it scans read at `level`.
    ///
    /// Each table is read as a snapshot, so joins between views, sources and reference tables are
    /// bounded. A committed read loads its views before this returns and holds its checkpoint
    /// against retention until then; the rows then stream to the reader as it reads them.
    ///
    /// # Errors
    /// [`DbError::InvalidOperation`] if `sql` is not one query or reads a view at
    /// [`ReadLevel::Latest`] on a cluster node; the errors of [`Self::query_committed`] for a
    /// committed read; any planning or execution error of `sql`.
    pub async fn read(&self, sql: &str, level: ReadLevel) -> Result<ReadResult, DbError> {
        if self.shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(DbError::Shutdown);
        }
        ensure_query(sql)?;
        let result = match level {
            ReadLevel::Latest => {
                let frame = crate::mv_store::plan_statement(&self.ctx, &self.mv_store, sql).await?;
                let stream = frame.execute_stream().await.map_err(execution_error)?;
                if let Some(metrics) = self.engine_metrics.lock().as_ref() {
                    metrics.views.observe_read("latest", None);
                }
                ReadResult {
                    cut: None,
                    rows: self.spawn_rows(sql, stream),
                }
            }
            ReadLevel::Committed(at) => {
                let (cut, stream) = self
                    .committed_stream(sql, at, Instant::now() + VIEW_READ_DEADLINE)
                    .await?;
                ReadResult {
                    cut: Some(cut),
                    rows: self.spawn_rows(sql, stream),
                }
            }
        };
        Ok(result)
    }

    /// The schema of the rows [`Self::read`] returns for `sql` and the parameter count it
    /// declares, planned without reading any table.
    ///
    /// # Errors
    /// [`DbError::InvalidOperation`] if `sql` is not one query; any planning error of `sql`.
    pub async fn read_schema(&self, sql: &str) -> Result<(SchemaRef, usize), DbError> {
        ensure_query(sql)?;
        let plan = self.ctx.state().create_logical_plan(sql).await?;
        let parameters = plan.get_parameter_types()?.len();
        Ok((
            std::sync::Arc::new(plan.schema().as_arrow().clone()),
            parameters,
        ))
    }

    /// Plan `sql` at the committed cut `at` resolves to and load its views.
    pub(crate) async fn committed_stream(
        &self,
        sql: &str,
        at: CommittedRead,
        deadline: Instant,
    ) -> Result<
        (
            ReadCut,
            datafusion::physical_plan::SendableRecordBatchStream,
        ),
        DbError,
    > {
        let planned = crate::mv_store::plan_committed_statement(&self.ctx, sql).await?;
        let views = self
            .mv_store
            .read()
            .committed_views()
            .ok_or(CommittedReadError::CheckpointingDisabled)?;
        let (level, cut) = match at {
            CommittedRead::Recent { at_least } => (
                "recent",
                self.pin_recent(&views, planned.views(), at_least, deadline)
                    .await?,
            ),
            at => ("committed", views.pin(at, deadline).await?),
        };
        let (frame, cut) = planned.at_cut(&self.mv_store, views, cut);
        // View scans load their artifacts while the plan is built.
        let stream = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            frame.execute_stream(),
        )
        .await
        .map_err(|_| CommittedReadError::DeadlineExceeded { epoch: cut.epoch })?
        .map_err(execution_error)?;
        if let Some(metrics) = self.engine_metrics.lock().as_ref() {
            metrics.views.observe_read(level, Some(cut.taken_at_ms));
        }
        Ok((cut, stream))
    }

    /// Pin the newest committed cut at or after `at_least` that holds what every source behind
    /// `scanned` had upstream when this was called.
    async fn pin_recent(
        &self,
        views: &CommittedViews,
        scanned: &[String],
        at_least: u64,
        deadline: Instant,
    ) -> Result<PinnedCut, DbError> {
        let arrived = Instant::now();
        let pinned = match self.recency_targets(scanned, deadline).await {
            Ok(targets) => views.pin_covering(&targets, at_least, deadline).await,
            Err(error) => Err(error),
        };
        if let Some(metrics) = self.engine_metrics.lock().as_ref() {
            match &pinned {
                Ok(_) => metrics
                    .views
                    .recent_read_wait_seconds
                    .observe(arrived.elapsed().as_secs_f64()),
                Err(DbError::CommittedRead(CommittedReadError::RecencyProbe { input, .. })) => {
                    metrics
                        .views
                        .recency_probe_failures_total
                        .with_label_values(&[input])
                        .inc();
                }
                Err(_) => {}
            }
        }
        pinned
    }

    /// Each source behind `views`, with its upstream's end now.
    async fn recency_targets(
        &self,
        views: &[String],
        deadline: Instant,
    ) -> Result<RecencyTargets, DbError> {
        let probe_failed = |source: &str, reason: String| CommittedReadError::RecencyProbe {
            input: source.to_owned(),
            reason,
        };
        let mut probes = Vec::new();
        for (source, config) in self.view_lineage(views)? {
            let probe = self
                .connector_registry
                .source_recency(&config)
                .map_err(|error| probe_failed(&source, error.to_string()))?
                .ok_or_else(|| CommittedReadError::RecencyUnsupported {
                    input: source.clone(),
                })?;
            probes.push((source, probe));
        }
        let ends = probes.into_iter().map(|(source, probe)| async move {
            let end = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                probe.upstream_end(deadline),
            )
            .await;
            match end {
                Ok(Ok(end)) => Ok((source, end)),
                Ok(Err(error)) => Err(probe_failed(&source, error.to_string())),
                Err(_) => Err(probe_failed(
                    &source,
                    "no answer before the read's deadline".into(),
                )),
            }
        });
        Ok(RecencyTargets::new(
            futures::future::try_join_all(ends).await?,
        ))
    }

    /// The connector configuration of every source whose records reach `views`, through the
    /// streams, stages and views between them. A table is looked up rather than read through an
    /// input, so it adds no source.
    fn view_lineage(&self, views: &[String]) -> Result<BTreeMap<String, ConnectorConfig>, DbError> {
        use laminar_core::catalog::CatalogObjectKind;
        let unsupported = |input: String| CommittedReadError::RecencyUnsupported { input };
        let kinds = self.catalog_namespace.lock().clone();
        let mut sources = BTreeMap::new();
        {
            let manager = self.connector_manager.lock();
            let mut pending = views.to_vec();
            let mut visited = HashSet::new();
            // Each catalog name is expanded once.
            while let Some(name) = pending.pop() {
                if !visited.insert(name.clone()) {
                    continue;
                }
                match kinds.get(&name) {
                    Some(CatalogObjectKind::Stream | CatalogObjectKind::MaterializedView) => {
                        let stream = manager.streams().get(&name);
                        let stream = stream.ok_or_else(|| unsupported(name.clone()))?;
                        pending.extend(crate::sql_analysis::extract_table_references(
                            &stream.query_sql,
                        ));
                    }
                    Some(CatalogObjectKind::Source) => {
                        let source = manager.sources().get(&name);
                        let source = source.filter(|source| source.connector_type.is_some());
                        let source = source.ok_or_else(|| unsupported(name.clone()))?.clone();
                        sources.insert(name, source);
                    }
                    Some(CatalogObjectKind::Table | CatalogObjectKind::LookupTable) => {}
                    Some(CatalogObjectKind::Sink) | None => return Err(unsupported(name).into()),
                }
            }
        }
        sources
            .into_iter()
            .map(|(name, registration)| {
                let config = self.build_registered_source_config(&name, &registration)?;
                Ok((name, config))
            })
            .collect()
    }

    fn spawn_rows(
        &self,
        sql: &str,
        stream: datafusion::physical_plan::SendableRecordBatchStream,
    ) -> QueryRows {
        let query_id = self.catalog.register_query(sql);
        let catalog = std::sync::Arc::clone(&self.catalog);
        QueryRows::spawn(
            stream,
            tokio_util::sync::CancellationToken::new(),
            move || {
                catalog.deactivate_query(query_id);
            },
        )
    }
}

/// A read runs one query; DDL, DML and session statements go through `execute`.
fn ensure_query(sql: &str) -> Result<(), DbError> {
    let statements = parse_streaming_sql(sql)?;
    match statements.as_slice() {
        [StreamingStatement::Standard(statement)]
            if matches!(**statement, sqlparser::ast::Statement::Query(_)) =>
        {
            Ok(())
        }
        [StreamingStatement::TemporalProbeQuery { .. }] => Err(DbError::Unsupported(
            "temporal queries require the managed vnode runtime and cannot execute through \
             DataFusion's ordinary join path"
                .into(),
        )),
        _ => Err(DbError::InvalidOperation(
            "a read must be exactly one query".into(),
        )),
    }
}

#[cfg(all(test, feature = "kafka"))]
mod tests {
    use super::*;
    use crate::config::LaminarConfig;

    #[tokio::test]
    async fn lineage_follows_streams_stages_and_views_to_their_sources() {
        let db = LaminarDB::open_with_config(LaminarConfig::default()).unwrap();
        for sql in [
            "CREATE SOURCE orders (k BIGINT, v BIGINT) FROM KAFKA (\
             'bootstrap.servers' = '127.0.0.1:1', 'topic' = 'orders', 'group.id' = 'lineage') \
             FORMAT JSON",
            "CREATE SOURCE pushed (k BIGINT)",
            "CREATE TABLE dims (k BIGINT PRIMARY KEY, name VARCHAR)",
            "CREATE STREAM doubled AS SELECT k, v * 2 AS v FROM orders",
            "CREATE MATERIALIZED VIEW sums AS SELECT k, SUM(v) AS total FROM doubled GROUP BY k",
            "CREATE MATERIALIZED VIEW big AS SELECT k FROM sums WHERE total > 10",
            "CREATE MATERIALIZED VIEW spread AS SELECT n, COUNT(*) AS keys \
             FROM (SELECT k, COUNT(*) AS n FROM orders GROUP BY k) GROUP BY n",
            "CREATE MATERIALIZED VIEW named AS SELECT s.k, s.total, d.name FROM sums s \
             JOIN dims d ON s.k = d.k",
            "CREATE MATERIALIZED VIEW pushed_counts AS SELECT k, COUNT(*) AS n FROM pushed GROUP BY k",
        ] {
            db.execute(sql)
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
        }
        let lineage = |views: &[&str]| {
            let views: Vec<String> = views.iter().map(|view| (*view).to_owned()).collect();
            db.view_lineage(&views)
                .map(|sources| sources.into_keys().collect::<Vec<_>>())
        };
        assert_eq!(lineage(&["big"]).unwrap(), ["orders"]);
        assert_eq!(lineage(&["spread"]).unwrap(), ["orders"]);
        assert_eq!(lineage(&["named"]).unwrap(), ["orders"]);
        assert!(lineage(&[]).unwrap().is_empty());
        let refused = lineage(&["big", "pushed_counts"]);
        assert!(
            matches!(
                &refused,
                Err(DbError::CommittedRead(CommittedReadError::RecencyUnsupported { input }))
                    if input == "pushed"
            ),
            "{refused:?}"
        );
    }
}
