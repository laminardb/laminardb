//! Ad-hoc queries that read materialized views at a level the session chose: after the latest
//! completed cycle, or at one committed checkpoint.

use std::time::Instant;

use arrow::datatypes::SchemaRef;
use laminar_sql::parser::{parse_streaming_sql, StreamingStatement};

use super::LaminarDB;
use crate::error::DbError;
use crate::mv_store::{CommittedRead, CommittedReadError, ReadCut, VIEW_READ_DEADLINE};
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
                    metrics.views.observe_read(None);
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
        let (frame, cut) =
            crate::mv_store::plan_committed_statement(&self.ctx, &self.mv_store, sql, at, deadline)
                .await?;
        // View scans load their artifacts while the plan is built.
        let stream = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            frame.execute_stream(),
        )
        .await
        .map_err(|_| CommittedReadError::DeadlineExceeded { epoch: cut.epoch })?
        .map_err(execution_error)?;
        if let Some(metrics) = self.engine_metrics.lock().as_ref() {
            metrics.views.observe_read(Some(cut.taken_at_ms));
        }
        Ok((cut, stream))
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
