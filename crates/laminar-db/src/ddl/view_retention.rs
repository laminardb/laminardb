//! The retention an append-only view keeps its log under, resolved at `CREATE`.

use std::time::Duration;

use arrow::datatypes::{DataType, Schema};
use laminar_sql::parser::MaterializedViewRetention;
use laminar_sql::translator::WindowType;

use crate::connector_manager::StreamRegistration;
use crate::db::LaminarDB;
use crate::error::DbError;
use crate::mv_store::ViewRetention;

/// Retention of an append-only view with an event-time column that declares none.
pub(crate) const DEFAULT_APPEND_VIEW_RETENTION: Duration = Duration::from_secs(3600);

impl LaminarDB {
    /// An interval keeps rows by the view's event-time column: the source column a projection
    /// forwards unchanged, or a window's start, which keeps a window while its end is inside
    /// the interval. A view without one must declare a row count.
    pub(super) async fn append_view_retention(
        &self,
        candidate: StreamRegistration,
        schema: &Schema,
        declared: Option<&MaterializedViewRetention>,
    ) -> Result<ViewRetention, DbError> {
        let name = candidate.name.clone();
        let interval = match declared {
            Some(MaterializedViewRetention::Rows(rows)) => return self.row_retention(&name, *rows),
            Some(MaterializedViewRetention::Unbounded) => {
                return Err(DbError::MaterializedView(format!(
                    "materialized view '{name}' cannot keep an unbounded log until log \
                     compaction is available; declare retention as an interval or a row count"
                )))
            }
            Some(MaterializedViewRetention::Interval(interval)) => *interval,
            None => DEFAULT_APPEND_VIEW_RETENTION,
        };
        let interval = interval.saturating_add(candidate.window_config.as_ref().map_or(
            Duration::ZERO,
            |window| match window.window_type {
                WindowType::Session => window.gap.unwrap_or_default(),
                WindowType::Tumbling | WindowType::Sliding | WindowType::Cumulate => window.size,
            },
        ));
        let column = self
            .append_view_event_time(candidate, schema)
            .await?
            .ok_or_else(|| {
                DbError::MaterializedView(format!(
                    "append-only materialized view '{name}' has no event-time column to keep \
                     an interval by; declare WITH (retention = '<n> rows')"
                ))
            })?;
        let interval_ms = i64::try_from(interval.as_millis()).map_err(|_| {
            DbError::MaterializedView(format!(
                "materialized view '{name}' retention exceeds the event-time range"
            ))
        })?;
        Ok(ViewRetention::EventTime {
            column,
            interval_ms,
        })
    }

    /// Without checkpointing the whole log stays in memory, so a cap at or above the view row
    /// limit would fail its first cycle past the limit.
    fn row_retention(&self, name: &str, rows: u64) -> Result<ViewRetention, DbError> {
        let limit = self.config.materialized_view_max_rows;
        if self.config.checkpoint.is_none()
            && usize::try_from(rows).map_or(true, |rows| rows >= limit)
        {
            return Err(DbError::MaterializedView(format!(
                "materialized view '{name}' keeps {rows} rows in memory without checkpointing; \
                 declare fewer than materialized_view_max_rows ({limit})"
            )));
        }
        Ok(ViewRetention::Rows(rows))
    }

    async fn append_view_event_time(
        &self,
        candidate: StreamRegistration,
        schema: &Schema,
    ) -> Result<Option<usize>, DbError> {
        let name = candidate.name.clone();
        let column = if candidate.window_config.is_some() {
            let plan = self
                .ctx
                .sql(&candidate.query_sql)
                .await?
                .into_unoptimized_plan();
            crate::exchange_plan::window_output_column(&plan)
                .map(|index| plan.schema().field(index).name().clone())
        } else {
            let resolved = self.resolve_catalog_stream_outputs(Some(candidate)).await?;
            resolved
                .exchange_plan
                .stream(&name)
                .and_then(|stream| stream.output.event_time.as_ref())
                .map(|event_time| event_time.column.clone())
        };
        Ok(column
            .and_then(|column| schema.index_of(&column).ok())
            .filter(|&index| matches!(schema.field(index).data_type(), DataType::Timestamp(_, _))))
    }
}
