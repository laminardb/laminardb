//! The retention an append-only view keeps its log under, resolved at `CREATE`.

use std::time::Duration;

use arrow::datatypes::{DataType, Schema};
use laminar_sql::parser::MaterializedViewRetention;
use laminar_sql::translator::{WindowOperatorConfig, WindowType};

use crate::connector_manager::StreamRegistration;
use crate::db::LaminarDB;
use crate::error::DbError;
use crate::exchange_plan::WindowBoundary;
use crate::mv_store::ViewRetention;

/// Retention of an append-only view with an event-time column that declares none.
pub(crate) const DEFAULT_APPEND_VIEW_RETENTION: Duration = Duration::from_secs(3600);

impl LaminarDB {
    /// An interval keeps rows by the view's event-time column. A view without one must declare a
    /// row count.
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
                return self.unbounded_retention(candidate, schema).await
            }
            Some(MaterializedViewRetention::Interval(interval)) => *interval,
            None => DEFAULT_APPEND_VIEW_RETENTION,
        };
        let (column, trail) = self
            .append_view_event_time(candidate, schema)
            .await?
            .ok_or_else(|| {
                DbError::MaterializedView(format!(
                    "append-only materialized view '{name}' has no event-time column to keep \
                     an interval by; declare WITH (retention = '<n> rows')"
                ))
            })?;
        let interval_ms =
            i64::try_from(interval.saturating_add(trail).as_millis()).map_err(|_| {
                DbError::MaterializedView(format!(
                    "materialized view '{name}' retention exceeds the event-time range"
                ))
            })?;
        Ok(ViewRetention::EventTime {
            column,
            interval_ms,
        })
    }

    /// Every row, with segment bounds on the event-time column when the view has one. Without
    /// checkpointing the whole log stays in memory, so it would fail its first cycle past the
    /// view limits.
    async fn unbounded_retention(
        &self,
        candidate: StreamRegistration,
        schema: &Schema,
    ) -> Result<ViewRetention, DbError> {
        if self.config.checkpoint.is_none() {
            return Err(DbError::MaterializedView(format!(
                "materialized view '{}' keeps every row in memory without checkpointing; \
                 declare retention as an interval or a row count",
                candidate.name
            )));
        }
        let column = self
            .append_view_event_time(candidate, schema)
            .await?
            .map(|(column, _)| column);
        Ok(ViewRetention::Unbounded { column })
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

    /// The view's event-time column, and how much further than the interval it trails the view's
    /// frontier:
    /// - a window view's first forwarded marker. A `TUMBLE` or `HOP` window that closes on
    ///   `EMIT ON WINDOW CLOSE` or `EMIT FINAL` holds its frontier back for the boundary the stream
    ///   carries, and any other window passes its input frontier on, so a start column trails by
    ///   the window size unless the frontier is held for the start. A session frontier is not
    ///   held, so a session start trails it by the gap and the allowed lateness.
    /// - otherwise the stream's event-time column.
    async fn append_view_event_time(
        &self,
        candidate: StreamRegistration,
        schema: &Schema,
    ) -> Result<Option<(usize, Duration)>, DbError> {
        let name = candidate.name.clone();
        let column = if let Some(window) = &candidate.window_config {
            let plan = self
                .ctx
                .sql(&candidate.query_sql)
                .await?
                .into_unoptimized_plan();
            let held = crate::exchange_plan::closes_windows(candidate.emit_clause.as_ref())
                .then(|| crate::exchange_plan::carried_window_boundary(&plan));
            crate::exchange_plan::window_retention_column(&plan).map(|(index, boundary)| {
                (
                    plan.schema().field(index).name().clone(),
                    window_trail(window, boundary, held),
                )
            })
        } else {
            let resolved = self.resolve_catalog_stream_outputs(Some(candidate)).await?;
            resolved
                .exchange_plan
                .stream(&name)
                .and_then(|stream| stream.output.event_time.as_ref())
                .map(|event_time| (event_time.column.clone(), Duration::ZERO))
        };
        Ok(column
            .and_then(|(column, trail)| Some((schema.index_of(&column).ok()?, trail)))
            .filter(|&(index, _)| {
                matches!(schema.field(index).data_type(), DataType::Timestamp(_, _))
            }))
    }
}

/// `held` is the boundary the window holds its frontier back for, or `None` when it passes its
/// input frontier on.
fn window_trail(
    window: &WindowOperatorConfig,
    column: WindowBoundary,
    held: Option<WindowBoundary>,
) -> Duration {
    match window.window_type {
        WindowType::Session => window
            .gap
            .unwrap_or_default()
            .saturating_add(window.allowed_lateness),
        WindowType::Tumbling | WindowType::Sliding | WindowType::Cumulate => match (column, held) {
            (WindowBoundary::Start, None | Some(WindowBoundary::End)) => window.size,
            (WindowBoundary::Start, Some(WindowBoundary::Start)) | (WindowBoundary::End, _) => {
                Duration::ZERO
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::db::LaminarDB;
    use crate::mv_store::{MvStorageMode, ViewRetention};

    /// A window view keeps a window while its end is within the interval of the frontier the window
    /// closes at, less the allowed lateness, on its first forwarded marker column, whether or not
    /// its `EMIT` clause holds its frontier back. A session keeps a session while its start is
    /// within the interval plus the gap and the lateness.
    #[tokio::test]
    async fn window_views_keep_an_interval_on_their_first_marker() {
        const TUMBLE: &str = "tumble(ts, INTERVAL '10' SECOND)";
        const TUMBLE_END: &str = "tumble_end(ts, INTERVAL '10' SECOND)";
        const HOP: &str = "hop(ts, INTERVAL '5' SECOND, INTERVAL '10' SECOND)";
        const SESSION: &str = "session(ts, INTERVAL '5' SECOND)";
        const CLOSE: &str = "EMIT ON WINDOW CLOSE";
        const LATE: &str = "EMIT ON WINDOW CLOSE ALLOW LATENESS INTERVAL '30' SECOND";
        let db = LaminarDB::open().unwrap();
        db.execute("CREATE SOURCE src (ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts)")
            .await
            .unwrap();
        for (view, select, group, emit, column, interval_ms) in [
            (
                "start",
                format!("{TUMBLE} AS w"),
                TUMBLE.to_owned(),
                CLOSE,
                0,
                60_000,
            ),
            (
                "late_start",
                format!("{TUMBLE} AS w"),
                TUMBLE.to_owned(),
                LATE,
                0,
                60_000,
            ),
            (
                "both",
                format!("{TUMBLE} AS ws, {TUMBLE_END} AS we"),
                format!("{TUMBLE}, {TUMBLE_END}"),
                CLOSE,
                0,
                70_000,
            ),
            (
                "end",
                format!("{TUMBLE_END} AS we"),
                format!("{TUMBLE}, {TUMBLE_END}"),
                CLOSE,
                0,
                60_000,
            ),
            (
                "hop",
                format!("{HOP} AS w"),
                HOP.to_owned(),
                "EMIT FINAL",
                0,
                60_000,
            ),
            (
                "unheld_start",
                format!("{TUMBLE} AS w"),
                TUMBLE.to_owned(),
                "",
                0,
                70_000,
            ),
            (
                "unheld_end",
                format!("{TUMBLE_END} AS we"),
                format!("{TUMBLE}, {TUMBLE_END}"),
                "",
                0,
                60_000,
            ),
            (
                "late_session",
                format!("{SESSION} AS w"),
                SESSION.to_owned(),
                LATE,
                0,
                95_000,
            ),
        ] {
            db.execute(&format!(
                "CREATE MATERIALIZED VIEW {view} AS SELECT {select}, COUNT(*) AS n FROM src \
                 GROUP BY {group} {emit} WITH (retention = '1 minute')"
            ))
            .await
            .unwrap_or_else(|error| panic!("{view}: {error}"));
            let mode = db.mv_store.read().storage_mode_for_test(view);
            assert!(
                matches!(
                    &mode,
                    Some(MvStorageMode::AppendLog { retention })
                        if *retention == ViewRetention::EventTime { column, interval_ms }
                ),
                "{view}: {mode:?}"
            );
        }
    }
}
