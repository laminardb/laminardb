//! Admission of event-time consumers, in every mode: an interval join or a window orders its
//! input by the one event-time column that input carries.

use laminar_sql::translator::{StreamJoinConfig, WindowOperatorConfig};

use crate::db::LaminarDB;
use crate::error::DbError;
use crate::exchange_plan::{ChangelogKind, EventTime, EventTimeOrigin};

impl LaminarDB {
    /// Certify that both interval-join inputs carry an event-time watermark on their time column.
    pub(super) async fn validate_interval_join_watermarks(
        &self,
        config: &StreamJoinConfig,
    ) -> Result<(), DbError> {
        for (side, input, time_column) in [
            (
                "left",
                config.left_table.as_str(),
                config.left_time_column.as_str(),
            ),
            (
                "right",
                config.right_table.as_str(),
                config.right_time_column.as_str(),
            ),
        ] {
            let carried = self.input_event_time(input).await?;
            if carried
                .as_ref()
                .is_some_and(|event_time| event_time.column == time_column)
            {
                continue;
            }
            let found = carried.map_or_else(
                || "carries no event-time column".to_string(),
                |event_time| format!("carries '{}'", event_time.column),
            );
            return Err(DbError::InvalidOperation(format!(
                "{side} interval join input '{input}' must carry an event-time watermark on '{time_column}' but {found}: a source that declares it, or an append-only stream that carries it (a projection that forwards it unchanged, an interval join, or a closed window)"
            )));
        }
        Ok(())
    }

    /// A window over a window stream closes on that stream's frontier, which is held back only
    /// far enough for the one boundary column the stream carries. Windowing on its other
    /// boundary, or over a stream that carries none, could close a window before its rows arrive.
    pub(super) async fn validate_window_input_event_time(
        &self,
        object_name: &str,
        query_sql: &str,
        window: Option<&WindowOperatorConfig>,
    ) -> Result<(), DbError> {
        let Some(window) = window else {
            return Ok(());
        };
        for input in crate::sql_analysis::extract_table_references(query_sql) {
            let carried = self.input_event_time(&input).await?;
            let windowed = self
                .connector_manager
                .lock()
                .streams()
                .get(&input)
                .is_some_and(|stream| stream.window_config.is_some())
                || carried.as_ref().is_some_and(|event_time| {
                    matches!(event_time.origin, EventTimeOrigin::Window(_))
                });
            if !windowed {
                continue;
            }
            match carried {
                Some(event_time) if event_time.column == window.time_column => {}
                Some(event_time) => {
                    return Err(DbError::InvalidOperation(format!(
                        "window '{object_name}' over window stream '{input}' must window on '{}', the event-time column '{input}' carries, not '{}'",
                        event_time.column, window.time_column
                    )));
                }
                None => {
                    return Err(DbError::InvalidOperation(format!(
                        "window '{object_name}' cannot window over '{input}': a session window, or a window that projects no TUMBLE or HOP boundary, carries no event-time column"
                    )));
                }
            }
        }
        Ok(())
    }

    /// The event time an input carries for a consumer that orders by it: a source's declared
    /// watermark, or the event time of an append-only stream.
    pub(super) async fn input_event_time(&self, input: &str) -> Result<Option<EventTime>, DbError> {
        if let Some(source) = self.catalog.get_source(input) {
            return Ok(crate::exchange_plan::source_event_time(&source));
        }
        let resolved = self.resolve_catalog_stream_outputs(None).await?;
        Ok(resolved
            .exchange_plan
            .stream(input)
            .filter(|stream| stream.output.changelog == ChangelogKind::AppendOnly)
            .and_then(|stream| stream.output.event_time.clone()))
    }
}
