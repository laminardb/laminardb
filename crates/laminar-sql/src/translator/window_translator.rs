//! Window operator configuration builder
//!
//! Translates parsed window functions and EMIT/late data clauses
//! into complete operator configurations.

use std::time::Duration;

use crate::parser::{
    EmitClause, EmitStrategy, LateDataClause, ParseError, WindowFunction, WindowRewriter,
};

/// Type of window operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowType {
    /// Fixed-size non-overlapping windows
    Tumbling,
    /// Fixed-size overlapping windows with slide
    Sliding,
    /// Dynamic windows based on activity gaps
    Session,
}

/// Complete configuration for instantiating a window operator.
///
/// This structure holds all the information needed to create and configure
/// a window operator in Ring 0.
///
/// # EMIT ON WINDOW CLOSE
///
/// When `emit_strategy` is `OnWindowClose` or `FinalOnly`, use [`validate()`](Self::validate)
/// to ensure the configuration is valid. These strategies require:
/// - A watermark definition on the source (timers are driven by watermark)
/// - A windowed aggregation context (non-windowed queries cannot use EOWC)
#[derive(Debug, Clone)]
pub struct WindowOperatorConfig {
    /// The type of window (tumbling, sliding, session)
    pub window_type: WindowType,
    /// The time column name used for windowing
    pub time_column: String,
    /// Window size (for tumbling and sliding)
    pub size: Duration,
    /// Slide interval for sliding windows
    pub slide: Option<Duration>,
    /// Gap interval for session windows
    pub gap: Option<Duration>,
    /// Maximum allowed lateness for late events
    pub allowed_lateness: Duration,
    /// Emit strategy (when to output results)
    pub emit_strategy: EmitStrategy,
    /// Side output name for late data (if configured)
    pub late_data_side_output: Option<String>,
}

impl WindowOperatorConfig {
    /// Create a new tumbling window configuration.
    #[must_use]
    pub fn tumbling(time_column: String, size: Duration) -> Self {
        Self {
            window_type: WindowType::Tumbling,
            time_column,
            size,
            slide: None,
            gap: None,
            allowed_lateness: Duration::ZERO,
            emit_strategy: EmitStrategy::OnWatermark,
            late_data_side_output: None,
        }
    }

    /// Create a new sliding window configuration.
    #[must_use]
    pub fn sliding(time_column: String, size: Duration, slide: Duration) -> Self {
        Self {
            window_type: WindowType::Sliding,
            time_column,
            size,
            slide: Some(slide),
            gap: None,
            allowed_lateness: Duration::ZERO,
            emit_strategy: EmitStrategy::OnWatermark,
            late_data_side_output: None,
        }
    }

    /// Create a new session window configuration.
    #[must_use]
    pub fn session(time_column: String, gap: Duration) -> Self {
        Self {
            window_type: WindowType::Session,
            time_column,
            size: Duration::ZERO, // Not used for session windows
            slide: None,
            gap: Some(gap),
            allowed_lateness: Duration::ZERO,
            emit_strategy: EmitStrategy::OnWatermark,
            late_data_side_output: None,
        }
    }

    /// Build configuration from a parsed `WindowFunction`.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if:
    /// - Time column cannot be extracted
    /// - Interval cannot be parsed
    pub fn from_window_function(window: &WindowFunction) -> Result<Self, ParseError> {
        let time_column = WindowRewriter::get_time_column_name(window).ok_or_else(|| {
            ParseError::WindowError("Cannot extract time column name".to_string())
        })?;

        match window {
            WindowFunction::Tumble { interval, .. } => {
                let size = WindowRewriter::parse_interval_to_duration(interval)?;
                Ok(Self::tumbling(time_column, size))
            }
            WindowFunction::Hop {
                slide_interval,
                window_interval,
                ..
            } => {
                let size = WindowRewriter::parse_interval_to_duration(window_interval)?;
                let slide = WindowRewriter::parse_interval_to_duration(slide_interval)?;
                Ok(Self::sliding(time_column, size, slide))
            }
            WindowFunction::Session { gap_interval, .. } => {
                let gap = WindowRewriter::parse_interval_to_duration(gap_interval)?;
                Ok(Self::session(time_column, gap))
            }
        }
    }

    /// Apply EMIT clause configuration.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if the emit clause cannot be converted.
    pub fn with_emit_clause(mut self, emit_clause: &EmitClause) -> Result<Self, ParseError> {
        self.emit_strategy = emit_clause.to_emit_strategy()?;
        Ok(self)
    }

    /// Apply late data clause configuration.
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if the allowed lateness cannot be parsed.
    pub fn with_late_data_clause(
        mut self,
        late_data_clause: &LateDataClause,
    ) -> Result<Self, ParseError> {
        self.allowed_lateness = late_data_clause.to_allowed_lateness()?;
        self.late_data_side_output
            .clone_from(&late_data_clause.side_output);
        Ok(self)
    }

    /// Set allowed lateness duration.
    #[must_use]
    pub fn with_allowed_lateness(mut self, lateness: Duration) -> Self {
        self.allowed_lateness = lateness;
        self
    }

    /// Set emit strategy.
    #[must_use]
    pub fn with_emit_strategy(mut self, strategy: EmitStrategy) -> Self {
        self.emit_strategy = strategy;
        self
    }

    /// Set late data side output.
    #[must_use]
    pub fn with_late_data_side_output(mut self, name: String) -> Self {
        self.late_data_side_output = Some(name);
        self
    }

    /// Validates that the window operator configuration is used in a valid context.
    ///
    /// Checks:
    /// - `EMIT ON WINDOW CLOSE` and `EMIT FINAL` require a watermark on the source
    /// - `EMIT ON WINDOW CLOSE` and `EMIT FINAL` require a windowed aggregation
    ///
    /// # Arguments
    ///
    /// * `has_watermark` - Whether the source has a watermark definition
    /// * `has_window` - Whether the query contains a windowed aggregation
    ///
    /// # Errors
    ///
    /// Returns `ParseError::WindowError` if validation fails.
    pub fn validate(&self, has_watermark: bool, has_window: bool) -> Result<(), ParseError> {
        if matches!(
            self.emit_strategy,
            EmitStrategy::OnWindowClose | EmitStrategy::FinalOnly
        ) {
            if !has_watermark {
                return Err(ParseError::WindowError(
                    "EMIT ON WINDOW CLOSE requires a watermark definition \
                     on the source. Add WATERMARK FOR <column> AS <expr> \
                     to the CREATE SOURCE statement."
                        .to_string(),
                ));
            }
            if !has_window {
                return Err(ParseError::WindowError(
                    "EMIT ON WINDOW CLOSE is only valid with windowed \
                     aggregation queries. Use EMIT ON UPDATE for \
                     non-windowed queries."
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Check if this configuration supports append-only output.
    ///
    /// Append-only sinks (Kafka, S3, Delta Lake) require emit strategies
    /// that don't produce retractions.
    #[must_use]
    pub fn is_append_only_compatible(&self) -> bool {
        matches!(
            self.emit_strategy,
            EmitStrategy::OnWatermark | EmitStrategy::OnWindowClose | EmitStrategy::FinalOnly
        )
    }

    /// Check if late data handling is configured.
    #[must_use]
    pub fn has_late_data_handling(&self) -> bool {
        self.allowed_lateness > Duration::ZERO || self.late_data_side_output.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{Expr, Ident};

    fn make_tumble_window() -> WindowFunction {
        // Create a simple tumble window for testing
        WindowFunction::Tumble {
            time_column: Box::new(Expr::Identifier(Ident::new("event_time"))),
            interval: Box::new(Expr::Identifier(Ident::new("5 MINUTE"))),
        }
    }

    #[test]
    fn test_tumbling_config() {
        let config =
            WindowOperatorConfig::tumbling("event_time".to_string(), Duration::from_secs(300));

        assert_eq!(config.window_type, WindowType::Tumbling);
        assert_eq!(config.time_column, "event_time");
        assert_eq!(config.size, Duration::from_secs(300));
        assert!(config.slide.is_none());
        assert!(config.gap.is_none());
    }

    #[test]
    fn test_sliding_config() {
        let config = WindowOperatorConfig::sliding(
            "ts".to_string(),
            Duration::from_secs(300),
            Duration::from_secs(60),
        );

        assert_eq!(config.window_type, WindowType::Sliding);
        assert_eq!(config.size, Duration::from_secs(300));
        assert_eq!(config.slide, Some(Duration::from_secs(60)));
    }

    #[test]
    fn test_session_config() {
        let config =
            WindowOperatorConfig::session("click_time".to_string(), Duration::from_secs(1800));

        assert_eq!(config.window_type, WindowType::Session);
        assert_eq!(config.gap, Some(Duration::from_secs(1800)));
    }

    #[test]
    fn test_from_window_function() {
        let window = make_tumble_window();
        let config = WindowOperatorConfig::from_window_function(&window).unwrap();

        assert_eq!(config.window_type, WindowType::Tumbling);
        assert_eq!(config.time_column, "event_time");
        assert_eq!(config.size, Duration::from_secs(300));
    }

    #[test]
    fn test_with_emit_clause() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

        let config = config.with_emit_clause(&EmitClause::OnWindowClose).unwrap();
        assert_eq!(config.emit_strategy, EmitStrategy::OnWindowClose);

        let config2 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));
        let config2 = config2.with_emit_clause(&EmitClause::Changes).unwrap();
        assert_eq!(config2.emit_strategy, EmitStrategy::Changelog);
    }

    #[test]
    fn test_with_late_data_clause() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

        let late_clause = LateDataClause::side_output_only("late_events".to_string());
        let config = config.with_late_data_clause(&late_clause).unwrap();

        assert_eq!(
            config.late_data_side_output,
            Some("late_events".to_string())
        );
    }

    #[test]
    fn test_append_only_compatible() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

        // Default emit strategy (OnWatermark) is append-only compatible
        assert!(config.is_append_only_compatible());

        // OnUpdate is NOT append-only compatible (produces retractions)
        let config2 = config.with_emit_strategy(EmitStrategy::OnUpdate);
        assert!(!config2.is_append_only_compatible());

        // Changelog is NOT append-only compatible
        let config3 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::Changelog);
        assert!(!config3.is_append_only_compatible());
    }

    #[test]
    fn test_has_late_data_handling() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300));

        // No late data handling by default
        assert!(!config.has_late_data_handling());

        // With allowed lateness
        let config2 = config
            .clone()
            .with_allowed_lateness(Duration::from_secs(60));
        assert!(config2.has_late_data_handling());

        // With side output
        let config3 = config.with_late_data_side_output("late".to_string());
        assert!(config3.has_late_data_handling());
    }

    #[test]
    fn test_eowc_without_watermark_errors() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::OnWindowClose);

        let result = config.validate(false, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("watermark"),
            "Expected watermark error, got: {err}"
        );
    }

    #[test]
    fn test_eowc_without_window_errors() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::OnWindowClose);

        let result = config.validate(true, false);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("windowed"),
            "Expected windowed query error, got: {err}"
        );
    }

    #[test]
    fn test_eowc_with_watermark_and_window_passes() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::OnWindowClose);

        assert!(config.validate(true, true).is_ok());
    }

    #[test]
    fn test_final_without_watermark_errors() {
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::FinalOnly);

        let result = config.validate(false, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_eowc_without_watermark_ok() {
        // OnUpdate does not require watermark
        let config = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::OnUpdate);
        assert!(config.validate(false, false).is_ok());

        // Periodic does not require watermark
        let config2 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::Periodic(Duration::from_secs(5)));
        assert!(config2.validate(false, false).is_ok());

        // Changelog does not require watermark
        let config3 = WindowOperatorConfig::tumbling("ts".to_string(), Duration::from_secs(300))
            .with_emit_strategy(EmitStrategy::Changelog);
        assert!(config3.validate(false, false).is_ok());
    }
}
