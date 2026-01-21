//! Query planner for streaming SQL

// use datafusion::logical_expr::LogicalPlan;
type LogicalPlan = (); // Temporary placeholder

/// Streaming query planner
pub struct StreamingPlanner {
    // TODO: Add planner state
}

impl StreamingPlanner {
    /// Creates a new streaming planner
    pub fn new() -> Self {
        Self {}
    }

    /// Plans a streaming query
    pub fn plan(&self, _statement: &crate::StreamingStatement) -> Result<LogicalPlan, PlanningError> {
        // TODO: Implement query planning
        todo!("Implement streaming query planning (F006)")
    }
}

impl Default for StreamingPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Planning errors
#[derive(Debug, thiserror::Error)]
pub enum PlanningError {
    /// Unsupported SQL feature
    #[error("Unsupported SQL: {0}")]
    UnsupportedSql(String),

    /// Invalid query
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
}