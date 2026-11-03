//! The frontier a window passes downstream.

use datafusion::prelude::SessionContext;

use super::EowcQueryOperator;
use crate::core_window_state::CoreWindowState;
use crate::error::DbError;
use crate::operator_graph::InputFrontier;

impl EowcQueryOperator {
    /// The hold for `window`, from the boundary column `sql` projects.
    pub(super) async fn plan_output_hold_ms(
        ctx: &SessionContext,
        sql: &str,
        window: &CoreWindowState,
    ) -> Result<i64, DbError> {
        let plan = ctx.sql(sql).await?.into_unoptimized_plan();
        let boundary = crate::exchange_plan::carried_window_boundary(&plan);
        Ok(window.output_frontier_hold_ms(boundary))
    }

    /// `frontier` held back so that no window this operator has yet to emit is behind it. The
    /// graph never lowers a published watermark, so none is published before initialization.
    pub(super) fn held_output_frontier(&self, frontier: InputFrontier) -> InputFrontier {
        InputFrontier {
            watermark: self.output_hold_ms.and_then(|hold| {
                frontier
                    .watermark
                    .map(|watermark| watermark.saturating_sub(hold))
            }),
            idle: frontier.idle,
        }
    }

    /// The frontier the cluster merge closed windows at, which stays active while peer input
    /// is still queued.
    #[cfg(feature = "cluster")]
    pub(super) fn merged_output_frontier(&self) -> Option<InputFrontier> {
        self.cluster_scope.as_ref()?;
        let mut frontier = self.effective_frontier;
        if self.pending_cluster_input.is_some() || self.queued_remote_events != 0 {
            frontier.idle = false;
        }
        Some(frontier)
    }
}
