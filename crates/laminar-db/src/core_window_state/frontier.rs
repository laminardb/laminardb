//! Window sizes for closing, and how far a window's output frontier trails its close frontier.

use super::{CoreWindowAssigner, CoreWindowState};
use crate::exchange_plan::WindowBoundary;

impl CoreWindowState {
    /// The size of a tumbling or hopping window; a session has none.
    #[inline]
    pub(super) fn fixed_size_ms(&self) -> Option<i64> {
        match &self.assigner {
            CoreWindowAssigner::Tumbling(assigner) => Some(assigner.size_ms()),
            CoreWindowAssigner::Hopping(assigner) => Some(assigner.size_ms()),
            CoreWindowAssigner::Session { .. } => None,
        }
    }

    /// How far the output frontier trails the close frontier `F`, so that no row this window has
    /// yet to emit is behind it: a window still open at `F` ends after `F - lateness` and starts
    /// after `F - lateness - size`. A session's start trails its end without bound, so a session
    /// output carries no event time and its frontier is not held.
    pub(crate) fn output_frontier_hold_ms(&self, boundary: WindowBoundary) -> i64 {
        let Some(size_ms) = self.fixed_size_ms() else {
            return 0;
        };
        match boundary {
            WindowBoundary::End => self.allowed_lateness_ms,
            WindowBoundary::Start => self.allowed_lateness_ms.saturating_add(size_ms),
        }
    }
}
