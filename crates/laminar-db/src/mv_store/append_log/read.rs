//! Reads an append-only view at the last completed cycle: its committed segments, from the
//! checkpoint the store last committed and pinned for the read, then the rows held in memory,
//! filtered by the view's retention.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use super::{SegmentMeta, ViewRetention};
use crate::error::DbError;
use crate::mv_store::committed::CommittedViews;

/// Why a log read could not finish.
#[derive(Debug)]
pub(crate) enum LogReadError {
    /// Retention moved past the checkpoint the read captured before the read could pin it;
    /// capture the view again.
    Retired,
    Failed(DbError),
}

impl From<DbError> for LogReadError {
    fn from(error: DbError) -> Self {
        Self::Failed(error)
    }
}

/// Everything one read of an append-only view needs, captured under the store read guard.
pub(crate) struct LogRead {
    pub(in crate::mv_store) view: String,
    pub(in crate::mv_store) schema: SchemaRef,
    pub(in crate::mv_store) retention: ViewRetention,
    pub(in crate::mv_store) frontier: Option<i64>,
    pub(in crate::mv_store) committed: Option<(Arc<CommittedViews>, u64)>,
    pub(in crate::mv_store) segments: Vec<SegmentMeta>,
    pub(in crate::mv_store) memory: Vec<RecordBatch>,
}

impl LogRead {
    /// Read the view's committed segments and memory rows, in log order.
    pub(crate) async fn materialize(self, deadline: Instant) -> Result<RecordBatch, LogReadError> {
        let mut batches = match &self.committed {
            Some((views, checkpoint_id)) if !self.segments.is_empty() => {
                let cut = views
                    .pin_checkpoint(*checkpoint_id, deadline)
                    .await?
                    .ok_or(LogReadError::Retired)?;
                cut.local_segments(views, &self.view, &self.schema, &self.segments)
                    .await?
            }
            _ => Vec::new(),
        };
        batches.extend(self.memory);
        let batches = self.retention.apply(batches, self.frontier)?;
        arrow::compute::concat_batches(&self.schema, &batches).map_err(|error| {
            LogReadError::Failed(DbError::Storage(format!("MV batch concat: {error}")))
        })
    }
}
