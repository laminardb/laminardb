//! Attaching a local `SUBSCRIBE` portal to a stream's or a materialized view's shared log.

use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::physical_expr::PhysicalExpr;

use super::LaminarDB;
use crate::error::DbError;
use crate::subscription::{
    SubscribeStart, SubscriptionOpenError, SubscriptionPortal, SubscriptionReader,
};

impl LaminarDB {
    /// Attach to stream `name`'s log at `start`.
    pub(super) fn open_local_stream_subscription(
        &self,
        name: &str,
        schema: SchemaRef,
        filter: Option<Arc<dyn PhysicalExpr>>,
        start: SubscribeStart,
    ) -> Result<SubscriptionPortal, DbError> {
        let reader = self.attach_reader(name, start)?;
        Ok(match filter {
            Some(filter) => SubscriptionPortal::open_with_filter(name, schema, reader, filter),
            None => SubscriptionPortal::open(name, schema, reader),
        })
    }

    /// Attach to view `name`: its rows at the latest completed cycle, then each cycle's change
    /// to them. The rows and the cursor are taken under one store read guard, and a cycle
    /// publishes its change under the write guard, so each change is in the snapshot or after
    /// the cursor, never both.
    pub(super) async fn open_local_view_subscription(
        &self,
        name: &str,
        schema: SchemaRef,
        filter: Option<Arc<dyn PhysicalExpr>>,
        start: SubscribeStart,
    ) -> Result<SubscriptionPortal, DbError> {
        if !matches!(start, SubscribeStart::Tail) {
            return Err(DbError::Unsupported(format!(
                "a subscription to materialized view '{name}' starts at the view's current \
                 rows; subscribe from the tail"
            )));
        }
        let (mut rows, reader) = crate::mv_store::read_captured(&self.mv_store, name, |store| {
            let read = store
                .subscription_snapshot(name)?
                .ok_or_else(|| DbError::StreamNotFound(name.to_owned()))?;
            let reader = self.attach_reader(name, start)?;
            Ok((vec![(name.to_owned(), read)], reader))
        })
        .await?;
        let snapshot = rows
            .remove(name)
            .ok_or_else(|| DbError::StreamNotFound(name.to_owned()))?;
        Ok(SubscriptionPortal::open_view(
            name, schema, snapshot, reader, filter,
        ))
    }

    fn attach_reader(
        &self,
        name: &str,
        start: SubscribeStart,
    ) -> Result<SubscriptionReader, DbError> {
        self.subscription_registry
            .subscribe(name, start)
            .map_err(|error| open_error(name, start, error))
    }
}

fn open_error(name: &str, start: SubscribeStart, error: SubscriptionOpenError) -> DbError {
    match error {
        SubscriptionOpenError::ReplayPruned { earliest_retained } => {
            let requested = match start {
                SubscribeStart::AsOfEpoch(n) => n,
                SubscribeStart::AfterSequence(_) | SubscribeStart::Tail => 0,
            };
            DbError::SubscriptionReplayPruned {
                name: name.to_string(),
                requested,
                earliest_retained,
            }
        }
        SubscriptionOpenError::SequencePruned {
            requested,
            earliest_retained,
        } => DbError::SubscriptionSequencePruned {
            name: name.to_string(),
            requested_sequence: requested,
            earliest_retained_sequence: earliest_retained,
        },
        SubscriptionOpenError::SequenceNotPublished {
            requested,
            next_sequence,
        } => DbError::SubscriptionSequenceNotPublished {
            name: name.to_string(),
            requested_sequence: requested,
            next_sequence,
        },
        SubscriptionOpenError::EpochNotCommitted {
            requested,
            latest_committed,
        } => DbError::SubscriptionEpochNotCommitted {
            name: name.to_string(),
            requested,
            latest_committed,
        },
        SubscriptionOpenError::Capacity { attached, limit } => DbError::Pipeline(format!(
            "subscriber cap reached for '{name}' ({attached}/{limit})"
        )),
    }
}
