//! A cluster subscription to a materialized view, read from committed checkpoints as a committed
//! read is: the view's rows at one cut, then its change from each committed cut to the next,
//! each closed by a progress frame. The reader holds the cut it last delivered against retention,
//! because the next change is computed against that cut's artifacts. When this node's pipeline
//! restarts, as a topology migration or a coordinated recovery round does, the reader waits for
//! the restarted pipeline's reads, holds that cut through them and continues.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Semaphore};

use super::{
    record_terminal_error, ActiveReaderGuard, ClusterReaderFrame, ClusterSubscriptionReader,
    GATEWAY_SEND_TIMEOUT, MAX_GATEWAY_QUEUE_BYTES, MAX_GATEWAY_QUEUE_FRAMES,
};
use crate::engine_metrics::EngineMetrics;
use crate::error::DbError;
use crate::mv_store::{
    CommittedRead, CommittedReadError, CommittedViews, MvStore, PinnedCut, ViewShape,
};
use crate::subscription::{ClusterSubscriptionError, SubscribeStart};

/// How long the reader holds a cut against retention before renewing its hold.
const PIN_DEADLINE: Duration = Duration::from_secs(30);
/// How often an idle reader renews its hold and looks for a newer cut.
const RENEW_INTERVAL: Duration = Duration::from_secs(5);
/// How often a reader whose pipeline restarted looks for the restarted pipeline's reads.
const RESTART_POLL: Duration = Duration::from_millis(50);

impl ClusterSubscriptionReader {
    /// Read view `shape` from the committed cut `start` names: the newest for the tail, or
    /// exactly the given epoch.
    pub(crate) async fn open_view(
        store: Arc<parking_lot::RwLock<MvStore>>,
        shape: ViewShape,
        start: SubscribeStart,
        metrics: Option<Arc<EngineMetrics>>,
    ) -> Result<Self, DbError> {
        let views = store
            .read()
            .committed_views()
            .ok_or(ClusterSubscriptionError::BackendUnavailable)?;
        let (cut, snapshot) = attach(&views, &shape, start).await?;
        tracing::info!(
            view = shape.name(),
            epoch = cut.epoch,
            "opened committed cluster view subscription"
        );
        let (sender, receiver) = mpsc::channel(MAX_GATEWAY_QUEUE_FRAMES);
        let (cancel, cancel_rx) = tokio::sync::oneshot::channel();
        let terminal = Arc::new(Mutex::new(None));
        let task_terminal = Arc::clone(&terminal);
        let active_reader = ActiveReaderGuard::new(metrics.clone(), shape.name().to_owned());
        let task = tokio::spawn(async move {
            let _active_reader = active_reader;
            let result = tokio::select! {
                result = follow(&store, views, &shape, (cut, snapshot), &sender) => result,
                _ = cancel_rx => Ok(()),
            };
            if let Err(error) = result {
                record_terminal_error(metrics.as_deref(), &error);
                tracing::warn!(
                    view = shape.name(),
                    error_code = error.code(),
                    error = %error,
                    "committed cluster view subscription terminated"
                );
                *task_terminal.lock() = Some(error);
            }
        });
        Ok(Self {
            receiver,
            terminal,
            cancel: Some(cancel),
            _task: task,
        })
    }
}

/// Pin the cut `start` names and read the view's rows there. The tail waits for a cut that holds
/// the view, since a view a topology migration just added is in no cut until the migration's
/// first checkpoint commits.
async fn attach(
    views: &CommittedViews,
    shape: &ViewShape,
    start: SubscribeStart,
) -> Result<(PinnedCut, Vec<RecordBatch>), DbError> {
    let deadline = Instant::now() + PIN_DEADLINE;
    let mut at = match start {
        SubscribeStart::Tail => CommittedRead::AtLeast(1),
        SubscribeStart::AsOfEpoch(epoch) => CommittedRead::Exact(epoch),
        SubscribeStart::AfterSequence(_) => return Err(super::unsupported_sequence_start()),
    };
    loop {
        let cut = views.pin(at, deadline).await.map_err(open_error)?;
        match cut.subscription_rows(views, shape).await {
            Ok(rows) => return Ok((cut, rows)),
            Err(DbError::CommittedRead(CommittedReadError::ViewNotCommitted { .. }))
                if matches!(start, SubscribeStart::Tail) =>
            {
                at = CommittedRead::AtLeast(cut.epoch + 1);
            }
            Err(error) => return Err(open_error(error)),
        }
    }
}

fn open_error(error: DbError) -> DbError {
    let DbError::CommittedRead(error) = error else {
        return super::authority::into_subscription_error(error).into();
    };
    match error {
        CommittedReadError::NotCommitted {
            requested: Some(requested),
            ..
        } => ClusterSubscriptionError::EpochNotCommitted { requested },
        CommittedReadError::Retired { requested } => {
            ClusterSubscriptionError::ReplayPruned { requested }
        }
        CommittedReadError::ViewNotCommitted { .. } => ClusterSubscriptionError::GenerationMismatch,
        CommittedReadError::NotCommitted {
            requested: None, ..
        }
        | CommittedReadError::CheckpointingDisabled
        | CommittedReadError::PinCapacity
        | CommittedReadError::DeadlineExceeded { .. }
        | CommittedReadError::LiveTable { .. } => ClusterSubscriptionError::BackendUnavailable,
    }
    .into()
}

/// Deliver the snapshot, then each later cut's change, until the portal closes.
async fn follow(
    store: &parking_lot::RwLock<MvStore>,
    mut views: Arc<CommittedViews>,
    shape: &ViewShape,
    (mut cut, snapshot): (PinnedCut, Vec<RecordBatch>),
    sender: &mpsc::Sender<ClusterReaderFrame>,
) -> Result<(), ClusterSubscriptionError> {
    let mut delivery = Delivery {
        sender,
        budget: Arc::new(Semaphore::new(MAX_GATEWAY_QUEUE_BYTES)),
        sequence: 0,
    };
    // A slow client may hold each frame for most of the send timeout, so every delivered frame
    // renews the hold on the cut the next change is computed against.
    for batch in snapshot {
        delivery.rows(batch, cut.epoch, true).await?;
        cut.renew(&views, Instant::now() + PIN_DEADLINE)
            .map_err(follow_error)?;
    }
    delivery.progress(cut.epoch, cut.checkpoint_id).await?;
    let mut cuts = views.cuts();
    loop {
        if sender.is_closed() {
            return Ok(());
        }
        let successors = views.successors(&cut).await.map_err(follow_error)?;
        if successors.is_empty() {
            cut.renew(&views, Instant::now() + PIN_DEADLINE)
                .map_err(follow_error)?;
            if let Ok(Err(_)) = tokio::time::timeout(RENEW_INTERVAL, cuts.changed()).await {
                views = tokio::select! {
                    views = restarted(store, &views, &mut cut) => views?,
                    () = sender.closed() => return Ok(()),
                };
                cuts = views.cuts();
            }
            continue;
        }
        for reference in successors {
            cut.renew(&views, Instant::now() + PIN_DEADLINE)
                .map_err(follow_error)?;
            let mut next = views
                .pin_reference(&reference, Instant::now() + PIN_DEADLINE)
                .await
                .map_err(follow_error)?;
            for batch in next
                .changes_since(&cut, &views, shape)
                .await
                .map_err(follow_error)?
            {
                delivery.rows(batch, next.epoch, false).await?;
                next.renew(&views, Instant::now() + PIN_DEADLINE)
                    .map_err(follow_error)?;
            }
            delivery.progress(next.epoch, next.checkpoint_id).await?;
            cut = next;
        }
    }
}

/// The restarted pipeline's committed reads, once it publishes them, with `cut` held through
/// them. The restart may take as long as a coordinated recovery round, so the caller ends the
/// wait only when the subscriber goes away. A cut the restarted pipeline still reports retired
/// `PIN_DEADLINE` after it appeared ends the subscription.
async fn restarted(
    store: &parking_lot::RwLock<MvStore>,
    stopped: &Arc<CommittedViews>,
    cut: &mut PinnedCut,
) -> Result<Arc<CommittedViews>, ClusterSubscriptionError> {
    let mut retired_since = None;
    loop {
        let current = store.read().committed_views();
        if let Some(views) = current.filter(|views| !Arc::ptr_eq(views, stopped)) {
            match cut.renew(&views, Instant::now() + PIN_DEADLINE) {
                Ok(()) => return Ok(views),
                Err(DbError::CommittedRead(CommittedReadError::Retired { .. }))
                    if retired_since.get_or_insert_with(Instant::now).elapsed() < PIN_DEADLINE => {}
                Err(error) => return Err(follow_error(error)),
            }
        }
        tokio::time::sleep(RESTART_POLL).await;
    }
}

fn follow_error(error: DbError) -> ClusterSubscriptionError {
    match error {
        DbError::CommittedRead(CommittedReadError::ViewNotCommitted { .. }) => {
            ClusterSubscriptionError::GenerationMismatch
        }
        DbError::CommittedRead(CommittedReadError::Retired { .. }) => {
            ClusterSubscriptionError::RetentionLost
        }
        DbError::CommittedRead(_) => ClusterSubscriptionError::BackendUnavailable,
        other => super::authority::into_subscription_error(other),
    }
}

/// Frames toward the portal, bounded by the gateway queue budget.
struct Delivery<'a> {
    sender: &'a mpsc::Sender<ClusterReaderFrame>,
    budget: Arc<Semaphore>,
    sequence: u64,
}

impl Delivery<'_> {
    async fn rows(
        &mut self,
        batch: RecordBatch,
        epoch: u64,
        snapshot: bool,
    ) -> Result<(), ClusterSubscriptionError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        // A batch larger than the whole budget is delivered alone.
        let bytes = batch
            .get_array_memory_size()
            .clamp(1, MAX_GATEWAY_QUEUE_BYTES);
        let permits =
            u32::try_from(bytes).map_err(|_| ClusterSubscriptionError::SubscriberLagged)?;
        let permit = tokio::time::timeout(
            GATEWAY_SEND_TIMEOUT,
            Arc::clone(&self.budget).acquire_many_owned(permits),
        )
        .await
        .map_err(|_| ClusterSubscriptionError::SubscriberLagged)?
        .map_err(|_| ClusterSubscriptionError::BackendUnavailable)?;
        let frame = ClusterReaderFrame::ViewRows {
            batch,
            delivery_sequence: self.next_sequence()?,
            epoch,
            snapshot,
            permit: Arc::new(permit),
        };
        self.send(frame).await
    }

    async fn progress(
        &mut self,
        epoch: u64,
        checkpoint_id: u64,
    ) -> Result<(), ClusterSubscriptionError> {
        let frame = ClusterReaderFrame::ViewProgress {
            delivery_sequence: self.next_sequence()?,
            epoch,
            checkpoint_id,
        };
        self.send(frame).await
    }

    fn next_sequence(&mut self) -> Result<u64, ClusterSubscriptionError> {
        let sequence = self.sequence;
        self.sequence =
            sequence
                .checked_add(1)
                .ok_or_else(|| ClusterSubscriptionError::ManifestCorrupt {
                    reason: "view subscription delivery sequence overflowed".into(),
                })?;
        Ok(sequence)
    }

    async fn send(&self, frame: ClusterReaderFrame) -> Result<(), ClusterSubscriptionError> {
        tokio::time::timeout(GATEWAY_SEND_TIMEOUT, self.sender.send(frame))
            .await
            .map_err(|_| ClusterSubscriptionError::SubscriberLagged)?
            .map_err(|_| ClusterSubscriptionError::BackendUnavailable)
    }
}
