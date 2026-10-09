//! Bounded change-stream reading, snapshot bootstrap, retry, and cancellation ownership.

use std::sync::Arc;

use mongodb::bson::{RawDocumentBuf, Timestamp};
use mongodb::change_stream::event::ResumeToken;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::super::change_event::ChangeOperation;
use super::buffering::{buffered_retained_bytes, BufferedMongoPayload, ChangeRecord};
use super::checkpoint::{canonical_resume_token, MongoCheckpointPosition, SnapshotCut};
use super::decoding::{event_operation, event_token};
use super::{
    observe_mongodb_admission, BufferedMongoEvent, ChangeStreamTx, ConnectorError,
    MongoAdmissionObservation, MongoCollectionObservation, MongoDbCdcMetrics, MongoDbSourceConfig,
    MongoDeploymentIdentity, MongoReaderFailure, MongoReaderReady, CURSOR_MAX_AWAIT_TIME,
    MAX_MONGODB_WIRE_EVENT_BYTES,
};

mod failure;
mod reconnect;
mod snapshot;

use failure::{classify_stream_error, ReadFailure};
use reconnect::{
    open_verified_cursor, prepare_reader_admission, ReaderAdmission, ReconnectControl,
};

/// Maximum consecutive failures before the reader gives up.
const MAX_FAILURES: u32 = 10;

pub(super) const READER_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

pub(super) type MongoChangeStream = mongodb::change_stream::ChangeStream<RawDocumentBuf>;
pub(super) type ReadyTx =
    Option<tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>>;

/// Driver resume option for the next cursor open.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum MongoResumePosition {
    ResumeAfter(ResumeToken),
    StartAfter(ResumeToken),
    /// Inclusive: every event at or after this cluster time.
    StartAt(Timestamp),
}

/// Where a reader generation begins.
#[derive(Clone, Debug)]
pub(super) enum ReaderStart {
    /// Changes only, from the exclusive post-batch token of an empty initial open.
    FreshChanges,
    /// Copy the collection at a new snapshot time, then stream from that time.
    FreshSnapshot,
    /// Continue a durable snapshot scan, then stream from its snapshot time.
    Snapshot(SnapshotCut),
    /// Continue streaming from a stored position.
    Stream(MongoResumePosition),
}

/// The reader's half of the bounded reader-to-poll queue.
pub(super) struct ReaderOutput {
    pub(super) tx: ChangeStreamTx,
    pub(super) data_ready: Arc<Notify>,
    pub(super) metrics: Arc<MongoDbCdcMetrics>,
    pub(super) byte_budget: Arc<Semaphore>,
    pub(super) max_buffered_bytes: usize,
}

impl ReaderOutput {
    /// Charge one item to the shared byte budget and enqueue it. `Ok(false)` means shutdown.
    pub(super) async fn send(
        &self,
        payload: BufferedMongoPayload,
        shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    ) -> Result<bool, ConnectorError> {
        let retained_bytes = buffered_retained_bytes(&payload)?;
        let Some(permit) = acquire_mongo_byte_permit(
            retained_bytes,
            &self.byte_budget,
            self.max_buffered_bytes,
            shutdown_rx,
        )
        .await?
        else {
            return Ok(false);
        };
        if !send_event_or_shutdown(
            &self.tx,
            BufferedMongoEvent::new(payload, permit),
            shutdown_rx,
        )
        .await
        {
            return Ok(false);
        }
        self.data_ready.notify_one();
        Ok(true)
    }
}

pub(super) enum ChangeStreamRead {
    Stop,
    Reconnect,
}

pub(super) fn reader_stopping(shutdown_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow() || shutdown_rx.has_changed().is_err()
}

pub(super) fn namespace(config: &MongoDbSourceConfig) -> String {
    format!("{}.{}", config.database, config.collection)
}

fn publish_reader_ready(
    ready_tx: &mut ReadyTx,
    initial_position: &mut Option<MongoCheckpointPosition>,
    admission: &ReaderAdmission,
) {
    if let Some(ready_tx) = ready_tx.take() {
        let _ = ready_tx.send(Ok(MongoReaderReady {
            initial_position: initial_position.take(),
            collection_uuid: admission.collection_uuid,
            deployment_identity: admission.deployment_identity.clone(),
        }));
    }
}

pub(super) fn report_mongo_reader_admission_error(ready_tx: &mut ReadyTx, error: &ConnectorError) {
    if let Some(ready_tx) = ready_tx.take() {
        let _ = ready_tx.send(Err(MongoReaderFailure::from_connector(error)));
    }
}

pub(super) fn change_stream_options(
    config: &MongoDbSourceConfig,
    position: Option<&MongoResumePosition>,
) -> mongodb::options::ChangeStreamOptions {
    let mut options = mongodb::options::ChangeStreamOptions::default();
    options.full_document = match config.full_document_mode {
        super::super::config::FullDocumentMode::Delta => None,
        super::super::config::FullDocumentMode::RequirePostImage => {
            Some(mongodb::options::FullDocumentType::Required)
        }
    };
    options.max_await_time = Some(CURSOR_MAX_AWAIT_TIME);
    options.batch_size = Some(config.cursor_batch_size());
    options.show_expanded_events = Some(true);
    match position {
        Some(MongoResumePosition::ResumeAfter(token)) => options.resume_after = Some(token.clone()),
        Some(MongoResumePosition::StartAfter(token)) => options.start_after = Some(token.clone()),
        Some(MongoResumePosition::StartAt(at)) => options.start_at_operation_time = Some(*at),
        None => {}
    }
    options
}

pub(super) fn bootstrap_change_stream_options(
    config: &MongoDbSourceConfig,
    position: Option<&MongoResumePosition>,
) -> mongodb::options::ChangeStreamOptions {
    let mut options = change_stream_options(config, position);
    // MongoDB guarantees an empty firstBatch for batchSize=0, so its PBRT is an exact opening
    // cut and cannot skip concurrently buffered events.
    options.batch_size = Some(0);
    options
}

/// Forward one opened cursor until shutdown, invalidation, or a reconnectable failure.
#[allow(clippy::too_many_lines)] // PERF: one getMore/forward kernel; splitting fragments its state.
pub(super) async fn forward_change_stream(
    cursor: &mut MongoChangeStream,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    resume_position: &mut Option<MongoResumePosition>,
    output: &ReaderOutput,
    consecutive_failures: &mut u32,
    namespace: &str,
) -> Result<ChangeStreamRead, ConnectorError> {
    loop {
        if reader_stopping(shutdown_rx) {
            tracing::info!("change stream reader shutting down");
            return Ok(ChangeStreamRead::Stop);
        }

        // Poll getMore to completion during normal operation; maxAwaitTime keeps cooperative
        // shutdown prompt. The connector aborts and joins the owned task at its hard deadline.
        let next = cursor.next_if_any().await;
        if reader_stopping(shutdown_rx) {
            tracing::info!("change stream reader shutting down after completed getMore");
            return Ok(ChangeStreamRead::Stop);
        }

        match next {
            Ok(Some(raw)) => {
                *consecutive_failures = 0;
                let wire_bytes = raw.as_bytes().len();
                if wire_bytes > MAX_MONGODB_WIRE_EVENT_BYTES {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "MongoDB CDC event exceeds the supported unsplit BSON bound: \
                         event={wire_bytes}, limit={MAX_MONGODB_WIRE_EVENT_BYTES}"
                    )));
                }
                output
                    .metrics
                    .record_bytes(u64::try_from(wire_bytes).unwrap_or(u64::MAX));
                let operation = event_operation(&raw)?;
                let token = canonical_resume_token(&event_token(&raw)?)?;
                let invalidated = operation == ChangeOperation::Invalidate;
                let event_resume_token: ResumeToken = serde_json::from_str(&token)
                    .map_err(|error| ConnectorError::ReadError(format!("resume token: {error}")))?;
                let record = ChangeRecord {
                    raw,
                    token,
                    operation,
                };
                if !output
                    .send(BufferedMongoPayload::Change(record), shutdown_rx)
                    .await?
                {
                    return Ok(ChangeStreamRead::Stop);
                }
                *resume_position = Some(if invalidated {
                    MongoResumePosition::StartAfter(event_resume_token)
                } else {
                    MongoResumePosition::ResumeAfter(
                        cursor.resume_token().unwrap_or(event_resume_token),
                    )
                });
                if invalidated {
                    return Ok(ChangeStreamRead::Reconnect);
                }
            }
            Ok(None) => {
                let cursor_alive = cursor.is_alive();
                if !matches!(
                    resume_position.as_ref(),
                    Some(MongoResumePosition::StartAfter(_))
                ) {
                    if let Some(token) = cursor.resume_token() {
                        let requires_start_after = !cursor_alive;
                        let changed = match resume_position.as_ref() {
                            Some(MongoResumePosition::ResumeAfter(current)) => {
                                requires_start_after || current != &token
                            }
                            Some(
                                MongoResumePosition::StartAfter(_)
                                | MongoResumePosition::StartAt(_),
                            )
                            | None => true,
                        };
                        if changed {
                            let marker = BufferedMongoPayload::HighWatermark {
                                token: canonical_post_batch_token(&token)?,
                                requires_start_after,
                            };
                            if !output.send(marker, shutdown_rx).await? {
                                return Ok(ChangeStreamRead::Stop);
                            }
                        }
                        *resume_position = Some(if requires_start_after {
                            MongoResumePosition::StartAfter(token)
                        } else {
                            MongoResumePosition::ResumeAfter(token)
                        });
                    }
                }
                if !cursor_alive {
                    tracing::info!("change stream cursor exhausted");
                    return Ok(ChangeStreamRead::Reconnect);
                }
                *consecutive_failures = 0;
            }
            Err(error) => match classify_stream_error(&error, namespace) {
                ReadFailure::Transient(message) => {
                    tracing::warn!(error = %message, "change stream read failed; reconnecting");
                    return Ok(ChangeStreamRead::Reconnect);
                }
                ReadFailure::Permanent(error) => return Err(error),
            },
        }
    }
}

fn canonical_post_batch_token(token: &ResumeToken) -> Result<String, ConnectorError> {
    let encoded = serde_json::to_string(token).map_err(|error| {
        ConnectorError::ReadError(format!(
            "serialize MongoDB post-batch resume token: {error}"
        ))
    })?;
    canonical_resume_token(&encoded).map_err(|error| {
        ConnectorError::ReadError(format!("invalid MongoDB post-batch resume token: {error}"))
    })
}

pub(super) async fn send_event_or_shutdown(
    tx: &ChangeStreamTx,
    event: BufferedMongoEvent,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    if reader_stopping(shutdown_rx) {
        return false;
    }

    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => false,
        result = tx.send(event) => {
            if result.is_err() {
                tracing::warn!("source channel closed, stopping reader");
            }
            result.is_ok()
        }
    }
}

pub(super) async fn acquire_mongo_byte_permit(
    retained_bytes: usize,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<OwnedSemaphorePermit>, ConnectorError> {
    if reader_stopping(shutdown_rx) {
        return Ok(None);
    }
    let too_large = || {
        ConnectorError::ConfigurationError(format!(
            "MongoDB CDC item exceeds the hard byte bound: item={retained_bytes}, \
             limit={max_buffered_bytes}; raise max.buffered.bytes"
        ))
    };
    if retained_bytes > max_buffered_bytes {
        return Err(too_large());
    }
    let permits = u32::try_from(retained_bytes).map_err(|_| too_large())?;
    let byte_permit = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return Ok(None),
        permit = Arc::clone(byte_budget).acquire_many_owned(permits) => permit.map_err(|_| {
            ConnectorError::ReadError("MongoDB CDC byte budget closed".into())
        })?,
    };
    Ok(Some(byte_permit))
}

pub(super) async fn retry_interrupted(
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    delay: std::time::Duration,
) -> bool {
    tokio::select! {
        changed = shutdown_rx.changed() => changed.is_err() || *shutdown_rx.borrow(),
        () = tokio::time::sleep(delay) => false,
    }
}

pub(super) fn parse_change_stream_pipeline(
    pipeline: &[serde_json::Value],
) -> Result<Vec<mongodb::bson::Document>, ConnectorError> {
    pipeline
        .iter()
        .enumerate()
        .map(|(index, value)| {
            mongodb::bson::to_document(value).map_err(|error| {
                ConnectorError::ConfigurationError(format!(
                    "pipeline stage {index} cannot be represented as BSON: {error}"
                ))
            })
        })
        .collect()
}

pub(super) fn verify_mongodb_collection_uuid(
    expected: Uuid,
    observed: Uuid,
    database: &str,
    collection: &str,
) -> Result<(), ConnectorError> {
    if expected == observed {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(format!(
        "MongoDB CDC collection identity changed for {database}.{collection}: \
         checkpoint/bound UUID={expected}, observed UUID={observed}"
    )))
}

pub(super) fn verify_mongodb_collection(
    config: &MongoDbSourceConfig,
    expected_uuid: Uuid,
    observation: &MongoCollectionObservation,
) -> Result<(), ConnectorError> {
    verify_mongodb_collection_uuid(
        expected_uuid,
        observation.collection_uuid,
        &config.database,
        &config.collection,
    )?;
    if config.full_document_mode == super::super::config::FullDocumentMode::RequirePostImage
        && !observation.post_images_enabled
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "MongoDB CDC full.document.mode=required needs changeStreamPreAndPostImages enabled \
             on {}.{} before the source starts",
            config.database, config.collection
        )));
    }
    Ok(())
}

pub(super) fn verify_mongodb_deployment_identity(
    expected: &MongoDeploymentIdentity,
    observed: &MongoDeploymentIdentity,
) -> Result<(), ConnectorError> {
    if expected == observed {
        return Ok(());
    }
    Err(ConnectorError::ConfigurationError(format!(
        "MongoDB CDC deployment identity changed: checkpoint/bound identity={}, observed \
         identity={}",
        expected.encode(),
        observed.encode()
    )))
}

pub(super) fn verify_mongodb_admission(
    config: &MongoDbSourceConfig,
    expected_deployment: &MongoDeploymentIdentity,
    expected_uuid: Uuid,
    observation: &MongoAdmissionObservation,
) -> Result<(), ConnectorError> {
    verify_mongodb_deployment_identity(expected_deployment, &observation.deployment_identity)?;
    verify_mongodb_collection(config, expected_uuid, &observation.collection)
}

pub(super) fn fresh_stream_anchor(
    cursor: &MongoChangeStream,
) -> Result<(ResumeToken, String), ConnectorError> {
    // The bootstrap aggregate uses batchSize=0, so MongoDB returns an empty firstBatch and its
    // exact postBatchResumeToken. Refuse an inclusive timestamp fallback: it can replay the final
    // write that preceded admission.
    let token = cursor.resume_token().ok_or_else(|| {
        ConnectorError::ReadError(
            "fresh MongoDB change stream omitted its initial postBatchResumeToken".into(),
        )
    })?;
    let encoded = canonical_post_batch_token(&token)?;
    Ok((token, encoded))
}

/// Background task that reads from the `MongoDB` change stream and sends
/// events to the source via a channel.
pub(super) async fn run_change_stream_reader(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    output: ReaderOutput,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    start: ReaderStart,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ready_tx: tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    snapshot_committed: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ConnectorError> {
    let client = db.client().clone();
    let result = run_change_stream_reader_loop(
        db,
        config,
        output,
        shutdown_rx,
        start,
        expected_collection_uuid,
        expected_deployment_identity,
        Some(ready_tx),
        snapshot_committed,
    )
    .await;

    // The loop owns every database, collection, and cursor handle. Once it
    // returns, shutdown can drain the driver's own async cleanup tasks.
    client.shutdown().await;
    result
}

/// Everything a reader generation borrows while it owns the driver.
struct ReaderSession<'a> {
    db: &'a mongodb::Database,
    config: &'a MongoDbSourceConfig,
    admission: &'a ReaderAdmission,
    output: &'a ReaderOutput,
    namespace: String,
}

/// Retry state shared by every cursor open in one reader generation.
struct ReaderControl {
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    ready_tx: ReadyTx,
    consecutive_failures: u32,
    verify_before_open: bool,
}

impl ReaderControl {
    async fn open(
        &mut self,
        session: &ReaderSession<'_>,
        position: Option<&MongoResumePosition>,
        empty_first_batch: bool,
    ) -> Result<Result<MongoChangeStream, ReconnectControl>, ConnectorError> {
        open_verified_cursor(
            session.db,
            session.config,
            session.admission,
            position,
            empty_first_batch,
            &mut self.verify_before_open,
            &mut self.shutdown_rx,
            &session.output.metrics,
            &mut self.consecutive_failures,
            &mut self.ready_tx,
        )
        .await
    }

    fn fail_admission(&mut self, error: ConnectorError) -> ConnectorError {
        report_mongo_reader_admission_error(&mut self.ready_tx, &error);
        error
    }
}

async fn run_change_stream_reader_loop(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    output: ReaderOutput,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    start: ReaderStart,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    mut ready_tx: ReadyTx,
    snapshot_committed: tokio::sync::watch::Receiver<bool>,
) -> Result<(), ConnectorError> {
    let Some(admission) = prepare_reader_admission(
        &db,
        &config,
        expected_collection_uuid,
        expected_deployment_identity,
        &mut shutdown_rx,
        &output.metrics,
        &mut ready_tx,
    )
    .await?
    else {
        return Ok(());
    };
    let session = ReaderSession {
        db: &db,
        config: &config,
        admission: &admission,
        output: &output,
        namespace: namespace(&config),
    };
    let mut control = ReaderControl {
        shutdown_rx,
        ready_tx,
        consecutive_failures: 0,
        verify_before_open: false,
    };
    let resume_position = match start {
        ReaderStart::FreshChanges => None,
        ReaderStart::Stream(position) => Some(position),
        ReaderStart::FreshSnapshot => {
            let at = snapshot::choose_snapshot_time(&db, &config)
                .await
                .map_err(|error| control.fail_admission(error))?;
            let cut = SnapshotCut {
                at,
                after_key: None,
            };
            let Some(position) =
                bootstrap_snapshot(&session, &mut control, cut, Some(snapshot_committed)).await?
            else {
                return Ok(());
            };
            Some(position)
        }
        ReaderStart::Snapshot(cut) => {
            let Some(position) = bootstrap_snapshot(&session, &mut control, cut, None).await?
            else {
                return Ok(());
            };
            Some(position)
        }
    };
    stream_changes(&session, &mut control, resume_position).await?;
    if let Some(ready_tx) = control.ready_tx.take() {
        let _ = ready_tx.send(Err(MongoReaderFailure::Read(
            "change stream reader was shut down before the cursor opened".into(),
        )));
    }
    Ok(())
}

/// Copy the collection at `cut`, then return the inclusive stream start. A fresh cut first waits
/// for `committed`. `Ok(None)` means shutdown.
async fn bootstrap_snapshot(
    session: &ReaderSession<'_>,
    control: &mut ReaderControl,
    cut: SnapshotCut,
    committed: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<Option<MongoResumePosition>, ConnectorError> {
    if matches!(
        session.admission.deployment_identity,
        MongoDeploymentIdentity::ShardedCluster(_)
    ) {
        return Err(
            control.fail_admission(ConnectorError::ConfigurationError(format!(
                "MongoDB CDC snapshot.mode=initial supports replica sets only; {} is on a sharded \
             cluster, where _id need not be unique across shards",
                session.namespace
            ))),
        );
    }
    // The stream must already be able to start at the snapshot time before any copy.
    let probe = MongoResumePosition::StartAt(cut.at);
    loop {
        match control.open(session, Some(&probe), true).await? {
            Ok(cursor) => {
                drop(cursor);
                break;
            }
            Err(ReconnectControl::Retry) => {}
            Err(ReconnectControl::Stop) => return Ok(None),
        }
    }
    let mut initial_position = committed
        .is_some()
        .then(|| MongoCheckpointPosition::Snapshot(cut.clone()));
    publish_reader_ready(
        &mut control.ready_tx,
        &mut initial_position,
        session.admission,
    );
    if let Some(committed) = committed {
        if !snapshot::await_committed_cut(committed, &mut control.shutdown_rx).await? {
            return Ok(None);
        }
    }
    let output = session.output;
    if !snapshot::scan(
        session.db,
        session.config,
        &cut,
        output,
        &mut control.shutdown_rx,
    )
    .await?
        || !output
            .send(
                BufferedMongoPayload::SnapshotComplete,
                &mut control.shutdown_rx,
            )
            .await?
    {
        return Ok(None);
    }
    Ok(Some(MongoResumePosition::StartAt(cut.at)))
}

/// Forward changes from `resume_position` (or a fresh exclusive anchor) until shutdown.
async fn stream_changes(
    session: &ReaderSession<'_>,
    control: &mut ReaderControl,
    mut resume_position: Option<MongoResumePosition>,
) -> Result<(), ConnectorError> {
    let mut initial_position = None;
    loop {
        let bootstrap = resume_position.is_none() && control.ready_tx.is_some();
        let mut cursor = match control
            .open(session, resume_position.as_ref(), bootstrap)
            .await?
        {
            Ok(opened) => opened,
            Err(ReconnectControl::Retry) => continue,
            Err(ReconnectControl::Stop) => return Ok(()),
        };

        if bootstrap {
            let (token, encoded) =
                fresh_stream_anchor(&cursor).map_err(|error| control.fail_admission(error))?;
            resume_position = Some(MongoResumePosition::ResumeAfter(token));
            initial_position = Some(MongoCheckpointPosition::Stream(
                super::checkpoint::StreamPosition::ResumeAfter(encoded),
            ));
            drop(cursor);
            continue;
        }

        publish_reader_ready(
            &mut control.ready_tx,
            &mut initial_position,
            session.admission,
        );
        tracing::info!(
            database = %session.config.database,
            collection = %session.config.collection,
            resumed = resume_position.is_some(),
            "change stream reader started"
        );

        if matches!(
            forward_change_stream(
                &mut cursor,
                &mut control.shutdown_rx,
                &mut resume_position,
                session.output,
                &mut control.consecutive_failures,
                &session.namespace,
            )
            .await?,
            ChangeStreamRead::Stop
        ) {
            return Ok(());
        }

        // Exited recv loop due to error or cursor exhaustion — attempt reconnect.
        control.consecutive_failures += 1;
        if control.consecutive_failures >= MAX_FAILURES {
            let msg = format!(
                "change stream of {} failed after {MAX_FAILURES} consecutive transient failures",
                session.namespace
            );
            tracing::error!(%msg);
            return Err(ConnectorError::ReadError(msg));
        }

        let backoff = crate::retry::Backoff::broker_reconnect().delay(control.consecutive_failures);
        tracing::warn!(
            resume_position = ?resume_position,
            attempt = control.consecutive_failures,
            ?backoff,
            "reconnecting change stream"
        );
        session.output.metrics.record_reconnect();

        if retry_interrupted(&mut control.shutdown_rx, backoff).await {
            return Ok(());
        }

        // The MongoDB client owns topology monitoring and reconnects its pool.
        // Reusing it avoids spawning untracked driver generations on each retry.
        control.verify_before_open = true;
    }
}
