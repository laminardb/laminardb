//! Bounded change-stream reading, retry, and cancellation ownership.

use std::sync::Arc;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

use super::{
    canonical_resume_token, mongo_event_retained_bytes, mongo_high_watermark_retained_bytes,
    observe_mongodb_admission, parse_change_stream_event, BufferedMongoEvent, ChangeStreamTx,
    ConnectorError, MongoAdmissionObservation, MongoCollectionObservation, MongoDbCdcMetrics,
    MongoDbChangeEvent, MongoDbSourceConfig, MongoDeploymentIdentity, MongoReaderFailure,
    MongoReaderReady, MongoResumePosition, OperationType, CURSOR_MAX_AWAIT_TIME,
    MAX_MONGODB_WIRE_EVENT_BYTES,
};

#[cfg(feature = "mongodb-cdc")]
mod reconnect;
#[cfg(feature = "mongodb-cdc")]
use reconnect::{
    open_verified_cursor, prepare_reader_admission, ReaderAdmission, ReconnectControl,
};

/// Maximum consecutive failures before the reader gives up.
#[cfg(feature = "mongodb-cdc")]
const MAX_FAILURES: u32 = 10;

#[cfg(feature = "mongodb-cdc")]
pub(super) const READER_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[cfg(feature = "mongodb-cdc")]
fn publish_reader_ready(
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
    initial_resume_token: &mut Option<String>,
    admission: &ReaderAdmission,
) {
    if let Some(ready_tx) = ready_tx.take() {
        let _ = ready_tx.send(Ok(MongoReaderReady {
            initial_resume_token: initial_resume_token.take(),
            collection_uuid: admission.collection_uuid,
            deployment_identity: admission.deployment_identity.clone(),
        }));
    }
}

#[cfg(feature = "mongodb-cdc")]
fn report_reader_stopped_before_ready(
    ready_tx: Option<tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>>,
) {
    if let Some(ready_tx) = ready_tx {
        let _ = ready_tx.send(Err(MongoReaderFailure::Read(
            "change stream reader was shut down before the cursor opened".into(),
        )));
    }
}

#[cfg(feature = "mongodb-cdc")]
pub(super) enum ChangeStreamRead {
    Stop,
    Reconnect,
}

#[cfg(feature = "mongodb-cdc")]
pub(super) fn reader_stopping(shutdown_rx: &tokio::sync::watch::Receiver<bool>) -> bool {
    *shutdown_rx.borrow() || shutdown_rx.has_changed().is_err()
}

#[cfg(feature = "mongodb-cdc")]
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
        None => {}
    }
    options
}

#[cfg(feature = "mongodb-cdc")]
pub(super) fn bootstrap_change_stream_options(
    config: &MongoDbSourceConfig,
) -> mongodb::options::ChangeStreamOptions {
    let mut options = change_stream_options(config, None);
    // MongoDB guarantees an empty firstBatch for batchSize=0, so its PBRT is an exact opening
    // cut and cannot skip concurrently buffered events.
    options.batch_size = Some(0);
    options
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn forward_change_stream(
    cursor: &mut mongodb::change_stream::ChangeStream<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    resume_position: &mut Option<MongoResumePosition>,
    tx: &ChangeStreamTx,
    data_ready: &Notify,
    consecutive_failures: &mut u32,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    metrics: &MongoDbCdcMetrics,
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
            Ok(Some(event)) => {
                *consecutive_failures = 0;
                let event_token = event.id.clone();
                let wire_bytes = change_stream_wire_bytes(&event)?;
                metrics.record_bytes(u64::try_from(wire_bytes).unwrap_or(u64::MAX));
                let change_event = parse_change_stream_event(&event)?;
                let invalidated = change_event.operation_type == OperationType::Invalidate;
                let Some(change_event) = acquire_mongo_event_ownership(
                    change_event,
                    byte_budget,
                    max_buffered_bytes,
                    shutdown_rx,
                )
                .await?
                else {
                    return Ok(ChangeStreamRead::Stop);
                };
                if !send_event_or_shutdown(tx, change_event, shutdown_rx).await {
                    return Ok(ChangeStreamRead::Stop);
                }
                *resume_position = Some(if invalidated {
                    MongoResumePosition::StartAfter(event_token)
                } else {
                    MongoResumePosition::ResumeAfter(cursor.resume_token().unwrap_or(event_token))
                });
                data_ready.notify_one();
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
                            Some(MongoResumePosition::StartAfter(_)) | None => true,
                        };
                        if changed {
                            let encoded = canonical_post_batch_token(&token)?;
                            let Some(marker) = acquire_mongo_high_watermark_ownership(
                                encoded,
                                requires_start_after,
                                byte_budget,
                                max_buffered_bytes,
                                shutdown_rx,
                            )
                            .await?
                            else {
                                return Ok(ChangeStreamRead::Stop);
                            };
                            if !send_event_or_shutdown(tx, marker, shutdown_rx).await {
                                return Ok(ChangeStreamRead::Stop);
                            }
                            data_ready.notify_one();
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
            Err(error) => {
                tracing::error!(%error, "change stream error");
                return Ok(ChangeStreamRead::Reconnect);
            }
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
fn change_stream_wire_bytes(
    event: &mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
) -> Result<usize, ConnectorError> {
    let wire_bytes = mongodb::bson::to_vec(event)
        .map_err(|error| {
            ConnectorError::ReadError(format!(
                "serialize change stream event for byte accounting: {error}"
            ))
        })?
        .len();
    if wire_bytes > MAX_MONGODB_WIRE_EVENT_BYTES {
        return Err(ConnectorError::ReadError(format!(
            "MongoDB CDC wire event exceeds the supported unsplit BSON bound: event={wire_bytes}, \
             limit={MAX_MONGODB_WIRE_EVENT_BYTES}"
        )));
    }
    Ok(wire_bytes)
}

#[cfg(feature = "mongodb-cdc")]
fn canonical_post_batch_token(
    token: &mongodb::change_stream::event::ResumeToken,
) -> Result<String, ConnectorError> {
    let encoded = serde_json::to_string(token).map_err(|error| {
        ConnectorError::ReadError(format!(
            "serialize MongoDB post-batch resume token: {error}"
        ))
    })?;
    canonical_resume_token(&encoded).map_err(|error| {
        ConnectorError::ReadError(format!("invalid MongoDB post-batch resume token: {error}"))
    })
}

#[cfg(feature = "mongodb-cdc")]
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

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn acquire_mongo_event_ownership(
    event: MongoDbChangeEvent,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<BufferedMongoEvent>, ConnectorError> {
    let retained_bytes = mongo_event_retained_bytes(&event)?;
    let Some(byte_permit) =
        acquire_mongo_byte_permit(retained_bytes, byte_budget, max_buffered_bytes, shutdown_rx)
            .await?
    else {
        return Ok(None);
    };

    Ok(Some(BufferedMongoEvent::new(event, byte_permit)))
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn acquire_mongo_high_watermark_ownership(
    token: String,
    requires_start_after: bool,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<BufferedMongoEvent>, ConnectorError> {
    let retained_bytes = mongo_high_watermark_retained_bytes(token.capacity())?;
    let Some(byte_permit) =
        acquire_mongo_byte_permit(retained_bytes, byte_budget, max_buffered_bytes, shutdown_rx)
            .await?
    else {
        return Ok(None);
    };
    Ok(Some(BufferedMongoEvent::high_watermark(
        token,
        requires_start_after,
        byte_permit,
    )))
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn acquire_mongo_byte_permit(
    retained_bytes: usize,
    byte_budget: &Arc<Semaphore>,
    max_buffered_bytes: usize,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<Option<OwnedSemaphorePermit>, ConnectorError> {
    if reader_stopping(shutdown_rx) {
        return Ok(None);
    }
    if retained_bytes > max_buffered_bytes {
        return Err(ConnectorError::ReadError(format!(
            "MongoDB CDC decoded item exceeds the hard byte bound: item={retained_bytes}, \
             limit={max_buffered_bytes}"
        )));
    }
    let permits = u32::try_from(retained_bytes).map_err(|_| {
        ConnectorError::ReadError(format!(
            "MongoDB CDC decoded item exceeds the hard byte bound: item={retained_bytes}, \
             limit={max_buffered_bytes}"
        ))
    })?;
    let byte_permit = tokio::select! {
        biased;
        _ = shutdown_rx.changed() => return Ok(None),
        permit = Arc::clone(byte_budget).acquire_many_owned(permits) => permit.map_err(|_| {
            ConnectorError::ReadError("MongoDB CDC byte budget closed".into())
        })?,
    };
    Ok(Some(byte_permit))
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn retry_interrupted(
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    delay: std::time::Duration,
) -> bool {
    tokio::select! {
        changed = shutdown_rx.changed() => changed.is_err() || *shutdown_rx.borrow(),
        () = tokio::time::sleep(delay) => false,
    }
}

#[cfg(feature = "mongodb-cdc")]
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

#[cfg(feature = "mongodb-cdc")]
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

#[cfg(feature = "mongodb-cdc")]
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

#[cfg(feature = "mongodb-cdc")]
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

#[cfg(feature = "mongodb-cdc")]
pub(super) fn verify_mongodb_admission(
    config: &MongoDbSourceConfig,
    expected_deployment: &MongoDeploymentIdentity,
    expected_uuid: Uuid,
    observation: &MongoAdmissionObservation,
) -> Result<(), ConnectorError> {
    verify_mongodb_deployment_identity(expected_deployment, &observation.deployment_identity)?;
    verify_mongodb_collection(config, expected_uuid, &observation.collection)
}

#[cfg(feature = "mongodb-cdc")]
pub(super) fn fresh_stream_anchor(
    cursor: &mongodb::change_stream::ChangeStream<
        mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
    >,
) -> Result<(mongodb::change_stream::event::ResumeToken, String), ConnectorError> {
    // The bootstrap aggregate uses batchSize=0, so MongoDB returns an empty firstBatch and its
    // exact postBatchResumeToken. Refuse an inclusive timestamp fallback: it can replay the final
    // write that preceded admission.
    let token = cursor.resume_token().ok_or_else(|| {
        ConnectorError::ReadError(
            "fresh MongoDB change stream omitted its initial postBatchResumeToken".into(),
        )
    })?;
    let encoded = serde_json::to_string(&token).map_err(|error| {
        ConnectorError::ReadError(format!(
            "serialize initial MongoDB post-batch resume token: {error}"
        ))
    })?;
    let encoded = canonical_resume_token(&encoded).map_err(|error| {
        ConnectorError::ReadError(format!(
            "invalid initial MongoDB post-batch resume token: {error}"
        ))
    })?;
    Ok((token, encoded))
}

#[cfg(feature = "mongodb-cdc")]
pub(super) fn report_mongo_reader_admission_error(
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
    error: &ConnectorError,
) {
    if let Some(ready_tx) = ready_tx.take() {
        let _ = ready_tx.send(Err(MongoReaderFailure::from_connector(error)));
    }
}

/// Background task that reads from the `MongoDB` change stream and sends
/// events to the source via a channel.
///
/// Uses a `'reconnect` / `'recv` double-loop pattern (mirroring the
/// Postgres CDC source) with exponential backoff capped at 30 seconds.
#[cfg(feature = "mongodb-cdc")]
pub(super) async fn run_change_stream_reader(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    tx: ChangeStreamTx,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_ready: Arc<Notify>,
    metrics: Arc<MongoDbCdcMetrics>,
    byte_budget: Arc<Semaphore>,
    max_buffered_bytes: usize,
    initial_resume_position: Option<MongoResumePosition>,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ready_tx: tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
) -> Result<(), ConnectorError> {
    let client = db.client().clone();
    let result = run_change_stream_reader_loop(
        db,
        config,
        tx,
        shutdown_rx,
        data_ready,
        metrics,
        byte_budget,
        max_buffered_bytes,
        initial_resume_position,
        expected_collection_uuid,
        expected_deployment_identity,
        ready_tx,
    )
    .await;

    // The loop owns every database, collection, and cursor handle. Once it
    // returns, shutdown can drain the driver's own async cleanup tasks.
    client.shutdown().await;
    result
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn run_change_stream_reader_loop(
    db: mongodb::Database,
    config: MongoDbSourceConfig,
    tx: ChangeStreamTx,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    data_ready: Arc<Notify>,
    metrics: Arc<MongoDbCdcMetrics>,
    byte_budget: Arc<Semaphore>,
    max_buffered_bytes: usize,
    initial_resume_position: Option<MongoResumePosition>,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    ready_tx: tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
) -> Result<(), ConnectorError> {
    let mut resume_position = initial_resume_position;
    let fresh_start = resume_position.is_none();
    let mut initial_resume_token = None;
    let mut ready_tx = Some(ready_tx);
    let current_db = db;
    let Some(admission) = prepare_reader_admission(
        &current_db,
        &config,
        expected_collection_uuid,
        expected_deployment_identity,
        &mut shutdown_rx,
        &metrics,
        &mut ready_tx,
    )
    .await?
    else {
        return Ok(());
    };
    let mut consecutive_failures = 0;
    let mut verify_before_open = false;

    'reconnect: loop {
        let (mut cursor, bootstrap) = match open_verified_cursor(
            &current_db,
            &config,
            &admission,
            fresh_start,
            resume_position.as_ref(),
            &mut verify_before_open,
            &mut shutdown_rx,
            &metrics,
            &mut consecutive_failures,
            &mut ready_tx,
        )
        .await?
        {
            Ok(opened) => opened,
            Err(ReconnectControl::Retry) => continue 'reconnect,
            Err(ReconnectControl::Stop) => break 'reconnect,
        };

        if bootstrap {
            match fresh_stream_anchor(&cursor) {
                Ok((token, encoded)) => {
                    resume_position = Some(MongoResumePosition::ResumeAfter(token));
                    initial_resume_token = Some(encoded);
                }
                Err(error) => {
                    report_mongo_reader_admission_error(&mut ready_tx, &error);
                    return Err(error);
                }
            }
            drop(cursor);
            continue 'reconnect;
        }

        publish_reader_ready(&mut ready_tx, &mut initial_resume_token, &admission);

        tracing::info!(
            database = %config.database,
            collection = %config.collection,
            resumed = resume_position.is_some(),
            "change stream reader started"
        );

        if matches!(
            forward_change_stream(
                &mut cursor,
                &mut shutdown_rx,
                &mut resume_position,
                &tx,
                &data_ready,
                &mut consecutive_failures,
                &byte_budget,
                max_buffered_bytes,
                &metrics,
            )
            .await?,
            ChangeStreamRead::Stop
        ) {
            break 'reconnect;
        }

        // Exited recv loop due to error or cursor exhaustion — attempt reconnect.
        consecutive_failures += 1;
        if consecutive_failures >= MAX_FAILURES {
            let msg = format!("change stream failed after {MAX_FAILURES} consecutive failures");
            tracing::error!(%msg);
            return Err(ConnectorError::ReadError(msg));
        }

        let backoff = crate::retry::Backoff::broker_reconnect().delay(consecutive_failures);
        tracing::warn!(
            resume_position = ?resume_position,
            attempt = consecutive_failures,
            ?backoff,
            "reconnecting change stream"
        );
        metrics.record_reconnect();

        if retry_interrupted(&mut shutdown_rx, backoff).await {
            break 'reconnect;
        }

        // The MongoDB client owns topology monitoring and reconnects its pool.
        // Reusing it avoids spawning untracked driver generations on each retry.
        verify_before_open = true;
    }

    report_reader_stopped_before_ready(ready_tx.take());
    Ok(())
}
