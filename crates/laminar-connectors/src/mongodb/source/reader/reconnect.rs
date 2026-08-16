//! Bounded identity verification, cursor opening, and reconnect admission.

use super::{
    bootstrap_change_stream_options, change_stream_options, observe_mongodb_admission,
    parse_change_stream_pipeline, report_mongo_reader_admission_error, retry_interrupted,
    verify_mongodb_admission, ConnectorError, MongoAdmissionObservation, MongoDbCdcMetrics,
    MongoDbSourceConfig, MongoDeploymentIdentity, MongoReaderFailure, MongoReaderReady,
    MongoResumePosition, Uuid, MAX_FAILURES,
};

#[cfg(feature = "mongodb-cdc")]
enum AdmissionPhase {
    BeforeCursorOpen,
    AfterCursorOpen,
}

#[cfg(feature = "mongodb-cdc")]
enum AdmissionAttempt {
    Verified,
    Retry,
    Stop,
}

#[cfg(feature = "mongodb-cdc")]
pub(super) enum ReconnectControl {
    Retry,
    Stop,
}

#[cfg(feature = "mongodb-cdc")]
type MongoChangeStream = mongodb::change_stream::ChangeStream<
    mongodb::change_stream::event::ChangeStreamEvent<mongodb::bson::Document>,
>;

#[cfg(feature = "mongodb-cdc")]
type CursorAttempt = Result<MongoChangeStream, ReconnectControl>;

#[cfg(feature = "mongodb-cdc")]
pub(super) struct ReaderAdmission {
    pub(super) pipeline: Vec<mongodb::bson::Document>,
    pub(super) collection_uuid: Uuid,
    pub(super) deployment_identity: MongoDeploymentIdentity,
}

#[cfg(feature = "mongodb-cdc")]
pub(super) type VerifiedCursorAttempt = Result<(MongoChangeStream, bool), ReconnectControl>;

#[cfg(feature = "mongodb-cdc")]
async fn observe_initial_admission(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
) -> Result<Option<MongoAdmissionObservation>, ConnectorError> {
    loop {
        match observe_mongodb_admission(db, &config.database, &config.collection).await {
            Ok(observation) => return Ok(Some(observation)),
            Err(error) if !error.is_transient() => {
                report_mongo_reader_admission_error(ready_tx, &error);
                return Err(error);
            }
            Err(error) => {
                *consecutive_failures += 1;
                if *consecutive_failures >= MAX_FAILURES {
                    report_mongo_reader_admission_error(ready_tx, &error);
                    return Err(error);
                }
                let backoff =
                    crate::retry::Backoff::broker_reconnect().delay(*consecutive_failures);
                tracing::warn!(
                    attempt = *consecutive_failures,
                    ?backoff,
                    error = %error,
                    "failed to inspect MongoDB deployment or collection identity, retrying"
                );
                metrics.record_reconnect();
                if retry_interrupted(shutdown_rx, backoff).await {
                    return Ok(None);
                }
            }
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn prepare_reader_admission(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
) -> Result<Option<ReaderAdmission>, ConnectorError> {
    let pipeline = match parse_change_stream_pipeline(&config.pipeline) {
        Ok(pipeline) => pipeline,
        Err(error) => {
            report_mongo_reader_admission_error(ready_tx, &error);
            return Err(error);
        }
    };
    let mut consecutive_failures = 0;
    let Some(observation) = observe_initial_admission(
        db,
        config,
        shutdown_rx,
        metrics,
        &mut consecutive_failures,
        ready_tx,
    )
    .await?
    else {
        return Ok(None);
    };
    let collection_uuid =
        expected_collection_uuid.unwrap_or(observation.collection.collection_uuid);
    let deployment_identity =
        expected_deployment_identity.unwrap_or_else(|| observation.deployment_identity.clone());
    if let Err(error) =
        verify_mongodb_admission(config, &deployment_identity, collection_uuid, &observation)
    {
        report_mongo_reader_admission_error(ready_tx, &error);
        return Err(error);
    }
    Ok(Some(ReaderAdmission {
        pipeline,
        collection_uuid,
        deployment_identity,
    }))
}

#[cfg(feature = "mongodb-cdc")]
async fn verify_reconnect_admission(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    deployment_identity: &MongoDeploymentIdentity,
    collection_uuid: Uuid,
    phase: AdmissionPhase,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
) -> Result<AdmissionAttempt, ConnectorError> {
    let error = match observe_mongodb_admission(db, &config.database, &config.collection).await {
        Ok(observation) => {
            if let Err(error) =
                verify_mongodb_admission(config, deployment_identity, collection_uuid, &observation)
            {
                report_mongo_reader_admission_error(ready_tx, &error);
                return Err(error);
            }
            return Ok(AdmissionAttempt::Verified);
        }
        Err(error) => error,
    };
    if !error.is_transient() {
        report_mongo_reader_admission_error(ready_tx, &error);
        return Err(error);
    }

    *consecutive_failures += 1;
    if *consecutive_failures >= MAX_FAILURES {
        report_mongo_reader_admission_error(ready_tx, &error);
        return Err(error);
    }
    let backoff = crate::retry::Backoff::broker_reconnect().delay(*consecutive_failures);
    match phase {
        AdmissionPhase::BeforeCursorOpen => tracing::warn!(
            attempt = *consecutive_failures,
            ?backoff,
            error = %error,
            "failed to verify MongoDB deployment or collection identity before reconnect"
        ),
        AdmissionPhase::AfterCursorOpen => tracing::warn!(
            attempt = *consecutive_failures,
            ?backoff,
            error = %error,
            "failed to verify MongoDB deployment or collection identity after opening change stream"
        ),
    }
    metrics.record_reconnect();
    if retry_interrupted(shutdown_rx, backoff).await {
        Ok(AdmissionAttempt::Stop)
    } else {
        Ok(AdmissionAttempt::Retry)
    }
}

#[cfg(feature = "mongodb-cdc")]
async fn open_change_stream_cursor(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    pipeline: &[mongodb::bson::Document],
    options: mongodb::options::ChangeStreamOptions,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
) -> Result<CursorAttempt, ConnectorError> {
    let result = db
        .collection::<mongodb::bson::Document>(&config.collection)
        .watch()
        .pipeline(pipeline.to_vec())
        .with_options(options)
        .await;
    match result {
        Ok(cursor) => Ok(Ok(cursor)),
        Err(error) => {
            *consecutive_failures += 1;
            if *consecutive_failures >= MAX_FAILURES {
                let msg =
                    format!("change stream open failed after {MAX_FAILURES} attempts: {error}");
                tracing::error!(%msg);
                let error = ConnectorError::ReadError(msg);
                report_mongo_reader_admission_error(ready_tx, &error);
                return Err(error);
            }
            let backoff = crate::retry::Backoff::broker_reconnect().delay(*consecutive_failures);
            tracing::warn!(
                attempt = *consecutive_failures,
                ?backoff,
                error = %error,
                "failed to open change stream, retrying"
            );
            metrics.record_reconnect();
            if retry_interrupted(shutdown_rx, backoff).await {
                Ok(Err(ReconnectControl::Stop))
            } else {
                Ok(Err(ReconnectControl::Retry))
            }
        }
    }
}

#[cfg(feature = "mongodb-cdc")]
pub(super) async fn open_verified_cursor(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    admission: &ReaderAdmission,
    fresh_start: bool,
    resume_position: Option<&MongoResumePosition>,
    verify_before_open: &mut bool,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut Option<
        tokio::sync::oneshot::Sender<Result<MongoReaderReady, MongoReaderFailure>>,
    >,
) -> Result<VerifiedCursorAttempt, ConnectorError> {
    if *verify_before_open {
        match verify_reconnect_admission(
            db,
            config,
            &admission.deployment_identity,
            admission.collection_uuid,
            AdmissionPhase::BeforeCursorOpen,
            shutdown_rx,
            metrics,
            consecutive_failures,
            ready_tx,
        )
        .await?
        {
            AdmissionAttempt::Verified => *verify_before_open = false,
            AdmissionAttempt::Retry => return Ok(Err(ReconnectControl::Retry)),
            AdmissionAttempt::Stop => return Ok(Err(ReconnectControl::Stop)),
        }
    }

    let bootstrap = fresh_start && ready_tx.is_some() && resume_position.is_none();
    let options = if bootstrap {
        bootstrap_change_stream_options(config)
    } else {
        change_stream_options(config, resume_position)
    };
    let cursor = match open_change_stream_cursor(
        db,
        config,
        &admission.pipeline,
        options,
        shutdown_rx,
        metrics,
        consecutive_failures,
        ready_tx,
    )
    .await?
    {
        Ok(cursor) => cursor,
        Err(ReconnectControl::Retry) => {
            *verify_before_open = true;
            return Ok(Err(ReconnectControl::Retry));
        }
        Err(ReconnectControl::Stop) => return Ok(Err(ReconnectControl::Stop)),
    };

    match verify_reconnect_admission(
        db,
        config,
        &admission.deployment_identity,
        admission.collection_uuid,
        AdmissionPhase::AfterCursorOpen,
        shutdown_rx,
        metrics,
        consecutive_failures,
        ready_tx,
    )
    .await?
    {
        AdmissionAttempt::Verified => {
            *consecutive_failures = 0;
            Ok(Ok((cursor, bootstrap)))
        }
        AdmissionAttempt::Retry => {
            *verify_before_open = true;
            Ok(Err(ReconnectControl::Retry))
        }
        AdmissionAttempt::Stop => Ok(Err(ReconnectControl::Stop)),
    }
}
