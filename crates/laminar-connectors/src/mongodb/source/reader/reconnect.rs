//! Bounded identity verification, cursor opening, and reconnect admission.

use super::failure::{classify_stream_error, ReadFailure};
use super::{
    bootstrap_change_stream_options, change_stream_options, namespace, observe_mongodb_admission,
    parse_change_stream_pipeline, report_mongo_reader_admission_error, retry_interrupted,
    verify_mongodb_admission, ConnectorError, MongoAdmissionObservation, MongoChangeStream,
    MongoDbCdcMetrics, MongoDbSourceConfig, MongoDeploymentIdentity, MongoResumePosition, ReadyTx,
    Uuid, MAX_FAILURES,
};

enum AdmissionPhase {
    BeforeCursorOpen,
    AfterCursorOpen,
}

enum AdmissionAttempt {
    Verified,
    Retry,
    Stop,
}

pub(super) enum ReconnectControl {
    Retry,
    Stop,
}

type CursorAttempt = Result<MongoChangeStream, ReconnectControl>;

pub(super) struct ReaderAdmission {
    pub(super) pipeline: Vec<mongodb::bson::Document>,
    pub(super) collection_uuid: Uuid,
    pub(super) deployment_identity: MongoDeploymentIdentity,
}

/// Count one transient failure and back off. `Ok(false)` means shutdown interrupted the wait.
pub(super) async fn back_off(
    error: &str,
    context: &str,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
) -> Result<bool, ConnectorError> {
    *consecutive_failures += 1;
    if *consecutive_failures >= MAX_FAILURES {
        return Err(ConnectorError::ConnectionFailed(format!(
            "{context} failed after {MAX_FAILURES} consecutive attempts: {error}"
        )));
    }
    let backoff = crate::retry::Backoff::broker_reconnect().delay(*consecutive_failures);
    tracing::warn!(attempt = *consecutive_failures, ?backoff, %error, "{context} failed, retrying");
    metrics.record_reconnect();
    Ok(!retry_interrupted(shutdown_rx, backoff).await)
}

async fn observe_initial_admission(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    ready_tx: &mut ReadyTx,
) -> Result<Option<MongoAdmissionObservation>, ConnectorError> {
    let mut consecutive_failures = 0;
    loop {
        let error = match observe_mongodb_admission(db, &config.database, &config.collection).await
        {
            Ok(observation) => return Ok(Some(observation)),
            Err(error) if !error.is_transient() => error,
            Err(error) => match back_off(
                &error.to_string(),
                "MongoDB deployment and collection identity inspection",
                shutdown_rx,
                metrics,
                &mut consecutive_failures,
            )
            .await
            {
                Ok(true) => continue,
                Ok(false) => return Ok(None),
                Err(error) => error,
            },
        };
        report_mongo_reader_admission_error(ready_tx, &error);
        return Err(error);
    }
}

pub(super) async fn prepare_reader_admission(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    expected_collection_uuid: Option<Uuid>,
    expected_deployment_identity: Option<MongoDeploymentIdentity>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    ready_tx: &mut ReadyTx,
) -> Result<Option<ReaderAdmission>, ConnectorError> {
    let pipeline = match parse_change_stream_pipeline(&config.pipeline) {
        Ok(pipeline) => pipeline,
        Err(error) => {
            report_mongo_reader_admission_error(ready_tx, &error);
            return Err(error);
        }
    };
    let Some(observation) =
        observe_initial_admission(db, config, shutdown_rx, metrics, ready_tx).await?
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

async fn verify_reconnect_admission(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    admission: &ReaderAdmission,
    phase: AdmissionPhase,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut ReadyTx,
) -> Result<AdmissionAttempt, ConnectorError> {
    let error = match observe_mongodb_admission(db, &config.database, &config.collection).await {
        Ok(observation) => {
            if let Err(error) = verify_mongodb_admission(
                config,
                &admission.deployment_identity,
                admission.collection_uuid,
                &observation,
            ) {
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
    let context = match phase {
        AdmissionPhase::BeforeCursorOpen => "MongoDB identity verification before reconnect",
        AdmissionPhase::AfterCursorOpen => "MongoDB identity verification after cursor open",
    };
    match back_off(
        &error.to_string(),
        context,
        shutdown_rx,
        metrics,
        consecutive_failures,
    )
    .await
    {
        Ok(true) => Ok(AdmissionAttempt::Retry),
        Ok(false) => Ok(AdmissionAttempt::Stop),
        Err(error) => {
            report_mongo_reader_admission_error(ready_tx, &error);
            Err(error)
        }
    }
}

async fn open_change_stream_cursor(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    pipeline: &[mongodb::bson::Document],
    options: mongodb::options::ChangeStreamOptions,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut ReadyTx,
) -> Result<CursorAttempt, ConnectorError> {
    let result = db
        .collection::<mongodb::bson::Document>(&config.collection)
        .watch()
        .pipeline(pipeline.to_vec())
        .with_options(options)
        .await;
    let error = match result {
        Ok(cursor) => return Ok(Ok(cursor.with_type::<mongodb::bson::RawDocumentBuf>())),
        Err(error) => error,
    };
    let message = match classify_stream_error(&error, &namespace(config)) {
        ReadFailure::Permanent(error) => {
            report_mongo_reader_admission_error(ready_tx, &error);
            return Err(error);
        }
        ReadFailure::Transient(message) => message,
    };
    match back_off(
        &message,
        "MongoDB change stream open",
        shutdown_rx,
        metrics,
        consecutive_failures,
    )
    .await
    {
        Ok(true) => Ok(Err(ReconnectControl::Retry)),
        Ok(false) => Ok(Err(ReconnectControl::Stop)),
        Err(error) => {
            report_mongo_reader_admission_error(ready_tx, &error);
            Err(error)
        }
    }
}

/// Open a cursor at `resume_position` between two identity checks. Opening successfully does
/// not reset the failure budget; only stream progress does.
pub(super) async fn open_verified_cursor(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    admission: &ReaderAdmission,
    resume_position: Option<&MongoResumePosition>,
    empty_first_batch: bool,
    verify_before_open: &mut bool,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
    metrics: &MongoDbCdcMetrics,
    consecutive_failures: &mut u32,
    ready_tx: &mut ReadyTx,
) -> Result<CursorAttempt, ConnectorError> {
    if *verify_before_open {
        match verify_reconnect_admission(
            db,
            config,
            admission,
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

    let options = if empty_first_batch {
        bootstrap_change_stream_options(config, resume_position)
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
        admission,
        AdmissionPhase::AfterCursorOpen,
        shutdown_rx,
        metrics,
        consecutive_failures,
        ready_tx,
    )
    .await?
    {
        AdmissionAttempt::Verified => Ok(Ok(cursor)),
        AdmissionAttempt::Retry => {
            *verify_before_open = true;
            Ok(Err(ReconnectControl::Retry))
        }
        AdmissionAttempt::Stop => Ok(Err(ReconnectControl::Stop)),
    }
}
