//! Initial collection snapshot tied to the change-stream boundary.
//!
//! Protocol: choose a majority snapshot time `T` from the server, prove the change stream can
//! start at `T`, durably commit `T` before copying anything, copy the collection with
//! `readConcern: snapshot` at `T` in `_id` order, then stream every change at or after `T`.
//! A snapshot at `T` contains every write committed at or before `T`, and
//! `startAtOperationTime: T` returns every change at or after `T`, so no write is missed;
//! changes at exactly `T` (including whole transactions) are delivered twice, which full-image
//! puts, deletes, and history consumers tolerate. Resume tokens are never decoded.

use futures_util::StreamExt;
use mongodb::bson::{doc, Bson, RawDocumentBuf, Timestamp};
use mongodb::options::{Hint, SessionOptions};

use super::super::buffering::{BufferedMongoPayload, SnapshotRecord};
use super::super::checkpoint::SnapshotCut;
use super::failure::{classify_stream_error, ReadFailure};
use super::reconnect::back_off;
use super::{namespace, reader_stopping, ConnectorError, MongoDbSourceConfig, ReaderOutput};
use crate::mongodb::change_event::canonical_extjson;

fn driver_error(config: &MongoDbSourceConfig, error: &mongodb::error::Error) -> ConnectorError {
    match classify_stream_error(error, &namespace(config)) {
        ReadFailure::Permanent(error) => error,
        ReadFailure::Transient(message) => ConnectorError::ConnectionFailed(message),
    }
}

/// Majority-committed snapshot time chosen by the server for a one-document probe read.
pub(super) async fn choose_snapshot_time(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
) -> Result<Timestamp, ConnectorError> {
    let mut session = db
        .client()
        .start_session()
        .snapshot(true)
        .await
        .map_err(|error| driver_error(config, &error))?;
    db.collection::<RawDocumentBuf>(&config.collection)
        .find_one(doc! {})
        .projection(doc! { "_id": 1 })
        .session(&mut session)
        .await
        .map_err(|error| driver_error(config, &error))?;
    session.snapshot_time().ok_or_else(|| {
        ConnectorError::ConfigurationError(format!(
            "MongoDB did not report a snapshot time for {}; snapshot reads need a replica set \
             running MongoDB 5.0 or later",
            namespace(config)
        ))
    })
}

/// Wait until a checkpoint carrying the fresh snapshot cut is durable. Copying before that
/// would let a crash restart a new cut over partially written targets. `Ok(false)` = shutdown.
pub(super) async fn await_committed_cut(
    mut committed: tokio::sync::watch::Receiver<bool>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, ConnectorError> {
    if reader_stopping(shutdown_rx) {
        return Ok(false);
    }
    tracing::info!(
        "MongoDB CDC initial snapshot waits for a committed checkpoint carrying its cut \
         before copying; with manual checkpoints, take one after start"
    );
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => Ok(false),
        result = committed.wait_for(|committed| *committed) => result.map(|_| true).map_err(|_| {
            ConnectorError::Internal("MongoDB snapshot commit signal closed".into())
        }),
    }
}

/// Copy every document after `cut.after_key` at `cut.at`. `Ok(false)` means shutdown.
pub(super) async fn scan(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    cut: &SnapshotCut,
    output: &ReaderOutput,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, ConnectorError> {
    let mut after_key = cut.after_key.clone();
    let mut consecutive_failures = 0;
    loop {
        let attempt_start = after_key.clone();
        match scan_from(db, config, cut.at, &mut after_key, output, shutdown_rx).await {
            Ok(completed) => return Ok(completed),
            Err(ReadFailure::Permanent(error)) => return Err(error),
            Err(ReadFailure::Transient(message)) => {
                // Only copy progress resets the budget, as stream progress does for changes.
                if after_key != attempt_start {
                    consecutive_failures = 0;
                }
                if !back_off(
                    &message,
                    "MongoDB snapshot scan",
                    shutdown_rx,
                    &output.metrics,
                    &mut consecutive_failures,
                )
                .await?
                {
                    return Ok(false);
                }
            }
        }
    }
}

async fn scan_from(
    db: &mongodb::Database,
    config: &MongoDbSourceConfig,
    at: Timestamp,
    after_key: &mut Option<String>,
    output: &ReaderOutput,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<bool, ReadFailure> {
    let namespace = namespace(config);
    let permanent = |error: ConnectorError| ReadFailure::Permanent(error);
    let options = SessionOptions::builder()
        .snapshot(true)
        .snapshot_time(at)
        .build();
    let mut session = db
        .client()
        .start_session()
        .with_options(options)
        .await
        .map_err(|error| classify_stream_error(&error, &namespace))?;
    // `min` with the `_id` index hint resumes in index order across BSON types; `$gt` would
    // compare only within one type bracket and skip documents with other `_id` types.
    let collection = db.collection::<RawDocumentBuf>(&config.collection);
    let mut find = collection
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .hint(Hint::Keys(doc! { "_id": 1 }))
        .batch_size(config.cursor_batch_size());
    if let Some(key) = after_key.as_deref() {
        let value: serde_json::Value = serde_json::from_str(key).map_err(|error| {
            permanent(ConnectorError::ConfigurationError(format!(
                "MongoDB snapshot resume key is invalid: {error}"
            )))
        })?;
        let id = Bson::try_from(value).map_err(|error| {
            permanent(ConnectorError::ConfigurationError(format!(
                "MongoDB snapshot resume key is not Extended JSON: {error}"
            )))
        })?;
        find = find.min(doc! { "_id": id });
    }
    let mut cursor = find
        .session(&mut session)
        .await
        .map_err(|error| classify_stream_error(&error, &namespace))?;
    let mut stream = cursor.stream(&mut session);
    let mut skip_resume_key = after_key.is_some();
    while let Some(next) = stream.next().await {
        if reader_stopping(shutdown_rx) {
            return Ok(false);
        }
        let raw = next.map_err(|error| classify_stream_error(&error, &namespace))?;
        let id = raw
            .get("_id")
            .map_err(|error| {
                permanent(ConnectorError::ReadError(format!(
                    "malformed snapshot document in {namespace}: {error}"
                )))
            })?
            .ok_or_else(|| {
                permanent(ConnectorError::ReadError(format!(
                    "snapshot document in {namespace} has no _id"
                )))
            })?;
        let key = canonical_extjson(id).map_err(permanent)?;
        if std::mem::take(&mut skip_resume_key) && after_key.as_deref() == Some(key.as_str()) {
            continue;
        }
        let record = SnapshotRecord {
            raw,
            key: key.clone(),
        };
        if !output
            .send(BufferedMongoPayload::Snapshot(record), shutdown_rx)
            .await
            .map_err(permanent)?
        {
            return Ok(false);
        }
        output.metrics.snapshot_documents.inc();
        *after_key = Some(key);
    }
    Ok(true)
}
