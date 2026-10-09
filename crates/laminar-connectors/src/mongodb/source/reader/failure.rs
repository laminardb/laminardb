//! Classification of change-stream and snapshot driver errors.
//!
//! The driver already resumes once on errors it labels resumable. An error that reaches the
//! reader is retried with bounded backoff only when it is transient: a network failure, a
//! `ResumableChangeStreamError`-labelled server error, or one of the election and routing codes
//! the change-stream resume specification lists. Every other server error is non-resumable and
//! stops the source with a specific, actionable error.

use mongodb::error::{ErrorKind, RESUMABLE_CHANGE_STREAM_ERROR};

use super::ConnectorError;

const UNAUTHORIZED: i32 = 13;
const AUTHENTICATION_FAILED: i32 = 18;
const BAD_VALUE: i32 = 2;
const FAILED_TO_PARSE: i32 = 9;
const NO_MATCHING_DOCUMENT: i32 = 47;
const COMMAND_NOT_FOUND: i32 = 59;
const INVALID_OPTIONS: i32 = 72;
const COMMAND_NOT_SUPPORTED: i32 = 115;
const SNAPSHOT_TOO_OLD: i32 = 239;
const INVALID_RESUME_TOKEN: i32 = 260;
const CHANGE_STREAM_FATAL: i32 = 280;
const CHANGE_STREAM_HISTORY_LOST: i32 = 286;
const IDL_UNKNOWN_FIELD: i32 = 40415;
/// Election, shutdown, and routing codes the resume specification treats as resumable.
const TRANSIENT_CODES: &[i32] = &[
    6, 7, 43, 63, 89, 91, 133, 150, 189, 234, 262, 9001, 10107, 11600, 11602, 13388, 13435, 13436,
];

/// Reader-side disposition of one driver error.
#[derive(Debug)]
pub(super) enum ReadFailure {
    /// Network, election, or other condition a bounded reconnect can repair.
    Transient(String),
    /// A condition that reconnecting cannot repair; the source stops with this error.
    Permanent(ConnectorError),
}

fn permanent(message: String) -> ReadFailure {
    ReadFailure::Permanent(ConnectorError::ConfigurationError(message))
}

/// Classify an error from opening or reading the change stream.
pub(super) fn classify_stream_error(error: &mongodb::error::Error, namespace: &str) -> ReadFailure {
    if let ErrorKind::Authentication { .. } = error.kind.as_ref() {
        return permanent(format!(
            "MongoDB CDC authentication failed for {namespace}; verify the connection \
             credentials: {error}"
        ));
    }
    let ErrorKind::Command(command) = error.kind.as_ref() else {
        return ReadFailure::Transient(error.to_string());
    };
    match command.code {
        CHANGE_STREAM_HISTORY_LOST => permanent(format!(
            "MongoDB CDC resume position for {namespace} is no longer in the oplog; changes \
             after the last checkpoint were lost. A saved token does not reserve oplog \
             history: increase the oplog window, then rebuild the targets from a new snapshot \
             with fresh pipeline state: {error}"
        )),
        INVALID_RESUME_TOKEN | CHANGE_STREAM_FATAL => permanent(format!(
            "MongoDB CDC cannot resume {namespace} from its checkpointed token; the server \
             rejected the position. Rebuild the targets with fresh pipeline state: {error}"
        )),
        NO_MATCHING_DOCUMENT => permanent(format!(
            "MongoDB CDC full.document.mode=required found no post-image for a change on \
             {namespace}; the image expired (expireAfterSeconds) or \
             changeStreamPreAndPostImages was disabled. Never treated as a delete: {error}"
        )),
        UNAUTHORIZED | AUTHENTICATION_FAILED => permanent(format!(
            "MongoDB CDC is not authorized to read the change stream of {namespace}; grant \
             changeStream and find on the collection: {error}"
        )),
        BAD_VALUE
        | FAILED_TO_PARSE
        | INVALID_OPTIONS
        | COMMAND_NOT_SUPPORTED
        | COMMAND_NOT_FOUND
        | IDL_UNKNOWN_FIELD => permanent(format!(
            "MongoDB server rejected the change-stream options for {namespace}; this source \
             needs MongoDB 6.0+ (expanded events and pre/post-images): {error}"
        )),
        SNAPSHOT_TOO_OLD => permanent(format!(
            "MongoDB CDC snapshot of {namespace} outlived the server snapshot history window \
             (minSnapshotHistoryWindowInSeconds). The targets hold a partial snapshot: empty \
             them and restart with fresh pipeline state after raising the window: {error}"
        )),
        code if TRANSIENT_CODES.contains(&code)
            || error.contains_label(RESUMABLE_CHANGE_STREAM_ERROR) =>
        {
            ReadFailure::Transient(error.to_string())
        }
        _ => permanent(format!(
            "MongoDB rejected the change stream of {namespace} with a non-resumable error; the \
             checkpointed position cannot be resumed as is: {error}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command_error(code: i32) -> mongodb::error::Error {
        let kind = ErrorKind::Command(
            serde_json::from_value(serde_json::json!({
                "code": code,
                "codeName": "Test",
                "errmsg": "test failure",
            }))
            .unwrap(),
        );
        mongodb::error::Error::from(kind)
    }

    #[test]
    fn permanent_conditions_stop_instead_of_reconnecting() {
        for (code, needle) in [
            (CHANGE_STREAM_HISTORY_LOST, "no longer in the oplog"),
            (INVALID_RESUME_TOKEN, "checkpointed token"),
            (CHANGE_STREAM_FATAL, "checkpointed token"),
            (NO_MATCHING_DOCUMENT, "post-image"),
            (UNAUTHORIZED, "not authorized"),
            (INVALID_OPTIONS, "rejected the change-stream options"),
            (SNAPSHOT_TOO_OLD, "snapshot history window"),
        ] {
            match classify_stream_error(&command_error(code), "app.users") {
                ReadFailure::Permanent(error) => {
                    assert!(error.to_string().contains(needle), "{code}: {error}");
                    assert!(!error.is_transient());
                }
                ReadFailure::Transient(message) => panic!("{code} classified transient: {message}"),
            }
        }
    }

    #[test]
    fn unlabeled_server_errors_are_not_retried() {
        // A malformed resume token surfaces as a generic KeyString location error.
        match classify_stream_error(&command_error(50811), "app.users") {
            ReadFailure::Permanent(error) => {
                assert!(error.to_string().contains("non-resumable"), "{error}");
            }
            ReadFailure::Transient(message) => panic!("retried a non-resumable error: {message}"),
        }
    }

    #[test]
    fn elections_and_network_failures_remain_retryable() {
        // NotWritablePrimary and InterruptedDueToReplStateChange surface during elections.
        for code in [10107, 11602, 91] {
            assert!(matches!(
                classify_stream_error(&command_error(code), "app.users"),
                ReadFailure::Transient(_)
            ));
        }
        let io = mongodb::error::Error::from(ErrorKind::Io(std::sync::Arc::new(
            std::io::Error::from(std::io::ErrorKind::ConnectionReset),
        )));
        assert!(matches!(
            classify_stream_error(&io, "app.users"),
            ReadFailure::Transient(_)
        ));
    }
}
