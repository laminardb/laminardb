//! `MongoDB` bulk-failure certainty and retry classification.

use super::{ConnectorError, Duration};

#[derive(Clone, Copy)]
pub(super) enum MongoBulkFailure<'a> {
    Driver(&'a mongodb::error::Error),
    Deadline(Duration),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MongoBulkFailureShape {
    PreCommandTransient,
    PreCommandTerminal,
    Transport,
    Command,
    WriteRejected,
    WriteConcern,
    Bulk {
        partial: bool,
        write_errors: bool,
        write_concern_errors: bool,
    },
    Deadline,
    Unknown,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct MongoBulkFailureFacts {
    pub(super) no_writes: bool,
    pub(super) retryable_signal: bool,
    pub(super) shape: MongoBulkFailureShape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MongoBulkDisposition {
    DefinitelyNotApplied { retryable: bool },
    OutcomeUnknown { retryable: bool },
}

pub(super) fn mongo_bulk_failure_facts(error: &mongodb::error::Error) -> MongoBulkFailureFacts {
    use mongodb::error::{
        ErrorKind, WriteFailure, NO_WRITES_PERFORMED, RETRYABLE_ERROR, RETRYABLE_WRITE_ERROR,
        SYSTEM_OVERLOADED_ERROR,
    };

    let retryable_label = error.contains_label(RETRYABLE_WRITE_ERROR)
        || (error.contains_label(SYSTEM_OVERLOADED_ERROR) && error.contains_label(RETRYABLE_ERROR));
    let transient_cause = mongo_error_chain_has_transient_cause(error);
    let shape = match error.kind.as_ref() {
        ErrorKind::ServerSelection { .. } | ErrorKind::DnsResolve { .. } => {
            MongoBulkFailureShape::PreCommandTransient
        }
        ErrorKind::InvalidArgument { .. }
        | ErrorKind::Authentication { .. }
        | ErrorKind::BsonSerialization(_)
        | ErrorKind::SessionsNotSupported
        | ErrorKind::InvalidTlsConfig { .. }
        | ErrorKind::IncompatibleServer { .. }
        | ErrorKind::Shutdown => MongoBulkFailureShape::PreCommandTerminal,
        ErrorKind::Io(_) | ErrorKind::ConnectionPoolCleared { .. } => {
            MongoBulkFailureShape::Transport
        }
        ErrorKind::Command(_) => MongoBulkFailureShape::Command,
        ErrorKind::Write(WriteFailure::WriteError(_)) => MongoBulkFailureShape::WriteRejected,
        ErrorKind::Write(WriteFailure::WriteConcernError(_)) => MongoBulkFailureShape::WriteConcern,
        ErrorKind::BulkWrite(bulk) => MongoBulkFailureShape::Bulk {
            partial: bulk.partial_result.is_some(),
            write_errors: !bulk.write_errors.is_empty(),
            write_concern_errors: !bulk.write_concern_errors.is_empty(),
        },
        _ => MongoBulkFailureShape::Unknown,
    };

    MongoBulkFailureFacts {
        no_writes: error.contains_label(NO_WRITES_PERFORMED),
        retryable_signal: retryable_label || transient_cause,
        shape,
    }
}

pub(super) fn mongo_error_chain_has_transient_cause(error: &mongodb::error::Error) -> bool {
    use std::error::Error as _;

    use mongodb::error::ErrorKind;

    let mut current = Some(error);
    while let Some(error) = current {
        if matches!(
            error.kind.as_ref(),
            ErrorKind::Io(_)
                | ErrorKind::ConnectionPoolCleared { .. }
                | ErrorKind::ServerSelection { .. }
                | ErrorKind::DnsResolve { .. }
        ) {
            return true;
        }
        current = error
            .source()
            .and_then(|source| source.downcast_ref::<mongodb::error::Error>());
    }
    false
}

pub(super) fn classify_mongo_bulk_facts(facts: MongoBulkFailureFacts) -> MongoBulkDisposition {
    use MongoBulkDisposition::{DefinitelyNotApplied, OutcomeUnknown};
    use MongoBulkFailureShape::{
        Bulk, Command, Deadline, PreCommandTerminal, PreCommandTransient, Transport, Unknown,
        WriteConcern, WriteRejected,
    };

    let partial_result = matches!(facts.shape, Bulk { partial: true, .. });
    // A nested NoWritesPerformed label only describes the failed wire batch. Earlier wire
    // batches are still applied when the driver reports a partial result on the wrapper error.
    if facts.no_writes && !partial_result {
        let retryable = match facts.shape {
            PreCommandTerminal
            | Bulk {
                write_errors: true, ..
            } => false,
            PreCommandTransient | Transport => true,
            _ => facts.retryable_signal,
        };
        return DefinitelyNotApplied { retryable };
    }

    match facts.shape {
        PreCommandTransient => DefinitelyNotApplied { retryable: true },
        PreCommandTerminal => DefinitelyNotApplied { retryable: false },
        WriteRejected => DefinitelyNotApplied {
            retryable: facts.retryable_signal,
        },
        Bulk {
            partial,
            write_errors: true,
            write_concern_errors,
        } => {
            if partial || write_concern_errors {
                OutcomeUnknown { retryable: false }
            } else {
                DefinitelyNotApplied { retryable: false }
            }
        }
        Transport | Deadline => OutcomeUnknown { retryable: true },
        Command | Bulk { .. } | WriteConcern | Unknown => OutcomeUnknown {
            retryable: facts.retryable_signal,
        },
    }
}

pub(super) fn classify_mongo_bulk_failure(
    context: &str,
    failure: MongoBulkFailure<'_>,
) -> ConnectorError {
    let (facts, detail) = match failure {
        MongoBulkFailure::Driver(error) => (mongo_bulk_failure_facts(error), error.to_string()),
        MongoBulkFailure::Deadline(timeout) => (
            MongoBulkFailureFacts {
                no_writes: false,
                retryable_signal: true,
                shape: MongoBulkFailureShape::Deadline,
            },
            format!("timed out after {timeout:?}"),
        ),
    };

    match classify_mongo_bulk_facts(facts) {
        MongoBulkDisposition::DefinitelyNotApplied { retryable: true } => {
            ConnectorError::WriteError(format!(
                "{context} failed without applying any ordered bulk write: {detail}"
            ))
        }
        MongoBulkDisposition::DefinitelyNotApplied { retryable: false } => {
            ConnectorError::ConfigurationError(format!(
                "{context} was rejected without applying any ordered bulk write: {detail}"
            ))
        }
        MongoBulkDisposition::OutcomeUnknown { retryable } => ConnectorError::outcome_unknown(
            format!(
                "{context} failed after dispatch; MongoDB may have applied part or all of the \
                 ordered bulk: {detail}"
            ),
            retryable,
        ),
    }
}
