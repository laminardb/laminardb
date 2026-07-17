use std::error::Error as StdError;
use std::sync::Arc;

use async_nats::jetstream;

use crate::connector::ConnectorTaskOwner;
use crate::error::ConnectorError;

pub(super) fn track_connection_tasks(
    options: async_nats::ConnectOptions,
    owner: &ConnectorTaskOwner,
    connector: &'static str,
) -> Result<async_nats::ConnectOptions, ConnectorError> {
    let terminal_guard = Arc::new(owner.track().ok_or_else(|| {
        ConnectorError::Internal(format!(
            "NATS {connector} task generation is already retired"
        ))
    })?);

    // async-nats does not expose its connection/event task handles. Its event callback is owned
    // by the event task, which exits only after the connection handler drops its event sender.
    // Retaining this guard in that callback therefore gives the runtime a supported terminal
    // signal without depending on a timer.
    Ok(options.event_callback(move |_| {
        let terminal_guard = Arc::clone(&terminal_guard);
        async move {
            let _terminal_guard = terminal_guard;
        }
    }))
}

pub(super) fn classify_connect_error(error: &async_nats::ConnectError) -> ConnectorError {
    use async_nats::ConnectErrorKind;

    let detail = format!("nats connect: {error}");
    match error.kind() {
        ConnectErrorKind::Dns
        | ConnectErrorKind::TimedOut
        | ConnectErrorKind::Io
        | ConnectErrorKind::MaxReconnects => ConnectorError::ConnectionFailed(detail),
        ConnectErrorKind::ServerParse
        | ConnectErrorKind::Authentication
        | ConnectErrorKind::AuthorizationViolation
        | ConnectErrorKind::Tls => ConnectorError::ConfigurationError(detail),
    }
}

pub(super) fn classify_get_stream_error(
    error: &jetstream::context::GetStreamError,
    stream_name: &str,
) -> ConnectorError {
    use jetstream::context::{GetStreamErrorKind, RequestError};

    let retryable = match error.kind() {
        GetStreamErrorKind::Request => StdError::source(error)
            .and_then(|source| source.downcast_ref::<RequestError>())
            .is_some_and(request_error_is_retryable),
        GetStreamErrorKind::JetStream(broker_error) => broker_error_is_retryable(&broker_error),
        GetStreamErrorKind::EmptyName | GetStreamErrorKind::InvalidStreamName => false,
    };
    setup_result(
        format!("get JetStream stream '{stream_name}': {error}"),
        retryable,
    )
}

pub(super) fn classify_create_consumer_error(
    error: &jetstream::stream::ConsumerError,
    consumer_name: &str,
) -> ConnectorError {
    use jetstream::stream::ConsumerErrorKind;
    use jetstream::ErrorCode;

    let drift = match error.kind() {
        ConsumerErrorKind::JetStream(server_error) => matches!(
            server_error.error_code(),
            ErrorCode::CONSUMER_ALREADY_EXISTS | ErrorCode::CONSUMER_NAME_EXIST
        ),
        _ => false,
    };
    if drift {
        return ConnectorError::ConfigurationError(format!(
            "[LDB-5070] consumer '{consumer_name}' exists with incompatible config; \
             rotate the durable name or delete the consumer out-of-band. \
             Server said: {error}"
        ));
    }

    let retryable = match error.kind() {
        ConsumerErrorKind::TimedOut => true,
        ConsumerErrorKind::Request => StdError::source(error)
            .and_then(|source| source.downcast_ref::<jetstream::context::RequestError>())
            .is_some_and(request_error_is_retryable),
        ConsumerErrorKind::JetStream(server_error) => broker_error_is_retryable(&server_error),
        ConsumerErrorKind::InvalidConsumerType
        | ConsumerErrorKind::InvalidName
        | ConsumerErrorKind::Other => false,
    };
    setup_result(
        format!("create JetStream consumer '{consumer_name}': {error}"),
        retryable,
    )
}

pub(super) fn classify_subscribe_error(
    error: &async_nats::SubscribeError,
    operation: &str,
) -> ConnectorError {
    use async_nats::SubscribeErrorKind;

    let detail = format!("{operation}: {error}");
    match error.kind() {
        SubscribeErrorKind::Other => ConnectorError::ConnectionFailed(detail),
        SubscribeErrorKind::InvalidSubject | SubscribeErrorKind::InvalidQueueName => {
            ConnectorError::ConfigurationError(detail)
        }
    }
}

fn broker_error_is_retryable(error: &jetstream::Error) -> bool {
    matches!(error.code(), 408 | 429 | 500..=599)
}

fn request_error_is_retryable(error: &jetstream::context::RequestError) -> bool {
    use jetstream::context::RequestErrorKind;

    match error.kind() {
        RequestErrorKind::TimedOut | RequestErrorKind::NoResponders => true,
        RequestErrorKind::InvalidSubject => false,
        RequestErrorKind::Other => StdError::source(error)
            .and_then(classify_request_error_source)
            .unwrap_or(false),
    }
}

fn classify_request_error_source(error: &(dyn StdError + 'static)) -> Option<bool> {
    if let Some(error) = error.downcast_ref::<jetstream::context::RequestError>() {
        return Some(request_error_is_retryable(error));
    }
    if let Some(error) = error.downcast_ref::<async_nats::RequestError>() {
        return Some(match error.kind() {
            async_nats::RequestErrorKind::TimedOut | async_nats::RequestErrorKind::NoResponders => {
                true
            }
            async_nats::RequestErrorKind::InvalidSubject => false,
            async_nats::RequestErrorKind::Other => StdError::source(error)
                .and_then(classify_request_error_source)
                .unwrap_or(false),
        });
    }
    if let Some(error) = error.downcast_ref::<async_nats::PublishError>() {
        return Some(matches!(
            error.kind(),
            async_nats::client::PublishErrorKind::Send
        ));
    }
    if let Some(error) = error.downcast_ref::<async_nats::SubscribeError>() {
        return Some(matches!(
            error.kind(),
            async_nats::SubscribeErrorKind::Other
        ));
    }
    if let Some(error) = error.downcast_ref::<jetstream::context::PublishError>() {
        use jetstream::context::PublishErrorKind;

        return Some(match error.kind() {
            PublishErrorKind::TimedOut | PublishErrorKind::BrokenPipe => true,
            PublishErrorKind::Other => StdError::source(error)
                .and_then(classify_request_error_source)
                .unwrap_or(false),
            PublishErrorKind::StreamNotFound
            | PublishErrorKind::WrongLastMessageId
            | PublishErrorKind::WrongLastSequence
            | PublishErrorKind::MaxAckPending => false,
        });
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        use std::io::ErrorKind;

        return Some(matches!(
            error.kind(),
            ErrorKind::BrokenPipe
                | ErrorKind::ConnectionAborted
                | ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
                | ErrorKind::Interrupted
                | ErrorKind::NotConnected
                | ErrorKind::TimedOut
                | ErrorKind::UnexpectedEof
                | ErrorKind::WouldBlock
        ));
    }
    if error.is::<serde_json::Error>() || error.is::<async_nats::SubjectError>() {
        return Some(false);
    }
    if let Some(error) = error.downcast_ref::<jetstream::Error>() {
        return Some(broker_error_is_retryable(error));
    }

    StdError::source(error).and_then(classify_request_error_source)
}

fn setup_result(detail: String, retryable: bool) -> ConnectorError {
    if retryable {
        ConnectorError::ReadError(detail)
    } else {
        ConnectorError::ConfigurationError(detail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker_error(code: u16) -> jetstream::Error {
        serde_json::from_value(serde_json::json!({
            "code": code,
            "err_code": 10008,
            "description": "test response"
        }))
        .unwrap()
    }

    #[test]
    fn connection_callback_retains_generation_guard() {
        let (owner, tracker) = ConnectorTaskOwner::new();
        let options =
            track_connection_tasks(async_nats::ConnectOptions::new(), &owner, "test").unwrap();

        drop(owner);
        assert!(!tracker.is_terminated());
        drop(options);
        assert!(tracker.is_terminated());
    }

    #[test]
    fn setup_classification_table_is_fail_closed() {
        use async_nats::ConnectErrorKind;
        use jetstream::context::{
            GetStreamError, GetStreamErrorKind, RequestError, RequestErrorKind,
        };
        use jetstream::stream::{ConsumerError, ConsumerErrorKind};

        for kind in [
            ConnectErrorKind::Dns,
            ConnectErrorKind::TimedOut,
            ConnectErrorKind::Io,
            ConnectErrorKind::MaxReconnects,
        ] {
            assert!(classify_connect_error(&async_nats::ConnectError::new(kind)).is_transient());
        }
        for kind in [
            ConnectErrorKind::Authentication,
            ConnectErrorKind::AuthorizationViolation,
            ConnectErrorKind::Tls,
            ConnectErrorKind::ServerParse,
        ] {
            assert!(!classify_connect_error(&async_nats::ConnectError::new(kind)).is_transient());
        }

        for kind in [RequestErrorKind::TimedOut, RequestErrorKind::NoResponders] {
            let error =
                GetStreamError::with_source(GetStreamErrorKind::Request, RequestError::new(kind));
            assert!(classify_get_stream_error(&error, "EVENTS").is_transient());
        }

        let broken_pipe = GetStreamError::with_source(
            GetStreamErrorKind::Request,
            RequestError::with_source(
                RequestErrorKind::Other,
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "connection closed"),
            ),
        );
        assert!(classify_get_stream_error(&broken_pipe, "EVENTS").is_transient());

        let client_send = async_nats::RequestError::with_source(
            async_nats::RequestErrorKind::Other,
            async_nats::PublishError::new(async_nats::client::PublishErrorKind::Send),
        );
        let nested_client = GetStreamError::with_source(
            GetStreamErrorKind::Request,
            RequestError::with_source(RequestErrorKind::Other, client_send),
        );
        assert!(classify_get_stream_error(&nested_client, "EVENTS").is_transient());

        let malformed = serde_json::from_slice::<serde_json::Value>(b"{").unwrap_err();
        let protocol_error = GetStreamError::with_source(
            GetStreamErrorKind::Request,
            RequestError::with_source(RequestErrorKind::Other, malformed),
        );
        assert!(!classify_get_stream_error(&protocol_error, "EVENTS").is_transient());

        let unknown = GetStreamError::with_source(
            GetStreamErrorKind::Request,
            RequestError::with_source(RequestErrorKind::Other, std::fmt::Error),
        );
        assert!(!classify_get_stream_error(&unknown, "EVENTS").is_transient());
        for code in [408, 429, 500, 503, 599] {
            let error = GetStreamError::new(GetStreamErrorKind::JetStream(broker_error(code)));
            assert!(classify_get_stream_error(&error, "EVENTS").is_transient());
        }
        for code in [400, 401, 403, 404, 422] {
            let error = GetStreamError::new(GetStreamErrorKind::JetStream(broker_error(code)));
            assert!(!classify_get_stream_error(&error, "EVENTS").is_transient());
        }

        let timed_out = ConsumerError::new(ConsumerErrorKind::TimedOut);
        assert!(classify_create_consumer_error(&timed_out, "worker").is_transient());
        let no_responders = ConsumerError::with_source(
            ConsumerErrorKind::Request,
            RequestError::new(RequestErrorKind::NoResponders),
        );
        assert!(classify_create_consumer_error(&no_responders, "worker").is_transient());
        for kind in [
            ConsumerErrorKind::InvalidConsumerType,
            ConsumerErrorKind::InvalidName,
            ConsumerErrorKind::Other,
        ] {
            assert!(
                !classify_create_consumer_error(&ConsumerError::new(kind), "worker").is_transient()
            );
        }
    }
}
