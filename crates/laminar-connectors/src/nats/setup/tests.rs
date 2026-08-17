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
    use jetstream::context::{GetStreamError, GetStreamErrorKind, RequestError, RequestErrorKind};
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
