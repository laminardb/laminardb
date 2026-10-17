use super::*;

fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("ibmmq");
    for (key, value) in pairs {
        config.set(*key, *value);
    }
    config
}

fn source(pairs: &[(&str, &str)]) -> Result<IbmMqSourceConfig, ConnectorError> {
    let mut all = vec![("queue", "DEV.QUEUE.1")];
    all.extend_from_slice(pairs);
    IbmMqSourceConfig::from_config(&config(&all))
}

fn sink(pairs: &[(&str, &str)]) -> Result<IbmMqSinkConfig, ConnectorError> {
    let mut all = vec![("queue", "DEV.QUEUE.1")];
    all.extend_from_slice(pairs);
    IbmMqSinkConfig::from_config(&config(&all))
}

fn message(result: Result<impl std::fmt::Debug, ConnectorError>) -> String {
    result
        .expect_err("configuration must be rejected")
        .to_string()
}

#[test]
fn source_defaults_are_bounded_and_byte_preserving() {
    let parsed = source(&[]).unwrap();
    assert_eq!(parsed.connection.endpoint, Endpoint::ClientEnvironment);
    assert_eq!(parsed.connection.reconnect, Reconnect::Disabled);
    assert_eq!(parsed.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
    assert_eq!(parsed.max_uow_messages, DEFAULT_MAX_UOW_MESSAGES);
    assert_eq!(parsed.wait_interval, DEFAULT_WAIT);
    assert!(!parsed.convert);
    assert_eq!(parsed.on_decode_error, DecodeErrorPolicy::Fail);
    assert_eq!(parsed.identity()["queue"], "DEV.QUEUE.1");
}

#[test]
fn endpoint_selection_is_exclusive() {
    let channel = source(&[
        ("channel", "DEV.APP.SVRCONN"),
        ("connection.name", "mq(1414),mq2(1414)"),
    ])
    .unwrap();
    assert!(matches!(
        channel.connection.endpoint,
        Endpoint::Channel { .. }
    ));
    assert!(message(source(&[("channel", "C")])).contains("together"));
    assert!(message(source(&[
        ("channel", "C"),
        ("connection.name", "h(1)"),
        ("ccdt.url", "file:///ccdt.json")
    ]))
    .contains("not both"));
    let ccdt = source(&[("ccdt.url", "file:///ccdt.json")]).unwrap();
    assert_eq!(
        ccdt.connection.endpoint,
        Endpoint::Ccdt("file:///ccdt.json".into())
    );
    assert!(message(source(&[
        ("channel", &"C".repeat(21)),
        ("connection.name", "h(1)")
    ]))
    .contains("20"));
}

#[test]
fn source_reconnect_and_limits_fail_closed() {
    assert!(message(source(&[("reconnect", "qmgr")])).contains("disabled"));
    assert!(sink(&[("reconnect", "qmgr")]).is_ok());
    assert!(message(source(&[("max.message.bytes", "0")])).contains("positive"));
    assert!(message(source(&[("max.message.bytes", "209715200")])).contains("limit"));
    assert!(message(source(&[
        ("max.message.bytes", "100"),
        ("max.batch.bytes", "10")
    ]))
    .contains("<="));
    assert!(message(source(&[("wait.interval.ms", "60000")])).contains("between"));
    assert!(message(source(&[("on.decode.error", "drop")])).contains("on.decode.error"));
    assert!(message(source(&[("format", "avro")])).contains("not qualified"));
    assert!(message(source(&[("selector", "x")])).contains("unknown IBM MQ source"));
}

#[test]
fn credentials_and_tls_are_validated_and_redacted() {
    let parsed = source(&[
        ("username", "app"),
        ("password", "s3cret"),
        ("tls.cipher.spec", "ANY_TLS13_OR_HIGHER"),
        ("tls.key.repository", "/keys/client"),
        ("tls.key.repository.password", "k3y"),
        ("tls.peer.name", "CN=QM1"),
        ("channel", "C"),
        ("connection.name", "h(1)"),
    ])
    .unwrap();
    let rendered = format!("{:?}", parsed.connection);
    assert!(
        !rendered.contains("s3cret") && !rendered.contains("k3y"),
        "{rendered}"
    );
    assert!(rendered.contains("CN=QM1"));
    assert!(message(source(&[("password", "x")])).contains("requires 'username'"));
    assert!(message(source(&[("tls.peer.name", "CN=QM1")])).contains("tls.cipher.spec"));
    assert!(message(source(&[
        ("tls.cipher.spec", "ANY"),
        ("ccdt.url", "file:///c.json")
    ]))
    .contains("ccdt"));
    assert!(message(source(&[("application.name", &"a".repeat(29))])).contains("28"));
}

#[test]
fn sink_descriptor_fields_use_native_units_and_ranges() {
    let parsed = sink(&[
        ("priority.column", "p"),
        ("message.id.column", "id"),
        ("property.columns", "a,b"),
        ("expiry", "600"),
        ("persistence", "not-persistent"),
    ])
    .unwrap();
    assert_eq!(parsed.property_columns(), vec!["p", "id", "a", "b"]);
    assert_eq!(parsed.expiry, Some(600));
    assert_eq!(parsed.persistence, Persistence::NotPersistent);
    assert_eq!(parsed.ccsid, 1208);
    assert!(message(sink(&[("priority", "10")])).contains("0 and 9"));
    assert!(message(sink(&[("priority", "1"), ("priority.column", "p")])).contains("not both"));
    assert!(message(sink(&[("expiry", "0")])).contains("tenths"));
    assert!(message(sink(&[("message.format", "TOOLONGFMT")])).contains("8 characters"));
    assert!(message(sink(&[
        ("message.id.column", "x"),
        ("property.columns", "x")
    ]))
    .contains("more than one"));
}
