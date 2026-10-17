use super::*;

fn config(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut config = ConnectorConfig::new("rabbitmq");
    for (key, value) in pairs {
        config.set(*key, *value);
    }
    config
}

fn source(pairs: &[(&str, &str)]) -> Result<RabbitMqSourceConfig, ConnectorError> {
    let mut all = vec![("url", "amqp://127.0.0.1:5672/%2f"), ("queue", "orders")];
    all.extend_from_slice(pairs);
    RabbitMqSourceConfig::from_config(&config(&all))
}

fn sink(pairs: &[(&str, &str)]) -> Result<RabbitMqSinkConfig, ConnectorError> {
    let mut all = vec![("url", "amqp://127.0.0.1:5672/%2f")];
    all.extend_from_slice(pairs);
    RabbitMqSinkConfig::from_config(&config(&all))
}

fn message(result: Result<impl std::fmt::Debug, ConnectorError>) -> String {
    result
        .expect_err("configuration must be rejected")
        .to_string()
}

#[test]
fn source_defaults_use_existing_queue_and_bounded_prefetch() {
    let parsed = source(&[]).unwrap();
    assert_eq!(parsed.queue, "orders");
    assert!(parsed.declare.is_none());
    assert_eq!(parsed.prefetch, 1_000);
    assert_eq!(parsed.max_message_bytes, DEFAULT_MAX_MESSAGE_BYTES);
    assert_eq!(parsed.on_decode_error, DecodeErrorPolicy::Fail);
    assert_eq!(parsed.format, Format::Json);
    assert_eq!(parsed.connection.endpoints[0].vhost, "/");
    assert_eq!(parsed.identity()["queue"], "orders");
}

#[test]
fn runtime_keys_are_accepted_and_unknown_keys_rejected() {
    assert!(source(&[("laminar.source.name", "s"), ("format", "csv")]).is_ok());
    assert!(message(source(&[("prefetch", "10")])).contains("unknown RabbitMQ source property"));
    assert!(message(sink(&[("routing.key", ""), ("queue", "q")])).contains("unknown RabbitMQ sink"));
}

#[test]
fn declaration_settings_require_opt_in_declaration() {
    assert!(message(source(&[("queue.type", "quorum")])).contains("queue.declare"));
    let declared = source(&[("queue.declare", "true"), ("queue.type", "quorum")]).unwrap();
    let declaration = declared.declare.unwrap();
    assert!(declaration.durable);
    assert_eq!(
        declaration
            .arguments
            .inner()
            .get(&ShortString::from("x-queue-type")),
        Some(&AMQPValue::LongString("quorum".into()))
    );
    assert!(message(source(&[
        ("queue.declare", "true"),
        ("queue.type", "quorum"),
        ("queue.arguments", r#"{"x-queue-type":"classic"}"#),
    ]))
    .contains("both set"));
    assert!(message(source(&[
        ("queue.declare", "true"),
        ("queue.type", "stream")
    ]))
    .contains("stream queues"));
}

#[test]
fn json_arguments_keep_native_scalar_types() {
    let parsed = source(&[
        ("queue.declare", "true"),
        (
            "queue.arguments",
            r#"{"x-max-priority":10,"x-dead-letter-exchange":"dlx","flag":true,"ratio":0.5}"#,
        ),
    ])
    .unwrap();
    let arguments = parsed.declare.unwrap().arguments;
    let get = |key: &str| arguments.inner().get(&ShortString::from(key)).cloned();
    assert_eq!(get("x-max-priority"), Some(AMQPValue::LongLongInt(10)));
    assert_eq!(
        get("x-dead-letter-exchange"),
        Some(AMQPValue::LongString("dlx".into()))
    );
    assert_eq!(get("flag"), Some(AMQPValue::Boolean(true)));
    assert_eq!(get("ratio"), Some(AMQPValue::Double(0.5)));
    assert!(message(source(&[("consumer.arguments", "[1]")])).contains("JSON object"));
    assert!(
        message(source(&[("consumer.arguments", r#"{"x":{"y":1}}"#)])).contains("string, number")
    );
}

#[test]
fn unbounded_or_inconsistent_limits_are_rejected() {
    assert!(message(source(&[("prefetch.count", "0")])).contains("prefetch.count"));
    assert!(message(source(&[("prefetch.count", "70000")])).contains("invalid value"));
    assert!(message(source(&[
        ("max.message.bytes", "10"),
        ("max.batch.bytes", "5")
    ]))
    .contains("max.batch.bytes"));
    assert!(message(source(&[("on.decode.error", "skip")])).contains("on.decode.error"));
    assert!(message(source(&[("format", "avro")])).contains("not qualified"));
    assert!(message(source(&[("queue.bind.routing.keys", "a")])).contains("queue.bind.exchange"));
}

#[test]
fn bindings_default_to_the_empty_routing_key() {
    let parsed = source(&[("queue.bind.exchange", "amq.fanout")]).unwrap();
    assert_eq!(parsed.binding.unwrap().routing_keys, vec![String::new()]);
}

#[test]
fn credentials_have_one_source_and_are_redacted() {
    let parsed = source(&[("username", "laminar"), ("password", "s3cret")]).unwrap();
    let userinfo = &parsed.connection.endpoints[0].authority.userinfo;
    assert_eq!(
        (userinfo.username.as_str(), userinfo.password.as_str()),
        ("laminar", "s3cret")
    );
    assert!(!format!("{:?}", parsed.connection).contains("s3cret"));
    let mut pairs = vec![
        ("url", "amqp://u:p@127.0.0.1/%2f"),
        ("queue", "q"),
        ("username", "x"),
    ];
    assert!(message(RabbitMqSourceConfig::from_config(&config(&pairs))).contains("both"));
    pairs.pop();
    pairs.push(("password", "only"));
    assert!(message(RabbitMqSourceConfig::from_config(&config(&pairs))).contains("requires"));
}

#[test]
fn endpoint_and_tls_settings_must_agree() {
    assert!(message(source(&[("tls.ca.location", "/ca.pem")])).contains("amqps"));
    assert!(message(source(&[("tls.cert.location", "/c.pem")])).contains("together"));
    let mut pairs = vec![
        (
            "url",
            "amqps://a.example:5671/%2f,amqps://b.example:5671/%2f",
        ),
        ("queue", "q"),
        ("tls.ca.location", "/ca.pem"),
    ];
    let parsed = RabbitMqSourceConfig::from_config(&config(&pairs)).unwrap();
    assert_eq!(parsed.connection.endpoints.len(), 2);
    pairs[0] = ("url", "amqp://a/one,amqp://b/two");
    pairs.pop();
    assert!(message(RabbitMqSourceConfig::from_config(&config(&pairs))).contains("virtual host"));
    let conflicting = vec![
        ("url", "amqp://a/%2f?connection_timeout=2000"),
        ("queue", "q"),
        ("connect.timeout.ms", "1000"),
    ];
    assert!(message(RabbitMqSourceConfig::from_config(&config(&conflicting))).contains("both"));
    let uri_timeout = RabbitMqSourceConfig::from_config(&config(&conflicting[..2])).unwrap();
    assert_eq!(
        uri_timeout.connection.connect_timeout,
        Duration::from_millis(2_000)
    );
}

#[test]
fn sink_routing_and_properties_are_explicit() {
    assert!(message(sink(&[])).contains("routing.key"));
    assert!(
        message(sink(&[("routing.key", "a"), ("routing.key.column", "k")])).contains("not both")
    );
    let parsed = sink(&[
        ("routing.key.column", "k"),
        ("message.id.column", "id"),
        ("header.columns", "h1,h2"),
        ("priority.column", "p"),
        ("delivery.mode", "transient"),
        ("expiration.ms", "60000"),
    ])
    .unwrap();
    assert_eq!(parsed.property_columns(), vec!["k", "p", "id", "h1", "h2"]);
    assert_eq!(parsed.delivery_mode, DeliveryMode::Transient);
    assert_eq!(parsed.expiration_ms, Some(60_000));
    assert!(parsed.mandatory);
    assert!(message(sink(&[
        ("routing.key.column", "k"),
        ("message.id.column", "k")
    ]))
    .contains("more than one"));
    assert!(message(sink(&[("routing.key", ""), ("priority", "256")])).contains("invalid value"));
}

#[test]
fn exchange_declaration_is_opt_in_and_typed() {
    assert!(
        message(sink(&[("routing.key", ""), ("exchange.type", "topic")]))
            .contains("exchange.declare")
    );
    assert!(
        message(sink(&[("routing.key", ""), ("exchange.declare", "true")])).contains("default")
    );
    assert!(message(sink(&[
        ("routing.key", ""),
        ("exchange", "events"),
        ("exchange.declare", "true"),
    ]))
    .contains("exchange.type"));
    let parsed = sink(&[
        ("routing.key", ""),
        ("exchange", "events"),
        ("exchange.declare", "true"),
        ("exchange.type", "topic"),
    ])
    .unwrap();
    assert_eq!(
        parsed.exchange_declare,
        Some(ExchangeDeclaration {
            kind: "topic".into(),
            durable: true
        })
    );
}

#[test]
fn malformed_urls_never_echo_credentials() {
    for url in [
        "amqp://user:hunter2@[bad",
        "amqp://user:hunter2@host:notaport/%2f",
        "amqpx://user:hunter2@h/",
    ] {
        let error = message(RabbitMqSourceConfig::from_config(&config(&[
            ("url", url),
            ("queue", "q"),
        ])));
        assert!(!error.contains("hunter2"), "{error}");
    }
}

#[test]
fn fpml_options_need_the_fpml_source_format() {
    assert!(message(source(&[("fpml.profile", "x")])).contains("unknown RabbitMQ source"));
    assert!(message(sink(&[("routing.key", "r"), ("format", "fpml")])).contains("not qualified"));
    let fpml = source(&[("format", "fpml"), ("fpml.max.document.bytes", "1024")]);
    if cfg!(feature = "fpml") {
        assert_eq!(fpml.unwrap().format, Format::Fpml);
        assert!(message(source(&[("format", "fpml"), ("fpml.profle", "x")]))
            .contains("unknown RabbitMQ source"));
    } else {
        assert!(message(fpml).contains("requires a build with the fpml feature"));
    }
}
