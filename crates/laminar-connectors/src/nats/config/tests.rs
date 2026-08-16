use super::*;
use std::collections::HashMap;

fn cfg(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut props = HashMap::new();
    for (k, v) in pairs {
        props.insert((*k).to_string(), (*v).to_string());
    }
    ConnectorConfig::with_properties("nats", props)
}

// ── source ──

#[test]
fn source_jetstream_requires_stream() {
    let err = NatsSourceConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("consumer", "c"),
        ("subject", "x.>"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5040"), "got: {err}");
}

#[test]
fn source_jetstream_requires_consumer() {
    let err = NatsSourceConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("stream", "S"),
        ("subject", "x.>"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5041"), "got: {err}");
}

#[test]
fn source_jetstream_requires_subject_or_filters() {
    let err = NatsSourceConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("stream", "S"),
        ("consumer", "c"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5042"), "got: {err}");
}

#[test]
fn source_core_requires_subject() {
    let err =
        NatsSourceConfig::from_config(&cfg(&[("servers", "nats://a:4222"), ("mode", "core")]))
            .unwrap_err()
            .to_string();
    assert!(err.contains("LDB-5045"), "got: {err}");
}

#[test]
fn source_by_start_sequence_requires_start_sequence() {
    let err = NatsSourceConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("stream", "S"),
        ("consumer", "c"),
        ("subject", "x.>"),
        ("deliver.policy", "by_start_sequence"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5043"), "got: {err}");
}

#[test]
fn source_happy_path_jetstream() {
    let parsed = NatsSourceConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222,nats://b:4222"),
        ("stream", "ORDERS"),
        ("consumer", "laminar-orders"),
        ("subject.filters", "orders.us.*,orders.eu.*"),
        ("ack.wait.ms", "45000"),
        ("max.deliver", "3"),
    ]))
    .unwrap();
    assert_eq!(parsed.servers.len(), 2);
    assert_eq!(parsed.mode, Mode::JetStream);
    assert_eq!(parsed.subject_filters, vec!["orders.us.*", "orders.eu.*"]);
    assert_eq!(parsed.ack_wait, Duration::from_secs(45));
    assert_eq!(parsed.max_deliver, 3);
}

#[test]
fn source_happy_path_core() {
    let parsed = NatsSourceConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("mode", "core"),
        ("subject", "events"),
        ("queue.group", "workers"),
    ]))
    .unwrap();
    assert_eq!(parsed.mode, Mode::Core);
    assert_eq!(parsed.subject.as_deref(), Some("events"));
    assert_eq!(parsed.queue_group.as_deref(), Some("workers"));
}

// ── sink ──

#[test]
fn sink_rejects_subject_and_subject_column() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("subject", "x"),
        ("subject.column", "c"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5050"), "got: {err}");
}

#[test]
fn sink_rejects_neither_subject_nor_column() {
    let err = NatsSinkConfig::from_config(&cfg(&[("servers", "nats://a:4222")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5051"), "got: {err}");
}

#[test]
fn sink_rejects_header_column_colliding_with_reserved() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("subject", "x"),
        ("header.columns", "trace_id,nats-msg-id"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5065"), "got: {err}");
}

#[test]
fn sink_rejects_invalid_header_column_name() {
    for name in ["trace:id", "trŁce"] {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("subject", "x"),
            ("header.columns", name),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5066"), "got: {err}");
    }
}

#[test]
fn sink_rejects_core_with_dedup() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("mode", "core"),
        ("subject", "x"),
        ("dedup.id.column", "event_id"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5053"), "got: {err}");
}

#[test]
fn sink_rejects_dedup_without_stream() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("subject", "x"),
        ("dedup.id.column", "event_id"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5055"), "got: {err}");
}

#[test]
fn jetstream_sink_requires_named_stream() {
    let err = NatsSinkConfig::from_config(&cfg(&[("servers", "nats://a:4222"), ("subject", "x")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5071"), "got: {err}");
}

#[test]
fn jetstream_sink_rejects_blank_stream() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("stream", "   "),
        ("subject", "x"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5071"), "got: {err}");
}

#[test]
fn core_sink_rejects_ignored_stream() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("mode", "core"),
        ("stream", "OUT"),
        ("subject", "x"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5071"), "got: {err}");
}

#[test]
fn sink_happy_path_bounded_dedup() {
    let parsed = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("stream", "OUT"),
        ("subject.column", "out_subject"),
        ("dedup.id.column", "event_id"),
        ("header.columns", "trace_id,tenant"),
    ]))
    .unwrap();
    assert_eq!(parsed.stream.as_deref(), Some("OUT"));
    assert_eq!(parsed.subject, SubjectSpec::Column("out_subject".into()));
    assert_eq!(parsed.dedup_id_column.as_deref(), Some("event_id"));
    let header_columns: Vec<&str> = parsed
        .header_columns
        .iter()
        .map(AsRef::<str>::as_ref)
        .collect();
    assert_eq!(header_columns, vec!["trace_id", "tenant"]);
}

// ── servers ──

#[test]
fn servers_required() {
    let err = NatsSourceConfig::from_config(&cfg(&[]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("servers"), "got: {err}");
}

#[test]
fn zero_fetch_batch_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("fetch.batch", "0")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("fetch.batch must be > 0"), "got: {err}");
}

#[test]
fn zero_ack_wait_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("ack.wait.ms", "0")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("ack.wait.ms must be > 0"), "got: {err}");
}

#[test]
fn max_deliver_unlimited_accepted() {
    let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[("max.deliver", "-1")])).unwrap();
    assert_eq!(parsed.max_deliver, -1);
}

#[test]
fn max_deliver_negative_other_than_minus_one_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("max.deliver", "-5")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("max.deliver"), "got: {err}");
}

#[test]
fn sink_zero_max_pending_rejected() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("subject", "x"),
        ("max.pending", "0"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("max.pending must be > 0"), "got: {err}");
}

#[test]
fn sink_max_pending_is_capped_below_the_client_publish_window() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("subject", "x"),
        ("max.pending", "4097"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("max.pending must be <= 4096"), "got: {err}");
}

#[test]
fn sink_ack_timeout_has_a_fixed_client_side_ceiling() {
    let err = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("subject", "x"),
        ("ack.timeout.ms", "300001"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("ack.timeout.ms must be <= 300000"),
        "got: {err}"
    );
}

#[test]
fn servers_empty_csv_rejected() {
    let err = NatsSourceConfig::from_config(&cfg(&[("servers", ",,")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5030"), "got: {err}");
}

// ── auth ──

fn jetstream_happy(pairs: &[(&str, &str)]) -> ConnectorConfig {
    let mut base = vec![
        ("servers", "nats://a:4222"),
        ("stream", "S"),
        ("consumer", "c"),
        ("subject", "x.>"),
    ];
    base.extend_from_slice(pairs);
    cfg(&base)
}

#[test]
fn auth_user_pass_requires_user() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[
        ("auth.mode", "user_pass"),
        ("password", "secret"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5060"), "got: {err}");
}

#[test]
fn auth_user_pass_requires_password() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[
        ("auth.mode", "user_pass"),
        ("user", "alice"),
    ]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5060"), "got: {err}");
}

#[test]
fn auth_user_pass_happy_path() {
    let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
        ("auth.mode", "user_pass"),
        ("user", "alice"),
        ("password", "wonderland"),
    ]))
    .unwrap();
    assert_eq!(
        parsed.auth,
        AuthMode::UserPass {
            user: "alice".into(),
            password: "wonderland".into(),
        }
    );
}

#[test]
fn auth_token_requires_token() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("auth.mode", "token")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5061"), "got: {err}");
}

#[test]
fn auth_token_happy_path() {
    let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
        ("auth.mode", "token"),
        ("token", "abc123"),
    ]))
    .unwrap();
    assert_eq!(parsed.auth, AuthMode::Token("abc123".into()));
}

#[test]
fn auth_none_with_stray_credentials_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("user", "alice")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5063"), "got: {err}");
}

#[test]
fn auth_creds_file_requires_path() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("auth.mode", "creds_file")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5064"), "got: {err}");
}

#[test]
fn auth_creds_file_happy_path() {
    let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
        ("auth.mode", "creds_file"),
        ("creds.file", "/secrets/user.creds"),
    ]))
    .unwrap();
    assert_eq!(
        parsed.auth,
        AuthMode::CredsFile("/secrets/user.creds".into())
    );
}

#[test]
fn auth_unknown_mode_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[("auth.mode", "banana")]))
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid auth.mode"), "got: {err}");
}

// ── tls ──

#[test]
fn tls_cert_without_key_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[(
        "tls.cert.location",
        "/certs/client.pem",
    )]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5062"), "got: {err}");
}

#[test]
fn tls_key_without_cert_rejected() {
    let err = NatsSourceConfig::from_config(&jetstream_happy(&[(
        "tls.key.location",
        "/certs/client.key",
    )]))
    .unwrap_err()
    .to_string();
    assert!(err.contains("LDB-5062"), "got: {err}");
}

#[test]
fn tls_happy_path() {
    let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
        ("tls.enabled", "true"),
        ("tls.ca.location", "/certs/ca.pem"),
        ("tls.cert.location", "/certs/client.pem"),
        ("tls.key.location", "/certs/client.key"),
    ]))
    .unwrap();
    assert!(parsed.tls.enabled);
    assert_eq!(parsed.tls.ca_location.as_deref(), Some("/certs/ca.pem"));
    assert_eq!(
        parsed.tls.cert_location.as_deref(),
        Some("/certs/client.pem")
    );
}

#[test]
fn auth_and_tls_on_sink() {
    let parsed = NatsSinkConfig::from_config(&cfg(&[
        ("servers", "nats://a:4222"),
        ("stream", "OUT"),
        ("subject", "x"),
        ("auth.mode", "user_pass"),
        ("user", "alice"),
        ("password", "wonderland"),
        ("tls.enabled", "true"),
    ]))
    .unwrap();
    assert!(matches!(parsed.auth, AuthMode::UserPass { .. }));
    assert!(parsed.tls.enabled);
}
