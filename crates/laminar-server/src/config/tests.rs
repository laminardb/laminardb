use super::*;

#[test]
fn shipped_server_configs_deserialize() {
    for (name, input) in [
        (
            "minimal",
            include_str!("../../../../examples/laminardb-minimal.toml"),
        ),
        (
            "standalone",
            include_str!("../../../../examples/laminardb.toml"),
        ),
        (
            "cluster",
            include_str!("../../../../examples/laminardb-cluster.toml"),
        ),
        (
            "binance-1",
            include_str!("../../../../examples/binance-cluster-node1.toml"),
        ),
        (
            "binance-2",
            include_str!("../../../../examples/binance-cluster-node2.toml"),
        ),
        (
            "bluesky-firehose",
            include_str!("../../../../examples/bluesky-firehose/laminar.toml"),
        ),
        (
            "bluesky-news",
            include_str!("../../../../examples/bluesky-news/laminar.toml"),
        ),
        (
            "aiops",
            include_str!("../../../../examples/claude-code-aiops/config.toml"),
        ),
        (
            "iceberg",
            include_str!("../../../../examples/kafka-iceberg-timeseries/laminar.toml"),
        ),
        (
            "nats",
            include_str!("../../../../examples/nats-payments/config.toml"),
        ),
        (
            "server-demo",
            include_str!("../../../../examples/server-demo/laminardb.toml"),
        ),
    ] {
        toml::from_str::<ServerConfig>(input)
            .unwrap_or_else(|error| panic!("{name} config does not deserialize: {error}"));
    }
}

const AI_TOML: &str = r#"
[server]

[ai.providers.anthropic]
api_key_env = "LAMINAR_ANTHROPIC_API_KEY"
base_url = "https://api.anthropic.com"
max_concurrency = 8

[ai.providers.openai]
api_key_env = "LAMINAR_OPENAI_API_KEY"
base_url = "https://api.openai.com/v1"

[ai.providers.local]
cache_dir = "/var/lib/laminar/models"

[models.finbert]
kind = "local"
source = "hf:onnx-community/finbert"
task = "classify"

[models.haiku]
kind = "remote"
provider = "anthropic"
model = "claude-haiku-4-5-20251001"
task = ["classify", "extract", "complete"]

[ai.defaults]
classify = "finbert"
complete = "haiku"
"#;

fn canonical_http_auth_secret(byte: u8) -> Secret {
    Secret::new(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([byte; 32]))
}

fn diagnostic_auth_config(
    diagnostic_read_token: Secret,
    console_token: Option<Secret>,
) -> ServerConfig {
    let mut config: ServerConfig = toml::from_str("[server]\n").unwrap();
    config.server.mode = ServerMode::Cluster;
    config.server.bind = "127.0.0.1:8080".to_string();
    config.server.console_token = console_token;
    config.server.diagnostic_read_token = Some(diagnostic_read_token);
    config
}

fn http_auth_errors(config: &ServerConfig) -> Vec<String> {
    match validate_http_auth(config).unwrap_err() {
        ConfigError::ValidationErrors { errors } => errors,
        error => panic!("expected validation errors, got {error:?}"),
    }
}

#[test]
fn parses_ai_section_and_models() {
    let config: ServerConfig = toml::from_str(AI_TOML).unwrap();
    assert_eq!(config.ai.providers.len(), 3);
    assert_eq!(
        config.ai.providers["anthropic"].api_key_env.as_deref(),
        Some("LAMINAR_ANTHROPIC_API_KEY")
    );
    assert_eq!(config.ai.providers["openai"].max_concurrency, 8);
    assert_eq!(
        config.ai.providers["local"].cache_dir.as_deref(),
        Some("/var/lib/laminar/models")
    );
    assert_eq!(config.models["finbert"].task.tasks(), vec!["classify"]);
    assert_eq!(
        config.models["haiku"].task.tasks(),
        vec!["classify", "extract", "complete"]
    );
    assert_eq!(config.ai.defaults["classify"], "finbert");
    validate_config(&config).unwrap();
}

#[test]
fn rejects_local_provider_without_cache_dir() {
    let toml = r#"
[server]
[ai.providers.local]
[models.m]
kind = "local"
source = "hf:x/y"
task = "classify"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    assert!(validate_config(&config).is_err());
}

#[test]
fn rejects_unknown_provider_and_default() {
    let toml = r#"
[server]
[ai.providers.anthropic]
api_key_env = "K"
[models.bad]
kind = "remote"
provider = "ghost"
model = "x"
task = "classify"
[ai.defaults]
classify = "missing"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("unknown provider 'ghost'"), "{msg}");
    assert!(msg.contains("unknown model 'missing'"), "{msg}");
}

#[test]
fn rejects_remote_provider_without_api_key_env() {
    let toml = r#"
[server]
[ai.providers.openai]
base_url = "http://localhost:8000/v1"
[models.m]
kind = "remote"
provider = "openai"
model = "x"
task = "embed"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    assert!(format!("{err:?}").contains("requires 'api_key_env'"));
}

#[test]
fn local_model_requires_source() {
    let toml = r#"
[server]
[models.m]
kind = "local"
task = "classify"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    assert!(format!("{err:?}").contains("requires a 'source'"));
}

#[test]
fn test_parse_minimal_config() {
    let toml = "[server]\n";
    let config: ServerConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.server.mode, ServerMode::Single);
    assert_eq!(config.server.bind, "127.0.0.1:8080");
    assert!(config.server.incremental_emit);
    assert_eq!(config.server.delivery, DeliveryGuarantee::AtLeastOnce);
    assert!(config.sources.is_empty());
    assert!(config.pipelines.is_empty());
    assert!(config.sinks.is_empty());
}

#[test]
fn parse_error_does_not_retain_substituted_input() {
    const SENTINEL: &str = "LDB_PARSE_SECRET_SENTINEL_4f8757d46e";
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid-secret.toml");
    let input =
        format!("[server]\nconsole_token = ${{LDB_PARSE_REDACTION_TEST_TOKEN:-{SENTINEL}}}\n");
    std::fs::write(&path, input).unwrap();

    let error = load_config(&path).expect_err("the unquoted substituted token is invalid TOML");
    assert!(matches!(&error, ConfigError::ParseError { .. }));
    assert!(!error.to_string().contains(SENTINEL));
    assert!(!format!("{error:?}").contains(SENTINEL));

    let mut source = std::error::Error::source(&error);
    assert!(
        source.is_some(),
        "parse errors must retain their typed source"
    );
    while let Some(cause) = source {
        assert!(!cause.to_string().contains(SENTINEL));
        assert!(!format!("{cause:?}").contains(SENTINEL));
        source = cause.source();
    }
}

#[test]
fn test_server_mode_rejects_unknown_values() {
    let error = toml::from_str::<ServerConfig>("[server]\nmode = \"cluser\"\n")
        .expect_err("a mistyped runtime mode must not fall back to single-node mode");
    let message = error.to_string();
    assert!(message.contains("unknown variant"), "{message}");
    assert!(
        message.contains("single") && message.contains("cluster"),
        "{message}"
    );
    assert!(!message.contains("embedded"), "{message}");
}

#[test]
fn test_server_mode_rejects_retired_embedded_value() {
    let error = toml::from_str::<ServerConfig>("[server]\nmode = \"embedded\"\n")
        .expect_err("the standalone server mode is named single");
    let message = error.to_string();
    assert!(message.contains("unknown variant"), "{message}");
    assert!(
        message.contains("single") && message.contains("cluster"),
        "{message}"
    );
}

#[test]
fn test_removed_coordination_section_is_rejected() {
    let error = toml::from_str::<ServerConfig>(
        "[server]\nmode = \"cluster\"\n[coordination]\nstrategy = \"raft\"\n",
    )
    .expect_err("removed coordination settings must not be silently ignored");
    assert!(error.to_string().contains("unknown field"), "{error}");
    assert!(error.to_string().contains("coordination"), "{error}");
}

#[test]
fn test_removed_discovery_key_is_rejected() {
    let error = toml::from_str::<ServerConfig>(
            "[server]\nmode = \"cluster\"\n[discovery]\nstrategy = \"gossip\"\nraft_address = \"127.0.0.1:9001\"\n",
        )
        .expect_err("retired discovery settings must not be silently ignored");
    assert!(error.to_string().contains("unknown field"), "{error}");
    assert!(error.to_string().contains("raft_address"), "{error}");
}

#[test]
fn test_parse_full_single_config() {
    let toml = r#"
[server]
mode = "single"
bind = "127.0.0.1:8080"

[checkpoint]
url = "file:///tmp/checkpoints"
interval = "10s"

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "localhost:9092"
"group.id" = "laminardb-trades"
topic = "trades"
[[source.schema]]
name = "symbol"
type = "VARCHAR"
nullable = false
[[source.schema]]
name = "price"
type = "DOUBLE"
[source.watermark]
column = "trade_time"
max_out_of_orderness = "5s"

[[pipeline]]
name = "vwap"
sql = "SELECT symbol, SUM(price) FROM trades GROUP BY symbol"

[[sink]]
name = "output"
pipeline = "vwap"
connector = "kafka"
format = "json"
[sink.properties]
"bootstrap.servers" = "localhost:9092"
topic = "vwap_output"
"#;

    let config: ServerConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.sources.len(), 1);
    assert_eq!(config.sources[0].name, "trades");
    assert_eq!(
        config.sources[0]
            .properties
            .get("bootstrap.servers")
            .and_then(toml::Value::as_str),
        Some("localhost:9092")
    );
    assert_eq!(
        config.sources[0]
            .properties
            .get("group.id")
            .and_then(toml::Value::as_str),
        Some("laminardb-trades")
    );
    assert_eq!(config.sources[0].schema.len(), 2);
    assert!(!config.sources[0].schema[0].nullable);
    assert!(config.sources[0].schema[1].nullable); // default true
    assert!(config.sources[0].watermark.is_some());
    assert_eq!(config.pipelines.len(), 1);
    assert_eq!(config.sinks.len(), 1);
    assert_eq!(config.sinks[0].pipeline, "vwap");
    assert_eq!(config.sinks[0].format.as_deref(), Some("json"));
    assert_eq!(
        config.sinks[0]
            .properties
            .get("bootstrap.servers")
            .and_then(toml::Value::as_str),
        Some("localhost:9092")
    );

    validate_config(&config).unwrap();
}

#[test]
fn test_format_is_not_a_connector_property() {
    let config: ServerConfig = toml::from_str(
        r#"
[[source]]
name = "input"
connector = "kafka"
[source.properties]
FoRmAt = "json"

[[pipeline]]
name = "events"
sql = "SELECT 1"

[[sink]]
name = "output"
pipeline = "events"
connector = "kafka"
[sink.properties]
format = "json"
"#,
    )
    .unwrap();

    let error = validate_config(&config).unwrap_err().to_string();
    assert!(error.contains("top-level source field"), "{error}");
    assert!(error.contains("top-level sink field"), "{error}");

    let error = toml::from_str::<ServerConfig>(
        r#"
[[source]]
name = "input"
connector = "kafka"
formt = "json"
"#,
    )
    .expect_err("misspelled source runtime fields must fail closed");
    assert!(error.to_string().contains("formt"), "{error}");

    let config: ServerConfig = toml::from_str(
        r#"
[[source]]
name = "input"
connector = "kafka"
[source.properties]
bootstrap.servers = "localhost:9092"
"#,
    )
    .unwrap();
    let error = validate_config(&config).unwrap_err().to_string();
    assert!(error.contains("quote dotted keys"), "{error}");
}

#[test]
fn test_parse_full_cluster_config() {
    let toml = r#"
node_id = "star-1"

[server]
mode = "cluster"
bind = "0.0.0.0:8080"
delivery = "at_least_once"
key_groups = 256

[checkpoint]
url = "s3://bucket/checkpoints"
interval = "30s"

[discovery]
strategy = "static"
seeds = ["node-1:7946", "node-2:7946"]
gossip_port = 7946
failure_domain = "region=us-east-1;zone=us-east-1a;rack=r17"
placement_isolation_tier = 1

[[source]]
name = "orders"
connector = "kafka"
format = "avro"

[[pipeline]]
name = "enrichment"
sql = "SELECT * FROM orders"
parallelism = 8

[[sink]]
name = "output"
pipeline = "enrichment"
connector = "kafka"
"#;

    let config: ServerConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.node_id.as_deref(), Some("star-1"));
    assert_eq!(config.server.mode, ServerMode::Cluster);
    assert_eq!(config.server.delivery, DeliveryGuarantee::AtLeastOnce);
    assert_eq!(config.server.resolved_key_groups().get(), 256);
    assert_eq!(
        CheckpointStorageScope::for_url(&config.checkpoint.url),
        CheckpointStorageScope::ClusterShared
    );
    assert!(config.discovery.is_some());

    let disc = config.discovery.as_ref().unwrap();
    assert_eq!(
        disc.failure_domain.as_deref(),
        Some("region=us-east-1;zone=us-east-1a;rack=r17")
    );
    assert_eq!(disc.placement_isolation_tier, 1);

    validate_config(&config).unwrap();
}

#[test]
fn checkpoint_storage_scope_is_fail_closed() {
    let local_exact: ServerConfig =
        toml::from_str("[server]\ndelivery = \"exactly_once\"\n").unwrap();
    validate_config(&local_exact)
        .expect("the default durable checkpoint URL is sufficient for local exactly-once");

    let uppercase_local_exact: ServerConfig = toml::from_str(
        "[server]\ndelivery = \"exactly_once\"\n[checkpoint]\nurl = \"FILE:///tmp/checkpoints\"\n",
    )
    .unwrap();
    validate_config(&uppercase_local_exact)
        .expect("file URL scheme matching is case-insensitive for local exactly-once");

    let local_cluster: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
    )
    .unwrap();
    let ConfigError::ValidationErrors { errors } = validate_config(&local_cluster).unwrap_err()
    else {
        panic!("expected validation errors");
    };
    assert!(errors
        .iter()
        .any(|error| error.contains("ClusterShared [checkpoint]")));

    let cluster_exact: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"
delivery = "exactly_once"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
    )
    .unwrap();
    let ConfigError::ValidationErrors { errors } = validate_config(&cluster_exact).unwrap_err()
    else {
        panic!("expected validation errors");
    };
    assert!(errors
        .iter()
        .any(|error| { error.contains("ClusterShared [checkpoint]") }));

    let cluster_exact_complete: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"
delivery = "exactly_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
    )
    .unwrap();
    validate_config(&cluster_exact_complete)
        .expect("connector contracts, not the server mode, gate cluster exact delivery");

    let cluster_best_effort: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"
delivery = "best_effort"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
    )
    .unwrap();
    let ConfigError::ValidationErrors { errors } =
        validate_config(&cluster_best_effort).unwrap_err()
    else {
        panic!("expected validation errors");
    };
    assert!(errors.iter().any(|error| {
        error.contains("cluster mode requires at_least_once") && error.contains("best_effort")
    }));

    let volatile_checkpoint: ServerConfig = toml::from_str(
        r#"
[server]
delivery = "at_least_once"

[checkpoint]
url = "memory://checkpoint"
"#,
    )
    .unwrap();
    let ConfigError::ValidationErrors { errors } =
        validate_config(&volatile_checkpoint).unwrap_err()
    else {
        panic!("expected validation errors");
    };
    assert!(errors.iter().any(|error| {
        error.contains("NodeDurable [checkpoint]") && error.contains("source acknowledgements")
    }));

    let shared_local_exact: ServerConfig = toml::from_str(
        r#"
[server]
delivery = "exactly_once"

[checkpoint]
url = "s3://bucket/checkpoints"
"#,
    )
    .unwrap();
    let ConfigError::ValidationErrors { errors } =
        validate_config(&shared_local_exact).unwrap_err()
    else {
        panic!("expected validation errors");
    };
    assert!(errors.iter().any(|error| error.contains("[LDB-0014]")));
}

#[test]
fn test_env_var_substitution_resolves() {
    std::env::set_var("LAMINAR_TEST_VAR_1", "resolved_value");
    let input = "brokers = \"${LAMINAR_TEST_VAR_1}\"";
    let result = substitute_env_vars(input).unwrap();
    assert_eq!(result, "brokers = \"resolved_value\"");
    std::env::remove_var("LAMINAR_TEST_VAR_1");
}

#[test]
fn test_env_var_substitution_with_default() {
    std::env::remove_var("LAMINAR_TEST_UNSET_VAR");
    let input = "brokers = \"${LAMINAR_TEST_UNSET_VAR:-localhost:9092}\"";
    let result = substitute_env_vars(input).unwrap();
    assert_eq!(result, "brokers = \"localhost:9092\"");
}

#[test]
fn escaped_env_var_is_preserved_for_per_node_connector_resolution() {
    std::env::remove_var("LAMINAR_TEST_CONNECTOR_PASSWORD");
    let input = "password = \"$${LAMINAR_TEST_CONNECTOR_PASSWORD}\"";
    let result = substitute_env_vars(input).unwrap();
    assert_eq!(result, "password = \"${LAMINAR_TEST_CONNECTOR_PASSWORD}\"");
}

#[test]
fn test_env_var_substitution_missing_errors() {
    std::env::remove_var("LAMINAR_TEST_MISSING_1");
    std::env::remove_var("LAMINAR_TEST_MISSING_2");
    let input = "a = \"${LAMINAR_TEST_MISSING_1}\"\nb = \"${LAMINAR_TEST_MISSING_2}\"";
    let err = substitute_env_vars(input).unwrap_err();
    match err {
        ConfigError::MissingEnvVars { vars } => {
            assert!(vars.contains(&"LAMINAR_TEST_MISSING_1".to_string()));
            assert!(vars.contains(&"LAMINAR_TEST_MISSING_2".to_string()));
        }
        _ => panic!("expected MissingEnvVars"),
    }
}

#[test]
fn test_validate_sink_references_missing_pipeline() {
    let toml = r#"
[[pipeline]]
name = "exists"
sql = "SELECT 1"

[[sink]]
name = "broken"
pipeline = "nonexistent"
connector = "kafka"
"#;

    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(errors[0].contains("nonexistent"));
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_duplicate_source_names() {
    let toml = r#"
[[source]]
name = "dup"
connector = "kafka"

[[source]]
name = "dup"
connector = "kafka"

[[pipeline]]
name = "p"
sql = "SELECT 1"
"#;

    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(errors.iter().any(|e| e.contains("duplicate source")));
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_duplicate_pipeline_names() {
    let toml = r#"
[[pipeline]]
name = "dup"
sql = "SELECT 1"

[[pipeline]]
name = "dup"
sql = "SELECT 2"
"#;

    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(errors.iter().any(|e| e.contains("duplicate pipeline")));
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_cluster_mode_rejects_tight_checkpoint_interval() {
    // Below 100ms the capture-quorum round-trip itself dominates
    // the barrier.
    let toml = r#"
node_id = "n1"

[server]
mode = "cluster"

[checkpoint]
interval = "50ms"

[discovery]
strategy = "static"
seeds = ["x:1"]

"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors.iter().any(|e| e.contains("too tight")),
                "expected tight-interval error, got: {errors:?}",
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_invalid_bind_address() {
    let toml = r#"
[server]
bind = "not-a-socket-addr"
"#;

    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(errors.iter().any(|e| e.contains("invalid server bind")));
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn remote_cluster_plaintext_is_accepted() {
    let config: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"
bind = "0.0.0.0:8080"
delivery = "at_least_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]
"#,
    )
    .unwrap();
    validate_config(&config).expect("remote cluster may explicitly run without TLS");
}

#[test]
fn cluster_plaintext_and_complete_mtls_are_accepted() {
    let mut config: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"
bind = "127.0.0.1:8080"
delivery = "at_least_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["127.0.0.1:7946"]
"#,
    )
    .unwrap();
    validate_config(&config).expect("cluster control may remain plaintext");

    let directory = tempfile::tempdir().unwrap();
    let cert = directory.path().join("node.crt");
    let key = directory.path().join("node.key");
    let ca = directory.path().join("cluster-ca.crt");
    for path in [&cert, &key, &ca] {
        std::fs::write(path, b"test material").unwrap();
    }
    config.server.bind = "0.0.0.0:8080".into();
    let discovery = config.discovery.as_mut().unwrap();
    discovery.seeds = vec!["10.0.0.2:7946".into()];
    discovery.cluster_tls_cert = Some(cert);
    discovery.cluster_tls_key = Some(key);
    discovery.cluster_tls_client_ca = Some(ca);
    discovery.cluster_tls_server_name = Some("laminardb-cluster.internal".into());
    validate_config(&config).expect("complete remote cluster mTLS should be admitted");
}

#[test]
fn invalid_cluster_tls_server_name_is_not_treated_as_absent() {
    let mut config: ServerConfig = toml::from_str(
        r#"
node_id = "node-1"

[server]
mode = "cluster"
bind = "127.0.0.1:8080"
delivery = "at_least_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["127.0.0.1:7946"]
cluster_tls_server_name = "bad name"
"#,
    )
    .unwrap();
    let ConfigError::ValidationErrors { errors } = validate_config(&config).unwrap_err() else {
        panic!("expected validation errors");
    };
    assert!(
        errors
            .iter()
            .any(|error| error.contains("cluster_tls requires")),
        "errors: {errors:?}"
    );

    config.server.mode = ServerMode::Single;
    validate_config(&config).expect("cluster TLS settings do not affect single-node mode");
}

#[test]
fn test_validate_zero_max_connections() {
    let toml = r#"
[server]
pgwire_max_connections = 0
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors.iter().any(|e| e.contains("must be > 0")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_remote_pgwire_requires_tls() {
    let toml = r#"
[server]
pgwire_bind = "0.0.0.0:5432"
pgwire_allow_remote = true
pgwire_users = { alice = "wonderland-key" }
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("non-loopback pgwire_bind requires")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_client_ca_requires_server_cert() {
    let toml = r#"
[server]
pgwire_tls_client_ca = "/does/not/matter.pem"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("requires pgwire_tls_cert")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_rejects_unknown_tls_min_version() {
    let toml = r#"
[server]
pgwire_tls_min_version = "1.4"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors.iter().any(|e| e.contains("pgwire_tls_min_version")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_accepts_well_formed_pre_hashed_pgwire_password() {
    let toml = r#"
[server]
[server.pgwire_users]
alice = "md55d41402abc4b2a76b9719d911017c592"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    // 35-char pre-hashed value bypasses the MIN_PGWIRE_PASSWORD_LEN gate.
    validate_config(&config).expect("well-formed pre-hash must validate");
}

#[test]
fn test_validate_rejects_malformed_pre_hashed_pgwire_password() {
    // 'md5' prefix followed by non-hex — clearly meant to be pre-hashed
    // but malformed; rejected so a typo doesn't slip through as plaintext.
    let toml = r#"
[server]
[server.pgwire_users]
alice = "md5zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors.iter().any(|e| e.contains("pre-hashed")),
                "errors: {errors:?}",
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_short_pgwire_password() {
    let toml = r#"
[server]
[server.pgwire_users]
alice = "short"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors.iter().any(|e| e.contains("at least 12 characters")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_short_console_token() {
    let toml = r#"
[server]
console_token = "abc"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("server.console_token must be at least 8 characters")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_validate_accepts_well_formed_console_token() {
    let toml = r#"
[server]
console_token = "supersecret-token"
console_cors_allowed_origins = ["https://console.example.com", "http://localhost:5173"]
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    validate_config(&config).expect("8+ char console token must validate");
    assert_eq!(config.server.console_token.as_ref().unwrap().len(), 17);
    assert_eq!(
        config.server.console_cors_allowed_origins,
        Some(vec![
            "https://console.example.com".to_string(),
            "http://localhost:5173".to_string(),
        ])
    );
}

#[test]
fn legacy_console_only_token_remains_compatible() {
    let config: ServerConfig = toml::from_str(
        r#"
[server]
console_token = "supersecret-token"
"#,
    )
    .unwrap();

    validate_http_auth(&config).expect("legacy console-only credentials remain valid");
}

#[test]
fn diagnostic_token_requires_console_token() {
    let config = diagnostic_auth_config(canonical_http_auth_secret(1), None);
    let errors = http_auth_errors(&config);

    assert!(
        errors
            .iter()
            .any(|error| error.contains("requires server.console_token")),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_token_rejects_weak_diagnostic_credential() {
    let config = diagnostic_auth_config(Secret::new("weak"), Some(canonical_http_auth_secret(2)));
    let errors = http_auth_errors(&config);

    assert!(
        errors.iter().any(|error| {
            error.contains("server.diagnostic_read_token") && error.contains("exactly 32 bytes")
        }),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_token_rejects_noncanonical_base64url() {
    let mut noncanonical = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([3_u8; 32]);
    noncanonical.replace_range(HTTP_AUTH_TOKEN_ENCODED_LEN - 1.., "B");
    assert_eq!(noncanonical.len(), HTTP_AUTH_TOKEN_ENCODED_LEN);

    let config = diagnostic_auth_config(
        Secret::new(noncanonical),
        Some(canonical_http_auth_secret(4)),
    );
    let errors = http_auth_errors(&config);

    assert!(
        errors.iter().any(|error| {
            error.contains("server.diagnostic_read_token") && error.contains("canonical")
        }),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_token_requires_strong_console_credential() {
    let config = diagnostic_auth_config(canonical_http_auth_secret(5), Some(Secret::new("weak")));
    let errors = http_auth_errors(&config);

    assert!(
        errors.iter().any(|error| {
            error.contains("server.console_token") && error.contains("exactly 32 bytes")
        }),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_and_console_tokens_must_be_distinct() {
    let token = canonical_http_auth_secret(6);
    let config = diagnostic_auth_config(token.clone(), Some(token));
    let errors = http_auth_errors(&config);

    assert!(
        errors.iter().any(|error| error.contains("must differ")),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_token_requires_cluster_mode() {
    let mut config = diagnostic_auth_config(
        canonical_http_auth_secret(7),
        Some(canonical_http_auth_secret(8)),
    );
    config.server.mode = ServerMode::Single;
    let errors = http_auth_errors(&config);

    assert!(
        errors
            .iter()
            .any(|error| error.contains("requires server.mode = \"cluster\"")),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_token_requires_loopback_http_bind() {
    let mut config = diagnostic_auth_config(
        canonical_http_auth_secret(9),
        Some(canonical_http_auth_secret(10)),
    );
    config.server.bind = "0.0.0.0:8080".to_string();
    let errors = http_auth_errors(&config);

    assert!(
        errors.iter().any(|error| error.contains("loopback")),
        "errors: {errors:?}"
    );
}

#[test]
fn diagnostic_token_accepts_valid_split_credentials() {
    let diagnostic = canonical_http_auth_secret(11);
    let console = canonical_http_auth_secret(12);
    let diagnostic_value = diagnostic.expose().to_string();
    let console_value = console.expose().to_string();
    let config = diagnostic_auth_config(diagnostic, Some(console));

    validate_http_auth(&config).expect("valid split diagnostic credentials must pass");
    let debug = format!("{:?}", config.server);
    assert!(
        !debug.contains(&diagnostic_value),
        "diagnostic token leaked"
    );
    assert!(!debug.contains(&console_value), "console token leaked");
}

#[test]
fn file_loader_uses_the_shared_diagnostic_auth_validator() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("diagnostic-auth.toml");
    let diagnostic = canonical_http_auth_secret(13);
    let console = canonical_http_auth_secret(14);
    let invalid = format!(
        "[server]\nconsole_token = \"{}\"\ndiagnostic_read_token = \"{}\"\n",
        console.expose(),
        diagnostic.expose()
    );
    std::fs::write(&path, invalid).unwrap();
    let error = load_config(&path).expect_err("single-node diagnostic auth must fail");
    assert!(
        error
            .to_string()
            .contains("diagnostic_read_token requires server.mode"),
        "{error}"
    );

    let valid = format!(
        r#"node_id = "node-1"

[server]
mode = "cluster"
bind = "127.0.0.1:8080"
console_token = "{}"
diagnostic_read_token = "{}"

[checkpoint]
url = "az://laminardb-test/checkpoints"

[discovery]
strategy = "static"
seeds = []
gossip_port = 7946
"#,
        console.expose(),
        diagnostic.expose()
    );
    std::fs::write(&path, valid).unwrap();
    let loaded = load_config(&path).expect("valid file-based split credentials must pass");
    assert_eq!(loaded.server.mode, ServerMode::Cluster);
    assert_eq!(
        loaded
            .server
            .diagnostic_read_token
            .as_ref()
            .unwrap()
            .expose(),
        diagnostic.expose()
    );
}

#[test]
fn test_console_token_redacted_in_debug() {
    let toml = r#"
[server]
console_token = "supersecret-token"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    validate_config(&config).unwrap();
    let dump = format!("{:?}", config.server);
    assert!(!dump.contains("supersecret"), "secret leaked: {dump}");
    assert!(
        dump.contains("REDACTED"),
        "expected REDACTED marker: {dump}"
    );
}

#[test]
fn test_validate_invalid_cors_origin() {
    // A control character (bell, U+0007) in the origin makes it an invalid
    // HTTP header value, so config validation must reject it. TOML basic
    // strings can't carry a raw control byte, so the field is set in Rust.
    let toml = r#"
[server]
"#;
    let mut config: ServerConfig = toml::from_str(toml).unwrap();
    config.server.console_cors_allowed_origins =
        Some(vec!["http://e\u{0007}vil.example.com".to_string()]);
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(
                errors
                    .iter()
                    .any(|e| e.contains("invalid origin in server.console_cors_allowed_origins")),
                "errors: {errors:?}"
            );
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_console_auth_defaults_to_none() {
    let config = ServerSection::default();
    assert!(config.console_token.is_none());
    assert!(config.diagnostic_read_token.is_none());
    assert!(config.console_cors_allowed_origins.is_none());
}

#[test]
fn test_validate_pgwire_password_redacted_in_debug() {
    let toml = r#"
[server]
[server.pgwire_users]
alice = "wonderland-key"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    validate_config(&config).unwrap();
    let dump = format!("{:?}", config.server);
    assert!(!dump.contains("wonderland"), "secret leaked: {dump}");
    assert!(
        dump.contains("REDACTED"),
        "expected REDACTED marker: {dump}"
    );
}

#[test]
fn test_default_values_applied() {
    let config = ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        discovery: None,
        node_id: None,
        sql: None,
        ai: Default::default(),
        models: Default::default(),
    };

    assert_eq!(config.server.mode, ServerMode::Single);
    assert_eq!(config.server.bind, "127.0.0.1:8080");
    assert_eq!(config.checkpoint.interval, Duration::from_secs(10));
    assert_eq!(config.checkpoint.timeout, Duration::from_secs(120));
}

#[test]
fn event_time_durations_use_engine_millisecond_bounds() {
    let parsed: ServerConfig = toml::from_str(
            "[server]\ntemporal_join_idle_history_retention = \"24h\"\nsource_idle_timeout = \"5s\"\nevent_time_max_future_skew = \"30s\"\n",
        )
        .unwrap();
    assert_eq!(
        parsed.server.temporal_join_idle_history_retention,
        Some(Duration::from_secs(24 * 60 * 60))
    );
    assert_eq!(
        parsed.server.source_idle_timeout,
        Some(Duration::from_secs(5))
    );
    assert_eq!(
        parsed.server.event_time_max_future_skew,
        Duration::from_secs(30)
    );
    validate_config(&parsed).unwrap();

    let default: ServerConfig = toml::from_str("").unwrap();
    assert_eq!(default.server.temporal_join_idle_history_retention, None);
    assert_eq!(default.server.source_idle_timeout, None);
    assert_eq!(
        default.server.event_time_max_future_skew,
        Duration::from_millis(laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS.unsigned_abs())
    );

    for (retention, expected) in [
        (
            Duration::ZERO,
            "temporal_join_idle_history_retention must be at least 1ms",
        ),
        (
            Duration::from_nanos(999_999),
            "temporal_join_idle_history_retention must be at least 1ms",
        ),
        (
            Duration::from_millis((i64::MAX as u64) + 1),
            "temporal_join_idle_history_retention exceeds the supported millisecond range",
        ),
    ] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.temporal_join_idle_history_retention = Some(retention);
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }

    for (timeout, expected) in [
        (Duration::ZERO, "source_idle_timeout must be at least 1ms"),
        (
            Duration::from_nanos(999_999),
            "source_idle_timeout must be at least 1ms",
        ),
        (
            Duration::MAX,
            "source_idle_timeout exceeds the supported millisecond range",
        ),
    ] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.source_idle_timeout = Some(timeout);
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }

    for (skew, expected) in [
        (
            Duration::from_nanos(1),
            "event_time_max_future_skew must be zero or at least 1ms",
        ),
        (
            Duration::MAX,
            "event_time_max_future_skew exceeds the supported millisecond range",
        ),
    ] {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.event_time_max_future_skew = skew;
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains(expected), "{error}");
    }
    let mut disabled: ServerConfig = toml::from_str("").unwrap();
    disabled.server.event_time_max_future_skew = Duration::ZERO;
    validate_config(&disabled).unwrap();
}

#[test]
fn key_groups_are_mode_neutral_and_typed() {
    let single: ServerConfig = toml::from_str("[server]\n").unwrap();
    assert_eq!(single.server.resolved_key_groups(), DEFAULT_KEY_GROUP_COUNT);

    let configured_single: ServerConfig =
        toml::from_str("[server]\nmode = \"single\"\nkey_groups = 64\n").unwrap();
    assert_eq!(configured_single.server.resolved_key_groups().get(), 64);
    validate_config(&configured_single).unwrap();

    let cluster: ServerConfig = toml::from_str("[server]\nmode = \"cluster\"\n").unwrap();
    assert_eq!(
        cluster.server.resolved_key_groups(),
        DEFAULT_KEY_GROUP_COUNT
    );

    let configured: ServerConfig =
        toml::from_str("[server]\nmode = \"cluster\"\nkey_groups = 1024\n").unwrap();
    assert_eq!(configured.server.resolved_key_groups().get(), 1024);

    for mode in ["single", "cluster"] {
        for invalid in [0_u32, u32::from(u16::MAX) + 1] {
            let input = format!("[server]\nmode = \"{mode}\"\nkey_groups = {invalid}\n");
            assert!(toml::from_str::<ServerConfig>(&input).is_err());
        }
    }
}

#[test]
fn test_checkpoint_duration_parsing() {
    let toml = r#"
[checkpoint]
interval = "30s"
timeout = "2m"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.checkpoint.interval, Duration::from_secs(30));
    assert_eq!(config.checkpoint.timeout, Duration::from_secs(120));

    let toml2 = r#"
[checkpoint]
interval = "1m"
"#;
    let config2: ServerConfig = toml::from_str(toml2).unwrap();
    assert_eq!(config2.checkpoint.interval, Duration::from_secs(60));

    let toml3 = r#"
[checkpoint]
interval = "500ms"
"#;
    let config3: ServerConfig = toml::from_str(toml3).unwrap();
    assert_eq!(config3.checkpoint.interval, Duration::from_millis(500));
}

#[test]
fn incremental_emit_is_server_execution_policy() {
    let config: ServerConfig = toml::from_str(
        r#"
[server]
incremental_emit = false
"#,
    )
    .unwrap();

    assert!(!config.server.incremental_emit);
}

#[test]
fn sink_rejects_per_connector_delivery_dimension() {
    let error = toml::from_str::<ServerConfig>(
        r#"
[[sink]]
name = "out"
pipeline = "p"
connector = "kafka"
delivery = "exactly_once"
"#,
    )
    .expect_err("delivery is a pipeline-wide server contract");
    assert!(error.to_string().contains("unknown field"), "{error}");
}

#[test]
fn test_watermark_config_parsing() {
    let toml = r#"
[[source]]
name = "s"
connector = "kafka"
[source.watermark]
column = "event_time"
max_out_of_orderness = "10s"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let wm = config.sources[0].watermark.as_ref().unwrap();
    assert_eq!(wm.column, "event_time");
    assert_eq!(wm.max_out_of_orderness, Duration::from_secs(10));
}

#[test]
fn test_lookup_cache_defaults() {
    let cache = LookupCacheConfig::default();
    assert_eq!(cache.size_bytes, 100 * 1024 * 1024);
    assert_eq!(cache.ttl, Duration::from_secs(300));
}

#[test]
fn test_cluster_mode_requires_discovery() {
    let toml = r#"
[server]
mode = "cluster"

[checkpoint]
interval = "10s"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    let err = validate_config(&config).unwrap_err();
    match err {
        ConfigError::ValidationErrors { errors } => {
            assert!(errors.iter().any(|e| e.contains("[discovery]")));
            assert!(errors.iter().any(|e| e.contains("node_id")));
        }
        _ => panic!("expected ValidationErrors"),
    }
}

#[test]
fn test_source_schema_parsing() {
    let toml = r#"
[[source]]
name = "test"
connector = "kafka"
primary_key = ["id"]
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "name"
type = "VARCHAR"
"#;
    let config: ServerConfig = toml::from_str(toml).unwrap();
    assert_eq!(config.sources[0].schema.len(), 2);
    assert_eq!(config.sources[0].schema[0].data_type, "BIGINT");
    assert!(!config.sources[0].schema[0].nullable);
    assert_eq!(config.sources[0].schema[1].data_type, "VARCHAR");
    assert!(config.sources[0].schema[1].nullable); // default
    assert_eq!(config.sources[0].primary_key, ["id"]);
}

#[test]
fn test_config_error_display_messages() {
    let err = ConfigError::MissingEnvVars {
        vars: vec!["A".to_string(), "B".to_string()],
    };
    assert_eq!(err.to_string(), "missing environment variables: A, B");

    let err = ConfigError::ValidationErrors {
        errors: vec!["error one".to_string(), "error two".to_string()],
    };
    let msg = err.to_string();
    assert!(msg.contains("error one"));
    assert!(msg.contains("error two"));
}
